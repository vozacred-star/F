import asyncio
import aiohttp
import logging
import math
import os
from aiohttp_socks import ProxyConnector

BASE = "https://api.bybit.com"

PROXY = "socks5://184.178.172.5:15303"

LIMIT = 10
POLL = 20
CONCURRENCY = 50

MIN_OI = 800000
MIN_VOL = 200000
ALERT_THRESHOLD = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

ema = {}
ema_var = {}


# ---------------- SAFE JSON ----------------
async def safe_json(resp):
    try:
        return await resp.json()
    except:
        text = await resp.text()
        logging.warning(f"BAD JSON: {text[:200]}")
        return None


# ---------------- RETRY REQUEST ----------------
async def safe_request(session, url, params=None):
    for _ in range(3):
        try:
            async with session.get(url, params=params, timeout=10) as r:
                return await safe_json(r)
        except Exception as e:
            logging.warning(f"retry request error: {e}")
            await asyncio.sleep(1)
    return None


# ---------------- MATH ----------------
def safe_div(a, b):
    return a / b if b != 0 else 0


def update_ema(key, value, alpha=0.2):
    if key not in ema:
        ema[key] = value
    else:
        ema[key] = alpha * value + (1 - alpha) * ema[key]
    return ema[key]


def update_var(key, value, alpha=0.2):
    if key not in ema_var:
        ema_var[key] = value
    else:
        ema_var[key] = alpha * abs(value - ema.get(key, value)) + (1 - alpha) * ema_var[key]
    return ema_var[key]


def zscore(key, value):
    m = ema.get(key, value)
    v = ema_var.get(key, 1e-6)
    if v < 1e-6:
        return 0
    return (value - m) / v


# ---------------- TELEGRAM ----------------
async def send_alert(session, text):
    if not BOT_TOKEN or not CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    try:
        async with session.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }) as r:
            await r.text()
    except Exception as e:
        logging.error(f"Telegram error: {e}")


# ---------------- SYMBOLS ----------------
async def fetch_symbols(session):
    url = f"{BASE}/v5/market/instruments-info"
    params = {"category": "linear", "limit": 1000}

    symbols = []
    cursor = None

    while True:
        p = dict(params)
        if cursor:
            p["cursor"] = cursor

        data = await safe_request(session, url, p)

        if not data or "result" not in data:
            logging.error("Failed to load symbols (proxy/API blocked)")
            return symbols

        for s in data["result"]["list"]:
            symbols.append(s["symbol"])

        cursor = data["result"].get("nextPageCursor")
        if not cursor:
            break

    return symbols


# ---------------- DATA ----------------
async def fetch_data(session, symbol, sem):
    async with sem:
        try:
            oi_url = f"{BASE}/v5/market/open-interest"
            k_url = f"{BASE}/v5/market/kline"

            params = {
                "category": "linear",
                "symbol": symbol,
                "interval": "5",
                "limit": LIMIT
            }

            oi_data = await safe_request(session, oi_url, params)
            k_data = await safe_request(session, k_url, params)

            if not oi_data or not k_data:
                return None

            oi_list = oi_data.get("result", {}).get("list", [])
            kl_list = k_data.get("result", {}).get("list", [])

            if len(oi_list) < 5 or len(kl_list) < 5:
                return None

            oi = float(oi_list[0]["openInterest"])
            close = [float(x[4]) for x in kl_list]
            vol = [float(x[5]) for x in kl_list]

            return symbol, oi, close, vol

        except:
            return None


# ---------------- ANALYSIS ----------------
def analyze(symbol, oi, close, vol):
    if oi < MIN_OI:
        return None

    price_move = safe_div(close[-1] - close[-2], close[-2]) * 100
    vol_now = vol[-1]

    if vol_now < MIN_VOL:
        return None

    oi_z = zscore(symbol + "_oi", oi)
    vol_z = zscore(symbol + "_vol", vol_now)

    update_ema(symbol + "_oi", oi)
    update_ema(symbol + "_vol", vol_now)

    update_var(symbol + "_oi", oi)
    update_var(symbol + "_vol", vol_now)

    flow_score = (
        oi_z * 3 +
        vol_z * 2 +
        price_move * 4
    ) * math.log10(oi + 1)

    accumulation = (oi_z > 0.5 and vol_z > 1.0 and abs(price_move) < 0.3)

    return {
        "symbol": symbol,
        "score": flow_score,
        "oi_z": oi_z,
        "vol_z": vol_z,
        "price": price_move,
        "accumulation": accumulation
    }


def get_leaders(data):
    data = [x for x in data if x]
    return sorted(data, key=lambda x: x["score"], reverse=True)[:3]


# ---------------- MAIN ----------------
async def main():
    connector = ProxyConnector.from_url(PROXY)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers
    ) as session:

        symbols = await fetch_symbols(session)

        if not symbols:
            logging.error("NO SYMBOLS → proxy dead or blocked")
            return

        logging.info(f"symbols: {len(symbols)}")

        sem = asyncio.Semaphore(CONCURRENCY)

        while True:
            tasks = [fetch_data(session, s, sem) for s in symbols]
            results = await asyncio.gather(*tasks)

            analyzed = []

            for r in results:
                if not r:
                    continue
                symbol, oi, close, vol = r
                res = analyze(symbol, oi, close, vol)
                if res:
                    analyzed.append(res)

            leaders = get_leaders(analyzed)

            logging.info("====== FLOW LEADERS ======")

            for i, l in enumerate(leaders, 1):
                logging.info(
                    f"{i}. {l['symbol']} score={l['score']:.2f} "
                    f"oi_z={l['oi_z']:.2f} vol_z={l['vol_z']:.2f}"
                )

            if leaders:
                top = leaders[0]

                if top["score"] > ALERT_THRESHOLD:
                    msg = (
                        f"🚨 <b>FLOW ALERT</b>\n"
                        f"{top['symbol']}\n"
                        f"Score: {top['score']:.2f}\n"
                        f"OI Z: {top['oi_z']:.2f}\n"
                        f"VOL Z: {top['vol_z']:.2f}\n"
                        f"Price: {top['price']:.3f}%"
                    )

                    await send_alert(session, msg)

            await asyncio.sleep(POLL)


if __name__ == "__main__":
    asyncio.run(main())
