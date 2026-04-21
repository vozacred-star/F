import asyncio
import json
import logging
import math
import os
import websockets
import aiohttp

WS_URL = "wss://stream.bybit.com/v5/public/linear"

ALERT_THRESHOLD = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ---------------- STORAGE ----------------
data_store = {}
ema = {}
ema_var = {}

# ---------------- TELEGRAM ----------------
async def send_alert(text):
    if not BOT_TOKEN or not CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    async with aiohttp.ClientSession() as session:
        await session.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        })

# ---------------- MATH ----------------
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

# ---------------- ANALYSIS ----------------
def analyze(symbol):
    d = data_store.get(symbol)
    if not d:
        return None

    price = d.get("price")
    volume = d.get("volume")
    oi = d.get("oi")

    if not price or not volume or not oi:
        return None

    oi_z = zscore(symbol + "_oi", oi)
    vol_z = zscore(symbol + "_vol", volume)

    update_ema(symbol + "_oi", oi)
    update_ema(symbol + "_vol", volume)

    update_var(symbol + "_oi", oi)
    update_var(symbol + "_vol", volume)

    flow_score = (
        oi_z * 3 +
        vol_z * 2
    ) * math.log10(oi + 1)

    return {
        "symbol": symbol,
        "score": flow_score
    }

# ---------------- WS ----------------
async def ws_handler():
    async with websockets.connect(WS_URL, ping_interval=20) as ws:

        await ws.send(json.dumps({
            "op": "subscribe",
            "args": ["tickers.*"]
        }))

        logging.info("Subscribed to tickers")

        while True:
            msg = await ws.recv()

            try:
                data = json.loads(msg)
            except:
                continue

            # ignore system messages
            if "data" not in data:
                continue

            items = data["data"]

            if not isinstance(items, list):
                continue

            for item in items:
                symbol = item.get("symbol")

                if not symbol:
                    continue

                price = float(item.get("lastPrice", 0))
                volume = float(item.get("turnover24h", 0))
                oi = float(item.get("openInterest", 0))

                # store
                data_store[symbol] = {
                    "price": price,
                    "volume": volume,
                    "oi": oi
                }

                # DEBUG LOG (ВАЖНО)
                logging.info(f"{symbol} price={price} vol={volume} oi={oi}")

                result = analyze(symbol)

                if result and result["score"] > ALERT_THRESHOLD:
                    logging.info(f"ALERT {symbol} {result['score']:.2f}")

                    asyncio.create_task(send_alert(
                        f"🚨 <b>FLOW ALERT</b>\n"
                        f"{symbol}\n"
                        f"Score: {result['score']:.2f}"
                    ))

# ---------------- MAIN ----------------
async def main():
    while True:
        try:
            await ws_handler()
        except Exception as e:
            logging.error(f"WS reconnect: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
