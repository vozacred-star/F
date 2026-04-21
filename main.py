import asyncio
import json
import logging
import aiohttp
import websockets
from aiohttp_socks import ProxyConnector
import random

BASE_REST = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear"

BATCH_SIZE = 40

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


# ---------------- PROXIES ----------------
PROXIES = [
    "socks5://103.71.22.23:1080",
    "socks5://104.248.151.220:57554",
    "socks5://206.123.156.224:10192",
    "socks5://103.197.242.78:1080",
    "socks5://192.248.95.98:54126",
    "socks5://136.243.68.243:7981",
    "socks5://125.24.156.113:7080",
    "socks5://194.87.191.118:20090",
    "socks5://185.175.229.58:1080",
    "socks5://94.130.16.48:30153",
    "socks5://203.189.152.79:1080",
]

BAD_IP = "103.239.52.100:1080"


# ---------------- SAFE REQUEST ----------------
async def safe_json(session, url, params):
    try:
        async with session.get(url, params=params, timeout=10) as r:
            text = await r.text()

            if "result" not in text:
                logging.warning(f"BAD RESP: {text[:150]}")
                return None

            return json.loads(text)

    except Exception as e:
        logging.warning(f"REQ ERROR: {e}")
        return None


# ---------------- SYMBOLS ----------------
async def get_symbols():
    url = f"{BASE_REST}/v5/market/instruments-info"
    params = {"category": "linear", "limit": 1000}

    symbols = []
    cursor = None

    proxy = random.choice(PROXIES)
    connector = ProxyConnector.from_url(proxy)

    logging.info(f"REST proxy: {proxy}")

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            p = dict(params)
            if cursor:
                p["cursor"] = cursor

            data = await safe_json(session, url, p)

            if not data:
                return symbols

            for s in data["result"]["list"]:
                symbols.append(s["symbol"])

            cursor = data["result"].get("nextPageCursor")
            if not cursor:
                break

    return symbols


# ---------------- CHUNK ----------------
def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# ---------------- WS WORKER ----------------
async def ws_worker(batch_id, symbols):
    proxy = random.choice([p for p in PROXIES if BAD_IP not in p])

    logging.info(f"[WS {batch_id}] proxy: {proxy}")

    connector = ProxyConnector.from_url(proxy)

    async with websockets.connect(WS_URL, ping_interval=20) as ws:

        args = [f"tickers.{s}" for s in symbols]

        await ws.send(json.dumps({
            "op": "subscribe",
            "args": args
        }))

        logging.info(f"[{batch_id}] subscribed {len(symbols)}")

        while True:
            msg = await ws.recv()

            try:
                data = json.loads(msg)
            except:
                continue

            if "data" not in data:
                continue

            for item in data["data"]:
                symbol = item.get("symbol")
                price = item.get("lastPrice")
                vol = item.get("turnover24h")

                logging.info(f"[{batch_id}] {symbol} price={price} vol={vol}")


# ---------------- MAIN ----------------
async def main():
    symbols = await get_symbols()

    if not symbols:
        logging.error("NO SYMBOLS")
        return

    logging.info(f"symbols: {len(symbols)}")

    batches = list(chunk(symbols, BATCH_SIZE))

    tasks = []

    for i, batch in enumerate(batches):
        tasks.append(ws_worker(i, batch))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
