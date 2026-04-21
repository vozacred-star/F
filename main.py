import asyncio
import json
import logging
import aiohttp
import websockets

BASE_REST = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear"

BATCH_SIZE = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


# ---------------- SAFE REST REQUEST ----------------
async def safe_get_json(session, url, params):
    try:
        async with session.get(url, params=params, timeout=10) as r:
            text = await r.text()

            # если HTML или блок
            if "result" not in text:
                logging.warning(f"BAD RESPONSE: {text[:200]}")
                return None

            return json.loads(text)

    except Exception as e:
        logging.warning(f"REQ ERROR: {e}")
        return None


# ---------------- GET SYMBOLS ----------------
async def get_symbols():
    url = f"{BASE_REST}/v5/market/instruments-info"
    params = {"category": "linear", "limit": 1000}

    symbols = []
    cursor = None

    async with aiohttp.ClientSession() as session:
        while True:
            p = dict(params)
            if cursor:
                p["cursor"] = cursor

            data = await safe_get_json(session, url, p)

            if not data:
                return symbols

            if "result" not in data:
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
    async with websockets.connect(WS_URL, ping_interval=20) as ws:

        args = [f"tickers.{s}" for s in symbols]

        await ws.send(json.dumps({
            "op": "subscribe",
            "args": args
        }))

        logging.info(f"[{batch_id}] subscribed {len(symbols)} symbols")

        while True:
            msg = await ws.recv()

            try:
                data = json.loads(msg)
            except:
                continue

            if "data" not in data:
                continue

            items = data["data"]

            if not isinstance(items, list):
                continue

            for item in items:
                symbol = item.get("symbol")
                price = item.get("lastPrice")
                volume = item.get("turnover24h")

                logging.info(
                    f"[{batch_id}] {symbol} price={price} vol={volume}"
                )


# ---------------- MAIN ----------------
async def main():
    symbols = await get_symbols()

    if not symbols:
        logging.error("NO SYMBOLS (blocked or API fail)")
        return

    logging.info(f"Total symbols: {len(symbols)}")

    batches = list(chunk(symbols, BATCH_SIZE))

    tasks = []

    for i, batch in enumerate(batches):
        tasks.append(ws_worker(i, batch))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
