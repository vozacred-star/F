import asyncio
import json
import logging
import websockets

WS_URL = "wss://stream.bybit.com/v5/public/linear"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


async def ws_dump():
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
                logging.info(f"RAW (not json): {msg}")
                continue

            # просто печатаем ВСЁ что приходит
            logging.info("========== NEW MESSAGE ==========")
            logging.info(json.dumps(data, indent=2)[:2000])  # ограничение вывода


async def main():
    while True:
        try:
            await ws_dump()
        except Exception as e:
            logging.error(f"Reconnect: {e}")
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
