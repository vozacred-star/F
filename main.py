import asyncio
import json
import logging
import math
import os
import aiohttp
import websockets

WS_URL = "wss://stream.bybit.com/v5/public/linear"

ALERT_THRESHOLD = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ---------------- STORAGE ----------------
data_store = {}
ema = {}
ema_var = {}

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

    oi_z = zscore(symbol+"_oi", oi)
    vol_z = zscore(symbol+"_vol", volume)

    update_ema(symbol+"_oi", oi)
    update_ema(symbol+"_vol", volume)

    update_var(symbol+"_oi", oi)
    update_var(symbol+"_vol", volume)

    flow_score = (
        oi_z * 3 +
        vol_z * 2 +
        0  # price move пока упрощён
    ) * math.log10(oi + 1)

    return {
        "symbol": symbol,
        "score": flow_score
    }

# ---------------- WS HANDLER ----------------
async def handle_ws():
    async with websockets.connect(WS_URL) as ws:

        # подписка (ВСЕ тикеры)
        sub_msg = {
            "op": "subscribe",
            "args": ["tickers.*"]
        }
        await ws.send(json.dumps(sub_msg))

        logging.info("Subscribed to tickers")

        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                if "data" not in data:
                    continue

                for item in data["data"]:
                    symbol = item.get("symbol")

                    price = float(item.get("lastPrice", 0))
                    volume = float(item.get("turnover24h", 0))
                    oi = float(item.get("openInterest", 0))

                    if symbol not in data_store:
                        data_store[symbol] = {}

                    data_store[symbol]["price"] = price
                    data_store[symbol]["volume"] = volume
                    data_store[symbol]["oi"] = oi

                    result = analyze(symbol)

                    if result and result["score"] > ALERT_THRESHOLD:
                        logging.info(f"ALERT {symbol} {result['score']:.2f}")

                        asyncio.create_task(send_alert(
                            f"🚨 <b>FLOW ALERT</b>\n"
                            f"{symbol}\n"
                            f"Score: {result['score']:.2f}"
                        ))

            except Exception as e:
                logging.error(f"WS error: {e}")
                await asyncio.sleep(3)

# ---------------- MAIN ----------------
async def main():
    while True:
        try:
            await handle_ws()
        except Exception as e:
            logging.error(f"Reconnect: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
