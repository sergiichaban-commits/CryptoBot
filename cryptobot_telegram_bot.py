# cryptobot_telegram_bot_v10.5.py
import asyncio
import aiohttp
import logging
import time
from datetime import datetime, timedelta
import os
from statistics import mean

# === Logging ===
logging.basicConfig(level=logging.INFO, format='[%(asctime)s %(levelname)s] %(message)s')

# === ENV ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_IDS = [int(cid) for cid in os.getenv("ALLOWED_CHAT_IDS", "").split(",") if cid]
STALL_EXIT_SEC = int(os.getenv("STALL_EXIT_SEC", "240"))
REPORT_ERRORS_TO_TG = os.getenv("REPORT_ERRORS_TO_TG") == "1"
PUBLIC_URL = os.getenv("PUBLIC_URL")

# === Settings ===
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"
]
RSI_PERIOD = 14
TIMEFRAME = "5m"

# === Signal tracking ===
sent_signals = {}
signal_time_threshold_sec = 60 * 5  # 5 минут

# === Telegram ===
class Telegram:
    def __init__(self, token, session):
        self.token = token
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = session

    async def send(self, chat_id, text):
        try:
            url = f"{self.base}/sendMessage"
            payload = {"chat_id": chat_id, "text": text}
            async with self.http.post(url, json=payload) as r:
                if r.status != 200:
                    logging.warning("Failed to send message: %s", await r.text())
        except Exception as e:
            logging.error("Telegram send failed: %s", e)

# === RSI Calculation ===
def calculate_rsi(closes, period):
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        (gains if delta > 0 else losses).append(abs(delta))
    avg_gain = mean(gains[-period:]) if gains else 0
    avg_loss = mean(losses[-period:]) if losses else 0
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# === Signal Generation ===
def detect_reversal(symbol, closes):
    if len(closes) < RSI_PERIOD + 1:
        return None
    rsi = calculate_rsi(closes[-(RSI_PERIOD + 1):], RSI_PERIOD)
    now = datetime.utcnow()
    if symbol in sent_signals and (now - sent_signals[symbol]).total_seconds() < signal_time_threshold_sec:
        return None
    if rsi < 30:
        sent_signals[symbol] = now
        return f"\U0001F7E2 Лонг сигнал по {symbol}\nЦена: {closes[-1]:.2f}\nRSI: {rsi:.1f}"
    elif rsi > 70:
        sent_signals[symbol] = now
        return f"\U0001F534 Шорт сигнал по {symbol}\nЦена: {closes[-1]:.2f}\nRSI: {rsi:.1f}"
    return None

# === Fetch Klines ===
async def fetch_klines(session, symbol):
    url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval=5&limit={RSI_PERIOD + 50}"
    try:
        async with session.get(url) as r:
            data = await r.json()
            items = data.get("result", {}).get("list", [])
            closes = [float(c[4]) for c in items]
            return closes[-(RSI_PERIOD + 1):]
    except Exception as e:
        logging.error("Fetch failed for %s: %s", symbol, e)
        return []

# === Core Loop ===
async def run_loop():
    async with aiohttp.ClientSession() as session:
        tg = Telegram(TELEGRAM_TOKEN, session)
        while True:
            try:
                tasks = [fetch_klines(session, sym) for sym in SYMBOLS]
                all_closes = await asyncio.gather(*tasks)
                for symbol, closes in zip(SYMBOLS, all_closes):
                    signal = detect_reversal(symbol, closes)
                    if signal:
                        for chat_id in CHAT_IDS:
                            await tg.send(chat_id, signal)
                await asyncio.sleep(60)
            except Exception as e:
                logging.exception("Error in main loop")
                if REPORT_ERRORS_TO_TG:
                    for cid in CHAT_IDS:
                        await tg.send(cid, f"[Ошибка] {e}")
                await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(run_loop())
