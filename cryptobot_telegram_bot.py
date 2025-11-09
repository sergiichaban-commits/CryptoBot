import asyncio
import aiohttp
import os
import json
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pytz import UTC
from typing import List, Dict

import telebot

# Environment configuration
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_IDS = [int(cid) for cid in os.getenv("ALLOWED_CHAT_IDS", "").split(",") if cid]
ONLY_CHANNEL = os.getenv("ONLY_CHANNEL", "0") == "1"
REPORT_ERRORS = os.getenv("REPORT_ERRORS_TO_TG", "0") == "1"
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "900"))
STALL_EXIT_SEC = int(os.getenv("STALL_EXIT_SEC", "240"))

# Thresholds
VOLUME_MIN_USD = float(os.getenv("VOLUME_MIN_USD", "5000000"))
VOLUME_SPIKE_MULT = float(os.getenv("VOLUME_SPIKE_MULT", "3.0"))
RSI_LONG_THRESHOLD = float(os.getenv("RSI_LONG_THRESHOLD", "35"))
RSI_SHORT_THRESHOLD = float(os.getenv("RSI_SHORT_THRESHOLD", "65"))
MOMENTUM_THRESHOLD = float(os.getenv("MOMENTUM_THRESHOLD", "0.5"))
SIGNAL_EXPIRY_SEC = int(os.getenv("SIGNAL_EXPIRY_SEC", "60"))

TF = "5m"
TZ = UTC

bot = telebot.TeleBot(BOT_TOKEN, parse_mode='HTML')

symbols: List[str] = []
latest_prices: Dict[str, float] = {}
candles: Dict[str, pd.DataFrame] = {}
last_signal_time: Dict[str, datetime] = {}

logger = logging.getLogger("cryptobot")
logging.basicConfig(level=logging.INFO)

async def fetch_symbols(session):
    url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
    async with session.get(url) as r:
        res = await r.json()
        data = res.get("result", {}).get("list", [])
        valid = [x['symbol'] for x in data if x['quoteCoin'] == 'USDT' and x['status'] == 'Trading']
        return sorted(valid)

async def fetch_candles(session, symbol):
    url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval=5&limit=50"
    async with session.get(url) as r:
        res = await r.json()
        klines = res.get("result", {}).get("list", [])
        df = pd.DataFrame(klines, columns=['ts','o','h','l','c','v','t'])
        df = df.astype({'ts':'int64','o':'float','h':'float','l':'float','c':'float','v':'float'})
        df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        df.set_index('ts', inplace=True)
        return df

def compute_indicators(df):
    df['rsi'] = ta_rsi(df['c'], period=14)
    df['mom'] = df['c'].pct_change(periods=5) * 100
    df['ema'] = df['c'].ewm(span=20).mean()
    df['sr_support'] = df['l'].rolling(window=20).min()
    df['sr_resist'] = df['h'].rolling(window=20).max()
    return df

def ta_rsi(series, period=14):
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_gain = pd.Series(gain).rolling(window=period).mean()
    avg_loss = pd.Series(loss).rolling(window=period).mean()
    rs = avg_gain / (avg_loss + 1e-10)
    return 100 - (100 / (1 + rs))

def build_signal_text(symbol, direction, strength, price, rsi, mom, support, resist, confidence, ts, sl, tp, rr):
    emoji = "🟢" if direction == "LONG" else "🔴"
    lines = [
        f"{emoji} {symbol} | {direction} | {strength}",
        f"⏰ ТФ: 5m (скальп)",
        f"📍 Текущая: {price:.6f}",
        f"➡️ Вход: {price:.6f}",
        f"🛡️ Стоп-Лосс: {sl:.6f} ({abs(sl/price-1)*100:.2f}%)",
        f"🎯 Тейк-Профит: {tp:.6f} ({abs(tp/price-1)*100:.2f}%)",
        f"⚖️ Risk/Reward: {rr:.2f}",
        f"\n📊 Метрики:",
        f"• RSI(14): {rsi:.1f}",
        f"• Momentum(5): {mom:.2f}%",
        f"• Support/Resistance: {support:.6f} / {resist:.6f}",
        f"• Уверенность: {confidence}%",
        f"\n⏱️ {ts.strftime('%Y-%m-%d %H:%M:%S')} UTC"
    ]
    return "\n".join(lines)

def classify_signal(df):
    latest = df.iloc[-1]
    price = latest['c']
    rsi = latest['rsi']
    mom = latest['mom']
    support = latest['sr_support']
    resist = latest['sr_resist']
    ts = latest.name
    ema = latest['ema']

    if rsi < RSI_LONG_THRESHOLD and mom < -MOMENTUM_THRESHOLD and price < ema:
        direction = "LONG"
        strength = "слабый" if rsi > 30 else "сильный"
        sl = support
        tp = price + (price - sl) * 1.5
        rr = (tp - price) / (price - sl + 1e-6)
        return direction, strength, price, rsi, mom, support, resist, 70, ts, sl, tp, rr

    if rsi > RSI_SHORT_THRESHOLD and mom > MOMENTUM_THRESHOLD and price > ema:
        direction = "SHORT"
        strength = "слабый" if rsi < 75 else "сильный"
        sl = resist
        tp = price - (sl - price) * 1.5
        rr = (price - tp) / (sl - price + 1e-6)
        return direction, strength, price, rsi, mom, support, resist, 70, ts, sl, tp, rr

    return None

async def signal_worker():
    global symbols
    async with aiohttp.ClientSession() as session:
        while True:
            for sym in symbols:
                try:
                    df = await fetch_candles(session, sym)
                    df = compute_indicators(df)
                    signal = classify_signal(df)
                    if not signal:
                        continue
                    direction, strength, price, rsi, mom, support, resist, conf, ts, sl, tp, rr = signal
                    if (datetime.now(UTC) - ts).total_seconds() > SIGNAL_EXPIRY_SEC:
                        continue
                    if (ts == last_signal_time.get(sym)):
                        continue
                    last_signal_time[sym] = ts
                    text = build_signal_text(sym, direction, strength, price, rsi, mom, support, resist, conf, ts, sl, tp, rr)
                    for cid in CHAT_IDS:
                        if cid < 0 or not ONLY_CHANNEL:
                            bot.send_message(cid, text)
                except Exception as e:
                    logger.error(f"{sym} failed: {e}")
            await asyncio.sleep(30)

async def refresh_universe():
    global symbols
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                symbols = await fetch_symbols(session)
            except Exception as e:
                logger.error(f"Universe refresh failed: {e}")
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)

async def main():
    await asyncio.gather(
        refresh_universe(),
        signal_worker(),
    )

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down bot...")
