"""
CryptoBot — Bybit 1m Candles Test (Render-ready, JobQueue)
- Secure: whitelist chat_ids
- Health-check every 60 minutes (JobQueue)
- Bybit WS: kline.1 <SYMBOL>
- Sends a compact candle summary every 5 minutes (on confirmed candle)
"""

import os
import asyncio
import json
from typing import List, Set, Optional
from datetime import datetime, timezone, timedelta

import websockets
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ---------------- Config / Security ----------------
ALLOWED_DEFAULT: Set[int] = {533232884, -1002870952333}  # ты и твой канал
PING_INTERVAL_MIN = 60
POST_EVERY_N_MIN = 5           # отправляем сводку раз в 5 минут
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
SYMBOL = os.environ.get("BYBIT_SYMBOL", "BTCUSDT")

def _parse_id_list(value: str) -> List[int]:
    out: List[int] = []
    if not value:
        return out
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            pass
    return out

ALLOWED_CHAT_IDS: Set[int] = set(_parse_id_list(os.environ.get("ALLOWED_CHAT_IDS", ""))) or ALLOWED_DEFAULT
RECIPIENTS: List[int] = [cid for cid in _parse_id_list(os.environ.get("TELEGRAM_CHAT_ID", "")) if cid in ALLOWED_CHAT_IDS]

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

# ---------------- Handlers (whitelist) ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id not in ALLOWED_CHAT_IDS:
        return
    await update.message.reply_text("Бот онлайн ✅")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id not in ALLOWED_CHAT_IDS:
        return
    await update.message.reply_text(
        "Статус: онлайн\n"
        f"Whitelist: {sorted(ALLOWED_CHAT_IDS)}\n"
        f"Recipients: {RECIPIENTS}\n"
        f"Symbol: {SYMBOL}\n"
        f"Uptime: {utcnow().isoformat()}"
    )

async def ignore_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Полная тишина для чужих
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id in ALLOWED_CHAT_IDS:
        return

# ---------------- Bybit WS consumer ----------------
async def bybit_candles(application: Application):
    """
    Подписка на kline.1.<SYMBOL>.
    Шлём краткую сводку раз в POST_EVERY_N_MIN минут на подтверждённой свече.
    """
    last_posted_min: Optional[int] = None

    async def send_summary(candle: dict):
        o = float(candle["open"]); h = float(candle["high"]); l = float(candle["low"]); c = float(candle["close"])
        v = float(candle["volume"])
        ts = int(candle["end"])  # ms
        t = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%H:%M")
        text = f"{SYMBOL} 1m {t} UTC — O:{o:.2f} H:{h:.2f} L:{l:.2f} C:{c:.2f} V:{v:.0f}"
        for cid in RECIPIENTS:
            try:
                await application.bot.send_message(chat_id=cid, text=text, disable_notification=True)
            except Exception as e:
                print(f"[warn] bybit summary -> {cid}: {e}")

    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL, ping_interval=25, ping_timeout=20) as ws:
                sub = {"op": "subscribe", "args": [f"kline.1.{SYMBOL}"]}
                await ws.send(json.dumps(sub))
                print("[info] Subscribed to", sub["args"])

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    if msg.get("topic", "").startswith("kline."):
                        for item in msg.get("data", []):
                            if not item.get("confirm"):
                                continue  # ждём закрытия свечи
                            end_ms = int(item["end"])
                            minute = datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).minute
                            if last_posted_min is None or ((minute % POST_EVERY_N_MIN) == 0 and minute != last_posted_min):
                                last_posted_min = minute
                                await send_summary(item)
        except Exception as e:
            print("[warn] WS reconnecting due to:", e)
            await asyncio.sleep(3)

# ---------------- Jobs (JobQueue) ----------------
async def health_job(context: ContextTypes.DEFAULT_TYPE):
    for cid in RECIPIENTS:
        try:
            await context.bot.send_message(chat_id=cid, text="🟢 online", disable_notification=True)
        except Exception as e:
            print(f"[warn] health-check -> {cid}: {e}")

async def start_ws_job(context: ContextTypes.DEFAULT_TYPE):
    # Запускаем длинную фоновую корутину уже ПОСЛЕ старта приложения
    context.application.create_task(bybit_candles(context.application))

# ---------------- App lifecycle ----------------
async def post_init(application: Application):
    if not RECIPIENTS:
        raise RuntimeError("TELEGRAM_CHAT_ID пуст или не в whitelist. Проверь переменные окружения.")
    # Стартовое сообщение (короткое — допустимо в post_init)
    for cid in RECIPIENTS:
        try:
            await application.bot.send_message(chat_id=cid, text=f"✅ Render: бот запущен. Symbol={SYMBOL}")
        except Exception as e:
            print(f"[warn] startup -> {cid}: {e}")
    # Планируем задачи через JobQueue (никаких create_task до запуска!)
    application.job_queue.run_repeating(
        health_job,
        interval=timedelta(minutes=PING_INTERVAL_MIN),
        first=timedelta(minutes=1),
        name="health"
    )
    application.job_queue.run_once(
        start_ws_job,
        when=timedelta(seconds=2),
        name="start_ws"
    )

def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN.")
    app = Application.builder().token(token).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.ALL, ignore_all))
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
