"""
CryptoBot — Bybit 1m Candles Test (Render free Web Service)
- Secure: whitelist chat_ids (env: ALLOWED_CHAT_IDS)
- Recipients from TELEGRAM_CHAT_ID filtered by whitelist
- Bybit WS: kline.1 <SYMBOL> (env: BYBIT_SYMBOL, default BTCUSDT)
- Sends a compact candle summary every 5 minutes (on confirmed candle)
- Health ping every 60 minutes
- Mini HTTP server binds to $PORT (/, /health) so Render Web Service stays up on free plan
- Start Command: python cryptobot_telegram_bot.py
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

ALLOWED_DEFAULT: Set[int] = {533232884, -1002870952333}  # ты и канал ChaSerBot
PING_INTERVAL_MIN = 60
POST_EVERY_N_MIN = 5
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
SYMBOL = os.environ.get("BYBIT_SYMBOL", "BTCUSDT").strip() or "BTCUSDT"

HTTP_PORT = int(os.environ.get("PORT", "8000"))  # Render присваивает $PORT для Web Service

def _parse_id_list(value: str) -> List[int]:
    """Парсер chat_id, устойчивый к пробелам/кавычкам."""
    out: List[int] = []
    if not value:
        return out
    for part in value.split(","):
        part = part.strip().strip('"').strip("'")
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            print(f"[warn] cannot parse chat id from: {repr(part)}")
    return out

ALLOWED_CHAT_IDS: Set[int] = set(_parse_id_list(os.environ.get("ALLOWED_CHAT_IDS", ""))) or ALLOWED_DEFAULT
RECIPIENTS: List[int] = [cid for cid in _parse_id_list(os.environ.get("TELEGRAM_CHAT_ID", "")) if cid in ALLOWED_CHAT_IDS]

print(f"[info] ALLOWED_CHAT_IDS = {sorted(ALLOWED_CHAT_IDS)}")
print(f"[info] TELEGRAM_CHAT_ID(raw) = {os.environ.get('TELEGRAM_CHAT_ID', '')!r}")
print(f"[info] RECIPIENTS (whitelisted) = {RECIPIENTS}")
print(f"[info] BYBIT_SYMBOL = {SYMBOL!r}")
print(f"[info] HTTP_PORT = {HTTP_PORT}")

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
    """Подписка на kline.1.<SYMBOL>, сводка раз в POST_EVERY_N_MIN минут на подтверждённой свече."""
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
            await asyncio.sleep(3)  # backoff

# ---------------- Health-check ----------------

async def health_loop(application: Application):
    if not RECIPIENTS:
        return
    while True:
        for cid in RECIPIENTS:
            try:
                await application.bot.send_message(chat_id=cid, text="🟢 online", disable_notification=True)
            except Exception as e:
                print(f"[warn] health-check -> {cid}: {e}")
        await asyncio.sleep(PING_INTERVAL_MIN * 60)

# ---------------- Tiny HTTP server (for Render Web Service) ----------------

async def run_http_server():
    # Мини-сервер на aiohttp: / и /health
    from aiohttp import web

    async def hello(request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/", hello)
    app.router.add_get("/health", hello)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=HTTP_PORT)
    await site.start()
    print(f"[info] HTTP server listening on 0.0.0.0:{HTTP_PORT}")

# ---------------- App lifecycle ----------------

async def post_init(application: Application):
    # Стартовое сообщение (если есть кому слать)
    for cid in RECIPIENTS:
        try:
            await application.bot.send_message(chat_id=cid, text=f"✅ Render Web Service: бот запущен. Symbol={SYMBOL}")
        except Exception as e:
            print(f"[warn] startup -> {cid}: {e}")

    # Запускаем фоновые задачи (без JobQueue, чистый asyncio)
    asyncio.create_task(health_loop(application))
    asyncio.create_task(bybit_candles(application))

async def run_all():
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN env var.")

    # 1) Telegram App
    tg_app = Application.builder().token(token).post_init(post_init).build()
    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("status", status))
    tg_app.add_handler(MessageHandler(filters.ALL, ignore_all))

    # Запускаем polling как задачу (не блокируем цикл)
    tg_task = asyncio.create_task(tg_app.run_polling(allowed_updates=Update.ALL_TYPES))

    # 2) HTTP health server for Render Web Service
    await run_http_server()

    # 3) Ждём завершения Telegram задачи
    await tg_task

def main():
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
