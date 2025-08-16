# cryptobot_telegram_bot.py
from __future__ import annotations

import os
import asyncio
from datetime import datetime, timezone

import aiohttp
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ============ ENV & WHITELIST ============

def parse_int_list(s: str | None) -> list[int]:
    out: list[int] = []
    if not s:
        return out
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            pass
    return out

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN")

# ALLOWED_CHAT_IDS — кто может взаимодействовать с ботом
allowed_from_env = parse_int_list(os.environ.get("ALLOWED_CHAT_IDS"))
fallback_chat = parse_int_list(os.environ.get("TELEGRAM_CHAT_ID"))
ALLOWED_CHAT_IDS: list[int] = allowed_from_env or fallback_chat
if not ALLOWED_CHAT_IDS:
    raise SystemExit("Set ALLOWED_CHAT_IDS and/or TELEGRAM_CHAT_ID")

# получатели, куда бот сам инициирует сообщения (health, сводки и т.п.)
RECIPIENTS: list[int] = fallback_chat or ALLOWED_CHAT_IDS

BYBIT_SYMBOL = os.environ.get("BYBIT_SYMBOL", "BTCUSDT").upper()

# Webhook hosting (Render)
PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL")
HTTP_PORT = int(os.environ.get("PORT", "10000"))
# Делаем короткий path без раскрытия полного токена
tok_left = BOT_TOKEN.split(":")[0] if ":" in BOT_TOKEN else BOT_TOKEN
WEBHOOK_PATH = f"/wh-{tok_left[-8:]}"

print(f"[info] ALLOWED_CHAT_IDS = {sorted(ALLOWED_CHAT_IDS)}")
print(f"[info] TELEGRAM_CHAT_ID(raw) = '{os.environ.get('TELEGRAM_CHAT_ID', '')}'")
print(f"[info] RECIPIENTS (whitelisted) = {sorted(RECIPIENTS)}")
print(f"[info] BYBIT_SYMBOL = '{BYBIT_SYMBOL}'")
print(f"[info] HTTP_PORT = {HTTP_PORT}")
if PUBLIC_URL:
    print(f"[info] PUBLIC_URL = '{PUBLIC_URL}'")
print(f"[info] WEBHOOK_PATH = '{WEBHOOK_PATH}'")

# ======== SIMPLE GUARDS & UTILS =========

def is_allowed(chat_id: int | None) -> bool:
    return chat_id is not None and chat_id in ALLOWED_CHAT_IDS

async def safe_reply(update: Update, text: str, **kwargs):
    """Отвечает в том же чате, где пришла команда, если чат в whitelist."""
    chat = update.effective_chat
    if not chat or not is_allowed(chat.id):
        return
    await update.effective_message.reply_text(text, **kwargs)

# ============ COMMAND HANDLERS ===========

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    await safe_reply(
        update,
        "Привет! Я ChaSerBot.\n"
        "Команды:\n"
        "• /ping — проверить, что бот жив\n"
        "• /about — сведения о сервисе\n",
    )

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    await safe_reply(update, "pong")

async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    await safe_reply(
        update,
        f"ChaSerBot (webhook)\n"
        f"Symbol: {BYBIT_SYMBOL}\n"
        f"Whitelisted: {', '.join(map(str, ALLOWED_CHAT_IDS))}\n",
    )

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Мягко игнорим чужие чаты и любые неизвестные команды
    if not is_allowed(update.effective_chat.id):
        return
    await safe_reply(update, "Неизвестная команда. Попробуй /ping или /about.")

# ============ BYBIT 1m SNAPSHOT ==========

BYBIT_REST_BASE = "https://api.bybit.com"

async def fetch_bybit_1m(symbol: str) -> dict | None:
    """Берём последний 1m бар через REST."""
    params = {"category": "linear", "symbol": symbol, "interval": "1", "limit": "1"}
    url = f"{BYBIT_REST_BASE}/v5/market/kline"
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(url, params=params) as r:
            if r.status != 200:
                return None
            data = await r.json()
    if data.get("retCode") != 0:
        return None
    items = data.get("result", {}).get("list") or []
    if not items:
        return None
    # Bybit: newest first; берём 0-й
    start, o, h, l, c, v, _turnover = items[0]
    return {
        "t": int(start),
        "o": float(o),
        "h": float(h),
        "l": float(l),
        "c": float(c),
        "v": float(v),
    }

def fmt_price(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.6f}"

async def send_last_candle(app: Application):
    snap = await fetch_bybit_1m(BYBIT_SYMBOL)
    if not snap:
        return
    t_utc = datetime.fromtimestamp(snap["t"] / 1000, tz=timezone.utc).strftime("%H:%M UTC")
    text = (
        f"{BYBIT_SYMBOL} 1m {t_utc} — "
        f"O:{fmt_price(snap['o'])} H:{fmt_price(snap['h'])} "
        f"L:{fmt_price(snap['l'])} C:{fmt_price(snap['c'])} V:{int(snap['v'])}"
    )
    for chat_id in RECIPIENTS:
        if is_allowed(chat_id):
            try:
                await app.bot.send_message(chat_id=chat_id, text=text)
            except Exception:
                pass

# ============ HEALTH CHECK ===============

async def send_health(app: Application):
    for chat_id in RECIPIENTS:
        if is_allowed(chat_id):
            try:
                await app.bot.send_message(chat_id=chat_id, text="🟢 online")
            except Exception:
                pass

# ============ BOOTSTRAP & WEBHOOK =========

def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # Команды — работают в PRIVATES, GROUPS и CHANNELS
    common_chats = filters.ChatType.PRIVATE | filters.ChatType.GROUPS | filters.ChatType.CHANNEL
    app.add_handler(CommandHandler("start", cmd_start, filters=common_chats))
    app.add_handler(CommandHandler("ping", cmd_ping, filters=common_chats))
    app.add_handler(CommandHandler("about", cmd_about, filters=common_chats))
    # ловим неизвестные команды
    app.add_handler(MessageHandler(filters.COMMAND & common_chats, unknown_command))

    # JobQueue доступен по умолчанию (extras: job-queue)
    jq = app.job_queue
    # health — раз в 60 минут, первая через 10 сек
    jq.run_repeating(lambda ctx: send_health(app), interval=60 * 60, first=10)
    # bybit snapshot — каждую минуту, первая через 15 сек
    jq.run_repeating(lambda ctx: send_last_candle(app), interval=60, first=15)

    return app

def main():
    app = build_application()

    # Если PUBLIC_URL задан — запускаем webhook (Render Web Service)
    if PUBLIC_URL:
        webhook_url = f"{PUBLIC_URL.rstrip('/')}{WEBHOOK_PATH}"
        print(f"[info] Volume trigger params: VOL_MULT=2.0, VOL_SMA_PERIOD=20, BODY_ATR_MULT=0.6, ATR_PERIOD=14, COOLDOWN=600s")
        print(f"[info] Setting webhook to: {webhook_url}")

        # В PTB 21.6 run_webhook сам выставит вебхук и забиндит порт
        app.run_webhook(
            listen="0.0.0.0",
            port=HTTP_PORT,
            url_path=WEBHOOK_PATH.lstrip("/"),
            webhook_url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
            stop_signals=None,  # не трогаем сигналы в контейнере Render
        )
    else:
        # Фолбэк — локальный запуск (polling)
        app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
