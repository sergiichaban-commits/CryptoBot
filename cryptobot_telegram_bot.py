# cryptobot_telegram_bot.py
# Telegram bot: webhook + access control + health ping + self-ping (Render)
# Requirements (requirements.txt):
#   python-telegram-bot[job-queue,webhooks]==21.6
#   httpx~=0.27
#
# Env:
#   TELEGRAM_BOT_TOKEN           (required)
#   TELEGRAM_CHAT_ID             (primary recipient: channel/chat id)
#   ALLOWED_CHAT_IDS             (CSV of allowed chat ids; must include TELEGRAM_CHAT_ID)
#   RENDER_EXTERNAL_URL or PUBLIC_URL (Render sets the first automatically)
#   PORT                         (Render sets)
#   (opt) WEBHOOK_PATH           (default /wh-<token_prefix8>)
#   (opt) HEALTH_INTERVAL_SEC    (default 1200 = 20 min)
#   (opt) HEALTH_FIRST_SEC       (default 60)  <-- первый health сразу через минуту
#   (opt) STARTUP_PING_SEC       (default 10)  <-- единоразовый «бот запущен» после старта
#   (opt) SELF_PING_ENABLED      ("1"/"0", default 1)
#   (opt) SELF_PING_INTERVAL_SEC (default 780 ≈ 13 min)
#   (opt) SELF_PING_URL          (if empty -> PUBLIC_URL + SELF_PING_PATH)
#   (opt) SELF_PING_PATH         (default "/")

from __future__ import annotations
import os
import asyncio
import logging
from typing import List, Set

import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)s %(message)s"
)
log = logging.getLogger("cryptobot")

# ---------- helpers ----------
def parse_int_list(csv: str) -> List[int]:
    out = []
    for part in (csv or "").split(","):
        s = part.strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except ValueError:
            pass
    return out

def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

# ---------- env ----------
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("TELEGRAM_BOT_TOKEN is required.")

primary_chat_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
ALLOWED_CHAT_IDS: Set[int] = set(parse_int_list(os.environ.get("ALLOWED_CHAT_IDS", "")))
PRIMARY_RECIPIENTS: List[int] = []

if primary_chat_raw:
    try:
        cid = int(primary_chat_raw)
        if cid in ALLOWED_CHAT_IDS:
            PRIMARY_RECIPIENTS.append(cid)
    except ValueError:
        pass

PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL") or ""
PORT = getenv_int("PORT", getenv_int("HTTP_PORT", 10000))
token_prefix = BOT_TOKEN.split(":")[0] if ":" in BOT_TOKEN else BOT_TOKEN[:8]
WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH", f"/wh-{token_prefix[:8]}")
WEBHOOK_URL = (PUBLIC_URL.rstrip("/") + WEBHOOK_PATH) if PUBLIC_URL else ""

HEALTH_INTERVAL_SEC = getenv_int("HEALTH_INTERVAL_SEC", 1200)     # 20 мин
HEALTH_FIRST_SEC    = getenv_int("HEALTH_FIRST_SEC", 60)          # первый health через 1 мин
STARTUP_PING_SEC    = getenv_int("STARTUP_PING_SEC", 10)          # «бот запущен» через 10 сек

SELF_PING_ENABLED        = os.environ.get("SELF_PING_ENABLED", "1").lower() not in {"0","false","no"}
SELF_PING_INTERVAL_SEC   = getenv_int("SELF_PING_INTERVAL_SEC", 780)  # ~13 мин
SELF_PING_URL            = os.environ.get("SELF_PING_URL", "").strip()
SELF_PING_PATH           = os.environ.get("SELF_PING_PATH", "/")
if not SELF_PING_URL and PUBLIC_URL:
    SELF_PING_URL = PUBLIC_URL.rstrip("/") + SELF_PING_PATH

log.info("[cfg] ALLOWED_CHAT_IDS=%s", sorted(ALLOWED_CHAT_IDS))
log.info("[cfg] PRIMARY_RECIPIENTS=%s", PRIMARY_RECIPIENTS)
log.info("[cfg] PUBLIC_URL='%s' PORT=%s WEBHOOK_PATH='%s'", PUBLIC_URL, PORT, WEBHOOK_PATH)
log.info("[cfg] HEALTH_INTERVAL=%ss FIRST=%ss STARTUP_PING_SEC=%s",
         HEALTH_INTERVAL_SEC, HEALTH_FIRST_SEC, STARTUP_PING_SEC)
log.info("[cfg] SELF_PING_ENABLED=%s INTERVAL=%s URL='%s'",
         SELF_PING_ENABLED, SELF_PING_INTERVAL_SEC, SELF_PING_URL or "(disabled)")

# ---------- access control ----------
def is_allowed(update: Update) -> bool:
    cid = update.effective_chat.id if update.effective_chat else None
    return (cid in ALLOWED_CHAT_IDS) if cid is not None else False

async def guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if is_allowed(update):
        return True
    return False  # молча игнорируем чужие чаты

# ---------- commands ----------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update, context):
        return
    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text="Привет! Я онлайн. Используй /ping для проверки.")

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update, context):
        return
    await context.bot.send_message(chat_id=update.effective_chat.id, text="🟢 online")

# ---------- jobs ----------
async def job_health(context: ContextTypes.DEFAULT_TYPE) -> None:
    for chat_id in PRIMARY_RECIPIENTS:
        try:
            await context.bot.send_message(chat_id=chat_id, text="🟢 online", disable_notification=True)
        except Exception as e:
            log.warning("[health] send to %s failed: %s", chat_id, repr(e))

async def job_self_ping(context: ContextTypes.DEFAULT_TYPE) -> None:
    url = SELF_PING_URL
    if not (SELF_PING_ENABLED and url):
        return
    try:
        timeout = httpx.Timeout(8.0, connect=4.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url)
            log.debug("[self-ping] %s -> %s", url, resp.status_code)
    except Exception as e:
        log.warning("[self-ping] %s failed: %s", url, repr(e))

async def job_startup_ping(context: ContextTypes.DEFAULT_TYPE) -> None:
    for chat_id in PRIMARY_RECIPIENTS:
        try:
            await context.bot.send_message(chat_id=chat_id,
                                           text="✅ Бот запущен (webhook активен).")
        except Exception as e:
            log.warning("[startup] send to %s failed: %s", chat_id, repr(e))

# ---------- app ----------
def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("ping", cmd_ping))

    # job queue
    if app.job_queue is None:
        log.warning("JobQueue отсутствует — убедитесь, что установлен 'python-telegram-bot[job-queue]'.")
    else:
        # единоразовый стартовый пинг
        app.job_queue.run_once(job_startup_ping, when=STARTUP_PING_SEC)
        # периодический health
        app.job_queue.run_repeating(job_health, interval=HEALTH_INTERVAL_SEC, first=HEALTH_FIRST_SEC)
        # self-ping
        if SELF_PING_ENABLED and SELF_PING_URL:
            app.job_queue.run_repeating(job_self_ping, interval=SELF_PING_INTERVAL_SEC, first=60)

    return app

def main():
    app = build_application()

    if not PUBLIC_URL:
        log.warning("PUBLIC_URL не задан — запускаю polling-режим (локально).")
        app.run_polling(allowed_updates=Update.ALL_TYPES)
        return

    # обнуляем старый вебхук и очередь обновлений
    log.info("Удаляю старый webhook (если был) и сбрасываю pending updates…")
    app.bot.delete_webhook(drop_pending_updates=True)

    log.info("Стартую webhook: listen=0.0.0.0 port=%s path=%s url=%s", PORT, WEBHOOK_PATH, WEBHOOK_URL)
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=WEBHOOK_PATH,
        webhook_url=WEBHOOK_URL,
        allowed_updates=Update.ALL_TYPES,
        stop_signals=None,  # безопасно для контейнеров на Render
    )

if __name__ == "__main__":
    main()
