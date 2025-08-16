# cryptobot_telegram_bot.py
# Telegram bot: webhook + health ping + self-ping for Render free tier
# Requirements (requirements.txt):
#   python-telegram-bot[job-queue,webhooks]==21.6
#   httpx~=0.27
# Env:
#   TELEGRAM_BOT_TOKEN           - обязательно
#   TELEGRAM_CHAT_ID             - главный получатель (канал/чат)
#   ALLOWED_CHAT_IDS             - CSV: список разрешённых chat_id
#   RENDER_EXTERNAL_URL / PUBLIC_URL (Render сам проставляет первое)
#   PORT                         - Render задаёт порт для вебсервиса
#   (опц.) WEBHOOK_PATH          - путь вебхука, по умолчанию /wh-<token_prefix>
#   (опц.) HEALTH_INTERVAL_SEC   - интервал "🟢 online" (по умолчанию 1200 = 20 мин)
#   (опц.) SELF_PING_ENABLED     - "1"/"0" (по умолчанию 1)
#   (опц.) SELF_PING_INTERVAL_SEC- интервал self-ping (по умолчанию 780 ≈ 13 мин)
#   (опц.) SELF_PING_URL         - если нужно пинговать конкретный URL
#   (опц.) SELF_PING_PATH        - если SELF_PING_URL не задан, используем PUBLIC_URL + PATH (по умолчанию "/")
#
# Команды:
#   /start  /ping

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

# -------------------- Config helpers --------------------

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

# -------------------- Env --------------------

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("TELEGRAM_BOT_TOKEN is required.")

primary_chat_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
ALLOWED_CHAT_IDS: Set[int] = set(parse_int_list(os.environ.get("ALLOWED_CHAT_IDS", "")))
PRIMARY_RECIPIENTS: List[int] = []

if primary_chat_raw:
    try:
        cid = int(primary_chat_raw)
        # в список получателей добавляем только те, кто есть в whitelist
        if cid in ALLOWED_CHAT_IDS:
            PRIMARY_RECIPIENTS.append(cid)
    except ValueError:
        pass

PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL") or ""
PORT = getenv_int("PORT", getenv_int("HTTP_PORT", 10000))
token_prefix = BOT_TOKEN.split(":")[0] if ":" in BOT_TOKEN else BOT_TOKEN[:8]
WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH", f"/wh-{token_prefix[:8]}")
WEBHOOK_URL = (PUBLIC_URL.rstrip("/") + WEBHOOK_PATH) if PUBLIC_URL else ""

HEALTH_INTERVAL_SEC = getenv_int("HEALTH_INTERVAL_SEC", 1200)  # 20 минут
SELF_PING_ENABLED = os.environ.get("SELF_PING_ENABLED", "1").lower() not in {"0", "false", "no"}
SELF_PING_INTERVAL_SEC = getenv_int("SELF_PING_INTERVAL_SEC", 780)  # ~13 минут
SELF_PING_URL = os.environ.get("SELF_PING_URL", "").strip()
SELF_PING_PATH = os.environ.get("SELF_PING_PATH", "/")

if not SELF_PING_URL and PUBLIC_URL:
    SELF_PING_URL = PUBLIC_URL.rstrip("/") + SELF_PING_PATH

log.info("[cfg] ALLOWED_CHAT_IDS=%s", sorted(ALLOWED_CHAT_IDS))
log.info("[cfg] PRIMARY_RECIPIENTS=%s", PRIMARY_RECIPIENTS)
log.info("[cfg] PUBLIC_URL='%s' PORT=%s WEBHOOK_PATH='%s'", PUBLIC_URL, PORT, WEBHOOK_PATH)
log.info("[cfg] HEALTH_INTERVAL_SEC=%s", HEALTH_INTERVAL_SEC)
log.info("[cfg] SELF_PING_ENABLED=%s SELF_PING_INTERVAL_SEC=%s SELF_PING_URL='%s'",
         SELF_PING_ENABLED, SELF_PING_INTERVAL_SEC, SELF_PING_URL or "(disabled)")

# -------------------- Access control --------------------

def is_allowed(update: Update) -> bool:
    """Разрешаем команды только из whitelisted чатов."""
    cid = None
    if update.effective_chat:
        cid = update.effective_chat.id
    return (cid in ALLOWED_CHAT_IDS) if cid is not None else False

async def guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if is_allowed(update):
        return True
    # Молча игнорируем чужаков
    return False

# -------------------- Handlers --------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update, context):
        return
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Привет! Я онлайн. Используй /ping для проверки.")

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update, context):
        return
    await context.bot.send_message(chat_id=update.effective_chat.id, text="🟢 online")

# -------------------- Jobs (JobQueue) --------------------

async def job_health(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Периодический 'я жив' — отправляем только whitelisted получателям."""
    for chat_id in PRIMARY_RECIPIENTS:
        try:
            await context.bot.send_message(chat_id=chat_id, text="🟢 online", disable_notification=True)
        except Exception as e:
            log.warning("[health] send to %s failed: %s", chat_id, repr(e))

async def job_self_ping(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Self-ping: дергаем свой URL, чтобы Render считал сервис активным."""
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

# -------------------- App bootstrap --------------------

def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("ping", cmd_ping))

    # Планировщик задач
    if app.job_queue is None:
        log.warning("JobQueue отсутствует — убедитесь, что установлен пакет 'python-telegram-bot[job-queue]'.")
    else:
        # Health every HEALTH_INTERVAL_SEC, стартуем через ту же паузу
        app.job_queue.run_repeating(job_health, interval=HEALTH_INTERVAL_SEC, first=HEALTH_INTERVAL_SEC)
        # Self-ping каждые ~13 минут (или из ENV), старт — через 60 сек после запуска,
        # чтобы успел подняться вебсервер/вебхук.
        if SELF_PING_ENABLED and SELF_PING_URL:
            app.job_queue.run_repeating(job_self_ping, interval=SELF_PING_INTERVAL_SEC, first=60)

    return app

def main():
    app = build_application()

    if not PUBLIC_URL:
        # Fallback на polling (локальный запуск без Render), но лучше на сервере всегда WEBHOOK.
        log.warning("PUBLIC_URL не задан — запускаю polling-режим (для локальных тестов).")
        app.run_polling(allowed_updates=Update.ALL_TYPES)
        return

    # Чистим старый вебхук (если был), чтобы избежать конфликтов.
    # Drop pending updates — так не подтянутся старые очереди.
    log.info("Удаляю старый webhook (если был) и сбрасываю pending updates…")
    # NB: в run_webhook можно указать webhook_url, PTB сам поставит хук. Но явная очистка — полезна.
    app.bot.delete_webhook(drop_pending_updates=True)

    # Webhook-сервер PTB (tornado) слушает порт и обрабатывает пути.
    # Любые GET к корню ("/") вернут 404, но это достаточно для self-ping,
    # а POST на WEBHOOK_PATH принимает апдейты от Telegram.
    log.info("Стартую webhook-сервер: listen=0.0.0.0 port=%s path=%s url=%s", PORT, WEBHOOK_PATH, WEBHOOK_URL)
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=WEBHOOK_PATH,
        webhook_url=WEBHOOK_URL,           # Telegram будет слать POST сюда
        allowed_updates=Update.ALL_TYPES,
        stop_signals=None,                 # не трогаем системные сигналы в контейнерах
    )

if __name__ == "__main__":
    main()
