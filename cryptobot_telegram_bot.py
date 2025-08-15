import os
import asyncio
from typing import List, Set
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ---------- Config ----------
TELEGRAM_BOT_TOKEN = "7858710318:AAGSao5jFgXyVYE2VyqvxqEHUDP7O90dq68"
DEFAULT_ALLOWED_CHAT_IDS: Set[int] = {533232884, -1002870952333}
ALLOWED_CHAT_IDS: Set[int] = DEFAULT_ALLOWED_CHAT_IDS
TELEGRAM_CHAT_IDS: Set[int] = {-1002870952333}  # куда бот пишет на старте и пинги
PING_INTERVAL_MIN = 60  # каждые 60 минут


# ---------- Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id not in ALLOWED_CHAT_IDS:
        return
    await update.message.reply_text("Бот онлайн ✅ (тест)")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id not in ALLOWED_CHAT_IDS:
        return
    await update.message.reply_text(
        "Статус: онлайн\n"
        f"Whitelist: {sorted(ALLOWED_CHAT_IDS)}"
    )

async def ignore_everything(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id in ALLOWED_CHAT_IDS:
        return


# ---------- Health Check ----------
async def health_check(application: Application):
    while True:
        for cid in TELEGRAM_CHAT_IDS:
            try:
                await application.bot.send_message(
                    chat_id=cid,
                    text="🟢 online",
                    disable_notification=True  # без звука
                )
            except Exception as e:
                print(f"[warn] Failed to send health-check to {cid}: {e}")
        await asyncio.sleep(PING_INTERVAL_MIN * 60)


# ---------- App lifecycle ----------
async def post_init(application: Application):
    for cid in TELEGRAM_CHAT_IDS:
        try:
            await application.bot.send_message(
                chat_id=cid,
                text="✅ Тест: бот запущен и может писать в этот чат/канал."
            )
        except Exception as e:
            print(f"[warn] Failed to send startup message to {cid}: {e}")

    # Запускаем health-check в фоне
    application.create_task(health_check(application))


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN is empty!")

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(MessageHandler(filters.ALL, ignore_everything))
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
