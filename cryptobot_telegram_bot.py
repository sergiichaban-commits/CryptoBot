# cryptobot_telegram_bot.py
from __future__ import annotations

import os
import json
import asyncio
from datetime import datetime, timezone
from typing import List, Optional

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

# ================= ENV & CONSTANTS =================

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

allowed_from_env = parse_int_list(os.environ.get("ALLOWED_CHAT_IDS"))
fallback_chat = parse_int_list(os.environ.get("TELEGRAM_CHAT_ID"))  # single or CSV
ALLOWED_CHAT_IDS: list[int] = allowed_from_env or fallback_chat
if not ALLOWED_CHAT_IDS:
    raise SystemExit("Set ALLOWED_CHAT_IDS or TELEGRAM_CHAT_ID")

# получатели ежеминутных/ежечасных сообщений
RECIPIENTS: list[int] = fallback_chat or ALLOWED_CHAT_IDS

# MULTI-SYMBOL: берём из ENV SYMBOLS или fallback BYBIT_SYMBOL
ENV_SYMBOLS = [s.strip().upper() for s in (os.environ.get("SYMBOLS") or "").split(",") if s.strip()]
BYBIT_SYMBOL_FALLBACK = os.environ.get("BYBIT_SYMBOL", "BTCUSDT").upper()
CONFIG_PATH = os.environ.get("CONFIG_PATH", "bot_config.json")

PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL")
HTTP_PORT = int(os.environ.get("PORT", "10000"))
tok_left = BOT_TOKEN.split(":")[0] if ":" in BOT_TOKEN else BOT_TOKEN
WEBHOOK_PATH = f"/wh-{tok_left[-8:]}"

# Schedules
HEALTH_INTERVAL_SEC = 60 * 60  # hourly
SNAPSHOT_INTERVAL_SEC = 60     # minutely

# ================= LOG HEADERS =================
print(f"[info] ALLOWED_CHAT_IDS = {sorted(ALLOWED_CHAT_IDS)}")
print(f"[info] TELEGRAM_CHAT_ID(raw) = '{os.environ.get('TELEGRAM_CHAT_ID', '')}'")
print(f"[info] RECIPIENTS (whitelisted) = {sorted(RECIPIENTS)}")
print(f"[info] HTTP_PORT = {HTTP_PORT}")
if PUBLIC_URL:
    print(f"[info] PUBLIC_URL = '{PUBLIC_URL}'")
print(f"[info] WEBHOOK_PATH = '{WEBHOOK_PATH}'")

# ================= SHARED STATE =================

class State:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.symbols: List[str] = []

    async def load(self):
        # приоритет: ENV SYMBOLS -> config.json -> fallback
        symbols = list(ENV_SYMBOLS)
        if not symbols:
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        cfg_syms = data.get("symbols")
                        if isinstance(cfg_syms, list):
                            symbols = [str(x).upper() for x in cfg_syms if str(x).strip()]
            except Exception:
                pass
        if not symbols:
            symbols = [BYBIT_SYMBOL_FALLBACK]
        async with self._lock:
            self.symbols = symbols

    async def set_symbols(self, new_symbols: List[str]):
        async with self._lock:
            self.symbols = new_symbols
            try:
                with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump({"symbols": self.symbols}, f, ensure_ascii=False, indent=2)
            except Exception:
                pass

    async def get_symbols(self) -> List[str]:
        async with self._lock:
            return list(self.symbols)

STATE = State()

# ================= BYBIT HELPERS =================

BYBIT_REST_BASE = "https://api.bybit.com"

async def bybit_get(session: aiohttp.ClientSession, path: str, params: dict) -> Optional[dict]:
    url = f"{BYBIT_REST_BASE}{path}"
    try:
        async with session.get(url, params=params) as r:
            if r.status != 200:
                return None
            data = await r.json()
            return data
    except Exception:
        return None

async def validate_symbols_linear(symbols: List[str]) -> tuple[List[str], List[str]]:
    """Проверяем, что символ существует в linear category на Bybit."""
    ok, bad = [], []
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        for sym in symbols:
            data = await bybit_get(s, "/v5/market/instruments-info", {"category": "linear", "symbol": sym})
            if data and data.get("retCode") == 0:
                items = (data.get("result") or {}).get("list") or []
                if items:
                    ok.append(sym)
                else:
                    bad.append(sym)
            else:
                bad.append(sym)
    return ok, bad

async def fetch_bybit_1m(symbol: str) -> Optional[dict]:
    """Берём последний 1m бар через REST."""
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        data = await bybit_get(s, "/v5/market/kline", {"category":"linear", "symbol":symbol, "interval":"1", "limit":"1"})
    if not data or data.get("retCode") != 0:
        return None
    items = (data.get("result") or {}).get("list") or []
    if not items:
        return None
    start, o, h, l, c, v, _turnover = items[0]
    return {"t": int(start), "o": float(o), "h": float(h), "l": float(l), "c": float(c), "v": float(v)}

def fmt_price(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.6f}"

# ================= WHITELIST & REPLIES =================

def is_allowed(chat_id: int | None) -> bool:
    return chat_id is not None and chat_id in ALLOWED_CHAT_IDS

async def safe_reply(update: Update, text: str, **kwargs):
    chat = update.effective_chat
    if not chat or not is_allowed(chat.id):
        return
    await update.effective_message.reply_text(text, **kwargs)

# ================= COMMANDS =================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    await safe_reply(update,
        "Привет! Я ChaSerBot.\n"
        "Команды:\n"
        "• /ping — проверить, что бот жив\n"
        "• /about — сведения о сервисе\n"
        "• /status — активные символы\n"
        "• /symbol <SYMBOL> — один символ (например, /symbol BTCUSDT)\n"
        "• /symbols — показать текущие\n"
        "• /symbols <S1,S2,...> — установить список (например, /symbols BTCUSDT,ETHUSDT,SOLUSDT)\n",
    )

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    await safe_reply(update, "pong")

async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    syms = await STATE.get_symbols()
    await safe_reply(update, f"ChaSerBot (webhook)\nSymbols: {', '.join(syms)}\nWhitelisted: {', '.join(map(str, ALLOWED_CHAT_IDS))}")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    syms = await STATE.get_symbols()
    await safe_reply(update, f"Активные символы: {', '.join(syms)}")

def _normalize_symbols_arg(args: List[str]) -> List[str]:
    joined = " ".join(args).replace(";", ",")
    parts = [p.strip().upper() for p in joined.split(",") if p.strip()]
    # убираем дубли, сохраняя порядок
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

async def cmd_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    if not context.args:
        await safe_reply(update, "Формат: /symbol BTCUSDT")
        return
    sym = context.args[0].strip().upper()
    ok, bad = await validate_symbols_linear([sym])
    if bad:
        await safe_reply(update, f"Символ не найден на Bybit (linear): {bad[0]}")
        return
    await STATE.set_symbols(ok)
    await safe_reply(update, f"Готово. Активный символ: {ok[0]}")

async def cmd_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    if not context.args:
        syms = await STATE.get_symbols()
        await safe_reply(update, f"Текущие символы: {', '.join(syms)}\n"
                                 f"Чтобы установить: /symbols BTCUSDT,ETHUSDT,SOLUSDT")
        return
    wanted = _normalize_symbols_arg(context.args)
    if not wanted:
        await safe_reply(update, "Не распознал список. Пример: /symbols BTCUSDT,ETHUSDT,SOLUSDT")
        return
    ok, bad = await validate_symbols_linear(wanted)
    if bad:
        await safe_reply(update, f"Не найдены на Bybit (linear): {', '.join(bad)}\n"
                                 f"Принято: {', '.join(ok) if ok else '(ничего)'}")
    if ok:
        await STATE.set_symbols(ok)
        await safe_reply(update, f"Новые символы: {', '.join(ok)}")

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_chat.id):
        return
    await safe_reply(update, "Неизвестная команда. Попробуй /ping, /about, /status, /symbol, /symbols.")

# ================= PERIODIC JOBS =================

async def job_health(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    for chat_id in RECIPIENTS:
        if is_allowed(chat_id):
            try:
                await app.bot.send_message(chat_id=chat_id, text="🟢 online")
            except Exception:
                pass

async def job_snapshots(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    syms = await STATE.get_symbols()
    # собираем сводку одним сообщением
    lines = []
    for sym in syms:
        snap = await fetch_bybit_1m(sym)
        if not snap:
            continue
        t_utc = datetime.fromtimestamp(snap["t"]/1000, tz=timezone.utc).strftime("%H:%M UTC")
        line = (f"{sym} 1m {t_utc} — "
                f"O:{fmt_price(snap['o'])} H:{fmt_price(snap['h'])} "
                f"L:{fmt_price(snap['l'])} C:{fmt_price(snap['c'])} V:{int(snap['v'])}")
        lines.append(line)
    if not lines:
        return
    text = "\n".join(lines)
    for chat_id in RECIPIENTS:
        if is_allowed(chat_id):
            try:
                await app.bot.send_message(chat_id=chat_id, text=text)
            except Exception:
                pass

# ================= APP BOOTSTRAP =================

def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # Команды в приватах, группах и каналах
    common_chats = filters.ChatType.PRIVATE | filters.ChatType.GROUPS | filters.ChatType.CHANNEL
    app.add_handler(CommandHandler("start", cmd_start, filters=common_chats))
    app.add_handler(CommandHandler("ping", cmd_ping, filters=common_chats))
    app.add_handler(CommandHandler("about", cmd_about, filters=common_chats))
    app.add_handler(CommandHandler("status", cmd_status, filters=common_chats))
    app.add_handler(CommandHandler("symbol", cmd_symbol, filters=common_chats))
    app.add_handler(CommandHandler("symbols", cmd_symbols, filters=common_chats))
    app.add_handler(MessageHandler(filters.COMMAND & common_chats, unknown_command))

    # JobQueue (требует установку extras: job-queue)
    jq = app.job_queue
    jq.run_repeating(job_health, interval=HEALTH_INTERVAL_SEC, first=10)
    jq.run_repeating(job_snapshots, interval=SNAPSHOT_INTERVAL_SEC, first=15)

    return app

async def _async_main(app: Application):
    await STATE.load()
    syms = await STATE.get_symbols()
    print(f"[info] Symbols at start: {', '.join(syms)}")

    if PUBLIC_URL:
        webhook_url = f"{(PUBLIC_URL or '').rstrip('/')}{WEBHOOK_PATH}"
        print(f"[info] Starting in WEBHOOK mode at: {webhook_url}")
        await app.bot.delete_webhook(drop_pending_updates=True)
        # run_webhook (blocking) из sync-оболочки ниже
        return

def main():
    app = build_application()
    # подгружаем стартовые символы
    asyncio.get_event_loop().run_until_complete(_async_main(app))

    # режим WEBHOOK (Render Web Service) — если PUBLIC_URL задан
    if PUBLIC_URL:
        webhook_url = f"{PUBLIC_URL.rstrip('/')}{WEBHOOK_PATH}"
        app.run_webhook(
            listen="0.0.0.0",
            port=HTTP_PORT,
            url_path=WEBHOOK_PATH.lstrip("/"),
            webhook_url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
            stop_signals=None,  # не трогаем системные сигналы в контейнере
        )
    else:
        # режим POLLING (локально)
        app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
