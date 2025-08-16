#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import aiohttp
import httpx
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,  # на будущее
    filters,
)

# =========================
# ЛОГИРОВАНИЕ
# =========================

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("cryptobot")


# =========================
# УТИЛИТЫ
# =========================

def _parse_int_list(val: str) -> List[int]:
    arr: List[int] = []
    if not val:
        return arr
    for p in val.replace(" ", "").split(","):
        if not p:
            continue
        try:
            arr.append(int(p))
        except Exception:
            pass
    return arr


# =========================
# КОНФИГ
# =========================

@dataclass(frozen=True)
class Cfg:
    token: str
    public_url: str
    port: int
    webhook_path: str

    allowed_chat_ids: List[int]
    primary_recipients: List[int]

    # расписания/хелсы
    health_sec: int = 20 * 60         # каждые 20 минут
    health_first_sec: int = 60        # первое сообщение через 60с
    startup_delay_sec: int = 10       # задержка стартовых джобов
    self_ping_enable: bool = True
    self_ping_sec: int = 13 * 60      # раз в 13 минут
    self_ping_timeout: float = 10.0

    # рынок/фильтры
    signal_cooldown_sec: int = 600
    signal_ttl_min: int = 12
    universe_mode: str = "all"        # all / top
    universe_top_n: int = 15
    ws_symbols_max: int = 60
    rotate_min: int = 5               # каждые 5 минут ротация набора
    prob_min: float = 69.9
    profit_min_pct: float = 1.0
    rr_min: float = 2.0

    # триггеры/параметры
    vol_mult: float = 2.0
    vol_sma_period: int = 20
    body_atr_mult: float = 0.6
    atr_period: int = 14


def load_cfg() -> Cfg:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    public_url = (os.environ.get("PUBLIC_URL") or "").strip()
    port = int(os.environ.get("PORT", "10000"))
    webhook_path = os.environ.get("WEBHOOK_PATH") or f"/wh-{random.randint(10_000_000, 99_999_999)}"

    allowed = _parse_int_list(os.environ.get("ALLOWED_CHAT_IDS", "") or "")

    primary = []
    primary_raw = os.environ.get("TELEGRAM_CHAT_ID", "") or ""
    if primary_raw:
        try:
            primary.append(int(primary_raw))
        except Exception:
            pass
    if primary and primary[0] not in allowed:
        allowed = allowed + primary

    cfg = Cfg(
        token=token,
        public_url=public_url,
        port=port,
        webhook_path=webhook_path,
        allowed_chat_ids=allowed or [],
        primary_recipients=primary or [],

        health_sec=int(os.environ.get("HEALTH_SEC", "1200") or 1200),
        health_first_sec=int(os.environ.get("HEALTH_FIRST_SEC", "60") or 60),
        startup_delay_sec=int(os.environ.get("STARTUP_DELAY_SEC", "10") or 10),
        self_ping_enable=(os.environ.get("SELF_PING", "true").lower() in ("1", "true", "yes")),
        self_ping_sec=int(os.environ.get("SELF_PING_SEC", "780") or 780),

        signal_cooldown_sec=int(os.environ.get("SIGNAL_COOLDOWN_SEC", "600") or 600),
        signal_ttl_min=int(os.environ.get("SIGNAL_TTL_MIN", "12") or 12),

        universe_mode=os.environ.get("UNIVERSE_MODE", "all"),
        universe_top_n=int(os.environ.get("UNIVERSE_TOP_N", "15") or 15),
        ws_symbols_max=int(os.environ.get("WS_SYMBOLS_MAX", "60") or 60),
        rotate_min=int(os.environ.get("ROTATE_MIN", "5") or 5),

        prob_min=float(os.environ.get("PROB_MIN", "69.9") or 69.9),
        profit_min_pct=float(os.environ.get("PROFIT_MIN_PCT", "1.0") or 1.0),
        rr_min=float(os.environ.get("RR_MIN", "2.0") or 2.0),

        vol_mult=float(os.environ.get("VOL_MULT", "2.0") or 2.0),
        vol_sma_period=int(os.environ.get("VOL_SMA_PERIOD", "20") or 20),
        body_atr_mult=float(os.environ.get("BODY_ATR_MULT", "0.6") or 0.6),
        atr_period=int(os.environ.get("ATR_PERIOD", "14") or 14),
    )

    logger.info("INFO [cfg] ALLOWED_CHAT_IDS=%s", cfg.allowed_chat_ids)
    logger.info("INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.primary_recipients)
    logger.info("INFO [cfg] PUBLIC_URL='%s' PORT=%s WEBHOOK_PATH='%s'", cfg.public_url, cfg.port, cfg.webhook_path)
    logger.info(
        "INFO [cfg] HEALTH=%ss FIRST=%ss STARTUP=%ss SELF_PING=%s/%ss",
        cfg.health_sec, cfg.health_first_sec, cfg.startup_delay_sec,
        "True" if cfg.self_ping_enable else "False", cfg.self_ping_sec,
    )
    logger.info(
        "INFO [cfg] SIGNAL_COOLDOWN_SEC=%s SIGNAL_TTL_MIN=%s UNIVERSE_MODE=%s UNIVERSE_TOP_N=%s "
        "WS_SYMBOLS_MAX=%s ROTATE_MIN=%s PROB_MIN>%.1f PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
        cfg.signal_cooldown_sec, cfg.signal_ttl_min, cfg.universe_mode, cfg.universe_top_n,
        cfg.ws_symbols_max, cfg.rotate_min, cfg.prob_min, cfg.profit_min_pct, cfg.rr_min,
    )
    logger.info(
        "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
        cfg.vol_mult, cfg.vol_sma_period, cfg.body_atr_mult, cfg.atr_period,
    )
    return cfg


# =========================
# BYBIT CLIENT (минимум)
# =========================

class BybitClient:
    BASE = "https://api.bybit.com"

    def __init__(self, session: aiohttp.ClientSession):
        self.sess = session

    async def get_linear_instruments(self) -> List[Dict]:
        url = f"{self.BASE}/v5/market/instruments-info"
        params = {"category": "linear", "limit": 1000}
        try:
            async with self.sess.get(url, params=params, timeout=15) as r:
                data = await r.json()
                if data.get("retCode") == 0:
                    return data.get("result", {}).get("list", []) or []
                logger.warning("Bybit instruments retCode=%s retMsg=%s", data.get("retCode"), data.get("retMsg"))
        except Exception as e:
            logger.warning("Bybit instruments error: %s", e)
        return []

    async def get_open_interest(self, symbol: str, interval: str = "5min", limit: int = 4) -> Optional[List[Dict]]:
        url = f"{self.BASE}/v5/market/open-interest"
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        try:
            async with self.sess.get(url, params=params, timeout=15) as r:
                data = await r.json()
                if data.get("retCode") == 0:
                    return data.get("result", {}).get("list", [])
                logger.warning("Bybit OI for %s retCode=%s retMsg=%s", symbol, data.get("retCode"), data.get("retMsg"))
        except Exception as e:
            logger.warning("Bybit OI error for %s: %s", symbol, e)
        return None


# =========================
# ДВИЖОК РЫНКА (каркас)
# =========================

class MarketEngine:
    def __init__(self, cfg: Cfg, bot_send):
        self.cfg = cfg
        self.bot_send = bot_send  # async def(chat_id, text)
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.client: Optional[BybitClient] = None

        self.total_symbols: int = 0
        self.active_symbols: List[str] = []
        self._all_symbols: List[str] = []

        self.ws_started: bool = False
        self.ws_topics: int = 0

    async def bootstrap(self):
        if not self.http_session:
            self.http_session = aiohttp.ClientSession()
        self.client = BybitClient(self.http_session)

        instruments = await self.client.get_linear_instruments()
        symbols = []
        for it in instruments:
            if it.get("status") == "Trading":
                sym = it.get("symbol")
                if sym and sym.endswith("USDT"):
                    symbols.append(sym)

        self._all_symbols = sorted(set(symbols))
        self.total_symbols = len(self._all_symbols)

        if self.cfg.universe_mode == "top":
            self.active_symbols = self._all_symbols[: self.cfg.universe_top_n]
        else:
            self.active_symbols = self._all_symbols[: self.cfg.ws_symbols_max]

        logger.info("INFO [universe] total=%d active=%d mode=%s",
                    self.total_symbols, len(self.active_symbols), self.cfg.universe_mode)

    async def rotate_active(self):
        if not self._all_symbols:
            return
        if self.cfg.universe_mode == "top":
            self.active_symbols = self._all_symbols[: self.cfg.universe_top_n]
        else:
            batch = self.cfg.ws_symbols_max
            now_slot = int(time.time() // (self.cfg.rotate_min * 60))
            start = (now_slot * batch) % max(1, self.total_symbols)
            self.active_symbols = [self._all_symbols[(start + i) % self.total_symbols] for i in range(batch)]
        logger.info("INFO [rotate] active=%d", len(self.active_symbols))

    async def start_ws(self):
        self.ws_started = True
        self.ws_topics = min(len(self.active_symbols) * 2, self.cfg.ws_symbols_max * 2)
        logger.info("INFO [ws] subscribed %d topics for %d symbols", self.ws_topics, len(self.active_symbols))

    async def poll_open_interest(self):
        if not self.client:
            return
        syms = self.active_symbols[:]
        random.shuffle(syms)
        sample = syms[: min(10, len(syms))]
        for sym in sample:
            _ = await self.client.get_open_interest(sym, interval="5min", limit=4)
            await asyncio.sleep(0.1)

    async def analyze_and_emit_signals(self):
        # TODO: Ваша логика сетапов (SMC/объём/OI/ликвидации)
        pass

    async def close(self):
        try:
            if self.http_session:
                await self.http_session.close()
        except Exception:
            pass
        self.http_session = None
        self.client = None


# =========================
# ДОСТУП
# =========================

def _is_allowed(cfg: Cfg, chat_id: int, user_id: Optional[int]) -> bool:
    """Разрешаем, если whitelist пуст, либо chat_id в списке, либо user_id в списке."""
    if not cfg.allowed_chat_ids:
        return True
    if chat_id in cfg.allowed_chat_ids:
        return True
    if user_id is not None and user_id in cfg.allowed_chat_ids:
        return True
    return False


# =========================
# ХЭНДЛЕРЫ КОМАНД
# =========================

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg: Cfg = ctx.application.bot_data["cfg"]
    chat_id = update.effective_chat.id if update.effective_chat else 0
    user_id = update.effective_user.id if update.effective_user else None
    if not _is_allowed(cfg, chat_id, user_id):
        return
    await update.effective_message.reply_text("Привет! Бот запущен ✅")

async def cmd_ping(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg: Cfg = ctx.application.bot_data["cfg"]
    chat_id = update.effective_chat.id if update.effective_chat else 0
    user_id = update.effective_user.id if update.effective_user else None
    if not _is_allowed(cfg, chat_id, user_id):
        return
    await update.effective_message.reply_text("🟢 online")

async def cmd_universe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg: Cfg = ctx.application.bot_data["cfg"]
    chat_id = update.effective_chat.id if update.effective_chat else 0
    user_id = update.effective_user.id if update.effective_user else None
    if not _is_allowed(cfg, chat_id, user_id):
        return
    engine: MarketEngine = ctx.application.bot_data["engine"]
    total = engine.total_symbols
    active = len(engine.active_symbols)
    ws_topics = engine.ws_topics if engine.ws_started else 0
    batch_num = 0
    txt = f"Вселенная: total={total}, active={active}, batch#{batch_num}, ws_topics={ws_topics}"
    await update.effective_message.reply_text(txt)

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg: Cfg = ctx.application.bot_data["cfg"]
    chat_id = update.effective_chat.id if update.effective_chat else 0
    user_id = update.effective_user.id if update.effective_user else None
    if not _is_allowed(cfg, chat_id, user_id):
        return
    engine: MarketEngine = ctx.application.bot_data["engine"]
    parts = [
        f"mode={cfg.universe_mode}",
        f"total={engine.total_symbols}",
        f"active={len(engine.active_symbols)}",
        f"ws={'on' if engine.ws_started else 'off'}/{engine.ws_topics}",
        f"filters: prob>{cfg.prob_min:.1f}%, profit>={cfg.profit_min_pct:.1f}%, RR>={cfg.rr_min:.2f}",
    ]
    await update.effective_message.reply_text("Статус: " + ", ".join(parts))

async def cmd_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Вспомогательная команда — показывает, кто вы и где пишете."""
    cfg: Cfg = ctx.application.bot_data["cfg"]
    chat = update.effective_chat
    user = update.effective_user
    chat_id = chat.id if chat else 0
    user_id = user.id if user else None
    allowed = _is_allowed(cfg, chat_id, user_id)
    lines = [
        f"chat_id={chat_id}",
        f"user_id={user_id}",
        f"type={chat.type if chat else 'unknown'}",
        f"allowed={allowed}",
    ]
    await update.effective_message.reply_text("• " + "\n• ".join(lines))

async def cmd_debug(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Краткая сводка для отладки."""
    cfg: Cfg = ctx.application.bot_data["cfg"]
    engine: MarketEngine = ctx.application.bot_data["engine"]
    chat_id = update.effective_chat.id if update.effective_chat else 0
    user_id = update.effective_user.id if update.effective_user else None
    lines = [
        f"cfg.public_url={cfg.public_url!r}",
        f"port={cfg.port} webhook_path={cfg.webhook_path}",
        f"universe: total={engine.total_symbols}, active={len(engine.active_symbols)}",
        f"ws_started={engine.ws_started} ws_topics={engine.ws_topics}",
        f"allowed_by_chat_or_user={_is_allowed(cfg, chat_id, user_id)}",
    ]
    await update.effective_message.reply_text("DEBUG:\n" + "\n".join(lines))


# =========================
# ГЛОБАЛЬНЫЕ ХЭНДЛЕРЫ/ЛОГИ
# =========================

async def on_any_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Ловим любые командные сообщения (Message и channel_post через effective_message)."""
    msg = update.effective_message
    if not msg:
        return
    if msg.text and msg.text.startswith("/"):
        who = (msg.from_user.id if msg.from_user else None)
        logger.info("[cmd] %r in chat=%s user=%s type=%s",
                    msg.text, msg.chat_id, who, update.effective_chat.type if update.effective_chat else None)

async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    logger.exception("Handler error:", exc_info=ctx.error)
    try:
        cfg: Cfg = ctx.application.bot_data.get("cfg")
        if cfg and cfg.primary_recipients:
            await ctx.application.bot.send_message(chat_id=cfg.primary_recipients[0], text=f"⚠️ Handler error: {ctx.error}")
    except Exception:
        pass


# =========================
# СЕРВИСНЫЕ ДЖОБЫ
# =========================

async def send_health(bot, cfg: Cfg):
    for chat_id in cfg.primary_recipients:
        try:
            await bot.send_message(chat_id=chat_id, text="🟢 online")
        except Exception as e:
            logger.warning("health-check -> %s: %s", chat_id, e)

async def self_ping(cfg: Cfg):
    if not cfg.self_ping_enable:
        return
    url = cfg.public_url or ""
    if not url:
        return
    try:
        async with httpx.AsyncClient(timeout=cfg.self_ping_timeout) as client:
            r = await client.get(url)
            logger.info("self-ping %s -> %s", url, r.status_code)
    except Exception as e:
        logger.warning("self-ping err: %s", e)


# =========================
# МИНИ HTTP (фолбэк для Render)
# =========================

async def start_tiny_http_server(port: int):
    app = web.Application()

    async def ok(_):
        return web.Response(text="ok")

    app.add_routes([web.get("/", ok), web.get("/healthz", ok)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("[http] tiny server listening on 0.0.0.0:%s", port)
    return runner


# =========================
# СБОРКА И ЗАПУСК
# =========================

def build_app(cfg: Cfg) -> Application:
    app = ApplicationBuilder().token(cfg.token).build()
    app.add_error_handler(on_error)

    # bot_data
    app.bot_data["cfg"] = cfg
    app.bot_data["engine"] = MarketEngine(cfg, bot_send=lambda chat_id, text: app.bot.send_message(chat_id, text))

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("universe", cmd_universe))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("debug", cmd_debug))

    # Диагностика входящих команд (и в чатах, и в каналах)
    app.add_handler(MessageHandler(filters.COMMAND, on_any_message), group=1)

    return app


async def bootstrap_scheduled_jobs(app: Application, cfg: Cfg):
    engine: MarketEngine = app.bot_data["engine"]

    scheduler = AsyncIOScheduler()
    app.bot_data["scheduler"] = scheduler

    async def job_bootstrap():
        try:
            await engine.bootstrap()
        except Exception as e:
            logger.warning("bootstrap warn: %s", e)

    async def job_start_ws():
        try:
            await engine.start_ws()
        except Exception as e:
            logger.warning("ws start warn: %s", e)

    async def job_rotate():
        try:
            await engine.rotate_active()
        except Exception as e:
            logger.warning("rotate warn: %s", e)

    async def job_poll_oi():
        try:
            await engine.poll_open_interest()
        except Exception as e:
            logger.warning("poll OI warn: %s", e)

    async def job_health():
        await send_health(app.bot, cfg)

    async def job_self_ping():
        await self_ping(cfg)

    # Регистрация
    scheduler.add_job(job_rotate, "interval", minutes=max(1, cfg.rotate_min))
    scheduler.add_job(job_poll_oi, "interval", minutes=2)
    scheduler.add_job(job_health, "interval", seconds=cfg.health_sec, next_run_time=None)
    scheduler.add_job(job_self_ping, "interval", seconds=cfg.self_ping_sec, next_run_time=None)

    scheduler.start()
    logger.info("Scheduler started")

    # Первичный прогон после задержки, чтобы порт точно был открыт
    await asyncio.sleep(max(0, cfg.startup_delay_sec))
    await job_bootstrap()
    await job_start_ws()
    await asyncio.sleep(max(0, cfg.health_first_sec))
    await job_health()


async def main_async():
    cfg = load_cfg()
    application = build_app(cfg)

    # Сначала — снести старый вебхук
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.warning("delete_webhook warn: %s", e)

    webhook_url = f"{cfg.public_url.rstrip('/')}{cfg.webhook_path}" if cfg.public_url else ""

    # Пытаемся вебхуком
    try:
        if not webhook_url.startswith("https://"):
            raise ValueError(f"PUBLIC_URL invalid or empty ('{cfg.public_url}')")

        logger.info("HTTP Request: POST setWebhook -> %s", webhook_url)
        await application.initialize()
        await application.updater.start_webhook(
            listen="0.0.0.0",
            port=cfg.port,
            url_path=cfg.webhook_path.lstrip("/"),
            webhook_url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
        )
        await application.start()

        await bootstrap_scheduled_jobs(application, cfg)
        logger.info("Application started")
        await application.updater.wait_until_idle()
        return

    except Exception as e:
        logger.error("Failed webhook path, falling back to polling: %s", e)

    # Фолбэк: tiny HTTP + polling
    await start_tiny_http_server(cfg.port)

    await application.initialize()
    await application.start()
    await bootstrap_scheduled_jobs(application, cfg)

    await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    logger.info("Polling started (fallback)")
    await application.updater.wait_until_idle()


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception("Fatal: %s", e)


if __name__ == "__main__":
    main()
