#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CryptoBot Telegram Bot — Render-friendly launcher with webhook/polling fallback.

Основные фичи:
- Правильная работа с PTB 21.6: пост-инициализация задач через post_init.
- Webhook при наличии корректного HTTPS PUBLIC_URL, иначе — polling.
- В polling-режиме поднимает мини-HTTP сервер на PORT, чтобы Render видел открытый порт.
- Вселенная Bybit (USDT perpetual, category=linear), ротация активного поднабора.
- Health-пинг в чат раз в HEALTH_SEC и self-ping на PUBLIC_URL для предотвращения idle sleep.
- Команды /ping, /status, /universe + фильтрация по ALLOWED_CHAT_IDS.

ENV (на Render):
- TELEGRAM_BOT_TOKEN (обяз.)
- ALLOWED_CHAT_IDS (через запятую, например: "-1002870952333,533232884")
- TELEGRAM_CHAT_ID (основной получатель, один ID)
- PUBLIC_URL (опционально, https://... для webhook)
- PORT (по умолчанию 10000)
- BYBIT_SYMBOL (необяз., если хотите привязать один символ — не используется в «вселенной»)
- Прочие числовые настройки можно менять в блоке Config.from_env()

Зависимости (requirements.txt уже включает):
python-telegram-bot[job-queue,webhooks]==21.6
aiohttp
httpx~=0.27
pandas, numpy, pytz (для дальнейшего анализа)
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import signal
import string
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    MessageHandler,
    filters,
)

# ---------- ЛОГГЕР ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------- УТИЛИТЫ ----------
def parse_ids(csv: str) -> List[int]:
    out = []
    for raw in csv.split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            out.append(int(raw))
        except ValueError:
            logger.warning("Bad chat id in ALLOWED_CHAT_IDS: %r", raw)
    return out


def gen_webhook_path(prefix: str = "/wh-") -> str:
    rng = random.Random(os.environ.get("WEBHOOK_SEED", str(time.time())))
    digits = "".join(rng.choice(string.digits) for _ in range(8))
    return f"{prefix}{digits}"


def is_https_url(url: Optional[str]) -> bool:
    if not url:
        return False
    try:
        pu = urlparse(url.strip())
        return pu.scheme == "https" and bool(pu.netloc)
    except Exception:
        return False


# ---------- КОНФИГ ----------
@dataclass(frozen=True)
class Config:
    token: str
    allowed_chat_ids: List[int]
    primary_recipients: List[int]
    public_url: str
    port: int
    webhook_path: str

    # Скрининг/сигналы
    prob_min: float = 69.9
    profit_min_pct: float = 1.0
    rr_min: float = 2.0
    signal_cooldown_sec: int = 600  # защита от дублирования

    # Здоровье/самопинг
    health_sec: int = 1200          # каждые 20 минут
    health_first_sec: int = 60
    startup_delay_sec: int = 10
    self_ping_sec: int = 780        # 13 минут
    self_ping_enabled: bool = True

    # Вселенная и ротация
    universe_mode: str = "all"      # all — все USDT perpetual
    universe_top_n: int = 15        # если будете резать «топ n» — задел
    ws_symbols_max: int = 60        # лимит «топиков»; условно 2 топика/символ => 30 символов
    rotate_min: int = 5             # каждые N минут пересбор активного набора

    # Триггеры объёма/ATR (плейсхолдеры)
    vol_mult: float = 2.0
    vol_sma_period: int = 20
    body_atr_mult: float = 0.6
    atr_period: int = 14

    @staticmethod
    def from_env() -> "Config":
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

        allowed = parse_ids(os.environ.get("ALLOWED_CHAT_IDS", "").strip())
        primary_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        primary = []
        if primary_raw:
            with contextlib.suppress(Exception):
                pid = int(primary_raw)
                if pid in allowed or not allowed:
                    primary.append(pid)
        if not primary and allowed:
            # если не задан основной — возьмём первый из белого списка
            primary = [allowed[0]]

        public_url = os.environ.get("PUBLIC_URL", "").strip()
        port = int(os.environ.get("PORT", "10000"))
        wh_path = os.environ.get("WEBHOOK_PATH", "").strip() or gen_webhook_path()

        # тюнинги
        prob_min = float(os.environ.get("PROB_MIN", "69.9"))
        profit_min_pct = float(os.environ.get("PROFIT_MIN_PCT", "1.0"))
        rr_min = float(os.environ.get("RR_MIN", "2.0"))
        signal_cooldown_sec = int(os.environ.get("SIGNAL_COOLDOWN_SEC", "600"))

        health_sec = int(os.environ.get("HEALTH_SEC", "1200"))
        health_first_sec = int(os.environ.get("HEALTH_FIRST_SEC", "60"))
        startup_delay_sec = int(os.environ.get("STARTUP_DELAY_SEC", "10"))
        self_ping_sec = int(os.environ.get("SELF_PING_SEC", "780"))
        self_ping_enabled = os.environ.get("SELF_PING_ENABLED", "true").lower() != "false"

        universe_mode = os.environ.get("UNIVERSE_MODE", "all")
        universe_top_n = int(os.environ.get("UNIVERSE_TOP_N", "15"))
        ws_symbols_max = int(os.environ.get("WS_SYMBOLS_MAX", "60"))
        rotate_min = int(os.environ.get("ROTATE_MIN", "5"))

        vol_mult = float(os.environ.get("VOL_MULT", "2.0"))
        vol_sma_period = int(os.environ.get("VOL_SMA_PERIOD", "20"))
        body_atr_mult = float(os.environ.get("BODY_ATR_MULT", "0.6"))
        atr_period = int(os.environ.get("ATR_PERIOD", "14"))

        cfg = Config(
            token=token,
            allowed_chat_ids=allowed,
            primary_recipients=primary,
            public_url=public_url,
            port=port,
            webhook_path=wh_path,
            prob_min=prob_min,
            profit_min_pct=profit_min_pct,
            rr_min=rr_min,
            signal_cooldown_sec=signal_cooldown_sec,
            health_sec=health_sec,
            health_first_sec=health_first_sec,
            startup_delay_sec=startup_delay_sec,
            self_ping_sec=self_ping_sec,
            self_ping_enabled=self_ping_enabled,
            universe_mode=universe_mode,
            universe_top_n=universe_top_n,
            ws_symbols_max=ws_symbols_max,
            rotate_min=rotate_min,
            vol_mult=vol_mult,
            vol_sma_period=vol_sma_period,
            body_atr_mult=body_atr_mult,
            atr_period=atr_period,
        )
        # Логируем ключевые параметры
        logger.info(
            "INFO [cfg] ALLOWED_CHAT_IDS=%s",
            cfg.allowed_chat_ids,
        )
        logger.info("INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.primary_recipients)
        logger.info(
            "INFO [cfg] PUBLIC_URL=%r PORT=%d WEBHOOK_PATH=%r",
            cfg.public_url, cfg.port, cfg.webhook_path
        )
        logger.info(
            "INFO [cfg] HEALTH=%ss FIRST=%ss STARTUP=%ss SELF_PING=%s/%ss",
            cfg.health_sec, cfg.health_first_sec, cfg.startup_delay_sec,
            "True" if cfg.self_ping_enabled else "False",
            cfg.self_ping_sec
        )
        logger.info(
            "INFO [cfg] SIGNAL_COOLDOWN_SEC=%d SIGNAL_TTL_MIN=%d UNIVERSE_MODE=%s "
            "UNIVERSE_TOP_N=%d WS_SYMBOLS_MAX=%d ROTATE_MIN=%d PROB_MIN>%.1f "
            "PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
            cfg.signal_cooldown_sec, 12, cfg.universe_mode, cfg.universe_top_n,
            cfg.ws_symbols_max, cfg.rotate_min, cfg.prob_min, cfg.profit_min_pct, cfg.rr_min
        )
        logger.info(
            "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, "
            "BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
            cfg.vol_mult, cfg.vol_sma_period, cfg.body_atr_mult, cfg.atr_period
        )
        return cfg


# ---------- BYBIT КЛИЕНТ ----------
class BybitClient:
    BASE = "https://api.bybit.com"

    def __init__(self):
        self.http = httpx.AsyncClient(base_url=self.BASE, timeout=15.0)

    async def close(self):
        await self.http.aclose()

    async def get_linear_instruments(self) -> List[str]:
        """
        Возвращает список символов USDT perpetual (category=linear), которые торгуются.
        """
        out = []
        cursor = None
        while True:
            params = {"category": "linear"}
            if cursor:
                params["cursor"] = cursor
            r = await self.http.get("/v5/market/instruments-info", params=params)
            r.raise_for_status()
            data = r.json()
            if data.get("retCode") != 0:
                raise RuntimeError(f"Bybit instruments error: {data}")
            result = data.get("result", {})
            for item in result.get("list", []):
                if item.get("status") == "Trading" and item.get("quoteCoin") == "USDT":
                    out.append(item["symbol"])
            cursor = result.get("nextPageCursor") or None
            if not cursor:
                break
        return out

    async def get_open_interest(self, symbol: str, interval: str = "5min", limit: int = 4) -> Optional[float]:
        """
        Возвращает последнее значение OI (USD) для symbol.
        """
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": str(limit),
        }
        r = await self.http.get("/v5/market/open-interest", params=params)
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0:
            # Не считаем фатально — просто None
            logger.debug("Bybit OI non-zero retCode for %s: %s", symbol, data)
            return None
        lst = data.get("result", {}).get("list") or []
        if not lst:
            return None
        # формат: [timestamp, openInterest, ...]
        try:
            # берём последнее
            last = lst[-1]
            oi = float(last[1])
            return oi
        except Exception:
            return None


# ---------- ВСЕЛЕННАЯ/ДВИЖОК ----------
@dataclass
class UniverseState:
    total: int = 0
    active: int = 0
    batch_index: int = 0
    ws_topics: int = 0
    symbols_all: List[str] = field(default_factory=list)
    symbols_active: List[str] = field(default_factory=list)


class Engine:
    def __init__(self, cfg: Config, app: Application):
        self.cfg = cfg
        self.app = app
        self.client = BybitClient()
        self.state = UniverseState()
        self._last_health_ts = 0.0
        self._cooldown_until: dict[str, float] = {}

    async def bootstrap(self):
        # Получаем весь список USDT perpetual
        syms = await self.client.get_linear_instruments()
        random.shuffle(syms)
        self.state.symbols_all = syms
        self.state.total = len(syms)

        # Сформируем активный срез исходя из лимита топиков
        per_symbol_topics = 2  # publicTrade + tickers (пример)
        max_symbols = max(1, self.cfg.ws_symbols_max // per_symbol_topics)
        self.state.symbols_active = syms[:max_symbols]
        self.state.active = len(self.state.symbols_active)
        self.state.ws_topics = self.state.active * per_symbol_topics

        logger.info(
            "INFO [universe] total=%d active=%d mode=%s",
            self.state.total, self.state.active, self.cfg.universe_mode
        )

    async def rotate(self, *_):
        """Ротация активного поднабора."""
        if not self.state.symbols_all:
            return
        per_symbol_topics = 2
        max_symbols = max(1, self.cfg.ws_symbols_max // per_symbol_topics)
        # двигаем окно
        start = (self.state.batch_index * max_symbols) % max(1, self.state.total)
        self.state.batch_index += 1
        # циклическая выборка
        sl = (self.state.symbols_all + self.state.symbols_all)[start: start + max_symbols]
        self.state.symbols_active = sl
        self.state.active = len(sl)
        self.state.ws_topics = self.state.active * per_symbol_topics
        logger.debug("rotate -> active=%d batch#%d", self.state.active, self.state.batch_index)

    async def poll_oi(self, *_):
        """Простая выборочная проверка OI по активным символам (для демонстрации/прогрева)."""
        if not self.state.symbols_active:
            return
        sample = random.sample(self.state.symbols_active, k=min(5, len(self.state.symbols_active)))
        results = []
        for sym in sample:
            with contextlib.suppress(Exception):
                oi = await self.client.get_open_interest(sym)
                if oi is not None:
                    results.append((sym, oi))
        if results:
            logger.debug("OI sample: %s", results[:3])

    async def send_health(self, *_):
        """Проверочное сообщение 'online' раз в HEALTH_SEC, чтобы не засыпал free-инстанс."""
        now = time.time()
        if now - self._last_health_ts < self.cfg.health_sec:
            return
        self._last_health_ts = now
        txt = "online"
        for chat_id in self.cfg.primary_recipients:
            with contextlib.suppress(Exception):
                await self.app.bot.send_message(chat_id=chat_id, text=txt)

    async def self_ping(self, *_):
        """Пингуем свой PUBLIC_URL, если есть — для поддержания активности Render."""
        if not self.cfg.self_ping_enabled or not is_https_url(self.cfg.public_url):
            return
        url = self.cfg.public_url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=10.0) as cli:
                await cli.get(url)
        except Exception:
            pass

    def build_status_text(self) -> str:
        st = self.state
        lines = [
            f"Вселенная: total={st.total}, active={st.active}, batch#{st.batch_index}, ws_topics={st.ws_topics}"
        ]
        return "\n".join(lines)

    async def close(self):
        await self.client.close()


# ---------- МИНИ HTTP СЕРВЕР (для polling) ----------
class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/healthz"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        # приглушим шум
        return


def start_tiny_http_server_thread(port: int):
    def _run():
        try:
            httpd = HTTPServer(("0.0.0.0", port), _Handler)
            httpd.serve_forever()
        except Exception as e:
            logger.error("tiny http server failed: %s", e)

    t = threading.Thread(target=_run, name="tiny-http", daemon=True)
    t.start()
    logger.info("tiny HTTP server started on 0.0.0.0:%d", port)


# ---------- ФИЛЬТР ДОСТУПА ----------
def is_allowed(cfg: Config, update: Update) -> bool:
    if not cfg.allowed_chat_ids:
        return True
    chat_id = None
    if update.effective_chat:
        chat_id = update.effective_chat.id
    return chat_id in cfg.allowed_chat_ids


# ---------- ХЕНДЛЕРЫ КОМАНД ----------
async def cmd_ping(update: Update, context: CallbackContext):
    cfg: Config = context.application.bot_data["cfg"]
    if not is_allowed(cfg, update):
        return
    await update.effective_message.reply_text("pong")


async def cmd_status(update: Update, context: CallbackContext):
    cfg: Config = context.application.bot_data["cfg"]
    if not is_allowed(cfg, update):
        return
    engine: Engine = context.application.bot_data["engine"]
    await update.effective_message.reply_text(engine.build_status_text())


async def cmd_universe(update: Update, context: CallbackContext):
    cfg: Config = context.application.bot_data["cfg"]
    if not is_allowed(cfg, update):
        return
    engine: Engine = context.application.bot_data["engine"]
    st = engine.state
    text = engine.build_status_text()
    if st.symbols_active:
        sample = ", ".join(st.symbols_active[:15])
        text += f"\nАктивные (пример): {sample}" + (" ..." if len(st.symbols_active) > 15 else "")
    await update.effective_message.reply_text(text)


# ---------- post_init / post_shutdown ----------
async def post_init(app: Application):
    cfg: Config = app.bot_data["cfg"]
    engine: Engine = app.bot_data["engine"]

    # Bootstrap данных (вселенная и пр.)
    try:
        await engine.bootstrap()
    except Exception as e:
        logger.exception("bootstrap failed: %s", e)
        # не останавливаем приложение — продолжим, команды будут отвечать

    # Планировщик задач
    jq = app.job_queue
    jq.run_once(lambda *_: logger.info("Scheduler started"), when=0)

    # Ротация каждые N минут
    jq.run_repeating(engine.rotate, interval=cfg.rotate_min * 60, first=cfg.startup_delay_sec)
    # OI выборка
    jq.run_repeating(engine.poll_oi, interval=60, first=cfg.startup_delay_sec + 5)
    # Health -> чат
    jq.run_repeating(engine.send_health, interval=cfg.health_sec, first=cfg.health_first_sec)
    # Self-ping -> PUBLIC_URL
    if cfg.self_ping_enabled and is_https_url(cfg.public_url):
        jq.run_repeating(engine.self_ping, interval=cfg.self_ping_sec, first=cfg.health_first_sec)

    # Приветственное сообщение один раз (не спамим)
    if cfg.primary_recipients:
        with contextlib.suppress(Exception):
            await app.bot.send_message(
                chat_id=cfg.primary_recipients[0],
                text="Bot started ✅",
            )


async def post_shutdown(app: Application):
    engine: Engine = app.bot_data.get("engine")
    if engine:
        with contextlib.suppress(Exception):
            await engine.close()


# ---------- СБОРОЧКА ПРИЛОЖЕНИЯ ----------
def build_application(cfg: Config) -> Application:
    application = (
        ApplicationBuilder()
        .token(cfg.token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # Ботовые данные
    application.bot_data["cfg"] = cfg
    application.bot_data["engine"] = Engine(cfg, application)

    # Команды
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("universe", cmd_universe))

    # Игнор всего остального (по желанию)
    application.add_handler(MessageHandler(filters.COMMAND, cmd_ping))  # на неизвестные команды — pong

    return application


# ---------- MAIN ----------
def main():
    cfg = Config.from_env()
    application = build_application(cfg)

    # Решение: webhook если PUBLIC_URL валидный https, иначе polling.
    webhook_ok = is_https_url(cfg.public_url)
    if webhook_ok:
        webhook_url = f"{cfg.public_url.rstrip('/')}{cfg.webhook_path}"
        # Перед запуском попробуем снести старый вебхук (без паники в случае ошибки)
        async def _pre_del():
            with contextlib.suppress(Exception):
                await application.bot.delete_webhook(drop_pending_updates=True)
        asyncio.run(_pre_del())

        # run_webhook сам держит цикл; stop_signals=None — безопаснее для Render
        logger.info("Starting in WEBHOOK mode @ %s", webhook_url)
        application.run_webhook(
            listen="0.0.0.0",
            port=cfg.port,
            url_path=cfg.webhook_path,
            webhook_url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
            stop_signals=None,
        )
    else:
        # POLLING + tiny HTTP на PORT (для Render port scan)
        logger.info("PUBLIC_URL not https/empty — starting in POLLING mode")
        start_tiny_http_server_thread(cfg.port)

        async def _run_polling():
            with contextlib.suppress(Exception):
                await application.bot.delete_webhook(drop_pending_updates=True)
            await application.initialize()
            await application.start()
            await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            logger.info("Polling started (fallback)")
            # держим процесс живым
            while True:
                await asyncio.sleep(3600)

        try:
            asyncio.run(_run_polling())
        except KeyboardInterrupt:
            pass
        finally:
            # Аккуратная остановка
            async def _shutdown():
                with contextlib.suppress(Exception):
                    await application.updater.stop()
                with contextlib.suppress(Exception):
                    await application.stop()
                with contextlib.suppress(Exception):
                    await application.shutdown()
            with contextlib.suppress(Exception):
                asyncio.run(_shutdown())


if __name__ == "__main__":
    main()
