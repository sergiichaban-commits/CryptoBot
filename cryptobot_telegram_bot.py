#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CryptoSignals Telegram Bot (Render friendly)
- PTB 21.6
- Webhook if PUBLIC_URL is https, иначе fallback на polling + tiny HTTP для Render
- Ротация вселенной (все USDT perpetual на Bybit, category=linear)
- Заготовки под OI и WS тикеры (минимально необходимые, без минутных отчётов)
- Фильтры сигналов оставлены (порог прибыли, RR, вероятность), но сами сигналы не генерятся
  до вашей дальнейшей логики анализа — сейчас бот стабилен и управляем командами.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import signal
import string
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import web
from websockets.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ------------------------- ЛОГИРОВАНИЕ -------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------- КОНФИГ -------------------------

def _parse_ids(value: str) -> List[int]:
    if not value:
        return []
    try:
        arr = json.loads(value)
        if isinstance(arr, list):
            return [int(x) for x in arr]
    except json.JSONDecodeError:
        pass
    # comma/space separated
    out = []
    for part in value.replace(";", ",").replace(" ", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out


def _rand_path() -> str:
    return "".join(random.choices(string.digits, k=8))


@dataclass(frozen=True)
class Cfg:
    token: str
    allowed_chat_ids: List[int]
    primary_recipients: List[int]
    public_url: str
    port: int
    webhook_path: str

    # сервисные интервалы / поведение Render
    health_sec: int = 20 * 60        # каждые 20 минут "online"
    first_health_sec: int = 60       # первая проверка
    startup_delay_sec: int = 10      # перед стартом
    self_ping: bool = True
    self_ping_sec: int = 13 * 60     # self-ping каждые ~13 минут

    # маркет-скан
    signal_cooldown_sec: int = 10 * 60
    signal_ttl_min: int = 12

    # вселенная
    universe_mode: str = "all"       # all / topN (оставили только all)
    universe_top_n: int = 15
    ws_symbols_max: int = 60
    rotate_min: int = 5

    # фильтры сигналов
    prob_min: float = 69.9
    profit_min_pct: float = 1.0
    rr_min: float = 2.0

    # параметры простого volume-триггера (на будущее)
    vol_mult: float = 2.0
    vol_sma_period: int = 20
    body_atr_mult: float = 0.6
    atr_period: int = 14


def load_cfg() -> Cfg:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    allowed = _parse_ids(os.getenv("ALLOWED_CHAT_IDS", ""))
    primary_raw = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    primary_id = []
    if primary_raw:
        try:
            primary_id = [int(primary_raw)]
        except ValueError:
            primary_id = []

    # PUBLIC_URL: RENDER_EXTERNAL_URL как fallback
    public_url = os.getenv("PUBLIC_URL", "").strip()
    if not public_url:
        public_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()

    # порт: Render передаёт env PORT, используем его в приоритете
    port = 0
    for key in ("PORT", "HTTP_PORT"):
        raw = os.getenv(key, "").strip()
        if raw:
            try:
                port = int(raw)
                break
            except ValueError:
                pass
    if not port:
        port = 10000

    # путь вебхука
    wh_env = os.getenv("WEBHOOK_PATH", "").strip()
    webhook_path = wh_env if wh_env.startswith("/") else f"/wh-{_rand_path()}"

    # пороги
    profit_min_pct = float(os.getenv("PROFIT_MIN_PCT", "1.0"))
    rr_min = float(os.getenv("RR_MIN", "2.0"))
    prob_min = float(os.getenv("PROB_MIN", "69.9"))

    rotate_min = int(os.getenv("ROTATE_MIN", "5"))
    ws_symbols_max = int(os.getenv("WS_SYMBOLS_MAX", "60"))

    cfg = Cfg(
        token=token,
        allowed_chat_ids=allowed,
        primary_recipients=[x for x in primary_id if x in allowed] or [x for x in allowed if x < 0] or allowed[:1],
        public_url=public_url,
        port=port,
        webhook_path=webhook_path,
        profit_min_pct=profit_min_pct,
        rr_min=rr_min,
        prob_min=prob_min,
        rotate_min=rotate_min,
        ws_symbols_max=ws_symbols_max,
    )

    logger.info(
        "INFO [cfg] ALLOWED_CHAT_IDS=%s", cfg.allowed_chat_ids
    )
    logger.info(
        "INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.primary_recipients
    )
    logger.info(
        "INFO [cfg] PUBLIC_URL='%s' PORT=%d WEBHOOK_PATH='%s'",
        cfg.public_url, cfg.port, cfg.webhook_path
    )
    logger.info(
        "INFO [cfg] HEALTH=%ss FIRST=%ss STARTUP=%ss SELF_PING=%s/%ss",
        cfg.health_sec, cfg.first_health_sec, cfg.startup_delay_sec, cfg.self_ping, cfg.self_ping_sec
    )
    logger.info(
        "INFO [cfg] SIGNAL_COOLDOWN_SEC=%d SIGNAL_TTL_MIN=%d UNIVERSE_MODE=%s UNIVERSE_TOP_N=%d WS_SYMBOLS_MAX=%d ROTATE_MIN=%d PROB_MIN>%.1f PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
        cfg.signal_cooldown_sec, cfg.signal_ttl_min, cfg.universe_mode, cfg.universe_top_n,
        cfg.ws_symbols_max, cfg.rotate_min, cfg.prob_min, cfg.profit_min_pct, cfg.rr_min
    )
    logger.info(
        "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
        cfg.vol_mult, cfg.vol_sma_period, cfg.body_atr_mult, cfg.atr_period
    )
    return cfg

# ------------------------- BYBIT API -------------------------

BYBIT_HTTP = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"


class BybitClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_instruments_linear_usdt(self) -> List[str]:
        """
        Все линейные (USDT perpetual) инструменты. Возвращает список символов, например ['BTCUSDT','ETHUSDT',...]
        """
        url = f"{BYBIT_HTTP}/v5/market/instruments-info?category=linear&limit=1000"
        async with self.session.get(url, timeout=30) as resp:
            data = await resp.json(content_type=None)
        if str(data.get("retCode")) != "0":
            logger.warning("Bybit instruments error: %s", data)
            return []
        lst = data.get("result", {}).get("list", []) or []
        out = []
        for it in lst:
            if it.get("quoteCoin") == "USDT" and it.get("status") == "Trading":
                sym = it.get("symbol")
                if sym:
                    out.append(sym)
        return sorted(out)

    async def get_open_interest_last(self, symbol: str) -> Optional[float]:
        """
        Последнее значение OI (USD) 5-мин. Свелось к одному значению для справки.
        """
        params = f"category=linear&symbol={symbol}&interval=5min&limit=1"
        url = f"{BYBIT_HTTP}/v5/market/open-interest?{params}"
        async with self.session.get(url, timeout=15) as resp:
            data = await resp.json(content_type=None)
        if str(data.get("retCode")) != "0":
            logger.debug("Bybit OI error for %s: %s", symbol, data)
            return None
        rows = data.get("result", {}).get("list", [])
        if not rows:
            return None
        try:
            # значение в USD
            return float(rows[-1]["openInterest"])
        except Exception:
            return None

# ------------------------- ДВИЖОК -------------------------

class Engine:
    def __init__(self, cfg: Cfg, bot):
        self.cfg = cfg
        self.bot = bot
        self.session: Optional[aiohttp.ClientSession] = None
        self.client: Optional[BybitClient] = None

        self.universe_all: List[str] = []
        self.active_symbols: List[str] = []
        self.rotate_idx: int = 0

        self.ws_task: Optional[asyncio.Task] = None
        self.ws_topics: int = 0
        self.ws_should_run: bool = False

        self.oi_cache: Dict[str, float] = {}
        self._oi_rr: int = 0  # round-robin index

    async def bootstrap(self) -> None:
        self.session = aiohttp.ClientSession()
        self.client = BybitClient(self.session)

        # Вселенная
        all_syms = await self.client.get_instruments_linear_usdt()
        self.universe_all = all_syms
        # начальный активный батч
        self._apply_rotation(reset=True)
        logger.info("INFO [universe] total=%d active=%d mode=%s", len(self.universe_all), len(self.active_symbols), self.cfg.universe_mode)

    async def close(self) -> None:
        self.ws_should_run = False
        if self.ws_task and not self.ws_task.done():
            self.ws_task.cancel()
        if self.session:
            await self.session.close()

    def _apply_rotation(self, reset: bool = False) -> None:
        if reset:
            self.rotate_idx = 0
        n = len(self.universe_all)
        if n == 0:
            self.active_symbols = []
            return
        size = min(self.cfg.ws_symbols_max, n)
        start = (self.rotate_idx * size) % n
        batch = []
        i = start
        while len(batch) < size and i < start + n:
            batch.append(self.universe_all[i % n])
            i += 1
        self.active_symbols = batch

    async def rotate(self) -> None:
        if not self.universe_all:
            return
        self.rotate_idx += 1
        self._apply_rotation()
        logger.info("INFO [rotate] batch#%d active=%d", self.rotate_idx, len(self.active_symbols))
        # Переподписка WS произойдёт при следующем цикле run_ws (просто перестроим список)

    async def run_ws(self) -> None:
        """
        Лёгкая подписка на тикеры для активного набора (для живости).
        """
        self.ws_should_run = True
        while self.ws_should_run:
            try:
                topics = [f"tickers.{s}" for s in self.active_symbols]
                self.ws_topics = len(topics)
                if not topics:
                    await asyncio.sleep(5)
                    continue
                logger.info("INFO [ws] connecting, topics=%d", len(topics))
                async with ws_connect(
                    BYBIT_WS_PUBLIC_LINEAR,
                    ping_interval=25,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=2048,
                ) as ws:
                    sub_msg = {"op": "subscribe", "args": topics}
                    await ws.send(json.dumps(sub_msg))
                    logger.info("INFO [ws] subscribed %d topics", len(topics))

                    while self.ws_should_run:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=60)
                        except asyncio.TimeoutError:
                            # периодически слать ping
                            try:
                                await ws.ping()
                            except Exception:
                                break
                            continue
                        if not raw:
                            continue
                        # можно добавить парс/аккумуляцию последних цен при желании
                        # msg = json.loads(raw)
                        # ...
            except ConnectionClosed:
                logger.warning("WS closed, reconnecting soon ...")
            except Exception as e:
                logger.warning("WS error: %s", e)
            await asyncio.sleep(3)

    async def poll_oi_once(self) -> None:
        """Опрашиваем OI по одному символу за тик, round-robin."""
        if not self.client or not self.active_symbols:
            return
        sym = self.active_symbols[self._oi_rr % len(self.active_symbols)]
        self._oi_rr += 1
        oi = await self.client.get_open_interest_last(sym)
        if oi is not None:
            self.oi_cache[sym] = oi

    # Заготовка под сигналы — здесь вставите свою SMC/OB/LIQ/ОI-логику.
    # Сейчас — ничего не посылаем, чтобы не шуметь.


# ------------------------- ТЕЛЕГРАМ ХЕНДЛЕРЫ -------------------------

def is_authorized(update: Update, cfg: Cfg) -> bool:
    chat_id = None
    if update.effective_chat:
        chat_id = update.effective_chat.id
    elif update.effective_message:
        chat_id = update.effective_message.chat_id
    if chat_id is None:
        return False
    return chat_id in cfg.allowed_chat_ids


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Cfg = context.bot_data["cfg"]
    if not is_authorized(update, cfg):
        return
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    await update.effective_message.reply_text(f"pong · {now}")


async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Cfg = context.bot_data["cfg"]
    if not is_authorized(update, cfg):
        return
    engine: Engine = context.bot_data["engine"]
    total = len(engine.universe_all)
    active = len(engine.active_symbols)
    batch = engine.rotate_idx
    ws_topics = engine.ws_topics
    text = f"Вселенная: total={total}, active={active}, batch#{batch}, ws_topics={ws_topics}"
    await update.effective_message.reply_text(text)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Cfg = context.bot_data["cfg"]
    if not is_authorized(update, cfg):
        return
    engine: Engine = context.bot_data["engine"]
    lines = [
        "Статус:",
        f"- universe total={len(engine.universe_all)} active={len(engine.active_symbols)} batch#{engine.rotate_idx}",
        f"- ws_topics={engine.ws_topics}",
        f"- filters: prob>{cfg.prob_min:.1f}% profit>={cfg.profit_min_pct:.1f}% RR>={cfg.rr_min:.2f}",
    ]
    await update.effective_message.reply_text("\n".join(lines))


# ------------------------- ПЛАНИРОВЩИКИ -------------------------

async def job_start_ws(context: ContextTypes.DEFAULT_TYPE) -> None:
    engine: Engine = context.bot_data["engine"]
    if engine.ws_task is None or engine.ws_task.done():
        engine.ws_task = asyncio.create_task(engine.run_ws())


async def job_rotate(context: ContextTypes.DEFAULT_TYPE) -> None:
    engine: Engine = context.bot_data["engine"]
    await engine.rotate()


async def job_poll_oi(context: ContextTypes.DEFAULT_TYPE) -> None:
    engine: Engine = context.bot_data["engine"]
    await engine.poll_oi_once()


async def job_health(context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Cfg = context.bot_data["cfg"]
    try:
        for chat_id in cfg.primary_recipients:
            await context.bot.send_message(chat_id, "online")
    except Exception as e:
        logger.warning("health-check send error: %s", e)


async def job_self_ping(context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Cfg = context.bot_data["cfg"]
    if not cfg.public_url:
        return
    url = cfg.public_url.rstrip("/")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=10) as resp:
                _ = await resp.text()
    except Exception as e:
        logger.debug("self-ping failed: %s", e)


# ------------------------- HTTP (для Render) -------------------------

async def start_tiny_http_server(port: int) -> None:
    """
    Минимальный HTTP-сервер, чтобы Render увидел открытый порт,
    когда мы в режиме polling (без вебхука).
    """
    async def index(_request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/health", index)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("HTTP tiny server started on 0.0.0.0:%d", port)


# ------------------------- MAIN -------------------------

def build_application(cfg: Cfg) -> Application:
    application = (
        Application.builder()
        .token(cfg.token)
        .concurrent_updates(True)
        .build()
    )

    # Handlers
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("universe", cmd_universe))
    application.add_handler(CommandHandler("status", cmd_status))

    # Доступ к cfg/engine через bot_data (user_data — mappingproxy)
    application.bot_data["cfg"] = cfg
    return application


async def bootstrap_scheduled_jobs(application: Application, cfg: Cfg, engine: Engine) -> None:
    jq = application.job_queue
    # WS стартуем сразу после запуска
    jq.run_once(job_start_ws, when=1)
    # Ротация набора
    jq.run_repeating(job_rotate, interval=cfg.rotate_min * 60, first=cfg.rotate_min * 60)
    # OI поллим скромно
    jq.run_repeating(job_poll_oi, interval=30, first=5)
    # Health
    jq.run_repeating(job_health, interval=cfg.health_sec, first=cfg.first_health_sec)
    # Self-ping (если есть PUBLIC_URL)
    if cfg.self_ping and cfg.public_url:
        jq.run_repeating(job_self_ping, interval=cfg.self_ping_sec, first=30)

    logger.info("Scheduler started")


async def main_async() -> None:
    cfg = load_cfg()

    # небольшая задержка старта контейнера
    await asyncio.sleep(cfg.startup_delay_sec)

    application = build_application(cfg)
    bot = application.bot

    # Engine
    engine = Engine(cfg, bot)
    application.bot_data["engine"] = engine

    # Базовая проверка, приветствие и загрузка вселенной
    try:
        me = await bot.get_me()
        for chat_id in cfg.primary_recipients:
            await bot.send_message(chat_id, f"Бот запущен: @{me.username}")
    except Exception:
        # не критично для старта
        pass

    await engine.bootstrap()

    # Старт приложения
    await application.initialize()
    await application.start()
    await bootstrap_scheduled_jobs(application, cfg, engine)
    logger.info("Application started")

    # Ветка WEBHOOK (если PUBLIC_URL начинается с https)
    if cfg.public_url and cfg.public_url.startswith("https://"):
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        await application.updater.start_webhook(
            listen="0.0.0.0",
            port=cfg.port,
            url_path=cfg.webhook_path,
            secret_token=None,
            allowed_updates=Update.ALL_TYPES,
        )
        try:
            await bot.set_webhook(
                url=f"{cfg.public_url.rstrip('/')}{cfg.webhook_path}",
                allowed_updates=Update.ALL_TYPES,
            )
        except Exception as e:
            logger.error("set_webhook failed: %s — fallback to polling", e)
            await application.updater.stop()
            # Fallback на polling
            await start_tiny_http_server(cfg.port)
            await bot.delete_webhook(drop_pending_updates=True)
            await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            logger.info("Polling started (fallback)")
            # Держим процесс живым
            await asyncio.Event().wait()
        else:
            logger.info("Webhook set: %s", f"{cfg.public_url.rstrip('/')}{cfg.webhook_path}")
            # Держим процесс живым
            await asyncio.Event().wait()
    else:
        # POLLING + tiny HTTP server для Render (порт-скан)
        await start_tiny_http_server(cfg.port)
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        logger.info("Polling started (fallback)")
        # Держим процесс живым (замена отсутствующему wait_until_idle())
        await asyncio.Event().wait()


def main() -> None:
    # Корректное завершение на SIGTERM/SIGINT
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, loop.stop)
        except NotImplementedError:
            # Windows/или окружения без signal
            pass

    try:
        loop.run_until_complete(main_async())
    finally:
        # петля уже остановлена обработчиком сигналов/ошибками
        pending = asyncio.all_tasks(loop=loop)
        for task in pending:
            task.cancel()
        try:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    main()
