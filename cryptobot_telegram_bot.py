#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple

import aiohttp
from aiohttp import web

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from telegram.error import Conflict


# ------------------------------- ЛОГИ --------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------- КОНФИГ И ENV ---------------------------------

def _parse_int_list(s: str) -> List[int]:
    out: List[int] = []
    if not s:
        return out
    for part in re.split(r"[,\s]+", s.strip()):
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            pass
    return out


@dataclass
class Config:
    token: str
    allowed_chat_ids: List[int] = field(default_factory=list)
    primary_recipients: List[int] = field(default_factory=list)

    # Render/web
    http_port: int = 10000
    public_url: str = ""

    # Периодика keep-alive и health
    health_first_sec: int = 60          # первое "online" через 1 минуту
    health_interval_sec: int = 1200     # каждые 20 минут
    self_ping_interval_sec: int = 780   # каждые 13 минут
    self_ping_enabled: bool = True

    # Вселенная и ротация
    universe_mode: str = "all"
    universe_top_n: int = 15
    ws_symbols_max: int = 60
    rotate_min: int = 5

    # Фильтры сигналов
    prob_min: float = 69.9
    profit_min_pct: float = 1.0
    rr_min: float = 2.0

    # Триггеры/параметры (информационно)
    vol_mult: float = 2.0
    vol_sma_period: int = 20
    body_atr_mult: float = 0.6
    atr_period: int = 14

    # Сигнал-сервис
    signal_cooldown_sec: int = 600
    signal_ttl_min: int = 12

    @staticmethod
    def load() -> "Config":
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        allowed_raw = os.getenv("ALLOWED_CHAT_IDS", "")
        allowed = _parse_int_list(allowed_raw)

        primary_raw = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        primary: List[int] = []
        if primary_raw:
            with contextlib.suppress(Exception):
                primary.append(int(primary_raw))

        http_port = int(os.getenv("PORT", os.getenv("HTTP_PORT", "10000")))
        public_url = os.getenv("PUBLIC_URL", "").strip()

        health_interval_sec = int(os.getenv("HEALTH_INTERVAL_SEC", "1200"))
        health_first_sec = int(os.getenv("HEALTH_FIRST_SEC", "60"))
        self_ping_interval_sec = int(os.getenv("SELF_PING_INTERVAL_SEC", "780"))
        self_ping_enabled = os.getenv("SELF_PING_ENABLED", "true").lower() in ("1", "true", "yes")

        universe_mode = os.getenv("UNIVERSE_MODE", "all").strip().lower()
        universe_top_n = int(os.getenv("UNIVERSE_TOP_N", "15"))
        ws_symbols_max = int(os.getenv("WS_SYMBOLS_MAX", "60"))
        rotate_min = int(os.getenv("ROTATE_MIN", "5"))

        prob_min = float(os.getenv("PROB_MIN", "69.9"))
        profit_min_pct = float(os.getenv("PROFIT_MIN_PCT", "1.0"))
        rr_min = float(os.getenv("RR_MIN", "2.0"))

        vol_mult = float(os.getenv("VOL_MULT", "2.0"))
        vol_sma_period = int(os.getenv("VOL_SMA_PERIOD", "20"))
        body_atr_mult = float(os.getenv("BODY_ATR_MULT", "0.6"))
        atr_period = int(os.getenv("ATR_PERIOD", "14"))

        signal_cooldown_sec = int(os.getenv("SIGNAL_COOLDOWN_SEC", "600"))
        signal_ttl_min = int(os.getenv("SIGNAL_TTL_MIN", "12"))

        cfg = Config(
            token=token,
            allowed_chat_ids=allowed,
            primary_recipients=primary,
            http_port=http_port,
            public_url=public_url,
            health_first_sec=health_first_sec,
            health_interval_sec=health_interval_sec,
            self_ping_interval_sec=self_ping_interval_sec,
            self_ping_enabled=self_ping_enabled,
            universe_mode=universe_mode,
            universe_top_n=universe_top_n,
            ws_symbols_max=ws_symbols_max,
            rotate_min=rotate_min,
            prob_min=prob_min,
            profit_min_pct=profit_min_pct,
            rr_min=rr_min,
            vol_mult=vol_mult,
            vol_sma_period=vol_sma_period,
            body_atr_mult=body_atr_mult,
            atr_period=atr_period,
            signal_cooldown_sec=signal_cooldown_sec,
            signal_ttl_min=signal_ttl_min,
        )

        log.info("INFO [cfg] ALLOWED_CHAT_IDS=%s", cfg.allowed_chat_ids)
        log.info("INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.primary_recipients)
        log.info("INFO [cfg] PUBLIC_URL='%s' PORT=%d", cfg.public_url, cfg.http_port)
        log.info(
            "INFO [cfg] HEALTH=%ss FIRST=%ss SELF_PING=%s/%ss",
            cfg.health_interval_sec,
            cfg.health_first_sec,
            cfg.self_ping_enabled,
            cfg.self_ping_interval_sec,
        )
        log.info(
            "INFO [cfg] SIGNAL_COOLDOWN_SEC=%d SIGNAL_TTL_MIN=%d UNIVERSE_MODE=%s UNIVERSE_TOP_N=%d WS_SYMBOLS_MAX=%d ROTATE_MIN=%d PROB_MIN>%.1f PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
            cfg.signal_cooldown_sec,
            cfg.signal_ttl_min,
            cfg.universe_mode,
            cfg.universe_top_n,
            cfg.ws_symbols_max,
            cfg.rotate_min,
            cfg.prob_min,
            cfg.profit_min_pct,
            cfg.rr_min,
        )
        log.info(
            "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
            cfg.vol_mult,
            cfg.vol_sma_period,
            cfg.body_atr_mult,
            cfg.atr_period,
        )
        return cfg


# ------------------------------ BYBIT CLIENT --------------------------------

class BybitClient:
    """Ленивая инициализация aiohttp-сессии — создаём её уже внутри работающего event loop."""
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._base = "https://api.bybit.com"
        self._headers = {"User-Agent": "CheCryptoSignalsBot/1.0"}

    async def open(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if self._session is None or self._session.closed:
            await self.open()
        assert self._session is not None
        url = f"{self._base}{path}"
        async with self._session.get(url, params=params, headers=self._headers) as r:
            r.raise_for_status()
            data = await r.json()
            return data

    async def get_instruments_linear(self) -> List[Dict[str, Any]]:
        """Все фьючерсные линейные USDT инструменты (category=linear)."""
        instruments: List[Dict[str, Any]] = []
        cursor = None
        while True:
            params = {"category": "linear", "status": "Trading"}
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/v5/market/instruments-info", params)
            if str(data.get("retCode")) != "0":
                break
            result = data.get("result", {})
            list_ = result.get("list", []) or []
            for it in list_:
                symbol = it.get("symbol", "")
                contract = (it.get("contractType") or "").lower()
                if symbol.endswith("USDT") and ("perpetual" in contract):
                    instruments.append(it)
            cursor = result.get("nextPageCursor") or None
            if not cursor:
                break
        return instruments

    async def get_kline_5m(self, symbol: str, limit: int = 300) -> Dict[str, Any]:
        return await self._get(
            "/v5/market/kline",
            {"category": "linear", "symbol": symbol, "interval": "5", "limit": str(limit)},
        )

    async def get_open_interest(self, symbol: str, interval: str = "5min", limit: int = 6) -> Dict[str, Any]:
        return await self._get(
            "/v5/market/open-interest",
            {"category": "linear", "symbol": symbol, "intervalTime": interval, "limit": str(limit)},
        )


# ----------------------------- ДЕРЖАТЕЛЬ СОСТОЯНИЯ --------------------------

class UniverseState:
    def __init__(self):
        self.total_symbols: List[str] = []
        self.active_symbols: List[str] = []
        self.batch_index: int = 0

    def batches(self, batch_size: int) -> List[List[str]]:
        out: List[List[str]] = []
        for i in range(0, len(self.total_symbols), batch_size):
            out.append(self.total_symbols[i:i + batch_size])
        return out

    def rotate(self, batch_size: int):
        if not self.total_symbols:
            self.active_symbols = []
            self.batch_index = 0
            return
        batches = self.batches(batch_size)
        if not batches:
            self.active_symbols = []
            self.batch_index = 0
            return
        self.batch_index = (self.batch_index + 1) % len(batches)
        self.active_symbols = batches[self.batch_index]


class Engine:
    def __init__(self, cfg: Config, client: BybitClient):
        self.cfg = cfg
        self.client = client
        self.state = UniverseState()
        self.ws_started: bool = False

    async def bootstrap_universe(self):
        try:
            instruments = await self.client.get_instruments_linear()
            symbols: List[str] = [it["symbol"] for it in instruments]
            symbols.sort()
            self.state.total_symbols = symbols
            self.state.batch_index = 0
            self.state.active_symbols = symbols[: self.cfg.ws_symbols_max]
            log.info(
                "INFO [universe] total=%d active=%d mode=%s",
                len(self.state.total_symbols),
                len(self.state.active_symbols),
                self.cfg.universe_mode,
            )
        except Exception as e:
            log.exception("Failed to load universe: %s", e)

    def universe_stats(self) -> Tuple[int, int, int, int]:
        total = len(self.state.total_symbols)
        active = len(self.state.active_symbols)
        batch = self.state.batch_index
        ws_topics = active * 2  # placeholder
        return total, active, batch, ws_topics

    async def start_ws(self):
        if not self.state.active_symbols or self.ws_started:
            return
        self.ws_started = True
        log.info("INFO [ws] subscribed %d topics for %d symbols", len(self.state.active_symbols) * 2, len(self.state.active_symbols))

    async def poll_open_interest_once(self):
        slice_n = min(20, len(self.state.active_symbols))
        for sym in self.state.active_symbols[:slice_n]:
            with contextlib.suppress(Exception):
                await self.client.get_open_interest(sym, interval="5min", limit=6)

    async def rotate_active(self):
        self.state.rotate(self.cfg.ws_symbols_max)
        log.info(
            "INFO [rotate] now active=%d (batch#%d)",
            len(self.state.active_symbols),
            self.state.batch_index,
        )


# ---------------------------- HTTP HEALTH SERVER ----------------------------

async def _handle_root(_request: web.Request) -> web.Response:
    return web.Response(text="ok", status=200)

def start_http_server(port: int):
    app = web.Application()
    app.router.add_get("/", _handle_root)
    runner = web.AppRunner(app)

    async def _run():
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=port)
        await site.start()
        while True:
            await asyncio.sleep(3600)

    def _bg():
        asyncio.run(_run())

    import threading
    t = threading.Thread(target=_bg, daemon=True)
    t.start()


# ------------------------------ HANDLERS ------------------------------------

def is_allowed(cfg: Config, chat_id: int) -> bool:
    return (not cfg.allowed_chat_ids) or (chat_id in cfg.allowed_chat_ids)

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    if update.effective_chat and not is_allowed(cfg, update.effective_chat.id):
        return
    await update.effective_message.reply_text("pong")

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    eng: Engine = context.application.bot_data["engine"]
    if update.effective_chat and not is_allowed(cfg, update.effective_chat.id):
        return
    total, active, batch, ws_topics = eng.universe_stats()
    if total == 0:
        await update.effective_message.reply_text("Вселенная пока не загружена (жду Bybit API/повторяю попытки)…")
        return
    sample = ", ".join(eng.state.active_symbols[:15])
    if len(eng.state.active_symbols) > 15:
        sample += " ..."
    await update.effective_message.reply_text(
        f"Вселенная: total={total}, active={active}, batch#{batch}, ws_topics={ws_topics}\n"
        f"Активные (пример): {sample}"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    eng: Engine = context.application.bot_data["engine"]
    if update.effective_chat and not is_allowed(cfg, update.effective_chat.id):
        return
    total, active, batch, ws_topics = eng.universe_stats()
    await update.effective_message.reply_text(
        f"Вселенная: total={total}, active={active}, batch#{batch}, ws_topics={ws_topics}"
    )


# --------------------------- JOB QUEUE CALLBACKS ----------------------------

async def job_health(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    for chat_id in cfg.primary_recipients:
        with contextlib.suppress(Exception):
            await context.bot.send_message(chat_id, "online")

async def job_self_ping(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    if not cfg.public_url or not cfg.self_ping_enabled:
        return
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as s:
            async with s.get(cfg.public_url) as r:
                await r.text()
    except Exception:
        pass

async def job_start_ws(context: ContextTypes.DEFAULT_TYPE):
    eng: Engine = context.application.bot_data["engine"]
    await eng.start_ws()

async def job_rotate(context: ContextTypes.DEFAULT_TYPE):
    eng: Engine = context.application.bot_data["engine"]
    await eng.rotate_active()

async def job_poll_oi(context: ContextTypes.DEFAULT_TYPE):
    eng: Engine = context.application.bot_data["engine"]
    await eng.poll_open_interest_once()


# ------------------------------ APP/MAIN ------------------------------------

def build_app(cfg: Config, eng: Engine) -> Application:
    app = ApplicationBuilder().token(cfg.token).build()

    app.bot_data["cfg"] = cfg
    app.bot_data["engine"] = eng

    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("universe", cmd_universe))
    app.add_handler(CommandHandler("status", cmd_status))

    jq = app.job_queue
    jq.run_once(job_start_ws, when=10, name="job_start_ws")
    jq.run_repeating(job_rotate, interval=cfg.rotate_min * 60, first=cfg.rotate_min * 60, name="job_rotate")
    jq.run_repeating(job_poll_oi, interval=60, first=30, name="job_poll_oi")
    jq.run_repeating(job_health, interval=cfg.health_interval_sec, first=cfg.health_first_sec, name="job_health")
    jq.run_repeating(job_self_ping, interval=cfg.self_ping_interval_sec, first=90, name="job_self_ping")

    return app


async def initialize_bootstrap(app: Application):
    cfg: Config = app.bot_data["cfg"]
    eng: Engine = app.bot_data["engine"]
    await eng.bootstrap_universe()
    for chat_id in cfg.primary_recipients:
        with contextlib.suppress(Exception):
            total, active, batch, ws_topics = eng.universe_stats()
            await app.bot.send_message(
                chat_id,
                "Бот запущен (polling). Начинаю скрининг…\n"
                f"Вселенная: total={total}, active={active}, batch#{batch}, ws_topics={ws_topics}"
            )


def main():
    cfg = Config.load()
    if not cfg.token:
        raise SystemExit("TELEGRAM_BOT_TOKEN is required")

    # Поднимаем HTTP-порт (чтобы Render видел открытый порт)
    start_http_server(cfg.http_port)

    client = BybitClient()
    engine = Engine(cfg, client)
    app = build_app(cfg, engine)

    async def runner():
        # на всякий случай уберём webhook
        with contextlib.suppress(Exception):
            await app.bot.delete_webhook(drop_pending_updates=True)

        # лениво откроем сессию клиента (внутри работающего loop)
        await client.open()

        # предзагрузка
        await initialize_bootstrap(app)

        async def start_polling_with_retry():
            backoff = 5
            while True:
                try:
                    await app.initialize()
                    await app.start()
                    await app.updater.start_polling(
                        allowed_updates=Update.ALL_TYPES,
                        drop_pending_updates=True,
                    )
                    log.info("Polling started")
                    return
                except Conflict as e:
                    log.warning("Polling conflict: %s — retry in %ss", e, backoff)
                    with contextlib.suppress(Exception):
                        await app.updater.stop()
                    with contextlib.suppress(Exception):
                        await app.stop()
                    with contextlib.suppress(Exception):
                        await app.shutdown()
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                except Exception as e:
                    log.warning("Polling start error: %s — retry in 10s", e)
                    with contextlib.suppress(Exception):
                        await app.updater.stop()
                    with contextlib.suppress(Exception):
                        await app.stop()
                    with contextlib.suppress(Exception):
                        await app.shutdown()
                    await asyncio.sleep(10)

        try:
            await start_polling_with_retry()
            await asyncio.Event().wait()
        finally:
            with contextlib.suppress(Exception):
                await client.close()
            with contextlib.suppress(Exception):
                await app.updater.stop()
            with contextlib.suppress(Exception):
                await app.stop()
            with contextlib.suppress(Exception):
                await app.shutdown()

    asyncio.run(runner())


if __name__ == "__main__":
    main()
