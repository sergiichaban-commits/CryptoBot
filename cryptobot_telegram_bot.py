#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CheCryptoSignalsBot — устойчивый запуск на Render:
- Webhook если PUBLIC_URL=https, иначе polling + tiny HTTP server на PORT (для порт-скана Render).
- Надёжная загрузка вселенной Bybit (USDT perpetual), авто-ретраи до успешной и ежеминутные догрузки.
- Команды: /ping, /status, /universe, /reload.
- Health-пинг "online" в чат каждые HEALTH_SEC; self-ping на PUBLIC_URL, чтобы не «засыпал» free-инстанс.

ENV:
  TELEGRAM_BOT_TOKEN (обяз.)
  ALLOWED_CHAT_IDS="-100...,533..."  (список разрешённых чатов)
  TELEGRAM_CHAT_ID                   (основной получатель; можно один из ALLOWED)
  PUBLIC_URL="https://..."           (для webhook; если пусто/не https — будет polling)
  PORT=10000
  SELF_PING_ENABLED=true|false
  SELF_PING_SEC=780
  HEALTH_SEC=1200
  ...и др. см. Config.from_env()
"""

import asyncio
import contextlib
import logging
import os
import random
import signal
import string
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import List, Optional
from urllib.parse import urlparse

import httpx
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    MessageHandler,
    filters,
)

# ========== ЛОГИ ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ========== УТИЛИТЫ ==========
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
    return f"{prefix}{''.join(rng.choice(string.digits) for _ in range(8))}"


def is_https_url(url: Optional[str]) -> bool:
    if not url:
        return False
    try:
        u = urlparse(url.strip())
        return u.scheme == "https" and bool(u.netloc)
    except Exception:
        return False


# ========== КОНФИГ ==========
@dataclass(frozen=True)
class Config:
    token: str
    allowed_chat_ids: List[int]
    primary_recipients: List[int]
    public_url: str
    port: int
    webhook_path: str

    # фильтры сигналов (оставлены как в ваших настройках)
    prob_min: float = 69.9
    profit_min_pct: float = 1.0
    rr_min: float = 2.0
    signal_cooldown_sec: int = 600

    # пинги / запуск
    health_sec: int = 1200
    health_first_sec: int = 60
    startup_delay_sec: int = 10
    self_ping_sec: int = 780
    self_ping_enabled: bool = True

    # вселенная + ротация
    universe_mode: str = "all"
    universe_top_n: int = 15
    ws_symbols_max: int = 60
    rotate_min: int = 5

    # плейсхолдеры под индикаторы
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
        primary = []
        cid_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        if cid_raw:
            with contextlib.suppress(Exception):
                cid = int(cid_raw)
                if not allowed or cid in allowed:
                    primary = [cid]
        if not primary and allowed:
            primary = [allowed[0]]

        public_url = os.environ.get("PUBLIC_URL", "").strip()
        port = int(os.environ.get("PORT", "10000"))
        wh_path = os.environ.get("WEBHOOK_PATH", "").strip() or gen_webhook_path()

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

        logger.info("INFO [cfg] ALLOWED_CHAT_IDS=%s", cfg.allowed_chat_ids)
        logger.info("INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.primary_recipients)
        logger.info("INFO [cfg] PUBLIC_URL=%r PORT=%d WEBHOOK_PATH=%r", cfg.public_url, cfg.port, cfg.webhook_path)
        logger.info(
            "INFO [cfg] HEALTH=%ss FIRST=%ss STARTUP=%ss SELF_PING=%s/%ss",
            cfg.health_sec, cfg.health_first_sec, cfg.startup_delay_sec,
            "True" if cfg.self_ping_enabled else "False", cfg.self_ping_sec
        )
        logger.info(
            "INFO [cfg] SIGNAL_COOLDOWN_SEC=%d SIGNAL_TTL_MIN=%d UNIVERSE_MODE=%s "
            "UNIVERSE_TOP_N=%d WS_SYMBOLS_MAX=%d ROTATE_MIN=%d PROB_MIN>%.1f "
            "PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
            cfg.signal_cooldown_sec, 12, cfg.universe_mode, cfg.universe_top_n,
            cfg.ws_symbols_max, cfg.rotate_min, cfg.prob_min, cfg.profit_min_pct, cfg.rr_min
        )
        logger.info(
            "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
            cfg.vol_mult, cfg.vol_sma_period, cfg.body_atr_mult, cfg.atr_period
        )
        return cfg


# ========== BYBIT API ==========
class BybitClient:
    BASE = "https://api.bybit.com"

    def __init__(self):
        self.http = httpx.AsyncClient(base_url=self.BASE, timeout=15.0)

    async def close(self):
        await self.http.aclose()

    async def get_linear_instruments(self) -> List[str]:
        """USDT perpetual (category=linear, status=Trading)."""
        out: List[str] = []
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
            for item in result.get("list", []) or []:
                if item.get("status") == "Trading" and item.get("quoteCoin") == "USDT":
                    out.append(item["symbol"])
            cursor = (result.get("nextPageCursor") or "").strip() or None
            if not cursor:
                break
        return out

    async def get_open_interest(self, symbol: str, interval: str = "5min", limit: int = 4) -> Optional[float]:
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)}
        r = await self.http.get("/v5/market/open-interest", params=params)
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0:
            return None
        lst = data.get("result", {}).get("list") or []
        if not lst:
            return None
        try:
            return float(lst[-1][1])
        except Exception:
            return None


# ========== ДВИЖОК ==========
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
        self._boot_lock = asyncio.Lock()

    async def bootstrap(self, send_notice: bool = False):
        """Надёжная загрузка вселенной c ретраями."""
        async with self._boot_lock:
            # если уже загружено — просто обновим активный срез
            if self.state.total > 0 and self.state.symbols_all:
                self._rebuild_active_slice()
                return

            last_exc: Optional[Exception] = None
            for attempt in range(1, 6):  # до 5 попыток
                try:
                    syms = await self.client.get_linear_instruments()
                    if syms:
                        random.shuffle(syms)
                        self.state.symbols_all = syms
                        self.state.total = len(syms)
                        self._rebuild_active_slice()
                        logger.info("INFO [universe] total=%d active=%d mode=%s",
                                    self.state.total, self.state.active, self.cfg.universe_mode)
                        if send_notice and self.cfg.primary_recipients:
                            with contextlib.suppress(Exception):
                                await self.app.bot.send_message(
                                    chat_id=self.cfg.primary_recipients[0],
                                    text=f"Вселенная загружена: {self.state.total} инструментов ✅",
                                )
                        return
                    else:
                        last_exc = RuntimeError("Empty instruments list")
                except Exception as e:
                    last_exc = e
                    logger.warning("bootstrap attempt %d failed: %s", attempt, e)
                await asyncio.sleep(2 * attempt)

            # если сюда дошли — неудача
            logger.error("bootstrap failed after retries: %s", last_exc)
            if send_notice and self.cfg.primary_recipients:
                with contextlib.suppress(Exception):
                    await self.app.bot.send_message(
                        chat_id=self.cfg.primary_recipients[0],
                        text="Не удалось загрузить вселенную (попробую позже) ⚠️",
                    )

    def _rebuild_active_slice(self):
        per_symbol_topics = 2  # trades + tickers (условно)
        max_symbols = max(1, self.cfg.ws_symbols_max // per_symbol_topics)
        self.state.batch_index = 0
        self.state.symbols_active = self.state.symbols_all[:max_symbols]
        self.state.active = len(self.state.symbols_active)
        self.state.ws_topics = self.state.active * per_symbol_topics

    async def ensure_bootstrap(self, *_):
        """Ежеминутная попытка, пока вселенная не будет загружена."""
        if self.state.total == 0:
            await self.bootstrap(send_notice=False)

    async def rotate(self, *_):
        if not self.state.symbols_all:
            return
        per_symbol_topics = 2
        max_symbols = max(1, self.cfg.ws_symbols_max // per_symbol_topics)
        start = (self.state.batch_index * max_symbols) % max(1, self.state.total)
        self.state.batch_index += 1
        sl = (self.state.symbols_all + self.state.symbols_all)[start:start + max_symbols]
        self.state.symbols_active = sl
        self.state.active = len(sl)
        self.state.ws_topics = self.state.active * per_symbol_topics

    async def poll_oi(self, *_):
        if not self.state.symbols_active:
            return
        sample = random.sample(self.state.symbols_active, k=min(5, len(self.state.symbols_active)))
        for sym in sample:
            with contextlib.suppress(Exception):
                await self.client.get_open_interest(sym)  # прогрев

    async def send_health(self, *_):
        now = time.time()
        if now - getattr(self, "_last_health_ts", 0.0) < self.cfg.health_sec:
            return
        self._last_health_ts = now
        for chat_id in self.cfg.primary_recipients:
            with contextlib.suppress(Exception):
                await self.app.bot.send_message(chat_id=chat_id, text="online")

    async def self_ping(self, *_):
        if not self.cfg.self_ping_enabled or not is_https_url(self.cfg.public_url):
            return
        try:
            async with httpx.AsyncClient(timeout=10.0) as cli:
                await cli.get(self.cfg.public_url.rstrip("/"))
        except Exception:
            pass

    def status_text(self) -> str:
        st = self.state
        if st.total == 0:
            return "Вселенная пока не загружена (жду Bybit API/повторяю попытки)…"
        return f"Вселенная: total={st.total}, active={st.active}, batch#{st.batch_index}, ws_topics={st.ws_topics}"

    async def close(self):
        await self.client.close()


# ========== МИНИ HTTP СЕРВЕР ==========
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

    def log_message(self, *_):
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


# ========== ДОСТУП ==========
def is_allowed(cfg: Config, update: Update) -> bool:
    if not cfg.allowed_chat_ids:
        return True
    chat_id = update.effective_chat.id if update.effective_chat else None
    return chat_id in cfg.allowed_chat_ids


# ========== КОМАНДЫ ==========
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
    await update.effective_message.reply_text(engine.status_text())


async def cmd_universe(update: Update, context: CallbackContext):
    cfg: Config = context.application.bot_data["cfg"]
    if not is_allowed(cfg, update):
        return
    engine: Engine = context.application.bot_data["engine"]
    txt = engine.status_text()
    if engine.state.total > 0 and engine.state.symbols_active:
        sample = ", ".join(engine.state.symbols_active[:15])
        if sample:
            txt += f"\nАктивные (пример): {sample}" + (" ..." if len(engine.state.symbols_active) > 15 else "")
    await update.effective_message.reply_text(txt)


async def cmd_reload(update: Update, context: CallbackContext):
    cfg: Config = context.application.bot_data["cfg"]
    if not is_allowed(cfg, update):
        return
    engine: Engine = context.application.bot_data["engine"]
    await update.effective_message.reply_text("Перезагружаю вселенную…")
    await engine.bootstrap(send_notice=False)
    await update.effective_message.reply_text(engine.status_text())


# ========== post_init / post_shutdown ==========
async def post_init(app: Application):
    cfg: Config = app.bot_data["cfg"]
    engine: Engine = app.bot_data["engine"]

    # первая попытка загрузки + ретраи внутри
    await engine.bootstrap(send_notice=True)

    jq = app.job_queue
    # если не получилось — подстраховка каждую минуту до успеха
    jq.run_repeating(engine.ensure_bootstrap, interval=60, first=60)
    # ротация
    jq.run_repeating(engine.rotate, interval=cfg.rotate_min * 60, first=cfg.startup_delay_sec)
    # OI прогрев
    jq.run_repeating(engine.poll_oi, interval=60, first=cfg.startup_delay_sec + 5)
    # health в чат
    jq.run_repeating(engine.send_health, interval=cfg.health_sec, first=cfg.health_first_sec)
    # self-ping
    if cfg.self_ping_enabled and is_https_url(cfg.public_url):
        jq.run_repeating(engine.self_ping, interval=cfg.self_ping_sec, first=cfg.health_first_sec)


async def post_shutdown(app: Application):
    engine: Engine = app.bot_data.get("engine")
    if engine:
        with contextlib.suppress(Exception):
            await engine.close()


# ========== СБОРКА ПРИЛОЖЕНИЯ ==========
def build_application(cfg: Config) -> Application:
    app = (
        ApplicationBuilder()
        .token(cfg.token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.bot_data["cfg"] = cfg
    app.bot_data["engine"] = Engine(cfg, app)

    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("universe", cmd_universe))
    app.add_handler(CommandHandler("reload", cmd_reload))
    app.add_handler(MessageHandler(filters.COMMAND, cmd_ping))  # fallback на неизвестные команды

    return app


# ========== MAIN ==========
def main():
    cfg = Config.from_env()
    app = build_application(cfg)

    if is_https_url(cfg.public_url):
        webhook_url = f"{cfg.public_url.rstrip('/')}{cfg.webhook_path}"

        async def _pre_del():
            with contextlib.suppress(Exception):
                await app.bot.delete_webhook(drop_pending_updates=True)

        asyncio.run(_pre_del())
        logger.info("Starting in WEBHOOK mode @ %s", webhook_url)
        app.run_webhook(
            listen="0.0.0.0",
            port=cfg.port,
            url_path=cfg.webhook_path,
            webhook_url=webhook_url,
            stop_signals=None,
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        logger.info("PUBLIC_URL not https/empty — starting in POLLING mode")
        start_tiny_http_server_thread(cfg.port)

        async def _run():
            with contextlib.suppress(Exception):
                await app.bot.delete_webhook(drop_pending_updates=True)
            await app.initialize()
            await app.start()
            await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            logger.info("Polling started (fallback)")
            while True:
                await asyncio.sleep(3600)

        try:
            asyncio.run(_run())
        except KeyboardInterrupt:
            pass
        finally:
            async def _shutdown():
                with contextlib.suppress(Exception):
                    await app.updater.stop()
                with contextlib.suppress(Exception):
                    await app.stop()
                with contextlib.suppress(Exception):
                    await app.shutdown()

            with contextlib.suppress(Exception):
                asyncio.run(_shutdown())


if __name__ == "__main__":
    main()
