#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CheCryptoSignalsBot — стабильный каркас с ротацией фьючерсных монет Bybit,
периодическими задачами и расширенной "online"-проверкой.

Основное:
- Если PUBLIC_URL начинается с https — ставим webhook, иначе работаем в polling.
- Вселенная = все USDT linear perpetual (category=linear, quote=USDT, status=Trading).
- Активный набор для "мониторинга" = WS_SYMBOLS_MAX, ротация каждые ROTATE_MIN минут.
- /ping, /universe, /status, /health
- "online" отправляется каждые HEALTH_INTERVAL_SEC (первый через HEALTH_FIRST_SEC),
  ошибки отправки логируются — видно, почему не дошло.
- Оставлены заглушки для генерации сигналов (compute_signals), фильтры можно развивать.

Требуемые env:
- TELEGRAM_TOKEN (обязательно)
- TELEGRAM_CHAT_ID   (основные получатели; можно список через запятую)
- TELEGRAM_ALLOWED_CHAT_IDS (whitelist чатов; можно список через запятую; если пусто —
  разрешены только PRIMARY_RECIPIENTS)
- PUBLIC_URL (опционально; если пусто/не https — будет polling)
- PORT (по умолчанию 10000 — для Render)
- HEALTH_INTERVAL_SEC (по умолчанию 1200)
- HEALTH_FIRST_SEC (по умолчанию 60)
- SELF_PING (true/false — пинговать PUBLIC_URL раз в 780с для удержания free-инстанса)
- SIGNAL_COOLDOWN_SEC, SIGNAL_TTL_MIN, UNIVERSE_MODE, UNIVERSE_TOP_N, WS_SYMBOLS_MAX,
  ROTATE_MIN, PROB_MIN, PROFIT_MIN_PCT, RR_MIN — см. Config ниже.
"""

from __future__ import annotations
import os
import re
import ssl
import json
import time
import math
import random
import asyncio
import logging
import contextlib
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple

import aiohttp
import pandas as pd

from telegram import Update
from telegram.ext import (
    Application,
    AIORateLimiter,
    CommandHandler,
    ContextTypes,
)

# ---------------------- Логирование ----------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------- Конфиг ----------------------
def _parse_id_list(val: str | None) -> List[int]:
    if not val:
        return []
    parts = re.split(r"[,\s;]+", val.strip())
    out: List[int] = []
    for p in parts:
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            pass
    return out

def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default

def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

@dataclass
class Config:
    token: str
    primary_recipients: List[int]
    allowed_chat_ids: List[int]

    public_url: str = os.getenv("PUBLIC_URL", "").strip()
    port: int = _env_int("PORT", 10000)

    # health / keepalive
    health_interval_sec: int = _env_int("HEALTH_INTERVAL_SEC", 1200)
    health_first_sec: int = _env_int("HEALTH_FIRST_SEC", 60)
    self_ping_enabled: bool = _env_bool("SELF_PING", True)
    self_ping_interval_sec: int = _env_int("SELF_PING_INTERVAL_SEC", 780)

    # сигналы/фильтры (пока заглушки используются частично)
    signal_cooldown_sec: int = _env_int("SIGNAL_COOLDOWN_SEC", 600)
    signal_ttl_min: int = _env_int("SIGNAL_TTL_MIN", 12)
    universe_mode: str = os.getenv("UNIVERSE_MODE", "all").strip()  # all|top|custom (пока all)
    universe_top_n: int = _env_int("UNIVERSE_TOP_N", 15)
    ws_symbols_max: int = _env_int("WS_SYMBOLS_MAX", 60)
    rotate_min: int = _env_int("ROTATE_MIN", 5)

    prob_min: float = _env_float("PROB_MIN", 69.9)
    profit_min_pct: float = _env_float("PROFIT_MIN_PCT", 1.0)
    rr_min: float = _env_float("RR_MIN", 2.0)

    # триггеры (примерные параметры)
    vol_mult: float = _env_float("VOL_MULT", 2.0)
    vol_sma_period: int = _env_int("VOL_SMA_PERIOD", 20)
    body_atr_mult: float = _env_float("BODY_ATR_MULT", 0.6)
    atr_period: int = _env_int("ATR_PERIOD", 14)

    @classmethod
    def load(cls) -> "Config":
        token = os.getenv("TELEGRAM_TOKEN", "").strip()
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN is required")

        primary = _parse_id_list(os.getenv("TELEGRAM_CHAT_ID"))
        allowed = _parse_id_list(os.getenv("TELEGRAM_ALLOWED_CHAT_IDS"))

        # Если явного allowlist нет — разрешим как минимум primary
        if not allowed:
            allowed = list(primary)

        cfg = cls(token=token, primary_recipients=primary, allowed_chat_ids=allowed)
        log.info(
            "INFO [cfg] ALLOWED_CHAT_IDS=%s", cfg.allowed_chat_ids
        )
        log.info(
            "INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.primary_recipients
        )
        log.info(
            "INFO [cfg] PUBLIC_URL='%s' PORT=%s",
            cfg.public_url, cfg.port
        )
        log.info(
            "INFO [cfg] HEALTH=%ss FIRST=%ss SELF_PING=%s/%ss",
            cfg.health_interval_sec, cfg.health_first_sec,
            cfg.self_ping_enabled, cfg.self_ping_interval_sec
        )
        log.info(
            "INFO [cfg] SIGNAL_COOLDOWN_SEC=%s SIGNAL_TTL_MIN=%s "
            "UNIVERSE_MODE=%s UNIVERSE_TOP_N=%s WS_SYMBOLS_MAX=%s ROTATE_MIN=%s "
            "PROB_MIN>%s PROFIT_MIN_PCT>=%s%% RR_MIN>=%s",
            cfg.signal_cooldown_sec, cfg.signal_ttl_min,
            cfg.universe_mode, cfg.universe_top_n, cfg.ws_symbols_max, cfg.rotate_min,
            cfg.prob_min, cfg.profit_min_pct, cfg.rr_min
        )
        log.info(
            "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, "
            "BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
            cfg.vol_mult, cfg.vol_sma_period, cfg.body_atr_mult, cfg.atr_period
        )
        return cfg

def is_allowed(cfg: Config, chat_id: int) -> bool:
    return chat_id in cfg.allowed_chat_ids or chat_id in cfg.primary_recipients

# ---------------------- Bybit API ----------------------
class BybitClient:
    BASE = "https://api.bybit.com"

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_universe_linear_usdt(self) -> List[str]:
        """
        Все фьючерсные USDT perpetual (linear), статус Trading.
        """
        s = await self._ensure_session()
        url = f"{self.BASE}/v5/market/instruments-info"
        params = {"category": "linear"}
        out = []
        async with s.get(url, params=params) as r:
            r.raise_for_status()
            data = await r.json()
        if data.get("retCode") != 0:
            log.warning("Bybit instruments error: %s", data)
            return out
        items = data.get("result", {}).get("list", []) or []
        for it in items:
            if it.get("quoteCoin") == "USDT" and it.get("status") == "Trading":
                sym = it.get("symbol")
                if sym and sym.endswith("USDT"):
                    out.append(sym)
        return sorted(set(out))

    async def get_open_interest(self, symbol: str, interval: str = "5min", limit: int = 6) -> List[Tuple[int, float]]:
        s = await self._ensure_session()
        url = f"{self.BASE}/v5/market/open-interest"
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval, "limit": str(limit)}
        async with s.get(url, params=params) as r:
            r.raise_for_status()
            data = await r.json()
        if data.get("retCode") != 0:
            log.debug("Bybit OI error: %s", data)
            return []
        rows = data.get("result", {}).get("list", []) or []
        # rows: [ [timestamp(ms), oi], ... ]
        out: List[Tuple[int, float]] = []
        for row in rows:
            try:
                ts = int(row[0])
                oi = float(row[1])
                out.append((ts, oi))
            except Exception:
                continue
        return out

    async def get_kline(self, symbol: str, interval: str = "5", limit: int = 300) -> pd.DataFrame:
        s = await self._ensure_session()
        url = f"{self.BASE}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)}
        async with s.get(url, params=params) as r:
            r.raise_for_status()
            data = await r.json()
        if data.get("retCode") != 0:
            log.debug("Bybit kline error: %s", data)
            return pd.DataFrame()
        rows = data.get("result", {}).get("list", []) or []
        # Bybit возвращает newest first — развернём:
        rows = rows[::-1]
        cols = ["start", "open", "high", "low", "close", "volume", "turnover"]
        parsed = []
        for row in rows:
            try:
                parsed.append([
                    int(row[0]), float(row[1]), float(row[2]), float(row[3]),
                    float(row[4]), float(row[5]), float(row[6]),
                ])
            except Exception:
                pass
        df = pd.DataFrame(parsed, columns=cols)
        return df

# ---------------------- Движок ----------------------
class Engine:
    def __init__(self, cfg: Config, client: BybitClient):
        self.cfg = cfg
        self.client = client
        self.total_symbols: List[str] = []
        self.active_symbols: List[str] = []
        self.batch_idx: int = 0
        self.ws_topics: int = 0  # счётчик подписок (для вида)
        self.last_rotate_ts: float = 0.0

    async def bootstrap(self):
        # Подтянуть вселенную
        try:
            universe = await self.client.get_universe_linear_usdt()
        except Exception as e:
            log.error("bootstrap: failed to load universe: %s", e)
            universe = []
        self.total_symbols = universe
        # Первичная активация
        self._rotate_active(first_time=True)
        log.info("INFO [universe] total=%d active=%d mode=%s", len(self.total_symbols), len(self.active_symbols), self.cfg.universe_mode)

    def _rotate_active(self, first_time: bool = False):
        if not self.total_symbols:
            self.active_symbols = []
            self.ws_topics = 0
            return
        # простая ротация по батчам
        n = self.cfg.ws_symbols_max
        if n <= 0:
            n = 30
        total = len(self.total_symbols)
        start = (self.batch_idx * n) % total
        end = min(start + n, total)
        if end - start < n and end == total:
            # "хвост" + "начало"
            remain = n - (end - start)
            batch = self.total_symbols[start:end] + self.total_symbols[0:remain]
        else:
            batch = self.total_symbols[start:end]
        self.active_symbols = batch
        self.ws_topics = len(self.active_symbols) * 2  # к примеру: trades + tickers
        if not first_time:
            self.batch_idx += 1
        self.last_rotate_ts = time.time()
        if first_time:
            sample = ", ".join(self.active_symbols[:15]) + (" ..." if len(self.active_symbols) > 15 else "")
            log.info("INFO [universe] active init=%d, sample=%s", len(self.active_symbols), sample)

    async def job_rotate(self, context: ContextTypes.DEFAULT_TYPE):
        self._rotate_active()
        log.debug("rotate -> batch#%d, active=%d", self.batch_idx, len(self.active_symbols))

    async def job_poll_oi(self, context: ContextTypes.DEFAULT_TYPE):
        # Просто трогаем OI для нескольких активных — как keep-warm и задел для анализа.
        sample = self.active_symbols[:10]
        for sym in sample:
            with contextlib.suppress(Exception):
                await self.client.get_open_interest(sym, interval="5min", limit=6)

    def universe_stats(self) -> Tuple[int, int, int, int]:
        return len(self.total_symbols), len(self.active_symbols), self.batch_idx, self.ws_topics

    # Заглушка — сюда будете добавлять реальную логику SMC/объём/OI/ликвидации и т.д.
    async def compute_signals(self) -> List[Dict[str, Any]]:
        # Верните список сигналов; здесь просто пусто
        return []

# ---------------------- Команды ----------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    sample = ", ".join(eng.active_symbols[:15]) + (" ..." if len(eng.active_symbols) > 15 else "")
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

async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    eng: Engine = context.application.bot_data["engine"]
    if update.effective_chat and not is_allowed(cfg, update.effective_chat.id):
        return
    total, active, batch, ws_topics = eng.universe_stats()
    msg = (f"online\n"
           f"Вселенная: total={total}, active={active}, batch#{batch}, ws_topics={ws_topics}\n"
           f"HEALTH: every {cfg.health_interval_sec}s (first {cfg.health_first_sec}s)")
    await update.effective_message.reply_text(msg)
    # продублируем в основные чаты
    for chat_id in cfg.primary_recipients:
        if update.effective_chat and chat_id == update.effective_chat.id:
            continue
        with contextlib.suppress(Exception):
            await context.bot.send_message(chat_id, "online (manual)")

# ---------------------- Jobs: health & keepalive ----------------------
async def job_health(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    for chat_id in cfg.primary_recipients:
        try:
            await context.bot.send_message(chat_id, "online")
        except Exception as e:
            log.warning("health send failed -> chat_id=%s: %s", chat_id, e)

async def job_self_ping(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    if not cfg.public_url:
        return
    # Пингуем корень — Render free держим бодрым
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6)) as s:
            async with s.get(cfg.public_url, allow_redirects=True) as r:
                _ = await r.text()
        log.debug("self-ping ok")
    except Exception as e:
        log.debug("self-ping failed: %s", e)

# ---------------------- Инициализация/Запуск ----------------------
async def post_init(application: Application, cfg: Config):
    # Создаём клиент/движок только внутри event-loop
    client = BybitClient()
    engine = Engine(cfg, client)
    application.bot_data["cfg"] = cfg
    application.bot_data["engine"] = engine
    application.bot_data["bybit"] = client

    # Bootstrap вселенной
    await engine.bootstrap()

    # Планировщик PTB (надёжно работает в одном loop)
    jq = application.job_queue
    # ротация
    jq.run_repeating(engine.job_rotate, interval=cfg.rotate_min * 60, first=30)
    # опциональный опрос OI
    jq.run_repeating(engine.job_poll_oi, interval=60, first=35)
    # health
    jq.run_repeating(job_health, interval=cfg.health_interval_sec, first=cfg.health_first_sec)
    # self-ping
    if cfg.self_ping_enabled:
        jq.run_repeating(job_self_ping, interval=cfg.self_ping_interval_sec, first=90)

def build_application(cfg: Config) -> Application:
    app = (
        Application.builder()
        .token(cfg.token)
        .rate_limiter(AIORateLimiter())
        .build()
    )

    # handlers
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("universe", cmd_universe))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("health", cmd_health))

    return app

async def main_async():
    cfg = Config.load()
    app = build_application(cfg)

    # post init (внутри уже запущенного приложения)
    await post_init(app, cfg)

    # Режим запуска: webhook если есть https PUBLIC_URL, иначе polling
    use_webhook = bool(cfg.public_url and cfg.public_url.lower().startswith("https"))
    if use_webhook:
        # Webhook путь фиксируем, чтобы был стабильный URL, но можно и корень
        webhook_path = os.getenv("WEBHOOK_PATH", f"/wh-{random.randint(10_000_000, 99_999_999)}")
        webhook_url = cfg.public_url.rstrip("/") + webhook_path
        # Telegram требует именно https URL
        log.info("Starting webhook at %s (port %s)", webhook_url, cfg.port)
        await app.bot.delete_webhook(drop_pending_updates=True)
        await app.bot.set_webhook(url=webhook_url, allowed_updates=Update.ALL_TYPES)
        # run_webhook в PTB 21.6 сам блокирует до остановки
        await app.run_webhook(
            listen="0.0.0.0",
            port=cfg.port,
            url_path=webhook_path,
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        # Надёжный fallback — polling (без конфликтов, если единственный инстанс).
        log.info("PUBLIC_URL not https — using polling mode")
        await app.bot.delete_webhook(drop_pending_updates=True)
        await app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            poll_interval=1.0,
            timeout=10
        )

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
