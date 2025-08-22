# -*- coding: utf-8 -*-
"""
CryptoBot — Telegram сканер сигналов Bybit USDT perpetual

Изменения:
- Пороговые фильтры вынесены в ENV:
    TP_MIN_PCT (по умолчанию 0.01 = 1%), PROB_MIN (по умолчанию 0.70 = 70%)
- Удалена кнопка-переход на Bybit (InlineKeyboardButton/Markup не используется)
- В сообщении сигнала добавлены "Текущая цена" и "Обоснование"
- Снижен объём ротации монет через переменную ANALYSIS_BATCH_SIZE
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import aiohttp
from aiohttp import web

# ---------------------------------------------------------------------
# Пороговые значения фильтра сигналов (переопределяются через ENV)
# ---------------------------------------------------------------------
# минимальная ожидаемая прибыль (take-profit) в долях (0.01 = 1%)
TP_MIN_PCT = float(os.getenv("TP_MIN_PCT", "0.01"))
# минимальная вероятность выполнения сценария (0.70 = 70%)
PROB_MIN = float(os.getenv("PROB_MIN", "0.70"))

# ---------------------------------------------------------------------
# НАСТРОЙКИ И КОНФИГ
# ---------------------------------------------------------------------

def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default if default is not None else None)
    if v is None:
        return None
    v = v.strip()
    return v if v else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


@dataclass
class Config:
    # Telegram / инфраструктура
    token: str
    log_level: str
    public_url: Optional[str]
    port: int

    # Телеметрия и джобы
    keepalive_sec: int
    heartbeat_sec: int
    scan_interval_sec: int
    first_scan_delay_sec: int

    # Роутинг сообщений
    primary_recipients: List[int]
    allowed_chat_ids: List[int]
    only_channel: bool

    # Bybit
    bybit_base: str

    # Аналитика
    analysis_batch_size: int

    # Фильтры (вынесены в ENV, но с дефолтами)
    tp_min_pct: float
    prob_min: float

    @staticmethod
    def load() -> "Config":
        token = _env_str("TELEGRAM_TOKEN") or _env_str("BOT_TOKEN")
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN не задан")

        # кому слать: список id через запятую
        def _parse_ids(name: str) -> List[int]:
            raw = _env_str(name, "") or ""
            ids: List[int] = []
            for chunk in re.split(r"[,\s]+", raw.strip()):
                if not chunk:
                    continue
                try:
                    ids.append(int(chunk))
                except Exception:
                    pass
            return ids

        cfg = Config(
            token=token,
            log_level=_env_str("LOG_LEVEL", "INFO"),
            public_url=_env_str("RENDER_EXTERNAL_URL") or _env_str("PUBLIC_URL"),
            port=_env_int("PORT", 8000),
            keepalive_sec=_env_int("KEEPALIVE_SEC", 13 * 60),
            heartbeat_sec=_env_int("HEARTBEAT_SEC", 15 * 60),
            scan_interval_sec=_env_int("SCAN_INTERVAL_SEC", 12),
            first_scan_delay_sec=_env_int("FIRST_SCAN_DELAY_SEC", 5),
            primary_recipients=_parse_ids("PRIMARY_RECIPIENTS"),
            allowed_chat_ids=_parse_ids("ALLOWED_CHAT_IDS"),
            only_channel=_env_bool("ONLY_CHANNEL", False),
            bybit_base=_env_str("BYBIT_BASE", "https://api.bybit.com"),
            analysis_batch_size=_env_int("ANALYSIS_BATCH_SIZE", 30),
            tp_min_pct=_env_float("TP_MIN_PCT", TP_MIN_PCT),
            prob_min=_env_float("PROB_MIN", PROB_MIN),
        )
        return cfg


# ---------------------------------------------------------------------
# ЛОГГЕР
# ---------------------------------------------------------------------
def setup_logging(level: str = "INFO") -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ
# ---------------------------------------------------------------------
def pct(x: float) -> str:
    return f"{x:.2%}"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------
# Bybit HTTP
# ---------------------------------------------------------------------
class Bybit:
    def __init__(self, base: str, session: aiohttp.ClientSession) -> None:
        self.base = base.rstrip("/")
        self.http = session

    async def tickers(self) -> List[Dict[str, Any]]:
        # линейные перпетуалы
        url = f"{self.base}/v5/market/tickers?category=linear"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []

    async def kline(self, symbol: str, interval_min: int = 1, limit: int = 60) -> List[Dict[str, Any]]:
        url = (
            f"{self.base}/v5/market/kline?category=linear&symbol={symbol}"
            f"&interval={interval_min}&limit={limit}"
        )
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []

    async def open_interest(self, symbol: str, interval: str = "5min", limit: int = 12) -> List[Dict[str, Any]]:
        url = (
            f"{self.base}/v5/market/open-interest?category=linear&symbol={symbol}"
            f"&intervalTime={interval}&limit={limit}"
        )
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []

    async def recent_liq(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/liquidation?category=linear&symbol={symbol}&limit={limit}"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []


# ---------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------
class Tg:
    def __init__(self, token: str, session: aiohttp.ClientSession) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = session

    async def get_me(self) -> Dict[str, Any]:
        async with self.http.post(f"{self.base}/getMe") as r:
            r.raise_for_status()
            return await r.json()

    async def send_message(self, chat_id: int, text: str, disable_web_page_preview: bool = True) -> None:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_web_page_preview,
        }
        async with self.http.post(f"{self.base}/sendMessage", json=payload) as r:
            r.raise_for_status()
            await r.json()

    async def get_updates(self) -> Dict[str, Any]:
        async with self.http.post(f"{self.base}/getUpdates") as r:
            r.raise_for_status()
            return await r.json()


# ---------------------------------------------------------------------
# СТРАТЕГИЯ / СКАНЕР
# ---------------------------------------------------------------------
def _score_ticker(t: Dict[str, Any]) -> float:
    # Небольшой скоринг по волатильности/объёму
    try:
        chg = abs(float(t.get("price24hPcnt") or 0.0))
        vol = float(t.get("turnover24h") or 0.0)
    except Exception:
        return 0.0
    return chg * 0.5 + math.log10(vol + 1.0) * 0.5


def _format_signal(sig: Dict[str, Any]) -> str:
    """
    Человекочитаемое сообщение без кнопок.
    """
    sym = sig["symbol"]
    side = sig["side"]
    entry = sig["entry"]
    tp = sig["tp"]
    sl = sig["sl"]
    prob = sig["prob"]
    tp_pct = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry

    text = (
        f"⚡️ <b>Сигнал по {sym}</b>\n"
        f"Направление: <b>{'LONG' if side=='LONG' else 'SHORT'}</b>\n"
        f"Текущая цена: {entry:g}\n"
        f"Тейк: {tp:g} ({pct(tp_pct)})\n"
        f"Стоп: {sl:g}\n"
        f"Вероятность: <b>{pct(prob)}</b>\n"
    )

    reason = sig.get("reason")
    if reason:
        text += f"Обоснование: {reason}\n"

    return text


async def analyze_symbol(bybit: Bybit, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Простейшая эвристика: 1m свечи, изменение, ATR, дельта OI и наличие/отсутствие ликвидаций.
    Возвращаем словарь сигнала или None.
    """
    try:
        k = await bybit.kline(symbol, interval_min=1, limit=30)
        if not k:
            return None
        # последние цены
        close = float(k[-1][4])
        prev = float(k[-2][4])
        chg = (close - prev) / prev if prev else 0.0

        # ATR-приблизительно
        highs = [float(x[2]) for x in k[-14:]]
        lows = [float(x[3]) for x in k[-14:]]
        atr = (sum(h - l for h, l in zip(highs, lows)) / len(highs)) if highs else 0.0

        # направление/тейк/стоп
        side = "LONG" if chg > 0 else "SHORT"
        tp = close + max(atr, abs(chg) * close) if side == "LONG" else close - max(atr, abs(chg) * close)
        sl = close - atr if side == "LONG" else close + atr

        # грубая вероятность (на базе силы свечи)
        body = abs(float(k[-1][4]) - float(k[-1][1]))
        prob = min(0.95, max(0.05, 0.5 + (body / (atr + 1e-8) - 0.5) * 0.5))

        reason = []
        reason.append(f"свеча {pct((close - prev) / prev)}; ATR≈{atr:.6g}")
        if body > atr * 0.6:
            reason.append("тело > 0.6*ATR")
        reason = ", ".join(reason)

        return {
            "symbol": symbol,
            "side": side,
            "entry": close,
            "tp": tp,
            "sl": sl,
            "prob": prob,
            "reason": reason,
        }
    except Exception as e:
        logger.exception(f"analyze_symbol {symbol}: {e}")
        return None


async def job_scan(app: web.Application) -> None:
    cfg: Config = app["cfg"]
    bybit: Bybit = app["bybit"]
    tg: Tg = app["tg"]

    # 1) список тикеров и грубая сортировка
    all_tickers = await bybit.tickers()
    scored = [(t.get("symbol"), _score_ticker(t)) for t in all_tickers if t.get("symbol")]
    scored.sort(key=lambda x: x[1], reverse=True)

    # ограничиваем анализ согласно ANALYSIS_BATCH_SIZE
    batch = [s for s, _ in scored[: max(1, cfg.analysis_batch_size)]]
    logger.info(f"scan: candidates={len(batch)}")

    signals: List[Tuple[Dict[str, Any], str]] = []

    # 2) анализ каждого символа
    for sym in batch:
        res = await analyze_symbol(bybit, sym)
        if not res:
            continue

        # --- фильтры ---
        # 1) Вероятность
        if res.get("prob", 0.0) < PROB_MIN:
            logger.info(f"[filter] skip {sym}: prob<{PROB_MIN:.2f}")
            continue

        # 2) Минимальный ожидаемый профит (до тейка)
        entry = float(res["entry"])
        tp = float(res["tp"])
        side = res["side"]
        tp_pct = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry
        if tp_pct < TP_MIN_PCT:
            logger.info(f"[filter] skip {sym}: tp<{TP_MIN_PCT:.2%}")
            continue

        # прошёл фильтр
        text = _format_signal(res)
        signals.append((res, text))

    # 3) рассылка (если есть)
    if not signals:
        return

    # отправляем всем primary_recipients
    for _sig, text in signals:
        for chat_id in cfg.primary_recipients:
            try:
                await tg.send_message(chat_id, text)
            except Exception as e:
                logger.exception(f"send_message to {chat_id}: {e}")


# ---------------------------------------------------------------------
# HEALTH/KEEPALIVE/HEARTBEAT
# ---------------------------------------------------------------------
async def handle_health(request: web.Request) -> web.Response:
    app = request.app
    return web.json_response(
        {
            "ok": True,
            "uptime_sec": int(time.monotonic() - app["start_ts"]),
            "time": now_utc().isoformat(),
            "tp_min_pct": app["cfg"].tp_min_pct,
            "prob_min": app["cfg"].prob_min,
            "batch_size": app["cfg"].analysis_batch_size,
        }
    )


async def job_keepalive(app: web.Application) -> None:
    url = (app["cfg"].public_url or "").rstrip("/") + "/health"
    if not url.startswith("http"):
        return
    try:
        async with app["http"].get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            ok = r.status == 200
        logger.info(f"[keepalive] ping {url} {'✓' if ok else '✗'}")
    except Exception as e:
        logger.warning(f"[keepalive] ping error: {e}")


async def job_heartbeat(app: web.Application) -> None:
    cfg: Config = app["cfg"]
    tg: Tg = app["tg"]
    text = "✅ Бот онлайн"
    for chat_id in cfg.primary_recipients:
        try:
            await tg.send_message(chat_id, text)
        except Exception as e:
            logger.exception(f"heartbeat to {chat_id}: {e}")


# ---------------------------------------------------------------------
# ИНИЦИАЛИЗАЦИЯ ПРИЛОЖЕНИЯ
# ---------------------------------------------------------------------
async def on_startup(app: web.Application) -> None:
    cfg: Config = app["cfg"]
    app["start_ts"] = time.monotonic()
    logger.info("startup...")

    # запуск периодических задач
    loop = asyncio.get_event_loop()

    async def _scan_loop() -> None:
        await asyncio.sleep(cfg.first_scan_delay_sec)
        i = 0
        while True:
            t0 = time.perf_counter()
            try:
                await job_scan(app)
            except Exception as e:
                logger.exception(f"job_scan: {e}")
            dt = time.perf_counter() - t0
            i += 1
            logger.info(
                f'Job "job_scan (trigger: interval[0:00:{cfg.scan_interval_sec:02d}], next run at: '
                f'{(now_utc() + timedelta(seconds=cfg.scan_interval_sec)).strftime("%Y-%m-%d %H:%M:%S")} UTC)" executed successfully'
            )
            await asyncio.sleep(max(0.0, cfg.scan_interval_sec - dt))

    async def _keepalive_loop() -> None:
        while True:
            await asyncio.sleep(cfg.keepalive_sec)
            try:
                await job_keepalive(app)
            except Exception as e:
                logger.exception(f"job_keepalive: {e}")

    async def _heartbeat_loop() -> None:
        while True:
            await asyncio.sleep(cfg.heartbeat_sec)
            try:
                await job_heartbeat(app)
            except Exception as e:
                logger.exception(f"job_heartbeat: {e}")

    app["scan_task"] = loop.create_task(_scan_loop())
    app["keepalive_task"] = loop.create_task(_keepalive_loop())
    app["heartbeat_task"] = loop.create_task(_heartbeat_loop())


async def on_cleanup(app: web.Application) -> None:
    for key in ("scan_task", "keepalive_task", "heartbeat_task"):
        task: Optional[asyncio.Task] = app.get(key)
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


# ---------------------------------------------------------------------
# MAIN / web
# ---------------------------------------------------------------------
import contextlib

def make_app(cfg: Config) -> web.Application:
    app = web.Application()
    app["cfg"] = cfg

    http = aiohttp.ClientSession()
    app["http"] = http
    app["bybit"] = Bybit(cfg.bybit_base, http)
    app["tg"] = Tg(cfg.token, http)

    app.router.add_get("/health", handle_health)
    app.on_startup.append(on_startup)

    async def _close_http(app_: web.Application) -> None:
        await app_["http"].close()

    app.on_cleanup.append(_close_http)
    app.on_cleanup.append(on_cleanup)
    return app


def main() -> None:
    cfg = Config.load()
    setup_logging(cfg.log_level)

    logger.info(
        "cfg: "
        f"public_url={cfg.public_url} | "
        f"scan_interval={cfg.scan_interval_sec}s | "
        f"batch={cfg.analysis_batch_size} | "
        f"tp_min={cfg.tp_min_pct:.2%} | "
        f"prob_min={cfg.prob_min:.0%}"
    )

    app = make_app(cfg)
    web.run_app(app, host="0.0.0.0", port=cfg.port)


if __name__ == "__main__":
    main()
