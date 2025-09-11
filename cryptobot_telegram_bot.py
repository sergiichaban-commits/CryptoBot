# -*- coding: utf-8 -*-
"""
Cryptobot — Telegram сигналы (Bybit V5 WebSocket)
v3.0 (скальпинг + стакан/плотности + уровни + тренд + зоны объёма)

Что добавлено по ТЗ:
- Автонастройка и мониторинг стакана (L2 50) с snapshot+diff, адаптивный ladder-step, OBI, стенки (liquidity walls)
- Базовая стратегия от плотностей: вход перед зоной, SL за зоной (buffer), TP/ RR контроль
- Горизонтальные уровни: пробой/ретест/ложный
- Торговля по тренду (контекст 5m; фон 1h/4h)
- Зоны объёма и их «защита» (импульс → возврат → отскок)
- Осторожные контртренды с пометкой [Контртренд]
- Динамический фильтр котировок: оборот ≥150M USD и |изменение| ≥1% (+ белый список CORE_SYMBOLS)
- Авто-обновление универса каждые 10 минут (subscribe/unsubscribe без рестарта)
- Фильтр «ожидаемый ход ≥1%» (было; сохранено)

Примечания:
- Сигналы публикуются ТОЛЬКО когда все фильтры пройдены (включая стакан).
- При отсутствии валидного стакана по символу — сигнал не будет отправлен (скальпинг завязан на spread/OBI/стенки).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Iterable, Set

import aiohttp
from aiohttp import web

# =========================
# Параметры
# =========================
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL = "INFO"

# --- РЕЖИМ КОРОТКИХ СДЕЛОК ---
INTRADAY_SCALP = True
TIMEBOX_MINUTES = 90
TIMEBOX_FACTOR = 0.50

# Символы и фильтры (TURBO)
ACTIVE_SYMBOLS = 80
TP_MIN_PCT = 0.004              # 0.4% (внутренняя нижняя планка)
TP_MAX_PCT = 0.025              # 2.5% лимит для таймбокса
MIN_PROFIT_PCT = 0.010          # ≥1% — фильтр публикации
ATR_PERIOD_1M = 14
BODY_ATR_MULT = 0.30
VOL_SMA_PERIOD = 20
VOL_MULT = 1.00
SIGNAL_COOLDOWN_SEC = 10

# SMC / уровни / тренд
SWING_FRAC = 2
USE_FVG = True
USE_SWEEP = True
RR_TARGET = 1.10
USE_5M_FILTER = True
ALIGN_5M_STRICT = False
BOS_FRESH_BARS = 12

# Momentum (турбо)
MOMENTUM_N_BARS = 3
MOMENTUM_MIN_PCT = 0.0025
BODY_ATR_MOMO = 0.50
MOMENTUM_PROB_BONUS = 0.08

# Pending/Retest
PENDING_EXPIRE_BARS = 20
RETEST_WICK_PCT = 0.15
PENDING_BODY_ATR_MIN = 0.15
PENDING_VOL_MULT_MIN = 0.85

# VWAP
VWAP_WINDOW = 60
VWAP_SLOPE_BARS = 10
VWAP_SLOPE_MIN = 0.0
VWAP_TOLERANCE = 0.002

# Анти-истощение
EXT_RUN_BARS = 5
EXT_RUN_ATR_MULT = 2.2

# Equal highs/lows guard
EQL_LOOKBACK = 30
EQL_EPS_PCT = 0.0007
EQL_PROX_PCT = 0.0012

# Вероятность
PROB_THRESHOLDS = {
    "retest-OB": 0.46,
    "retest-FVG": 0.50,
    "clean-pass": 0.54,
    "momo-pass": 0.56,
    "BounceDensity": 0.56,
    "default": 0.50
}
USE_PROB_70_STRICT = False
PROB_THRESHOLDS_STRICT = {
    "retest-OB": 0.66,
    "retest-FVG": 0.68,
    "clean-pass": 0.70,
    "momo-pass": 0.72,
    "BounceDensity": 0.70,
    "default": 0.70
}
# АДАПТИВ
ADAPTIVE_SILENCE_MINUTES = 30
ADAPT_BODY_ATR = 0.24
ADAPT_VOL_MULT = 0.98
ADAPT_PROB_DELTA = -0.08

# Стакан/плотности
LADDER_BPS_DEFAULT = 0.0004     # 4 bps = 0.04%
LADDER_BPS_ALT = 0.0008         # для волатильных альтов
ORDERBOOK_DEPTH_BINS = 5        # для OBI
WALL_PCTL = 95                  # p95 ноушнл за lookback
WALL_LOOKBACK_SEC = 30 * 60     # 30 минут
SPREAD_P25_LOOKBACK_SEC = 10 * 60
SPREAD_TICKS_CAP = 8            # safety cap на spready в тиках
OPPOSITE_WALL_NEAR_TICKS = 6    # «слишком близкая» встречная стена

# Фильтр котировок (динамический)
UNIVERSE_REFRESH_SEC = 600
TURNOVER_MIN_USD = 150_000_000.0
CHANGE24H_MIN_ABS = 0.01  # было 0.05 → стало 0.01 (1%)

# Ядро рынков, которые всегда должны быть в подписках (whitelist)
CORE_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "TONUSDT", "DOGEUSDT", "ADAUSDT", "LINKUSDT", "AVAXUSDT",
]

# Диагностика/веб
DEBUG_SIGNALS = True
HEARTBEAT_SEC = 60 * 60
KEEPALIVE_SEC = 13 * 60
WATCHDOG_SEC = 60
PORT = int(os.getenv("PORT", "10000"))

# Роутинг
PRIMARY_RECIPIENTS = [-1002870952333]
ALLOWED_CHAT_IDS = [533232884, -1002870952333]
ONLY_CHANNEL = True

# =========================
# Утилиты
# =========================
def pct(x: float) -> str:
    return f"{x:.2%}"

def now_ts_ms() -> int:
    return int(time.time() * 1000)

def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt)

def dedup_preserve(seq: List[str]) -> List[str]:
    """Удаляет дубликаты, сохраняя исходный порядок."""
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

logger = logging.getLogger("cryptobot")

# =========================
# Telegram API
# =========================
class Tg:
    def __init__(self, token: str, session: aiohttp.ClientSession) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = session

    async def send(self, chat_id: int, text: str) -> None:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        async with self.http.post(f"{self.base}/sendMessage", json=payload) as r:
            r.raise_for_status()
            await r.json()

    async def get_updates(self, offset: Optional[int] = None, timeout: int = 25) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"timeout": timeout, "allowed_updates": ["message", "channel_post", "my_chat_member"]}
        if offset is not None:
            payload["offset"] = offset
        async with self.http.post(f"{self.base}/getUpdates", json=payload, timeout=aiohttp.ClientTimeout(total=timeout+10)) as r:
            r.raise_for_status()
            return await r.json()

# =========================
# Bybit REST
# =========================
class BybitRest:
    def __init__(self, base: str, session: aiohttp.ClientSession) -> None:
        self.base = base.rstrip("/")
        self.http = session

    async def tickers_linear(self) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/tickers?category=linear"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []

    async def instrument_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/v5/market/instruments-info?category=linear&symbol={symbol}"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
        lst = data.get("result", {}).get("list", []) or []
        return lst[0] if lst else None

# =========================
# Bybit WebSocket
# =========================
class BybitWS:
    def __init__(self, url: str, session: aiohttp.ClientSession) -> None:
        self.url = url
        self.http = session
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._subs: Set[str] = set()
        self._ping_task: Optional[asyncio.Task] = None
        self.on_message: Optional[callable] = None

    async def connect(self) -> None:
        if self.ws and not self.ws.closed:
            return
        logger.info(f"WS connecting: {self.url}")
        self.ws = await self.http.ws_connect(self.url, heartbeat=25)
        if self._ping_task is None or self._ping_task.done():
            self._ping_task = asyncio.create_task(self._ping_loop())
        if self._subs:
            await self.subscribe(list(self._subs))

    async def _ping_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(20)
                if not self.ws or self.ws.closed:
                    continue
                await self.ws.send_json({"op": "ping"})
            except Exception as e:
                logger.warning(f"WS ping error: {e}")

    async def subscribe(self, args: List[str]) -> None:
        for a in args:
            self._subs.add(a)
        if not self.ws or self.ws.closed:
            await self.connect()
        if not args:
            return
        await self.ws.send_json({"op": "subscribe", "args": args})
        logger.info(f"WS subscribed: {args}")

    async def unsubscribe(self, args: List[str]) -> None:
        for a in args:
            self._subs.discard(a)
        if not self.ws or self.ws.closed:
            return
        if not args:
            return
        await self.ws.send_json({"op": "unsubscribe", "args": args})
        logger.info(f"WS unsubscribed: {args}")

    async def run(self) -> None:
        assert self.ws is not None, "call connect() first"
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue
                if data.get("op") in {"subscribe", "unsubscribe", "ping", "pong"} or "success" in data:
                    continue
                if self.on_message:
                    try:
                        await self.on_message(data)
                    except Exception as e:
                        logger.exception(f"on_message error: {e}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
        await asyncio.sleep(2)
        with contextlib.suppress(Exception):
            await self.connect()
        if self.ws and not self.ws.closed:
            await self.run()

# =========================
# OrderBook / Features
# =========================
class AutoLadder:
    def __init__(self, tick: float, bps: float = LADDER_BPS_DEFAULT) -> None:
        self.tick = max(tick, 1e-12)
        self.bps = bps

    def step(self, mid: float) -> float:
        # округление к кратному tick
        raw = max(self.tick, mid * self.bps)
        steps = max(1, round(raw / self.tick))
        return steps * self.tick

class OrderBookState:
    """Простая L2 книга с агрегированием и seq."""
    __slots__ = ("tick", "seq", "bids", "asks", "last_mid", "last_spreads", "last_levels_ts")

    def __init__(self, tick: float) -> None:
        self.tick = max(tick, 1e-12)
        self.seq: Optional[int] = None
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_mid: float = 0.0
        self.last_spreads: List[float] = []  # для p25 спреда
        self.last_levels_ts: List[Tuple[int, float, float]] = []  # (ts, price, notional)

    def apply_snapshot(self, bids: Iterable[Tuple[float,float]], asks: Iterable[Tuple[float,float]], seq: Optional[int]) -> None:
        self.bids = {float(p): float(s) for p, s in bids if float(s) > 0.0}
        self.asks = {float(p): float(s) for p, s in asks if float(s) > 0.0}
        self.seq = int(seq) if seq is not None else None
        self._update_mid_spread()

    def apply_delta(self, bids: Iterable[Tuple[float,float]], asks: Iterable[Tuple[float,float]], seq: Optional[int]) -> None:
        if
