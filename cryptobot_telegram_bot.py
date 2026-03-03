# -*- coding: utf-8 -*-
"""
Cryptobot — SCALPING Signals (Bybit V5, USDT Perpetuals)
v14 — Новая стратегия: BB Breakout/Reversal + MACD + VWAP + RSI + Volume
      Цель: минимум 1% TP на сделку, 4/5 подтверждений

Логика входа:
  LONG:
    1. MACD: macd_line > signal_line  (бычий тренд)
    2. BB:   цена пробила верхнюю BB (импульс вверх)
             ИЛИ отбилась от нижней BB + RSI < 38 (разворот)
    3. VWAP: цена выше VWAP (институциональный bias вверх)
    4. RSI:  45–72 (зона моментума, не перекуплен)
             ИЛИ < 38 (разворот от нижней BB)
    5. VOL:  объём > средний × 1.3 (подтверждение движения)

  SHORT — зеркально.

  Требуется 4 из 5 совпадений (MIN_CONFIRMATIONS=4).

TP: min 1.0%, max 2.5%
SL: 0.55 × ATR (tight stop)
RR: min 1.6
"""
from __future__ import annotations

import asyncio
import contextlib
import html
import json
import logging
import os
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web

# =========================
# БАЗОВЫЙ КОНФИГ
# =========================
BYBIT_REST             = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL              = os.getenv("LOG_LEVEL", "INFO")

PORT           = int(os.getenv("PORT", "10000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or ""


def _bool_env(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


ALLOWED_CHAT_IDS   = [int(x) for x in (os.getenv("ALLOWED_CHAT_IDS") or "").split(",") if x.strip()]
PRIMARY_RECIPIENTS = [i for i in ALLOWED_CHAT_IDS if i < 0] or ALLOWED_CHAT_IDS[:1] or []
ONLY_CHANNEL       = _bool_env("ONLY_CHANNEL", True)

# Вселенная символов
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "600"))
TURNOVER_MIN_USD     = float(os.getenv("TURNOVER_MIN_USD", "2000000"))
ACTIVE_SYMBOLS       = int(os.getenv("ACTIVE_SYMBOLS", "60"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
                "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT"]

# =========================
# SCALPING КОНФИГ (5m) v14
# =========================
TF_SCALP   = "5"
RSI_PERIOD = 14
EMA_FAST   = 5
EMA_SLOW   = 13

# MACD
MACD_FAST   = int(os.getenv("MACD_FAST",   "12"))
MACD_SLOW   = int(os.getenv("MACD_SLOW",   "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))

# Bollinger Bands
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_STDDEV = float(os.getenv("BB_STDDEV", "2.0"))

# VWAP — считаем по последним N барам (нет тиковых данных)
VWAP_BARS = int(os.getenv("VWAP_BARS", "50"))

# RSI зоны
RSI_OVERSOLD_REV  = float(os.getenv("RSI_OVERSOLD",   "38"))   # разворот от нижней BB
RSI_OVERBOUGHT_REV = float(os.getenv("RSI_OVERBOUGHT", "62"))  # разворот от верхней BB
RSI_MOM_LOW  = 45.0   # нижняя граница зоны моментума (breakout long)
RSI_MOM_HIGH = 72.0   # верхняя граница зоны моментума (breakout long)

# Объём
VOLUME_SPIKE_MULT = float(os.getenv("VOLUME_SPIKE_MULT", "1.3"))

# TP/SL — цель минимум 1%
ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "0.55"))   # tight stop
ATR_TP_MULT = float(os.getenv("ATR_TP_MULT", "1.6"))
TP_MIN_PCT  = float(os.getenv("TP_MIN_PCT",  "0.010"))   # минимум 1.0%
TP_MAX_PCT  = float(os.getenv("TP_MAX_PCT",  "0.025"))   # максимум 2.5%
RR_MIN      = float(os.getenv("RR_MIN",      "1.6"))

# v14: 4 из 5 подтверждений
MIN_CONFIRMATIONS = int(os.getenv("MIN_CONFIRMATIONS", "4"))

SIGNAL_COOLDOWN_SEC   = int(os.getenv("SIGNAL_COOLDOWN_SEC",   "300"))
POSITION_COOLDOWN_SEC = int(os.getenv("POSITION_COOLDOWN_SEC", "45"))
KEEPALIVE_SEC         = int(os.getenv("KEEPALIVE_SEC",          str(13 * 60)))
WATCHDOG_SEC          = int(os.getenv("WATCHDOG_SEC",           "60"))
STALL_EXIT_SEC        = int(os.getenv("STALL_EXIT_SEC",         "600"))
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS",          "5"))

PRELOAD_BARS    = int(os.getenv("PRELOAD_BARS",    "150"))  # больше для MACD (нужно 26+9=35 мин)
PRELOAD_WORKERS = int(os.getenv("PRELOAD_WORKERS", "10"))

REPORT_ERRORS_TO_TG       = _bool_env("REPORT_ERRORS_TO_TG", False)
ERROR_REPORT_COOLDOWN_SEC = int(os.getenv("ERROR_REPORT_COOLDOWN_SEC", "180"))

# =========================
# УТИЛИТЫ
# =========================
def now_ms() -> int: return int(time.time() * 1000)
def now_s()  -> int: return int(time.time())


def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, force=True)


logger = logging.getLogger("cryptobot.scalp")


async def report_error(
    app: web.Application,
    where: str,
    exc: Optional[BaseException] = None,
    note: Optional[str] = None,
) -> None:
    if not REPORT_ERRORS_TO_TG:
        return
    tg: Optional[Tg] = app.get("tg")
    if not tg:
        return
    now  = now_s()
    last = app.setdefault("_last_error_ts", 0)
    if now - last < ERROR_REPORT_COOLDOWN_SEC:
        return
    app["_last_error_ts"] = now
    title = f"⚠️ <b>Runtime error</b> @ {html.escape(where)}"
    ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    body  = f"\n<b>Note:</b> {html.escape(note)}" if note else ""
    if exc:
        tb   = traceback.format_exc()
        tail = "\n".join(tb.strip().splitlines()[-20:])
        body += "\n<pre>" + html.escape(tail[:3500]) + "</pre>"
    text    = f"{title}\n🕒 {ts} UTC{body}"
    targets = PRIMARY_RECIPIENTS if ONLY_CHANNEL and PRIMARY_RECIPIENTS else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS)
    for chat_id in targets:
        with contextlib.suppress(Exception):
            await tg.send(chat_id, text)


# =========================
# ИНДИКАТОРЫ
# =========================
def ema(values: List[float], period: int) -> float:
    """EMA от списка значений, возвращает последнее."""
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    k   = 2.0 / (period + 1.0)
    val = sum(values[:period]) / period
    for v in values[period:]:
        val = v * k + val * (1 - k)
    return val


def ema_series(values: List[float], period: int) -> List[float]:
    """EMA — возвращает весь ряд (нужен для MACD signal)."""
    if len(values) < period:
        return [sum(values[:i+1]) / (i+1) for i in range(len(values))]
    k      = 2.0 / (period + 1.0)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    # выравниваем длину с входом (первые period-1 элементов заполняем NaN-заменой)
    prefix = [result[0]] * (period - 1)
    return prefix + result


def calc_macd(
    closes: List[float],
    fast: int = MACD_FAST,
    slow: int = MACD_SLOW,
    signal_p: int = MACD_SIGNAL,
) -> Tuple[float, float, float]:
    """
    Возвращает (macd_line, signal_line, histogram) для последней свечи.
    macd_line  = EMA_fast - EMA_slow
    signal     = EMA(macd_line, signal_p)
    histogram  = macd_line - signal
    """
    if len(closes) < slow + signal_p:
        return 0.0, 0.0, 0.0
    ema_fast_s = ema_series(closes, fast)
    ema_slow_s = ema_series(closes, slow)
    macd_line_s = [f - s for f, s in zip(ema_fast_s, ema_slow_s)]
    signal_s    = ema_series(macd_line_s, signal_p)
    ml  = macd_line_s[-1]
    sl  = signal_s[-1]
    return ml, sl, ml - sl


def calc_bb(
    closes: List[float],
    period: int = BB_PERIOD,
    stddev: float = BB_STDDEV,
) -> Tuple[float, float, float]:
    """Возвращает (upper, middle, lower) для последней свечи."""
    if len(closes) < period:
        m = closes[-1] if closes else 0.0
        return m, m, m
    window = closes[-period:]
    mid    = sum(window) / period
    std    = (sum((x - mid) ** 2 for x in window) / period) ** 0.5
    return mid + stddev * std, mid, mid - stddev * std


def calc_vwap(
    data: List[Tuple[float, float, float, float, float]],
    bars: int = VWAP_BARS,
) -> float:
    """
    VWAP-приближение по последним N барам.
    Типичная цена = (high + low + close) / 3.
    """
    window = data[-bars:]
    if not window:
        return 0.0
    tv = sum(((b[1] + b[2] + b[3]) / 3.0) * b[4] for b in window)
    tv_vol = sum(b[4] for b in window)
    return tv / tv_vol if tv_vol > 0 else 0.0


def calc_rsi(
    data: List[Tuple[float, float, float, float, float]],
    period: int = RSI_PERIOD,
) -> float:
    if len(data) < period + 1:
        return 50.0
    closes        = [b[3] for b in data[-(period + 1):]]
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(0.0, ch))
        losses.append(max(0.0, -ch))
    avg_g, avg_l = sum(gains) / period, sum(losses) / period
    if avg_l == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + avg_g / avg_l))


def calc_atr(
    data: List[Tuple[float, float, float, float, float]],
    period: int = 14,
) -> float:
    if len(data) < period + 1:
        return 0.0
    total = 0.0
    for i in range(len(data) - period, len(data)):
        h, l, pc = data[i][1], data[i][2], data[i - 1][3]
        total += max(h - l, abs(h - pc), abs(pc - l))
    return total / period


# =========================
# КЛИЕНТЫ
# =========================
class BybitWS:
    def __init__(self, url: str, http: aiohttp.ClientSession) -> None:
        self.url, self.http = url, http
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.on_message = None
        self._running   = False

    async def connect(self) -> None:
        with contextlib.suppress(Exception):
            if self.ws and not self.ws.closed:
                await self.ws.close()
        self.ws = await self.http.ws_connect(self.url, heartbeat=30)
        logger.info("BybitWS connected")

    async def subscribe(self, topics: List[str]) -> None:
        if not self.ws or self.ws.closed:
            await self.connect()
        for i in range(0, len(topics), 10):
            await self.ws.send_json({"op": "subscribe", "args": topics[i:i + 10]})
            await asyncio.sleep(0.05)

    async def run(self) -> None:
        self._running = True
        delay = 1.0
        while self._running:
            try:
                if not self.ws or self.ws.closed:
                    await self.connect()
                async for msg in self.ws:
                    if not self._running:
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if self.on_message:
                            res = self.on_message(data)
                            if asyncio.iscoroutine(res):
                                asyncio.create_task(res)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception:
                logger.exception("WS error, reconnecting...")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 30.0)
        logger.info("BybitWS stopped")

    async def stop(self) -> None:
        self._running = False
        if self.ws:
            await self.ws.close()


class Tg:
    def __init__(self, token: str, http: aiohttp.ClientSession):
        self.token    = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session  = http

    async def delete_webhook(self, drop_pending_updates: bool = False):
        url = f"{self.base_url}/deleteWebhook"
        async with self.session.post(url, json={"drop_pending_updates": drop_pending_updates}) as r:
            return await r.json()

    async def get_updates(self, offset: Optional[int] = None, timeout: int = 25) -> List[Dict]:
        url  = f"{self.base_url}/getUpdates"
        data = {"timeout": timeout, "offset": offset} if offset else {"timeout": timeout}
        try:
            async with self.session.post(url, json=data, timeout=timeout + 5) as r:
                if r.status == 200:
                    res = await r.json()
                    return res.get("result", [])
        except Exception:
            pass
        return []

    async def send(self, chat_id: Any, text: str) -> bool:
        url     = f"{self.base_url}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with self.session.post(url, json=payload) as r:
                return r.status == 200
        except Exception:
            return False


class BybitRest:
    def __init__(self, base: str, http: aiohttp.ClientSession) -> None:
        self.base, self.http = base.rstrip("/"), http

    async def tickers_linear(self) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/tickers?category=linear"
        async with self.http.get(url, timeout=10) as r:
            return (await r.json()).get("result", {}).get("list", [])

    async def klines(
        self, category: str, symbol: str, interval: str, limit: int = 200
    ) -> List[Tuple[float, float, float, float, float]]:
        url = (f"{self.base}/v5/market/kline"
               f"?category={category}&symbol={symbol}&interval={interval}&limit={limit}")
        async with self.http.get(url, timeout=10) as r:
            arr = (await r.json()).get("result", {}).get("list", [])
            return [(float(it[1]), float(it[2]), float(it[3]), float(it[4]), float(it[5]))
                    for it in reversed(arr)]


# =========================
# МОДЕЛИ
# =========================
@dataclass
class Position:
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_time: int


class Market:
    def __init__(self):
        self.symbols: List[str]              = []
        self.state:   Dict[str, SymbolState] = defaultdict(SymbolState)
        self.last_ws_msg_ts      = now_ms()
        self.signal_stats        = {"total": 0, "long": 0, "short": 0}
        self.last_signal_sent_ts = 0


@dataclass
class SymbolState:
    k5: List[Tuple[float, float, float, float, float]] = field(default_factory=list)
    # Индикаторы
    rsi:        float = 50.0
    macd_line:  float = 0.0
    macd_sig:   float = 0.0
    macd_hist:  float = 0.0
    bb_upper:   float = 0.0
    bb_mid:     float = 0.0
    bb_lower:   float = 0.0
    vwap:       float = 0.0
    atr:        float = 0.0
    # EMA (для совместимости)
    ema_fast:   float = 0.0
    ema_slow:   float = 0.0
    # Служебные
    last_signal_ts:   int = 0
    last_position_ts: int = 0


# =========================
# ДВИЖОК v14
# =========================
class ScalpingEngine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def update_indicators(self, sym: str) -> None:
        st = self.mkt.state[sym]
        if len(st.k5) < MACD_SLOW + MACD_SIGNAL:
            return
        closes = [b[3] for b in st.k5]

        st.rsi                          = calc_rsi(st.k5)
        st.macd_line, st.macd_sig, st.macd_hist = calc_macd(closes)
        st.bb_upper, st.bb_mid, st.bb_lower     = calc_bb(closes)
        st.vwap                         = calc_vwap(st.k5)
        st.atr                          = calc_atr(st.k5, 14)
        st.ema_fast                     = ema(closes, EMA_FAST)
        st.ema_slow                     = ema(closes, EMA_SLOW)

    def generate_scalp_signal(self, sym: str) -> Optional[Dict]:
        st = self.mkt.state[sym]
        # Нужно достаточно баров для всех индикаторов
        min_bars = MACD_SLOW + MACD_SIGNAL + 5
        if len(st.k5) < min_bars:
            return None
        nowt = now_s()
        if nowt - st.last_signal_ts < SIGNAL_COOLDOWN_SEC:
            return None

        price = st.k5[-1][3]
        rsi   = st.rsi
        atr_v = st.atr if st.atr > 0 else price * 0.005

        # Объём
        avg_vol   = sum(b[4] for b in st.k5[-7:-1]) / 6.0 if len(st.k5) >= 7 else 0.0
        volume_ok = (st.k5[-1][4] > avg_vol * VOLUME_SPIKE_MULT) if avg_vol > 0 else True

        # BB сигналы
        bb_breakout_up   = price > st.bb_upper                        # пробой вверх
        bb_reversal_up   = price <= st.bb_lower * 1.001               # касание нижней полосы
        bb_breakout_down = price < st.bb_lower                        # пробой вниз
        bb_reversal_down = price >= st.bb_upper * 0.999               # касание верхней полосы

        # MACD
        macd_bull = st.macd_line > st.macd_sig   # бычий
        macd_bear = st.macd_line < st.macd_sig   # медвежий

        # VWAP
        above_vwap = st.vwap > 0 and price > st.vwap
        below_vwap = st.vwap > 0 and price < st.vwap

        # RSI зоны
        rsi_momentum_long  = RSI_MOM_LOW  <= rsi <= RSI_MOM_HIGH   # breakout long
        rsi_reversal_long  = rsi < RSI_OVERSOLD_REV                 # разворот от нижней BB
        rsi_ok_long        = rsi_momentum_long or rsi_reversal_long

        rsi_momentum_short = (100 - RSI_MOM_HIGH) <= (100 - rsi) <= (100 - RSI_MOM_LOW)
        rsi_reversal_short = rsi > RSI_OVERBOUGHT_REV
        rsi_ok_short       = rsi_momentum_short or rsi_reversal_short

        # ── LONG: 5 проверок ─────────────────────────────────────────────
        long_checks = [
            macd_bull,                            # 1. MACD бычий
            bb_breakout_up or bb_reversal_up,     # 2. BB сигнал (пробой или разворот)
            above_vwap,                           # 3. Цена выше VWAP
            rsi_ok_long,                          # 4. RSI в зоне моментума или разворота
            volume_ok,                            # 5. Подтверждение объёмом
        ]

        # ── SHORT: 5 проверок ────────────────────────────────────────────
        short_checks = [
            macd_bear,                            # 1. MACD медвежий
            bb_breakout_down or bb_reversal_down, # 2. BB сигнал
            below_vwap,                           # 3. Цена ниже VWAP
            rsi_ok_short,                         # 4. RSI в зоне
            volume_ok,                            # 5. Объём
        ]

        if   sum(long_checks)  >= MIN_CONFIRMATIONS:
            side = "LONG"
            score = sum(long_checks)
        elif sum(short_checks) >= MIN_CONFIRMATIONS:
            side = "SHORT"
            score = sum(short_checks)
        else:
            return None

        # ── TP / SL ───────────────────────────────────────────────────────
        sl = price - atr_v * ATR_SL_MULT if side == "LONG" else price + atr_v * ATR_SL_MULT
        tp = price + atr_v * ATR_TP_MULT if side == "LONG" else price - atr_v * ATR_TP_MULT

        # Применяем TP_MIN_PCT / TP_MAX_PCT
        raw_tp_pct = abs(tp - price) / price
        if raw_tp_pct < TP_MIN_PCT:
            tp = (price * (1 + TP_MIN_PCT)) if side == "LONG" else (price * (1 - TP_MIN_PCT))
        elif raw_tp_pct > TP_MAX_PCT:
            tp = (price * (1 + TP_MAX_PCT)) if side == "LONG" else (price * (1 - TP_MAX_PCT))

        rr = abs(tp - price) / max(1e-9, abs(price - sl))
        if rr < RR_MIN:
            return None

        tp_pct = abs(tp - price) / price * 100.0
        sl_pct = abs(sl - price) / price * 100.0

        # Определяем тип сигнала для информации
        if side == "LONG":
            sig_type = "Breakout ↑" if bb_breakout_up else "Reversal ↑"
        else:
            sig_type = "Breakout ↓" if bb_breakout_down else "Reversal ↓"

        st.last_signal_ts = nowt
        self.mkt.signal_stats["total"] += 1
        self.mkt.signal_stats[side.lower()] += 1

        return {
            "symbol":   sym,
            "side":     side,
            "sig_type": sig_type,
            "entry":    price,
            "tp":       tp,
            "sl":       sl,
            "tp_pct":   tp_pct,
            "sl_pct":   sl_pct,
            "rr":       rr,
            "rsi":      rsi,
            "macd_h":   st.macd_hist,
            "vwap":     st.vwap,
            "checks":   f"{score}/5",
        }


# =========================
# ПРЕДЗАГРУЗКА
# =========================
async def preload_symbol(
    rest: BybitRest, mkt: Market, engine: ScalpingEngine,
    sym: str, sem: asyncio.Semaphore,
) -> None:
    async with sem:
        try:
            bars = await rest.klines("linear", sym, TF_SCALP, limit=PRELOAD_BARS)
            if bars:
                mkt.state[sym].k5 = list(bars[-200:])
                engine.update_indicators(sym)
        except Exception as e:
            logger.warning(f"Preload failed {sym}: {e}")


async def preload_all(rest: BybitRest, mkt: Market, engine: ScalpingEngine) -> int:
    sem   = asyncio.Semaphore(PRELOAD_WORKERS)
    tasks = [preload_symbol(rest, mkt, engine, sym, sem) for sym in mkt.symbols]
    logger.info(f"Preloading {len(tasks)} symbols (workers={PRELOAD_WORKERS})...")
    await asyncio.gather(*tasks)
    min_bars = MACD_SLOW + MACD_SIGNAL + 5
    ready = sum(1 for sym in mkt.symbols if len(mkt.state[sym].k5) >= min_bars)
    logger.info(f"Preload complete: {ready}/{len(mkt.symbols)} symbols ready")
    return ready


# =========================
# WS / СИГНАЛЫ
# =========================
async def ws_on_message(app: web.Application, data: Dict) -> None:
    mkt: Market = app["mkt"]
    topic = data.get("topic", "")
    if not topic.startswith("kline."):
        return
    mkt.last_ws_msg_ts = now_ms()

    payload = data.get("data", [])
    if not payload:
        return
    symbol = topic.split(".")[-1]
    st     = mkt.state[symbol]

    for p in payload:
        o, h, l, c, v = (float(p["open"]), float(p["high"]), float(p["low"]),
                         float(p["close"]), float(p["volume"]))
        if p.get("confirm") is False and st.k5:
            st.k5[-1] = (o, h, l, c, v)
        else:
            st.k5.append((o, h, l, c, v))
            if len(st.k5) > 200:
                st.k5.pop(0)
            if p.get("confirm") is True:
                app["engine"].update_indicators(symbol)
                sig = app["engine"].generate_scalp_signal(symbol)
                if sig:
                    asyncio.create_task(send_signal(app, sig))


async def send_signal(app: web.Application, sig: Dict) -> None:
    side_emoji = "🟢" if sig["side"] == "LONG" else "🔴"
    text = (
        f"{side_emoji} <b>{sig['side']} | {sig['symbol']}</b> (5m) — {sig['sig_type']}\n"
        f"📍 Entry:  <code>{sig['entry']:.5f}</code>\n"
        f"🎯 TP:     <code>{sig['tp']:.5f}</code>  <i>(+{sig['tp_pct']:.2f}%)</i>\n"
        f"🛡 SL:     <code>{sig['sl']:.5f}</code>  <i>(-{sig['sl_pct']:.2f}%)</i>\n"
        f"📊 RSI: {sig['rsi']:.1f}  |  MACD hist: {sig['macd_h']:+.5f}  |  RR: {sig['rr']:.2f}\n"
        f"💧 VWAP: <code>{sig['vwap']:.5f}</code>\n"
        f"✅ Подтверждений: {sig['checks']}"
    )
    targets = PRIMARY_RECIPIENTS if ONLY_CHANNEL else ALLOWED_CHAT_IDS
    for cid in targets:
        await app["tg"].send(cid, text)
    app["mkt"].last_signal_sent_ts = now_ms()
    logger.info(
        f"Signal: {sig['side']} {sig['symbol']} {sig['sig_type']} "
        f"entry={sig['entry']:.5f} tp=+{sig['tp_pct']:.2f}% sl=-{sig['sl_pct']:.2f}% "
        f"rr={sig['rr']:.2f} checks={sig['checks']}"
    )


# =========================
# ВСПОМОГАТЕЛЬНЫЕ ЦИКЛЫ
# =========================
async def tg_loop(app: web.Application) -> None:
    tg: Tg = app["tg"]
    offset: Optional[int] = None
    while True:
        try:
            updates = await tg.get_updates(offset=offset)
            for upd in updates:
                offset = upd["update_id"] + 1
                msg    = upd.get("message") or upd.get("channel_post")
                if not msg or "text" not in msg:
                    continue
                text = msg["text"]
                cid  = msg["chat"]["id"]
                if text == "/ping":
                    await tg.send(cid, "🏓 pong")
                elif text == "/status":
                    mkt      = app["mkt"]
                    stats    = mkt.signal_stats
                    min_bars = MACD_SLOW + MACD_SIGNAL + 5
                    ready    = sum(1 for s in mkt.symbols if len(mkt.state[s].k5) >= min_bars)
                    last_ago = (
                        f"{int((now_ms() - mkt.last_signal_sent_ts) / 1000)}s назад"
                        if mkt.last_signal_sent_ts else "ещё не было"
                    )
                    await tg.send(cid, (
                        f"✅ <b>Cryptobot SCALP v14</b>\n"
                        f"Стратегия: BB + MACD + VWAP + RSI + Vol\n"
                        f"Фильтр: {MIN_CONFIRMATIONS}/5 | TP мин: {TP_MIN_PCT*100:.1f}%\n"
                        f"Символов: {ready}/{len(mkt.symbols)} готовы\n"
                        f"Сигналов: {stats['total']} (L:{stats['long']} / S:{stats['short']})\n"
                        f"Последний: {last_ago}\n"
                        f"WS: {int((now_ms() - mkt.last_ws_msg_ts) / 1000)}s назад"
                    ))
        except Exception:
            await asyncio.sleep(5)


async def watchdog_loop(app: web.Application) -> None:
    while True:
        await asyncio.sleep(WATCHDOG_SEC)
        if now_ms() - app["mkt"].last_ws_msg_ts > STALL_EXIT_SEC * 1000:
            logger.error("WS stalled, exiting for restart")
            os._exit(1)


async def keepalive_loop(app: web.Application) -> None:
    while True:
        await asyncio.sleep(KEEPALIVE_SEC)
        logger.info(f"Keepalive: WS last msg {(now_ms() - app['mkt'].last_ws_msg_ts) / 1000:.1f}s ago")


async def universe_refresh_loop(app: web.Application) -> None:
    while True:
        await asyncio.sleep(UNIVERSE_REFRESH_SEC)
        try:
            new_syms = await build_universe_once(app["rest"])
            old_set  = set(app["mkt"].symbols)
            to_add   = [s for s in new_syms if s not in old_set]
            if to_add:
                sem = asyncio.Semaphore(PRELOAD_WORKERS)
                await asyncio.gather(*[
                    preload_symbol(app["rest"], app["mkt"], app["engine"], sym, sem)
                    for sym in to_add
                ])
                await app["ws"].subscribe([f"kline.{TF_SCALP}.{s}" for s in to_add])
                app["mkt"].symbols = new_syms
                logger.info(f"Universe updated: +{len(to_add)} symbols")
        except Exception:
            logger.exception("Universe refresh error")


async def build_universe_once(rest: BybitRest) -> List[str]:
    try:
        tickers = await rest.tickers_linear()
        pool = [
            t["symbol"] for t in tickers
            if t["symbol"].endswith("USDT") and float(t.get("turnover24h", 0)) > TURNOVER_MIN_USD
        ]
        return list(dict.fromkeys(CORE_SYMBOLS + pool))[:ACTIVE_SYMBOLS]
    except Exception:
        return list(CORE_SYMBOLS)


# =========================
# СТАРТ
# =========================
async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    http        = aiohttp.ClientSession()
    app["http"] = http
    app["tg"]   = Tg(TELEGRAM_TOKEN, http)

    await app["tg"].delete_webhook(drop_pending_updates=True)
    logger.info("🚀 Starting Cryptobot SCALP v14")

    app["rest"]   = BybitRest(BYBIT_REST, http)
    app["mkt"]    = Market()
    app["engine"] = ScalpingEngine(app["mkt"])
    app["ws"]     = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: ws_on_message(app, data)

    app["mkt"].symbols = await build_universe_once(app["rest"])
    ready = await preload_all(app["rest"], app["mkt"], app["engine"])

    await app["ws"].connect()
    await app["ws"].subscribe([f"kline.{TF_SCALP}.{s}" for s in app["mkt"].symbols])

    app["ws_task"]        = asyncio.create_task(app["ws"].run())
    app["tg_task"]        = asyncio.create_task(tg_loop(app))
    app["watchdog_task"]  = asyncio.create_task(watchdog_loop(app))
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["universe_task"]  = asyncio.create_task(universe_refresh_loop(app))

    try:
        targets = PRIMARY_RECIPIENTS if PRIMARY_RECIPIENTS else (ALLOWED_CHAT_IDS[:1] if ALLOWED_CHAT_IDS else [])
        for chat_id in targets:
            await app["tg"].send(chat_id, (
                "🟢 <b>Cryptobot SCALP v14 Online</b>\n"
                f"• Стратегия: BB Breakout/Reversal + MACD + VWAP + RSI + Vol\n"
                f"• Таймфрейм: {TF_SCALP}m\n"
                f"• Фильтр: {MIN_CONFIRMATIONS}/5 подтверждений\n"
                f"• TP: {TP_MIN_PCT*100:.1f}% – {TP_MAX_PCT*100:.1f}%  |  RR ≥ {RR_MIN}\n"
                f"• Символов: {len(app['mkt'].symbols)} ({ready} готовы)\n"
                "• История: предзагружена ✅\n"
                "• WebSocket: Connected ✅"
            ))
    except Exception as e:
        logger.warning(f"Startup notify failed: {e}")


async def on_cleanup(app: web.Application) -> None:
    for k in ("ws_task", "tg_task", "watchdog_task", "keepalive_task", "universe_task"):
        if k in app:
            app[k].cancel()
    if "ws" in app:
        await app["ws"].stop()
    if "http" in app:
        await app["http"].close()


async def handle_health(request: web.Request) -> web.Response:
    return web.Response(text="OK", status=200)


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/",        handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=PORT)
