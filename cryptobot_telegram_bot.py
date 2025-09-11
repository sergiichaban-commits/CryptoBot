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
        if seq is not None and self.seq is not None and int(seq) <= int(self.seq):
            return
        for p, s in bids:
            p = float(p); s = float(s)
            if s <= 0.0: self.bids.pop(p, None)
            else: self.bids[p] = s
        for p, s in asks:
            p = float(p); s = float(s)
            if s <= 0.0: self.asks.pop(p, None)
            else: self.asks[p] = s
        self.seq = int(seq) if seq is not None else self.seq
        self._update_mid_spread()

    def _update_mid_spread(self) -> None:
        bb = max(self.bids) if self.bids else 0.0
        ba = min(self.asks) if self.asks else 0.0
        if bb > 0 and ba > 0 and ba >= bb:
            self.last_mid = (bb + ba) / 2.0
            spread = (ba - bb)
            if spread > 0:
                self.last_spreads.append(spread)
                if len(self.last_spreads) > 600:
                    self.last_spreads = self.last_spreads[-600:]

    def best(self) -> Tuple[Optional[float], Optional[float]]:
        bb = max(self.bids) if self.bids else None
        ba = min(self.asks) if self.asks else None
        return bb, ba

class OrderBookManager:
    def __init__(self) -> None:
        self.books: Dict[str, OrderBookState] = {}
        self.ticks: Dict[str, float] = {}
        self.last_wall_levels: Dict[str, List[Tuple[int, float, float]]] = {}  # symbol -> [(ts, price, notional)]

    def ensure(self, sym: str, tick: float) -> OrderBookState:
        if sym not in self.books:
            self.books[sym] = OrderBookState(tick)
        self.ticks[sym] = tick
        return self.books[sym]

    def note_snapshot(self, sym: str, tick: float, bids: Iterable[Tuple[float,float]], asks: Iterable[Tuple[float,float]], seq: Optional[int]) -> None:
        ob = self.ensure(sym, tick)
        ob.apply_snapshot(bids, asks, seq)

    def note_delta(self, sym: str, tick: float, bids: Iterable[Tuple[float,float]], asks: Iterable[Tuple[float,float]], seq: Optional[int]) -> None:
        ob = self.ensure(sym, tick)
        ob.apply_delta(bids, asks, seq)

    def spread_ticks(self, sym: str) -> Optional[float]:
        ob = self.books.get(sym)
        if not ob: return None
        bb, ba = ob.best()
        if not bb or not ba: return None
        spread = ba - bb
        return spread / max(ob.tick, 1e-12)

    def p25_spread_ticks(self, sym: str) -> Optional[float]:
        ob = self.books.get(sym)
        if not ob or not ob.last_spreads: return None
        arr = sorted(ob.last_spreads[-600:])
        idx = int(0.25 * (len(arr)-1))
        return arr[idx] / max(ob.tick, 1e-12)

    def features(self, sym: str, ladder: AutoLadder) -> Optional[Dict[str, Any]]:
        ob = self.books.get(sym)
        if not ob or ob.last_mid <= 0: return None
        step = ladder.step(ob.last_mid)
        # агрегируем корзины bids/asks по шагу
        def agg(book: Dict[float,float], reverse: bool) -> List[Tuple[float,float]]:
            buckets: Dict[float, float] = {}
            for p, s in book.items():
                b = round(p / step) * step
                buckets[b] = buckets.get(b, 0.0) + s * p  # считаем ноушнл
                # если бы нужен был чисто объём, вместо s*p ставь s
            levels = sorted(buckets.items(), key=lambda x: x[0], reverse=reverse)
            return levels

        bids_levels = agg(ob.bids, True)
        asks_levels = agg(ob.asks, False)

        # OBI
        b_not = sum(s for _, s in bids_levels[:ORDERBOOK_DEPTH_BINS])
        a_not = sum(s for _, s in asks_levels[:ORDERBOOK_DEPTH_BINS])
        denom = max(1e-9, a_not + b_not)
        obi = (b_not - a_not) / denom

        # стенки: порог по p95 от истории
        ts_now = now_ts_ms()
        hist = self.last_wall_levels.setdefault(sym, [])
        # пополним историю последним срезом верхней десятки
        for p, s in (bids_levels[:10] + asks_levels[:10]):
            hist.append((ts_now, p, s))
        # чистим
        cutoff = ts_now - WALL_LOOKBACK_SEC * 1000
        self.last_wall_levels[sym] = [x for x in hist if x[0] >= cutoff]
        only_vals = [x[2] for x in self.last_wall_levels[sym]]
        wall_thr = 0.0
        if only_vals:
            only_vals_sorted = sorted(only_vals)
            idx = int(len(only_vals_sorted) * WALL_PCTL / 100)
            idx = min(max(idx, 0), len(only_vals_sorted)-1)
            wall_thr = only_vals_sorted[idx]

        walls_bid = [(p, s) for p, s in bids_levels if s >= wall_thr]
        walls_ask = [(p, s) for p, s in asks_levels if s >= wall_thr]

        bb, ba = ob.best()
        spread_ticks_now = self.spread_ticks(sym) or 0.0
        p25 = self.p25_spread_ticks(sym) or spread_ticks_now

        return {
            "step": float(step),
            "obi": float(obi),
            "walls_bid": walls_bid,
            "walls_ask": walls_ask,
            "best_bid": float(bb) if bb else None,
            "best_ask": float(ba) if ba else None,
            "spread_ticks": float(spread_ticks_now),
            "spread_p25_ticks": float(p25),
            "tick": float(ob.tick),
        }

# =========================
# Рыночное состояние
# =========================
class MarketState:
    def __init__(self) -> None:
        self.tickers: Dict[str, Dict[str, Any]] = {}
        self.kline: Dict[str, Dict[str, List[Tuple[float,float,float,float,float]]]] = {"1": {}, "5": {}, "60": {}, "240": {}}
        self.kline_maxlen = 900
               self.liq_events: Dict[str, List[int]] = {}
        self.last_ws_msg_ts: int = now_ts_ms()
        self.cooldown: Dict[Tuple[str,str], int] = {}
        self.last_signal_sent_ts: int = 0
        self.instruments: Dict[str, Dict[str, Any]] = {}

    def note_ticker(self, d: Dict[str, Any]) -> None:
        sym = d.get("symbol")
        if not sym: return
        self.tickers[sym] = d
        self.last_ws_msg_ts = now_ts_ms()

    def note_kline(self, tf: str, sym: str, points: List[Dict[str, Any]]) -> None:
        buf = self.kline.setdefault(tf, {}).setdefault(sym, [])
        for p in points:
            o = float(p["open"]); h = float(p["high"]); l = float(p["low"]); c = float(p["close"])
            v = float(p.get("volume") or p.get("turnover") or 0.0)
            if p.get("confirm") is False and buf:
                buf[-1] = (o,h,l,c,v)
            else:
                buf.append((o,h,l,c,v))
                if len(buf) > self.kline_maxlen:
                    del buf[0:len(buf)-self.kline_maxlen]
        self.last_ws_msg_ts = now_ts_ms()

    def note_liq(self, sym: str, ts_ms: int) -> None:
        arr = self.liq_events.setdefault(sym, [])
        arr.append(ts_ms)
        cutoff = now_ts_ms() - 5*60*1000
        while arr and arr[0] < cutoff:
            arr.pop(0)
        self.last_ws_msg_ts = now_ts_ms()

# =========================
# Индикаторы / вспомогательные
# =========================
def atr(rows: List[Tuple[float,float,float,float,float]], period: int) -> float:
    if len(rows) < period + 1: return 0.0
    trs: List[float] = []
    for i in range(1, period+1):
        _,h,l,c,_ = rows[-i]
        _,_,_,pc,_ = rows[-i-1]
        tr = max(h-l, abs(h - pc), abs(pc - l))
        trs.append(tr)
    return sum(trs)/period if trs else 0.0

def sma(vals: List[float], period: int) -> float:
    if len(vals) < period or period <= 0: return 0.0
    return sum(vals[-period:]) / period

def rolling_vwap(rows: List[Tuple[float,float,float,float,float]], window: int) -> Tuple[float, float]:
    n = len(rows)
    need = max(window, VWAP_SLOPE_BARS + 1)
    if n < need: return 0.0, 0.0
    tp = [(r[1] + r[2] + r[3]) / 3.0 for r in rows]
    v  = [r[4] for r in rows]
    tpw_now = sum(tp[-window+i] * v[-window+i] for i in range(window))
    vw_now  = sum(v[-window+i] for i in range(window)) or 1e-9
    vwap_now = tpw_now / vw_now
    start = n - window - VWAP_SLOPE_BARS
    if start < 0: return vwap_now, 0.0
    tpw_prev = sum(tp[start+i] * v[start+i] for i in range(window))
    vw_prev  = sum(v[start+i] for i in range(window)) or 1e-9
    vwap_prev = tpw_prev / vw_prev
    slope = vwap_now - vwap_prev
    return vwap_now, slope

def find_swings(rows: List[Tuple[float,float,float,float,float]], frac: int = SWING_FRAC) -> Tuple[List[int], List[int]]:
    n = len(rows); sh: List[int] = []; sl: List[int] = []
    for i in range(frac, n - frac):
        h = rows[i][1]; l = rows[i][2]
        hs = [rows[j][1] for j in range(i-frac, i+frac+1)]
        ls = [rows[j][2] for j in range(i-frac, i+frac+1)]
        if h == max(hs) and hs.count(h) == 1: sh.append(i)
        if l == min(ls) and ls.count(l) == 1: sl.append(i)
    return sh, sl

def bos_choch(rows, sh, sl) -> Tuple[str, bool, bool, Optional[int], Optional[int]]:
    if len(rows) < 3 or (not sh and not sl): return "RANGE", False, False, None, None
    c = rows[-1][3]
    bos_up = bos_dn = False; bos_up_idx = bos_dn_idx = None
    if sh:
        idx = sh[-1]; ifh = rows[idx][1]
        if c > ifh: bos_up, bos_up_idx = True, len(rows)-1
    if sl:
        idx = sl[-1]; ifl = rows[idx][2]
        if c < ifl: bos_dn, bos_dn_idx = True, len(rows)-1
    if bos_up and not bos_dn: trend = "UP"
    elif bos_dn and not bos_up: trend = "DOWN"
    else: trend = "UP" if rows[-1][3] > rows[-3][3] else ("DOWN" if rows[-1][3] < rows[-3][3] else "RANGE")
    return trend, bos_up, bos_dn, bos_up_idx, bos_dn_idx

def has_fvg(rows, bullish: bool) -> Optional[Tuple[float,float]]:
    if len(rows) < 3: return None
    h2 = rows[-3][1]; l2 = rows[-3][2]
    h0 = rows[-1][1]; l0 = rows[-1][2]
    if bullish and (l0 > h2): return (h2, l0)
    if (not bullish) and (h0 < l2): return (h0, l2)
    return None

def swept_liquidity(rows, sh, sl, side: str) -> bool:
    if side == "LONG":
        if not sl: return False
        key = rows[sl[-1]][2]; low = rows[-1][2]; close = rows[-1][3]
        return (low < key) and (close > key)
    else:
        if not sh: return False
        key = rows[sh[-1]][1]; high = rows[-1][1]; close = rows[-1][3]
        return (high > key) and (close < key)

def simple_ob(rows, side: str, body_atr_thr: float, atr_val: float) -> Optional[Tuple[float,float]]:
    if len(rows) < 3 or atr_val <= 0: return None
    o,h,l,c,_ = rows[-1]
    body = abs(c - o)
    if body < body_atr_thr * atr_val: return None
    for i in range(len(rows)-2, max(-1, len(rows)-8), -1):
        o2,h2,l2,c2,_ = rows[i]
        opposite = (c2 < o2) if side == "LONG" else (c2 > o2)
        if opposite:
            return (l2, max(o2, c2)) if side == "LONG" else (min(o2, c2), h2)
    return None

def expected_move_pct(entry: float, targets: List[float]) -> float:
    if not targets: return 0.0
    dist = max(abs(t - entry) for t in targets)
    return dist / max(1e-9, entry)

# =========================
# Движок сигналов
# =========================
class Engine:
    def __init__(self, mkt: MarketState, obm: OrderBookManager) -> None:
        self.mkt = mkt
        self.obm = obm
        self.pending: Dict[str, Dict[str, Any]] = {}

    # ---- Probability profile
    def _probability(self, body_ratio: float, vol_ok: bool, liq_cnt: int, confluence: int, mom_ok: bool, ob_bonus: float=0.0) -> float:
        p = 0.45
        p += min(0.3, max(0.0, body_ratio - BODY_ATR_MULT) * 0.25)
        if vol_ok: p += 0.12
        if liq_cnt >= 3: p += 0.05
        p += min(0.12, 0.04 * max(0, confluence-1))
        if mom_ok: p += MOMENTUM_PROB_BONUS
        p += max(0.0, min(0.12, ob_bonus))
        return max(0.0, min(0.99, p))

    def _validate_rr(self, entry: float, tp: float, sl: float, side: str) -> float:
        reward = (tp - entry) if side == "LONG" else (entry - tp)
        risk = (entry - sl) if side == "LONG" else (sl - entry)
        if risk <= 0: return 0.0
        return reward / risk

    # ---- Pending-зоны (OB/FVG)
    def _build_pending(self, sym:str, side:str, rows1, atr1:float, prefer_ob_first:bool=True) -> None:
        ob = simple_ob(rows1, side, BODY_ATR_MULT, atr1) if prefer_ob_first else None
        if ob:
            zone_lo, zone_hi = ob[0], ob[1]
            mode = "retest-OB"
        else:
            fvg = has_fvg(rows1, bullish=(side=="LONG"))
            if not fvg: return
            zone_lo, zone_hi = fvg
            mode = "retest-FVG"
        created_len = len(rows1)
        self.pending[sym] = {
            "side": side, "zone_lo": float(zone_lo), "zone_hi": float(zone_hi),
            "created_len": created_len, "expire_len": created_len + PENDING_EXPIRE_BARS,
            "atr": float(atr1), "mode": mode,
        }
        if DEBUG_SIGNALS:
            logger.info(f"[pending:set] {sym} {side} zone=({zone_lo:.8g},{zone_hi:.8g}) exp@{created_len + PENDING_EXPIRE_BARS}")

    def _check_pending_trigger(self, sym:str, rows1) -> Optional[Dict[str,Any]]:
        pend = self.pending.get(sym)
        if not pend: return None
        n = len(rows1)
        if n >= pend["expire_len"]:
            if DEBUG_SIGNALS: logger.info(f"[pending:expire] {sym}")
            self.pending.pop(sym, None); return None
        side = pend["side"]; zone_lo = pend["zone_lo"]; zone_hi = pend["zone_hi"]
        o,h,l,c,v = rows1[-1]; rng = max(1e-9, h - l)
        if side == "LONG":
            touched = (l <= zone_hi and h >= zone_lo)
            rejection = (c > o) and ((o - l) / rng >= RETEST_WICK_PCT)
        else:
            touched = (h >= zone_lo and l <= zone_hi)
            rejection = (c < o) and ((h - o) / rng >= RETEST_WICK_PCT)
        if touched and rejection:
            self.pending.pop(sym, None)
            return {"triggered": True, "entry_mode": pend["mode"], "ob_zone": (zone_lo, zone_hi)}
        return None

    # ---- Сигналы на закрытии 1m
    def on_kline_closed_1m(self, sym: str) -> Optional[Dict[str, Any]]:
        rows1 = self.mkt.kline["1"].get(sym) or []
        if len(rows1) < max(ATR_PERIOD_1M+3, VOL_SMA_PERIOD+3, VWAP_WINDOW+VWAP_SLOPE_BARS+2):
            return None

        atr1 = atr(rows1, ATR_PERIOD_1M)
        if atr1 <= 0: return None
        o,h,l,c,v = rows1[-1]
        body = abs(c - o)
        body_ratio = body / atr1
        vols = [x[4] for x in rows1]
        v_sma = sma(vols, VOL_SMA_PERIOD)
        vol_mult_ratio = (v / v_sma) if v_sma > 0 else 0.0

        # адаптив мягких порогов при тишине
        silent_min = (now_ts_ms() - self.mkt.last_signal_sent_ts)/60000.0 if self.mkt.last_signal_sent_ts else 1e9
        cur_body_thr = ADAPT_BODY_ATR if silent_min >= ADAPTIVE_SILENCE_MINUTES else BODY_ATR_MULT
        cur_vol_mult_thr = ADAPT_VOL_MULT if silent_min >= ADAPTIVE_SILENCE_MINUTES else VOL_MULT

        allow_clean = (body_ratio >= cur_body_thr) and (vol_mult_ratio >= cur_vol_mult_thr)
        allow_pending = (body_ratio >= PENDING_BODY_ATR_MIN) or (vol_mult_ratio >= PENDING_VOL_MULT_MIN)

        # VWAP
        tp_price = (h + l + c) / 3.0
        vwap_now, vwap_slope = rolling_vwap(rows1, VWAP_WINDOW)
        if vwap_now == 0.0 and vwap_slope == 0.0: return None
        price_rel_to_vwap = (tp_price - vwap_now) / max(1e-9, vwap_now)

        # 1m SMC/уровни
        SH1, SL1 = find_swings(rows1, SWING_FRAC)
        _, bos_up, bos_dn, bos_up_idx, bos_dn_idx = bos_choch(rows1, SH1, SL1)

        smc_hits = 0
        bullish_bar = c > o
        side: Optional[str] = "LONG" if bullish_bar else "SHORT"
        if bullish_bar:
            if bos_up and (bos_up_idx is None or bos_up_idx >= len(rows1) - BOS_FRESH_BARS): smc_hits += 1
            if USE_SWEEP and swept_liquidity(rows1, SH1, SL1, "LONG"): smc_hits += 1
            if USE_FVG and has_fvg(rows1, bullish=True): smc_hits += 1
        else:
            if bos_dn and (bos_dn_idx is None or bos_dn_idx >= len(rows1) - BOS_FRESH_BARS): smc_hits += 1
            if USE_SWEEP and swept_liquidity(rows1, SH1, SL1, "SHORT"): smc_hits += 1
            if USE_FVG and has_fvg(rows1, bullish=False): smc_hits += 1

        # Momentum
        mom_ok = False
        if len(rows1) > MOMENTUM_N_BARS:
            c_prev = rows1[-MOMENTUM_N_BARS][3]
            ret = (c - c_prev) / max(1e-9, c_prev)
            strong_bar = body_ratio >= BODY_ATR_MOMO
            if abs(ret) >= MOMENTUM_MIN_PCT or strong_bar:
                mom_ok = True
                if smc_hits == 0:
                    side = "LONG" if ret > 0 else "SHORT"

        # Pending-триггер (OB/FVG)
        pend_sig = self._check_pending_trigger(sym, rows1)
        entry_mode = pend_sig.get("entry_mode", "retest") if (pend_sig and pend_sig.get("triggered")) else None
        ob_zone = pend_sig.get("ob_zone") if pend_sig else None

        # ---- Контекст 5m (мягкий): против явного 5m — только ретест
        if USE_5M_FILTER and not entry_mode:
            rows5 = self.mkt.kline["5"].get(sym) or []
            if len(rows5) >= ATR_PERIOD_1M + 3:
                SH5, SL5 = find_swings(rows5, SWING_FRAC)
                trend5, bos5_up, bos5_dn, _, _ = bos_choch(rows5, SH5, SL5)
                if side == "LONG" and trend5 == "DOWN" and not bos5_up: allow_clean = False
                if side == "SHORT" and trend5 == "UP" and not bos5_dn: allow_clean = False

        # VWAP guard: «против склона» и по «не той» стороне VWAP — только pending
        if side == "LONG" and (vwap_slope < VWAP_SLOPE_MIN and price_rel_to_vwap < -VWAP_TOLERANCE): allow_clean = False
        if side == "SHORT" and (vwap_slope > -VWAP_SLOPE_MIN and price_rel_to_vwap > VWAP_TOLERANCE): allow_clean = False

        # Анти-истощение — только pending
        def _extended_run(rows1m: List[Tuple[float,float,float,float,float]], atr1v: float) -> Tuple[bool,float]:
            if len(rows1m) < EXT_RUN_BARS + 1 or atr1v <= 0: return False, 0.0
            c0 = rows1m[-EXT_RUN_BARS-1][3]; c1 = rows1m[-1][3]
            move = abs(c1 - c0); return (move >= EXT_RUN_ATR_MULT * atr1v), move/atr1v
        ext, _ = _extended_run(rows1, atr1)
        if ext and not entry_mode: allow_clean = False

        # Равные уровни близко — только pending (для clean)
        def detect_equal_levels(rows: List[Tuple[float,float,float,float,float]], lookback:int, eps_pct:float, side_in:str) -> Optional[float]:
            n = len(rows); 
            if n < lookback + 1: return None
            levels = []
            if side_in == "LONG":
                highs = sorted([rows[i][1] for i in range(n - lookback, n)])
                for i in range(1, len(highs)):
                    if abs(highs[i] - highs[i-1]) / max(1e-9, highs[i]) <= eps_pct:
                        levels.append((highs[i] + highs[i-1]) / 2.0)
            else:
                lows = sorted([rows[i][2] for i in range(n - lookback, n)])
                for i in range(1, len(lows)):
                    if abs(lows[i] - lows[i-1]) / max(1e-9, lows[i]) <= eps_pct:
                        levels.append((lows[i] + lows[i-1]) / 2.0)
            if not levels: return None
            price = rows[-1][3]
            levels.sort(key=lambda x: abs(x - price))
            return levels[0]
        eql = detect_equal_levels(rows1, EQL_LOOKBACK, EQL_EPS_PCT, side)
        if not entry_mode and allow_clean and eql is not None:
            dist_pct = abs(eql - c) / max(1e-9, c)
            if dist_pct <= EQL_PROX_PCT: allow_clean = False

        # Если ещё не в pending — ставим pending при слабых порогах
        if not entry_mode and (smc_hits >= 1 or mom_ok):
            if allow_clean:
                pass
            elif allow_pending:
                self._build_pending(sym, side, rows1, atr1)
                return None
            else:
                return None

        # Если нет SMC/MOMO и не ретест — пропуск
        if smc_hits == 0 and not mom_ok and not entry_mode:
            return None

        # Тип входа до стакана
        if not entry_mode:
            entry_mode = "clean-pass" if allow_clean else ("momo-pass" if mom_ok else "clean-pass")

        # ---- СТАКАН / ПЛОТНОСТИ ----
        instr = self.mkt.instruments.get(sym) or {}
        tick = float(instr.get("priceFilter", {}).get("tickSize") or instr.get("tickSize") or 0.0) or 1e-6
        bps = LADDER_BPS_DEFAULT if (sym.startswith("BTC") or sym.startswith("ETH")) else LADDER_BPS_ALT
        ladder = AutoLadder(tick=tick, bps=bps)
        obf = self.obm.features(sym, ladder)
        if not obf:
            return None  # без стакана сигналы не отправляем

        # спред: требуем узкий (≤ p25 и ≤ cap)
        spread_ticks_now = obf["spread_ticks"]
        spread_p25 = min(obf["spread_p25_ticks"], SPREAD_TICKS_CAP)
        if spread_ticks_now > spread_p25:
            return None

        # OBI подтверждает сторону
        obi = obf["obi"]
        if side == "LONG" and obi < 0.02:   # слабый дисбаланс — отбой
            if not entry_mode.startswith("retest"): return None
        if side == "SHORT" and obi > -0.02:
            if not entry_mode.startswith("retest"): return None

        # стенки (density): ищем ближайшую по стороне входа и встречную слишком близкую
        bb = obf["best_bid"]; ba = obf["best_ask"]; step = obf["step"]; tick_sz = obf["tick"]
        near_same = None; near_opp = None
        if side == "LONG":
            if obf["walls_bid"]:
                near_same = max([p for p, s in obf["walls_bid"] if p <= c], default=None)
            if obf["walls_ask"]:
                near_opp = min([p for p, s in obf["walls_ask"] if p >= c], default=None)
        else:
            if obf["walls_ask"]:
                near_same = min([p for p, s in obf["walls_ask"] if p >= c], default=None)
            if obf["walls_bid"]:
                near_opp = max([p for p, s in obf["walls_bid"] if p <= c], default=None)

        # встречная стена слишком близко?
        if near_opp is not None:
            dist_ticks_opp = abs(near_opp - c) / tick_sz
            if dist_ticks_opp <= OPPOSITE_WALL_NEAR_TICKS:
                return None

        # бонус за близость к «своей» стене (bounce density)
        entry_mode_density = None
        ob_bonus = 0.0
        if near_same is not None:
            dist_ticks_same = abs(c - near_same) / tick_sz
            if 2 <= dist_ticks_same <= 6:
                entry_mode_density = "BounceDensity"
                ob_bonus = 0.06

        # ---- Контекст старших ТФ (1h/4h) — для пометки контртренда
        note_tags: List[str] = []
        def tf_trend(tf: str) -> str:
            rows = self.mkt.kline[tf].get(sym) or []
            if len(rows) < 30: return "RANGE"
            SH, SL = find_swings(rows, SWING_FRAC)
            trend, *_ = bos_choch(rows, SH, SL)
            return trend

        trend1h = tf_trend("60"); trend4h = tf_trend("240")
        if side == "LONG" and (trend1h == "DOWN" or trend4h == "DOWN"):
            note_tags.append("[Контртренд]")
        if side == "SHORT" and (trend1h == "UP" or trend4h == "UP"):
            note_tags.append("[Контртренд]")

        # ---- Вероятность/пороги c учётом стакана
        liq_cnt = sum(1 for t in self.mkt.liq_events.get(sym, []) if t >= now_ts_ms() - 60_000)
        vol_ok = (vol_mult_ratio >= (ADAPT_VOL_MULT if silent_min >= ADAPTIVE_SILENCE_MINUTES else VOL_MULT))
        entry_mode_final = entry_mode_density or entry_mode
        prob = self._probability(body_ratio, vol_ok, liq_cnt, smc_hits + (1 if entry_mode_density else 0), mom_ok, ob_bonus=ob_bonus)
        base_profile = PROB_THRESHOLDS_STRICT if USE_PROB_70_STRICT else PROB_THRESHOLDS
        thr = base_profile.get(entry_mode_final, base_profile.get("default", 0.5))
        if silent_min >= ADAPTIVE_SILENCE_MINUTES:
            thr = max(0.40, thr + ADAPT_PROB_DELTA)
        if prob < thr:
            if DEBUG_SIGNALS and (prob >= thr - 0.06):
                logger.info(f"[signal:reject] {sym} side={side} prob={prob:.2f} (<{thr:.2f}) entry={entry_mode_final} bodyATR={body_ratio:.2f} OBI={obi:.2f} spreadTicks={spread_ticks_now:.1f}/{spread_p25:.1f}")
            return None

        # --- Постановка целей/стопов
        entry = c
        if side == "LONG":
            sl = (ob_zone[0] - 0.05 * atr1) if ob_zone else (rows1[-1][2] - 0.20 * atr1)
        else:
            sl = (ob_zone[1] + 0.05 * atr1) if ob_zone else (rows1[-1][1] + 0.20 * atr1)

        SH1, SL1 = find_swings(rows1, SWING_FRAC)
        if side == "LONG":
            tp_struct = None
            for i in reversed(SH1):
                ph = rows1[i][1]
                if ph > entry: tp_struct = ph; break
        else:
            tp_struct = None
            for i in reversed(SL1):
                pl = rows1[i][2]
                if pl < entry: tp_struct = pl; break

        tp_pct_struct = abs(tp_struct - entry)/entry if tp_struct else 0.0
        atr_pct_1m = atr1 / max(1e-9, entry)
        tp_pct_timebox = atr_pct_1m * TIMEBOX_MINUTES * TIMEBOX_FACTOR
        tp_pct_atr = max(TP_MIN_PCT, 0.6 * atr1 / max(1e-9, entry))
        tp_pct = max(tp_pct_atr, min(tp_pct_struct if tp_pct_struct > 0 else TP_MAX_PCT, tp_pct_timebox))
        tp_pct = max(TP_MIN_PCT, min(TP_MAX_PCT, tp_pct))
        tp = entry * (1.0 + tp_pct) if side == "LONG" else entry * (1.0 - tp_pct)

        rr = self._validate_rr(entry, tp, sl, side)
        if rr < RR_TARGET:
            if side == "LONG": tp = entry * (1.0 + min(TP_MAX_PCT, tp_pct_timebox))
            else: tp = entry * (1.0 - min(TP_MAX_PCT, tp_pct_timebox))
            rr = self._validate_rr(entry, tp, sl, side)
            if rr < RR_TARGET:
                if DEBUG_SIGNALS:
                    logger.info(f"[signal:reject] {sym} side={side} rr={rr:.2f} (<{RR_TARGET:.2f}) entry={entry_mode_final}")
                return None

        tp_pct_final = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry
        if tp_pct_final < MIN_PROFIT_PCT:
            if DEBUG_SIGNALS:
                logger.info(f"[signal:reject] {sym} side={side} tp_pct={tp_pct_final:.3f} (<{MIN_PROFIT_PCT:.3f})")
            return None

        key = (sym, side)
        last_ts = self.mkt.cooldown.get(key, 0)
        if now_ts_ms() - last_ts < SIGNAL_COOLDOWN_SEC * 1000:
            return None
        self.mkt.cooldown[key] = now_ts_ms()

        return {
            "symbol": sym, "side": side, "entry": float(entry), "tp": float(tp), "sl": float(sl),
            "prob": float(prob), "atr": float(atr1), "body_ratio": float(body_ratio),
            "liq_cnt": int(liq_cnt), "rr": float(rr),
            "confluence": int(max(smc_hits, 1) + (1 if mom_ok else 0) + (1 if entry_mode_density else 0)),
            "entry_mode": entry_mode_final,
            "tags": note_tags,
            "obi": float(obi),
            "spread_ticks": float(spread_ticks_now),
        }

# =========================
# Формат сообщения
# =========================
def format_signal(sig: Dict[str, Any]) -> str:
    sym = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; tp = sig["tp"]; sl = sig["sl"]
    prob = sig["prob"]; liq_cnt = sig.get("liq_cnt", 0)
    body_ratio = sig.get("body_ratio", 0.0); rr = sig.get("rr", 0.0); con = sig.get("confluence", 0)
    entry_mode = sig.get("entry_mode", "n/a")
    tp_pct = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry
    tags = " ".join(sig.get("tags", []))
    add = f" | OBI={sig.get('obi', 0.0):.2f} | spreadTicks={sig.get('spread_ticks', 0.0):.1f}"
    lines = [
        f"⚡️ <b>Сигнал по {sym}</b> {tags}",
        f"Направление: <b>{'LONG' if side=='LONG' else 'SHORT'}</b>",
        f"Вход: <b>{entry_mode}</b> • Горизонт: ~<b>{TIMEBOX_MINUTES} мин</b>",
        f"Текущая цена: <b>{entry:g}</b>",
        f"Тейк: <b>{tp:g}</b> ({pct(tp_pct)})",
        f"Стоп: <b>{sl:g}</b>  •  RR ≈ <b>1:{rr:.2f}</b>",
        f"Вероятность: <b>{pct(prob)}</b>  •  Конфлюэнс: <b>{con}</b>{add}",
        f"Фильтры: TP≥{pct(MIN_PROFIT_PCT)}; профиль={'STRICT 70%' if USE_PROB_70_STRICT else 'Balanced'}",
        f"Основание: тело/ATR={body_ratio:.2f}; ликвидаций(60с)={liq_cnt}",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
    ]
    return "\n".join(lines)

# =========================
# Командный интерфейс (Telegram)
# =========================
async def tg_updates_loop(app: web.Application) -> None:
    tg: Tg = app["tg"]; mkt: MarketState = app["mkt"]; ws: BybitWS = app["ws"]
    offset: Optional[int] = None
    while True:
        try:
            resp = await tg.get_updates(offset=offset, timeout=25)
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("channel_post")
                if not msg: continue
                chat = msg.get("chat", {}); chat_id = chat.get("id")
                text = msg.get("text") or ""
                if not isinstance(chat_id, int) or not text.startswith("/"): continue
                if chat_id not in ALLOWED_CHAT_IDS and chat_id not in PRIMARY_RECIPIENTS: continue
                cmd = text.split()[0].lower()
                if cmd == "/help":
                    await tg.send(chat_id, "Команды:\n/universe — список символов\n/ping — связь\n/status — статус\n/jobs — фоновые задачи\n/diag — диагностика\n/healthz — проверка здоровья сервиса")
                elif cmd == "/universe":
                    await tg.send(chat_id, "Подписаны символы:\n" + ", ".join(app.get("symbols", [])))
                elif cmd == "/ping":
                    ago = (now_ts_ms() - mkt.last_ws_msg_ts)/1000.0
                    await tg.send(chat_id, f"pong • WS last msg {ago:.1f}s ago • symbols={len(app.get('symbols', []))}")
                elif cmd == "/status":
                    ago = (now_ts_ms() - mkt.last_ws_msg_ts)/1000.0
                    mode = 'SCALP TURBO + ORDERBOOK'
                    await tg.send(chat_id, f"✅ Online\nWS: ok (last {ago:.1f}s)\nSymbols: {len(app.get('symbols', []))}\n"
                                           f"Mode: {mode}\nProb profile: {'STRICT 70%' if USE_PROB_70_STRICT else 'Balanced'}\n"
                                           f"Min TP filter: ≥{pct(MIN_PROFIT_PCT)}")
                elif cmd in ("/diag", "/debug"):
                    syms = app.get("symbols", [])
                    k1 = sum(len(mkt.kline['1'].get(s, [])) for s in syms)
                    k5 = sum(len(mkt.kline['5'].get(s, [])) for s in syms)
                    await tg.send(chat_id, f"Diag:\n1m buffers: {k1} pts\n5m buffers: {k5} pts\nCooldowns: {len(mkt.cooldown)}\nLast signal ts: {mkt.last_signal_sent_ts}")
                elif cmd == "/jobs":
                    ws_alive = bool(ws.ws and not ws.ws.closed)
                    tasks = {k: (not app[k].done()) if app.get(k) else False for k in
                             ["ws_task","keepalive_task","hb_task","watchdog_task","tg_updates_task","universe_task"]}
                    await tg.send(chat_id, "Jobs:\n"
                                     f"WS connected: {ws_alive}\n" +
                                     "\n".join(f"{k}: {'running' if v else 'stopped'}" for k,v in tasks.items()))
                elif cmd == "/healthz":
                    t0 = app.get("start_ts") or time.monotonic()
                    uptime = int(time.monotonic() - t0)
                    last_age = int((now_ts_ms() - mkt.last_ws_msg_ts)/1000.0)
                    await tg.send(chat_id, f"ok: true\nuptime_sec: {uptime}\nlast_ws_msg_age_sec: {last_age}\nsymbols: {len(app.get('symbols', []))}")
                else:
                    await tg.send(chat_id, "Неизвестная команда. /help")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"tg_updates_loop error: {e}")
            await asyncio.sleep(2)

# =========================
# Фоновые циклы
# =========================
async def keepalive_loop(app: web.Application) -> None:
    public_url: Optional[str] = app["public_url"]; http: aiohttp.ClientSession = app["http"]
    if not public_url: return
    while True:
        try:
            await asyncio.sleep(KEEPALIVE_SEC)
            with contextlib.suppress(Exception):
                await http.get(public_url, timeout=aiohttp.ClientTimeout(total=10))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"keepalive error: {e}")

async def heartbeat_loop(app: web.Application) -> None:
    tg: Tg = app["tg"]
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_SEC)
            text = f"✅ Cryptobot активен • WS подписки • символов: {len(app.get('symbols', []))}"
            for chat_id in PRIMARY_RECIPIENTS:
                await tg.send(chat_id, text)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"heartbeat error: {e}")

async def watchdog_loop(app: web.Application) -> None:
    mkt: MarketState = app["mkt"]
    while True:
        try:
            await asyncio.sleep(WATCHDOG_SEC)
            ago = (now_ts_ms() - mkt.last_ws_msg_ts) / 1000.0
            logger.info(f"[watchdog] alive; last WS msg {ago:.1f}s ago; symbols={len(app.get('symbols', []))}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"watchdog error: {e}")

async def universe_refresh_loop(app: web.Application) -> None:
    """Динамический фильтр котировок (оборот/изменение) + (де)подписка без рестарта."""
    rest: BybitRest = app["rest"]; ws: BybitWS = app["ws"]
    mkt: MarketState = app["mkt"]
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)
            tickers = await rest.tickers_linear()
            cands: List[Tuple[str, float]] = []
            filtered: List[str] = []
            for t in tickers:
                sym = t.get("symbol") or ""
                if not sym.endswith("USDT"):
                    continue
                try:
                    turn = float(t.get("turnover24h") or 0.0)
                    chg = float(t.get("price24hPcnt") or 0.0)
                except Exception:
                    turn, chg = 0.0, 0.0
                if turn >= TURNOVER_MIN_USD and abs(chg) >= CHANGE24H_MIN_ABS:
                    filtered.append(sym)
                    cands.append((sym, turn))
            cands.sort(key=lambda x: x[1], reverse=True)
            # Всегда держим CORE_SYMBOLS и добираем ликвидные
            symbols_new: List[str] = dedup_preserve(CORE_SYMBOLS + [s for s,_ in cands])[:ACTIVE_SYMBOLS]
            symbols_old: List[str] = app.get("symbols", [])
            if not symbols_new:
                symbols_new = symbols_old or CORE_SYMBOLS[:ACTIVE_SYMBOLS]
            add = sorted(set(symbols_new) - set(symbols_old))
            rem = sorted(set(symbols_old) - set(symbols_new))
            if add or rem:
                if rem:
                    args = []
                    for s in rem:
                        args += [f"tickers.{s}", f"kline.1.{s}", f"kline.5.{s}", f"kline.60.{s}", f"kline.240.{s}", f"liquidation.{s}", f"orderbook.50.{s}"]
                    await ws.unsubscribe(args)
                if add:
                    args = []
                    for s in add:
                        args += [f"tickers.{s}", f"kline.1.{s}", f"kline.5.{s}", f"kline.60.{s}", f"kline.240.{s}", f"liquidation.{s}", f"orderbook.50.{s}"]
                    await ws.subscribe(args)
                    for s in add:
                        with contextlib.suppress(Exception):
                            info = await rest.instrument_info(s)
                            if info:
                                pf = info.get("priceFilter") or {}
                                tick_sz = float(pf.get("tickSize") or info.get("tickSize") or 0.0) or 1e-6
                                mkt.instruments[s] = {"priceFilter": {"tickSize": tick_sz}}
                app["symbols"] = symbols_new
                logger.info(f"[universe] updated; +{len(add)} / -{len(rem)} | total={len(symbols_new)}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"universe_refresh_loop error: {e}")

# =========================
# WS handler
# =========================
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    mkt: MarketState = app["mkt"]; tg: Tg = app["tg"]; eng: Engine = app["engine"]; obm: OrderBookManager = app["obm"]

    topic = data.get("topic") or ""
    if topic.startswith("tickers."):
        d = data.get("data") or {}
        if isinstance(d, dict): mkt.note_ticker(d)

    elif topic.startswith("kline.1."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            mkt.note_kline("1", sym, payload)
            if any(x.get("confirm") for x in payload):
                sig = eng.on_kline_closed_1m(sym)
                if sig:
                    text = format_signal(sig)
                    chats = PRIMARY_RECIPIENTS if ONLY_CHANNEL else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS)
                    for chat_id in chats:
                        with contextlib.suppress(Exception):
                            await tg.send(chat_id, text)
                            logger.info(f"signal sent: {sym} {sig['side']}")
                    mkt.last_signal_sent_ts = now_ts_ms()

    elif topic.startswith("kline.5."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            mkt.note_kline("5", sym, payload)

    elif topic.startswith("kline.60."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            mkt.note_kline("60", sym, payload)

    elif topic.startswith("kline.240."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            mkt.note_kline("240", sym, payload)

    elif topic.startswith("liquidation."):
        arr = data.get("data") or []
        for liq in arr:
            if not isinstance(liq, dict): continue
            sym = (liq.get("s") or liq.get("symbol"))
            ts = int(liq.get("T") or data.get("ts") or now_ts_ms())
            if sym: mkt.note_liq(sym, ts)

    elif topic.startswith("orderbook.50."):
        d = data.get("data") or {}
        if not isinstance(d, dict):
            return
        sym = (d.get("s") or d.get("symbol") or topic.split(".")[-1])
        typ = (d.get("type") or data.get("type") or "").lower()
        bids = d.get("b") or d.get("bids") or []
        asks = d.get("a") or d.get("asks") or []
        seq = d.get("u") or d.get("seq") or d.get("update") or None
        def _to_pairs(arr: Iterable[List[str]]) -> List[Tuple[float,float]]:
            out: List[Tuple[float,float]] = []
            for it in arr:
                if not it: continue
                try:
                    if isinstance(it, dict):
                        p = float(it.get("price") or it.get("p")); s = float(it.get("size") or it.get("s"))
                    else:
                        p = float(it[0]); s = float(it[1])
                except Exception:
                    continue
                out.append((p, s))
            return out
        bids = _to_pairs(bids); asks = _to_pairs(asks)
        instr = mkt.instruments.get(sym) or {}
        tick = float(instr.get("priceFilter", {}).get("tickSize") or instr.get("tickSize") or 0.0) or 1e-6
        if typ == "snapshot":
            obm.note_snapshot(sym, tick, bids, asks, seq)
        else:
            obm.note_delta(sym, tick, bids, asks, seq)

# =========================
# Web-приложение
# =========================
async def handle_health(request: web.Request) -> web.Response:
    app = request.app
    t0 = app.get("start_ts") or time.monotonic()
    uptime = time.monotonic() - t0
    mkt: MarketState = app.get("mkt")
    last_msg_age = int((now_ts_ms() - mkt.last_ws_msg_ts) / 1000) if mkt else None
    return web.json_response({
        "ok": True,
        "uptime_sec": int(uptime),
        "symbols": app.get("symbols", []),
        "last_ws_msg_age_sec": last_msg_age,
    })

async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    logger.info("startup.")

    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        raise RuntimeError("Не задан TELEGRAM_TOKEN")

    http = aiohttp.ClientSession()
    app["http"] = http
    app["tg"] = Tg(token, http)
    app["public_url"] = os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL")

    rest = BybitRest(BYBIT_REST, http); app["rest"] = rest
    tickers = await rest.tickers_linear()

    # Фильтр котировок (оборот≥150M и |change|≥1%) + whitelist CORE_SYMBOLS
    cands: List[Tuple[str, float]] = []
    for t in tickers:
        sym = t.get("symbol") or ""
        if not sym.endswith("USDT"): 
            continue
        try:
            turn = float(t.get("turnover24h") or 0.0)
            chg = float(t.get("price24hPcnt") or 0.0)
        except Exception:
            continue
        if turn >= TURNOVER_MIN_USD and abs(chg) >= CHANGE24H_MIN_ABS:
            cands.append((sym, turn))
    cands.sort(key=lambda x: x[1], reverse=True)

    # Всегда включаем ядро и добираем ликвидные
    symbols = dedup_preserve(CORE_SYMBOLS + [s for s,_ in cands])[:ACTIVE_SYMBOLS]
    if not symbols:
        symbols = CORE_SYMBOLS[:ACTIVE_SYMBOLS]
    app["symbols"] = symbols
    logger.info(f"symbols: {symbols}")

    mkt = MarketState(); app["mkt"] = mkt
    for s in symbols:
        with contextlib.suppress(Exception):
            info = await rest.instrument_info(s)
            if info:
                pf = info.get("priceFilter") or {}
                tick_sz = float(pf.get("tickSize") or info.get("tickSize") or 0.0) or 1e-6
                mkt.instruments[s] = {"priceFilter": {"tickSize": tick_sz}}

    obm = OrderBookManager(); app["obm"] = obm
    app["engine"] = Engine(mkt, obm)

    ws = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http); app["ws"] = ws
    async def _on_msg(msg: Dict[str, Any]) -> None: await ws_on_message(app, msg)
    ws.on_message = _on_msg
    await ws.connect()

    args: List[str] = []
    for s in symbols:
        args += [f"tickers.{s}", f"kline.1.{s}", f"kline.5.{s}", f"kline.60.{s}", f"kline.240.{s}", f"liquidation.{s}", f"orderbook.50.{s}"]
    await ws.subscribe(args)

    # фоновые задачи
    app["ws_task"] = asyncio.create_task(ws.run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["hb_task"] = asyncio.create_task(heartbeat_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_updates_task"] = asyncio.create_task(tg_updates_loop(app))
    app["universe_task"] = asyncio.create_task(universe_refresh_loop(app))

async def on_cleanup(app: web.Application) -> None:
    for key in ("ws_task","keepalive_task","hb_task","watchdog_task","tg_updates_task","universe_task"):
        t = app.get(key)
        if t:
            t.cancel()
            with contextlib.suppress(Exception):
                await t
    ws: BybitWS = app.get("ws")
    if ws and ws.ws and not ws.ws.closed:
        await ws.ws.close()
    http: aiohttp.ClientSession = app.get("http")
    if http:
        await http.close()

def make_app() -> web.Application:
    app = web.Application()
    app["start_ts"] = time.monotonic()
    app.router.add_get("/", handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

def main() -> None:
    setup_logging(LOG_LEVEL)
    logger.info(
        f"cfg: ws={BYBIT_WS_PUBLIC_LINEAR} | active={ACTIVE_SYMBOLS} | "
        f"tp=[{TP_MIN_PCT:.1%}..{TP_MAX_PCT:.1%}] | rr≥{RR_TARGET:.2f} | "
        f"min_tp_filter=≥{MIN_PROFIT_PCT:.1%} | "
        f"prob_profile={'STRICT 70%' if USE_PROB_70_STRICT else 'Balanced'} | "
        f"mode=SCALP TURBO + ORDERBOOK"
    )
    app = make_app()
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("FATAL: app crashed on startup")
        raise
