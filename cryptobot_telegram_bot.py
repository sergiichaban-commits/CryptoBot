# -*- coding: utf-8 -*-
"""
Cryptobot — Telegram сигналы (Bybit V5, Multi-TF S/R)
v4.0 — D(1D) → H4(240m) → H1(60m) уровни + входы от S/R

Главное:
  • Базовые уровни с 1D, уточнение с 4H, входы на 1H.
  • Два режима входа: ретест уровня ИЛИ «clean breakout» (импульс через уровень).
  • Подтверждение: тело свечи ≥ 0.45 ATR(1H) и объём ≥ 1.6× SMA20(1H).
  • VWAP — мягкий фильтр (даёт бонус/штраф вероятности, не режет жёстко).
  • RR ≥ 1:1.2, минимальный потенциальный TP по фильтру ≥ 0.7% (ослаблено).
  • Стакан: избегаем входа «в стену» противоположной стороны слишком близко.
  • /levels <SYMBOL> [tf] — показывает уровни с D+H4+H1 и ближайшие зоны.

Зависимости/окружение:
  TELEGRAM_TOKEN, ALLOWED_CHAT_IDS (через запятую), PUBLIC_URL (необяз.), STALL_EXIT_SEC (необяз.)
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
# Константы/параметры
# =========================
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Торговая логика (ослаблено для большего числа сигналов)
EXEC_TF = "60"                 # рабочий ТФ (H1)
BASE_TF = "D"                  # базовый ТФ (дневка) — тянем по REST
MID_TF = "240"                 # средний ТФ (H4) — получаем по WS
ACTIVE_SYMBOLS = 80

ATR_PERIOD_1H = 14
VOL_SMA_PERIOD = 20
BODY_ATR_ENTRY = 0.45          # тело свечи ≥ 0.45 ATR (ранее 0.6–0.8)
VOL_MULT_ENTRY = 1.60          # объём ≥ 1.6× SMA20

TP_MIN_PCT = 0.007             # 0.7% — минимальный TP для публикации (ослаблено)
TP_MAX_PCT = 0.025             # 2.5% — верхняя планка (скальп-дей)
RR_TARGET = 1.20               # RR требование
PROB_BASE = 0.44               # базовый порог вероятности (бывш. 0.5)
PROB_SILENT_DELTA = -0.06      # если долго молчим — ещё снижаем порог
SILENCE_MIN = 30               # минут тишины для адаптации

VWAP_WINDOW = 60
VWAP_BONUS = 0.06              # бонус к вероятности по направлению уклона
VWAP_PENALTY = -0.05

LEVEL_MERGE_TOL = 0.0018       # «склейка» уровней ~0.18%
TOUCH_TOL = 0.0010             # близость к уровню ~0.10%

SPREAD_TICKS_CAP = 10
SPREAD_Q = 0.30

# Стакан/стены
WALL_PCTL = 95
WALL_LOOKBACK_SEC = 30 * 60
OPPOSITE_WALL_NEAR_TICKS = 8   # не входить, если напротив стена слишком близко
LADDER_BPS_DEFAULT = 0.0004
LADDER_BPS_ALT = 0.0008
ORDERBOOK_DEPTH_BINS = 5
OBI_MIN = {"LONG": 0.02, "SHORT": -0.02}  # мягче, чем раньше

# Вселенная символов
UNIVERSE_REFRESH_SEC = 600
TURNOVER_MIN_USD = 75_000_000.0
CHANGE24H_MIN_ABS = 0.005
CORE_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "TONUSDT", "DOGEUSDT", "ADAUSDT", "LINKUSDT", "AVAXUSDT",
]

# Прочее
SIGNAL_COOLDOWN_SEC = 20
KEEPALIVE_SEC = 13 * 60
WATCHDOG_SEC = 60
STALL_EXIT_SEC = int(os.getenv("STALL_EXIT_SEC", "240"))
PORT = int(os.getenv("PORT", "10000"))

PRIMARY_RECIPIENTS = []  # заполним ниже из ALLOWED_CHAT_IDS / каналов
ALLOWED_CHAT_IDS: List[int] = []
ONLY_CHANNEL = True

DEBUG_SIGNALS = True

# =========================
# Утилиты
# =========================
def pct(x: float) -> str:
    return f"{x:.2%}"

def now_ts_ms() -> int:
    return int(time.time() * 1000)

def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, force=True)

def dedup_preserve(seq: List[str]) -> List[str]:
    seen: Set[str] = set(); out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
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

    async def send_with_keyboard(self, chat_id: int, text: str, buttons: List[List[str]]) -> None:
        keyboard = [[{"text": b} for b in row] for row in buttons]
        payload = {
            "chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True,
            "reply_markup": {"keyboard": keyboard, "resize_keyboard": True, "is_persistent": True, "one_time_keyboard": False}
        }
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

    async def klines(self, symbol: str, interval: str, limit: int = 200) -> List[Tuple[float,float,float,float,float]]:
        # interval: "D","240","60","15","5","1"
        url = f"{self.base}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={min(200, max(1, limit))}"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
        arr = (data.get("result") or {}).get("list") or []
        out: List[Tuple[float,float,float,float,float]] = []
        # Bybit возвращает: [start, open, high, low, close, volume, turnOver] в строках
        for it in arr:
            try:
                o = float(it[1]); h = float(it[2]); l = float(it[3]); c = float(it[4])
                v = float(it[5]) if it[5] is not None else (float(it[6]) if len(it) > 6 else 0.0)
                out.append((o,h,l,c,v))
            except Exception:
                continue
        # сортируем по времени возрастанию (у REST обычно уже по времени)
        return out[-200:]

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
        raw = max(self.tick, mid * self.bps)
        steps = max(1, round(raw / self.tick))
        return steps * self.tick

class OrderBookState:
    __slots__ = ("tick", "seq", "bids", "asks", "last_mid", "last_spreads")

    def __init__(self, tick: float) -> None:
        self.tick = max(tick, 1e-12)
        self.seq: Optional[int] = None
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_mid: float = 0.0
        self.last_spreads: List[float] = []

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
        self.wall_hist: Dict[str, List[Tuple[int, float, float]]] = {}

    def ensure(self, sym: str, tick: float) -> OrderBookState:
        if sym not in self.books:
            self.books[sym] = OrderBookState(tick)
        self.ticks[sym] = tick
        return self.books[sym]

    def note_snapshot(self, sym: str, tick: float, bids: Iterable[Tuple[float,float]], asks: Iterable[Tuple[float,float]], seq: Optional[int]) -> None:
        ob = self.ensure(sym, tick); ob.apply_snapshot(bids, asks, seq)

    def note_delta(self, sym: str, tick: float, bids: Iterable[Tuple[float,float]], asks: Iterable[Tuple[float,float]], seq: Optional[int]) -> None:
        ob = self.ensure(sym, tick); ob.apply_delta(bids, asks, seq)

    def p_quantile_spread_ticks(self, sym: str, q: float = SPREAD_Q) -> Optional[float]:
        ob = self.books.get(sym)
        if not ob or not ob.last_spreads:
            return None
        arr = sorted(ob.last_spreads[-600:])
        idx = int(max(0, min(len(arr)-1, q * (len(arr)-1))))
        return arr[idx] / max(ob.tick, 1e-12)

    def features(self, sym: str, ladder: AutoLadder) -> Optional[Dict[str, Any]]:
        ob = self.books.get(sym)
        if not ob or ob.last_mid <= 0: return None
        step = ladder.step(ob.last_mid)

        def agg(book: Dict[float,float], reverse: bool) -> List[Tuple[float,float]]:
            buckets: Dict[float, float] = {}
            for p, s in book.items():
                b = round(p / step) * step
                buckets[b] = buckets.get(b, 0.0) + s * p
            levels = sorted(buckets.items(), key=lambda x: x[0], reverse=reverse)
            return levels

        bids_levels = agg(ob.bids, True)
        asks_levels = agg(ob.asks, False)

        b_not = sum(s for _, s in bids_levels[:ORDERBOOK_DEPTH_BINS])
        a_not = sum(s for _, s in asks_levels[:ORDERBOOK_DEPTH_BINS])
        denom = max(1e-9, a_not + b_not)
        obi = (b_not - a_not) / denom

        ts_now = now_ts_ms()
        hist = self.wall_hist.setdefault(sym, [])
        for p, s in (bids_levels[:10] + asks_levels[:10]):
            hist.append((ts_now, p, s))
        cutoff = ts_now - WALL_LOOKBACK_SEC * 1000
        self.wall_hist[sym] = [x for x in hist if x[0] >= cutoff]
        only_vals = [x[2] for x in self.wall_hist[sym]]
        wall_thr = 0.0
        if only_vals:
            only_vals_sorted = sorted(only_vals)
            idx = int(len(only_vals_sorted) * WALL_PCTL / 100)
            idx = min(max(idx, 0), len(only_vals_sorted)-1)
            wall_thr = only_vals_sorted[idx]

        walls_bid = [(p, s) for p, s in bids_levels if s >= wall_thr]
        walls_ask = [(p, s) for p, s in asks_levels if s >= wall_thr]

        bb, ba = ob.best()
        spread_now = (ba - bb) / max(ob.tick, 1e-12) if (bb and ba) else 0.0
        spread_q = self.p_quantile_spread_ticks(sym, SPREAD_Q) or spread_now

        return {
            "step": float(step),
            "obi": float(obi),
            "walls_bid": walls_bid,
            "walls_ask": walls_ask,
            "best_bid": float(bb) if bb else None,
            "best_ask": float(ba) if ba else None,
            "spread_ticks": float(spread_now),
            "spread_pq_ticks": float(spread_q),
            "tick": float(ob.tick),
        }

# =========================
# Market state
# =========================
class MarketState:
    def __init__(self) -> None:
        self.tickers: Dict[str, Dict[str, Any]] = {}
        # Буферы по ТФ
        self.kline: Dict[str, Dict[str, List[Tuple[float,float,float,float,float]]]] = {
            "60": {}, "240": {}, "D": {}
        }
        self.kline_maxlen = 900
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

    def set_klines(self, tf: str, sym: str, rows: List[Tuple[float,float,float,float,float]]) -> None:
        self.kline.setdefault(tf, {})[sym] = rows[-self.kline_maxlen:]
        self.last_ws_msg_ts = now_ts_ms()

# =========================
# Индикаторы/вспомогательные
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
    need = window + 10
    if n < need: return 0.0, 0.0
    tp = [(r[1] + r[2] + r[3]) / 3.0 for r in rows]
    v  = [r[4] for r in rows]
    tpw_now = sum(tp[-window+i] * v[-window+i] for i in range(window))
    vw_now  = sum(v[-window+i] for i in range(window)) or 1e-9
    vwap_now = tpw_now / vw_now
    tpw_prev = sum(tp[-window-10+i] * v[-window-10+i] for i in range(window))
    vw_prev  = sum(v[-window-10+i] for i in range(window)) or 1e-9
    vwap_prev = tpw_prev / vw_prev
    slope = vwap_now - vwap_prev
    return vwap_now, slope

def find_swings(rows: List[Tuple[float,float,float,float,float]], frac: int = 2) -> Tuple[List[int], List[int]]:
    n = len(rows); sh: List[int] = []; sl: List[int] = []
    for i in range(frac, n - frac):
        h = rows[i][1]; l = rows[i][2]
        hs = [rows[j][1] for j in range(i-frac, i+frac+1)]
        ls = [rows[j][2] for j in range(i-frac, i+frac+1)]
        if h == max(hs) and hs.count(h) == 1: sh.append(i)
        if l == min(ls) and ls.count(l) == 1: sl.append(i)
    return sh, sl

# =========================
# Уровни S/R (D + H4 + H1)
# =========================
def build_levels(rows_D, rows_H4, rows_H1, last_price: float) -> Tuple[List[float], List[float], Dict[str, Any]]:
    def levels_from_swings(rows, take: int = 8) -> Tuple[List[float], List[float]]:
        SH, SL = find_swings(rows, 2)
        highs = [rows[i][1] for i in SH][-take:]
        lows  = [rows[i][2] for i in SL][-take:]
        return highs, lows

    R_D, S_D = levels_from_swings(rows_D, 12) if rows_D else ([], [])
    R_H4, S_H4 = levels_from_swings(rows_H4, 10) if rows_H4 else ([], [])
    R_H1, S_H1 = levels_from_swings(rows_H1, 8) if rows_H1 else ([], [])

    def merge_levels(arrs, tol=LEVEL_MERGE_TOL):
        out: List[float] = []
        for arr in arrs:
            for x in arr:
                if not out:
                    out.append(x); continue
                if all(abs(x - y) / max(1e-9, y) > tol for y in out):
                    out.append(x)
        return sorted(out)

    R = merge_levels([R_D, R_H4, R_H1])
    S = merge_levels([S_D, S_H4, S_H1])

    # Ближайшие
    R_near = [x for x in R if x > last_price]
    S_near = [x for x in S if x < last_price]
    R_near = sorted(R_near)[:3]
    S_near = sorted(S_near, reverse=True)[:3]

    return R_near, S_near, {"R_all": R, "S_all": S}

# =========================
# Движок сигналов (H1)
# =========================
class Engine:
    def __init__(self, mkt: MarketState, obm: OrderBookManager) -> None:
        self.mkt = mkt
        self.obm = obm
        self.last_levels: Dict[str, Dict[str, Any]] = {}  # кеш уровней

    def _probability(self, body_ratio: float, vol_ok: bool, vwap_bias: float, obi: float, confluence: int) -> float:
        p = PROB_BASE
        p += min(0.25, max(0.0, body_ratio - BODY_ATR_ENTRY) * 0.30)
        if vol_ok: p += 0.12
        # VWAP
        p += VWAP_BONUS if vwap_bias > 0 else (VWAP_PENALTY if vwap_bias < 0 else 0.0)
        # Ордера
        p += max(-0.04, min(0.04, obi * 0.20))
        # Конфлюэнс (наложение уровней)
        p += min(0.10, 0.04 * max(0, confluence-1))
        return max(0.0, min(0.99, p))

    def _rr(self, entry: float, tp: float, sl: float, side: str) -> float:
        reward = (tp - entry) if side == "LONG" else (entry - tp)
        risk = (entry - sl) if side == "LONG" else (sl - entry)
        if risk <= 0: return 0.0
        return reward / risk

    def _choose_tp(self, entry: float, side: str, r_levels: List[float], s_levels: List[float], atr1h: float) -> float:
        # целимся в противоположный ближайший уровень; fallback — ATR-базированный скальп
        if side == "LONG":
            for r in r_levels:
                if r > entry: return r
            return entry * (1.0 + max(TP_MIN_PCT, 0.6 * atr1h / max(1e-9, entry)))
        else:
            for s in s_levels:
                if s < entry: return s
            return entry * (1.0 - max(TP_MIN_PCT, 0.6 * atr1h / max(1e-9, entry)))

    def _touches_level(self, price: float, levels: List[float]) -> Optional[float]:
        for lv in levels:
            if abs(price - lv) / max(1e-9, lv) <= TOUCH_TOL:
                return lv
        return None

    def _near_wall_block(self, side: str, c: float, obf: Dict[str, Any]) -> bool:
        tick = obf["tick"]
        walls_bid = obf.get("walls_bid") or []
        walls_ask = obf.get("walls_ask") or []
        if side == "LONG":
            near_opp = [p for p, s in walls_ask if p >= c]
            if near_opp:
                dist_ticks = abs(min(near_opp) - c) / max(tick,1e-12)
                return dist_ticks <= OPPOSITE_WALL_NEAR_TICKS
        else:
            near_opp = [p for p, s in walls_bid if p <= c]
            if near_opp:
                dist_ticks = abs(c - max(near_opp)) / max(tick,1e-12)
                return dist_ticks <= OPPOSITE_WALL_NEAR_TICKS
        return False

    def on_h1_close(self, sym: str) -> Optional[Dict[str, Any]]:
        H1 = self.mkt.kline["60"].get(sym) or []
        H4 = self.mkt.kline["240"].get(sym) or []
        D1 = self.mkt.kline["D"].get(sym) or []

        if len(H1) < max(ATR_PERIOD_1H+3, VOL_SMA_PERIOD+3, VWAP_WINDOW+12) or len(H4) < 60 or len(D1) < 60:
            return None

        o,h,l,c,v = H1[-1]
        atr1h = atr(H1, ATR_PERIOD_1H)
        if atr1h <= 0: return None

        body = abs(c - o)
        body_ratio = body / atr1h
        vols = [x[4] for x in H1]
        v_sma = sma(vols, VOL_SMA_PERIOD)
        vol_ok = (v >= (VOL_MULT_ENTRY * v_sma)) if v_sma > 0 else False

        vwap_now, vwap_slope = rolling_vwap(H1, VWAP_WINDOW)
        vwap_bias = 0.0
        if vwap_now and vwap_slope:
            if c > vwap_now and vwap_slope > 0: vwap_bias = 1.0
            elif c < vwap_now and vwap_slope < 0: vwap_bias = 1.0
            elif (c > vwap_now and vwap_slope < 0) or (c < vwap_now and vwap_slope > 0): vwap_bias = -1.0

        # Уровни
        R_near, S_near, cache = build_levels(D1, H4, H1, c)
        self.last_levels[sym] = {"R": R_near, "S": S_near, **cache, "last": c}

        # Конфлюэнс (наложение уровней поблизости)
        confluence = 1
        for r in R_near:
            if any(abs(r - x)/max(1e-9,x) <= LEVEL_MERGE_TOL for x in (cache["R_all"] or [])):
                confluence += 1
                break
        for s_ in S_near:
            if any(abs(s_ - x)/max(1e-9,x) <= LEVEL_MERGE_TOL for x in (cache["S_all"] or [])):
                confluence += 1
                break

        # Сценарии входа: ретест уровня или «clean breakout»
        long_level = self._touches_level(c, S_near)
        short_level = self._touches_level(c, R_near)

        side: Optional[str] = None
        mode: Optional[str] = None

        # Ретест: касание уровня и разворотная свеча по телу/объёму
        if long_level and (body_ratio >= BODY_ATR_ENTRY) and vol_ok:
            side, mode = "LONG", "retest-S"
        elif short_level and (body_ratio >= BODY_ATR_ENTRY) and vol_ok:
            side, mode = "SHORT", "retest-R"
        else:
            # Clean breakout: уверенное закрытие за уровнем с импульсом
            # LONG: закрылись выше ближайшего R_near; SHORT: ниже ближайшего S_near
            if R_near and c > R_near[0] and (body_ratio >= BODY_ATR_ENTRY) and vol_ok:
                side, mode = "LONG", "clean-breakout↑"
            elif S_near and c < S_near[0] and (body_ratio >= BODY_ATR_ENTRY) and vol_ok:
                side, mode = "SHORT", "clean-breakout↓"

        if not side:
            return None

        # Инструмент/тик
        instr = self.mkt.instruments.get(sym) or {}
        tick = float((instr.get("priceFilter") or {}).get("tickSize") or instr.get("tickSize") or 0.0) or 1e-6
        ladder = AutoLadder(tick=tick, bps=(LADDER_BPS_DEFAULT if (sym.startswith("BTC") or sym.startswith("ETH")) else LADDER_BPS_ALT))
        obf = self.obm.features(sym, ladder)
        if not obf: return None

        # Спред-фильтр (мягкий)
        spread_now = obf["spread_ticks"]
        spread_thr = min(obf["spread_pq_ticks"] or spread_now, SPREAD_TICKS_CAP)
        if spread_now > spread_thr:
            return None

        # OBI мягкий гард
        obi = obf["obi"]
        if side == "LONG" and obi < OBI_MIN["LONG"]:
            return None
        if side == "SHORT" and obi > OBI_MIN["SHORT"]:
            return None

        # Не входить "в стену"
        if self._near_wall_block(side, c, obf):
            return None

        # Стоп за уровень (1.0–1.3 ATR)
        if side == "LONG":
            base_lv = long_level if long_level else (S_near[0] if S_near else c - 1.0*atr1h)
            sl = min(base_lv - 0.10 * atr1h, l - 0.15 * atr1h)
        else:
            base_lv = short_level if short_level else (R_near[0] if R_near else c + 1.0*atr1h)
            sl = max(base_lv + 0.10 * atr1h, h + 0.15 * atr1h)

        # Тейк — ближайший противоположный уровень, иначе ATR-фолбэк
        tp = self._choose_tp(c, side, R_near, S_near, atr1h)

        rr = self._rr(c, tp, sl, side)
        if rr < RR_TARGET:
            return None

        tp_pct_final = (tp - c) / c if side == "LONG" else (c - tp) / c
        if tp_pct_final < TP_MIN_PCT:
            return None

        # Вероятность
        silent_min = (now_ts_ms() - self.mkt.last_signal_sent_ts)/60000.0 if self.mkt.last_signal_sent_ts else 1e9
        prob = self._probability(body_ratio, vol_ok, vwap_bias, obi, confluence)
        thr = PROB_BASE + (PROB_SILENT_DELTA if silent_min >= SILENCE_MIN else 0.0)
        if prob < thr:
            if DEBUG_SIGNALS and (prob >= thr - 0.05):
                logger.info(f"[signal:reject] {sym} side={side} prob={prob:.2f} (<{thr:.2f}) bodyATR={body_ratio:.2f} volOK={vol_ok} OBI={obi:.2f}")
            return None

        # Кулдаун по направлению
        key = (sym, side)
        last_ts = self.mkt.cooldown.get(key, 0)
        if now_ts_ms() - last_ts < SIGNAL_COOLDOWN_SEC * 1000:
            return None
        self.mkt.cooldown[key] = now_ts_ms()

        return {
            "symbol": sym, "side": side, "entry": float(c), "tp": float(tp), "sl": float(sl),
            "prob": float(prob), "body_ratio": float(body_ratio), "rr": float(rr),
            "entry_mode": mode, "obi": float(obi), "spread_ticks": float(spread_now),
        }

# =========================
# Формат сообщения
# =========================
def format_signal(sig: Dict[str, Any]) -> str:
    sym = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; tp = sig["tp"]; sl = sig["sl"]
    prob = sig["prob"]; body_ratio = sig.get("body_ratio", 0.0); rr = sig.get("rr", 0.0)
    entry_mode = sig.get("entry_mode", "n/a")
    tp_pct = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry
    add = f" | OBI={sig.get('obi', 0.0):.2f} | spreadTicks={sig.get('spread_ticks', 0.0):.1f}"
    lines = [
        f"⚡️ <b>Сигнал по {sym}</b>",
        f"Направление: <b>{'LONG' if side=='LONG' else 'SHORT'}</b>",
        f"Вход: <b>{entry_mode}</b> • ТФ: <b>H1</b>",
        f"Цена: <b>{entry:g}</b>",
        f"Тейк: <b>{tp:g}</b> ({pct(tp_pct)})",
        f"Стоп: <b>{sl:g}</b>  •  RR ≈ <b>1:{rr:.2f}</b>",
        f"Вероятность: <b>{pct(prob)}</b> • Основание: тело/ATR={body_ratio:.2f}{add}",
        f"Фильтры: TP≥{pct(TP_MIN_PCT)}; VWAP — мягкий",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
    ]
    return "\n".join(lines)

# =========================
# /levels: показать уровни
# =========================
def calc_levels(app: web.Application, sym: str, tf: str = "60") -> Optional[str]:
    mkt: MarketState = app["mkt"]; obm: OrderBookManager = app["obm"]
    tf = tf.upper()
    tf_map = {"1":"60","5":"60","60":"60","240":"240","D":"D"}
    use_tf = tf_map.get(tf, "60")

    H1 = mkt.kline["60"].get(sym) or []
    H4 = mkt.kline["240"].get(sym) or []
    D1 = mkt.kline["D"].get(sym) or []
    rows = {"60": H1, "240": H4, "D": D1}[use_tf]
    if len(rows) < 60:
        return None

    last = rows[-1][3]
    R_near, S_near, meta = build_levels(D1, H4, H1, last)

    # VWAP/ATR
    a = atr(rows, ATR_PERIOD_1H if use_tf == "60" else 14)
    vwap_now, vwap_slope = rolling_vwap(rows, VWAP_WINDOW)
    vwap_line = f"VWAP: {vwap_now:g} ({'↗' if vwap_slope>0 else ('↘' if vwap_slope<0 else '→')})" if vwap_now else "VWAP: —"
    atr_line  = f"ATR({ATR_PERIOD_1H if use_tf=='60' else 14}): {a:g}"

    def fmt_lvls(name, arr):
        return f"{name}: " + (", ".join(f"{x:g}" for x in arr) if arr else "—")

    # Стены
    instr = mkt.instruments.get(sym) or {}
    tick = float((instr.get("priceFilter") or {}).get("tickSize") or 1e-6) or 1e-6
    ladder = AutoLadder(tick=tick, bps=(LADDER_BPS_DEFAULT if (sym.startswith("BTC") or sym.startswith("ETH")) else LADDER_BPS_ALT))
    obf = obm.features(sym, ladder) or {}

    walls_up = [p for p, s in (obf.get("walls_ask") or []) if p > last][:3]
    walls_dn = sorted([p for p, s in (obf.get("walls_bid") or []) if p < last], reverse=True)[:3]

    lines = [
        f"📊 <b>Уровни {sym} [{use_tf}]</b>",
        f"Цена: <b>{last:g}</b>",
        fmt_lvls("Сопротивления (ближайшие)", R_near),
        fmt_lvls("Поддержки (ближайшие)",    S_near),
        vwap_line + f" • {atr_line}",
        "Стены: ask≈" + (", ".join(f"{x:g}" for x in walls_up) if walls_up else "—") +
        " | bid≈" + (", ".join(f"{x:g}" for x in walls_dn) if walls_dn else "—"),
        "Источник: 1D + 4H + 1H (свинги), VWAP, плотности стакана",
    ]
    return "\n".join(lines)

# =========================
# Командный интерфейс
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
                    kb = [
                        ["/ping", "/status"],
                        ["/healthz", "/diag"],
                        ["/jobs", "/levels BTCUSDT D"]
                    ]
                    await tg.send_with_keyboard(
                        chat_id,
                        "Команды:\n"
                        "/ping — связь\n"
                        "/status — статус\n"
                        "/healthz — проверка здоровья\n"
                        "/diag — диагностика\n"
                        "/jobs — фоновые задачи\n"
                        "/levels <SYMBOL> [tf: 60|240|D] — уровни S/R (1D+4H+1H)",
                        kb
                    )

                elif cmd == "/universe":
                    await tg.send(chat_id, "Подписаны символы:\n" + ", ".join(app.get("symbols", [])))

                elif cmd == "/ping":
                    ago = (now_ts_ms() - mkt.last_ws_msg_ts)/1000.0
                    await tg.send(chat_id, f"pong • WS last msg {ago:.1f}s ago • symbols={len(app.get('symbols', []))}")

                elif cmd == "/status":
                    ago = (now_ts_ms() - mkt.last_ws_msg_ts)/1000.0
                    silent_min = (now_ts_ms() - mkt.last_signal_sent_ts)/60000.0 if mkt.last_signal_sent_ts else 1e9
                    stall_risk = "HIGH" if ago >= STALL_EXIT_SEC else ("MED" if ago >= STALL_EXIT_SEC/2 else "LOW")
                    mode = 'S/R Multi-TF (D→H4→H1): retest + clean breakout'
                    await tg.send(chat_id, f"✅ Online\nWS: ok (last {ago:.1f}s)\nSymbols: {len(app.get('symbols', []))}\n"
                                           f"Mode: {mode}\nRR≥{RR_TARGET:.2f} • Min TP ≥{pct(TP_MIN_PCT)}\n"
                                           f"VWAP: мягкий • Prob base: {PROB_BASE:.2f}\n"
                                           f"Silent (signals): {silent_min:.1f}m\nStall risk: {stall_risk}")

                elif cmd in ("/diag", "/debug"):
                    syms = app.get("symbols", [])
                    k60 = sum(len(mkt.kline['60'].get(s, [])) for s in syms)
                    k240 = sum(len(mkt.kline['240'].get(s, [])) for s in syms)
                    kD = sum(len(mkt.kline['D'].get(s, [])) for s in syms)
                    ago = (now_ts_ms() - mkt.last_ws_msg_ts)/1000.0
                    silent_min = (now_ts_ms() - mkt.last_signal_sent_ts)/60000.0 if mkt.last_signal_sent_ts else 1e9
                    await tg.send(chat_id, f"Diag:\nH1 buffers: {k60} pts\nH4 buffers: {k240} pts\nD1 buffers: {kD} pts\n"
                                           f"Cooldowns: {len(mkt.cooldown)}\nLast signal ts: {mkt.last_signal_sent_ts}\n"
                                           f"WS last msg age: {ago:.1f}s\nSilent minutes: {silent_min:.1f}")

                elif cmd == "/jobs":
                    ws_alive = bool(ws.ws and not ws.ws.closed)
                    tasks = {k: (not app[k].done()) if app.get(k) else False for k in
                             ["ws_task","keepalive_task","watchdog_task","tg_updates_task","universe_task","d1_refresh_task"]}
                    await tg.send(chat_id, "Jobs:\n"
                                     f"WS connected: {ws_alive}\n" +
                                     "\n".join(f"{k}: {'running' if v else 'stopped'}" for k,v in tasks.items()))

                elif cmd == "/healthz":
                    t0 = app.get("start_ts") or time.monotonic()
                    uptime = int(time.monotonic() - t0)
                    last_age = int((now_ts_ms() - mkt.last_ws_msg_ts)/1000.0)
                    await tg.send(chat_id, f"ok: true\nuptime_sec: {uptime}\nlast_ws_msg_age_sec: {last_age}\nsymbols: {len(app.get('symbols', []))}")

                elif cmd.startswith("/levels"):
                    parts = text.split()
                    sym = parts[1].upper() if len(parts) >= 2 else None
                    tf = parts[2] if len(parts) >= 3 else "60"
                    if not sym or tf.upper() not in ("60","240","D","1","5"):
                        await tg.send(chat_id, "Формат: /levels <SYMBOL> [tf: 60|240|D]\nНапример: /levels BTCUSDT D")
                    else:
                        msg = calc_levels(app, sym, tf.upper())
                        if msg:
                            await tg.send(chat_id, msg)
                        else:
                            await tg.send(chat_id, "Недостаточно данных для расчёта уровней. Буферы наполняются.")

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

async def watchdog_loop(app: web.Application) -> None:
    mkt: MarketState = app["mkt"]
    while True:
        try:
            await asyncio.sleep(WATCHDOG_SEC)
            ago = (now_ts_ms() - mkt.last_ws_msg_ts) / 1000.0
            logger.info(f"[watchdog] alive; last WS msg {ago:.1f}s ago; symbols={len(app.get('symbols', []))}")
            if ago >= STALL_EXIT_SEC:
                logger.error(f"[watchdog] WS stalled for {ago:.1f}s (>= {STALL_EXIT_SEC}s). Exiting for platform restart.")
                os._exit(3)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"watchdog error: {e}")

async def universe_refresh_loop(app: web.Application) -> None:
    rest: BybitRest = app["rest"]; ws: BybitWS = app["ws"]; mkt: MarketState = app["mkt"]
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)
            tickers = await rest.tickers_linear()

            primary: List[Tuple[str, float]] = []
            fallback: List[Tuple[str, float]] = []

            for t in tickers:
                sym = t.get("symbol") or ""
                if not sym.endswith("USDT"):
                    continue
                try:
                    turn = float(t.get("turnover24h") or 0.0)
                    chg  = float(t.get("price24hPcnt") or 0.0)
                except Exception:
                    continue
                if turn >= TURNOVER_MIN_USD:
                    fallback.append((sym, turn))
                    if abs(chg) >= CHANGE24H_MIN_ABS:
                        primary.append((sym, turn))

            primary.sort(key=lambda x: x[1], reverse=True)
            fallback.sort(key=lambda x: x[1], reverse=True)

            symbols_new: List[str] = dedup_preserve(CORE_SYMBOLS + [s for s,_ in primary] + [s for s,_ in fallback])[:ACTIVE_SYMBOLS]
            symbols_old: List[str] = app.get("symbols", [])
            if not symbols_new:
                symbols_new = symbols_old or CORE_SYMBOLS[:ACTIVE_SYMBOLS]

            add = sorted(set(symbols_new) - set(symbols_old))
            rem = sorted(set(symbols_old) - set(symbols_new))
            if add or rem:
                if rem:
                    args = []
                    for s in rem:
                        args += [f"tickers.{s}", f"kline.60.{s}", f"kline.240.{s}", f"orderbook.50.{s}"]
                    await ws.unsubscribe(args)
                if add:
                    args = []
                    for s in add:
                        args += [f"tickers.{s}", f"kline.60.{s}", f"kline.240.{s}", f"orderbook.50.{s}"]
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

async def d1_refresh_loop(app: web.Application) -> None:
    """Периодически подтягиваем дневные свечи по REST (для всех символов)."""
    rest: BybitRest = app["rest"]; mkt: MarketState = app["mkt"]
    while True:
        try:
            await asyncio.sleep(7 * 60)  # раз в ~7 минут
            for s in app.get("symbols", []):
                with contextlib.suppress(Exception):
                    rows_D = await rest.klines(s, "D", limit=180)
                    if rows_D:
                        mkt.set_klines("D", s, rows_D)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"d1_refresh_loop error: {e}")

# =========================
# WS handler
# =========================
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    mkt: MarketState = app["mkt"]; tg: Tg = app["tg"]; eng: Engine = app["engine"]; obm: OrderBookManager = app["obm"]

    topic = data.get("topic") or ""
    if topic.startswith("tickers."):
        d = data.get("data") or {}
        if isinstance(d, dict): mkt.note_ticker(d)

    elif topic.startswith("kline.60."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            mkt.note_kline("60", sym, payload)
            if any(x.get("confirm") for x in payload):
                sig = eng.on_h1_close(sym)
                if sig:
                    text = format_signal(sig)
                    chats = PRIMARY_RECIPIENTS if ONLY_CHANNEL else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS)
                    for chat_id in chats:
                        with contextlib.suppress(Exception):
                            await tg.send(chat_id, text)
                            logger.info(f"signal sent: {sym} {sig['side']}")
                    mkt.last_signal_sent_ts = now_ts_ms()

    elif topic.startswith("kline.240."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            mkt.note_kline("240", sym, payload)

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
        tick = float((instr.get("priceFilter") or {}).get("tickSize") or instr.get("tickSize") or 0.0) or 1e-6
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

    # Разбор ALLOWED_CHAT_IDS
    global ALLOWED_CHAT_IDS, PRIMARY_RECIPIENTS
    ids_env = (os.getenv("ALLOWED_CHAT_IDS") or "").strip()
    if ids_env:
        try:
            ALLOWED_CHAT_IDS = [int(x.strip()) for x in ids_env.split(",") if x.strip()]
        except Exception:
            ALLOWED_CHAT_IDS = []
    PRIMARY_RECIPIENTS = [i for i in ALLOWED_CHAT_IDS if i < 0] or ALLOWED_CHAT_IDS[:1] or []

    http = aiohttp.ClientSession()
    app["http"] = http
    app["tg"] = Tg(token, http)
    app["public_url"] = os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL")

    rest = BybitRest(BYBIT_REST, http); app["rest"] = rest
    tickers = await rest.tickers_linear()

    primary: List[Tuple[str, float]] = []
    fallback: List[Tuple[str, float]] = []
    for t in tickers:
        sym = t.get("symbol") or ""
        if not sym.endswith("USDT"):
            continue
        try:
            turn = float(t.get("turnover24h") or 0.0)
            chg  = float(t.get("price24hPcnt") or 0.0)
        except Exception:
            continue
        if turn >= TURNOVER_MIN_USD:
            fallback.append((sym, turn))
            if abs(chg) >= CHANGE24H_MIN_ABS:
                primary.append((sym, turn))

    primary.sort(key=lambda x: x[1], reverse=True)
    fallback.sort(key=lambda x: x[1], reverse=True)

    symbols = dedup_preserve(CORE_SYMBOLS + [s for s,_ in primary] + [s for s,_ in fallback])[:ACTIVE_SYMBOLS]
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
        # Инициализируем дневные свечи (REST) при старте
        with contextlib.suppress(Exception):
            rows_D = await rest.klines(s, "D", limit=180)
            if rows_D:
                mkt.set_klines("D", s, rows_D)

    obm = OrderBookManager(); app["obm"] = obm
    app["engine"] = Engine(mkt, obm)

    ws = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http); app["ws"] = ws
    async def _on_msg(msg: Dict[str, Any]) -> None: await ws_on_message(app, msg)
    ws.on_message = _on_msg
    await ws.connect()

    args: List[str] = []
    for s in symbols:
        args += [f"tickers.{s}", f"kline.60.{s}", f"kline.240.{s}", f"orderbook.50.{s}"]
    await ws.subscribe(args)

    # Фоновые задачи
    app["ws_task"] = asyncio.create_task(ws.run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_updates_task"] = asyncio.create_task(tg_updates_loop(app))
    app["universe_task"] = asyncio.create_task(universe_refresh_loop(app))
    app["d1_refresh_task"] = asyncio.create_task(d1_refresh_loop(app))

    # Сообщение о запуске
    try:
        tg: Tg = app["tg"]
        for chat_id in PRIMARY_RECIPIENTS or ALLOWED_CHAT_IDS:
            await tg.send(chat_id, "🟢 Cryptobot запущен (S/R Multi-TF: 1D→4H→1H)")
    except Exception:
        logger.warning("startup notify failed")

async def on_cleanup(app: web.Application) -> None:
    for key in ("ws_task","keepalive_task","watchdog_task","tg_updates_task","universe_task","d1_refresh_task"):
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
        f"prob_base={PROB_BASE:.2f} | strategy=S/R Multi-TF (D→H4→H1); entries: retest+clean breakout"
    )
    app = make_app()
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("FATAL: app crashed on startup")
        raise
