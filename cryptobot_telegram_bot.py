# -*- coding: utf-8 -*-
"""
Cryptobot — Derivatives Signals (Bybit V5, USDT Perpetuals)
v6.1 — SMC-lite + OI + Liquidations + Impulse + VWAP + ATR targets + TREND mode
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web

# =========================
# Конфиг
# =========================
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

PORT = int(os.getenv("PORT", "10000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or ""
ALLOWED_CHAT_IDS = [int(x) for x in (os.getenv("ALLOWED_CHAT_IDS") or "").split(",") if x.strip()]
PRIMARY_RECIPIENTS = [i for i in ALLOWED_CHAT_IDS if i < 0] or ALLOWED_CHAT_IDS[:1] or []
ONLY_CHANNEL = True

# Вселенная: мягкие фильтры (ENV)
UNIVERSE_REFRESH_SEC = 600
TURNOVER_MIN_USD = float(os.getenv("TURNOVER_MIN_USD", "5000000"))
VOLUME_MIN_USD  = float(os.getenv("VOLUME_MIN_USD",  "5000000"))
ACTIVE_SYMBOLS  = int(os.getenv("ACTIVE_SYMBOLS",     "60"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "TONUSDT"]

# Таймфреймы
EXEC_TF_MAIN = "15"   # 15m close — вход
EXEC_TF_AUX  = "5"    # 5m — уточнение/буфер
CONTEXT_TF   = "60"   # 1h — буфер

# Индикаторные периоды (ENV)
ATR_PERIOD_15   = int(os.getenv("ATR_PERIOD_15",   "14"))
VOL_SMA_15      = int(os.getenv("VOL_SMA_15",      "20"))
EMA_PERIOD_1H   = int(os.getenv("EMA_PERIOD_1H",   "100"))   # в коде пока не используем, оставлен на будущее
VWAP_WINDOW_15  = int(os.getenv("VWAP_WINDOW_15",  "60"))    # 60×15m ≈ 15h

# Пороги/режимы (ENV)
IMPULSE_BODY_ATR     = float(os.getenv("IMPULSE_BODY_ATR",     "0.6"))
VOLUME_SPIKE_MULT    = float(os.getenv("VOLUME_SPIKE_MULT",    "2.0"))
CH_LEN               = int(os.getenv("CH_LEN",                 "12"))
OI_WINDOW_MIN        = int(os.getenv("OI_WINDOW_MIN",          "15"))
OI_DELTA_LONG_MAX    = float(os.getenv("OI_DELTA_LONG_MAX",    "-0.02"))  # ≤ -2% за 15м (deleverage)
OI_DELTA_SHORT_MIN   = float(os.getenv("OI_DELTA_SHORT_MIN",   "0.02"))   # ≥ +2% за 15м (build-up)
LIQ_SPIKE_MINUTES    = int(os.getenv("LIQ_SPIKE_MINUTES",      "15"))
LIQ_SPIKE_WINDOW_MIN = int(os.getenv("LIQ_SPIKE_WINDOW_MIN",   "120"))
LIQ_SPIKE_QUANTILE   = float(os.getenv("LIQ_SPIKE_QUANTILE",   "0.95"))

# Trend mode (ENV): 1 — включён, допускаем сигналы по BOS+Импульс+VWAP без OI/Liq/Sweep
MODE_TREND = int(os.getenv("MODE_TREND", "1"))

# Funding «крайние» зоны — информативный «встречный ветер»
FUNDING_EXTREME_POS = 0.0005      # +0.05%
FUNDING_EXTREME_NEG = -0.0005     # -0.05%

# TP/SL и риск (ENV)
ATR_SL_MULT  = float(os.getenv("ATR_SL_MULT",  "0.8"))
ATR_TP_MULT  = float(os.getenv("ATR_TP_MULT",  "1.2"))
TP_MIN_PCT   = float(os.getenv("TP_MIN_PCT",   "0.003"))   # 0.3%
TP_MAX_PCT   = float(os.getenv("TP_MAX_PCT",   "0.015"))   # 1.5%
RR_MIN       = float(os.getenv("RR_MIN",       "1.5"))

# Антиспам/надёжность (ENV)
SIGNAL_COOLDOWN_SEC = int(os.getenv("SIGNAL_COOLDOWN_SEC", "90"))
KEEPALIVE_SEC = 13 * 60
WATCHDOG_SEC = 60
STALL_EXIT_SEC = int(os.getenv("STALL_EXIT_SEC", "240"))

# =========================
# Утилиты/логгер
# =========================
def now_ms() -> int: return int(time.time() * 1000)
def pct(x: float) -> str: return f"{x:.2%}"

def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, force=True)

logger = logging.getLogger("cryptobot")

# =========================
# Telegram
# =========================
class Tg:
    def __init__(self, token: str, http: aiohttp.ClientSession) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = http

    async def send(self, chat_id: int, text: str) -> None:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        async with self.http.post(f"{self.base}/sendMessage", json=payload) as r:
            r.raise_for_status()
            await r.json()

    async def updates(self, offset: Optional[int], timeout: int = 25) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"timeout": timeout, "allowed_updates": ["message", "channel_post", "my_chat_member"]}
        if offset is not None: payload["offset"] = offset
        async with self.http.post(f"{self.base}/getUpdates", json=payload, timeout=aiohttp.ClientTimeout(total=timeout+10)) as r:
            r.raise_for_status()
            return await r.json()

# =========================
# Bybit REST
# =========================
class BybitRest:
    def __init__(self, base: str, http: aiohttp.ClientSession) -> None:
        self.base = base.rstrip("/")
        self.http = http

    async def tickers_linear(self) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/tickers?category=linear"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            return (await r.json()).get("result", {}).get("list", []) or []

    async def instruments_info(self, category: str, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        q = f"category={category}" + (f"&symbol={symbol}" if symbol else "")
        url = f"{self.base}/v5/market/instruments-info?{q}"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            return (await r.json()).get("result", {}).get("list", []) or []

    async def klines(self, category: str, symbol: str, interval: str, limit: int = 200) -> List[Tuple[float,float,float,float,float]]:
        url = f"{self.base}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={min(200, max(1, limit))}"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
        arr = (data.get("result") or {}).get("list") or []
        out: List[Tuple[float,float,float,float,float]] = []
        for it in arr:
            try:
                o,h,l,c,v = float(it[1]), float(it[2]), float(it[3]), float(it[4]), float(it[5])
                out.append((o,h,l,c,v))
            except Exception:
                continue
        return out[-200:]

# =========================
# Bybit WS
# =========================
class BybitWS:
    def __init__(self, url: str, http: aiohttp.ClientSession) -> None:
        self.url = url; self.http = http
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._subs: set[str] = set()
        self.on_message = None

    async def connect(self) -> None:
        if self.ws and not self.ws.closed: return
        logger.info(f"WS connecting: {self.url}")
        self.ws = await self.http.ws_connect(self.url, heartbeat=25)
        if self._subs:
            await self.ws.send_json({"op":"subscribe","args":list(self._subs)})

    async def subscribe(self, args: List[str]) -> None:
        for a in args: self._subs.add(a)
        if not self.ws or self.ws.closed: await self.connect()
        if args:
            await self.ws.send_json({"op":"subscribe","args":args})
            logger.info(f"WS subscribed: {args}")

    async def unsubscribe(self, args: List[str]) -> None:
        for a in args: self._subs.discard(a)
        if not self.ws or self.ws.closed: return
        if args:
            await self.ws.send_json({"op":"unsubscribe","args":args})
            logger.info(f"WS unsubscribed: {args}")

    async def run(self) -> None:
        assert self.ws is not None
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue
                if data.get("op") in {"subscribe","unsubscribe","ping","pong"} or "success" in data:
                    continue
                if self.on_message:
                    try:
                        await self.on_message(data)
                    except Exception:
                        logger.exception("on_message error")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
        await asyncio.sleep(2)
        with contextlib.suppress(Exception):
            await self.connect()
        if self.ws and not self.ws.closed:
            await self.run()

# =========================
# Индикаторы
# =========================
def atr(rows: List[Tuple[float,float,float,float,float]], period: int) -> float:
    if len(rows) < period + 1: return 0.0
    s = 0.0
    for i in range(1, period+1):
        _,h,l,c,_ = rows[-i]
        _,_,_,pc,_ = rows[-i-1]
        tr = max(h-l, abs(h-pc), abs(pc-l))
        s += tr
    return s/period

def sma(vals: List[float], period: int) -> float:
    if len(vals) < period or period <= 0: return 0.0
    return sum(vals[-period:]) / period

def ema(values: List[float], period: int) -> float:
    if not values or period <= 1 or len(values) < period: return 0.0
    k = 2.0 / (period + 1.0)
    ema_val = sum(values[:period]) / period
    for v in values[period:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val

def rolling_vwap(rows: List[Tuple[float,float,float,float,float]], window: int) -> Tuple[float, float]:
    n = len(rows)
    if n < window + 10: return 0.0, 0.0
    tp = [(r[1]+r[2]+r[3])/3.0 for r in rows]
    v  = [r[4] for r in rows]
    vw_now  = sum(tp[-window+i] * v[-window+i] for i in range(window)) / max(1e-9, sum(v[-window+i] for i in range(window)))
    vw_prev = sum(tp[-window-10+i] * v[-window-10+i] for i in range(window)) / max(1e-9, sum(v[-window-10+i] for i in range(window)))
    return vw_now, (vw_now - vw_prev)

def rsi14(rows: List[Tuple[float, float, float, float, float]]) -> float:
    """RSI(14) по закрытиям, расчёт по Wilder."""
    period = 14
    closes = [r[3] for r in rows]
    if len(closes) <= period:
        return 0.0
    gains = [max(0.0, closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    losses = [max(0.0, closes[i - 1] - closes[i]) for i in range(1, len(closes))]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

# =========================
# Рыночные данные / состояние
# =========================
@dataclass
class SymbolState:
    k15: List[Tuple[float,float,float,float,float]] = field(default_factory=list)
    k5:  List[Tuple[float,float,float,float,float]] = field(default_factory=list)
    k60: List[Tuple[float,float,float,float,float]] = field(default_factory=list)

    funding_rate: float = 0.0
    next_funding_ms: Optional[int] = None

    oi_points: deque = field(default_factory=lambda: deque(maxlen=120))     # (ts_ms, oi_float)
    liq_events: deque = field(default_factory=lambda: deque(maxlen=10000))  # (ts_ms, price, side, notional)

    last_signal_ts: int = 0
    cooldown_ts: Dict[str, int] = field(default_factory=dict)  # key=(side)

class Market:
    def __init__(self):
        self.symbols: List[str] = []
        self.state: Dict[str, SymbolState] = defaultdict(SymbolState)
        self.last_ws_msg_ts: int = now_ms()
        self.last_signal_sent_ts: int = 0

# =========================
# Heatmap / Liquidations helpers
# =========================
def price_bin(price: float, bin_bps: int) -> float:
    step = max(1e-6, price * (bin_bps / 10000.0))
    bins = round(price / step)
    return bins * step

def heatmap_top_clusters(st: SymbolState, last_price: float) -> Tuple[List[Tuple[float,float]], List[Tuple[float,float]]]:
    cutoff = now_ms() - LIQ_SPIKE_WINDOW_MIN * 60_000
    by_bin_buy: Dict[float, float] = defaultdict(float)
    by_bin_sell: Dict[float, float] = defaultdict(float)
    for ts, p, side, notional in st.liq_events:
        if ts < cutoff: continue
        b = price_bin(p, 25)
        if side == "Buy":  by_bin_buy[b]  += notional
        if side == "Sell": by_bin_sell[b] += notional
    ups  = sorted([(b, v) for b,v in by_bin_buy.items()  if b > last_price], key=lambda x: abs(x[0]-last_price))[:3]
    dows = sorted([(b, v) for b,v in by_bin_sell.items() if b < last_price], key=lambda x: abs(x[0]-last_price))[:3]
    return ups, dows

def _percentile(sorted_vals: List[float], q: float) -> float:
    if not sorted_vals: return 0.0
    if q <= 0: return sorted_vals[0]
    if q >= 1: return sorted_vals[-1]
    idx = int(q * (len(sorted_vals)-1))
    return sorted_vals[idx]

def liq_spike(st: SymbolState,
              minutes: int = LIQ_SPIKE_MINUTES,
              window_min: int = LIQ_SPIKE_WINDOW_MIN,
              q: float = LIQ_SPIKE_QUANTILE) -> Tuple[bool, float, float]:
    """Поминутные суммы ликвидаций за окно; проверяем, что последние <minutes> >= квантиля q."""
    now = now_ms()
    start = now - window_min * 60_000
    bucket: Dict[int, float] = defaultdict(float)
    recent_sum = 0.0
    recent_from = now - minutes * 60_000
    for ts, _, _, notional in st.liq_events:
        if ts < start: continue
        minute = ts // 60_000
        bucket[minute] += float(notional)
        if ts >= recent_from:
            recent_sum += float(notional)
    if not bucket:
        return (False, 0.0, 0.0)
    history = [v for m, v in bucket.items() if (m*60_000) < recent_from]
    history.sort()
    threshold = _percentile(history, q) if history else 0.0
    return (recent_sum >= threshold and recent_sum > 0.0), recent_sum, threshold

# =========================
# Сигнальный движок (15m)
# =========================
class Engine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def _oi_delta(self, st: SymbolState, minutes: int) -> float:
        if len(st.oi_points) < 2: return 0.0
        target = now_ms() - minutes*60*1000
        prev = None; last = st.oi_points[-1]
        for i in range(len(st.oi_points)-1, -1, -1):
            ts, v = st.oi_points[i]
            if ts <= target:
                prev = (ts, v); break
        if not prev: prev = st.oi_points[0]
        oi0 = float(prev[1]); oi1 = float(last[1])
        if oi0 <= 0: return 0.0
        return (oi1 - oi0) / oi0

    def _bos_flags(self, rows: List[Tuple[float,float,float,float,float]], lookback: int) -> Tuple[bool, bool]:
        if len(rows) < lookback + 2: return False, False
        highs = [r[1] for r in rows]
        lows  = [r[2] for r in rows]
        c = rows[-1][3]
        bos_up = c > max(highs[-(lookback+1):-1])
        bos_dn = c < min(lows[-(lookback+1):-1])
        return bos_up, bos_dn

    def _impulse_flags(self, rows: List[Tuple[float,float,float,float,float]]) -> Tuple[bool, bool, float, float, float]:
        atr15 = atr(rows, ATR_PERIOD_15)
        o,h,l,c,v = rows[-1]
        body = abs(c - o)
        vol_sma = sma([r[4] for r in rows], VOL_SMA_15)
        imp_up = (c > o) and (atr15 > 0) and (body >= IMPULSE_BODY_ATR * atr15) and (v >= VOLUME_SPIKE_MULT * max(1e-9, vol_sma))
        imp_dn = (c < o) and (atr15 > 0) and (body >= IMPULSE_BODY_ATR * atr15) and (v >= VOLUME_SPIKE_MULT * max(1e-9, vol_sma))
        return imp_up, imp_dn, atr15, body, vol_sma

    def on_15m_close(self, sym: str) -> Optional[Dict[str, Any]]:
        st = self.mkt.state[sym]
        K15 = st.k15
        if len(K15) < max(20, VOL_SMA_15 + 2, ATR_PERIOD_15 + 2):
            return None

        # Кулдаун по символу/стороне
        now_s = int(time.time())
        def cooled(side: str) -> bool:
            last = st.cooldown_ts.get(side, 0)
            return (now_s - last) >= SIGNAL_COOLDOWN_SEC

        # Индикаторы/контекст
        c_prev = K15[-2][3]
        o,h,l,c,v = K15[-1]
        if c_prev <= 0: return None

        imp_up, imp_dn, atr15, body, vol_sma15 = self._impulse_flags(K15)
        bos_up, bos_dn = self._bos_flags(K15, CH_LEN)
        rsi_val = rsi14(K15)

        vwap_now, vwap_slope = rolling_vwap(K15, VWAP_WINDOW_15)
        above_vwap = c >= vwap_now if vwap_now > 0 else True
        below_vwap = c <= vwap_now if vwap_now > 0 else True

        oi15 = self._oi_delta(st, OI_WINDOW_MIN)
        liq_ok, liq_recent, liq_thr = liq_spike(st)

        # «смыв/вынос»: прокол экстремума предыдущих 5 свечей
        sweep_down = l <= min(r[2] for r in K15[-6:-1])
        sweep_up   = h >= max(r[1] for r in K15[-6:-1])

        # Funding headwind (для информации)
        funding = st.funding_rate
        headwind_long  = funding >= FUNDING_EXTREME_POS
        headwind_short = funding <= FUNDING_EXTREME_NEG

        # ---- LONG SETUP ----
        if cooled("LONG") and imp_up and bos_up and above_vwap and vwap_slope > 0:
            cond_oi    = (oi15 <= OI_DELTA_LONG_MAX)
            cond_sweep = sweep_down
            cond_liq   = liq_ok
            trend_gate = bool(MODE_TREND == 1)
            if cond_oi or cond_sweep or cond_liq or trend_gate:
                sl = max(1e-9, c - ATR_SL_MULT * atr15)
                tp = c + ATR_TP_MULT * atr15
                rr = (tp - c) / max(1e-9, (c - sl))
                tp_pct = (tp - c) / c
                if tp_pct < TP_MIN_PCT: tp = c * (1 + TP_MIN_PCT)
                if tp_pct > TP_MAX_PCT: tp = c * (1 + TP_MAX_PCT)
                rr = (tp - c) / max(1e-9, (c - sl))
                if rr >= RR_MIN:
                    reasons: List[str] = [
                        f"Импульс UP: body={body:.4g} ≥ {IMPULSE_BODY_ATR}×ATR({atr15:.4g}), vol≥{VOLUME_SPIKE_MULT}×SMA20({vol_sma15:.4g})",
                        f"BOS↑({CH_LEN}) и VWAP↑: c≥VWAP({vwap_now:.4g}), slope>0",
                        f"RSI(14)={rsi_val:.2f}" + (" (отскок)" if rsi_val<40 else ""),
                        f"OIΔ(15m)={oi15:+.2%} {'(deleverage)' if cond_oi else ''}",
                        f"Liq spike: {liq_recent:,.0f} vs q{int(LIQ_SPIKE_QUANTILE*100)}={liq_thr:,.0f}" if cond_liq else "Liq spike: нет",
                        f"Sweep↓={cond_sweep}"
                    ]
                    if trend_gate and not (cond_oi or cond_liq or cond_sweep):
                        reasons.append("Mode: TREND (без OI/Liq/Sweep)")
                    if headwind_long:
                        reasons.append(f"⚠️ Funding {funding:+.4%} — встречный ветер для LONG")
                    st.cooldown_ts["LONG"] = now_s
                    return {
                        "symbol": sym, "side": "LONG", "entry": float(c),
                        "tp1": float(tp), "tp2": None, "sl": float(sl), "rr": float(rr),
                        "funding": funding, "oi15": float(oi15),
                        "vwap_bias": "UP", "ema100_1h": c,
                        "heat_up": [], "heat_dn": [],
                        "reason": reasons,
                        "next_funding_ms": st.next_funding_ms,
                        "rsi14": round(rsi_val, 2),
                    }

        # ---- SHORT SETUP ----
        if cooled("SHORT") and imp_dn and bos_dn and below_vwap and vwap_slope < 0:
            cond_oi    = (oi15 >= OI_DELTA_SHORT_MIN)
            cond_sweep = sweep_up
            cond_liq   = liq_ok
            trend_gate = bool(MODE_TREND == 1)
            if cond_oi or cond_sweep or cond_liq or trend_gate:
                sl = c + ATR_SL_MULT * atr15
                tp = c - ATR_TP_MULT * atr15
                rr = (c - tp) / max(1e-9, (sl - c))
                tp_pct = (c - tp) / c
                if tp_pct < TP_MIN_PCT: tp = c * (1 - TP_MIN_PCT)
                if tp_pct > TP_MAX_PCT: tp = c * (1 - TP_MAX_PCT)
                rr = (c - tp) / max(1e-9, (sl - c))
                if rr >= RR_MIN:
                    reasons: List[str] = [
                        f"Импульс DOWN: body={body:.4g} ≥ {IMPULSE_BODY_ATR}×ATR({atr15:.4g}), vol≥{VOLUME_SPIKE_MULT}×SMA20({vol_sma15:.4g})",
                        f"BOS↓({CH_LEN}) и VWAP↓: c≤VWAP({vwap_now:.4g}), slope<0",
                        f"RSI(14)={rsi_val:.2f}" + (" (перекуплен)" if rsi_val>60 else ""),
                        f"OIΔ(15m)={oi15:+.2%} {'(build-up)' if cond_oi else ''}",
                        f"Liq spike: {liq_recent:,.0f} vs q{int(LIQ_SPIKE_QUANTILE*100)}={liq_thr:,.0f}" if cond_liq else "Liq spike: нет",
                        f"Sweep↑={cond_sweep}"
                    ]
                    if trend_gate and not (cond_oi or cond_liq or cond_sweep):
                        reasons.append("Mode: TREND (без OI/Liq/Sweep)")
                    if headwind_short:
                        reasons.append(f"⚠️ Funding {funding:+.4%} — встречный ветер для SHORT")
                    st.cooldown_ts["SHORT"] = now_s
                    return {
                        "symbol": sym, "side": "SHORT", "entry": float(c),
                        "tp1": float(tp), "tp2": None, "sl": float(sl), "rr": float(rr),
                        "funding": funding, "oi15": float(oi15),
                        "vwap_bias": "DOWN", "ema100_1h": c,
                        "heat_up": [], "heat_dn": [],
                        "reason": reasons,
                        "next_funding_ms": st.next_funding_ms,
                        "rsi14": round(rsi_val, 2),
                    }

        return None

# =========================
# Формат сообщения
# =========================
def fmt_signal(sig: Dict[str, Any]) -> str:
    sym = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; tp1 = sig["tp1"]; sl = sig["sl"]; rr = sig["rr"]
    tp1_pct = (tp1 - entry)/entry if side=="LONG" else (entry - tp1)/entry
    tp2 = sig.get("tp2")
    fr = sig.get("funding", 0.0)
    ups = sig.get("heat_up") or []; dns = sig.get("heat_dn") or []
    next_f = sig.get("next_funding_ms")
    rsi_val = sig.get("rsi14")
    oi15 = sig.get("oi15", 0.0)

    nf = ""
    if next_f:
        mins = max(0, int((next_f - now_ms())/60000)); nf = f" (через ~{mins} мин)" if mins else " (скоро)"
    heat_line = "Heatmap: "
    heat_line += ("вверху≈" + ", ".join(f"{p:g}" for p,_ in ups[:2]) if side=="LONG" else
                  "внизу≈" + ", ".join(f"{p:g}" for p,_ in dns[:2]))

    reasons = "".join(f"\n- {r}" for r in (sig.get("reason") or []))
    lines = [
        f"🎯 <b>DERIVATIVES | {side} SIGNAL</b> on <b>[{sym}]</b> (15m)",
        "<b>Параметры:</b>",
        f"- <b>Funding:</b> {fr:+.4%}{nf}",
        f"- <b>OIΔ(15m):</b> {oi15:+.2%}",
        f"- <b>RSI(14):</b> {rsi_val:.2f}" if rsi_val is not None else None,
        f"- {heat_line}",
        f"<b>Вход:</b> {entry:g}",
        f"<b>Стоп-Лосс:</b> {sl:g}",
        f"<b>Тейк-Профит 1:</b> {tp1:g} ({pct(tp1_pct)})" + (f"\n<b>Тейк-Профит 2:</b> {tp2:g}" if tp2 else ""),
        "<b>Обоснование:</b>" + reasons if reasons else None,
        f"<b>Риск:</b> RR≈{rr:.2f} • плечо ≤ x10; риск ≤ 1% депозита.",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
    ]
    return "\n".join([x for x in lines if x])

# =========================
# TG команды
# =========================
async def tg_loop(app: web.Application) -> None:
    tg: Tg = app["tg"]; mkt: Market = app["mkt"]
    offset = None
    while True:
        try:
            resp = await tg.updates(offset=offset, timeout=25)
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("channel_post")
                if not msg: continue
                chat_id = msg.get("chat", {}).get("id")
                text = (msg.get("text") or "").strip()
                if not isinstance(chat_id, int) or not text.startswith("/"): continue
                if chat_id not in ALLOWED_CHAT_IDS and chat_id not in PRIMARY_RECIPIENTS: continue
                cmd = text.split()[0].lower()

                if cmd == "/ping":
                    ago = (now_ms() - mkt.last_ws_msg_ts)/1000.0
                    await tg.send(chat_id, f"pong • WS last msg {ago:.1f}s ago • symbols={len(mkt.symbols)}")

                elif cmd == "/status":
                    silent_line = "—" if mkt.last_signal_sent_ts == 0 else f"{(now_ms()-mkt.last_signal_sent_ts)/60000.0:.1f}m"
                    await tg.send(chat_id,
                        "✅ Online\n"
                        f"Symbols: {len(mkt.symbols)}\n"
                        f"Mode: Derivatives (OI + Liquidations + Impulse + VWAP) • TREND={'ON' if MODE_TREND==1 else 'OFF'}\n"
                        f"TP≥{pct(TP_MIN_PCT)} • RR≥{RR_MIN:.2f}\n"
                        f"Silent (signals): {silent_line}")

                elif cmd == "/help":
                    await tg.send(chat_id,
                        "Команды:\n"
                        "/ping — пинг\n"
                        "/status — статус\n"
                        "/diag — диагностика буферов и метрик\n"
                        "/jobs — фоновые задачи\n"
                        "/metrics <SYMBOL> — Funding/OI/Heatmap")

                elif cmd == "/jobs":
                    jobs = []
                    for k in ("ws_task","keepalive_task","watchdog_task","tg_task","universe_task"):
                        t = app.get(k); jobs.append(f"{k}: {'running' if (t and not t.done()) else 'stopped'}")
                    await tg.send(chat_id, "Jobs:\n" + "\n".join(jobs))

                elif cmd == "/diag":
                    k15_pts = sum(len(mkt.state[s].k15) for s in mkt.symbols)
                    k5_pts  = sum(len(mkt.state[s].k5) for s in mkt.symbols)
                    k60_pts = sum(len(mkt.state[s].k60) for s in mkt.symbols)
                    ago = (now_ms() - mkt.last_ws_msg_ts)/1000.0
                    silent_line = "—" if mkt.last_signal_sent_ts == 0 else f"{(now_ms()-mkt.last_signal_sent_ts)/60000.0:.1f}m"
                    head = ", ".join(mkt.symbols[:10]) if mkt.symbols else "—"
                    await tg.send(chat_id,
                        "Diag:\n"
                        f"WS last msg: {ago:.1f}s ago\n"
                        f"Symbols: {len(mkt.symbols)} (head: {head})\n"
                        f"Kline buffers: 15m={k15_pts} • 5m={k5_pts} • 60m={k60_pts}\n"
                        f"Silent (signals): {silent_line}")

                elif cmd.startswith("/metrics"):
                    parts = text.split()
                    if len(parts) < 2:
                        await tg.send(chat_id, "Формат: /metrics SYMBOL\nПример: /metrics BTCUSDT")
                    else:
                        s = parts[1].upper()
                        st = mkt.state.get(s)
                        if not st or not st.k15:
                            await tg.send(chat_id, "Нет данных. Буферы ещё наполняются.")
                        else:
                            oi_15 = Engine(mkt)._oi_delta(st, OI_WINDOW_MIN)
                            ups, dns = heatmap_top_clusters(st, st.k15[-1][3])
                            hf = lambda arr: ", ".join(f"{p:g}" for p,_ in arr) if arr else "—"
                            next_f = ""
                            if st.next_funding_ms:
                                mins = max(0, int((st.next_funding_ms - now_ms())/60000))
                                next_f = f"через ~{mins} мин"
                            await tg.send(chat_id,
                                f"📈 <b>{s}</b>\nFunding: {st.funding_rate:+.4%} {('('+next_f+')' if next_f else '')}\n"
                                f"OIΔ(15m): {oi_15:+.2%}\n"
                                f"Heatmap up: {hf(ups)}\nHeatmap down: {hf(dns)}")

                else:
                    await tg.send(chat_id, "Неизвестная команда. /help")

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("tg_loop error")
            await asyncio.sleep(2)

# =========================
# WS handler
# =========================
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    mkt: Market = app["mkt"]; tg: Tg = app["tg"]; eng: Engine = app["engine"]

    topic = data.get("topic") or ""
    mkt.last_ws_msg_ts = now_ms()

    # TICKER (fundingRate, nextFundingTime, openInterest)
    if topic.startswith("tickers."):
        d = data.get("data") or {}
        sym = d.get("symbol")
        if not sym: return
        st = mkt.state[sym]
        with contextlib.suppress(Exception):
            st.funding_rate = float(d.get("fundingRate") or 0.0)
        with contextlib.suppress(Exception):
            st.next_funding_ms = int(d.get("nextFundingTime")) if d.get("nextFundingTime") else None
        with contextlib.suppress(Exception):
            oi = float(d.get("openInterest") or 0.0)
            st.oi_points.append((now_ms(), oi))

    # KLINE 15m
    elif topic.startswith(f"kline.{EXEC_TF_MAIN}."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            st = mkt.state[sym]
            for p in payload:
                o,h,l,c,v = float(p["open"]), float(p["high"]), float(p["low"]), float(p["close"]), float(p.get("volume") or 0.0)
                if p.get("confirm") is False and st.k15:
                    st.k15[-1] = (o,h,l,c,v)
                else:
                    st.k15.append((o,h,l,c,v))
                    if len(st.k15) > 900: st.k15 = st.k15[-900:]
                # На закрытии 15m — генерим сигнал
                if p.get("confirm") is True:
                    sig = eng.on_15m_close(sym)
                    if sig:
                        text = fmt_signal(sig)
                        for chat_id in (PRIMARY_RECIPIENTS if ONLY_CHANNEL else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS)):
                            with contextlib.suppress(Exception):
                                await tg.send(chat_id, text)
                        mkt.last_signal_sent_ts = now_ms()
                        mkt.state[sym].last_signal_ts = now_ms()
            try:
                if len(st.k15) % 50 == 0:
                    logger.info(f"[DATA] {sym} 15m buffer: {len(st.k15)} candles")
            except Exception:
                pass

    # KLINE 5m
    elif topic.startswith(f"kline.{EXEC_TF_AUX}."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            st = mkt.state[sym]
            for p in payload:
                o,h,l,c,v = float(p["open"]), float(p["high"]), float(p["low"]), float(p["close"]), float(p.get("volume") or 0.0)
                if p.get("confirm") is False and st.k5:
                    st.k5[-1] = (o,h,l,c,v)
                else:
                    st.k5.append((o,h,l,c,v))
                    if len(st.k5) > 900: st.k5 = st.k5[-900:]

    # KLINE 60m
    elif topic.startswith("kline.60."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            st = mkt.state[sym]
            for p in payload:
                o,h,l,c,v = float(p["open"]), float(p["high"]), float(p["low"]), float(p["close"]), float(p.get("volume") or 0.0)
                if p.get("confirm") is False and st.k60:
                    st.k60[-1] = (o,h,l,c,v)
                else:
                    st.k60.append((o,h,l,c,v))
                    if len(st.k60) > 900: st.k60 = st.k60[-900:]

    # All Liquidations
    elif topic.startswith("allLiquidation."):
        d = data.get("data") or []
        for it in d:
            try:
                sym = it.get("symbol") or topic.split(".")[-1]
                st = mkt.state[sym]
                p = float(it.get("price") or 0.0)
                side = (it.get("side") or "").strip()
                qty = float(it.get("qty") or it.get("size") or 0.0)
                ts  = int(it.get("timestamp") or now_ms())
                if p>0 and side in ("Buy","Sell") and qty>0:
                    st.liq_events.append((ts, p, side, p*qty))
            except Exception:
                continue

# =========================
# Фоновые задачи
# =========================
async def keepalive_loop(app: web.Application) -> None:
    public_url = os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL")
    http: aiohttp.ClientSession = app["http"]
    if not public_url: return
    while True:
        try:
            await asyncio.sleep(KEEPALIVE_SEC)
            with contextlib.suppress(Exception):
                await http.get(public_url, timeout=aiohttp.ClientTimeout(total=10))
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("keepalive error")

async def watchdog_loop(app: web.Application) -> None:
    mkt: Market = app["mkt"]
    while True:
        try:
            await asyncio.sleep(WATCHDOG_SEC)
            ago = (now_ms() - mkt.last_ws_msg_ts) / 1000.0
            logger.info(f"[watchdog] alive; last WS msg {ago:.1f}s ago; symbols={len(mkt.symbols)}")
            if ago >= STALL_EXIT_SEC:
                logger.error(f"[watchdog] WS stalled {ago:.1f}s >= {STALL_EXIT_SEC}. Exit for restart.")
                os._exit(3)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("watchdog error")

# -------- Вселенная символов
async def build_universe_once(rest: BybitRest) -> List[str]:
    """Строим вселенную c фолбэком на CORE_SYMBOLS. Spot-верификация — неблокирующая."""
    symbols: List[str] = []
    try:
        tickers = await rest.tickers_linear()
        pool: List[str] = []
        for t in tickers:
            sym = t.get("symbol") or ""
            if not sym.endswith("USDT"):
                continue
            try:
                turn = float(t.get("turnover24h") or 0.0)
                vol  = float(t.get("volume24h") or 0.0)
            except Exception:
                continue
            if turn >= TURNOVER_MIN_USD or vol >= VOLUME_MIN_USD:
                pool.append(sym)

        verified: List[str] = []
        try:
            for s in pool:
                with contextlib.suppress(Exception):
                    spot = await rest.instruments_info("spot", s)
                    if spot: verified.append(s)
        except Exception:
            verified = pool[:]

        symbols = CORE_SYMBOLS + [x for x in verified if x not in CORE_SYMBOLS]
        symbols = symbols[:ACTIVE_SYMBOLS]
    except Exception:
        logger.exception("build_universe_once error")
        symbols = CORE_SYMBOLS[:ACTIVE_SYMBOLS]

    if not symbols:
        symbols = CORE_SYMBOLS[:ACTIVE_SYMBOLS]
    return symbols

async def universe_refresh_loop(app: web.Application) -> None:
    rest: BybitRest = app["rest"]; ws: BybitWS = app["ws"]; mkt: Market = app["mkt"]
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)
            symbols_new = await build_universe_once(rest)
            symbols_old = set(mkt.symbols)
            add = [s for s in symbols_new if s not in symbols_old]
            rem = [s for s in mkt.symbols if s not in set(symbols_new)]
            if add or rem:
                if rem:
                    args = []
                    for s in rem:
                        args += [f"tickers.{s}", f"kline.{EXEC_TF_MAIN}.{s}", f"kline.{EXEC_TF_AUX}.{s}", f"kline.60.{s}", f"allLiquidation.{s}"]
                    await ws.unsubscribe(args)
                if add:
                    args = []
                    for s in add:
                        args += [f"tickers.{s}", f"kline.{EXEC_TF_MAIN}.{s}", f"kline.{EXEC_TF_AUX}.{s}", f"kline.60.{s}", f"allLiquidation.{s}"]
                    await ws.subscribe(args)
                    logger.info(f"[WS] Subscribed to {len(args)} topics for {len(add)} symbols")
                mkt.symbols = symbols_new
                logger.info(f"[universe] +{len(add)} / -{len(rem)} • total={len(mkt.symbols)}")
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("universe_refresh_loop error")

# =========================
# Web app
# =========================
async def handle_health(request: web.Request) -> web.Response:
    app = request.app; mkt: Market = app["mkt"]
    return web.json_response({
        "ok": True,
        "symbols": mkt.symbols,
        "last_ws_msg_age_sec": int((now_ms() - mkt.last_ws_msg_ts)/1000),
    })

async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    if not TELEGRAM_TOKEN: raise RuntimeError("Не задан TELEGRAM_TOKEN")

    http = aiohttp.ClientSession()
    app["http"] = http
    app["tg"] = Tg(TELEGRAM_TOKEN, http)
    app["rest"] = BybitRest(BYBIT_REST, http)
    app["mkt"] = Market()
    app["engine"] = Engine(app["mkt"])
    app["ws"] = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: ws_on_message(app, data)

    # Вселенная
    symbols = await build_universe_once(app["rest"])
    app["mkt"].symbols = symbols
    logger.info(f"symbols: {symbols}")

    # Подписки
    await app["ws"].connect()
    args = []
    for s in symbols:
        args += [f"tickers.{s}", f"kline.{EXEC_TF_MAIN}.{s}", f"kline.{EXEC_TF_AUX}.{s}", f"kline.60.{s}", f"allLiquidation.{s}"]
    if args:
        await app["ws"].subscribe(args)
        logger.info(f"[WS] Initial subscribed to {len(args)} topics for {len(symbols)} symbols")
    else:
        fallback = CORE_SYMBOLS[:]
        app["mkt"].symbols = fallback
        fargs = []
        for s in fallback:
            fargs += [f"tickers.{s}", f"kline.{EXEC_TF_MAIN}.{s}", f"kline.{EXEC_TF_AUX}.{s}", f"kline.60.{s}", f"allLiquidation.{s}"]
        await app["ws"].subscribe(fargs)
        logger.info(f"[WS] Fallback subscribed to {len(fargs)} topics for {len(fallback)} symbols")

    # таски
    app["ws_task"] = asyncio.create_task(app["ws"].run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_task"] = asyncio.create_task(tg_loop(app))
    app["universe_task"] = asyncio.create_task(universe_refresh_loop(app))

    # привет
    try:
        for chat_id in PRIMARY_RECIPIENTS or ALLOWED_CHAT_IDS:
            await app["tg"].send(chat_id, f"🟢 Cryptobot v6.1: SMC-lite + OI + Liq + Impulse + VWAP + ATR targets • TREND={'ON' if MODE_TREND==1 else 'OFF'} • RR≥{RR_MIN}")
    except Exception:
        logger.warning("startup notify failed")

async def on_cleanup(app: web.Application) -> None:
    for k in ("ws_task","keepalive_task","watchdog_task","tg_task","universe_task"):
        t = app.get(k)
        if t:
            t.cancel()
            with contextlib.suppress(Exception): await t
    if app.get("ws") and app["ws"].ws and not app["ws"].closed:
        await app["ws"].ws.close()
    if app.get("http"):
        await app["http"].close()

def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

def main() -> None:
    setup_logging(LOG_LEVEL)
    logger.info("Starting Cryptobot v6.1 — TF=15m/5m, Context=1H, OI+Liq+Impulse+VWAP, RR via ATR, TREND mode")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
