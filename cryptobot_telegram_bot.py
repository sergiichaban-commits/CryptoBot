# cryptobot_telegram_bot.py
# Telegram webhook bot on Render + signals engine (Bybit v5)
# Strategy: Intraday SMC-lite + Volume + OI + Liquidations
# Filters: RR>=2.0, Profit>=2%, Probability>69.9; only fresh signals; sorted by probability
#
# ENV (Render -> Environment):
#   TELEGRAM_BOT_TOKEN   (required)
#   TELEGRAM_CHAT_ID     (required) channel/chat id to receive alerts (e.g. -1002870952333)
#   ALLOWED_CHAT_IDS     (required) CSV including TELEGRAM_CHAT_ID and your user id
#   RENDER_EXTERNAL_URL  (Render sets) OR PUBLIC_URL
#   PORT                 (Render sets)
#   (opt) WEBHOOK_PATH            default /wh-<token8>
#   (opt) HEALTH_INTERVAL_SEC     default 1200 (20 min)
#   (opt) HEALTH_FIRST_SEC        default 60
#   (opt) STARTUP_PING_SEC        default 10
#   (opt) SELF_PING_ENABLED       "1"/"0" default "1"
#   (opt) SELF_PING_INTERVAL_SEC  default 780 (~13 min)
#   (opt) SELF_PING_PATH          default "/"
#   (opt) SIGNAL_COOLDOWN_SEC     default 600
#   (opt) SIGNAL_TTL_MIN          default 12  (signal validity window, mins)
#   (opt) UNIVERSE_TOP_N          default 15
#   (opt) BYBIT_SYMBOLS_CANDIDATES CSV override candidate universe (else default list)
#   (opt) PROB_MIN                default 69.9  (strictly greater-than)
#
# Requirements:
#   python-telegram-bot[job-queue,webhooks]==21.6
#   aiohttp websockets pandas numpy pytz httpx

from __future__ import annotations

import os, asyncio, json, math, time, logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytz
import aiohttp
import websockets
import httpx

from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes

# ------------ logging ------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s")
log = logging.getLogger("cryptobot")

# ------------ helpers/env ------------
def parse_int_list(csv: str) -> List[int]:
    out = []
    for part in (csv or "").split(","):
        s = part.strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except ValueError:
            pass
    return out

def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def getenv_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def now_ms() -> int:
    return int(time.time() * 1000)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("TELEGRAM_BOT_TOKEN is required.")

primary_chat_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
ALLOWED_CHAT_IDS: Set[int] = set(parse_int_list(os.environ.get("ALLOWED_CHAT_IDS", "")))
PRIMARY_RECIPIENTS: List[int] = []
if primary_chat_raw:
    try:
        cid = int(primary_chat_raw)
        if cid in ALLOWED_CHAT_IDS:
            PRIMARY_RECIPIENTS.append(cid)
    except ValueError:
        pass

PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL") or ""
PORT = getenv_int("PORT", 10000)
token_prefix = BOT_TOKEN.split(":")[0] if ":" in BOT_TOKEN else BOT_TOKEN[:8]
WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH", f"/wh-{token_prefix[:8]}")
WEBHOOK_URL = (PUBLIC_URL.rstrip("/") + WEBHOOK_PATH) if PUBLIC_URL else ""

HEALTH_INTERVAL_SEC = getenv_int("HEALTH_INTERVAL_SEC", 1200)
HEALTH_FIRST_SEC    = getenv_int("HEALTH_FIRST_SEC", 60)
STARTUP_PING_SEC    = getenv_int("STARTUP_PING_SEC", 10)

SELF_PING_ENABLED        = os.environ.get("SELF_PING_ENABLED", "1").lower() not in {"0","false","no"}
SELF_PING_INTERVAL_SEC   = getenv_int("SELF_PING_INTERVAL_SEC", 780)
SELF_PING_URL            = (PUBLIC_URL.rstrip("/") + os.environ.get("SELF_PING_PATH", "/")) if PUBLIC_URL else ""

SIGNAL_COOLDOWN_SEC      = getenv_int("SIGNAL_COOLDOWN_SEC", 600)
SIGNAL_TTL_MIN           = getenv_int("SIGNAL_TTL_MIN", 12)
UNIVERSE_TOP_N           = getenv_int("UNIVERSE_TOP_N", 15)
PROB_MIN                 = getenv_float("PROB_MIN", 69.9)  # strict >

log.info("[cfg] ALLOWED_CHAT_IDS=%s", sorted(ALLOWED_CHAT_IDS))
log.info("[cfg] PRIMARY_RECIPIENTS=%s", PRIMARY_RECIPIENTS)
log.info("[cfg] PUBLIC_URL='%s' PORT=%s WEBHOOK_PATH='%s'", PUBLIC_URL, PORT, WEBHOOK_PATH)
log.info("[cfg] HEALTH=%ss FIRST=%ss STARTUP=%s SELF_PING=%s/%ss", HEALTH_INTERVAL_SEC, HEALTH_FIRST_SEC, STARTUP_PING_SEC, bool(SELF_PING_ENABLED), SELF_PING_INTERVAL_SEC)
log.info("[cfg] SIGNAL_COOLDOWN_SEC=%s SIGNAL_TTL_MIN=%s UNIVERSE_TOP_N=%s PROB_MIN>%s", SIGNAL_COOLDOWN_SEC, SIGNAL_TTL_MIN, UNIVERSE_TOP_N, PROB_MIN)

# ------------ Bybit constants ------------
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
BYBIT_REST_BASE = "https://api.bybit.com"

# ------------ Data structures ------------
class Ring:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buf: List[Any] = []
    def append(self, x: Any):
        if len(self.buf) >= self.capacity:
            self.buf.pop(0)
        self.buf.append(x)
    def last(self, n: int) -> List[Any]:
        return self.buf[-n:]
    def __len__(self):
        return len(self.buf)

@dataclass
class Candle:
    t: int; o: float; h: float; l: float; c: float; v: float; turn: float; confirm: bool

@dataclass
class SymbolState:
    candles: Ring = field(default_factory=lambda: Ring(1200))    # ~20h of 1m
    vwap_num: float = 0.0
    vwap_den: float = 0.0
    vwap: float = float('nan')
    last_daily_reset: Optional[datetime] = None
    oi_series: Ring = field(default_factory=lambda: Ring(1200))  # (ts, oi)
    liq_5m_buckets: Ring = field(default_factory=lambda: Ring(7*24*12))
    current_liq_bucket_start: Optional[int] = None
    current_liq_bucket_notional: float = 0.0
    last_alert_ts: int = 0

# ------------ math/indicators ------------
def atr(series: List[Candle], period: int = 14) -> float:
    if len(series) < period + 1:
        return float('nan')
    trs = []
    for i in range(-period, 0):
        c0 = series[i-1]; c1 = series[i]
        tr = max(c1.h - c1.l, abs(c1.h - c0.c), abs(c1.l - c0.c))
        trs.append(tr)
    return float(sum(trs) / period)

def sma(vals: List[float], period: int) -> float:
    if len(vals) < period:
        return float('nan')
    return float(sum(vals[-period:]) / period)

def percentile(values: List[float], p: float) -> float:
    if not values:
        return float('nan')
    return float(np.percentile(np.array(values), p*100))

def detect_recent_fvg(candles: List[Candle]) -> Optional[str]:
    if len(candles) < 3:
        return None
    c0, c1, c2 = candles[-3], candles[-2], candles[-1]
    if c2.l > c0.h:
        return "bull"
    if c2.h < c0.l:
        return "bear"
    return None

def detect_sweep(candles: List[Candle], lookback: int) -> Optional[str]:
    if len(candles) < lookback + 2:
        return None
    last = candles[-1]
    lows = [c.l for c in candles[-(lookback+1):-1]]
    highs = [c.h for c in candles[-(lookback+1):-1]]
    took_low  = last.l < min(lows) and last.c > last.o  # down sweep + bull close
    took_high = last.h > max(highs) and last.c < last.o # up sweep + bear close
    if took_low:  return "down"
    if took_high: return "up"
    return None

def update_vwap(state: SymbolState, candle: Candle):
    typical = (candle.h + candle.l + candle.c) / 3.0
    state.vwap_num += typical * candle.v
    state.vwap_den += candle.v
    if state.vwap_den > 0:
        state.vwap = state.vwap_num / state.vwap_den

def maybe_reset_daily(state: SymbolState, candle_time_ms: int):
    t = datetime.fromtimestamp(candle_time_ms/1000, tz=timezone.utc)
    if state.last_daily_reset is None:
        state.last_daily_reset = t.replace(hour=0, minute=0, second=0, microsecond=0)
        return
    if t.date() != state.last_daily_reset.date():
        state.vwap_num = 0.0; state.vwap_den = 0.0; state.vwap = float('nan')
        state.last_daily_reset = t.replace(hour=0, minute=0, second=0, microsecond=0)

# ------------ Bybit client ------------
class BybitClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_open_interest(self, symbol: str, interval: str = "5min", limit: int = 2) -> List[Dict[str, Any]]:
        params = {"category":"linear","symbol":symbol,"interval":interval,"limit":str(limit)}
        url = f"{BYBIT_REST_BASE}/v5/market/open-interest"
        async with self.session.get(url, params=params, timeout=10) as r:
            r.raise_for_status()
            data = await r.json()
            if data.get("retCode") != 0:
                raise RuntimeError(f"Bybit OI error: {data}")
            return data["result"]["list"]

    async def get_kline(self, symbol: str, interval: str = "1", limit: int = 400) -> List[Candle]:
        params = {"category":"linear","symbol":symbol,"interval":interval,"limit":str(limit)}
        url = f"{BYBIT_REST_BASE}/v5/market/kline"
        async with self.session.get(url, params=params, timeout=10) as r:
            r.raise_for_status()
            data = await r.json()
            if data.get("retCode") != 0:
                raise RuntimeError(f"Bybit kline error: {data}")
            out: List[Candle] = []
            for row in data["result"]["list"]:
                t = int(row[0]); o,h,l,c = map(float, row[1:5]); v = float(row[5]); turn = float(row[6])
                out.append(Candle(t,o,h,l,c,v,turn,True))
            out.sort(key=lambda x: x.t)
            return out

# ------------ Signal model ------------
@dataclass
class Signal:
    symbol: str
    side: str  # LONG or SHORT
    price_now: float
    entry_from: float
    entry_to: float
    sl: float
    tp1: float
    tp2: float
    rr_tp1: float
    profit_pct_tp1: float
    probability: float
    reasons: List[str]
    ttl_min: int

# ------------ Engine ------------
class Engine:
    def __init__(self, bot: Bot, chat_ids: List[int], session: aiohttp.ClientSession):
        self.bot = bot
        self.chat_ids = chat_ids
        self.session = session
        self.client = BybitClient(session)
        self.state: Dict[str, SymbolState] = {}
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.symbols: List[str] = []

        # thresholds
        self.oi_delta_pct_5m = 2.0
        self.oi_delta_pct_15m = 3.0
        self.volume_sma_mult = 2.0
        self.body_atr_mult   = 0.6
        self.body_atr_strong = 0.8
        self.liq_percentile  = 0.95
        self.vwap_dev_pct    = 0.5
        self.btc_sync_div    = 0.4
        self.cooldown_ms     = SIGNAL_COOLDOWN_SEC * 1000

        self.sweep_lookback  = 30
        self.fvg_lookback    = 20

    async def discover_universe(self) -> List[str]:
        # pick high-volatility coins over last 7d (1h candles) from candidates
        default_candidates = [
            "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","TONUSDT","HBARUSDT",
            "ARBUSDT","OPUSDT","RNDRUSDT","AVAXUSDT","NEARUSDT","ATOMUSDT","APTUSDT","SUIUSDT","PEPEUSDT",
            "WIFUSDT","ENAUSDT","FETUSDT","JUPUSDT","TAOUSDT","PYTHUSDT","SEIUSDT"
        ]
        raw = os.getenv("BYBIT_SYMBOLS_CANDIDATES","").strip()
        candidates = [s.strip().upper() for s in raw.split(",") if s.strip()] or default_candidates
        log.info("[universe] candidates=%d", len(candidates))

        vols: List[Tuple[str,float]] = []
        for sym in candidates:
            try:
                # 1h data for ~7 days → 7*24=168, take 200
                kl = await self.client.get_kline(sym, interval="60", limit=200)
                if len(kl) < 50: 
                    continue
                closes = np.array([c.c for c in kl], dtype=float)
                rets = np.diff(np.log(closes))
                vol = float(np.std(rets[-168:]) * math.sqrt(24))  # dailyized approx
                vols.append((sym, vol))
            except Exception:
                continue
        vols.sort(key=lambda x: x[1], reverse=True)
        top = [s for s,_ in vols[:UNIVERSE_TOP_N]] or candidates[:UNIVERSE_TOP_N]
        log.info("[universe] selected=%s", top)
        return top

    async def bootstrap(self):
        self.symbols = await self.discover_universe()
        self.state = {s: SymbolState() for s in self.symbols}

        # seed candles & vwap
        for sym in self.symbols:
            kl = await self.client.get_kline(sym, interval="1", limit=400)
            st = self.state[sym]
            for c in kl:
                st.candles.append(c)
                maybe_reset_daily(st, c.t)
                update_vwap(st, c)
        # seed OI 5m series
        for sym in self.symbols:
            oi5 = await self.client.get_open_interest(sym, interval="5min", limit=4)
            st = self.state[sym]
            for row in reversed(oi5):
                ts = int(row["timestamp"]); oi = float(row["openInterest"])
                st.oi_series.append((ts, oi))

    async def ws_connect(self):
        self.ws = await websockets.connect(BYBIT_WS_PUBLIC_LINEAR, ping_interval=25, ping_timeout=20)
        args = []
        for sym in self.symbols:
            args.append(f"kline.1.{sym}")
            args.append(f"liquidation.{sym}")
        sub = {"op":"subscribe","args": args}
        await self.ws.send(json.dumps(sub))

    async def run_ws(self):
        assert self.ws is not None
        async for msg in self.ws:
            try:
                data = json.loads(msg)
            except Exception:
                continue
            if "topic" not in data:
                continue
            topic: str = data["topic"]
            if topic.startswith("kline."):
                await self.on_kline(data)
            elif topic.startswith("liquidation."):
                await self.on_liq(data)

    async def on_kline(self, data: Dict[str, Any]):
        topic = data["topic"]
        _, _, symbol = topic.split(".")
        st = self.state.get(symbol)
        if not st:
            return
        for item in data.get("data", []):
            c = Candle(
                t=int(item.get("start")),
                o=float(item.get("open")),
                h=float(item.get("high")),
                l=float(item.get("low")),
                c=float(item.get("close")),
                v=float(item.get("volume")),
                turn=float(item.get("turnover", 0) or 0.0),
                confirm=bool(item.get("confirm", False))
            )
            # update/append
            if st.candles and not st.candles.buf[-1].confirm:
                st.candles.buf[-1] = c
            else:
                st.candles.append(c)
            maybe_reset_daily(st, c.t)
            if c.confirm:
                update_vwap(st, c)
                await self.evaluate_symbol(symbol)

    async def on_liq(self, data: Dict[str, Any]):
        _, symbol = data["topic"].split(".")
        st = self.state.get(symbol)
        if not st: return
        now_bucket = int(datetime.now(timezone.utc).timestamp() // 300) * 300
        if st.current_liq_bucket_start is None:
            st.current_liq_bucket_start = now_bucket
        if now_bucket != st.current_liq_bucket_start:
            st.liq_5m_buckets.append(st.current_liq_bucket_notional)
            st.current_liq_bucket_notional = 0.0
            st.current_liq_bucket_start = now_bucket
        for it in data.get("data", []):
            try:
                qty = float(it.get("execQty", 0)); price = float(it.get("execPrice", 0))
                st.current_liq_bucket_notional += qty * price
            except Exception:
                continue

    async def poll_oi(self):
        while True:
            try:
                for sym in self.symbols:
                    oi5 = await self.client.get_open_interest(sym, interval="5min", limit=1)
                    if oi5:
                        row = oi5[0]
                        ts = int(row["timestamp"]); oi = float(row["openInterest"])
                        st = self.state[sym]
                        st.oi_series.append((ts, oi))
            except Exception:
                pass
            await asyncio.sleep(60)

    # ---- helpers ----
    def _oi_change_pct(self, st: SymbolState, minutes: int) -> Optional[float]:
        if len(st.oi_series) < 2:
            return None
        now_ts = st.oi_series.buf[-1][0]
        cutoff = now_ts - minutes*60*1000
        recent = [x for x in st.oi_series.buf if x[0] >= cutoff]
        if len(recent) < 2:
            return None
        first, last = recent[0][1], recent[-1][1]
        if first == 0: return None
        return (last - first) / first * 100.0

    def _volume_sma20(self, st: SymbolState) -> Optional[float]:
        vols = [c.v for c in st.candles.buf]
        return sma(vols, 20)

    def _atr14(self, st: SymbolState) -> Optional[float]:
        return atr(st.candles.buf, 14)

    def _liq_5m_percentile(self, st: SymbolState, p: float) -> Optional[float]:
        vals = list(st.liq_5m_buckets.buf)
        if len(vals) < 50:
            return None
        return percentile(vals, p)

    def _nearest_swings(self, st: SymbolState, window: int = 20) -> Tuple[Optional[float], Optional[float]]:
        if len(st.candles) < window:
            return (None, None)
        highs = [c.h for c in st.candles.last(window)]
        lows  = [c.l for c in st.candles.last(window)]
        return (max(highs), min(lows))

    # ---- probability model (simple heuristic 60..95%) ----
    def _probability(self, strong_impulse: bool, fvg: Optional[str], vwap_dev_ok: bool, oi_hint: Optional[str], side: str) -> float:
        p = 70.0  # base when all core triggers passed (impulse+ΔOI+liq)
        if strong_impulse: p += 6.0
        if fvg:            p += 5.0
        if vwap_dev_ok:    p += 4.0
        if (oi_hint == 'long' and side=='LONG') or (oi_hint=='short' and side=='SHORT'):
            p += 3.0
        return float(max(60.0, min(95.0, p)))

    # ---- evaluation ----
    async def evaluate_symbol(self, symbol: str):
        st = self.state[symbol]
        if len(st.candles) < 40:
            return
        last = st.candles.buf[-1]
        if not last.confirm:
            return

        body = abs(last.c - last.o)
        atr14 = self._atr14(st)
        vol_sma20 = self._volume_sma20(st)
        oi_5 = self._oi_change_pct(st, 5)
        oi_15 = self._oi_change_pct(st, 15)
        liq_p95 = self._liq_5m_percentile(st, self.liq_percentile)
        liq_now = st.current_liq_bucket_notional

        if any(math.isnan(x) for x in [atr14 or float('nan'), vol_sma20 or float('nan')]):
            return

        impulse = (body >= self.body_atr_mult * atr14) and (last.v >= self.volume_sma_mult * vol_sma20)
        strong_impulse = (body >= self.body_atr_strong * atr14)
        oi_trigger = False
        oi_side_hint = None
        if oi_5 is not None and abs(oi_5) >= self.oi_delta_pct_5m:
            oi_trigger = True
            oi_side_hint = 'short' if oi_5 > 0 else 'long'
        elif oi_15 is not None and abs(oi_15) >= self.oi_delta_pct_15m:
            oi_trigger = True
            oi_side_hint = 'short' if oi_15 > 0 else 'long'

        liq_trigger = False
        if liq_p95 is not None and liq_now >= liq_p95:
            liq_trigger = True

        sweep = detect_sweep(st.candles.buf, self.sweep_lookback)
        fvg = detect_recent_fvg(st.candles.buf[-self.fvg_lookback:])
        vwap = st.vwap
        vwap_dev_ok = False
        if vwap and vwap > 0:
            dev_pct = abs(last.c - vwap) / vwap * 100
            vwap_dev_ok = (dev_pct >= self.vwap_dev_pct)

        # BTC sync veto (light)
        btc_ok = True
        if symbol != "BTCUSDT" and "BTCUSDT" in self.state and len(self.state["BTCUSDT"].candles) >= 4:
            b = self.state["BTCUSDT"].candles
            btc_ret_3m = (b.buf[-1].c - b.buf[-4].c) / b.buf[-4].c * 100
            if sweep == 'up' and btc_ret_3m > self.btc_sync_div:   # avoid short against strong BTC up
                btc_ok = False
            if sweep == 'down' and btc_ret_3m < -self.btc_sync_div: # avoid long against strong BTC down
                btc_ok = False

        if not (impulse and oi_trigger and liq_trigger and btc_ok and sweep in ('up','down')):
            return

        # cooldown
        now_ts = now_ms()
        if now_ts - st.last_alert_ts < self.cooldown_ms:
            return

        side = 'LONG' if sweep == 'down' else 'SHORT'
        entry_from = min(last.o, last.c)
        entry_to   = max(last.o, last.c)
        price_now  = last.c

        if side == 'LONG':
            sl = last.l
            sw_hi, sw_lo = self._nearest_swings(st, window=20)
            tp1 = sw_hi if sw_hi and sw_hi > last.c else last.c + (entry_to - sl)
            tp2 = tp1 + (tp1 - entry_to)
            move_to_tp1 = tp1 - ((entry_from + entry_to)/2)
            move_to_sl  = ((entry_from + entry_to)/2) - sl
        else:
            sl = last.h
            sw_hi, sw_lo = self._nearest_swings(st, window=20)
            tp1 = sw_lo if sw_lo and sw_lo < last.c else last.c - (sl - entry_from)
            tp2 = tp1 - (entry_to - tp1)
            move_to_tp1 = ((entry_from + entry_to)/2) - tp1
            move_to_sl  = sl - ((entry_from + entry_to)/2)

        rr_tp1 = (move_to_tp1 / max(1e-9, move_to_sl)) if move_to_tp1>0 and move_to_sl>0 else 0.0
        profit_pct_tp1 = (abs(tp1 - ((entry_from+entry_to)/2)) / price_now) * 100.0

        # filters (unchanged except probability)
        if rr_tp1 < 2.0:
            return
        if profit_pct_tp1 < 2.0:
            return

        probability = self._probability(strong_impulse, fvg, vwap_dev_ok, oi_side_hint, side)
        if not (probability > PROB_MIN):
            return

        reasons = [
            f"импульс {body/atr14:.2f}×ATR",
            f"объём {last.v/max(1e-9, (vol_sma20 or 1)):.2f}×SMA20",
            f"ΔOI {'5' if oi_5 is not None else '15'}м {oi_5 if oi_5 is not None else oi_15:.2f}%",
            "ликвидации ≥ P95",
            "съём ликвидности вниз" if side=='LONG' else "съём ликвидности вверх"
        ]
        if fvg == 'bull': reasons.append("bull FVG")
        if fvg == 'bear': reasons.append("bear FVG")
        if vwap_dev_ok:   reasons.append("отклонение от VWAP")

        sig = Signal(
            symbol=symbol, side=side, price_now=price_now,
            entry_from=entry_from, entry_to=entry_to, sl=sl, tp1=tp1, tp2=tp2,
            rr_tp1=rr_tp1, profit_pct_tp1=profit_pct_tp1,
            probability=probability, reasons=reasons, ttl_min=SIGNAL_TTL_MIN
        )

        st.last_alert_ts = now_ts
        await self.send_signal_batch([sig])

    # ---- telegram output ----
    @staticmethod
    def _fmt_price(x: float) -> str:
        if x >= 100: return f"{x:.2f}"
        if x >= 1:   return f"{x:.4f}"
        return f"{x:.6f}"

    async def send_signal_batch(self, sigs: List[Signal]):
        if not sigs: return
        # sort by probability desc
        sigs = sorted(sigs, key=lambda s: (-s.probability, s.symbol))

        lines = ["*⚡ Сигналы (интрадей)*"]
        for s in sigs:
            entry = f"{self._fmt_price(s.entry_from)}–{self._fmt_price(s.entry_to)}"
            rr = f"{s.rr_tp1:.2f}"
            tp1p = f"+{s.profit_pct_tp1:.2f}%"
            # SL/TP deltas vs mid-entry
            mid = (s.entry_from + s.entry_to)/2
            sl_pct = (abs(mid - s.sl)/mid) * 100.0
            slp = f"-{sl_pct:.2f}%"
            block = (
                f"*{s.symbol}* | {s.side}\n"
                f"Цена: {self._fmt_price(s.price_now)}\n"
                f"Вход: {entry}\n"
                f"SL: {self._fmt_price(s.sl)} ({slp})\n"
                f"TP1: {self._fmt_price(s.tp1)} ({tp1p})  •  TP2: {self._fmt_price(s.tp2)}\n"
                f"R/R: {rr}  •  Вероятность: {s.probability:.0f}%  •  Валиден: ~{s.ttl_min} мин\n"
                f"_Причины:_ {', '.join(s.reasons)}"
            )
            lines.append(block)
        text = "\n\n".join(lines)

        for chat_id in self.chat_ids:
            try:
                await self.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            except Exception as e:
                log.warning("send_signal -> %s: %s", chat_id, repr(e))

# ------------ health & self-ping ------------
async def health_loop(bot: Bot, recipients: List[int]):
    await asyncio.sleep(HEALTH_FIRST_SEC)
    while True:
        for chat_id in recipients:
            try:
                await bot.send_message(chat_id=chat_id, text="🟢 online")
            except Exception as e:
                log.warning("[warn] health-check -> %s: %s", chat_id, repr(e))
        await asyncio.sleep(HEALTH_INTERVAL_SEC)

async def self_ping_loop():
    if not (SELF_PING_ENABLED and SELF_PING_URL):
        return
    await asyncio.sleep(STARTUP_PING_SEC)
    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            try:
                await client.get(SELF_PING_URL)
            except Exception:
                pass
            await asyncio.sleep(SELF_PING_INTERVAL_SEC)

# ------------ Telegram handlers ------------
def allowed(update: Update) -> bool:
    chat_id = update.effective_chat.id if update and update.effective_chat else None
    return chat_id in ALLOWED_CHAT_IDS

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    await update.effective_message.reply_text("pong")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update): return
    await update.effective_message.reply_text("🤖 OK. Фильтры: RR≥2.0, Profit≥2%, Вероятность>"+f"{PROB_MIN:.1f}%")

# ------------ main (webhook mode for Render) ------------
def main():
    if not PRIMARY_RECIPIENTS:
        raise SystemExit("PRIMARY_RECIPIENTS empty — ensure TELEGRAM_CHAT_ID is included into ALLOWED_CHAT_IDS.")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("status", cmd_status))

    bot = app.bot

    async def post_init(application: Application):
        # startup notif
        for cid in PRIMARY_RECIPIENTS:
            try:
                await bot.send_message(cid, f"✅ Render Web Service: бот запущен. Universe=auto-top-{UNIVERSE_TOP_N}, PROB>{PROB_MIN:.1f}%")
            except Exception as e:
                log.warning("startup msg -> %s: %s", cid, repr(e))

        # launch engine & loops
        session = aiohttp.ClientSession()
        engine = Engine(bot, PRIMARY_RECIPIENTS, session)
        await engine.bootstrap()
        await engine.ws_connect()

        application.create_task(engine.run_ws())
        application.create_task(engine.poll_oi())
        application.create_task(health_loop(bot, PRIMARY_RECIPIENTS))
        application.create_task(self_ping_loop())

    app.post_init = post_init

    # Webhook setup
    if not (PUBLIC_URL and WEBHOOK_URL):
        raise SystemExit("PUBLIC_URL/RENDER_EXTERNAL_URL is required for webhook mode on Render.")

    async def run():
        # ensure webhook is set fresh
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        await app.bot.set_webhook(WEBHOOK_URL, allowed_updates=Update.ALL_TYPES)

        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=WEBHOOK_PATH.lstrip("/"),
            webhook_url=WEBHOOK_URL,
            allowed_updates=Update.ALL_TYPES,
            stop_signals=None,  # safer for Render containers
        )

    asyncio.run(run())

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
