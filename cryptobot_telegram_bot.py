# cryptobot_telegram_bot.py
from __future__ import annotations

import os
import json
import math
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone, time as dtime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import numpy as np
import websockets
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ====================== OPTIONAL HARDCODED ENV DEFAULTS ======================
# Эти значения используются ТОЛЬКО если переменные окружения не заданы на Render.
# ⚠️ НЕ РЕКОМЕНДУЕТСЯ коммитить сюда реальный Telegram-токен.
HARDCODED_ENV = {
    # "TELEGRAM_BOT_TOKEN": "ВАШ_ТОКЕН_ЕСЛИ_ОСОЗНАННО_ХРАНИТЕ_В_КОДЕ",  # ⚠️ рискованно
    "ALLOWED_CHAT_IDS": "533232884,-1002870952333",
    "TELEGRAM_CHAT_ID": "-1002870952333",

    # Опционально: вручную зафиксированный список (ENV SYMBOLS имеет приоритет)
    # "SYMBOLS": "BTCUSDT,ETHUSDT,SOLUSDT",

    # Авто-подбор по волатильности
    "AUTO_VOL_ENABLED": "1",
    "AUTO_VOL_TOP_N": "15",
    "AUTO_VOL_SCAN_COUNT": "80",
    "AUTO_VOL_UTC_HOUR": "0",
    "AUTO_VOL_UTC_MIN": "10",
    "MAX_SYMBOLS": "30",

    # Интервалы задач (сек)
    "HEALTH_INTERVAL_SEC": "3600",
    "ENGINE_INTERVAL_SEC": "60",

    # Минутные отчёты отключены
    "SNAPSHOT_ENABLED": "0",

    # Фильтры сигналов
    "RR_MIN": "2.0",
    "MIN_PROFIT_PCT": "2.0",     # минимальная ожидаемая прибыль (%)
    "MIN_CONFIDENCE": "75",      # минимальная уверенность (0..100)

    # HTTP порт
    "PORT": "10000",
}
for k, v in HARDCODED_ENV.items():
    os.environ.setdefault(k, str(v))

# ====================== ENV & CONSTANTS ======================

def parse_int_list(s: str | None) -> list[int]:
    out: list[int] = []
    if not s:
        return out
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            pass
    return out

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN (в Render или раскомментируйте HARDCODED_ENV['TELEGRAM_BOT_TOKEN'])")

allowed_from_env = parse_int_list(os.environ.get("ALLOWED_CHAT_IDS"))
fallback_chat = parse_int_list(os.environ.get("TELEGRAM_CHAT_ID"))
ALLOWED_CHAT_IDS: list[int] = allowed_from_env or fallback_chat
if not ALLOWED_CHAT_IDS:
    raise SystemExit("Set ALLOWED_CHAT_IDS или TELEGRAM_CHAT_ID (в Render или HARDCODED_ENV)")

RECIPIENTS: list[int] = fallback_chat or ALLOWED_CHAT_IDS

ENV_SYMBOLS = [s.strip().upper() for s in (os.environ.get("SYMBOLS") or "").split(",") if s.strip()]
BYBIT_SYMBOL_FALLBACK = os.environ.get("BYBIT_SYMBOL", "BTCUSDT").upper()
CONFIG_PATH = os.environ.get("CONFIG_PATH", "bot_config.json")

PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL")
HTTP_PORT = int(os.environ.get("PORT", "10000"))
tok_left = BOT_TOKEN.split(":")[0] if ":" in BOT_TOKEN else BOT_TOKEN
WEBHOOK_PATH = f"/wh-{tok_left[-8:]}"

# Пороговые параметры анализа
VOL_MULT = float(os.environ.get("VOL_MULT", "2.0"))
VOL_SMA_PERIOD = int(os.environ.get("VOL_SMA_PERIOD", "20"))
ATR_PERIOD = int(os.environ.get("ATR_PERIOD", "14"))
BODY_ATR_MULT = float(os.environ.get("BODY_ATR_MULT", "0.6"))
BODY_ATR_STRONG = float(os.environ.get("BODY_ATR_STRONG", "0.8"))
OI_DELTA_PCT_5M = float(os.environ.get("OI_DELTA_PCT_5M", "2.0"))
OI_DELTA_PCT_15M = float(os.environ.get("OI_DELTA_PCT_15M", "3.0"))
LIQ_PCTL = float(os.environ.get("LIQ_PCTL", "95"))
VWAP_DEV_PCT = float(os.environ.get("VWAP_DEV_PCT", "0.5"))
BTC_SYNC_MAX_DIV = float(os.environ.get("BTC_SYNC_MAX_DIV", "0.4"))
ALERT_COOLDOWN_SEC = int(os.environ.get("ALERT_COOLDOWN_SEC", "240"))
RECENCY_MAX_SEC = int(os.environ.get("RECENCY_MAX_SEC", "120"))  # бар должен быть не старше 2 минут

# Авто-подбор
AUTO_VOL_ENABLED = (os.environ.get("AUTO_VOL_ENABLED", "1") != "0")
AUTO_VOL_TOP_N = int(os.environ.get("AUTO_VOL_TOP_N", "15"))
AUTO_VOL_SCAN_COUNT = int(os.environ.get("AUTO_VOL_SCAN_COUNT", "80"))
AUTO_VOL_UTC_HOUR = int(os.environ.get("AUTO_VOL_UTC_HOUR", "0"))
AUTO_VOL_UTC_MIN = int(os.environ.get("AUTO_VOL_UTC_MIN", "10"))
MAX_SYMBOLS = int(os.environ.get("MAX_SYMBOLS", "30"))

# Частоты задач
HEALTH_INTERVAL_SEC = int(os.environ.get("HEALTH_INTERVAL_SEC", "3600"))
ENGINE_INTERVAL_SEC = int(os.environ.get("ENGINE_INTERVAL_SEC", "60"))
SNAPSHOT_ENABLED = (os.environ.get("SNAPSHOT_ENABLED", "0") != "0")

# Фильтры сигналов
RR_MIN = float(os.environ.get("RR_MIN", "2.0"))
MIN_PROFIT_PCT = float(os.environ.get("MIN_PROFIT_PCT", "2.0"))
MIN_CONFIDENCE = int(os.environ.get("MIN_CONFIDENCE", "75"))

print(f"[info] ALLOWED_CHAT_IDS = {sorted(ALLOWED_CHAT_IDS)}")
print(f"[info] TELEGRAM_CHAT_ID(raw) = '{os.environ.get('TELEGRAM_CHAT_ID', '')}'")
print(f"[info] RECIPIENTS (whitelisted) = {sorted(RECIPIENTS)}")
print(f"[info] HTTP_PORT = {HTTP_PORT}")
if PUBLIC_URL:
    print(f"[info] PUBLIC_URL = '{PUBLIC_URL}'")
print(f"[info] WEBHOOK_PATH = '{WEBHOOK_PATH}'")
print(f"[info] Volume trigger params: VOL_MULT={VOL_MULT}, VOL_SMA_PERIOD={VOL_SMA_PERIOD}, "
      f"BODY_ATR_MULT={BODY_ATR_MULT}, ATR_PERIOD={ATR_PERIOD}, COOLDOWN={ALERT_COOLDOWN_SEC}s, RECENCY={RECENCY_MAX_SEC}s")
print(f"[info] AutoVol: enabled={AUTO_VOL_ENABLED}, topN={AUTO_VOL_TOP_N}, "
      f"scan={AUTO_VOL_SCAN_COUNT}, time={AUTO_VOL_UTC_HOUR:02d}:{AUTO_VOL_UTC_MIN:02d}Z, max={MAX_SYMBOLS}")
print(f"[info] Filters: RR_MIN={RR_MIN}, MIN_PROFIT_PCT={MIN_PROFIT_PCT}%, MIN_CONFIDENCE={MIN_CONFIDENCE}")

# ====================== STATE & MODELS ======================

BYBIT_REST_BASE = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"

@dataclass
class Candle:
    t: int
    o: float
    h: float
    l: float
    c: float
    v: float
    confirm: bool = True

@dataclass
class SymState:
    candles: List[Candle] = field(default_factory=list)  # 1m, ~200+ баров
    vwap_num: float = 0.0
    vwap_den: float = 0.0
    vwap: float = float("nan")
    last_daily_reset: Optional[int] = None
    oi_series: List[Tuple[int, float]] = field(default_factory=list)
    liq_5m_history: List[float] = field(default_factory=list)
    liq_bucket_start: Optional[int] = None
    liq_bucket_notional: float = 0.0
    last_alert_ms: int = 0
    last_alert_key: Optional[str] = None  # для анти-дублей

class GlobalState:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.symbols: List[str] = []
        self.syms: Dict[str, SymState] = {}

    async def load_symbols(self):
        symbols = list(ENV_SYMBOLS)
        if not symbols:
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict) and isinstance(data.get("symbols"), list):
                        symbols = [str(x).upper() for x in data["symbols"] if str(x).strip()]
            except Exception:
                pass
        if not symbols:
            symbols = [BYBIT_SYMBOL_FALLBACK]
        async with self._lock:
            self.symbols = symbols
            for s in symbols:
                self.syms.setdefault(s, SymState())

    async def set_symbols(self, new_symbols: List[str]):
        async with self._lock:
            self.symbols = new_symbols
            for s in new_symbols:
                self.syms.setdefault(s, SymState())
            try:
                with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump({"symbols": self.symbols}, f, ensure_ascii=False, indent=2)
            except Exception:
                pass

    async def get_symbols(self) -> List[str]:
        async with self._lock:
            return list(self.symbols)

    async def get_symstate(self, symbol: str) -> SymState:
        async with self._lock:
            return self.syms.setdefault(symbol, SymState())

STATE = GlobalState()

# ====================== BYBIT HELPERS ======================

async def bybit_get(session: aiohttp.ClientSession, path: str, params: dict) -> Optional[dict]:
    url = f"{BYBIT_REST_BASE}{path}"
    try:
        async with session.get(url, params=params, timeout=10) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None

async def validate_symbols_linear(symbols: List[str]) -> tuple[List[str], List[str]]:
    ok, bad = [], []
    async with aiohttp.ClientSession() as s:
        for sym in symbols:
            data = await bybit_get(s, "/v5/market/instruments-info", {"category": "linear", "symbol": sym})
            if data and data.get("retCode") == 0 and (data.get("result") or {}).get("list"):
                ok.append(sym)
            else:
                bad.append(sym)
    return ok, bad

async def get_kline(symbol: str, interval: str, limit: int = 200) -> List[Candle]:
    async with aiohttp.ClientSession() as s:
        data = await bybit_get(s, "/v5/market/kline", {
            "category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)
        })
    out: List[Candle] = []
    if not data or data.get("retCode") != 0:
        return out
    rows = (data.get("result") or {}).get("list") or []
    for row in rows:
        t = int(row[0]); o, h, l, c, v = float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])
        out.append(Candle(t, o, h, l, c, v, True))
    out.sort(key=lambda x: x.t)
    return out

async def get_kline_1m(symbol: str, limit: int = 200) -> List[Candle]:
    return await get_kline(symbol, "1", limit)

async def get_kline_daily_closes(session: aiohttp.ClientSession, symbol: str, limit: int = 8) -> List[float]:
    data = await bybit_get(session, "/v5/market/kline", {
        "category": "linear", "symbol": symbol, "interval": "D", "limit": str(limit)
    })
    closes: List[float] = []
    if not data or data.get("retCode") != 0:
        return closes
    rows = (data.get("result") or {}).get("list") or []
    for row in rows:
        closes.append(float(row[4]))
    closes.sort()
    return closes

async def get_oi_snapshots(symbol: str, interval: str = "5min", limit: int = 4) -> List[Tuple[int, float]]:
    async with aiohttp.ClientSession() as s:
        data = await bybit_get(s, "/v5/market/open-interest", {
            "category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)
        })
    out: List[Tuple[int, float]] = []
    if not data or data.get("retCode") != 0:
        return out
    rows = (data.get("result") or {}).get("list") or []
    for row in reversed(rows):
        ts = int(row["timestamp"]); oi = float(row["openInterest"])
        out.append((ts, oi))
    return out

# ====================== ИНДИКАТОРЫ / SMC ======================

def fmt_price(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.6f}"

def atr(candles: List[Candle], period: int = 14) -> float:
    if len(candles) < period + 1:
        return float("nan")
    trs = []
    for i in range(1, period + 1):
        c0 = candles[-i-1]; c1 = candles[-i]
        tr = max(c1.h - c1.l, abs(c1.h - c0.c), abs(c1.l - c0.c))
        trs.append(tr)
    return float(sum(trs) / period)

def sma(values: List[float], period: int) -> float:
    if len(values) < period:
        return float("nan")
    return float(sum(values[-period:]) / period)

def percentile(values: List[float], p: float) -> float:
    if not values:
        return float("nan")
    arr = np.array(values, dtype=float)
    return float(np.percentile(arr, p))

def detect_recent_fvg(candles: List[Candle]) -> Optional[str]:
    if len(candles) < 3:
        return None
    c0, c1, c2 = candles[-3], candles[-2], candles[-1]
    if c2.l > c0.h:
        return "bull"
    if c2.h < c0.l:
        return "bear"
    return None

def detect_sweep(candles: List[Candle], lookback: int = 30) -> Optional[str]:
    if len(candles) < lookback + 1:
        return None
    last = candles[-1]
    highs = [c.h for c in candles[-(lookback+1):-1]]
    lows  = [c.l for c in candles[-(lookback+1):-1]]
    down = last.l < min(lows) and last.c > last.o
    up   = last.h > max(highs) and last.c < last.o
    if down: return "down"
    if up:   return "up"
    return None

def maybe_reset_vwap(st: SymState, t_ms: int):
    day_ms = int(datetime.fromtimestamp(t_ms/1000, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    if st.last_daily_reset is None or day_ms != st.last_daily_reset:
        st.vwap_num = 0.0; st.vwap_den = 0.0; st.vwap = float("nan"); st.last_daily_reset = day_ms

def update_vwap(st: SymState, c: Candle):
    typical = (c.h + c.l + c.c) / 3.0
    st.vwap_num += typical * c.v
    st.vwap_den += c.v
    if st.vwap_den > 0:
        st.vwap = st.vwap_num / st.vwap_den

def bos_bias(candles: List[Candle], lb: int = 30) -> Optional[str]:
    """Простой BOS: пробой max/min последних lb баров телом/закрытием."""
    if len(candles) < lb + 1:
        return None
    prev_high = max(c.h for c in candles[-(lb+1):-1])
    prev_low  = min(c.l for c in candles[-(lb+1):-1])
    last = candles[-1]
    if last.c > prev_high:
        return "up"
    if last.c < prev_low:
        return "down"
    return None

def premium_discount(candles: List[Candle], lb: int = 50) -> Optional[str]:
    """Цена относительно mid диапазона lb баров: premium(>mid) / discount(<mid)."""
    if len(candles) < lb:
        return None
    window = candles[-lb:]
    hi = max(c.h for c in window)
    lo = min(c.l for c in window)
    if hi <= lo:
        return None
    mid = (hi + lo) / 2.0
    px = window[-1].c
    return "premium" if px > mid else "discount"

# === Калибровка confidence -> вероятность (%)
def conf_to_prob_pct(conf: int) -> float:
    """
    Преобразует целочисленную уверенность [0..100] в «вероятность (оценку)» в процентах.
    Линейная сжатая шкала вокруг 50:
      50 -> ~55%, 60 -> ~62%, 70 -> ~69%, 80 -> ~76%, 90 -> ~83%, 100 -> ~90%
    Сделано, чтобы не обещать экстремальные проценты без статистической калибровки.
    """
    p = 0.55 + (conf - 50) * 0.007
    p = max(0.45, min(0.90, p))
    return round(p * 100.0, 1)

# ====================== ENGINE ======================

@dataclass
class Signal:
    symbol: str
    side: str   # LONG/SHORT
    price: float
    entry: float
    sl: float
    tp: float
    pct_tp: float
    pct_sl: float
    confidence: int
    rr_ratio: float
    notes: List[str]
    prob_pct: float  # «вероятность (оценка)» в %

def rr_ratio_calc(side: str, entry: float, tp: float, sl: float) -> float:
    if side == "LONG":
        reward = max(1e-9, tp - entry)
        risk = max(1e-9, entry - sl)
    else:
        reward = max(1e-9, entry - tp)
        risk = max(1e-9, sl - entry)
    return float(reward / risk)

async def bootstrap_history():
    syms = await STATE.get_symbols()
    for sym in syms:
        st = await STATE.get_symstate(sym)
        kl = await get_kline_1m(sym, limit=220)
        if not kl:
            continue
        st.candles = kl
        for c in kl:
            maybe_reset_vwap(st, c.t); update_vwap(st, c)
        st.oi_series = await get_oi_snapshots(sym, "5min", 4)

async def bootstrap_for_symbols(symbols: List[str]):
    for sym in symbols:
        st = await STATE.get_symstate(sym)
        kl = await get_kline_1m(sym, limit=220)
        if not kl:
            continue
        st.candles = kl
        st.vwap_num = st.vwap_den = 0.0; st.vwap = float("nan"); st.last_daily_reset = None
        for c in kl:
            maybe_reset_vwap(st, c.t); update_vwap(st, c)
        st.oi_series = await get_oi_snapshots(sym, "5min", 4)

async def ws_liq_loop():
    syms = await STATE.get_symbols()
    if not syms:
        return
    args = [f"liquidation.{s}" for s in syms]
    sub = {"op": "subscribe", "args": args}
    async with websockets.connect(BYBIT_WS_PUBLIC_LINEAR, ping_interval=25, ping_timeout=20) as ws:
        await ws.send(json.dumps(sub))
        async for msg in ws:
            try:
                data = json.loads(msg)
            except Exception:
                continue
            topic = data.get("topic")
            if not topic or not topic.startswith("liquidation."):
                continue
            _, symbol = topic.split(".", 1)
            st = await STATE.get_symstate(symbol)
            now_bucket = (int(datetime.now(timezone.utc).timestamp()) // 300) * 300
            if st.liq_bucket_start is None:
                st.liq_bucket_start = now_bucket
            if now_bucket != st.liq_bucket_start:
                st.liq_5m_history.append(st.liq_bucket_notional)
                if len(st.liq_5m_history) > 2100:
                    st.liq_5m_history = st.liq_5m_history[-2100:]
                st.liq_bucket_notional = 0.0
                st.liq_bucket_start = now_bucket
            for it in data.get("data", []):
                try:
                    qty = float(it.get("execQty", 0.0)); price = float(it.get("execPrice", 0.0))
                    st.liq_bucket_notional += qty * price
                except Exception:
                    continue

def _oi_change_pct(series: List[Tuple[int, float]], minutes: int) -> Optional[float]:
    if len(series) < 2:
        return None
    now_ts = series[-1][0]
    cutoff = now_ts - minutes*60*1000
    recent = [x for x in series if x[0] >= cutoff]
    if len(recent) < 2:
        return None
    a, b = recent[0][1], recent[-1][1]
    if a == 0:
        return None
    return (b - a) / a * 100.0

async def evaluate_symbol(symbol: str) -> Optional[Signal]:
    st = await STATE.get_symstate(symbol)
    # Обновим последний бар, VWAP, OI
    recent = await get_kline_1m(symbol, limit=2)
    if recent:
        last = recent[-1]
        if st.candles and st.candles[-1].t == last.t:
            st.candles[-1] = last
        else:
            st.candles.extend(recent if not st.candles else [last])
            st.candles = st.candles[-240:]
        maybe_reset_vwap(st, last.t); update_vwap(st, last)
    if len(st.candles) < max(ATR_PERIOD+1, VOL_SMA_PERIOD, 60):
        return None

    # Актуальность бара
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if now_ms - st.candles[-1].t > RECENCY_MAX_SEC * 1000:
        return None

    oi_last = await get_oi_snapshots(symbol, "5min", 1)
    if oi_last:
        st.oi_series.extend(oi_last); st.oi_series = st.oi_series[-40:]

    c = st.candles[-1]
    body = abs(c.c - c.o)
    atr14 = atr(st.candles, ATR_PERIOD)
    vol_sma = sma([x.v for x in st.candles], VOL_SMA_PERIOD)
    if math.isnan(atr14) or math.isnan(vol_sma) or vol_sma == 0:
        return None

    # --- Индикаторы/фичи ---
    fvg = detect_recent_fvg(st.candles[-20:])
    sweep = detect_sweep(st.candles, 30)
    bos  = bos_bias(st.candles, 30)
    pd   = premium_discount(st.candles, 50)
    vwap_dev_ok = True
    vwap_dev_pct = float("nan")
    if st.vwap and st.vwap > 0:
        vwap_dev_pct = abs(c.c - st.vwap) / st.vwap * 100.0
        vwap_dev_ok = (vwap_dev_pct >= VWAP_DEV_PCT)

    oi5  = _oi_change_pct(st.oi_series, 5)
    oi15 = _oi_change_pct(st.oi_series, 15)
    oi_trigger = False
    oi_hint: Optional[str] = None
    if oi5 is not None and abs(oi5) >= OI_DELTA_PCT_5M:
        oi_trigger = True; oi_hint = "short" if oi5 > 0 else "long"
    elif oi15 is not None and abs(oi15) >= OI_DELTA_PCT_15M:
        oi_trigger = True; oi_hint = "short" if oi15 > 0 else "long"

    liq_p95 = percentile(st.liq_5m_history[-500:], LIQ_PCTL) if st.liq_5m_history else float("nan")
    liq_now = st.liq_bucket_notional
    liq_trigger = (not math.isnan(liq_p95)) and liq_now >= liq_p95

    impulse = (body >= BODY_ATR_MULT * atr14) and (c.v >= VOL_MULT * vol_sma)
    strong_impulse = (body >= BODY_ATR_STRONG * atr14)

    # BTC sync
    btc_ok = True
    if symbol != "BTCUSDT":
        bstate = await STATE.get_symstate("BTCUSDT")
        if len(bstate.candles) >= 4:
            br = (bstate.candles[-1].c - bstate.candles[-4].c) / bstate.candles[-4].c * 100.0
            if sweep == "up" and br > BTC_SYNC_MAX_DIV:
                btc_ok = False
            if sweep == "down" and br < -BTC_SYNC_MAX_DIV:
                btc_ok = False

    # --- SMC bias & комбинированный скор ---
    side: Optional[str] = None
    reasons: List[str] = []

    smc_long = ((bos == "up") or (sweep == "down") or (fvg == "bull"))
    smc_short = ((bos == "down") or (sweep == "up") or (fvg == "bear"))

    # Премиум/Дискаунт фильтр
    if pd == "discount":
        smc_long = smc_long or True
    if pd == "premium":
        smc_short = smc_short or True

    long_score = 0
    short_score = 0

    if smc_long: long_score += 1
    if smc_short: short_score += 1

    if impulse:
        if c.c > c.o: long_score += 1
        if c.c < c.o: short_score += 1
    if strong_impulse:
        if c.c > c.o: long_score += 1
        if c.c < c.o: short_score += 1

    if oi_trigger:
        if oi_hint == "long": long_score += 1
        if oi_hint == "short": short_score += 1

    if liq_trigger:
        if sweep == "down": long_score += 1
        if sweep == "up": short_score += 1

    if vwap_dev_ok:
        if sweep == "down": long_score += 1
        if sweep == "up": short_score += 1

    if btc_ok:
        if c.c >= c.o: long_score += 1
        else: short_score += 1

    if long_score >= max(2, short_score + 1):
        side = "LONG"
    elif short_score >= max(2, long_score + 1):
        side = "SHORT"
    else:
        return None  # слабый сетап

    # --- Расчёт входа/SL/TP ---
    entry = c.c
    if side == "LONG":
        swing_low = min(x.l for x in st.candles[-20:])
        sl = min(c.l, swing_low) - 0.2 * atr14
        tp1_struct = max(x.h for x in st.candles[-20:])
        tp1_atr = entry + 1.6 * atr14
        tp = max(tp1_struct, tp1_atr)
    else:
        swing_high = max(x.h for x in st.candles[-20:])
        sl = max(c.h, swing_high) + 0.2 * atr14
        tp1_struct = min(x.l for x in st.candles[-20:])
        tp1_atr = entry - 1.6 * atr14
        tp = min(tp1_struct, tp1_atr)

    # sanity
    if (side == "LONG" and (tp <= entry or sl >= entry)) or (side == "SHORT" and (tp >= entry or sl <= entry)):
        return None

    pct_tp = (tp - entry) / entry * 100.0
    pct_sl = (sl - entry) / entry * 100.0
    if side == "LONG" and pct_sl > 0: pct_sl = -abs(pct_sl)
    if side == "SHORT" and pct_sl < 0: pct_sl = abs(pct_sl)

    conf = 50
    conf += (long_score if side == "LONG" else short_score) * 5
    if strong_impulse: conf += 5
    if fvg: conf += 5
    if vwap_dev_ok: conf += 3
    conf = max(0, min(100, conf))

    # причины
    if bos: reasons.append(f"BOS {bos}")
    if sweep: reasons.append(f"sweep {sweep}")
    if fvg: reasons.append(f"FVG {fvg}")
    reasons.append(f"vol {c.v/max(1e-9, vol_sma):.2f}×")
    oi5v = _oi_change_pct(st.oi_series, 5)
    oi15v = _oi_change_pct(st.oi_series, 15)
    if oi5v is not None: reasons.append(f"ΔOI5 {oi5v:.2f}%")
    if oi15v is not None: reasons.append(f"ΔOI15 {oi15v:.2f}%")
    if liq_trigger: reasons.append("liq≥P95")
    if not math.isnan(vwap_dev_pct): reasons.append(f"|Px-VWAP| {vwap_dev_pct:.2f}%")

    rr = rr_ratio_calc(side, entry, tp, sl)
    prob_pct = conf_to_prob_pct(conf)

    return Signal(
        symbol=symbol, side=side,
        price=c.c, entry=entry, sl=sl, tp=tp,
        pct_tp=pct_tp, pct_sl=pct_sl,
        confidence=int(round(conf)), rr_ratio=rr, notes=reasons,
        prob_pct=prob_pct,
    )

def _signal_key(sig: Signal) -> str:
    # округляем для устойчивости
    return f"{sig.symbol}:{sig.side}:{round(sig.entry, 6)}:{round(sig.sl, 6)}:{round(sig.tp, 6)}"

async def send_signals(app: Application, sigs: List[Signal]):
    if not sigs:
        return

    # Сортировка: самый вероятный (conf) → лучший RR → больший потенциал TP
    sigs.sort(key=lambda s: (-s.confidence, -s.rr_ratio, -abs(s.pct_tp), s.symbol))

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    filtered: List[Signal] = []
    for s in sigs:
        st = await STATE.get_symstate(s.symbol)
        key = _signal_key(s)
        allow_time = (now_ms - st.last_alert_ms) >= (ALERT_COOLDOWN_SEC * 1000)
        changed = (st.last_alert_key != key)
        if changed or allow_time:
            filtered.append(s)
            st.last_alert_key = key
            st.last_alert_ms = now_ms

    if not filtered:
        return

    def fp(x: float) -> str: return fmt_price(x)
    for s in filtered:
        lines = [
            f"*{s.symbol}* | *{s.side}*",
            f"Цена: {fp(s.price)}",
            f"Вход: {fp(s.entry)}",
        ]
        if s.side == "LONG":
            lines.append(f"TP: {fp(s.tp)} (+{s.pct_tp:.2f}%)")
            lines.append(f"SL: {fp(s.sl)} ({s.pct_sl:.2f}%)")
        else:
            lines.append(f"TP: {fp(s.tp)} ({s.pct_tp:.2f}%)")
            lines.append(f"SL: {fp(s.sl)} (+{abs(s.pct_sl):.2f}%)")
        lines.append(f"Уверенность: {s.confidence}/100  |  Вероятность (оценка): {s.prob_pct:.1f}%  |  R/R: {s.rr_ratio:.2f}")
        if s.notes:
            lines.append("Основания: " + ", ".join(s.notes))
        text = "\n".join(lines)
        for chat_id in RECIPIENTS:
            if chat_id in ALLOWED_CHAT_IDS:
                try:
                    await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
                except Exception:
                    pass

# ====================== PERIODIC JOBS ======================

async def job_health(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    for chat_id in RECIPIENTS:
        if chat_id in ALLOWED_CHAT_IDS:
            try:
                await app.bot.send_message(chat_id=chat_id, text="🟢 online")
            except Exception:
                pass

async def job_engine(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    syms = await STATE.get_symbols()
    sigs: List[Signal] = []
    for sym in syms:
        sig = await evaluate_symbol(sym)
        # ---- ФИЛЬТРЫ: R/R, минимум прибыли, минимум уверенности ----
        if not sig:
            continue
        if sig.rr_ratio < RR_MIN:
            continue
        if abs(sig.pct_tp) < MIN_PROFIT_PCT:
            continue
        if sig.confidence < MIN_CONFIDENCE:
            continue
        sigs.append(sig)
    # если пусто — НИЧЕГО не отправляем
    await send_signals(app, sigs)

# ====================== AUTO-VOL PICKER ======================

async def bybit_get_tickers(session: aiohttp.ClientSession) -> List[dict]:
    data = await bybit_get(session, "/v5/market/tickers", {"category": "linear"})
    if not data or data.get("retCode") != 0:
        return []
    return (data.get("result") or {}).get("list") or []

async def fetch_top_by_turnover(session: aiohttp.ClientSession, n: int) -> List[str]:
    rows = await bybit_get_tickers(session)
    pool: List[Tuple[str, float]] = []
    for r in rows:
        sym = str(r.get("symbol", ""))
        if not sym.endswith("USDT"):
            continue
        try:
            turn = float(r.get("turnover24h", 0.0))
        except Exception:
            turn = 0.0
        pool.append((sym, turn))
    pool.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in pool[:max(1, n)]]

def realized_vol_pct(closes: List[float]) -> float:
    if len(closes) < 5:
        return float("nan")
    rets = []
    for i in range(1, len(closes)):
        if closes[i-1] <= 0:
            return float("nan")
        rets.append((closes[i] - closes[i-1]) / closes[i-1] * 100.0)
    if not rets:
        return float("nan")
    return float(np.std(np.array(rets, dtype=float), ddof=1))

async def pick_symbols_by_week_vol(top_n: int, scan_count: int) -> List[str]:
    async with aiohttp.ClientSession() as s:
        pool = await fetch_top_by_turnover(s, scan_count)
        if not pool:
            return []
        sem = asyncio.Semaphore(10)
        vols: Dict[str, float] = {}

        async def worker(sym: str):
            async with sem:
                closes = await get_kline_daily_closes(s, sym, limit=8)
                vol = realized_vol_pct(closes)
                if not math.isnan(vol):
                    vols[sym] = vol
                await asyncio.sleep(0.02)

        await asyncio.gather(*(worker(sym) for sym in pool))
        ranked = sorted(vols.items(), key=lambda kv: kv[1], reverse=True)
        return [sym for sym, _ in ranked[:max(1, top_n)]]

async def job_autovol(context: ContextTypes.DEFAULT_TYPE, manual_topn: Optional[int] = None) -> Tuple[List[str], List[str]]:
    topn = manual_topn if manual_topn is not None else AUTO_VOL_TOP_N
    before = await STATE.get_symbols()
    try:
        picked = await pick_symbols_by_week_vol(topn, AUTO_VOL_SCAN_COUNT)
        if not picked:
            return [], before
        new = list(before)
        for sym in picked:
            if sym not in new:
                new.append(sym)
        if len(new) > MAX_SYMBOLS:
            new = new[:MAX_SYMBOLS]
        await STATE.set_symbols(new)
        to_bootstrap = [s for s in new if s not in before]
        if to_bootstrap:
            await bootstrap_for_symbols(to_bootstrap)
        added = [s for s in new if s not in before]
        # уведомление
        try:
            await context.application.bot.send_message(
                chat_id=RECIPIENTS[0],
                text=f"🔁 AutoVol обновил список.\nДобавлены: {', '.join(added) if added else '—'}\nИтог: {', '.join(new)}"
            )
        except Exception:
            pass
        return added, new
    except Exception:
        return [], before

# ====================== COMMANDS ======================

async def safe_reply(update: Update, text: str):
    chat = update.effective_chat
    if not chat or chat.id not in ALLOWED_CHAT_IDS:
        return
    await update.effective_message.reply_text(text)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    await safe_reply(update,
        "Привет! Я ChaSerBot.\n"
        "Команды:\n"
        "• /ping — проверка бота\n"
        "• /about — сведения\n"
        "• /status — активные символы\n"
        "• /symbol <SYMBOL>\n"
        "• /symbols — показать/установить список (через запятую)\n"
        "• /autosymbols [N] — добавить топ-N самых волатильных за неделю"
    )

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    await safe_reply(update, "pong")

async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    syms = await STATE.get_symbols()
    auto_line = f"AutoVol: {'ON' if AUTO_VOL_ENABLED else 'OFF'}, topN={AUTO_VOL_TOP_N}, scan={AUTO_VOL_SCAN_COUNT}, max={MAX_SYMBOLS}"
    await safe_reply(update, f"ChaSerBot (webhook)\nSymbols: {', '.join(syms)}\nWhitelist: {', '.join(map(str, ALLOWED_CHAT_IDS))}\n"
                             f"{auto_line}\nFilters: RR_MIN={RR_MIN}, MIN_PROFIT_PCT={MIN_PROFIT_PCT}%, MIN_CONFIDENCE={MIN_CONFIDENCE}")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    syms = await STATE.get_symbols()
    await safe_reply(update, f"Активные символы ({len(syms)}): {', '.join(syms)}")

def _normalize_symbols_arg(args: List[str]) -> List[str]:
    joined = " ".join(args).replace(";", ",")
    parts = [p.strip().upper() for p in joined.split(",") if p.strip()]
    seen, out = set(), []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

async def cmd_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    if not context.args:
        await safe_reply(update, "Формат: /symbol BTCUSDT")
        return
    sym = context.args[0].strip().upper()
    ok, bad = await validate_symbols_linear([sym])
    if bad:
        await safe_reply(update, f"Нет такого linear на Bybit: {bad[0]}")
        return
    await STATE.set_symbols(ok)
    await bootstrap_for_symbols(ok)
    await safe_reply(update, f"Готово: {ok[0]}")

async def cmd_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    if not context.args:
        syms = await STATE.get_symbols()
        await safe_reply(update, f"Текущие: {', '.join(syms)}\nУстановить: /symbols BTCUSDT,ETHUSDT,SOLUSDT")
        return
    wanted = _normalize_symbols_arg(context.args)
    if not wanted:
        await safe_reply(update, "Пример: /symbols BTCUSDT,ETHUSDT")
        return
    ok, bad = await validate_symbols_linear(wanted)
    msg = []
    if bad:
        msg.append(f"Не найдены: {', '.join(bad)}")
    if ok:
        await STATE.set_symbols(ok)
        await bootstrap_for_symbols(ok)
        msg.append(f"Новые символы: {', '.join(ok)}")
    await safe_reply(update, "\n".join(msg) if msg else "Не удалось обновить список.")

async def cmd_autosymbols(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    topn = None
    if context.args:
        try:
            topn = int(context.args[0])
            if topn <= 0:
                topn = None
        except Exception:
            topn = None
    added, newlist = await job_autovol(context, manual_topn=topn)
    if not added:
        await safe_reply(update, "AutoVol: ничего не добавлено (возможно, уже в списке).")
    else:
        await safe_reply(update, f"AutoVol: добавил {len(added)} — {', '.join(added)}\nИтог ({len(newlist)}): {', '.join(newlist)}")

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        return
    await safe_reply(update, "Неизвестная команда. Попробуй /ping, /about, /status, /symbol, /symbols, /autosymbols.")

# ====================== APP BOOTSTRAP ======================

async def post_init(app: Application):
    await STATE.load_symbols()
    syms = await STATE.get_symbols()
    print(f"[info] Symbols at start: {', '.join(syms)}")
    await bootstrap_history()
    app.create_task(ws_liq_loop())

    if AUTO_VOL_ENABLED:
        app.job_queue.run_once(lambda ctx: job_autovol(ctx), when=10)

def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    common = filters.ChatType.PRIVATE | filters.ChatType.GROUPS | filters.ChatType.CHANNEL
    app.add_handler(CommandHandler("start", cmd_start, filters=common))
    app.add_handler(CommandHandler("ping", cmd_ping, filters=common))
    app.add_handler(CommandHandler("about", cmd_about, filters=common))
    app.add_handler(CommandHandler("status", cmd_status, filters=common))
    app.add_handler(CommandHandler("symbol", cmd_symbol, filters=common))
    app.add_handler(CommandHandler("symbols", cmd_symbols, filters=common))
    app.add_handler(CommandHandler("autosymbols", cmd_autosymbols, filters=common))
    app.add_handler(MessageHandler(filters.COMMAND & common, unknown_command))

    jq = app.job_queue
    jq.run_repeating(job_health, interval=HEALTH_INTERVAL_SEC, first=10)
    jq.run_repeating(job_engine, interval=ENGINE_INTERVAL_SEC, first=20)

    if AUTO_VOL_ENABLED:
        jq.run_daily(
            callback=lambda ctx: job_autovol(ctx),
            time=dtime(hour=AUTO_VOL_UTC_HOUR, minute=AUTO_VOL_UTC_MIN, tzinfo=timezone.utc),
        )

    return app

def main():
    app = build_application()
    if PUBLIC_URL:
        app.run_webhook(
            listen="0.0.0.0",
            port=HTTP_PORT,
            url_path=WEBHOOK_PATH.lstrip("/"),
            webhook_url=f"{PUBLIC_URL.rstrip('/')}{WEBHOOK_PATH}",
            allowed_updates=Update.ALL_TYPES,
            stop_signals=None,
        )
    else:
        app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
