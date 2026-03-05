# -*- coding: utf-8 -*-
"""
Cryptobot — SCALPING Signals (Bybit V5, USDT Perpetuals)
v15 — Адаптивный BB Squeeze (перцентильный порог вместо абсолютного):

  Шаг 1. Фильтр пула (каждые 15 мин):
           Топ-50 по объёму + ATR(15m) > 1.5% цены

  Шаг 2. Контекст тренда (15m):
           VWAP(15m) → разрешаем только LONG если цена > VWAP,
                        только SHORT если цена < VWAP

  Шаг 3. Триггер (5m):
           BB Squeeze (адаптивный) → Пробой закрытой свечи
           за верхнюю BB (LONG) или нижнюю BB (SHORT)

  Шаг 4. Жёсткая фильтрация:
           Объём пробойной свечи > SMA(20, vol) × VOL_SPIKE_MULT
           MACD-гистограмма совпадает с направлением и растёт

  Шаг 5. Сигнал в Telegram:
           SL — за нижнюю/верхнюю BB или локальный min/max
           TP — минимум TP_MIN_PCT от точки входа

  Изменения v14 → v15:
    - BB_SQUEEZE_THRESHOLD (абсолютный порог) УДАЛЁН.
      Вместо него — BB_SQUEEZE_PERCENTILE + BB_SQUEEZE_LOOKBACK.
      Squeeze теперь определяется не как "bandwidth < 3%", а как
      "bandwidth в нижних N% от последних LOOKBACK баров того же
      символа". Это делает фильтр адаптивным к рыночным условиям:
      в тренде baseline шире — порог автоматически выше.
    - POSITION_COOLDOWN_SEC удалена (не использовалась в коде).
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

# =========================
# ШАГ 1: ФИЛЬТР ПУЛА
# =========================
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "900"))   # 15 минут
TOP_SYMBOLS_COUNT    = int(os.getenv("TOP_SYMBOLS_COUNT",    "50"))
ATR_MIN_PCT          = float(os.getenv("ATR_MIN_PCT",        "0.015")) # ATR(15m) >= 1.5%
ATR_FILTER_PERIOD    = int(os.getenv("ATR_FILTER_PERIOD",    "14"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
                "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT"]

# =========================
# ШАГ 2: КОНТЕКСТ ТРЕНДА
# =========================
TF_TREND  = "15"
VWAP_BARS = int(os.getenv("VWAP_BARS", "50"))

# =========================
# ШАГ 3: ТРИГГЕР BB (адаптивный Squeeze)
# =========================
TF_SCALP  = "5"
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_STDDEV = float(os.getenv("BB_STDDEV", "2.0"))

# Squeeze считается адаптивно: bandwidth текущих BB_SQUEEZE_BARS свечей
# должен быть ≤ BB_SQUEEZE_PERCENTILE-му перцентилю bandwidth за
# последние BB_SQUEEZE_LOOKBACK баров того же символа.
# Пример: PERCENTILE=0.30, LOOKBACK=50 → сжатие = нижние 30% за 50 баров.
BB_SQUEEZE_BARS       = int(os.getenv("BB_SQUEEZE_BARS",       "3"))
BB_SQUEEZE_PERCENTILE = float(os.getenv("BB_SQUEEZE_PERCENTILE", "0.30"))
BB_SQUEEZE_LOOKBACK   = int(os.getenv("BB_SQUEEZE_LOOKBACK",    "50"))

# =========================
# ШАГ 4: ФИЛЬТРЫ
# =========================
VOL_SMA_PERIOD = int(os.getenv("VOL_SMA_PERIOD",  "20"))
VOL_SPIKE_MULT = float(os.getenv("VOL_SPIKE_MULT", "1.5"))

MACD_FAST   = int(os.getenv("MACD_FAST",   "12"))
MACD_SLOW   = int(os.getenv("MACD_SLOW",   "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))

# =========================
# ШАГ 5: TP / SL
# =========================
TP_MIN_PCT   = float(os.getenv("TP_MIN_PCT",   "0.010"))
TP_MAX_PCT   = float(os.getenv("TP_MAX_PCT",   "0.025"))
SL_BB_BUFFER = float(os.getenv("SL_BB_BUFFER", "0.001"))
RR_MIN       = float(os.getenv("RR_MIN",       "1.8"))

# =========================
# СЛУЖЕБНЫЙ КОНФИГ
# =========================
SIGNAL_COOLDOWN_SEC       = int(os.getenv("SIGNAL_COOLDOWN_SEC",       "300"))
KEEPALIVE_SEC             = int(os.getenv("KEEPALIVE_SEC",              str(13 * 60)))
WATCHDOG_SEC              = int(os.getenv("WATCHDOG_SEC",               "60"))
STALL_EXIT_SEC            = int(os.getenv("STALL_EXIT_SEC",             "300"))

PRELOAD_BARS_5M  = int(os.getenv("PRELOAD_BARS_5M",  "100"))
PRELOAD_BARS_15M = int(os.getenv("PRELOAD_BARS_15M", "100"))
PRELOAD_WORKERS  = int(os.getenv("PRELOAD_WORKERS",  "10"))

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
Bar = Tuple[float, float, float, float, float]  # open, high, low, close, volume


def calc_atr(data: List[Bar], period: int) -> float:
    if len(data) < period + 1:
        return 0.0
    total = 0.0
    for i in range(len(data) - period, len(data)):
        h, l, pc = data[i][1], data[i][2], data[i - 1][3]
        total += max(h - l, abs(h - pc), abs(pc - l))
    return total / period


def calc_vwap(data: List[Bar], bars: int = VWAP_BARS) -> float:
    window = data[-bars:]
    if not window:
        return 0.0
    pv  = sum(((b[1] + b[2] + b[3]) / 3.0) * b[4] for b in window)
    vol = sum(b[4] for b in window)
    return pv / vol if vol > 0 else 0.0


def ema_series(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return [sum(values[:i+1]) / (i+1) for i in range(len(values))]
    k      = 2.0 / (period + 1.0)
    seed   = sum(values[:period]) / period
    result = [seed]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return [result[0]] * (period - 1) + result


def calc_macd(closes: List[float]) -> Tuple[float, float, float, float]:
    """Возвращает (macd_line, signal, hist_now, hist_prev)."""
    if len(closes) < MACD_SLOW + MACD_SIGNAL + 1:
        return 0.0, 0.0, 0.0, 0.0
    ef   = ema_series(closes, MACD_FAST)
    es   = ema_series(closes, MACD_SLOW)
    ml   = [f - s for f, s in zip(ef, es)]
    sig  = ema_series(ml, MACD_SIGNAL)
    h_now  = ml[-1] - sig[-1]
    h_prev = ml[-2] - sig[-2] if len(ml) >= 2 else h_now
    return ml[-1], sig[-1], h_now, h_prev


def calc_bb(closes: List[float]) -> Tuple[float, float, float, float]:
    """Возвращает (upper, mid, lower, bandwidth)."""
    if len(closes) < BB_PERIOD:
        m = closes[-1] if closes else 0.0
        return m, m, m, 0.0
    w   = closes[-BB_PERIOD:]
    mid = sum(w) / BB_PERIOD
    std = (sum((x - mid) ** 2 for x in w) / BB_PERIOD) ** 0.5
    u   = mid + BB_STDDEV * std
    l   = mid - BB_STDDEV * std
    bw  = (u - l) / mid if mid > 0 else 0.0
    return u, mid, l, bw


def calc_vol_sma(data: List[Bar], period: int = VOL_SMA_PERIOD) -> float:
    """SMA объёма по period барам, не считая последний."""
    if len(data) < period + 1:
        return 0.0
    vols = [b[4] for b in data[-(period + 1):-1]]
    return sum(vols) / len(vols)


def is_bb_squeeze(data: List[Bar]) -> bool:
    """
    Правильная логика BB Squeeze стратегии:

    БЫЛО сжатие недавно (в последних BB_SQUEEZE_BARS+5 барах хотя бы
    один бар находился ниже динамического порога). Текущий бар может
    уже выходить из сжатия — это нормально и даже желательно.

    Старая логика требовала squeeze И breakout одновременно —
    взаимоисключающие условия: когда полосы сжаты, цена внутри них;
    когда цена выбивается за полосы, squeeze уже закончился.

    Правильный порядок: сжатие → выход → пробой.
    Детектируем сжатие в недавней истории (1-(BB_SQUEEZE_BARS+5) баров назад).
    Пробой проверяется отдельно через bb_mid.
    """
    need = BB_PERIOD + max(BB_SQUEEZE_BARS + 8, BB_SQUEEZE_LOOKBACK) + 1
    if len(data) < need:
        return False

    # Собираем bandwidth за BB_SQUEEZE_LOOKBACK баров (baseline)
    lookback_bws: List[float] = []
    for i in range(BB_SQUEEZE_LOOKBACK, 0, -1):
        closes = [b[3] for b in data[:-i]]
        if len(closes) < BB_PERIOD:
            continue
        _, _, _, bw = calc_bb(closes)
        lookback_bws.append(bw)

    if not lookback_bws:
        return False

    lookback_bws.sort()
    idx = max(0, int(len(lookback_bws) * BB_SQUEEZE_PERCENTILE) - 1)
    dynamic_threshold = lookback_bws[idx]

    # True если ХОТЯ БЫ ОДИН из последних (BB_SQUEEZE_BARS+5) баров был в сжатии
    check_window = BB_SQUEEZE_BARS + 5
    for offset in range(1, check_window + 1):
        closes = [b[3] for b in data[:-offset]]
        if len(closes) < BB_PERIOD:
            continue
        _, _, _, bw = calc_bb(closes)
        if bw < dynamic_threshold:
            return True
    return False


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

    async def klines(self, symbol: str, interval: str, limit: int = 100) -> List[Bar]:
        url = (f"{self.base}/v5/market/kline"
               f"?category=linear&symbol={symbol}&interval={interval}&limit={limit}")
        async with self.http.get(url, timeout=15) as r:
            arr = (await r.json()).get("result", {}).get("list", [])
            return [(float(it[1]), float(it[2]), float(it[3]), float(it[4]), float(it[5]))
                    for it in reversed(arr)]


# =========================
# МОДЕЛИ
# =========================
class Market:
    def __init__(self):
        self.symbols: List[str]              = []
        self.state:   Dict[str, SymbolState] = defaultdict(SymbolState)
        self.last_ws_msg_ts      = now_ms()
        self.signal_stats        = {"total": 0, "long": 0, "short": 0}
        self.last_signal_sent_ts = 0
        # Счётчики фильтров — сколько символов отвалилось на каждом шаге
        self.filter_stats: Dict[str, int] = {
            "checked":   0,
            "cooldown":  0,
            "atr":       0,
            "vwap":      0,
            "squeeze":   0,
            "breakout":  0,
            "volume":    0,
            "macd":      0,
            "rr":        0,
            "passed":    0,
        }
        self.filter_stats_reset_ts = now_s()


@dataclass
class SymbolState:
    k5:  List[Bar] = field(default_factory=list)
    k15: List[Bar] = field(default_factory=list)
    # 15m
    vwap_15m:    float = 0.0
    atr_15m:     float = 0.0
    atr_15m_pct: float = 0.0
    trend_bias:  str   = "NONE"
    # 5m
    bb_upper:        float = 0.0
    bb_mid:          float = 0.0
    bb_lower:        float = 0.0
    bb_bw:           float = 0.0
    macd_hist:       float = 0.0
    macd_hist_prev:  float = 0.0
    vol_sma:         float = 0.0
    # служебные
    last_signal_ts: int = 0


# =========================
# ДВИЖОК
# =========================
class ScalpingEngine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def update_context_15m(self, sym: str) -> None:
        st = self.mkt.state[sym]
        if len(st.k15) < ATR_FILTER_PERIOD + 1:
            return
        price           = st.k15[-1][3]
        st.vwap_15m     = calc_vwap(st.k15, VWAP_BARS)
        st.atr_15m      = calc_atr(st.k15, ATR_FILTER_PERIOD)
        st.atr_15m_pct  = st.atr_15m / price if price > 0 else 0.0
        if st.vwap_15m > 0:
            st.trend_bias = "LONG" if price > st.vwap_15m else "SHORT"
        else:
            st.trend_bias = "NONE"

    def update_signals_5m(self, sym: str) -> None:
        st = self.mkt.state[sym]
        min_bars = max(BB_PERIOD, MACD_SLOW + MACD_SIGNAL + 2, VOL_SMA_PERIOD + 1)
        if len(st.k5) < min_bars:
            return
        closes = [b[3] for b in st.k5]
        st.bb_upper, st.bb_mid, st.bb_lower, st.bb_bw = calc_bb(closes)
        _, _, st.macd_hist, st.macd_hist_prev          = calc_macd(closes)
        st.vol_sma = calc_vol_sma(st.k5)

    def generate_signal(self, sym: str) -> Optional[Dict]:
        st  = self.mkt.state[sym]
        mkt = self.mkt
        # Нужно достаточно баров для адаптивного squeeze-окна
        min_5m  = BB_PERIOD + max(BB_SQUEEZE_BARS, BB_SQUEEZE_LOOKBACK) + 1
        min_5m  = max(min_5m, MACD_SLOW + MACD_SIGNAL + 2, VOL_SMA_PERIOD + 1)
        min_15m = ATR_FILTER_PERIOD + 1
        if len(st.k5) < min_5m or len(st.k15) < min_15m:
            return None

        mkt.filter_stats["checked"] += 1

        nowt = now_s()
        if nowt - st.last_signal_ts < SIGNAL_COOLDOWN_SEC:
            mkt.filter_stats["cooldown"] += 1
            return None

        price = st.k5[-1][3]

        # Шаг 1: ATR-фильтр
        if st.atr_15m_pct < ATR_MIN_PCT:
            mkt.filter_stats["atr"] += 1
            return None

        # Шаг 2: тренд по VWAP
        bias = st.trend_bias
        if bias == "NONE":
            mkt.filter_stats["vwap"] += 1
            return None

        # Шаг 3: адаптивный BB Squeeze + пробой
        if not is_bb_squeeze(st.k5):
            mkt.filter_stats["squeeze"] += 1
            return None
        # Пробой: цена в верхней/нижней половине полос Боллинджера.
        # Требовать price > bb_upper после squeeze нельзя — это взаимоисключающие
        # условия (в момент squeeze цена внутри полос, после выхода из squeeze
        # полосы уже расширились и price > upper).
        # bb_mid как порог: цена выше средней = бычий импульс, ниже = медвежий.
        breakout_long  = price > st.bb_mid
        breakout_short = price < st.bb_mid
        if bias == "LONG"  and not breakout_long:
            mkt.filter_stats["breakout"] += 1
            return None
        if bias == "SHORT" and not breakout_short:
            mkt.filter_stats["breakout"] += 1
            return None

        side = bias

        # Шаг 4a: объём × VOL_SPIKE_MULT
        cur_vol   = st.k5[-1][4]
        vol_ratio = cur_vol / st.vol_sma if st.vol_sma > 0 else 0.0
        if st.vol_sma > 0 and vol_ratio < VOL_SPIKE_MULT:
            mkt.filter_stats["volume"] += 1
            return None

        # Шаг 4б: MACD гистограмма совпадает с направлением (рост не обязателен)
        if side == "LONG":
            if not (st.macd_hist > 0):
                mkt.filter_stats["macd"] += 1
                return None
        else:
            if not (st.macd_hist < 0):
                mkt.filter_stats["macd"] += 1
                return None

        # Шаг 5: TP / SL
        local_low  = min(b[2] for b in st.k5[-10:])
        local_high = max(b[1] for b in st.k5[-10:])

        if side == "LONG":
            sl = max(st.bb_lower * (1 - SL_BB_BUFFER), local_low * (1 - SL_BB_BUFFER))
        else:
            sl = min(st.bb_upper * (1 + SL_BB_BUFFER), local_high * (1 + SL_BB_BUFFER))

        tp = price * (1 + TP_MIN_PCT) if side == "LONG" else price * (1 - TP_MIN_PCT)

        tp_pct = abs(tp - price) / price
        if tp_pct > TP_MAX_PCT:
            tp = price * (1 + TP_MAX_PCT) if side == "LONG" else price * (1 - TP_MAX_PCT)

        sl_dist = abs(price - sl)
        if sl_dist < 1e-9:
            return None
        rr = abs(tp - price) / sl_dist

        # Если RR не хватает — расширяем TP до MAX
        if rr < RR_MIN:
            tp = price * (1 + TP_MAX_PCT) if side == "LONG" else price * (1 - TP_MAX_PCT)
            rr = abs(tp - price) / sl_dist
            if rr < RR_MIN:
                self.mkt.filter_stats["rr"] += 1
                return None

        tp_pct_out = abs(tp - price) / price * 100.0
        sl_pct_out = abs(sl - price) / price * 100.0

        st.last_signal_ts = nowt
        self.mkt.signal_stats["total"] += 1
        self.mkt.signal_stats[side.lower()] += 1
        self.mkt.filter_stats["passed"] += 1

        return {
            "symbol":    sym,
            "side":      side,
            "price":     price,
            "tp":        tp,
            "sl":        sl,
            "tp_pct":    tp_pct_out,
            "sl_pct":    sl_pct_out,
            "rr":        rr,
            "vol_ratio": vol_ratio,
            "vwap_15m":  st.vwap_15m,
            "atr_pct":   st.atr_15m_pct * 100,
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
            # Нужно больше баров для адаптивного squeeze-окна
            bars5_limit  = max(PRELOAD_BARS_5M,  BB_PERIOD + BB_SQUEEZE_LOOKBACK + 10)
            bars5  = await rest.klines(sym, TF_SCALP, limit=bars5_limit)
            bars15 = await rest.klines(sym, TF_TREND, limit=PRELOAD_BARS_15M)
            st = mkt.state[sym]
            if bars5:
                st.k5 = list(bars5[-200:])
                engine.update_signals_5m(sym)
            if bars15:
                st.k15 = list(bars15[-200:])
                engine.update_context_15m(sym)
        except Exception as e:
            logger.warning(f"Preload failed {sym}: {e}")


async def preload_all(rest: BybitRest, mkt: Market, engine: ScalpingEngine) -> int:
    sem   = asyncio.Semaphore(PRELOAD_WORKERS)
    tasks = [preload_symbol(rest, mkt, engine, sym, sem) for sym in mkt.symbols]
    logger.info(f"Preloading {len(tasks)} symbols (5m + 15m)...")
    await asyncio.gather(*tasks)
    min_5m = BB_PERIOD + max(BB_SQUEEZE_BARS, BB_SQUEEZE_LOOKBACK) + 1
    ready  = sum(1 for s in mkt.symbols
                 if len(mkt.state[s].k5) >= min_5m and len(mkt.state[s].k15) >= ATR_FILTER_PERIOD + 1)
    logger.info(f"Preload done: {ready}/{len(mkt.symbols)} ready")
    return ready


# =========================
# UNIVERSE (топ-50 + ATR)
# =========================
async def build_universe(rest: BybitRest, mkt: Market, engine: ScalpingEngine) -> List[str]:
    try:
        tickers = await rest.tickers_linear()
        usdt    = [t for t in tickers if t["symbol"].endswith("USDT")]
        usdt.sort(key=lambda t: float(t.get("turnover24h", 0)), reverse=True)
        top     = [t["symbol"] for t in usdt[:TOP_SYMBOLS_COUNT * 2]]
        candidates = list(dict.fromkeys(CORE_SYMBOLS + top))[:TOP_SYMBOLS_COUNT * 2]

        sem = asyncio.Semaphore(PRELOAD_WORKERS)

        async def fetch_atr(sym: str) -> Tuple[str, float]:
            async with sem:
                try:
                    st = mkt.state[sym]
                    if len(st.k15) < ATR_FILTER_PERIOD + 1:
                        bars = await rest.klines(sym, TF_TREND, limit=ATR_FILTER_PERIOD + 5)
                        if bars:
                            st.k15 = list(bars)
                            engine.update_context_15m(sym)
                    return sym, st.atr_15m_pct
                except Exception:
                    return sym, 0.0

        results = await asyncio.gather(*[fetch_atr(s) for s in candidates])
        passed  = [sym for sym, atr_pct in results if atr_pct >= ATR_MIN_PCT]
        final   = list(dict.fromkeys(
            [s for s in CORE_SYMBOLS if s in candidates] + passed
        ))[:TOP_SYMBOLS_COUNT]

        logger.info(f"Universe: {len(candidates)} candidates → {len(final)} passed ATR filter")
        return final
    except Exception:
        logger.exception("build_universe error")
        return list(CORE_SYMBOLS)


# =========================
# WS ОБРАБОТЧИК
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
    parts = topic.split(".")
    if len(parts) < 3:
        return
    tf, symbol = parts[1], parts[2]
    st = mkt.state[symbol]

    for p in payload:
        bar = (float(p["open"]), float(p["high"]), float(p["low"]),
               float(p["close"]), float(p["volume"]))
        buf = st.k5 if tf == TF_SCALP else st.k15 if tf == TF_TREND else None
        if buf is None:
            continue

        if p.get("confirm") is False and buf:
            buf[-1] = bar
        else:
            buf.append(bar)
            if len(buf) > 200:
                buf.pop(0)
            if p.get("confirm") is True:
                if tf == TF_SCALP:
                    app["engine"].update_signals_5m(symbol)
                    if symbol in mkt.symbols:
                        sig = app["engine"].generate_signal(symbol)
                        if sig:
                            asyncio.create_task(send_signal(app, sig))
                elif tf == TF_TREND:
                    app["engine"].update_context_15m(symbol)


# =========================
# ОТПРАВКА СИГНАЛА
# =========================
async def send_signal(app: web.Application, sig: Dict) -> None:
    side_emoji = "🟢" if sig["side"] == "LONG" else "🔴"
    side_label = "LONG ALERT" if sig["side"] == "LONG" else "SHORT ALERT"
    sym_pretty = sig["symbol"].replace("USDT", "/USDT")
    ts_utc     = datetime.now(timezone.utc).strftime("%H:%M UTC")
    sl_label   = "нижней BB" if sig["side"] == "LONG" else "верхней BB"
    vwap_dir   = "выше" if sig["side"] == "LONG" else "ниже"

    text = (
        f"{side_emoji} <b>{side_label} | {sym_pretty}</b>\n\n"
        f"📍 Цена сейчас:  <code>{sig['price']:.5f}</code>\n"
        f"🎯 Take Profit:  <code>{sig['tp']:.5f}</code>  <i>(+{sig['tp_pct']:.2f}%)</i>\n"
        f"🛡 Stop Loss:    <code>{sig['sl']:.5f}</code>  <i>(-{sig['sl_pct']:.2f}%)</i>"
        f" — за {sl_label}\n"
        f"⚖️ RR:           {sig['rr']:.2f}\n"
        f"⏰ Сигнал:      {ts_utc}\n\n"
        f"📊 <b>Основание:</b> BB Squeeze → Пробой на 5m"
        f" + Объём ×{sig['vol_ratio']:.1f}."
        f" Цена {vwap_dir} 15m VWAP (<code>{sig['vwap_15m']:.5f}</code>)."
        f" ATR(15m): {sig['atr_pct']:.2f}%."
    )

    targets = PRIMARY_RECIPIENTS if ONLY_CHANNEL else ALLOWED_CHAT_IDS
    for cid in targets:
        await app["tg"].send(cid, text)
    app["mkt"].last_signal_sent_ts = now_ms()
    logger.info(
        f"Signal: {sig['side']} {sig['symbol']} "
        f"entry={sig['price']:.5f} tp=+{sig['tp_pct']:.2f}% "
        f"sl=-{sig['sl_pct']:.2f}% rr={sig['rr']:.2f} vol×{sig['vol_ratio']:.1f}"
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
                    min_5    = BB_PERIOD + max(BB_SQUEEZE_BARS, BB_SQUEEZE_LOOKBACK) + 1
                    ready    = sum(1 for s in mkt.symbols
                                   if len(mkt.state[s].k5) >= min_5
                                   and len(mkt.state[s].k15) >= ATR_FILTER_PERIOD + 1)
                    volatile = sum(1 for s in mkt.symbols if mkt.state[s].atr_15m_pct >= ATR_MIN_PCT)
                    last_ago = (
                        f"{int((now_ms() - mkt.last_signal_sent_ts) / 1000)}s назад"
                        if mkt.last_signal_sent_ts else "ещё не было"
                    )
                    await tg.send(cid, (
                        f"✅ <b>Cryptobot SCALP v17</b>\n"
                        f"Стратегия: BB Squeeze (адапт.) + VWAP(15m) + MACD + Vol×{VOL_SPIKE_MULT}\n"
                        f"Squeeze: перцентиль {int(BB_SQUEEZE_PERCENTILE*100)}% / окно {BB_SQUEEZE_LOOKBACK} баров\n"
                        f"Пул: {len(mkt.symbols)} монет ({volatile} волатильных)\n"
                        f"Готовы: {ready}/{len(mkt.symbols)}\n"
                        f"TP мин: {TP_MIN_PCT*100:.1f}%  |  RR ≥ {RR_MIN}\n"
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
        mkt: Market = app["mkt"]
        fs = mkt.filter_stats
        elapsed = max(1, now_s() - mkt.filter_stats_reset_ts)
        logger.info(
            f"Keepalive: WS {(now_ms() - mkt.last_ws_msg_ts) / 1000:.1f}s ago | "
            f"Universe: {len(mkt.symbols)} syms | "
            f"Signals total: {mkt.signal_stats['total']} "
            f"(L:{mkt.signal_stats['long']} S:{mkt.signal_stats['short']})"
        )
        logger.info(
            f"Filter stats (last {elapsed//60}min): "
            f"checked={fs['checked']} "
            f"cooldown={fs['cooldown']} "
            f"atr={fs['atr']} "
            f"vwap={fs['vwap']} "
            f"squeeze={fs['squeeze']} "
            f"breakout={fs['breakout']} "
            f"volume={fs['volume']} "
            f"macd={fs['macd']} "
            f"rr={fs['rr']} "
            f"→passed={fs['passed']}"
        )
        # Сброс счётчиков
        for k in fs:
            fs[k] = 0
        mkt.filter_stats_reset_ts = now_s()


async def universe_refresh_loop(app: web.Application) -> None:
    while True:
        await asyncio.sleep(UNIVERSE_REFRESH_SEC)
        try:
            new_syms = await build_universe(app["rest"], app["mkt"], app["engine"])
            old_set  = set(app["mkt"].symbols)
            to_add   = [s for s in new_syms if s not in old_set]
            if to_add:
                sem = asyncio.Semaphore(PRELOAD_WORKERS)
                await asyncio.gather(*[
                    preload_symbol(app["rest"], app["mkt"], app["engine"], sym, sem)
                    for sym in to_add
                ])
                topics = ([f"kline.{TF_SCALP}.{s}" for s in to_add] +
                          [f"kline.{TF_TREND}.{s}"  for s in to_add])
                await app["ws"].subscribe(topics)
            app["mkt"].symbols = new_syms
            logger.info(f"Universe refreshed: {len(new_syms)} symbols ({len(to_add)} new)")
        except Exception:
            logger.exception("Universe refresh error")


# =========================
# СТАРТ
# =========================
async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    http        = aiohttp.ClientSession()
    app["http"] = http
    app["tg"]   = Tg(TELEGRAM_TOKEN, http)

    await app["tg"].delete_webhook(drop_pending_updates=True)
    logger.info("🚀 Starting Cryptobot SCALP v17")

    app["rest"]   = BybitRest(BYBIT_REST, http)
    app["mkt"]    = Market()
    app["engine"] = ScalpingEngine(app["mkt"])
    app["ws"]     = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: ws_on_message(app, data)

    # 1. Пул с ATR-фильтром
    app["mkt"].symbols = await build_universe(app["rest"], app["mkt"], app["engine"])

    # 2. Предзагрузка 5m + 15m
    ready = await preload_all(app["rest"], app["mkt"], app["engine"])

    # 3. WS подписка на оба TF
    topics = ([f"kline.{TF_SCALP}.{s}" for s in app["mkt"].symbols] +
              [f"kline.{TF_TREND}.{s}"  for s in app["mkt"].symbols])
    await app["ws"].connect()
    await app["ws"].subscribe(topics)

    # 4. Фоновые задачи
    app["ws_task"]        = asyncio.create_task(app["ws"].run())
    app["tg_task"]        = asyncio.create_task(tg_loop(app))
    app["watchdog_task"]  = asyncio.create_task(watchdog_loop(app))
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["universe_task"]  = asyncio.create_task(universe_refresh_loop(app))

    # 5. Уведомление о старте
    try:
        volatile = sum(1 for s in app["mkt"].symbols
                       if app["mkt"].state[s].atr_15m_pct >= ATR_MIN_PCT)
        targets = PRIMARY_RECIPIENTS if PRIMARY_RECIPIENTS else (ALLOWED_CHAT_IDS[:1] if ALLOWED_CHAT_IDS else [])
        for chat_id in targets:
            await app["tg"].send(chat_id, (
                "🟢 <b>Cryptobot SCALP v17 Online</b>\n\n"
                "<b>Стратегия (5 шагов):</b>\n"
                f"  1️⃣ Топ-{TOP_SYMBOLS_COUNT} по объёму + ATR(15m) ≥ {ATR_MIN_PCT*100:.1f}%\n"
                "  2️⃣ Тренд по VWAP(15m)\n"
                f"  3️⃣ BB Squeeze (адапт. {int(BB_SQUEEZE_PERCENTILE*100)}%‑перцентиль"
                f" / {BB_SQUEEZE_LOOKBACK} баров) → Пробой на 5m\n"
                f"  4️⃣ Объём ×{VOL_SPIKE_MULT} + MACD растёт\n"
                f"  5️⃣ TP ≥ {TP_MIN_PCT*100:.0f}%  |  RR ≥ {RR_MIN}\n\n"
                f"• Пул: {len(app['mkt'].symbols)} монет ({volatile} волатильных)\n"
                f"• Готовы к анализу: {ready}\n"
                "• История 5m + 15m: предзагружена ✅\n"
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
