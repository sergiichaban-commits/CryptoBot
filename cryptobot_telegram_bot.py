#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CheCryptoSignalsBot — комбинированные сигналы (SMC + Volume + OI + Liquidations)
- PTB 21.6 (polling)
- Bybit Linear USDT-Perp универсум с ротацией
- Скрининг каждые ~30с активного окна, фильтры: Prob>=69.9, RR>=2.0, Profit>=1%
- Health "online" каждые 20 минут (по умолч.)
"""

import os
import asyncio
import json
import math
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Any
import logging
from datetime import datetime, timezone, timedelta

import httpx
import pandas as pd
import numpy as np
from aiohttp import web

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ------------------------ ЛОГИ ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ------------------------ КОНФИГ ------------------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return v.strip() if isinstance(v, str) else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
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
    return v.lower() in ("1", "true", "yes", "y", "on")


def _env_json_list(name: str, default: List[int]) -> List[int]:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        # допускаем формат "1,2,3" или JSON "[1,2,3]"
        if raw.strip().startswith("["):
            return json.loads(raw)
        return [int(x.strip()) for x in raw.split(",") if x.strip()]
    except Exception:
        return default


@dataclass
class Config:
    token: str
    allowed_chat_ids: List[int]
    primary_recipients: List[int]
    public_url: str = ""
    http_port: int = 10000
    # ротация/вселенная
    universe_top_n: int = 30          # одновременно активных символов
    rotate_min: int = 5               # раз в N минут менять окно
    ws_symbols_max: int = 60          # лимит «активности» (для будущих WS)
    universe_mode: str = "all"        # all — все USDT-перпы
    # расписания
    scan_sec: int = 30                # частота скрининга активного окна
    poll_oi_sec: int = 60             # частота опроса OI
    health_sec: int = 20 * 60         # "online" каждые 20 минут
    first_health_sec: int = 60
    self_ping_sec: int = 13 * 60      # мягкое пробуждение
    # триггеры и фильтры
    vol_mult: float = 2.0
    vol_sma_period: int = 20
    atr_period: int = 14
    body_atr_mult: float = 0.60
    prob_min_pct: float = 69.9
    rr_min: float = 2.0
    profit_min_pct: float = 1.0
    signal_cooldown_sec: int = 600
    signal_ttl_min: int = 12
    # фрейм анализа
    tf: str = "5"                     # 5m
    klines_limit: int = 300

    @staticmethod
    def load() -> "Config":
        token = _env_str("TELEGRAM_BOT_TOKEN")
        allowed = _env_json_list("ALLOWED_CHAT_IDS", [])
        primary = []
        chat_raw = _env_str("TELEGRAM_CHAT_ID", "")
        if chat_raw:
            try:
                primary = [int(chat_raw)]
            except Exception:
                pass
        public_url = _env_str("PUBLIC_URL", "")
        http_port = _env_int("PORT", 10000)

        cfg = Config(
            token=token,
            allowed_chat_ids=allowed,
            primary_recipients=primary or [x for x in allowed if x < 0][:1],
            public_url=public_url,
            http_port=http_port,
            universe_top_n=_env_int("UNIVERSE_TOP_N", 30),
            rotate_min=_env_int("ROTATE_MIN", 5),
            ws_symbols_max=_env_int("WS_SYMBOLS_MAX", 60),
            scan_sec=_env_int("SCAN_SEC", 30),
            poll_oi_sec=_env_int("POLL_OI_SEC", 60),
            health_sec=_env_int("HEALTH_SEC", 1200),
            first_health_sec=_env_int("FIRST_HEALTH_SEC", 60),
            self_ping_sec=_env_int("SELF_PING_SEC", 780),
            vol_mult=_env_float("VOL_MULT", 2.0),
            vol_sma_period=_env_int("VOL_SMA_PERIOD", 20),
            atr_period=_env_int("ATR_PERIOD", 14),
            body_atr_mult=_env_float("BODY_ATR_MULT", 0.60),
            prob_min_pct=_env_float("PROB_MIN_PCT", 69.9),
            rr_min=_env_float("RR_MIN", 2.0),
            profit_min_pct=_env_float("PROFIT_MIN_PCT", 1.0),
            signal_cooldown_sec=_env_int("SIGNAL_COOLDOWN_SEC", 600),
            signal_ttl_min=_env_int("SIGNAL_TTL_MIN", 12),
            tf=_env_str("TF", "5"),
            klines_limit=_env_int("KLINES_LIMIT", 300),
            universe_mode=_env_str("UNIVERSE_MODE", "all"),
        )
        log.info(
            "INFO [cfg] ALLOWED_CHAT_IDS=%s", cfg.allowed_chat_ids
        )
        log.info(
            "INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.primary_recipients
        )
        log.info(
            "INFO [cfg] PUBLIC_URL=%r PORT=%d (polling mode, HTTP for health only)",
            cfg.public_url, cfg.http_port
        )
        log.info(
            "INFO [cfg] HEALTH=%ss FIRST=%ss SELF_PING=True/%ss",
            cfg.health_sec, cfg.first_health_sec, cfg.self_ping_sec
        )
        log.info(
            "INFO [cfg] SIGNAL_COOLDOWN_SEC=%d SIGNAL_TTL_MIN=%d "
            "UNIVERSE_MODE=%s UNIVERSE_TOP_N=%d WS_SYMBOLS_MAX=%d ROTATE_MIN=%d "
            "PROB_MIN>%.1f PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
            cfg.signal_cooldown_sec, cfg.signal_ttl_min,
            cfg.universe_mode, cfg.universe_top_n, cfg.ws_symbols_max, cfg.rotate_min,
            cfg.prob_min_pct, cfg.profit_min_pct, cfg.rr_min
        )
        log.info(
            "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
            cfg.vol_mult, cfg.vol_sma_period, cfg.body_atr_mult, cfg.atr_period
        )
        return cfg


# ------------------------ BYBIT CLIENT ------------------------
BYBIT_HOST = "https://api.bybit.com"

class BybitClient:
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=15.0, headers={"User-Agent": "CheCryptoSignalsBot/1.0"})
        # кэш OI: symbol -> list[(ts_ms, oi_value)]
        self.oi_cache: Dict[str, List[Tuple[int, float]]] = {}
        # кэш ликвидаций: symbol -> list[(ts_ms, side, qtyUsd)]
        self.liq_cache: Dict[str, List[Tuple[int, str, float]]] = {}

    async def close(self):
        await self.http.aclose()

    async def get_linear_symbols(self) -> List[str]:
        """
        Все линейные USDT-perp символы (category=linear)
        """
        url = f"{BYBIT_HOST}/v5/market/instruments-info"
        params = {"category": "linear"}
        r = await self.http.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        result = []
        for it in data.get("result", {}).get("list", []):
            if it.get("quoteCoin") == "USDT" and it.get("status") == "Trading":
                result.append(it.get("symbol"))
        return sorted(list(set(result)))

    async def get_tickers(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        24h статистика (для ранжирования ликвидности/волатильности).
        """
        out = {}
        # В v5 у тикеров есть лимит по множеству; делаем чанками
        BATCH = 50
        for i in range(0, len(symbols), BATCH):
            sub = symbols[i : i + BATCH]
            url = f"{BYBIT_HOST}/v5/market/tickers"
            params = {"category": "linear", "symbol": ",".join(sub)}
            r = await self.http.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            for it in data.get("result", {}).get("list", []):
                sym = it.get("symbol")
                out[sym] = it
        return out

    async def get_klines(self, symbol: str, interval: str = "5", limit: int = 300) -> pd.DataFrame:
        """
        Свечи v5: category=linear, interval как строка (1,3,5,15,30,60,240,1440)
        Возвращает DataFrame с колонками: ts, open, high, low, close, volume, turnover
        """
        url = f"{BYBIT_HOST}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        r = await self.http.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        arr = data.get("result", {}).get("list", [])
        # Bybit возвращает в обратном порядке (новые первые), перевернём
        arr = list(reversed(arr))
        rows = []
        for it in arr:
            # [start, open, high, low, close, volume, turnover]
            ts = int(it[0])
            rows.append({
                "ts": ts,
                "open": float(it[1]),
                "high": float(it[2]),
                "low": float(it[3]),
                "close": float(it[4]),
                "volume": float(it[5]),
                "turnover": float(it[6]) if len(it) > 6 and it[6] is not None else np.nan,
            })
        df = pd.DataFrame(rows)
        return df

    async def get_open_interest(self, symbol: str, interval: str = "5min", limit: int = 4) -> List[Tuple[int, float]]:
        """
        OI v5: /v5/market/open-interest
        interval: "5min","15min","30min","1h","4h","1d"
        """
        url = f"{BYBIT_HOST}/v5/market/open-interest"
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval, "limit": limit}
        r = await self.http.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        lst = data.get("result", {}).get("list", [])
        out = []
        # формат: [{"openInterest": "...", "timestamp": "..."}...]
        for it in lst:
            ts = int(it.get("timestamp"))
            oi = float(it.get("openInterest"))
            out.append((ts, oi))
        return out

    def update_oi_cache(self, symbol: str, points: List[Tuple[int, float]]):
        if not points:
            return
        cur = self.oi_cache.setdefault(symbol, [])
        # слияние по ts
        seen = {ts for ts, _ in cur}
        for ts, oi in points:
            if ts not in seen:
                cur.append((ts, oi))
        # поддерживать последние 24 записи
        cur.sort(key=lambda x: x[0])
        if len(cur) > 48:
            self.oi_cache[symbol] = cur[-48:]

    # ЗАГЛУШКА для ликвидаций (мягкий фактор):
    def push_liq(self, symbol: str, ts_ms: int, side: str, qty_usd: float):
        cur = self.liq_cache.setdefault(symbol, [])
        cur.append((ts_ms, side, float(qty_usd)))
        # последние ~2 часа
        cutoff = int(time.time() * 1000) - 2 * 60 * 60 * 1000
        self.liq_cache[symbol] = [x for x in cur if x[0] >= cutoff]

    def liq_stats(self, symbol: str, minutes: int = 30) -> Tuple[float, float]:
        """
        Возвращает (sum_long_usd, sum_short_usd) за последние minutes.
        Если данных нет — (0,0).
        """
        cur = self.liq_cache.get(symbol, [])
        if not cur:
            return (0.0, 0.0)
        cutoff = int(time.time() * 1000) - minutes * 60 * 1000
        lon = 0.0
        sho = 0.0
        for ts, side, usd in cur:
            if ts >= cutoff:
                if side.lower().startswith("buy"):   # по конвенции биржи бывают «buy/sell» события
                    # buy liquidation => ликвидация шортов
                    sho += usd
                else:
                    # sell liquidation => ликвидация лонгов
                    lon += usd
        return (lon, sho)


# ------------------------ ИНДИКАТОРЫ/АНАЛИЗ ------------------------
def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    atr_val = tr.ewm(alpha=1/period, adjust=False).mean()
    return atr_val


def swing_points(df: pd.DataFrame, left: int = 2, right: int = 2) -> Tuple[List[int], List[int]]:
    """
    Возвращает индексы pivot High/Low (SMC-база на «свингах»).
    """
    highs = []
    lows = []
    for i in range(left, len(df) - right):
        h = df["high"].iloc[i]
        l = df["low"].iloc[i]
        if all(h > df["high"].iloc[i - k - 1] for k in range(left)) and all(h > df["high"].iloc[i + k + 1] for k in range(right)):
            highs.append(i)
        if all(l < df["low"].iloc[i - k - 1] for k in range(left)) and all(l < df["low"].iloc[i + k + 1] for k in range(right)):
            lows.append(i)
    return highs, lows


@dataclass
class SMCSignal:
    side: str              # "long"|"short"
    kind: str              # "breakout"|"sweep"
    level: float           # уровень структуры (пробой/снос ликвидности)
    ref_idx: int           # индекс свечи-референса


def smc_detect(df: pd.DataFrame, atr_series: pd.Series, body_atr_mult: float = 0.6) -> Optional[SMCSignal]:
    """
    Примитивный SMC:
    - Breakout UP: закрытие выше последнего swing high, тело >= 0.6*ATR
    - Breakout DOWN: закрытие ниже последнего swing low, тело >= 0.6*ATR
    - Sweep: прокол экстремума хвостом и возврат закрытием внутрь диапазона (сильная тень)
    Приоритет: breakout над sweep.
    """
    if len(df) < 50:
        return None
    highs_idx, lows_idx = swing_points(df, 2, 2)
    if not highs_idx or not lows_idx:
        return None

    last_high_i = highs_idx[-1]
    last_low_i = lows_idx[-1]
    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    i = len(df) - 1  # текущая
    body = abs(close[i] - open_[i])
    atr_now = float(atr_series.iloc[i])

    # Breakouts
    swing_high = float(df["high"].iloc[last_high_i])
    swing_low = float(df["low"].iloc[last_low_i])

    if close[i] > swing_high and body >= body_atr_mult * atr_now:
        return SMCSignal(side="long", kind="breakout", level=swing_high, ref_idx=last_high_i)

    if close[i] < swing_low and body >= body_atr_mult * atr_now:
        return SMCSignal(side="short", kind="breakout", level=swing_low, ref_idx=last_low_i)

    # Sweeps
    # long sweep: хвостом выше swing_high, но закрытие вернулось ниже уровня
    if high[i] > swing_high and close[i] < swing_high and (high[i] - max(open_[i], close[i])) >= body_atr_mult * atr_now:
        return SMCSignal(side="short", kind="sweep", level=swing_high, ref_idx=last_high_i)

    # short sweep: хвостом ниже swing_low, но закрытие вернулось выше уровня
    if low[i] < swing_low and close[i] > swing_low and (min(open_[i], close[i]) - low[i]) >= body_atr_mult * atr_now:
        return SMCSignal(side="long", kind="sweep", level=swing_low, ref_idx=last_low_i)

    return None


def volume_score(df: pd.DataFrame, vol_sma_period: int, vol_mult: float) -> float:
    v = df["volume"].astype(float)
    sma = v.rolling(vol_sma_period).mean()
    if len(v) < vol_sma_period + 2:
        return 0.5
    last = float(v.iloc[-1])
    base = float(sma.iloc[-1]) if not math.isnan(sma.iloc[-1]) and sma.iloc[-1] > 0 else 1.0
    ratio = last / base
    # 1.0 балл при ratio>=vol_mult, далее плавно от 0.5...
    if ratio >= vol_mult:
        return 1.0
    return 0.5 + 0.5 * min(1.0, (ratio / vol_mult))


def oi_score(symbol: str, side: str, client: BybitClient) -> float:
    """
    Пытаемся согласовать OI с направлением:
    - для long: если OI растёт за последние 3 точки → лучше (0.8..1.0), падает → хуже (0.3..0.5)
    - для short: аналогично, но желательно растущий OI на снижении цены — мы цены тут не смотрим, т.к. оцениваем моментум OI.
    """
    arr = client.oi_cache.get(symbol, [])
    if len(arr) < 3:
        return 0.5
    last3 = arr[-3:]
    vals = [x[1] for x in last3]
    delta = vals[-1] - vals[0]
    # простой градиент
    if delta > 0:
        return 0.9 if side == "long" else 0.8
    elif delta < 0:
        return 0.8 if side == "short" else 0.4
    return 0.6


def liq_score(symbol: str, side: str, client: BybitClient) -> float:
    """
    Если были крупные ликвидации противоположной стороны за 30 мин → лайтовый бонус.
    """
    long_liq_usd, short_liq_usd = client.liq_stats(symbol, minutes=30)
    total = long_liq_usd + short_liq_usd
    if total <= 0:
        return 0.5
    share_opposite = 0.0
    if side == "long":
        # хотим видеть ликвидации лонгов (сильные sell-liq) — как контртрендовый сигнал
        share_opposite = long_liq_usd / total
    else:
        share_opposite = short_liq_usd / total
    # 0.5..0.9
    return 0.5 + 0.4 * share_opposite


@dataclass
class TradePlan:
    symbol: str
    side: str               # long|short
    price: float
    entry: float
    sl: float
    tp: float
    rr: float
    profit_pct: float
    prob_pct: float
    reason: str


def build_trade_plan(symbol: str, side: str, df: pd.DataFrame, atr_series: pd.Series, smc: SMCSignal,
                     cfg: Config) -> Optional[TradePlan]:
    """
    Простейший money-management:
    - entry = текущая цена (market) [можно заменить на ретест уровня позже]
    - SL: за противоположный экстремум или ATR
    - TP: не меньше 2R и не меньше min profit %
    """
    close = float(df["close"].iloc[-1])
    a = float(atr_series.iloc[-1])

    # SL исходя из smc.ref уровня
    if side == "long":
        sl = min(float(df["low"].iloc[smc.ref_idx]), close - 1.0 * a)
        # защитимся от «слишком близкого sl»
        if sl >= close:
            sl = close * 0.995
        dist = close - sl
        tp_min = close + max(cfg.rr_min * dist, close * (cfg.profit_min_pct / 100.0))
        tp = tp_min
        rr = (tp - close) / (close - sl) if (close - sl) > 0 else 0.0
        profit_pct = (tp - close) / close * 100.0
        entry = close
    else:
        sl = max(float(df["high"].iloc[smc.ref_idx]), close + 1.0 * a)
        if sl <= close:
            sl = close * 1.005
        dist = sl - close
        tp_min = close - max(cfg.rr_min * dist, close * (cfg.profit_min_pct / 100.0))
        tp = tp_min
        rr = (close - tp) / (sl - close) if (sl - close) > 0 else 0.0
        profit_pct = (close - tp) / close * 100.0
        entry = close

    if profit_pct < cfg.profit_min_pct or rr < cfg.rr_min:
        return None

    return TradePlan(
        symbol=symbol, side=side, price=close, entry=entry, sl=sl, tp=tp,
        rr=rr, profit_pct=profit_pct, prob_pct=0.0, reason=""
    )


def combined_probability(smc: SMCSignal, vol_sc: float, oi_sc: float, liq_sc: float) -> float:
    """
    Весовая модель:
    - SMC 50%
    - Volume 20%
    - OI 20%
    - Liquidations 10% (если 0.5 -> нейтрально)
    Breakout чуть сильнее Sweep.
    """
    smc_base = 0.85 if smc.kind == "breakout" else 0.70
    score = 0.50 * smc_base + 0.20 * vol_sc + 0.20 * oi_sc + 0.10 * liq_sc
    return max(0.0, min(1.0, score)) * 100.0


# ------------------------ СОСТОЯНИЕ/ДВИЖОК ------------------------
@dataclass
class Universe:
    total: List[str] = field(default_factory=list)
    active: List[str] = field(default_factory=list)
    batch_index: int = 0
    last_rotate_ts: float = 0.0


class Engine:
    def __init__(self, cfg: Config, client: BybitClient):
        self.cfg = cfg
        self.client = client
        self.uni = Universe()
        self.last_signal_ts: Dict[str, float] = {}  # cooldown
        self.ready = False

    async def bootstrap_universe(self):
        """
        Загружаем вселенную и формируем первый активный «срез».
        """
        syms = await self.client.get_linear_symbols()
        self.uni.total = syms
        self._rebuild_active(initial=True)
        self.ready = True
        log.info("INFO [universe] total=%d active=%d mode=%s", len(self.uni.total), len(self.uni.active), self.cfg.universe_mode)

    def _rebuild_active(self, initial: bool = False):
        N = min(self.cfg.universe_top_n, len(self.uni.total))
        if N <= 0:
            self.uni.active = []
            return
        # Ротация по batch_index (карусель)
        start = (self.uni.batch_index * N) % max(1, len(self.uni.total))
        window = []
        i = 0
        while len(window) < N and i < len(self.uni.total):
            window.append(self.uni.total[(start + i) % len(self.uni.total)])
            i += 1
        self.uni.active = window
        if not initial:
            self.uni.batch_index += 1
        self.uni.last_rotate_ts = time.time()

    async def rotate_active(self):
        if not self.uni.total:
            return
        self._rebuild_active(initial=False)
        log.info("INFO [rotate] batch#%d active=%d", self.uni.batch_index, len(self.uni.active))

    async def poll_oi(self):
        """
        периодический опрос OI по активным символам
        """
        if not self.uni.active:
            return
        for sym in self.uni.active:
            try:
                pts = await self.client.get_open_interest(sym, interval="5min", limit=6)
                self.client.update_oi_cache(sym, pts)
            except Exception as e:
                log.debug("OI error %s: %s", sym, e)

    def _cooldown_ok(self, symbol: str) -> bool:
        ts = self.last_signal_ts.get(symbol)
        if ts is None:
            return True
        return (time.time() - ts) >= self.cfg.signal_cooldown_sec

    def _mark_signal(self, symbol: str):
        self.last_signal_ts[symbol] = time.time()

    async def scan_once(self) -> List[TradePlan]:
        """
        Скрининг активного окна: собираем свечи, считаем SMC+объём+ATR+OI(+liq),
        строим планы и фильтруем.
        """
        results: List[TradePlan] = []
        if not self.uni.active:
            return results
        for sym in self.uni.active:
            if not self._cooldown_ok(sym):
                continue
            try:
                df = await self.client.get_klines(sym, interval=self.cfg.tf, limit=self.cfg.klines_limit)
                if df.empty or len(df) < max(60, self.cfg.vol_sma_period + 5, self.cfg.atr_period + 5):
                    continue
                atr_s = atr(df, self.cfg.atr_period)
                smc = smc_detect(df, atr_s, self.cfg.body_atr_mult)
                if not smc:
                    continue

                vol_sc = volume_score(df, self.cfg.vol_sma_period, self.cfg.vol_mult)
                oi_sc = oi_score(sym, smc.side, self.client)
                liq_sc = liq_score(sym, smc.side, self.client)

                plan = build_trade_plan(sym, smc.side, df, atr_s, smc, self.cfg)
                if not plan:
                    continue

                # вероятность
                prob = combined_probability(smc, vol_sc, oi_sc, liq_sc)
                plan.prob_pct = prob
                plan.reason = f"SMC={smc.kind}, Vol={vol_sc:.2f}, OI={oi_sc:.2f}, Liq={liq_sc:.2f}"

                # фильтры
                if plan.prob_pct < self.cfg.prob_min_pct:
                    continue
                if plan.rr < self.cfg.rr_min:
                    continue
                if plan.profit_pct < self.cfg.profit_min_pct:
                    continue

                results.append(plan)

            except Exception as e:
                log.debug("scan %s error: %s", sym, e)
                continue

        # сортировка по вероятности (desc)
        results.sort(key=lambda x: x.prob_pct, reverse=True)
        return results


# ------------------------ ТЕЛЕГРАМ ХЭНДЛЕРЫ ------------------------
def _guard_user(update: Update, cfg: Config) -> bool:
    uid = None
    if update.effective_chat:
        uid = update.effective_chat.id
    if uid is None:
        return False
    return (uid in cfg.allowed_chat_ids) or (len(cfg.allowed_chat_ids) == 0)


async def cmd_ping(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _guard_user(update, ctx.bot_data["cfg"]):
        return
    await update.effective_chat.send_message("pong")


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _guard_user(update, ctx.bot_data["cfg"]):
        return
    eng: Engine = ctx.bot_data.get("engine")
    if not eng or not eng.ready:
        await update.effective_chat.send_message("Вселенная пока не загружена (жду Bybit API/повторяю попытки)…")
        return
    txt = (
        f"Вселенная: total={len(eng.uni.total)}, active={len(eng.uni.active)}, "
        f"batch#{eng.uni.batch_index}, ws_topics=0"
    )
    await update.effective_chat.send_message(txt)


async def cmd_universe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _guard_user(update, ctx.bot_data["cfg"]):
        return
    eng: Engine = ctx.bot_data.get("engine")
    if not eng or not eng.ready:
        await update.effective_chat.send_message("Вселенная пока не загружена (жду Bybit API/повторяю попытки)…")
        return
    sample = ", ".join(eng.uni.active[:15])
    txt = (
        f"Вселенная: total={len(eng.uni.total)}, active={len(eng.uni.active)}, "
        f"batch#{eng.uni.batch_index}, ws_topics=0\n"
    )
    if sample:
        txt += f"Активные (пример): {sample} ..."
    await update.effective_chat.send_message(txt)


async def cmd_rotate(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _guard_user(update, ctx.bot_data["cfg"]):
        return
    eng: Engine = ctx.bot_data.get("engine")
    if not eng or not eng.ready:
        await update.effective_chat.send_message("Нельзя — вселенная ещё не загружена.")
        return
    await eng.rotate_active()
    await update.effective_chat.send_message(
        f"Ротация выполнена: batch#{eng.uni.batch_index}, active={len(eng.uni.active)}"
    )


async def cmd_reload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _guard_user(update, ctx.bot_data["cfg"]):
        return
    eng: Engine = ctx.bot_data.get("engine")
    if not eng:
        await update.effective_chat.send_message("Движок ещё не инициализирован.")
        return
    eng.ready = False
    try:
        await eng.bootstrap_universe()
        await update.effective_chat.send_message("Вселенная перезагружена.")
    except Exception as e:
        await update.effective_chat.send_message(f"Ошибка перезагрузки: {e}")


# ------------------------ ОТПРАВКА СИГНАЛОВ ------------------------
def fmt_price(x: float) -> str:
    # грубая динамическая точность
    if x >= 1000:
        return f"{x:,.2f}".replace(",", " ")
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.8f}"


def render_signal(plan: TradePlan) -> str:
    side_emoji = "🟢 LONG" if plan.side == "long" else "🔴 SHORT"
    sign = "+" if plan.side == "long" else "-"
    msg = (
        f"📈 <b>{plan.symbol}</b>\n"
        f"{side_emoji}\n"
        f"Цена: <code>{fmt_price(plan.price)}</code>\n"
        f"Вход: <code>{fmt_price(plan.entry)}</code>\n"
        f"Тейк: <code>{fmt_price(plan.tp)}</code> ({sign}{abs(plan.profit_pct):.2f}%)\n"
        f"Стоп: <code>{fmt_price(plan.sl)}</code>\n"
        f"R/R: <b>{plan.rr:.2f}</b>\n"
        f"Вероятность: <b>{plan.prob_pct:.1f}%</b>\n"
        f"<i>{plan.reason}</i>"
    )
    return msg


async def push_signals(ctx: ContextTypes.DEFAULT_TYPE, recipients: List[int], plans: List[TradePlan], engine: Engine):
    if not plans:
        return
    # Отсортировано по prob desc; отправляем все (или ограничить top-K)
    top = plans[:10]
    for p in top:
        for chat_id in recipients:
            try:
                await ctx.bot.send_message(
                    chat_id=chat_id,
                    text=render_signal(p),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception as e:
                log.warning("send signal %s -> %s failed: %s", p.symbol, chat_id, e)
        engine._mark_signal(p.symbol)


# ------------------------ ДЖОБЫ ------------------------
async def job_health(ctx: ContextTypes.DEFAULT_TYPE):
    cfg: Config = ctx.bot_data["cfg"]
    for chat_id in cfg.primary_recipients:
        try:
            await ctx.bot.send_message(chat_id, "online")
        except Exception as e:
            log.debug("health send failed: %s", e)


async def job_build_universe(ctx: ContextTypes.DEFAULT_TYPE):
    cfg: Config = ctx.bot_data["cfg"]
    eng: Engine = ctx.bot_data["engine"]
    if eng.ready:
        return
    try:
        await eng.bootstrap_universe()
        # сразу сделаем первичную загрузку OI
        await eng.poll_oi()
        # и один скан — вдруг уже есть сетапы
        plans = await eng.scan_once()
        if plans:
            await push_signals(ctx, cfg.primary_recipients, plans, eng)
    except Exception as e:
        log.error("bootstrap failed: %s", e)


async def job_rotate(ctx: ContextTypes.DEFAULT_TYPE):
    eng: Engine = ctx.bot_data["engine"]
    if not eng.ready:
        return
    await eng.rotate_active()


async def job_poll_oi(ctx: ContextTypes.DEFAULT_TYPE):
    eng: Engine = ctx.bot_data["engine"]
    if not eng.ready:
        return
    await eng.poll_oi()


async def job_scan(ctx: ContextTypes.DEFAULT_TYPE):
    cfg: Config = ctx.bot_data["cfg"]
    eng: Engine = ctx.bot_data["engine"]
    if not eng.ready:
        return
    plans = await eng.scan_once()
    if plans:
        await push_signals(ctx, cfg.primary_recipients, plans, eng)


async def job_self_ping(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Лёгкий self-ping на локальный HTTP — держит контейнер бодрым.
    """
    cfg: Config = ctx.bot_data["cfg"]
    url = f"http://127.0.0.1:{cfg.http_port}/healthz"
    try:
        async with httpx.AsyncClient(timeout=5.0) as h:
            await h.get(url)
    except Exception:
        pass


# ------------------------ HTTP (HEALTH) ------------------------
async def http_health(request: web.Request) -> web.Response:
    return web.Response(text="OK")

async def http_root(request: web.Request) -> web.Response:
    return web.Response(text="CheCryptoSignalsBot")


def start_http_server(port: int) -> web.AppRunner:
    app = web.Application()
    app.add_routes([
        web.get("/", http_root),
        web.get("/healthz", http_health),
    ])
    runner = web.AppRunner(app)
    loop = asyncio.get_event_loop()

    async def _run():
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        log.info("HTTP health server started on :%d", port)

    loop.create_task(_run())
    return runner


# ------------------------ MAIN ------------------------
def build_app(cfg: Config, engine: Engine) -> Application:
    app: Application = (
        ApplicationBuilder()
        .token(cfg.token)
        .concurrent_updates(True)
        .build()
    )
    # bot_data для доступа из джобов/хэндлеров
    app.bot_data["cfg"] = cfg
    app.bot_data["engine"] = engine

    # Команды
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("universe", cmd_universe))
    app.add_handler(CommandHandler("rotate", cmd_rotate))
    app.add_handler(CommandHandler("reload", cmd_reload))

    # Джобы
    jq = app.job_queue
    # разовая: сбор вселенной
    jq.run_once(lambda c: job_build_universe(c), when=1.0)
    # периодика
    jq.run_repeating(lambda c: job_poll_oi(c), interval=cfg.poll_oi_sec, first=15.0)
    jq.run_repeating(lambda c: job_scan(c), interval=cfg.scan_sec, first=10.0)
    jq.run_repeating(lambda c: job_rotate(c), interval=cfg.rotate_min * 60, first=cfg.rotate_min * 60)
    jq.run_repeating(lambda c: job_health(c), interval=cfg.health_sec, first=cfg.first_health_sec)
    jq.run_repeating(lambda c: job_self_ping(c), interval=cfg.self_ping_sec, first=cfg.self_ping_sec)

    return app


def main():
    cfg = Config.load()
    if not cfg.token:
        raise SystemExit("TELEGRAM_BOT_TOKEN is required")

    # HTTP health server (обязателен для Render web service)
    start_http_server(cfg.http_port)

    client = BybitClient()
    engine = Engine(cfg, client)

    app = build_app(cfg, engine)

    async def runner():
        # на polling — убедимся, что вебхук снят
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass

        try:
            # приветствие
            for chat_id in cfg.primary_recipients:
                try:
                    await app.bot.send_message(chat_id, "Бот запущен (polling). Начинаю скрининг…")
                except Exception:
                    pass
            # запускаем PTB
            await app.initialize()
            await app.start()
            # запустим polling (async-вариант)
            await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            # блокируемся, пока не остановят процесс
            await asyncio.Event().wait()
        finally:
            try:
                await client.close()
            except Exception:
                pass
            try:
                await app.stop()
            except Exception:
                pass
            try:
                await app.shutdown()
            except Exception:
                pass

    asyncio.run(runner())


if __name__ == "__main__":
    main()
