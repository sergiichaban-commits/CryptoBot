#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import time
import math
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, replace

import aiohttp
from aiohttp import web

import numpy as np
import pandas as pd

from telegram import Update
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
)

# --------------------------------
# ЛОГИ
# --------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# --------------------------------
# КОНФИГ
# --------------------------------
@dataclass(frozen=True)
class Config:
    TELEGRAM_TOKEN: str
    ALLOWED_CHAT_IDS: List[int]
    PRIMARY_RECIPIENTS: List[int]

    PORT: int
    PUBLIC_URL: str  # может быть пустым -> работаем в режиме polling
    HEALTH_SEC: int
    SELF_PING_SEC: int

    SIGNAL_COOLDOWN_SEC: int
    SIGNAL_TTL_MIN: int

    WS_SYMBOLS_MAX: int
    ROTATE_MIN: int

    PROB_MIN: float
    RR_MIN: float
    PROFIT_MIN_PCT: float

    VOL_MULT: float
    VOL_SMA_PERIOD: int
    BODY_ATR_MULT: float
    ATR_PERIOD: int

    @staticmethod
    def _read_list_int(name: str, default: Optional[str]) -> List[int]:
        raw = os.getenv(name, default or "").strip()
        if not raw:
            return []
        out: List[int] = []
        for tok in raw.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                out.append(int(tok))
            except Exception:
                pass
        return out

    @staticmethod
    def load() -> "Config":
        token = os.getenv("TELEGRAM_TOKEN", "").strip()
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN is required")

        allowed = Config._read_list_int("ALLOWED_CHAT_IDS", os.getenv("PRIMARY_CHAT_ID", ""))  # bwd compat
        if not allowed:
            # В крайнем случае — разрешим только себе (если указан PRIMARY_CHAT_ID)
            prim_chat = os.getenv("PRIMARY_CHAT_ID", "").strip()
            if prim_chat:
                try:
                    allowed = [int(prim_chat)]
                except Exception:
                    allowed = []
        recipients = Config._read_list_int("PRIMARY_RECIPIENTS", os.getenv("PRIMARY_CHAT_ID", "")) or allowed

        port = int(os.getenv("PORT", "10000"))
        public_url = os.getenv("PUBLIC_URL", "").strip()

        health = int(os.getenv("HEALTH_SEC", "1200"))           # 20 мин
        self_ping = int(os.getenv("SELF_PING_SEC", "780"))      # 13 мин

        cooldown = int(os.getenv("SIGNAL_COOLDOWN_SEC", "600"))
        ttl_min = int(os.getenv("SIGNAL_TTL_MIN", "12"))

        ws_max = int(os.getenv("WS_SYMBOLS_MAX", "60"))
        rotate_min = int(os.getenv("ROTATE_MIN", "5"))

        prob_min = float(os.getenv("PROB_MIN", "69.9"))
        rr_min = float(os.getenv("RR_MIN", "2.0"))
        profit_min = float(os.getenv("PROFIT_MIN_PCT", "1.0"))

        vol_mult = float(os.getenv("VOL_MULT", "2.0"))
        vol_sma = int(os.getenv("VOL_SMA_PERIOD", "20"))
        body_atr = float(os.getenv("BODY_ATR_MULT", "0.60"))
        atr_period = int(os.getenv("ATR_PERIOD", "14"))

        log.info("INFO [cfg] ALLOWED_CHAT_IDS=%s", allowed)
        log.info("INFO [cfg] PRIMARY_RECIPIENTS=%s", recipients)
        log.info("INFO [cfg] PUBLIC_URL='%s' PORT=%d", public_url, port)
        log.info("INFO [cfg] HEALTH=%ss SELF_PING=%ss", health, self_ping)
        log.info(
            "INFO [cfg] SIGNAL_COOLDOWN_SEC=%d SIGNAL_TTL_MIN=%d WS_SYMBOLS_MAX=%d ROTATE_MIN=%d PROB_MIN>%.1f PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
            cooldown, ttl_min, ws_max, rotate_min, prob_min, profit_min, rr_min
        )
        log.info(
            "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
            vol_mult, vol_sma, body_atr, atr_period
        )

        return Config(
            TELEGRAM_TOKEN=token,
            ALLOWED_CHAT_IDS=allowed,
            PRIMARY_RECIPIENTS=recipients,
            PORT=port,
            PUBLIC_URL=public_url,
            HEALTH_SEC=health,
            SELF_PING_SEC=self_ping,
            SIGNAL_COOLDOWN_SEC=cooldown,
            SIGNAL_TTL_MIN=ttl_min,
            WS_SYMBOLS_MAX=ws_max,
            ROTATE_MIN=rotate_min,
            PROB_MIN=prob_min,
            RR_MIN=rr_min,
            PROFIT_MIN_PCT=profit_min,
            VOL_MULT=vol_mult,
            VOL_SMA_PERIOD=vol_sma,
            BODY_ATR_MULT=body_atr,
            ATR_PERIOD=atr_period,
        )


# --------------------------------
# BYBIT CLIENT
# --------------------------------
BYBIT_V5 = "https://api.bybit.com"

class BybitClient:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def _get(self, path: str, params: Dict[str, str]) -> dict:
        assert self._session is not None, "session not started"
        url = f"{BYBIT_V5}{path}"
        async with self._session.get(url, params=params) as r:
            r.raise_for_status()
            js = await r.json()
            return js

    async def list_futures_usdt(self) -> List[str]:
        """Все linear USDT-контракты в статусе Trading."""
        out: List[str] = []
        cursor = None
        while True:
            params = {"category": "linear"}
            if cursor:
                params["cursor"] = cursor
            js = await self._get("/v5/market/instruments-info", params)
            if js.get("retCode") != 0:
                raise RuntimeError(f"Bybit instruments error: {js}")
            res = js["result"]
            for it in res.get("list", []):
                if it.get("status") == "Trading" and it.get("quoteCoin") == "USDT":
                    sym = it.get("symbol")
                    if sym:
                        out.append(sym)
            cursor = res.get("nextPageCursor") or None
            if not cursor:
                break
        return sorted(set(out))

    async def get_klines(self, symbol: str, interval: str = "5", limit: int = 300) -> pd.DataFrame:
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)}
        js = await self._get("/v5/market/kline", params)
        if js.get("retCode") != 0:
            raise RuntimeError(f"Bybit kline error: {js}")
        arr = js["result"]["list"]
        # Bybit v5 kline: [start, open, high, low, close, volume, turnover]
        data = []
        for row in arr:
            ts = int(row[0])
            o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4])
            v = float(row[5])
            data.append((ts, o, h, l, c, v))
        df = pd.DataFrame(data, columns=["ts", "open", "high", "low", "close", "volume"])
        df.sort_values("ts", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    async def get_oi(self, symbol: str, interval: str = "5min", limit: int = 6) -> pd.DataFrame:
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval, "limit": str(limit)}
        js = await self._get("/v5/market/open-interest", params)
        if js.get("retCode") != 0:
            raise RuntimeError(f"Bybit OI error: {js}")
        arr = js["result"]["list"]
        data = []
        # list entries: [timestamp, openInterest, ...]
        for row in arr:
            ts = int(row[0])
            oi = float(row[1])
            data.append((ts, oi))
        df = pd.DataFrame(data, columns=["ts", "oi"])
        df.sort_values("ts", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df


# --------------------------------
# СОСТОЯНИЕ
# --------------------------------
@dataclass
class Signal:
    symbol: str
    dir: str              # "LONG"/"SHORT"
    price: float
    entry: float
    take: float
    stop: float
    take_pct: float
    stop_pct: float
    rr: float
    prob: float
    ttl_min: int
    created_ts: float

@dataclass
class State:
    universe_all: List[str]
    active_symbols: List[str]
    batch_idx: int
    ws_topics: int

    last_signal_sent: Dict[str, float]
    live_signals: Dict[str, Signal]

    last_health_ts: float


# --------------------------------
# ТЕХНИКА/АНАЛИТИКА
# --------------------------------
def _atr(df: pd.DataFrame, period: int) -> pd.Series:
    h, l, c = df["high"].values, df["low"].values, df["close"].values
    prev_close = np.r_[c[0], c[:-1]]
    tr = np.maximum(h - l, np.maximum(abs(h - prev_close), abs(l - prev_close)))
    atr = pd.Series(tr).rolling(window=period, min_periods=period).mean()
    atr.index = df.index
    return atr

def _sma(x: pd.Series, period: int) -> pd.Series:
    return x.rolling(window=period, min_periods=period).mean()

def _analyze(cfg: Config, symbol: str, kl: pd.DataFrame, oi: pd.DataFrame) -> Optional[Signal]:
    if len(kl) < max(cfg.ATR_PERIOD, cfg.VOL_SMA_PERIOD) + 5:
        return None

    df = kl.copy()
    df["atr"] = _atr(df, cfg.ATR_PERIOD)
    df["vol_sma"] = _sma(df["volume"], cfg.VOL_SMA_PERIOD)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # признаки
    body = abs(last["close"] - last["open"])
    body_vs_atr = (df["atr"].iloc[-1] > 0) and (body / df["atr"].iloc[-1] >= cfg.BODY_ATR_MULT)
    vol_spike = (last["volume"] >= cfg.VOL_MULT * (last["vol_sma"] if not math.isnan(last["vol_sma"]) else 0))

    # свип локального экстремума последних N (10) свечей
    N = 10
    hh = df["high"].iloc[-(N+1):-1].max()
    ll = df["low"].iloc[-(N+1):-1].min()
    sweep_up = (last["high"] > hh) and (last["close"] < hh)        # прокол вверх и закрытие ниже
    sweep_down = (last["low"] < ll) and (last["close"] > ll)       # прокол вниз и закрытие выше

    # простая тенденция: close vs SMA(20)
    df["ma20"] = _sma(df["close"], 20)
    trend_up = last["close"] > df["ma20"].iloc[-1] if not math.isnan(df["ma20"].iloc[-1]) else False
    trend_down = last["close"] < df["ma20"].iloc[-1] if not math.isnan(df["ma20"].iloc[-1]) else False

    # OI дельта %
    oi_delta_pct = 0.0
    if len(oi) >= 3:
        oi_now = oi["oi"].iloc[-1]
        oi_prev = oi["oi"].iloc[-3]
        if oi_prev > 0:
            oi_delta_pct = (oi_now - oi_prev) / oi_prev * 100.0

    # Решаем направление
    long_score = 0
    short_score = 0

    if body_vs_atr and vol_spike:
        if last["close"] >= last["open"]:
            long_score += 2
        else:
            short_score += 2

    if sweep_down:
        long_score += 2
    if sweep_up:
        short_score += 2

    if trend_up:
        long_score += 1
    if trend_down:
        short_score += 1

    # OI: рост OI при росте цены — больше за LONG; рост OI при падении цены — за SHORT
    price_chg = last["close"] - prev["close"]
    if oi_delta_pct != 0:
        if price_chg > 0 and oi_delta_pct > 0:
            long_score += 1
        if price_chg < 0 and oi_delta_pct > 0:
            short_score += 1
        if oi_delta_pct < 0:
            # сокращение OI ослабляет направление — легкий минус
            if price_chg > 0:
                long_score -= 0.5
            if price_chg < 0:
                short_score -= 0.5

    if long_score <= 0 and short_score <= 0:
        return None

    direction = "LONG" if long_score >= short_score else "SHORT"
    price = float(last["close"])
    atr = float(df["atr"].iloc[-1]) if not math.isnan(df["atr"].iloc[-1]) else 0.0
    if atr <= 0:
        return None

    # Конструируем вход/стоп/тейк
    # вход — лёгкий ретест 0.15*ATR от цены (в сторону против направления)
    retr = 0.15 * atr
    if direction == "LONG":
        entry = max(df["low"].iloc[-1], price - retr)
        # стоп — ниже минимума бара/или 0.8*ATR от входа (берём дальше)
        stop_a = df["low"].iloc[-1] - 1e-9
        stop_b = entry - 0.8 * atr
        stop = min(stop_a, stop_b)
        risk = entry - stop
    else:
        entry = min(df["high"].iloc[-1], price + retr)
        stop_a = df["high"].iloc[-1] + 1e-9
        stop_b = entry + 0.8 * atr
        stop = max(stop_a, stop_b)
        risk = stop - entry

    if risk <= 0:
        return None

    # множитель тейка зависит от качества набора сигналов, чтобы R/R не был одинаковым
    quality = 0
    quality += 1 if body_vs_atr else 0
    quality += 1 if vol_spike else 0
    quality += 1 if (sweep_up or sweep_down) else 0
    quality += 1 if ((direction == "LONG" and trend_up) or (direction == "SHORT" and trend_down)) else 0
    quality += 1 if ((direction == "LONG" and oi_delta_pct > 0) or (direction == "SHORT" and oi_delta_pct > 0)) else 0

    # базовый множитель
    base_rr = 1.6
    tp_mult = base_rr + 0.2 * quality    # ~1.6 .. 2.6 (иногда 2.8)
    # слегка рандомизируем через рыночные данные (без RNG): используем доли цены
    frac = (price - math.floor(price)) if price > 1 else price
    tp_mult += (frac % 0.07)  # 0..0.07 — мелкая де-синхронизация для «разных R/R»

    if direction == "LONG":
        take = entry + tp_mult * risk
    else:
        take = entry - tp_mult * risk

    # проценты и R/R
    stop_pct = abs((entry - stop) / entry) * 100.0
    take_pct = abs((take - entry) / entry) * 100.0
    rr = take_pct / max(1e-9, stop_pct)

    # вероятность: базовая 60 + бонусы за сигналы
    prob = 60.0
    if body_vs_atr: prob += 6.0
    if vol_spike: prob += 6.0
    if sweep_up or sweep_down: prob += 5.0
    if (direction == "LONG" and trend_up) or (direction == "SHORT" and trend_down):
        prob += 5.0
    if (direction == "LONG" and oi_delta_pct > 0) or (direction == "SHORT" and oi_delta_pct > 0):
        prob += 4.0
    if rr >= 2.0:
        prob += 2.0
    prob = max(50.0, min(92.0, prob))  # зажимаем

    # фильтры
    if rr < cfg.RR_MIN:
        return None
    if take_pct < cfg.PROFIT_MIN_PCT:
        return None
    if not (prob > cfg.PROB_MIN):
        return None

    return Signal(
        symbol=symbol,
        dir=direction,
        price=price,
        entry=float(entry),
        take=float(take),
        stop=float(stop),
        take_pct=float(take_pct),
        stop_pct=float(stop_pct),
        rr=float(rr),
        prob=float(prob),
        ttl_min=cfg.SIGNAL_TTL_MIN,
        created_ts=time.time(),
    )


# --------------------------------
# КОМАНДЫ
# --------------------------------
def _allowed(update: Update, cfg: Config) -> bool:
    chat_id = update.effective_chat.id if update.effective_chat else None
    return chat_id in cfg.ALLOWED_CHAT_IDS

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not _allowed(update, cfg):
        return
    await update.effective_message.reply_text("pong")

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    st: State = context.bot_data["state"]
    if not _allowed(update, cfg):
        return
    if not st.universe_all:
        await update.effective_message.reply_text("Вселенная пока не загружена (жду Bybit API/повторяю попытки)…")
        return
    sample = ", ".join(st.active_symbols[:15])
    await update.effective_message.reply_text(
        f"Вселенная: total={len(st.universe_all)}, active={len(st.active_symbols)}, batch#{st.batch_idx}, ws_topics={st.ws_topics}\n"
        + (f"Активные (пример): {sample} ..." if sample else "")
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    st: State = context.bot_data["state"]
    if not _allowed(update, cfg):
        return
    await update.effective_message.reply_text(
        f"Вселенная: total={len(st.universe_all)}, active={len(st.active_symbols)}, batch#{st.batch_idx}, ws_topics={st.ws_topics}"
    )

async def cmd_probe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    st: State = context.bot_data["state"]
    client: BybitClient = context.bot_data["bybit"]
    if not _allowed(update, cfg):
        return
    if not context.args:
        await update.effective_message.reply_text("Использование: /probe SYMBOL (например: /probe BTCUSDT)")
        return
    sym = context.args[0].upper().strip()
    try:
        kl = await client.get_klines(sym, interval="5", limit=300)
        oi = await client.get_oi(sym, interval="5min", limit=6)
        sig = _analyze(cfg, sym, kl, oi)
        if sig:
            sign = "ЛОНГ" if sig.dir == "LONG" else "ШОРТ"
            msg = (
                f"#{sig.symbol} — {sign}\n"
                f"Текущая: {sig.price:.6g}\n"
                f"Вход: {sig.entry:.6g}\n"
                f"Тейк: {sig.take:.6g} (+{sig.take_pct:.2f}%)\n"
                f"Стоп: {sig.stop:.6g} (-{sig.stop_pct:.2f}%)\n"
                f"R/R: {sig.rr:.2f} | Вероятность: {sig.prob:.1f}%"
            )
        else:
            msg = "Сетап не прошёл фильтр (недостаточный R/R / прибыль / вероятность или мало данных)."
        await update.effective_message.reply_text(msg)
    except Exception as e:
        await update.effective_message.reply_text(f"Ошибка /probe: {e}")


# --------------------------------
# ДЖОБЫ
# --------------------------------
async def job_health(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    st: State = context.bot_data["state"]
    now = time.time()
    # не спамим чаще чем HEALTH_SEC/2
    if now - st.last_health_ts < max(30, cfg.HEALTH_SEC / 2):
        return
    st.last_health_ts = now
    msg = f"online · total={len(st.universe_all)} active={len(st.active_symbols)} batch#{st.batch_idx}"
    for cid in cfg.PRIMARY_RECIPIENTS:
        try:
            await context.bot.send_message(cid, msg)
        except Exception as e:
            log.warning("health send failed: %s", e)

async def job_self_ping(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not cfg.PUBLIC_URL:
        return
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as s:
            async with s.get(cfg.PUBLIC_URL) as r:
                _ = await r.text()
    except Exception:
        pass

async def job_rotate(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    st: State = context.bot_data["state"]
    if not st.universe_all:
        return
    total = len(st.universe_all)
    size = max(5, min(cfg.WS_SYMBOLS_MAX, total))
    start = (st.batch_idx * size) % total
    end = start + size
    if end <= total:
        st.active_symbols = st.universe_all[start:end]
    else:
        st.active_symbols = st.universe_all[start:] + st.universe_all[: (end % total)]
    st.batch_idx += 1
    st.ws_topics = len(st.active_symbols) * 2  # профанация метрики ws для статуса

async def job_refresh_universe(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    st: State = context.bot_data["state"]
    client: BybitClient = context.bot_data["bybit"]
    try:
        syms = await client.list_futures_usdt()
        if syms:
            st.universe_all = syms
            if not st.active_symbols:
                await job_rotate(context)
            log.info("INFO [universe] total=%d active=%d", len(st.universe_all), len(st.active_symbols))
    except Exception as e:
        log.warning("universe load failed: %s", e)

async def job_scan(context: ContextTypes.DEFAULT_TYPE):
    st: State = context.bot_data["state"]
    cfg: Config = context.bot_data["cfg"]
    client: BybitClient = context.bot_data["bybit"]
    if not st.active_symbols or not client:
        return

    # --- АДАПТИВНЫЕ ПОРОГИ ПРИ ТИШИНЕ ---
    now = time.time()
    last_any = max(st.last_signal_sent.values()) if st.last_signal_sent else 0.0
    minutes_since_any = (now - last_any) / 60 if last_any else 1e9
    eff_cfg = cfg
    if minutes_since_any > 45:
        prob = max(55.0, cfg.PROB_MIN - 8.0)
        rr = max(1.4, cfg.RR_MIN - 0.3)
        prof = max(0.6, cfg.PROFIT_MIN_PCT - 0.3)
        eff_cfg = replace(cfg, PROB_MIN=prob, RR_MIN=rr, PROFIT_MIN_PCT=prof)
        if int(now) % 300 < 2:
            log.info("ADAPTIVE: relax thresholds to PROB_MIN=%.1f RR_MIN=%.2f PROFIT_MIN_PCT=%.2f",
                     eff_cfg.PROB_MIN, eff_cfg.RR_MIN, eff_cfg.PROFIT_MIN_PCT)

    candidates: List[Signal] = []
    for sym in st.active_symbols:
        try:
            kl = await client.get_klines(sym, interval="5", limit=300)
            oi = await client.get_oi(sym, interval="5min", limit=6)
            s = _analyze(eff_cfg, sym, kl, oi)
            if s:
                candidates.append(s)
            await asyncio.sleep(0.03)
        except Exception as e:
            log.debug("scan %s error: %s", sym, e)

    if not candidates:
        return

    candidates.sort(key=lambda s: (-s.prob, -s.rr, -s.take_pct))
    now_ts = time.time()
    out_msgs: List[str] = []

    for s in candidates:
        last_ts = st.last_signal_sent.get(s.symbol, 0.0)
        if now_ts - last_ts < cfg.SIGNAL_COOLDOWN_SEC:
            continue
        live = st.live_signals.get(s.symbol)
        if live and (now_ts - live.created_ts) < (live.ttl_min * 60) and s.prob <= live.prob:
            continue
        st.last_signal_sent[s.symbol] = now_ts
        st.live_signals[s.symbol] = s
        sign = "ЛОНГ" if s.dir == "LONG" else "ШОРТ"
        msg = (
            f"#{s.symbol} — {sign}\n"
            f"Текущая: {s.price:.6g}\n"
            f"Вход: {s.entry:.6g}\n"
            f"Тейк: {s.take:.6g} (+{s.take_pct:.2f}%)\n"
            f"Стоп: {s.stop:.6g} (-{s.stop_pct:.2f}%)\n"
            f"R/R: {s.rr:.2f} | Вероятность: {s.prob:.1f}%"
        )
        out_msgs.append(msg)

    for msg in out_msgs:
        for cid in cfg.PRIMARY_RECIPIENTS:
            try:
                await context.bot.send_message(chat_id=cid, text=msg)
            except Exception as e:
                log.warning("send signal failed: %s", e)
        await asyncio.sleep(0.5)


# --------------------------------
# ВЕБ-СЕРВЕР (для Render)
# --------------------------------
async def _http_index(_):
    return web.Response(text="OK", content_type="text/plain")

async def start_http_server(port: int):
    app = web.Application()
    app.add_routes([web.get("/", _http_index), web.get("/health", _http_index)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("HTTP server started on :%d", port)


# --------------------------------
# MAIN
# --------------------------------
def build_app(cfg: Config) -> Application:
    application: Application = (
        ApplicationBuilder()
        .token(cfg.TELEGRAM_TOKEN)
        .build()
    )

    # handlers
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("universe", cmd_universe))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("probe", cmd_probe))

    # простая защита: игнор всех сообщений не из списка
    application.add_handler(MessageHandler(~filters.COMMAND & filters.ALL, lambda u, c: None))

    return application

async def main_async():
    cfg = Config.load()

    # HTTP сервер для Render (порт должен быть открыт)
    await start_http_server(cfg.PORT)

    # Telegram application
    app = build_app(cfg)

    # Состояние и клиент
    st = State(
        universe_all=[],
        active_symbols=[],
        batch_idx=0,
        ws_topics=0,
        last_signal_sent={},
        live_signals={},
        last_health_ts=0.0,
    )
    client = BybitClient()
    await client.start()

    # bot_data
    app.bot_data["cfg"] = cfg
    app.bot_data["state"] = st
    app.bot_data["bybit"] = client

    # Планировщик (JobQueue PTB)
    jq = app.job_queue
    # начальная загрузка вселенной и ротация
    jq.run_once(job_refresh_universe, when=1.0)
    jq.run_repeating(job_refresh_universe, interval=15 * 60, first=15 * 60)  # периодически обновляем список
    jq.run_repeating(job_rotate, interval=cfg.ROTATE_MIN * 60, first=10)
    # сканер
    jq.run_repeating(job_scan, interval=30, first=20)
    # health
    jq.run_repeating(job_health, interval=cfg.HEALTH_SEC, first=cfg.HEALTH_SEC)
    # self-ping (если есть PUBLIC_URL)
    jq.run_repeating(job_self_ping, interval=cfg.SELF_PING_SEC, first=cfg.SELF_PING_SEC)

    # ВАЖНО: не вызываем run_polling(), чтобы не словить loop.run_until_complete внутри PTB.
    await app.initialize()
    await app.start()
    # Включаем polling "вручную"
    await app.updater.start_polling(drop_pending_updates=True)
    log.info("Application started (polling)")

    try:
        # Спим бесконечно; Updater сам крутит polling-таски
        while True:
            await asyncio.sleep(3600)
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        await client.close()

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
