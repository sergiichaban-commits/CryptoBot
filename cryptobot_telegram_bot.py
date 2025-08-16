#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import math
import os
import random
import string
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple

import aiohttp
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# --------------------------- ЛОГИ ---------------------------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# --------------------------- КОНФИГ ---------------------------
def _parse_int_list(val: str) -> List[int]:
    if not val:
        return []
    try:
        # Разрешаем форматы: "1,2,3" или JSON "[1,2,3]"
        if val.strip().startswith("["):
            arr = json.loads(val)
            return [int(x) for x in arr]
        return [int(x.strip()) for x in val.split(",") if x.strip()]
    except Exception:
        return []


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


@dataclass
class Config:
    TELEGRAM_TOKEN: str
    ALLOWED_CHAT_IDS: List[int]
    PRIMARY_RECIPIENTS: List[int]

    PUBLIC_URL: str
    PORT: int
    WEBHOOK_PATH: str

    HEALTH_SECONDS: int
    FIRST_HEALTH_DELAY: int
    SELF_PING_SECONDS: int

    SIGNAL_COOLDOWN_SEC: int
    SIGNAL_TTL_MIN: int

    UNIVERSE_MODE: str
    UNIVERSE_TOP_N: int
    WS_SYMBOLS_MAX: int
    ROTATE_MIN: int

    PROB_MIN: float
    PROFIT_MIN_PCT: float
    RR_MIN: float

    VOL_MULT: float
    VOL_SMA_PERIOD: int
    BODY_ATR_MULT: float
    ATR_PERIOD: int

    @staticmethod
    def load() -> "Config":
        token = os.getenv("TELEGRAM_TOKEN", "").strip()
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN is required")

        allowed = _parse_int_list(os.getenv("ALLOWED_CHAT_IDS", ""))
        primary = _parse_int_list(os.getenv("PRIMARY_RECIPIENTS", "")) or allowed

        public_url = os.getenv("PUBLIC_URL", "").strip()
        port = _env_int("PORT", 10000)
        wh_path = os.getenv("WEBHOOK_PATH", f"/wh-{random.randint(10_000_000, 99_999_999)}")

        health = _env_int("HEALTH_SECONDS", 20 * 60)  # 20 минут
        first = _env_int("FIRST_HEALTH_DELAY", 60)
        self_ping = _env_int("SELF_PING_SECONDS", 13 * 60)

        cooldown = _env_int("SIGNAL_COOLDOWN_SEC", 600)
        ttl = _env_int("SIGNAL_TTL_MIN", 12)

        universe_mode = os.getenv("UNIVERSE_MODE", "all").strip()
        universe_top = _env_int("UNIVERSE_TOP_N", 15)
        ws_symbols = _env_int("WS_SYMBOLS_MAX", 60)
        rotate_min = _env_int("ROTATE_MIN", 5)

        prob_min = _env_float("PROB_MIN", 69.9)
        profit_min_pct = _env_float("PROFIT_MIN_PCT", 1.0)  # снижено до 1%
        rr_min = _env_float("RR_MIN", 2.0)

        vol_mult = _env_float("VOL_MULT", 2.0)
        vol_sma = _env_int("VOL_SMA_PERIOD", 20)
        body_atr = _env_float("BODY_ATR_MULT", 0.60)
        atr_period = _env_int("ATR_PERIOD", 14)

        cfg = Config(
            TELEGRAM_TOKEN=token,
            ALLOWED_CHAT_IDS=allowed,
            PRIMARY_RECIPIENTS=primary,
            PUBLIC_URL=public_url,
            PORT=port,
            WEBHOOK_PATH=wh_path,
            HEALTH_SECONDS=health,
            FIRST_HEALTH_DELAY=first,
            SELF_PING_SECONDS=self_ping,
            SIGNAL_COOLDOWN_SEC=cooldown,
            SIGNAL_TTL_MIN=ttl,
            UNIVERSE_MODE=universe_mode,
            UNIVERSE_TOP_N=universe_top,
            WS_SYMBOLS_MAX=ws_symbols,
            ROTATE_MIN=rotate_min,
            PROB_MIN=prob_min,
            PROFIT_MIN_PCT=profit_min_pct,
            RR_MIN=rr_min,
            VOL_MULT=vol_mult,
            VOL_SMA_PERIOD=vol_sma,
            BODY_ATR_MULT=body_atr,
            ATR_PERIOD=atr_period,
        )

        log.info(
            "INFO [cfg] ALLOWED_CHAT_IDS=%s",
            cfg.ALLOWED_CHAT_IDS,
        )
        log.info("INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.PRIMARY_RECIPIENTS)
        log.info("INFO [cfg] PUBLIC_URL='%s' PORT=%s", cfg.PUBLIC_URL, cfg.PORT)
        log.info(
            "INFO [cfg] HEALTH=%ss FIRST=%ss SELF_PING=%ss",
            cfg.HEALTH_SECONDS,
            cfg.FIRST_HEALTH_DELAY,
            cfg.SELF_PING_SECONDS,
        )
        log.info(
            "INFO [cfg] SIGNAL_COOLDOWN_SEC=%s SIGNAL_TTL_MIN=%s UNIVERSE_MODE=%s "
            "UNIVERSE_TOP_N=%s WS_SYMBOLS_MAX=%s ROTATE_MIN=%s PROB_MIN>%.1f "
            "PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
            cfg.SIGNAL_COOLDOWN_SEC, cfg.SIGNAL_TTL_MIN, cfg.UNIVERSE_MODE,
            cfg.UNIVERSE_TOP_N, cfg.WS_SYMBOLS_MAX, cfg.ROTATE_MIN, cfg.PROB_MIN,
            cfg.PROFIT_MIN_PCT, cfg.RR_MIN,
        )
        log.info(
            "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
            cfg.VOL_MULT, cfg.VOL_SMA_PERIOD, cfg.BODY_ATR_MULT, cfg.ATR_PERIOD,
        )
        return cfg


# --------------------------- BYBIT API ---------------------------
class BybitClient:
    BASE_URL = "https://api.bybit.com"

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session

    async def _get(self, path: str, params: Dict) -> Dict:
        url = f"{self.BASE_URL}{path}"
        async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data

    async def get_symbols_linear(self) -> List[str]:
        """
        Получить все фьючерсные (linear/USDT Perpetual) торгуемые символы.
        """
        out: List[str] = []
        cursor = ""
        while True:
            params = {"category": "linear", "cursor": cursor, "limit": 500}
            data = await self._get("/v5/market/instruments-info", params)
            if data.get("retCode") != 0:
                raise RuntimeError(f"Bybit instruments error: {data}")
            result = data.get("result", {})
            rows = result.get("list", []) or []
            for r in rows:
                if r.get("status") == "Trading" and r.get("quoteCoin") == "USDT" and r.get("contractType") == "LinearPerpetual":
                    sym = r.get("symbol")
                    if sym:
                        out.append(sym)
            cursor = result.get("nextPageCursor") or ""
            if not cursor:
                break
            await asyncio.sleep(0.05)
        return sorted(list(set(out)))

    async def get_klines(self, symbol: str, interval: str = "5", limit: int = 300) -> List[Dict]:
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        data = await self._get("/v5/market/kline", params)
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit kline error: {data}")
        rows = (data.get("result", {}) or {}).get("list", []) or []
        # Bybit возвращает отсортированные по времени с конца к началу или наоборот — нормализуем по времени
        candles: List[Dict] = []
        for r in rows:
            # r: [start, open, high, low, close, volume, turnover]
            candles.append(
                {
                    "t": int(r[0]),
                    "o": float(r[1]),
                    "h": float(r[2]),
                    "l": float(r[3]),
                    "c": float(r[4]),
                    "v": float(r[5]),
                }
            )
        candles.sort(key=lambda x: x["t"])
        return candles

    async def get_open_interest(self, symbol: str, interval: str = "5min", limit: int = 6) -> List[Tuple[int, float]]:
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval, "limit": limit}
        data = await self._get("/v5/market/open-interest", params)
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit OI error: {data}")
        rows = (data.get("result", {}) or {}).get("list", []) or []
        out: List[Tuple[int, float]] = []
        for r in rows:
            # r: [timestamp, value]
            out.append((int(r[0]), float(r[1])))
        out.sort(key=lambda x: x[0])
        return out


# --------------------------- МАТЕМАТИКА/ИНДИКАТОРЫ ---------------------------
def _atr(candles: List[Dict], period: int) -> float:
    if len(candles) < period + 1:
        return float("nan")
    trs: List[float] = []
    for i in range(1, len(candles)):
        h = candles[i]["h"]
        l = candles[i]["l"]
        pc = candles[i - 1]["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return float("nan")
    return sum(trs[-period:]) / period


def _volume_sma(candles: List[Dict], period: int) -> float:
    if len(candles) < period:
        return float("nan")
    vs = [c["v"] for c in candles[-period:]]
    return sum(vs) / max(1, len(vs))


def _ema_series(vals: List[float], period: int) -> List[float]:
    if not vals:
        return []
    k = 2.0 / (period + 1.0)
    ema = []
    for i, v in enumerate(vals):
        if i == 0:
            ema.append(v)
        else:
            ema.append(v * k + ema[-1] * (1.0 - k))
    return ema


def _rolling_high(candles: List[Dict], lookback: int) -> float:
    hi = [c["h"] for c in candles[-lookback:]] if len(candles) >= lookback else [c["h"] for c in candles]
    return max(hi) if hi else float("nan")


def _rolling_low(candles: List[Dict], lookback: int) -> float:
    lo = [c["l"] for c in candles[-lookback:]] if len(candles) >= lookback else [c["l"] for c in candles]
    return min(lo) if lo else float("nan")


@dataclass
class Signal:
    symbol: str
    dir: str  # "LONG" / "SHORT"
    price: float
    entry: float
    take: float
    stop: float
    take_pct: float
    stop_pct: float
    rr: float
    prob: float
    ttl_min: int
    created_ts: float = time.time()


# --------------------------- АНАЛИЗ ---------------------------
def _analyze_symbol(
    cfg: Config,
    symbol: str,
    candles: List[Dict],
    oi: List[Tuple[int, float]],
) -> Optional[Signal]:
    # Требуется история для ATR/EMA/объёма
    need = max(cfg.VOL_SMA_PERIOD, cfg.ATR_PERIOD, 30) + 2
    if len(candles) < need:
        return None

    last = candles[-1]
    close = last["c"]
    body = abs(last["c"] - last["o"])

    atr = _atr(candles, cfg.ATR_PERIOD)
    if not math.isfinite(atr) or atr <= 0:
        return None

    vol_sma = _volume_sma(candles, cfg.VOL_SMA_PERIOD)
    if not math.isfinite(vol_sma) or vol_sma <= 0:
        return None

    vol_last = last["v"]
    # Базовые фильтры — не «выравниваем» сигналы на самом пороге
    if body <= cfg.BODY_ATR_MULT * atr:
        return None
    if vol_last <= (cfg.VOL_MULT * vol_sma):
        return None

    # Направление по телу свечи
    direction = "LONG" if last["c"] > last["o"] else "SHORT"

    # ===== ТРЕНД / ДИНАМИЧЕСКИЕ МНОЖИТЕЛИ =====
    closes = [c["c"] for c in candles]
    ema20_ser = _ema_series(closes, 20)
    ema20, ema20_prev = ema20_ser[-1], ema20_ser[-2]

    # сила тренда: насколько EMA20 движется за бар в единицах ATR
    trend_unit = abs((ema20 - ema20_prev) / max(1e-9, atr))
    # базовое «растяжение» 0.9..1.4
    trend_strength = max(0.9, min(1.4, 0.9 + trend_unit * 0.6))
    # бонус/штраф, если торгуем по наклону EMA20
    ema_dir = 1 if ema20 > ema20_prev else -1
    dir_sign = 1 if direction == "LONG" else -1
    trend_strength *= (1.05 if ema_dir == dir_sign else 0.95)
    trend_strength = max(0.85, min(1.5, trend_strength))

    # Динамические множители ATR
    TAKE_ATR_BASE = 1.20
    STOP_ATR_BASE = 0.60
    if direction == "LONG":
        take = close + (TAKE_ATR_BASE * trend_strength) * atr
        stop = close - (STOP_ATR_BASE / max(0.5, trend_strength)) * atr
    else:
        take = close - (TAKE_ATR_BASE * trend_strength) * atr
        stop = close + (STOP_ATR_BASE / max(0.5, trend_strength)) * atr

    entry = close
    risk = abs(entry - stop)
    reward = abs(take - entry)
    rr = (reward / risk) if risk > 0 else 0.0

    take_pct = (abs(take - entry) / entry) * 100.0 if entry > 0 else 0.0
    stop_pct = (abs(entry - stop) / entry) * 100.0 if entry > 0 else 0.0

    # Мини-структура: пробой/отбой от экстремума последних 20 баров
    breakout_units = 0.0
    lookback = 20
    if direction == "LONG":
        hh = _rolling_high(candles, lookback)
        if math.isfinite(hh):
            breakout_units = max(0.0, (close - hh) / max(1e-9, atr))
    else:
        ll = _rolling_low(candles, lookback)
        if math.isfinite(ll):
            breakout_units = max(0.0, (ll - close) / max(1e-9, atr))

    # Открытый интерес — величина и знак
    oi_boost = 0.0
    if len(oi) >= 2 and oi[-2][1] > 0:
        oi_delta_pct = (oi[-1][1] - oi[-2][1]) / oi[-2][1] * 100.0
        oi_boost = max(-8.0, min(8.0, 0.6 * oi_delta_pct * dir_sign))

    # Вероятность: объём, тело/ATR, тренд, пробой, OI, сонаправленность с EMA
    vol_spike = vol_last / max(1e-9, vol_sma)
    body_rel = body / max(1e-9, atr)

    prob = 50.0
    prob += 10.0 * math.log2(max(1.0, vol_spike))      # 2x=+10; 3x~+15.9
    prob += 8.0 * (body_rel / cfg.BODY_ATR_MULT - 1.0)
    prob += 6.0 * (trend_strength - 1.0) * 5.0
    prob += 10.0 * breakout_units
    prob += oi_boost
    prob += (4.0 if ema_dir == dir_sign else -6.0)
    prob = max(0.0, min(99.9, prob))

    # ФИЛЬТРЫ
    if prob < cfg.PROB_MIN:
        return None
    if rr < cfg.RR_MIN:
        return None
    if take_pct < cfg.PROFIT_MIN_PCT:
        return None

    return Signal(
        symbol=symbol,
        dir=direction,
        price=close,
        entry=entry,
        take=take,
        stop=stop,
        take_pct=take_pct,
        stop_pct=stop_pct,
        rr=rr,
        prob=prob,
        ttl_min=cfg.SIGNAL_TTL_MIN,
    )


# --------------------------- УНИВЕРС / СОСТОЯНИЕ ---------------------------
class State:
    def __init__(self):
        self.total_symbols: List[str] = []
        self.active_symbols: List[str] = []
        self.batch_idx: int = 0
        self.last_signal_sent: Dict[str, float] = {}  # symbol -> ts
        self.live_signals: Dict[str, Signal] = {}     # символы с активным TTL


# --------------------------- TELEGRAM ХЭНДЛЕРЫ ---------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _is_allowed(update, context):
        return
    await update.effective_message.reply_text("pong")


async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _is_allowed(update, context):
        return
    st: State = context.bot_data.get("state")
    if not st or not st.total_symbols:
        await update.effective_message.reply_text("Вселенная пока не загружена (жду Bybit API/повторяю попытки)…")
        return
    sample = ", ".join(st.active_symbols[:15])
    txt = (
        f"Вселенная: total={len(st.total_symbols)}, active={len(st.active_symbols)}, "
        f"batch#{st.batch_idx}, ws_topics={len(st.active_symbols)*2}\n"
        f"Активные (пример): {sample} ..."
    )
    await update.effective_message.reply_text(txt)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _is_allowed(update, context):
        return
    st: State = context.bot_data.get("state")
    if not st:
        await update.effective_message.reply_text("Статус недоступен…")
        return
    txt = f"Вселенная: total={len(st.total_symbols)}, active={len(st.active_symbols)}, batch#{st.batch_idx}, ws_topics={len(st.active_symbols)*2}"
    await update.effective_message.reply_text(txt)


async def _is_allowed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    cfg: Config = context.bot_data["cfg"]
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id is None:
        return False
    if cfg.ALLOWED_CHAT_IDS and chat_id not in cfg.ALLOWED_CHAT_IDS:
        try:
            await update.effective_message.reply_text("⛔️ Доступ запрещён.")
        except Exception:
            pass
        return False
    return True


# --------------------------- ДЖОБЫ ---------------------------
async def job_health(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    try:
        for cid in cfg.PRIMARY_RECIPIENTS:
            await context.bot.send_message(chat_id=cid, text="online")
    except Exception as e:
        log.warning("health send failed: %s", e)


async def job_self_ping(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not cfg.PUBLIC_URL:
        return
    url = cfg.PUBLIC_URL
    # пингуем корень — Render держит инстанс бодрым
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as s:
            async with s.get(url) as _:
                pass
    except Exception as e:
        log.debug("self-ping error: %s", e)


async def job_load_universe(context: ContextTypes.DEFAULT_TYPE):
    """
    Загружаем полный список линейных USDT-перпетуалов и активируем батч.
    """
    st: State = context.bot_data["state"]
    cfg: Config = context.bot_data["cfg"]
    client: BybitClient = context.bot_data["bybit"]

    try:
        all_syms = await client.get_symbols_linear()
        if cfg.UNIVERSE_MODE == "top" and cfg.UNIVERSE_TOP_N > 0:
            # Тут можно сортировать по обороту/volatility, но оставим как есть (пример).
            st.total_symbols = all_syms[: cfg.UNIVERSE_TOP_N]
        else:
            st.total_symbols = all_syms

        # первая активация
        await _rotate_active(context)
        log.info("INFO [universe] total=%d active=%d mode=%s", len(st.total_symbols), len(st.active_symbols), cfg.UNIVERSE_MODE)
    except Exception as e:
        log.error("universe load failed: %s", e)


async def _rotate_active(context: ContextTypes.DEFAULT_TYPE):
    """
    Ротация активных символов «по кольцу».
    """
    st: State = context.bot_data["state"]
    cfg: Config = context.bot_data["cfg"]
    if not st.total_symbols:
        return
    n = cfg.WS_SYMBOLS_MAX
    start = (st.batch_idx * n) % len(st.total_symbols)
    # набираем n символов по кругу
    new_active: List[str] = []
    i = start
    while len(new_active) < n and st.total_symbols:
        new_active.append(st.total_symbols[i % len(st.total_symbols)])
        i += 1
    st.active_symbols = new_active
    st.batch_idx += 1
    log.info(
        "INFO [universe] rotated: active=%d batch#%d",
        len(st.active_symbols),
        st.batch_idx,
    )


async def job_rotate(context: ContextTypes.DEFAULT_TYPE):
    await _rotate_active(context)


async def job_scan(context: ContextTypes.DEFAULT_TYPE):
    """
    Периодически сканируем активные символы и выдаём только актуальные сетапы,
    отсортированные по вероятности (без публикации «нет сетапов»).
    """
    st: State = context.bot_data["state"]
    cfg: Config = context.bot_data["cfg"]
    client: BybitClient = context.bot_data["bybit"]

    if not st.active_symbols:
        return

    candidates: List[Signal] = []
    for sym in st.active_symbols:
        try:
            candles = await client.get_klines(sym, interval="5", limit=300)
            oi = await client.get_open_interest(sym, interval="5min", limit=6)
            sig = _analyze_symbol(cfg, sym, candles, oi)
            if sig:
                candidates.append(sig)
            # маленькая пауза, чтобы не бить лимиты
            await asyncio.sleep(0.05)
        except Exception as e:
            log.debug("scan %s error: %s", sym, e)

    if not candidates:
        return

    # сортировка по вероятности убыв.
    candidates.sort(key=lambda s: (-s.prob, -s.rr, -s.take_pct))

    now_ts = time.time()
    out_msgs: List[Tuple[str, Signal]] = []

    for s in candidates:
        last_ts = st.last_signal_sent.get(s.symbol, 0.0)
        # Кулдаун на символ
        if now_ts - last_ts < cfg.SIGNAL_COOLDOWN_SEC:
            continue
        # Если есть «живой» сигнал — не дублируем, если прежний ещё жив
        live = st.live_signals.get(s.symbol)
        if live and (now_ts - live.created_ts) < (live.ttl_min * 60):
            # если новый явно лучше по prob — заменим
            if s.prob <= live.prob:
                continue

        st.last_signal_sent[s.symbol] = now_ts
        st.live_signals[s.symbol] = s

        # Формат вывода (только вероятные, как выше отфильтровано)
        sign = "ЛОНГ" if s.dir == "LONG" else "ШОРТ"
        take_delta = s.take_pct
        stop_delta = s.stop_pct

        msg = (
            f"#{s.symbol} — {sign}\n"
            f"Текущая: {s.price:.6g}\n"
            f"Вход: {s.entry:.6g}\n"
            f"Тейк: {s.take:.6g} (+{take_delta:.2f}%)\n"
            f"Стоп: {s.stop:.6g} (-{stop_delta:.2f}%)\n"
            f"R/R: {s.rr:.2f} | Вероятность: {s.prob:.1f}%"
        )
        out_msgs.append((msg, s))

    # Публикуем в порядке убывания вероятности
    if not out_msgs:
        return

    for msg, _ in out_msgs:
        for cid in cfg.PRIMARY_RECIPIENTS:
            try:
                await context.bot.send_message(chat_id=cid, text=msg)
            except Exception as e:
                log.warning("send signal failed: %s", e)
        # маленькая пауза между сообщениями
        await asyncio.sleep(0.6)


# --------------------------- HTTP (для Render) ---------------------------
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")
            return
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"not found")

    # глушим лишний лог http.server
    def log_message(self, format, *args):
        return


def _start_health_server(port: int):
    def _run():
        srv = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
        log.info("HTTP health server started on :%d", port)
        srv.serve_forever()

    th = threading.Thread(target=_run, daemon=True)
    th.start()
    return th


# --------------------------- MAIN ---------------------------
def _random_path(n=8) -> str:
    return "/wh-" + "".join(random.choice(string.digits) for _ in range(n))


async def _bootstrap(app: Application, cfg: Config):
    """
    Планирование задач и инициализация общего состояния.
    """
    # Общее состояние
    st = State()
    app.bot_data["state"] = st
    # HTTP сессия для Bybit
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    app.bot_data["aiohttp_session"] = session
    app.bot_data["bybit"] = BybitClient(session)

    # Команды
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("universe", cmd_universe))
    app.add_handler(CommandHandler("status", cmd_status))

    # Задачи
    jq = app.job_queue
    # здоровье в чат раз в HEALTH_SECONDS
    jq.run_repeating(job_health, interval=cfg.HEALTH_SECONDS, first=cfg.FIRST_HEALTH_DELAY, name="health")
    # self-ping (если задан PUBLIC_URL)
    if cfg.PUBLIC_URL:
        jq.run_repeating(job_self_ping, interval=cfg.SELF_PING_SECONDS, first=cfg.SELF_PING_SECONDS, name="self_ping")
    # загрузка универса при старте + ротация
    jq.run_once(job_load_universe, when=3)
    jq.run_repeating(job_rotate, interval=cfg.ROTATE_MIN * 60, first=cfg.ROTATE_MIN * 60, name="rotate")
    # сканер сигналов
    jq.run_repeating(job_scan, interval=30, first=15, name="scan")


def main():
    # Настройка конфигурации
    cfg = Config.load()

    # Собираем приложение
    application = Application.builder().token(cfg.TELEGRAM_TOKEN).build()
    application.bot_data["cfg"] = cfg

    # Поднимаем минимальный HTTP сервер ВСЕГДА в polling-режиме,
    # чтобы Render видел открытый порт и не падал с Port Scan Timeout.
    use_webhook = bool(cfg.PUBLIC_URL and cfg.PUBLIC_URL.startswith("https://"))
    if not use_webhook:
        _start_health_server(cfg.PORT)

    async def runner():
        await _bootstrap(application, cfg)
        if use_webhook:
            # Вебхук-режим (порт открывает PTB сам)
            webhook_url = cfg.PUBLIC_URL.rstrip("/") + (cfg.WEBHOOK_PATH or _random_path())
            # сбросить старый вебхук на всякий случай
            try:
                await application.bot.delete_webhook()
            except Exception:
                pass
            log.info("Setting webhook to %s", webhook_url)
            await application.run_webhook(
                listen="0.0.0.0",
                port=cfg.PORT,
                webhook_url=webhook_url,
                allowed_updates=Update.ALL_TYPES,
            )
        else:
            # Polling-режим (блокирующий вызов)
            try:
                # убеждаемся, что вебхук снят
                await application.bot.delete_webhook()
            except Exception:
                pass
            log.info("Polling started (fallback)")
            await application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, close_loop=False)

    # Аккуратно запускаем основной async-раннер
    try:
        asyncio.run(runner())
    finally:
        # Грамотно закрываем aiohttp-сессию если она была создана
        try:
            session: aiohttp.ClientSession = application.bot_data.get("aiohttp_session")
            if session and not session.closed:
                # Здесь нельзя использовать asyncio.run — уже в finally.
                # Откроем временный цикл:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(session.close())
                loop.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
