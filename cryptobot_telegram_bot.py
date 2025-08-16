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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple

import aiohttp
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ============================ ЛОГИ ============================
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("cryptobot")


# ============================ УТИЛС ============================
def _parse_int_list(val: str) -> List[int]:
    if not val:
        return []
    try:
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


def _random_path(n=8) -> str:
    return "/wh-" + "".join(random.choice(string.digits) for _ in range(n))


# ============================ КОНФИГ ============================
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
        wh_path = os.getenv("WEBHOOK_PATH", "") or _random_path()

        health = _env_int("HEALTH_SECONDS", 20 * 60)
        first = _env_int("FIRST_HEALTH_DELAY", 60)
        self_ping = _env_int("SELF_PING_SECONDS", 13 * 60)

        cooldown = _env_int("SIGNAL_COOLDOWN_SEC", 600)
        ttl = _env_int("SIGNAL_TTL_MIN", 12)

        universe_mode = os.getenv("UNIVERSE_MODE", "all").strip()  # all/top
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

        log.info("INFO [cfg] ALLOWED_CHAT_IDS=%s", cfg.ALLOWED_CHAT_IDS)
        log.info("INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.PRIMARY_RECIPIENTS)
        log.info("INFO [cfg] PUBLIC_URL='%s' PORT=%s", cfg.PUBLIC_URL, cfg.PORT)
        log.info("INFO [cfg] HEALTH=%ss FIRST=%ss SELF_PING=%ss", cfg.HEALTH_SECONDS, cfg.FIRST_HEALTH_DELAY, cfg.SELF_PING_SECONDS)
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


# ============================ BYBIT API ============================
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
        out: List[str] = []
        cursor = ""
        while True:
            params = {"category": "linear", "cursor": cursor, "limit": 500}
            data = await self._get("/v5/market/instruments-info", params)
            if data.get("retCode") != 0:
                raise RuntimeError(f"Bybit instruments error: {data}")
            result = data.get("result", {}) or {}
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
        candles: List[Dict] = []
        for r in rows:
            candles.append({"t": int(r[0]), "o": float(r[1]), "h": float(r[2]), "l": float(r[3]), "c": float(r[4]), "v": float(r[5])})
        candles.sort(key=lambda x: x["t"])
        return candles

    async def get_oi(self, symbol: str, interval: str = "5min", limit: int = 6) -> List[Tuple[int, float]]:
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval, "limit": limit}
        data = await self._get("/v5/market/open-interest", params)
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit OI error: {data}")
        rows = (data.get("result", {}) or {}).get("list", []) or []
        out: List[Tuple[int, float]] = []
        for r in rows:
            out.append((int(r[0]), float(r[1])))
        out.sort(key=lambda x: x[0])
        return out


# ============================ ИНДИКАТОРЫ ============================
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


# ============================ СИГНАЛ ============================
from dataclasses import dataclass as _dataclass

@_dataclass
class Signal:
    symbol: str
    dir: str  # "LONG"/"SHORT"
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


def _analyze(cfg: Config, symbol: str, candles: List[Dict], oi: List[Tuple[int, float]]) -> Optional[Signal]:
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

    if body <= cfg.BODY_ATR_MULT * atr:
        return None
    if last["v"] <= (cfg.VOL_MULT * vol_sma):
        return None

    direction = "LONG" if last["c"] > last["o"] else "SHORT"

    closes = [c["c"] for c in candles]
    ema20_ser = _ema_series(closes, 20)
    ema20, ema20_prev = ema20_ser[-1], ema20_ser[-2]
    ema_dir = 1 if ema20 > ema20_prev else -1
    dir_sign = 1 if direction == "LONG" else -1

    trend_unit = abs((ema20 - ema20_prev) / max(1e-9, atr))
    trend_strength = max(0.9, min(1.4, 0.9 + trend_unit * 0.6))
    trend_strength *= (1.05 if ema_dir == dir_sign else 0.95)
    trend_strength = max(0.85, min(1.5, trend_strength))

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

    oi_boost = 0.0
    if len(oi) >= 2 and oi[-2][1] > 0:
        oi_delta_pct = (oi[-1][1] - oi[-2][1]) / oi[-2][1] * 100.0
        oi_boost = max(-8.0, min(8.0, 0.6 * oi_delta_pct * dir_sign))

    vol_spike = last["v"] / max(1e-9, vol_sma)
    body_rel = body / max(1e-9, atr)

    prob = 50.0
    prob += 10.0 * math.log2(max(1.0, vol_spike))
    prob += 8.0 * (body_rel / cfg.BODY_ATR_MULT - 1.0)
    prob += 6.0 * (trend_strength - 1.0) * 5.0
    prob += 10.0 * breakout_units
    prob += oi_boost
    prob += (4.0 if ema_dir == dir_sign else -6.0)
    prob = max(0.0, min(99.9, prob))

    if prob < cfg.PROB_MIN or rr < cfg.RR_MIN or take_pct < cfg.PROFIT_MIN_PCT:
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


# ============================ СОСТОЯНИЕ ============================
class State:
    def __init__(self):
        self.total_symbols: List[str] = []
        self.active_symbols: List[str] = []
        self.batch_idx: int = 0
        self.last_signal_sent: Dict[str, float] = {}
        self.live_signals: Dict[str, Signal] = {}
        self.ready: bool = False


# ============================ ХЭНДЛЕРЫ ============================
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


async def err_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error: %s", context.error)


# ============================ ДЖОБЫ ============================
async def job_health(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    for cid in cfg.PRIMARY_RECIPIENTS:
        try:
            await context.bot.send_message(chat_id=cid, text="online")
        except Exception as e:
            log.warning("health send failed: %s", e)


async def job_self_ping(context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.bot_data["cfg"]
    if not cfg.PUBLIC_URL:
        return
    session: aiohttp.ClientSession = context.bot_data.get("aiohttp_session")
    try:
        url = cfg.PUBLIC_URL
        if session and not session.closed:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)):
                pass
        else:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as s:
                async with s.get(url):
                    pass
    except Exception as e:
        log.debug("self-ping error: %s", e)


async def job_rotate(context: ContextTypes.DEFAULT_TYPE):
    st: State = context.bot_data["state"]
    cfg: Config = context.bot_data["cfg"]
    if not st.total_symbols:
        return
    n = cfg.WS_SYMBOLS_MAX
    start = (st.batch_idx * n) % len(st.total_symbols)
    st.active_symbols = [st.total_symbols[(start + i) % len(st.total_symbols)] for i in range(min(n, len(st.total_symbols)))]
    st.batch_idx += 1
    log.info("INFO [universe] rotated: active=%d batch#%d", len(st.active_symbols), st.batch_idx)


async def job_scan(context: ContextTypes.DEFAULT_TYPE):
    st: State = context.bot_data["state"]
    cfg: Config = context.bot_data["cfg"]
    client: BybitClient = context.bot_data.get("bybit")
    if not st.active_symbols or not client:
        return

    candidates: List[Signal] = []
    for sym in st.active_symbols:
        try:
            candles = await client.get_klines(sym, interval="5", limit=300)
            oi = await client.get_oi(sym, interval="5min", limit=6)
            sig = _analyze(cfg, sym, candles, oi)
            if sig:
                candidates.append(sig)
            await asyncio.sleep(0.05)
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
        await asyncio.sleep(0.6)


async def job_bootstrap(context: ContextTypes.DEFAULT_TYPE):
    """Стартовая джоба: создаёт HTTP-сессию, Bybit клиент, грузит вселенную,
    а затем регистрирует остальные джобы."""
    cfg: Config = context.bot_data["cfg"]
    st: State = context.bot_data["state"]

    if context.bot_data.get("bootstrapped"):
        return

    log.info("BOOTSTRAP: start")

    # 1) HTTP-сессия/Bybit
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    context.bot_data["aiohttp_session"] = session
    client = BybitClient(session)
    context.bot_data["bybit"] = client

    # 2) Вселенная
    try:
        all_syms = await client.get_symbols_linear()
        if cfg.UNIVERSE_MODE == "top" and cfg.UNIVERSE_TOP_N > 0:
            st.total_symbols = all_syms[: cfg.UNIVERSE_TOP_N]
        else:
            st.total_symbols = all_syms
        await job_rotate(context)
        st.ready = True
        log.info("INFO [universe] total=%d active=%d mode=%s", len(st.total_symbols), len(st.active_symbols), cfg.UNIVERSE_MODE)
    except Exception as e:
        log.error("universe load failed: %s", e)

    # 3) Регулярные джобы
    jq = context.job_queue
    jq.run_repeating(job_health, interval=cfg.HEALTH_SECONDS, first=cfg.FIRST_HEALTH_DELAY, name="health")
    if cfg.PUBLIC_URL:
        jq.run_repeating(job_self_ping, interval=cfg.SELF_PING_SECONDS, first=cfg.SELF_PING_SECONDS, name="self_ping")
    jq.run_repeating(job_rotate, interval=cfg.ROTATE_MIN * 60, first=cfg.ROTATE_MIN * 60, name="rotate")
    jq.run_repeating(job_scan, interval=30, first=15, name="scan")

    # 4) Уведомление о старте
    for cid in cfg.PRIMARY_RECIPIENTS:
        try:
            await context.bot.send_message(chat_id=cid, text="✅ Bot started. Universe loading done.")
        except Exception:
            pass

    context.bot_data["bootstrapped"] = True
    log.info("BOOTSTRAP: done")


# ============================ HTTP HEALTH ============================
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        # Чтобы Render видел порт — на / тоже отвечаем 200
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"web-ok")

    def log_message(self, fmt, *args):
        return


def _start_health_server(port: int):
    def _run():
        srv = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
        log.info("HTTP health server started on :%d", port)
        srv.serve_forever()

    th = threading.Thread(target=_run, daemon=True)
    th.start()
    return th


# ============================ MAIN ============================
def main():
    cfg = Config.load()

    # Собираем приложение
    application = Application.builder().token(cfg.TELEGRAM_TOKEN).build()
    application.bot_data["cfg"] = cfg
    application.bot_data["state"] = State()

    # Хэндлеры команд
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("universe", cmd_universe))
    application.add_handler(CommandHandler("status", cmd_status))

    # (необязательный) логгер всех апдейтов — поможет понять, доходят ли апдейты
    application.add_handler(MessageHandler(filters.ALL, lambda u, c: log.debug("update: %s", getattr(u, "to_dict", lambda: u)())))

    # Глобальный обработчик ошибок
    application.add_error_handler(err_handler)

    # Планируем bootstrap сразу после старта (внутри уже будет рабочий loop)
    application.job_queue.run_once(job_bootstrap, when=1, name="bootstrap")

    use_webhook = bool(cfg.PUBLIC_URL and cfg.PUBLIC_URL.startswith("https://"))
    if use_webhook:
        webhook_url = cfg.PUBLIC_URL.rstrip("/") + (cfg.WEBHOOK_PATH or _random_path())
        log.info("WEBHOOK mode: %s", webhook_url)
        application.run_webhook(
            listen="0.0.0.0",
            port=cfg.PORT,
            webhook_url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        # Открываем порт, чтобы Render считал сервис живым (и можно смотреть /healthz)
        _start_health_server(cfg.PORT)
        log.info("POLLING mode: starting long-polling …")
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
