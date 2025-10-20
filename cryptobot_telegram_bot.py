# -*- coding: utf-8 -*-
"""
Cryptobot — Derivatives Signals (Bybit V5, USDT Perpetuals)
v7.0 — RSI 5m signals + ATR targets + price action confirmations
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
# Config
# =========================
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

PORT = int(os.getenv("PORT", "10000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or ""
ALLOWED_CHAT_IDS = [int(x) for x in (os.getenv("ALLOWED_CHAT_IDS") or "").split(",") if x.strip()]
PRIMARY_RECIPIENTS = [i for i in ALLOWED_CHAT_IDS if i < 0] or ALLOWED_CHAT_IDS[:1] or []
ONLY_CHANNEL = True

# Universe filters
UNIVERSE_REFRESH_SEC = 600
TURNOVER_MIN_USD = float(os.getenv("TURNOVER_MIN_USD", "5000000"))
VOLUME_MIN_USD  = float(os.getenv("VOLUME_MIN_USD",  "5000000"))
ACTIVE_SYMBOLS  = int(os.getenv("ACTIVE_SYMBOLS",     "60"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "TONUSDT"]

# Timeframes
EXEC_TF_MAIN = "15"
EXEC_TF_AUX  = "5"
CONTEXT_TF   = "60"

# Indicators / thresholds
ATR_PERIOD_15 = int(os.getenv("ATR_PERIOD_15", "14"))
VOL_SMA_15    = int(os.getenv("VOL_SMA_15",    "20"))
VOLUME_SPIKE_MULT = float(os.getenv("VOLUME_SPIKE_MULT", "2.0"))

# TP/SL and risk
ATR_SL_MULT  = float(os.getenv("ATR_SL_MULT",  "1.0"))
ATR_TP_MULT  = float(os.getenv("ATR_TP_MULT",  "1.5"))
TP_MIN_PCT   = float(os.getenv("TP_MIN_PCT",   "0.01"))
TP_MAX_PCT   = float(os.getenv("TP_MAX_PCT",   "0.015"))
RR_MIN       = float(os.getenv("RR_MIN",       "1.5"))

# Anti-spam / reliability
SIGNAL_COOLDOWN_SEC = int(os.getenv("SIGNAL_COOLDOWN_SEC", "30"))
KEEPALIVE_SEC       = 13 * 60
WATCHDOG_SEC        = 60
STALL_EXIT_SEC      = int(os.getenv("STALL_EXIT_SEC", "240"))

# =========================
# Utilities / Logging
# =========================
def now_ms() -> int:
    return int(time.time() * 1000)

def pct(x: float) -> str:
    return f"{x:.2%}"

def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        format=fmt, force=True)

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

    async def klines(self, category: str, symbol: str, interval: str, limit: int = 200) -> List[Tuple[float, float, float, float, float]]:
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

# =========================
# Indicators
# =========================
def atr(rows: List[Tuple[float,float,float,float,float]], period: int) -> float:
    if len(rows) < period + 1: return 0.0
    s = 0.0
    for i in range(1, period+1):
        _, h, l, c, _ = rows[-i]
        _, _, _, pc, _ = rows[-i-1]
        tr = max(h-l, abs(h-pc), abs(pc-l))
        s += tr
    return s / period

def sma(vals: List[float], period: int) -> float:
    if len(vals) < period or period <= 0: return 0.0
    return sum(vals[-period:]) / period

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
# Market data / state
# =========================
@dataclass
class SymbolState:
    k15: List[Tuple[float,float,float,float,float]] = field(default_factory=list)
    k5:  List[Tuple[float,float,float,float,float]] = field(default_factory=list)
    k60: List[Tuple[float,float,float,float,float]] = field(default_factory=list)
    funding_rate: float = 0.0
    next_funding_ms: Optional[int] = None
    oi_points: deque = field(default_factory=lambda: deque(maxlen=120))
    liq_events: deque = field(default_factory=lambda: deque(maxlen=10000))
    last_signal_ts: int = 0
    cooldown_ts: Dict[str, int] = field(default_factory=dict)

class Market:
    def __init__(self):
        self.symbols: List[str] = []
        self.state: Dict[str, SymbolState] = defaultdict(SymbolState)
        self.last_ws_msg_ts: int = now_ms()
        self.last_signal_sent_ts: int = 0

# =========================
# Signal Engine (5m RSI)
# =========================
class Engine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def on_5m_close(self, sym: str) -> Optional[Dict[str, Any]]:
        st = self.mkt.state[sym]
        K5 = st.k5
        # require enough bars for RSI and confirmations
        if len(K5) < 30:
            return None
        prev_rsi = rsi14(K5[:-1])
        curr_rsi = rsi14(K5)
        long_signal = prev_rsi < 30 <= curr_rsi
        short_signal = prev_rsi > 70 >= curr_rsi
        if not long_signal and not short_signal:
            return None
        side = "LONG" if long_signal else "SHORT"
        # cooldown
        now_s = int(time.time())
        last_ts = st.cooldown_ts.get(side, 0)
        if now_s - last_ts < SIGNAL_COOLDOWN_SEC:
            return None
        confirms = []
        # extreme of 30 bars
        if side == "LONG":
            low30 = min(r[2] for r in K5[-30:])
            if K5[-1][2] <= low30:
                confirms.append("касание 30-св. минимума")
        else:
            high30 = max(r[1] for r in K5[-30:])
            if K5[-1][1] >= high30:
                confirms.append("касание 30-св. максимума")
        # candle pattern
        o,h,l,c,v = K5[-1]
        if side == "LONG":
            body = c - o
            lower_wick = o - l
            upper_wick = h - c
            # hammer
            if c > o and lower_wick >= 2 * body and upper_wick <= 0.5 * body:
                confirms.append("паттерн: молот")
            else:
                # bullish engulfing
                o_prev, h_prev, l_prev, c_prev, _ = K5[-2]
                if c_prev < o_prev and o <= c_prev and c >= o_prev:
                    confirms.append("паттерн: бычье поглощение")
        else:
            body = o - c
            upper_wick = h - o
            lower_wick = c - l
            # shooting star (pin-bar)
            if o > c and upper_wick >= 2 * body and lower_wick <= 0.5 * body:
                confirms.append("паттерн: пин-бар")
            else:
                # bearish engulfing
                o_prev, h_prev, l_prev, c_prev, _ = K5[-2]
                if c_prev > o_prev and o >= c_prev and c <= o_prev:
                    confirms.append("паттерн: медвежье поглощение")
        # volume spike
        if len(K5) >= VOL_SMA_15+1:
            avg_vol = sma([r[4] for r in K5[-(VOL_SMA_15+1):-1]], VOL_SMA_15)
            if avg_vol > 0 and v >= 2 * avg_vol:
                confirms.append("спайк объема")
        # RSI divergence (optional)
        if confirms:
            if side == "LONG":
                lows_idx = [i for i in range(len(K5)-30, len(K5)-1)]
                if lows_idx:
                    prev_low_idx = min(lows_idx, key=lambda i: K5[i][2])
                    rsi_prev_low = rsi14(K5[:prev_low_idx+1])
                    if curr_rsi > rsi_prev_low:
                        confirms.append("бычья дивергенция RSI")
            else:
                highs_idx = [i for i in range(len(K5)-30, len(K5)-1)]
                if highs_idx:
                    prev_high_idx = max(highs_idx, key=lambda i: K5[i][1])
                    rsi_prev_high = rsi14(K5[:prev_high_idx+1])
                    if curr_rsi < rsi_prev_high:
                        confirms.append("медвежья дивергенция RSI")
        if len(confirms) < 2:
            return None
        strength = "сильный" if len(confirms) >= 3 else "слабый"
        # ATR-based SL/TP
        entry_price = K5[-1][3]
        atr5 = atr(K5, ATR_PERIOD_15)
        if atr5 == 0:
            return None
        if side == "LONG":
            sl_price = entry_price - ATR_SL_MULT * atr5
            risk = entry_price - sl_price
            tp_price = entry_price + ATR_TP_MULT * atr5
        else:
            sl_price = entry_price + ATR_SL_MULT * atr5
            risk = sl_price - entry_price
            tp_price = entry_price - ATR_TP_MULT * atr5
        # enforce min/max TP% and RR
        tp_pct = abs(tp_price - entry_price) / entry_price
        if tp_pct < TP_MIN_PCT:
            tp_price = entry_price * (1 + (TP_MIN_PCT if side=="LONG" else -TP_MIN_PCT))
        if tp_pct > TP_MAX_PCT:
            tp_price = entry_price * (1 + (TP_MAX_PCT if side=="LONG" else -TP_MAX_PCT))
        rr = abs(tp_price - entry_price) / max(1e-9, risk)
        if rr < RR_MIN:
            return None
        # update cooldown
        st.cooldown_ts[side] = now_s
        return {
            "symbol": sym,
            "side": side,
            "entry": float(entry_price),
            "sl": float(sl_price),
            "tp": float(tp_price),
            "strength": strength,
            "rsi": round(curr_rsi, 2),
            "confirms": confirms
        }

# =========================
# Signal formatting
# =========================
def fmt_signal(sig: Dict[str, Any]) -> str:
    sym = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; sl = sig["sl"]; tp = sig["tp"]
    strength = sig.get("strength")
    rsi_val = sig.get("rsi")
    confirms = sig.get("confirms", [])
    emoji = "🟢" if side=="LONG" else "🔴"
    lines = [
        f"{emoji} <b>Сигнал {side} по {sym}</b>",
        f"Вход: <b>{entry:g}</b>",
        f"Стоп-Лосс: <b>{sl:g}</b>",
        f"Тейк-Профит: <b>{tp:g}</b> ({pct((tp - entry)/entry) if entry else ''})",
        f"Сила сигнала: <b>{strength}</b>",
        f"RSI(14): <b>{rsi_val:.2f}</b>",
        "Подтверждения:"
    ]
    for c in confirms:
        lines.append(f"• {c}")
    return "\n".join([x for x in lines if x])

# =========================
# Telegram bot commands
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
                if not msg:
                    continue
                chat_id = msg.get("chat", {}).get("id")
                text = (msg.get("text") or "").strip()
                if not isinstance(chat_id, int) or not text.startswith("/"):
                    continue
                if chat_id not in ALLOWED_CHAT_IDS and chat_id not in PRIMARY_RECIPIENTS:
                    continue
                cmd = text.split()[0].lower()
                if cmd == "/ping":
                    ago = (now_ms() - mkt.last_ws_msg_ts)/1000.0
                    await tg.send(chat_id, f"pong • WS last msg {ago:.1f}s ago • symbols={len(mkt.symbols)}")
                elif cmd == "/status":
                    silent_line = "—" if mkt.last_signal_sent_ts == 0 else f"{(now_ms()-mkt.last_signal_sent_ts)/60000.0:.1f}m"
                    await tg.send(chat_id,
                        "✅ Online\n"
                        f"Symbols: {len(mkt.symbols)}\n"
                        f"Mode: RSI 5m signals\n"
                        f"TP≥{pct(TP_MIN_PCT)} • RR≥{RR_MIN:.2f}\n"
                        f"Silent (signals): {silent_line}")
                elif cmd == "/help":
                    await tg.send(chat_id,
                        "Команды:\n"
                        "/ping — пинг\n"
                        "/status — статус\n"
                        "/diag — диагностика буферов и метрик\n"
                        "/jobs — фоновые задачи\n")
                elif cmd == "/jobs":
                    jobs = []
                    for k in ("ws_task", "keepalive_task", "watchdog_task", "tg_task", "universe_task"):
                        t = app.get(k)
                        jobs.append(f"{k}: {'running' if (t and not t.done()) else 'stopped'}")
                    await tg.send(chat_id, "Jobs:\n" + "\n".join(jobs))
                elif cmd == "/diag":
                    k5_pts = sum(len(mkt.state[s].k5) for s in mkt.symbols)
                    ago = (now_ms() - mkt.last_ws_msg_ts)/1000.0
                    silent_line = "—" if mkt.last_signal_sent_ts == 0 else f"{(now_ms()-mkt.last_signal_sent_ts)/60000.0:.1f}m"
                    head = ", ".join(mkt.symbols[:10]) if mkt.symbols else "—"
                    await tg.send(chat_id,
                        "Diag:\n"
                        f"WS last msg: {ago:.1f}s ago\n"
                        f"Symbols: {len(mkt.symbols)} (head: {head})\n"
                        f"Kline buffer: 5m={k5_pts}\n"
                        f"Silent (signals): {silent_line}")
                else:
                    await tg.send(chat_id, "Неизвестная команда. /help")
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("tg_loop error")
            await asyncio.sleep(2)

# =========================
# WS message handler
# =========================
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    mkt: Market = app["mkt"]; eng: Engine = app["engine"]
    topic = data.get("topic") or ""
    mkt.last_ws_msg_ts = now_ms()
    # 5m KLINE data
    if topic.startswith(f"kline.{EXEC_TF_AUX}."):
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
                    if len(st.k5) > 900:
                        st.k5 = st.k5[-900:]
                # On 5m close, generate signal
                if p.get("confirm") is True:
                    sig = eng.on_5m_close(sym)
                    if sig:
                        text = fmt_signal(sig)
                        targets = PRIMARY_RECIPIENTS if ONLY_CHANNEL else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS)
                        for chat_id in targets:
                            with contextlib.suppress(Exception):
                                await app["tg"].send(chat_id, text)
                        mkt.last_signal_sent_ts = now_ms()
                        mkt.state[sym].last_signal_ts = now_ms()
            try:
                if len(st.k5) % 50 == 0:
                    logger.info(f"[DATA] {sym} 5m buffer: {len(st.k5)} candles")
            except Exception:
                pass

# =========================
# Background tasks
# =========================
async def keepalive_loop(app: web.Application) -> None:
    public_url = os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL")
    http: aiohttp.ClientSession = app["http"]
    if not public_url:
        return
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

async def build_universe_once(rest: BybitRest) -> List[str]:
    """Build the universe of active symbols (fallback to CORE_SYMBOLS if needed)."""
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
                    if spot:
                        verified.append(s)
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
                        args += [f"kline.5.{s}"]
                    await ws.unsubscribe(args)
                if add:
                    args = []
                    for s in add:
                        args += [f"kline.5.{s}"]
                    await ws.subscribe(args)
                    logger.info(f"[WS] Subscribed to {len(args)} topics for {len(add)} symbols")
                mkt.symbols = symbols_new
                logger.info(f"[universe] +{len(add)} / -{len(rem)} • total={len(mkt.symbols)}")
        except Exception:
            logger.exception("universe_refresh_loop error")
        except asyncio.CancelledError:
            break

async def handle_health(request: web.Request) -> web.Response:
    mkt: Market = request.app["mkt"]
    return web.json_response({
        "ok": True,
        "symbols": mkt.symbols,
        "last_ws_msg_age_sec": int((now_ms() - mkt.last_ws_msg_ts)/1000),
    })

async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    if not TELEGRAM_TOKEN:
        raise RuntimeError("Не задан TELEGRAM_TOKEN")
    http = aiohttp.ClientSession()
    app["http"] = http
    app["tg"] = Tg(TELEGRAM_TOKEN, http)
    app["rest"] = BybitRest(BYBIT_REST, http)
    app["mkt"] = Market()
    app["engine"] = Engine(app["mkt"])
    app["ws"] = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: asyncio.create_task(ws_on_message(app, data))
    # Build initial universe and subscribe
    symbols = await build_universe_once(app["rest"])
    app["mkt"].symbols = symbols
    logger.info(f"symbols: {symbols}")
    await app["ws"].connect()
    args = []
    for s in symbols:
        args += [f"kline.5.{s}"]
    if args:
        await app["ws"].subscribe(args)
        logger.info(f"[WS] Initial subscribed to {len(args)} topics for {len(symbols)} symbols")
    else:
        fallback = CORE_SYMBOLS[:]
        app["mkt"].symbols = fallback
        fargs = []
        for s in fallback:
            fargs += [f"kline.5.{s}"]
        await app["ws"].subscribe(fargs)
        logger.info(f"[WS] Fallback subscribed to {len(fargs)} topics for {len(fallback)} symbols")
    # Start background tasks
    app["ws_task"] = asyncio.create_task(app["ws"].run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_task"] = asyncio.create_task(tg_loop(app))
    app["universe_task"] = asyncio.create_task(universe_refresh_loop(app))
    # Notify startup
    try:
        for chat_id in PRIMARY_RECIPIENTS or ALLOWED_CHAT_IDS:
            await app["tg"].send(chat_id, f"🟢 Cryptobot v7.0: RSI 5m signals + ATR targets")
    except Exception:
        logger.warning("startup notify failed")

async def on_cleanup(app: web.Application) -> None:
    for k in ("ws_task", "keepalive_task", "watchdog_task", "tg_task", "universe_task"):
        t = app.get(k)
        if t:
            t.cancel()
            with contextlib.suppress(Exception):
                await t
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
    logger.info("Starting Cryptobot v7.0 — RSI 5m signals + ATR targets")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
