# -*- coding: utf-8 -*-
"""
Cryptobot — SCALPING Signals (Bybit V5, USDT Perpetuals)
v11 — Fast RSI + Momentum + Real-time levels (tuned)
       Long polling (deleteWebhook) + WS klines (5m) + Telegram loop
       Telegram error reporting (REPORT_ERRORS_TO_TG=1)
       WS reconnect fix + richer logging
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
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web

# =========================
# БАЗОВЫЙ КОНФИГ
# =========================
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

PORT = int(os.getenv("PORT", "10000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or ""

def _bool_env(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")

ALLOWED_CHAT_IDS = [int(x) for x in (os.getenv("ALLOWED_CHAT_IDS") or "").split(",") if x.strip()]
PRIMARY_RECIPIENTS = [i for i in ALLOWED_CHAT_IDS if i < 0] or ALLOWED_CHAT_IDS[:1] or []
ONLY_CHANNEL = _bool_env("ONLY_CHANNEL", True)

# Вселенная
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "600"))
TURNOVER_MIN_USD = float(os.getenv("TURNOVER_MIN_USD", "2000000"))
VOLUME_MIN_USD   = float(os.getenv("VOLUME_MIN_USD",  "2000000"))
ACTIVE_SYMBOLS   = int(os.getenv("ACTIVE_SYMBOLS",    "60"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT"]

# =========================
# SCALPING КОНФИГ (5m)
# =========================
TF_SCALP = "5"
RSI_PERIOD     = 14
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD",   "40"))
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "60"))
EMA_FAST = 5
EMA_SLOW = 13
VOLUME_SPIKE_MULT = float(os.getenv("VOLUME_SPIKE_MULT", "1.2"))

ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "0.8"))
ATR_TP_MULT = float(os.getenv("ATR_TP_MULT", "1.5"))
TP_MIN_PCT  = float(os.getenv("TP_MIN_PCT",  "0.002"))
TP_MAX_PCT  = float(os.getenv("TP_MAX_PCT",  "0.008"))
RR_MIN      = float(os.getenv("RR_MIN",      "1.8"))
MIN_CONFIRMATIONS = int(os.getenv("MIN_CONFIRMATIONS", "2"))

SIGNAL_COOLDOWN_SEC   = int(os.getenv("SIGNAL_COOLDOWN_SEC",   "20"))
POSITION_COOLDOWN_SEC = int(os.getenv("POSITION_COOLDOWN_SEC", "45"))
KEEPALIVE_SEC         = int(os.getenv("KEEPALIVE_SEC",         str(13*60)))
WATCHDOG_SEC          = int(os.getenv("WATCHDOG_SEC",          "60"))
STALL_EXIT_SEC        = int(os.getenv("STALL_EXIT_SEC",        "600")) # Увеличено для стабильности
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "5"))

REPORT_ERRORS_TO_TG = _bool_env("REPORT_ERRORS_TO_TG", False)
ERROR_REPORT_COOLDOWN_SEC = int(os.getenv("ERROR_REPORT_COOLDOWN_SEC", "180"))

# =========================
# УТИЛИТЫ
# =========================
def now_ms() -> int: return int(time.time() * 1000)
def now_s() -> int: return int(time.time())
def pct(x: float) -> str: return f"{x:.2%}"

def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, force=True)

logger = logging.getLogger("cryptobot.scalp")

async def report_error(app: web.Application, where: str, exc: Optional[BaseException] = None, note: Optional[str] = None) -> None:
    if not REPORT_ERRORS_TO_TG: return
    tg: Optional[Tg] = app.get("tg")
    if not tg: return
    now = now_s()
    last = app.setdefault("_last_error_ts", 0)
    if now - last < ERROR_REPORT_COOLDOWN_SEC: return
    app["_last_error_ts"] = now
    title = f"⚠️ <b>Runtime error</b> @ {html.escape(where)}"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    body = f"\n<b>Note:</b> {html.escape(note)}" if note else ""
    if exc:
        tb = traceback.format_exc()
        tail = "\n".join(tb.strip().splitlines()[-20:])
        body += "\n<pre>" + html.escape(tail[:3500]) + "</pre>"
    text = f"{title}\n🕒 {ts} UTC{body}"
    targets = PRIMARY_RECIPIENTS if ONLY_CHANNEL and PRIMARY_RECIPIENTS else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS)
    for chat_id in targets:
        with contextlib.suppress(Exception): await tg.send(chat_id, text)

# =========================
# ИНДИКАТОРЫ
# =========================
def exponential_moving_average(values: List[float], period: int) -> float:
    if not values: return 0.0
    if len(values) < period: return sum(values) / len(values)
    k = 2.0 / (period + 1.0)
    ema_val = sum(values[:period]) / period
    for price in values[period:]: ema_val = price * k + ema_val * (1 - k)
    return ema_val

def average_true_range(data: List[Tuple[float, float, float, float, float]], period: int) -> float:
    if len(data) < period + 1: return 0.0
    total = 0.0
    for i in range(len(data) - period, len(data)):
        h, l, pc = data[i][1], data[i][2], data[i-1][3]
        total += max(h - l, abs(h - pc), abs(pc - l))
    return total / period

def relative_strength_index_fixed(data: List[Tuple[float, float, float, float, float]], period: int = RSI_PERIOD) -> float:
    if len(data) < period + 1: return 50.0
    closes = [bar[3] for bar in data[-(period+1):]]
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(0, ch)); losses.append(max(0, -ch))
    avg_g, avg_l = sum(gains)/period, sum(losses)/period
    if avg_l == 0: return 100.0
    return 100.0 - (100.0 / (1.0 + avg_g/avg_l))

def momentum_indicator(data: List[Tuple[float, float, float, float, float]], period: int = 5) -> float:
    if len(data) < period + 1: return 0.0
    return (data[-1][3] - data[-(period+1)][3]) / data[-(period+1)][3] * 100.0

def detect_key_levels(data: List[Tuple[float, float, float, float, float]], lookback: int = 20) -> Tuple[float, float]:
    if len(data) < lookback: 
        c = data[-1][3] if data else 0.0
        return c, c
    recent = data[-lookback:]
    return min(b[2] for b in recent), max(b[1] for b in recent)

# =========================
# КЛИЕНТЫ
# =========================
class BybitWS:
    def __init__(self, url: str, http: aiohttp.ClientSession) -> None:
        self.url, self.http = url, http
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.on_message = None
        self._running = False

    async def connect(self) -> None:
        with contextlib.suppress(Exception):
            if self.ws and not self.ws.closed: await self.ws.close()
        self.ws = await self.http.ws_connect(self.url, heartbeat=30)
        logger.info("BybitWS connected")

    async def subscribe(self, topics: List[str]) -> None:
        if not self.ws or self.ws.closed: await self.connect()
        await self.ws.send_json({"op": "subscribe", "args": topics})

    async def run(self) -> None:
        self._running = True
        delay = 1.0
        while self._running:
            try:
                if not self.ws or self.ws.closed: await self.connect()
                async for msg in self.ws:
                    if not self._running: break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if self.on_message:
                            res = self.on_message(data)
                            if asyncio.iscoroutine(res): asyncio.create_task(res)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
            except Exception:
                logger.exception("WS error, reconnecting...")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 30.0)
        logger.info("BybitWS stopped")

    async def stop(self) -> None:
        self._running = False
        if self.ws: await self.ws.close()

class Tg:
    def __init__(self, token: str, http: aiohttp.ClientSession):
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session = http

    async def delete_webhook(self, drop_pending_updates: bool = False):
        url = f"{self.base_url}/deleteWebhook"
        async with self.session.post(url, json={"drop_pending_updates": drop_pending_updates}) as r:
            return await r.json()

    async def get_updates(self, offset: Optional[int] = None, timeout: int = 25) -> List[Dict]:
        url = f"{self.base_url}/getUpdates"
        data = {"timeout": timeout, "offset": offset} if offset else {"timeout": timeout}
        try:
            async with self.session.post(url, json=data, timeout=timeout+5) as r:
                if r.status == 200:
                    res = await r.json()
                    return res.get("result", [])
        except Exception: pass
        return []

    async def send(self, chat_id: Any, text: str) -> bool:
        url = f"{self.base_url}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with self.session.post(url, json=payload) as r:
                return r.status == 200
        except Exception: return False

class BybitRest:
    def __init__(self, base: str, http: aiohttp.ClientSession) -> None:
        self.base, self.http = base.rstrip("/"), http

    async def tickers_linear(self) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/tickers?category=linear"
        async with self.http.get(url, timeout=10) as r:
            return (await r.json()).get("result", {}).get("list", [])

    async def klines(self, category: str, symbol: str, interval: str, limit: int = 200) -> List[Tuple[float, float, float, float, float]]:
        url = f"{self.base}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
        async with self.http.get(url, timeout=10) as r:
            arr = (await r.json()).get("result", {}).get("list", [])
            return [(float(it[1]), float(it[2]), float(it[3]), float(it[4]), float(it[5])) for it in reversed(arr)]

# =========================
# МОДЕЛИ И ЛОГИКА
# =========================
@dataclass
class Position:
    symbol: str; side: str; entry_price: float; stop_loss: float; take_profit: float; entry_time: int

class Market:
    def __init__(self):
        self.symbols: List[str] = []
        self.state: Dict[str, SymbolState] = defaultdict(SymbolState)
        self.last_ws_msg_ts = now_ms()
        self.signal_stats = {"total": 0, "long": 0, "short": 0}
        self.last_signal_sent_ts = 0

@dataclass
class SymbolState:
    k5: List[Tuple[float, float, float, float, float]] = field(default_factory=list)
    rsi_5m: float = 50.0; ema_fast: float = 0.0; ema_slow: float = 0.0; momentum: float = 0.0
    support: float = 0.0; resistance: float = 0.0; atr: float = 0.0
    last_signal_ts: int = 0; last_position_ts: int = 0

class ScalpingEngine:
    def __init__(self, mkt: Market): self.mkt = mkt

    def update_indicators_fast(self, sym: str):
        st = self.mkt.state[sym]
        if len(st.k5) < 20: return
        closes = [b[3] for b in st.k5]
        st.rsi_5m = relative_strength_index_fixed(st.k5)
        st.ema_fast = exponential_moving_average(closes, EMA_FAST)
        st.ema_slow = exponential_moving_average(closes, EMA_SLOW)
        st.momentum = momentum_indicator(st.k5)
        st.support, st.resistance = detect_key_levels(st.k5)
        st.atr = average_true_range(st.k5, 10)

    def generate_scalp_signal(self, sym: str) -> Optional[Dict]:
        st = self.mkt.state[sym]
        if len(st.k5) < 30: return None
        nowt = now_s()
        if nowt - st.last_signal_ts < SIGNAL_COOLDOWN_SEC: return None
        
        price, rsi, mom = st.k5[-1][3], st.rsi_5m, st.momentum
        avg_vol = sum(b[4] for b in st.k5[-6:-1]) / 5.0
        volume_ok = st.k5[-1][4] > avg_vol * VOLUME_SPIKE_MULT if avg_vol > 0 else True

        long_checks = [rsi < RSI_OVERSOLD, mom > -0.5, price > st.ema_fast, volume_ok]
        short_checks = [rsi > RSI_OVERBOUGHT, mom < 0.5, price < st.ema_fast, volume_ok]

        side = "LONG" if sum(long_checks) >= MIN_CONFIRMATIONS else "SHORT" if sum(short_checks) >= MIN_CONFIRMATIONS else None
        if not side: return None

        atr_v = st.atr if st.atr > 0 else price * 0.005
        sl = price - (atr_v * ATR_SL_MULT) if side == "LONG" else price + (atr_v * ATR_SL_MULT)
        tp = price + (atr_v * ATR_TP_MULT) if side == "LONG" else price - (atr_v * ATR_TP_MULT)
        
        rr = abs(tp - price) / max(1e-9, abs(price - sl))
        if rr < RR_MIN: return None

        st.last_signal_ts = nowt
        self.mkt.signal_stats["total"] += 1
        self.mkt.signal_stats[side.lower()] += 1
        return {"symbol": sym, "side": side, "entry": price, "tp": tp, "sl": sl, "rr": rr, "rsi": rsi, "momentum": mom}

# =========================
# ЦИКЛЫ И ОБРАБОТЧИКИ
# =========================
async def ws_on_message(app: web.Application, data: Dict):
    mkt: Market = app["mkt"]
    topic = data.get("topic", "")
    if not topic.startswith("kline."): return
    mkt.last_ws_msg_ts = now_ms()
    
    payload = data.get("data", [])
    if not payload: return
    symbol = topic.split(".")[-1]
    st = mkt.state[symbol]
    
    for p in payload:
        o, h, l, c, v = float(p["open"]), float(p["high"]), float(p["low"]), float(p["close"]), float(p["volume"])
        if p.get("confirm") is False and st.k5: st.k5[-1] = (o, h, l, c, v)
        else:
            st.k5.append((o, h, l, c, v))
            if len(st.k5) > 200: st.k5.pop(0)
            if p.get("confirm") is True:
                app["engine"].update_indicators_fast(symbol)
                sig = app["engine"].generate_scalp_signal(symbol)
                if sig: asyncio.create_task(send_signal(app, sig))

async def send_signal(app: web.Application, sig: Dict):
    text = (f"<b>{sig['side']} | {sig['symbol']}</b> (5m)\n"
            f"📍 Price: {sig['entry']:.5f}\n🎯 TP: {sig['tp']:.5f}\n🛡️ SL: {sig['sl']:.5f}\n"
            f"📊 RSI: {sig['rsi']:.1f} | RR: {sig['rr']:.2f}")
    targets = PRIMARY_RECIPIENTS if ONLY_CHANNEL else ALLOWED_CHAT_IDS
    for cid in targets: await app["tg"].send(cid, text)
    app["mkt"].last_signal_sent_ts = now_ms()

async def tg_loop(app: web.Application):
    tg: Tg = app["tg"]; offset = None
    while True:
        try:
            updates = await tg.get_updates(offset=offset)
            for upd in updates:
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("channel_post")
                if not msg or "text" not in msg: continue
                text = msg["text"]
                cid = msg["chat"]["id"]
                if text == "/ping": await tg.send(cid, "pong")
                elif text == "/status":
                    stats = app["mkt"].signal_stats
                    await tg.send(cid, f"Online. Signals: {stats['total']} (L:{stats['long']} S:{stats['short']})")
        except Exception: await asyncio.sleep(5)

async def watchdog_loop(app: web.Application):
    while True:
        await asyncio.sleep(WATCHDOG_SEC)
        if now_ms() - app["mkt"].last_ws_msg_ts > STALL_EXIT_SEC * 1000:
            logger.error("WS stalled, exiting for restart")
            os._exit(1)

async def keepalive_loop(app: web.Application):
    while True:
        await asyncio.sleep(KEEPALIVE_SEC)
        logger.info(f"Keepalive: WS last msg {(now_ms()-app['mkt'].last_ws_msg_ts)/1000:.1f}s ago")

async def universe_refresh_loop(app: web.Application):
    while True:
        await asyncio.sleep(UNIVERSE_REFRESH_SEC)
        try:
            new_syms = await build_universe_once(app["rest"])
            old_set = set(app["mkt"].symbols)
            to_add = [s for s in new_syms if s not in old_set]
            if to_add:
                await app["ws"].subscribe([f"kline.{TF_SCALP}.{s}" for s in to_add])
                app["mkt"].symbols = new_syms
                logger.info(f"Universe updated: +{len(to_add)} symbols")
        except Exception: logger.exception("Universe refresh error")

async def build_universe_once(rest: BybitRest) -> List[str]:
    try:
        tickers = await rest.tickers_linear()
        pool = [t["symbol"] for t in tickers if t["symbol"].endswith("USDT") and float(t.get("turnover24h",0)) > TURNOVER_MIN_USD]
        res = list(dict.fromkeys(CORE_SYMBOLS + pool))[:ACTIVE_SYMBOLS]
        return res
    except Exception: return CORE_SYMBOLS

# =========================
# СТАРТ ПРИЛОЖЕНИЯ
# =========================
async def on_startup(app: web.Application):
    setup_logging(LOG_LEVEL)
    http = aiohttp.ClientSession()
    app["http"] = http
    app["tg"] = Tg(TELEGRAM_TOKEN, http)
    
    # Очистка очереди сообщений
    await app["tg"].delete_webhook(drop_pending_updates=True)
    logger.info("🚀 Starting Cryptobot SCALP v11 (Queue cleared)")

    app["rest"] = BybitRest(BYBIT_REST, http)
    app["mkt"] = Market()
    app["engine"] = ScalpingEngine(app["mkt"])
    app["ws"] = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: ws_on_message(app, data)

    app["mkt"].symbols = await build_universe_once(app["rest"])
    await app["ws"].connect()
    await app["ws"].subscribe([f"kline.{TF_SCALP}.{s}" for s in app["mkt"].symbols])

    # Запуск фоновых задач
    app["ws_task"] = asyncio.create_task(app["ws"].run())
    app["tg_task"] = asyncio.create_task(tg_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["universe_task"] = asyncio.create_task(universe_refresh_loop(app))

async def on_cleanup(app: web.Application):
    for k in ("ws_task", "tg_task", "watchdog_task", "keepalive_task", "universe_task"):
        if k in app: app[k].cancel()
    if "ws" in app: await app["ws"].stop()
    if "http" in app: await app["http"].close()

async def handle_health(request): return web.Response(text="OK", status=200)

def make_app():
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=PORT)
