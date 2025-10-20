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
...
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
ONLY_CHANNEL = Tr

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
        out: List[Tuple[float, float, float, float, float]] = []
        for it in arr:
            try:
                o = float(it[1]); h = float(it[2]); l = float(it[3]); c = float(it[4]); v = float(it[5])
                out.append((o, h, l, c, v))
            except Exception:
                continue
        return out

# =========================
# Indicators
# =========================
def atr(rows: List[Tuple[float,float,float,float,float]], period: int) -> float:
    if len(rows) < period + 1: return 0.0
    tr_list = []
    for i in range(1, len(rows)):
        prev_close = rows[i-1][3]
        high = rows[i][1]
        low = rows[i][2]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_list.append(tr)
    return sum(tr_list[-period:]) / period

def sma(vals: List[float], period: int) -> float:
    if len(vals) < period:
        return 0.0
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

@dataclass
class Market:
    symbols: List[str] = field(default_factory=list)
    state: Dict[str, SymbolState] = field(default_factory=lambda: defaultdict(SymbolState))
    last_ws_msg_ts: int = 0
    last_signal_sent_ts: int = 0

# =========================
# Bybit WebSocket
# =========================
class BybitWS:
    def __init__(self, url: str, http: aiohttp.ClientSession) -> None:
        self.url = url
        self.http = http
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.on_message = None

    async def connect(self) -> None:
        self.ws = await self.http.ws_connect(self.url)

    async def subscribe(self, topics: List[str]) -> None:
        if not self.ws:
            raise RuntimeError("WebSocket is not connected")
        await self.ws.send_json({
            "op": "subscribe",
            "args": topics
        })

    async def unsubscribe(self, topics: List[str]) -> None:
        if not self.ws:
            raise RuntimeError("WebSocket is not connected")
        await self.ws.send_json({
            "op": "unsubscribe",
            "args": topics
        })

    async def run(self) -> None:
        if not self.ws:
            raise RuntimeError("WebSocket is not connected")
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if self.on_message:
                        # If on_message returns a coroutine, schedule it
                        result = self.on_message(data)
                        if asyncio.iscoroutine(result):
                            asyncio.create_task(result)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {msg}")
                    break
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Error in WebSocket run loop")
        finally:
            if self.ws and not self.ws.closed:
                await self.ws.close()

# =========================
# Telegram
# =========================
class Tg:
    def __init__(self, token: str, http: aiohttp.ClientSession) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = http
...
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    mkt: Market = app["mkt"]; eng: Engine = app["engine"]
    topic = data.get("topic") or ""
    mkt.last_ws_msg_ts = now_ms()
    # 5m KLINE data
    if topic.startswith(f"kline.{EXEC_TF_AUX}."):
        payload = data.get("data") or []
        # process kline data...
        ...
    # other message handling...

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
    if app.get("ws") and app["ws"].ws and not app["ws"].ws.closed:
        await app["ws"].ws.close()
    if app.get("http"):
        await app["http"].close()

async def handle_health(request: web.Request) -> web.Response:
    return web.Response(text="OK")

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
