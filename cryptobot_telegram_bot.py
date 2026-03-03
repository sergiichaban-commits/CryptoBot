# -*- coding: utf-8 -*-
"""
Cryptobot — SCALPING Signals (Bybit V5, USDT Perpetuals)
v12 — Fixes over v11:
  1. Pre-load historical klines on startup (no more 2.5h silence after restart)
  2. Fixed signal conditions (removed RSI/EMA contradiction)
  3. Realistic momentum thresholds
  4. TP_MIN_PCT / TP_MAX_PCT now actually applied
  5. Batched WS subscribe (max 10 topics per message — Bybit limit)
  6. Parallel kline preload with semaphore (fast startup)
  7. Improved /status command
  8. Signal log to console
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

# Вселенная символов
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "600"))
TURNOVER_MIN_USD     = float(os.getenv("TURNOVER_MIN_USD", "2000000"))
ACTIVE_SYMBOLS       = int(os.getenv("ACTIVE_SYMBOLS", "60"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
                "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT"]

# =========================
# SCALPING КОНФИГ (5m)
# =========================
TF_SCALP       = "5"
RSI_PERIOD     = 14
# FIX v12: расширены зоны (было 40/60 — слишком близко к нейтральным 50)
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD",   "35"))
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "65"))
EMA_FAST       = 5
EMA_SLOW       = 13

VOLUME_SPIKE_MULT = float(os.getenv("VOLUME_SPIKE_MULT", "1.2"))

ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "0.8"))
ATR_TP_MULT = float(os.getenv("ATR_TP_MULT", "1.5"))
TP_MIN_PCT  = float(os.getenv("TP_MIN_PCT",  "0.002"))
TP_MAX_PCT  = float(os.getenv("TP_MAX_PCT",  "0.012"))
# FIX v12: снижен RR_MIN (1.8 было слишком строго для 5m ATR)
RR_MIN            = float(os.getenv("RR_MIN",      "1.5"))
MIN_CONFIRMATIONS = int(os.getenv("MIN_CONFIRMATIONS", "2"))

# FIX v12: SIGNAL_COOLDOWN увеличен — 20 сек было слишком мало
SIGNAL_COOLDOWN_SEC   = int(os.getenv("SIGNAL_COOLDOWN_SEC",   "300"))
POSITION_COOLDOWN_SEC = int(os.getenv("POSITION_COOLDOWN_SEC", "45"))
KEEPALIVE_SEC         = int(os.getenv("KEEPALIVE_SEC",          str(13 * 60)))
WATCHDOG_SEC          = int(os.getenv("WATCHDOG_SEC",           "60"))
STALL_EXIT_SEC        = int(os.getenv("STALL_EXIT_SEC",         "600"))
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS",          "5"))

# FIX v12: предзагрузка истории
PRELOAD_BARS    = int(os.getenv("PRELOAD_BARS",    "100"))
PRELOAD_WORKERS = int(os.getenv("PRELOAD_WORKERS", "10"))

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
def exponential_moving_average(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    k       = 2.0 / (period + 1.0)
    ema_val = sum(values[:period]) / period
    for price in values[period:]:
        ema_val = price * k + ema_val * (1 - k)
    return ema_val


def average_true_range(
    data: List[Tuple[float, float, float, float, float]], period: int
) -> float:
    if len(data) < period + 1:
        return 0.0
    total = 0.0
    for i in range(len(data) - period, len(data)):
        h, l, pc = data[i][1], data[i][2], data[i - 1][3]
        total += max(h - l, abs(h - pc), abs(pc - l))
    return total / period


def relative_strength_index_fixed(
    data: List[Tuple[float, float, float, float, float]], period: int = RSI_PERIOD
) -> float:
    if len(data) < period + 1:
        return 50.0
    closes        = [bar[3] for bar in data[-(period + 1):]]
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(0, ch))
        losses.append(max(0, -ch))
    avg_g, avg_l = sum(gains) / period, sum(losses) / period
    if avg_l == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + avg_g / avg_l))


def momentum_indicator(
    data: List[Tuple[float, float, float, float, float]], period: int = 5
) -> float:
    if len(data) < period + 1:
        return 0.0
    return (data[-1][3] - data[-(period + 1)][3]) / data[-(period + 1)][3] * 100.0


def detect_key_levels(
    data: List[Tuple[float, float, float, float, float]], lookback: int = 20
) -> Tuple[float, float]:
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
        self._running   = False

    async def connect(self) -> None:
        with contextlib.suppress(Exception):
            if self.ws and not self.ws.closed:
                await self.ws.close()
        self.ws = await self.http.ws_connect(self.url, heartbeat=30)
        logger.info("BybitWS connected")

    async def subscribe(self, topics: List[str]) -> None:
        """FIX v12: Bybit принимает максимум 10 топиков за раз."""
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

    async def klines(
        self, category: str, symbol: str, interval: str, limit: int = 200
    ) -> List[Tuple[float, float, float, float, float]]:
        url = (f"{self.base}/v5/market/kline"
               f"?category={category}&symbol={symbol}&interval={interval}&limit={limit}")
        async with self.http.get(url, timeout=10) as r:
            arr = (await r.json()).get("result", {}).get("list", [])
            return [(float(it[1]), float(it[2]), float(it[3]), float(it[4]), float(it[5]))
                    for it in reversed(arr)]


# =========================
# МОДЕЛИ И ЛОГИКА
# =========================
@dataclass
class Position:
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_time: int


class Market:
    def __init__(self):
        self.symbols: List[str]              = []
        self.state:   Dict[str, SymbolState] = defaultdict(SymbolState)
        self.last_ws_msg_ts      = now_ms()
        self.signal_stats        = {"total": 0, "long": 0, "short": 0}
        self.last_signal_sent_ts = 0


@dataclass
class SymbolState:
    k5:         List[Tuple[float, float, float, float, float]] = field(default_factory=list)
    rsi_5m:     float = 50.0
    ema_fast:   float = 0.0
    ema_slow:   float = 0.0
    momentum:   float = 0.0
    support:    float = 0.0
    resistance: float = 0.0
    atr:        float = 0.0
    last_signal_ts:   int = 0
    last_position_ts: int = 0


class ScalpingEngine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def update_indicators_fast(self, sym: str) -> None:
        st = self.mkt.state[sym]
        if len(st.k5) < 20:
            return
        closes      = [b[3] for b in st.k5]
        st.rsi_5m   = relative_strength_index_fixed(st.k5)
        st.ema_fast = exponential_moving_average(closes, EMA_FAST)
        st.ema_slow = exponential_moving_average(closes, EMA_SLOW)
        st.momentum = momentum_indicator(st.k5)
        st.support, st.resistance = detect_key_levels(st.k5)
        st.atr      = average_true_range(st.k5, 10)

    def generate_scalp_signal(self, sym: str) -> Optional[Dict]:
        st = self.mkt.state[sym]
        if len(st.k5) < 30:
            return None
        nowt = now_s()
        if nowt - st.last_signal_ts < SIGNAL_COOLDOWN_SEC:
            return None

        price = st.k5[-1][3]
        rsi   = st.rsi_5m
        mom   = st.momentum

        avg_vol   = sum(b[4] for b in st.k5[-6:-1]) / 5.0 if len(st.k5) >= 6 else 0.0
        volume_ok = (st.k5[-1][4] > avg_vol * VOLUME_SPIKE_MULT) if avg_vol > 0 else True

        # ── FIX v12: исправленная логика сигнала ──────────────────────────
        # v11 имел противоречие: RSI<40 (цена падает) + price>EMA_fast (бычий)
        # При RSI<35 цена почти всегда НИЖЕ EMA5 — условия взаимоисключали друг друга.
        #
        # Новая логика — разворот от уровней:
        #   LONG:  перепродан + импульс разворачивается вверх
        #          + (EMA бычий ИЛИ цена у поддержки)
        #   SHORT: перекуплен + импульс разворачивается вниз
        #          + (EMA медвежий ИЛИ цена у сопротивления)
        # ──────────────────────────────────────────────────────────────────
        near_support     = (st.support > 0)    and (price <= st.support    * 1.005)
        near_resistance  = (st.resistance > 0) and (price >= st.resistance * 0.995)
        ema_bullish      = st.ema_fast >= st.ema_slow
        ema_bearish      = st.ema_fast <= st.ema_slow
        mom_turning_up   = mom >  0.05
        mom_turning_down = mom < -0.05

        long_checks = [
            rsi < RSI_OVERSOLD,
            mom_turning_up,
            ema_bullish or near_support,
            volume_ok,
        ]
        short_checks = [
            rsi > RSI_OVERBOUGHT,
            mom_turning_down,
            ema_bearish or near_resistance,
            volume_ok,
        ]

        if   sum(long_checks)  >= MIN_CONFIRMATIONS:
            side = "LONG"
        elif sum(short_checks) >= MIN_CONFIRMATIONS:
            side = "SHORT"
        else:
            return None

        atr_v = st.atr if st.atr > 0 else price * 0.005
        sl = price - atr_v * ATR_SL_MULT if side == "LONG" else price + atr_v * ATR_SL_MULT
        tp = price + atr_v * ATR_TP_MULT if side == "LONG" else price - atr_v * ATR_TP_MULT

        # FIX v12: применяем TP_MIN_PCT / TP_MAX_PCT (в v11 были объявлены но не использовались)
        raw_tp_pct = abs(tp - price) / price
        if raw_tp_pct < TP_MIN_PCT:
            tp = (price + price * TP_MIN_PCT) if side == "LONG" else (price - price * TP_MIN_PCT)
        elif raw_tp_pct > TP_MAX_PCT:
            tp = (price + price * TP_MAX_PCT) if side == "LONG" else (price - price * TP_MAX_PCT)

        rr = abs(tp - price) / max(1e-9, abs(price - sl))
        if rr < RR_MIN:
            return None

        st.last_signal_ts = nowt
        self.mkt.signal_stats["total"] += 1
        self.mkt.signal_stats[side.lower()] += 1

        checks_str = f"{sum(long_checks)}/4" if side == "LONG" else f"{sum(short_checks)}/4"
        return {
            "symbol":   sym,
            "side":     side,
            "entry":    price,
            "tp":       tp,
            "sl":       sl,
            "rr":       rr,
            "rsi":      rsi,
            "momentum": mom,
            "checks":   checks_str,
        }


# =========================
# FIX v12: ПРЕДЗАГРУЗКА ИСТОРИИ
# =========================
async def preload_symbol(
    rest: BybitRest, mkt: Market, engine: ScalpingEngine,
    sym: str, sem: asyncio.Semaphore,
) -> None:
    """Загружаем исторические klines через REST.
    Без этого бот молчит ~2.5 часа после каждого рестарта."""
    async with sem:
        try:
            bars = await rest.klines("linear", sym, TF_SCALP, limit=PRELOAD_BARS)
            if bars:
                mkt.state[sym].k5 = list(bars[-200:])
                engine.update_indicators_fast(sym)
        except Exception as e:
            logger.warning(f"Preload failed {sym}: {e}")


async def preload_all(rest: BybitRest, mkt: Market, engine: ScalpingEngine) -> int:
    sem   = asyncio.Semaphore(PRELOAD_WORKERS)
    tasks = [preload_symbol(rest, mkt, engine, sym, sem) for sym in mkt.symbols]
    logger.info(f"Preloading {len(tasks)} symbols (workers={PRELOAD_WORKERS})...")
    await asyncio.gather(*tasks)
    ready = sum(1 for sym in mkt.symbols if len(mkt.state[sym].k5) >= 30)
    logger.info(f"Preload complete: {ready}/{len(mkt.symbols)} symbols ready")
    return ready


# =========================
# ЦИКЛЫ И ОБРАБОТЧИКИ
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
    symbol = topic.split(".")[-1]
    st     = mkt.state[symbol]

    for p in payload:
        o, h, l, c, v = (float(p["open"]), float(p["high"]), float(p["low"]),
                         float(p["close"]), float(p["volume"]))
        if p.get("confirm") is False and st.k5:
            st.k5[-1] = (o, h, l, c, v)
        else:
            st.k5.append((o, h, l, c, v))
            if len(st.k5) > 200:
                st.k5.pop(0)
            if p.get("confirm") is True:
                app["engine"].update_indicators_fast(symbol)
                sig = app["engine"].generate_scalp_signal(symbol)
                if sig:
                    asyncio.create_task(send_signal(app, sig))


async def send_signal(app: web.Application, sig: Dict) -> None:
    side_emoji = "🟢" if sig["side"] == "LONG" else "🔴"
    text = (
        f"{side_emoji} <b>{sig['side']} | {sig['symbol']}</b> (5m)\n"
        f"📍 Entry:  <code>{sig['entry']:.5f}</code>\n"
        f"🎯 TP:     <code>{sig['tp']:.5f}</code>\n"
        f"🛡 SL:     <code>{sig['sl']:.5f}</code>\n"
        f"📊 RSI: {sig['rsi']:.1f}  |  Mom: {sig['momentum']:+.2f}%  |  RR: {sig['rr']:.2f}\n"
        f"✅ Подтверждений: {sig['checks']}"
    )
    targets = PRIMARY_RECIPIENTS if ONLY_CHANNEL else ALLOWED_CHAT_IDS
    for cid in targets:
        await app["tg"].send(cid, text)
    app["mkt"].last_signal_sent_ts = now_ms()
    logger.info(
        f"Signal: {sig['side']} {sig['symbol']} "
        f"entry={sig['entry']:.5f} tp={sig['tp']:.5f} sl={sig['sl']:.5f} rr={sig['rr']:.2f}"
    )


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
                    mkt   = app["mkt"]
                    stats = mkt.signal_stats
                    ready = sum(1 for s in mkt.symbols if len(mkt.state[s].k5) >= 30)
                    last_ago = (
                        f"{int((now_ms() - mkt.last_signal_sent_ts) / 1000)}s назад"
                        if mkt.last_signal_sent_ts else "ещё не было"
                    )
                    await tg.send(cid, (
                        f"✅ <b>Cryptobot SCALP v12</b>\n"
                        f"Символов: {ready}/{len(mkt.symbols)} готовы\n"
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
        logger.info(f"Keepalive: WS last msg {(now_ms() - app['mkt'].last_ws_msg_ts) / 1000:.1f}s ago")


async def universe_refresh_loop(app: web.Application) -> None:
    while True:
        await asyncio.sleep(UNIVERSE_REFRESH_SEC)
        try:
            new_syms = await build_universe_once(app["rest"])
            old_set  = set(app["mkt"].symbols)
            to_add   = [s for s in new_syms if s not in old_set]
            if to_add:
                sem = asyncio.Semaphore(PRELOAD_WORKERS)
                await asyncio.gather(*[
                    preload_symbol(app["rest"], app["mkt"], app["engine"], sym, sem)
                    for sym in to_add
                ])
                await app["ws"].subscribe([f"kline.{TF_SCALP}.{s}" for s in to_add])
                app["mkt"].symbols = new_syms
                logger.info(f"Universe updated: +{len(to_add)} symbols")
        except Exception:
            logger.exception("Universe refresh error")


async def build_universe_once(rest: BybitRest) -> List[str]:
    try:
        tickers = await rest.tickers_linear()
        pool = [
            t["symbol"] for t in tickers
            if t["symbol"].endswith("USDT") and float(t.get("turnover24h", 0)) > TURNOVER_MIN_USD
        ]
        return list(dict.fromkeys(CORE_SYMBOLS + pool))[:ACTIVE_SYMBOLS]
    except Exception:
        return list(CORE_SYMBOLS)


# =========================
# СТАРТ ПРИЛОЖЕНИЯ
# =========================
async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    http        = aiohttp.ClientSession()
    app["http"] = http
    app["tg"]   = Tg(TELEGRAM_TOKEN, http)

    await app["tg"].delete_webhook(drop_pending_updates=True)
    logger.info("🚀 Starting Cryptobot SCALP v12")

    app["rest"]   = BybitRest(BYBIT_REST, http)
    app["mkt"]    = Market()
    app["engine"] = ScalpingEngine(app["mkt"])
    app["ws"]     = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: ws_on_message(app, data)

    # 1. Строим список символов
    app["mkt"].symbols = await build_universe_once(app["rest"])

    # 2. FIX v12: предзагружаем историю — без этого сигналов нет 2.5ч после рестарта
    ready = await preload_all(app["rest"], app["mkt"], app["engine"])

    # 3. WebSocket
    await app["ws"].connect()
    await app["ws"].subscribe([f"kline.{TF_SCALP}.{s}" for s in app["mkt"].symbols])

    # 4. Фоновые задачи
    app["ws_task"]        = asyncio.create_task(app["ws"].run())
    app["tg_task"]        = asyncio.create_task(tg_loop(app))
    app["watchdog_task"]  = asyncio.create_task(watchdog_loop(app))
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["universe_task"]  = asyncio.create_task(universe_refresh_loop(app))

    # 5. Уведомление о старте
    try:
        targets = PRIMARY_RECIPIENTS if PRIMARY_RECIPIENTS else (ALLOWED_CHAT_IDS[:1] if ALLOWED_CHAT_IDS else [])
        for chat_id in targets:
            await app["tg"].send(chat_id, (
                "🟢 <b>Cryptobot SCALP v12 Online</b>\n"
                f"• Таймфрейм: {TF_SCALP}m\n"
                f"• Символов: {len(app['mkt'].symbols)} ({ready} готовы к сигналам)\n"
                f"• RSI зоны: &lt;{RSI_OVERSOLD} long / &gt;{RSI_OVERBOUGHT} short\n"
                f"• RR мин: {RR_MIN}  |  Cooldown: {SIGNAL_COOLDOWN_SEC}s\n"
                "• История: предзагружена ✅\n"
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
