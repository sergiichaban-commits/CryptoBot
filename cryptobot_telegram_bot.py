# -*- coding: utf-8 -*-
"""
Cryptobot — SCALPING Signals (Bybit V5, USDT Perpetuals)
v10.4 — Stabilized Telegram long-polling (webhook purge + backoff + ACK),
        de-dup of signals, safe sender, no banner spam, WS watchdog.
        Strategy: 5m RSI re-entry + Momentum + EMA + Levels.

Author: Sergii Chaban & ChatGPT
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
ONLY_CHANNEL = _bool_env("ONLY_CHANNEL", True)  # по умолчанию выводим только в канал(ы)

# Вселенная
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "600"))
TURNOVER_MIN_USD = float(os.getenv("TURNOVER_MIN_USD", "2000000"))
VOLUME_MIN_USD   = float(os.getenv("VOLUME_MIN_USD",  "2000000"))
ACTIVE_SYMBOLS   = int(os.getenv("ACTIVE_SYMBOLS",    "40"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT"]

# =========================
# SCALPING КОНФИГ (5m)
# =========================
TF_SCALP = "5"

# RSI и пороги
RSI_PERIOD     = 14
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD",   "30"))
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))

# EMA / Mom
EMA_FAST = 5
EMA_SLOW = 13

# Объёмный фильтр (по умолчанию выкл, чтобы увеличить число сигналов)
USE_VOLUME_FILTER = _bool_env("USE_VOLUME_FILTER", False)
VOLUME_SPIKE_MULT = float(os.getenv("VOLUME_SPIKE_MULT", "1.10"))

# Risk management
ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "0.8"))
ATR_TP_MULT = float(os.getenv("ATR_TP_MULT", "1.5"))
TP_MIN_PCT  = float(os.getenv("TP_MIN_PCT",  "0.01"))   # >= 1%
TP_MAX_PCT  = float(os.getenv("TP_MAX_PCT",  "0.02"))   # до 2%
RR_MIN      = float(os.getenv("RR_MIN",      "1.5"))

# Подтверждения
MIN_CONFIRMATIONS = int(os.getenv("MIN_CONFIRMATIONS", "2"))

# Антиспам / мониторинг
SIGNAL_COOLDOWN_SEC   = int(os.getenv("SIGNAL_COOLDOWN_SEC",   "60"))
POSITION_COOLDOWN_SEC = int(os.getenv("POSITION_COOLDOWN_SEC", "120"))
KEEPALIVE_SEC         = int(os.getenv("KEEPALIVE_SEC",         str(13*60)))
WATCHDOG_SEC          = int(os.getenv("WATCHDOG_SEC",          "60"))
STALL_EXIT_SEC        = int(os.getenv("STALL_EXIT_SEC",        "300"))

MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "5"))

# Error report в TG
REPORT_ERRORS_TO_TG = _bool_env("REPORT_ERRORS_TO_TG", False)
ERROR_REPORT_COOLDOWN_SEC = int(os.getenv("ERROR_REPORT_COOLDOWN_SEC", "180"))

# Telegram long polling
TG_POLL_TIMEOUT = int(os.getenv("TG_POLL_TIMEOUT", "25"))
ENABLE_TG_SENDER_QUEUE = _bool_env("ENABLE_TG_SENDER_QUEUE", False)

# Дедуп сигналов
DEDUP_TTL_SEC = int(os.getenv("DEDUP_TTL_SEC", "900"))  # 15 минут

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

# =========================
# Telegram error reporting
# =========================
async def report_error(app: web.Application, where: str, exc: Optional[BaseException] = None, note: Optional[str] = None) -> None:
    if not REPORT_ERRORS_TO_TG:
        return
    tg: Optional[Tg] = app.get("tg")
    if not tg:
        return
    nowt = now_s()
    last = app.setdefault("_last_error_ts", 0)
    if nowt - last < ERROR_REPORT_COOLDOWN_SEC:
        return
    app["_last_error_ts"] = nowt

    title = f"⚠️ <b>Runtime error</b> @ {html.escape(where)}"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    body = ""
    if note:
        body += f"\n<b>Note:</b> {html.escape(note)}"
    if exc:
        tb = traceback.format_exc()
        tail = "\n".join(tb.strip().splitlines()[-25:])
        body += "\n<pre>" + html.escape(tail[:3500]) + "</pre>"
    elif note is None:
        body += "\n(no traceback)"
    text = f"{title}\n🕒 {ts} UTC{body}"

    targets = (PRIMARY_RECIPIENTS if ONLY_CHANNEL and PRIMARY_RECIPIENTS
               else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS))
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
    k = 2.0 / (period + 1.0)
    ema_val = sum(values[:period]) / period
    for price in values[period:]:
        ema_val = price * k + ema_val * (1 - k)
    return ema_val

def average_true_range(data: List[Tuple[float, float, float, float, float]], period: int) -> float:
    if len(data) < period + 1:
        return 0.0
    total = 0.0
    cnt = 0
    for i in range(len(data) - period, len(data)):
        high = data[i][1]; low = data[i][2]; prev_close = data[i-1][3]
        tr = max(high - low, abs(high - prev_close), abs(prev_close - low))
        total += tr
        cnt += 1
    return total / cnt if cnt else 0.0

def relative_strength_index_fixed(data: List[Tuple[float, float, float, float, float]], period: int = RSI_PERIOD) -> float:
    if len(data) < period + 1:
        return 50.0
    closes = [bar[3] for bar in data[-(period+1):]]
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        if ch > 0:
            gains.append(ch); losses.append(0.0)
        else:
            gains.append(0.0); losses.append(-ch)
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def momentum_indicator(data: List[Tuple[float, float, float, float, float]], period: int = 5) -> float:
    if len(data) < period + 1:
        return 0.0
    current_close = data[-1][3]
    past_close = data[-(period+1)][3]
    return (current_close - past_close) / past_close * 100.0

def detect_key_levels(data: List[Tuple[float,float,float,float,float]], lookback: int = 20) -> Tuple[float, float]:
    if len(data) < lookback:
        c = data[-1][3] if data else 0.0
        return c, c
    recent = data[-lookback:]
    highs = [b[1] for b in recent]
    lows  = [b[2] for b in recent]
    return min(lows), max(highs)

# =========================
# КЛИЕНТЫ
# =========================
class BybitWS:
    def __init__(self, url: str, http: aiohttp.ClientSession) -> None:
        self.url = url
        self.http = http
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.on_message = None

    async def connect(self) -> None:
        self.ws = await self.http.ws_connect(self.url, heartbeat=30)

    async def subscribe(self, topics: List[str]) -> None:
        if not self.ws:
            await self.connect()
        await self.ws.send_json({"op": "subscribe", "args": topics})

    async def unsubscribe(self, topics: List[str]) -> None:
        if not self.ws:
            return
        await self.ws.send_json({"op": "unsubscribe", "args": topics})

    async def run(self) -> None:
        if not self.ws:
            await self.connect()
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if self.on_message:
                        res = self.on_message(data)
                        if asyncio.iscoroutine(res):
                            asyncio.create_task(res)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("WS connection error, reconnecting...")
        finally:
            with contextlib.suppress(Exception):
                if self.ws and not self.ws.closed:
                    await self.ws.close()
            await asyncio.sleep(2.0)
            await self.connect()
            asyncio.create_task(self.run())

class Tg:
    def __init__(self, token: str, http: aiohttp.ClientSession) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = http
        self._backoff = 1.0  # seconds

    async def delete_webhook(self, drop_pending_updates: bool = True) -> None:
        payload = {"drop_pending_updates": drop_pending_updates}
        async with self.http.post(f"{self.base}/deleteWebhook", json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
            with contextlib.suppress(Exception):
                await r.text()

    async def send(self, chat_id: int, text: str) -> None:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        for _ in range(4):
            try:
                async with self.http.post(f"{self.base}/sendMessage", json=payload, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    r.raise_for_status()
                    await r.json()
                self._backoff = 1.0
                return
            except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError, ConnectionResetError):
                sleep = min(self._backoff, 10.0); await asyncio.sleep(sleep); self._backoff = min(self._backoff * 2, 20.0)
            except aiohttp.ClientResponseError as e:
                if e.status in (429, 500, 502, 503, 504):
                    sleep = min(self._backoff, 10.0); await asyncio.sleep(sleep); self._backoff = min(self._backoff * 2, 20.0); continue
                raise

    async def updates(self, offset: Optional[int], timeout: int = TG_POLL_TIMEOUT) -> Dict[str, Any]:
        """GET getUpdates с ретраями и авто-починкой webhook на 409."""
        params = {"timeout": timeout, "allowed_updates": json.dumps(["message", "channel_post", "my_chat_member"])}
        if offset is not None:
            params["offset"] = offset

        for _ in range(6):
            try:
                async with self.http.get(
                    f"{self.base}/getUpdates",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=timeout + 20)
                ) as r:
                    r.raise_for_status()
                    data = await r.json()
                self._backoff = 1.0
                return data
            except aiohttp.ClientResponseError as e:
                if e.status == 409:
                    with contextlib.suppress(Exception):
                        await self.delete_webhook(drop_pending_updates=True)
                    await asyncio.sleep(1.0)
                    continue
                if e.status in (429, 500, 502, 503, 504):
                    sleep = min(self._backoff, 10.0); await asyncio.sleep(sleep); self._backoff = min(self._backoff * 2, 20.0); continue
                raise
            except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError, ConnectionResetError):
                sleep = min(self._backoff, 10.0); await asyncio.sleep(sleep); self._backoff = min(self._backoff * 2, 20.0); continue

        return {"ok": True, "result": []}

class BybitRest:
    def __init__(self, base: str, http: aiohttp.ClientSession) -> None:
        self.base = base.rstrip("/")
        self.http = http

    async def tickers_linear(self) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/tickers?category=linear"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            r.raise_for_status()
            return (await r.json()).get("result", {}).get("list", []) or []

    async def klines(self, category: str, symbol: str, interval: str, limit: int = 200) -> List[Tuple[float, float, float, float, float]]:
        url = f"{self.base}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={min(200, max(1, limit))}"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            r.raise_for_status()
            data = await r.json()
        arr = (data.get("result") or {}).get("list") or []
        out: List[Tuple[float, float, float, float, float]] = []
        for it in arr:
            try:
                o, h, l, c, v = float(it[1]), float(it[2]), float(it[3]), float(it[4]), float(it[5])
                out.append((o, h, l, c, v))
            except Exception:
                continue
        return out[-200:]

# =========================
# СИСТЕМА ПОЗИЦИЙ
# =========================
@dataclass
class Position:
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_time: int
    size: float = 0.0

class PositionManager:
    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self.max_positions = MAX_POSITIONS

    def can_open_position(self, symbol: str) -> bool:
        return (len(self.positions) < self.max_positions and symbol not in self.positions)

    def open_position(self, pos: Position) -> bool:
        if self.can_open_position(pos.symbol):
            self.positions[pos.symbol] = pos
            return True
        return False

    def close_position(self, symbol: str) -> Optional[Position]:
        return self.positions.pop(symbol, None)

    def get_active_symbols(self) -> List[str]:
        return list(self.positions.keys())

# =========================
# СОСТОЯНИЕ РЫНКА
# =========================
@dataclass
class SymbolState:
    k5:  List[Tuple[float, float, float, float, float]] = field(default_factory=list)
    rsi_5m: float = 50.0
    prev_rsi_5m: float = 50.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    momentum: float = 0.0
    support: float = 0.0
    resistance: float = 0.0
    atr: float = 0.0
    last_signal_ts: int = 0
    last_position_ts: int = 0

class Market:
    def __init__(self):
        self.symbols: List[str] = []
        self.state: Dict[str, SymbolState] = defaultdict(SymbolState)
        self.position_manager = PositionManager()
        self.last_ws_msg_ts: int = now_ms()
        self.signal_stats: Dict[str, int] = {"total": 0, "long": 0, "short": 0}
        self.last_signal_sent_ts: int = 0
        # дедуп кэш (key -> expiry_ts)
        self._dedup: Dict[Tuple[str,str,str,int], int] = {}
        self._dedup_order: deque = deque(maxlen=3000)

    def dedup_should_emit(self, key: Tuple[str,str,str,int]) -> bool:
        nowt = now_s()
        # очистка протухших записей «лениво»
        while self._dedup_order:
            k = self._dedup_order[0]
            if self._dedup.get(k, 0) <= nowt:
                self._dedup_order.popleft()
                self._dedup.pop(k, None)
            else:
                break
        if key in self._dedup:
            return False
        self._dedup[key] = nowt + DEDUP_TTL_SEC
        self._dedup_order.append(key)
        return True

# =========================
# СКАЛЬП-ДВИЖОК
# =========================
class ScalpingEngine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def update_indicators_fast(self, sym: str) -> None:
        st = self.mkt.state[sym]
        if len(st.k5) >= RSI_PERIOD + 1:
            st.prev_rsi_5m = st.rsi_5m
            st.rsi_5m = relative_strength_index_fixed(st.k5)
        if len(st.k5) >= EMA_SLOW:
            closes = [bar[3] for bar in st.k5]
            st.ema_fast = exponential_moving_average(closes, EMA_FAST)
            st.ema_slow = exponential_moving_average(closes, EMA_SLOW)
        st.momentum = momentum_indicator(st.k5)
        st.support, st.resistance = detect_key_levels(st.k5)
        if len(st.k5) >= 11:
            st.atr = average_true_range(st.k5, 10)

    def generate_scalp_signal(self, sym: str) -> Optional[Dict[str, Any]]:
        st = self.mkt.state[sym]
        if len(st.k5) < max(RSI_PERIOD + 5, EMA_SLOW + 5):
            return None

        nowt = now_s()
        if nowt - st.last_position_ts < POSITION_COOLDOWN_SEC:
            return None
        if nowt - st.last_signal_ts < SIGNAL_COOLDOWN_SEC:
            return None
        if not self.mkt.position_manager.can_open_position(sym):
            return None

        price = st.k5[-1][3]
        rsi = st.rsi_5m
        prev_rsi = st.prev_rsi_5m
        mom = st.momentum
        ema_fast = st.ema_fast
        ema_slow = st.ema_slow

        # объёмный фильтр
        if len(st.k5) >= 6:
            avg_vol = sum(b[4] for b in st.k5[-6:-1]) / 5.0
            cur_vol = st.k5[-1][4]
            volume_ok = (not USE_VOLUME_FILTER) or (avg_vol == 0) or (cur_vol > avg_vol * VOLUME_SPIKE_MULT)
        else:
            volume_ok = True

        # RSI re-entry
        long_reentry  = (prev_rsi < RSI_OVERSOLD)   and (rsi >= RSI_OVERSOLD)
        short_reentry = (prev_rsi > RSI_OVERBOUGHT) and (rsi <= RSI_OVERBOUGHT)

        long_checks = [
            (rsi < RSI_OVERSOLD) or long_reentry,
            mom > -1.2,
            (ema_fast > ema_slow) or (price > ema_fast),
            volume_ok,
            price >= st.support
        ]
        short_checks = [
            (rsi > RSI_OVERBOUGHT) or short_reentry,
            mom < 1.2,
            (ema_fast < ema_slow) or (price < ema_fast),
            volume_ok,
            price <= st.resistance
        ]

        need = max(2, MIN_CONFIRMATIONS)
        side = None
        if sum(1 for c in long_checks if c) >= need:
            side = "LONG"
        elif sum(1 for c in short_checks if c) >= need:
            side = "SHORT"
        if not side:
            return None

        atr_val = st.atr if st.atr > 0 else price * 0.005
        if side == "LONG":
            sl = price - (atr_val * ATR_SL_MULT)
            tp = price + (atr_val * ATR_TP_MULT)
            tp_pct = (tp - price) / price
            if tp_pct < TP_MIN_PCT: tp = price * (1 + TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT: tp = price * (1 + TP_MAX_PCT)
            rr = (tp - price) / max(1e-9, (price - sl))
        else:
            sl = price + (atr_val * ATR_SL_MULT)
            tp = price - (atr_val * ATR_TP_MULT)
            tp_pct = (price - tp) / price
            if tp_pct < TP_MIN_PCT: tp = price * (1 - TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT: tp = price * (1 - TP_MAX_PCT)
            rr = (price - tp) / max(1e-9, (sl - price))

        if rr < RR_MIN:
            return None

        # дедуп: ключ = (symbol, side, '5m', последняя закрытая минута)
        last_bar_close_ts = int(time.time() // (5*60)) * (5*60)
        key = (sym, side, "5m", last_bar_close_ts)
        if not self.mkt.dedup_should_emit(key):
            return None

        st.last_signal_ts = nowt
        st.last_position_ts = nowt
        pos = Position(symbol=sym, side=side, entry_price=price, stop_loss=sl, take_profit=tp, entry_time=nowt)
        if not self.mkt.position_manager.open_position(pos):
            return None
        self.mkt.signal_stats["total"] += 1
        self.mkt.signal_stats["long" if side == "LONG" else "short"] += 1

        reasons: List[str] = []
        if side == "LONG":
            if (rsi < RSI_OVERSOLD) or long_reentry: reasons.append(f"RSI(14) re-entry↑/OS<{RSI_OVERSOLD}: {rsi:.1f}")
            if mom > -1.2:                            reasons.append(f"Momentum {mom:.2f}%")
            if (ema_fast > ema_slow) or (price > ema_fast): reasons.append("EMA импульс ↑")
            if volume_ok and USE_VOLUME_FILTER:       reasons.append("Объём подтверждает")
            if price >= st.support:                   reasons.append("Над поддержкой")
        else:
            if (rsi > RSI_OVERBOUGHT) or short_reentry: reasons.append(f"RSI(14) re-entry↓/OB>{RSI_OVERBOUGHT}: {rsi:.1f}")
            if mom < 1.2:                              reasons.append(f"Momentum {mom:.2f}%")
            if (ema_fast < ema_slow) or (price < ema_fast): reasons.append("EMA импульс ↓")
            if volume_ok and USE_VOLUME_FILTER:        reasons.append("Объём подтверждает")
            if price <= st.resistance:                 reasons.append("Под сопротивлением")

        strength = "сильный" if (rr >= 2.0 or (USE_VOLUME_FILTER and volume_ok)) else "слабый"

        return {
            "symbol": sym, "side": side, "entry": price, "tp1": tp, "sl": sl, "rr": rr,
            "rsi": rsi, "momentum": mom, "support": st.support, "resistance": st.resistance,
            "timeframe": "5m SCALP", "duration": "5-15min",
            "confidence": min(90, 40 + (sum(long_checks if side == "LONG" else short_checks))*10),
            "reasons": reasons, "strength": strength,
        }

# =========================
# ФОРМАТ СИГНАЛА
# =========================
def format_scalp_signal(sig: Dict[str, Any]) -> str:
    symbol = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; tp = sig["tp1"]; sl = sig["sl"]; rr = sig["rr"]
    tp_pct = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry
    sl_pct = (entry - sl) / entry if side == "LONG" else (sl - entry) / entry
    emoji = "🟢" if side == "LONG" else "🔴"
    lines = [
        f"{emoji} <b>SCALP SIGNAL | {side} | {symbol} | {sig.get('strength','')}</b>",
        "⏰ ТФ: 5m (скальп)",
        f"📍 <b>Текущая/Вход:</b> {entry:.6f}",
        f"🛡️ <b>Стоп-Лосс:</b> {sl:.6f} ({pct(abs(sl_pct))})",
        f"🎯 <b>Тейк-Профит:</b> {tp:.6f} ({pct(abs(tp_pct))})",
        f"⚖️ <b>Risk/Reward:</b> {rr:.2f}",
        "",
        "📊 <b>Метрики:</b>",
        f"• RSI(14): {sig['rsi']:.1f}",
        f"• Momentum(5): {sig['momentum']:.2f}%",
        f"• Support/Resistance: {sig['support']:.6f} / {sig['resistance']:.6f}",
        f"• Уверенность: {sig.get('confidence', 60)}%",
        "",
        "📈 <b>Обоснование:</b>",
    ]
    for r in sig.get("reasons", []):
        lines.append(f"• {r}")
    lines += [
        "",
        "⚠️ Не финсовет. Риск ≤ 1% депозита.",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"#{'LONG' if side=='LONG' else 'SHORT'}_{symbol}"
    ]
    return "\n".join(lines)

# =========================
# WS message handler (5m)
# =========================
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    try:
        mkt: Market = app["mkt"]; eng: ScalpingEngine = app["engine"]; tg: Tg = app["tg"]
        topic = data.get("topic") or ""
        mkt.last_ws_msg_ts = now_ms()
        payload = (data.get("data") or [])
        if not payload:
            return

        def _upd(buf: List[Tuple[float,float,float,float,float]], p: Dict[str, Any]) -> None:
            o = float(p["open"]); h = float(p["high"]); l = float(p["low"]); c = float(p["close"]); v = float(p.get("volume") or 0.0)
            if p.get("confirm") is False and buf:
                buf[-1] = (o, h, l, c, v)
            else:
                buf.append((o, h, l, c, v))
                if len(buf) > 1000:
                    del buf[:len(buf)-1000]

        if topic.startswith("kline."):
            parts = topic.split(".")
            if len(parts) >= 3:
                interval = parts[1]
                symbol = parts[2]
            else:
                symbol = payload[0].get("symbol") or ""
                interval = payload[0].get("interval") or TF_SCALP

            st = mkt.state[symbol]
            for p in payload:
                if interval == TF_SCALP:
                    _upd(st.k5, p)
                    if p.get("confirm") is True:
                        eng.update_indicators_fast(symbol)
                        sig = eng.generate_scalp_signal(symbol)
                        if sig:
                            text = format_scalp_signal(sig)
                            targets = (PRIMARY_RECIPIENTS if ONLY_CHANNEL and PRIMARY_RECIPIENTS
                                       else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS))
                            for chat_id in targets:
                                with contextlib.suppress(Exception):
                                    await tg.send(chat_id, text)
                            mkt.last_signal_sent_ts = now_ms()
    except Exception as e:
        logger.exception("ws_on_message error")
        await report_error(app, "ws_on_message", e)

# =========================
# Telegram loop (long polling, устойчивый)
# =========================
async def tg_loop(app: web.Application) -> None:
    tg: Tg = app["tg"]; mkt: Market = app["mkt"]
    # гарантированно выключаем webhook и чистим очередь
    with contextlib.suppress(Exception):
        await tg.delete_webhook(drop_pending_updates=True)
    # ACK всего, что могло висеть (доп. страховка)
    with contextlib.suppress(Exception):
        await tg.updates(offset=2_147_483_647, timeout=1)

    offset: Optional[int] = None
    backoff = 1
    while True:
        try:
            resp = await tg.updates(offset=offset, timeout=TG_POLL_TIMEOUT)
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
                    stats = mkt.signal_stats
                    await tg.send(chat_id,
                        "✅ Online (SCALP)\n"
                        f"Symbols: {len(mkt.symbols)}\n"
                        f"Signals: total={stats.get('total',0)}, long={stats.get('long',0)}, short={stats.get('short',0)}\n"
                        f"TP range: {pct(TP_MIN_PCT)}–{pct(TP_MAX_PCT)} • RR≥{RR_MIN:.2f}\n"
                        f"Silent: {silent_line}")
                elif cmd == "/diag":
                    k5_pts = sum(len(mkt.state[s].k5) for s in mkt.symbols)
                    ago = (now_ms() - mkt.last_ws_msg_ts)/1000.0
                    head = ", ".join(mkt.symbols[:10]) if mkt.symbols else "—"
                    await tg.send(chat_id,
                        "Diag:\n"
                        f"WS last msg: {ago:.1f}s ago\n"
                        f"Symbols: {len(mkt.symbols)} (head: {head})\n"
                        f"Kline buffers: 5m_total={k5_pts}")
                elif cmd == "/jobs":
                    jobs = []
                    for k in ("ws_task", "keepalive_task", "watchdog_task", "tg_task", "universe_task"):
                        t = app.get(k)
                        jobs.append(f"{k}: {'running' if (t and not t.done()) else 'stopped'}")
                    await tg.send(chat_id, "Jobs:\n" + "\n".join(jobs))
                else:
                    await tg.send(chat_id, "Неизвестная команда. /ping /status /diag /jobs")
            backoff = 1
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("tg_loop error")
            await report_error(app, "tg_loop", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# =========================
# Universe (symbols) + подписки (5m)
# =========================
async def build_universe_once(rest: BybitRest) -> List[str]:
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
        symbols = CORE_SYMBOLS + [x for x in pool if x not in CORE_SYMBOLS]
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
            oldset = set(mkt.symbols)
            add = [s for s in symbols_new if s not in oldset]
            rem = [s for s in mkt.symbols if s not in set(symbols_new)]
            if rem:
                args = [f"kline.{TF_SCALP}.{s}" for s in rem]
                await ws.unsubscribe(args)
            if add:
                args = [f"kline.{TF_SCALP}.{s}" for s in add]
                await ws.subscribe(args)
                logger.info(f"[WS] Subscribed to {len(args)} topics for {len(add)} symbols")
            if add or rem:
                mkt.symbols = symbols_new
                logger.info(f"[universe] +{len(add)} / -{len(rem)} • total={len(mkt.symbols)}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("universe_refresh_loop error")
            await report_error(app, "universe_refresh_loop", e)

# =========================
# Фоновые задачи
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
        except Exception as e:
            logger.exception("keepalive error")
            await report_error(app, "keepalive_loop", e)

async def watchdog_loop(app: web.Application) -> None:
    mkt: Market = app["mkt"]
    while True:
        try:
            await asyncio.sleep(WATCHDOG_SEC)
            ago = (now_ms() - mkt.last_ws_msg_ts) / 1000.0
            logger.info(f"[watchdog] alive; last WS msg {ago:.1f}s ago; symbols={len(mkt.symbols)}")
            if ago >= STALL_EXIT_SEC:
                logger.error(f"[watchdog] WS stalled {ago:.1f}s >= {STALL_EXIT_SEC}. Exit for restart.")
                os._exit(3)  # позволяем платформе рестартнуть контейнер
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("watchdog error")
            await report_error(app, "watchdog_loop", e)

# =========================
# Web app
# =========================
async def handle_health(request: web.Request) -> web.Response:
    mkt: Market = request.app["mkt"]
    return web.json_response({
        "ok": True,
        "symbols": mkt.symbols,
        "last_ws_msg_age_sec": int((now_ms() - mkt.last_ws_msg_ts)/1000),
        "positions": list(request.app["mkt"].position_manager.positions.keys()),
        "signals": request.app["mkt"].signal_stats,
    })

async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    if not TELEGRAM_TOKEN:
        raise RuntimeError("Не задан TELEGRAM_TOKEN")

    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=10,
        ttl_dns_cache=300,
        keepalive_timeout=45,
        enable_cleanup_closed=True,
    )
    http = aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=60),
        trust_env=True,
        headers={"Connection": "keep-alive"}
    )
    app["http"] = http
    app["tg"] = Tg(TELEGRAM_TOKEN, http)

    # long polling mode: убираем webhook + чистим pending updates
    with contextlib.suppress(Exception):
        await app["tg"].delete_webhook(drop_pending_updates=True)
        logger.info("Telegram webhook deleted (drop_pending_updates=True)")
    with contextlib.suppress(Exception):
        await app["tg"].updates(offset=2_147_483_647, timeout=1)

    app["rest"] = BybitRest(BYBIT_REST, http)
    app["mkt"] = Market()
    app["engine"] = ScalpingEngine(app["mkt"])
    app["ws"] = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: asyncio.create_task(ws_on_message(app, data))

    # Вселенная
    symbols = await build_universe_once(app["rest"])
    app["mkt"].symbols = symbols
    logger.info(f"symbols: {symbols}")

    # Предзагрузка истории (5m)
    for s in app["mkt"].symbols:
        with contextlib.suppress(Exception):
            app["mkt"].state[s].k5 = await app["rest"].klines("linear", s, TF_SCALP, limit=200)
            app["engine"].update_indicators_fast(s)
    logger.info("[bootstrap] 5m klines loaded")

    # WS
    await app["ws"].connect()
    args = [f"kline.{TF_SCALP}.{s}" for s in app["mkt"].symbols]
    if args:
        await app["ws"].subscribe(args)
        logger.info(f"[WS] Initial subscribed to {len(args)} topics for {len(app['mkt'].symbols)} symbols")

    # Фоновые задачи
    app["ws_task"] = asyncio.create_task(app["ws"].run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_task"] = asyncio.create_task(tg_loop(app))
    app["universe_task"] = asyncio.create_task(universe_refresh_loop(app))

    # Уведомление при первом старте (без спама при реконнектах)
    if not app.get("_banner_sent"):
        app["_banner_sent"] = True
        try:
            targets = PRIMARY_RECIPIENTS or ALLOWED_CHAT_IDS
            for chat_id in targets:
                await app["tg"].send(chat_id, "🟢 Cryptobot SCALP v10.4: polling mode enabled, WS live, engine ready")
        except Exception as e:
            logger.warning("startup notify failed")
            await report_error(app, "on_startup:notify", e)

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

def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

def main() -> None:
    setup_logging(LOG_LEVEL)
    logger.info("🚀 Starting Cryptobot SCALP v10.4 — TF=5m, polling + WS + resilient TG")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
