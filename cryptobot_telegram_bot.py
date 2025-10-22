# -*- coding: utf-8 -*-
"""
Cryptobot — Professional Derivatives Signals (Bybit V5, USDT Perpetuals)
v9.1 — Multi-timeframe confluence + VWAP + EMA + RSI + ATR
       Long polling (deleteWebhook) + WS klines (5m/15m/1h) + Telegram loop
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
# ПРОФЕССИОНАЛЬНЫЙ КОНФИГ
# =========================
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

PORT = int(os.getenv("PORT", "10000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or ""
ALLOWED_CHAT_IDS = [int(x) for x in (os.getenv("ALLOWED_CHAT_IDS") or "").split(",") if x.strip()]
PRIMARY_RECIPIENTS = [i for i in ALLOWED_CHAT_IDS if i < 0] or ALLOWED_CHAT_IDS[:1] or []
ONLY_CHANNEL = True  # отправлять только в канал(ы), если указаны, иначе всем ALLOWED_CHAT_IDS

# Вселенная
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "600"))
TURNOVER_MIN_USD = float(os.getenv("TURNOVER_MIN_USD", "2000000"))   # $2M
VOLUME_MIN_USD   = float(os.getenv("VOLUME_MIN_USD",  "2000000"))
ACTIVE_SYMBOLS   = int(os.getenv("ACTIVE_SYMBOLS",    "40"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT"]

# Мультитаймфрейм анализ
TF_ENTRY   = "5"      # Входной ТФ
TF_TREND   = "15"     # Тренд
TF_CONTEXT = "60"     # Контекст

# Индикаторы
RSI_PERIOD = 14
EMA_FAST   = 9
EMA_SLOW   = 21
VWAP_PERIOD = 20
ATR_PERIOD  = 14
VOL_SMA     = int(os.getenv("VOL_SMA", "20"))

# Пороги сигналов
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD",   "32"))
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "68"))

VOLUME_SPIKE_MULT   = float(os.getenv("VOLUME_SPIKE_MULT", "1.8"))
MIN_CONFIRMATIONS   = int(os.getenv("MIN_CONFIRMATIONS",   "2"))   # из 4+

# Риск-менеджмент
ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "1.0"))
ATR_TP_MULT = float(os.getenv("ATR_TP_MULT", "1.8"))
TP_MIN_PCT  = float(os.getenv("TP_MIN_PCT",  "0.004"))   # 0.4%
TP_MAX_PCT  = float(os.getenv("TP_MAX_PCT",  "0.012"))   # 1.2%
RR_MIN      = float(os.getenv("RR_MIN",      "1.4"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "3"))

# Антиспам/мониторинг
SIGNAL_COOLDOWN_SEC   = int(os.getenv("SIGNAL_COOLDOWN_SEC",   "120"))
POSITION_COOLDOWN_SEC = int(os.getenv("POSITION_COOLDOWN_SEC", "300"))
KEEPALIVE_SEC         = int(os.getenv("KEEPALIVE_SEC",         str(13*60)))
WATCHDOG_SEC          = int(os.getenv("WATCHDOG_SEC",          "60"))
STALL_EXIT_SEC        = int(os.getenv("STALL_EXIT_SEC",        "300"))

# =========================
# УТИЛИТЫ
# =========================
def now_ms() -> int: return int(time.time() * 1000)
def now_s() -> int: return int(time.time())
def pct(x: float) -> str: return f"{x:.2%}"

def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, force=True)

logger = logging.getLogger("cryptobot.pro")

# =========================
# ПРОФЕССИОНАЛЬНЫЕ ИНДИКАТОРЫ
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

def volume_weighted_average_price(data: List[Tuple[float, float, float, float, float]], period: int) -> Tuple[float, float]:
    if len(data) < period:
        return 0.0, 0.0
    window = data[-period:]
    num = 0.0
    den = 0.0
    for _, h, l, c, v in window:
        tp = (h + l + c) / 3.0
        num += tp * v
        den += v
    if den == 0:
        return 0.0, 0.0
    cur_vwap = num / den

    if len(data) >= 2 * period:
        prev = data[-2*period:-period]
        num2 = 0.0
        den2 = 0.0
        for _, h, l, c, v in prev:
            tp = (h + l + c) / 3.0
            num2 += tp * v
            den2 += v
        if den2 > 0:
            prev_vwap = num2 / den2
            slope = (cur_vwap - prev_vwap) / prev_vwap if prev_vwap != 0 else 0.0
        else:
            slope = 0.0
    else:
        slope = 0.0
    return cur_vwap, slope

def relative_strength_index(data: List[Tuple[float, float, float, float, float]], period: int = RSI_PERIOD) -> float:
    if len(data) < period + 1:
        return 50.0
    closes = [bar[3] for bar in data[-(period+1):]]
    gains = 0.0
    losses = 0.0
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        if ch > 0:
            gains += ch
        else:
            losses += -ch
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

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

def detect_divergence(price_extremes: List[float], rsi_extremes: List[float]) -> str:
    if len(price_extremes) < 2 or len(rsi_extremes) < 2:
        return "NONE"
    # Bullish: price LL, RSI HL
    if price_extremes[-1] < price_extremes[-2] and rsi_extremes[-1] > rsi_extremes[-2]:
        return "BULLISH"
    # Bearish: price HH, RSI LH
    if price_extremes[-1] > price_extremes[-2] and rsi_extremes[-1] < rsi_extremes[-2]:
        return "BEARISH"
    return "NONE"

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
            # reconnection loop
            await asyncio.sleep(2.0)
            await self.connect()
            asyncio.create_task(self.run())

class Tg:
    def __init__(self, token: str, http: aiohttp.ClientSession) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = http

    async def delete_webhook(self, drop_pending_updates: bool = True) -> Dict[str, Any]:
        payload = {"drop_pending_updates": drop_pending_updates}
        async with self.http.post(f"{self.base}/deleteWebhook", json=payload) as r:
            r.raise_for_status()
            return await r.json()

    async def send(self, chat_id: int, text: str) -> None:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        async with self.http.post(f"{self.base}/sendMessage", json=payload) as r:
            r.raise_for_status()
            await r.json()

    async def updates(self, offset: Optional[int], timeout: int = 25) -> Dict[str, Any]:
        payload = {"timeout": timeout, "allowed_updates": ["message", "channel_post", "my_chat_member"]}
        if offset is not None:
            payload["offset"] = offset
        async with self.http.post(f"{self.base}/getUpdates", json=payload, timeout=aiohttp.ClientTimeout(total=timeout+10)) as r:
            r.raise_for_status()
            return await r.json()

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
    # Мультитаймфрейм данные
    k5:  List[Tuple[float, float, float, float, float]] = field(default_factory=list)
    k15: List[Tuple[float, float, float, float, float]] = field(default_factory=list)
    k60: List[Tuple[float, float, float, float, float]] = field(default_factory=list)
    # Индикаторы
    rsi_5m: float = 50.0
    rsi_15m: float = 50.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    vwap: float = 0.0
    vwap_slope: float = 0.0
    atr: float = 0.0
    # Метки времени
    last_signal_ts: int = 0
    last_position_ts: int = 0
    cooldown_ts: Dict[str, int] = field(default_factory=dict)
    # Экстремумы для дивергенций
    price_highs: deque = field(default_factory=lambda: deque(maxlen=5))
    price_lows:  deque = field(default_factory=lambda: deque(maxlen=5))
    rsi_highs:   deque = field(default_factory=lambda: deque(maxlen=5))
    rsi_lows:    deque = field(default_factory=lambda: deque(maxlen=5))

class Market:
    def __init__(self):
        self.symbols: List[str] = []
        self.state: Dict[str, SymbolState] = defaultdict(SymbolState)
        self.position_manager = PositionManager()
        self.last_ws_msg_ts: int = now_ms()
        self.signal_stats: Dict[str, int] = {"total": 0, "long": 0, "short": 0}
        self.last_signal_sent_ts: int = 0

# =========================
# ПРОФЕССИОНАЛЬНАЯ ЛОГИКА СИГНАЛОВ
# =========================
class ProfessionalEngine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def update_indicators(self, sym: str) -> None:
        st = self.mkt.state[sym]
        # RSI
        if len(st.k5) >= RSI_PERIOD + 1:
            st.rsi_5m = relative_strength_index(st.k5)
        if len(st.k15) >= RSI_PERIOD + 1:
            st.rsi_15m = relative_strength_index(st.k15)
        # EMA
        if len(st.k5) >= EMA_SLOW:
            closes = [b[3] for b in st.k5]
            st.ema_fast = exponential_moving_average(closes, EMA_FAST)
            st.ema_slow = exponential_moving_average(closes, EMA_SLOW)
        # VWAP
        if len(st.k5) >= VWAP_PERIOD:
            st.vwap, st.vwap_slope = volume_weighted_average_price(st.k5, VWAP_PERIOD)
        # ATR
        if len(st.k5) >= ATR_PERIOD + 1:
            st.atr = average_true_range(st.k5, ATR_PERIOD)
        # Локальные экстремумы (для простой дивергенции)
        if len(st.k5) >= 3:
            # локальный хай
            if st.k5[-2][1] > st.k5[-3][1] and st.k5[-2][1] > st.k5[-1][1]:
                st.price_highs.append(st.k5[-2][1])
                st.rsi_highs.append(st.rsi_5m)
            # локальный лой
            if st.k5[-2][2] < st.k5[-3][2] and st.k5[-2][2] < st.k5[-1][2]:
                st.price_lows.append(st.k5[-2][2])
                st.rsi_lows.append(st.rsi_5m)

    def calculate_trend(self, sym: str) -> Tuple[str, float]:
        st = self.mkt.state[sym]
        if not st.k5:
            return "NEUTRAL", 0.0
        price = st.k5[-1][3]
        score = 0
        total = 0
        # EMA alignment
        if st.ema_fast and st.ema_slow:
            score += 1 if st.ema_fast > st.ema_slow else -1
            total += 1
        # Price vs VWAP
        if st.vwap:
            if price > st.vwap and st.vwap_slope >= 0: score += 1
            if price < st.vwap and st.vwap_slope <= 0: score -= 1
            total += 1
        # RSI(15m) tilt
        if st.rsi_15m:
            if st.rsi_15m > 55: score += 1
            elif st.rsi_15m < 45: score -= 1
            total += 1
        # High-highs check
        if len(st.k5) >= 20:
            recent_high = max(b[1] for b in st.k5[-10:])
            prev_high   = max(b[1] for b in st.k5[-20:-10])
            score += 1 if recent_high > prev_high else -1
            total += 1
        strength = (score / total) if total else 0.0
        if strength > 0.2:
            return "BULLISH", strength
        if strength < -0.2:
            return "BEARISH", abs(strength)
        return "NEUTRAL", abs(strength)

    def generate_signal(self, sym: str) -> Optional[Dict[str, Any]]:
        st = self.mkt.state[sym]
        if (len(st.k5) < max(RSI_PERIOD+5, EMA_SLOW+5, VWAP_PERIOD+5) or
            len(st.k15) < RSI_PERIOD+1 or len(st.k60) < 1):
            return None

        tnow = now_s()
        if tnow - st.last_position_ts < POSITION_COOLDOWN_SEC:
            return None

        if not self.mkt.position_manager.can_open_position(sym):
            return None

        price = st.k5[-1][3]
        trend, tstrength = self.calculate_trend(sym)

        # Volume spike (к последней свече)
        avg_vol = sum(b[4] for b in st.k5[-VOL_SMA-1:-1]) / max(1, min(VOL_SMA, len(st.k5)-1))
        vol_spike = st.k5[-1][4] >= VOLUME_SPIKE_MULT * avg_vol if avg_vol > 0 else False

        # Дивергенции
        bull_div = detect_divergence(list(st.price_lows), list(st.rsi_lows)) == "BULLISH"
        bear_div = detect_divergence(list(st.price_highs), list(st.rsi_highs)) == "BEARISH"

        # LONG conditions
        base_long = st.rsi_5m <= RSI_OVERSOLD and trend in ("BULLISH", "NEUTRAL")
        conf_long = sum([
            1 if st.ema_fast > st.ema_slow else 0,
            1 if (price > st.vwap or st.vwap_slope > 0) else 0,
            1 if vol_spike else 0,
            1 if bull_div else 0,
        ])

        # SHORT conditions
        base_short = st.rsi_5m >= RSI_OVERBOUGHT and trend in ("BEARISH", "NEUTRAL")
        conf_short = sum([
            1 if st.ema_fast < st.ema_slow else 0,
            1 if (price < st.vwap or st.vwap_slope < 0) else 0,
            1 if vol_spike else 0,
            1 if bear_div else 0,
        ])

        side = None
        reasons: List[str] = []
        if base_long and conf_long >= MIN_CONFIRMATIONS:
            side = "LONG"
            reasons = [
                f"RSI({RSI_PERIOD})={st.rsi_5m:.1f} ≤ {RSI_OVERSOLD}",
                f"Тренд: {trend} (сила {tstrength:.2f})",
                "VWAP восходящий/цена выше VWAP" if (price > st.vwap or st.vwap_slope > 0) else "—",
                "Объёмный всплеск" if vol_spike else "—",
                "Бычья дивергенция" if bull_div else "—",
            ]
        elif base_short and conf_short >= MIN_CONFIRMATIONS:
            side = "SHORT"
            reasons = [
                f"RSI({RSI_PERIOD})={st.rsi_5m:.1f} ≥ {RSI_OVERBOUGHT}",
                f"Тренд: {trend} (сила {tstrength:.2f})",
                "VWAP нисходящий/цена ниже VWAP" if (price < st.vwap or st.vwap_slope < 0) else "—",
                "Объёмный всплеск" if vol_spike else "—",
                "Медвежья дивергенция" if bear_div else "—",
            ]

        if not side:
            return None

        # SL/TP
        atr_val = st.atr if st.atr > 0 else price * 0.01
        if side == "LONG":
            sl = max(1e-9, price - ATR_SL_MULT * atr_val)
            tp = price + ATR_TP_MULT * atr_val
            tp_pct = (tp - price) / price
            if tp_pct < TP_MIN_PCT: tp = price * (1 + TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT: tp = price * (1 + TP_MAX_PCT)
            rr = (tp - price) / max(1e-9, (price - sl))
        else:
            sl = price + ATR_SL_MULT * atr_val
            tp = price - ATR_TP_MULT * atr_val
            tp_pct = (price - tp) / price
            if tp_pct < TP_MIN_PCT: tp = price * (1 - TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT: tp = price * (1 - TP_MAX_PCT)
            rr = (price - tp) / max(1e-9, (sl - price))

        if rr < RR_MIN:
            return None

        # Кулдаун на направление
        if tnow - self.mkt.state[sym].cooldown_ts.get(side, 0) < SIGNAL_COOLDOWN_SEC:
            return None
        self.mkt.state[sym].cooldown_ts[side] = tnow

        pos = Position(symbol=sym, side=side, entry_price=price, stop_loss=sl, take_profit=tp, entry_time=tnow)
        if not self.mkt.position_manager.open_position(pos):
            return None
        st.last_position_ts = tnow
        self.mkt.signal_stats["total"] += 1
        self.mkt.signal_stats["long" if side == "LONG" else "short"] += 1

        return {
            "symbol": sym,
            "side": side,
            "entry": price,
            "tp1": tp,
            "sl": sl,
            "rr": rr,
            "atr": atr_val,
            "rsi_5m": st.rsi_5m,
            "rsi_15m": st.rsi_15m,
            "trend": trend,
            "trend_strength": tstrength,
            "vwap": st.vwap,
            "reason": [r for r in reasons if r != "—"],
            "position_size": "1-2% от депозита",
            "confidence": min(90, 50 + 10 * (conf_long if side == "LONG" else conf_short)),
        }

# =========================
# ФОРМАТИРОВАНИЕ СИГНАЛА
# =========================
def format_pro_signal(sig: Dict[str, Any]) -> str:
    symbol = sig["symbol"]
    side = sig["side"]
    entry = sig["entry"]
    tp = sig["tp1"]
    sl = sig["sl"]
    rr = sig["rr"]
    tp_pct = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry
    sl_pct = (entry - sl) / entry if side == "LONG" else (sl - entry) / entry
    emoji = "🟢" if side == "LONG" else "🔴"
    lines = [
        f"{emoji} <b>PRO SIGNAL | {side} | {symbol}</b>",
        "⏰ ТФ: 5m (вход), 15m (тренд), 1h (контекст)",
        f"📍 <b>Текущая/Вход:</b> {entry:.6f}",
        f"🛡️ <b>Стоп-Лосс:</b> {sl:.6f} ({pct(abs(sl_pct))})",
        f"🎯 <b>Тейк-Профит:</b> {tp:.6f} ({pct(abs(tp_pct))})",
        f"⚖️ <b>Risk/Reward:</b> {rr:.2f}",
        "",
        "📊 <b>Метрики:</b>",
        f"• RSI(5m): {sig['rsi_5m']:.1f} | RSI(15m): {sig['rsi_15m']:.1f}",
        f"• Тренд: {sig['trend']} (сила: {sig['trend_strength']:.2f})",
        f"• VWAP: {sig['vwap']:.6f}",
        f"• ATR: {sig['atr']:.6f}",
        f"• Уверенность: {sig.get('confidence',70)}%",
        "",
        "📈 <b>Обоснование:</b>",
    ]
    for r in sig.get("reason", []):
        lines.append(f"• {r}")
    lines += [
        "",
        f"💰 <b>Размер позиции:</b> {sig['position_size']}",
        "⚠️ Это не финансовый совет. Риск ≤ 1% депозита, плечо ≤ x5.",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"#{'LONG' if side=='LONG' else 'SHORT'}_{symbol}"
    ]
    return "\n".join(lines)

# =========================
# WS message handler (5m/15m/60m)
# =========================
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    mkt: Market = app["mkt"]; eng: ProfessionalEngine = app["engine"]; tg: Tg = app["tg"]
    topic = data.get("topic") or ""
    mkt.last_ws_msg_ts = now_ms()
    payload = data.get("data") or []
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
        # topic: kline.<interval>.<symbol>
        parts = topic.split(".")
        if len(parts) >= 3:
            interval = parts[1]
            symbol = parts[2]
        else:
            symbol = payload[0].get("symbol") or ""
            interval = payload[0].get("interval") or TF_ENTRY

        st = mkt.state[symbol]
        for p in payload:
            if interval == TF_ENTRY:
                _upd(st.k5, p)
                if p.get("confirm") is True:
                    # 5m close -> обновить индикаторы и, возможно, сгенерировать сигнал
                    eng.update_indicators(symbol)
                    sig = eng.generate_signal(symbol)
                    if sig:
                        text = format_pro_signal(sig)
                        targets = (PRIMARY_RECIPIENTS if ONLY_CHANNEL and PRIMARY_RECIPIENTS else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS))
                        for chat_id in targets:
                            with contextlib.suppress(Exception):
                                await tg.send(chat_id, text)
                        mkt.last_signal_sent_ts = now_ms()
                        st.last_signal_ts = now_ms()
            elif interval == TF_TREND:
                _upd(st.k15, p)
            elif interval == TF_CONTEXT:
                _upd(st.k60, p)

# =========================
# Telegram loop (long polling)
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
                    stats = mkt.signal_stats
                    await tg.send(chat_id,
                        "✅ Online (PRO)\n"
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
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("tg_loop error")
            await asyncio.sleep(2)

# =========================
# Universe (symbols) + подписки
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
            if add or rem:
                if rem:
                    args = []
                    for s in rem:
                        args += [f"kline.{TF_ENTRY}.{s}", f"kline.{TF_TREND}.{s}", f"kline.{TF_CONTEXT}.{s}"]
                    await ws.unsubscribe(args)
                if add:
                    args = []
                    for s in add:
                        args += [f"kline.{TF_ENTRY}.{s}", f"kline.{TF_TREND}.{s}", f"kline.{TF_CONTEXT}.{s}"]
                    await ws.subscribe(args)
                    logger.info(f"[WS] Subscribed to {len(args)} topics for {len(add)} symbols")
                mkt.symbols = symbols_new
                logger.info(f"[universe] +{len(add)} / -{len(rem)} • total={len(mkt.symbols)}")
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("universe_refresh_loop error")

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
    http = aiohttp.ClientSession()
    app["http"] = http
    app["tg"] = Tg(TELEGRAM_TOKEN, http)
    # critical for long polling
    with contextlib.suppress(Exception):
        await app["tg"].delete_webhook(drop_pending_updates=True)
        logger.info("Telegram webhook deleted (drop_pending_updates=True)")

    app["rest"] = BybitRest(BYBIT_REST, http)
    app["mkt"] = Market()
    app["engine"] = ProfessionalEngine(app["mkt"])
    app["ws"] = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: asyncio.create_task(ws_on_message(app, data))

    # Вселенная и начальная подписка
    symbols = await build_universe_once(app["rest"])
    app["mkt"].symbols = symbols
    logger.info(f"symbols: {symbols}")

    # Предзагрузка истории (для корректных индикаторов)
    try:
        for s in symbols:
            with contextlib.suppress(Exception):
                app["mkt"].state[s].k5  = await app["rest"].klines("linear", s, TF_ENTRY,   limit=200)
                app["mkt"].state[s].k15 = await app["rest"].klines("linear", s, TF_TREND,   limit=200)
                app["mkt"].state[s].k60 = await app["rest"].klines("linear", s, TF_CONTEXT, limit=200)
                app["engine"].update_indicators(s)
        logger.info("[bootstrap] historical klines loaded")
    except Exception:
        logger.exception("bootstrap history error")

    await app["ws"].connect()
    args = []
    for s in symbols:
        args += [f"kline.{TF_ENTRY}.{s}", f"kline.{TF_TREND}.{s}", f"kline.{TF_CONTEXT}.{s}"]
    if args:
        await app["ws"].subscribe(args)
        logger.info(f"[WS] Initial subscribed to {len(args)} topics for {len(symbols)} symbols")

    # Фоновые задачи
    app["ws_task"] = asyncio.create_task(app["ws"].run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_task"] = asyncio.create_task(tg_loop(app))
    app["universe_task"] = asyncio.create_task(universe_refresh_loop(app))

    # Уведомление
    try:
        targets = PRIMARY_RECIPIENTS or ALLOWED_CHAT_IDS
        for chat_id in targets:
            await app["tg"].send(chat_id, "🟢 Cryptobot PRO v9.1: polling mode enabled, WS live, MTF engine ready")
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

def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

def main() -> None:
    setup_logging(LOG_LEVEL)
    logger.info("🚀 Starting Professional Cryptobot v9.1 — TF=5m/15m/60m, polling + WS")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
