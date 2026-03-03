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
ONLY_CHANNEL = _bool_env("ONLY_CHANNEL", True)  # по умолчанию только в канал(ы)

# Вселенная
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "600"))
TURNOVER_MIN_USD = float(os.getenv("TURNOVER_MIN_USD", "2000000"))
VOLUME_MIN_USD   = float(os.getenv("VOLUME_MIN_USD",  "2000000"))
ACTIVE_SYMBOLS   = int(os.getenv("ACTIVE_SYMBOLS",    "60"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT"]

# =========================
# SCALPING КОНФИГ (только 5m)
# =========================
TF_SCALP = "5"

# Более чувствительные параметры
RSI_PERIOD     = 14
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD",   "40"))  # было 38
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "60"))  # было 62

# Быстрые EMA для момента
EMA_FAST = 5
EMA_SLOW = 13

# Объемный фильтр
VOLUME_SPIKE_MULT = float(os.getenv("VOLUME_SPIKE_MULT", "1.2"))  # было 1.3

# Risk management для скальпинга
ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "0.8"))
ATR_TP_MULT = float(os.getenv("ATR_TP_MULT", "1.5"))
TP_MIN_PCT  = float(os.getenv("TP_MIN_PCT",  "0.002"))  # 0.2%
TP_MAX_PCT  = float(os.getenv("TP_MAX_PCT",  "0.008"))  # 0.8% (хард-верх)
RR_MIN      = float(os.getenv("RR_MIN",      "1.8"))

# Подтверждения
MIN_CONFIRMATIONS = int(os.getenv("MIN_CONFIRMATIONS", "2"))  # по умолчанию 2

# Антиспам/мониторинг
SIGNAL_COOLDOWN_SEC   = int(os.getenv("SIGNAL_COOLDOWN_SEC",   "20"))
POSITION_COOLDOWN_SEC = int(os.getenv("POSITION_COOLDOWN_SEC", "45"))
KEEPALIVE_SEC         = int(os.getenv("KEEPALIVE_SEC",         str(13*60)))
WATCHDOG_SEC          = int(os.getenv("WATCHDOG_SEC",          "60"))
STALL_EXIT_SEC        = int(os.getenv("STALL_EXIT_SEC",        "300"))

MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "5"))

# Error report
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

# =========================
# Telegram error reporting
# =========================
async def report_error(app: web.Application, where: str, exc: Optional[BaseException] = None, note: Optional[str] = None) -> None:
    if not REPORT_ERRORS_TO_TG:
        return
    tg: Optional[Tg] = app.get("tg")
    if not tg:
        return
    now = now_s()
    last = app.setdefault("_last_error_ts", 0)
    if now - last < ERROR_REPORT_COOLDOWN_SEC:
        return
    app["_last_error_ts"] = now

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
    """RSI по последним period+1 свечам (простая, быстрая версия)."""
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
    """Процентное изменение за N свечей (скорость)."""
    if len(data) < period + 1:
        return 0.0
    current_close = data[-1][3]
    past_close = data[-(period+1)][3]
    return (current_close - past_close) / past_close * 100.0

def detect_key_levels(data: List[Tuple[float, float, float, float, float]], lookback: int = 20) -> Tuple[float, float]:
    """Примитивные уровни: min/max за lookback."""
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
        self._running: bool = False
        self._reconnect_delay: float = 1.0

    async def connect(self) -> None:
        """Establish new WS connection."""
        # Закрываем старый сокет, если он ещё жив
        with contextlib.suppress(Exception):
            if self.ws and not self.ws.closed:
                await self.ws.close()
        self.ws = await self.http.ws_connect(self.url, heartbeat=30)
        logger.info("BybitWS connected")

    async def subscribe(self, topics: List[str]) -> None:
        if not self.ws or self.ws.closed:
            await self.connect()
        await self.ws.send_json({"op": "subscribe", "args": topics})

    async def unsubscribe(self, topics: List[str]) -> None:
        if not self.ws or self.ws.closed:
            return
        await self.ws.send_json({"op": "unsubscribe", "args": topics})

    async def run(self) -> None:
        """Main WS loop with controlled reconnects (no recursive create_task)."""
        self._running = True
        self._reconnect_delay = 1.0
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
                        logger.warning(f"WS closed or error: {msg.type}, reconnecting...")
                        break

                # дошли до конца цикла async for → будет reconnect
            except asyncio.CancelledError:
                logger.info("BybitWS.run cancelled")
                break
            except Exception:
                logger.exception("WS connection error, reconnecting...")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 1.5, 30.0)
            finally:
                with contextlib.suppress(Exception):
                    if self.ws and not self.ws.closed:
                        await self.ws.close()

        logger.info("BybitWS.run stopped")

    async def stop(self) -> None:
        """Gracefully stop WS loop and close socket."""
        self._running = False
        if self.ws and not self.ws.closed:
            with contextlib.suppress(Exception):
                await self.ws.close()

class Tg:
    """Упрощенный и надежный клиент для Telegram Bot API"""
    def __init__(self, token: str):
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session: aiohttp.ClientSession | None = None

    async def init(self):
        if not self.session:
            # Настройка таймаутов: 10 сек на установку связи, 45 сек на общий запрос
            timeout = aiohttp.ClientTimeout(total=45, connect=10)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def delete_webhook(self, drop_pending_updates: bool = False):
        """Удаляет вебхук и, если нужно, очищает очередь старых сообщений"""
        url = f"{self.base_url}/deleteWebhook"
        try:
            async with self.session.post(url, json={"drop_pending_updates": drop_pending_updates}) as resp:
                return await resp.json()
        except Exception as e:
            logger.error(f"Tg.delete_webhook error: {e}")
            return None

    async def get_updates(self, offset: Optional[int] = None, timeout: int = 25) -> List[Dict]:
        """Получение сообщений методом Long Polling"""
        url = f"{self.base_url}/getUpdates"
        data = {"timeout": timeout}
        if offset:
            data["offset"] = offset
        
        try:
            async with self.session.post(url, json=data, timeout=timeout + 5) as resp:
                # Если Телеграм вернул ошибку сети (502, 504), просто ждем и идем дальше
                if resp.status in (502, 503, 504):
                    logger.warning(f"Telegram API Gateway Error: {resp.status}. Retrying...")
                    await asyncio.sleep(2)
                    return []
                
                res = await resp.json()
                return res.get("result", []) if res.get("ok") else []
        except asyncio.TimeoutError:
            return []
        except Exception as e:
            logger.error(f"Tg.get_updates error: {e}")
            await asyncio.sleep(3)
            return []

    async def send(self, chat_id: Any, text: str, parse_mode: str = "HTML") -> bool:
        """Отправка сообщения"""
        url = f"{self.base_url}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
        try:
            async with self.session.post(url, json=payload) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"Tg.send error: {e}")
            return False

    async def close(self):
        if self.session:
            await self.session.close()

class BybitRest:
    def __init__(self, base: str, http: aiohttp.ClientSession) -> None:
        self.base = base.rstrip("/")
        self.http = http

    async def tickers_linear(self) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/tickers?category=linear"
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
    # данные только 5m
    k5:  List[Tuple[float, float, float, float, float]] = field(default_factory=list)
    # индикаторы
    rsi_5m: float = 50.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    momentum: float = 0.0
    support: float = 0.0
    resistance: float = 0.0
    atr: float = 0.0
    # метки времени / кулдауны
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

# =========================
# СКАЛЬП-ДВИЖОК
# =========================
class ScalpingEngine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def update_indicators_fast(self, sym: str) -> None:
        st = self.mkt.state[sym]
        if len(st.k5) >= RSI_PERIOD + 1:
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
        # cooldown по позициям
        if nowt - st.last_position_ts < POSITION_COOLDOWN_SEC:
            logger.debug(
                f"[{sym}] skip: position cooldown {nowt - st.last_position_ts}s < {POSITION_COOLDOWN_SEC}s"
            )
            return None
        # cooldown по сигналам
        if nowt - st.last_signal_ts < SIGNAL_COOLDOWN_SEC:
            logger.debug(
                f"[{sym}] skip: signal cooldown {nowt - st.last_signal_ts}s < {SIGNAL_COOLDOWN_SEC}s"
            )
            return None
        # лимит позиций
        if not self.mkt.position_manager.can_open_position(sym):
            logger.debug(f"[{sym}] skip: cannot open position (max positions or already open)")
            return None

        price = st.k5[-1][3]
        rsi = st.rsi_5m
        mom = st.momentum
        ema_fast = st.ema_fast
        ema_slow = st.ema_slow

        # объемный фильтр
        if len(st.k5) >= 6:
            avg_vol = sum(b[4] for b in st.k5[-6:-1]) / 5.0
            cur_vol = st.k5[-1][4]
            volume_ok = cur_vol > avg_vol * VOLUME_SPIKE_MULT if avg_vol > 0 else True
        else:
            volume_ok = True

        long_checks = [
            rsi < RSI_OVERSOLD and rsi > 25,              # не «перепродан» слишком глубоко
            mom > -1.0,                                   # не обвал
            (ema_fast > ema_slow) or (price > ema_fast),  # импульс вверх
            volume_ok,
            price > st.support                            # выше поддержки
        ]
        short_checks = [
            rsi > RSI_OVERBOUGHT and rsi < 75,            # не «перекуплен» слишком высоко
            mom < 1.0,                                    # не ракета
            (ema_fast < ema_slow) or (price < ema_fast),  # импульс вниз
            volume_ok,
            price < st.resistance                         # ниже сопротивления
        ]

        side: Optional[str] = None
        min_checks = max(2, MIN_CONFIRMATIONS)  # минимум 2 подтверждения
        if sum(long_checks) >= min_checks:
            side = "LONG"
        elif sum(short_checks) >= min_checks:
            side = "SHORT"
        if not side:
            return None

        # SL/TP через ATR (быстро)
        atr_val = st.atr if st.atr > 0 else price * 0.005  # 0.5% запасной
        if side == "LONG":
            sl = price - (atr_val * ATR_SL_MULT)
            tp = price + (atr_val * ATR_TP_MULT)
            tp_pct = (tp - price) / price
            if tp_pct < TP_MIN_PCT:
                tp = price * (1 + TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT:
                tp = price * (1 + TP_MAX_PCT)
            rr = (tp - price) / max(1e-9, (price - sl))
        else:
            sl = price + (atr_val * ATR_SL_MULT)
            tp = price - (atr_val * ATR_TP_MULT)
            tp_pct = (price - tp) / price
            if tp_pct < TP_MIN_PCT:
                tp = price * (1 - TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT:
                tp = price * (1 - TP_MAX_PCT)
            rr = (price - tp) / max(1e-9, (sl - price))

        if rr < RR_MIN:
            logger.debug(f"[{sym}] skip: RR {rr:.2f} < RR_MIN {RR_MIN:.2f}")
            return None

        logger.info(f"[{sym}] {side} signal generated at {price:.6f}, RSI={rsi:.1f}, momentum={mom:.2f}%")

        st.last_signal_ts = nowt
        st.last_position_ts = nowt  # блокируем повторные открытия на короткое время
        pos = Position(symbol=sym, side=side, entry_price=price, stop_loss=sl, take_profit=tp, entry_time=nowt)
        if not self.mkt.position_manager.open_position(pos):
            logger.debug(f"[{sym}] skip: position manager rejected open_position")
            return None
        self.mkt.signal_stats["total"] += 1
        self.mkt.signal_stats["long" if side == "LONG" else "short"] += 1

        reasons: List[str] = []
        if side == "LONG":
            if long_checks[0]:
                reasons.append(f"RSI<OS ({RSI_OVERSOLD}) → {rsi:.1f}")
            if long_checks[1]:
                reasons.append(f"Momentum {mom:.2f}%")
            if long_checks[2]:
                reasons.append("EMA импульс ↑")
            if long_checks[3]:
                reasons.append("Объём подтверждает")
            if long_checks[4]:
                reasons.append("Выше поддержки")
        else:
            if short_checks[0]:
                reasons.append(f"RSI>OB ({RSI_OVERBOUGHT}) → {rsi:.1f}")
            if short_checks[1]:
                reasons.append(f"Momentum {mom:.2f}%")
            if short_checks[2]:
                reasons.append("EMA импульс ↓")
            if short_checks[3]:
                reasons.append("Объём подтверждает")
            if short_checks[4]:
                reasons.append("Ниже сопротивления")

        return {
            "symbol": sym,
            "side": side,
            "entry": price,
            "tp1": tp,
            "sl": sl,
            "rr": rr,
            "rsi": rsi,
            "momentum": mom,
            "support": st.support,
            "resistance": st.resistance,
            "timeframe": "5m SCALP",
            "duration": "5-15min",
            "confidence": min(90, 40 + (sum(long_checks if side == "LONG" else short_checks)) * 10),
        }

# =========================
# ФОРМАТ СИГНАЛА
# =========================
def format_scalp_signal(sig: Dict[str, Any]) -> str:
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
        f"{emoji} <b>SCALP SIGNAL | {side} | {symbol}</b>",
        "⏰ ТФ: 5m (скальп)",
        f"📍 <b>Цена/Вход:</b> {entry:.6f}",
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
    for r in sig.get("reason", []):
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
    start_time = time.time()
    try:
        mkt: Market = app["mkt"]
        eng: ScalpingEngine = app["engine"]
        tg: Tg = app["tg"]

        topic = data.get("topic") or ""
        mkt.last_ws_msg_ts = now_ms()
        payload = data.get("data") or []
        if not payload:
            return

        def _upd(buf: List[Tuple[float, float, float, float, float]], p: Dict[str, Any]) -> None:
            o = float(p["open"]); h = float(p["high"]); l = float(p["low"]); c = float(p["close"]); v = float(p.get("volume") or 0.0)
            if p.get("confirm") is False and buf:
                buf[-1] = (o, h, l, c, v)
            else:
                buf.append((o, h, l, c, v))
                if len(buf) > 1000:
                    del buf[:len(buf) - 1000]

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
                    # на закрытии 5m — расчёт и возможный сигнал
                    if p.get("confirm") is True:
                        logger.debug(
                            f"[{symbol}] 5m closed: o={p.get('open')} h={p.get('high')} "
                            f"l={p.get('low')} c={p.get('close')} v={p.get('volume')}"
                        )
                        asyncio.create_task(process_confirmed_candle(app, symbol))
    except Exception as e:
        logger.exception("ws_on_message error")
        await report_error(app, "ws_on_message", e)
    finally:
        processing_time = time.time() - start_time
        if processing_time > 0.1:
            logger.warning(
                f"Slow WS message processing: {processing_time:.3f}s for topic={data.get('topic')}"
            )


async def process_confirmed_candle(app: web.Application, symbol: str) -> None:
    """Асинхронная обработка закрытой 5m-свечи: индикаторы + генерация и отправка сигнала."""
    try:
        mkt: Market = app["mkt"]
        eng: ScalpingEngine = app["engine"]
        tg: Tg = app["tg"]

        eng.update_indicators_fast(symbol)
        sig = eng.generate_scalp_signal(symbol)
        if not sig:
            return

        logger.info(f"🚨 SIGNAL GENERATED: {sig['symbol']} {sig['side']} at {sig['entry']}")

        text = format_scalp_signal(sig)
        # только в канал, если ONLY_CHANNEL=1 и есть PRIMARY_RECIPIENTS
        if ONLY_CHANNEL and PRIMARY_RECIPIENTS:
            targets = PRIMARY_RECIPIENTS
        else:
            targets = (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS)

        send_tasks: List[asyncio.Task] = []
        for chat_id in targets:
            send_tasks.append(asyncio.create_task(tg.send(chat_id, text)))

        if send_tasks:
            await asyncio.gather(*send_tasks, return_exceptions=True)
            mkt.last_signal_sent_ts = now_ms()
    except Exception as e:
        logger.error(f"Error processing confirmed candle for {symbol}: {e}")
        await report_error(app, f"process_confirmed_candle({symbol})", e)

# =========================
# Telegram loop (long polling)
# =========================
async def tg_loop(app: web.Application) -> None:
    tg: Tg = app["tg"]; mkt: Market = app["mkt"]
    offset: Optional[int] = None
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
        except asyncio.CancelledError:
            break
        except Exception as e:
            # Частый сетевой шум: Telegram рвёт соединение (Errno 104)
            if isinstance(e, aiohttp.ClientOSError) and getattr(e, "errno", None) == 104:
                logger.warning("tg_loop: connection reset by peer (Telegram). Will retry.")
                await asyncio.sleep(2)
                continue

            logger.exception("tg_loop error")
            await report_error(app, "tg_loop", e)
            await asyncio.sleep(2)

# =========================
# Universe (symbols) + подписки (только 5m)
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

async def watchdog_loop(app: web.Application):
    """Следит за активностью WebSocket и предотвращает 'зависание' бота"""
    logger.info("[watchdog] task started")
    while True:
        await asyncio.sleep(30)
        now = time.time()
        
        # Берем время последнего сообщения от биржи
        last_ws = app.get("_last_ws_msg_ts", now)
        diff = now - last_ws
        
        # Если сообщений нет более 10 минут (600 сек) — перезапускаем. 
        # 3 минуты было слишком мало и вызывало ложные перезапуски.
        if diff > 600:
            logger.error(f"[watchdog] NO WS ACTIVITY FOR {diff:.1f}s! Triggering restart...")
            os._exit(1)
        
        # Раз в 2 минуты пишем в лог, что всё ок
        if int(now) % 120 < 31:
            logger.info(f"[watchdog] system healthy; last market data {diff:.1f}s ago")

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

async def on_startup(app: web.Application):
    # Код инициализации (Redis, Bybit и прочее)
    # Убедитесь, что ВСЕ строки ниже имеют одинаковый тип отступов
    app["tg"] = Tg(TELEGRAM_TOKEN)
    await app["tg"].init()

    # Вот та самая строка, на которой была ошибка
    logger.info("Telegram webhook deleted (drop_pending_updates=True)")
    await app["tg"].delete_webhook(drop_pending_updates=True)

    # Запуск циклов (tasks)
    app["ws_task"] = asyncio.create_task(ws_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_task"] = asyncio.create_task(tg_loop(app))
    
    logger.info("Bot started successfully")

    # Вселенная
    try:
        symbols = await build_universe_once(app["rest"])
        app["mkt"].symbols = symbols
        logger.info(f"symbols: {symbols}")
    except Exception as e:
        await report_error(app, "on_startup:build_universe_once", e)
        raise

    # Предзагрузка истории (только 5m)
    try:
        for s in app["mkt"].symbols:
            with contextlib.suppress(Exception):
                app["mkt"].state[s].k5 = await app["rest"].klines("linear", s, TF_SCALP, limit=200)
                app["engine"].update_indicators_fast(s)
        logger.info("[bootstrap] 5m klines loaded")
    except Exception as e:
        logger.exception("bootstrap history error")
        await report_error(app, "on_startup:bootstrap_history", e)

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

    # Уведомление
    try:
        targets = PRIMARY_RECIPIENTS or ALLOWED_CHAT_IDS
        for chat_id in targets:
            await app["tg"].send(chat_id, "🟢 Cryptobot SCALP v11 : polling mode enabled, WS live, engine ready")
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
    if app.get("ws"):
        with contextlib.suppress(Exception):
            await app["ws"].stop()
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
    logger.info("🚀 Starting Cryptobot SCALP v11  — TF=5m, polling + WS + TG error reports")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
