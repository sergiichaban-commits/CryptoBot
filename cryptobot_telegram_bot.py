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
BYBIT_REST            = "https://api.bybit.com"
BYBIT_WSS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

PORT = int(os.getenv("PORT", "10000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or ""
ALLOWED_CHAT_IDS = [int(x) for x in (os.getenv("ALLOWED_CHAT_IDS") or "").split(",") if x.strip()]
PRIMARY_RECIPIENTS = [i for i in ALLOWED_CHAT_IDS if i < 0] or ALLOWED_CHAT_IDS[:1] or []
ONLY_CHANNEL = True

UNIVERSE_REFRESH_SEC = 600
TURNOVER_MIN_USD = float(os.getenv("TURNOVER_MIN_USD", "5000000"))
VOLUME_MIN_USD   = float(os.getenv("VOLUME_MIN_USD",  "5000000"))
ACTIVE_SYMBOLS   = int(os.getenv("ACTIVE_SYMBOLS", "60"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "TONUSDT"]

# Timeframes
EXEC_TF_MAIN = "15"
EXEC_TF_AUX  = "5"
CONTEXT_TF   = "60"

# Indicator periods
ATR_PERIOD_15   = int(os.getenv("ATR_PERIOD_15", "14"))
VOL_SMA_15      = int(os.getenv("VOL_SMA_15", "20"))
EMA_PERIOD_1H   = int(os.getenv("EMA_PERIOD_1H", "100"))
VWAP_WINDOW_15  = int(os.getenv("VWAP_WINDOW_15", "60"))

TP_MIN_PCT      = float(os.getenv("TP_MIN_PCT", "0.01"))
TP_MAX_PCT      = float(os.getenv("TP_MAX_PCT", "0.5"))
ATR_SL_MULT     = float(os.getenv("ATR_SL_MULT", "0.8"))
ATR_TP_MULT     = float(os.getenv("ATR_TP_MULT", "1.2"))
RR_MIN          = float(os.getenv("RR_MIN", "1.2"))
VOLUME_SPIKE_MULT = float(os.getenv("VOLUME_SPIKE_MULT", "2.0"))
MAX_POSITIONS   = int(os.getenv("MAX_POSITIONS", "10"))

# =========================
# HTTP/WebSocket helpers
# =========================
def now_ms() -> int:
    return int(time.time() * 1000)
def pct(x: float) -> str:
    return f"{x:.2%}"
def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, force=True)
logger = logging.getLogger("cryptobot")

@dataclass
class SymbolState:
    k5: List[Tuple[float, float, float, float, float]] = field(default_factory=list)
    last_signal_ts: int = 0
    cooldown_ts: Dict[str, int] = field(default_factory=dict)

class Market:
    def __init__(self):
        self.symbols: List[str] = []
        self.state: Dict[str, SymbolState] = defaultdict(SymbolState)
        self.last_ws_msg_ts: int = now_ms()
        self.last_signal_sent_ts: int = 0

mkt = Market()

async def fetch_klines(symbol: str, limit: int = 200) -> List[Tuple[float,float,float,float,float]]:
    async with aiohttp.ClientSession() as session:
        async with session.get(BYBIT_REST + f"/public/linear/kline?symbol={symbol}&interval={EXEC_TF_AUX}&limit={limit}") as resp:
            data = await resp.json()
    arr = (data.get("result") or {}).get("list") or []
    out: List[Tuple[float,float,float,float,float]] = []
    for it in arr:
        try:
            o,h,l,c,v = float(it[1]), float(it[2]), float(it[3]), float(it[4]), float(it[5])
            out.append((o,h,l,c,v))
        except Exception:
            continue
    return out[-200:]

# =========================
# Signal Engine (5m RSI)
# =========================
class Engine:
    def __init__(self):
        pass

    async def find_signal(self, sym: str) -> Optional[Dict[str, Any]]:
        state = mkt.state[sym]
        K5 = state.k5
        if len(K5) < 15:
            return None

        # RSI crossing conditions
        prev_rsi = rsi14(K5[:-1])
        curr_rsi = rsi14(K5)
        long_signal = prev_rsi < 30 <= curr_rsi
        short_signal = prev_rsi > 70 >= curr_rsi
        if not long_signal and not short_signal:
            return None

        side = "LONG" if long_signal else "SHORT"
        if not cooled(side):
            return None

        if side == "LONG":
            reasons = ["RSI вышел из перепроданности"]
        else:
            reasons = ["RSI вышел из перекупленности"]

        # Confirmations
        confirm_extreme = False
        confirm_pattern = False
        confirm_volume = False
        confirm_divergence = False

        # Local extrema touch/break
        lookback = 6
        if side == "LONG":
            local_min = min(r[2] for r in K5[-(lookback+1):-1])
            if K5[-1][2] <= local_min:
                confirm_extreme = True
                reasons.append(f"Обновлён локальный минимум ({lookback} свечей)")
        else:
            local_max = max(r[1] for r in K5[-(lookback+1):-1])
            if K5[-1][1] >= local_max:
                confirm_extreme = True
                reasons.append(f"Обновлён локальный максимум ({lookback} свечей)")

        # Candlestick pattern
        o,h,l,c,v = K5[-1]
        if side == "LONG":
            # Hammer
            body = abs(c - o); upper_wick = h - max(c, o); lower_wick = min(c, o) - l
            if c > o and lower_wick >= 2 * body and upper_wick <= 0.5 * body:
                confirm_pattern = True
                reasons.append("Свечной паттерн: молот")
            else:
                # Bullish engulfing
                o_prev, h_prev, l_prev, c_prev, _ = K5[-2]
                if c_prev < o_prev and c > o and c >= o_prev and o <= c_prev:
                    confirm_pattern = True
                    reasons.append("Свечной паттерн: бычье поглощение")
        else:
            # Shooting star / inverted hammer
            body = abs(c - o); upper_wick = h - max(c, o); lower_wick = min(c, o) - l
            if o > c and upper_wick >= 2 * body and lower_wick <= 0.5 * body:
                confirm_pattern = True
                reasons.append("Свечной паттерн: пин-бар")
            else:
                # Bearish engulfing
                o_prev, h_prev, l_prev, c_prev, _ = K5[-2]
                if c_prev > o_prev and c < o and o >= c_prev and c <= o_prev:
                    confirm_pattern = True
                    reasons.append("Свечной паттерн: медвежье поглощение")

        # Volume spike
        vol_list = [r[4] for r in K5[-11:-1]]
        avg_vol = sum(vol_list)/len(vol_list) if len(vol_list) > 0 else 0
        if avg_vol > 0 and v >= VOLUME_SPIKE_MULT * avg_vol:
            confirm_volume = True
            reasons.append(f"Объёмный всплеск: vol={v:.0f} ≥ {VOLUME_SPIKE_MULT}×SMA10({avg_vol:.0f})")

        # RSI divergence (price/RSI)
        if side == "LONG":
            prev_low_idx = min(range(len(K5)-6, len(K5)-1), key=lambda i: K5[i][2])
            rsi_prev_low = rsi14(K5[:prev_low_idx+1])
            if curr_rsi > rsi_prev_low:
                confirm_divergence = True
                reasons.append("Бычья дивергенция RSI")
        else:
            prev_high_idx = max(range(len(K5)-6, len(K5)-1), key=lambda i: K5[i][1])
            rsi_prev_high = rsi14(K5[:prev_high_idx+1])
            if curr_rsi < rsi_prev_high:
                confirm_divergence = True
                reasons.append("Медвежья дивергенция RSI")

        # Determine signal strength
        count = (1 if confirm_extreme else 0) + (1 if confirm_pattern else 0) + (1 if confirm_volume else 0) + (1 if confirm_divergence else 0)
        if count < 2:
            return None
        strength = "сильный" if count >= 3 else "слабый"

        # ATR-based TP/SL calculation
        atr5 = atr(K5, ATR_PERIOD_15)
        entry_price = K5[-1][3]
        if side == "LONG":
            sl_price = max(1e-9, entry_price - ATR_SL_MULT * atr5)
            tp_price = entry_price + ATR_TP_MULT * atr5
            rr = (tp_price - entry_price) / max(1e-9, (entry_price - sl_price))
            tp_pct = (tp_price - entry_price) / entry_price
            if tp_pct < TP_MIN_PCT:
                tp_price = entry_price * (1 + TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT:
                tp_price = entry_price * (1 + TP_MAX_PCT)
            rr = (tp_price - entry_price) / max(1e-9, (entry_price - sl_price))
        else:
            sl_price = entry_price + ATR_SL_MULT * atr5
            tp_price = entry_price - ATR_TP_MULT * atr5
            rr = (entry_price - tp_price) / max(1e-9, (sl_price - entry_price))
            tp_pct = (entry_price - tp_price) / entry_price
            if tp_pct < TP_MIN_PCT:
                tp_price = entry_price * (1 - TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT:
                tp_price = entry_price * (1 - TP_MAX_PCT)
            rr = (entry_price - tp_price) / max(1e-9, (sl_price - entry_price))

        if rr < RR_MIN:
            return None

        state.cooldown_ts[side] = now_s
        return {
            "symbol": sym,
            "side": side,
            "entry": float(entry_price),
            "tp1": float(tp_price),
            "tp2": None,
            "sl": float(sl_price),
            "rr": float(rr),
            "reason": reasons,
            "rsi14": round(curr_rsi, 2),
            "strength": strength
        }

def fmt_signal(sig: Dict[str, Any]) -> str:
    sym = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; tp1 = sig["tp1"]; sl = sig["sl"]; rr = sig["rr"]
    tp1_pct = (tp1 - entry)/entry if side == "LONG" else (entry - tp1)/entry
    tp2 = sig.get("tp2")
    rsi_val = sig.get("rsi14")
    strength = sig.get("strength")
    reasons = "".join(f"\n- {r}" for r in (sig.get("reason") or []))
    lines = [
        f"{'🟢' if side=='LONG' else '🔴'} <b>{side} SIGNAL</b> on <b>[{sym}]</b> (5m)",
        "<b>Параметры:</b>",
        f"- <b>Сила сигнала:</b> {strength}" if strength else None,
        f"- <b>RSI(14):</b> {rsi_val:.2f}" if rsi_val is not None else None,
        f"<b>Текущая цена:</b> {entry:g}",
        f"<b>Вход:</b> {entry:g}",
        f"<b>Стоп-Лосс:</b> {sl:g}",
        f"<b>Тейк-Профит 1:</b> {tp1:g} ({pct(tp1_pct)})" + (f"\n<b>Тейк-Профит 2:</b> {tp2:g}" if tp2 else ""),
        "<b>Обоснование:</b>" + reasons if reasons else None,
        f"<b>Риск:</b> RR≈{rr:.2f} • плечо ≤ x10; риск ≤ 1% депозита.",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
    ]
    return "\n".join([x for x in lines if x])

# ... (rest of the bot code remains unchanged, including Bybit API handling, Telegram loop, etc.)
