# -*- coding: utf-8 -*-
"""
Cryptobot — Derivatives Signals (Bybit V5, USDT Perpetuals)
v5.1 — фиксы вселенной, Telegram-диагностика (/diag, /jobs), нормальный вывод Silent
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
# Конфиг
# =========================
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

PORT = int(os.getenv("PORT", "10000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or ""
ALLOWED_CHAT_IDS = [int(x) for x in (os.getenv("ALLOWED_CHAT_IDS") or "").split(",") if x.strip()]
PRIMARY_RECIPIENTS = [i for i in ALLOWED_CHAT_IDS if i < 0] or ALLOWED_CHAT_IDS[:1] or []
ONLY_CHANNEL = True

# Вселенная: фильтры
UNIVERSE_REFRESH_SEC = 600
TURNOVER_MIN_USD = 100_000_000.0   # >$100M
VOLUME_MIN_USD = 100_000_000.0
ACTIVE_SYMBOLS = 60
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "TONUSDT"]

# Таймфреймы
EXEC_TF_MAIN = "15"               # базовый сигнал — 15m close
EXEC_TF_AUX  = "5"                # уточнение — 5m close
CONTEXT_TF   = "60"               # контекст EMA/VWAP (1H)

# Индикаторы / пороги
ATR_PERIOD_15 = 14
VOL_SMA_15 = 20
VOL_MULT_ENTRY = 1.5
EMA_PERIOD_1H = 100
VWAP_WINDOW_15 = 60

FUNDING_EXTREME_POS = 0.0005      # +0.05%
FUNDING_EXTREME_NEG = -0.0005     # -0.05%

OI_WINDOW_MIN = 15
OI_SHORT_MIN = 5

HEATMAP_WINDOW_SEC = 2 * 60 * 60
HEATMAP_BIN_BPS = 25
HEATMAP_TOP_K = 3

TP_MIN_PCT = 0.007
TP_MAX_PCT = 0.02
RR_MIN = 1.2

SIGNAL_COOLDOWN_SEC = 30
KEEPALIVE_SEC = 13 * 60
WATCHDOG_SEC = 60
STALL_EXIT_SEC = int(os.getenv("STALL_EXIT_SEC", "240"))

# =========================
# Утилиты/логгер
# =========================
def now_ms() -> int: return int(time.time() * 1000)
def pct(x: float) -> str: return f"{x:.2%}"

def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, force=True)

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

    async def klines(self, category: str, symbol: str, interval: str, limit: int = 200) -> List[Tuple[float,float,float,float,float]]:
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
        return out[-200:]

# =========================
# Bybit WS
# =========================
class BybitWS:
    def __init__(self, url: str, http: aiohttp.ClientSession) -> None:
        self.url = url; self.http = http
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._subs: set[str] = set()
        self.on_message = None

    async def connect(self) -> None:
        if self.ws and not self.ws.closed: return
        logger.info(f"WS connecting: {self.url}")
        self.ws = await self.http.ws_connect(self.url, heartbeat=25)
        if self._subs:
            await self.ws.send_json({"op":"subscribe","args":list(self._subs)})

    async def subscribe(self, args: List[str]) -> None:
        for a in args: self._subs.add(a)
        if not self.ws or self.ws.closed: await self.connect()
        if args:
            await self.ws.send_json({"op":"subscribe","args":args})
            logger.info(f"WS subscribed: {args}")

    async def unsubscribe(self, args: List[str]) -> None:
        for a in args: self._subs.discard(a)
        if not self.ws or self.ws.closed: return
        if args:
            await self.ws.send_json({"op":"unsubscribe","args":args})
            logger.info(f"WS unsubscribed: {args}")

    async def run(self) -> None:
        assert self.ws is not None
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue
                if data.get("op") in {"subscribe","unsubscribe","ping","pong"} or "success" in data:
                    continue
                if self.on_message:
                    try:
                        await self.on_message(data)
                    except Exception:
                        logger.exception("on_message error")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
        await asyncio.sleep(2)
        with contextlib.suppress(Exception):
            await self.connect()
        if self.ws and not self.ws.closed:
            await self.run()

# =========================
# Индикаторы
# =========================
def atr(rows: List[Tuple[float,float,float,float,float]], period: int) -> float:
    if len(rows) < period + 1: return 0.0
    s = 0.0
    for i in range(1, period+1):
        _,h,l,c,_ = rows[-i]
        _,_,_,pc,_ = rows[-i-1]
        tr = max(h-l, abs(h-pc), abs(pc-l))
        s += tr
    return s/period

def sma(vals: List[float], period: int) -> float:
    if len(vals) < period or period <= 0: return 0.0
    return sum(vals[-period:]) / period

def ema(values: List[float], period: int) -> float:
    if not values or period <= 1 or len(values) < period: return 0.0
    k = 2.0 / (period + 1.0)
    ema_val = sum(values[:period]) / period
    for v in values[period:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val

def rolling_vwap(rows: List[Tuple[float,float,float,float,float]], window: int) -> Tuple[float, float]:
    n = len(rows)
    if n < window + 10: return 0.0, 0.0
    tp = [(r[1]+r[2]+r[3])/3.0 for r in rows]
    v  = [r[4] for r in rows]
    vw_now  = sum(tp[-window+i] * v[-window+i] for i in range(window)) / max(1e-9, sum(v[-window+i] for i in range(window)))
    vw_prev = sum(tp[-window-10+i] * v[-window-10+i] for i in range(window)) / max(1e-9, sum(v[-window-10+i] for i in range(window)))
    return vw_now, (vw_now - vw_prev)

# =========================
# Рыночные данные / состояние
# =========================
@dataclass
class SymbolState:
    k15: List[Tuple[float,float,float,float,float]] = field(default_factory=list)
    k5:  List[Tuple[float,float,float,float,float]] = field(default_factory=list)
    k60: List[Tuple[float,float,float,float,float]] = field(default_factory=list)
    funding_rate: float = 0.0
    next_funding_ms: Optional[int] = None
    oi_points: deque = field(default_factory=lambda: deque(maxlen=120))  # (ts_ms, oi_float)
    liq_events: deque = field(default_factory=lambda: deque(maxlen=10000))  # (ts, price, side, notional)
    last_signal_ts: int = 0
    cooldown_ts: Dict[str, int] = field(default_factory=dict)

class Market:
    def __init__(self):
        self.symbols: List[str] = []
        self.state: Dict[str, SymbolState] = defaultdict(SymbolState)
        self.last_ws_msg_ts: int = now_ms()
        self.last_signal_sent_ts: int = 0

# =========================
# Heatmap
# =========================
def price_bin(price: float, bin_bps: int) -> float:
    step = max(1e-6, price * (bin_bps / 10000.0))
    bins = round(price / step)
    return bins * step

def heatmap_top_clusters(st: SymbolState, last_price: float) -> Tuple[List[Tuple[float,float]], List[Tuple[float,float]]]:
    cutoff = now_ms() - HEATMAP_WINDOW_SEC * 1000
    by_bin_buy: Dict[float, float] = defaultdict(float)
    by_bin_sell: Dict[float, float] = defaultdict(float)
    for ts, p, side, notional in st.liq_events:
        if ts < cutoff: continue
        b = price_bin(p, HEATMAP_BIN_BPS)
        if side == "Buy":  by_bin_buy[b]  += notional
        if side == "Sell": by_bin_sell[b] += notional
    ups  = sorted([(b, v) for b,v in by_bin_buy.items()  if b > last_price], key=lambda x: abs(x[0]-last_price))[:HEATMAP_TOP_K]
    dows = sorted([(b, v) for b,v in by_bin_sell.items() if b < last_price], key=lambda x: abs(x[0]-last_price))[:HEATMAP_TOP_K]
    return ups, dows

# =========================
# Сигнальный движок (15m)
# =========================
class Engine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def _oi_delta(self, st: SymbolState, minutes: int) -> float:
        if len(st.oi_points) < 2: return 0.0
        target = now_ms() - minutes*60*1000
        prev = None; last = st.oi_points[-1]
        for i in range(len(st.oi_points)-1, -1, -1):
            ts, v = st.oi_points[i]
            if ts <= target:
                prev = (ts, v); break
        if not prev: prev = st.oi_points[0]
        oi0 = float(prev[1]); oi1 = float(last[1])
        if oi0 <= 0: return 0.0
        return (oi1 - oi0) / oi0

    def _side_bias(self, oi_pct_15: float, c_prev: float, c_now: float) -> str:
        price_up = c_now > c_prev
        if price_up and oi_pct_15 > 0:  return "CONT_UP"
        if price_up and oi_pct_15 <= 0: return "REV_DOWN"
        if not price_up and oi_pct_15 > 0: return "CONT_DOWN"
        return "REV_UP"  # not price_up and oi<=0

    def _rr(self, entry: float, tp: float, sl: float, side: str) -> float:
        reward = (tp - entry) if side == "LONG" else (entry - tp)
        risk   = (entry - sl) if side == "LONG" else (sl - entry)
        return (reward / risk) if risk > 0 else 0.0

    def _pick_tps(self, side: str, entry: float, atr15: float, clusters_up, clusters_dn) -> Tuple[float, Optional[float]]:
        if side == "LONG":
            tp1 = clusters_up[0][0] if clusters_up else entry * (1.0 + max(TP_MIN_PCT, 0.6 * atr15 / max(1e-9, entry)))
            tp2 = clusters_up[1][0] if len(clusters_up) > 1 else None
        else:
            tp1 = clusters_dn[0][0] if clusters_dn else entry * (1.0 - max(TP_MIN_PCT, 0.6 * atr15 / max(1e-9, entry)))
            tp2 = clusters_dn[1][0] if len(clusters_dn) > 1 else None
        return tp1, tp2

    def on_15m_close(self, sym: str) -> Optional[Dict[str, Any]]:
        st = self.mkt.state[sym]
        K15 = st.k15; K60 = st.k60
        if len(K15) < max(ATR_PERIOD_15+2, VOL_SMA_15+2) or len(K60) < EMA_PERIOD_1H+2:
            return None

        o,h,l,c,v = K15[-1]
        atr15 = atr(K15, ATR_PERIOD_15)
        vols = [x[4] for x in K15]; v_sma = sma(vols, VOL_SMA_15)
        vol_ok = v_sma > 0 and v >= VOL_MULT_ENTRY * v_sma

        closes_1h = [x[3] for x in K60]
        ema100_1h = ema(closes_1h, EMA_PERIOD_1H)
        vwap15, vwap_slope = rolling_vwap(K15, VWAP_WINDOW_15)
        vwap_bias_up = (c > vwap15 and vwap_slope > 0)
        vwap_bias_dn = (c < vwap15 and vwap_slope < 0)

        fr = st.funding_rate
        oi_pct_15 = self._oi_delta(st, OI_WINDOW_MIN)

        c_prev = K15[-2][3]
        bias = self._side_bias(oi_pct_15, c_prev, c)

        ups, dns = heatmap_top_clusters(st, c)

        long_block  = fr >= FUNDING_EXTREME_POS
        short_block = fr <= FUNDING_EXTREME_NEG

        side: Optional[str] = None
        reason: List[str] = []

        if bias == "CONT_UP" and not long_block and (vwap_bias_up or c > ema100_1h):
            side = "LONG"; reason += ["Продолжение: ↑цена + ↑OI", "VWAP/EMA100 поддерживают рост"]
        if not side and bias == "CONT_DOWN" and not short_block and (vwap_bias_dn or c < ema100_1h):
            side = "SHORT"; reason += ["Продолжение: ↓цена + ↑OI", "VWAP/EMA100 поддерживают падение"]
        if not side and bias == "REV_UP" and not long_block and vol_ok:
            side = "LONG"; reason += ["Де-левередж на падении: ↓цена + ↓OI", "Объём подтверждает отскок"]
        if not side and bias == "REV_DOWN" and not short_block and vol_ok:
            side = "SHORT"; reason += ["Слабость роста: ↑цена + ↓OI", "Объём подтверждает разворот"]

        if not side: return None

        if side == "LONG":
            sl = min(l - 0.2*atr15, c - 0.8*atr15)
        else:
            sl = max(h + 0.2*atr15, c + 0.8*atr15)

        tp1, tp2 = self._pick_tps(side, c, atr15, ups, dns)
        tp_pct = (tp1 - c)/c if side=="LONG" else (c - tp1)/c
        if tp_pct < TP_MIN_PCT: return None
        if tp_pct > TP_MAX_PCT:
            tp1 = c * (1.0 + TP_MAX_PCT) if side=="LONG" else c * (1.0 - TP_MAX_PCT)

        rr = self._rr(c, tp1, sl, side)
        if rr < RR_MIN: return None

        # кулдаун на сторону
        nowt = now_ms()
        last = st.cooldown_ts.get(side, 0)
        if nowt - last < SIGNAL_COOLDOWN_SEC * 1000:
            return None
        st.cooldown_ts[side] = nowt

        return {
            "symbol": sym, "side": side, "entry": float(c),
            "tp1": float(tp1), "tp2": float(tp2) if tp2 else None,
            "sl": float(sl), "rr": float(rr),
            "funding": float(fr), "oi15": float(oi_pct_15),
            "vwap_bias": ("UP" if vwap_bias_up else ("DOWN" if vwap_bias_dn else "NEUTRAL")),
            "ema100_1h": float(ema100_1h),
            "heat_up": ups, "heat_dn": dns,
            "reason": reason,
            "next_funding_ms": st.next_funding_ms,
        }

# =========================
# Формат сообщения
# =========================
def fmt_signal(sig: Dict[str, Any]) -> str:
    sym = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; tp1 = sig["tp1"]; sl = sig["sl"]; rr = sig["rr"]
    tp1_pct = (tp1 - entry)/entry if side=="LONG" else (entry - tp1)/entry
    tp2 = sig.get("tp2")
    fr = sig.get("funding", 0.0)
    ups = sig.get("heat_up") or []; dns = sig.get("heat_dn") or []
    next_f = sig.get("next_funding_ms")
    nf = ""
    if next_f:
        mins = max(0, int((next_f - now_ms())/60000)); nf = f" (через ~{mins} мин)" if mins else " (скоро)"
    heat_line = "Heatmap: "
    heat_line += ("вверху≈" + ", ".join(f"{p:g}" for p,_ in ups[:2]) if side=="LONG" else
                  "внизу≈" + ", ".join(f"{p:g}" for p,_ in dns[:2]))

    reasons = "".join(f"\n- {r}" for r in (sig.get("reason") or []))
    warn_lines = []
    if fr >= FUNDING_EXTREME_POS: warn_lines.append("Высокий фандинг — лонги дороже.")
    if fr <= FUNDING_EXTREME_NEG: warn_lines.append("Низкий фандинг — шорты дороже.")

    lines = [
        f"🎯 <b>ФЬЮЧЕРСЫ | {side} SIGNAL</b> на <b>[{sym}]</b> (15m/5m)",
        "<b>Параметры:</b>",
        f"- <b>Funding Rate:</b> {fr:+.4%}{nf}",
        f"- {heat_line}",
        f"<b>Вход:</b> {entry:g}",
        f"<b>Стоп-Лосс:</b> {sl:g}",
        f"<b>Тейк-Профит 1:</b> {tp1:g} ({pct(tp1_pct)})" + (f"\n<b>Тейк-Профит 2:</b> {tp2:g}" if tp2 else ""),
        "<b>Обоснование:</b>" + reasons if reasons else None,
        "<b>Риск:</b> плечо ≤ x10; риск ≤ 1% депозита.",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
    ]
    return "\n".join([x for x in lines if x])

# =========================
# TG команды
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
                if not msg: continue
                chat_id = msg.get("chat", {}).get("id")
                text = (msg.get("text") or "").strip()
                if not isinstance(chat_id, int) or not text.startswith("/"): continue
                if chat_id not in ALLOWED_CHAT_IDS and chat_id not in PRIMARY_RECIPIENTS: continue
                cmd = text.split()[0].lower()

                if cmd == "/ping":
                    ago = (now_ms() - mkt.last_ws_msg_ts)/1000.0
                    await tg.send(chat_id, f"pong • WS last msg {ago:.1f}s ago • symbols={len(mkt.symbols)}")

                elif cmd == "/status":
                    silent_line = "—" if mkt.last_signal_sent_ts == 0 else f"{(now_ms()-mkt.last_signal_sent_ts)/60000.0:.1f}m"
                    await tg.send(chat_id,
                        "✅ Online\n"
                        f"Symbols: {len(mkt.symbols)}\n"
                        "Mode: Derivatives (Funding + OI + All-Liquidations)\n"
                        f"TP≥{pct(TP_MIN_PCT)} • RR≥{RR_MIN:.2f}\n"
                        f"Silent (signals): {silent_line}")

                elif cmd == "/help":
                    await tg.send(chat_id,
                        "Команды:\n"
                        "/ping — пинг\n"
                        "/status — статус\n"
                        "/diag — диагностика буферов и метрик\n"
                        "/jobs — фоновые задачи\n"
                        "/metrics <SYMBOL> — Funding/OI/Heatmap")

                elif cmd == "/jobs":
                    jobs = []
                    for k in ("ws_task","keepalive_task","watchdog_task","tg_task","universe_task"):
                        t = app.get(k); jobs.append(f"{k}: {'running' if (t and not t.done()) else 'stopped'}")
                    await tg.send(chat_id, "Jobs:\n" + "\n".join(jobs))

                elif cmd == "/diag":
                    # краткая диагностика
                    k15_pts = sum(len(mkt.state[s].k15) for s in mkt.symbols)
                    k5_pts  = sum(len(mkt.state[s].k5) for s in mkt.symbols)
                    k60_pts = sum(len(mkt.state[s].k60) for s in mkt.symbols)
                    ago = (now_ms() - mkt.last_ws_msg_ts)/1000.0
                    silent_line = "—" if mkt.last_signal_sent_ts == 0 else f"{(now_ms()-mkt.last_signal_sent_ts)/60000.0:.1f}m"
                    head = ", ".join(mkt.symbols[:10]) if mkt.symbols else "—"
                    await tg.send(chat_id,
                        "Diag:\n"
                        f"WS last msg: {ago:.1f}s ago\n"
                        f"Symbols: {len(mkt.symbols)} (head: {head})\n"
                        f"Kline buffers: 15m={k15_pts} • 5m={k5_pts} • 60m={k60_pts}\n"
                        f"Silent (signals): {silent_line}")

                elif cmd.startswith("/metrics"):
                    parts = text.split()
                    if len(parts) < 2:
                        await tg.send(chat_id, "Формат: /metrics SYMBOL\nПример: /metrics BTCUSDT")
                    else:
                        s = parts[1].upper()
                        st = mkt.state.get(s)
                        if not st or not st.k15:
                            await tg.send(chat_id, "Нет данных. Буферы ещё наполняются.")
                        else:
                            # простая сводка
                            oi_15 = Engine(mkt)._oi_delta(st, OI_WINDOW_MIN)
                            ups, dns = heatmap_top_clusters(st, st.k15[-1][3])
                            hf = lambda arr: ", ".join(f"{p:g}" for p,_ in arr) if arr else "—"
                            next_f = ""
                            if st.next_funding_ms:
                                mins = max(0, int((st.next_funding_ms - now_ms())/60000))
                                next_f = f"через ~{mins} мин"
                            await tg.send(chat_id,
                                f"📈 <b>{s}</b>\nFunding: {st.funding_rate:+.4%} {('('+next_f+')' if next_f else '')}\n"
                                f"OIΔ(15m): {oi_15:+.2%}\n"
                                f"Heatmap up: {hf(ups)}\nHeatmap down: {hf(dns)}")

                else:
                    await tg.send(chat_id, "Неизвестная команда. /help")

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("tg_loop error")
            await asyncio.sleep(2)

# =========================
# WS handler
# =========================
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    mkt: Market = app["mkt"]; tg: Tg = app["tg"]; eng: Engine = app["engine"]

    topic = data.get("topic") or ""
    mkt.last_ws_msg_ts = now_ms()

    if topic.startswith("tickers."):
        d = data.get("data") or {}
        sym = d.get("symbol")
        if not sym: return
        st = mkt.state[sym]
        with contextlib.suppress(Exception):
            st.funding_rate = float(d.get("fundingRate") or 0.0)
        with contextlib.suppress(Exception):
            st.next_funding_ms = int(d.get("nextFundingTime")) if d.get("nextFundingTime") else None
        with contextlib.suppress(Exception):
            oi = float(d.get("openInterest") or 0.0)
            st.oi_points.append((now_ms(), oi))

    elif topic.startswith(f"kline.{EXEC_TF_MAIN}."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            st = mkt.state[sym]
            for p in payload:
                o,h,l,c,v = float(p["open"]), float(p["high"]), float(p["low"]), float(p["close"]), float(p.get("volume") or 0.0)
                if p.get("confirm") is False and st.k15:
                    st.k15[-1] = (o,h,l,c,v)
                else:
                    st.k15.append((o,h,l,c,v))
                    if len(st.k15) > 900: st.k15 = st.k15[-900:]
                if p.get("confirm") is True:
                    sig = eng.on_15m_close(sym)
                    if sig:
                        text = fmt_signal(sig)
                        for chat_id in (PRIMARY_RECIPIENTS if ONLY_CHANNEL else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS)):
                            with contextlib.suppress(Exception):
                                await tg.send(chat_id, text)
                        mkt.last_signal_sent_ts = now_ms()
                        mkt.state[sym].last_signal_ts = now_ms()

    elif topic.startswith(f"kline.{EXEC_TF_AUX}."):
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
                    if len(st.k5) > 900: st.k5 = st.k5[-900:]

    elif topic.startswith("kline.60."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            st = mkt.state[sym]
            for p in payload:
                o,h,l,c,v = float(p["open"]), float(p["high"]), float(p["low"]), float(p["close"]), float(p.get("volume") or 0.0)
                if p.get("confirm") is False and st.k60:
                    st.k60[-1] = (o,h,l,c,v)
                else:
                    st.k60.append((o,h,l,c,v))
                    if len(st.k60) > 900: st.k60 = st.k60[-900:]

    elif topic.startswith("allLiquidation."):
        d = data.get("data") or []
        for it in d:
            try:
                sym = it.get("symbol") or topic.split(".")[-1]
                st = mkt.state[sym]
                p = float(it.get("price") or 0.0)
                side = (it.get("side") or "").strip()
                qty = float(it.get("qty") or it.get("size") or 0.0)
                ts  = int(it.get("timestamp") or now_ms())
                if p>0 and side in ("Buy","Sell") and qty>0:
                    st.liq_events.append((ts, p, side, p*qty))
            except Exception:
                continue

# =========================
# Фоновые задачи
# =========================
async def keepalive_loop(app: web.Application) -> None:
    public_url = os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL")
    http: aiohttp.ClientSession = app["http"]
    if not public_url: return
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

# -------- Вселенная символов
async def build_universe_once(rest: BybitRest) -> List[str]:
    """Надёжное построение вселенной c фолбэком на CORE_SYMBOLS.
       Spot-верификация — опциональна (если упала, не блокируем)."""
    symbols: List[str] = []
    try:
        tickers = await rest.tickers_linear()
        pool: List[str] = []
        for t in tickers:
            sym = t.get("symbol") or ""
            if not sym.endswith("USDT"):  # работаем с USDT-перпами
                continue
            try:
                turn = float(t.get("turnover24h") or 0.0)
                vol  = float(t.get("volume24h") or 0.0)
            except Exception:
                continue
            if turn >= TURNOVER_MIN_USD or vol >= VOLUME_MIN_USD:
                pool.append(sym)

        # пробуем SPOT-верификацию, но не делаем её блокирующей
        verified: List[str] = []
        try:
            for s in pool:
                with contextlib.suppress(Exception):
                    spot = await rest.instruments_info("spot", s)
                    if spot: verified.append(s)
        except Exception:
            verified = pool[:]  # если упала — берём без проверки

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
                        args += [f"tickers.{s}", f"kline.{EXEC_TF_MAIN}.{s}", f"kline.{EXEC_TF_AUX}.{s}", f"kline.60.{s}", f"allLiquidation.{s}"]
                    await ws.unsubscribe(args)
                if add:
                    args = []
                    for s in add:
                        args += [f"tickers.{s}", f"kline.{EXEC_TF_MAIN}.{s}", f"kline.{EXEC_TF_AUX}.{s}", f"kline.60.{s}", f"allLiquidation.{s}"]
                    await ws.subscribe(args)
                mkt.symbols = symbols_new
                logger.info(f"[universe] +{len(add)} / -{len(rem)} • total={len(mkt.symbols)}")
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("universe_refresh_loop error")

# =========================
# Web app
# =========================
async def handle_health(request: web.Request) -> web.Response:
    app = request.app; mkt: Market = app["mkt"]
    return web.json_response({
        "ok": True,
        "symbols": mkt.symbols,
        "last_ws_msg_age_sec": int((now_ms() - mkt.last_ws_msg_ts)/1000),
    })

async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    if not TELEGRAM_TOKEN: raise RuntimeError("Не задан TELEGRAM_TOKEN")

    http = aiohttp.ClientSession()
    app["http"] = http
    app["tg"] = Tg(TELEGRAM_TOKEN, http)
    app["rest"] = BybitRest(BYBIT_REST, http)
    app["mkt"] = Market()
    app["engine"] = Engine(app["mkt"])
    app["ws"] = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: ws_on_message(app, data)

    # Надёжная инициализация вселенной
    symbols = await build_universe_once(app["rest"])
    app["mkt"].symbols = symbols
    logger.info(f"symbols: {symbols}")

    # Подключение WS и подписки
    await app["ws"].connect()
    args = []
    for s in symbols:
        args += [f"tickers.{s}", f"kline.{EXEC_TF_MAIN}.{s}", f"kline.{EXEC_TF_AUX}.{s}", f"kline.60.{s}", f"allLiquidation.{s}"]
    if args:
        await app["ws"].subscribe(args)
    else:
        # страховка: подписываемся хотя бы на CORE_SYMBOLS
        fallback = CORE_SYMBOLS[:]
        app["mkt"].symbols = fallback
        fargs = []
        for s in fallback:
            fargs += [f"tickers.{s}", f"kline.{EXEC_TF_MAIN}.{s}", f"kline.{EXEC_TF_AUX}.{s}", f"kline.60.{s}", f"allLiquidation.{s}"]
        await app["ws"].subscribe(fargs)

    # фоновые таски
    app["ws_task"] = asyncio.create_task(app["ws"].run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_task"] = asyncio.create_task(tg_loop(app))
    app["universe_task"] = asyncio.create_task(universe_refresh_loop(app))

    # привет
    try:
        for chat_id in PRIMARY_RECIPIENTS or ALLOWED_CHAT_IDS:
            await app["tg"].send(chat_id, "🟢 Cryptobot v5.1 запущен: деривативы (Funding + OI + All-Liquidations)")
    except Exception:
        logger.warning("startup notify failed")

async def on_cleanup(app: web.Application) -> None:
    for k in ("ws_task","keepalive_task","watchdog_task","tg_task","universe_task"):
        t = app.get(k)
        if t:
            t.cancel()
            with contextlib.suppress(Exception): await t
    if app.get("ws") and app["ws"].ws and not app["ws"].ws.closed:
        await app["ws"].ws.close()
    if app.get("http"):
        await app["http"].close()

def make_app() -> web.Application:
    app
