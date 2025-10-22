# -*- coding: utf-8 -*-
"""
Cryptobot — Derivatives Signals (Bybit V5, USDT Perpetuals)
v8.1 — Long polling fix (deleteWebhook) + RSI 5m signals + ATR targets + confirmations
"""
from __future__ import annotations
import asyncio
import contextlib
import json
import logging
import os
import time
from collections import defaultdict
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
ONLY_CHANNEL = True  # отправлять только в канал (если есть), иначе всем разрешённым chat_id

# Вселенная
UNIVERSE_REFRESH_SEC = 600
TURNOVER_MIN_USD = float(os.getenv("TURNOVER_MIN_USD", "5000000"))
VOLUME_MIN_USD  = float(os.getenv("VOLUME_MIN_USD",  "5000000"))
ACTIVE_SYMBOLS  = int(os.getenv("ACTIVE_SYMBOLS",     "60"))
CORE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "TONUSDT"]

# Таймфреймы
EXEC_TF_AUX  = "5"    # 5m — генерация сигналов

# Индикаторные периоды
ATR_PERIOD_15   = int(os.getenv("ATR_PERIOD_15",   "14"))
VOL_SMA_15      = int(os.getenv("VOL_SMA_15",      "20"))

# Пороги/режимы
VOLUME_SPIKE_MULT = float(os.getenv("VOLUME_SPIKE_MULT", "2.0"))

# TP/SL и риск
ATR_SL_MULT  = float(os.getenv("ATR_SL_MULT",  "0.8"))
ATR_TP_MULT  = float(os.getenv("ATR_TP_MULT",  "1.2"))
TP_MIN_PCT   = float(os.getenv("TP_MIN_PCT",   "0.01"))   # >=1%
TP_MAX_PCT   = float(os.getenv("TP_MAX_PCT",   "0.015"))  # <=1.5%
RR_MIN       = float(os.getenv("RR_MIN",       "1.5"))

# Антиспам / надёжность
SIGNAL_COOLDOWN_SEC = int(os.getenv("SIGNAL_COOLDOWN_SEC", "90"))
KEEPALIVE_SEC = 13 * 60
WATCHDOG_SEC  = 60
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
# Индикаторы
# =========================
def sma(values: List[float], period: int) -> float:
    if not values or period <= 0:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    return sum(values[-period:]) / period

def atr(data: List[Tuple[float, float, float, float, float]], period: int) -> float:
    if len(data) < period + 1:
        return 0.0
    total = 0.0
    # берём последние `period` интервалов, каждый TR зависит от предыдущего close
    for i in range(len(data) - period, len(data)):
        high = data[i][1]; low = data[i][2]; prev_close = data[i-1][3]
        tr = max(high - low, abs(high - prev_close), abs(prev_close - low))
        total += tr
    return total / period

def rsi14(data: List[Tuple[float, float, float, float, float]]) -> float:
    period = 14
    closes = [bar[3] for bar in data]
    if len(closes) < period + 1:
        return 50.0
    closes = closes[-(period+1):]
    gains = 0.0
    losses = 0.0
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        if d > 0:
            gains += d
        else:
            losses += -d
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# =========================
# Клиенты: WS/Telegram/REST
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
        await self.ws.send_json({"op": "subscribe", "args": topics})

    async def unsubscribe(self, topics: List[str]) -> None:
        if not self.ws:
            raise RuntimeError("WebSocket is not connected")
        await self.ws.send_json({"op": "unsubscribe", "args": topics})

    async def run(self) -> None:
        if not self.ws:
            raise RuntimeError("WebSocket is not connected")
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if self.on_message:
                        res = self.on_message(data)
                        if asyncio.iscoroutine(res):
                            asyncio.create_task(res)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        except asyncio.CancelledError:
            pass
        except Exception:
            logging.exception("WebSocket run error")
        finally:
            if self.ws and not self.ws.closed:
                await self.ws.close()

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
        payload: Dict[str, Any] = {"timeout": timeout, "allowed_updates": ["message", "channel_post", "my_chat_member"]}
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

# =========================
# Состояние рынка
# =========================
@dataclass
class SymbolState:
    k5:  List[Tuple[float,float,float,float,float]] = field(default_factory=list)
    last_signal_ts: int = 0
    cooldown_ts: Dict[str, int] = field(default_factory=dict)

class Market:
    def __init__(self):
        self.symbols: List[str] = []
        self.state: Dict[str, SymbolState] = defaultdict(SymbolState)
        self.last_ws_msg_ts: int = now_ms()
        self.last_signal_sent_ts: int = 0

# =========================
# Логика сигналов (5m RSI)
# =========================
class Engine:
    def __init__(self, mkt: Market):
        self.mkt = mkt

    def on_5m_close(self, sym: str) -> Optional[Dict[str, Any]]:
        st = self.mkt.state[sym]
        K5 = st.k5
        if len(K5) < max(31, VOL_SMA_15 + 1, ATR_PERIOD_15 + 1):
            return None

        now_s = int(time.time())
        def cooled(side: str) -> bool:
            return (now_s - st.cooldown_ts.get(side, 0)) >= SIGNAL_COOLDOWN_SEC

        # RSI выход из зон
        prev_rsi = rsi14(K5[:-1])
        curr_rsi = rsi14(K5)
        long_signal  = prev_rsi < 30 <= curr_rsi
        short_signal = prev_rsi > 70 >= curr_rsi
        if not long_signal and not short_signal:
            return None

        side = "LONG" if long_signal else "SHORT"
        if not cooled(side):
            return None

        # Подтверждения
        confirm_extreme = False
        confirm_pattern = False
        confirm_volume  = False
        confirm_diverg  = False
        reasons: List[str] = ["RSI вышел из " + ("перепроданности" if side=="LONG" else "перекупленности")]

        # Экстремумы (ложный пробой за 30 свечей)
        lookback = 30
        if side == "LONG":
            local_min = min(r[2] for r in K5[-(lookback+1):-1])
            if K5[-1][2] <= local_min:
                confirm_extreme = True
                reasons.append(f"Обновлён локальный минимум ({lookback} св.)")
        else:
            local_max = max(r[1] for r in K5[-(lookback+1):-1])
            if K5[-1][1] >= local_max:
                confirm_extreme = True
                reasons.append(f"Обновлён локальный максимум ({lookback} св.)")

        # Свечной паттерн
        o,h,l,c,v = K5[-1]
        if side == "LONG":
            body = abs(c - o); upper = h - max(c, o); lower = min(c, o) - l
            # молот
            if c > o and lower >= 2*body and upper <= 0.5*body:
                confirm_pattern = True; reasons.append("Паттерн: молот")
            else:
                # бычье поглощение
                o2,h2,l2,c2,_ = K5[-2]
                if c2 < o2 and c > o and c >= o2 and o <= c2:
                    confirm_pattern = True; reasons.append("Паттерн: бычье поглощение")
        else:
            body = abs(c - o); upper = h - max(c, o); lower = min(c, o) - l
            # пин-бар
            if o > c and upper >= 2*body and lower <= 0.5*body:
                confirm_pattern = True; reasons.append("Паттерн: пин-бар")
            else:
                # медвежье поглощение
                o2,h2,l2,c2,_ = K5[-2]
                if c2 > o2 and c < o and o >= c2 and c <= o2:
                    confirm_pattern = True; reasons.append("Паттерн: медвежье поглощение")

        # Объёмный всплеск
        avg_vol = sma([r[4] for r in K5[-(VOL_SMA_15+1):-1]], VOL_SMA_15)
        if avg_vol > 0 and v >= VOLUME_SPIKE_MULT * avg_vol:
            confirm_volume = True
            reasons.append(f"Объёмный всплеск ≥ {VOLUME_SPIKE_MULT}×SMA{VOL_SMA_15}")

        # Дивергенция RSI (если был экстремум)
        if confirm_extreme:
            if side == "LONG":
                prev_low_idx = min(range(len(K5)-lookback, len(K5)-1), key=lambda i: K5[i][2])
                rsi_prev_low = rsi14(K5[:prev_low_idx+1])
                if curr_rsi > rsi_prev_low:
                    confirm_diverg = True; reasons.append("Бычья дивергенция RSI")
            else:
                prev_high_idx = max(range(len(K5)-lookback, len(K5)-1), key=lambda i: K5[i][1])
                rsi_prev_high = rsi14(K5[:prev_high_idx+1])
                if curr_rsi < rsi_prev_high:
                    confirm_diverg = True; reasons.append("Медвежья дивергенция RSI")

        conf_count = int(confirm_extreme) + int(confirm_pattern) + int(confirm_volume) + int(confirm_diverg)
        if conf_count < 2:
            return None
        strength = "сильный" if conf_count >= 3 else "слабый"

        # ATR SL/TP, TP в пределах [1%; 1.5%]
        a = atr(K5, ATR_PERIOD_15)
        entry = K5[-1][3]
        if side == "LONG":
            sl = max(1e-9, entry - ATR_SL_MULT * a)
            tp = entry + ATR_TP_MULT * a
            tp_pct = (tp - entry)/entry
            if tp_pct < TP_MIN_PCT: tp = entry * (1 + TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT: tp = entry * (1 + TP_MAX_PCT)
            rr = (tp - entry) / max(1e-9, (entry - sl))
        else:
            sl = entry + ATR_SL_MULT * a
            tp = entry - ATR_TP_MULT * a
            tp_pct = (entry - tp)/entry
            if tp_pct < TP_MIN_PCT: tp = entry * (1 - TP_MIN_PCT)
            if tp_pct > TP_MAX_PCT: tp = entry * (1 - TP_MAX_PCT)
            rr = (entry - tp) / max(1e-9, (sl - entry))
        if rr < RR_MIN:
            return None

        st.cooldown_ts[side] = now_s
        return {
            "symbol": sym,
            "side": side,
            "entry": float(entry),
            "tp1": float(tp),
            "tp2": None,
            "sl": float(sl),
            "rr": float(rr),
            "reason": reasons,
            "rsi14": round(curr_rsi, 2),
            "strength": strength
        }

# =========================
# Форматирование сигнала
# =========================
def fmt_signal(sig: Dict[str, Any]) -> str:
    sym = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; tp1 = sig["tp1"]; sl = sig["sl"]; rr = sig["rr"]
    tp1_pct = (tp1 - entry)/entry if side == "LONG" else (entry - tp1)/entry
    rsi_val = sig.get("rsi14"); strength = sig.get("strength")
    reasons = "".join(f"\n- {r}" for r in (sig.get("reason") or []))
    emoji = "🟢" if side == "LONG" else "🔴"
    lines = [
        f"{emoji} <b>{side} SIGNAL</b> — <b>{sym}</b> (5m)",
        "<b>Параметры:</b>",
        f"- <b>Сила сигнала:</b> {strength}" if strength else None,
        f"- <b>RSI(14):</b> {rsi_val:.2f}" if rsi_val is not None else None,
        f"<b>Текущая цена:</b> {entry:g}",
        f"<b>Вход:</b> {entry:g}",
        f"<b>Стоп-Лосс:</b> {sl:g}",
        f"<b>Тейк-Профит:</b> {tp1:g} ({pct(tp1_pct)})",
        "<b>Причины:</b>" + reasons if reasons else None,
        f"<b>Риск:</b> RR≈{rr:.2f} • плечо ≤ x10; риск ≤ 1% депозита.",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
    ]
    return "\n".join([x for x in lines if x])

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
                        "/diag — диагностика\n"
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

    if topic.startswith(f"kline.{EXEC_TF_AUX}."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            st = mkt.state[sym]
            for p in payload:
                o = float(p["open"]); h = float(p["high"]); l = float(p["low"])
                c = float(p["close"]); v = float(p.get("volume") or 0.0)
                if p.get("confirm") is False and st.k5:
                    st.k5[-1] = (o,h,l,c,v)
                else:
                    st.k5.append((o,h,l,c,v))
                    if len(st.k5) > 900:
                        st.k5 = st.k5[-900:]
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
# Вселенная тикеров
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
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("universe_refresh_loop error")

# =========================
# Web app
# =========================
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
    # Критично для long polling: снять webhook, чтобы не ловить 409 Conflict
    try:
        await app["tg"].delete_webhook(drop_pending_updates=True)
        logger.info("Telegram webhook deleted (drop_pending_updates=True)")
    except Exception:
        logger.exception("Failed to delete Telegram webhook (harmless in polling mode)")

    app["rest"] = BybitRest(BYBIT_REST, http)
    app["mkt"] = Market()
    app["engine"] = Engine(app["mkt"])
    app["ws"] = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http)
    app["ws"].on_message = lambda data: asyncio.create_task(ws_on_message(app, data))

    # Вселенная и подписка
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

    # Фоновые задачи
    app["ws_task"] = asyncio.create_task(app["ws"].run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_task"] = asyncio.create_task(tg_loop(app))
    app["universe_task"] = asyncio.create_task(universe_refresh_loop(app))

    # Уведомление
    try:
        for chat_id in PRIMARY_RECIPIENTS or ALLOWED_CHAT_IDS:
            await app["tg"].send(chat_id, f"🟢 Cryptobot v8.1: polling mode enabled, RSI 5m signals live")
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
    logger.info("Starting Cryptobot v8.1 — TF=5m, RSI reversal signals (long polling)")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
```0
