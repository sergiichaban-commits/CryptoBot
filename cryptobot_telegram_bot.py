# -*- coding: utf-8 -*-
"""
Cryptobot — Telegram сигналы (Bybit V5 WebSocket, SMC-lite) + командный интерфейс

Что добавлено в этой версии:
- Обработчик Telegram-команд через long-polling (getUpdates) без вебхука.
- Команды: /ping, /status, /diag, /jobs, /universe, /help
- Ответы приходят только от чатов из ALLOWED_CHAT_IDS (и каналов из PRIMARY_RECIPIENTS).

Остальное:
- WebSocket V5 (public/linear): tickers, kline.1, kline.5, liquidation (опц.)
- Быстрый движок сигналов на закрытии 1m, SMC-lite (BOS/CHOCH, свип, FVG, простой OB)
- TP/SL: структурные цели + ATR-клапаны; RR ≥ 1:1.5
- Нужен только TELEGRAM_TOKEN; все прочие параметры — в коде ниже.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web

# =========================
# Жёстко заданные параметры (меняются прямо здесь)
# =========================
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
LOG_LEVEL = "INFO"

# Символы и фильтры
ACTIVE_SYMBOLS = 30                # подписываем топ N USDT-перпетуалов
PROB_MIN = 0.58                    # минимум «вероятности» (эвристика)
TP_MIN_PCT = 0.010                 # TP нижний зажим (1%)
TP_MAX_PCT = 0.060                 # TP верхний зажим (6%)
ATR_PERIOD_1M = 14                 # ATR по 1m
BODY_ATR_MULT = 0.55                # порог «сильной» свечи: тело/ATR
VOL_SMA_PERIOD = 20                # SMA по объёму (1m)
VOL_MULT = 1.5                     # всплеск объёма >= 2x SMA
SIGNAL_COOLDOWN_SEC = 30           # антиспам по направлению

# SMC-параметры
SWING_FRAC = 2                     # фрактал для свингов (2 — классика)
USE_FVG = True                     # требовать FVG (3-свечная «чистая» зона)
USE_SWEEP = True                   # искать свип ликвидности перед входом
RR_TARGET = 1.4                    # минимальный RR для допуска сигнала
USE_5M_FILTER = True               # фильтр по 5m структуре (не торгуем против явного тренда 5м)
ALIGN_5M_STRICT = False            # если True — требовать BOS на 5m в сторону сделки

# Телеметрия / веб
HEARTBEAT_SEC = 60 * 60            # ⬅️ телеграм «я жив» теперь раз в 60 минут
KEEPALIVE_SEC = 13 * 60            # пинг публичного URL
WATCHDOG_SEC = 60                  # строка «жив» в лог раз в 60s
PORT = 10000                       # web порт

# Роутинг по умолчанию (куда слать сигналы; кто может писать боту)
PRIMARY_RECIPIENTS = [-1002870952333]     # канал(ы) для сигналов
ALLOWED_CHAT_IDS = [533232884, -1002870952333]  # кто может использовать команды
ONLY_CHANNEL = True                        # если True — сигналы только в PRIMARY_RECIPIENTS

# =========================
# Утилиты
# =========================
def pct(x: float) -> str:
    return f"{x:.2%}"

def now_ts_ms() -> int:
    return int(time.time() * 1000)

def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt)

logger = logging.getLogger("cryptobot")

# =========================
# Telegram
# =========================
class Tg:
    def __init__(self, token: str, session: aiohttp.ClientSession) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = session

    async def send(self, chat_id: int, text: str) -> None:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        async with self.http.post(f"{self.base}/sendMessage", json=payload) as r:
            r.raise_for_status()
            await r.json()

    async def get_updates(self, offset: Optional[int] = None, timeout: int = 25) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"timeout": timeout, "allowed_updates": ["message", "channel_post", "my_chat_member"]}
        if offset is not None:
            payload["offset"] = offset
        async with self.http.post(f"{self.base}/getUpdates", json=payload, timeout=aiohttp.ClientTimeout(total=timeout+10)) as r:
            r.raise_for_status()
            return await r.json()

# =========================
# Bybit REST (стартер)
# =========================
class BybitRest:
    def __init__(self, base: str, session: aiohttp.ClientSession) -> None:
        self.base = base.rstrip("/")
        self.http = session

    async def tickers_linear(self) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/tickers?category=linear"
        async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []

# =========================
# Bybit WebSocket
# =========================
class BybitWS:
    def __init__(self, url: str, session: aiohttp.ClientSession) -> None:
        self.url = url
        self.http = session
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._subs: List[str] = []
        self._ping_task: Optional[asyncio.Task] = None
        self.on_message: Optional[callable] = None

    async def connect(self) -> None:
        if self.ws and not self.ws.closed:
            return
        logger.info(f"WS connecting: {self.url}")
        self.ws = await self.http.ws_connect(self.url, heartbeat=25)
        if self._ping_task is None or self._ping_task.done():
            self._ping_task = asyncio.create_task(self._ping_loop())
        if self._subs:
            await self.subscribe(self._subs)

    async def _ping_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(20)
                if not self.ws or self.ws.closed:
                    continue
                await self.ws.send_json({"op": "ping"})
            except Exception as e:
                logger.warning(f"WS ping error: {e}")

    async def subscribe(self, args: List[str]) -> None:
        for a in args:
            if a not in self._subs:
                self._subs.append(a)
        if not self.ws or self.ws.closed:
            await self.connect()
        if not args:
            return
        await self.ws.send_json({"op": "subscribe", "args": args})
        logger.info(f"WS subscribed: {args}")

    async def run(self) -> None:
        assert self.ws is not None, "call connect() first"
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue
                if data.get("op") in {"subscribe", "ping", "pong"} or "success" in data:
                    continue
                if self.on_message:
                    try:
                        await self.on_message(data)
                    except Exception as e:
                        logger.exception(f"on_message error: {e}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
        await asyncio.sleep(2)
        with contextlib.suppress(Exception):
            await self.connect()
        if self.ws and not self.ws.closed:
            await self.run()

# =========================
# Рыночное состояние (1m и 5m)
# =========================
class MarketState:
    def __init__(self) -> None:
        self.tickers: Dict[str, Dict[str, Any]] = {}
        # kline по таймфреймам: {"1": {sym: [(o,h,l,c,v), ...]}, "5": { ... }}
        self.kline: Dict[str, Dict[str, List[Tuple[float,float,float,float,float]]]] = {"1": {}, "5": {}}
        self.kline_maxlen = 600
        self.liq_events: Dict[str, List[int]] = {}
        self.last_ws_msg_ts: int = now_ts_ms()
        self.cooldown: Dict[Tuple[str,str], int] = {}  # (sym, side) -> ts

    def note_ticker(self, d: Dict[str, Any]) -> None:
        sym = d.get("symbol")
        if not sym:
            return
        self.tickers[sym] = d
        self.last_ws_msg_ts = now_ts_ms()

    def note_kline(self, tf: str, sym: str, points: List[Dict[str, Any]]) -> None:
        buf = self.kline.setdefault(tf, {}).setdefault(sym, [])
        for p in points:
            o = float(p["open"]); h = float(p["high"]); l = float(p["low"]); c = float(p["close"])
            v = float(p.get("volume") or p.get("turnover") or 0.0)
            if p.get("confirm") is False and buf:
                buf[-1] = (o,h,l,c,v)
            else:
                buf.append((o,h,l,c,v))
                if len(buf) > self.kline_maxlen:
                    del buf[0:len(buf)-self.kline_maxlen]
        self.last_ws_msg_ts = now_ts_ms()

    def note_liq(self, sym: str, ts_ms: int) -> None:
        arr = self.liq_events.setdefault(sym, [])
        arr.append(ts_ms)
        cutoff = now_ts_ms() - 5*60*1000
        while arr and arr[0] < cutoff:
            arr.pop(0)
        self.last_ws_msg_ts = now_ts_ms()

# =========================
# SMC-утилиты
# =========================
def atr(rows: List[Tuple[float,float,float,float,float]], period: int) -> float:
    if len(rows) < period + 1:
        return 0.0
    trs: List[float] = []
    for i in range(1, period+1):
        _,h,l,c,_ = rows[-i]
        _,_,_,prev_c,_ = rows[-i-1]
        tr = max(h-l, abs(h - prev_c), abs(prev_c - l))
        trs.append(tr)
    return sum(trs)/period if trs else 0.0

def sma(vals: List[float], period: int) -> float:
    if len(vals) < period or period <= 0:
        return 0.0
    return sum(vals[-period:]) / period

def find_swings(rows: List[Tuple[float,float,float,float,float]], frac: int = SWING_FRAC) -> Tuple[List[int], List[int]]:
    n = len(rows)
    sh: List[int] = []
    sl: List[int] = []
    for i in range(frac, n - frac):
        h = rows[i][1]; l = rows[i][2]
        highs = [rows[j][1] for j in range(i-frac, i+frac+1)]
        lows  = [rows[j][2] for j in range(i-frac, i+frac+1)]
        if h == max(highs) and highs.count(h) == 1:
            sh.append(i)
        if l == min(lows) and lows.count(l) == 1:
            sl.append(i)
    return sh, sl

def last_swing_price(rows, idxs, kind: str) -> Optional[float]:
    if not idxs:
        return None
    i = idxs[-1]
    return rows[i][1] if kind == "H" else rows[i][2]

def bos_choch(rows, sh, sl) -> Tuple[str, bool, bool]:
    if len(rows) < 3 or (not sh and not sl):
        return "RANGE", False, False
    c = rows[-1][3]
    bos_up = False; bos_dn = False
    last_h = last_swing_price(rows, sh, "H")
    last_l = last_swing_price(rows, sl, "L")
    if last_h is not None and c > last_h: bos_up = True
    if last_l is not None and c < last_l: bos_dn = True
    if bos_up and not bos_dn: trend = "UP"
    elif bos_dn and not bos_up: trend = "DOWN"
    else: trend = "UP" if rows[-1][3] > rows[-3][3] else ("DOWN" if rows[-1][3] < rows[-3][3] else "RANGE")
    return trend, bos_up, bos_dn

def has_fvg(rows, bullish: bool) -> bool:
    if len(rows) < 3: return False
    h2 = rows[-3][1]; l2 = rows[-3][2]
    h0 = rows[-1][1]; l0 = rows[-1][2]
    return (l0 > h2) if bullish else (h0 < l2)

def swept_liquidity(rows, sh, sl, side: str) -> bool:
    if side == "LONG":
        if not sl: return False
        key = rows[sl[-1]][2]
        low = rows[-1][2]; close = rows[-1][3]
        return (low < key) and (close > key)
    else:
        if not sh: return False
        key = rows[sh[-1]][1]
        high = rows[-1][1]; close = rows[-1][3]
        return (high > key) and (close < key)

def simple_ob(rows, side: str, body_atr_thr: float, atr_val: float) -> Optional[Tuple[float,float]]:
    if len(rows) < 3 or atr_val <= 0: return None
    o,h,l,c,_ = rows[-1]
    body = abs(c - o)
    if body < body_atr_thr * atr_val: return None
    for i in range(len(rows)-2, max(-1, len(rows)-8), -1):
        o2,h2,l2,c2,_ = rows[i]
        is_opposite = (c2 < o2) if side == "LONG" else (c2 > o2)
        if is_opposite:
            return (l2, max(o2, c2)) if side == "LONG" else (min(o2, c2), h2)
    return None

# =========================
# Движок сигналов
# =========================
class Engine:
    def __init__(self, mkt: MarketState) -> None:
        self.mkt = mkt

    def _probability(self, body_ratio: float, vol_ok: bool, liq_cnt: int, confluence: int) -> float:
        p = 0.45
        p += min(0.3, max(0.0, body_ratio - BODY_ATR_MULT) * 0.2)
        if vol_ok: p += 0.15
        if liq_cnt >= 3: p += 0.05
        p += min(0.1, 0.03 * max(0, confluence-1))
        return max(0.0, min(0.99, p))

    def _nearest_opposite_swing_tp(self, rows, sh, sl, side: str, entry: float) -> Optional[float]:
        if side == "LONG":
            for i in reversed(sh):
                ph = rows[i][1]
                if ph > entry: return ph
            return rows[-1][1]
        else:
            for i in reversed(sl):
                pl = rows[i][2]
                if pl < entry: return pl
            return rows[-1][2]

    def _validate_rr(self, entry: float, tp: float, sl: float, side: str) -> float:
        reward = (tp - entry) if side == "LONG" else (entry - tp)
        risk = (entry - sl) if side == "LONG" else (sl - entry)
        if risk <= 0: return 0.0
        return reward / risk

    def on_kline_closed_1m(self, sym: str) -> Optional[Dict[str, Any]]:
        rows1 = self.mkt.kline["1"].get(sym) or []
        if len(rows1) < max(ATR_PERIOD_1M+3, VOL_SMA_PERIOD+3):
            return None

        atr1 = atr(rows1, ATR_PERIOD_1M)
        if atr1 <= 0: return None
        o,h,l,c,v = rows1[-1]
        body = abs(c - o)
        body_ratio = body / atr1
        vols = [x[4] for x in rows1]
        v_sma = sma(vols, VOL_SMA_PERIOD)
        vol_ok = v > VOL_MULT * v_sma if v_sma > 0 else False
        if body_ratio < BODY_ATR_MULT or not vol_ok:
            return None

        SH1, SL1 = find_swings(rows1, SWING_FRAC)
        trend1, bos_up, bos_dn = bos_choch(rows1, SH1, SL1)

        confluence = 0
        side: Optional[str] = None
        if c > o:
            side = "LONG"
            if bos_up: confluence += 1
            if USE_SWEEP and swept_liquidity(rows1, SH1, SL1, "LONG"): confluence += 1
            if USE_FVG and has_fvg(rows1, bullish=True): confluence += 1
        else:
            side = "SHORT"
            if bos_dn: confluence += 1
            if USE_SWEEP and swept_liquidity(rows1, SH1, SL1, "SHORT"): confluence += 1
            if USE_FVG and has_fvg(rows1, bullish=False): confluence += 1

        ob = simple_ob(rows1, side, BODY_ATR_MULT, atr1)

        if USE_5M_FILTER:
            rows5 = self.mkt.kline["5"].get(sym) or []
            if len(rows5) >= ATR_PERIOD_1M + 3:
                SH5, SL5 = find_swings(rows5, SWING_FRAC)
                trend5, bos5_up, bos5_dn = bos_choch(rows5, SH5, SL5)
                if side == "LONG":
                    if ALIGN_5M_STRICT and not bos5_up: return None
                    if trend5 == "DOWN" and not bos5_up: return None
                else:
                    if ALIGN_5M_STRICT and not bos5_dn: return None
                    if trend5 == "UP" and not bos5_dn: return None

        liq_cnt = sum(1 for t in self.mkt.liq_events.get(sym, []) if t >= now_ts_ms() - 60_000)
        prob = self._probability(body_ratio, vol_ok, liq_cnt, confluence)
        if prob < PROB_MIN: return None

        entry = c
        tp_struct = self._nearest_opposite_swing_tp(rows1, SH1, SL1, side, entry)
        tp_pct_struct = abs(tp_struct - entry) / entry if tp_struct else 0.0
        tp_pct_atr = max(TP_MIN_PCT, 0.6 * atr1 / max(1e-9, entry))
        tp_pct = max(tp_pct_struct, tp_pct_atr)
        tp_pct = max(TP_MIN_PCT, min(TP_MAX_PCT, tp_pct))

        if side == "LONG":
            sl_struct = (rows1[SL1[-1]][2] - 0.1 * atr1) if (USE_SWEEP and SL1) else ((ob[0] - 0.05 * atr1) if ob else (l - 0.2 * atr1))
            sl = sl_struct
            tp = entry * (1.0 + tp_pct)
        else:
            sl_struct = (rows1[SH1[-1]][1] + 0.1 * atr1) if (USE_SWEEP and SH1) else ((ob[1] + 0.05 * atr1) if ob else (h + 0.2 * atr1))
            sl = sl_struct
            tp = entry * (1.0 - tp_pct)

        rr = self._validate_rr(entry, tp, sl, side)
        if rr < RR_TARGET:
            tp = entry * (1.0 + (TP_MAX_PCT if side == "LONG" else -TP_MAX_PCT))
            rr = self._validate_rr(entry, tp, sl, side)
            if rr < RR_TARGET:
                return None

        key = (sym, side)
        last_ts = self.mkt.cooldown.get(key, 0)
        if now_ts_ms() - last_ts < SIGNAL_COOLDOWN_SEC * 1000:
            return None
        self.mkt.cooldown[key] = now_ts_ms()

        return {
            "symbol": sym, "side": side, "entry": float(entry), "tp": float(tp), "sl": float(sl),
            "prob": float(prob), "atr": float(atr1), "body_ratio": float(body_ratio),
            "liq_cnt": int(liq_cnt), "rr": float(rr), "confluence": int(confluence),
        }

# =========================
# Форматирование сообщения
# =========================
def format_signal(sig: Dict[str, Any]) -> str:
    sym = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; tp = sig["tp"]; sl = sig["sl"]
    prob = sig["prob"]; liq_cnt = sig.get("liq_cnt", 0)
    body_ratio = sig.get("body_ratio", 0.0); rr = sig.get("rr", 0.0); con = sig.get("confluence", 0)
    tp_pct = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry
    return "\n".join([
        f"⚡️ <b>Сигнал по {sym}</b>",
        f"Направление: <b>{'LONG' if side=='LONG' else 'SHORT'}</b>",
        f"Текущая цена: <b>{entry:g}</b>",
        f"Тейк: <b>{tp:g}</b> ({pct(tp_pct)})",
        f"Стоп: <b>{sl:g}</b>  •  RR ≈ <b>1:{rr:.2f}</b>",
        f"Вероятность: <b>{pct(prob)}</b>  •  SMC-конфлюэнс: <b>{con}</b>",
        f"Основание: тело/ATR={body_ratio:.2f}; ликвидаций(60с)={liq_cnt}",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
    ])

# =========================
# Командный интерфейс (Telegram)
# =========================
async def tg_updates_loop(app: web.Application) -> None:
    tg: Tg = app["tg"]
    mkt: MarketState = app["mkt"]
    ws: BybitWS = app["ws"]
    offset: Optional[int] = None
    while True:
        try:
            resp = await tg.get_updates(offset=offset, timeout=25)
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("channel_post")
                if not msg: continue
                chat = msg.get("chat", {})
                chat_id = chat.get("id")
                text = msg.get("text") or ""
                if not isinstance(chat_id, int) or not text.startswith("/"):
                    continue
                # допускаем команды только от разрешённых чатов
                if chat_id not in ALLOWED_CHAT_IDS and chat_id not in PRIMARY_RECIPIENTS:
                    continue
                cmd = text.split()[0].lower()
                if cmd == "/help":
                    await tg.send(chat_id, "Доступные команды:\n/universe — список символов\n/ping — проверка связи\n/status — краткий статус\n/jobs — фоновые задачи\n/diag — диагностика движка\n/debug — alias для /diag")
                elif cmd == "/universe":
                    syms = app.get("symbols", [])
                    await tg.send(chat_id, "Подписаны символы:\n" + ", ".join(syms))
                elif cmd == "/ping":
                    ago = (now_ts_ms() - mkt.last_ws_msg_ts)/1000.0
                    await tg.send(chat_id, f"pong • WS last msg {ago:.1f}s ago • symbols={len(app.get('symbols', []))}")
                elif cmd == "/status":
                    ago = (now_ts_ms() - mkt.last_ws_msg_ts)/1000.0
                    cfg = f"prob_min={PROB_MIN:.0%}, tp=[{TP_MIN_PCT:.1%}..{TP_MAX_PCT:.1%}], rr≥{RR_TARGET:.2f}, body/ATR≥{BODY_ATR_MULT}, vol≥{VOL_MULT}×SMA"
                    await tg.send(chat_id, f"✅ Online\nWS: ok (last {ago:.1f}s)\nSymbols: {len(app.get('symbols', []))}\nCfg: {cfg}")
                elif cmd in ("/diag", "/debug"):
                    syms = app.get("symbols", [])
                    k1 = sum(len(mkt.kline['1'].get(s, [])) for s in syms)
                    k5 = sum(len(mkt.kline['5'].get(s, [])) for s in syms)
                    await tg.send(chat_id, f"Diag:\n1m buffers: {k1} pts\n5m buffers: {k5} pts\nCooldowns: {len(mkt.cooldown)} keys")
                elif cmd == "/jobs":
                    ws_alive = bool(ws.ws and not ws.ws.closed)
                    tasks = {k: (not app[k].done()) if app.get(k) else False for k in ["ws_task","keepalive_task","hb_task","watchdog_task","tg_updates_task"]}
                    await tg.send(chat_id, "Jobs:\n"
                                     f"WS connected: {ws_alive}\n" +
                                     "\n".join(f"{k}: {'running' if v else 'stopped'}" for k,v in tasks.items()))
                else:
                    await tg.send(chat_id, "Неизвестная команда. Команды: /help")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"tg_updates_loop error: {e}")
            await asyncio.sleep(2)

# =========================
# Фоновые циклы (keepalive/heartbeat/watchdog)
# =========================
async def keepalive_loop(app: web.Application) -> None:
    public_url: Optional[str] = app["public_url"]
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
            logger.warning(f"keepalive error: {e}")

async def heartbeat_loop(app: web.Application) -> None:
    tg: Tg = app["tg"]
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_SEC)
            text = f"✅ Cryptobot активен • WS подписки • символов: {len(app.get('symbols', []))}"
            for chat_id in PRIMARY_RECIPIENTS:
                await tg.send(chat_id, text)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"heartbeat error: {e}")

async def watchdog_loop(app: web.Application) -> None:
    mkt: MarketState = app["mkt"]
    while True:
        try:
            await asyncio.sleep(WATCHDOG_SEC)
            ago = (now_ts_ms() - mkt.last_ws_msg_ts) / 1000.0
            logger.info(f"[watchdog] alive; last WS msg {ago:.1f}s ago; symbols={len(app.get('symbols', []))}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"watchdog error: {e}")

# =========================
# WS message handler (сигналы)
# =========================
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    mkt: MarketState = app["mkt"]
    tg: Tg = app["tg"]
    eng: Engine = app["engine"]

    topic = data.get("topic") or ""
    if topic.startswith("tickers."):
        d = data.get("data") or {}
        if isinstance(d, dict):
            mkt.note_ticker(d)

    elif topic.startswith("kline.1."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            mkt.note_kline("1", sym, payload)
            if any(x.get("confirm") for x in payload):
                sig = eng.on_kline_closed_1m(sym)
                if sig:
                    text = format_signal(sig)
                    chats = PRIMARY_RECIPIENTS if ONLY_CHANNEL else (ALLOWED_CHAT_IDS or PRIMARY_RECIPIENTS)
                    for chat_id in chats:
                        with contextlib.suppress(Exception):
                            await tg.send(chat_id, text)
                            logger.info(f"signal sent: {sym} {sig['side']}")

    elif topic.startswith("kline.5."):
        payload = data.get("data") or []
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            mkt.note_kline("5", sym, payload)

    elif topic.startswith("liquidation."):
        arr = data.get("data") or []
        for liq in arr:
            if not isinstance(liq, dict): continue
            sym = (liq.get("s") or liq.get("symbol"))
            ts = int(liq.get("T") or data.get("ts") or now_ts_ms())
            if sym:
                mkt.note_liq(sym, ts)

# =========================
# Web-приложение
# =========================
async def handle_health(request: web.Request) -> web.Response:
    app = request.app
    t0 = app.get("start_ts") or time.monotonic()
    uptime = time.monotonic() - t0
    mkt: MarketState = app.get("mkt")
    last_msg_age = int((now_ts_ms() - mkt.last_ws_msg_ts) / 1000) if mkt else None
    return web.json_response({
        "ok": True,
        "uptime_sec": int(uptime),
        "symbols": app.get("symbols", []),
        "last_ws_msg_age_sec": last_msg_age,
    })

async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    logger.info("startup.")

    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        raise RuntimeError("Не задан TELEGRAM_TOKEN")

    http = aiohttp.ClientSession()
    app["http"] = http
    app["tg"] = Tg(token, http)
    app["public_url"] = os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL")

    rest = BybitRest(BYBIT_REST, http)
    tickers = await rest.tickers_linear()
    cands: List[Tuple[str, float]] = []
    for t in tickers:
        sym = t.get("symbol")
        try:
            turn = float(t.get("turnover24h") or 0.0)
        except Exception:
            turn = 0.0
        if sym and sym.endswith("USDT"):
            cands.append((sym, turn))
    cands.sort(key=lambda x: x[1], reverse=True)
    symbols = [s for s,_ in cands[:ACTIVE_SYMBOLS]] or ["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT","DOGEUSDT","TONUSDT","ADAUSDT","LINKUSDT","AVAXUSDT"][:ACTIVE_SYMBOLS]
    app["symbols"] = symbols
    logger.info(f"symbols: {symbols}")

    mkt = MarketState(); app["mkt"] = mkt
    app["engine"] = Engine(mkt)

    ws = BybitWS(BYBIT_WS_PUBLIC_LINEAR, http); app["ws"] = ws
    async def _on_msg(msg: Dict[str, Any]) -> None: await ws_on_message(app, msg)
    ws.on_message = _on_msg
    await ws.connect()

    args: List[str] = []
    for s in symbols:
        args += [f"tickers.{s}", f"kline.1.{s}", f"kline.5.{s}", f"liquidation.{s}"]
    await ws.subscribe(args)

    # фоновые задачи
    app["ws_task"] = asyncio.create_task(ws.run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["hb_task"] = asyncio.create_task(heartbeat_loop(app))
    app["watchdog_task"] = asyncio.create_task(watchdog_loop(app))
    app["tg_updates_task"] = asyncio.create_task(tg_updates_loop(app))  # <— командный интерфейс

async def on_cleanup(app: web.Application) -> None:
    for key in ("ws_task","keepalive_task","hb_task","watchdog_task","tg_updates_task"):
        t = app.get(key)
        if t:
            t.cancel()
            with contextlib.suppress(Exception):
                await t
    ws: BybitWS = app.get("ws")
    if ws and ws.ws and not ws.ws.closed:
        await ws.ws.close()
    http: aiohttp.ClientSession = app.get("http")
    if http:
        await http.close()

def make_app() -> web.Application:
    app = web.Application()
    app["start_ts"] = time.monotonic()
    app.router.add_get("/", handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

def main() -> None:
    setup_logging(LOG_LEVEL)
    logger.info(
        f"cfg: ws={BYBIT_WS_PUBLIC_LINEAR} | active={ACTIVE_SYMBOLS} | "
        f"tp=[{TP_MIN_PCT:.1%}..{TP_MAX_PCT:.1%}] | prob_min={PROB_MIN:.0%} | rr≥{RR_TARGET:.2f}"
    )
    app = make_app()
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
