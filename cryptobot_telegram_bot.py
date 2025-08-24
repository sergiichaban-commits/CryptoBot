# -*- coding: utf-8 -*-
"""
CryptoBot — Telegram сканер сигналов (Bybit V5, WebSocket, ~5s реакция)

Что нового в этой версии:
- Переход на Bybit WebSocket V5 (public/linear) вместо REST-поллинга — задержка ≈100–500мс
- Подписка на: tickers.{symbol}, kline.1.{symbol}, allLiquidation.{symbol}
- Безопасный и быстрый движок сигналов на событиях (без cron/джобов)
- Надёжный авто‑reconnect и ping каждые 20s (рекомендация Bybit)
- Минимизация Render окружения: большинство параметров заданы в коде с возможностью override через ENV
- REST теперь используется только один раз на старте — чтобы выбрать топ‑символы по обороту

ПРИМЕЧАНИЕ
- В качестве источника символов используется REST /v5/market/tickers?category=linear ровно один раз на старте
  для выбора топ N активов по turnover24h. Дальше — только WebSocket.
- Ликвидации берём из ws‑топика allLiquidation.{symbol} (старый REST /v5/market/liquidation отсутствует/меняется).
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
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web

# =========================
# Константы/дефолты (можно переопределить ENV)
# =========================
DEFAULT_BYBIT_REST = "https://api.bybit.com"
DEFAULT_BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"
DEFAULT_LOG_LEVEL = "INFO"

# Логика сигналов/фильтры (дефолты можно ослабить под себя)
DEFAULT_ACTIVE_SYMBOLS = 30               # сколько топ‑символов подписывать
DEFAULT_PROB_MIN = 0.65                   # минимальная условная "вероятность"
DEFAULT_TP_MIN_PCT = 0.010                # минимум тейк‑профита (1%)
DEFAULT_ATR_PERIOD = 14                   # по 1m kline
DEFAULT_BODY_ATR_MULT = 0.6               # "сила" свечи (реальное тело/ATR)
DEFAULT_VOL_SMA_PERIOD = 20               # скользящее по объёму (клайны)
DEFAULT_VOL_MULT = 2.0                    # во сколько раз объём больше среднего
DEFAULT_SIGNAL_COOLDOWN_SEC = 60          # антиспам по одному направлению
DEFAULT_HEARTBEAT_SEC = 15 * 60           # сообщение в канал "я жив"
DEFAULT_KEEPALIVE_SEC = 13 * 60           # self‑ping хостинга
DEFAULT_FIRST_DELAY_SEC = 3               # быстрый старт
DEFAULT_PORT = 10000

# Роутинг (заданы дефолты, при желании можно переопределить через ENV)
DEFAULT_PRIMARY_RECIPIENTS = [-1002870952333]   # id канала(ов) с сигналами
DEFAULT_ALLOWED_CHAT_IDS = [533232884, -1002870952333]
DEFAULT_ONLY_CHANNEL = True  # если True — слать только в каналы из PRIMARY_RECIPIENTS

# =========================
# Утилиты
# =========================
def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and v.strip() != "") else default

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}: return True
    if v in {"0", "false", "no", "n", "off"}: return False
    return default

def pct(x: float) -> str:
    return f"{x:.2%}"

def now_ts_ms() -> int:
    return int(time.time() * 1000)

# =========================
# Конфиг
# =========================
@dataclass
class Config:
    # Telegram/infra
    token: str
    log_level: str = DEFAULT_LOG_LEVEL
    public_url: Optional[str] = None
    port: int = DEFAULT_PORT

    # Таймеры
    keepalive_sec: int = DEFAULT_KEEPALIVE_SEC
    heartbeat_sec: int = DEFAULT_HEARTBEAT_SEC
    first_delay_sec: int = DEFAULT_FIRST_DELAY_SEC

    # Роутинг
    primary_recipients: List[int] = None
    allowed_chat_ids: List[int] = None
    only_channel: bool = DEFAULT_ONLY_CHANNEL

    # Bybit
    bybit_rest: str = DEFAULT_BYBIT_REST
    bybit_ws_public_linear: str = DEFAULT_BYBIT_WS_PUBLIC_LINEAR

    # Символы/фильтры
    active_symbols: int = DEFAULT_ACTIVE_SYMBOLS
    prob_min: float = DEFAULT_PROB_MIN
    tp_min_pct: float = DEFAULT_TP_MIN_PCT
    atr_period: int = DEFAULT_ATR_PERIOD
    body_atr_mult: float = DEFAULT_BODY_ATR_MULT
    vol_sma_period: int = DEFAULT_VOL_SMA_PERIOD
    vol_mult: float = DEFAULT_VOL_MULT
    signal_cooldown_sec: int = DEFAULT_SIGNAL_COOLDOWN_SEC

    @staticmethod
    def load() -> "Config":
        token = _env_str("TELEGRAM_TOKEN")
        if not token:
            raise RuntimeError("Не задан TELEGRAM_TOKEN")

        def _ids(env_name: str, default_ids: List[int]) -> List[int]:
            raw = _env_str(env_name)
            if not raw:
                return list(default_ids)
            ids: List[int] = []
            for s in raw.replace(";", ",").split(","):
                s = s.strip()
                if not s: continue
                with contextlib.suppress(Exception):
                    ids.append(int(s))
            return ids or list(default_ids)

        return Config(
            token=token,
            log_level=_env_str("LOG_LEVEL", DEFAULT_LOG_LEVEL),
            public_url=_env_str("PUBLIC_URL") or _env_str("RENDER_EXTERNAL_URL"),
            port=_env_int("PORT", DEFAULT_PORT),
            keepalive_sec=_env_int("KEEPALIVE_SEC", DEFAULT_KEEPALIVE_SEC),
            heartbeat_sec=_env_int("HEARTBEAT_SEC", DEFAULT_HEARTBEAT_SEC),
            first_delay_sec=_env_int("FIRST_SCAN_DELAY_SEC", DEFAULT_FIRST_DELAY_SEC),
            primary_recipients=_ids("PRIMARY_RECIPIENTS", DEFAULT_PRIMARY_RECIPIENTS),
            allowed_chat_ids=_ids("ALLOWED_CHAT_IDS", DEFAULT_ALLOWED_CHAT_IDS),
            only_channel=_env_bool("ONLY_CHANNEL", DEFAULT_ONLY_CHANNEL),
            bybit_rest=_env_str("BYBIT_BASE", DEFAULT_BYBIT_REST) or DEFAULT_BYBIT_REST,
            bybit_ws_public_linear=_env_str("BYBIT_WS_PUBLIC_LINEAR", DEFAULT_BYBIT_WS_PUBLIC_LINEAR) or DEFAULT_BYBIT_WS_PUBLIC_LINEAR,
            active_symbols=_env_int("ACTIVE_SYMBOLS", DEFAULT_ACTIVE_SYMBOLS),
            prob_min=_env_float("PROB_MIN", DEFAULT_PROB_MIN),
            tp_min_pct=_env_float("TP_MIN_PCT", DEFAULT_TP_MIN_PCT),
            atr_period=_env_int("ATR_PERIOD", DEFAULT_ATR_PERIOD),
            body_atr_mult=_env_float("BODY_ATR_MULT", DEFAULT_BODY_ATR_MULT),
            vol_sma_period=_env_int("VOL_SMA_PERIOD", DEFAULT_VOL_SMA_PERIOD),
            vol_mult=_env_float("VOL_MULT", DEFAULT_VOL_MULT),
            signal_cooldown_sec=_env_int("SIGNAL_COOLDOWN_SEC", DEFAULT_SIGNAL_COOLDOWN_SEC),
        )

# =========================
# Логгер
# =========================
def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt)

logger = logging.getLogger("cryptobot")

# =========================
# Telegram API
# =========================
class Tg:
    def __init__(self, token: str, session: aiohttp.ClientSession) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = session

    async def send(self, chat_id: int, text: str) -> None:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        async with self.http.post(f"{self.base}/sendMessage", json=payload) as r:
            r.raise_for_status()
            await r.json()

# =========================
# Bybit REST (только на старте)
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
# Bybit WebSocket (public linear)
# =========================
class BybitWS:
    def __init__(self, url: str, session: aiohttp.ClientSession) -> None:
        self.url = url
        self.http = session
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._subs: List[str] = []
        self._task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self.on_message: Optional[callable] = None  # async def on_message(msg: dict)

    async def connect(self) -> None:
        if self.ws and not self.ws.closed:
            return
        logger.info(f"WS connecting: {self.url}")
        self.ws = await self.http.ws_connect(self.url, heartbeat=25)  # aiohttp heartbeat -> ping/pong at transport level
        # Дополнительно — собственный ping по рекомендации Bybit (каждые 20s)
        if self._ping_task is None or self._ping_task.done():
            self._ping_task = asyncio.create_task(self._ping_loop())
        # Подписываемся повторно после реконнекта
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
        # накапливаем список подписок для auto-resubscribe
        for a in args:
            if a not in self._subs:
                self._subs.append(a)
        if not self.ws or self.ws.closed:
            await self.connect()
        if not args:
            return
        payload = {"op": "subscribe", "args": args}
        await self.ws.send_str(json.dumps(payload))
        logger.info(f"WS subscribed: {args}")

    async def run(self) -> None:
        assert self.ws is not None, "call connect() first"
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue
                if "success" in data and data.get("op") in {"subscribe", "ping"}:
                    # ack
                    continue
                if self.on_message:
                    try:
                        await self.on_message(data)
                    except Exception as e:
                        logger.exception(f"on_message error: {e}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
        # try to reconnect
        await asyncio.sleep(2)
        with contextlib.suppress(Exception):
            await self.connect()
        if self.ws and not self.ws.closed:
            await self.run()

# =========================
# Буферы рын.данных для сигналов
# =========================
class MarketState:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.tickers: Dict[str, Dict[str, Any]] = {}    # last ticker per symbol
        self.oi_prev: Dict[str, float] = {}             # предыдущий openInterest
        self.liq_events: Dict[str, List[int]] = {}      # timestamps(ms) последних ликвидаций
        self.kline: Dict[str, List[Tuple[float,float,float,float,float]]] = {}  # [ (o,h,l,c,vol) ] max ~300
        self.kline_maxlen = 300

    def note_ticker(self, d: Dict[str, Any]) -> None:
        sym = d.get("symbol")
        if not sym:
            return
        self.tickers[sym] = d

    def note_liq(self, sym: str, ts_ms: int) -> None:
        arr = self.liq_events.setdefault(sym, [])
        arr.append(ts_ms)
        # чистим > 5 мин
        cutoff = now_ts_ms() - 5 * 60 * 1000
        while arr and arr[0] < cutoff:
            arr.pop(0)

    def note_kline(self, sym: str, points: List[Dict[str, Any]]) -> None:
        buf = self.kline.setdefault(sym, [])
        # добавляем в порядке прихода
        for p in points:
            o = float(p["open"]); h = float(p["high"]); l = float(p["low"]); c = float(p["close"])
            v = float(p.get("volume") or p.get("turnover") or 0.0)
            # если confirm=false — обновляем последнюю свечу; если true — фиксируем новую
            if p.get("confirm") is False and buf:
                buf[-1] = (o,h,l,c,v)
            else:
                buf.append((o,h,l,c,v))
                if len(buf) > self.kline_maxlen:
                    del buf[0:len(buf)-self.kline_maxlen]

    def last_close(self, sym: str) -> Optional[float]:
        arr = self.kline.get(sym)
        return arr[-1][3] if arr else None

    def atr(self, sym: str, period: int) -> Optional[float]:
        arr = self.kline.get(sym)
        if not arr or len(arr) < period + 1:
            return None
        trs: List[float] = []
        prev_c = arr[0][3]
        for o,h,l,c,v in arr[1:period+1]:
            tr = max(h-l, abs(h-prev_c), abs(l-prev_c))
            trs.append(tr)
            prev_c = c
        return sum(trs)/len(trs) if trs else None

    def vol_sma(self, sym: str, period: int) -> Optional[float]:
        arr = self.kline.get(sym)
        if not arr or len(arr) < period:
            return None
        vols = [x[4] for x in arr[-period:]]
        return sum(vols)/len(vols) if vols else None

    def liq_burst(self, sym: str, window_sec: int = 60) -> int:
        arr = self.liq_events.get(sym, [])
        if not arr:
            return 0
        cutoff = now_ts_ms() - window_sec * 1000
        # количество событий за окно
        n = 0
        for t in reversed(arr):
            if t < cutoff:
                break
            n += 1
        return n

# =========================
# Генерация сигналов
# =========================
class SignalEngine:
    def __init__(self, cfg: Config, mkt: MarketState) -> None:
        self.cfg = cfg
        self.mkt = mkt
        self.cooldown: Dict[Tuple[str,str], float] = {}  # (sym, side) -> last_ts

    def _can_emit(self, sym: str, side: str) -> bool:
        key = (sym, side)
        t = time.time()
        last = self.cooldown.get(key, 0.0)
        if t - last < self.cfg.signal_cooldown_sec:
            return False
        self.cooldown[key] = t
        return True

    def _probability(self, sym: str, side: str, atr_val: Optional[float], body_ratio: float, oi_delta: float, liq_cnt: int) -> float:
        # Простая эвристика в диапазоне [0..1]
        score = 0.0
        score += max(0.0, min(1.0, body_ratio)) * 0.4
        score += max(0.0, min(1.0, abs(oi_delta))) * 0.3
        score += max(0.0, min(1.0, liq_cnt / 5.0)) * 0.3  # 5+ ликвидаций за 60с = насыщено
        return max(0.0, min(1.0, score))

    def on_kline_closed(self, sym: str) -> Optional[Dict[str, Any]]:
        # При закрытии 1m свечи оцениваем сигнал
        k = self.mkt.kline.get(sym)
        if not k or len(k) < max(self.cfg.atr_period, self.cfg.vol_sma_period) + 2:
            return None

        close = k[-1][3]; prev_close = k[-2][3]
        chg = (close - prev_close) / prev_close if prev_close else 0.0

        atr_val = self.mkt.atr(sym, self.cfg.atr_period)
        if not atr_val or atr_val <= 0:
            return None

        real_body = abs(close - k[-1][0])  # |close - open|
        body_ratio = min(2.0, real_body / atr_val)  # cap

        vol = k[-1][4]
        vol_sma = self.mkt.vol_sma(sym, self.cfg.vol_sma_period) or 0.0
        big_vol = vol > self.cfg.vol_mult * vol_sma if vol_sma > 0 else False

        tkr = self.mkt.tickers.get(sym, {})
        oi_raw = float(tkr.get("openInterest") or 0.0)
        prev_oi = self.mkt.oi_prev.get(sym, oi_raw)
        oi_delta = 0.0
        if prev_oi > 0:
            oi_delta = (oi_raw - prev_oi) / prev_oi
        self.mkt.oi_prev[sym] = oi_raw

        liq_cnt = self.mkt.liq_burst(sym, window_sec=60)

        # Условия входа:
        # 1) сильная свеча относительно ATR, 2) объём выше среднего, 3) OI меняется, 4) optionally ликвидации
        if body_ratio < self.cfg.body_atr_mult:
            return None
        if not big_vol:
            return None

        side = "LONG" if chg > 0 else "SHORT"
        if not self._can_emit(sym, side):
            return None

        # Цели/стоп (минимум по tp_min_pct)
        entry = close
        tp_pct = max(self.cfg.tp_min_pct, min(0.03, 0.5 * abs(chg) + 0.5 * (atr_val / entry)))  # 1%..3%
        sl_pct = tp_pct * 0.6

        if side == "LONG":
            tp = entry * (1.0 + tp_pct)
            sl = entry * (1.0 - sl_pct)
        else:
            tp = entry * (1.0 - tp_pct)
            sl = entry * (1.0 + sl_pct)

        prob = self._probability(sym, side, atr_val, body_ratio, oi_delta, liq_cnt)
        if prob < self.cfg.prob_min:
            return None

        return {
            "symbol": sym,
            "side": side,
            "entry": entry,
            "tp": tp,
            "sl": sl,
            "prob": prob,
            "atr": atr_val,
            "body_ratio": body_ratio,
            "oi_delta": oi_delta,
            "liq_cnt": liq_cnt,
        }

# =========================
# Форматирование сообщения
# =========================
def format_signal(sig: Dict[str, Any]) -> str:
    sym = sig["symbol"]; side = sig["side"]
    entry = sig["entry"]; tp = sig["tp"]; sl = sig["sl"]
    prob = sig["prob"]; liq_cnt = sig.get("liq_cnt", 0)
    body_ratio = sig.get("body_ratio", 0.0); oi_delta = sig.get("oi_delta", 0.0)

    lines = [
        f"⚡️ <b>Сигнал по {sym}</b>",
        f"Направление: <b>{'LONG' if side=='LONG' else 'SHORT'}</b>",
        f"Текущая цена: <b>{entry:g}</b>",
        f"Тейк: <b>{tp:g}</b> ({pct((tp-entry)/entry if side=='LONG' else (entry-tp)/entry)})",
        f"Стоп: <b>{sl:g}</b>",
        f"Вероятность: <b>{prob:.0%}</b>",
        f"Обоснование: тело/ATR={body_ratio:.2f}; ΔOI={oi_delta:+.2%}; ликвидаций(60с)={liq_cnt}",
        f"⏱️ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
    ]
    return "\n".join(lines)

# =========================
# Приложение
# =========================
async def keepalive_loop(app: web.Application) -> None:
    cfg: Config = app["cfg"]
    http: aiohttp.ClientSession = app["http"]
    if not cfg.public_url:
        return
    while True:
        try:
            await asyncio.sleep(cfg.keepalive_sec)
            with contextlib.suppress(Exception):
                await http.get(cfg.public_url, timeout=aiohttp.ClientTimeout(total=10))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"keepalive error: {e}")

async def heartbeat_loop(app: web.Application) -> None:
    cfg: Config = app["cfg"]
    tg: Tg = app["tg"]
    while True:
        try:
            await asyncio.sleep(cfg.heartbeat_sec)
            text = f"✅ Cryptobot активен • подписки WS • символов: {len(app['symbols'])}"
            for chat_id in cfg.primary_recipients:
                await tg.send(chat_id, text)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"heartbeat error: {e}")

# Основной обработчик ws‑сообщений
async def ws_on_message(app: web.Application, data: Dict[str, Any]) -> None:
    mkt: MarketState = app["mkt"]
    eng: SignalEngine = app["engine"]
    tg: Tg = app["tg"]
    cfg: Config = app["cfg"]

    topic = data.get("topic") or ""
    if topic.startswith("tickers."):
        d = data.get("data") or {}
        if isinstance(d, dict):
            mkt.note_ticker(d)

    elif topic.startswith("kline.1."):
        payload = data.get("data") or []
        # data — список, confirm=false/true
        if payload:
            sym = payload[0].get("symbol") or topic.split(".")[-1]
            mkt.note_kline(sym, payload)
            # если свеча закрыта — оцениваем
            if any(x.get("confirm") for x in payload):
                sig = eng.on_kline_closed(sym)
                if sig:
                    text = format_signal(sig)
                    # роутинг
                    chats = cfg.primary_recipients if cfg.only_channel else cfg.allowed_chat_ids or cfg.primary_recipients
                    for chat_id in chats:
                        with contextlib.suppress(Exception):
                            await tg.send(chat_id, text)
                            logger.info(f"signal sent: {sym} {sig['side']}")
    elif topic.startswith("allLiquidation."):
        # Содержит список событий ликвидаций
        arr = data.get("data") or []
        for liq in arr:
            sym = (liq.get("s") or liq.get("symbol"))
            ts = int(liq.get("T") or data.get("ts") or now_ts_ms())
            if sym:
                mkt.note_liq(sym, ts)

async def on_startup(app: web.Application) -> None:
    cfg: Config = app["cfg"]
    setup_logging(cfg.log_level)
    logger.info("startup...")

    http = aiohttp.ClientSession()
    app["http"] = http
    app["tg"] = Tg(cfg.token, http)
    rest = BybitRest(cfg.bybit_rest, http)

    # Выбираем топ‑символы по обороту (24h)
    tickers = await rest.tickers_linear()
    # отфильтровываем только USDT perpetual (на всякий случай)
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
    symbols = [s for s, _ in cands[: cfg.active_symbols]]
    if not symbols:
        # fallback на набор по умолчанию
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "TONUSDT", "ADAUSDT", "LINKUSDT", "AVAXUSDT"][: cfg.active_symbols]
    app["symbols"] = symbols
    logger.info(f"symbols: {symbols}")

    # Маркет‑состояние и движок
    mkt = MarketState(cfg); app["mkt"] = mkt
    engine = SignalEngine(cfg, mkt); app["engine"] = engine

    # WebSocket
    ws = BybitWS(cfg.bybit_ws_public_linear, http)
    app["ws"] = ws
    async def _on_msg(msg: Dict[str, Any]) -> None:
        await ws_on_message(app, msg)
    ws.on_message = _on_msg
    await ws.connect()

    # Подписки
    args = []
    for s in symbols:
        args.append(f"tickers.{s}")
        args.append(f"kline.1.{s}")
        args.append(f"allLiquidation.{s}")
    await ws.subscribe(args)

    # Запускаем чтение сокета и служебные циклы
    app["ws_task"] = asyncio.create_task(ws.run())
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["hb_task"] = asyncio.create_task(heartbeat_loop(app))

async def on_cleanup(app: web.Application) -> None:
    for key in ("ws_task", "keepalive_task", "hb_task"):
        task = app.get(key)
        if task:
            task.cancel()
            with contextlib.suppress(Exception):
                await task
    ws: BybitWS = app.get("ws")
    if ws and ws.ws and not ws.ws.closed:
        await ws.ws.close()
    http: aiohttp.ClientSession = app.get("http")
    if http:
        await http.close()

# =========================
# HTTP сервер (healthz)
# =========================
async def handle_health(request: web.Request) -> web.Response:
    app = request.app
    t0 = app.get("start_ts") or time.monotonic()
    uptime = time.monotonic() - t0
    return web.json_response({"ok": True, "uptime_sec": int(uptime), "symbols": app.get("symbols", [])})

def make_app(cfg: Config) -> web.Application:
    app = web.Application()
    app["cfg"] = cfg
    app["start_ts"] = time.monotonic()
    app.router.add_get("/", handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

def main() -> None:
    cfg = Config.load()
    setup_logging(cfg.log_level)
    logger.info(
        "cfg: "
        f"public_url={cfg.public_url} | ws={cfg.bybit_ws_public_linear} | "
        f"active={cfg.active_symbols} | tp_min={cfg.tp_min_pct:.2%} | prob_min={cfg.prob_min:.0%}"
    )
    app = make_app(cfg)
    web.run_app(app, host="0.0.0.0", port=cfg.port)

if __name__ == "__main__":
    main()
