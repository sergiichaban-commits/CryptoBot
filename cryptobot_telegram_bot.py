#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import logging
import json
import math
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# -----------------------------
# ЛОГГИРОВАНИЕ
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# -----------------------------
# .env ХЕЛПЕР (по просьбе)
# -----------------------------
def _load_envfile():
    """
    Опционально подхватить переменные из .env (если есть).
    Не затирает уже заданные переменные окружения.
    """
    try:
        path = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k and k not in os.environ:
                        os.environ[k] = v
            log.info("[env] Loaded .env variables")
    except Exception as e:
        log.warning("[env] .env load skipped: %s", e)


# -----------------------------
# КОНФИГ
# -----------------------------
@dataclass
class Config:
    TELEGRAM_TOKEN: str
    PRIMARY_RECIPIENTS: List[int]
    ALLOWED_CHAT_IDS: List[int]

    PUBLIC_URL: str
    PORT: int

    HEALTH_INTERVAL_SEC: int
    SELF_PING_SEC: int
    FIRST_DELAY_SEC: int

    SIGNAL_COOLDOWN_SEC: int
    SIGNAL_TTL_MIN: int

    UNIVERSE_MODE: str  # 'all' (все линейные USDT фьючи с Bybit)
    UNIVERSE_TOP_N: int
    WS_SYMBOLS_MAX: int
    ROTATE_MIN: int

    PROB_MIN: float            # 69.9
    PROFIT_MIN_PCT: float      # 1.0 (%)
    RR_MIN: float              # 2.0

    # Триггеры (простые)
    VOL_MULT: float
    VOL_SMA_PERIOD: int
    BODY_ATR_MULT: float
    ATR_PERIOD: int

    @staticmethod
    def _parse_ids(s: str) -> List[int]:
        out = []
        for part in s.replace(" ", "").split(","):
            if not part:
                continue
            try:
                out.append(int(part))
            except Exception:
                pass
        return out

    @classmethod
    def load(cls) -> "Config":
        # Подхват из .env если нужно
        _load_envfile()

        token = os.environ.get("TELEGRAM_TOKEN", "").strip()
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN is required")

        allowed = os.environ.get("TELEGRAM_ALLOWED_CHAT_IDS", "")
        primary = os.environ.get("TELEGRAM_CHAT_ID", allowed)

        public_url = os.environ.get("PUBLIC_URL", "").strip()
        port = int(os.environ.get("PORT", "10000"))

        health_sec = int(os.environ.get("HEALTH_INTERVAL_SEC", "1200"))  # 20 минут
        self_ping_sec = int(os.environ.get("SELF_PING_SEC", "780"))      # ~13 мин
        first_delay = int(os.environ.get("FIRST_DELAY_SEC", "60"))

        cooldown = int(os.environ.get("SIGNAL_COOLDOWN_SEC", "600"))
        ttl_min = int(os.environ.get("SIGNAL_TTL_MIN", "12"))

        universe_mode = os.environ.get("UNIVERSE_MODE", "all")
        top_n = int(os.environ.get("UNIVERSE_TOP_N", "15"))
        ws_symbols_max = int(os.environ.get("WS_SYMBOLS_MAX", "60"))
        rotate_min = int(os.environ.get("ROTATE_MIN", "5"))

        prob_min = float(os.environ.get("PROB_MIN", "69.9"))
        profit_min_pct = float(os.environ.get("PROFIT_MIN_PCT", "1.0"))
        rr_min = float(os.environ.get("RR_MIN", "2.0"))

        vol_mult = float(os.environ.get("VOL_MULT", "2.0"))
        vol_sma_period = int(os.environ.get("VOL_SMA_PERIOD", "20"))
        body_atr_mult = float(os.environ.get("BODY_ATR_MULT", "0.60"))
        atr_period = int(os.environ.get("ATR_PERIOD", "14"))

        cfg = cls(
            TELEGRAM_TOKEN=token,
            PRIMARY_RECIPIENTS=cls._parse_ids(primary),
            ALLOWED_CHAT_IDS=cls._parse_ids(allowed),

            PUBLIC_URL=public_url,
            PORT=port,

            HEALTH_INTERVAL_SEC=health_sec,
            SELF_PING_SEC=self_ping_sec,
            FIRST_DELAY_SEC=first_delay,

            SIGNAL_COOLDOWN_SEC=cooldown,
            SIGNAL_TTL_MIN=ttl_min,

            UNIVERSE_MODE=universe_mode,
            UNIVERSE_TOP_N=top_n,
            WS_SYMBOLS_MAX=ws_symbols_max,
            ROTATE_MIN=rotate_min,

            PROB_MIN=prob_min,
            PROFIT_MIN_PCT=profit_min_pct,
            RR_MIN=rr_min,

            VOL_MULT=vol_mult,
            VOL_SMA_PERIOD=vol_sma_period,
            BODY_ATR_MULT=body_atr_mult,
            ATR_PERIOD=atr_period,
        )
        # Лог-конфиг
        log.info(
            "INFO [cfg] ALLOWED_CHAT_IDS=%s", cfg.ALLOWED_CHAT_IDS
        )
        log.info(
            "INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.PRIMARY_RECIPIENTS
        )
        log.info(
            "INFO [cfg] PUBLIC_URL='%s' PORT=%s",
            cfg.PUBLIC_URL, cfg.PORT
        )
        log.info(
            "INFO [cfg] HEALTH=%ss FIRST=%ss SELF_PING=True/%ss",
            cfg.HEALTH_INTERVAL_SEC, cfg.FIRST_DELAY_SEC, cfg.SELF_PING_SEC
        )
        log.info(
            "INFO [cfg] SIGNAL_COOLDOWN_SEC=%s SIGNAL_TTL_MIN=%s "
            "UNIVERSE_MODE=%s UNIVERSE_TOP_N=%s WS_SYMBOLS_MAX=%s ROTATE_MIN=%s "
            "PROB_MIN>%.1f PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
            cfg.SIGNAL_COOLDOWN_SEC, cfg.SIGNAL_TTL_MIN,
            cfg.UNIVERSE_MODE, cfg.UNIVERSE_TOP_N, cfg.WS_SYMBOLS_MAX, cfg.ROTATE_MIN,
            cfg.PROB_MIN, cfg.PROFIT_MIN_PCT, cfg.RR_MIN,
        )
        log.info(
            "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, "
            "BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
            cfg.VOL_MULT, cfg.VOL_SMA_PERIOD, cfg.BODY_ATR_MULT, cfg.ATR_PERIOD
        )
        return cfg


# -----------------------------
# BYBIT CLIENT
# -----------------------------
class BybitClient:
    BASE = "https://api.bybit.com"

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session

    async def get_linear_symbols(self) -> List[str]:
        """
        Все линейные фьючерсы в USDT (category=linear).
        """
        out = []
        url = f"{self.BASE}/v5/market/instruments-info"
        params = {"category": "linear", "limit": 1000}
        cursor = None
        while True:
            p = dict(params)
            if cursor:
                p["cursor"] = cursor
            async with self._session.get(url, params=p, timeout=15) as r:
                data = await r.json()
            if data.get("retCode") != 0:
                log.warning("[bybit] instruments retCode=%s retMsg=%s",
                            data.get("retCode"), data.get("retMsg"))
                break
            result = data.get("result", {}) or {}
            rows = result.get("list", []) or []
            for it in rows:
                # Фильтр по USDT
                sym = it.get("symbol")
                quote = it.get("quoteCoin") or it.get("quoteCurrency")
                status = it.get("status", "Trading")
                if sym and (quote == "USDT") and status == "Trading":
                    out.append(sym)
            cursor = result.get("nextPageCursor")
            if not cursor:
                break
        return sorted(list(set(out)))

    async def get_kline(self, symbol: str, interval: str = "5", limit: int = 300) -> List[Dict]:
        """
        Kline: category=linear
        """
        url = f"{self.BASE}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        async with self._session.get(url, params=params, timeout=15) as r:
            data = await r.json()
        if data.get("retCode") != 0:
            return []
        # list: [[start, open, high, low, close, volume, turnover], ...]
        rows = data.get("result", {}).get("list", []) or []
        # Преобразуем в dict
        out = []
        for row in rows:
            # Bybit: все строки как строки
            ts = int(row[0])
            open_, high, low, close = map(float, row[1:5])
            volume = float(row[5])
            out.append(
                dict(ts=ts, o=open_, h=high, l=low, c=close, v=volume)
            )
        # Возвращает старые->новые; нам удобно именно так
        out.sort(key=lambda x: x["ts"])
        return out

    async def get_open_interest(self, symbol: str, interval: str = "5min", limit: int = 6) -> List[Tuple[int, float]]:
        """
        OI (open interest) history.
        """
        url = f"{self.BASE}/v5/market/open-interest"
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval, "limit": limit}
        async with self._session.get(url, params=params, timeout=15) as r:
            data = await r.json()
        if data.get("retCode") != 0:
            # логгируем, но не валим
            log.debug("[bybit] OI bad ret: %s %s", data.get("retCode"), data.get("retMsg"))
            return []
        rows = data.get("result", {}).get("list", []) or []
        out = []
        for row in rows:
            # ["1719739800000","1618.676"]
            try:
                ts = int(row[0])
                val = float(row[1])
                out.append((ts, val))
            except Exception:
                pass
        out.sort(key=lambda x: x[0])
        return out


# -----------------------------
# ВСЕЛЕННАЯ С И Р О Т А Ц И Е Й
# -----------------------------
class Universe:
    def __init__(self, symbols_all: List[str], batch_size: int):
        self.symbols_all = symbols_all[:]  # полный список
        self.batch_size = max(1, batch_size)
        self.batch_idx = 0

    def active(self) -> List[str]:
        if not self.symbols_all:
            return []
        start = (self.batch_idx * self.batch_size) % len(self.symbols_all)
        end = start + self.batch_size
        if end <= len(self.symbols_all):
            return self.symbols_all[start:end]
        return self.symbols_all[start:] + self.symbols_all[:end - len(self.symbols_all)]

    def rotate(self):
        if not self.symbols_all:
            return
        self.batch_idx = (self.batch_idx + 1) % math.ceil(len(self.symbols_all) / self.batch_size)


# -----------------------------
# СИГНАЛЫ (минимальный анализ)
# -----------------------------
@dataclass
class Signal:
    symbol: str
    dir: str             # "LONG" | "SHORT"
    price: float
    entry: float
    take: float
    stop: float
    take_pct: float
    stop_pct: float
    rr: float
    prob: float          # 0..100
    ttl_min: int


def _sma(vals: List[float], period: int) -> float:
    if len(vals) < period or period <= 0:
        return float("nan")
    return sum(vals[-period:]) / period


def _atr(candles: List[Dict], period: int) -> float:
    # очень упрощённо
    if len(candles) < period + 1:
        return float("nan")
    trs = []
    prev_close = candles[-period - 1]["c"]
    for c in candles[-period:]:
        tr = max(c["h"] - c["l"], abs(c["h"] - prev_close), abs(c["l"] - prev_close))
        trs.append(tr)
        prev_close = c["c"]
    return sum(trs) / period if trs else float("nan")


def _volume_sma(candles: List[Dict], period: int) -> float:
    if len(candles) < period:
        return float("nan")
    return sum(c["v"] for c in candles[-period:]) / period


def _analyze_symbol(
    cfg: Config,
    symbol: str,
    candles: List[Dict],
    oi: List[Tuple[int, float]],
) -> Optional[Signal]:
    """
    Очень упрощенная логика комбинированного анализа:
    - всплеск объема: vol_last > VOL_MULT * vol_sma
    - тело свечи > BODY_ATR_MULT * ATR
    - подтверждение направлением изменения OI (рост для LONG, падение для SHORT)
    """
    if len(candles) < max(cfg.VOL_SMA_PERIOD, cfg.ATR_PERIOD) + 2:
        return None

    last = candles[-1]
    prev = candles[-2]

    close = last["c"]
    body = abs(last["c"] - last["o"])
    atr = _atr(candles, cfg.ATR_PERIOD)
    if not math.isfinite(atr) or atr <= 0:
        return None

    vol_sma = _volume_sma(candles, cfg.VOL_SMA_PERIOD)
    if not math.isfinite(vol_sma) or vol_sma <= 0:
        return None

    vol_last = last["v"]
    body_ok = body > cfg.BODY_ATR_MULT * atr
    vol_ok = vol_last > (cfg.VOL_MULT * vol_sma)

    if not (body_ok and vol_ok):
        return None

    direction = "LONG" if last["c"] > last["o"] else "SHORT"

    # OI: простое подтверждение направлением (последний - предпоследний)
    oi_ok = True
    if len(oi) >= 2:
        delta_oi = oi[-1][1] - oi[-2][1]
        if direction == "LONG" and delta_oi < 0:
            oi_ok = False
        if direction == "SHORT" and delta_oi > 0:
            oi_ok = False

    if not oi_ok:
        return None

    # Элементарные уровни: entry=close, take=close ± 1.5*ATR, stop=close ∓ 0.75*ATR
    if direction == "LONG":
        entry = close
        take = close + 1.5 * atr
        stop = close - 0.75 * atr
    else:
        entry = close
        take = close - 1.5 * atr
        stop = close + 0.75 * atr

    # RR
    risk = abs(entry - stop)
    reward = abs(take - entry)
    rr = (reward / risk) if risk > 0 else 0.0

    # Процент прибыли до тейка
    take_pct = (abs(take - entry) / entry) * 100.0 if entry > 0 else 0.0
    stop_pct = (abs(entry - stop) / entry) * 100.0 if entry > 0 else 0.0

    # Простейшая оценка вероятности: масштабируем по силе тела и всплеску объема + OI-фактор
    vol_ratio = min(3.0, vol_last / max(1e-9, vol_sma))
    body_ratio = min(3.0, (body / max(1e-9, atr)) / cfg.BODY_ATR_MULT)
    prob = 50.0 + 10.0 * (vol_ratio - 1.0) + 10.0 * (body_ratio - 1.0)
    if len(oi) >= 2:
        prob += 5.0  # небольшой бонус за подтверждение OI
    prob = max(0.0, min(99.9, prob))

    # Фильтры
    if prob < cfg.PROB_MIN:
        return None
    if rr < cfg.RR_MIN:
        return None
    if take_pct < cfg.PROFIT_MIN_PCT:
        return None

    return Signal(
        symbol=symbol,
        dir=direction,
        price=close,
        entry=entry,
        take=take,
        stop=stop,
        take_pct=take_pct,
        stop_pct=stop_pct,
        rr=rr,
        prob=prob,
        ttl_min=cfg.SIGNAL_TTL_MIN,
    )


# -----------------------------
# ЭНДЖИН
# -----------------------------
class Engine:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self.cfg = cfg
        self.client = BybitClient(session)
        self.universe: Optional[Universe] = None
        self.signals_cache: Dict[str, Signal] = {}  # symbol -> signal
        self.last_signal_ts: Dict[str, float] = {}  # cooldown

    async def bootstrap(self):
        # Загружаем вселенную
        if self.cfg.UNIVERSE_MODE == "all":
            syms = await self.client.get_linear_symbols()
        else:
            syms = await self.client.get_linear_symbols()  # на будущее можно иное
        total = len(syms)
        active_count = min(self.cfg.WS_SYMBOLS_MAX // 2, max(1, self.cfg.UNIVERSE_TOP_N * 2))
        active_count = min(active_count, max(1, total))
        self.universe = Universe(syms, batch_size=active_count)
        log.info("INFO [universe] total=%d active=%d mode=%s",
                 total, active_count, self.cfg.UNIVERSE_MODE)

    def universe_stats(self) -> Tuple[int, int, int]:
        if not self.universe:
            return (0, 0, 0)
        total = len(self.universe.symbols_all)
        active = len(self.universe.active())
        ws_topics = active * 2  # условно (kline+ticker), для вывода
        return (total, active, ws_topics)

    async def rotate(self):
        if not self.universe:
            return
        self.universe.rotate()
        total, active, ws_topics = self.universe_stats()
        log.info("INFO [rotate] total=%d active=%d ws_topics~%d", total, active, ws_topics)

    async def scan_active_and_signal(self, app: Application):
        """
        Простая склейка анализа по активным символам.
        """
        if not self.universe:
            return
        now = asyncio.get_event_loop().time()
        out_signals: List[Signal] = []
        syms = self.universe.active()

        # ограничим параллелизм чтобы не ловить лимиты
        sem = asyncio.Semaphore(10)

        async def process(sym: str):
            async with sem:
                try:
                    kl = await self.client.get_kline(sym, interval="5", limit=300)
                    if not kl:
                        return
                    oi = await self.client.get_open_interest(sym, interval="5min", limit=6)
                    sig = _analyze_symbol(self.cfg, sym, kl, oi)
                    if not sig:
                        return
                    # cooldown
                    last_ts = self.last_signal_ts.get(sym, 0.0)
                    if now - last_ts < self.cfg.SIGNAL_COOLDOWN_SEC:
                        return
                    out_signals.append(sig)
                except Exception as e:
                    log.debug("[scan] %s err: %s", sym, e)

        await asyncio.gather(*(process(s) for s in syms))

        # Сортировка по вероятности (убывание)
        out_signals.sort(key=lambda s: (-s.prob, -s.rr, -s.take_pct))

        # Отправляем
        for s in out_signals:
            self.last_signal_ts[s.symbol] = now
            await self._send_signal(app, s)

    async def _send_signal(self, app: Application, s: Signal):
        text = (
            f"🔔 Сигнал\n"
            f"{s.symbol}\n"
            f"Цена: {s.price:.6g}\n"
            f"Направление: {('ЛОНГ' if s.dir=='LONG' else 'ШОРТ')}\n"
            f"Вход: {s.entry:.6g}\n"
            f"Тейк: {s.take:.6g} (+{s.take_pct:.2f}%)\n"
            f"Стоп: {s.stop:.6g} (-{s.stop_pct:.2f}%)\n"
            f"R/R: {s.rr:.2f}\n"
            f"Вероятность: {s.prob:.1f}%\n"
            f"TTL: {s.ttl_min} мин"
        )
        for chat_id in self.cfg.PRIMARY_RECIPIENTS:
            try:
                await app.bot.send_message(chat_id=chat_id, text=text)
            except Exception as e:
                log.warning("[tg] send_signal failed: %s", e)


# -----------------------------
# МАЛЕНЬКИЙ HTTP-СЕРВЕР (для Render port scan)
# -----------------------------
async def start_health_server(app: Application, port: int):
    async def root(_):
        return web.Response(text="ok")

    runner = web.AppRunner(web.Application())
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    # Добавим маршрут после setup
    # Создаём новый app, иначе проще:
    health_app = web.Application()
    health_app.router.add_get("/", root)
    await runner.cleanup()
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    app.bot_data["http_runner"] = runner
    app.bot_data["http_site"] = site
    log.info("HTTP server started on 0.0.0.0:%d", port)


# -----------------------------
# ТЕЛЕГРАМ ХЕНДЛЕРЫ
# -----------------------------
def _is_allowed(cfg: Config, chat_id: int) -> bool:
    return (not cfg.ALLOWED_CHAT_IDS) or (chat_id in cfg.ALLOWED_CHAT_IDS)

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    if update.effective_chat and not _is_allowed(cfg, update.effective_chat.id):
        return
    await update.effective_message.reply_text("pong")

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    if update.effective_chat and not _is_allowed(cfg, update.effective_chat.id):
        return
    engine: Engine = context.application.bot_data.get("engine")
    if not engine or not engine.universe:
        await update.effective_message.reply_text("Вселенная пока не загружена (жду Bybit API/повторяю попытки)…")
        return
    total, active, ws_topics = engine.universe_stats()
    act_list = engine.universe.active()[:20]
    suffix = f"\nАктивные (пример): {', '.join(act_list)} ..." if act_list else ""
    await update.effective_message.reply_text(
        f"Вселенная: total={total}, active={active}, batch#{engine.universe.batch_idx}, ws_topics={ws_topics}{suffix}"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    if update.effective_chat and not _is_allowed(cfg, update.effective_chat.id):
        return
    engine: Engine = context.application.bot_data.get("engine")
    if not engine or not engine.universe:
        await update.effective_message.reply_text("Вселенная пока не загружена (жду Bybit API/повторяю попытки)…")
        return
    total, active, ws_topics = engine.universe_stats()
    await update.effective_message.reply_text(
        f"Вселенная: total={total}, active={active}, batch#{engine.universe.batch_idx}, ws_topics={ws_topics}"
    )

async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg: Config = context.application.bot_data["cfg"]
    if update.effective_chat and not _is_allowed(cfg, update.effective_chat.id):
        return
    await update.effective_message.reply_text("Команда не распознана. Доступно: /ping, /universe, /status")


# -----------------------------
# ЗАПУСК
# -----------------------------
async def post_init(app: Application):
    """
    Вызывается автоматически в run_polling (внутри PTB) перед стартом.
    Здесь:
    - поднимаем HTTP-сервер на PORT (для Render),
    - создаём aiohttp.Session и Engine,
    - грузим вселенную,
    - настраиваем периодические задачи (APScheduler PTB).
    """
    cfg: Config = app.bot_data["cfg"]

    # HTTP server (порт для Render)
    await start_health_server(app, cfg.PORT)

    # AIOHTTP Session
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    app.bot_data["aiohttp_session"] = session

    # Engine
    engine = Engine(cfg, session)
    app.bot_data["engine"] = engine

    # Bootstrap с повторами
    async def bootstrap_loop():
        for i in range(10):
            try:
                await engine.bootstrap()
                total, active, ws_topics = engine.universe_stats()
                for chat_id in cfg.PRIMARY_RECIPIENTS:
                    try:
                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=f"Стартую… Вселенная: total={total}, active={active}, batch#0, ws_topics={ws_topics}",
                        )
                    except Exception:
                        pass
                return
            except Exception as e:
                log.warning("[bootstrap] attempt %d failed: %s", i + 1, e)
                await asyncio.sleep(3)
        log.error("Failed to bootstrap after retries")

    await bootstrap_loop()

    # Задачи
    jq = app.job_queue

    # 1) Ротация активов
    jq.run_repeating(
        lambda *_: app.create_task(engine.rotate()),
        interval=cfg.ROTATE_MIN * 60,
        first=cfg.FIRST_DELAY_SEC
    )

    # 2) Скан и сигналы
    jq.run_repeating(
        lambda *_: app.create_task(engine.scan_active_and_signal(app)),
        interval=60,   # каждую минуту
        first=cfg.FIRST_DELAY_SEC + 5
    )

    # 3) health сообщения раз в 20 минут
    async def health_msg():
        for chat_id in cfg.PRIMARY_RECIPIENTS:
            try:
                await app.bot.send_message(chat_id=chat_id, text="online")
            except Exception:
                pass

    jq.run_repeating(
        lambda *_: app.create_task(health_msg()),
        interval=cfg.HEALTH_INTERVAL_SEC,
        first=cfg.FIRST_DELAY_SEC + 10
    )

    # 4) self-ping, если есть PUBLIC_URL
    if cfg.PUBLIC_URL:
        async def self_ping():
            try:
                async with session.get(cfg.PUBLIC_URL, timeout=10) as r:
                    _ = await r.text()
            except Exception:
                pass

        jq.run_repeating(
            lambda *_: app.create_task(self_ping()),
            interval=cfg.SELF_PING_SEC,
            first=cfg.FIRST_DELAY_SEC + 15
        )


async def post_shutdown(app: Application):
    # Аккуратно закрыть HTTP и aiohttp
    runner: web.AppRunner = app.bot_data.get("http_runner")
    session: aiohttp.ClientSession = app.bot_data.get("aiohttp_session")
    try:
        if runner:
            await runner.cleanup()
    except Exception:
        pass
    try:
        if session and not session.closed:
            await session.close()
    except Exception:
        pass


def build_application(cfg: Config) -> Application:
    application = (
        Application.builder()
        .token(cfg.TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    application.bot_data["cfg"] = cfg

    # Команды
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("universe", cmd_universe))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(MessageHandler(filters.COMMAND, fallback))
    return application


async def main_async():
    cfg = Config.load()
    app = build_application(cfg)

    # RUN POLLING (никаких вебхуков — устойчиво для Render)
    # allowed_updates берем все типы — удобно для будущего
    await app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        stop_signals=(),     # на Render сигналы от ОС могут отсутствовать — не блокируемся на них
        close_loop=False,    # петля принадлежит PTB — не закрываем насильно
    )


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
