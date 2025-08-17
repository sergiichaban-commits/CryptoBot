#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import math
import time
import json
import asyncio
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

import aiohttp

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# =========================
# ЛОГИ
# =========================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("cryptobot")

# =========================
# КОНФИГ
# =========================

@dataclass
class Config:
    TELEGRAM_TOKEN: str
    ALLOWED_CHAT_IDS: List[int]
    PRIMARY_RECIPIENTS: List[int]

    # поведение/тайминги
    HEALTH_SEC: int = 1200
    FIRST_SCAN_DELAY: int = 60
    SIGNAL_COOLDOWN_SEC: int = 600
    SIGNAL_TTL_MIN: int = 12

    # вселенная
    UNIVERSE_MODE: str = "all"    # "all" или "top"
    UNIVERSE_TOP_N: int = 15
    WS_SYMBOLS_MAX: int = 60
    ROTATE_MIN: int = 5

    # триггера и фильтры
    PROB_MIN: float = 69.9
    PROFIT_MIN_PCT: float = 1.0
    RR_MIN: float = 2.0
    VOL_MULT: float = 2.0
    VOL_SMA_PERIOD: int = 20
    BODY_ATR_MULT: float = 0.60
    ATR_PERIOD: int = 14

    @staticmethod
    def _parse_int_list(val: str) -> List[int]:
        if not val:
            return []
        parts = [p.strip() for p in val.replace(";", ",").split(",") if p.strip()]
        out: List[int] = []
        for p in parts:
            try:
                out.append(int(p))
            except Exception:
                pass
        return out

    @classmethod
    def load(cls) -> "Config":
        token = os.getenv("TELEGRAM_TOKEN", "").strip()
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN is required")

        allowed = cls._parse_int_list(os.getenv("ALLOWED_CHAT_IDS", ""))
        recipients = cls._parse_int_list(os.getenv("PRIMARY_RECIPIENTS", ""))

        cfg = cls(
            TELEGRAM_TOKEN=token,
            ALLOWED_CHAT_IDS=allowed or recipients,
            PRIMARY_RECIPIENTS=recipients or allowed,
            HEALTH_SEC=int(os.getenv("HEALTH_SEC", "1200")),
            FIRST_SCAN_DELAY=int(os.getenv("FIRST", os.getenv("FIRST_SCAN_DELAY", "60"))),
            SIGNAL_COOLDOWN_SEC=int(os.getenv("SIGNAL_COOLDOWN_SEC", "600")),
            SIGNAL_TTL_MIN=int(os.getenv("SIGNAL_TTL_MIN", "12")),
            UNIVERSE_MODE=os.getenv("UNIVERSE_MODE", "all"),
            UNIVERSE_TOP_N=int(os.getenv("UNIVERSE_TOP_N", "15")),
            WS_SYMBOLS_MAX=int(os.getenv("WS_SYMBOLS_MAX", "60")),
            ROTATE_MIN=int(os.getenv("ROTATE_MIN", "5")),
            PROB_MIN=float(os.getenv("PROB_MIN", "69.9")),
            PROFIT_MIN_PCT=float(os.getenv("PROFIT_MIN_PCT", "1.0")),
            RR_MIN=float(os.getenv("RR_MIN", "2.0")),
            VOL_MULT=float(os.getenv("VOL_MULT", "2.0")),
            VOL_SMA_PERIOD=int(os.getenv("VOL_SMA_PERIOD", "20")),
            BODY_ATR_MULT=float(os.getenv("BODY_ATR_MULT", "0.60")),
            ATR_PERIOD=int(os.getenv("ATR_PERIOD", "14")),
        )

        log.info(
            "INFO [cfg] ALLOWED_CHAT_IDS=%s",
            cfg.ALLOWED_CHAT_IDS,
        )
        log.info("INFO [cfg] PRIMARY_RECIPIENTS=%s", cfg.PRIMARY_RECIPIENTS)
        log.info(
            "INFO [cfg] HEALTH=%ss FIRST=%ss",
            cfg.HEALTH_SEC,
            cfg.FIRST_SCAN_DELAY,
        )
        log.info(
            "INFO [cfg] SIGNAL_COOLDOWN_SEC=%s SIGNAL_TTL_MIN=%s "
            "UNIVERSE_MODE=%s UNIVERSE_TOP_N=%s WS_SYMBOLS_MAX=%s ROTATE_MIN=%s "
            "PROB_MIN>%.1f PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
            cfg.SIGNAL_COOLDOWN_SEC,
            cfg.SIGNAL_TTL_MIN,
            cfg.UNIVERSE_MODE,
            cfg.UNIVERSE_TOP_N,
            cfg.WS_SYMBOLS_MAX,
            cfg.ROTATE_MIN,
            cfg.PROB_MIN,
            cfg.PROFIT_MIN_PCT,
            cfg.RR_MIN,
        )
        log.info(
            "INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, "
            "BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
            cfg.VOL_MULT,
            cfg.VOL_SMA_PERIOD,
            cfg.BODY_ATR_MULT,
            cfg.ATR_PERIOD,
        )
        return cfg

# =========================
# BYBIT CLIENT
# =========================

class BybitClient:
    BASE = "https://api.bybit.com"

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_symbols_linear_usdt(self) -> List[str]:
        """Все линейные USDT perpetual (Trading)."""
        url = f"{self.BASE}/v5/market/instruments-info"
        params = {"category": "linear", "status": "Trading"}
        session = await self._get_session()
        out: List[str] = []
        cursor: Optional[str] = None
        for _ in range(6):  # ограничим пагинацию
            p = dict(params)
            if cursor:
                p["cursor"] = cursor
            async with session.get(url, params=p) as resp:
                data = await resp.json()
            if data.get("retCode") != 0:
                log.warning("Bybit instruments error: %s", data)
                break
            result = data.get("result", {}) or {}
            rows = result.get("list", []) or []
            for r in rows:
                # формат: dict с полями "symbol", "quoteCoin", "contractType", ...
                sym = r.get("symbol")
                if not sym:
                    continue
                # фильтр USDT линейных perpetual
                if r.get("quoteCoin") != "USDT":
                    continue
                if str(r.get("contractType", "")).lower().startswith("linear"):
                    out.append(sym)
            cursor = result.get("nextPageCursor") or None
            if not cursor:
                break
        # уникализируем и отсортируем
        out = sorted(set(out))
        return out

    async def get_klines(self, symbol: str, interval: str = "5", limit: int = 200) -> List[List[str]]:
        """v5/market/kline (linear). Возвращает список массивов строк."""
        url = f"{self.BASE}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)}
        session = await self._get_session()
        async with session.get(url, params=params) as resp:
            data = await resp.json()
        if data.get("retCode") != 0:
            log.debug("kline error %s: %s", symbol, data)
            return []
        lst = (data.get("result") or {}).get("list") or []
        # Bybit возвращает новые свечи в порядке возрастания времени (или наоборот) — нормализуем:
        lst = sorted(lst, key=lambda x: int(x[0]))
        return lst

    async def get_open_interest(self, symbol: str, interval_time: str = "5min", limit: int = 6) -> List[Tuple[int, float]]:
        """v5/market/open-interest (linear). Вернёт [(ts, oi_float), ...]"""
        url = f"{self.BASE}/v5/market/open-interest"
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval_time, "limit": str(limit)}
        session = await self._get_session()
        async with session.get(url, params=params) as resp:
            data = await resp.json()
        if data.get("retCode") != 0:
            log.debug("OI error %s: %s", symbol, data)
            return []
        lst = (data.get("result") or {}).get("list") or []
        out: List[Tuple[int, float]] = []
        # формат может быть списком массивов, возьмём [ts, openInterest, ...]
        for row in lst:
            try:
                ts = int(row[0])
                oi = float(row[1])
                out.append((ts, oi))
            except Exception:
                try:
                    ts = int(row.get("timestamp"))
                    oi = float(row.get("openInterest"))
                    out.append((ts, oi))
                except Exception:
                    continue
        out = sorted(out, key=lambda x: x[0])
        return out

# =========================
# СОСТОЯНИЕ/ЮЗЛОГИКА
# =========================

@dataclass
class State:
    client: BybitClient
    universe_all: List[str] = field(default_factory=list)
    active_symbols: List[str] = field(default_factory=list)
    batch_idx: int = 0
    last_signal_ts: Dict[str, float] = field(default_factory=dict)
    ws_topics: int = 0  # просто счётчик для отображения

    # кеш OI для комбинированного анализа (обновляется job'ом)
    oi_cache: Dict[str, List[Tuple[int, float]]] = field(default_factory=dict)

    def can_send_signal(self, symbol: str, cooldown_sec: int) -> bool:
        now = time.time()
        last = self.last_signal_ts.get(symbol, 0)
        return (now - last) >= max(0, cooldown_sec)

    def mark_signal(self, symbol: str):
        self.last_signal_ts[symbol] = time.time()

# =========================
# ВСПОМОГАТЕЛЬНЫЕ РАССЧЁТЫ
# =========================

def _atr_from_klines(klines: List[List[str]], period: int) -> float:
    """Простой ATR по последним N свечам."""
    if len(klines) < period + 1:
        return 0.0
    trs: List[float] = []
    prev_close = float(klines[-period-1][4])
    for k in klines[-period:]:
        high = float(k[2]); low = float(k[3]); close_prev = prev_close
        tr = max(high - low, abs(high - close_prev), abs(low - close_prev))
        trs.append(tr)
        prev_close = float(k[4])
    if not trs:
        return 0.0
    return sum(trs) / len(trs)

def _sma(vals: List[float], period: int) -> float:
    if len(vals) < period or period <= 0:
        return 0.0
    return sum(vals[-period:]) / period

def _std(vals: List[float], period: int) -> float:
    if len(vals) < period:
        return 0.0
    m = _sma(vals, period)
    ss = sum((v - m) ** 2 for v in vals[-period:])
    return math.sqrt(ss / period)

def _estimate_probability(z: float) -> float:
    """Грубая оценка вероятности по |z| (0.. ~95)."""
    p = 50.0 + min(45.0, abs(z) * 12.0)
    return max(0.0, min(99.9, p))

# =========================
# СИГНАЛЫ
# =========================

@dataclass
class Signal:
    side: str         # "LONG" / "SHORT"
    entry: float
    take: float
    stop: float
    rr: float
    prob: float
    profit_pct: float

def make_signal_from_data(
    symbol: str,
    klines: List[List[str]],
    oi_points: List[Tuple[int, float]],
    cfg: Config,
) -> Optional[Signal]:
    """Простая комбинированная логика:
       — breakout относительно SMA и ATR
       — тренд OI (последний минус предпоследний)
    """
    if len(klines) < max(cfg.ATR_PERIOD + 1, cfg.VOL_SMA_PERIOD + 1):
        return None
    closes = [float(k[4]) for k in klines]
    opens  = [float(k[1]) for k in klines]
    last_close = closes[-1]
    last_open  = opens[-1]

    atr = _atr_from_klines(klines, cfg.ATR_PERIOD)
    if atr <= 0:
        return None

    sma = _sma(closes, cfg.VOL_SMA_PERIOD)
    std = _std(closes, cfg.VOL_SMA_PERIOD)
    body = abs(last_close - last_open)

    # Тренд OI
    oi_delta = 0.0
    if len(oi_points) >= 2:
        oi_delta = (oi_points[-1][1] - oi_points[-2][1])

    # Условия для LONG/SHORT:
    long_cond = (last_close > sma + 0.25 * atr) and (oi_delta > 0)
    short_cond = (last_close < sma - 0.25 * atr) and (oi_delta > 0)

    if not (long_cond or short_cond):
        return None

    # Z-score относительно SMA/STD
    z = 0.0
    if std > 0:
        z = (last_close - sma) / std

    # Оценка вероятности
    prob = _estimate_probability(z)

    # Стоп по доле ATR; тейк так, чтобы RR >= RR_MIN
    stop_dist = max(atr * 0.4, body * 0.8)
    if stop_dist <= 0:
        return None

    if long_cond:
        entry = last_close
        stop  = entry - stop_dist
        take  = entry + cfg.RR_MIN * stop_dist
        side  = "LONG"
    else:
        entry = last_close
        stop  = entry + stop_dist
        take  = entry - cfg.RR_MIN * stop_dist
        side  = "SHORT"

    rr = (abs(take - entry) / abs(entry - stop)) if abs(entry - stop) > 1e-12 else 0.0
    profit_pct = abs(take - entry) / entry * 100.0

    # Фильтры
    if rr < cfg.RR_MIN:
        return None
    if profit_pct < cfg.PROFIT_MIN_PCT:
        return None
    if prob < cfg.PROB_MIN:
        return None

    return Signal(side=side, entry=entry, take=take, stop=stop, rr=rr, prob=prob, profit_pct=profit_pct)

def format_signal_text(symbol: str, sig: Signal, current: float) -> str:
    sign = "ЛОНГ" if sig.side == "LONG" else "ШОРТ"
    ppct = f"{sig.profit_pct:.2f}%"
    rr   = f"{sig.rr:.2f}"
    prob = f"{sig.prob:.1f}%"

    # красивый формат без научной нотации
    def fnum(x: float) -> str:
        if x == 0:
            return "0"
        # подбираем точность
        digits = 6
        if abs(x) < 0.001:
            digits = 8
        return f"{x:.{digits}f}".rstrip("0").rstrip(".")

    return (
        f"#{symbol} — {sign}\n"
        f"Текущая: {fnum(current)}\n"
        f"Вход: {fnum(sig.entry)}\n"
        f"Тейк: {fnum(sig.take)} (+{ppct})\n"
        f"Стоп: {fnum(sig.stop)} ({'-' if sig.side=='LONG' else ''}"
        f"{abs((sig.stop - sig.entry)/sig.entry)*100:.2f}%)\n"
        f"R/R: {rr} | Вероятность: {prob}"
    )

# =========================
# КОМАНДЫ
# =========================

def _allowed(update: Update, cfg: Config) -> bool:
    try:
        chat_id = update.effective_chat.id
        return (not cfg.ALLOWED_CHAT_IDS) or (chat_id in cfg.ALLOWED_CHAT_IDS)
    except Exception:
        return True

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    if not _allowed(update, cfg):
        return
    await update.effective_message.reply_text("pong")

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    st: State = context.application.bot_data["st"]
    if not _allowed(update, cfg):
        return
    total = len(st.universe_all)
    active = len(st.active_symbols)
    topics = st.ws_topics or (active * 2)
    msg = f"Вселенная: total={total}, active={active}, batch#{st.batch_idx}, ws_topics={topics}"
    if active:
        sample = ", ".join(st.active_symbols[:15])
        msg += f"\nАктивные (пример): {sample} ..."
    await update.effective_message.reply_text(msg)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    st: State = context.application.bot_data["st"]
    if not _allowed(update, cfg):
        return
    total = len(st.universe_all)
    active = len(st.active_symbols)
    topics = st.ws_topics or (active * 2)
    await update.effective_message.reply_text(
        f"Вселенная: total={total}, active={active}, batch#{st.batch_idx}, ws_topics={topics}"
    )

# =========================
# JOBS
# =========================

async def job_boot_online(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    cfg: Config = app.bot_data["cfg"]
    st: State = app.bot_data["st"]
    try:
        boot_msg = (
            f"online · boot · total={len(st.universe_all)} "
            f"active={len(st.active_symbols)} batch#{st.batch_idx}"
        )
        for cid in cfg.PRIMARY_RECIPIENTS:
            await app.bot.send_message(chat_id=cid, text=boot_msg)
    except Exception as e:
        log.warning("boot online send failed: %s", e)

async def job_health(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    cfg: Config = app.bot_data["cfg"]
    st: State = app.bot_data["st"]
    try:
        msg = (
            f"online · health · total={len(st.universe_all)} "
            f"active={len(st.active_symbols)} batch#{st.batch_idx}"
        )
        for cid in cfg.PRIMARY_RECIPIENTS:
            await app.bot.send_message(chat_id=cid, text=msg)
    except Exception as e:
        log.warning("health send failed: %s", e)

async def job_rotate(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    cfg: Config = app.bot_data["cfg"]
    st: State = app.bot_data["st"]

    # при первом запуске загрузим вселенную
    if not st.universe_all:
        try:
            all_syms = await st.client.get_symbols_linear_usdt()
            if cfg.UNIVERSE_MODE == "top" and cfg.UNIVERSE_TOP_N > 0:
                all_syms = all_syms[: cfg.UNIVERSE_TOP_N]
            st.universe_all = all_syms
            log.info("INFO [universe] total=%d", len(all_syms))
        except Exception as e:
            log.warning("universe fetch failed: %s", e)
            return

    total = len(st.universe_all)
    if total == 0:
        st.active_symbols = []
        st.batch_idx = 0
        st.ws_topics = 0
        return

    n = max(1, cfg.WS_SYMBOLS_MAX)
    start = (st.batch_idx * n) % total
    end = start + n
    if end <= total:
        batch = st.universe_all[start:end]
    else:
        batch = st.universe_all[start:] + st.universe_all[: end - total]

    st.active_symbols = batch
    st.ws_topics = len(batch) * 2  # условный счётчик
    # обновим индекс батча
    st.batch_idx = (st.batch_idx + 1) % max(1, math.ceil(total / n))

    log.info("INFO [universe] total=%d active=%d mode=%s", total, len(batch), cfg.UNIVERSE_MODE)

async def job_poll_oi(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    st: State = app.bot_data["st"]
    if not st.active_symbols:
        return

    async def fetch(sym: str):
        try:
            oi = await st.client.get_open_interest(sym, interval_time="5min", limit=6)
            if oi:
                st.oi_cache[sym] = oi
        except Exception:
            pass

    # параллельно, но ограничим
    sem = asyncio.Semaphore(12)
    async def run(sym: str):
        async with sem:
            await fetch(sym)

    await asyncio.gather(*(run(s) for s in st.active_symbols))

async def job_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    cfg: Config = app.bot_data["cfg"]
    st: State = app.bot_data["st"]

    if not st.active_symbols:
        return

    out_msgs: List[Tuple[str, str]] = []

    async def scan_symbol(sym: str):
        # кулдаун на символ
        if not st.can_send_signal(sym, cfg.SIGNAL_COOLDOWN_SEC):
            return

        try:
            kl = await st.client.get_klines(sym, interval="5", limit= max(60, cfg.VOL_SMA_PERIOD + cfg.ATR_PERIOD + 2))
            if not kl:
                return
            oi = st.oi_cache.get(sym) or await st.client.get_open_interest(sym, interval_time="5min", limit=6)
            if not oi:
                return

            sig = make_signal_from_data(sym, kl, oi, cfg)
            if not sig:
                return

            current = float(kl[-1][4])
            text = format_signal_text(sym, sig, current)
            out_msgs.append((sym, text))
            st.mark_signal(sym)
        except Exception as e:
            log.debug("scan %s failed: %s", sym, e)

    sem = asyncio.Semaphore(10)
    async def run(sym: str):
        async with sem:
            await scan_symbol(sym)

    await asyncio.gather(*(run(s) for s in st.active_symbols))

    # отправим
    for sym, text in out_msgs:
        for cid in cfg.PRIMARY_RECIPIENTS:
            try:
                await app.bot.send_message(chat_id=cid, text=text)
            except Exception as e:
                log.warning("send signal %s failed: %s", sym, e)

    log.info(
        "scan: candidates=%d sent=%d active=%d batch#%d",
        len(st.active_symbols), len(out_msgs), len(st.active_symbols), st.batch_idx
    )

# =========================
# LIFECYCLE HOOKS
# =========================

async def on_shutdown(app: Application) -> None:
    try:
        st: State = app.bot_data.get("st")
        if st:
            await st.client.close()
    except Exception:
        pass

# =========================
# MAIN
# =========================

def main() -> None:
    cfg = Config.load()

    # создаём приложение
    application: Application = (
        ApplicationBuilder()
        .token(cfg.TELEGRAM_TOKEN)
        .post_shutdown(on_shutdown)
        .build()
    )

    # Состояние
    client = BybitClient()
    st = State(client=client)

    # Сохраняем в bot_data (мутабельный dict)
    application.bot_data["cfg"] = cfg
    application.bot_data["st"] = st

    # Команды
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("universe", cmd_universe))
    application.add_handler(CommandHandler("status", cmd_status))

    # Планировщик PTB
    jq = application.job_queue
    # Быстрый разовый boot-online
    jq.run_once(job_boot_online, when=3)
    # Health-пульс: первый через 15 сек, затем каждые HEALTH_SEC
    jq.run_repeating(job_health, interval=cfg.HEALTH_SEC, first=15)
    # Ротация вселенной
    jq.run_repeating(job_rotate, interval=cfg.ROTATE_MIN * 60, first=cfg.FIRST_SCAN_DELAY)
    # OI-пуллинг
    jq.run_repeating(job_poll_oi, interval=60, first=cfg.FIRST_SCAN_DELAY + 5)
    # Сканер/сигналы
    jq.run_repeating(job_scan, interval=30, first=cfg.FIRST_SCAN_DELAY + 10)

    # Стартуем POLLING (блокирующе)
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )

if __name__ == "__main__":
    main()
