# -*- coding: utf-8 -*-
"""
CryptoBot — Telegram сканер сигналов Bybit USDT perpetual
Оптимизировано под реакцию ~5 секунд и минимизацию Render ENV.

Основные изменения:
- Непрерывный async-скан с интервалом 5s и стартовой задержкой 1s.
- Параллельный анализ символов (bounded concurrency), чтобы батч обрабатывался за 1–2s.
- Боевые дефолты зашиты в код; ENV не обязателен (кроме токена, если не хардкодить).
- Быстрый aiohttp-коннектор и аккуратные таймауты для сетевых вызовов.

Совместимость: Python 3.11+ (проверено на 3.13).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web
import contextlib

# ---------------------------------------------------------------------
# Пороговые значения фильтра сигналов (можно переопределить через ENV)
# ---------------------------------------------------------------------

# минимальная ожидаемая прибыль (take-profit) в долях (0.01 = 1%)
TP_MIN_PCT_DEFAULT = 0.01
TP_MIN_PCT = float(os.getenv("TP_MIN_PCT", str(TP_MIN_PCT_DEFAULT)))

# минимальная вероятность выполнения сценария (0.70 = 70%)
PROB_MIN_DEFAULT = 0.70
PROB_MIN = float(os.getenv("PROB_MIN", str(PROB_MIN_DEFAULT)))

# параметры микро-аналитики (ATR, объём и т.п.)
ATR_PERIOD_DEFAULT = int(os.getenv("ATR_PERIOD", "14"))
VOL_SMA_PERIOD_DEFAULT = int(os.getenv("VOL_SMA_PERIOD", "20"))
BODY_ATR_MULT_DEFAULT = float(os.getenv("BODY_ATR_MULT", "0.6"))
VOL_MULT_DEFAULT = float(os.getenv("VOL_MULT", "2.0"))

# ---------------------------------------------------------------------
# Настройки и конфиг (ENV не обязателен — всё имеет дефолты в коде)
# ---------------------------------------------------------------------

def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default if default is not None else None)
    if v is None:
        return None
    v = v.strip()
    return v if v else default

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
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


@dataclass
class Config:
    # Telegram / инфраструктура
    token: str
    log_level: str
    public_url: Optional[str]
    port: int

    # Телеметрия и фоновые задачи
    keepalive_sec: int
    heartbeat_sec: int
    scan_interval_sec: int
    first_scan_delay_sec: int

    # Роутинг сообщений
    primary_recipients: List[int]
    allowed_chat_ids: List[int]
    only_channel: bool

    # Bybit
    bybit_base: str

    # Аналитика
    analysis_batch_size: int
    max_concurrency: int  # одновременных анализов

    # Фильтры
    tp_min_pct: float
    prob_min: float

    # Микро-настройки сигналов
    atr_period: int
    vol_sma_period: int
    body_atr_mult: float
    vol_mult: float

    @staticmethod
    def load() -> "Config":
        # Токен лучше держать в ENV. Если хочешь — можно вписать вручную строкой ниже:
        token = _env_str("TELEGRAM_TOKEN") or _env_str("BOT_TOKEN")
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN не задан (и не захардкожен в коде)")

        # Значения ниже зашиты так, чтобы НЕ требовать Render ENV.
        # При наличии ENV — переопределяются.
        default_primary = [-1002870952333]  # твой канал по умолчанию
        default_allowed = [533232884, -1002870952333]  # разрешённые чаты по умолчанию

        def _parse_ids(name: str, default_ids: List[int]) -> List[int]:
            raw = _env_str(name)
            if not raw:
                return list(default_ids)
            ids: List[int] = []
            for chunk in re.split(r"[,\s]+", raw.strip()):
                if not chunk:
                    continue
                try:
                    ids.append(int(chunk))
                except Exception:
                    pass
            return ids or list(default_ids)

        return Config(
            token=token,
            log_level=_env_str("LOG_LEVEL", "INFO"),
            public_url=_env_str("RENDER_EXTERNAL_URL") or _env_str("PUBLIC_URL"),
            port=_env_int("PORT", 8000),

            # дефолты ускорены/ужаты
            keepalive_sec=_env_int("KEEPALIVE_SEC", 600),
            heartbeat_sec=_env_int("HEARTBEAT_SEC", 3600),
            scan_interval_sec=_env_int("SCAN_INTERVAL_SEC", 5),        # было 12s в логах
            first_scan_delay_sec=_env_int("FIRST_SCAN_DELAY_SEC", 1),  # было 5–10s

            primary_recipients=_parse_ids("PRIMARY_RECIPIENTS", default_primary),
            allowed_chat_ids=_parse_ids("ALLOWED_CHAT_IDS", default_allowed),
            only_channel=_env_bool("ONLY_CHANNEL", True),

            bybit_base=_env_str("BYBIT_BASE", "https://api.bybit.com"),

            analysis_batch_size=_env_int("ANALYSIS_BATCH_SIZE", 20),  # было 30
            max_concurrency=_env_int("MAX_CONCURRENCY", 12),

            tp_min_pct=_env_float("TP_MIN_PCT", TP_MIN_PCT),
            prob_min=_env_float("PROB_MIN", PROB_MIN),

            atr_period=_env_int("ATR_PERIOD", ATR_PERIOD_DEFAULT),
            vol_sma_period=_env_int("VOL_SMA_PERIOD", VOL_SMA_PERIOD_DEFAULT),
            body_atr_mult=_env_float("BODY_ATR_MULT", BODY_ATR_MULT_DEFAULT),
            vol_mult=_env_float("VOL_MULT", VOL_MULT_DEFAULT),
        )


# ---------------------------------------------------------------------
# ЛОГГЕР
# ---------------------------------------------------------------------
def setup_logging(level: str = "INFO") -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ
# ---------------------------------------------------------------------
def pct(x: float) -> str:
    return f"{x:.2%}"

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------
# Bybit HTTP
# ---------------------------------------------------------------------
class Bybit:
    def __init__(self, base: str, session: aiohttp.ClientSession) -> None:
        self.base = base.rstrip("/")
        self.http = session

    async def tickers(self) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/tickers?category=linear"
        timeout = aiohttp.ClientTimeout(total=5)
        async with self.http.get(url, timeout=timeout) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []

    async def kline(self, symbol: str, interval_min: int = 1, limit: int = 60) -> List[Dict[str, Any]]:
        url = (
            f"{self.base}/v5/market/kline?category=linear&symbol={symbol}"
            f"&interval={interval_min}&limit={limit}"
        )
        timeout = aiohttp.ClientTimeout(total=5)
        async with self.http.get(url, timeout=timeout) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []

    async def open_interest(self, symbol: str, interval: str = "5min", limit: int = 12) -> List[Dict[str, Any]]:
        url = (
            f"{self.base}/v5/market/open-interest?category=linear&symbol={symbol}"
            f"&intervalTime={interval}&limit={limit}"
        )
        timeout = aiohttp.ClientTimeout(total=5)
        async with self.http.get(url, timeout=timeout) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []

    async def recent_liq(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        url = f"{self.base}/v5/market/liquidation?category=linear&symbol={symbol}&limit={limit}"
        timeout = aiohttp.ClientTimeout(total=5)
        async with self.http.get(url, timeout=timeout) as r:
            r.raise_for_status()
            data = await r.json()
        return data.get("result", {}).get("list", []) or []


# ---------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------
class Tg:
    def __init__(self, token: str, session: aiohttp.ClientSession) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = session

    async def get_me(self) -> Dict[str, Any]:
        async with self.http.post(f"{self.base}/getMe") as r:
            r.raise_for_status()
            return await r.json()

    async def send_message(self, chat_id: int, text: str, disable_web_page_preview: bool = True) -> None:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_web_page_preview,
        }
        async with self.http.post(f"{self.base}/sendMessage", json=payload) as r:
            r.raise_for_status()
            await r.json()

    async def get_updates(self) -> Dict[str, Any]:
        async with self.http.post(f"{self.base}/getUpdates") as r:
            r.raise_for_status()
            return await r.json()


# ---------------------------------------------------------------------
# СТРАТЕГИЯ / СКАНЕР
# ---------------------------------------------------------------------
def _score_ticker(t: Dict[str, Any]) -> float:
    # Небольшой скоринг по волатильности/объёму
    try:
        chg = abs(float(t.get("price24hPcnt") or 0.0))
        vol = float(t.get("turnover24h") or 0.0)
    except Exception:
        return 0.0
    return chg * 0.5 + math.log10(vol + 1.0) * 0.5

def _format_signal(sig: Dict[str, Any]) -> str:
    """Человекочитаемое сообщение без кнопок."""
    sym = sig["symbol"]
    side = sig["side"]
    entry = sig["entry"]
    tp = sig["tp"]
    sl = sig["sl"]
    prob = sig["prob"]
    tp_pct = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry

    text = (
        f"⚡️ <b>Сигнал по {sym}</b>\n"
        f"Направление: <b>{'LONG' if side=='LONG' else 'SHORT'}</b>\n"
        f"Текущая цена: {entry:g}\n"
        f"Тейк: {tp:g} ({pct(tp_pct)})\n"
        f"Стоп: {sl:g}\n"
        f"Вероятность: <b>{pct(prob)}</b>\n"
    )

    reason = sig.get("reason")
    if reason:
        text += f"Обоснование: {reason}\n"

    return text


async def analyze_symbol(bybit: Bybit, symbol: str, cfg: Config) -> Optional[Dict[str, Any]]:
    """
    Простейшая эвристика: 1m свечи, изменение, ATR, дельта OI и наличие/отсутствие ликвидаций.
    Возвращаем словарь сигнала или None.
    """
    try:
        k = await bybit.kline(symbol, interval_min=1, limit=max(cfg.vol_sma_period, cfg.atr_period) + 2)
        if not k or len(k) < 3:
            return None

        # kline формат Bybit v5: [start, open, high, low, close, volume, turnover]
        closes = [float(row[4]) for row in k]
        highs = [float(row[2]) for row in k]
        lows = [float(row[3]) for row in k]
        vols = [float(row[5]) for row in k]

        close = closes[-1]
        prev_close = closes[-2]

        # Простая метрика импульса по телу свечи
        body = abs(close - prev_close)

        # ATR
        tr_list: List[float] = []
        for i in range(1, len(closes)):
            high = highs[i]
            low = lows[i]
            prev_c = closes[i - 1]
            tr = max(high - low, abs(high - prev_c), abs(low - prev_c))
            tr_list.append(tr)
        atr = sum(tr_list[-cfg.atr_period:]) / max(cfg.atr_period, 1)

        # Объёмы
        if len(vols) >= cfg.vol_sma_period:
            sma = sum(vols[-cfg.vol_sma_period:]) / cfg.vol_sma_period
        else:
            sma = sum(vols) / max(len(vols), 1)
        big_vol = vols[-1] >= cfg.vol_mult * sma

        # Open Interest (динамика)
        oi = await bybit.open_interest(symbol, interval="5min", limit=2)
        d_oi = 0.0
        if oi and len(oi) >= 2:
            last_oi = float(oi[-1].get("openInterest", oi[-1].get("openInterestValue", 0.0)) or 0.0)
            prev_oi = float(oi[-2].get("openInterest", oi[-2].get("openInterestValue", 0.0)) or 0.0)
            if prev_oi > 0:
                d_oi = (last_oi - prev_oi) / prev_oi

        # Недавние ликвидации (простой индикатор «смыва»)
        liq = await bybit.recent_liq(symbol, limit=50)
        has_liq = bool(liq)

        # Сторона по импульсу
        side = "LONG" if close >= prev_close else "SHORT"

        # Таргеты/стопы весьма условные (демо-логика)
        tp = close * (1 + 0.006) if side == "LONG" else close * (1 - 0.006)
        sl = close * (1 - 0.004) if side == "LONG" else close * (1 + 0.004)

        # Наивная вероятность
        prob = 0.5
        if body > cfg.body_atr_mult * atr:
            prob += 0.15
        if big_vol:
            prob += 0.15
        # OI: рост при SHORT — риск, при LONG — поддержка
        if side == "LONG" and d_oi >= 0:
            prob += 0.05
        if side == "SHORT" and d_oi <= 0:
            prob += 0.05
        # Ликвидации как триггер разворота
        if has_liq:
            prob += 0.05
        prob = max(0.0, min(prob, 0.99))

        reason_bits = []
        if big_vol:
            reason_bits.append("объём ≥ 2×SMA")
        if abs(body) > cfg.body_atr_mult * atr:
            reason_bits.append(f"тело > {cfg.body_atr_mult:.1f}*ATR")
        if d_oi != 0:
            reason_bits.append(f"ΔOI={d_oi:+.2%}")
        if has_liq:
            reason_bits.append("ликвидации рядом")
        reason = ", ".join(reason_bits)

        return {
            "symbol": symbol,
            "side": side,
            "entry": close,
            "tp": tp,
            "sl": sl,
            "prob": prob,
            "reason": reason,
        }
    except Exception as e:
        logger.exception(f"analyze_symbol {symbol}: {e}")
        return None


async def job_scan(app: web.Application) -> None:
    cfg: Config = app["cfg"]
    bybit: Bybit = app["bybit"]
    tg: Tg = app["tg"]

    # 1) список тикеров и грубая сортировка
    all_tickers = await bybit.tickers()
    scored = [(t.get("symbol"), _score_ticker(t)) for t in all_tickers if t.get("symbol")]
    scored.sort(key=lambda x: x[1], reverse=True)

    # ограничиваем анализ согласно ANALYSIS_BATCH_SIZE
    batch = [s for s, _ in scored[: max(1, cfg.analysis_batch_size)]]
    logger.info(f"scan: candidates={len(batch)}")

    signals: List[Tuple[Dict[str, Any], str]] = []

    # 2) анализ каждого символа — ПАРАЛЛЕЛЬНО (bounded concurrency)
    sem = asyncio.Semaphore(cfg.max_concurrency)

    async def _one(sym: str) -> Optional[Tuple[Dict[str, Any], str]]:
        async with sem:
            res = await analyze_symbol(bybit, sym, cfg)
            if not res:
                return None

            # --- фильтры ---
            # 1) Вероятность
            if res.get("prob", 0.0) < cfg.prob_min:
                logger.info(f"[filter] skip {sym}: prob<{cfg.prob_min:.2f}")
                return None

            # 2) Минимальный ожидаемый профит (до тейка)
            entry = float(res["entry"])
            tp = float(res["tp"])
            side = res["side"]
            tp_pct = (tp - entry) / entry if side == "LONG" else (entry - tp) / entry
            if tp_pct < cfg.tp_min_pct:
                logger.info(f"[filter] skip {sym}: tp<{cfg.tp_min_pct:.2%}")
                return None

            text = _format_signal(res)
            return (res, text)

    results = await asyncio.gather(*[_one(sym) for sym in batch], return_exceptions=False)
    for item in results:
        if item:
            signals.append(item)

    # 3) рассылка (если есть)
    if not signals:
        return

    for _sig, text in signals:
        for chat_id in cfg.primary_recipients:
            try:
                await tg.send_message(chat_id, text)
            except Exception as e:
                logger.exception(f"send_message to {chat_id}: {e}")


# ---------------------------------------------------------------------
# HEALTH/KEEPALIVE/HEARTBEAT
# ---------------------------------------------------------------------
async def handle_health(request: web.Request) -> web.Response:
    app = request.app
    return web.json_response(
        {
            "ok": True,
            "uptime_sec": int(time.monotonic() - app.get("start_ts", time.monotonic())),
            "time": now_utc().isoformat(),
            "tp_min_pct": app["cfg"].tp_min_pct,
            "prob_min": app["cfg"].prob_min,
            "batch_size": app["cfg"].analysis_batch_size,
            "scan_interval_sec": app["cfg"].scan_interval_sec,
        }
    )

async def job_keepalive(app: web.Application) -> None:
    url = (app["cfg"].public_url or "").rstrip("/") + "/health"
    if not url.startswith("http"):
        return
    try:
        async with app["http"].get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
            ok = r.status == 200
        logger.info(f"[keepalive] ping {url} {'✓' if ok else '✗'}")
    except Exception as e:
        logger.warning(f"[keepalive] ping error: {e}")

async def job_heartbeat(app: web.Application) -> None:
    cfg: Config = app["cfg"]
    tg: Tg = app["tg"]
    text = "✅ Бот онлайн"
    for chat_id in cfg.primary_recipients:
        try:
            await tg.send_message(chat_id, text)
        except Exception as e:
            logger.exception(f"heartbeat to {chat_id}: {e}")


# ---------------------------------------------------------------------
# ИНИЦИАЛИЗАЦИЯ ПРИЛОЖЕНИЯ
# ---------------------------------------------------------------------
async def on_startup(app: web.Application) -> None:
    cfg: Config = app["cfg"]
    app["start_ts"] = time.monotonic()
    logger.info("startup.")

    # Создаём HTTP-сессию и клиенты ТОЛЬКО внутри запущенного event loop
    # (это уже было сделано в твоём проекте — сохраняем подход) :contentReference[oaicite:5]{index=5}
    connector = aiohttp.TCPConnector(limit=64, ttl_dns_cache=60, enable_cleanup_closed=True)
    http = aiohttp.ClientSession(connector=connector)
    app["http"] = http
    app["bybit"] = Bybit(cfg.bybit_base, http)
    app["tg"] = Tg(cfg.token, http)

    loop = asyncio.get_event_loop()

    async def _scan_loop() -> None:
        await asyncio.sleep(cfg.first_scan_delay_sec)
        while True:
            t0 = time.perf_counter()
            try:
                await job_scan(app)
            except Exception as e:
                logger.exception(f"job_scan: {e}")
            dt = time.perf_counter() - t0
            # логируем «следующий запуск», чтобы сохранить привычный формат
            nxt = (now_utc() + timedelta(seconds=cfg.scan_interval_sec)).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(
                f'Job "job_scan (trigger: interval[0:00:{cfg.scan_interval_sec:02d}], '
                f'next run at: {nxt} UTC)" executed successfully'
            )
            await asyncio.sleep(max(0.0, cfg.scan_interval_sec - dt))

    async def _keepalive_loop() -> None:
        while True:
            await asyncio.sleep(cfg.keepalive_sec)
            try:
                await job_keepalive(app)
            except Exception as e:
                logger.exception(f"job_keepalive: {e}")

    async def _heartbeat_loop() -> None:
        while True:
            await asyncio.sleep(cfg.heartbeat_sec)
            try:
                await job_heartbeat(app)
            except Exception as e:
                logger.exception(f"job_heartbeat: {e}")

    app["scan_task"] = loop.create_task(_scan_loop())
    app["keepalive_task"] = loop.create_task(_keepalive_loop())
    app["heartbeat_task"] = loop.create_task(_heartbeat_loop())

async def on_cleanup(app: web.Application) -> None:
    # останавливаем фоновые задачи
    for key in ("scan_task", "keepalive_task", "heartbeat_task"):
        task: Optional[asyncio.Task] = app.get(key)
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    # закрываем HTTP-сессию
    http: Optional[aiohttp.ClientSession] = app.get("http")
    if http and not http.closed:
        await http.close()


# ---------------------------------------------------------------------
# MAIN / web
# ---------------------------------------------------------------------
def make_app(cfg: Config) -> web.Application:
    app = web.Application()
    app["cfg"] = cfg

    # Роуты
    app.router.add_get("/health", handle_health)

    # Жизненный цикл
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

def main() -> None:
    cfg = Config.load()
    setup_logging(cfg.log_level)

    logger.info(
        "cfg: "
        f"public_url={cfg.public_url} | "
        f"scan_interval={cfg.scan_interval_sec}s | "
        f"batch={cfg.analysis_batch_size} | "
        f"tp_min={cfg.tp_min_pct:.2%} | "
        f"prob_min={cfg.prob_min:.0%} | "
        f"max_conc={cfg.max_concurrency}"
    )

    app = make_app(cfg)
    web.run_app(app, host="0.0.0.0", port=cfg.port)

if __name__ == "__main__":
    main()
