#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
from statistics import mean
from datetime import datetime, timedelta

import aiohttp
from aiohttp import web

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from telegram.error import Conflict


# -----------------------------------------------------------------------------
# ЛОГИРОВАНИЕ
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cryptobot")


# -----------------------------------------------------------------------------
# УТИЛИТЫ ENV
# -----------------------------------------------------------------------------
def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


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


def _env_list_int(name: str, default: Optional[List[int]] = None) -> List[int]:
    raw = os.getenv(name)
    if not raw:
        return default or []
    out: List[int] = []
    for part in re.split(r"[,\s]+", raw.strip()):
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            pass
    return out


def _normalize_public_url(u: Optional[str]) -> Optional[str]:
    """Нормализуем публичный URL сервиса (для keepalive)."""
    if not u:
        return None
    u = u.strip()
    if not u:
        return None
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u
    return u.rstrip("/")


# -----------------------------------------------------------------------------
# КОНФИГ
# -----------------------------------------------------------------------------
class Config:
    def __init__(self, **kw: Any) -> None:
        self.telegram_token: str = kw["telegram_token"]
        self.port: int = kw["port"]
        self.allowed_chat_ids: List[int] = kw.get("allowed_chat_ids", [])
        self.primary_recipients: List[int] = kw.get("primary_recipients", [])

        # управление отправкой и таймингами
        self.only_channel: bool = kw.get("only_channel", True)
        self.startup_delay_sec: int = kw.get("startup_delay_sec", 5)
        self.first_scan_delay_sec: int = kw.get("first_scan_delay_sec", 10)
        self.heartbeat_sec: int = kw.get("heartbeat_sec", 900)

        # интервал сканирования
        self.scan_interval_sec: int = kw.get("scan_interval_sec", 30)

        # Параметры «вселенной»
        self.universe_top_n: int = kw.get("universe_top_n", 30)
        self.ws_symbols_max: int = kw.get("ws_symbols_max", 60)

        # Bybit
        self.bybit_base: str = kw.get("bybit_base", "https://api.bybit.com")

        # Keepalive (само-пинг)
        self.public_url: Optional[str] = kw.get("public_url")
        self.self_ping: bool = kw.get("self_ping", True)
        self.self_ping_interval_sec: int = kw.get("self_ping_interval_sec", 780)  # ~13 мин

        # Аналитика/сигналы
        self.analysis_enabled: bool = kw.get("analysis_enabled", True)
        self.analysis_batch_size: int = kw.get("analysis_batch_size", 3)
        self.signal_ttl_min: int = kw.get("signal_ttl_min", 12)
        self.signal_cooldown_sec: int = kw.get("signal_cooldown_sec", 600)

        # Пороговые параметры анализа
        self.vol_sma_period: int = kw.get("vol_sma_period", 20)
        self.vol_mult: float = kw.get("vol_mult", 2.0)
        self.atr_period: int = kw.get("atr_period", 14)
        self.body_atr_mult: float = kw.get("body_atr_mult", 0.60)

        # --- ДОБАВЛЕНО: параметры «шока ликвидаций» и реверс-сценария ---
        self.liq_enable_reversal: bool = kw.get("liq_enable_reversal", True)
        self.liq_events_min: int = kw.get("liq_events_min", 5)              # мин. число событий
        self.liq_notional_min: float = kw.get("liq_notional_min", 25000.0)  # мин. номинал (USDT)
        self.liq_reversal_lookback: int = kw.get("liq_reversal_lookback", 3)  # глубина проверки реверса
        self.liq_vol_mult_min: float = kw.get("liq_vol_mult_min", 1.05)        # ↓ было 1.2

    @staticmethod
    def load() -> "Config":
        token = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN is required")

        pub_url = (
            os.getenv("PUBLIC_URL")
            or os.getenv("RENDER_EXTERNAL_URL")
        )
        pub_url = _normalize_public_url(pub_url)

        return Config(
            telegram_token=token,
            port=_env_int("PORT", 10000),
            allowed_chat_ids=_env_list_int("ALLOWED_CHAT_IDS", []),
            primary_recipients=_env_list_int("PRIMARY_RECIPIENTS", []),

            only_channel=_env_bool("ONLY_CHANNEL", True),
            startup_delay_sec=_env_int("STARTUP_DELAY_SEC", 5),
            first_scan_delay_sec=_env_int("FIRST_SCAN_DELAY_SEC", 10),
            heartbeat_sec=_env_int("HEARTBEAT_SEC", 900),

            scan_interval_sec=_env_int("SCAN_INTERVAL_SEC", 30),

            universe_top_n=_env_int("UNIVERSE_TOP_N", 30),
            ws_symbols_max=_env_int("WS_SYMBOLS_MAX", 60),

            bybit_base=os.getenv("BYBIT_BASE", "https://api.bybit.com"),

            public_url=pub_url,
            self_ping=_env_bool("SELF_PING", True),
            self_ping_interval_sec=_env_int("KEEPALIVE_SEC", 780),

            analysis_enabled=_env_bool("ANALYSIS_ENABLED", True),
            analysis_batch_size=_env_int("ANALYSIS_BATCH_SIZE", 3),
            signal_ttl_min=_env_int("SIGNAL_TTL_MIN", 12),
            signal_cooldown_sec=_env_int("SIGNAL_COOLDOWN_SEC", 600),

            vol_sma_period=_env_int("VOL_SMA_PERIOD", 20),
            vol_mult=_env_float("VOL_MULT", 2.0),
            atr_period=_env_int("ATR_PERIOD", 14),
            body_atr_mult=_env_float("BODY_ATR_MULT", 0.60),

            # --- ДОБАВЛЕНО: ENV для liq-reversal ---
            liq_enable_reversal=_env_bool("LIQ_REVERSAL_ENABLED", True),
            liq_events_min=_env_int("LIQ_EVENTS_MIN", 5),
            liq_notional_min=_env_float("LIQ_NOTIONAL_MIN", 25000.0),
            liq_reversal_lookback=_env_int("LIQ_REVERSAL_LOOKBACK", 3),
            liq_vol_mult_min=_env_float("LIQ_VOL_MULT_MIN", 1.05),  # ↓ было 1.2
        )


# -----------------------------------------------------------------------------
# BYBIT КЛИЕНТ (создаём СЕССИЮ ТОЛЬКО ПОСЛЕ СТАРТА ЛУПА!)
# -----------------------------------------------------------------------------
class BybitClient:
    def __init__(self, base: str = "https://api.bybit.com") -> None:
        self.base = base
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def close(self) -> None:
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None

    async def fetch_linear_symbols(self) -> List[str]:
        """
        Тянем список фьючерсных USDT-символов (category=linear).
        """
        session = await self._ensure_session()
        url = f"{self.base}/v5/market/instruments-info?category=linear"
        out: List[str] = []
        cursor = None
        for _ in range(5):
            u = url if not cursor else f"{url}&cursor={cursor}"
            async with session.get(u) as resp:
                data = await resp.json()
            if data.get("retCode") != 0:
                break
            list_ = (data.get("result") or {}).get("list") or []
            for it in list_:
                sym = it.get("symbol")
                if sym and sym.endswith("USDT"):
                    out.append(sym)
            cursor = (data.get("result") or {}).get("nextPageCursor")
            if not cursor:
                break
            await asyncio.sleep(0)
        return sorted(set(out))

    async def fetch_kline(self, symbol: str, interval: str = "5", limit: int = 200) -> Dict[str, Any]:
        """
        Kline по v5:
        GET /v5/market/kline?category=linear&symbol=SYM&interval=5&limit=200
        """
        session = await self._ensure_session()
        url = f"{self.base}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
        async with session.get(url) as resp:
            return await resp.json()

    async def fetch_open_interest(self, symbol: str, interval: str = "5min", limit: int = 6) -> Dict[str, Any]:
        """
        Open Interest:
        GET /v5/market/open-interest?category=linear&symbol=SYM&intervalTime=5min&limit=6
        """
        session = await self._ensure_session()
        url = f"{self.base}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval}&limit={limit}"
        async with session.get(url) as resp:
            return await resp.json()

    async def fetch_liquidations(self, symbol: str, limit: int = 50) -> Dict[str, Any]:
        """
        Liquidations:
        GET /v5/market/liquidation?category=linear&symbol=SYM&limit=50
        """
        session = await self._ensure_session()
        url = f"{self.base}/v5/market/liquidation?category=linear&symbol={symbol}&limit={limit}"
        async with session.get(url) as resp:
            return await resp.json()


# -----------------------------------------------------------------------------
# УНИВЕРС-МЕНЕДЖЕР
# -----------------------------------------------------------------------------
class UniverseState:
    def __init__(self) -> None:
        self.total: int = 0
        self.active: int = 0
        self.batch: int = 0
        self.ws_topics: int = 0
        self.sample_active: List[str] = []


async def build_universe(app: Application, cfg: Config) -> None:
    """
    Тяжёлая инициализация вселенной — запускаем ПОСЛЕ открытия порта.
    """
    client: BybitClient = app.bot_data["bybit"]
    st: UniverseState = app.bot_data["universe_state"]

    try:
        syms = await client.fetch_linear_symbols()
        st.total = len(syms)
        st.active = min(cfg.universe_top_n, st.total)
        st.ws_topics = min(cfg.ws_symbols_max, st.active)
        st.sample_active = syms[:st.active]
        st.batch = 0
        logger.info(f"[universe] total={st.total} active={st.active} mode=all")
    except Exception:
        logger.exception("[universe] failed to load; will retry later")


# -----------------------------------------------------------------------------
# HEALTH-СЕРВЕР — открываем порт сразу
# -----------------------------------------------------------------------------
async def start_health_server(port: int) -> None:
    app = web.Application()

    async def health(_):
        return web.Response(text="ok")

    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    logger.info(f"[health] listening on :{port} (/health)")


# -----------------------------------------------------------------------------
# ОТПРАВКА (только в канал при ONLY_CHANNEL=1)
# -----------------------------------------------------------------------------
async def notify(
    app: Application,
    text: str,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = True,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    cfg: Config = app.bot_data["cfg"]
    recipients = list(cfg.primary_recipients)

    if cfg.only_channel:
        recipients = [cid for cid in recipients if isinstance(cid, int) and cid < 0]

    # предупреждение, если некуда слать
    if not recipients:
        logger.warning(
            "[notify] recipients list is empty. ONLY_CHANNEL=%s, PRIMARY_RECIPIENTS=%s",
            cfg.only_channel,
            cfg.primary_recipients,
        )
        return

    for cid in recipients:
        try:
            await app.bot.send_message(
                chat_id=cid,
                text=text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                reply_markup=reply_markup,
            )
        except Exception as e:
            logger.warning(f"notify: failed to send to {cid}: {e}")


# -----------------------------------------------------------------------------
# ПРОСТЕЙШИЙ АНАЛИЗ
# -----------------------------------------------------------------------------
def _calc_atr(candles: List[List[str]], period: int) -> float:
    """
    candles: список строковых [startTs, open, high, low, close, volume, ...] (как от Bybit)
    ATR — упрощённо через средний (high-low).
    """
    if not candles:
        return 0.0
    span = candles[-period:]
    rng = []
    for c in span:
        try:
            high = float(c[2])
            low = float(c[3])
            rng.append(abs(high - low))
        except Exception:
            pass
    return mean(rng) if rng else 0.0


def _fmt_pct(x: float) -> str:
    return f"{x*100:.2f}%"


def _pick_batch_symbols(symbols: List[str], batch: int, batch_size: int) -> List[str]:
    if not symbols:
        return []
    n = len(symbols)
    start = (batch * batch_size) % n
    out = symbols[start:start + batch_size]
    if len(out) < batch_size:  # докрутить из начала, если вышли за край
        out += symbols[: batch_size - len(out)]
    return out


def _make_bybit_link(symbol: str) -> str:
    # простая ссылка на фьючерс USDT
    base = symbol.replace("USDT", "")
    return f"https://www.bybit.com/trade/usdt/{base}"


async def _analyze_symbol(app: Application, cfg: Config, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Возвращает словарь с метриками и потенциальным направлением;
    либо None, если триггеров нет/ошибка.
    """
    client: BybitClient = app.bot_data["bybit"]

    # параллельно дернем три запроса
    k_task = asyncio.create_task(client.fetch_kline(symbol, interval="5", limit=max(60, cfg.vol_sma_period + 5)))
    oi_task = asyncio.create_task(client.fetch_open_interest(symbol, interval="5min", limit=6))
    liq_task = asyncio.create_task(client.fetch_liquidations(symbol, limit=50))

    k = await k_task
    oi = await oi_task
    lq = await liq_task

    # --- KLINE ---
    k_ok = (k or {}).get("retCode") == 0
    k_list = ((k or {}).get("result") or {}).get("list") or []
    k_list = list(reversed(k_list))  # Bybit часто отдает от старых к новым — перевернём
    chg = vol_mult = atr_val = body = last_close = 0.0
    if k_ok and len(k_list) >= max(3, cfg.vol_sma_period + 1):
        try:
            o1 = float(k_list[-2][1])
            c1 = float(k_list[-2][4])
            o2 = float(k_list[-1][1])
            c2 = float(k_list[-1][4])
            v2 = float(k_list[-1][5])
            vols = [float(x[5]) for x in k_list[-(cfg.vol_sma_period+1):-1]]
            v_sma = mean(vols) if vols else 0.0
            vol_mult = (v2 / v_sma) if v_sma > 0 else 0.0
            atr_val = _calc_atr(k_list, cfg.atr_period)
            body = abs(c2 - o2)
            last_close = c2
            chg = (c2 - c1) / c1 if c1 else 0.0
        except Exception:
            pass
    logger.info(f"[kline] {symbol}: chg={_fmt_pct(chg)} volx={vol_mult:.2f} atr={atr_val:.6f} body={body:.6f}")

    # --- OPEN INTEREST ---  (ИСПРАВЛЕНО: парсим словари v5)
    oi_ok = (oi or {}).get("retCode") == 0
    oi_list = ((oi or {}).get("result") or {}).get("list") or []
    oi_d = 0.0
    oi_last = 0.0
    if oi_ok and len(oi_list) >= 2:
        try:
            def _oi_val(item: Any) -> float:
                # v5 обычный формат — словарь {"openInterest": "...", "timestamp": "..."}
                if isinstance(item, dict):
                    v = item.get("openInterest") or item.get("value") or item.get("open_interest") or item.get("oi")
                    return float(v or 0)
                # совместимость на случай массивов [ts, value]
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    return float(item[1] or 0)
                return 0.0

            oi_prev = _oi_val(oi_list[-2])
            oi_last = _oi_val(oi_list[-1])
            oi_d = (oi_last - oi_prev) / oi_prev if oi_prev else 0.0
        except Exception:
            oi_d = 0.0
            oi_last = 0.0
    logger.info(f"[open-interest] {symbol}: d_5min={_fmt_pct(oi_d)} last={oi_last:.3f}")

    # --- LIQUIDATIONS ---
    lq_ok = (lq or {}).get("retCode") == 0
    lq_list = ((lq or {}).get("result") or {}).get("list") or []
    liq_events = 0
    liq_notional = 0.0
    side_long = side_short = 0
    for it in lq_list:
        try:
            liq_events += 1
            # Обычно 'value' — USDT-номинал; если нет — будет 0. Это ок.
            liq_notional += float(it.get("value", 0) or 0)
            s = (it.get("side") or "").lower()
            if s == "buy":
                side_long += 1
            elif s == "sell":
                side_short += 1
        except Exception:
            pass
    dom = "long>short" if side_long > side_short else ("short>long" if side_short > side_long else "balanced")
    logger.info(f"[liquidation] {symbol}: events={liq_events} notional≈{liq_notional:.0f} side={dom}")

    # --- БАЗОВЫЙ ТРИГГЕР (как было) ---
    if not cfg.analysis_enabled:
        return None

    cond_vol = vol_mult >= cfg.vol_mult
    cond_body = body >= cfg.body_atr_mult * atr_val if atr_val > 0 else False
    cond_oi = (chg > 0 and oi_d > 0) or (chg < 0 and oi_d < 0)
    cond_liq = liq_events >= 1

    # Доп. отладка условий — видно при LOG_LEVEL=DEBUG
    logger.debug(f"[triggers] {symbol}: cond_vol={cond_vol} cond_body={cond_body} cond_oi={cond_oi} cond_liq={cond_liq}")

    if cond_vol and cond_body and cond_oi and cond_liq and last_close > 0:
        side = "LONG" if chg > 0 else "SHORT"

        sl_dist = max(1e-6, cfg.body_atr_mult * atr_val)
        rr = 2.0 + max(0.0, min(1.0, vol_mult - cfg.vol_mult)) * 0.5  # чуть разбросать R/R
        tp_dist = rr * sl_dist

        if side == "LONG":
            entry = last_close
            sl = entry - sl_dist
            tp = entry + tp_dist
        else:
            entry = last_close
            sl = entry + sl_dist
            tp = entry - tp_dist

        prob = 0.60 + min(0.22, 0.04 * max(0.0, vol_mult - cfg.vol_mult) + (0.04 if cond_oi else 0.0))

        return {
            "symbol": symbol,
            "side": side,
            "entry": entry,
            "tp": tp,
            "sl": sl,
            "rr": rr,
            "prob": prob,
        }

    # --- ДОБАВЛЕНО: LIQUIDATION SHOCK → REVERSAL ---
    # Идея: после крупной односторонней волны ликвидаций ждём разворот цены в противоположную сторону.
    # Требования: достаточный «шок», согласованный переворот на последней свече и минимальный объём.
    if cfg.liq_enable_reversal and last_close > 0 and atr_val > 0:
        # ↓ точечная правка: мягче критерии «шока»
        liq_shock = (
            (liq_events >= cfg.liq_events_min)
            or (liq_notional >= cfg.liq_notional_min)
            or (liq_events >= 3)
            or (liq_notional >= 0.6 * cfg.liq_notional_min)
        )

        expected_side: Optional[str] = None
        if side_short > side_long:
            expected_side = "LONG"   # больше sell-ликвидов → ищем отскок вверх
        elif side_long > side_short:
            expected_side = "SHORT"  # больше buy-ликвидов → ждём разворот вниз

        # ↓ точечная правка: порог тела свечи 0.4 вместо 0.5
        rev_ok = False
        if expected_side == "LONG":
            rev_ok = (chg > 0) and (body >= 0.4 * cfg.body_atr_mult * atr_val)
        elif expected_side == "SHORT":
            rev_ok = (chg < 0) and (body >= 0.4 * cfg.body_atr_mult * atr_val)

        # ↓ точечная правка: по умолчанию требуем vol_mult >= 1.05 (можно поднять через ENV)
        vol_ok = vol_mult >= cfg.liq_vol_mult_min

        logger.info(f"[liq-reversal] {symbol}: shock={liq_shock} expect={expected_side or '-'} rev_ok={rev_ok} vol_ok={vol_ok}")

        if liq_shock and expected_side and rev_ok and vol_ok:
            side = expected_side

            # тот же калькулятор SL/TP, но R/R чуть поднимем, т.к. это отдельный сетап
            sl_dist = max(1e-6, 0.8 * cfg.body_atr_mult * atr_val)  # чуть ближе стоп для реверса
            rr = 2.2 + min(0.6, 0.2 * max(0.0, vol_mult - cfg.liq_vol_mult_min))
            tp_dist = rr * sl_dist

            entry = last_close
            if side == "LONG":
                sl = entry - sl_dist
                tp = entry + tp_dist
            else:
                sl = entry + sl_dist
                tp = entry - tp_dist

            # вероятность — базовая + бонус за шок ликвидаций
            prob = 0.62 + min(0.20, 0.03 * max(0, liq_events - 3) + 0.00001 * max(0.0, liq_notional - 0.6 * cfg.liq_notional_min))

            return {
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "tp": tp,
                "sl": sl,
                "rr": rr,
                "prob": min(prob, 0.85),
            }

    return None


def _format_signal(sig: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    sym = sig["symbol"]
    side = "ЛОНГ" if sig["side"] == "LONG" else "ШОРТ"
    entry = sig["entry"]
    tp = sig["tp"]
    sl = sig["sl"]
    rr = sig["rr"]
    prob = sig["prob"]

    # проценты к тейку/стопу
    tp_pct = (tp - entry) / entry if sig["side"] == "LONG" else (entry - tp) / entry
    sl_pct = (entry - sl) / entry if sig["side"] == "LONG" else (sl - entry) / entry

    text = (
        f"#{sym} — {side}\n"
        f"Вход: {entry:g}\n"
        f"Тейк: {tp:g} (+{tp_pct*100:.2f}%)\n"
        f"Стоп: {sl:g} (-{sl_pct*100:.2f}%)\n"
        f"R/R: {rr:.2f} | Вероятность: {prob*100:.1f}%"
    )
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Открыть в Bybit", url=_make_bybit_link(sym))]]
    )
    return text, kb


# -----------------------------------------------------------------------------
# ДЖОБЫ
# -----------------------------------------------------------------------------
async def job_heartbeat_simple(app: Application) -> None:
    st: UniverseState = app.bot_data["universe_state"]
    msg = (
        "Онлайн ✅\n"
        f"Вселенная: total={st.total}, active={st.active}, batch#{st.batch}, ws_topics={st.ws_topics}"
    )
    await notify(app, msg)


async def job_heartbeat(context: ContextTypes.DEFAULT_TYPE) -> None:
    await job_heartbeat_simple(context.application)


async def job_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    cfg: Config = app.bot_data["cfg"]
    st: UniverseState = app.bot_data["universe_state"]

    try:
        if not st.total:
            await build_universe(app, cfg)

        # вращаем batch-счётчик как раньше — чтобы не мешать существующей логике
        if st.sample_active:
            # на всякий увеличим модуль от количества "частей" (3 для красоты)
            st.batch = (st.batch + 1) % max(1, (st.active // 10) or 1)

        # --- МИНИ-АНАЛИЗ БАТЧА ---
        sent_now = 0
        if cfg.analysis_enabled and st.sample_active:
            batch_syms = _pick_batch_symbols(st.sample_active, st.batch, max(1, cfg.analysis_batch_size))

            # параллельно анализируем символы
            results = await asyncio.gather(
                *[ _analyze_symbol(app, cfg, s) for s in batch_syms ],
                return_exceptions=True
            )

            # анти-дубли (TTL и cooldown)
            sent_map: Dict[str, float] = app.bot_data.setdefault("sent_signals", {})
            now_ts = datetime.utcnow().timestamp()

            for sym, res in zip(batch_syms, results):
                if isinstance(res, Exception) or not res:
                    continue

                last_ts = sent_map.get(sym, 0.0)
                if now_ts - last_ts < cfg.signal_cooldown_sec:
                    continue  # рано

                # внутри TTL — не повторяем
                if now_ts - last_ts < cfg.signal_ttl_min * 60:
                    continue

                text, kb = _format_signal(res)
                await notify(app, text, reply_markup=kb)
                sent_map[sym] = now_ts
                sent_now += 1
                if sent_now >= 2:
                    break  # на один проход — максимум 2 сигнала

        logger.info(
            f"scan: candidates={st.active} sent={sent_now} active={st.active} batch#{st.batch}"
        )
    except Exception:
        logger.exception("job_scan failed")
    await asyncio.sleep(0)


# keepalive — периодический само-пинг, чтобы Render не засыпал
async def job_keepalive(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    cfg: Config = app.bot_data["cfg"]
    url = (cfg.public_url + "/health") if cfg.public_url else f"http://127.0.0.1:{cfg.port}/health"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url) as r:
                await r.text()
        logger.info(f"[keepalive] ping {url} ✓")
    except Exception:
        logger.exception(f"[keepalive] ping {url} failed")


# -----------------------------------------------------------------------------
# КОМАНДЫ
# -----------------------------------------------------------------------------
def _is_allowed(update: Update, cfg: Config) -> bool:
    cid = (update.effective_chat.id if update.effective_chat else None)
    if cid is None:
        return False
    return (not cfg.allowed_chat_ids) or (cid in cfg.allowed_chat_ids)


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    if not _is_allowed(update, cfg):
        return
    await update.effective_message.reply_text("pong")


async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    if not _is_allowed(update, cfg):
        return
    st: UniverseState = context.application.bot_data["universe_state"]
    preview = ", ".join(st.sample_active[:15])
    if st.sample_active and len(st.sample_active) > 15:
        preview += " ..."
    text = (
        f"Вселенная: total={st.total}, active={st.active}, batch#{st.batch}, ws_topics={st.ws_topics}"
    )
    if preview:
        text += f"\nАктивные (пример): {preview}"
    await update.effective_message.reply_text(text)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_universe(update, context)


def _fmt_dt(dt) -> str:
    try:
        return dt.isoformat(sep=" ", timespec="seconds")
    except Exception:
        return str(dt)


async def cmd_jobs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    if not _is_allowed(update, cfg):
        return
    jobs: Dict[str, Any] = context.application.bot_data.get("jobs", {})
    lines = ["Задачи:"]
    for name, job in jobs.items():
        nrt = getattr(job, "next_run_time", None)
        lines.append(f"• {name}: next={_fmt_dt(nrt) if nrt else '—'}")
    await update.effective_message.reply_text("\n".join(lines))


async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    if not _is_allowed(update, cfg):
        return
    st: UniverseState = context.application.bot_data["universe_state"]
    text = (
        "DEBUG:\n"
        f"ONLY_CHANNEL={cfg.only_channel}\n"
        f"PRIMARY_RECIPIENTS={cfg.primary_recipients}\n"
        f"ALLOWED_CHAT_IDS={cfg.allowed_chat_ids}\n"
        f"SCAN_INTERVAL_SEC={cfg.scan_interval_sec}\n"
        f"HEARTBEAT_SEC={cfg.heartbeat_sec}\n"
        f"SELF_PING={cfg.self_ping} KEEPALIVE_SEC={cfg.self_ping_interval_sec}\n"
        f"ANALYSIS_ENABLED={cfg.analysis_enabled} BATCH_SIZE={cfg.analysis_batch_size}\n"
        f"SIGNAL_TTL_MIN={cfg.signal_ttl_min} SIGNAL_COOLDOWN_SEC={cfg.signal_cooldown_sec}\n"
        f"VOL_SMA_PERIOD={cfg.vol_sma_period} VOL_MULT={cfg.vol_mult} ATR_PERIOD={cfg.atr_period} BODY_ATR_MULT={cfg.body_atr_mult}\n"
        f"LIQ_REVERSAL_ENABLED={cfg.liq_enable_reversal} LIQ_EVENTS_MIN={cfg.liq_events_min} LIQ_NOTIONAL_MIN={cfg.liq_notional_min} "
        f"LIQ_REVERSAL_LOOKBACK={cfg.liq_reversal_lookback} LIQ_VOL_MULT_MIN={cfg.liq_vol_mult_min}\n"
        f"PUBLIC_URL={cfg.public_url or '-'} PORT={cfg.port}\n"
        f"universe total={st.total} active={st.active} ws_topics={st.ws_topics} batch#{st.batch}\n"
    )
    await update.effective_message.reply_text(text)


# -----------------------------------------------------------------------------
# ТЁПЛЫЙ СТАРТ / ПЛАНИРОВАНИЕ
# -----------------------------------------------------------------------------
async def _warmup_and_schedule(app: Application, cfg: Config) -> None:
    await asyncio.sleep(cfg.startup_delay_sec)

    try:
        await build_universe(app, cfg)
    except Exception:
        logger.exception("warmup: universe init failed")

    try:
        jq = app.job_queue

        # перенос ограничителей в job_kwargs (совместимо с PTB 21.x)
        job_scan_obj = jq.run_repeating(
            job_scan,
            interval=cfg.scan_interval_sec,
            first=cfg.first_scan_delay_sec,
            name="job_scan",
            job_kwargs={"misfire_grace_time": 30, "coalesce": True},
        )
        job_hb_obj = jq.run_repeating(
            job_heartbeat,
            interval=cfg.heartbeat_sec,
            first=120,
            name="job_heartbeat",
            job_kwargs={"misfire_grace_time": 120, "coalesce": True},
        )

        job_ka_obj = None
        if cfg.self_ping:
            job_ka_obj = jq.run_repeating(
                job_keepalive,
                interval=cfg.self_ping_interval_sec,
                first=cfg.self_ping_interval_sec,
                name="job_keepalive",
                job_kwargs={"misfire_grace_time": cfg.self_ping_interval_sec, "coalesce": True},
            )

        jobs_map: Dict[str, Any] = {
            "scan": job_scan_obj,
            "heartbeat": job_hb_obj,
        }
        if job_ka_obj:
            jobs_map["keepalive"] = job_ka_obj

        app.bot_data["jobs"] = jobs_map
    except Exception:
        logger.exception("warmup: scheduling failed")

    try:
        await job_heartbeat_simple(app)
    except Exception:
        logger.exception("warmup: first heartbeat failed")


# -----------------------------------------------------------------------------
# MAIN — явный lifecycle
# -----------------------------------------------------------------------------
async def main_async() -> None:
    cfg = Config.load()

    # 1) health-сервер для быстрого аптайма на Render
    try:
        asyncio.create_task(start_health_server(cfg.port))
    except Exception:
        logger.exception("health server failed to start")

    # 2) Telegram app
    application = Application.builder().token(cfg.telegram_token).build()
    application.bot_data["cfg"] = cfg
    application.bot_data["universe_state"] = UniverseState()
    application.bot_data["bybit"] = BybitClient(cfg.bybit_base)

    chan_ids = [cid for cid in cfg.primary_recipients if isinstance(cid, int) and cid < 0]
    logger.info(
        "[cfg] ONLY_CHANNEL=%s PRIMARY_RECIPIENTS=%s ALLOWED_CHAT_IDS=%s PORT=%s",
        cfg.only_channel, cfg.primary_recipients, cfg.allowed_chat_ids, cfg.port
    )
    if cfg.only_channel and not chan_ids:
        logger.warning(
            "[cfg] ONLY_CHANNEL=1, но среди PRIMARY_RECIPIENTS нет ID канала (отрицательного chat_id)."
        )

    # 3) Хэндлеры
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("universe", cmd_universe))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("jobs", cmd_jobs))
    application.add_handler(CommandHandler("debug", cmd_debug))
    application.add_handler(CommandHandler("diag", cmd_debug))
    application.add_handler(CommandHandler("diagnostics", cmd_debug))

    # 4) Удаляем вебхук перед polling
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        logger.exception("delete_webhook failed")

    try:
        # --- ИНИЦИАЛИЗАЦИЯ/СТАРТ ---
        await application.initialize()
        await application.start()

        # тёплый старт и задания запускаем ПОСЛЕ старта приложения
        application.create_task(_warmup_and_schedule(application, cfg), name="warmup")

        # --- ЗАПУСК POLLING ---
        try:
            await application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
            )
        except Conflict:
            logger.error("Another instance is polling (Conflict). Exiting this one.")
            return

        # --- ОЖИДАНИЕ ---
        stop_forever = asyncio.Event()
        await stop_forever.wait()

    finally:
        # --- КОРРЕКТНАЯ ОСТАНОВКА ---
        try:
            if application.updater:
                await application.updater.stop()
        except Exception:
            pass
        try:
            await application.stop()
        except Exception:
            pass
        try:
            await application.shutdown()
        except Exception:
            pass
        # Закрываем Bybit-сессию
        try:
            client: BybitClient = application.bot_data["bybit"]
            await client.close()
        except Exception:
            pass


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
