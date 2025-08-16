# cryptobot_telegram_bot.py
# -*- coding: utf-8 -*-

import os
import asyncio
import json
import math
import random
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set, Optional, Tuple, Deque
from collections import deque, defaultdict

import aiohttp
import httpx

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# =========================
# Конфигурация из ENV
# =========================

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v is not None else default
    except:
        return default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except:
        return default

def _parse_id_list(s: Optional[str]) -> List[int]:
    if not s:
        return []
    out = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except:
            # поддержка вида -10012345 (как строка)
            if part.startswith("-") and part[1:].isdigit():
                out.append(int(part))
    return out

@dataclass
class Config:
    token: str
    recipients: List[int]
    allowed_chat_ids: List[int]
    port: int
    public_url: str
    webhook_path: str

    # режимы/ротация
    universe_mode: str = os.getenv("UNIVERSE_MODE", "all").lower()  # all|topn
    ws_symbols_max: int = _env_int("WS_SYMBOLS_MAX", 60)
    universe_rotate_min: int = _env_int("UNIVERSE_ROTATE_MIN", 5)
    universe_top_n: int = _env_int("UNIVERSE_TOP_N", 15)

    # health & keepalive
    health_interval_sec: int = _env_int("HEALTH_INTERVAL_SEC", 1200)  # 20m
    first_health_sec: int = _env_int("FIRST_HEALTH_SEC", 60)
    startup_notice_sec: int = _env_int("STARTUP_NOTICE_SEC", 10)
    self_ping_enabled: bool = _env_bool("SELF_PING_ENABLED", True)
    self_ping_interval_sec: int = _env_int("SELF_PING_INTERVAL_SEC", 780)  # 13m

    # фильтры сигналов
    prob_min: float = _env_float("PROB_MIN", 69.9)
    rr_min: float = _env_float("RR_MIN", 2.0)
    profit_min_pct: float = _env_float("PROFIT_MIN_PCT", 1.0)
    signal_cooldown_sec: int = _env_int("SIGNAL_COOLDOWN_SEC", 600)  # не спамить чаще 10м
    signal_ttl_min: int = _env_int("SIGNAL_TTL_MIN", 12)

    # параметры триггеров (можно править через ENV)
    vol_mult: float = _env_float("VOL_MULT", 2.0)
    vol_sma_period: int = _env_int("VOL_SMA_PERIOD", 20)
    body_atr_mult: float = _env_float("BODY_ATR_MULT", 0.6)
    atr_period: int = _env_int("ATR_PERIOD", 14)

def load_config() -> Config:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    # получатель по умолчанию — TELEGRAM_CHAT_ID
    chat_raw = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    recipients = []
    if chat_raw:
        try:
            recipients.append(int(chat_raw))
        except:
            pass

    allowed = _parse_id_list(os.getenv("ALLOWED_CHAT_IDS"))
    # чтобы /ping работал хотя бы у владельца канала
    for rid in recipients:
        if rid not in allowed:
            allowed.append(rid)

    port = _env_int("PORT", 10000)
    public_url = os.getenv("PUBLIC_URL", "").strip()
    if not public_url:
        # Render обычно подставляет RENDER_EXTERNAL_URL, но мы позволим указать PUBLIC_URL
        public_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if not public_url:
        raise RuntimeError("PUBLIC_URL/RENDER_EXTERNAL_URL is required (https://<your-service>.onrender.com)")

    # Уникальный путь вебхука
    rnd = random.randint(10_000_000, 99_999_999)
    webhook_path = os.getenv("WEBHOOK_PATH", f"/wh-{rnd}").strip()

    cfg = Config(
        token=token,
        recipients=recipients,
        allowed_chat_ids=allowed,
        port=port,
        public_url=public_url,
        webhook_path=webhook_path,
    )
    logging.info("INFO [cfg] ALLOWED_CHAT_IDS=%s", allowed)
    logging.info("INFO [cfg] PRIMARY_RECIPIENTS=%s", recipients)
    logging.info("INFO [cfg] PUBLIC_URL='%s' PORT=%d WEBHOOK_PATH='%s'", public_url, port, webhook_path)
    logging.info("INFO [cfg] HEALTH=%ds FIRST=%ds STARTUP=%ds SELF_PING=%s/%ds",
                 cfg.health_interval_sec, cfg.first_health_sec, cfg.startup_notice_sec,
                 "True" if cfg.self_ping_enabled else "False", cfg.self_ping_interval_sec)
    logging.info("INFO [cfg] SIGNAL_COOLDOWN_SEC=%d SIGNAL_TTL_MIN=%d UNIVERSE_MODE=%s "
                 "UNIVERSE_TOP_N=%d WS_SYMBOLS_MAX=%d ROTATE_MIN=%d PROB_MIN>%.1f PROFIT_MIN_PCT>=%.1f%% RR_MIN>=%.2f",
                 cfg.signal_cooldown_sec, cfg.signal_ttl_min, cfg.universe_mode,
                 cfg.universe_top_n, cfg.ws_symbols_max, cfg.universe_rotate_min,
                 cfg.prob_min, cfg.profit_min_pct, cfg.rr_min)
    logging.info("INFO [cfg] Trigger params: VOL_MULT=%.2f, VOL_SMA_PERIOD=%d, BODY_ATR_MULT=%.2f, ATR_PERIOD=%d",
                 cfg.vol_mult, cfg.vol_sma_period, cfg.body_atr_mult, cfg.atr_period)
    return cfg

# =========================
# Bybit API Клиент
# =========================

BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_LINEAR = "wss://stream.bybit.com/v5/public/linear"

class BybitClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def _get(self, path: str, params: Dict[str, str]) -> Dict:
        url = BYBIT_REST + path
        for _ in range(3):
            try:
                async with self.session.get(url, params=params, timeout=15) as resp:
                    data = await resp.json()
                    return data
            except Exception:
                await asyncio.sleep(0.5)
        raise RuntimeError(f"Bybit GET failed: {path} {params}")

    async def get_instruments_linear_usdt(self) -> List[str]:
        # Получаем все линейные деривативы и фильтруем по USDT + Trading
        res = await self._get("/v5/market/instruments-info", {
            "category": "linear",
            "status": "Trading"
        })
        if res.get("retCode") != 0:
            raise RuntimeError(f"instruments-info error: {res}")
        out = []
        for it in res["result"]["list"]:
            if it.get("quoteCoin") == "USDT":
                # Некоторые могут быть опциональными типами, проверим contractType
                if str(it.get("contractType", "")).lower().startswith("linear"):
                    out.append(it["symbol"])
        return sorted(out)

    async def get_tickers_linear(self) -> Dict[str, Dict]:
        # 24h оборот/изменение — для ранжирования
        res = await self._get("/v5/market/tickers", {"category": "linear"})
        if res.get("retCode") != 0:
            raise RuntimeError(f"tickers error: {res}")
        out = {}
        for it in res["result"]["list"]:
            sym = it["symbol"]
            out[sym] = it
        return out

    async def get_open_interest(self, symbol: str, interval="5min", limit=4) -> Optional[float]:
        # Вернем последнее OI (в контрактах). Используем разницу для тренда.
        res = await self._get("/v5/market/open-interest", {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": str(limit)
        })
        if res.get("retCode") != 0:
            raise RuntimeError(f"open-interest error: {res}")
        lst = res["result"].get("list", [])
        if not lst:
            return None
        # Формат: [{ "openInterest": "12345", "timestamp": "..."} ...]
        try:
            last = float(lst[-1]["openInterest"])
        except Exception:
            return None
        return last

# =========================
# Вспомогательные функции TA
# =========================

def atr_from_bars(bars: Deque[Tuple[float, float, float, float]], period: int) -> Optional[float]:
    """ bars: deque of (o,h,l,c), последний — свежий """
    if len(bars) < period + 1:
        return None
    trs = []
    prev_close = bars[-period-1][3]
    for i in range(len(bars) - period, len(bars)):
        o,h,l,c = bars[i]
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = c
    if not trs:
        return None
    return sum(trs) / len(trs)

def sma(seq: Deque[float], period: int) -> Optional[float]:
    if len(seq) < period:
        return None
    return sum(list(seq)[-period:]) / period

def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0

# =========================
# Торговый движок (скрининг + сигналы)
# =========================

class TradeEngine:
    def __init__(self, cfg: Config, bybit: BybitClient, bot_send_callable):
        self.cfg = cfg
        self.bybit = bybit
        self.bot_send = bot_send_callable

        # Вселенная
        self.all_symbols: List[str] = []
        self.rotation_order: List[str] = []
        self.active_symbols: Set[str] = set()
        self.rot_idx: int = 0

        # Маркеты
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None

        # Данные по символам
        self.bars: Dict[str, Deque[Tuple[float,float,float,float]]] = defaultdict(lambda: deque(maxlen=300))  # (o,h,l,c)
        self.vols: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=300))
        self.last_price: Dict[str, float] = {}
        self.last_oi: Dict[str, float] = {}
        self.liq_5m: Dict[str, float] = defaultdict(float)  # простая агрегация ликвидаций (USD) последних ~5м
        self.liq_decay: Dict[str, float] = defaultdict(float)

        # Антиспам
        self.last_signal_ts: Dict[str, float] = {}

        # Статистика
        self.ws_topics_count: int = 0

        # Флаг живости WS
        self._ws_lock = asyncio.Lock()

    # -------- Вселенная / Ротация --------

    async def bootstrap_universe(self):
        # 1) Все линейные USDT-перпы
        all_syms = await self.bybit.get_instruments_linear_usdt()
        self.all_symbols = all_syms

        # 2) Ранжирование по 24h turnover
        tickers = await self.bybit.get_tickers_linear()
        ranked = sorted(
            [s for s in all_syms if s in tickers],
            key=lambda s: float(tickers[s].get("turnover24h", "0") or "0"),
            reverse=True
        )
        # сохраняем порядок
        self.rotation_order = ranked if ranked else all_syms[:]
        if self.cfg.universe_mode == "topn":
            # без ротации — фиксированный набор top N
            self.active_symbols = set(self.rotation_order[:self.cfg.universe_top_n])
        else:
            # режим all — активный батч №0
            self._apply_rotation_batch(0)
        logging.info("INFO [universe] total=%d active=%d mode=%s",
                     len(self.rotation_order), len(self.active_symbols), self.cfg.universe_mode)

    def _apply_rotation_batch(self, batch_index: int):
        if not self.rotation_order:
            self.active_symbols = set()
            return
        n = self.cfg.ws_symbols_max
        chunks = math.ceil(len(self.rotation_order) / n)
        idx = batch_index % max(chunks, 1)
        start = idx * n
        end = min(len(self.rotation_order), start + n)
        batch = self.rotation_order[start:end]
        self.active_symbols = set(batch)
        self.rot_idx = idx

    async def rotate_batch(self):
        if self.cfg.universe_mode != "all":
            return  # в topn не крутим
        next_idx = self.rot_idx + 1
        self._apply_rotation_batch(next_idx)
        # перестроить подписки WS
        await self._ws_resubscribe()
        logging.info("INFO [rotate] idx=%d active=%d", self.rot_idx, len(self.active_symbols))

    # -------- WS управление --------

    def _topics_for(self, symbols: Set[str]) -> List[str]:
        topics = []
        for s in symbols:
            topics.append(f"kline.1.{s}")
            topics.append(f"liquidation.{s}")
        return topics

    async def ensure_ws(self):
        async with self._ws_lock:
            if self.ws and not self.ws.closed:
                return
            # новое соединение
            self.ws = await self.bybit.session.ws_connect(BYBIT_WS_LINEAR, heartbeat=20, timeout=20)
            await self._ws_resubscribe(first=True)
            asyncio.create_task(self._ws_reader())

    async def _ws_resubscribe(self, first: bool=False):
        async with self._ws_lock:
            if not self.ws or self.ws.closed:
                return
            # отписка от всего, если не первый раз
            if not first:
                try:
                    await self.ws.send_json({"op": "unsubscribe", "args": ["*"]})
                except Exception:
                    pass
            topics = self._topics_for(self.active_symbols)
            # подписываемся порциями, чтобы не упираться в лимит на пакет
            batch = 20
            sent = 0
            for i in range(0, len(topics), batch):
                args = topics[i:i+batch]
                await self.ws.send_json({"op": "subscribe", "args": args})
                sent += len(args)
                await asyncio.sleep(0.05)
            self.ws_topics_count = sent
            logging.info("INFO [ws] subscribed %d topics for %d symbols", sent, len(self.active_symbols))

    async def _ws_reader(self):
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._ws_handle_text(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        except Exception as e:
            logging.warning("WARN [ws] reader error: %s", e)
        finally:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
            logging.warning("WARN [ws] closed; will reconnect on next tick")

    async def _ws_handle_text(self, data: str):
        try:
            js = json.loads(data)
        except Exception:
            return
        # ответы на подписку/пинг — игнор
        if "topic" not in js:
            return
        topic = js["topic"]
        if topic.startswith("kline.1."):
            sym = topic.split(".")[-1]
            await self._on_kline(sym, js)
        elif topic.startswith("liquidation."):
            sym = topic.split(".")[-1]
            await self._on_liq(sym, js)

    async def _on_kline(self, sym: str, js: Dict):
        """
        Bybit v5 kline payload sample:
        {
          "topic":"kline.1.BTCUSDT",
          "data":[{"start":..., "open":"...", "high":"...", "low":"...", "close":"...", "volume":"...","confirm":true, ...}],
          "ts":... }
        """
        arr = js.get("data") or []
        if not arr:
            return
        k = arr[-1]
        try:
            o = float(k["open"]); h = float(k["high"]); l = float(k["low"])
            c = float(k["close"]); v = float(k["volume"])
        except Exception:
            return
        self.bars[sym].append((o,h,l,c))
        self.vols[sym].append(v)
        self.last_price[sym] = c

        # экспоненциальное «затухание» агрегата ликвидаций (очищаем старыe через время)
        self.liq_5m[sym] *= 0.96

        # попытка анализа только по закрытому бару
        if k.get("confirm"):
            await self._maybe_signal(sym)

    async def _on_liq(self, sym: str, js: Dict):
        """
        Bybit v5 liquidation sample:
        {
          "topic":"liquidation.BTCUSDT",
          "data":[{"updatedTime":"...", "symbol":"BTCUSDT","side":"Buy"|"Sell",
                   "price":"...", "qty":"...", "value":"..."}], "ts":... }
        """
        arr = js.get("data") or []
        if not arr:
            return
        for it in arr:
            try:
                val = float(it.get("value") or "0")  # в USDT
                side = str(it.get("side", ""))
            except Exception:
                continue
            # грубо: суммируем ликвидации, знак используем как прокси-направление импульса
            sgn = -1.0 if side == "Buy" else 1.0  # long liq обычно на падении → возможный отскок вверх
            self.liq_5m[sym] += sgn * val

    # -------- OI polling --------

    async def poll_oi_once(self):
        if not self.active_symbols:
            return
        # ограничим нагрузку: не больше 40 запросов подряд
        symbols = list(self.active_symbols)
        batch = 40
        for i in range(0, len(symbols), batch):
            chunk = symbols[i:i+batch]
            tasks = [self.bybit.get_open_interest(s, interval="5min", limit=4) for s in chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for s, r in zip(chunk, results):
                if isinstance(r, Exception) or r is None:
                    continue
                self.last_oi[s] = r
            await asyncio.sleep(0.2)

    # -------- Аналитика / Сигналы --------

    def _cooldown_ok(self, sym: str) -> bool:
        ts = self.last_signal_ts.get(sym, 0)
        return (time.time() - ts) >= self.cfg.signal_cooldown_sec

    async def _maybe_signal(self, sym: str):
        # необходимые данные
        bars = self.bars[sym]
        vols = self.vols[sym]
        price = self.last_price.get(sym)
        if price is None:
            return

        # ATR и телобары
        atr = atr_from_bars(bars, self.cfg.atr_period)
        if atr is None or atr <= 0:
            return
        o,h,l,c = bars[-1]
        body = abs(c - o)

        # Volume spike
        v_sma = sma(vols, self.cfg.vol_sma_period)
        vol_ok = (v_sma is not None) and (vols[-1] >= self.cfg.vol_mult * v_sma)

        # Momentum по телу к ATR
        mom_ok = (body >= self.cfg.body_atr_mult * atr)
        mom_dir = 1 if c > o else -1

        # OI дельта (грубая оценка; просто сравним с предыдущим сохранённым)
        oi_now = self.last_oi.get(sym)
        oi_prev = None  # ищем второй срез в прошлом deque? упростим — используем последнее запомненное при прошлых обходах
        # Для эвристики возьмём «наклон» по цене: если цена ↑ и OI ↑ → лонг; цена ↑ и OI ↓ → вынос шортов и возможный разворот.
        oi_signal = 0.0
        if oi_now is not None:
            # сохраним в цикле poll_oi_once; тут только направленная эвристика от последнего известного
            oi_prev = oi_now  # нет истории — дадим нейтраль
            # оставим neutral, чтобы не шуметь без истории

        # Ликвидации (накопленные): положительные => в сумме «Sell» (short liq) преобладали → импульс вверх; отрицательные — вниз
        liq_bias = 0.0
        L = self.liq_5m.get(sym, 0.0)
        if abs(L) > 0:
            liq_bias = 1.0 if L > 0 else -1.0

        # Итоговый скоринг
        score = 0.0
        if vol_ok:
            score += 20.0
        if mom_ok:
            score += 25.0 * mom_dir  # направление движения телом

        # Добавим вклад ликвидаций как импульс
        score += 10.0 * liq_bias

        # слабый вклад OI (нейтральный без истории)
        # (оставлено как 0.0)

        # небольшая нормализация и ограничение
        score = max(-100.0, min(100.0, score))

        # Вероятность (эмпирическое сопоставление)
        probability = 50.0 + score * 0.4  # [-100..100] → [10..90]
        probability = max(1.0, min(99.0, probability))

        # Направление
        direction = "long" if score >= 0 else "short"

        # Генерация торгового плана
        # Риск — 0.5*ATR; стоп — по ту сторону бара; тейк — из условия RR >= rr_min
        risk_abs = 0.5 * atr
        if risk_abs <= 0:
            return

        entry = price
        if direction == "long":
            sl = entry - max(risk_abs, 0.3 * atr)
            tp = entry + max(self.cfg.rr_min * (entry - sl), atr)  # чтобы RR >= rr_min
        else:
            sl = entry + max(risk_abs, 0.3 * atr)
            tp = entry - max(self.cfg.rr_min * (sl - entry), atr)

        # Проверим RR и прибыль в %
        rr = abs((tp - entry) / (entry - sl)) if (entry != sl) else 0.0
        profit_pct = abs(pct(tp, entry))

        if probability <= self.cfg.prob_min:
            return
        if rr < self.cfg.rr_min:
            return
        if profit_pct < self.cfg.profit_min_pct:
            return
        if not self._cooldown_ok(sym):
            return

        # готовим текст
        arrow = "🟢 LONG" if direction == "long" else "🔴 SHORT"
        sign = "+" if (tp - entry) * (1 if direction == "long" else -1) > 0 else ""
        msg = (
            f"{arrow} <b>{sym}</b>\n"
            f"Цена: <code>{entry:.6g}</code>\n"
            f"Вход: <code>{entry:.6g}</code>\n"
            f"Тейк: <code>{tp:.6g}</code> ({sign}{profit_pct:.2f}%)\n"
            f"Стоп: <code>{sl:.6g}</code> ({pct(sl, entry):.2f}%)\n"
            f"R/R: <b>{rr:.2f}</b>\n"
            f"Вероятность: <b>{probability:.1f}%</b>\n"
        )

        # отправим
        await self.bot_send(msg)
        self.last_signal_ts[sym] = time.time()

    # -------- Публичные задачи (JobQueue) --------

    async def job_start_ws(self, ctx: ContextTypes.DEFAULT_TYPE):
        await self.ensure_ws()

    async def job_rotate(self, ctx: ContextTypes.DEFAULT_TYPE):
        await self.rotate_batch()

    async def job_poll_oi(self, ctx: ContextTypes.DEFAULT_TYPE):
        await self.poll_oi_once()

# =========================
# Утилиты: Telegram и сервисные задачи
# =========================

async def send_to_recipients(application: Application, recipients: List[int], text: str):
    for chat_id in recipients:
        try:
            await application.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except httpx.ReadError:
            logging.warning("WARN send->%s: httpx.ReadError", chat_id)
        except Exception as e:
            logging.warning("WARN send->%s: %s", chat_id, e)

# health/check
async def health_job(app: Application, recipients: List[int]):
    await send_to_recipients(app, recipients, "🟢 online")

# self-ping для бесплатного Render
async def self_ping_job(url: str):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=10) as r:
                _ = await r.text()
    except Exception:
        pass

# =========================
# Команды бота
# =========================

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cfg: Config = context.application.user_data.get("cfg")
    if chat_id not in cfg.allowed_chat_ids:
        return
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    await update.effective_message.reply_text(f"pong — {now}")

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cfg: Config = context.application.user_data.get("cfg")
    if chat_id not in cfg.allowed_chat_ids:
        return
    eng: TradeEngine = context.application.user_data.get("engine")
    total = len(eng.rotation_order)
    active = len(eng.active_symbols)
    idx = eng.rot_idx
    await update.effective_message.reply_text(
        f"Вселенная: total={total}, active={active}, batch#{idx}, ws_topics={eng.ws_topics_count}"
    )

# =========================
# MAIN
# =========================

async def post_init(app: Application):
    # Установим вебхук
    cfg: Config = app.user_data["cfg"]

    # Сначала почистим
    await app.bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(0.1)
    await app.bot.set_webhook(url=cfg.public_url.rstrip("/") + cfg.webhook_path)
    logging.info("INFO webhook set: %s", cfg.public_url.rstrip("/") + cfg.webhook_path)

    # Сообщение о запуске
    await send_to_recipients(app, cfg.recipients, f"✅ Render Web Service: бот запущен. Mode={cfg.universe_mode}, rotation={cfg.universe_rotate_min}m, WS={cfg.ws_symbols_max}")

def build_application(cfg: Config) -> Application:
    application = ApplicationBuilder().token(cfg.token).post_init(post_init).build()
    application.user_data["cfg"] = cfg

    # Команды
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("universe", cmd_universe))

    return application

async def main_async():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    cfg = load_config()

    app = build_application(cfg)

    # HTTP session для Bybit
    aio_sess = aiohttp.ClientSession()
    bybit = BybitClient(aio_sess)

    # отправитель в канал
    async def bot_send(msg: str):
        await send_to_recipients(app, cfg.recipients, msg)

    # Движок
    engine = TradeEngine(cfg, bybit, bot_send)
    app.user_data["engine"] = engine

    # Bootstrap вселенной до старта вебхука
    try:
        await engine.bootstrap_universe()
    except Exception as e:
        logging.exception("bootstrap_universe failed: %s", e)
        # все равно продолжим, но активных символов может не быть

    # Планировщик задач
    jq = app.job_queue

    # WS запуск — сразу после старта приложения
    jq.run_once(engine.job_start_ws, when=1)

    # Ротация (только для all)
    if cfg.universe_mode == "all":
        jq.run_repeating(engine.job_rotate, first=cfg.universe_rotate_min * 60, interval=cfg.universe_rotate_min * 60)

    # OI poll
    jq.run_repeating(engine.job_poll_oi, first=10, interval=60)

    # health ping в канал
    jq.run_repeating(lambda c: health_job(app, cfg.recipients), first=cfg.first_health_sec, interval=cfg.health_interval_sec)

    # self-ping
    if cfg.self_ping_enabled and cfg.public_url:
        jq.run_repeating(lambda c: self_ping_job(cfg.public_url), first=30, interval=cfg.self_ping_interval_sec)

    # Запускаем webhook-сервер PTB (tornado)
    app.run_webhook(
        listen="0.0.0.0",
        port=cfg.port,
        url_path=cfg.webhook_path,
        stop_signals=None,  # безопаснее для контейнеров
        allowed_updates=Update.ALL_TYPES
    )

def main():
    try:
        asyncio.run(main_async())
    except RuntimeError as e:
        # Подстраховка от "event loop already running" на некоторых платформах —
        # в PTB run_webhook закрывает цикл сам; если вдруг он уже активен, fallback.
        logging.warning("RuntimeError(main_async): %s — fallback to direct call", e)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main_async())

if __name__ == "__main__":
    main()
