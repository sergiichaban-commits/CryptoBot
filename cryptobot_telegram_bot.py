import os
import asyncio
import logging
import math
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple, Set

import aiohttp
from telegram import Update
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler,
    ContextTypes
)

# --------------------------- Logging ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("cryptobot")

# --------------------------- Config ---------------------------

def _parse_ids(val: str) -> List[int]:
    if not val:
        return []
    parts = [p.strip() for p in val.split(",") if p.strip()]
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except ValueError:
            pass
    return out

@dataclass
class Config:
    token: str
    allowed_chat_ids: Set[int]
    primary_recipients: List[int]

    # universe / rotation
    active_symbols: int = 30        # сколько монет сканируем в «активной» корзине
    rotate_min: int = 5             # каждые N минут смещаем окно
    scan_interval_sec: int = 12     # как часто сканим активную корзину
    heartbeat_sec: int = 900        # «онлайн»-пинг в канал

    # триггеры
    vol_mult: float = 2.0
    vol_sma_period: int = 20
    atr_period: int = 14
    body_atr_mult: float = 0.6

    # фильтры сигналов
    profit_min_pct: float = 1.0     # минимум профит-таргета в %
    rr_min: float = 2.0
    prob_min: float = 0.70          # 0..1

    @staticmethod
    def load() -> "Config":
        token = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN is required")

        allowed = set(_parse_ids(os.getenv("ALLOWED_CHAT_IDS", "")))
        recipients = _parse_ids(os.getenv("PRIMARY_RECIPIENTS", ""))
        if not recipients:
            raise RuntimeError("PRIMARY_RECIPIENTS must contain at least one channel id")

        cfg = Config(
            token=token,
            allowed_chat_ids=allowed,
            primary_recipients=recipients,
            active_symbols=int(os.getenv("ACTIVE_SYMBOLS", "30")),
            rotate_min=int(os.getenv("ROTATE_MIN", "5")),
            scan_interval_sec=int(os.getenv("SCAN_INTERVAL_SEC", "12")),
            heartbeat_sec=int(os.getenv("HEARTBEAT_SEC", "900")),
            vol_mult=float(os.getenv("VOL_MULT", "2.0")),
            vol_sma_period=int(os.getenv("VOL_SMA_PERIOD", "20")),
            atr_period=int(os.getenv("ATR_PERIOD", "14")),
            body_atr_mult=float(os.getenv("BODY_ATR_MULT", "0.6")),
            profit_min_pct=float(os.getenv("PROFIT_MIN_PCT", "1.0")),
            rr_min=float(os.getenv("RR_MIN", "2.0")),
            prob_min=float(os.getenv("PROB_MIN", "0.70")),
        )
        log.info(
            "CFG loaded: PRIMARY_RECIPIENTS=%s ALLOWED=%s ACTIVE_SYMBOLS=%d ROTATE_MIN=%d "
            "SCAN=%ds HEARTBEAT=%ds RR_MIN=%.2f PROFIT_MIN=%.2f%% PROB_MIN=%.2f",
            cfg.primary_recipients, list(cfg.allowed_chat_ids), cfg.active_symbols,
            cfg.rotate_min, cfg.scan_interval_sec, cfg.heartbeat_sec,
            cfg.rr_min, cfg.profit_min_pct, cfg.prob_min,
        )
        return cfg

# --------------------------- Bybit REST client ---------------------------

class BybitClient:
    BASE = "https://api.bybit.com"

    def __init__(self, session: aiohttp.ClientSession):
        self.sess = session

    async def get_instruments_linear_usdt(self) -> List[str]:
        """Все USDT линейные perpetual/linear контракты со статусом Trading."""
        url = f"{self.BASE}/v5/market/instruments-info"
        params = {"category": "linear", "limit": "1000"}
        symbols: List[str] = []
        cursor: Optional[str] = None

        while True:
            p = dict(params)
            if cursor:
                p["cursor"] = cursor
            async with self.sess.get(url, params=p, timeout=15) as r:
                r.raise_for_status()
                data = await r.json()
            if data.get("retCode") != 0:
                raise RuntimeError(f"Bybit instruments error: {data}")
            res = data["result"]
            for it in res.get("list", []):
                if it.get("status") == "Trading" and it.get("quoteCoin") == "USDT":
                    # фильтруем perpetual/futures линейные
                    sym = it.get("symbol")
                    if sym:
                        symbols.append(sym)
            cursor = res.get("nextPageCursor") or ""
            if not cursor:
                break

        # dedup & sort
        symbols = sorted(list(dict.fromkeys(symbols)))
        return symbols

    async def get_klines_5m(self, symbol: str, limit: int = 120) -> List[Dict[str, float]]:
        url = f"{self.BASE}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": "5", "limit": str(limit)}
        async with self.sess.get(url, params=params, timeout=15) as r:
            r.raise_for_status()
            data = await r.json()
        if data.get("retCode") != 0:
            raise RuntimeError(f"kline error {symbol}: {data}")
        out: List[Dict[str, float]] = []
        for row in data["result"]["list"]:
            # Bybit: [start, open, high, low, close, volume, turnover]
            o, h, l, c, v = float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])
            out.append({"o": o, "h": h, "l": l, "c": c, "v": v})
        out.reverse()  # к последней свече в конце -> в финале последняя
        return out

    async def get_open_interest_5m(self, symbol: str, limit: int = 6) -> List[float]:
        url = f"{self.BASE}/v5/market/open-interest"
        params = {"category": "linear", "symbol": symbol, "intervalTime": "5min", "limit": str(limit)}
        async with self.sess.get(url, params=params, timeout=15) as r:
            r.raise_for_status()
            data = await r.json()
        if data.get("retCode") != 0:
            raise RuntimeError(f"OI error {symbol}: {data}")
        vals: List[float] = []
        for row in data["result"]["list"]:
            # row: [timestamp, openInterest, turnover, ...] — специфика: берем openInterest
            try:
                vals.append(float(row[1]))
            except Exception:
                continue
        vals.reverse()
        return vals

# --------------------------- Indicators & Signals ---------------------------

def sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period

def atr(kl: List[Dict[str, float]], period: int) -> Optional[float]:
    if len(kl) < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, len(kl)):
        h, l, c_prev = kl[i]["h"], kl[i]["l"], kl[i-1]["c"]
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period

def volume_spike(kl: List[Dict[str, float]], period: int, mult: float) -> bool:
    if len(kl) < period + 1:
        return False
    vols = [x["v"] for x in kl]
    avg = sma(vols[:-1], period)
    if not avg or avg <= 0:
        return False
    return vols[-1] >= avg * mult

def estimate_probability(
    long: bool, vol_ok: bool, body_vs_atr: float, oi_delta: float
) -> float:
    # базовая вероятностная модель — варьируется по факторам, чтобы НЕ было одинаковых 75% постоянно
    prob = 0.58
    if vol_ok:
        prob += 0.08
    # чем больше тело свечи относительно ATR — тем выше доверие
    prob += min(0.12, max(0.0, (body_vs_atr - 0.4) * 0.15))
    # подтверждение OI в сторону сигнала
    if (long and oi_delta > 0) or ((not long) and oi_delta < 0):
        prob += min(0.10, abs(oi_delta) * 0.25)
    # лёгкая «рандомизация» внутри диапазона на основе синуса последней цены
    drift = (math.sin(body_vs_atr * 3.1415) + 1) * 0.02  # 0..0.04
    prob += drift
    return max(0.50, min(0.88, prob))

def make_signal(
    symbol: str,
    kl: List[Dict[str, float]],
    oi: List[float],
    cfg: Config
) -> Optional[Dict[str, Any]]:
    if len(kl) < max(cfg.vol_sma_period + 1, cfg.atr_period + 1):
        return None

    last = kl[-1]
    c, o, h, l, v = last["c"], last["o"], last["h"], last["l"], last["v"]
    closes = [x["c"] for x in kl]
    highs  = [x["h"] for x in kl]
    lows   = [x["l"] for x in kl]
    vols   = [x["v"] for x in kl]

    sma20 = sma(closes, cfg.vol_sma_period)
    atr14 = atr(kl, cfg.atr_period)
    if not sma20 or not atr14 or atr14 <= 0:
        return None

    body = abs(c - o)
    body_vs_atr = body / atr14
    vol_ok = volume_spike(kl, cfg.vol_sma_period, cfg.vol_mult)

    trend_up = c > sma20
    trend_dn = c < sma20

    oi_delta = 0.0
    if len(oi) >= 2:
        oi_delta = (oi[-1] - oi[-2]) / max(1e-9, abs(oi[-2]))

    # простая логика
    long_cond  = trend_up and vol_ok and body_vs_atr >= cfg.body_atr_mult and c > o
    short_cond = trend_dn and vol_ok and body_vs_atr >= cfg.body_atr_mult and c < o

    if not (long_cond or short_cond):
        return None

    is_long = bool(long_cond)

    # расстояния: стоп около 0.45*ATR, тейк ~ RR*стоп
    stop_dist  = max(atr14 * 0.45,  c * 0.002)
    # RR варьируем слегка по силе факторов
    rr = cfg.rr_min + min(0.8, 0.5 * max(0.0, body_vs_atr - cfg.body_atr_mult)) + min(0.5, abs(oi_delta) * 2.0)

    take_dist = stop_dist * rr

    if is_long:
        entry = c
        tp = entry + take_dist
        sl = entry - stop_dist
        profit_pct = (tp / entry - 1.0) * 100.0
    else:
        entry = c
        tp = entry - take_dist
        sl = entry + stop_dist
        profit_pct = (1.0 - tp / entry) * 100.0

    if profit_pct < cfg.profit_min_pct or rr < cfg.rr_min:
        return None

    prob = estimate_probability(is_long, vol_ok, body_vs_atr, oi_delta)
    if prob < cfg.prob_min:
        return None

    return {
        "symbol": symbol,
        "side": "ЛОНГ" if is_long else "ШОРТ",
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "rr": rr,
        "prob": prob,
        "profit_pct": profit_pct,
    }

# --------------------------- Bot state ---------------------------

@dataclass
class BotState:
    cfg: Config
    session: Optional[aiohttp.ClientSession] = None
    bybit: Optional[BybitClient] = None

    universe: List[str] = field(default_factory=list)     # все символы
    active_idx: int = 0                                   # откуда берём окно
    active: List[str] = field(default_factory=list)       # активная корзина

    last_signal_ts: Dict[str, float] = field(default_factory=dict)
    signal_cooldown_sec: int = 600

# --------------------------- Helpers ---------------------------

def allowed_chat(update: Update, cfg: Config) -> bool:
    chat_id = update.effective_chat.id if update.effective_chat else 0
    return chat_id in cfg.allowed_chat_ids

def fmt_price(x: float) -> str:
    # компактный формат чисел
    if x == 0:
        return "0"
    mag = abs(x)
    if mag >= 100:
        return f"{x:.2f}"
    if mag >= 10:
        return f"{x:.4f}"
    if mag >= 1:
        return f"{x:.6f}"
    return f"{x:.8f}"

async def send_only_to_channels(app: Application, cfg: Config, text: str):
    for cid in cfg.primary_recipients:
        try:
            await app.bot.send_message(chat_id=cid, text=text, disable_notification=True)
        except Exception as e:
            log.warning("send_message to %s failed: %s", cid, e)

# --------------------------- Command handlers ---------------------------

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st: BotState = context.application.bot_data["state"]
    if not allowed_chat(update, st.cfg):
        return
    await update.effective_message.reply_text("pong")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st: BotState = context.application.bot_data["state"]
    if not allowed_chat(update, st.cfg):
        return
    await update.effective_message.reply_text(
        f"Вселенная: total={len(st.universe)}, active={len(st.active)}, "
        f"batch#{st.active_idx}, scan={st.cfg.scan_interval_sec}s"
    )

async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st: BotState = context.application.bot_data["state"]
    if not allowed_chat(update, st.cfg):
        return
    if not st.universe:
        await update.effective_message.reply_text("Вселенная пока не загружена…")
        return
    sample = ", ".join(st.active[:15]) if st.active else "—"
    await update.effective_message.reply_text(
        f"Вселенная: total={len(st.universe)}, active={len(st.active)}, "
        f"batch#{st.active_idx}\nАктивные (пример): {sample}"
    )

# --------------------------- Jobs ---------------------------

async def job_boot(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    st: BotState = app.bot_data["state"]
    # загрузка вселенной
    try:
        st.universe = await st.bybit.get_instruments_linear_usdt()
    except Exception as e:
        log.exception("Failed to load instruments: %s", e)
        st.universe = []

    # сформировать активную корзину
    st.active_idx = 0
    st.active = st.universe[: st.cfg.active_symbols] if st.universe else []
    await send_only_to_channels(app, st.cfg,
        f"Бот запущен ✅\nВселенная: total={len(st.universe)}, active={len(st.active)}, batch#0"
    )

async def job_rotate(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    st: BotState = app.bot_data["state"]
    if not st.universe:
        return
    n = st.cfg.active_symbols
    st.active_idx = (st.active_idx + n) % max(1, len(st.universe))
    new = []
    for i in range(n):
        new.append(st.universe[(st.active_idx + i) % len(st.universe)])
    st.active = new
    log.info("Rotate -> batch#%d active=%d", st.active_idx, len(st.active))

async def _scan_symbol(st: BotState, symbol: str) -> Optional[Dict[str, Any]]:
    try:
        kl, oi = await asyncio.gather(
            st.bybit.get_klines_5m(symbol, limit=max(st.cfg.vol_sma_period + st.cfg.atr_period + 5, 80)),
            st.bybit.get_open_interest_5m(symbol, limit=6),
        )
        sig = make_signal(symbol, kl, oi, st.cfg)
        return sig
    except Exception as e:
        log.debug("scan %s failed: %s", symbol, e)
        return None

async def job_scan(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    st: BotState = app.bot_data["state"]
    if not st.active:
        return

    sem = asyncio.Semaphore(6)

    async def guarded(sym: str):
        async with sem:
            return await _scan_symbol(st, sym)

    tasks = [guarded(s) for s in st.active]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    now = asyncio.get_running_loop().time()
    out_msgs = []
    for sig in results:
        if not sig:
            continue
        last_ts = st.last_signal_ts.get(sig["symbol"], 0.0)
        if now - last_ts < st.signal_cooldown_sec:
            continue
        st.last_signal_ts[sig["symbol"]] = now

        side = sig["side"]
        entry = fmt_price(sig["entry"])
        tp    = fmt_price(sig["tp"])
        sl    = fmt_price(sig["sl"])
        rr    = f"{sig['rr']:.2f}"
        prob  = f"{sig['prob']*100:.1f}%"
        profp = f"{sig['profit_pct']:.2f}%"

        text = (
            f"#{sig['symbol']} — {side}\n"
            f"Вход: {entry}\n"
            f"Тейк: {tp} (+{profp})\n"
            f"Стоп: {sl}\n"
            f"R/R: {rr} | Вероятность: {prob}"
        )
        out_msgs.append(text)

    for msg in out_msgs:
        await send_only_to_channels(app, st.cfg, msg)

async def job_heartbeat(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    st: BotState = app.bot_data["state"]
    await send_only_to_channels(
        app, st.cfg,
        f"онлайн ✅ | total={len(st.universe)} active={len(st.active)} batch#{st.active_idx}"
    )

# --------------------------- Lifecycle hooks ---------------------------

async def post_init(app: Application):
    cfg: Config = app.bot_data["cfg"]
    # session + client
    timeout = aiohttp.ClientTimeout(total=20)
    st = BotState(cfg=cfg, session=aiohttp.ClientSession(timeout=timeout))
    st.bybit = BybitClient(st.session)
    app.bot_data["state"] = st

    # планируем задачи
    app.job_queue.run_once(job_boot, when=1)
    app.job_queue.run_repeating(job_rotate, interval=cfg.rotate_min * 60, first=cfg.rotate_min * 60)
    app.job_queue.run_repeating(job_scan, interval=cfg.scan_interval_sec, first=cfg.scan_interval_sec + 5)
    app.job_queue.run_repeating(job_heartbeat, interval=cfg.heartbeat_sec, first=cfg.heartbeat_sec)

async def post_shutdown(app: Application):
    st: BotState = app.bot_data.get("state")
    if st and st.session and not st.session.closed:
        await st.session.close()

# --------------------------- Main ---------------------------

def build_app(cfg: Config) -> Application:
    app = ApplicationBuilder().token(cfg.token).post_init(post_init).post_shutdown(post_shutdown).build()
    app.bot_data["cfg"] = cfg

    # команды (только для разрешённых чатов)
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("universe", cmd_universe))
    return app

def main():
    cfg = Config.load()
    app = build_app(cfg)

    # ВАЖНО: run_polling — синхронный вызов, НЕ внутри asyncio.run и без await
    log.info("Starting polling…")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        stop_signals=None  # безопаснее для хостингов, которые сами шлют SIGTERM/SIGINT
    )

if __name__ == "__main__":
    main()
