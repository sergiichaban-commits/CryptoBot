"""
CryptoBot — Bybit 1m Candles + Volume Spike Signal (Render FREE Web Service)
- Secure: whitelist chat_ids (env: ALLOWED_CHAT_IDS)
- Recipients filtered by TELEGRAM_CHAT_ID
- Bybit WS: kline.1 <SYMBOL> (env: BYBIT_SYMBOL, default BTCUSDT)
- Candle summary every 5 minutes on confirmed 1m close
- Health ping every 60 minutes
- NEW: Volume spike signal (Volume >= VOL_MULT × SMA(V, VOL_SMA_PERIOD) and |body| >= BODY_ATR_MULT × ATR(ATR_PERIOD))
- Mini HTTP server binds to $PORT (/, /health) so Render Web Service stays up
- Start Command: python cryptobot_telegram_bot.py
"""

import os
import json
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone
from typing import List, Set, Optional
from collections import deque
import math

import websockets
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# ---------------- Config / Security ----------------

ALLOWED_DEFAULT: Set[int] = {533232884, -1002870952333}  # ты и канал ChaSerBot

# env-переключатели для триггера (можно не трогать — есть дефолты)
VOL_SMA_PERIOD = int(os.environ.get("VOL_SMA_PERIOD", "20"))
ATR_PERIOD = int(os.environ.get("ATR_PERIOD", "14"))
BODY_ATR_MULT = float(os.environ.get("BODY_ATR_MULT", "0.6"))
VOL_MULT = float(os.environ.get("VOL_MULT", "2.0"))
ALERT_COOLDOWN_SEC = int(os.environ.get("ALERT_COOLDOWN_SEC", "600"))  # 10 минут

PING_INTERVAL_MIN = 60
POST_EVERY_N_MIN = 5
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
SYMBOL = os.environ.get("BYBIT_SYMBOL", "BTCUSDT").strip() or "BTCUSDT"
HTTP_PORT = int(os.environ.get("PORT", "8000"))  # Render присваивает $PORT

def _parse_id_list(value: str) -> List[int]:
    """Парсер chat_id, устойчивый к пробелам/кавычкам."""
    out: List[int] = []
    if not value:
        return out
    for part in value.split(","):
        part = part.strip().strip('"').strip("'")
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            print(f"[warn] cannot parse chat id from: {repr(part)}")
    return out

ALLOWED_CHAT_IDS: Set[int] = set(_parse_id_list(os.environ.get("ALLOWED_CHAT_IDS", ""))) or ALLOWED_DEFAULT
RECIPIENTS: List[int] = [cid for cid in _parse_id_list(os.environ.get("TELEGRAM_CHAT_ID", "")) if cid in ALLOWED_CHAT_IDS]

print(f"[info] ALLOWED_CHAT_IDS = {sorted(ALLOWED_CHAT_IDS)}")
print(f"[info] TELEGRAM_CHAT_ID(raw) = {os.environ.get('TELEGRAM_CHAT_ID', '')!r}")
print(f"[info] RECIPIENTS (whitelisted) = {RECIPIENTS}")
print(f"[info] BYBIT_SYMBOL = {SYMBOL!r}")
print(f"[info] HTTP_PORT = {HTTP_PORT}")
print(f"[info] Volume trigger params: VOL_MULT={VOL_MULT}, VOL_SMA_PERIOD={VOL_SMA_PERIOD}, BODY_ATR_MULT={BODY_ATR_MULT}, ATR_PERIOD={ATR_PERIOD}, COOLDOWN={ALERT_COOLDOWN_SEC}s")

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

# ---------------- Telegram handlers (whitelist) ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id not in ALLOWED_CHAT_IDS:
        return
    await update.message.reply_text("Бот онлайн ✅")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id not in ALLOWED_CHAT_IDS:
        return
    await update.message.reply_text(
        "Статус: онлайн\n"
        f"Whitelist: {sorted(ALLOWED_CHAT_IDS)}\n"
        f"Recipients: {RECIPIENTS}\n"
        f"Symbol: {SYMBOL}\n"
        f"Trigger: Volume ≥ {VOL_MULT}×SMA{VOL_SMA_PERIOD} and |body| ≥ {BODY_ATR_MULT}×ATR{ATR_PERIOD}\n"
        f"Uptime: {utcnow().isoformat()}"
    )

async def ignore_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id in ALLOWED_CHAT_IDS:
        return

# ---------------- Helpers for indicators ----------------

class Candle:
    __slots__ = ("t","o","h","l","c","v")
    def __init__(self, t:int, o:float, h:float, l:float, c:float, v:float):
        self.t=t; self.o=o; self.h=h; self.l=l; self.c=c; self.v=v

candle_hist: deque[Candle] = deque(maxlen=400)  # ~6.5 часов 1м
_last_alert_ts: int = 0  # epoch seconds

def _sma_volume(period: int) -> Optional[float]:
    if len(candle_hist) < period:
        return None
    vals = [c.v for c in list(candle_hist)[-period:]]
    return sum(vals)/period

def _atr(period: int) -> Optional[float]:
    if len(candle_hist) < period + 1:
        return None
    trs = []
    arr = list(candle_hist)
    for i in range(-period, 0):
        c0 = arr[i-1]
        c1 = arr[i]
        tr = max(c1.h - c1.l, abs(c1.h - c0.c), abs(c1.l - c0.c))
        trs.append(tr)
    return sum(trs)/period

def _fmt_price(x: float) -> str:
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.6f}"

# ---------------- Bybit WS consumer ----------------

async def bybit_candles(application: Application):
    """Подписка на kline.1.<SYMBOL>, сводка раз в POST_EVERY_N_MIN минут + сигнал по объёму на подтверждённой свече."""
    global _last_alert_ts
    last_posted_min: Optional[int] = None

    async def send_summary(candle: dict):
        o = float(candle["open"]); h = float(candle["high"]); l = float(candle["low"]); c = float(candle["close"])
        v = float(candle["volume"])
        ts = int(candle["end"])  # ms
        t = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%H:%M")
        text = f"{SYMBOL} 1m {t} UTC — O:{_fmt_price(o)} H:{_fmt_price(h)} L:{_fmt_price(l)} C:{_fmt_price(c)} V:{v:.0f}"
        for cid in RECIPIENTS:
            try:
                await application.bot.send_message(chat_id=cid, text=text, disable_notification=True)
            except Exception as e:
                print(f"[warn] bybit summary -> {cid}: {e}")

    async def maybe_send_volume_signal(candle: dict):
        """Проверяет условия и отправляет сигнал (антиспам — кулдаун)."""
        global _last_alert_ts
        end_ms = int(candle["end"])
        o = float(candle["open"]); h = float(candle["high"]); l = float(candle["low"]); c = float(candle["close"])
        v = float(candle["volume"])
        # обновляем историю
        candle_hist.append(Candle(end_ms, o,h,l,c,v))

        atr = _atr(ATR_PERIOD)
        v_sma = _sma_volume(VOL_SMA_PERIOD)
        if atr is None or v_sma is None or atr <= 0 or v_sma <= 0:
            return

        body = abs(c - o)
        body_ok = (body >= BODY_ATR_MULT * atr)
        vol_ok  = (v    >= VOL_MULT * v_sma)
        if not (body_ok and vol_ok):
            return

        # антиспам
        now_s = int(end_ms/1000)
        if now_s - _last_alert_ts < ALERT_COOLDOWN_SEC:
            return
        _last_alert_ts = now_s

        side = "LONG" if c >= o else "SHORT"
        t = datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).strftime("%H:%M")
        text = (
            f"⚡ Volume spike: {SYMBOL} — {side}\n"
            f"1m {t} UTC  |  body={_fmt_price(body)} ({body/atr:.2f}×ATR{ATR_PERIOD})  |  "
            f"vol={v:.0f} ({v/v_sma:.2f}×SMA{VOL_SMA_PERIOD})\n"
            f"O={_fmt_price(o)}  H={_fmt_price(h)}  L={_fmt_price(l)}  C={_fmt_price(c)}"
        )
        for cid in RECIPIENTS:
            try:
                await application.bot.send_message(chat_id=cid, text=text, disable_notification=False)
            except Exception as e:
                print(f"[warn] volume signal -> {cid}: {e}")

    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL, ping_interval=25, ping_timeout=20) as ws:
                sub = {"op": "subscribe", "args": [f"kline.1.{SYMBOL}"]}
                await ws.send(json.dumps(sub))
                print("[info] Subscribed to", sub["args"])

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    if msg.get("topic", "").startswith("kline."):
                        for item in msg.get("data", []):
                            if not item.get("confirm"):
                                continue  # ждём закрытия свечи
                            # 1) короткая сводка — раз в N минут
                            end_ms = int(item["end"])
                            minute = datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).minute
                            if (last_posted_min is None) or ((minute % POST_EVERY_N_MIN) == 0 and minute != last_posted_min):
                                last_posted_min = minute
                                await send_summary(item)
                            # 2) проверка триггера по объёму
                            await maybe_send_volume_signal(item)
        except Exception as e:
            print("[warn] WS reconnecting due to:", e)
            await asyncio.sleep(3)  # backoff

# ---------------- Health-check ----------------

async def health_loop(application: Application):
    if not RECIPIENTS:
        return
    while True:
        for cid in RECIPIENTS:
            try:
                await application.bot.send_message(chat_id=cid, text="🟢 online", disable_notification=True)
            except Exception as e:
                print(f"[warn] health-check -> {cid}: {e}")
        await asyncio.sleep(PING_INTERVAL_MIN * 60)

# ---------------- Tiny HTTP server (no asyncio) ----------------

class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)
    def log_message(self, fmt, *args):
        return  # тише лог

def start_http_server():
    srv = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), _Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    print(f"[info] HTTP server listening on 0.0.0.0:{HTTP_PORT}")

# ---------------- App lifecycle ----------------

async def post_init(application: Application):
    # стартовое сообщение (если есть кому слать)
    for cid in RECIPIENTS:
        try:
            await application.bot.send_message(chat_id=cid, text=f"✅ Render Web Service: бот запущен. Symbol={SYMBOL}")
        except Exception as e:
            print(f"[warn] startup -> {cid}: {e}")
    # фоновые задачи (без JobQueue)
    asyncio.create_task(health_loop(application))
    asyncio.create_task(bybit_candles(application))

def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN env var.")

    # 1) поднять HTTP-порт для Render (в отдельном потоке)
    start_http_server()

    # 2) Telegram bot (PTB сам управляет своим event loop)
    app = Application.builder().token(token).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.ALL, ignore_all))
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
