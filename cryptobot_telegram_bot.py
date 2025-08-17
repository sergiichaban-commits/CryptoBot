#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import json
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple

import aiohttp
from aiohttp import web

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ======================================================================
#                             CONFIG
# ======================================================================

def _parse_int_list(val: str) -> List[int]:
    if not val:
        return []
    out = []
    for part in val.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            pass
    return out


@dataclass
class Config:
    TELEGRAM_TOKEN: str
    PRIMARY_RECIPIENTS: List[int]
    ALLOWED_CHAT_IDS: List[int]
    PUBLIC_URL: str
    PORT: int

    # Deeplinks / flags / heartbeat
    BYBIT_APP_URL_TMPL: str
    BYBIT_WEB_URL_TMPL: str
    ONLINE_INTERVAL_SEC: int
    SUPPRESS_DM_SIGNALS: bool

    # Universe
    UNIVERSE_TOP_N: int
    WS_SYMBOLS_MAX: int
    ROTATE_MIN: int

    @staticmethod
    def load() -> "Config":
        token = os.getenv("TELEGRAM_TOKEN", "").strip()
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN is required")

        recipients = _parse_int_list(os.getenv("PRIMARY_RECIPIENTS", ""))
        allowed = _parse_int_list(os.getenv("ALLOWED_CHAT_IDS", ""))

        public_url = os.getenv("PUBLIC_URL", "").strip()
        port = int(os.getenv("PORT", "10000"))

        bybit_app = os.getenv("BYBIT_APP_URL_TMPL", "bybit://trade?symbol={symbol}&category=linear")
        bybit_web = os.getenv("BYBIT_WEB_URL_TMPL", "https://www.bybit.com/trade/derivatives/USDT/{symbol}")
        online_sec = int(os.getenv("ONLINE_INTERVAL_SEC", "1800"))
        suppress_dm = os.getenv("SUPPRESS_DM_SIGNALS", "1") == "1"

        universe_top_n = int(os.getenv("UNIVERSE_TOP_N", "15"))
        ws_symbols_max = int(os.getenv("WS_SYMBOLS_MAX", "60"))
        rotate_min = int(os.getenv("ROTATE_MIN", "5"))

        return Config(
            TELEGRAM_TOKEN=token,
            PRIMARY_RECIPIENTS=recipients,
            ALLOWED_CHAT_IDS=allowed,
            PUBLIC_URL=public_url,
            PORT=port,
            BYBIT_APP_URL_TMPL=bybit_app,
            BYBIT_WEB_URL_TMPL=bybit_web,
            ONLINE_INTERVAL_SEC=online_sec,
            SUPPRESS_DM_SIGNALS=suppress_dm,
            UNIVERSE_TOP_N=universe_top_n,
            WS_SYMBOLS_MAX=ws_symbols_max,
            ROTATE_MIN=rotate_min,
        )

# ======================================================================
#                          BYBIT CLIENT
# ======================================================================

class BybitClient:
    BASE = "https://api.bybit.com"

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    @classmethod
    async def create(cls) -> "BybitClient":
        self = cls()
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        assert self._session is not None, "ClientSession is not initialized"
        url = f"{self.BASE}{path}"
        async with self._session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_linear_instruments(self) -> List[str]:
        params = {"category": "linear", "limit": 1000}
        data = await self._get("/v5/market/instruments-info", params)
        result = data.get("result", {}) or {}
        rows = result.get("list", []) or []
        syms: List[str] = []
        for r in rows:
            sym = r.get("symbol", "")
            if sym.endswith("USDT"):
                syms.append(sym)
        return sorted(set(syms))

# ======================================================================
#                          UTILS / PATCHES
# ======================================================================

def _channels_only(recipients: List[int], suppress_dm: bool) -> List[int]:
    if not suppress_dm:
        return recipients
    return [cid for cid in recipients if isinstance(cid, int) and cid < 0]


def _bybit_links(symbol: str, cfg: Config) -> Tuple[str, str]:
    s = symbol.upper()
    return (
        cfg.BYBIT_APP_URL_TMPL.format(symbol=s),
        cfg.BYBIT_WEB_URL_TMPL.format(symbol=s),
    )


def _bybit_markup(symbol: str, cfg: Config) -> InlineKeyboardMarkup:
    app_url, web_url = _bybit_links(symbol, cfg)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Открыть в Bybit", url=app_url)],
        [InlineKeyboardButton("Открыть в браузере", url=web_url)],
    ])


async def send_signal_msg(
    bot,
    cfg: Config,
    recipients: List[int],
    *,
    symbol: str,
    direction: str,     # 'long' | 'short'
    price_now: float,
    entry: float,
    take: float,
    stop: float,
    rr: float,
    prob_pct: float
) -> None:
    app_url, _ = _bybit_links(symbol, cfg)
    link_as_hashtag = f'<a href="{app_url}">#{symbol.upper()}</a>'

    text = (
        f"{link_as_hashtag} — <b>{'ЛОНГ' if direction.lower()=='long' else 'ШОРТ'}</b>\n"
        f"Текущая: <b>{price_now}</b>\n"
        f"Вход: <b>{entry}</b>\n"
        f"Тейк: <b>{take}</b>\n"
        f"Стоп: <b>{stop}</b>\n"
        f"R/R: <b>{rr:.2f}</b> | Вероятность: <b>{prob_pct:.1f}%</b>"
    )

    kb = _bybit_markup(symbol, cfg)
    targets = _channels_only(recipients, cfg.SUPPRESS_DM_SIGNALS)

    for cid in targets:
        try:
            await bot.send_message(
                chat_id=cid,
                text=text,
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception:
            pass

# ======================================================================
#                          UNIVERSE MANAGER
# ======================================================================

class Universe:
    def __init__(self, all_symbols: List[str], ws_symbols_max: int = 60) -> None:
        self.all_symbols = all_symbols
        self.ws_symbols_max = ws_symbols_max
        self._idx = 0

    @property
    def active(self) -> List[str]:
        if not self.all_symbols:
            return []
        end = min(self._idx + self.ws_symbols_max, len(self.all_symbols))
        return self.all_symbols[self._idx:end]

    def rotate(self) -> None:
        if not self.all_symbols:
            return
        self._idx += self.ws_symbols_max
        if self._idx >= len(self.all_symbols):
            self._idx = 0

    def summary(self) -> str:
        return f"Вселенная: total={len(self.all_symbols)}, active={len(self.active)}"

# ======================================================================
#                         HTTP HEALTH SERVER
# ======================================================================

async def start_health_server(port: int) -> None:
    app = web.Application()

    async def root(request: web.Request) -> web.Response:
        return web.Response(text="OK", content_type="text/plain")

    async def health(request: web.Request) -> web.Response:
        payload = {"status": "ok"}
        return web.json_response(payload)

    app.router.add_get("/", root)
    app.router.add_get("/healthz", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()

# ======================================================================
#                         COMMAND HANDLERS
# ======================================================================

def _is_allowed(cfg: Config, update: Update) -> bool:
    chat = update.effective_chat
    if not chat:
        return False
    if not cfg.ALLOWED_CHAT_IDS:
        return True
    return chat.id in cfg.ALLOWED_CHAT_IDS


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    if not _is_allowed(cfg, update):
        return
    await update.effective_message.reply_text("pong")


async def cmd_universe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    if not _is_allowed(cfg, update):
        return
    uni: Universe = context.application.bot_data.get("universe")  # type: ignore
    if not uni or not uni.all_symbols:
        await update.effective_message.reply_text("Вселенная пока не загружена (жду Bybit API/повторяю попытки)…")
        return
    act = uni.active
    sample = ", ".join(act[:15]) + (" ..." if len(act) > 15 else "")
    text = f"{uni.summary()}, ws_topics={len(act)*2}\nАктивные (пример): {sample}"
    await update.effective_message.reply_text(text)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_universe(update, context)


# ======================================================================
#                           JOBS (JobQueue)
# ======================================================================

async def job_online(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    cfg: Config = app.bot_data["cfg"]  # type: ignore
    targets = _channels_only(cfg.PRIMARY_RECIPIENTS, cfg.SUPPRESS_DM_SIGNALS)
    if not targets:
        return
    text = "🟢 Online"
    for cid in targets:
        try:
            await app.bot.send_message(chat_id=cid, text=text)
        except Exception:
            pass


async def job_rotate_universe(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    uni: Universe = app.bot_data.get("universe")  # type: ignore
    if not uni:
        return
    uni.rotate()


async def job_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    cfg: Config = app.bot_data["cfg"]  # type: ignore
    uni: Universe = app.bot_data.get("universe")  # type: ignore
    if not uni or not uni.active:
        return
    # тут ваш анализ; отправка через send_signal_msg(...)

# ======================================================================
#                               MAIN
# ======================================================================

async def main_async() -> None:
    cfg = Config.load()

    # health порт для Render
    asyncio.create_task(start_health_server(cfg.PORT))

    # Telegram application
    application = Application.builder().token(cfg.TELEGRAM_TOKEN).build()

    # Bybit client
    bybit = await BybitClient.create()

    # Вселенная
    all_syms: List[str] = []
    try:
        all_syms = await bybit.get_linear_instruments()
    except Exception:
        all_syms = []

    universe = Universe(all_syms, ws_symbols_max=cfg.WS_SYMBOLS_MAX)

    # shared state
    application.bot_data["cfg"] = cfg
    application.bot_data["bybit"] = bybit
    application.bot_data["universe"] = universe

    # handlers
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("universe", cmd_universe))
    application.add_handler(CommandHandler("status", cmd_status))

    # jobs
    jq = application.job_queue
    jq.run_repeating(job_online, interval=cfg.ONLINE_INTERVAL_SEC, first=10, name="job_online")
    jq.run_repeating(job_rotate_universe, interval=cfg.ROTATE_MIN * 60, first=30, name="job_rotate")
    jq.run_repeating(job_scan, interval=30, first=15, name="job_scan")

    # ---------- THE ONLY CHANGE HERE ----------
    # избегаем попытки закрыть уже работающий loop
    await application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        stop_signals=None,
        close_loop=False,   # <— ключ к вашей ошибке
    )
    # -----------------------------------------

    try:
        await bybit.close()
    except Exception:
        pass


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
