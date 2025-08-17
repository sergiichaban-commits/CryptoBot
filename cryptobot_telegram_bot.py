#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import asyncio
import logging
from typing import Any, Dict, List, Optional

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
    MessageHandler,
    filters,
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


# -----------------------------------------------------------------------------
# КОНФИГ
# -----------------------------------------------------------------------------
class Config:
    def __init__(self, **kw: Any) -> None:
        self.telegram_token: str = kw["telegram_token"]
        self.port: int = kw["port"]
        self.allowed_chat_ids: List[int] = kw.get("allowed_chat_ids", [])
        self.primary_recipients: List[int] = kw.get("primary_recipients", [])

        # ★ ADD: только канал, задержки и «онлайн»-пульс
        self.only_channel: bool = kw.get("only_channel", True)
        self.startup_delay_sec: int = kw.get("startup_delay_sec", 5)
        self.first_scan_delay_sec: int = kw.get("first_scan_delay_sec", 10)
        self.heartbeat_sec: int = kw.get("heartbeat_sec", 900)

        # Параметры «вселенной»
        self.universe_top_n: int = kw.get("universe_top_n", 30)
        self.ws_symbols_max: int = kw.get("ws_symbols_max", 60)

        # Bybit
        self.bybit_base: str = kw.get("bybit_base", "https://api.bybit.com")

    @staticmethod
    def load() -> "Config":
        token = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN is required")

        return Config(
            telegram_token=token,
            port=_env_int("PORT", 10000),
            allowed_chat_ids=_env_list_int("ALLOWED_CHAT_IDS", []),
            primary_recipients=_env_list_int("PRIMARY_RECIPIENTS", []),

            # ★ ADD: управляем через .env
            only_channel=_env_bool("ONLY_CHANNEL", True),
            startup_delay_sec=_env_int("STARTUP_DELAY_SEC", 5),
            first_scan_delay_sec=_env_int("FIRST_SCAN_DELAY_SEC", 10),
            heartbeat_sec=_env_int("HEARTBEAT_SEC", 900),

            universe_top_n=_env_int("UNIVERSE_TOP_N", 30),
            ws_symbols_max=_env_int("WS_SYMBOLS_MAX", 60),

            bybit_base=os.getenv("BYBIT_BASE", "https://api.bybit.com"),
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
            # Создаём внутри работающего event loop
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
        for _ in range(5):  # ограничим пагинацию для стабильности
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
            await asyncio.sleep(0)  # отдаём цикл
        return sorted(set(out))


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
# HEALTH-СЕРВЕР (★ ADD: открываем порт сразу)
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
# ОТПРАВКА (★ CHANGE: только в канал при ONLY_CHANNEL=1)
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

    # имитируем лёгкий «скан» — не блокируем цикл
    try:
        if not st.total:
            # если не успели загрузить — ещё раз попробуем
            await build_universe(app, cfg)
        if st.sample_active:
            st.batch = (st.batch + 1) % max(1, (st.active // 10) or 1)
        logger.info(
            f"scan: candidates={st.active} sent=0 active={st.active} batch#{st.batch}"
        )
    except Exception:
        logger.exception("job_scan failed")
    await asyncio.sleep(0)


# -----------------------------------------------------------------------------
# КОМАНДЫ
# -----------------------------------------------------------------------------
def _is_allowed(update: Update, cfg: Config) -> bool:
    cid = (update.effective_chat.id if update.effective_chat else None)
    if cid is None:
        return False
    # Если список пуст — разрешаем всем (как раньше). Если указан — фильтруем.
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


# -----------------------------------------------------------------------------
# ТЁПЛЫЙ СТАРТ / ПЛАНИРОВАНИЕ (★ ADD: запускаем после открытия порта)
# -----------------------------------------------------------------------------
async def _warmup_and_schedule(app: Application, cfg: Config) -> None:
    # маленькая задержка, чтобы Render успел заметить открытый порт
    await asyncio.sleep(cfg.startup_delay_sec)

    # Инициализируем клиентов/вселенную
    try:
        await build_universe(app, cfg)
    except Exception:
        logger.exception("warmup: universe init failed")

    # Планируем периодические задачи
    try:
        jq = app.job_queue
        jq.run_repeating(
            job_scan,
            interval=30,
            first=cfg.first_scan_delay_sec,
            name="job_scan",
            coalesce=True,
            misfire_grace_time=30,
        )
        jq.run_repeating(
            job_heartbeat,
            interval=cfg.heartbeat_sec,
            first=120,
            name="job_heartbeat",
            coalesce=True,
            misfire_grace_time=120,
        )
    except Exception:
        logger.exception("warmup: scheduling failed")

    # Одноразовый heartbeat
    try:
        await job_heartbeat_simple(app)
    except Exception:
        logger.exception("warmup: first heartbeat failed")


# -----------------------------------------------------------------------------
# MAIN (★ CHANGE: порядок запуска под Render Web Service)
# -----------------------------------------------------------------------------
async def main_async() -> None:
    cfg = Config.load()

    # 1) Поднимаем health-сервер (порт открыт с самого начала)
    try:
        asyncio.create_task(start_health_server(cfg.port))
    except Exception:
        logger.exception("health server failed to start")

    # 2) Telegram app
    application = Application.builder().token(cfg.telegram_token).build()
    application.bot_data["cfg"] = cfg
    application.bot_data["universe_state"] = UniverseState()
    application.bot_data["bybit"] = BybitClient(cfg.bybit_base)

    # 3) Хэндлеры (не трогаем остальное)
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("universe", cmd_universe))
    application.add_handler(CommandHandler("status", cmd_status))
    # Можно оставить и другие ваши хэндлеры/фильтры здесь без изменений

    # 4) Перед polling — удаляем вебхук
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        logger.exception("delete_webhook failed")

    # 5) Тяжёлая инициализация и джобы — уже после поднятия порта
    application.create_task(_warmup_and_schedule(application, cfg), name="warmup")

    # 6) Стабильный polling — без закрытия event loop (★ ключевая правка)
    try:
        await application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False,  # не закрывать loop → нет ошибки "loop is already running"
        )
    except Conflict:
        logger.error("Another instance is polling (Conflict). Exiting this one.")
    finally:
        # Чисто закрываем Bybit-сессию
        try:
            client: BybitClient = application.bot_data["bybit"]
            await client.close()
        except Exception:
            pass


def main() -> None:
    # Запускаем как обычный Web Service процесс
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
