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

        # управление отправкой и таймингами
        self.only_channel: bool = kw.get("only_channel", True)
        self.startup_delay_sec: int = kw.get("startup_delay_sec", 5)
        self.first_scan_delay_sec: int = kw.get("first_scan_delay_sec", 10)
        self.heartbeat_sec: int = kw.get("heartbeat_sec", 900)

        # >>> CHANGE: добавлен конфиг интервала сканирования
        self.scan_interval_sec: int = kw.get("scan_interval_sec", 30)

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

            only_channel=_env_bool("ONLY_CHANNEL", True),
            startup_delay_sec=_env_int("STARTUP_DELAY_SEC", 5),
            first_scan_delay_sec=_env_int("FIRST_SCAN_DELAY_SEC", 10),
            heartbeat_sec=_env_int("HEARTBEAT_SEC", 900),

            # >>> CHANGE: читаем SCAN_INTERVAL_SEC
            scan_interval_sec=_env_int("SCAN_INTERVAL_SEC", 30),

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

    # >>> CHANGE: предупреждение, если некуда слать
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


# >>> CHANGE: команда /jobs — показать расписание задач
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


# >>> CHANGE: команда /debug — краткая диагностика
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

        # >>> CHANGE: сохраняем ссылки на Job-объекты
        job_scan_obj = jq.run_repeating(
            job_scan,
            interval=cfg.scan_interval_sec,  # <<< CHANGE: из конфига
            first=cfg.first_scan_delay_sec,
            name="job_scan",
            coalesce=True,
            misfire_grace_time=30,
        )
        job_hb_obj = jq.run_repeating(
            job_heartbeat,
            interval=cfg.heartbeat_sec,
            first=120,
            name="job_heartbeat",
            coalesce=True,
            misfire_grace_time=120,
        )
        app.bot_data["jobs"] = {
            "scan": job_scan_obj,
            "heartbeat": job_hb_obj,
        }
    except Exception:
        logger.exception("warmup: scheduling failed")

    try:
        await job_heartbeat_simple(app)
    except Exception:
        logger.exception("warmup: first heartbeat failed")


# -----------------------------------------------------------------------------
# MAIN — явный lifecycle (фикс RuntimeError и отсутствия wait_until_closed)
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

    # >>> CHANGE: логируем ключевые конфиги и предупреждаем, если некуда слать
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
    # >>> CHANGE: регистрируем новые/алиасы команд
    application.add_handler(CommandHandler("jobs", cmd_jobs))
    application.add_handler(CommandHandler("debug", cmd_debug))
    application.add_handler(CommandHandler("diag", cmd_debug))          # <<< добавлен алиас
    application.add_handler(CommandHandler("diagnostics", cmd_debug))   # <<< добавлен алиас

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

        # --- ОЖИДАНИЕ (замена отсутствующего Updater.wait_until_closed) ---
        stop_forever = asyncio.Event()
        await stop_forever.wait()  # блокируемся, пока процесс живёт

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
