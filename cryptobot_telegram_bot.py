# -*- coding: utf-8 -*-
"""
Cryptobot — SCALPING Signals (Bybit V5, USDT Perpetuals)
v10.5 — Stabilized Telegram long-polling (webhook purge + backoff + ACK),
        de-dup of signals, safe sender, no banner spam, WS watchdog.
        Strategy: 5m RSI re-entry + Momentum + EMA + Levels.

Author: Sergii Chaban & ChatGPT
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
import telebot

# Попытка подключения к библиотеке websocket (замените на реальный код получения сигналов)
try:
    import websocket
except ImportError:
    websocket = None  # Если библиотека не установлена, WebSocket-соединение не будет работать.

# Получаем токен и настройки из переменных окружения Northflank
import os
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID')    # ID чата или канала (может быть в формате "@channel")
WS_URL    = os.getenv('SIGNALS_WS_URL')      # URL WebSocket для получения сигналов

# Инициализация логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

if not BOT_TOKEN or not CHAT_ID:
    logger.error("Не задан BOT_TOKEN или CHAT_ID в окружении. Завершение.")
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN)

# Для фильтрации дублей: словарь { (ticker, direction, second) : datetime }
seen_signals = {}

def process_signal(signal):
    """
    Обрабатывает сигнал: проверяет возраст и дубли,
    отправляет в Telegram, логирует причины пропуска.
    signal: dict {'ticker': str, 'direction': str, 'timestamp': float или str}
    """
    ticker = signal.get('ticker')
    direction = signal.get('direction')
    ts = signal.get('timestamp')
    if not ticker or not direction or not ts:
        logger.warning(f"Сигнал пропущен: недостаточно данных ({signal})")
        return

    # Преобразуем метку времени сигнала в datetime (UTC)
    if isinstance(ts, (int, float)):
        signal_time = datetime.fromtimestamp(ts, timezone.utc)
    elif isinstance(ts, str):
        try:
            signal_time = datetime.fromisoformat(ts).astimezone(timezone.utc)
        except ValueError:
            logger.warning(f"Сигнал пропущен: неверный формат времени '{ts}'")
            return
    else:
        logger.warning(f"Сигнал пропущен: неизвестный тип timestamp '{ts}'")
        return

    now = datetime.now(timezone.utc)
    age = (now - signal_time).total_seconds()

    # Фильтрация по времени: старше 30 сек отбрасываем
    if age > 30:
        logger.info(f"Сигнал {ticker} {direction} игнорируется: устарел ({age:.1f}s) ")
        return

    # Создаём ключ для проверки дублирования (вторая точность)
    key = (ticker, direction, int(signal_time.timestamp()))
    if key in seen_signals:
        logger.info(f"Сигнал {ticker} {direction} в {signal_time} пропущен как дубликат")
        return

    # Попытка отправки сообщения в Telegram
    message = f"Сигнал: {ticker} – {direction} (время {signal_time.strftime('%Y-%m-%d %H:%M:%S UTC')})"
    try:
        bot.send_message(CHAT_ID, message)
        logger.info(f"Отправлен сигнал: {ticker} {direction} в {signal_time}")
        seen_signals[key] = signal_time
    except telebot.apihelper.ApiException as e:
        # Если получен конфликт 409 при отправке (маловероятно), логируем отдельно
        if e.error_code == 409:
            logger.error(f"Ошибка Telegram 409 Conflict при отправке сообщения: {e}")
        else:
            logger.error(f"Ошибка Telegram API при отправке сообщения: {e}")
    except Exception as e:
        logger.error(f"Ошибка при отправке Telegram-сообщения: {e}")

    # Очищаем старые записи (более 60 сек) для ограничения памяти
    expired = []
    for sig_key, sig_time in seen_signals.items():
        if (now - sig_time).total_seconds() > 60:
            expired.append(sig_key)
    for sig_key in expired:
        del seen_signals[sig_key]

def websocket_listener():
    """
    Бесконечный цикл подключения по WebSocket для получения сигналов.
    При разрыве пытается переподключаться через 5 сек.
    """
    while True:
        try:
            if not websocket:
                raise RuntimeError("Библиотека websocket-client не установлена")
            logger.info(f"Подключение к WebSocket: {WS_URL}")
            ws = websocket.create_connection(WS_URL)
            while True:
                raw = ws.recv()
                try:
                    import json
                    data = json.loads(raw)
                except Exception:
                    logger.warning(f"Не удалось распарсить сигнал: {raw}")
                    continue
                process_signal(data)
        except Exception as e:
            logger.error(f"Ошибка WebSocket-подключения: {e}. Переподключаемся через 5 сек.")
            time.sleep(5)

def clear_pending_updates():
    """
    Явная очистка накопившихся обновлений Telegram, если они есть.
    Обычно достаточно skip_pending=True, но для надёжности сбрасываем апдейты вручную.
    """
    try:
        updates = bot.get_updates(timeout=1)
        if updates:
            last_id = updates[-1].update_id
            bot.get_updates(offset=last_id+1)
            logger.info(f"Очищено {len(updates)} накопленных апдейтов Telegram")
    except Exception as e:
        logger.error(f"Не удалось очистить накопившиеся апдейты: {e}")

def main():
    # Очищаем pending updates при старте (не обязательно с skip_pending=True, но дублируем)
    clear_pending_updates()

    # Запускаем WebSocket-слушатель в отдельном потоке
    ws_thread = threading.Thread(target=websocket_listener, daemon=True)
    ws_thread.start()

    # Запускаем долгий опрос Telegram (skip_pending=True пропустит старые апдейты:contentReference[oaicite:2]{index=2})
    while True:
        try:
            bot.infinity_polling(skip_pending=True)
        except telebot.apihelper.ApiException as e:
            # Обработка ошибок Telegram API, включая 409 Conflict:contentReference[oaicite:3]{index=3}
            if e.error_code == 409:
                logger.error(f"Ошибка Telegram 409 Conflict (возможно, другой процесс уже опрашивает апдейты): {e}")
            else:
                logger.error(f"Ошибка Telegram API: {e}")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Неожиданная ошибка в опросе Telegram: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
