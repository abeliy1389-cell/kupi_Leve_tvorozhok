import os
import logging
import sqlite3
import uuid
import random
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from contextlib import contextmanager

# ===== ТЕЛЕГРАМ =====
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler,
    ContextTypes
)
# ====================

# ===== КОНФИГУРАЦИЯ =====
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8307261021:AAGCawbFqDzd9osxDOCeUHNRE0G5GaeJKJs")
DB_NAME = 'family_shopping_v2.db'

# Константы
TEMPLATES_COUNT = 4
DEFAULT_TEMPLATES = ['Хлеб', 'Молоко', 'Творожок гугу', 'Сыр']
THANK_YOU_PHRASES = ["Куплено!", "Вычёркиваем!", "Это пригодится!", "Похаем...", 
                     "Из этого что-то можно приготовить...", "Спасибо, дорогой! 🙏", 
                     "Отлично! Так держать! 👍", "Супер !Будет, что поесть! 🎉"]
MOSCOW_TZ_OFFSET = timedelta(hours=3)  # UTC+3

# Состояния для ConversationHandler
ASKING_FAMILY_NAME, ASKING_USER_NAME = range(2)
# =======================

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== ПОМОЩНИКИ ====================

def get_moscow_time(dt: Optional[datetime] = None) -> datetime:
    """Возвращает текущее время по Москве (UTC+3)"""
    if dt is None:
        dt = datetime.utcnow()
    return dt + MOSCOW_TZ_OFFSET

def format_time(dt_str: str) -> str:
    """Форматирует время для отображения (Московское время)"""
    if not dt_str:
        return "давно"
    try:
        # Преобразуем из UTC в MSK
        dt_utc = datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
        dt = get_moscow_time(dt_utc)

        now_msk = get_moscow_time()
        today_msk = now_msk.date()

        if dt.date() == today_msk:
            return f"сегодня {dt.strftime('%H:%M')}"
        elif dt.date() == today_msk - timedelta(days=1):
            return f"вчера {dt.strftime('%H:%M')}"
        elif (today_msk - dt.date()).days < 7:
            days = (today_msk - dt.date()).days
            # Правильное склонение
            if days == 1:
                return "вчера"
            elif days == 2:
                return "позавчера"
            else:
                return f"{days} дней назад"
        else:
            return dt.strftime("%d.%m")
    except Exception as e:
        logger.error(f"Ошибка форматирования времени {dt_str}: {e}")
        return "давно"

def split_multiline_items(text: str) -> List[str]:
    """Разделяет многострочный текст на отдельные товары"""
    items = [line.strip() for line in text.split('\n') if line.strip()]
    return items

def get_random_thankyou() -> str:
    """Возвращает случайную благодарность"""
    return random.choice(THANK_YOU_PHRASES)

# ==================== БАЗА ДАННЫХ ====================

class Database:
    def __init__(self, db_name: str = DB_NAME):
        self.db_name = db_name
        self.init_db()

    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для подключения к БД"""
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def init_db(self):
        """Инициализация базы данных с новой структурой"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Таблица семей (оставляем без изменений)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS families (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        invite_code TEXT UNIQUE NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Таблица пользователей (оставляем без изменений)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        telegram_id INTEGER UNIQUE NOT NULL,
                        family_id INTEGER,
                        username TEXT,
                        full_name TEXT,
                        family_display_name TEXT,
                        is_admin BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (family_id) REFERENCES families (id),
                        UNIQUE(family_id, family_display_name)
                    )
                ''')

                # Таблица активных покупок (оставляем без изменений)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS shopping_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        family_id INTEGER NOT NULL,
                        user_id INTEGER NOT NULL,
                        text TEXT NOT NULL,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (family_id) REFERENCES families (id),
                        FOREIGN KEY (user_id) REFERENCES users (id)
                    )
                ''')

                # Таблица архива (оставляем без изменений)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS archive_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        family_id INTEGER NOT NULL,
                        user_id INTEGER NOT NULL,
                        added_by_user_id INTEGER NOT NULL,
                        text TEXT NOT NULL,
                        bought_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP,
                        FOREIGN KEY (family_id) REFERENCES families (id),
                        FOREIGN KEY (user_id) REFERENCES users (id),
                        FOREIGN KEY (added_by_user_id) REFERENCES users (id)
                    )
                ''')

                # Таблица корзины (ПОКА ОСТАВЛЯЕМ, но не используем в коде)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trash_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        family_id INTEGER NOT NULL,
                        user_id INTEGER NOT NULL,
                        added_by_user_id INTEGER NOT NULL,
                        text TEXT NOT NULL,
                        deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP,
                        FOREIGN KEY (family_id) REFERENCES families (id),
                        FOREIGN KEY (user_id) REFERENCES users (id),
                        FOREIGN KEY (added_by_user_id) REFERENCES users (id)
                    )
                ''')

                # Таблица шаблонов (оставляем без изменений)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        family_id INTEGER NOT NULL,
                        item_text TEXT NOT NULL,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (family_id) REFERENCES families (id),
                        UNIQUE(family_id, item_text)
                    )
                ''')

                # Индексы для производительности +
