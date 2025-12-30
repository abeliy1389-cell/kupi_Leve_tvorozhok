import os
import logging
import sqlite3
import uuid
import random
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any
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
BOT_TOKEN = "8307261021:AAGCawbFqDzd9osxDOCeUHNRE0G5GaeJKJs"
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

# Состояния для подтверждения удаления
CONFIRM_DELETE = 100
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

def format_item_text(item_text: str) -> str:
    """Форматирует текст товара - делает жирным"""
    return f"**{item_text}**"

def get_recent_activities_text(family_id: int) -> str:
    """Возвращает текст последних 5 действий"""
    recent = db.get_recent_activities(family_id)
    if not recent:
        return "\n\n📭 *Последних действий пока нет*"
    
    text = "\n\n🕐 *Последние действия:*\n"
    for i, activity in enumerate(recent, 1):
        action = "🛒 Купил" if activity['type'] == 'bought' else "➕ Добавил"
        text += f"{i}. {action} **{activity['text']}**\n   👤 {activity['user_name']}, {activity['time']}\n"
    
    return text

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
        """Инициализация базы данных"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Таблица семей
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS families (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        invite_code TEXT UNIQUE NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Таблица пользователей
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

                # Таблица активных покупок
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

                # Таблица архива
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

                # Таблица шаблонов
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        family_id INTEGER NOT NULL,
                        item_text TEXT NOT NULL,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (family_id) REFERENCES families (id),
                        UNIQUE(family_id, item_text COLLATE NOCASE)
                    )
                ''')

                # Индексы для производительности
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_family ON shopping_items(family_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_active ON shopping_items(is_active)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_archive_family ON archive_items(family_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_family ON users(family_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_shopping_created ON shopping_items(created_at)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_archive_bought ON archive_items(bought_at)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram ON users(telegram_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_templates_family ON templates(family_id)')

                conn.commit()
                logger.info("✅ База данных инициализирована")

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")

    def create_family(self, name: str) -> Tuple[int, str]:
        """Создает новую семью и возвращает (family_id, invite_code)"""
        try:
            invite_code = str(uuid.uuid4())[:8].upper()
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'INSERT INTO families (name, invite_code) VALUES (?, ?)',
                    (name, invite_code)
                )
                family_id = cursor.lastrowid
                conn.commit()
                return family_id, invite_code
        except Exception as e:
            logger.error(f"Ошибка создания семьи: {e}")
            return 0, ""

    def get_or_create_user(self, telegram_id: int, username: str = None, full_name: str = None):
        """Получает или создает пользователя"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT id, family_id, is_admin, family_display_name FROM users WHERE telegram_id = ?',
                    (telegram_id,)
                )
                result = cursor.fetchone()
                if result:
                    return result['id'], result['family_id'], bool(result['is_admin']), result['family_display_name']

                display_name = full_name or username or f"User{telegram_id}"
                cursor.execute(
                    '''INSERT INTO users (telegram_id, username, full_name, family_display_name)
                       VALUES (?, ?, ?, ?)''',
                    (telegram_id, username, full_name, display_name)
                )
                user_id = cursor.lastrowid
                conn.commit()
                return user_id, None, False, display_name
        except Exception as e:
            logger.error(f"Ошибка get_or_create_user: {e}")
            return 0, None, False, None

    def update_user_display_name(self, user_id: int, display_name: str) -> bool:
        """Обновляет отображаемое имя пользователя в семье"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'UPDATE users SET family_display_name = ? WHERE id = ?',
                    (display_name, user_id)
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка update_user_display_name: {e}")
            return False

    def add_user_to_family(self, user_id: int, family_id: int, is_admin: bool = False):
        """Добавляет пользователя в семью"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'UPDATE users SET family_id = ?, is_admin = ? WHERE id = ?',
                    (family_id, is_admin, user_id)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка add_user_to_family: {e}")

    def get_family_by_invite_code(self, invite_code: str):
        """Находит семью по коду приглашения"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT id, name FROM families WHERE invite_code = ?',
                    (invite_code,)
                )
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"Ошибка get_family_by_invite_code: {e}")
            return None

    def get_family_members(self, family_id: int):
        """Получает список участников семьи"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, telegram_id, family_display_name, is_admin
                    FROM users
                    WHERE family_id = ?
                    ORDER BY is_admin DESC, family_display_name
                ''', (family_id,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка get_family_members: {e}")
            return []

    def update_family_name(self, family_id: int, new_name: str) -> bool:
        """Изменяет название семьи"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'UPDATE families SET name = ? WHERE id = ?',
                    (new_name, family_id)
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка update_family_name: {e}")
            return False

    def remove_user_from_family(self, user_id: int, family_id: int) -> bool:
        """Исключает пользователя из семьи"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'UPDATE users SET family_id = NULL, is_admin = FALSE WHERE id = ? AND family_id = ?',
                    (user_id, family_id)
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка remove_user_from_family: {e}")
            return False

    def transfer_admin_rights(self, family_id: int, from_user_id: int, to_user_id: int) -> bool:
        """Передает права администратора"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('BEGIN TRANSACTION')
                
                # Проверяем, что from_user_id действительно админ
                cursor.execute(
                    'SELECT is_admin FROM users WHERE id = ? AND family_id = ?',
                    (from_user_id, family_id)
                )
                from_user = cursor.fetchone()
                
                if not from_user or not from_user['is_admin']:
                    conn.rollback()
                    return False

                # Снимаем права у старого админа
                cursor.execute(
                    'UPDATE users SET is_admin = FALSE WHERE id = ? AND family_id = ?',
                    (from_user_id, family_id)
                )

                # Даем права новому админу
                cursor.execute(
                    'UPDATE users SET is_admin = TRUE WHERE id = ? AND family_id = ?',
                    (to_user_id, family_id)
                )

                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка transfer_admin_rights: {e}")
            conn.rollback()
            return False

    # ===== ТОВАРЫ =====

    def add_shopping_item(self, family_id: int, user_id: int, text: str) -> int:
        """Добавляет товар в список покупок"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    '''INSERT INTO shopping_items (family_id, user_id, text)
                       VALUES (?, ?, ?)''',
                    (family_id, user_id, text)
                )
                item_id = cursor.lastrowid
                conn.commit()
                return item_id
        except Exception as e:
            logger.error(f"Ошибка add_shopping_item: {e}")
            return 0

    def add_multiple_items(self, family_id: int, user_id: int, items: List[str]) -> int:
        """Добавляет несколько товаров"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                count = 0
                for item in items:
                    cursor.execute(
                        '''INSERT INTO shopping_items (family_id, user_id, text)
                           VALUES (?, ?, ?)''',
                        (family_id, user_id, item)
                    )
                    count += 1
                conn.commit()
                return count
        except Exception as e:
            logger.error(f"Ошибка add_multiple_items: {e}")
            return 0

    def get_active_items_with_users(self, family_id: int):
        """Получает активные товары с именами добавивших"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT si.id, si.text, si.created_at, u.family_display_name as user_name
                    FROM shopping_items si
                    JOIN users u ON si.user_id = u.id
                    WHERE si.family_id = ? AND si.is_active = TRUE
                    ORDER BY si.created_at ASC
                ''', (family_id,))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка get_active_items_with_users: {e}")
            return []

    def get_archive_items_with_users(self, family_id: int, limit: int = 50):
        """Получает архивные (купленные) товары"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT ai.id, ai.text, ai.bought_at, ai.created_at,
                           u1.family_display_name as bought_by,
                           u2.family_display_name as added_by
                    FROM archive_items ai
                    JOIN users u1 ON ai.user_id = u1.id
                    JOIN users u2 ON ai.added_by_user_id = u2.id
                    WHERE ai.family_id = ?
                    ORDER BY ai.bought_at DESC
                    LIMIT ?
                ''', (family_id, limit))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка get_archive_items_with_users: {e}")
            return []

    def mark_item_as_bought(self, item_id: int, user_id: int) -> bool:
        """Отмечает товар как купленный (перемещает в архив)"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('BEGIN TRANSACTION')

                # Получаем данные товара
                cursor.execute('''
                    SELECT si.id, si.family_id, si.user_id, si.text, si.created_at
                    FROM shopping_items si
                    WHERE si.id = ? AND si.is_active = TRUE
                ''', (item_id,))
                item = cursor.fetchone()

                if not item:
                    conn.rollback()
                    return False

                # Добавляем в архив
                cursor.execute('''
                    INSERT INTO archive_items (family_id, user_id, added_by_user_id, text, created_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (item['family_id'], user_id, item['user_id'], item['text'], item['created_at']))

                # Удаляем из активных
                cursor.execute('DELETE FROM shopping_items WHERE id = ?', (item_id,))

                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка mark_item_as_bought: {e}")
            conn.rollback()
            return False

    def delete_item_permanently(self, item_id: int, user_id: int) -> bool:
        """Удаляет товар навсегда"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'DELETE FROM shopping_items WHERE id = ?',
                    (item_id,)
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка delete_item_permanently: {e}")
            return False

    # ===== ШАБЛОНЫ И СТАТИСТИКА =====

    def get_family_templates(self, family_id: int):
        """Получает шаблоны для семьи с учетом регистронезависимости"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT item_text FROM templates
                    WHERE family_id = ?
                    ORDER BY last_updated DESC
                    LIMIT ?
                ''', (family_id, TEMPLATES_COUNT))
                results = cursor.fetchall()

                if results:
                    return [r['item_text'] for r in results]
                else:
                    # Добавляем дефолтные шаблоны
                    for template in DEFAULT_TEMPLATES:
                        cursor.execute('''
                            INSERT OR IGNORE INTO templates (family_id, item_text)
                            VALUES (?, ?)
                        ''', (family_id, template))
                    conn.commit()
                    return DEFAULT_TEMPLATES
        except Exception as e:
            logger.error(f"Ошибка get_family_templates: {e}")
            return DEFAULT_TEMPLATES

    def get_monthly_stats(self, family_id: int, year: int, month: int):
        """Получает статистику за конкретный месяц"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Определяем диапазон дат для месяца
                start_date = f"{year:04d}-{month:02d}-01"
                if month == 12:
                    end_date = f"{year+1:04d}-01-01"
                else:
                    end_date = f"{year:04d}-{month+1:02d}-01"

                # Топ-5 товаров за месяц (регистронезависимо)
                cursor.execute('''
                    SELECT LOWER(text) as normalized_text, COUNT(*) as count
                    FROM archive_items
                    WHERE family_id = ?
                      AND bought_at >= ? AND bought_at < ?
                    GROUP BY normalized_text
                    ORDER BY count DESC
                    LIMIT 5
                ''', (family_id, start_date, end_date))
                top_items_month = cursor.fetchall()

                # Статистика пользователей за месяц
                cursor.execute('''
                    SELECT u.family_display_name,
                           SUM(CASE WHEN ai.user_id = u.id THEN 1 ELSE 0 END) as bought_count,
                           SUM(CASE WHEN ai.added_by_user_id = u.id THEN 1 ELSE 0 END) as added_count
                    FROM archive_items ai
                    JOIN users u ON ai.family_id = u.family_id
                    WHERE ai.family_id = ?
                      AND ai.bought_at >= ? AND ai.bought_at < ?
                    GROUP BY u.id, u.family_display_name
                    ORDER BY bought_count DESC
                ''', (family_id, start_date, end_date))
                user_stats_month = cursor.fetchall()

                # Общая статистика за месяц
                cursor.execute('''
                    SELECT
                        COUNT(*) as total_items,
                        COUNT(DISTINCT user_id) as unique_buyers,
                        COUNT(DISTINCT LOWER(text)) as unique_items
                    FROM archive_items
                    WHERE family_id = ?
                      AND bought_at >= ? AND bought_at < ?
                ''', (family_id, start_date, end_date))
                total_month = cursor.fetchone()

                return {
                    'top_items_month': [dict(row) for row in top_items_month],
                    'user_stats_month': [dict(row) for row in user_stats_month],
                    'total_items': total_month['total_items'] if total_month else 0,
                    'unique_buyers': total_month['unique_buyers'] if total_month else 0,
                    'unique_items': total_month['unique_items'] if total_month else 0,
                    'month': f"{month:02d}.{year:04d}"
                }
        except Exception as e:
            logger.error(f"Ошибка get_monthly_stats: {e}")
            return {
                'top_items_month': [],
                'user_stats_month': [],
                'total_items': 0,
                'unique_buyers': 0,
                'unique_items': 0,
                'month': f"{month:02d}.{year:04d}"
            }

    def get_all_time_stats(self, family_id: int):
        """Получает общую статистику за все время"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Топ-10 товаров за все время (регистронезависимо)
                cursor.execute('''
                    SELECT LOWER(text) as normalized_text, COUNT(*) as count
                    FROM archive_items
                    WHERE family_id = ?
                    GROUP BY normalized_text
                    ORDER BY count DESC
                    LIMIT 10
                ''', (family_id,))
                top_items_all_time = cursor.fetchall()

                # Статистика пользователей за все время
                cursor.execute('''
                    SELECT u.family_display_name,
                           SUM(CASE WHEN ai.user_id = u.id THEN 1 ELSE 0 END) as bought_count,
                           SUM(CASE WHEN ai.added_by_user_id = u.id THEN 1 ELSE 0 END) as added_count,
                           SUM(CASE WHEN si.user_id = u.id THEN 1 ELSE 0 END) as currently_added
                    FROM users u
                    LEFT JOIN archive_items ai ON u.id = ai.user_id OR u.id = ai.added_by_user_id
                    LEFT JOIN shopping_items si ON u.id = si.user_id AND si.is_active = TRUE
                    WHERE u.family_id = ?
                    GROUP BY u.id, u.family_display_name
                    ORDER BY bought_count DESC
                ''', (family_id,))
                user_stats_all_time = cursor.fetchall()

                # Активные товары
                cursor.execute('''
                    SELECT COUNT(*) as active_items
                    FROM shopping_items
                    WHERE family_id = ? AND is_active = TRUE
                ''', (family_id,))
                active = cursor.fetchone()

                # Купленные товары
                cursor.execute('''
                    SELECT COUNT(*) as bought_items
                    FROM archive_items
                    WHERE family_id = ?
                ''', (family_id,))
                bought = cursor.fetchone()

                return {
                    'top_items_all_time': [dict(row) for row in top_items_all_time],
                    'user_stats_all_time': [dict(row) for row in user_stats_all_time],
                    'active_items': active['active_items'] if active else 0,
                    'bought_items': bought['bought_items'] if bought else 0
                }
        except Exception as e:
            logger.error(f"Ошибка get_all_time_stats: {e}")
            return {
                'top_items_all_time': [],
                'user_stats_all_time': [],
                'active_items': 0,
                'bought_items': 0
            }

    def get_recent_activities(self, family_id: int, limit: int = 5):
        """Получает последние 5 действий (добавления и покупки)"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Получаем последние покупки
                cursor.execute('''
                    SELECT ai.text, ai.bought_at as action_time, 
                           u1.family_display_name as user_name, 'bought' as type
                    FROM archive_items ai
                    JOIN users u1 ON ai.user_id = u1.id
                    WHERE ai.family_id = ?
                    ORDER BY ai.bought_at DESC
                    LIMIT ?
                ''', (family_id, limit))
                
                bought_activities = cursor.fetchall()
                
                # Получаем последние добавления активных товаров
                cursor.execute('''
                    SELECT si.text, si.created_at as action_time,
                           u.family_display_name as user_name, 'added' as type
                    FROM shopping_items si
                    JOIN users u ON si.user_id = u.id
                    WHERE si.family_id = ? AND si.is_active = TRUE
                    ORDER BY si.created_at DESC
                    LIMIT ?
                ''', (family_id, limit))
                
                added_activities = cursor.fetchall()
                
                # Объединяем и сортируем по времени
                all_activities = []
                for row in bought_activities:
                    all_activities.append({
                        'text': row['text'],
                        'time': format_time(row['action_time']),
                        'user_name': row['user_name'],
                        'type': 'bought',
                        'timestamp': row['action_time']
                    })
                
                for row in added_activities:
                    all_activities.append({
                        'text': row['text'],
                        'time': format_time(row['action_time']),
                        'user_name': row['user_name'],
                        'type': 'added',
                        'timestamp': row['action_time']
                    })
                
                # Сортируем по времени и берем последние limit
                all_activities.sort(key=lambda x: x['timestamp'], reverse=True)
                return all_activities[:limit]
                
        except Exception as e:
            logger.error(f"Ошибка get_recent_activities: {e}")
            return []

    def get_family_name(self, family_id: int) -> str:
        """Получает название семьи"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT name FROM families WHERE id = ?',
                    (family_id,)
                )
                result = cursor.fetchone()
                return result['name'] if result else "Семья"
        except Exception as e:
            logger.error(f"Ошибка get_family_name: {e}")
            return "Семья"

db = Database()

# ==================== КЛАВИАТУРЫ ====================

def get_main_keyboard(family_id: int = None, is_admin: bool = False):
    """Главное меню БЕЗ подсказок"""
    buttons = []

    # Шаблоны
    if family_id:
        templates = db.get_family_templates(family_id)
        if templates:
            template_buttons = []
            for template in templates[:TEMPLATES_COUNT]:
                template_buttons.append(
                    InlineKeyboardButton(str(template)[:15], callback_data=f"template_{template}")
                )
            for i in range(0, len(template_buttons), 2):
                row = template_buttons[i:i+2]
                if row:
                    buttons.append(row)

    # Основные кнопки - БЕЗ корзины
    buttons.extend([
        [InlineKeyboardButton("📃 Список покупок", callback_data="show_list")],
        [InlineKeyboardButton("🛒 Купленные товары", callback_data="show_archive")],
        [InlineKeyboardButton("📊 Статистика", callback_data="show_stats")]
    ])

    # Админ кнопки
    if is_admin:
        buttons.append([InlineKeyboardButton("👑 Админ", callback_data="admin_panel")])

    return InlineKeyboardMarkup(buttons)

def get_list_keyboard(items, context: ContextTypes.DEFAULT_TYPE = None):
    """Клавиатура для списка покупок (с кнопками купить/удалить)"""
    keyboard = []
    for item in items:
        if len(item) >= 4:
            item_id, text, created_at, user_name = item
            formatted_text = format_item_text(text[:20] if len(text) <= 20 else f"{text[:17]}...")
            
            # Большая кнопка "купить" (3/4 строки) и маленькая "удалить" (1/4 строки)
            keyboard.append([
                InlineKeyboardButton(f"✅ {formatted_text}", callback_data=f"buy_{item_id}"),
                InlineKeyboardButton("🗑️", callback_data=f"ask_delete_{item_id}")
            ])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(keyboard)

def get_confirmation_keyboard(item_id: int):
    """Клавиатура для подтверждения удаления"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{item_id}"),
            InlineKeyboardButton("❌ Нет, отмена", callback_data="cancel_delete")
        ]
    ])

def get_archive_keyboard(items, is_admin: bool = False):
    """Клавиатура для архива (купленные товары)"""
    keyboard = []
    for item in items:
        if len(item) >= 6:
            item_id, text, bought_at, created_at, bought_by, added_by = item
            btn_text = f"{text[:20]}" if len(text) <= 20 else f"{text[:17]}..."
            keyboard.append([
                InlineKeyboardButton(f"↩️ {btn_text}", callback_data=f"restore_archive_{item_id}")
            ])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_keyboard():
    """Админ-панель БЕЗ рассылки дайджеста и очистки корзины"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👪 Пригласить", callback_data="admin_invite")],
        [InlineKeyboardButton("✏️ Изменить название семьи", callback_data="admin_rename")],
        [InlineKeyboardButton("👥 Участники", callback_data="admin_members")],
        [InlineKeyboardButton("🔄 Обновить шаблоны", callback_data="admin_update_templates")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])

def get_members_keyboard(members, family_id: int, current_user_id: int):
    """Клавиатура для управления участниками"""
    keyboard = []
    for member in members:
        name = member['family_display_name'] or f"User{member['telegram_id']}"
        role = " 👑" if member['is_admin'] else ""

        if member['id'] != current_user_id:
            keyboard.append([
                InlineKeyboardButton(f"{name}{role}", callback_data=f"member_{member['id']}"),
                InlineKeyboardButton("❌", callback_data=f"remove_{member['id']}")
            ])
            if member['is_admin']:
                keyboard[-1].append(InlineKeyboardButton("⬇️", callback_data=f"demote_{member['id']}"))
            else:
                keyboard[-1].append(InlineKeyboardButton("⬆️", callback_data=f"promote_{member['id']}"))
        else:
            keyboard.append([
                InlineKeyboardButton(f"{name}{role} (Вы)", callback_data="none")
            ])

    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)

def get_invite_keyboard(invite_code: str):
    """Клавиатура для приглашения"""
    invite_link = f"https://t.me/share/url?url=Присоединяйся%20к%20семье!%20Используй%20код:%20{invite_code}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📱 Поделиться приглашением", url=invite_link)],
        [InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]
    ])

def get_back_keyboard():
    """Простая кнопка назад"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])

def get_cancel_keyboard():
    """Кнопка отмены"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Отмена", callback_data="admin_panel")]
    ])

# ==================== ОБРАБОТЧИКИ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    try:
        user = update.effective_user
        user_id, family_id, is_admin, display_name = db.get_or_create_user(
            user.id, user.username, user.full_name
        )

        if family_id:
            # Получаем название семьи
            family_name = db.get_family_name(family_id)
            
            # Получаем последние действия
            recent_activities_text = get_recent_activities_text(family_id)
            
            # Новое приветствие БЕЗ подсказок
            welcome_text = f"👋 *{display_name}, добро пожаловать в семью {family_name}!*"
            welcome_text += recent_activities_text
            
            await update.message.reply_text(
                welcome_text,
                reply_markup=get_main_keyboard(family_id, is_admin),
                parse_mode='Markdown'
            )
        else:
            # Нет семьи - предлагаем создать или присоединиться
            await update.message.reply_text(
                f"Привет, {user.first_name}! 👋\n\n"
                "У тебя пока нет семьи.\n"
                "Создай новую или присоединись по приглашению.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Создать семью", callback_data="create_family")],
                    [InlineKeyboardButton("🔗 Присоединиться по коду", callback_data="join_family")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка в start: {e}")

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений (обычные товары)"""
    try:
        user = update.effective_user
        user_id, family_id, is_admin, display_name = db.get_or_create_user(user.id)

        text = update.message.text.strip()

        # Проверяем, не команда ли это
        if text.startswith('/'):
            return

        # Проверяем, ожидаем ли мы новое название семьи
        if context.user_data.get('awaiting_new_family_name'):
            new_name = text[:50]

            if not new_name:
                await update.message.reply_text(
                    "Название не может быть пустым. Попробуйте еще раз:",
                    reply_markup=get_cancel_keyboard()
                )
                return

            success = db.update_family_name(family_id, new_name)

            if success:
                await update.message.reply_text(
                    f"✅ Название семьи изменено на: *{new_name}*",
                    parse_mode='Markdown',
                    reply_markup=get_admin_keyboard()
                )
            else:
                await update.message.reply_text(
                    "❌ Ошибка при изменении названия.",
                    reply_markup=get_admin_keyboard()
                )

            context.user_data.pop('awaiting_new_family_name', None)
            return

        # Если пользователь не в семье, проверяем код приглашения
        if not family_id:
            # Код приглашения должен быть 6-8 символов, буквы/цифры
            if len(text) in [6, 7, 8] and text.isalnum():
                family = db.get_family_by_invite_code(text.upper())
                if family:
                    family_id = family['id']
                    db.add_user_to_family(user_id, family_id)
                    
                    # Сохраняем family_id в context для следующего шага
                    context.user_data['joining_family_id'] = family_id
                    context.user_data['joining_family_name'] = family['name']

                    # Запрашиваем имя для семьи
                    await update.message.reply_text(
                        f"✅ Вы присоединились к '{family['name']}'!\n\n"
                        "📝 *Введите ваше имя, которое будут видеть другие участники семьи:*\n"
                        "(Можно использовать ваше настоящее имя или никнейм)",
                        parse_mode='Markdown'
                    )
                    return ASKING_USER_NAME
                else:
                    await update.message.reply_text(
                        "❌ Неверный код приглашения!",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🏠 Создать семью", callback_data="create_family")],
                            [InlineKeyboardButton("🔗 Присоединиться", callback_data="join_family")]
                        ])
                    )
                    return ConversationHandler.END
            else:
                # Не код приглашения и не в семье - просим создать семью
                await update.message.reply_text(
                    "У вас еще нет семьи. Используйте /start чтобы создать семью или присоединиться.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🏠 Создать семью", callback_data="create_family")],
                        [InlineKeyboardButton("🔗 Присоединиться", callback_data="join_family")]
                    ])
                )
            return ConversationHandler.END

        # Если пользователь в семье - добавляем товар(ы)
        items = split_multiline_items(text)

        if len(items) == 1:
            # Один товар
            item_id = db.add_shopping_item(family_id, user_id, items[0])
            if item_id:
                await update.message.reply_text(
                    f"✅ Добавлено: *{items[0]}*",
                    parse_mode='Markdown',
                    reply_markup=get_main_keyboard(family_id, is_admin)
                )
        elif len(items) > 1:
            # Несколько товаров
            added_count = db.add_multiple_items(family_id, user_id, items)
            if added_count:
                await update.message.reply_text(
                    f"✅ Добавлено *{added_count}* товаров!",
                    parse_mode='Markdown',
                    reply_markup=get_main_keyboard(family_id, is_admin)
                )

    except Exception as e:
        logger.error(f"Ошибка в handle_text_message: {e}")
        return ConversationHandler.END

async def ask_family_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает название семьи"""
    query = update.callback_query
    await query.answer()

    await query.edit_message_text(
        "🏠 *Создание новой семьи*\n\n"
        "📝 Введите название для вашей семьи (до 50 символов):\n"
        "Пример: 'Семья Ивановых', 'Наша квартира', 'Комната 404'",
        parse_mode='Markdown'
    )

    return ASKING_FAMILY_NAME

async def handle_family_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод названия семьи (только для создания семьи)"""
    family_name = update.message.text.strip()[:50]

    if not family_name:
        await update.message.reply_text(
            "Название не может быть пустым. Попробуйте еще раз:"
        )
        return ASKING_FAMILY_NAME

    # Создаем семью
    user = update.effective_user
    user_id, _, _, _ = db.get_or_create_user(user.id)

    family_id, invite_code = db.create_family(family_name)

    if family_id:
        # Сохраняем данные в context для следующего шага
        context.user_data['new_family_id'] = family_id
        context.user_data['new_family_name'] = family_name
        context.user_data['new_invite_code'] = invite_code
        
        # Делаем пользователя админом
        db.add_user_to_family(user_id, family_id, is_admin=True)

        await update.message.reply_text(
            f"✅ Семья '{family_name}' создана!\n\n"
            "📝 *Введите ваше имя, которое будут видеть другие участники семьи:*\n"
            "(Можно использовать ваше настоящее имя или никнейм)",
            parse_mode='Markdown'
        )
        return ASKING_USER_NAME
    else:
        await update.message.reply_text(
            "❌ Ошибка при создании семьи. Попробуйте еще раз:"
        )
        return ASKING_FAMILY_NAME

async def handle_user_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод имени пользователя для семьи"""
    user_name = update.message.text.strip()[:30]

    if not user_name:
        await update.message.reply_text(
            "Имя не может быть пустым. Введите ваше имя:"
        )
        return ASKING_USER_NAME

    user = update.effective_user
    user_id, family_id, is_admin, _ = db.get_or_create_user(user.id)

    # Получаем family_id из context (для создания или присоединения)
    if 'new_family_id' in context.user_data:
        family_id = context.user_data['new_family_id']
        family_name = context.user_data['new_family_name']
        invite_code = context.user_data['new_invite_code']
    elif 'joining_family_id' in context.user_data:
        family_id = context.user_data['joining_family_id']
        family_name = context.user_data['joining_family_name']
        
        # Добавляем пользователя в семью (если еще не добавлен)
        db.add_user_to_family(user_id, family_id)
        
        # Получаем invite_code семьи
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT invite_code FROM families WHERE id = ?',
                (family_id,)
            )
            family = cursor.fetchone()
            invite_code = family['invite_code'] if family else "???"
    else:
        # Не нашли family_id в context - ошибка
        await update.message.reply_text(
            "❌ Ошибка при обработке. Попробуйте снова: /start"
        )
        context.user_data.clear()
        return ConversationHandler.END

    if family_id and db.update_user_display_name(user_id, user_name):
        # Получаем актуальный is_admin
        user_id, family_id, is_admin, _ = db.get_or_create_user(user.id)
        
        # Получаем последние действия для приветствия
        recent_activities_text = get_recent_activities_text(family_id)
        
        # Новое приветствие БЕЗ подсказок
        welcome_text = f"🎉 Отлично, {user_name}!\n\n"
        welcome_text += f"Вы в семье *'{family_name}'*."
        welcome_text += recent_activities_text
        welcome_text += f"\n\n🔑 *Код приглашения:* `{invite_code}`"

        await update.message.reply_text(
            welcome_text,
            parse_mode='Markdown',
            reply_markup=get_main_keyboard(family_id, is_admin)
        )

    context.user_data.clear()
    return ConversationHandler.END

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик inline-кнопок"""
    query = update.callback_query
    await query.answer()

    data = query.data
    user = query.from_user
    user_id, family_id, is_admin, display_name = db.get_or_create_user(user.id)

    # Основные действия
    if data == "back_to_main":
        if family_id:
            family_name = db.get_family_name(family_id)
            recent_activities_text = get_recent_activities_text(family_id)
            
            welcome_text = f"👋 *{display_name}, добро пожаловать в семью {family_name}!*"
            welcome_text += recent_activities_text
            
            await query.edit_message_text(
                welcome_text,
                parse_mode='Markdown',
                reply_markup=get_main_keyboard(family_id, is_admin)
            )
        else:
            await query.edit_message_text(
                "У вас нет семьи. Используйте /start",
                reply_markup=get_back_keyboard()
            )

    elif data == "show_list":
        if not family_id:
            await query.edit_message_text(
                "У вас нет семьи. Используйте /start",
                reply_markup=get_back_keyboard()
            )
            return

        items = db.get_active_items_with_users(family_id)
        if not items:
            await query.edit_message_text(
                "📭 *Список покупок пуст!*\n\n"
                "Просто напишите товар в чат, чтобы добавить его.\n"
                "Можно добавить несколько товаров через перенос строки.",
                parse_mode='Markdown',
                reply_markup=get_back_keyboard()
            )
        else:
            text = "📃 *Список покупок:*\n\n"
            for i, item in enumerate(items, 1):
                if len(item) >= 4:
                    item_id, item_text, created_at, user_name = item
                    time_str = format_time(created_at)
                    # Товар жирным, остальное обычным
                    text += f"{i}. {format_item_text(item_text)} ({user_name}, {time_str})\n"

            await query.edit_message_text(
                text,
                parse_mode='Markdown',
                reply_markup=get_list_keyboard(items, context)
            )

    elif data == "show_archive":
        if not family_id:
            await query.edit_message_text(
                "У вас нет семьи. Используйте /start",
                reply_markup=get_back_keyboard()
            )
            return

        items = db.get_archive_items_with_users(family_id, 20)
        if not items:
            await query.edit_message_text(
                "🛒 *Купленные товары*\n\n"
                "Здесь появятся товары, которые вы отметите как купленные.",
                parse_mode='Markdown',
                reply_markup=get_back_keyboard()
            )
        else:
            text = "🛒 *Купленные товары:*\n\n"
            for i, item in enumerate(items, 1):
                if len(item) >= 6:
                    item_id, item_text, bought_at, created_at, bought_by, added_by = item
                    time_str = format_time(bought_at)
                    text += f"{i}. {format_item_text(item_text)}\n   👤 {added_by} → {bought_by}, {time_str}\n"

            await query.edit_message_text(
                text,
                parse_mode='Markdown',
                reply_markup=get_archive_keyboard(items, is_admin)
            )

    elif data.startswith("buy_"):
        if not family_id:
            return

        item_id = int(data.split("_")[1])
        success = db.mark_item_as_bought(item_id, user_id)

        if success:
            # Благодарственная фраза на 3 секунды
            thankyou = get_random_thankyou()
            await query.answer(thankyou, show_alert=False, cache_time=3)

            # Обновляем список
            items = db.get_active_items_with_users(family_id)
            if items:
                text = "📃 *Список покупок:*\n\n"
                for i, item in enumerate(items, 1):
                    if len(item) >= 4:
                        item_id, item_text, created_at, user_name = item
                        time_str = format_time(created_at)
                        text += f"{i}. {format_item_text(item_text)} ({user_name}, {time_str})\n"

                await query.edit_message_text(
                    text,
                    parse_mode='Markdown',
                    reply_markup=get_list_keyboard(items, context)
                )
            else:
                await query.edit_message_text(
                    "📭 *Список покупок пуст!*\n\n"
                    "Просто напишите товар в чат, чтобы добавить его.",
                    parse_mode='Markdown',
                    reply_markup=get_back_keyboard()
                )

    elif data.startswith("ask_delete_"):
        if not family_id:
            return

        item_id = int(data.split("_")[2])
        context.user_data['pending_delete_item_id'] = item_id
        
        # Получаем информацию о товаре
        items = db.get_active_items_with_users(family_id)
        item_text = ""
        for item in items:
            if item[0] == item_id:
                item_text = item[1]
                break
        
        await query.edit_message_text(
            f"🗑️ *Подтверждение удаления*\n\n"
            f"Вы уверены, что хотите удалить товар:\n"
            f"**{item_text}**\n\n"
            f"⚠️ *Это действие нельзя отменить!*",
            parse_mode='Markdown',
            reply_markup=get_confirmation_keyboard(item_id)
        )

    elif data.startswith("confirm_delete_"):
        if not family_id:
            return

        item_id = int(data.split("_")[2])
        success = db.delete_item_permanently(item_id, user_id)

        if success:
            await query.answer("✅ Товар удален навсегда", show_alert=False)
            
            items = db.get_active_items_with_users(family_id)
            if items:
                text = "📃 *Список покупок:*\n\n"
                for i, item in enumerate(items, 1):
                    if len(item) >= 4:
                        item_id, item_text, created_at, user_name = item
                        time_str = format_time(created_at)
                        text += f"{i}. {format_item_text(item_text)} ({user_name}, {time_str})\n"

                await query.edit_message_text(
                    text,
                    parse_mode='Markdown',
                    reply_markup=get_list_keyboard(items, context)
                )
            else:
                await query.edit_message_text(
                    "📭 *Список покупок пуст!*\n\n"
                    "Просто напишите товар в чат, чтобы добавить его.",
                    parse_mode='Markdown',
                    reply_markup=get_back_keyboard()
                )
        else:
            await query.answer("❌ Ошибка при удалении", show_alert=True)

    elif data == "cancel_delete":
        # Возвращаемся к списку покупок
        if not family_id:
            return

        items = db.get_active_items_with_users(family_id)
        if items:
            text = "📃 *Список покупок:*\n\n"
            for i, item in enumerate(items, 1):
                if len(item) >= 4:
                    item_id, item_text, created_at, user_name = item
                    time_str = format_time(created_at)
                    text += f"{i}. {format_item_text(item_text)} ({user_name}, {time_str})\n"

            await query.edit_message_text(
                text,
                parse_mode='Markdown',
                reply_markup=get_list_keyboard(items, context)
            )
        else:
            await query.edit_message_text(
                "📭 *Список покупок пуст!*\n\n"
                "Просто напишите товар в чат, чтобы добавить его.",
                parse_mode='Markdown',
                reply_markup=get_back_keyboard()
            )

    elif data.startswith("restore_archive_"):
        if not family_id:
            return

        item_id = int(data.split("_")[2])
        # Временно отключаем восстановление из архива
        await query.answer("❌ Восстановление временно отключено", show_alert=True)

    elif data == "show_stats":
        if not family_id:
            await query.edit_message_text(
                "У вас нет семьи. Используйте /start",
                reply_markup=get_back_keyboard()
            )
            return

        # Статистика за последние 30 дней
        now_msk = get_moscow_time()
        thirty_days_ago = now_msk - timedelta(days=30)
        stats_30_days = db.get_monthly_stats(family_id, thirty_days_ago.year, thirty_days_ago.month)
        
        # Общая статистика за все время
        all_time_stats = db.get_all_time_stats(family_id)

        text = f"📊 *Статистика покупок*\n\n"
        
        # За последние 30 дней
        text += f"*📅 За последние 30 дней:*\n"
        if stats_30_days['total_items'] > 0:
            text += f"Всего куплено: *{stats_30_days['total_items']}*\n"
            text += f"Покупателей: *{stats_30_days['unique_buyers']}*\n"
            text += f"Уникальных товаров: *{stats_30_days['unique_items']}*\n\n"
            
            if stats_30_days['top_items_month']:
                text += "*Топ-5 товаров за 30 дней:*\n"
                for i, item_stat in enumerate(stats_30_days['top_items_month'][:5], 1):
                    text += f"{i}. {item_stat['normalized_text'].capitalize()}: {item_stat['count']}\n"
            
            if stats_30_days['user_stats_month']:
                text += "\n*Активность за 30 дней:*\n"
                for i, user_stat in enumerate(stats_30_days['user_stats_month'][:5], 1):
                    text += f"{i}. {user_stat['family_display_name']}: добавил {user_stat['added_count']}, купил {user_stat['bought_count']}\n"
        else:
            text += "Покупок не было\n\n"
        
        # За все время
        text += f"\n---\n*📈 За все время:*\n"
        text += f"Активных товаров: *{all_time_stats['active_items']}*\n"
        text += f"Всего куплено: *{all_time_stats['bought_items']}*\n\n"
        
        if all_time_stats['top_items_all_time']:
            text += "*Топ-10 товаров за все время:*\n"
            for i, item_stat in enumerate(all_time_stats['top_items_all_time'][:10], 1):
                text += f"{i}. {item_stat['normalized_text'].capitalize()}: {item_stat['count']}\n"
        
        if all_time_stats['user_stats_all_time']:
            text += "\n*Общая активность:*\n"
            for i, user_stat in enumerate(all_time_stats['user_stats_all_time'][:5], 1):
                current = user_stat['currently_added'] or 0
                text += f"{i}. {user_stat['family_display_name']}: добавил {user_stat['added_count']}, купил {user_stat['bought_count']}, сейчас в списке: {current}\n"

        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )

    elif data == "admin_panel":
        if not family_id or not is_admin:
            await query.answer("Только для администраторов!", show_alert=True)
            return

        await query.edit_message_text(
            "👑 *Панель администратора*\n\n"
            "Здесь вы можете управлять семьей:",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )

    elif data == "admin_invite":
        if not family_id or not is_admin:
            return

        # Получаем код приглашения
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT invite_code, name FROM families WHERE id = ?',
                (family_id,)
            )
            family = cursor.fetchone()

        if family:
            text = f"👪 *Приглашение в семью '{family['name']}'*\n\n"
            text += f"Код для присоединения:\n`{family['invite_code']}`\n\n"
            text += "Поделитесь этим кодом с членами семьи."

            await query.edit_message_text(
                text,
                parse_mode='Markdown',
                reply_markup=get_invite_keyboard(family['invite_code'])
            )

    elif data == "admin_rename":
        if not family_id or not is_admin:
            return

        # Устанавливаем флаг, что ожидаем новое название
        context.user_data['awaiting_new_family_name'] = True

        await query.edit_message_text(
            "✏️ *Изменение названия семьи*\n\n"
            "Введите новое название для семьи (до 50 символов):",
            parse_mode='Markdown',
            reply_markup=get_cancel_keyboard()
        )

    elif data == "admin_members":
        if not family_id or not is_admin:
            return

        members = db.get_family_members(family_id)

        if not members:
            await query.edit_message_text(
                "👥 *Участники семьи*\n\n"
                "В семье пока только вы.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]
                ])
            )
            return

        text = "👥 *Участники семьи:*\n\n"
        for member in members:
            role = "👑 " if member['is_admin'] else "👤 "
            name = member['family_display_name'] or f"User{member['telegram_id']}"
            text += f"{role}{name}\n"

        text += "\n❌ - исключить, ⬆️ - сделать админом, ⬇️ - убрать админа"

        await query.edit_message_text(
            text,
            parse_mode='Markdown',
            reply_markup=get_members_keyboard(members, family_id, user_id)
        )

    elif data.startswith("remove_"):
        if not family_id or not is_admin:
            return

        member_id = int(data.split("_")[1])
        success = db.remove_user_from_family(member_id, family_id)

        if success:
            await query.answer("Участник исключен", show_alert=True)
            # Возвращаемся к списку участников
            members = db.get_family_members(family_id)

            if members:
                text = "👥 *Участники семьи:*\n\n"
                for member in members:
                    role = "👑 " if member['is_admin'] else "👤 "
                    name = member['family_display_name'] or f"User{member['telegram_id']}"
                    text += f"{role}{name}\n"

                text += "\n❌ - исключить, ⬆️ - сделать админом, ⬇️ - убрать админа"

                await query.edit_message_text(
                    text,
                    parse_mode='Markdown',
                    reply_markup=get_members_keyboard(members, family_id, user_id)
                )
            else:
                await query.edit_message_text(
                    "👥 *Участники семьи*\n\n"
                    "В семье пока только вы.",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]
                    ])
                )

    elif data.startswith("promote_"):
        if not family_id or not is_admin:
            return

        member_id = int(data.split("_")[1])
        success = db.transfer_admin_rights(family_id, user_id, member_id)

        if success:
            await query.answer("Права администратора переданы", show_alert=True)
            is_admin = False  # Теперь текущий пользователь не админ

            members = db.get_family_members(family_id)

            if members:
                text = "👥 *Участники семьи:*\n\n"
                for member in members:
                    role = "👑 " if member['is_admin'] else "👤 "
                    name = member['family_display_name'] or f"User{member['telegram_id']}"
                    text += f"{role}{name}\n"

                text += "\n❌ - исключить, ⬆️ - сделать админом, ⬇️ - убрать админа"

                await query.edit_message_text(
                    text,
                    parse_mode='Markdown',
                    reply_markup=get_members_keyboard(members, family_id, user_id)
                )

    elif data.startswith("demote_"):
        if not family_id or not is_admin:
            return

        member_id = int(data.split("_")[1])
        # Для понижения просто снимаем флаг админа
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE users SET is_admin = FALSE WHERE id = ? AND family_id = ?',
                (member_id, family_id)
            )
            conn.commit()

        await query.answer("Админские права сняты", show_alert=True)

        members = db.get_family_members(family_id)

        if members:
            text = "👥 *Участники семьи:*\n\n"
            for member in members:
                role = "👑 " if member['is_admin'] else "👤 "
                name = member['family_display_name'] or f"User{member['telegram_id']}"
                text += f"{role}{name}\n"

            text += "\n❌ - исключить, ⬆️ - сделать админом, ⬇️ - убрать админа"

            await query.edit_message_text(
                text,
                parse_mode='Markdown',
                reply_markup=get_members_keyboard(members, family_id, user_id)
            )

    elif data == "admin_update_templates":
        if not family_id or not is_admin:
            return

        # Обновляем шаблоны на основе часто покупаемых товаров
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Получаем топ-4 часто покупаемых товара за все время
                cursor.execute('''
                    SELECT LOWER(text) as normalized_text, COUNT(*) as count
                    FROM archive_items
                    WHERE family_id = ?
                    GROUP BY normalized_text
                    ORDER BY count DESC
                    LIMIT ?
                ''', (family_id, TEMPLATES_COUNT))
                
                top_items = cursor.fetchall()
                
                if top_items:
                    # Очищаем старые шаблоны
                    cursor.execute('DELETE FROM templates WHERE family_id = ?', (family_id,))
                    
                    # Добавляем новые шаблоны с оригинальным регистром (берем первое вхождение)
                    for item in top_items:
                        cursor.execute('''
                            SELECT text FROM archive_items 
                            WHERE family_id = ? AND LOWER(text) = ?
                            LIMIT 1
                        ''', (family_id, item['normalized_text']))
                        
                        original_text = cursor.fetchone()
                        if original_text:
                            cursor.execute('''
                                INSERT INTO templates (family_id, item_text)
                                VALUES (?, ?)
                            ''', (family_id, original_text['text']))
                    
                    conn.commit()
                    
                    new_templates = [item['normalized_text'].capitalize() for item in top_items]
                    await query.answer(f"Шаблоны обновлены: {', '.join(new_templates)}", show_alert=True)
                else:
                    await query.answer("Недостаточно данных для обновления шаблонов", show_alert=True)
                
        except Exception as e:
            logger.error(f"Ошибка при обновлении шаблонов: {e}")
            await query.answer("Ошибка при обновлении шаблонов", show_alert=True)
        
        await query.edit_message_text(
            "🔄 *Обновление шаблонов*\n\n"
            "Шаблоны обновлены на основе часто покупаемых товаров.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]
            ])
        )

    elif data.startswith("template_"):
        if not family_id:
            return

        template_text = data.split("_", 1)[1]
        item_id = db.add_shopping_item(family_id, user_id, template_text)

        if item_id:
            await query.answer(f"Добавлено: {template_text}", show_alert=False)
            await query.edit_message_text(
                f"✅ Добавлено из шаблона: *{template_text}*",
                parse_mode='Markdown',
                reply_markup=get_main_keyboard(family_id, is_admin)
            )

    elif data == "create_family":
        await ask_family_name(update, context)
        return ASKING_FAMILY_NAME

    elif data == "join_family":
        await query.edit_message_text(
            "🔗 *Присоединение к семье*\n\n"
            "Введите код приглашения (6-8 символов):",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
            ])
        )

# ==================== ГЛАВНАЯ ФУНКЦИЯ ====================

def main():
    print("="*60)
    print("🤖 Запуск ОБНОВЛЕННОГО бота для списка покупок...")
    print("="*60)
    print("✅ Изменения:")
    print("1. Товары выделены жирным шрифтом")
    print("2. Работающая статистика (топ-5 за 30 дней, топ-10 за всё время)")
    print("3. Убрана рассылка дайджеста")
    print("4. Благодарственные фразы на 3 секунды после покупки")
    print("5. Регистронезависимость через COLLATE NOCASE")
    print("6. Новое приветствие без аннотаций + последние 5 действий")
    print("7. Убрана корзина (удаление навсегда с подтверждением)")
    print("8. Кнопка удаления занимает 1/4 строки")
    print("="*60)

    if "ВАШ_ТОКЕН_ЗДЕСЬ" in BOT_TOKEN:
        print("⚠️  ВНИМАНИЕ: Замените BOT_TOKEN на ваш реальный токен!")
        print("Или установите переменную окружения TELEGRAM_BOT_TOKEN")
        print("="*60)

    try:
        # Создаем Application
        from telegram.ext import ApplicationBuilder
        app = ApplicationBuilder().token(BOT_TOKEN).build()

        # ConversationHandler для создания семьи и присоединения
        conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(ask_family_name, pattern="^create_family$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message),
            ],
            states={
                ASKING_FAMILY_NAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_family_name)
                ],
                ASKING_USER_NAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_name)
                ],
            },
            fallbacks=[
                CommandHandler("start", start),
                CallbackQueryHandler(button_handler, pattern="^back_to_main$"),
            ],
        )

        # Регистрируем обработчики в правильном порядке
        app.add_handler(CommandHandler("start", start))
        app.add_handler(conv_handler)  # ConversationHandler
        app.add_handler(CallbackQueryHandler(button_handler))  # Обычные кнопки

        print("✅ Бот инициализирован успешно!")
        print("📱 Используйте /start в Telegram")
        print("="*60)

        # Запускаем бота
        app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    # Очистка старых процессов (опционально)
    import sys
    if '--kill' in sys.argv:
        import subprocess
        subprocess.run(["pkill", "-f", "python.*shopping_bot"])
        print("⚠️ Все процессы бота завершены")
    else:
        main()
