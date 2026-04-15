import os
import re
import asyncio
import logging
import random
import sqlite3
import uuid
import aiohttp
import hashlib
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Set, Tuple, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    ChatPermissions,
    Message,
    ChatMemberUpdated,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton
)
from aiogram.enums import ChatMemberStatus, ChatType
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from logging.handlers import RotatingFileHandler
from aiogram.types import FSInputFile
from aiohttp import web

# Создаем необходимые директории
os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# Настройка логирования для хостинга
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler("logs/safe_deal_bot.log", maxBytes=10485760, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация бота
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "7461610956")
ADMIN_IDS = [int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',') if admin_id.strip().isdigit()]
REVIEWS_CHANNEL = os.getenv("REVIEWS_CHANNEL", "@ReviewsSafeDeal")
BOT_USERNAME = os.getenv("BOT_USERNAME", "SafeDealGuardBot")

# ЮMoney конфигурация
YOO_MONEY_CLIENT_ID = os.getenv("YOO_MONEY_CLIENT_ID")
YOO_MONEY_CLIENT_SECRET = os.getenv("YOO_MONEY_CLIENT_SECRET")
YOO_MONEY_ACCOUNT = os.getenv("YOO_MONEY_ACCOUNT")
YOO_MONEY_ACCESS_TOKEN = os.getenv("YOO_MONEY_ACCESS_TOKEN")

# Комиссия гаранта (8%)
GUARANTOR_FEE_PERCENT = 0.08

# Состояния FSM
class DealStates(StatesGroup):
    DEAL_ROLE = State()
    DEAL_AMOUNT = State()
    DEAL_DESCRIPTION = State()
    DEAL_DEADLINE = State()
    DEAL_PARTNER = State()
    DEAL_GROUP_LINK = State()  # Новое состояние для ссылки на группу
    CONFIRM_DEAL = State()
    REVIEW_TEXT = State()
    SERVICE_REVIEW_TEXT = State()  # Новое состояние для отзыва о сервисе
    WAITING_FOR_GROUP = State()
    WITHDRAWAL_WALLET = State()

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

def adapt_datetime(dt):
    """Адаптер для преобразования datetime в строку для SQLite"""
    return dt.isoformat()

def convert_datetime(text):
    """Конвертер для преобразования строки в datetime из SQLite"""
    return datetime.fromisoformat(text.decode() if isinstance(text, bytes) else text)

# Регистрируем адаптеры для SQLite
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("TIMESTAMP", convert_datetime)

def get_db_connection():
    """Создает соединение с базой данных с поддержкой datetime"""
    conn = sqlite3.connect(
        "data/safe_deal_bot.db", 
        detect_types=sqlite3.PARSE_DECLTYPES
    )
    conn.row_factory = sqlite3.Row
    return conn

# Обновите функцию init_db()
def init_db():
    db_path = "data/safe_deal_bot.db"
    conn = get_db_connection()
    cursor = conn.cursor()

    # Таблица сделок
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deals (
            id TEXT PRIMARY KEY,
            creator_id INTEGER,
            creator_role TEXT,
            buyer_id INTEGER,
            seller_id INTEGER,
            buyer_username TEXT,
            seller_username TEXT,
            amount REAL,
            description TEXT,
            deadline_days INTEGER,
            created_at TIMESTAMP,
            status TEXT,
            private_chat_id TEXT,
            buyer_confirmed BOOLEAN DEFAULT FALSE,
            seller_confirmed BOOLEAN DEFAULT FALSE,
            payment_sent BOOLEAN DEFAULT FALSE,
            work_completed BOOLEAN DEFAULT FALSE,
            payment_confirmed BOOLEAN DEFAULT FALSE,
            dispute_opened BOOLEAN DEFAULT FALSE,
            payment_url TEXT,
            total_amount REAL,
            guarantor_fee REAL,
            group_chat_id TEXT,
            group_created BOOLEAN DEFAULT FALSE,
            group_link TEXT,
            buyer_reviewed BOOLEAN DEFAULT FALSE,
            seller_reviewed BOOLEAN DEFAULT FALSE
        )
    ''')
    
    # Таблица отзывов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id TEXT,
            reviewer_id INTEGER,
            reviewed_user_id INTEGER,
            review_text TEXT,
            rating INTEGER,
            created_at TIMESTAMP,
            FOREIGN KEY (deal_id) REFERENCES deals (id)
        )
    ''')
    
    # Таблица пользователей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at TIMESTAMP,
            balance REAL DEFAULT 0.0
        )
    ''')
    
    # Таблица выводов средств
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount REAL,
            status TEXT,
            created_at TIMESTAMP,
            processed_at TIMESTAMP,
            yoomoney_wallet TEXT
        )
    ''')
    
    # Таблица отзывов о сервисе
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS service_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reviewer_id INTEGER,
            review_text TEXT,
            rating INTEGER,
            created_at TIMESTAMP
        )
    ''')

    # Проверяем и добавляем отсутствующие колонки в таблицу users
    try:
        cursor.execute("SELECT balance FROM users LIMIT 1")
    except sqlite3.OperationalError:
        # Колонка balance отсутствует, добавляем её
        logger.info("Добавляем колонку balance в таблицу users")
        cursor.execute("ALTER TABLE users ADD COLUMN balance REAL DEFAULT 0.0")

    # Индексы для улучшения производительности
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_deals_creator ON deals(creator_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_deals_buyer ON deals(buyer_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_deals_seller ON deals(seller_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_deal ON reviews(deal_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(reviewed_user_id)")

    conn.commit()
    conn.close()

init_db()

class YooMoneyAPI:
    def __init__(self, client_id: str, client_secret: str, account: str, access_token: str = ""):
        self.client_id = client_id
        self.client_secret = client_secret
        self.account = account
        self.access_token = access_token
        self.base_url = "https://yoomoney.ru/api"

    async def create_payment(self, amount: float, deal_id: str, description: str = "") -> Optional[str]:
        """Создание платежа"""
        try:
            logger.info(f"Создание платежа для сделки {deal_id}, сумма: {amount}")
            
            total_amount = amount * (1 + GUARANTOR_FEE_PERCENT)
            
            payment_params = {
                'receiver': self.account,
                'quickpay-form': 'shop',
                'sum': total_amount,
                'label': f"deal_{deal_id}",
                'targets': f"Сделка {deal_id}",
                'comment': description[:100] if description else "",
                'need-fio': 'false',
                'need-email': 'false',
                'need-phone': 'false',
                'need-address': 'false',
                'paymentType': 'AC'
            }
            
            from urllib.parse import urlencode
            payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?{urlencode(payment_params)}"
            
            logger.info(f"Payment URL created for deal {deal_id}: {total_amount} RUB")
            return payment_url
                        
        except Exception as e:
            logger.error(f"Error creating payment: {e}")
            return None

    async def check_payment(self, deal_id: str) -> bool:
        """АВТОМАТИЧЕСКАЯ проверка оплаты через API ЮMoney с OAuth токеном"""
        try:
            logger.info(f"АВТОМАТИЧЕСКАЯ проверка оплаты для сделки {deal_id}")
            
            if not self.access_token:
                logger.error("❌ OAuth токен не установлен")
                return False
            
            async with aiohttp.ClientSession() as session:
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                
                params = {
                    "label": f"deal_{deal_id}",
                    "type": "deposition",
                    "records": 10,
                    "details": "true"
                }
                
                async with session.get(
                    f"{self.base_url}/operation-history",
                    headers=headers,
                    params=params
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        operations = data.get('operations', [])
                        
                        logger.info(f"Найдено операций: {len(operations)} для сделки {deal_id}")
                        
                        for operation in operations:
                            if (operation.get('label') == f"deal_{deal_id}" and 
                                operation.get('status') == 'success' and 
                                operation.get('direction') == 'in'):
                                
                                logger.info(f"Платеж ПОДТВЕРЖДЕН для сделки {deal_id}: {operation}")
                                return True
                        
                        logger.info(f"Платеж НЕ найден для сделки {deal_id}")
                        return False
                    else:
                        error_text = await response.text()
                        logger.error(f"API error: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error checking payment for deal {deal_id}: {e}")
            return False

    async def get_account_info(self):
        """Получение информации об аккаунте для проверки подключения"""
        try:
            if not self.access_token:
                logger.error("❌ OAuth токен не установлен")
                return None
            
            async with aiohttp.ClientSession() as session:
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                
                async with session.post(
                    f"{self.base_url}/account-info",
                    headers=headers
                ) as response:
                    
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Account info error: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error getting account info: {e}")
            return None

    async def make_payout(self, wallet: str, amount: float) -> bool:
        """Вывод средств на кошелек ЮMoney"""
        try:
            logger.info(f"Вывод средств {amount} RUB на кошелек {wallet}")
            
            if not self.access_token:
                logger.error("❌ OAuth токен не установлен")
                return False
            
            async with aiohttp.ClientSession() as session:
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                
                data = {
                    'pattern': 'p2p',
                    'to': wallet,
                    'amount': amount,
                    'comment': 'Вывод средств из OnlineSoprovod',
                    'message': 'Вывод средств',
                    'label': f"payout_{int(datetime.now().timestamp())}"
                }
                
                async with session.post(
                    f"{self.base_url}/request-payment",
                    headers=headers,
                    data=data
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        request_id = result.get('request_id')
                        
                        if request_id:
                            # Подтверждаем перевод
                            confirm_data = {
                                'request_id': request_id
                            }
                            
                            async with session.post(
                                f"{self.base_url}/process-payment",
                                headers=headers,
                                data=confirm_data
                            ) as confirm_response:
                                
                                if confirm_response.status == 200:
                                    logger.info(f"Payout successful: {amount} RUB to {wallet}")
                                    return True
                    
                    logger.error(f"Payout failed: {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error making payout: {e}")
            return False

    async def test_connection(self) -> bool:
        """Тестирование подключения к API ЮMoney"""
        try:
            account_info = await self.get_account_info()
            if account_info:
                logger.info(f"✅ Подключение к ЮMoney успешно! Баланс: {account_info.get('balance', 'N/A')}")
                return True
            else:
                logger.error("❌ Ошибка подключения к ЮMoney")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка тестирования подключения: {e}")
            return False

yoo_money = YooMoneyAPI(
    YOO_MONEY_CLIENT_ID, 
    YOO_MONEY_CLIENT_SECRET, 
    YOO_MONEY_ACCOUNT, 
    YOO_MONEY_ACCESS_TOKEN  # Добавьте этот параметр
)

# Функции для работы с базой данных
def get_db_connection():
    """Создает соединение с базой данных"""
    return sqlite3.connect("data/safe_deal_bot.db")

def save_user(user_id: int, username: str, first_name: str, last_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    # Сохраняем username в нижнем регистре для consistent поиска
    normalized_username = username.lower() if username else None
    cursor.execute('''
        INSERT OR REPLACE INTO users 
        (user_id, username, first_name, last_name, created_at, balance)
        VALUES (?, ?, ?, ?, ?, COALESCE((SELECT balance FROM users WHERE user_id = ?), 0.0))
    ''', (user_id, normalized_username, first_name, last_name, datetime.now(), user_id))
    conn.commit()
    conn.close()
    logger.info(f"Пользователь сохранен: {user_id} (@{username})")

def create_deal(deal_data: Dict) -> bool:
    """Создание сделки"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        guarantor_fee = deal_data['amount'] * GUARANTOR_FEE_PERCENT
        total_amount = deal_data['amount'] + guarantor_fee
        
        cursor.execute('''
            INSERT INTO deals 
            (id, creator_id, creator_role, buyer_id, seller_id, buyer_username, seller_username, amount, description, 
             deadline_days, created_at, status, guarantor_fee, total_amount, group_link)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            deal_data['id'], deal_data['creator_id'], deal_data['creator_role'],
            deal_data['buyer_id'], deal_data['seller_id'],
            deal_data['buyer_username'], deal_data['seller_username'],
            deal_data['amount'], deal_data['description'], 
            deal_data['deadline_days'], datetime.now(), 'created',
            guarantor_fee, total_amount, deal_data.get('group_link', '')
        ))
        conn.commit()
        logger.info(f"Сделка {deal_data['id']} создана успешно")
        return True
    except Exception as e:
        logger.error(f"Ошибка создания сделки: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def get_deal(deal_id: str) -> Optional[Dict]:
    """Получение сделки"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM deals WHERE id = ?', (deal_id,))
        row = cursor.fetchone()
        if row:
            # Правильное преобразование sqlite3.Row в обычный словарь
            deal_dict = dict(zip([col[0] for col in cursor.description], row))
            logger.info(f"Сделка {deal_id} найдена в базе данных")
            return deal_dict
        logger.warning(f"Сделка {deal_id} не найдена")
        return None
    except Exception as e:
        logger.error(f"Ошибка получения сделки {deal_id}: {e}")
        return None
    finally:
        conn.close()

def update_deal_status(deal_id: str, status: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE deals SET status = ? WHERE id = ?', (status, deal_id))
    conn.commit()
    conn.close()
    logger.info(f"Статус сделки {deal_id} обновлен на {status}")

def set_payment_url(deal_id: str, payment_url: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE deals SET payment_url = ? WHERE id = ?', (payment_url, deal_id))
    conn.commit()
    conn.close()
    logger.info(f"URL платежа установлен для сделки {deal_id}")

def set_payment_confirmed(deal_id: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE deals SET payment_confirmed = TRUE, status = ? WHERE id = ?', ('payment_received', deal_id))
    conn.commit()
    conn.close()
    logger.info(f"Платеж подтвержден для сделки {deal_id}")

def set_user_confirmed(deal_id: str, user_type: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    if user_type == 'buyer':
        cursor.execute('UPDATE deals SET buyer_confirmed = TRUE WHERE id = ?', (deal_id,))
    else:
        cursor.execute('UPDATE deals SET seller_confirmed = TRUE WHERE id = ?', (deal_id,))
    conn.commit()
    conn.close()
    logger.info(f"Сделка {deal_id}: {user_type} подтвержден")

def are_both_confirmed(deal_id: str) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT buyer_confirmed, seller_confirmed FROM deals WHERE id = ?', (deal_id,))
    result = cursor.fetchone()
    conn.close()
    confirmed = result and result[0] and result[1]
    if confirmed:
        logger.info(f"Оба пользователя подтвердили сделку {deal_id}")
    return confirmed

def set_group_chat_id(deal_id: str, group_chat_id: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE deals SET group_chat_id = ?, group_created = TRUE WHERE id = ?', (group_chat_id, deal_id))
    conn.commit()
    conn.close()
    logger.info(f"Группа {group_chat_id} установлена для сделки {deal_id}")

def add_review(review_data: Dict):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO reviews 
        (deal_id, reviewer_id, reviewed_user_id, review_text, rating, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        review_data['deal_id'], review_data['reviewer_id'],
        review_data['reviewed_user_id'], review_data['review_text'],
        review_data['rating'], datetime.now()
    ))
    conn.commit()
    conn.close()
    logger.info(f"Отзыв добавлен для сделки {review_data['deal_id']}")

def get_user_by_username(username: str) -> Optional[Dict]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE username = ?', (username.lower(),))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        # Правильное преобразование sqlite3.Row в словарь
        user_dict = dict(zip([col[0] for col in cursor.description], row))
        logger.info(f"Пользователь @{username} найден в базе данных, ID: {user_dict['user_id']}")
        return user_dict
    
    logger.info(f"Пользователь @{username} не найден в базе данных")
    return None

def get_user_deals(user_id: int) -> List[Dict]:
    """Получить все сделки пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM deals 
        WHERE buyer_id = ? OR seller_id = ? 
        ORDER BY created_at DESC
    ''', (user_id, user_id))
    rows = cursor.fetchall()
    conn.close()
    
    # Правильное преобразование каждой строки в словарь
    deals = []
    for row in rows:
        deal_dict = dict(zip([col[0] for col in cursor.description], row))
        deals.append(deal_dict)
    
    logger.info(f"Найдено {len(deals)} сделок для пользователя {user_id}")
    return deals

def update_user_balance(user_id: int, amount: float):
    """Обновление баланса пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
    conn.commit()
    conn.close()
    logger.info(f"Баланс пользователя {user_id} обновлен на {amount}")

def get_user_balance(user_id: int) -> float:
    """Получение баланса пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # СНАЧАЛА ПРОВЕРЯЕМ, СУЩЕСТВУЕТ ЛИ ПОЛЬЗОВАТЕЛЬ
    cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
    user_exists = cursor.fetchone()
    
    if not user_exists:
        # ЕСЛИ ПОЛЬЗОВАТЕЛЯ НЕТ, СОЗДАЕМ ЗАПИСЬ
        cursor.execute('''
            INSERT OR IGNORE INTO users 
            (user_id, username, first_name, last_name, created_at, balance)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, None, None, None, datetime.now(), 0.0))
        conn.commit()
        balance = 0.0
        logger.info(f"Создана запись для пользователя {user_id} с балансом 0")
    else:
        # ЕСЛИ ПОЛЬЗОВАТЕЛЬ ЕСТЬ, ПОЛУЧАЕМ БАЛАНС
        cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        balance = result[0] if result else 0.0
    
    conn.close()
    logger.info(f"Баланс пользователя {user_id}: {balance}")
    return balance

def create_withdrawal_request(user_id: int, amount: float, wallet: str):
    """Создание запроса на вывод средств"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO withdrawals 
        (user_id, amount, status, created_at, yoomoney_wallet)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, amount, 'pending', datetime.now(), wallet))
    conn.commit()
    conn.close()
    logger.info(f"Запрос на вывод {amount} RUB создан для пользователя {user_id}")

# Функции для работы с отзывами
def get_user_reviews(user_id: int) -> List[Dict]:
    """Получить все отзывы о пользователе"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM reviews 
        WHERE reviewed_user_id = ? 
        ORDER BY created_at DESC
    ''', (user_id,))
    rows = cursor.fetchall()
    conn.close()
    
    reviews = []
    for row in rows:
        review_dict = dict(zip([col[0] for col in cursor.description], row))
        reviews.append(review_dict)
    
    logger.info(f"Найдено {len(reviews)} отзывов о пользователе {user_id}")
    return reviews

def get_reviews_by_reviewer(reviewer_id: int) -> List[Dict]:
    """Получить все отзывы, написанные пользователем"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM reviews 
        WHERE reviewer_id = ? 
        ORDER BY created_at DESC
    ''', (reviewer_id,))
    rows = cursor.fetchall()
    conn.close()
    
    reviews = []
    for row in rows:
        review_dict = dict(zip([col[0] for col in cursor.description], row))
        reviews.append(review_dict)
    
    logger.info(f"Найдено {len(reviews)} отзывов от пользователя {reviewer_id}")
    return reviews

def add_service_review(review_data: Dict):
    """Добавление отзыва о сервисе"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO service_reviews 
        (reviewer_id, review_text, rating, created_at)
        VALUES (?, ?, ?, ?)
    ''', (
        review_data['reviewer_id'],
        review_data['review_text'],
        review_data['rating'],
        datetime.now()
    ))
    conn.commit()
    conn.close()
    logger.info(f"Отзыв о сервисе добавлен пользователем {review_data['reviewer_id']}")

# Вспомогательные функции
async def get_user_mention(user_id: int) -> str:
    try:
        user = await bot.get_chat(user_id)
        name = user.first_name or user.username or str(user_id)
        return f'<a href="tg://user?id={user_id}">{name}</a>'
    except Exception:
        return str(user_id)

async def format_duration(duration: timedelta) -> str:
    if not duration:
        return "навсегда"

    days = duration.days
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    parts = []
    if days > 0:
        parts.append(f"{days} д.")
    if hours > 0:
        parts.append(f"{hours} ч.")
    if minutes > 0 and days == 0:
        parts.append(f"{minutes} мин.")

    return " ".join(parts) if parts else "менее минуты"

async def is_owner(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def is_admin_user(user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором бота"""
    return user_id in ADMIN_IDS

async def get_user_id_from_message(text: str) -> Optional[int]:
    if not text:
        return None

    # Очищаем текст от лишних символов
    username = text.strip().lstrip('@')
    
    # Проверяем, является ли текст числом (user_id)
    if username.isdigit():
        user_id = int(username)
        try:
            # Проверяем, что пользователь действительно существует и начал диалог
            user = await bot.get_chat(user_id)
            logger.info(f"Пользователь с ID {user_id} найден: @{getattr(user, 'username', 'No username')}")
            return user_id
        except Exception as e:
            logger.error(f"Пользователь с ID {user_id} не найден или не начал диалог: {e}")
            return None
    
    # Если это username, ищем через базу данных бота
    try:
        logger.info(f"Поиск пользователя по username: @{username}")
        
        # Сначала ищем в нашей базе данных
        user_from_db = get_user_by_username(username)
        if user_from_db:
            user_id = user_from_db['user_id']
            logger.info(f"Пользователь @{username} найден в базе данных, ID: {user_id}")
            
            # Дополнительно проверяем через Telegram API
            try:
                user = await bot.get_chat(user_id)
                logger.info(f"Подтверждено через Telegram API: @{username} -> {user_id}")
                return user_id
            except Exception as e:
                logger.error(f"Пользователь @{username} не доступен через Telegram API: {e}")
                return None
        
        # Если не нашли в базе, пробуем через get_chat
        logger.info(f"Пользователь @{username} не найден в базе, пробуем через Telegram API...")
        user = await bot.get_chat(f"@{username}")
        
        if user and user.id:
            logger.info(f"Пользователь @{username} найден через Telegram API, ID: {user.id}")
            
            # Сохраняем пользователя в базу для будущих поисков
            save_user(
                user_id=user.id,
                username=user.username,
                first_name=user.first_name,
                last_name=user.last_name
            )
            
            return user.id
        else:
            logger.warning(f"Пользователь @{username} не найден через Telegram API")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при поиске пользователя @{username}: {e}")
        return None

def generate_deal_number() -> str:
    """Генерация 6-значного номера сделки"""
    return str(random.randint(100000, 999999))

async def format_deal_info(deal):
    """Форматирует информацию о сделке для отображения"""
    deal_id = deal.get('id', 'N/A')
    amount = deal.get('amount', 0)
    description = deal.get('description', 'Не указано')
    deadline_days = deal.get('deadline_days', 0)
    buyer_username = deal.get('buyer_username', 'Не указан')
    seller_username = deal.get('seller_username', 'Не указан')
    status = deal.get('status', 'Неизвестен')
    
    guarantor_fee = amount * GUARANTOR_FEE_PERCENT
    total_amount = amount + guarantor_fee
    
    # Отображаем статус сделки понятным текстом
    status_text = {
        'created': '🟡 Ожидает подтверждения',
        'active': '🟢 Активна',
        'completed': '✅ Завершена',
        'cancelled': '❌ Отменена',
        'rejected': '🚫 Отклонена',
        'payment_received': '💰 Оплата получена',
        'dispute': '🚨 Спор открыт'
    }.get(status, status)
    
    deal_info = (
        f"🤝 <b>Информация о сделке</b>\n\n"
        f"🔢 <b>Номер сделки:</b> <code>#{deal_id}</code>\n"
        f"💰 <b>Сумма сделки:</b> {amount} руб.\n"
        f"💼 <b>Комиссия гаранта (8%):</b> {guarantor_fee:.2f} руб.\n"
        f"💵 <b>Итого к оплате:</b> {total_amount:.2f} руб.\n"
        f"📝 <b>Описание:</b> {description}\n"
        f"⏰ <b>Срок выполнения:</b> {deadline_days} дней\n"
        f"👤 <b>Покупатель:</b> @{buyer_username}\n"
        f"👤 <b>Продавец:</b> @{seller_username}\n"
        f"📊 <b>Статус:</b> {status_text}\n\n"
    )
    
    return deal_info

# Обработчики команд
@dp.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject = None):
    user = message.from_user
    logger.info(f"Команда /start от пользователя {user.id} (@{user.username})")
    save_user(user.id, user.username, user.first_name, user.last_name)
    
    # Проверяем, не является ли это приглашением к сделке
    if command and command.args and command.args.startswith('deal_'):
        deal_id = command.args.replace('deal_', '')
        logger.info(f"Приглашение к сделке {deal_id} для пользователя {user.id}")
        await join_deal_via_link(message, deal_id, user)
        return
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="📝 Создать сделку", callback_data="create_deal"))
    keyboard.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="my_deals_callback"))
    keyboard.row(InlineKeyboardButton(text="💰 Мой баланс", callback_data="my_balance"))
    keyboard.row(InlineKeyboardButton(text="⭐ Мои отзывы", callback_data="my_reviews"))
    keyboard.row(InlineKeyboardButton(text="💬 Отзыв о сервисе", callback_data="service_review"))
    keyboard.row(InlineKeyboardButton(text="ℹ️ О боте", callback_data="about_bot"))
    
    # Отправляем фотографию с использованием FSInputFile
    try:
        photo_path = "main_menu.png"
        photo = FSInputFile(photo_path)
        
        await message.answer_photo(
            photo=photo,
            caption=(
                "🤝 Добро пожаловать в OnlineSoprovod!\n\n"
                "Я помогу организовать дистанционную сделку между покупателем и продавцом: проконсультирую, сопровожу и помогу довести её до успешного завершения."
            ),
            reply_markup=keyboard.as_markup()
        )
    except FileNotFoundError:
        logger.error(f"Файл {photo_path} не найден")
        # Запасной вариант: отправляем текстовое сообщение
        await message.answer(
            "🤝 Добро пожаловать в OnlineSoprovod!\n\n"
            "Я помогу организовать дистанционную сделку между покупателем и продавцом: проконсультирую, сопровожу и помогу довести её до успешного завершения.",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке фотографии: {e}")
        # Запасной вариант: отправляем текстовое сообщение
        await message.answer(
            "🤝 Добро пожаловать в OnlineSoprovod!\n\n"
            "Я помогу организовать дистанционную сделку между покупателем и продавцом: проконсультирую, сопровожу и помогу довести её до успешного завершения.",
            reply_markup=keyboard.as_markup()
        )

@dp.message(Command("reviews"))
async def cmd_reviews(message: Message, command: CommandObject = None):
    """Просмотр отзывов о пользователе"""
    if not command or not command.args:
        await message.answer("❌ Использование: /reviews @username или /reviews ID_пользователя")
        return
    
    target = command.args.strip().lstrip('@')
    
    # Пытаемся найти пользователя
    user_id = await get_user_id_from_message(target)
    
    if not user_id:
        await message.answer("❌ Пользователь не найден или не начал диалог с ботом.")
        return
    
    # Получаем отзывы о пользователе
    reviews = get_user_reviews(user_id)
    
    if not reviews:
        await message.answer(f"📝 У пользователя пока нет отзывов.")
        return
    
    # Получаем информацию о пользователе
    try:
        user = await bot.get_chat(user_id)
        user_name = user.first_name or user.username or f"Пользователь {user_id}"
    except:
        user_name = f"Пользователь {user_id}"
    
    # Формируем список отзывов
    reviews_text = f"⭐ <b>Отзывы о пользователе {user_name}</b>\n\n"
    
    total_rating = 0
    for review in reviews[:10]:  # Ограничиваем 10 отзывами
        try:
            reviewer = await bot.get_chat(review['reviewer_id'])
            reviewer_name = reviewer.first_name or reviewer.username or f"Пользователь {review['reviewer_id']}"
        except:
            reviewer_name = f"Пользователь {review['reviewer_id']}"
        
        rating = review['rating']
        review_text = review['review_text']
        created_at = review['created_at']
        
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        
        reviews_text += (
            f"⭐ <b>Оценка:</b> {rating}/5\n"
            f"👤 <b>От:</b> {reviewer_name}\n"
            f"📝 <b>Отзыв:</b> {review_text}\n"
            f"📅 <b>Дата:</b> {created_at.strftime('%d.%m.%Y %H:%M')}\n"
            f"────────────────────\n\n"
        )
        total_rating += rating
    
    # Добавляем средний рейтинг
    avg_rating = total_rating / len(reviews)
    reviews_text = f"⭐ <b>Отзывы о пользователе {user_name}</b>\n🏆 <b>Средний рейтинг:</b> {avg_rating:.1f}/5\n\n" + reviews_text
    
    await message.answer(reviews_text, parse_mode="HTML")

@dp.message(Command("mydeals"))
async def cmd_mydeals(message: Message):
    user_id = message.from_user.id
    logger.info(f"Команда /mydeals от пользователя {user_id}")
    await show_user_deals(user_id, message)

@dp.message(Command("balance"))
async def cmd_balance(message: Message):
    """Показать баланс пользователя"""
    user_id = message.from_user.id
    balance = get_user_balance(user_id)
    
    keyboard = InlineKeyboardBuilder()
    if balance >= 50:
        keyboard.row(InlineKeyboardButton(text="💰 Подать заявку на вывод", callback_data="withdraw_request"))
    keyboard.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="my_deals_callback"))
    keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main"))
    
    await message.answer(
        f"💰 <b>Ваш баланс:</b> {balance:.2f} руб.\n\n"
        f"Минимальная сумма для вывода: 50 руб.",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )

@dp.message(Command("finduser"))
async def cmd_finduser(message: Message, command: CommandObject = None):
    """Команда для поиска пользователя по username"""
    if not command or not command.args:
        await message.answer("❌ Использование: /finduser username")
        return
    
    username = command.args.strip().lstrip('@')
    user_id = await get_user_id_from_message(username)
    
    if user_id:
        try:
            user = await bot.get_chat(user_id)
            user_info = (
                f"✅ Пользователь найден:\n\n"
                f"Username: @{username}\n"
                f"ID: {user_id}\n"
                f"Имя: {user.first_name or 'Не указано'}\n"
                f"Фамилия: {user.last_name or 'Не указана'}\n"
                f"Доступен для бота: Да"
            )
        except Exception as e:
            user_info = (
                f"⚠️ Пользователь найден, но недоступен:\n\n"
                f"Username: @{username}\n"
                f"ID: {user_id}\n"
                f"Ошибка доступа: {e}"
            )
    else:
        user_info = f"❌ Пользователь @{username} не найден или недоступен"
    
    await message.answer(user_info)

@dp.message(Command("checkpayment"))
async def cmd_checkpayment(message: Message, command: CommandObject = None):
    """Ручная проверка платежа администратором"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Недостаточно прав")
        return
        
    if not command or not command.args:
        await message.answer("Использование: /checkpayment номер_сделки")
        return
    
    deal_id = command.args.strip()
    deal = get_deal(deal_id)
    
    if not deal:
        await message.answer("❌ Сделка не найдена")
        return
    
    await message.answer("🔄 Проверяем оплату через ЮMoney...")
    
    # РЕАЛЬНАЯ проверка оплаты через API
    payment_confirmed = await yoo_money.check_payment(deal_id)
    
    if payment_confirmed:
        set_payment_confirmed(deal_id)
        await message.answer("✅ Оплата подтверждена! Сделка активирована.")
        
        # Уведомляем участников
        if deal['group_chat_id']:
            await bot.send_message(
                chat_id=deal['group_chat_id'],
                text=f"✅ <b>Оплата подтверждена!</b>\n\n"
                     f"Покупатель @{deal['buyer_username']} оплатил сделку #{deal_id}\n"
                     f"💰 <b>Сумма:</b> {deal['total_amount']:.2f} руб.\n\n"
                     f"Средства заморожены в системе. Продавец может приступать к работе.",
                parse_mode="HTML"
            )
    else:
        await message.answer("❌ Оплата не найдена.")

@dp.message(Command("payout"))
async def cmd_payout(message: Message, command: CommandObject = None):
    """Команда для выполнения вывода средств администратором"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Недостаточно прав")
        return
    
    if not command or not command.args:
        await message.answer("❌ Использование: /payout user_id amount wallet")
        return
    
    args = command.args.split()
    if len(args) != 3:
        await message.answer("❌ Использование: /payout user_id amount wallet")
        return
    
    try:
        user_id = int(args[0])
        amount = float(args[1])
        wallet = args[2]
        
        # Выполняем вывод через ЮMoney
        success = await yoo_money.make_payout(wallet, amount)
        
        if success:
            # Обновляем статус вывода в базе
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE withdrawals SET status = ?, processed_at = ? WHERE user_id = ? AND amount = ? AND status = ?',
                ('completed', datetime.now(), user_id, amount, 'pending')
            )
            conn.commit()
            conn.close()
            
            await message.answer(f"✅ Вывод {amount} руб. на кошелек {wallet} выполнен успешно!")
            
            # Уведомляем пользователя
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"✅ <b>Вывод средств выполнен!</b>\n\n"
                         f"💰 <b>Сумма:</b> {amount} руб.\n"
                         f"📱 <b>Кошелек:</b> {wallet}\n\n"
                         f"Средства поступят в течение 24 часов.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления пользователя {user_id}: {e}")
        else:
            await message.answer("❌ Ошибка при выполнении вывода")
            
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("get_token"))
async def cmd_get_token(message: Message, command: CommandObject = None):
    """Команда для получения OAuth токена ЮMoney"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Недостаточно прав")
        return
    
    if not command or not command.args:
        await message.answer("❌ Использование: /get_token код_авторизации")
        return
    
    auth_code = command.args.strip()
    
    await message.answer("🔄 Получаем OAuth токен...")
    
    try:
        async with aiohttp.ClientSession() as session:
            data = {
                'code': auth_code,
                'client_id': YOO_MONEY_CLIENT_ID,
                'client_secret': YOO_MONEY_CLIENT_SECRET,
                'grant_type': 'authorization_code',
                'redirect_uri': 'https://example.com'
            }
            
            async with session.post('https://yoomoney.ru/oauth/token', data=data) as response:
                if response.status == 200:
                    result = await response.json()
                    access_token = result.get('access_token')
                    
                    if access_token:
                        # Сохраняем токен в переменные окружения или базу данных
                        await message.answer(
                            f"✅ <b>OAuth токен получен успешно!</b>\n\n"
                            f"Токен: <code>{access_token}</code>\n\n"
                            f"Добавьте этот токен в переменную окружения:\n"
                            f"<code>YOO_MONEY_ACCESS_TOKEN={access_token}</code>",
                            parse_mode="HTML"
                        )
                        
                        # Тестируем подключение с новым токеном
                        yoo_money.access_token = access_token
                        account_info = await yoo_money.get_account_info()
                        
                        if account_info:
                            balance = account_info.get('balance', 'N/A')
                            await message.answer(
                                f"✅ <b>Подключение успешно!</b>\n\n"
                                f"💰 Баланс: {balance} руб.\n"
                                f"📊 Статус: {account_info.get('account_status', 'N/A')}",
                                parse_mode="HTML"
                            )
                        else:
                            await message.answer("❌ Ошибка тестирования подключения")
                    else:
                        await message.answer("❌ Не удалось получить токен из ответа")
                else:
                    error_text = await response.text()
                    await message.answer(f"❌ Ошибка: {response.status} - {error_text}")
                    
    except Exception as e:
        await message.answer(f"❌ Ошибка при получении токена: {e}")

@dp.message(Command("test_connection"))
async def cmd_test_connection(message: Message):
    """Тестирование подключения к ЮMoney"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Недостаточно прав")
        return
    
    await message.answer("🔄 Тестируем подключение к ЮMoney...")
    
    try:
        # Тестируем получение информации об аккаунте
        account_info = await yoo_money.get_account_info()
        
        if account_info:
            balance = account_info.get('balance', 'N/A')
            account_status = account_info.get('account_status', 'N/A')
            account_type = account_info.get('account_type', 'N/A')
            currency = account_info.get('currency', 'N/A')
            
            # Форматируем баланс
            if isinstance(balance, (int, float)):
                formatted_balance = f"{balance:.2f}"
            else:
                formatted_balance = str(balance)
            
            await message.answer(
                f"✅ <b>Подключение к ЮMoney успешно!</b>\n\n"
                f"💰 <b>Баланс:</b> {formatted_balance} {currency}\n"
                f"📊 <b>Статус счета:</b> {account_status}\n"
                f"🏦 <b>Тип счета:</b> {account_type}\n"
                f"👤 <b>Кошелек:</b> {YOO_MONEY_ACCOUNT}\n"
                f"🔑 <b>Токен активен:</b> Да",
                parse_mode="HTML"
            )
            
            # Дополнительно тестируем историю операций
            await message.answer("🔄 Проверяем доступ к истории операций...")
            
            # Пробуем получить последние операции
            async with aiohttp.ClientSession() as session:
                headers = {
                    'Authorization': f'Bearer {yoo_money.access_token}',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                
                params = {
                    "type": "deposition",
                    "records": 5,  # Последние 5 операций
                    "details": "true"
                }
                
                async with session.get(
                    f"{yoo_money.base_url}/operation-history",
                    headers=headers,
                    params=params
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        operations = data.get('operations', [])
                        
                        await message.answer(
                            f"✅ <b>Доступ к истории операций подтвержден!</b>\n\n"
                            f"📈 <b>Последних операций:</b> {len(operations)}\n"
                            f"🔧 <b>API работает корректно</b>",
                            parse_mode="HTML"
                        )
                    else:
                        await message.answer(
                            f"⚠️ <b>Основное подключение работает, но есть ограничения:</b>\n\n"
                            f"❌ Не удалось получить историю операций\n"
                            f"📝 <b>Статус:</b> {response.status}\n\n"
                            f"<i>Проверьте scope разрешений в OAuth</i>",
                            parse_mode="HTML"
                        )
                        
        else:
            await message.answer(
                "❌ <b>Ошибка подключения к ЮMoney</b>\n\n"
                "Возможные причины:\n"
                "• Неверный OAuth токен\n"
                "• Токен устарел или отозван\n"
                "• Неправильные client_id или client_secret\n"
                "• Проблемы с сетью\n\n"
                "Попробуйте получить новый токен командой /get_token",
                parse_mode="HTML"
            )
            
    except aiohttp.ClientError as e:
        await message.answer(
            f"❌ <b>Сетевая ошибка при подключении к ЮMoney:</b>\n\n"
            f"Ошибка: {e}\n\n"
            f"Проверьте интернет-соединение и доступность API ЮMoney.",
            parse_mode="HTML"
        )
        
    except Exception as e:
        await message.answer(
            f"❌ <b>Неожиданная ошибка:</b>\n\n"
            f"Ошибка: {e}\n\n"
            f"Проверьте настройки и логи бота.",
            parse_mode="HTML"
        )

@dp.message(Command("deal"))
async def cmd_deal(message: Message, command: CommandObject = None):
    """Привязка чата к сделке"""
    # Проверяем, что команда используется в группе
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    if not command or not command.args:
        await message.answer(
            "🔗 <b>Привязка чата к сделке</b>\n\n"
            "Использование: <code>/deal номер_сделки</code>\n\n"
            "Пример: <code>/deal 123456</code>\n\n"
            "Номер сделки можно найти в деталях сделки в личном чате с ботом.",
            parse_mode="HTML"
        )
        return
    
    deal_id = command.args.strip()
    deal = get_deal(deal_id)
    
    if not deal:
        await message.answer("❌ Сделка не найдена. Проверьте номер сделки.")
        return
    
    # Проверяем, что пользователь является участником сделки
    user_id = message.from_user.id
    if user_id not in [deal['buyer_id'], deal['seller_id']]:
        await message.answer("❌ Вы не являетесь участником этой сделки.")
        return
    
    # Проверяем, не привязан ли уже чат к другой сделке
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM deals WHERE group_chat_id = ? AND id != ?', 
                  (str(message.chat.id), deal_id))
    existing_deal = cursor.fetchone()
    conn.close()
    
    if existing_deal:
        await message.answer(
            f"❌ Этот чат уже привязан к сделке #{existing_deal[0]}\n\n"
            f"Один чат может быть привязан только к одной сделке."
        )
        return
    
    # Привязываем чат к сделке
    set_group_chat_id(deal_id, str(message.chat.id))
    
    # Получаем информацию об участниках
    buyer_mention = await get_user_mention(deal['buyer_id'])
    seller_mention = await get_user_mention(deal['seller_id'])
    
    await message.answer(
        f"✅ <b>Чат успешно привязан к сделке!</b>\n\n"
        f"🔢 <b>Номер сделки:</b> <code>#{deal_id}</code>\n"
        f"💰 <b>Сумма:</b> {deal['amount']} руб.\n"
        f"📝 <b>Описание:</b> {deal['description']}\n\n"
        f"👥 <b>Участники:</b>\n"
        f"• Покупатель: {buyer_mention}\n"
        f"• Продавец: {seller_mention}\n\n"
        f"💬 <b>Этот чат теперь является официальным чатом для обсуждения сделки.</b>",
        parse_mode="HTML"
    )
    
    # Уведомляем второго участника
    other_participant_id = deal['seller_id'] if user_id == deal['buyer_id'] else deal['buyer_id']
    try:
        await bot.send_message(
            other_participant_id,
            f"🔗 <b>Чат привязан к сделке</b>\n\n"
            f"Чат успешно привязан к сделке #{deal_id}\n"
            f"💬 Теперь вы можете общаться в привязанном чате.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления участника {other_participant_id}: {e}")
    
    logger.info(f"Чат {message.chat.id} привязан к сделке {deal_id}")

@dp.message(Command("deal_info"))
async def cmd_deal_info(message: Message):
    """Показать информацию о привязанной сделке"""
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM deals WHERE group_chat_id = ?', (str(message.chat.id),))
    deal_row = cursor.fetchone()
    conn.close()
    
    if not deal_row:
        await message.answer("❌ К этому чату не привязана ни одна сделка.")
        return
    
    # Преобразуем строку в словарь
    deal = dict(zip([col[0] for col in cursor.description], deal_row))
    deal_info = await format_deal_info(deal)
    
    await message.answer(
        f"🔗 <b>Информация о привязанной сделке</b>\n\n{deal_info}",
        parse_mode="HTML"
    )

# Обработчики callback queries
@dp.callback_query(F.data == "about_bot")
async def about_bot(callback: CallbackQuery):
    """Обработчик кнопки 'О боте'"""
    await callback.answer()
    
    about_text = (
        "🤖 <b>OnlineSoprovod — сопровождение онлайн-сделок</b>\n\n"
        "Я помогаю безопасно проводить сделки между покупателями и продавцами:\n\n"
        "✅ <b>Безопасность</b> - средства хранятся до выполнения условий сделки\n"
        "✅ <b>Защита от мошенников</b> - обе стороны защищены\n"
        "✅ <b>Процесс споров</b> - администратор поможет разрешить конфликты\n"
        "✅ <b>Система отзывов</b> - создавайте репутацию надежного партнера\n\n"
        "Комиссия сервиса: <b>8%</b> от суммы сделки\n\n"
        "Начните сделку прямо сейчас!"
    )
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="📝 Создать сделку", callback_data="create_deal"))
    keyboard.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="my_deals_callback"))
    keyboard.row(InlineKeyboardButton(text="💰 Мой баланс", callback_data="my_balance"))
    keyboard.row(InlineKeyboardButton(text="⭐ Мои отзывы", callback_data="my_reviews"))
    keyboard.row(InlineKeyboardButton(text="💬 Отзыв о сервисе", callback_data="service_review"))
    
    # Всегда отправляем новое сообщение вместо редактирования
    await callback.message.answer(about_text, reply_markup=keyboard.as_markup(), parse_mode="HTML")
    
    logger.info(f"Пользователь {callback.from_user.id} запросил информацию о боте")

@dp.callback_query(F.data == "my_balance")
async def my_balance_callback(callback: CallbackQuery):
    """Обработчик кнопки 'Мой баланс'"""
    await callback.answer()
    user_id = callback.from_user.id
    balance = get_user_balance(user_id)
    
    keyboard = InlineKeyboardBuilder()
    if balance >= 50:
        keyboard.row(InlineKeyboardButton(text="💰 Подать заявку на вывод", callback_data="withdraw_request"))
    keyboard.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="my_deals_callback"))
    keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main"))
    
    # Всегда отправляем новое сообщение вместо редактирования
    await callback.message.answer(
        f"💰 <b>Ваш баланс:</b> {balance:.2f} руб.\n\n"
        f"Минимальная сумма для вывода: 50 руб.",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "withdraw_request")
async def withdraw_request(callback: CallbackQuery, state: FSMContext):
    """Обработчик запроса на вывод средств"""
    await callback.answer()
    user_id = callback.from_user.id
    balance = get_user_balance(user_id)
    
    if balance < 50:
        await callback.answer("❌ Минимальная сумма для вывода - 50 руб.", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"💰 <b>Заявка на вывод средств</b>\n\n"
        f"Доступно для вывода: {balance:.2f} руб.\n\n"
        f"Введите номер кошелька ЮMoney для получения средств:",
        parse_mode="HTML"
    )
    
    # Используем правильное состояние для вывода
    await state.set_state(DealStates.WITHDRAWAL_WALLET)
    await state.update_data(withdraw_amount=balance)

@dp.message(DealStates.WITHDRAWAL_WALLET)
async def process_withdrawal_wallet(message: Message, state: FSMContext):
    """Обработка ввода кошелька для вывода"""
    wallet = message.text.strip()
    state_data = await state.get_data()
    amount = state_data.get('withdraw_amount')
    
    if not amount:
        await message.answer("❌ Ошибка: сумма не найдена.")
        await state.clear()
        return
    
    # Проверяем формат кошелька ЮMoney
    if not re.match(r'^\d{11,16}$', wallet):
        await message.answer("❌ Неверный формат кошелька ЮMoney. Введите 11-16 цифр:")
        return
    
    user_id = message.from_user.id
    
    # Создаем запрос на вывод
    create_withdrawal_request(user_id, amount, wallet)
    
    # Обнуляем баланс пользователя
    update_user_balance(user_id, -amount)
    
    # Уведомляем администраторов
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"🚨 <b>Новая заявка на вывод</b>\n\n"
                f"👤 Пользователь: @{message.from_user.username or 'No username'}\n"
                f"💰 Сумма: {amount:.2f} руб.\n"
                f"📱 Кошелек: {wallet}\n\n"
                f"Для выполнения вывода используйте команду:\n"
                f"<code>/payout {user_id} {amount} {wallet}</code>",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления администратора {admin_id}: {e}")
    
    await message.answer(
        f"✅ Заявка на вывод {amount:.2f} руб. на кошелек {wallet} принята!\n\n"
        f"Вывод будет выполнен в течение 24 часов.",
        reply_markup=InlineKeyboardBuilder().row(
            InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main")
        ).as_markup()
    )
    
    await state.clear()

@dp.callback_query(F.data == "my_reviews")
async def my_reviews_callback(callback: CallbackQuery):
    """Показать все отзывы пользователя (как оставленные им, так и полученные)"""
    await callback.answer()
    user_id = callback.from_user.id
    
    # Получаем отзывы, оставленные пользователем
    reviews_given = get_reviews_by_reviewer(user_id)
    
    # Получаем отзывы о пользователе
    reviews_received = get_user_reviews(user_id)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="📋 Полученные отзывы", callback_data="show_received_reviews"))
    keyboard.row(InlineKeyboardButton(text="✍️ Оставленные отзывы", callback_data="show_given_reviews"))
    keyboard.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main"))
    
    text = (
        f"⭐ <b>Мои отзывы</b>\n\n"
        f"📥 <b>Получено отзывов:</b> {len(reviews_received)}\n"
        f"📤 <b>Оставлено отзывов:</b> {len(reviews_given)}\n\n"
        f"Выберите, какие отзывы посмотреть:"
    )
    
    # Всегда отправляем новое сообщение вместо редактирования
    await callback.message.answer(text, reply_markup=keyboard.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "show_received_reviews")
async def show_received_reviews(callback: CallbackQuery):
    """Показать полученные отзывы"""
    await callback.answer()
    user_id = callback.from_user.id
    reviews = get_user_reviews(user_id)
    
    if not reviews:
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="my_reviews"))
        keyboard.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main"))
        
        await callback.message.answer(
            "📝 У вас пока нет полученных отзывов.",
            reply_markup=keyboard.as_markup()
        )
        return
    
    # Формируем список отзывов
    reviews_text = "⭐ <b>Отзывы обо мне</b>\n\n"
    
    total_rating = 0
    for review in reviews[:10]:  # Ограничиваем 10 отзывами
        try:
            reviewer = await bot.get_chat(review['reviewer_id'])
            reviewer_name = reviewer.first_name or reviewer.username or f"Пользователь {review['reviewer_id']}"
        except:
            reviewer_name = f"Пользователь {review['reviewer_id']}"
        
        rating = review['rating']
        review_text = review['review_text']
        created_at = review['created_at']
        
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        
        reviews_text += (
            f"⭐ <b>Оценка:</b> {rating}/5\n"
            f"👤 <b>От:</b> {reviewer_name}\n"
            f"📝 <b>Отзыв:</b> {review_text}\n"
            f"📅 <b>Дата:</b> {created_at.strftime('%d.%m.%Y %H:%M')}\n"
            f"────────────────────\n\n"
        )
        total_rating += rating
    
    # Добавляем средний рейтинг
    avg_rating = total_rating / len(reviews)
    reviews_text = f"⭐ <b>Отзывы обо мне</b>\n🏆 <b>Средний рейтинг:</b> {avg_rating:.1f}/5\n\n" + reviews_text
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="my_reviews"))
    keyboard.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(reviews_text, reply_markup=keyboard.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "show_given_reviews")
async def show_given_reviews(callback: CallbackQuery):
    """Показать оставленные отзывы"""
    await callback.answer()
    user_id = callback.from_user.id
    reviews = get_reviews_by_reviewer(user_id)
    
    if not reviews:
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="my_reviews"))
        keyboard.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main"))
        
        await callback.message.edit_text(
            "📝 Вы пока не оставляли отзывов.",
            reply_markup=keyboard.as_markup()
        )
        return
    
    # Формируем список отзывов
    reviews_text = "✍️ <b>Мои оставленные отзывы</b>\n\n"
    
    for review in reviews[:10]:  # Ограничиваем 10 отзывами
        try:
            reviewed_user = await bot.get_chat(review['reviewed_user_id'])
            reviewed_name = reviewed_user.first_name or reviewed_user.username or f"Пользователь {review['reviewed_user_id']}"
        except:
            reviewed_name = f"Пользователь {review['reviewed_user_id']}"
        
        rating = review['rating']
        review_text = review['review_text']
        created_at = review['created_at']
        
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        
        reviews_text += (
            f"👤 <b>Пользователь:</b> {reviewed_name}\n"
            f"⭐ <b>Оценка:</b> {rating}/5\n"
            f"📝 <b>Отзыв:</b> {review_text}\n"
            f"📅 <b>Дата:</b> {created_at.strftime('%d.%m.%Y %H:%M')}\n"
            f"────────────────────\n\n"
        )
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="my_reviews"))
    keyboard.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(reviews_text, reply_markup=keyboard.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "service_review")
async def service_review_callback(callback: CallbackQuery, state: FSMContext):
    """Начало процесса оставления отзыва о сервисе"""
    await callback.answer()
    
    await state.set_state(DealStates.SERVICE_REVIEW_TEXT)
    
    rating_keyboard = InlineKeyboardBuilder()
    for i in range(1, 6):
        rating_keyboard.row(InlineKeyboardButton(text="⭐" * i, callback_data=f"service_rating_{i}"))
    
    # Всегда отправляем новое сообщение вместо редактирования
    await callback.message.answer(
        "💬 <b>Отзыв о сервисе</b>\n\n"
        "Пожалуйста, оцените нашу работу и оставьте отзыв о сервисе OnlineSoprovod.\n\n"
        "Выберите оценку:",
        reply_markup=rating_keyboard.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery):
    """Возврат в главное меню"""
    await callback.answer()
    user = callback.from_user
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="📝 Создать сделку", callback_data="create_deal"))
    keyboard.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="my_deals_callback"))
    keyboard.row(InlineKeyboardButton(text="💰 Мой баланс", callback_data="my_balance"))
    keyboard.row(InlineKeyboardButton(text="⭐ Мои отзывы", callback_data="my_reviews"))
    keyboard.row(InlineKeyboardButton(text="💬 Отзыв о сервисе", callback_data="service_review"))
    keyboard.row(InlineKeyboardButton(text="ℹ️ О боте", callback_data="about_bot"))
    
    # Всегда отправляем новое сообщение вместо редактирования
    await callback.message.answer(
        "🤝 Добро пожаловать в OnlineSoprovod!\n\n"
        "Я помогу организовать дистанционную сделку между покупателем и продавцом: проконсультирую, сопровожу и помогу довести её до успешного завершения.",
        reply_markup=keyboard.as_markup()
    )

@dp.callback_query(F.data.startswith("service_rating_"))
async def process_service_rating(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора рейтинга для отзыва о сервисе"""
    await callback.answer()
    rating = int(callback.data.split("_")[2])
    
    await state.update_data(rating=rating)
    
    await callback.message.answer(
        f"⭐ <b>Оценка: {rating}/5</b>\n\n"
        f"Теперь напишите текстовый отзыв о нашем сервисе:",
        parse_mode="HTML"
    )

@dp.message(DealStates.SERVICE_REVIEW_TEXT)
async def process_service_review_text(message: Message, state: FSMContext):
    """Обработка текста отзыва о сервисе"""
    review_text = message.text.strip()
    state_data = await state.get_data()
    
    if 'rating' not in state_data:
        await message.answer("❌ Ошибка: оценка не найдена. Попробуйте начать заново.")
        await state.clear()
        return
    
    if len(review_text) < 5:
        await message.answer("❌ Отзыв слишком короткий. Напишите более развернутый отзыв:")
        return
    
    # Сохраняем отзыв о сервисе
    review_data = {
        'reviewer_id': message.from_user.id,
        'review_text': review_text,
        'rating': state_data['rating']
    }
    
    add_service_review(review_data)
    
    # Публикуем отзыв в канал
    try:
        await bot.send_message(
            chat_id=REVIEWS_CHANNEL,
            text=f"💬 <b>Новый отзыв о сервисе!</b>\n\n"
                 f"👤 <b>Пользователь:</b> @{message.from_user.username or 'No username'}\n"
                 f"⭐ <b>Оценка:</b> {state_data['rating']}/5\n"
                 f"📝 <b>Отзыв:</b> {review_text}\n\n"
                 f"#отзыв_о_сервисе",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка публикации отзыва о сервисе: {e}")
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main"))
    
    await message.answer(
        "✅ <b>Спасибо за ваш отзыв о сервисе!</b>\n\n"
        "Ваш отзыв был опубликован и поможет нам стать лучше.",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )
    
    await state.clear()

@dp.callback_query(F.data == "create_deal")
async def create_deal_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(DealStates.DEAL_ROLE)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="👤 Я покупатель", callback_data="role_buyer"))
    keyboard.row(InlineKeyboardButton(text="👨‍💼 Я продавец", callback_data="role_seller"))
    keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main"))
    
    # Всегда отправляем новое сообщение вместо редактирования
    await callback.message.answer(
        "🤝 <b>Создание новой сделки</b>\n\n"
        "Выберите вашу роль в сделке:",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )
    logger.info(f"Пользователь {callback.from_user.id} начал создание сделки")

@dp.callback_query(F.data.startswith("role_"))
async def process_role(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    role = callback.data.split("_")[1]
    await state.update_data(creator_role=role)
    await state.set_state(DealStates.DEAL_AMOUNT)
    
    await callback.message.edit_text(
        "💰 <b>Введите сумму сделки в рублях:</b>\n\n"
        "Пример: <code>1500</code> или <code>2999.50</code>",
        parse_mode="HTML"
    )
    logger.info(f"Пользователь {callback.from_user.id} выбрал роль: {role}")

@dp.message(DealStates.DEAL_AMOUNT)
async def process_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            await message.answer("❌ Сумма должна быть больше 0. Введите корректную сумму:")
            return
        await state.update_data(amount=amount)
        await state.set_state(DealStates.DEAL_DESCRIPTION)
        
        await message.answer(
            "📝 <b>Опишите предмет сделки:</b>\n\n"
            "Пример: <code>Разработка логотипа для компании</code>",
            parse_mode="HTML"
        )
        logger.info(f"Пользователь {message.from_user.id} указал сумму: {amount}")
    except ValueError:
        await message.answer("❌ Неверный формат суммы. Введите число:")

@dp.message(DealStates.DEAL_DESCRIPTION)
async def process_description(message: Message, state: FSMContext):
    description = message.text.strip()
    if len(description) < 5:
        await message.answer("❌ Описание слишком короткое. Введите подробное описание:")
        return
        
    await state.update_data(description=description)
    await state.set_state(DealStates.DEAL_DEADLINE)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="1 день", callback_data="deadline_1"))
    keyboard.row(InlineKeyboardButton(text="3 дня", callback_data="deadline_3"))
    keyboard.row(InlineKeyboardButton(text="7 дней", callback_data="deadline_7"))
    keyboard.row(InlineKeyboardButton(text="14 дней", callback_data="deadline_14"))
    
    await message.answer(
        "⏰ <b>Выберите срок выполнения сделки:</b>",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )
    logger.info(f"Пользователь {message.from_user.id} указал описание: {description}")

@dp.callback_query(F.data.startswith("deadline_"))
async def process_deadline(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    deadline_days = int(callback.data.split("_")[1])
    await state.update_data(deadline_days=deadline_days)
    await state.set_state(DealStates.DEAL_PARTNER)
    
    state_data = await state.get_data()
    role = state_data['creator_role']
    
    partner_type = "продавца" if role == "buyer" else "покупателя"
    
    await callback.message.edit_text(
        f"👤 <b>Введите username {partner_type}:</b>\n\n"
        f"Пример: <code>@username</code>\n\n"
        f"<i>Пользователь должен быть в базе бота. Если его нет, попросите его начать диалог с @{BOT_USERNAME}</i>",
        parse_mode="HTML"
    )
    logger.info(f"Пользователь {callback.from_user.id} выбрал срок: {deadline_days} дней")

@dp.message(DealStates.DEAL_PARTNER)
async def process_partner(message: Message, state: FSMContext):
    partner_username = message.text.strip().lstrip('@')
    state_data = await state.get_data()
    
    # Получаем user_id партнера
    partner_user_id = await get_user_id_from_message(partner_username)
    
    if not partner_user_id:
        await message.answer(
            f"❌ Пользователь @{partner_username} не найден или не начал диалог с ботом.\n\n"
            f"Попросите его начать диалог с @{BOT_USERNAME} и повторите ввод:"
        )
        return
    
    # Дополнительная проверка через Telegram API
    try:
        partner_chat = await bot.get_chat(partner_user_id)
        logger.info(f"Подтвержден доступ к пользователю @{partner_username} через Telegram API")
        
        # Сохраняем актуальные данные пользователя в базу
        save_user(
            user_id=partner_user_id,
            username=partner_chat.username,
            first_name=partner_chat.first_name,
            last_name=partner_chat.last_name
        )
    except Exception as e:
        logger.error(f"Пользователь @{partner_username} недоступен через Telegram API: {e}")
        await message.answer(
            f"❌ Не удалось получить доступ к пользователю @{partner_username}.\n"
            f"Убедитесь, что пользователь существует и начал диалог с ботом.\n\n"
            f"Повторите ввод username:"
        )
        return
    
    # Проверяем, что это не сам создатель
    if partner_user_id == message.from_user.id:
        await message.answer("❌ Нельзя создать сделку с самим собой. Введите username другого пользователя:")
        return
    
    # Сохраняем данные партнера
    creator_role = state_data['creator_role']
    if creator_role == "buyer":
        buyer_id = message.from_user.id
        buyer_username = message.from_user.username or "No username"
        seller_id = partner_user_id
        seller_username = partner_username
    else:
        seller_id = message.from_user.id
        seller_username = message.from_user.username or "No username"
        buyer_id = partner_user_id
        buyer_username = partner_username
    
    # Сохраняем все данные в состоянии
    await state.update_data(
        buyer_id=buyer_id,
        seller_id=seller_id,
        buyer_username=buyer_username,
        seller_username=seller_username
    )
    
    # Переходим к запросу ссылки на группу
    await state.set_state(DealStates.DEAL_GROUP_LINK)
    
    await message.answer(
        "👥 <b>Создание группы для сделки</b>\n\n"
        "Теперь создайте группу в Telegram и добавьте в неё:\n\n"
        "1. Второго участника сделки\n"
        "2. Бота @SafeDealGuardBot (с правами администратора)\n\n"
        "После создания группы отправьте сюда ссылку-приглашение в группу.\n\n"
        "<i>Ссылка должна начинаться с https://t.me/ или t.me/</i>",
        parse_mode="HTML"
    )

@dp.message(DealStates.DEAL_GROUP_LINK)
async def process_group_link(message: Message, state: FSMContext):
    """Обработка ссылки на группу при создании сделки"""
    group_link = message.text.strip()
    
    # Проверяем, что ссылка валидная
    if not (group_link.startswith("https://t.me/") or group_link.startswith("t.me/")):
        await message.answer("❌ Неверный формат ссылки. Отправьте корректную ссылку на группу Telegram:")
        return
    
    await state.update_data(group_link=group_link)
    
    # Формируем информацию о сделке для подтверждения
    state_data = await state.get_data()
    deal_data = {
        **state_data,
        'id': 'PREVIEW',  # Временный ID для предпросмотра
        'status': 'created'
    }
    
    deal_info = await format_deal_info(deal_data)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="✅ Подтвердить создание", callback_data="confirm_deal_creation"))
    keyboard.row(InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_deal"))
    
    await message.answer(
        f"📋 <b>Проверьте данные сделки:</b>\n\n{deal_info}"
        f"🔗 <b>Ссылка на группу:</b> {group_link}\n\n",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "confirm_deal_creation")
async def confirm_deal_creation(callback: CallbackQuery, state: FSMContext):
    """Подтверждение создания сделки создателем"""
    await callback.answer()
    state_data = await state.get_data()
    
    # Создаем уникальный номер сделки (6 цифр)
    deal_id = generate_deal_number()
    
    # Проверяем, что номер уникален
    while get_deal(deal_id):
        deal_id = generate_deal_number()
    
    # Сохраняем сделку в базу
    deal_data = {
        'id': deal_id,
        'creator_id': callback.from_user.id,
        'creator_role': state_data['creator_role'],
        'buyer_id': state_data['buyer_id'],
        'seller_id': state_data['seller_id'],
        'buyer_username': state_data['buyer_username'],
        'seller_username': state_data['seller_username'],
        'amount': state_data['amount'],
        'description': state_data['description'],
        'deadline_days': state_data['deadline_days'],
        'group_link': state_data.get('group_link', '')  # Сохраняем ссылку на группу
    }
    
    if create_deal(deal_data):
        # Отмечаем, что создатель подтвердил сделку
        creator_role = state_data['creator_role']
        set_user_confirmed(deal_id, creator_role)
        
        # Отправляем приглашение второй стороне
        partner_id = state_data['seller_id'] if creator_role == 'buyer' else state_data['buyer_id']
        partner_role = "продавца" if creator_role == 'buyer' else "покупателя"
        creator_username = callback.from_user.username or "пользователь"
        
        invite_keyboard = InlineKeyboardBuilder()
        invite_keyboard.row(InlineKeyboardButton(text="📋 Посмотреть сделку", callback_data=f"view_invite_{deal_id}"))
        invite_keyboard.row(InlineKeyboardButton(text="✅ Принять сделку", callback_data=f"accept_invite_{deal_id}"))
        invite_keyboard.row(InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_invite_{deal_id}"))
        
        try:
            await bot.send_message(
                chat_id=partner_id,
                text=f"🤝 <b>Приглашение к сделке</b>\n\n"
                     f"@{creator_username} приглашает вас стать {partner_role} в сделке.\n\n"
                     f"💼 <b>Сумма:</b> {state_data['amount']} руб.\n"
                     f"📝 <b>Описание:</b> {state_data['description']}\n"
                     f"🔗 <b>Группа для обсуждения:</b> {state_data.get('group_link', 'Не указана')}\n\n"
                     f"Для подтверждения участия нажмите кнопку ниже:",
                reply_markup=invite_keyboard.as_markup(),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка отправки приглашения пользователю {partner_id}: {e}")
            await callback.message.answer(
                f"❌ Не удалось отправить приглашение пользователю. "
                f"Убедитесь, что он начал диалог с ботом."
            )
            await state.clear()
            return
        
        await callback.message.edit_text(
            f"✅ <b>Сделка создана и ожидает подтверждения второй стороны!</b>\n\n"
            f"🆔 <b>Номер сделки:</b> <code>#{deal_id}</code>\n"
            f"👤 <b>Вторая сторона:</b> @{state_data['seller_username'] if creator_role == 'buyer' else state_data['buyer_username']}\n"
            f"🔗 <b>Группа сделки:</b> {state_data.get('group_link', 'Не указана')}\n\n"
            f"Ожидайте подтверждения от второй стороны.",
            parse_mode="HTML"
        )
        
        logger.info(f"Сделка #{deal_id} создана, приглашение отправлено второй стороне")
    else:
        await callback.message.edit_text(
            "❌ Произошла ошибка при создании сделки. Попробуйте позже.",
            reply_markup=InlineKeyboardBuilder().row(
                InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main")
            ).as_markup()
        )
    
    await state.clear()

@dp.callback_query(F.data.startswith("view_invite_"))
async def view_deal_invite(callback: CallbackQuery):
    """Просмотр деталей сделки из приглашения"""
    await callback.answer()
    deal_id = callback.data.split("_")[2]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal_info = await format_deal_info(deal)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="✅ Принять сделку", callback_data=f"accept_invite_{deal_id}"))
    keyboard.row(InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_invite_{deal_id}"))
    
    await callback.message.edit_text(
        f"🤝 <b>Детали сделки</b>\n\n{deal_info}\n"
        f"🔗 <b>Группа сделки:</b> {deal.get('group_link', 'Не указана')}\n\n"
        f"Вы хотите принять участие в этой сделке?",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("accept_invite_"))
async def accept_deal_invite(callback: CallbackQuery):
    """Принятие сделки второй стороной"""
    await callback.answer()
    deal_id = callback.data.split("_")[2]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    user_id = callback.from_user.id
    
    # Проверяем, что пользователь является второй стороной
    if user_id not in [deal['buyer_id'], deal['seller_id']]:
        await callback.answer("❌ Вы не являетесь участником этой сделки", show_alert=True)
        return
    
    # Определяем роль пользователя
    user_role = 'buyer' if user_id == deal['buyer_id'] else 'seller'
    
    # Проверяем, не подтверждена ли уже сделка
    if are_both_confirmed(deal_id):
        await callback.answer("✅ Сделка уже подтверждена", show_alert=True)
        return
    
    # Отмечаем подтверждение
    set_user_confirmed(deal_id, user_role)
    
    # Уведомляем создателя
    creator_id = deal['creator_id']
    user_username = callback.from_user.username or "пользователь"
    
    try:
        await bot.send_message(
            chat_id=creator_id,
            text=f"✅ <b>Сделка подтверждена!</b>\n\n"
                 f"👤 @{user_username} подтвердил(а) участие в сделке #{deal_id}\n\n"
                 f"Теперь вы можете перейти в группу сделки и начать обсуждение.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления создателя {creator_id}: {e}")
    
    await callback.message.edit_text(
        f"✅ <b>Вы подтвердили участие в сделке #{deal_id}</b>\n\n"
        f"Теперь вы можете перейти в группу сделки и начать обсуждение:\n"
        f"{deal.get('group_link', 'Ссылка на группу не указана')}",
        parse_mode="HTML"
    )
    
    logger.info(f"Сделка #{deal_id} подтверждена пользователем {user_id}")

@dp.callback_query(F.data.startswith("reject_invite_"))
async def reject_deal_invite(callback: CallbackQuery):
    """Отклонение сделки второй стороной"""
    await callback.answer()
    deal_id = callback.data.split("_")[2]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    user_id = callback.from_user.id
    
    # Проверяем, что пользователь является второй стороной
    if user_id not in [deal['buyer_id'], deal['seller_id']]:
        await callback.answer("❌ Вы не являетесь участником этой сделки", show_alert=True)
        return
    
    # Уведомляем создателя
    creator_id = deal['creator_id']
    user_username = callback.from_user.username or "пользователь"
    
    try:
        await bot.send_message(
            chat_id=creator_id,
            text=f"❌ <b>Сделка отклонена</b>\n\n"
                 f"👤 @{user_username} отклонил(а) приглашение к сделке #{deal_id}",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления создателя {creator_id}: {e}")
    
    # Обновляем статус сделки
    update_deal_status(deal_id, 'rejected')
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="my_deals_callback"))
    keyboard.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(
        "❌ <b>Вы отклонили приглашение к сделке</b>\n\n"
        "Создатель сделки был уведомлен.",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )
    
    logger.info(f"Сделка #{deal_id} отклонена пользователем {user_id}")

@dp.callback_query(F.data == "my_deals_callback")
async def my_deals_callback(callback: CallbackQuery):
    """Обработчик кнопки 'Мои сделки'"""
    await callback.answer()
    user_id = callback.from_user.id
    await show_user_deals(user_id, callback.message)

async def show_user_deals(user_id: int, message: Message):
    """Показать сделки пользователя"""
    deals = get_user_deals(user_id)
    
    if not deals:
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="📝 Создать сделку", callback_data="create_deal"))
        keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main"))
        
        # Всегда отправляем новое сообщение вместо редактирования
        await message.answer(
            "📋 <b>Мои сделки</b>\n\n"
            "У вас пока нет сделок.",
            reply_markup=keyboard.as_markup(),
            parse_mode="HTML"
        )
        return
    
    keyboard = InlineKeyboardBuilder()
    for deal in deals[:10]:  # Ограничиваем 10 сделками
        status_emoji = {
            'created': '🟡',
            'active': '🟢',
            'completed': '✅',
            'cancelled': '❌',
            'rejected': '🚫',
            'payment_received': '💰',
            'dispute': '🚨'
        }.get(deal['status'], '⚪')
        
        deal_title = f"{status_emoji} Сделка #{deal['id']} - {deal['amount']} руб."
        keyboard.row(InlineKeyboardButton(
            text=deal_title,
            callback_data=f"view_deal_details_{deal['id']}"
        ))
    
    keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main"))
    
    # Всегда отправляем новое сообщение вместо редактирования
    await message.answer(
        f"📋 <b>Мои сделки</b>\n\n"
        f"Найдено сделок: {len(deals)}\n\n"
        f"Выберите сделку для просмотра деталей:",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("view_deal_details_"))
async def view_deal_details(callback: CallbackQuery):
    """Просмотр деталей конкретной сделки"""
    await callback.answer()
    deal_id = callback.data.split("_")[3]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal_info = await format_deal_info(deal)
    
    keyboard = InlineKeyboardBuilder()
    
    user_id = callback.from_user.id
    
    # Если сделка подтверждена обеими сторонами
    if deal['status'] == 'created' and are_both_confirmed(deal_id):
        if user_id == deal['buyer_id']:
            keyboard.row(InlineKeyboardButton(text="💳 Оплатить сделку", callback_data=f"payment_{deal_id}"))
    
    # Если оплата получена
    elif deal['status'] == 'payment_received':
        if user_id == deal['seller_id']:
            keyboard.row(InlineKeyboardButton(text="✅ Работа выполнена", callback_data=f"work_done_{deal_id}"))
        elif user_id == deal['buyer_id']:
            keyboard.row(InlineKeyboardButton(text="✅ Подтвердить получение", callback_data=f"confirm_receipt_{deal_id}"))
    
    # Кнопка спора доступна всегда
    keyboard.row(InlineKeyboardButton(text="🚨 Открыть спор", callback_data=f"dispute_{deal_id}"))
    
    # Кнопка для перехода в группу
    if deal.get('group_link'):
        keyboard.row(InlineKeyboardButton(text="💬 Перейти в группу", url=deal['group_link']))
    
    keyboard.row(InlineKeyboardButton(text="📋 Все сделки", callback_data="my_deals_callback"))
    keyboard.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(
        f"📋 <b>Детали сделки</b>\n\n{deal_info}",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("payment_"))
async def initiate_payment(callback: CallbackQuery):
    """Инициация оплаты сделки"""
    await callback.answer()
    deal_id = callback.data.split("_")[1]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    # Проверяем, что пользователь является покупателем
    if callback.from_user.id != deal['buyer_id']:
        await callback.answer("❌ Только покупатель может оплатить сделку", show_alert=True)
        return
    
    # Проверяем, что сделка готова к оплате
    if deal['status'] != 'created' or not are_both_confirmed(deal_id):
        await callback.answer("❌ Сделка еще не готова к оплате", show_alert=True)
        return
    
    # Создаем платеж через ЮMoney
    payment_url = await yoo_money.create_payment(deal['amount'], deal_id, deal['description'])
    
    if not payment_url:
        await callback.answer("❌ Ошибка создания платежа", show_alert=True)
        return
    
    # Сохраняем URL платежа
    set_payment_url(deal_id, payment_url)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="💳 Перейти к оплате", url=payment_url))
    keyboard.row(InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"confirm_payment_{deal_id}"))
    keyboard.row(InlineKeyboardButton(text="🚨 Открыть спор", callback_data=f"dispute_{deal_id}"))
    
    await callback.message.edit_text(
        f"💳 <b>Оплата сделки #{deal_id}</b>\n\n"
        f"💰 <b>Сумма к оплате:</b> {deal['total_amount']:.2f} руб.\n"
        f"💼 <b>Комиссия гаранта:</b> {deal['guarantor_fee']:.2f} руб.\n\n"
        f"Для оплаты нажмите кнопку ниже:\n",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("confirm_payment_"))
async def confirm_payment(callback: CallbackQuery):
    """Автоматическое подтверждение оплаты через API ЮMoney"""
    await callback.answer()
    deal_id = callback.data.split("_")[2]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔄 <b>Проверяем оплату через ЮMoney...</b>\n\n"
        "Это может занять несколько секунд.",
        parse_mode="HTML"
    )
    
    # АВТОМАТИЧЕСКАЯ проверка оплаты через API ЮMoney
    payment_confirmed = await yoo_money.check_payment(deal_id)
    
    if payment_confirmed:
        set_payment_confirmed(deal_id)
        
        # Уведомляем участников
        for participant_id in [deal['buyer_id'], deal['seller_id']]:
            try:
                await bot.send_message(
                    participant_id,
                    f"✅ <b>Оплата подтверждена!</b>\n\n"
                    f"Покупатель оплатил сделку #{deal_id}\n"
                    f"💰 <b>Сумма:</b> {deal['total_amount']:.2f} руб.\n\n"
                    f"Средства заморожены в системе. Продавец может приступать к работе.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления участника {participant_id}: {e}")
        
        await callback.message.edit_text(
            "✅ <b>Оплата подтверждена!</b>\n\n"
            "Система успешно проверила вашу оплату. Ожидайте выполнения работы продавцом.",
            reply_markup=InlineKeyboardBuilder().row(
                InlineKeyboardButton(text="🚨 Открыть спор", callback_data=f"dispute_{deal_id}")
            ).as_markup()
        )
        
        logger.info(f"Оплата автоматически подтверждена для сделки {deal_id}")
    else:
        await callback.message.edit_text(
            "❌ <b>Оплата не найдена</b>\n\n"
            "Система не обнаружила вашу оплату. Возможно:\n\n"
            "• Платеж еще обрабатывается\n"
            "• Возникла ошибка при оплате\n"
            "• Неверно указаны данные платежа\n\n"
            "Пожалуйста, проверьте:\n"
            "1. Совершили ли вы оплату\n"
            "2. Прошло ли достаточно времени для обработки\n"
            "3. Корректность реквизитов платежа\n\n"
            "Если вы уверены, что оплатили, попробуйте проверить снова через несколько минут.",
            reply_markup=InlineKeyboardBuilder().row(
                InlineKeyboardButton(text="🔄 Проверить снова", callback_data=f"confirm_payment_{deal_id}")
            ).as_markup()
        )

@dp.callback_query(F.data.startswith("work_done_"))
async def work_done(callback: CallbackQuery):
    """Отметка о выполнении работы продавцом"""
    await callback.answer()
    deal_id = callback.data.split("_")[2]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    # Проверяем, что пользователь является продавцом
    if callback.from_user.id != deal['seller_id']:
        await callback.answer("❌ Только продавец может отметить выполнение работы", show_alert=True)
        return
    
    # Проверяем, что оплата получена
    if not deal['payment_confirmed']:
        await callback.answer("❌ Оплата еще не получена", show_alert=True)
        return
    
    # Уведомляем покупателя
    await bot.send_message(
        chat_id=deal['buyer_id'],
        text=f"✅ <b>Работа выполнена!</b>\n\n"
             f"Продавец @{deal['seller_username']} отметил, что работа по сделке #{deal_id} выполнена.\n\n"
             f"Проверьте работу и подтвердите получение.",
        parse_mode="HTML"
    )
    
    await callback.message.edit_text(
        "✅ <b>Вы отметили работу как выполненную!</b>\n\n"
        "Ожидайте подтверждения от покупателя.",
        reply_markup=InlineKeyboardBuilder().row(
            InlineKeyboardButton(text="🚨 Открыть спор", callback_data=f"dispute_{deal_id}")
        ).as_markup()
    )
    
    logger.info(f"Работа отмечена как выполненная для сделки {deal_id}")

@dp.callback_query(F.data.startswith("confirm_receipt_"))
async def confirm_receipt(callback: CallbackQuery):
    """Подтверждение получения работы покупателем"""
    await callback.answer()
    deal_id = callback.data.split("_")[2]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    # Проверяем, что пользователь является покупателем
    if callback.from_user.id != deal['buyer_id']:
        await callback.answer("❌ Только покупатель может подтвердить получение", show_alert=True)
        return
    
    # Зачисляем средства продавцу (за вычетом комиссии)
    seller_amount = deal['amount']
    update_user_balance(deal['seller_id'], seller_amount)
    
    # Обновляем статус сделки
    update_deal_status(deal_id, 'completed')
    
    # Уведомляем продавца
    await bot.send_message(
        chat_id=deal['seller_id'],
        text=f"🎉 <b>Сделка завершена!</b>\n\n"
             f"Покупатель подтвердил получение работы по сделке #{deal_id}\n"
             f"💰 <b>Средства зачислены на ваш баланс:</b> {seller_amount} руб.\n\n"
             f"Вы можете вывести средства через раздел 'Мой баланс'.",
        parse_mode="HTML"
    )
    
    # Запрашиваем отзывы
    review_keyboard = InlineKeyboardBuilder()
    review_keyboard.row(InlineKeyboardButton(text="⭐ Оставить отзыв продавцу", callback_data=f"review_seller_{deal_id}"))
    review_keyboard.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="my_deals_callback"))
    
    await callback.message.edit_text(
        "🎉 <b>Сделка завершена успешно!</b>\n\n"
        "Спасибо за использование нашего сервиса!\n\n"
        "Пожалуйста, оставьте отзыв о продавце:",
        reply_markup=review_keyboard.as_markup(),
        parse_mode="HTML"
    )
    
    logger.info(f"Сделка {deal_id} завершена успешно")

@dp.callback_query(F.data.startswith("review_seller_"))
async def review_seller(callback: CallbackQuery, state: FSMContext):
    """Начало процесса оставления отзыва"""
    await callback.answer()
    deal_id = callback.data.split("_")[2]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    await state.set_state(DealStates.REVIEW_TEXT)
    await state.update_data(
        deal_id=deal_id,
        reviewed_user_id=deal['seller_id']
    )
    
    rating_keyboard = InlineKeyboardBuilder()
    for i in range(1, 6):
        rating_keyboard.row(InlineKeyboardButton(text="⭐" * i, callback_data=f"rating_{i}_{deal_id}"))
    
    await callback.message.edit_text(
        f"⭐ <b>Оставьте отзыв продавцу</b>\n\n"
        f"Сделка: #{deal_id}\n"
        f"Продавец: @{deal['seller_username']}\n\n"
        f"Выберите оценку:",
        reply_markup=rating_keyboard.as_markup(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("rating_"))
async def process_rating(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора рейтинга"""
    await callback.answer()
    parts = callback.data.split("_")
    rating = int(parts[1])
    deal_id = parts[2]
    
    await state.update_data(rating=rating)
    
    await callback.message.edit_text(
        f"⭐ <b>Оценка: {rating}/5</b>\n\n"
        f"Теперь напишите текстовый отзыв о продавце:",
        parse_mode="HTML"
    )

@dp.message(DealStates.REVIEW_TEXT)
async def process_review_text(message: Message, state: FSMContext):
    """Обработка текста отзыва"""
    review_text = message.text.strip()
    state_data = await state.get_data()
    
    # Проверяем, это отзыв или вывод средств
    if 'withdraw_amount' in state_data:
        # Это запрос на вывод средств
        await process_withdrawal_wallet(message, state)
        return
    
    # Это отзыв
    if len(review_text) < 5:
        await message.answer("❌ Отзыв слишком короткий. Напишите более развернутый отзыв:")
        return
    
    # Проверяем, что у нас есть все необходимые данные для отзыва
    if 'deal_id' not in state_data or 'reviewed_user_id' not in state_data or 'rating' not in state_data:
        await message.answer("❌ Ошибка: данные отзыва не найдены. Попробуйте начать заново.")
        await state.clear()
        return
    
    # Сохраняем отзыв
    review_data = {
        'deal_id': state_data['deal_id'],
        'reviewer_id': message.from_user.id,
        'reviewed_user_id': state_data['reviewed_user_id'],
        'review_text': review_text,
        'rating': state_data['rating']
    }
    
    add_review(review_data)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="my_deals_callback"))
    keyboard.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main"))
    
    await message.answer(
        "✅ <b>Спасибо за ваш отзыв!</b>\n\n"
        "Ваш отзыв был сохранен в профиле продавца.",
        reply_markup=keyboard.as_markup(),
        parse_mode="HTML"
    )
    
    await state.clear()

@dp.callback_query(F.data.startswith("dispute_"))
async def open_dispute(callback: CallbackQuery):
    """Открытие спора по сделке"""
    await callback.answer()
    deal_id = callback.data.split("_")[1]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    # Проверяем, что пользователь является участником сделки
    user_id = callback.from_user.id
    if user_id not in [deal['buyer_id'], deal['seller_id']]:
        await callback.answer("❌ Вы не являетесь участником этой сделки", show_alert=True)
        return
    
    # Отмечаем сделку как спорную
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE deals SET dispute_opened = TRUE, status = ? WHERE id = ?', ('dispute', deal_id))
    conn.commit()
    conn.close()
    
    # Формируем ссылку на группу
    group_link = deal.get('group_link', '')
    
    # Уведомляем администраторов с ссылкой на группу
    user_username = callback.from_user.username or "No username"
    
    admin_keyboard = InlineKeyboardBuilder()
    if group_link:
        admin_keyboard.row(InlineKeyboardButton(text="💬 Перейти в чат сделки", url=group_link))
    admin_keyboard.row(InlineKeyboardButton(text="💸 Вернуть деньги покупателю", callback_data=f"admin_refund_{deal_id}"))
    admin_keyboard.row(InlineKeyboardButton(text="💰 Отправить деньги продавцу", callback_data=f"admin_pay_{deal_id}"))
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"🚨 <b>ОТКРЫТ СПОР!</b>\n\n"
                f"🆔 <b>Сделка:</b> #{deal_id}\n"
                f"👤 <b>Инициатор:</b> @{user_username}\n"
                f"💼 <b>Сумма:</b> {deal['amount']} руб.\n"
                f"🔗 <b>Чат сделки:</b> {group_link or 'Не указан'}\n\n"
                f"<i>Администратор вызван в чат сделки. Участники должны детально описать проблему.</i>",
                reply_markup=admin_keyboard.as_markup(),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления администратора {admin_id}: {e}")
    
    # Уведомляем участников сделки
    dispute_keyboard = InlineKeyboardBuilder()
    if group_link:
        dispute_keyboard.row(InlineKeyboardButton(text="💬 Перейти в чат", url=group_link))
    
    for participant_id in [deal['buyer_id'], deal['seller_id']]:
        if participant_id != user_id:  # Не отправляем инициатору спора
            try:
                await bot.send_message(
                    participant_id,
                    f"🚨 <b>Открыт спор по сделке #{deal_id}</b>\n\n"
                    f"Администратор вызван в чат сделки. Пожалуйста, перейдите в группу и детально опишите проблему, "
                    f"чтобы администратор мог быстро разобраться в ситуации.",
                    reply_markup=dispute_keyboard.as_markup(),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления участника {participant_id}: {e}")
    
    await callback.message.edit_text(
        f"🚨 <b>Спор по сделке #{deal_id} открыт!</b>\n\n"
        f"Администратор уведомлен и скоро подключится к разбирательству.\n\n"
        f"Пожалуйста, перейдите в группу сделки и детально опишите проблему.",
        reply_markup=dispute_keyboard.as_markup() if group_link else None,
        parse_mode="HTML"
    )
    
    logger.info(f"Спор по сделке #{deal_id} открыт пользователем {user_id}")

@dp.callback_query(F.data.startswith("admin_refund_"))
async def admin_refund(callback: CallbackQuery):
    """Администратор возвращает деньги покупателю"""
    await callback.answer()
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    deal_id = callback.data.split("_")[2]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    # Обновляем статус сделки
    update_deal_status(deal_id, 'cancelled')
    
    # Уведомляем участников
    for participant_id in [deal['buyer_id'], deal['seller_id']]:
        try:
            await bot.send_message(
                participant_id,
                text=f"⚖️ <b>Решение администратора</b>\n\n"
                     f"По результатам рассмотрения спора по сделке #{deal_id}:\n\n"
                     f"✅ <b>Деньги возвращены покупателю</b>\n"
                     f"👤 Покупатель: @{deal['buyer_username']}\n"
                     f"💰 Сумма возврата: {deal['total_amount']:.2f} руб.",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления участника {participant_id}: {e}")
    
    await callback.message.edit_text(
        f"✅ <b>Деньги возвращены покупателю</b>\n\n"
        f"Сделка: #{deal_id}\n"
        f"Покупатель: @{deal['buyer_username']}\n"
        f"Сумма: {deal['total_amount']:.2f} руб.",
        parse_mode="HTML"
    )
    
    logger.info(f"Администратор {callback.from_user.id} вернул деньги покупателю по сделке {deal_id}")

@dp.callback_query(F.data.startswith("admin_pay_"))
async def admin_pay(callback: CallbackQuery):
    """Администратор отправляет деньги продавцу"""
    await callback.answer()
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    deal_id = callback.data.split("_")[2]
    deal = get_deal(deal_id)
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    # Зачисляем средства продавцу
    seller_amount = deal['amount']
    update_user_balance(deal['seller_id'], seller_amount)
    
    # Обновляем статус сделки
    update_deal_status(deal_id, 'completed')
    
    # Уведомляем участников
    for participant_id in [deal['buyer_id'], deal['seller_id']]:
        try:
            await bot.send_message(
                participant_id,
                text=f"⚖️ <b>Решение администратора</b>\n\n"
                     f"По результатам рассмотрения спора по сделке #{deal_id}:\n\n"
                     f"✅ <b>Деньги отправлены продавцу</b>\n"
                     f"👤 Продавец: @{deal['seller_username']}\n"
                     f"💰 Сумма: {seller_amount} руб.\n"
                     f"💼 Комиссия: {deal['guarantor_fee']:.2f} руб.",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления участника {participant_id}: {e}")
    
    await callback.message.edit_text(
        f"✅ <b>Деньги отправлены продавцу</b>\n\n"
        f"Сделка: #{deal_id}\n"
        f"Продавец: @{deal['seller_username']}\n"
        f"Сумма: {seller_amount} руб.",
        parse_mode="HTML"
    )
    
    logger.info(f"Администратор {callback.from_user.id} отправил деньги продавцу по сделке {deal_id}")

@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def handle_group_message(message: Message):
    """Обработчик сообщений в группах"""
    # Если бот упоминается с номером сделки
    if message.text and BOT_USERNAME in message.text and "deal" in message.text.lower():
        # Попробуем найти номер сделки в сообщении
        deal_match = re.search(r'#?(\d{6})', message.text)
        if deal_match:
            deal_id = deal_match.group(1)
            # Создаем фиктивный CommandObject для вызова cmd_deal
            class FakeCommandObject:
                def __init__(self, args):
                    self.args = args
            await cmd_deal(message, FakeCommandObject(args=deal_id))

async def join_deal_via_link(message: Message, deal_id: str, user: types.User):
    """Обработка присоединения к сделке по ссылке"""
    deal = get_deal(deal_id)
    if not deal:
        await message.answer("❌ Сделка не найдена или была удалена.")
        return
    
    # Проверяем, является ли пользователь второй стороной
    if user.id not in [deal['buyer_id'], deal['seller_id']]:
        await message.answer("❌ Вы не являетесь участником этой сделки.")
        return
    
    # Определяем роль пользователя
    user_role = 'buyer' if user.id == deal['buyer_id'] else 'seller'
    
    # Проверяем, не подтверждена ли уже сделка
    if are_both_confirmed(deal_id):
        await message.answer("✅ Сделка уже подтверждена обеими сторонами.")
        return
    
    # Отмечаем подтверждение
    set_user_confirmed(deal_id, user_role)
    
    # Уведомляем создателя
    creator_id = deal['creator_id']
    try:
        await bot.send_message(
            chat_id=creator_id,
            text=f"✅ <b>Сделка подтверждена!</b>\n\n"
                 f"👤 @{user.username or 'пользователь'} подтвердил(а) участие в сделке #{deal_id}\n\n"
                 f"Теперь вы можете перейти в группу сделки и начать обсуждение.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления создателя {creator_id}: {e}")
    
    await message.answer(
        f"✅ <b>Вы подтвердили участие в сделке #{deal_id}</b>\n\n"
        f"Теперь вы можете перейти в группу сделки и начать обсуждение:\n"
        f"{deal.get('group_link', 'Ссылка на группу не указана')}",
        parse_mode="HTML"
    )
    
    logger.info(f"Пользователь {user.id} подтвердил сделку {deal_id} через ссылку")

# Запуск бота
async def robokassa_handler(request):
    """Лёгкий обработчик уведомлений от Робокассы"""
    try:
        data = await request.post()
        logger.info(f"Робокасса уведомление: {dict(data)}")

        inv_id = data.get('InvId', '')
        out_sum = data.get('OutSum', '')
        # deal_id передаётся в Shp_deal или извлекается из InvId
        shp_deal = data.get('Shp_deal', '') or data.get('shp_deal', '')

        logger.info(f"Оплата получена: сделка={shp_deal}, сумма={out_sum}, InvId={inv_id}")
        return web.Response(text=f"OK{inv_id}")
    except Exception as e:
        logger.error(f"Ошибка обработки Робокассы: {e}")
        return web.Response(text="OK")

async def health_handler(request):
    return web.Response(text="OK")

async def start_web_server():
    """Запуск лёгкого веб-сервера для Робокассы"""
    app = web.Application()
    app.router.add_post('/webhook', robokassa_handler)
    app.router.add_get('/webhook', health_handler)
    app.router.add_get('/', health_handler)

    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Веб-сервер запущен на порту {port}")

async def main():
    logger.info("Запуск OnlineSoprovod с обновленной системой отзывов и сделок...")

    # Сбрасываем webhook чтобы polling работал чисто
    await bot.delete_webhook(drop_pending_updates=True)

    # Запускаем веб-сервер для Робокассы в фоне
    await start_web_server()

    # Запускаем polling
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())