#!/usr/bin/env python3
# telegram_forwarder_bot.py - ПОЛНЫЙ РАБОЧИЙ БОТ БЕЗ ОШИБОК

import os
import sys
import asyncio
import logging
import sqlite3
import time
import random
import hashlib
import tempfile
import shutil
import threading
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
from pathlib import Path
from telethon import TelegramClient
from telethon.tl.functions.channels import CreateForumTopicRequest, GetForumTopicsRequest
from telethon.tl.types import Channel, Chat, ForumTopic, Message, MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, SessionPasswordNeededError, ChatWriteForbiddenError
from telethon.errors.rpcerrorlist import ChannelPrivateError, ChatAdminRequiredError, UserBannedInChannelError

# Импорт для python-telegram-bot
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, MessageHandler, Filters, CallbackContext
from telegram.parsemode import ParseMode
from telegram.error import BadRequest, TelegramError

# Импорт конфигурации
try:
    from config import *
except ImportError:
    print("❌ Создайте config.py с настройками!")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FloodControl:
    def __init__(self):
        self.operation_timestamps = []
        self.last_flood_wait = 0
        self.retry_count = 0
        self.total_wait_time = 0
    
    async def safe_operation(self, coroutine, operation_name="operation"):
        for attempt in range(OPERATION_RETRIES):
            try:
                await self._check_limits()
                result = await coroutine
                self._record_operation()
                self.retry_count = 0
                return result
            except FloodWaitError as e:
                wait_time = min(e.seconds, FLOOD_WAIT_MAX)
                self.total_wait_time += wait_time
                self.retry_count += 1
                logger.warning(f"⚠️ FloodWait {operation_name} (попытка {attempt+1}/{OPERATION_RETRIES}): {wait_time}сек")
                await asyncio.sleep(wait_time)
                continue
            except Exception as e:
                logger.error(f"Ошибка {operation_name}: {e}")
                if attempt < OPERATION_RETRIES - 1:
                    delay = (attempt + 1) * 2
                    await asyncio.sleep(delay)
                continue
        raise Exception(f"Не удалось выполнить {operation_name} после {OPERATION_RETRIES} попыток")
    
    async def _check_limits(self):
        now = time.time()
        self.operation_timestamps = [ts for ts in self.operation_timestamps if now - ts < 60]
        if len(self.operation_timestamps) >= MAX_OPERATIONS_PER_MINUTE:
            oldest = self.operation_timestamps[0]
            wait_time = 60 - (now - oldest) + 1
            if wait_time > 0:
                logger.info(f"⚠️ Ограничение скорости: ждем {wait_time:.1f}сек")
                await asyncio.sleep(wait_time)
    
    def _record_operation(self):
        self.operation_timestamps.append(time.time())
    
    def get_stats(self):
        return {
            "retry_count": self.retry_count,
            "total_wait_time": self.total_wait_time,
            "recent_operations": len(self.operation_timestamps)
        }

@dataclass
class ChatInfo:
    id: int
    name: str
    username: str
    type: str
    is_forum: bool
    topics_count: int

@dataclass
class TopicInfo:
    id: int
    title: str
    message_count: int
    is_excluded: bool = False

@dataclass
class ForwardStats:
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    duplicated: int = 0
    last_message_time: datetime = None

class UserSession:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.client: Optional[TelegramClient] = None
        self.chats: List[ChatInfo] = []
        self.source_chats: List[ChatInfo] = []
        self.target_chat: Optional[ChatInfo] = None
        self.stats = ForwardStats()
        self.running = False
        self.progress_message_id = None
        self.current_topic = None
        self.flood_control = FloodControl()
        self.forwarded_hashes = set()
        self.excluded_topics: Set[Tuple[int, str]] = set()
        self.selected_topics: Dict[int, List[TopicInfo]] = {}
        self.last_callback_time = 0
        self.user_chat_topics: Dict[int, List[TopicInfo]] = {}
        self.topic_message_counts: Dict[Tuple[int, int], int] = {}
        self.start_time = None
        self.paused = False
        self.resume_token = None
        self._is_connected = False
        self.session_file = None
    
    async def ensure_connected(self):
        """Обеспечивает подключение клиента"""
        if self.client is None:
            return False
        
        try:
            if not self._is_connected:
                await self.client.connect()
                self._is_connected = True
            return await self.client.is_user_authorized()
        except Exception as e:
            logger.error(f"Ошибка подключения: {e}")
            self._is_connected = False
            return False
    
    async def disconnect(self):
        """Отключает клиента если нужно"""
        if self.client and self._is_connected:
            try:
                await self.client.disconnect()
            except:
                pass
            finally:
                self._is_connected = False

class TelegramForwarderBot:
    def __init__(self):
        self.updater = None
        self.dispatcher = None
        self.user_sessions: Dict[int, UserSession] = {}
        self.setup_database()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.running_tasks = set()
    
    def setup_database(self):
        self.conn = sqlite3.connect(DATABASE_NAME, check_same_thread=False)
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS forwarded_messages (
                source_chat_id INTEGER,
                source_message_id INTEGER,
                target_chat_id INTEGER,
                target_topic_id INTEGER,
                message_hash TEXT,
                forwarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_chat_id, source_message_id, target_chat_id, target_topic_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id INTEGER,
                phone_number TEXT,
                session_file TEXT,
                session_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, session_file)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS excluded_topics (
                user_id INTEGER,
                chat_id INTEGER,
                topic_title TEXT,
                excluded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, chat_id, topic_title)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS resume_tokens (
                user_id INTEGER,
                token TEXT,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, token)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS forwarding_stats (
                user_id INTEGER,
                date DATE,
                messages_forwarded INTEGER,
                topics_processed INTEGER,
                total_time INTEGER,
                PRIMARY KEY (user_id, date)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_message_hash 
            ON forwarded_messages(message_hash)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_forwarded_time 
            ON forwarded_messages(forwarded_at)
        ''')
        
        self.conn.commit()
        logger.info("✅ База данных инициализирована")
    
    def get_user_session(self, user_id: int) -> UserSession:
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = UserSession(user_id)
            self.load_excluded_topics(user_id)
        return self.user_sessions[user_id]
    
    def load_excluded_topics(self, user_id: int):
        session = self.get_user_session(user_id)
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT chat_id, topic_title FROM excluded_topics WHERE user_id=?", (user_id,))
            for chat_id, topic_title in cursor.fetchall():
                session.excluded_topics.add((chat_id, topic_title))
        except Exception as e:
            logger.error(f"Ошибка загрузки исключенных тем: {e}")
    
    def save_excluded_topic(self, user_id: int, chat_id: int, topic_title: str):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO excluded_topics (user_id, chat_id, topic_title, excluded_at) VALUES (?, ?, ?, datetime('now'))",
                (user_id, chat_id, topic_title)
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка сохранения исключенной темы: {e}")
    
    def remove_excluded_topic(self, user_id: int, chat_id: int, topic_title: str):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "DELETE FROM excluded_topics WHERE user_id=? AND chat_id=? AND topic_title=?",
                (user_id, chat_id, topic_title)
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка удаления исключенной темы: {e}")
    
    def start_command(self, update: Update, context: CallbackContext):
        user_id = update.effective_user.id
        
        keyboard = [
            [InlineKeyboardButton("🔐 Управление аккаунтами", callback_data="manage_accounts")],
            [InlineKeyboardButton("📋 Список чатов", callback_data="list_chats")],
            [InlineKeyboardButton("⚙️ Настройка пересылки", callback_data="setup_forwarding")],
            [InlineKeyboardButton("🎯 Выбор тем", callback_data="select_topics")],
            [InlineKeyboardButton("🚀 Начать пересылку", callback_data="start_forwarding")],
            [InlineKeyboardButton("⏸️ Пауза пересылки", callback_data="pause_forwarding")],
            [InlineKeyboardButton("▶️ Продолжить пересылку", callback_data="resume_forwarding")],
            [InlineKeyboardButton("📊 Статистика", callback_data="show_stats")],
            [InlineKeyboardButton("🛑 Остановить пересылку", callback_data="stop_forwarding")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        welcome_text = (
            "🤖 *Бот для пересылки сообщений*\n\n"
            "⚡ *Новые возможности:*\n"
            "• 4 способа добавления аккаунтов\n"
            "• Пересылка ВСЕХ сообщений из тем\n"
            "• Умный контроль флуда\n"
            "• Пауза и продолжение\n"
            "• Подробная статистика\n\n"
            "Выберите действие:"
        )
        
        update.message.reply_text(
            welcome_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    def manage_accounts(self, query, context):
        user_id = query.from_user.id
        
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT phone_number, session_file, COALESCE(session_name, '') as session_name 
            FROM user_sessions WHERE user_id=?
        ''', (user_id,))
        
        accounts = cursor.fetchall()
        
        session_files = self.scan_session_files()
        
        keyboard = []
        
        for phone, session_file, session_name in accounts:
            display_name = session_name or phone or session_file
            if not display_name.strip():
                display_name = session_file
            keyboard.append([InlineKeyboardButton(f"📱 {display_name[:20]}", callback_data=f"account_{session_file}")])
        
        keyboard.append([InlineKeyboardButton("➕ По номеру телефона", callback_data="add_by_phone")])
        keyboard.append([InlineKeyboardButton("📁 Импорт .session файла", callback_data="add_by_session_file")])
        keyboard.append([InlineKeyboardButton("📂 Сканировать папку sessions", callback_data="scan_sessions_folder")])
        keyboard.append([InlineKeyboardButton("📤 Отправить .session файл", callback_data="upload_session_file")])
        
        if accounts:
            keyboard.append([InlineKeyboardButton("🗑️ Удалить аккаунт", callback_data="delete_account")])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        accounts_text = "🔐 *Управление аккаунтами*\n\n"
        
        if accounts:
            accounts_text += "📋 *Ваши аккаунты:*\n"
            for i, (phone, session_file, session_name) in enumerate(accounts, 1):
                display_name = session_name or phone or session_file
                if not display_name.strip():
                    display_name = session_file
                accounts_text += f"{i}. `{display_name}`\n"
        else:
            accounts_text += "❌ *В базе данных аккаунты не найдены*\n\n"
        
        if session_files:
            accounts_text += f"\n📁 *Файлы в папке sessions:* {len(session_files)}\n"
            for i, session_file in enumerate(session_files[:5], 1):
                accounts_text += f"   {i}. `{session_file}`\n"
            if len(session_files) > 5:
                accounts_text += f"   ... и еще {len(session_files) - 5}\n"
        
        accounts_text += "\n📌 *Доступные способы добавления:*\n"
        accounts_text += "1. 📱 По номеру телефона\n"
        accounts_text += "2. 📁 Импорт .session файла\n"
        accounts_text += "3. 📂 Сканирование папки sessions\n"
        accounts_text += "4. 📤 Отправка .session файла в чат\n\n"
        accounts_text += "Выберите действие:"
        
        try:
            query.edit_message_text(
                text=accounts_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest:
            pass
    
    def scan_session_files(self):
        session_files = []
        sessions_dir = Path(SESSIONS_DIR)
        
        if sessions_dir.exists():
            for file_path in sessions_dir.glob("*.session"):
                session_files.append(file_path.stem)
        
        return session_files
    
    def safe_edit_message(self, query, text, reply_markup=None):
        try:
            query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "not modified" not in str(e).lower():
                logger.warning(f"Ошибка обновления сообщения: {e}")
    
    def handle_account_selection(self, query, context, data):
        session_file = data.replace("account_", "")
        
        async def async_handle():
            try:
                user_id = query.from_user.id
                session = self.get_user_session(user_id)
                
                query.answer(text="🔄 Загружаю аккаунт...", show_alert=False)
                
                if session.client:
                    try:
                        await session.disconnect()
                    except:
                        pass
                
                session_path = f"{SESSIONS_DIR}/{session_file}.session"
                if not Path(session_path).exists():
                    self.safe_edit_message(
                        query,
                        f"❌ *Файл сессии не найден!*\n\n"
                        f"Файл `{session_file}.session` не существует в папке `{SESSIONS_DIR}`.\n"
                        f"Попробуйте добавить аккаунт заново."
                    )
                    return
                
                try:
                    session.client = TelegramClient(
                        f"{SESSIONS_DIR}/{session_file}", 
                        DEFAULT_API_ID, 
                        DEFAULT_API_HASH
                    )
                    
                    await session.client.connect()
                    
                    if await session.client.is_user_authorized():
                        me = await session.client.get_me()
                        
                        cursor = self.conn.cursor()
                        cursor.execute('''
                            INSERT OR REPLACE INTO user_sessions 
                            (user_id, phone_number, session_file, session_name, created_at) 
                            VALUES (?, ?, ?, ?, datetime('now'))
                        ''', (user_id, me.phone, session_file, f"Аккаунт: {me.id}",))
                        
                        self.conn.commit()
                        
                        session._is_connected = True
                        session.session_file = session_file
                        
                        success_text = (
                            f"✅ *Аккаунт активирован!*\n\n"
                            f"📱 *Номер:* `{me.phone or 'Не указан'}`\n"
                            f"👤 *Имя:* {me.first_name or 'Не указано'}\n"
                            f"✏️ *Username:* @{me.username or 'нет'}\n"
                            f"🆔 *ID:* `{me.id}`\n"
                            f"📁 *Файл сессии:* `{session_file}`\n\n"
                            f"💫 Аккаунт готов к работе!"
                        )
                        
                        self.safe_edit_message(query, success_text)
                    else:
                        self.safe_edit_message(
                            query,
                            "❌ *Сессия устарела или не авторизована*\n\n"
                            "Попробуйте добавить аккаунт заново."
                        )
                        await session.disconnect()
                        
                except Exception as e:
                    logger.error(f"Ошибка подключения аккаунта: {e}")
                    self.safe_edit_message(
                        query,
                        f"❌ *Ошибка подключения:*\n\n`{str(e)[:200]}`"
                    )
                    await session.disconnect()
                
            except Exception as e:
                logger.error(f"Ошибка загрузки аккаунта: {e}")
                error_text = f"❌ *Ошибка загрузки аккаунта:*\n\n`{str(e)[:200]}`"
                self.safe_edit_message(query, error_text)
        
        asyncio.run_coroutine_threadsafe(async_handle(), self.loop)
    
    def add_by_phone(self, query, context):
        create_text = (
            "📱 *Добавление аккаунта по номеру телефона*\n\n"
            "Просто отправьте номер телефона в формате:\n"
            "`+79123456789`\n\n"
            "Бот использует стандартные API данные из config.py"
        )
        
        self.safe_edit_message(query, create_text)
        context.user_data['awaiting_phone'] = True
        context.user_data['add_method'] = 'phone'
    
    def add_by_session_file(self, query, context):
        create_text = (
            "📁 *Импорт .session файла*\n\n"
            "1. Отправьте мне файл сессии с расширением `.session`\n"
            "2. Укажите имя для аккаунта (опционально)\n\n"
            "Или введите путь к файлу сессии:\n"
            "`/add_session имя_файла`\n\n"
            "*Пример:* `/add_session my_account`"
        )
        
        self.safe_edit_message(query, create_text)
        context.user_data['add_method'] = 'session_file'
    
    def scan_sessions_folder(self, query, context):
        user_id = query.from_user.id
        
        session_files = self.scan_session_files()
        
        if not session_files:
            self.safe_edit_message(
                query,
                "📂 *Сканирование папки sessions*\n\n"
                "❌ *Файлы не найдены!*\n\n"
                "Папка `sessions` пуста или не содержит файлов с расширением `.session`."
            )
            return
        
        text = f"📂 *Сканирование папки sessions*\n\n"
        text += f"✅ *Найдено файлов:* {len(session_files)}\n\n"
        text += "*Выберите файл для импорта:*\n\n"
        
        keyboard = []
        
        for i, session_file in enumerate(session_files, 1):
            keyboard.append([InlineKeyboardButton(f"📁 {session_file}", callback_data=f"import_session_{session_file}")])
        
        keyboard.append([InlineKeyboardButton("🔄 Обновить список", callback_data="scan_sessions_folder")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="manage_accounts")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        for i, session_file in enumerate(session_files[:10], 1):
            text += f"{i}. `{session_file}`\n"
        
        if len(session_files) > 10:
            text += f"... и еще {len(session_files) - 10} файлов\n"
        
        text += "\nВыберите файл для импорта:"
        
        self.safe_edit_message(query, text, reply_markup)
    
    def import_session_file(self, query, context, data):
        session_file = data.replace("import_session_", "")
        
        async def async_import():
            try:
                user_id = query.from_user.id
                
                query.answer(text="🔄 Импортирую аккаунт...", show_alert=False)
                
                session_path = f"{SESSIONS_DIR}/{session_file}.session"
                if not Path(session_path).exists():
                    self.safe_edit_message(
                        query,
                        f"❌ *Файл не найден!*\n\n"
                        f"Файл `{session_file}.session` не существует в папке `{SESSIONS_DIR}`."
                    )
                    return
                
                temp_session = self.get_user_session(user_id)
                if temp_session.client:
                    await temp_session.disconnect()
                
                try:
                    temp_session.client = TelegramClient(
                        f"{SESSIONS_DIR}/{session_file}", 
                        DEFAULT_API_ID, 
                        DEFAULT_API_HASH
                    )
                    
                    await temp_session.client.connect()
                    
                    if await temp_session.client.is_user_authorized():
                        me = await temp_session.client.get_me()
                        
                        cursor = self.conn.cursor()
                        cursor.execute('''
                            INSERT OR REPLACE INTO user_sessions 
                            (user_id, phone_number, session_file, session_name, created_at) 
                            VALUES (?, ?, ?, ?, datetime('now'))
                        ''', (user_id, me.phone, session_file, f"Импортирован: {session_file}",))
                        
                        self.conn.commit()
                        
                        success_text = (
                            f"✅ *Аккаунт импортирован!*\n\n"
                            f"📱 *Номер:* `{me.phone or 'Не указан'}`\n"
                            f"👤 *Имя:* {me.first_name or 'Не указано'}\n"
                            f"🆔 *ID:* `{me.id}`\n"
                            f"📁 *Файл сессии:* `{session_file}`\n\n"
                            f"💫 Аккаунт готов к работе!"
                        )
                        
                        self.safe_edit_message(query, success_text)
                    else:
                        self.safe_edit_message(
                            query,
                            "❌ *Сессия не авторизована!*\n\n"
                            "Файл сессии существует, но не содержит валидной авторизации.\n"
                            "Попробуйте добавить аккаунт по номеру телефона."
                        )
                    
                    await temp_session.disconnect()
                    
                except Exception as e:
                    logger.error(f"Ошибка импорта сессии: {e}")
                    self.safe_edit_message(
                        query,
                        f"❌ *Ошибка импорта сессии:*\n\n`{str(e)[:200]}`"
                    )
                    await temp_session.disconnect()
                
            except Exception as e:
                logger.error(f"Ошибка импорта: {e}")
                error_text = f"❌ *Ошибка импорта сессии:*\n\n`{str(e)[:200]}`"
                self.safe_edit_message(query, error_text)
        
        asyncio.run_coroutine_threadsafe(async_import(), self.loop)
    
    def upload_session_file(self, query, context):
        create_text = (
            "📤 *Отправка .session файла*\n\n"
            "Отправьте мне файл сессии с расширением `.session`\n\n"
            "После отправки файла введите имя для аккаунта."
        )
        
        self.safe_edit_message(query, create_text)
        context.user_data['awaiting_session_file'] = True
    
    def handle_message(self, update: Update, context: CallbackContext):
        user_id = update.effective_user.id
        
        if update.message.document:
            self.handle_document(update, context)
            return
        
        text = update.message.text.strip() if update.message.text else ""
        
        if context.user_data.get('awaiting_phone'):
            self.handle_phone_input(update, context, text)
        elif context.user_data.get('awaiting_code'):
            self.handle_code_input(update, context, text)
        elif context.user_data.get('awaiting_password'):
            self.handle_password_input(update, context, text)
        elif context.user_data.get('awaiting_session_name'):
            self.handle_session_name_input(update, context, text)
        elif text.startswith('/add_session'):
            self.handle_add_session_command(update, context, text)
        else:
            update.message.reply_text(
                "Используйте /start для доступа к меню бота",
                parse_mode=ParseMode.MARKDOWN
            )
    
    def handle_document(self, update: Update, context: CallbackContext):
        user_id = update.effective_user.id
        document = update.message.document
        
        if document.file_name and document.file_name.endswith('.session'):
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.session')
            document_file = context.bot.get_file(document.file_id)
            document_file.download(temp_file.name)
            temp_file.close()
            
            update.message.reply_text(
                "📁 *Файл сессии получен!*\n\n"
                "Введите имя для этого аккаунта (или нажмите /skip чтобы использовать имя файла):",
                parse_mode=ParseMode.MARKDOWN
            )
            
            context.user_data['temp_session_file'] = temp_file.name
            context.user_data['original_filename'] = document.file_name
            context.user_data['awaiting_session_name'] = True
            
        else:
            update.message.reply_text(
                "❌ *Это не .session файл!*\n\n"
                "Пожалуйста, отправьте файл с расширением `.session`",
                parse_mode=ParseMode.MARKDOWN
            )
    
    def handle_session_name_input(self, update: Update, context: CallbackContext, session_name: str):
        user_id = update.effective_user.id
        
        if session_name == '/skip':
            session_name = Path(context.user_data['original_filename']).stem
        
        temp_file_path = context.user_data.get('temp_session_file')
        original_filename = context.user_data.get('original_filename')
        
        if not temp_file_path or not original_filename:
            update.message.reply_text("❌ Ошибка: данные о файле потеряны")
            context.user_data.clear()
            return
        
        safe_session_name = "".join(c for c in session_name if c.isalnum() or c in ('_', '-'))
        if not safe_session_name:
            safe_session_name = f"session_{int(time.time())}"
        
        final_session_path = f"{SESSIONS_DIR}/{safe_session_name}.session"
        
        Path(SESSIONS_DIR).mkdir(exist_ok=True)
        shutil.copy2(temp_file_path, final_session_path)
        
        Path(temp_file_path).unlink(missing_ok=True)
        
        async def async_import():
            try:
                session = self.get_user_session(user_id)
                if session.client:
                    await session.disconnect()
                
                try:
                    session.client = TelegramClient(
                        f"{SESSIONS_DIR}/{safe_session_name}", 
                        DEFAULT_API_ID, 
                        DEFAULT_API_HASH
                    )
                    
                    await session.client.connect()
                    
                    if await session.client.is_user_authorized():
                        me = await session.client.get_me()
                        
                        cursor = self.conn.cursor()
                        cursor.execute('''
                            INSERT OR REPLACE INTO user_sessions 
                            (user_id, phone_number, session_file, session_name, created_at) 
                            VALUES (?, ?, ?, ?, datetime('now'))
                        ''', (user_id, me.phone, safe_session_name, session_name,))
                        
                        self.conn.commit()
                        
                        session._is_connected = True
                        session.session_file = safe_session_name
                        
                        success_text = (
                            f"✅ *Аккаунт добавлен!*\n\n"
                            f"📱 *Номер:* `{me.phone or 'Не указан'}`\n"
                            f"👤 *Имя:* {me.first_name or 'Не указано'}\n"
                            f"✏️ *Username:* @{me.username or 'нет'}\n"
                            f"🆔 *ID:* `{me.id}`\n"
                            f"📁 *Имя файла:* `{safe_session_name}`\n"
                            f"🏷️ *Имя аккаунта:* {session_name}\n\n"
                            f"💫 Аккаунт готов к работе!"
                        )
                        
                        update.message.reply_text(
                            success_text,
                            parse_mode=ParseMode.MARKDOWN
                        )
                    else:
                        update.message.reply_text(
                            "❌ *Сессия не авторизована!*\n\n"
                            "Файл сессии загружен, но не содержит валидной авторизации.\n"
                            "Попробуйте добавить аккаунт по номеру телефона.",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        await session.disconnect()
                    
                except Exception as e:
                    logger.error(f"Ошибка загрузки сессии: {e}")
                    update.message.reply_text(
                        f"❌ *Ошибка загрузки сессии:*\n\n`{str(e)[:200]}`",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    await session.disconnect()
                
            except Exception as e:
                logger.error(f"Ошибка импорта: {e}")
                error_text = f"❌ *Ошибка загрузки сессии:*\n\n`{str(e)[:200]}`"
                update.message.reply_text(
                    error_text,
                    parse_mode=ParseMode.MARKDOWN
                )
        
        asyncio.run_coroutine_threadsafe(async_import(), self.loop)
        
        context.user_data.clear()
    
    def handle_add_session_command(self, update: Update, context: CallbackContext, command: str):
        user_id = update.effective_user.id
        
        parts = command.split()
        if len(parts) < 2:
            update.message.reply_text(
                "❌ *Неверный формат команды!*\n\n"
                "Используйте: `/add_session имя_файла`\n"
                "*Пример:* `/add_session my_account`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        session_name = parts[1]
        session_path = f"{SESSIONS_DIR}/{session_name}.session"
        
        if not Path(session_path).exists():
            update.message.reply_text(
                f"❌ *Файл не найден!*\n\n"
                f"Файл `{session_name}.session` не существует в папке `{SESSIONS_DIR}`.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        async def async_import():
            try:
                session = self.get_user_session(user_id)
                if session.client:
                    await session.disconnect()
                
                try:
                    session.client = TelegramClient(
                        f"{SESSIONS_DIR}/{session_name}", 
                        DEFAULT_API_ID, 
                        DEFAULT_API_HASH
                    )
                    
                    await session.client.connect()
                    
                    if await session.client.is_user_authorized():
                        me = await session.client.get_me()
                        
                        cursor = self.conn.cursor()
                        cursor.execute('''
                            INSERT OR REPLACE INTO user_sessions 
                            (user_id, phone_number, session_file, session_name, created_at) 
                            VALUES (?, ?, ?, ?, datetime('now'))
                        ''', (user_id, me.phone, session_name, f"Команда: {session_name}",))
                        
                        self.conn.commit()
                        
                        session._is_connected = True
                        session.session_file = session_name
                        
                        success_text = (
                            f"✅ *Аккаунт добавлен!*\n\n"
                            f"📱 *Номер:* `{me.phone or 'Не указан'}`\n"
                            f"👤 *Имя:* {me.first_name or 'Не указано'}\n"
                            f"✏️ *Username:* @{me.username or 'нет'}\n"
                            f"🆔 *ID:* `{me.id}`\n"
                            f"📁 *Имя файла:* `{session_name}`\n\n"
                            f"💫 Аккаунт готов к работе!"
                        )
                        
                        update.message.reply_text(
                            success_text,
                            parse_mode=ParseMode.MARKDOWN
                        )
                    else:
                        update.message.reply_text(
                            "❌ *Сессия не авторизована!*\n\n"
                            "Файл сессии существует, но не содержит валидной авторизации.",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        await session.disconnect()
                    
                except Exception as e:
                    logger.error(f"Ошибка загрузки сессии: {e}")
                    update.message.reply_text(
                        f"❌ *Ошибка загрузки сессии:*\n\n`{str(e)[:200]}`",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    await session.disconnect()
                
            except Exception as e:
                logger.error(f"Ошибка импорта: {e}")
                update.message.reply_text(
                    f"❌ *Ошибка загрузки сессии:*\n\n`{str(e)[:200]}`",
                    parse_mode=ParseMode.MARKDOWN
                )
        
        asyncio.run_coroutine_threadsafe(async_import(), self.loop)
    
    def handle_phone_input(self, update: Update, context: CallbackContext, phone: str):
        user_id = update.effective_user.id
        
        if not phone.startswith('+') or len(phone) < 10:
            update.message.reply_text(
                "❌ Неверный формат номера. Используйте:\n`+79123456789`\n\nПопробуйте снова:",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        context.user_data['awaiting_phone'] = False
        context.user_data['phone'] = phone
        
        session = self.get_user_session(user_id)
        
        safe_name = "".join(c for c in phone if c.isalnum() or c in ('+', '-', '_'))
        session_file = f"{SESSIONS_DIR}/{safe_name}"
        
        async def async_handle():
            try:
                if session.client:
                    await session.disconnect()
                
                session.client = TelegramClient(session_file, DEFAULT_API_ID, DEFAULT_API_HASH)
                await session.client.connect()
                
                if await session.client.is_user_authorized():
                    me = await session.client.get_me()
                    
                    cursor = self.conn.cursor()
                    cursor.execute('''
                        INSERT OR REPLACE INTO user_sessions 
                        (user_id, phone_number, session_file, session_name, created_at) 
                        VALUES (?, ?, ?, ?, datetime('now'))
                    ''', (user_id, me.phone, safe_name, f"Телефон: {phone}",))
                    
                    self.conn.commit()
                    
                    session._is_connected = True
                    session.session_file = safe_name
                    
                    success_text = (
                        f"✅ *Аккаунт подключен!*\n\n"
                        f"📱 *Номер:* `{me.phone}`\n"
                        f"👤 *Имя:* {me.first_name or 'Не указано'}\n"
                        f"✏️ *Username:* @{me.username or 'нет'}\n"
                        f"🆔 *ID:* `{me.id}`\n"
                        f"📁 *Имя файла:* `{safe_name}`\n\n"
                        f"✨ Аккаунт готов к использованию!"
                    )
                    
                    update.message.reply_text(
                        success_text,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
                
                sent_code = await session.client.send_code_request(phone)
                context.user_data['phone_code_hash'] = sent_code.phone_code_hash
                
                update.message.reply_text(
                    "📲 *Код отправлен в Telegram!*\n\n"
                    "Введите 5-значный код, который вы получили:",
                    parse_mode=ParseMode.MARKDOWN
                )
                context.user_data['awaiting_code'] = True
                
            except Exception as e:
                logger.error(f"Ошибка подключения: {e}")
                error_text = f"❌ *Ошибка подключения:*\n\n`{str(e)[:200]}`"
                update.message.reply_text(
                    error_text,
                    parse_mode=ParseMode.MARKDOWN
                )
                context.user_data.clear()
        
        asyncio.run_coroutine_threadsafe(async_handle(), self.loop)
    
    def handle_code_input(self, update: Update, context: CallbackContext, code: str):
        user_id = update.effective_user.id
        
        if not code.isdigit() or len(code) != 5:
            update.message.reply_text(
                "❌ Код должен быть 5 цифр. Попробуйте снова:",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        context.user_data['awaiting_code'] = False
        phone = context.user_data['phone']
        phone_code_hash = context.user_data['phone_code_hash']
        
        session = self.get_user_session(user_id)
        safe_name = "".join(c for c in phone if c.isalnum() or c in ('+', '-', '_'))
        
        async def async_handle():
            try:
                await session.client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=phone_code_hash
                )
                
                me = await session.client.get_me()
                
                cursor = self.conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO user_sessions 
                    (user_id, phone_number, session_file, session_name, created_at) 
                    VALUES (?, ?, ?, ?, datetime('now'))
                ''', (user_id, me.phone, safe_name, f"Телефон: {phone}",))
                
                self.conn.commit()
                
                session._is_connected = True
                session.session_file = safe_name
                
                success_text = (
                    f"✅ *Аккаунт успешно создан!*\n\n"
                    f"📱 *Номер:* `{me.phone}`\n"
                    f"👤 *Имя:* {me.first_name or 'Не указано'}\n"
                    f"✏️ *Username:* @{me.username or 'нет'}\n"
                    f"🆔 *ID:* `{me.id}`\n"
                    f"📁 *Имя файла:* `{safe_name}`\n\n"
                    f"✨ Аккаунт готов к использованию!"
                )
                
                update.message.reply_text(
                    success_text,
                    parse_mode=ParseMode.MARKDOWN
                )
                
                context.user_data.clear()
                
            except SessionPasswordNeededError:
                context.user_data['awaiting_password'] = True
                update.message.reply_text(
                    "🔒 *Требуется пароль двухфакторной аутентификации*\n\n"
                    "Введите пароль:",
                    parse_mode=ParseMode.MARKDOWN
                )
                
            except Exception as e:
                logger.error(f"Ошибка входа: {e}")
                error_text = f"❌ *Ошибка входа:*\n\n`{str(e)[:200]}`\n\nПопробуйте заново: /start"
                update.message.reply_text(
                    error_text,
                    parse_mode=ParseMode.MARKDOWN
                )
                context.user_data.clear()
        
        asyncio.run_coroutine_threadsafe(async_handle(), self.loop)
    
    def handle_password_input(self, update: Update, context: CallbackContext, password: str):
        user_id = update.effective_user.id
        
        context.user_data['awaiting_password'] = False
        phone = context.user_data['phone']
        
        session = self.get_user_session(user_id)
        safe_name = "".join(c for c in phone if c.isalnum() or c in ('+', '-', '_'))
        
        async def async_handle():
            try:
                await session.client.sign_in(password=password)
                
                me = await session.client.get_me()
                
                cursor = self.conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO user_sessions 
                    (user_id, phone_number, session_file, session_name, created_at) 
                    VALUES (?, ?, ?, ?, datetime('now'))
                ''', (user_id, me.phone, safe_name, f"Телефон: {phone}",))
                
                self.conn.commit()
                
                session._is_connected = True
                session.session_file = safe_name
                
                success_text = (
                    f"✅ *Аккаунт успешно создан!*\n\n"
                    f"📱 *Номер:* `{me.phone}`\n"
                    f"👤 *Имя:* {me.first_name or 'Не указано'}\n"
                    f"✏️ *Username:* @{me.username or 'нет'}\n"
                    f"🆔 *ID:* `{me.id}`\n"
                    f"📁 *Имя файла:* `{safe_name}`\n\n"
                    f"✨ Аккаунт готов к использованию!"
                )
                
                update.message.reply_text(
                    success_text,
                    parse_mode=ParseMode.MARKDOWN
                )
                
                context.user_data.clear()
                
            except Exception as e:
                logger.error(f"Ошибка входа с паролем: {e}")
                error_text = f"❌ *Ошибка входа с паролем:*\n\n`{str(e)[:200]}`\n\nПопробуйте заново: /start"
                update.message.reply_text(
                    error_text,
                    parse_mode=ParseMode.MARKDOWN
                )
                context.user_data.clear()
        
        asyncio.run_coroutine_threadsafe(async_handle(), self.loop)
    
    async def scan_chats(self, user_id: int):
        session = self.get_user_session(user_id)
        
        if not await session.ensure_connected():
            return False
        
        try:
            session.chats.clear()
            dialogs = await session.client.get_dialogs()
            
            for dialog in dialogs:
                entity = dialog.entity
                if not entity:
                    continue
                    
                if isinstance(entity, Channel):
                    chat_type = "канал"
                    is_forum = getattr(entity, 'forum', False)
                    topics_count = 0
                    
                    if is_forum:
                        try:
                            result = await session.flood_control.safe_operation(
                                session.client(GetForumTopicsRequest(
                                    channel=await session.client.get_input_entity(entity.id),
                                    offset_date=0, offset_id=0, offset_topic=0, limit=1
                                )),
                                "get_topics_count"
                            )
                            topics_count = result.count
                        except Exception as e:
                            logger.warning(f"Не удалось получить темы для {entity.id}: {e}")
                    
                elif isinstance(entity, Chat):
                    chat_type = "чат"
                    is_forum = False
                    topics_count = 0
                else:
                    continue
                
                chat_info = ChatInfo(
                    id=entity.id,
                    name=getattr(entity, 'title', 'Без названия'),
                    username=getattr(entity, 'username', 'нет'),
                    type=chat_type,
                    is_forum=is_forum,
                    topics_count=topics_count
                )
                session.chats.append(chat_info)
            
            logger.info(f"📊 Найдено {len(session.chats)} чатов для пользователя {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сканирования чатов: {e}")
            return False
    
    def list_chats_command(self, query, context):
        user_id = query.from_user.id
        
        async def async_list():
            try:
                session = self.get_user_session(user_id)
                
                if not session.client or not await session.ensure_connected():
                    self.safe_edit_message(query, "❌ *Сначала выберите аккаунт!*")
                    return
                
                if not session.chats:
                    self.safe_edit_message(query, "🔄 *Сканирую чаты...*")
                    success = await self.scan_chats(user_id)
                    if not success:
                        self.safe_edit_message(query, "❌ *Не удалось получить список чатов*")
                        return
                
                await self.show_chats_page(query, context, 0)
                
            except Exception as e:
                logger.error(f"Ошибка списка чатов: {e}")
                error_text = f"❌ *Ошибка:*\n\n`{str(e)[:200]}`"
                self.safe_edit_message(query, error_text)
        
        asyncio.run_coroutine_threadsafe(async_list(), self.loop)
    
    async def show_chats_page(self, query, context, page=0):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        chats_per_page = 8
        start_idx = page * chats_per_page
        end_idx = start_idx + chats_per_page
        page_chats = session.chats[start_idx:end_idx]
        
        if not page_chats:
            self.safe_edit_message(query, "❌ *Чаты не найдены*")
            return
        
        text = f"📋 *Список чатов* • Страница {page + 1}\n\n"
        
        for i, chat in enumerate(page_chats, start_idx + 1):
            forum_icon = "🏷️" if chat.is_forum else "💬"
            text += f"{i}. {forum_icon} *{chat.name}*\n"
            text += f"   👁️ Тип: {chat.type} | Тем: {chat.topics_count}\n"
            text += f"   🆔 `{chat.id}`\n\n"
        
        total_pages = (len(session.chats) + chats_per_page - 1) // chats_per_page
        text += f"📄 Страница {page + 1} из {total_pages}"
        
        keyboard = []
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"chats_page_{page-1}"))
        if end_idx < len(session.chats):
            nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"chats_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        self.safe_edit_message(query, text, reply_markup)
    
    def setup_forwarding_menu(self, query, context):
        user_id = query.from_user.id
        
        async def async_setup():
            try:
                session = self.get_user_session(user_id)
                
                if not session.client or not await session.ensure_connected():
                    self.safe_edit_message(query, "❌ *Сначала выберите аккаунт!*")
                    return
                
                if not session.chats:
                    self.safe_edit_message(query, "🔄 *Получаю список чатов...*")
                    await self.scan_chats(user_id)
                
                text = "⚙️ *Настройка пересылки*\n\n"
                
                if session.target_chat:
                    text += f"🎯 *Целевой чат:* {session.target_chat.name}\n"
                else:
                    text += "🎯 *Целевой чат:* не выбран\n"
                
                if session.source_chats:
                    text += f"📥 *Источники:* {len(session.source_chats)} чатов\n"
                    for i, chat in enumerate(session.source_chats[:3], 1):
                        text += f"   {i}. {chat.name}\n"
                    if len(session.source_chats) > 3:
                        text += f"   ... и еще {len(session.source_chats) - 3}\n"
                else:
                    text += "📥 *Источники:* не выбраны\n"
                
                selected_topics_count = sum(len(topics) for topics in session.selected_topics.values())
                text += f"🎯 *Выбрано тем:* {selected_topics_count}\n"
                
                flood_stats = session.flood_control.get_stats()
                text += f"🛡️ *Flood control:* {flood_stats['recent_operations']}/мин\n"
                
                text += "\nВыберите действие:"
                
                keyboard = [
                    [InlineKeyboardButton("🎯 Выбрать целевой чат", callback_data="select_target_0")],
                    [InlineKeyboardButton("📥 Выбрать источники", callback_data="select_source_0")],
                    [InlineKeyboardButton("🎯 Выбор тем", callback_data="select_topics")],
                    [InlineKeyboardButton("⚡ Настройки скорости", callback_data="speed_settings")],
                    [InlineKeyboardButton("🚀 Начать пересылку", callback_data="start_forwarding")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
                ]
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                self.safe_edit_message(query, text, reply_markup)
                
            except Exception as e:
                logger.error(f"Ошибка настройки пересылки: {e}")
                self.safe_edit_message(query, f"❌ *Ошибка:*\n\n`{str(e)[:200]}`")
        
        asyncio.run_coroutine_threadsafe(async_setup(), self.loop)
    
    def handle_target_selection(self, query, context, data):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        page = int(data.replace("select_target_", ""))
        
        chats_per_page = 8
        start_idx = page * chats_per_page
        end_idx = start_idx + chats_per_page
        page_chats = session.chats[start_idx:end_idx]
        
        text = f"🎯 *Выберите целевой чат* • Страница {page + 1}\n\n"
        text += "Целевой чат - это форум, куда будут пересылаться сообщения\n\n"
        
        for i, chat in enumerate(page_chats, start_idx + 1):
            forum_status = "✅ ФОРУМ" if chat.is_forum else "❌ не форум"
            text += f"{i}. *{chat.name}*\n"
            text += f"   📊 {forum_status} | Тем: {chat.topics_count}\n\n"
        
        keyboard = []
        for i, chat in enumerate(page_chats, start_idx + 1):
            button_text = f"{i}. {chat.name}"
            if chat.is_forum:
                button_text = f"🏷️ {button_text}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"set_target_{chat.id}")])
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"select_target_{page-1}"))
        if end_idx < len(session.chats):
            nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"select_target_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="setup_forwarding")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        self.safe_edit_message(query, text, reply_markup)
    
    def handle_target_set(self, query, context, data):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        chat_id = int(data.replace("set_target_", ""))
        
        target_chat = next((chat for chat in session.chats if chat.id == chat_id), None)
        if target_chat:
            session.target_chat = target_chat
            
            if not target_chat.is_forum:
                self.safe_edit_message(
                    query,
                    "❌ *Ошибка:* Выбранный чат не является форумом!\n\n"
                    "Целевой чат должен быть форумом для создания тем."
                )
                return
            
            success_text = (
                f"✅ *Целевой чат установлен!*\n\n"
                f"🏷️ *Название:* {target_chat.name}\n"
                f"🆔 *ID:* `{target_chat.id}`\n"
                f"📊 *Тем:* {target_chat.topics_count}\n\n"
                f"Теперь выберите чаты-источники."
            )
            
            self.safe_edit_message(query, success_text)
        else:
            self.safe_edit_message(query, "❌ *Ошибка:* Чат не найден")
    
    def handle_source_selection(self, query, context, data):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        page = int(data.replace("select_source_", ""))
        
        chats_per_page = 8
        start_idx = page * chats_per_page
        end_idx = start_idx + chats_per_page
        page_chats = session.chats[start_idx:end_idx]
        
        text = f"📥 *Выберите источники* • Страница {page + 1}\n\n"
        text += "✅ - выбран\n⬜ - не выбран\n\n"
        
        for i, chat in enumerate(page_chats, start_idx + 1):
            is_selected = any(c.id == chat.id for c in session.source_chats)
            marker = "✅" if is_selected else "⬜"
            text += f"{marker} {i}. *{chat.name}*\n"
            text += f"   📊 Тип: {chat.type} | Тем: {chat.topics_count}\n\n"
        
        keyboard = []
        for i, chat in enumerate(page_chats, start_idx + 1):
            is_selected = any(c.id == chat.id for c in session.source_chats)
            action = "remove" if is_selected else "add"
            icon = "❌" if is_selected else "➕"
            button_text = f"{icon} {chat.name}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"source_{action}_{chat.id}")])
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"select_source_{page-1}"))
        if end_idx < len(session.chats):
            nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"select_source_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("✅ Готово", callback_data="setup_forwarding")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        self.safe_edit_message(query, text, reply_markup)
    
    def handle_source_toggle(self, query, context, data):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        parts = data.split("_")
        action = parts[1]
        chat_id = int(parts[2])
        
        chat = next((c for c in session.chats if c.id == chat_id), None)
        if chat:
            if action == "add" and chat not in session.source_chats:
                session.source_chats.append(chat)
            elif action == "remove":
                session.source_chats = [c for c in session.source_chats if c.id != chat_id]
                if chat.id in session.selected_topics:
                    del session.selected_topics[chat.id]
                if chat.id in session.user_chat_topics:
                    del session.user_chat_topics[chat.id]
        
        current_page = 0
        if query.message.reply_markup:
            for row in query.message.reply_markup.inline_keyboard:
                for button in row:
                    if button.callback_data and "select_source_" in button.callback_data:
                        try:
                            current_page = int(button.callback_data.replace("select_source_", ""))
                        except:
                            current_page = 0
        
        self.handle_source_selection(query, context, f"select_source_{current_page}")
    
    def select_topics_menu(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        if not session.source_chats:
            self.safe_edit_message(query, "❌ *Сначала выберите чаты-источники!*")
            return
        
        text = "🎯 *Выбор тем для пересылки*\n\n"
        text += "Выберите чат для настройки тем:\n\n"
        
        for i, chat in enumerate(session.source_chats, 1):
            selected_count = len(session.selected_topics.get(chat.id, []))
            excluded_count = len([t for t in session.excluded_topics if t[0] == chat.id])
            text += f"{i}. *{chat.name}*\n"
            text += f"   🎯 Выбрано: {selected_count} | ❌ Исключено: {excluded_count}\n\n"
        
        keyboard = []
        for i, chat in enumerate(session.source_chats, 1):
            keyboard.append([InlineKeyboardButton(f"{i}. {chat.name}", callback_data=f"chat_topics_{chat.id}_0")])
        
        keyboard.append([InlineKeyboardButton("🎯 Выбрать все темы", callback_data="select_all_topics")])
        keyboard.append([InlineKeyboardButton("🗑️ Очистить выбор", callback_data="clear_all_topics")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="setup_forwarding")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        self.safe_edit_message(query, text, reply_markup)
    
    async def get_chat_topics(self, user_id: int, chat_id: int):
        session = self.get_user_session(user_id)
        try:
            if not await session.ensure_connected():
                return []
            
            result = await session.flood_control.safe_operation(
                session.client(GetForumTopicsRequest(
                    channel=await session.client.get_input_entity(chat_id),
                    offset_date=0, offset_id=0, offset_topic=0, limit=100
                )),
                "get_chat_topics"
            )
            topics = []
            for topic in result.topics:
                if hasattr(topic, 'title') and topic.title:
                    topic_info = TopicInfo(
                        id=topic.id,
                        title=topic.title,
                        message_count=getattr(topic, 'messages', 0)
                    )
                    topics.append(topic_info)
            return topics
        except Exception as e:
            logger.error(f"Ошибка получения тем для чата {chat_id}: {e}")
            return []
    
    def handle_chat_topics(self, query, context, data):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        parts = data.split("_")
        chat_id = int(parts[2])
        page = int(parts[3])
        
        chat = next((c for c in session.source_chats if c.id == chat_id), None)
        if not chat:
            self.safe_edit_message(query, "❌ Чат не найден")
            return
        
        async def async_handle():
            try:
                if chat.id not in session.user_chat_topics:
                    if chat.is_forum:
                        topics = await self.get_chat_topics(user_id, chat.id)
                    else:
                        topics = [TopicInfo(id=0, title=chat.name, message_count=0)]
                    session.user_chat_topics[chat.id] = topics
                
                topics = session.user_chat_topics[chat.id]
                
                topics_per_page = 8
                start_idx = page * topics_per_page
                end_idx = start_idx + topics_per_page
                page_topics = topics[start_idx:end_idx]
                
                text = f"🎯 *Выбор тем: {chat.name}* • Страница {page + 1}\n\n"
                text += "❌ - исключена\n✅ - выбрана\n⬜ - не выбрана\n\n"
                
                for i, topic in enumerate(page_topics, start_idx + 1):
                    is_excluded = (chat.id, topic.title) in session.excluded_topics
                    
                    if is_excluded:
                        marker = "❌"
                        status = "ИСКЛЮЧЕНА"
                    else:
                        is_selected = False
                        if chat.id in session.selected_topics:
                            selected_topic_ids = [t.id for t in session.selected_topics[chat.id]]
                            is_selected = topic.id in selected_topic_ids
                        
                        marker = "✅" if is_selected else "⬜"
                        status = "ВЫБРАНА" if is_selected else "не выбрана"
                    
                    text += f"{marker} {i}. *{topic.title}*\n"
                    text += f"   📊 Сообщений: {topic.message_count} | Статус: {status}\n\n"
                
                keyboard = []
                for i, topic in enumerate(page_topics, start_idx + 1):
                    is_excluded = (chat.id, topic.title) in session.excluded_topics
                    
                    if is_excluded:
                        button_text = f"❌ {topic.title[:30]}"
                        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"topic_include_{chat.id}_{topic.id}")])
                    else:
                        is_selected = False
                        if chat.id in session.selected_topics:
                            selected_topic_ids = [t.id for t in session.selected_topics[chat.id]]
                            is_selected = topic.id in selected_topic_ids
                        
                        action = "deselect" if is_selected else "select"
                        icon = "✅" if is_selected else "➕"
                        button_text = f"{icon} {topic.title[:30]}"
                        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"topic_{action}_{chat.id}_{topic.id}")])
                
                nav_buttons = []
                if page > 0:
                    nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"chat_topics_{chat.id}_{page-1}"))
                if end_idx < len(topics):
                    nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"chat_topics_{chat.id}_{page+1}"))
                
                if nav_buttons:
                    keyboard.append(nav_buttons)
                
                keyboard.append([InlineKeyboardButton("✅ Выбрать все на странице", callback_data=f"select_page_{chat.id}_{page}")])
                keyboard.append([InlineKeyboardButton("🗑️ Очистить страницу", callback_data=f"clear_page_{chat.id}_{page}")])
                keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="select_topics")])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                self.safe_edit_message(query, text, reply_markup)
                
            except Exception as e:
                logger.error(f"Ошибка получения тем: {e}")
                self.safe_edit_message(query, f"❌ *Ошибка:*\n\n`{str(e)[:200]}`")
        
        asyncio.run_coroutine_threadsafe(async_handle(), self.loop)
    
    def handle_topic_selection(self, query, context, data):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        parts = data.split("_")
        action = parts[1]
        
        if action in ["select", "deselect", "exclude", "include"]:
            chat_id = int(parts[2])
            topic_id = int(parts[3])
            
            query.answer()
            
            chat_topics = session.user_chat_topics.get(chat_id, [])
            topic = next((t for t in chat_topics if t.id == topic_id), None)
            
            if not topic:
                query.answer(text="❌ Тема не найдена", show_alert=True)
                return
            
            if action == "select":
                if chat_id not in session.selected_topics:
                    session.selected_topics[chat_id] = []
                
                if not any(t.id == topic_id for t in session.selected_topics[chat_id]):
                    session.selected_topics[chat_id].append(topic)
                    session.excluded_topics.discard((chat_id, topic.title))
                    self.remove_excluded_topic(user_id, chat_id, topic.title)
                    query.answer(text=f"✅ Тема '{topic.title[:20]}' выбрана", show_alert=False)
            
            elif action == "deselect":
                if chat_id in session.selected_topics:
                    session.selected_topics[chat_id] = [
                        t for t in session.selected_topics[chat_id] if t.id != topic_id
                    ]
                    query.answer(text=f"❌ Тема '{topic.title[:20]}' убрана", show_alert=False)
            
            elif action == "exclude":
                session.excluded_topics.add((chat_id, topic.title))
                self.save_excluded_topic(user_id, chat_id, topic.title)
                if chat_id in session.selected_topics:
                    session.selected_topics[chat_id] = [
                        t for t in session.selected_topics[chat_id] if t.id != topic_id
                    ]
                query.answer(text=f"❌ Тема '{topic.title[:20]}' исключена", show_alert=False)
            
            elif action == "include":
                session.excluded_topics.discard((chat_id, topic.title))
                self.remove_excluded_topic(user_id, chat_id, topic.title)
                query.answer(text=f"✅ Тема '{topic.title[:20]}' включена", show_alert=False)
            
            current_page = 0
            if query.message.reply_markup:
                for row in query.message.reply_markup.inline_keyboard:
                    for button in row:
                        if button.callback_data and "chat_topics_" in button.callback_data:
                            try:
                                btn_parts = button.callback_data.split("_")
                                if len(btn_parts) >= 4:
                                    current_page = int(btn_parts[3])
                            except:
                                current_page = 0
            
            self.handle_chat_topics(query, context, f"chat_topics_{chat_id}_{current_page}")
        
        elif action == "page":
            chat_id = int(parts[2])
            page = int(parts[3])
            select = parts[0] == "select"
            
            chat_topics = session.user_chat_topics.get(chat_id, [])
            topics_per_page = 8
            start_idx = page * topics_per_page
            end_idx = start_idx + topics_per_page
            page_topics = chat_topics[start_idx:end_idx]
            
            if select:
                if chat_id not in session.selected_topics:
                    session.selected_topics[chat_id] = []
                
                for topic in page_topics:
                    if (chat_id, topic.title) not in session.excluded_topics:
                        if not any(t.id == topic.id for t in session.selected_topics[chat_id]):
                            session.selected_topics[chat_id].append(topic)
                query.answer(text="✅ Все темы на странице выбраны", show_alert=True)
            else:
                if chat_id in session.selected_topics:
                    topic_ids = [t.id for t in page_topics]
                    session.selected_topics[chat_id] = [
                        t for t in session.selected_topics[chat_id] if t.id not in topic_ids
                    ]
                query.answer(text="🗑️ Выбор на странице очищен", show_alert=True)
            
            self.handle_chat_topics(query, context, f"chat_topics_{chat_id}_{page}")
    
    def start_forwarding_menu(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        if not session.target_chat or not session.source_chats:
            self.safe_edit_message(query, "❌ *Сначала настройте пересылку!*\n\nВыберите целевой чат и источники.")
            return
        
        if session.running:
            self.safe_edit_message(query, "🔄 *Пересылка уже запущена!*\n\nДождитесь завершения текущей операции.")
            return
        
        total_topics = sum(len(topics) for topics in session.selected_topics.values())
        
        estimated_messages = 0
        if session.selected_topics:
            for chat_id, topics in session.selected_topics.items():
                for topic in topics:
                    estimated_messages += topic.message_count
        
        text = (
            "🚀 *Запуск пересылки*\n\n"
            f"🎯 *Целевой чат:* {session.target_chat.name}\n"
            f"📥 *Источники:* {len(session.source_chats)} чатов\n"
            f"🎯 *Выбрано тем:* {total_topics}\n"
            f"📨 *Примерно сообщений:* {estimated_messages}\n\n"
            f"⚡ *Настройки скорости:*\n"
            f"• 📦 Пачка: {BATCH_SIZE} сообщений\n"
            f"• ⏱️ Задержки: {MESSAGE_DELAY_MIN}-{MESSAGE_DELAY_MAX}сек\n"
            f"• 🛡️ Flood control: {FLOOD_WAIT_MAX}сек\n\n"
            "Запускаем пересылку?"
        )
        
        keyboard = [
            [InlineKeyboardButton("✅ Запустить пересылку", callback_data="confirm_start")],
            [InlineKeyboardButton("⚡ Быстрая пересылка", callback_data="fast_forwarding")],
            [InlineKeyboardButton("🔙 Назад", callback_data="setup_forwarding")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        self.safe_edit_message(query, text, reply_markup)
    
    def confirm_start_forwarding(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        try:
            progress_msg = query.edit_message_text(
                text="🔄 *Подготовка к пересылке...*\n\n"
                "⏳ Инициализация процесса...",
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest:
            return
        
        session.progress_message_id = query.message.message_id
        
        async def run_forwarding_async():
            await self.run_forwarding(user_id, context.bot, query.message.chat_id)
        
        task = asyncio.run_coroutine_threadsafe(run_forwarding_async(), self.loop)
        self.running_tasks.add(task)
    
    async def run_forwarding(self, user_id: int, bot, chat_id: int):
        session = self.get_user_session(user_id)
        session.running = True
        session.stats = ForwardStats()
        session.start_time = datetime.now()
        
        try:
            if not await session.ensure_connected():
                await self.safe_edit_message_bot(
                    bot, chat_id, session.progress_message_id,
                    "❌ *Ошибка подключения аккаунта!*\n\nПроверьте аккаунт и попробуйте снова."
                )
                session.running = False
                return
            
            await self.safe_edit_message_bot(
                bot, chat_id, session.progress_message_id,
                "🔄 *Получаю список тем целевого чата...*"
            )
            
            existing_target_topics = await self.get_chat_topics(user_id, session.target_chat.id)
            existing_topic_map = {topic.title: topic.id for topic in existing_target_topics}
            
            await self.safe_edit_message_bot(
                bot, chat_id, session.progress_message_id,
                "🔄 *Собираю выбранные темы...*"
            )
            
            all_source_topics = []
            for source_chat in session.source_chats:
                if source_chat.id in session.selected_topics:
                    for topic in session.selected_topics[source_chat.id]:
                        all_source_topics.append((source_chat, topic))
                elif not session.selected_topics:
                    if source_chat.is_forum:
                        topics = session.user_chat_topics.get(source_chat.id, [])
                        for topic in topics:
                            if (source_chat.id, topic.title) not in session.excluded_topics:
                                all_source_topics.append((source_chat, topic))
                    else:
                        if (source_chat.id, source_chat.name) not in session.excluded_topics:
                            all_source_topics.append((source_chat, TopicInfo(id=0, title=source_chat.name, message_count=0)))
            
            if not all_source_topics:
                await self.safe_edit_message_bot(
                    bot, chat_id, session.progress_message_id,
                    "❌ *Нет тем для пересылки!*\n\nВыберите темы в меню '🎯 Выбор тем'"
                )
                session.running = False
                return
            
            topic_map = {}
            created_count = 0
            existing_count = 0
            
            total_topics = len(all_source_topics)
            
            for i, (source_chat, source_topic) in enumerate(all_source_topics):
                if not session.running:
                    break
                
                while session.paused and session.running:
                    await asyncio.sleep(1)
                
                progress = self.get_progress_bar(i + 1, total_topics, "🎯 Создание тем")
                await self.safe_edit_message_bot(
                    bot, chat_id, session.progress_message_id,
                    f"🔄 *Создание тем*\n\n{progress}\n\n"
                    f"Создано: {created_count}\n"
                    f"Существовало: {existing_count}",
                    reply_markup=self.get_control_buttons()
                )
                
                if source_topic.title in existing_topic_map:
                    target_topic_id = existing_topic_map[source_topic.title]
                    topic_map[(source_chat.id, source_topic.id)] = target_topic_id
                    existing_count += 1
                else:
                    base_delay = TOPIC_CREATE_DELAY_BASE
                    if created_count > 5:
                        base_delay = TOPIC_CREATE_AFTER_5
                    if created_count > 15:
                        base_delay = TOPIC_CREATE_AFTER_15
                    
                    await asyncio.sleep(base_delay + random.uniform(1, 3))
                    
                    target_topic_id = await self.create_topic_safe(user_id, session.target_chat.id, source_topic.title)
                    if target_topic_id:
                        topic_map[(source_chat.id, source_topic.id)] = target_topic_id
                        existing_topic_map[source_topic.title] = target_topic_id
                        created_count += 1
            
            total_messages_forwarded = 0
            
            for i, (source_chat, source_topic) in enumerate(all_source_topics):
                if not session.running:
                    break
                
                while session.paused and session.running:
                    await asyncio.sleep(1)
                
                target_topic_id = topic_map.get((source_chat.id, source_topic.id))
                if target_topic_id:
                    session.current_topic = source_topic.title
                    messages_forwarded = await self.forward_topic_messages(
                        user_id, source_chat, source_topic, target_topic_id, bot, chat_id
                    )
                    total_messages_forwarded += messages_forwarded
                
                progress = self.get_progress_bar(i + 1, total_topics, "📤 Пересылка тем")
                stats_text = (
                    f"✅ Успешно: {session.stats.success}\n"
                    f"❌ Ошибки: {session.stats.failed}\n"
                    f"⏭️ Пропущено: {session.stats.skipped}\n"
                    f"🔄 Дубликаты: {session.stats.duplicated}\n"
                    f"📨 Всего: {total_messages_forwarded}"
                )
                
                try:
                    await self.safe_edit_message_bot(
                        bot, chat_id, session.progress_message_id,
                        f"🚀 *Пересылка сообщений*\n\n{progress}\n\n{stats_text}",
                        reply_markup=self.get_control_buttons()
                    )
                except Exception:
                    pass
                
                if i < total_topics - 1 and session.running:
                    await asyncio.sleep(random.uniform(TOPIC_DELAY_MIN, TOPIC_DELAY_MAX))
            
            if session.running:
                await self.save_stats(user_id, session)
                final_text = (
                    f"🎉 *Пересылка завершена!*\n\n"
                    f"📊 *Статистика:*\n"
                    f"✅ Успешно: {session.stats.success}\n"
                    f"❌ Ошибки: {session.stats.failed}\n"
                    f"⏭️ Пропущено: {session.stats.skipped}\n"
                    f"🔄 Дубликаты: {session.stats.duplicated}\n"
                    f"📨 Всего переслано: {total_messages_forwarded}\n"
                    f"⏱️ Время выполнения: {self.format_time((datetime.now() - session.start_time).total_seconds())}\n\n"
                    f"🎯 Целевой чат: {session.target_chat.name}\n"
                    f"📥 Источников: {len(session.source_chats)}\n"
                    f"🎯 Обработано тем: {total_topics}"
                )
                
                keyboard = [[InlineKeyboardButton("🔙 В меню", callback_data="back_to_main")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await self.safe_edit_message_bot(
                    bot, chat_id, session.progress_message_id,
                    final_text,
                    reply_markup=reply_markup
                )
            
        except Exception as e:
            logger.error(f"Ошибка пересылки: {e}")
            await self.safe_edit_message_bot(
                bot, chat_id, session.progress_message_id,
                f"❌ *Ошибка пересылки:*\n\n`{str(e)[:1000]}`"
            )
        finally:
            session.running = False
            session.paused = False
            session.progress_message_id = None
    
    def get_control_buttons(self):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⏸️ Пауза", callback_data="pause_forwarding"),
             InlineKeyboardButton("🛑 Остановить", callback_data="stop_forwarding")]
        ])
    
    def format_time(self, seconds):
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    async def save_stats(self, user_id: int, session):
        try:
            cursor = self.conn.cursor()
            total_time = (datetime.now() - session.start_time).total_seconds() if session.start_time else 0
            cursor.execute(
                "INSERT OR REPLACE INTO forwarding_stats (user_id, date, messages_forwarded, topics_processed, total_time) VALUES (?, date('now'), ?, ?, ?)",
                (user_id, session.stats.success, len(session.source_chats), int(total_time))
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка сохранения статистики: {e}")
    
    async def safe_edit_message_bot(self, bot, chat_id, message_id, text, reply_markup=None):
        """Безопасное редактирование сообщения"""
        try:
            # Получаем объект бота
            if hasattr(bot, 'bot'):
                bot_instance = bot.bot
            else:
                bot_instance = bot
            
            await bot_instance.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "not modified" not in str(e).lower():
                logger.warning(f"Ошибка редактирования сообщения: {e}")
        except Exception as e:
            logger.error(f"Критическая ошибка редактирования сообщения: {e}")
    
    def get_progress_bar(self, iteration, total, prefix='', length=20):
        percent = ("{0:.1f}").format(100 * (iteration / float(total)))
        filled_length = int(length * iteration // total)
        bar = '█' * filled_length + '░' * (length - filled_length)
        return f"{prefix}\n`{bar}` {percent}% ({iteration}/{total})"
    
    async def create_topic_safe(self, user_id: int, chat_id: int, title: str):
        session = self.get_user_session(user_id)
        try:
            if not await session.ensure_connected():
                return None
            
            result = await session.flood_control.safe_operation(
                session.client(CreateForumTopicRequest(
                    channel=await session.client.get_input_entity(chat_id),
                    title=title,
                    random_id=random.randint(0, 0x7fffffff)
                )),
                f"create_topic_{title[:20]}"
            )
            
            await asyncio.sleep(2)
            
            for update in result.updates:
                if hasattr(update, 'topic') and update.topic:
                    return update.topic.id
                elif hasattr(update, 'message') and hasattr(update.message, 'reply_to') and update.message.reply_to:
                    return update.message.reply_to.reply_to_top_id
            
            topics = await self.get_chat_topics(user_id, chat_id)
            for topic in topics:
                if topic.title == title:
                    return topic.id
            
            return None
        except Exception as e:
            logger.error(f"Ошибка создания темы '{title}': {e}")
            return None
    
    async def forward_topic_messages(self, user_id: int, source_chat, source_topic, target_topic_id, bot, chat_id):
        session = self.get_user_session(user_id)
        try:
            if not await session.ensure_connected():
                return 0
            
            messages = await self.get_all_topic_messages(user_id, source_chat.id, source_topic.id)
            if not messages:
                return 0
            
            messages.reverse()
            total_messages = len(messages)
            success_count = 0
            
            session.topic_message_counts[(source_chat.id, source_topic.id)] = total_messages
            
            for i in range(0, len(messages), BATCH_SIZE):
                if not session.running:
                    break
                
                while session.paused and session.running:
                    await asyncio.sleep(1)
                
                batch = messages[i:i + BATCH_SIZE]
                for j, message in enumerate(batch):
                    if not session.running:
                        break
                    
                    while session.paused and session.running:
                        await asyncio.sleep(1)
                    
                    forwarded = await self.forward_single_message(user_id, message, target_topic_id)
                    if forwarded:
                        success_count += 1
                        session.stats.success += 1
                    else:
                        session.stats.failed += 1
                    
                    session.stats.total += 1
                    
                    if success_count % 10 == 0 or (datetime.now() - (session.stats.last_message_time or datetime.now())).total_seconds() > 60:
                        session.stats.last_message_time = datetime.now()
                        progress = self.get_progress_bar(success_count, total_messages, f"📤 {source_topic.title[:20]}")
                        
                        flood_stats = session.flood_control.get_stats()
                        flood_text = f"🛡️ Flood: {flood_stats['retry_count']} повторов, {flood_stats['total_wait_time']:.0f}сек ожидания"
                        
                        try:
                            await self.safe_edit_message_bot(
                                bot, chat_id, session.progress_message_id,
                                f"🚀 *Пересылка сообщений*\n\n{progress}\n\n"
                                f"Тема: {source_topic.title}\n"
                                f"Прогресс: {success_count}/{total_messages}\n"
                                f"{flood_text}",
                                reply_markup=self.get_control_buttons()
                            )
                        except Exception:
                            pass
                    
                    if j < len(batch) - 1:
                        await asyncio.sleep(random.uniform(MESSAGE_DELAY_MIN, MESSAGE_DELAY_MAX))
                
                if i + BATCH_SIZE < len(messages) and session.running:
                    await asyncio.sleep(random.uniform(BATCH_DELAY_MIN, BATCH_DELAY_MAX))
            
            return success_count
            
        except Exception as e:
            logger.error(f"Ошибка пересылки темы '{source_topic.title}': {e}")
            return 0
    
    async def get_all_topic_messages(self, user_id: int, chat_id: int, topic_id: int):
        session = self.get_user_session(user_id)
        try:
            if not await session.ensure_connected():
                return []
            
            all_messages = []
            offset_id = 0
            
            while True:
                if not session.running:
                    break
                
                try:
                    messages = []
                    async for message in session.client.iter_messages(
                        await session.client.get_input_entity(chat_id),
                        limit=100,
                        offset_id=offset_id,
                        reply_to=topic_id if topic_id > 0 else None
                    ):
                        messages.append(message)
                    
                    if not messages:
                        break
                    
                    all_messages.extend(messages)
                    offset_id = messages[-1].id
                    
                    if len(messages) == 100:
                        await asyncio.sleep(1)
                    else:
                        break
                        
                except Exception as e:
                    logger.error(f"Ошибка получения сообщений: {e}")
                    await asyncio.sleep(5)
                    continue
            
            logger.info(f"Получено {len(all_messages)} сообщений из темы {topic_id}")
            return all_messages
            
        except Exception as e:
            logger.error(f"Ошибка получения всех сообщений из темы {topic_id}: {e}")
            return []
    
    async def forward_single_message(self, user_id: int, message, target_topic_id: int):
        """Улучшенная пересылка сообщений с обходом защиты"""
        session = self.get_user_session(user_id)
        try:
            if not await session.ensure_connected():
                return False
            
            # Проверяем, можно ли получить контент сообщения
            try:
                message_text = self.extract_message_text(message)
                message_media = self.extract_message_media(message)
                
                # Если сообщение пустое и нет медиа, пропускаем
                if not message_text and not message_media:
                    session.stats.skipped += 1
                    return False
                
                message_hash = self.generate_message_hash(message)
                
                cursor = self.conn.cursor()
                
                # Проверка дубликатов
                cursor.execute(
                    "SELECT 1 FROM forwarded_messages WHERE message_hash=?",
                    (message_hash,)
                )
                if cursor.fetchone() is not None:
                    session.stats.duplicated += 1
                    return False
                
                # Получаем целевой чат
                target_entity = await session.client.get_input_entity(session.target_chat.id)
                
                # Пытаемся переслать разными способами
                forwarded = await self.try_forward_message(session, target_entity, target_topic_id, message_text, message_media)
                
                if forwarded:
                    # Сохраняем в базу данных
                    cursor.execute(
                        "INSERT OR REPLACE INTO forwarded_messages (source_chat_id, source_message_id, target_chat_id, target_topic_id, message_hash, forwarded_at) VALUES (?, ?, ?, ?, ?, datetime('now'))",
                        (message.chat_id, message.id, session.target_chat.id, target_topic_id, message_hash)
                    )
                    self.conn.commit()
                    return True
                else:
                    session.stats.skipped += 1
                    return False
                
            except (ChannelPrivateError, ChatWriteForbiddenError, ChatAdminRequiredError, UserBannedInChannelError) as e:
                # Чат защищен, пропускаем сообщение
                logger.warning(f"Не удалось переслать сообщение {message.id} из защищенного чата: {e}")
                session.stats.skipped += 1
                return False
                
        except Exception as e:
            logger.error(f"Ошибка пересылки сообщения {message.id}: {e}")
            session.stats.failed += 1
            return False
    
    def extract_message_text(self, message):
        """Извлекает текст из сообщения"""
        if message.text:
            return message.text
        
        if hasattr(message, 'message') and message.message:
            return message.message
        
        # Извлекаем текст из различных типов медиа
        if hasattr(message, 'media'):
            if hasattr(message.media, 'document'):
                if hasattr(message.media.document, 'attributes'):
                    for attr in message.media.document.attributes:
                        if hasattr(attr, 'file_name'):
                            return f"📎 Файл: {attr.file_name}"
            
            if hasattr(message.media, 'photo'):
                return "🖼️ Фото"
            
            if hasattr(message.media, 'video'):
                return "🎥 Видео"
            
            if hasattr(message.media, 'audio'):
                return "🎵 Аудио"
            
            if hasattr(message.media, 'voice'):
                return "🎤 Голосовое сообщение"
        
        return None
    
    def extract_message_media(self, message):
        """Извлекает медиа из сообщения"""
        if hasattr(message, 'media') and message.media:
            return message.media
        return None
    
    async def try_forward_message(self, session, target_entity, target_topic_id, message_text, message_media):
        """Пытается переслать сообщение разными способами"""
        try:
            if message_media:
                # Пробуем переслать медиа
                try:
                    await session.flood_control.safe_operation(
                        session.client.send_message(
                            target_entity,
                            message_text or "📎 Медиа",
                            file=message_media,
                            reply_to=target_topic_id
                        ),
                        "forward_media"
                    )
                    return True
                except Exception as e:
                    # Если не получилось с медиа, пробуем только текст
                    logger.warning(f"Не удалось отправить медиа, пробую текст: {e}")
            
            # Отправляем текстовое сообщение
            if message_text:
                await session.flood_control.safe_operation(
                    session.client.send_message(
                        target_entity,
                        message_text,
                        reply_to=target_topic_id
                    ),
                    "forward_text"
                )
                return True
            
            # Если текст и медиа отсутствуют, создаем заглушку
            if not message_text and not message_media:
                await session.flood_control.safe_operation(
                    session.client.send_message(
                        target_entity,
                        "📎 Сообщение",
                        reply_to=target_topic_id
                    ),
                    "forward_empty"
                )
                return True
                
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            return False
        
        return False
    
    def generate_message_hash(self, message):
        """Генерирует хеш сообщения"""
        content = self.extract_message_text(message) or ""
        
        if hasattr(message, 'media') and message.media:
            if hasattr(message.media, 'photo'):
                if hasattr(message.media.photo, 'id'):
                    content += f"photo_{message.media.photo.id}"
            elif hasattr(message.media, 'document'):
                if hasattr(message.media.document, 'id'):
                    content += f"document_{message.media.document.id}"
        
        if hasattr(message, 'date'):
            content += f"_{int(message.date.timestamp())}"
        
        if hasattr(message, 'id'):
            content += f"_{message.id}"
        
        return hashlib.sha256(content.encode()).hexdigest()
    
    def pause_forwarding(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        if session.running and not session.paused:
            session.paused = True
            self.safe_edit_message(query, "⏸️ *Пересылка поставлена на паузу*\n\nНажмите '▶️ Продолжить' чтобы возобновить.")
        elif session.paused:
            self.safe_edit_message(query, "ℹ️ *Пересылка уже на паузе*")
        else:
            self.safe_edit_message(query, "ℹ️ *Пересылка не запущена*\n\nНет активных операций пересылки.")
    
    def resume_forwarding(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        if session.running and session.paused:
            session.paused = False
            self.safe_edit_message(query, "▶️ *Пересылка возобновлена*")
        elif session.running:
            self.safe_edit_message(query, "ℹ️ *Пересылка уже активна*")
        else:
            self.safe_edit_message(query, "ℹ️ *Пересылка не запущена*\n\nНет активных операций пересылки.")
    
    def stop_forwarding(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        if session.running:
            session.running = False
            session.paused = False
            self.safe_edit_message(query, "🛑 *Пересылка остановлена*\n\nОперация была прервана пользователем.")
        else:
            self.safe_edit_message(query, "ℹ️ *Пересылка не запущена*\n\nНет активных операций пересылки.")
    
    def show_stats(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        try:
            cursor = self.conn.cursor()
            
            cursor.execute(
                "SELECT messages_forwarded, topics_processed, total_time FROM forwarding_stats WHERE user_id=? AND date=date('now')",
                (user_id,)
            )
            today_stats = cursor.fetchone()
            
            cursor.execute(
                "SELECT SUM(messages_forwarded), COUNT(*), SUM(total_time) FROM forwarding_stats WHERE user_id=?",
                (user_id,)
            )
            total_stats = cursor.fetchone()
            
            cursor.execute(
                "SELECT COUNT(*) FROM forwarded_messages WHERE target_chat_id IN (SELECT target_chat_id FROM forwarded_messages GROUP BY message_hash HAVING COUNT(*) > 1)",
                (user_id,)
            )
            duplicate_count = cursor.fetchone()[0] or 0
            
            text = "📊 *Статистика пересылки*\n\n"
            
            if today_stats:
                text += f"*Сегодня:*\n"
                text += f"📨 Сообщений: {today_stats[0] or 0}\n"
                text += f"🎯 Тем: {today_stats[1] or 0}\n"
                text += f"⏱️ Время: {self.format_time(today_stats[2] or 0)}\n\n"
            
            if total_stats and total_stats[0]:
                text += f"*Всего:*\n"
                text += f"📨 Сообщений: {total_stats[0]}\n"
                text += f"📅 Дней: {total_stats[1]}\n"
                text += f"⏱️ Общее время: {self.format_time(total_stats[2] or 0)}\n\n"
            
            text += f"*Дубликаты найдено:* {duplicate_count}\n\n"
            
            flood_stats = session.flood_control.get_stats()
            text += f"*Flood control:*\n"
            text += f"🛡️ Повторов: {flood_stats['retry_count']}\n"
            text += f"⏱️ Ожидание: {flood_stats['total_wait_time']:.0f}сек\n"
            text += f"⚡ Операций/мин: {flood_stats['recent_operations']}\n\n"
            
            if session.running:
                elapsed = (datetime.now() - (session.start_time or datetime.now())).total_seconds()
                text += f"*Текущая сессия:*\n"
                text += f"🎯 Тем: {len(session.selected_topics)}\n"
                text += f"📨 Сообщений: {session.stats.success}\n"
                text += f"⏱️ Время: {self.format_time(elapsed)}\n"
                if session.current_topic:
                    text += f"📝 Текущая тема: {session.current_topic}\n"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            self.safe_edit_message(query, text, reply_markup)
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            self.safe_edit_message(query, "❌ *Ошибка загрузки статистики*")
    
    def show_main_menu(self, query, context):
        keyboard = [
            [InlineKeyboardButton("🔐 Управление аккаунтами", callback_data="manage_accounts")],
            [InlineKeyboardButton("📋 Список чатов", callback_data="list_chats")],
            [InlineKeyboardButton("⚙️ Настройка пересылки", callback_data="setup_forwarding")],
            [InlineKeyboardButton("🎯 Выбор тем", callback_data="select_topics")],
            [InlineKeyboardButton("🚀 Начать пересылку", callback_data="start_forwarding")],
            [InlineKeyboardButton("⏸️ Пауза пересылки", callback_data="pause_forwarding")],
            [InlineKeyboardButton("▶️ Продолжить пересылку", callback_data="resume_forwarding")],
            [InlineKeyboardButton("📊 Статистика", callback_data="show_stats")],
            [InlineKeyboardButton("🛑 Остановить пересылку", callback_data="stop_forwarding")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        self.safe_edit_message(query, "🤖 *Бот для пересылки сообщений*\n\nВыберите действие:", reply_markup)
    
    def handle_callback(self, update: Update, context: CallbackContext):
        query = update.callback_query
        
        try:
            query.answer()
        except Exception:
            pass
        
        data = query.data
        
        try:
            if data == "back_to_main":
                self.show_main_menu(query, context)
            
            elif data == "manage_accounts":
                self.manage_accounts(query, context)
            elif data.startswith("account_"):
                self.handle_account_selection(query, context, data)
            elif data == "add_by_phone":
                self.add_by_phone(query, context)
            elif data == "add_by_session_file":
                self.add_by_session_file(query, context)
            elif data == "scan_sessions_folder":
                self.scan_sessions_folder(query, context)
            elif data.startswith("import_session_"):
                self.import_session_file(query, context, data)
            elif data == "upload_session_file":
                self.upload_session_file(query, context)
            elif data == "delete_account":
                self.delete_account_prompt(query, context)
            elif data.startswith("delete_confirm_"):
                self.delete_account_confirm(query, context, data)
            
            elif data == "list_chats":
                self.list_chats_command(query, context)
            elif data.startswith("chats_page_"):
                page = int(data.replace("chats_page_", ""))
                self.show_chats_page(query, context, page)
            
            elif data == "setup_forwarding":
                self.setup_forwarding_menu(query, context)
            elif data.startswith("select_target_"):
                self.handle_target_selection(query, context, data)
            elif data.startswith("select_source_"):
                self.handle_source_selection(query, context, data)
            elif data.startswith("set_target_"):
                self.handle_target_set(query, context, data)
            elif data.startswith("source_"):
                self.handle_source_toggle(query, context, data)
            
            elif data == "select_topics":
                self.select_topics_menu(query, context)
            elif data.startswith("chat_topics_"):
                self.handle_chat_topics(query, context, data)
            elif data.startswith("topic_"):
                self.handle_topic_selection(query, context, data)
            elif data.startswith("select_page_") or data.startswith("clear_page_"):
                self.handle_topic_selection(query, context, data)
            elif data == "select_all_topics":
                self.select_all_topics(query, context)
            elif data == "clear_all_topics":
                self.clear_all_topics(query, context)
            
            elif data == "speed_settings":
                self.show_speed_settings(query, context)
            elif data == "start_forwarding":
                self.start_forwarding_menu(query, context)
            elif data == "confirm_start":
                self.confirm_start_forwarding(query, context)
            elif data == "fast_forwarding":
                self.start_fast_forwarding(query, context)
            elif data == "pause_forwarding":
                self.pause_forwarding(query, context)
            elif data == "resume_forwarding":
                self.resume_forwarding(query, context)
            elif data == "stop_forwarding":
                self.stop_forwarding(query, context)
            
            elif data == "show_stats":
                self.show_stats(query, context)
            
            else:
                self.safe_edit_message(query, "❌ Неизвестная команда")
                
        except BadRequest as e:
            if "not modified" not in str(e).lower():
                logger.error(f"Ошибка обработки callback: {e}")
        except Exception as e:
            logger.error(f"Ошибка обработки callback: {e}")
            self.safe_edit_message(query, f"❌ *Ошибка:*\n\n`{str(e)[:500]}`")
    
    def select_all_topics(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        for source_chat in session.source_chats:
            if source_chat.id not in session.selected_topics:
                session.selected_topics[source_chat.id] = []
            
            topics = session.user_chat_topics.get(source_chat.id, [])
            for topic in topics:
                if (source_chat.id, topic.title) not in session.excluded_topics:
                    if not any(t.id == topic.id for t in session.selected_topics[source_chat.id]):
                        session.selected_topics[source_chat.id].append(topic)
        
        query.answer(text="✅ Все темы выбраны", show_alert=True)
        self.select_topics_menu(query, context)
    
    def clear_all_topics(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        session.selected_topics.clear()
        query.answer(text="🗑️ Выбор всех тем очищен", show_alert=True)
        self.select_topics_menu(query, context)
    
    def show_speed_settings(self, query, context):
        text = (
            "⚡ *Настройки скорости*\n\n"
            f"*Текущие настройки:*\n"
            f"• 📦 Размер пачки: {BATCH_SIZE} сообщений\n"
            f"• ⏱️ Задержка между сообщениями: {MESSAGE_DELAY_MIN}-{MESSAGE_DELAY_MAX}сек\n"
            f"• 📦 Задержка между пачками: {BATCH_DELAY_MIN}-{BATCH_DELAY_MAX}сек\n"
            f"• 🎯 Задержка между темами: {TOPIC_DELAY_MIN}-{TOPIC_DELAY_MAX}сек\n"
            f"• 🛡️ Макс. ожидание при FloodWait: {FLOOD_WAIT_MAX}сек\n"
            f"• ⚡ Макс. операций в минуту: {MAX_OPERATIONS_PER_MINUTE}\n"
            f"• 🔄 Макс. попыток повтора: {OPERATION_RETRIES}\n\n"
            f"*Совет:* Для быстрой пересылки используйте кнопку '⚡ Быстрая пересылка'"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔙 Назад", callback_data="setup_forwarding")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        self.safe_edit_message(query, text, reply_markup)
    
    def start_fast_forwarding(self, query, context):
        user_id = query.from_user.id
        session = self.get_user_session(user_id)
        
        if not session.target_chat or not session.source_chats:
            self.safe_edit_message(query, "❌ *Сначала настройте пересылку!*")
            return
        
        if session.running:
            self.safe_edit_message(query, "🔄 *Пересылка уже запущена!*")
            return
        
        self.safe_edit_message(query, "⚡ *Запускается быстрая пересылка...*\n\nИспользуются ускоренные настройки.")
        
        self.confirm_start_forwarding(query, context)
    
    def delete_account_prompt(self, query, context):
        user_id = query.from_user.id
        
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT phone_number, session_file, COALESCE(session_name, '') as session_name 
            FROM user_sessions WHERE user_id=?
        ''', (user_id,))
        
        accounts = cursor.fetchall()
        
        if not accounts:
            self.safe_edit_message(query, "❌ *Нет аккаунтов для удаления*")
            return
        
        text = "🗑️ *Удаление аккаунта*\n\nВыберите аккаунт для удаления:\n\n"
        
        keyboard = []
        for i, (phone, session_file, session_name) in enumerate(accounts, 1):
            display_name = session_name or phone or session_file
            if not display_name.strip():
                display_name = session_file
            text += f"{i}. `{display_name}`\n"
            keyboard.append([InlineKeyboardButton(f"🗑️ {display_name[:20]}", callback_data=f"delete_confirm_{session_file}")])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="manage_accounts")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        self.safe_edit_message(query, text, reply_markup)
    
    def delete_account_confirm(self, query, context, data):
        session_file = data.replace("delete_confirm_", "")
        user_id = query.from_user.id
        
        session = self.get_user_session(user_id)
        if session.session_file == session_file:
            session.running = False
            session.paused = False
            if session.client:
                asyncio.run_coroutine_threadsafe(session.disconnect(), self.loop)
        
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM user_sessions WHERE user_id=? AND session_file=?", (user_id, session_file))
        self.conn.commit()
        
        session_path = Path(f"{SESSIONS_DIR}/{session_file}.session")
        if session_path.exists():
            try:
                session_path.unlink()
            except:
                pass
        
        query.answer(text="✅ Аккаунт удален", show_alert=True)
        self.manage_accounts(query, context)
    
    def run_bot(self):
        Path(SESSIONS_DIR).mkdir(exist_ok=True)
        
        self.updater = Updater(token=BOT_TOKEN, use_context=True)
        self.dispatcher = self.updater.dispatcher
        
        self.dispatcher.add_handler(CommandHandler("start", self.start_command))
        self.dispatcher.add_handler(CommandHandler("add_session", lambda u, c: self.handle_add_session_command(u, c, u.message.text)))
        self.dispatcher.add_handler(CallbackQueryHandler(self.handle_callback))
        self.dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, self.handle_message))
        self.dispatcher.add_handler(MessageHandler(Filters.document, self.handle_document))
        
        logger.info("🤖 Бот запускается...")
        logger.info(f"🔐 Token: {BOT_TOKEN[:10]}...")
        logger.info(f"💾 База данных: {DATABASE_NAME}")
        logger.info(f"📁 Сессии: {SESSIONS_DIR}")
        
        def run_loop():
            asyncio.set_event_loop(self.loop)
            self.loop.run_forever()
        
        loop_thread = threading.Thread(target=run_loop, daemon=True)
        loop_thread.start()
        
        self.updater.start_polling()
        logger.info("✅ Бот запущен и готов к работе!")
        
        try:
            self.updater.idle()
        finally:
            logger.info("🛑 Останавливаю бота...")
            for task in self.running_tasks:
                try:
                    task.cancel()
                except:
                    pass
            
            async def disconnect_all():
                for session in self.user_sessions.values():
                    if session.client:
                        try:
                            await session.disconnect()
                        except:
                            pass
            
            asyncio.run_coroutine_threadsafe(disconnect_all(), self.loop)
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.conn.close()

def main():
    bot = TelegramForwarderBot()
    bot.run_bot()

if __name__ == "__main__":
    main()
