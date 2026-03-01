import os
import re
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, MessageHandler, CommandHandler, filters,
    ContextTypes, CallbackContext
)
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from typing import Optional, Dict, Tuple
from functools import wraps
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import io

# ======================
# Импорты для визуализации и аналитики
# ======================
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# ======================
# Настройка логгера
# ======================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ======================
# Загрузка конфигурации
# ======================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
RPZ_DISCUSSION_CHAT_ID = int(os.getenv("RPZ_DISCUSSION_CHAT_ID"))
RPZ_ANNOUNCE_CHANNEL_ID = int(os.getenv("RPZ_ANNOUNCE_CHANNEL_ID"))
SOROKIN_USER_ID = int(os.getenv("SOROKIN_USER_ID"))
ALLOWED_USERS = list(map(int, os.getenv("ALLOWED_USERS").split(",")))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
REPORT_HOUR, REPORT_MINUTE = map(int, os.getenv("REPORT_TIME", "20:00").split(":"))

# Для обратной совместимости
ENGINEERS_CHANNEL_ID = RPZ_DISCUSSION_CHAT_ID

# Параметры уведомлений
UNASSIGNED_REPORT_INTERVAL = int(os.getenv("UNASSIGNED_REPORT_INTERVAL", "30"))
URGENT_UNASSIGNED_DELAY = int(os.getenv("URGENT_UNASSIGNED_DELAY", "5"))
URGENT_UNASSIGNED_INTERVAL = int(os.getenv("URGENT_UNASSIGNED_INTERVAL", "5"))
OVERDUE_HOURS_THRESHOLD = int(os.getenv("OVERDUE_HOURS_THRESHOLD", "24"))
STALE_CHECK_INTERVAL = int(os.getenv("STALE_CHECK_INTERVAL", "30"))

# Настраиваемые лимиты зависших задач (в часах)
STALE_HIGH_PRIORITY_HOURS = float(os.getenv("STALE_HIGH_PRIORITY_HOURS", "1"))
STALE_MEDIUM_PRIORITY_HOURS = float(os.getenv("STALE_MEDIUM_PRIORITY_HOURS", "5"))
STALE_LOW_PRIORITY_HOURS = float(os.getenv("STALE_LOW_PRIORITY_HOURS", "24"))

# Альтернативный формат в минутах
STALE_HIGH_PRIORITY_MINUTES = int(os.getenv("STALE_HIGH_PRIORITY_MINUTES", str(int(STALE_HIGH_PRIORITY_HOURS * 60))))
STALE_MEDIUM_PRIORITY_MINUTES = int(os.getenv("STALE_MEDIUM_PRIORITY_MINUTES", str(int(STALE_MEDIUM_PRIORITY_HOURS * 60))))
STALE_LOW_PRIORITY_MINUTES = int(os.getenv("STALE_LOW_PRIORITY_MINUTES", str(int(STALE_LOW_PRIORITY_HOURS * 60))))

# Конвертация в часы
STALE_HIGH_PRIORITY_LIMIT = STALE_HIGH_PRIORITY_MINUTES / 60
STALE_MEDIUM_PRIORITY_LIMIT = STALE_MEDIUM_PRIORITY_MINUTES / 60
STALE_LOW_PRIORITY_LIMIT = STALE_LOW_PRIORITY_MINUTES / 60

# Валидация минимальных значений
MIN_LIMITS = {"Высокий": 0.1, "Средний": 1, "Низкий": 4}
if STALE_HIGH_PRIORITY_LIMIT < MIN_LIMITS["Высокий"]:
    logger.warning(f"STALE_HIGH_PRIORITY_LIMIT={STALE_HIGH_PRIORITY_LIMIT}ч слишком мал (минимум {MIN_LIMITS['Высокий']}ч). Установлено 1ч.")
    STALE_HIGH_PRIORITY_LIMIT = 1.0
if STALE_MEDIUM_PRIORITY_LIMIT < MIN_LIMITS["Средний"]:
    logger.warning(f"STALE_MEDIUM_PRIORITY_LIMIT={STALE_MEDIUM_PRIORITY_LIMIT}ч слишком мал (минимум {MIN_LIMITS['Средний']}ч). Установлено 5ч.")
    STALE_MEDIUM_PRIORITY_LIMIT = 5.0
if STALE_LOW_PRIORITY_LIMIT < MIN_LIMITS["Низкий"]:
    logger.warning(f"STALE_LOW_PRIORITY_LIMIT={STALE_LOW_PRIORITY_LIMIT}ч слишком мал (минимум {MIN_LIMITS['Низкий']}ч). Установлено 24ч.")
    STALE_LOW_PRIORITY_LIMIT = 24.0

logger.info(
    f"Лимиты зависших задач: Высокий={STALE_HIGH_PRIORITY_LIMIT}ч, "
    f"Средний={STALE_MEDIUM_PRIORITY_LIMIT}ч, Низкий={STALE_LOW_PRIORITY_LIMIT}ч"
)

# ======================
# Подключение к Google Sheets
# ======================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
GC = gspread.authorize(CREDS)
SHEET = GC.open_by_key(GOOGLE_SHEET_ID).sheet1

# ======================
# Глобальные переменные
# ======================
pending_tasks = {}
task_counter = 0
users_mapping = {}
urgent_watchlist = {}
paused_timers = {}
user_settings = {}  # {user_id: {"digest_pm": bool, "digest_type": str}}

# ======================
# Вспомогательные функции
# ======================
def get_moscow_time():
    return datetime.now(timezone('Europe/Moscow'))

def is_weekend() -> bool:
    moscow_now = get_moscow_time()
    return moscow_now.weekday() >= 5

def is_weekday() -> bool:
    return not is_weekend()

def get_morning_start_hour() -> int:
    return 10 if is_weekend() else 8

def is_quiet_hours() -> bool:
    moscow_now = get_moscow_time()
    hour = moscow_now.hour
    if is_weekend():
        return hour >= 21 or hour < 10
    else:
        return hour >= 21 or hour < 8

def is_allowed_user(user_id: int) -> bool:
    return user_id in ALLOWED_USERS

def has_sorokin_tag(user_id: int) -> str:
    return "#От Сорокина" if user_id == SOROKIN_USER_ID else ""

# ======================
# Декораторы для надёжности Sheets
# ======================
def retry_sheet_operation(max_attempts: int = 3):
    """Декоратор для повторных попыток операций с Google Sheets"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except (gspread.exceptions.APIError, ConnectionError, TimeoutError) as e:
                    last_exception = e
                    wait_time = min(2 ** (attempt - 1), 10)
                    logger.warning(f"Попытка {attempt}/{max_attempts} не удалась: {e}. Повтор через {wait_time}с")
                    await asyncio.sleep(wait_time)
            logger.error(f"Все {max_attempts} попыток не удалась для {func.__name__}: {last_exception}")
            raise last_exception
        return wrapper
    return decorator

# ======================
# Безопасные операции с таблицей
# ======================
@retry_sheet_operation(max_attempts=3)
async def safe_get_all_records():
    """Безопасное получение всех записей"""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        return await loop.run_in_executor(pool, SHEET.get_all_records)

@retry_sheet_operation(max_attempts=3)
async def safe_append_row(row: list):
    """Безопасное добавление строки"""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        return await loop.run_in_executor(pool, SHEET.append_row, row)

@retry_sheet_operation(max_attempts=3)
async def safe_update_cell(row: int, col: int, value: str):
    """Безопасное обновление ячейки"""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        return await loop.run_in_executor(pool, SHEET.update_cell, row, col, value)

# ======================
# Кэширование данных таблицы
# ======================
_sheet_cache = {"data": None, "timestamp": 0}
CACHE_TTL = 60  # 60 секунд

async def get_cached_sheet_data():
    """Возвращает кэшированные данные таблицы или обновляет кэш"""
    global _sheet_cache
    now = datetime.now().timestamp()
    
    if _sheet_cache["data"] is None or (now - _sheet_cache["timestamp"]) > CACHE_TTL:
        try:
            _sheet_cache["data"] = SHEET.get_all_records()
            _sheet_cache["timestamp"] = now
            logger.debug("Данные таблицы обновлены в кэше")
        except Exception as e:
            logger.error(f"Ошибка обновления кэша таблицы: {e}")
            if _sheet_cache["data"] is None:
                raise
    
    return _sheet_cache["data"]

# ======================
# Форматирование сообщения задачи
# ======================
def format_task_message(task_data, status_line=""):
    author = task_data["author"]
    topic = task_data["topic"]
    description = task_data["description"] or "—"
    priority = task_data.get("priority", "Средний")
    created = task_data["created_str"]
    assigned = task_data.get("assigned_str", "")
    completed = task_data.get("completed_str", "")
    executor = task_data.get("executor", "")
    closed_by = task_data.get("closed_by", "")
    status = task_data["status"].strip()
    
    # Надёжное определение иконки
    status_lower = status.lower()
    if "не распределено" in status_lower:
        status_with_icon = "🔴 Не распределено"
    elif "в работе" in status_lower:
        status_with_icon = "🟠 В работе"
    elif "выполнено" in status_lower:
        status_with_icon = "🟢 Выполнено"
    elif "операционная задача" in status_lower or "опер. задача" in status_lower:
        status_with_icon = "🔵 Операционная задача"
    else:
        status_with_icon = status
    
    lines = [f"Задача #{task_data['id']}", ""]
    lines.append(f"Автор: {author}")
    lines.append(f"Тема: {topic}")
    lines.append(f"Описание: ")
    lines.append(f"{description}")
    lines.append(f"")
    lines.append(f"Приоритет: {priority}")
    lines.append(f"Статус: {status_with_icon}")
    
    if executor and "не распределено" not in status_lower:
        lines.append(f"Ответственный: {executor}")
    
    if closed_by and "выполнено" in status_lower and closed_by.strip() and closed_by != executor:
        lines.append(f"Фактически выполнил: {closed_by}")
    
    lines.append("______________________")
    lines.append(f"Оформлено: {created}")
    
    if assigned:
        lines.append(f"Взято в работу: {assigned}")
    
    if completed and "выполнено" in status_lower:
        lines.append(f"Выполнено: {completed}")
    
    if status_line:
        lines.append("______________________")
        lines.append(status_line)
    
    return "\n".join(lines)

# ======================
# Извлечение приоритета
# ======================
def extract_priority(text: str) -> str:
    text_lower = text.lower()
    high_priority_triggers = ["#в ", "#с ", "#v ", "#c ", "#высокий", "#high"]
    low_priority_triggers = ["#н ", "#низкий", "#low"]
    
    if any(trigger in text_lower for trigger in high_priority_triggers):
        return "Высокий"
    elif any(trigger in text_lower for trigger in low_priority_triggers):
        return "Низкий"
    else:
        return "Средний"

# ======================
# Загрузка пользователей
# ======================
def load_users_mapping():
    global users_mapping
    try:
        user_sheet = GC.open_by_key(GOOGLE_SHEET_ID).worksheet("Пользователи")
        records = user_sheet.get_all_records()
        users_mapping = {}
        for row in records:
            username = str(row.get("Username", "")).strip()
            name = str(row.get("Имя", "")).strip()
            if username and name and username.startswith("@"):
                users_mapping[username] = name
        logger.info(f"Загружено {len(users_mapping)} пользователей")
    except Exception as e:
        logger.error(f"Не удалось загрузить пользователей: {e}")
        users_mapping = {}

# ======================
# Управление настройками пользователей
# ======================
def load_user_settings():
    """Загружает настройки пользователей из листа 'Настройки'"""
    global user_settings
    try:
        settings_sheet = GC.open_by_key(GOOGLE_SHEET_ID).worksheet("Настройки")
        records = settings_sheet.get_all_records()
        user_settings = {}
        for row in records:
            try:
                user_id = int(row.get("User ID", 0))
                if user_id:
                    user_settings[user_id] = {
                        "digest_pm": str(row.get("Дайджест в ЛС", "нет")).lower() == "да",
                        "digest_type": str(row.get("Тип дайджеста", "краткий")).lower()
                    }
            except (ValueError, TypeError):
                continue
        logger.info(f"Загружено настроек пользователей: {len(user_settings)}")
    except Exception as e:
        logger.warning(f"Не удалось загрузить настройки пользователей: {e}. Создаём лист 'Настройки'")
        try:
            settings_sheet = GC.open_by_key(GOOGLE_SHEET_ID).add_worksheet(
                title="Настройки", rows=100, cols=5
            )
            settings_sheet.append_row(["User ID", "Username", "Дайджест в ЛС", "Тип дайджеста", "Обновлено"])
            user_settings = {}
        except Exception as ex:
            logger.error(f"Ошибка создания листа 'Настройки': {ex}")
            user_settings = {}

def save_user_setting(user_id: int, username: str, setting_key: str, value: str):
    """Сохраняет настройку пользователя в Google Sheets"""
    try:
        settings_sheet = GC.open_by_key(GOOGLE_SHEET_ID).worksheet("Настройки")
        user_ids = settings_sheet.col_values(1)
        row_idx = None
        for i, uid in enumerate(user_ids[1:], start=2):
            if uid.strip() == str(user_id):
                row_idx = i
                break
        
        now_str = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
        if row_idx:
            col_map = {"digest_pm": 3, "digest_type": 4}
            if setting_key in col_map:
                settings_sheet.update_cell(row_idx, col_map[setting_key], value)
                settings_sheet.update_cell(row_idx, 5, now_str)
        else:
            new_row = [
                str(user_id),
                username,
                "да" if setting_key == "digest_pm" and value == "да" else "нет",
                value if setting_key == "digest_type" else "краткий",
                now_str
            ]
            settings_sheet.append_row(new_row)
        
        if user_id not in user_settings:
            user_settings[user_id] = {"digest_pm": False, "digest_type": "краткий"}
        if setting_key == "digest_pm":
            user_settings[user_id]["digest_pm"] = (value == "да")
        elif setting_key == "digest_type":
            user_settings[user_id]["digest_type"] = value
        
        logger.info(f"Настройка {setting_key} для {user_id} сохранена: {value}")
    except Exception as e:
        logger.error(f"Ошибка сохранения настройки {setting_key} для {user_id}: {e}")

# ======================
# Логирование метрик в лист "Аналитика"
# ======================
def log_digest_metrics(digest_type: str, metrics: dict):
    """
    Записывает метрики дайджеста в отдельный лист "Аналитика"
    """
    try:
        try:
            analytics_sheet = GC.open_by_key(GOOGLE_SHEET_ID).worksheet("Аналитика")
        except gspread.exceptions.WorksheetNotFound:
            analytics_sheet = GC.open_by_key(GOOGLE_SHEET_ID).add_worksheet(
                title="Аналитика", rows=1000, cols=15
            )
            headers = [
                "Дата записи", "Тип дайджеста", "Период", "Всего задач", 
                "Высокий приоритет", "Средний приоритет", "Низкий приоритет",
                "Операционные", "Просроченные", "Зависшие", "Срочные нераспределённые",
                "Выполнено в срок (%)", "Примечание"
            ]
            analytics_sheet.append_row(headers)
            logger.info("Создан лист 'Аналитика' для метрик дайджестов")
        
        now = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
        row = [
            now,
            digest_type,
            metrics.get("period", ""),
            metrics.get("total", 0),
            metrics.get("high", 0),
            metrics.get("medium", 0),
            metrics.get("low", 0),
            metrics.get("operational", 0),
            metrics.get("overdue", 0),
            metrics.get("stale", 0),
            metrics.get("urgent_unassigned", 0),
            metrics.get("on_time_percent", 0),
            metrics.get("note", "")
        ]
        analytics_sheet.append_row(row)
        logger.info(f"Записаны метрики {digest_type} дайджеста в лист 'Аналитика'")
    except Exception as e:
        logger.error(f"Ошибка записи метрик дайджеста в 'Аналитика': {e}")

# ======================
# Визуализация для еженедельного дайджеста
# ======================
def generate_weekly_charts(tasks_df: pd.DataFrame, week_start, week_end) -> Tuple[io.BytesIO, io.BytesIO]:
    """Генерация графика активности и heatmap"""
    # 1. График выполнения по дням
    fig1, ax1 = plt.subplots(figsize=(10, 5))
    
    tasks_df['created_date'] = pd.to_datetime(tasks_df['Дата создания'], errors='coerce').dt.date
    
    daily_stats = tasks_df.groupby('created_date')['Статус'].value_counts().unstack(fill_value=0)
    
    for status in ['Выполнено', 'В работе', 'Не распределено', 'Операционная задача']:
        if status not in daily_stats.columns:
            daily_stats[status] = 0
    
    x = np.arange(len(daily_stats))
    width = 0.2
    
    ax1.bar(x - 1.5*width, daily_stats.get('Выполнено', 0), width, label='Выполнено', color='#4CAF50')
    ax1.bar(x - 0.5*width, daily_stats.get('В работе', 0), width, label='В работе', color='#FF9800')
    ax1.bar(x + 0.5*width, daily_stats.get('Не распределено', 0), width, label='Не распределено', color='#F44336')
    ax1.bar(x + 1.5*width, daily_stats.get('Операционная задача', 0), width, label='Операционные', color='#2196F3')
    
    ax1.set_xticks(x)
    ax1.set_xticklabels([d.strftime('%a %d') for d in daily_stats.index], rotation=45)
    ax1.set_ylabel('Количество задач')
    ax1.set_title(f'Активность задач: {week_start.strftime("%d.%m")} - {week_end.strftime("%d.%m")}')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    chart_buf = io.BytesIO()
    plt.savefig(chart_buf, format='png', dpi=150)
    plt.close(fig1)
    chart_buf.seek(0)
    
    # 2. Heatmap активности
    fig2, ax2 = plt.subplots(figsize=(12, 6))
    
    tasks_df['created_time'] = pd.to_datetime(tasks_df['Время создания'], format='%H:%M:%S', errors='coerce').dt.hour
    tasks_df['created_date'] = pd.to_datetime(tasks_df['Дата создания'], errors='coerce')
    tasks_df['weekday'] = tasks_df['created_date'].dt.dayofweek
    
    heatmap_data = np.zeros((7, 24))
    for _, row in tasks_df.iterrows():
        if pd.notnull(row['weekday']) and pd.notnull(row['created_time']):
            day = int(row['weekday'])
            hour = int(row['created_time'])
            if 0 <= day < 7 and 0 <= hour < 24:
                heatmap_data[day, hour] += 1
    
    days = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
    hours = [f'{h:02d}:00' for h in range(24)]
    
    sns.heatmap(heatmap_data, ax=ax2, cmap='YlOrRd', cbar_kws={'label': 'Количество задач'},
                xticklabels=hours, yticklabels=days, linewidths=0.5)
    ax2.set_title('Heatmap: Активность создания задач по дням и часам')
    ax2.set_xlabel('Время')
    ax2.set_ylabel('День недели')
    plt.setp(ax2.get_xticklabels(), rotation=90)
    plt.tight_layout()
    
    heatmap_buf = io.BytesIO()
    plt.savefig(heatmap_buf, format='png', dpi=150)
    plt.close(fig2)
    heatmap_buf.seek(0)
    
    return chart_buf, heatmap_buf

# ======================
# Инициализация счётчика задач
# ======================
def initialize_task_counter():
    global task_counter
    try:
        ids = SHEET.col_values(1)
        if not ids:
            task_counter = 0
            return
        if len(ids) > 0 and ids[0].strip().upper() in ("ID", ""):
            task_ids = ids[1:]
        else:
            task_ids = ids
        numbers = []
        for tid in task_ids:
            if tid and isinstance(tid, str) and tid.startswith("TASK-"):
                try:
                    num = int(tid.split("-")[1])
                    numbers.append(num)
                except (ValueError, IndexError):
                    continue
        task_counter = max(numbers) if numbers else 0
        logger.info(f"Счётчик задач инициализирован: следующий ID = TASK-{task_counter + 1:04d}")
    except Exception as e:
        logger.warning(f"Ошибка инициализации счётчика: {e}")
        task_counter = 0

def generate_task_id():
    global task_counter
    task_counter += 1
    return f"TASK-{task_counter:04d}"

# ======================
# Извлечение темы и описания
# ======================
def extract_topic_and_desc(text: str):
    if not (text.startswith("#З ") or text == "#З"):
        return None, None
    if text == "#З":
        return "", ""
    content = text[3:].lstrip()
    if "\n" in content:
        topic, desc = content.split("\n", 1)
    else:
        topic, desc = content, ""
    return topic.strip(), desc.strip()

# ======================
# Команды бота
# ======================

async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    if user:
        await update.message.reply_text(
            f"✅ Бот работает!\n"
            f"Chat ID: {chat_id}\n"
            f"User ID: {user.id}\n"
            f"Username: @{user.username}"
        )
    else:
        await update.message.reply_text("⚠️ Анонимное сообщение.")

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    today = datetime.now(timezone('Europe/Moscow')).strftime("%Y-%m-%d")
    try:
        all_records = await get_cached_sheet_data()
        today_tasks = [r for r in all_records if r.get("Дата создания") == today]
        
        pending = []
        in_progress = {}
        completed = {}
        operational = {}
        
        for task in today_tasks:
            task_id = task.get("ID", "—")
            topic = task.get("Тема задачи", "—")
            executor = task.get("Исполнитель", "")
            status = task.get("Статус", "—")
            line = f"• {task_id} {topic}"
            
            status_lower = status.lower()
            
            if status == "Не распределено":
                pending.append(line)
            elif status == "В работе":
                if executor not in in_progress:
                    in_progress[executor] = []
                in_progress[executor].append(line)
            elif status == "Выполнено":
                if executor not in completed:
                    completed[executor] = []
                completed[executor].append(line)
            elif "операционная задача" in status_lower or "опер. задача" in status_lower:
                if executor not in operational:
                    operational[executor] = []
                operational[executor].append(line)
        
        overdue = []
        for task in all_records:
            if (task.get("Статус") == "Не распределено" and
                task.get("Дата создания") != today):
                task_id = task.get("ID", "—")
                topic = task.get("Тема задачи", "—")
                overdue.append(f"• {task_id} {topic}")
        
        lines = [f"📅 Дата: {today}", ""]
        
        if pending:
            lines.extend(["⏳ Не распределено:"] + pending + [""])
        
        if in_progress:
            lines.append("🔄 В работе:")
            for executor, tasks in in_progress.items():
                display_executor = executor if executor.startswith("@") else f"@{executor}"
                lines.append(display_executor)
                lines.extend(tasks)
            lines.append("")
        
        if operational:
            lines.append("🔵 Операционные задачи:")
            for executor, tasks in operational.items():
                display_executor = executor if executor.startswith("@") else f"@{executor}"
                lines.append(display_executor)
                lines.extend(tasks)
            lines.append("")
        
        if completed:
            lines.append("✅ Выполнено:")
            for executor, tasks in completed.items():
                display_executor = executor if executor.startswith("@") else f"@{executor}"
                lines.append(display_executor)
                lines.extend(tasks)
            lines.append("")
        
        if overdue:
            lines.extend(["⚠️ Просроченные (нераспределённые за прошлые дни):"] + overdue)
        
        message = "\n".join(lines) if len(lines) > 2 else "📅 Нет задач за сегодня."
    except Exception as e:
        logger.error(f"Ошибка при получении задач за сегодня: {e}")
        message = "❌ Не удалось загрузить задачи за сегодня."
    
    try:
        await context.bot.send_message(chat_id=user.id, text=message)
        await update.message.reply_text("✅ Отчёт отправлен в личные сообщения.")
    except Exception as e:
        logger.error(f"Не удалось отправить в личку: {e}")
        await update.message.reply_text("⚠️ Напишите боту в личку /start.")

async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        all_records = await get_cached_sheet_data()
        pending_tasks = [
            r for r in all_records
            if r.get("Статус") == "Не распределено"
        ]
        if pending_tasks:
            lines = ["⏳ Нераспределённые задачи:", ""]
            for i, task in enumerate(pending_tasks, 1):
                task_id = task.get("ID", "—")
                topic = task.get("Тема задачи", "—")
                lines.append(f"{i}. {task_id} {topic}")
            message = "\n".join(lines)
        else:
            message = "⏳ Нет нераспределённых задач."
    except Exception as e:
        logger.error(f"Ошибка при получении нераспределённых задач: {e}")
        message = "❌ Не удалось загрузить нераспределённые задачи."
    
    try:
        await context.bot.send_message(chat_id=user.id, text=message)
        await update.message.reply_text("✅ Список нераспределённых отправлен в личку.")
    except Exception as e:
        logger.error(f"Не удалось отправить в личку: {e}")
        await update.message.reply_text("⚠️ Напишите боту в личку /start.")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        all_records = await get_cached_sheet_data()
        total = len(all_records)
        completed = sum(1 for r in all_records if r.get("Статус") == "Выполнено")
        pending = sum(1 for r in all_records if r.get("Статус") == "Не распределено")
        operational = sum(1 for r in all_records if "операционная задача" in str(r.get("Статус", "")).lower() or "опер. задача" in str(r.get("Статус", "")).lower())
        in_progress = total - completed - pending - operational
        
        today = datetime.now(timezone('Europe/Moscow')).strftime("%Y-%m-%d")
        created_today = sum(1 for r in all_records if r.get("Дата создания") == today)
        
        message = (
            "📊 **Статистика по задачам**\n"
            f"Всего задач: {total}\n"
            f"✅ Выполнено: {completed}\n"
            f"🔄 В работе: {in_progress}\n"
            f"🔵 Операционные: {operational}\n"
            f"⏳ Не распределено: {pending}\n"
            f"📅 Создано сегодня: {created_today}"
        )
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        message = "❌ Не удалось загрузить статистику."
    
    try:
        await context.bot.send_message(chat_id=user.id, text=message, parse_mode="Markdown")
        await update.message.reply_text("✅ Статистика отправлена в личные сообщения.")
    except Exception as e:
        logger.error(f"Не удалось отправить в личку: {e}")
        await update.message.reply_text("⚠️ Напишите боту в личку /start.")

async def cmd_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not context.args:
        await update.message.reply_text(
            "ℹ️ Использование: `/task TASK-1234` или `/task 1234`",
            parse_mode="Markdown"
        )
        return
    
    raw_arg = context.args[0].strip().upper()
    if raw_arg.startswith("TASK-"):
        task_id = raw_arg
    else:
        if not raw_arg.isdigit():
            await update.message.reply_text(
                "❌ Неверный формат ID. Используйте: `TASK-1234` или `1234`",
                parse_mode="Markdown"
            )
            return
        task_id = f"TASK-{raw_arg.zfill(4)}"
    
    try:
        all_ids = SHEET.col_values(1)
        row_idx = None
        for i, tid in enumerate(all_ids):
            if tid and tid.strip().upper() == task_id:
                row_idx = i + 1
                break
        
        if not row_idx:
            await update.message.reply_text(f"❌ Задача `{task_id}` не найдена.", parse_mode="Markdown")
            return
        
        row = SHEET.row_values(row_idx)
        while len(row) < 15:
            row.append("")
        
        task_data = {
            "id": row[0] or "—",
            "created_date": row[1] or "",
            "created_time": row[2] or "",
            "topic": row[3] or "—",
            "description": row[4] or "—",
            "author": row[5] or "—",
            "executor": row[6] or "",
            "status": row[7] or "—",
            "assigned": row[8] or "",
            "completed": row[9] or "",
            "tags": row[10] or "",
            "msg_id": row[11] or "",
            "thread_id": row[12] or "",
            "priority": row[13] or "Средний",
            "closed_by": row[14] or ""
        }
        
        created_str = f"{task_data['created_date']} {task_data['created_time']}".strip()
        status = task_data["status"].strip()
        status_lower = status.lower()
        
        if "не распределено" in status_lower:
            status_with_icon = "🔴 Не распределено"
        elif "в работе" in status_lower:
            status_with_icon = "🟠 В работе"
        elif "выполнено" in status_lower:
            status_with_icon = "🟢 Выполнено"
        elif "операционная задача" in status_lower or "опер. задача" in status_lower:
            status_with_icon = "🔵 Операционная задача"
        else:
            status_with_icon = status
        
        lines = [
            f"🔍 Задача #{task_data['id']}",
            "",
            f"Автор: {task_data['author']}",
            f"Тема: {task_data['topic']}",
            f"Описание: {task_data['description']}",
            "",
            f"Приоритет: {task_data['priority']}",
            f"Статус: {status_with_icon}"
        ]
        
        if task_data["executor"] and "не распределено" not in status_lower:
            lines.append(f"Ответственный: {task_data['executor']}")
        
        if task_data["closed_by"] and "выполнено" in status_lower and task_data["closed_by"].strip():
            if task_data["closed_by"] != task_data["executor"]:
                lines.append(f"Фактически выполнил: {task_data['closed_by']}")
        
        if task_data["tags"]:
            lines.append(f"Теги: {task_data['tags']}")
        
        lines.append("______________________")
        lines.append(f"Оформлено: {created_str}")
        
        if task_data["assigned"] and "не распределено" not in status_lower:
            lines.append(f"Взято в работу: {task_data['assigned']}")
        
        if task_data["completed"] and "выполнено" in status_lower:
            lines.append(f"Выполнено: {task_data['completed']}")
        
        message = "\n".join(lines)
        
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=message,
                parse_mode=None
            )
            await update.message.reply_text("✅ Информация о задаче отправлена в личные сообщения.")
        except Exception as e:
            logger.error(f"Не удалось отправить в личку: {e}")
            await update.message.reply_text(
                "⚠️ Не удалось отправить сообщение. Напишите боту в личку `/start` и повторите запрос."
            )
    except Exception as e:
        logger.error(f"Ошибка при поиске задачи {task_id}: {e}")
        await update.message.reply_text("❌ Произошла ошибка при поиске задачи. Обратитесь к администратору.")

async def cmd_limits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = (
        "⏱️ Текущие лимиты зависших задач:\n"
        f"🔴 Высокий приоритет: {STALE_HIGH_PRIORITY_LIMIT}ч ({int(STALE_HIGH_PRIORITY_LIMIT * 60)} мин)\n"
        f"🟠 Средний приоритет: {STALE_MEDIUM_PRIORITY_LIMIT}ч ({int(STALE_MEDIUM_PRIORITY_LIMIT * 60)} мин)\n"
        f"🟡 Низкий приоритет: {STALE_LOW_PRIORITY_LIMIT}ч ({int(STALE_LOW_PRIORITY_LIMIT * 60)} мин)\n"
        "Измените в .env и перезапустите бота."
    )
    await update.message.reply_text(message)

async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /settings - настройка персональной доставки дайджеста"""
    user = update.effective_user
    if not user:
        return
    
    if not user_settings:
        load_user_settings()
    
    user_id = user.id
    settings = user_settings.get(user_id, {"digest_pm": False, "digest_type": "краткий"})
    
    digest_status = "✅ Включена" if settings["digest_pm"] else "❌ Отключена"
    digest_type = "📊 Полная" if settings["digest_type"] == "полная" else "📝 Краткая"
    
    text = (
        "⚙️ *Ваши настройки уведомлений*\n"
        f"📬 Дайджест в личные сообщения: {digest_status}\n"
        f"📋 Тип дайджеста: {digest_type}\n"
        "Выберите действие:"
    )
    
    keyboard = [
        [
            InlineKeyboardButton(
                "📬 Переключить доставку в ЛС", 
                callback_data=f"toggle_digest_pm_{user_id}"
            )
        ],
        [
            InlineKeyboardButton(
                "📋 Сменить тип дайджеста", 
                callback_data=f"toggle_digest_type_{user_id}"
            )
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=reply_markup)

async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатия кнопок настроек"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    if not user:
        return
    
    data = query.data
    user_id = user.id
    
    if data.startswith("toggle_digest_pm_"):
        current = user_settings.get(user_id, {}).get("digest_pm", False)
        new_value = "нет" if current else "да"
        save_user_setting(user_id, f"@{user.username}" if user.username else user.full_name, "digest_pm", new_value)
        
        status = "✅ Включена" if new_value == "да" else "❌ Отключена"
        await query.edit_message_text(
            f"⚙️ Настройки обновлены!\n📬 Дайджест в личные сообщения: {status}\n"
            "Изменения вступят в силу с следующего дайджеста."
        )
    
    elif data.startswith("toggle_digest_type_"):
        current_type = user_settings.get(user_id, {}).get("digest_type", "краткий")
        new_type = "полная" if current_type == "краткий" else "краткий"
        save_user_setting(user_id, f"@{user.username}" if user.username else user.full_name, "digest_type", new_type)
        
        display_type = "📊 Полная" if new_type == "полная" else "📝 Краткая"
        await query.edit_message_text(
            f"⚙️ Настройки обновлены!\n📋 Тип дайджеста: {display_type}\n"
            "Полная версия включает детальную статистику и рекомендации."
        )

async def cmd_operational(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /oper - перевод задачи в статус операционной"""
    user = update.effective_user
    if not context.args:
        await update.message.reply_text(
            "ℹ️ Использование: `/oper TASK-1234` или `/oper 1234`\n\n"
            "Переводит задачу в статус «Операционная задача»",
            parse_mode="Markdown"
        )
        return
    
    raw_arg = context.args[0].strip().upper()
    if raw_arg.startswith("TASK-"):
        task_id = raw_arg
    else:
        if not raw_arg.isdigit():
            await update.message.reply_text("❌ Неверный формат ID. Используйте: `TASK-1234` или `1234`", parse_mode="Markdown")
            return
        task_id = f"TASK-{raw_arg.zfill(4)}"
    
    try:
        all_ids = SHEET.col_values(1)
        row_idx = None
        for i, tid in enumerate(all_ids):
            if tid and tid.strip().upper() == task_id:
                row_idx = i + 1
                break
        
        if not row_idx:
            await update.message.reply_text(f"❌ Задача `{task_id}` не найдена.", parse_mode="Markdown")
            return
        
        row = SHEET.row_values(row_idx)
        while len(row) < 15:
            row.append("")
        
        current_status = row[7].strip() if len(row) > 7 else "—"
        
        if "выполнено" in current_status.lower():
            await update.message.reply_text(
                f"❌ Задача `{task_id}` уже выполнена. Нельзя изменить статус.",
                parse_mode="Markdown"
            )
            return
        
        now = get_moscow_time()
        operational_status = "Операционная задача"
        SHEET.update_cell(row_idx, 8, operational_status)
        
        if "не распределено" in current_status.lower():
            assign_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
            SHEET.update_cell(row_idx, 9, assign_time_str)
            logger.info(f"Задача {task_id} переведена в операционную и автоматически назначена")
        
        author = row[5] or "—"
        topic = row[3] or "—"
        description = row[4] or "—"
        priority = row[13] or "Средний"
        executor = row[6] or ""
        created_date = row[1] or ""
        created_time = row[2] or ""
        created = f"{created_date} {created_time}".strip()
        assigned = row[8] or ""
        
        new_text = format_task_message({
            "id": task_id,
            "author": author,
            "topic": topic,
            "description": description,
            "priority": priority,
            "status": operational_status,
            "created_str": created,
            "assigned_str": assigned if assigned else now.strftime("%Y-%m-%d %H:%M:%S"),
            "executor": executor or "—"
        }, "🔵 Операционная задача")
        
        try:
            msg_id_str = row[11] if len(row) > 11 else ""
            if msg_id_str and str(msg_id_str).isdigit():
                msg_id = int(msg_id_str)
                await context.bot.edit_message_text(
                    chat_id=RPZ_ANNOUNCE_CHANNEL_ID,
                    message_id=msg_id,
                    text=new_text
                )
                logger.info(f"Сообщение в канале обновлено для операционной задачи: {task_id}")
        except Exception as e:
            logger.error(f"Ошибка редактирования сообщения в канале: {e}")
        
        await context.bot.send_message(
            chat_id=RPZ_DISCUSSION_CHAT_ID,
            text=f"✅ Задача №{task_id} переведена в статус «Операционная задача»"
        )
        
        await update.message.reply_text(f"✅ Задача `{task_id}` переведена в статус «Операционная задача»", parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Ошибка при переводе задачи {task_id} в операционную: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Обратитесь к администратору.")

# ======================
# Обработка задач
# ======================

async def handle_new_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return
    if user.id in (777000, 1087968824):
        logger.debug("Игнорируем анонимное сообщение")
        return
    if not is_allowed_user(user.id):
        logger.debug(f"Пользователь {user.id} не в списке разрешённых")
        return
    chat_id = message.chat_id
    if chat_id != RPZ_DISCUSSION_CHAT_ID:
        return
    text = (message.text or message.caption or "").strip()
    if not text:
        return
    key = (user.id, chat_id, None)
    if key in pending_tasks:
        pending_tasks[key]["desc_parts"].append(text)
        pending_tasks[key]["msg_ids"].append(message.message_id)
        return
    if not (text.startswith("#З ") or text == "#З"):
        return
    logger.info(f"Получено сообщение в чате обсуждений {chat_id} от {user.id} (@{user.username}): {text[:50]}...")
    topic, desc_part = extract_topic_and_desc(text)
    if topic is None and text != "#З":
        return
    now = get_moscow_time()
    pending_tasks[key] = {
        "user": user,
        "topic": topic,
        "desc_parts": [desc_part] if desc_part else [],
        "msg_ids": [message.message_id],
        "start_time": now,
    }
    context.job_queue.run_once(
        finalize_task_job,
        20,
        data={"key": key},
        name=f"task_timer_{hash(key)}"
    )

async def finalize_task_job(context: CallbackContext):
    job = context.job
    key = job.data["key"]
    if key not in pending_tasks:
        return
    data = pending_tasks.pop(key)
    user = data["user"]
    topic = data["topic"]
    description = "\n".join(data["desc_parts"]).strip()
    msg_ids = data["msg_ids"]
    now = get_moscow_time()
    task_id = generate_task_id()
    raw_username = f"@{user.username}" if user.username else None
    display_name = users_mapping.get(raw_username, raw_username or user.full_name)
    author = display_name
    tags = has_sorokin_tag(user.id)
    full_text = topic + "\n" + description
    priority = extract_priority(full_text)
    logger.info(f"Определён приоритет '{priority}' для задачи {task_id} по тексту: {full_text[:100]}")
    
    for msg_id in msg_ids:
        try:
            await context.bot.delete_message(chat_id=RPZ_DISCUSSION_CHAT_ID, message_id=msg_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить {msg_id}: {e}")
    
    try:
        channel_msg = await context.bot.send_message(
            chat_id=RPZ_ANNOUNCE_CHANNEL_ID,
            text=format_task_message({
                "id": task_id,
                "author": author,
                "topic": topic,
                "description": description,
                "priority": priority,
                "status": "Не распределено",
                "created_str": now.strftime("%Y-%m-%d %H:%M:%S")
            })
        )
        logger.info(f"Задача опубликована в канале: {task_id}")
    except Exception as e:
        logger.error(f"Не удалось опубликовать в канале: {e}")
        return
    
    try:
        row = [
            task_id,
            now.strftime("%Y-%m-%d"),
            now.strftime("%H:%M:%S"),
            topic,
            description,
            author,
            "",
            "Не распределено",
            "",
            "",
            tags,
            str(channel_msg.message_id),
            "",
            priority,
            ""
        ]
        await safe_append_row(row)
        logger.info(f"Задача сохранена в таблицу: {task_id} (приоритет: {priority})")
    except Exception as e:
        logger.error(f"Ошибка сохранения в таблицу: {e}")
        return
    
    if priority == "Высокий":
        job_name = f"urgent_watch_{task_id}"
        urgent_watchlist[task_id] = {
            "created_at": now,
            "job_name": job_name,
            "topic": topic,
            "notified_count": 0
        }
        context.job_queue.run_once(
            check_urgent_unassigned,
            URGENT_UNASSIGNED_DELAY * 60,
            data={
                "task_id": task_id,
                "topic": topic,
                "created_at": now.isoformat()
            },
            name=job_name
        )
        logger.info(f"Запущен таймер срочной задачи {task_id} (первое уведомление через {URGENT_UNASSIGNED_DELAY} мин)")

# ======================
# Обработка команд через ответ
# ======================

async def handle_task_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return
    if user.id in (777000, 1087968824):
        return
    if message.chat_id != RPZ_DISCUSSION_CHAT_ID:
        return
    text = (message.text or "").strip()
    if not text:
        return
    logger.info(f"Обработка команды: {text}")
    
    task_id = None
    if message.reply_to_message:
        quoted_text = ""
        if message.reply_to_message.text:
            quoted_text = message.reply_to_message.text
        elif message.reply_to_message.caption:
            quoted_text = message.reply_to_message.caption
        task_match = re.search(r'TASK-(\d{4})', quoted_text)
        if task_match:
            task_id = f"TASK-{task_match.group(1)}"
        logger.info(f"Найден ID задачи из цитаты: {task_id}")
    
    if not task_id:
        logger.warning("TASK-XXXX не найден в цитате")
        return
    
    try:
        all_ids = SHEET.col_values(1)
        cell = None
        for i, tid in enumerate(all_ids):
            if tid and tid.strip() == task_id:
                cell = type('Cell', (), {'row': i + 1})()
                break
        if not cell:
            logger.error(f"Задача '{task_id}' не найдена в таблице!")
            return
        logger.info(f"Задача найдена в строке {cell.row}")
    except Exception as e:
        logger.error(f"Ошибка при поиске задачи: {e}")
        return
    
    row_idx = cell.row
    try:
        current_status = SHEET.cell(row_idx, 8).value
        if current_status == "Выполнено":
            logger.info("Задача уже выполнена")
            return
    except Exception as e:
        logger.error(f"Ошибка чтения статуса: {e}")
        return
    
    processed = False
    action_msg_id = message.message_id
    
    username_match = re.search(r'@[\w\d_]+', text)
    if username_match:
        executor_handle = username_match.group(0)
        executor_name = users_mapping.get(executor_handle, executor_handle)
        display_name_with_username = f"{executor_name} ({executor_handle})"
        now = get_moscow_time()
        try:
            row = SHEET.row_values(row_idx)
            while len(row) < 15:
                row.append("")
            
            author = row[5] or "—"
            topic = row[3] or "—"
            description = row[4] or "—"
            priority = row[13] or "Средний"
            created_date = row[1] or ""
            created_time = row[2] or ""
            created = f"{created_date} {created_time}".strip()
            
            SHEET.update_cell(row_idx, 7, executor_name)
            SHEET.update_cell(row_idx, 8, "В работе")
            assign_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
            SHEET.update_cell(row_idx, 9, assign_time_str)
            
            if task_id in urgent_watchlist:
                job_name = urgent_watchlist[task_id]["job_name"]
                current_jobs = context.job_queue.get_jobs_by_name(job_name)
                for job in current_jobs:
                    job.schedule_removal()
                del urgent_watchlist[task_id]
                logger.info(f"Таймер срочной задачи {task_id} отменён после назначения")
            
            new_text = format_task_message({
                "id": task_id,
                "author": author,
                "topic": topic,
                "description": description,
                "priority": priority,
                "status": "В работе",
                "created_str": created,
                "assigned_str": assign_time_str,
                "executor": executor_name
            }, f"В работе у {executor_name}")
            
            try:
                msg_id_str = row[11] if len(row) > 11 else ""
                if msg_id_str and str(msg_id_str).isdigit():
                    msg_id = int(msg_id_str)
                    await context.bot.edit_message_text(
                        chat_id=RPZ_ANNOUNCE_CHANNEL_ID,
                        message_id=msg_id,
                        text=new_text
                    )
                    logger.info(f"Сообщение в канале обновлено: {msg_id}")
                else:
                    raise ValueError("Msg_ID не является числом")
            except Exception as e:
                logger.error(f"Ошибка редактирования: {e}. Пересоздание...")
                new_msg = await context.bot.send_message(
                    chat_id=RPZ_ANNOUNCE_CHANNEL_ID,
                    text=new_text
                )
                SHEET.update_cell(row_idx, 12, str(new_msg.message_id))
                logger.info(f"Задача пересоздана. Новое Msg_ID: {new_msg.message_id}")
            
            await context.bot.send_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                text=f"Задача №{task_id} → назначена на {display_name_with_username}"
            )
            logger.info(f"Задача №{task_id} назначена на {display_name_with_username}")
            processed = True
        except Exception as e:
            logger.error(f"Ошибка при назначении: {e}")
    
    elif any(trigger in text.lower() for trigger in ["#выполнено", "готово", "решено"]):
        now = get_moscow_time()
        try:
            assigned_executor = SHEET.cell(row_idx, 7).value or ""
            raw_closer_username = f"@{user.username}" if user.username else None
            closer_display_name = users_mapping.get(raw_closer_username, raw_closer_username or user.full_name)
            complete_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
            
            SHEET.update_cell(row_idx, 8, "Выполнено")
            SHEET.update_cell(row_idx, 10, complete_time_str)
            SHEET.update_cell(row_idx, 15, closer_display_name)
            
            if task_id in urgent_watchlist:
                job_name = urgent_watchlist[task_id]["job_name"]
                current_jobs = context.job_queue.get_jobs_by_name(job_name)
                for job in current_jobs:
                    job.schedule_removal()
                del urgent_watchlist[task_id]
                logger.info(f"Таймер срочной задачи {task_id} отменён после закрытия")
            
            author = SHEET.cell(row_idx, 6).value or "—"
            topic = SHEET.cell(row_idx, 4).value or "—"
            description = SHEET.cell(row_idx, 5).value or "—"
            priority = SHEET.cell(row_idx, 14).value or "Средний"
            created_date = SHEET.cell(row_idx, 2).value or ""
            created_time = SHEET.cell(row_idx, 3).value or ""
            created = f"{created_date} {created_time}".strip()
            assigned = SHEET.cell(row_idx, 9).value or ""
            
            status_line_parts = []
            if assigned_executor and assigned_executor.strip() and assigned_executor != closer_display_name:
                status_line_parts.append(f"назначено на {assigned_executor}")
            status_line_parts.append(f"выполнил {closer_display_name}")
            status_line = " | ".join(status_line_parts)
            
            new_text = format_task_message({
                "id": task_id,
                "author": author,
                "topic": topic,
                "description": description,
                "priority": priority,
                "status": "Выполнено",
                "created_str": created,
                "assigned_str": assigned,
                "completed_str": complete_time_str,
                "executor": assigned_executor,
                "closed_by": closer_display_name
            }, f"🟢 Выполнено ({status_line})")
            
            try:
                msg_id_str = SHEET.cell(row_idx, 12).value
                if msg_id_str and str(msg_id_str).isdigit():
                    msg_id = int(msg_id_str)
                    await context.bot.edit_message_text(
                        chat_id=RPZ_ANNOUNCE_CHANNEL_ID,
                        message_id=msg_id,
                        text=new_text
                    )
                else:
                    raise ValueError("Msg_ID не является числом")
            except Exception as e:
                logger.error(f"Ошибка редактирования при закрытии: {e}. Пересоздание...")
                new_msg = await context.bot.send_message(
                    chat_id=RPZ_ANNOUNCE_CHANNEL_ID,
                    text=new_text
                )
                SHEET.update_cell(row_idx, 12, str(new_msg.message_id))
                logger.info(f"Задача пересоздана после закрытия. Новое Msg_ID: {new_msg.message_id}")
            
            if assigned_executor and assigned_executor.strip() and assigned_executor != closer_display_name:
                notification_text = f"Задача №{task_id} → выполнена {closer_display_name} (ответственный: {assigned_executor})"
            else:
                notification_text = f"Задача №{task_id} → выполнена {closer_display_name}"
            
            await context.bot.send_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                text=notification_text
            )
            logger.info(f"№{task_id} закрыто. Ответственный: {assigned_executor or '—'}, фактически выполнил: {closer_display_name}")
            processed = True
        except Exception as e:
            logger.error(f"Ошибка при закрытии: {e}")
    
    elif any(trigger in text.lower() for trigger in ["#опер", "#операционная", "#опер. задача"]):
        now = get_moscow_time()
        try:
            author = SHEET.cell(row_idx, 6).value or "—"
            topic = SHEET.cell(row_idx, 4).value or "—"
            description = SHEET.cell(row_idx, 5).value or "—"
            priority = SHEET.cell(row_idx, 14).value or "Средний"
            created_date = SHEET.cell(row_idx, 2).value or ""
            created_time = SHEET.cell(row_idx, 3).value or ""
            created = f"{created_date} {created_time}".strip()
            executor = SHEET.cell(row_idx, 7).value or ""
            
            operational_status = "Операционная задача"
            SHEET.update_cell(row_idx, 8, operational_status)
            
            if not executor.strip():
                auto_executor = f"@{user.username}" if user.username else user.full_name
                display_name = users_mapping.get(auto_executor, auto_executor)
                SHEET.update_cell(row_idx, 7, display_name)
                executor = display_name
            
            current_assigned = SHEET.cell(row_idx, 9).value or ""
            if not current_assigned.strip():
                assign_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
                SHEET.update_cell(row_idx, 9, assign_time_str)
            
            new_text = format_task_message({
                "id": task_id,
                "author": author,
                "topic": topic,
                "description": description,
                "priority": priority,
                "status": operational_status,
                "created_str": created,
                "assigned_str": now.strftime("%Y-%m-%d %H:%M:%S") if not current_assigned.strip() else current_assigned,
                "executor": executor
            }, "🔵 Операционная задача")
            
            try:
                msg_id_str = SHEET.cell(row_idx, 12).value
                if msg_id_str and str(msg_id_str).isdigit():
                    msg_id = int(msg_id_str)
                    await context.bot.edit_message_text(
                        chat_id=RPZ_ANNOUNCE_CHANNEL_ID,
                        message_id=msg_id,
                        text=new_text
                    )
            except Exception as e:
                logger.error(f"Ошибка редактирования при переводе в операционную: {e}")
            
            await context.bot.send_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                text=f"✅ Задача №{task_id} переведена в статус «Операционная задача»"
            )
            
            logger.info(f"Задача {task_id} переведена в операционную через тег")
            processed = True
        except Exception as e:
            logger.error(f"Ошибка перевода в операционную задачу: {e}")
    
    if processed:
        try:
            await context.bot.delete_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                message_id=action_msg_id
            )
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение действия: {e}")

# ======================
# Уведомления и проверки
# ======================

async def check_urgent_unassigned(context: CallbackContext):
    if is_quiet_hours():
        logger.debug("Режим тишины — проверка срочной задачи пропущена")
        return
    if is_weekend():
        logger.debug("Выходной день — циклические уведомления отключены")
        return
    
    task_id = context.job.data["task_id"]
    topic = context.job.data["topic"]
    created_at = datetime.fromisoformat(context.job.data["created_at"])
    
    if task_id not in urgent_watchlist:
        logger.debug(f"Задача {task_id} уже назначена (удалена из ватчлиста)")
        return
    
    try:
        all_ids = SHEET.col_values(1)
        row_idx = None
        for i, tid in enumerate(all_ids):
            if tid and tid.strip() == task_id:
                row_idx = i + 1
                break
        
        if not row_idx:
            logger.warning(f"Задача {task_id} не найдена в таблице")
            urgent_watchlist.pop(task_id, None)
            return
        
        status = (SHEET.cell(row_idx, 8).value or "").strip()
        executor = (SHEET.cell(row_idx, 7).value or "").strip()
        priority = (SHEET.cell(row_idx, 14).value or "").strip()
        
        if priority != "Высокий" or status != "Не распределено" or executor.strip():
            urgent_watchlist.pop(task_id, None)
            logger.info(f"Задача {task_id} больше не требует срочных уведомлений")
            return
        
        moscow_now = get_moscow_time()
        elapsed_minutes = int((moscow_now - created_at).total_seconds() / 60)
        urgent_watchlist[task_id]["notified_count"] += 1
        notify_count = urgent_watchlist[task_id]["notified_count"]
        
        alert_msg = (
            f"🚨 СРОЧНО! Нераспределённая задача с ВЫСОКИМ приоритетом\n"
            f"Задача: {task_id}\n"
            f"Тема: {topic}\n"
            f"Время ожидания: {elapsed_minutes} мин\n"
            f"Уведомление: #{notify_count}\n"
            f"❗ Требуется немедленное назначение исполнителя!"
        )
        
        await context.bot.send_message(
            chat_id=RPZ_DISCUSSION_CHAT_ID,
            text=alert_msg
        )
        logger.warning(
            f"Срочное уведомление #{notify_count} для {task_id} отправлено "
            f"(ожидание: {elapsed_minutes} мин)"
        )
        
        context.job_queue.run_once(
            check_urgent_unassigned,
            URGENT_UNASSIGNED_INTERVAL * 60,
            data={
                "task_id": task_id,
                "topic": topic,
                "created_at": created_at.isoformat()
            },
            name=urgent_watchlist[task_id]["job_name"]
        )
    except Exception as e:
        logger.error(f"Ошибка проверки срочной задачи {task_id}: {e}")
        urgent_watchlist.pop(task_id, None)

async def report_unassigned_non_urgent(context: CallbackContext):
    if is_weekend():
        logger.debug("Выходной день — регулярные отчёты отключены")
        return
    if is_quiet_hours():
        logger.debug("Режим тишины (21:00–8:00) — регулярный отчёт пропущен")
        return
    
    try:
        all_records = await get_cached_sheet_data()
        non_urgent_unassigned = [
            r for r in all_records
            if str(r.get("Статус", "")).strip() == "Не распределено"
            and str(r.get("Приоритет", "")).strip() in ("Средний", "Низкий")
        ]
        
        if not non_urgent_unassigned:
            logger.debug("Нет нераспределённых задач со Средним/Низким приоритетом")
            return
        
        medium = [t for t in non_urgent_unassigned if str(t.get("Приоритет", "")).strip() == "Средний"]
        low = [t for t in non_urgent_unassigned if str(t.get("Приоритет", "")).strip() == "Низкий"]
        
        lines = ["📋 НЕРАСПРЕДЕЛЁННЫЕ ЗАДАЧИ (Средний/Низкий приоритет):", ""]
        
        if medium:
            lines.append(f"⚠️ Средний приоритет ({len(medium)}):")
            for task in medium:
                created = task.get("Дата создания", "")
                lines.append(f"  • {task.get('ID', '—')} — {task.get('Тема задачи', '—')} (создана: {created})")
            lines.append("")
        
        if low:
            lines.append(f"⏳ Низкий приоритет ({len(low)}):")
            for task in low:
                created = task.get("Дата создания", "")
                lines.append(f"  • {task.get('ID', '—')} — {task.get('Тема задачи', '—')} (создана: {created})")
            lines.append("")
        
        lines.append(f"Всего: {len(non_urgent_unassigned)} задач")
        message = "\n".join(lines)
        
        await context.bot.send_message(
            chat_id=RPZ_DISCUSSION_CHAT_ID,
            text=message,
            disable_notification=True
        )
        logger.info(
            f"Отправлен отчёт по нераспределённым (Средний/Низкий): "
            f"Средний={len(medium)}, Низкий={len(low)}, Всего={len(non_urgent_unassigned)}"
        )
    except Exception as e:
        logger.error(f"Ошибка отчёта по нераспределённым (Средний/Низкий): {e}")

async def check_overdue_unassigned(context: CallbackContext):
    if is_quiet_hours():
        logger.warning("Проверка просроченных задач пропущена из-за режима тишины")
        return
    if is_weekend():
        logger.info("Выходной день — уведомления о просроченных задачах отключены (только логирование)")
        return
    
    moscow_now = get_moscow_time()
    threshold_dt = moscow_now - timedelta(hours=OVERDUE_HOURS_THRESHOLD)
    
    try:
        all_records = await get_cached_sheet_data()
        overdue_tasks = []
        
        for record in all_records:
            status = str(record.get("Статус", "")).strip()
            if status != "Не распределено":
                continue
            
            created_date = str(record.get("Дата создания", "")).strip()
            created_time = str(record.get("Время создания", "")).strip() or str(record.get("Время", "")).strip()
            
            try:
                if created_time:
                    created_dt = datetime.strptime(f"{created_date} {created_time}", "%Y-%m-%d %H:%M:%S")
                else:
                    created_dt = datetime.strptime(created_date, "%Y-%m-%d")
                created_dt = timezone('Europe/Moscow').localize(created_dt)
            except (ValueError, TypeError):
                continue
            
            if created_dt < threshold_dt:
                elapsed = moscow_now - created_dt
                hours_overdue = int(elapsed.total_seconds() / 3600)
                overdue_tasks.append({
                    "id": record.get("ID", "—"),
                    "topic": record.get("Тема задачи", "—"),
                    "priority": record.get("Приоритет", "—"),
                    "author": record.get("Автор", "—"),
                    "hours": hours_overdue,
                    "created": created_dt.strftime("%Y-%m-%d %H:%M")
                })
        
        if overdue_tasks:
            overdue_tasks.sort(key=lambda x: (
                0 if x["priority"] == "Высокий" else 1 if x["priority"] == "Средний" else 2,
                -x["hours"]
            ))
            
            lines = [
                f"⚠️ ПРОСРОЧЕННЫЕ НЕРАСПРЕДЕЛЁННЫЕ ЗАДАЧИ (> {OVERDUE_HOURS_THRESHOLD} часов):",
                ""
            ]
            
            for priority_label, emoji in [("Высокий", "🚨"), ("Средний", "⚠️"), ("Низкий", "⏳")]:
                priority_tasks = [t for t in overdue_tasks if t["priority"] == priority_label]
                if priority_tasks:
                    lines.append(f"{emoji} {priority_label} приоритет ({len(priority_tasks)}):")
                    for task in priority_tasks:
                        lines.append(
                            f"  • {task['id']} — {task['topic']} "
                            f"({task['hours']}ч, автор: {task['author']})"
                        )
                    lines.append("")
            
            lines.append("❗ Требуется срочное назначение исполнителей!")
            message = "\n".join(lines)
            
            await context.bot.send_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                text=message
            )
            logger.info(f"Отправлено уведомление о {len(overdue_tasks)} просроченных задачах (>24ч)")
        else:
            logger.info("Просроченных нераспределённых задач (>24ч) не обнаружено")
    except Exception as e:
        logger.error(f"Ошибка проверки просроченных задач: {e}")

async def check_stale_in_progress(context: CallbackContext):
    moscow_now = get_moscow_time()
    if is_weekend():
        logger.debug("Выходной день — проверка зависших задач отключена")
        return
    if is_quiet_hours():
        logger.debug("Режим тишины (21:00–8:00) — проверка зависших задач пропущена")
        return
    
    try:
        all_records = await get_cached_sheet_data()
        stale_tasks = []
        
        for record in all_records:
            status = str(record.get("Статус", "")).strip()
            
            # === ИСКЛЮЧЕНИЕ: Операционные задачи не проверяются ===
            status_lower = status.lower()
            if "операционная задача" in status_lower or "опер. задача" in status_lower:
                continue
            
            if status != "В работе":
                continue
            
            executor_name = str(record.get("Исполнитель", "")).strip()
            if not executor_name:
                continue
            
            priority = str(record.get("Приоритет", "Средний")).strip()
            assigned_date_str = str(record.get("Дата время назначения", "")).strip()
            if not assigned_date_str:
                continue
            
            if priority == "Высокий":
                max_hours = STALE_HIGH_PRIORITY_LIMIT
                emoji = "🔴"
            elif priority == "Средний":
                max_hours = STALE_MEDIUM_PRIORITY_LIMIT
                emoji = "🟠"
            else:
                max_hours = STALE_LOW_PRIORITY_LIMIT
                emoji = "🟡"
            
            try:
                assigned_dt = datetime.strptime(assigned_date_str, "%Y-%m-%d %H:%M:%S")
                assigned_dt = timezone('Europe/Moscow').localize(assigned_dt)
            except (ValueError, TypeError):
                continue
            
            elapsed = moscow_now - assigned_dt
            elapsed_hours = elapsed.total_seconds() / 3600
            
            if elapsed_hours > max_hours:
                overdue_hours = elapsed_hours - max_hours
                
                executor_username = None
                for username, name in users_mapping.items():
                    if name == executor_name:
                        executor_username = username
                        break
                
                if executor_username:
                    executor_display = f"{executor_name} ({executor_username})"
                else:
                    if executor_name.startswith("@"):
                        executor_display = executor_name
                    else:
                        executor_display = executor_name
                
                stale_tasks.append({
                    "id": record.get("ID", "—"),
                    "topic": record.get("Тема задачи", "—"),
                    "executor_display": executor_display,
                    "priority": priority,
                    "assigned": assigned_date_str,
                    "elapsed_hours": round(elapsed_hours, 1),
                    "overdue_hours": round(overdue_hours, 1),
                    "emoji": emoji,
                    "max_hours": max_hours
                })
        
        if stale_tasks:
            lines = [
                "⏳ Задачи 'В РАБОТЕ' превысили допустимый лимит:",
                ""
            ]
            
            for task in stale_tasks:
                lines.append(
                    f"{task['emoji']} {task['id']} — {task['topic']}\n"
                    f"   Исполнитель: {task['executor_display']}\n"
                    f"   Приоритет: {task['priority']} (лимит: {task['max_hours']}ч)\n"
                    f"   В работе: {task['elapsed_hours']}ч (превышение: +{task['overdue_hours']}ч)\n"
                )
            
            lines.append("Рекомендуется уточнить статус у исполнителя")
            message = "\n".join(lines)
            
            await context.bot.send_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                text=message,
                disable_notification=True
            )
            logger.info(
                f"Отправлено уведомление о {len(stale_tasks)} зависших задачах "
                f"(Высокий: {STALE_HIGH_PRIORITY_LIMIT}ч, Средний: {STALE_MEDIUM_PRIORITY_LIMIT}ч, Низкий: {STALE_LOW_PRIORITY_LIMIT}ч)"
            )
        else:
            logger.debug("Зависших задач не обнаружено")
    except Exception as e:
        logger.error(f"Ошибка проверки зависших задач: {e}")

async def morning_digest(context: CallbackContext):
    if is_quiet_hours():
        logger.warning("Утренний дайджест пропущен из-за режима тишины")
        return
    
    try:
        all_records = await get_cached_sheet_data()
        moscow_now = get_moscow_time()
        threshold_dt = moscow_now - timedelta(hours=OVERDUE_HOURS_THRESHOLD)
        
        urgent_unassigned = [
            r for r in all_records
            if r.get("Статус", "").strip() == "Не распределено"
            and r.get("Приоритет", "").strip() == "Высокий"
        ]
        
        overdue = []
        for r in all_records:
            if r.get("Статус", "").strip() != "Не распределено":
                continue
            try:
                created_dt_str = f"{r.get('Дата создания', '')} {r.get('Время создания', '')}".strip()
                created_dt = datetime.strptime(created_dt_str, "%Y-%m-%d %H:%M:%S")
                created_dt = timezone('Europe/Moscow').localize(created_dt)
                if created_dt < threshold_dt:
                    overdue.append(r)
            except:
                continue
        
        stale = []
        for r in all_records:
            if r.get("Статус", "").strip() != "В работе":
                continue
            try:
                assigned_dt = datetime.strptime(r.get("Дата время назначения", ""), "%Y-%m-%d %H:%M:%S")
                assigned_dt = timezone('Europe/Moscow').localize(assigned_dt)
                elapsed_hours = (moscow_now - assigned_dt).total_seconds() / 3600
                priority = r.get("Приоритет", "Средний").strip()
                
                status_lower = r.get("Статус", "").lower()
                if "операционная задача" in status_lower or "опер. задача" in status_lower:
                    continue
                
                max_hours = STALE_HIGH_PRIORITY_LIMIT if priority == "Высокий" else \
                           STALE_MEDIUM_PRIORITY_LIMIT if priority == "Средний" else \
                           STALE_LOW_PRIORITY_LIMIT
                
                if elapsed_hours > max_hours:
                    stale.append(r)
            except:
                continue
        
        if is_weekend():
            lines = ["🌅 УТРЕННИЙ ДАЙДЖЕСТ (выходной день)", ""]
            if urgent_unassigned or overdue or stale:
                lines.append("ℹ️ Сводка по задачам на начало выходного дня:")
                if urgent_unassigned:
                    lines.append(f" • Срочных нераспределённых: {len(urgent_unassigned)}")
                if overdue:
                    lines.append(f" • Просроченных (> {OVERDUE_HOURS_THRESHOLD}ч): {len(overdue)}")
                if stale:
                    lines.append(f" • Зависших в работе: {len(stale)}")
                lines.append("")
                lines.append("💡 Задачи будут обработаны в рабочие дни.")
            else:
                lines.append("✅ Все задачи в нормальном состоянии")
                lines.append("")
                lines.append("Хороших выходных! 🌴")
        else:
            lines = ["🌅 УТРЕННИЙ ДАЙДЖЕСТ (рабочий день)", ""]
            if urgent_unassigned:
                lines.append(f"🚨 НЕРАСПРЕДЕЛЁННЫЕ СРОЧНЫЕ ЗАДАЧИ ({len(urgent_unassigned)}):")
                for task in urgent_unassigned:
                    lines.append(f"  • {task.get('ID', '—')} — {task.get('Тема задачи', '—')}")
                lines.append("")
            if overdue:
                lines.append(f"⚠️ ПРОСРОЧЕННЫЕ ЗАДАЧИ (> {OVERDUE_HOURS_THRESHOLD}ч, {len(overdue)}):")
                for task in overdue:
                    lines.append(f"  • {task.get('ID', '—')} — {task.get('Тема задачи', '—')}")
                lines.append("")
            if stale:
                lines.append(f"⏳ ЗАВИСШИЕ ЗАДАЧИ В РАБОТЕ ({len(stale)}):")
                for task in stale:
                    prio = task.get("Приоритет", "—")
                    executor = task.get("Исполнитель", "—")
                    lines.append(f"  • {task.get('ID', '—')} [{prio}] у {executor}")
                lines.append("")
            if not (urgent_unassigned or overdue or stale):
                lines.append("✅ Все задачи в нормальном состоянии")
            else:
                lines.append("❗ Требуется внимание руководителя")
        
        message = "\n".join(lines)
        
        await context.bot.send_message(
            chat_id=RPZ_DISCUSSION_CHAT_ID,
            text=message
        )
        
        if not (urgent_unassigned or overdue):
            note = "Все задачи в нормальном состоянии"
        else:
            note = "Требуется внимание руководителя"
        
        metrics = {
            "period": moscow_now.strftime("%Y-%m-%d"),
            "total": len(all_records),
            "high": len([t for t in all_records if t.get("Приоритет") == "Высокий"]),
            "medium": len([t for t in all_records if t.get("Приоритет") == "Средний"]),
            "low": len([t for t in all_records if t.get("Приоритет") == "Низкий"]),
            "operational": len([t for t in all_records if "операционная задача" in str(t.get("Статус", "")).lower() or "опер. задача" in str(t.get("Статус", "")).lower()]),
            "overdue": len(overdue),
            "stale": len(stale),
            "urgent_unassigned": len(urgent_unassigned),
            "note": note
        }
        log_digest_metrics("morning", metrics)
        
        logger.info(f"Отправлен утренний дайджест ({'выходной' if is_weekend() else 'будний'} день)")
    except Exception as e:
        logger.error(f"Ошибка формирования утреннего дайджеста: {e}")

async def weekly_digest(app: Application):
    """Еженедельный дайджест по понедельникам в 9:00"""
    bot = app.bot
    moscow_now = get_moscow_time()
    
    week_start = moscow_now.date() - timedelta(days=moscow_now.weekday() + 7)
    week_end = week_start + timedelta(days=6)
    
    try:
        all_records = await safe_get_all_records()
        tasks_df = pd.DataFrame(all_records)
        
        if tasks_df.empty:
            await bot.send_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                text="📊 Еженедельный дайджест\nНет данных за прошлую неделю."
            )
            return
        
        tasks_df['created_date'] = pd.to_datetime(tasks_df['Дата создания'], errors='coerce').dt.date
        weekly_tasks = tasks_df[
            (tasks_df['created_date'] >= week_start) & 
            (tasks_df['created_date'] <= week_end)
        ].copy()
        
        if weekly_tasks.empty:
            await bot.send_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                text=f"📊 Еженедельный дайджест ({week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')})\nНет задач за указанный период."
            )
            return
        
        priority_counts = weekly_tasks['Приоритет'].value_counts()
        
        operational_tasks = weekly_tasks[
            weekly_tasks['Статус'].apply(lambda s: "операционная задача" in str(s).lower() or "опер. задача" in str(s).lower())
        ]
        regular_tasks = weekly_tasks[~weekly_tasks['Статус'].apply(lambda s: "операционная задача" in str(s).lower() or "опер. задача" in str(s).lower())]
        
        completed_on_time = regular_tasks[
            (regular_tasks['Статус'] == 'Выполнено') & 
            (pd.to_datetime(regular_tasks['Дата время выполнения'], errors='coerce').dt.date <= week_end)
        ]
        total_created = len(weekly_tasks)
        on_time_percent = (len(completed_on_time) / len(regular_tasks) * 100) if len(regular_tasks) > 0 else 0
        
        stale_tasks = []
        for _, task in weekly_tasks.iterrows():
            status = str(task['Статус']).lower()
            if "операционная задача" in status or "опер. задача" in status:
                continue
            
            if task['Статус'] in ['В работе', 'Не распределено']:
                try:
                    created_dt = datetime.strptime(
                        f"{task['Дата создания']} {task.get('Время создания', task.get('Время', '00:00:00'))}", 
                        "%Y-%m-%d %H:%M:%S"
                    )
                    created_dt = timezone('Europe/Moscow').localize(created_dt)
                    elapsed_hours = (moscow_now - created_dt).total_seconds() / 3600
                    
                    priority = task['Приоритет']
                    limit = {
                        'Высокий': STALE_HIGH_PRIORITY_LIMIT,
                        'Средний': STALE_MEDIUM_PRIORITY_LIMIT,
                        'Низкий': STALE_LOW_PRIORITY_LIMIT
                    }.get(priority, 24)
                    
                    if elapsed_hours > limit:
                        stale_tasks.append({
                            'id': task['ID'],
                            'topic': task['Тема задачи'],
                            'priority': priority,
                            'hours': round(elapsed_hours, 1),
                            'executor': task.get('Исполнитель', '—')
                        })
                except:
                    continue
        
        stale_tasks = sorted(stale_tasks, key=lambda x: x['hours'], reverse=True)[:3]
        
        lines = [
            f"📊 ЕЖЕНЕДЕЛЬНЫЙ ДАЙДЖЕСТ",
            f"Период: {week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')}",
            "",
            f"📈 Создано задач: {total_created}",
            f"✅ Выполнено в срок: {len(completed_on_time)} ({on_time_percent:.1f}%)",
            f"🔵 Операционные задачи: {len(operational_tasks)}",
            ""
        ]
        
        lines.append("📌 Распределение по приоритетам:")
        for prio in ['Высокий', 'Средний', 'Низкий']:
            count = priority_counts.get(prio, 0)
            oper_count = len(operational_tasks[operational_tasks['Приоритет'] == prio])
            if count > 0:
                if oper_count > 0:
                    lines.append(f"  • {prio}: {count} (в т.ч. {oper_count} операционные)")
                else:
                    lines.append(f"  • {prio}: {count}")
        lines.append("")
        
        if stale_tasks:
            lines.append("⚠️ Топ-3 самых долгих задач (исключая операционные):")
            for i, task in enumerate(stale_tasks, 1):
                exec_info = f" (исполнитель: {task['executor']})" if task['executor'] != '—' else ""
                lines.append(
                    f"  {i}. {task['id']} [{task['priority']}] — {task['hours']}ч{exec_info}\n"
                    f"     {task['topic'][:50]}..."
                )
            lines.append("")
        
        lines.append("📎 Визуализация активности прикреплена ниже")
        message = "\n".join(lines)
        
        try:
            chart_buf, heatmap_buf = generate_weekly_charts(weekly_tasks, week_start, week_end)
            
            await bot.send_message(chat_id=RPZ_DISCUSSION_CHAT_ID, text=message)
            await bot.send_photo(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                photo=chart_buf,
                caption="📈 График активности задач по дням"
            )
            await bot.send_photo(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                photo=heatmap_buf,
                caption="🌡️ Heatmap активности по дням недели и часам"
            )
            
            metrics = {
                "period": f"{week_start.strftime('%Y-%m-%d')} - {week_end.strftime('%Y-%m-%d')}",
                "total": total_created,
                "high": priority_counts.get("Высокий", 0),
                "medium": priority_counts.get("Средний", 0),
                "low": priority_counts.get("Низкий", 0),
                "operational": len(operational_tasks),
                "overdue": len(weekly_tasks) - len(completed_on_time),
                "stale": len(stale_tasks),
                "urgent_unassigned": len([t for t in weekly_tasks if t["Статус"] == "Не распределено" and t["Приоритет"] == "Высокий"]),
                "on_time_percent": round(on_time_percent, 1),
                "note": "Еженедельный дайджест с визуализацией"
            }
            log_digest_metrics("weekly", metrics)
            
            logger.info(f"Еженедельный дайджест отправлен за период {week_start} - {week_end}")
        except Exception as e:
            logger.error(f"Ошибка генерации визуализации: {e}")
            await bot.send_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                text=f"{message}\n\n⚠️ Не удалось сгенерировать графики: {str(e)}"
            )
    
    except Exception as e:
        logger.error(f"Ошибка формирования еженедельного дайджеста: {e}")
        await bot.send_message(
            chat_id=RPZ_DISCUSSION_CHAT_ID,
            text=f"❌ Ошибка при формировании еженедельного дайджеста: {str(e)}"
        )

async def evening_pause_notify(context: CallbackContext):
    if is_weekend():
        msg = (
            "🌙 Режим тишины активирован (21:00–10:00 МСК)\n"
            "Все уведомления и проверки приостановлены до 10:00 утра.\n"
            "Хороших выходных! 🌴"
        )
    else:
        msg = (
            "🌙 Режим тишины активирован (21:00–8:00 МСК)\n"
            "Все уведомления и проверки приостановлены до 8:00 утра."
        )
    
    await context.bot.send_message(
        chat_id=RPZ_DISCUSSION_CHAT_ID,
        text=msg,
        disable_notification=True
    )
    logger.info("Отправлено уведомление о начале режима тишины")

async def pause_all_timers(context: CallbackContext):
    global urgent_watchlist, paused_timers
    if not urgent_watchlist:
        logger.info("Нет активных таймеров для приостановки")
        return
    
    moscow_now = get_moscow_time()
    paused_timers = urgent_watchlist.copy()
    
    for task_id, data in urgent_watchlist.items():
        job_name = data["job_name"]
        current_jobs = context.job_queue.get_jobs_by_name(job_name)
        for job in current_jobs:
            job.schedule_removal()
        logger.info(f"Таймер задачи {task_id} приостановлен")
    
    for task_id, data in paused_timers.items():
        elapsed = moscow_now - data["created_at"]
        elapsed_minutes = int(elapsed.total_seconds() / 60)
        paused_timers[task_id]["elapsed_minutes"] = elapsed_minutes
    
    urgent_watchlist = {}
    logger.info(f"Приостановлено {len(paused_timers)} таймеров срочных задач в 21:00")

async def resume_all_timers(context: CallbackContext):
    global urgent_watchlist, paused_timers
    if not paused_timers:
        logger.info("Нет приостановленных таймеров для возобновления")
        return
    
    moscow_now = get_moscow_time()
    urgent_watchlist = {}
    
    for task_id, data in paused_timers.items():
        elapsed_minutes = data.get("elapsed_minutes", 0)
        next_notify_at = (elapsed_minutes // URGENT_UNASSIGNED_INTERVAL + 1) * URGENT_UNASSIGNED_INTERVAL
        remaining_minutes = next_notify_at - elapsed_minutes
        if remaining_minutes <= 0:
            remaining_minutes = URGENT_UNASSIGNED_INTERVAL
        
        job_name = f"urgent_watch_{task_id}"
        urgent_watchlist[task_id] = {
            "created_at": data["created_at"],
            "job_name": job_name,
            "topic": data["topic"],
            "notified_count": data.get("notified_count", 0)
        }
        
        context.job_queue.run_once(
            check_urgent_unassigned,
            remaining_minutes * 60,
            data={
                "task_id": task_id,
                "topic": data["topic"],
                "created_at": data["created_at"].isoformat()
            },
            name=job_name
        )
        
        logger.info(
            f"Таймер задачи {task_id} возобновлён (следующее уведомление через {remaining_minutes} мин)"
        )
    
    paused_timers = {}
    logger.info(f"Возобновлено {len(urgent_watchlist)} таймеров срочных задач")

async def recover_urgent_tasks_on_startup(application: Application):
    logger.info("Восстановление таймеров срочных задач после перезапуска...")
    try:
        all_records = await safe_get_all_records()
        moscow_now = get_moscow_time()
        recovered = 0
        notified_immediately = 0
        
        for record in all_records:
            status = str(record.get("Статус", "")).strip()
            priority = str(record.get("Приоритет", "")).strip()
            task_id = str(record.get("ID", "")).strip()
            
            if status != "Не распределено" or priority != "Высокий" or not task_id.startswith("TASK-"):
                continue
            
            created_date = str(record.get("Дата создания", "")).strip()
            created_time = str(record.get("Время создания", "")).strip() or str(record.get("Время", "")).strip()
            
            try:
                if created_time:
                    created_dt_str = f"{created_date} {created_time}"
                    created_dt = datetime.strptime(created_dt_str, "%Y-%m-%d %H:%M:%S")
                else:
                    created_dt = datetime.strptime(created_date, "%Y-%m-%d")
                created_dt = timezone('Europe/Moscow').localize(created_dt)
            except (ValueError, TypeError) as e:
                logger.warning(f"Не удалось распарсить время создания для {task_id}: {e}")
                continue
            
            elapsed = moscow_now - created_dt
            elapsed_minutes = int(elapsed.total_seconds() / 60)
            topic = str(record.get("Тема задачи", "Без темы")).strip()
            job_name = f"urgent_watch_{task_id}"
            
            if elapsed_minutes >= URGENT_UNASSIGNED_DELAY and is_weekday():
                notify_count = (elapsed_minutes - URGENT_UNASSIGNED_DELAY) // URGENT_UNASSIGNED_INTERVAL + 1
                alert_msg = (
                    f"🚨 СРОЧНО! Нераспределённая задача с ВЫСОКИМ приоритетом (восстановлено после перезапуска)\n"
                    f"Задача: {task_id}\n"
                    f"Тема: {topic}\n"
                    f"Время ожидания: {elapsed_minutes} мин\n"
                    f"Уведомление: #{notify_count}\n"
                    f"❗ Требуется немедленное назначение исполнителя!"
                )
                
                await application.bot.send_message(
                    chat_id=RPZ_DISCUSSION_CHAT_ID,
                    text=alert_msg
                )
                logger.warning(
                    f"Восстановлено уведомление #{notify_count} для {task_id} "
                    f"(ожидание: {elapsed_minutes} мин после перезапуска)"
                )
                notified_immediately += 1
            
            if is_weekday():
                if elapsed_minutes < URGENT_UNASSIGNED_DELAY:
                    next_notify_in = URGENT_UNASSIGNED_DELAY - elapsed_minutes
                else:
                    next_interval = (elapsed_minutes - URGENT_UNASSIGNED_DELAY) % URGENT_UNASSIGNED_INTERVAL
                    next_notify_in = URGENT_UNASSIGNED_INTERVAL - next_interval
                
                application.job_queue.run_once(
                    check_urgent_unassigned,
                    next_notify_in * 60,
                    data={
                        "task_id": task_id,
                        "topic": topic,
                        "created_at": created_dt.isoformat()
                    },
                    name=job_name
                )
                
                urgent_watchlist[task_id] = {
                    "created_at": created_dt,
                    "job_name": job_name,
                    "topic": topic,
                    "notified_count": notified_immediately
                }
                recovered += 1
                logger.info(
                    f"Восстановлен таймер для {task_id} (ожидание: {elapsed_minutes} мин, "
                    f"следующее уведомление через {next_notify_in if is_weekday() else 'выходные'} мин)"
                )
        
        if recovered:
            logger.info(
                f"Восстановлено {recovered} таймеров срочных задач "
                f"({notified_immediately} получили немедленное уведомление)"
            )
        else:
            logger.info("Нет нераспределённых задач с высоким приоритетом для восстановления")
    except Exception as e:
        logger.error(f"Ошибка восстановления таймеров после перезапуска: {e}")

# ======================
# Запуск бота
# ======================

def main():
    initialize_task_counter()
    load_users_mapping()
    load_user_settings()
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("test", test_command))
    application.add_handler(CommandHandler("today", cmd_today))
    application.add_handler(CommandHandler("pending", cmd_pending))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("task", cmd_task))
    application.add_handler(CommandHandler("limits", cmd_limits))
    application.add_handler(CommandHandler("settings", cmd_settings))
    application.add_handler(CommandHandler("oper", cmd_operational))
    
    application.add_handler(MessageHandler(
        filters.REPLY & filters.Chat(RPZ_DISCUSSION_CHAT_ID),
        handle_task_reply
    ))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Chat(RPZ_DISCUSSION_CHAT_ID),
        handle_new_task
    ))
    
    application.add_handler(CallbackQueryHandler(settings_callback, pattern="^toggle_digest_"))
    
    async def startup_recovery(app):
        await recover_urgent_tasks_on_startup(app)
    
    application.post_init = startup_recovery
    
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    
    scheduler.add_job(
        evening_pause_notify,
        'cron',
        hour=21,
        minute=0,
        args=[application],
        id='evening_pause_notify'
    )
    scheduler.add_job(
        pause_all_timers,
        'cron',
        hour=21,
        minute=0,
        args=[application],
        id='pause_timers'
    )
    scheduler.add_job(
        morning_digest,
        'cron',
        hour=8,
        minute=0,
        args=[application],
        id='morning_digest'
    )
    scheduler.add_job(
        resume_all_timers,
        'cron',
        hour=8,
        minute=1,
        args=[application],
        id='resume_timers'
    )
    scheduler.add_job(
        weekly_digest,
        'cron',
        day_of_week='mon',
        hour=9,
        minute=0,
        args=[application],
        id='weekly_digest',
        misfire_grace_time=3600
    )
    scheduler.add_job(
        report_unassigned_non_urgent,
        'interval',
        minutes=UNASSIGNED_REPORT_INTERVAL,
        args=[application],
        id='non_urgent_unassigned_report'
    )
    scheduler.add_job(
        check_stale_in_progress,
        'interval',
        minutes=STALE_CHECK_INTERVAL,
        args=[application],
        id='stale_check'
    )
    scheduler.add_job(
        check_overdue_unassigned,
        'cron',
        hour=10,
        minute=0,
        args=[application],
        id='overdue_check'
    )
    
    scheduler.start()
    
    logger.info(
        "Бот запущен:\n"
        f"  • 🌙 Режим тишины: будни 21:00–8:00, выходные 21:00–10:00\n"
        f"  • 📅 Выходные (Сб/Вс): отключены циклические уведомления, регулярные отчёты и проверка зависших задач\n"
        f"  • 🌅 Утренний дайджест: будни 8:00 (стандартный), выходные 8:00 (мягкий сводный)\n"
        f"  • 📊 Еженедельный дайджест: Пн 9:00 с визуализацией и логированием в 'Аналитика'\n"
        f"  • 🚨 Срочные задачи: уведомления каждые {URGENT_UNASSIGNED_INTERVAL} мин (> {URGENT_UNASSIGNED_DELAY} мин без назначения, только будни)\n"
        f"  • 📋 Средний/Низкий: автоотчёт каждые {UNASSIGNED_REPORT_INTERVAL} мин (только будни)\n"
        f"  • ⚠️ Просроченные: ежедневно в 10:00 (> {OVERDUE_HOURS_THRESHOLD} часов, только будни)\n"
        f"  • ⏳ Зависшие задачи: лимиты — Высокий: {STALE_HIGH_PRIORITY_LIMIT}ч, Средний: {STALE_MEDIUM_PRIORITY_LIMIT}ч, Низкий: {STALE_LOW_PRIORITY_LIMIT}ч (только будни)\n"
        f"  • 🔵 Операционные задачи: не участвуют в проверке зависших, отдельная команда /oper и тег #опер, #операционная, #опер. задача"
    )
    
    application.run_polling()

if __name__ == "__main__":
    main()
