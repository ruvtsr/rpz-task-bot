import os
import re
import logging
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application, MessageHandler, CommandHandler, filters,
    ContextTypes, CallbackContext
)
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone

# ======================
# Настройка логгера
# ======================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ======================
# Загрузка конфигурации
# ======================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
RPZ_DISCUSSION_CHAT_ID = int(os.getenv("RPZ_DISCUSSION_CHAT_ID"))
RPZ_ANNOUNCE_CHANNEL_ID = int(os.getenv("RPZ_ANNOUNCE_CHANNEL_ID"))
ENGINEERS_CHANNEL_ID = int(os.getenv("ENGINEERS_CHANNEL_ID"))
SOROKIN_USER_ID = int(os.getenv("SOROKIN_USER_ID"))
ALLOWED_USERS = list(map(int, os.getenv("ALLOWED_USERS").split(",")))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
REPORT_HOUR, REPORT_MINUTE = map(int, os.getenv("REPORT_TIME", "20:00").split(":"))

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

def get_moscow_time():
    return datetime.now(timezone('Europe/Moscow'))

# ======================
# Вспомогательная функция для форматирования списка задач (резервная)
# ======================
def format_task_list(tasks, title="Задачи"):
    if not tasks:
        return f"📋 {title}\n\nНет задач."
    
    lines = [f"📋 {title} ({len(tasks)}):", ""]
    for task in tasks:
        task_id = task.get("ID", "—")
        topic = task.get("Тема задачи", "—")
        author = task.get("Автор", "—")
        status = task.get("Статус", "—")
        executor = task.get("Исполнитель", "")
        created = task.get("Дата создания", "")
        
        line = f"• {task_id} — {topic}"
        if status != "Не распределено":
            line += f" → {executor}"
        line += f" ({author}, {created})"
        lines.append(line)
    
    return "\n".join(lines)

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

def is_allowed_user(user_id: int) -> bool:
    return user_id in ALLOWED_USERS

def extract_topic_and_desc(text: str):
    # Проверяем точный префикс
    if not (text.startswith("#З ") or text == "#З"):
        return None, None
        
    if text == "#З":
        return "", ""
        
    content = text[3:].lstrip()  # Пропускаем "#З "
    if "\n" in content:
        topic, desc = content.split("\n", 1)
    else:
        topic, desc = content, ""
    return topic.strip(), desc.strip()

def has_sorokin_tag(user_id: int) -> str:
    return "#От Сорокина" if user_id == SOROKIN_USER_ID else ""

def extract_priority(text: str) -> str:
    text_lower = text.lower()
    if "#высокий" in text_lower or "#срочно" in text_lower:
        return "Высокий"
    elif "#низкий" in text_lower:
        return "Низкий"
    else:
        return "Средний"

def format_task_message(task_data, status_line=""):
    author = task_data["author"]
    topic = task_data["topic"]
    description = task_data["description"] or "—"
    priority = task_data.get("priority", "Средний")
    created = task_data["created_str"]
    assigned = task_data.get("assigned_str", "")
    completed = task_data.get("completed_str", "")
    executor = task_data.get("executor", "")

    lines = [f"Задача #{task_data['id']}\n"]
    lines.append(f"Автор: {author}")
    lines.append(f"Тема: {topic}")
    lines.append(f"Описание: ")
    lines.append(f"{description}")
    lines.append(f"\nПриоритет: {priority}")
    lines.append(f"Статус: {task_data['status']}")
    lines.append("______________________")
    lines.append(f"Оформлено: {created}")

    if assigned:
        lines.append(f"Взято в работу: {assigned}")
    if completed:
        lines.append(f"Выполнено: {completed}")
    if status_line:
        lines.append("______________________")
        lines.append(status_line)

    return "\n".join(lines)

# ======================
# Команда /test
# ======================

async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    if user:
        await update.message.reply_text(
            f"✅ Бот работает!\nChat ID: {chat_id}\nUser ID: {user.id}\nUsername: @{user.username}"
        )
    else:
        await update.message.reply_text("⚠️ Анонимное сообщение.")

# ======================
# Команда /today — задачи за сегодня (улучшенная)
# ======================

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    today = datetime.now(timezone('Europe/Moscow')).strftime("%Y-%m-%d")
    
    try:
        all_records = SHEET.get_all_records()
        today_tasks = [r for r in all_records if r.get("Дата создания") == today]
        
        # Группировка
        pending = []
        in_progress = {}
        completed = {}
        
        for task in today_tasks:
            task_id = task.get("ID", "—")
            topic = task.get("Тема задачи", "—")
            executor = task.get("Исполнитель", "")
            status = task.get("Статус", "—")
            
            line = f"• {task_id} {topic}"
            
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
        
        # Просроченные (нераспределённые не сегодня)
        overdue = []
        for task in all_records:
            if (task.get("Статус") == "Не распределено" and 
                task.get("Дата создания") != today):
                task_id = task.get("ID", "—")
                topic = task.get("Тема задачи", "—")
                overdue.append(f"• {task_id} {topic}")
        
        # Формирование сообщения
        lines = [f"📅 Дата: {today}", ""]
        
        # Не распределено
        if pending:
            lines.extend(["⏳ Не распределено:"] + pending + [""])
        
        # В работе
        if in_progress:
            lines.append("🔄 В работе:")
            for executor, tasks in in_progress.items():
                display_executor = executor if executor.startswith("@") else f"@{executor}"
                lines.append(display_executor)
                lines.extend(tasks)
            lines.append("")
        
        # Выполнено
        if completed:
            lines.append("✅ Выполнено:")
            for executor, tasks in completed.items():
                display_executor = executor if executor.startswith("@") else f"@{executor}"
                lines.append(display_executor)
                lines.extend(tasks)
            lines.append("")
        
        # Просроченные
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

# ======================
# Команда /pending — нераспределённые задачи (улучшенная)
# ======================

async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    try:
        all_records = SHEET.get_all_records()
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

# ======================
# Команда /stats — статистика
# ======================

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    try:
        all_records = SHEET.get_all_records()
        total = len(all_records)
        completed = sum(1 for r in all_records if r.get("Статус") == "Выполнено")
        pending = sum(1 for r in all_records if r.get("Статус") == "Не распределено")
        in_progress = total - completed - pending
        
        # Статистика за сегодня
        today = datetime.now(timezone('Europe/Moscow')).strftime("%Y-%m-%d")
        created_today = sum(1 for r in all_records if r.get("Дата создания") == today)
        
        message = (
            "📊 **Статистика по задачам**\n\n"
            f"Всего задач: {total}\n"
            f"✅ Выполнено: {completed}\n"
            f"🔄 В работе: {in_progress}\n"
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

# ======================
# Создание задачи (строго по #З + поддержка описания)
# ======================

async def handle_new_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user

    if not message or not user:
        return

    # Игнорируем анонимные сообщения
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

    # Если уже есть начатая задача — добавляем к описанию
    if key in pending_tasks:
        pending_tasks[key]["desc_parts"].append(text)
        pending_tasks[key]["msg_ids"].append(message.message_id)
        return

    # Проверяем точный формат для начала новой задачи
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

# ======================
# Финализация задачи
# ======================

async def finalize_task_now(context: CallbackContext, key):
    if key in pending_tasks:
        job = type('Job', (), {'data': {"key": key}})()
        await finalize_task_job(job, context)

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

    # Удаляем исходные сообщения из чата обсуждений
    for msg_id in msg_ids:
        try:
            await context.bot.delete_message(chat_id=RPZ_DISCUSSION_CHAT_ID, message_id=msg_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить {msg_id}: {e}")

    # Публикуем ТОЛЬКО в канале РПЗ
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

    # Сохраняем в таблицу
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
            str(channel_msg.message_id),  # Msg_ID канала
            "",  # Thread_ID не используется
            priority
        ]
        SHEET.append_row(row)
        logger.info(f"Задача сохранена в таблицу: {task_id}")
    except Exception as e:
        logger.error(f"Ошибка сохранения в таблицу: {e}")

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

    # === Извлекаем TASK-XXXX из цитаты ===
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

    # === Находим задачу по ID ===
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

    # === Назначение ===
    username_match = re.search(r'@[\w\d_]+', text)
    if username_match:
        executor_handle = username_match.group(0)
        executor_name = users_mapping.get(executor_handle, executor_handle)
        display_name_with_username = f"{executor_name} ({executor_handle})"

        now = get_moscow_time()
        try:
            author = SHEET.cell(row_idx, 6).value or "—"
            topic = SHEET.cell(row_idx, 4).value or "—"
            description = SHEET.cell(row_idx, 5).value or "—"
            priority = SHEET.cell(row_idx, 14).value or "Средний"
            created_date = SHEET.cell(row_idx, 2).value or ""
            created_time = SHEET.cell(row_idx, 3).value or ""
            created = f"{created_date} {created_time}".strip()

            # Сохраняем в таблицу только имя (без username)
            SHEET.update_cell(row_idx, 7, executor_name)
            SHEET.update_cell(row_idx, 8, "В работе")
            assign_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
            SHEET.update_cell(row_idx, 9, assign_time_str)

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

            # Обновляем сообщение в канале
            try:
                msg_id_str = SHEET.cell(row_idx, 12).value
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

            # Уведомление с именем и username
            try:
                await context.bot.send_message(
                    chat_id=ENGINEERS_CHANNEL_ID,
                    text=f"Назначено на {display_name_with_username}"
                )
            except Exception as e:
                logger.error(f"Не удалось отправить в Инженеры КС: {e}")

            logger.info(f"Задача {task_id} назначена на {display_name_with_username}")
            processed = True

        except Exception as e:
            logger.error(f"Ошибка при назначении: {e}")

    # === Закрытие ===
    elif any(trigger in text.lower() for trigger in ["#выполнено", "готово", "решено"]):
        now = get_moscow_time()
        try:
            # Получаем только имя из таблицы (без username)
            executor_name = SHEET.cell(row_idx, 7).value or (
                users_mapping.get(f"@{user.username}", user.full_name) if user.username else user.full_name
            )
            complete_time_str = now.strftime("%Y-%m-%d %H:%M:%S")

            SHEET.update_cell(row_idx, 8, "Выполнено")
            SHEET.update_cell(row_idx, 10, complete_time_str)

            author = SHEET.cell(row_idx, 6).value or "—"
            topic = SHEET.cell(row_idx, 4).value or "—"
            description = SHEET.cell(row_idx, 5).value or "—"
            priority = SHEET.cell(row_idx, 14).value or "Средний"
            created_date = SHEET.cell(row_idx, 2).value or ""
            created_time = SHEET.cell(row_idx, 3).value or ""
            created = f"{created_date} {created_time}".strip()
            assigned = SHEET.cell(row_idx, 9).value or ""

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
                "executor": executor_name
            }, f"Выполнил {executor_name}")

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

            # Уведомление ТОЛЬКО с именем (без username)
            try:
                await context.bot.send_message(
                    chat_id=ENGINEERS_CHANNEL_ID,
                    text=f"Задача №{task_id} — Выполнено {executor_name}"
                )
            except Exception as e:
                logger.error(f"Не удалось отправить в Инженеры КС: {e}")

            logger.info(f"Задача {task_id} закрыта")
            processed = True

        except Exception as e:
            logger.error(f"Ошибка при закрытии: {e}")

    # Удаляем сообщение-действие
    if processed:
        try:
            await context.bot.delete_message(
                chat_id=RPZ_DISCUSSION_CHAT_ID,
                message_id=action_msg_id
            )
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение действия: {e}")

# ======================
# Ежедневный отчёт
# ======================

async def daily_report(context: CallbackContext):
    today = get_moscow_time().strftime("%Y-%m-%d")
    try:
        all_records = SHEET.get_all_records()
        created = sum(1 for r in all_records if r.get("Дата создания") == today)
        assigned = sum(1 for r in all_records if str(r.get("Дата время назначения", "")).startswith(today))
        completed = sum(1 for r in all_records if str(r.get("Дата время выполнения", "")).startswith(today))
        pending = sum(1 for r in all_records if r.get("Статус") != "Выполнено" and r.get("Дата создания") == today)
    except Exception as e:
        logger.error(f"Ошибка отчёта: {e}")
        created = assigned = completed = pending = "ошибка"

    report = (
        f"📆 Отчёт за {today}\n\n"
        f"📥 Создано задач: {created}\n"
        f"📤 Назначено: {assigned}\n"
        f"✅ Выполнено: {completed}\n"
        f"⏳ В работе / не распределено: {pending}"
    )
    try:
        await context.bot.send_message(chat_id=ENGINEERS_CHANNEL_ID, text=report)
    except Exception as e:
        logger.error(f"Не удалось отправить отчёт: {e}")

# ======================
# Запуск
# ======================

def main():
    initialize_task_counter()
    load_users_mapping()

    application = Application.builder().token(BOT_TOKEN).build()

    # Основные команды
    application.add_handler(CommandHandler("test", test_command))
    application.add_handler(CommandHandler("today", cmd_today))
    application.add_handler(CommandHandler("pending", cmd_pending))
    application.add_handler(CommandHandler("stats", cmd_stats))
    
    # Обработчики задач
    application.add_handler(MessageHandler(
        filters.REPLY & filters.Chat(RPZ_DISCUSSION_CHAT_ID),
        handle_task_reply
    ))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Chat(RPZ_DISCUSSION_CHAT_ID),
        handle_new_task
    ))

    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(daily_report, 'cron', hour=REPORT_HOUR, minute=REPORT_MINUTE, args=[application])
    scheduler.start()

    logger.info("Бот запущен (Канал + Чат обсуждений с исправленным описанием)...")
    application.run_polling()

if __name__ == "__main__":
    main()