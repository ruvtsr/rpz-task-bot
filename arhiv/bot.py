import os
import re
import logging
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application, MessageHandler, filters, ContextTypes, CallbackContext
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
RPZ_CHANNEL_ID = int(os.getenv("RPZ_CHANNEL_ID"))
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
    if not text.startswith("#З"):
        return None, None
    content = text[2:].lstrip()
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
    lines.append(f"Описание: {description}")
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
# Обработка создания задачи (#З)
# ======================

async def handle_new_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    chat_id = message.chat_id
    thread_id = message.message_thread_id

    if not message or not user or chat_id != RPZ_CHANNEL_ID:
        return
    if not is_allowed_user(user.id):
        return

    text = (message.text or message.caption or "").strip()
    key = (user.id, chat_id, thread_id)

    if text.startswith("#З"):
        if key in pending_tasks:
            await finalize_task_now(context, key)

        topic, desc_part = extract_topic_and_desc(text)
        if topic is None:
            return

        now = get_moscow_time()
        pending_tasks[key] = {
            "user": user,
            "topic": topic,
            "desc_parts": [desc_part] if desc_part else [],
            "msg_ids": [message.message_id],
            "start_time": now,
            "thread_id": thread_id
        }

        context.job_queue.run_once(
            finalize_task_job,
            30,
            data={"key": key},
            name=f"task_timer_{hash(key)}"
        )
        return

    # Если есть активная задача — добавляем к описанию
    if key in pending_tasks:
        pending_tasks[key]["desc_parts"].append(text)
        pending_tasks[key]["msg_ids"].append(message.message_id)
        return

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
    thread_id = data["thread_id"]
    now = get_moscow_time()

    task_id = generate_task_id()

    raw_username = f"@{user.username}" if user.username else None
    display_name = users_mapping.get(raw_username, raw_username or user.full_name)
    author = display_name

    tags = has_sorokin_tag(user.id)
    full_text = topic + "\n" + description
    priority = extract_priority(full_text)

    for msg_id in msg_ids:
        try:
            await context.bot.delete_message(chat_id=RPZ_CHANNEL_ID, message_id=msg_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить {msg_id}: {e}")

    msg = await context.bot.send_message(
        chat_id=RPZ_CHANNEL_ID,
        message_thread_id=thread_id,
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
        str(msg.message_id),
        str(thread_id) if thread_id else "",
        priority
    ]
    SHEET.append_row(row)
    logger.info(f"Задача создана: {task_id} от {author}")

# ======================
# Обработка ОТВЕТОВ на задачи (назначение / закрытие) + УДАЛЕНИЕ
# ======================

async def handle_task_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user

    if not message.reply_to_message or message.chat_id != RPZ_CHANNEL_ID:
        return

    replied_msg_id = message.reply_to_message.message_id
    action_msg_id = message.message_id
    text = (message.text or "").strip()
    if not text:
        return

    # Находим задачу по Msg_ID
    try:
        cell = SHEET.find(str(replied_msg_id), in_column=12)
        if not cell:
            return
        row_idx = cell.row
        current_status = SHEET.cell(row_idx, 8).value
        if current_status == "Выполнено":
            return
    except Exception as e:
        logger.error(f"Ошибка поиска задачи по Msg_ID: {e}")
        return

    processed = False

    # === Назначение
    username_match = re.search(r'@[\w\d_]+', text)
    if username_match:
        executor_handle = username_match.group(0)
        executor_name = users_mapping.get(executor_handle, executor_handle)

        now = get_moscow_time()
        task_id = SHEET.cell(row_idx, 1).value
        author = SHEET.cell(row_idx, 6).value
        topic = SHEET.cell(row_idx, 4).value
        description = SHEET.cell(row_idx, 5).value
        priority = SHEET.cell(row_idx, 14).value
        created = SHEET.cell(row_idx, 2).value + " " + SHEET.cell(row_idx, 3).value

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

        try:
            await context.bot.edit_message_text(
                chat_id=RPZ_CHANNEL_ID,
                message_id=replied_msg_id,
                text=new_text
            )
        except Exception as e:
            logger.warning(f"Не удалось обновить сообщение: {e}")

        await context.bot.send_message(
            chat_id=ENGINEERS_CHANNEL_ID,
            text=f"Задача {task_id} назначена на {executor_name}"
        )
        logger.info(f"Задача {task_id} назначена на {executor_name}")
        processed = True

    # === Закрытие
    elif any(trigger in text.lower() for trigger in ["#выполнено", "готово", "решено"]):
        now = get_moscow_time()
        task_id = SHEET.cell(row_idx, 1).value
        executor = SHEET.cell(row_idx, 7).value or (users_mapping.get(f"@{user.username}", f"@{user.username}") if user.username else user.full_name)
        complete_time_str = now.strftime("%Y-%m-%d %H:%M:%S")

        SHEET.update_cell(row_idx, 8, "Выполнено")
        SHEET.update_cell(row_idx, 10, complete_time_str)

        author = SHEET.cell(row_idx, 6).value
        topic = SHEET.cell(row_idx, 4).value
        description = SHEET.cell(row_idx, 5).value
        priority = SHEET.cell(row_idx, 14).value
        created = SHEET.cell(row_idx, 2).value + " " + SHEET.cell(row_idx, 3).value
        assigned = SHEET.cell(row_idx, 9).value

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
            "executor": executor
        }, f"Выполнил {executor}")

        try:
            await context.bot.edit_message_text(
                chat_id=RPZ_CHANNEL_ID,
                message_id=replied_msg_id,
                text=new_text
            )
        except Exception as e:
            logger.warning(f"Не удалось обновить сообщение: {e}")

        await context.bot.send_message(
            chat_id=ENGINEERS_CHANNEL_ID,
            text=f"Задача №{task_id} — Выполнено {executor}"
        )
        logger.info(f"Задача {task_id} закрыта")
        processed = True

    # 🔥 УДАЛЯЕМ сообщение-действие, если оно было обработано
    if processed:
        try:
            await context.bot.delete_message(
                chat_id=RPZ_CHANNEL_ID,
                message_id=action_msg_id
            )
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение действия {action_msg_id}: {e}")

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
    await context.bot.send_message(chat_id=ENGINEERS_CHANNEL_ID, text=report)

# ======================
# Запуск
# ======================

def main():
    initialize_task_counter()
    load_users_mapping()

    application = Application.builder().token(BOT_TOKEN).build()

    # Обработчики в правильном порядке:
    application.add_handler(MessageHandler(
        filters.REPLY & filters.Chat(RPZ_CHANNEL_ID),
        handle_task_reply  # обрабатывает и удаляет @ и "готово"
    ))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Chat(RPZ_CHANNEL_ID),
        handle_new_task
    ))

    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(daily_report, 'cron', hour=REPORT_HOUR, minute=REPORT_MINUTE, args=[application])
    scheduler.start()

    logger.info("Бот запущен...")
    application.run_polling()

if __name__ == "__main__":
    main()