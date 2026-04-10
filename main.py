import asyncio
import sqlite3
import json
import uuid
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, BaseFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import boto3
from botocore.client import Config

# =========== НАСТРОЙКИ ===========
TOKEN = ""
ADMIN_ID = 0

YANDEX_ACCESS_KEY = ""
YANDEX_SECRET_KEY = ""
YANDEX_BUCKET_NAME = "nogotochki1"
YANDEX_ENDPOINT = "https://storage.yandexcloud.net"

bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler()

# Инициализация S3 клиента для Yandex Object Storage
s3_client = boto3.client(
    's3',
    endpoint_url=YANDEX_ENDPOINT,
    aws_access_key_id=YANDEX_ACCESS_KEY,
    aws_secret_access_key=YANDEX_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# =========== БАЗА ДАННЫХ ===========
conn = sqlite3.connect('salon.db')
cursor = conn.cursor()

# Таблица записей клиентов
cursor.execute('''
    CREATE TABLE IF NOT EXISTS appointments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        username TEXT,
        phone TEXT,
        service TEXT,
        master_id INTEGER DEFAULT 1,
        datetime TEXT,
        status TEXT,
        file_url TEXT
    )
''')

# Таблица расписания
cursor.execute('''
    CREATE TABLE IF NOT EXISTS work_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        time TEXT,
        master_id INTEGER DEFAULT 1,
        is_available INTEGER DEFAULT 1,
        UNIQUE(date, time, master_id)
    )
''')

# Уникальный индекс для защиты от двойных записей
cursor.execute('''
    CREATE UNIQUE INDEX IF NOT EXISTS unique_booking 
    ON appointments (master_id, datetime) WHERE status = 'active'
''')

conn.commit()


# =========== СОСТОЯНИЯ ===========
class Booking(StatesGroup):
    service = State()
    date = State()
    time = State()
    phone = State()


class AdminSchedule(StatesGroup):
    action = State()
    date = State()
    time = State()


# =========== ФИЛЬТР АДМИНА ===========
class IsAdmin(BaseFilter):
    def __init__(self, admin_ids: list[int]) -> None:
        self.admin_ids = admin_ids

    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id in self.admin_ids


# =========== КЛАВИАТУРЫ ===========
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📅 Записаться на маникюр")],
        [KeyboardButton(text="❌ Отменить мою запись")],
        [KeyboardButton(text="📋 Мои записи")]
    ],
    resize_keyboard=True
)

admin_kb = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="📅 Показать расписание", callback_data="show")],
    [InlineKeyboardButton(text="➕ Добавить слот", callback_data="add")],
    [InlineKeyboardButton(text="❌ Удалить слот", callback_data="del")],
    [InlineKeyboardButton(text="📊 Экспорт в JSON", callback_data="export")]
])

services_kb = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="💅 Маникюр + покрытие", callback_data="service_manicure")],
    [InlineKeyboardButton(text="✂️ Коррекция + покрытие", callback_data="service_correction")],
    [InlineKeyboardButton(text="🖐 Педикюр", callback_data="service_pedicure")],
    [InlineKeyboardButton(text="💪 Снятие гель-лака", callback_data="service_remove")]
])


# =========== РАБОТА С YANDEX OBJECT STORAGE ===========
def upload_to_yandex_storage(data: dict, filename: str = None) -> str:
    """
    Загружает данные в Yandex Object Storage и возвращает публичную ссылку
    """
    if filename is None:
        filename = f"booking_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"

    try:
        # Загружаем файл в бакет
        s3_client.put_object(
            Bucket=YANDEX_BUCKET_NAME,
            Key=f"bookings/{filename}",
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )

        # Формируем публичную ссылку
        file_url = f"https://storage.yandexcloud.net/{YANDEX_BUCKET_NAME}/bookings/{filename}"
        print(f"✅ Данные загружены в Object Storage: {file_url}")
        return file_url
    except Exception as e:
        print(f"❌ Ошибка загрузки в Object Storage: {e}")
        return None


def export_all_bookings_to_json():
    """
    Экспортирует все записи из БД в JSON и загружает в Object Storage
    """
    cursor.execute('''
        SELECT id, user_id, username, phone, service, datetime, status 
        FROM appointments 
        ORDER BY datetime DESC
    ''')
    rows = cursor.fetchall()

    bookings = []
    for row in rows:
        bookings.append({
            "id": row[0],
            "user_id": row[1],
            "username": row[2],
            "phone": row[3],
            "service": row[4],
            "datetime": row[5],
            "status": row[6]
        })

    filename = f"all_bookings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    return upload_to_yandex_storage({"bookings": bookings, "export_date": datetime.now().isoformat()}, filename)


# =========== РАБОТА С БАЗОЙ ДАННЫХ ===========
def add_work_slot(date: str, time: str, master_id: int = 1):
    """Добавить доступный слот в расписание"""
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO work_schedule (date, time, master_id, is_available)
            VALUES (?, ?, ?, 1)
        ''', (date, time, master_id))
        conn.commit()
        return True
    except:
        return False


def remove_work_slot(date: str, time: str, master_id: int = 1):
    """Удалить слот из расписания"""
    cursor.execute('''
        DELETE FROM work_schedule 
        WHERE date = ? AND time = ? AND master_id = ?
    ''', (date, time, master_id))
    conn.commit()
    return cursor.rowcount > 0


def get_available_slots(date: str, master_id: int = 1):
    """Получить все свободные слоты на дату"""
    cursor.execute('''
        SELECT time FROM work_schedule 
        WHERE date = ? AND master_id = ? AND is_available = 1
    ''', (date, master_id))
    scheduled = [row[0] for row in cursor.fetchall()]

    available = []
    for slot_time in scheduled:
        cursor.execute('''
            SELECT COUNT(*) FROM appointments 
            WHERE master_id = ? AND datetime = ? AND status = 'active'
        ''', (master_id, f"{date} {slot_time}:00"))
        count = cursor.fetchone()[0]
        if count == 0:
            available.append(slot_time)
    return available


def get_all_work_dates():
    """Получить все даты, на которые есть слоты"""
    cursor.execute('SELECT DISTINCT date FROM work_schedule ORDER BY date')
    return [row[0] for row in cursor.fetchall()]


def get_slots_by_date(date: str):
    """Получить все слоты на конкретную дату"""
    cursor.execute('SELECT time FROM work_schedule WHERE date = ? ORDER BY time', (date,))
    return [row[0] for row in cursor.fetchall()]


def save_appointment(user_id, username, phone, service, datetime_str, master_id=1):
    """Сохранить запись клиента с отправкой в Object Storage"""
    # Проверяем, не занято ли уже это время
    cursor.execute('''
        SELECT COUNT(*) FROM appointments 
        WHERE master_id = ? AND datetime = ? AND status = 'active'
    ''', (master_id, datetime_str))
    if cursor.fetchone()[0] > 0:
        return None

    cursor.execute('''
        INSERT INTO appointments (user_id, username, phone, service, datetime, status, master_id)
        VALUES (?, ?, ?, ?, ?, 'active', ?)
    ''', (user_id, username, phone, service, datetime_str, master_id))
    conn.commit()
    app_id = cursor.lastrowid

    # Отправляем данные в Object Storage
    booking_data = {
        "appointment_id": app_id,
        "user_id": user_id,
        "username": username,
        "phone": phone,
        "service": service,
        "datetime": datetime_str,
        "status": "active",
        "created_at": datetime.now().isoformat()
    }
    file_url = upload_to_yandex_storage(booking_data)

    # Обновляем запись с ссылкой на файл
    if file_url:
        cursor.execute('UPDATE appointments SET file_url = ? WHERE id = ?', (file_url, app_id))
        conn.commit()

    return app_id


def cancel_appointment(user_id):
    cursor.execute('''
        UPDATE appointments SET status = 'cancelled' 
        WHERE user_id = ? AND status = 'active' AND datetime > datetime('now')
    ''', (user_id,))
    conn.commit()
    return cursor.rowcount > 0


def get_active_appointments(user_id):
    cursor.execute('''
        SELECT id, service, datetime FROM appointments 
        WHERE user_id = ? AND status = 'active' AND datetime > datetime('now')
    ''', (user_id,))
    return cursor.fetchall()


def get_all_future_appointments():
    cursor.execute('''
        SELECT id, user_id, datetime, service FROM appointments 
        WHERE status = 'active' AND datetime > datetime('now')
    ''')
    return cursor.fetchall()


# =========== НАПОМИНАНИЯ ===========
async def send_reminder(appointment_id, user_id, datetime_str, service):
    """Отправить напоминание за 2 часа до записи"""
    dt_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    dt_readable = dt_obj.strftime("%d.%m.%Y в %H:%M")

    text = (f"⏰ НАПОМИНАНИЕ\n\n"
            f"Вы записаны на {service}\n"
            f"🗓 Дата и время: {dt_readable}\n\n"
            f"Пожалуйста, не опаздывайте. Если вы передумали, нажмите кнопку ❌ Отменить мою запись в главном меню.")
    try:
        await bot.send_message(chat_id=user_id, text=text)
    except:
        pass


def schedule_reminders():
    """Запланировать все напоминания при запуске бота"""
    appointments = get_all_future_appointments()
    for app in appointments:
        app_id, user_id, dt_str, service = app
        app_datetime = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        remind_time = app_datetime - timedelta(hours=2)
        now = datetime.now()
        if remind_time > now:
            scheduler.add_job(
                send_reminder,
                'date',
                run_date=remind_time,
                args=[app_id, user_id, dt_str, service],
                id=f"remind_{app_id}",
                replace_existing=True
            )


# =========== КЛАВИАТУРЫ ДЛЯ ДАТ ===========
def get_dates_kb():
    """Показать только те даты, на которых есть свободные слоты"""
    dates = get_all_work_dates()
    if not dates:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Нет доступных дат", callback_data="no_dates")]
        ])
        return kb

    kb = []
    for date in dates:
        available = get_available_slots(date)
        if available:
            try:
                date_obj = datetime.strptime(date, "%Y-%m-%d")
                display_date = date_obj.strftime("%d.%m (%a)")
            except:
                display_date = date
            kb.append([InlineKeyboardButton(text=display_date, callback_data=f"date_{date}")])

    if not kb:
        kb = [[InlineKeyboardButton(text="❌ Нет свободных слотов", callback_data="no_dates")]]

    return InlineKeyboardMarkup(inline_keyboard=kb)


def get_times_kb(date: str):
    """Показать только свободное время на выбранную дату"""
    available_times = get_available_slots(date)
    if not available_times:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Нет свободного времени", callback_data="no_times")]
        ])
        return kb

    kb = []
    for t in available_times:
        kb.append([InlineKeyboardButton(text=t, callback_data=f"time_{t}")])
    kb.append([InlineKeyboardButton(text="◀️ Назад к датам", callback_data="back_to_dates")])

    return InlineKeyboardMarkup(inline_keyboard=kb)


# =========== ХЕНДЛЕРЫ ===========
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "✨ Добро пожаловать в маникюрный салон!\n\n"
        "Я помогу записаться и напомню о визите.\n"
        "Используйте кнопки ниже:",
        reply_markup=main_kb
    )


@dp.message(F.text == "📅 Записаться на маникюр")
async def start_booking(message: types.Message, state: FSMContext):
    await message.answer("Выберите услугу:", reply_markup=services_kb)
    await state.set_state(Booking.service)


@dp.callback_query(F.data.startswith("service_"))
async def choose_service(callback: types.CallbackQuery, state: FSMContext):
    service_map = {
        "service_manicure": "Маникюр + покрытие",
        "service_correction": "Коррекция + покрытие",
        "service_pedicure": "Педикюр",
        "service_remove": "Снятие гель-лака"
    }
    service = service_map.get(callback.data)
    await state.update_data(service=service)

    dates_kb = get_dates_kb()
    await callback.message.edit_text("Выберите удобную ДАТУ:", reply_markup=dates_kb)
    await state.set_state(Booking.date)
    await callback.answer()


@dp.callback_query(F.data.startswith("date_"), Booking.date)
async def choose_date(callback: types.CallbackQuery, state: FSMContext):
    date = callback.data.split("_")[1]
    await state.update_data(date=date)

    times_kb = get_times_kb(date)
    await callback.message.edit_text("Выберите ВРЕМЯ:", reply_markup=times_kb)
    await state.set_state(Booking.time)
    await callback.answer()


@dp.callback_query(F.data == "back_to_dates", Booking.time)
async def back_to_dates(callback: types.CallbackQuery, state: FSMContext):
    dates_kb = get_dates_kb()
    await callback.message.edit_text("Выберите удобную ДАТУ:", reply_markup=dates_kb)
    await state.set_state(Booking.date)
    await callback.answer()


@dp.callback_query(F.data.startswith("time_"), Booking.time)
async def choose_time(callback: types.CallbackQuery, state: FSMContext):
    time = callback.data.split("_")[1]
    await state.update_data(time=time)

    data = await state.get_data()
    datetime_str = f"{data['date']} {time}:00"

    cursor.execute('''
        SELECT COUNT(*) FROM appointments 
        WHERE datetime = ? AND status = 'active'
    ''', (datetime_str,))
    if cursor.fetchone()[0] > 0:
        await callback.message.edit_text(
            "❌ К сожалению, это время уже заняли.\n"
            "Пожалуйста, выберите другое время:",
            reply_markup=get_times_kb(data['date'])
        )
        await callback.answer()
        return

    await state.update_data(datetime_str=datetime_str)

    await callback.message.edit_text(
        f"📞 Отлично! Остался последний шаг.\n"
        f"Напишите ваш номер телефона (например, +79991234567), чтобы мы могли связаться при отмене."
    )
    await state.set_state(Booking.phone)
    await callback.answer()


@dp.message(Booking.phone)
async def save_phone(message: types.Message, state: FSMContext):
    phone = message.text
    data = await state.get_data()

    app_id = save_appointment(
        user_id=message.from_user.id,
        username=message.from_user.username or message.from_user.full_name,
        phone=phone,
        service=data['service'],
        datetime_str=data['datetime_str']
    )

    if app_id is None:
        await message.answer(
            "❌ К сожалению, это время только что заняли.\n"
            "Пожалуйста, начните запись заново.",
            reply_markup=main_kb
        )
        await state.clear()
        return

    dt_obj = datetime.strptime(data['datetime_str'], "%Y-%m-%d %H:%M:%S")
    dt_readable = dt_obj.strftime("%d.%m.%Y в %H:%M")

    await message.answer(
        f"✅ Вы успешно записаны!\n\n"
        f"Услуга: {data['service']}\n"
        f"Дата и время: {dt_readable}\n"
        f"Телефон: {phone}\n\n"
        f"Данные о записи сохранены в Yandex Cloud ☁️\n"
        f"Я напомню вам о визите за 2 часа.\n"
        f"Если передумаете — нажмите ❌ Отменить мою запись.",
        reply_markup=main_kb
    )

    remind_time = dt_obj - timedelta(hours=2)
    if remind_time > datetime.now():
        scheduler.add_job(
            send_reminder,
            'date',
            run_date=remind_time,
            args=[app_id, message.from_user.id, data['datetime_str'], data['service']],
            id=f"remind_{app_id}",
            replace_existing=True
        )

    await state.clear()


@dp.message(F.text == "❌ Отменить мою запись")
async def cancel_booking(message: types.Message):
    if cancel_appointment(message.from_user.id):
        await message.answer(
            "❌ Ваша ближайшая запись успешно отменена.\n\nЕсли хотите — можете записаться снова через кнопку 📅",
            reply_markup=main_kb)
    else:
        await message.answer("🔍 У вас нет активных записей в будущем.", reply_markup=main_kb)


@dp.message(F.text == "📋 Мои записи")
async def show_my_bookings(message: types.Message):
    apps = get_active_appointments(message.from_user.id)
    if not apps:
        await message.answer("У вас пока нет активных записей.", reply_markup=main_kb)
        return

    text = "📋 Ваши активные записи:\n\n"
    for app in apps:
        dt_obj = datetime.strptime(app[2], "%Y-%m-%d %H:%M:%S")
        text += f"🔹 {app[1]}\n   🗓 {dt_obj.strftime('%d.%m.%Y в %H:%M')}\n\n"
    text += "Чтобы отменить — нажмите ❌ Отменить мою запись"
    await message.answer(text, reply_markup=main_kb)


# =========== АДМИН ПАНЕЛЬ ===========
@dp.message(Command("admin"))
async def admin_panel(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет доступа к этой команде.")
        return
    await message.answer("⚙️ Панель администратора\n\nВыберите действие:", reply_markup=admin_kb)
    await state.set_state(AdminSchedule.action)


@dp.callback_query(AdminSchedule.action, F.data.in_(["show", "add", "del", "export"]))
async def admin_action(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    action = callback.data
    await state.update_data(action=action)

    if action == "show":
        dates = get_all_work_dates()
        if not dates:
            await callback.message.edit_text("📭 Расписание пусто. Используйте '➕ Добавить слот'")
        else:
            text = "📅 ТЕКУЩЕЕ РАСПИСАНИЕ:\n\n"
            for date in dates:
                slots = get_slots_by_date(date)
                text += f"📆 {date}: {', '.join(slots)}\n"
            await callback.message.edit_text(text, reply_markup=admin_kb)

    elif action == "export":
        url = export_all_bookings_to_json()
        if url:
            await callback.message.edit_text(
                f"✅ Данные экспортированы в Yandex Object Storage!\n\n"
                f"📁 Ссылка на файл: {url}\n\n"
                f"Эту ссылку можно подключить к Yandex DataLens для визуализации.",
                reply_markup=admin_kb
            )
        else:
            await callback.message.edit_text(
                "❌ Ошибка экспорта. Проверьте настройки Object Storage.",
                reply_markup=admin_kb
            )

    elif action == "add":
        await callback.message.edit_text("Введите ДАТУ в формате ГГГГ-ММ-ДД (например, 2025-12-25):")
        await state.set_state(AdminSchedule.date)

    elif action == "del":
        dates = get_all_work_dates()
        if not dates:
            await callback.message.edit_text("📭 Расписание пусто, нечего удалять.", reply_markup=admin_kb)
        else:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                                                          [InlineKeyboardButton(text=d, callback_data=f"deldate_{d}")]
                                                          for d in dates
                                                      ] + [[InlineKeyboardButton(text="◀️ Назад",
                                                                                 callback_data="back_to_admin")]])
            await callback.message.edit_text("Выберите ДАТУ для удаления:", reply_markup=kb)
            await state.set_state(AdminSchedule.date)

    await callback.answer()


@dp.message(AdminSchedule.date)
async def admin_add_date(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return

    date = message.text.strip()
    data = await state.get_data()

    if data['action'] == "add":
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except:
            await message.answer("❌ Неверный формат. Введите дату как ГГГГ-ММ-ДД (например, 2025-12-25):")
            return

        await state.update_data(date=date)
        await message.answer("Введите ВРЕМЯ в формате ЧЧ:ММ (например, 14:30):")
        await state.set_state(AdminSchedule.time)

    elif data['action'] == "del":
        cursor.execute('DELETE FROM work_schedule WHERE date = ?', (date,))
        conn.commit()
        await message.answer(f"✅ Все слоты на {date} удалены.", reply_markup=admin_kb)
        await state.clear()


@dp.message(AdminSchedule.time)
async def admin_add_time(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return

    time = message.text.strip()
    data = await state.get_data()
    date = data['date']

    try:
        datetime.strptime(time, "%H:%M")
    except:
        await message.answer("❌ Неверный формат. Введите время как ЧЧ:ММ (например, 14:30):")
        return

    if add_work_slot(date, time):
        await message.answer(
            f"✅ Слот {date} {time} добавлен в расписание.\n\nДобавить еще? (введите время или /admin для выхода)")
        await state.set_state(AdminSchedule.time)
    else:
        await message.answer("❌ Ошибка при добавлении (возможно, такой слот уже существует)")


@dp.callback_query(F.data.startswith("deldate_"))
async def admin_delete_date(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа")
        return

    date = callback.data.split("_")[1]
    cursor.execute('DELETE FROM work_schedule WHERE date = ?', (date,))
    conn.commit()
    await callback.message.edit_text(f"✅ Все слоты на {date} удалены.", reply_markup=admin_kb)
    await state.clear()
    await callback.answer()


@dp.callback_query(F.data == "back_to_admin")
async def back_to_admin(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет доступа")
        return
    await callback.message.edit_text("⚙️ Панель администратора\n\nВыберите действие:", reply_markup=admin_kb)
    await state.set_state(AdminSchedule.action)
    await callback.answer()


# =========== ЗАПУСК ===========
async def main():
    scheduler.start()
    schedule_reminders()
    print("✅ Бот запущен! Данные будут сохраняться в Yandex Object Storage.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())