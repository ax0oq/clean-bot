import os
import json
import io
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
import boto3
from botocore.client import Config

# ========== КОНФИГУРАЦИЯ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ==========
TOKEN = os.environ.get("TOKEN")
ADMINS_STR = os.environ.get("ADMINS", os.environ.get("ADMIN_ID", ""))
ADMINS = [int(x.strip()) for x in ADMINS_STR.split(",") if x.strip()]

# Yandex Cloud Storage
YANDEX_ACCESS_KEY = os.environ.get("YANDEX_ACCESS_KEY")
YANDEX_SECRET_KEY = os.environ.get("YANDEX_SECRET_KEY")
YANDEX_BUCKET_NAME = os.environ.get("YANDEX_BUCKET_NAME", "nogotochki1")
YANDEX_ENDPOINT = os.environ.get("YANDEX_ENDPOINT", "https://storage.yandexcloud.net")

# ========== ИНИЦИАЛИЗАЦИЯ YANDEX STORAGE ==========
session = boto3.session.Session()
s3_client = session.client(
    's3',
    endpoint_url=YANDEX_ENDPOINT,
    aws_access_key_id=YANDEX_ACCESS_KEY,
    aws_secret_access_key=YANDEX_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# ========== ХРАНИЛИЩЕ ДАННЫХ ==========
masters = {}
appointments = {}
next_master_id = 1
next_appointment_id = 1

DATA_FILE = "salon_data.json"

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С YANDEX STORAGE ==========
def load_data_from_yandex():
    global masters, appointments, next_master_id, next_appointment_id
    try:
        response = s3_client.get_object(Bucket=YANDEX_BUCKET_NAME, Key=DATA_FILE)
        data = json.loads(response['Body'].read().decode('utf-8'))
        masters = {int(k): v for k, v in data.get("masters", {}).items()}
        appointments = {int(k): v for k, v in data.get("appointments", {}).items()}
        next_master_id = data.get("next_master_id", 1)
        next_appointment_id = data.get("next_appointment_id", 1)
        print(f"✅ Данные загружены из Yandex Cloud: {len(masters)} мастеров, {len(appointments)} записей")
    except Exception as e:
        print(f"📁 Создаём тестовые данные: {e}")
        masters = {
            1: {"name": "Анна", "services": ["💅 Маникюр", "💅 Педикюр", "✨ Shellac"]},
            2: {"name": "Елена", "services": ["💇‍♀️ Стрижка", "🎨 Окрашивание", "✨ Укладка"]}
        }
        appointments = {}
        next_master_id = 3
        next_appointment_id = 1
        save_data_to_yandex()

def save_data_to_yandex():
    try:
        data = {
            "masters": masters,
            "appointments": appointments,
            "next_master_id": next_master_id,
            "next_appointment_id": next_appointment_id,
            "last_updated": datetime.now().isoformat()
        }
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        json_bytes = io.BytesIO(json_str.encode('utf-8'))
        s3_client.upload_fileobj(json_bytes, YANDEX_BUCKET_NAME, DATA_FILE,
                                  ExtraArgs={'ContentType': 'application/json'})
        print(f"✅ Данные сохранены в Yandex Cloud")
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")

# Загружаем данные
load_data_from_yandex()

# ========== FSM СОСТОЯНИЯ ==========
class AppointmentStates(StatesGroup):
    choosing_master = State()
    choosing_service = State()
    entering_date = State()
    confirming = State()

class MasterManagementStates(StatesGroup):
    entering_name = State()
    entering_services = State()
    renaming_master = State()
    editing_services = State()

# ========== ИНИЦИАЛИЗАЦИЯ БОТА ==========
storage = MemoryStorage()
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=storage)

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def is_admin(user_id: int) -> bool:
    return user_id in ADMINS

def get_main_keyboard(user_id: int):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(KeyboardButton(text="📅 Записаться"))
    keyboard.add(KeyboardButton(text="📋 Мои записи"))
    keyboard.add(KeyboardButton(text="👩‍🎨 Наши мастера"))
    if is_admin(user_id):
        keyboard.add(KeyboardButton(text="⚙️ Админ-панель"))
    return keyboard

def get_masters_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for master_id, master in masters.items():
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=f"👩‍🎨 {master['name']}", callback_data=f"master_{master_id}")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main")])
    return keyboard

def get_admin_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Управление мастерами", callback_data="admin_masters")],
        [InlineKeyboardButton(text="📊 Все записи", callback_data="admin_appointments")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="💾 Сохранить данные", callback_data="admin_save")],
        [InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_main")]
    ])
    return keyboard

def get_admin_masters_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for master_id, master in masters.items():
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=f"✏️ {master['name']}", callback_data=f"edit_master_{master_id}")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="➕ Добавить мастера", callback_data="add_master")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_admin")])
    return keyboard

def get_edit_master_keyboard(master_id):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить имя", callback_data=f"rename_master_{master_id}")],
        [InlineKeyboardButton(text="📋 Изменить услуги", callback_data=f"edit_services_{master_id}")],
        [InlineKeyboardButton(text="🗑 Удалить мастера", callback_data=f"delete_master_{master_id}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_masters")]
    ])
    return keyboard

# ========== ОБРАБОТЧИКИ КОМАНД ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    welcome_text = (
        "✨ Добро пожаловать в студию ногтевого сервиса! ✨\n\n"
        "Я помогу вам:\n"
        "• Записаться к мастеру\n"
        "• Посмотреть свои записи\n"
        "• Узнать информацию о мастерах\n\n"
        "Используйте кнопки ниже для навигации 👇"
    )
    await message.reply(welcome_text, reply_markup=get_main_keyboard(message.from_user.id))

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    help_text = (
        "📖 Доступные команды:\n"
        "/start - начать работу\n"
        "/help - эта справка\n"
        "/masters - список мастеров\n"
        "/my_appointments - мои записи\n"
        "/cancel - отменить текущее действие"
    )
    await message.reply(help_text)

@dp.message(Command("masters"))
async def cmd_masters(message: types.Message):
    if not masters:
        await message.reply("😔 Список мастеров пока пуст. Скоро они появятся!")
        return
    text = "👩‍🎨 Наши мастера:\n\n"
    for master_id, master in masters.items():
        services = ", ".join(master.get("services", []))
        text += f"• {master['name']}\n   💇 Услуги: {services}\n\n"
    await message.reply(text)

@dp.message(Command("my_appointments"))
async def cmd_my_appointments(message: types.Message):
    user_id = message.from_user.id
    user_appointments = {k: v for k, v in appointments.items() if v["user_id"] == user_id}
    if not user_appointments:
        await message.reply("📭 У вас нет записей")
        return
    text = "📋 Ваши записи:\n\n"
    for app_id, app in user_appointments.items():
        master_name = masters.get(app["master_id"], {}).get("name", "Неизвестно")
        status_emoji = "✅" if app.get("status") == "confirmed" else "⏳"
        text += f"{status_emoji} #{app_id}\n👤 Мастер: {master_name}\n💇 Услуга: {app['service']}\n📅 Дата: {app['date']}\n\n"
    await message.reply(text)

@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.reply("🤔 Нет активного действия для отмены")
        return
    await state.clear()
    await message.reply("✅ Действие отменено", reply_markup=get_main_keyboard(message.from_user.id))

# ========== ОСНОВНЫЕ КНОПКИ ==========
@dp.message(lambda message: message.text == "📅 Записаться")
async def appointment_start(message: types.Message, state: FSMContext):
    if not masters:
        await message.reply("😔 К сожалению, сейчас нет доступных мастеров. Попробуйте позже!")
        return
    await state.set_state(AppointmentStates.choosing_master)
    await message.reply("Выберите мастера:", reply_markup=get_masters_keyboard())

@dp.message(lambda message: message.text == "📋 Мои записи")
async def my_appointments_button(message: types.Message):
    await cmd_my_appointments(message)

@dp.message(lambda message: message.text == "👩‍🎨 Наши мастера")
async def masters_button(message: types.Message):
    await cmd_masters(message)

@dp.message(lambda message: message.text == "⚙️ Админ-панель")
async def admin_panel(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("⛔ У вас нет доступа к админ-панели")
        return
    await message.reply("⚙️ Админ-панель:", reply_markup=get_admin_keyboard())

# ========== INLINE CALLBACK ==========
@dp.callback_query(lambda c: c.data.startswith("master_"))
async def process_master_selection(callback: types.CallbackQuery, state: FSMContext):
    master_id = int(callback.data.split("_")[1])
    master = masters.get(master_id)
    
    if not master:
        await callback.message.edit_text("❌ Мастер не найден")
        return
    
    await state.update_data(master_id=master_id, master_name=master['name'])
    
    services = master.get("services", [])
    if services:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[])
        for service in services:
            keyboard.inline_keyboard.append([InlineKeyboardButton(text=service, callback_data=f"service_{master_id}_{service}")])
        keyboard.inline_keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_masters")])
        await state.set_state(AppointmentStates.choosing_service)
        await callback.message.edit_text(f"👩‍🎨 Мастер: {master['name']}\n\nВыберите услугу:", reply_markup=keyboard)
    else:
        await state.set_state(AppointmentStates.entering_date)
        await callback.message.edit_text(
            f"👩‍🎨 Мастер: {master['name']}\n\n"
            f"Введите желаемую дату и время в формате:\n"
            f"ДД.ММ.ГГГГ ЧЧ:ММ\n\n"
            f"Пример: 25.04.2025 15:30"
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("service_"))
async def process_service_selection(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    service = "_".join(parts[2:])
    
    await state.update_data(service=service)
    await state.set_state(AppointmentStates.entering_date)
    
    await callback.message.edit_text(
        f"✅ Вы выбрали услугу: {service}\n\n"
        f"Теперь введите желаемую дату и время в формате:\n"
        f"ДД.ММ.ГГГГ ЧЧ:ММ\n\n"
        f"Пример: 25.04.2025 15:30"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_masters")
async def back_to_masters(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AppointmentStates.choosing_master)
    await callback.message.edit_text("Выберите мастера:", reply_markup=get_masters_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("Главное меню:", reply_markup=get_main_keyboard(callback.from_user.id))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_admin")
async def back_to_admin(callback: types.CallbackQuery):
    await callback.message.edit_text("⚙️ Админ-панель:", reply_markup=get_admin_keyboard())
    await callback.answer()

# ========== АДМИН-КОЛБЭКИ ==========
@dp.callback_query(lambda c: c.data == "admin_masters")
async def admin_masters_menu(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await callback.message.edit_text("👥 Управление мастерами:", reply_markup=get_admin_masters_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_appointments")
async def admin_all_appointments(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    if not appointments:
        await callback.message.edit_text("📭 Нет ни одной записи")
        return
    text = "📊 ВСЕ ЗАПИСИ:\n\n"
    for app_id, app in appointments.items():
        master_name = masters.get(app["master_id"], {}).get("name", "Неизвестно")
        status_emoji = "✅" if app.get("status") == "confirmed" else "⏳"
        text += f"{status_emoji} #{app_id}\n👤 Клиент: {app['user_name']}\n👩‍🎨 Мастер: {master_name}\n💇 Услуга: {app['service']}\n📅 Дата: {app['date']}\n➖➖➖➖➖\n"
    if len(text) > 4000:
        text = text[:4000] + "..."
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    total_appointments = len(appointments)
    confirmed = len([a for a in appointments.values() if a.get("status") == "confirmed"])
    pending = len([a for a in appointments.values() if a.get("status") == "pending"])
    text = (
        "📈 СТАТИСТИКА:\n\n"
        f"👥 Всего мастеров: {len(masters)}\n"
        f"📅 Всего записей: {total_appointments}\n"
        f"✅ Подтверждено: {confirmed}\n"
        f"⏳ Ожидает: {pending}\n"
    )
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_save")
async def admin_save_data(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    if save_data_to_yandex():
        await callback.message.edit_text("✅ Данные успешно сохранены в Yandex Cloud!")
    else:
        await callback.message.edit_text("❌ Ошибка сохранения в Yandex Cloud")
    await callback.answer()

# ========== УПРАВЛЕНИЕ МАСТЕРАМИ ==========
@dp.callback_query(lambda c: c.data == "add_master")
async def add_master_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(MasterManagementStates.entering_name)
    await callback.message.edit_text("➕ Добавление нового мастера\n\nВведите имя мастера:")
    await callback.answer()

@dp.message(MasterManagementStates.entering_name)
async def add_master_name(message: types.Message, state: FSMContext):
    global next_master_id
    name = message.text.strip()
    if len(name) < 2:
        await message.reply("❌ Имя слишком короткое. Попробуйте еще раз:")
        return
    await state.update_data(master_name=name)
    await state.set_state(MasterManagementStates.entering_services)
    await message.reply(f"✅ Имя мастера: {name}\n\nТеперь введите список услуг через запятую\nПример: маникюр, педикюр, shellac")

@dp.message(MasterManagementStates.entering_services)
async def add_master_services(message: types.Message, state: FSMContext):
    global next_master_id
    services_text = message.text.strip()
    services = [s.strip() for s in services_text.split(",")]
    data = await state.get_data()
    master_name = data.get("master_name")
    masters[next_master_id] = {"name": master_name, "services": services}
    await message.reply(f"✅ Мастер {master_name} добавлен!\n📋 Услуги: {', '.join(services)}\n\nID мастера: {next_master_id}")
    next_master_id += 1
    save_data_to_yandex()
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("edit_master_"))
async def edit_master_menu(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    master_id = int(callback.data.split("_")[2])
    master = masters.get(master_id)
    if not master:
        await callback.message.edit_text("❌ Мастер не найден")
        return
    await callback.message.edit_text(
        f"👩‍🎨 Мастер: {master['name']}\n📋 Услуги: {', '.join(master.get('services', []))}\n\nЧто хотите сделать?",
        reply_markup=get_edit_master_keyboard(master_id)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("rename_master_"))
async def rename_master_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    master_id = int(callback.data.split("_")[2])
    await state.update_data(edit_master_id=master_id)
    await state.set_state(MasterManagementStates.renaming_master)
    await callback.message.edit_text("✏️ Введите новое имя для мастера:")
    await callback.answer()

@dp.message(MasterManagementStates.renaming_master)
async def rename_master(message: types.Message, state: FSMContext):
    data = await state.get_data()
    master_id = data.get("edit_master_id")
    new_name = message.text.strip()
    if master_id in masters:
        old_name = masters[master_id]["name"]
        masters[master_id]["name"] = new_name
        save_data_to_yandex()
        await message.reply(f"✅ Имя мастера изменено с '{old_name}' на '{new_name}'")
    else:
        await message.reply("❌ Мастер не найден")
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("edit_services_"))
async def edit_services_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    master_id = int(callback.data.split("_")[2])
    await state.update_data(edit_master_id=master_id)
    await state.set_state(MasterManagementStates.editing_services)
    await callback.message.edit_text("✏️ Введите новый список услуг через запятую\nПример: маникюр, педикюр, shellac, дизайн")
    await callback.answer()

@dp.message(MasterManagementStates.editing_services)
async def edit_services(message: types.Message, state: FSMContext):
    data = await state.get_data()
    master_id = data.get("edit_master_id")
    services_text = message.text.strip()
    services = [s.strip() for s in services_text.split(",")]
    if master_id in masters:
        masters[master_id]["services"] = services
        save_data_to_yandex()
        await message.reply(f"✅ Услуги обновлены: {', '.join(services)}")
    else:
        await message.reply("❌ Мастер не найден")
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("delete_master_"))
async def delete_master(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    master_id = int(callback.data.split("_")[2])
    master_name = masters.get(master_id, {}).get("name", "Неизвестно")
    if master_id in masters:
        del masters[master_id]
        save_data_to_yandex()
        await callback.message.edit_text(f"✅ Мастер '{master_name}' удален")
    else:
        await callback.message.edit_text("❌ Мастер не найден")
    await callback.answer()

# ========== ОБРАБОТКА ДАТЫ ==========
@dp.message(AppointmentStates.entering_date)
async def process_date(message: types.Message, state: FSMContext):
    text = message.text.strip()
    try:
        if " " in text:
            date_str, time_str = text.split()
            day, month, year = map(int, date_str.split('.'))
            hour, minute = map(int, time_str.split(':'))
            dt = datetime(year, month, day, hour, minute)
            if dt < datetime.now():
                await message.reply("❌ Нельзя записаться в прошлое! Введите будущую дату:")
                return
            await state.update_data(appointment_date=text)
            data = await state.get_data()
            confirm_text = (
                "📝 Проверьте данные записи:\n\n"
                f"👩‍🎨 Мастер: {data.get('master_name')}\n"
                f"💇 Услуга: {data.get('service', 'Не указана')}\n"
                f"📅 Дата и время: {text}\n\n"
                f"✅ Всё верно?\n"
                f"Напишите 'да' для подтверждения или 'нет' для отмены"
            )
            await state.set_state(AppointmentStates.confirming)
            await message.reply(confirm_text)
        else:
            await message.reply("❌ Неверный формат. Используйте: ДД.ММ.ГГГГ ЧЧ:ММ\nПример: 25.04.2025 15:30")
    except Exception as e:
        await message.reply(f"❌ Ошибка в формате даты. Используйте: ДД.ММ.ГГГГ ЧЧ:ММ\nПример: 25.04.2025 15:30")

@dp.message(AppointmentStates.confirming)
async def confirm_appointment(message: types.Message, state: FSMContext):
    global next_appointment_id
    if message.text.lower() == "да":
        data = await state.get_data()
        appointment = {
            "user_id": message.from_user.id,
            "user_name": message.from_user.full_name,
            "master_id": data.get("master_id"),
            "master_name": data.get("master_name"),
            "service": data.get("service", "Не указана"),
            "date": data.get("appointment_date"),
            "status": "pending",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        appointments[next_appointment_id] = appointment
        save_data_to_yandex()
        for admin_id in ADMINS:
            try:
                await bot.send_message(admin_id, f"🔔 НОВАЯ ЗАПИСЬ!\n\n👤 Клиент: {message.from_user.full_name}\n👩‍🎨 Мастер: {data.get('master_name')}\n💇 Услуга: {appointment['service']}\n📅 Дата: {data.get('appointment_date')}\n🆔 Номер записи: #{next_appointment_id}")
            except:
                pass
        await message.reply(f"✅ Запись создана!\n\nНомер записи: #{next_appointment_id}\nМастер свяжется с вами для подтверждения.\n\nСпасибо, что выбрали нас! 💅", reply_markup=get_main_keyboard(message.from_user.id))
        next_appointment_id += 1
        await state.clear()
    elif message.text.lower() == "нет":
        await state.clear()
        await message.reply("❌ Запись отменена. Можете начать заново через кнопку 📅 Записаться", reply_markup=get_main_keyboard(message.from_user.id))
    else:
        await message.reply("Пожалуйста, ответьте 'да' или 'нет'")

# ========== ЗАПУСК ==========
async def main():
    print(f"🤖 Бот запущен!")
    print(f"👑 Админы: {ADMINS if ADMINS else 'Не заданы!'}")
    print(f"👩‍🎨 Мастеров в базе: {len(masters)}")
    print(f"📅 Записей: {len(appointments)}")
    print(f"☁️ Yandex Cloud Storage: {YANDEX_BUCKET_NAME}")
    await dp.start_polling(bot)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
