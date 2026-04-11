import os
import json
import io
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils import executor
import boto3
from botocore.client import Config
from aiohttp import web
import asyncio

# ========== КОНФИГУРАЦИЯ ==========
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("TOKEN не задан")

# Секретный код для админ-доступа (задайте свой!)
ADMIN_SECRET_CODE = "admin123"   # <--- Смените на свой секретный код

# Yandex Cloud
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

# ========== ГЛОБАЛЬНЫЕ ДАННЫЕ ==========
masters = {}
appointments = {}
next_master_id = 1
next_appointment_id = 1
DATA_FILE = "salon_data.json"

# ========== РАБОТА С YANDEX ==========
def load_data():
    global masters, appointments, next_master_id, next_appointment_id
    try:
        response = s3_client.get_object(Bucket=YANDEX_BUCKET_NAME, Key=DATA_FILE)
        data = json.loads(response['Body'].read().decode('utf-8'))
        masters = {int(k): v for k, v in data.get("masters", {}).items()}
        appointments = {int(k): v for k, v in data.get("appointments", {}).items()}
        next_master_id = data.get("next_master_id", 1)
        next_appointment_id = data.get("next_appointment_id", 1)
        print(f"✅ Загружено: {len(masters)} мастеров, {len(appointments)} записей")
    except:
        masters = {
            1: {"name": "Анна", "services": ["💅 Маникюр", "💅 Педикюр", "✨ Shellac"]},
            2: {"name": "Елена", "services": ["💇‍♀️ Стрижка", "🎨 Окрашивание", "✨ Укладка"]}
        }
        appointments = {}
        next_master_id = 3
        next_appointment_id = 1
        save_data()

def save_data():
    try:
        data = {
            "masters": masters,
            "appointments": appointments,
            "next_master_id": next_master_id,
            "next_appointment_id": next_appointment_id,
            "last_updated": datetime.now().isoformat()
        }
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        s3_client.upload_fileobj(io.BytesIO(json_str.encode()), YANDEX_BUCKET_NAME, DATA_FILE,
                                 ExtraArgs={'ContentType': 'application/json'})
        print("✅ Сохранено в Yandex Cloud")
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")

load_data()

# ========== FSM ==========
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
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

# Хранилище для сессий админа
admin_sessions = set()

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard(is_admin=False):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("📅 Записаться"))
    kb.add(KeyboardButton("📋 Мои записи"))
    kb.add(KeyboardButton("👩‍🎨 Наши мастера"))
    if is_admin:
        kb.add(KeyboardButton("⚙️ Админ-панель"))
    return kb

def get_masters_keyboard():
    kb = InlineKeyboardMarkup(row_width=1)
    for mid, m in masters.items():
        kb.add(InlineKeyboardButton(f"👩‍🎨 {m['name']}", callback_data=f"master_{mid}"))
    kb.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
    return kb

def get_admin_keyboard():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("👥 Управление мастерами", callback_data="admin_masters"))
    kb.add(InlineKeyboardButton("📊 Все записи", callback_data="admin_appointments"))
    kb.add(InlineKeyboardButton("📈 Статистика", callback_data="admin_stats"))
    kb.add(InlineKeyboardButton("💾 Сохранить данные", callback_data="admin_save"))
    kb.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
    return kb

def get_admin_masters_keyboard():
    kb = InlineKeyboardMarkup(row_width=1)
    for mid, m in masters.items():
        kb.add(InlineKeyboardButton(f"✏️ {m['name']}", callback_data=f"edit_master_{mid}"))
    kb.add(InlineKeyboardButton("➕ Добавить мастера", callback_data="add_master"))
    kb.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin"))
    return kb

def get_edit_master_keyboard(master_id):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✏️ Изменить имя", callback_data=f"rename_master_{master_id}"))
    kb.add(InlineKeyboardButton("📋 Изменить услуги", callback_data=f"edit_services_{master_id}"))
    kb.add(InlineKeyboardButton("🗑 Удалить мастера", callback_data=f"delete_master_{master_id}"))
    kb.add(InlineKeyboardButton("🔙 Назад", callback_data="admin_masters"))
    return kb

# ========== КОМАНДЫ ==========
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    is_admin = message.from_user.id in admin_sessions
    await message.reply(
        "✨ Добро пожаловать в студию красоты! ✨\n\n"
        "Я помогу вам записаться к мастеру, посмотреть ваши записи.\n"
        "Используйте кнопки ниже.",
        reply_markup=get_main_keyboard(is_admin)
    )

@dp.message_handler(commands=['masters'])
async def cmd_masters(message: types.Message):
    if not masters:
        await message.reply("Список мастеров пока пуст.")
        return
    text = "👩‍🎨 Наши мастера:\n\n"
    for m in masters.values():
        text += f"• {m['name']}\n   💇 {', '.join(m.get('services', []))}\n\n"
    await message.reply(text)

@dp.message_handler(commands=['my_appointments'])
async def cmd_my_appointments(message: types.Message):
    user_id = message.from_user.id
    my = {k: v for k, v in appointments.items() if v['user_id'] == user_id}
    if not my:
        await message.reply("📭 У вас нет записей.")
        return
    text = "📋 Ваши записи:\n\n"
    for aid, a in my.items():
        text += f"#{aid} — {a['master_name']}, {a['service']}, {a['date']}\n"
    await message.reply(text)

@dp.message_handler(commands=['cancel'], state='*')
async def cmd_cancel(message: types.Message, state: FSMContext):
    await state.finish()
    is_admin = message.from_user.id in admin_sessions
    await message.reply("❌ Действие отменено.", reply_markup=get_main_keyboard(is_admin))

# ========== СЕКРЕТНАЯ КОМАНДА ДЛЯ АДМИН-ДОСТУПА ==========
@dp.message_handler(commands=[ADMIN_SECRET_CODE.lstrip('/')])
async def admin_login(message: types.Message):
    admin_sessions.add(message.from_user.id)
    await message.reply(
        "✅ Вы получили права администратора!\n"
        "Теперь в главном меню появится кнопка '⚙️ Админ-панель'.\n"
        "Отправьте /start или нажмите любую кнопку, чтобы обновить меню.",
        reply_markup=get_main_keyboard(is_admin=True)
    )

# ========== КНОПКИ МЕНЮ ==========
@dp.message_handler(lambda m: m.text == "📅 Записаться")
async def appointment_start(message: types.Message, state: FSMContext):
    if not masters:
        await message.reply("Нет доступных мастеров.")
        return
    await state.set_state(AppointmentStates.choosing_master)
    await message.reply("Выберите мастера:", reply_markup=get_masters_keyboard())

@dp.message_handler(lambda m: m.text == "📋 Мои записи")
async def my_appointments_button(message: types.Message):
    await cmd_my_appointments(message)

@dp.message_handler(lambda m: m.text == "👩‍🎨 Наши мастера")
async def masters_button(message: types.Message):
    await cmd_masters(message)

@dp.message_handler(lambda m: m.text == "⚙️ Админ-панель")
async def admin_panel(message: types.Message):
    if message.from_user.id not in admin_sessions:
        await message.reply("⛔ У вас нет доступа к админ-панели.\n\nЧтобы получить доступ, отправьте секретную команду, которую вы установили в коде (например, /admin123).")
        return
    await message.reply("⚙️ Админ-панель:", reply_markup=get_admin_keyboard())

# ========== ПРИВЕТСТВИЕ НА ЛЮБОЕ СООБЩЕНИЕ ==========
@dp.message_handler(state='*', content_types=types.ContentTypes.ANY)
async def echo_welcome(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        is_admin = message.from_user.id in admin_sessions
        await message.reply(
            "👋 Привет! Я бот студии красоты.\n"
            "Чтобы записаться, нажмите кнопку '📅 Записаться'.\n"
            "Для списка мастеров — '👩‍🎨 Наши мастера'.\n"
            "Ваши записи — '📋 Мои записи'.\n\n"
            "Если вы администратор, отправьте секретную команду (например, /admin123).",
            reply_markup=get_main_keyboard(is_admin)
        )

# ========== INLINE CALLBACK ==========
@dp.callback_query_handler(lambda c: c.data.startswith("master_"), state=AppointmentStates.choosing_master)
async def process_master(callback: types.CallbackQuery, state: FSMContext):
    master_id = int(callback.data.split("_")[1])
    master = masters.get(master_id)
    if not master:
        await callback.answer("Мастер не найден")
        return
    await state.update_data(master_id=master_id, master_name=master['name'])
    services = master.get('services', [])
    if services:
        kb = InlineKeyboardMarkup(row_width=1)
        for s in services:
            kb.add(InlineKeyboardButton(s, callback_data=f"service_{master_id}_{s}"))
        kb.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_masters"))
        await state.set_state(AppointmentStates.choosing_service)
        await callback.message.edit_text(f"Мастер {master['name']}\nВыберите услугу:", reply_markup=kb)
    else:
        await state.set_state(AppointmentStates.entering_date)
        await callback.message.edit_text("Введите дату и время в формате:\nДД.ММ.ГГГГ ЧЧ:ММ")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("service_"), state=AppointmentStates.choosing_service)
async def process_service(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    service = "_".join(parts[2:])
    await state.update_data(service=service)
    await state.set_state(AppointmentStates.entering_date)
    await callback.message.edit_text("Введите дату и время в формате:\nДД.ММ.ГГГГ ЧЧ:ММ")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "back_to_masters", state=AppointmentStates.choosing_service)
async def back_to_masters(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AppointmentStates.choosing_master)
    await callback.message.edit_text("Выберите мастера:", reply_markup=get_masters_keyboard())
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await callback.message.delete()
    is_admin = callback.from_user.id in admin_sessions
    await callback.message.answer("Главное меню:", reply_markup=get_main_keyboard(is_admin))
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "back_to_admin")
async def back_to_admin(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    await callback.message.edit_text("⚙️ Админ-панель:", reply_markup=get_admin_keyboard())
    await callback.answer()

# ========== АДМИН: МАСТЕРА ==========
@dp.callback_query_handler(lambda c: c.data == "admin_masters")
async def admin_masters_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    await callback.message.edit_text("Управление мастерами:", reply_markup=get_admin_masters_keyboard())
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "add_master")
async def add_master_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    await state.set_state(MasterManagementStates.entering_name)
    await callback.message.edit_text("➕ Введите имя нового мастера:")
    await callback.answer()

@dp.message_handler(state=MasterManagementStates.entering_name)
async def add_master_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.reply("Слишком короткое имя, попробуйте ещё:")
        return
    await state.update_data(master_name=name)
    await state.set_state(MasterManagementStates.entering_services)
    await message.reply("Теперь введите список услуг через запятую:\nПример: маникюр, педикюр, shellac")

@dp.message_handler(state=MasterManagementStates.entering_services)
async def add_master_services(message: types.Message, state: FSMContext):
    global next_master_id
    services = [s.strip() for s in message.text.split(",")]
    data = await state.get_data()
    name = data['master_name']
    masters[next_master_id] = {"name": name, "services": services}
    await message.reply(f"✅ Мастер {name} добавлен с услугами: {', '.join(services)}")
    next_master_id += 1
    save_data()
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("edit_master_"))
async def edit_master_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    master_id = int(callback.data.split("_")[2])
    master = masters.get(master_id)
    if not master:
        await callback.message.edit_text("Мастер не найден")
        return
    await callback.message.edit_text(
        f"👩‍🎨 {master['name']}\nУслуги: {', '.join(master.get('services', []))}\n\nЧто делаем?",
        reply_markup=get_edit_master_keyboard(master_id)
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("rename_master_"))
async def rename_master_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    master_id = int(callback.data.split("_")[2])
    await state.update_data(edit_master_id=master_id)
    await state.set_state(MasterManagementStates.renaming_master)
    await callback.message.edit_text("Введите новое имя мастера:")
    await callback.answer()

@dp.message_handler(state=MasterManagementStates.renaming_master)
async def rename_master(message: types.Message, state: FSMContext):
    data = await state.get_data()
    master_id = data['edit_master_id']
    new_name = message.text.strip()
    if master_id in masters:
        old = masters[master_id]['name']
        masters[master_id]['name'] = new_name
        save_data()
        await message.reply(f"✅ Имя изменено: {old} → {new_name}")
    else:
        await message.reply("Мастер не найден")
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("edit_services_"))
async def edit_services_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    master_id = int(callback.data.split("_")[2])
    await state.update_data(edit_master_id=master_id)
    await state.set_state(MasterManagementStates.editing_services)
    await callback.message.edit_text("Введите новый список услуг через запятую:")
    await callback.answer()

@dp.message_handler(state=MasterManagementStates.editing_services)
async def edit_services(message: types.Message, state: FSMContext):
    data = await state.get_data()
    master_id = data['edit_master_id']
    services = [s.strip() for s in message.text.split(",")]
    if master_id in masters:
        masters[master_id]['services'] = services
        save_data()
        await message.reply(f"✅ Услуги обновлены: {', '.join(services)}")
    else:
        await message.reply("Мастер не найден")
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("delete_master_"))
async def delete_master(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    master_id = int(callback.data.split("_")[2])
    name = masters.get(master_id, {}).get('name', '')
    if master_id in masters:
        del masters[master_id]
        save_data()
        await callback.message.edit_text(f"✅ Мастер {name} удалён")
    else:
        await callback.message.edit_text("Мастер не найден")
    await callback.answer()

# ========== АДМИН: ЗАПИСИ И СТАТИСТИКА ==========
@dp.callback_query_handler(lambda c: c.data == "admin_appointments")
async def admin_appointments(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    if not appointments:
        await callback.message.edit_text("Нет ни одной записи.")
        return
    text = "📊 ВСЕ ЗАПИСИ:\n\n"
    for aid, a in appointments.items():
        text += f"#{aid} | {a['user_name']} → {a['master_name']}, {a['service']}, {a['date']}\n"
    if len(text) > 4000:
        text = text[:4000] + "..."
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    total = len(appointments)
    text = f"📈 СТАТИСТИКА:\n\nМастеров: {len(masters)}\nВсего записей: {total}"
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "admin_save")
async def admin_save(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_sessions:
        await callback.answer("Нет доступа")
        return
    save_data()
    await callback.message.edit_text("✅ Данные принудительно сохранены в Yandex Cloud.")
    await callback.answer()

# ========== ОБРАБОТКА ДАТЫ И ПОДТВЕРЖДЕНИЕ ЗАПИСИ ==========
@dp.message_handler(state=AppointmentStates.entering_date)
async def process_date(message: types.Message, state: FSMContext):
    text = message.text.strip()
    try:
        if " " in text:
            date_str, time_str = text.split()
            day, month, year = map(int, date_str.split('.'))
            hour, minute = map(int, time_str.split(':'))
            dt = datetime(year, month, day, hour, minute)
            if dt < datetime.now():
                await message.reply("❌ Нельзя записаться в прошлое. Введите будущую дату:")
                return
            await state.update_data(appointment_date=text)
            data = await state.get_data()
            confirm = (
                f"📝 Проверьте данные:\n"
                f"Мастер: {data['master_name']}\n"
                f"Услуга: {data.get('service', 'Не указана')}\n"
                f"Дата: {text}\n\n"
                f"Всё верно? (да / нет)"
            )
            await state.set_state(AppointmentStates.confirming)
            await message.reply(confirm)
        else:
            await message.reply("❌ Неверный формат. Нужно: ДД.ММ.ГГГГ ЧЧ:ММ")
    except:
        await message.reply("❌ Ошибка. Пример: 25.04.2025 15:30")

@dp.message_handler(state=AppointmentStates.confirming)
async def confirm_appointment(message: types.Message, state: FSMContext):
    global next_appointment_id
    if message.text.lower() == "да":
        data = await state.get_data()
        new_app = {
            "user_id": message.from_user.id,
            "user_name": message.from_user.full_name,
            "master_id": data['master_id'],
            "master_name": data['master_name'],
            "service": data.get('service', 'Не указана'),
            "date": data['appointment_date'],
            "status": "pending",
            "created_at": datetime.now().isoformat()
        }
        appointments[next_appointment_id] = new_app
        save_data()
        for admin_id in admin_sessions:
            try:
                await bot.send_message(
                    admin_id,
                    f"🔔 НОВАЯ ЗАПИСЬ!\n\n"
                    f"Клиент: {message.from_user.full_name}\n"
                    f"Мастер: {data['master_name']}\n"
                    f"Услуга: {new_app['service']}\n"
                    f"Дата: {data['appointment_date']}\n"
                    f"№ записи: {next_appointment_id}"
                )
            except:
                pass
        await message.reply(
            f"✅ Запись #{next_appointment_id} создана!\n"
            f"Мастер свяжется с вами для подтверждения.",
            reply_markup=get_main_keyboard(message.from_user.id in admin_sessions)
        )
        next_appointment_id += 1
        await state.finish()
    elif message.text.lower() == "нет":
        await state.finish()
        is_admin = message.from_user.id in admin_sessions
        await message.reply("❌ Запись отменена.", reply_markup=get_main_keyboard(is_admin))
    else:
        await message.reply('Пожалуйста, ответьте "да" или "нет".')

# ========== ВЕБ-СЕРВЕР ДЛЯ HEALTHCHECK (чтобы Render не убивал) ==========
async def health_check(request):
    return web.Response(text="I'm alive!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 Веб-сервер для healthcheck запущен на порту {port}")

# ========== ЗАПУСК БОТА И ВЕБ-СЕРВЕРА ПАРАЛЛЕЛЬНО ==========
async def main():
    # Запускаем веб-сервер
    await start_web_server()
    # Запускаем бота
    print("🤖 Бот запущен. Секретный код для админа: /" + ADMIN_SECRET_CODE)
    await dp.start_polling()

if __name__ == '__main__':
    asyncio.run(main())