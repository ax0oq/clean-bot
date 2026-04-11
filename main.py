import os
import logging
import secrets
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple, Any, Set
from collections import defaultdict
from functools import wraps
from enum import Enum
import sqlite3
import tempfile
import uuid
import calendar

# Pydantic
from pydantic import BaseModel, Field, validator, SecretStr
from pydantic_settings import BaseSettings

# Aiogram v3
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram import Router
from aiohttp import web

# S3
import aioboto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# База данных
import aiosqlite
from contextlib import asynccontextmanager

# Fuzzy date parsing
try:
    import dateparser

    HAS_DATEPARSER = True
except ImportError:
    HAS_DATEPARSER = False

# ========== СООБЩЕНИЯ ==========
MESSAGES = {
    'welcome': "✨ *Добро пожаловать в студию красоты!* ✨",
    'main_menu': "🏠 *Главное меню*",
    'choose_master': "💅 *Выберите мастера:*",
    'choose_service': "💇 *Выберите услугу:*",
    'choose_date': "📅 *Выберите дату:*",
    'choose_time': "🕐 *Выберите время:*",
    'confirm': "✅ *Проверьте вашу запись:*\n\n💅 Мастер: `{}`\n💇 Услуга: `{}` ({} мин)\n📅 Дата: `{}`\n\nВсе верно?",
    'success': "🎉 *Запись #{} создана!*",
    'cancelled': "❌ Действие отменено.",
    'no_masters': "😔 Список мастеров пока пуст.",
    'no_appointments': "📭 У вас нет записей.",
    'no_available_slots': "❌ Нет свободных слотов на эту дату.",
    'rate_limit': "⏱️ Слишком много запросов.",
    'no_access': "🔐 Нет доступа.",
    'error': "⚠️ Произошла ошибка.",
}


# ========== ENUM ==========
class UserRole(str, Enum):
    CLIENT = "client"
    MASTER = "master"
    OWNER = "owner"


class AppointmentStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"
    COMPLETED = "completed"


class MasterScheduleStatus(str, Enum):
    WORKING = "working"
    DAY_OFF = "day_off"


class BreakType(str, Enum):
    BREAK = "break"
    LUNCH = "lunch"


class ReviewRating(int, Enum):
    EXCELLENT = 5
    GOOD = 4
    NORMAL = 3
    BAD = 2
    TERRIBLE = 1


class CallbackCommand(str, Enum):
    M_ = "m_"
    S_ = "s_"
    D_ = "d_"
    T_ = "t_"
    BACK_M = "bm"
    BACK_S = "bs"
    BACK_D = "bd"
    BACK_MAIN = "main"
    RESCHEDULE_PREFIX = "resc_"
    CANCEL_APPT_PREFIX = "canc_"

    O_MASTERS = "om"
    O_APPTS = "oa"
    O_STATS = "os"
    O_BROADCAST = "ob"
    O_HOLIDAYS = "oh"
    O_SETTINGS = "osett"
    O_LOGOUT = "ol"

    MASTER_SCHED = "ms"
    MASTER_APPTS = "ma"
    MASTER_SERVICES = "mser"
    MASTER_BREAKS = "mbrk"
    MASTER_TODAY = "mst"
    MASTER_WEEK = "msw"
    MASTER_ADD_SHIFT = "mas"
    MASTER_CONFIRM_PREFIX = "mconf_"
    MASTER_CANCEL_PREFIX = "mcanc_"
    MASTER_LOGOUT = "mlogout"


# ========== КОНФИГУРАЦИЯ ==========
class Settings(BaseSettings):
    token: str
    admin_password: SecretStr

    yandex_access_key: str = ""
    yandex_secret_key: str = ""
    yandex_bucket_name: str = "nogotochki1"
    yandex_endpoint: str = "https://storage.yandexcloud.net"
    s3_retry_attempts: int = 3
    s3_timeout: int = 10
    s3_retry_delay: float = 0.5
    crash_on_startup_failure: bool = True

    database_url: str = "salon.db"
    db_pool_size: int = 5

    session_ttl_hours: int = 24

    rate_limit_requests: int = 100
    rate_limit_window_seconds: int = 60
    rate_limit_callback_requests: int = 50
    rate_limit_callback_window: int = 5

    working_hours_start: int = 9
    working_hours_end: int = 20
    allow_weekends: bool = False

    default_service_duration_minutes: int = 60
    cleaning_buffer_minutes: int = 10
    cancellation_limit_hours: int = 2
    max_appointments_per_service: int = 2

    reminder_24h_enabled: bool = True
    reminder_1h_enabled: bool = True
    master_reminder_15m_enabled: bool = True

    backup_retention_days: int = 7
    backup_interval_hours: int = 1

    health_check_secret: str = ""
    masters_cache_ttl_minutes: int = 60
    slots_cache_ttl_minutes: int = 5
    max_service_length: int = 100
    max_services_per_master: int = 20

    notify_owner_unset_shift: bool = True
    notify_owner_cancel_late: bool = True
    notify_owner_unconfirmed: bool = True
    unconfirmed_timeout_minutes: int = 60

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ========== PYDANTIC МОДЕЛИ ==========
class Master(BaseModel):
    id: Optional[int] = None
    name: str = Field(..., min_length=2, max_length=50)
    telegram_id: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.now)


class MasterService(BaseModel):
    id: Optional[int] = None
    master_id: int
    service: str = Field(..., min_length=1, max_length=100)
    duration_minutes: int = Field(default=60, ge=15, le=240)
    is_available: bool = True


class MasterSchedule(BaseModel):
    id: Optional[int] = None
    master_id: int
    date: str
    start_time: str
    end_time: str
    status: MasterScheduleStatus = MasterScheduleStatus.WORKING
    created_at: datetime = Field(default_factory=datetime.now)


class MasterBreak(BaseModel):
    id: Optional[int] = None
    master_id: int
    date: str
    start_time: str
    end_time: str
    break_type: BreakType = BreakType.BREAK
    created_at: datetime = Field(default_factory=datetime.now)


class Appointment(BaseModel):
    id: Optional[int] = None
    user_id: int
    user_name: str
    master_id: int
    master_name: str
    service: str
    duration_minutes: int = 60
    date: datetime
    status: AppointmentStatus = AppointmentStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.now)


class ClientReview(BaseModel):
    id: Optional[int] = None
    appointment_id: int
    client_id: int
    master_id: int
    rating: ReviewRating
    comment: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)


class SalonHoliday(BaseModel):
    id: Optional[int] = None
    date: str
    reason: str
    created_at: datetime = Field(default_factory=datetime.now)


# ========== SECURITY ==========
ADMIN_PASSWORD = settings.admin_password.get_secret_value()


def verify_password(password: str) -> bool:
    return secrets.compare_digest(password, ADMIN_PASSWORD)


# ========== КЕШИРОВАНИЕ ==========
class SlotsCache:
    def __init__(self, ttl_minutes: int = 5):
        self.ttl_minutes = ttl_minutes
        self.cache: Dict[str, Tuple[List[Tuple[str, str]], datetime]] = {}
        self.lock = asyncio.Lock()

    def _make_key(self, master_id: int, date: str, duration: int) -> str:
        return f"{master_id}_{date}_{duration}"

    async def get(self, master_id: int, date: str, duration: int, fetch_fn) -> List[Tuple[str, str]]:
        key = self._make_key(master_id, date, duration)
        async with self.lock:
            if key in self.cache:
                slots, cached_at = self.cache[key]
                if (datetime.now() - cached_at).total_seconds() < self.ttl_minutes * 60:
                    return slots

            slots = await fetch_fn(master_id, date, duration)
            self.cache[key] = (slots, datetime.now())
            return slots

    async def invalidate(self, master_id: Optional[int] = None) -> None:
        async with self.lock:
            if master_id is None:
                self.cache.clear()
            else:
                keys_to_delete = [k for k in self.cache.keys() if k.startswith(f"{master_id}_")]
                for k in keys_to_delete:
                    del self.cache[k]


class MastersCache:
    def __init__(self, db: 'DatabaseRepository', ttl_minutes: int = 60):
        self.db = db
        self.ttl_minutes = ttl_minutes
        self.cache: Optional[List[Tuple[Master, List[MasterService]]]] = None
        self.last_update: Optional[datetime] = None
        self.lock = asyncio.Lock()

    async def get(self) -> List[Tuple[Master, List[MasterService]]]:
        async with self.lock:
            now = datetime.now()
            if (self.cache is not None and
                    self.last_update and
                    (now - self.last_update).total_seconds() < self.ttl_minutes * 60):
                return self.cache

            self.cache = await self.db.get_all_masters_with_services()
            self.last_update = now
            return self.cache

    async def invalidate(self) -> None:
        async with self.lock:
            self.cache = None
            self.last_update = None


# ========== DATABASE REPOSITORY ==========
class DatabaseRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(settings.db_pool_size)

    @asynccontextmanager
    async def _get_connection(self):
        async with self.semaphore:
            async with aiosqlite.connect(self.db_path) as db:
                yield db

    async def init_db(self) -> None:
        async with self._get_connection() as db:
            await db.executescript("""
                PRAGMA foreign_keys = ON;
                PRAGMA journal_mode = WAL;

                CREATE TABLE IF NOT EXISTS masters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    telegram_id INTEGER UNIQUE,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS master_services (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    master_id INTEGER NOT NULL,
                    service TEXT NOT NULL,
                    duration_minutes INTEGER NOT NULL DEFAULT 60,
                    is_available BOOLEAN NOT NULL DEFAULT 1,
                    FOREIGN KEY (master_id) REFERENCES masters(id) ON DELETE CASCADE,
                    UNIQUE(master_id, service)
                );

                CREATE TABLE IF NOT EXISTS master_schedule (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    master_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'working',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (master_id) REFERENCES masters(id) ON DELETE CASCADE,
                    UNIQUE(master_id, date)
                );

                CREATE INDEX IF NOT EXISTS idx_schedule_master_date ON master_schedule(master_id, date);

                CREATE TABLE IF NOT EXISTS master_breaks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    master_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    break_type TEXT NOT NULL DEFAULT 'break',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (master_id) REFERENCES masters(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_breaks_master_date ON master_breaks(master_id, date);

                CREATE TABLE IF NOT EXISTS appointments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    user_name TEXT NOT NULL,
                    master_id INTEGER NOT NULL,
                    master_name TEXT NOT NULL,
                    service TEXT NOT NULL,
                    duration_minutes INTEGER NOT NULL DEFAULT 60,
                    date TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (master_id) REFERENCES masters(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_appointments_user_id ON appointments(user_id);
                CREATE INDEX IF NOT EXISTS idx_appointments_master_id ON appointments(master_id);
                CREATE INDEX IF NOT EXISTS idx_appointments_date ON appointments(date);

                CREATE TABLE IF NOT EXISTS client_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    appointment_id INTEGER NOT NULL UNIQUE,
                    client_id INTEGER NOT NULL,
                    master_id INTEGER NOT NULL,
                    rating INTEGER NOT NULL,
                    comment TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (appointment_id) REFERENCES appointments(id) ON DELETE CASCADE,
                    FOREIGN KEY (master_id) REFERENCES masters(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS salon_holidays (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL UNIQUE,
                    reason TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS owner_sessions (
                    user_id INTEGER PRIMARY KEY,
                    session_token TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_owner_sessions_token ON owner_sessions(session_token);

                CREATE TABLE IF NOT EXISTS sent_reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    appointment_id INTEGER NOT NULL,
                    reminder_type TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    FOREIGN KEY (appointment_id) REFERENCES appointments(id) ON DELETE CASCADE,
                    UNIQUE(appointment_id, reminder_type)
                );
            """)
            await db.commit()
            logger.info("✅ База данных инициализирована")

    async def get_all_masters_with_services(self) -> List[Tuple[Master, List[MasterService]]]:
        async with self._get_connection() as db:
            db.row_factory = aiosqlite.Row
            try:
                async with db.execute(
                        "SELECT id, name, telegram_id, created_at FROM masters ORDER BY name"
                ) as cursor:
                    rows = await cursor.fetchall()
                    masters: List[Tuple[Master, List[MasterService]]] = []

                    for row in rows:
                        try:
                            services = await self._fetch_master_services(db, row['id'])
                            master = Master(
                                id=row['id'],
                                name=row['name'],
                                telegram_id=row['telegram_id'],
                                created_at=datetime.fromisoformat(row['created_at'])
                            )
                            masters.append((master, services))
                        except Exception as err:
                            logger.error(f"❌ Ошибка загрузки мастера {row['id']}: {err}")

                    return masters
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}", exc_info=True)
                return []

    async def _fetch_master_services(self, db: aiosqlite.Connection, master_id: int) -> List[MasterService]:
        async with db.execute(
                "SELECT id, master_id, service, duration_minutes, is_available FROM master_services WHERE master_id = ? ORDER BY service",
                (master_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            services: List[MasterService] = []
            for row in rows:
                services.append(MasterService(
                    id=row[0],
                    master_id=row[1],
                    service=row[2],
                    duration_minutes=row[3] or settings.default_service_duration_minutes,
                    is_available=bool(row[4])
                ))
            return services

    async def get_master_by_telegram_id(self, telegram_id: int) -> Optional[Master]:
        async with self._get_connection() as db:
            db.row_factory = aiosqlite.Row
            try:
                async with db.execute(
                        "SELECT id, name, telegram_id, created_at FROM masters WHERE telegram_id = ?",
                        (telegram_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return Master(
                            id=row['id'],
                            name=row['name'],
                            telegram_id=row['telegram_id'],
                            created_at=datetime.fromisoformat(row['created_at'])
                        )
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
            return None

    async def get_master_by_id(self, master_id: int) -> Optional[Master]:
        async with self._get_connection() as db:
            db.row_factory = aiosqlite.Row
            try:
                async with db.execute(
                        "SELECT id, name, telegram_id, created_at FROM masters WHERE id = ?",
                        (master_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return Master(
                            id=row['id'],
                            name=row['name'],
                            telegram_id=row['telegram_id'],
                            created_at=datetime.fromisoformat(row['created_at'])
                        )
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
            return None

    async def add_master_schedule(self, schedule: MasterSchedule) -> bool:
        async with self.lock:
            async with self._get_connection() as db:
                try:
                    await db.execute(
                        """INSERT OR REPLACE INTO master_schedule 
                           (master_id, date, start_time, end_time, status, created_at)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (schedule.master_id, schedule.date, schedule.start_time,
                         schedule.end_time, schedule.status.value, schedule.created_at.isoformat())
                    )
                    await db.commit()
                    return True
                except Exception as err:
                    logger.error(f"❌ Ошибка: {err}")
                    return False

    async def get_master_schedule(self, master_id: int, date: str) -> Optional[MasterSchedule]:
        async with self._get_connection() as db:
            db.row_factory = aiosqlite.Row
            try:
                async with db.execute(
                        "SELECT * FROM master_schedule WHERE master_id = ? AND date = ?",
                        (master_id, date)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return MasterSchedule(
                            id=row['id'],
                            master_id=row['master_id'],
                            date=row['date'],
                            start_time=row['start_time'],
                            end_time=row['end_time'],
                            status=MasterScheduleStatus(row['status']),
                            created_at=datetime.fromisoformat(row['created_at'])
                        )
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
            return None

    async def get_master_schedule_range(self, master_id: int, date_from: str, date_to: str) -> Dict[
        str, MasterSchedule]:
        result = {}
        async with self._get_connection() as db:
            db.row_factory = aiosqlite.Row
            try:
                async with db.execute(
                        """SELECT * FROM master_schedule 
                           WHERE master_id = ? AND date BETWEEN ? AND ?
                           ORDER BY date""",
                        (master_id, date_from, date_to)
                ) as cursor:
                    rows = await cursor.fetchall()
                    for row in rows:
                        schedule = MasterSchedule(
                            id=row['id'],
                            master_id=row['master_id'],
                            date=row['date'],
                            start_time=row['start_time'],
                            end_time=row['end_time'],
                            status=MasterScheduleStatus(row['status']),
                            created_at=datetime.fromisoformat(row['created_at'])
                        )
                        result[row['date']] = schedule
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
        return result

    async def get_master_breaks(self, master_id: int, date: str) -> List[MasterBreak]:
        async with self._get_connection() as db:
            db.row_factory = aiosqlite.Row
            try:
                async with db.execute(
                        "SELECT * FROM master_breaks WHERE master_id = ? AND date = ? ORDER BY start_time",
                        (master_id, date)
                ) as cursor:
                    rows = await cursor.fetchall()
                    breaks_: List[MasterBreak] = []
                    for row in rows:
                        breaks_.append(MasterBreak(
                            id=row['id'],
                            master_id=row['master_id'],
                            date=row['date'],
                            start_time=row['start_time'],
                            end_time=row['end_time'],
                            break_type=BreakType(row['break_type']),
                            created_at=datetime.fromisoformat(row['created_at'])
                        ))
                    return breaks_
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
                return []

    async def get_available_slots(self, master_id: int, date_str: str, duration_minutes: int) -> List[Tuple[str, str]]:
        async with self._get_connection() as db:
            try:
                # Проверка выходного салона
                async with db.execute(
                        "SELECT COUNT(*) FROM salon_holidays WHERE date = ?",
                        (date_str,)
                ) as cursor:
                    if (await cursor.fetchone())[0] > 0:
                        return []

                schedule = await self.get_master_schedule(master_id, date_str)
                if not schedule or schedule.status == MasterScheduleStatus.DAY_OFF:
                    return []

                breaks_ = await self.get_master_breaks(master_id, date_str)

                # Существующие записи
                async with db.execute(
                        """SELECT datetime(date) as start, datetime(date, '+' || duration_minutes || ' minutes') as end
                           FROM appointments 
                           WHERE master_id = ? AND date LIKE ? AND status != ?""",
                        (master_id, f"{date_str}%", AppointmentStatus.CANCELLED.value)
                ) as cursor:
                    occupied = await cursor.fetchall()

                slots: List[Tuple[str, str]] = []
                current = datetime.strptime(f"{date_str} {schedule.start_time}", "%Y-%m-%d %H:%M")
                end_time = datetime.strptime(f"{date_str} {schedule.end_time}", "%Y-%m-%d %H:%M")

                while current + timedelta(minutes=duration_minutes + settings.cleaning_buffer_minutes) <= end_time:
                    slot_end = current + timedelta(minutes=duration_minutes)

                    # Проверка перерывов
                    in_break = False
                    for brk in breaks_:
                        brk_start = datetime.strptime(f"{date_str} {brk.start_time}", "%Y-%m-%d %H:%M")
                        brk_end = datetime.strptime(f"{date_str} {brk.end_time}", "%Y-%m-%d %H:%M")
                        if not (slot_end <= brk_start or current >= brk_end):
                            in_break = True
                            break

                    if in_break:
                        current += timedelta(minutes=30)
                        continue

                    # Проверка пересечения с записями
                    is_free = True
                    for occ_start, occ_end in occupied:
                        occ_start_dt = datetime.fromisoformat(occ_start)
                        occ_end_dt = datetime.fromisoformat(occ_end)
                        if not (slot_end <= occ_start_dt or current >= occ_end_dt):
                            is_free = False
                            break

                    if is_free and current >= datetime.now():
                        slots.append((current.strftime("%H:%M"), slot_end.strftime("%H:%M")))

                    current += timedelta(minutes=30)

                return slots
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
                return []

    async def add_appointment(self, appointment: Appointment) -> Optional[int]:
        async with self.lock:
            async with self._get_connection() as db:
                try:
                    # Проверка мастера
                    async with db.execute(
                            "SELECT id FROM masters WHERE id = ?",
                            (appointment.master_id,)
                    ) as cursor:
                        if not await cursor.fetchone():
                            return None

                    # Проверка лимита записей на услугу
                    async with db.execute(
                            """SELECT COUNT(*) FROM appointments 
                               WHERE user_id = ? AND service = ? AND status != ?""",
                            (appointment.user_id, appointment.service, AppointmentStatus.CANCELLED.value)
                    ) as cursor:
                        count = (await cursor.fetchone())[0]
                        if count >= settings.max_appointments_per_service:
                            return None

                    cursor = await db.execute(
                        """INSERT INTO appointments 
                           (user_id, user_name, master_id, master_name, service, duration_minutes, date, status, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (appointment.user_id, appointment.user_name, appointment.master_id,
                         appointment.master_name, appointment.service, appointment.duration_minutes,
                         appointment.date.isoformat(), appointment.status.value,
                         appointment.created_at.isoformat())
                    )
                    appt_id = cursor.lastrowid
                    await db.commit()
                    return appt_id
                except Exception as err:
                    logger.error(f"❌ Ошибка: {err}")
                    return None

    async def get_appointment_by_id(self, appointment_id: int) -> Optional[Appointment]:
        async with self._get_connection() as db:
            db.row_factory = aiosqlite.Row
            try:
                async with db.execute(
                        "SELECT * FROM appointments WHERE id = ?",
                        (appointment_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return Appointment(
                            id=row['id'], user_id=row['user_id'], user_name=row['user_name'],
                            master_id=row['master_id'], master_name=row['master_name'],
                            service=row['service'], duration_minutes=row['duration_minutes'],
                            date=datetime.fromisoformat(row['date']),
                            status=AppointmentStatus(row['status']),
                            created_at=datetime.fromisoformat(row['created_at'])
                        )
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
            return None

    async def get_user_appointments(self, user_id: int) -> List[Appointment]:
        async with self._get_connection() as db:
            db.row_factory = aiosqlite.Row
            try:
                async with db.execute(
                        "SELECT * FROM appointments WHERE user_id = ? ORDER BY date DESC",
                        (user_id,)
                ) as cursor:
                    rows = await cursor.fetchall()
                    appointments: List[Appointment] = []
                    for row in rows:
                        try:
                            appointments.append(Appointment(
                                id=row['id'], user_id=row['user_id'], user_name=row['user_name'],
                                master_id=row['master_id'], master_name=row['master_name'],
                                service=row['service'], duration_minutes=row['duration_minutes'],
                                date=datetime.fromisoformat(row['date']),
                                status=AppointmentStatus(row['status']),
                                created_at=datetime.fromisoformat(row['created_at'])
                            ))
                        except Exception:
                            pass
                    return appointments
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
                return []

    async def update_appointment_status(self, appointment_id: int, status: AppointmentStatus) -> bool:
        async with self.lock:
            async with self._get_connection() as db:
                try:
                    await db.execute(
                        "UPDATE appointments SET status = ? WHERE id = ?",
                        (status.value, appointment_id)
                    )
                    await db.commit()
                    return True
                except Exception as err:
                    logger.error(f"❌ Ошибка: {err}")
                    return False

    async def can_cancel_appointment(self, appointment_id: int) -> bool:
        appt = await self.get_appointment_by_id(appointment_id)
        if not appt:
            return False
        time_until = appt.date - datetime.now()
        return time_until.total_seconds() > (settings.cancellation_limit_hours * 3600)

    async def get_master_rating(self, master_id: int) -> Tuple[float, int]:
        async with self._get_connection() as db:
            try:
                async with db.execute(
                        "SELECT AVG(rating), COUNT(*) FROM client_reviews WHERE master_id = ?",
                        (master_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row and row[0]:
                        return float(row[0]), int(row[1])
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
        return 0.0, 0

    async def toggle_service_availability(self, master_id: int, service_id: int, is_available: bool) -> bool:
        async with self.lock:
            async with self._get_connection() as db:
                try:
                    await db.execute(
                        "UPDATE master_services SET is_available = ? WHERE id = ? AND master_id = ?",
                        (int(is_available), service_id, master_id)
                    )
                    await db.commit()
                    return True
                except Exception as err:
                    logger.error(f"❌ Ошибка: {err}")
                    return False

    async def mark_reminder_sent(self, appointment_id: int, reminder_type: str) -> bool:
        async with self._get_connection() as db:
            try:
                await db.execute(
                    "INSERT OR IGNORE INTO sent_reminders (appointment_id, reminder_type, sent_at) VALUES (?, ?, ?)",
                    (appointment_id, reminder_type, datetime.now().isoformat())
                )
                await db.commit()
                return True
            except Exception as err:
                logger.error(f"❌ Ошибка: {err}")
                return False

    async def is_reminder_sent(self, appointment_id: int, reminder_type: str) -> bool:
        async with self._get_connection() as db:
            try:
                async with db.execute(
                        "SELECT COUNT(*) FROM sent_reminders WHERE appointment_id = ? AND reminder_type = ?",
                        (appointment_id, reminder_type)
                ) as cursor:
                    return (await cursor.fetchone())[0] > 0
            except Exception:
                return False


# ========== OWNER SESSIONS ==========
class OwnerSessionRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = asyncio.Lock()

    async def create_session(self, user_id: int, ttl_hours: int = 24) -> str:
        session_token = secrets.token_urlsafe(32)
        now = datetime.now()
        expires_at = now + timedelta(hours=ttl_hours)
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO owner_sessions (user_id, session_token, created_at, expires_at) VALUES (?, ?, ?, ?)",
                    (user_id, session_token, now.isoformat(), expires_at.isoformat())
                )
                await db.commit()
        return session_token

    async def verify_session(self, user_id: int) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                        "SELECT expires_at FROM owner_sessions WHERE user_id = ?",
                        (user_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        expires_at = datetime.fromisoformat(row[0])
                        return expires_at > datetime.now()
        except Exception:
            pass
        return False

    async def delete_session(self, user_id: int) -> None:
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("DELETE FROM owner_sessions WHERE user_id = ?", (user_id,))
                await db.commit()

    async def cleanup_expired(self) -> int:
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                now = datetime.now().isoformat()
                cursor = await db.execute("DELETE FROM owner_sessions WHERE expires_at < ?", (now,))
                await db.commit()
                return cursor.rowcount


# ========== RATE LIMITER ==========
class RateLimiter:
    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: Dict[int, List[float]] = defaultdict(list)
        self.lock = asyncio.Lock()

    async def is_allowed(self, user_id: int) -> bool:
        async with self.lock:
            now = time.time()
            window_start = now - self.window_seconds
            self.requests[user_id] = [t for t in self.requests[user_id] if t > window_start]
            if len(self.requests[user_id]) >= self.max_requests:
                return False
            self.requests[user_id].append(now)
            return True

    async def cleanup_old_entries(self) -> None:
        while True:
            try:
                await asyncio.sleep(3600)
                async with self.lock:
                    now = time.time()
                    window_start = now - self.window_seconds
                    to_delete = []
                    for uid in list(self.requests.keys()):
                        self.requests[uid] = [t for t in self.requests[uid] if t > window_start]
                        if not self.requests[uid]:
                            to_delete.append(uid)
                    for uid in to_delete:
                        del self.requests[uid]
            except asyncio.CancelledError:
                break
            except Exception:
                pass


rate_limiter_message = RateLimiter(settings.rate_limit_requests, settings.rate_limit_window_seconds)
rate_limiter_callback = RateLimiter(settings.rate_limit_callback_requests, settings.rate_limit_callback_window)


# ========== FSM ==========
class ClientAppointmentStates(StatesGroup):
    choosing_master = State()
    choosing_service = State()
    choosing_date = State()
    choosing_time = State()
    confirming = State()


class MasterShiftStates(StatesGroup):
    selecting_day = State()
    entering_start = State()
    entering_end = State()


class AdminAuthStates(StatesGroup):
    waiting_password = State()


# ========== ИНИЦИАЛИЗАЦИЯ ==========
storage = MemoryStorage()
bot = Bot(token=settings.token)
dp = Dispatcher(storage=storage)
router = Router()

db = DatabaseRepository(settings.database_url)
owner_sessions_repo = OwnerSessionRepository(settings.database_url)
masters_cache = MastersCache(db, settings.masters_cache_ttl_minutes)
slots_cache = SlotsCache(settings.slots_cache_ttl_minutes)


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
async def get_user_role(user_id: int) -> UserRole:
    if await owner_sessions_repo.verify_session(user_id):
        return UserRole.OWNER
    master = await db.get_master_by_telegram_id(user_id)
    if master:
        return UserRole.MASTER
    return UserRole.CLIENT


def get_main_keyboard(role: UserRole) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    if role == UserRole.OWNER:
        kb.add(KeyboardButton("👑 Панель владельца"))
    elif role == UserRole.MASTER:
        kb.add(KeyboardButton("👤 Кабинет мастера"))
    else:
        kb.add(KeyboardButton("💅 Записаться"))
    kb.add(KeyboardButton("📋 Мои записи"))
    kb.add(KeyboardButton("👥 Наши мастера"))
    return kb


def handle_errors(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            logger.error(f"❌ {func.__name__}: {err}", exc_info=True)
            msg = args[0] if args else None
            if isinstance(msg, types.CallbackQuery):
                await msg.answer(MESSAGES['error'], show_alert=True)
            elif isinstance(msg, types.Message):
                await msg.reply(MESSAGES['error'])

    return wrapper


def create_calendar(year: int, month: int, available_dates: Set[str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=7)
    # Заголовок с навигацией
    kb.add(
        InlineKeyboardButton("◀️", callback_data=f"cal_prev_{year}_{month}"),
        InlineKeyboardButton(f"{calendar.month_name[month]} {year}", callback_data="ignore"),
        InlineKeyboardButton("▶️", callback_data=f"cal_next_{year}_{month}")
    )
    # Дни недели
    days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    kb.add(*[InlineKeyboardButton(d, callback_data="ignore") for d in days])

    cal = calendar.monthcalendar(year, month)
    for week in cal:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="ignore"))
            else:
                date_str = f"{year}-{month:02d}-{day:02d}"
                emoji = "🟢" if date_str in available_dates else "⚪"
                cb_data = f"{CallbackCommand.D_.value}{date_str}" if date_str in available_dates else "nodate"
                row.append(InlineKeyboardButton(f"{emoji}{day}", callback_data=cb_data))
        kb.add(*row)
    return kb


# ========== КОМАНДЫ ==========
@router.message(commands=["start"])
@handle_errors
async def cmd_start(message: types.Message):
    role = await get_user_role(message.from_user.id)
    await message.reply(MESSAGES['welcome'], reply_markup=get_main_keyboard(role), parse_mode="Markdown")


@router.message(commands=["masters"])
@handle_errors
async def cmd_masters(message: types.Message):
    if not await rate_limiter_message.is_allowed(message.from_user.id):
        await message.reply(MESSAGES['rate_limit'])
        return
    masters = await masters_cache.get()
    if not masters:
        await message.reply(MESSAGES['no_masters'])
        return
    text = "👥 *Наши мастера:*\n\n"
    for master, services in masters:
        if not services:
            continue
        rating, count = await db.get_master_rating(master.id)
        stars = "⭐" * int(rating) if rating > 0 else "📊"
        text += f"💅 *{master.name}* {stars} ({rating:.1f}, {count})\n"
        text += f"   {', '.join([s.service for s in services[:3]])}\n\n"
    await message.reply(text, parse_mode="Markdown")


@router.message(commands=["cancel"])
@handle_errors
async def cmd_cancel(message: types.Message, state: FSMContext):
    await state.clear()
    role = await get_user_role(message.from_user.id)
    await message.reply(MESSAGES['cancelled'], reply_markup=get_main_keyboard(role))


@router.message(commands=["admin"])
@handle_errors
async def admin_login_start(message: types.Message, state: FSMContext):
    if await owner_sessions_repo.verify_session(message.from_user.id):
        await message.reply("✅ Вы уже владелец!")
        return
    await state.set_state(AdminAuthStates.waiting_password)
    await message.reply("🔐 Введите пароль:")


@router.message(AdminAuthStates.waiting_password)
@handle_errors
async def admin_password_check(message: types.Message, state: FSMContext):
    if verify_password(message.text):
        await owner_sessions_repo.create_session(message.from_user.id)
        await state.clear()
        role = await get_user_role(message.from_user.id)
        await message.reply("✅ Добро пожаловать!", reply_markup=get_main_keyboard(role))
    else:
        await message.reply("❌ Неверный пароль")


# ========== КНОПКИ ГЛАВНОГО МЕНЮ ==========
@router.message(F.text == "💅 Записаться")
@handle_errors
async def appointment_start(message: types.Message, state: FSMContext):
    if not await rate_limiter_message.is_allowed(message.from_user.id):
        await message.reply(MESSAGES['rate_limit'])
        return
    masters = await masters_cache.get()
    available = [m for m, s in masters if any(srv.is_available for srv in s)]
    if not available:
        await message.reply(MESSAGES['no_masters'])
        return
    await state.set_state(ClientAppointmentStates.choosing_master)
    kb = InlineKeyboardMarkup(row_width=1)
    for master, _ in available:
        kb.add(InlineKeyboardButton(f"💅 {master.name}", callback_data=f"{CallbackCommand.M_.value}{master.id}"))
    kb.add(InlineKeyboardButton("🔙 Главное меню", callback_data=CallbackCommand.BACK_MAIN.value))
    await message.reply(MESSAGES['choose_master'], reply_markup=kb, parse_mode="Markdown")


@router.message(F.text == "📋 Мои записи")
@handle_errors
async def my_appointments_button(message: types.Message):
    if not await rate_limiter_message.is_allowed(message.from_user.id):
        await message.reply(MESSAGES['rate_limit'])
        return
    appointments = await db.get_user_appointments(message.from_user.id)
    if not appointments:
        await message.reply(MESSAGES['no_appointments'])
        return
    text = "📋 *Ваши записи:*\n\n"
    kb = InlineKeyboardMarkup(row_width=1)
    for appt in appointments:
        status = "✅" if appt.status == AppointmentStatus.CONFIRMED else "⏳" if appt.status == AppointmentStatus.PENDING else "❌"
        text += f"{status} #{appt.id} — *{appt.master_name}*\n   {appt.service}\n   {appt.date.strftime('%d.%m.%Y %H:%M')}\n"
        if await db.can_cancel_appointment(appt.id) and appt.status != AppointmentStatus.CANCELLED:
            kb.add(InlineKeyboardButton(f"❌ Отменить #{appt.id}",
                                        callback_data=f"{CallbackCommand.CANCEL_APPT_PREFIX.value}{appt.id}"))
        text += "\n"
    await message.reply(text, reply_markup=kb, parse_mode="Markdown")


@router.message(F.text == "👥 Наши мастера")
@handle_errors
async def masters_button(message: types.Message):
    await cmd_masters(message)


@router.message(F.text == "👑 Панель владельца")
@handle_errors
async def owner_panel(message: types.Message):
    if not await owner_sessions_repo.verify_session(message.from_user.id):
        await message.reply(MESSAGES['no_access'])
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("👥 Мастера", callback_data=CallbackCommand.O_MASTERS.value))
    kb.add(InlineKeyboardButton("📊 Записи", callback_data=CallbackCommand.O_APPTS.value))
    kb.add(InlineKeyboardButton("📈 Статистика", callback_data=CallbackCommand.O_STATS.value))
    kb.add(InlineKeyboardButton("🚪 Выход", callback_data=CallbackCommand.O_LOGOUT.value))
    await message.reply("👑 *Панель владельца*", reply_markup=kb, parse_mode="Markdown")


@router.message(F.text == "👤 Кабинет мастера")
@handle_errors
async def master_panel(message: types.Message):
    user_id = message.from_user.id
    master = await db.get_master_by_telegram_id(user_id)
    if not master:
        await message.reply(MESSAGES['no_access'])
        return
    rating, count = await db.get_master_rating(master.id)
    stars = "⭐" * int(rating) if rating > 0 else "Нет оценок"
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📅 Расписание", callback_data=CallbackCommand.MASTER_SCHED.value),
        InlineKeyboardButton("✅ Мои записи", callback_data=CallbackCommand.MASTER_APPTS.value),
        InlineKeyboardButton("💇 Мои услуги", callback_data=CallbackCommand.MASTER_SERVICES.value),
    )
    await message.reply(f"👤 *{master.name}*\n{stars} ({rating:.1f}, {count})", reply_markup=kb, parse_mode="Markdown")


# ========== CALLBACKS: MASTER ==========
@router.callback_query(F.data == CallbackCommand.MASTER_SCHED.value)
@handle_errors
async def master_schedule_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    master = await db.get_master_by_telegram_id(user_id)
    if not master:
        await callback.answer(MESSAGES['no_access'])
        return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📅 На неделю", callback_data=CallbackCommand.MASTER_WEEK.value),
        InlineKeyboardButton("⏰ Сегодня", callback_data=CallbackCommand.MASTER_TODAY.value),
        InlineKeyboardButton("➕ Добавить смену", callback_data=CallbackCommand.MASTER_ADD_SHIFT.value),
    )
    await callback.message.edit_text("📅 *Расписание*", reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == CallbackCommand.MASTER_WEEK.value)
@handle_errors
async def master_week_schedule(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    master = await db.get_master_by_telegram_id(user_id)
    if not master:
        return
    today = datetime.now()
    date_from = (today - timedelta(days=today.weekday())).date().isoformat()
    date_to = (today + timedelta(days=6 - today.weekday())).date().isoformat()
    schedules = await db.get_master_schedule_range(master.id, date_from, date_to)
    text = "📅 *Расписание на неделю:*\n\n"
    for i in range(7):
        date = today + timedelta(days=i - today.weekday())
        date_str = date.strftime("%A, %d.%m")
        date_iso = date.date().isoformat()
        if date_iso in schedules:
            sch = schedules[date_iso]
            if sch.status == MasterScheduleStatus.WORKING:
                emoji = "✅"
                time_str = f"{sch.start_time}-{sch.end_time}"
            else:
                emoji = "🏖️"
                time_str = "Выходной"
            text += f"{emoji} {date_str}: {time_str}\n"
        else:
            text += f"❓ {date_str}: не установлено\n"
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == CallbackCommand.MASTER_TODAY.value)
@handle_errors
async def master_today_shift(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    master = await db.get_master_by_telegram_id(user_id)
    if not master:
        return
    today = datetime.now().date().isoformat()
    schedule = await db.get_master_schedule(master.id, today)
    text = f"⏰ *Сегодня ({today}):*\n\n"
    if schedule and schedule.status == MasterScheduleStatus.WORKING:
        text += f"✅ Рабочий день\n🕐 {schedule.start_time} - {schedule.end_time}\n"
    elif schedule and schedule.status == MasterScheduleStatus.DAY_OFF:
        text += f"🏖️ Выходной\n"
    else:
        text += f"❓ Расписание не установлено\n"
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == CallbackCommand.MASTER_ADD_SHIFT.value)
@handle_errors
async def master_add_shift_start(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    master = await db.get_master_by_telegram_id(user_id)
    if not master:
        return
    today = datetime.now()
    available_dates = set()
    for i in range(14):
        date = today + timedelta(days=i)
        available_dates.add(date.date().isoformat())
    kb = create_calendar(today.year, today.month, available_dates)
    await state.set_state(MasterShiftStates.selecting_day)
    await callback.message.edit_text("📅 Выберите дату:", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith(CallbackCommand.D_.value), MasterShiftStates.selecting_day)
@handle_errors
async def process_shift_date(callback: types.CallbackQuery, state: FSMContext):
    date_str = callback.data.replace(CallbackCommand.D_.value, "")
    await state.update_data(shift_date=date_str)
    await state.set_state(MasterShiftStates.entering_start)
    await callback.message.edit_text(f"Введите время начала смены (ЧЧ:ММ) для {date_str}:")
    await callback.answer()


@router.message(MasterShiftStates.entering_start)
@handle_errors
async def process_shift_start(message: types.Message, state: FSMContext):
    start_time = message.text.strip()
    # Простая проверка формата
    try:
        datetime.strptime(start_time, "%H:%M")
    except ValueError:
        await message.reply("Неверный формат. Пример: 09:00")
        return
    await state.update_data(shift_start=start_time)
    await state.set_state(MasterShiftStates.entering_end)
    await message.reply("Введите время окончания смены (ЧЧ:ММ):")


@router.message(MasterShiftStates.entering_end)
@handle_errors
async def process_shift_end(message: types.Message, state: FSMContext):
    end_time = message.text.strip()
    try:
        datetime.strptime(end_time, "%H:%M")
    except ValueError:
        await message.reply("Неверный формат. Пример: 18:00")
        return
    data = await state.get_data()
    user_id = message.from_user.id
    master = await db.get_master_by_telegram_id(user_id)
    if not master:
        await state.clear()
        return
    schedule = MasterSchedule(
        master_id=master.id,
        date=data['shift_date'],
        start_time=data['shift_start'],
        end_time=end_time,
        status=MasterScheduleStatus.WORKING
    )
    success = await db.add_master_schedule(schedule)
    if success:
        await slots_cache.invalidate(master.id)
        await message.reply("✅ Смена добавлена!")
    else:
        await message.reply("❌ Ошибка")
    await state.clear()


@router.callback_query(F.data == CallbackCommand.MASTER_APPTS.value)
@handle_errors
async def master_appointments(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    master = await db.get_master_by_telegram_id(user_id)
    if not master:
        return
    appointments = await db.get_master_appointments(master.id, datetime.now())
    if not appointments:
        await callback.message.edit_text("📭 Нет предстоящих записей")
        await callback.answer()
        return
    text = "✅ *Мои записи:*\n\n"
    for appt in appointments[:10]:
        text += f"#{appt.id} — *{appt.user_name}*\n   {appt.service}\n   {appt.date.strftime('%d.%m.%Y %H:%M')}\n\n"
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == CallbackCommand.MASTER_SERVICES.value)
@handle_errors
async def master_services_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    master = await db.get_master_by_telegram_id(user_id)
    if not master:
        return
    masters = await masters_cache.get()
    services = next((s for m, s in masters if m.id == master.id), [])
    text = "💇 *Мои услуги:*\n\n"
    kb = InlineKeyboardMarkup(row_width=1)
    for srv in services:
        status = "✅" if srv.is_available else "❌"
        text += f"{status} {srv.service} ({srv.duration_minutes} мин)\n"
        kb.add(InlineKeyboardButton(
            f"{'Скрыть' if srv.is_available else 'Показать'} '{srv.service}'",
            callback_data=f"toggle_service_{srv.id}"
        ))
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("toggle_service_"))
@handle_errors
async def toggle_service(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    master = await db.get_master_by_telegram_id(user_id)
    if not master:
        return
    service_id = int(callback.data.split("_")[2])
    masters = await masters_cache.get()
    service = None
    for m, services in masters:
        if m.id == master.id:
            service = next((s for s in services if s.id == service_id), None)
            break
    if not service:
        await callback.answer("Услуга не найдена")
        return
    new_avail = not service.is_available
    success = await db.toggle_service_availability(master.id, service_id, new_avail)
    if success:
        await masters_cache.invalidate()
        await callback.answer(f"Услуга {'показана' if new_avail else 'скрыта'}")
        await master_services_menu(callback)
    else:
        await callback.answer("Ошибка")


# ========== CALLBACKS: КЛИЕНТ ==========
@router.callback_query(F.data.startswith(CallbackCommand.M_.value), ClientAppointmentStates.choosing_master)
@handle_errors
async def process_master(callback: types.CallbackQuery, state: FSMContext):
    master_id = int(callback.data.replace(CallbackCommand.M_.value, ""))
    masters = await masters_cache.get()
    master_data = next(((m, s) for m, s in masters if m.id == master_id), None)
    if not master_data:
        await callback.answer("Мастер не найден")
        return
    master, services = master_data
    await state.update_data(master_id=master_id, master_name=master.name)
    kb = InlineKeyboardMarkup(row_width=1)
    for srv in services:
        if srv.is_available:
            kb.add(InlineKeyboardButton(
                f"{srv.service} ({srv.duration_minutes} мин)",
                callback_data=f"{CallbackCommand.S_.value}{srv.id}"
            ))
    kb.add(InlineKeyboardButton("🔙 Назад", callback_data=CallbackCommand.BACK_M.value))
    await state.set_state(ClientAppointmentStates.choosing_service)
    await callback.message.edit_text(f"💅 *{master.name}*\n\n{MESSAGES['choose_service']}", reply_markup=kb,
                                     parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith(CallbackCommand.S_.value), ClientAppointmentStates.choosing_service)
@handle_errors
async def process_service(callback: types.CallbackQuery, state: FSMContext):
    service_id = int(callback.data.replace(CallbackCommand.S_.value, ""))
    data = await state.get_data()
    master_id = data['master_id']
    masters = await masters_cache.get()
    services = next((s for m, s in masters if m.id == master_id), [])
    service = next((s for s in services if s.id == service_id), None)
    if not service:
        await callback.answer("Услуга не найдена")
        return
    await state.update_data(service_id=service_id, service=service.service, duration=service.duration_minutes)
    await state.set_state(ClientAppointmentStates.choosing_date)
    # Показываем календарь
    today = datetime.now()
    available_dates = set()
    for i in range(14):
        date = today + timedelta(days=i)
        slots = await db.get_available_slots(master_id, date.date().isoformat(), service.duration_minutes)
        if slots:
            available_dates.add(date.date().isoformat())
    kb = create_calendar(today.year, today.month, available_dates)
    await callback.message.edit_text(MESSAGES['choose_date'], reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith(CallbackCommand.D_.value), ClientAppointmentStates.choosing_date)
@handle_errors
async def process_date(callback: types.CallbackQuery, state: FSMContext):
    date_str = callback.data.replace(CallbackCommand.D_.value, "")
    data = await state.get_data()
    master_id = data['master_id']
    duration = data['duration']
    slots = await slots_cache.get(master_id, date_str, duration, db.get_available_slots)
    if not slots:
        await callback.answer(MESSAGES['no_available_slots'])
        return
    await state.update_data(appointment_date=date_str)
    kb = InlineKeyboardMarkup(row_width=2)
    for start, end in slots[:16]:
        kb.add(InlineKeyboardButton(start, callback_data=f"{CallbackCommand.T_.value}{date_str}_{start}"))
    kb.add(InlineKeyboardButton("🔙 Назад", callback_data=CallbackCommand.BACK_D.value))
    await state.set_state(ClientAppointmentStates.choosing_time)
    await callback.message.edit_text(MESSAGES['choose_time'], reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith(CallbackCommand.T_.value), ClientAppointmentStates.choosing_time)
@handle_errors
async def process_time(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.replace(CallbackCommand.T_.value, "").split("_")
    date_str = parts[0]
    time_str = parts[1]
    data = await state.get_data()
    appointment_datetime = datetime.fromisoformat(f"{date_str} {time_str}")
    await state.update_data(appointment_datetime=appointment_datetime)
    confirm_text = MESSAGES['confirm'].format(
        data['master_name'],
        data['service'],
        data['duration'],
        appointment_datetime.strftime("%d.%m.%Y %H:%M")
    )
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("✅ Да", callback_data="confirm_yes"),
           InlineKeyboardButton("❌ Нет", callback_data="confirm_no"))
    await state.set_state(ClientAppointmentStates.confirming)
    await callback.message.edit_text(confirm_text, reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == "confirm_yes", ClientAppointmentStates.confirming)
@handle_errors
async def confirm_appointment(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    appointment = Appointment(
        user_id=callback.from_user.id,
        user_name=callback.from_user.full_name or f"User{callback.from_user.id}",
        master_id=data['master_id'],
        master_name=data['master_name'],
        service=data['service'],
        duration_minutes=data['duration'],
        date=data['appointment_datetime'],
        status=AppointmentStatus.PENDING
    )
    appt_id = await db.add_appointment(appointment)
    if appt_id:
        await slots_cache.invalidate(data['master_id'])
        await callback.message.edit_text(MESSAGES['success'].format(appt_id))
    else:
        await callback.message.edit_text("❌ Ошибка создания записи")
    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "confirm_no", ClientAppointmentStates.confirming)
@handle_errors
async def cancel_confirmation(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(MESSAGES['cancelled'])
    await callback.answer()


@router.callback_query(F.data == CallbackCommand.BACK_M.value, ClientAppointmentStates.choosing_service)
@handle_errors
async def back_to_masters(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(ClientAppointmentStates.choosing_master)
    # Перегенерируем клавиатуру мастеров
    masters = await masters_cache.get()
    kb = InlineKeyboardMarkup(row_width=1)
    for master, _ in masters:
        kb.add(InlineKeyboardButton(f"💅 {master.name}", callback_data=f"{CallbackCommand.M_.value}{master.id}"))
    kb.add(InlineKeyboardButton("🔙 Главное меню", callback_data=CallbackCommand.BACK_MAIN.value))
    await callback.message.edit_text(MESSAGES['choose_master'], reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == CallbackCommand.BACK_D.value, ClientAppointmentStates.choosing_time)
@handle_errors
async def back_to_dates(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(ClientAppointmentStates.choosing_date)
    # Вернуть календарь
    await callback.answer("Вернитесь к выбору даты")


@router.callback_query(F.data == CallbackCommand.BACK_MAIN.value)
@handle_errors
async def back_to_main(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    role = await get_user_role(callback.from_user.id)
    await callback.message.delete()
    await callback.message.answer(MESSAGES['main_menu'], reply_markup=get_main_keyboard(role))
    await callback.answer()


# ========== КАЛЕНДАРЬ НАВИГАЦИЯ ==========
@router.callback_query(F.data.startswith("cal_prev_"))
@handle_errors
async def cal_prev(callback: types.CallbackQuery, state: FSMContext):
    _, _, year, month = callback.data.split("_")
    year, month = int(year), int(month)
    if month == 1:
        month = 12
        year -= 1
    else:
        month -= 1
    # Определяем доступные даты из состояния
    data = await state.get_data()
    master_id = data.get('master_id')
    duration = data.get('duration')
    available_dates = set()
    if master_id and duration:
        for i in range(14):
            date = datetime(year, month, 1) + timedelta(days=i)
            slots = await db.get_available_slots(master_id, date.date().isoformat(), duration)
            if slots:
                available_dates.add(date.date().isoformat())
    kb = create_calendar(year, month, available_dates)
    await callback.message.edit_reply_markup(reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("cal_next_"))
@handle_errors
async def cal_next(callback: types.CallbackQuery, state: FSMContext):
    _, _, year, month = callback.data.split("_")
    year, month = int(year), int(month)
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
    data = await state.get_data()
    master_id = data.get('master_id')
    duration = data.get('duration')
    available_dates = set()
    if master_id and duration:
        for i in range(14):
            date = datetime(year, month, 1) + timedelta(days=i)
            slots = await db.get_available_slots(master_id, date.date().isoformat(), duration)
            if slots:
                available_dates.add(date.date().isoformat())
    kb = create_calendar(year, month, available_dates)
    await callback.message.edit_reply_markup(reply_markup=kb)
    await callback.answer()


# ========== ОТМЕНА ЗАПИСИ ==========
@router.callback_query(F.data.startswith(CallbackCommand.CANCEL_APPT_PREFIX.value))
@handle_errors
async def cancel_appointment(callback: types.CallbackQuery):
    appt_id = int(callback.data.split("_")[1])
    appointment = await db.get_appointment_by_id(appt_id)
    if not appointment or appointment.user_id != callback.from_user.id:
        await callback.answer("Запись не найдена")
        return
    if not await db.can_cancel_appointment(appt_id):
        await callback.answer("Слишком поздно отменять")
        return
    success = await db.update_appointment_status(appt_id, AppointmentStatus.CANCELLED)
    if success:
        await slots_cache.invalidate(appointment.master_id)
        await callback.message.edit_text("✅ Запись отменена")
    else:
        await callback.answer("Ошибка")


# ========== ВЕБ-СЕРВЕР ==========
async def health_check(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def start_web_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"🌐 Веб-сервер на порту {port}")


# ========== НАПОМИНАНИЯ ==========
async def send_reminders():
    while True:
        try:
            await asyncio.sleep(60)
            # Упрощённая версия: проверяем записи за 24 часа
            if settings.reminder_24h_enabled:
                now = datetime.now()
                time_from = now + timedelta(hours=23, minutes=50)
                time_to = now + timedelta(hours=24, minutes=10)
                async with aiosqlite.connect(settings.database_url) as db:
                    async with db.execute(
                            """SELECT user_id, id, master_name, service, date FROM appointments 
                               WHERE date BETWEEN ? AND ? AND status = ?""",
                            (time_from.isoformat(), time_to.isoformat(), AppointmentStatus.PENDING.value)
                    ) as cursor:
                        rows = await cursor.fetchall()
                        for row in rows:
                            user_id, appt_id, master_name, service, date_str = row
                            if not await db.is_reminder_sent(appt_id, "24h"):
                                try:
                                    await bot.send_message(user_id,
                                                           f"📢 Напоминание о записи завтра: {master_name} - {service} в {datetime.fromisoformat(date_str).strftime('%H:%M')}")
                                    await db.mark_reminder_sent(appt_id, "24h")
                                except Exception as e:
                                    logger.warning(f"Не удалось отправить напоминание: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка в reminder: {e}")


# ========== GRACEFUL SHUTDOWN ==========
shutdown_event = asyncio.Event()


async def cleanup_expired_sessions():
    while not shutdown_event.is_set():
        await asyncio.sleep(3600)
        await owner_sessions_repo.cleanup_expired()


# ========== MAIN ==========
async def main():
    await db.init_db()
    await start_web_server()

    tasks = [
        asyncio.create_task(cleanup_expired_sessions()),
        asyncio.create_task(rate_limiter_message.cleanup_old_entries()),
        asyncio.create_task(rate_limiter_callback.cleanup_old_entries()),
        asyncio.create_task(send_reminders()),
    ]

    dp.include_router(router)
    logger.info("🤖 Бот запущен")

    try:
        await dp.start_polling(bot, skip_updates=True)
    except KeyboardInterrupt:
        logger.info("⏹️ Остановка")
        shutdown_event.set()
        for task in tasks:
            task.cancel()
            try:
                await task
            except:
                pass


if __name__ == '__main__':
    asyncio.run(main())