import json, asyncio, os, logging, warnings, time, re
from datetime import datetime
from typing import Optional
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from playwright.async_api import async_playwright
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup

# ─────────────────────────────────────────
# LOGS
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=UserWarning)

# ─────────────────────────────────────────
# SOZLAMALAR — barchasi shu yerda
# ─────────────────────────────────────────
TOKEN     = "8442363419:AAFkWt3a77-QISXcbNTATPWyCohRUAeUgj4"
SHEET_ID  = "1vZLVKA__HPQAL70HfzI0eYu3MpsE-Namho6D-2RLIYw"
ADMIN_IDS = [7693087447]   # ← o'z Telegram ID ingizni yozing

CREDENTIALS_FILE = "credentials.json"

GOOGLE_CREDS_DICT = {
    "type": "service_account",
    "project_id": "tsuebaza",
    "private_key_id": "f6ce26b4270e74d332e2521f32b1e36708709f32",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC6xxzpzHtF93hgg1SVXOg8oqhxIMAT93a9H8JmXwbuO9FIEc5Vac9L+vHxR4XzIYUWT54qreoU03uGVJs3HuaOq+cMfKW/vUeDoy8XMDWb1sMys6l88zoMPjl4qTOMuspHwElhxHMQzCqrzvIGsGU2/lnYo6dT1X7V2M5Sy1ELeNMlrN5VjzqrzpnZo592violqgarJmLFwuZTDiZBS0ziery7ENxJYHlSXtKSesh6Kr/HCw9FmtCvTY24bAGPiJ7rTguqr/Bd9p3nT8orGYTmYMWdufMrJAaOxvPi4g3+wAh1yPKjYhAk9N2UaYlZhpwgGYJG6zcHP3FZAfy8h4tJAgMBAAECggEAC8FQWTPyEOXnAPFYYiDQylAqbcRJPSMhU/yAYv+uvfusdkQN9Q44amhxexAB/DAAjPZB1snPUQmRB/6h2NxBrSULNkaktTvtGPnXkuFeiQpWirBdWxcrxhgnIdL0WEfBR2FwvOMnosesFS1sdMNRUMln+jHX7CIkXhfLvyY7WY9gxOlOS3QRCd52Mr+LkZ7KHsPl8gqPsD7e3UNqJj/6gJf9tXa+1H37moPjT8nxjNnm164mWRVxTzzJ8+W/W5wSOIOewSGZtFQwnuiK3GkB0CPQgb8dqlu8udcGbnzeZ4wnibqEQD+AkJyWHq+lf6j8ok1F+GEFD2JPMEXWXR/X6QKBgQD3Gz2Pks7z+utCeF/IYZMUkS0qFxFUdveBCLMMkqWI9U2OrPC9k5XZgFoB4+ZZVbgjCWdHF0WezHGKa7g7gFBLXisXFcWmpI4pLnK2ZXLl9NeSNGBXjz3ELQTooKWOK6NzKtnYz82r/xvlH4F7/o1E4iB8sK+XBPRrZvbeFIgh3wKBgQDBgAYX+MRiIMLlxCF6PjEuzmoddiKaXEpFi8F2WKhzFN4sGlhKsyz4wA0kiC/OAxclhJoZFFHRpGpRS1BkCSG0IWaHN4FNyJmYJ85cfse1bD36gP/wVyr18XndkTorDv2wm906LWIb5XnhySopSVvMzBFAfO5zQVl4O/ZEf3oH1wKBgQClGDQXGpTeeUR5UodcD6T+1sgAGQ1hC2LteOx3d/rU+F9q9ayaXGJ/wLtiC+u4wAaYkg3Id4nyQNGTE8kxJ9d9o1rSv58gaUz5wqitqDYN4XmtmwTLO06fJRzkd4Bk7k70ASsvKhdV1MnEWTpfp4CnBxWBeWK3WX4by90gH0OcrQKBgQCUZzLFOdasB8JSY+zlC0O/STIbbFo3luajlf8HXBG9SVjn7ECg8fBgyzKJPSWk07vx21QwPeROa9HajW+XeWVu+FrhA5HY7SvXhVgAmn9iRiT2te8VDlV1kQDNDK/XDEVHKRMvplTjXkjvANCkH44XcL8efXKH1KvgyeQjPr2BuwKBgCpzMaE2EFz3U94HGKpkmiDHRA8Y8vba/351dwuLX8WgqBFmhAvCMqI1IznSTGzBNUvKf0N7Ubwj2NkLzgYDWApDVlLhNAn2Z7BGqf6zrdUy8y7P/LK2BIGRwGHnVdOUAsjUyQIynfGGFj8NxFbxAN25mWEGUpHo4C7eJAbzfCsd\n-----END PRIVATE KEY-----\n",
    "client_email": "tsuebaza@tsuebaza.iam.gserviceaccount.com",
    "client_id": "115326693502707680364",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/tsuebaza%40tsuebaza.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

TALABA_JSON  = "talaba.json"
USTOZ_JSON   = "ustoz.json"
XONALAR_JSON = "xonalar.json"

TASHKENT_TZ = pytz.timezone("Asia/Tashkent")

PARA_TIMES = {
    1: ("08:30", "09:50"),
    2: ("10:00", "11:20"),
    3: ("11:30", "12:50"),
    4: ("13:30", "14:50"),
    5: ("15:00", "16:20"),
    6: ("16:30", "17:50"),
    7: ("18:00", "19:20"),
    8: ("19:30", "20:50"),
}

DAYS_UZ = {0:"Dushanba",1:"Seshanba",2:"Chorshanba",3:"Payshanba",4:"Juma",5:"Shanba",6:"Yakshanba"}
DAYS_RU = {0:"Понедельник",1:"Вторник",2:"Среда",3:"Четверг",4:"Пятница",5:"Суббота",6:"Воскресенье"}

# ─────────────────────────────────────────
# BOT & DISPATCHER
# ─────────────────────────────────────────
bot       = Bot(token=TOKEN)
dp        = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TASHKENT_TZ)
BOT_START_TIME = datetime.now(TASHKENT_TZ)

# ─────────────────────────────────────────
# FSM STATES
# ─────────────────────────────────────────
class TeacherFlow(StatesGroup):
    search = State()

class FreeRoomFlow(StatesGroup):
    time_input = State()

class AutoSchedule(StatesGroup):
    day  = State()
    time = State()

class SaveFlow(StatesGroup):
    naming = State()

class BroadcastFlow(StatesGroup):
    waiting = State()

# ─────────────────────────────────────────
# IN-MEMORY CACHE
# ─────────────────────────────────────────
screenshot_cache: dict = {}
user_lang: dict        = {}
last_msgs: dict        = {}
pending_save: dict     = {}

# ─────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────
_sheets: dict = {}

SHEET_NAMES = {
    "users":      "foydalanuvchilar",
    "auto":       "guruh_avto",
    "teachers":   "ustozlar",
    "free_rooms": "bosh_xonalar",
    "logs":       "loglar",
    "saved":      "saqlangan_jadvallar",
}

def init_sheets():
    global _sheets
    if not SHEET_ID: return
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(GOOGLE_CREDS_DICT, scope)
        log.info("Sheets: ulandi")

        client = gspread.authorize(creds)
        wb     = client.open_by_key(SHEET_ID)
        existing = [s.title for s in wb.worksheets()]
        for key, name in SHEET_NAMES.items():
            if name not in existing:
                wb.add_worksheet(title=name, rows=2000, cols=20)
            _sheets[key] = wb.worksheet(name)
        _ensure_header("users",      ["Vaqt","UserID","Username","Ism","Tur","Ma'lumot","URL"])
        _ensure_header("auto",       ["ChatID","ChatTitle","GroupName","URL","Kun","Vaqt","QoshilganVaqt"])
        _ensure_header("teachers",   ["Vaqt","UserID","Username","Ism","Ustoz","URL"])
        _ensure_header("free_rooms", ["Sana","Xona","Bino","VaqtOraliqi","Para"])
        _ensure_header("logs",       ["Vaqt","UserID","Action","Ma'lumot"])
        _ensure_header("saved",      ["UserID","Nom","URL","Tur","QoshilganVaqt"])
        log.info("Google Sheets ulandi ✅")
    except Exception as e:
        log.error(f"Sheets init error: {e}")

def _ensure_header(key: str, headers: list):
    try:
        ws = _sheets.get(key)
        if not ws: return
        if not ws.row_values(1):
            ws.append_row(headers)
    except: pass

def sheet_append(key: str, row: list):
    try:
        ws = _sheets.get(key)
        if ws: ws.append_row(row)
    except Exception as e:
        log.error(f"Sheet write [{key}]: {e}")

def save_user(user: types.User, tur: str, malumot: str, url: str):
    ws = _sheets.get("users")
    if not ws: return
    try:
        now   = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
        uname = f"@{user.username}" if user.username else "-"
        row   = [now, str(user.id), uname, user.full_name, tur, malumot, url]
        ids   = ws.col_values(2)
        if str(user.id) in ids:
            ws.update(f"A{ids.index(str(user.id))+1}:G{ids.index(str(user.id))+1}", [row])
        else:
            ws.append_row(row)
    except Exception as e:
        log.error(f"save_user: {e}")

def save_teacher_visit(user: types.User, teacher: str, url: str):
    ws = _sheets.get("teachers")
    if not ws: return
    try:
        now   = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
        uname = f"@{user.username}" if user.username else "-"
        row   = [now, str(user.id), uname, user.full_name, teacher, url]
        ids   = ws.col_values(2)
        if str(user.id) in ids:
            ws.update(f"A{ids.index(str(user.id))+1}:F{ids.index(str(user.id))+1}", [row])
        else:
            ws.append_row(row)
    except Exception as e:
        log.error(f"save_teacher: {e}")

def save_auto_schedule(chat_id: int, chat_title: str, group: str, url: str, day: int, vaqt: str):
    ws = _sheets.get("auto")
    if not ws: return
    try:
        now = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
        row = [str(chat_id), chat_title, group, url, str(day), vaqt, now]
        ids = ws.col_values(1)
        if str(chat_id) in ids:
            ws.update(f"A{ids.index(str(chat_id))+1}:G{ids.index(str(chat_id))+1}", [row])
        else:
            ws.append_row(row)
    except Exception as e:
        log.error(f"save_auto: {e}")

def get_auto_schedules() -> list:
    ws = _sheets.get("auto")
    if not ws: return []
    try:
        return [r for r in ws.get_all_values()[1:] if len(r) >= 6 and r[0]]
    except: return []

def add_log(user_id: int, action: str, data: str = ""):
    now = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
    sheet_append("logs", [now, str(user_id), action, data])

# ─────────────────────────────────────────
# SAQLANGAN JADVALLAR
# ─────────────────────────────────────────
def get_saved_schedules(user_id: int) -> list:
    ws = _sheets.get("saved")
    if not ws: return []
    try:
        return [(r[1], r[2], r[3]) for r in ws.get_all_values()[1:] if r and r[0] == str(user_id)]
    except: return []

def save_schedule(user_id: int, nom: str, url: str, tur: str):
    ws = _sheets.get("saved")
    if not ws: return
    try:
        now = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
        ws.append_row([str(user_id), nom, url, tur, now])
    except Exception as e:
        log.error(f"save_schedule: {e}")

def delete_saved_schedule(user_id: int, nom: str):
    ws = _sheets.get("saved")
    if not ws: return
    try:
        rows = ws.get_all_values()
        for i, r in enumerate(rows):
            if r and r[0] == str(user_id) and r[1] == nom:
                ws.delete_rows(i + 1)
                break
    except Exception as e:
        log.error(f"delete_saved: {e}")

def get_all_user_ids() -> list:
    ws = _sheets.get("users")
    if not ws: return []
    try:
        return [int(i) for i in ws.col_values(2)[1:] if i.strip().isdigit()]
    except: return []

# ─────────────────────────────────────────
# JSON YUKLASH
# ─────────────────────────────────────────
def load_json(path: str) -> dict:
    if not os.path.exists(path): return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ─────────────────────────────────────────
# TIL
# ─────────────────────────────────────────
def lang(chat_id: int) -> str:
    return user_lang.get(chat_id, "uz")

T = {
    "uz": {
        "main_menu":     "🏠 Asosiy menyu\n\nNimani izlayapsiz?",
        "student":       "🎓 Talaba",
        "teacher":       "👨‍🏫 O'qituvchi",
        "rooms":         "🏛 Xonalar",
        "free_rooms":    "🔍 Bosh xonalar",
        "my_saved":      "⭐ Saqlangan jadvallar",
        "select_fak":    "📚 Fakultetni tanlang:",
        "select_kurs":   "📖 Kursni tanlang:",
        "select_group":  "👥 Guruhni tanlang:",
        "teacher_ask":   "👨‍🏫 Ustoz ismini yozing:\n(masalan: Karimov Sherali)",
        "teacher_found": "✅ Topildi! Yuklanmoqda...",
        "teacher_not":   "❌ Ustoz topilmadi. Qayta urinib ko'ring.",
        "teachers_list": "Bir nechta ustoz topildi:\n\n",
        "select_bino":   "🏢 Binoni tanlang:",
        "select_xona":   "🚪 Xonani tanlang:",
        "free_ask":      (
            "⏰ Qaysi vaqtga bosh xona izlaysiz?\n\n"
            "Format: 13:30-14:50\n\n"
            "Mavjud paralar:\n"
            "1️⃣ 08:30-09:50\n2️⃣ 10:00-11:20\n3️⃣ 11:30-12:50\n"
            "4️⃣ 13:30-14:50\n5️⃣ 15:00-16:20\n6️⃣ 16:30-17:50\n"
            "7️⃣ 18:00-19:20\n8️⃣ 19:30-20:50"
        ),
        "free_invalid":  "❌ Format noto'g'ri. Masalan: 13:30-14:50",
        "free_none":     "😔 Bu vaqtga bosh xona topilmadi yoki ma'lumotlar yangilanmagan.\n\n💡 Admin /scan_rooms bilan yangilay oladi.",
        "free_title":    "🔍 *{time}* ga bosh xonalar:\n\n",
        "loading":       "⏳ Yuklanmoqda...",
        "error":         "❌ Xatolik yuz berdi. Qaytadan urinib ko'ring.",
        "back":          "⬅️ Orqaga",
        "menu_btn":      "🏠 Menyu",
        "select_day":    "📅 Qaysi kuni jadval yuborilsin?",
        "select_time":   "⏰ Vaqtni tanlang yoki yozing (HH:MM):\nHozir: {now}",
        "auto_saved":    "✅ Sozlandi! Har *{day}* kuni soat *{time}* da jadval yuboriladi.",
        "time_invalid":  "❌ Format noto'g'ri. HH:MM shaklida yozing (14:20)",
        "caption":       "📅 *{name}*\n\n🤖 @tsuetimebot",
        "free_updated":  "✅ Bosh xonalar yangilandi: {count} ta xona.",
        "saved_btn":     "⭐ Saqlash",
        "save_ask_name": "📝 Jadval uchun nom yozing:\n💡 Standart: *{default}*",
        "save_ok":       "✅ *{name}* nomi bilan saqlandi!",
        "save_list":     "⭐ *Saqlangan jadvallaringiz:*\n\n",
        "save_empty":    "😔 Hali hech narsa saqlanmagan.\n\nJadval ko'rganda '⭐ Saqlash' tugmasini bosing.",
        "save_deleted":  "🗑 *{name}* o'chirildi.",
    },
    "ru": {
        "main_menu":     "🏠 Главное меню\n\nЧто ищете?",
        "student":       "🎓 Студент",
        "teacher":       "👨‍🏫 Преподаватель",
        "rooms":         "🏛 Кабинеты",
        "free_rooms":    "🔍 Свободные кабинеты",
        "my_saved":      "⭐ Сохранённые расписания",
        "select_fak":    "📚 Выберите факультет:",
        "select_kurs":   "📖 Выберите курс:",
        "select_group":  "👥 Выберите группу:",
        "teacher_ask":   "👨‍🏫 Введите имя преподавателя:\n(пример: Karimov Sherali)",
        "teacher_found": "✅ Найдено! Загружается...",
        "teacher_not":   "❌ Преподаватель не найден. Попробуйте снова.",
        "teachers_list": "Найдено несколько преподавателей:\n\n",
        "select_bino":   "🏢 Выберите корпус:",
        "select_xona":   "🚪 Выберите кабинет:",
        "free_ask":      (
            "⏰ На какое время ищете свободный кабинет?\n\n"
            "Формат: 13:30-14:50\n\n"
            "Доступные пары:\n"
            "1️⃣ 08:30-09:50\n2️⃣ 10:00-11:20\n3️⃣ 11:30-12:50\n"
            "4️⃣ 13:30-14:50\n5️⃣ 15:00-16:20\n6️⃣ 16:30-17:50\n"
            "7️⃣ 18:00-19:20\n8️⃣ 19:30-20:50"
        ),
        "free_invalid":  "❌ Неверный формат. Пример: 13:30-14:50",
        "free_none":     "😔 Свободных кабинетов не найдено или данные не обновлены.\n\n💡 Администратор может обновить командой /scan_rooms.",
        "free_title":    "🔍 *{time}* — свободные кабинеты:\n\n",
        "loading":       "⏳ Загружается...",
        "error":         "❌ Произошла ошибка. Попробуйте снова.",
        "back":          "⬅️ Назад",
        "menu_btn":      "🏠 Меню",
        "select_day":    "📅 В какой день отправлять расписание?",
        "select_time":   "⏰ Выберите или введите время (ЧЧ:ММ):\nСейчас: {now}",
        "auto_saved":    "✅ Настроено! Каждый *{day}* в *{time}* будет отправляться расписание.",
        "time_invalid":  "❌ Неверный формат. Введите ЧЧ:ММ (пример: 14:20)",
        "caption":       "📅 *{name}*\n\n🤖 @tsuetimebot",
        "free_updated":  "✅ Свободные кабинеты обновлены: {count} кабинетов.",
        "saved_btn":     "⭐ Сохранить",
        "save_ask_name": "📝 Введите название:\n💡 По умолчанию: *{default}*",
        "save_ok":       "✅ Сохранено как *{name}*!",
        "save_list":     "⭐ *Сохранённые расписания:*\n\n",
        "save_empty":    "😔 Ничего не сохранено.\n\nПри просмотре расписания нажмите '⭐ Сохранить'.",
        "save_deleted":  "🗑 *{name}* удалено.",
    }
}

def tr(key: str, chat_id: int, **kw) -> str:
    lg = lang(chat_id)
    text = T[lg].get(key, key)
    if kw:
        try: text = text.format(**kw)
        except: pass
    return text

# ─────────────────────────────────────────
# KLAVIATURALAR
# ─────────────────────────────────────────
def menu_kb(chat_id: int):
    lg = lang(chat_id)
    kb = InlineKeyboardBuilder()
    kb.row(
        types.InlineKeyboardButton(text=T[lg]["student"],    callback_data="menu_student"),
        types.InlineKeyboardButton(text=T[lg]["teacher"],    callback_data="menu_teacher"),
    )
    kb.row(
        types.InlineKeyboardButton(text=T[lg]["rooms"],      callback_data="menu_rooms"),
        types.InlineKeyboardButton(text=T[lg]["free_rooms"], callback_data="menu_free"),
    )
    kb.row(
        types.InlineKeyboardButton(text=T[lg]["my_saved"],   callback_data="menu_saved"),
    )
    return kb.as_markup()

def back_kb(chat_id: int, cb: str = "go_menu"):
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data=cb))
    return kb.as_markup()

# ─────────────────────────────────────────
# SCREENSHOT
# ─────────────────────────────────────────
async def take_screenshot(url: str, filename: str):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page(viewport={"width": 1280, "height": 800}, device_scale_factor=2)
        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.add_style_tag(content=".no-print,.main-menu,.footer,#header,.navigation{display:none!important}")
            target = await page.query_selector(".tt-grid-container, #PRINT_SCENE_BG_1")
            if target:
                await target.screenshot(path=filename)
            else:
                await page.screenshot(path=filename, full_page=False)
        finally:
            await browser.close()

async def get_photo_id(chat_id: int, url: str) -> Optional[str]:
    now = time.time()
    if url in screenshot_cache and (now - screenshot_cache[url]["time"]) < 3600:
        return screenshot_cache[url]["id"]
    fname = f"/tmp/sc_{chat_id}_{int(now)}.png"
    try:
        await take_screenshot(url, fname)
        msg = await bot.send_photo(chat_id, types.FSInputFile(fname), caption="⏳", disable_notification=True)
        fid = msg.photo[-1].file_id
        screenshot_cache[url] = {"id": fid, "time": now}
        await msg.delete()
        return fid
    except Exception as e:
        log.error(f"Screenshot error: {e}")
        return None
    finally:
        if os.path.exists(fname): os.remove(fname)

# ─────────────────────────────────────────
# XABAR O'CHIRISH
# ─────────────────────────────────────────
async def delete_last(chat_id: int):
    for key in ["last_pic", "last_msg"]:
        mid = last_msgs.get(chat_id, {}).get(key)
        if mid:
            try: await bot.delete_message(chat_id, mid)
            except: pass
            last_msgs.setdefault(chat_id, {})[key] = None

# ─────────────────────────────────────────
# JADVAL YUBORISH
# ─────────────────────────────────────────
async def send_schedule(chat_id: int, url: str, name: str,
                         user: types.User = None, tur: str = "talaba"):
    last_msgs.setdefault(chat_id, {})
    await delete_last(chat_id)
    lg = lang(chat_id)
    status = await bot.send_message(chat_id, T[lg]["loading"])

    kb = InlineKeyboardBuilder()
    if user and tur in ("talaba", "ustoz"):
        pending_save[user.id] = {"name": name, "url": url, "tur": tur}
        kb.row(types.InlineKeyboardButton(text=T[lg]["saved_btn"], callback_data="dosave_prompt"))
    kb.row(types.InlineKeyboardButton(text=T[lg]["menu_btn"], callback_data="go_menu"))

    try:
        fid = await get_photo_id(chat_id, url)
        if fid:
            sent = await bot.send_photo(
                chat_id, fid,
                caption=T[lg]["caption"].format(name=name),
                reply_markup=kb.as_markup(), parse_mode="Markdown"
            )
            last_msgs[chat_id]["last_pic"] = sent.message_id
        else:
            await bot.send_message(chat_id, T[lg]["error"], reply_markup=back_kb(chat_id))
    except Exception as e:
        log.error(f"send_schedule error: {e}")
        await bot.send_message(chat_id, T[lg]["error"], reply_markup=back_kb(chat_id))
    finally:
        try: await status.delete()
        except: pass

    if user:
        if tur == "talaba":   save_user(user, tur, name, url)
        elif tur == "ustoz":  save_teacher_visit(user, name, url)
        elif tur == "xona":   save_user(user, tur, name, url)
        add_log(user.id, f"view_{tur}", name)

# ─────────────────────────────────────────
# BOSH XONA ANIQLASH — optimallashtirilgan
# ─────────────────────────────────────────
DAY_X  = [55, 145, 235, 325, 415, 505]
PARA_Y = [250, 414, 578, 742, 906, 1070, 1234, 1398]
COL_W, ROW_H = 90, 164

def parse_svg_html(html: str) -> dict:
    """HTML dan bosh/band vaqtlarni aniqlash"""
    result = {i: list(range(1, 9)) for i in range(6)}
    soup = BeautifulSoup(html, "html.parser")
    svg  = soup.find("g", {"id": "PRINT_SCENE_BG_1"})
    if not svg: return result
    occupied = set()
    for rect in svg.find_all("rect"):
        try:
            x = float(rect.get("x", 0)); y = float(rect.get("y", 0))
            w = float(rect.get("width", 0)); h = float(rect.get("height", 0))
            fill = rect.get("fill", "transparent")
            if fill in ["transparent", "none", ""] or w < 50 or h < 50: continue
            for di, dx in enumerate(DAY_X):
                if abs(x - dx) < COL_W * 0.6:
                    for pi, py in enumerate(PARA_Y):
                        if abs(y - py) < ROW_H * 0.6:
                            occupied.add((di, pi + 1)); break
                    break
        except: continue
    for di in range(6):
        result[di] = [p for p in range(1, 9) if (di, p) not in occupied]
    return result

async def parse_free_room(url: str, room_name: str) -> dict:
    """Eski interfeys — bitta xona uchun (skaner tashqarisida ishlatilsa)"""
    result = {i: list(range(1, 9)) for i in range(6)}
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
            )
            page = await browser.new_page(viewport={"width": 1280, "height": 800})
            await page.goto(url, wait_until="networkidle", timeout=60000)
            html = await page.content()
            await browser.close()
        return parse_svg_html(html)
    except Exception as e:
        log.error(f"parse_free_room [{room_name}]: {e}")
    return result

async def _scan_page(sem, page, room_name: str, url: str) -> tuple:
    """Bitta sahifani parallel skanerlash"""
    async with sem:
        try:
            await page.goto(url, wait_until="networkidle", timeout=45000)
            html = await page.content()
            return room_name, parse_svg_html(html)
        except Exception as e:
            log.error(f"Scan [{room_name}]: {e}")
            return room_name, {i: list(range(1, 9)) for i in range(6)}

async def job_scan_free_rooms():
    log.info("Bosh xona skanerlash boshlandi...")
    xonalar = load_json(XONALAR_JSON)
    if not xonalar:
        log.warning("xonalar.json bo'sh"); return

    PARALLEL = 5   # Bir vaqtda 5 ta sahifa — server yukini kamaytirish uchun
    free_data = {}
    items = list(xonalar.items())

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
            )
            sem = asyncio.Semaphore(PARALLEL)

            # Sahifalarni oldindan yaratib qo'yamiz
            pages = [await browser.new_page(viewport={"width": 800, "height": 600})
                     for _ in range(PARALLEL)]

            # Batch larga bo'lib ishlaymiz
            BATCH = 50
            for batch_start in range(0, len(items), BATCH):
                batch = items[batch_start:batch_start + BATCH]
                tasks = []
                for i, (room_name, url) in enumerate(batch):
                    page = pages[i % PARALLEL]
                    tasks.append(_scan_page(sem, page, room_name, url))

                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, tuple):
                        free_data[res[0]] = res[1]

                done = batch_start + len(batch)
                log.info(f"Skanerlash: {done}/{len(items)} ta xona")
                await asyncio.sleep(1)  # serverga nafas berish

            for pg in pages:
                await pg.close()
            await browser.close()

    except Exception as e:
        log.error(f"Scan browser error: {e}")

    save_free_rooms_to_sheets(free_data)
    log.info(f"Skanerlash tugadi: {len(free_data)} xona")

def save_free_rooms_to_sheets(free_data: dict):
    ws = _sheets.get("free_rooms")
    if not ws: return
    try:
        today = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y")
        all_rows = ws.get_all_values()
        to_delete = [i+1 for i, r in enumerate(all_rows[1:], 1) if r and r[0] == today]
        for idx in reversed(to_delete): ws.delete_rows(idx)
        rows = []
        for room, days in free_data.items():
            bino = room.split("-")[0].split("/")[0].strip()
            for day_idx, paras in days.items():
                for para in paras:
                    s, e = PARA_TIMES[para]
                    rows.append([today, room, bino, f"{s}-{e}", str(para)])
        if rows: ws.append_rows(rows)
        log.info(f"Bosh xonalar saqlandi: {len(rows)} yozuv")
    except Exception as e:
        log.error(f"save_free_rooms: {e}")

def get_free_rooms_by_time(time_str: str) -> list:
    ws = _sheets.get("free_rooms")
    if not ws: return []
    try:
        today = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y")
        return [(r[1], r[2], r[4]) for r in ws.get_all_values()[1:]
                if len(r) >= 5 and r[0] == today and r[3] == time_str]
    except: return []

# ─────────────────────────────────────────
# AVTO JADVAL
# ─────────────────────────────────────────
async def job_send_auto(chat_id: int, url: str, group: str):
    fname = f"/tmp/auto_{chat_id}.png"
    try:
        await take_screenshot(url, fname)
        await bot.send_photo(
            chat_id, types.FSInputFile(fname),
            caption=f"📅 *{group}* — haftalik jadval\n\n🤖 @tsuetimebot",
            parse_mode="Markdown"
        )
    except Exception as e:
        log.error(f"Auto send [{chat_id}]: {e}")
    finally:
        if os.path.exists(fname): os.remove(fname)



def restore_auto_schedules():
    for row in get_auto_schedules():
        try:
            chat_id = int(row[0]); group = row[2]; url = row[3]
            day = int(row[4]); vaqt = row[5]
            h, m = map(int, vaqt.split(":"))
            job_id = f"auto_{chat_id}"
            if scheduler.get_job(job_id): scheduler.remove_job(job_id)
            scheduler.add_job(job_send_auto, "cron", day_of_week=day, hour=h, minute=m,
                              args=[chat_id, url, group], id=job_id)
            log.info(f"Avto tiklandi: {chat_id} {group} {DAYS_UZ[day]} {vaqt}")
        except Exception as e:
            log.error(f"Restore auto: {e}")

# ─────────────────────────────────────────
# ADMIN YORDAMCHISI
# ─────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# ═══════════════════════════════════════════
# H A N D L E R L A R
# ═══════════════════════════════════════════

# ─── /start ───
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    try: await message.delete()
    except: pass
    kb = InlineKeyboardBuilder()
    kb.row(
        types.InlineKeyboardButton(text="🇺🇿 O'zbek",  callback_data="setlang_uz"),
        types.InlineKeyboardButton(text="🇷🇺 Русский", callback_data="setlang_ru"),
    )
    sent = await message.answer("🌐 Tilni tanlang / Выберите язык:", reply_markup=kb.as_markup())
    last_msgs[message.chat.id] = {"last_msg": sent.message_id}

@dp.callback_query(F.data.startswith("setlang_"))
async def cb_setlang(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    chat_id = callback.message.chat.id
    user_lang[chat_id] = callback.data.split("_")[1]
    try:
        await callback.message.edit_text(tr("main_menu", chat_id), reply_markup=menu_kb(chat_id))
    except: pass

@dp.callback_query(F.data == "go_menu")
async def cb_go_menu(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    chat_id = callback.message.chat.id
    try:
        await callback.message.edit_text(tr("main_menu", chat_id), reply_markup=menu_kb(chat_id))
    except Exception:
        try:
            sent = await callback.message.answer(tr("main_menu", chat_id), reply_markup=menu_kb(chat_id))
            last_msgs[chat_id] = {"last_msg": sent.message_id}
        except: pass

# ─── TALABA ───
@dp.callback_query(F.data == "menu_student")
async def cb_student(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    data = load_json(TALABA_JSON)
    kb   = InlineKeyboardBuilder()
    for fak in data.keys():
        kb.row(types.InlineKeyboardButton(text=fak.upper(), callback_data=f"fak_{fak}"))
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(tr("select_fak", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("fak_"))
async def cb_fak(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    fak  = callback.data[4:]
    data = load_json(TALABA_JSON)
    kb   = InlineKeyboardBuilder()
    for kurs in data.get(fak, {}).keys():
        kb.row(types.InlineKeyboardButton(text=kurs, callback_data=f"kurs_{fak}||{kurs}"))
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="menu_student"))
    await callback.message.edit_text(tr("select_kurs", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("kurs_"))
async def cb_kurs(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    _, rest = callback.data.split("_", 1)
    fak, kurs = rest.split("||")
    data   = load_json(TALABA_JSON)
    groups = data.get(fak, {}).get(kurs, {})
    kb     = InlineKeyboardBuilder()
    for g in groups.keys():
        kb.add(types.InlineKeyboardButton(text=g, callback_data=f"grp_{fak}||{kurs}||{g}"))
    kb.adjust(3)
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data=f"fak_{fak}"))
    await callback.message.edit_text(tr("select_group", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("grp_"))
async def cb_group(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    _, rest = callback.data.split("_", 1)
    fak, kurs, group = rest.split("||")
    data = load_json(TALABA_JSON)
    url  = data.get(fak, {}).get(kurs, {}).get(group)
    if not url:
        await callback.answer("Xatolik!", show_alert=True); return

    if callback.message.chat.type in ["group", "supergroup"]:
        await state.update_data(url=url, group=group, fak=fak)
        lg   = lang(chat_id)
        days = DAYS_UZ if lg == "uz" else DAYS_RU
        kb   = InlineKeyboardBuilder()
        for i, name in days.items():
            kb.row(types.InlineKeyboardButton(text=name, callback_data=f"autoday_{i}"))
        await state.set_state(AutoSchedule.day)
        await callback.message.edit_text(tr("select_day", chat_id), reply_markup=kb.as_markup())
    else:
        await callback.message.delete()
        await send_schedule(chat_id, url, group, callback.from_user, tur="talaba")

# ─── AVTO JADVAL SETUP ───
@dp.callback_query(F.data.startswith("autoday_"))
async def cb_autoday(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    day = int(callback.data.split("_")[1])
    await state.update_data(day=day)
    kb = InlineKeyboardBuilder()
    for vt in ["07:00", "08:00", "10:00", "12:00", "16:00", "20:00"]:
        kb.add(types.InlineKeyboardButton(text=vt, callback_data=f"autotime_{vt}"))
    kb.adjust(3)
    now_str = datetime.now(TASHKENT_TZ).strftime("%H:%M")
    await state.set_state(AutoSchedule.time)
    await callback.message.edit_text(tr("select_time", chat_id, now=now_str), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("autotime_"), AutoSchedule.time)
async def cb_autotime_btn(callback: types.CallbackQuery, state: FSMContext):
    await _finalize_auto(callback.message, state, callback.data.split("_")[1], callback.message.chat)

@dp.message(AutoSchedule.time)
async def msg_autotime(message: types.Message, state: FSMContext):
    vaqt = message.text.strip().replace(".", ":").replace("-", ":")
    if re.match(r'^([01]?\d|2[0-3]):[0-5]\d$', vaqt):
        if len(vaqt.split(":")[0]) == 1: vaqt = "0" + vaqt
        await _finalize_auto(message, state, vaqt, message.chat)
    else:
        await message.answer(tr("time_invalid", message.chat.id))

async def _finalize_auto(message, state: FSMContext, vaqt: str, chat):
    chat_id = chat.id
    data  = await state.get_data()
    day   = int(data["day"]); url = data["url"]; group = data["group"]
    h, m  = map(int, vaqt.split(":"))
    job_id = f"auto_{chat_id}"
    if scheduler.get_job(job_id): scheduler.remove_job(job_id)
    scheduler.add_job(job_send_auto, "cron", day_of_week=day, hour=h, minute=m,
                      args=[chat_id, url, group], id=job_id)
    save_auto_schedule(chat_id, getattr(chat, "title", ""), group, url, day, vaqt)
    lg   = lang(chat_id)
    days = DAYS_UZ if lg == "uz" else DAYS_RU
    await message.answer(tr("auto_saved", chat_id, day=days[day], time=vaqt), parse_mode="Markdown")
    await state.clear()

# ─── O'QITUVCHI ───
@dp.callback_query(F.data == "menu_teacher")
async def cb_teacher(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    await state.set_state(TeacherFlow.search)
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(tr("teacher_ask", chat_id), reply_markup=kb.as_markup())

@dp.message(TeacherFlow.search)
async def msg_teacher_search(message: types.Message, state: FSMContext):
    chat_id  = message.chat.id
    query    = message.text.strip()
    ustozlar = load_json(USTOZ_JSON)

    # To'g'ri moslik
    for name, url in ustozlar.items():
        if query.lower() == name.lower():
            await state.clear()
            await send_schedule(chat_id, url, name, message.from_user, tur="ustoz")
            return

    # Qisman moslik
    matches = [(n, u) for n, u in ustozlar.items() if query.lower() in n.lower()]

    if not matches:
        await message.answer(tr("teacher_not", chat_id)); return

    if len(matches) == 1:
        await state.clear()
        await send_schedule(chat_id, matches[0][1], matches[0][0], message.from_user, tur="ustoz")
        return

    kb = InlineKeyboardBuilder()
    for name, _ in matches[:10]:
        kb.row(types.InlineKeyboardButton(text=name, callback_data=f"tchr_{name[:60]}"))
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="go_menu"))
    await message.answer(
        tr("teachers_list", chat_id) + "\n".join(m[0] for m in matches[:10]),
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("tchr_"))
async def cb_teacher_select(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    name    = callback.data[5:]
    ustozlar = load_json(USTOZ_JSON)
    url = ustozlar.get(name)
    if not url:
        await callback.answer("Topilmadi!", show_alert=True); return
    await state.clear()
    await callback.message.delete()
    await send_schedule(chat_id, url, name, callback.from_user, tur="ustoz")

# ─── XONA ───
# Cache — callback_data 64 belgi limitini yechish uchun
_bino_cache: dict = {}   # chat_id -> {idx: bino_name}
_room_cache: dict = {}   # chat_id -> {idx: (name, url)}

def get_bino(room_key: str) -> str:
    return room_key.split("-")[0].split("/")[0].strip()

@dp.callback_query(F.data == "menu_rooms")
async def cb_rooms(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    xonalar = load_json(XONALAR_JSON)
    if not xonalar:
        await callback.answer("Xonalar ma'lumotlari topilmadi!", show_alert=True); return

    binolar = sorted(set(get_bino(r) for r in xonalar if get_bino(r)),
                     key=lambda x: (int(x) if x.isdigit() else 999, x))

    # Bino cache
    _bino_cache[chat_id] = {str(i): b for i, b in enumerate(binolar)}

    kb = InlineKeyboardBuilder()
    for i, bino in enumerate(binolar):
        label = f"🏢 {bino}" if len(bino) <= 20 else f"🏢 {bino[:18]}…"
        kb.row(types.InlineKeyboardButton(text=label, callback_data=f"bi_{i}"))
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(tr("select_bino", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("bi_"))
async def cb_bino(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    idx     = callback.data[3:]
    bino    = (_bino_cache.get(chat_id) or {}).get(idx)
    if not bino:
        await callback.answer("Qayta menyudan kirins!", show_alert=True); return

    xonalar = load_json(XONALAR_JSON)
    rooms   = {n: u for n, u in xonalar.items() if get_bino(n) == bino}
    if not rooms:
        await callback.answer("Bu binoda xona topilmadi!", show_alert=True); return

    # Xona cache
    _room_cache[chat_id] = {str(i): (n, u) for i, (n, u) in enumerate(sorted(rooms.items()))}

    kb = InlineKeyboardBuilder()
    for i, name in enumerate(sorted(rooms.keys())):
        # Xona nomini qisqartirish (faqat ko'rinish uchun)
        parts = name.split("-")
        label = "-".join(parts[:3]) if len(parts) >= 3 else name
        kb.add(types.InlineKeyboardButton(text=label[:20], callback_data=f"ri_{i}"))
    kb.adjust(3)
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="menu_rooms"))
    await callback.message.edit_text(tr("select_xona", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("ri_"))
async def cb_room_select(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    idx     = callback.data[3:]
    entry   = (_room_cache.get(chat_id) or {}).get(idx)
    if not entry:
        await callback.answer("Topilmadi! Qayta tanlang.", show_alert=True); return
    room_name, url = entry
    await callback.message.delete()
    await send_schedule(chat_id, url, room_name, callback.from_user, tur="xona")

# ─── BOSH XONA ───
@dp.callback_query(F.data == "menu_free")
async def cb_free_rooms(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    await state.set_state(FreeRoomFlow.time_input)
    kb = InlineKeyboardBuilder()
    for para, (s, e) in PARA_TIMES.items():
        kb.add(types.InlineKeyboardButton(text=f"{para}️⃣ {s}-{e}", callback_data=f"freetime_{s}-{e}"))
    kb.adjust(2)
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(tr("free_ask", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("freetime_"))
async def cb_freetime_btn(callback: types.CallbackQuery, state: FSMContext):
    await _handle_free(callback.message, state, callback.data[9:], callback.from_user)

@dp.message(FreeRoomFlow.time_input)
async def msg_free_time(message: types.Message, state: FSMContext):
    raw = message.text.strip()
    if re.match(r'^([01]?\d|2[0-3]):[0-5]\d-([01]?\d|2[0-3]):[0-5]\d$', raw):
        await _handle_free(message, state, raw, message.from_user)
    else:
        await message.answer(tr("free_invalid", message.chat.id))

async def _handle_free(message, state: FSMContext, time_str: str, user: types.User):
    chat_id = message.chat.id
    await state.clear()
    rooms = get_free_rooms_by_time(time_str)
    kb    = InlineKeyboardBuilder()

    if not rooms:
        kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="menu_free"))
        kb.row(types.InlineKeyboardButton(text=tr("menu_btn", chat_id), callback_data="go_menu"))
        try: await message.edit_text(tr("free_none", chat_id), reply_markup=kb.as_markup())
        except: await message.answer(tr("free_none", chat_id), reply_markup=kb.as_markup())
        return

    xonalar = load_json(XONALAR_JSON)
    binolar: dict = {}
    for room_name, bino, para in rooms:
        binolar.setdefault(bino, []).append(room_name)

    text = tr("free_title", chat_id, time=time_str)

    # Bosh xona uchun ham cache
    _room_cache[chat_id] = {}
    idx = 0
    for bino in sorted(binolar, key=lambda x: (int(x) if x.isdigit() else 999, x)):
        text += f"🏢 *{bino}-bino:*\n"
        for room in sorted(binolar[bino]):
            text += f"  🚪 {room}\n"
            if xonalar.get(room):
                _room_cache[chat_id][str(idx)] = (room, xonalar[room])
                kb.add(types.InlineKeyboardButton(text=f"📅 {room[:15]}", callback_data=f"ri_{idx}"))
                idx += 1
        text += "\n"

    kb.adjust(3)
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="menu_free"))
    kb.row(types.InlineKeyboardButton(text=tr("menu_btn", chat_id), callback_data="go_menu"))

    try: await message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    except: await message.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    add_log(user.id, "free_rooms_search", time_str)

# ─── SAQLASH ───
@dp.callback_query(F.data == "dosave_prompt")
async def cb_dosave_prompt(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    uid     = callback.from_user.id
    if uid not in pending_save:
        await callback.answer("Avval jadval oching!", show_alert=True); return
    default = pending_save[uid]["name"]
    await state.set_state(SaveFlow.naming)
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text=f"✅ {default}", callback_data="dosave_default"))
    kb.row(types.InlineKeyboardButton(text=tr("back", chat_id), callback_data="go_menu"))
    await callback.message.answer(
        tr("save_ask_name", chat_id, default=default),
        reply_markup=kb.as_markup(), parse_mode="Markdown"
    )

@dp.callback_query(F.data == "dosave_default", SaveFlow.naming)
async def cb_dosave_default(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid not in pending_save:
        await callback.answer("Xatolik!", show_alert=True); return
    info = pending_save.pop(uid)
    save_schedule(uid, info["name"], info["url"], info["tur"])
    await state.clear()
    await callback.message.edit_text(
        tr("save_ok", callback.message.chat.id, name=info["name"]),
        reply_markup=back_kb(callback.message.chat.id), parse_mode="Markdown"
    )

@dp.message(SaveFlow.naming)
async def msg_save_name(message: types.Message, state: FSMContext):
    uid     = message.from_user.id
    chat_id = message.chat.id
    nom     = message.text.strip()
    if uid not in pending_save:
        await message.answer("Xatolik!"); await state.clear(); return
    info = pending_save.pop(uid)
    save_schedule(uid, nom, info["url"], info["tur"])
    await state.clear()
    await message.answer(tr("save_ok", chat_id, name=nom),
                         reply_markup=back_kb(chat_id), parse_mode="Markdown")

@dp.callback_query(F.data == "menu_saved")
async def cb_menu_saved(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    uid     = callback.from_user.id
    saved   = get_saved_schedules(uid)

    if not saved:
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=tr("menu_btn", chat_id), callback_data="go_menu"))
        await callback.message.edit_text(tr("save_empty", chat_id), reply_markup=kb.as_markup())
        return

    pending_save[f"list_{uid}"] = saved
    lg   = lang(chat_id)
    text = T[lg]["save_list"]
    kb   = InlineKeyboardBuilder()
    for i, (nom, url, tur) in enumerate(saved):
        icon = "🎓" if tur == "talaba" else "👨‍🏫" if tur == "ustoz" else "🚪"
        text += f"{icon} {nom}\n"
        kb.row(
            types.InlineKeyboardButton(text=f"📅 {nom[:20]}", callback_data=f"svopen_{i}"),
            types.InlineKeyboardButton(text="🗑",              callback_data=f"svdel_{nom[:30]}"),
        )
    kb.row(types.InlineKeyboardButton(text=tr("menu_btn", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("svopen_"))
async def cb_svopen(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    uid     = callback.from_user.id
    idx     = int(callback.data.split("_")[1])
    saved   = pending_save.get(f"list_{uid}") or get_saved_schedules(uid)
    if idx >= len(saved):
        await callback.answer("Topilmadi!", show_alert=True); return
    nom, url, tur = saved[idx]
    await callback.message.delete()
    await send_schedule(chat_id, url, nom, callback.from_user, tur=tur)

@dp.callback_query(F.data.startswith("svdel_"))
async def cb_svdel(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    uid     = callback.from_user.id
    nom     = callback.data[6:]
    delete_saved_schedule(uid, nom)
    pending_save.pop(f"list_{uid}", None)
    await callback.answer(tr("save_deleted", chat_id, name=nom), show_alert=True)
    saved = get_saved_schedules(uid)
    if not saved:
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=tr("menu_btn", chat_id), callback_data="go_menu"))
        await callback.message.edit_text(tr("save_empty", chat_id), reply_markup=kb.as_markup())
        return
    pending_save[f"list_{uid}"] = saved
    lg = lang(chat_id); text = T[lg]["save_list"]
    kb = InlineKeyboardBuilder()
    for i, (n, u, tr_) in enumerate(saved):
        icon = "🎓" if tr_ == "talaba" else "👨‍🏫" if tr_ == "ustoz" else "🚪"
        text += f"{icon} {n}\n"
        kb.row(
            types.InlineKeyboardButton(text=f"📅 {n[:20]}", callback_data=f"svopen_{i}"),
            types.InlineKeyboardButton(text="🗑",              callback_data=f"svdel_{n[:30]}"),
        )
    kb.row(types.InlineKeyboardButton(text=tr("menu_btn", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

# ─── BROADCAST ───
@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await _broadcast_prompt(message, state)

async def _broadcast_prompt(message, state: FSMContext):
    await state.set_state(BroadcastFlow.waiting)
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="❌ Bekor qilish", callback_data="broadcast_cancel"))
    text = (
        "📢 *Broadcast rejimi*\n\n"
        "Quyidagilardan birini yuboring:\n"
        "• Matn xabar\n• Rasm + matn\n• Hujjat/fayl\n• Kanaldan forward\n\n"
        "Barcha foydalanuvchilarga yuboriladi!"
    )
    try: await message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    except: await message.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "broadcast_cancel")
async def cb_broadcast_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Broadcast bekor qilindi.",
                                      reply_markup=back_kb(callback.message.chat.id, "adm_menu"))

@dp.message(BroadcastFlow.waiting)
async def broadcast_receive(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    await state.clear()
    user_ids = get_all_user_ids()
    if not user_ids:
        await message.answer("❌ Foydalanuvchilar topilmadi!"); return

    status = await message.answer(f"⏳ Yuborilmoqda... 0/{len(user_ids)}")
    ok, fail = 0, 0

    for uid in user_ids:
        try:
            if message.forward_origin or message.forward_from or message.forward_from_chat:
                await bot.forward_message(uid, message.chat.id, message.message_id)
            elif message.photo:
                await bot.send_photo(uid, message.photo[-1].file_id,
                                     caption=message.caption or "", parse_mode="Markdown")
            elif message.video:
                await bot.send_video(uid, message.video.file_id,
                                     caption=message.caption or "", parse_mode="Markdown")
            elif message.document:
                await bot.send_document(uid, message.document.file_id,
                                        caption=message.caption or "", parse_mode="Markdown")
            elif message.voice:
                await bot.send_voice(uid, message.voice.file_id)
            elif message.sticker:
                await bot.send_sticker(uid, message.sticker.file_id)
            elif message.text:
                await bot.send_message(uid, message.text, parse_mode="Markdown")
            else:
                await bot.copy_message(uid, message.chat.id, message.message_id)
            ok += 1
        except Exception as e:
            fail += 1
            log.warning(f"Broadcast fail [{uid}]: {e}")

        if (ok + fail) % 20 == 0:
            try: await status.edit_text(f"⏳ {ok+fail}/{len(user_ids)} — ✅{ok} ❌{fail}")
            except: pass
        await asyncio.sleep(0.05)

    await status.edit_text(
        f"📢 *Broadcast tugadi!*\n\n✅ {ok}\n❌ {fail}\n👥 Jami: {len(user_ids)}",
        parse_mode="Markdown"
    )
    add_log(message.from_user.id, "broadcast", f"ok={ok},fail={fail}")

# ─── ADMIN PANEL ───
def admin_panel_kb():
    kb = InlineKeyboardBuilder()
    kb.row(
        types.InlineKeyboardButton(text="📊 Statistika",     callback_data="adm_stats"),
        types.InlineKeyboardButton(text="🤖 Bot ma'lumoti",  callback_data="adm_info"),
    )
    kb.row(
        types.InlineKeyboardButton(text="📅 Avto jadvallar", callback_data="adm_auto"),
        types.InlineKeyboardButton(text="📢 Broadcast",      callback_data="adm_broadcast"),
    )
    kb.row(types.InlineKeyboardButton(text="🔍 Xona skaner", callback_data="adm_scan"))
    return kb.as_markup()

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id): return
    await message.answer("👨‍💼 *Admin panel*\n\nKerakli bo'limni tanlang:",
                         reply_markup=admin_panel_kb(), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_menu")
async def cb_adm_menu(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("👨‍💼 *Admin panel*\n\nKerakli bo'limni tanlang:",
                                      reply_markup=admin_panel_kb(), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_stats")
async def cb_adm_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.answer("⏳ Yuklanmoqda...")
    try:
        ws_u  = _sheets.get("users");      total_users  = len(ws_u.col_values(2))-1  if ws_u  else 0
        ws_a  = _sheets.get("auto");       total_auto   = len(ws_a.get_all_values())-1 if ws_a else 0
        ws_s  = _sheets.get("saved");      total_saved  = len(ws_s.get_all_values())-1 if ws_s else 0
        ws_l  = _sheets.get("logs")
        today = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y")
        today_logs = sum(1 for d in (ws_l.col_values(1)[1:] if ws_l else []) if d.startswith(today))
        text = (
            f"📊 *Statistika*\n\n"
            f"👥 Foydalanuvchilar: *{total_users}*\n"
            f"📅 Avto jadval guruhlari: *{total_auto}*\n"
            f"⭐ Saqlangan jadvallar: *{total_saved}*\n"
            f"📝 Bugungi so'rovlar: *{today_logs}*\n"
            f"🖼 Screenshot cache: *{len(screenshot_cache)}* ta"
        )
    except Exception as e:
        text = f"❌ Xatolik: {e}"
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="🔄 Yangilash", callback_data="adm_stats"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Orqaga",   callback_data="adm_menu"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_info")
async def cb_adm_info(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    now    = datetime.now(TASHKENT_TZ)
    uptime = now - BOT_START_TIME
    d = uptime.days; h = uptime.seconds//3600; m = (uptime.seconds%3600)//60; s = uptime.seconds%60
    jobs     = scheduler.get_jobs()
    auto_j   = [j for j in jobs if j.id.startswith("auto_")]
    text = (
        f"🤖 *Bot ma'lumoti*\n\n"
        f"⏱ Uptime: *{d}k {h}s {m}d {s}sek*\n"
        f"🕐 Vaqt: *{now.strftime('%d.%m.%Y %H:%M:%S')}*\n\n"
        f"⚙️ Scheduler:\n"
        f"  • Faol joblar: *{len(jobs)}*\n"
        f"  • Avto jadval: *{len(auto_j)}* guruh\n"
        f"  • Xona skaner: har kuni *05:30*\n\n"
        f"📁 Fayllar:\n"
        f"  • talaba.json: *{'✅' if os.path.exists(TALABA_JSON) else '❌'}*\n"
        f"  • ustoz.json: *{'✅' if os.path.exists(USTOZ_JSON) else '❌'}*\n"
        f"  • xonalar.json: *{'✅' if os.path.exists(XONALAR_JSON) else '❌'}*\n\n"
        f"🗄 Google Sheets: *{'✅ Ulangan' if _sheets else '❌ Ulanmagan'}*"
    )
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="🔄 Yangilash", callback_data="adm_info"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Orqaga",   callback_data="adm_menu"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_auto")
async def cb_adm_auto(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.answer("⏳")
    rows = get_auto_schedules()
    kb   = InlineKeyboardBuilder()
    if not rows:
        text = "📅 *Avto jadvallar*\n\n😔 Hech qaysi guruhda sozlanmagan."
    else:
        text = f"📅 *Avto jadvallar* — {len(rows)} ta\n\n"
        for row in rows:
            try:
                cid = row[0]; title = row[1] or "Noma'lum"
                group = row[2]; day_n = DAYS_UZ.get(int(row[4]), row[4]); vaqt = row[5]
                text += f"👥 *{title}*\n   📌 {group} | {day_n} {vaqt}\n\n"
                kb.row(types.InlineKeyboardButton(text=f"🗑 {title[:25]}", callback_data=f"adm_delauto_{cid}"))
            except: continue
    kb.row(types.InlineKeyboardButton(text="⬅️ Orqaga", callback_data="adm_menu"))
    try: await callback.message.edit_text(text[:4096], reply_markup=kb.as_markup(), parse_mode="Markdown")
    except: pass

@dp.callback_query(F.data.startswith("adm_delauto_"))
async def cb_adm_delauto(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    cid_str = callback.data.split("_")[2]
    job_id  = f"auto_{cid_str}"
    if scheduler.get_job(job_id): scheduler.remove_job(job_id)
    ws = _sheets.get("auto")
    if ws:
        try:
            ids = ws.col_values(1)
            if cid_str in ids: ws.delete_rows(ids.index(cid_str)+1)
        except Exception as e:
            log.error(f"delauto: {e}")
    await callback.answer("✅ O'chirildi!", show_alert=True)
    await cb_adm_auto(callback)

@dp.callback_query(F.data == "adm_broadcast")
async def cb_adm_broadcast(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await _broadcast_prompt(callback.message, state)

@dp.callback_query(F.data == "adm_scan")
async def cb_adm_scan(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("⏳ Xonalar skanerlash boshlandi...\nBir necha daqiqa olishi mumkin.")
    await job_scan_free_rooms()
    count = len(load_json(XONALAR_JSON))
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="⬅️ Orqaga", callback_data="adm_menu"))
    await callback.message.edit_text(f"✅ Tugadi! {count} ta xona tekshirildi.", reply_markup=kb.as_markup())

@dp.message(Command("scan_rooms"))
async def cmd_scan_rooms(message: types.Message):
    if not is_admin(message.from_user.id): return
    await message.answer("⏳ Skanerlash boshlandi...")
    await job_scan_free_rooms()
    await message.answer(tr("free_updated", message.chat.id, count=len(load_json(XONALAR_JSON))))

# ═══════════════════════════════════════════
# M A I N
# ═══════════════════════════════════════════
async def main():
    log.info("Bot ishga tushmoqda...")
    init_sheets()
    restore_auto_schedules()
    scheduler.add_job(job_scan_free_rooms, "cron", hour=5, minute=30, id="scan_rooms")
    if not scheduler.running:
        scheduler.start()
    await bot.delete_webhook(drop_pending_updates=True)
    log.info("Bot polling boshlandi ✅")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
