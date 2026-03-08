import json, asyncio, os, logging, warnings, time, re
from datetime import datetime, timedelta
from typing import Optional
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramBadRequest
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
# SOZLAMALAR — env yoki to'g'ridan-to'g'ri
# ─────────────────────────────────────────
TOKEN             = os.getenv("BOT_TOKEN", "8544087301:AAG5zpzLBbuuLm3khbg4c6_GZcqBgSFFy10")
SHEET_ID          = os.getenv("SHEET_ID",  "1vZLVKA__HPQAL70HfzI0eYu3MpsE-Namho6D-2RLIYw")
CREDENTIALS_FILE  = os.getenv("CREDENTIALS_FILE", "credentials.json")

TALABA_JSON  = "talaba.json"
USTOZ_JSON   = "ustoz.json"
XONALAR_JSON = "xonalar.json"

TASHKENT_TZ = pytz.timezone("Asia/Tashkent")

# Para vaqtlari
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
bot = Bot(token=TOKEN)
dp  = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TASHKENT_TZ)

# ─────────────────────────────────────────
# FSM STATES
# ─────────────────────────────────────────
class StudentFlow(StatesGroup):
    fak   = State()
    kurs  = State()
    group = State()

class TeacherFlow(StatesGroup):
    search = State()

class RoomFlow(StatesGroup):
    bino = State()
    xona = State()

class FreeRoomFlow(StatesGroup):
    time_input = State()

class AutoSchedule(StatesGroup):
    day  = State()
    time = State()

class SaveFlow(StatesGroup):
    naming = State()   # foydalanuvchi nom yozayapti

# ─────────────────────────────────────────
# IN-MEMORY CACHE
# ─────────────────────────────────────────
screenshot_cache: dict = {}   # url -> {id, time}
user_lang: dict       = {}    # chat_id -> 'uz'|'ru'
last_msgs: dict       = {}    # chat_id -> {last_msg, last_pic}

# ─────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────
_sheets: dict = {}   # sheet name -> worksheet

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
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            log.warning("credentials.json topilmadi!")
            return
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds  = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        wb     = client.open_by_key(SHEET_ID)

        existing = [s.title for s in wb.worksheets()]
        for key, name in SHEET_NAMES.items():
            if name not in existing:
                wb.add_worksheet(title=name, rows=1000, cols=20)
            _sheets[key] = wb.worksheet(name)

        # Header qo'yish
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
        first = ws.row_values(1)
        if not first:
            ws.append_row(headers)
    except: pass

def sheet_log(key: str, row: list):
    try:
        ws = _sheets.get(key)
        if ws: ws.append_row(row)
    except Exception as e:
        log.error(f"Sheet write error [{key}]: {e}")

def save_user(user: types.User, tur: str, malumot: str, url: str):
    now = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
    uname = f"@{user.username}" if user.username else "-"
    ws = _sheets.get("users")
    if not ws: return
    try:
        ids = ws.col_values(2)
        row = [now, str(user.id), uname, user.full_name, tur, malumot, url]
        if str(user.id) in ids:
            idx = ids.index(str(user.id)) + 1
            ws.update(f"A{idx}:G{idx}", [row])
        else:
            ws.append_row(row)
    except Exception as e:
        log.error(f"save_user error: {e}")

def save_teacher_visit(user: types.User, teacher: str, url: str):
    now = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
    uname = f"@{user.username}" if user.username else "-"
    ws = _sheets.get("teachers")
    if not ws: return
    try:
        ids = ws.col_values(2)
        row = [now, str(user.id), uname, user.full_name, teacher, url]
        if str(user.id) in ids:
            idx = ids.index(str(user.id)) + 1
            ws.update(f"A{idx}:F{idx}", [row])
        else:
            ws.append_row(row)
    except Exception as e:
        log.error(f"save_teacher error: {e}")

def save_auto_schedule(chat_id: int, chat_title: str, group: str, url: str, day: int, vaqt: str):
    ws = _sheets.get("auto")
    if not ws: return
    try:
        now = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
        ids = ws.col_values(1)
        row = [str(chat_id), chat_title, group, url, str(day), vaqt, now]
        if str(chat_id) in ids:
            idx = ids.index(str(chat_id)) + 1
            ws.update(f"A{idx}:G{idx}", [row])
        else:
            ws.append_row(row)
    except Exception as e:
        log.error(f"save_auto error: {e}")

def get_auto_schedules() -> list:
    ws = _sheets.get("auto")
    if not ws: return []
    try:
        rows = ws.get_all_values()
        return [r for r in rows[1:] if len(r) >= 6 and r[0]]
    except: return []

def add_log(user_id: int, action: str, data: str = ""):
    now = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
    sheet_log("logs", [now, str(user_id), action, data])

# ─────────────────────────────────────────
# SAQLANGAN JADVALLAR
# ─────────────────────────────────────────
def get_saved_schedules(user_id: int) -> list:
    """[(nom, url, tur)] qaytaradi"""
    ws = _sheets.get("saved")
    if not ws: return []
    try:
        rows = ws.get_all_values()
        return [(r[1], r[2], r[3]) for r in rows[1:] if r and r[0] == str(user_id)]
    except: return []

def save_schedule(user_id: int, nom: str, url: str, tur: str):
    ws = _sheets.get("saved")
    if not ws: return
    try:
        now = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M:%S")
        ws.append_row([str(user_id), nom, url, tur, now])
    except Exception as e:
        log.error(f"save_schedule error: {e}")

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
        log.error(f"delete_saved error: {e}")

# ─────────────────────────────────────────
# JSON YUKLASH
# ─────────────────────────────────────────
def load_json(path: str) -> dict:
    if not os.path.exists(path): return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ─────────────────────────────────────────
# TIL YORDAMCHISI
# ─────────────────────────────────────────
def lang(chat_id: int) -> str:
    return user_lang.get(chat_id, "uz")

T = {
    "uz": {
        "choose_lang":   "🌐 Tilni tanlang:",
        "main_menu":     "🏠 Asosiy menyu\n\nNimani izlayapsiz?",
        "student":       "🎓 Talaba",
        "teacher":       "👨‍🏫 O'qituvchi",
        "rooms":         "🏛 Xonalar",
        "free_rooms":    "🔍 Bosh xonalar",
        "select_fak":    "📚 Fakultetni tanlang:",
        "select_kurs":   "📖 Kursni tanlang:",
        "select_group":  "👥 Guruhni tanlang:",
        "teacher_ask":   "👨‍🏫 Ustoz ismini yozing (masalan: Karimov Sherali):",
        "teacher_found": "✅ Topildi! Jadval yuklanmoqda...",
        "teacher_not":   "❌ Ustoz topilmadi. Qaytadan urinib ko'ring.",
        "select_bino":   "🏢 Binoni tanlang:",
        "select_xona":   "🚪 Xonani tanlang:",
        "free_ask":      "⏰ Qaysi vaqtga bosh xona izlaysiz?\n\nFormat: 13:30-14:00\n\nMavjud vaqtlar:\n1️⃣ 08:30-09:50\n2️⃣ 10:00-11:20\n3️⃣ 11:30-12:50\n4️⃣ 13:30-14:50\n5️⃣ 15:00-16:20\n6️⃣ 16:30-17:50\n7️⃣ 18:00-19:20\n8️⃣ 19:30-20:50",
        "free_invalid":  "❌ Format noto'g'ri. Masalan: 13:30-14:50",
        "free_none":     "😔 Bu vaqtga bosh xona topilmadi yoki ma'lumotlar yangilanmagan.",
        "free_title":    "🔍 {time} ga bosh xonalar ro'yxati:\n\n",
        "loading":       "⏳ Jadval yuklanmoqda...",
        "error":         "❌ Xatolik yuz berdi. Qaytadan urinib ko'ring.",
        "back":          "⬅️ Orqaga",
        "menu_btn":      "🏠 Menyu",
        "select_day":    "📅 Qaysi kuni jadval yuborilsin?",
        "select_time":   "⏰ Vaqtni tanlang yoki yozing (HH:MM):\nHozir: {now}",
        "auto_saved":    "✅ Sozlandi! Har {day} kuni soat {time}da jadval yuboriladi.",
        "time_invalid":  "❌ Vaqt formati noto'g'ri. HH:MM formatda yozing (masalan: 14:20)",
        "fav_caption":   "✅ *Guruh: {group}*\n\n🤖 @tsuetimebot",
        "free_updated":  "✅ Bosh xonalar yangilandi: {count} ta xona tekshirildi.",
        "view_schedule": "📅 Jadvalni ko'rish",
        "teachers_list": "Ustozlar ro'yxatidan:\n",
        "suggest":       "Siz {name}ni qidiryapsizmi?\n",
        "saved_btn":     "⭐ Saqlash",
        "my_saved":      "⭐ Saqlangan jadvallar",
        "save_ask_name": "📝 Bu jadval uchun nom yozing:\n(Masalan: Karimov Sherali, MNP-80)",
        "save_ok":       "✅ '{name}' nomi bilan saqlandi!",
        "save_list":     "⭐ Saqlangan jadvallaringiz:\n\n",
        "save_empty":    "😔 Hali hech narsa saqlanmagan.\n\nJadval ko'rganda '⭐ Saqlash' tugmasini bosing.",
        "save_delete":   "🗑 O'chirish",
        "save_deleted":  "🗑 '{name}' o'chirildi.",
        "save_open":     "📅 Ko'rish",
    },
    "ru": {
        "choose_lang":   "🌐 Выберите язык:",
        "main_menu":     "🏠 Главное меню\n\nЧто ищете?",
        "student":       "🎓 Студент",
        "teacher":       "👨‍🏫 Преподаватель",
        "rooms":         "🏛 Кабинеты",
        "free_rooms":    "🔍 Свободные кабинеты",
        "select_fak":    "📚 Выберите факультет:",
        "select_kurs":   "📖 Выберите курс:",
        "select_group":  "👥 Выберите группу:",
        "teacher_ask":   "👨‍🏫 Введите имя преподавателя (пример: Karimov Sherali):",
        "teacher_found": "✅ Найдено! Загружается расписание...",
        "teacher_not":   "❌ Преподаватель не найден. Попробуйте снова.",
        "select_bino":   "🏢 Выберите корпус:",
        "select_xona":   "🚪 Выберите кабинет:",
        "free_ask":      "⏰ На какое время ищете свободный кабинет?\n\nФормат: 13:30-14:00\n\nДоступные пары:\n1️⃣ 08:30-09:50\n2️⃣ 10:00-11:20\n3️⃣ 11:30-12:50\n4️⃣ 13:30-14:50\n5️⃣ 15:00-16:20\n6️⃣ 16:30-17:50\n7️⃣ 18:00-19:20\n8️⃣ 19:30-20:50",
        "free_invalid":  "❌ Неверный формат. Пример: 13:30-14:50",
        "free_none":     "😔 Свободных кабинетов не найдено или данные ещё не обновлены.",
        "free_title":    "🔍 Свободные кабинеты на {time}:\n\n",
        "loading":       "⏳ Расписание загружается...",
        "error":         "❌ Произошла ошибка. Попробуйте снова.",
        "back":          "⬅️ Назад",
        "menu_btn":      "🏠 Меню",
        "select_day":    "📅 В какой день отправлять расписание?",
        "select_time":   "⏰ Выберите время или введите (ЧЧ:ММ):\nСейчас: {now}",
        "auto_saved":    "✅ Настроено! Каждый {day} в {time} будет отправляться расписание.",
        "time_invalid":  "❌ Неверный формат. Введите в формате ЧЧ:ММ (пример: 14:20)",
        "fav_caption":   "✅ *Группа: {group}*\n\n🤖 @tsuetimebot",
        "free_updated":  "✅ Свободные кабинеты обновлены: проверено {count} кабинетов.",
        "view_schedule": "📅 Посмотреть расписание",
        "teachers_list": "Из списка преподавателей:\n",
        "suggest":       "Вы ищете {name}?\n",
        "saved_btn":     "⭐ Сохранить",
        "my_saved":      "⭐ Сохранённые расписания",
        "save_ask_name": "📝 Введите название для этого расписания:\n(Например: Karimov Sherali, MNP-80)",
        "save_ok":       "✅ Сохранено под именем '{name}'!",
        "save_list":     "⭐ Ваши сохранённые расписания:\n\n",
        "save_empty":    "😔 Ничего не сохранено.\n\nПри просмотре расписания нажмите '⭐ Сохранить'.",
        "save_delete":   "🗑 Удалить",
        "save_deleted":  "🗑 '{name}' удалено.",
        "save_open":     "📅 Открыть",
    }
}

def t(key: str, chat_id: int, **kwargs) -> str:
    lg = lang(chat_id)
    text = T[lg].get(key, key)
    if kwargs:
        try: text = text.format(**kwargs)
        except: pass
    return text

# ─────────────────────────────────────────
# KEYBOARD HELPERS
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

def back_menu_kb(chat_id: int):
    lg = lang(chat_id)
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text=T[lg]["menu_btn"], callback_data="go_menu"))
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
        page = await browser.new_page(
            viewport={"width": 1280, "height": 800},
            device_scale_factor=2
        )
        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.add_style_tag(content="""
                .no-print, .main-menu, .footer, #header,
                .navigation, .breadcrumb { display: none !important; }
            """)
            target = await page.query_selector(".tt-grid-container, #PRINT_SCENE_BG_1")
            if target:
                await target.screenshot(path=filename)
            else:
                await page.screenshot(path=filename, full_page=False)
        finally:
            await browser.close()

async def get_schedule_photo(chat_id: int, url: str) -> Optional[str]:
    """Returns file_id from cache or takes new screenshot. Returns None on error."""
    now = time.time()
    if url in screenshot_cache and (now - screenshot_cache[url]["time"]) < 3600:
        return screenshot_cache[url]["id"]
    fname = f"/tmp/sc_{chat_id}_{int(now)}.png"
    try:
        await take_screenshot(url, fname)
        msg = await bot.send_photo(
            chat_id, types.FSInputFile(fname),
            caption="⏳", disable_notification=True
        )
        fid = msg.photo[-1].file_id
        screenshot_cache[url] = {"id": fid, "time": now}
        await msg.delete()
        if os.path.exists(fname): os.remove(fname)
        return fid
    except Exception as e:
        log.error(f"Screenshot error: {e}")
        if os.path.exists(fname): os.remove(fname)
        return None

# ─────────────────────────────────────────
# BOSH XONA ANIQLASH (HTML tahlil)
# ─────────────────────────────────────────
async def parse_free_room(url: str, room_name: str) -> dict:
    """
    Returns dict: {day_idx: [para_nums]} — bosh para raqamlari
    day_idx: 0=Mon..5=Sat
    """
    result = {i: list(range(1, 9)) for i in range(6)}  # default: hamma vaqt bosh
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
            )
            page = await browser.new_page(viewport={"width": 1280, "height": 800})
            await page.goto(url, wait_until="networkidle", timeout=60000)
            html = await page.content()
            await browser.close()

        soup = BeautifulSoup(html, "html.parser")

        # SVG dan dars ma'lumotlarini topish
        # Har bir dars bloki — <g> yoki <rect> bilan, ichida kun va para info bor
        # EduPage SVG grid: x koordinata = kun, y = para
        # Grid o'lchamlari taxminan:
        # x: [55, 145, 235, 325, 415, 505] = [Mon, Tue, Wed, Thu, Fri, Sat] (har bir ~90px)
        # y: para 1-8 ~ [250, 414, 578, 742, ...] (har biri ~164px)

        svg = soup.find("g", {"id": "PRINT_SCENE_BG_1"})
        if not svg:
            return result

        # Barcha rect larni topish — dars bo'lgan joylar
        rects = svg.find_all("rect")
        occupied = set()  # (day_idx, para_num)

        # Grid koordinatalarini hisoblash
        # Saytdan olingan ma'lumotlarga asosan:
        DAY_X = [55, 145, 235, 325, 415, 505]    # Mon..Sat markazlari (taxminan)
        PARA_Y = [250, 414, 578, 742, 906, 1070, 1234, 1398]  # 1-8 para (taxminan)
        COL_W = 90   # har bir kun kengligi
        ROW_H = 164  # har bir para balandligi

        for rect in rects:
            try:
                x = float(rect.get("x", 0))
                y = float(rect.get("y", 0))
                w = float(rect.get("width", 0))
                h = float(rect.get("height", 0))
                fill = rect.get("fill", "transparent")

                # Transparent rect larni skip
                if fill in ["transparent", "none", ""] or w < 50 or h < 50:
                    continue

                # Kun va para aniqlash
                for di, dx in enumerate(DAY_X):
                    if abs(x - dx) < COL_W * 0.6:
                        for pi, py in enumerate(PARA_Y):
                            if abs(y - py) < ROW_H * 0.6:
                                occupied.add((di, pi + 1))
                                break
                        break
            except:
                continue

        # Natija: band bo'lmagan vaqtlar
        for di in range(6):
            result[di] = [p for p in range(1, 9) if (di, p) not in occupied]

    except Exception as e:
        log.error(f"parse_free_room error [{room_name}]: {e}")

    return result

def save_free_rooms_to_sheets(free_data: dict):
    """free_data: {room_name: {day_idx: [para_nums]}}"""
    ws = _sheets.get("free_rooms")
    if not ws: return
    try:
        today = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y")
        # Eski bugungi ma'lumotlarni o'chirish
        all_rows = ws.get_all_values()
        to_delete = [i+1 for i, r in enumerate(all_rows[1:], 1) if r and r[0] == today]
        for idx in reversed(to_delete):
            ws.delete_rows(idx)

        # Yangi ma'lumotlar
        rows = []
        for room, days in free_data.items():
            bino = room.split("-")[0].split("/")[0]
            for day_idx, paras in days.items():
                for para in paras:
                    start, end = PARA_TIMES[para]
                    rows.append([today, room, bino, f"{start}-{end}", str(para)])

        if rows:
            ws.append_rows(rows)
        log.info(f"Bosh xonalar saqlandi: {len(rows)} ta yozuv")
    except Exception as e:
        log.error(f"save_free_rooms error: {e}")

def get_free_rooms_by_time(time_str: str) -> list:
    """
    time_str: "13:30-14:50"
    Returns: [(room_name, bino, para)] list
    """
    ws = _sheets.get("free_rooms")
    if not ws: return []
    try:
        today = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y")
        all_rows = ws.get_all_values()
        result = []
        for row in all_rows[1:]:
            if len(row) >= 5 and row[0] == today and row[3] == time_str:
                result.append((row[1], row[2], row[4]))
        return result
    except: return []

def get_free_rooms_para(time_str: str) -> Optional[int]:
    """Vaqt oralig'idan para raqamini topish"""
    for para, (start, end) in PARA_TIMES.items():
        if f"{start}-{end}" == time_str:
            return para
    # Taxminiy moslik
    parts = time_str.split("-")
    if len(parts) == 2:
        input_start = parts[0].strip()
        for para, (start, end) in PARA_TIMES.items():
            if start == input_start:
                return para
    return None

# ─────────────────────────────────────────
# BOSH XONA TEKSHIRISH (Scheduled job)
# ─────────────────────────────────────────
async def job_scan_free_rooms():
    """Har kuni 05:00-08:00 orasida ishlaydi — barcha xonalarni skanerlaydi"""
    log.info("Bosh xona skanerlash boshlandi...")
    xonalar = load_json(XONALAR_JSON)
    if not xonalar:
        log.warning("xonalar.json bo'sh yoki topilmadi")
        return

    free_data = {}
    count = 0
    for room_name, url in xonalar.items():
        try:
            days_free = await parse_free_room(url, room_name)
            free_data[room_name] = days_free
            count += 1
            await asyncio.sleep(2)  # serverga ortiqcha yuklanmaslik uchun
        except Exception as e:
            log.error(f"Xona scan error [{room_name}]: {e}")

    save_free_rooms_to_sheets(free_data)
    log.info(f"Skanerlash tugadi: {count} ta xona tekshirildi")

# ─────────────────────────────────────────
# AVTO JADVAL (Scheduled job)
# ─────────────────────────────────────────
async def job_send_auto(chat_id: int, url: str, group: str):
    try:
        fname = f"/tmp/auto_{chat_id}.png"
        await take_screenshot(url, fname)
        caption = f"📅 *{group}* — haftalik jadval\n\n🤖 @tsuetimebot"
        await bot.send_photo(chat_id, types.FSInputFile(fname), caption=caption, parse_mode="Markdown")
        if os.path.exists(fname): os.remove(fname)
        log.info(f"Avto jadval yuborildi: {chat_id} -> {group}")
    except Exception as e:
        log.error(f"Auto send error [{chat_id}]: {e}")

def restore_auto_schedules():
    """Bot start bo'lganda Sheets dan avto jadvallarni tiklash"""
    rows = get_auto_schedules()
    for row in rows:
        try:
            chat_id    = int(row[0])
            group_name = row[2]
            url        = row[3]
            day        = int(row[4])
            vaqt       = row[5]
            h, m = map(int, vaqt.split(":"))
            job_id = f"auto_{chat_id}"
            if scheduler.get_job(job_id):
                scheduler.remove_job(job_id)
            scheduler.add_job(
                job_send_auto, "cron",
                day_of_week=day, hour=h, minute=m,
                args=[chat_id, url, group_name],
                id=job_id
            )
            log.info(f"Avto jadval tiklandi: {chat_id} ({group_name}) {DAYS_UZ[day]} {vaqt}")
        except Exception as e:
            log.error(f"Restore auto error: {e}")

# ─────────────────────────────────────────
# XABAR YO'Q QILISH
# ─────────────────────────────────────────
async def delete_last(chat_id: int):
    info = last_msgs.get(chat_id, {})
    for key in ["last_pic", "last_msg"]:
        mid = info.get(key)
        if mid:
            try: await bot.delete_message(chat_id, mid)
            except: pass
            info[key] = None

# ─────────────────────────────────────────
# JADVAL YUBORISH
# ─────────────────────────────────────────
# url_cache: pending_save[user_id] = {name, url, tur}
pending_save: dict = {}

async def send_schedule(chat_id: int, url: str, name: str, user: types.User = None,
                         tur: str = "talaba", extra_btn: list = None):
    if chat_id not in last_msgs: last_msgs[chat_id] = {}
    await delete_last(chat_id)
    lg = lang(chat_id)

    status = await bot.send_message(chat_id, T[lg]["loading"])
    kb = InlineKeyboardBuilder()

    # Saqlash tugmasi (faqat shaxsiy chatda)
    if user and tur in ("talaba", "ustoz"):
        pending_save[user.id] = {"name": name, "url": url, "tur": tur}
        kb.row(types.InlineKeyboardButton(
            text=T[lg]["saved_btn"],
            callback_data="dosave_prompt"
        ))

    if extra_btn:
        for btn in extra_btn:
            kb.row(btn)
    kb.row(types.InlineKeyboardButton(text=T[lg]["menu_btn"], callback_data="go_menu"))

    caption = T[lg]["fav_caption"].format(group=name)

    try:
        fid = await get_schedule_photo(chat_id, url)
        if fid:
            sent = await bot.send_photo(
                chat_id, fid, caption=caption,
                reply_markup=kb.as_markup(), parse_mode="Markdown"
            )
            last_msgs[chat_id]["last_pic"] = sent.message_id
        else:
            await bot.send_message(chat_id, T[lg]["error"], reply_markup=back_menu_kb(chat_id))
    except Exception as e:
        log.error(f"send_schedule error: {e}")
        await bot.send_message(chat_id, T[lg]["error"], reply_markup=back_menu_kb(chat_id))
    finally:
        try: await status.delete()
        except: pass

    if user:
        if tur == "talaba":
            save_user(user, tur, name, url)
        elif tur == "ustoz":
            save_teacher_visit(user, name, url)
        add_log(user.id, f"view_{tur}", name)

# ─────────────────────────────────────────
# /start HANDLER
# ─────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    chat_id = message.chat.id
    try: await message.delete()
    except: pass

    kb = InlineKeyboardBuilder()
    kb.row(
        types.InlineKeyboardButton(text="🇺🇿 O'zbek",   callback_data="setlang_uz"),
        types.InlineKeyboardButton(text="🇷🇺 Русский",  callback_data="setlang_ru"),
    )
    sent = await message.answer("🌐 Tilni tanlang / Выберите язык:", reply_markup=kb.as_markup())
    last_msgs[chat_id] = {"last_msg": sent.message_id}

@dp.callback_query(F.data.startswith("setlang_"))
async def cb_setlang(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    chat_id = callback.message.chat.id
    user_lang[chat_id] = callback.data.split("_")[1]
    await callback.message.edit_text(t("main_menu", chat_id), reply_markup=menu_kb(chat_id))

@dp.callback_query(F.data == "go_menu")
async def cb_go_menu(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    chat_id = callback.message.chat.id
    try:
        await callback.message.edit_text(t("main_menu", chat_id), reply_markup=menu_kb(chat_id))
    except:
        sent = await callback.message.answer(t("main_menu", chat_id), reply_markup=menu_kb(chat_id))
        last_msgs[chat_id] = {"last_msg": sent.message_id}

# ─────────────────────────────────────────
# TALABA FLOW
# ─────────────────────────────────────────
@dp.callback_query(F.data == "menu_student")
async def cb_student(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    data = load_json(TALABA_JSON)
    kb = InlineKeyboardBuilder()
    for fak in data.keys():
        kb.row(types.InlineKeyboardButton(text=fak.upper(), callback_data=f"fak_{fak}"))
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(t("select_fak", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("fak_"))
async def cb_fak(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    fak = callback.data[4:]
    data = load_json(TALABA_JSON)
    kb = InlineKeyboardBuilder()
    for kurs in data.get(fak, {}).keys():
        kb.row(types.InlineKeyboardButton(text=kurs, callback_data=f"kurs_{fak}||{kurs}"))
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="menu_student"))
    await callback.message.edit_text(t("select_kurs", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("kurs_"))
async def cb_kurs(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    _, rest = callback.data.split("_", 1)
    fak, kurs = rest.split("||")
    data = load_json(TALABA_JSON)
    groups = data.get(fak, {}).get(kurs, {})
    kb = InlineKeyboardBuilder()
    for g in groups.keys():
        kb.add(types.InlineKeyboardButton(text=g, callback_data=f"grp_{fak}||{kurs}||{g}"))
    kb.adjust(3)
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data=f"fak_{fak}"))
    await callback.message.edit_text(t("select_group", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("grp_"))
async def cb_group(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    _, rest = callback.data.split("_", 1)
    fak, kurs, group = rest.split("||")
    data = load_json(TALABA_JSON)
    url = data.get(fak, {}).get(kurs, {}).get(group)

    if not url:
        await callback.answer("Xatolik!", show_alert=True)
        return

    # Guruhda bo'lsa — avto jadval sozlash
    if callback.message.chat.type in ["group", "supergroup"]:
        await state.update_data(url=url, group=group, fak=fak)
        kb = InlineKeyboardBuilder()
        lg = lang(chat_id)
        days = DAYS_UZ if lg == "uz" else DAYS_RU
        for i, name in days.items():
            kb.row(types.InlineKeyboardButton(text=name, callback_data=f"autoday_{i}"))
        kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data=f"kurs_{fak}||{kurs}"))
        await callback.message.edit_text(t("select_day", chat_id), reply_markup=kb.as_markup())
        await state.set_state(AutoSchedule.day)
    else:
        await callback.message.delete()
        await send_schedule(chat_id, url, group, callback.from_user, tur="talaba")

# ─────────────────────────────────────────
# AVTO JADVAL SETUP
# ─────────────────────────────────────────
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
    await callback.message.edit_text(
        t("select_time", chat_id, now=now_str), reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("autotime_"), AutoSchedule.time)
async def cb_autotime_btn(callback: types.CallbackQuery, state: FSMContext):
    vaqt = callback.data.split("_")[1]
    await _finalize_auto(callback.message, state, vaqt, callback.message.chat)

@dp.message(AutoSchedule.time)
async def msg_autotime(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    vaqt = message.text.strip().replace(".", ":").replace("-", ":")
    if re.match(r'^([01]?\d|2[0-3]):[0-5]\d$', vaqt):
        if len(vaqt.split(":")[0]) == 1: vaqt = "0" + vaqt
        await _finalize_auto(message, state, vaqt, message.chat)
    else:
        await message.answer(t("time_invalid", chat_id))

async def _finalize_auto(message, state: FSMContext, vaqt: str, chat):
    chat_id = chat.id
    data = await state.get_data()
    day  = int(data["day"])
    url  = data["url"]
    group = data["group"]
    h, m = map(int, vaqt.split(":"))

    job_id = f"auto_{chat_id}"
    if scheduler.get_job(job_id): scheduler.remove_job(job_id)
    scheduler.add_job(
        job_send_auto, "cron",
        day_of_week=day, hour=h, minute=m,
        args=[chat_id, url, group],
        id=job_id
    )
    save_auto_schedule(chat_id, chat.title or "", group, url, day, vaqt)

    lg = lang(chat_id)
    days = DAYS_UZ if lg == "uz" else DAYS_RU
    await message.answer(t("auto_saved", chat_id, day=days[day], time=vaqt))
    await state.clear()

# ─────────────────────────────────────────
# O'QITUVCHI FLOW
# ─────────────────────────────────────────
@dp.callback_query(F.data == "menu_teacher")
async def cb_teacher(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    await state.set_state(TeacherFlow.search)
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(t("teacher_ask", chat_id), reply_markup=kb.as_markup())

@dp.message(TeacherFlow.search)
async def msg_teacher_search(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    query   = message.text.strip()
    ustozlar = load_json(USTOZ_JSON)

    # To'g'ri moslik
    exact = None
    for name, url in ustozlar.items():
        if query.lower() == name.lower():
            exact = (name, url)
            break

    if exact:
        await state.clear()
        await message.answer(t("teacher_found", chat_id))
        await send_schedule(chat_id, exact[1], exact[0], message.from_user, tur="ustoz")
        return

    # Qisman moslik
    matches = [(n, u) for n, u in ustozlar.items() if query.lower() in n.lower()]

    if not matches:
        await message.answer(t("teacher_not", chat_id))
        return

    if len(matches) == 1:
        await state.clear()
        await message.answer(t("teacher_found", chat_id))
        await send_schedule(chat_id, matches[0][1], matches[0][0], message.from_user, tur="ustoz")
        return

    # Bir nechta natija — tugmalar ko'rsat (max 10)
    kb = InlineKeyboardBuilder()
    for name, url in matches[:10]:
        kb.row(types.InlineKeyboardButton(
            text=name, callback_data=f"tchr_{name}"
        ))
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="go_menu"))
    lg = lang(chat_id)
    text = T[lg]["teachers_list"] + "\n".join([m[0] for m in matches[:10]])
    await message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("tchr_"))
async def cb_teacher_select(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    name = callback.data[5:]
    ustozlar = load_json(USTOZ_JSON)
    url = ustozlar.get(name)
    if not url:
        await callback.answer("Topilmadi!", show_alert=True)
        return
    await state.clear()
    await callback.message.delete()
    await send_schedule(chat_id, url, name, callback.from_user, tur="ustoz")

# ─────────────────────────────────────────
# XONA FLOW
# ─────────────────────────────────────────
@dp.callback_query(F.data == "menu_rooms")
async def cb_rooms(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    xonalar = load_json(XONALAR_JSON)
    if not xonalar:
        await callback.answer("Xonalar ma'lumotlari topilmadi!", show_alert=True)
        return

    # Binolarni ajratib olish
    binolar = set()
    for room in xonalar.keys():
        bino = room.split("-")[0].split("/")[0].strip()
        binolar.add(bino)

    kb = InlineKeyboardBuilder()
    for bino in sorted(binolar, key=lambda x: int(x) if x.isdigit() else 999):
        kb.row(types.InlineKeyboardButton(
            text=f"🏢 {bino}-bino", callback_data=f"bino_{bino}"
        ))
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(t("select_bino", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("bino_"))
async def cb_bino(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    bino = callback.data[5:]
    xonalar = load_json(XONALAR_JSON)

    # Shu binodagi xonalar
    rooms = {name: url for name, url in xonalar.items()
             if name.split("-")[0].split("/")[0].strip() == bino}

    if not rooms:
        await callback.answer("Bu binoda xona topilmadi!", show_alert=True)
        return

    kb = InlineKeyboardBuilder()
    for name in sorted(rooms.keys()):
        kb.add(types.InlineKeyboardButton(
            text=name, callback_data=f"room_{name}"
        ))
    kb.adjust(3)
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="menu_rooms"))
    await callback.message.edit_text(t("select_xona", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("room_"))
async def cb_room_select(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    room_name = callback.data[5:]
    xonalar = load_json(XONALAR_JSON)
    url = xonalar.get(room_name)
    if not url:
        await callback.answer("Topilmadi!", show_alert=True)
        return
    await callback.message.delete()
    await send_schedule(chat_id, url, room_name, callback.from_user, tur="xona")

# ─────────────────────────────────────────
# BOSH XONA FLOW
# ─────────────────────────────────────────
@dp.callback_query(F.data == "menu_free")
async def cb_free_rooms(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    await state.set_state(FreeRoomFlow.time_input)

    # Para tugmalari
    kb = InlineKeyboardBuilder()
    for para, (start, end) in PARA_TIMES.items():
        kb.add(types.InlineKeyboardButton(
            text=f"{para}️⃣ {start}-{end}",
            callback_data=f"freetime_{start}-{end}"
        ))
    kb.adjust(2)
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(t("free_ask", chat_id), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("freetime_"))
async def cb_freetime_btn(callback: types.CallbackQuery, state: FSMContext):
    time_str = callback.data[9:]
    await _handle_free_rooms(callback.message, state, time_str, callback.from_user)

@dp.message(FreeRoomFlow.time_input)
async def msg_free_time(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    raw = message.text.strip()
    # Format: HH:MM-HH:MM
    if re.match(r'^([01]?\d|2[0-3]):[0-5]\d-([01]?\d|2[0-3]):[0-5]\d$', raw):
        await _handle_free_rooms(message, state, raw, message.from_user)
    else:
        await message.answer(t("free_invalid", chat_id))

async def _handle_free_rooms(message, state: FSMContext, time_str: str, user: types.User):
    chat_id = message.chat.id
    await state.clear()

    rooms = get_free_rooms_by_time(time_str)

    if not rooms:
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="menu_free"))
        kb.row(types.InlineKeyboardButton(text=t("menu_btn", chat_id), callback_data="go_menu"))
        try:
            await message.edit_text(t("free_none", chat_id), reply_markup=kb.as_markup())
        except:
            await message.answer(t("free_none", chat_id), reply_markup=kb.as_markup())
        return

    # Binolar bo'yicha guruhlash
    binolar: dict = {}
    for room_name, bino, para in rooms:
        if bino not in binolar: binolar[bino] = []
        binolar[bino].append(room_name)

    text = t("free_title", chat_id, time=time_str)
    xonalar = load_json(XONALAR_JSON)
    kb = InlineKeyboardBuilder()

    for bino in sorted(binolar.keys(), key=lambda x: int(x) if x.isdigit() else 999):
        text += f"🏢 *{bino}-bino:*\n"
        for room in sorted(binolar[bino]):
            text += f"  🚪 {room}\n"
            url = xonalar.get(room)
            if url:
                kb.add(types.InlineKeyboardButton(
                    text=f"📅 {room}", callback_data=f"room_{room}"
                ))
        text += "\n"

    kb.adjust(3)
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="menu_free"))
    kb.row(types.InlineKeyboardButton(text=t("menu_btn", chat_id), callback_data="go_menu"))

    try:
        await message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    except:
        await message.answer(text, reply_markup=kb.as_markup(), parse_mode="Markdown")

    add_log(user.id, "free_rooms_search", time_str)

# ─────────────────────────────────────────
# SAQLANGAN JADVALLAR HANDLERLARI
# ─────────────────────────────────────────

@dp.callback_query(F.data == "dosave_prompt")
async def cb_dosave_prompt(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    uid = callback.from_user.id
    if uid not in pending_save:
        await callback.answer("Avval jadval oching!", show_alert=True)
        return
    await state.set_state(SaveFlow.naming)
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(
        text=pending_save[uid]["name"],
        callback_data=f"dosave_default"
    ))
    kb.row(types.InlineKeyboardButton(text=t("back", chat_id), callback_data="go_menu"))
    await callback.message.answer(
        t("save_ask_name", chat_id) + f"\n\n💡 Standart: {pending_save[uid]['name']}",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data == "dosave_default", SaveFlow.naming)
async def cb_dosave_default(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    if uid not in pending_save:
        await callback.answer("Xatolik!", show_alert=True)
        return
    info = pending_save.pop(uid)
    save_schedule(uid, info["name"], info["url"], info["tur"])
    await state.clear()
    await callback.message.edit_text(t("save_ok", callback.message.chat.id, name=info["name"]),
                                      reply_markup=back_menu_kb(callback.message.chat.id))
    add_log(uid, "save_schedule", info["name"])

@dp.message(SaveFlow.naming)
async def msg_save_name(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    chat_id = message.chat.id
    nom = message.text.strip()
    if uid not in pending_save:
        await message.answer("Xatolik! Avval jadval oching.")
        await state.clear()
        return
    info = pending_save.pop(uid)
    save_schedule(uid, nom, info["url"], info["tur"])
    await state.clear()
    await message.answer(t("save_ok", chat_id, name=nom), reply_markup=back_menu_kb(chat_id))
    add_log(uid, "save_schedule", nom)

@dp.callback_query(F.data == "menu_saved")
async def cb_menu_saved(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    uid = callback.from_user.id
    saved = get_saved_schedules(uid)

    if not saved:
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=t("menu_btn", chat_id), callback_data="go_menu"))
        await callback.message.edit_text(t("save_empty", chat_id), reply_markup=kb.as_markup())
        return

    lg = lang(chat_id)
    text = T[lg]["save_list"]
    kb = InlineKeyboardBuilder()

    for i, (nom, url, tur) in enumerate(saved):
        icon = "🎓" if tur == "talaba" else "👨‍🏫"
        text += f"{icon} {nom}\n"
        kb.row(
            types.InlineKeyboardButton(
                text=f"📅 {nom[:20]}",
                callback_data=f"svopen_{i}"
            ),
            types.InlineKeyboardButton(
                text="🗑",
                callback_data=f"svdel_{nom[:30]}"
            )
        )

    # pending_save_list da saqlash (index uchun)
    pending_save[f"list_{uid}"] = saved

    kb.row(types.InlineKeyboardButton(text=t("menu_btn", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("svopen_"))
async def cb_svopen(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    uid = callback.from_user.id
    idx = int(callback.data.split("_")[1])
    saved_list = pending_save.get(f"list_{uid}", get_saved_schedules(uid))
    if idx >= len(saved_list):
        await callback.answer("Topilmadi!", show_alert=True)
        return
    nom, url, tur = saved_list[idx]
    await callback.message.delete()
    await send_schedule(chat_id, url, nom, callback.from_user, tur=tur)

@dp.callback_query(F.data.startswith("svdel_"))
async def cb_svdel(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    uid = callback.from_user.id
    nom = callback.data[6:]
    delete_saved_schedule(uid, nom)
    pending_save.pop(f"list_{uid}", None)
    await callback.answer(t("save_deleted", chat_id, name=nom), show_alert=True)
    # Ro'yxatni yangilash
    saved = get_saved_schedules(uid)
    if not saved:
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text=t("menu_btn", chat_id), callback_data="go_menu"))
        await callback.message.edit_text(t("save_empty", chat_id), reply_markup=kb.as_markup())
        return
    lg = lang(chat_id)
    text = T[lg]["save_list"]
    kb = InlineKeyboardBuilder()
    for i, (n, u, tr) in enumerate(saved):
        icon = "🎓" if tr == "talaba" else "👨‍🏫"
        text += f"{icon} {n}\n"
        kb.row(
            types.InlineKeyboardButton(text=f"📅 {n[:20]}", callback_data=f"svopen_{i}"),
            types.InlineKeyboardButton(text="🗑", callback_data=f"svdel_{n[:30]}")
        )
    pending_save[f"list_{uid}"] = saved
    kb.row(types.InlineKeyboardButton(text=t("menu_btn", chat_id), callback_data="go_menu"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

# ─────────────────────────────────────────
# /scan_rooms — admin buyrug'i (qo'lda ishga tushirish)
# ─────────────────────────────────────────
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

@dp.message(Command("scan_rooms"))
async def cmd_scan_rooms(message: types.Message):
    if not is_admin(message.from_user.id): return
    await message.answer("⏳ Xonalar skanerlash boshlandi...")
    await job_scan_free_rooms()
    count = len(load_json(XONALAR_JSON))
    await message.answer(t("free_updated", message.chat.id, count=count))

# ─────────────────────────────────────────
# BROADCAST — admin ommaviy xabar
# ─────────────────────────────────────────
class BroadcastFlow(StatesGroup):
    waiting = State()   # xabar kutilmoqda

def get_all_user_ids() -> list:
    """Sheets dan barcha foydalanuvchi IDlarini olish"""
    ws = _sheets.get("users")
    if not ws: return []
    try:
        ids = ws.col_values(2)[1:]  # header ni o'tkazib yuborish
        return [int(i) for i in ids if i.strip().isdigit()]
    except: return []

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(BroadcastFlow.waiting)
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="❌ Bekor qilish", callback_data="broadcast_cancel"))
    await message.answer(
        "📢 *Broadcast rejimi*\n\n"
        "Quyidagilardan birini yuboring:\n"
        "• Matn xabar\n"
        "• Rasm + matn (caption)\n"
        "• Hujjat/fayl\n"
        "• Kanaldan forward qiling\n\n"
        "Barcha foydalanuvchilarga yuboriladi!",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "broadcast_cancel")
async def cb_broadcast_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Broadcast bekor qilindi.")

@dp.message(BroadcastFlow.waiting)
async def broadcast_receive(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    await state.clear()
    user_ids = get_all_user_ids()
    if not user_ids:
        await message.answer("❌ Foydalanuvchilar topilmadi!")
        return

    status = await message.answer(f"⏳ Yuborilmoqda... 0/{len(user_ids)}")
    ok, fail = 0, 0

    for uid in user_ids:
        try:
            # Forward (kanaldan yoki boshqa xabar)
            if message.forward_origin or message.forward_from or message.forward_from_chat:
                await bot.forward_message(
                    chat_id=uid,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id
                )
            # Rasm + matn
            elif message.photo:
                await bot.send_photo(
                    uid,
                    message.photo[-1].file_id,
                    caption=message.caption or "",
                    parse_mode="Markdown"
                )
            # Video
            elif message.video:
                await bot.send_video(
                    uid,
                    message.video.file_id,
                    caption=message.caption or "",
                    parse_mode="Markdown"
                )
            # Hujjat/fayl
            elif message.document:
                await bot.send_document(
                    uid,
                    message.document.file_id,
                    caption=message.caption or "",
                    parse_mode="Markdown"
                )
            # Ovozli xabar
            elif message.voice:
                await bot.send_voice(uid, message.voice.file_id)
            # Sticker
            elif message.sticker:
                await bot.send_sticker(uid, message.sticker.file_id)
            # Faqat matn
            elif message.text:
                await bot.send_message(uid, message.text, parse_mode="Markdown")
            else:
                await bot.copy_message(uid, message.chat.id, message.message_id)

            ok += 1
        except Exception as e:
            fail += 1
            log.warning(f"Broadcast failed [{uid}]: {e}")

        # Har 20 ta da status yangilash
        if (ok + fail) % 20 == 0:
            try:
                await status.edit_text(
                    f"⏳ Yuborilmoqda... {ok+fail}/{len(user_ids)}\n✅ {ok} | ❌ {fail}"
                )
            except: pass

        await asyncio.sleep(0.05)  # Telegram rate limit

    await status.edit_text(
        f"📢 *Broadcast tugadi!*\n\n"
        f"✅ Muvaffaqiyatli: {ok}\n"
        f"❌ Xatolik: {fail}\n"
        f"👥 Jami: {len(user_ids)}",
        parse_mode="Markdown"
    )
    add_log(message.from_user.id, "broadcast", f"ok={ok}, fail={fail}")

# ─────────────────────────────────────────
# ADMIN PANEL
# ─────────────────────────────────────────
BOT_START_TIME = datetime.now(TASHKENT_TZ)

def admin_panel_kb():
    kb = InlineKeyboardBuilder()
    kb.row(
        types.InlineKeyboardButton(text="📊 Statistika",      callback_data="adm_stats"),
        types.InlineKeyboardButton(text="🤖 Bot ma'lumoti",   callback_data="adm_info"),
    )
    kb.row(
        types.InlineKeyboardButton(text="📅 Avto jadvallar",  callback_data="adm_auto"),
        types.InlineKeyboardButton(text="📢 Broadcast",       callback_data="adm_broadcast"),
    )
    kb.row(
        types.InlineKeyboardButton(text="🔍 Xona skaner",     callback_data="adm_scan"),
    )
    return kb.as_markup()

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "👨‍💼 *Admin panel*\n\nKerakli bo'limni tanlang:",
        reply_markup=admin_panel_kb(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "adm_menu")
async def cb_adm_menu(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text(
        "👨‍💼 *Admin panel*\n\nKerakli bo'limni tanlang:",
        reply_markup=admin_panel_kb(),
        parse_mode="Markdown"
    )

# --- STATISTIKA ---
@dp.callback_query(F.data == "adm_stats")
async def cb_adm_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.answer("⏳ Yuklanmoqda...")

    try:
        # Foydalanuvchilar
        ws_users = _sheets.get("users")
        total_users = len(ws_users.col_values(2)) - 1 if ws_users else 0

        # Avto jadvallar
        ws_auto = _sheets.get("auto")
        total_auto = len(ws_auto.get_all_values()) - 1 if ws_auto else 0

        # Saqlangan jadvallar
        ws_saved = _sheets.get("saved")
        total_saved = len(ws_saved.get_all_values()) - 1 if ws_saved else 0

        # Loglar (bugungi)
        ws_logs = _sheets.get("logs")
        today = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y")
        today_logs = 0
        if ws_logs:
            all_logs = ws_logs.col_values(1)[1:]
            today_logs = sum(1 for d in all_logs if d.startswith(today))

        # Screenshot cache
        cache_count = len(screenshot_cache)

        text = (
            f"📊 *Statistika*\n\n"
            f"👥 Jami foydalanuvchilar: *{total_users}*\n"
            f"📅 Avto jadval ulangan guruhlar: *{total_auto}*\n"
            f"⭐ Saqlangan jadvallar: *{total_saved}*\n"
            f"📝 Bugungi so'rovlar: *{today_logs}*\n"
            f"🖼 Screenshot cache: *{cache_count}* ta\n"
        )
    except Exception as e:
        text = f"❌ Xatolik: {e}"

    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="🔄 Yangilash", callback_data="adm_stats"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Orqaga",   callback_data="adm_menu"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")

# --- BOT MA'LUMOTI ---
@dp.callback_query(F.data == "adm_info")
async def cb_adm_info(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return

    now = datetime.now(TASHKENT_TZ)
    uptime = now - BOT_START_TIME
    days    = uptime.days
    hours   = uptime.seconds // 3600
    minutes = (uptime.seconds % 3600) // 60
    seconds = uptime.seconds % 60

    # Scheduler joblar
    jobs = scheduler.get_jobs()
    auto_jobs = [j for j in jobs if j.id.startswith("auto_")]

    text = (
        f"🤖 *Bot ma'lumoti*\n\n"
        f"⏱ Uptime: *{days}k {hours}s {minutes}d {seconds}sek*\n"
        f"🕐 Hozirgi vaqt: *{now.strftime('%d.%m.%Y %H:%M:%S')}*\n\n"
        f"⚙️ Scheduler:\n"
        f"  • Faol joblar: *{len(jobs)}* ta\n"
        f"  • Avto jadval: *{len(auto_jobs)}* ta guruh\n"
        f"  • Xona skaner: har kuni *05:30*\n\n"
        f"📁 JSON fayllar:\n"
        f"  • talaba.json: *{'✅' if os.path.exists(TALABA_JSON) else '❌'}*\n"
        f"  • ustoz.json: *{'✅' if os.path.exists(USTOZ_JSON) else '❌'}*\n"
        f"  • xonalar.json: *{'✅' if os.path.exists(XONALAR_JSON) else '❌'}*\n\n"
        f"🗄 Google Sheets: *{'✅ Ulangan' if _sheets else '❌ Ulanmagan'}*\n"
    )

    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="🔄 Yangilash", callback_data="adm_info"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Orqaga",   callback_data="adm_menu"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")

# --- AVTO JADVALLAR ---
@dp.callback_query(F.data == "adm_auto")
async def cb_adm_auto(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.answer("⏳ Yuklanmoqda...")

    rows = get_auto_schedules()
    kb = InlineKeyboardBuilder()

    if not rows:
        text = "📅 *Avto jadvallar*\n\n😔 Hozircha hech qaysi guruhda sozlanmagan."
    else:
        text = f"📅 *Avto jadvallar* — {len(rows)} ta\n\n"
        lg_days = DAYS_UZ
        for row in rows:
            try:
                chat_id   = row[0]
                chat_title = row[1] or "Noma'lum guruh"
                group     = row[2]
                day_idx   = int(row[4])
                vaqt      = row[5]
                day_name  = lg_days.get(day_idx, str(day_idx))
                text += f"👥 *{chat_title}*\n"
                text += f"   📌 {group} | {day_name} {vaqt}\n\n"
                kb.row(types.InlineKeyboardButton(
                    text=f"🗑 {chat_title[:25]}",
                    callback_data=f"adm_delauto_{chat_id}"
                ))
            except: continue

    kb.row(types.InlineKeyboardButton(text="⬅️ Orqaga", callback_data="adm_menu"))
    try:
        await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    except:
        await callback.message.edit_text(text[:4000], reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_delauto_"))
async def cb_adm_delauto(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    chat_id_str = callback.data.split("_")[2]

    # Schedulerdan o'chirish
    job_id = f"auto_{chat_id_str}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    # Sheetsdan o'chirish
    ws = _sheets.get("auto")
    if ws:
        try:
            ids = ws.col_values(1)
            if chat_id_str in ids:
                idx = ids.index(chat_id_str) + 1
                ws.delete_rows(idx)
        except Exception as e:
            log.error(f"delauto sheets error: {e}")

    await callback.answer("✅ Avto jadval o'chirildi!", show_alert=True)
    # Ro'yxatni yangilash
    await cb_adm_auto(callback)

# --- BROADCAST (admin panel orqali) ---
@dp.callback_query(F.data == "adm_broadcast")
async def cb_adm_broadcast(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.set_state(BroadcastFlow.waiting)
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="❌ Bekor qilish", callback_data="broadcast_cancel"))
    await callback.message.edit_text(
        "📢 *Broadcast rejimi*\n\n"
        "Quyidagilardan birini yuboring:\n"
        "• Matn xabar\n"
        "• Rasm + matn (caption)\n"
        "• Hujjat/fayl\n"
        "• Kanaldan forward\n\n"
        "Barcha foydalanuvchilarga yuboriladi!",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown"
    )

# --- XONA SKANER ---
@dp.callback_query(F.data == "adm_scan")
async def cb_adm_scan(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("⏳ Xonalar skanerlash boshlandi...\nBu bir necha daqiqa olishi mumkin.")
    await job_scan_free_rooms()
    count = len(load_json(XONALAR_JSON))
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="⬅️ Orqaga", callback_data="adm_menu"))
    await callback.message.edit_text(
        f"✅ Skanerlash tugadi!\n{count} ta xona tekshirildi.",
        reply_markup=kb.as_markup()
    )

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
async def main():
    log.info("Bot ishga tushmoqda...")

    # Sheets ulash
    init_sheets()

    # Avto jadvallarni tiklash
    restore_auto_schedules()

    # Schedulerga bosh xona skanerlashni qo'shish (05:30 da)
    scheduler.add_job(
        job_scan_free_rooms, "cron",
        hour=5, minute=30, id="scan_rooms"
    )

    if not scheduler.running:
        scheduler.start()

    await bot.delete_webhook(drop_pending_updates=True)
    log.info("Bot polling boshlandi ✅")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
