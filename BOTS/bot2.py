import asyncio
import logging
import os
import csv
import time
import threading
from aiohttp import web
import functools
import traceback
from datetime import datetime, timedelta
import pymongo
import pytz 
from collections import Counter
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter, CommandObject
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest, TelegramConflictError

# ==========================================
# ⚡ CONFIGURATION (GHOST PROTOCOL)
# ==========================================
MANAGER_BOT_TOKEN = os.getenv("MANAGER_BOT_TOKEN")
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

# SECURE OWNER ID FETCH
try:
    OWNER_ID = int(os.getenv("OWNER_ID", 0))
except (TypeError, ValueError):
    OWNER_ID = 0

if not all([MANAGER_BOT_TOKEN, MAIN_BOT_TOKEN, MONGO_URI, OWNER_ID]):
    print("❌ CRITICAL ERROR: Environment variables missing in Render! Check OWNER_ID, Tokens, and URI.")

# Timezone for Intelligence Reports
IST = pytz.timezone('Asia/Kolkata')

# ==========================================
# 🛠 SYSTEM SETUP
# ==========================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

manager_bot = Bot(token=MANAGER_BOT_TOKEN)
worker_bot = Bot(token=MAIN_BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# GLOBAL TRACKERS (IRON DOME)
ERROR_COUNTER = 0
LAST_ERROR_TIME = time.time()
LAST_REPORT_DATE = None 
LAST_INVENTORY_CHECK = 0

# STATES
class ManagementState(StatesGroup):
    waiting_for_find_query = State()
    waiting_for_delete_id = State()

class BroadcastState(StatesGroup):
    waiting_for_filter = State()
    waiting_for_message = State()
    confirm_send = State()
    waiting_for_edit = State()

class AdminState(StatesGroup):
    waiting_for_id = State()
    waiting_for_name = State()

class BanState(StatesGroup):
    waiting_for_id = State()

class SniperState(StatesGroup):
    waiting_for_target_id = State()
    waiting_for_message = State()
    confirm_send = State()

# --- MONGODB CONNECTION ---
print("🔄 Synchronizing Manager with MSANode Database...")
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["MSANodeDB"]
    
    col_users = db["user_logs"]
    col_admins = db["admins"]
    col_settings = db["settings"]
    col_active = db["active_content"] 
    col_viral = db["viral_videos"]
    col_reels = db["viral_reels"]
    col_banned = db["banned_users"]
    col_broadcast_logs = db["broadcast_logs"]
    
    print("✅ MSANode Data Core: CONNECTED")
except Exception as e:
    print(f"❌ DATABASE OFFLINE: {e}")
    exit()

# --- RENDER PORT BINDER (SHIELD) ---
async def handle_health(request):
    return web.Response(text="MSANODE MANAGER CORE IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")

# ==========================================
# 🛡️ IRON DOME & HELPERS (UNREDUCED)
# ==========================================
async def send_alert(msg):
    """Sends critical alerts to Owner."""
    try:
        await manager_bot.send_message(OWNER_ID, f"🚨 **MSANODE SYSTEM ALERT** 🚨\n\n{msg}")
    except: pass

async def emergency_backup():
    """Generates and sends a CSV backup during Panic Protocol."""
    try:
        filename = f"EMERGENCY_BACKUP_{int(time.time())}.csv"
        cursor = col_users.find({}, {"_id": 0})
        df = list(cursor)
        if df:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                csv.DictWriter(f, df[0].keys()).writeheader()
                csv.DictWriter(f, df[0].keys()).writerows(df)
            await manager_bot.send_document(OWNER_ID, FSInputFile(filename), caption="💾 **BLACK BOX DATA RECOVERY**")
            os.remove(filename)
    except Exception as e:
        logger.error(f"Backup Failed: {e}")

def safe_execute(func):
    """Retries functions, auto-heals, and triggers Black Box."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        global ERROR_COUNTER, LAST_ERROR_TIME
        retries = 3
        while retries > 0:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                retries -= 1
                ERROR_COUNTER += 1
                if time.time() - LAST_ERROR_TIME < 60 and ERROR_COUNTER > 5:
                    col_settings.update_one({"setting": "maintenance"}, {"$set": {"value": True}}, upsert=True)
                    await emergency_backup()
                    await send_alert(f"**PANIC PROTOCOL ACTIVE**\n`{traceback.format_exc()}`")
                    ERROR_COUNTER = 0 
                LAST_ERROR_TIME = time.time()
                await asyncio.sleep(1)
        return None 
    return wrapper

def is_admin(user_id):
    if user_id == OWNER_ID: return True
    try:
        return col_admins.find_one({"user_id": str(user_id)}) is not None
    except: return False

# ==========================================
# 👁️ SUPERVISOR WATCHDOG (5 MINUTE SCAN)
# ==========================================
@safe_execute
async def supervisor_routine():
    global LAST_REPORT_DATE, LAST_INVENTORY_CHECK
    print("👁️ Supervisor Watchdog Active...")
    last_health_check = 0
    while True:
        now_time = time.time()
        now_ist = datetime.now(IST)
        
        if now_time - last_health_check >= 300: 
            try:
                await manager_bot.get_me()
                col_users.find_one()
                logger.info("✅ Watchdog Heartbeat: STABLE")
            except Exception as e:
                await send_alert(f"**System Failure Detected**\n{e}")
            last_health_check = now_time

        if now_time - LAST_INVENTORY_CHECK >= 3600: 
            count = col_active.count_documents({})
            if count < 5:
                await send_alert(f"📉 **LOW VAULT INVENTORY**")
            LAST_INVENTORY_CHECK = now_time

        current_date_str = now_ist.strftime("%Y-%m-%d")
        if now_ist.hour == 8 and now_ist.minute == 40 and LAST_REPORT_DATE != current_date_str:
            total_u = col_users.count_documents({})
            report = (
                f"🌅 **MSANODE DAILY EMPIRE AUDIT**\n"
                f"📅 `{now_ist.strftime('%d-%m-%Y %I:%M %p')}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"✅ **Command Hub:** Active\n"
                f"👥 **Army Size:** `{total_u}`"
            )
            await manager_bot.send_message(OWNER_ID, report)
            LAST_REPORT_DATE = current_date_str
        await asyncio.sleep(30) 

# --- SCHEDULED TASKS ---
@safe_execute
async def scheduled_health_check():
    while True:
        try:
            now = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
            col_settings.update_one({"setting": "manager_status"}, {"$set": {"last_check": now, "status": "Online"}}, upsert=True)
        except: pass
        await asyncio.sleep(300)

@safe_execute
async def scheduled_pruning_cleanup():
    while True:
        await asyncio.sleep(43200) # 12 Hours
        try: col_users.delete_many({"status": "LEFT"})
        except: pass

def back_kb(): 
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Hub", callback_data="btn_refresh")
    return kb.as_markup()

# ==========================================
# 👑 THE HUB UI (DASHBOARD)
# ==========================================
@safe_execute
async def show_dashboard_ui(message_obj, user_id, is_edit=False):
    if not is_admin(user_id): return
    
    total_u = col_users.count_documents({})
    m_doc = col_settings.find_one({"setting": "maintenance"})
    status = "🟠 LOCKDOWN" if m_doc and m_doc.get("value") == True else "🟢 NORMAL"

    text = (
        f"👑 **MSANODE SUPREME COMMAND HUB**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 **Operatives:** `{total_u}`\n"
        f"🛠 **System:** {status}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardBuilder()
    # Row 1: Direct Comms
    kb.row(InlineKeyboardButton(text="📢 Broadcast", callback_data="btn_broadcast"), 
           InlineKeyboardButton(text="🎯 Snipe DM", callback_data="btn_sniper"))
    # Row 2: Management
    kb.row(InlineKeyboardButton(text="📋 List All", callback_data="btn_list_all"), 
           InlineKeyboardButton(text="🔎 Find User", callback_data="btn_find_op"))
    # Row 3: Intelligence
    kb.row(InlineKeyboardButton(text="📈 Traffic", callback_data="btn_traffic"), 
           InlineKeyboardButton(text="📊 Supreme Audit", callback_data="btn_supreme_stats"))
    # Row 4: Security
    kb.row(InlineKeyboardButton(text="🗑 Delete User", callback_data="btn_delete_user"), 
           InlineKeyboardButton(text="🚫 Ban Menu", callback_data="btn_ban_menu"))
    # Row 5: Systems
    kb.row(InlineKeyboardButton(text="💾 Backup", callback_data="btn_backup"), 
           InlineKeyboardButton(text="🩺 Diagnosis", callback_data="btn_diagnosis"))
    # Row 6: Configuration
    kb.row(InlineKeyboardButton(text="🛡️ Admins", callback_data="btn_add_admin"),
           InlineKeyboardButton(text="🛠 Lockdown", callback_data="btn_maint_toggle"))
    # Row 7: Session
    kb.row(InlineKeyboardButton(text="🔄 Sync Hub", callback_data="btn_refresh"), 
           InlineKeyboardButton(text="💤 Sleep", callback_data="btn_sleep"))
    
    try:
        if is_edit: await message_obj.edit_text(text, reply_markup=kb.as_markup())
        else: await message_obj.answer(text, reply_markup=kb.as_markup())
    except: pass

@dp.message(Command("start"), StateFilter("*"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear() 
    if message.from_user.id == (await manager_bot.get_me()).id: return
    await show_dashboard_ui(message, message.from_user.id)

@dp.callback_query(F.data == "btn_refresh")
async def hub_refresh(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=True)

# ==========================================
# 🛠 BUTTON DRIVEN LOGIC (UNREDUCED)
# ==========================================

@dp.callback_query(F.data == "btn_list_all")
async def hub_list_all(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    users = list(col_users.find({}, {"username": 1, "user_id": 1, "_id": 0}))
    if not users: return await callback.answer("📂 Vault Empty.")
    
    text = "📋 **MSANODE DIRECTORY**\n━━━━━━━━━━━━━━━━━━\n"
    count = 0
    for u in users:
        count += 1
        text += f"{count}. {u.get('username') or 'Anonymous'} | `{u.get('user_id')}`\n"
        if len(text) > 3800:
            await callback.message.answer(text)
            text = "━━━━━━━━━━━━━━━━━━\n"
    text += f"━━━━━━━━━━━━━━━━━━\n👥 Total Army: `{count}`"
    await callback.message.answer(text, reply_markup=back_kb())

@dp.callback_query(F.data == "btn_supreme_stats")
async def hub_supreme_audit(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    u = col_users.count_documents({}); m = col_active.count_documents({})
    yt = col_viral.count_documents({}); ig = col_reels.count_documents({})
    b = col_banned.count_documents({})
    fmt_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
    audit = (
        f"📊 **MSANODE SUPREME AUDIT**\n"
        f"📅 `{fmt_time}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 Army: `{u}`\n"
        f"🔑 M-Codes: `{m}`\n"
        f"🎥 YT Videos: `{yt}`\n"
        f"📸 IG Reels: `{ig}`\n"
        f"🚫 Banned: `{b}`\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    await callback.message.edit_text(audit, reply_markup=back_kb())

@dp.callback_query(F.data == "btn_find_op")
async def hub_find_trigger(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("🔎 **SEARCH PROTOCOL**\nEnter Username (@) or ID to locate operative:")
    await state.set_state(ManagementState.waiting_for_find_query)

@dp.message(ManagementState.waiting_for_find_query)
async def process_hub_find(message: types.Message, state: FSMContext):
    clean_q = message.text.replace("@", "").strip()
    user = col_users.find_one({"$or": [{"user_id": clean_q}, {"username": {"$regex": f"^{clean_q}$", "$options": "i"}}]})
    if not user: return await message.answer("❌ No Operative found.", reply_markup=back_kb())
    
    dossier = (
        f"🕵️ **OPERATIVE DOSSIER**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 Name: {user.get('first_name')}\n"
        f"🆔 Username: {user.get('username')}\n"
        f"🔢 User ID: `{user.get('user_id')}`\n"
        f"📅 Joined: {user.get('joined_date')}\n"
        f"📍 Origin: {user.get('source')}\n"
        f"🛡️ Status: {user.get('status')}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    await message.answer(dossier, reply_markup=back_kb())
    await state.clear()

@dp.callback_query(F.data == "btn_delete_user")
async def hub_delete_trigger(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("🗑 **PURGE PROTOCOL**\nEnter User ID to wipe from empire:")
    await state.set_state(ManagementState.waiting_for_delete_id)

@dp.message(ManagementState.waiting_for_delete_id)
async def process_hub_delete(message: types.Message, state: FSMContext):
    res = col_users.delete_one({"user_id": message.text.strip()})
    text = f"✅ **Operative `{message.text}` Purged.**" if res.deleted_count > 0 else "❌ ID not found."
    await message.answer(text, reply_markup=back_kb())
    await state.clear()

# ==========================================
# 📢 BROADCAST & TARGETING RADAR (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_broadcast")
async def broadcast_menu(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    last = col_broadcast_logs.find_one(sort=[("_id", -1)])
    kb = InlineKeyboardBuilder()
    kb.button(text="🎯 New Transmission", callback_data="start_broadcast_new")
    if last:
        kb.button(text="✏️ Edit Last", callback_data="edit_last_broadcast")
        kb.button(text="🔥 Purge Last", callback_data="unsend_last_broadcast")
    kb.button(text="🔙 Back", callback_data="btn_refresh")
    kb.adjust(1)
    await callback.message.edit_text(f"📢 **Transmission Manager**", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "start_broadcast_new")
async def start_broadcast(callback: types.CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="👥 ALL", callback_data="target_all"),
           InlineKeyboardButton(text="🔴 YT Path", callback_data="target_yt"),
           InlineKeyboardButton(text="📸 IG Path", callback_data="target_ig"))
    kb.row(InlineKeyboardButton(text="❌ ABORT", callback_data="btn_broadcast"))
    await callback.message.edit_text("🎯 **Select Target Group:**", reply_markup=kb.as_markup())
    await state.set_state(BroadcastState.waiting_for_filter)

@dp.callback_query(BroadcastState.waiting_for_filter, F.data.startswith("target_"))
async def select_filter(callback: types.CallbackQuery, state: FSMContext):
    target = callback.data.split("_")[1]
    await state.update_data(target_filter=target)
    await callback.message.edit_text(f"📝 **Enter Content to Broadcast:**")
    await state.set_state(BroadcastState.waiting_for_message)

@dp.message(BroadcastState.waiting_for_message)
async def receive_broadcast(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    ctype = "text"; path = None; text = message.text or message.caption or ""
    if not message.text: 
        file_obj = None; ext = "dat"
        if message.photo: ctype = "photo"; file_obj = message.photo[-1]; ext="jpg"
        elif message.video: ctype = "video"; file_obj = message.video; ext="mp4"
        elif message.document: ctype = "document"; file_obj = message.document; ext="pdf"
        if file_obj: 
            await message.answer("📥 **Downloading Buffers...**")
            path = f"t_{message.from_user.id}.{ext}"
            await manager_bot.download(file_obj, destination=path)
            
    await state.update_data(ctype=ctype, text=text, path=path)
    kb = InlineKeyboardBuilder().button(text="🚀 FIRE", callback_data="confirm_send").button(text="❌ ABORT", callback_data="cancel_send").as_markup()
    await message.answer(f"📢 **Ready for Transmission?**", reply_markup=kb)
    await state.set_state(BroadcastState.confirm_send)

@dp.callback_query(F.data == "confirm_send")
async def execute_broadcast(callback: types.CallbackQuery, state: FSMContext):
    d = await state.get_data(); t_filter = d.get('target_filter', 'all')
    query = {"status": "Active"}
    if t_filter == "yt": query["source"] = "YouTube"
    elif t_filter == "ig": query["source"] = "Instagram"
    
    total = col_users.count_documents(query)
    radar = await callback.message.edit_text(f"🚀 **TRANSMITTING**\n📡 Radar: `0 / {total}`")
    
    file_id = None; sent = 0; blocked = 0; path = d.get('path'); msg_ids = []
    
    try:
        cursor = col_users.find(query, {"user_id": 1})
        for doc in cursor:
            uid = doc.get("user_id")
            try:
                media = file_id or (FSInputFile(path) if path else None)
                m = None
                if d['ctype'] == 'text': m = await worker_bot.send_message(uid, d['text'])
                else:
                    if d['ctype'] == 'photo': m = await worker_bot.send_photo(uid, media, caption=d['text'])
                    elif d['ctype'] == 'video': m = await worker_bot.send_video(uid, media, caption=d['text'])
                    elif d['ctype'] == 'document': m = await worker_bot.send_document(uid, media, caption=d['text'])
                if m:
                    msg_ids.append({"chat_id": int(uid), "message_id": m.message_id})
                    if not file_id:
                        if d['ctype'] == 'photo': file_id = m.photo[-1].file_id
                        elif d['ctype'] == 'video': file_id = m.video.file_id
                        elif d['ctype'] == 'document': file_id = m.document.file_id
                sent += 1
                if sent % 10 == 0:
                    try: await radar.edit_text(f"🚀 **LIVE RADAR**\n📡 Progress: `{sent} / {total}`\n🛡️ Blocked: `{blocked}`")
                    except: pass
                await asyncio.sleep(0.05) 
            except TelegramForbiddenError: 
                blocked += 1
                col_users.update_one({"user_id": uid}, {"$set": {"status": "BLOCKED"}})
            except: pass
        if msg_ids: 
            col_broadcast_logs.insert_one({"date": datetime.now(IST).strftime("%d-%m-%Y %I:%M %p"), "messages": msg_ids, "type": d['ctype'], "original_text": d['text']})
        await callback.message.answer(f"✅ Finished. Sent: {sent} | Failure: {blocked}")
    except Exception as e: await callback.message.answer(f"❌ Error: {e}")
    if path and os.path.exists(path): os.remove(path)
    await state.clear(); await show_dashboard_ui(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "unsend_last_broadcast")
async def unsend_last(callback: types.CallbackQuery):
    await callback.message.edit_text("⏳ **Recalling messages...**")
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    if not last_log: return await callback.message.edit_text("❌ No history.")
    deleted = 0
    for entry in last_log.get("messages", []):
        try:
            await worker_bot.delete_message(chat_id=entry['chat_id'], message_id=entry['message_id'])
            deleted += 1
            await asyncio.sleep(0.03)
        except: pass
    col_broadcast_logs.delete_one({"_id": last_log["_id"]})
    await callback.message.answer(f"✅ Recalled {deleted} transmissions.")
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=False)

@dp.callback_query(F.data == "edit_last_broadcast")
async def edit_last_start(callback: types.CallbackQuery, state: FSMContext):
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    if not last_log or last_log.get("type") != "text": return await callback.answer("❌ Intelligence must be text.")
    await callback.message.edit_text(f"📝 **Current Intelligence:**\n{last_log.get('original_text')}\n\n👇 **NEW Intelligence:**")
    await state.set_state(BroadcastState.waiting_for_edit)

@dp.message(BroadcastState.waiting_for_edit)
async def edit_last_execute(message: types.Message, state: FSMContext):
    new_text = message.text; last_log = col_broadcast_logs.find_one(sort=[("_id", -1)]); edited = 0
    for entry in last_log.get("messages", []):
        try:
            await worker_bot.edit_message_text(text=new_text, chat_id=entry['chat_id'], message_id=entry['message_id'])
            edited += 1
            await asyncio.sleep(0.03)
        except: pass
    col_broadcast_logs.update_one({"_id": last_log["_id"]}, {"$set": {"original_text": new_text}})
    await message.answer(f"✅ Patched {edited} transmissions."); await state.clear()
    await show_dashboard_ui(message, message.from_user.id, is_edit=False)

# ==========================================
# 🛡️ SECURITY & ADMIN (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_ban_menu")
async def ban_menu(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("🚫 **PURGE PROTOCOL**\nEnter User ID to Ban:")
    await state.set_state(BanState.waiting_for_id)

@dp.message(BanState.waiting_for_id)
async def execute_ban(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    target_id = message.text.strip()
    if not target_id.isdigit(): return await message.answer("❌ Invalid ID format.")
    col_banned.update_one({"user_id": target_id}, {"$set": {"banned_at": datetime.now(IST), "banned_by": message.from_user.first_name}}, upsert=True)
    col_users.update_one({"user_id": target_id}, {"$set": {"status": "BLOCKED"}})
    await message.answer(f"⛔ **Operative {target_id} purged.**"); await state.clear()
    await show_dashboard_ui(message, message.from_user.id)

@dp.callback_query(F.data == "btn_add_admin")
async def add_admin_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("👮 **New Admin ID:**")
    await state.set_state(AdminState.waiting_for_id)

@dp.message(AdminState.waiting_for_id)
async def add_admin_id(message: types.Message, state: FSMContext):
    await state.update_data(new_id=message.text); await message.answer("👤 **Identity Label:**")
    await state.set_state(AdminState.waiting_for_name)

@dp.message(AdminState.waiting_for_name)
async def add_admin_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    col_admins.insert_one({"user_id": data['new_id'], "name": message.text, "role": "Admin"})
    await message.answer("✅ Clearance Granted."); await state.clear()
    await show_dashboard_ui(message, message.from_user.id)

# ==========================================
# 🩺 DIAGNOSTICS & BACKUP (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_diagnosis")
async def run_diagnosis(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("🩺 **Scanning MSANode Fabric...**")
    try:
        ts = time.time(); u = col_users.count_documents({}); ca = col_active.count_documents({})
        lat = round((time.time()-ts)*1000, 2)
        report = f"🩺 **DIAGNOSTICS**\n━━━━━━━━━━━━━━━━━━\n📦 Data Core: Stable ({lat}ms)\n👥 Army: `{u}`\n🔑 Vaults: `{ca}`\n🤖 Status: SHIELD ACTIVE"
        await callback.message.edit_text(report, reply_markup=back_kb())
    except Exception as e: await callback.message.edit_text(f"❌ Error: {e}", reply_markup=back_kb())

@dp.callback_query(F.data == "btn_maint_toggle")
async def toggle_maintenance(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    curr = col_settings.find_one({"setting": "maintenance"})
    new_val = not (curr and curr.get("value"))
    col_settings.update_one({"setting": "maintenance"}, {"$set": {"value": new_val}}, upsert=True)
    await callback.answer(f"Lockdown: {'ENGAGED' if new_val else 'OFF'}")
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=True)

@dp.callback_query(F.data == "btn_backup")
async def backup_data(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("⏳ **Securing Backup...**")
    try:
        df = list(col_users.find({}, {"_id": 0}))
        if df:
            with open("Vault_Backup.csv", 'w', newline='', encoding='utf-8') as f: 
                csv.DictWriter(f, df[0].keys()).writeheader()
                csv.DictWriter(f, df[0].keys()).writerows(df)
            await callback.message.answer_document(FSInputFile("Vault_Backup.csv"), caption="💾 **ENCRYPTED BACKUP**")
            os.remove("Vault_Backup.csv")
    except: await callback.message.answer("❌ Protocol Failure")
    await show_dashboard_ui(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "btn_traffic")
async def hub_traffic(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    raw = list(col_users.aggregate([{"$group": {"_id": "$source", "count": {"$sum": 1}}}]))
    t = {r['_id']: r['count'] for r in raw}
    rep = f"📈 **INTELLIGENCE TRAFFIC**\n━━━━━━━━━━━━━━━━━━\n🔴 YT: `{t.get('YouTube', 0)}`\n📸 IG: `{t.get('Instagram', 0)}`\n📊 Total Flow: `{sum(t.values())}`"
    await callback.message.edit_text(rep, reply_markup=back_kb())

@dp.callback_query(F.data == "btn_sniper")
async def start_sniper(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("🎯 **Sniper Protocol**\nEnter User ID to contact:")
    await state.set_state(SniperState.waiting_for_target_id)

@dp.message(SniperState.waiting_for_target_id)
async def sniper_id(message: types.Message, state: FSMContext):
    await state.update_data(target_id=message.text); await message.answer("📝 **Intelligence DM Content:**")
    await state.set_state(SniperState.waiting_for_message)

@dp.message(SniperState.waiting_for_message)
async def sniper_msg(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text); kb = InlineKeyboardBuilder().button(text="🚀 EXECUTE SNIPE", callback_data="confirm_sniper").as_markup()
    await message.answer("Verify Transmission?", reply_markup=kb); await state.set_state(SniperState.confirm_send)

@dp.callback_query(F.data == "confirm_sniper")
async def execute_sniper(callback: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try: 
        await worker_bot.send_message(chat_id=d['target_id'], text=d['text'])
        await callback.message.answer("✅ Snipe Delivered.")
    except Exception as e: await callback.message.answer(f"❌ Failed: {e}")
    await state.clear(); await show_dashboard_ui(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "cancel_send")
async def cancel_op(callback: types.CallbackQuery, state: FSMContext):
    await state.clear(); await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=True)

@dp.callback_query(F.data == "btn_sleep")
async def sleep_mode(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.message.delete()
    await callback.message.answer("💤 **Command Terminal Suspended.**\nType `/start` to re-engage.")

@dp.callback_query(F.data == "btn_help")
async def help_guide(callback: types.CallbackQuery):
    h = "📘 **APEX HUB PROTOCOLS**\n━━━━━━━━━━━━━━━━━━\n• **List:** View all Recruits.\n• **Find:** Surgical search dossier.\n• **Delete:** Purge operative.\n• **Audit:** Supreme stats count.\n• **Radar:** Live transmission tracking."
    await callback.message.edit_text(h, reply_markup=back_kb(), parse_mode="Markdown")

# ==========================================
# 🚀 NUCLEAR MAIN EXECUTION (GHOST SHIELD)
# ==========================================
async def main():
    print("👑 MSANode Command Center (Apex Edition) is Online...")
    try: await manager_bot.send_message(OWNER_ID, "🟢 **Command Terminal Activated**\nIron Dome and Nuclear Ghost Protocols are ENGAGED.")
    except: pass
    await manager_bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(supervisor_routine()) 
    asyncio.create_task(scheduled_health_check())
    asyncio.create_task(scheduled_pruning_cleanup()) 
    await dp.start_polling(manager_bot, skip_updates=True)

if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    while True:
        try:
            time.sleep(2)
            asyncio.run(main())
        except TelegramConflictError:
            print("💀 GHOST DETECTED! Waiting 20 seconds to purge ghost...")
            time.sleep(20)
        except (KeyboardInterrupt, SystemExit):
            print("🛑 Command Hub Stopped Safely")
            break
        except Exception as e:
            print(f"💥 SYSTEM BREACH: {e}")
            traceback.print_exc()
            time.sleep(15)
