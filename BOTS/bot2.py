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

# Timezone for MSANode Intelligence Reports
IST = pytz.timezone('Asia/Kolkata')

# ==========================================
# 🛠 SYSTEM SETUP
# ==========================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

manager_bot = Bot(token=MANAGER_BOT_TOKEN)
worker_bot = Bot(token=MAIN_BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# GLOBAL TRACKERS (IRON DOME INFRASTRUCTURE)
ERROR_COUNTER = 0
LAST_ERROR_TIME = time.time()
LAST_REPORT_DATE = None 
LAST_INVENTORY_CHECK = 0

# STATES
class BroadcastState(StatesGroup):
    waiting_for_filter = State() # NEW: All/YT/IG
    waiting_for_message = State()
    confirm_send = State()
    waiting_for_edit = State()

class SniperState(StatesGroup):
    waiting_for_target_id = State()
    waiting_for_message = State()
    confirm_send = State()

class AdminState(StatesGroup):
    waiting_for_id = State()
    waiting_for_name = State()

class BanState(StatesGroup):
    waiting_for_id = State()

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

# --- RENDER PORT BINDER ---
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
    except:
        pass

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
            await manager_bot.send_document(OWNER_ID, FSInputFile(filename), caption="💾 **BLACK BOX DATA RECOVERY**\nOperative data secured during crash.")
            os.remove(filename)
    except Exception as e:
        logger.error(f"Backup Failed: {e}")

def safe_execute(func):
    """Decorator: Retries functions, auto-heals, and triggers Black Box."""
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
                logger.error(f"⚠️ Error in {func.__name__}: {e}")
                
                if time.time() - LAST_ERROR_TIME < 60 and ERROR_COUNTER > 5:
                    col_settings.update_one({"setting": "maintenance"}, {"$set": {"value": True}}, upsert=True)
                    await emergency_backup()
                    await send_alert(f"**PANIC PROTOCOL ACTIVE**\nMaintenance Engaged.\n`{traceback.format_exc()}`")
                    ERROR_COUNTER = 0 
                
                LAST_ERROR_TIME = time.time()
                await asyncio.sleep(1)
        return None 
    return wrapper

def is_admin(user_id):
    if user_id == OWNER_ID: return True
    try:
        admin = col_admins.find_one({"user_id": str(user_id)})
        return admin is not None
    except: return False

# ==========================================
# 👁️ SUPERVISOR ROUTINE (UNREDUCED WATCHDOG)
# ==========================================
@safe_execute
async def supervisor_routine():
    """Checks bots, DB, and Inventory every 5 minutes."""
    global LAST_REPORT_DATE, LAST_INVENTORY_CHECK
    print("👁️ Supervisor Watchdog Started (5 Min Scan)...")
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
                await send_alert(f"📉 **LOW VAULT INVENTORY**\nOnly {count} M-Codes remaining.")
            LAST_INVENTORY_CHECK = now_time

        current_date_str = now_ist.strftime("%Y-%m-%d")
        if now_ist.hour == 8 and now_ist.minute == 40 and LAST_REPORT_DATE != current_date_str:
            total_u = col_users.count_documents({})
            total_m = col_active.count_documents({})
            banned = col_banned.count_documents({})
            fmt_time = now_ist.strftime('%d-%m-%Y %I:%M %p')
            
            report = (
                f"🌅 **MSANODE DAILY EMPIRE AUDIT**\n"
                f"📅 `{fmt_time}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"✅ **Command Hub:** Online\n"
                f"👥 **Total Army:** `{total_u}`\n"
                f"🔑 **Vault Codes:** `{total_m}`\n"
                f"🚫 **Purged Users:** `{banned}`\n"
                f"━━━━━━━━━━━━━━━━━━"
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
            try:
                await worker_bot.get_me()
                ws = "Online"
            except: ws = "Offline"
            col_settings.update_one({"setting": "worker_status"}, {"$set": {"last_check": now, "status": ws}}, upsert=True)
        except: pass
        await asyncio.sleep(300)

@safe_execute
async def scheduled_pruning_cleanup():
    while True:
        await asyncio.sleep(43200) # 12 Hours
        try:
            col_users.delete_many({"status": "LEFT"})
        except: pass

def back_kb(): 
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Hub", callback_data="btn_refresh")
    return kb.as_markup()

# ==========================================
# 👑 THE APEX COMMANDS (UNREDUCED NEW FEATURES)
# ==========================================

@dp.message(Command("delete_user"))
async def cmd_delete_user(message: types.Message, command: CommandObject):
    """Surgically erases an operative from the database."""
    if not is_admin(message.from_user.id): return
    target_id = command.args
    if not target_id:
        await message.answer("❌ **Error:** ID required. Usage: `/delete_user <id>`")
        return
    
    res = col_users.delete_one({"user_id": target_id.strip()})
    if res.deleted_count > 0:
        await message.answer(f"🗑 **Target Purged.**\nUser `{target_id}` erased from the empire.")
    else:
        await message.answer("❌ Operative not found in records.")

@dp.message(Command("list"))
async def cmd_list_users(message: types.Message):
    """Generates a clean Operative Directory (Username | ID)."""
    if not is_admin(message.from_user.id): return
    users = list(col_users.find({}, {"username": 1, "user_id": 1, "_id": 0}))
    
    if not users:
        await message.answer("📂 **Vault Empty.** No recruits detected.")
        return

    text = "📋 **MSANODE OPERATIVE DIRECTORY**\n━━━━━━━━━━━━━━━━━━\n"
    count = 0
    for u in users:
        count += 1
        name = u.get("username") or "Anonymous"
        uid = u.get("user_id")
        text += f"{count}. {name} | `{uid}`\n"
        
        if len(text) > 3800:
            await message.answer(text)
            text = "━━━━━━━━━━━━━━━━━━\n"
            
    text += f"━━━━━━━━━━━━━━━━━━\n👥 **Total Count:** `{count}`"
    await message.answer(text)

@dp.message(Command("find"))
async def search_operative(message: types.Message, command: CommandObject):
    """High-speed search for a specific recruit dossier."""
    if not is_admin(message.from_user.id): return
    query = command.args
    if not query:
        await message.answer("❌ **Command Error**\nUsage: `/find @username` or `/find user_id`")
        return
    
    clean_q = query.replace("@", "").strip()
    user_doc = col_users.find_one({"$or": [{"user_id": clean_q}, {"username": {"$regex": f"^{clean_q}$", "$options": "i"}}]})
    
    if not user_doc:
        await message.answer(f"🔎 **No Operative Found** for: `{query}`")
        return

    report = (
        f"🕵️ **OPERATIVE DOSSIER FOUND**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 **Name:** {user_doc.get('first_name')}\n"
        f"🆔 **Username:** {user_doc.get('username')}\n"
        f"🔢 **User ID:** `{user_doc.get('user_id')}`\n"
        f"📅 **Joined:** {user_doc.get('joined_date')}\n"
        f"⚡ **Last Active:** {user_doc.get('last_active')}\n"
        f"📍 **Origin:** {user_doc.get('source')}\n"
        f"🛡️ **Status:** {user_doc.get('status')}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    await message.answer(report)

@dp.message(Command("stats"))
async def cmd_enhanced_stats(message: types.Message):
    """Supreme Audit of all MSANode Database assets."""
    if not is_admin(message.from_user.id): return
    
    u_count = col_users.count_documents({})
    m_count = col_active.count_documents({})
    yt_count = col_viral.count_documents({})
    ig_count = col_reels.count_documents({})
    b_count = col_banned.count_documents({})
    
    traffic = list(col_users.aggregate([{"$group": {"_id": "$source", "count": {"$sum": 1}}}]))
    t_map = {r['_id']: r['count'] for r in traffic}
    fmt_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
    
    text = (
        f"📊 **MSANODE SUPREME AUDIT**\n"
        f"📅 `{fmt_time}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 **Total Army:** `{u_count}`\n"
        f"🔑 **M-Codes:** `{m_count}`\n"
        f"🎥 **YT Files:** `{yt_count}`\n"
        f"📸 **IG Reels:** `{ig_count}`\n"
        f"🚫 **Total Banned:** `{b_count}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📈 **Source Traffic:**\n"
        f"🔴 YouTube: `{t_map.get('YouTube', 0)}`\n"
        f"📸 Instagram: `{t_map.get('Instagram', 0)}`\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    await message.answer(text)

# ==========================================
# 👑 THE HUB UI (DASHBOARD)
# ==========================================
@safe_execute
async def show_dashboard_ui(message_obj, user_id, is_edit=False):
    if not is_admin(user_id): return
    
    total_u = col_users.count_documents({})
    total_b = col_banned.count_documents({})
    m_doc = col_settings.find_one({"setting": "maintenance"})
    status = "🟠 LOCKDOWN" if m_doc and m_doc.get("value") == True else "🟢 NORMAL"

    text = (
        f"👑 **MSANODE COMMAND HUB**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 **Operatives:** `{total_u}`\n"
        f"🚫 **Purged:** `{total_b}`\n"
        f"🛠 **System:** {status}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📢 Broadcast", callback_data="btn_broadcast"), InlineKeyboardButton(text="🎯 Snipe DM", callback_data="btn_sniper"))
    kb.row(InlineKeyboardButton(text="📈 Audit Traffic", callback_data="btn_traffic"), InlineKeyboardButton(text="🩺 Diagnostics", callback_data="btn_diagnosis"))
    kb.row(InlineKeyboardButton(text="🚫 Ban Target", callback_data="btn_ban_menu"), InlineKeyboardButton(text="🛡️ Admin List", callback_data="btn_add_admin"))
    kb.row(InlineKeyboardButton(text="💾 Black Box Backup", callback_data="btn_backup"))
    kb.row(InlineKeyboardButton(text="🔄 Sync Hub", callback_data="btn_refresh"), InlineKeyboardButton(text="ℹ️ Protocols", callback_data="btn_help"))
    kb.row(InlineKeyboardButton(text="🛠 Lockdown Toggle", callback_data="btn_maint_toggle"), InlineKeyboardButton(text="💤 Sleep", callback_data="btn_sleep"))
    
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

@dp.callback_query(F.data == "btn_sleep")
async def cmd_sleep(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.message.delete()
    await callback.message.answer("💤 **Background Monitoring Active.**\nSend `/start` to re-engage terminal.")

# ==========================================
# 📢 BROADCAST & TARGETING RADAR (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_broadcast")
async def broadcast_menu(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    last = col_broadcast_logs.find_one(sort=[("_id", -1)])
    kb = InlineKeyboardBuilder()
    kb.button(text="🎯 New Targeted Transmission", callback_data="start_broadcast_new")
    if last:
        kb.button(text="✏️ Edit Last", callback_data="edit_last_broadcast")
        kb.button(text="🔥 Purge Last", callback_data="unsend_last_broadcast")
    kb.button(text="🔙 Back", callback_data="btn_refresh")
    kb.adjust(1)
    await callback.message.edit_text(f"📢 **Transmission Manager**", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "start_broadcast_new")
async def start_broadcast(callback: types.CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="👥 ALL Operatives", callback_data="target_all"))
    kb.row(InlineKeyboardButton(text="🔴 YouTube Recruits", callback_data="target_yt"))
    kb.row(InlineKeyboardButton(text="📸 Instagram Recruits", callback_data="target_ig"))
    kb.row(InlineKeyboardButton(text="❌ CANCEL", callback_data="btn_broadcast"))
    await callback.message.edit_text("🎯 **Step 1: Select Target Group**", reply_markup=kb.as_markup())
    await state.set_state(BroadcastState.waiting_for_filter)

@dp.callback_query(BroadcastState.waiting_for_filter, F.data.startswith("target_"))
async def select_filter(callback: types.CallbackQuery, state: FSMContext):
    target = callback.data.split("_")[1]
    label = "ALL" if target == "all" else ("YouTube Only" if target == "yt" else "Instagram Only")
    await state.update_data(target_filter=target)
    await callback.message.edit_text(f"📝 **Step 2: Enter Content for {label}**")
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
            await message.answer("📥 **Downloading Data Packets...**")
            path = f"t_{message.from_user.id}.{ext}"
            await manager_bot.download(file_obj, destination=path)
            
    await state.update_data(ctype=ctype, text=text, path=path)
    kb = InlineKeyboardBuilder().button(text="🚀 TRANSMIT", callback_data="confirm_send").button(text="❌ ABORT", callback_data="cancel_send").as_markup()
    await message.answer(f"📢 **Transmit {ctype}?**", reply_markup=kb)
    await state.set_state(BroadcastState.confirm_send)

@dp.callback_query(F.data == "confirm_send")
async def execute_broadcast(callback: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    t_filter = d.get('target_filter', 'all')
    query = {"status": "Active"}
    if t_filter == "yt": query["source"] = "YouTube"
    elif t_filter == "ig": query["source"] = "Instagram"
    
    total_targets = col_users.count_documents(query)
    radar_msg = await callback.message.edit_text(f"🚀 **TRANSMISSION BEGUN**\n━━━━━━━━━━━━━━━━━━\n📡 Radar: `0 / {total_targets}` recruits\n━━━━━━━━━━━━━━━━━━")
    
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
                    try: await radar_msg.edit_text(f"🚀 **LIVE TRANSMISSION RADAR**\n━━━━━━━━━━━━━━━━━━\n📡 Radar: `{sent} / {total_targets}` recruits\n🛡️ Blocked: `{blocked}`\n━━━━━━━━━━━━━━━━━━")
                    except: pass
                await asyncio.sleep(0.05) 
            except TelegramForbiddenError: 
                blocked += 1
                col_users.update_one({"user_id": uid}, {"$set": {"status": "BLOCKED"}})
            except: pass
        
        if msg_ids: 
            log_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
            col_broadcast_logs.insert_one({"date": log_time, "messages": msg_ids, "type": d['ctype'], "original_text": d['text']})
        await callback.message.answer(f"✅ Success: {sent} | Failure: {blocked}")
    
    except Exception as e: await callback.message.answer(f"❌ Error: {e}")
    if path and os.path.exists(path): os.remove(path)
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "unsend_last_broadcast")
async def unsend_last(callback: types.CallbackQuery):
    await callback.message.edit_text("⏳ **Purging Last Transmission...**")
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    if not last_log: 
        await callback.message.edit_text("❌ No history.")
        return
    deleted = 0
    for entry in last_log.get("messages", []):
        try:
            await worker_bot.delete_message(chat_id=entry['chat_id'], message_id=entry['message_id'])
            deleted += 1
            await asyncio.sleep(0.03)
        except: pass
    col_broadcast_logs.delete_one({"_id": last_log["_id"]})
    await callback.message.answer(f"✅ Recalled {deleted} messages.")
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=False)

@dp.callback_query(F.data == "edit_last_broadcast")
async def edit_last_start(callback: types.CallbackQuery, state: FSMContext):
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    if not last_log or last_log.get("type") != "text": 
        await callback.answer("❌ Text only to edit.", show_alert=True)
        return
    await callback.message.edit_text(f"📝 **Current:**\n{last_log.get('original_text')}\n\n👇 **NEW Intelligence:**")
    await state.set_state(BroadcastState.waiting_for_edit)

@dp.message(BroadcastState.waiting_for_edit)
async def edit_last_execute(message: types.Message, state: FSMContext):
    new_text = message.text
    await message.answer("⏳ **Editing Packets...**")
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    edited = 0
    for entry in last_log.get("messages", []):
        try:
            await worker_bot.edit_message_text(text=new_text, chat_id=entry['chat_id'], message_id=entry['message_id'])
            edited += 1
            await asyncio.sleep(0.03)
        except: pass
    col_broadcast_logs.update_one({"_id": last_log["_id"]}, {"$set": {"original_text": new_text}})
    await message.answer(f"✅ Edited {edited} messages.")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id, is_edit=False)

# ==========================================
# 🚫 BAN & ADMIN (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_ban_menu")
async def ban_menu(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("🚫 **Enter ID to Purge:**")
    await state.set_state(BanState.waiting_for_id)

@dp.message(BanState.waiting_for_id)
async def execute_ban(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    target_id = message.text.strip()
    if not target_id.isdigit(): 
        await message.answer("❌ Invalid.")
        return
    col_banned.update_one({"user_id": target_id}, {"$set": {"banned_at": datetime.now(IST), "banned_by": message.from_user.first_name}}, upsert=True)
    col_users.update_one({"user_id": target_id}, {"$set": {"status": "BLOCKED"}})
    await message.answer(f"⛔ **Purged {target_id}.**")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id)

@dp.callback_query(F.data == "btn_add_admin")
async def add_admin_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await callback.message.edit_text("👮 **Enter New Admin ID:**")
    await state.set_state(AdminState.waiting_for_id)

@dp.message(AdminState.waiting_for_id)
async def add_admin_id(message: types.Message, state: FSMContext):
    await state.update_data(new_id=message.text)
    await message.answer("👤 **Name:**")
    await state.set_state(AdminState.waiting_for_name)

@dp.message(AdminState.waiting_for_name)
async def add_admin_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    col_admins.insert_one({"user_id": data['new_id'], "name": message.text, "role": "Admin"})
    await message.answer("✅ Clearance Granted.")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id)

# ==========================================
# 🩺 DIAGNOSTICS & SYSTEM (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_diagnosis")
async def run_diagnosis(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await callback.message.edit_text("🩺 **Scanning MSANode Fabric...**")
    try:
        ts = time.time()
        u = col_users.count_documents({})
        ca = col_active.count_documents({})
        lat = round((time.time()-ts)*1000, 2)
        report = (
            f"🩺 **SYSTEM DIAGNOSTICS**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📦 **Data Core:** Healthy ({lat}ms)\n"
            f"👥 **Users:** `{u}`\n"
            f"🔑 **Vaults:** `{ca}`\n"
            f"🤖 **Status:** Shield Active"
        )
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
    await callback.message.edit_text("⏳ **Securing Black Box Backup...**")
    try:
        df = list(col_users.find({}, {"_id": 0}))
        if df:
            with open("Vault_Backup.csv", 'w', newline='', encoding='utf-8') as f: 
                csv.DictWriter(f, df[0].keys()).writeheader()
                csv.DictWriter(f, df[0].keys()).writerows(df)
            await callback.message.answer_document(FSInputFile("Vault_Backup.csv"), caption="💾 **MSANODE ENCRYPTED BACKUP**")
            os.remove("Vault_Backup.csv")
    except: await callback.message.answer("❌ Protocol Failure")
    await show_dashboard_ui(callback.message, callback.from_user.id)

# ==========================================
# 🎯 SNIPER & TRAFFIC (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_traffic")
async def traffic_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    res = list(col_users.aggregate([{"$group": {"_id": "$source", "count": {"$sum": 1}}}]))
    t = {r['_id']: r['count'] for r in res}
    report = (
        f"📈 **INTELLIGENCE TRAFFIC**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔴 YT Entry: `{t.get('YouTube', 0)}`\n"
        f"📸 IG Entry: `{t.get('Instagram', 0)}`\n"
        f"📊 Overall: `{sum(t.values())}`"
    )
    await callback.message.edit_text(report, reply_markup=back_kb())

@dp.callback_query(F.data == "btn_sniper")
async def start_sniper(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await callback.message.edit_text("🎯 **Target User ID:**")
    await state.set_state(SniperState.waiting_for_target_id)

@dp.message(SniperState.waiting_for_target_id)
async def sniper_id(message: types.Message, state: FSMContext):
    await state.update_data(target_id=message.text)
    await message.answer("📝 **Intelligence to DM:**")
    await state.set_state(SniperState.waiting_for_message)

@dp.message(SniperState.waiting_for_message)
async def sniper_msg(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    kb = InlineKeyboardBuilder().button(text="🚀 EXECUTE", callback_data="confirm_sniper").as_markup()
    await message.answer("Confirm Snipe?", reply_markup=kb)
    await state.set_state(SniperState.confirm_send)

@dp.callback_query(F.data == "confirm_sniper")
async def execute_sniper(callback: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try: 
        await worker_bot.send_message(chat_id=d['target_id'], text=d['text'])
        await callback.message.answer("✅ Delivered.")
    except Exception as e: await callback.message.answer(f"❌ Failed: {e}")
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "cancel_send")
async def cancel_op(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=True)

@dp.callback_query(F.data == "btn_help")
async def help_guide(callback: types.CallbackQuery):
    h = (
        "📘 **APEX MANAGER PROTOCOL**\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "**⚡ SUPREME COMMANDS**\n"
        "• `/stats` - Empire audit.\n"
        "• `/list` - Recruits directory.\n"
        "• `/find <user>` - Dossier search.\n"
        "• `/delete_user <id>` - Purge target.\n\n"
        "**🛡️ AUTOMATION**\n"
        "• **Radar:** Live broadcast tracking.\n"
        "• **Targeting:** Filter All/YT/IG users.\n"
        "• **Iron Dome:** 5-min self-healing."
    )
    await callback.message.edit_text(h, reply_markup=back_kb(), parse_mode="Markdown")

# ==========================================
# 🚀 NUCLEAR MAIN EXECUTION (GHOST SHIELD)
# ==========================================
async def main():
    print("👑 MSANode Manager Bot is Online...")
    try: await manager_bot.send_message(OWNER_ID, "🟢 **Command Center Active**\nGhost Shield and Apex Protocols Initialized.")
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
            print("💀 GHOST DETECTED! Purging competing connection...")
            time.sleep(20)
        except (KeyboardInterrupt, SystemExit):
            print("🛑 Hub Stopped Safely")
            break
        except Exception as e:
            print(f"💥 SYSTEM BREACH: {e}")
            traceback.print_exc()
            time.sleep(15)
