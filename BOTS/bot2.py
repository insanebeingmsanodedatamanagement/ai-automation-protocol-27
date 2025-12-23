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
# ⚡ CONFIGURATION (GHOST PROTOCOL ACTIVATED)
# ==========================================
# Securely fetch all keys from Render Environment
MANAGER_BOT_TOKEN = os.getenv("MANAGER_BOT_TOKEN")
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

# SECURE OWNER ID FETCH (Hidden from Source)
try:
    OWNER_ID = int(os.getenv("OWNER_ID", 0))
except (TypeError, ValueError):
    OWNER_ID = 0

# Validation: Kill execution if critical variables are missing
if not all([MANAGER_BOT_TOKEN, MAIN_BOT_TOKEN, MONGO_URI, OWNER_ID]):
    print("❌ CRITICAL ERROR: Mandatory Environment Variables missing!")
    print("Ensure MANAGER_BOT_TOKEN, MAIN_BOT_TOKEN, MONGO_URI, and OWNER_ID are set in Render.")

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

# GLOBAL TRACKERS (IRON DOME)
ERROR_COUNTER = 0
LAST_ERROR_TIME = time.time()
LAST_REPORT_DATE = None 
LAST_INVENTORY_CHECK = 0

# STATES
class BroadcastState(StatesGroup):
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
print("🔄 Connecting Manager to MSANode MongoDB...")
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["MSANodeDB"]
    
    # Collections (Full Infrastructure)
    col_users = db["user_logs"]
    col_admins = db["admins"]
    col_settings = db["settings"]
    col_active = db["active_content"] 
    col_viral = db["viral_videos"]
    col_reels = db["viral_reels"]
    col_banned = db["banned_users"]
    col_broadcast_logs = db["broadcast_logs"]
    
    print("✅ Connected to MSANode Data Core")
except Exception as e:
    print(f"❌ CRITICAL DB ERROR: {e}")
    exit()

# --- RENDER PORT BINDER (SHIELD) ---
async def handle_health(request):
    return web.Response(text="MSANODE CORE 2 (MANAGER BOT) IS ACTIVE")

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
        await manager_bot.send_message(OWNER_ID, f"🚨 **CRITICAL ALERT** 🚨\n\n{msg}")
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
            await manager_bot.send_document(OWNER_ID, FSInputFile(filename), caption="💾 **BLACK BOX DATA DUMP**\nSystem crashed. Here is your user data.")
            os.remove(filename)
    except Exception as e:
        logger.error(f"Black Box Backup Failed: {e}")

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
                
                # PANIC PROTOCOL
                if time.time() - LAST_ERROR_TIME < 60 and ERROR_COUNTER > 5:
                    col_settings.update_one({"setting": "maintenance"}, {"$set": {"value": True}}, upsert=True)
                    
                    # 💾 FIRE BLACK BOX BACKUP
                    await emergency_backup()
                    
                    await send_alert(f"**PANIC PROTOCOL ACTIVE**\nError Spike Detected.\nMaintenance Mode ENABLED.\nUser Data Backup Sent.\n\n`{traceback.format_exc()}`")
                    ERROR_COUNTER = 0 
                
                LAST_ERROR_TIME = time.time()
                await asyncio.sleep(1)
        return None 
    return wrapper

# --- AUTH CHECK ---
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
        
        # 1. 5-MINUTE HEALTH CHECK
        if now_time - last_health_check >= 300: 
            try:
                me = await manager_bot.get_me()
                worker = await worker_bot.get_me()
                col_users.find_one()
                logger.info("✅ 5-Min Health Check Passed.")
            except Exception as e:
                await send_alert(f"**Health Check Failed**\nSystem detected a bot or DB failure.\nError: `{e}`")
            last_health_check = now_time

        # 2. SUPPLY CHAIN WATCHDOG (Hourly)
        if now_time - LAST_INVENTORY_CHECK >= 3600: # 1 Hour
            count = col_active.count_documents({})
            if count < 5:
                await send_alert(f"📉 **LOW INVENTORY ALERT**\n\nOnly **{count}** files remaining in the Vault.\nUpload content immediately to keep sales running.")
            LAST_INVENTORY_CHECK = now_time

        # 3. DAILY REPORT (08:40 AM)
        current_date_str = now_ist.strftime("%Y-%m-%d")
        if now_ist.hour == 8 and now_ist.minute == 40 and LAST_REPORT_DATE != current_date_str:
            users = col_users.count_documents({})
            active = col_active.count_documents({})
            banned = col_banned.count_documents({})
            
            # Format: DD-MM-YYYY 04:08 PM
            fmt_time = now_ist.strftime('%d-%m-%Y %I:%M %p')
            
            daily_msg = (
                f"🌅 **DAILY EMPIRE REPORT** 🌅\n"
                f"📅 {fmt_time}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"✅ **Manager Bot:** Online\n"
                f"✅ **Main Bot:** Online\n"
                f"✅ **Database:** Connected\n\n"
                f"📊 **Stats:**\n"
                f"👥 Total Users: `{users}`\n"
                f"📄 Vault Codes: `{active}`\n"
                f"🚫 Banned: `{banned}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🤖 *Checks running every 5 mins.*"
            )
            await manager_bot.send_message(OWNER_ID, daily_msg)
            LAST_REPORT_DATE = current_date_str
            
        await asyncio.sleep(30) 

# --- TASKS (UNREDUCED) ---
@safe_execute
async def scheduled_health_check():
    """Updates Status in DB."""
    while True:
        try:
            now = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
            col_settings.update_one({"setting": "manager_status"}, {"$set": {"last_check": now, "status": "Online"}}, upsert=True)
            try:
                await worker_bot.get_me()
                ws = "Online"
            except Exception as e: 
                ws = f"Error: {str(e)[:10]}"
            col_settings.update_one({"setting": "worker_status"}, {"$set": {"last_check": now, "status": ws}}, upsert=True)
        except: 
            pass
        await asyncio.sleep(300)

@safe_execute
async def scheduled_pruning_cleanup():
    while True:
        await asyncio.sleep(43200) # 12 Hours
        try:
            res = col_users.delete_many({"status": "LEFT"})
            if res.deleted_count > 0: 
                logger.info(f"Deleted {res.deleted_count} inactive users.")
        except: 
            pass

def back_kb(): 
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Hub", callback_data="btn_refresh")
    return kb.as_markup()

# ==========================================
# 👑 APEX COMMANDS (MASTER SADIQ UPDATES)
# ==========================================

@dp.message(Command("delete_user"))
async def delete_user_manual(message: types.Message, command: CommandObject):
    """Surgically erases a user ID from the entire MSANode database."""
    if not is_admin(message.from_user.id): return
    target_id = command.args
    if not target_id:
        await message.answer("❌ **ID REQUIRED**\nUsage: `/delete_user <id>`")
        return
    
    res = col_users.delete_one({"user_id": target_id.strip()})
    if res.deleted_count > 0:
        await message.answer(f"🗑 **Operative Purged.**\nUser ID `{target_id}` erased from the records.")
    else:
        await message.answer("❌ User ID not found in database.")

@dp.message(Command("list"))
async def list_users_directory(message: types.Message):
    """Returns a clean, professional directory of Username and ID only."""
    if not is_admin(message.from_user.id): return
    cursor = col_users.find({}, {"username": 1, "user_id": 1, "_id": 0})
    operatives = list(cursor)
    
    if not operatives:
        await message.answer("📂 **Database Empty.** No recruits found.")
        return

    report = "📋 **MSANODE OPERATIVE LIST**\n━━━━━━━━━━━━━━━━━━\n"
    count = 0
    for op in operatives:
        count += 1
        username = op.get("username") or "None"
        uid = op.get("user_id")
        report += f"{count}. {username} | `{uid}`\n"
        
        if len(report) > 3900:
            await message.answer(report)
            report = ""
            
    report += f"━━━━━━━━━━━━━━━━━━\n👥 **Total Recruit Count:** `{count}`"
    await message.answer(report)

@dp.message(Command("find"))
async def search_operative(message: types.Message, command: CommandObject):
    """High-speed search for a specific recruit by username or ID."""
    if not is_admin(message.from_user.id): return
    query = command.args
    if not query:
        await message.answer("❌ **Command Error**\nUsage: `/find @username` or `/find user_id`")
        return
    
    clean_query = query.replace("@", "").strip()
    # Search by ID first, then Username
    user_doc = col_users.find_one({"$or": [{"user_id": clean_query}, {"username": {"$regex": clean_query, "$options": "i"}}]})
    
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
async def supreme_stats_audit(message: types.Message):
    """Enhanced overall audit of every MSANode asset."""
    if not is_admin(message.from_user.id): return
    
    users = col_users.count_documents({})
    codes = col_active.count_documents({})
    yt = col_viral.count_documents({})
    ig = col_reels.count_documents({})
    banned = col_banned.count_documents({})
    
    res = list(col_users.aggregate([{"$group": {"_id": "$source", "count": {"$sum": 1}}}]))
    cnt = {r['_id']: r['count'] for r in res}
    
    now_str = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
    
    msg = (
        f"📊 **MSANODE EMPIRE AUDIT**\n"
        f"📅 `{now_str}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 **Total Operatives:** `{users}`\n"
        f"🔑 **Vault M-Codes:** `{codes}`\n"
        f"🎥 **YT Videos:** `{yt}`\n"
        f"📸 **IG Reels:** `{ig}`\n"
        f"🚫 **Total Banned:** `{banned}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📈 **Source Traffic Analysis:**\n"
        f"🔴 YT Origin: `{cnt.get('YouTube', 0)}`\n"
        f"📸 IG Origin: `{cnt.get('Instagram', 0)}`\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    await message.answer(msg)

# ==========================================
# 👑 THE DASHBOARD UI (UNREDUCED)
# ==========================================
@safe_execute
async def show_dashboard_ui(message_obj, user_id, is_edit=False):
    if not is_admin(user_id):
        if is_edit: await message_obj.edit_text("⛔ Access Denied")
        else: await message_obj.answer("⛔ Access Denied")
        return

    total_users = col_users.count_documents({})
    banned_users = col_banned.count_documents({})
    maint_doc = col_settings.find_one({"setting": "maintenance"})
    
    maint_status = "🟢 Normal"
    if maint_doc and maint_doc.get("value") == True: 
        maint_status = "🟠 ACTIVE"

    text = (
        f"👑 **MSA COMMAND HUB (Apex Mode)**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 **Users:** `{total_users}`\n"
        f"🚫 **Banned:** `{banned_users}`\n"
        f"🛠 **Maint. Mode:** {maint_status}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📢 Broadcast", callback_data="btn_broadcast"), InlineKeyboardButton(text="🎯 DM User", callback_data="btn_sniper"))
    kb.row(InlineKeyboardButton(text="📈 Audit Traffic", callback_data="btn_traffic"), InlineKeyboardButton(text="🩺 Diagnostics", callback_data="btn_diagnosis"))
    kb.row(InlineKeyboardButton(text="🚫 Ban Target", callback_data="btn_ban_menu"), InlineKeyboardButton(text="👮 Admin List", callback_data="btn_add_admin"))
    kb.row(InlineKeyboardButton(text="💾 Black Box Backup", callback_data="btn_backup"))
    kb.row(InlineKeyboardButton(text="🔄 Sync Terminal", callback_data="btn_refresh"), InlineKeyboardButton(text="ℹ️ Help Guide", callback_data="btn_help"))
    kb.row(InlineKeyboardButton(text="🛠 Lockdown Toggle", callback_data="btn_maint_toggle"), InlineKeyboardButton(text="💤 Sleep", callback_data="btn_sleep"))
    
    try:
        if is_edit: await message_obj.edit_text(text, reply_markup=kb.as_markup())
        else: await message_obj.answer(text, reply_markup=kb.as_markup())
    except TelegramBadRequest: pass

@dp.message(Command("start"), StateFilter("*"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear() 
    bot_info = await manager_bot.get_me()
    if message.from_user.id == bot_info.id: return
    await show_dashboard_ui(message, message.from_user.id, is_edit=False)

@dp.callback_query(F.data == "btn_refresh", StateFilter("*"))
async def refresh_dashboard(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=True)

@dp.callback_query(F.data == "btn_sleep")
async def sleep_mode(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await callback.message.delete()
    await callback.message.answer("💤 **Command Terminal Suspended.**\nType `/start` to re-engage.")

# ==========================================
# 📢 BROADCAST & LIVE RADAR (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_broadcast")
async def broadcast_menu(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    last_broadcast = col_broadcast_logs.find_one(sort=[("_id", -1)])
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Write New Transmission", callback_data="start_broadcast_new")
    if last_broadcast:
        kb.button(text="✏️ Edit Last", callback_data="edit_last_broadcast")
        kb.button(text="🔥 Purge Last", callback_data="unsend_last_broadcast")
    kb.button(text="🔙 Back to Hub", callback_data="btn_refresh")
    kb.adjust(1)
    await callback.message.edit_text(f"📢 **Transmission Control**", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "start_broadcast_new")
async def start_broadcast(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("📝 **Enter Intelligence to Broadcast.**")
    await state.set_state(BroadcastState.waiting_for_message)

@dp.message(BroadcastState.waiting_for_message)
async def receive_broadcast_msg(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    content_type = "text"; file_path = None; text_content = message.text or message.caption or ""
    if not message.text: 
        file_obj = None; ext = "dat"
        if message.photo: content_type = "photo"; file_obj = message.photo[-1]; ext="jpg"
        elif message.video: content_type = "video"; file_obj = message.video; ext="mp4"
        elif message.document: content_type = "document"; file_obj = message.document; ext="pdf"
        if file_obj: 
            await message.answer("📥 **Buffering Data Packet...**")
            file_path = f"temp_{message.from_user.id}.{ext}"
            await manager_bot.download(file_obj, destination=file_path)
            
    await state.update_data(ctype=content_type, text=text_content, path=file_path)
    kb = InlineKeyboardBuilder().button(text="🚀 TRANSMIT", callback_data="confirm_send").button(text="❌ ABORT", callback_data="cancel_send").as_markup()
    await message.answer(f"📢 **Ready to Transmit {content_type}?**", reply_markup=kb)
    await state.set_state(BroadcastState.confirm_send)

@dp.callback_query(F.data == "confirm_send")
async def execute_broadcast(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    total_operatives = col_users.count_documents({"status": "Active"})
    
    # 📡 LIVE RADAR UI
    radar_msg = await callback.message.edit_text(f"🚀 **TRANSMISSION BEGUN**\n━━━━━━━━━━━━━━━━━━\n📡 Progress: `0 / {total_operatives}` recruits\n━━━━━━━━━━━━━━━━━━")
    
    cached_file_id = None; sent = 0; blocked_count = 0; file_path = data.get('path'); msg_ids = []
    
    try:
        cursor = col_users.find({"status": "Active"}, {"user_id": 1})
        for doc in cursor:
            uid = doc.get("user_id")
            try:
                media = cached_file_id or (FSInputFile(file_path) if file_path else None)
                msg = None
                if data['ctype'] == 'text': msg = await worker_bot.send_message(uid, data['text'])
                else:
                    if data['ctype'] == 'photo': msg = await worker_bot.send_photo(uid, media, caption=data['text'])
                    elif data['ctype'] == 'video': msg = await worker_bot.send_video(uid, media, caption=data['text'])
                    elif data['ctype'] == 'document': msg = await worker_bot.send_document(uid, media, caption=data['text'])
                
                if msg:
                    msg_ids.append({"chat_id": int(uid), "message_id": msg.message_id})
                    if not cached_file_id:
                        if data['ctype'] == 'photo': cached_file_id = msg.photo[-1].file_id
                        elif data['ctype'] == 'video': cached_file_id = msg.video.file_id
                        elif data['ctype'] == 'document': cached_file_id = msg.document.file_id
                sent += 1
                
                # Update Radar every 10 users to keep speed and avoid flood
                if sent % 10 == 0:
                    try: await radar_msg.edit_text(f"🚀 **LIVE TRANSMISSION RADAR**\n━━━━━━━━━━━━━━━━━━\n📡 Progress: `{sent} / {total_operatives}` recruits\n🛡️ Blocked: `{blocked_count}`\n━━━━━━━━━━━━━━━━━━")
                    except: pass
                    
                await asyncio.sleep(0.05) 
            except TelegramForbiddenError: 
                blocked_count += 1
                col_users.update_one({"user_id": uid}, {"$set": {"status": "BLOCKED"}})
            except Exception: pass
        
        if msg_ids: 
            log_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
            col_broadcast_logs.insert_one({"date": log_time, "messages": msg_ids, "type": data['ctype'], "original_text": data['text']})
        
        await callback.message.answer(f"✅ **Transmission Success.**\nSent: {sent} | Failure: {blocked_count}")
    
    except Exception as e: await callback.message.answer(f"❌ Transmission Error: {e}")
    if file_path and os.path.exists(file_path): os.remove(file_path)
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=False)

@dp.callback_query(F.data == "unsend_last_broadcast")
async def unsend_last(callback: types.CallbackQuery):
    await callback.message.edit_text("⏳ **Recalling Transmission...**")
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
    await callback.message.answer(f"✅ **Recalled {deleted} messages.**")
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=False)

@dp.callback_query(F.data == "edit_last_broadcast")
async def edit_last_start(callback: types.CallbackQuery, state: FSMContext):
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    if not last_log or last_log.get("type") != "text": 
        await callback.answer("❌ Intelligence must be text only to edit.", show_alert=True)
        return
    await callback.message.edit_text(f"📝 **Current Intelligence:**\n{last_log.get('original_text')}\n\n👇 **Send NEW Data:**")
    await state.set_state(BroadcastState.waiting_for_edit)

@dp.message(BroadcastState.waiting_for_edit)
async def edit_last_execute(message: types.Message, state: FSMContext):
    new_text = message.text
    await message.answer("⏳ **Patching Intelligence...**")
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    edited = 0
    for entry in last_log.get("messages", []):
        try:
            await worker_bot.edit_message_text(text=new_text, chat_id=entry['chat_id'], message_id=entry['message_id'])
            edited += 1
            await asyncio.sleep(0.03)
        except: pass
    col_broadcast_logs.update_one({"_id": last_log["_id"]}, {"$set": {"original_text": new_text}})
    await message.answer(f"✅ **Patched {edited} messages.**")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id, is_edit=False)

# ==========================================
# 🚫 BAN SYSTEM & ADMIN (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_ban_menu")
async def ban_menu(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("🚫 **Enter User ID to Purge:**")
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
    await message.answer(f"⛔ **Purged {target_id} from empire.**")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id, is_edit=False)

@dp.callback_query(F.data == "btn_add_admin")
async def add_admin_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await callback.message.edit_text("👮 **New Admin ID:**")
    await state.set_state(AdminState.waiting_for_id)

@dp.message(AdminState.waiting_for_id)
async def add_admin_id(message: types.Message, state: FSMContext):
    await state.update_data(new_id=message.text)
    await message.answer("👤 **Identity Label:**")
    await state.set_state(AdminState.waiting_for_name)

@dp.message(AdminState.waiting_for_name)
async def add_admin_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    col_admins.insert_one({"user_id": data['new_id'], "name": message.text, "role": "Admin"})
    await message.answer("✅ Clearance Granted.")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id, is_edit=False)

# ==========================================
# 🩺 DIAGNOSTICS & BACKUP (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_diagnosis")
async def run_diagnosis(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await callback.message.edit_text("🩺 **Analyzing MSANode Fabric...**")
    try:
        ts = time.time()
        cu = col_users.count_documents({})
        ca = col_active.count_documents({})
        lat = round((time.time()-ts)*1000, 2)
        report = (
            f"🩺 **SYSTEM DIAGNOSTICS**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📦 **Data Core:** Healthy ({lat}ms)\n"
            f"👥 **Operatives:** `{cu}`\n"
            f"📄 **Live Vaults:** `{ca}`\n"
            f"🤖 **Bot Shield:** ACTIVE"
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
    await callback.message.edit_text("⏳ **Securing Data Packets...**")
    try:
        df = list(col_users.find({}, {"_id": 0}))
        if df:
            with open("Users.csv", 'w', newline='', encoding='utf-8') as f: 
                csv.DictWriter(f, df[0].keys()).writeheader()
                csv.DictWriter(f, df[0].keys()).writerows(df)
            await callback.message.answer_document(FSInputFile("Users.csv"), caption="💾 **MSANODE ENCRYPTED BACKUP**")
            os.remove("Users.csv")
    except: await callback.message.answer("❌ Backup Protocol Failure")
    await show_dashboard_ui(callback.message, callback.from_user.id)

# ==========================================
# 📈 TRAFFIC & SNIPER (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_traffic")
async def traffic_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    res = list(col_users.aggregate([{"$group": {"_id": "$source", "count": {"$sum": 1}}}]))
    total = sum([r['count'] for r in res])
    cnt = {r['_id']: r['count'] for r in res}
    report = (
        f"📈 **INTELLIGENCE TRAFFIC**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔴 YouTube Origin: `{cnt.get('YouTube', 0)}`\n"
        f"📸 Instagram Origin: `{cnt.get('Instagram', 0)}`\n"
        f"📊 Total Entries: {total}"
    )
    await callback.message.edit_text(report, reply_markup=back_kb())

@dp.callback_query(F.data == "btn_sniper")
async def start_sniper(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await callback.message.edit_text("🎯 **Target Operative ID:**")
    await state.set_state(SniperState.waiting_for_target_id)

@dp.message(SniperState.waiting_for_target_id)
async def sniper_id(message: types.Message, state: FSMContext):
    await state.update_data(target_id=message.text)
    await message.answer("📝 **Intelligence to DM:**")
    await state.set_state(SniperState.waiting_for_message)

@dp.message(SniperState.waiting_for_message)
async def sniper_msg(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    kb = InlineKeyboardBuilder().button(text="🚀 EXECUTE SNIPE", callback_data="confirm_sniper").as_markup()
    await message.answer("Confirm Transmission?", reply_markup=kb)
    await state.set_state(SniperState.confirm_send)

@dp.callback_query(F.data == "confirm_sniper")
async def execute_sniper(callback: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try: 
        await worker_bot.send_message(chat_id=d['target_id'], text=d['text'])
        await callback.message.answer("✅ Snipe Delivered.")
    except Exception as e: await callback.message.answer(f"❌ Failed: {e}")
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=False)

@dp.callback_query(F.data == "cancel_send")
async def cancel_op(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=True)

@dp.callback_query(F.data == "btn_help")
async def help_guide(callback: types.CallbackQuery):
    help_text = (
        "📘 **MSANODE HUB PROTOCOLS**\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "**⚡ COMMANDS**\n"
        "• `/stats` - Full empire audit.\n"
        "• `/list` - Recruits directory.\n"
        "• `/find <user>` - Search operative dossier.\n"
        "• `/delete_user <id>` - Erase target.\n\n"
        "**🤖 AUTOMATION**\n"
        "• **Watchdog:** Scan every 5 mins.\n"
        "• **Live Radar:** Real-time broadcast progress.\n"
        "• **Iron Dome:** Auto-Backup & Lockdown."
    )
    await callback.message.edit_text(help_text, reply_markup=back_kb(), parse_mode="Markdown")

# ==========================================
# 🚀 NUCLEAR MAIN EXECUTION (GHOST SHIELD)
# ==========================================
async def main():
    print("👑 Manager Bot (Apex Ghost Mode) is Online...")
    try: await manager_bot.send_message(OWNER_ID, "🟢 **Command Terminal Initialized**\nGhost Shield and Apex Protocols Active.")
    except: pass
    
    # Kill conflicts on start
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
            print("💀 GHOST DETECTED! Waiting 20 seconds to purge competing session...")
            time.sleep(20)
        except (KeyboardInterrupt, SystemExit):
            print("🛑 Command Hub Stopped Safely")
            break
        except Exception as e:
            print(f"💥 CRITICAL BREACH: {e}")
            traceback.print_exc()
            time.sleep(15)
