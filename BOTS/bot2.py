import asyncio
import logging
import os
import csv
import time
import functools
import traceback
from datetime import datetime, timedelta
import pymongo
import pytz 
from collections import Counter
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest

# ==========================================
# ⚡ CONFIGURATION
# ==========================================
# ⚠️ REPLACE WITH YOUR REAL KEYS
# Securely fetch from Render Environment
MANAGER_BOT_TOKEN = os.getenv("MANAGER_BOT_TOKEN")
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

# Safety Check: If Render isn't set up right, the bot will tell you why
if not all([MANAGER_BOT_TOKEN, MAIN_BOT_TOKEN, MONGO_URI]):
    print("❌ ERROR: One or more environment variables are missing in Render!")
OWNER_ID = 6988593629 

# Timezone for Reports
IST = pytz.timezone('Asia/Kolkata')

# ==========================================
# 🛠 SYSTEM SETUP
# ==========================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

manager_bot = Bot(token=MANAGER_BOT_TOKEN)
worker_bot = Bot(token=MAIN_BOT_TOKEN)
dp = Dispatcher()

# GLOBAL TRACKERS
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
print("🔄 Connecting Manager to MongoDB...")
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["MSANodeDB"]
    
    # Collections
    col_users = db["user_logs"]
    col_admins = db["admins"]
    col_settings = db["settings"]
    col_active = db["active_content"] 
    col_viral = db["viral_videos"]
    col_reels = db["viral_reels"]
    col_banned = db["banned_users"]
    col_broadcast_logs = db["broadcast_logs"]
    
    print("✅ Connected to MongoDB Atlas")
except Exception as e:
    print(f"❌ CRITICAL DB ERROR: {e}")
    exit()

# ==========================================
# 🛡️ IRON DOME & HELPERS
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
# 👁️ SUPERVISOR ROUTINE
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
            
            daily_msg = (
                f"🌅 **DAILY EMPIRE REPORT** 🌅\n"
                f"📅 {now_ist.strftime('%d-%m-%Y %I:%M %p')}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"✅ **Manager Bot:** Online\n"
                f"✅ **Main Bot:** Online\n"
                f"✅ **Database:** Connected\n\n"
                f"📊 **Stats:**\n"
                f"👥 Total Users: `{users}`\n"
                f"📄 Files: `{active}`\n"
                f"🚫 Banned: `{banned}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🤖 *Checks running every 5 mins.*"
            )
            await manager_bot.send_message(OWNER_ID, daily_msg)
            LAST_REPORT_DATE = current_date_str
            
        await asyncio.sleep(30) 

# --- TASKS ---
@safe_execute
async def scheduled_health_check():
    """Updates Status in DB."""
    while True:
        try:
            now = datetime.now().strftime("%d-%m-%Y %I:%M %p")
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
    kb.button(text="🔙 Back to Dashboard", callback_data="btn_refresh")
    return kb.as_markup()

# ==========================================
# 👑 THE DASHBOARD UI
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
        f"👑 **MSA COMMAND CENTER (Apex God Mode)**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 **Users:** `{total_users}`\n"
        f"🚫 **Banned:** `{banned_users}`\n"
        f"🛠 **Maint. Mode:** {maint_status}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardBuilder()
    # Row 1
    kb.row(
        InlineKeyboardButton(text="📢 Broadcast", callback_data="btn_broadcast"),
        InlineKeyboardButton(text="🎯 DM User", callback_data="btn_sniper")
    )
    # Row 2
    kb.row(
        InlineKeyboardButton(text="📈 Traffic Stats", callback_data="btn_traffic"),
        InlineKeyboardButton(text="🩺 Diagnosis", callback_data="btn_diagnosis")
    )
    # Row 3
    kb.row(
        InlineKeyboardButton(text="🚫 Ban User", callback_data="btn_ban_menu"),
        InlineKeyboardButton(text="🛡️ Admins", callback_data="btn_add_admin")
    )
    # Row 4
    kb.row(
        InlineKeyboardButton(text="💾 Backup", callback_data="btn_backup")
    )
    # Row 5
    kb.row(
        InlineKeyboardButton(text="🔄 Refresh", callback_data="btn_refresh"),
        InlineKeyboardButton(text="ℹ️ Help Guide", callback_data="btn_help")
    )
    # Row 6
    kb.row(
        InlineKeyboardButton(text="🛠 Maintenance", callback_data="btn_maint_toggle"),
        InlineKeyboardButton(text="💤 Sleep", callback_data="btn_sleep")
    )
    
    try:
        if is_edit: 
            await message_obj.edit_text(text, reply_markup=kb.as_markup())
        else: 
            await message_obj.answer(text, reply_markup=kb.as_markup())
    except TelegramBadRequest as e:
        pass

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
    await callback.message.answer("💤 **System Running in Background.**\nType `/start` to wake up.")

# ==========================================
# 📢 BROADCAST SYSTEM
# ==========================================
@dp.callback_query(F.data == "btn_broadcast")
async def broadcast_menu(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    
    last_broadcast = col_broadcast_logs.find_one(sort=[("_id", -1)])
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Write New", callback_data="start_broadcast_new")
    
    if last_broadcast:
        kb.button(text="✏️ Edit Last", callback_data="edit_last_broadcast")
        kb.button(text="🔥 Unsend Last", callback_data="unsend_last_broadcast")
    
    kb.button(text="🔙 Back", callback_data="btn_refresh")
    kb.adjust(1)
    await callback.message.edit_text(f"📢 **Broadcast Manager**", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "start_broadcast_new")
async def start_broadcast(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("📝 **Send Content to Broadcast.**")
    await state.set_state(BroadcastState.waiting_for_message)

@dp.message(BroadcastState.waiting_for_message)
async def receive_broadcast_msg(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    content_type = "text"; file_path = None; text_content = message.text or message.caption or ""
    
    if not message.text: 
        file_obj = None; ext = "dat"
        if message.photo: 
            content_type = "photo"
            file_obj = message.photo[-1]
            ext="jpg"
        elif message.video: 
            content_type = "video"
            file_obj = message.video
            ext="mp4"
        elif message.document: 
            content_type = "document"
            file_obj = message.document
            ext="pdf"
        
        if file_obj: 
            await message.answer("📥 **Downloading...**")
            file_path = f"temp_{message.from_user.id}.{ext}"
            await manager_bot.download(file_obj, destination=file_path)
            
    await state.update_data(ctype=content_type, text=text_content, path=file_path)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔥 FIRE", callback_data="confirm_send")
    kb.button(text="❌ CANCEL", callback_data="cancel_send")
    
    await message.answer(f"📢 **Ready to Broadcast {content_type}?**", reply_markup=kb.as_markup())
    await state.set_state(BroadcastState.confirm_send)

@dp.callback_query(F.data == "confirm_send")
async def execute_broadcast(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await callback.message.edit_text("🚀 **Broadcasting...**")
    
    cached_file_id = None; sent = 0; blocked_count = 0; file_path = data.get('path'); msg_ids = []
    batch_id = int(time.time())
    
    try:
        cursor = col_users.find({"status": "Active"}, {"user_id": 1})
        for doc in cursor:
            uid = doc.get("user_id")
            try:
                chat_id = int(uid)
                media = cached_file_id or (FSInputFile(file_path) if file_path else None)
                msg = None
                
                if data['ctype'] == 'text': 
                    msg = await worker_bot.send_message(chat_id, data['text'])
                else:
                    if data['ctype'] == 'photo': 
                        msg = await worker_bot.send_photo(chat_id, media, caption=data['text'])
                    elif data['ctype'] == 'video': 
                        msg = await worker_bot.send_video(chat_id, media, caption=data['text'])
                    elif data['ctype'] == 'document': 
                        msg = await worker_bot.send_document(chat_id, media, caption=data['text'])
                
                if msg:
                    msg_ids.append({"chat_id": chat_id, "message_id": msg.message_id})
                    if not cached_file_id:
                        if data['ctype'] == 'photo': cached_file_id = msg.photo[-1].file_id
                        elif data['ctype'] == 'video': cached_file_id = msg.video.file_id
                        elif data['ctype'] == 'document': cached_file_id = msg.document.file_id
                
                sent += 1
                await asyncio.sleep(0.05) 
            except TelegramForbiddenError: 
                blocked_count += 1
                col_users.update_one({"user_id": uid}, {"$set": {"status": "BLOCKED"}})
            except Exception: pass
        
        if msg_ids: 
            col_broadcast_logs.insert_one({
                "batch_id": batch_id, 
                "date": datetime.now().strftime("%d-%m-%Y %I:%M %p"), 
                "messages": msg_ids, 
                "type": data['ctype'], 
                "original_text": data['text']
            })
        
        await callback.message.answer(f"✅ **Done.** Sent: {sent} | Blocked: {blocked_count}")
    
    except Exception as e: 
        await callback.message.answer(f"❌ Error: {e}")
    
    if file_path and os.path.exists(file_path): os.remove(file_path)
    await state.clear()
    await callback.message.delete()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=False)

@dp.callback_query(F.data == "unsend_last_broadcast")
async def unsend_last(callback: types.CallbackQuery):
    await callback.message.edit_text("⏳ **Deleting last broadcast...**")
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    if not last_log: 
        await callback.message.edit_text("❌ No history.")
        return
    
    deleted = 0
    messages = last_log.get("messages", [])
    for entry in messages:
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
        await callback.answer("❌ Text only.", show_alert=True)
        return
    await callback.message.edit_text(f"📝 **Current Text:**\n{last_log.get('original_text')}\n\n👇 **Send NEW Text:**")
    await state.set_state(BroadcastState.waiting_for_edit)

@dp.message(BroadcastState.waiting_for_edit)
async def edit_last_execute(message: types.Message, state: FSMContext):
    new_text = message.text
    await message.answer("⏳ **Editing...**")
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    edited = 0
    messages = last_log.get("messages", [])
    
    for entry in messages:
        try:
            await worker_bot.edit_message_text(text=new_text, chat_id=entry['chat_id'], message_id=entry['message_id'])
            edited += 1
            await asyncio.sleep(0.03)
        except: pass
    
    col_broadcast_logs.update_one({"_id": last_log["_id"]}, {"$set": {"original_text": new_text}})
    await message.answer(f"✅ **Edited {edited} messages.**")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id, is_edit=False)

# ==========================================
# 🚫 BAN SYSTEM & ADMIN
# ==========================================
@dp.callback_query(F.data == "btn_ban_menu")
async def ban_menu(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("🚫 **Enter User ID to Ban:**")
    await state.set_state(BanState.waiting_for_id)

@dp.message(BanState.waiting_for_id)
async def execute_ban(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    target_id = message.text.strip()
    if not target_id.isdigit(): 
        await message.answer("❌ Invalid.")
        return
    
    col_banned.update_one(
        {"user_id": target_id},
        {"$set": {"banned_at": datetime.now(), "banned_by": message.from_user.first_name}},
        upsert=True
    )
    col_users.update_one({"user_id": target_id}, {"$set": {"status": "BLOCKED"}})
    await message.answer(f"⛔ **Banned {target_id}.**")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id, is_edit=False)

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
    await message.answer("✅ Added.")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id, is_edit=False)

# ==========================================
# 🩺 DIAGNOSTICS & SYSTEM
# ==========================================
@dp.callback_query(F.data == "btn_diagnosis")
async def run_diagnosis(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await callback.message.edit_text("🩺 **Scanning...**")
    try:
        ts = time.time()
        cu = col_users.count_documents({})
        ca = col_active.count_documents({})
        lat = round((time.time()-ts)*1000, 2)
        report = (
            f"🩺 **DIAGNOSTICS**\n"
            f"📦 **DB:** Stable ({lat}ms)\n"
            f"👥 Users: `{cu}`\n"
            f"📄 Files: `{ca}`\n"
            f"🤖 **Bots:** Online"
        )
        await callback.message.edit_text(report, reply_markup=back_kb())
    except Exception as e: 
        await callback.message.edit_text(f"❌ Error: {e}", reply_markup=back_kb())

@dp.callback_query(F.data == "btn_status")
async def show_status(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    try:
        m_status = col_settings.find_one({"setting": "manager_status"}) or {}
        w_status = col_settings.find_one({"setting": "worker_status"}) or {}
        report = f"📡 **STATUS**\n\nManager: {m_status.get('status', 'Unknown')}\nWorker: {w_status.get('status', 'Unknown')}"
        await callback.message.edit_text(report, reply_markup=back_kb())
    except Exception as e: 
        await callback.message.answer(f"❌ Error: {e}")

@dp.callback_query(F.data == "btn_maint_toggle")
async def toggle_maintenance(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    curr = col_settings.find_one({"setting": "maintenance"})
    new_val = not (curr and curr.get("value"))
    col_settings.update_one({"setting": "maintenance"}, {"$set": {"value": new_val}}, upsert=True)
    await callback.answer(f"Maintenance: {'ON' if new_val else 'OFF'}")
    await show_dashboard_ui(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "btn_backup")
async def backup_data(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.message.edit_text("⏳ **Backing up...**")
    try:
        df = list(col_users.find({}, {"_id": 0}))
        if df:
            with open("Users.csv", 'w', newline='', encoding='utf-8') as f: 
                csv.DictWriter(f, df[0].keys()).writeheader()
                csv.DictWriter(f, df[0].keys()).writerows(df)
            await callback.message.answer_document(FSInputFile("Users.csv"), caption="💾 Users")
            os.remove("Users.csv")
    except: 
        await callback.message.answer("❌ Error")
    await show_dashboard_ui(callback.message, callback.from_user.id)

# ==========================================
# 📈 TRAFFIC & SNIPER
# ==========================================
@dp.callback_query(F.data == "btn_traffic")
async def traffic_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    res = list(col_users.aggregate([{"$group": {"_id": "$source", "count": {"$sum": 1}}}]))
    total = sum([r['count'] for r in res])
    cnt = {r['_id']: r['count'] for r in res}
    
    report = (
        f"📈 **TRAFFIC**\n"
        f"🔴 YT: {cnt.get('YouTube', 0)}\n"
        f"📸 IG: {cnt.get('Instagram', 0)}\n"
        f"📊 Total: {total}"
    )
    await callback.message.edit_text(report, reply_markup=back_kb())

@dp.callback_query(F.data == "btn_sniper")
async def start_sniper(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return await callback.answer("⛔ Access Denied")
    await callback.message.edit_text("🎯 **Enter User ID:**")
    await state.set_state(SniperState.waiting_for_target_id)

@dp.message(SniperState.waiting_for_target_id)
async def sniper_id(message: types.Message, state: FSMContext):
    await state.update_data(target_id=message.text)
    await message.answer("📝 **Message:**")
    await state.set_state(SniperState.waiting_for_message)

@dp.message(SniperState.waiting_for_message)
async def sniper_msg(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    kb = InlineKeyboardBuilder()
    kb.button(text="🚀 SEND", callback_data="confirm_sniper")
    await message.answer("Confirm?", reply_markup=kb.as_markup())
    await state.set_state(SniperState.confirm_send)

@dp.callback_query(F.data == "confirm_sniper")
async def execute_sniper(callback: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try: 
        await worker_bot.send_message(chat_id=d['target_id'], text=d['text'])
        await callback.message.answer("✅ Sent.")
    except Exception as e: 
        await callback.message.answer(f"❌ Failed: {e}")
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=False)

@dp.callback_query(F.data == "cancel_send")
async def cancel_op(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=False)

@dp.callback_query(F.data == "btn_help")
async def help_guide(callback: types.CallbackQuery):
    help_text = (
        "📘 **APEX MANAGER PROTOCOL**\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "**🤖 AUTOMATION (Active)**\n"
        "• **Watchdog:** Checks System every 5 mins.\n"
        "• **Daily Report:** Sent at 08:40 AM.\n"
        "• **Panic Protocol:** Auto-Backup & Lock if crashed.\n\n"
        "**📢 MARKETING**\n"
        "• **Broadcast:** Blast msg to all users.\n"
        "• **Unsend/Edit:** Fix mistakes in last broadcast.\n\n"
        "**🛡️ SECURITY**\n"
        "• **Ban:** Block User ID instantly.\n"
        "• **Sniper:** Send private DM to 1 user.\n"
        "• **Maintenance:** Force 'System Upgrade' mode.\n\n"
        "**📊 DATA**\n"
        "• **Traffic:** See YouTube vs Instagram %.\n"
        "• **Backup:** Download User Database (CSV)."
    )
    await callback.message.edit_text(help_text, reply_markup=back_kb(), parse_mode="Markdown")

# ==========================================
# MAIN EXECUTION
# ==========================================
async def main():
    print("👑 Manager Bot (Apex God Mode) is Online...")
    try: 
        await manager_bot.send_message(OWNER_ID, "🟢 **Manager Bot Online**\nSystem is self-healing and active.")
    except: 
        pass
    
    asyncio.create_task(supervisor_routine()) 
    asyncio.create_task(scheduled_health_check())
    asyncio.create_task(scheduled_pruning_cleanup()) 
    await dp.start_polling(manager_bot)

if __name__ == "__main__":

    asyncio.run(main())
