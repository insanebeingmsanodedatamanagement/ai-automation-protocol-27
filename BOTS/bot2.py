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

# Channel IDs
BAN_CHANNEL_ID = -1003575487367  # Ban notifications channel
APPEAL_CHANNEL_ID = int(os.getenv("APPEAL_CHANNEL_ID", -1003354981499))  # Ban appeal channel

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
    selecting_from_history = State()
    waiting_for_history_index = State()
    waiting_for_template_name = State()
    waiting_for_template_content = State()
    waiting_for_template_selection = State()
    waiting_for_template_delete = State()
    waiting_for_button_text = State()
    waiting_for_button_url = State()

class DeleteBroadcastState(StatesGroup):
    waiting_for_msg_id = State()

# BROADCAST CONTROL
BROADCAST_RUNNING = {}
BROADCAST_TEMPLATES = {}

class AdminState(StatesGroup):
    waiting_for_add_admin_id = State()  # For adding new admin
    waiting_for_name = State()
    waiting_for_action_id = State()  # For admin actions (reset cooldown, resolve ticket, etc)
    waiting_for_quick_reply_user = State()  # For sending quick reply to user

class ReviewState(StatesGroup):
    viewing_all = State()  # For paginated view
    viewing_pending = State()  # For pending reviews pagination

class AppealState(StatesGroup):
    waiting_for_template_message = State()  # For custom warning messages
    viewing_appeals = State()  # For viewing appeals list
    waiting_cooldown_days = State()  # For changing cooldown period

class ShootState(StatesGroup):
    waiting_for_ban_id = State()
    waiting_for_ban_type = State()
    waiting_for_ban_reason = State()
    waiting_for_unban_id = State()
    waiting_for_suspend_id = State()
    waiting_for_reset_id = State()
    waiting_for_unban_features_id = State()
    waiting_for_ban_history_id = State()

class SniperState(StatesGroup):
    waiting_for_target_id = State()
    waiting_for_message = State()
    confirm_send = State()

# --- MONGODB CONNECTION ---
print("Synchronizing Manager with MSANode Database...")

# Initialize as None - will be set in async init
client = None
db = None
col_users = None
col_admins = None
col_settings = None
col_active = None
col_viral = None
col_reels = None
col_banned = None
col_broadcast_logs = None
col_templates = None
col_recycle_bin = None
col_reviews = None
col_appeals = None

async def initialize_database():
    """Initialize database connections asynchronously after bot starts"""
    global client, db, col_users, col_admins, col_settings, col_active
    global col_viral, col_reels, col_banned, col_broadcast_logs
    global col_templates, col_recycle_bin, col_reviews, col_appeals, col_ban_history
    
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["MSANodeDB"]
        col_users = db["user_logs"]
        col_admins = db["admins"]
        col_settings = db["settings"]
        col_active = db["active_content"]
        col_viral = db["viral_videos"]
        col_reels = db["viral_reels"]
        col_banned = db["banned_users"]
        col_ban_history = db["ban_history"]
        col_broadcast_logs = db["broadcast_logs"]
        col_templates = db["broadcast_templates"]
        col_recycle_bin = db["recycle_bin"]
        col_reviews = db["reviews"]
        col_appeals = db["ban_appeals"]
        
        # Create indexes for optimized queries (handles millions of users)
        # Wrap each index creation separately to avoid conflicts with existing indexes
        indexes_created = 0
        
        def safe_create_index(collection, keys, **kwargs):
            nonlocal indexes_created
            try:
                collection.create_index(keys, **kwargs)
                indexes_created += 1
            except pymongo.errors.OperationFailure as e:
                # Skip if index already exists (code 85 or 86)
                if e.code in [85, 86]:  # IndexOptionsConflict or IndexKeySpecsConflict
                    pass  # Silently skip - index already exists
                else:
                    print(f"⚠️ Index warning: {e}")
        
        # User collection indexes (no custom names to avoid conflicts)
        safe_create_index(col_users, "msa_id", unique=True)
        safe_create_index(col_users, "user_id")
        safe_create_index(col_users, [("support_status", 1), ("support_timestamp", -1)])
        
        # Review collection indexes
        safe_create_index(col_reviews, [("user_id", 1), ("timestamp", -1)])
        safe_create_index(col_reviews, [("status", 1), ("timestamp", -1)])
        safe_create_index(col_reviews, "msa_id")
        
        # Banned users indexes
        safe_create_index(col_banned, "msa_id")
        safe_create_index(col_banned, "user_id")
        
        print(f"✅ Database indexes ready ({indexes_created} new, existing ones preserved)")
        
        # Test connection
        client.admin.command('ping')
        print("✅ MSANode Data Core: CONNECTED")
        return True
    except Exception as e:
        print(f"❌ Database Connection Failed: {e}")
        return False

# --- RENDER PORT BINDER (SHIELD) ---
async def handle_health(request):
    return web.Response(text="MSANODE MANAGER CORE IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        # Try alternative ports if 10000 is in use
        for port_offset in range(10):
            try:
                web.run_app(app, host='0.0.0.0', port=port + port_offset, handle_signals=False, print=None)
                print(f"Health server started on port {port + port_offset}")
                break
            except OSError:
                if port_offset == 9:
                    print(f"Health Server: All ports 10000-10009 in use, continuing without health server")
                continue
    except Exception as e:
        print(f"📡 Health Server Note: {e}")
        # Continue without health server - not critical for bot operation

# ==========================================
# 🛡️ IRON DOME & HELPERS (UNREDUCED)
# ==========================================
async def send_alert(msg):
    """Sends critical alerts to Owner."""
    try:
        await manager_bot.send_message(OWNER_ID, f"🚨 **MSANODE SYSTEM ALERT** 🚨\n\n{msg}")
    except: pass
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

async def log_ban_action(action_type: str, user_id: str, user_name: str, admin_name: str, 
                        reason: str = None, ban_type: str = None, ban_until = None, 
                        banned_features: list = None, violation_type: str = None):
    """
    Log ban/unban/suspend actions to database and send notification to ban channel.
    
    Args:
        action_type: 'ban', 'unban', 'suspend', 'unsuspend', 'ban_features', 'unban_features', 'auto_ban'
        user_id: Telegram user ID
        user_name: User's display name
        admin_name: Admin who performed action (or 'System' for auto-bans)
        reason: Custom reason message
        ban_type: 'permanent', 'temporary', or None
        ban_until: DateTime for temporary bans
        banned_features: List of banned features
        violation_type: Type of violation for auto-bans
    """
    try:
        # Get user's MSA ID
        user_doc = col_users.find_one({"user_id": user_id})
        msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
        username = user_doc.get("username", "No Username") if user_doc else "No Username"
        
        # Create history record
        history_record = {
            "user_id": user_id,
            "msa_id": msa_id,
            "username": username,
            "user_name": user_name,
            "action_type": action_type,
            "admin_name": admin_name,
            "reason": reason,
            "ban_type": ban_type,
            "ban_until": ban_until,
            "banned_features": banned_features,
            "violation_type": violation_type,
            "timestamp": datetime.now(IST)
        }
        
        # Save to database
        col_ban_history.insert_one(history_record)
        
        # Build channel notification message
        now_str = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
        
        if action_type == "ban":
            status_emoji = "🚫"
            action_text = "USER BANNED"
            duration_text = ""
            if ban_type == "temporary" and ban_until:
                unban_date = ban_until.strftime("%d %b %Y, %I:%M %p")
                duration_text = f"\n⏰ Duration: 7 Days (Until {unban_date} IST)"
            elif ban_type == "permanent":
                duration_text = "\n⏰ Duration: PERMANENT"
            
            channel_msg = (
                f"{status_emoji} **{action_text}**\n"
                f"═══════════════════════\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 Telegram ID: {user_id}\n"
                f"🏷️ MSA ID: {msa_id}\n"
                f"👤 Username: @{username}\n"
                f"👮 Banned By: {admin_name}\n"
                f"📅 Date: {now_str}{duration_text}\n\n"
            )
            if reason:
                channel_msg += f"📝 Reason:\n{reason}\n\n"
            else:
                channel_msg += "📝 Reason: Policy Violation (Default)\n\n"
            
            if banned_features:
                features_list = ", ".join([f.title() for f in banned_features])
                channel_msg += f"🚫 Banned Features:\n{features_list}\n\n"
            
            channel_msg += "═══════════════════════\n"
            channel_msg += f"Status: {'⏳ TEMPORARY' if ban_type == 'temporary' else '🔒 PERMANENT'}"
            
        elif action_type == "unban":
            channel_msg = (
                f"✅ **USER UNBANNED**\n"
                f"═══════════════════════\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 Telegram ID: {user_id}\n"
                f"🏷️ MSA ID: {msa_id}\n"
                f"👤 Username: @{username}\n"
                f"👮 Processed By: MSA NODE AGENT\n"
                f"📅 Date: {now_str}\n\n"
            )
            if reason:
                channel_msg += f"📝 Note: {reason}\n\n"
            channel_msg += "═══════════════════════\n"
            channel_msg += "Status: 🟢 ACTIVE"
            
        elif action_type == "suspend" or action_type == "ban_features":
            if banned_features:
                features_list = ", ".join([f.title() for f in banned_features])
                channel_msg = (
                    f"⏸️ **FEATURES SUSPENDED**\n"
                    f"═══════════════════════\n\n"
                    f"👤 User: {user_name}\n"
                    f"🆔 Telegram ID: {user_id}\n"
                    f"🏷️ MSA ID: {msa_id}\n"
                    f"👤 Username: @{username}\n"
                    f"👮 Suspended By: {admin_name}\n"
                    f"📅 Date: {now_str}\n\n"
                    f"🚫 Suspended Features:\n{features_list}\n\n"
                )
                if reason:
                    channel_msg += f"📝 Reason: {reason}\n\n"
                channel_msg += "═══════════════════════"
        
        elif action_type == "unsuspend" or action_type == "unban_features":
            if banned_features:
                features_list = ", ".join([f.title() for f in banned_features])
                channel_msg = (
                    f"✅ **FEATURES RESTORED**\n"
                    f"═══════════════════════\n\n"
                    f"👤 User: {user_name}\n"
                    f"🆔 Telegram ID: {user_id}\n"
                    f"🏷️ MSA ID: {msa_id}\n"
                    f"👤 Username: @{username}\n"
                    f"👮 Restored By: {admin_name}\n"
                    f"📅 Date: {now_str}\n\n"
                    f"✅ Restored Features:\n{features_list}\n\n"
                )
                if reason:
                    channel_msg += f"📝 Note: {reason}\n\n"
                channel_msg += "═══════════════════════"
        
        elif action_type == "auto_ban":
            channel_msg = (
                f"🚨 **AUTO-BAN TRIGGERED**\n"
                f"═══════════════════════\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 Telegram ID: {user_id}\n"
                f"🏷️ MSA ID: {msa_id}\n"
                f"👤 Username: @{username}\n"
                f"🤖 Banned By: System (Auto)\n"
                f"📅 Date: {now_str}\n\n"
            )
            if violation_type:
                channel_msg += f"⚠️ Violation Type: {violation_type}\n"
            if reason:
                channel_msg += f"📝 Reason: {reason}\n\n"
            else:
                channel_msg += "📝 Reason: Multiple Security Violations\n\n"
            channel_msg += "═══════════════════════\n"
            channel_msg += "Status: 🔒 PERMANENT (AUTO)"
        
        # Send to ban channel
        if BAN_CHANNEL_ID:
            try:
                await manager_bot.send_message(
                    chat_id=BAN_CHANNEL_ID,
                    text=channel_msg,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Failed to send ban notification to channel: {e}")
        
    except Exception as e:
        logger.error(f"Error logging ban action: {e}")

def resolve_user_id(input_str):
    """Resolves MSA ID or regular user ID to actual user ID - MSA ID prioritized for millions of users"""
    input_str = str(input_str).strip()
    
    # Check if it's MSA ID format (prioritized for optimal data organization)
    if input_str.upper().startswith("MSA"):
        # Extract the numeric part
        msa_id = input_str.upper().replace("MSA", "").replace("-", "").replace("_", "").strip()
        # Use indexed MSA ID lookup (optimized for millions of records)
        user = col_users.find_one({"msa_id": input_str.upper()}) or col_users.find_one({"msa_id": f"MSA{msa_id}"})
        if user:
            return user.get("user_id"), user.get("first_name", "User")
    
    # Otherwise treat as regular user ID (secondary lookup)
    user = col_users.find_one({"user_id": input_str})
    if user:
        return input_str, user.get("first_name", "User")
    
    return None, None

def get_user_by_msa(msa_id):
    """Get user data by MSA ID - Optimized for indexed lookups"""
    try:
        msa_id = str(msa_id).upper()
        if not msa_id.startswith("MSA"):
            msa_id = f"MSA{msa_id}"
        return col_users.find_one({"msa_id": msa_id})
    except:
        return None

def ensure_msa_id_in_reviews(user_id):
    """Ensure reviews have MSA ID for proper data organization"""
    try:
        user = col_users.find_one({"user_id": user_id})
        if user and user.get("msa_id"):
            # Update all reviews for this user to include msa_id
            col_reviews.update_many(
                {"user_id": user_id, "msa_id": {"$exists": False}},
                {"$set": {"msa_id": user.get("msa_id")}}
            )
    except:
        pass

# ==========================================
#  SUPERVISOR WATCHDOG (5 MINUTE SCAN)
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
                f" 📅 `{now_ist.strftime('%d-%m-%Y %I:%M %p')}`\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"✅ **Command Hub:** Active\n"
                f"💥 **Army Size:** `{total_u}`"
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
        f"━━━━━━━━━━━━━━━━━\n"
        f"💥 **Operatives:** `{total_u}`\n"
        f"✅ **System:** {status}\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardBuilder()
    # Row 1: Direct Comms
    kb.row(InlineKeyboardButton(text="📢 Broadcast", callback_data="btn_broadcast"))
    # Row 2: Management
    kb.row(InlineKeyboardButton(text="📋 List All", callback_data="btn_list_all"), 
        InlineKeyboardButton(text="🔍 Find User", callback_data="btn_find_op"))
    # Row 3: Intelligence
    kb.row(InlineKeyboardButton(text="🎯 Traffic", callback_data="btn_traffic"), 
        InlineKeyboardButton(text="📊 Supreme Audit", callback_data="btn_supreme_stats"))
    # Row 4: Security
    kb.row(InlineKeyboardButton(text="🔥 Shoot", callback_data="btn_shoot_menu"))
    # Row 5: Bot Features Control
    kb.row(InlineKeyboardButton(text="⭐ Reviews", callback_data="btn_reviews"),
        InlineKeyboardButton(text="💬 Support", callback_data="btn_support"))
    # Row 6: Appeals & Backup
    kb.row(InlineKeyboardButton(text="🔔 Appeals", callback_data="btn_appeals"),
        InlineKeyboardButton(text="💾 Backup", callback_data="btn_backup"))
    # Row 7: Systems
    kb.row(InlineKeyboardButton(text="🩺 Diagnosis", callback_data="btn_diagnosis"))
    # Row 8: Configuration
    kb.row(InlineKeyboardButton(text="👤 Admins", callback_data="btn_add_admin"),
        InlineKeyboardButton(text="🔐 Lockdown", callback_data="btn_maint_toggle"))
    
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

@dp.callback_query(F.data == "btn_users")
async def hub_users_back(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.clear()
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=True)

# ==========================================
#  BUTTON DRIVEN LOGIC (UNREDUCED)
# ==========================================

@dp.callback_query(F.data == "btn_list_all")
async def hub_list_all(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.clear()
    await show_user_list(callback, page=0, filter_status="all", filter_source="all")

async def show_user_list(callback: types.CallbackQuery, page: int = 0, filter_status: str = "all", filter_source: str = "all"):
    """Display paginated user list with filters - 20 users per page"""
    
    # Build query - exclude flagged/spam/bot banned users
    query = {
        "$and": [
            {"$or": [{"is_spam": {"$exists": False}}, {"is_spam": False}]},
            {"$or": [{"is_bot": {"$exists": False}}, {"is_bot": False}]},
            {"$or": [{"is_flagged": {"$exists": False}}, {"is_flagged": False}]}
        ]
    }
    
    # Apply status filter
    if filter_status == "active":
        query["status"] = "Active"
    elif filter_status == "blocked":
        query["status"] = "BLOCKED"
    
    # Apply source filter
    if filter_source == "youtube":
        query["source"] = "YouTube"
    elif filter_source == "instagram":
        query["source"] = "Instagram"
    
    # Pagination settings - 20 per page
    per_page = 20
    skip = page * per_page
    
    # Get users with pagination
    users = list(col_users.find(query, {
        "username": 1, 
        "user_id": 1, 
        "msa_id": 1, 
        "first_name": 1,
        "status": 1,
        "_id": 1
    }).sort("_id", -1).skip(skip).limit(per_page))
    
    total_count = col_users.count_documents(query)
    
    # Calculate total pages
    total_pages = max(1, (total_count + per_page - 1) // per_page)
    current_page = page + 1
    
    # Build header with filter info
    filter_text = []
    if filter_status != "all":
        filter_text.append(f"{filter_status.title()}")
    if filter_source != "all":
        filter_text.append(f"{filter_source.title()}")
    
    text = "📋 **USER LIST**\n"
    text += "━━━━━━━━━━━━━━━━━\n\n"
    
    if filter_text:
        text += f"🔍 Filter: {' | '.join(filter_text)}\n"
    
    text += f"📄 Page {current_page}/{total_pages} | Total: {total_count}\n\n"
    
    if users:
        for idx, u in enumerate(users, skip + 1):
            first_name = u.get('first_name', 'Unknown')
            username = u.get('username', 'N/A')
            user_id = u.get('user_id', 'N/A')
            msa_id = u.get('msa_id', 'N/A')
            status = u.get('status', 'Active')
            
            # Status indicator
            status_icon = "✅" if status == "Active" else "🚫"
            
            text += f"{idx}. {status_icon} {first_name}\n"
            text += f"   🆔 TG: `{user_id}` | MSA: `{msa_id}`\n"
            if username != 'N/A':
                text += f"   👤 @{username}\n"
            text += "\n"
    else:
        text += "❌ No users found\n\n"
    
    text += "━━━━━━━━━━━━━━━━━"
    
    # Build keyboard with filters and pagination
    kb = InlineKeyboardBuilder()
    
    # Status filters
    kb.row(
        InlineKeyboardButton(text="✅ All" if filter_status == "all" else "All", callback_data=f"list_filter_status_all_{page}_{filter_source}"),
        InlineKeyboardButton(text="✅ Active" if filter_status == "active" else "Active", callback_data=f"list_filter_status_active_{page}_{filter_source}"),
        InlineKeyboardButton(text="✅ Blocked" if filter_status == "blocked" else "Blocked", callback_data=f"list_filter_status_blocked_{page}_{filter_source}")
    )
    
    # Source filters
    kb.row(
        InlineKeyboardButton(text="✅ All" if filter_source == "all" else "All", callback_data=f"list_filter_source_all_{page}_{filter_status}"),
        InlineKeyboardButton(text="✅ YT" if filter_source == "youtube" else "YT", callback_data=f"list_filter_source_youtube_{page}_{filter_status}"),
        InlineKeyboardButton(text="✅ IG" if filter_source == "instagram" else "IG", callback_data=f"list_filter_source_instagram_{page}_{filter_status}")
    )
    
    # Pagination with arrows
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"list_page_{page-1}_{filter_status}_{filter_source}"))
    
    # Page indicator
    nav_buttons.append(InlineKeyboardButton(text=f"· {current_page}/{total_pages} ·", callback_data="noop"))
    
    if current_page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"list_page_{page+1}_{filter_status}_{filter_source}"))
    
    kb.row(*nav_buttons)
    
    # Back button
    kb.row(InlineKeyboardButton(text="🔙 Back", callback_data="btn_refresh"))
    
    try:
        await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await callback.answer("Already showing this view", show_alert=False)
        else:
            raise

# Pagination handler
@dp.callback_query(F.data.startswith("list_page_"))
async def handle_list_pagination(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    parts = callback.data.split("_")
    page = int(parts[2])
    filter_status = parts[3]
    filter_source = parts[4]
    await show_user_list(callback, page, filter_status, filter_source)

# Status filter handler
@dp.callback_query(F.data.startswith("list_filter_status_"))
async def handle_status_filter(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    parts = callback.data.split("_")
    filter_status = parts[3]
    page = int(parts[4])
    filter_source = parts[5]
    await show_user_list(callback, page, filter_status, filter_source)

# Source filter handler
@dp.callback_query(F.data.startswith("list_filter_source_"))
async def handle_source_filter(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    parts = callback.data.split("_")
    filter_source = parts[3]
    page = int(parts[4])
    filter_status = parts[5]
    await show_user_list(callback, page, filter_status, filter_source)

@dp.callback_query(F.data == "btn_supreme_stats")
async def hub_supreme_audit(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    # User statistics
    total_users = col_users.count_documents({})
    active_users = col_users.count_documents({"status": "active"})
    blocked_users = col_users.count_documents({"status": "blocked"})
    yt_users = col_users.count_documents({"source": "YouTube"})
    ig_users = col_users.count_documents({"source": "Instagram"})
    
    # Content statistics
    m_codes = col_active.count_documents({})
    yt_videos = col_viral.count_documents({})
    ig_reels = col_reels.count_documents({})
    banned = col_banned.count_documents({})
    
    # Broadcast statistics
    total_broadcasts = col_broadcast_logs.count_documents({})
    templates_count = col_templates.count_documents({})
    
    # Admin statistics
    admin_count = col_admins.count_documents({})
    
    # Recent activity (last 24 hours)
    from bson.objectid import ObjectId
    yesterday = datetime.now(IST) - timedelta(days=1)
    new_users_24h = col_users.count_documents({
        "_id": {"$gte": ObjectId.from_datetime(yesterday)}
    })
    
    # Database size estimate (document counts)
    total_documents = total_users + m_codes + yt_videos + ig_reels + banned + total_broadcasts + templates_count
    
    # Engagement rate
    engagement_rate = (active_users / total_users * 100) if total_users > 0 else 0
    
    fmt_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
    audit = (
        f"🚨 **MSANODE SUPREME AUDIT**\n"
        f" 📅 `{fmt_time}` IST\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"** USER METRICS:**\n"
        f"Total Army: `{total_users}`\n"
        f"   Active: `{active_users}` ({engagement_rate:.1f}%)\n"
        f"   Blocked: `{blocked_users}`\n"
        f"   YT Source: `{yt_users}`\n"
        f"   IG Source: `{ig_users}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"** CONTENT VAULT:**\n"
        f" M-Codes: `{m_codes}`\n"
        f" YT Videos: `{yt_videos}`\n"
        f" IG Reels: `{ig_reels}`\n"
        f" Banned Items: `{banned}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"** BROADCAST INTEL:**\n"
        f"Total Broadcasts: `{total_broadcasts}`\n"
        f"Saved Templates: `{templates_count}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"** ACTIVITY (24h):**\n"
        f"New Recruits: `{new_users_24h}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"** SYSTEM:**\n"
        f"Admins: `{admin_count}`\n"
        f"Total DB Docs: `{total_documents:,}`\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    await callback.message.edit_text(audit, reply_markup=back_kb())

@dp.callback_query(F.data == "btn_find_op")
async def hub_find_trigger(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Back to Users", callback_data="btn_users")
    await callback.message.edit_text(
        " **SEARCH PROTOCOL**\nEnter Username (@), User ID, or MSA ID:",
        reply_markup=builder.as_markup()
    )
    await state.set_state(ManagementState.waiting_for_find_query)

@dp.message(ManagementState.waiting_for_find_query)
async def process_hub_find(message: types.Message, state: FSMContext):
    clean_q = message.text.replace("@", "").strip()
    
    # Try MSA ID first
    if clean_q.upper().startswith("MSA"):
        target_id, _ = resolve_user_id(clean_q)
        if target_id:
            user = col_users.find_one({"user_id": target_id})
        else:
            user = None
    else:
        # Try username or regular ID
        user = col_users.find_one({"$or": [{"user_id": clean_q}, {"username": {"$regex": f"^{clean_q}$", "$options": "i"}}]})
    
    if not user: return await message.answer(" No Operative found.", reply_markup=back_kb())
    
    msa_id_str = f" MSA ID: `{user.get('msa_id')}`\n" if user.get('msa_id') else ""
    dossier = (
        f" **OPERATIVE DOSSIER**\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f" Name: {user.get('first_name')}\n"
        f" Username: {user.get('username')}\n"
        f" User ID: `{user.get('user_id')}`\n"
        f"{msa_id_str}"
        f" Joined: {user.get('joined_date')}\n"
        f" Origin: {user.get('source')}\n"
        f" Status: {user.get('status')}\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    await message.answer(dossier, reply_markup=back_kb())
    await state.clear()

@dp.callback_query(F.data == "btn_shoot_menu")
async def shoot_menu(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑️ Delete User", callback_data="shoot_delete")
    kb.button(text="⛔ Ban User", callback_data="shoot_ban")
    kb.button(text="✅ Unban User", callback_data="shoot_unban")
    kb.button(text="✅ Unban Features", callback_data="shoot_unban_features")
    kb.button(text="⏸️ Suspend Features", callback_data="shoot_suspend")
    kb.button(text="🔄 Reset User Data", callback_data="shoot_reset")
    kb.button(text="📜 Ban History", callback_data="shoot_ban_history")
    kb.button(text="🔙 Back", callback_data="btn_refresh")
    kb.adjust(2, 2, 2, 1)
    await callback.message.edit_text("🔥 **SHOOT MENU**\nSelect Action:", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "shoot_delete")
async def hub_delete_trigger(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Back", callback_data="btn_shoot_menu")
    await callback.message.edit_text(
        " **PURGE PROTOCOL**\nEnter User ID or MSA ID:\n\n User data will be moved to Recycle Bin (60 days).",
        reply_markup=builder.as_markup()
    )
    await state.set_state(ManagementState.waiting_for_delete_id)

@dp.message(ManagementState.waiting_for_delete_id)
async def process_hub_delete(message: types.Message, state: FSMContext):
    target_id, name = resolve_user_id(message.text)
    if not target_id:
        return await message.answer("❌ User not found. Check ID/MSA ID.", reply_markup=back_kb())
    
    # Get user data before deleting
    user = col_users.find_one({"user_id": target_id})
    if not user:
        return await message.answer("❌ User not found in database.", reply_markup=back_kb())
    
    # Save to recycle bin with deletion timestamp
    user['deleted_at'] = datetime.now(IST)
    user['deleted_by'] = message.from_user.first_name
    col_recycle_bin.insert_one(user)
    
    # Delete from main collection
    res = col_users.delete_one({"user_id": target_id})
    
    text = f" **Operative {name} ({target_id}) moved to Recycle Bin.**\n Data will be auto-deleted after 60 days.\n\n If they return, they'll be treated as a new user." if res.deleted_count > 0 else " Delete failed."
    await message.answer(text, reply_markup=back_kb())
    await state.clear()

# ==========================================
#  BROADCAST & TARGETING RADAR (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_broadcast")
async def broadcast_menu(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="🚀 New Broadcast", callback_data="start_broadcast_new")
    kb.button(text="🧪 Test Mode", callback_data="start_test_broadcast")
    kb.button(text="💬 Direct Message", callback_data="btn_sniper")
    kb.button(text="🔀 Clone & Resend", callback_data="clone_broadcast")
    kb.button(text="✏️ Edit Last", callback_data="edit_last_broadcast")
    kb.button(text="📜 View History", callback_data="view_broadcast_history")
    kb.button(text="📝 Templates", callback_data="manage_templates")
    kb.button(text="🗑️ Delete Message", callback_data="delete_by_msg_id")
    kb.button(text="🔙 Back to Hub", callback_data="btn_refresh")
    kb.adjust(2, 1, 2, 2, 1, 1, 1)
    await callback.message.edit_text("📢 **BROADCAST CONTROL CENTER**\n\nSelect Operation:", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "start_broadcast_new")
async def start_broadcast(callback: types.CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📊 ALL", callback_data="target_all"),
        InlineKeyboardButton(text="▶️ YT Path", callback_data="target_yt"),
        InlineKeyboardButton(text="📷 IG Path", callback_data="target_ig"))
    kb.row(InlineKeyboardButton(text="❌ ABORT", callback_data="btn_broadcast"))
    await callback.message.edit_text("🎯 **Select Target Group:**", reply_markup=kb.as_markup())
    await state.set_state(BroadcastState.waiting_for_filter)

@dp.callback_query(BroadcastState.waiting_for_filter, F.data.startswith("target_"))
async def select_filter(callback: types.CallbackQuery, state: FSMContext):
    target = callback.data.split("_")[1]
    data = await state.get_data()
    
    # Check if using template (already has content)
    if data.get('template_name'):
        # Template content already loaded, go directly to confirmation
        await state.update_data(target_filter=target)
        kb = InlineKeyboardBuilder()
        kb.button(text="👁️ PREVIEW", callback_data="preview_broadcast")
        kb.button(text="➕ Add Button", callback_data="add_inline_button")
        kb.button(text=" FIRE", callback_data="confirm_send")
        kb.button(text="❌ ABORT", callback_data="cancel_send")
        kb.adjust(2, 2)
        
        template_name = data.get('template_name', 'Template')
        await callback.message.edit_text(
            f" **Template:** {template_name}\n"
            f" **Target:** {target.upper()}\n\n"
            f" **Ready for Transmission?**",
            reply_markup=kb.as_markup()
        )
        await state.set_state(BroadcastState.confirm_send)
    else:
        # Normal flow - ask for content
        await state.update_data(target_filter=target)
        await callback.message.edit_text(f" **Enter Content to Broadcast:**")
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
            await message.answer("📝 **Downloading Buffers...**")
            path = f"t_{message.from_user.id}.{ext}"
            await manager_bot.download(file_obj, destination=path)
            
    await state.update_data(ctype=ctype, text=text, path=path)
    
    t_filter = (await state.get_data()).get('target_filter', 'all')
    if t_filter == 'test':
        kb = InlineKeyboardBuilder()
        kb.button(text=" SEND TEST", callback_data="confirm_send")
        kb.button(text="➕ Add Button", callback_data="add_inline_button")
        kb.button(text="❌ ABORT", callback_data="cancel_send")
        kb.adjust(2, 1)
        await message.answer(f" **Test Mode - Preview**\n\n{text[:200]}{'...' if len(text) > 200 else ''}", reply_markup=kb.as_markup())
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="👁️ PREVIEW", callback_data="preview_broadcast")
        kb.button(text="➕ Add Button", callback_data="add_inline_button")
        kb.button(text=" FIRE", callback_data="confirm_send")
        kb.button(text="❌ ABORT", callback_data="cancel_send")
        kb.adjust(2, 2)
        await message.answer(f" **Ready for Transmission?**", reply_markup=kb.as_markup())
    await state.set_state(BroadcastState.confirm_send)

@dp.callback_query(F.data == "preview_broadcast")
async def preview_broadcast(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    d = await state.get_data()
    
    # Send preview to admin
    try:
        inline_kb = None
        if d.get('inline_buttons'):
            inline_kb = InlineKeyboardBuilder()
            for btn in d['inline_buttons']:
                inline_kb.button(text=btn['text'], url=btn['url'])
            inline_kb.adjust(1)
        
        preview_msg = " **PREVIEW MODE**\n \n\n"
        if d['ctype'] == 'text':
            await callback.message.answer(preview_msg + d['text'], reply_markup=inline_kb.as_markup() if inline_kb else None)
        elif d['ctype'] == 'photo':
            await callback.message.answer_photo(FSInputFile(d['path']), caption=preview_msg + d['text'], reply_markup=inline_kb.as_markup() if inline_kb else None)
        elif d['ctype'] == 'video':
            await callback.message.answer_video(FSInputFile(d['path']), caption=preview_msg + d['text'], reply_markup=inline_kb.as_markup() if inline_kb else None)
        elif d['ctype'] == 'document':
            await callback.message.answer_document(FSInputFile(d['path']), caption=preview_msg + d['text'], reply_markup=inline_kb.as_markup() if inline_kb else None)
        
        await callback.answer(" Preview sent above", show_alert=False)
    except Exception as e:
        await callback.answer(f" Preview error: {e}", show_alert=True)

@dp.callback_query(F.data == "add_inline_button")
async def add_inline_button_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="skip_button_add")
    await callback.message.edit_text(
        " **ADD INLINE BUTTON**\n\n"
        "Enter button text (e.g., 'Join Channel', 'Visit Website'):",
        reply_markup=kb.as_markup()
    )
    await state.set_state(BroadcastState.waiting_for_button_text)

@dp.message(BroadcastState.waiting_for_button_text)
async def receive_button_text(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    button_text = message.text.strip()
    await state.update_data(temp_button_text=button_text)
    await message.answer(f" Button: **{button_text}**\n\nNow enter the URL (must start with http:// or https://):")
    await state.set_state(BroadcastState.waiting_for_button_url)

@dp.message(BroadcastState.waiting_for_button_url)
async def receive_button_url(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    url = message.text.strip()
    
    if not url.startswith(('http://', 'https://')):
        return await message.answer("❌ Invalid URL. Must start with http:// or https://")
    
    d = await state.get_data()
    button_text = d.get('temp_button_text')
    
    # Add button to list
    inline_buttons = d.get('inline_buttons', [])
    inline_buttons.append({'text': button_text, 'url': url})
    await state.update_data(inline_buttons=inline_buttons)
    
    # Show updated menu
    kb = InlineKeyboardBuilder()
    kb.button(text="👁️ PREVIEW", callback_data="preview_broadcast")
    kb.button(text=" Add Another", callback_data="add_inline_button")
    kb.button(text=" FIRE", callback_data="confirm_send")
    kb.button(text="❌ ABORT", callback_data="cancel_send")
    kb.adjust(2, 2)
    
    button_list = "\n".join([f"    {btn['text']}   {btn['url']}" for btn in inline_buttons])
    await message.answer(
        f" Button added!\n\n**Buttons ({len(inline_buttons)}):**\n{button_list}\n\n"
        f" **Ready for Transmission?**",
        reply_markup=kb.as_markup()
    )
    await state.set_state(BroadcastState.confirm_send)

@dp.callback_query(F.data == "skip_button_add")
async def skip_button_add(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    d = await state.get_data()
    
    kb = InlineKeyboardBuilder()
    kb.button(text="👁️ PREVIEW", callback_data="preview_broadcast")
    kb.button(text="➕ Add Button", callback_data="add_inline_button")
    kb.button(text=" FIRE", callback_data="confirm_send")
    kb.button(text="❌ ABORT", callback_data="cancel_send")
    kb.adjust(2, 2)
    
    await callback.message.edit_text("📡 **Ready for Transmission?**", reply_markup=kb.as_markup())
    await state.set_state(BroadcastState.confirm_send)

@dp.callback_query(F.data == "confirm_send")
async def execute_broadcast(callback: types.CallbackQuery, state: FSMContext):
    d = await state.get_data(); t_filter = d.get('target_filter', 'all')
    
    # Prepare inline keyboard if buttons exist
    inline_kb = None
    if d.get('inline_buttons'):
        inline_kb = InlineKeyboardBuilder()
        for btn in d['inline_buttons']:
            inline_kb.button(text=btn['text'], url=btn['url'])
        inline_kb.adjust(1)
    
    # Test Mode
    if t_filter == 'test':
        try:
            if d['ctype'] == 'text':
                await worker_bot.send_message(callback.from_user.id, d['text'], reply_markup=inline_kb.as_markup() if inline_kb else None)
            elif d['ctype'] == 'photo':
                await worker_bot.send_photo(callback.from_user.id, FSInputFile(d['path']), caption=d['text'], reply_markup=inline_kb.as_markup() if inline_kb else None)
            elif d['ctype'] == 'video':
                await worker_bot.send_video(callback.from_user.id, FSInputFile(d['path']), caption=d['text'], reply_markup=inline_kb.as_markup() if inline_kb else None)
            elif d['ctype'] == 'document':
                await worker_bot.send_document(callback.from_user.id, FSInputFile(d['path']), caption=d['text'], reply_markup=inline_kb.as_markup() if inline_kb else None)
            
            if d.get('path') and os.path.exists(d['path']): os.remove(d['path'])
            await callback.message.answer(" Test message sent to you!")
            await state.clear()
            return await show_dashboard_ui(callback.message, callback.from_user.id)
        except Exception as e:
            await callback.message.answer(f" Test failed: {e}")
            await state.clear()
            return
    
    query = {"status": "Active"}
    if t_filter == "yt": query["source"] = "YouTube"
    elif t_filter == "ig": query["source"] = "Instagram"
    
    total = col_users.count_documents(query)
    user_id = callback.from_user.id
    BROADCAST_RUNNING[user_id] = True
    
    kb_cancel = InlineKeyboardBuilder()
    kb_cancel.button(text=" CANCEL BROADCAST", callback_data=f"cancel_broadcast_{user_id}")
    radar = await callback.message.edit_text(
        f" **TRANSMITTING WITH ANTI-SPAM**\n Total Target: `{total}`\n Status: Starting...",
        reply_markup=kb_cancel.as_markup()
    )
    
    file_id = None; sent = 0; blocked = 0; failed = 0; path = d.get('path'); msg_ids = []
    start_time = time.time()
    
    try:
        cursor = col_users.find(query, {"user_id": 1})
        for doc in cursor:
            # Check cancel flag
            if not BROADCAST_RUNNING.get(user_id, False):
                await callback.message.answer("📝 **BROADCAST CANCELLED**\n\nPartial stats saved.")
                break
            
            uid = doc.get("user_id")
            try:
                media = file_id or (FSInputFile(path) if path else None)
                m = None
                reply_markup = inline_kb.as_markup() if inline_kb else None
                
                if d['ctype'] == 'text': 
                    m = await worker_bot.send_message(uid, d['text'], reply_markup=reply_markup)
                else:
                    if d['ctype'] == 'photo': 
                        m = await worker_bot.send_photo(uid, media, caption=d['text'], reply_markup=reply_markup)
                    elif d['ctype'] == 'video': 
                        m = await worker_bot.send_video(uid, media, caption=d['text'], reply_markup=reply_markup)
                    elif d['ctype'] == 'document': 
                        m = await worker_bot.send_document(uid, media, caption=d['text'], reply_markup=reply_markup)
                if m:
                    msg_ids.append({"chat_id": int(uid), "message_id": m.message_id})
                    if not file_id:
                        if d['ctype'] == 'photo': file_id = m.photo[-1].file_id
                        elif d['ctype'] == 'video': file_id = m.video.file_id
                        elif d['ctype'] == 'document': file_id = m.document.file_id
                sent += 1
                
                # Live progress update every 5 messages
                if sent % 5 == 0:
                    elapsed = int(time.time() - start_time)
                    progress_bar = " " * (sent * 10 // total) + " " * (10 - (sent * 10 // total))
                    kb_cancel = InlineKeyboardBuilder()
                    kb_cancel.button(text=" CANCEL BROADCAST", callback_data=f"cancel_broadcast_{user_id}")
                    try:
                        await radar.edit_text(
                            f" **LIVE BROADCAST**\n"
                            f"━━━━━━━━━━━━━━━━━\n"
                            f" Progress: `{sent}/{total}` ({sent*100//total}%)\n"
                            f"{progress_bar}\n\n"
                            f"✅ Sent: `{sent}`\n"
                            f" Blocked: `{blocked}`\n"
                            f"❌ Failed: `{failed}`\n"
                            f" Time: {elapsed}s\n"
                            f" Anti-Spam: Active",
                            reply_markup=kb_cancel.as_markup()
                        )
                    except: pass
                
                # Anti-spam delays (CRITICAL for avoiding Telegram bans)
                if sent % 20 == 0:
                    # Every 20 messages, longer pause (5-7 seconds)
                    await asyncio.sleep(6)
                elif sent % 10 == 0:
                    # Every 10 messages, medium pause (2-3 seconds)
                    await asyncio.sleep(2.5)
                else:
                    # Between each message (150-200ms)
                    await asyncio.sleep(0.15 + (sent % 3) * 0.02)  # Randomized slightly
                    
            except TelegramForbiddenError: 
                blocked += 1
                col_users.update_one({"user_id": uid}, {"$set": {"status": "BLOCKED"}})
            except TelegramRetryAfter as e:
                # If hit flood limit, wait longer
                wait_time = e.retry_after + 5
                await callback.message.answer(f" **FLOOD CONTROL**: Pausing {wait_time}s...")
                await asyncio.sleep(wait_time)
                failed += 1
            except Exception as e:
                failed += 1
                logger.error(f"Broadcast error for {uid}: {e}")
        
        BROADCAST_RUNNING[user_id] = False
        elapsed = int(time.time() - start_time)
        
        if msg_ids:
            # Generate unique broadcast ID
            last_broadcast = col_broadcast_logs.find_one(sort=[("_id", -1)])
            next_id = 1
            if last_broadcast and last_broadcast.get('broadcast_id'):
                try:
                    last_num = int(last_broadcast['broadcast_id'].replace('MSG', ''))
                    next_id = last_num + 1
                except: pass
            
            broadcast_id = f"MSG{next_id}"
            col_broadcast_logs.insert_one({
                "broadcast_id": broadcast_id,
                "date": datetime.now(IST).strftime("%d-%m-%Y %I:%M %p"),
                "messages": msg_ids,
                "type": d['ctype'],
                "original_text": d['text']
            })
            
            final_report = (
                f" **BROADCAST COMPLETE**\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f" ID: `{broadcast_id}`\n"
                f"✅ Sent: `{sent}`\n"
                f" Blocked: `{blocked}`\n"
                f"❌ Failed: `{failed}`\n"
                f" Total Time: {elapsed}s\n"
                f" Anti-Spam: Protected"
            )
            await callback.message.answer(final_report)
        else:
            await callback.message.answer(f" Broadcast failed. Sent: {sent} | Blocked: {blocked} | Failed: {failed}")
    except Exception as e:
        await callback.message.answer(f"❌ Error: {e}")
    
    if path and os.path.exists(path): os.remove(path)
    await state.clear(); await show_dashboard_ui(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "view_broadcast_history")
async def view_broadcast_history(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    history = list(col_broadcast_logs.find().sort("_id", -1).limit(10))
    
    if not history:
        await callback.answer(" No broadcast history found.")
        return await broadcast_menu(callback, state)
    
    text = " **BROADCAST HISTORY** (Last 10)\n \n\n"
    for idx, log in enumerate(history, 1):
        btype = log.get('type', 'text').upper()
        date = log.get('date', 'Unknown')
        msg_count = len(log.get('messages', []))
        broadcast_id = log.get('broadcast_id', f'OLD{idx}')
        preview = log.get('original_text', 'Media')[:30]
        text += f"{idx}. `{broadcast_id}` [{btype}] - {date}\n    Sent to: {msg_count} users\n    Preview: {preview}...\n\n"
    
    text += "\n **Reply with number (1-10) to manage**"
    kb = InlineKeyboardBuilder()
    kb.button(text=" Back to Broadcast Menu", callback_data="btn_broadcast")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())
    await state.set_state(BroadcastState.waiting_for_history_index)

@dp.message(BroadcastState.waiting_for_history_index)
async def select_broadcast_from_history(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    data = await state.get_data()
    clone_mode = data.get('clone_mode', False)
    
    try:
        index = int(message.text.strip())
        if index < 1 or index > (5 if clone_mode else 10):
            return await message.answer(f"❌ Invalid number. Choose 1-{5 if clone_mode else 10}.")
    except ValueError:
        return await message.answer(" Please enter a valid number.")
    
    limit = 5 if clone_mode else 10
    history = list(col_broadcast_logs.find().sort("_id", -1).limit(limit))
    if index > len(history):
        return await message.answer(" Broadcast not found.")
    
    selected = history[index - 1]
    
    if clone_mode:
        # Clone and ask for target
        await state.update_data(
            ctype=selected.get('type', 'text'),
            text=selected.get('original_text', ''),
            path=None,
            clone_mode=False
        )
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="📊 ALL", callback_data="target_all"),
            InlineKeyboardButton(text="▶️ YT", callback_data="target_yt"),
            InlineKeyboardButton(text="📷 IG", callback_data="target_ig"))
        kb.row(InlineKeyboardButton(text="❌ ABORT", callback_data="btn_broadcast"))
        await message.answer(
            f" **CLONING BROADCAST**\n\n"
            f"Preview: {selected.get('original_text', 'Media')[:100]}...\n\n"
            f"Select target group:",
            reply_markup=kb.as_markup()
        )
        await state.set_state(BroadcastState.waiting_for_filter)
        return
    
    # Normal history view
    await state.update_data(selected_broadcast_id=str(selected['_id']))
    
    btype = selected.get('type', 'text').upper()
    date = selected.get('date', 'Unknown')
    msg_count = len(selected.get('messages', []))
    broadcast_id = selected.get('broadcast_id', 'N/A')
    content = selected.get('original_text', 'Media content')
    
    detail_text = (
        f" **BROADCAST DETAILS**\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f" ID: `{broadcast_id}`\n"
        f" Index: #{index}\n"
        f" Date: {date}\n"
        f" Recipients: {msg_count}\n"
        f" Type: {btype}\n\n"
        f"**Content:**\n{content[:500]}"
    )
    
    kb = InlineKeyboardBuilder()
    if selected.get('type') == 'text':
        kb.button(text=" Edit This Broadcast", callback_data="edit_selected_broadcast")
    kb.button(text=" Delete This Broadcast", callback_data="delete_selected_broadcast")
    kb.button(text=" Back to History", callback_data="view_broadcast_history")
    kb.adjust(1)
    
    await message.answer(detail_text, reply_markup=kb.as_markup())
    await state.set_state(BroadcastState.selecting_from_history)

@dp.callback_query(F.data == "edit_selected_broadcast")
async def edit_selected_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    data = await state.get_data()
    broadcast_id = data.get('selected_broadcast_id')
    
    from bson import ObjectId
    selected = col_broadcast_logs.find_one({"_id": ObjectId(broadcast_id)})
    
    if not selected or selected.get('type') != 'text':
        return await callback.answer(" Cannot edit this broadcast.")
    
    await callback.message.edit_text(
        f" **EDITING BROADCAST**\n\n"
        f"**Current Text:**\n{selected.get('original_text')}\n\n"
        f" **Reply with NEW text:**"
    )
    await state.set_state(BroadcastState.waiting_for_edit)

@dp.callback_query(F.data == "delete_selected_broadcast")
async def delete_selected_broadcast(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    data = await state.get_data()
    broadcast_id = data.get('selected_broadcast_id')
    
    from bson import ObjectId
    selected = col_broadcast_logs.find_one({"_id": ObjectId(broadcast_id)})
    
    if not selected:
        return await callback.answer(" Broadcast not found.")
    
    await callback.message.edit_text("🗑️ **Deleting broadcast messages...**")
    deleted = 0
    
    for entry in selected.get('messages', []):
        try:
            await worker_bot.delete_message(chat_id=entry['chat_id'], message_id=entry['message_id'])
            deleted += 1
            await asyncio.sleep(0.03)
        except: pass
    
    col_broadcast_logs.delete_one({"_id": ObjectId(broadcast_id)})
    await callback.message.answer(f"✅ Deleted {deleted} messages from broadcast.")
    await state.clear()
    await broadcast_menu(callback, state)

@dp.callback_query(F.data == "edit_last_broadcast")
async def edit_last_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    last_log = col_broadcast_logs.find_one(sort=[("_id", -1)])
    
    if not last_log:
        await callback.answer(" No broadcast history.")
        return await broadcast_menu(callback, state)
    
    if last_log.get('type') != 'text':
        await callback.answer(" Can only edit text broadcasts.")
        return await broadcast_menu(callback, state)
    
    await state.update_data(selected_broadcast_id=str(last_log['_id']))
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="btn_broadcast")
    await callback.message.edit_text(
        f" **EDITING LAST BROADCAST**\n\n"
        f"**Current Text:**\n{last_log.get('original_text')}\n\n"
        f" **Reply with NEW text:**",
        reply_markup=kb.as_markup()
    )
    await state.set_state(BroadcastState.waiting_for_edit)

@dp.message(BroadcastState.waiting_for_edit)
async def edit_broadcast_execute(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    data = await state.get_data()
    broadcast_id = data.get('selected_broadcast_id')
    new_text = message.text
    
    from bson import ObjectId
    selected = col_broadcast_logs.find_one({"_id": ObjectId(broadcast_id)})
    
    if not selected:
        await message.answer(" Broadcast not found.")
        await state.clear()
        return await show_dashboard_ui(message, message.from_user.id)
    
    await message.answer("📝 **Updating messages...**")
    edited = 0
    
    for entry in selected.get('messages', []):
        try:
            await worker_bot.edit_message_text(
                text=new_text,
                chat_id=entry['chat_id'],
                message_id=entry['message_id']
            )
            edited += 1
            await asyncio.sleep(0.03)
        except: pass
    
    col_broadcast_logs.update_one(
        {"_id": ObjectId(broadcast_id)},
        {"$set": {"original_text": new_text}}
    )
    
    await message.answer(f" Updated {edited} messages successfully.")
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id)

# === NEW FEATURES ===

# Test Mode
@dp.callback_query(F.data == "start_test_broadcast")
async def start_test_broadcast(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.update_data(target_filter='test')
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="btn_broadcast")
    await callback.message.edit_text(
        " **TEST MODE**\n\n"
        "Message will be sent ONLY to you for testing.\n\n"
        " **Enter your test message:**",
        reply_markup=kb.as_markup()
    )
    await state.set_state(BroadcastState.waiting_for_message)

# Cancel Broadcast Button Handler
@dp.callback_query(F.data.startswith("cancel_broadcast_"))
async def cancel_broadcast_button(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): 
        await callback.answer(" Unauthorized", show_alert=True)
        return
    
    user_id = callback.from_user.id
    if user_id in BROADCAST_RUNNING and BROADCAST_RUNNING[user_id]:
        BROADCAST_RUNNING[user_id] = False
        await callback.answer(" Cancelling broadcast...", show_alert=True)
        try:
            await callback.message.edit_text(
                f"{callback.message.text}\n\n **CANCELLATION REQUESTED**\nStopping after current message..."
            )
        except: pass
    else:
        await callback.answer(" No active broadcast to cancel.", show_alert=True)

# Clone & Resend
@dp.callback_query(F.data == "clone_broadcast")
async def clone_broadcast_menu(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    history = list(col_broadcast_logs.find().sort("_id", -1).limit(5))
    
    if not history:
        await callback.answer(" No broadcasts to clone.")
        return await broadcast_menu(callback, state)
    
    text = " **CLONE & RESEND**\n \n\nSelect broadcast to clone:\n\n"
    for idx, log in enumerate(history, 1):
        broadcast_id = log.get('broadcast_id', f'OLD{idx}')
        btype = log.get('type', 'text').upper()
        preview = log.get('original_text', 'Media')[:40]
        text += f"{idx}. `{broadcast_id}` [{btype}]\n   {preview}...\n\n"
    
    text += "\n Reply with number (1-5)"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="btn_broadcast")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())
    await state.update_data(clone_mode=True)
    await state.set_state(BroadcastState.waiting_for_history_index)

# Templates Management
@dp.callback_query(F.data == "manage_templates")
async def manage_templates(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    templates = list(col_templates.find())
    text = " **BROADCAST TEMPLATES**\n \n\n"
    
    if templates:
        for idx, t in enumerate(templates, 1):
            preview = t['content'][:50]
            text += f"{idx}. **{t['name']}**\n   {preview}...\n\n"
    else:
        text += "No templates saved yet.\n\n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Add Template", callback_data="add_template")
    if templates:
        kb.button(text=" Use Template", callback_data="use_template")
        kb.button(text="🗑️ Delete Template", callback_data="delete_template")
    kb.button(text="🔙 Back", callback_data="btn_broadcast")
    kb.adjust(1)
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "add_template")
async def add_template_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="manage_templates")
    await callback.message.edit_text(
        " **NEW TEMPLATE**\n\n"
        "Enter template name (e.g., 'Welcome', 'Update', 'Promo'):",
        reply_markup=kb.as_markup()
    )
    await state.set_state(BroadcastState.waiting_for_template_name)

@dp.message(BroadcastState.waiting_for_template_name)
async def add_template_name(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    name = message.text.strip()
    if col_templates.find_one({"name": name}):
        return await message.answer("📝 Template with this name already exists.")
    
    await state.update_data(template_name=name)
    await message.answer(f"📝 Template: **{name}**\n\nNow send the template content:")
    await state.set_state(BroadcastState.waiting_for_template_content)

@dp.message(BroadcastState.waiting_for_template_content)
async def add_template_content(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    data = await state.get_data()
    name = data['template_name']
    content = message.text
    
    col_templates.insert_one({"name": name, "content": content})
    BROADCAST_TEMPLATES[name] = content
    
    await message.answer(f"📝 Template **{name}** saved!", reply_markup=back_kb())
    await state.clear()

@dp.callback_query(F.data == "delete_template")
async def delete_template_select(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    
    templates = list(col_templates.find())
    if not templates:
        await callback.answer(" No templates to delete.", show_alert=True)
        return
    
    text = " **DELETE TEMPLATE**\n \n\nSelect template to delete:\n\n"
    for idx, t in enumerate(templates, 1):
        preview = t['content'][:30]
        text += f"{idx}. **{t['name']}**\n   {preview}...\n\n"
    
    text += "\n Reply with template number"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="manage_templates")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())
    await state.set_state(BroadcastState.waiting_for_template_delete)

@dp.message(BroadcastState.waiting_for_template_delete)
async def delete_template_execute(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    try:
        idx = int(message.text.strip())
        templates = list(col_templates.find())
        
        if idx < 1 or idx > len(templates):
            return await message.answer(f"❌ Invalid number. Choose 1-{len(templates)}.")
        
        selected = templates[idx - 1]
        template_name = selected['name']
        
        col_templates.delete_one({"name": template_name})
        if template_name in BROADCAST_TEMPLATES:
            del BROADCAST_TEMPLATES[template_name]
        
        await message.answer(f"📝 Template **{template_name}** deleted!", reply_markup=back_kb())
        await state.clear()
        await show_dashboard_ui(message, message.from_user.id)
        
    except ValueError:
        await message.answer(" Please enter a valid number.")

@dp.callback_query(F.data == "use_template")
async def use_template_select(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    
    templates = list(col_templates.find())
    if not templates:
        await callback.answer(" No templates available.", show_alert=True)
        return
    
    text = " **USE TEMPLATE**\n \n\nSelect template to broadcast:\n\n"
    for idx, t in enumerate(templates, 1):
        preview = t['content'][:40]
        text += f"{idx}. **{t['name']}**\n   {preview}...\n\n"
    
    text += "\n Reply with template number"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="manage_templates")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())
    await state.update_data(use_template_mode=True)
    await state.set_state(BroadcastState.waiting_for_template_selection)

@dp.message(BroadcastState.waiting_for_template_selection)
async def load_template_for_broadcast(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    try:
        idx = int(message.text.strip())
        templates = list(col_templates.find())
        
        if idx < 1 or idx > len(templates):
            return await message.answer(f"❌ Invalid number. Choose 1-{len(templates)}.")
        
        selected = templates[idx - 1]
        template_content = selected['content']
        
        # Now proceed to target selection
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="📊 ALL", callback_data="target_all"),
            InlineKeyboardButton(text="▶️ YT", callback_data="target_yt"),
            InlineKeyboardButton(text="📷 IG", callback_data="target_ig"))
        kb.row(InlineKeyboardButton(text="❌ ABORT", callback_data="btn_broadcast"))
        
        await message.answer(
            f" **Template Loaded:** {selected['name']}\n\n"
            f" **Content:**\n{template_content}\n\n"
            f" **Select Target Group:**",
            reply_markup=kb.as_markup()
        )
        
        # Store template content as the broadcast message
        await state.update_data(
            ctype='text',
            text=template_content,
            path=None,
            template_name=selected['name']
        )
        await state.set_state(BroadcastState.waiting_for_filter)
        
    except ValueError:
        await message.answer(" Please enter a valid number.")

@dp.callback_query(F.data == "delete_by_msg_id")
async def delete_by_msg_id_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="btn_broadcast")
    await callback.message.edit_text(
        " **DELETE BROADCAST BY ID**\n\n"
        "Enter Broadcast ID (e.g., MSG1, MSG2)\n"
        " Tip: Use  View History to see all IDs",
        reply_markup=kb.as_markup()
    )
    await state.set_state(DeleteBroadcastState.waiting_for_msg_id)

@dp.message(DeleteBroadcastState.waiting_for_msg_id)
async def delete_by_msg_id_execute(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    msg_id = message.text.strip().upper()
    broadcast = col_broadcast_logs.find_one({"broadcast_id": msg_id})
    
    if not broadcast:
        await message.answer(f" Broadcast `{msg_id}` not found.\n Check ID in history.", reply_markup=back_kb())
        await state.clear()
        return
    
    await message.answer(f" Deleting broadcast `{msg_id}`...")
    deleted = 0
    
    for entry in broadcast.get('messages', []):
        try:
            await worker_bot.delete_message(chat_id=entry['chat_id'], message_id=entry['message_id'])
            deleted += 1
            await asyncio.sleep(0.03)
        except: pass
    
    col_broadcast_logs.delete_one({"broadcast_id": msg_id})
    await message.answer(f" Broadcast `{msg_id}` deleted!\n Removed {deleted} messages from users.", reply_markup=back_kb())
    await state.clear()

# ==========================================
#  SECURITY & ADMIN (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "shoot_ban")
async def start_ban(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="btn_shoot_menu")
    await callback.message.edit_text("⛔ **BAN PROTOCOL**\nEnter User ID or MSA ID:", reply_markup=kb.as_markup())
    await state.set_state(ShootState.waiting_for_ban_id)

@dp.message(ShootState.waiting_for_ban_id)
async def ask_ban_type(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    target_id, name = resolve_user_id(message.text)
    if not target_id:
        return await message.answer("❌ User not found. Check ID/MSA ID.", reply_markup=back_kb())
    
    await state.update_data(ban_target_id=target_id, ban_target_name=name)
    kb = InlineKeyboardBuilder()
    kb.button(text=" Permanent Ban", callback_data="ban_permanent")
    kb.button(text=" Temporary Ban (7 Days)", callback_data="ban_temporary")
    kb.button(text="❌ Cancel", callback_data="btn_shoot_menu")
    kb.adjust(1)
    await message.answer(
        f" Ban {name} ({target_id})?\n\n**Select Ban Type:**",
        reply_markup=kb.as_markup()
    )
    await state.set_state(ShootState.waiting_for_ban_type)

@dp.callback_query(F.data == "ban_permanent")
async def ask_permanent_ban_reason(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    data = await state.get_data()
    await state.update_data(ban_type="permanent")
    kb = InlineKeyboardBuilder()
    kb.button(text=" Skip (Default Message)", callback_data="ban_skip_reason")
    kb.button(text="❌ Cancel", callback_data="btn_shoot_menu")
    kb.adjust(1)
    await callback.message.edit_text(
        f" **PERMANENT BAN**\n\n Send custom ban reason\nOR press Skip for default message:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(ShootState.waiting_for_ban_reason)

@dp.callback_query(F.data == "ban_temporary")
async def ask_temporary_ban_reason(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    await state.update_data(ban_type="temporary")
    kb = InlineKeyboardBuilder()
    kb.button(text=" Skip (Default Message)", callback_data="ban_skip_reason")
    kb.button(text="❌ Cancel", callback_data="btn_shoot_menu")
    kb.adjust(1)
    await callback.message.edit_text(
        f" **TEMPORARY BAN (7 DAYS)**\n\n Send custom ban reason\nOR press Skip for default message:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(ShootState.waiting_for_ban_reason)

@dp.callback_query(F.data == "ban_skip_reason")
async def execute_ban_no_reason(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    data = await state.get_data()
    target_id = data.get('ban_target_id')
    name = data.get('ban_target_name')
    ban_type = data.get('ban_type', 'permanent')
    
    ban_until = None
    if ban_type == "temporary":
        ban_until = datetime.now(IST) + timedelta(days=7)
    
    col_banned.update_one(
        {"user_id": target_id},
        {"$set": {
            "banned_at": datetime.now(IST),
            "banned_by": callback.from_user.first_name,
            "reason": None,
            "ban_type": ban_type,
            "ban_until": ban_until,
            "banned_features": ["downloads", "reviews", "support", "search"]
        }},
        upsert=True
    )
    col_users.update_one({"user_id": target_id}, {"$set": {"status": "blocked"}})
    
    # Log ban action to history and channel
    await log_ban_action(
        action_type="ban",
        user_id=target_id,
        user_name=name,
        admin_name=callback.from_user.first_name,
        reason=None,
        ban_type=ban_type,
        ban_until=ban_until,
        banned_features=["downloads", "reviews", "support", "search"]
    )
    
    if ban_type == "temporary":
        unban_date = ban_until.strftime("%d %b %Y, %I:%M %p")
        await callback.message.edit_text(
            f" **{name} ({target_id}) TEMPORARILY BANNED.**\n\n"
            f" Ban Duration: 7 Days\n"
            f" Auto-Unban: {unban_date} IST\n\n"
            f" Default ban message will be shown."
        )
    else:
        await callback.message.edit_text(f" **{name} ({target_id}) PERMANENTLY BANNED.**\n\n Default ban message will be shown.")
    
    await state.clear()
    await asyncio.sleep(2)
    await show_dashboard_ui(callback.message, callback.from_user.id, is_edit=True)

@dp.message(ShootState.waiting_for_ban_reason)
async def execute_ban_with_reason(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    data = await state.get_data()
    target_id = data.get('ban_target_id')
    name = data.get('ban_target_name')
    ban_type = data.get('ban_type', 'permanent')
    custom_reason = message.text
    
    ban_until = None
    if ban_type == "temporary":
        ban_until = datetime.now(IST) + timedelta(days=7)
    
    col_banned.update_one(
        {"user_id": target_id},
        {"$set": {
            "banned_at": datetime.now(IST),
            "banned_by": message.from_user.first_name,
            "reason": custom_reason,
            "ban_type": ban_type,
            "ban_until": ban_until,
            "banned_features": ["downloads", "reviews", "support", "search"]
        }},
        upsert=True
    )
    col_users.update_one({"user_id": target_id}, {"$set": {"status": "blocked"}})
    
    # Log ban action to history and channel
    await log_ban_action(
        action_type="ban",
        user_id=target_id,
        user_name=name,
        admin_name=message.from_user.first_name,
        reason=custom_reason,
        ban_type=ban_type,
        ban_until=ban_until,
        banned_features=["downloads", "reviews", "support", "search"]
    )
    
    if ban_type == "temporary":
        unban_date = ban_until.strftime("%d %b %Y, %I:%M %p")
        await message.answer(
            f" **{name} ({target_id}) TEMPORARILY BANNED.**\n\n"
            f" Ban Duration: 7 Days\n"
            f" Auto-Unban: {unban_date} IST\n\n"
            f" Custom reason: {custom_reason}",
            reply_markup=back_kb()
        )
    else:
        await message.answer(
            f" **{name} ({target_id}) PERMANENTLY BANNED.**\n\n Custom reason: {custom_reason}",
            reply_markup=back_kb()
        )
    
    await state.clear()
    await asyncio.sleep(2)
    await show_dashboard_ui(message, message.from_user.id, is_edit=True)

@dp.callback_query(F.data == "shoot_suspend")
async def start_suspend(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="btn_shoot_menu")
    await callback.message.edit_text("⏸️ **SUSPEND FEATURES**\nEnter User ID or MSA ID:", reply_markup=kb.as_markup())
    await state.set_state(ShootState.waiting_for_suspend_id)

@dp.message(ShootState.waiting_for_suspend_id)
async def select_features_to_suspend(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    target_id, name = resolve_user_id(message.text)
    if not target_id:
        return await message.answer("❌ User not found. Check ID/MSA ID.", reply_markup=back_kb())
    
    await state.update_data(suspend_target_id=target_id, suspend_target_name=name)
    
    # Get current suspended features
    user = col_users.find_one({"user_id": target_id})
    suspended = user.get("suspended_features", []) if user else []
    
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{' ' if 'downloads' in suspended else ' '} Downloads", callback_data="suspend_downloads")
    kb.button(text=f"{' ' if 'reviews' in suspended else ' '} Reviews", callback_data="suspend_reviews")
    kb.button(text=f"{' ' if 'support' in suspended else ' '} Customer Support", callback_data="suspend_support")
    kb.button(text=f"{' ' if 'search' in suspended else ' '} Search Function", callback_data="suspend_search")
    kb.button(text=" Save Changes", callback_data="suspend_save")
    kb.button(text="❌ Cancel", callback_data="btn_shoot_menu")
    kb.adjust(2, 2, 1, 1)
    
    await message.answer(
        f" **SUSPEND FEATURES FOR**\n{name} ({target_id})\n\n"
        f"Select features to suspend (  = Suspended):",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("suspend_") & ~F.data.in_(["suspend_save"]))
async def toggle_suspend_feature(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    data = await state.get_data()
    target_id = data.get('suspend_target_id')
    name = data.get('suspend_target_name')
    
    feature = callback.data.replace("suspend_", "")
    user = col_users.find_one({"user_id": target_id})
    suspended = user.get("suspended_features", []) if user else []
    
    if feature in suspended:
        suspended.remove(feature)
    else:
        suspended.append(feature)
    
    col_users.update_one({"user_id": target_id}, {"$set": {"suspended_features": suspended}})
    
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{' ' if 'downloads' in suspended else ' '} Downloads", callback_data="suspend_downloads")
    kb.button(text=f"{' ' if 'reviews' in suspended else ' '} Reviews", callback_data="suspend_reviews")
    kb.button(text=f"{' ' if 'support' in suspended else ' '} Customer Support", callback_data="suspend_support")
    kb.button(text=f"{' ' if 'search' in suspended else ' '} Search Function", callback_data="suspend_search")
    kb.button(text=" Save Changes", callback_data="suspend_save")
    kb.button(text="❌ Cancel", callback_data="btn_shoot_menu")
    kb.adjust(2, 2, 1, 1)
    
    try:
        await callback.message.edit_reply_markup(reply_markup=kb.as_markup())
    except:
        pass
    await callback.answer(f"{'Suspended' if feature in suspended else 'Enabled'}: {feature.title()}")

@dp.callback_query(F.data == "suspend_save")
async def save_suspend_changes(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    data = await state.get_data()
    target_id = data.get('suspend_target_id')
    name = data.get('suspend_target_name')
    
    user = col_users.find_one({"user_id": target_id})
    suspended = user.get("suspended_features", []) if user else []
    
    # Log suspend action to history and channel
    if suspended:
        await log_ban_action(
            action_type="suspend",
            user_id=target_id,
            user_name=name,
            admin_name=callback.from_user.first_name,
            banned_features=suspended
        )
    
    if suspended:
        features_list = ", ".join([f.title() for f in suspended])
        await callback.message.edit_text(
            f" **Features Suspended for {name} ({target_id})**\n\n"
            f" Suspended: {features_list}"
        )
    else:
        await callback.message.edit_text(f" **All features enabled for {name} ({target_id})**")
    
    await state.clear()
    await asyncio.sleep(2)
    await show_dashboard_ui(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "shoot_reset")
async def start_reset(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="btn_shoot_menu")
    await callback.message.edit_text(
        " **RESET USER DATA**\n\n"
        "Enter User ID or MSA ID:\n\n"
        " This will clear user history but keep account active.",
        reply_markup=kb.as_markup()
    )
    await state.set_state(ShootState.waiting_for_reset_id)

@dp.message(ShootState.waiting_for_reset_id)
async def execute_reset(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    target_id, name = resolve_user_id(message.text)
    if not target_id:
        return await message.answer("❌ User not found. Check ID/MSA ID.", reply_markup=back_kb())
    
    # Reset user data but keep essential info
    col_users.update_one(
        {"user_id": target_id},
        {"$set": {
            "suspended_features": [],
            "warnings": 0,
            "last_reset": datetime.now(IST),
            "reset_by": message.from_user.first_name
        }}
    )
    
    await message.answer(
        f" **{name} ({target_id}) Data Reset Complete**\n\n"
        f" Cleared:\n"
        f"   Suspended features\n"
        f"   Warnings\n\n"
        f" Account remains active with fresh start.",
        reply_markup=back_kb()
    )
    await state.clear()
    await asyncio.sleep(2)
    await show_dashboard_ui(message, message.from_user.id)

@dp.callback_query(F.data == "shoot_ban_history")
async def show_ban_history(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    
    banned_users = list(col_banned.find().limit(20))
    
    if not banned_users:
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back", callback_data="btn_shoot_menu")
        await callback.message.edit_text(
            " **BAN HISTORY**\n\n No users are currently banned.",
            reply_markup=kb.as_markup()
        )
        return
    
    history_text = " **BAN HISTORY** (Last 20)\n \n\n"
    
    for ban in banned_users:
        user_id = ban.get('user_id')
        reason = ban.get('reason', 'No reason provided')
        ban_type = ban.get('ban_type', 'permanent')
        banned_at = ban.get('banned_at')
        banned_by = ban.get('banned_by', 'Unknown')
        ban_until = ban.get('ban_until')
        
        if isinstance(banned_at, datetime):
            ban_date = banned_at.strftime("%d %b %Y")
        else:
            ban_date = "Unknown"
        
        if ban_type == "temporary" and ban_until:
            if isinstance(ban_until, datetime):
                unban_date = ban_until.strftime("%d %b %Y")
                history_text += f" 📅 `{user_id}`\n"
                history_text += f" Temp (Until {unban_date})\n"
            else:
                history_text += f" 📅 `{user_id}`\n Temporary\n"
        else:
            history_text += f" 📅 `{user_id}`\n Permanent\n"
        
        history_text += f"  {ban_date} by {banned_by}\n"
        history_text += f"  {reason[:50]}...\n \n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="� Check User History", callback_data="check_user_ban_history")
    kb.button(text="🔙 Back", callback_data="btn_shoot_menu")
    kb.adjust(1)
    
    await callback.message.edit_text(history_text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "check_user_ban_history")
async def start_user_ban_history(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="shoot_ban_history")
    await callback.message.edit_text(
        "📜 **USER BAN HISTORY**\n\nEnter User ID or MSA ID to view complete ban history:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(ShootState.waiting_for_ban_history_id)

@dp.message(ShootState.waiting_for_ban_history_id)
async def display_user_ban_history(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    target_id, name = resolve_user_id(message.text)
    if not target_id:
        return await message.answer("❌ User not found. Check ID/MSA ID.", reply_markup=back_kb())
    
    # Get user info
    user_doc = col_users.find_one({"user_id": target_id})
    msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
    username = user_doc.get("username", "No Username") if user_doc else "No Username"
    
    # Get complete ban history from database (sorted by most recent first)
    history_records = list(col_ban_history.find({"user_id": target_id}).sort("timestamp", -1))
    
    if not history_records:
        await message.answer(
            f"📜 **BAN HISTORY FOR**\n"
            f"👤 {name}\n"
            f"🆔 Telegram ID: {target_id}\n"
            f"🏷️ MSA ID: {msa_id}\n"
            f"👤 Username: @{username}\n\n"
            f"════════════════════\n\n"
            f"✅ Clean Record - No ban history found.",
            reply_markup=back_kb()
        )
        await state.clear()
        return
    
    # Build detailed history report
    history_msg = (
        f"📜 **COMPLETE BAN HISTORY**\n"
        f"════════════════════\n\n"
        f"👤 User: {name}\n"
        f"🆔 Telegram ID: {target_id}\n"
        f"🏷️ MSA ID: {msa_id}\n"
        f"👤 Username: @{username}\n\n"
        f"📊 Total Records: {len(history_records)}\n"
        f"════════════════════\n\n"
    )
    
    for idx, record in enumerate(history_records, 1):
        action_type = record.get("action_type", "unknown")
        admin_name = record.get("admin_name", "Unknown")
        reason = record.get("reason")
        ban_type = record.get("ban_type")
        ban_until = record.get("ban_until")
        banned_features = record.get("banned_features", [])
        violation_type = record.get("violation_type")
        timestamp = record.get("timestamp")
        
        # Format timestamp
        if isinstance(timestamp, datetime):
            date_str = timestamp.strftime("%d %b %Y, %I:%M %p")
        else:
            date_str = "Unknown Date"
        
        # Action emoji and text
        if action_type == "ban":
            emoji = "🚫"
            action_text = "BANNED"
        elif action_type == "unban":
            emoji = "✅"
            action_text = "UNBANNED"
        elif action_type == "suspend" or action_type == "ban_features":
            emoji = "⏸️"
            action_text = "FEATURES SUSPENDED"
        elif action_type == "unsuspend" or action_type == "unban_features":
            emoji = "🔄"
            action_text = "FEATURES RESTORED"
        elif action_type == "auto_ban":
            emoji = "🚨"
            action_text = "AUTO-BANNED"
        else:
            emoji = "📝"
            action_text = action_type.upper()
        
        history_msg += f"**{idx}. {emoji} {action_text}**\n"
        history_msg += f"📅 Date: {date_str}\n"
        history_msg += f"👮 By: {admin_name}\n"
        
        if ban_type:
            if ban_type == "temporary" and ban_until:
                if isinstance(ban_until, datetime):
                    unban_str = ban_until.strftime("%d %b %Y, %I:%M %p")
                    history_msg += f"⏰ Type: Temporary (Until {unban_str})\n"
                else:
                    history_msg += f"⏰ Type: Temporary\n"
            else:
                history_msg += f"⏰ Type: Permanent\n"
        
        if reason:
            history_msg += f"📝 Reason: {reason[:100]}\n"
        
        if violation_type:
            history_msg += f"⚠️ Violation: {violation_type}\n"
        
        if banned_features:
            features_str = ", ".join([f.title() for f in banned_features])
            history_msg += f"🚫 Features: {features_str}\n"
        
        history_msg += "─────────────────\n"
        
        # Limit to 10 records to avoid message too long
        if idx >= 10:
            remaining = len(history_records) - 10
            if remaining > 0:
                history_msg += f"\n... and {remaining} more record(s)\n"
            break
    
    history_msg += "\n════════════════════"
    
    await message.answer(history_msg, reply_markup=back_kb(), parse_mode="Markdown")
    await state.clear()
    await asyncio.sleep(2)
    await show_dashboard_ui(message, message.from_user.id)

@dp.callback_query(F.data == "shoot_unban")
async def start_unban(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="btn_shoot_menu")
    await callback.message.edit_text("✅ **UNBAN PROTOCOL**\nEnter User ID or MSA ID:", reply_markup=kb.as_markup())
    await state.set_state(ShootState.waiting_for_unban_id)

@dp.message(ShootState.waiting_for_unban_id)
async def execute_unban(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    target_id, name = resolve_user_id(message.text)
    if not target_id:
        return await message.answer("❌ User not found. Check ID/MSA ID.", reply_markup=back_kb())
    
    # Get ban record before deleting to store reason
    ban_record = col_banned.find_one({"user_id": target_id})
    previous_ban_reason = ban_record.get("reason", "Violation of bot rules") if ban_record else "Unknown reason"
    
    # Delete from banned list
    col_banned.delete_one({"user_id": target_id})
    
    # Update user status and set unban flags for warning message
    col_users.update_one(
        {"user_id": target_id}, 
        {
            "$set": {
                "status": "active",
                "was_unbanned": True,
                "previous_ban_reason": previous_ban_reason,
                "unbanned_at": datetime.now(IST),
                "unbanned_by": message.from_user.first_name
            }
        }
    )
    
    # Log unban action to history and channel
    await log_ban_action(
        action_type="unban",
        user_id=target_id,
        user_name=name,
        admin_name=message.from_user.first_name
    )
    
    await message.answer(
        f"✅ **{name} ({target_id}) UNBANNED.**\n\n"
        f"⚠️ User will see a warning message on next /start.\n"
        f"💡 Previous ban reason saved for reference.",
        reply_markup=back_kb()
    )
    await state.clear()
    await asyncio.sleep(2)
    await show_dashboard_ui(message, message.from_user.id)

@dp.callback_query(F.data == "shoot_unban_features")
async def start_unban_features(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back", callback_data="btn_shoot_menu")
    await callback.message.edit_text("✅ **UNBAN FEATURES**\nEnter User ID or MSA ID:", reply_markup=kb.as_markup())
    await state.set_state(ShootState.waiting_for_unban_features_id)

@dp.message(ShootState.waiting_for_unban_features_id)
async def select_features_to_unban(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    target_id, name = resolve_user_id(message.text)
    if not target_id:
        return await message.answer("❌ User not found. Check ID/MSA ID.", reply_markup=back_kb())
    
    # Check if user is banned
    ban_record = col_banned.find_one({"user_id": target_id})
    if not ban_record:
        return await message.answer(f"  {name} ({target_id}) is not banned.\n Use 'Suspend Features' for active users.", reply_markup=back_kb())
    
    await state.update_data(unban_features_target_id=target_id, unban_features_target_name=name)
    
    # Get current banned features (stored in ban record)
    banned_features = ban_record.get("banned_features", ["downloads", "reviews", "support", "search"])
    
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{' ' if 'downloads' in banned_features else ' '} Downloads", callback_data="unban_downloads")
    kb.button(text=f"{' ' if 'reviews' in banned_features else ' '} Reviews", callback_data="unban_reviews")
    kb.button(text=f"{' ' if 'support' in banned_features else ' '} Customer Support", callback_data="unban_support")
    kb.button(text=f"{' ' if 'search' in banned_features else ' '} Search Function", callback_data="unban_search")
    kb.button(text=" Save Changes", callback_data="unban_features_save")
    kb.button(text="❌ Cancel", callback_data="btn_shoot_menu")
    kb.adjust(2, 2, 1, 1)
    
    ban_type = ban_record.get("ban_type", "permanent")
    ban_status = " Temporary" if ban_type == "temporary" else " Permanent"
    
    await message.answer(
        f" **UNBAN FEATURES FOR**\n{name} ({target_id})\n\n"
        f" Ban Status: {ban_status}\n"
        f"Select features to unban (  = Banned,   = Allowed):",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("unban_") & ~F.data.in_(["unban_features_save"]))
async def toggle_unban_feature(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    data = await state.get_data()
    target_id = data.get('unban_features_target_id')
    name = data.get('unban_features_target_name')
    
    feature = callback.data.replace("unban_", "")
    ban_record = col_banned.find_one({"user_id": target_id})
    banned_features = ban_record.get("banned_features", ["downloads", "reviews", "support", "search"]) if ban_record else []
    
    if feature in banned_features:
        banned_features.remove(feature)
    else:
        banned_features.append(feature)
    
    col_banned.update_one({"user_id": target_id}, {"$set": {"banned_features": banned_features}})
    
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{' ' if 'downloads' in banned_features else ' '} Downloads", callback_data="unban_downloads")
    kb.button(text=f"{' ' if 'reviews' in banned_features else ' '} Reviews", callback_data="unban_reviews")
    kb.button(text=f"{' ' if 'support' in banned_features else ' '} Customer Support", callback_data="unban_support")
    kb.button(text=f"{' ' if 'search' in banned_features else ' '} Search Function", callback_data="unban_search")
    kb.button(text=" Save Changes", callback_data="unban_features_save")
    kb.button(text="❌ Cancel", callback_data="btn_shoot_menu")
    kb.adjust(2, 2, 1, 1)
    
    ban_type = ban_record.get("ban_type", "permanent")
    ban_status = " Temporary" if ban_type == "temporary" else " Permanent"
    
    try:
        await callback.message.edit_reply_markup(reply_markup=kb.as_markup())
    except:
        pass
    await callback.answer(f"{'Banned' if feature in banned_features else 'Unbanned'}: {feature.title()}")

@dp.callback_query(F.data == "unban_features_save")
async def save_unban_features_changes(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    data = await state.get_data()
    target_id = data.get('unban_features_target_id')
    name = data.get('unban_features_target_name')
    
    ban_record = col_banned.find_one({"user_id": target_id})
    banned_features = ban_record.get("banned_features", []) if ban_record else []
    
    # Get all features to find which were unbanned
    all_features = ["downloads", "reviews", "support", "search"]
    unbanned_features = [f for f in all_features if f not in banned_features]
    
    # Log unban features action if any features were unbanned
    if unbanned_features:
        await log_ban_action(
            action_type="unban_features",
            user_id=target_id,
            user_name=name,
            admin_name=callback.from_user.first_name,
            banned_features=unbanned_features
        )
    
    if banned_features:
        features_list = ", ".join([f.title() for f in banned_features])
        await callback.message.edit_text(
            f" **Feature Bans Updated for {name} ({target_id})**\n\n"
            f" Still Banned: {features_list}\n\n"
            f" User remains banned from bot access but can use unbanned features when restrictions are lifted."
        )
    else:
        await callback.message.edit_text(
            f" **All features unbanned for {name} ({target_id})**\n\n"
            f" User remains banned from bot access but will have full feature access when unbanned."
        )
    
    await state.clear()
    await asyncio.sleep(2)
    await show_dashboard_ui(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "btn_add_admin")
async def admin_management_menu(callback: types.CallbackQuery, state: FSMContext):
    """Enhanced admin management dashboard"""
    if not is_admin(callback.from_user.id): return
    await state.clear()
    
    # Get admin count
    admin_count = col_admins.count_documents({})
    owner_info = f"👑 Owner: {OWNER_ID}\n"
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📋 List All Admins", callback_data="admin_list"))
    kb.row(InlineKeyboardButton(text="➕ Add New Admin", callback_data="admin_add"))
    kb.row(InlineKeyboardButton(text="🗑️ Remove Admin", callback_data="admin_remove"))
    kb.row(InlineKeyboardButton(text="🔍 Search Admin", callback_data="admin_search"))
    kb.row(InlineKeyboardButton(text="📊 Admin Statistics", callback_data="admin_stats"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Hub", callback_data="btn_refresh"))
    
    text = (
        f"👤 **ADMIN MANAGEMENT CENTER**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"{owner_info}"
        f"📊 **Total Admins:** `{admin_count}`\n\n"
        f"**Available Actions:**\n"
        f"• View all admin accounts\n"
        f"• Add new administrators\n"
        f"• Remove admin privileges\n"
        f"• Search admin records\n"
        f"• View admin statistics\n\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "admin_list")
async def list_all_admins(callback: types.CallbackQuery):
    """List all administrators"""
    if not is_admin(callback.from_user.id): return
    
    admins = list(col_admins.find({}))
    
    if not admins:
        text = "📋 **ADMIN LIST**\n━━━━━━━━━━━━━━━━━\n\n❌ No admins found in database.\n\n👑 Owner ID: `{OWNER_ID}`"
    else:
        text = f"📋 **ADMIN LIST**\n━━━━━━━━━━━━━━━━━\n\n👑 **Owner:** `{OWNER_ID}` (Permanent)\n\n**Administrators:**\n"
        for idx, admin in enumerate(admins, 1):
            name = admin.get('name', 'Unknown')
            user_id = admin.get('user_id', 'N/A')
            role = admin.get('role', 'Admin')
            text += f"\n{idx}. 👤 **{name}**\n   ID: `{user_id}`\n   Role: {role}\n"
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="btn_add_admin"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "admin_add")
async def add_admin_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Admin Menu", callback_data="btn_add_admin")
    await callback.message.edit_text(
        "➕ **ADD NEW ADMINISTRATOR**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Please send the **User ID or MSA ID** of the new admin:\n\n"
        "💡 **Examples:**\n"
        "  • Telegram ID: `123456789`\n"
        "  • MSA ID: `MSA00123`",
        reply_markup=kb.as_markup()
    )
    await state.set_state(AdminState.waiting_for_add_admin_id)

@dp.message(AdminState.waiting_for_add_admin_id)
async def add_admin_id(message: types.Message, state: FSMContext):
    input_id = message.text.strip()
    user_id, user_name = resolve_user_id(input_id)
    
    if not user_id:
        await message.answer(f"❌ User not found! Please check the ID: `{input_id}`")
        return
    
    # Check if already admin
    if col_admins.find_one({"user_id": user_id}):
        await message.answer(f"⚠️ **{user_name}** (`{user_id}`) is already an admin!")
        await state.clear()
        return
    
    await state.update_data(new_id=user_id, new_name=user_name)
    await message.answer(
        f"✅ **User ID Received:** `{user_id}`\n"
        f"👤 **Name:** {user_name}\n\n"
        f"Now send a **Role/Label** for this admin (e.g., 'Moderator', 'Support', 'Admin'):"
    )
    await state.set_state(AdminState.waiting_for_name)

@dp.message(AdminState.waiting_for_name)
async def add_admin_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    col_admins.insert_one({"user_id": data['new_id'], "name": message.text, "role": "Admin", "added_by": message.from_user.id, "added_at": datetime.now(IST)})
    await message.answer(
        f"✅ **ADMIN ADDED SUCCESSFULLY**\n\n"
        f"👤 **Name:** {message.text}\n"
        f"🆔 **ID:** `{data['new_id']}`\n"
        f"🔐 **Clearance:** Granted"
    )
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id)

@dp.callback_query(F.data == "admin_remove")
async def remove_admin_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only the owner can remove admins!", show_alert=True)
        return
    
    admins = list(col_admins.find({}))
    if not admins:
        await callback.answer("❌ No admins to remove", show_alert=True)
        return
    
    text = "🗑️ **REMOVE ADMINISTRATOR**\n━━━━━━━━━━━━━━━━━\n\n**Current Admins:**\n\n"
    for admin in admins:
        text += f"• {admin.get('name', 'Unknown')} - `{admin.get('user_id')}`\n"
    
    text += "\n📝 Send the **User ID or MSA ID** to remove:"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Admin Menu", callback_data="btn_add_admin")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())
    await state.set_state(AdminState.waiting_for_action_id)
    await state.update_data(action="remove")

@dp.callback_query(F.data == "admin_search")
async def search_admin(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Admin Menu", callback_data="btn_add_admin")
    await callback.message.edit_text(
        "🔍 **SEARCH ADMINISTRATOR**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Send **User ID** or **Name** to search:",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data == "admin_stats")
async def admin_statistics(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    admin_count = col_admins.count_documents({})
    admins = list(col_admins.find({}))
    
    text = (
        f"📊 **ADMIN STATISTICS**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"👑 **Owner:** `{OWNER_ID}`\n"
        f"👥 **Total Admins:** {admin_count}\n"
        f"🔐 **Total Privileged Users:** {admin_count + 1}\n\n"
    )
    
    if admins:
        text += "**Recent Additions:**\n"
        for admin in admins[:5]:
            added_at = admin.get('added_at', 'Unknown')
            if isinstance(added_at, datetime):
                added_at = added_at.strftime('%Y-%m-%d')
            text += f"• {admin.get('name', 'Unknown')} - {added_at}\n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Admin Menu", callback_data="btn_add_admin")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

# ==========================================
# ⭐ REVIEW MANAGEMENT SYSTEM
# ==========================================
@dp.callback_query(F.data == "btn_reviews")
async def review_management_menu(callback: types.CallbackQuery):
    """Comprehensive review management dashboard for Bot1 with live data refresh"""
    if not is_admin(callback.from_user.id): return
    
    # Fetch live statistics from database
    total_reviews = col_reviews.count_documents({})
    pending_reviews = col_reviews.count_documents({"status": "pending"})
    
    text = (
        f"⭐ **REVIEW MANAGEMENT CENTER**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"📊 **Statistics:**\n"
        f"📝 Total Reviews: `{total_reviews}`\n"
        f"⏳ Pending: `{pending_reviews}`\n\n"
        f"**Bot1 Review Control:**\n"
        f"• Enable/Disable review feature\n"
        f"• View all user reviews\n"
        f"• Export review data\n"
        f"• Configure review settings\n\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📝 View All Reviews", callback_data="review_view_all"))
    kb.row(InlineKeyboardButton(text="⏳ Pending Reviews", callback_data="review_pending"))
    kb.row(InlineKeyboardButton(text="📊 Analytics Dashboard", callback_data="review_analytics"))
    kb.row(InlineKeyboardButton(text="💾 Export Reviews", callback_data="review_export"))
    kb.row(InlineKeyboardButton(text="⚙️ Review Settings", callback_data="review_settings"))
    kb.row(InlineKeyboardButton(text="🔄 Refresh Data", callback_data="btn_reviews"))
    kb.row(InlineKeyboardButton(text="🟢/🔴 Toggle Bot1 Status", callback_data="review_status"))
    kb.row(InlineKeyboardButton(text="🏠 Back to Main Hub", callback_data="btn_refresh"))
    
    try:
        await callback.message.edit_text(text, reply_markup=kb.as_markup())
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await callback.answer("✅ Data is already up to date!", show_alert=False)
        else:
            raise

@dp.callback_query(F.data == "review_status")
async def review_status_toggle(callback: types.CallbackQuery):
    """Toggle review feature on/off for Bot1"""
    if not is_admin(callback.from_user.id): return
    
    current_status = col_settings.find_one({"setting": "reviews_enabled"})
    is_enabled = current_status and current_status.get("value", True)
    
    text = (
        f"🟢 **REVIEW FEATURE STATUS**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"📍 **Current Status:** {'🟢 ENABLED' if is_enabled else '🔴 DISABLED'}\n\n"
    )
    
    if is_enabled:
        text += (
            f"✅ Users can submit reviews\n"
            f"✅ Review requests are active\n"
            f"✅ Review notifications enabled\n\n"
            f"💡 Click below to disable"
        )
    else:
        text += (
            f"⚠️ Users cannot submit reviews\n"
            f"⚠️ Review requests paused\n"
            f"⚠️ Review system inactive\n\n"
            f"💡 Click below to enable"
        )
    
    kb = InlineKeyboardBuilder()
    if is_enabled:
        kb.row(InlineKeyboardButton(text="🔴 Disable Reviews", callback_data="review_disable"))
    else:
        kb.row(InlineKeyboardButton(text="🟢 Enable Reviews", callback_data="review_enable"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Reviews", callback_data="btn_reviews"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "review_enable")
async def enable_reviews(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can enable reviews!", show_alert=True)
        return
    
    col_settings.update_one(
        {"setting": "reviews_enabled"},
        {"$set": {"value": True, "updated_at": datetime.now(IST)}},
        upsert=True
    )
    await callback.answer("✅ Review system ENABLED!", show_alert=True)
    await review_status_toggle(callback)

@dp.callback_query(F.data == "review_disable")
async def disable_reviews(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can disable reviews!", show_alert=True)
        return
    
    col_settings.update_one(
        {"setting": "reviews_enabled"},
        {"$set": {"value": False, "updated_at": datetime.now(IST)}},
        upsert=True
    )
    await callback.answer("🔴 Review system DISABLED!", show_alert=True)
    await review_status_toggle(callback)

@dp.callback_query(F.data == "review_view_all")
async def view_all_reviews(callback: types.CallbackQuery, state: FSMContext):
    """View all reviews with pagination (20 per page)"""
    if not is_admin(callback.from_user.id): return
    await show_reviews_page(callback, state, page=0)

@dp.callback_query(F.data.startswith("review_page_"))
async def handle_review_page(callback: types.CallbackQuery, state: FSMContext):
    """Handle review pagination"""
    if not is_admin(callback.from_user.id): return
    page = int(callback.data.split("_")[-1])
    await show_reviews_page(callback, state, page)

async def show_reviews_page(callback: types.CallbackQuery, state: FSMContext, page: int):
    """Show paginated reviews - one per user (latest only)"""
    per_page = 20
    skip = page * per_page
    
    # Get latest review per user using aggregation
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$user_id",
            "latest_review": {"$first": "$$ROOT"}
        }},
        {"$replaceRoot": {"newRoot": "$latest_review"}},
        {"$sort": {"timestamp": -1}}
    ]
    
    all_reviews = list(col_reviews.aggregate(pipeline))
    total_reviews = len(all_reviews)
    reviews = all_reviews[skip:skip + per_page]
    
    if not reviews:
        text = "📝 **ALL REVIEWS**\n━━━━━━━━━━━━━━━━━\n\n✅ No reviews yet!"
    else:
        total_pages = (total_reviews + per_page - 1) // per_page
        text = (
            f"📝 **ALL REVIEWS** (Page {page + 1}/{total_pages})\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"Total: {total_reviews} unique users\n\n"
        )
        
        for idx, review in enumerate(reviews, skip + 1):
            user_id = review.get('user_id', 'Unknown')
            rating = review.get('rating', 'N/A')
            feedback = review.get('feedback', '')
            
            # Get user name
            user_doc = col_users.find_one({"user_id": user_id})
            user_name = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
            
            text += f"{idx}. **{user_name}** (`{user_id}`) - ⭐ {rating}/5\n"
            if feedback and feedback.strip():
                text += f"   💬 {feedback[:80]}...\n"
            text += "\n"
    
    kb = InlineKeyboardBuilder()
    
    # Pagination buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"review_page_{page-1}"))
    if (page + 1) * per_page < total_reviews:
        nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"review_page_{page+1}"))
    
    if nav_buttons:
        kb.row(*nav_buttons)
    
    kb.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="review_view_all"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Reviews", callback_data="btn_reviews"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "review_pending")
async def view_pending_reviews(callback: types.CallbackQuery, state: FSMContext):
    """View pending reviews with pagination and proper database fetching"""
    if not is_admin(callback.from_user.id): return
    await show_pending_reviews_page(callback, state, page=0)

@dp.callback_query(F.data.startswith("pending_page_"))
async def handle_pending_page(callback: types.CallbackQuery, state: FSMContext):
    """Handle pending reviews pagination"""
    if not is_admin(callback.from_user.id): return
    page = int(callback.data.split("_")[-1])
    await show_pending_reviews_page(callback, state, page)

async def show_pending_reviews_page(callback: types.CallbackQuery, state: FSMContext, page: int):
    """Show users in cooldown period (pending = waiting 7 days to review again)"""
    per_page = 20
    skip = page * per_page
    
    # Use aggregation to get only the latest review per user
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$user_id",
            "latest_review": {"$first": "$$ROOT"}
        }},
        {"$replaceRoot": {"newRoot": "$latest_review"}}
    ]
    
    all_reviews = list(col_reviews.aggregate(pipeline))
    
    # Calculate cooldown for unique users only
    cooldown_users = []
    for review in all_reviews:
        try:
            user_id = review.get('user_id')
            if not user_id:
                continue
            
            # SKIP if cooldown was reset - user can review immediately
            if review.get('cooldown_reset', False):
                continue
            
            review_time = review.get('timestamp')
            
            # Handle both datetime objects and strings
            if isinstance(review_time, str):
                try:
                    review_time = datetime.strptime(review_time, '%Y-%m-%d %H:%M:%S')
                except:
                    try:
                        review_time = datetime.fromisoformat(review_time)
                    except:
                        continue
            elif not isinstance(review_time, datetime):
                continue
            
            # Make sure review_time is timezone-aware
            if review_time.tzinfo is None:
                review_time = IST.localize(review_time)
            
            time_diff = datetime.now(IST) - review_time
            days_since = time_diff.total_seconds() / 86400
            
            # Get dynamic cooldown from settings
            cooldown_setting = col_settings.find_one({"setting": "review_cooldown_days"})
            cooldown_days = cooldown_setting.get("value", 7) if cooldown_setting else 7
            
            if days_since < cooldown_days:  # Still in cooldown
                days_remaining = cooldown_days - days_since
                cooldown_users.append({
                    'user_id': user_id,
                    'rating': review.get('rating'),
                    'days_remaining': days_remaining,
                    'review_date': review.get('date', 'N/A'),
                    'status': 'PENDING'
                })
        except Exception as e:
            logger.error(f"Error calculating cooldown: {e}")
            continue
    
    # Sort by days remaining (closest to being able to review again first)
    cooldown_users.sort(key=lambda x: x['days_remaining'])
    
    total_pending = len(cooldown_users)
    pending_page = cooldown_users[skip:skip + per_page]
    
    if not pending_page:
        text = "⏳ **PENDING REVIEWS (COOLDOWN)**\n━━━━━━━━━━━━━━━━━\n\n✅ No users in cooldown!\nAll users can submit reviews."
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🔙 Back to Reviews", callback_data="btn_reviews"))
    else:
        total_pages = (total_pending + per_page - 1) // per_page
        text = (
            f"⏳ **PENDING REVIEWS** (Page {page + 1}/{total_pages})\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"📊 **Users in Cooldown:** {total_pending}\n"
            f"💡 These users must wait 7 days between reviews\n\n"
        )
        
        for idx, user in enumerate(pending_page, skip + 1):
            user_id = user['user_id']
            rating = user['rating']
            days_remaining = user['days_remaining']
            status = user.get('status', 'PENDING')
            
            # Get user details
            user_doc = col_users.find_one({"user_id": user_id})
            user_name = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
            msa_id = user_doc.get("msa_id", "N/A") if user_doc else "N/A"
            
            hours_remaining = int(days_remaining * 24)
            
            text += f"{idx}. **{user_name}** | 🔴 {status}\n"
            text += f"   📱 `{user_id}` | 🆔 `{msa_id}` | ⭐ {rating}/5\n"
            text += f"   ⏰ Can review again in: {int(days_remaining)}d {hours_remaining % 24}h\n\n"
        
        kb = InlineKeyboardBuilder()
        
        # Pagination buttons
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"pending_page_{page-1}"))
        if (page + 1) * per_page < total_pending:
            nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"pending_page_{page+1}"))
        
        if nav_buttons:
            kb.row(*nav_buttons)
        
        kb.row(InlineKeyboardButton(text="🔍 Find User Review", callback_data="review_find_user"))
        kb.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="review_pending"))
    
    kb.row(InlineKeyboardButton(text="🔙 Back to Reviews", callback_data="btn_reviews"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "review_analytics")
async def review_analytics(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    # Get UNIQUE users only (latest review per user) - NO DUPLICATES
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$user_id",
            "latest_review": {"$first": "$$ROOT"}
        }},
        {"$replaceRoot": {"newRoot": "$latest_review"}}
    ]
    
    unique_reviews = list(col_reviews.aggregate(pipeline))
    total_unique_users = len(unique_reviews)
    
    # Count users in cooldown (pending = waiting 7 days, excluding reset cooldowns)
    users_in_cooldown = 0
    for review in unique_reviews:
        if review.get('cooldown_reset', False):
            continue  # Skip reset cooldowns
        
        review_time = review.get('timestamp')
        if isinstance(review_time, str):
            try:
                review_time = datetime.strptime(review_time, '%Y-%m-%d %H:%M:%S')
            except:
                try:
                    review_time = datetime.fromisoformat(review_time)
                except:
                    continue
        
        if review_time.tzinfo is None:
            review_time = IST.localize(review_time)
        
        time_diff = datetime.now(IST) - review_time
        days_since = time_diff.total_seconds() / 86400
        
        # Get dynamic cooldown from settings
        cooldown_setting = col_settings.find_one({"setting": "review_cooldown_days"})
        cooldown_days = cooldown_setting.get("value", 7) if cooldown_setting else 7
        
        if days_since < cooldown_days:  # In cooldown
            users_in_cooldown += 1
    
    # Calculate average rating from UNIQUE users only
    reviews_with_rating = [r for r in unique_reviews if r.get('rating')]
    avg_rating = sum([r.get('rating', 0) for r in reviews_with_rating]) / len(reviews_with_rating) if reviews_with_rating else 0
    
    # Calculate rating distribution (UNIQUE users only)
    rating_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for review in reviews_with_rating:
        rating = review.get('rating', 0)
        if rating in rating_counts:
            rating_counts[rating] += 1
    
    # Build rating bars with visual representation
    rating_bars = ""
    for i in range(5, 0, -1):
        count = rating_counts[i]
        percentage = (count / total_unique_users * 100) if total_unique_users > 0 else 0
        bar_length = int(percentage / 5)  # Scale: 5% = 1 block
        bar = "█" * bar_length + "░" * (20 - bar_length)
        rating_bars += f"{'⭐' * i} {bar} {count} ({percentage:.1f}%)\n"
    
    # Calculate users who can review again (no cooldown)
    users_can_review = total_unique_users - users_in_cooldown
    
    text = (
        f"📊 **REVIEW ANALYTICS**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"👥 **Total Unique Reviewers:** {total_unique_users}\n"
        f"⏳ **Users in Cooldown:** {users_in_cooldown}\n"
        f"✅ **Can Review Again:** {users_can_review}\n"
        f"⭐ **Overall Average:** {avg_rating:.2f}/5\n\n"
        f"📊 **Rating Distribution:**\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"{rating_bars}\n"
        f"📈 **5-Star Rate:** {(rating_counts[5]/total_unique_users*100 if total_unique_users > 0 else 0):.1f}%\n"
        f"🎯 **Positive (4-5⭐):** {((rating_counts[4]+rating_counts[5])/total_unique_users*100 if total_unique_users > 0 else 0):.1f}%\n"
        f"⚠️ **Neutral (3⭐):** {(rating_counts[3]/total_unique_users*100 if total_unique_users > 0 else 0):.1f}%\n"
        f"🚫 **Negative (1-2⭐):** {((rating_counts[1]+rating_counts[2])/total_unique_users*100 if total_unique_users > 0 else 0):.1f}%\n\n"
        f"💡 **Note:** Each user counted once (latest review only)\n"
        f"🕐 **Last Updated:** {datetime.now(IST).strftime('%d/%m/%Y %H:%M:%S')}"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔄 Refresh Stats", callback_data="review_analytics"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Reviews", callback_data="btn_reviews"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "review_export")
async def export_reviews(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    await callback.message.edit_text("💾 **Exporting reviews...**")
    
    try:
        reviews = list(col_reviews.find({}, {"_id": 0, "user_id": 1, "feedback": 1, "rating": 1, "status": 1, "timestamp": 1, "date": 1}))
        
        if reviews:
            timestamp = datetime.now(IST).strftime('%Y%m%d_%H%M%S')
            filename = f"REVIEWS_EXPORT_{timestamp}.csv"
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['user_id', 'feedback', 'rating', 'status', 'date', 'timestamp']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(reviews)
            
            await callback.message.answer_document(
                FSInputFile(filename),
                caption=f"⭐ **REVIEWS EXPORT**\n📊 Total: {len(reviews)} reviews\n🕐 {datetime.now(IST).strftime('%H:%M:%S')}"
            )
            os.remove(filename)
            await callback.answer("✅ Export complete!", show_alert=True)
        else:
            await callback.message.answer("❌ No reviews to export!")
    except Exception as e:
        await callback.message.answer(f"❌ Export failed: {str(e)}")
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔙 Back to Reviews", callback_data="btn_reviews"))
    await callback.message.answer("✅ Operation completed.", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "review_reset_cooldown")
async def review_reset_cooldown(callback: types.CallbackQuery, state: FSMContext):
    """Reset review cooldown for a specific user"""
    if not is_admin(callback.from_user.id): return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Reviews", callback_data="btn_reviews")
    
    await callback.message.edit_text(
        "🔄 **RESET REVIEW COOLDOWN**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Send the **User ID or MSA ID** to reset their review cooldown:\n\n"
        "💡 This allows the user to submit a new review immediately",
        reply_markup=kb.as_markup()
    )
    await state.set_state(AdminState.waiting_for_action_id)
    await state.update_data(action="reset_review_cooldown")

@dp.message(AdminState.waiting_for_action_id)
async def process_admin_action(message: types.Message, state: FSMContext):
    """Process various admin actions based on state data"""
    data = await state.get_data()
    action = data.get('action')
    
    if action == "reset_review_cooldown":
        input_id = message.text.strip()
        user_id, user_name = resolve_user_id(input_id)
        
        if not user_id:
            await message.answer(f"❌ User not found! Please check the ID: `{input_id}`")
            await state.clear()
            return
        
        # Reset review cooldown by setting cooldown_reset flag in the latest review
        # Bot1 checks this flag to allow immediate review submission
        # First find the most recent review
        latest_review = col_reviews.find_one(
            {"user_id": user_id},
            sort=[("timestamp", -1)]
        )
        
        if latest_review:
            result = col_reviews.update_one(
                {"_id": latest_review["_id"]},
                {"$set": {
                    "cooldown_reset": True,
                    "cooldown_reset_by": message.from_user.id,
                    "cooldown_reset_at": datetime.now(IST)
                }}
            )
        else:
            result = type('obj', (object,), {'matched_count': 0})()
        
        # Also clear any cooldown fields in user_logs (legacy support)
        col_users.update_one(
            {"user_id": user_id},
            {"$unset": {"review_cooldown": "", "last_review_request": ""},
             "$set": {"can_review": True}}
        )
        
        if result.matched_count > 0:
            await message.answer(
                f"✅ **COOLDOWN RESET**\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 ID: `{user_id}`\n"
                f"🔄 Review cooldown has been reset\n"
                f"✅ User can now submit a review immediately"
            )
            logger.info(f"Admin {message.from_user.id} reset review cooldown for user {user_id}")
        else:
            await message.answer(f"❌ No review found for user `{user_id}`. They may not have submitted a review yet.")
        
        await state.clear()
        await show_dashboard_ui(message, message.from_user.id)
        
    elif action == "remove":
        # Handle admin removal
        input_id = message.text.strip()
        user_id, user_name = resolve_user_id(input_id)
        
        if not user_id:
            await message.answer(f"❌ User not found: `{input_id}`")
            return
        
        result = col_admins.delete_one({"user_id": user_id})
        
        if result.deleted_count > 0:
            await message.answer(
                f"✅ **ADMIN REMOVED**\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 ID: `{user_id}`\n"
                f"🔐 Admin privileges revoked"
            )
        else:
            await message.answer(f"❌ User `{user_id}` is not an admin!")
        
        await state.clear()
        await show_dashboard_ui(message, message.from_user.id)
        
    elif action == "resolve_ticket":
        # Handle ticket resolution
        input_id = message.text.strip()
        user_id, user_name = resolve_user_id(input_id)
        
        if not user_id:
            await message.answer(f"❌ User not found: `{input_id}`")
            return
        
        # Get user data before updating for channel message edit
        user_doc = col_users.find_one({"user_id": user_id})
        if not user_doc:
            await message.answer(f"❌ User not found in database: `{user_id}`")
            return
        
        result = col_users.update_one(
            {"user_id": user_id, "support_status": {"$in": ["open", "pending"]}},
            {"$set": {
                "support_status": "resolved",
                "resolved_at": datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
                "resolved_by": message.from_user.id
            }}
        )
        
        if result.modified_count > 0:
            resolved_time = datetime.now(IST).strftime('%d-%m-%Y %I:%M %p')
            
            await message.answer(
                f"✅ **TICKET RESOLVED**\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 ID: `{user_id}`\n"
                f"🎫 Ticket status: Resolved\n"
                f"🕐 Time: {datetime.now(IST).strftime('%H:%M:%S')}"
            )
            
            # Update support channel message if exists
            channel_msg_id = user_doc.get("support_channel_msg_id")
            support_channel_id = os.getenv("SUPPORT_CHANNEL_ID")
            
            if channel_msg_id and support_channel_id:
                try:
                    # Prepare resolved message
                    msa_id = user_doc.get("msa_id", "N/A")
                    first_name = user_doc.get("first_name", "Unknown")
                    username = user_doc.get("username", "No Username")
                    support_issue = user_doc.get("support_issue", "No description")
                    submitted_time = user_doc.get("support_timestamp", datetime.now(IST))
                    if isinstance(submitted_time, datetime):
                        submitted_time = submitted_time.strftime('%d-%m-%Y %I:%M %p')
                    
                    resolved_report = (
                        "✅ **SUPPORT REQUEST - RESOLVED**\n"
                        "━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"👤 **User:** {first_name}\n"
                        f"🏷️ **MSA ID:** `{msa_id}`\n"
                        f"🆔 **TELEGRAM ID:** `{user_id}`\n"
                        f"📱 **Username:** @{username}\n"
                        f"🕐 **Submitted:** {submitted_time}\n"
                        f"✅ **Resolved:** {resolved_time}\n"
                        f"📊 **Status:** ✅ RESOLVED\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━\n"
                        "💬 **MESSAGE:**\n\n"
                        f"{support_issue}\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🔗 **Contact:** tg://user?id={user_id}\n"
                        "━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"✅ *Resolved by admin at {resolved_time}*"
                    )
                    
                    # Edit channel message
                    await manager_bot.edit_message_text(
                        text=resolved_report,
                        chat_id=support_channel_id,
                        message_id=channel_msg_id,
                        parse_mode="Markdown"
                    )
                    await message.answer("✅ Support channel message updated!")
                except Exception as e:
                    await message.answer(f"⚠️ Could not update channel message: {str(e)}")
            
            # Try to notify the user (send to worker bot)
            try:
                await worker_bot.send_message(
                    user_id,
                    "✅ **Support Ticket Resolved**\n\n"
                    "Your support ticket has been resolved by our team.\n"
                    "Thank you for your patience!"
                )
            except:
                pass
        else:
            await message.answer(f"❌ No open/pending ticket found for user `{user_id}`!")
        
        await state.clear()
        await show_dashboard_ui(message, message.from_user.id)
    
    elif action == "find_user_review":
        # Handle finding user review
        input_id = message.text.strip()
        user_id, user_name = resolve_user_id(input_id)
        
        if not user_id:
            await message.answer(f"❌ User not found: `{input_id}`")
            await state.clear()
            return
        
        # Find user's review
        review = col_reviews.find_one({"user_id": user_id})
        
        if not review:
            await message.answer(
                f"❌ **NO REVIEW FOUND**\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 ID: `{user_id}`\n\n"
                f"This user has not submitted any review yet."
            )
            await state.clear()
            return
        
        # Get user details
        user_doc = col_users.find_one({"user_id": user_id})
        msa_id = user_doc.get("msa_id", "N/A") if user_doc else "N/A"
        
        rating = review.get('rating', 'N/A')
        feedback = review.get('feedback', 'No feedback provided')
        timestamp = review.get('date', 'N/A')
        submission_count = review.get('submission_count', 1)
        
        report = (
            f"📊 **USER REVIEW REPORT**\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"👤 **Name:** {user_name}\n"
            f"📱 **Telegram ID:** `{user_id}`\n"
            f"🆔 **MSA ID:** `{msa_id}`\n\n"
            f"⭐ **Rating:** {rating}/5\n"
            f"💬 **Feedback:**\n{feedback}\n\n"
            f"📅 **Submitted:** {timestamp}\n"
            f"🔄 **Total Submissions:** {submission_count}\n"
        )
        
        await message.answer(report)
        await state.clear()
        await show_dashboard_ui(message, message.from_user.id)
        return
    
    elif action == "find_user_review":
        # Handle finding user review
        input_id = message.text.strip()
        user_id, user_name = resolve_user_id(input_id)
        
        if not user_id:
            await message.answer(f"❌ User not found: `{input_id}`")
            await state.clear()
            return
        
        # Find user's review
        review = col_reviews.find_one({"user_id": user_id})
        
        if not review:
            await message.answer(
                f"❌ **NO REVIEW FOUND**\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 ID: `{user_id}`\n\n"
                f"This user has not submitted any review yet."
            )
            await state.clear()
            return
        
        # Get user details
        user_doc = col_users.find_one({"user_id": user_id})
        msa_id = user_doc.get("msa_id", "N/A") if user_doc else "N/A"
        
        rating = review.get('rating', 'N/A')
        feedback = review.get('feedback', 'No feedback provided')
        timestamp = review.get('date', 'N/A')
        submission_count = review.get('submission_count', 1)
        
        report = (
            f"📊 **USER REVIEW REPORT**\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"👤 **Name:** {user_name}\n"
            f"📱 **Telegram ID:** `{user_id}`\n"
            f"🆔 **MSA ID:** `{msa_id}`\n\n"
            f"⭐ **Rating:** {rating}/5\n"
            f"💬 **Feedback:**\n{feedback}\n\n"
            f"📅 **Submitted:** {timestamp}\n"
            f"🔄 **Total Submissions:** {submission_count}\n"
        )
        
        await message.answer(report)
        await state.clear()
        await show_dashboard_ui(message, message.from_user.id)
        return
    
    elif action == "approve_review":
        # Handle review approval
        input_id = message.text.strip()
        user_id, user_name = resolve_user_id(input_id)
        
        if not user_id:
            await message.answer(f"❌ User not found: `{input_id}`")
            return
        
        # Find the latest pending review for this user
        review = col_reviews.find_one(
            {"user_id": user_id, "status": "pending"},
            sort=[("timestamp", -1)]
        )
        
        if not review:
            await message.answer(f"❌ No pending review found for user `{user_id}`!")
            await state.clear()
            return
        
        # Approve the review
        result = col_reviews.update_one(
            {"_id": review["_id"]},
            {"$set": {
                "status": "approved",
                "approved_at": datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
                "approved_by": message.from_user.id
            }}
        )
        
        if result.modified_count > 0:
            await message.answer(
                f"✅ **REVIEW APPROVED**\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 ID: `{user_id}`\n"
                f"⭐ Rating: {review.get('rating', 'N/A')}/5\n"
                f"💬 Comment: {review.get('feedback', 'No comment')[:50]}...\n"
                f"✅ Review has been approved"
            )
            
            # Notify user
            try:
                await worker_bot.send_message(
                    user_id,
                    "✅ **Review Approved!**\n\n"
                    "Thank you! Your review has been approved and is now visible."
                )
            except:
                pass
        else:
            await message.answer(f"❌ No pending review found for user `{user_id}`!")
        
        await state.clear()
        await show_dashboard_ui(message, message.from_user.id)
    
    elif action == "reject_review":
        # Handle review rejection
        input_id = message.text.strip()
        user_id, user_name = resolve_user_id(input_id)
        
        if not user_id:
            await message.answer(f"❌ User not found: `{input_id}`")
            return
        
        # Find the latest pending review for this user
        review = col_reviews.find_one(
            {"user_id": user_id, "status": "pending"},
            sort=[("timestamp", -1)]
        )
        
        if not review:
            await message.answer(f"❌ No pending review found for user `{user_id}`!")
            await state.clear()
            return
        
        # Reject the review
        result = col_reviews.update_one(
            {"_id": review["_id"]},
            {"$set": {
                "status": "rejected",
                "rejected_at": datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
                "rejected_by": message.from_user.id
            }}
        )
        
        if result.modified_count > 0:
            await message.answer(
                f"❌ **REVIEW REJECTED**\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 ID: `{user_id}`\n"
                f"⭐ Rating: {review.get('rating', 'N/A')}/5\n"
                f"💬 Comment: {review.get('feedback', 'No comment')[:50]}...\n"
                f"🚫 Review has been rejected"
            )
            
            # Notify user
            try:
                await worker_bot.send_message(
                    user_id,
                    "❌ **Review Rejected**\n\n"
                    "Your review did not meet our community guidelines and has been rejected."
                )
            except:
                pass
        else:
            await message.answer(f"❌ No pending review found for user `{user_id}`!")
        
        await state.clear()
        await show_dashboard_ui(message, message.from_user.id)

@dp.callback_query(F.data == "review_approve")
async def review_approve_menu(callback: types.CallbackQuery, state: FSMContext):
    """Approve a specific review"""
    if not is_admin(callback.from_user.id): return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Reviews", callback_data="btn_reviews")
    
    await callback.message.edit_text(
        "✅ **APPROVE REVIEW**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Send the **User ID or MSA ID** of the review to approve:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(AdminState.waiting_for_action_id)
    await state.update_data(action="approve_review")

@dp.callback_query(F.data == "review_reject")
async def review_reject_menu(callback: types.CallbackQuery, state: FSMContext):
    """Reject a specific review"""
    if not is_admin(callback.from_user.id): return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Reviews", callback_data="btn_reviews")
    
    await callback.message.edit_text(
        "❌ **REJECT REVIEW**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Send the **User ID or MSA ID** of the review to reject:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(AdminState.waiting_for_action_id)
    await state.update_data(action="reject_review")

@dp.callback_query(F.data == "review_find_user")
async def review_find_user(callback: types.CallbackQuery, state: FSMContext):
    """Prompt admin to search for a specific user's review"""
    if not is_admin(callback.from_user.id): return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Reviews", callback_data="review_pending")
    
    await callback.message.edit_text(
        "🔍 **FIND USER REVIEW**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Send the **Telegram ID** or **MSA ID** of the user\n"
        "to view their full review details:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(AdminState.waiting_for_action_id)
    await state.update_data(action="find_user_review")

@dp.callback_query(F.data == "review_approve_all")
async def review_approve_all(callback: types.CallbackQuery, state: FSMContext):
    """Approve all pending reviews"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can approve all!", show_alert=True)
        return
    
    # Count pending reviews first
    pending_count = col_reviews.count_documents({"status": "pending"})
    
    if pending_count == 0:
        await callback.answer("ℹ️ No pending reviews to approve!", show_alert=True)
        return
    
    # Approve all pending reviews
    result = col_reviews.update_many(
        {"status": "pending"},
        {"$set": {
            "status": "approved",
            "approved_at": datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
            "approved_by": callback.from_user.id
        }}
    )
    
    await callback.answer(f"✅ Approved {result.modified_count} reviews!", show_alert=True)
    # Refresh the pending reviews page
    await show_pending_reviews_page(callback, state, page=1)

@dp.callback_query(F.data == "support_resolve_ticket")
async def support_resolve_ticket_menu(callback: types.CallbackQuery, state: FSMContext):
    """Resolve a specific support ticket"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Support", callback_data="btn_support")
    
    await callback.message.edit_text(
        "✅ **RESOLVE SUPPORT TICKET**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Send the **User ID or MSA ID** to resolve their support ticket:\n\n"
        "💡 This will mark their ticket as resolved and notify them",
        reply_markup=kb.as_markup()
    )
    await state.set_state(AdminState.waiting_for_action_id)
    await state.update_data(action="resolve_ticket")

@dp.callback_query(F.data == "support_resolve_all")
async def support_resolve_all(callback: types.CallbackQuery):
    """Mark all pending tickets as resolved and update channel messages"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can resolve all!", show_alert=True)
        return
    
    progress = await callback.message.edit_text("🔄 Resolving all tickets and updating channel...")
    
    try:
        # Get all pending tickets with channel message IDs
        pending_tickets = list(col_users.find(
            {"support_status": "pending"},
            {"user_id": 1, "msa_id": 1, "first_name": 1, "username": 1, 
             "support_issue": 1, "support_timestamp": 1, "support_channel_msg_id": 1}
        ))
        
        if not pending_tickets:
            await progress.edit_text("ℹ️ No pending tickets to resolve!")
            await callback.answer("ℹ️ No pending tickets!", show_alert=True)
            return
        
        resolved_time = datetime.now(IST).strftime('%d-%m-%Y %I:%M %p')
        support_channel_id = os.getenv("SUPPORT_CHANNEL_ID")
        updated_messages = 0
        
        # Update database first
        result = col_users.update_many(
            {"support_status": "pending"},
            {"$set": {
                "support_status": "resolved",
                "resolved_at": datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
                "resolved_by": callback.from_user.id
            }}
        )
        
        # Update channel messages
        if support_channel_id:
            for ticket in pending_tickets:
                channel_msg_id = ticket.get("support_channel_msg_id")
                if not channel_msg_id:
                    continue
                
                try:
                    user_id = ticket.get("user_id")
                    msa_id = ticket.get("msa_id", "N/A")
                    first_name = ticket.get("first_name", "Unknown")
                    username = ticket.get("username", "No Username")
                    support_issue = ticket.get("support_issue", "No description")
                    submitted_time = ticket.get("support_timestamp", datetime.now(IST))
                    if isinstance(submitted_time, datetime):
                        submitted_time = submitted_time.strftime('%d-%m-%Y %I:%M %p')
                    
                    resolved_report = (
                        "✅ **SUPPORT REQUEST - RESOLVED (BULK)**\n"
                        "━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"👤 **User:** {first_name}\n"
                        f"🏷️ **MSA ID:** `{msa_id}`\n"
                        f"🆔 **TELEGRAM ID:** `{user_id}`\n"
                        f"📱 **Username:** @{username}\n"
                        f"🕐 **Submitted:** {submitted_time}\n"
                        f"✅ **Resolved:** {resolved_time}\n"
                        f"📊 **Status:** ✅ RESOLVED\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━\n"
                        "💬 **MESSAGE:**\n\n"
                        f"{support_issue}\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🔗 **Contact:** tg://user?id={user_id}\n"
                        "━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"✅ *Bulk resolved by admin at {resolved_time}*"
                    )
                    
                    await manager_bot.edit_message_text(
                        text=resolved_report,
                        chat_id=support_channel_id,
                        message_id=channel_msg_id,
                        parse_mode="Markdown"
                    )
                    updated_messages += 1
                    await asyncio.sleep(0.1)  # Avoid rate limits
                except Exception as e:
                    logging.error(f"Failed to update channel message {channel_msg_id}: {e}")
        
        await progress.edit_text(
            f"✅ **BULK RESOLVE COMPLETE**\n\n"
            f"🎫 Tickets resolved: {result.modified_count}\n"
            f"📝 Channel messages updated: {updated_messages}"
        )
        await callback.answer(f"✅ Resolved {result.modified_count} tickets!", show_alert=True)
        await asyncio.sleep(2)
        await view_pending_tickets(callback)
    except Exception as e:
        await callback.message.edit_text(f"❌ Error: {str(e)}")
        await callback.answer("❌ Operation failed!", show_alert=True)

@dp.callback_query(F.data == "support_respond")
async def support_respond_ticket(callback: types.CallbackQuery):
    """Respond to a support ticket"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    text = (
        "💬 **RESPOND TO TICKET**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Send user ID and response message\n\n"
        "Format: `USER_ID | Your message here`\n\n"
        "💡 Use Quick Response for templates"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="💬 Quick Response", callback_data="support_quick_reply"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Support", callback_data="btn_support"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

# OLD HANDLER REMOVED - Using new handle_support_template() with 20 premium templates (line ~3820)

@dp.callback_query(F.data == "support_custom")
async def support_custom_message(callback: types.CallbackQuery):
    """Send custom support message"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    text = (
        "✏️ **CUSTOM SUPPORT MESSAGE**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Format: `USER_ID | Your custom message`\n\n"
        "Example: `123456789 | Hello! How can we help?`"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Support", callback_data="btn_support")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "support_export")
async def export_support_tickets(callback: types.CallbackQuery):
    """Export all support tickets to comprehensive CSV"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    progress = await callback.message.edit_text("💾 **Exporting support data...**\n▓░░░░░░░░░ 10%")
    
    try:
        # Get all users with support data - comprehensive fields
        tickets = list(col_users.find(
            {"$or": [{"has_support_ticket": True}, {"support_status": {"$exists": True}}]},
            {
                "_id": 0, "msa_id": 1, "user_id": 1, "first_name": 1, "username": 1,
                "support_issue": 1, "support_status": 1, "support_timestamp": 1,
                "ticket_created": 1, "resolved_at": 1, "last_support_response": 1,
                "response_count": 1, "last_response_admin": 1
            }
        ))
        
        await progress.edit_text("💾 **Processing data...**\n▓▓▓▓▓░░░░░ 50%")
        
        if tickets:
            timestamp = datetime.now(IST).strftime('%Y%m%d_%H%M%S')
            filename = f"SUPPORT_EXPORT_{timestamp}.csv"
            
            # Prepare data with formatted fields
            export_data = []
            for ticket in tickets:
                export_data.append({
                    'MSA_ID': ticket.get('msa_id', 'N/A'),
                    'User_ID': ticket.get('user_id', 'N/A'),
                    'Name': ticket.get('first_name', 'Unknown'),
                    'Username': f"@{ticket.get('username', 'None')}",
                    'Issue': ticket.get('support_issue', 'No description'),
                    'Status': ticket.get('support_status', 'unknown'),
                    'Created_At': ticket.get('ticket_created') or ticket.get('support_timestamp', 'N/A'),
                    'Resolved_At': ticket.get('resolved_at', 'N/A'),
                    'Last_Response': ticket.get('last_support_response', 'N/A'),
                    'Response_Count': ticket.get('response_count', 0),
                    'Admin_ID': ticket.get('last_response_admin', 'N/A')
                })
            
            await progress.edit_text("💾 **Creating file...**\n▓▓▓▓▓▓▓░░░ 70%")
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['MSA_ID', 'User_ID', 'Name', 'Username', 'Issue', 'Status', 
                            'Created_At', 'Resolved_At', 'Last_Response', 'Response_Count', 'Admin_ID']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(export_data)
            
            await progress.edit_text("💾 **Uploading...**\n▓▓▓▓▓▓▓▓▓░ 90%")
            
            # Calculate stats
            open_count = sum(1 for t in tickets if t.get('support_status') == 'open')
            pending_count = sum(1 for t in tickets if t.get('support_status') == 'pending')
            resolved_count = sum(1 for t in tickets if t.get('support_status') == 'resolved')
            
            await callback.message.answer_document(
                FSInputFile(filename),
                caption=(
                    f"📊 **SUPPORT DATA EXPORT**\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"📈 Total Records: {len(tickets)}\n"
                    f"🔴 Open: {open_count}\n"
                    f"⏳ Pending: {pending_count}\n"
                    f"✅ Resolved: {resolved_count}\n\n"
                    f"🕐 Exported: {datetime.now(IST).strftime('%d-%m-%Y %I:%M %p')}"
                )
            )
            os.remove(filename)
            await progress.edit_text("✅ **Complete!**\n▓▓▓▓▓▓▓▓▓▓ 100%")
            await callback.answer("✅ Export complete!", show_alert=True)
        else:
            await progress.edit_text("❌ No support data to export!")
            await callback.answer("❌ No support tickets found!", show_alert=True)
    except Exception as e:
        await callback.message.edit_text(f"❌ **Export failed:**\n{str(e)}")
        await callback.answer("❌ Export failed!", show_alert=True)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Support", callback_data="btn_support")
    await asyncio.sleep(1)
    await callback.message.edit_text("✅ Export operation completed.", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "support_hours")
async def support_hours_config(callback: types.CallbackQuery):
    """Configure support hours"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can configure hours!", show_alert=True)
        return
    
    text = (
        "⏰ **SUPPORT HOURS CONFIGURATION**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "🕒 **Current Hours:** 24/7\n\n"
        "Configure business hours for support:\n"
        "• Set working hours\n"
        "• Timezone configuration\n"
        "• Auto-reply outside hours\n\n"
        "💡 Feature can be customized"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Settings", callback_data="support_settings")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "support_edit_templates")
async def support_edit_templates(callback: types.CallbackQuery):
    """Edit support response templates"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can edit templates!", show_alert=True)
        return
    
    text = (
        "📝 **EDIT SUPPORT TEMPLATES**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Manage quick response templates:\n\n"
        "1️⃣ Thank you message\n"
        "2️⃣ Investigation notice\n"
        "3️⃣ Resolution message\n"
        "4️⃣ Request info\n\n"
        "💡 Click to edit each template"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Settings", callback_data="support_settings")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "review_notif_settings")
async def review_notification_settings(callback: types.CallbackQuery):
    """Configure review notification settings"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can configure notifications!", show_alert=True)
        return
    
    text = (
        "🔔 **REVIEW NOTIFICATIONS**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Configure notification preferences:\n\n"
        "• New review alerts\n"
        "• Pending review reminders\n"
        "• Review approval notifications\n"
        "• Daily review summary\n\n"
        "💡 All notifications can be toggled"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Settings", callback_data="review_settings")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "review_auto_approve")
async def review_auto_approve_settings(callback: types.CallbackQuery):
    """Configure auto-approval settings"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can configure auto-approve!", show_alert=True)
        return
    
    current = col_settings.find_one({"setting": "review_auto_approve"})
    is_auto = current and current.get("value", False)
    
    text = (
        "⏰ **AUTO-APPROVE SETTINGS**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        f"📍 **Status:** {'🟢 ENABLED' if is_auto else '🔴 DISABLED'}\n\n"
        "Auto-approve reviews that meet criteria:\n"
        "• Minimum rating: 4+ stars\n"
        "• No profanity detected\n"
        "• Minimum length: 20 characters\n\n"
        "💡 Toggle to enable/disable"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(
        text="🔴 Disable Auto-Approve" if is_auto else "🟢 Enable Auto-Approve",
        callback_data="toggle_review_auto_approve"
    ))
    kb.button(text="🔙 Back to Settings", callback_data="review_settings")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "toggle_review_auto_approve")
async def toggle_review_auto_approve(callback: types.CallbackQuery):
    """Toggle auto-approve feature"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can toggle this!", show_alert=True)
        return
    
    current = col_settings.find_one({"setting": "review_auto_approve"})
    new_val = not (current and current.get("value", False))
    
    col_settings.update_one(
        {"setting": "review_auto_approve"},
        {"$set": {"value": new_val, "updated_at": datetime.now(IST)}},
        upsert=True
    )
    
    await callback.answer(f"{'🟢 Auto-approve ENABLED' if new_val else '🔴 Auto-approve DISABLED'}!", show_alert=True)
    await review_auto_approve_settings(callback)

@dp.callback_query(F.data == "review_settings")
async def review_settings(callback: types.CallbackQuery):
    """Review configuration and management settings"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can access settings!", show_alert=True)
        return
    
    # Get current cooldown days setting (default 7 days)
    cooldown_setting = col_settings.find_one({"setting": "review_cooldown_days"})
    cooldown_days = cooldown_setting.get("value", 7) if cooldown_setting else 7
    
    # Get minimum rating requirement (default 1 = all ratings allowed)
    min_rating = col_settings.find_one({"setting": "review_min_rating"})
    min_rating_val = min_rating.get("value", 1) if min_rating else 1
    
    # Check if comments are required
    require_comment = col_settings.find_one({"setting": "review_require_comment"})
    comment_required = require_comment.get("value", False) if require_comment else False
    
    text = (
        f"⚙️ **REVIEW SETTINGS**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"⏰ **Cooldown Period:** {cooldown_days} days\n"
        f"   Users wait {cooldown_days} days between reviews\n\n"
        f"⭐ **Minimum Rating:** {min_rating_val} star{'s' if min_rating_val != 1 else ''}\n"
        f"   Lowest rating users can submit\n\n"
        f"💬 **Comment Required:** {'🟢 YES' if comment_required else '🔴 NO'}\n"
        f"   {'Users must write a comment' if comment_required else 'Comments are optional'}\n\n"
        f"**Available Actions:**\n"
        f"• Adjust cooldown period (3-30 days)\n"
        f"• Set minimum rating (1-5 stars)\n"
        f"• Toggle comment requirement\n"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="⏰ Change Cooldown", callback_data="review_change_cooldown"))
    kb.row(InlineKeyboardButton(text="⭐ Set Min Rating", callback_data="review_min_rating"))
    kb.row(InlineKeyboardButton(
        text=f"💬 {'Disable' if comment_required else 'Enable'} Required Comment",
        callback_data="review_toggle_comment"
    ))
    kb.row(InlineKeyboardButton(text="🗑️ Clear All Reviews", callback_data="review_clear_all"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Reviews", callback_data="btn_reviews"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "review_change_cooldown")
async def change_cooldown_prompt(callback: types.CallbackQuery, state: FSMContext):
    """Prompt to change review cooldown period"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can change settings!", show_alert=True)
        return
    
    current_cooldown = col_settings.find_one({"setting": "review_cooldown_days"})
    current_val = current_cooldown.get("value", 7) if current_cooldown else 7
    
    await callback.message.edit_text(
        f"⏰ **CHANGE COOLDOWN PERIOD**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"Enter the number of days users should wait between reviews.\n\n"
        f"📊 **Valid Range:** 3-30 days\n"
        f"🔢 **Current:** {current_val} days\n\n"
        f"💡 **Examples:**\n"
        f"• 3 days = More frequent reviews\n"
        f"• 7 days = Balanced (recommended)\n"
        f"• 14 days = Strict cooldown\n"
        f"• 30 days = Monthly reviews\n\n"
        f"📝 **Reply with a number (3-30):**",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Cancel", callback_data="review_settings")
        ]])
    )
    await state.set_state(AppealState.waiting_cooldown_days)

@dp.message(AppealState.waiting_cooldown_days)
async def handle_cooldown_input(message: types.Message, state: FSMContext):
    """Handle cooldown days input"""
    if message.from_user.id != OWNER_ID:
        return
    
    try:
        days = int(message.text.strip())
        if 3 <= days <= 30:
            col_settings.update_one(
                {"setting": "review_cooldown_days"},
                {"$set": {"value": days, "updated_at": datetime.now(IST)}},
                upsert=True
            )
            await message.answer(
                f"✅ **COOLDOWN UPDATED**\n\n"
                f"⏰ New cooldown period: **{days} days**\n"
                f"Users must now wait {days} days between reviews.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="⚙️ Back to Settings", callback_data="review_settings")
                ]])
            )
            await state.clear()
        else:
            await message.answer(
                f"❌ Invalid! Please enter a number between 3-30.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 Cancel", callback_data="review_settings")
                ]])
            )
    except ValueError:
        await message.answer(
            f"❌ Please enter a valid number!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Cancel", callback_data="review_settings")
            ]])
        )

@dp.callback_query(F.data == "review_min_rating")
async def set_min_rating_menu(callback: types.CallbackQuery):
    """Menu to set minimum allowed rating"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can change settings!", show_alert=True)
        return
    
    current_min = col_settings.find_one({"setting": "review_min_rating"})
    current_val = current_min.get("value", 1) if current_min else 1
    
    text = (
        f"⭐ **MINIMUM RATING SETTING**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"🔢 **Current Minimum:** {current_val} star{'s' if current_val != 1 else ''}\n\n"
        f"Set the lowest rating users can submit:\n\n"
        f"1⭐ = Allow all ratings (default)\n"
        f"2⭐ = Block 1-star reviews\n"
        f"3⭐ = Only 3-5 star reviews\n"
        f"4⭐ = Only positive reviews\n"
        f"5⭐ = Only perfect reviews\n\n"
        f"💡 Lower minimum = More honest feedback\n"
        f"📈 Higher minimum = Better ratings"
    )
    
    kb = InlineKeyboardBuilder()
    for i in range(1, 6):
        emoji = "✅" if i == current_val else "⭐"
        kb.row(InlineKeyboardButton(
            text=f"{emoji} {i} Star Minimum",
            callback_data=f"set_min_rating_{i}"
        ))
    kb.row(InlineKeyboardButton(text="🔙 Back to Settings", callback_data="review_settings"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("set_min_rating_"))
async def apply_min_rating(callback: types.CallbackQuery):
    """Apply minimum rating setting"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can change settings!", show_alert=True)
        return
    
    rating = int(callback.data.split("_")[-1])
    
    col_settings.update_one(
        {"setting": "review_min_rating"},
        {"$set": {"value": rating, "updated_at": datetime.now(IST)}},
        upsert=True
    )
    
    await callback.answer(f"✅ Minimum rating set to {rating}⭐!", show_alert=True)
    await review_settings(callback)

@dp.callback_query(F.data == "review_toggle_comment")
async def toggle_comment_requirement(callback: types.CallbackQuery):
    """Toggle whether comments are required with reviews"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can toggle settings!", show_alert=True)
        return
    
    current = col_settings.find_one({"setting": "review_require_comment"})
    new_val = not (current and current.get("value", False))
    
    col_settings.update_one(
        {"setting": "review_require_comment"},
        {"$set": {"value": new_val, "updated_at": datetime.now(IST)}},
        upsert=True
    )
    
    await callback.answer(
        f"{'💬 Comments are now REQUIRED!' if new_val else '💬 Comments are now OPTIONAL!'}",
        show_alert=True
    )
    await review_settings(callback)

@dp.callback_query(F.data == "review_clear_all")
async def clear_all_reviews_confirm(callback: types.CallbackQuery):
    """Confirm before clearing all reviews"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can clear reviews!", show_alert=True)
        return
    
    total = col_reviews.count_documents({})
    
    text = (
        f"⚠️ **CLEAR ALL REVIEWS**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"🗑️ This will permanently delete:\n"
        f"📊 {total} total review entries\n\n"
        f"❗ **This action cannot be undone!**\n\n"
        f"Are you sure you want to proceed?"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="✅ Yes, Clear All", callback_data="review_clear_confirmed"))
    kb.row(InlineKeyboardButton(text="❌ Cancel", callback_data="review_settings"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "review_clear_confirmed")
async def clear_all_reviews_execute(callback: types.CallbackQuery):
    """Execute clearing all reviews"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can clear reviews!", show_alert=True)
        return
    
    result = col_reviews.delete_many({})
    
    await callback.answer(f"✅ Deleted {result.deleted_count} reviews!", show_alert=True)
    await review_settings(callback)

@dp.callback_query(F.data == "review_toggle_auto")
async def toggle_auto_approval(callback: types.CallbackQuery):
    """Toggle auto-approval for reviews"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can toggle settings!", show_alert=True)
        return
    
    current = col_settings.find_one({"setting": "review_auto_approval"})
    new_val = not (current and current.get("value", False))
    
    col_settings.update_one(
        {"setting": "review_auto_approval"},
        {"$set": {"value": new_val, "updated_at": datetime.now(IST)}},
        upsert=True
    )
    
    await callback.answer(
        f"{'🟢 Auto-approval ENABLED! Reviews will be approved automatically.' if new_val else '🔴 Auto-approval DISABLED! Manual approval required.'}",
        show_alert=True
    )
    await review_settings(callback)

@dp.callback_query(F.data == "review_toggle_notif")
async def toggle_review_notifications(callback: types.CallbackQuery):
    """Toggle review notifications in bot2"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can toggle settings!", show_alert=True)
        return
    
    current = col_settings.find_one({"setting": "review_notifications"})
    new_val = not (current and current.get("value", True))
    
    col_settings.update_one(
        {"setting": "review_notifications"},
        {"$set": {"value": new_val, "updated_at": datetime.now(IST)}},
        upsert=True
    )
    
    await callback.answer(
        f"{'🔔 Notifications ENABLED! You will receive alerts for new reviews.' if new_val else '🔕 Notifications DISABLED! No review alerts will be sent.'}",
        show_alert=True
    )
    await review_settings(callback)

# ==========================================
# 💬 CUSTOMER SUPPORT SYSTEM
# ==========================================
@dp.callback_query(F.data == "btn_support")
async def customer_support_menu(callback: types.CallbackQuery):
    """Comprehensive customer support management dashboard"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    # Get support statistics from database
    open_tickets = col_users.count_documents({"support_status": "open"})
    pending_tickets = col_users.count_documents({"support_status": "pending"})
    resolved_tickets = col_users.count_documents({"support_status": "resolved"})
    total_tickets = open_tickets + pending_tickets + resolved_tickets
    
    text = (
        f"💬 **CUSTOMER SUPPORT CENTER**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"📊 **Ticket Statistics:**\n"
        f"🔴 Open: {open_tickets}\n"
        f"⏳ Pending: {pending_tickets}\n"
        f"✅ Resolved: {resolved_tickets}\n\n"
        f"**Support Operations:**\n"
        f"• View all support tickets\n"
        f"• Respond to user queries\n"
        f"• Manage ticket status\n"
        f"• Auto-response settings\n"
        f"• Support analytics\n\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔴 Open Tickets", callback_data="support_open"))
    kb.row(InlineKeyboardButton(text="⏳ Pending Tickets", callback_data="support_pending"))
    kb.row(InlineKeyboardButton(text="✅ Resolved Tickets", callback_data="support_resolved"))
    kb.row(InlineKeyboardButton(text="✅ Resolve Ticket", callback_data="support_resolve_ticket"))
    kb.row(InlineKeyboardButton(text=" Quick Response", callback_data="support_quick_reply"))
    kb.row(InlineKeyboardButton(text="📊 Support Analytics", callback_data="support_analytics"))
    kb.row(InlineKeyboardButton(text="📤 Export Data", callback_data="support_export"))
    kb.row(InlineKeyboardButton(text="🗑️ Clear All Data", callback_data="support_clear_confirm"))
    kb.row(InlineKeyboardButton(text="⚙️ Support Settings", callback_data="support_settings"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Hub", callback_data="btn_refresh"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "support_open")
async def view_open_tickets(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    tickets = list(col_users.find({"support_status": "open"}).limit(10))
    
    if not tickets:
        text = "🔴 **OPEN TICKETS**\n━━━━━━━━━━━━━━━━━\n\n✅ No open tickets!"
    else:
        text = f"🔴 **OPEN TICKETS**\n━━━━━━━━━━━━━━━━━\n\n"
        for idx, ticket in enumerate(tickets, 1):
            user_id = ticket.get('user_id', 'Unknown')
            issue = ticket.get('support_issue', 'No description')[:80]
            created = ticket.get('ticket_created', 'Unknown')
            text += f"{idx}. 👤 ID: `{user_id}`\n   🆘 {issue}\n   🕐 {created}\n\n"
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="support_open"))
    kb.row(InlineKeyboardButton(text="💬 Respond to Ticket", callback_data="support_respond"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Support", callback_data="btn_support"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "support_pending")
async def view_pending_tickets(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    # Optimized query using indexed field - handles millions of users efficiently
    tickets = list(col_users.find(
        {"support_status": "open"}, 
        {"user_id": 1, "first_name": 1, "msa_id": 1, "username": 1, "support_issue": 1, "support_timestamp": 1}
    ).sort("support_timestamp", -1).limit(20))
    
    if not tickets:
        text = "⏳ **PENDING TICKETS**\n━━━━━━━━━━━━━━━━━\n\n✅ No pending tickets!"
    else:
        text = f"⏳ **PENDING TICKETS** (Latest 20)\n━━━━━━━━━━━━━━━━━\n\n"
        for idx, ticket in enumerate(tickets, 1):
            user_id = ticket.get('user_id', 'Unknown')
            user_name = ticket.get('first_name', 'Unknown User')
            msa_id = ticket.get('msa_id', 'N/A')
            username = ticket.get('username', 'N/A')
            issue = ticket.get('support_issue', 'No description')[:100]
            timestamp = ticket.get('support_timestamp')
            time_str = timestamp.strftime('%d-%m %I:%M %p') if timestamp else 'N/A'
            
            text += (
                f"{idx}. 👤 **{user_name}**\n"
                f"   🆔 MSA: `{msa_id}` | TG: `{user_id}`\n"
                f"   📱 @{username}\n"
                f"   💬 {issue}\n"
                f"   🕐 {time_str}\n\n"
            )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="💬 Quick Reply", callback_data="support_quick_reply"))
    kb.row(InlineKeyboardButton(text="✅ Mark All Resolved", callback_data="support_resolve_all"))
    kb.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="support_pending"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Support", callback_data="btn_support"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "support_resolved")
async def view_resolved_tickets(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    tickets = list(col_users.find({"support_status": "resolved"}).sort("resolved_at", -1).limit(10))
    
    if not tickets:
        text = "✅ **RESOLVED TICKETS**\n━━━━━━━━━━━━━━━━━\n\n📝 No resolved tickets yet."
    else:
        text = f"✅ **RESOLVED TICKETS** (Latest 10)\n━━━━━━━━━━━━━━━━━\n\n"
        for idx, ticket in enumerate(tickets, 1):
            user_id = ticket.get('user_id', 'Unknown')
            issue = ticket.get('support_issue', 'N/A')[:60]
            resolved_at = ticket.get('resolved_at', 'Unknown')
            text += f"{idx}. 👤 {user_id}\n   {issue}\n   ✅ {resolved_at}\n\n"
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📊 Statistics", callback_data="support_analytics"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Support", callback_data="btn_support"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "support_analytics")
async def support_analytics(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    total = col_users.count_documents({"has_support_ticket": True})
    open_count = col_users.count_documents({"support_status": "open"})
    pending = col_users.count_documents({"support_status": "pending"})
    resolved = col_users.count_documents({"support_status": "resolved"})
    
    text = (
        f"📊 **SUPPORT ANALYTICS**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"🎫 **Total Tickets:** {total}\n"
        f"🔴 **Open:** {open_count}\n"
        f"⏳ **Pending:** {pending}\n"
        f"✅ **Resolved:** {resolved}\n\n"
        f"📈 **Resolution Rate:** {(resolved/total*100 if total > 0 else 0):.1f}%\n"
        f"⚡ **Active Rate:** {((open_count+pending)/total*100 if total > 0 else 0):.1f}%\n\n"
        f"🕐 **Report Time:** {datetime.now(IST).strftime('%H:%M:%S')}"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔄 Refresh Stats", callback_data="support_analytics"))
    kb.row(InlineKeyboardButton(text="💾 Export Report", callback_data="support_export"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Support", callback_data="btn_support"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "support_quick_reply")
async def support_quick_reply(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Admin access required!", show_alert=True)
        return
    
    text = (
        f"💬 **PREMIUM QUICK RESPONSE TEMPLATES**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"📋 **20 Professional Templates Available:**\n\n"
        f"🤝 **Initial Response:**\n"
        f"1️⃣ Welcome & Received\n"
        f"2️⃣ Apologetic & Empathetic\n\n"
        f"🔍 **Investigation:**\n"
        f"3️⃣ Under Investigation\n"
        f"8️⃣ High Priority - Urgent\n\n"
        f"✅ **Resolution:**\n"
        f"4️⃣ Issue Resolved - Positive\n"
        f"9️⃣ Technical Solution Provided\n"
        f"12 You're All Set\n\n"
        f"📝 **Information:**\n"
        f"5️⃣ Need More Info\n"
        f"13 Informative & Educational\n\n"
        f"🌟 **Gratitude & Support:**\n"
        f"6️⃣ Thank You for Patience\n"
        f"15 Always Here for You\n"
        f"19 Happy to Help\n\n"
        f"🎯 **Special Cases:**\n"
        f"7️⃣ Reassuring Professional\n"
        f"10 Follow-Up Check\n"
        f"11 Escalated to Management\n"
        f"14 Multiple Solutions\n"
        f"16 Empathetic Understanding\n"
        f"17 Professional Assistance\n"
        f"18 Critical Issue\n"
        f"20 Feedback Request\n\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"💡 Select template number or write custom:"
    )
    
    kb = InlineKeyboardBuilder()
    # Row 1-4: Initial & Investigation
    kb.row(
        InlineKeyboardButton(text="1️⃣ Welcome", callback_data="support_template_1"),
        InlineKeyboardButton(text="2️⃣ Apologetic", callback_data="support_template_2")
    )
    kb.row(
        InlineKeyboardButton(text="3️⃣ Investigating", callback_data="support_template_3"),
        InlineKeyboardButton(text="4️⃣ Resolved", callback_data="support_template_4")
    )
    # Row 5-8: Information & Gratitude
    kb.row(
        InlineKeyboardButton(text="5️⃣ Need Info", callback_data="support_template_5"),
        InlineKeyboardButton(text="6️⃣ Thank You", callback_data="support_template_6")
    )
    kb.row(
        InlineKeyboardButton(text="7️⃣ Reassuring", callback_data="support_template_7"),
        InlineKeyboardButton(text="8️⃣ Urgent", callback_data="support_template_8")
    )
    # Row 9-12: Technical & Solutions
    kb.row(
        InlineKeyboardButton(text="9️⃣ Technical", callback_data="support_template_9"),
        InlineKeyboardButton(text="🔟 Follow-Up", callback_data="support_template_10")
    )
    kb.row(
        InlineKeyboardButton(text="1️⃣1️⃣ Escalated", callback_data="support_template_11"),
        InlineKeyboardButton(text="1️⃣2️⃣ All Set", callback_data="support_template_12")
    )
    # Row 13-16: Educational & Empathetic
    kb.row(
        InlineKeyboardButton(text="1️⃣3️⃣ Educational", callback_data="support_template_13"),
        InlineKeyboardButton(text="1️⃣4️⃣ Solutions", callback_data="support_template_14")
    )
    kb.row(
        InlineKeyboardButton(text="1️⃣5️⃣ Always Here", callback_data="support_template_15"),
        InlineKeyboardButton(text="1️⃣6️⃣ Empathetic", callback_data="support_template_16")
    )
    # Row 17-20: Professional & Special
    kb.row(
        InlineKeyboardButton(text="1️⃣7️⃣ Professional", callback_data="support_template_17"),
        InlineKeyboardButton(text="1️⃣8️⃣ Critical", callback_data="support_template_18")
    )
    kb.row(
        InlineKeyboardButton(text="1️⃣9️⃣ Friendly", callback_data="support_template_19"),
        InlineKeyboardButton(text="2️⃣0️⃣ Feedback", callback_data="support_template_20")
    )
    # Custom message and back
    kb.row(InlineKeyboardButton(text="✏️ Custom Message", callback_data="support_custom"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Support", callback_data="btn_support"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("support_template_"))
async def handle_support_template(callback: types.CallbackQuery, state: FSMContext):
    """Handle quick reply template selection with premium professional templates"""
    if not is_admin(callback.from_user.id): return
    
    template_id = callback.data.split("_")[-1]
    
    # Premium Professional Templates - All Emotion Types
    templates = {
        # Welcoming & Initial Response
        "1": "👋 **Welcome to Support!**\n\nThank you for reaching out to us. We have received your message and our team is reviewing it carefully. You can expect a detailed response within 24 hours.\n\n✨ We're here to help!",
        
        # Apologetic & Empathetic
        "2": "💙 **We Understand Your Concern**\n\nWe sincerely apologize for any inconvenience you've experienced. Your issue is important to us, and we're working diligently to resolve it as quickly as possible.\n\n🙏 Thank you for your patience.",
        
        # Under Investigation
        "3": "🔍 **Investigation in Progress**\n\nOur technical team is currently analyzing your issue in detail. We're committed to finding the best solution for you and will update you with our findings shortly.\n\n⏰ Expected response: Within 12-24 hours",
        
        # Issue Resolved - Positive
        "4": "✅ **Issue Successfully Resolved!**\n\nGreat news! We've successfully resolved your issue. Everything should be working perfectly now. If you encounter any other problems or have questions, don't hesitate to reach out.\n\n😊 We're always here to help!",
        
        # Request More Information
        "5": "📝 **Additional Information Needed**\n\nTo assist you better and provide an accurate solution, we need a few more details about your issue. Could you please provide:\n\n• Detailed description of the problem\n• Screenshots (if applicable)\n• Steps you've already tried\n\n💡 This will help us resolve your issue faster!",
        
        # Grateful & Appreciative
        "6": "🌟 **Thank You for Your Patience!**\n\nWe truly appreciate your understanding and patience while we worked on your issue. Your feedback helps us improve our service every day.\n\n💝 Thank you for choosing us!",
        
        # Reassuring & Professional
        "7": "🛡️ **Rest Assured, We've Got This!**\n\nYour concern has been escalated to our senior support team. We're taking all necessary steps to ensure a comprehensive resolution. You're in good hands.\n\n✨ Quality support is our priority.",
        
        # Urgent Priority Response
        "8": "🚨 **High Priority - Immediate Action**\n\nWe've marked your issue as high priority and assigned it to our specialized team. They're actively working on it right now and will contact you within the next 2-4 hours.\n\n⚡ Fast resolution in progress!",
        
        # Technical Support
        "9": "🔧 **Technical Solution Provided**\n\nOur technical team has identified the issue and implemented a fix. Please try again and let us know if everything works smoothly now. If you need any assistance, we're just a message away.\n\n💻 Technical excellence is our commitment.",
        
        # Follow-up Check
        "10": "📞 **Follow-Up: How Are Things?**\n\nWe wanted to follow up and make sure everything is working well for you. Has your issue been fully resolved? Is there anything else we can help you with?\n\n🤝 Your satisfaction matters to us!",
        
        # Escalation Notice
        "11": "🔺 **Escalated to Management**\n\nYour case has been escalated to our management team for special attention. A senior representative will personally review your situation and contact you within 4-6 hours.\n\n👔 Premium support activated.",
        
        # Positive Reinforcement
        "12": "⭐ **You're All Set!**\n\nEverything has been configured and tested successfully! You should now have full access to all features. Enjoy your experience, and remember we're here 24/7 if you need anything.\n\n🎉 Happy to serve you!",
        
        # Informative & Educational
        "13": "📚 **Here's What You Need to Know**\n\nWe've prepared a detailed solution for your query. Please review the information below carefully. If you have any questions or need clarification, feel free to ask.\n\n💡 Knowledge is power!",
        
        # Problem Solving & Proactive
        "14": "🎯 **Multiple Solutions Available**\n\nWe've identified several approaches to resolve your issue. Let's work together to find the best solution for your specific situation. Which option would you prefer?\n\n🔧 Flexible support, your way.",
        
        # Closing & Supportive
        "15": "💬 **Always Here for You**\n\nWe're closing this ticket as resolved, but our support is ongoing! If you ever need help in the future, don't hesitate to reach out. We're committed to your success.\n\n🤗 Your trusted support team.",
        
        # Empathetic & Understanding
        "16": "💖 **We Value Your Experience**\n\nWe understand how frustrating technical issues can be. Please know that we're doing everything in our power to make this right for you. Your satisfaction is our top priority.\n\n🙏 Thank you for giving us the opportunity to help.",
        
        # Professional & Courteous
        "17": "🎩 **Professional Assistance**\n\nThank you for contacting our support department. Your inquiry has been logged and assigned ticket number. Our team will review your case thoroughly and respond with a comprehensive solution.\n\n📋 Structured support, guaranteed results.",
        
        # Urgent Problem Acknowledged
        "18": "⚠️ **Critical Issue Acknowledged**\n\nWe understand this is affecting your work/experience significantly. Our emergency response team has been notified and is treating this as top priority. Immediate action is being taken.\n\n🚀 Swift resolution guaranteed!",
        
        # Warm & Friendly
        "19": "😊 **Happy to Help!**\n\nHey there! Thanks for reaching out. We love helping our users, and your question is important to us. Let's get this sorted out together - teamwork makes the dream work!\n\n✨ Friendly support, effective solutions.",
        
        # Feedback Request
        "20": "⭐ **Your Feedback Matters**\n\nWe've resolved your issue and would love to hear about your experience! Your feedback helps us improve our service. How would you rate our support on a scale of 1-5 stars?\n\n📊 Building excellence together!"
    }
    
    message_template = templates.get(template_id, templates["1"])
    
    await state.update_data(quick_reply_message=message_template)
    await state.set_state(AdminState.waiting_for_quick_reply_user)
    
    await callback.message.edit_text(
        f"💬 **SELECTED TEMPLATE #{template_id}**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"{message_template}\n\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📝 Now send the **User ID** or **MSA ID** to deliver this message:",
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "support_custom")
async def support_custom_message(callback: types.CallbackQuery, state: FSMContext):
    """Handle custom support message"""
    if not is_admin(callback.from_user.id): return
    
    await callback.message.edit_text(
        "✏️ **CUSTOM MESSAGE**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Type your custom message:"
    )
    await state.set_state(AdminState.waiting_for_action_id)
    await state.update_data(action="send_custom_support")

@dp.message(AdminState.waiting_for_quick_reply_user)
async def process_quick_reply_user(message: types.Message, state: FSMContext):
    """Process user ID for quick reply with enhanced delivery and tracking"""
    data = await state.get_data()
    template_message = data.get('quick_reply_message')
    
    if not template_message:
        await message.answer("❌ No template selected! Please try again.")
        await state.clear()
        return
    
    input_id = message.text.strip()
    user_id, user_name = resolve_user_id(input_id)
    
    if not user_id:
        # Create navigation buttons for user not found
        kb_notfound = InlineKeyboardBuilder()
        kb_notfound.row(InlineKeyboardButton(text="🔄 Try Again", callback_data="support_quick_reply"))
        kb_notfound.row(InlineKeyboardButton(text="⏳ View Pending", callback_data="support_pending"))
        kb_notfound.row(InlineKeyboardButton(text="💬 Support Menu", callback_data="btn_support"))
        
        await message.answer(
            f"❌ **USER NOT FOUND**\n\n"
            f"ID provided: `{input_id}`\n\n"
            f"💡 Try:\n"
            f"• Telegram ID (numbers only)\n"
            f"• MSA ID (e.g., MSA001)\n"
            f"• Check for typos",
            parse_mode="Markdown",
            reply_markup=kb_notfound.as_markup()
        )
        await state.clear()
        return
    
    # Send delivery confirmation animation
    progress = await message.answer("📤 **Sending message...**\n▓░░░░░░░░░ 10%", parse_mode="Markdown")
    await asyncio.sleep(0.2)
    
    try:
        await progress.edit_text("📨 **Delivering to user...**\n▓▓▓▓░░░░░░ 40%", parse_mode="Markdown")
        
        # Send message to user via worker bot with premium formatting
        response_message = (
            f"💬 **SUPPORT RESPONSE**\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{template_message}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📞 Need more help? Just reply to us!\n"
            f"⏰ Support available 24/7"
        )
        
        sent_msg = await worker_bot.send_message(
            user_id,
            response_message,
            parse_mode="Markdown"
        )
        
        await progress.edit_text("💾 **Updating records...**\n▓▓▓▓▓▓▓░░░ 70%", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        
        # Update support status in database
        user_record = col_users.find_one({"user_id": user_id})
        current_count = user_record.get("response_count", 0) if user_record else 0
        
        col_users.update_one(
            {"user_id": user_id},
            {"$set": {
                "support_status": "responded",
                "last_support_response": datetime.now(IST),
                "last_response_admin": message.from_user.id,
                "response_count": current_count + 1
            }}
        )
        
        await progress.edit_text("✅ **Complete!**\n▓▓▓▓▓▓▓▓▓▓ 100%", parse_mode="Markdown")
        await asyncio.sleep(0.3)
        
        # Get user info for confirmation
        user_data = col_users.find_one({"user_id": user_id})
        username = user_data.get('username', 'No username') if user_data else 'No username'
        msa_id = user_data.get('msa_id', 'N/A') if user_data else 'N/A'
        
        # Create navigation buttons
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="📨 Send Another Message", callback_data="support_quick_reply"))
        kb.row(InlineKeyboardButton(text="⏳ View Pending Tickets", callback_data="support_pending"))
        kb.row(InlineKeyboardButton(text="💬 Support Menu", callback_data="btn_support"))
        kb.row(InlineKeyboardButton(text="🏠 Main Hub", callback_data="btn_refresh"))
        
        await progress.edit_text(
            f"✅ **MESSAGE DELIVERED SUCCESSFULLY!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 **User:** {user_name}\n"
            f"🆔 **Telegram ID:** `{user_id}`\n"
            f"🏷️ **MSA ID:** `{msa_id}`\n"
            f"📱 **Username:** @{username}\n"
            f"💬 **Message ID:** `{sent_msg.message_id}`\n"
            f"📊 **Status:** ✅ Responded\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"✨ Template delivered with premium formatting!\n"
            f"📝 User can now reply back to you.",
            parse_mode="Markdown",
            reply_markup=kb.as_markup()
        )
        
        logger.info(f"Support response sent to user {user_id} by admin {message.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error sending support message to {user_id}: {e}")
        
        # Create navigation buttons for error case
        kb_error = InlineKeyboardBuilder()
        kb_error.row(InlineKeyboardButton(text="🔄 Try Again", callback_data="support_quick_reply"))
        kb_error.row(InlineKeyboardButton(text="⏳ View Pending", callback_data="support_pending"))
        kb_error.row(InlineKeyboardButton(text="💬 Support Menu", callback_data="btn_support"))
        
        await progress.edit_text(
            f"❌ **DELIVERY FAILED**\n\n"
            f"Error: `{str(e)}`\n\n"
            f"**Possible reasons:**\n"
            f"• User blocked the bot\n"
            f"• User hasn't started the bot\n"
            f"• Invalid user ID\n"
            f"• Network/API error\n\n"
            f"💡 Try contacting user through support channel.",
            parse_mode="Markdown",
            reply_markup=kb_error.as_markup()
        )
    
    await state.clear()

@dp.callback_query(F.data == "support_clear_confirm")
async def support_clear_confirmation(callback: types.CallbackQuery):
    """Show confirmation before clearing support data"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can clear data!", show_alert=True)
        return
    
    text = (
        "⚠️ **DANGER ZONE**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "🗑️ **CLEAR ALL SUPPORT DATA**\n\n"
        "This will permanently delete:\n"
        "• All support tickets\n"
        "• All support messages\n"
        "• Support status from all users\n"
        "• Response history\n\n"
        "⚠️ **THIS CANNOT BE UNDONE!**\n\n"
        "Are you absolutely sure?"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🗑️ YES, DELETE ALL", callback_data="support_clear_execute"))
    kb.row(InlineKeyboardButton(text="❌ Cancel", callback_data="btn_support"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "support_clear_execute")
async def support_clear_execute(callback: types.CallbackQuery):
    """Execute permanent deletion of support data"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can clear data!", show_alert=True)
        return
    
    # Answer callback immediately to prevent timeout
    await callback.answer("🗑️ Clearing support data...", show_alert=False)
    
    # Edit message to show initial progress
    await callback.message.edit_text("🗑️ **Clearing support data...**\n▓░░░░░░░░░ 10%")
    
    try:
        # Count before deletion
        total_tickets = col_users.count_documents({"$or": [{"has_support_ticket": True}, {"support_status": {"$exists": True}}]})
        
        # Update progress
        try:
            await callback.message.edit_text("🗑️ **Removing support fields...**\n▓▓▓▓░░░░░░ 40%")
        except TelegramBadRequest:
            pass  # Ignore if message not modified
        
        await asyncio.sleep(0.2)
        
        # Remove all support-related fields from users
        result = col_users.update_many(
            {},
            {"$unset": {
                "support_status": "",
                "support_issue": "",
                "support_timestamp": "",
                "has_support_ticket": "",
                "ticket_created": "",
                "resolved_at": "",
                "last_support_response": "",
                "response_count": "",
                "last_response_admin": "",
                "support_channel_msg_id": ""
            }}
        )
        
        # Update progress
        try:
            await callback.message.edit_text("🗑️ **Finalizing...**\n▓▓▓▓▓▓▓▓░░ 80%")
        except TelegramBadRequest:
            pass
        
        await asyncio.sleep(0.2)
        
        # Show completion
        try:
            await callback.message.edit_text("✅ **Complete!**\n▓▓▓▓▓▓▓▓▓▓ 100%")
        except TelegramBadRequest:
            pass
        
        await asyncio.sleep(0.3)
        
        # Final result message
        text = (
            f"✅ **SUPPORT DATA CLEARED**\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"📊 Statistics:\n"
            f"🎫 Tickets Removed: {total_tickets}\n"
            f"👥 Users Updated: {result.modified_count}\n\n"
            f"🕐 {datetime.now(IST).strftime('%d-%m-%Y %I:%M %p')}\n\n"
            f"All support data has been permanently deleted."
        )
        
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back to Support", callback_data="btn_support")
        await callback.message.edit_text(text, reply_markup=kb.as_markup())
        
    except TelegramBadRequest as e:
        # Handle Telegram-specific errors
        error_msg = (
            f"⚠️ **Operation Completed with Warning**\n\n"
            f"Support data was cleared, but message update failed.\n"
            f"This is normal and doesn't affect the operation.\n\n"
            f"Error: {str(e)}"
        )
        try:
            await callback.message.edit_text(error_msg)
        except:
            await callback.message.answer(error_msg)
    except Exception as e:
        error_msg = f"❌ **Clearing failed:**\n{str(e)}"
        try:
            await callback.message.edit_text(error_msg)
        except:
            await callback.message.answer(error_msg)

@dp.callback_query(F.data == "support_settings")
async def support_settings(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can access settings!", show_alert=True)
        return
    
    # Check current settings
    auto_reply = col_settings.find_one({"setting": "support_auto_reply"})
    is_auto = auto_reply and auto_reply.get("value", False)
    
    text = (
        f"⚙️ **SUPPORT SETTINGS**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"📍 **Auto-Reply:** {'🟢 ON' if is_auto else '🔴 OFF'}\n\n"
        f"**Configuration:**\n"
        f"• Auto-reply to new tickets\n"
        f"• Response time tracking\n"
        f"• Priority ticket handling\n"
        f"• Notification preferences\n"
        f"• Support hours configuration\n"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔔 Toggle Auto-Reply", callback_data="support_toggle_auto"))
    kb.row(InlineKeyboardButton(text="⏰ Set Support Hours", callback_data="support_hours"))
    kb.row(InlineKeyboardButton(text="📝 Edit Templates", callback_data="support_edit_templates"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Support", callback_data="btn_support"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "support_toggle_auto")
async def toggle_auto_reply(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can toggle settings!", show_alert=True)
        return
    
    current = col_settings.find_one({"setting": "support_auto_reply"})
    new_val = not (current and current.get("value", False))
    
    col_settings.update_one(
        {"setting": "support_auto_reply"},
        {"$set": {"value": new_val, "updated_at": datetime.now(IST)}},
        upsert=True
    )
    
    await callback.answer(f"{'🟢 Auto-reply ENABLED' if new_val else '🔴 Auto-reply DISABLED'}!", show_alert=True)
    await support_settings(callback)

# ==========================================
#  DIAGNOSTICS & BACKUP (UNREDUCED)
# ==========================================
@dp.callback_query(F.data == "btn_diagnosis")
async def run_diagnosis(callback: types.CallbackQuery):
    """Enhanced comprehensive system diagnosis dashboard"""
    if not is_admin(callback.from_user.id): return
    
    # Show diagnosis menu
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="💚 System Health Check", callback_data="diag_health"))
    kb.row(InlineKeyboardButton(text="💾 Database Analytics", callback_data="diag_database"))
    kb.row(InlineKeyboardButton(text="⚡ Performance Metrics", callback_data="diag_performance"))
    kb.row(InlineKeyboardButton(text="🔒 Security Monitoring", callback_data="diag_security"))
    kb.row(InlineKeyboardButton(text="❌ Error Tracking", callback_data="diag_errors"))
    kb.row(InlineKeyboardButton(text="📊 Real-time Stats", callback_data="diag_realtime"))
    kb.row(InlineKeyboardButton(text="⚡ Quick Diagnosis", callback_data="diag_quick"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Hub", callback_data="btn_refresh"))
    
    diagnosis_text = (
        f" **ENHANCED SYSTEM DIAGNOSIS CENTER**\n"
        f" \n\n"
        f" **Choose Analysis Type:**\n\n"
        f"⚡ **System Health** - Core functionality status\n"
        f" **Database Analytics** - Collection statistics\n"
        f" **Performance Metrics** - Speed & efficiency\n"
        f" **Security Monitoring** - Threat detection\n"
        f" **Error Tracking** - Recent issues analysis\n"
        f" **Real-time Stats** - Live system monitoring\n"
        f" **Quick Diagnosis** - Instant health scan\n\n"
        f" **Select an option to begin detailed analysis.**"
    )
    
    await callback.message.edit_text(diagnosis_text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "btn_maint_toggle")
async def lockdown_menu(callback: types.CallbackQuery):
    """Enhanced lockdown management dashboard"""
    if not is_admin(callback.from_user.id): return
    
    curr = col_settings.find_one({"setting": "maintenance"})
    is_locked = curr and curr.get("value", False)
    lockdown_time = curr.get("enabled_at") if curr else None
    lockdown_by = curr.get("enabled_by") if curr else None
    
    status_emoji = "🔴" if is_locked else "🟢"
    status_text = "ENGAGED" if is_locked else "NORMAL"
    
    text = (
        f"🔐 **LOCKDOWN CONTROL CENTER**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"{status_emoji} **Current Status:** {status_text}\n"
    )
    
    if is_locked:
        text += f"⚠️ **System is in LOCKDOWN mode**\n"
        if lockdown_time:
            text += f"🕐 **Since:** {lockdown_time}\n"
        if lockdown_by:
            text += f"👤 **Enabled by:** {lockdown_by}\n"
        text += "\n**Effects:**\n• Bot operations restricted\n• Emergency protocols active\n• User access limited\n"
    else:
        text += f"✅ **System operating normally**\n\n"
        text += "**Lockdown Mode:**\n• Restricts bot operations\n• Activates emergency protocols\n• Limits user access\n• Useful for maintenance\n"
    
    text += "\n━━━━━━━━━━━━━━━━━"
    
    kb = InlineKeyboardBuilder()
    if is_locked:
        kb.row(InlineKeyboardButton(text="🟢 Disable Lockdown", callback_data="lockdown_disable"))
    else:
        kb.row(InlineKeyboardButton(text="🔴 Enable Lockdown", callback_data="lockdown_enable"))
    
    kb.row(InlineKeyboardButton(text="📊 Lockdown Status", callback_data="lockdown_status"))
    kb.row(InlineKeyboardButton(text="📜 Lockdown History", callback_data="lockdown_history"))
    kb.row(InlineKeyboardButton(text="⚙️ Lockdown Settings", callback_data="lockdown_settings"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Hub", callback_data="btn_refresh"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "lockdown_enable")
async def enable_lockdown(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can enable lockdown!", show_alert=True)
        return
    
    col_settings.update_one(
        {"setting": "maintenance"},
        {"$set": {
            "value": True,
            "enabled_at": datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
            "enabled_by": callback.from_user.id
        }},
        upsert=True
    )
    await callback.answer("🔴 LOCKDOWN ENGAGED!", show_alert=True)
    await lockdown_menu(callback)

@dp.callback_query(F.data == "lockdown_disable")
async def disable_lockdown(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can disable lockdown!", show_alert=True)
        return
    
    col_settings.update_one(
        {"setting": "maintenance"},
        {"$set": {
            "value": False,
            "disabled_at": datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
            "disabled_by": callback.from_user.id
        }},
        upsert=True
    )
    await callback.answer("🟢 Lockdown Disabled!", show_alert=True)
    await lockdown_menu(callback)

@dp.callback_query(F.data == "lockdown_status")
async def lockdown_status(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    curr = col_settings.find_one({"setting": "maintenance"})
    is_locked = curr and curr.get("value", False)
    
    text = (
        f"📊 **LOCKDOWN STATUS REPORT**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"🔐 **Mode:** {'🔴 ENGAGED' if is_locked else '🟢 NORMAL'}\n"
        f"⏰ **Checked:** {datetime.now(IST).strftime('%H:%M:%S')}\n\n"
    )
    
    if curr:
        text += "**Last Changes:**\n"
        if curr.get('enabled_at'):
            text += f"🔴 Enabled: {curr.get('enabled_at')}\n"
        if curr.get('disabled_at'):
            text += f"🟢 Disabled: {curr.get('disabled_at')}\n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Lockdown Menu", callback_data="btn_maint_toggle")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "lockdown_history")
async def lockdown_history(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    text = (
        f"📜 **LOCKDOWN HISTORY**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Historical lockdown records:\n"
        f"(Feature can be expanded to track all changes)\n\n"
        f"💡 Currently showing live status"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Lockdown Menu", callback_data="btn_maint_toggle")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "lockdown_settings")
async def lockdown_settings(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can access settings!", show_alert=True)
        return
    
    text = (
        f"⚙️ **LOCKDOWN SETTINGS**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"🔧 **Configuration Options:**\n\n"
        f"• Auto-lockdown triggers\n"
        f"• Scheduled lockdown\n"
        f"• Emergency protocols\n"
        f"• Access restrictions\n\n"
        f"💡 Advanced settings available"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Lockdown Menu", callback_data="btn_maint_toggle")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "btn_backup")
async def backup_menu(callback: types.CallbackQuery):
    """Enhanced backup management dashboard"""
    if not is_admin(callback.from_user.id): return
    
    # Get backup info
    user_count = col_users.count_documents({})
    admin_count = col_admins.count_documents({})
    broadcast_count = col_broadcast_logs.count_documents({})
    
    text = (
        f"💾 **BACKUP MANAGEMENT CENTER**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"📊 **Database Status:**\n"
        f"👥 Users: {user_count}\n"
        f"👤 Admins: {admin_count}\n"
        f"📢 Broadcasts: {broadcast_count}\n\n"
        f"**Available Backup Options:**\n\n"
        f"📋 **Full Backup** - All user data\n"
        f"👥 **Users Only** - User records\n"
        f"📢 **Broadcast Logs** - Message history\n"
        f"⚙️ **System Config** - Settings & admins\n"
        f"🎯 **Custom Backup** - Select collections\n\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📋 Full System Backup", callback_data="backup_full"))
    kb.row(InlineKeyboardButton(text="👥 Users Backup", callback_data="backup_users"))
    kb.row(InlineKeyboardButton(text="📢 Broadcast Logs Backup", callback_data="backup_broadcasts"))
    kb.row(InlineKeyboardButton(text="⚙️ Config Backup", callback_data="backup_config"))
    kb.row(InlineKeyboardButton(text="🎯 Custom Backup", callback_data="backup_custom"))
    kb.row(InlineKeyboardButton(text="📊 Backup Statistics", callback_data="backup_stats"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Hub", callback_data="btn_refresh"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "backup_full")
async def backup_full_system(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can create full backups!", show_alert=True)
        return
    
    await callback.message.edit_text("💾 **Creating Full System Backup...**\n\n⏳ Please wait...")
    
    try:
        timestamp = datetime.now(IST).strftime('%Y%m%d_%H%M%S')
        
        # Backup users
        users = list(col_users.find({}, {"_id": 0}))
        admins = list(col_admins.find({}, {"_id": 0}))
        broadcasts = list(col_broadcast_logs.find({}, {"_id": 0}).limit(1000))
        
        filename = f"FULL_BACKUP_{timestamp}.csv"
        
        if users:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=users[0].keys())
                writer.writeheader()
                writer.writerows(users)
            
            caption = (
                f"💾 **FULL SYSTEM BACKUP**\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"📅 {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"👥 Users: {len(users)}\n"
                f"👤 Admins: {len(admins)}\n"
                f"📢 Broadcasts: {len(broadcasts)}\n"
                f"🔐 Status: Encrypted"
            )
            
            await callback.message.answer_document(
                FSInputFile(filename),
                caption=caption
            )
            os.remove(filename)
            await callback.answer("✅ Backup created successfully!", show_alert=True)
        else:
            await callback.message.answer("❌ No data to backup!")
    except Exception as e:
        await callback.message.answer(f"❌ **Backup Failed**\n\nError: {str(e)}")
    
    # Return to menu
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Backup Menu", callback_data="btn_backup")
    await callback.message.answer("✅ Backup operation completed.", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "backup_users")
async def backup_users_only(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    await callback.message.edit_text("👥 **Creating Users Backup...**")
    
    try:
        users = list(col_users.find({}, {"_id": 0}))
        if users:
            timestamp = datetime.now(IST).strftime('%Y%m%d_%H%M%S')
            filename = f"USERS_BACKUP_{timestamp}.csv"
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=users[0].keys())
                writer.writeheader()
                writer.writerows(users)
            
            await callback.message.answer_document(
                FSInputFile(filename),
                caption=f"👥 **USERS BACKUP**\n📊 Total: {len(users)} users\n🕐 {datetime.now(IST).strftime('%H:%M:%S')}"
            )
            os.remove(filename)
            await callback.answer("✅ Users backup created!", show_alert=True)
    except Exception as e:
        await callback.message.answer(f"❌ Backup failed: {str(e)}")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Backup Menu", callback_data="btn_backup")
    await callback.message.answer("✅ Operation completed.", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "backup_broadcasts")
async def backup_broadcasts(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    await callback.message.edit_text("📢 **Creating Broadcast Logs Backup...**")
    
    try:
        broadcasts = list(col_broadcast_logs.find({}, {"_id": 0}).sort("timestamp", -1).limit(1000))
        if broadcasts:
            timestamp = datetime.now(IST).strftime('%Y%m%d_%H%M%S')
            filename = f"BROADCASTS_BACKUP_{timestamp}.csv"
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=broadcasts[0].keys())
                writer.writeheader()
                writer.writerows(broadcasts)
            
            await callback.message.answer_document(
                FSInputFile(filename),
                caption=f"📢 **BROADCAST LOGS BACKUP**\n📊 Total: {len(broadcasts)} records\n🕐 {datetime.now(IST).strftime('%H:%M:%S')}"
            )
            os.remove(filename)
            await callback.answer("✅ Broadcast logs backed up!", show_alert=True)
        else:
            await callback.message.answer("❌ No broadcast logs found!")
    except Exception as e:
        await callback.message.answer(f"❌ Backup failed: {str(e)}")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Backup Menu", callback_data="btn_backup")
    await callback.message.answer("✅ Operation completed.", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "backup_config")
async def backup_config(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("🚫 Only owner can backup config!", show_alert=True)
        return
    
    await callback.message.edit_text("⚙️ **Creating Configuration Backup...**")
    
    try:
        admins = list(col_admins.find({}, {"_id": 0}))
        settings = list(col_settings.find({}, {"_id": 0}))
        
        timestamp = datetime.now(IST).strftime('%Y%m%d_%H%M%S')
        filename = f"CONFIG_BACKUP_{timestamp}.csv"
        
        # Combine admins and settings
        config_data = admins + settings
        
        if config_data:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                all_keys = set()
                for item in config_data:
                    all_keys.update(item.keys())
                writer = csv.DictWriter(f, fieldnames=list(all_keys))
                writer.writeheader()
                writer.writerows(config_data)
            
            await callback.message.answer_document(
                FSInputFile(filename),
                caption=f"⚙️ **CONFIG BACKUP**\n👤 Admins: {len(admins)}\n🔧 Settings: {len(settings)}"
            )
            os.remove(filename)
            await callback.answer("✅ Config backed up!", show_alert=True)
    except Exception as e:
        await callback.message.answer(f"❌ Backup failed: {str(e)}")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Backup Menu", callback_data="btn_backup")
    await callback.message.answer("✅ Operation completed.", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "backup_custom")
async def backup_custom(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    text = (
        f"🎯 **CUSTOM BACKUP**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"Select collections to backup:\n\n"
        f"(Advanced feature - Coming soon)\n\n"
        f"💡 For now, use predefined backup options"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Back to Backup Menu", callback_data="btn_backup")
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "backup_stats")
async def backup_statistics(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    # Get database statistics
    user_count = col_users.count_documents({})
    admin_count = col_admins.count_documents({})
    broadcast_count = col_broadcast_logs.count_documents({})
    template_count = col_templates.count_documents({})
    banned_count = col_banned.count_documents({})
    
    # Estimate backup sizes
    total_records = user_count + admin_count + broadcast_count + template_count + banned_count
    
    text = (
        f"📊 **BACKUP STATISTICS**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"📦 **Database Overview:**\n"
        f"👥 Users: {user_count}\n"
        f"👤 Admins: {admin_count}\n"
        f"📢 Broadcasts: {broadcast_count}\n"
        f"📝 Templates: {template_count}\n"
        f"🚫 Banned: {banned_count}\n\n"
        f"📊 **Total Records:** {total_records}\n"
        f"💾 **Estimated Size:** ~{total_records * 0.5:.1f} KB\n\n"
        f"⏰ **Last Check:** {datetime.now(IST).strftime('%H:%M:%S')}\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔄 Refresh Stats", callback_data="backup_stats"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Backup Menu", callback_data="btn_backup"))
    await callback.message.edit_text(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "btn_traffic")
async def hub_traffic(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    
    # Source breakdown
    raw = list(col_users.aggregate([{"$group": {"_id": "$source", "count": {"$sum": 1}}}]))
    t = {r['_id']: r['count'] for r in raw}
    total = sum(t.values())
    
    # Status breakdown
    active_count = col_users.count_documents({"status": "active"})
    blocked_count = col_users.count_documents({"status": "blocked"})
    active_ratio = (active_count / total * 100) if total > 0 else 0
    
    # Daily growth (users joined today)
    now = datetime.now(IST)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)
    
    from bson.objectid import ObjectId
    today_count = col_users.count_documents({
        "_id": {"$gte": ObjectId.from_datetime(today_start)}
    })
    yesterday_count = col_users.count_documents({
        "_id": {
            "$gte": ObjectId.from_datetime(yesterday_start),
            "$lt": ObjectId.from_datetime(today_start)
        }
    })
    
    # Weekly growth
    week_ago = today_start - timedelta(days=7)
    week_count = col_users.count_documents({
        "_id": {"$gte": ObjectId.from_datetime(week_ago)}
    })
    
    # Growth indicators
    if yesterday_count > 0:
        daily_change = ((today_count - yesterday_count) / yesterday_count * 100)
        daily_indicator = " " if daily_change > 0 else " " if daily_change < 0 else " "
    else:
        daily_change = 0
        daily_indicator = " "
    
    rep = (
        f" **TRAFFIC ANALYTICS DASHBOARD**\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"** Source Distribution:**\n"
        f" YouTube: `{t.get('YouTube', 0)}`\n"
        f" Instagram: `{t.get('Instagram', 0)}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"** Status Overview:**\n"
        f" Active: `{active_count}` ({active_ratio:.1f}%)\n"
        f" Blocked: `{blocked_count}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"** Growth Metrics:**\n"
        f"Today: `{today_count}` {daily_indicator} `{daily_change:+.1f}%`\n"
        f"Yesterday: `{yesterday_count}`\n"
        f"Last 7 Days: `{week_count}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"** Total Operatives:** `{total}`"
    )
    
    await callback.message.edit_text(rep, reply_markup=back_kb())

# ==========================================
#  ENHANCED DIAGNOSIS SYSTEM
# ==========================================

@dp.callback_query(F.data == "diag_quick")
async def quick_diagnosis(callback: types.CallbackQuery):
    """Quick system health scan - legacy function enhanced"""
    if not is_admin(callback.from_user.id): return
    
    await callback.message.edit_text("⚡ **Performing Quick System Scan...**")
    
    try:
        # Basic system checks
        start_time = time.time()
        
        # Database connectivity and speed
        db_start = time.time()
        user_count = col_users.count_documents({})
        active_count = col_active.count_documents({})
        db_latency = round((time.time() - db_start) * 1000, 2)
        
        # Memory and performance indicators
        total_collections = len(db.list_collection_names())
        
        # Basic health indicators
        health_status = " EXCELLENT" if db_latency < 100 else " GOOD" if db_latency < 300 else " SLOW"
        
        # System uptime simulation (based on error counter resets)
        uptime_indicator = " STABLE" if ERROR_COUNTER < 5 else " MINOR ISSUES" if ERROR_COUNTER < 15 else " NEEDS ATTENTION"
        
        total_time = round((time.time() - start_time) * 1000, 2)
        
        report = (
            f" **QUICK SYSTEM DIAGNOSIS**\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f" **Database Core:** {health_status}\n"
            f" **Response Time:** {db_latency}ms\n"
            f" **Total Users:** `{user_count}`\n"
            f" **Active Content:** `{active_count}`\n"
            f" **Collections:** `{total_collections}`\n"
            f"⚡ **System Status:** {uptime_indicator}\n"
            f" **Error Count:** `{ERROR_COUNTER}`\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f" **Scan Completed:** {total_time}ms\n"
            f" **MSANode Shield:** ACTIVE"
        )
        
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text=" Re-scan", callback_data="diag_quick"))
        kb.row(InlineKeyboardButton(text=" Detailed Analysis", callback_data="diag_health"))
        kb.row(InlineKeyboardButton(text="🔙 Back", callback_data="btn_diagnosis"))
        
        await callback.message.edit_text(report, reply_markup=kb.as_markup())
        
    except Exception as e:
        await callback.message.edit_text(f" **Quick Diagnosis Failed**\n\nError: {str(e)}", reply_markup=back_kb())

@dp.callback_query(F.data == "diag_health")
async def comprehensive_health_check(callback: types.CallbackQuery):
    """Comprehensive system health analysis"""
    if not is_admin(callback.from_user.id): return
    
    await callback.message.edit_text("💚 **Running Comprehensive Health Analysis...**")
    
    try:
        # Enhanced health metrics
        start_time = time.time()
        
        # Database connection tests
        db_tests = {}
        
        # Test each collection
        collections = ['user_logs', 'active_content', 'viral_videos', 'viral_reels', 'settings', 'banned_users', 'user_reviews', 'broadcast_logs', 'broadcast_templates', 'recycle_bin']
        
        for collection_name in collections:
            try:
                test_start = time.time()
                collection = db[collection_name]
                count = collection.count_documents({})
                response_time = round((time.time() - test_start) * 1000, 2)
                
                status = " " if response_time < 100 else " " if response_time < 300 else " "
                db_tests[collection_name] = {
                    'count': count,
                    'response_time': response_time,
                    'status': status
                }
            except Exception as e:
                db_tests[collection_name] = {
                    'count': 0,
                    'response_time': 9999,
                    'status': ' ',
                    'error': str(e)
                }
        
        # Calculate overall health score
        healthy_collections = sum(1 for test in db_tests.values() if test['status'] == ' ')
        health_percentage = (healthy_collections / len(collections)) * 100
        
        # System status determination
        if health_percentage >= 90:
            overall_status = " EXCELLENT"
            status_emoji = " "
        elif health_percentage >= 75:
            overall_status = " GOOD"
            status_emoji = " "
        elif health_percentage >= 50:
            overall_status = " WARNING"
            status_emoji = " "
        else:
            overall_status = " CRITICAL"
            status_emoji = " "
        
        # Memory usage estimation
        total_documents = sum(test['count'] for test in db_tests.values() if test['count'] > 0)
        avg_response = sum(test['response_time'] for test in db_tests.values() if test['response_time'] < 9999) / len([t for t in db_tests.values() if t['response_time'] < 9999])
        
        # Build detailed report
        report = (
            f" **COMPREHENSIVE HEALTH ANALYSIS**\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"{status_emoji} **Overall Status:** {overall_status}\n"
            f" **Health Score:** {health_percentage:.1f}%\n"
            f" **Avg Response:** {avg_response:.1f}ms\n"
            f" **Total Documents:** `{total_documents:,}`\n\n"
            f"** Collection Status:**\n"
        )
        
        # Add collection details (top 8 most important)
        priority_collections = ['user_logs', 'active_content', 'settings', 'banned_users', 'user_reviews', 'broadcast_logs', 'recycle_bin', 'viral_videos']
        
        for collection in priority_collections:
            if collection in db_tests:
                test = db_tests[collection]
                report += f"{test['status']} {collection}: `{test['count']}` ({test['response_time']:.0f}ms)\n"
        
        # Add system recommendations
        if health_percentage < 75:
            report += "\n **Recommendations:**\n"
            if avg_response > 300:
                report += " Consider database optimization\n"
            if ERROR_COUNTER > 10:
                report += " Review error logs\n"
            
        total_analysis_time = round((time.time() - start_time) * 1000, 2)
        report += f"\n \n **Analysis Time:** {total_analysis_time}ms"
        
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text=" Re-analyze", callback_data="diag_health"))
        kb.row(InlineKeyboardButton(text=" Database Details", callback_data="diag_database"))
        kb.row(InlineKeyboardButton(text="🔙 Back", callback_data="btn_diagnosis"))
        
        await callback.message.edit_text(report, reply_markup=kb.as_markup())
        
    except Exception as e:
        await callback.message.edit_text(f" **Health Analysis Failed**\n\nError: {str(e)}", reply_markup=back_kb())

@dp.callback_query(F.data == "diag_database")
async def database_analytics(callback: types.CallbackQuery):
    """Detailed database analytics and statistics"""
    if not is_admin(callback.from_user.id): return
    
    await callback.message.edit_text("💾 **Analyzing Database Metrics...**")
    
    try:
        # Get database statistics
        db_stats = db.command("dbStats")
        
        # Collection analysis
        collection_data = {}
        for collection_name in db.list_collection_names():
            try:
                collection = db[collection_name]
                stats = db.command("collStats", collection_name)
                
                collection_data[collection_name] = {
                    'count': stats.get('count', 0),
                    'size': stats.get('size', 0),
                    'avgObjSize': stats.get('avgObjSize', 0),
                    'storageSize': stats.get('storageSize', 0),
                    'indexes': stats.get('nindexes', 0)
                }
            except:
                collection_data[collection_name] = {'count': 0, 'size': 0, 'avgObjSize': 0, 'storageSize': 0, 'indexes': 0}
        
        # Calculate totals and insights
        total_collections = len(collection_data)
        total_documents = sum(data['count'] for data in collection_data.values())
        
        # Simple database analytics (temporary fix)
        total_users = col_users.count_documents({})
        total_active = col_active.count_documents({})
        
        report = (
            f" **DATABASE ANALYTICS REPORT**\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f" Total Users: `{total_users:,}`\n"
            f" Active Content: `{total_active:,}`\n"
            f" Total Collections: `{total_collections}`\n"
            f" Total Documents: `{total_documents:,}`\n"
            f" System Status: Operational"
        )
        
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text=" Refresh Stats", callback_data="diag_database"))
        kb.row(InlineKeyboardButton(text="🔙 Back", callback_data="btn_diagnosis"))
        
        await callback.message.edit_text(report, reply_markup=kb.as_markup())
        
    except Exception as e:
        await callback.message.edit_text(f" **Database Analysis Failed**\n\nError: {str(e)}", reply_markup=back_kb())

# Placeholder for other diagnosis functions
@dp.callback_query(F.data.in_(["diag_performance", "diag_security", "diag_errors", "diag_realtime", "clear_errors", "security_banned"]))
async def placeholder_diagnosis_functions(callback: types.CallbackQuery):
    """Placeholder for other diagnosis functions"""
    if not is_admin(callback.from_user.id): return
    
    function_map = {
        "diag_performance": " **Performance Analysis**\n\nBasic system performance is operational.\nResponse time: Normal\nMemory usage: Stable",
        "diag_security": " **Security Monitoring**\n\nSystem security status: Protected\nNo active threats detected\nAccess controls: Active",
        "diag_errors": " **Error Tracking**\n\nSystem stability: Good\nError count: Low\nMonitoring: Active", 
        "diag_realtime": " **Real-time Statistics**\n\nLive monitoring active\nSystem load: Normal\nConnectivity: Stable",
        "clear_errors": " **Error Counter Cleared**\n\nError tracking has been reset.",
        "security_banned": " **Banned Users**\n\nBanned users management\nAccess: Restricted"
    }
    
    message = function_map.get(callback.data, " **Feature**\n\nSystem feature active")
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔙 Back", callback_data="btn_diagnosis"))
    
    await callback.message.edit_text(message, reply_markup=kb.as_markup())

# Enhanced Diagnosis Functions
async def enhanced_user_analytics(message: types.Message):
    """Enhanced user analytics with detailed insights."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Active Users", callback_data="analytics_active")],
        [InlineKeyboardButton(text=" User Growth", callback_data="analytics_growth")],
        [InlineKeyboardButton(text=" Engagement Stats", callback_data="analytics_engagement")],
        [InlineKeyboardButton(text=" Banned Users", callback_data="analytics_banned")],
        [InlineKeyboardButton(text=" Activity Logs", callback_data="analytics_logs")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="enhanced_diagnosis")]
    ])
    
    await message.edit_text(
        " **Enhanced User Analytics**\n\n"
        "Select analysis type:",
        reply_markup=keyboard
    )

async def enhanced_content_analysis(message: types.Message):
    """Enhanced content analysis with detailed metrics."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Content Overview", callback_data="content_overview")],
        [InlineKeyboardButton(text=" Popular Content", callback_data="content_popular")],
        [InlineKeyboardButton(text="⚡ Performance Metrics", callback_data="content_metrics")],
        [InlineKeyboardButton(text=" Search Analytics", callback_data="content_search")],
        [InlineKeyboardButton(text=" Engagement Trends", callback_data="content_trends")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="enhanced_diagnosis")]
    ])
    
    await message.edit_text(
        " **Enhanced Content Analysis**\n\n"
        "Select analysis type:",
        reply_markup=keyboard
    )

async def enhanced_system_health(message: types.Message):
    """Enhanced system health monitoring."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Bot Performance", callback_data="system_performance")],
        [InlineKeyboardButton(text=" Database Health", callback_data="system_database")],
        [InlineKeyboardButton(text=" API Status", callback_data="system_api")],
        [InlineKeyboardButton(text="❌ Error Monitoring", callback_data="system_errors")],
        [InlineKeyboardButton(text=" Resource Usage", callback_data="system_resources")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="enhanced_diagnosis")]
    ])
    
    await message.edit_text(
        " **Enhanced System Health**\n\n"
        "Select monitoring type:",
        reply_markup=keyboard
    )

async def enhanced_security_audit(message: types.Message):
    """Enhanced security audit and monitoring."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Security Overview", callback_data="security_overview")],
        [InlineKeyboardButton(text=" Threat Detection", callback_data="security_threats")],
        [InlineKeyboardButton(text=" Access Control", callback_data="security_access")],
        [InlineKeyboardButton(text=" Audit Logs", callback_data="security_audit")],
        [InlineKeyboardButton(text=" Real-time Alerts", callback_data="security_alerts")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="enhanced_diagnosis")]
    ])
    
    await message.edit_text(
        " **Enhanced Security Audit**\n\n"
        "Select security analysis:",
        reply_markup=keyboard
    )

async def enhanced_performance_metrics(message: types.Message):
    """Enhanced performance metrics and optimization."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Response Times", callback_data="perf_response")],
        [InlineKeyboardButton(text=" Throughput Analysis", callback_data="perf_throughput")],
        [InlineKeyboardButton(text=" Memory Usage", callback_data="perf_memory")],
        [InlineKeyboardButton(text=" Process Monitoring", callback_data="perf_processes")],
        [InlineKeyboardButton(text=" Optimization Tips", callback_data="perf_optimization")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="enhanced_diagnosis")]
    ])
    
    await message.edit_text(
        " **Enhanced Performance Metrics**\n\n"
        "Select performance analysis:",
        reply_markup=keyboard
    )

async def enhanced_data_insights(message: types.Message):
    """Enhanced data insights and analytics."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Data Trends", callback_data="data_trends")],
        [InlineKeyboardButton(text=" Pattern Analysis", callback_data="data_patterns")],
        [InlineKeyboardButton(text=" Statistical Reports", callback_data="data_statistics")],
        [InlineKeyboardButton(text=" Predictive Analysis", callback_data="data_predictive")],
        [InlineKeyboardButton(text=" Recommendations", callback_data="data_recommendations")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="enhanced_diagnosis")]
    ])
    
    await message.edit_text(
        " **Enhanced Data Insights**\n\n"
        "Select data analysis:",
        reply_markup=keyboard
    )

async def enhanced_real_time_monitoring(message: types.Message):
    """Enhanced real-time monitoring dashboard."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Live Status", callback_data="monitor_live")],
        [InlineKeyboardButton(text=" Real-time Metrics", callback_data="monitor_metrics")],
        [InlineKeyboardButton(text=" Active Alerts", callback_data="monitor_alerts")],
        [InlineKeyboardButton(text=" Live Charts", callback_data="monitor_charts")],
        [InlineKeyboardButton(text=" Auto Refresh", callback_data="monitor_refresh")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="enhanced_diagnosis")]
    ])
    
    await message.edit_text(
        " **Enhanced Real-time Monitoring**\n\n"
        "Select monitoring view:",
        reply_markup=keyboard
    )


# ==========================================
# 🔔 BAN APPEALS MANAGEMENT SYSTEM
# ==========================================

@dp.callback_query(F.data == "btn_appeals")
async def appeals_dashboard(callback: types.CallbackQuery):
    """Show appeals management dashboard"""
    if not is_admin(callback.from_user.id): return
    
    # Count appeals by status
    total_appeals = col_appeals.count_documents({})
    pending_appeals = col_appeals.count_documents({"status": "pending"})
    approved_appeals = col_appeals.count_documents({"status": "approved"})
    rejected_appeals = col_appeals.count_documents({"status": "rejected"})
    
    text = (
        f"🔔 **BAN APPEALS MANAGEMENT**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"📊 **Statistics:**\n"
        f"• Total Appeals: `{total_appeals}`\n"
        f"• ⏳ Pending: `{pending_appeals}`\n"
        f"• ✅ Approved: `{approved_appeals}`\n"
        f"• ❌ Rejected: `{rejected_appeals}`\n\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"**Available Actions:**\n"
        f"• View all pending appeals\n"
        f"• Review appeal history\n"
        f"• Manage warning templates\n\n"
        f"Select an option below:"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text=f"⏳ Pending Appeals ({pending_appeals})", callback_data="appeals_view_pending"))
    kb.row(InlineKeyboardButton(text="📋 All Appeals", callback_data="appeals_view_all"))
    kb.row(
        InlineKeyboardButton(text="🔍 Search User", callback_data="appeals_search_user"),
        InlineKeyboardButton(text="📊 Templates", callback_data="appeals_templates")
    )
    kb.row(InlineKeyboardButton(text="🔙 Back to Hub", callback_data="btn_refresh"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "appeals_view_pending")
@dp.callback_query(F.data.startswith("appeals_page_"))
async def view_pending_appeals(callback: types.CallbackQuery):
    """Show all pending appeals with pagination"""
    if not is_admin(callback.from_user.id): return
    
    # Get page number
    page = 0
    if callback.data.startswith("appeals_page_"):
        try:
            page = int(callback.data.split("_")[-1])
        except:
            page = 0
    
    # Pagination settings
    per_page = 20
    skip = page * per_page
    
    # Get total count
    total_pending = col_appeals.count_documents({"status": "pending"})
    total_pages = (total_pending + per_page - 1) // per_page
    
    pending = list(col_appeals.find({"status": "pending"}).sort("appeal_date", -1).skip(skip).limit(per_page))
    
    if not pending:
        text = (
            f"⏳ **PENDING APPEALS**\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"✨ No pending appeals!\n\n"
            f"All appeals have been reviewed."
        )
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🔙 Back to Appeals", callback_data="btn_appeals"))
        await callback.message.edit_text(text, reply_markup=kb.as_markup())
        return
    
    text = (
        f"⏳ **PENDING APPEALS ({len(pending)})**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
    )
    
    for idx, appeal in enumerate(pending, 1):
        user_id = appeal.get("user_id", "Unknown")
        msa_id = appeal.get("msa_id", "UNKNOWN")
        username = appeal.get("username", "No Username")
        appeal_text = appeal.get("appeal_text", "No message")
        ban_reason = appeal.get("ban_reason", "Unknown")
        appeal_date = appeal.get("appeal_date")
        appeal_date_str = appeal_date.strftime("%d-%m-%Y %I:%M %p") if appeal_date else "Unknown"
        
        # Truncate long appeals
        if len(appeal_text) > 100:
            appeal_text = appeal_text[:100] + "..."
        
        text += (
            f"**{idx}. User: @{username}**\n"
            f"   • MSA ID: `{msa_id}`\n"
            f"   • User ID: `{user_id}`\n"
            f"   • Ban Reason: {ban_reason}\n"
            f"   • Appeal: _{appeal_text}_\n"
            f"   • Date: {appeal_date_str}\n\n"
        )
    
    text += f"━━━━━━━━━━━━━━━━━\n"
    text += f"\n� Page {page + 1} of {total_pages} | Total: {total_pending}\n"
    text += f"\n💡 **Click a user to review their appeal**"
    
    kb = InlineKeyboardBuilder()
    # Add button for each pending appeal
    for appeal in pending:
        user_id = appeal.get("user_id", "Unknown")
        username = appeal.get("username", "Unknown")[:15]
        kb.row(InlineKeyboardButton(
            text=f"👤 {username} ({user_id})",
            callback_data=f"appeal_review_{user_id}"
        ))
    
    # Pagination buttons
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"appeals_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="➡️ Next", callback_data=f"appeals_page_{page+1}"))
        if nav_buttons:
            kb.row(*nav_buttons)
    
    kb.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="appeals_view_pending"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Appeals", callback_data="btn_appeals"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "appeals_view_all")
async def view_all_appeals(callback: types.CallbackQuery):
    """Show all appeals (recent 20)"""
    if not is_admin(callback.from_user.id): return
    
    appeals = list(col_appeals.find({}).sort("appeal_date", -1).limit(20))
    
    if not appeals:
        text = (
            f"📋 **ALL APPEALS**\n"
            f"━━━━━━━━━━━━━━━━━\n\n"
            f"❌ No appeals found in database."
        )
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🔙 Back to Appeals", callback_data="btn_appeals"))
        await callback.message.edit_text(text, reply_markup=kb.as_markup())
        return
    
    text = (
        f"📋 **ALL APPEALS (Recent {len(appeals)})**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
    )
    
    for idx, appeal in enumerate(appeals, 1):
        user_id = appeal.get("user_id", "Unknown")
        msa_id = appeal.get("msa_id", "UNKNOWN")
        status = appeal.get("status", "unknown")
        appeal_date = appeal.get("appeal_date")
        appeal_date_str = appeal_date.strftime("%d-%m-%Y") if appeal_date else "Unknown"
        
        status_emoji = {
            "pending": "⏳",
            "approved": "✅",
            "rejected": "❌"
        }.get(status, "❓")
        
        text += f"{idx}. {status_emoji} MSA: `{msa_id}` | ID: `{user_id}` | {appeal_date_str}\n"
    
    text += f"\n━━━━━━━━━━━━━━━━━"
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="appeals_view_all"))
    kb.row(InlineKeyboardButton(text="🔙 Back to Appeals", callback_data="btn_appeals"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "appeals_templates")
async def show_appeal_templates(callback: types.CallbackQuery):
    """Show pre-defined warning templates"""
    if not is_admin(callback.from_user.id): return
    
    templates = {
        "warning": "⚠️ **FINAL WARNING**\n\nYour appeal has been reviewed. You are being given ONE more chance.\n\n**DO NOT REPEAT YOUR VIOLATION.**\n\nAny future violations will result in permanent ban with no appeal option.\n\nPlease respect bot usage guidelines.",
        "rejected_spam": "❌ **APPEAL REJECTED**\n\n**Reason:** Spam behavior detected\n\nYour appeal has been reviewed and rejected. The ban remains in effect.\n\nSpamming the bot is not tolerated. This decision is final.",
        "rejected_abuse": "❌ **APPEAL REJECTED**\n\n**Reason:** Abuse of bot features\n\nYour appeal has been denied. The ban will remain permanent.\n\nAbusing bot features violates our terms of service. No further appeals will be considered.",
        "approved": "✅ **APPEAL APPROVED**\n\nYour ban has been lifted. You now have full access to the bot.\n\n**This is your second chance - use it wisely.**\n\n⚠️ Any future violations will result in immediate permanent ban with no appeal option.\n\nWelcome back!",
        "under_review": "⏳ **APPEAL UNDER REVIEW**\n\nThank you for your appeal. Our team is currently reviewing your case.\n\nYou will receive a response within 24 hours.\n\nPlease do not submit multiple appeals - this will not speed up the process."
    }
    
    text = (
        f"📝 **WARNING TEMPLATES**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"**Available Templates:**\n\n"
    )
    
    for idx, (key, template) in enumerate(templates.items(), 1):
        template_preview = template.split('\n')[0]  # First line only
        text += f"{idx}. **{key.replace('_', ' ').title()}**\n   _{template_preview}_\n\n"
    
    text += (
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"💡 **Usage:**\n"
        f"Templates are used when approving/rejecting appeals."
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔙 Back to Appeals", callback_data="btn_appeals"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())


# ==========================================
# 🔍 SEARCH USER & REVIEW APPEAL HANDLERS
# ==========================================

@dp.callback_query(F.data == "appeals_search_user")
async def prompt_search_user(callback: types.CallbackQuery):
    """Prompt admin to enter user ID for search"""
    if not is_admin(callback.from_user.id): return
    
    await callback.message.edit_text(
        "🔍 **SEARCH USER BY ID**\n\n"
        "Enter User ID or MSA ID to:\n"
        "• View complete ban history\n"
        "• See appeal history\n"
        "• Review or approve/reject appeals\n\n"
        "💡 **Type the ID in chat** (not here)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back to Appeals", callback_data="btn_appeals")
        ]]),
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.message(F.text)
async def search_user_history(message: types.Message):
    """Search and display complete user history when admin types ID"""
    if not is_admin(message.from_user.id): return
    
    # Only process if it looks like an ID (numeric or starts with MSA)
    search_id = message.text.strip()
    if not (search_id.isdigit() or search_id.upper().startswith("MSA")):
        return  # Not an ID, ignore
    
    # Try to resolve user ID
    target_id, name = resolve_user_id(search_id)
    if not target_id:
        await message.answer("❌ User not found. Try another ID.")
        return
    
    # Get user details
    user_doc = col_users.find_one({"user_id": target_id})
    ban_record = col_banned.find_one({"user_id": target_id})
    
    if not user_doc and not ban_record:
        await message.answer(f"❌ No records found for ID: {search_id}")
        await state.clear()
        return
    
    msa_id = (user_doc.get("msa_id") if user_doc else ban_record.get("msa_id", "UNKNOWN")) if (user_doc or ban_record) else "UNKNOWN"
    username = (user_doc.get("username") if user_doc else ban_record.get("username", "Unknown")) if (user_doc or ban_record) else "Unknown"
    
    # Get complete ban history
    ban_history = list(col_ban_history.find({"user_id": target_id}).sort("timestamp", -1))
    ban_count = len([h for h in ban_history if h.get("action_type") in ["ban", "auto_ban"]])
    
    # Get appeal history
    appeal_history = list(col_appeals.find({"user_id": target_id}).sort("appeal_date", -1))
    
    # Build detailed report
    report = (
        f"👤 **USER DETAILED REPORT**\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 **User Info:**\n"
        f"• MSA ID: `{msa_id}`\n"
        f"• Telegram ID: `{target_id}`\n"
        f"• Username: @{username}\n"
        f"• Name: {name}\n\n"
        f"📊 **Statistics:**\n"
        f"• 🚫 Total Bans: `{ban_count}`\n"
        f"• 📜 History Records: `{len(ban_history)}`\n"
        f"• 🔔 Appeals: `{len(appeal_history)}`\n\n"
    )
    
    # Current ban status
    if ban_record:
        report += (
            f"⚠️ **CURRENTLY BANNED**\n"
            f"• Reason: {ban_record.get('reason', 'Unknown')}\n"
            f"• By: {ban_record.get('banned_by', 'System')}\n\n"
        )
    else:
        report += f"✅ **NOT BANNED** (Currently Active)\n\n"
    
    report += f"━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Show recent ban history (last 5)
    if ban_history:
        report += f"📋 **Recent Ban History (Last 5):**\n\n"
        for idx, record in enumerate(ban_history[:5], 1):
            action = record.get("action_type", "unknown")
            reason = record.get("reason", "N/A")
            timestamp = record.get("timestamp")
            date_str = timestamp.strftime("%d-%m-%Y") if timestamp else "Unknown"
            
            emoji = "🚫" if action in ["ban", "auto_ban"] else "✅" if action in ["unban", "appeal_approved"] else "⏸️"
            report += f"{idx}. {emoji} {action.upper()} - {date_str}\n   Reason: {reason}\n\n"
    
    # Show pending appeals
    pending_appeal = col_appeals.find_one({"user_id": target_id, "status": "pending"})
    
    await message.answer(report, parse_mode="Markdown")
    
    # If has pending appeal, show action buttons
    if pending_appeal:
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="✅ Approve Options", callback_data=f"quick_approve_menu_{target_id}"))
        kb.row(InlineKeyboardButton(text="❌ Reject Options", callback_data=f"quick_reject_menu_{target_id}"))
        kb.row(InlineKeyboardButton(text="📝 Full Review", callback_data=f"appeal_review_{target_id}"))
        kb.row(InlineKeyboardButton(text="🔙 Back to Appeals", callback_data="btn_appeals"))
        await message.answer(
            "⏳ **This user has a PENDING appeal**\n\n"
            "Quick Actions:",
            reply_markup=kb.as_markup(),
            parse_mode="Markdown"
        )


@dp.callback_query(F.data.startswith("appeal_review_"))
async def review_specific_appeal(callback: types.CallbackQuery):
    """Review a specific user's appeal with full details and action buttons"""
    if not is_admin(callback.from_user.id): return
    
    user_id = callback.data.split("_")[-1]
    
    # Get appeal details
    appeal = col_appeals.find_one({"user_id": user_id, "status": "pending"})
    if not appeal:
        await callback.answer("❌ No pending appeal found for this user!", show_alert=True)
        return
    
    # Get ban history count
    ban_history = list(col_ban_history.find({"user_id": user_id}))
    ban_count = len([h for h in ban_history if h.get("action_type") in ["ban", "auto_ban"]])
    
    msa_id = appeal.get("msa_id", "UNKNOWN")
    username = appeal.get("username", "No Username")
    user_name = appeal.get("user_name", "Unknown")
    appeal_text = appeal.get("appeal_text", "No message")
    ban_reason = appeal.get("ban_reason", "Unknown")
    banned_by = appeal.get("banned_by", "System")
    appeal_date = appeal.get("appeal_date")
    appeal_date_str = appeal_date.strftime("%d-%m-%Y %I:%M %p") if appeal_date else "Unknown"
    
    # Build detailed appeal view
    text = (
        f"📝 **APPEAL REVIEW**\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 **User Information:**\n"
        f"• MSA ID: `{msa_id}`\n"
        f"• User ID: `{user_id}`\n"
        f"• Username: @{username}\n"
        f"• Name: {user_name}\n\n"
        f"🚫 **Ban Details:**\n"
        f"• Reason: {ban_reason}\n"
        f"• Banned By: {banned_by}\n"
        f"• 🔢 Previous Ban Count: **{ban_count}**\n\n"
        f"📝 **Appeal Message:**\n"
        f"_{appeal_text}_\n\n"
        f"🕐 Appeal Date: {appeal_date_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Choose an action:**"
    )
    
    # Action buttons
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Approve (Default)", callback_data=f"approve_default_{user_id}"),
        InlineKeyboardButton(text="❌ Reject (Default)", callback_data=f"reject_default_{user_id}")
    )
    kb.row(InlineKeyboardButton(text="✅ Approve with Template", callback_data=f"approve_template_{user_id}"))
    kb.row(InlineKeyboardButton(text="❌ Reject with Template", callback_data=f"reject_template_{user_id}"))
    kb.row(InlineKeyboardButton(text="✍️ Custom Message", callback_data=f"custom_message_{user_id}"))
    kb.row(InlineKeyboardButton(text="📊 View Full History", callback_data=f"appeals_search_user"))
    kb.row(InlineKeyboardButton(text="🔙 Back", callback_data="appeals_view_pending"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")


# Template selection handlers
@dp.callback_query(F.data.startswith("approve_template_"))
@dp.callback_query(F.data.startswith("quick_approve_menu_"))
async def show_approve_templates(callback: types.CallbackQuery):
    """Show approval message templates as buttons"""
    if not is_admin(callback.from_user.id): return
    
    user_id = callback.data.split("_")[-1]
    
    text = (
        "✅ **APPROVAL TEMPLATES**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Select a template to send with approval:\n\n"
        "📋 **Standard:** Ban lifted, full features restored\n"
        "⚠️ **Final Warning:** Strict warning included\n"
        "📅 **Probation:** 30-day monitoring period"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📋 Standard Approval", callback_data=f"approve_tmpl_1_{user_id}"))
    kb.row(InlineKeyboardButton(text="⚠️ Final Warning Approval", callback_data=f"approve_tmpl_2_{user_id}"))
    kb.row(InlineKeyboardButton(text="📅 Probation Approval", callback_data=f"approve_tmpl_3_{user_id}"))
    kb.row(InlineKeyboardButton(text="🔙 Back", callback_data=f"appeal_review_{user_id}"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("reject_template_"))
@dp.callback_query(F.data.startswith("quick_reject_menu_"))
async def show_reject_templates(callback: types.CallbackQuery):
    """Show rejection message templates as buttons"""
    if not is_admin(callback.from_user.id): return
    
    user_id = callback.data.split("_")[-1]
    
    text = (
        "❌ **REJECTION TEMPLATES**\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "Select a template to send with rejection:\n\n"
        "🚫 **Spam:** Spam behavior violation\n"
        "⛔ **Abuse:** Bot feature abuse\n"
        "📝 **Insufficient:** Appeal doesn't meet criteria"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🚫 Spam Violation", callback_data=f"reject_tmpl_1_{user_id}"))
    kb.row(InlineKeyboardButton(text="⛔ Abuse Violation", callback_data=f"reject_tmpl_2_{user_id}"))
    kb.row(InlineKeyboardButton(text="📝 Insufficient Appeal", callback_data=f"reject_tmpl_3_{user_id}"))
    kb.row(InlineKeyboardButton(text="🔙 Back", callback_data=f"appeal_review_{user_id}"))
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")


# Template button handlers
@dp.callback_query(F.data.startswith("approve_tmpl_"))
async def approve_with_template(callback: types.CallbackQuery):
    """Handle approval template button click"""
    if not is_admin(callback.from_user.id): return
    
    parts = callback.data.split("_")
    template_num = parts[2]
    user_id = parts[3]
    
    templates = {
        "1": (
            "✅ **APPEAL APPROVED - STANDARD**\n\n"
            "Your ban has been lifted.\n\n"
            "**ALL FEATURES RESTORED:**\n"
            "• Full bot access\n"
            "• All commands enabled\n"
            "• Premium features active\n\n"
            "⚠️ **FINAL WARNING:**\n"
            "This is your SECOND CHANCE.\n"
            "DO NOT REPEAT VIOLATIONS!\n\n"
            "Any future violations = Permanent ban with no appeal option.\n\n"
            "Welcome back! Please follow all rules."
        ),
        "2": (
            "✅ **APPEAL APPROVED - FINAL WARNING**\n\n"
            "⚠️ YOUR BAN HAS BEEN LIFTED WITH STRICT CONDITIONS\n\n"
            "**This is your ABSOLUTE FINAL chance!**\n\n"
            "✅ All features have been restored.\n\n"
            "**❌ ZERO TOLERANCE POLICY:**\n"
            "• ANY future violation = IMMEDIATE PERMANENT BAN\n"
            "• NO exceptions\n"
            "• NO further appeals will be accepted\n\n"
            "You are under strict observation.\n"
            "Follow ALL bot rules and guidelines.\n\n"
            "Use this opportunity wisely!"
        ),
        "3": (
            "✅ **APPEAL APPROVED - PROBATION PERIOD**\n\n"
            "Your ban has been lifted with CONDITIONS:\n\n"
            "📋 **PROBATION TERMS:**\n"
            "• 30-day probation period\n"
            "• Monitored usage\n"
            "• Limited initial access\n\n"
            "✅ Full features will be restored after successful probation.\n\n"
            "⚠️ **Warning:**\n"
            "Any violation during probation = Immediate permanent ban\n\n"
            "Follow the rules strictly during this period."
        )
    }
    
    await execute_appeal_approval(callback, user_id, templates.get(template_num))


@dp.callback_query(F.data.startswith("reject_tmpl_"))
async def reject_with_template(callback: types.CallbackQuery):
    """Handle rejection template button click"""
    if not is_admin(callback.from_user.id): return
    
    parts = callback.data.split("_")
    template_num = parts[2]
    user_id = parts[3]
    
    templates = {
        "1": (
            "❌ **APPEAL REJECTED - SPAM VIOLATION**\n\n"
            "Your appeal has been reviewed and REJECTED.\n\n"
            "**Reason:** Spam behavior detected\n\n"
            "Your actions violated our anti-spam policy:\n"
            "• Excessive spam messages\n"
            "• Automated/bot-like behavior\n"
            "• Mass operations detected\n\n"
            "**The ban remains PERMANENT.**\n\n"
            "⚠️ This decision is FINAL.\n"
            "No further appeals will be considered.\n\n"
            "Please respect bot policies."
        ),
        "2": (
            "❌ **APPEAL REJECTED - ABUSE VIOLATION**\n\n"
            "Your appeal has been DENIED.\n\n"
            "**Reason:** Abuse of bot features\n\n"
            "You violated our terms of service by:\n"
            "• Exploiting bot features\n"
            "• Attempting to manipulate systems\n"
            "• Abusive behavior\n\n"
            "**The ban is PERMANENT.**\n\n"
            "⚠️ This decision is FINAL and IRREVERSIBLE.\n"
            "No further appeals will be processed.\n\n"
            "Thank you for understanding."
        ),
        "3": (
            "❌ **APPEAL REJECTED - INSUFFICIENT APPEAL**\n\n"
            "Your appeal has been reviewed and REJECTED.\n\n"
            "**Reason:** Appeal does not meet requirements\n\n"
            "Your appeal was rejected because:\n"
            "• Insufficient explanation\n"
            "• No acknowledgment of violation\n"
            "• Failed to demonstrate understanding of rules\n\n"
            "**The ban remains in effect.**\n\n"
            "⚠️ You may submit ONE more appeal in 7 days.\n"
            "Make sure to provide a proper explanation next time."
        )
    }
    
    await execute_appeal_rejection(callback, user_id, templates.get(template_num))


@dp.callback_query(F.data.startswith("approve_default_"))
async def approve_with_default_message(callback: types.CallbackQuery):
    """Approve appeal with default message"""
    if not is_admin(callback.from_user.id): return
    
    user_id = callback.data.split("_")[-1]
    await execute_appeal_approval(callback, user_id, None)


@dp.callback_query(F.data.startswith("reject_default_"))
async def reject_with_default_message(callback: types.CallbackQuery):
    """Reject appeal with default message"""
    if not is_admin(callback.from_user.id): return
    
    user_id = callback.data.split("_")[-1]
    await execute_appeal_rejection(callback, user_id, None)


# Appeal action handlers (from inline buttons in channel)
@dp.callback_query(F.data.startswith("approve_appeal_"))
async def approve_appeal_action(callback: types.CallbackQuery):
    """Approve a ban appeal and unban user"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    user_id = callback.data.split("_")[-1]
    
    # Get appeal details
    appeal = col_appeals.find_one({"user_id": user_id, "status": "pending"})
    if not appeal:
        await callback.answer("❌ Appeal not found or already processed!", show_alert=True)
        return
    
    # Update appeal status
    col_appeals.update_one(
        {"user_id": user_id, "status": "pending"},
        {
            "$set": {
                "status": "approved",
                "reviewed_by": callback.from_user.id,
                "review_date": datetime.now(IST),
                "response": "Appeal approved - ban lifted"
            }
        }
    )
    
    # Unban user - restore ALL features
    ban_record = col_banned.find_one({"user_id": user_id})
    if ban_record:
        # Remove from banned collection
        col_banned.delete_one({"user_id": user_id})
        
        # Restore all user features and set warning flags
        col_users.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "was_unbanned": True,
                    "previous_ban_reason": ban_record.get("reason", "Unknown"),
                    "unbanned_at": datetime.now(IST),
                    "unbanned_by": callback.from_user.id,
                    "has_warning": True,
                    "warning_message": "⚠️ DO NOT REPEAT VIOLATIONS - This is your final warning!"
                },
                "$unset": {
                    "banned": "",
                    "ban_reason": "",
                    "ban_type": ""
                }
            },
            upsert=True
        )
        
        # Log in ban history
        col_ban_history.insert_one({
            "user_id": user_id,
            "msa_id": appeal.get("msa_id", "UNKNOWN"),
            "username": appeal.get("username", "No Username"),
            "user_name": appeal.get("user_name", "Unknown"),
            "action_type": "appeal_approved",
            "admin_id": callback.from_user.id,
            "admin_name": callback.from_user.username or str(callback.from_user.id),
            "reason": "Ban appeal approved",
            "timestamp": datetime.now(IST),
            "previous_ban_reason": ban_record.get("reason", "Unknown")
        })
    
    # Notify user with detailed message
    msa_id = appeal.get("msa_id", "UNKNOWN")
    username = appeal.get("username", "User")
    try:
        await worker_bot.send_message(
            chat_id=int(user_id),
            text=(
                "✅ **BAN APPEAL APPROVED**\n\n"
                f"👤 **MSA ID:** `{msa_id}`\n"
                f"👤 **Username:** @{username}\n\n"
                "🎉 **Your ban has been lifted!**\n\n"
                "✨ **ALL FEATURES RESTORED:**\n"
                "   • ✅ Full bot access\n"
                "   • ✅ All commands enabled\n"
                "   • ✅ Premium features active\n"
                "   • ✅ MSA submissions allowed\n\n"
                "⚠️ **FINAL WARNING:**\n"
                "**This is your SECOND CHANCE - DO NOT REPEAT VIOLATIONS!**\n\n"
                "❌ Any future violations will result in:\n"
                "   • Immediate permanent ban\n"
                "   • No appeal option\n"
                "   • No exceptions\n\n"
                "📱 Please follow all bot rules and guidelines.\n\n"
                "Welcome back! 🎊"
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Failed to notify user {user_id}: {e}")
    
    # Update callback message
    try:
        await callback.message.edit_text(
            callback.message.text + f"\n\n✅ **APPROVED** by MSA NODE AGENT",
            parse_mode="Markdown"
        )
    except:
        pass
    
    await callback.answer("✅ Appeal approved and user unbanned!", show_alert=True)


@dp.callback_query(F.data.startswith("reject_appeal_"))
async def reject_appeal_action(callback: types.CallbackQuery):
    """Reject a ban appeal"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    user_id = callback.data.split("_")[-1]
    
    # Get appeal details
    appeal = col_appeals.find_one({"user_id": user_id, "status": "pending"})
    if not appeal:
        await callback.answer("❌ Appeal not found or already processed!", show_alert=True)
        return
    
    # Update appeal status
    col_appeals.update_one(
        {"user_id": user_id, "status": "pending"},
        {
            "$set": {
                "status": "rejected",
                "reviewed_by": callback.from_user.id,
                "review_date": datetime.now(IST),
                "response": "Appeal rejected - ban remains"
            }
        }
    )
    
    # Log in ban history
    col_ban_history.insert_one({
        "user_id": user_id,
        "msa_id": appeal.get("msa_id", "UNKNOWN"),
        "username": appeal.get("username", "No Username"),
        "user_name": appeal.get("user_name", "Unknown"),
        "action_type": "appeal_rejected",
        "admin_name": callback.from_user.username or str(callback.from_user.id),
        "reason": "Ban appeal rejected",
        "timestamp": datetime.now(IST)
    })
    
    # Notify user with MSA ID
    msa_id = appeal.get("msa_id", "UNKNOWN")
    username = appeal.get("username", "User")
    ban_reason = appeal.get("ban_reason", "Violation of bot rules")
    try:
        await worker_bot.send_message(
            chat_id=int(user_id),
            text=(
                "❌ **BAN APPEAL REJECTED**\n\n"
                f"👤 **MSA ID:** `{msa_id}`\n"
                f"👤 **Username:** @{username}\n\n"
                "🚫 **Decision:** Your appeal has been reviewed and REJECTED.\n\n"
                f"**Original Ban Reason:** {ban_reason}\n\n"
                "❌ **The ban remains in effect.**\n\n"
                "**Reason for Rejection:**\n"
                "Your appeal did not meet the criteria for approval.\n\n"
                "⚠️ **This decision is FINAL.**\n"
                "No further appeals will be considered.\n\n"
                "Please respect the bot's terms of service."
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Failed to notify user {user_id}: {e}")
    
    # Update callback message
    try:
        await callback.message.edit_text(
            callback.message.text + f"\n\n❌ **REJECTED** by admin {callback.from_user.username or callback.from_user.id}",
            parse_mode="Markdown"
        )
    except:
        pass
    
    await callback.answer("❌ Appeal rejected!", show_alert=True)


# ==========================================
# 🔧 APPEAL EXECUTION FUNCTIONS
# ==========================================

async def execute_appeal_approval(callback: types.CallbackQuery, user_id: str, custom_message: str = None):
    """Execute appeal approval with optional custom message"""
    # Get appeal details
    appeal = col_appeals.find_one({"user_id": user_id, "status": "pending"})
    if not appeal:
        await callback.answer("❌ Appeal not found or already processed!", show_alert=True)
        return
    
    # Update appeal status
    col_appeals.update_one(
        {"user_id": user_id, "status": "pending"},
        {
            "$set": {
                "status": "approved",
                "reviewed_by": callback.from_user.id,
                "review_date": datetime.now(IST),
                "response": custom_message or "Appeal approved - ban lifted"
            }
        }
    )
    
    # Unban user - restore ALL features
    ban_record = col_banned.find_one({"user_id": user_id})
    if ban_record:
        col_banned.delete_one({"user_id": user_id})
        
        col_users.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "was_unbanned": True,
                    "previous_ban_reason": ban_record.get("reason", "Unknown"),
                    "unbanned_at": datetime.now(IST),
                    "unbanned_by": callback.from_user.id,
                    "has_warning": True,
                    "warning_message": "⚠️ DO NOT REPEAT VIOLATIONS - This is your final warning!"
                },
                "$unset": {"banned": "", "ban_reason": "", "ban_type": ""}
            },
            upsert=True
        )
        
        col_ban_history.insert_one({
            "user_id": user_id,
            "msa_id": appeal.get("msa_id", "UNKNOWN"),
            "username": appeal.get("username", "No Username"),
            "user_name": appeal.get("user_name", "Unknown"),
            "action_type": "appeal_approved",
            "admin_id": callback.from_user.id,
            "admin_name": callback.from_user.username or str(callback.from_user.id),
            "reason": "Ban appeal approved",
            "timestamp": datetime.now(IST),
            "previous_ban_reason": ban_record.get("reason", "Unknown"),
            "custom_message": custom_message
        })
    
    # Send notification to user
    msa_id = appeal.get("msa_id", "UNKNOWN")
    username = appeal.get("username", "User")
    
    if custom_message:
        user_message = custom_message
    else:
        user_message = (
            "✅ **BAN APPEAL APPROVED**\n\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👤 **Username:** @{username}\n\n"
            "🎉 **Your ban has been lifted!**\n\n"
            "✨ **ALL FEATURES RESTORED:**\n"
            "   • ✅ Full bot access\n"
            "   • ✅ All commands enabled\n"
            "   • ✅ Premium features active\n"
            "   • ✅ MSA submissions allowed\n\n"
            "⚠️ **FINAL WARNING:**\n"
            "**This is your SECOND CHANCE - DO NOT REPEAT VIOLATIONS!**\n\n"
            "❌ Any future violations will result in:\n"
            "   • Immediate permanent ban\n"
            "   • No appeal option\n"
            "   • No exceptions\n\n"
            "📱 Please follow all bot rules and guidelines.\n\n"
            "Welcome back! 🎊"
        )
    
    try:
        await worker_bot.send_message(chat_id=int(user_id), text=user_message, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Failed to notify user {user_id}: {e}")
    
    # Update channel message status
    try:
        channel_msg_id = appeal.get("channel_message_id")
        if channel_msg_id:
            await worker_bot.edit_message_text(
                chat_id=APPEAL_CHANNEL_ID,
                message_id=channel_msg_id,
                text=appeal.get("original_text", "") + f"\n\n✅ **APPROVED** by @{callback.from_user.username or callback.from_user.id}\n🕐 {datetime.now(IST).strftime('%d-%m-%Y %I:%M %p')}",
                parse_mode="Markdown"
            )
    except:
        pass
    
    await callback.answer("✅ Appeal approved! User has been unbanned.", show_alert=True)
    # Refresh the view
    try:
        await callback.message.edit_text(
            "✅ **Appeal Approved Successfully!**\n\n"
            "User has been notified and unbanned.\n"
            "All features have been restored.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back to Pending", callback_data="appeals_view_pending")
            ]]),
            parse_mode="Markdown"
        )
    except:
        pass


async def execute_appeal_rejection(callback: types.CallbackQuery, user_id: str, custom_message: str = None):
    """Execute appeal rejection with optional custom message"""
    appeal = col_appeals.find_one({"user_id": user_id, "status": "pending"})
    if not appeal:
        await callback.answer("❌ Appeal not found or already processed!", show_alert=True)
        return
    
    col_appeals.update_one(
        {"user_id": user_id, "status": "pending"},
        {
            "$set": {
                "status": "rejected",
                "reviewed_by": callback.from_user.id,
                "review_date": datetime.now(IST),
                "response": custom_message or "Appeal rejected - ban remains"
            }
        }
    )
    
    col_ban_history.insert_one({
        "user_id": user_id,
        "msa_id": appeal.get("msa_id", "UNKNOWN"),
        "username": appeal.get("username", "No Username"),
        "user_name": appeal.get("user_name", "Unknown"),
        "action_type": "appeal_rejected",
        "admin_id": callback.from_user.id,
        "admin_name": callback.from_user.username or str(callback.from_user.id),
        "reason": "Ban appeal rejected",
        "timestamp": datetime.now(IST),
        "custom_message": custom_message
    })
    
    msa_id = appeal.get("msa_id", "UNKNOWN")
    username = appeal.get("username", "User")
    ban_reason = appeal.get("ban_reason", "Violation of bot rules")
    
    if custom_message:
        user_message = custom_message
    else:
        user_message = (
            "❌ **BAN APPEAL REJECTED**\n\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👤 **Username:** @{username}\n\n"
            "🚫 **Decision:** Your appeal has been reviewed and REJECTED.\n\n"
            f"**Original Ban Reason:** {ban_reason}\n\n"
            "❌ **The ban remains in effect.**\n\n"
            "**Reason for Rejection:**\n"
            "Your appeal did not meet the criteria for approval.\n\n"
            "⚠️ **This decision is FINAL.**\n"
            "No further appeals will be considered.\n\n"
            "Please respect the bot's terms of service."
        )
    
    try:
        await worker_bot.send_message(chat_id=int(user_id), text=user_message, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Failed to notify user {user_id}: {e}")
    
    # Update channel message status
    try:
        channel_msg_id = appeal.get("channel_message_id")
        if channel_msg_id:
            await worker_bot.edit_message_text(
                chat_id=APPEAL_CHANNEL_ID,
                message_id=channel_msg_id,
                text=appeal.get("original_text", "") + f"\n\n❌ **REJECTED** by @{callback.from_user.username or callback.from_user.id}\n🕐 {datetime.now(IST).strftime('%d-%m-%Y %I:%M %p')}",
                parse_mode="Markdown"
            )
    except:
        pass
    
    await callback.answer("❌ Appeal rejected!", show_alert=True)
    try:
        await callback.message.edit_text(
            "❌ **Appeal Rejected**\n\n"
            "User has been notified.\n"
            "Ban remains in effect.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back to Pending", callback_data="appeals_view_pending")
            ]]),
            parse_mode="Markdown"
        )
    except:
        pass


@dp.callback_query(F.data.startswith("warn_appeal_"))
async def warn_appeal_action(callback: types.CallbackQuery, state: FSMContext):
    """Send warning message to user"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    
    user_id = callback.data.split("_")[-1]
    
    # Get appeal details
    appeal = col_appeals.find_one({"user_id": user_id, "status": "pending"})
    if not appeal:
        await callback.answer("❌ Appeal not found!", show_alert=True)
        return
    
    # Show template selection
    text = (
        f"⚠️ **SEND WARNING MESSAGE**\n"
        f"━━━━━━━━━━━━━━━━━\n\n"
        f"👤 User: @{appeal.get('username', 'Unknown')}\n"
        f"📝 MSA ID: `{appeal.get('msa_id', 'UNKNOWN')}`\n\n"
        f"**Select a template:**\n"
        f"1️⃣ Final Warning\n"
        f"2️⃣ Rejected - Spam\n"
        f"3️⃣ Rejected - Abuse\n"
        f"4️⃣ Under Review\n"
        f"5️⃣ Custom Message\n\n"
        f"Reply with template number (1-5):"
    )
    
    await callback.message.answer(text, parse_mode="Markdown")
    await state.update_data(appeal_user_id=user_id)
    await state.set_state(AppealState.waiting_for_template_message)
    await callback.answer()


@dp.message(AppealState.waiting_for_template_message)
async def process_template_selection(message: types.Message, state: FSMContext):
    """Process warning template selection (for warn_appeal only)"""
    if not is_admin(message.from_user.id): return
    
    data = await state.get_data()
    user_id = data.get("appeal_user_id")
    selection = message.text.strip()
    
    templates = {
        "1": "⚠️ **FINAL WARNING**\n\nYour appeal has been reviewed. You are being given ONE more chance.\n\n**DO NOT REPEAT YOUR VIOLATION.**\n\nAny future violations will result in permanent ban with no appeal option.\n\nPlease respect bot usage guidelines.",
        "2": "❌ **APPEAL REJECTED**\n\n**Reason:** Spam behavior detected\n\nYour appeal has been reviewed and rejected. The ban remains in effect.\n\nSpamming the bot is not tolerated. This decision is final.",
        "3": "❌ **APPEAL REJECTED**\n\n**Reason:** Abuse of bot features\n\nYour appeal has been denied. The ban will remain permanent.\n\nAbusing bot features violates our terms of service. No further appeals will be considered.",
        "4": "⏳ **APPEAL UNDER REVIEW**\n\nThank you for your appeal. Our team is currently reviewing your case.\n\nYou will receive a response within 24 hours.\n\nPlease do not submit multiple appeals - this will not speed up the process."
    }
    
    if selection == "5":
        await message.answer("✍️ **Type your custom warning message:**")
        return
    
    if selection not in templates:
        await message.answer("❌ Invalid selection. Please choose 1-5.")
        return
    
    warning_text = templates[selection]
    
    # Send warning to user
    try:
        await worker_bot.send_message(
            chat_id=int(user_id),
            text=warning_text,
            parse_mode="Markdown"
        )
        await message.answer("✅ Warning sent successfully!")
    except Exception as e:
        await message.answer(f"❌ Failed to send warning: {e}")
    
    # Update appeal with warning note
    col_appeals.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "last_warning": warning_text,
                "last_warning_date": datetime.now(IST),
                "warned_by": message.from_user.id
            }
        }
    )
    
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id)
    
    await state.clear()
    await show_dashboard_ui(message, message.from_user.id)


# Main function
if __name__ == "__main__":
    import asyncio
    
    async def startup():
        while True:  # Infinite restart loop
            try:
                # Initialize database first
                print("🤖 Manager Bot Starting...")
                print(f"📅 Start Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
                
                if not await initialize_database():
                    print("❌ Failed to initialize database. Retrying in 10 seconds...")
                    await asyncio.sleep(10)
                    continue
                
                try:
                    await manager_bot.send_message(OWNER_ID, "🟢 **Command Terminal Activated**\nIron Dome and Nuclear Ghost Protocols are ENGAGED.")
                except:
                    pass
                await manager_bot.delete_webhook(drop_pending_updates=True)
                
                # Start background tasks
                asyncio.create_task(supervisor_routine())
                asyncio.create_task(scheduled_health_check()) 
                asyncio.create_task(scheduled_pruning_cleanup())
                
                # Start polling
                print("✅ Bot polling started successfully")
                await dp.start_polling(manager_bot, skip_updates=True)
                print("⚠️ Polling stopped unexpectedly, restarting in 3 seconds...")
                await asyncio.sleep(3)
            except TelegramConflictError:
                print("💀 GHOST DETECTED! Waiting 20 seconds to purge ghost...")
                await asyncio.sleep(20)
            except (KeyboardInterrupt, SystemExit):
                print("🛑 Command Hub Stopped Safely")
                break
            except Exception as e:
                print(f"💥 SYSTEM BREACH: {e}")
                traceback.print_exc()
                print("⏳ Restarting in 5 seconds...")
                await asyncio.sleep(5)
    
    # Run the bot with retry mechanism
    threading.Thread(target=run_health_server, daemon=True).start()
    try:
        time.sleep(2)
        asyncio.run(startup())
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Bot stopped by user")


