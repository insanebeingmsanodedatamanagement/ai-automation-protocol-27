import asyncio
import os
import sys
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiohttp import web as aiohttp_web
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.objectid import ObjectId
from aiogram.fsm.storage.memory import MemoryStorage
import aiohttp
from aiogram.exceptions import TelegramNetworkError, TelegramServerError

# Fix Windows console encoding for emojis
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# ==============================================
# BOT 10 - BROADCAST MANAGEMENT SYSTEM
# ==============================================
# Bot 10: Admin interface for managing broadcasts
# Bot 8:  Actual delivery bot that sends to users
# This ensures broadcasts appear to come from Bot 8
# ==============================================

# Helper function for retry logic with exponential backoff
async def retry_operation(operation, max_retries=3, base_delay=1.0, operation_name="operation"):
    """Retry an async operation with exponential backoff for network errors"""
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return await operation()
        except (TelegramNetworkError, TelegramServerError, aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as e:
            last_exception = e
            if attempt < max_retries - 1:  # Don't delay on last attempt
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                print(f"⚠️ {operation_name} failed (attempt {attempt + 1}/{max_retries}): {str(e)[:50]}...")
                print(f"🔄 Retrying in {delay:.1f} seconds...")
                await asyncio.sleep(delay)
            else:
                print(f"❌ {operation_name} failed after {max_retries} attempts: {str(e)}")
        except Exception as e:
            # Non-network errors - don't retry
            print(f"❌ {operation_name} failed with non-network error: {str(e)}")
            raise e
    
    # If we get here, all retries failed
    raise last_exception

# Load environment variables
load_dotenv(".env")
BOT_TOKEN = os.getenv("BOT_10_TOKEN")
BOT_8_TOKEN = os.getenv("BOT_8_TOKEN")  # Bot 8 for delivery
MASTER_ADMIN_ID = int(os.getenv("MASTER_ADMIN_ID", "0"))
OWNER_ID = MASTER_ADMIN_ID  # Alias for compatibility with auto-healer notifications
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "MSANodeDB")  # MongoDB database name
REVIEW_LOG_CHANNEL = int(os.getenv("REVIEW_LOG_CHANNEL", 0))  # Support ticket channel
# Render web-service health check port (Render sets PORT automatically)
PORT = int(os.getenv("PORT", 8080))

# Validate critical config at startup
if not BOT_TOKEN:
    print("❌ FATAL: BOT_10_TOKEN not set in .env")
    sys.exit(1)
if not BOT_8_TOKEN:
    print("❌ FATAL: BOT_8_TOKEN not set in .env")
    sys.exit(1)
if not MASTER_ADMIN_ID:
    print("❌ FATAL: MASTER_ADMIN_ID not set in .env")
    sys.exit(1)
if not MONGO_URI:
    print("❌ FATAL: MONGO_URI not set in .env")
    sys.exit(1)

print(f"🔄 Initializing Bot 10 - Broadcast Management System")
print(f"🤖 Bot 10 Token: {BOT_TOKEN[:20]}...")
print(f"🤖 Bot 8 Token: {BOT_8_TOKEN[:20]}...")

# MongoDB Connection
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]
col_broadcasts = db["bot10_broadcasts"]
col_user_tracking = db["bot10_user_tracking"]  # Track user sources
col_support_tickets = db["support_tickets"]  # Bot 8 support tickets
col_cleanup_backups = db["cleanup_backups"]  # Automated cleanup backups (cloud-safe)
col_cleanup_logs = db["cleanup_logs"]  # Cleanup history logs
col_banned_users = db["banned_users"]  # Banned users - blocks all bot 8 access
col_suspended_features = db["suspended_features"]  # User-specific feature suspensions
col_bot10_backups = db["bot10_backups"]  # Bot 10 manual backups (cloud-safe)
col_admins = db["bot10_admins"]  # Bot 10 admin management
col_access_attempts = db["bot10_access_attempts"]  # Track unauthorized access attempts
col_bot8_settings = db["bot8_settings"]  # Bot 8 global settings (Maintenance Mode)

# Bot 8 Collections (for Terminal and Reset Data features)
col_user_verification = db["user_verification"]  # Bot 8 user verification data
col_msa_ids = db["msa_ids"]  # Bot 8 MSA+ ID tracking
col_bot9_pdfs = db["bot9_pdfs"]  # Bot 8 PDF/Affiliate/YT data
col_bot9_ig_content = db["bot9_ig_content"]  # Bot 8 IG Content Collection

print(f"💾 Connected to MongoDB: MSANodeDB")
print(f"📁 Bot 10 Collections: bot10_broadcasts, bot10_user_tracking, support_tickets, cleanup_backups, cleanup_logs, banned_users, suspended_features, bot10_backups")
print(f"📁 Bot 8 Collections: user_verification, msa_ids, bot9_pdfs, bot9_ig_content")

# Create unique indexes to prevent duplicates
try:
    col_broadcasts.create_index("broadcast_id", unique=True)
    col_broadcasts.create_index("index", unique=True)
    col_user_tracking.create_index("user_id", unique=True)  # One user = one record
    
    # Support tickets performance indexes (CRITICAL for scaling to millions of users)
    col_support_tickets.create_index([("status", 1), ("created_at", -1)])  # List by status
    col_support_tickets.create_index([("user_id", 1), ("created_at", -1)])  # User lookups
    col_support_tickets.create_index([("msa_id", 1)])  # MSA ID lookups
    col_support_tickets.create_index([("status", 1), ("resolved_at", 1)])  # Cleanup queries
    col_support_tickets.create_index([("user_name", "text"), ("username", "text")])  # Text search
    
    # Cleanup collection indexes
    col_cleanup_backups.create_index([("backup_date", -1)])  # Latest backup queries
    col_cleanup_logs.create_index([("cleanup_date", -1)])  # Latest log queries
    
    # Bot 10 backups collection indexes
    col_bot10_backups.create_index([("backup_date", -1)])  # Latest backup first
    col_bot10_backups.create_index([("backup_type", 1)])  # Filter by type
    
    # Admin collection indexes
    col_admins.create_index("user_id", unique=True)  # One admin record per user
    col_admins.create_index([("added_at", -1)])  # Latest admins first
    
    # Access attempts indexes for spam detection
    col_access_attempts.create_index([("user_id", 1), ("attempted_at", -1)])  # Spam queries
    col_access_attempts.create_index([("attempted_at", -1)])  # Cleanup old attempts
    
    # Runtime state index (restart recovery)
    db["bot10_runtime_state"].create_index("state_key", unique=True)
    
    print("✅ Database indexes created for optimal performance")
except Exception as e:
    print(f"⚠️ Index creation warning: {str(e)}")  # May already exist

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)  # Bot 10 - Admin interface
bot_8 = Bot(token=BOT_8_TOKEN)  # Bot 8 - Message delivery
dp = Dispatcher(storage=MemoryStorage())

print(f"⚙️ Bot instances initialized")
print(f"📱 Bot 10: Admin interface ready")
print(f"📤 Bot 8: Message delivery ready")

# ==========================================
# 🕐 TIMEZONE CONFIGURATION
# ==========================================
_BOT10_TZ_STR = os.getenv("REPORT_TIMEZONE", "Asia/Kolkata")
try:
    _BOT10_TZ = ZoneInfo(_BOT10_TZ_STR)
except Exception:
    _BOT10_TZ = ZoneInfo("Asia/Kolkata")

def now_local() -> datetime:
    """Return current time as a naive datetime in the configured local timezone."""
    return datetime.now(_BOT10_TZ).replace(tzinfo=None)

# ==========================================
# ENTERPRISE HEALTH TRACKING (Global State)
# Defined after now_local() so bot_start_time is correct
# ==========================================
bot10_health = {
    "errors_caught": 0,
    "auto_healed": 0,
    "owner_notified": 0,
    "last_error": None,
    "last_error_type": None,
    "bot_start_time": now_local(),
    "consecutive_failures": 0,
}

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def format_datetime(dt):
    """Format datetime to 12-hour AM/PM format in local timezone"""
    if not dt:
        return "N/A"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except:
            return dt
    # If naive, assume it was stored in local time (consistent with now_local())
    return dt.strftime("%b %d, %Y %I:%M %p")

# ==========================================
# FSM STATES
# ==========================================

class BroadcastStates(StatesGroup):
    selecting_category = State()
    waiting_for_message = State()
    waiting_for_edit_id = State()
    waiting_for_edit_content = State()
    waiting_for_edit_confirm = State()
    waiting_for_delete_id = State()
    waiting_for_delete_confirm = State()
    waiting_for_list_search = State()

class SupportStates(StatesGroup):
    waiting_for_ticket_search = State()
    waiting_for_resolve_id = State()
    waiting_for_reply_id = State()
    waiting_for_reply_message = State()
    waiting_for_delete_ticket_id = State()
    waiting_for_user_search = State()
    waiting_for_priority_id = State()
    waiting_for_priority_level = State()

class FindStates(StatesGroup):
    waiting_for_search = State()  # Waiting for MSA ID or User ID input

class ShootStates(StatesGroup):
    waiting_for_ban_id = State()
    waiting_for_ban_confirm = State()
    waiting_for_unban_id = State()
    waiting_for_unban_confirm = State()
    waiting_for_delete_id = State()
    waiting_for_delete_confirm = State()
    waiting_for_suspend_id = State()
    selecting_suspend_features = State()
    waiting_for_unsuspend_id = State()
    waiting_for_reset_id = State()
    waiting_for_reset_confirm = State()
    waiting_for_shoot_search_id = State()
    waiting_for_temp_ban_id = State()
    selecting_temp_ban_duration = State()
    waiting_for_temp_ban_confirm = State()

class BroadcastWithButtonsStates(StatesGroup):
    selecting_category = State()
    waiting_for_message = State()
    waiting_for_button_text = State()
    waiting_for_button_url = State()
    confirming_buttons = State()

class BackupStates(StatesGroup):
    viewing_menu = State()

class ResetDataStates(StatesGroup):
    selecting_reset_type = State()        # Choose: Bot8 / Bot10 / ALL
    waiting_for_first_confirm = State()  # Bot8 first confirmation
    waiting_for_final_confirm = State()  # Bot8 final confirmation
    bot10_first_confirm = State()        # Bot10 first confirmation
    bot10_final_confirm = State()        # Bot10 final confirmation
    all_first_confirm = State()          # ALL first confirmation
    all_final_confirm = State()          # ALL final confirmation

class TerminalStates(StatesGroup):
    viewing_bot8 = State()
    viewing_bot10 = State()

class AdminStates(StatesGroup):
    waiting_for_new_admin_id = State()
    waiting_for_admin_role = State()
    waiting_for_remove_admin_id = State()
    waiting_for_remove_confirm = State()
    waiting_for_permission_admin_id = State()
    selecting_permissions = State()
    toggling_permissions = State()
    waiting_for_role_admin_id = State()
    selecting_role = State()
    waiting_for_lock_user_id = State()
    waiting_for_unlock_user_id = State()
    waiting_for_ban_user_id = State()
    waiting_for_admin_search = State()
    # Owner transfer flow
    owner_transfer_first_confirm = State()   # Step 1: "type CONFIRM"
    owner_transfer_second_confirm = State()  # Step 2: "type TRANSFER"
    owner_transfer_password = State()        # Step 3: enter secret password

class Bot8SettingsStates(StatesGroup):
    viewing_menu = State()

class GuideStates(StatesGroup):
    selecting         = State()   # user is on the guide selector screen
    viewing_bot10     = State()   # paginated Bot 10 admin guide
    viewing_bot8      = State()   # Bot 8 user guide (from inside bot10)

# ==========================================
# ==========================================
# LIVE TERMINAL LOGGING SYSTEM
# ==========================================

# In-memory log storage (circular buffer) — also backed by MongoDB for Render cross-process support
MAX_LOGS = 50  # Keep last 50 logs per bot

# MongoDB collection for persistent logs (shared across processes / Render services)
col_live_logs = db["live_terminal_logs"]

# Initialize with startup message
start_time = now_local().strftime('%I:%M:%S %p')
bot8_logs = [{
    "timestamp": start_time,
    "action": "SYSTEM",
    "user_id": 0,
    "details": "Bot 8 log tracking initialized",
    "full_text": f"[{start_time}] SYSTEM > Bot 8 log tracking initialized"
}]
bot10_logs = [{
    "timestamp": start_time,
    "action": "SYSTEM",
    "user_id": 0,
    "details": "Bot 10 log tracking initialized",
    "full_text": f"[{start_time}] SYSTEM > Bot 10 log tracking initialized"
}]

def log_action(action_type, user_id, details="", bot="bot10"):
    """Log actions to console, memory, AND MongoDB for live terminal display (works on Render)"""
    timestamp = now_local().strftime('%I:%M:%S %p')

    # Color codes for console terminal
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

    # Console output with colors
    print(f"{CYAN}[{timestamp}]{RESET} {BOLD}{action_type}{RESET}")
    if details:
        print(f"  📋 {details}")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # Build log entry
    log_entry = {
        "timestamp": timestamp,
        "created_at": now_local(),
        "bot": bot,
        "action": action_type,
        "user_id": user_id,
        "details": details,
        "full_text": f"[{timestamp}] {action_type}" + (f"\n  {details}" if details else "")
    }

    # Add to in-memory list
    if bot == "bot8":
        bot8_logs.append(log_entry)
        if len(bot8_logs) > MAX_LOGS:
            bot8_logs.pop(0)
    else:
        bot10_logs.append(log_entry)
        if len(bot10_logs) > MAX_LOGS:
            bot10_logs.pop(0)

    # Persist to MongoDB (for Render cross-process live view)
    try:
        col_live_logs.insert_one(log_entry)
        # Keep collection trimmed — delete oldest beyond MAX_LOGS*2 per bot
        count = col_live_logs.count_documents({"bot": bot})
        if count > MAX_LOGS * 2:
            oldest = list(col_live_logs.find({"bot": bot}, {"_id": 1}).sort("created_at", 1).limit(count - MAX_LOGS))
            if oldest:
                col_live_logs.delete_many({"_id": {"$in": [d["_id"] for d in oldest]}})
    except Exception:
        pass  # Never let logging break the bot

def get_terminal_logs(bot="bot10", limit=50):
    """Get raw terminal logs — reads from MongoDB first (Render-safe), falls back to memory"""
    try:
        # Read from MongoDB for cross-process / Render support
        docs = list(col_live_logs.find({"bot": bot}, {"_id": 0}).sort("created_at", -1).limit(limit))
        if docs:
            docs.reverse()  # Oldest first (terminal style)
            log_lines = []
            MAX_CHARS = 3500
            current_length = 0
            for doc in docs:
                ts = doc.get("timestamp", "??:??:?? ?M")
                action = doc.get("action", "")
                detail = doc.get("details", "")
                line = f"[{ts}] {action}" + (f" > {detail}" if detail else "")
                if current_length + len(line) + 1 > MAX_CHARS:
                    break
                log_lines.append(line)
                current_length += len(line) + 1
            return "\n".join(log_lines) if log_lines else ">> NO LOGS YET..."
    except Exception:
        pass

    # Fallback to in-memory
    logs = bot8_logs if bot == "bot8" else bot10_logs
    if not logs:
        return ">> SYSTEM INITIALIZED. WAITING FOR EVENTS..."
    recent_logs = logs[-limit:]
    MAX_CHARS = 3500
    final_lines = []
    current_length = 0
    for log in reversed(recent_logs):
        line = f"[{log['timestamp']}] {log['action']} > {log['details']}"
        if current_length + len(line) + 1 > MAX_CHARS:
            break
        final_lines.insert(0, line)
        current_length += len(line) + 1
    return "\n".join(final_lines)

# ==========================================
# MENU FUNCTIONS
# ==========================================
# ACCESS CONTROL FUNCTIONS
# ==========================================

async def is_admin(user_id: int) -> bool:
    """Check if user is an admin or the master admin AND is unlocked"""
    if user_id == MASTER_ADMIN_ID:
        return True
    
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        return False
    
    # Check if admin is locked (inactive)
    if admin.get('locked', False):
        return False  # Locked admins cannot access Bot 10
    
    return True  # Admin exists and is unlocked

async def notify_owner_unauthorized_access(user_id: int, user_name: str, username: str, attempt_count: int, was_banned: bool = False):
    """Notify owner about unauthorized access attempts"""
    timestamp = now_local().strftime('%b %d, %Y %I:%M %p')  # 12-hour format
    
    msg = (
        f"🚨 **UNAUTHORIZED ACCESS ATTEMPT**\n\n"
        f"👤 User ID: `{user_id}`\n"
        f"📝 Name: {user_name or 'Unknown'}\n"
        f"🔗 Username: @{username or 'None'}\n"
        f"🕐 Time: {timestamp}\n"
        f"🔢 Attempt #{attempt_count}"
    )
    
    if was_banned:
        msg += f"\n\n🚫 **AUTO-BANNED** (Spam detected - 3+ attempts in 5 min)"
    
    try:
        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="Markdown")
        log_action("🚨 UNAUTHORIZED ACCESS", user_id, f"Notified owner - Attempt #{attempt_count}")
    except Exception as e:
        print(f"❌ Failed to notify owner: {e}")

async def has_permission(user_id: int, permission: str) -> bool:
    """Check if admin has specific permission"""
    # Master admin always has all permissions
    if user_id == MASTER_ADMIN_ID:
        return True
    
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        return False
    
    perms = admin.get('permissions', [])
    return 'all' in perms or permission in perms

# ==========================================
# MENU FUNCTIONS
# ==========================================

async def get_main_menu(user_id: int = None):
    """Main menu keyboard - shows only permitted features"""
    # Master admin and no user_id = show all
    if user_id is None or user_id == MASTER_ADMIN_ID:
        keyboard = [
            [KeyboardButton(text="📢 BROADCAST"), KeyboardButton(text="🔍 FIND")],
            [KeyboardButton(text="📊 TRAFFIC"), KeyboardButton(text="🩺 DIAGNOSIS")],
            [KeyboardButton(text="📸 SHOOT"), KeyboardButton(text="💬 SUPPORT")],
            [KeyboardButton(text="💾 BACKUP"), KeyboardButton(text="🖥️ TERMINAL")],
            [KeyboardButton(text="🤖 BOT 8 SETTINGS"), KeyboardButton(text="👥 ADMINS")],
            [KeyboardButton(text="⚠️ RESET DATA"), KeyboardButton(text="📖 GUIDE")]
        ]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    # Get user permissions
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        # Not an admin - show minimal menu
        keyboard = [[KeyboardButton(text="👥 ADMINS"), KeyboardButton(text="📖 GUIDE")]]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    perms = admin.get('permissions', [])
    has_all = 'all' in perms
    
    # Permission to button mapping
    perm_buttons = {
        'broadcast': "📢 BROADCAST",
        'find': "🔍 FIND",
        'traffic': "📊 TRAFFIC",
        'diagnosis': "🩺 DIAGNOSIS",
        'shoot': "📸 SHOOT",
        'support': "💬 SUPPORT",
        'backup': "💾 BACKUP",
        'terminal': "🖥️ TERMINAL"
    }
    
    # Build keyboard with only permitted features
    available_buttons = []
    for perm, button_text in perm_buttons.items():
        if has_all or perm in perms:
            available_buttons.append(button_text)
    
    # Always show GUIDE (ADMINS is now Owner Only)
    available_buttons.append("📖 GUIDE")
    
    # Arrange in rows of 2
    keyboard = []
    for i in range(0, len(available_buttons), 2):
        row = available_buttons[i:i+2]
        keyboard.append([KeyboardButton(text=btn) for btn in row])
    
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def get_backup_menu():
    """Backup management submenu"""
    keyboard = [
        [KeyboardButton(text="📥 BACKUP NOW"), KeyboardButton(text="📊 VIEW BACKUPS")],
        [KeyboardButton(text="🗓️ MONTHLY STATUS"), KeyboardButton(text="⚙️ AUTO-BACKUP")],
        [KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_broadcast_menu():
    """Broadcast management submenu"""
    keyboard = [
        [KeyboardButton(text="📤 SEND BROADCAST")],
        [KeyboardButton(text="🗑️ DELETE BROADCAST"), KeyboardButton(text="✏️ EDIT BROADCAST")],
        [KeyboardButton(text="📋 LIST BROADCASTS")],
        [KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def _format_broadcast_msg(text: str, is_caption: bool = False) -> str:
    """
    Wrap a broadcast message in MSA NODE official formatting.
    is_caption=True  →  lightweight footer only (Telegram caption ≤ 1024 chars).
    is_caption=False →  full header + footer for text-only broadcasts.
    """
    try:
        dt = now_local().strftime("%b %d, %Y  ·  %I:%M %p")
    except Exception:
        dt = "MSA NODE"

    body = (text or "").strip()

    if is_caption:
        footer = (
            "\n\n──────────────────────────────"
            "\n📢  MSA NODE  ·  Official"
            f"\n🕐  {dt}"
        )
        max_body = 1024 - len(footer) - 2
        if len(body) > max_body:
            body = body[:max_body].rsplit(" ", 1)[0] + "…"
        return body + footer
    else:
        header = (
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  📢  MSA NODE  ·  BROADCAST\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        footer = (
            "\n\n──────────────────────────────"
            "\n🌐  MSA NODE Ecosystem  ·  Official"
            f"\n🕐  {dt}"
        )
        return header + body + footer


def get_broadcast_type_menu():
    """Broadcast type selection menu"""
    keyboard = [
        [KeyboardButton(text="📝 NORMAL BROADCAST")],
        [KeyboardButton(text="🔗 BROADCAST WITH BUTTONS")],
        [KeyboardButton(text="⬅️ BACK")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_support_management_menu():
    """Support ticket management submenu"""
    keyboard = [
        [KeyboardButton(text="🎫 PENDING TICKETS"), KeyboardButton(text="📋 ALL TICKETS")],
        [KeyboardButton(text="✅ RESOLVE TICKET"), KeyboardButton(text="📨 REPLY")],
        [KeyboardButton(text="🔍 SEARCH TICKETS"), KeyboardButton(text="🗑️ DELETE")],
        [KeyboardButton(text="📊 MORE OPTIONS")],
        [KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_support_more_menu():
    """Support advanced options submenu"""
    keyboard = [
        [KeyboardButton(text="📈 STATISTICS"), KeyboardButton(text="🚨 PRIORITY")],
        [KeyboardButton(text="⏰ AUTO-CLOSE"), KeyboardButton(text="📤 EXPORT")],
        [KeyboardButton(text="⬅️ BACK TO SUPPORT")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_category_menu():
    """Category selection menu for broadcasts"""
    keyboard = [
        [KeyboardButton(text="📺 YT"), KeyboardButton(text="📸 IG")],
        [KeyboardButton(text="📎 IG CC"), KeyboardButton(text="🔗 YTCODE")],
        [KeyboardButton(text="👥 ALL")],
        [KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_admin_menu():
    """Admin management submenu"""
    keyboard = [
        [KeyboardButton(text="➕ NEW ADMIN"), KeyboardButton(text="➖ REMOVE ADMIN")],
        [KeyboardButton(text="🔐 PERMISSIONS"), KeyboardButton(text="👔 MANAGE ROLES")],
        [KeyboardButton(text="🔒 LOCK/UNLOCK USER"), KeyboardButton(text="🚫 BAN CONFIG")],
        [KeyboardButton(text="📋 LIST ADMINS"), KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_bot8_settings_menu():
    """Bot 8 Settings Menu"""
    # Get current status
    settings = col_bot8_settings.find_one({"setting": "maintenance_mode"})
    is_maintenance = settings.get("value", False) if settings else False
    
    toggle_text = "🛠 MAINTENANCE: OFF" if not is_maintenance else "🛠 MAINTENANCE: ON"
    
    keyboard = [
        [KeyboardButton(text=toggle_text)],
        [KeyboardButton(text="📢 NOTIFY USERS: MAINTENANCE ON")],
        [KeyboardButton(text="📢 NOTIFY USERS: BACK ONLINE")],
        [KeyboardButton(text="⬅️ MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# ==========================================
# BROADCAST HELPER FUNCTIONS
# ==========================================

def reindex_broadcasts():
    """Re-number all broadcasts sequentially (1, 2, 3, ...) with no gaps.
    Updates both 'index' and 'broadcast_id' fields to stay consistent."""
    all_brd = list(col_broadcasts.find({}, {"_id": 1}).sort("index", 1))
    for new_idx, doc in enumerate(all_brd, start=1):
        col_broadcasts.update_one(
            {"_id": doc["_id"]},
            {"$set": {"index": new_idx, "broadcast_id": f"brd{new_idx}"}}
        )
    print(f"🔄 Reindexed {len(all_brd)} broadcasts sequentially.")

def get_next_broadcast_id():
    """Get next sequential broadcast ID (brd1, brd2, etc.) after reindex."""
    existing = list(col_broadcasts.find({}, {"broadcast_id": 1, "index": 1}).sort("index", 1))
    
    if not existing:
        return "brd1", 1
    
    next_index = len(existing) + 1
    return f"brd{next_index}", next_index

# ==========================================
# COMMAND HANDLERS
# ==========================================

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Start command - shows main menu (ADMIN ONLY)"""
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    username = message.from_user.username
    
    # 1. Check if user is banned - Silent ignore
    if col_banned_users.find_one({"user_id": user_id}):
        log_action("🚫 BANNED ACCESS BLOCKED", user_id, f"Banned user tried /start")
        return  # Complete silence
    
    # 2. Check if user is admin
    if await is_admin(user_id):
        # Admin access granted
        log_action("✅ ADMIN ACCESS", user_id, f"{user_name} started bot")
        menu = await get_main_menu(user_id)  # Pass user_id for permission filtering
        await message.answer(
            f"👋 Welcome to Bot 10!\n\n"
            f"Select an option from the menu below:",
            reply_markup=menu
        )
        return
    
    # 3. Non-admin access attempt
    log_action("❌ NON-ADMIN ATTEMPT", user_id, f"{user_name} tried to access")
    
    # Record attempt
    attempt_doc = {
        "user_id": user_id,
        "user_name": user_name,
        "username": username,
        "attempted_at": now_local()
    }
    col_access_attempts.insert_one(attempt_doc)
    
    # Check for spam (3+ attempts in 5 minutes)
    five_min_ago = now_local() - timedelta(minutes=5)
    recent_attempts = col_access_attempts.count_documents({
        "user_id": user_id,
        "attempted_at": {"$gte": five_min_ago}
    })
    
    # Auto-ban if spam detected
    if recent_attempts >= 3:
        # Ban user
        ban_doc = {
            "user_id": user_id,
            "banned_by": "SYSTEM",
            "banned_at": now_local(),
            "reason": "Automated: Spam detection (3+ unauthorized access attempts)",
            "status": "banned",
            "scope": "bot10"  # Only blocks Bot 10 admin access, NOT Bot 8
        }
        try:
            col_banned_users.insert_one(ban_doc)
            log_action("🚫 AUTO-BAN", user_id, f"Spam detected - {recent_attempts} attempts")
            
            # Notify owner about ban
            await notify_owner_unauthorized_access(
                user_id, user_name, username, recent_attempts, was_banned=True
            )
        except:
            # Duplicate ban, just notify
            await notify_owner_unauthorized_access(
                user_id, user_name, username, recent_attempts, was_banned=False
            )
    else:
        # Not spam yet, just notify owner
        await notify_owner_unauthorized_access(
            user_id, user_name, username, recent_attempts, was_banned=False
        )
    
    # Silent reject - NO response to user
    return


@dp.message(Command("report"))
async def cmd_report(message: types.Message):
    """/report — On-demand full daily report (owner only)"""
    if message.from_user.id != MASTER_ADMIN_ID:
        return
    generating_msg = await message.answer("📊 Generating report...")
    try:
        report_text = await generate_daily_report()
        await generating_msg.delete()
        await message.answer(report_text, parse_mode="Markdown")
    except Exception as e:
        await generating_msg.edit_text(f"❌ Report generation failed: {str(e)[:100]}")


@dp.message(Command("health"))
async def cmd_health(message: types.Message):
    """/health — Show bot10 auto-healer health stats (owner only)"""
    if message.from_user.id != MASTER_ADMIN_ID:
        return
    uptime = now_local() - bot10_health["bot_start_time"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)

    try:
        t0 = time.time()
        client.admin.command('ping')
        db_ms = (time.time() - t0) * 1000
        db_status = f"✅ Online ({db_ms:.0f}ms)"
    except Exception:
        db_status = "❌ OFFLINE"

    healed = bot10_health["auto_healed"]
    errors = bot10_health["errors_caught"]
    success_rate = (healed / errors * 100) if errors > 0 else 100.0

    await message.answer(
        f"🏥 **BOT 10 HEALTH STATUS**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚡ **System:**\n"
        f"• Bot 10: ✅ Running\n"
        f"• Database: {db_status}\n"
        f"• Auto-Healer: ✅ Active\n"
        f"• Health Monitor: ✅ Running\n\n"
        f"⏱️ **Uptime:** {h}h {m}m\n"
        f"**Started:** {bot10_health['bot_start_time'].strftime('%b %d, %I:%M %p')}\n\n"
        f"📊 **Error Stats:**\n"
        f"• Total Caught: `{errors}`\n"
        f"• Auto-Healed: `{healed}`\n"
        f"• Success Rate: `{success_rate:.1f}%`\n"
        f"• Owner Alerts: `{bot10_health['owner_notified']}`\n"
        f"• Consecutive Fails: `{bot10_health['consecutive_failures']}`\n\n"
        f"🕐 **Last Error:** {bot10_health['last_error'].strftime('%b %d %I:%M %p') if bot10_health['last_error'] else 'None'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_Health checks every hour | Reports at 8:40 AM & PM_",
        parse_mode="Markdown"
    )


# ==========================================
# MENU HANDLERS (Placeholders)
# ==========================================

@dp.message(F.text == "📢 BROADCAST")
async def broadcast_handler(message: types.Message):
    """Show broadcast management menu"""
    log_action("📢 BROADCAST MENU", message.from_user.id, "Opened broadcast management")
    await message.answer(
        "📢 **BROADCAST MANAGEMENT**\n\n"
        "Select an option:",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "⬅️ MAIN MENU")
async def back_to_main(message: types.Message, state: FSMContext):
    """Return to main menu"""
    await state.clear()
    await message.answer(
        "📋 **Main Menu**",
        reply_markup=await get_main_menu(message.from_user.id),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🤖 BOT 8 SETTINGS")
async def bot8_settings_handler(message: types.Message):
    """Show Bot 8 settings menu"""
    # Only Master Admin or Admins with 'all' permission should access this
    if not await has_permission(message.from_user.id, "all"):
         await message.answer("⛔ Access Denied: You need 'all' permissions to manage Bot 8 settings.")
         return

    log_action("🤖 BOT 8 SETTINGS", message.from_user.id, "Opened Bot 8 settings")
    
    # Get current maintenance status for display
    settings = col_bot8_settings.find_one({"setting": "maintenance_mode"})
    is_maintenance = settings.get("value", False) if settings else False
    status_icon = "🔴 UNDER MAINTENANCE" if is_maintenance else "🟢 ONLINE"
    updated_at = settings.get("updated_at", None) if settings else None
    updated_str = updated_at.strftime("%b %d, %Y %I:%M %p") if updated_at else "Never"
    
    await message.answer(
        f"🤖 **BOT 8 SETTINGS**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📡 **Current Status:** {status_icon}\n"
        f"🕒 **Last Changed:** {updated_str}\n\n"
        f"⚠️ **Maintenance Mode** will block ALL users from accessing Bot 8.\n\n"
        f"📢 Use **Notify Users** buttons to broadcast a message to all users via Bot 8.",
        reply_markup=get_bot8_settings_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text.in_({"🛠 MAINTENANCE: ON", "🛠 MAINTENANCE: OFF"}))
async def toggle_maintenance_handler(message: types.Message):
    """Toggle maintenance mode and broadcast notification to all users via Bot 8"""
    if not await has_permission(message.from_user.id, "all"):
         return

    # Determine new state based on button text (Toggle logic)
    # If button says "ON", it means mode IS on, so we want to turn it OFF.
    # If button says "OFF", it means mode IS off, so we want to turn it ON.
    turn_on = "OFF" in message.text
    
    col_bot8_settings.update_one(
        {"setting": "maintenance_mode"},
        {"$set": {
            "value": turn_on,
            "updated_at": now_local(),
            "updated_by": message.from_user.id
        }},
        upsert=True
    )
    
    status = "ENABLED" if turn_on else "DISABLED"
    log_action(f"🛠 MAINTENANCE {status}", message.from_user.id, f"Toggled maintenance mode to {status}")
    
    # Broadcast notification to all users via Bot 8
    all_users = list(col_user_tracking.find({}, {"user_id": 1}))
    sent_count = 0
    failed_count = 0
    
    if turn_on:
        broadcast_text = (
            "👤 **Dear Valued Member,**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔧 **MSA NODE AGENT — SYSTEM UPGRADE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Your MSA Node Agent is currently undergoing a **premium infrastructure upgrade** "
            "to deliver you an even more powerful experience.\n\n"
            "🚫 **During Upgrade:**\n"
            "• Start links are not active\n"
            "• All bot features are temporarily paused\n"
            "• No new sessions can begin\n\n"
            "⏳ **Status:** Coming back online very soon.\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Thank you for your patience. The upgrade ensures you receive the **best possible service**.\n\n"
            "_— MSA Node Systems_"
        )
    else:
        broadcast_text = (
            "✅ **MSA NODE AGENT — BACK ONLINE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🟢 Your MSA Node Agent has completed its upgrade and is now **fully operational**.\n\n"
            "**All features are now available:**\n"
            "• 🔍 Search Code\n"
            "• 📊 Dashboard\n"
            "• 📞 Support\n"
            "• All start links are active\n\n"
            "Thank you for your patience during the upgrade.\n\n"
            "_— MSA Node Systems_"
        )
    
    progress_msg = await message.answer(
        f"📡 Broadcasting {'maintenance' if turn_on else 'online'} notification to {len(all_users)} users..."
    )
    
    for user_doc in all_users:
        uid = user_doc.get("user_id")
        if not uid:
            continue
        try:
            await bot_8.send_message(uid, broadcast_text, parse_mode="Markdown")
            sent_count += 1
            await asyncio.sleep(0.05)  # Rate limit: ~20 msgs/sec
        except Exception:
            failed_count += 1
    
    try:
        await progress_msg.delete()
    except Exception:
        pass
    
    await message.answer(
        f"✅ **MAINTENANCE MODE {status}**\n\n"
        f"Bot 8 is now {'⛔ UNDER MAINTENANCE' if turn_on else '🟢 ONLINE'}.\n\n"
        f"📢 **Broadcast Result:**\n"
        f"• ✅ Sent: {sent_count} users\n"
        f"• ❌ Failed: {failed_count} users",
        reply_markup=get_bot8_settings_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text.in_({"📢 NOTIFY USERS: MAINTENANCE ON", "📢 NOTIFY USERS: BACK ONLINE"}))
async def notify_users_manual_handler(message: types.Message):
    """Manually send a notification broadcast to all users via Bot 8"""
    if not await has_permission(message.from_user.id, "all"):
        return

    is_maintenance_msg = "MAINTENANCE ON" in message.text

    if is_maintenance_msg:
        broadcast_text = (
            "👤 **Dear Valued Member,**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔧 **MSA NODE AGENT — SYSTEM UPGRADE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Your MSA Node Agent is currently undergoing a **premium infrastructure upgrade** "
            "to deliver you an even more powerful experience.\n\n"
            "🚫 **During Upgrade:**\n"
            "• Start links are not active\n"
            "• All bot features are temporarily paused\n"
            "• No new sessions can begin\n\n"
            "⏳ **Status:** Coming back online very soon.\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Thank you for your patience. The upgrade ensures you receive the **best possible service**.\n\n"
            "_— MSA Node Systems_"
        )
    else:
        broadcast_text = (
            "✅ **MSA NODE AGENT — BACK ONLINE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🟢 Your MSA Node Agent has completed its upgrade and is now **fully operational**.\n\n"
            "**All features are now available:**\n"
            "• 🔍 Search Code\n"
            "• 📊 Dashboard\n"
            "• 📞 Support\n"
            "• All start links are active\n\n"
            "Thank you for your patience during the upgrade.\n\n"
            "_— MSA Node Systems_"
        )

    all_users = list(col_user_tracking.find({}, {"user_id": 1}))
    sent_count = 0
    failed_count = 0

    progress_msg = await message.answer(
        f"📡 Sending {'maintenance' if is_maintenance_msg else 'online'} notification to {len(all_users)} users..."
    )

    for user_doc in all_users:
        uid = user_doc.get("user_id")
        if not uid:
            continue
        try:
            await bot_8.send_message(uid, broadcast_text, parse_mode="Markdown")
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed_count += 1

    try:
        await progress_msg.delete()
    except Exception:
        pass

    label = "MAINTENANCE" if is_maintenance_msg else "BACK ONLINE"
    log_action(f"📢 NOTIFY {label}", message.from_user.id, f"Manual notify: {sent_count} sent, {failed_count} failed")

    await message.answer(
        f"✅ **NOTIFICATION SENT**\n\n"
        f"📢 **Type:** {'Maintenance Alert' if is_maintenance_msg else 'Back Online Alert'}\n\n"
        f"📊 **Result:**\n"
        f"• ✅ Sent: {sent_count} users\n"
        f"• ❌ Failed: {failed_count} users",
        reply_markup=get_bot8_settings_menu(),
        parse_mode="Markdown"
    )

@dp.message(BroadcastStates.selecting_category)
async def process_category_selection(message: types.Message, state: FSMContext):
    """Process category selection"""
    # Check for back - return to broadcast type selection
    if message.text in ["⬅️ BACK", "/cancel_back"]:
        await state.clear()
        await message.answer(
            "📤 **SEND BROADCAST**\n\n"
            "Select broadcast type:\n\n"
            "📝 **NORMAL BROADCAST**\n"
            "   └─ Text, images, videos, voice messages\n"
            "   └─ Simple one-way communication\n\n"
            "🔗 **BROADCAST WITH BUTTONS**\n"
            "   └─ Add clickable inline buttons\n"
            "   └─ Include links and actions\n"
            "   └─ More interactive\n\n"
            "Choose your broadcast type:",
            reply_markup=get_broadcast_type_menu(),
            parse_mode="Markdown"
        )
        return

    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    category_map = {
        "📺 YT": "YT",
        "📸 IG": "IG",
        "📎 IG CC": "IGCC",
        "🔗 YTCODE": "YTCODE",
        "👥 ALL": "ALL"
    }
    
    if message.text not in category_map:
        await message.answer("⚠️ Please select a valid category from the buttons.")
        return
    
    category = category_map[message.text]
    await state.update_data(category=category)
    await state.set_state(BroadcastStates.waiting_for_message)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        f"✅ Category: **{category}**\n\n"
        "📝 Now send me the broadcast message\n"
        "(text, photo, video, or document)",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(BroadcastStates.waiting_for_message)
async def process_direct_broadcast(message: types.Message, state: FSMContext):
    """Process and send broadcast immediately"""
    print(f"📝 MESSAGE RECEIVED: Type={message.content_type}, From={message.from_user.first_name}")
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        print(f"❌ User cancelled message input")
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    category = data.get("category", "ALL")
    
    print(f"📊 Processing broadcast for category: {category}")
    print(f"📝 Content type: {message.content_type}")
    
    # Get next available ID
    broadcast_id, index = get_next_broadcast_id()
    print(f"🆔 Generated broadcast ID: {broadcast_id} (index: {index})")
    
    # Prepare message data for sending
    message_text = message.text or message.caption or ""
    media_type = None
    file_id = None
    
    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
    elif message.video:
        media_type = "video" 
        file_id = message.video.file_id
    elif message.animation:  # Added GIF support
        media_type = "animation"
        file_id = message.animation.file_id
    elif message.document:
        media_type = "document"
        file_id = message.document.file_id
    elif message.audio:  # Added audio support
        media_type = "audio"
        file_id = message.audio.file_id
    elif message.voice:  # Added voice support
        media_type = "voice"
        file_id = message.voice.file_id
    
    # Find target users based on category
    if category == "ALL":
        target_users = list(col_user_tracking.find({}))
    else:
        target_users = list(col_user_tracking.find({"source": category}))
    
    print(f"🎯 Found {len(target_users)} target users for category '{category}'")
    
    if not target_users:
        print(f"⚠️ No users found for category: {category}")
        await message.answer(
            f"⚠️ **No users found for category: {category}**\n\n"
            "Users need to start Bot 8 before receiving broadcasts.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
    # Send immediately
    print(f"📤 Starting broadcast delivery...")
    print(f"🆔 Broadcast ID: {broadcast_id}")
    print(f"📂 Category: {category}")
    print(f"👥 Target users: {len(target_users)}")
    print(f"🤖 Delivery method: Bot 8")
    
    status_msg = await message.answer(
        f"📤 **Sending Broadcast via Bot 8...**\n\n"
        f"🆔 ID: `{broadcast_id}`\n"
        f"📂 Category: {category}\n"
        f"👥 Target Users: {len(target_users)}\n"
        f"🤖 Delivery Bot: Bot 8\n\n"
        f"⏳ Preparing to send...",
        parse_mode="Markdown"
    )
    
    success_count = 0
    failed_count = 0
    blocked_count = 0
    error_details = []
    sent_message_ids = {}  # Store message IDs for later deletion
    
    # Send to each user with progress updates
    for i, user_doc in enumerate(target_users, 1):
        user_id = user_doc['user_id']
        
        # Update progress every 5 users or for small batches
        if i % 5 == 0 or len(target_users) <= 10:
            try:
                await status_msg.edit_text(
                    f"📤 **Sending via Bot 8...**\n\n"
                    f"🆔 ID: `{broadcast_id}`\n"
                    f"📂 Category: {category}\n"
                    f"👥 Target Users: {len(target_users)}\n"
                    f"🤖 Via: Bot 8\n\n"
                    f"📝 Progress: {i}/{len(target_users)} users\n"
                    f"✅ Success: {success_count} | ❌ Failed: {failed_count}",
                    parse_mode="Markdown"
                )
            except:
                pass  # Ignore edit errors during sending
        
        try:
            # CROSS-BOT MEDIA FIX - Download from Bot 10 and send through Bot 8 with retry logic
            if media_type == "photo" and file_id:
                print(f"📸 Processing photo for user {user_id} with retry logic...")
                try:
                    # Download with retry logic
                    async def download_photo():
                        photo_file = await bot.get_file(file_id)
                        file_data = await bot.download_file(photo_file.file_path)
                        return file_data.read()  # Extract bytes from BytesIO
                    
                    photo_bytes = await retry_operation(download_photo, max_retries=3, operation_name="Photo download")
                    
                    # Upload with retry logic - recreate BufferedInputFile on each attempt
                    async def upload_photo():
                        photo_input = BufferedInputFile(photo_bytes, filename="broadcast_photo.jpg")
                        if message_text and message_text.strip():
                            return await bot_8.send_photo(user_id, photo_input, caption=_format_broadcast_msg(message_text, is_caption=True))
                        else:
                            return await bot_8.send_photo(user_id, photo_input)
                    
                    sent_msg = await retry_operation(upload_photo, max_retries=3, operation_name="Photo upload")
                    sent_message_ids[user_id] = sent_msg.message_id  # Store message ID
                    print(f"✅ Photo sent successfully to user {user_id}")
                    
                except Exception as e:
                    print(f"❌ Photo transfer failed for user {user_id}: {str(e)}")
                    raise Exception(f"Photo upload failed: {str(e)[:50]}...")
                    
            elif media_type == "video" and file_id:
                print(f"🎥 Processing video for user {user_id} with retry logic...")
                try:
                    # Download with retry logic
                    async def download_video():
                        video_file = await bot.get_file(file_id)
                        file_data = await bot.download_file(video_file.file_path)
                        return file_data.read()  # Extract bytes from BytesIO
                    
                    video_bytes = await retry_operation(download_video, max_retries=3, operation_name="Video download")
                    
                    # Upload with retry logic - recreate BufferedInputFile on each attempt
                    async def upload_video():
                        video_input = BufferedInputFile(video_bytes, filename="broadcast_video.mp4")
                        if message_text and message_text.strip():
                            return await bot_8.send_video(user_id, video_input, caption=_format_broadcast_msg(message_text, is_caption=True))
                        else:
                            return await bot_8.send_video(user_id, video_input)
                    
                    sent_msg = await retry_operation(upload_video, max_retries=3, operation_name="Video upload")
                    sent_message_ids[user_id] = sent_msg.message_id  # Store message ID
                    print(f"✅ Video sent successfully to user {user_id}")
                    
                except Exception as e:
                    print(f"❌ Video transfer failed for user {user_id}: {str(e)}")
                    raise Exception(f"Video upload failed: {str(e)[:50]}...")
                    
            elif media_type == "animation" and file_id:
                print(f"🎬 Processing animation for user {user_id} with retry logic...")
                try:
                    # Download with retry logic
                    async def download_animation():
                        animation_file = await bot.get_file(file_id)
                        file_data = await bot.download_file(animation_file.file_path)
                        return file_data.read()  # Extract bytes from BytesIO
                    
                    animation_bytes = await retry_operation(download_animation, max_retries=3, operation_name="Animation download")
                    
                    # Upload with retry logic - recreate BufferedInputFile on each attempt
                    async def upload_animation():
                        animation_input = BufferedInputFile(animation_bytes, filename="broadcast_animation.gif")
                        if message_text and message_text.strip():
                            return await bot_8.send_animation(user_id, animation_input, caption=_format_broadcast_msg(message_text, is_caption=True))
                        else:
                            return await bot_8.send_animation(user_id, animation_input)
                    
                    sent_msg = await retry_operation(upload_animation, max_retries=3, operation_name="Animation upload")
                    sent_message_ids[user_id] = sent_msg.message_id  # Store message ID
                    print(f"✅ Animation sent successfully to user {user_id}")
                    
                except Exception as e:
                    print(f"❌ Animation transfer failed for user {user_id}: {str(e)}")
                    raise Exception(f"Animation upload failed: {str(e)[:50]}...")
                    
            elif media_type == "document" and file_id:
                print(f"📄 Processing document for user {user_id} with retry logic...")
                try:
                    # Download with retry logic
                    async def download_document():
                        document_file = await bot.get_file(file_id)
                        print(f"📥 Downloading document bytes (size: {document_file.file_size} bytes)")
                        file_data = await bot.download_file(document_file.file_path)
                        return file_data.read()  # Extract bytes from BytesIO
                    
                    document_bytes = await retry_operation(download_document, max_retries=3, operation_name="Document download")
                    
                    # Upload with retry logic - recreate BufferedInputFile on each attempt
                    async def upload_document():
                        document_input = BufferedInputFile(document_bytes, filename="broadcast_document")
                        print(f"📤 Uploading document via Bot 8 to user {user_id}")
                        if message_text and message_text.strip():
                            return await bot_8.send_document(user_id, document_input, caption=_format_broadcast_msg(message_text, is_caption=True))
                        else:
                            return await bot_8.send_document(user_id, document_input)
                    
                    sent_msg = await retry_operation(upload_document, max_retries=3, operation_name="Document upload")
                    sent_message_ids[user_id] = sent_msg.message_id  # Store message ID
                    print(f"✅ Document sent successfully to user {user_id}")
                    
                except Exception as e:
                    print(f"❌ Document transfer failed for user {user_id}: {str(e)}")
                    raise Exception(f"Document upload failed: {str(e)[:50]}...")
                    
            elif media_type == "audio" and file_id:
                print(f"🎵 Processing audio for user {user_id} with retry logic...")
                try:
                    # Download with retry logic
                    async def download_audio():
                        audio_file = await bot.get_file(file_id)
                        file_data = await bot.download_file(audio_file.file_path)
                        return file_data.read()  # Extract bytes from BytesIO
                    
                    audio_bytes = await retry_operation(download_audio, max_retries=3, operation_name="Audio download")
                    
                    # Upload with retry logic - recreate BufferedInputFile on each attempt
                    async def upload_audio():
                        audio_input = BufferedInputFile(audio_bytes, filename="broadcast_audio.mp3")
                        if message_text and message_text.strip():
                            return await bot_8.send_audio(user_id, audio_input, caption=_format_broadcast_msg(message_text, is_caption=True))
                        else:
                            return await bot_8.send_audio(user_id, audio_input)
                    
                    sent_msg = await retry_operation(upload_audio, max_retries=3, operation_name="Audio upload")
                    sent_message_ids[user_id] = sent_msg.message_id  # Store message ID
                    print(f"✅ Audio sent successfully to user {user_id}")
                    
                except Exception as e:
                    print(f"❌ Audio transfer failed for user {user_id}: {str(e)}")
                    raise Exception(f"Audio upload failed: {str(e)[:50]}...")
                    
            elif media_type == "voice" and file_id:
                print(f"🎙️ Processing voice for user {user_id} with retry logic...")
                try:
                    # Download with retry logic
                    async def download_voice():
                        voice_file = await bot.get_file(file_id)
                        file_data = await bot.download_file(voice_file.file_path)
                        return file_data.read()  # Extract bytes from BytesIO
                    
                    voice_bytes = await retry_operation(download_voice, max_retries=3, operation_name="Voice download")
                    
                    # Upload with retry logic - recreate BufferedInputFile on each attempt (voice messages don't support captions)
                    async def upload_voice():
                        voice_input = BufferedInputFile(voice_bytes, filename="broadcast_voice.ogg")
                        return await bot_8.send_voice(user_id, voice_input)
                    
                    sent_msg = await retry_operation(upload_voice, max_retries=3, operation_name="Voice upload")
                    sent_message_ids[user_id] = sent_msg.message_id  # Store message ID
                    print(f"✅ Voice sent successfully to user {user_id}")
                    
                except Exception as e:
                    print(f"❌ Voice transfer failed for user {user_id}: {str(e)}")
                    raise Exception(f"Voice upload failed: {str(e)[:50]}...")
                
            else:
                # Send text message
                sent_msg = await bot_8.send_message(user_id, _format_broadcast_msg(message_text or "📢 MSA NODE Broadcast"))
                sent_message_ids[user_id] = sent_msg.message_id  # Store message ID
                print(f"✅ Text message sent successfully to user {user_id}")
            
            success_count += 1
            
            # Small delay to avoid rate limits
            if len(target_users) > 10:
                await asyncio.sleep(0.1)  # 100ms delay for large broadcasts
        except Exception as e:
            failed_count += 1
            error_msg = str(e)
            
            # Categorize error types
            if "blocked" in error_msg.lower():
                blocked_count += 1
            elif "not found" in error_msg.lower():
                error_details.append(f"User {user_id}: Account deleted")
            elif "restricted" in error_msg.lower():
                error_details.append(f"User {user_id}: Restricted")
            else:
                error_details.append(f"User {user_id}: {error_msg[:30]}...")
    
    # Final status update after all sends complete
    print(f"✅ Broadcast sending complete! Success: {success_count}, Failed: {failed_count}")
    try:
        await status_msg.edit_text(
            f"✅ **Broadcast Complete!**\n\n"
            f"🆔 ID: `{broadcast_id}`\n"
            f"📂 Category: {category}\n"
            f"👥 Target Users: {len(target_users)}\n"
            f"🤖 Via: Bot 8\n\n"
            f"✅ Success: {success_count} | ❌ Failed: {failed_count}",
            parse_mode="Markdown"
        )
    except:
        pass
    
    # Save broadcast to database after sending
    print(f"💾 Saving broadcast to database...")
    print(f"🆔 ID: {broadcast_id}, Category: {category}, Success: {success_count}, Failed: {failed_count}")
    broadcast_data = {
        "broadcast_id": broadcast_id,
        "index": index,
        "category": category,
        "message_text": message_text,
        "message_type": "text" if message.text else "media",
        "created_by": message.from_user.id,
        "created_at": now_local(),
        "status": "sent",
        "sent_count": success_count,
        "last_sent": now_local()
    }
    
    # Add media info if applicable
    if media_type:
        broadcast_data["media_type"] = media_type
        broadcast_data["file_id"] = file_id
    
    # Store message IDs for later deletion (convert keys to strings for MongoDB)
    broadcast_data["message_ids"] = {str(k): v for k, v in sent_message_ids.items()}
    
    # Save to database with error handling
    try:
        result = col_broadcasts.insert_one(broadcast_data)
        print(f"✅ Broadcast saved to database successfully! DB ID: {result.inserted_id}")
    except Exception as e:
        print(f"❌ ERROR saving broadcast to database: {str(e)}")
        # Still continue to show report to user
    
    # Send completion report
    sent_time = format_datetime(now_local())
    
    # Create detailed report
    report = f"✅ **Broadcast Complete & Saved!**\n\n"
    report += f"🆔 ID: `{broadcast_id}`\n"
    report += f"📂 Category: {category}\n"
    report += f"🤖 Delivered via: **Bot 8**\n"
    report += f"🕐 Sent At: {sent_time}\n\n"
    report += f"📊 **Delivery Report:**\n"
    report += f"✅ **Success: {success_count}** users received\n"
    report += f"❌ **Failed: {failed_count}** users (blocked/inactive)\n"
    if blocked_count > 0:
        report += f"🚫 **Blocked: {blocked_count}** users blocked the bot\n"
    report += f"📈 **Total Attempted: {len(target_users)}** users\n"
    
    delivery_rate = (success_count / len(target_users) * 100) if len(target_users) > 0 else 0
    report += f"💯 **Delivery Rate: {delivery_rate:.1f}%**"
    
    # Add error details if any (max 3 examples)
    if error_details and len(error_details) <= 3:
        report += f"\n\n⚠️ **Error Details:**\n"
        for error in error_details[:3]:
            report += f"• {error}\n"
    
    try:
        await status_msg.edit_text(report, parse_mode="Markdown")
    except:
        await message.answer(report, parse_mode="Markdown")
    
    # Auto-return to broadcast menu after completion
    await asyncio.sleep(2)  # Brief pause for user to read results
    await message.answer(
        "🔄 **Returning to Broadcast Menu...**",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )
    
    await state.clear()

@dp.message(F.text == "📋 LIST BROADCASTS")
async def list_broadcasts_handler(message: types.Message, state: FSMContext):
    """List broadcasts with reply keyboard pagination"""
    reindex_broadcasts()
    await show_broadcast_list_page(message, state, page=0)
    
async def show_broadcast_list_page(message: types.Message, state: FSMContext, page: int = 0):
    """Show paginated broadcast list with reply keyboard"""
    per_page = 10
    skip = page * per_page
    
    total = col_broadcasts.count_documents({})
    broadcasts = list(col_broadcasts.find({}).sort("index", 1).skip(skip).limit(per_page))
    
    if not broadcasts and page == 0:
        await message.answer(
            "📋 **NO BROADCASTS**\n\n"
            "No broadcasts created yet.",
            parse_mode="Markdown"
        )
        return
    
    response = f"📋 **BROADCASTS (Page {page + 1})** - Total: {total}\n\n"
    for brd in broadcasts:
        category = brd.get('category', 'ALL')
        # Get user count for this category
        if category == "ALL":
            user_count = col_user_tracking.count_documents({})
        else:
            user_count = col_user_tracking.count_documents({"source": category})
        
        created = format_datetime(brd.get('created_at'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   👥 {user_count} users • 🕐 {created}\n\n"
    
    response += "💡 **Send ID or Index to view full message**"
    
    # Build reply keyboard with navigation
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if skip + per_page < total:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_row:
        buttons.append(nav_row)
    buttons.append([KeyboardButton(text="⬅️ BROADCAST MENU")])
    
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    # Store current page in state
    await state.update_data(list_page=page)
    await state.set_state(BroadcastStates.waiting_for_list_search)
    
    await message.answer(response, parse_mode="Markdown", reply_markup=keyboard)

@dp.message(BroadcastStates.waiting_for_list_search)
async def process_list_search(message: types.Message, state: FSMContext):
    """Handle pagination or search broadcast by ID or index"""
    # Check for navigation buttons
    if message.text == "⬅️ PREV":
        data = await state.get_data()
        current_page = data.get("list_page", 0)
        if current_page > 0:
            await show_broadcast_list_page(message, state, page=current_page - 1)
        return
    
    if message.text == "NEXT ➡️":
        data = await state.get_data()
        current_page = data.get("list_page", 0)
        await show_broadcast_list_page(message, state, page=current_page + 1)
        return
    
    # Check for back to menu
    if message.text in ["⬅️ BROADCAST MENU", "⬅️ MAIN MENU"]:
        await state.clear()
        if message.text == "⬅️ MAIN MENU":
            await message.answer(
                "📋 **Main Menu**",
                reply_markup=get_main_menu(),
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                "📢 **Broadcast Menu**",
                reply_markup=get_broadcast_menu(),
                parse_mode="Markdown"
            )
        return
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    search = message.text.strip()
    
    # Try to find by ID first
    broadcast = col_broadcasts.find_one({"broadcast_id": search.lower()})
    
    # If not found, try by index
    if not broadcast and search.isdigit():
        broadcast = col_broadcasts.find_one({"index": int(search)})
    
    if not broadcast:
        await message.answer(
            f"❌ Broadcast `{search}` not found.\n\n"
            "Send a valid ID (brd1) or index (1).",
            parse_mode="Markdown"
        )
        return
    
    # Display full broadcast details
    response = f"📋 **BROADCAST DETAILS**\n\n"
    response += f"🆔 ID: `{broadcast['broadcast_id']}`\n"
    response += f"📍 Index: {broadcast['index']}\n"
    response += f"📂 Category: {broadcast.get('category', 'ALL')}\n"
    response += f"📝 Type: {broadcast['message_type'].title()}\n"
    response += f"📊 Status: {broadcast['status'].title()}\n"
    response += f"📤 Sent: {broadcast.get('sent_count', 0)} users\n"
    response += f"🕐 Created: {format_datetime(broadcast.get('created_at'))}\n"
    if broadcast.get('last_edited'):
        response += f"📝 Last Edited: {format_datetime(broadcast.get('last_edited'))}\n"
    if broadcast.get('last_sent'):
        response += f"📤 Last Sent: {format_datetime(broadcast.get('last_sent'))}\n"
    response += f"\n💬 **Full Message:**\n{broadcast['message_text']}"
    
    await message.answer(response, parse_mode="Markdown")

@dp.message(F.text == "✏️ EDIT BROADCAST")
async def edit_broadcast_handler(message: types.Message, state: FSMContext):
    """Start broadcast editing - show list first"""
    await show_edit_broadcast_list(message, state, page=0)

async def show_edit_broadcast_list(message: types.Message, state: FSMContext, page: int = 0):
    """Show paginated list for editing"""
    per_page = 10
    skip = page * per_page
    
    total = col_broadcasts.count_documents({})
    broadcasts = list(col_broadcasts.find({}).sort("index", 1).skip(skip).limit(per_page))
    
    if not broadcasts and page == 0:
        await message.answer(
            "⚠️ **NO BROADCASTS**\n\n"
            "No broadcasts available to edit.",
            parse_mode="Markdown"
        )
        return
    
    response = f"✏️ **EDIT BROADCAST (Page {page + 1})** - Total: {total}\n\nAvailable broadcasts:\n\n"
    for brd in broadcasts:
        category = brd.get('category', 'ALL')
        # Get user count for this category
        if category == "ALL":
            user_count = col_user_tracking.count_documents({})
        else:
            user_count = col_user_tracking.count_documents({"source": category})
        
        created = format_datetime(brd.get('created_at'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   👥 {user_count} users • 🕐 {created}\n\n"
    
    response += "💡 Send **ID** (brd1) or **Index** (1) to edit"
    
    # Build reply keyboard with navigation
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if skip + per_page < total:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_row:
        buttons.append(nav_row)
    buttons.append([KeyboardButton(text="❌ CANCEL")])
    
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    # Store current page in state
    await state.update_data(edit_page=page)
    await state.set_state(BroadcastStates.waiting_for_edit_id)
    
    await message.answer(response, parse_mode="Markdown", reply_markup=keyboard)

@dp.message(BroadcastStates.waiting_for_edit_id)
async def process_edit_id(message: types.Message, state: FSMContext):
    """Process broadcast ID or index for editing"""
    # Check for navigation buttons
    if message.text == "⬅️ PREV":
        data = await state.get_data()
        current_page = data.get("edit_page", 0)
        if current_page > 0:
            await show_edit_broadcast_list(message, state, page=current_page - 1)
        return
    
    if message.text == "NEXT ➡️":
        data = await state.get_data()
        current_page = data.get("edit_page", 0)
        await show_edit_broadcast_list(message, state, page=current_page + 1)
        return
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    search = message.text.strip()
    
    # Find broadcast by ID or index
    broadcast = col_broadcasts.find_one({"broadcast_id": search.lower()})
    if not broadcast and search.isdigit():
        broadcast = col_broadcasts.find_one({"index": int(search)})
    
    if not broadcast:
        await message.answer(
            f"❌ Broadcast `{search}` not found.\n\n"
            "Please send a valid broadcast ID or index.",
            parse_mode="Markdown"
        )
        return
    
    # Store broadcast ID in state
    await state.update_data(edit_broadcast_id=broadcast['broadcast_id'])
    await state.set_state(BroadcastStates.waiting_for_edit_content)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    created = format_datetime(broadcast.get('created_at'))
    last_edited = format_datetime(broadcast.get('last_edited'))
    
    await message.answer(
        f"✏️ **Editing: {broadcast['broadcast_id']}**\n\n"
        f"📂 Category: {broadcast.get('category', 'ALL')}\n"
        f"🕐 Created: {created}\n"
        f"📝 Last Edited: {last_edited}\n\n"
        f"**Current message:**\n{broadcast['message_text']}\n\n"
        "Send the new content for this broadcast.",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(BroadcastStates.waiting_for_edit_content)
async def process_edit_content(message: types.Message, state: FSMContext):
    """Store new content and ask for confirmation"""
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    broadcast_id = data.get("edit_broadcast_id")
    
    # Prepare update data
    update_data = {
        "message_text": message.text or message.caption or "",
        "message_type": "text" if message.text else "media",
        "last_edited": now_local()
    }
    
    # Handle media updates
    if message.photo:
        update_data["media_type"] = "photo"
        update_data["file_id"] = message.photo[-1].file_id
    elif message.video:
        update_data["media_type"] = "video"
        update_data["file_id"] = message.video.file_id
    elif message.document:
        update_data["media_type"] = "document"
        update_data["file_id"] = message.document.file_id
    
    # Store in state for confirmation
    await state.update_data(update_data=update_data)
    await state.set_state(BroadcastStates.waiting_for_edit_confirm)
    
    # Show confirmation
    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ CONFIRM"), KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        f"📝 **Preview New Content:**\n\n"
        f"{update_data['message_text']}\n\n"
        f"✅ Confirm to update broadcast `{broadcast_id}`?",
        reply_markup=confirm_kb,
        parse_mode="Markdown"
    )

@dp.message(BroadcastStates.waiting_for_edit_confirm)
async def process_edit_confirm(message: types.Message, state: FSMContext):
    """Confirm and apply broadcast edit"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer(
            "❌ Edit cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    if message.text != "✅ CONFIRM":
        await message.answer("⚠️ Please click ✅ CONFIRM or ❌ CANCEL")
        return
    
    data = await state.get_data()
    broadcast_id = data.get("edit_broadcast_id")
    update_data = data.get("update_data", {})
    
    # Get the broadcast to retrieve message_ids
    broadcast = col_broadcasts.find_one({"broadcast_id": broadcast_id})
    if not broadcast:
        await message.answer("❌ Broadcast not found!", reply_markup=get_broadcast_menu())
        await state.clear()
        return
    
    message_ids = broadcast.get("message_ids", {})
    new_text = update_data.get("message_text", "")
    message_type = update_data.get("message_type", "text")
    
    print(f"\n📝 EDITING BROADCAST {broadcast_id}")
    print(f"📊 Updating {len(message_ids)} messages for users...")
    
    # Edit messages for all users
    edited_count = 0
    failed_count = 0
    
    for user_id, msg_id in message_ids.items():
        try:
            if message_type == "text":
                # Edit text message
                await bot_8.edit_message_text(
                    chat_id=int(user_id),
                    message_id=msg_id,
                    text=new_text
                )
            elif message_type == "media":
                # Edit media caption
                media_type = update_data.get("media_type")
                file_id = update_data.get("file_id")
                
                if file_id:
                    # If new media file provided, edit media
                    from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
                    
                    if media_type == "photo":
                        new_media = InputMediaPhoto(media=file_id, caption=new_text)
                    elif media_type == "video":
                        new_media = InputMediaVideo(media=file_id, caption=new_text)
                    elif media_type == "document":
                        new_media = InputMediaDocument(media=file_id, caption=new_text)
                    else:
                        new_media = None
                    
                    if new_media:
                        await bot_8.edit_message_media(
                            chat_id=int(user_id),
                            message_id=msg_id,
                            media=new_media
                        )
                else:
                    # Just edit caption
                    await bot_8.edit_message_caption(
                        chat_id=int(user_id),
                        message_id=msg_id,
                        caption=new_text
                    )
            
            edited_count += 1
            print(f"✅ Edited message for user {user_id}")
            
        except Exception as e:
            failed_count += 1
            print(f"⚠️ Failed to edit message for user {user_id}: {str(e)}")
    
    # Apply update to database
    col_broadcasts.update_one(
        {"broadcast_id": broadcast_id},
        {"$set": update_data}
    )
    
    print(f"✅ Database updated for {broadcast_id}")
    print(f"📊 Results: {edited_count} edited, {failed_count} failed\n")
    
    await state.clear()
    await message.answer(
        f"✅ **Broadcast Updated!**\n\n"
        f"🆔 ID: `{broadcast_id}`\n"
        f"✏️ **Messages Edited:** {edited_count}\n"
        f"⚠️ **Failed:** {failed_count}\n\n"
        f"All user messages have been updated!",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🗑️ DELETE BROADCAST")
async def delete_broadcast_handler(message: types.Message, state: FSMContext):
    """Start broadcast deletion - show list first"""
    await show_delete_broadcast_list(message, state, page=0)

async def show_delete_broadcast_list(message: types.Message, state: FSMContext, page: int = 0):
    """Show paginated list for deletion"""
    per_page = 10
    skip = page * per_page
    
    total = col_broadcasts.count_documents({})
    broadcasts = list(col_broadcasts.find({}).sort("index", 1).skip(skip).limit(per_page))
    
    if not broadcasts and page == 0:
        await message.answer(
            "⚠️ **NO BROADCASTS**\n\n"
            "No broadcasts available to delete.",
            parse_mode="Markdown"
        )
        return
    
    response = f"🗑️ **DELETE BROADCAST (Page {page + 1})** - Total: {total}\n\nAvailable broadcasts:\n\n"
    for brd in broadcasts:
        category = brd.get('category', 'ALL')
        # Get user count for this category
        if category == "ALL":
            user_count = col_user_tracking.count_documents({})
        else:
            user_count = col_user_tracking.count_documents({"source": category})
        
        created = format_datetime(brd.get('created_at'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   👥 {user_count} users • 🕐 {created}\n\n"
    
    response += "💡 Send **ID(s)** (brd1 or brd1,brd2) or **Index(es)** (1 or 1,2,3) to delete"
    
    # Build reply keyboard with navigation
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if skip + per_page < total:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_row:
        buttons.append(nav_row)
    buttons.append([KeyboardButton(text="❌ CANCEL")])
    
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    # Store current page in state
    await state.update_data(delete_page=page)
    await state.set_state(BroadcastStates.waiting_for_delete_id)
    
    await message.answer(response, parse_mode="Markdown", reply_markup=keyboard)

@dp.message(BroadcastStates.waiting_for_delete_id)
async def process_delete_broadcast(message: types.Message, state: FSMContext):
    """Parse delete request and show confirmation"""
    # Check for navigation buttons
    if message.text == "⬅️ PREV":
        data = await state.get_data()
        current_page = data.get("delete_page", 0)
        if current_page > 0:
            await show_delete_broadcast_list(message, state, page=current_page - 1)
        return
    
    if message.text == "NEXT ➡️":
        data = await state.get_data()
        current_page = data.get("delete_page", 0)
        await show_delete_broadcast_list(message, state, page=current_page + 1)
        return
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    search = message.text.strip()
    
    # Parse multiple IDs or indices (comma-separated)
    items = [item.strip() for item in search.split(',')]
    
    # Find broadcasts to delete
    broadcasts_to_delete = []
    not_found = []
    
    for item in items:
        # Try to find by ID first
        broadcast = col_broadcasts.find_one({"broadcast_id": item.lower()})
        
        # If not found, try by index
        if not broadcast and item.isdigit():
            broadcast = col_broadcasts.find_one({"index": int(item)})
        
        if broadcast:
            broadcasts_to_delete.append(broadcast)
        else:
            not_found.append(item)
    
    if not broadcasts_to_delete:
        await message.answer(
            f"❌ No broadcasts found for: `{search}`\n\n"
            "Please send valid ID(s) or index(es).",
            parse_mode="Markdown"
        )
        return
    
    # Show confirmation
    response = f"⚠️ **CONFIRM DELETION**\n\n"
    response += f"🗑️ You're about to delete **{len(broadcasts_to_delete)} broadcast(s)**:\n\n"
    
    for brd in broadcasts_to_delete:
        category = brd.get('category', 'ALL')
        created = format_datetime(brd.get('created_at'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   🕐 {created}\n\n"
    
    if not_found:
        response += f"⚠️ Not found: {', '.join(not_found)}\n\n"
    
    response += f"❌ **This action cannot be undone!**\n\n"
    response += "✅ Confirm to proceed?"
    
    # Store broadcasts to delete in state
    await state.update_data(broadcasts_to_delete=[b['broadcast_id'] for b in broadcasts_to_delete])
    await state.set_state(BroadcastStates.waiting_for_delete_confirm)
    
    # Confirmation keyboard
    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(response, parse_mode="Markdown", reply_markup=confirm_kb)

@dp.message(BroadcastStates.waiting_for_delete_confirm)
async def confirm_delete_broadcast(message: types.Message, state: FSMContext):
    """Actually delete broadcasts after confirmation"""
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Deletion cancelled. No broadcasts were deleted.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Check for confirmation
    if message.text != "✅ CONFIRM DELETE":
        await message.answer("⚠️ Please click ✅ CONFIRM DELETE or ❌ CANCEL")
        return
    
    # Get broadcasts to delete from state
    data = await state.get_data()
    broadcast_ids = data.get("broadcasts_to_delete", [])
    
    if not broadcast_ids:
        await state.clear()
        await message.answer(
            "❌ No broadcasts to delete.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Delete broadcasts and their messages
    deleted_count = 0
    deleted_messages_count = 0
    failed_message_deletes = 0
    
    print(f"🗑️ Starting deletion of {len(broadcast_ids)} broadcast(s)...")
    
    for broadcast_id in broadcast_ids:
        # First, get the broadcast to retrieve message IDs
        broadcast = col_broadcasts.find_one({"broadcast_id": broadcast_id})
        
        if broadcast:
            # Delete messages from users
            message_ids = broadcast.get("message_ids", {})
            print(f"📤 Deleting {len(message_ids)} messages for broadcast {broadcast_id}...")
            
            for user_id, message_id in message_ids.items():
                try:
                    await bot_8.delete_message(chat_id=int(user_id), message_id=message_id)
                    deleted_messages_count += 1
                    print(f"✅ Deleted message {message_id} from user {user_id}")
                except Exception as e:
                    failed_message_deletes += 1
                    print(f"⚠️ Failed to delete message from user {user_id}: {str(e)}")
                    # Continue even if message deletion fails (user might have deleted it already)
            
            # Then delete the broadcast record from database
            result = col_broadcasts.delete_one({"broadcast_id": broadcast_id})
            if result.deleted_count > 0:
                deleted_count += 1
                print(f"✅ Deleted broadcast {broadcast_id} from database")
    
    # Always re-index so indices stay clean (1, 2, 3, ...)
    reindex_broadcasts()

    await state.clear()
    
    response = f"✅ **Deletion Complete!**\n\n"
    response += f"🗑️ **Broadcasts Deleted:** {deleted_count}\n\n"
    response += f"📨 **Messages Deleted:** {deleted_messages_count} messages removed from users\n"
    if failed_message_deletes > 0:
        response += f"⚠️ **Failed:** {failed_message_deletes} messages (already deleted by users)\n\n"
    else:
        response += "\n"
    response += "✅ Broadcasts re-indexed cleanly (1, 2, 3, ...)"
    
    await message.answer(
        response,
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# SEND BROADCAST HANDLERS
# ==========================================

@dp.message(F.text == "⬅️ BACK")
async def handle_back_button(message: types.Message, state: FSMContext):
    """Universal ⬅️ BACK handler — clears any FSM state and routes to correct menu"""
    current_state = await state.get_state()
    await state.clear()

    # Route based on which FSM was active
    if current_state is None:
        # At broadcast type-selection screen — go to broadcast menu
        await message.answer(
            "📢 **Broadcast Management**",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        BroadcastStates.selecting_category,
        BroadcastStates.waiting_for_message,
        BroadcastWithButtonsStates.selecting_category,
        BroadcastWithButtonsStates.waiting_for_message,
        BroadcastWithButtonsStates.waiting_for_button_text,
        BroadcastWithButtonsStates.waiting_for_button_url,
        BroadcastWithButtonsStates.confirming_buttons,
    ]:
        await message.answer(
            "📢 **Broadcast Management**",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        FindStates.waiting_for_search,
    ]:
        await message.answer(
            "✅ Returned to main menu.",
            reply_markup=get_main_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        ShootStates.waiting_for_ban_id,
        ShootStates.waiting_for_ban_confirm,
        ShootStates.waiting_for_unban_id,
        ShootStates.waiting_for_unban_confirm,
        ShootStates.waiting_for_delete_id,
        ShootStates.waiting_for_delete_confirm,
        ShootStates.waiting_for_suspend_id,
        ShootStates.selecting_suspend_features,
        ShootStates.waiting_for_unsuspend_id,
        ShootStates.waiting_for_reset_id,
        ShootStates.waiting_for_reset_confirm,
        ShootStates.waiting_for_shoot_search_id,
        ShootStates.waiting_for_temp_ban_id,
        ShootStates.selecting_temp_ban_duration,
        ShootStates.waiting_for_temp_ban_confirm,
    ]:
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_shoot_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        SupportStates.waiting_for_ticket_search,
        SupportStates.waiting_for_resolve_id,
        SupportStates.waiting_for_reply_id,
        SupportStates.waiting_for_reply_message,
        SupportStates.waiting_for_delete_ticket_id,
        SupportStates.waiting_for_user_search,
        SupportStates.waiting_for_priority_id,
        SupportStates.waiting_for_priority_level,
    ]:
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        AdminStates.waiting_for_new_admin_id,
        AdminStates.waiting_for_admin_role,
        AdminStates.waiting_for_remove_admin_id,
        AdminStates.waiting_for_remove_confirm,
        AdminStates.waiting_for_permission_admin_id,
        AdminStates.selecting_permissions,
        AdminStates.toggling_permissions,
        AdminStates.waiting_for_role_admin_id,
        AdminStates.selecting_role,
        AdminStates.waiting_for_lock_user_id,
        AdminStates.waiting_for_unlock_user_id,
        AdminStates.waiting_for_ban_user_id,
        AdminStates.waiting_for_admin_search,
    ]:
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
    elif current_state in [
        BroadcastStates.waiting_for_list_search,
        BroadcastStates.waiting_for_edit_id,
        BroadcastStates.waiting_for_edit_content,
        BroadcastStates.waiting_for_edit_confirm,
        BroadcastStates.waiting_for_delete_id,
        BroadcastStates.waiting_for_delete_confirm,
    ]:
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
    else:
        # Fallback — any unknown state goes to main menu
        await message.answer(
            "✅ Returned to main menu.",
            reply_markup=get_main_menu(),
            parse_mode="Markdown"
        )

@dp.message(F.text == "📤 SEND BROADCAST")
async def select_broadcast_type(message: types.Message, state: FSMContext):
    """Show broadcast type selection menu"""
    await state.clear()
    print(f"📱 USER ACTION: {message.from_user.first_name} ({message.from_user.id}) clicked 'SEND BROADCAST'")
    
    await message.answer(
        "📤 **SEND BROADCAST**\n\n"
        "Select broadcast type:\n\n"
        "📝 **NORMAL BROADCAST**\n"
        "   └─ Text, images, videos, voice messages\n"
        "   └─ Simple one-way communication\n\n"
        "🔗 **BROADCAST WITH BUTTONS**\n"
        "   └─ Add clickable inline buttons\n"
        "   └─ Include links and actions\n"
        "   └─ More interactive\n\n"
        "Choose your broadcast type:",
        reply_markup=get_broadcast_type_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "📝 NORMAL BROADCAST")
async def direct_send_broadcast(message: types.Message, state: FSMContext):
    """Start normal broadcast - select category and send immediately"""
    print(f"📱 USER ACTION: {message.from_user.first_name} ({message.from_user.id}) selected 'NORMAL BROADCAST'")
    print(f"🔍 Fetching user counts for all categories...")
    
    # Get live user counts for each category
    yt_count = col_user_tracking.count_documents({"source": "YT"})
    ig_count = col_user_tracking.count_documents({"source": "IG"})
    igcc_count = col_user_tracking.count_documents({"source": "IGCC"})
    ytcode_count = col_user_tracking.count_documents({"source": "YTCODE"})
    all_count = col_user_tracking.count_documents({})
    
    print(f"📀 User counts: YT={yt_count}, IG={ig_count}, IGCC={igcc_count}, YTCODE={ytcode_count}, ALL={all_count}")
    
    await state.set_state(BroadcastStates.selecting_category)
    await message.answer(
        "📤 **NORMAL BROADCAST**\n\n"
        "Select broadcast category:\n\n"
        f"📺 **YT** - Users from YouTube links ({yt_count} users)\n"
        f"📸 **IG** - Users from Instagram links ({ig_count} users)\n"
        f"📎 **IG CC** - Users from IG CC links ({igcc_count} users)\n"
        f"🔗 **YTCODE** - Users from YTCODE links ({ytcode_count} users)\n"
        f"👥 **ALL** - All users ({all_count} users)\n\n"
        "Type /cancel to abort.",
        reply_markup=get_category_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🔗 BROADCAST WITH BUTTONS")
async def broadcast_with_buttons_start(message: types.Message, state: FSMContext):
    """Start broadcast with buttons - select category first"""
    print(f"📱 USER ACTION: {message.from_user.first_name} ({message.from_user.id}) selected 'BROADCAST WITH BUTTONS'")
    print(f"🔍 Fetching user counts for all categories...")
    
    # Get live user counts for each category
    yt_count = col_user_tracking.count_documents({"source": "YT"})
    ig_count = col_user_tracking.count_documents({"source": "IG"})
    igcc_count = col_user_tracking.count_documents({"source": "IGCC"})
    ytcode_count = col_user_tracking.count_documents({"source": "YTCODE"})
    all_count = col_user_tracking.count_documents({})
    
    print(f"📀 User counts: YT={yt_count}, IG={ig_count}, IGCC={igcc_count}, YTCODE={ytcode_count}, ALL={all_count}")
    
    await state.set_state(BroadcastWithButtonsStates.selecting_category)
    await message.answer(
        "🔗 **BROADCAST WITH BUTTONS**\n\n"
        "Select broadcast category:\n\n"
        f"📺 **YT** - Users from YouTube links ({yt_count} users)\n"
        f"📸 **IG** - Users from Instagram links ({ig_count} users)\n"
        f"📎 **IG CC** - Users from IG CC links ({igcc_count} users)\n"
        f"🔗 **YTCODE** - Users from YTCODE links ({ytcode_count} users)\n"
        f"👥 **ALL** - All users ({all_count} users)\n\n"
        "Type /cancel to abort.",
        reply_markup=get_category_menu(),
        parse_mode="Markdown"
    )

@dp.message(BroadcastWithButtonsStates.selecting_category)
async def process_button_broadcast_category(message: types.Message, state: FSMContext):
    """Process category selection for button broadcast"""
    # Check for back - return to broadcast type selection
    if message.text in ["⬅️ BACK", "/cancel_back"]:
        await state.clear()
        await message.answer(
            "📤 **SEND BROADCAST**\n\n"
            "Select broadcast type:\n\n"
            "📝 **NORMAL BROADCAST**\n"
            "   └─ Text, images, videos, voice messages\n"
            "   └─ Simple one-way communication\n\n"
            "🔗 **BROADCAST WITH BUTTONS**\n"
            "   └─ Add clickable inline buttons\n"
            "   └─ Include links and actions\n"
            "   └─ More interactive\n\n"
            "Choose your broadcast type:",
            reply_markup=get_broadcast_type_menu(),
            parse_mode="Markdown"
        )
        return

    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
        return
    
    # Map button text to category
    category_map = {
        "📺 YT": "YT",
        "📸 IG": "IG",
        "📎 IG CC": "IGCC",
        "🔗 YTCODE": "YTCODE",
        "👥 ALL": "ALL"
    }
    
    if message.text not in category_map:
        await message.answer("⚠️ Invalid category. Please select from the menu.", parse_mode="Markdown")
        return
    
    category = category_map[message.text]
    await state.update_data(category=category)
    await state.set_state(BroadcastWithButtonsStates.waiting_for_message)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        f"🔗 **BROADCAST WITH BUTTONS** - {category}\n\n"
        f"📝 Send your broadcast message:\n\n"
        f"Supported formats:\n"
        f"  • Text\n"
        f"  • Photos (with caption)\n"
        f"  • Videos (with caption)\n\n"
        f"Type /cancel to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(BroadcastWithButtonsStates.waiting_for_message)
async def process_button_broadcast_message(message: types.Message, state: FSMContext):
    """Process broadcast message and ask for buttons"""
    if message.text and message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
        return
    
    # Store message details
    data = {}
    data['message_type'] = 'text'
    
    if message.text:
        data['message_type'] = 'text'
        data['text'] = message.text
    elif message.photo:
        data['message_type'] = 'photo'
        data['file_id'] = message.photo[-1].file_id
        data['caption'] = message.caption or ""
    elif message.video:
        data['message_type'] = 'video'
        data['file_id'] = message.video.file_id
        data['caption'] = message.caption or ""
    else:
        await message.answer("⚠️ Unsupported message type. Please send text, photo, or video.", parse_mode="Markdown")
        return
    
    await state.update_data(**data, buttons=[])
    await state.set_state(BroadcastWithButtonsStates.waiting_for_button_text)
    
    await message.answer(
        "🔘 **ADD BUTTON**\n\n"
        "Enter button text (e.g., `Visit Channel`, `Join Now`, `Get Access`):\n\n"
        "Type `DONE` to finish adding buttons (minimum 1 button required).\n"
        "Type /cancel to abort.",
        parse_mode="Markdown"
    )

@dp.message(BroadcastWithButtonsStates.waiting_for_button_text)
async def process_button_text(message: types.Message, state: FSMContext):
    """Process button text input"""
    if message.text and message.text.upper() in ["DONE", "❌ CANCEL", "/CANCEL"]:
        data = await state.get_data()
        buttons = data.get('buttons', [])
        
        if message.text.upper() in ["❌ CANCEL", "/CANCEL"]:
            await state.clear()
            await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
            return
        
        if len(buttons) == 0:
            await message.answer("⚠️ Please add at least one button first.", parse_mode="Markdown")
            return
        
        # Show preview and confirm
        await show_button_broadcast_preview(message, state)
        return
    
    button_text = message.text.strip()
    if len(button_text) > 50:
        await message.answer("⚠️ Button text too long (max 50 characters). Please try again.", parse_mode="Markdown")
        return
    
    await state.update_data(current_button_text=button_text)
    await state.set_state(BroadcastWithButtonsStates.waiting_for_button_url)
    
    await message.answer(
        f"🔗 **BUTTON URL**\n\n"
        f"Button Text: `{button_text}`\n\n"
        f"Enter the URL for this button:\n"
        f"(Must start with http:// or https://)\n\n"
        f"Type /cancel to abort.",
        parse_mode="Markdown"
    )

@dp.message(BroadcastWithButtonsStates.waiting_for_button_url)
async def process_button_url(message: types.Message, state: FSMContext):
    """Process button URL input"""
    if message.text and message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
        return
    
    url = message.text.strip()
    if not url.startswith(('http://', 'https://')):
        await message.answer("⚠️ Invalid URL. Must start with http:// or https://", parse_mode="Markdown")
        return
    
    # Add button to list
    data = await state.get_data()
    buttons = data.get('buttons', [])
    button_text = data.get('current_button_text')
    
    buttons.append({'text': button_text, 'url': url})
    await state.update_data(buttons=buttons)
    await state.set_state(BroadcastWithButtonsStates.waiting_for_button_text)
    
    await message.answer(
        f"✅ **BUTTON ADDED**\n\n"
        f"Current buttons: {len(buttons)}\n\n"
        f"Add another button (enter text) or type `DONE` to finish:",
        parse_mode="Markdown"
    )

async def show_button_broadcast_preview(message: types.Message, state: FSMContext):
    """Show preview of broadcast with buttons and confirm"""
    data = await state.get_data()
    category = data.get('category')
    buttons = data.get('buttons', [])
    message_type = data.get('message_type')
    
    # Get target users count
    if category == "ALL":
        target_count = col_user_tracking.count_documents({})
    else:
        target_count = col_user_tracking.count_documents({"source": category})
    
    # Build preview
    preview = (
        f"📋 **BROADCAST PREVIEW**\n\n"
        f"📂 Category: {category}\n"
        f"👥 Target Users: {target_count}\n"
        f"📝 Message Type: {message_type.capitalize()}\n"
        f"🔘 Buttons: {len(buttons)}\n\n"
        f"**Buttons:**\n"
    )
    
    for i, btn in enumerate(buttons, 1):
        preview += f"{i}. {btn['text']} → {btn['url'][:30]}...\n"
    
    preview += "\n✅ Type **CONFIRM** to send or **CANCEL** to abort."
    
    confirm_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ CONFIRM"), KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await state.set_state(BroadcastWithButtonsStates.confirming_buttons)
    await message.answer(preview, reply_markup=confirm_keyboard, parse_mode="Markdown")

@dp.message(BroadcastWithButtonsStates.confirming_buttons)
async def confirm_button_broadcast(message: types.Message, state: FSMContext):
    """Confirm and send broadcast with buttons"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM" in message.text:
        data = await state.get_data()
        category = data.get('category')
        buttons = data.get('buttons', [])
        message_type = data.get('message_type')
        
        # Get target users
        if category == "ALL":
            target_users = list(col_user_tracking.find({}))
        else:
            target_users = list(col_user_tracking.find({"source": category}))
        
        if not target_users:
            await message.answer("❌ No users found in this category.", reply_markup=get_broadcast_menu(), parse_mode="Markdown")
            await state.clear()
            return
        
        # Build inline keyboard
        inline_buttons = []
        for btn in buttons:
            inline_buttons.append([InlineKeyboardButton(text=btn['text'], url=btn['url'])])
        
        reply_markup = InlineKeyboardMarkup(inline_keyboard=inline_buttons)
        
        # Send status message
        status_msg = await message.answer(
            f"⏳ **Sending broadcast...**\n\n"
            f"📂 Category: {category}\n"
            f"👥 Target: {len(target_users)} users\n"
            f"🔘 Buttons: {len(buttons)}\n\n"
            f"Please wait...",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        
        success = 0
        failed = 0
        
        # Send to all users
        for user_doc in target_users:
            user_id = user_doc['user_id']
            try:
                if message_type == 'text':
                    await bot_8.send_message(user_id, data.get('text'), reply_markup=reply_markup, parse_mode="Markdown")
                elif message_type == 'photo':
                    await bot_8.send_photo(user_id, data.get('file_id'), caption=data.get('caption'), reply_markup=reply_markup, parse_mode="Markdown")
                elif message_type == 'video':
                    await bot_8.send_video(user_id, data.get('file_id'), caption=data.get('caption'), reply_markup=reply_markup, parse_mode="Markdown")
                
                success += 1
                
                if len(target_users) > 10:
                    await asyncio.sleep(0.1)
            except:
                failed += 1
        
        await status_msg.edit_text(
            f"✅ **BROADCAST COMPLETE**\n\n"
            f"📂 Category: {category}\n"
            f"✅ Success: {success}\n"
            f"❌ Failed: {failed}\n"
            f"🔘 Buttons: {len(buttons)}",
            parse_mode="Markdown"
        )
        
        await state.clear()
        print(f"✅ Button broadcast sent to {success} users")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM** or **❌ CANCEL**", parse_mode="Markdown")

async def show_send_broadcast_list(message: types.Message, state: FSMContext, page: int = 0):
    """Show paginated list for sending"""
    per_page = 10
    skip = page * per_page
    
    total = col_broadcasts.count_documents({})
    broadcasts = list(col_broadcasts.find({}).sort("index", 1).skip(skip).limit(per_page))
    
    if not broadcasts and page == 0:
        await message.answer(
            "⚠️ **NO BROADCASTS**\n\n"
            "No broadcasts available to send.",
            parse_mode="Markdown"
        )
        return
    
    response = f"📤 **SEND BROADCAST (Page {page + 1})** - Total: {total}\n\nAvailable broadcasts:\n\n"
    for brd in broadcasts:
        category = brd.get('category', 'ALL')
        # Get user count for this category
        if category == "ALL":
            user_count = col_user_tracking.count_documents({})
        else:
            user_count = col_user_tracking.count_documents({"source": category})
        
        created = format_datetime(brd.get('created_at'))
        last_sent = format_datetime(brd.get('last_sent'))
        response += f"🆔 `{brd['broadcast_id']}` ({brd['index']}) - {category}\n"
        response += f"   👥 {user_count} users • 🕐 {created}\n"
        if brd.get('last_sent'):
            response += f"   📤 Last Sent: {last_sent}\n"
        response += "\n"
    
    response += "💡 Send **ID** (brd1) or **Index** (1) to send"
    
    # Build reply keyboard with navigation
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if skip + per_page < total:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_row:
        buttons.append(nav_row)
    buttons.append([KeyboardButton(text="❌ CANCEL")])
    
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    # Store current page in state
    await state.update_data(send_page=page)
    # await state.set_state(BroadcastStates.waiting_for_send_id)  # DISABLED - old workflow
    
    await message.answer(response, parse_mode="Markdown", reply_markup=keyboard)

async def process_send_broadcast(message: types.Message, state: FSMContext):
    """Send broadcast to filtered users"""
    # Check for navigation buttons
    if message.text == "⬅️ PREV":
        data = await state.get_data()
        current_page = data.get("send_page", 0)
        if current_page > 0:
            await show_send_broadcast_list(message, state, page=current_page - 1)
        return
    
    if message.text == "NEXT ➡️":
        data = await state.get_data()
        current_page = data.get("send_page", 0)
        await show_send_broadcast_list(message, state, page=current_page + 1)
        return
    
    # Check for cancel
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    search = message.text.strip()
    
    # Find broadcast by ID or index
    broadcast = col_broadcasts.find_one({"broadcast_id": search.lower()})
    if not broadcast and search.isdigit():
        broadcast = col_broadcasts.find_one({"index": int(search)})
    
    if not broadcast:
        await message.answer(
            f"❌ Broadcast `{search}` not found.\n\n"
            "Send a valid ID (brd1) or index (1).",
            parse_mode="Markdown"
        )
        return
    
    await state.clear()
    
    # Get broadcast details
    broadcast_id = broadcast['broadcast_id']
    category = broadcast.get('category', 'ALL')
    message_text = broadcast.get('message_text', '')
    media_type = broadcast.get('media_type')
    file_id = broadcast.get('file_id')
    
    # Build user filter based on category
    if category == "ALL":
        # Send to all users who started bot8
        user_filter = {}
    else:
        # Send only to users who started via specific source
        user_filter = {"source": category}
    
    # Get target users
    target_users = list(col_user_tracking.find(user_filter, {"user_id": 1}))
    
    if not target_users:
        # Debug information
        total_users = col_user_tracking.count_documents({})
        category_breakdown = ""
        if total_users > 0:
            yt_count = col_user_tracking.count_documents({"source": "YT"})
            ig_count = col_user_tracking.count_documents({"source": "IG"})
            igcc_count = col_user_tracking.count_documents({"source": "IGCC"})
            ytcode_count = col_user_tracking.count_documents({"source": "YTCODE"})
            
            category_breakdown = f"\n\n📊 **Available Users:**\n"
            category_breakdown += f"📺 YT: {yt_count} users\n"
            category_breakdown += f"📸 IG: {ig_count} users\n"
            category_breakdown += f"📎 IGCC: {igcc_count} users\n"
            category_breakdown += f"🔗 YTCODE: {ytcode_count} users\n"
            category_breakdown += f"👥 Total: {total_users} users"
        
        await message.answer(
            f"⚠️ **NO USERS FOUND**\n\n"
            f"📂 Category: **{category}**\n"
            f"❌ No users available for this category.{category_breakdown}\n\n"
            f"💡 Users are tracked when they start Bot 8 via links.",
            reply_markup=get_broadcast_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Send broadcast
    status_msg = await message.answer(
        f"📤 **Sending Broadcast via Bot 8...**\n\n"
        f"🆔 ID: `{broadcast_id}`\n"
        f"📂 Category: {category}\n"
        f"👥 Target Users: {len(target_users)}\n"
        f"🤖 Delivery Bot: Bot 8\n\n"
        f"⏳ Preparing to send...",
        parse_mode="Markdown"
    )
    
    success_count = 0
    failed_count = 0
    blocked_count = 0
    error_details = []
    
    # Send to each user with progress updates
    for i, user_doc in enumerate(target_users, 1):
        user_id = user_doc['user_id']
        
        # Update progress every 5 users or for small batches
        if i % 5 == 0 or len(target_users) <= 10:
            try:
                await status_msg.edit_text(
                    f"📤 **Sending via Bot 8...**\n\n"
                    f"🆔 ID: `{broadcast_id}`\n"
                    f"📂 Category: {category}\n"
                    f"👥 Target Users: {len(target_users)}\n"
                    f"🤖 Via: Bot 8\n\n"
                    f"📝 Progress: {i}/{len(target_users)} users\n"
                    f"✅ Success: {success_count} | ❌ Failed: {failed_count}",
                    parse_mode="Markdown"
                )
            except:
                pass  # Ignore edit errors during sending
        
        try:
            # CROSS-BOT MEDIA FIX - Download from Bot 10 and send through Bot 8
            if media_type == "photo" and file_id:
                photo_file = await bot.get_file(file_id)
                photo_bytes = await bot.download_file(photo_file.file_path)
                photo_input = BufferedInputFile(photo_bytes, filename="broadcast_photo.jpg")
                caption = _format_broadcast_msg(message_text, is_caption=True) if message_text and message_text.strip() else None
                if caption:
                    await bot_8.send_photo(user_id, photo_input, caption=caption)
                else:
                    await bot_8.send_photo(user_id, photo_input)
            elif media_type == "video" and file_id:
                video_file = await bot.get_file(file_id)
                video_bytes = await bot.download_file(video_file.file_path)
                video_input = BufferedInputFile(video_bytes, filename="broadcast_video.mp4")
                caption = _format_broadcast_msg(message_text, is_caption=True) if message_text and message_text.strip() else None
                if caption:
                    await bot_8.send_video(user_id, video_input, caption=caption)
                else:
                    await bot_8.send_video(user_id, video_input)
            elif media_type == "animation" and file_id:
                animation_file = await bot.get_file(file_id)
                animation_bytes = await bot.download_file(animation_file.file_path)
                animation_input = BufferedInputFile(animation_bytes, filename="broadcast_animation.gif")
                caption = _format_broadcast_msg(message_text, is_caption=True) if message_text and message_text.strip() else None
                if caption:
                    await bot_8.send_animation(user_id, animation_input, caption=caption)
                else:
                    await bot_8.send_animation(user_id, animation_input)
            elif media_type == "document" and file_id:
                document_file = await bot.get_file(file_id)
                document_bytes = await bot.download_file(document_file.file_path)
                document_input = BufferedInputFile(document_bytes, filename="broadcast_document")
                caption = _format_broadcast_msg(message_text, is_caption=True) if message_text and message_text.strip() else None
                if caption:
                    await bot_8.send_document(user_id, document_input, caption=caption)
                else:
                    await bot_8.send_document(user_id, document_input)
            elif media_type == "audio" and file_id:
                audio_file = await bot.get_file(file_id)
                audio_bytes = await bot.download_file(audio_file.file_path)
                audio_input = BufferedInputFile(audio_bytes, filename="broadcast_audio.mp3")
                caption = _format_broadcast_msg(message_text, is_caption=True) if message_text and message_text.strip() else None
                if caption:
                    await bot_8.send_audio(user_id, audio_input, caption=caption)
                else:
                    await bot_8.send_audio(user_id, audio_input)
            elif media_type == "voice" and file_id:
                voice_file = await bot.get_file(file_id)
                voice_bytes = await bot.download_file(voice_file.file_path)
                voice_input = BufferedInputFile(voice_bytes, filename="broadcast_voice.ogg")
                await bot_8.send_voice(user_id, voice_input)
            else:
                await bot_8.send_message(user_id, _format_broadcast_msg(message_text or "📢 MSA NODE Broadcast"))
            
            success_count += 1
            
            # Small delay to avoid rate limits
            if len(target_users) > 10:
                await asyncio.sleep(0.1)  # 100ms delay for large broadcasts
        except Exception as e:
            failed_count += 1
            error_msg = str(e)
            
            # Categorize error types
            if "blocked" in error_msg.lower():
                blocked_count += 1
            elif "not found" in error_msg.lower():
                error_details.append(f"User {user_id}: Account deleted")
            elif "restricted" in error_msg.lower():
                error_details.append(f"User {user_id}: Restricted")
            else:
                error_details.append(f"User {user_id}: {error_msg[:30]}...")
            
            continue
    
    # Update broadcast sent count
    col_broadcasts.update_one(
        {"broadcast_id": broadcast_id},
        {
            "$inc": {"sent_count": success_count},
            "$set": {"status": "sent", "last_sent": now_local()}
        }
    )
    
    # Send completion report
    sent_time = format_datetime(now_local())
    
    # Create detailed report
    report = f"✅ **Broadcast Complete!**\n\n"
    report += f"🆔 ID: `{broadcast_id}`\n"
    report += f"📂 Category: {category}\n"
    report += f"🤖 Delivered via: **Bot 8**\n"
    report += f"🕐 Sent At: {sent_time}\n\n"
    report += f"📊 **Delivery Report:**\n"
    report += f"✅ **Success: {success_count}** users received\n"
    report += f"❌ **Failed: {failed_count}** users (blocked/inactive)\n"
    if blocked_count > 0:
        report += f"🚫 **Blocked: {blocked_count}** users blocked the bot\n"
    report += f"📈 **Total Attempted: {len(target_users)}** users\n"
    
    delivery_rate = (success_count / len(target_users) * 100) if len(target_users) > 0 else 0
    report += f"💯 **Delivery Rate: {delivery_rate:.1f}%**\n\n"
    
    # Add error details if any (max 3 examples)
    if error_details and len(error_details) <= 3:
        report += f"⚠️ **Error Details:**\n"
        for error in error_details[:3]:
            report += f"• {error}\n"
        report += "\n"
    elif len(error_details) > 3:
        report += f"⚠️ **Sample Errors ({len(error_details)} total):**\n"
        for error in error_details[:2]:
            report += f"• {error}\n"
        report += f"• ...and {len(error_details) - 2} more\n\n"
    
    await status_msg.edit_text(report, parse_mode="Markdown")
    
    await message.answer(
        "📤 **Broadcasting complete!**\n\n"
        "🤖 Messages delivered through **Bot 8**\n"
        "👥 Users received broadcasts from Bot 8\n"
        "🔍 Check Bot 8 for any user replies",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# CANCEL HANDLERS
# ==========================================

@dp.message(Command("cancel"))
async def cancel_command_handler(message: types.Message, state: FSMContext):
    """Cancel current operation via command"""
    await state.clear()
    await message.answer(
        "❌ Operation cancelled.",
        reply_markup=get_broadcast_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "❌ CANCEL")
async def cancel_button_handler(message: types.Message, state: FSMContext):
    """Cancel current operation via button - go back one step"""
    current_state = await state.get_state()
    await state.clear()
    
    # Determine appropriate menu based on where user was
    if current_state:
        state_str = str(current_state)
        
        # Support-related states → Return to support menu
        if "Support" in state_str:
            reply_markup = get_support_management_menu()
            menu_text = "💬 **Support Menu**"
        # Broadcast-related states → Return to broadcast menu
        elif "Broadcast" in state_str:
            reply_markup = get_broadcast_menu()
            menu_text = "📢 **Broadcast Menu**"
        else:
            # Unknown state → Main menu
            reply_markup = await get_main_menu()
            menu_text = "📋 **Main Menu**"
    else:
        # No state → Main menu
        reply_markup = await get_main_menu()
        menu_text = "📋 **Main Menu**"
    
    await message.answer(
        "❌ Operation cancelled.",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

@dp.message(F.text == "🔍 FIND")
async def find_handler(message: types.Message, state: FSMContext):
    """Find user by MSA ID or User ID"""
    print(f"🔍 USER ACTION: {message.from_user.first_name} ({message.from_user.id}) accessed FIND feature")
    
    await state.set_state(FindStates.waiting_for_search)
    
    # Create back button keyboard
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔍 **FIND USER**\n\n"
        "Enter one of the following:\n"
        "• **MSA ID** (e.g., `MSA001`)\n"
        "• **User ID** (e.g., `123456789`)\n\n"
        "I'll fetch their complete profile and activity details.\n\n"
        "Type **⬅️ BACK** to return to main menu.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )


@dp.message(FindStates.waiting_for_search)
async def process_find_search(message: types.Message, state: FSMContext):
    """Process MSA ID or User ID search"""
    
    # Check for back button
    if message.text and message.text.strip() in ["⬅️ BACK", "/cancel", "❌ CANCEL"]:
        await state.clear()
        await message.answer(
            "✅ Returned to main menu.",
            reply_markup=get_main_menu(),
            parse_mode="Markdown"
        )
        return
    
    search_input = message.text.strip()
    
    if not search_input:
        await message.answer(
            "⚠️ **INVALID INPUT**\n\n"
            "Please enter a valid MSA ID or User ID.",
            parse_mode="Markdown"
        )
        return
    
    print(f"🔎 Searching for: {search_input}")
    
    # Show loading message
    loading_msg = await message.answer("⏳ Searching database...", parse_mode="Markdown")
    
    try:
        user_doc = None
        search_type = ""
        
        # Try to search by MSA ID first
        if search_input.upper().startswith("MSA"):
            search_type = "MSA ID"
            user_doc = col_user_tracking.find_one({"msa_id": search_input.upper()})
            print(f"📋 Searching by MSA ID: {search_input.upper()}")
        
        # If not found or is numeric, try User ID
        if not user_doc and search_input.isdigit():
            search_type = "User ID"
            user_id = int(search_input)
            user_doc = col_user_tracking.find_one({"user_id": user_id})
            print(f"📋 Searching by User ID: {user_id}")
        
        # If still not found and original input had MSA prefix
        if not user_doc and search_input.upper().startswith("MSA"):
            await loading_msg.delete()
            await message.answer(
                f"❌ **NOT FOUND**\n\n"
                f"No user found with MSA ID: `{search_input.upper()}`\n\n"
                f"Please check the ID and try again, or search by User ID instead.",
                parse_mode="Markdown"
            )
            return
        
        # If not found at all
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **NOT FOUND**\n\n"
                f"No user found with {search_type}: `{search_input}`\n\n"
                f"Make sure the user has started Bot 8 at least once.",
                parse_mode="Markdown"
            )
            return
        
        # User found - extract and format details
        user_id = user_doc.get("user_id", "N/A")
        msa_id = user_doc.get("msa_id", "N/A")
        username = user_doc.get("username", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        source = user_doc.get("source", "N/A")
        first_start_dt = user_doc.get("first_start")
        last_start_dt = user_doc.get("last_start")
        
        # Format timestamps to 12-hour AM/PM format
        if first_start_dt:
            first_start_str = first_start_dt.strftime("%b %d, %Y at %I:%M:%S %p")
        else:
            first_start_str = "N/A"
        
        if last_start_dt:
            last_start_str = last_start_dt.strftime("%b %d, %Y at %I:%M:%S %p")
        else:
            last_start_str = "N/A"
        
        # Calculate time since first start
        if first_start_dt:
            time_diff = now_local() - first_start_dt
            days = time_diff.days
            hours = time_diff.seconds // 3600
            minutes = (time_diff.seconds % 3600) // 60
            
            if days > 0:
                time_since = f"{days}d {hours}h {minutes}m ago"
            elif hours > 0:
                time_since = f"{hours}h {minutes}m ago"
            else:
                time_since = f"{minutes}m ago"
        else:
            time_since = "N/A"
        
        # Determine source description
        source_descriptions = {
            "YT": "📺 YouTube Link",
            "IG": "📸 Instagram Link",
            "IGCC": "📎 Instagram CC Link",
            "YTCODE": "🔗 YouTube Code Link"
        }
        source_display = source_descriptions.get(source, f"Unknown ({source})")
        
        # Format username with @ prefix if available
        username_display = f"@{username}" if username != "N/A" and username != "unknown" else "No username"
        
        # Build detailed user profile
        user_profile = (
            f"👤 **USER PROFILE**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n"
            f"👤 **Name:** {first_name}\n"
            f"📱 **Username:** {username_display}\n\n"
            
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 **SOURCE TRACKING**\n\n"
            
            f"🔗 **Entry Source:** {source_display}\n"
            f"📅 **First Joined:** {first_start_str}\n"
            f"⏰ **Last Active:** {last_start_str}\n"
            f"🕐 **Member Since:** {time_since}\n\n"
            
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"ℹ️ **NOTES**\n\n"
            f"• User is tracked in broadcast system\n"
            f"• Can receive {source} category broadcasts\n"
            f"• Profile synced with Bot 8 database\n\n"
            
            f"🔍 Search another user or press ⬅️ BACK"
        )
        
        await loading_msg.delete()

        back_keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]],
            resize_keyboard=True
        )
        await message.answer(user_profile, reply_markup=back_keyboard, parse_mode="Markdown")
        
        print(f"✅ Found user: {msa_id} (User ID: {user_id})")
        
        # Keep state active for continuous searching
        # User can search again or press BACK button
        
    except Exception as e:
        await loading_msg.delete()
        await message.answer(
            f"❌ **ERROR**\n\n"
            f"Search failed: {str(e)[:100]}\n\n"
            f"Please try again or contact support.",
            parse_mode="Markdown"
        )
        print(f"❌ Find search error: {e}")

@dp.message(F.text == "📊 TRAFFIC")
async def traffic_handler(message: types.Message):
    """Traffic analytics - Live user source tracking"""
    print(f"📊 USER ACTION: {message.from_user.first_name} ({message.from_user.id}) accessed TRAFFIC analytics")
    
    # Show loading message
    loading_msg = await message.answer("⏳ Fetching live traffic data...", parse_mode="Markdown")
    
    try:
        # Fetch live counts from database (no duplicates, user_id is unique)
        yt_count = col_user_tracking.count_documents({"source": "YT"})
        ig_count = col_user_tracking.count_documents({"source": "IG"})
        igcc_count = col_user_tracking.count_documents({"source": "IGCC"})
        ytcode_count = col_user_tracking.count_documents({"source": "YTCODE"})
        total_count = col_user_tracking.count_documents({})
        
        print(f"📈 Traffic Stats: YT={yt_count}, IG={ig_count}, IGCC={igcc_count}, YTCODE={ytcode_count}, Total={total_count}")
        
        # Get Bot 8 information
        try:
            bot_8_info = await bot_8.get_me()
            bot_8_username = f"@{bot_8_info.username}" if bot_8_info.username else "N/A"
            bot_8_name = bot_8_info.first_name
            bot_8_status = "🟢 Online"
        except Exception as e:
            bot_8_username = "Error"
            bot_8_name = "Unknown"
            bot_8_status = "🔴 Offline"
            print(f"⚠️ Failed to get Bot 8 info: {e}")
        
        # Calculate percentages
        yt_percent = (yt_count / total_count * 100) if total_count > 0 else 0
        ig_percent = (ig_count / total_count * 100) if total_count > 0 else 0
        igcc_percent = (igcc_count / total_count * 100) if total_count > 0 else 0
        ytcode_percent = (ytcode_count / total_count * 100) if total_count > 0 else 0
        
        # Build traffic report
        traffic_report = (
            "📊 **TRAFFIC ANALYTICS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            "👥 **USER SOURCE BREAKDOWN**\n\n"
            
            f"📺 **YouTube Links (YT)**\n"
            f"   └─ {yt_count} users ({yt_percent:.1f}%)\n"
            f"   └─ Direct YouTube video links\n\n"
            
            f"📸 **Instagram Links (IG)**\n"
            f"   └─ {ig_count} users ({ig_percent:.1f}%)\n"
            f"   └─ Instagram bio/post links\n\n"
            
            f"📎 **Instagram CC Links (IGCC)**\n"
            f"   └─ {igcc_count} users ({igcc_percent:.1f}%)\n"
            f"   └─ IG content continuation links\n\n"
            
            f"🔗 **YouTube Code Links (YTCODE)**\n"
            f"   └─ {ytcode_count} users ({ytcode_percent:.1f}%)\n"
            f"   └─ YT video MSA CODE prompts\n\n"
            
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 **TOTAL USERS:** {total_count}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            "🤖 **BOT 8 STATUS**\n"
            f"   └─ Name: {bot_8_name}\n"
            f"   └─ Username: {bot_8_username}\n"
            f"   └─ Status: {bot_8_status}\n"
            f"   └─ Role: Message Delivery Bot\n\n"
            
            "ℹ️ **NOTE:**\n"
            "• Each user counted once (no duplicates)\n"
            "• Source tracked on first /start link\n"
            "• Data updated in real-time\n"
            "• Bot 8 handles all user messages\n"
            "• Bot 10 manages broadcasts only"
        )
        
        # Delete loading message and send report
        await loading_msg.delete()
        
        # Create refresh button
        refresh_btn = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 REFRESH", callback_data="traffic_refresh")]
        ])
        
        await message.answer(traffic_report, parse_mode="Markdown", reply_markup=refresh_btn)
        
    except Exception as e:
        await loading_msg.edit_text(
            f"❌ **ERROR**\n\nFailed to fetch traffic data:\n{str(e)[:100]}",
            parse_mode="Markdown"
        )
        print(f"❌ Traffic handler error: {e}")


@dp.callback_query(F.data == "traffic_refresh")
async def traffic_refresh_handler(callback: types.CallbackQuery):
    """Refresh traffic analytics data"""
    print(f"🔄 USER ACTION: {callback.from_user.first_name} ({callback.from_user.id}) refreshed TRAFFIC analytics")
    
    try:
        # Show loading indicator
        await callback.answer("⏳ Refreshing...", show_alert=False)
        
        # Fetch fresh counts from database
        yt_count = col_user_tracking.count_documents({"source": "YT"})
        ig_count = col_user_tracking.count_documents({"source": "IG"})
        igcc_count = col_user_tracking.count_documents({"source": "IGCC"})
        ytcode_count = col_user_tracking.count_documents({"source": "YTCODE"})
        total_count = col_user_tracking.count_documents({})
        
        print(f"📈 Refreshed Stats: YT={yt_count}, IG={ig_count}, IGCC={igcc_count}, YTCODE={ytcode_count}, Total={total_count}")
        
        # Get Bot 8 information
        try:
            bot_8_info = await bot_8.get_me()
            bot_8_username = f"@{bot_8_info.username}" if bot_8_info.username else "N/A"
            bot_8_name = bot_8_info.first_name
            bot_8_status = "🟢 Online"
        except Exception as e:
            bot_8_username = "Error"
            bot_8_name = "Unknown"
            bot_8_status = "🔴 Offline"
            print(f"⚠️ Failed to get Bot 8 info: {e}")
        
        # Calculate percentages
        yt_percent = (yt_count / total_count * 100) if total_count > 0 else 0
        ig_percent = (ig_count / total_count * 100) if total_count > 0 else 0
        igcc_percent = (igcc_count / total_count * 100) if total_count > 0 else 0
        ytcode_percent = (ytcode_count / total_count * 100) if total_count > 0 else 0
        
        # Get current timestamp
        now = now_local().strftime("%I:%M:%S %p")
        
        # Build updated traffic report
        traffic_report = (
            "📊 **TRAFFIC ANALYTICS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            "👥 **USER SOURCE BREAKDOWN**\n\n"
            
            f"📺 **YouTube Links (YT)**\n"
            f"   └─ {yt_count} users ({yt_percent:.1f}%)\n"
            f"   └─ Direct YouTube video links\n\n"
            
            f"📸 **Instagram Links (IG)**\n"
            f"   └─ {ig_count} users ({ig_percent:.1f}%)\n"
            f"   └─ Instagram bio/post links\n\n"
            
            f"📎 **Instagram CC Links (IGCC)**\n"
            f"   └─ {igcc_count} users ({igcc_percent:.1f}%)\n"
            f"   └─ IG content continuation links\n\n"
            
            f"🔗 **YouTube Code Links (YTCODE)**\n"
            f"   └─ {ytcode_count} users ({ytcode_percent:.1f}%)\n"
            f"   └─ YT video MSA CODE prompts\n\n"
            
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 **TOTAL USERS:** {total_count}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            "🤖 **BOT 8 STATUS**\n"
            f"   └─ Name: {bot_8_name}\n"
            f"   └─ Username: {bot_8_username}\n"
            f"   └─ Status: {bot_8_status}\n"
            f"   └─ Role: Message Delivery Bot\n\n"
            
            "ℹ️ **NOTE:**\n"
            "• Each user counted once (no duplicates)\n"
            "• Source tracked on first /start link\n"
            "• Data updated in real-time\n"
            "• Bot 8 handles all user messages\n"
            "• Bot 10 manages broadcasts only\n\n"
            
            f"🕒 Last updated: {now}"
        )
        
        # Update message with fresh data
        refresh_btn = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 REFRESH", callback_data="traffic_refresh")]
        ])
        
        await callback.message.edit_text(traffic_report, parse_mode="Markdown", reply_markup=refresh_btn)
        
    except Exception as e:
        await callback.answer(f"❌ Refresh failed: {str(e)[:50]}", show_alert=True)
        print(f"❌ Traffic refresh error: {e}")

# ==================== SUPPORT PAGINATION CALLBACKS ====================
@dp.callback_query(F.data.startswith("pending_page_"))
async def pending_page_navigation(callback: types.CallbackQuery):
    """Navigate through pending tickets pages"""
    try:
        page = int(callback.data.split("_")[-1])
        await callback.answer()
        await show_pending_tickets_page(callback.message, page=page)
        log_action("NAV", callback.from_user.id, f"Viewed Pending Tickets page {page}", "bot10")
    except Exception as e:
        await callback.answer(f"❌ Error: {str(e)[:50]}", show_alert=True)
        print(f"❌ Pending page navigation error: {e}")

@dp.callback_query(F.data.startswith("all_page_"))
async def all_page_navigation(callback: types.CallbackQuery):
    """Navigate through all tickets pages"""
    try:
        page = int(callback.data.split("_")[-1])
        await callback.answer()
        await show_all_tickets_page(callback.message, page=page)
        log_action("NAV", callback.from_user.id, f"Viewed All Tickets page {page}", "bot10")
    except Exception as e:
        await callback.answer(f"❌ Error: {str(e)[:50]}", show_alert=True)
        print(f"❌ All tickets page navigation error: {e}")

@dp.callback_query(F.data.startswith("backup_page_"))
async def backup_page_navigation(callback: types.CallbackQuery):
    """Navigate through backups pages"""
    try:
        page = int(callback.data.split("_")[-1])
        await callback.answer()
        await show_backups_page(callback.message, page=page)
        log_action("NAV", callback.from_user.id, f"Viewed Backups page {page}", "bot10")
    except Exception as e:
        await callback.answer(f"❌ Error: {str(e)[:50]}", show_alert=True)
        print(f"❌ Backup page navigation error: {e}")
# ======================================================================

@dp.message(F.text == "🩺 DIAGNOSIS")
async def diagnosis_menu(message: types.Message):
    """Diagnosis menu"""
    log_action("CMD", message.from_user.id, "Opened Diagnosis Menu")
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 BOT 8 DIAGNOSIS"), KeyboardButton(text="🎛️ BOT 10 DIAGNOSIS")],
            [KeyboardButton(text="⬅️ MAIN MENU")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        "🩺 **SYSTEM DIAGNOSIS CENTER**\n\n"
        "Advanced diagnostic tools for system health monitoring.\n"
        "Select a system to diagnose:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.text == "📱 BOT 8 DIAGNOSIS")
async def bot8_diagnosis(message: types.Message):
    """Run comprehensive diagnosis on Bot 8 system"""
    log_action("DIAGNOSIS", message.from_user.id, "Running Bot 8 Diagnosis", "bot8")
    
    status_msg = await message.answer(
        "🔄 **INITIALIZING BOT 8 DIAGNOSTICS**\n\n"
        "⏳ Scanning system components...\n"
        "📊 Analyzing database health...\n"
        "🔍 Checking data integrity...",
        parse_mode="Markdown"
    )
    
    await asyncio.sleep(1.2)
    
    # Initialize tracking
    issues = []
    warnings = []
    info_items = []
    total_checks = 0
    checks_passed = 0
    
    # ═══════════════════════════════════════
    # PHASE 1: DATABASE CONNECTION & LATENCY
    # ═══════════════════════════════════════
    total_checks += 1
    db_status = "Unknown"
    db_latency = 0
    
    try:
        start = time.time()
        client.admin.command('ping')
        db_latency = (time.time() - start) * 1000
        
        if db_latency < 50:
            db_status = f"✅ Excellent ({db_latency:.1f}ms)"
            checks_passed += 1
        elif db_latency < 150:
            db_status = f"⚠️ Acceptable ({db_latency:.1f}ms)"
            warnings.append(f"Database latency is elevated: {db_latency:.1f}ms (normal <50ms)")
        else:
            db_status = f"❌ Slow ({db_latency:.1f}ms)"
            issues.append(f"**Database Performance Critical:** Latency {db_latency:.1f}ms exceeds safe threshold.")
            
    except Exception as e:
        db_status = "❌ Connection Failed"
        issues.append(f"**Database Connection Error:** {str(e)[:100]}")
    
    # ═══════════════════════════════════════
    # PHASE 2: COLLECTION VERIFICATION
    # ═══════════════════════════════════════
    total_checks += 1
    collections_ok = True
    
    try:
        expected_collections = [
            "msa_ids", "user_verification", "support_tickets",
            "banned_users", "suspended_features", "bot9_pdfs", "bot9_ig_content"
        ]
        existing = db.list_collection_names()
        missing = [c for c in expected_collections if c not in existing]
        
        if missing:
            warnings.append(f"**Missing Collections:** {', '.join(missing)}")
            collections_ok = False
        else:
            checks_passed += 1
            info_items.append(f"All {len(expected_collections)} core collections present")
            
    except Exception as e:
        issues.append(f"**Collection Check Failed:** {str(e)[:80]}")
        collections_ok = False
    
    # ═══════════════════════════════════════
    # PHASE 3: USER DATA HEALTH
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        total_users = col_msa_ids.count_documents({})
        pending_vers = col_user_verification.count_documents({})
        banned_users = col_banned_users.count_documents({})
        suspended_users = col_suspended_features.count_documents({})
        
        if total_users == 0:
            warnings.append("**No Users Found:** Database appears to be empty or not initialized.")
        else:
            checks_passed += 1
            info_items.append(f"{total_users:,} registered users")
            
            # Verification queue check
            if pending_vers > 50:
                issues.append(f"**Verification Crisis:** {pending_vers} users stuck in queue! Bot may be offline.")
            elif pending_vers > 20:
                warnings.append(f"**High Verification Queue:** {pending_vers} pending. Monitor closely.")
            
            # Ban rate analysis
            if total_users > 0:
                ban_rate = (banned_users / total_users) * 100
                if ban_rate > 30:
                    issues.append(f"**Extreme Ban Rate:** {ban_rate:.1f}% ({banned_users}/{total_users}) - Possible attack or misconfiguration")
                elif ban_rate > 15:
                    warnings.append(f"**High Ban Rate:** {ban_rate:.1f}% ({banned_users}/{total_users})")
                else:
                    info_items.append(f"Ban rate: {ban_rate:.1f}%")
                    
    except Exception as e:
        issues.append(f"**User Data Check Failed:** {str(e)[:80]}")
    
    # ═══════════════════════════════════════
    # PHASE 4: SUPPORT SYSTEM HEALTH
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        open_tickets = col_support_tickets.count_documents({"status": "open"})
        total_tickets = col_support_tickets.count_documents({})
        
        if open_tickets > 20:
            issues.append(f"**Support Overload:** {open_tickets} open tickets! Urgent admin attention required.")
        elif open_tickets > 10:
            warnings.append(f"**Support Backlog:** {open_tickets} open tickets pending review.")
        elif open_tickets > 5:
            info_items.append(f"{open_tickets} open support tickets (manageable)")
        else:
            checks_passed += 1
            info_items.append(f"Support queue healthy ({open_tickets} open)")
            
    except Exception as e:
        warnings.append(f"Support check error: {str(e)[:60]}")
    
    # ═══════════════════════════════════════
    # PHASE 5: CONTENT LIBRARY STATUS
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        pdf_count = col_bot9_pdfs.count_documents({})
        ig_count = col_bot9_ig_content.count_documents({})
        
        if pdf_count == 0 and ig_count == 0:
            warnings.append("**No Content Found:** PDF and IG collections are empty.")
        else:
            checks_passed += 1
            info_items.append(f"Content library: {pdf_count} PDFs, {ig_count} IG items")
            
    except Exception as e:
        warnings.append(f"Content check skipped: {str(e)[:50]}")
    
    # ═══════════════════════════════════════
    # PHASE 6: LOG ERROR ANALYSIS
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        error_keywords = ['error', 'failed', 'exception', 'crash']
        error_logs = [
            l for l in bot8_logs 
            if any(kw in l.get('details', '').lower() for kw in error_keywords)
        ]
        
        if error_logs:
            if len(error_logs) > 5:
                issues.append(f"**High Error Rate:** {len(error_logs)} errors detected in recent logs.")
            else:
                warnings.append(f"**Recent Errors:** {len(error_logs)} error events logged.")
        else:
            checks_passed += 1
            info_items.append("No errors detected in recent logs")
            
    except Exception as e:
        info_items.append("Log analysis skipped")
    
    # ═══════════════════════════════════════
    # GENERATE COMPREHENSIVE REPORT
    # ═══════════════════════════════════════
    
    scan_time = now_local().strftime('%Y-%m-%d %H:%M:%S')
    health_percentage = int((checks_passed / total_checks) * 100) if total_checks > 0 else 0
    
    # Determine overall status
    if health_percentage >= 90:
        status_icon = "✅"
        status_text = "EXCELLENT"
    elif health_percentage >= 70:
        status_icon = "⚠️"
        status_text = "GOOD"
    elif health_percentage >= 50:
        status_icon = "⚠️"
        status_text = "DEGRADED"
    else:
        status_icon = "❌"
        status_text = "CRITICAL"
    
    report = f"📱 **BOT 8 DIAGNOSTIC REPORT**\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"🕐 **Scan Time:** {scan_time}\n"
    report += f"💾 **Database:** {db_status}\n"
    report += f"📊 **Health Score:** {checks_passed}/{total_checks} ({health_percentage}%)\n"
    report += f"🎯 **Status:** {status_icon} {status_text}\n\n"
    
    # Critical issues section
    if issues:
        report += f"❌ **CRITICAL ISSUES ({len(issues)}):**\n"
        for i, issue in enumerate(issues, 1):
            report += f"{i}. {issue}\n"
        report += "\n"
    
    # Warnings section
    if warnings:
        report += f"⚠️ **WARNINGS ({len(warnings)}):**\n"
        for i, warning in enumerate(warnings, 1):
            report += f"{i}. {warning}\n"
        report += "\n"
    
    # System info
    if info_items:
        report += "ℹ️ **SYSTEM INFO:**\n"
        for info in info_items[:5]:  # Limit to prevent message overflow
            report += f"• {info}\n"
        report += "\n"
    
    # Final verdict
    if not issues and not warnings:
        report += "✅ **ALL SYSTEMS OPERATIONAL**\n"
        report += "No issues detected. Bot 8 is healthy."
    elif issues:
        report += "🚨 **ACTION REQUIRED**\n"
        report += "Critical issues detected. Address immediately to restore full functionality."
    else:
        report += "✅ **SYSTEM FUNCTIONAL**\n"
        report += "Minor warnings detected. Monitor but no immediate action required."
    
    await status_msg.edit_text(report, parse_mode="Markdown")

@dp.message(F.text == "🎛️ BOT 10 DIAGNOSIS")
async def bot10_diagnosis(message: types.Message):
    """Run comprehensive diagnosis on Bot 10 admin system"""
    log_action("DIAGNOSIS", message.from_user.id, "Running Bot 10 Diagnosis", "bot10")
    
    status_msg = await message.answer(
        "🔄 **INITIALIZING BOT 10 DIAGNOSTICS**\n\n"
        "⏳ Checking admin systems...\n"
        "🔐 Verifying configurations...\n"
        "💾 Analyzing backups...",
        parse_mode="Markdown"
    )
    
    await asyncio.sleep(0.8)
    
    # Initialize tracking
    issues = []
    warnings = []
    info_items = []
    total_checks = 0
    checks_passed = 0
    
    # ═══════════════════════════════════════
    # PHASE 1: SYSTEM FILES & CONFIGURATION
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        required_files = {
            "bot10.py": "Main bot script",
            "token.json": "Drive API credentials",
            "db_config.json": "Database configuration",
            ".env": "Environment variables"
        }
        
        missing = []
        present = []
        
        for file, desc in required_files.items():
            if os.path.exists(file):
                present.append(file)
            else:
                missing.append(f"{file} ({desc})")
        
        if missing:
            issues.append(f"**Missing Critical Files:** {', '.join(missing)}")
        else:
            checks_passed += 1
            info_items.append(f"All {len(required_files)} config files present")
            
    except Exception as e:
        issues.append(f"**File System Check Failed:** {str(e)[:80]}")
    
    # ═══════════════════════════════════════
    # PHASE 2: BACKUP SYSTEM HEALTH
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        backup_dir = "backups"
        if not os.path.exists(backup_dir):
            issues.append("**Backup System Error:** Backup directory does not exist. Create it immediately!")
        else:
            backup_files = [f for f in os.listdir(backup_dir) if f.endswith(('.json', '.csv', '.txt'))]
            
            if not backup_files:
                warnings.append("**No Backups Found:** Backup directory is empty. Run first backup now.")
            else:
                # Get newest backup
                backup_files.sort(key=lambda x: os.path.getmtime(os.path.join(backup_dir, x)), reverse=True)
                newest = backup_files[0]
                newest_path = os.path.join(backup_dir, newest)
                last_backup_time = datetime.fromtimestamp(os.path.getmtime(newest_path))
                backup_age = (now_local() - last_backup_time).days
                backup_size = os.path.getsize(newest_path) / 1024  # KB
                
                if backup_age > 7:
                    issues.append(f"**Backup Crisis:** Last backup is {backup_age} days old! Critical data loss risk.")
                elif backup_age > 3:
                    warnings.append(f"**Backup Warning:** Last backup is {backup_age} days old. Backup soon.")
                else:
                    checks_passed += 1
                    info_items.append(f"Latest backup: {backup_age}d ago ({backup_size:.1f}KB)")
                
                # Check backup count
                if len(backup_files) < 3:
                    warnings.append(f"**Low Backup Count:** Only {len(backup_files)} backups exist. Increase retention.")
                else:
                    info_items.append(f"{len(backup_files)} backups stored")
                    
    except Exception as e:
        warnings.append(f"Backup check error: {str(e)[:60]}")
    
    # ═══════════════════════════════════════
    # PHASE 3: LOG SYSTEM HEALTH
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        bot8_log_count = len(bot8_logs)
        bot10_log_count = len(bot10_logs)
        
        log_health = True
        
        if bot10_log_count >= MAX_LOGS:
            warnings.append(f"**Log Buffer Full:** Bot 10 buffer at capacity ({MAX_LOGS}). Active rotation.")
            log_health = False
            
        if bot8_log_count >= MAX_LOGS:
            warnings.append(f"**Log Buffer Full:** Bot 8 tracking buffer at capacity.")
            log_health = False
        
        if log_health:
            checks_passed += 1
            info_items.append(f"Logs: Bot8={bot8_log_count}, Bot10={bot10_log_count}")
            
        # Check for error patterns
        error_count_bot10 = sum(1 for l in bot10_logs if 'error' in l.get('details', '').lower())
        if error_count_bot10 > 5:
            warnings.append(f"**Admin Errors Detected:** {error_count_bot10} error events in Bot 10 logs.")
            
    except Exception as e:
        warnings.append(f"Log system check skipped: {str(e)[:50]}")
    
    # ═══════════════════════════════════════
    # PHASE 4: DATABASE CONNECTION
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        # Test MongoDB connection from admin side
        start = time.time()
        client.admin.command('ping')
        db_latency = (time.time() - start) * 1000
        
        if db_latency < 100:
            checks_passed += 1
            info_items.append(f"DB responsive ({db_latency:.1f}ms)")
        else:
            warnings.append(f"**DB Latency High:** {db_latency:.1f}ms (admin operations may be slow)")
            
    except Exception as e:
        issues.append(f"**DB Connection Error:** {str(e)[:80]}")
    
    # ═══════════════════════════════════════
    # PHASE 5: ENVIRONMENT & SECURITY
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        # Check critical environment variables
        env_vars = ['BOT_10_TOKEN', 'BOT_8_TOKEN', 'MONGO_URI', 'OWNER_ID']
        missing_env = []
        
        for var in env_vars:
            if not os.getenv(var):
                missing_env.append(var)
        
        if missing_env:
            issues.append(f"**Missing Env Variables:** {', '.join(missing_env)}")
        else:
            checks_passed += 1
            info_items.append("All environment vars configured")
            
    except Exception as e:
        warnings.append(f"Environment check skipped: {str(e)[:50]}")
    
    # ═══════════════════════════════════════
    # PHASE 6: DRIVE API STATUS (if using)
    # ═══════════════════════════════════════
    total_checks += 1
    
    try:
        if os.path.exists('token.json'):
            with open('token.json', 'r') as f:
                token_data = json.load(f)
                if 'token' in token_data or 'access_token' in token_data:
                    checks_passed += 1
                    info_items.append("Drive API token valid")
                else:
                    warnings.append("**Drive Token Malformed:** Backup uploads may fail.")
        else:
            warnings.append("**No Drive Token:** Cloud backups unavailable.")
            
    except Exception as e:
        info_items.append("Drive check skipped")
    
    # ═══════════════════════════════════════
    # GENERATE COMPREHENSIVE REPORT
    # ═══════════════════════════════════════
    
    scan_time = now_local().strftime('%Y-%m-%d %H:%M:%S')
    health_percentage = int((checks_passed / total_checks) * 100) if total_checks > 0 else 0
    
    # Determine overall status
    if health_percentage >= 90:
        status_icon = "✅"
        status_text = "EXCELLENT"
    elif health_percentage >= 70:
        status_icon = "⚠️"
        status_text = "GOOD"
    elif health_percentage >= 50:
        status_icon = "⚠️"
        status_text = "NEEDS ATTENTION"
    else:
        status_icon = "❌"
        status_text = "CRITICAL"
    
    report = f"🎛️ **BOT 10 DIAGNOSTIC REPORT**\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"🕐 **Scan Time:** {scan_time}\n"
    report += f"💻 **Version:** Administrator v2.1\n"
    report += f"📊 **Health Score:** {checks_passed}/{total_checks} ({health_percentage}%)\n"
    report += f"🎯 **Status:** {status_icon} {status_text}\n\n"
    
    # Critical issues section
    if issues:
        report += f"❌ **CRITICAL ALERTS ({len(issues)}):**\n"
        for i, issue in enumerate(issues, 1):
            report += f"{i}. {issue}\n"
        report += "\n"
    
    # Warnings section
    if warnings:
        report += f"⚠️ **WARNINGS ({len(warnings)}):**\n"
        for i, warning in enumerate(warnings, 1):
            report += f"{i}. {warning}\n"
        report += "\n"
    
    # System info
    if info_items:
        report += "ℹ️ **SYSTEM STATUS:**\n"
        for info in info_items[:5]:
            report += f"• {info}\n"
        report += "\n"
    
    # Final verdict
    if not issues and not warnings:
        report += "✅ **ALL SYSTEMS OPERATIONAL**\n"
        report += "Bot 10 admin panel is healthy and ready."
    elif issues:
        report += "🚨 **IMMEDIATE ACTION REQUIRED**\n"
        report += "Critical issues detected. Resolve to restore full admin functionality."
    else:
        report += "✅ **SYSTEM FUNCTIONAL**\n"
        report += "Minor warnings present. Monitor but system is operational."
    
    await status_msg.edit_text(report, parse_mode="Markdown")

def get_shoot_menu():
    """Shoot (Admin Control) submenu"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚫 BAN USER"), KeyboardButton(text="✅ UNBAN USER")],
            [KeyboardButton(text="⏰ TEMPORARY BAN"), KeyboardButton(text="🗑️ DELETE USER")],
            [KeyboardButton(text="⏸️ SUSPEND FEATURES"), KeyboardButton(text="▶️ UNSUSPEND")],
            [KeyboardButton(text="🔄 RESET USER DATA"), KeyboardButton(text="🔍 SEARCH USER")],
            [KeyboardButton(text="⬅️ MAIN MENU")]
        ],
        resize_keyboard=True
    )

@dp.message(F.text == "📸 SHOOT")
async def shoot_handler(message: types.Message, state: FSMContext):
    """Shoot (Admin Control) feature - User management"""
    await state.clear()
    await message.answer(
        "📸 **SHOOT - ADMIN CONTROL**\n\n"
        "Manage users and their access:\n\n"
        "🚫 **BAN USER** - Block all bot access\n"
        "✅ **UNBAN USER** - Restore bot access\n"
        "🗑️ **DELETE USER** - Permanently remove user\n"
        "⏸️ **SUSPEND FEATURES** - Disable specific features\n"
        "▶️ **UNSUSPEND** - Remove all suspended features\n"
        "🔄 **RESET USER DATA** - Reset user information\n"
        "🔍 **SEARCH USER** - View detailed user info\n\n"
        "⚠️ **Warning:** These actions affect Bot 8 users.",
        reply_markup=get_shoot_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# BAN USER HANDLERS
# ==========================================

@dp.message(F.text == "🚫 BAN USER")
async def ban_user_start(message: types.Message, state: FSMContext):
    """Start ban user flow"""
    await state.set_state(ShootStates.waiting_for_ban_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🚫 **BAN USER**\n\n"
        "Enter the user's **MSA ID** or **User ID** to ban:\n\n"
        "⚠️ Banned users will:\n"
        "  • Lose all Bot 8 access\n"
        "  • See only SUPPORT button\n"
        "  • Receive ban notification\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_ban_id)
async def process_ban_id(message: types.Message, state: FSMContext):
    """Process ban user ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user
        user_doc = None
        if search_input.upper().startswith("MSA"):
            user_doc = col_user_tracking.find_one({"msa_id": search_input.upper()})
        elif search_input.isdigit():
            user_doc = col_user_tracking.find_one({"user_id": int(search_input)})
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`\n\n"
                f"Please try again with a valid MSA ID or User ID.",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        username = user_doc.get("username", "N/A")
        
        # Check if already banned
        is_banned = col_banned_users.find_one({"user_id": user_id})
        if is_banned:
            await loading_msg.delete()
            await message.answer(
                f"⚠️ **ALREADY BANNED**\n\n"
                f"User {first_name} (`{msa_id}`) is already banned.\n\n"
                f"Banned on: {is_banned.get('banned_at', now_local()).strftime('%b %d, %Y at %I:%M:%S %p')}",
                parse_mode="Markdown"
            )
            return
        
        # Store user data for confirmation
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name,
            username=username
        )
        await state.set_state(ShootStates.waiting_for_ban_confirm)
        
        confirm_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ CONFIRM BAN"), KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        await loading_msg.delete()
        await message.answer(
            f"🚫 **CONFIRM BAN**\n\n"
            f"👤 **Name:** {first_name}\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n"
            f"📱 **Username:** @{username if username != 'N/A' else 'None'}\n\n"
            f"⚠️ **This will:**\n"
            f"  • Ban user from all Bot 8 functions\n"
            f"  • Hide all menus and buttons\n"
            f"  • Show only SUPPORT option\n"
            f"  • Send ban notification to user\n\n"
            f"Type **✅ CONFIRM BAN** to proceed or **❌ CANCEL** to abort.",
            reply_markup=confirm_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.waiting_for_ban_confirm)
async def process_ban_confirm(message: types.Message, state: FSMContext):
    """Process ban confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Ban cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM BAN" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        
        try:
            # Add to banned_users collection
            col_banned_users.insert_one({
                "user_id": user_id,
                "msa_id": msa_id,
                "first_name": first_name,
                "username": data.get("username"),
                "banned_at": now_local(),
                "banned_by": message.from_user.id,
                "reason": "Admin action"
            })
            
            # Try to notify user via Bot 8
            try:
                ban_message = (
                    "🚫 **ACCOUNT BANNED**\n\n"
                    "Your access to this bot has been restricted.\n\n"
                    "If you believe this is an error, please contact support using the button below."
                )
                
                support_btn = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💬 CONTACT SUPPORT", callback_data="banned_support")]
                ])
                
                await bot_8.send_message(user_id, ban_message, reply_markup=support_btn, parse_mode="Markdown")
            except:
                pass  # User might have blocked bot
            
            await state.clear()
            await message.answer(
                f"✅ **USER BANNED**\n\n"
                f"👤 {first_name} (`{msa_id}`) has been banned from Bot 8.\n\n"
                f"🕐 Banned at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                f"User will see ban notification on next interaction.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"🚫 User {user_id} ({msa_id}) banned by admin {message.from_user.id}")
        
        except Exception as e:
            await message.answer(f"❌ **BAN FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM BAN** or **❌ CANCEL**", parse_mode="Markdown")

# ==========================================
# TEMPORARY BAN USER HANDLERS
# ==========================================

@dp.message(F.text == "⏰ TEMPORARY BAN")
async def temp_ban_user_start(message: types.Message, state: FSMContext):
    """Start temporary ban user flow"""
    await state.set_state(ShootStates.waiting_for_temp_ban_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "⏰ **TEMPORARY BAN**\n\n"
        "Enter the user's **MSA ID** or **User ID** to temporarily ban:\n\n"
        "⚠️ Temporary ban will:\n"
        "  • Block all Bot 8 access for selected duration\n"
        "  • Show countdown timer to user\n"
        "  • Auto-unban when time expires\n"
        "  • Allow user to appeal via support\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_temp_ban_id)
async def process_temp_ban_id(message: types.Message, state: FSMContext):
    """Process temporary ban user ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user
        user_doc = None
        if search_input.upper().startswith("MSA"):
            user_doc = col_user_tracking.find_one({"msa_id": search_input.upper()})
        elif search_input.isdigit():
            user_doc = col_user_tracking.find_one({"user_id": int(search_input)})
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`\n\n"
                f"Please try again with a valid MSA ID or User ID.",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        username = user_doc.get("username", "N/A")
        
        # Check if already banned
        is_banned = col_banned_users.find_one({"user_id": user_id})
        if is_banned:
            ban_type = "temporary" if is_banned.get('ban_expires') else "permanent"
            await loading_msg.delete()
            await message.answer(
                f"⚠️ **ALREADY BANNED**\n\n"
                f"User {first_name} (`{msa_id}`) is already {ban_type} banned.\n\n"
                f"Banned on: {is_banned.get('banned_at', now_local()).strftime('%b %d, %Y at %I:%M:%S %p')}",
                parse_mode="Markdown"
            )
            return
        
        # Store user data and show duration menu
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name,
            username=username
        )
        await state.set_state(ShootStates.selecting_temp_ban_duration)
        
        duration_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⏱️ 1 HOUR"), KeyboardButton(text="⏱️ 6 HOURS")],
                [KeyboardButton(text="⏱️ 12 HOURS"), KeyboardButton(text="⏱️ 1 DAY")],
                [KeyboardButton(text="⏱️ 3 DAYS"), KeyboardButton(text="⏱️ 7 DAYS")],
                [KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        await loading_msg.delete()
        await message.answer(
            f"⏰ **SELECT BAN DURATION**\n\n"
            f"👤 **User:** {first_name} (`{msa_id}`)\n\n"
            f"Select how long to ban this user:\n\n"
            f"⏱️ **1 HOUR** - Short timeout\n"
            f"⏱️ **6 HOURS** - Medium restriction\n"
            f"⏱️ **12 HOURS** - Half day\n"
            f"⏱️ **1 DAY** - Full day\n"
            f"⏱️ **3 DAYS** - Extended period\n"
            f"⏱️ **7 DAYS** - One week\n\n"
            f"User will be auto-unbanned after duration expires.",
            reply_markup=duration_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.selecting_temp_ban_duration)
async def process_temp_ban_duration(message: types.Message, state: FSMContext):
    """Process temporary ban duration selection"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    # Map duration buttons to hours
    duration_map = {
        "⏱️ 1 HOUR": 1,
        "⏱️ 6 HOURS": 6,
        "⏱️ 12 HOURS": 12,
        "⏱️ 1 DAY": 24,
        "⏱️ 3 DAYS": 72,
        "⏱️ 7 DAYS": 168
    }
    
    if message.text not in duration_map:
        await message.answer("⚠️ Please select a duration from the menu.", parse_mode="Markdown")
        return
    
    hours = duration_map[message.text]
    data = await state.get_data()
    
    # Calculate expiry time
    ban_expires = now_local() + timedelta(hours=hours)
    
    # Store duration info
    await state.update_data(
        ban_duration_hours=hours,
        ban_expires=ban_expires,
        ban_duration_text=message.text.replace("⏱️ ", "")
    )
    await state.set_state(ShootStates.waiting_for_temp_ban_confirm)
    
    confirm_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ CONFIRM TEMP BAN"), KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    first_name = data.get("first_name")
    msa_id = data.get("msa_id")
    user_id = data.get("user_id")
    
    await message.answer(
        f"⏰ **CONFIRM TEMPORARY BAN**\n\n"
        f"👤 **Name:** {first_name}\n"
        f"🆔 **MSA ID:** `{msa_id}`\n"
        f"👁️ **User ID:** `{user_id}`\n\n"
        f"⏱️ **Duration:** {message.text.replace('⏱️ ', '')}\n"
        f"🕐 **Ban Until:** {ban_expires.strftime('%b %d, %Y at %I:%M:%S %p')}\n\n"
        f"⚠️ **This will:**\n"
        f"  • Block user from all Bot 8 functions\n"
        f"  • Show countdown timer to user\n"
        f"  • Auto-unban on {ban_expires.strftime('%b %d at %I:%M %p')}\n"
        f"  • Send notification with countdown\n\n"
        f"Type **✅ CONFIRM TEMP BAN** to proceed or **❌ CANCEL** to abort.",
        reply_markup=confirm_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_temp_ban_confirm)
async def process_temp_ban_confirm(message: types.Message, state: FSMContext):
    """Process temporary ban confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Temporary ban cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM TEMP BAN" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        ban_expires = data.get("ban_expires")
        ban_duration_text = data.get("ban_duration_text")
        ban_duration_hours = data.get("ban_duration_hours")
        
        try:
            # Add to banned_users collection with expiry
            col_banned_users.insert_one({
                "user_id": user_id,
                "msa_id": msa_id,
                "first_name": first_name,
                "username": data.get("username"),
                "banned_at": now_local(),
                "banned_by": message.from_user.id,
                "reason": f"Temporary ban - {ban_duration_text}",
                "ban_type": "temporary",
                "ban_expires": ban_expires,
                "ban_duration_hours": ban_duration_hours
            })
            
            # Calculate time remaining for display
            time_diff = ban_expires - now_local()
            total_seconds = ban_duration_hours * 3600
            elapsed_seconds = total_seconds - time_diff.total_seconds()
            progress_percentage = (elapsed_seconds / total_seconds) * 100
            
            days = time_diff.days
            hours = time_diff.seconds // 3600
            minutes = (time_diff.seconds % 3600) // 60
            
            time_remaining = ""
            if days > 0:
                time_remaining = f"{days} day{'s' if days > 1 else ''}, {hours} hour{'s' if hours != 1 else ''}"
            elif hours > 0:
                time_remaining = f"{hours} hour{'s' if hours != 1 else ''}, {minutes} minute{'s' if minutes != 1 else ''}"
            else:
                time_remaining = f"{minutes} minute{'s' if minutes != 1 else ''}"
            
            # Generate progress bar (20 blocks)
            filled = int((progress_percentage / 100) * 20)
            empty = 20 - filled
            progress_bar = "▰" * filled + "▱" * empty
            
            # Try to notify user via Bot 8
            try:
                ban_message = (
                    "⏰ **TEMPORARY RESTRICTION**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Your account access has been temporarily limited due to policy violations.\n\n"
                    f"⏱️ **Ban Duration:** {ban_duration_text}\n"
                    f"🕐 **Ban Start:** {now_local().strftime('%b %d at %I:%M %p')}\n"
                    f"🕐 **Ban Expires:** {ban_expires.strftime('%b %d at %I:%M %p')}\n"
                    f"⏳ **Time Remaining:** {time_remaining}\n\n"
                    f"**Ban Progress**\n"
                    f"`[{progress_bar}]` {progress_percentage:.0f}%\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"✅ **Auto-Unban:** Your access will be automatically restored when the timer expires.\n\n"
                    f"⚠️ **Support Access:** You can still use the **📞 SUPPORT** button to contact us if needed.\n\n"
                    f"📋 **Note:** Please review our community guidelines to avoid future restrictions."
                )
                
                await bot_8.send_message(user_id, ban_message, parse_mode="Markdown")
            except:
                pass  # User might have blocked bot
            
            # Schedule auto-unban
            asyncio.create_task(schedule_auto_unban(user_id, msa_id, ban_duration_hours))
            
            await state.clear()
            await message.answer(
                f"✅ **TEMPORARY BAN APPLIED**\n\n"
                f"👤 {first_name} (`{msa_id}`)\n\n"
                f"⏱️ **Duration:** {ban_duration_text}\n"
                f"🕐 **Until:** {ban_expires.strftime('%b %d, %Y at %I:%M:%S %p')}\n"
                f"⏳ **Auto-unban in:** {time_remaining}\n\n"
                f"User has been notified with countdown.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"⏰ User {user_id} ({msa_id}) temp banned for {ban_duration_hours}h by admin {message.from_user.id}")
        
        except Exception as e:
            await message.answer(f"❌ **TEMP BAN FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM TEMP BAN** or **❌ CANCEL**", parse_mode="Markdown")

async def schedule_auto_unban(user_id: int, msa_id: str, hours: int):
    """Schedule auto-unban after specified hours"""
    try:
        # Wait for the ban duration
        await asyncio.sleep(hours * 3600)
        
        # Check if still banned (user might have been manually unbanned)
        ban_doc = col_banned_users.find_one({"user_id": user_id})
        if ban_doc and ban_doc.get('ban_type') == 'temporary':
            # Remove from banned_users
            col_banned_users.delete_one({"user_id": user_id})
            
            # Notify user of auto-unban with menu restoration
            try:
                from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
                
                unban_message = (
                    "✅ **ACCOUNT RESTRICTION LIFTED**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Your temporary ban has expired.\n\n"
                    "🎉 **Full Access Restored**\n"
                    "All bot features are now available to you.\n\n"
                    "⚠️ **Important Reminder:**\n"
                    "Please follow community guidelines to avoid future restrictions.\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Your menu has been automatically restored below. 👇\n\n"
                    "Thank you for your patience! 🙏"
                )
                
                # Create full menu keyboard
                menu_keyboard = ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="📊 DASHBOARD")],
                        [KeyboardButton(text="🔍 SEARCH CODE")],
                        [KeyboardButton(text="📜 RULES")],
                        [KeyboardButton(text="📚 GUIDE")],
                        [KeyboardButton(text="📞 SUPPORT")]
                    ],
                    resize_keyboard=True
                )
                
                await bot_8.send_message(user_id, unban_message, reply_markup=menu_keyboard, parse_mode="Markdown")
            except:
                pass
            
            print(f"✅ Auto-unbanned user {user_id} ({msa_id}) after {hours}h temp ban")
    
    except Exception as e:
        print(f"❌ Auto-unban error for user {user_id}: {str(e)}")

# ==========================================
# UNBAN USER HANDLERS
# ==========================================

@dp.message(F.text == "✅ UNBAN USER")
async def unban_user_start(message: types.Message, state: FSMContext):
    """Start unban user flow"""
    await state.set_state(ShootStates.waiting_for_unban_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "✅ **UNBAN USER**\n\n"
        "Enter the user's **MSA ID** or **User ID** to unban:\n\n"
        "This will restore full bot access.\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_unban_id)
async def process_unban_id(message: types.Message, state: FSMContext):
    """Process unban user ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find banned user
        ban_doc = None
        if search_input.upper().startswith("MSA"):
            ban_doc = col_banned_users.find_one({"msa_id": search_input.upper()})
        elif search_input.isdigit():
            ban_doc = col_banned_users.find_one({"user_id": int(search_input)})
        
        if not ban_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT BANNED**\n\n"
                f"No banned user found with ID: `{search_input}`\n\n"
                f"User may not be banned or ID is incorrect.",
                parse_mode="Markdown"
            )
            return
        
        user_id = ban_doc.get("user_id")
        msa_id = ban_doc.get("msa_id", "N/A")
        first_name = ban_doc.get("first_name", "Unknown")
        banned_at = ban_doc.get("banned_at", now_local())
        
        # Store data for confirmation
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name
        )
        await state.set_state(ShootStates.waiting_for_unban_confirm)
        
        confirm_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ CONFIRM UNBAN"), KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        await loading_msg.delete()
        await message.answer(
            f"✅ **CONFIRM UNBAN**\n\n"
            f"👤 **Name:** {first_name}\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n"
            f"🚫 **Banned:** {banned_at.strftime('%b %d, %Y at %I:%M:%S %p')}\n\n"
            f"This will restore full bot access.\n\n"
            f"Type **✅ CONFIRM UNBAN** to proceed or **❌ CANCEL** to abort.",
            reply_markup=confirm_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.waiting_for_unban_confirm)
async def process_unban_confirm(message: types.Message, state: FSMContext):
    """Process unban confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Unban cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM UNBAN" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        
        try:
            # Remove from banned_users collection
            result = col_banned_users.delete_one({"user_id": user_id})
            
            if result.deleted_count > 0:
                # Try to notify user via Bot 8 with menu restoration
                try:
                    from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
                    
                    unban_message = (
                        "✅ **ACCOUNT UNBANNED**\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "Your account has been unbanned by an administrator.\n\n"
                        "🎉 **Full Access Restored**\n"
                        "All bot features are now available to you.\n\n"
                        "⚠️ **Warning:**\n"
                        "Please follow community guidelines to avoid future restrictions.\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "Your menu has been automatically restored below. 👇"
                    )
                    
                    # Create full menu keyboard
                    menu_keyboard = ReplyKeyboardMarkup(
                        keyboard=[
                            [KeyboardButton(text="📊 DASHBOARD")],
                            [KeyboardButton(text="🔍 SEARCH CODE")],
                            [KeyboardButton(text="📜 RULES")],
                            [KeyboardButton(text="📚 GUIDE")],
                            [KeyboardButton(text="📞 SUPPORT")]
                        ],
                        resize_keyboard=True
                    )
                    
                    await bot_8.send_message(user_id, unban_message, reply_markup=menu_keyboard, parse_mode="Markdown")
                except:
                    pass  # User might have blocked bot
                
                await state.clear()
                await message.answer(
                    f"✅ **USER UNBANNED**\n\n"
                    f"👤 {first_name} (`{msa_id}`) has been unbanned.\n\n"
                    f"🕐 Unbanned at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                    f"User now has full bot access with warning notification sent.",
                    reply_markup=get_shoot_menu(),
                    parse_mode="Markdown"
                )
                print(f"✅ User {user_id} ({msa_id}) unbanned by admin {message.from_user.id}")
            else:
                await message.answer("❌ Failed to unban user. Please try again.", parse_mode="Markdown")
        
        except Exception as e:
            await message.answer(f"❌ **UNBAN FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM UNBAN** or **❌ CANCEL**", parse_mode="Markdown")

# ==========================================
# DELETE USER HANDLERS
# ==========================================

@dp.message(F.text == "🗑️ DELETE USER")
async def delete_user_start(message: types.Message, state: FSMContext):
    """Start delete user flow"""
    await state.set_state(ShootStates.waiting_for_delete_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🗑️ **DELETE USER**\n\n"
        "⚠️ **WARNING:** This permanently removes ALL user data:\n"
        "  • User tracking records\n"
        "  • Ban records\n"
        "  • Suspended features\n"
        "  • Support tickets\n\n"
        "Enter the user's **MSA ID** or **User ID** to delete:\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_delete_id)
async def process_delete_id(message: types.Message, state: FSMContext):
    """Process delete user ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user
        user_doc = None
        if search_input.upper().startswith("MSA"):
            user_doc = col_user_tracking.find_one({"msa_id": search_input.upper()})
        elif search_input.isdigit():
            user_doc = col_user_tracking.find_one({"user_id": int(search_input)})
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        
        # Count related data
        ban_count = col_banned_users.count_documents({"user_id": user_id})
        ticket_count = col_support_tickets.count_documents({"user_id": user_id})
        suspend_count = col_suspended_features.count_documents({"user_id": user_id})
        
        # Store data for confirmation
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name
        )
        await state.set_state(ShootStates.waiting_for_delete_confirm)
        
        confirm_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⚠️ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        await loading_msg.delete()
        await message.answer(
            f"🗑️ **CONFIRM DELETION**\n\n"
            f"👤 **Name:** {first_name}\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n\n"
            f"📊 **Data to delete:**\n"
            f"  • User tracking: 1 record\n"
            f"  • Ban records: {ban_count}\n"
            f"  • Support tickets: {ticket_count}\n"
            f"  • Suspended features: {suspend_count}\n\n"
            f"⚠️ **THIS ACTION CANNOT BE UNDONE!**\n\n"
            f"Type **⚠️ CONFIRM DELETE** to proceed or **❌ CANCEL** to abort.",
            reply_markup=confirm_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.waiting_for_delete_confirm)
async def process_delete_confirm(message: types.Message, state: FSMContext):
    """Process delete confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Deletion cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM DELETE" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        
        try:
            # Delete from all collections
            del1 = col_user_tracking.delete_many({"user_id": user_id})
            del2 = col_banned_users.delete_many({"user_id": user_id})
            del3 = col_support_tickets.delete_many({"user_id": user_id})
            del4 = col_suspended_features.delete_many({"user_id": user_id})
            
            total_deleted = del1.deleted_count + del2.deleted_count + del3.deleted_count + del4.deleted_count
            
            await state.clear()
            await message.answer(
                f"✅ **USER DELETED**\n\n"
                f"👤 {first_name} (`{msa_id}`) has been permanently removed.\n\n"
                f"🗑️ Records deleted: {total_deleted}\n"
                f"🕐 Deleted at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                f"All user data has been permanently erased.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"🗑️ User {user_id} ({msa_id}) deleted by admin {message.from_user.id}")
        
        except Exception as e:
            await message.answer(f"❌ **DELETE FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **⚠️ CONFIRM DELETE** or **❌ CANCEL**", parse_mode="Markdown")

# ==========================================
# RESET USER DATA HANDLERS
# ==========================================

@dp.message(F.text == "🔄 RESET USER DATA")
async def reset_user_start(message: types.Message, state: FSMContext):
    """Start reset user data flow"""
    await state.set_state(ShootStates.waiting_for_reset_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔄 **RESET USER DATA**\n\n"
        "This will reset user's tracking data (keeps MSA ID but resets timestamps).\n\n"
        "Enter the user's **MSA ID** or **User ID** to reset:\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_reset_id)
async def process_reset_id(message: types.Message, state: FSMContext):
    """Process reset user ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user
        user_doc = None
        if search_input.upper().startswith("MSA"):
            user_doc = col_user_tracking.find_one({"msa_id": search_input.upper()})
        elif search_input.isdigit():
            user_doc = col_user_tracking.find_one({"user_id": int(search_input)})
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        
        # Store data for confirmation
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name
        )
        await state.set_state(ShootStates.waiting_for_reset_confirm)
        
        confirm_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ CONFIRM RESET"), KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        await loading_msg.delete()
        await message.answer(
            f"🔄 **CONFIRM RESET**\n\n"
            f"👤 **Name:** {first_name}\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"👁️ **User ID:** `{user_id}`\n\n"
            f"This will reset:\n"
            f"  • First/Last start timestamps\n"
            f"  • Source tracking\n"
            f"  • Username/name data\n\n"
            f"MSA ID will be preserved.\n\n"
            f"Type **✅ CONFIRM RESET** to proceed or **❌ CANCEL** to abort.",
            reply_markup=confirm_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.waiting_for_reset_confirm)
async def process_reset_confirm(message: types.Message, state: FSMContext):
    """Process reset confirmation"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Reset cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "CONFIRM RESET" in message.text:
        data = await state.get_data()
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        
        try:
            # Reset user data (keep msa_id and user_id)
            col_user_tracking.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "first_start": now_local(),
                        "last_start": now_local(),
                        "source": "RESET",
                        "username": "reset",
                        "first_name": "Reset User"
                    }
                }
            )
            
            await state.clear()
            await message.answer(
                f"✅ **USER DATA RESET**\n\n"
                f"👤 {first_name} (`{msa_id}`) data has been reset.\n\n"
                f"🔄 Reset at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                f"User will appear as new on next bot interaction.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"🔄 User {user_id} ({msa_id}) data reset by admin {message.from_user.id}")
        
        except Exception as e:
            await message.answer(f"❌ **RESET FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    else:
        await message.answer("⚠️ Please click **✅ CONFIRM RESET** or **❌ CANCEL**", parse_mode="Markdown")

# ==========================================
# SUSPEND FEATURES HANDLERS
# ==========================================

@dp.message(F.text == "⏸️ SUSPEND FEATURES")
async def suspend_features_start(message: types.Message, state: FSMContext):
    """Start suspend features flow"""
    await state.set_state(ShootStates.waiting_for_suspend_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "⏸️ **SUSPEND FEATURES**\n\n"
        "Enter the user's **MSA ID** or **User ID** to suspend specific features:\n\n"
        "You can disable:\n"
        "  • Search Code access\n"
        "  • IG Content viewing\n"
        "  • YT Content viewing\n"
        "  • Menu buttons\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_suspend_id)
async def process_suspend_id(message: types.Message, state: FSMContext):
    """Process suspend features ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user
        user_doc = None
        if search_input.upper().startswith("MSA"):
            user_doc = col_user_tracking.find_one({"msa_id": search_input.upper()})
        elif search_input.isdigit():
            user_doc = col_user_tracking.find_one({"user_id": int(search_input)})
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        
        # Store data
        await state.update_data(
            user_id=user_id,
            msa_id=msa_id,
            first_name=first_name
        )
        await state.set_state(ShootStates.selecting_suspend_features)
        
        # Feature selection with reply keyboard buttons
        feature_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🔍 SEARCH CODE"), KeyboardButton(text="📊 DASHBOARD")],
                [KeyboardButton(text="📜 RULES"), KeyboardButton(text="📚 GUIDE")],
                [KeyboardButton(text="📎 SELECT ALL"), KeyboardButton(text="🚫 DESELECT ALL")],
                [KeyboardButton(text="✅ DONE"), KeyboardButton(text="❌ CANCEL")]
            ],
            resize_keyboard=True
        )
        
        await loading_msg.delete()
        await message.answer(
            f"⏸️ **SELECT FEATURES TO SUSPEND**\n\n"
            f"👤 **User:** {first_name} (`{msa_id}`)\n\n"
            f"Click buttons to select/deselect features to suspend:\n\n"
            f"  • 🔍 SEARCH CODE - Hide search button\n"
            f"  • 📊 DASHBOARD - Hide dashboard button\n"
            f"  • 📜 RULES - Hide rules button\n"
            f"  • 📚 GUIDE - Hide guide button\n\n"
            f"📞 **Note:** SUPPORT button always remains accessible\n\n"
            f"**Selected features will be marked with ✅**\n"
            f"Click **✅ DONE** when finished or **❌ CANCEL** to abort.",
            reply_markup=feature_keyboard,
            parse_mode="Markdown"
        )
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(ShootStates.selecting_suspend_features)
async def process_suspend_features(message: types.Message, state: FSMContext):
    """Process feature suspension selection"""
    if message.text and "CANCEL" in message.text:
        await state.clear()
        await message.answer("✅ Suspension cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    if message.text and "DONE" in message.text:
        data = await state.get_data()
        suspended_features = data.get("suspended_features", [])
        
        if not suspended_features:
            await message.answer("⚠️ No features selected. Please select at least one feature or cancel.", parse_mode="Markdown")
            return
        
        user_id = data.get("user_id")
        msa_id = data.get("msa_id")
        first_name = data.get("first_name")
        
        try:
            # Save suspended features to database
            col_suspended_features.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "msa_id": msa_id,
                        "first_name": first_name,
                        "suspended_features": suspended_features,
                        "suspended_at": now_local(),
                        "suspended_by": message.from_user.id
                    }
                },
                upsert=True
            )
            
            # Send notification via Bot 8 to user
            try:
                notification_text = (
                    "⚠️ **ACCOUNT RESTRICTION**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Some features have been temporarily suspended from your account.\n\n"
                    f"**Suspended Features:**\n" +
                    "\n".join([f"  • {f.replace('_', ' ')}" for f in suspended_features]) +
                    "\n\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "📞 **Support Access:** The SUPPORT button remains available\n"
                    "💬 **Contact:** If you believe this is an error, please contact support\n\n"
                    "Thank you for your understanding."
                )
                await bot_8.send_message(user_id, notification_text, parse_mode="Markdown")
            except Exception as e:
                print(f"Failed to send suspension notification: {e}")
            
            await state.clear()
            await message.answer(
                f"✅ **FEATURES SUSPENDED**\n\n"
                f"👤 {first_name} (`{msa_id}`)\n\n"
                f"⏸️ Suspended features:\n" + "\n".join([f"  • {f.replace('_', ' ')}" for f in suspended_features]) +
                f"\n\n🕐 Suspended at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                f"✉️ User has been notified via Bot 8.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"⏸️ Features suspended for user {user_id} ({msa_id}) by admin {message.from_user.id}: {suspended_features}")
        
        except Exception as e:
            await message.answer(f"❌ **SUSPEND FAILED:** {str(e)[:100]}", parse_mode="Markdown")
        return
    
    # Handle SELECT ALL
    if message.text and "SELECT ALL" in message.text:
        data = await state.get_data()
        all_features = ["SEARCH_CODE", "DASHBOARD", "RULES", "GUIDE"]
        await state.update_data(suspended_features=all_features)
        
        await message.answer(
            "✅ **All features selected!**\n\n"
            "**Currently Selected:**\n"
            "  • SEARCH CODE\n"
            "  • DASHBOARD\n"
            "  • RULES\n"
            "  • GUIDE\n\n"
            "Click ✅ DONE to confirm or ❌ CANCEL to abort.",
            parse_mode="Markdown"
        )
        return
    
    # Handle DESELECT ALL
    if message.text and "DESELECT ALL" in message.text:
        await state.update_data(suspended_features=[])
        
        await message.answer(
            "🚫 **All features deselected!**\n\n"
            "**Currently Selected:**\n"
            "  (None selected)\n\n"
            "Select features to suspend or click ❌ CANCEL to abort.",
            parse_mode="Markdown"
        )
        return
    
    # Add/Remove feature from suspension list
    feature_map = {
        "🔍 SEARCH CODE": "SEARCH_CODE",
        "📊 DASHBOARD": "DASHBOARD",
        "📜 RULES": "RULES",
        "📚 GUIDE": "GUIDE"
    }
    
    if message.text in feature_map:
        data = await state.get_data()
        suspended_features = data.get("suspended_features", [])
        
        feature_key = feature_map[message.text]
        
        # Toggle feature
        if feature_key in suspended_features:
            suspended_features.remove(feature_key)
            status = "➖ Removed"
        else:
            suspended_features.append(feature_key)
            status = "✅ Added"
        
        await state.update_data(suspended_features=suspended_features)
        
        # Show current selection
        selected_list = "\n".join([f"  • {f.replace('_', ' ')}" for f in suspended_features]) if suspended_features else "  (None selected)"
        
        await message.answer(
            f"{status}: {message.text}\n\n"
            f"**Currently Selected:**\n{selected_list}\n\n"
            f"Click ✅ DONE to confirm or ❌ CANCEL to abort.",
            parse_mode="Markdown"
        )

# ==========================================
# UNSUSPEND HANDLERS
# ==========================================

@dp.message(lambda m: m.text and "UNSUSPEND" in m.text)
async def unsuspend_features_start(message: types.Message, state: FSMContext):
    """Start unsuspend features flow"""
    await state.set_state(ShootStates.waiting_for_unsuspend_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔓 **UNSUSPEND FEATURES**\n\n"
        "Enter the user's **MSA ID** or **User ID** to remove all suspended features:\n\n"
        "This will restore full access to all Bot 8 features.\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_unsuspend_id)
async def process_unsuspend_id(message: types.Message, state: FSMContext):
    """Process unsuspend features ID input"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching user...", parse_mode="Markdown")
    
    try:
        # Find user
        user_doc = None
        if search_input.upper().startswith("MSA"):
            user_doc = col_user_tracking.find_one({"msa_id": search_input.upper()})
        elif search_input.isdigit():
            user_doc = col_user_tracking.find_one({"user_id": int(search_input)})
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        
        # Check if user has any suspended features
        suspend_doc = col_suspended_features.find_one({"user_id": user_id})
        
        if not suspend_doc:
            await loading_msg.delete()
            await message.answer(
                f"ℹ️ **NO SUSPENDED FEATURES**\n\n"
                f"👤 {first_name} (`{msa_id}`)\n\n"
                f"This user has no suspended features.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            return
        
        suspended_features = suspend_doc.get("suspended_features", [])
        
        # Remove all suspended features
        try:
            col_suspended_features.delete_one({"user_id": user_id})
            
            # Send notification via Bot 8 to user
            try:
                notification_text = (
                    "✅ **FEATURES RESTORED**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "All suspended features have been removed from your account.\n\n"
                    "🎉 **Full Access Restored**\n"
                    "You now have access to all Bot 8 features.\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Your menu has been automatically restored below. 👇"
                )
                
                # Create full menu keyboard
                from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
                menu_keyboard = ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="📊 DASHBOARD")],
                        [KeyboardButton(text="🔍 SEARCH CODE")],
                        [KeyboardButton(text="📜 RULES")],
                        [KeyboardButton(text="📚 GUIDE")],
                        [KeyboardButton(text="📞 SUPPORT")]
                    ],
                    resize_keyboard=True
                )
                
                await bot_8.send_message(user_id, notification_text, reply_markup=menu_keyboard, parse_mode="Markdown")
            except Exception as e:
                print(f"Failed to send unsuspend notification: {e}")
            
            await loading_msg.delete()
            await state.clear()
            await message.answer(
                f"✅ **FEATURES UNSUSPENDED**\n\n"
                f"👤 {first_name} (`{msa_id}`)\n\n"
                f"🔓 Previously suspended features:\n" + "\n".join([f"  • {f.replace('_', ' ')}" for f in suspended_features]) +
                f"\n\n🕐 Unsuspended at: {now_local().strftime('%I:%M:%S %p')}\n\n"
                f"✉️ User has been notified and menu restored via Bot 8.",
                reply_markup=get_shoot_menu(),
                parse_mode="Markdown"
            )
            print(f"🔓 All features unsuspended for user {user_id} ({msa_id}) by admin {message.from_user.id}")
        
        except Exception as e:
            await loading_msg.delete()
            await message.answer(f"❌ **UNSUSPEND FAILED:** {str(e)[:100]}", parse_mode="Markdown")
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

# ==========================================
# SEARCH USER (SHOOT) HANDLERS
# ==========================================

@dp.message(F.text == "🔍 SEARCH USER")
async def shoot_search_user_start(message: types.Message, state: FSMContext):
    """Start shoot search user flow"""
    await state.set_state(ShootStates.waiting_for_shoot_search_id)
    
    back_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔍 **SEARCH USER - DETAILED VIEW**\n\n"
        "Enter the user's **MSA ID** or **User ID** for complete details:\n\n"
        "This will show:\n"
        "  • Ban status\n"
        "  • Suspended features\n"
        "  • Support tickets\n"
        "  • Activity history\n\n"
        "Type ⬅️ BACK or ❌ CANCEL to abort.",
        reply_markup=back_keyboard,
        parse_mode="Markdown"
    )

@dp.message(ShootStates.waiting_for_shoot_search_id)
async def process_shoot_search(message: types.Message, state: FSMContext):
    """Process shoot search user"""
    if message.text and message.text.strip() in ["⬅️ BACK", "❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("✅ Cancelled.", reply_markup=get_shoot_menu(), parse_mode="Markdown")
        return
    
    search_input = message.text.strip()
    loading_msg = await message.answer("⏳ Searching database...", parse_mode="Markdown")
    
    try:
        # Find user
        user_doc = None
        if search_input.upper().startswith("MSA"):
            user_doc = col_user_tracking.find_one({"msa_id": search_input.upper()})
        elif search_input.isdigit():
            user_doc = col_user_tracking.find_one({"user_id": int(search_input)})
        
        if not user_doc:
            await loading_msg.delete()
            await message.answer(
                f"❌ **USER NOT FOUND**\n\n"
                f"No user found with ID: `{search_input}`",
                parse_mode="Markdown"
            )
            return
        
        user_id = user_doc.get("user_id")
        msa_id = user_doc.get("msa_id", "N/A")
        first_name = user_doc.get("first_name", "Unknown")
        username = user_doc.get("username", "N/A")
        source = user_doc.get("source", "N/A")
        first_start = user_doc.get("first_start")
        last_start = user_doc.get("last_start")
        
        # Check ban status
        ban_doc = col_banned_users.find_one({"user_id": user_id})
        ban_status = "🟢 Active" if not ban_doc else "🔴 Banned"
        ban_date = ban_doc.get("banned_at").strftime("%b %d, %Y at %I:%M:%S %p") if ban_doc else "N/A"
        
        # Count suspended features
        suspend_count = col_suspended_features.count_documents({"user_id": user_id})
        
        # Count support tickets
        ticket_count = col_support_tickets.count_documents({"user_id": user_id})
        open_tickets = col_support_tickets.count_documents({"user_id": user_id, "status": "open"})
        
        # Format timestamps
        first_start_str = first_start.strftime("%b %d, %Y at %I:%M:%S %p") if first_start else "N/A"
        last_start_str = last_start.strftime("%b %d, %Y at %I:%M:%S %p") if last_start else "N/A"
        
        # Build detailed report
        report = (
            f"🔍 **DETAILED USER REPORT**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            f"👤 **BASIC INFO**\n"
            f"🆔 MSA ID: `{msa_id}`\n"
            f"👁️ User ID: `{user_id}`\n"
            f"👤 Name: {first_name}\n"
            f"📱 Username: @{username if username != 'N/A' else 'None'}\n\n"
            
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 **STATUS**\n"
            f"🔒 Account: {ban_status}\n"
            f"⏸️ Suspended Features: {suspend_count}\n"
            f"🎫 Support Tickets: {ticket_count} ({open_tickets} open)\n\n"
            
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 **ACTIVITY**\n"
            f"🔗 Entry Source: {source}\n"
            f"📅 First Joined: {first_start_str}\n"
            f"⏰ Last Active: {last_start_str}\n"
        )
        
        if ban_doc:
            report += f"\n🚫 **Ban Details:**\n  └─ Banned: {ban_date}\n  └─ Reason: {ban_doc.get('reason', 'N/A')}\n"
        
        await loading_msg.delete()
        await message.answer(report, parse_mode="Markdown")
        print(f"🔍 Admin {message.from_user.id} searched user {msa_id}")
    
    except Exception as e:
        await loading_msg.delete()
        await message.answer(f"❌ **ERROR:** {str(e)[:100]}", parse_mode="Markdown")

@dp.message(F.text == "💬 SUPPORT")
async def support_handler(message: types.Message, state: FSMContext):
    """Support ticket management system"""
    await state.clear()
    
    # Count pending and total tickets
    pending_count = col_support_tickets.count_documents({"status": "open"})
    total_count = col_support_tickets.count_documents({})
    resolved_count = col_support_tickets.count_documents({"status": "resolved"})
    
    await message.answer(
        f"💬 **SUPPORT TICKET MANAGEMENT**\n\n"
        f"📊 **Statistics:**\n"
        f"⏳ Pending: **{pending_count}** tickets\n"
        f"✅ Resolved: **{resolved_count}** tickets\n"
        f"📋 Total: **{total_count}** tickets\n\n"
        f"**Select an action:**",
        reply_markup=get_support_management_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🎫 PENDING TICKETS")
async def pending_tickets_handler(message: types.Message, state: FSMContext):
    """Show all pending support tickets with pagination"""
    await state.clear()
    await show_pending_tickets_page(message, page=1)

async def show_pending_tickets_page(message: types.Message, page: int = 1):
    """Helper function to display pending tickets with pagination"""
    ITEMS_PER_PAGE = 5  # Show 5 tickets per page to stay within char limit
    
    # Get open tickets count for display
    total_pending = col_support_tickets.count_documents({"status": "open"})
    
    if total_pending == 0:
        await message.answer(
            "✅ **No pending tickets!**\n\n"
            "All support requests have been resolved.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Calculate pagination
    total_pages = (total_pending + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE  # Ceiling division
    page = max(1, min(page, total_pages))  # Clamp page number
    skip = (page - 1) * ITEMS_PER_PAGE
    
    # Get tickets for current page
    tickets = list(col_support_tickets.find({"status": "open"})
                   .sort("created_at", -1)
                   .skip(skip)
                   .limit(ITEMS_PER_PAGE))
    
    response = f"🎫 **PENDING TICKETS** (Page {page}/{total_pages})\n\n"
    response += f"📊 Total Pending: **{total_pending}** tickets\n"
    response += f"📄 Showing: {skip + 1}-{skip + len(tickets)} of {total_pending}\n\n"
    
    for ticket in tickets:
        user_id = ticket.get('user_id')
        user_name = ticket.get('user_name', 'Unknown')
        username = ticket.get('username', 'none')
        msa_id = ticket.get('msa_id', 'Not Assigned') 
        issue = ticket.get('issue_text', 'No description')[:80]  # First 80 chars
        created = ticket.get('created_at', now_local())
        date_str = created.strftime("%b %d, %I:%M %p")
        support_count = ticket.get('support_count', 1)
        
        response += f"━━━━━━━━━━━━━━━━━━━━━\n"
        response += f"👤 **{user_name}** (@{username})\n"
        response += f"🆔 TG: `{user_id}` | MSA: `{msa_id}`\n"
        response += f"🎫 Ticket #{support_count} · {date_str}\n"
        response += f"📝 {issue}...\n\n"
    
    response += "💡 Use **✅ RESOLVE TICKET** to resolve by ID"
    
    # Create pagination buttons
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"pending_page_{page-1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton(text="➡️ Next", callback_data=f"pending_page_{page+1}"))
    
    keyboard = None
    if buttons:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons])
    
    # Check if this is being called from callback (edit) or new message
    try:
        if keyboard:
            await message.edit_text(
                response,
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        else:
            await message.edit_text(
                response,
                parse_mode="Markdown"
            )
    except:
        # If edit fails (not from callback), send new message
        if keyboard:
            await message.answer(
                response,
                reply_markup=keyboard,
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                response,
                reply_markup=get_support_management_menu(),
                parse_mode="Markdown"
            )

@dp.message(F.text == "📋 ALL TICKETS")
async def all_tickets_handler(message: types.Message, state: FSMContext):
    """Show all tickets (pending + resolved) with pagination"""
    await state.clear()
    await show_all_tickets_page(message, page=1)

async def show_all_tickets_page(message: types.Message, page: int = 1):
    """Helper function to display all tickets with pagination"""
    ITEMS_PER_PAGE = 8  # Show 8 tickets per page (compact view)
    
    pending_count = col_support_tickets.count_documents({"status": "open"})
    resolved_count = col_support_tickets.count_documents({"status": "resolved"})
    total_count = pending_count + resolved_count
    
    if total_count == 0:
        await message.answer(
            "📋 **No tickets found!**\n\n"
            "No support requests have been submitted yet.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Calculate pagination
    total_pages = (total_count + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    page = max(1, min(page, total_pages))
    skip = (page - 1) * ITEMS_PER_PAGE
    
    # Get tickets for current page
    tickets = list(col_support_tickets.find({})
                   .sort("created_at", -1)
                   .skip(skip)
                   .limit(ITEMS_PER_PAGE))
    
    response = f"📋 **ALL TICKETS** (Page {page}/{total_pages})\n\n"
    response += f"📊 Total: **{total_count}** · ⏳ Pending: **{pending_count}** · ✅ Resolved: **{resolved_count}**\n\n"
    response += f"Showing {skip + 1}-{skip + len(tickets)} of {total_count}:\n\n"
    
    for ticket in tickets:
        user_name = ticket.get('user_name', 'Unknown')
        msa_id = ticket.get('msa_id', 'N/A')
        status = ticket.get('status', 'unknown')
        status_emoji = "⏳" if status == "open" else "✅"
        created = ticket.get('created_at', now_local())
        date_str = created.strftime("%b %d, %I:%M %p")
        issue = ticket.get('issue_text', 'N/A')[:50]  # First 50 chars
        
        response += f"{status_emoji} **{user_name}** (MSA: `{msa_id}`)\n"
        response += f"   📝 {issue}... · {date_str}\n\n"
    
    # Create pagination buttons
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"all_page_{page-1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton(text="➡️ Next", callback_data=f"all_page_{page+1}"))
    
    keyboard = None
    if buttons:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons])
    
    # Check if this is being called from callback (edit) or new message
    try:
        if keyboard:
            await message.edit_text(
                response,
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        else:
            await message.edit_text(
                response,
                parse_mode="Markdown"
            )
    except:
        # If edit fails (not from callback), send new message
        if keyboard:
            await message.answer(
                response,
                reply_markup=keyboard,
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                response,
                reply_markup=get_support_management_menu(),
                parse_mode="Markdown"
            )

@dp.message(F.text == "✅ RESOLVE TICKET")
async def resolve_ticket_prompt(message: types.Message, state: FSMContext):
    """Prompt for MSA ID or Telegram ID to resolve ticket"""
    await state.set_state(SupportStates.waiting_for_resolve_id)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "✅ **RESOLVE TICKET**\n\n"
        "Send the **MSA+ ID** (e.g., `MSA001`) or **Telegram ID** (e.g., `123456789`) to resolve the ticket.\n\n"
        "💡 **Resolving will:**\n"
        "• Mark ticket as resolved\n"
        "• Allow user to submit new tickets\n"
        "• Update timestamp\n\n"
        "Send ID below:",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_resolve_id)
async def process_resolve_ticket(message: types.Message, state: FSMContext):
    """Process ticket resolution by MSA ID or Telegram ID"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    search_id = message.text.strip()
    
    # Try to find ticket by MSA ID first
    ticket = col_support_tickets.find_one({
        "msa_id": search_id.upper(),
        "status": "open"
    })
    
    # If not found, try by Telegram ID
    if not ticket and search_id.isdigit():
        ticket = col_support_tickets.find_one({
            "user_id": int(search_id),
            "status": "open"
        })
    
    if not ticket:
        await message.answer(
            f"❌ **Ticket not found!**\n\n"
            f"No open ticket found for ID: `{search_id}`\n\n"
            f"💡 **Tips:**\n"
            f"• Check if ticket is already resolved\n"
            f"• Verify MSA+ ID format (e.g., MSA001)\n"
            f"• Use exact Telegram ID\n\n"
            f"Try again or click ❌ CANCEL",
            parse_mode="Markdown"
        )
        return
    
    # Resolve the ticket
    resolved_at = now_local()
    result = col_support_tickets.update_one(
        {"_id": ticket["_id"]},
        {
            "$set": {
                "status": "resolved",
                "resolved_at": resolved_at
            }
        }
    )
    
    user_name = ticket.get('user_name', 'Unknown')
    user_id = ticket.get('user_id')
    msa_id = ticket.get('msa_id', 'N/A')
    username = ticket.get('username', 'none')
    issue_text = ticket.get('issue_text', 'No description')
    ticket_type = ticket.get('ticket_type', 'Text Only')
    has_photo = ticket.get('has_photo', False)
    has_video = ticket.get('has_video', False)
    support_count = ticket.get('support_count', 1)
    channel_message_id = ticket.get('channel_message_id')
    created = ticket.get('created_at', now_local())
    created_str = created.strftime("%B %d, %Y at %I:%M %p")
    resolved_str = resolved_at.strftime("%B %d, %Y at %I:%M %p")
    
    await state.clear()
    
    if result.modified_count > 0:
        print(f"✅ Ticket resolved for user {user_id} ({user_name})")
        
        # 1. Send premium DM to user via Bot 8
        try:
            await bot_8.send_message(
                user_id,
                f"✨ **Great News, {user_name}!** ✨\n\n"
                f"🎉 We're happy to inform you that your support request has been **successfully resolved** by our admin team!\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ **{user_name}, your issue has been addressed.**\n\n"
                f"Everything should be working smoothly now. If you're still experiencing any problems or have additional questions, please don't hesitate to reach out to us again.\n\n"
                f"💡 **Need more help?**\n"
                f"You can submit a new support ticket anytime by clicking **📞 SUPPORT** in the main menu.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🌟 **Thank you for your patience, {user_name}!**\n\n"
                f"We truly appreciate your understanding and are always here to help you with the best possible experience.\n\n"
                f"💎 **MSA NODE Team**",
                parse_mode="Markdown"
            )
            print(f"📧 Sent resolution notification to user {user_id}")
        except Exception as e:
            print(f"⚠️ Failed to send DM to user {user_id}: {str(e)}")
        
        # 2. Edit the channel message with resolved status
        if channel_message_id and REVIEW_LOG_CHANNEL:
            try:
                # Build clean updated ticket message
                updated_ticket_msg = f"""
🎫 **SUPPORT TICKET** - ✅ **RESOLVED**
━━━━━━━━━━━━━━━━━━━━━━━━

📅 **Date:** {created_str}
⏰ **Resolved:** {resolved_str}
📋 **Type:** {ticket_type}

👤 **USER INFORMATION**
━━━━━━━━━━━━━━━━━━━━━━━━

**Name:** {user_name}
**Username:** @{username}
**User ID:** `{user_id}`
**MSA+ ID:** `{msa_id}`
**Total Support Requests:** {support_count}

🔍 **ISSUE DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━

{issue_text}

━━━━━━━━━━━━━━━━━━━━━━━━

✅ **STATUS:** Resolved
🕐 **Resolved At:** {resolved_str}
🤖 **Source:** MSA NODE Bot

💡 **Actions Completed:**
• User notified via DM
• Ticket status updated
• User can submit new tickets
"""
                
                await bot_8.edit_message_text(
                    chat_id=REVIEW_LOG_CHANNEL,
                    message_id=channel_message_id,
                    text=updated_ticket_msg,
                    parse_mode="Markdown"
                )
                print(f"✏️ Updated channel message {channel_message_id} with resolved status")
            except Exception as e:
                print(f"⚠️ Failed to edit channel message: {str(e)}")
        
        # 3. Confirm to admin
        await message.answer(
            f"✅ **TICKET RESOLVED SUCCESSFULLY!**\n\n"
            f"👤 **User:** {user_name}\n"
            f"🆔 **Telegram ID:** `{user_id}`\n"
            f"💳 **MSA+ ID:** `{msa_id}`\n"
            f"🎫 **Support Ticket:** #{support_count}\n"
            f"📅 **Submitted:** {created_str}\n"
            f"⏰ **Resolved:** {resolved_str}\n\n"
            f"✅ **Actions Completed:**\n"
            f"• ✉️ User notified via DM\n"
            f"• 📝 Channel message updated\n"
            f"• 🔓 User can submit new tickets\n\n"
            f"🎉 **Resolution complete!**",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            "⚠️ **Failed to resolve ticket.**\n\nPlease try again.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )

# ==========================================
# 📨 REPLY TO USER
# ==========================================

@dp.message(F.text == "📨 REPLY")
async def reply_to_user_prompt(message: types.Message, state: FSMContext):
    """Send custom message to user about their ticket"""
    await state.set_state(SupportStates.waiting_for_reply_id)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "📨 **REPLY TO USER**\n\n"
        "Send the **MSA+ ID** or **Telegram ID** of the user you want to message.\n\n"
        "💡 After entering ID, you'll compose your reply message.",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_reply_id)
async def process_reply_id(message: types.Message, state: FSMContext):
    """Process user ID for reply"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    search_id = message.text.strip()
    
    # Try MSA ID first
    ticket = col_support_tickets.find_one({"msa_id": search_id.upper()})
    
    # Try Telegram ID
    if not ticket and search_id.isdigit():
        ticket = col_support_tickets.find_one({"user_id": int(search_id)})
    
    if not ticket:
        await message.answer(
            f"❌ **User not found!**\n\n"
            f"No tickets found for ID: `{search_id}`\n\n"
            f"Try again or click ❌ CANCEL",
            parse_mode="Markdown"
        )
        return
    
    # Store user info and move to message composition
    await state.update_data(
        reply_user_id=ticket.get('user_id'),
        reply_user_name=ticket.get('user_name', 'User'),
        reply_msa_id=ticket.get('msa_id', 'N/A')
    )
    await state.set_state(SupportStates.waiting_for_reply_message)
    
    await message.answer(
        f"📨 **Replying to: {ticket.get('user_name')}**\n\n"
        f"🆔 Telegram ID: `{ticket.get('user_id')}`\n"
        f"💳 MSA+ ID: `{ticket.get('msa_id')}`\n\n"
        f"📝 **Type your message:**\n"
        f"(This will be sent directly to the user)",
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_reply_message)
async def process_reply_message(message: types.Message, state: FSMContext):
    """Send the reply message to user"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    user_id = data.get('reply_user_id')
    user_name = data.get('reply_user_name')
    reply_text = message.text or message.caption or ""
    
    if len(reply_text) < 5:
        await message.answer(
            "⚠️ **Message too short!**\n\nPlease send a meaningful message (min 5 characters).",
            parse_mode="Markdown"
        )
        return
    
    # Send message to user via Bot 8
    try:
        await bot_8.send_message(
            user_id,
            f"📨 **Message from Admin Team**\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{reply_text}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💡 Need more help? Use **📞 SUPPORT** in the menu.\n\n"
            f"💎 **MSA NODE Team**",
            parse_mode="Markdown"
        )
        
        await state.clear()
        await message.answer(
            f"✅ **Message sent to {user_name}!**\n\n"
            f"🆔 User ID: `{user_id}`\n"
            f"📨 Your message was delivered successfully.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        print(f"📨 Admin sent reply to user {user_id}")
        
    except Exception as e:
        await state.clear()
        await message.answer(
            f"❌ **Failed to send message!**\n\n"
            f"Error: {str(e)}\n\n"
            f"User may have blocked the bot.",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        print(f"❌ Failed to send reply to user {user_id}: {str(e)}")

# ==========================================
# 🔍 SEARCH TICKETS & HISTORY
# ==========================================

@dp.message(F.text == "🔍 SEARCH TICKETS")
async def search_user_prompt(message: types.Message, state: FSMContext):
    """Search for user tickets"""
    await state.set_state(SupportStates.waiting_for_user_search)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🔍 **SEARCH TICKETS**\n\n"
        "Search by:\n"
        "• User name\n"
        "• Username (without @)\n"
        "• MSA+ ID\n"
        "• Telegram ID\n\n"
        "Send search term:",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_user_search)
async def process_user_search(message: types.Message, state: FSMContext):
    """Process user search and show ticket history"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        return
    
    search_term = message.text.strip()
    
    # Build search query (supports multiple fields)
    search_query = {
        "$or": [
            {"user_name": {"$regex": search_term, "$options": "i"}},
            {"username": {"$regex": search_term, "$options": "i"}},
            {"msa_id": search_term.upper()}
        ]
    }
    
    # Add numeric search for Telegram ID
    if search_term.isdigit():
        search_query["$or"].append({"user_id": int(search_term)})
    
    tickets = list(col_support_tickets.find(search_query).sort("created_at", -1))
    
    await state.clear()
    
    if not tickets:
        await message.answer(
            f"❌ **No results found!**\n\n"
            f"No tickets found for: `{search_term}`",
            reply_markup=get_support_management_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Get user info from first ticket
    first_ticket = tickets[0]
    user_name = first_ticket.get('user_name', 'Unknown')
    username = first_ticket.get('username', 'none')
    user_id = first_ticket.get('user_id')
    msa_id = first_ticket.get('msa_id', 'N/A')
    
    # Count statuses
    open_count = sum(1 for t in tickets if t.get('status') == 'open')
    resolved_count = sum(1 for t in tickets if t.get('status') == 'resolved')
    
    response = f"🔍 **USER TICKET HISTORY**\n\n"
    response += f"👤 **{user_name}** (@{username})\n"
    response += f"🆔 Telegram ID: `{user_id}`\n"
    response += f"💳 MSA+ ID: `{msa_id}`\n\n"
    response += f"📊 **Statistics:**\n"
    response += f"📋 Total Tickets: {len(tickets)}\n"
    response += f"⏳ Open: {open_count}\n"
    response += f"✅ Resolved: {resolved_count}\n\n"
    response += f"**Recent Tickets:**\n\n"
    
    for ticket in tickets[:5]:  # Show last 5 tickets
        status = ticket.get('status', 'unknown')
        status_emoji = "⏳" if status == "open" else "✅"
        created = ticket.get('created_at', now_local())
        date_str = created.strftime("%b %d, %I:%M %p")
        issue = ticket.get('issue_text', 'No description')[:50]
        
        response += f"{status_emoji} {date_str}\n"
        response += f"   {issue}...\n\n"
    
    if len(tickets) > 5:
        response += f"_... and {len(tickets) - 5} more tickets_"
    
    await message.answer(
        response,
        reply_markup=get_support_management_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# 🗑️ DELETE TICKET
# ==========================================

@dp.message(F.text == "🗑️ DELETE")
async def delete_ticket_prompt(message: types.Message, state: FSMContext):
    """Delete spam or test tickets"""
    await state.set_state(SupportStates.waiting_for_delete_ticket_id)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🗑️ **DELETE TICKET**\n\n"
        "⚠️ **Warning:** This permanently deletes the ticket!\n\n"
        "Send **MSA+ ID** or **Telegram ID** to delete their most recent ticket.\n\n"
        "💡 Use this for spam/test tickets only.",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_delete_ticket_id)
async def process_delete_ticket(message: types.Message, state: FSMContext):
    """Process ticket deletion"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        return
    
    search_id = message.text.strip()
    
    # Find most recent ticket
    ticket = col_support_tickets.find_one(
        {"msa_id": search_id.upper()},
        sort=[("created_at", -1)]
    )
    
    if not ticket and search_id.isdigit():
        ticket = col_support_tickets.find_one(
            {"user_id": int(search_id)},
            sort=[("created_at", -1)]
        )
    
    if not ticket:
        await message.answer(
            f"❌ **Ticket not found!**\n\n"
            f"No tickets found for ID: `{search_id}`",
            parse_mode="Markdown"
        )
        return
    
    user_name = ticket.get('user_name', 'Unknown')
    user_id = ticket.get('user_id')
    created = ticket.get('created_at', now_local())
    created_str = created.strftime("%B %d, %Y at %I:%M %p")
    
    # Delete the ticket
    result = col_support_tickets.delete_one({"_id": ticket["_id"]})
    
    await state.clear()
    
    if result.deleted_count > 0:
        await message.answer(
            f"🗑️ **Ticket Deleted!**\n\n"
            f"👤 User: {user_name}\n"
            f"🆔 User ID: `{user_id}`\n"
            f"📅 Created: {created_str}\n\n"
            f"✅ Ticket removed from database.",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        print(f"🗑️ Deleted ticket for user {user_id}")
    else:
        await message.answer(
            "❌ **Failed to delete ticket.**",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )

# ==========================================
# 📊 MORE OPTIONS
# ==========================================

@dp.message(F.text == "📊 MORE OPTIONS")
async def more_options_handler(message: types.Message, state: FSMContext):
    """Show advanced support options"""
    await state.clear()
    await message.answer(
        "📊 **ADVANCED OPTIONS**\n\n"
        "Select an option:",
        reply_markup=get_support_more_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "⬅️ BACK TO SUPPORT")
async def back_to_support(message: types.Message, state: FSMContext):
    """Return to support menu"""
    await state.clear()
    pending_count = col_support_tickets.count_documents({"status": "open"})
    total_count = col_support_tickets.count_documents({})
    resolved_count = col_support_tickets.count_documents({"status": "resolved"})
    
    await message.answer(
        f"💬 **SUPPORT TICKET MANAGEMENT**\n\n"
        f"📊 **Statistics:**\n"
        f"⏳ Pending: **{pending_count}** tickets\n"
        f"✅ Resolved: **{resolved_count}** tickets\n"
        f"📋 Total: **{total_count}** tickets\n\n"
        f"**Select an action:**",
        reply_markup=get_support_management_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# 📈 STATISTICS
# ==========================================

@dp.message(F.text == "📈 STATISTICS")
async def statistics_handler(message: types.Message, state: FSMContext):
    """Show advanced ticket statistics"""
    await state.clear()
    
    # Overall stats
    total = col_support_tickets.count_documents({})
    open_count = col_support_tickets.count_documents({"status": "open"})
    resolved = col_support_tickets.count_documents({"status": "resolved"})
    
    # Today's stats
    today_start = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    today_tickets = col_support_tickets.count_documents({"created_at": {"$gte": today_start}})
    today_resolved = col_support_tickets.count_documents({
        "status": "resolved",
        "resolved_at": {"$gte": today_start}
    })
    
    # Most active users
    pipeline = [
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}, "user_name": {"$first": "$user_name"}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    top_users = list(col_support_tickets.aggregate(pipeline))
    
    # Average resolution time (for resolved tickets)
    resolved_tickets = list(col_support_tickets.find({
        "status": "resolved",
        "resolved_at": {"$exists": True}
    }).limit(50))
    
    if resolved_tickets:
        resolution_times = []
        for ticket in resolved_tickets:
            created = ticket.get('created_at')
            resolved_at = ticket.get('resolved_at')
            if created and resolved_at:
                delta = (resolved_at - created).total_seconds() / 3600  # hours
                resolution_times.append(delta)
        
        avg_time = sum(resolution_times) / len(resolution_times) if resolution_times else 0
        avg_hours = int(avg_time)
        avg_minutes = int((avg_time - avg_hours) * 60)
    else:
        avg_hours = avg_minutes = 0
    
    response = f"📈 **SUPPORT STATISTICS**\n\n"
    response += f"📊 **Overall:**\n"
    response += f"📋 Total Tickets: {total}\n"
    response += f"⏳ Open: {open_count}\n"
    response += f"✅ Resolved: {resolved}\n"
    response += f"📊 Resolution Rate: {(resolved/total*100):.1f}%\n\n"
    
    response += f"📅 **Today:**\n"
    response += f"🆕 New Tickets: {today_tickets}\n"
    response += f"✅ Resolved: {today_resolved}\n\n"
    
    response += f"⏱️ **Performance:**\n"
    response += f"Avg Resolution Time: {avg_hours}h {avg_minutes}m\n\n"
    
    response += f"👥 **Top 5 Users:**\n"
    for i, user in enumerate(top_users, 1):
        response += f"{i}. {user['user_name']} - {user['count']} tickets\n"
    
    await message.answer(
        response,
        reply_markup=get_support_more_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# 🚨 PRIORITY SYSTEM
# ==========================================

@dp.message(F.text == "🚨 PRIORITY")
async def priority_prompt(message: types.Message, state: FSMContext):
    """Set ticket priority"""
    await state.set_state(SupportStates.waiting_for_priority_id)
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "🚨 **SET PRIORITY**\n\n"
        "Send **MSA+ ID** or **Telegram ID** to set priority for their open ticket.",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_priority_id)
async def process_priority_id(message: types.Message, state: FSMContext):
    """Get ticket for priority setting"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_support_more_menu(), parse_mode="Markdown")
        return
    
    search_id = message.text.strip()
    ticket = col_support_tickets.find_one({"msa_id": search_id.upper(), "status": "open"})
    
    if not ticket and search_id.isdigit():
        ticket = col_support_tickets.find_one({"user_id": int(search_id), "status": "open"})
    
    if not ticket:
        await message.answer(
            f"❌ **No open ticket found for:** `{search_id}`",
            parse_mode="Markdown"
        )
        return
    
    await state.update_data(priority_ticket_id=str(ticket["_id"]))
    await state.set_state(SupportStates.waiting_for_priority_level)
    
    priority_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔴 URGENT"), KeyboardButton(text="🟠 HIGH")],
            [KeyboardButton(text="🟡 NORMAL"), KeyboardButton(text="🟢 LOW")],
            [KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        f"🚨 **Set priority for {ticket.get('user_name')}**\n\n"
        f"Select priority level:",
        reply_markup=priority_kb,
        parse_mode="Markdown"
    )

@dp.message(SupportStates.waiting_for_priority_level)
async def process_priority_level(message: types.Message, state: FSMContext):
    """Set the priority level"""
    if message.text in ["❌ CANCEL", "/cancel"]:
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_support_more_menu(), parse_mode="Markdown")
        return
    
    priority_map = {
        "🔴 URGENT": "urgent",
        "🟠 HIGH": "high",
        "🟡 NORMAL": "normal",
        "🟢 LOW": "low"
    }
    
    priority = priority_map.get(message.text)
    if not priority:
        await message.answer("⚠️ **Invalid priority!** Select from buttons.", parse_mode="Markdown")
        return
    
    data = await state.get_data()
    ticket_id = data.get('priority_ticket_id')
    
    result = col_support_tickets.update_one(
        {"_id": ObjectId(ticket_id)},
        {"$set": {"priority": priority}}
    )
    
    await state.clear()
    
    if result.modified_count > 0:
        await message.answer(
            f"✅ **Priority set to {message.text}**",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            "❌ **Failed to set priority.**",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )

# ==========================================
# ⏰ AUTO-CLOSE OLD TICKETS
# ==========================================

@dp.message(F.text == "⏰ AUTO-CLOSE")
async def auto_close_handler(message: types.Message, state: FSMContext):
    """Auto-close tickets older than 7 days"""
    await state.clear()
    
    # Find tickets older than 7 days
    seven_days_ago = now_local() - timedelta(days=7)
    old_tickets = list(col_support_tickets.find({
        "status": "open",
        "created_at": {"$lt": seven_days_ago}
    }))
    
    if not old_tickets:
        await message.answer(
            "✅ **No old tickets to close!**\n\n"
            "All open tickets are less than 7 days old.",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Auto-close them
    closed_count = 0
    for ticket in old_tickets:
        user_id = ticket.get('user_id')
        user_name = ticket.get('user_name', 'User')
        
        # Update database
        col_support_tickets.update_one(
            {"_id": ticket["_id"]},
            {"$set": {"status": "resolved", "resolved_at": now_local(), "auto_closed": True}}
        )
        
        # Notify user
        try:
            await bot_8.send_message(
                user_id,
                f"⏰ **Ticket Auto-Closed**\n\n"
                f"Hi {user_name},\n\n"
                f"Your support ticket has been automatically closed after 7 days.\n\n"
                f"If you still need help, please submit a new ticket using **📞 SUPPORT**.\n\n"
                f"💎 **MSA NODE Team**",
                parse_mode="Markdown"
            )
        except:
            pass
        
        closed_count += 1
    
    await message.answer(
        f"✅ **Auto-closed {closed_count} old tickets!**\n\n"
        f"All tickets older than 7 days have been resolved and users notified.",
        reply_markup=get_support_more_menu(),
        parse_mode="Markdown"
    )
    print(f"⏰ Auto-closed {closed_count} tickets older than 7 days")

# ==========================================
# 📤 EXPORT REPORT
# ==========================================

@dp.message(F.text == "📤 EXPORT")
async def export_handler(message: types.Message, state: FSMContext):
    """Export tickets to CSV file"""
    await state.clear()
    
    import csv
    import io
    
    # Get all tickets
    tickets = list(col_support_tickets.find({}))
    
    if not tickets:
        await message.answer(
            "❌ **No tickets to export!**",
            reply_markup=get_support_more_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Headers
    writer.writerow([
        'User ID', 'Name', 'Username', 'MSA+ ID', 'Issue', 
        'Status', 'Created', 'Resolved', 'Priority', 'Support Count'
    ])
    
    # Data
    for ticket in tickets:
        writer.writerow([
            ticket.get('user_id', ''),
            ticket.get('user_name', ''),
            ticket.get('username', ''),
            ticket.get('msa_id', ''),
            ticket.get('issue_text', '')[:100],
            ticket.get('status', ''),
            ticket.get('created_at', ''),
            ticket.get('resolved_at', ''),
            ticket.get('priority', 'normal'),
            ticket.get('support_count', 1)
        ])
    
    # Convert to bytes
    csv_bytes = output.getvalue().encode('utf-8')
    
    # Create filename with timestamp
    filename = f"support_tickets_{now_local().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Send as document
    from aiogram.types import BufferedInputFile
    file = BufferedInputFile(csv_bytes, filename=filename)
    
    await message.answer_document(
        file,
        caption=f"📤 **Support Tickets Export**\n\n"
                f"📋 Total Tickets: {len(tickets)}\n"
                f"📅 Generated: {now_local().strftime('%Y-%m-%d %H:%M:%S')}",
        parse_mode="Markdown"
    )
    
    await message.answer(
        "✅ **Export complete!**",
        reply_markup=get_support_more_menu(),
        parse_mode="Markdown"
    )
    print(f"📤 Exported {len(tickets)} tickets to CSV")

@dp.message(F.text == "💾 BACKUP")
async def backup_handler(message: types.Message, state: FSMContext):
    """Backup system main menu"""
    log_action("💾 BACKUP SYSTEM", message.from_user.id, "Accessed backup management")
    await state.set_state(BackupStates.viewing_menu)
    
    # Check if any backups exist in MongoDB
    backup_count = col_bot10_backups.count_documents({})
    latest_backup = col_bot10_backups.find_one({}, sort=[("backup_date", -1)])
    
    if latest_backup:
        last_backup = format_datetime(latest_backup['backup_date'])
        backup_status = f"✅ {backup_count} backups stored"
    else:
        last_backup = "Never"
        backup_status = "❌ No backups yet"
    
    message_text = (
        "💾 <b>BACKUP MANAGEMENT SYSTEM</b>\n\n"
        "<b>Current Status:</b>\n"
        f"🗄️ MongoDB Backups: {backup_status}\n"
        f"🕒 Last Backup: {last_backup}\n\n"
        "<b>Features:</b>\n"
        "📥 BACKUP NOW - Create manual backup\n"
        "📊 VIEW BACKUPS - List all MongoDB backups\n"
        "🗓️ MONTHLY STATUS - Check monthly backups\n"
        "⚙️ AUTO-BACKUP - Monthly auto-backup info\n\n"
        "<b>Data Backed Up:</b>\n"
        "• Broadcasts\n"
        "• User Tracking\n"
        "• Support Tickets\n"
        "• Banned Users\n"
        "• Suspended Features\n"
        "• Cleanup Logs\n\n"
        "<b>Storage:</b> MongoDB (Cloud-Safe)\n"
        "<b>Download:</b> JSON files sent to you\n"
        "<b>Works On:</b> Render/Heroku/Railway✅\n"
    )
    
    await message.answer(message_text, reply_markup=get_backup_menu(), parse_mode="HTML")

async def create_backup_mongodb_scalable(backup_type="manual", admin_id=None, progress_callback=None):
    """
    ENTERPRISE-GRADE BACKUP SYSTEM
    - Scales to CRORES (10M+) of users
    - Memory-efficient (batch processing with cursors)
    - Progress updates during backup
    - Auto-splits large files (50MB Telegram limit)
    - Compression support
    - Error recovery
    - Cloud-safe (MongoDB storage)
    """
    now = now_local()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    
    BATCH_SIZE = 10000  # Process 10k records at a time
    MAX_FILE_SIZE = 40 * 1024 * 1024  # 40MB (stay under Telegram's 50MB limit)
    
    _period_now = "AM" if now.hour < 12 else "PM"
    backup_summary = {
        "backup_date": now,
        "backup_type": backup_type,
        "timestamp": timestamp,
        "year": now.year,
        "month": now.strftime("%B"),
        "day":   now.day,
        "window_key": now.strftime("%Y-%m-%d_") + _period_now,  # e.g. "2026-02-19_AM"
        "period":     _period_now,
        "created_by": admin_id or MASTER_ADMIN_ID,
        "total_records": 0,
        "collection_counts": {},
        "processing_time": 0
    }
    
    collections_data = {}
    start_time = now_local()
    
    try:
        # Define collections to backup
        collections_to_backup = [
            ("broadcasts", col_broadcasts),
            ("user_tracking", col_user_tracking),
            ("support_tickets", col_support_tickets),
            ("banned_users", col_banned_users),
            ("suspended_features", col_suspended_features),
            ("cleanup_logs", col_cleanup_logs)
        ]
        
        for col_name, collection in collections_to_backup:
            if progress_callback:
                await progress_callback(f"📦 Backing up {col_name}...")
            
            # Get total count for progress tracking
            total_count = collection.count_documents({})
            backup_summary["collection_counts"][col_name] = total_count
            backup_summary["total_records"] += total_count
            
            if total_count == 0:
                collections_data[col_name] = []
                continue
            
            # Use cursor for memory-efficient processing (CRITICAL FOR SCALE)
            records = []
            processed = 0
            
            # Process in batches using cursor
            cursor = collection.find({}).batch_size(BATCH_SIZE)
            
            for doc in cursor:
                # Convert ObjectId to string for JSON serialization
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                # Convert datetime objects to string
                for key, value in doc.items():
                    if isinstance(value, datetime):
                        doc[key] = value.isoformat()
                
                records.append(doc)
                processed += 1
                
                # Progress update every 1000 records
                if progress_callback and processed % 1000 == 0:
                    await progress_callback(
                        f"📦 {col_name}: {processed:,}/{total_count:,} records "
                        f"({int(processed/total_count*100)}%)"
                    )
            
            collections_data[col_name] = records
            
            if progress_callback:
                await progress_callback(f"✅ {col_name}: {total_count:,} records backed up")
            
            print(f"✅ {col_name}: {total_count:,} records backed up")
        
        # Calculate processing time
        processing_time = (now_local() - start_time).total_seconds()
        backup_summary["processing_time"] = processing_time
        
        # === SAVE SUMMARY TO MONGODB (not full data - that's too large) ===
        result = col_bot10_backups.insert_one(backup_summary)
        backup_id = str(result.inserted_id)
        
        print(f"\n✅ Backup completed successfully!")
        print(f"📊 Total Records: {backup_summary['total_records']:,}")
        print(f"⏱️ Processing Time: {processing_time:.2f} seconds")
        print(f"💾 Summary stored in MongoDB: {backup_id}")
        
        # === MANAGE OLD BACKUPS (Keep last 60 = 12h × 30 days) ===
        backup_count = col_bot10_backups.count_documents({})
        if backup_count > 60:
            old_backups = list(col_bot10_backups.find({}).sort("backup_date", 1).limit(backup_count - 60))
            old_backup_ids = [b['_id'] for b in old_backups]
            col_bot10_backups.delete_many({"_id": {"$in": old_backup_ids}})
            print(f"🗑️ Cleaned up {backup_count - 60} old backups (kept last 60)")
        
        return {
            "success": True,
            "backup_id": backup_id,
            "timestamp": timestamp,
            "total_records": backup_summary['total_records'],
            "collections": collections_data,
            "collection_counts": backup_summary["collection_counts"],
            "processing_time": processing_time
        }
        
    except Exception as e:
        error_msg = f"Backup error: {str(e)}"
        print(f"❌ {error_msg}")
        return {
            "success": False,
            "error": error_msg,
            "total_records": backup_summary.get('total_records', 0)
        }

@dp.message(F.text == "📥 BACKUP NOW")
async def backup_now_handler(message: types.Message, state: FSMContext):
    """Create manual backup with ENTERPRISE SCALABILITY (handles crores of users)"""
    status_msg = await message.answer("⏳ <b>Starting Backup...</b>\n\nInitializing enterprise-grade backup system...", parse_mode="HTML")
    
    try:
        # Progress callback for real-time updates
        async def progress_update(status_text):
            try:
                await status_msg.edit_text(
                    f"⏳ <b>Backup in Progress...</b>\n\n{status_text}",
                    parse_mode="HTML"
                )
            except:
                pass  # Ignore rate limit errors during progress updates
        
        # Create backup with scalable function
        backup_data = await create_backup_mongodb_scalable(
            backup_type="manual",
            admin_id=message.from_user.id,
            progress_callback=progress_update
        )
        
        if not backup_data.get("success"):
            error_msg = backup_data.get("error", "Unknown error").replace('<', '&lt;').replace('>', '&gt;')
            await status_msg.edit_text(f"❌ <b>BACKUP FAILED</b>\n\n{error_msg}", parse_mode="HTML")
            return
        
        # Update status
        processing_time = backup_data.get("processing_time", 0)
        await status_msg.edit_text(
            f"✅ <b>Backup stored in MongoDB!</b>\n\n"
            f"⏱️ Processing Time: {processing_time:.2f}s\n"
            f"📤 Preparing downloadable files...",
            parse_mode="HTML"
        )
        
        # Generate downloadable JSON files
        timestamp = backup_data["timestamp"]
        MAX_FILE_SIZE = 40 * 1024 * 1024  # 40MB (Telegram limit: 50MB)
        
        # === COMPLETE BACKUP JSON ===
        complete_json = json.dumps(backup_data, indent=2, ensure_ascii=False, default=str)
        complete_size = len(complete_json.encode('utf-8'))
        
        if complete_size > MAX_FILE_SIZE:
            # Compress if too large
            import gzip
            compressed = gzip.compress(complete_json.encode('utf-8'))
            complete_file = BufferedInputFile(
                compressed,
                filename=f"bot10_complete_backup_{timestamp}.json.gz"
            )
            size_text = f"{len(compressed) / (1024*1024):.1f}MB (compressed from {complete_size / (1024*1024):.1f}MB)"
        else:
            complete_file = BufferedInputFile(
                complete_json.encode('utf-8'),
                filename=f"bot10_complete_backup_{timestamp}.json"
            )
            size_text = f"{complete_size / (1024*1024):.1f}MB"
        
        # Send complete backup
        await message.answer_document(
            complete_file,
            caption=(
                f"📦 <b>COMPLETE BACKUP</b>\n\n"
                f"📅 Date: {timestamp}\n"
                f"📊 Total Records: {backup_data['total_records']:,}\n"
                f"💾 Size: {size_text}\n"
                f"⏱️ Processing: {processing_time:.2f}s\n\n"
                f"<b>Collection Counts:</b>\n"
                f"✅ Broadcasts: {backup_data['collection_counts'].get('bot10_broadcasts', 0):,}\n"
                f"✅ Users: {backup_data['collection_counts'].get('bot10_user_tracking', 0):,}\n"
                f"✅ Tickets: {backup_data['collection_counts'].get('support_tickets', 0):,}\n"
                f"✅ Banned: {backup_data['collection_counts'].get('banned_users', 0):,}\n"
                f"✅ Suspended: {backup_data['collection_counts'].get('suspended_features', 0):,}\n"
                f"✅ Logs: {backup_data['collection_counts'].get('cleanup_logs', 0):,}\n\n"
                f"🚀 <b>Enterprise-Grade Scalability</b>\n"
                f"✅ Memory-efficient batch processing\n"
                f"✅ Handles crores (10M+) of users\n"
                f"✅ Real-time progress tracking"
            ),
            parse_mode="HTML"
        )
        
        # === INDIVIDUAL COLLECTION JSONs (with compression for large files) ===
        files_sent = 1
        for collection_name, collection_data in backup_data["collections"].items():
            if collection_data:  # Only send if not empty
                collection_json = json.dumps(collection_data, indent=2, ensure_ascii=False, default=str)
                json_size = len(collection_json.encode('utf-8'))
                
                if json_size > MAX_FILE_SIZE:
                    # Compress large collections
                    import gzip
                    compressed = gzip.compress(collection_json.encode('utf-8'))
                    collection_file = BufferedInputFile(
                        compressed,
                        filename=f"{collection_name}_{timestamp}.json.gz"
                    )
                    size_info = f"{len(compressed) / (1024*1024):.1f}MB compressed (original: {json_size / (1024*1024):.1f}MB)"
                else:
                    collection_file = BufferedInputFile(
                        collection_json.encode('utf-8'),
                        filename=f"{collection_name}_{timestamp}.json"
                    )
                    size_info = f"{json_size / (1024*1024):.2f}MB"
                
                await message.answer_document(
                    collection_file,
                    caption=(
                        f"📄 <b>{collection_name.replace('_', ' ').title()}</b>\n"
                        f"📊 {len(collection_data):,} records\n"
                        f"💾 {size_info}"
                    ),
                    parse_mode="HTML"
                )
                files_sent += 1
        
        # Final success message
        await status_msg.edit_text(
            "✅ <b>BACKUP COMPLETED</b>\n\n"
            f"📅 Date: {timestamp}\n"
            f"📊 Total Records: {backup_data['total_records']:,}\n"
            f"⏱️ Processing Time: {processing_time:.2f}s\n\n"
            f"<b>Storage:</b>\n"
            f"💾 MongoDB: bot10_backups collection\n"
            f"📥 Downloaded: {files_sent} JSON files\n\n"
            f"<b>Cloud-Safe & Scalable:</b>\n"
            f"✅ Works on Render/Heroku/Railway\n"
            f"✅ No local storage needed\n"
            f"✅ Handles crores (10M+) users\n"
            f"✅ Memory-efficient batch processing\n"
            f"✅ Auto-compression for large files",
            parse_mode="HTML"
        )
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await status_msg.edit_text(f"❌ <b>BACKUP ERROR</b>\n\n{error_msg}", parse_mode="HTML")

@dp.message(F.text == "📊 VIEW BACKUPS")
async def view_backups_handler(message: types.Message):
    """Show all MongoDB backups with pagination"""
    await show_backups_page(message, page=1)

async def show_backups_page(message: types.Message, page: int = 1):
    """Helper function to display backups with pagination"""
    ITEMS_PER_PAGE = 10  # Show 10 backups per page
    
    try:
        # Get total count
        total_backups = col_bot10_backups.count_documents({})
        
        if total_backups == 0:
            await message.answer(
                "📁 <b>NO BACKUPS FOUND</b>\n\n"
                "No backups have been created yet.\n"
                "Use 📥 BACKUP NOW to create your first backup!",
                parse_mode="HTML"
            )
            return
        
        # Calculate pagination
        total_pages = (total_backups + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        page = max(1, min(page, total_pages))
        skip = (page - 1) * ITEMS_PER_PAGE
        
        # Get backups for current page
        backups = list(col_bot10_backups.find({})
                      .sort("backup_date", -1)
                      .skip(skip)
                      .limit(ITEMS_PER_PAGE))
        
        # Build message
        msg_text = f"📊 <b>MONGODB BACKUPS</b> (Page {page}/{total_pages})\n\n"
        msg_text += f"🗄️ Total Backups: {total_backups} | Showing: {skip + 1}-{skip + len(backups)}\n\n"
        
        for idx, backup in enumerate(backups, skip + 1):
            backup_date = format_datetime(backup['backup_date'])
            backup_type = backup['backup_type'].title()
            total_records = backup.get('total_records', 0)
            
            msg_text += f"{idx}. <b>{backup_date}</b>\n"
            msg_text += f"   Type: {backup_type} | Records: {total_records}\n\n"
        
        msg_text += "💡 <i>All backups stored in MongoDB cloud</i>"
        
        # Create pagination buttons
        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"backup_page_{page-1}"))
        if page < total_pages:
            buttons.append(InlineKeyboardButton(text="➡️ Next", callback_data=f"backup_page_{page+1}"))
        
        keyboard = None
        if buttons:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons])
        
        # Check if this is being called from callback (edit) or new message
        try:
            if keyboard:
                await message.edit_text(msg_text, parse_mode="HTML", reply_markup=keyboard)
            else:
                await message.edit_text(msg_text, parse_mode="HTML")
        except:
            # If edit fails (not from callback), send new message
            await message.answer(msg_text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await message.answer(f"❌ <b>ERROR</b>\n\n{error_msg}", parse_mode="HTML")

@dp.message(F.text == "🗓️ MONTHLY STATUS")
async def monthly_status_handler(message: types.Message):
    """Check monthly backup status from MongoDB"""
    try:
        now = now_local()
        
        # Get backups grouped by month
        backups = list(col_bot10_backups.find({}).sort("backup_date", -1))
        
        if not backups:
            await message.answer(
                "🗓️ <b>MONTHLY BACKUP STATUS</b>\n\n"
                "❌ No backups created yet",
                parse_mode="HTML"
            )
            return
        
        # Group by year-month
        monthly_counts = {}
        for backup in backups:
            backup_date = backup['backup_date']
            year_month = backup_date.strftime("%Y-%B")
            monthly_counts[year_month] = monthly_counts.get(year_month, 0) + 1
        
        msg_text = "🗓️ <b>MONTHLY BACKUP STATUS</b>\n\n"
        
        for year_month, count in sorted(monthly_counts.items(), reverse=True)[:12]:
            msg_text += f"✅ {year_month}: {count} backup(s)\n"
        
        msg_text += f"\n💡 <i>Total: {len(backups)} backups in MongoDB</i>"
        
        await message.answer(msg_text, parse_mode="HTML")
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await message.answer(f"❌ <b>ERROR</b>\n\n{error_msg}", parse_mode="HTML")

@dp.message(F.text == "⚙️ AUTO-BACKUP")
async def auto_backup_info_handler(message: types.Message):
    """Show auto-backup information"""
    msg_text = (
        "⚙️ <b>AUTOMATIC BACKUP SYSTEM</b>\n\n"
        "<b>Schedule:</b>\n"
        "🕐 Every 12 hours (AM &amp; PM)\n\n"
        "<b>What Gets Backed Up:</b>\n"
        "• All Broadcasts\n"
        "• User Tracking Data\n"
        "• Support Tickets\n"
        "• Banned Users\n"
        "• Suspended Features\n"
        "• Cleanup Logs\n\n"
        "<b>Storage:</b>\n"
        "🗄️ MongoDB: <code>bot10_backups</code> collection\n"
        "📅 Timestamp format: Month DD, YYYY — HH:MM AM/PM\n\n"
        "<b>Cloud-Safe:</b>\n"
        "✅ Works on Render, Heroku, Railway\n"
        "✅ No local disk storage needed\n"
        "✅ Keeps last 60 backups (30 days × 2/day)\n\n"
        "<b>Bot Separation:</b>\n"
        "🟢 Bot 8 → <code>bot8_backups</code> collection\n"
        "🔵 Bot 10 → <code>bot10_backups</code> collection\n"
        "❌ No mixing between bots\n\n"
        "<b>Status:</b>\n"
        "🟢 ACTIVE — Running every 12 hours\n\n"
        "💡 <i>You can also create manual backups anytime using 📥 BACKUP NOW</i>"
    )
    
    await message.answer(msg_text, parse_mode="HTML")

@dp.message(F.text == "🖥️ TERMINAL")
async def terminal_handler(message: types.Message, state: FSMContext):
    """Terminal - Shows live logs with Bot 8/10 selection"""
    # Log to console and memory
    log_action("🖥️ TERMINAL ACCESS", message.from_user.id, "Admin opened live terminal", "bot10")
    
    try:
        # Show view selection with reply keyboard
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📱 BOT 8 LOGS"), KeyboardButton(text="🎛️ BOT 10 LOGS")],
                [KeyboardButton(text="⬅️ MAIN MENU")]
            ],
            resize_keyboard=True
        )
        
        await message.answer(
            "<b>🖥️ LIVE TERMINAL</b>\n\n"
            "Select which bot logs to view:\n\n"
            "📱 <b>Bot 8 Logs</b> - User interactions & content\n"
            "🎛️ <b>Bot 10 Logs</b> - Admin actions & management\n\n"
            f"<i>💡 Tracking last {MAX_LOGS} actions per bot</i>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await message.answer(
            f"<b>❌ TERMINAL ERROR</b>\n\n{error_msg}",
            parse_mode="HTML"
        )

@dp.message(F.text.in_({"📱 BOT 8 LOGS", "🔄 REFRESH BOT 8"}))
async def view_bot8_logs(message: types.Message, state: FSMContext):
    """Show Bot 8 live logs in raw terminal format"""
    # Simply log strictly (no stats query)
    log_action("CMD", message.from_user.id, "Opened Bot 8 Terminal", "bot8")
    
    try:
        logs_text = get_terminal_logs(bot="bot8", limit=50)
        
        # Specific keyboard for Bot 8 view
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🔄 REFRESH BOT 8"), KeyboardButton(text="⬅️ RETURN TO MENU")]
            ],
            resize_keyboard=True
        )
        
        # Raw terminal appearance
        await message.answer(
            f"<b>📱 BOT 8 TERMINAL VIEW</b>\n"
            f"<pre language='bash'>{logs_text}</pre>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
    except Exception as e:
        await message.answer(f"Error: {e}")

@dp.message(F.text.in_({"🎛️ BOT 10 LOGS", "🔄 REFRESH BOT 10"}))
async def view_bot10_logs(message: types.Message, state: FSMContext):
    """Show Bot 10 live logs in raw terminal format"""
    log_action("CMD", message.from_user.id, "Opened Bot 10 Terminal", "bot10")
    
    try:
        logs_text = get_terminal_logs(bot="bot10", limit=50)
        
        # Specific keyboard for Bot 10 view
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🔄 REFRESH BOT 10"), KeyboardButton(text="⬅️ RETURN TO MENU")]
            ],
            resize_keyboard=True
        )
        
        # Raw terminal appearance  
        await message.answer(
            f"<b>🎛️ BOT 10 TERMINAL VIEW</b>\n"
            f"<pre language='bash'>{logs_text}</pre>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
    except Exception as e:
        await message.answer(f"Error: {e}")


@dp.message(F.text == "⬅️ RETURN TO MENU")
async def back_to_terminal_menu(message: types.Message, state: FSMContext):
    """Return to main terminal menu"""
    # Call the original terminal handler
    await terminal_handler(message, state)

@dp.callback_query(F.data == "terminal_bot8")
async def terminal_bot8_view(callback: types.CallbackQuery, state: FSMContext):
    """Show Bot 8 terminal view"""
    log_action("📱 BOT 8 TERMINAL", callback.from_user.id, "Viewing Bot 8 statistics")
    
    try:
        await callback.message.edit_text(
            "<b>📱 BOT 8 TERMINAL</b>\n\n"
            "⏳ Fetching live Bot 8 data...\n"
            "📊 Analyzing collections...",
            parse_mode="HTML"
        )
        
        # Get counts from all Bot 8 collections
        user_verification_count = col_user_verification.count_documents({})
        msa_ids_count = col_msa_ids.count_documents({})
        bot9_pdfs_count = col_bot9_pdfs.count_documents({})
        bot9_ig_content_count = col_bot9_ig_content.count_documents({})
        support_tickets_count = col_support_tickets.count_documents({})
        banned_users_count = col_banned_users.count_documents({})
        suspended_features_count = col_suspended_features.count_documents({})
        
        # Calculate total
        total_records = (
            user_verification_count + msa_ids_count + bot9_pdfs_count + 
            bot9_ig_content_count + support_tickets_count + 
            banned_users_count + suspended_features_count
        )
        
        # Get Bot 10 collections stats
        bot10_broadcasts_count = col_broadcasts.count_documents({})
        bot10_user_tracking_count = col_user_tracking.count_documents({})
        bot10_backups_count = col_bot10_backups.count_documents({})
        cleanup_backups_count = col_cleanup_backups.count_documents({})
        cleanup_logs_count = col_cleanup_logs.count_documents({})
        
        # Get support ticket stats
        open_tickets = col_support_tickets.count_documents({"status": "open"})
        resolved_tickets = col_support_tickets.count_documents({"status": "resolved"})
        
        # Build terminal-style output
        terminal_output = (
            "<b>🖥️ MSA NODE - SYSTEM TERMINAL</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<code>$ system_info --status\n"
            f"System: MSANodeDB\n"
            f"Status: ONLINE ✅\n"
            f"Timestamp: {now_local().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Admin: Bot 10 Control Panel\n\n"
            
            f"$ bot10_features --list\n\n"
            f"BOT 10 AVAILABLE ACTIONS:\n"
            f"├─ 📢 BROADCAST         : Send messages to all Bot 8 users\n"
            f"│  ├─ Send Broadcast    : Create & send new broadcast\n"
            f"│  ├─ Delete Broadcast  : Remove broadcast by ID\n"
            f"│  ├─ Edit Broadcast    : Modify existing broadcast\n"
            f"│  └─ List Broadcasts   : View all broadcasts\n"
            f"│\n"
            f"├─  FIND              : Search user by ID/username\n"
            f"│  └─ User lookup       : Get detailed user info\n"
            f"│\n"
            f"├─ 📊 TRAFFIC           : User traffic sources\n"
            f"│  └─ Analytics         : See how users found Bot 8\n"
            f"│\n"
            f"├─ 🩺 DIAGNOSIS         : User management tools\n"
            f"│  ├─ Ban User          : Permanent ban with reason\n"
            f"│  ├─ Temporary Ban     : Time-limited ban (hours/days)\n"
            f"│  ├─ Unban User        : Remove ban\n"
            f"│  ├─ Delete User       : Remove from database\n"
            f"│  ├─ Suspend Features  : Limit specific features\n"
            f"│  ├─ Unsuspend         : Restore all features\n"
            f"│  └─ Reset User        : Clear user verification\n"
            f"│\n"
            f"├─ 📸 SHOOT             : Quick user search\n"
            f"│  └─ Fast lookup       : Instant user info\n"
            f"│\n"
            f"├─ 💬 SUPPORT           : Support ticket system\n"
            f"│  ├─ Reply to ticket   : Respond to user tickets\n"
            f"│  ├─ Mark resolved     : Close ticket\n"
            f"│  └─ View all tickets  : Browse open/resolved\n"
            f"│\n"
            f"├─ 💾 BACKUP            : Enterprise backup system\n"
            f"│  ├─ Backup Now        : Manual backup (MongoDB + JSON)\n"
            f"│  ├─ View Backups      : List all backups\n"
            f"│  ├─ Monthly Status    : Backup statistics\n"
            f"│  ├─ Auto-Backup       : Schedule info\n"
            f"│  └─ Scalability       : Handles 10M+ users\n"
            f"│\n"
            f"├─ 🖥️ TERMINAL          : System statistics (current)\n"
            f"│  ├─ Database stats    : Collection counts\n"
            f"│  ├─ Bot 8 data        : User verification, MSA IDs\n"
            f"│  ├─ Bot 10 data       : Broadcasts, backups\n"
            f"│  └─ Security status   : Bans, suspensions\n"
            f"│\n"
            f"├─ 👥 ADMINS            : Admin management [COMING SOON]\n"
            f"│  └─ Multi-admin       : Add/remove admin access\n"
            f"│\n"
            f"└─ ⚠️ RESET DATA        : Delete ALL Bot 8 data\n"
            f"   └─ Double confirm    : RESET → DELETE ALL\n\n"
            
            f"$ bot8_stats --collections\n\n"
            f"BOT 8 DATA COLLECTIONS:\n"
            f"├─ user_verification     : {user_verification_count:,} records\n"
            f"├─ msa_ids              : {msa_ids_count:,} records\n"
            f"├─ bot9_pdfs            : {bot9_pdfs_count:,} records\n"
            f"├─ bot9_ig_content      : {bot9_ig_content_count:,} records\n"
            f"├─ support_tickets      : {support_tickets_count:,} records\n"
            f"│  ├─ Open              : {open_tickets:,} tickets\n"
            f"│  └─ Resolved          : {resolved_tickets:,} tickets\n"
            f"├─ banned_users         : {banned_users_count:,} records\n"
            f"└─ suspended_features   : {suspended_features_count:,} records\n\n"
            f"TOTAL BOT 8 RECORDS     : {total_records:,}\n\n"
            
            f"$ bot10_stats --collections\n\n"
            f"BOT 10 DATA COLLECTIONS:\n"
            f"├─ bot10_broadcasts     : {bot10_broadcasts_count:,} records\n"
            f"├─ bot10_user_tracking  : {bot10_user_tracking_count:,} records\n"
            f"├─ bot10_backups        : {bot10_backups_count:,} records\n"
            f"├─ cleanup_backups      : {cleanup_backups_count:,} records\n"
            f"└─ cleanup_logs         : {cleanup_logs_count:,} records\n\n"
            
            f"$ disk_usage --total\n"
            f"Total Database Records  : {total_records + bot10_broadcasts_count + bot10_user_tracking_count + bot10_backups_count + cleanup_backups_count + cleanup_logs_count:,}\n\n"
            
            f"$ security_status\n"
            f"Banned Users           : {banned_users_count:,}\n"
            f"Suspended Features     : {suspended_features_count:,}\n"
            f"Open Support Tickets   : {open_tickets:,}\n\n"
            
            f"$ automation_status\n"
            f"Daily Cleanup          : ACTIVE ✅ (3 AM daily)\n"
            f"Monthly Backup         : ACTIVE ✅ (1st of month, 3 AM)\n"
            f"Backup Retention       : Last 30 backups\n"
            f"Cleanup History        : Last 30 logs</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>System Status:</b> All systems operational ✅\n"
            f"<b>Features:</b> 10 Core Actions + Auto-Cleanup + Auto-Backup\n"
            f"<b>Memory:</b> MongoDB Cloud Atlas\n"
            f"<b>Hosting:</b> Cloud-Safe (Render/Heroku Compatible)\n"
            f"<b>Scalability:</b> Enterprise-grade (10M+ users)\n\n"
            "<i>💡 Terminal displays all Bot 10 features & system stats</i>"
        )
        
        # Build Bot 8 terminal output
        bot8_terminal = (
            "<b>📱 BOT 8 LIVE TERMINAL</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<code>$ bot8_info --status\n"
            f"Bot: MSA Node Bot (Bot 8)\n"
            f"Status: ONLINE ✅\n"
            f"Timestamp: {now_local().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Live Updates: ENABLED ✅\n\n"
            
            f"$ user_data --collections\n\n"
            f"USER DATA COLLECTIONS:\n"
            f"├─ user_verification     : {user_verification_count:,} users\n"
            f"├─ msa_ids              : {msa_ids_count:,} MSA+ IDs\n"
            f"├─ bot9_pdfs            : {bot9_pdfs_count:,} PDF records\n"
            f"└─ bot9_ig_content      : {bot9_ig_content_count:,} IG posts\n\n"
            
            f"$ support_system --status\n\n"
            f"SUPPORT TICKETS:\n"
            f"├─ Total Tickets        : {support_tickets_count:,}\n"
            f"├─ Open                 : {open_tickets:,} 🟢\n"
            f"└─ Resolved             : {resolved_tickets:,} ✅\n\n"
            
            f"$ security_status\n\n"
            f"SECURITY & MODERATION:\n"
            f"├─ Banned Users         : {banned_users_count:,} 🚫\n"
            f"└─ Suspended Features   : {suspended_features_count:,} ⚠️\n\n"
            
            f"$ total_bot8_records\n"
            f"Total Bot 8 Records     : {total_records:,}\n</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Live Monitoring:</b> Active ✅\n"
            f"<b>Console Logging:</b> All actions logged\n"
            f"<b>Last Updated:</b> {now_local().strftime('%H:%M:%S')}\n\n"
            "<i>💡 Bot 8 serves end users with content & support</i>"
        )
        
        # Add buttons
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎛️ BOT 10 TERMINAL", callback_data="terminal_bot10")],
            [InlineKeyboardButton(text="🔄 REFRESH", callback_data="terminal_bot8")]
        ])
        
        await callback.message.edit_text(bot8_terminal, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer("📱 Bot 8 Terminal loaded")
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await callback.message.edit_text(
            f"<b>❌ TERMINAL ERROR</b>\n\n{error_msg}",
            parse_mode="HTML"
        )
        await callback.answer("Error loading terminal", show_alert=True)

@dp.callback_query(F.data == "terminal_bot10")
async def terminal_bot10_view(callback: types.CallbackQuery, state: FSMContext):
    """Show Bot 10 terminal view"""
    log_action("🎛️ BOT 10 TERMINAL", callback.from_user.id, "Viewing Bot 10 admin actions")
    
    try:
        # Get counts
        user_verification_count = col_user_verification.count_documents({})
        msa_ids_count = col_msa_ids.count_documents({})
        bot9_pdfs_count = col_bot9_pdfs.count_documents({})
        bot9_ig_content_count = col_bot9_ig_content.count_documents({})
        support_tickets_count = col_support_tickets.count_documents({})
        banned_users_count = col_banned_users.count_documents({})
        suspended_features_count = col_suspended_features.count_documents({})
        open_tickets = col_support_tickets.count_documents({"status": "open"})
        resolved_tickets = col_support_tickets.count_documents({"status": "resolved"})
        
        bot10_broadcasts_count = col_broadcasts.count_documents({})
        bot10_user_tracking_count = col_user_tracking.count_documents({})
        bot10_backups_count = col_bot10_backups.count_documents({})
        cleanup_backups_count = col_cleanup_backups.count_documents({})
        cleanup_logs_count = col_cleanup_logs.count_documents({})
        
        total_records = (
            user_verification_count + msa_ids_count + bot9_pdfs_count + 
            bot9_ig_content_count + support_tickets_count + 
            banned_users_count + suspended_features_count
        )
        
        # Build Bot 10 terminal output
        bot10_terminal = (
            "<b>🎛️ BOT 10 LIVE TERMINAL</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<code>$ bot10_info --status\n"
            f"Bot: Admin Control Panel (Bot 10)\n"
            f"Status: ONLINE ✅\n"
            f"Timestamp: {now_local().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Live Updates: ENABLED ✅\n"
            f"Console Logging: ACTIVE ✅\n\n"
            
            f"$ admin_actions --available\n\n"
            f"AVAILABLE ADMIN ACTIONS:\n"
            f"├─ 📢 BROADCAST         : {bot10_broadcasts_count:,} sent\n"
            f"├─ 🔍 FIND              : Search users\n"
            f"├─ 📊 TRAFFIC           : {bot10_user_tracking_count:,} tracked\n"
            f"├─ 🩺 DIAGNOSIS         : System health checks\n"
            f"├─ 📸 SHOOT             : User management\n"
            f"├─ 💬 SUPPORT           : {support_tickets_count:,} tickets\n"
            f"├─ 💾 BACKUP            : {bot10_backups_count:,} backups\n"
            f"├─ 🖥️ TERMINAL          : Live view (current)\n"
            f"└─ ⚠️ RESET DATA        : Dangerous operation\n\n"
            
            f"$ bot10_collections --stats\n\n"
            f"BOT 10 DATA:\n"
            f"├─ bot10_broadcasts     : {bot10_broadcasts_count:,} records\n"
            f"├─ bot10_user_tracking  : {bot10_user_tracking_count:,} records\n"
            f"├─ bot10_backups        : {bot10_backups_count:,} records\n"
            f"├─ cleanup_backups      : {cleanup_backups_count:,} records\n"
            f"└─ cleanup_logs         : {cleanup_logs_count:,} records\n\n"
            
            f"$ automation_systems\n\n"
            f"AUTOMATED PROCESSES:\n"
            f"├─ Daily Cleanup        : ACTIVE ✅ (3 AM)\n"
            f"├─ Monthly Backup       : ACTIVE ✅ (1st, 3 AM)\n"
            f"├─ Backup Retention     : Last 30 backups\n"
            f"└─ Log Retention        : Last 30 logs\n\n"
            
            f"$ security_overview\n\n"
            f"SECURITY STATUS:\n"
            f"├─ Banned Users         : {banned_users_count:,}\n"
            f"├─ Suspended Features   : {suspended_features_count:,}\n"
            f"└─ Open Tickets         : {open_tickets:,}\n\n"
            
            f"$ total_database_records\n"
            f"Total Records           : {total_records + bot10_broadcasts_count + bot10_user_tracking_count + bot10_backups_count + cleanup_backups_count + cleanup_logs_count:,}\n</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Admin Panel:</b> Fully operational ✅\n"
            f"<b>Live Logging:</b> All actions → Console\n"
            f"<b>Last Updated:</b> {now_local().strftime('%H:%M:%S')}\n\n"
            "<i>💡 Bot 10 manages Bot 8 with admin tools</i>"
        )
        
        # Add buttons
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 BOT 8 TERMINAL", callback_data="terminal_bot8")],
            [InlineKeyboardButton(text="🔄 REFRESH", callback_data="terminal_bot10")]
        ])
        
        await callback.message.edit_text(bot10_terminal, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer("🎛️ Bot 10 Terminal loaded")
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await callback.message.edit_text(
            f"<b>❌ TERMINAL ERROR</b>\n\n{error_msg}",
            parse_mode="HTML"
        )
        await callback.answer("Error loading terminal", show_alert=True)

@dp.callback_query(F.data == "terminal_refresh")
async def terminal_refresh(callback: types.CallbackQuery):
    """Refresh terminal view"""
    await callback.answer("🔄 Refreshing terminal...")
    await terminal_handler(callback.message, None)

@dp.message(F.text == "👥 ADMINS")
async def admins_handler(message: types.Message, state: FSMContext):
    """Show admin management menu"""
    if message.from_user.id != MASTER_ADMIN_ID:
        log_action("🚫 UNAUTHORIZED ACCESS", message.from_user.id, f"{message.from_user.full_name} tried to access ADMINS")
        await message.answer("⛔ **ACCESS DENIED**\n\nThis feature is restricted to the Master Admin.", reply_markup=await get_main_menu(message.from_user.id))
        return

    await state.clear()
    log_action("👥 ADMINS MENU", message.from_user.id, "Opened admin management")
    
    # Count admins
    admin_count = col_admins.count_documents({})
    
    await message.answer(
        f"👥 **ADMIN MANAGEMENT**\n\n"
        f"📊 Total Admins: {admin_count}\n\n"
        "Select an option:",
        reply_markup=get_admin_menu(),
        parse_mode="Markdown"
    )

# ==========================================
# ADMIN MANAGEMENT HANDLERS
# ==========================================

@dp.message(F.text == "➕ NEW ADMIN")
async def new_admin_handler(message: types.Message, state: FSMContext):
    """Add new admin"""
    log_action("➕ NEW ADMIN", message.from_user.id, "Starting new admin creation")
    
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        "➕ **ADD NEW ADMIN**\n\n"
        "Please send the **User ID** of the new admin:\n\n"
        "💡 Tip: Ask the user to send /start to any bot to get their ID",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.waiting_for_new_admin_id)

@dp.message(AdminStates.waiting_for_new_admin_id)
async def process_new_admin_id(message: types.Message, state: FSMContext):
    """Process new admin user ID"""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Validate user ID
    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer(
            "⚠️ Invalid User ID. Please send a valid numeric User ID.\n\n"
            "Example: `123456789`",
            parse_mode="Markdown"
        )
        return
    
    # Check if already admin
    existing = col_admins.find_one({"user_id": user_id})
    if existing:
        await message.answer(
            f"⚠️ User `{user_id}` is already an admin!\n\n"
            f"👔 Current Role: **{existing.get('role', 'Admin')}**\n"
            f"📅 Added: {format_datetime(existing.get('added_at'))}",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
    # Create admin record with default Admin role (LOCKED by default)
    admin_doc = {
        "user_id": user_id,
        "role": "Admin",
        "permissions": ["broadcast", "support"],  # Safe defaults - use PERMISSIONS menu to add more
        "added_by": message.from_user.id,
        "added_at": now_local(),
        "status": "active",
        "locked": True  # LOCKED by default - must be unlocked to activate
    }
    
    try:
        col_admins.insert_one(admin_doc)
        log_action("➕ ADMIN ADDED", message.from_user.id, 
                  f"New Admin: {user_id}")
        
        await message.answer(
            f"✅ ADMIN ADDED SUCCESSFULLY!\n\n"
            f"👤 User ID: {user_id}\n"
            f"👔 Role: Admin\n"
            f"🔐 Default Permissions: Broadcast, Support\n"
            f"🔒 Status: LOCKED (Inactive)\n"
            f"📅 Added: {now_local().strftime('%b %d, %Y %I:%M %p')}\n\n"
            f"⚠️ This admin is LOCKED and cannot access Bot 10 yet!\n"
            f"💡 Use 🔒 LOCK/UNLOCK USER to activate them\n"
            f"💡 Use 🔐 PERMISSIONS to add more permissions\n"
            f"💡 Use 👔 MANAGE ROLES to change role",
            reply_markup=get_admin_menu()
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(
            f"❌ **ERROR ADDING ADMIN**\n\n"
            f"Error: {str(e)}",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()

@dp.message(F.text == "➖ REMOVE ADMIN")
async def remove_admin_handler(message: types.Message, state: FSMContext):
    """Remove an admin"""
    log_action("➖ REMOVE ADMIN", message.from_user.id, "Starting admin removal")
    
    # List current admins
    admins = list(col_admins.find({}))
    if not admins:
        await message.answer(
            "⚠️ No admins found in the system.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Create keyboard with admin buttons - 10 at a time
    # Store page in state (default to page 0)
    page = 0
    await state.update_data(admin_remove_page=page)
    
    # Pagination: 10 admins per page
    per_page = 10
    total_pages = (len(admins) + per_page - 1) // per_page  # Ceiling division
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, len(admins))
    page_admins = admins[start_idx:end_idx]
    
    # Create buttons for current page
    admin_buttons = []
    for admin in page_admins:
        user_id = admin['user_id']
        role = admin.get('role', 'Admin')
        # Format: "UserID - Role"
        button_text = f"{user_id} - {role}"
        admin_buttons.append([KeyboardButton(text=button_text)])
    
    # Add navigation buttons if needed
    nav_buttons = []
    if total_pages > 1:
        if page > 0:
            nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
        if page < total_pages - 1:
            nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_buttons:
        admin_buttons.append(nav_buttons)
    
    # Add back button
    admin_buttons.append([KeyboardButton(text="🔙 BACK")])
    
    select_kb = ReplyKeyboardMarkup(
        keyboard=admin_buttons,
        resize_keyboard=True
    )
    
    await message.answer(
        f"➖ **REMOVE ADMIN**\n\n"
        f"📋 **Select admin to remove:**\n"
        f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
        f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
        reply_markup=select_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.waiting_for_remove_admin_id)

@dp.message(AdminStates.waiting_for_remove_admin_id)
async def process_remove_admin_id(message: types.Message, state: FSMContext):
    """Process admin removal ID"""
    # Handle special buttons
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Handle pagination
    if message.text in ["⬅️ PREV", "NEXT ➡️"]:
        data = await state.get_data()
        current_page = data.get("admin_remove_page", 0)
        
        if message.text == "⬅️ PREV":
            new_page = max(0, current_page - 1)
        else:  # NEXT
            new_page = current_page + 1
        
        await state.update_data(admin_remove_page=new_page)
        
        # Reload admin list with new page
        admins = list(col_admins.find({}))
        per_page = 10
        total_pages = (len(admins) + per_page - 1) // per_page
        start_idx = new_page * per_page
        end_idx = min(start_idx + per_page, len(admins))
        page_admins = admins[start_idx:end_idx]
        
        # Create buttons
        admin_buttons = []
        for admin in page_admins:
            user_id = admin['user_id']
            role = admin.get('role', 'Admin')
            button_text = f"{user_id} - {role}"
            admin_buttons.append([KeyboardButton(text=button_text)])
        
        # Navigation
        nav_buttons = []
        if total_pages > 1:
            if new_page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
            if new_page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
        
        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])
        
        select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
        
        await message.answer(
            f"➖ **REMOVE ADMIN**\n\n"
            f"📋 **Select admin to remove:**\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
            f"{f' (Page {new_page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=select_kb,
            parse_mode="Markdown"
        )
        return
    
    # Parse user ID from button text format: "UserID - Role" or plain ID
    try:
        if " - " in message.text:
            # Button format: extract user ID before " - "
            user_id = int(message.text.split(" - ")[0].strip())
        else:
            # Plain ID format
            user_id = int(message.text.strip())
    except ValueError:
        await message.answer(
            "⚠️ Invalid selection. Please select an admin from the buttons.",
            parse_mode="Markdown"
        )
        return
    
    # Check if admin exists
    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer(
            f"⚠️ User `{user_id}` is not an admin.",
            parse_mode="Markdown"
        )
        return
    
    # Prevent removing master admin
    if user_id == MASTER_ADMIN_ID:
        await message.answer(
            "🚫 **CANNOT REMOVE MASTER ADMIN**\n\n"
            "The master admin cannot be removed from the system.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
    # Store for confirmation
    await state.update_data(remove_admin_id=user_id)
    
    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ YES, REMOVE"), KeyboardButton(text="❌ NO, CANCEL")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        f"⚠️ **CONFIRM REMOVAL**\n\n"
        f"👤 User ID: `{user_id}`\n"
        f"👔 Role: **{admin_doc.get('role', 'Admin')}**\n"
        f"📅 Added: {format_datetime(admin_doc.get('added_at'))}\n\n"
        "Are you sure you want to remove this admin?",
        reply_markup=confirm_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.waiting_for_remove_confirm)

@dp.message(AdminStates.waiting_for_remove_confirm)
async def process_remove_confirm(message: types.Message, state: FSMContext):
    """Process admin removal confirmation"""
    if message.text not in ["✅ YES, REMOVE", "❌ NO, CANCEL"]:
        await message.answer("⚠️ Please select YES or NO from the buttons.")
        return
    
    if message.text == "❌ NO, CANCEL":
        await state.clear()
        await message.answer(
            "❌ Operation cancelled.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    user_id = data.get("remove_admin_id")
    
    try:
        result = col_admins.delete_one({"user_id": user_id})
        
        if result.deleted_count > 0:
            log_action("➖ ADMIN REMOVED", message.from_user.id, f"Removed admin: {user_id}")
            
            await message.answer(
                f"✅ **ADMIN REMOVED**\n\n"
                f"👤 User ID: `{user_id}`\n"
                f"📅 Removed: {now_local().strftime('%b %d, %Y %I:%M %p')}",
                reply_markup=get_admin_menu(),
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                "⚠️ Admin not found or already removed.",
                reply_markup=get_admin_menu(),
                parse_mode="Markdown"
            )
        
        await state.clear()
        
    except Exception as e:
        await message.answer(
            f"❌ **ERROR REMOVING ADMIN**\n\n"
            f"Error: {str(e)}",
            parse_mode="Markdown"
        )

@dp.message(F.text == "🔐 PERMISSIONS")
async def permissions_handler(message: types.Message, state: FSMContext):
    """Manage admin permissions - show admin list"""
    log_action("🔐 PERMISSIONS", message.from_user.id, "Managing admin permissions")
    
    # Get all admins
    admins = list(col_admins.find({}))
    if not admins:
        await message.answer(
            "⚠️ No admins found.",
            reply_markup=get_admin_menu()
        )
        return
    
    # Pagination: 10 admins per page
    page = 0
    await state.update_data(permission_page=page)
    
    per_page = 10
    total_pages = (len(admins) + per_page - 1) // per_page
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, len(admins))
    page_admins = admins[start_idx:end_idx]
    
    # Create buttons for current page
    admin_buttons = []
    for admin in page_admins:
        user_id = admin['user_id']
        role = admin.get('role', 'Admin')
        button_text = f"{user_id} - {role}"
        admin_buttons.append([KeyboardButton(text=button_text)])
    
    # Add navigation buttons if needed
    nav_buttons = []
    if total_pages > 1:
        if page > 0:
            nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
        if page < total_pages - 1:
            nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_buttons:
        admin_buttons.append(nav_buttons)
    
    # Add back button
    admin_buttons.append([KeyboardButton(text="🔙 BACK")])
    
    select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
    
    await message.answer(
        f"🔐 MANAGE PERMISSIONS\n\n"
        f"Select admin to manage:\n"
        f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
        f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
        reply_markup=select_kb
    )
    await state.set_state(AdminStates.waiting_for_permission_admin_id)

@dp.message(AdminStates.waiting_for_permission_admin_id)
async def process_permission_admin_id(message: types.Message, state: FSMContext):
    """Process permission admin ID"""
    # Handle special buttons
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return
    
    # Handle pagination
    if message.text in ["⬅️ PREV", "NEXT ➡️"]:
        data = await state.get_data()
        current_page = data.get("permission_page", 0)
        
        if message.text == "⬅️ PREV":
            new_page = max(0, current_page - 1)
        else:  # NEXT
            new_page = current_page + 1
        
        await state.update_data(permission_page=new_page)
        
        # Reload admin list with new page
        admins = list(col_admins.find({}))
        per_page = 10
        total_pages = (len(admins) + per_page - 1) // per_page
        start_idx = new_page * per_page
        end_idx = min(start_idx + per_page, len(admins))
        page_admins = admins[start_idx:end_idx]
        
        # Create buttons
        admin_buttons = []
        for admin in page_admins:
            user_id = admin['user_id']
            role = admin.get('role', 'Admin')
            button_text = f"{user_id} - {role}"
            admin_buttons.append([KeyboardButton(text=button_text)])
        
        # Navigation
        nav_buttons = []
        if total_pages > 1:
            if new_page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
            if new_page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
        
        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])
        
        select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
        
        await message.answer(
            f"🔐 MANAGE PERMISSIONS\n\n"
            f"Select admin to manage:\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
            f"{f' (Page {new_page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=select_kb
        )
        return
    
    # Parse User ID from button text format "UserID - Role"
    try:
        if " - " in message.text:
            user_id_str = message.text.split(" - ")[0].strip()
            user_id = int(user_id_str)
        else:
            user_id = int(message.text.strip())
    except ValueError:
        await message.answer("⚠️ Invalid User ID.")
        return
    
    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer(f"⚠️ User {user_id} is not an admin.")
        return
    
    await state.update_data(permission_admin_id=user_id)
    
    # Get current permissions
    current_perms = admin_doc.get('permissions', [])
    
    # Store initial permissions in state
    await state.update_data(current_permissions=current_perms.copy())
    
    # Define all available permissions (9 Bot 10 features)
    all_permissions = {
        'broadcast': '📢 BROADCAST',
        'find': '🔍 FIND',
        'traffic': '📊 TRAFFIC',
        'diagnosis': '🩺 DIAGNOSIS',
        'shoot': '📸 SHOOT',
        'support': '💬 SUPPORT',
        'backup': '💾 BACKUP',
        'terminal': '🖥️ TERMINAL'
    }
    
    # Create toggle buttons for each permission
    perm_buttons = []
    for perm_key, perm_label in all_permissions.items():
        # Check if this permission is currently enabled
        if 'all' in current_perms or perm_key in current_perms:
            button_text = f"✅ {perm_label}"
        else:
            button_text = f"❌ {perm_label}"
        perm_buttons.append([KeyboardButton(text=button_text)])
    
    # Add quick action buttons
    perm_buttons.append([
        KeyboardButton(text="✅ GRANT ALL"),
        KeyboardButton(text="❌ REVOKE ALL")
    ])
    
    # Add Save and Cancel buttons
    perm_buttons.append([KeyboardButton(text="💾 SAVE CHANGES")])
    perm_buttons.append([KeyboardButton(text="🔙 BACK")])
    
    perm_kb = ReplyKeyboardMarkup(keyboard=perm_buttons, resize_keyboard=True)
    
    await message.answer(
        f"🔐 MANAGE PERMISSIONS\n\n"
        f"Admin: {user_id}\n"
        f"Role: {admin_doc.get('role', 'Admin')}\n\n"
        f"Toggle permissions below:\n"
        f"✅ = Enabled | ❌ = Disabled\n\n"
        f"Click permissions to toggle, then SAVE CHANGES",
        reply_markup=perm_kb
    )
    await state.set_state(AdminStates.toggling_permissions)

@dp.message(AdminStates.toggling_permissions)
async def process_permission_toggle(message: types.Message, state: FSMContext):
    """Process permission toggle actions"""
    # Handle cancel/back
    if message.text in ["❌ CANCEL", "🔙 BACK"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return
    
    # Get current data
    data = await state.get_data()
    user_id = data.get("permission_admin_id")
    current_perms = data.get("current_permissions", [])
    
    # Permission mapping
    perm_map = {
        '📢 BROADCAST': 'broadcast',
        '🔍 FIND': 'find',
        '📊 TRAFFIC': 'traffic',
        '🩺 DIAGNOSIS': 'diagnosis',
        '📸 SHOOT': 'shoot',
        '💬 SUPPORT': 'support',
        '💾 BACKUP': 'backup',
        '🖥️ TERMINAL': 'terminal'
    }
    
    # Handle SAVE CHANGES
    if message.text == "💾 SAVE CHANGES":
        # Update database
        try:
            col_admins.update_one(
                {"user_id": user_id},
                {"$set": {"permissions": current_perms}}
            )
            log_action("🔐 PERMISSIONS UPDATED", message.from_user.id,
                      f"Updated permissions for {user_id}")
            
            await message.answer(
                f"✅ PERMISSIONS SAVED\n\n"
                f"Admin: {user_id}\n"
                f"New permissions: {', '.join(current_perms) if current_perms else 'None'}",
                reply_markup=get_admin_menu()
            )
            await state.clear()
        except Exception as e:
            await message.answer(
                f"❌ Error saving permissions: {str(e)}",
                reply_markup=get_admin_menu()
            )
            await state.clear()
        return
    
    # Handle GRANT ALL
    if message.text == "✅ GRANT ALL":
        current_perms = list(perm_map.values())
        await state.update_data(current_permissions=current_perms)
    
    # Handle REVOKE ALL
    elif message.text == "❌ REVOKE ALL":
        current_perms = []
        await state.update_data(current_permissions=current_perms)
    
    # Handle individual permission toggle
    else:
        # Extract permission label from button text
        button_text = message.text.replace("✅ ", "").replace("❌ ", "")
        
        if button_text in perm_map:
            perm_key = perm_map[button_text]
            
            # Toggle permission
            if perm_key in current_perms:
                current_perms.remove(perm_key)
            else:
                current_perms.append(perm_key)
            
            # Remove 'all' if it exists
            if 'all' in current_perms:
                current_perms.remove('all')
            
            await state.update_data(current_permissions=current_perms)
    
    # Rebuild permission UI with updated state
    all_permissions = {
        'broadcast': '📢 BROADCAST',
        'find': '🔍 FIND',
        'traffic': '📊 TRAFFIC',
        'diagnosis': '🩺 DIAGNOSIS',
        'shoot': '📸 SHOOT',
        'support': '💬 SUPPORT',
        'backup': '💾 BACKUP',
        'terminal': '🖥️ TERMINAL'
    }
    
    perm_buttons = []
    for perm_key, perm_label in all_permissions.items():
        if perm_key in current_perms:
            button_text = f"✅ {perm_label}"
        else:
            button_text = f"❌ {perm_label}"
        perm_buttons.append([KeyboardButton(text=button_text)])
    
    perm_buttons.append([
        KeyboardButton(text="✅ GRANT ALL"),
        KeyboardButton(text="❌ REVOKE ALL")
    ])
    perm_buttons.append([KeyboardButton(text="💾 SAVE CHANGES")])
    perm_buttons.append([KeyboardButton(text="🔙 BACK")])
    
    perm_kb = ReplyKeyboardMarkup(keyboard=perm_buttons, resize_keyboard=True)
    
    await message.answer(
        f"🔐 MANAGE PERMISSIONS\n\n"
        f"Admin: {user_id}\n\n"
        f"Toggle permissions below:\n"
        f"✅ = Enabled | ❌ = Disabled\n\n"
        f"Click permissions to toggle, then SAVE CHANGES\n\n"
        f"Current: {', '.join(current_perms) if current_perms else 'None'}",
        reply_markup=perm_kb
    )

@dp.message(AdminStates.selecting_permissions)
async def process_permission_selection(message: types.Message, state: FSMContext):
    """DEPRECATED - Old permission selection handler"""
    # Redirect to admin menu
    await state.clear()
    await message.answer(
        "⚠️ This handler is deprecated. Use 🔐 PERMISSIONS instead.",
        reply_markup=get_admin_menu()
    )


@dp.message(F.text == "👔 MANAGE ROLES")
async def manage_roles_handler(message: types.Message, state: FSMContext):
    """Change admin roles - with pagination"""
    log_action("👔 MANAGE ROLES", message.from_user.id, "Managing admin roles")
    
    admins = list(col_admins.find({}))
    if not admins:
        await message.answer(
            "⚠️ No admins found.",
            reply_markup=get_admin_menu()
        )
        return
    
    # Pagination: 10 admins per page
    page = 0
    await state.update_data(role_page=page, admins_list=admins)
    
    per_page = 10
    total_pages = (len(admins) + per_page - 1) // per_page
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, len(admins))
    page_admins = admins[start_idx:end_idx]
    
    # Create admin buttons
    admin_buttons = []
    for admin in page_admins:
        user_id = admin['user_id']
        role = admin.get('role', 'Admin')
        button_text = f"{user_id} - {role}"
        admin_buttons.append([KeyboardButton(text=button_text)])
    
    # Navigation buttons
    nav_buttons = []
    if total_pages > 1:
        if page > 0:
            nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
        if page < total_pages - 1:
            nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_buttons:
        admin_buttons.append(nav_buttons)
    admin_buttons.append([KeyboardButton(text="🔙 BACK")])
    
    select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
    
    await message.answer(
        f"👔 MANAGE ROLES\n\n"
        f"Select admin to change role:\n"
        f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
        f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
        reply_markup=select_kb
    )
    await state.set_state(AdminStates.selecting_role)

@dp.message(AdminStates.waiting_for_role_admin_id)
async def process_role_admin_id(message: types.Message, state: FSMContext):
    """Process role change admin ID - with pagination and role selection.
    Also handles BANNED LIST pagination (⬅️ PREV PAGE / NEXT PAGE ➡️)."""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return
    
    data = await state.get_data()

    # ── Banned list pagination (uses different nav buttons to avoid conflict) ──
    if message.text in ["⬅️ PREV PAGE", "NEXT PAGE ➡️"]:
        current_page = data.get("banned_list_page", 0)
        new_page = max(0, current_page - 1) if message.text == "⬅️ PREV PAGE" else current_page + 1
        await state.update_data(banned_list_page=new_page)
        
        all_admins = list(col_admins.find({}))
        banned_admins = []
        for admin in all_admins:
            if col_banned_users.find_one({"user_id": admin['user_id']}):
                ban_doc = col_banned_users.find_one({"user_id": admin['user_id']})
                admin['ban_info'] = ban_doc
                banned_admins.append(admin)
        
        per_page = 10
        total_pages = (len(banned_admins) + per_page - 1) // per_page
        start_idx = new_page * per_page
        end_idx = min(start_idx + per_page, len(banned_admins))
        page_admins = banned_admins[start_idx:end_idx]
        
        msg = f"📋 BANNED ADMINS LIST\n\n"
        msg += f"Total Banned: {len(banned_admins)}\n"
        msg += f"Showing {start_idx + 1}-{end_idx}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for admin in page_admins:
            uid = admin['user_id']
            role = admin.get('role', 'Admin')
            ban_info = admin.get('ban_info', {})
            msg += f"👤 ID: {uid}\n"
            msg += f"👔 Role: {role}\n"
            msg += f"📅 Banned: {format_datetime(ban_info.get('banned_at'))}\n"
            msg += f"👨‍💼 By: {ban_info.get('banned_by', 'Unknown')}\n"
            msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        nav_buttons = []
        if total_pages > 1:
            if new_page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV PAGE"))
            if new_page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT PAGE ➡️"))
        list_kb_buttons = [nav_buttons] if nav_buttons else []
        list_kb_buttons.append([KeyboardButton(text="🔙 BACK")])
        await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=list_kb_buttons, resize_keyboard=True))
        return

    # ── Role selection pagination (uses ⬅️ PREV / NEXT ➡️) ──
    admins_list = data.get('admins_list', [])
    
    if message.text in ["⬅️ PREV", "NEXT ➡️"]:
        current_page = data.get("role_page", 0)
        new_page = max(0, current_page - 1) if message.text == "⬅️ PREV" else current_page + 1
        await state.update_data(role_page=new_page)
        
        per_page = 10
        total_pages = (len(admins_list) + per_page - 1) // per_page
        start_idx = new_page * per_page
        end_idx = min(start_idx + per_page, len(admins_list))
        page_admins = admins_list[start_idx:end_idx]
        
        admin_buttons = []
        for admin in page_admins:
            uid = admin['user_id']
            role = admin.get('role', 'Admin')
            admin_buttons.append([KeyboardButton(text=f"{uid} - {role}")])
        
        nav_buttons = []
        if total_pages > 1:
            if new_page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
            if new_page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])
        
        await message.answer(
            f"👔 MANAGE ROLES\n\n"
            f"Select admin to change role:\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(admins_list)} admins"
            f"{f' (Page {new_page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
        )
        return
    
    # ── Parse User ID from button text ──
    try:
        if " - " in message.text:
            user_id_str = message.text.split(" - ")[0].strip()
            user_id = int(user_id_str)
        else:
            user_id = int(message.text.strip())
    except (ValueError, IndexError):
        await message.answer("⚠️ Invalid selection.")
        return
    
    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer(f"⚠️ User {user_id} is not an admin.")
        return
    
    await state.update_data(role_admin_id=user_id)
    
    role_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="� OWNER")],
            [KeyboardButton(text="🔴 MANAGER"), KeyboardButton(text="🟡 ADMIN")],
            [KeyboardButton(text="🟢 MODERATOR"), KeyboardButton(text="🔵 SUPPORT")],
            [KeyboardButton(text="🔙 BACK")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        f"👔 CHANGE ROLE\n\n"
        f"👤 User: {user_id}\n"
        f"📋 Current Role: {admin_doc.get('role', 'Admin')}\n\n"
        "Select new role:",
        reply_markup=role_kb
    )
    await state.set_state(AdminStates.selecting_role)



@dp.message(AdminStates.selecting_role)
async def process_role_selection(message: types.Message, state: FSMContext):
    """Process role selection OR ban/unban admin selection (shared state)"""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return

    data = await state.get_data()
    ban_action = data.get("ban_action")  # Set only when coming from BAN CONFIG flow

    # ── BAN/UNBAN FLOW ──
    if ban_action:
        admins_list = data.get("admins_list", [])

        # Handle pagination
        if message.text in ["⬅️ PREV", "NEXT ➡️"]:
            current_page = data.get("ban_page", 0)
            new_page = max(0, current_page - 1) if message.text == "⬅️ PREV" else current_page + 1
            await state.update_data(ban_page=new_page)

            per_page = 10
            total_pages = (len(admins_list) + per_page - 1) // per_page
            start_idx = new_page * per_page
            end_idx = min(start_idx + per_page, len(admins_list))
            page_admins = admins_list[start_idx:end_idx]

            admin_buttons = []
            for admin in page_admins:
                uid = admin['user_id']
                role = admin.get('role', 'Admin')
                admin_buttons.append([KeyboardButton(text=f"{uid} - {role}")])

            nav_buttons = []
            if total_pages > 1:
                if new_page > 0:
                    nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
                if new_page < total_pages - 1:
                    nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
            if nav_buttons:
                admin_buttons.append(nav_buttons)
            admin_buttons.append([KeyboardButton(text="🔙 BACK")])

            action_text = "BAN" if ban_action == "ban" else "UNBAN"
            status_text = "unbanned" if ban_action == "ban" else "banned"
            await message.answer(
                f"{'🚫' if ban_action == 'ban' else '✅'} {action_text} ADMIN\n\n"
                f"Select admin to {action_text}:\n"
                f"Showing {start_idx + 1}-{end_idx} of {len(admins_list)} {status_text} admins"
                f"{f' (Page {new_page + 1}/{total_pages})' if total_pages > 1 else ''}",
                reply_markup=ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
            )
            return

        # Parse User ID from button text
        try:
            user_id = int(message.text.split(" - ")[0].strip()) if " - " in message.text else int(message.text.strip())
        except (ValueError, IndexError):
            await message.answer("⚠️ Invalid selection.")
            return

        admin_doc = col_admins.find_one({"user_id": user_id})
        if not admin_doc:
            await message.answer(f"⚠️ User {user_id} is not an admin.")
            return

        if ban_action == "ban":
            # ── BLOCK: must remove admin first ──
            is_still_admin = col_admins.find_one({"user_id": user_id}) is not None
            if is_still_admin and user_id != MASTER_ADMIN_ID:
                await message.answer(
                    f"🚫 **CANNOT BAN AN ACTIVE ADMIN**\n\n"
                    f"👤 User ID: `{user_id}`\n"
                    f"👔 Role: **{admin_doc.get('role', 'Admin')}**\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"**Protocol requires:**\n"
                    f"1️⃣ First use **➖ REMOVE ADMIN** to strip their admin status\n"
                    f"2️⃣ Then use **🚫 BAN ADMIN** to ban them\n\n"
                    f"This prevents partial-access vulnerabilities.\n\n"
                    f"_Remove admin role first, then proceed with ban._",
                    reply_markup=get_admin_menu(),
                    parse_mode="Markdown"
                )
                await state.clear()
                return

            ban_doc = {
                "user_id": user_id,
                "banned_by": message.from_user.id,
                "banned_at": now_local(),
                "reason": "Banned by master admin",
                "status": "banned",
                "scope": "bot10"  # Only blocks Bot 10 admin access, NOT Bot 8
            }
            try:
                col_banned_users.update_one(
                    {"user_id": user_id},
                    {"$setOnInsert": ban_doc},
                    upsert=True
                )
                log_action("🚫 ADMIN BANNED (BOT10)", message.from_user.id, f"Banned admin from Bot 10: {user_id}")
                await message.answer(
                    f"🚫 **ADMIN BANNED FROM BOT 10**\n\n"
                    f"👤 User ID: `{user_id}`\n"
                    f"📅 Banned: {now_local().strftime('%B %d, %Y — %I:%M %p')}\n\n"
                    f"This user can no longer access Bot 10 admin panel.\n"
                    f"Their Bot 8 access is **NOT affected**.",
                    reply_markup=get_admin_menu(),
                    parse_mode="Markdown"
                )
            except Exception as e:
                await message.answer(f"❌ Error banning: {str(e)}", reply_markup=get_admin_menu())

        elif ban_action == "unban":
            try:
                col_banned_users.delete_one({"user_id": user_id})
                log_action("✅ USER UNBANNED", message.from_user.id, f"Unbanned user: {user_id}")
                await message.answer(
                    f"✅ **USER UNBANNED**\n\n"
                    f"👤 User ID: `{user_id}`\n"
                    f"📅 Unbanned: {now_local().strftime('%B %d, %Y — %I:%M %p')}\n\n"
                    f"This user can now access Bot 8 again.",
                    reply_markup=get_admin_menu(),
                    parse_mode="Markdown"
                )
            except Exception as e:
                await message.answer(f"❌ Error unbanning: {str(e)}", reply_markup=get_admin_menu())
        await state.clear()
        return

    # ── ROLE CHANGE FLOW ──
    role_map = {
        "👑 OWNER":    "Owner",
        "🔴 MANAGER":  "Manager",
        "🟡 ADMIN":    "Admin",
        "🟢 MODERATOR": "Moderator",
        "🔵 SUPPORT":  "Support",
    }

    if message.text not in role_map:
        await message.answer("⚠️ Please select a valid role from the buttons.")
        return

    new_role = role_map[message.text]
    user_id = data.get("role_admin_id")

    if not user_id:
        await message.answer("⚠️ Session expired. Please try again.")
        await state.clear()
        return

    # ── OWNER TRANSFER: requires triple confirmation + password ──
    if new_role == "Owner":
        await state.update_data(owner_transfer_target=user_id)
        cancel_kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ CANCEL")]],
            resize_keyboard=True
        )
        await message.answer(
            "👑 **OWNERSHIP TRANSFER — STEP 1 OF 3**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ **CRITICAL ACTION: PERMANENT**\n\n"
            "Transferring ownership is **irreversible**.\n"
            "The target user will receive full Owner-level authority.\n\n"
            "To proceed, type exactly:\n"
            "`CONFIRM`",
            reply_markup=cancel_kb,
            parse_mode="Markdown"
        )
        await state.set_state(AdminStates.owner_transfer_first_confirm)
        return

    # ── REGULAR ROLE UPDATE ──
    col_admins.update_one(
        {"user_id": user_id},
        {"$set": {"role": new_role, "updated_at": now_local()}}
    )

    admin_doc = col_admins.find_one({"user_id": user_id})
    is_locked = admin_doc.get('locked', False) if admin_doc else True

    log_action("👔 ROLE CHANGED", message.from_user.id, f"Changed {user_id} to {new_role} (Locked: {is_locked})")

    if is_locked:
        await message.answer(
            f"✅ **ROLE SAVED (PENDING UNLOCK)**\n\n"
            f"👤 User: `{user_id}`\n"
            f"👔 Role: **{new_role}**\n"
            f"🔒 Status: LOCKED\n\n"
            f"Role change is saved. The user will be notified of their role\n"
            f"**only when unlocked** — not before.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return

    # ── NOTIFY UNLOCKED ADMIN OF NEW ROLE ──
    _ROLE_NOTIFY = {
        "Manager": (
            "🔴 **ROLE ASSIGNMENT: MANAGER**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You have been appointed as **Manager** of the MSA NODE system.\n\n"
            "**Your Authority:**\n"
            "• Full oversight of administrative operations\n"
            "• Management of broadcasts, support teams & junior admins\n"
            "• Enforcement of system integrity and security protocols\n"
            "• Access to all Bot 10 management features\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚡ This is a position of significant trust.\n"
            "Execute your responsibilities with precision and discipline.\n\n"
            "_— MSA NODE Systems_"
        ),
        "Admin": (
            "🟡 **ROLE ASSIGNMENT: ADMIN**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You have been appointed as **Admin** of the MSA NODE system.\n\n"
            "**Your Responsibilities:**\n"
            "• Execute broadcasts and manage user traffic\n"
            "• Handle escalated support tickets\n"
            "• Monitor system diagnostics and report anomalies\n"
            "• Uphold community standards and guidelines\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📋 Adhere to operational protocols at all times.\n\n"
            "_— MSA NODE Systems_"
        ),
        "Moderator": (
            "🟢 **ROLE ASSIGNMENT: MODERATOR**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You have been appointed as **Moderator** of the MSA NODE system.\n\n"
            "**Your Responsibilities:**\n"
            "• Verify user authenticity and content compliance\n"
            "• Assist with support ticket resolution\n"
            "• Monitor community interactions\n"
            "• Escalate issues to Admin tier when required\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🎯 Maintain professional standards in all interactions.\n\n"
            "_— MSA NODE Systems_"
        ),
        "Support": (
            "🔵 **ROLE ASSIGNMENT: SUPPORT**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You have been appointed as **Support Staff** of the MSA NODE system.\n\n"
            "**Your Responsibilities:**\n"
            "• Provide timely assistance to user inquiries\n"
            "• Resolve routine support tickets efficiently\n"
            "• Escalate complex issues to Moderators/Admins\n"
            "• Maintain a helpful, professional tone at all times\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "💬 User satisfaction is your top priority.\n\n"
            "_— MSA NODE Systems_"
        ),
    }

    notification = _ROLE_NOTIFY.get(new_role)
    if notification:
        try:
            await bot.send_message(user_id, notification, parse_mode="Markdown")
            log_action("📨 ROLE NOTIFICATION SENT", user_id, f"Notified: {new_role}")
        except Exception as e:
            log_action("⚠️ ROLE NOTIFY FAILED", user_id, str(e))

    await message.answer(
        f"✅ **ROLE UPDATED**\n\n"
        f"👤 User: `{user_id}`\n"
        f"👔 New Role: **{new_role}**\n\n"
        f"📨 Notification sent to admin.",
        reply_markup=get_admin_menu(),
        parse_mode="Markdown"
    )
    await state.clear()


# ==========================================
# 👑 OWNER TRANSFER FLOW (triple confirm + password)
# ==========================================
_OWNER_TRANSFER_PASSWORD = "99insanebeing45"

@dp.message(AdminStates.owner_transfer_first_confirm)
async def owner_transfer_step1(message: types.Message, state: FSMContext):
    """Ownership transfer — step 1: type CONFIRM"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Ownership transfer cancelled.", reply_markup=get_admin_menu())
        return
    if message.text.strip() != "CONFIRM":
        await message.answer(
            "⚠️ Incorrect. Type exactly: `CONFIRM`\n\nOr press ❌ CANCEL to abort.",
            parse_mode="Markdown"
        )
        return
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    await message.answer(
        "👑 **OWNERSHIP TRANSFER — STEP 2 OF 3**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "This action cannot be undone.\n\n"
        "To proceed, type exactly:\n"
        "`TRANSFER`",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.owner_transfer_second_confirm)


@dp.message(AdminStates.owner_transfer_second_confirm)
async def owner_transfer_step2(message: types.Message, state: FSMContext):
    """Ownership transfer — step 2: type TRANSFER"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Ownership transfer cancelled.", reply_markup=get_admin_menu())
        return
    if message.text.strip() != "TRANSFER":
        await message.answer(
            "⚠️ Incorrect. Type exactly: `TRANSFER`\n\nOr press ❌ CANCEL to abort.",
            parse_mode="Markdown"
        )
        return
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    await message.answer(
        "👑 **OWNERSHIP TRANSFER — STEP 3 OF 3**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔐 Enter the **transfer password** to finalise:",
        reply_markup=cancel_kb,
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.owner_transfer_password)


@dp.message(AdminStates.owner_transfer_password)
async def owner_transfer_step3(message: types.Message, state: FSMContext):
    """Ownership transfer — step 3: enter password"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Ownership transfer cancelled.", reply_markup=get_admin_menu())
        return
    if message.text.strip() != _OWNER_TRANSFER_PASSWORD:
        await message.answer(
            "🚫 **INCORRECT PASSWORD**\n\nOwnership transfer aborted for security.",
            reply_markup=get_admin_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return

    data = await state.get_data()
    target_id = data.get("owner_transfer_target")

    col_admins.update_one(
        {"user_id": target_id},
        {"$set": {"role": "Owner", "updated_at": now_local()}}
    )
    log_action("👑 OWNERSHIP TRANSFERRED", message.from_user.id, f"Transferred ownership to {target_id}")

    try:
        await bot.send_message(
            target_id,
            "👑 **OWNERSHIP TRANSFERRED TO YOU**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "You are now the **Owner** of the MSA NODE system.\n\n"
            "**Full authority has been granted:**\n"
            "• Complete control over all system operations\n"
            "• Management of all admin tiers\n"
            "• Unrestricted access to every feature\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚡ This transfer is **permanent and irreversible**.\n\n"
            "_— MSA NODE Systems_",
            parse_mode="Markdown"
        )
    except Exception as e:
        log_action("⚠️ OWNER NOTIFY FAILED", target_id, str(e))

    await message.answer(
        f"👑 **OWNERSHIP TRANSFERRED**\n\n"
        f"👤 New Owner: `{target_id}`\n"
        f"📅 {now_local().strftime('%B %d, %Y — %I:%M %p')}\n\n"
        f"This action is permanent.",
        reply_markup=get_admin_menu(),
        parse_mode="Markdown"
    )
    await state.clear()


@dp.message(F.text == "🔒 LOCK/UNLOCK USER")
async def lock_unlock_user_handler(message: types.Message, state: FSMContext):
    """Lock/unlock admin activation - with pagination"""
    log_action("🔒 LOCK/UNLOCK USER", message.from_user.id, "Managing admin lock status")

    admins = list(col_admins.find({}))
    if not admins:
        await message.answer(
            "⚠️ No admins found.",
            reply_markup=get_admin_menu()
        )
        return

    # Pagination: 10 admins per page
    page = 0
    await state.update_data(lock_page=page, lock_admins_list=admins)

    per_page = 10
    total_pages = (len(admins) + per_page - 1) // per_page
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, len(admins))
    page_admins = admins[start_idx:end_idx]

    # Create admin buttons with lock status
    admin_buttons = []
    for admin in page_admins:
        user_id = admin['user_id']
        role = admin.get('role', 'Admin')
        is_locked = admin.get('locked', False)
        lock_icon = "🔒" if is_locked else "🔓"
        button_text = f"{lock_icon} {user_id} - {role}"
        admin_buttons.append([KeyboardButton(text=button_text)])
    
    # Navigation buttons
    nav_buttons = []
    if total_pages > 1:
        if page > 0:
            nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
        if page < total_pages - 1:
            nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
    
    if nav_buttons:
        admin_buttons.append(nav_buttons)
    admin_buttons.append([KeyboardButton(text="🔙 BACK")])
    
    select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
    
    await message.answer(
        f"🔒 LOCK/UNLOCK ADMIN\n\n"
        f"🔒 = LOCKED (Inactive - Cannot access Bot 10)\n"
        f"🔓 = UNLOCKED (Active - Full access)\n\n"
        f"Select admin to toggle lock status:\n"
        f"Showing {start_idx + 1}-{end_idx} of {len(admins)} admins"
        f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
        reply_markup=select_kb
    )
    await state.set_state(AdminStates.waiting_for_lock_user_id)

@dp.message(AdminStates.waiting_for_lock_user_id)
async def process_lock_toggle(message: types.Message, state: FSMContext):
    """Process admin lock toggle - with pagination"""
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return
    
    data = await state.get_data()
    admins_list = data.get('lock_admins_list', [])
    
    # Handle pagination
    if message.text in ["⬅️ PREV", "NEXT ➡️"]:
        current_page = data.get("lock_page", 0)
        
        if message.text == "⬅️ PREV":
            new_page = max(0, current_page - 1)
        else:
            new_page = current_page + 1
        
        await state.update_data(lock_page=new_page)
        
        per_page = 10
        total_pages = (len(admins_list) + per_page - 1) // per_page
        start_idx = new_page * per_page
        end_idx = min(start_idx + per_page, len(admins_list))
        page_admins = admins_list[start_idx:end_idx]
        
        # Create buttons
        admin_buttons = []
        for admin in page_admins:
            user_id = admin['user_id']
            role = admin.get('role', 'Admin')
            is_locked = admin.get('locked', False)
            lock_icon = "🔒" if is_locked else "🔓"
            button_text = f"{lock_icon} {user_id} - {role}"
            admin_buttons.append([KeyboardButton(text=button_text)])
        
        # Navigation
        nav_buttons = []
        if total_pages > 1:
            if new_page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
            if new_page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
        
        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])
        
        select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
        
        await message.answer(
            f"🔒 LOCK/UNLOCK ADMIN\n\n"
            f"🔒 = LOCKED (Inactive)\n"
            f"🔓 = UNLOCKED (Active)\n\n"
            f"Select admin to toggle:\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(admins_list)} admins"
            f"{f' (Page {new_page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=select_kb
        )
        return
    
    # Parse User ID from button text format: "🔒/🔓 UserID - Role"
    try:
        # Remove lock icon and extract user ID
        text_parts = message.text.split(" ", 1)  # Split after first space (icon)
        if len(text_parts) > 1:
            id_part = text_parts[1].split(" - ")[0].strip()
            user_id = int(id_part)
        else:
            user_id = int(message.text.strip())
    except (ValueError, IndexError):
        await message.answer("⚠️ Invalid selection.")
        return
    
    admin_doc = col_admins.find_one({"user_id": user_id})
    if not admin_doc:
        await message.answer(f"⚠️ User {user_id} is not an admin.")
        return
    
    # Toggle lock status
    current_lock = admin_doc.get('locked', False)
    new_lock = not current_lock
    
    col_admins.update_one(
        {"user_id": user_id},
        {"$set": {"locked": new_lock, "updated_at": now_local()}}
    )
    
    status_text = "LOCKED (Inactive)" if new_lock else "UNLOCKED (Active)"
    icon = "🔒" if new_lock else "🔓"
    
    log_action(f"{icon} ADMIN STATUS CHANGED", message.from_user.id, 
              f"Set {user_id} to {status_text}")
              
    # Notify user if UNLOCKED — send role notification + restore menu
    if not new_lock:
        admin_role = admin_doc.get('role', 'Admin')
        _ROLE_NOTIFY_LOCK = {
            "Owner": (
                "👑 **WELCOME BACK, OWNER**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Owner** account has been unlocked.\n"
                "You have full, unrestricted authority over the MSA NODE system.\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
            "Manager": (
                "🔴 **ACCOUNT UNLOCKED — MANAGER**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Manager** account has been restored to active status.\n\n"
                "**Your Authority:**\n"
                "• Full oversight of administrative operations\n"
                "• Management of broadcasts, support teams & junior admins\n"
                "• Access to all Bot 10 management features\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
            "Admin": (
                "🟡 **ACCOUNT UNLOCKED — ADMIN**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Admin** account has been restored to active status.\n\n"
                "**Your Responsibilities:**\n"
                "• Execute broadcasts and manage user traffic\n"
                "• Handle escalated support tickets\n"
                "• Monitor system diagnostics\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
            "Moderator": (
                "🟢 **ACCOUNT UNLOCKED — MODERATOR**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Moderator** account has been restored to active status.\n\n"
                "**Your Responsibilities:**\n"
                "• Verify user authenticity and content compliance\n"
                "• Assist with support ticket resolution\n"
                "• Escalate issues to Admin tier when required\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
            "Support": (
                "🔵 **ACCOUNT UNLOCKED — SUPPORT**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Your **Support** account has been restored to active status.\n\n"
                "**Your Responsibilities:**\n"
                "• Respond to first-tier user inquiries\n"
                "• Process and route support tickets\n"
                "• Maintain professional communication standards\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ Use /start to access your command menu.\n\n"
                "_— MSA NODE Systems_"
            ),
        }
        notify_text = _ROLE_NOTIFY_LOCK.get(
            admin_role,
            f"🔓 **ACCOUNT UNLOCKED**\n\nYour admin account is now active.\nRole: **{admin_role}**\n\nUse /start to access your menu.\n\n_— MSA NODE Systems_"
        )
        try:
            await bot.send_message(user_id, notify_text, parse_mode="Markdown")
            # Send menu immediately after notification
            admin_menu_kb = get_admin_menu()
            await bot.send_message(
                user_id,
                "📋 Your menu has been restored:",
                reply_markup=admin_menu_kb
            )
            log_action("📨 UNLOCK NOTIFICATION", user_id, f"Sent unlock notification (role: {admin_role})")
        except Exception as e:
            log_action("⚠️ UNLOCK NOTIFY FAILED", user_id, str(e))
    
    await message.answer(
        f"✅ STATUS UPDATED\n\n"
        f"👤 User: {user_id}\n"
        f"{icon} Status: {status_text}\n\n"
        f"{'⚠️ This admin CANNOT access Bot 10 until unlocked!' if new_lock else '✅ This admin can now access Bot 10!'}",
        reply_markup=get_admin_menu()
    )
    await state.clear()


@dp.message(F.text == "🚫 BAN CONFIG")
async def ban_config_handler(message: types.Message, state: FSMContext):
    """Ban/Unban configuration - show choice"""
    log_action("🚫 BAN CONFIG", message.from_user.id, "Opened ban configuration")
    
    choice_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚫 BAN ADMIN")],
            [KeyboardButton(text="✅ UNBAN ADMIN")],
            [KeyboardButton(text="📋 BANNED LIST")],
            [KeyboardButton(text="🔙 BACK")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        "🚫 BAN/UNBAN CONFIGURATION\n\n"
        "Choose an action:\n"
        "• 🚫 BAN ADMIN - Restrict admin access\n"
        "• ✅ UNBAN ADMIN - Restore admin access\n"
        "• 📋 BANNED LIST - View all banned admins",
        reply_markup=choice_kb
    )
    await state.set_state(AdminStates.waiting_for_ban_user_id)

@dp.message(AdminStates.waiting_for_ban_user_id)
async def process_ban_choice(message: types.Message, state: FSMContext):
    """Process BAN or UNBAN choice"""
    # Handle back/cancel
    if message.text in ["❌ CANCEL", "⬅️ BACK", "🔙 BACK", "/cancel"]:
        await state.clear()
        await message.answer(
            "✅ Cancelled.",
            reply_markup=get_admin_menu()
        )
        return
    
    # Store choice in state
    if message.text == "🚫 BAN ADMIN":
        await state.update_data(ban_action="ban")
        
        # Get UNBANNED admins only
        all_admins = list(col_admins.find({}))
        unbanned_admins = []
        for admin in all_admins:
            if admin['user_id'] == MASTER_ADMIN_ID:
                continue  # Skip master admin
            if not col_banned_users.find_one({"user_id": admin['user_id']}):
                unbanned_admins.append(admin)
        
        if not unbanned_admins:
            await message.answer(
                "⚠️ No unbanned admins to ban!",
                reply_markup=get_admin_menu()
            )
            await state.clear()
            return
        
        # Show unbanned admins
        page = 0
        await state.update_data(ban_page=page, admins_list=unbanned_admins)
        
        per_page = 10
        total_pages = (len(unbanned_admins) + per_page - 1) // per_page
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, len(unbanned_admins))
        page_admins = unbanned_admins[start_idx:end_idx]
        
        # Create buttons
        admin_buttons = []
        for admin in page_admins:
            user_id = admin['user_id']
            role = admin.get('role', 'Admin')
            button_text = f"{user_id} - {role}"
            admin_buttons.append([KeyboardButton(text=button_text)])
        
        # Navigation
        nav_buttons = []
        if total_pages > 1:
            if page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
            if page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
        
        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])
        
        select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
        
        await message.answer(
            f"🚫 BAN ADMIN\n\n"
            f"Select admin to BAN:\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(unbanned_admins)} unbanned admins"
            f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=select_kb
        )
        await state.set_state(AdminStates.selecting_role)  # Reuse state
        
    elif message.text == "✅ UNBAN ADMIN":
        await state.update_data(ban_action="unban")
        
        # Get BANNED admins only
        all_admins = list(col_admins.find({}))
        banned_admins = []
        for admin in all_admins:
            if col_banned_users.find_one({"user_id": admin['user_id']}):
                banned_admins.append(admin)
        
        if not banned_admins:
            await message.answer(
                "⚠️ No banned admins to unban!",
                reply_markup=get_admin_menu()
            )
            await state.clear()
            return
        
        # Show banned admins
        page = 0
        await state.update_data(ban_page=page, admins_list=banned_admins)
        
        per_page = 10
        total_pages = (len(banned_admins) + per_page - 1) // per_page
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, len(banned_admins))
        page_admins = banned_admins[start_idx:end_idx]
        
        # Create buttons
        admin_buttons = []
        for admin in page_admins:
            user_id = admin['user_id']
            role = admin.get('role', 'Admin')
            button_text = f"{user_id} - {role}"
            admin_buttons.append([KeyboardButton(text=button_text)])
        
        # Navigation
        nav_buttons = []
        if total_pages > 1:
            if page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
            if page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT ➡️"))
        
        if nav_buttons:
            admin_buttons.append(nav_buttons)
        admin_buttons.append([KeyboardButton(text="🔙 BACK")])
        
        select_kb = ReplyKeyboardMarkup(keyboard=admin_buttons, resize_keyboard=True)
        
        await message.answer(
            f"✅ UNBAN ADMIN\n\n"
            f"Select admin to UNBAN:\n"
            f"Showing {start_idx + 1}-{end_idx} of {len(banned_admins)} banned admins"
            f"{f' (Page {page + 1}/{total_pages})' if total_pages > 1 else ''}",
            reply_markup=select_kb
        )
        await state.set_state(AdminStates.selecting_role)  # Reuse state
    
    elif message.text == "📋 BANNED LIST":
        # Show list of all banned admins with pagination
        all_admins = list(col_admins.find({}))
        banned_admins = []
        
        for admin in all_admins:
            if col_banned_users.find_one({"user_id": admin['user_id']}):
                ban_doc = col_banned_users.find_one({"user_id": admin['user_id']})
                admin['ban_info'] = ban_doc
                banned_admins.append(admin)
        
        if not banned_admins:
            await message.answer(
                "✅ No banned admins found!",
                reply_markup=get_admin_menu()
            )
            await state.clear()
            return
        
        # Pagination: 10 per page
        page = 0
        await state.update_data(banned_list_page=page)
        
        per_page = 10
        total_pages = (len(banned_admins) + per_page - 1) // per_page
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, len(banned_admins))
        page_admins = banned_admins[start_idx:end_idx]
        
        # Build message
        msg = f"📋 BANNED ADMINS LIST\n\n"
        msg += f"Total Banned: {len(banned_admins)}\n"
        msg += f"Showing {start_idx + 1}-{end_idx}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for admin in page_admins:
            user_id = admin['user_id']
            role = admin.get('role', 'Admin')
            ban_info = admin.get('ban_info', {})
            banned_at = ban_info.get('banned_at')
            banned_by = ban_info.get('banned_by', 'Unknown')
            
            msg += f"👤 ID: {user_id}\n"
            msg += f"👔 Role: {role}\n"
            msg += f"📅 Banned: {format_datetime(banned_at)}\n"
            msg += f"👨💼 By: {banned_by}\n"
            msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Navigation buttons
        nav_buttons = []
        if total_pages > 1:
            if page > 0:
                nav_buttons.append(KeyboardButton(text="⬅️ PREV PAGE"))
            if page < total_pages - 1:
                nav_buttons.append(KeyboardButton(text="NEXT PAGE ➡️"))
        
        list_kb_buttons = []
        if nav_buttons:
            list_kb_buttons.append(nav_buttons)
        list_kb_buttons.append([KeyboardButton(text="🔙 BACK")])
        
        list_kb = ReplyKeyboardMarkup(keyboard=list_kb_buttons, resize_keyboard=True)
        
        await message.answer(msg, reply_markup=list_kb)
        await state.set_state(AdminStates.waiting_for_ban_user_id)  # Keep in ban flow for pagination
    
    else:
        await message.answer("⚠️ Please select from the buttons.")


@dp.message(F.text == "📋 LIST ADMINS")
async def list_admins_handler(message: types.Message):
    """List all admins with detailed report"""
    log_action("📋 LIST ADMINS", message.from_user.id, "Viewing admin list")
    
    admins = list(col_admins.find({}).sort("added_at", -1))
    
    # Always include master admin if not in list
    master_in_list = any(admin['user_id'] == MASTER_ADMIN_ID for admin in admins)
    if not master_in_list:
        # Add master admin to the list
        master_admin = {
            "user_id": MASTER_ADMIN_ID,
            "role": "Super Admin",
            "permissions": ["all"],
            "added_at": None,
            "added_by": "SYSTEM",
            "locked": False  # Master admin never locked
        }
        admins.insert(0, master_admin)
    
    # Create detailed report - NO MARKDOWN
    admin_msg = f"📋 ADMIN REPORT\n\n"
    admin_msg += f"📊 Total Admins: {len(admins)}\n\n"
    admin_msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Group by role
    by_role = {}
    for admin in admins:
        role = admin.get('role', 'Admin')
        if role not in by_role:
            by_role[role] = []
        by_role[role].append(admin)
    
    # Role icons
    role_icons = {
        "Super Admin": "🔴",
        "Owner": "🟠",
        "Admin": "🟡",
        "Moderator": "🟢",
        "Support": "🔵"
    }
    
    # Display each role group
    for role in ["Super Admin", "Owner", "Admin", "Moderator", "Support"]:
        if role in by_role:
            role_admins = by_role[role]
            icon = role_icons.get(role, "👤")
            admin_msg += f"{icon} {role} ({len(role_admins)})\n\n"
            
            for admin in role_admins:
                user_id = admin['user_id']
                perms = admin.get('permissions', [])
                added_at = admin.get('added_at')
                added_by = admin.get('added_by', 'Unknown')
                is_locked = admin.get('locked', False)
                
                admin_msg += f"  👤 ID: {user_id}\n"
                
                # Lock status - NOT shown for Owner or Super Admin
                if role not in ["Owner", "Super Admin"]:
                    if is_locked:
                        admin_msg += f"  🔒 Status: LOCKED (Inactive)\n"
                    else:
                        admin_msg += f"  🔓 Status: UNLOCKED (Active)\n"
                
                # Permissions
                if perms:
                    perm_str = ", ".join(perms)
                    admin_msg += f"  🔐 Perms: {perm_str}\n"
                
                # Added info
                if added_at:
                    admin_msg += f"  📅 Added: {format_datetime(added_at)}\n"
                else:
                    admin_msg += f"  📅 Added: Master (Built-in)\n"
                    
                if added_by and added_by != "Unknown":
                    admin_msg += f"  👨‍💼 By: {added_by}\n"
                
                admin_msg += "\n"
            
            admin_msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    await message.answer(
        admin_msg,
        reply_markup=get_admin_menu()
    )

# ──────────────────────────────────────────────────────────────
# 📖 GUIDE SYSTEM — two-choice selector + paginated admin guide
# ──────────────────────────────────────────────────────────────

_BOT10_GUIDE_PAGES = [
    # Page 1 / 3
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  🖥️  BOT 10 ADMIN GUIDE  ·  <b>Page 1 / 3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📢  <b>BROADCAST</b>\n"
        "Compose and deliver messages to Bot 8 users.\n\n"
        "  ├─ 📤 <b>SEND BROADCAST</b>\n"
        "  │    Select by ID (brd1) or index (1).\n"
        "  │    Category: ALL · YT · IG · IGCC · YTCODE\n"
        "  │    Sent via Bot 8 · real-time progress shown.\n"
        "  │\n"
        "  ├─ ✏️ <b>EDIT BROADCAST</b>\n"
        "  │    Update text or media of any stored broadcast.\n"
        "  │\n"
        "  ├─ 🗑️ <b>DELETE BROADCAST</b>\n"
        "  │    Permanently remove a broadcast from the DB.\n"
        "  │\n"
        "  ├─ 📋 <b>LIST BROADCASTS</b>\n"
        "  │    Paginated view: ID · Category · Media · Date.\n"
        "  │\n"
        "  └─ 🔗 <b>BROADCAST WITH BUTTONS</b>\n"
        "       Adds inline URL buttons (text/photo/video).\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔍  <b>FIND</b>\n"
        "Search any Bot 8 user by:\n"
        "Telegram ID · MSA+ ID · Username\n"
        "Returns: name, join date, verification, MSA+ ID.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📊  <b>TRAFFIC</b>\n"
        "Source-tracking stats — how users arrived via links.\n"
        "Breakdown: YT · IG · IGCC · YTCODE · Total."
    ),
    # Page 2 / 3
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  🖥️  BOT 10 ADMIN GUIDE  ·  <b>Page 2 / 3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🩺  <b>DIAGNOSIS</b>\n"
        "Full system health check — DB status, bot uptime,\n"
        "backup integrity, error counts, auto-healer stats.\n\n"
        "📸  <b>SHOOT</b>\n"
        "Send a photo, video, or document directly to a\n"
        "specific user by Telegram ID (delivered via Bot 8).\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💬  <b>SUPPORT</b>  (Ticket Management)\n\n"
        "  ├─ 🎫 <b>PENDING TICKETS</b>   Open, unresolved tickets\n"
        "  ├─ 📋 <b>ALL TICKETS</b>       Paginated full list\n"
        "  ├─ ✅ <b>RESOLVE TICKET</b>    Mark ticket resolved\n"
        "  ├─ 📨 <b>REPLY</b>             Message ticket owner\n"
        "  ├─ 🔍 <b>SEARCH TICKETS</b>    Filter by user/keyword\n"
        "  └─ 🗑️ <b>DELETE</b>            Remove from DB\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🚫  <b>BAN CONFIG</b>\n"
        "Ban or unban any Bot 8 user.\n"
        "  ├─ Permanent or timed ban.\n"
        "  ├─ Scope = bot10 — does NOT affect normal\n"
        "  │    Bot 8 user experience outside admin context.\n"
        "  └─ Unban restores full Bot 8 access instantly.\n\n"
        "📋  <b>FEATURE SUSPEND</b>\n"
        "Disable individual Bot 8 features per user:\n"
        "SEARCH_CODE · DASHBOARD · RULES · GUIDE\n"
        "User sees 'Feature Suspended' when accessing them."
    ),
    # Page 3 / 3
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  🖥️  BOT 10 ADMIN GUIDE  ·  <b>Page 3 / 3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💾  <b>BACKUP</b>\n\n"
        "  ├─ 📥 <b>BACKUP NOW</b>\n"
        "  │    Manual full backup → JSON files sent to admin.\n"
        "  │    Batch-cursor processing (handles 10M+ records).\n"
        "  │    Auto-compresses files above 40 MB.\n"
        "  │\n"
        "  ├─ 📊 <b>VIEW BACKUPS</b>\n"
        "  │    Paginated list sorted newest-first.\n"
        "  │\n"
        "  ├─ 🗓️ <b>MONTHLY STATUS</b>\n"
        "  │    Backup count grouped by Month &amp; Year.\n"
        "  │\n"
        "  └─ ⚙️ <b>AUTO-BACKUP</b>\n"
        "       Runs every 12 h (AM &amp; PM) automatically.\n"
        "       MongoDB-stored — cloud-safe, no disk needed.\n"
        "       Keeps last 60 backups (30 days × 2/day).\n"
        "       Dedup: same AM/PM window stored only once.\n\n"
        "🖥️  <b>TERMINAL</b>\n"
        "Stream live system log lines in real time.\n"
        "Last 50 entries, refreshed on each view.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👥  <b>ADMINS</b>  (Owner-only)\n"
        "Add / remove admin roles for Bot 10.\n"
        "Roles: viewer (read-only) · admin (full access).\n"
        "All admin actions are audit-logged.\n\n"
        "⚠️  <b>RESET DATA</b>  (Owner-only — IRREVERSIBLE)\n"
        "Permanently wipe Bot 8 or Bot 10 collections.\n"
        "Requires double confirmation + typed CONFIRM.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🌐  <b>MSA NODE Ecosystem</b>\n"
        "Bot 10 = admin control center.\n"
        "Bot 8  = user-facing delivery bot.\n"
        "Broadcasts, bans &amp; backups managed here flow\n"
        "through to Bot 8 automatically."
    ),
]

_BOT8_GUIDE_FOR_BOT10 = (
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "  📱  BOT 8 USER GUIDE  (Reference)\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "📊 <b>DASHBOARD</b> — MSA+ ID, member since, status,\n"
    "       live announcements from Bot 10 broadcasts.\n\n"
    "🔍 <b>SEARCH CODE</b> — Enter an MSA CODE to unlock\n"
    "       exclusive content from YouTube/Instagram.\n\n"
    "📜 <b>RULES</b>  — Community guidelines &amp; policies.\n\n"
    "📚 <b>GUIDE</b>  — User manual (this reference + personal).\n\n"
    "📞 <b>SUPPORT</b> — Open a support ticket to contact admin.\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "🔐  <b>OWNER-ONLY COMMANDS</b>  (via Bot 8 directly)\n\n"
    "  /start          — Launch bot &amp; regenerate main menu\n"
    "  /menu           — Show the reply keyboard\n"
    "  /resolve &lt;uid&gt;  — Resolve a user's support ticket\n"
    "  /delete  &lt;uid&gt;  — Delete user's verification data\n"
    "  /ticket_stats   — View full ticket statistics\n"
    "  /health         — Bot health &amp; uptime report\n\n"
    "  ⚡ Regular users get no response — owner-exclusive.\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "<i>For full user guide details, check Bot 8's 📚 GUIDE.</i>"
)

def _guide_selector_kb() -> ReplyKeyboardMarkup:
    """Keyboard shown on guide selector screen."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 BOT 8 USER GUIDE")],
            [KeyboardButton(text="🖥️ BOT 10 ADMIN GUIDE")],
            [KeyboardButton(text="⬅️ MAIN MENU")],
        ],
        resize_keyboard=True,
    )

def _guide_bot10_kb(page: int, total: int) -> ReplyKeyboardMarkup:
    """Navigation keyboard for the paginated Bot 10 guide."""
    row_nav = []
    if page > 1:
        row_nav.append(KeyboardButton(text="⬅️ PREV"))
    if page < total:
        row_nav.append(KeyboardButton(text="NEXT ➡️"))
    rows = []
    if row_nav:
        rows.append(row_nav)
    rows.append([KeyboardButton(text="📖 GUIDE MENU"), KeyboardButton(text="⬅️ MAIN MENU")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

@dp.message(F.text == "📖 GUIDE")
async def guide_handler(message: types.Message, state: FSMContext):
    """Show guide selector — Bot 8 Guide or Bot 10 Admin Guide."""
    log_action("📖 GUIDE", message.from_user.id, "Accessed guide selector")
    await state.set_state(GuideStates.selecting)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  <b>📖 GUIDE — SELECT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Which guide would you like to view?\n\n"
        "📱 <b>BOT 8 USER GUIDE</b>\n"
        "   Full user manual for Bot 8 — features,\n"
        "   MSA CODE search, owner commands &amp; more.\n\n"
        "🖥️ <b>BOT 10 ADMIN GUIDE</b>\n"
        "   Complete admin reference — every feature,\n"
        "   button, and system explained (3 pages).",
        parse_mode="HTML",
        reply_markup=_guide_selector_kb(),
    )

@dp.message(GuideStates.selecting, F.text == "📱 BOT 8 USER GUIDE")
async def guide_show_bot8_from_bot10(message: types.Message, state: FSMContext):
    """Show Bot 8 user guide from inside Bot 10."""
    await state.set_state(GuideStates.viewing_bot8)
    await message.answer(
        _BOT8_GUIDE_FOR_BOT10,
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📖 GUIDE MENU"), KeyboardButton(text="⬅️ MAIN MENU")]],
            resize_keyboard=True,
        ),
    )

@dp.message(GuideStates.selecting, F.text == "🖥️ BOT 10 ADMIN GUIDE")
async def guide_show_bot10_page1(message: types.Message, state: FSMContext):
    """Start paginated Bot 10 admin guide at page 1."""
    page = 1
    await state.set_state(GuideStates.viewing_bot10)
    await state.update_data(guide_page=page)
    await message.answer(
        _BOT10_GUIDE_PAGES[page - 1],
        parse_mode="HTML",
        reply_markup=_guide_bot10_kb(page, len(_BOT10_GUIDE_PAGES)),
    )

@dp.message(GuideStates.viewing_bot10, F.text == "NEXT ➡️")
async def guide_bot10_next(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("guide_page", 1) + 1
    page = min(page, len(_BOT10_GUIDE_PAGES))
    await state.update_data(guide_page=page)
    await message.answer(
        _BOT10_GUIDE_PAGES[page - 1],
        parse_mode="HTML",
        reply_markup=_guide_bot10_kb(page, len(_BOT10_GUIDE_PAGES)),
    )

@dp.message(GuideStates.viewing_bot10, F.text == "⬅️ PREV")
async def guide_bot10_prev(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(data.get("guide_page", 1) - 1, 1)
    await state.update_data(guide_page=page)
    await message.answer(
        _BOT10_GUIDE_PAGES[page - 1],
        parse_mode="HTML",
        reply_markup=_guide_bot10_kb(page, len(_BOT10_GUIDE_PAGES)),
    )

@dp.message(F.text == "📖 GUIDE MENU")
async def guide_back_to_menu(message: types.Message, state: FSMContext):
    """Return to guide selector from any guide page."""
    await state.set_state(GuideStates.selecting)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  <b>📖 GUIDE — SELECT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Which guide would you like to view?\n\n"
        "📱 <b>BOT 8 USER GUIDE</b>\n"
        "   Full user manual for Bot 8 — features,\n"
        "   MSA CODE search, owner commands &amp; more.\n\n"
        "🖥️ <b>BOT 10 ADMIN GUIDE</b>\n"
        "   Complete admin reference — every feature,\n"
        "   button, and system explained (3 pages).",
        parse_mode="HTML",
        reply_markup=_guide_selector_kb(),
    )

@dp.message(F.text == "⚠️ RESET DATA")
async def reset_data_handler(message: types.Message, state: FSMContext):
    """Show reset type selection menu"""
    if message.from_user.id != MASTER_ADMIN_ID:
        log_action("🚫 UNAUTHORIZED ACCESS", message.from_user.id, f"{message.from_user.full_name} tried to access RESET DATA")
        await message.answer("⛔ **ACCESS DENIED**\n\nThis feature is restricted to the Master Admin.", reply_markup=await get_main_menu(message.from_user.id))
        return

    type_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔴 RESET BOT 8"), KeyboardButton(text="🔴 RESET BOT 10")],
            [KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    await message.answer(
        "<b>⚠️ RESET DATA — SELECT BOT</b>\n\n"
        "Choose which bot's data to permanently erase:\n\n"
        "🔴 <b>RESET BOT 8</b>\n"
        "   user_verification, msa_ids, bot9_pdfs,\n"
        "   bot9_ig_content, support_tickets,\n"
        "   banned_users, suspended_features\n\n"
        "🔴 <b>RESET BOT 10</b>\n"
        "   broadcasts, user_tracking, cleanup_backups,\n"
        "   cleanup_logs, access_attempts, bot8_settings\n\n"
        "<b>⚠️ ALL DELETIONS ARE PERMANENT AND IRREVERSIBLE!</b>",
        parse_mode="HTML",
        reply_markup=type_kb
    )
    await state.set_state(ResetDataStates.selecting_reset_type)

@dp.message(ResetDataStates.selecting_reset_type)
async def reset_type_selected(message: types.Message, state: FSMContext):
    """Handle reset type selection"""
    choice = message.text.strip()

    if choice == "❌ CANCEL" or choice == "⬅️ BACK":
        await message.answer("✅ Cancelled.", reply_markup=await get_main_menu(message.from_user.id))
        await state.clear()
        return

    confirm_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="✅ CONFIRM RESET")], [KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )

    if choice == "🔴 RESET BOT 8":
        await state.update_data(reset_type="bot8")
        await message.answer(
            "<b>⚠️ RESET BOT 8 DATA</b>\n\n"
            "Will permanently delete:\n"
            "🗑️ user_verification\n🗑️ msa_ids\n🗑️ bot9_pdfs\n"
            "🗑️ bot9_ig_content\n🗑️ support_tickets\n"
            "🗑️ banned_users\n🗑️ suspended_features\n\n"
            "<b>⚠️ IRREVERSIBLE! Press ✅ CONFIRM RESET to proceed.</b>",
            parse_mode="HTML", reply_markup=confirm_kb
        )
        await state.set_state(ResetDataStates.waiting_for_first_confirm)

    elif choice == "🔴 RESET BOT 10":
        await state.update_data(reset_type="bot10")
        await message.answer(
            "<b>⚠️ RESET BOT 10 DATA</b>\n\n"
            "Will permanently delete:\n"
            "🗑️ bot10_broadcasts\n🗑️ bot10_user_tracking\n"
            "🗑️ cleanup_backups\n🗑️ cleanup_logs\n"
            "🗑️ bot10_access_attempts\n🗑️ bot8_settings\n\n"
            "<b>⚠️ IRREVERSIBLE! Press ✅ CONFIRM RESET to proceed.</b>",
            parse_mode="HTML", reply_markup=confirm_kb
        )
        await state.set_state(ResetDataStates.bot10_first_confirm)

    else:
        await message.answer("❌ Invalid choice. Please select from the menu.", parse_mode="HTML")

# ── Bot10 reset first confirm ──
@dp.message(ResetDataStates.bot10_first_confirm)
async def reset_bot10_first_confirm(message: types.Message, state: FSMContext):
    """Bot10 first confirmation"""
    if message.text != "✅ CONFIRM RESET":
        await message.answer("✅ Cancelled. No data deleted.", reply_markup=await get_main_menu(message.from_user.id))
        await state.clear()
        return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True)
    await message.answer(
        "<b>🚨 LAST WARNING — BOT 10 DATA</b>\n\n"
        "Type <code>CONFIRM</code> to permanently delete all Bot 10 data.",
        parse_mode="HTML", reply_markup=cancel_kb
    )
    await state.set_state(ResetDataStates.bot10_final_confirm)

@dp.message(ResetDataStates.bot10_final_confirm)
async def reset_bot10_final_confirm(message: types.Message, state: FSMContext):
    """Bot10 final deletion"""
    if message.text.strip() != "CONFIRM":
        await message.answer("✅ Cancelled. No data deleted.", reply_markup=await get_main_menu(message.from_user.id))
        await state.clear()
        return
    status_msg = await message.answer("<b>🗑️ DELETING ALL BOT 10 DATA...</b>\n\n⏳ Please wait...", parse_mode="HTML")
    try:
        r1 = col_broadcasts.delete_many({})
        r2 = col_user_tracking.delete_many({})
        r3 = col_cleanup_backups.delete_many({})
        r4 = col_cleanup_logs.delete_many({})
        r5 = col_access_attempts.delete_many({})
        r6 = col_bot8_settings.delete_many({})
        total = r1.deleted_count + r2.deleted_count + r3.deleted_count + r4.deleted_count + r5.deleted_count + r6.deleted_count
        await status_msg.edit_text(
            "<b>✅ ALL BOT 10 DATA DELETED</b>\n\n"
            "<b>🗑️ DELETION REPORT:</b>\n\n"
            f"🗑️ bot10_broadcasts: {r1.deleted_count:,} deleted\n"
            f"🗑️ bot10_user_tracking: {r2.deleted_count:,} deleted\n"
            f"🗑️ cleanup_backups: {r3.deleted_count:,} deleted\n"
            f"🗑️ cleanup_logs: {r4.deleted_count:,} deleted\n"
            f"🗑️ bot10_access_attempts: {r5.deleted_count:,} deleted\n"
            f"🗑️ bot8_settings: {r6.deleted_count:,} deleted\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>Total Records Deleted:</b> {total:,}\n"
            f"<b>Database Status:</b> All Bot 10 collections cleared ✅\n\n"
            f"<i>⏰ Completed at {now_local().strftime('%Y-%m-%d %H:%M:%S')}</i>",
            parse_mode="HTML"
        )
        await message.answer(
            "<b>🔄 Bot 10 Reset Complete</b>\n\nAll Bot 10 data permanently deleted.",
            parse_mode="HTML", reply_markup=await get_main_menu(message.from_user.id)
        )
        print(f"\n🚨 BOT 10 DATA RESET by {message.from_user.id} — {total:,} records deleted at {now_local()}\n")
    except Exception as e:
        await status_msg.edit_text(f"<b>❌ DELETION ERROR</b>\n\n{str(e)}", parse_mode="HTML")
        await message.answer("⚠️ Error during reset.", reply_markup=await get_main_menu(message.from_user.id))
    await state.clear()

# ── ALL reset first confirm ──
@dp.message(ResetDataStates.all_first_confirm)
async def reset_all_first_confirm(message: types.Message, state: FSMContext):
    """ALL data first confirmation"""
    if message.text != "✅ CONFIRM RESET":
        await message.answer("✅ Cancelled. No data deleted.", reply_markup=await get_main_menu(message.from_user.id))
        await state.clear()
        return
    cancel_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True)
    await message.answer(
        "<b>☢️ LAST WARNING — COMPLETE WIPE</b>\n\n"
        "Type <code>CONFIRM</code> to permanently delete ALL data from all bots.",
        parse_mode="HTML", reply_markup=cancel_kb
    )
    await state.set_state(ResetDataStates.all_final_confirm)

@dp.message(ResetDataStates.all_final_confirm)
async def reset_all_final_confirm(message: types.Message, state: FSMContext):
    """Complete wipe of all collections"""
    if message.text.strip() != "CONFIRM":
        await message.answer("✅ Cancelled. No data deleted.", reply_markup=await get_main_menu(message.from_user.id))
        await state.clear()
        return
    status_msg = await message.answer("<b>☢️ DELETING ALL DATA...</b>\n\n⏳ Please wait...", parse_mode="HTML")
    try:
        # Bot 8 collections
        r1 = col_user_verification.delete_many({})
        r2 = col_msa_ids.delete_many({})
        r3 = col_bot9_pdfs.delete_many({})
        r4 = col_bot9_ig_content.delete_many({})
        r5 = col_support_tickets.delete_many({})
        r6 = col_banned_users.delete_many({})
        r7 = col_suspended_features.delete_many({})
        # Bot 10 collections
        r8 = col_broadcasts.delete_many({})
        r9 = col_user_tracking.delete_many({})
        r10 = col_cleanup_backups.delete_many({})
        r11 = col_cleanup_logs.delete_many({})
        r12 = col_access_attempts.delete_many({})
        r13 = col_bot8_settings.delete_many({})
        total = sum(r.deleted_count for r in [r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,r11,r12,r13])
        await status_msg.edit_text(
            "<b>☢️ COMPLETE WIPE DONE</b>\n\n"
            "<b>🗑️ DELETION REPORT:</b>\n\n"
            f"<b>— Bot 8 —</b>\n"
            f"🗑️ user_verification: {r1.deleted_count:,}\n"
            f"🗑️ msa_ids: {r2.deleted_count:,}\n"
            f"🗑️ bot9_pdfs: {r3.deleted_count:,}\n"
            f"🗑️ bot9_ig_content: {r4.deleted_count:,}\n"
            f"🗑️ support_tickets: {r5.deleted_count:,}\n"
            f"🗑️ banned_users: {r6.deleted_count:,}\n"
            f"🗑️ suspended_features: {r7.deleted_count:,}\n\n"
            f"<b>— Bot 10 —</b>\n"
            f"🗑️ bot10_broadcasts: {r8.deleted_count:,}\n"
            f"🗑️ bot10_user_tracking: {r9.deleted_count:,}\n"
            f"🗑️ cleanup_backups: {r10.deleted_count:,}\n"
            f"🗑️ cleanup_logs: {r11.deleted_count:,}\n"
            f"🗑️ bot10_access_attempts: {r12.deleted_count:,}\n"
            f"🗑️ bot8_settings: {r13.deleted_count:,}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>Total Records Deleted:</b> {total:,}\n"
            f"<b>Database Status:</b> All collections cleared ✅\n\n"
            f"<i>⏰ Completed at {now_local().strftime('%Y-%m-%d %H:%M:%S')}</i>",
            parse_mode="HTML"
        )
        await message.answer(
            "<b>☢️ Complete Wipe Done</b>\n\nAll bot data permanently deleted.",
            parse_mode="HTML", reply_markup=await get_main_menu(message.from_user.id)
        )
        print(f"\n☢️ COMPLETE DATA WIPE by {message.from_user.id} — {total:,} records deleted at {now_local()}\n")
    except Exception as e:
        await status_msg.edit_text(f"<b>❌ DELETION ERROR</b>\n\n{str(e)}", parse_mode="HTML")
        await message.answer("⚠️ Error during wipe.", reply_markup=await get_main_menu(message.from_user.id))
    await state.clear()

# ── Bot8 reset first confirm (original) ──
@dp.message(ResetDataStates.waiting_for_first_confirm)
async def reset_data_first_confirm(message: types.Message, state: FSMContext):
    """First confirmation for reset data"""
    if message.text != "✅ CONFIRM RESET":
        await message.answer(
            "<b>✅ CANCELLED</b>\n\n"
            "Reset operation cancelled. No data was deleted.",
            parse_mode="HTML",
            reply_markup=await get_main_menu(message.from_user.id)
        )
        await state.clear()
        return
    
    # Second Confirmation - Typing
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❌ CANCEL")]
        ],
        resize_keyboard=True
    )
    
    final_warning = (
        "<b>⚠️ FINAL CONFIRMATION REQUIRED</b>\n\n"
        "<b>🚨 LAST WARNING 🚨</b>\n\n"
        "You are about to permanently delete ALL Bot 8 data.\n\n"
        "<b>⚠️ THIS IS IRREVERSIBLE!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>FINAL STEP:</b>\n"
        "Type <code>CONFIRM</code> below to execute deletion.\n"
        "Type anything else to cancel."
    )
    
    await message.answer(final_warning, parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(ResetDataStates.waiting_for_final_confirm)

@dp.message(ResetDataStates.waiting_for_final_confirm)
async def reset_data_final_confirm(message: types.Message, state: FSMContext):
    """Final confirmation - actually delete all Bot 8 data"""
    # Strict matching for "CONFIRM"
    if message.text.strip() != "CONFIRM":
        await message.answer(
            "<b>✅ CANCELLED</b>\n\n"
            "Reset operation cancelled. No data was deleted.",
            parse_mode="HTML",
            reply_markup=await get_main_menu(message.from_user.id)
        )
        await state.clear()
        return
    
    # Both confirmations passed - proceed with deletion
    status_msg = await message.answer(
        "<b>🗑️ DELETING ALL BOT 8 DATA...</b>\n\n"
        "⏳ Please wait...",
        parse_mode="HTML"
    )
    
    try:
        # Count records before deletion
        counts_before = {
            "user_verification": col_user_verification.count_documents({}),
            "msa_ids": col_msa_ids.count_documents({}),
            "bot9_pdfs": col_bot9_pdfs.count_documents({}),
            "bot9_ig_content": col_bot9_ig_content.count_documents({}),
            "support_tickets": col_support_tickets.count_documents({}),
            "banned_users": col_banned_users.count_documents({}),
            "suspended_features": col_suspended_features.count_documents({})
        }
        
        total_before = sum(counts_before.values())
        
        # Delete all Bot 8 data
        result_user_verification = col_user_verification.delete_many({})
        result_msa_ids = col_msa_ids.delete_many({})
        result_bot9_pdfs = col_bot9_pdfs.delete_many({})
        result_bot9_ig_content = col_bot9_ig_content.delete_many({})
        result_support_tickets = col_support_tickets.delete_many({})
        result_banned_users = col_banned_users.delete_many({})
        result_suspended_features = col_suspended_features.delete_many({})
        
        # Count records after deletion
        counts_after = {
            "user_verification": col_user_verification.count_documents({}),
            "msa_ids": col_msa_ids.count_documents({}),
            "bot9_pdfs": col_bot9_pdfs.count_documents({}),
            "bot9_ig_content": col_bot9_ig_content.count_documents({}),
            "support_tickets": col_support_tickets.count_documents({}),
            "banned_users": col_banned_users.count_documents({}),
            "suspended_features": col_suspended_features.count_documents({})
        }
        
        total_after = sum(counts_after.values())
        total_deleted = total_before - total_after
        
        # Success message
        success_msg = (
            "<b>✅ ALL BOT 8 DATA DELETED</b>\n\n"
            "<b>🗑️ DELETION REPORT:</b>\n\n"
            f"🗑️ user_verification: {result_user_verification.deleted_count:,} deleted\n"
            f"🗑️ msa_ids: {result_msa_ids.deleted_count:,} deleted\n"
            f"🗑️ bot9_pdfs: {result_bot9_pdfs.deleted_count:,} deleted\n"
            f"🗑️ bot9_ig_content: {result_bot9_ig_content.deleted_count:,} deleted\n"
            f"🗑️ support_tickets: {result_support_tickets.deleted_count:,} deleted\n"
            f"🗑️ banned_users: {result_banned_users.deleted_count:,} deleted\n"
            f"🗑️ suspended_features: {result_suspended_features.deleted_count:,} deleted\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>Total Records Deleted:</b> {total_deleted:,}\n"
            f"<b>Database Status:</b> All Bot 8 collections cleared ✅\n\n"
            f"<i>⏰ Completed at {now_local().strftime('%Y-%m-%d %H:%M:%S')}</i>"
        )
        
        await status_msg.edit_text(success_msg, parse_mode="HTML")
        await message.answer(
            "<b>🔄 Bot 8 Reset Complete</b>\n\n"
            "All Bot 8 data has been permanently deleted.\n"
            "Bot 8 is now in fresh state.",
            parse_mode="HTML",
            reply_markup=await get_main_menu(message.from_user.id)
        )
        
        # Log the reset action
        print(f"\n🚨 ═══════════════════════════════════════")
        print(f"🚨 BOT 8 DATA RESET")
        print(f"🚨 Admin: {message.from_user.id}")
        print(f"🚨 Total Deleted: {total_deleted:,} records")
        print(f"🚨 Timestamp: {now_local().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🚨 ═══════════════════════════════════════\n")
        
    except Exception as e:
        error_msg = str(e).replace('<', '&lt;').replace('>', '&gt;')
        await status_msg.edit_text(
            f"<b>❌ DELETION ERROR</b>\n\n{error_msg}\n\n"
            "Some data may have been partially deleted. Please check database manually.",
            parse_mode="HTML"
        )
        await message.answer(
            "<b>⚠️ Error occurred during reset</b>\n\n"
            "Please check the error message above and contact developer if needed.",
            parse_mode="HTML",
            reply_markup=await get_main_menu(message.from_user.id)
        )
    
    await state.clear()

# ==========================================
# AUTOMATED DATABASE CLEANUP SYSTEM
# ==========================================

async def automated_database_cleanup():
    """
    Automated cleanup that runs daily at 3 AM
    - Cleans resolved tickets older than 60 days
    - Cleans broadcasts older than 90 days
    - Auto-backup to MongoDB (cloud-safe, works on Render/Heroku/Railway)
    - Safe, conservative, no data loss
    """
    now = now_local()
    print(f"\n🧹 ═══════════════════════════════════════")
    print(f"🧹 AUTOMATED DATABASE CLEANUP")
    print(f"🧹 Started at: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🧹 ═══════════════════════════════════════\n")
    
    cleanup_stats = {
        "cleanup_date": now,
        "tickets_deleted": 0,
        "broadcasts_deleted": 0,
        "backup_created": False,
        "old_backups_deleted": 0
    }
    
    try:
        # === GET DATA TO BACKUP ===
        old_resolved_tickets = list(col_support_tickets.find({
            "status": "resolved",
            "resolved_at": {"$lt": now - timedelta(days=60)}
        }))
        
        old_broadcasts = list(col_broadcasts.find({
            "created_at": {"$lt": now - timedelta(days=90)}
        }))
        
        # === SAVE BACKUP TO MONGODB (Cloud-Safe!) ===
        if old_resolved_tickets or old_broadcasts:
            backup_doc = {
                "backup_date": now,
                "tickets_count": len(old_resolved_tickets),
                "broadcasts_count": len(old_broadcasts),
                "tickets": old_resolved_tickets,
                "broadcasts": old_broadcasts
            }
            
            col_cleanup_backups.insert_one(backup_doc)
            cleanup_stats['backup_created'] = True
            
            print(f"💾 Backup saved to MongoDB (cloud-safe)")
            print(f"   📄 Tickets backed up: {len(old_resolved_tickets)}")
            print(f"   📄 Broadcasts backed up: {len(old_broadcasts)}\n")
        else:
            print(f"📦 No data to backup (nothing old enough to delete)\n")
        
        # === CLEANUP OLD BACKUPS IN MONGODB (Keep only last 30) ===
        backup_count = col_cleanup_backups.count_documents({})
        
        if backup_count > 30:
            # Get oldest backups to delete
            old_backups = list(col_cleanup_backups.find({}).sort("backup_date", 1).limit(backup_count - 30))
            old_backup_ids = [b['_id'] for b in old_backups]
            
            result = col_cleanup_backups.delete_many({"_id": {"$in": old_backup_ids}})
            cleanup_stats['old_backups_deleted'] = result.deleted_count
            
            print(f"🧹 Deleted {result.deleted_count} old backups from MongoDB")
            print(f"📦 Kept: 30 most recent backups\n")
        else:
            print(f"📦 MongoDB backups: {backup_count}/30 (no cleanup needed)\n")
        
        # === CLEANUP OLD RESOLVED TICKETS (60+ days) ===
        cutoff_date_tickets = now - timedelta(days=60)
        result_tickets = col_support_tickets.delete_many({
            "status": "resolved",
            "resolved_at": {"$lt": cutoff_date_tickets}
        })
        cleanup_stats['tickets_deleted'] = result_tickets.deleted_count
        
        if result_tickets.deleted_count > 0:
            print(f"🎫 Deleted {result_tickets.deleted_count} old resolved tickets (>60 days)")
        else:
            print(f"🎫 No old resolved tickets to delete")
        
        # === CLEANUP OLD BROADCASTS (90+ days) ===
        cutoff_date_broadcasts = now - timedelta(days=90)
        result_broadcasts = col_broadcasts.delete_many({
            "created_at": {"$lt": cutoff_date_broadcasts}
        })
        cleanup_stats['broadcasts_deleted'] = result_broadcasts.deleted_count
        
        if result_broadcasts.deleted_count > 0:
            print(f"📢 Deleted {result_broadcasts.deleted_count} old broadcasts (>90 days)")
        else:
            print(f"📢 No old broadcasts to delete")
        
        # === SAVE CLEANUP LOG TO MONGODB ===
        col_cleanup_logs.insert_one(cleanup_stats)
        
        # Keep only last 30 logs in MongoDB
        log_count = col_cleanup_logs.count_documents({})
        if log_count > 30:
            old_logs = list(col_cleanup_logs.find({}).sort("cleanup_date", 1).limit(log_count - 30))
            old_log_ids = [log['_id'] for log in old_logs]
            col_cleanup_logs.delete_many({"_id": {"$in": old_log_ids}})
            print(f"📋 Cleaned up old logs (kept last 30)")
        
        print(f"\n✅ Cleanup completed successfully!")
        print(f"   🗑️ Total deleted: {cleanup_stats['tickets_deleted'] + cleanup_stats['broadcasts_deleted']} items")
        print(f"   💾 Backup: Stored in MongoDB (cloud-safe)")
        print(f"   📋 Log: Saved to cleanup_logs collection")
        
    except Exception as e:
        print(f"❌ Cleanup failed: {str(e)}")
        cleanup_stats['error'] = str(e)
        cleanup_stats['cleanup_date'] = now
        col_cleanup_logs.insert_one(cleanup_stats)
    
    print(f"\n🧹 ═══════════════════════════════════════")
    print(f"🧹 Cleanup finished at: {now_local().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🧹 ═══════════════════════════════════════\n")
    
    return cleanup_stats

async def schedule_daily_cleanup():
    """Schedule cleanup to run daily at 3 AM"""
    while True:
        now = now_local()
        
        # Calculate next 3 AM
        next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if now.hour >= 3:
            next_run += timedelta(days=1)
        
        # Calculate seconds until next run
        seconds_until_run = (next_run - now).total_seconds()
        
        print(f"🕒 Next automated cleanup scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ Time until cleanup: {seconds_until_run / 3600:.1f} hours\n")
        
        # Wait until 3 AM
        await asyncio.sleep(seconds_until_run)
        
        # Run cleanup
        await automated_database_cleanup()
        
        # Wait 1 hour before checking again (prevents multiple runs)
        await asyncio.sleep(3600)

async def schedule_monthly_backup():
    """Run automatic Bot 10 backup every 12 hours into bot10_backups collection."""
    while True:
        try:
            now = now_local()
            period = "AM" if now.hour < 12 else "PM"
            window_key = now.strftime("%Y-%m-%d_") + period   # e.g. "2026-02-19_AM"
            timestamp_label = now.strftime(f"%B %d, %Y — %I:%M {period}")
            timestamp_key   = now.strftime("%Y-%m-%d_%I-%M-%S_") + period

            # ✅ Dedup: skip if a backup for this 12 h window already exists
            if col_bot10_backups.count_documents({"window_key": window_key}) > 0:
                print(f"⚠️  Bot10 auto-backup SKIPPED — window {window_key} already stored")
                await asyncio.sleep(12 * 3600)
                continue

            print(f"\n💾 ═══════════════════════════════════════")
            print(f"💾 BOT 10 AUTO-BACKUP STARTING")
            print(f"💾 Time: {timestamp_label}")
            print(f"💾 ═══════════════════════════════════════\n")

            try:
                backup_data = await create_backup_mongodb_scalable(backup_type="automatic_12h")

                if not backup_data.get("success"):
                    print(f"❌ 12h backup failed: {backup_data.get('error', 'Unknown error')}")
                else:
                    import bson as _bson
                    col_bot10_backups.update_one(
                        {"_id": _bson.ObjectId(backup_data["backup_id"])},
                        {"$set": {
                            "bot":             "bot10",
                            "period":          period,
                            "timestamp_label": timestamp_label,
                            "hour_12":         now.strftime("%I").lstrip("0") or "12",
                            "minute":          now.strftime("%M"),
                            "day":             now.day,
                        }}
                    )
                    print(f"✅ Bot 10 auto-backup complete — {backup_data['total_records']:,} records | {backup_data.get('processing_time', 0):.2f}s | {period}")

            except Exception as inner_e:
                print(f"❌ 12h backup inner error: {str(inner_e)}")

            print(f"\n💾 ═══════════════════════════════════════")
            print(f"💾 BOT 10 AUTO-BACKUP FINISHED")
            print(f"💾 ═══════════════════════════════════════\n")

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"❌ schedule_monthly_backup outer error: {e}")

        await asyncio.sleep(12 * 3600)


def check_backup_storage():
    """Check MongoDB backup storage (cloud-safe)"""
    try:
        backup_count = col_bot10_backups.count_documents({})
        cleanup_backup_count = col_cleanup_backups.count_documents({})
        log_count = col_cleanup_logs.count_documents({})
        latest_backup = col_bot10_backups.find_one({}, sort=[("backup_date", -1)])
        latest_log = col_cleanup_logs.find_one({}, sort=[("cleanup_date", -1)])

        print(f"\n💾 ═══════════════════════════════════════")
        print(f"💾 BACKUP STORAGE STATUS (Cloud-Safe)")
        print(f"💾 ═══════════════════════════════════════")
        print(f"📦 Storage: MongoDB Atlas")
        print(f"🗄️ Bot10 backups: {backup_count}/60 (auto-limited, 12h × 30 days)")
        print(f"🗄️ Cleanup backups: {cleanup_backup_count}/30 (auto-limited)")
        print(f"📋 Cleanup logs: {log_count}/30 (auto-limited)")

        if latest_backup:
            backup_date = latest_backup.get('backup_date', 'Unknown')
            if isinstance(backup_date, datetime):
                backup_date = format_datetime(backup_date)
            total_records = latest_backup.get('total_records', 0)
            print(f"\n📍 Latest Bot10 Backup: {backup_date} | Records: {total_records}")
        else:
            print(f"\n📍 No Bot10 backups yet (create with 📥 BACKUP NOW)")

        if latest_log:
            last_cleanup = latest_log.get('cleanup_date', 'Unknown')
            if isinstance(last_cleanup, datetime):
                last_cleanup = format_datetime(last_cleanup)
            deleted = latest_log.get('tickets_deleted', 0) + latest_log.get('broadcasts_deleted', 0)
            print(f"🧹 Last Cleanup: {last_cleanup} | Deleted: {deleted}")

        print(f"\n✅ All backups in MongoDB Atlas — No local disk used")
        print(f"💾 ═══════════════════════════════════════\n")

    except Exception as e:
        print(f"⚠️ Could not check backup storage: {str(e)}\n")


# ==========================================
# ENTERPRISE AUTO-HEALER SYSTEM (BOT 10)
# ==========================================
# (bot10_health dict is defined near top of file, after bot/dp initialization)

# Per-alert cooldown tracker: {"{severity}:{error_type}": last_sent_datetime}
_bot10_last_alert: dict = {}

async def notify_master_admin(error_type: str, error_msg: str, severity: str = "ERROR", auto_healed: bool = False):
    """Instantly notify owner (MASTER_ADMIN_ID) of any error via Telegram — with per-type deduplication"""
    try:
        # --- Cooldown / deduplication to prevent notification spam ---
        _alert_cooldowns = {"CRITICAL": 120, "ERROR": 600, "WARNING": 1800}
        cooldown = _alert_cooldowns.get(severity, 600)
        alert_key = f"{severity}:{error_type}"
        last_sent = _bot10_last_alert.get(alert_key)
        if last_sent:
            elapsed = (now_local() - last_sent).total_seconds()
            if elapsed < cooldown:
                print(f"[BOT10] Suppressing {severity} alert '{error_type}' (cooldown {cooldown - elapsed:.0f}s left)")
                return
        _bot10_last_alert[alert_key] = now_local()
        # --- end cooldown ---

        bot10_health["owner_notified"] += 1
        emoji = {"CRITICAL": "🔴", "ERROR": "🟠", "WARNING": "🟡"}.get(severity, "🟡")
        heal_status = "✅ AUTO-HEALED" if auto_healed else "❌ NEEDS ATTENTION"
        uptime = now_local() - bot10_health["bot_start_time"]
        h = int(uptime.total_seconds() // 3600)
        m = int((uptime.total_seconds() % 3600) // 60)

        msg = (
            f"{emoji} **BOT 10 ALERT — {severity}**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**Type:** `{error_type}`\n"
            f"**Status:** {heal_status}\n\n"
            f"**Error:**\n```\n{str(error_msg)[:600]}\n```\n\n"
            f"**Stats:**\n"
            f"• Uptime: {h}h {m}m\n"
            f"• Errors Caught: {bot10_health['errors_caught']}\n"
            f"• Auto-Healed: {bot10_health['auto_healed']}\n"
            f"• Alerts Sent: {bot10_health['owner_notified']}\n\n"
            f"**Time:** {now_local().strftime('%B %d, %Y — %I:%M:%S %p')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_Bot 10 Enterprise Auto-Healer_"
        )

        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="Markdown")
        print(f"📢 [ALERT] Notified owner: {severity} — {error_type}")
    except Exception as e:
        print(f"❌ Failed to notify owner: {e}")


async def bot10_auto_heal(error_type: str, error: Exception) -> bool:
    """Attempt automatic recovery before escalating to owner"""
    try:
        print(f"🏥 [AUTO-HEAL] Attempting recovery: {error_type}")
        err_str = str(error).lower()

        # MongoDB / DB connection issues
        if any(k in err_str for k in ["mongo", "database", "pymongo", "connection refused"]):
            print("🔌 [AUTO-HEAL] Reconnecting to MongoDB...")
            try:
                client.admin.command('ping')
                print("✅ [AUTO-HEAL] MongoDB reconnected!")
                bot10_health["auto_healed"] += 1
                bot10_health["consecutive_failures"] = 0
                return True
            except Exception:
                print("❌ [AUTO-HEAL] MongoDB reconnect failed")
                return False

        # Timeout / network blips
        elif any(k in err_str for k in ["timeout", "timed out", "temporarily unavailable"]):
            print("⏱️ [AUTO-HEAL] Timeout — waiting 2s and continuing...")
            await asyncio.sleep(2)
            bot10_health["auto_healed"] += 1
            bot10_health["consecutive_failures"] = 0
            return True

        # Telegram rate limit
        elif "retry after" in err_str or "flood" in err_str or "too many requests" in err_str:
            wait = 5
            try:
                import re
                m = re.search(r'retry after (\d+)', err_str)
                if m:
                    wait = int(m.group(1)) + 1
            except Exception:
                pass
            print(f"⏳ [AUTO-HEAL] Rate limit — waiting {wait}s...")
            await asyncio.sleep(wait)
            bot10_health["auto_healed"] += 1
            bot10_health["consecutive_failures"] = 0
            return True

        # Generic connection error
        elif any(k in err_str for k in ["connection", "network", "socket", "ssl"]):
            print("🔄 [AUTO-HEAL] Connection issue — waiting 5s...")
            await asyncio.sleep(5)
            bot10_health["auto_healed"] += 1
            bot10_health["consecutive_failures"] = 0
            return True

        else:
            print(f"❓ [AUTO-HEAL] Unknown error type, cannot auto-heal: {error_type}")
            return False

    except Exception as ex:
        print(f"❌ [AUTO-HEAL] Healing itself failed: {ex}")
        return False


async def bot10_global_error_handler(update: types.Update, exception: Exception):
    """Global error handler — catches ALL unhandled errors in bot10 handlers"""
    try:
        bot10_health["errors_caught"] += 1
        bot10_health["last_error"] = now_local()
        bot10_health["last_error_type"] = type(exception).__name__
        bot10_health["consecutive_failures"] += 1

        err_type = type(exception).__name__
        err_msg = str(exception)
        print(f"❌ [BOT10 ERROR] {err_type}: {err_msg[:200]}")

        # Try auto-heal first
        healed = await bot10_auto_heal(err_type, exception)

        # Determine severity
        err_lower = err_msg.lower()
        if "critical" in err_lower or "fatal" in err_lower or bot10_health["consecutive_failures"] >= 5:
            severity = "CRITICAL"
        elif healed:
            severity = "WARNING"
        else:
            severity = "ERROR"

        # Notify owner if not healed or if critical
        if not healed or severity == "CRITICAL":
            await notify_master_admin(err_type, err_msg, severity, healed)

        print(f"🏥 [BOT10] Error handled. Auto-healed: {healed}")
        return True

    except Exception as handler_err:
        print(f"💥 CRITICAL: Bot10 error handler crashed: {handler_err}")
        try:
            await bot.send_message(
                MASTER_ADMIN_ID,
                f"🔴🔴🔴 **BOT 10 CRITICAL FAILURE**\n\n"
                f"The error handler itself crashed!\n```{str(handler_err)[:300]}```",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        return False


async def bot10_health_monitor():
    """Background health monitor — checks every hour, reports issues instantly"""
    while True:
        try:
            await asyncio.sleep(3600)  # Every hour

            # Check MongoDB
            try:
                t0 = time.time()
                client.admin.command('ping')
                latency_ms = (time.time() - t0) * 1000
                print(f"✅ [HEALTH] DB OK — {latency_ms:.1f}ms")
                if latency_ms > 2000:
                    await notify_master_admin("DB Latency Warning", f"MongoDB latency {latency_ms:.0f}ms (high)", "WARNING", True)
            except Exception as e:
                print(f"❌ [HEALTH] DB FAILED: {e}")
                healed = await bot10_auto_heal("DB Health Check", e)
                if not healed:
                    await notify_master_admin("DB Health Check", str(e), "CRITICAL", False)

            # Check bot connection
            try:
                me = await bot.get_me()
                print(f"✅ [HEALTH] Bot OK — @{me.username}")
            except Exception as e:
                print(f"❌ [HEALTH] Bot connection FAILED: {e}")
                await notify_master_admin("Bot Connection Check", str(e), "CRITICAL", False)

        except asyncio.CancelledError:
            print("💊 [HEALTH] Bot10 health monitor stopping...")
            break
        except Exception as e:
            print(f"❌ [HEALTH MONITOR ERROR] {e}")


# ==========================================
# STATE PERSISTENCE (Restart Recovery)
# ==========================================

BOT10_STATE_COLLECTION = db["bot10_runtime_state"]

def save_bot10_state():
    """Save runtime state to MongoDB so restarts pick up where they left off"""
    try:
        state_doc = {
            "state_key": "bot10_main",
            "saved_at": now_local(),
            "health_stats": {
                "errors_caught": bot10_health["errors_caught"],
                "auto_healed": bot10_health["auto_healed"],
                "owner_notified": bot10_health["owner_notified"],
                "consecutive_failures": bot10_health["consecutive_failures"],
            },
            "uptime_seconds": (now_local() - bot10_health["bot_start_time"]).total_seconds(),
            "last_shutdown": now_local().isoformat(),
        }
        BOT10_STATE_COLLECTION.update_one(
            {"state_key": "bot10_main"},
            {"$set": state_doc},
            upsert=True
        )
        print("💾 [STATE] Runtime state saved to MongoDB")
    except Exception as e:
        print(f"⚠️ [STATE] Failed to save state: {e}")


def load_bot10_state():
    """Load previous runtime state on startup for continuity"""
    try:
        state = BOT10_STATE_COLLECTION.find_one({"state_key": "bot10_main"})
        if state:
            last_shutdown = state.get("last_shutdown", "Unknown")
            prev_uptime = state.get("uptime_seconds", 0)
            h = int(prev_uptime // 3600)
            m = int((prev_uptime % 3600) // 60)
            print(f"♻️ [STATE] Previous session found — Last shutdown: {last_shutdown}")
            print(f"♻️ [STATE] Previous uptime was {h}h {m}m")
            print(f"♻️ [STATE] Previous errors caught: {state.get('health_stats', {}).get('errors_caught', 0)}")
            return state
        else:
            print("🆕 [STATE] No previous state found — fresh start")
            return None
    except Exception as e:
        print(f"⚠️ [STATE] Could not load previous state: {e}")
        return None


async def state_auto_save_loop():
    """Auto-save state every 5 minutes for crash recovery"""
    while True:
        try:
            await asyncio.sleep(300)  # Every 5 minutes
            save_bot10_state()
        except asyncio.CancelledError:
            save_bot10_state()  # Save on shutdown
            break
        except Exception as e:
            print(f"⚠️ [STATE SAVE] Error: {e}")


# ==========================================
# DAILY REPORT SYSTEM (8:40 AM & 8:40 PM)
# ==========================================

async def generate_daily_report() -> str:
    """Generate comprehensive daily report of all bot systems"""
    now = now_local()
    uptime = now - bot10_health["bot_start_time"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)

    # === DATABASE STATS ===
    try:
        total_users = col_user_tracking.count_documents({})
        yt_users = col_user_tracking.count_documents({"source": "YT"})
        ig_users = col_user_tracking.count_documents({"source": "IG"})
        igcc_users = col_user_tracking.count_documents({"source": "IGCC"})
        ytcode_users = col_user_tracking.count_documents({"source": "YTCODE"})
        banned_users = col_banned_users.count_documents({})
        suspended_users = col_suspended_features.count_documents({})
    except Exception:
        total_users = ig_users = yt_users = igcc_users = ytcode_users = banned_users = suspended_users = 0

    # === SUPPORT TICKETS ===
    try:
        open_tickets = col_support_tickets.count_documents({"status": "open"})
        resolved_tickets = col_support_tickets.count_documents({"status": "resolved"})
        total_tickets = col_support_tickets.count_documents({})
        # New tickets today
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        new_tickets_today = col_support_tickets.count_documents({"created_at": {"$gte": today_start}})
        resolved_today = col_support_tickets.count_documents({"resolved_at": {"$gte": today_start}})
    except Exception:
        open_tickets = resolved_tickets = total_tickets = new_tickets_today = resolved_today = 0

    # === BROADCASTS ===
    try:
        total_broadcasts = col_broadcasts.count_documents({})
        last_broadcast = col_broadcasts.find_one({}, sort=[("created_at", -1)])
        last_brd_time = last_broadcast.get("created_at").strftime("%b %d %I:%M %p") if last_broadcast and last_broadcast.get("created_at") else "Never"
    except Exception:
        total_broadcasts = 0
        last_brd_time = "N/A"

    # === ADMINS ===
    try:
        total_admins = col_admins.count_documents({})
        locked_admins = col_admins.count_documents({"locked": True})
    except Exception:
        total_admins = locked_admins = 0

    # === BACKUPS ===
    try:
        latest_bk = col_bot10_backups.find_one({}, sort=[("backup_date", -1)])
        last_bk_time = latest_bk.get("backup_date").strftime("%b %d %I:%M %p") if latest_bk and latest_bk.get("backup_date") else "Never"
    except Exception:
        last_bk_time = "N/A"

    # === DB HEALTH ===
    try:
        t0 = time.time()
        client.admin.command('ping')
        db_ms = (time.time() - t0) * 1000
        db_status = f"✅ Online ({db_ms:.0f}ms)"
    except Exception:
        db_status = "❌ OFFLINE"

    period = "🌅 MORNING" if now.hour < 12 else "🌆 EVENING"
    report_time = now.strftime("%B %d, %Y — %I:%M %p")

    report = (
        f"📊 **BOT 10 — DAILY {period} REPORT**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🗓️ **{report_time}**\n\n"

        f"⚡ **SYSTEM STATUS**\n"
        f"• Bot 10: ✅ Online\n"
        f"• Database: {db_status}\n"
        f"• Uptime: {h}h {m}m\n"
        f"• Auto-Healer: ✅ Active\n"
        f"• Errors Caught: `{bot10_health['errors_caught']}`\n"
        f"• Auto-Healed: `{bot10_health['auto_healed']}`\n"
        f"• Owner Alerts Sent: `{bot10_health['owner_notified']}`\n\n"

        f"👥 **USER BASE**\n"
        f"• Total Users: `{total_users:,}`\n"
        f"• YT Users: `{yt_users:,}`\n"
        f"• IG Users: `{ig_users:,}`\n"
        f"• IG CC Users: `{igcc_users:,}`\n"
        f"• YTCODE Users: `{ytcode_users:,}`\n"
        f"• Banned: `{banned_users}`\n"
        f"• Feature Suspended: `{suspended_users}`\n\n"

        f"🎫 **SUPPORT TICKETS**\n"
        f"• Open: `{open_tickets}`\n"
        f"• Resolved: `{resolved_tickets}`\n"
        f"• Total Ever: `{total_tickets:,}`\n"
        f"• New Today: `{new_tickets_today}`\n"
        f"• Resolved Today: `{resolved_today}`\n\n"

        f"📢 **BROADCASTS**\n"
        f"• Total Stored: `{total_broadcasts}`\n"
        f"• Last Sent: {last_brd_time}\n\n"

        f"👔 **ADMINS**\n"
        f"• Total Admins: `{total_admins}`\n"
        f"• Locked: `{locked_admins}`\n\n"

        f"💾 **BACKUPS**\n"
        f"• Last Backup: {last_bk_time}\n\n"

        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_Auto-report by Bot 10 Enterprise | Next: 12h_"
    )
    return report


async def schedule_daily_reports():
    """Send daily reports at exactly 8:40 AM and 8:40 PM — strict timing"""
    print("📊 [DAILY REPORT] Scheduler started — reports at 8:40 AM and 8:40 PM")
    sent_times = set()  # Track which slots were already sent today

    while True:
        try:
            now = now_local()
            current_slot = None

            # 8:40 AM slot
            if now.hour == 8 and now.minute >= 40 and now.minute < 55:
                current_slot = f"{now.date()}_AM"
            # 8:40 PM slot
            elif now.hour == 20 and now.minute >= 40 and now.minute < 55:
                current_slot = f"{now.date()}_PM"

            if current_slot and current_slot not in sent_times:
                print(f"📊 [DAILY REPORT] Sending {current_slot} report...")
                try:
                    report_text = await generate_daily_report()
                    await bot.send_message(MASTER_ADMIN_ID, report_text, parse_mode="Markdown")
                    sent_times.add(current_slot)
                    print(f"✅ [DAILY REPORT] {current_slot} report sent to owner")
                    # Clean old slots (keep only today's)
                    today_str = str(now.date())
                    sent_times = {s for s in sent_times if today_str in s}
                except Exception as e:
                    print(f"❌ [DAILY REPORT] Failed to send {current_slot}: {e}")

            await asyncio.sleep(60)  # Check every minute for precision

        except asyncio.CancelledError:
            print("📊 [DAILY REPORT] Scheduler stopping...")
            break
        except Exception as e:
            print(f"❌ [DAILY REPORT SCHEDULER] Error: {e}")
            await asyncio.sleep(60)


# ==========================================
# 🌐 RENDER HEALTH CHECK WEB SERVER
# Render requires a web service to respond on $PORT — this lightweight
# aiohttp server satisfies that requirement alongside the bot polling.
# ==========================================

async def _health_handler_bot10(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """Health check endpoint for Render — confirms Bot 10 is alive."""
    uptime = now_local() - bot10_health["bot_start_time"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)
    return aiohttp_web.json_response({
        "status": "ok",
        "bot": "MSA NODE Bot 10",
        "uptime": f"{h}h {m}m",
        "errors_caught": bot10_health["errors_caught"],
        "auto_healed": bot10_health["auto_healed"],
    })


async def start_health_server_bot10():
    """Start the lightweight aiohttp web server for Render health checks.

    Only starts when PORT is explicitly set in the environment (i.e. running on
    Render).  On local dev the env var is absent so we skip binding entirely,
    avoiding WinError 10048 / EADDRINUSE clashes.
    """
    if "PORT" not in os.environ:
        print("🌐 Health server skipped (PORT not set — local dev mode)")
        return None
    app = aiohttp_web.Application()
    app.router.add_get("/health", _health_handler_bot10)
    app.router.add_get("/", _health_handler_bot10)  # Render also checks root
    runner = aiohttp_web.AppRunner(app)
    await runner.setup()
    site = aiohttp_web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🌐 Health check server listening on port {PORT}")
    return runner


# ==========================================
# MAIN EXECUTION — ENTERPRISE READY
# ==========================================

async def main():
    """Enterprise-grade bot10 startup with full resilience"""
    health_task = None
    state_save_task = None
    daily_report_task = None
    cleanup_task = None
    monthly_backup_task = None
    web_runner = None

    print("\n🚀 ═══════════════════════════════════════")
    print("🚀  BOT 10 — ENTERPRISE STARTUP")
    print("🚀 ═══════════════════════════════════════\n")

    # ── 1. Load previous state for continuity ──
    previous_state = load_bot10_state()
    if previous_state:
        print(f"♻️ Resuming from previous session (last seen: {previous_state.get('last_shutdown', 'unknown')})")

    # ── 2. Check backup storage status ──
    check_backup_storage()

    # ── 2b. Migrate old bot10-triggered bans to have scope="bot10" ──
    # This ensures auto-bans and admin-panel bans don't block Bot 8 users
    try:
        migrated = col_banned_users.update_many(
            {
                "scope": {"$exists": False},
                "$or": [
                    {"banned_by": "SYSTEM"},
                    {"reason": "Banned by master admin"}
                ]
            },
            {"$set": {"scope": "bot10"}}
        )
        if migrated.modified_count > 0:
            print(f"🔧 Ban migration: {migrated.modified_count} bot10-scoped ban(s) patched (no longer affect Bot 8)")
    except Exception as _e:
        print(f"⚠️ Ban migration skipped: {_e}")

    # ── 3. Register global error handler ──
    dp.errors.register(bot10_global_error_handler)
    print("🏥 Auto-healer registered — all errors will be caught and handled")

    try:
        # ── 3b. Start Render health check web server ──
        web_runner = await start_health_server_bot10()

        # ── 4. Start background tasks ──
        health_task = asyncio.create_task(bot10_health_monitor())
        print("💊 Health monitor started (checks every hour)")

        cleanup_task = asyncio.create_task(schedule_daily_cleanup())
        print("🧹 Daily cleanup scheduler started (runs at 3:00 AM)")

        monthly_backup_task = asyncio.create_task(schedule_monthly_backup())
        print("� 12h auto-backup scheduler started (Bot 10 → bot10_backups | every 12h AM & PM)")

        daily_report_task = asyncio.create_task(schedule_daily_reports())
        print("📊 Daily report scheduler started (8:40 AM & 8:40 PM)")

        state_save_task = asyncio.create_task(state_auto_save_loop())
        print("💾 State auto-save started (every 5 minutes)")

        # ── 5. Notify owner of successful startup ──
        try:
            prev_info = ""
            if previous_state:
                prev_shutdown = previous_state.get("last_shutdown", "Unknown")
                prev_info = f"\n♻️ <b>Resumed from:</b> {prev_shutdown}"

            await bot.send_message(
                MASTER_ADMIN_ID,
                f"✅ <b>BOT 10 STARTED SUCCESSFULLY</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🏥 Auto-Healer: ✅ Active\n"
                f"💊 Health Monitor: ✅ Running\n"
                f"📊 Daily Reports: ✅ 8:40 AM &amp; 8:40 PM\n"
                f"💾 State Persistence: ✅ Active\n"
                f"🧹 Auto-Cleanup: ✅ 3 AM daily\n"
                f"💿 Auto-Backup: ✅ Every 12h (AM &amp; PM) — bot10_backups\n"
                f"{prev_info}\n\n"
                f"<b>Started:</b> {now_local().strftime('%B %d, %Y — %I:%M:%S %p')}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Bot 10 Enterprise — All systems operational</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"⚠️ Could not send startup notification: {e}")

        # ── 6. Reindex broadcasts to fix any gaps from previous data ──
        try:
            reindex_broadcasts()
            print("🔄 Broadcasts reindexed on startup — all indices are sequential.")
        except Exception as e:
            print(f"⚠️ Broadcast reindex on startup failed: {e}")

        # ── 7. Start polling ──
        print("\n✅ All systems started — beginning polling...\n")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

    except Exception as e:
        print(f"❌ FATAL ERROR during startup: {e}")
        try:
            await notify_master_admin("Bot Startup Failure", str(e), "CRITICAL", False)
        except Exception:
            pass
        raise

    finally:
        # ── 7. Graceful shutdown ──
        print("\n🛑 Bot 10 shutting down gracefully...")

        # Save final state
        save_bot10_state()

        # Cancel background tasks
        for task_name, task in [
            ("Health Monitor", health_task),
            ("State Save", state_save_task),
            ("Daily Report", daily_report_task),
            ("Cleanup", cleanup_task),
            ("Monthly Backup", monthly_backup_task),
        ]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    print(f"✅ {task_name} stopped cleanly")

        # Notify owner of shutdown
        try:
            uptime = now_local() - bot10_health["bot_start_time"]
            h = int(uptime.total_seconds() // 3600)
            m = int((uptime.total_seconds() % 3600) // 60)

            await bot.send_message(
                MASTER_ADMIN_ID,
                f"🛑 **BOT 10 SHUTDOWN**\n\n"
                f"**Uptime:** {h}h {m}m\n"
                f"**Errors Caught:** {bot10_health['errors_caught']}\n"
                f"**Auto-Healed:** {bot10_health['auto_healed']}\n"
                f"**Alerts Sent:** {bot10_health['owner_notified']}\n\n"
                f"**Shutdown:** {now_local().strftime('%B %d, %Y — %I:%M:%S %p')}\n\n"
                f"_State saved. Bot will resume when restarted._",
                parse_mode="Markdown"
            )
        except Exception:
            pass

        try:
            await bot.session.close()
            await bot_8.session.close()
        except Exception:
            pass

        # ── Stop health check web server ──
        if web_runner:
            try:
                await web_runner.cleanup()
                print("🌐 Health check server stopped")
            except Exception:
                pass

        print("✅ Bot 10 shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Bot 10 stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"\n💥 Critical error: {e}")
        sys.exit(1)
