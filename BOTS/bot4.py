import asyncio
import logging
import os
import sys
import io
import pickle
import pymongo
import re
import threading
import traceback
from aiohttp import web
import shutil
import base64
import json
import time
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import FSInputFile, ReplyKeyboardMarkup, KeyboardButton, BotCommand, ReplyKeyboardRemove
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from collections import deque

# ==========================================
# 📡 LIVE TERMINAL CAPTURE
# ==========================================
LOG_BUFFER = deque(maxlen=50)

class StreamLogger:
    """Redirects stdout/stderr to a memory buffer for live bot viewing"""
    def __init__(self, original):
        self.original = original
    
    def write(self, message):
        if message.strip():
            # Get timestamp
            ts = datetime.now().strftime('%I:%M:%S %p')
            # Add to buffer
            LOG_BUFFER.append(f"[{ts}] {message.strip()}")
        # Pass to original stream
        self.original.write(message)
        self.original.flush()
        
    def flush(self):
        self.original.flush()

# Fix Windows console encoding for emoji support
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# Redirect Streams
sys.stdout = StreamLogger(sys.stdout)
sys.stderr = StreamLogger(sys.stderr)
# Timezone support
try:
    import pytz as _pytz
    _BOT4_TZ = _pytz.timezone(os.getenv("REPORT_TIMEZONE", "Asia/Kolkata"))
except Exception:
    _pytz = None
    _BOT4_TZ = None

def now_local() -> datetime:
    """Return current datetime in the configured timezone (12h-safe)."""
    if _pytz and _BOT4_TZ:
        return datetime.now(_BOT4_TZ).replace(tzinfo=None)
    return datetime.now()

# ReportLab & Google Imports
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.colors import Color, gray, black, HexColor
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# ==========================================
# ⚡ CONFIGURATION
# ==========================================
# ==========================================
# ⚡ CONFIGURATION (LOAD FROM DB)
# ==========================================
MONGO_URI = os.getenv("MONGO_URI")

# ==========================================
# ⚙️ DB CONFIGURATION
# ==========================================
DB_CONFIG_FILE = "db_config.json"

def load_db_config():
    default_config = {"connections": {"Default": ""}, "active": "Default"}
    
    config = default_config
    if os.path.exists(DB_CONFIG_FILE):
        try:
            with open(DB_CONFIG_FILE, "r") as f:
                config = json.load(f)
        except:
            config = default_config
            
    # Auto-Fix: If Default is empty, fill it with current env var
    if config["connections"].get("Default") == "" and os.getenv("MONGO_URI"):
        config["connections"]["Default"] = os.getenv("MONGO_URI")
        save_db_config(config)
        
    return config

def save_db_config(data):
    with open(DB_CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=4)

def update_env_file(key, value):
    """Updates the local .env file"""
    try:
        lines = []
        with open(".env", "r") as f:
            lines = f.readlines()
        
        found = False
        with open(".env", "w") as f:
            for line in lines:
                if line.startswith(f"{key}="):
                    f.write(f"{key}={value}\n")
                    found = True
                else:
                    f.write(line)
            if not found:
                f.write(f"\n{key}={value}\n")
    except Exception as e:
        print(f"⚠️ Env update failed: {e}")

def load_secrets_from_db():
    """Fetches BOT_TOKEN, OWNER_ID, and Google Files from MongoDB."""
    try:
        if not MONGO_URI:
            print("❌ Error: MONGO_URI missing from environment.")
            return None, 0, ""

        print("🔄 Fetching secrets from MongoDB...")
        client = pymongo.MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=15000,
            socketTimeoutMS=30000,
            connectTimeoutMS=15000,
            retryWrites=True,
            retryReads=True
        )
        db = client["MSANodeDB"]
        secrets = db["bot_secrets"].find_one({"bot": "bot4"})
        
        if not secrets:
            print("❌ Error: Secrets document not found in db['bot_secrets']!")
            return None, 0, ""

        # Extract Config
        token = secrets.get("BOT_TOKEN")
        owner = int(secrets.get("OWNER_ID", 0))
        folder_id = secrets.get("PARENT_FOLDER_ID", "")

        # Restore Files (credentials.json, token.pickle)
        files = secrets.get("files", {})
        for fname, b64_data in files.items():
            try:
                with open(fname, "wb") as f:
                    f.write(base64.b64decode(b64_data))
                print(f"✅ Restored: {fname}")
            except Exception as e:
                print(f"⚠️ Failed to restore {fname}: {e}")

        client.close()
        return token, owner, folder_id

    except Exception as e:
        print(f"❌ DB Secrets Load Error: {e}")
        # Fallthrough to local fallback
    
    # === LOCAL FALLBACK & AUTO-SEED ===
    print("⚠️ DB Fetch Failed/Empty. Attempting Local Fallback...")
    
    token = os.getenv("BOT_4_TOKEN") or os.getenv("BOT_TOKEN")
    owner_str = os.getenv("OWNER_ID", "0")
    folder_id = os.getenv("PARENT_FOLDER_ID", "")
    
    if token and owner_str:
        print("✅ Found Local Secrets in .env")
        owner = int(owner_str)
        
        # AUTO-SEED DB to prevent future failures
        try:
            print("🚀 Auto-Seeding MongoDB with Local Secrets...")
            client = pymongo.MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=15000,
                socketTimeoutMS=30000,
                connectTimeoutMS=15000,
                retryWrites=True,
                retryReads=True
            )
            db = client["MSANodeDB"]
            col = db["bot_secrets"]
            
            # Read Files for encoding
            files_data = {}
            for f in ['credentials.json', 'token.pickle', 'db_config.json', '.env']:
                 if os.path.exists(f):
                     with open(f, "rb") as file:
                         files_data[f] = base64.b64encode(file.read()).decode('utf-8')
            
            secret_doc = {
                "bot": "bot4",
                "BOT_TOKEN": token,
                "OWNER_ID": str(owner),
                "PARENT_FOLDER_ID": folder_id,
                "files": files_data,
                "updated_at": datetime.now()
            }
            
            col.update_one({"bot": "bot4"}, {"$set": secret_doc}, upsert=True)
            print("✅ MongoDB Auto-Seeded Successfully.")
            client.close()
        except Exception as seed_err:
             print(f"⚠️ Auto-Seed Failed: {seed_err}")
             
        return token, owner, folder_id
        
    return None, 0, ""

# Load Secrets
BOT_TOKEN, OWNER_ID, PARENT_FOLDER_ID = load_secrets_from_db()

if not BOT_TOKEN:
    print("❌ FATAL: BOT_4_TOKEN could not be loaded. Exiting.")
    sys.exit(1)

CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.pickle'

START_TIME = time.time() 

# ==========================================
# 🛠 SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
col_pdfs = None
col_trash = None
col_locked = None
col_trash_locked = None
col_admins = None
db_client = None

# prepare_secrets() - DEPRECATED (Moved to DB loading)

async def handle_health(request):
    return web.Response(text="CORE 4 (PDF INFRASTRUCTURE) IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10004))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")

def connect_db():
    global col_pdfs, col_trash, col_locked, col_trash_locked, col_admins, col_banned, db_client
    try:
        db_client = pymongo.MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=15000,  # Increased from 5s to 15s
            maxPoolSize=50,
            minPoolSize=10,
            maxIdleTimeMS=60000,  # Increased from 45s to 60s
            socketTimeoutMS=30000,  # Increased from 20s to 30s
            connectTimeoutMS=15000,  # Increased from 10s to 15s
            retryWrites=True,  # Enable automatic retry for write operations
            retryReads=True   # Enable automatic retry for read operations
            # Removed socketKeepAlive=True - not a valid pymongo option
        )
        db = db_client["MSANodeDB"]
        col_pdfs = db["pdf_library"]
        col_trash = db["recycle_bin"]
        col_locked = db["locked_content"] # NEW: Locked Content
        col_trash_locked = db["trash_locked"] # NEW: Locked Content Bin
        col_admins = db["admins_bot4"]
        col_banned = db["banned_list"]
        db_client.server_info()
        print("✅ Connected to MongoDB successfully")
        return True
    except Exception as e:
        logging.error(f"DB Connect Error: {e}")
        print(f"❌ Failed to connect to MongoDB: {e}")
        return False

# Initialize database collections with safe fallback
col_pdfs = None
col_trash = None
col_locked = None
col_trash_locked = None
col_admins = None
col_banned = None
db_client = None

# Attempt connection
if not connect_db():
    print("⚠️ WARNING: Bot starting without database connection!")
    print("⚠️ Database-dependent features will be disabled until connection is restored.")
    
# SECURITY GLOBALS
SECURITY_COOLDOWN = {}
SPAM_TRACKER = {} # Middleware: List of timestamps [t1, t2...]
START_TRACKER = {} # Start Handler: [timestamp, count]

# PERMISSION MAPPING
# Text Trigger -> Internal Key
PERMISSION_MAP = {
    "\U0001F4C4 Generate PDF": "gen_pdf",
    "\U0001F517 Get Link": "get_link",
    "\U0001F4CB Show Library": "show_lib",
    "📂 GDrive Explorer": "drive_access",
    "\u270F\uFE0F Edit PDF": "edit_pdf",
    "\U0001F4CA Storage Info": "storage_info",
    "\U0001FA7A System Diagnosis": "sys_diag",
    "\U0001F4BB Live Terminal": "live_term",
    "\u267B\uFE0F Recycle Bin": "recycle_bin",
    "\U0001F5D1 Remove PDF": "remove_pdf",
    "\u26A0\uFE0F NUKE ALL DATA": "nuke_data",
    "\U0001F465 Admin Config": "manage_admins", # Usually Owner only, but configurable
}

# Default Permissions (All True by default or False? request implies toggle-able. 
# "if unselected ... wont be available". Usually better to default to ALL if legacy, or NONE if strict.
# Let's assume newly added admins get ALL by default unless changed, to prevent breakage.)
DEFAULT_PERMISSIONS = list(PERMISSION_MAP.values())

def is_admin(user_id):
    """Checks if user is Owner or in Admin DB (and NOT locked)."""
    if user_id == OWNER_ID: return True
    
    try:
        if col_admins is None:
            return False  # If DB not connected, only owner has admin rights
        
        # Check int
        doc = col_admins.find_one({"user_id": user_id})
        if doc: return not doc.get('locked', False)
        
        # Check str
        doc = col_admins.find_one({"user_id": str(user_id)})
        if doc: return not doc.get('locked', False)
    except Exception as e:
        logging.error(f"is_admin check failed: {e}")
        return False  # On error, don't grant admin access
    
    return False

def is_banned(user_id):
    """Checks if user is in Banned DB."""
    try:
        if col_banned is None:
            return False  # If DB not connected, allow access
        if col_banned.find_one({"user_id": user_id}): 
            return True
        return False
    except Exception as e:
        logging.error(f"is_banned check failed: {e}")
        return False  # On error, don't block user

class SecurityMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.Message, data: dict):
        # Only process Messages (ignore other update types for now)
        if not isinstance(event, types.Message):
            return await handler(event, data)
            
        user = event.from_user
        if not user: return await handler(event, data)
        uid = user.id
        
        # 1. BANNED CHECK (Silence)
        # If banned, we return EARLY without calling handler.
        if is_banned(uid):
            return 

        # 2. ANTI-SPAM (Rate Limit: 5 msgs in 2s)
        now = time.time()
        if uid not in SPAM_TRACKER: SPAM_TRACKER[uid] = []
        
        # Prune old timestamps
        SPAM_TRACKER[uid] = [t for t in SPAM_TRACKER[uid] if now - t < 2.0]
        SPAM_TRACKER[uid].append(now)
        
        if len(SPAM_TRACKER[uid]) > 5:
            if not is_admin(uid): # Don't ban admins
                 # Auto-Ban Logic
                 try:
                     if col_banned is not None:
                         col_banned.insert_one({
                            "user_id": uid, 
                            "reason": "Auto-Ban: Spamming (Flood)", 
                            "timestamp": datetime.now()
                         })
                 except Exception as e:
                     logging.error(f"Auto-ban insert failed: {e}")
                 # Notify Owner
                 try:
                     await bot.send_message(OWNER_ID, f"🚨 **AUTO-BAN:** Banned user `{uid}` for Spamming.")
                 except: pass
                 return # Drop this update
        
        # 3. UNAUTHORIZED ALERT (Only on /start)
        # "if any other started this bot 4 instant notify who starting it"
        if event.text and event.text.startswith("/start"):
            if uid != OWNER_ID and not is_admin(uid):
                 alert = (
                     f"🚨 **SECURITY ALERT: UNAUTHORIZED ACCESS**\n"
                     f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                     f"👤 **Name:** {user.full_name}\n"
                     f"🆔 **ID:** `{uid}`\n"
                     f"🔗 **Username:** @{user.username if user.username else 'N/A'}\n"
                     f"🕐 **Time:** {datetime.now().strftime('%I:%M:%S %p')}\n"
                     f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                     f"⚠️ *Access was blocked.*"
                 )
                 try:
                     await bot.send_message(OWNER_ID, alert)
                 except: pass

        # 4. PERMISSION ENFORCEMENT
        # Check if text corresponds to a protected feature
        # Only apples to Admins (Owner overrides all)
        if uid != OWNER_ID and event.text:
            cleaned_text = event.text.strip()
            # Check if cleaned text is in MAP
            if cleaned_text in PERMISSION_MAP:
                required_perm = PERMISSION_MAP[cleaned_text]
                
                # Check Admin DB
                admin_doc = col_admins.find_one({"user_id": uid})
                if not admin_doc: admin_doc = col_admins.find_one({"user_id": str(uid)})
                
                # If user is admin, check specific permission
                if admin_doc:
                    allowed_perms = admin_doc.get("permissions", DEFAULT_PERMISSIONS)
                    if required_perm not in allowed_perms:
                        # BLOCK ACCESS (Silent drop)
                        return 

        return await handler(event, data)

# Register Middleware
dp.message.middleware(SecurityMiddleware())

class BotState(StatesGroup):
    waiting_for_code = State()
    processing_script = State()
    fetching_link = State()
    deleting_pdf = State()
    confirm_overwrite = State()
    confirm_nuke = State()
    waiting_for_range = State()
    choosing_retrieval_mode = State()
    choosing_retrieval_method = State() # NEW: Single vs Bulk
    choosing_delete_mode = State()
    confirm_delete = State()
    choosing_edit_mode = State()
    waiting_for_edit_target = State()
    waiting_for_new_code = State()
    confirm_empty_bin = State()
    # Admin States
    waiting_for_admin_id = State()
    waiting_for_remove_admin = State()
    waiting_for_ban_id = State()
    waiting_for_unban_id = State()
    # Permission Config
    waiting_for_perm_admin = State()
    waiting_for_perm_toggle = State()
    # Role Config
    waiting_for_role_admin = State()
    waiting_for_role_select = State()
    waiting_for_custom_role = State()
    # Lock Config
    waiting_for_lock_admin = State()
    waiting_for_lock_toggle = State()
    browsing_drive = State()
    # library states
    browsing_library = State() # NEW
    searching_library = State() # NEW
    
    # Recycle Bin States
    bin_category_select = State() # NEW: Choose PDF vs Locked
    bin_menu = State() 
    bin_viewing = State()
    bin_choosing_method = State() # NEW: For "Single vs Bulk"
    bin_restoring = State()
    bin_purging = State()
    bin_confirm_purge = State()
    
    # Database Management States
    databases_menu = State()
    mongo_management = State()
    env_management = State()
    waiting_for_mongo_url = State()
    waiting_for_mongo_alias = State()
    waiting_for_env_key = State()
    waiting_for_env_value = State()
    waiting_for_env_file = State()
    waiting_for_remove_mongo_confirm = State()
    waiting_for_switch_index = State() # NEW
    waiting_for_remove_index = State() # NEW # NEW
    waiting_for_env_file_selection = State() # NEW
    waiting_for_env_replacement_content = State() # NEW
    waiting_for_granular_option = State() # NEW: For Granular .env Edit

def get_main_menu(user_id=None):
    # Determine Permissions
    allowed_keys = set(DEFAULT_PERMISSIONS) # Default to all key strings
    
    if user_id:
        if user_id == OWNER_ID:
            # Owner gets everything
            pass
        elif is_admin(user_id):
            # Fetch specific permissions
            admin_doc = col_admins.find_one({"user_id": user_id})
            if not admin_doc: admin_doc = col_admins.find_one({"user_id": str(user_id)})
            
            if admin_doc:
                allowed_keys = set(admin_doc.get("permissions", DEFAULT_PERMISSIONS))
        else:
            # Non-admin / Stranger (Shouldn't see menu usually, but safe fallback)
            allowed_keys = set() 

    builder = ReplyKeyboardBuilder()
    
    # Helper to check if button should be shown
    def is_allowed(btn_text):
        # If button text is in MAP, check if its key is in allowed_keys
        # If button text is NOT in MAP, it is allowed by default (e.g. "Databases" if not mapped)
        if btn_text in PERMISSION_MAP:
             key = PERMISSION_MAP[btn_text]
             return key in allowed_keys
        return True # Allow unmapped buttons (like Back, or new features not yet secured)

    # 1. CORE OPERATIONS
    row1 = []
    if is_allowed("\U0001F4C4 Generate PDF"): row1.append(KeyboardButton(text="\U0001F4C4 Generate PDF"))
    if is_allowed("\U0001F517 Get Link"): row1.append(KeyboardButton(text="\U0001F517 Get Link"))
    if row1: builder.row(*row1)
    
    # 2. VISIBILITY
    row2 = []
    if is_allowed("\U0001F4CB Show Library"): row2.append(KeyboardButton(text="\U0001F4CB Show Library"))
    if is_allowed("📂 GDrive Explorer"): row2.append(KeyboardButton(text="📂 GDrive Explorer"))
    if row2: builder.row(*row2)

    # 3. MAINTENANCE
    row3 = []
    if is_allowed("\u270F\uFE0F Edit PDF"): row3.append(KeyboardButton(text="\u270F\uFE0F Edit PDF"))
    if is_allowed("\U0001F4CA Storage Info"): row3.append(KeyboardButton(text="\U0001F4CA Storage Info"))
    if row3: builder.row(*row3)
    
    # 4. DIAGNOSIS
    row4 = []
    if is_allowed("\U0001FA7A System Diagnosis"): row4.append(KeyboardButton(text="\U0001FA7A System Diagnosis"))
    if is_allowed("\U0001F4BB Live Terminal"): row4.append(KeyboardButton(text="\U0001F4BB Live Terminal"))
    if row4: builder.row(*row4)
    
    # 5. SAFETY
    row5 = []
    if is_allowed("\u267B\uFE0F Recycle Bin"): row5.append(KeyboardButton(text="\u267B\uFE0F Recycle Bin"))
    if is_allowed("\U0001F5D1 Remove PDF"): row5.append(KeyboardButton(text="\U0001F5D1 Remove PDF"))
    if row5: builder.row(*row5)
    
    # 6. CONFIG
    row6 = []
    # Standardize on Gear Emoji for Admin Config to match handler
    if is_allowed("⚙️ Admin Config"): row6.append(KeyboardButton(text="⚙️ Admin Config"))
    if is_allowed("\u26A0\uFE0F NUKE ALL DATA"): row6.append(KeyboardButton(text="\u26A0\uFE0F NUKE ALL DATA"))
    if row6: builder.row(*row6)

    # 7. EXTRAS & BACKUP
    row7 = []
    row7.append(KeyboardButton(text="📦 Backup")) # New Instant Backup
    row7.append(KeyboardButton(text="🗄️ Databases"))
    builder.row(*row7)

    # 8. HELP (Bottom Single Row)
    builder.row(KeyboardButton(text="\U0001F48E Elite Help"))

    return builder.as_markup(resize_keyboard=True)

def generate_progress_bar(percentage):
    """Creates a visual progress bar for Telegram."""
    filled_length = int(percentage // 10)
    bar = "▓" * filled_length + "░" * (10 - filled_length)
    return f"|{bar}| {percentage:.1f}%"

def get_formatted_file_list(docs, limit=30):
    """Generates a clean, consistent HTML list of files with indices and hyperlinks."""
    if not docs:
        return ["_No files found._"]
        
    lines = []
    for idx, doc in enumerate(docs[:limit], 1):
        code = doc.get('code', 'UNK')
        ts = doc.get('timestamp')
        date_str = ts.strftime('%d-%b %I:%M %p') if ts else "?"
        link = doc.get('link', None)
        restored_mark = " <b>[R]</b>" if doc.get('restored', False) else ""
        
        # Professional Format (Vertical & Clean)
        # 1️⃣ CODE
        # 📅 18-Jan • 🔗 Access
        
        # User requested: "display index 1 2 3 so on"
        # User requested: "enhance links display"
        
        if link:
            line = f"<b>{idx}. {code}</b>{restored_mark}\n<i>{date_str}</i> • <a href='{link}'>🔗 Drive Link</a>"
        else:
            line = f"<b>{idx}. {code}</b>{restored_mark}\n<i>{date_str}</i>"
            
        lines.append(line)
    
    if len(docs) > limit:
        lines.append(f"\n...and {len(docs)-limit} more.")
        
    return lines

# ==========================================
# 🚀 AUTOMATION TASKS
# ==========================================

# hourly_pulse removed


# ENTERPRISE: DAILY STATS TRACKING
DAILY_STATS_BOT4 = {"pdfs_generated": 0, "pdfs_deleted": 0, "errors": 0, "links_retrieved": 0}

# ENTERPRISE: INSTANT ERROR NOTIFICATION
async def notify_error_bot4(error_type, details):
    """Send instant error notification to owner with enhanced context"""
    global DAILY_STATS_BOT4
    try:
        # Get system context
        uptime_secs = int(time.time() - START_TIME)
        uptime_str = f"{uptime_secs // 3600}h {(uptime_secs % 3600) // 60}m"
        
        # Database status
        db_status = "✅ OK"
        try:
            db_client.server_info()
        except:
            db_status = "❌ DISCONNECTED"
        
        alert = (
            f"🚨 <b>BOT 4 INSTANT ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <b>Type:</b> {error_type}\n"
            f"📝 <b>Details:</b> {str(details)[:500]}\n"
            f"🕐 <b>Time:</b> {now_local().strftime('%I:%M:%S %p')}\n"
            f"💾 <b>Database:</b> {db_status}\n"
            f"🚀 <b>Started At:</b> {datetime.fromtimestamp(START_TIME).strftime('%I:%M %p')}\n"
            f"⏱ <b>Uptime:</b> {uptime_str}\n"
            f"📊 <b>Today's Errors:</b> {DAILY_STATS_BOT4['errors'] + 1}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        await bot.send_message(OWNER_ID, alert, parse_mode="HTML")
        logging.info(f"🚨 Error Alert Sent: {error_type}")
        
        # Increment error counter
        DAILY_STATS_BOT4["errors"] += 1
    except Exception as e:
        logging.error(f"Failed to send error alert: {e}")

# daily_briefing removed (Replaced by strict_daily_report)

async def system_guardian():
    """
    Auto-healer: checks DB and Drive every 30 min.
    On failure, attempts reconnect + notifies owner. Escalates on repeated failures.
    """
    print("🛡️ System Guardian (Auto-Healer): Online")
    consecutive_failures = 0
    while True:
        try:
            db_client.server_info()
            get_drive_service()
            if consecutive_failures > 0:
                # Recovered — notify owner
                try:
                    await bot.send_message(
                        OWNER_ID,
                        f"✅ <b>BOT 4 AUTO-HEALER: RECOVERED</b>\n\n"
                        f"All systems back online after {consecutive_failures} failure(s).\n"
                        f"🕐 {now_local().strftime('%I:%M %p  ·  %b %d, %Y')}",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            print(f"⚠️ Guardian: System issue detected (#{consecutive_failures}): {e}")

            # Attempt DB reconnect
            reconnected = await asyncio.to_thread(connect_db)
            reconnect_status = "✅ Reconnected" if reconnected else "❌ Still Down"

            # Escalating alert
            if consecutive_failures == 1 or consecutive_failures % 3 == 0:
                await notify_error_bot4(
                    f"Auto-Healer Alert (failure #{consecutive_failures})",
                    f"DB/Drive issue: {e}\nDB Reconnect: {reconnect_status}"
                )

        await asyncio.sleep(1800)  # Every 30 minutes

async def auto_janitor():
    while True:
        await asyncio.sleep(86400)
        for file in os.listdir():
            if file.endswith(".pdf"):
                try: os.remove(file)
                except: pass

# ==========================================
# 📦 INSTANT BACKUP SYSTEM
# ==========================================
def generate_system_backup():
    """Generates a comprehensive snapshot of the system."""
    try:
        now = now_local()
        timestamp = now.strftime('%b %d, %Y  ·  %I:%M %p')
        date_str  = now.strftime('%Y-%m-%d')
        month_str = now.strftime('%B_%Y')         # e.g. February_2026
        filename = f"MSANODE_BACKUP_{date_str}.txt"
        
        # 1. Header
        content = (
            f"🛡 MSANODE SYSTEM BACKUP\n"
            f"📅 Generated: {timestamp}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        
        # 2. PDF Library
        pdfs = list(col_pdfs.find().sort("_id", -1))
        content += f"📚 PDF LIBRARY ({len(pdfs)} Files)\n"
        content += f"----------------------------------------\n"
        if pdfs:
            for p in pdfs:
                 code = p.get('code', 'N/A')
                 link = p.get('link', 'N/A')
                 views = p.get('views', 0)
                 content += f"[{code}] Views:{views} | Link: {link}\n"
        else:
            content += "No PDFs found.\n"
        content += "\n"
        
        # 3. Admins
        admins = list(col_admins.find())
        content += f"👥 ADMIN ROSTER ({len(admins)} Users)\n"
        content += f"----------------------------------------\n"
        if admins:
            for a in admins:
                uid = a.get('user_id')
                role = a.get('role', 'Admin')
                locked = "LOCKED" if a.get('locked') else "Active"
                content += f"ID: {uid} | Role: {role} | Status: {locked}\n"
        else:
             content += "No Admins found.\n"
        content += "\n"
        
        # 4. Banned Users
        try:
            banned = list(col_banned.find()) if col_banned is not None else []
        except Exception as e:
            logging.error(f"Failed to fetch banned users: {e}")
            banned = []
        content += f"🚫 BLACKLISTED USERS ({len(banned)} Users)\n"
        content += f"----------------------------------------\n"
        if banned:
             for b in banned:
                 uid = b.get('user_id')
                 reason = b.get('reason', 'N/A')
                 content += f"ID: {uid} | Reason: {reason}\n"
        else:
             content += "No Banned users.\n"
             
        content += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        content += "💎 END OF REPORT | MSANODE SYSTEMS"
        
        # Write to temp file
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
            
        return filename
    except Exception as e:
        logging.error(f"Backup Gen Error: {e}")
        return None

@dp.message(F.text == "📦 Backup")
async def backup_menu_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📄 Text Report"), KeyboardButton(text="💾 JSON Dump"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "📦 **BACKUP & RECOVERY**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Select Backup Format:\n\n"
        "📄 **Text Report**: Human-readable summary of all data.\n"
        "💾 **JSON Dump**: Full database export for restoring the bot.\n\n"
        "<i>Files are automatically uploaded to Google Drive.</i>",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )

@dp.message(F.text == "📄 Text Report")
async def handle_backup_text(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    msg = await message.answer("⏳ **Generating Text Report...**")
    
    # Run in thread
    filename = await asyncio.to_thread(generate_system_backup)
    
    if filename and os.path.exists(filename):
        try:
            # Upload to Drive
            link = await asyncio.to_thread(upload_to_drive, filename)
            
            caption = (
                f"🛡 <b>SYSTEM REPORT SECURED</b>\n"
                f"📅 <code>{datetime.now().strftime('%Y-%m-%d %H:%M')}</code>\n"
                f"☁️ <a href='{link}'><b>Google Drive Link</b></a>"
            )
            await message.answer_document(
                FSInputFile(filename), 
                caption=caption,
                parse_mode="HTML"
            )
            os.remove(filename) 
            await msg.delete()
        except Exception as e:
            await msg.edit_text(f"❌ Upload Failed: {e}")
    else:
        await msg.edit_text("❌ Generation Failed.")

@dp.message(F.text == "💾 JSON Dump")
async def handle_backup_json(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    msg = await message.answer("⏳ **Exporting Database...**")
    
    try:
        data = {
            "timestamp": datetime.now().isoformat(),
            "pdfs": list(col_pdfs.find({}, {"_id": 0})) if col_pdfs is not None else [],
            "admins": list(col_admins.find({}, {"_id": 0})) if col_admins is not None else [],
            "banned": list(col_banned.find({}, {"_id": 0})) if col_banned is not None else [],
            "trash": list(col_trash.find({}, {"_id": 0})) if col_trash is not None else [],
            "trash_locked": list(col_trash_locked.find({}, {"_id": 0})) if col_trash_locked is not None else []
        }
        
        filename = f"MSANODE_FULL_DUMP_{datetime.now().strftime('%Y-%m-%d')}.json"
        
        # Write JSON
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, default=str)
            
        # Upload
        link = await asyncio.to_thread(upload_to_drive, filename)
        
        caption = (
            f"💾 <b>FULL DATABASE DUMP</b>\n"
            f"📅 <code>{datetime.now().strftime('%Y-%m-%d %H:%M')}</code>\n"
            f"🔢 <b>Records:</b> {len(data['pdfs']) + len(data['trash'])} PDFs\n"
            f"☁️ <a href='{link}'><b>Google Drive Link</b></a>\n\n"
            f"⚠️ <i>Keep this file safe! It contains your entire bot data.</i>"
        )
        
        await message.answer_document(
            FSInputFile(filename),
            caption=caption,
            parse_mode="HTML"
        )
        
        os.remove(filename)
        await msg.delete()
        
    except Exception as e:
        await msg.edit_text(f"❌ Export Failed: {e}")

async def weekly_backup():
    while True:
        now = datetime.now()
        days_until_sunday = (6 - now.weekday()) % 7
        if days_until_sunday == 0 and now.hour >= 3: days_until_sunday = 7
        target = now.replace(hour=3, minute=0, second=0, microsecond=0) + timedelta(days=days_until_sunday)
        
        # Calculate seconds to wait
        wait_seconds = (target - now).total_seconds()
        # logging.info(f"⏳ System: Weekly Backup scheduled in {wait_seconds/3600:.1f} hours.")
        await asyncio.sleep(wait_seconds)
        
        try:
            # Reuse the comprehensive backup generator
            filename = await asyncio.to_thread(generate_system_backup)
            
            if filename:
                # Upload to Drive (Monthly Vault)
                link = await asyncio.to_thread(upload_to_drive, filename)
                
                caption = (
                    f"🛡 <b>WEEKLY AUTO-BACKUP</b>\n"
                    f"📅 <code>{datetime.now().strftime('%Y-%m-%d')}</code>\n"
                    f"☁️ <a href='{link}'><b>Google Drive Link</b></a>"
                )
                
                # Send to Owner
                try:
                    await bot.send_document(
                        OWNER_ID, 
                        FSInputFile(filename), 
                        caption=caption, 
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logging.error(f"Failed to DM Owner backup: {e}")
                    
                # Cleanup
                if os.path.exists(filename):
                     os.remove(filename)
            
        except Exception as e:
            await notify_error_bot4("Weekly Backup Failed", str(e))


# ==========================================
# 🧠 PDF GENERATION - S19 STYLE
# ==========================================

def draw_canvas_extras(canvas_obj, doc):
    """Adds MSANODE watermark and page numbers like S19.pdf"""
    canvas_obj.saveState()
    
    # Watermark
    canvas_obj.translate(letter[0]/2, letter[1]/2)
    canvas_obj.rotate(45)
    canvas_obj.setFillColor(Color(0, 0, 0, alpha=0.08))
    canvas_obj.setFont("Helvetica-Bold", 70)
    canvas_obj.drawCentredString(0, 0, "MSANODE")
    canvas_obj.restoreState()
    
    # Premium Black Border
    canvas_obj.saveState()
    canvas_obj.setStrokeColor(HexColor('#000000'))
    canvas_obj.setLineWidth(2)  # Nice thick premium line
    # Draw border with 0.5 inch margin
    canvas_obj.rect(0.5*inch, 0.5*inch, letter[0]-1.0*inch, letter[1]-1.0*inch)
    canvas_obj.restoreState()
    
    # Page number footer
    canvas_obj.saveState()
    canvas_obj.setFont("Helvetica", 9)
    canvas_obj.setFillColor(gray)
    # Left footer: MSANODE OFFICIAL BLUEPRINT
    canvas_obj.drawString(
        0.75*inch, 
        0.25*inch, 
        "MSANODE OFFICIAL BLUEPRINT"
    )
    
    # Right footer: Page Number
    canvas_obj.drawRightString(
        letter[0] - 0.75*inch, 
        0.25*inch, 
        f"Page {doc.page}"
    )
    canvas_obj.restoreState()

def process_inline_formatting(text):
    """
    Process inline formatting markers:
    - ****** link ****** -> CLICKABLE BLUE LINK
    - ***** text ***** -> DARK BLACK BOLD ALL CAPS
    - **** text **** -> BLUE BOLD ALL CAPS
    - ***text*** -> RED BOLD ALL CAPS
    - *text* -> DARK BLACK BOLD (no caps)
    - Normal text -> standard lowercase (no special formatting)
    """
    # HIGHEST PRIORITY: Handle ****** link ****** (CLICKABLE LINKS)
    def create_clickable_link(match):
        url = match.group(1).strip()  # Extract the URL
        # Make it a clickable blue link
        return f'<a href="{url}" color="#1565C0"><u>{url}</u></a>'
    
    text = re.sub(r'\*\*\*\*\*\*([^*]+?)\*\*\*\*\*\*', create_clickable_link, text)
    
    # Then handle ***** text ***** (DARK BLACK BOLD ALL CAPS)
    def uppercase_black_5star(match):
        content = match.group(1).strip().upper()  # Strip spaces and uppercase
        return f'<font color="#000000"><b>{content}</b></font>'  # Dark black
    
    text = re.sub(r'\*\*\*\*\*([^*]+?)\*\*\*\*\*', uppercase_black_5star, text)

    # Then handle **** text **** (BLUE BOLD ALL CAPS)
    def uppercase_blue(match):
        content = match.group(1).strip().upper()  # Strip spaces and uppercase
        return f'<font color="#1565C0"><b>{content}</b></font>'  # Dark blue
    
    text = re.sub(r'\*\*\*\*([^*]+?)\*\*\*\*', uppercase_blue, text)
    
    # Then handle ***text*** (RED BOLD ALL CAPS)
    def uppercase_red(match):
        content = match.group(1).strip().upper()  # Strip spaces and uppercase
        return f'<font color="#D32F2F"><b>{content}</b></font>'
    
    text = re.sub(r'\*\*\*([^*]+?)\*\*\*', uppercase_red, text)
    
    # Finally handle *text* (DARK BLACK BOLD, no caps - keep original case)
    def bold_black_no_caps(match):
        content = match.group(1).strip()  # Strip spaces but keep original case
        return f'<font color="#000000"><b>{content}</b></font>'
    
    text = re.sub(r'\*([^*]+?)\*', bold_black_no_caps, text)
    
    return text


def create_goldmine_pdf(text, filename):
    """Creates PDF in S19 professional format"""
    
    # Clean text - remove non-ASCII characters
    text = re.compile(r'[^\x00-\x7F]+').sub('', text)
    
    # Remove line separator graphics (______________ style lines)
    text = re.sub(r'_{20,}', '', text)
    
    # Clean up excessive newlines but keep intentional breaks
    text = re.sub(r'\n{4,}', '\n\n', text)
    
    # CRITICAL FIX: Merge Roman numerals with their titles if split across lines
    # This fixes "I.\n THE OPPORTUNITY" -> "I. THE OPPORTUNITY"
    text = re.sub(r'(^|\n)(I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII)\.\s*\n\s*', r'\1\2. ', text, flags=re.MULTILINE)
    
    # Setup document
    doc = SimpleDocTemplate(
        filename, 
        pagesize=letter,
        leftMargin=0.75*inch,
        rightMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch
    )
    
    # Define styles matching S19.pdf
    styles = getSampleStyleSheet()
    
    # Header style (MSANODE VAULT BLUEPRINT) - Dark Black and Underlined
    styles.add(ParagraphStyle(
        name='MSAHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=16,
        leading=20,
        textColor=HexColor('#000000'),  # Dark black color
        alignment=TA_CENTER,
        spaceAfter=6,
        underlineWidth=1,
        underlineColor=HexColor('#000000')
    ))
    
    # Main Title style (for the very first line)
    styles.add(ParagraphStyle(
        name='MainTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        leading=14,
        textColor=black,
        alignment=TA_LEFT,
        spaceAfter=12
    ))
    
    # Section Header (I, II, III, etc.) - Keep Roman numeral with title on SAME line - RED COLOR
    styles.add(ParagraphStyle(
        name='SectionHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=12,
        leading=16,
        textColor=HexColor('#D32F2F'),  # Vibrant red for Roman numerals
        alignment=TA_LEFT,
        spaceAfter=10,
        spaceBefore=14
    ))
    
    # Subsection with parentheses - Medium gray
    styles.add(ParagraphStyle(
        name='ParenSubsection',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=10,
        leading=13,
        textColor=HexColor('#404040'),  # Medium gray for subsections
        alignment=TA_LEFT,
        spaceAfter=6,
        spaceBefore=6
    ))
    
    # Subsection (The, Core, etc.) - Medium gray
    styles.add(ParagraphStyle(
        name='Subsection',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=10,
        leading=13,
        textColor=HexColor('#404040'),  # Medium gray for subsections
        alignment=TA_LEFT,
        spaceAfter=6,
        spaceBefore=8
    ))
    
    # Body text - LIGHT GRAY - JUSTIFIED
    styles.add(ParagraphStyle(
        name='Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        leading=14,
        textColor=HexColor('#333333'),  # Light gray for body text
        alignment=TA_JUSTIFY,
        spaceAfter=8
    ))
    
    # Code/Formula Box style
    styles.add(ParagraphStyle(
        name='CodeBox',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=9,
        leading=12,
        textColor=HexColor('#212121'),
        backColor=HexColor('#F5F5F5'),
        borderColor=HexColor('#E0E0E0'),
        borderWidth=1,
        borderPadding=6,
        alignment=TA_LEFT,
        spaceAfter=12,
        spaceBefore=8,
        leftIndent=6,
        rightIndent=6
    ))
    
    # All-caps header style - DARK BLACK
    styles.add(ParagraphStyle(
        name='AllCapsHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        leading=14,
        textColor=HexColor('#000000'),  # Dark black for all-caps
        alignment=TA_LEFT,
        spaceAfter=10,
        spaceBefore=10
    ))
    
    # Build story
    story = []
    
    # Add header (MSANODE VAULT BLUEPRINT) - Underlined
    story.append(Paragraph("<u>MSANODE VAULT BLUEPRINT</u>", styles['MSAHeader']))
    story.append(Spacer(1, 0.1*inch))
    
    # Parse and format content
    lines = text.split('\n')
    
    # Track if we've added the main title
    main_title_added = False
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # First substantive line is the main title
        if not main_title_added and len(line) > 20:
            story.append(Paragraph(process_inline_formatting(line), styles['MainTitle']))
            main_title_added = True
            continue
        
        # CRITICAL FIX: Roman numerals sections - keep numeral AND title together
        # Matches "I. THE OPPORTUNITY" or "VII. FINAL WORD" etc.
        # Display in BOLD, ALL CAPS, RED
        if re.match(r'^(I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII)\.\s+', line):
            story.append(Spacer(1, 0.08*inch))
            # Convert to uppercase and bold for premium red appearance
            story.append(Paragraph(process_inline_formatting(f"<b>{line.upper()}</b>"), styles['SectionHeader']))
            continue
        
        # Parentheses subsections like (The Managerial Mindset) or (Precision Engineering)
        if re.match(r'^\(.*?\):', line) or (line.startswith('(') and line.endswith(':')):
            story.append(Paragraph(process_inline_formatting(line), styles['ParenSubsection']))
            continue
        
        # Subsections starting with "The" or "THE" followed by title
        if re.match(r'^(The|THE)\s+[A-Z].*?:', line):
            story.append(Paragraph(process_inline_formatting(line), styles['Subsection']))
            continue
        
        # Code/Example boxes
        if line.lower().strip().startswith('example:') or line.lower().strip().startswith('formula:'):
            story.append(Paragraph(process_inline_formatting(line), styles['CodeBox']))
            continue
        
        # Numbered subsections like "1. THE LOGIC TRANSLATION"
        if re.match(r'^\d+\.\s+THE\s+[A-Z]', line):
            story.append(Paragraph(process_inline_formatting(line), styles['Subsection']))
            continue
        
        # Other bold subsections (Core Tools, etc.)
        if line.startswith('CORE TOOLS') or line.startswith('Core Tools'):
            story.append(Paragraph(process_inline_formatting(f"<b>{line}</b>"), styles['Subsection']))
            continue
        
        # All caps section dividers (but not too long to avoid body text in caps) - DARK BLACK
        if line.isupper() and 5 < len(line) < 100:
            story.append(Paragraph(process_inline_formatting(f"<b>{line}</b>"), styles['AllCapsHeader']))
            continue
        
        # Bullet points or dashes
        if line.startswith('-') or line.startswith('•'):
            story.append(Paragraph(process_inline_formatting(line), styles['Body']))
            continue
        
        # Regular body text - split into chunks if extremely long
        if len(line) > 600:
            # Split at sentence boundaries for readability
            sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', line)
            for sentence in sentences:
                if sentence.strip():
                    story.append(Paragraph(process_inline_formatting(sentence.strip()), styles['Body']))
        else:
            story.append(Paragraph(process_inline_formatting(line), styles['Body']))
    
    # Build PDF
    doc.build(story, onFirstPage=draw_canvas_extras, onLaterPages=draw_canvas_extras)

def get_drive_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as t: creds = pickle.load(t)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token: creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/drive.file'])
            creds = flow.run_local_server(port=8080)
        with open(TOKEN_FILE, 'wb') as t: pickle.dump(creds, t)
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(filename):
    service = get_drive_service()
    
    # Generate dynamic folder name (e.g., "JANUARY 2026")
    # User Request: "HANUARY 2026" format
    month_name = datetime.now().strftime('%B %Y').upper()
    folder_name = month_name
    
    # Check if folder exists
    if PARENT_FOLDER_ID:
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and '{PARENT_FOLDER_ID}' in parents and trashed = false"
    else:
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        
    results = service.files().list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    folders = results.get('files', [])
    
    if folders:
        target_folder_id = folders[0]['id']
    else:
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if PARENT_FOLDER_ID:
            folder_metadata['parents'] = [PARENT_FOLDER_ID]
            
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        target_folder_id = folder.get('id')
        print(f"◈ System: Created new monthly vault: {folder_name}")

    # Upload file
    media = MediaIoBaseUpload(io.FileIO(filename, 'rb'), mimetype='application/pdf')
    file_metadata = {'name': filename, 'parents': [target_folder_id]}
    file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink', supportsAllDrives=True).execute()
    
    service.permissions().create(fileId=file.get('id'), body={'type': 'anyone', 'role': 'reader'}).execute()
    
    return file.get('webViewLink')

def download_from_drive(filename):
    """Downloads a file from Drive by name to local storage."""
    service = get_drive_service()
    
    # 1. Search for file by name (Global search to find it in subfolders)
    # We remove 'parents' check because files are inside Month Folders, not the root.
    query = f"name = '{filename}' and mimeType = 'application/pdf' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = results.get('files', [])
    
    if not files:
        return None
        
    file_id = files[0]['id']
    
    # 2. Download content
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(filename, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        
    return filename

def get_recycle_bin_id(service):
    """Finds or creates 'Recycle Bin' folder inside the Vault."""
    if PARENT_FOLDER_ID:
        query = f"mimeType='application/vnd.google-apps.folder' and name='Recycle Bin' and '{PARENT_FOLDER_ID}' in parents and trashed=false"
    else:
        # Fallback: Search globally if no parent defined (Not ideal, but prevents crash)
        query = "mimeType='application/vnd.google-apps.folder' and name='Recycle Bin' and trashed=false"
        
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    
    if files:
        return files[0]['id']
    else:
        # Create it
        metadata = {
            'name': 'Recycle Bin',
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if PARENT_FOLDER_ID:
            metadata['parents'] = [PARENT_FOLDER_ID]
            
        folder = service.files().create(body=metadata, fields='id').execute()
        return folder.get('id')

def move_to_recycle_bin(filename):
    """Moves a file to the Recycle Bin folder in Drive."""
    service = get_drive_service()
    
    # 1. Search for file by name
    query = f"name = '{filename}' and mimeType = 'application/pdf' and trashed = false"
    results = service.files().list(q=query, fields="files(id, parents)").execute()
    files = results.get('files', [])
    
    if not files:
        return False
        
    bin_id = get_recycle_bin_id(service)
    
    # 2. Move file
    try:
        for f in files:
            # Move key: addParents = bin, removeParents = current
            prev_parents = ",".join(f.get('parents', []))
            service.files().update(
                fileId=f['id'],
                addParents=bin_id,
                removeParents=prev_parents,
                fields='id, parents'
            ).execute()
        return True
    except:
        return False

def rename_file_in_drive(old_filename, new_filename):
    """Renames a file in Drive."""
    service = get_drive_service()
    
    # 1. Search for file
    query = f"name = '{old_filename}' and mimeType = 'application/pdf' and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    
    if not files:
        return False
    
    # 2. Rename (first match)
    file_id = files[0]['id']
    try:
        service.files().update(
            fileId=file_id,
            body={'name': new_filename},
            fields='id, name'
        ).execute()
        return True
    except:
        return False

def empty_drive_folder(folder_id):
    """Permanently deletes all files in a folder."""
    service = get_drive_service()
    
    deleted_count = 0
    page_token = None
    
    while True:
        # Search for all children
        q = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(q=q, fields="nextPageToken, files(id)", pageToken=page_token).execute()
        items = results.get('files', [])
        
        for item in items:
            try:
                service.files().delete(fileId=item['id']).execute()
                deleted_count += 1
            except:
                pass
        
        page_token = results.get('nextPageToken')
        if not page_token:
            break
            
            break
            
    return deleted_count

def list_drive_contents(folder_id):
    """Fetches folders and files for the Explorer."""
    service = get_drive_service()
    
    # 1. Get Folder Name (for header)
    try:
        if folder_id == 'root':
            folder_name = "My Drive (Root)"
            parents = []
        else:
            f = service.files().get(fileId=folder_id, fields="name, parents", supportsAllDrives=True).execute()
            folder_name = f.get('name', 'Unknown Folder')
            parents = f.get('parents', [])
    except:
        folder_name = "Unknown Folder"
        parents = []
        
    parent_id = parents[0] if parents else None
    
    # 2. List Items (Folders then Files)
    query = f"'{folder_id}' in parents and trashed = false"
    # Folders first
    q_folders = f"{query} and mimeType = 'application/vnd.google-apps.folder'"
    res_folders = service.files().list(
        q=q_folders, 
        fields="files(id, name)", 
        orderBy="name",
        pageSize=1000,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    folders = res_folders.get('files', [])
    
    # Files next
    q_files = f"{query} and mimeType != 'application/vnd.google-apps.folder'"
    res_files = service.files().list(
        q=q_files, 
        fields="files(id, name, webViewLink, size)", 
        orderBy="name",
        pageSize=1000,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = res_files.get('files', [])
    
    
    return folder_name, parent_id, folders, files

def find_folder_by_name(name):
    """Searches for a specific folder by name (Smart Strict)."""
    service = get_drive_service()
    # 1. Broad Search to find candidates
    query = f"mimeType = 'application/vnd.google-apps.folder' and name contains '{name}' and trashed = false"
    res = service.files().list(
        q=query, 
        fields="files(id, name)", 
        pageSize=10, # Check top 10 matches
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    files = res.get('files', [])
    
    # 2. Strict Python Filter (Case Insensitive)
    for f in files:
        if f['name'].strip().lower() == name.lower():
            return f['id']
            
    return None

def get_folder_parent(folder_id):
    """Gets the parent ID of a folder."""
    service = get_drive_service()
    try:
        f = service.files().get(fileId=folder_id, fields="parents", supportsAllDrives=True).execute()
        parents = f.get('parents', [])
        return parents[0] if parents else None
    except:
        return None

def get_folder_size(folder_id):
    """Recursively calculates total size and file count of a Drive folder."""
    try:
        service = get_drive_service()
        total_bytes = 0
        file_count = 0
        
        # List all files in this folder (including subfolders)
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id, name, mimeType, size)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        items = results.get('files', [])
        
        for item in items:
            mime_type = item.get('mimeType', '')
            
            # If it's a folder, recurse into it
            if mime_type == 'application/vnd.google-apps.folder':
                folder_bytes, folder_files = get_folder_size(item['id'])
                total_bytes += folder_bytes
                file_count += folder_files
            else:
                # It's a file
                file_count += 1
                # Google Docs/Sheets/Slides don't have a 'size' field
                if 'size' in item:
                    total_bytes += int(item.get('size', 0))
        
        return total_bytes, file_count
        
    except Exception as e:
        print(f"Folder Size Error: {e}")
        raise e

# ==========================================
# 🤖 HANDLERS
# ==========================================

@dp.message(Command("start"))
@dp.message(F.text == "🔙 Back to Menu")
async def start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # 1. SILENT BAN CHECK
    if is_banned(user_id): return # Ignore completely

    # 2. ADMIN CHECK
    if is_admin(user_id):
        await state.clear()
        reply_markup = get_main_menu(user_id)
        
        # Determine Greeting
        if user_id == OWNER_ID:
            greeting = "💎 **MSA NODE BOT 4**\nAt your command, Master."
        else:
            # Fetch Role
            admin_doc = col_admins.find_one({"user_id": user_id})
            if not admin_doc: admin_doc = col_admins.find_one({"user_id": str(user_id)})
            
            role = admin_doc.get("role", "Authorized Admin") if admin_doc else "Authorized Admin"
            name = message.from_user.full_name
            
            greeting = (
                f"💎 **MSA NODE SYSTEMS**\n"
                f"ASSIGNED BY 👑 **OWNER:** MSA\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🛡️ **ACCESS GRANTED**\n"
                f"👤 **Officer:** {name}\n"
                f"🔰 **Rank:** `{role}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🚀 System initialized."
            )

        await message.answer(greeting, reply_markup=reply_markup)
        return

    # 3. STRANGER / SPAM LOGIC
    now = time.time()
    
    # Initialize or reset tracker if > 60s
    if user_id not in START_TRACKER or (now - START_TRACKER[user_id][0] > 60):
        START_TRACKER[user_id] = [now, 1]
    else:
        START_TRACKER[user_id][1] += 1
        
    # CHECK FLOOD
    if START_TRACKER[user_id][1] > 5:
        # AUTO-BAN EXECUTION
        if not is_banned(user_id): # Double check
            try:
                if col_banned is not None:
                    col_banned.insert_one({
                        "user_id": user_id,
                        "reason": "Auto-Ban: Spamming /start",
                        "timestamp": datetime.now()
                    })
            except Exception as e:
                logging.error(f"Failed to ban user {user_id}: {e}")
            # Notify Owner One Last Time
            alert = (
                f"🚨 **SECURITY: AUTO-BANNED USER**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 `{user_id}`\n"
                f"⚠️ Reason: Spamming /start > 5 times/min.\n"
                f"⛔ **Status:** PERMANENTLY BLOCKED."
            )
            try: await bot.send_message(OWNER_ID, alert)
            except: pass
        return

    # 4. UNAUTHORIZED (Fallthrough)
    # The SecurityMiddleware has already alerted the Owner.
    # We just silently return or can send a generic denial if preferred.
    return

@dp.message(F.text == "📋 Show Library")
async def show_library(message: types.Message, state: FSMContext):
    # 1. Main Menu Fork
    await message.answer(
        "📚 **VAULT LIBRARY ACCESS**\n"
        "Select your preferred viewing mode:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📋 DISPLAY ALL"), KeyboardButton(text="🔍 SEARCH")],
                [KeyboardButton(text="🔙 Back to Menu")]
            ], resize_keyboard=True
        )
    )
    await state.set_state(BotState.choosing_retrieval_mode) # Reusing existing generic state or create specific?
    # Actually let's use a specific one to avoid confusion with "Get Link".
    # We'll use choosing_retrieval_mode IS for Get Link. 
    # Let's use a new handler or reuse "browsing_library" state for choice?
    # Let's make a temp state or just handle it. 
    # Wait, simple way: Just handle text in a new function? 
    # But we need state. 
    # Let's use `browsing_library` but initial step.
    # actually let's define `choosing_library_mode`.
    # Avoiding adding too many states. Let's reuse `browsing_library` and check text.
    await state.set_state(BotState.browsing_library)
    await state.update_data(lib_mode="menu")

@dp.message(BotState.browsing_library)
async def handle_library_logic(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    data = await state.get_data()
    mode = data.get("lib_mode", "menu")
    
    # === MENU SELECTION ===
    if mode == "menu":
        if text == "📋 DISPLAY ALL":
            # Initialize Pagination
            await state.update_data(lib_mode="display", page=0)
            await render_library_page(message, state, page=0)
            
        elif text == "🔍 SEARCH":
            # Fetch Docs for Reference List
            docs = list(col_pdfs.find().sort("timestamp", -1))
            list_msg = ["📂 **AVAILABLE FILES**", "━━━━━━━━━━━━━━━━━━━━"]
            list_msg.extend(get_formatted_file_list(docs, limit=30)) # Show top 30 as reference
            
            list_text = "\n".join(list_msg)
            if len(list_text) > 3500: list_text = list_text[:3500] + "\n..." # Safe truncate

            await message.answer(
                f"{list_text}\n\n"
                "🔍 **SEARCH PARAMETERS**\n"
                "Enter a **Project Code** (e.g., `S19`) or **Index Number** (e.g., `1`).",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="⬅️ BACK")],
                        [KeyboardButton(text="🔙 Back to Menu")]
                    ],
                    resize_keyboard=True
                ),
                parse_mode="HTML"
            )
            await state.set_state(BotState.searching_library)
            
        else:
            await message.answer("⚠️ Invalid Option.")
            
    # === BROWSING (PAGINATION) ===
    elif mode == "display":
        current_page = data.get("page", 0)
        
        # Handle BACK button - return to mode selection
        if text == "⬅️ BACK":
            await state.update_data(lib_mode="menu")
            await show_library(message, state)
            return
        
        if text == "➡️ NEXT":
            await render_library_page(message, state, page=current_page + 1)
        elif text == "⬅️ PREV":
            await render_library_page(message, state, page=current_page - 1)
        # Handle "Back to Menu" is top level check
        elif text == "🔍 SEARCH": # Allow switching
            await state.set_state(BotState.searching_library)
            await message.answer("🔍 Enter Code/Index:")
        else:
            await message.answer("⚠️ Navigation only. Use buttons.")

async def render_library_page(message, state, page):
    limit = 20
    docs = list(col_pdfs.find().sort("timestamp", 1))
    total_docs = len(docs)
    
    # Boundary Check
    max_page = (total_docs - 1) // limit
    if page < 0: page = 0
    if page > max_page: page = max_page
    
    # Slice
    start_idx = page * limit
    end_idx = start_idx + limit
    page_docs = docs[start_idx:end_idx]
    
    # Format (Using standard)
    lines = []
    # Re-use formatted list logic manually or call helper?
    # Helper uses different index logic (always starts at 1).
    # We want absolute index.
    
    for i, doc in enumerate(page_docs):
        abs_idx = start_idx + i + 1
        code = doc.get("code")
        ts = doc.get("timestamp")
        date_str = ts.strftime('%d-%b %I:%M %p') if ts else "?"
        link = doc.get('link')
        
        # Format: 1. CODE
        if link:
            line = f"<b>{abs_idx}. {code}</b>\n<i>{date_str}</i> • <a href='{link}'>🔗 Link</a>"
        else:
            line = f"<b>{abs_idx}. {code}</b>\n<i>{date_str}</i>"
        lines.append(line)
        
    text_content = "\n".join(lines)
    if not text_content: text_content = "EMPTY PAGE"
    
    # Header
    header = (
        f"📋 **LIBRARY INDEX** (Page {page+1}/{max_page+1})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{text_content}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Total: {total_docs} Files"
    )
    
    await state.update_data(page=page)
    
    # Build Nav Buttons
    builder = ReplyKeyboardBuilder()
    row_btns = []
    if page > 0: row_btns.append(KeyboardButton(text="⬅️ PREV"))
    if page < max_page: row_btns.append(KeyboardButton(text="➡️ NEXT"))
    if row_btns: builder.row(*row_btns)
    
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    
    # Send
    await message.answer(header, reply_markup=builder.as_markup(resize_keyboard=True), parse_mode="HTML", disable_web_page_preview=True)

@dp.message(BotState.searching_library)
async def handle_library_search(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    if text == "🔙 BACK TO MENU": return await start(message, state)
    
    # Handle BACK button - return to mode selection
    if text == "⬅️ BACK":
        await show_library(message, state)
        return
    
    doc = None
    all_docs = list(col_pdfs.find().sort("timestamp", 1))
    
    # 1. Try Index
    if text.isdigit():
        idx = int(text)
        if 1 <= idx <= len(all_docs):
            doc = all_docs[idx-1]
    
    # 2. Try Code
    if not doc:
        doc = next((d for d in all_docs if d.get('code') == text), None)
        
    if doc:
        # DETAILED REPORT
        code = doc.get('code')
        link = doc.get('link', 'N/A')
        ts = doc.get('timestamp')
        date_str = ts.strftime('%d-%b-%Y %I:%M %p') if ts else "Unknown"
        restored_mark = " 🔄<b>R</b>" if doc.get('restored', False) else ""
        
        # Check basic stats if available? (Usually not stored)
        
        msg = (
            f"💎 <b>VAULT ITEM REPORT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>Code:</b> <code>{code}</code>{restored_mark}\n"
            f"📅 <b>Added:</b> <code>{date_str}</code>\n"
            f"📂 <b>Status:</b> Active\n\n"
            f"🔗 <b>Drive Link:</b>\n{link}\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        await message.answer(msg, parse_mode="HTML", disable_web_page_preview=True)
        # Stay in search state for more searches?
        await message.answer("🔍 Search another or '🔙 Back to Menu'.")
    else:
        await message.answer(f"❌ Record `{text}` not found in the Library.")

@dp.message(F.text == "📊 Storage Info")
async def storage_info(message: types.Message):
    wait_msg = await message.answer("⏳ **Running Deep System Scan...**")
    start_t = time.time()
    
    try:
        # 1. MongoDB Health & Stats
        t0 = time.time()
        stats = db_client["MSANodeDB"].command("collstats", "pdf_library")
        t_mongo = (time.time() - t0) * 1000
        
        m_count = stats.get('count', 0)
        m_used = stats.get('size', 0) / (1024 * 1024)
        m_limit = 512.0 # Cluster Limit
        m_perc = (m_used / m_limit) * 100
        
        # 2. Drive Connectivity & Quota
        service = get_drive_service()
        about = service.about().get(fields="storageQuota, user").execute()
        drive_user = about.get('user', {}).get('emailAddress', 'Unknown')
        quota = about.get('storageQuota', {})
        
        total_limit_gb = int(quota.get('limit')) / (1024**3)
        total_used_gb = int(quota.get('usage')) / (1024**3)
        total_perc = (total_used_gb / total_limit_gb) * 100
        
        # 3. Vault Recursive Audit
        target_id = PARENT_FOLDER_ID if PARENT_FOLDER_ID else 'root'
        try:
            vault_bytes, vault_count = get_folder_size(target_id)
            scan_status = "✅ Deep Scan Complete"
        except Exception as e:
            print(f"Scan Warning: {e}")
            vault_bytes, vault_count = 0, 0
            scan_status = "⚠️ Partial Scan (Permission/Network)"

        vault_mb = vault_bytes / (1024 * 1024)
        
        # 4. System Health Check
        health_score = 100
        issues = []
        if total_perc > 90: health_score -= 20; issues.append("Drive Full")
        if m_perc > 80: health_score -= 10; issues.append("DB Heavy")
        if t_mongo > 150: health_score -= 5; issues.append("High Latency")
        
        health_emoji = "🟢 Excellent"
        if health_score < 90: health_emoji = "🟡 Good"
        if health_score < 70: health_emoji = "🔴 Critical"
        
        scan_time = time.time() - start_t

        # Enhanced Stats
        pdf_bin_count = col_trash.count_documents({}) if col_trash is not None else 0
        locked_bin_count = col_trash_locked.count_documents({}) if col_trash_locked is not None else 0

        msg = (
            f"📊 **MASTER STORAGE ANALYTICS**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💚 **System Health:** {health_emoji} ({health_score}%)\n"
            f"🕒 **Scan Time:** `{scan_time:.2f}s`\n"
            f"📅 **Last Check:** `{datetime.now().strftime('%d-%b %I:%M %p')}`\n\n"
            
            f"🍃 **MONGODB CORE**\n"
            f"• Active PDFs: `{m_count}` documents\n"
            f"• PDF Bin: `{pdf_bin_count}` items\n"
            f"• Locked Bin: `{locked_bin_count}` items\n"
            f"• Storage: `{m_used:.2f} MB` / `{m_limit} MB`\n"
            f"• Latency: `{t_mongo:.1f}ms` {'⚠️ (High)' if t_mongo > 150 else '✅'}\n"
            f"`{generate_progress_bar(m_perc)}`\n\n"
            
            f"☁️ **GOOGLE DRIVE VAULT**\n"
            f"• Account: `{drive_user}`\n"
            f"• Scan Status: `{scan_status}`\n"
            f"• Total Files: `{vault_count}` documents\n"
            f"• Vault Size: `{vault_mb:.2f} MB`\n"
            f"• Location: {'`Root`' if not PARENT_FOLDER_ID else f'`Folder ID: {PARENT_FOLDER_ID[:15]}...`'}\n\n"
            
            f"💿 **GLOBAL DRIVE QUOTA**\n"
            f"• Used: `{total_used_gb:.2f} GB` / `{total_limit_gb:.0f} GB`\n"
            f"• Free: `{total_limit_gb - total_used_gb:.2f} GB`\n"
            f"`{generate_progress_bar(total_perc)}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{'✅ **ALL SYSTEMS OPERATIONAL**' if health_score == 100 else f'⚠️ **ISSUES DETECTED:** {', '.join(issues)}'}"
        )
        await wait_msg.delete()
        await message.answer(msg, parse_mode="Markdown")
        
    except Exception as e:
        if 'wait_msg' in locals(): await wait_msg.delete()
        await message.answer(f"⚠️ **Analytics Error:** `{e}`")

@dp.message(F.text == "📄 Generate PDF")
async def gen_btn(message: types.Message, state: FSMContext):
    await state.update_data(raw_script="")
    
    # Fetch last 5 codes (Newest on top)
    recent_codes = []
    try:
        last_docs = list(col_pdfs.find({}, {"code": 1, "restored": 1}).sort("timestamp", -1).limit(5))
        # User wants latest at bottom, so reverse the list of last 5
        last_docs.reverse() 
        
        for i, d in enumerate(last_docs, 1): 
            if "code" in d:
                marker = " **[R]**" if d.get('restored', False) else ""
                recent_codes.append(f"{i}. `{d['code']}`{marker}")
    except: pass
    
    msg = "🔑 **AUTHENTICATED.**\n\nEnter your **Project Code** to begin:"
    if recent_codes:
        msg += f"\n\n🕒 **Recent (Latest ↓):**\n" + "\n".join(recent_codes)
    else:
        msg += "\n\n(No recent codes found)"

    await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True), parse_mode="Markdown")
    await state.set_state(BotState.waiting_for_code)

@dp.message(BotState.waiting_for_code)
async def code_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    code = message.text.strip().upper()
    exists = col_pdfs.find_one({"code": code})
    
    if exists:
        # Strict Uniqueness Check (Requested by User)
        await message.answer(
            f"⛔ **ERROR: Code `{code}` Already Exists!**\n"
            f"This code is already in use by another project.\n\n"
            f"🔄 **Please enter a DIFFERENT Project Code:**",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
        )
        return # Stay in waiting_for_code state

    await state.update_data(code=code)
    await message.answer(
        f"🖋 **Code `{code}` Available.**\n"
        "📝 **Awaiting Content...**\nPaste your script or data now, Master.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
    )
    await state.set_state(BotState.processing_script)

# Old Overwrite Handler Removed to enforce uniqueness
# @dp.message(BotState.confirm_overwrite) ... REMOVED

@dp.message(BotState.processing_script, F.text)
async def merge_script(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    data = await state.get_data()
    updated = data.get('raw_script', '') + "\n\n" + message.text
    await state.update_data(raw_script=updated)
    
    if not data.get('timer_active'):
        await state.update_data(timer_active=True)
        
        async def auto_finish(uid, st):
            await asyncio.sleep(5)
            await finalize_pdf(uid, st)
            
        asyncio.create_task(auto_finish(message.from_user.id, state))

async def finalize_pdf(user_id, state):
    global DAILY_STATS_BOT4
    data = await state.get_data()
    code, script = data.get('code'), data.get('raw_script', '').strip()
    if not script or not code: return
    
    msg = await bot.send_message(user_id, "⏳ **Compiling Assets...**")
    filename = f"{code}.pdf"
    
    try:
        await asyncio.to_thread(create_goldmine_pdf, script, filename)
        link = await asyncio.to_thread(upload_to_drive, filename)
        
        col_pdfs.delete_many({"code": code}) 
        col_pdfs.insert_one({
            "code": code, 
            "link": link, 
            "timestamp": datetime.now()
        })
        
        await bot.send_document(
            user_id, 
            FSInputFile(filename), 
            caption=f"✅ **READY**\nCode: `{code}`\n🔗 **Link:** {link}"
        )
        
        # Track success
        DAILY_STATS_BOT4["pdfs_generated"] += 1
        
        await asyncio.sleep(2)
        if os.path.exists(filename):
            try:
                os.remove(filename)
                print(f"◈ System: {filename} purged successfully.")
            except PermissionError:
                await asyncio.sleep(3)
                try:
                    os.remove(filename)
                except:
                    print(f"◈ Warning: {filename} locked by system. Janitor will clear it later.")
                
    except Exception as e: 
        await bot.send_message(user_id, f"❌ Error: `{e}`")
        await notify_error_bot4("PDF Generation Failed", f"Code: {code} | Error: {e}")
        DAILY_STATS_BOT4["errors"] += 1
    
    await state.clear()

@dp.message(F.text == "📋 Show Library")
async def list_library(message: types.Message):
    docs = list(col_pdfs.find().sort("timestamp", 1))
    
    if not docs: 
        return await message.answer("📂 **Vault Empty.** No assets found.")
    
    seen_codes = set()
    res = ["📋 **LIBRARY INDEX (SYNCED)**", "━━━━━━━━━━━━━━━━━━━━"]
    count = 1
    
    for d in docs:
        code = d.get('code')
        if code and code not in seen_codes:
            timestamp = d.get('timestamp', datetime.now()).strftime('%d/%m')
            restored_mark = " **[R]**" if d.get('restored', False) else ""
            res.append(f"{count}. `{code}`{restored_mark} — [{timestamp}]")
            seen_codes.add(code)
            count += 1
            if count > 25: break
            
    res.append("━━━━━━━━━━━━━━━━━━━━")
    res.append("💎 *System: God-Mode filtered entries.*")
    await message.answer("\n".join(res), parse_mode="Markdown")

@dp.message(F.text == "♻️ Recycle Bin")
async def recycle_bin_btn(message: types.Message, state: FSMContext):
    await state.clear()
    
    # Count items in bin
    count = col_trash.count_documents({}) if col_trash is not None else 0
    
    if count == 0:
        await message.answer("♻️ **RECYCLE BIN IS EMPTY**", parse_mode="Markdown")
        return
    
    # Show direct action menu
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="👁️ VIEW DELETED"), KeyboardButton(text="♻️ RESTORE"))
    builder.row(KeyboardButton(text="🔥 PURGE"), KeyboardButton(text="⬅️ BACK"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        f"♻️ **RECYCLE BIN**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🗑 Items in Bin: `{count}`\n\n"
        f"Select Action:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.bin_menu)

@dp.message(BotState.bin_category_select)
async def handle_bin_category_selection(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    if text == "📂 PDF BIN":
        await state.update_data(bin_category="PDF")
        target_col = col_trash
        label = "PDF BIN"
    elif text == "🔒 LOCKED BIN":
        await state.update_data(bin_category="LOCKED")
        target_col = col_trash_locked
        label = "LOCKED CONTENT BIN"
    else:
        await message.answer("⚠️ Invalid Selection.")
        return

    # Count items
    count = target_col.count_documents({}) if target_col is not None else 0
    
    if count == 0:
        await message.answer(f"♻️ **{label} IS EMPTY**", parse_mode="Markdown")
        # Stay in category select or go back? Better stay or re-show options.
        # Actually, let's show the menu anyway so they can see it's empty, or just return.
        # Standard behavior: show menu but maybe disable actions? 
        # For consistency with previous code, if empty, just tell them.
        return

    # Show Action Menu
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="👁️ VIEW DELETED"), KeyboardButton(text="♻️ RESTORE"))
    builder.row(KeyboardButton(text="🔥 PURGE"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        f"♻️ **{label} MANAGER**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🗑 Items in Bin: `{count}`\n\n"
        f"Select Action:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.bin_menu)

@dp.message(BotState.bin_menu)
async def handle_bin_menu_selection(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu" or text == "⬅️ BACK": return await start(message, state)
    
    # Always use col_trash (Locked bin removed)
    
    docs = list(col_trash.find().sort("deleted_at", 1))
    
    if text == "👁️ VIEW DELETED":
         # Use Pagination
         if not docs:
             await message.answer("🗑 **BIN IS EMPTY**")
             return
         await state.update_data(bin_page=0, bin_mode="view")
         await render_bin_page(message, state, page=0)
         await state.set_state(BotState.bin_viewing)
         
    elif text == "♻️ RESTORE":
         await ask_method(message, state, "RESTORE")
         
    elif text == "🔥 PURGE":
         await ask_method(message, state, "PURGE")
    
    else:
        await message.answer("⚠️ Invalid Option.")

async def ask_method(message, state, action):
    await state.update_data(bin_action=action)
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="👤 SINGLE"), KeyboardButton(text="🔢 BULK RANGE"))
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        f"⚙️ **{action} MODE SELECTED**\n"
        f"Select Method:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.bin_choosing_method)

@dp.message(BotState.bin_choosing_method)
async def handle_bin_method_selection(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # Handle BACK button - return to bin menu
    if text == "⬅️ BACK":
        await recycle_bin_btn(message, state)
        return
    
    data = await state.get_data()
    action = data.get("bin_action", "RESTORE")
    
    # Fetch all bin items
    docs = list(col_trash.find().sort("deleted_at", 1))
    
    if not docs:
        await message.answer("🗑 **BIN IS EMPTY**")
        return
    
    # Build available items list
    lines = ["♻️ **AVAILABLE ITEMS IN BIN**", "━━━━━━━━━━━━━━━━━━━━"]
    for i, doc in enumerate(docs, 1):
        code = doc.get('code', 'Unknown')
        deleted_at = doc.get('deleted_at')
        time_str = deleted_at.strftime('%d-%b %I:%M %p') if deleted_at else "Unknown"
        lines.append(f"{i}. `{code}` • 🗑 {time_str}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📊 Total: {len(docs)} Items\n")
    
    # Show list first
    await message.answer("\n".join(lines), parse_mode="Markdown")
    
    if text == "👤 SINGLE":
        # Create buttons for each available item
        builder = ReplyKeyboardBuilder()
        for i, doc in enumerate(docs, 1):
            code = doc.get('code', 'Unknown')
            builder.add(KeyboardButton(text=f"{i}"))
        builder.adjust(5)  # 5 buttons per row
        builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
        
        await message.answer(
            f"👤 **SINGLE {action}**\n"
            f"Select the **Index** to {action.lower()}:",
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="Markdown"
        )
        if action == "RESTORE": await state.set_state(BotState.bin_restoring)
        else: await state.set_state(BotState.bin_purging)
        
    elif text == "🔢 BULK RANGE":
        await message.answer(
            f"🔢 **BULK {action}**\n"
            f"Enter the **Index Range** (e.g. `1-{len(docs)}`) to {action.lower()}.\n"
            "Index 1 = Oldest Deleted Item.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="⬅️ BACK")],
                    [KeyboardButton(text="🔙 Back to Menu")]
                ],
                resize_keyboard=True
            ),
            parse_mode="Markdown"
        )
        if action == "RESTORE": await state.set_state(BotState.bin_restoring)
        else: await state.set_state(BotState.bin_purging)
        
    else:
        await message.answer("⚠️ Invalid.")

@dp.message(BotState.bin_viewing)
async def handle_bin_nav(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # Handle BACK button - return to bin menu
    if text == "⬅️ BACK":
        await recycle_bin_btn(message, state)
        return
    
    data = await state.get_data()
    page = data.get("bin_page", 0)
    
    if text == "➡️ NEXT":
        await render_bin_page(message, state, page + 1)
    elif text == "⬅️ PREV":
        await render_bin_page(message, state, page - 1)
    else:
        await message.answer("⚠️ Navigation only.")

async def render_bin_page(message, state, page):
    limit = 20
    # Always use col_trash
    docs = list(col_trash.find().sort("deleted_at", 1))
    total_docs = len(docs)
    
    max_page = (total_docs - 1) // limit
    if page < 0: page = 0
    if page > max_page: page = max_page
    
    start_idx = page * limit
    end_idx = start_idx + limit
    page_docs = docs[start_idx:end_idx]
    
    lines = []
    for i, doc in enumerate(page_docs):
        idx = start_idx + i + 1
        code = doc.get('code')
        ts = doc.get("deleted_at")
        # 12h Format Request: 12 am pm format
        date_str = ts.strftime('%d-%b %I:%M %p') if ts else "?"
        lines.append(f"<b>{idx}. {code}</b>\n❌ <i>Deleted: {date_str}</i>")
        
    content = "\n".join(lines)
    
    header = (
        f"🗑 **DELETED ITEMS** (Page {page+1}/{max_page+1})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{content}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"items: {total_docs}"
    )
    
    await state.update_data(bin_page=page)
    
    builder = ReplyKeyboardBuilder()
    row = []
    if page > 0: row.append(KeyboardButton(text="⬅️ PREV"))
    if page < max_page: row.append(KeyboardButton(text="➡️ NEXT"))
    if row: builder.row(*row)
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(header, reply_markup=builder.as_markup(resize_keyboard=True), parse_mode="HTML")

@dp.message(BotState.bin_restoring)
async def handle_bin_restore(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # Handle BACK button - return to bin menu
    if text == "⬅️ BACK":
        await recycle_bin_btn(message, state)
        return
    
    # Range Logic
    try:
        if "-" in text:
            s, e = map(int, text.split("-"))
        elif text.isdigit():
            s = int(text)
            e = s
        else:
            await message.answer("⚠️ Enter valid index or range (e.g. `1` or `1-5`).")
            return
            
        # Bounds check
        docs = list(col_trash.find().sort("deleted_at", 1))
        
        if s < 1 or e > len(docs) or s > e:
            await message.answer(
                f"❌ **Invalid Range**\n"
                f"Valid range: 1-{len(docs)}\n"
                f"Please enter a valid index or range.",
                parse_mode="Markdown"
            )
            return
            
        targets = docs[s-1 : e]
        
        await message.answer(f"⏳ Restoring {len(targets)} items...")
        
        count = 0
        for doc in targets:
            # RESTORE LOGIC
            restore_doc = doc.copy()
            if "_id" in restore_doc: del restore_doc["_id"]
            if "deleted_at" in restore_doc: del restore_doc["deleted_at"]
            
            # MARK AS RESTORED (Highlight)
            restore_doc['restored'] = True
            
            try:
                col_pdfs.insert_one(restore_doc)
                col_trash.delete_one({"_id": doc["_id"]})
                count += 1
            except Exception as e:
                print(f"Restore Error: {e}")
                
        await message.answer(f"✅ **SUCCESS:** {count} items restored to PDF Library.", parse_mode="Markdown")
        await start(message, state)
        
    except ValueError:
        await message.answer("⚠️ Invalid number format.")

@dp.message(BotState.bin_purging)
async def handle_bin_purge(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # Store range for confirmation
    try:
        if "-" in text:
            s, e = map(int, text.split("-"))
        elif text.isdigit():
            s = int(text)
            e = s
        else:
             await message.answer("⚠️ Invalid.")
             return
        
        await state.update_data(purge_range=(s, e))
        
        # CONFIRMATION
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="✅ CONFIRM PERMANENT DELETE"), KeyboardButton(text="❌ CANCEL"))
        builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
        
        await message.answer(
            f"🔥 **CONFIRM DESTRUCTION**\n"
            f"You are about to delete items `{s}-{e}` FOREVER.\n"
            f"Are you sure?",
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="Markdown"
        )
        await state.set_state(BotState.bin_confirm_purge)
        
    except:
        await message.answer("⚠️ Error parsing range.")

@dp.message(BotState.bin_confirm_purge)
async def process_purge_confirm(message: types.Message, state: FSMContext):
    text = message.text
    if text == "❌ CANCEL": return await start(message, state)
    
    # Handle BACK button - return to bin menu  
    if text == "⬅️ BACK":
        await recycle_bin_btn(message, state)
        return
    
    if text == "✅ CONFIRM PERMANENT DELETE":
        data = await state.get_data()
        s, e = data.get('purge_range')
        
        # Execute - always use col_trash
        docs = list(col_trash.find().sort("deleted_at", 1))
        targets = docs[s-1 : e]
        
        msg = await message.answer(f"🔥 Purging {len(targets)} items from Drive & DB...")
        
        # Drive Service
        try:
            service = get_drive_service()
            bin_id = get_recycle_bin_id(service)
        except: service = None
        
        count = 0
        for doc in targets:
            # 1. DB Delete
            col_trash.delete_one({"_id": doc["_id"]})
            
            # 2. Drive Delete
            if service:
                try:
                    fname = f"{doc['code']}.pdf"
                    q = f"name = '{fname}' and '{bin_id}' in parents and trashed = false"
                    res = service.files().list(q=q, fields="files(id)").execute()
                    files = res.get('files', [])
                    for f in files:
                        service.files().delete(fileId=f['id']).execute()
                except: pass
            count += 1
            
        await msg.edit_text(f"🔥 **PURGE COMPLETE**\nDeleted {count} items.")
        await asyncio.sleep(2)
        await start(message, state)
    else:
        await message.answer("Use buttons.")

def get_bin_formatted_list(docs, limit=30):
    if not docs: return ["_Bin is empty._"]
    lines = []
    for idx, doc in enumerate(docs[:limit], 1):
         code = doc.get('code')
         ts = doc.get('deleted_at') # Use deleted time
         date_str = ts.strftime('%d-%b %I:%M %p') if ts else "?"
         line = f"<b>{idx}. {code}</b>\n❌ <i>Deleted: {date_str}</i>"
         lines.append(line)
    return lines

@dp.message(F.text == "✏️ Edit PDF")
async def edit_btn(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔢 BY INDEX"), KeyboardButton(text="🆔 BY CODE"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "✏️ **EDIT PROTOCOL INITIATED**\n"
        "Select Selection Mode to Rename File:\n\n"
        "🔢 **BY INDEX**: Select by position (e.g. 1 = Newest).\n"
        "🆔 **BY CODE**: Select by Code Button.",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(BotState.choosing_edit_mode)

@dp.message(BotState.choosing_edit_mode)
async def handle_edit_mode(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # helper to generate list
    docs = list(col_pdfs.find().sort("timestamp", -1))
    
    list_msg = ["📋 **AVAILABLE PDFS**", "━━━━━━━━━━━━━━━━━━━━"]
    list_msg.extend(get_formatted_file_list(docs))
            
    list_text = "\n".join(list_msg)
    
    if text == "🔢 BY INDEX":
        await message.answer(
            f"{list_text}\n\n"
            "🔢 **INDEX SELECTION**\n"
            "Enter the **Index Number** from the list above (e.g. `1`).",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="⬅️ BACK")],
                    [KeyboardButton(text="🔙 Back to Menu")]
                ],
                resize_keyboard=True
            ),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.update_data(edit_mode="index")
        await state.set_state(BotState.waiting_for_edit_target)
        
    elif text == "🆔 BY CODE":
        builder = ReplyKeyboardBuilder()
        existing_codes = []
        for d in docs[:50]:
            code = d.get('code')
            if code and code not in existing_codes:
                builder.add(KeyboardButton(text=code))
                existing_codes.append(code)
        
        builder.adjust(3)
        builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
        
        await message.answer(
            f"{list_text}\n\n"
            "🆔 **CODE SELECTION**\n"
            "Select the Code you wish to Rename:",
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.update_data(edit_mode="code")
        await state.set_state(BotState.waiting_for_edit_target)
    else:
        await message.answer("⚠️ Invalid Option.")

@dp.message(BotState.waiting_for_edit_target)
async def select_edit_target(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    if text == "🔙 BACK TO MENU": return await start(message, state)
    
    # Handle BACK button - return to edit mode selection
    if text == "⬅️ BACK":
        await edit_btn(message, state)
        return
    
    data = await state.get_data()
    mode = data.get('edit_mode', 'code')
    doc = None
    
    if mode == 'index':
        if not text.isdigit():
            await message.answer("⚠️ Enter a valid number (e.g. 1).")
            return
        idx = int(text)
        if idx < 1:
            await message.answer("⚠️ Index must be 1 or greater.")
            return
            
        all_docs = list(col_pdfs.find().sort("timestamp", -1))
        if idx > len(all_docs):
            await message.answer(f"❌ Index {idx} not found. Max is {len(all_docs)}.")
            return
        doc = all_docs[idx-1]
        
    else:
        # Code mode
        doc = col_pdfs.find_one({"code": text})
        if not doc:
            await message.answer(f"❌ Code `{text}` not found.")
            return

    # Doc found, ask for new name
    old_code = doc.get('code')
    await state.update_data(target_doc_id=str(doc['_id']), old_code=old_code)
    
    await message.answer(
        f"📝 **EDITING: `{old_code}`**\n"
        f"Enter the **NEW UNIQUE CODE** for this file:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⬅️ BACK")],
                [KeyboardButton(text="🔙 Back to Menu")]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(BotState.waiting_for_new_code)

@dp.message(BotState.waiting_for_new_code)
async def save_new_code(message: types.Message, state: FSMContext):
    new_code = message.text.strip().upper()
    if new_code == "🔙 BACK TO MENU": return await start(message, state)
    
    # Handle BACK button - return to edit target selection
    if new_code == "⬅️ BACK":
        data = await state.get_data()
        edit_mode = data.get('edit_mode', 'index')
        # Re-trigger the mode
        message.text = "🔢 BY INDEX" if edit_mode == "index" else "🆔 BY CODE"
        await handle_edit_mode(message, state)
        return
    
    # Validation
    if not new_code: return await message.answer("⚠️ Code cannot be empty.")
    
    # Check uniqueness
    if col_pdfs.find_one({"code": new_code}):
        await message.answer(f"⚠️ Code `{new_code}` already exists! Choose another.")
        return
        
    data = await state.get_data()
    old_code = data.get('old_code')
    doc_id = data.get('target_doc_id')
    
    msg = await message.answer(f"⏳ **RENAMING: `{old_code}` ➡️ `{new_code}`...**")
    
    # 1. Drive Rename
    old_filename = f"{old_code}.pdf"
    new_filename = f"{new_code}.pdf"
    
    drive_res = await asyncio.to_thread(rename_file_in_drive, old_filename, new_filename)
    
    # 2. DB Update
    from bson.objectid import ObjectId
    col_pdfs.update_one(
        {"_id": ObjectId(doc_id)}, 
        {"$set": {"code": new_code, "filename": new_code}} # Assuming we want to sync filename too if used
    )
    
    status = "☁️ Drive: Renamed" if drive_res else "☁️ Drive: Not Found (DB Only Revised)"
    
    await msg.edit_text(
        f"✅ **SUCCESSFULLY RENAMED**\n"
        f"Old: `{old_code}`\n"
        f"New: `{new_code}`\n"
        f"{status}\n\n"
        f"Enter next command or '🔙 Back to Menu'."
    )
    # Return to Menu logic? 
    # User usually wants to stop editing after one rename.
    # But sticking to "State Persistance" rule:
    # await message.answer("✏️ Select next Edit Mode or '🔙 Back to Menu'.", reply_markup=get_main_menu())
    await state.clear() # Reset state since we are back at menu level essentially, or revert to choosing_edit_mode?
    # Actually, let's keep them in the Edit Menu flow?
    # But `save_new_code` finishes the specific task.
    # Let's show the Edit Menu again so they can pick another file?
    # Calling edit_btn logic manually:
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔢 BY INDEX"), KeyboardButton(text="🆔 BY CODE"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    await message.answer("Select Mode to Edit Another:", reply_markup=builder.as_markup(resize_keyboard=True))
    await state.set_state(BotState.choosing_edit_mode)

@dp.message(F.text == "🔗 Get Link")
async def link_btn(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📄 GET PDF FILE"), KeyboardButton(text="🔗 GET DRIVE LINK"))
    builder.row(KeyboardButton(text="🏠 MAIN MENU"))
    
    await message.answer(
        "🎛 **SELECT RETRIEVAL FORMAT:**\n\n"
        "📄 **GET PDF FILE**: Downloads and sends the actual file.\n"
        "🔗 **GET DRIVE LINK**: Sends the secure Google Drive URL.",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(BotState.choosing_retrieval_mode)

@dp.message(BotState.choosing_retrieval_mode)
async def handle_mode_selection(message: types.Message, state: FSMContext):
    if message.text == "🏠 MAIN MENU": return await start(message, state) # Reset
    if message.text == "⬅️ BACK": return await link_btn(message, state) # Back
    
    mode = "link"
    if "PDF" in message.text: mode = "pdf"
    
    await state.update_data(retrieval_mode=mode)
    
    # Step 2: Choose Method (Single vs Bulk)
    # Check for empty FIRST (as requested)
    count = col_pdfs.count_documents({})
    if count == 0:
        await message.answer("📭 **VAULT IS EMPTY**", parse_mode="Markdown")
        return await start(message, state)

    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="👤 SINGLE FILE"), KeyboardButton(text="🔢 BULK RANGE"))
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🏠 MAIN MENU"))
    
    mode_text = "PDF FILE" if mode == "pdf" else "DRIVE LINK"
    await message.answer(
        f"📂 <b>MODE SELECTED: {mode_text}</b>\n"
        f"📊 Available Files: {count}\n\n"
        f"How would you like to retrieve them?", 
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )
    await state.set_state(BotState.choosing_retrieval_method)

@dp.message(BotState.choosing_retrieval_method)
async def handle_retrieval_method_selection(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🏠 MAIN MENU": return await start(message, state)
    if text == "⬅️ BACK": 
        # Re-trigger Step 1 Logic manually to go back
        # We need the message text "PDF" or "LINK" to simulate it? 
        # Easier: Recall handle_mode_selection? No, that expects "PDF" text.
        # Better: Recall link_btn to restart? No, that goes too far back.
        # We want to go back to "Choose Single/Bulk"? NO, we ARE at Single/Bulk.
        # Wait, this handler IS the selection. So "Back" goes to Step 1 (Mode Selection)?
        # Actually, "Back" here means "I want to change PDF/LINK mode".
        return await link_btn(message, state)

    # FETCH DOCS FOR DISPLAY
    docs = list(col_pdfs.find().sort("timestamp", -1))
    
    # Generate List Helper
    # FIX: Increase limit to 50 to match buttons
    list_msg = ["📂 **AVAILABLE FILES**", "━━━━━━━━━━━━━━━━━━━━"]
    list_msg.extend(get_formatted_file_list(docs, limit=50))
        
    list_text = "\n".join(list_msg)
    
    # FIX: Safe Split Logic
    if len(list_text) > 4000:
        parts = [list_text[i:i+4000] for i in range(0, len(list_text), 4000)]
        for part in parts:
            await message.answer(part, parse_mode="HTML", disable_web_page_preview=True)
    else:
        await message.answer(list_text, parse_mode="HTML", disable_web_page_preview=True)

    if text == "🔢 BULK RANGE":
         await message.answer(
            "🔢 <b>BULK RETRIEVAL MODE</b>\n"
            "Enter the index range of PDFs you need (e.g., `1-5`, `10-20`).\n"
            "Index 1 = Newest PDF.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🏠 MAIN MENU")]], resize_keyboard=True),
            parse_mode="HTML"
        )
         await state.set_state(BotState.waiting_for_range)
         
    elif text == "👤 SINGLE FILE":
        builder = ReplyKeyboardBuilder()
        existing_codes = []
        for idx, d in enumerate(docs[:50], 1):
            code = d.get('code')
            if code and code not in existing_codes:
                # ADD INDEX TO BUTTON TEXT: "1. CODE"
                btn_text = f"{idx}. {code}"
                builder.add(KeyboardButton(text=btn_text))
                existing_codes.append(code)
        
        builder.adjust(3)
        builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🏠 MAIN MENU"))
        
        await message.answer(
            "👤 <b>SINGLE RETRIEVAL MODE</b>\n"
            "Select a Project Code below:",
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="HTML"
        )
        await state.set_state(BotState.fetching_link)
        
    else:
        await message.answer("⚠️ Invalid Option.")

@dp.message(BotState.fetching_link)
async def fetch_link(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    
    if text == "🏠 MAIN MENU": return await start(message, state)
    if text == "⬅️ BACK": 
        # Return to Method Selection
        # We need to simulate the message for handle_retrieval_method_selection?
        # No, we need to RE-SHOW the method prompt.
        # But `handle_mode_selection` does that. 
        # But `handle_mode_selection` needs "PDF" or "LINK" text to work.
        # So we check state data.
        data = await state.get_data()
        mode = data.get('retrieval_mode', 'link')
        msg = types.Message(message_id=0, date=datetime.now(), chat=message.chat, from_user=message.from_user, text="PDF" if mode=="pdf" else "LINK") # Hacky simulation
        # Better: Refactor `handle_mode_selection` to use data if text is invalid?
        # Or just manually call the logic:
        
        count = col_pdfs.count_documents({})
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="👤 SINGLE FILE"), KeyboardButton(text="🔢 BULK RANGE"))
        builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🏠 MAIN MENU"))
        
        mode_text = "PDF FILE" if mode == "pdf" else "DRIVE LINK"
        await message.answer(
            f"📂 <b>MODE SELECTED: {mode_text}</b>\n"
            f"📊 Available Files: {count}\n\n"
            f"How would you like to retrieve them?", 
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="HTML"
        )
        await state.set_state(BotState.choosing_retrieval_method)
        return

    # HANDLE BUTTON CLICK (e.g. "1. CODE")
    # Clean input to get just CODE
    if ". " in text:
        try:
            _, clean_code = text.split(". ", 1)
            text = clean_code.strip()
        except: pass
    
    # === BULK RANGE MODE ===
    
    # === BULK RANGE MODE ===
    if text == "🔢 BULK RANGE":
        # ... logic ...
        pass # Fallthrough if existing logic handles this, checking next Step

        # Generate List Helper
        docs = list(col_pdfs.find().sort("timestamp", -1))
        list_msg = ["🔢 **AVAILABLE INDICES**", "━━━━━━━━━━━━━━━━━━━━"]
        list_msg.extend(get_formatted_file_list(docs))
        
        list_text = "\n".join(list_msg)
        
        await message.answer(
            f"{list_text}\n\n"
            "🔢 <b>BULK RETRIEVAL MODE</b>\n"
            "Enter the index range of PDFs you need (e.g., `1-5`, `10-20`).\n"
            "Index 1 = Newest PDF.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.set_state(BotState.waiting_for_range)
        return

    # === SINGLE RETRIEVAL MODE (User clicked a code button) ===
    doc = col_pdfs.find_one({"code": text}, sort=[("timestamp", -1)])
    
    if doc:
        data = await state.get_data()
        mode = data.get('retrieval_mode', 'link')
        
        if mode == 'pdf':
            wait_msg = await message.answer(f"⏳ **Fetching PDF: `{text}`...**")
            filename = f"{text}.pdf"
            
            try:
                # Attempt to download from Drive
                local_path = await asyncio.to_thread(download_from_drive, filename)
                
                if local_path and os.path.exists(local_path):
                    await bot.send_document(message.from_user.id, FSInputFile(local_path), caption=f"📄 **FILE ACQUIRED**\nCode: `{text}`")
                    await wait_msg.delete()
                    try: os.remove(local_path) 
                    except: pass
                else:
                    await wait_msg.edit_text(f"❌ Error: File `{filename}` not found in Drive Vault.")
            except Exception as e:
                await wait_msg.edit_text(f"❌ Download Failed: {e}")
                
        else:
            # Link Mode
            await message.answer(f"✅ **RESOURCE ACQUIRED**\nCode: `{doc.get('code')}`\n🔗 {doc.get('link')}")
            
    else:
        # If they typed something random that isn't a code
        await message.answer(f"❌ Code `{text}` not found. Select from the buttons or try again.")

@dp.message(BotState.waiting_for_range)
async def process_bulk_range(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    if text == "🏠 MAIN MENU": return await start(message, state)
    if text == "⬅️ BACK": 
        # Return to Method Selection
        count = col_pdfs.count_documents({})
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="👤 SINGLE FILE"), KeyboardButton(text="🔢 BULK RANGE"))
        builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🏠 MAIN MENU"))
        
        data = await state.get_data()
        mode = data.get('retrieval_mode', 'link')
        mode_text = "PDF FILE" if mode == "pdf" else "DRIVE LINK"
        
        await message.answer(
            f"📂 <b>MODE SELECTED: {mode_text}</b>\n"
            f"📊 Available Files: {count}\n\n"
            f"How would you like to retrieve them?", 
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="HTML"
        )
        await state.set_state(BotState.choosing_retrieval_method)
        return
    
    try:
        # Parse "1-5" or just "1"
        if "-" in text:
            start_idx, end_idx = map(int, text.split('-'))
        elif text.isdigit():
            start_idx = int(text)
            end_idx = start_idx
        else:
            await message.answer("⚠️ Invalid format. Please enter a number (e.g. `1`) or range (e.g. `1-5`).")
            return
        
        if start_idx < 1 or end_idx < start_idx:
            await message.answer("⚠️ Invalid range logic.")
            return

        # Fetch all docs sorted by timestamp (Newest first)
        all_docs = list(col_pdfs.find().sort("timestamp", -1))
        total_docs = len(all_docs)
        
        # STRICT BOUNDS CHECK
        if end_idx > total_docs:
             await message.answer(
                 f"❌ **RANGE OUT OF BOUNDS**\n"
                 f"You requested up to `{end_idx}`, but only `{total_docs}` files exist.\n"
                 f"✅ Valid Range: `1-{total_docs}`",
                 parse_mode="Markdown"
             )
             return
             
        # SLICE LOGIC:
        # User 1-based inclusive. Python 0-based.
        # Start: 1 -> idx 0 (Start-1)
        # End: 2 -> idx 1. Slice [0:2] -> 0, 1. Correct.
        
        selected_docs = all_docs[start_idx-1 : end_idx]
        
        if not selected_docs:
            await message.answer(f"❌ No documents found in range {start_idx}-{end_idx} (Total: {total_docs}).")
            return
            
        data = await state.get_data()
        mode = data.get('retrieval_mode', 'link')
        
        if mode == 'pdf':
            # === BULK PDF MODE ===
            await message.answer(f"📦 **BULK DOWNLOAD INITIATED ({len(selected_docs)} files)...**\nPlease wait.")
            
            count = 0 
            for doc in selected_docs:
                code = doc.get('code')
                filename = f"{code}.pdf"
                try:
                    local_path = await asyncio.to_thread(download_from_drive, filename)
                    if local_path and os.path.exists(local_path):
                        await bot.send_document(message.from_user.id, FSInputFile(local_path), caption=f"Code: `{code}`")
                        count += 1
                        try: os.remove(local_path) 
                        except: pass
                        await asyncio.sleep(1) # Prevent flood wait
                except: continue
                
            await message.answer(f"✅ **Delivered {count}/{len(selected_docs)} files.**")
            
        else:
            # === BULK LINK MODE ===
            report = [f"🔢 **BULK DUMP: {start_idx}-{end_idx}**", "━━━━━━━━━━━━━━━━━━━━"]
            
            for i, doc in enumerate(selected_docs):
                current_num = start_idx + i
                report.append(f"**{current_num}. {doc.get('code')}**")
                report.append(f"🔗 {doc.get('link')}")
                report.append("") 
                
            report.append("━━━━━━━━━━━━━━━━━━━━")
            
            full_msg = "\n".join(report)
            if len(full_msg) > 4000:
                chunks = [full_msg[i:i+4000] for i in range(0, len(full_msg), 4000)]
                for chunk in chunks:
                    await message.answer(chunk, disable_web_page_preview=True)
            else:
                await message.answer(full_msg, disable_web_page_preview=True)
            
        await message.answer("💎 **Operation Complete.** Enter another range or click '🔙 Back to Menu'.")
        
    except ValueError:
        await message.answer("⚠️ Error: Please enter numeric values like `1-5`.")
    except Exception as e:
        await message.answer(f"❌ Error: {e}")

@dp.message(F.text == "🗑 Remove PDF")
async def remove_btn(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔢 DELETE BY RANGE"), KeyboardButton(text="🆔 DELETE BY CODE"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "🗑 **DELETION PROTOCOL INITIATED**\n"
        "Select Deletion Mode:\n\n"
        "🔢 **DELETE BY RANGE**: Delete multiple files (e.g., 1-5).\n"
        "🆔 **DELETE BY CODE**: Delete a specific code (e.g., P1).",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(BotState.choosing_delete_mode)

@dp.message(BotState.choosing_delete_mode)
async def handle_delete_mode(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # Generate List Helper
    docs = list(col_pdfs.find().sort("timestamp", -1))
    list_msg = ["🗑 **FILES AVAILABLE FOR DELETION**", "━━━━━━━━━━━━━━━━━━━━"]
    list_msg.extend(get_formatted_file_list(docs))
            
    list_text = "\n".join(list_msg)
    
    if text == "🔢 DELETE BY RANGE":
        await message.answer(
            f"{list_text}\n\n"
            "🔢 <b>BULK DELETE MODE</b>\n"
            "Enter range to purge (e.g., `1-5`).\n"
            "⚠️ <b>WARNING</b>: This deletes from Database AND Google Drive.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.update_data(delete_mode="range")
        await state.set_state(BotState.deleting_pdf)
        
    elif text == "🆔 DELETE BY CODE":
        # Fetch available codes for buttons
        builder = ReplyKeyboardBuilder()
        existing_codes = []
        for d in docs[:50]:
            code = d.get('code')
            if code and code not in existing_codes:
                builder.add(KeyboardButton(text=code))
                existing_codes.append(code)
        
        builder.adjust(3)
        builder.row(KeyboardButton(text="🔙 Back to Menu"))
        
        await message.answer(
            f"{list_text}\n\n"
            "🆔 <b>SINGLE DELETE MODE</b>\n"
            "Select a Code button below or type one (e.g., `P1`).",
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.update_data(delete_mode="code")
        await state.set_state(BotState.deleting_pdf)
    else:
        await message.answer("⚠️ Invalid Option. use buttons.")

@dp.message(BotState.deleting_pdf)
async def process_deletion(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    if text == "🔙 BACK TO MENU": return await start(message, state)
    
    data = await state.get_data()
    mode = data.get('delete_mode', 'code')
    
    if mode == 'code':
        # Single Deletion - Ask for Confirmation
        code = text
        await state.update_data(target_code=code)
        
        # Confirmation Keyboard
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="✅ YES, DELETE"), KeyboardButton(text="❌ CANCEL"))
        builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
        
        await message.answer(
            f"❓ **CONFIRM DELETION**\n"
            f"Are you sure you want to permanently delete **{code}** from Database and Drive?",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(BotState.confirm_delete)

    else:
        # Range Deletion (Keep existing logic for now, or add confirmation? Let's add simple confirmation)
        # Actually user specifically asked for "click button confirm". 
        # Range relies on text input. Single relies on buttons.
        # Let's just implement confirmation for EVERYTHING.
        pass # To be continued in next edit if needed, but for now focusing on Code mode changes.
        
        # ... Wait, I can't leave 'pass'. I need to keep the Range logic functioning.
        # Let's just update the Code block first.
        
        try:
            # Parse Range
            if "-" in text:
                start_idx, end_idx = map(int, text.split('-'))
            elif text.isdigit():
                start_idx = int(text)
                end_idx = start_idx
            else:
                await message.answer("⚠️ Invalid format. Use `1-5`.")
                return

            if start_idx < 1 or end_idx < start_idx:
                await message.answer("⚠️ Invalid range logic.")
                return
            
            # Fetch docs
            all_docs = list(col_pdfs.find().sort("timestamp", -1))
            selected_docs = all_docs[start_idx-1 : end_idx]
            
            if not selected_docs:
                await message.answer("❌ No documents in that range.")
                return
            
            # Store target docs for confirmation
            await state.update_data(target_range_indices=[start_idx, end_idx], target_range_len=len(selected_docs))
            
            builder = ReplyKeyboardBuilder()
            builder.row(KeyboardButton(text="✅ YES, DELETE"), KeyboardButton(text="❌ CANCEL"))
            
            await message.answer(
                f"❓ **CONFIRM BULK DELETION**\n"
                f"Range: {start_idx}-{end_idx}\n"
                f"Files to purge: **{len(selected_docs)}**\n"
                f"This cannot be undone.",
                reply_markup=builder.as_markup(resize_keyboard=True)
            )
            await state.set_state(BotState.confirm_delete)
            
        except ValueError:
            await message.answer("⚠️ Error: Use numeric format `1-5`.")
        except Exception as e:
            await message.answer(f"❌ Error: {e}")

@dp.message(BotState.confirm_delete)
async def execute_deletion(message: types.Message, state: FSMContext):
    text = message.text.upper()
    data = await state.get_data()
    mode = data.get('delete_mode', 'code')
    
    # Handle BACK button - return to delete mode
    if text == "⬅️ BACK":
        await remove_btn(message, state)
        return
    
    if text == "❌ CANCEL":
        await message.answer("� **DELETION ABORTED.**\nNo files were touched.")
        
        # Helper to re-show menu based on mode
        if mode == 'code':
            # Re-fetch buttons
            docs = list(col_pdfs.find().sort("timestamp", -1))
            builder = ReplyKeyboardBuilder()
            existing_codes = []
            for d in docs[:50]:
                code = d.get('code')
                if code and code not in existing_codes:
                    builder.add(KeyboardButton(text=code))
                    existing_codes.append(code)
            builder.adjust(3)
            builder.row(KeyboardButton(text="🔙 Back to Menu"))
            await message.answer("🆔 **Select Code to Delete:**", reply_markup=builder.as_markup(resize_keyboard=True))
            await state.set_state(BotState.deleting_pdf)
        else:
            await message.answer("🔢 **Enter range to purge (e.g. 1-5):**", 
                                 reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True))
            await state.set_state(BotState.deleting_pdf)
        return

    if text == "✅ YES, DELETE":
        if mode == 'code':
            code = data.get('target_code')
            msg = await message.answer(f"⏳ **MOVING TO RECYCLE BIN: `{code}`...**")
            
            # 1. Drive Move
            filename = f"{code}.pdf"
            drive_res = await asyncio.to_thread(move_to_recycle_bin, filename)
            
            # 2. MongoDB Move (Copy to Trash -> Delete from Library)
            doc = col_pdfs.find_one({"code": code})
            if doc:
                doc['deleted_at'] = datetime.now()  # Add timestamp
                col_trash.insert_one(doc)
                col_pdfs.delete_one({"_id": doc['_id']})
                db_res = True
            else:
                db_res = False
            
            status = []
            if drive_res: status.append("☁️ Drive: Moved to Bin")
            else: status.append("☁️ Drive: Not Found")
            
            if db_res: status.append("🍃 DB: Moved to Bin")
            else: status.append("🍃 DB: Not Found")
            
            await msg.edit_text(
                f"♻️ **REYCLED: `{code}`**\n" + "\n".join(status)
            )
        else:
            # Range Deletion
            indices = data.get('target_range_indices')
            start_idx, end_idx = indices
            
            msg = await message.answer(f"⏳ **EXECUTING BULK RECYCLE...**")
            
            all_docs = list(col_pdfs.find().sort("timestamp", -1))
            selected_docs = all_docs[start_idx-1 : end_idx]
            
            moved_count = 0
            for doc in selected_docs:
                code = doc.get('code')
                # Drive
                await asyncio.to_thread(move_to_recycle_bin, f"{code}.pdf")
                # DB
                doc['deleted_at'] = datetime.now()  # Add timestamp
                col_trash.insert_one(doc)
                col_pdfs.delete_one({"_id": doc['_id']})
                moved_count += 1
            
            await msg.edit_text(f"♻️ **BULK RECYCLE COMPLETE**\nMoved {moved_count} files to Bin.")
            
        # Re-Show Menu
        if mode == 'code':
            await asyncio.sleep(1)
            # Re-fetch buttons
            docs = list(col_pdfs.find().sort("timestamp", -1))
            builder = ReplyKeyboardBuilder()
            existing_codes = []
            for d in docs[:50]:
                code = d.get('code')
                if code and code not in existing_codes:
                    builder.add(KeyboardButton(text=code))
                    existing_codes.append(code)
            builder.adjust(3)
            builder.row(KeyboardButton(text="🔙 Back to Menu"))
            await message.answer("🆔 Select next Code or '🔙 Back to Menu'.", reply_markup=builder.as_markup(resize_keyboard=True))
        else:
            await message.answer("🔢 Enter next range or '🔙 Back to Menu'.",
                                 reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True))
        
        await state.set_state(BotState.deleting_pdf)
    else:
        await message.answer("⚠️ Please select YES or CANCEL.")



@dp.message(F.text == "\U0001F48E Elite Help")
async def send_elite_help(message: types.Message):
    # Split into 2 messages if needed, but we'll try to pack it densly first.
    # We will use a standard "Part 1" and "Part 2" approach if it gets too long, 
    # but for now, let's try a single comprehensive prompt.
    
    help_text = (
        "\U0001F48E <b>MSANODE GOD-MODE PREMIER MANUAL</b>\n"
        "<i>Classified Operational Protocol v5.0 (Ultimate)</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>💎 1. PDF GENERATION ENGINE</b>\n"
        "• <b>Input:</b> Paste text or script. Code = <code>S19</code> style.\n"
        "• <b>\U0001F4C4 Generate:</b> Auto-merges messages + Deduplication check.\n"
        "• <b>Formatting:</b>\n"
        "   - <code>******link******</code> → 🔵 Clickable Blue Link\n"
        "   - <code>*****TXT*****</code> → ⚫ <b>BLACK BOLD CAPS</b>\n"
        "   - <code>****TXT****</code> → 🔵 <b>BLUE BOLD CAPS</b>\n"
        "   - <code>***TXT***</code> → 🔴 <b>RED BOLD CAPS</b>\n"
        "   - <code>*TXT*</code> → ⚫ <b>Bold Black</b> (Normal case)\n"
        "   - Roman Numerals (I., II.) → Auto-Red Headers\n\n"

        "<b>\U0001F5C3\uFE0F 2. VAULT & LIBRARY</b>\n"
        "• <b>\U0001F4CB Show Library:</b> Full index. Sorts by Date. Search by Code/Index.\n"
        "• <b>\U0001F517 Get Link:</b> Retrieve specific asset. Mode: <i>PDF File</i> (Download) or <i>Drive Link</i> (URL).\n"
        "• <b>\u270F\uFE0F Edit PDF:</b> Rename files in DB + Drive instantly (Refreshes links).\n"
        "• <b>📂 GDrive Explorer:</b> Navigates your <b>actual</b> Google Drive folders. View/Traverse the cloud hierarchy.\n\n"
        
        "<b>\U0001F6E1 3. SECURITY & RECOVERY</b>\n"
        "• <b>\u267B\uFE0F Recycle Bin:</b> Deleted files go here first. <i>Restore</i> or <i>Purge</i> (Single/Range).\n"
        "• <b>\U0001F5D1 Remove PDF:</b> Soft-delete. Moves to Bin. Updates DB & Drive.\n"
        "• <b>\u26A0\uFE0F NUKE ALL DATA:</b> ☠️ <b>DANGER!</b> Wipes MongoDB + Drive Vault. Irreversible.\n"
        "• <b>Anti-Spam:</b> Auto-bans users flooding commands (>5/sec).\n\n"

        "<b>⚙️ 4. ADMIN & INFRASTRUCTURE</b>\n"
        "• <b>⚙️ Admin Config:</b>\n"
        "   - <b>Add/Remove Admin:</b> Assign By ID.\n"
        "   - <b>Roles:</b> Give titles (e.g., 'Chief Editor').\n"
        "   - <b>Locks:</b> Freeze admin access without removing them.\n"
        "• <b>🗄️ Databases:</b> Switch MongoDB connections on the fly. Manage <code>.env</code> file directly.\n"
        "• <b>\U0001F4BB Live Terminal:</b> Real-time log streaming of bot actions.\n"
        "• <b>\U0001F4CA Storage Info:</b> Health Score, Drive Quota, DB Latency.\n\n"
        
        "<b>📦 5. BACKUP SYSTEMS</b>\n"
        "• <b>Manual:</b> Click <code>📦 Backup</code>.\n"
        "   - <b>Text Report:</b> Summary of Admins + PDF List.\n"
        "   - <b>JSON Dump:</b> Full restore-ready DB export.\n"
        "• <b>Auto-Cloud:</b> All backups upload to <code>MONTH YEAR</code> folders in Drive.\n"
        "• <b>Weekly Auto-Pilot:</b> Every Sunday @ 3AM, full snapshot sent to Owner.\n\n"

        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "\U0001F680 <b>SYSTEM STATUS: \U0001F7E2 OPTIMAL</b>"
    )
    await message.answer(help_text, parse_mode="HTML")

@dp.message(F.text == "⚠️ NUKE ALL DATA")
async def nuke_warning(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="☢️ EXECUTE NUKE"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "⚠️ **NUCLEAR WARNING** ⚠️\n\n"
        "<b>AUTHORITY VERIFICATION REQUIRED</b>\n"
        "You are about to initiate a **TOTAL SYSTEM WIPE**.\n\n"
        "🔥 **This will destroy:**\n"
        "- All MongoDB Metadata records\n"
        "- All PDF files in your Google Drive Vault\n"
        "- All local temporary files\n\n"
        "**This action is IRREVERSIBLE.** Are you absolutely sure?",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.confirm_nuke)

@dp.message(BotState.confirm_nuke)
async def nuke_execution(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    if message.text == "☢️ EXECUTE NUKE":
        status_msg = await message.answer("☢️ **INITIATING NUCLEAR PROTOCOL...**")
        
        # 1. MongoDB Wipe
        await status_msg.edit_text("🔥 **STEP 1/3: Purging Database...**")
        try:
            x = col_pdfs.delete_many({})
            db_count = x.deleted_count
        except Exception as e:
            db_count = f"Error: {e}"
            
        # 2. Drive Wipe
        await status_msg.edit_text("🔥 **STEP 2/3: Incinerating Google Drive Vault...**")
        drive_count = 0
        try:
            if PARENT_FOLDER_ID:
                service = get_drive_service()
                # List only FILES inside the Parent Folder (Exclude Folders)
                query = f"'{PARENT_FOLDER_ID}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
                results = service.files().list(q=query, fields="files(id, name)").execute()
                items = results.get('files', [])
                
                if items:
                    for item in items:
                        try:
                            service.files().delete(fileId=item['id']).execute()
                            drive_count += 1
                        except: pass
            else:
                 drive_count = "Skipped (No Parent ID)"
        except Exception as e:
            drive_count = f"Error: {e}"
            
        # 3. Local Wipe
        await status_msg.edit_text("🔥 **STEP 3/3: Sterilizing Local Environment...**")
        local_count = 0
        for file in os.listdir():
            if file.endswith(".pdf"):
                try: 
                    os.remove(file)
                    local_count += 1
                except: pass
                
        # Final Report
        report = (
            "☢️ **NUCLEAR WIPEOUT COMPLETE** ☢️\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🛢 **Database:** {db_count} records destroyed.\n"
            f"☁️ **Drive:** {drive_count} items incinerated.\n"
            f"💻 **Local:** {local_count} files purged.\n\n"
            "☠️ **NUKE COMPLETE.**\n\n"
            "The system has been purged, Master."
        )
        await status_msg.edit_text(report)
        await state.clear()
        
        # Reset Menu
        await message.answer("💎 **READY FOR REBIRTH.**", reply_markup=get_main_menu())
    else:
        await message.answer("Please confirm with the button or go back.")

# ==========================================
# 🩺 DIAGNOSIS SYSTEM
# ==========================================
@dp.message(F.text == "🩺 System Diagnosis")
async def perform_bot4_diagnosis(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    status_msg = await message.answer("🔄 **INITIATING DEEP SYSTEM SCAN...**")
    start_time = time.time()
    
    # Trackers
    health_score = 100
    issues = []
    
    # 1. ENVIRONMENT CHECK
    await status_msg.edit_text("🔐 **SCANNING: SECURITY ENVIRONMENT...**")
    env_checks = []
    if BOT_TOKEN: env_checks.append("✅ Bot Token")
    else: 
        env_checks.append("❌ Bot Token Missing"); health_score -= 50; issues.append("Critial: No Token")
        
    if MONGO_URI: env_checks.append("✅ Mongo URI")
    else: 
        env_checks.append("❌ Mongo URI Missing"); health_score -= 30; issues.append("Critical: No DB URI")
        
    if os.path.exists(CREDENTIALS_FILE): env_checks.append("✅ Credentials.json")
    else: 
        env_checks.append("❌ Credentials.json Missing"); health_score -= 20; issues.append("Auth: No Credentials")
        
    if os.path.exists(TOKEN_FILE): env_checks.append("✅ Token Pickle")
    else: env_checks.append("⚠️ Token Pickle (Will Re-Auth)")
    
    await asyncio.sleep(0.5)
    
    # 2. DATABASE LATENCY
    await status_msg.edit_text("🍃 **SCANNING: DATABASE TOPOLOGY...**")
    db_status = "❌ FAIL"
    mongo_lat = 0
    try:
        t0 = time.time()
        db_client.admin.command('ping')
        t1 = time.time()
        mongo_lat = (t1 - t0) * 1000
        db_status = f"✅ ONLINE ({mongo_lat:.1f}ms)"
        if mongo_lat > 500: 
            health_score -= 5; issues.append(f"High DB Latency ({mongo_lat:.0f}ms)")
    except Exception as e:
        db_status = f"❌ FAIL: {str(e)[:20]}..."
        health_score -= 30; issues.append("Database Disconnected")

    await asyncio.sleep(0.5)

    # 3. GOOGLE DRIVE API
    await status_msg.edit_text("☁️ **SCANNING: GOOGLE DRIVE UPLINK...**")
    drive_status = "❌ FAIL"
    api_lat = 0
    try:
        t0 = time.time()
        service = get_drive_service()
        # Check Identity instead of Root (More robust)
        about = service.about().get(fields="user").execute()
        email = about.get('user', {}).get('emailAddress', 'Unknown')
        t1 = time.time()
        api_lat = (t1 - t0) * 1000
        drive_status = f"✅ ONLINE ({api_lat:.1f}ms)\n   👤 Auth: `{email}`"
    except Exception as e:
        drive_status = f"❌ FAIL: {str(e)[:50]}..."
        health_score -= 30; issues.append(f"Drive Auth Error")

    await asyncio.sleep(0.5)

    # 4. FILESYSTEM
    await status_msg.edit_text("📁 **SCANNING: LOCAL FILESYSTEM...**")
    fs_status = "✅ WRITEABLE"
    try:
        with open("test_write.tmp", "w") as f: f.write("test")
        os.remove("test_write.tmp")
    except Exception as e:
        fs_status = f"❌ READ-ONLY ({e})"
        health_score -= 20; issues.append("Filesystem Read-Only")

    # Final Compilation
    scan_duration = time.time() - start_time
    
    # Color Logic
    health_emoji = "🟢 EXCELLENT"
    if health_score < 90: health_emoji = "🟡 GOOD"
    if health_score < 70: health_emoji = "🟠 DEGRADED"
    if health_score < 50: health_emoji = "🔴 CRITICAL FAILURE"
    
    report = (
        f"🩺 **SYSTEM DIAGNOSTIC REPORT**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌡️ **HEALTH SCORE:** {health_emoji} ({health_score}%)\n"
        f"🤡 **Scan Duration:** `{scan_duration:.2f}s`\n\n"
        
        f"🔐 **ENVIRONMENT**\n"
        f"• {'  '.join(env_checks)}\n\n"
        
        f"🍃 **DATABASE CORE**\n"
        f"• Status: {db_status}\n"
        f"• Records: {col_pdfs.count_documents({}) if col_pdfs is not None else 'N/A'}\n\n"
        
        f"☁️ **GOOGLE UPLINK**\n"
        f"• Status: {drive_status}\n"
        f"• Parent ID: `{PARENT_FOLDER_ID if PARENT_FOLDER_ID else 'WARNING: USING ROOT'}`\n\n"
        
        f"💻 **HOST SYSTEM**\n"
        f"• Filesystem: {fs_status}\n"
        f"• Process ID: `{os.getpid()}`\n\n"
    )
    
    if issues:
        report += f"⚠️ **ISSUES DETECTED:**\n"
        for i in issues: report += f"• {i}\n"
        report += "\n🛑 **ACTION REQUIRED.**"
    else:
        report += "✅ **SYSTEM OPERATING AT PEAK EFFICIENCY.**"
        
    await status_msg.edit_text(report, parse_mode="Markdown")

# ==========================================
# 👥 ADMIN MANAGEMENT
# ==========================================
@dp.message(F.text == "⚙️ Admin Config")
async def admin_config_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="➕ Add Admin"), KeyboardButton(text="➖ Remove Admin"))
    builder.row(KeyboardButton(text="🔑 Permissions"), KeyboardButton(text="🎭 Manage Roles"))
    builder.row(KeyboardButton(text="🔒 Lock/Unlock User"), KeyboardButton(text="🚫 Banned Users"))
    builder.row(KeyboardButton(text="📜 List Admins"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "👥 **ADMINISTRATION CONSOLE**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Manage authorized users and security protocols.\n"
        "Select an option:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )

# === ADD ADMIN ===
@dp.message(F.text == "➕ Add Admin")
async def add_admin_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "➕ **ADD ADMINISTRATOR**\n"
        "Enter the **Telegram User ID** to promote.\n"
        "⚠️ They will have full access.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")], [KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
    )
    await state.set_state(BotState.waiting_for_admin_id)

@dp.message(BotState.waiting_for_admin_id)
async def process_add_admin(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": 
        await admin_config_btn(message)
        await state.clear()
        return

    if not text.isdigit():
        await message.answer("⚠️ Invalid ID.")
        return
        
    new_admin_id = int(text)
    
    if is_admin(new_admin_id):
        await message.answer("⚠️ User is already an Admin.")
        return
        
    col_admins.insert_one({
        "user_id": new_admin_id,
        "added_by": message.from_user.id,
        "timestamp": datetime.now(),
        "locked": True # LOCKED BY DEFAULT
    })
    
    await message.answer(
        f"✅ **USER ADDED**\n"
        f"👤 ID: `{new_admin_id}`\n"
        f"🔒 Status: **LOCKED (Inactive)**\n"
        f"⚠️ **Action Required:** Go to `🔒 Lock/Unlock` to grant access."
    )
    await state.clear()
    await admin_config_btn(message)

# === REMOVE ADMIN ===
@dp.message(F.text == "➖ Remove Admin")
async def remove_admin_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "➖ **REMOVE ADMINISTRATOR**\n"
        "Enter the **Telegram User ID** to demote.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")], [KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
    )
    await state.set_state(BotState.waiting_for_remove_admin)

@dp.message(BotState.waiting_for_remove_admin)
async def process_remove_admin(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": 
        await admin_config_btn(message)
        await state.clear()
        return

    if not text.isdigit():
        await message.answer("⚠️ Invalid ID.")
        return
        
    target_id = int(text)
    
    if target_id == OWNER_ID:
        await message.answer("❌ Cannot remove the Owner.")
        return
        
    res = col_admins.delete_one({"user_id": target_id})
    if res.deleted_count > 0:
        await message.answer(f"✅ **SUCCESS:** User `{target_id}` demoted.")
    else:
        # Try string cleanup
        res = col_admins.delete_one({"user_id": str(target_id)})
        if res.deleted_count > 0:
             await message.answer(f"✅ **SUCCESS:** User `{target_id}` demoted.")
        else:
             await message.answer("⚠️ User is not an admin.")
             
    await state.clear()
    await admin_config_btn(message)

# === LIST ADMINS ===
@dp.message(F.text == "📜 List Admins")
async def list_admins_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    admins = list(col_admins.find())
    msg = ["👥 **ADMINISTRATORS**", "━━━━━━━━━━━━━━━━━━━━"]
    msg.append(f"👑 `{OWNER_ID}` (Owner)")
    
    for a in admins:
        uid = a.get('user_id')
        role = a.get('role', 'Standard Admin')
        locked = a.get('locked', False)
        
        status_icon = "🛑 LOCKED" if locked else "✅ Active"
        
        msg.append(f"👤 `{uid}`\n   ├ 🏷 **Role:** {role}\n   └ 📊 **Status:** {status_icon}")
        
    await message.answer("\n".join(msg))

# === PERMISSION MANAGEMENT ===
@dp.message(F.text == "🔑 Permissions")
async def permissions_entry(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("⛔ **ACCESS DENIED.**\nOnly the Owner can modify clearance levels.")
        return

    admins = list(col_admins.find())
    if not admins:
        await message.answer("⚠️ No admins found to manage.")
        return

    # Generate Admin List
    msg = ["🔑 **SELECT ADMINISTRATOR**", "━━━━━━━━━━━━━━━━━━━━"]
    builder = ReplyKeyboardBuilder()
    
    for idx, a in enumerate(admins, 1):
        uid = a.get('user_id')
        name = f"Admin {idx}" 
        # Try to get name if possible or just use ID
        msg.append(f"**{idx}.** `{uid}`")
        builder.add(KeyboardButton(text=f"{idx}. {uid}"))
        
    builder.adjust(2)
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "\n".join(msg) + "\n\n👇 **Select an Admin to configure:**",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.waiting_for_perm_admin)

@dp.message(BotState.waiting_for_perm_admin)
async def permissions_admin_select(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": return await admin_config_btn(message)
    
    # Parse selection "1. 12345"
    target_id = None
    try:
        if ". " in text:
            target_id = int(text.split(". ")[1])
        else:
            target_id = int(text) # Fallback if they type ID
    except:
        await message.answer("⚠️ Invalid Selection.")
        return
        
    # Verify Admin
    admin_doc = col_admins.find_one({"user_id": target_id})
    if not admin_doc:
        # Check str id
        admin_doc = col_admins.find_one({"user_id": str(target_id)})
        
    if not admin_doc:
        await message.answer("❌ Admin not found.")
        return
        
    await state.update_data(perm_target_id=target_id)
    await render_permission_menu(message, state, target_id)

async def render_permission_menu(message, state, target_id):
    # Fetch current permissions
    # If key 'permissions' missing, assume ALL enabled
    admin_doc = col_admins.find_one({"user_id": target_id})
    if not admin_doc: admin_doc = col_admins.find_one({"user_id": str(target_id)})
    
    current_perms = admin_doc.get("permissions", DEFAULT_PERMISSIONS)
    
    builder = ReplyKeyboardBuilder()
    
    # Iterate through MAP to build toggles
    # PERMISSION_MAP = {"Text": "key"}
    # We want button text to be "✅ Generate PDF" or "❌ Generate PDF"
    
    for btn_text, key in PERMISSION_MAP.items():
        status = "✅" if key in current_perms else "❌"
        # Clean button text (remove existing emojis if duplicate?)
        # btn_text has emojis like "📄 Generate PDF". 
        # Result: "✅ 📄 Generate PDF"
        label = f"{status} {btn_text}"
        builder.add(KeyboardButton(text=label))
        
    builder.adjust(2)
    builder.row(KeyboardButton(text="🔐 GRANT ALL"), KeyboardButton(text="🔒 REVOKE ALL"))
    builder.row(KeyboardButton(text="💾 SAVE CHANGES"))
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        f"⚙️ **CONFIGURING: `{target_id}`**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ = Access Granted\n"
        f"❌ = Access Denied\n\n"
        f"👇 **Toggle features below:**",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.waiting_for_perm_toggle)

@dp.message(BotState.waiting_for_perm_toggle)
async def process_perm_toggle(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": return await permissions_entry(message, state)
    
    # Handle SAVE
    if text == "💾 SAVE CHANGES":
        await message.answer("✅ **SETTINGS SAVED SUCCESSFULLY**")
        await permissions_entry(message, state) # Return to Admin List
        return
    
    data = await state.get_data()
    target_id = data.get("perm_target_id")
    
    # Reload doc
    admin_doc = col_admins.find_one({"user_id": target_id})
    if not admin_doc: admin_doc = col_admins.find_one({"user_id": str(target_id)})
    current_perms = admin_doc.get("permissions", DEFAULT_PERMISSIONS)
    
    # HANDLE BULK
    if text == "🔐 GRANT ALL":
        new_perms = list(PERMISSION_MAP.values())
        update_admin_perms(target_id, new_perms)
        await message.answer("✅ **ALL FEATURES ENABLED**")
        await render_permission_menu(message, state, target_id)
        return
        
    if text == "🔒 REVOKE ALL":
        update_admin_perms(target_id, [])
        await message.answer("🔒 **ALL FEATURES DISABLED**")
        await render_permission_menu(message, state, target_id)
        return

    # HANDLE TOGGLE
    # Text format: "✅ 📄 Generate PDF"
    # We need to find which key this corresponds to.
    
    # Strip status icon
    clean_text = text[2:].strip() # Remove "✅ " or "❌ "
    
    target_key = PERMISSION_MAP.get(clean_text)
    
    if not target_key:
        await message.answer("⚠️ Unknown Option.")
        return
        
    # Toggle
    if target_key in current_perms:
        current_perms.remove(target_key)
        action = "❌ Disabled"
    else:
        current_perms.append(target_key)
        action = "✅ Enabled"
        
    update_admin_perms(target_id, current_perms)
    
    # Re-render
    await render_permission_menu(message, state, target_id)

def update_admin_perms(user_id, perms):
    # Try updating int
    res = col_admins.update_one({"user_id": user_id}, {"$set": {"permissions": perms}})
    if res.matched_count == 0:
        # Try str
        col_admins.update_one({"user_id": str(user_id)}, {"$set": {"permissions": perms}})

# === ROLE MANAGEMENT ===
@dp.message(F.text == "🎭 Manage Roles")
async def roles_entry(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("⛔ **ACCESS DENIED.**\nOnly the Owner can assign titles.")
        return

    admins = list(col_admins.find())
    if not admins:
        await message.answer("⚠️ No admins found.")
        return

    msg = ["🎭 **SELECT ADMINISTRATOR FOR ROLE ASSIGNMENT**", "━━━━━━━━━━━━━━━━━━━━"]
    builder = ReplyKeyboardBuilder()
    
    for idx, a in enumerate(admins, 1):
        uid = a.get('user_id')
        current_role = a.get('role', 'Standard Admin')
        msg.append(f"**{idx}.** `{uid}` - *{current_role}*")
        builder.add(KeyboardButton(text=f"{idx}. {uid}"))
        
    builder.adjust(2)
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "\n".join(msg),
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.waiting_for_role_admin)

@dp.message(BotState.waiting_for_role_admin)
async def roles_admin_select(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": return await admin_config_btn(message)
    
    # Parse ID
    target_id = None
    try:
        if ". " in text: target_id = int(text.split(". ")[1])
        else: target_id = int(text)
    except:
        await message.answer("⚠️ Invalid Selection.")
        return
        
    # Verify
    if not is_admin(target_id):
        await message.answer("❌ Admin not found.")
        return
        
    await state.update_data(role_target_id=target_id)
    
    # Show Roles
    builder = ReplyKeyboardBuilder()
    roles = ["Manager", "Supervisor", "Editor", "Moderator", "Head Admin"]
    for r in roles: builder.add(KeyboardButton(text=r))
    
    builder.adjust(2)
    builder.row(KeyboardButton(text="✏️ CUSTOM ROLE"))
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        f"🎭 **ASSIGN ROLE FOR: `{target_id}`**\n"
        f"Select a preset or create custom:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.waiting_for_role_select)

@dp.message(BotState.waiting_for_role_select)
async def process_role_assign(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": return await roles_entry(message, state)
    
    if text == "✏️ CUSTOM ROLE":
        await message.answer("✏️ **Enter Custom Role Title:**", reply_markup=ReplyKeyboardRemove())
        await state.set_state(BotState.waiting_for_custom_role)
        return
        
    # Assign Standard Role
    data = await state.get_data()
    target_id = data.get("role_target_id")
    
    update_admin_role(target_id, text)
    await message.answer(f"✅ **ROLE ASSIGNED:** `{text}`")
    await roles_entry(message, state) # Loop back

@dp.message(BotState.waiting_for_custom_role)
async def process_custom_role(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if not text: return 
    
    data = await state.get_data()
    target_id = data.get("role_target_id")
    
    update_admin_role(target_id, text)
    await message.answer(f"✅ **CUSTOM ROLE ASSIGNED:** `{text}`")
    await roles_entry(message, state)

def update_admin_role(user_id, role):
    res = col_admins.update_one({"user_id": user_id}, {"$set": {"role": role}})
    if res.matched_count == 0:
        col_admins.update_one({"user_id": str(user_id)}, {"$set": {"role": role}})

# === LOCK/UNLOCK MANAGEMENT ===
@dp.message(F.text == "🔒 Lock/Unlock User")
async def lock_entry(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("⛔ **ACCESS DENIED.**\nOnly the Owner can freeze accounts.")
        return

    admins = list(col_admins.find())
    if not admins:
        await message.answer("⚠️ No admins found.")
        return

    msg = ["🔒 **SELECT ADMIN TO FREEZE/THAW**", "━━━━━━━━━━━━━━━━━━━━"]
    builder = ReplyKeyboardBuilder()
    
    for idx, a in enumerate(admins, 1):
        uid = a.get('user_id')
        is_locked = a.get('locked', False)
        status = "🔴 LOCKED" if is_locked else "🟢 ACTIVE"
        
        msg.append(f"**{idx}.** `{uid}` - {status}")
        builder.add(KeyboardButton(text=f"{idx}. {uid}"))
        
    builder.adjust(2)
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "\n".join(msg),
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.waiting_for_lock_admin)

@dp.message(BotState.waiting_for_lock_admin)
async def lock_admin_select(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": return await admin_config_btn(message)
    
    # Parse ID
    target_id = None
    try:
        if ". " in text: target_id = int(text.split(". ")[1])
        else: target_id = int(text)
    except:
        await message.answer("⚠️ Invalid Selection.")
        return
        
    # Verify (Here we check raw DB because is_admin might return False if locked)
    admin_doc = col_admins.find_one({"user_id": target_id})
    if not admin_doc: admin_doc = col_admins.find_one({"user_id": str(target_id)})
    
    if not admin_doc:
         await message.answer("❌ Admin not found.")
         return

    await state.update_data(lock_target_id=target_id)
    
    is_locked = admin_doc.get('locked', False)
    status_text = "🔴 LOCKED (Inactive)" if is_locked else "🟢 ACTIVE (Operational)"
    
    builder = ReplyKeyboardBuilder()
    if is_locked:
        builder.row(KeyboardButton(text="🔓 UNLOCK (RESTORE ACCESS)"))
    else:
        builder.row(KeyboardButton(text="🔒 LOCK (REVOKE ACCESS)"))
        
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        f"⚙️ **CONFIGURING: `{target_id}`**\n"
        f"Current Status: **{status_text}**\n\n"
        f"👇 Select Action:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.waiting_for_lock_toggle)

@dp.message(BotState.waiting_for_lock_toggle)
async def process_lock_toggle(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": return await lock_entry(message, state)
    
    data = await state.get_data()
    target_id = data.get("lock_target_id")
    
    if "LOCK" in text:
        update_admin_lock(target_id, True)
        await message.answer(f"🔒 **ACCESS REVOKED:** `{target_id}` is now Frozen.")
    elif "UNLOCK" in text:
        update_admin_lock(target_id, False)
        await message.answer(f"🔓 **ACCESS RESTORED:** `{target_id}` is now Active.")
        
    await lock_entry(message, state)

def update_admin_lock(user_id, locked):
    res = col_admins.update_one({"user_id": user_id}, {"$set": {"locked": locked}})
    if res.matched_count == 0:
        col_admins.update_one({"user_id": str(user_id)}, {"$set": {"locked": locked}})

# BANNED MANAGEMENT SUBMENU
@dp.message(F.text == "🚫 Banned Users")
async def banned_mgmt_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔨 BAN USER"), KeyboardButton(text="🔓 UNBAN USER"))
    builder.row(KeyboardButton(text="📜 LIST BANNED"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "🚫 **BANNED USER MANAGEMENT**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Manage the Blacklist.\n"
        "Select Operation:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )

@dp.message(F.text == "🔨 BAN USER")
async def ban_user_manual_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "🔨 **MANUAL BAN**\n"
        "Enter the **Telegram User ID** to ban forever.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⬅️ BACK")],
                [KeyboardButton(text="🔙 Back to Menu")]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(BotState.waiting_for_ban_id)

@dp.message(BotState.waiting_for_ban_id)
async def process_manual_ban(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # Handle BACK button - return to admin config
    if text == "⬅️ BACK":
        await admin_config_btn(message)
        await state.clear()
        return
    
    if not text.isdigit():
        await message.answer("⚠️ Invalid ID.")
        return
    
    target_id = int(text)
    if target_id == OWNER_ID:
        await message.answer("❌ Cannot ban the Owner.")
        return
        
    if is_banned(target_id):
        await message.answer("⚠️ User is already banned.")
        return
    
    try:
        if col_banned is not None:
            col_banned.insert_one({
                "user_id": target_id,
                "reason": f"Manual Ban by {message.from_user.id}",
                "timestamp": datetime.now()
            })
            await message.answer(f"✅ **BANNED:** User `{target_id}` is now blacklisted.")
        else:
            await message.answer("❌ Database unavailable. Cannot ban user.")
            return
    except Exception as e:
        logging.error(f"Failed to ban user {target_id}: {e}")
        await message.answer(f"❌ Ban failed: {e}")
        return
    
    # Notify Owner/Log
    if message.from_user.id != OWNER_ID:
        log_msg = f"🔨 **ADMIN LOG: BAN**\nAdmin: `{message.from_user.id}`\nTarget: `{target_id}`\nTime: {datetime.now()}"
        try: await bot.send_message(OWNER_ID, log_msg)
        except: pass
    await state.clear()
    await banned_mgmt_btn(message, state)


@dp.message(F.text == "🔓 UNBAN USER")
async def unban_user_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "🔓 **UNBAN USER**\n"
        "Enter the **Telegram User ID** to forgive.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
    )
    await state.set_state(BotState.waiting_for_unban_id)

@dp.message(BotState.waiting_for_unban_id)
async def process_unban(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # Handle BACK button - return to admin config
    if text == "⬅️ BACK":
        await admin_config_btn(message)
        await state.clear()
        return
    
    if not text.isdigit():
        await message.answer("⚠️ Invalid ID.")
        return
        
    target_id = int(text)
    
    try:
        if col_banned is None:
            await message.answer("❌ Database unavailable. Cannot unban user.")
            await state.clear()
            return
        
        res = col_banned.delete_one({"user_id": target_id})
        
        if res.deleted_count > 0:
            await message.answer(f"✅ **UNBANNED:** User `{target_id}` is free.")
            
            # Notify Owner/Log
            if message.from_user.id != OWNER_ID:
                log_msg = f"🔓 **ADMIN LOG: UNBAN**\nAdmin: `{message.from_user.id}`\nTarget: `{target_id}`\nTime: {datetime.now()}"
                try: await bot.send_message(OWNER_ID, log_msg)
                except: pass
        else:
            await message.answer(f"⚠️ User `{target_id}` was not in the ban list.")
    except Exception as e:
        logging.error(f"Failed to unban user {target_id}: {e}")
        await message.answer(f"❌ Unban failed: {e}")
        
    await state.clear()
    await banned_mgmt_btn(message, state)

@dp.message(F.text == "📜 LIST BANNED")
async def list_banned_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    try:
        if col_banned is None:
            await message.answer("❌ Database unavailable. Cannot list banned users.")
            return
        
        bans = list(col_banned.find())
    except Exception as e:
        logging.error(f"Failed to fetch banned users: {e}")
        await message.answer(f"❌ Failed to retrieve banned users: {e}")
        return
    
    msg = ["🚫 **BLACKLISTED USERS**", "━━━━━━━━━━━━━━━━━━━━"]
    
    if not bans:
        msg.append("_No banned users._")
    else:
        for idx, b in enumerate(bans, 1):
            uid = b.get('user_id')
            reason = b.get('reason', 'Unknown')
            ts = b.get('timestamp')
            
            # Format Date
            if isinstance(ts, datetime):
                date_str = ts.strftime('%d-%b-%Y %I:%M %p')
            else:
                date_str = "Unknown"
                
            msg.append(
                f"{idx}. `{uid}`\n"
                f"   📅 **Date:** {date_str}\n"
                f"   📝 **Reason:** {reason}\n"
            )
            
    await message.answer("\n".join(msg))

# ==========================================
# 📂 GDRIVE EXPLORER
# ==========================================
@dp.message(F.text == "📂 GDrive Explorer")
async def drive_explorer_entry(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    msg = await message.answer("⏳ **Locating Bot Data Sector...**")
    
    msg = await message.answer("⏳ **Locating Bot Data Sector...**")
    
    # 1. Try Configured Parent
    root_id = PARENT_FOLDER_ID
    
    # 2. Dynamic Lookup if Config Missing
    if not root_id:
        # Search for the Main Folder explicitly
        target_name = "BOT 4 DATA" 
        root_id = await asyncio.to_thread(find_folder_by_name, target_name)
        
    # 3. Validation - NO ROOT FALLBACK
    if not root_id:
        await msg.delete()
        
        # DEBUG: List what we CAN see to help user
        debug_list = await asyncio.to_thread(list_drive_contents, 'root')
        # list_drive_contents returns: name, parent, folders, files
        # We just want top 5 folders
        visible_names = [f['name'] for f in debug_list[2][:5]]
        visible_str = ", ".join(visible_names) if visible_names else "None"
        
        await message.answer(
            "❌ **ACCESS DENIED: MISSING VAULT**\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "The system cannot locate the `BOT 4 DATA` folder.\n"
            "Security Protocol prohibits scanning the entire Drive.\n\n"
            "⚠️ **Action:** Please create a folder named `BOT 4 DATA` in your Google Drive.\n\n"
            f"🔍 **Debug - I can see:**\n`{visible_str}`"
        )
        return
        
    await msg.delete()
    
    # Store this ID as the Session Root
    await state.update_data(explorer_root=root_id)
    
    await render_explorer_gateway(message, state)

async def render_explorer_gateway(message: types.Message, state: FSMContext):
    """Displays the Entry Gateway with the single Root Folder button."""
    data = await state.get_data()
    root_id = data.get('explorer_root')
    
    if not root_id:
        await message.answer("❌ Error: Root not established.")
        return
        
    # Fetch stats for the Gateway View
    name, parent, folders, files = await asyncio.to_thread(list_drive_contents, root_id)
    
    # Preview of what we see
    visible_folders = [f['name'] for f in folders[:3]]
    preview_str = ", ".join(visible_folders) if visible_folders else "None"
    if len(folders) > 3: preview_str += "..."
        
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="📂 BOT 4 DATA"))
    builder.add(KeyboardButton(text="🔄 Force Refresh"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    # Map the visual button to the ID
    nav_map = {"📂 BOT 4 DATA": root_id}
    await state.update_data(drive_nav_map=nav_map)
    await state.set_state(BotState.browsing_drive)
    
    await message.answer(
        "📂 **GDRIVE EXPLORER**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 **Source:** `{name}`\n"
        f"🆔 **ID:** `{root_id[-6:]}` (Partial)\n"
        f"📁 **Folders:** `{len(folders)}`\n"
        f"📄 **Files:** `{len(files)}`\n"
        f"👀 **Visible:** `{preview_str}`\n\n"
        "👇 **Click below to Access Data:**",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )

async def show_drive_folder(message: types.Message, state: FSMContext, folder_id):
    try:
        name, parent, folders, files = await asyncio.to_thread(list_drive_contents, folder_id)
        
        # LOGIC RESTRICTION: Keep user inside the Session Root
        data = await state.get_data()
        session_root = data.get('explorer_root')
        
        # If we somehow didn't set it (legacy state), fallback
        if not session_root: 
            session_root = PARENT_FOLDER_ID if PARENT_FOLDER_ID else 'root'
            
        is_root = (folder_id == session_root)
        
        if is_root:
            name = "BOT 4 DATA" # Branding Override
            parent = "ENTRY" # UP goes back to Gateway
            
        builder = ReplyKeyboardBuilder()
        
        # Navigation Buttons
        if parent: 
            builder.add(KeyboardButton(text="⬆️ UP ONE LEVEL"))
            
        # Folders (📂)
        for f in folders[:50]: # Increased Limit for visibility
            builder.add(KeyboardButton(text=f"📂 {f['name']}"))
            
        # Files (📄)
        for f in files[:50]:
            builder.add(KeyboardButton(text=f"📄 {f['name']}"))
            
        builder.adjust(2) # 2 columns
        builder.row(KeyboardButton(text="🔙 Back to Menu"))
        
        # Store state for navigation mapping
        # We need to map names back to IDs since ReplyKeyboard sends text
        nav_map = { "UP": parent }
        for f in folders: nav_map[f"📂 {f['name']}"] = f['id']
        for f in files: nav_map[f"📄 {f['name']}"] = {"type": "file", "link": f.get('webViewLink'), "name": f['name']}
        
        await state.update_data(drive_nav_map=nav_map, current_folder_id=folder_id)
        await state.set_state(BotState.browsing_drive)
        
        # Display
        stats = f"📁 **Total Available Folders:** `{len(folders)}`\n📄 **Total Available PDFs:** `{len(files)}`"
        await message.answer(
            f"📂 **EXPLORER:** `{name}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{stats}\n\n"
            "👇 **Navigate or Open Files:**",
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        await message.answer(f"❌ **Explorer Error:** {e}")
        # await start(message, state)

@dp.message(BotState.browsing_drive)
async def drive_browser_nav(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    data = await state.get_data()
    nav_map = data.get('drive_nav_map', {})
    
    if text == "🔄 Force Refresh":
        msg = await message.answer("♻️ Refreshing...")
        await render_explorer_gateway(message, state)
        await msg.delete()
        return
    
    if text == "⬆️ UP ONE LEVEL":
        target = nav_map.get("UP")
        if target == "ENTRY":
            await render_explorer_gateway(message, state)
        elif target: 
            await show_drive_folder(message, state, target)
        else: 
            await message.answer("⚠️ Top of Vault.")
        return

    # Check if folder or file
    if text in nav_map:
        item = nav_map[text]
        if isinstance(item, str): 
            # It's a Folder ID -> Navigate
            await show_drive_folder(message, state, item)
        else:
            # It's a File Dict -> Show Details
            f_name = item['name']
            f_link = item['link']
            await message.answer(
                f"📄 **FILE DETAILS**\n"
                f"Name: `{f_name}`\n"
                f"🔗 [Open in Drive]({f_link})",
                parse_mode="Markdown"
            )
    else:
        # User typed something random?
        await message.answer("⚠️ Please select a folder/file from the buttons.")

@dp.message(F.text == "➕ Add Admin")
async def add_admin_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "➕ **ADD NEW ADMIN**\n"
        "Enter the **Telegram User ID** of the new admin.\n"
        "(Get it from @userinfobot)",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⬅️ BACK")],
                [KeyboardButton(text="🔙 Back to Menu")]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(BotState.waiting_for_admin_id)

@dp.message(BotState.waiting_for_admin_id)
async def process_add_admin(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # Handle BACK button - return to admin config
    if text == "⬅️ BACK":
        await admin_config_btn(message)
        await state.clear()
        return
    
    if not text.isdigit():
        await message.answer("⚠️ Invalid ID. Please enter numbers only.")
        return
        
    new_admin_id = int(text)
    
    if is_admin(new_admin_id):
        await message.answer(f"⚠️ User `{new_admin_id}` is already an Admin.")
        return
        
    col_admins.insert_one({
        "user_id": new_admin_id,
        "added_by": message.from_user.id,
        "timestamp": datetime.now()
    })
    
    await message.answer(f"✅ **SUCCESS:** User `{new_admin_id}` is now an Admin.")
    await state.clear()
    await admin_config_btn(message) # Return to Admin Menu

@dp.message(F.text == "➖ Remove Admin")
async def remove_admin_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "➖ **REMOVE ADMIN**\n"
        "Enter the **Telegram User ID** to revoke access.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
    )
    await state.set_state(BotState.waiting_for_remove_admin)

@dp.message(BotState.waiting_for_remove_admin)
async def process_remove_admin(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    
    if not text.isdigit():
        await message.answer("⚠️ Invalid ID. Numbers only.")
        return
        
    target_id = int(text)
    
    if target_id == OWNER_ID:
        await message.answer("❌ **CRITICAL DENIED:** Cannot remove the Supreme Owner.")
        return
        
    res = col_admins.delete_one({"user_id": target_id})
    
    if res.deleted_count > 0:
        await message.answer(f"🗑 **REVOKED:** User `{target_id}` removed from Admins.")
    else:
        await message.answer(f"⚠️ User `{target_id}` is not in the Admin List.")
        
    await state.clear()
    await admin_config_btn(message)

@dp.message(F.text == "📜 List Admins")
async def list_admins_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    admins = list(col_admins.find())
    
    msg = [
        "👥 **AUTHORIZED ADMINS**",
        "━━━━━━━━━━━━━━━━━━━━",
        f"👑 **OWNER:** `{OWNER_ID}`"
    ]
    
    if not admins:
        msg.append("\n_No additional admins configured._")
    else:
        for idx, a in enumerate(admins, 1):
            uid = a.get('user_id')
            added = a.get('timestamp', datetime.now()).strftime('%d/%m/%y')
            msg.append(f"{idx}. `{uid}` (Since {added})")
            
    msg.append("━━━━━━━━━━━━━━━━━━━━")
    await message.answer("\n".join(msg))


# ==========================================
# 💻 TERMINAL VIEWER
# ==========================================
@dp.message(BotState.waiting_for_remove_admin)
async def process_remove_admin(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return # Security Patch
    
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
# ...

# ==========================================
# 💻 TERMINAL VIEWER
# ==========================================
@dp.message(F.text == "💻 Live Terminal")
async def show_terminal(message: types.Message):
    """Shows live terminal logs from memory"""
    if not is_admin(message.from_user.id): return # Security Patch

    if not LOG_BUFFER:
        await message.answer("💻 <b>LIVE TERMINAL</b>\n━━━━━━━━━━━━━━━━━━━━\n<i>No logs captured yet.</i>", parse_mode="HTML")
        return
        
    # Get last 20 lines
    logs = list(LOG_BUFFER)[-20:]
    log_text = "\n".join(logs)
    
    # Escape HTML to prevent injection
    import html
    safe_log_text = html.escape(log_text)
    
    msg = (
        f"💻 <b>LIVE TERMINAL STREAM</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<pre>{safe_log_text}</pre>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 <i>Connection Active</i> | ⏱ {datetime.now().strftime('%I:%M:%S %p')}"
    )
    
    # Add Refresh Button
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔄 REFRESH TERMINAL"), KeyboardButton(text="🔙 Back to Menu")]
    ], resize_keyboard=True)
    
    await message.answer(msg, parse_mode="HTML", reply_markup=kb)

@dp.message(F.text == "🔄 REFRESH TERMINAL")
async def refresh_terminal(message: types.Message):
    if not is_admin(message.from_user.id): return # Security Patch
    await show_terminal(message)

@dp.message(F.text == "🔄 REFRESH TERMINAL")
async def refresh_terminal(message: types.Message):
    await show_terminal(message)

# Handle BACK TO MENU separately if not already global


# ==========================================
# 🗄️ DATABASE & ENV MANAGEMENT
# ==========================================

def migrate_env_vars(new_client):
    """
    Ensures the target DB has the bot_secrets from the current valid session.
    Only copies if they are missing in the destination.
    """
    try:
        # 1. Get current secrets from MEMORY (loaded at start)
        if not BOT_TOKEN:
            return "⚠️ Current session has no secrets to migrate."
            
        target_db = new_client["MSANodeDB"]
        target_col = target_db["bot_secrets"]
        
        # 2. Check if target has secrets
        existing = target_col.find_one({"bot": "bot4"})
        if existing:
            # User Feedback: Confirm we found it and are using it.
            return "✅ Target DB already has `bot_secrets`. Used existing data (No Overwrite)."
            
        # 3. Migrate
        # We need to construct the secret object. 
        # Since we don't have the raw files in memory as base64 strings easily available unless we re-read them.
        # Let's re-read the local files if they exist.
        
        files_data = {}
        if os.path.exists('credentials.json'):
            with open('credentials.json', "rb") as f:
                files_data['credentials.json'] = base64.b64encode(f.read()).decode('utf-8')
        if os.path.exists('token.pickle'):
            with open('token.pickle', "rb") as f:
                files_data['token.pickle'] = base64.b64encode(f.read()).decode('utf-8')
                
        secret_doc = {
            "bot": "bot4",
            "BOT_TOKEN": BOT_TOKEN,
            "OWNER_ID": str(OWNER_ID),
            "PARENT_FOLDER_ID": PARENT_FOLDER_ID,
            "files": files_data
        }
        
        target_col.insert_one(secret_doc)
        return "🚀 Secrets Migrated to New DB successfully."
        
    except Exception as e:
        return f"❌ Migration Failed: {e}"

@dp.message(F.text == "🗄️ Databases")
async def handle_databases_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🍃 MongoDB"), KeyboardButton(text="🔐 Env Variables"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "🗄️ **DATABASE MANAGEMENT**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Manage your data persistence layers.\n\n"
        "<b>🍃 MongoDB:</b> Switch core database connections.\n"
        "<b>🔐 Env Variables:</b> Manage API Keys and Secret Files.",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )

# --- MONGODB MENU ---
@dp.message(F.text == "🍃 MongoDB")
async def handle_mongodb_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    config = load_db_config()
    active_alias = config.get("active", "Unknown")
    
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="➕ Add Mongo"), KeyboardButton(text="👁️ Show Mongo"))
    builder.row(KeyboardButton(text="🔄 Switch Mongo"), KeyboardButton(text="🗑️ Remove Mongo"))
    builder.row(KeyboardButton(text="🔙 Back"), KeyboardButton(text="🏠 Main Menu"))
    
    await message.answer(
        f"🍃 **MONGODB DASHBOARD**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Active Connection:</b> <code>{active_alias}</code>\n\n"
        f"Select an operation:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )

# --- ADD MONGO ---
@dp.message(F.text == "➕ Add Mongo")
async def add_mongo_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    await message.answer(
        "➕ **ADD CONNECTION**\n"
        "Please send the **MongoDB Connection String (URI)**.\n"
        "<i>Format: mongodb+srv://...</i>",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back")]], resize_keyboard=True),
        parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_mongo_url)

@dp.message(BotState.waiting_for_mongo_url)
async def process_mongo_url(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back": return await handle_mongodb_btn(message)
    
    uri = message.text.strip()
    
    # Check for Duplicate URI
    config = load_db_config()
    existing_uris = config["connections"].values()
    if uri in existing_uris:
        await message.answer("⚠️ **Duplicate Connection!**\nThis MongoDB URI is already saved.\nPlease send a different one or Go Back.")
        return
        
    msg = await message.answer("🔄 Testing Connection...")
    
    try:
        # Test Connection
        client = pymongo.MongoClient(
            uri,
            serverSelectionTimeoutMS=15000,
            socketTimeoutMS=30000,
            connectTimeoutMS=15000,
            retryWrites=True,
            retryReads=True
        )
        client.server_info() # Trigger connection
        client.close()
        
        await msg.edit_text("✅ **Connection Successful!**\nNow send a **Name/Alias** for this connection (e.g., 'Production', 'Backup').", parse_mode="Markdown")
        await state.update_data(new_mongo_uri=uri)
        await state.set_state(BotState.waiting_for_mongo_alias)
        
    except Exception as e:
        await msg.edit_text(f"❌ **Connection Failed:**\n`{e}`\n\nPlease try again or Go Back.", parse_mode="Markdown")

@dp.message(BotState.waiting_for_mongo_alias)
async def process_mongo_alias(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back": return await handle_mongodb_btn(message)
    
    alias = message.text.strip()
    
    # Check for Duplicate Alias
    config = load_db_config()
    if alias in config["connections"]:
        await message.answer(f"⚠️ **Duplicate Name!**\nThe name `{alias}` is already used.\nPlease choose a different name.")
        return
        
    data = await state.get_data()
    uri = data.get("new_mongo_uri")
    
    config["connections"][alias] = uri
    
    # Save Timestamp
    import datetime
    now = datetime.datetime.now().strftime("%I:%M %p") # 12h format e.g. 04:30 PM
    if "timestamps" not in config: config["timestamps"] = {}
    config["timestamps"][alias] = now
    
    save_db_config(config)
    
    # AUTO-MIGRATE / CHECK ENV
    migration_report = ""
    try:
        temp_client = pymongo.MongoClient(
            uri,
            serverSelectionTimeoutMS=15000,
            socketTimeoutMS=30000,
            connectTimeoutMS=15000,
            retryWrites=True,
            retryReads=True
        )
        migration_status = migrate_env_vars(temp_client)
        migration_report = f"\n📝 {migration_status}"
        temp_client.close()
    except Exception as e:
        migration_report = f"\n⚠️ Migration Check Failed: {e}"
    
    await message.answer(
        f"✅ **Saved!**\n"
        f"Connection `{alias}` added to configuration.\n"
        f"{migration_report}", 
        parse_mode="Markdown"
    )
    await state.clear()
    await handle_mongodb_btn(message)

# --- SHOW MONGO ---
@dp.message(F.text == "👁️ Show Mongo")
async def show_mongo_btn(message: types.Message):
    if not is_admin(message.from_user.id): return

    config = load_db_config()
    conns = config.get("connections", {})
    timestamps = config.get("timestamps", {})
    active = config.get("active", "")
    
    text = "👁️ **AVAILABLE CONNECTIONS**\n━━━━━━━━━━━━━━━━━━━━\n"
    
    if not conns:
        text += "_No connections found._"
    else:
        for i, (alias, uri) in enumerate(conns.items(), 1):
            status = "✅ (Active)" if alias == active else ""
            added_time = timestamps.get(alias, "Unknown")
            
            # User requested FULL URL (No masking) and Copyable
            # SECURITY NOTE: Only admins can see this now.
            text += f"{i}. <b>{alias}</b> {status}\n   📅 Added: {added_time}\n   <code>{uri}</code>\n\n"            
    await message.answer(text, parse_mode="HTML")

# --- REMOVE MONGO ---
@dp.message(F.text == "🗑️ Remove Mongo")
async def remove_mongo_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    config = load_db_config()
    conns = config.get("connections", {})
    active = config.get("active", "")
    
    conn_list_text = ""
    builder = ReplyKeyboardBuilder()
    
    for i, alias in enumerate(conns.keys(), 1):
        if alias == active:
             conn_list_text += f"{i}. <b>{alias}</b> (Active - Cannot Remove)\n"
        else:
             conn_list_text += f"{i}. <b>{alias}</b>\n"
             builder.add(KeyboardButton(text=str(i)))
            
    builder.adjust(4)
    builder.row(KeyboardButton(text="🔙 Back"))
    
    await message.answer(
        f"🗑️ **REMOVE CONNECTION**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{conn_list_text}\n"
        f"👇 **Tap the number to remove:**", 
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_remove_index)

@dp.message(BotState.waiting_for_remove_index, F.text == "🔙 Back")
async def back_from_remove(message: types.Message, state: FSMContext):
    await state.clear()
    await handle_mongodb_btn(message)

@dp.message(BotState.waiting_for_remove_index, F.text.regexp(r'^\d+$'))
async def process_remove_mongo_index(message: types.Message, state: FSMContext):
    idx_str = message.text.strip()
    target_idx = int(idx_str)
    
    config = load_db_config()
    conns = list(config["connections"].keys())
    
    if target_idx < 1 or target_idx > len(conns):
        await message.answer("⚠️ Invalid Index Number.")
        return
        
    alias = conns[target_idx - 1]
    
    if alias == config.get("active"):
        await message.answer("⚠️ Cannot remove the **Active** connection.", parse_mode="Markdown")
        return
        
    # Ask for confirmation
    await state.update_data(remove_target_alias=alias)
    await state.set_state(BotState.waiting_for_remove_mongo_confirm)
    
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="✅ YES, DELETE"), KeyboardButton(text="🔙 NO, CANCEL"))
    
    await message.answer(
        f"⚠️ **CONFIRM DELETION**\n\n"
        f"Are you sure you want to delete **{alias}**?\n"
        f"<i>This action cannot be undone.</i>",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )

@dp.message(BotState.waiting_for_remove_mongo_confirm)
async def process_remove_mongo_confirm(message: types.Message, state: FSMContext):
    if message.text == "🔙 NO, CANCEL":
        await state.clear()
        await message.answer("❌ Deletion Cancelled.")
        await remove_mongo_btn(message, state) # Return to list
        return
        
    if message.text == "✅ YES, DELETE":
        data = await state.get_data()
        alias = data.get("remove_target_alias")
        
        config = load_db_config()
        if alias in config["connections"]:
            del config["connections"][alias]
            # Remove timestamp too
            if "timestamps" in config and alias in config["timestamps"]:
                 del config["timestamps"][alias]
            
            save_db_config(config)
            await message.answer(f"🗑️ Connection `{alias}` has been **permanently deleted**.", parse_mode="Markdown")
        else:
            await message.answer("⚠️ Connection was already removed or not found.")
            
        await state.clear()
        await remove_mongo_btn(message, state) # Back to list
        return
        
    await message.answer("Please choose YES or NO.")

# --- SWITCH MONGO ---
@dp.message(F.text == "🔄 Switch Mongo")
async def switch_mongo_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    config = load_db_config()
    conns = config.get("connections", {})
    active = config.get("active", "")
    
    # User Request: If only 1 URL (the active one), show alert to ADD first.
    if len(conns) < 2:
        await message.answer(
            "⚠️ **SWITCH OPTION UNAVAILABLE**\n\n"
            "You only have **1** database connection configured.\n"
            "Please **➕ Add Mongo** first to have something to switch to!",
            parse_mode="Markdown"
        )
        return
    
    # User requested Index in buttons: "only index is enough 1 2 3 so on"
    # Strategy: Show List in Text, Buttons are numbers.
    
    conn_list_text = ""
    target_map = {} # Map Index -> Alias
    
    builder = ReplyKeyboardBuilder()
    
    for i, alias in enumerate(conns.keys(), 1):
        status = "✅ (Active)" if alias == active else ""
        conn_list_text += f"{i}. <b>{alias}</b> {status}\n"
        
        if alias != active:
            builder.add(KeyboardButton(text=str(i)))
        else:
             # User Request: "3 bu in buttons 2 3 are displayed fix it please"
             # They want ALL buttons to be visible, even if one is active.
             # We will just show it. If clicked, the handler handles it.
             builder.add(KeyboardButton(text=str(i)))
    
    builder.adjust(4) # Compact grid of numbers
    builder.row(KeyboardButton(text="🔙 Back"))
    
    await message.answer(
        f"🔄 **SWITCH CONNECTION**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{conn_list_text}\n"
        f"👇 **Tap the number to switch:**", 
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )
    # Set state to capture digits only for this context
    await state.set_state(BotState.waiting_for_switch_index)

@dp.message(BotState.waiting_for_switch_index, F.text == "🔙 Back")
async def back_from_switch(message: types.Message, state: FSMContext):
    await state.clear()
    await handle_mongodb_btn(message)

@dp.message(BotState.waiting_for_switch_index, F.text.regexp(r'^\d+$')) # Match digits only
async def process_switch_mongo_index(message: types.Message, state: FSMContext):
    # User sent a number (e.g. "2")
    idx_str = message.text.strip()
    target_idx = int(idx_str)
    
    config = load_db_config()
    conns = list(config["connections"].keys()) # Order is preserved in Python 3.7+
    
    if target_idx < 1 or target_idx > len(conns):
        await message.answer("⚠️ Invalid Index Number.")
        return
        
    # Map 1-based index to 0-based list
    target_alias = conns[target_idx - 1]
    
    # Check if already active
    if target_alias == config.get("active"):
         await message.answer("⚠️ that is already the active connection!")
         return
    
    # State cleared only after successful switch or back? 
    # Actually, we should clear it if successful, or keep it if failed? 
    # Usually clear on success.
    await state.clear() 
         
    target_uri = config["connections"][target_alias]
    msg = await message.answer(f"🔄 **Switching to {target_alias}...**")
    
    try:
        # 1. Connect and Verify
        new_client = pymongo.MongoClient(
            target_uri,
            serverSelectionTimeoutMS=15000,
            socketTimeoutMS=30000,
            connectTimeoutMS=15000,
            retryWrites=True,
            retryReads=True
        )
        new_client.server_info()
        
        # 2. Migrate Env/Secrets (Idempotent Check)
        migration_status = migrate_env_vars(new_client)
        
        # 3. Update Config
        config["active"] = target_alias
        save_db_config(config)
        
        # 4. Update .env
        update_env_file("MONGO_URI", target_uri)
        
        # 5. Update Runtime Global
        global MONGO_URI, db_client
        MONGO_URI = target_uri
        
        # 6. Reconnect Runtime
        if connect_db():
            await msg.delete() # Clean up status message
            await message.answer(
                f"✅ **SWITCH COMPLETE!**\n"
                f"Now connected to: `{target_alias}`\n"
                f"📝 {migration_status}\n"
                f"♻️ **Bot Runtime Reloaded.**",
                parse_mode="Markdown"
            )
            # User requested: "gets udpated in displlaay lsit as well properly"
            # So we show the updated list immediately.
            await switch_mongo_btn(message, state) # Show the switch menu again (updated)
        else:
             await msg.edit_text("⚠️ Switched `.env` but Runtime Reconnection Failed. Please Restart Bot.")
             
    except Exception as e:
        await msg.edit_text(f"❌ **Switch Failed:** `{e}`", parse_mode="Markdown")

# --- ENV VARIABLES MENU ---
@dp.message(F.text == "🔐 Env Variables")
async def handle_env_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="👁️ View Files"), KeyboardButton(text="📝 Replace File"))
    builder.row(KeyboardButton(text="🔙 Back"), KeyboardButton(text="🏠 Main Menu"))
    
    await message.answer(
        "🔐 **ENV FILE MANAGER**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Manage your configuration files directly.\n\n"
        "👁️ **View**: See content of `.env`, `credentials.json`, etc.\n"
        "📝 **Replace**: Overwrite files with new text or uploads.",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )

# Common File List for Operations
ENV_FILES = ["credentials.json", "token.pickle", "db_config.json", ".env"]

# --- VIEW FILES ---
@dp.message(F.text == "👁️ View Files")
async def view_files_menu(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    builder = ReplyKeyboardBuilder()
    text = "👁️ **SELECT FILE TO VIEW**\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for i, fname in enumerate(ENV_FILES, 1):
        if os.path.exists(fname):
            size = os.path.getsize(fname)
            text += f"{i}. <b>{fname}</b> ({size} bytes)\n"
            builder.add(KeyboardButton(text=f"👁️ {i}. {fname}"))
        else:
            text += f"{i}. {fname} (Missing)\n"
            
    builder.adjust(2)
    builder.row(KeyboardButton(text="🔙 Back"))
    
    await message.answer(text, reply_markup=builder.as_markup(resize_keyboard=True), parse_mode="HTML")

@dp.message(F.text.startswith("👁️ "))
async def process_view_file(message: types.Message):
    # This handler catches the button clicks from View Menu
    # Format: "👁️ 1. filename"
    try:
        if ". " not in message.text: return # Ignore non-file buttons
        parts = message.text.replace("👁️ ", "").split(". ", 1)
        fname = parts[1]
    except:
        return
        
    if not os.path.exists(fname):
        await message.answer("❌ File not found.")
        return
        
    # Check if binary
    if fname.endswith(".pickle"):
        size = os.path.getsize(fname)
        await message.answer(f"📦 **Binary File** (`{fname}`)\nSize: {size} bytes\n_Cannot display binary content as text._", parse_mode="Markdown")
        return
        
    try:
        if fname != ".env":
            with open(fname, "r", encoding='utf-8') as f:
                content = f.read()
        else:
            # STRICT FILTERING FOR .ENV
            # User request: "only bot 4 related items"
            allowed_keys = ["MONGO_URI", "BOT_TOKEN", "OWNER_ID", "PARENT_FOLDER_ID", "PORT"]
            content = ""
            with open(".env", "r", encoding='utf-8') as f:
                for line in f:
                    # Check if line starts with any allowed key
                    if any(line.strip().startswith(k + "=") for k in allowed_keys):
                        content += line
                    elif not line.strip(): 
                         pass # Skip empty lines in filtered view or keep? Let's skip to be clean.
            
            if not content: content = "# No Bot 4 relevant keys found in .env"
            
        if len(content) > 3000:
            # Send as file if too long
            f = FSInputFile(fname)
            await message.reply_document(f, caption=f"📄 **{fname}** (Too long to print)")
        else:
            # Escape HTML tags for safety
            import html
            safe_content = html.escape(content)
            await message.answer(f"📄 **{fname}** (Filtered View)\n━━━━━━━━━━━━━━━━━━━━\n<pre>{safe_content}</pre>\n\n_Note: Only showing Bot 4 related keys._", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Error reading file: {e}")

# --- REPLACE FILES ---
@dp.message(F.text == "📝 Replace File")
async def replace_files_menu(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    
    builder = ReplyKeyboardBuilder()
    text = "📝 **SELECT FILE TO REPLACE**\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for i, fname in enumerate(ENV_FILES, 1):
        exists = "✅" if os.path.exists(fname) else "❌"
        text += f"{i}. <b>{fname}</b> {exists}\n"
        builder.add(KeyboardButton(text=f"📝 {i}. {fname}"))
            
    builder.adjust(2)
    builder.row(KeyboardButton(text="🔙 Back"))
    
    await message.answer(text, reply_markup=builder.as_markup(resize_keyboard=True), parse_mode="HTML")
    await state.set_state(BotState.waiting_for_env_file_selection)

@dp.message(BotState.waiting_for_env_file_selection, F.text.startswith("📝 "))
async def process_replace_selection(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back":
        await state.clear()
        await handle_env_btn(message)
        return

    try:
        if ". " not in message.text: return
        parts = message.text.replace("📝 ", "").split(". ", 1)
        fname = parts[1]
    except:
        await message.answer("⚠️ Selection error.")
        return
        
    await state.update_data(replace_target=fname)
    
    # GRANULAR EDIT LOGIC FOR .ENV
    if fname == ".env":
        # Parse Keys & Content for Preview
        found_keys = []
        env_content_display = ""
        allowed_keys = ["MONGO_URI", "BOT_TOKEN", "OWNER_ID", "PARENT_FOLDER_ID", "PORT"]
        
        try:
             with open(fname, "r", encoding="utf-8") as f:
                 lines = f.readlines()
                 
                 # 1. Build Keys List
                 for line in lines:
                     if "=" in line and not line.strip().startswith("#"):
                         key = line.split("=", 1)[0].strip()
                         if key: found_keys.append(key)
                 
                 # 2. Build Filtered Display (Reuse logic)
                 filtered_lines = []
                 for line in lines:
                     if any(line.strip().startswith(k + "=") for k in allowed_keys):
                         filtered_lines.append(line)
                 
                 if filtered_lines:
                     import html
                     safe_env = html.escape("".join(filtered_lines))
                     env_content_display = f"\n<pre>{safe_env}</pre>"
                 else:
                     env_content_display = "_(No Bot 4 keys found)_"

        except Exception as e: 
            env_content_display = f"_(Error reading file: {e})_"
            pass
        
        # Only offer granular if we found relevant keys
        relevant_keys = [k for k in found_keys if k in allowed_keys]
        
        if relevant_keys:
            builder = ReplyKeyboardBuilder()
            for k in relevant_keys:
                builder.add(KeyboardButton(text=f"🔑 {k}"))
            
            builder.adjust(2)
            builder.row(KeyboardButton(text="📄 Replace Entire File"))
            builder.row(KeyboardButton(text="🔙 Back"))
            
            await message.answer(
                f"📝 **Editing `{fname}`**\n"
                f"**Current Configuration:**\n{env_content_display}\n\n"
                f"Select a **Specific Variable** to edit, or replace the whole file.",
                reply_markup=builder.as_markup(resize_keyboard=True),
                parse_mode="HTML"
            )
            await state.set_state(BotState.waiting_for_granular_option)
            return

    # Default (Non-.env or No Keys found) -> Full Replace
    # SHOW CURRENT CONTENT (Limited)
    curr_content_display = ""
    try:
        with open(fname, "r", encoding="utf-8") as f:
            raw = f.read(2048) # Read first 2kb
            import html
            safe = html.escape(raw)
            if len(raw) >= 2048: safe += "..."
            curr_content_display = f"\n<pre>{safe}</pre>"
    except: curr_content_display = "_(Binary or unreadable)_"

    await message.answer(
        f"📝 **Replacing `{fname}`**\n"
        f"**Current Content:**\n{curr_content_display}\n\n"
        "Please send the **NEW CONTENT**:\n"
        "• Send **Text Message** to paste content.\n"
        "• Or **Upload a File** to overwrite it.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Cancel")]], resize_keyboard=True)
    )
    await state.set_state(BotState.waiting_for_env_replacement_content)

@dp.message(BotState.waiting_for_env_value)
async def process_env_value(message: types.Message, state: FSMContext):
    if message.text == "❌ Cancel": return await handle_env_btn(message)
    
    new_value = message.text.strip()
    data = await state.get_data()
    key = data.get("env_key")
    
    # 0. Capture Old Value
    old_value = os.environ.get(key, "Not Set")
    
    # 1. Update Local File
    update_env_file(key, new_value)
    
    # 2. Hot-reload check
    os.environ[key] = new_value
    
    # 3. Update MongoDB (CRITICAL for Bot 4 Secrets)
    db_status = ""
    if key in ["BOT_TOKEN", "OWNER_ID", "PARENT_FOLDER_ID"]:
        try:
             client = pymongo.MongoClient(
                 os.getenv("MONGO_URI"),
                 serverSelectionTimeoutMS=15000,
                 socketTimeoutMS=30000,
                 connectTimeoutMS=15000,
                 retryWrites=True,
                 retryReads=True
             )
             db = client["MSANodeDB"]
             col = db["bot_secrets"]
             col.update_one(
                 {"bot": "bot4"},
                 {"$set": {key: new_value}},
                 upsert=True
             )
             client.close()
             db_status = "\n✅ **Synced to MongoDB Secrets**"
        except Exception as e:
             db_status = f"\n⚠️ **DB Sync Failed:** {e}"
    
    # 4. Read NEW Full Content for Feedack
    final_view = ""
    allowed_keys = ["MONGO_URI", "BOT_TOKEN", "OWNER_ID", "PARENT_FOLDER_ID", "PORT"]
    try:
        with open(".env", "r", encoding="utf-8") as f:
             lines = f.readlines()
             filtered = [l for l in lines if any(l.strip().startswith(k + "=") for k in allowed_keys)]
             import html
             safe_final = html.escape("".join(filtered))
             final_view = f"<pre>{safe_final}</pre>"
    except: final_view = "(Could not read file)"

    # SHOW DIFF & FULL RESULT
    await message.answer(
        f"✅ **VARIABLE UPDATED**\n"
        f"Key: <code>{key}</code>\n"
        f"Old: <code>{old_value}</code>\n"
        f"New: <code>{new_value}</code>\n"
        f"{db_status}\n\n"
        f"**📄 Full New Config:**\n{final_view}", 
        parse_mode="HTML"
    )
    await asyncio.sleep(2) # Brief pause for reading
    await state.clear()
    
    # 5. SELF-RESTART if Identity Changed
    if key == "BOT_TOKEN":
        await message.answer(
            "⚠️ **IDENTITY CHANGE DETECTED**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "The Bot Token has been updated.\n"
            "I must **RESTART** now to apply the new identity.\n\n"
            "<i>System is rebooting... (Approx 5s)</i>", 
            parse_mode="HTML"
        )
        await asyncio.sleep(2) # Allow message to send
        print(f"🔄 RESTARTING BOT DUE TO TOKEN CHANGE ({new_value[:10]}...)")
        
        # Robust Restart Logic (Hybrid)
        import subprocess
        
        python = sys.executable
        script_path = os.path.abspath("bot4.py")
        
        print(f"🔄 RESTARTING: {python} {script_path}")
        
        if sys.platform == 'win32':
            # Windows: execv can be flaky with spaces/paths. Spawn new, kill old.
            subprocess.Popen([python, script_path])
            os._exit(0) # Exit immediately without cleanup/traceback
        else:
            # Linux/Render: execv is standard and cleaner (replaces PID)
            os.execv(python, [python, script_path])
    
    # implied by context, go back to env menu
    await handle_env_btn(message)



@dp.message(BotState.waiting_for_granular_option)
async def process_granular_option(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back":
        await replace_files_menu(message, state) # Go back to file list
        return
        
    if text == "📄 Replace Entire File":
        # Proceed to full replace logic
        data = await state.get_data()
        fname = data.get("replace_target")
        
        # Show Content Logic
        curr_content_display = ""
        try:
            with open(fname, "r", encoding="utf-8") as f:
                raw = f.read(2048)
                import html
                safe = html.escape(raw)
                if len(raw) >= 2048: safe += "..."
                curr_content_display = f"\n<pre>{safe}</pre>"
        except: curr_content_display = "_(Binary or unreadable)_"
        
        await message.answer(
            f"📝 **Replacing `{fname}` (FULL)**\n"
            f"**Current Content:**\n{curr_content_display}\n\n"
            "Please send the **NEW CONTENT** (Text or File).",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Cancel")]], resize_keyboard=True)
        )
        await state.set_state(BotState.waiting_for_env_replacement_content)
        return
        
    if text.startswith("🔑 "):
        # Specific Key Selected
        key = text.replace("🔑 ", "").strip()
        await state.update_data(env_key=key)
        
        # Fetch current value for display
        current_val = os.getenv(key, "Not Set / Empty")
        
        await message.answer(
            f"🔑 **Editing Variable: `{key}`**\n"
            f"**Current Value:**\n<code>{current_val}</code>\n\n"
            f"👇 Send the **NEW VALUE** now:",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Cancel")]], resize_keyboard=True),
            parse_mode="HTML"
        )
        # Reuse existing handler which does Sync
        await state.set_state(BotState.waiting_for_env_value)
    else:
        await message.answer("⚠️ Invalid Option.")

@dp.message(BotState.waiting_for_env_replacement_content)
async def process_replace_content(message: types.Message, state: FSMContext):
    if message.text == "❌ Cancel":
        await state.clear()
        await handle_env_btn(message)
        return

    data = await state.get_data()
    target_file = data.get("replace_target")
    result = "Unknown"
    
    try:
        # Handle File Upload
        if message.document:
            file_id = message.document.file_id
            file = await bot.get_file(file_id)
            # Download and overwrite
            # Note: aiogram download_file return BytesIO or path
            await bot.download_file(file.file_path, destination=target_file)
            result = "File Uploaded"
            
        # Handle Text Paste
        elif message.text:
            with open(target_file, "w", encoding='utf-8') as f:
                f.write(message.text)
            result = "Text Content Updated"
        else:
             await message.answer("⚠️ Please send Text or File.")
             return
            
        await message.answer(f"✅ **Success!**\n`{target_file}` has been updated.\n({result})", parse_mode="Markdown")
        
        # Reload Dotenv if .env changed
        if target_file == ".env":
            load_dotenv(override=True)
            await message.answer("♻️ Environment Variables Reloaded.")
            
    except Exception as e:
        await message.answer(f"❌ **Update Failed:** `{e}`", parse_mode="Markdown")
        
    await state.clear()
    await handle_env_btn(message)

# --- BACK NAVIGATION ---
@dp.message(F.text == "🔙 Back")
async def back_router(message: types.Message, state: FSMContext):
    await handle_databases_btn(message)
    
@dp.message(F.text == "🏠 Main Menu")
async def main_menu_return(message: types.Message, state: FSMContext):
    await start(message, state)

# --- ADD TEXT VAR ---
@dp.message(F.text == "➕ Add Env Var")
async def add_env_text_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "📝 **ENTER VARIABLE KEY**\n"
        "Example: `BOT_TOKEN`, `MONGO_URI`\n"
        "Send the **KEY** name now:",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Cancel")]], resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.waiting_for_env_key)

# ... (Key/Value states remain same) ...

# --- UPLOAD FILE ---
@dp.message(F.text == "➕ Upload File")
async def add_env_file_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "📂 **UPLOAD CONFIG FILE**\n"
        "Supported: `credentials.json`, `token.pickle`, `.env`\n\n"
        "👇 **Send the file now:**",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Cancel")]], resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.waiting_for_env_file)

@dp.message(BotState.waiting_for_env_key)
async def process_env_key(message: types.Message, state: FSMContext):
    if message.text == "❌ Cancel": return await handle_env_btn(message)
    
    key = message.text.strip().upper()
    # Simple validation
    if " " in key or "=" in key:
        await message.answer("⚠️ Invalid Key format. Use UPPERCASE_UNDERSCORE only.")
        return
        
    await state.update_data(env_key=key)
    await message.answer(f"🔑 **Key:** `{key}`\nNow send the **VALUE**:", parse_mode="Markdown")
    await state.set_state(BotState.waiting_for_env_value)

@dp.message(BotState.waiting_for_env_value)
async def process_env_value(message: types.Message, state: FSMContext):
    if message.text == "❌ Cancel": return await handle_env_btn(message)
    
    new_value = message.text.strip()
    data = await state.get_data()
    key = data.get("env_key")
    
    # 0. Capture Old Value
    old_value = os.environ.get(key, "Not Set")
    
    # 1. Update Local File
    update_env_file(key, new_value)
    
    # 2. Hot-reload check
    os.environ[key] = new_value
    
    # 3. Update MongoDB (CRITICAL for Bot 4 Secrets)
    db_status = ""
    if key in ["BOT_TOKEN", "OWNER_ID", "PARENT_FOLDER_ID"]:
        try:
             client = pymongo.MongoClient(
                 os.getenv("MONGO_URI"),
                 serverSelectionTimeoutMS=15000,
                 socketTimeoutMS=30000,
                 connectTimeoutMS=15000,
                 retryWrites=True,
                 retryReads=True
             )
             db = client["MSANodeDB"]
             col = db["bot_secrets"]
             col.update_one(
                 {"bot": "bot4"},
                 {"$set": {key: new_value}},
                 upsert=True
             )
             client.close()
             db_status = "\n✅ **Synced to MongoDB Secrets**"
        except Exception as e:
             db_status = f"\n⚠️ **DB Sync Failed:** {e}"
    
    # 4. Read NEW Full Content for Feedback
    final_view = ""
    allowed_keys = ["MONGO_URI", "BOT_TOKEN", "OWNER_ID", "PARENT_FOLDER_ID", "PORT"]
    try:
        with open(".env", "r", encoding="utf-8") as f:
             lines = f.readlines()
             filtered = [l for l in lines if any(l.strip().startswith(k + "=") for k in allowed_keys)]
             import html
             safe_final = html.escape("".join(filtered))
             final_view = f"<pre>{safe_final}</pre>"
    except: final_view = "(Could not read file)"

    # SHOW DIFF & FULL RESULT
    await message.answer(
        f"✅ **VARIABLE UPDATED**\n"
        f"Key: <code>{key}</code>\n"
        f"Old: <code>{old_value}</code>\n"
        f"New: <code>{new_value}</code>\n"
        f"{db_status}\n\n"
        f"**📄 New Config State:**\n{final_view}", 
        parse_mode="HTML"
    )
    await asyncio.sleep(2) # Brief pause for reading
    await state.clear()
    
    # implied by context, go back to env menu
    await handle_env_btn(message)

# --- ADD FILE ---
@dp.message(F.text == "📄 Upload File")
async def add_env_file_btn(message: types.Message, state: FSMContext):
    await message.answer(
        "📄 **UPLOAD FILE**\n"
        "Send the file you want to save (e.g., `credentials.json`).\n"
        "<i>It will overwrite any existing file with the same name!</i>",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back")]], resize_keyboard=True),
        parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_env_file)

@dp.message(BotState.waiting_for_env_file, F.document)
async def process_env_file_upload(message: types.Message, state: FSMContext):
    doc = message.document
    file_name = doc.file_name
    
    valid_files = ["credentials.json", "token.pickle", "db_config.json", ".env"]
    
    if file_name not in valid_files and not file_name.endswith(".json"):
        await message.answer("⚠️ **Warning:** Uncommon file name. Saving anyway...")
    
    file_id = doc.file_id
    file = await bot.get_file(file_id)
    file_path = file.file_path
    
    destination = file_name
    await bot.download_file(file_path, destination)
    
    await message.answer(f"✅ **FILE SAVED:** `{file_name}`", parse_mode="Markdown")
    await state.clear()
    await handle_env_btn(message)


# ==========================================
# 🛡️ SYSTEM HEALTH & MONITORING
# ==========================================

@dp.error()
async def global_error_handler(event: types.ErrorEvent):
    """
    Catches ALL unhandled exceptions from handlers.
    Notifies Owner immediately with traceback.
    """
    exception = event.exception
    
    # Get traceback
    tb_list = traceback.format_exception(type(exception), exception, exception.__traceback__)
    tb_str = "".join(tb_list)[-1000:] # Last 1000 chars
    
    alert = (
        f"🚨 **CRITICAL SYSTEM FAILURE**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ **Exception:** `{type(exception).__name__}`\n"
        f"📜 **Details:** `{str(exception)}`\n\n"
        f"💻 **Traceback (Last 1k chars):**\n"
        f"```\n{tb_str}\n```\n"
        f"🛑 **Action:** Automated Report Sent."
    )
    
    try:
        await bot.send_message(OWNER_ID, alert, parse_mode="Markdown")
    except:
        print("Failed to send Error Alert to Owner.")
        
    # We log it but do not crash the bot
    print(f"Global Error Caught: {exception}")


async def auto_health_monitor():
    """
    Runs every 5 minutes to deep-scan critical connections.
    If DB or Drive fails, alerts Owner.
    """
    print("🛡️ Health Monitor: Online")
    while True:
        await asyncio.sleep(300) # 5 Minutes
        
        issues = []
        
        # 1. DB CHECK
        try:
            t0 = time.time()
            col_admin_test = db_client.admin.command('ping')
            lat = (time.time() - t0) * 1000
            if lat > 2000: issues.append(f"High DB Latency: {lat:.0f}ms")
        except Exception as e:
            issues.append(f"Database DOWN: {e}")
            
        # 2. DRIVE CHECK
        try:
            # Simple metadata call
            # We skip full auth scan to save API quota, just check service obj
            if not get_drive_service():
                 issues.append("Drive Service: NoneType (Auth Failed)")
        except Exception as e:
            issues.append(f"Drive API Error: {e}")
            
        # 3. REPORT IF ISSUES
        if issues:
            report = (
                f"⚠️ **AUTO-CHECKUP WARNING**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Your system has detected irregularities:\n"
            )
            for i in issues: report += f"• 🔴 {i}\n"
            report += f"\n🕐 Time: {datetime.now().strftime('%I:%M %p')}"
            
            try: await bot.send_message(OWNER_ID, report, parse_mode="Markdown")
            except: pass

# ==========================================
# 🕰️ STRICT DAILY REPORTS (12H · TIMEZONE-AWARE)
# ==========================================
async def reset_daily_stats():
    """Resets DAILY_STATS_BOT4 at midnight local time every day."""
    global DAILY_STATS_BOT4
    print("🔄 Daily Stats Reset Loop: Online")
    while True:
        now = now_local()
        # Time until next midnight
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        wait_secs = (tomorrow - now).total_seconds()
        await asyncio.sleep(max(wait_secs, 1))
        DAILY_STATS_BOT4 = {"pdfs_generated": 0, "pdfs_deleted": 0, "errors": 0, "links_retrieved": 0}
        print(f"🔄 Daily stats reset at {now_local().strftime('%I:%M %p')}")


async def strict_daily_report():
    """
    Sends a detailed report EXACTLY at 08:40 AM and 08:40 PM (local timezone).
    Uses now_local() so timezone is always correct.
    """
    print("🕰️ Strict Daily Report: Online (08:40 AM/PM)")
    while True:
        now = now_local()
        current_time = now.strftime("%I:%M %p")  # e.g. "08:40 AM"

        if current_time in ["08:40 AM", "08:40 PM"]:
            try:
                # GATHER METRICS
                uptime_secs = int(time.time() - START_TIME)
                uptime_str = f"{uptime_secs // 3600}h {(uptime_secs % 3600) // 60}m"
                admin_count = col_admins.count_documents({}) if col_admins is not None else 0
                banned_count = col_banned.count_documents({}) if col_banned is not None else 0
                pdf_count = col_pdfs.count_documents({}) if col_pdfs is not None else 0
                locked_count = col_locked.count_documents({}) if col_locked is not None else 0
                trash_count = col_trash.count_documents({}) if col_trash is not None else 0

                # Check Drive
                drive_status = "🟢 Online"
                try:
                    if not get_drive_service():
                        drive_status = "🔴 Offline (Auth Failed)"
                except Exception:
                    drive_status = "🔴 Error"

                # Check DB with latency
                db_status = "🟢 Online"
                try:
                    t0 = time.time()
                    db_client.admin.command('ping')
                    lat = (time.time() - t0) * 1000
                    db_status = f"🟢 Connected ({lat:.0f}ms)"
                except Exception:
                    db_status = "🔴 Offline"

                report = (
                    f"📅 **DAILY SYSTEM REPORT · BOT 4**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🕐 **Time:** `{current_time}`  |  📆 `{now.strftime('%b %d, %Y')}`\n"
                    f"⚙️ **System:** 🟢 OPERATIONAL  |  ⏱ Uptime: `{uptime_str}`\n\n"
                    f"📊 **LIBRARY:**\n"
                    f"• 📚 PDFs Active: `{pdf_count}`\n"
                    f"• 🔒 Locked Content: `{locked_count}`\n"
                    f"• 🗑️ Recycle Bin: `{trash_count}`\n\n"
                    f"👤 **USERS:**\n"
                    f"• 👥 Admins: `{admin_count}`\n"
                    f"• 🚫 Blacklisted: `{banned_count}`\n\n"
                    f"📈 **TODAY'S ACTIVITY:**\n"
                    f"• 📄 PDFs Generated: `{DAILY_STATS_BOT4['pdfs_generated']}`\n"
                    f"• 🔗 Links Retrieved: `{DAILY_STATS_BOT4['links_retrieved']}`\n"
                    f"• 🗑️ PDFs Deleted: `{DAILY_STATS_BOT4['pdfs_deleted']}`\n"
                    f"• ⚠️ Errors Today: `{DAILY_STATS_BOT4['errors']}`\n\n"
                    f"🛡️ **INFRASTRUCTURE:**\n"
                    f"• ☁️ Google Drive: {drive_status}\n"
                    f"• 🗄️ MongoDB Atlas: {db_status}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💎 **MSA NODE SYSTEMS** | Verified."
                )

                await bot.send_message(OWNER_ID, report, parse_mode="Markdown")
                print(f"✅ Daily Report Sent at {current_time}")

                # Sleep 65s to avoid double-fire within the same minute
                await asyncio.sleep(65)

            except Exception as e:
                print(f"Daily Report Failed: {e}")
                await asyncio.sleep(60)
        else:
            # Check every 30 seconds
            await asyncio.sleep(30)


# ==========================================
# 📅 MONTHLY AUTO-BACKUP (1st of each month · 03:30 AM local)
# ==========================================
async def monthly_backup():
    """
    Runs on the 1st of every month at 03:30 AM local time.
    Creates a full JSON dump + text report with proper Month Year labeling.
    Stores a summary record in MongoDB and sends files to Owner.
    Dedup-guard: skips if this month's backup already exists.
    """
    print("📅 Monthly Backup Scheduler: Online")
    while True:
        now = now_local()
        # Target: 1st of next month at 03:30 AM
        if now.month == 12:
            first_next = now.replace(year=now.year + 1, month=1, day=1, hour=3, minute=30, second=0, microsecond=0)
        else:
            first_next = now.replace(month=now.month + 1, day=1, hour=3, minute=30, second=0, microsecond=0)

        # If we're past 03:30 on the 1st this month, skip to next month; otherwise
        # check if we should fire today (i.e. today IS the 1st and it's before 03:30)
        now_on_first = now.replace(day=1, hour=3, minute=30, second=0, microsecond=0)
        if now < now_on_first:
            target = now_on_first
        else:
            target = first_next

        wait_secs = (target - now).total_seconds()
        print(f"📅 Monthly Backup scheduled in {wait_secs/3600:.1f} hours (on {target.strftime('%b %d, %Y at %I:%M %p')})")
        await asyncio.sleep(max(wait_secs, 1))

        # Dedup: skip if already backed up this month
        fire_now = now_local()
        month_key = fire_now.strftime("%Y-%m")
        try:
            db = db_client["MSANodeDB"]
            existing = db["bot4_monthly_backups"].find_one({"month_key": month_key})
            if existing:
                print(f"📅 Monthly Backup for {month_key} already exists — skipping.")
                await asyncio.sleep(120)
                continue
        except Exception:
            pass

        try:
            month_label = fire_now.strftime("%B %Y")   # e.g. "February 2026"
            date_label  = fire_now.strftime("%Y-%m-%d")

            # ── TEXT REPORT ──────────────────────────────────
            txt_filename = await asyncio.to_thread(generate_system_backup)

            # ── JSON DUMP ────────────────────────────────────
            json_filename = f"MSANODE_MONTHLY_{date_label}.json"
            data = {
                "backup_type":  "monthly",
                "month":        month_label,
                "month_key":    month_key,
                "generated_at": fire_now.strftime("%b %d, %Y  ·  %I:%M %p"),
                "pdfs":         list(col_pdfs.find({}, {"_id": 0}))         if col_pdfs         is not None else [],
                "admins":       list(col_admins.find({}, {"_id": 0}))       if col_admins       is not None else [],
                "banned":       list(col_banned.find({}, {"_id": 0}))       if col_banned       is not None else [],
                "trash":        list(col_trash.find({}, {"_id": 0}))        if col_trash        is not None else [],
                "locked":       list(col_locked.find({}, {"_id": 0}))       if col_locked       is not None else [],
                "trash_locked": list(col_trash_locked.find({}, {"_id": 0})) if col_trash_locked is not None else [],
            }
            with open(json_filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, default=str)

            # Upload both to Drive
            txt_link  = await asyncio.to_thread(upload_to_drive, txt_filename)  if txt_filename  else "N/A"
            json_link = await asyncio.to_thread(upload_to_drive, json_filename)

            caption = (
                f"📅 <b>MONTHLY AUTO-BACKUP · {month_label}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📆 <b>Date:</b> {fire_now.strftime('%b %d, %Y  ·  %I:%M %p')}\n"
                f"📊 <b>PDFs:</b> {len(data['pdfs'])} | <b>Admins:</b> {len(data['admins'])} | <b>Banned:</b> {len(data['banned'])}\n"
                f"☁️ <a href='{txt_link}'><b>Text Report on Drive</b></a>\n"
                f"☁️ <a href='{json_link}'><b>Full JSON Dump on Drive</b></a>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <i>All data for {month_label} secured.</i>"
            )

            # Send JSON to Owner
            await bot.send_document(
                OWNER_ID,
                FSInputFile(json_filename),
                caption=caption,
                parse_mode="HTML"
            )

            # Store summary record in MongoDB (dedup guard)
            try:
                db = db_client["MSANodeDB"]
                db["bot4_monthly_backups"].update_one(
                    {"month_key": month_key},
                    {"$set": {
                        "month_key":  month_key,
                        "month":      month_label,
                        "date":       fire_now,
                        "pdf_count":  len(data['pdfs']),
                        "txt_link":   txt_link,
                        "json_link":  json_link,
                    }},
                    upsert=True
                )
            except Exception as e:
                print(f"⚠️ Monthly backup DB record failed: {e}")

            print(f"✅ Monthly Backup Completed — {month_label}")

            # Cleanup local files
            for f in [txt_filename, json_filename]:
                if f and os.path.exists(f):
                    try: os.remove(f)
                    except: pass

        except Exception as e:
            await notify_error_bot4("Monthly Backup Failed", str(e))

        # Sleep 2 hours after firing to avoid re-trigger
        await asyncio.sleep(7200)

async def main():
    # Retry loop for network startup
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.set_my_commands([BotCommand(command="start", description="Menu")])
            break
        except Exception as e:
            print(f"⚠️ Network Startup Error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
    
    asyncio.create_task(auto_janitor())
    asyncio.create_task(weekly_backup())
    asyncio.create_task(monthly_backup())           # 1st of each month · 03:30 AM
    asyncio.create_task(system_guardian())
    asyncio.create_task(reset_daily_stats())        # Midnight stat reset
    asyncio.create_task(strict_daily_report())      # 08:40 AM/PM live timezone
    asyncio.create_task(auto_health_monitor())      # Every 5 minutes deep-scan
    
    print("💎 MSANODE BOT 4 ONLINE")
    
    # 🧬 Define Heartbeat Loop (Nested)
    async def heartbeat_animation(chat_id, message_id):
        """Keeps the startup message alive with a 'breathing' pulse."""
        states = [
            "💓 <b>Pulse:</b> ACTIVE",
            "💗 <b>Pulse:</b> BEATING",
            "💖 <b>Pulse:</b> VITAL",
            "💞 <b>Pulse:</b> ALIVE"
        ]
        idx = 0
        while True:
            try:
                await asyncio.sleep(6) # 6s delay
                idx = (idx + 1) % len(states)
                pulse = states[idx]
                
                text = (
                    "💎 <b>MSA NODE BOT 4: ONLINE</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    "🟢 <b>Status:</b> ALIVE\n"
                    "🫁 <b>Breath:</b> STABLE\n"
                    f"{pulse}\n\n"
                    "<i>I am awake and ready to serve, Master.</i>"
                )
                
                await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode="HTML")
            except Exception as e:
                await asyncio.sleep(30) # Backoff

    try: 
        # 🎥 LIVE ANIMATION (STICKER)
        # Attempt to send a sticker. If invalid ID, ignores it.
        try:
            # Placeholder ID - Replace with valid one if desired
            # await bot.send_sticker(OWNER_ID, "CAACAgIAAxkBAAEgGqBlz9t6...")
             pass
        except: pass 

        # 🧬 "Breathing" Boot Animation Loop
        boot_msg = await bot.send_message(OWNER_ID, "🔌 **CONNECTING TO NEURAL NET...**")
        await asyncio.sleep(0.7)
        
        await boot_msg.edit_text("🧠 **SYNAPSES: FIRING**")
        await asyncio.sleep(0.7)
        
        await boot_msg.edit_text("🫁 **SYSTEM RESPIRATION: INITIALIZED**")
        await asyncio.sleep(0.7)
        
        await boot_msg.edit_text("💓 **HEARTBEAT: SYNCHRONIZED**")
        await asyncio.sleep(0.7)
        
        await boot_msg.edit_text(
            "💎 <b>MSA NODE BOT 4: ONLINE</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🟢 <b>Status:</b> ALIVE\n"
            "🫁 <b>Breath:</b> STABLE\n"
            "💓 <b>Pulse:</b> ACTIVE\n\n"
            "<i>I am awake and ready to serve, Master.</i>", 
            parse_mode="HTML"
        )
        
        # 🟢 START CONTINUOUS LIFE LOOP
        asyncio.create_task(heartbeat_animation(OWNER_ID, boot_msg.message_id))
        
    except Exception as e:
        print(f"Startup notify failed: {e}")

    try:
        # 🔄 Polling Loop (Robust)
        while True:
            try:
                await dp.start_polling(bot, skip_updates=True)
                print("⚠️ Polling loop returned. Restarting...")
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise  # bubble up to finally
            except Exception as e:
                logging.error(f"Polling Network Error: {e}. Retrying in 5s...")
                await asyncio.sleep(5)
    finally:
        # 🔴 SHUTDOWN NOTIFICATION
        try:
            await bot.send_message(
                OWNER_ID,
                f"🔴 **BOT 4 — GOING OFFLINE**\n\n"
                f"🟠 **Status:** Shutting down\n"
                f"📅 **Time:** {datetime.now().strftime('%B %d, %Y — %I:%M:%S %p')}\n\n"
                f"_Bot 4 has stopped. Restart me if needed._",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass

# ==========================================
# 🗄️ DATABASE HANDLERS (Main Bot)
# ==========================================
class DBState(StatesGroup):
    waiting_for_mongo_url = State()

@dp.message(F.text == "🗄️ Databases")
async def database_menu_main(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("⛔ **ACCESS DENIED:** Only the Supreme Owner can access the Core Database.")
        return
        
    masked_uri = MONGO_URI.split("@")[-1] if MONGO_URI and "@" in MONGO_URI else "Unknown"
    
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="👁️ Show Full Connection String"))
    builder.row(KeyboardButton(text="🔌 Switch/Update Database"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        f"🗄️ **CORE DATABASE CONTROL**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 **Current:** `...{masked_uri}`\n\n"
        f"⚠️ **Warning:** Changing this will switch the Bot's entire memory.",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )

@dp.message(F.text == "👁️ Show Full Connection String")
async def show_full_db_uri(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    # Send as monospaced code for easy copying, but warn
    await message.answer(
        f"🔐 **CORE SECRET REVEALED**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"`{MONGO_URI}`\n\n"
        f"⚠️ **KEEP THIS SAFE.** Do not share this screenshot.",
        parse_mode="Markdown"
    )

@dp.message(F.text == "🔌 Switch/Update Database")
async def switch_db_main(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    await message.answer(
        "🔌 **ENTER NEW MONGODB URI**\n"
        "Format: `mongodb+srv://user:pass@cluster...`\n\n"
        "🔎 I will **verify** the connection first.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
    )
    await state.set_state(DBState.waiting_for_mongo_url)

@dp.message(DBState.waiting_for_mongo_url)
async def process_db_switch(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state) 
    
    # Handle user confusion if they type the command manually
    if "show mongo" in text.lower():
         return await show_full_db_uri(message)

    if not text.startswith("mongodb"):
        await message.answer(
            "❌ **Invalid Format.**\n"
            "Must start with `mongodb://` or `mongodb+srv://`\n\n"
            "Please try again or go 🔙 Back.",
            parse_mode="Markdown"
        )
        return

    status_msg = await message.answer("⏳ **Testing Handshake...**")
    
    try:
        def test_connect():
            # 5s timeout to prevent hanging
            client = pymongo.MongoClient(
                text,
                serverSelectionTimeoutMS=15000,
                socketTimeoutMS=30000,
                connectTimeoutMS=15000,
                retryWrites=True,
                retryReads=True
            )
            return client.server_info()
            
        await asyncio.to_thread(test_connect)
        
        # Update .env
        update_env_file("MONGO_URI", text)
        
        await status_msg.edit_text(
            "✅ **CONNECTION VERIFIED**\n"
            "The database is reachable.\n\n"
            "💾 **Configuration Saved.**\n"
            "🔄 **RESTART REQUIRED** to switch context."
        )
        await state.clear()
        
    except Exception as e:
        await status_msg.edit_text(f"❌ **FAILED:** Unreachable.\nError: `{e}`")

if __name__ == "__main__":
    print("🚀 STARTING INDIVIDUAL CORE TEST: BOT 4")
    
    # prepare_secrets() # Load from DB now
    
    threading.Thread(target=run_health_server, daemon=True).start()
    
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("◈ Bot 4 Shutdown.")
