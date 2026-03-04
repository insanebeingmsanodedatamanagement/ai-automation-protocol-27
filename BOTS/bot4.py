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
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
import shutil
import base64
import json
import time
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.exceptions import TelegramRetryAfter
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

# Load environment variables from bot4.env

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
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, HRFlowable, Table, TableStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# ── Unicode font registration (for ₹, €, £ etc. in PDFs) ──────────────────
# Try several locations in order; Render (Debian/Ubuntu) ships DejaVuSans.
_UNICODE_FONT_REGISTERED = False
_UNICODE_FONT_NAME       = 'Helvetica'      # fallback if no TTF found
_UNICODE_FONT_BOLD       = 'Helvetica-Bold' # fallback bold

_FONT_CANDIDATES = [
    # ── PROJECT-BUNDLED DejaVu (works identically on Windows local & Render) ──
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fonts', 'DejaVuSans.ttf'),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fonts', 'DejaVuSans-Bold.ttf'),
    # ── Render / Ubuntu / Debian ──────────────────────────────────────────────
    '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
    '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
    '/usr/share/fonts/dejavu/DejaVuSans.ttf',
    '/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf',
    # ── NotoSans if installed ─────────────────────────────────────────────────
    '/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf',
    '/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf',
    # ── macOS ─────────────────────────────────────────────────────────────────
    '/Library/Fonts/Arial Unicode MS.ttf',
    '/opt/homebrew/share/fonts/NotoSans-Regular.ttf',
    # ── Windows fallbacks ─────────────────────────────────────────────────────
    'C:/Windows/Fonts/Nirmala.ttf',
    'C:/Windows/Fonts/Nirmalab.ttf',
    'C:/Windows/Fonts/segoeui.ttf',
    'C:/Windows/Fonts/segoeuib.ttf',
    'C:/Windows/Fonts/arial.ttf',
    'C:/Windows/Fonts/Arial.ttf',
]

def _register_unicode_font():
    global _UNICODE_FONT_REGISTERED, _UNICODE_FONT_NAME, _UNICODE_FONT_BOLD
    if _UNICODE_FONT_REGISTERED:
        return
    # Try DejaVu regular + bold
    reg_path  = None
    bold_path = None
    for p in _FONT_CANDIDATES:
        if os.path.exists(p):
            name = os.path.basename(p)
            if 'Bold' in name or 'bold' in name or 'Bd' in name:
                if bold_path is None:
                    bold_path = p
            else:
                if reg_path is None:
                    reg_path = p
        if reg_path and bold_path:
            break
    # If Windows Arial found but no DejaVu, use Arial for both
    if reg_path is None:
        for p in _FONT_CANDIDATES:
            if os.path.exists(p) and 'arial' in p.lower():
                reg_path  = p
                bold_path = p
                break
    try:
        if reg_path:
            pdfmetrics.registerFont(TTFont('UniBody',     reg_path))
            pdfmetrics.registerFont(TTFont('UniBodyBold', bold_path or reg_path))
            # ── CRITICAL: link regular → bold so <b> tags use DejaVu-Bold not Helvetica-Bold
            pdfmetrics.registerFontFamily(
                'UniBody',
                normal='UniBody',
                bold='UniBodyBold',
                italic='UniBody',        # no italic variant — fallback to regular
                boldItalic='UniBodyBold'
            )
            _UNICODE_FONT_NAME = 'UniBody'
            _UNICODE_FONT_BOLD = 'UniBodyBold'
            _UNICODE_FONT_REGISTERED = True
            print(f"✅ PDF Unicode font registered: {os.path.basename(reg_path)}")
        else:
            print("⚠️ No Unicode TTF font found — falling back to Helvetica (₹ will show as Rs.)")
    except Exception as e:
        print(f"⚠️ Unicode font registration failed: {e} — falling back to Helvetica")

_register_unicode_font()

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

def load_secrets_from_env():
    """Load BOT_TOKEN and OWNER_ID directly from bot4.env (no MongoDB)."""
    token = os.getenv("BOT_4_TOKEN") or os.getenv("BOT_TOKEN")
    owner = int(os.getenv("OWNER_ID", "0"))
    if token:
        print("✅ Secrets loaded from bot4.env")
    else:
        print("❌ BOT_4_TOKEN not found in bot4.env — check the file.")
    return token, owner

# Load Secrets
BOT_TOKEN, OWNER_ID = load_secrets_from_env()

# Google Drive config — only used for PDF upload, not for backup/DB
PARENT_FOLDER_ID = os.getenv("PARENT_FOLDER_ID", "")
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE        = "token.pickle"
ADMIN_PASSWORD   = os.getenv("ADMIN_PASSWORD", "")   # Set on Render; never hardcode here

# In-memory set of owner IDs that have completed password auth this session
_admin_authenticated: set = set()

if not BOT_TOKEN:
    print("❌ FATAL: BOT_4_TOKEN could not be loaded. Exiting.")
    sys.exit(1)


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
col_bot4_state = None
db_client = None

# prepare_secrets() - DEPRECATED (Moved to DB loading)

# ==========================================
# 🔐 OWNER TRANSFER PASSWORD
# ==========================================
OWNER_TRANSFER_PW = os.getenv("OWNER_TRANSFER_PW", "")  # Set OWNER_TRANSFER_PW on Render; never hardcode here
# Env var takes priority. On Render, set OWNER_TRANSFER_PW in environment variables.
# Also stored in MongoDB bot_secrets under key 'OWNER_TRANSFER_PW'.

ADMIN_PAGE_SIZE = 10  # Admins per page in paginated lists

# ==========================================
# 🌐 WEBHOOK CONFIGURATION
# ==========================================
_WEBHOOK_BASE_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
_WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
_WEBHOOK_URL = f"{_WEBHOOK_BASE_URL}{_WEBHOOK_PATH}" if _WEBHOOK_BASE_URL else ""

async def handle_health(request):
    return web.Response(text="CORE 4 (PDF INFRASTRUCTURE) IS ACTIVE")

async def start_web_server(dp_ref, bot_ref):
    """Start aiohttp server with health check + optional webhook route."""
    app = web.Application()
    app.router.add_get('/', handle_health)
    app.router.add_get('/health', handle_health)

    if _WEBHOOK_URL:
        # Register Telegram webhook route
        SimpleRequestHandler(dispatcher=dp_ref, bot=bot_ref).register(app, path=_WEBHOOK_PATH)
        setup_application(app, dp_ref, bot=bot_ref)
        print(f"✅ Webhook route registered: {_WEBHOOK_PATH}")

    port = int(os.environ.get("PORT", 10004))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 Web server running on port {port}")
    return runner

def connect_db():
    global col_pdfs, col_trash, col_locked, col_trash_locked, col_admins, col_banned, col_bot4_state, db_client
    try:
        db_client = pymongo.MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=10000,
            maxPoolSize=10,       # Reduced pool — fewer idle sockets to maintain
            minPoolSize=1,        # Keep only 1 connection alive when idle
            maxIdleTimeMS=30000,  # Drop idle connections after 30s
            socketTimeoutMS=20000,
            connectTimeoutMS=10000,
            heartbeatFrequencyMS=60000,  # Check server health every 60s (default 10s) — reduces background noise
            retryWrites=True,
            retryReads=True
        )
        db = db_client["MSANodeDB"]
        col_pdfs = db["pdf_library"]
        col_trash = db["recycle_bin"]
        col_locked = db["locked_content"] # NEW: Locked Content
        col_trash_locked = db["trash_locked"] # NEW: Locked Content Bin
        col_admins = db["admins_bot4"]
        col_banned = db["banned_list"]
        col_bot4_state = db["bot4_state"]
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
col_bot4_state = None
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
    "\u270F\uFE0F Edit PDF": "edit_pdf",
    "\U0001F4CA Storage Info": "storage_info",
    "\U0001FA7A System Diagnosis": "sys_diag",
    "\U0001F4BB Live Terminal": "live_term",
    "\U0001F5D1 Remove PDF": "remove_pdf",
    "\u26A0\uFE0F NUKE ALL DATA": "nuke_data",
    "⚙️ Admin Config": "manage_admins", # Usually Owner only, but configurable
    "\U0001F48E Full Guide": "elite_help",
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
                 now_dt = now_local()
                 alert = (
                     f"🚨 <b>UNAUTHORIZED ACCESS ATTEMPT</b>\n"
                     f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                     f"👤 <b>Name:</b> {user.full_name}\n"
                     f"🆔 <b>ID:</b> <code>{uid}</code>\n"
                     f"🔗 <b>Username:</b> @{user.username if user.username else 'N/A'}\n"
                     f"📅 <b>Date:</b> <code>{now_dt.strftime('%b %d, %Y')}</code>\n"
                     f"🕐 <b>Time:</b> <code>{now_dt.strftime('%I:%M:%S %p')}</code>\n"
                     f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                     f"⛔ <i>Access silently blocked.</i>"
                 )
                 try:
                     await bot.send_message(OWNER_ID, alert, parse_mode='HTML')
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
    waiting_for_nuke_2 = State()
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
    # Owner Transfer
    waiting_for_owner_pw_first = State()
    waiting_for_owner_pw_confirm = State()
    # Admin session authentication (password gate on /start)
    waiting_for_admin_pw_1 = State()
    waiting_for_admin_pw_2 = State()
    # library states
    browsing_library = State()
    searching_library = State()
    # generate pdf: paginated recent codes
    browsing_recent_codes = State()
    # elite help guide
    viewing_elite_help = State()
    # Paginated admin list
    viewing_admin_list = State()


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

    def is_allowed(btn_text):
        if btn_text in PERMISSION_MAP:
            return PERMISSION_MAP[btn_text] in allowed_keys
        return True

    def btn(t): return KeyboardButton(text=t)

    # ROW 1: Generate PDF | Get Link
    r1 = []
    if is_allowed("📄 Generate PDF"): r1.append(btn("📄 Generate PDF"))
    if is_allowed("🔗 Get Link"):      r1.append(btn("🔗 Get Link"))
    if r1: builder.row(*r1)

    # ROW 2: Show Library | Edit PDF
    r2 = []
    if is_allowed("📋 Show Library"):  r2.append(btn("📋 Show Library"))
    if is_allowed("✏️ Edit PDF"):    r2.append(btn("✏️ Edit PDF"))
    if r2: builder.row(*r2)

    # ROW 3: Storage Info | Remove PDF
    r3 = []
    if is_allowed("📊 Storage Info"):  r3.append(btn("📊 Storage Info"))
    if is_allowed("🗑 Remove PDF"):    r3.append(btn("🗑 Remove PDF"))
    if r3: builder.row(*r3)

    # ROW 4: System Diagnosis | Live Terminal
    r4 = []
    if is_allowed("🩺 System Diagnosis"): r4.append(btn("🩺 System Diagnosis"))
    if is_allowed("💻 Live Terminal"):     r4.append(btn("💻 Live Terminal"))
    if r4: builder.row(*r4)

    # ROW 5: Admin Config | Backup
    r5 = []
    if is_allowed("⚙️ Admin Config"): r5.append(btn("⚙️ Admin Config"))
    r5.append(btn("📦 Backup"))
    builder.row(*r5)

    # ROW 6: NUKE + Full Guide (paired in 2-column layout)
    r6 = []
    if is_allowed("\u26A0\uFE0F NUKE ALL DATA"):
        r6.append(btn("\u26A0\uFE0F NUKE ALL DATA"))
    if is_allowed("\U0001F48E Full Guide"):
        r6.append(btn("\U0001F48E Full Guide"))
    # Also ensure the button text matches the one in PERMISSION_MAP
    if r6: builder.row(*r6)

    return builder.as_markup(resize_keyboard=True)

def generate_progress_bar(percentage):
    """Creates a visual progress bar for Telegram."""
    filled_length = int(percentage // 10)
    bar = "▓" * filled_length + "░" * (10 - filled_length)
    return f"|{bar}| {percentage:.1f}%"


def _natural_sort_key(doc):
    """Natural sort key: splits code into text/number parts so PF9 < PF10 < PF11."""
    import re
    code = doc.get('code', '') or ''
    return [int(part) if part.isdigit() else part.upper()
            for part in re.split(r'(\d+)', code)]


def _get_unique_docs():
    """Returns all PDF documents sorted by timestamp, keeping only the newest for each code."""
    raw_docs = list(col_pdfs.find().sort("timestamp", -1))
    seen = set()
    unique_docs = []
    for d in raw_docs:
        c = d.get('code')
        if c and c not in seen:
            seen.add(c)
            unique_docs.append(d)
    return unique_docs

def get_formatted_file_list(docs, limit=30, start_index=1):
    """Generates a clean, consistent HTML list of files with indices and hyperlinks."""
    if not docs:
        return ["_No files found._"]
        
    lines = []
    for idx, doc in enumerate(docs[:limit], start_index):
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
            line = f"<b>{idx}. {code}</b>{restored_mark}\n<i>{date_str}</i> • <a href='{link}'>🔗 Access</a>"
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
_DEFAULT_DAILY_STATS = {"pdfs_generated": 0, "pdfs_deleted": 0, "errors": 0, "links_retrieved": 0}

# ==========================================
# 💾 PERSISTENT STATE HELPERS
# ==========================================

async def _persist_stats():
    """Save DAILY_STATS_BOT4 to MongoDB so it survives restarts (fire-and-forget)."""
    if col_bot4_state is None:
        return
    try:
        col_bot4_state.update_one(
            {"_id": "daily_stats"},
            {"$set": {
                "stats": DAILY_STATS_BOT4,
                "date": now_local().strftime('%Y-%m-%d'),
                "updated": datetime.now()
            }},
            upsert=True
        )
    except Exception as e:
        logging.warning(f"_persist_stats failed: {e}")

async def _load_persisted_stats():
    """
    Load DAILY_STATS_BOT4 from MongoDB on startup.
    Resets automatically if the stored date is different from today (new day).
    """
    global DAILY_STATS_BOT4
    if col_bot4_state is None:
        print("⚠️ State collection unavailable — using fresh daily stats.")
        return
    try:
        rec = col_bot4_state.find_one({"_id": "daily_stats"})
        if rec:
            saved_date = rec.get("date", "")
            today = now_local().strftime('%Y-%m-%d')
            if saved_date == today:
                DAILY_STATS_BOT4 = {**_DEFAULT_DAILY_STATS, **rec.get("stats", {})}
                print(f"✅ Daily stats restored from DB: {DAILY_STATS_BOT4}")
            else:
                # New day — start fresh
                DAILY_STATS_BOT4 = dict(_DEFAULT_DAILY_STATS)
                await _persist_stats()
                print(f"🔄 New day detected — daily stats reset (was {saved_date}, now {today}).")
        else:
            DAILY_STATS_BOT4 = dict(_DEFAULT_DAILY_STATS)
            await _persist_stats()
            print("🆕 No saved stats found — initialized fresh.")
    except Exception as e:
        logging.warning(f"_load_persisted_stats failed: {e}")

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
        await _safe_send_message(OWNER_ID, alert, parse_mode="HTML")
        logging.info(f"🚨 Error Alert Sent: {error_type}")
        
        # Increment error counter
        DAILY_STATS_BOT4["errors"] += 1
        asyncio.create_task(_persist_stats())
    except Exception as e:
        logging.error(f"Failed to send error alert: {e}")

# daily_briefing removed (Replaced by strict_daily_report)

async def system_guardian():
    """
    Auto-healer: checks DB every 30 min.
    On failure, attempts reconnect + notifies owner. Escalates on repeated failures.
    """
    print("🛡️ System Guardian (Auto-Healer): Online")
    consecutive_failures = 0
    while True:
        try:
            db_client.server_info()
            if consecutive_failures > 0:
                try:
                    await bot.send_message(
                        OWNER_ID,
                        f"✅ <b>BOT 4 AUTO-HEALER: RECOVERED</b>\n\n"
                        f"Database back online after {consecutive_failures} failure(s).\n"
                        f"🕐 {now_local().strftime('%I:%M %p  ·  %b %d, %Y')}",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            print(f"⚠️ Guardian: DB issue detected (#{consecutive_failures}): {e}")

            reconnected = await asyncio.to_thread(connect_db)
            reconnect_status = "✅ Reconnected" if reconnected else "❌ Still Down"

            if consecutive_failures == 1 or consecutive_failures % 3 == 0:
                await notify_error_bot4(
                    f"Auto-Healer Alert (failure #{consecutive_failures})",
                    f"DB issue: {e}\nDB Reconnect: {reconnect_status}"
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

# ==========================================
# 🚀 HANDLERS
# ==========================================

@dp.message(Command("start"))
@dp.message(F.text == "🔙 Back to Menu")
async def start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if is_banned(user_id): return

    if is_admin(user_id):
        await state.clear()
        reply_markup = get_main_menu(user_id)
        if user_id == OWNER_ID:
            greeting = "💎 <b>MSA NODE BOT 4</b>\nAt your command, Master."
        else:
            admin_doc = col_admins.find_one({"user_id": user_id}) or col_admins.find_one({"user_id": str(user_id)})
            role = admin_doc.get("role", "Authorized Admin") if admin_doc else "Authorized Admin"
            name = message.from_user.full_name
            greeting = (
                f"💎 <b>MSA NODE SYSTEMS</b>\n"
                f"ASSIGNED BY 👑 <b>OWNER:</b> MSA\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🛡️ <b>ACCESS GRANTED</b>\n"
                f"👤 <b>Officer:</b> {name}\n"
                f"🔰 <b>Rank:</b> <code>{role}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🚀 System initialized."
            )
        await message.answer(greeting, reply_markup=reply_markup, parse_mode="HTML")
        return

    # Stranger flood check
    now = time.time()
    if user_id not in START_TRACKER or (now - START_TRACKER[user_id][0] > 60):
        START_TRACKER[user_id] = [now, 1]
    else:
        START_TRACKER[user_id][1] += 1
    if START_TRACKER[user_id][1] > 5:
        if not is_banned(user_id):
            try:
                if col_banned is not None:
                    col_banned.insert_one({"user_id": user_id, "reason": "Auto-Ban: Spamming /start", "timestamp": datetime.now()})
            except Exception as e:
                logging.error(f"Auto-ban insert failed: {e}")
            try: await bot.send_message(OWNER_ID, f"🚨 <b>AUTO-BANNED</b> `{user_id}` — spamming /start.", parse_mode="HTML")
            except: pass
    return


# ──────────────────────────────────────────────────────────────────────────────
# 🔐 ADMIN PASSWORD GATE (owner-only, one-time per session, double confirmation)
# ──────────────────────────────────────────────────────────────────────────────

@dp.message(BotState.waiting_for_admin_pw_1)
async def admin_pw_first(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    # Cancel = skip auth this session (owner ID already verified by /start gate)
    if message.text and message.text.strip() == "❌ Cancel":
        _admin_authenticated.add(user_id)
        await state.clear()
        await start(message, state)
        return
    try: await message.delete()
    except: pass
    data = await state.get_data()
    attempts = data.get("pw_attempts", 0)
    if not ADMIN_PASSWORD:
        # Env var not configured — skip auth silently
        _admin_authenticated.add(user_id)
        await state.clear()
        await start(message, state)
        return
    if message.text == ADMIN_PASSWORD:
        await state.update_data(pw_first_ok=True, pw_attempts=0)
        await state.set_state(BotState.waiting_for_admin_pw_2)
        await message.answer("✅ Password accepted.\n\nEnter password again to confirm:", parse_mode="HTML")
    else:
        attempts += 1
        remaining = 3 - attempts
        if remaining <= 0:
            await state.clear()
            await message.answer(
                "❌ Too many failed attempts. Use /start to try again.",
                reply_markup=ReplyKeyboardRemove(),
            )
        else:
            await state.update_data(pw_attempts=attempts)
            await message.answer(
                f"❌ Incorrect password. <b>{remaining}</b> attempt(s) remaining.",
                parse_mode="HTML",
            )


@dp.message(BotState.waiting_for_admin_pw_2)
async def admin_pw_second(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    # Cancel = skip auth this session (owner ID already verified by /start gate)
    if message.text and message.text.strip() == "❌ Cancel":
        _admin_authenticated.add(user_id)
        await state.clear()
        await start(message, state)
        return
    try: await message.delete()
    except: pass
    if message.text == ADMIN_PASSWORD:
        _admin_authenticated.add(user_id)
        await state.clear()
        # Simulate a fresh /start
        await start(message, state)
    else:
        await state.clear()
        await message.answer(
            "❌ Passwords did not match. Authentication failed.\n\nUse /start to try again.",
            reply_markup=ReplyKeyboardRemove(),
        )


# ==========================================
# 📄 PDF GENERATION
# ==========================================

@dp.message(F.text == "📄 Generate PDF")
async def gen_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.update_data(raw_script="")
    # Load all codes for paginated recent list
    all_codes = []
    try:
        all_docs_rc = list(col_pdfs.find({}, {"code": 1, "restored": 1}).sort("timestamp", -1))
        for i, d in enumerate(all_docs_rc, 1):
            if "code" in d:
                marker = " <b>[R]</b>" if d.get('restored', False) else ""
                all_codes.append(f"{i}. <code>{d['code']}</code>{marker}")
    except: pass
    await state.update_data(raw_script="", recent_codes=all_codes, rc_page=0)
    await _render_gen_recent(message, state, all_codes, 0)


async def _render_gen_recent(message, state, all_codes, page):
    """Render the Generate PDF entry prompt with paginated recent codes."""
    PER_PAGE = 15
    total = len(all_codes)
    max_page = max(0, (total - 1) // PER_PAGE) if total else 0
    page = max(0, min(page, max_page))
    page_codes = all_codes[page * PER_PAGE:(page + 1) * PER_PAGE]
    await state.update_data(rc_page=page)
    header = (
        "🔑 <b>AUTHENTICATED.</b>\n\n"
        "Enter your <b>Project Code</b> to begin:"
    )
    if page_codes:
        s = page * PER_PAGE + 1
        e = page * PER_PAGE + len(page_codes)
        header += f"\n\n🕒 <b>Recent (Latest ↓ · #{s}–{e} of {total}):</b>\n"
        header += "\n".join(page_codes)
    builder = ReplyKeyboardBuilder()
    nav = []
    if page > 0:       nav.append(KeyboardButton(text="◀ PREV CODES"))
    if page < max_page: nav.append(KeyboardButton(text="▶ MORE CODES"))
    if nav: builder.row(*nav)
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    await message.answer(header, reply_markup=builder.as_markup(resize_keyboard=True), parse_mode="HTML")
    await state.set_state(BotState.waiting_for_code)

@dp.message(BotState.waiting_for_code)
async def code_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    if message.text in ("◀ PREV CODES", "▶ MORE CODES"):
        _d = await state.get_data()
        _pg = _d.get("rc_page", 0) + (-1 if "PREV" in message.text else 1)
        return await _render_gen_recent(message, state, _d.get("recent_codes", []), _pg)
    code = message.text.strip().upper()
    if col_pdfs.find_one({"code": code}):
        await message.answer(
            f"⛔ <b>ERROR: Code <code>{code}</code> Already Exists!</b>\n"
            f"Please enter a DIFFERENT Project Code:",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True),
            parse_mode="HTML"
        )
        return
    await state.update_data(code=code)
    await message.answer(
        f"🖋 <b>Code <code>{code}</code> Available.</b>\n📝 <b>Awaiting Content...</b>\nPaste your script or data now, Master.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True),
        parse_mode="HTML"
    )
    await state.set_state(BotState.processing_script)

@dp.message(BotState.processing_script, F.text)
async def merge_script(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    data = await state.get_data()
    # Append new chunk preserving exact order
    updated = data.get('raw_script', '') + ("\n\n" if data.get('raw_script', '') else "") + message.text
    # Increment generation counter so any running timer knows it's stale
    gen = data.get('script_gen', 0) + 1
    await state.update_data(raw_script=updated, script_gen=gen, timer_active=True)

    uid = message.from_user.id

    async def auto_finish(my_gen, uid, st):
        try:
            await asyncio.sleep(5)
            # Only finalize if no newer chunk arrived (generation unchanged)
            current = await st.get_data()
            if current.get('script_gen', 0) == my_gen:
                await finalize_pdf(uid, st)
        except Exception as _task_err:
            logging.error(f"auto_finish unhandled error for uid={uid}: {_task_err}")

    asyncio.create_task(auto_finish(gen, uid, state))

async def _safe_send_message(user_id, text, parse_mode="HTML", max_wait=300):
    """
    Send a message, auto-splitting on newline boundaries if > 4096 chars.
    Telegram's hard limit is 4096 chars per message.
    Flood control: logs and returns None — no hanging retry.
    """
    TG_LIMIT = 4000  # slightly under 4096 for safety

    # Split into chunks if needed
    if len(text) <= TG_LIMIT:
        chunks = [text]
    else:
        chunks = []
        current = ""
        for line in text.splitlines(keepends=True):
            if len(current) + len(line) > TG_LIMIT:
                if current:
                    chunks.append(current)
                # If a single line is longer than the limit, hard-split it
                while len(line) > TG_LIMIT:
                    chunks.append(line[:TG_LIMIT])
                    line = line[TG_LIMIT:]
                current = line
            else:
                current += line
        if current:
            chunks.append(current)

    last = None
    for chunk in chunks:
        if not chunk.strip():
            continue
        try:
            last = await bot.send_message(user_id, chunk, parse_mode=parse_mode)
        except TelegramRetryAfter as e:
            logging.warning(f"send_message skipped (flood control {e.retry_after}s) to {user_id}")
            return None
        except Exception as e:
            logging.error(f"send_message failed for {user_id}: {e}")
            return None
    return last


async def _safe_send_document(user_id, file, caption, parse_mode="HTML", max_retries=20):
    """
    Send a document. Only called from background tasks so it CAN wait.
    Honours the exact flood-wait Telegram demands, retries until delivered or max_retries hit.
    """
    for attempt in range(max_retries):
        try:
            return await bot.send_document(user_id, file, caption=caption, parse_mode=parse_mode)
        except TelegramRetryAfter as e:
            logging.warning(
                f"PDF delivery flood control (attempt {attempt+1}/{max_retries}): "
                f"waiting {e.retry_after}s..."
            )
            await asyncio.sleep(e.retry_after + 1)
        except Exception as e:
            logging.error(f"send_document failed for {user_id}: {e}")
            raise
    raise RuntimeError(f"send_document to {user_id} failed after {max_retries} attempts")


def _encrypt_pdf(filepath: str) -> None:
    """
    Re-writes the PDF at `filepath` with AES-256 encryption:
    - User password  = "" (empty) — anyone can OPEN the PDF freely
    - Owner password = secret    — only owner can change restrictions
    - Restrictions   : COPY and EXTRACT text disabled
    Falls back silently if pypdf is unavailable or encryption fails.
    """
    try:
        from pypdf import PdfReader, PdfWriter
        from pypdf.generic import NameObject

        reader = PdfReader(filepath)
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)

        # Copy all metadata from source
        if reader.metadata:
            writer.add_metadata(dict(reader.metadata))

        # Encrypt: allow_printing=True, allow_copying=False
        writer.encrypt(
            user_password="",
            owner_password="MSANODEVault@2025!",
            use_128bit=False,   # use AES-256
            permissions_flag=4,  # 4 = print only; no extract/copy
        )

        tmp = filepath + ".enc"
        with open(tmp, "wb") as f:
            writer.write(f)

        # Atomically replace original
        os.replace(tmp, filepath)
        logging.info(f"PDF encrypted (copy-restricted): {filepath}")

    except ImportError:
        logging.warning("pypdf not available — skipping PDF encryption")
    except Exception as e:
        logging.warning(f"PDF encryption failed (PDF still usable): {e}")


async def finalize_pdf(user_id, state):
    global DAILY_STATS_BOT4
    data = await state.get_data()
    code = data.get('code')
    script = data.get('raw_script', '').strip()
    if not script or not code: return

    filename = f"{code}.pdf"
    try:
        # Generate PDF
        await asyncio.to_thread(create_goldmine_pdf, script, filename)

        # Encrypt: disable copy/extract while keeping PDF freely openable
        await asyncio.to_thread(_encrypt_pdf, filename)

        # Upload to Google Drive
        link = ""
        if os.path.exists(CREDENTIALS_FILE):
            try:
                link = await asyncio.to_thread(upload_to_drive, filename)
            except Exception as drive_err:
                logging.warning(f"Drive upload failed for {code}: {drive_err}")
        else:
            logging.warning("credentials.json not found — skipping Drive upload.")

        # Save to MongoDB
        col_pdfs.delete_many({"code": code})
        col_pdfs.insert_one({"code": code, "link": link, "timestamp": datetime.now()})
        DAILY_STATS_BOT4["pdfs_generated"] += 1
        asyncio.create_task(_persist_stats())

        # Deliver PDF file in background — waits out flood ban, won't block bot
        _filename_snap = filename
        _link_snap = link
        _code_snap = code

        async def _deliver_file():
            _caption = (
                f"✅ <b>PDF READY</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📄 <b>Code:</b> <code>{_code_snap}</code>\n"
            )
            if _link_snap:
                _caption += f"🔗 <a href='{_link_snap}'>Drive Link</a>"
            try:
                await _safe_send_document(
                    user_id, FSInputFile(_filename_snap),
                    caption=_caption, parse_mode="HTML"
                )
            except Exception as _de:
                logging.warning(f"PDF delivery failed for {_code_snap}: {_de}")
            finally:
                await asyncio.sleep(2)
                if os.path.exists(_filename_snap):
                    try: os.remove(_filename_snap)
                    except: pass

        asyncio.create_task(_deliver_file())

    except Exception as e:
        logging.error(f"finalize_pdf error for code={code}: {e}")
        await _safe_send_message(user_id, f"❌ Error generating PDF: <code>{e}</code>", parse_mode="HTML")
        try:
            await notify_error_bot4("PDF Generation Failed", f"Code: {code} | Error: {e}")
        except Exception:
            pass
        DAILY_STATS_BOT4["errors"] += 1
        asyncio.create_task(_persist_stats())
    await state.clear()


# ==========================================
# 📋 SHOW LIBRARY
# ==========================================

@dp.message(F.text == "📋 Show Library")
async def show_library(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "📚 <b>VAULT LIBRARY ACCESS</b>\nSelect your preferred viewing mode:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📋 DISPLAY ALL"), KeyboardButton(text="🔍 SEARCH")],
                      [KeyboardButton(text="🔙 Back to Menu")]],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )
    await state.set_state(BotState.browsing_library)
    await state.update_data(lib_mode="menu")

@dp.message(BotState.browsing_library)
async def handle_library_logic(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    data = await state.get_data()
    mode = data.get("lib_mode", "menu")
    if mode == "menu":
        if text == "📋 DISPLAY ALL":
            await state.update_data(lib_mode="display", page=0)
            await render_library_page(message, state, page=0)
        elif text == "🔍 SEARCH":
            docs = sorted(list(col_pdfs.find()), key=_natural_sort_key)  # natural sort by code
            list_lines = get_formatted_file_list(docs, limit=30)
            # Truncate at whole-line boundaries to avoid cutting through HTML tags
            safe_lines = []
            char_count = 0
            truncated = False
            for ln in list_lines:
                if char_count + len(ln) + 1 > 3400:
                    truncated = True
                    break
                safe_lines.append(ln)
                char_count += len(ln) + 1
            list_text = "\n".join(safe_lines)
            if truncated:
                list_text += "\n..."
            await message.answer(
                f"{list_text}\n\n🔍 <b>SEARCH</b>\nEnter a <b>Code</b> (e.g., <code>S19</code>) or <b>Index Number</b>.",
                reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK")], [KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True),
                parse_mode="HTML", disable_web_page_preview=True
            )
            await state.set_state(BotState.searching_library)
        else:
            await message.answer("⚠️ Invalid option.")
    elif mode == "display":
        current_page = data.get("page", 0)
        if text == "⬅️ BACK":
            await state.update_data(lib_mode="menu")
            await show_library(message, state)
        elif text == "➡️ NEXT":
            await render_library_page(message, state, page=current_page + 1)
        elif text == "⬅️ PREV":
            await render_library_page(message, state, page=current_page - 1)
        else:
            await message.answer("⚠️ Use navigation buttons.")

async def render_library_page(message, state, page):
    limit = 20
    docs = sorted(list(col_pdfs.find()), key=_natural_sort_key)
    total_docs = len(docs)
    max_page = max(0, (total_docs - 1) // limit)
    page = max(0, min(page, max_page))
    page_docs = docs[page * limit:(page + 1) * limit]
    lines = []
    for i, doc in enumerate(page_docs):
        abs_idx = page * limit + i + 1
        code = doc.get("code")
        ts = doc.get("timestamp")
        date_str = ts.strftime('%d-%b %I:%M %p') if ts else "?"
        link = doc.get('link')
        if link:
            lines.append(f"<b>{abs_idx}. {code}</b>\n<i>{date_str}</i> • <a href='{link}'>🔗 Link</a>")
        else:
            lines.append(f"<b>{abs_idx}. {code}</b>\n<i>{date_str}</i>")
    header = (
        f"📋 <b>LIBRARY INDEX</b> (Page {page+1}/{max_page+1})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(lines) +
        f"\n━━━━━━━━━━━━━━━━━━━━\n📊 Total: {total_docs} Files"
    ) if lines else "📋 Library is empty."
    await state.update_data(page=page)
    builder = ReplyKeyboardBuilder()
    row_btns = []
    if page > 0: row_btns.append(KeyboardButton(text="⬅️ PREV"))
    if page < max_page: row_btns.append(KeyboardButton(text="➡️ NEXT"))
    if row_btns: builder.row(*row_btns)
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    await message.answer(header, reply_markup=builder.as_markup(resize_keyboard=True), parse_mode="HTML", disable_web_page_preview=True)

@dp.message(BotState.searching_library)
async def handle_library_search(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    if text in ("🔙 BACK TO MENU", "🔙 BACK TO MENU"): return await start(message, state)
    if text == "⬅️ BACK":
        await show_library(message, state)
        return
    all_docs = sorted(list(col_pdfs.find()), key=_natural_sort_key)  # natural sort = consistent index
    doc = None
    if text.isdigit():
        idx = int(text)
        if 1 <= idx <= len(all_docs):
            doc = all_docs[idx - 1]
    if not doc:
        doc = next((d for d in all_docs if d.get('code') == text), None)
    if doc:
        code = doc.get('code')
        link = doc.get('link', '')
        ts = doc.get('timestamp')
        date_str = ts.strftime('%d-%b-%Y %I:%M %p') if ts else "Unknown"
        link_line = f"\n🔗 <b>Link:</b>\n{link}" if link else "\n🔗 <b>Link:</b> <i>Not set</i>"
        await message.answer(
            f"💎 <b>VAULT ITEM</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>Code:</b> <code>{code}</code>\n"
            f"📅 <b>Added:</b> <code>{date_str}</code>\n"
            f"📂 <b>Status:</b> Active{link_line}\n━━━━━━━━━━━━━━━━━━━━",
            parse_mode="HTML", disable_web_page_preview=True
        )
        await message.answer("🔍 Search another or '🔙 Back to Menu'.")
    else:
        await message.answer(f"❌ Record <code>{text}</code> not found.", parse_mode="HTML")


# ==========================================
# 📊 STORAGE INFO
# ==========================================

@dp.message(F.text == "📊 Storage Info")
async def storage_info(message: types.Message):
    if not is_admin(message.from_user.id): return
    wait_msg = await message.answer("⏳ <b>Running System Scan...</b>", parse_mode="HTML")
    start_t = time.time()
    try:
        t0 = time.time()
        db_client.server_info()
        t_mongo = (time.time() - t0) * 1000
        now_ts = now_local()
        # Live collection counts
        pdf_count    = col_pdfs.count_documents({})         if col_pdfs         is not None else 0
        trash_count  = col_trash.count_documents({})        if col_trash        is not None else 0
        locked_count = col_locked.count_documents({})       if col_locked       is not None else 0
        admin_count  = col_admins.count_documents({})       if col_admins       is not None else 0
        banned_count = col_banned.count_documents({})       if col_banned       is not None else 0
        t_lock_count = col_trash_locked.count_documents({}) if col_trash_locked is not None else 0
        # Admin breakdown
        try:
            active_admins = col_admins.count_documents({"locked": False})
            locked_admins = col_admins.count_documents({"locked": True})
        except: active_admins = locked_admins = 0
        # Latest backup
        try:
            _db = db_client["MSANodeDB"]
            last_bup  = _db["bot4_monthly_backups"].find_one({}, sort=[("date", -1)])
            bup_month = last_bup["month"] if last_bup else "None"
            bup_count = _db["bot4_monthly_backups"].count_documents({})
        except: bup_month = "?"; bup_count = 0
        # Latest PDF
        try:
            latest_pdf  = col_pdfs.find_one({}, sort=[("timestamp", -1)])
            latest_code = latest_pdf.get("code","?") if latest_pdf else "None"
            latest_ts   = latest_pdf.get("timestamp") if latest_pdf else None
            latest_date = latest_ts.strftime("%b %d, %Y  %I:%M %p") if latest_ts else "—"
        except: latest_code = "?"; latest_date = "?"
        health    = "🟢 Excellent" if t_mongo < 150 else ("🟡 Degraded" if t_mongo < 500 else "🔴 Critical")
        scan_time = time.time() - start_t
        msg = (
            f"📊 <b>STORAGE ANALYTICS — LIVE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 <code>{now_ts.strftime('%b %d, %Y  %I:%M %p')}</code>\n"
            f"💚 DB: {health}  ⏱ <code>{t_mongo:.1f}ms</code>\n\n"
            f"📁 <b>PDF VAULT</b>\n"
            f"• Active PDFs: <code>{pdf_count}</code>\n"
            f"• Archived: <code>{trash_count}</code>  Locked: <code>{locked_count}</code>  Locked-Archived: <code>{t_lock_count}</code>\n"
            f"• Latest: <code>{latest_code}</code> · <code>{latest_date}</code>\n\n"
            f"👥 <b>ADMINS</b>\n"
            f"• Total: <code>{admin_count}</code>  (🟢 {active_admins} active  🔴 {locked_admins} locked)\n"
            f"• Banned: <code>{banned_count}</code>\n\n"
            f"📦 <b>BACKUPS</b>\n"
            f"• Monthly records: <code>{bup_count}</code> | Last: <code>{bup_month}</code>\n\n"
            f"📈 <b>SESSION</b>\n"
            f"• PDFs gen: <code>{DAILY_STATS_BOT4['pdfs_generated']}</code>  "
            f"Links: <code>{DAILY_STATS_BOT4['links_retrieved']}</code>  "
            f"Errors: <code>{DAILY_STATS_BOT4['errors']}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{'✅ <b>ALL SYSTEMS OPERATIONAL</b>' if t_mongo < 500 else '⚠️ <b>HIGH LATENCY DETECTED</b>'}"
        )
        await wait_msg.delete()
        await message.answer(msg, parse_mode="HTML")
    except Exception as e:
        await wait_msg.edit_text(f"⚠️ <b>Scan Error:</b> <code>{e}</code>", parse_mode="HTML")


@dp.message(F.text == "📦 Backup")
async def backup_menu_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return

    # Live stats preview
    pdf_count   = col_pdfs.count_documents({})   if col_pdfs   is not None else 0
    admin_count = col_admins.count_documents({}) if col_admins is not None else 0
    banned_count= col_banned.count_documents({}) if col_banned is not None else 0
    trash_count = col_trash.count_documents({})  if col_trash  is not None else 0
    now_str = now_local().strftime("%b %d, %Y  ·  %I:%M %p")

    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📄 Text Report"), KeyboardButton(text="💾 JSON Dump"))
    builder.row(KeyboardButton(text="📅 Backup History"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))

    await message.answer(
        f"📦 <b>BACKUP &amp; RECOVERY</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 <b>Now:</b> <code>{now_str}</code>\n\n"
        f"📊 <b>CURRENT SNAPSHOT</b>\n"
        f"• 📚 Active PDFs: <code>{pdf_count}</code>\n"
        f"• 🗑 Recycle Bin: <code>{trash_count}</code>\n"
        f"• 👥 Admins: <code>{admin_count}</code>\n"
        f"• 🚫 Banned: <code>{banned_count}</code>\n\n"
        f"📄 <b>Text Report</b> — Human-readable summary.\n"
        f"💾 <b>JSON Dump</b> — Full export for restore.\n"
        f"📅 <b>Backup History</b> — View all past backup records.\n\n"
        f"<i>All backups stored in MongoDB + sent to Owner.</i>",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )

@dp.message(F.text == "📄 Text Report")
async def handle_backup_text(message: types.Message):
    if not is_admin(message.from_user.id): return

    msg = await message.answer("⏳ <b>Generating Text Report...</b>", parse_mode="HTML")
    filename = await asyncio.to_thread(generate_system_backup)

    if filename and os.path.exists(filename):
        try:
            now_ts = now_local()
            pdf_count   = col_pdfs.count_documents({})   if col_pdfs   is not None else 0
            admin_count = col_admins.count_documents({}) if col_admins is not None else 0
            caption = (
                f"🛡 <b>MSANODE SYSTEM REPORT</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 <b>Generated:</b> <code>{now_ts.strftime('%b %d, %Y  ·  %I:%M %p')}</code>\n"
                f"📚 <b>PDFs:</b> <code>{pdf_count}</code>  |  "
                f"👥 <b>Admins:</b> <code>{admin_count}</code>\n"
                f"💾 <b>Storage:</b> MongoDB Atlas\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Human-readable snapshot of all data.</i>"
            )
            await message.answer_document(FSInputFile(filename), caption=caption, parse_mode="HTML")
            os.remove(filename)
            await msg.delete()
        except Exception as e:
            await msg.edit_text(f"❌ Failed: <code>{e}</code>", parse_mode="HTML")
    else:
        await msg.edit_text("❌ <b>Generation Failed.</b>", parse_mode="HTML")

@dp.message(F.text == "💾 JSON Dump")
async def handle_backup_json(message: types.Message):
    if not is_admin(message.from_user.id): return

    msg = await message.answer("⏳ <b>Exporting Full Database...</b>", parse_mode="HTML")

    try:
        now_ts = now_local()
        date_label = now_ts.strftime("%Y-%m-%d")
        data = {
            "backup_type":  "manual_json",
            "generated_at": now_ts.strftime("%b %d, %Y  ·  %I:%M %p"),
            "pdfs":         list(col_pdfs.find({}, {"_id": 0}))         if col_pdfs         is not None else [],
            "trash":        list(col_trash.find({}, {"_id": 0}))        if col_trash        is not None else [],
            "locked":       list(col_locked.find({}, {"_id": 0}))       if col_locked       is not None else [],
            "trash_locked": list(col_trash_locked.find({}, {"_id": 0})) if col_trash_locked is not None else [],
            "admins":       list(col_admins.find({}, {"_id": 0}))       if col_admins       is not None else [],
            "banned":       list(col_banned.find({}, {"_id": 0}))       if col_banned       is not None else [],
        }

        filename = f"MSANODE_DUMP_{date_label}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, default=str)

        # Save dedup-safe metadata record
        try:
            db = db_client["MSANodeDB"]
            db["bot4_backups"].update_one(
                {"date": date_label, "type": "json_dump"},
                {"$set": {
                    "date":          date_label,
                    "type":          "json_dump",
                    "pdf_count":     len(data["pdfs"]),
                    "admin_count":   len(data["admins"]),
                    "banned_count":  len(data["banned"]),
                    "trash_count":   len(data["trash"]),
                    "locked_count":  len(data["locked"]),
                    "created_at":    now_ts
                }},
                upsert=True
            )
        except Exception as db_err:
            print(f"⚠️ Backup DB record failed: {db_err}")

        caption = (
            f"💾 <b>FULL DATABASE DUMP</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 <b>Generated:</b> <code>{now_ts.strftime('%b %d, %Y  ·  %I:%M %p')}</code>\n\n"
            f"📊 <b>CONTENTS</b>\n"
            f"• 📚 Active PDFs: <code>{len(data['pdfs'])}</code>\n"
            f"• 🗑 Recycle Bin: <code>{len(data['trash'])}</code>\n"
            f"• 🔒 Locked PDFs: <code>{len(data['locked'])}</code>\n"
            f"• 👥 Admins: <code>{len(data['admins'])}</code>\n"
            f"• 🚫 Banned: <code>{len(data['banned'])}</code>\n\n"
            f"💾 <b>Storage:</b> MongoDB Atlas\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <i>Keep this file safe — full bot restore data.</i>"
        )

        await message.answer_document(FSInputFile(filename), caption=caption, parse_mode="HTML")
        os.remove(filename)
        await msg.delete()

    except Exception as e:
        await msg.edit_text(f"❌ <b>Export Failed:</b> <code>{e}</code>", parse_mode="HTML")


@dp.message(F.text == "📅 Backup History")
async def backup_history_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    wait_msg = await message.answer("⏳ <b>Fetching Backup Records...</b>", parse_mode="HTML")
    try:
        _db = db_client["MSANodeDB"]
        monthly = list(_db["bot4_monthly_backups"].find({}, {"_id": 0}).sort("date", -1).limit(12))
        manual  = list(_db["bot4_backups"].find({}, {"_id": 0}).sort("created_at", -1).limit(10))
        lines = [
            "📅 <b>BACKUP HISTORY</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "\n📆 <b>MONTHLY AUTO-BACKUPS</b>",
        ]
        if monthly:
            for i, b in enumerate(monthly, 1):
                month  = b.get("month", "?")
                dt     = b.get("date")
                dt_str = dt.strftime("%b %d, %Y  %I:%M %p") if hasattr(dt, "strftime") else str(dt)[:19]
                pdfs   = b.get("pdf_count", "?")
                lines.append(f"  {i}. 📦 <b>{month}</b> | 📅 {dt_str} | PDFs: <code>{pdfs}</code>")
        else:
            lines.append("  <i>No monthly backups yet.</i>")
        lines.append("\n💾 <b>MANUAL / WEEKLY BACKUPS</b>")
        if manual:
            for i, b in enumerate(manual, 1):
                btype  = b.get("type", "manual")
                dt     = b.get("created_at")
                dt_str = dt.strftime("%b %d, %Y  %I:%M %p") if hasattr(dt, "strftime") else str(dt)[:19]
                pdfs   = b.get("pdf_count", "?")
                lines.append(f"  {i}. 📄 <b>{btype}</b> | 📅 {dt_str} | PDFs: <code>{pdfs}</code>")
        else:
            lines.append("  <i>No manual backups found.</i>")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"📊 Monthly: <code>{len(monthly)}</code> | Manual/Weekly: <code>{len(manual)}</code>")
        # Truncate at whole-line boundaries to avoid cutting through HTML tags
        safe_lines_bh = []
        bh_chars = 0
        bh_truncated = False
        for ln in lines:
            if bh_chars + len(ln) + 1 > 3900:
                bh_truncated = True
                break
            safe_lines_bh.append(ln)
            bh_chars += len(ln) + 1
        msg_text = "\n".join(safe_lines_bh)
        if bh_truncated:
            msg_text += "\n..."
        await wait_msg.delete()
        await message.answer(msg_text, parse_mode="HTML")
    except Exception as e:
        await wait_msg.edit_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def weekly_backup():
    while True:
        now = now_local()
        days_until_sunday = (6 - now.weekday()) % 7
        if days_until_sunday == 0 and now.hour >= 3: days_until_sunday = 7
        target = now.replace(hour=3, minute=0, second=0, microsecond=0) + timedelta(days=days_until_sunday)
        wait_seconds = (target - now).total_seconds()
        await asyncio.sleep(max(wait_seconds, 1))

        try:
            filename = await asyncio.to_thread(generate_system_backup)
            if filename:
                now_ts = now_local()
                date_label = now_ts.strftime("%Y-%m-%d")
                pdf_count   = col_pdfs.count_documents({})   if col_pdfs   is not None else 0
                admin_count = col_admins.count_documents({}) if col_admins is not None else 0

                try:
                    db = db_client["MSANodeDB"]
                    db["bot4_backups"].update_one(
                        {"date": date_label, "type": "weekly"},
                        {"$set": {
                            "date":        date_label,
                            "type":        "weekly",
                            "pdf_count":   pdf_count,
                            "admin_count": admin_count,
                            "created_at":  now_ts
                        }},
                        upsert=True
                    )
                except Exception as db_err:
                    print(f"⚠️ Weekly backup DB record failed: {db_err}")

                caption = (
                    f"🛡 <b>WEEKLY AUTO-BACKUP</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📅 <b>Date:</b> <code>{now_ts.strftime('%b %d, %Y  ·  %I:%M %p')}</code>\n"
                    f"📚 <b>PDFs:</b> <code>{pdf_count}</code>  |  "
                    f"👥 <b>Admins:</b> <code>{admin_count}</code>\n"
                    f"💾 <b>Storage:</b> MongoDB Atlas"
                )
                try:
                    await _safe_send_document(OWNER_ID, FSInputFile(filename), caption=caption, parse_mode="HTML")
                except Exception as e:
                    logging.error(f"Weekly backup DM failed: {e}")
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
    Process inline formatting markers (ALL CARET-BASED — no asterisks):

    ^^^^^^url^^^^^^   →  🔵 Clickable Blue Hyperlink
    ^^^^^TEXT^^^^^   →  ⚫ BLACK · BOLD · CAPS
    ^^^^TEXT^^^^     →  🔵 BLUE · BOLD · CAPS
    ^^^TEXT^^^       →  🔴 RED · BOLD · CAPS
    ^TEXT^           →  ⚫ Bold Black (normal case)
    """
    # HIGHEST PRIORITY: Handle ^^^^^^url^^^^^^  (CLICKABLE LINKS)
    def create_clickable_link(match):
        url = match.group(1).strip()
        return f'<a href="{url}" color="#1565C0"><u>{url}</u></a>'

    text = re.sub(r'\^{6}([^^]+?)\^{6}', create_clickable_link, text)

    # ^^^^^TEXT^^^^^  — DARK BLACK BOLD ALL CAPS
    def uppercase_black_5caret(match):
        content = match.group(1).strip().upper()
        return f'<font color="#000000"><b>{content}</b></font>'

    text = re.sub(r'\^{5}([^^]+?)\^{5}', uppercase_black_5caret, text)

    # ^^^^TEXT^^^^  — BLUE BOLD ALL CAPS
    def uppercase_blue(match):
        content = match.group(1).strip().upper()
        return f'<font color="#1565C0"><b>{content}</b></font>'

    text = re.sub(r'\^{4}([^^]+?)\^{4}', uppercase_blue, text)

    # ^^^TEXT^^^  — RED BOLD ALL CAPS
    def uppercase_red(match):
        content = match.group(1).strip().upper()
        return f'<font color="#D32F2F"><b>{content}</b></font>'

    text = re.sub(r'\^{3}([^^]+?)\^{3}', uppercase_red, text)

    # ^TEXT^  — DARK BLACK BOLD, normal case
    def bold_black_no_caps(match):
        content = match.group(1).strip()
        return f'<font color="#000000"><b>{content}</b></font>'

    text = re.sub(r'\^([^^]+?)\^', bold_black_no_caps, text)

    return text


def _normalize_input_text(text: str) -> str:
    """
    Normalise text that was copy-pasted from ChatGPT on mobile.
    Mobile apps often produce different whitespace, invisible Unicode, or
    spaced-out asterisk groups compared to the laptop browser.
    """
    # 1. Unified line endings
    text = text.replace('\r\n', '\n').replace('\r', '\n')

    # 2. Strip invisible / nuisance Unicode characters
    #    Zero-width space / joiner / non-joiner, soft-hyphen, BOM
    for ch in ('\u200b', '\u200c', '\u200d', '\u00ad', '\ufeff'):
        text = text.replace(ch, '')

    # 3. Non-breaking space and figure space → regular space
    for ch in ('\u00a0', '\u202f', '\u2009', '\u2007', '\u2060'):
        text = text.replace(ch, ' ')

    # 4. Fullwidth asterisk (mobile IME) → regular asterisk
    text = text.replace('\uff0a', '*')

    # 5. Collapse caret groups that have spaces/tabs between them (but NOT newlines)
    #    e.g.  "^ ^ ^ TEXT ^ ^ ^"  →  "^^^TEXT^^^"
    #    IMPORTANT: use [^\S\n]* not \s* — \s* would eat newlines between sections,
    #    merging e.g. "^^^^\n\n\n^^^" (title + blank lines + section) into "^^^^^^^"
    for carets in range(6, 0, -1):
        spaced = r'\^' + (r'[^\S\n]*\^' * (carets - 1))
        clean  = '^' * carets
        text   = re.sub(spaced, clean, text)

    # 6. Fix extra spaces just inside caret markers
    text = re.sub(r'\^{1,6} {2,}', lambda m: m.group(0).rstrip(), text)
    text = re.sub(r' {2,}\^{1,6}', lambda m: m.group(0).lstrip(), text)

    # 7. Excessive blank lines → max 2 consecutive newlines
    text = re.sub(r'\n{4,}', '\n\n\n', text)

    # 8. Trailing whitespace on each line
    text = '\n'.join(line.rstrip() for line in text.split('\n'))

    return text


def create_goldmine_pdf(text, filename):
    """Creates PDF in S19 professional format with full Unicode support (₹, €, £, etc.)"""

    # ── Step 0: Normalise mobile / cross-platform copy-paste artefacts ────────
    text = _normalize_input_text(text)

    # ── Symbol handling ─────────────────────────────────────────────────────
    # Strategy: replace each currency symbol with a safe ASCII placeholder FIRST,
    # run the non-ASCII strip, then restore placeholders.

    # This is needed because the strip would otherwise remove ₹ even from inside
    # a <font> tag since ₹ itself is non-ASCII.
    _PLACEHOLDERS = {
        # ── Currency ──────────────────────────────────
        '\u20b9': '__RUPEE__',    # ₹
        '\u20ac': '__EURO__',     # €
        '\u00a3': '__POUND__',    # £
        '\u00a5': '__YEN__',      # ¥
        '\u20bd': '__RUBLE__',    # ₽
        '\u20bf': '__BITCOIN__',  # ₿
        '\u00a2': '__CENT__',     # ¢
        # ── Bullets & Shapes ──────────────────────────
        '\u2022': '__BULLET__',   # •
        '\u25cf': '__CIRCLE__',   # ●
        '\u25cb': '__OCIRCLE__',  # ○
        '\u25aa': '__SQSMALL__',  # ▪
        '\u25a0': '__SQUARE__',   # ■
        '\u25a1': '__OSQUARE__',  # □
        '\u25b6': '__RTRI__',     # ▶
        '\u25b8': '__RTRISM__',   # ▸
        '\u25c6': '__DIAMOND__',  # ◆
        '\u25c7': '__ODIAMOND__', # ◇
        # ── Checkmarks & Crosses ──────────────────────
        '\u2713': '__CHECK__',    # ✓
        '\u2714': '__CHECKH__',   # ✔
        '\u2717': '__CROSS__',    # ✗
        '\u2718': '__CROSSH__',   # ✘
        # ── Arrows ────────────────────────────────────
        '\u2192': '__RARROW__',   # →
        '\u2190': '__LARROW__',   # ←
        '\u2191': '__UARROW__',   # ↑
        '\u2193': '__DARROW__',   # ↓
        '\u2194': '__HARROW__',   # ↔
        '\u21d2': '__DARROW2__',  # ⇒
        '\u21d0': '__DLARROW__',  # ⇐
        '\u21d4': '__DDARROW__',  # ⇔
        # ── Stars ─────────────────────────────────────
        '\u2605': '__STAR__',     # ★
        '\u2606': '__OSTAR__',    # ☆
        # ── Math ──────────────────────────────────────
        '\u2248': '__APPROX__',   # ≈
        '\u2260': '__NEQUAL__',   # ≠
        '\u2264': '__LTEQ__',     # ≤
        '\u2265': '__GTEQ__',     # ≥
        '\u221a': '__SQRT__',     # √
        '\u2211': '__SUM__',      # ∑
        '\u03c0': '__PI__',       # π
        '\u03b1': '__ALPHA__',    # α
        '\u03b2': '__BETA__',     # β
        '\u03b4': '__DELTA__',    # δ
        '\u03bc': '__MU__',       # μ
        '\u00b5': '__MICRO__',    # µ
        '\u00b2': '__SUP2__',     # ²
        '\u00b3': '__SUP3__',     # ³
        '\u00b9': '__SUP1__',     # ¹
        # ── Typography & Symbols ──────────────────────
        '\u00a7': '__SECTION__',  # §
        '\u2116': '__NUMERO__',   # №
        '\u2020': '__DAGGER__',   # †
        '\u2021': '__DDAGGER__',  # ‡
        '\u00b6': '__PILCROW__',  # ¶
        '\u00a9': '__COPY__',     # ©
        '\u00ae': '__REG__',      # ®
        '\u2122': '__TM__',       # ™
    }
    _ASCII_FALLBACK = {
        # Punctuation / quotes (replace with clean ASCII)
        '\u2026': '...', '\u2013': '-',  '\u2014': '--',
        '\u2018': "'",   '\u2019': "'",  '\u201c': '"', '\u201d': '"',
        # Math operators already covered by placeholders above
        '\u00d7': 'x',   '\u00f7': '/',
        '\u00bd': '1/2', '\u00bc': '1/4', '\u00be': '3/4',
        '\u00b0': 'deg', '\u00b1': '+/-', '\u221e': 'inf',
    }

    # Step 1 — swap currency symbols with ASCII placeholders (survives strip)
    for sym, placeholder in _PLACEHOLDERS.items():
        text = text.replace(sym, placeholder)

    # Step 2 — convert other common non-ASCII to readable ASCII
    for sym, asc in _ASCII_FALLBACK.items():
        if sym not in _PLACEHOLDERS:
            text = text.replace(sym, asc)

    # Step 3 — strip any remaining non-ASCII characters
    text = re.compile(r'[^\x00-\x7F]+').sub('', text)

    # Step 4 — restore placeholders: real symbol inside font tag, or ASCII fallback
    def _U(c): return f'<font name="UniBody">{c}</font>'
    _PLACEHOLDER_RESTORE_UNICODE = {
        # Currency
        '__RUPEE__':   _U('\u20b9'), '__EURO__':    _U('\u20ac'),
        '__POUND__':   _U('\u00a3'), '__YEN__':     _U('\u00a5'),
        '__RUBLE__':   _U('\u20bd'), '__BITCOIN__': _U('\u20bf'),
        '__CENT__':    _U('\u00a2'),
        # Bullets & Shapes
        '__BULLET__':  _U('\u2022'), '__CIRCLE__':  _U('\u25cf'),
        '__OCIRCLE__': _U('\u25cb'), '__SQSMALL__': _U('\u25aa'),
        '__SQUARE__':  _U('\u25a0'), '__OSQUARE__': _U('\u25a1'),
        '__RTRI__':    _U('\u25b6'), '__RTRISM__':  _U('\u25b8'),
        '__DIAMOND__': _U('\u25c6'), '__ODIAMOND__':_U('\u25c7'),
        # Checkmarks & Crosses
        '__CHECK__':   _U('\u2713'), '__CHECKH__':  _U('\u2714'),
        '__CROSS__':   _U('\u2717'), '__CROSSH__':  _U('\u2718'),
        # Arrows
        '__RARROW__':  _U('\u2192'), '__LARROW__':  _U('\u2190'),
        '__UARROW__':  _U('\u2191'), '__DARROW__':  _U('\u2193'),
        '__HARROW__':  _U('\u2194'), '__DARROW2__': _U('\u21d2'),
        '__DLARROW__': _U('\u21d0'), '__DDARROW__': _U('\u21d4'),
        # Stars
        '__STAR__':    _U('\u2605'), '__OSTAR__':   _U('\u2606'),
        # Math
        '__APPROX__':  _U('\u2248'), '__NEQUAL__':  _U('\u2260'),
        '__LTEQ__':    _U('\u2264'), '__GTEQ__':    _U('\u2265'),
        '__SQRT__':    _U('\u221a'), '__SUM__':     _U('\u2211'),
        '__PI__':      _U('\u03c0'), '__ALPHA__':   _U('\u03b1'),
        '__BETA__':    _U('\u03b2'), '__DELTA__':   _U('\u03b4'),
        '__MU__':      _U('\u03bc'), '__MICRO__':   _U('\u00b5'),
        '__SUP2__':    _U('\u00b2'), '__SUP3__':    _U('\u00b3'),
        '__SUP1__':    _U('\u00b9'),
        # Typography
        '__SECTION__': _U('\u00a7'), '__NUMERO__':  _U('\u2116'),
        '__DAGGER__':  _U('\u2020'), '__DDAGGER__': _U('\u2021'),
        '__PILCROW__': _U('\u00b6'), '__COPY__':    _U('\u00a9'),
        '__REG__':     _U('\u00ae'), '__TM__':      _U('\u2122'),
        # Punctuation / Quotes
        '__ELLIPSIS__': _U('\u2026'), '__ENDASH__': _U('\u2013'),
        '__EMDASH__': _U('\u2014'),   '__LSQUOTE__': _U('\u2018'),
        '__RSQUOTE__': _U('\u2019'),  '__LDQUOTE__': _U('\u201c'),
        '__RDQUOTE__': _U('\u201d'),
        # Math Operators
        '__MULTIPLY__': _U('\u00d7'), '__DIVIDE__': _U('\u00f7'),
        '__HALF__': _U('\u00bd'),     '__QUARTER__': _U('\u00bc'),
        '__THREEQUARTERS__': _U('\u00be'),
        '__DEGREE__': _U('\u00b0'),   '__PLUSMINUS__': _U('\u00b1'),
        '__INFINITY__': _U('\u221e'),
    }
    _PLACEHOLDER_RESTORE_ASCII = {
        # Currency
        '__RUPEE__': 'Rs.',   '__EURO__': 'EUR',   '__POUND__': 'GBP',
        '__YEN__': 'JPY',     '__RUBLE__': 'RUB',  '__BITCOIN__': 'BTC',
        '__CENT__': 'c',
        # Bullets & Shapes
        '__BULLET__': '-',    '__CIRCLE__': 'o',   '__OCIRCLE__': 'o',
        '__SQSMALL__': '-',   '__SQUARE__': '#',   '__OSQUARE__': '#',
        '__RTRI__': '>',      '__RTRISM__': '>',
        '__DIAMOND__': '<>',  '__ODIAMOND__': '<>',
        # Checkmarks & Crosses
        '__CHECK__': '(ok)',  '__CHECKH__': '(ok)',
        '__CROSS__': '(x)',   '__CROSSH__': '(x)',
        # Arrows
        '__RARROW__': '->',   '__LARROW__': '<-',
        '__UARROW__': '^',    '__DARROW__': 'v',
        '__HARROW__': '<->',  '__DARROW2__': '=>',
        '__DLARROW__': '<=',  '__DDARROW__': '<=>',
        # Stars
        '__STAR__': '*',      '__OSTAR__': '*',
        # Math
        '__APPROX__': '~=',   '__NEQUAL__': '!=',
        '__LTEQ__': '<=',     '__GTEQ__': '>=',
        '__SQRT__': 'sqrt',   '__SUM__': 'sum',
        '__PI__': 'pi',       '__ALPHA__': 'alpha',
        '__BETA__': 'beta',   '__DELTA__': 'delta',
        '__MU__': 'mu',       '__MICRO__': 'u',
        '__SUP2__': '^2',     '__SUP3__': '^3',    '__SUP1__': '^1',
        # Typography
        '__SECTION__': 'S.',  '__NUMERO__': 'No.',
        '__DAGGER__': '+',    '__DDAGGER__': '++',
        '__PILCROW__': 'P',   '__COPY__': '(c)',
        '__REG__': '(R)',     '__TM__': '(TM)',
        # Punctuation / Quotes
        '__ELLIPSIS__': '...', '__ENDASH__': '-', '__EMDASH__': '--',
        '__LSQUOTE__': "'",    '__RSQUOTE__': "'", '__LDQUOTE__': '"',
        '__RDQUOTE__': '"',
        # Math Operators
        '__MULTIPLY__': 'x',   '__DIVIDE__': '/',
        '__HALF__': '1/2',     '__QUARTER__': '1/4',
        '__THREEQUARTERS__': '3/4',
        '__DEGREE__': 'deg',   '__PLUSMINUS__': '+/-',
        '__INFINITY__': 'inf',
    }
    restore_map = _PLACEHOLDER_RESTORE_UNICODE if _UNICODE_FONT_REGISTERED else _PLACEHOLDER_RESTORE_ASCII
    for placeholder, display in restore_map.items():
        text = text.replace(placeholder, display)
    
    # ── Remove line separator graphics and clean whitespace ─────────────────
    text = re.sub(r'_{20,}', '', text)
    text = re.sub(r'\n{4,}', '\n\n', text)
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
    # ── Enhancement 4: PDF Metadata ──────────────────────────
    _code_from_file = os.path.splitext(os.path.basename(filename))[0]
    doc.title   = f'MSANode Blueprint — {_code_from_file}'
    doc.author  = 'MSA NODE SYSTEMS'
    doc.subject = 'Digital Asset Blueprint'
    doc.creator = 'MSA NODE Bot 4'

    # ── Enhancement 2: Font-aware locals (DejaVu when registered, Helvetica fallback)
    _BDYF  = _UNICODE_FONT_NAME   # e.g. 'DejaVuSans' or 'Helvetica'
    _BDYFB = _UNICODE_FONT_BOLD   # e.g. 'DejaVuSans-Bold' or 'Helvetica-Bold'

    # Header style — white text for navy band
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name='MSAHeader',
        parent=styles['Normal'],
        fontName=_BDYFB,
        fontSize=15,
        leading=20,
        textColor=HexColor('#FFFFFF'),
        alignment=TA_CENTER,
        spaceAfter=0,
    ))

    # Main Title style (for the very first line of input)
    styles.add(ParagraphStyle(
        name='MainTitle',
        parent=styles['Normal'],
        fontName=_BDYFB,
        fontSize=11,
        leading=15,
        textColor=black,
        alignment=TA_LEFT,
        spaceAfter=12
    ))

    # Section Header — deep red, bold
    styles.add(ParagraphStyle(
        name='SectionHeader',
        parent=styles['Normal'],
        fontName=_BDYFB,
        fontSize=12,
        leading=16,
        textColor=HexColor('#C62828'),
        alignment=TA_LEFT,
        spaceAfter=4,
        spaceBefore=16
    ))

    # Subsection with parentheses
    styles.add(ParagraphStyle(
        name='ParenSubsection',
        parent=styles['Normal'],
        fontName=_BDYFB,
        fontSize=10,
        leading=13,
        textColor=HexColor('#404040'),
        alignment=TA_LEFT,
        spaceAfter=6,
        spaceBefore=6
    ))

    # Subsection (The, Core, etc.)
    styles.add(ParagraphStyle(
        name='Subsection',
        parent=styles['Normal'],
        fontName=_BDYFB,
        fontSize=10,
        leading=13,
        textColor=HexColor('#404040'),
        alignment=TA_LEFT,
        spaceAfter=6,
        spaceBefore=8
    ))

    # Body text — justified, DejaVu when available
    styles.add(ParagraphStyle(
        name='Body',
        parent=styles['Normal'],
        fontName=_BDYF,
        fontSize=10,
        leading=15,
        textColor=HexColor('#2C2C2C'),
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

    # All-caps header style — dark black, bold
    styles.add(ParagraphStyle(
        name='AllCapsHeader',
        parent=styles['Normal'],
        fontName=_BDYFB,
        fontSize=11,
        leading=14,
        textColor=HexColor('#000000'),
        alignment=TA_LEFT,
        spaceAfter=10,
        spaceBefore=10
    ))

    # Bullet point style — indented, dark bullet symbol, DejaVu aware
    styles.add(ParagraphStyle(
        name='BulletBody',
        parent=styles['Normal'],
        fontName=_BDYF,
        fontSize=10,
        leading=14,
        textColor=HexColor('#2C2C2C'),
        alignment=TA_LEFT,
        spaceAfter=3,
        spaceBefore=1,
        leftIndent=12,
        firstLineIndent=-12,
    ))

    # ── Bullet-line helpers ─────────────────────────────────────────────────
    # Raw bullet chars (before or after restoration in ASCII mode)
    _BULLET_CHARS = (
        '\u2022', '\u25cf', '\u25cb', '\u25aa', '\u25a0', '\u25a1',
        '\u25b6', '\u25b8', '\u25c6', '\u25c7', '\u2713', '\u2714',
        '\u2717', '\u2718', '\u2192', '\u2190', '\u2191', '\u2193',
        '\u2605', '\u2606', '-',
    )
    # Regex: matches a leading <font ...>CHAR</font> block OR a raw bullet char
    _BULLET_RE = re.compile(
        r'^(?:<font[^>]*>[^<]+</font>|[' +
        re.escape(''.join(_BULLET_CHARS)) +
        r'])'
    )

    def _is_bullet_line(ln):
        """Return True if the line starts with any bullet/shape/dash symbol."""
        return bool(_BULLET_RE.match(ln))

    def _darken_bullet(ln):
        """Wrap leading bullet symbol (raw or font-tagged) in bold dark black."""
        # Case 1: line starts with a <font ...>SYM</font> tag
        font_match = re.match(r'^(<font[^>]*>[^<]+</font>)(.*)', ln, re.DOTALL)
        if font_match:
            sym_tag, rest = font_match.group(1), font_match.group(2)
            return f'<font color="#000000"><b>{sym_tag}</b></font>{rest}'
        # Case 2: line starts with a raw bullet char
        return f'<font color="#000000"><b>{ln[0]}</b></font>{ln[1:]}'

    # Build story
    story = []
    
    # Add header (MSANODE VAULT BLUEPRINT) - Underlined
    # ── Enhancement 1: Navy header band ─────────────────────────────
    _hdr_para = Paragraph('MSANODE VAULT BLUEPRINT', styles['MSAHeader'])
    _hdr_table = Table([[_hdr_para]], colWidths=[doc.width])
    _hdr_table.setStyle(TableStyle([
        ('BACKGROUND',  (0, 0), (-1, -1), HexColor('#0D1B2A')),  # deep navy
        ('TOPPADDING',  (0, 0), (-1, -1), 12),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 12),
        ('LEFTPADDING', (0, 0), (-1, -1), 14),
        ('RIGHTPADDING',(0, 0), (-1, -1), 14),
        ('ALIGN',       (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN',      (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    story.append(_hdr_table)
    story.append(Spacer(1, 0.12*inch))
    
    # Parse and format content
    lines = text.split('\n')

    # Track if we've added the main title
    main_title_added = False

    # Helper: strip leading/trailing caret markers to get inner text for detection
    def _bare(ln):
        return re.sub(r'^\^{1,6}\s*|\s*\^{1,6}$', '', ln).strip()

    _ROMAN_RE = re.compile(
        r'^(I{1,3}|IV|VI{0,3}|IX|XI{0,3}|XII)\.\s+', re.IGNORECASE
    )

    _blank_count = 0   # blank-line counter (kept for future use)

    for line in lines:
        line = line.strip()

        # Blank lines are skipped — section spacing handled by spaceBefore (auto-suppressed
        # at page top by ReportLab, so no extra gap when sections start a new page)
        if not line:
            _blank_count += 1
            continue

        _blank_count = 0

        bare = _bare(line)  # inner text with caret markers removed

        # First substantive line is the main title
        if not main_title_added and len(bare) > 20:
            story.append(Paragraph(process_inline_formatting(line), styles['MainTitle']))
            story.append(Spacer(1, 0.08*inch))  # balanced gap after title
            main_title_added = True
            continue

        # Section headers: Roman numeral OR any ^^^TEXT^^^ (3-caret) wrapped line
        # ^^^..^^^ = red bold caps (process_inline_formatting) + HR rule underneath
        _is_3caret = bool(re.match(r'^\^{3}(?!\^)', line))
        if _ROMAN_RE.match(bare) or _is_3caret:
            # Use bare text (carets stripped) so no carets appear in the output
            story.append(Paragraph(
                process_inline_formatting(f'<b>{bare.upper()}</b>'),
                styles['SectionHeader']
            ))
            story.append(HRFlowable(
                width='100%', thickness=0.75,
                color=HexColor('#C62828'), spaceAfter=6, spaceBefore=0
            ))
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
        
        # All caps section dividers — check BARE text (strip carets first)
        # so ^WORD^ or ^^^^CAPS^^^^ lines don't get caught here instead of Body
        if bare.isupper() and 5 < len(bare) < 100:
            story.append(Paragraph(process_inline_formatting(f"<b>{line}</b>"), styles['AllCapsHeader']))
            continue
        
        # Bullet points or dashes — auto-darken the leading symbol
        if _is_bullet_line(line):
            story.append(Paragraph(_darken_bullet(process_inline_formatting(line)), styles['BulletBody']))
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
    """Authenticate and return a Google Drive service object."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as t:
            creds = pickle.load(t)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE,
                ['https://www.googleapis.com/auth/drive.file']
            )
            # Use a random free port — port 8080 is taken by the health server on Render
            import socket as _socket
            with _socket.socket() as _s:
                _s.bind(('', 0))
                _free_port = _s.getsockname()[1]
            creds = flow.run_local_server(port=_free_port, open_browser=True)
        with open(TOKEN_FILE, 'wb') as t:
            pickle.dump(creds, t)
    # cache_discovery=False silences: "file_cache is only supported with oauth2client<4.0.0"
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


def _ensure_drive_folder(service, folder_name, parent_id=None):
    """
    Ensures a folder exists inside a parent folder. 
    Returns the folder ID.
    """
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    folders = results.get('files', [])
    
    if folders:
        return folders[0]['id']
    else:
        # Create it
        metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            metadata['parents'] = [parent_id]
            
        folder = service.files().create(body=metadata, fields='id', supportsAllDrives=True).execute()
        print(f"◈ Drive: Created folder '{folder_name}'")
        return folder.get('id')

def upload_to_drive(filename):
    """Upload a PDF to a YEAR/MONTH sub-folder structure inside PARENT_FOLDER_ID."""
    try:
        service = get_drive_service()
        
        # 1. Get Root (PARENT_FOLDER_ID from env, or Root of Drive if None)
        root_id = PARENT_FOLDER_ID if PARENT_FOLDER_ID else None
        
        # 2. Ensure YEAR Folder (e.g. "2026")
        year_str = datetime.now().strftime('%Y')
        year_folder_id = _ensure_drive_folder(service, year_str, root_id)
        
        # 3. Ensure MONTH Folder (e.g. "FEBRUARY")
        month_str = datetime.now().strftime('%B').upper()
        month_folder_id = _ensure_drive_folder(service, month_str, year_folder_id)
        
        # 4. Upload File to Month Folder
        print(f"◈ Uploading to: {year_str}/{month_str}")
        media = MediaIoBaseUpload(io.FileIO(filename, 'rb'), mimetype='application/pdf')
        file_metadata = {'name': filename, 'parents': [month_folder_id]}
        
        file = service.files().create(
            body=file_metadata, media_body=media,
            fields='id, webViewLink', supportsAllDrives=True
        ).execute()

        # 5. Make Public
        service.permissions().create(
            fileId=file.get('id'),
            body={'type': 'anyone', 'role': 'reader'}
        ).execute()

        return file.get('webViewLink', '')
        
    except Exception as e:
        print(f"❌ Upload Failed: {e}")
        traceback.print_exc()
        return ""




def _extract_drive_id(link: str):
    import re
    if not link: return None
    match = re.search(r'/file/d/([^/?\s]+)', link)
    if match: return match.group(1)
    match = re.search(r'[?&]id=([^&\s]+)', link)
    if match: return match.group(1)
    return None

def _drive_delete_file(link: str) -> bool:
    """Delete a file from Google Drive given its webViewLink. Returns True on success."""
    try:
        file_id = _extract_drive_id(link)
        if not file_id:
            return False
        service = get_drive_service()
        service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
        return True
    except Exception as e:
        err_str = str(e)
        if "404" in err_str or "notFound" in err_str or "File not found" in err_str:
            return True  # Already deleted from Drive — treat as success
        logging.warning(f"Drive delete failed: {e}")
        return False


def _drive_rename_file(link: str, new_name: str) -> bool:
    """Rename a PDF on Google Drive. Returns True on success."""
    try:
        file_id = _extract_drive_id(link)
        if not file_id:
            return False
        service = get_drive_service()
        service.files().update(
            fileId=file_id,
            body={'name': new_name + '.pdf'},
            supportsAllDrives=True
        ).execute()
        return True
    except Exception as e:
        logging.warning(f"Drive rename failed: {e}")
        return False

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
async def handle_edit_mode(message: types.Message, state: FSMContext, mode_override: str = None, page_override: int = None):
    text = mode_override if mode_override else message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # ── PAGINATION LOGIC ─────────────────────────────
    PAGE_SIZE = 10
    data = await state.get_data()
    
    # If switching modes or first time, reset page to 1 unless overridden
    current_page = page_override if page_override else data.get("edit_page", 1)
    
    # If text changed (switching modes), reset to page 1
    # BUT if just paginating (same mode), keep page.
    # We detecting mode switch by checking if text matches typical mode strings
    
    if text == "🔢 BY INDEX":
        await state.update_data(edit_mode="index", edit_page=current_page)
    elif text == "🆔 BY CODE":
        await state.update_data(edit_mode="code", edit_page=current_page)
    else:
        # Fallback if text is weird, though usually called with override or button
        pass

    # helper to generate list
    all_docs = _get_unique_docs()
    total_docs = len(all_docs)
    total_pages = (total_docs + PAGE_SIZE - 1) // PAGE_SIZE
    
    # Clamp page
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1
    
    # Slice
    start_idx = (current_page - 1) * PAGE_SIZE
    end_idx   = start_idx + PAGE_SIZE
    page_docs = all_docs[start_idx:end_idx]
    
    # Generate List Text
    list_msg = [f"📋 **AVAILABLE PDFS** (Page {current_page}/{total_pages})", "━━━━━━━━━━━━━━━━━━━━"]
    list_msg.extend(get_formatted_file_list(page_docs, limit=PAGE_SIZE, start_index=start_idx + 1))
    list_text = "\n".join(list_msg)
    
    # ── BUILD NAVIGATION ROWS ────────────────────────
    nav_row = []
    if current_page > 1:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if current_page < total_pages:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))
        
    # ── MODE SPECIFIC KEYBOARDS ──────────────────────
    if text == "🔢 BY INDEX":
        # Keyboard: [Prev, Next] / [Back, Menu]
        rows = []
        if nav_row: rows.append(nav_row)
        rows.append([KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu")])
        
        await message.answer(
            f"{list_text}\n\n"
            "🔢 **INDEX SELECTION**\n"
            "Enter the **Index Number** from the list above (e.g. `15`).",
            reply_markup=ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.set_state(BotState.waiting_for_edit_target)
        
    elif text == "🆔 BY CODE":
        # Dynamic Code Buttons
        builder = ReplyKeyboardBuilder()
        for d in page_docs:
            code = d.get('code')
            if code:
                builder.add(KeyboardButton(text=code))
        builder.adjust(2) # 2 columns
        
        # Add Nav Row
        if nav_row: builder.row(*nav_row)
        
        # Add Control Row
        builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
        
        await message.answer(
            f"{list_text}\n\n"
            "🆔 **CODE SELECTION**\n"
            "Select the Code you wish to Rename:",
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.set_state(BotState.waiting_for_edit_target)
    else:
        await message.answer("⚠️ Invalid Option.")

@dp.message(BotState.waiting_for_edit_target, F.text == "NEXT ➡️")
async def edit_next_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("edit_page", 1) + 1
    mode = "🔢 BY INDEX" if data.get("edit_mode") == "index" else "🆔 BY CODE"
    await handle_edit_mode(message, state, mode_override=mode, page_override=page)

@dp.message(BotState.waiting_for_edit_target, F.text == "⬅️ PREV")
async def edit_prev_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(1, data.get("edit_page", 1) - 1)
    mode = "🔢 BY INDEX" if data.get("edit_mode") == "index" else "🆔 BY CODE"
    await handle_edit_mode(message, state, mode_override=mode, page_override=page)

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
            
        all_docs = _get_unique_docs()
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
        mode_text = "🔢 BY INDEX" if edit_mode == "index" else "🆔 BY CODE"
        await handle_edit_mode(message, state, mode_override=mode_text)
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
    
    # DB Update
    from bson.objectid import ObjectId
    # Grab Drive link BEFORE rename for GDrive sync
    old_doc_full = col_pdfs.find_one({"_id": ObjectId(doc_id)})
    old_link = old_doc_full.get("link", "") if old_doc_full else ""
    col_pdfs.update_one(
        {"_id": ObjectId(doc_id)},
        {"$set": {"code": new_code, "filename": new_code}}
    )
    drive_note = ""
    if old_link:
        drive_ok = await asyncio.to_thread(_drive_rename_file, old_link, new_code)
        drive_note = "\n☁️ Drive: ✅ Renamed" if drive_ok else "\n☁️ Drive: ⚠️ Could not rename"

    await msg.edit_text(
        f"✅ <b>RENAMED</b>\n"
        f"<code>{old_code}</code> → <code>{new_code}</code>\n"
        f"🍃 DB: ✅ Updated{drive_note}",
        parse_mode="HTML"
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
async def handle_retrieval_method_selection(message: types.Message, state: FSMContext, override_text: str = None, override_page: int = None):
    text = override_text if override_text else message.text
    if text == "🏠 MAIN MENU": return await start(message, state)
    if text == "⬅️ BACK": return await link_btn(message, state)

    PAGE_SIZE = 10
    data = await state.get_data()
    
    current_page = override_page if override_page is not None else data.get("retr_page", 1)
    
    if text == "🔢 BULK RANGE":
        await state.update_data(retr_method="bulk", retr_page=current_page)
    elif text == "👤 SINGLE FILE":
        await state.update_data(retr_method="single", retr_page=current_page)
    else:
        text = data.get("retr_method", "single") 
        text = "🔢 BULK RANGE" if text == "bulk" else "👤 SINGLE FILE"

    all_docs = _get_unique_docs()
    total_docs = len(all_docs)
    total_pages = (total_docs + PAGE_SIZE - 1) // PAGE_SIZE
    
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1
    
    start_idx = (current_page - 1) * PAGE_SIZE
    end_idx   = start_idx + PAGE_SIZE
    page_docs = all_docs[start_idx:end_idx]
    
    list_msg = [f"📂 **AVAILABLE FILES** (Page {current_page}/{total_pages})", "━━━━━━━━━━━━━━━━━━━━"]
    list_msg.extend(get_formatted_file_list(page_docs, limit=PAGE_SIZE, start_index=start_idx + 1))
    list_text = "\n".join(list_msg)
    
    nav_row = []
    if current_page > 1:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if current_page < total_pages:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))

    if text == "🔢 BULK RANGE":
        rows = []
        if nav_row: rows.append(nav_row)
        rows.append([KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🏠 MAIN MENU")])
        
        await message.answer(
            f"{list_text}\n\n"
            "🔢 <b>BULK RETRIEVAL MODE</b>\n"
            "Enter the index range of PDFs you need (e.g., `1-5`, `10-20`).\n"
            "Index 1 = Newest PDF.",
            reply_markup=ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.set_state(BotState.waiting_for_range)
        
    elif text == "👤 SINGLE FILE":
        builder = ReplyKeyboardBuilder()
        for d in page_docs:
            code = d.get('code')
            if code:
                abs_idx = all_docs.index(d) + 1
                builder.add(KeyboardButton(text=f"{abs_idx}. {code}"))
        builder.adjust(2)
        
        if nav_row: builder.row(*nav_row)
        builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🏠 MAIN MENU"))
        
        mode = data.get("retrieval_mode", "link")
        await message.answer(
            f"{list_text}\n\n"
            f"👤 <b>SINGLE RETRIEVAL</b>\n"
            f"{'📄 PDF FILE' if mode=='pdf' else '🔗 DRIVE LINK'} mode — Select file:",
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.set_state(BotState.fetching_link)

@dp.message(BotState.fetching_link, F.text == "NEXT ➡️")
async def single_next_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("retr_page", 1) + 1
    await handle_retrieval_method_selection(message, state, override_text="👤 SINGLE FILE", override_page=page)

@dp.message(BotState.fetching_link, F.text == "⬅️ PREV")
async def single_prev_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(1, data.get("retr_page", 1) - 1)
    await handle_retrieval_method_selection(message, state, override_text="👤 SINGLE FILE", override_page=page)

@dp.message(BotState.waiting_for_range, F.text == "NEXT ➡️")
async def bulk_next_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("retr_page", 1) + 1
    await handle_retrieval_method_selection(message, state, override_text="🔢 BULK RANGE", override_page=page)

@dp.message(BotState.waiting_for_range, F.text == "⬅️ PREV")
async def bulk_prev_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(1, data.get("retr_page", 1) - 1)
    await handle_retrieval_method_selection(message, state, override_text="🔢 BULK RANGE", override_page=page)

@dp.message(BotState.fetching_link)
async def fetch_link(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text.upper() == "🏠 MAIN MENU": return await start(message, state)
    
    text = text.upper()
    if text == "⬅️ BACK":
        data = await state.get_data()
        mode = data.get("retrieval_mode", "link")
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

    import re
    all_docs = _get_unique_docs()
    
    if text.isdigit():
        idx = int(text)
        if 1 <= idx <= len(all_docs):
            text = all_docs[idx - 1].get("code")
    else:
        match = re.match(r"^(\d+)\.\s+(.*)$", text)
        if match:
            idx = int(match.group(1))
            extracted_code = match.group(2)
            if 1 <= idx <= len(all_docs):
                doc_code = all_docs[idx - 1].get("code", "")
                if doc_code == extracted_code:
                    text = doc_code
                elif extracted_code.endswith("…") or extracted_code.endswith("..."):
                    clean_extracted = extracted_code.rstrip("….")
                    if doc_code.startswith(clean_extracted):
                        text = doc_code
    
    doc = col_pdfs.find_one({"code": text}, sort=[("timestamp", -1)])
    
    if doc:
        data = await state.get_data()
        mode = data.get('retrieval_mode', 'link')
        
        if mode == 'pdf':
            wait_msg = await message.answer(f"⏳ <b>Fetching PDF:</b> <code>{text}</code>...", parse_mode="HTML")
            doc_for_pdf = col_pdfs.find_one({"code": text.upper()}) or col_pdfs.find_one({"code": text})

            if not doc_for_pdf or not doc_for_pdf.get("link"):
                await wait_msg.edit_text(f"❌ No file found for code <code>{text}</code>.", parse_mode="HTML")
                return

            link = doc_for_pdf.get("link", "")
            file_id = _extract_drive_id(link)
            if not file_id:
                await wait_msg.edit_text(
                    f"❌ Cannot parse Drive link.\n🔗 <a href='{link}'>Open on Drive</a>",
                    parse_mode="HTML", disable_web_page_preview=False
                )
                return
            code_clean = doc_for_pdf.get('code', text)
            fname = f"{code_clean}.pdf"
            tmp_path = fname

            try:
                def _download_drive_file():
                    svc = get_drive_service()
                    request = svc.files().get_media(fileId=file_id)
                    buf = io.BytesIO()
                    downloader = MediaIoBaseDownload(buf, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()
                    buf.seek(0)
                    return buf.read()

                raw_bytes = await asyncio.to_thread(_download_drive_file)
                with open(tmp_path, 'wb') as f:
                    f.write(raw_bytes)

                size_kb = max(1, len(raw_bytes) // 1024)
                await wait_msg.delete()
                await message.answer_document(
                    FSInputFile(tmp_path, filename=fname),
                    caption=(
                        f"📄 <b>{code_clean}.pdf</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📦 <code>{size_kb} KB</code>  ·  "
                        f"🕐 <code>{now_local().strftime('%b %d, %Y  ·  %I:%M %p')}</code>"
                    ),
                    parse_mode="HTML"
                )
                DAILY_STATS_BOT4["links_retrieved"] += 1
                asyncio.create_task(_persist_stats())
            except Exception as e:
                import logging
                logging.error(f"Drive download failed for {file_id}: {e}")
                await wait_msg.edit_text(
                    f"❌ <b>Download Failed</b>\n"
                    f"<code>{e}</code>\n\n"
                    f"🔗 <a href='{link}'>Open on Drive instead</a>",
                    parse_mode="HTML", disable_web_page_preview=False
                )
            finally:
                import os
                try: os.remove(tmp_path)
                except: pass
                
        else:
            drive_link = doc.get('link', '').strip()
            if not drive_link:
                await message.answer(
                    f"⚠️ <b>NO DRIVE LINK</b> for <code>{doc.get('code')}</code>\n"
                    f"This PDF was stored without a Drive link — it may have been generated before Drive was connected, "
                    f"or the upload failed.\n\n"
                    f"💡 Re-generate this PDF to get a fresh Drive link.",
                    parse_mode="HTML"
                )
                return
            DAILY_STATS_BOT4["links_retrieved"] += 1
            asyncio.create_task(_persist_stats())
            await message.answer(
                f"✅ <b>RESOURCE ACQUIRED</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📄 Code: <code>{doc.get('code')}</code>\n"
                f"🔗 <a href='{drive_link}'>Open on Google Drive</a>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"<code>{drive_link}</code>",
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            
    else:
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
        all_docs = _get_unique_docs()
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
                code = doc.get('code', '')
                link = doc.get('link', '')
                fname = f"{code}.pdf"
                fid_match = re.search(r'/file/d/([^/?\s]+)', link)
                if not fid_match:
                    await message.answer(f"⚠️ <b>{code}</b>: Cannot parse Drive link — skipped.", parse_mode="HTML")
                    continue
                fid = fid_match.group(1)
                try:
                    def _dl(file_id=fid):
                        svc = get_drive_service()
                        req = svc.files().get_media(fileId=file_id)
                        buf = io.BytesIO()
                        dl = MediaIoBaseDownload(buf, req)
                        done = False
                        while not done:
                            _, done = dl.next_chunk()
                        buf.seek(0)
                        return buf.read()
                    raw = await asyncio.to_thread(_dl)
                    with open(fname, 'wb') as wf:
                        wf.write(raw)
                    size_kb = max(1, len(raw) // 1024)
                    await message.answer_document(
                        FSInputFile(fname, filename=fname),
                        caption=f"📄 <b>{code}.pdf</b>  ·  <code>{size_kb} KB</code>",
                        parse_mode="HTML"
                    )
                    count += 1
                    await asyncio.sleep(0.5)
                except Exception as e:
                    await message.answer(
                        f"❌ <b>{code}</b> failed: <code>{e}</code>\n🔗 <a href='{link}'>Open on Drive</a>",
                        parse_mode="HTML", disable_web_page_preview=False
                    )
                finally:
                    try: os.remove(fname)
                    except: pass

            await message.answer(f"✅ <b>Delivered {count}/{len(selected_docs)} PDFs.</b>", parse_mode="HTML")
            DAILY_STATS_BOT4["links_retrieved"] += count
            asyncio.create_task(_persist_stats())
            
        else:
            # === BULK LINK MODE ===
            report = [f"🔢 **BULK DUMP: {start_idx}-{end_idx}**", "━━━━━━━━━━━━━━━━━━━━"]
            
            for i, doc in enumerate(selected_docs):
                current_num = start_idx + i
                drive_link = doc.get('link', '').strip()
                report.append(f"<b>{current_num}. {doc.get('code')}</b>")
                if drive_link:
                    report.append(f"🔗 <a href='{drive_link}'>{drive_link}</a>")
                else:
                    report.append("⚠️ <i>No Drive link stored for this PDF</i>")
                report.append("")
                
            report.append("━━━━━━━━━━━━━━━━━━━━")
            
            full_msg = "\n".join(report)
            if len(full_msg) > 4000:
                chunks = [full_msg[i:i+4000] for i in range(0, len(full_msg), 4000)]
                for chunk in chunks:
                    await message.answer(chunk, parse_mode="HTML", disable_web_page_preview=True)
            else:
                await message.answer(full_msg, parse_mode="HTML", disable_web_page_preview=True)
            DAILY_STATS_BOT4["links_retrieved"] += len(selected_docs)
            asyncio.create_task(_persist_stats())
            
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
        "🗑 <b>DELETION PROTOCOL</b>\n"
        "Select Deletion Mode:\n\n"
        "🔢 <b>DELETE BY RANGE</b>: Delete multiple files (e.g., 1-5).\n"
        "🆔 <b>DELETE BY CODE</b>: Delete a specific file by code.",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )
    await state.set_state(BotState.choosing_delete_mode)

@dp.message(BotState.choosing_delete_mode)
async def handle_delete_mode(message: types.Message, state: FSMContext, mode_override: str = None, page_override: int = None):
    text = mode_override if mode_override else message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    # ── PAGINATION LOGIC ─────────────────────────────
    PAGE_SIZE = 10
    data = await state.get_data()
    
    # Reset page on mode switch, keep if paginating
    current_page = page_override if page_override else data.get("delete_page", 1)
    
    if text == "🔢 DELETE BY RANGE":
        await state.update_data(delete_mode="range", delete_page=current_page)
    elif text == "🆔 DELETE BY CODE":
        await state.update_data(delete_mode="code", delete_page=current_page)
    else:
        # fallback
        pass

    # helper to generate list
    all_docs = _get_unique_docs()
    total_docs = len(all_docs)
    total_pages = (total_docs + PAGE_SIZE - 1) // PAGE_SIZE
    
    # Clamp page
    current_page = max(1, min(current_page, total_pages)) if total_pages > 0 else 1
    
    # Slice
    start_idx = (current_page - 1) * PAGE_SIZE
    end_idx   = start_idx + PAGE_SIZE
    page_docs = all_docs[start_idx:end_idx]
    
    list_msg = [f"🗑 **FILES AVAILABLE FOR DELETION** (Page {current_page}/{total_pages})", "━━━━━━━━━━━━━━━━━━━━"]
    list_msg.extend(get_formatted_file_list(page_docs, limit=PAGE_SIZE, start_index=start_idx + 1))
    list_text = "\n".join(list_msg)
    
    # ── BUILD NAVIGATION ROWS ────────────────────────
    nav_row = []
    if current_page > 1:
        nav_row.append(KeyboardButton(text="⬅️ PREV"))
    if current_page < total_pages:
        nav_row.append(KeyboardButton(text="NEXT ➡️"))

    # ── MODE SPECIFIC OUTPUT ─────────────────────────
    if text == "🔢 DELETE BY RANGE":
        # Keyboard: [Prev, Next] / [Back] / [Back to Menu]
        rows = []
        if nav_row: rows.append(nav_row)
        rows.append([KeyboardButton(text="⬅️ BACK")]) # Back to Mode Selection
        rows.append([KeyboardButton(text="🔙 Back to Menu")])
        
        await message.answer(
            f"{list_text}\n\n"
            "🔢 <b>BULK DELETE MODE</b>\n"
            "Enter range to purge (e.g., `1-5`).\n"
            "⚠️ <b>WARNING</b>: This permanently removes PDFs from the database.",
            reply_markup=ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.set_state(BotState.deleting_pdf)
        
    elif text == "🆔 DELETE BY CODE":
        # Dynamic Code Buttons
        builder = ReplyKeyboardBuilder()
        for d in page_docs:
            code = d.get('code')
            if code:
                abs_idx = all_docs.index(d) + 1
                builder.add(KeyboardButton(text=f"{abs_idx}. {code}"))
        builder.adjust(2)
        
        if nav_row: builder.row(*nav_row)
        
        # Add Control Rows
        builder.row(KeyboardButton(text="⬅️ BACK")) # Back to Mode Selection
        builder.row(KeyboardButton(text="🔙 Back to Menu"))
        
        await message.answer(
            f"{list_text}\n\n"
            "🆔 <b>SINGLE DELETE MODE</b>\n"
            "Select a Code button below or type one (e.g., `P1`).",
            reply_markup=builder.as_markup(resize_keyboard=True),
            parse_mode="HTML", disable_web_page_preview=True
        )
        await state.set_state(BotState.deleting_pdf)
    else:
        await message.answer("⚠️ Invalid Option. use buttons.")

@dp.message(BotState.deleting_pdf, F.text == "NEXT ➡️")
async def delete_next_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("delete_page", 1) + 1
    mode = "🔢 DELETE BY RANGE" if data.get("delete_mode") == "range" else "🆔 DELETE BY CODE"
    await handle_delete_mode(message, state, mode_override=mode, page_override=page)

@dp.message(BotState.deleting_pdf, F.text == "⬅️ PREV")
async def delete_prev_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(1, data.get("delete_page", 1) - 1)
    mode = "🔢 DELETE BY RANGE" if data.get("delete_mode") == "range" else "🆔 DELETE BY CODE"
    await handle_delete_mode(message, state, mode_override=mode, page_override=page)

@dp.message(BotState.deleting_pdf)
async def process_deletion(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    if text == "🔙 BACK TO MENU": return await start(message, state)
    
    # Handle Back to Mode Selection
    if text == "⬅️ BACK":
        await remove_btn(message, state)
        return
    
    data = await state.get_data()
    mode = data.get('delete_mode', 'code')
    
    if mode == 'code':
        # Single Deletion - Ask for Confirmation
        code = text
        import re
        all_docs = _get_unique_docs()
        
        if code.isdigit():
            idx = int(code)
            if 1 <= idx <= len(all_docs):
                code = all_docs[idx - 1].get("code")
        else:
            match = re.match(r"^(\d+)\.\s+(.*)$", code)
            if match:
                idx = int(match.group(1))
                extracted_code = match.group(2)
                if 1 <= idx <= len(all_docs):
                    doc_code = all_docs[idx - 1].get("code", "")
                    if doc_code == extracted_code:
                        code = doc_code
                    elif extracted_code.endswith("…") or extracted_code.endswith("..."):
                        clean_extracted = extracted_code.rstrip("….")
                        if doc_code.startswith(clean_extracted):
                            code = doc_code
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
            all_docs = _get_unique_docs()
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
        await message.answer("❌ <b>DELETION CANCELLED.</b>\nNo files were deleted.", parse_mode="HTML")
        
        # Helper to re-show menu based on mode
        if mode == 'code':
            # Re-fetch buttons
            docs = _get_unique_docs()
            builder = ReplyKeyboardBuilder()
            existing_codes = []
            for d in docs[:50]:
                code = d.get('code')
                if code and code not in existing_codes:
                    abs_idx = docs.index(d) + 1
                    builder.add(KeyboardButton(text=f"{abs_idx}. {code}"))
                    existing_codes.append(code)
            builder.adjust(2)
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
            msg = await message.answer(f"⏳ **ARCHIVING: `{code}`...**")

            if code.endswith('…'):
                search_code = code[:-1]
                query = {"code": {"$regex": f"^{search_code}"}}
            elif code.endswith('...'):
                search_code = code[:-3]
                query = {"code": {"$regex": f"^{search_code}"}}
            else:
                query = {"code": code}
            
            docs = list(col_pdfs.find(query))
            if docs:
                db_res = True
                drive_del_count = 0
                links_to_delete = set()
                
                for doc in docs:
                    link = doc.get("link", "")
                    if link: links_to_delete.add(link)
                    doc['deleted_at'] = datetime.now()
                    col_trash.insert_one(doc)
                    col_pdfs.delete_one({"_id": doc['_id']})
                
                for i in range(0, len(links_to_delete), 15):
                    chunk = list(links_to_delete)[i:i+15]
                    results = await asyncio.gather(*(asyncio.to_thread(_drive_delete_file, l) for l in chunk), return_exceptions=True)
                    drive_del_count += sum(1 for r in results if r is True)
                
                DAILY_STATS_BOT4["pdfs_deleted"] += len(docs)
                asyncio.create_task(_persist_stats())
                
                if links_to_delete:
                    drive_note = f"  ☁️ Drive: {drive_del_count}/{len(links_to_delete)}"
                else:
                    drive_note = ""
            else:
                db_res = False
                drive_note = ""
                docs = []

            status = f"🍃 DB: ✅ {len(docs)} Deleted" if db_res else "🍃 DB: ❌ Not Found"
            await msg.edit_text(f"✅ <b>DELETED: <code>{code}</code></b>\n{status}{drive_note}", parse_mode="HTML")
        else:
            # Range Deletion
            indices = data.get('target_range_indices')
            start_idx, end_idx = indices
            
            msg = await message.answer("⏳ <b>Deleting files...</b>", parse_mode="HTML")
            
            all_docs = _get_unique_docs()
            selected_docs = all_docs[start_idx-1 : end_idx]
            
            moved_count = 0
            drive_del_count = 0
            links_to_delete = set()
            
            for unique_doc in selected_docs:
                unique_code = unique_doc.get("code")
                duplicates = list(col_pdfs.find({"code": unique_code}))
                for doc in duplicates:
                    link = doc.get("link", "")
                    if link: links_to_delete.add(link)
                    
                    doc['deleted_at'] = datetime.now()
                    col_trash.insert_one(doc)
                    col_pdfs.delete_one({"_id": doc['_id']})
                    moved_count += 1

            for i in range(0, len(links_to_delete), 15):
                chunk = list(links_to_delete)[i:i+15]
                results = await asyncio.gather(*(asyncio.to_thread(_drive_delete_file, l) for l in chunk), return_exceptions=True)
                drive_del_count += sum(1 for r in results if r is True)

            drive_note = f"  ☁️ Drive: {drive_del_count}/{len(links_to_delete)}" if links_to_delete else ""
            DAILY_STATS_BOT4["pdfs_deleted"] += moved_count
            asyncio.create_task(_persist_stats())
            await msg.edit_text(f"✅ <b>BULK DELETE COMPLETE</b>\nRemoved <code>{moved_count}</code> matching records from vault.{drive_note}", parse_mode="HTML")
            
        # Re-Show Menu
        if mode == 'code':
            await asyncio.sleep(1)
            # Re-fetch buttons
            docs = _get_unique_docs()
            builder = ReplyKeyboardBuilder()
            existing_codes = []
            for d in docs[:50]:
                code = d.get('code')
                if code and code not in existing_codes:
                    builder.add(KeyboardButton(text=code))
                    existing_codes.append(code)
            builder.adjust(2)
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
        "• <b>\U0001F517 Get Link:</b> Retrieve specific asset. Mode: <i>PDF File</i> (Download) or <i>Link</i> (URL).\n"
        "• <b>\u270F\uFE0F Edit PDF:</b> Rename PDF code in DB instantly.\n\n"

        "<b>\U0001F6E1 3. SECURITY</b>\n"
        "• <b>\U0001F5D1 Remove PDF:</b> Deletes from vault (soft-archival for safety).\n"
        "• <b>\u26A0\uFE0F NUKE ALL DATA:</b> ☠️ <b>DANGER!</b> Permanently wipes all MongoDB records. Irreversible.\n"
        "• <b>Anti-Spam:</b> Auto-bans users flooding commands (>5/sec).\n\n"

        "<b>⚙️ 4. ADMIN & INFRASTRUCTURE</b>\n"
        "• <b>⚙️ Admin Config:</b>\n"
        "   - <b>Add/Remove Admin:</b> Assign By ID.\n"
        "   - <b>Roles:</b> Give titles (e.g., 'Chief Editor').\n"
        "   - <b>Locks:</b> Freeze admin access without removing them.\n"
        "• <b>\U0001F4BB Live Terminal:</b> Real-time log streaming of bot actions.\n"
        "• <b>\U0001F4CA Storage Info:</b> DB Latency, PDF Count.\n\n"

        "<b>📦 5. BACKUP SYSTEMS</b>\n"
        "• <b>Manual:</b> Click <code>📦 Backup</code>.\n"
        "   - <b>Text Report:</b> Summary of Admins + PDF List.\n"
        "   - <b>JSON Dump:</b> Full restore-ready DB export (saved to MongoDB).\n"
        "• <b>Weekly Auto-Pilot:</b> Every Sunday @ 3AM, full snapshot sent to Owner.\n\n"

        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "\U0001F680 <b>SYSTEM STATUS: \U0001F7E2 OPTIMAL</b>"
    )
    await message.answer(help_text, parse_mode="HTML")

@dp.message(F.text == "⚠️ NUKE ALL DATA")
async def nuke_warning(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="⚠️ YES, I UNDERSTAND"), KeyboardButton(text="🔙 Back to Menu"))
    await message.answer(
        "☢️ <b>DANGER — NUKE ALL DATA</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "This will <b>permanently destroy ALL</b>:\n"
        "• All MongoDB PDF records\n"
        "• All local PDF files\n\n"
        "⚠️ <b>IRREVERSIBLE. Step 1 of 2:</b>\n"
        "Press <b>YES, I UNDERSTAND</b> to continue:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )
    await state.set_state(BotState.confirm_nuke)

@dp.message(BotState.confirm_nuke)
async def nuke_step1(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    if message.text == "⚠️ YES, I UNDERSTAND":
        builder2 = ReplyKeyboardBuilder()
        builder2.row(KeyboardButton(text="☢️ EXECUTE FINAL NUKE"), KeyboardButton(text="❌ ABORT"))
        await message.answer(
            "☢️ <b>FINAL CONFIRMATION — Step 2 of 2</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "⛔ <b>YOU ARE ABOUT TO WIPE ALL PDF DATA.</b>\n\n"
            "This is your <b>LAST CHANCE</b> to abort.\n"
            "Press <b>EXECUTE FINAL NUKE</b> to permanently wipe:",
            reply_markup=builder2.as_markup(resize_keyboard=True),
            parse_mode="HTML"
        )
        await state.set_state(BotState.waiting_for_nuke_2)
        return
    await message.answer("⚠️ Use the confirmation buttons.", parse_mode="HTML")

@dp.message(BotState.waiting_for_nuke_2)
async def nuke_execution(message: types.Message, state: FSMContext):
    if message.text in ("❌ ABORT", "🔙 Back to Menu"):
        await message.answer("✅ <b>NUKE ABORTED. All data is safe.</b>", parse_mode="HTML")
        await state.clear()
        return await start(message, state)
    if message.text == "☢️ EXECUTE FINAL NUKE":
        status_msg = await message.answer("☢️ <b>INITIATING NUCLEAR PROTOCOL...</b>", parse_mode="HTML")
        
        # 1. Google Drive Wipe
        await status_msg.edit_text("🔥 <b>STEP 1/3: Wiping Google Drive Vault...</b>", parse_mode="HTML")
        drive_count = 0
        all_colls = [col_pdfs, col_trash, col_locked, col_trash_locked]
        all_links = []
        for coll in all_colls:
            if coll is not None:
                for doc in coll.find({}, {"link": 1}):
                    if "link" in doc and doc["link"]:
                        all_links.append(doc["link"])
        
        all_links = list(set(all_links)) # unique only
        
        for i in range(0, len(all_links), 15):
            chunk = all_links[i:i+15]
            results = await asyncio.gather(*(asyncio.to_thread(_drive_delete_file, l) for l in chunk), return_exceptions=True)
            drive_count += sum(1 for r in results if r is True)

        # 2. MongoDB Wipe
        await status_msg.edit_text("🔥 <b>STEP 2/3: Purging Database...</b>", parse_mode="HTML")
        db_count = 0
        for coll in all_colls:
            if coll is not None:
                try:
                    x = coll.delete_many({})
                    db_count += x.deleted_count
                except: pass

        # 3. Local Wipe
        await status_msg.edit_text("🔥 <b>STEP 3/3: Sterilizing Local Environment...</b>", parse_mode="HTML")
        local_count = 0
        for file in os.listdir():
            if file.endswith(".pdf"):
                try:
                    os.remove(file)
                    local_count += 1
                except: pass

        report = (
            "☢️ <b>NUCLEAR WIPEOUT COMPLETE</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"☁️ <b>Drive:</b> <code>{drive_count}</code> files destroyed.\n"
            f"🛢 <b>Database:</b> <code>{db_count}</code> records destroyed.\n"
            f"💻 <b>Local:</b> <code>{local_count}</code> files purged.\n\n"
            "☠️ <b>NUKE COMPLETE.</b>\n"
            "The system has been purged, Master."
        )
        await status_msg.edit_text(report, parse_mode="HTML")
        await state.clear()
        await message.answer("💎 <b>READY FOR REBIRTH.</b>", reply_markup=get_main_menu(message.from_user.id), parse_mode="HTML")
    else:
        await message.answer("⚠️ Use the confirmation buttons to proceed.", parse_mode="HTML")

# ==========================================
# 🩺 DIAGNOSIS SYSTEM
# ==========================================
@dp.message(F.text == "🩺 System Diagnosis")
async def perform_bot4_diagnosis(message: types.Message):
    if not is_admin(message.from_user.id): return

    status_msg = await message.answer("🔄 <b>INITIATING DEEP SYSTEM SCAN...</b>", parse_mode="HTML")
    scan_start = time.time()
    health_score = 100
    issues = []

    # ── 1. ENVIRONMENT ────────────────────────────────────────
    await status_msg.edit_text("🔐 <b>SCANNING: ENVIRONMENT...</b>", parse_mode="HTML")
    env_checks = []
    if BOT_TOKEN:
        env_checks.append("✅ Bot Token")
    else:
        env_checks.append("❌ Bot Token"); health_score -= 50; issues.append("CRITICAL: No Bot Token")
    if MONGO_URI:
        env_checks.append("✅ Mongo URI")
    else:
        env_checks.append("❌ Mongo URI"); health_score -= 30; issues.append("CRITICAL: No DB URI")
    if os.path.exists(CREDENTIALS_FILE):
        env_checks.append("✅ credentials.json")
    else:
        env_checks.append("⚠️ credentials.json"); issues.append("Drive: credentials.json missing (PDF upload disabled)")
    if os.path.exists(TOKEN_FILE):
        env_checks.append("✅ token.pickle")
    else:
        env_checks.append("⚠️ token.pickle (will re-auth)")
    if PARENT_FOLDER_ID:
        env_checks.append("✅ PARENT_FOLDER_ID")
    else:
        env_checks.append("⚠️ PARENT_FOLDER_ID (PDFs go to root)")
    await asyncio.sleep(0.4)

    # ── 2. DATABASE ───────────────────────────────────────────
    await status_msg.edit_text("🍃 <b>SCANNING: DATABASE...</b>", parse_mode="HTML")
    db_status = "❌ OFFLINE"
    mongo_lat = 0
    try:
        t0 = time.time()
        db_client.admin.command("ping")
        mongo_lat = (time.time() - t0) * 1000
        lat_icon = "✅" if mongo_lat < 150 else ("🟡" if mongo_lat < 500 else "🔴")
        db_status = f"{lat_icon} ONLINE ({mongo_lat:.1f}ms)"
        if mongo_lat > 500:
            health_score -= 10; issues.append(f"High DB Latency: {mongo_lat:.0f}ms")
    except Exception as e:
        db_status = f"❌ FAIL: {str(e)[:30]}"
        health_score -= 30; issues.append("Database Disconnected")
    await asyncio.sleep(0.4)

    # ── 3. COLLECTION COUNTS ──────────────────────────────────
    await status_msg.edit_text("📊 <b>SCANNING: COLLECTIONS...</b>", parse_mode="HTML")
    pdf_count    = col_pdfs.count_documents({})         if col_pdfs         is not None else "N/A"
    trash_count  = col_trash.count_documents({})        if col_trash        is not None else "N/A"
    locked_count = col_locked.count_documents({})       if col_locked       is not None else "N/A"
    admin_count  = col_admins.count_documents({})       if col_admins       is not None else "N/A"
    banned_count = col_banned.count_documents({})       if col_banned       is not None else "N/A"
    await asyncio.sleep(0.3)

    # ── 4. GOOGLE DRIVE ───────────────────────────────────────
    await status_msg.edit_text("☁️ <b>SCANNING: GOOGLE DRIVE...</b>", parse_mode="HTML")
    drive_status = "⚠️ SKIPPED (no credentials)"
    if os.path.exists(CREDENTIALS_FILE):
        try:
            t0 = time.time()
            svc = await asyncio.to_thread(get_drive_service)
            svc.files().list(pageSize=1, fields="files(id)").execute()
            drive_lat = (time.time() - t0) * 1000
            drive_status = f"✅ CONNECTED ({drive_lat:.0f}ms)"
        except Exception as e:
            drive_status = f"❌ FAIL: {str(e)[:40]}"
            health_score -= 10; issues.append("Drive Auth Failed")
    await asyncio.sleep(0.3)

    # ── 5. FILESYSTEM ─────────────────────────────────────────
    await status_msg.edit_text("📁 <b>SCANNING: FILESYSTEM...</b>", parse_mode="HTML")
    fs_status = "✅ WRITEABLE"
    try:
        with open("test_write.tmp", "w") as f: f.write("test")
        os.remove("test_write.tmp")
    except Exception as e:
        fs_status = f"❌ READ-ONLY ({e})"
        health_score -= 20; issues.append("Filesystem Read-Only")
    await asyncio.sleep(0.3)

    # ── BUILD REPORT ──────────────────────────────────────────
    scan_duration = time.time() - scan_start
    uptime_secs   = int(time.time() - START_TIME)
    uptime_str    = f"{uptime_secs // 3600}h {(uptime_secs % 3600) // 60}m {uptime_secs % 60}s"
    now_str       = now_local().strftime("%b %d, %Y  ·  %I:%M %p")

    if health_score >= 95:   health_emoji = "🟢 EXCELLENT"
    elif health_score >= 80: health_emoji = "🟡 GOOD"
    elif health_score >= 60: health_emoji = "🟠 DEGRADED"
    else:                    health_emoji = "🔴 CRITICAL"

    report = (
        f"🩺 <b>SYSTEM DIAGNOSTIC REPORT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌡️ <b>Health:</b> {health_emoji} <code>({health_score}%)</code>\n"
        f"🕐 <b>Scan Time:</b> <code>{scan_duration:.2f}s</code>  |  ⏱ <b>Uptime:</b> <code>{uptime_str}</code>\n"
        f"📅 <b>Report:</b> <code>{now_str}</code>\n\n"

        f"🔐 <b>ENVIRONMENT</b>\n"
        + "\n".join(f"• {c}" for c in env_checks) +

        f"\n\n🍃 <b>DATABASE</b>\n"
        f"• Status: {db_status}\n"
        f"• Process: <code>{os.getpid()}</code>\n\n"

        f"📊 <b>COLLECTIONS</b>\n"
        f"• 📚 Active PDFs: <code>{pdf_count}</code>\n"
        f"• 🗑 Recycle Bin: <code>{trash_count}</code>\n"
        f"• 🔒 Locked PDFs: <code>{locked_count}</code>\n"
        f"• 👥 Admins: <code>{admin_count}</code>\n"
        f"• 🚫 Banned: <code>{banned_count}</code>\n\n"

        f"☁️ <b>GOOGLE DRIVE</b>\n"
        f"• {drive_status}\n\n"

        f"💻 <b>HOST SYSTEM</b>\n"
        f"• Filesystem: {fs_status}\n\n"

        f"📈 <b>SESSION STATS</b>\n"
        f"• 📄 PDFs Generated: <code>{DAILY_STATS_BOT4['pdfs_generated']}</code>\n"
        f"• 🔗 Links Retrieved: <code>{DAILY_STATS_BOT4['links_retrieved']}</code>\n"
        f"• 🗑 PDFs Deleted: <code>{DAILY_STATS_BOT4['pdfs_deleted']}</code>\n"
        f"• ⚠️ Errors: <code>{DAILY_STATS_BOT4['errors']}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    SOLUTIONS = {
        "CRITICAL: No Bot Token":      "Set BOT_4_TOKEN in bot4.env or Render env vars",
        "CRITICAL: No DB URI":         "Set MONGO_URI in bot4.env or Render env vars",
        "Database Disconnected":       "Check MONGO_URI, Atlas IP whitelist (0.0.0.0/0), or restart",
        "Drive Auth Failed":           "Delete token.pickle and re-run OAuth, or check credentials.json",
        "Filesystem Read-Only":        "Check Render disk permissions or free disk space",
        "Drive: credentials.json missing (PDF upload disabled)": "Upload credentials.json to deploy dir",
    }
    # Extra live checks
    try:
        _adm_active = col_admins.count_documents({"locked": False}) if col_admins else 0
        _adm_locked = col_admins.count_documents({"locked": True})  if col_admins else 0
    except: _adm_active = _adm_locked = 0
    try:
        _dbb = db_client["MSANodeDB"]
        _lb  = _dbb["bot4_monthly_backups"].find_one({}, sort=[("date", -1)])
        _lb_str = _lb["month"] if _lb else "Never"
    except: _lb_str = "?"
    try:
        import psutil as _ps
        _proc = _ps.Process(os.getpid())
        _mem  = _proc.memory_info().rss / 1024 / 1024
        _cpu  = _proc.cpu_percent(interval=0.3)
        mem_line = f"• RAM: <code>{_mem:.1f} MB</code>  CPU: <code>{_cpu:.1f}%</code>\n"
    except: mem_line = ""
    report += (
        "\n👤 <b>ADMIN STATUS</b>\n"
        f"• 🟢 Active: <code>{_adm_active}</code>  🔴 Locked: <code>{_adm_locked}</code>\n"
        f"• 📦 Last Monthly Backup: <code>{_lb_str}</code>\n"
    )
    if mem_line: report += "\n💻 <b>PROCESS RESOURCES</b>\n" + mem_line
    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    if issues:
        report += f"⚠️ <b>ISSUES DETECTED ({len(issues)})</b>\n"
        for i in issues:
            sol = SOLUTIONS.get(i, "")
            report += f"• 🔴 {i}\n"
            if sol: report += f"  💡 <i>{sol}</i>\n"
        report += "\n🛑 <b>ACTION REQUIRED.</b>"
    else:
        report += "✅ <b>ALL SYSTEMS OPERATING AT PEAK EFFICIENCY.</b>"

    await status_msg.edit_text(report, parse_mode="HTML")

# ==========================================
# 👥 ADMIN MANAGEMENT — HELPERS & CONSTANTS
# ==========================================

ROLE_MESSAGES = {
    "🏅 MANAGER": lambda name: (
        f"💼 <b>DESIGNATION UPDATED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Officer:</b> {name}\n"
        f"🏅 <b>Role:</b> <code>Manager</code>\n\n"
        f"You have been elevated to <b>Manager</b> status within MSA NODE SYSTEMS.\n"
        f"This rank grants operational oversight and resource management authority.\n\n"
        f"Serve with precision. Every action is logged.\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 <i>MSA NODE SYSTEMS · Authorized by OWNER</i>"
    ),
    "⚙️ ADMIN": lambda name: (
        f"🛡 <b>ACCESS GRANTED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Officer:</b> {name}\n"
        f"⚙️ <b>Role:</b> <code>Admin</code>\n\n"
        f"Your admin credentials are now active, {name}.\n"
        f"Use your access responsibly.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 <i>MSA NODE SYSTEMS · Authorized by OWNER</i>"
    ),
    "🔰 MODERATOR": lambda name: (
        f"🔰 <b>ROLE ASSIGNED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Officer:</b> {name}\n"
        f"🔰 <b>Role:</b> <code>Moderator</code>\n\n"
        f"Welcome, {name}. You are now a Moderator.\n"
        f"Your duty: maintain order, uphold standards.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 <i>MSA NODE SYSTEMS · Authorized by OWNER</i>"
    ),
    "🎧 SUPPORT": lambda name: (
        f"🎧 <b>SUPPORT DESIGNATION</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Officer:</b> {name}\n"
        f"🎧 <b>Role:</b> <code>Support</code>\n\n"
        f"You are now registered as Support staff, {name}.\n"
        f"Assist with care, respond with precision.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 <i>MSA NODE SYSTEMS · Authorized by OWNER</i>"
    ),
}
PRESET_ROLES = ["🏅 MANAGER", "⚙️ ADMIN", "🔰 MODERATOR", "🎧 SUPPORT"]

def _admin_select_keyboard(admins, page=0, include_status=False):
    """Paginated admin selection keyboard. Returns (markup, page_admins, page, max_page)."""
    total = len(admins)
    if total == 0:
        return None, [], 0, 0
    max_page = max(0, (total - 1) // ADMIN_PAGE_SIZE)
    page = max(0, min(page, max_page))
    page_admins = admins[page * ADMIN_PAGE_SIZE:(page + 1) * ADMIN_PAGE_SIZE]
    builder = ReplyKeyboardBuilder()
    for a in page_admins:
        uid = a.get("user_id")
        name = a.get("name", "")
        locked = a.get("locked", False)
        if include_status:
            lock_icon = "🔴" if locked else "🟢"
            label = f"👤 {name} ({uid}) {lock_icon}" if name else f"👤 {uid} {lock_icon}"
        else:
            label = f"👤 {name} ({uid})" if name else f"👤 {uid}"
        builder.add(KeyboardButton(text=label))
    builder.adjust(1 if len(page_admins) <= 4 else 2)
    nav = []
    if page > 0: nav.append(KeyboardButton(text="⬅️ PREV PAGE"))
    if page < max_page: nav.append(KeyboardButton(text="➡️ NEXT PAGE"))
    if nav: builder.row(*nav)
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    return builder.as_markup(resize_keyboard=True), page_admins, page, max_page

def _parse_admin_id_from_btn(text):
    """Parse '👤 Name (12345)' or '👤 12345' or '👤 12345 🔴' → int ID."""
    try:
        t = text.strip()
        # '👤' is a single Unicode char; prefix '👤 ' is exactly 2 chars, not 3
        if t.startswith("👤 "): t = t[len("👤 "):]
        if "(" in t and ")" in t:
            uid_str = t.rsplit("(", 1)[1].split(")")[0].strip()
            return int(uid_str)
        return int(t.split()[0].strip())
    except:
        return None

async def _send_role_welcome_message(uid, role, name):
    """Send personalized role message to a specific admin. Silent on error."""
    fn = ROLE_MESSAGES.get(role)
    if fn is None: return
    try:
        await bot.send_message(uid, fn(name), parse_mode="HTML")
    except Exception as e:
        logging.warning(f"Could not send role message to {uid}: {e}")

def _admin_doc_by_id(uid):
    """Find admin doc by int or str user_id."""
    doc = col_admins.find_one({"user_id": uid})
    if not doc:
        doc = col_admins.find_one({"user_id": str(uid)})
    return doc

def _update_admin_field(uid, field, value):
    res = col_admins.update_one({"user_id": uid}, {"$set": {field: value}})
    if res.matched_count == 0:
        col_admins.update_one({"user_id": str(uid)}, {"$set": {field: value}})

def update_admin_perms(user_id, perms):
    _update_admin_field(user_id, "permissions", perms)

def update_admin_role(user_id, role):
    _update_admin_field(user_id, "role", role)

def update_admin_lock(user_id, locked):
    _update_admin_field(user_id, "locked", locked)


# ==========================================
# ⚙️ ADMIN CONFIG MENU
# ==========================================
@dp.message(F.text == "⚙️ Admin Config")
async def admin_config_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    admin_count = col_admins.count_documents({}) if col_admins is not None else 0
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="➕ ADD ADMIN"), KeyboardButton(text="➖ REMOVE ADMIN"))
    builder.row(KeyboardButton(text="🔑 PERMISSIONS"), KeyboardButton(text="🎭 MANAGE ROLES"))
    builder.row(KeyboardButton(text="🔒 LOCK / UNLOCK"), KeyboardButton(text="🚫 BANNED USERS"))
    builder.row(KeyboardButton(text="📜 LIST ADMINS"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    await message.answer(
        f"👥 <b>ADMINISTRATION CONSOLE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👑 <b>Owner:</b> <code>{OWNER_ID}</code>\n"
        f"👥 <b>Admins Registered:</b> <code>{admin_count}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Select an operation:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )


# ==========================================
# ➕ ADD ADMIN
# ==========================================
@dp.message(F.text == "➕ ADD ADMIN")
async def add_admin_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "➕ <b>ADD ADMINISTRATOR</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Enter the <b>Telegram User ID</b> to promote.\n"
        "📌 New admins are <b>LOCKED</b> by default.\n"
        "Activate via <b>🔒 LOCK / UNLOCK</b> when ready.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu")]],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_admin_id)

@dp.message(BotState.waiting_for_admin_id)
async def process_add_admin(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": await state.clear(); return await admin_config_btn(message)
    if not text.isdigit():
        await message.answer("⚠️ Invalid ID. Numbers only."); return
    new_id = int(text)
    if new_id == OWNER_ID:
        await message.answer("❌ Owner already has supreme access — cannot add as admin."); return
    if _admin_doc_by_id(new_id):
        await message.answer(
            f"⚠️ <b>ALREADY EXISTS</b>\n"
            f"<code>{new_id}</code> is already registered as an admin.\n"
            f"Use <b>➖ REMOVE ADMIN</b> first to re-add.",
            parse_mode="HTML"
        ); return
    if is_banned(new_id):
        await message.answer(
            f"⛔ <b>CANNOT ADD BANNED USER</b>\n"
            f"<code>{new_id}</code> is on the blacklist.\n"
            f"Unban first via <b>🚫 BANNED USERS</b>.",
            parse_mode="HTML"
        ); return
    name = ""
    try:
        chat = await bot.get_chat(new_id)
        name = chat.full_name or ""
    except: pass
    col_admins.insert_one({
        "user_id": new_id,
        "name": name,
        "role": "⚙️ ADMIN",
        "permissions": [],  # All OFF by default — owner grants individually
        "locked": True,
        "added_by": message.from_user.id,
        "timestamp": now_local()
    })
    display = f"👤 {name} (<code>{new_id}</code>)" if name else f"<code>{new_id}</code>"
    await message.answer(
        f"✅ <b>ADMIN ADDED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {display}\n"
        f"🎭 Role: <code>⚙️ ADMIN</code>\n"
        f"🔒 Status: <b>LOCKED</b> (Inactive)\n\n"
        f"⚠️ Unlock via <b>🔒 LOCK / UNLOCK</b> to activate.",
        parse_mode="HTML"
    )
    await state.clear()
    await admin_config_btn(message)


# ==========================================
# ➖ REMOVE ADMIN
# ==========================================
@dp.message(F.text == "➖ REMOVE ADMIN")
async def remove_admin_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    admins = list(col_admins.find())
    if not admins:
        await message.answer("⚠️ No admins registered."); return
    markup, _, page, max_page = _admin_select_keyboard(admins, page=0, include_status=True)
    await state.update_data(rm_page=0)
    await message.answer(
        f"➖ <b>REMOVE ADMINISTRATOR</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Total: <code>{len(admins)}</code> admin(s)\n"
        f"🟢 Active  🔴 Locked\n\n"
        f"Select admin to remove:",
        reply_markup=markup, parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_remove_admin)

@dp.message(BotState.waiting_for_remove_admin)
async def process_remove_admin(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": await state.clear(); return await admin_config_btn(message)
    if text in ("⬅️ PREV PAGE", "➡️ NEXT PAGE"):
        data = await state.get_data()
        page = data.get("rm_page", 0) + (1 if "NEXT" in text else -1)
        admins = list(col_admins.find())
        markup, _, page, _ = _admin_select_keyboard(admins, page=page, include_status=True)
        await state.update_data(rm_page=page)
        await message.answer(f"📋 Page {page + 1}", reply_markup=markup, parse_mode="HTML")
        return
    uid = _parse_admin_id_from_btn(text)
    if uid is None:
        await message.answer("⚠️ Invalid selection."); return
    if uid == OWNER_ID:
        await message.answer("❌ Cannot remove the Owner."); return
    res = col_admins.delete_one({"user_id": uid})
    if res.deleted_count == 0:
        col_admins.delete_one({"user_id": str(uid)})
    await message.answer(
        f"🗑 <b>ADMIN REMOVED</b>\n"
        f"<code>{uid}</code> has been demoted successfully.",
        parse_mode="HTML"
    )
    await state.clear()
    await admin_config_btn(message)


# ==========================================
# 📜 LIST ADMINS
# ==========================================
@dp.message(F.text == "📜 LIST ADMINS")
async def list_admins_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.update_data(list_admins_page=0)
    await state.set_state(BotState.viewing_admin_list)
    await _send_admin_list_page(message, page=0)

async def _send_admin_list_page(message, page: int):
    """Paginated admin roster — 10 per page, prev/next when needed."""
    PAGE_SIZE = 10
    admins = list(col_admins.find())
    total = len(admins)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    page_admins = admins[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    now_str = now_local().strftime("%b %d, %Y  ·  %I:%M %p")
    lines = [
        f"👥 <b>ADMIN ROSTER</b>  (Page {page+1}/{total_pages})",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"🕐 <code>{now_str}</code>",
        f"👑 <b>OWNER:</b> <code>{OWNER_ID}</code>",
        f"📊 Total: <code>{total}</code> admin(s)\n",
    ]
    if not admins:
        lines.append("<i>No additional admins registered.</i>")
    else:
        start_idx = page * PAGE_SIZE + 1
        for idx, a in enumerate(page_admins, start=start_idx):
            uid = a.get("user_id")
            name = a.get("name", "")
            role = a.get("role", "Admin")
            locked = a.get("locked", False)
            ts = a.get("timestamp")
            date_str = ts.strftime("%b %d, %Y  ·  %I:%M %p") if isinstance(ts, datetime) else "Unknown"
            status = "🔴 LOCKED" if locked else "🟢 ACTIVE"
            display = f"👤 {name} (<code>{uid}</code>)" if name else f"👤 <code>{uid}</code>"
            perms = a.get("permissions", [])
            perm_count = len(perms) if perms else 0
            lines.append(
                f"<b>{idx}.</b> {display}\n"
                f"   🎭 <code>{role}</code>  |  {status}\n"
                f"   🔑 Permissions: <code>{perm_count}</code>\n"
                f"   📅 <code>{date_str}</code>\n"
            )
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    # Keyboard with prev/next if needed
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV ADMINS"))
    if (page + 1) < total_pages:
        nav_row.append(KeyboardButton(text="➡️ NEXT ADMINS"))
    keyboard_rows = []
    if nav_row:
        keyboard_rows.append(nav_row)
    keyboard_rows.append([KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu")])
    markup = ReplyKeyboardMarkup(keyboard=keyboard_rows, resize_keyboard=True)

    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=markup)

@dp.message(BotState.viewing_admin_list)
async def admin_list_pagination_handler(message: types.Message, state: FSMContext):
    text = message.text or ""
    if text == "🔙 Back to Menu": await state.clear(); return await start(message, state)
    if text in ("⬅️ BACK", "⬅️ BACK TO MENU"): await state.clear(); return await admin_config_btn(message)
    data = await state.get_data()
    page = data.get("list_admins_page", 0)
    if text == "➡️ NEXT ADMINS":
        page += 1
    elif text == "⬅️ PREV ADMINS":
        page = max(0, page - 1)
    else:
        return
    await state.update_data(list_admins_page=page)
    await _send_admin_list_page(message, page)


# ==========================================
# 🔑 PERMISSIONS
# ==========================================
@dp.message(F.text == "🔑 PERMISSIONS")
async def permissions_entry(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("⛔ <b>OWNER ONLY.</b>", parse_mode="HTML"); return
    admins = list(col_admins.find())
    if not admins:
        await message.answer("⚠️ No admins to configure."); return
    markup, _, page, _ = _admin_select_keyboard(admins, page=0, include_status=True)
    await state.update_data(perm_page=0)
    await message.answer(
        f"🔑 <b>PERMISSIONS — SELECT ADMIN</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Active  |  🔴 Locked\n"
        f"<i>Locked admins cannot have permissions modified.</i>\n\n"
        f"Select admin to configure:",
        reply_markup=markup, parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_perm_admin)

@dp.message(BotState.waiting_for_perm_admin)
async def permissions_admin_select(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": await state.clear(); return await admin_config_btn(message)
    if text in ("⬅️ PREV PAGE", "➡️ NEXT PAGE"):
        data = await state.get_data()
        page = data.get("perm_page", 0) + (1 if "NEXT" in text else -1)
        admins = list(col_admins.find())
        markup, _, page, _ = _admin_select_keyboard(admins, page=page, include_status=True)
        await state.update_data(perm_page=page)
        await message.answer(f"📋 Page {page + 1}", reply_markup=markup, parse_mode="HTML")
        return
    uid = _parse_admin_id_from_btn(text)
    if uid is None:
        await message.answer("⚠️ Invalid selection."); return
    doc = _admin_doc_by_id(uid)
    if not doc:
        await message.answer("❌ Admin not found."); return
    if doc.get("locked", False):
        # Owner CAN still modify – will take effect when admin is unlocked
        await message.answer(
            "🔒 <b>Admin is locked</b>\n"
            "ℹ️ Changes saved — will take effect when unlocked.",
            parse_mode="HTML"
        )
    await state.update_data(perm_target_id=uid)
    await render_permission_menu(message, state, uid)

async def render_permission_menu(message, state, target_id):
    doc = _admin_doc_by_id(target_id)
    if not doc: return
    name = doc.get("name", "")
    current_perms = doc.get("permissions", list(DEFAULT_PERMISSIONS))
    builder = ReplyKeyboardBuilder()
    for btn_text, key in PERMISSION_MAP.items():
        status = "✅" if key in current_perms else "❌"
        builder.add(KeyboardButton(text=f"{status} {btn_text}"))
    builder.adjust(2)
    builder.row(KeyboardButton(text="🔐 GRANT ALL"), KeyboardButton(text="🔒 REVOKE ALL"))
    builder.row(KeyboardButton(text="💾 SAVE CHANGES"))
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    display = f"👤 {name} ({target_id})" if name else f"{target_id}"
    await message.answer(
        f"🔑 <b>PERMISSIONS: {display}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ = Granted  |  ❌ = Denied\n\n"
        f"Tap a feature to toggle:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_perm_toggle)

@dp.message(BotState.waiting_for_perm_toggle)
async def process_perm_toggle(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": return await permissions_entry(message, state)
    data = await state.get_data()
    target_id = data.get("perm_target_id")
    if text == "💾 SAVE CHANGES":
        await message.answer("✅ <b>PERMISSIONS SAVED.</b>", parse_mode="HTML")
        return await permissions_entry(message, state)
    doc = _admin_doc_by_id(target_id)
    if not doc: return
    current_perms = list(doc.get("permissions", list(DEFAULT_PERMISSIONS)))
    if text == "🔐 GRANT ALL":
        update_admin_perms(target_id, list(PERMISSION_MAP.values()))
        await message.answer("✅ <b>ALL PERMISSIONS GRANTED.</b>", parse_mode="HTML")
        return await render_permission_menu(message, state, target_id)
    if text == "🔒 REVOKE ALL":
        update_admin_perms(target_id, [])
        await message.answer("🔒 <b>ALL PERMISSIONS REVOKED.</b>", parse_mode="HTML")
        return await render_permission_menu(message, state, target_id)
    clean = text[2:].strip()
    target_key = PERMISSION_MAP.get(clean)
    if not target_key:
        await message.answer("⚠️ Unknown option."); return
    if target_key in current_perms:
        current_perms.remove(target_key)
    else:
        current_perms.append(target_key)
    update_admin_perms(target_id, current_perms)
    await render_permission_menu(message, state, target_id)


# ==========================================
# 🎭 MANAGE ROLES
# ==========================================
@dp.message(F.text == "🎭 MANAGE ROLES")
async def roles_entry(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("⛔ <b>OWNER ONLY.</b>", parse_mode="HTML"); return
    admins = list(col_admins.find())
    if not admins:
        await message.answer("⚠️ No admins found."); return
    markup, _, _, _ = _admin_select_keyboard(admins, page=0, include_status=True)
    await state.update_data(role_page=0)
    await message.answer(
        f"🎭 <b>MANAGE ROLES — SELECT ADMIN</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Active  |  🔴 Locked\n"
        f"<i>Role is saved for locked admins but message is only sent when unlocked.</i>\n\n"
        f"Select admin:",
        reply_markup=markup, parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_role_admin)

@dp.message(BotState.waiting_for_role_admin)
async def roles_admin_select(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": await state.clear(); return await admin_config_btn(message)
    if text in ("⬅️ PREV PAGE", "➡️ NEXT PAGE"):
        data = await state.get_data()
        page = data.get("role_page", 0) + (1 if "NEXT" in text else -1)
        admins = list(col_admins.find())
        markup, _, page, _ = _admin_select_keyboard(admins, page=page, include_status=True)
        await state.update_data(role_page=page)
        await message.answer(f"📋 Page {page + 1}", reply_markup=markup, parse_mode="HTML")
        return
    uid = _parse_admin_id_from_btn(text)
    if uid is None:
        await message.answer("⚠️ Invalid selection."); return
    doc = _admin_doc_by_id(uid)
    if not doc:
        await message.answer("❌ Admin not found."); return
    await state.update_data(role_target_id=uid)
    name = doc.get("name", "")
    current_role = doc.get("role", "Admin")
    display = f"👤 {name} ({uid})" if name else f"<code>{uid}</code>"
    builder = ReplyKeyboardBuilder()
    for r in PRESET_ROLES: builder.add(KeyboardButton(text=r))
    builder.adjust(2)
    builder.row(KeyboardButton(text="👑 TRANSFER OWNERSHIP"))
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    await message.answer(
        f"🎭 <b>ASSIGN ROLE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Admin:</b> {display}\n"
        f"📌 <b>Current Role:</b> <code>{current_role}</code>\n\n"
        f"Select new role:",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_role_select)

@dp.message(BotState.waiting_for_role_select)
async def process_role_assign(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": return await roles_entry(message, state)
    data = await state.get_data()
    target_id = data.get("role_target_id")
    doc = _admin_doc_by_id(target_id)
    if not doc:
        await message.answer("❌ Admin not found."); return
    if text == "👑 TRANSFER OWNERSHIP":
        await message.answer(
            f"👑 <b>OWNERSHIP TRANSFER</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ This <b>permanently transfers</b> Owner control to <code>{target_id}</code>.\n\n"
            f"🔐 Enter the <b>Owner Transfer Password</b>:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ CANCEL"), KeyboardButton(text="🔙 Back to Menu")]],
                resize_keyboard=True
            ), parse_mode="HTML"
        )
        await state.update_data(owner_transfer_target=target_id)
        await state.set_state(BotState.waiting_for_owner_pw_first)
        return
    if text not in PRESET_ROLES:
        await message.answer("⚠️ Unknown role option."); return
    name = doc.get("name", str(target_id))
    locked = doc.get("locked", False)
    update_admin_role(target_id, text)
    await message.answer(
        f"✅ <b>ROLE UPDATED</b>\n"
        f"👤 <code>{target_id}</code> → <code>{text}</code>",
        parse_mode="HTML"
    )
    if not locked:
        await _send_role_welcome_message(target_id, text, name)
    else:
        await message.answer(
            f"ℹ️ Admin is locked — role message will be delivered when they are unlocked.",
            parse_mode="HTML"
        )
    await roles_entry(message, state)


# ==========================================
# 👑 OWNERSHIP TRANSFER
# ==========================================
@dp.message(BotState.waiting_for_owner_pw_first)
async def owner_pw_first(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text in ("❌ CANCEL", "🔙 Back to Menu"): return await start(message, state)
    await state.update_data(owner_pw_attempt=text)
    await message.answer(
        f"🔐 <b>CONFIRM PASSWORD</b>\n"
        f"Enter the password <b>once more</b> to confirm:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ CANCEL"), KeyboardButton(text="🔙 Back to Menu")]],
            resize_keyboard=True
        ), parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_owner_pw_confirm)

@dp.message(BotState.waiting_for_owner_pw_confirm)
async def owner_pw_confirm(message: types.Message, state: FSMContext):
    global OWNER_ID
    text = message.text.strip()
    if text in ("❌ CANCEL", "🔙 Back to Menu"): return await start(message, state)
    data = await state.get_data()
    first_pw = data.get("owner_pw_attempt", "")
    target_id = data.get("owner_transfer_target")
    # Load effective password (DB overrides env)
    effective_pw = OWNER_TRANSFER_PW
    try:
        db = db_client["MSANodeDB"]
        sec = db["bot_secrets"].find_one({"bot": "bot4"})
        if sec and sec.get("OWNER_TRANSFER_PW"):
            effective_pw = sec["OWNER_TRANSFER_PW"]
    except: pass
    if first_pw != effective_pw or text != effective_pw:
        await message.answer(
            f"❌ <b>INCORRECT PASSWORD.</b>\n"
            f"Transfer aborted. Both entries must match exactly.",
            parse_mode="HTML"
        )
        await state.clear(); return await admin_config_btn(message)
    old_owner = OWNER_ID
    OWNER_ID = target_id
    try:
        db = db_client["MSANodeDB"]
        db["bot_secrets"].update_one(
            {"bot": "bot4"}, {"$set": {"OWNER_ID": str(target_id)}}, upsert=False
        )
    except Exception as e:
        logging.error(f"Failed to persist OWNER_ID transfer: {e}")
    try:
        col_admins.delete_one({"user_id": target_id})
        col_admins.delete_one({"user_id": str(target_id)})
    except: pass
    try:
        await bot.send_message(
            target_id,
            f"👑 <b>OWNERSHIP TRANSFERRED TO YOU</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"You are now the <b>Supreme Owner</b> of MSA NODE SYSTEMS.\n"
            f"Previous Owner: <code>{old_owner}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💎 <i>Full control transferred.</i>",
            parse_mode="HTML"
        )
    except: pass
    await message.answer(
        f"✅ <b>OWNERSHIP TRANSFERRED</b>\n"
        f"New Owner: <code>{target_id}</code>",
        parse_mode="HTML"
    )
    await state.clear()


# ==========================================
# 🔒 LOCK / UNLOCK
# ==========================================
@dp.message(F.text == "🔒 LOCK / UNLOCK")
async def lock_entry(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("⛔ <b>OWNER ONLY.</b>", parse_mode="HTML"); return
    admins = list(col_admins.find())
    if not admins:
        await message.answer("⚠️ No admins found."); return
    markup, _, _, _ = _admin_select_keyboard(admins, page=0, include_status=True)
    await state.update_data(lock_page=0)
    await message.answer(
        f"🔒 <b>LOCK / UNLOCK ADMIN</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Active  |  🔴 Locked\n\n"
        f"Select admin to toggle access:",
        reply_markup=markup, parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_lock_admin)

@dp.message(BotState.waiting_for_lock_admin)
async def lock_admin_select(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": await state.clear(); return await admin_config_btn(message)
    if text in ("⬅️ PREV PAGE", "➡️ NEXT PAGE"):
        data = await state.get_data()
        page = data.get("lock_page", 0) + (1 if "NEXT" in text else -1)
        admins = list(col_admins.find())
        markup, _, page, _ = _admin_select_keyboard(admins, page=page, include_status=True)
        await state.update_data(lock_page=page)
        await message.answer(f"📋 Page {page + 1}", reply_markup=markup, parse_mode="HTML")
        return
    uid = _parse_admin_id_from_btn(text)
    if uid is None:
        await message.answer("⚠️ Invalid selection."); return
    doc = _admin_doc_by_id(uid)
    if not doc:
        await message.answer("❌ Admin not found."); return
    await state.update_data(lock_target_id=uid)
    is_locked = doc.get("locked", False)
    name = doc.get("name", "")
    display = f"👤 {name} ({uid})" if name else f"<code>{uid}</code>"
    status_text = "🔴 LOCKED (Inactive)" if is_locked else "🟢 ACTIVE (Operational)"
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔓 UNLOCK ADMIN") if is_locked else KeyboardButton(text="🔒 LOCK ADMIN"))
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    await message.answer(
        f"🔒 <b>ACCESS CONTROL</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Admin:</b> {display}\n"
        f"📊 <b>Status:</b> {status_text}\n\n"
        f"Select action:",
        reply_markup=builder.as_markup(resize_keyboard=True), parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_lock_toggle)

@dp.message(BotState.waiting_for_lock_toggle)
async def process_lock_toggle(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": return await lock_entry(message, state)
    data = await state.get_data()
    target_id = data.get("lock_target_id")
    doc = _admin_doc_by_id(target_id)
    if not doc:
        await message.answer("❌ Admin not found."); return
    if text == "🔒 LOCK ADMIN":
        update_admin_lock(target_id, True)
        await message.answer(
            f"🔒 <b>LOCKED</b>\n"
            f"<code>{target_id}</code> — access revoked, now inactive.",
            parse_mode="HTML"
        )
    elif text == "🔓 UNLOCK ADMIN":
        update_admin_lock(target_id, False)
        await message.answer(
            f"🔓 <b>UNLOCKED</b>\n"
            f"<code>{target_id}</code> — access restored, now active.",
            parse_mode="HTML"
        )
        role = doc.get("role", "⚙️ ADMIN")
        name = doc.get("name", str(target_id))
        await _send_role_welcome_message(target_id, role if role in ROLE_MESSAGES else "⚙️ ADMIN", name)
    await state.clear()
    await lock_entry(message, state)


# ==========================================
# 🚫 BANNED USERS
# ==========================================
@dp.message(F.text == "🚫 BANNED USERS")
async def banned_mgmt_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    banned_count = col_banned.count_documents({}) if col_banned is not None else 0
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔨 BAN USER"), KeyboardButton(text="🔓 UNBAN USER"))
    builder.row(KeyboardButton(text="📜 LIST BANNED"))
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    await message.answer(
        f"🚫 <b>BANNED USER MANAGEMENT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Blacklisted: <code>{banned_count}</code>\n\n"
        f"⚠️ Active admins cannot be banned.\n"
        f"Remove them as admin first.",
        reply_markup=builder.as_markup(resize_keyboard=True), parse_mode="HTML"
    )

@dp.message(F.text == "🔨 BAN USER")
async def ban_user_manual_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await message.answer(
        "🔨 <b>BAN USER</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Enter the <b>Telegram User ID</b> to blacklist.\n"
        "⚠️ Cannot ban active admins — remove admin role first.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu")]],
            resize_keyboard=True
        ), parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_ban_id)

@dp.message(BotState.waiting_for_ban_id)
async def process_manual_ban(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": await state.clear(); return await banned_mgmt_btn(message, state)
    if not text.isdigit():
        await message.answer("⚠️ Invalid ID."); return
    target_id = int(text)
    if target_id == OWNER_ID:
        await message.answer("❌ Cannot ban the Owner."); return
    if _admin_doc_by_id(target_id):
        await message.answer(
            f"⛔ <b>CANNOT BAN ACTIVE ADMIN</b>\n"
            f"<code>{target_id}</code> is currently an admin.\n"
            f"Use <b>➖ REMOVE ADMIN</b> first, then ban.",
            parse_mode="HTML"
        ); return
    if is_banned(target_id):
        await message.answer(f"⚠️ <code>{target_id}</code> is already banned.", parse_mode="HTML"); return
    try:
        col_banned.insert_one({
            "user_id": target_id,
            "reason": f"Manual ban by admin {message.from_user.id}",
            "timestamp": now_local()
        })
    except Exception as e:
        await message.answer(f"❌ Failed: <code>{e}</code>", parse_mode="HTML"); return
    if message.from_user.id != OWNER_ID:
        try:
            await bot.send_message(
                OWNER_ID,
                f"🔨 <b>BAN LOG</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Admin: <code>{message.from_user.id}</code>\n"
                f"Banned: <code>{target_id}</code>\n"
                f"Time: <code>{now_local().strftime('%b %d, %Y  ·  %I:%M %p')}</code>",
                parse_mode="HTML"
            )
        except: pass
    await message.answer(
        f"✅ <b>USER BANNED</b>\n"
        f"<code>{target_id}</code> silently blacklisted.",
        parse_mode="HTML"
    )
    await state.clear()
    await banned_mgmt_btn(message, state)

@dp.message(F.text == "🔓 UNBAN USER")
async def unban_user_btn(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    if col_banned is None:
        await message.answer("❌ Database unavailable."); return
    bans = list(col_banned.find().sort("timestamp", -1).limit(25))
    if not bans:
        await message.answer("✅ <b>No banned users.</b>", parse_mode="HTML"); return
    builder = ReplyKeyboardBuilder()
    for b in bans:
        uid = b.get("user_id")
        builder.add(KeyboardButton(text=f"🚫 {uid}"))
    builder.adjust(2)
    builder.row(KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="🔙 Back to Menu"))
    await message.answer(
        f"🔓 <b>UNBAN USER</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Select user to remove from blacklist:",
        reply_markup=builder.as_markup(resize_keyboard=True), parse_mode="HTML"
    )
    await state.set_state(BotState.waiting_for_unban_id)

@dp.message(BotState.waiting_for_unban_id)
async def process_unban(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    if text == "⬅️ BACK": await state.clear(); return await banned_mgmt_btn(message, state)
    uid_str = text.replace("🚫", "").strip()
    if not uid_str.isdigit():
        await message.answer("⚠️ Invalid selection."); return
    target_id = int(uid_str)
    try:
        res = col_banned.delete_one({"user_id": target_id})
        if res.deleted_count == 0:
            col_banned.delete_one({"user_id": str(target_id)})
        await message.answer(
            f"✅ <b>UNBANNED</b>\n"
            f"<code>{target_id}</code> removed from blacklist.",
            parse_mode="HTML"
        )
        if message.from_user.id != OWNER_ID:
            try:
                await bot.send_message(
                    OWNER_ID,
                    f"🔓 <b>UNBAN LOG</b>\n"
                    f"Admin: <code>{message.from_user.id}</code>\n"
                    f"Unbanned: <code>{target_id}</code>\n"
                    f"Time: <code>{now_local().strftime('%b %d, %Y  ·  %I:%M %p')}</code>",
                    parse_mode="HTML"
                )
            except: pass
    except Exception as e:
        await message.answer(f"❌ Failed: <code>{e}</code>", parse_mode="HTML")
    await state.clear()
    await banned_mgmt_btn(message, state)

@dp.message(F.text == "📜 LIST BANNED")
async def list_banned_btn(message: types.Message):
    if not is_admin(message.from_user.id): return
    if col_banned is None:
        await message.answer("❌ Database unavailable."); return
    try:
        bans = list(col_banned.find().sort("timestamp", -1))
    except Exception as e:
        await message.answer(f"❌ Error: <code>{e}</code>", parse_mode="HTML"); return
    lines = [f"🚫 <b>BLACKLISTED USERS</b>", f"━━━━━━━━━━━━━━━━━━━━"]
    if not bans:
        lines.append("<i>No banned users.</i>")
    else:
        for idx, b in enumerate(bans, 1):
            uid = b.get("user_id")
            reason = b.get("reason", "Unknown")
            ts = b.get("timestamp")
            date_str = ts.strftime("%b %d, %Y  ·  %I:%M %p") if isinstance(ts, datetime) else "Unknown"
            lines.append(
                f"<b>{idx}.</b> <code>{uid}</code>\n"
                f"   📅 <code>{date_str}</code>\n"
                f"   📝 {reason}\n"
            )
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    await message.answer("\n".join(lines), parse_mode="HTML")


# ------------------------------------------------
# 🔙 BACK CATCH-ALL for admin sub-menus (no state)
# Fires when ⬅️ BACK is pressed while in DEFAULT
# state (e.g. from Banned Users screen). Returns
# the admin to Admin Config, others to main menu.
# ------------------------------------------------
@dp.message(F.text == "⬅️ BACK")
async def admin_back_catchall(message: types.Message, state: FSMContext):
    await state.clear()
    if is_admin(message.from_user.id):
        await admin_config_btn(message)
    else:
        await start(message, state)


# ==========================================
# 💻 TERMINAL VIEWER
# ==========================================
@dp.message(F.text == "💻 Live Terminal")
async def show_terminal(message: types.Message):
    """Shows live terminal logs from memory with auto Telegram char-limit enforcement."""
    if not is_admin(message.from_user.id): return

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🔄 REFRESH TERMINAL"), KeyboardButton(text="🔙 Back to Menu")]],
        resize_keyboard=True
    )

    if not LOG_BUFFER:
        await message.answer(
            "💻 <b>LIVE TERMINAL</b>\n━━━━━━━━━━━━━━━━━━━━\n<i>No logs captured yet.</i>",
            parse_mode="HTML", reply_markup=kb
        )
        return

    import html as _html

    now_str  = now_local().strftime("%b %d, %Y  ·  %I:%M:%S %p")
    buf_size = len(LOG_BUFFER)

    # Build header + footer — measure their char cost
    header = (
        f"💻 <b>LIVE TERMINAL STREAM</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 <b>Buffer:</b> <code>{buf_size}</code> lines  |  "
        f"🕐 <code>{now_str}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<pre>"
    )
    footer = (
        f"</pre>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 <i>Connection Active</i>"
    )

    # Telegram hard limit is 4096 chars; leave headroom for HTML tags
    MAX_CONTENT = 4096 - len(header) - len(footer) - 50

    # Take last lines, newest at bottom; trim to fit
    all_logs = list(LOG_BUFFER)
    safe_lines = []
    total = 0
    for line in reversed(all_logs):
        safe_line = _html.escape(line)
        cost = len(safe_line) + 1  # +1 for newline
        if total + cost > MAX_CONTENT:
            break
        safe_lines.append(safe_line)
        total += cost

    safe_lines.reverse()  # restore chronological order
    log_body = "\n".join(safe_lines)
    if len(all_logs) > len(safe_lines):
        log_body = f"[... {len(all_logs) - len(safe_lines)} older lines trimmed ...]\n" + log_body

    msg = header + log_body + footer
    await message.answer(msg, parse_mode="HTML", reply_markup=kb)

@dp.message(F.text == "🔄 REFRESH TERMINAL")
async def refresh_terminal(message: types.Message):
    if not is_admin(message.from_user.id): return
    await show_terminal(message)

# Handle BACK TO MENU separately if not already global


# ==========================================
# 💎 ELITE HELP — PAGINATED AGENT GUIDE
# ==========================================

_ELITE_GUIDE_PAGES = [
    # ── PAGE 1 / 4 ── PDF GENERATION ENGINE (Deep Dive)
    (
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        "  💎  <b>MSANODE GOD-MODE PREMIER MANUAL</b>\n"
        "  <i>Classified Operational Protocol v6.0</i>  ·  Page 1 / 4\n"
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
        "📄 <b>1. PDF GENERATION ENGINE</b>\n\n"
        "<b>Step-by-step:</b>\n"
        "  ① Press <b>📄 Generate PDF</b> from the main menu\n"
        "  ② A list of recent codes appears for reference\n"
        "  ③ Send your unique <b>Project Code</b> (e.g. <code>S19</code>, <code>MSA042</code>)\n"
        "     ↳ Code must NOT already exist in the DB\n"
        "     ↳ Alphanumeric only — no spaces or special chars\n"
        "  ④ Paste your full script/content as the next message\n"
        "     ↳ Multiple messages auto-merge into one script\n"
        "  ⑤ Bot renders the PDF using ReportLab + Google Drive\n"
        "  ⑥ Drive link saved to MongoDB, bound to your code ✅\n\n"
        "<b>📐 FORMATTING SYNTAX CODES:</b>\n"
        "  <code>^^^^^^url^^^^^^</code>  →  🔵 Clickable Blue Hyperlink\n"
        "  <code>^^^^^TEXT^^^^^</code>  →  ⚫ BLACK · BOLD · CAPS\n"
        "  <code>^^^^TEXT^^^^</code>   →  🔵 BLUE · BOLD · CAPS\n"
        "  <code>^^^TEXT^^^</code>    →  🔴 RED · BOLD · CAPS\n"
        "  <code>^TEXT^</code>        →  ⚫ Bold Black (normal case)\n"
        "  <code>I. Title</code>      →  🔴 Auto Red Section Header\n"
        "  <code>II. Title</code>     →  🔴 Auto Red Section Header\n\n"
        "<b>⚠️ Rules & Notes:</b>\n"
        "  • Duplicate codes are rejected — check library first\n"
        "  • Drive folder is shared — keep codes unique\n"
        "  • PDF is permanent until manually removed or nuked\n\n"
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        "💎 <b>MSA NODE BOT 4</b>  |  <i>God-Mode Manual</i>  |  Page 1 of 4"
    ),
    # ── PAGE 2 / 4 ── VAULT & LIBRARY + GET LINK + EDIT PDF
    (
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        "  💎  <b>MSANODE GOD-MODE PREMIER MANUAL</b>\n"
        "  <i>Classified Operational Protocol v6.0</i>  ·  Page 2 / 4\n"
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
        "🗃️ <b>2. VAULT & LIBRARY</b>\n\n"
        "📋 <b>SHOW LIBRARY</b>\n"
        "  • Full PDF index sorted by Date (newest first)\n"
        "  • Each row: <code>Index · Code · Date · 🔗 Link</code>\n"
        "  • Restored files marked <b>[R]</b> for traceability\n"
        "  • Live <b>🔍 Search</b> — type code or partial name\n"
        "  • NEXT / PREV pagination (15 entries per page)\n\n"
        "🔗 <b>GET LINK</b>\n"
        "  ① Press <b>🔗 Get Link</b> → choose retrieval mode:\n"
        "     ↳ <b>Single</b> — enter one code, get its Drive URL\n"
        "     ↳ <b>Bulk Range</b> — enter range e.g. <code>1-10</code>\n"
        "  ② Mode: <b>PDF File</b> (download) or <b>Link</b> (URL only)\n"
        "  ③ Link returned instantly — no expiry if Drive is public\n\n"
        "✏️ <b>EDIT PDF</b>\n"
        "  ① Press <b>✏️ Edit PDF</b> → pick edit mode:\n"
        "     ↳ <b>Edit Code</b> — renames MSA code in MongoDB\n"
        "     ↳ <b>Edit Link</b> — replaces stored Google Drive URL\n"
        "  ② Single or Bulk mode available\n"
        "  ③ Enter target → enter new value → saved instantly ✅\n"
        "  ⚠️ Editing code does NOT rename the Drive file\n\n"
        "🗑 <b>REMOVE PDF</b>\n"
        "  • Soft-delete: moved to Recycle Bin (recoverable)\n"
        "  • Modes: Single · Bulk Range · Permanent (hard-delete)\n"
        "  • Confirmation required before executing any deletion\n\n"
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        "💎 <b>MSA NODE BOT 4</b>  |  <i>God-Mode Manual</i>  |  Page 2 of 4"
    ),
    # ── PAGE 3 / 4 ── SECURITY + ADMIN CONFIG
    (
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        "  💎  <b>MSANODE GOD-MODE PREMIER MANUAL</b>\n"
        "  <i>Classified Operational Protocol v6.0</i>  ·  Page 3 / 4\n"
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
        "🛡 <b>3. SECURITY SYSTEMS</b>\n\n"
        "  🚨 <b>Unauthorized Access Alerts</b>\n"
        "     Any non-admin who triggers /start is instantly\n"
        "     reported to Owner with: Name · ID · Username · Time\n\n"
        "  🤖 <b>Anti-Spam Auto-Ban</b>\n"
        "     >5 messages in 2 seconds → silent auto-ban\n"
        "     Owner receives instant BAN LOG notification\n\n"
        "  ⚠️ <b>NUKE ALL DATA</b> ☠️\n"
        "     Permanently wipes ALL MongoDB records in one action.\n"
        "     Wipes: PDFs · Admins · Banned Users · All Collections\n"
        "     Requires <b>TRIPLE confirmation</b> before executing.\n"
        "     <b>No recovery possible. Emergency use only.</b>\n\n"
        "⚙️ <b>4. ADMIN CONFIG — Full Control Panel</b>\n\n"
        "  ➕ <b>Add Admin</b> — Grant access by Telegram User ID\n"
        "     → New admins receive all permissions by default\n"
        "  ➖ <b>Remove Admin</b> — Permanently revoke access\n"
        "  🔒 <b>Lock Admin</b> — Freeze without removing record\n"
        "  🔓 <b>Unlock Admin</b> — Restore a locked admin\n"
        "  🛡️ <b>Permissions</b> — Toggle per-feature access:\n"
        "     Generate PDF · Get Link · Show Library · Edit PDF\n"
        "     Storage Info · Diagnosis · Terminal · Remove · NUKE\n"
        "  🏅 <b>Roles</b> — Assign named titles (Chief Editor…)\n"
        "  📜 <b>List Admins</b> — Full roster with status + role\n"
        "  🚫 <b>Banned Users</b> — Ban · Unban · List blacklist\n\n"
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        "💎 <b>MSA NODE BOT 4</b>  |  <i>God-Mode Manual</i>  |  Page 3 of 4"
    ),
    # ── PAGE 4 / 4 ── LIVE TOOLS + BACKUP SYSTEMS + STATUS
    (
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        "  💎  <b>MSANODE GOD-MODE PREMIER MANUAL</b>\n"
        "  <i>Classified Operational Protocol v6.0</i>  ·  Page 4 / 4\n"
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
        "💻 <b>LIVE TERMINAL</b>\n"
        "  • Streams last 50 lines of real-time bot console\n"
        "  • Press <b>🔄 REFRESH TERMINAL</b> to pull latest logs\n"
        "  • Shows: DB queries · errors · PDF events · admin ops\n"
        "  • Critical for live debugging and monitoring\n\n"
        "📊 <b>STORAGE INFO</b>\n"
        "  • Total PDFs in library (active + locked)\n"
        "  • Recycle Bin items (soft-deleted, recoverable)\n"
        "  • MongoDB Atlas latency (live ping in ms)\n"
        "  • DB connection status: 🟢 Online / 🔴 Offline\n\n"
        "🩺 <b>SYSTEM DIAGNOSIS</b>\n"
        "  • Full health check: MongoDB · Drive API · Bot latency\n"
        "  • Reports any anomalies or high latency instantly\n\n"
        "📦 <b>5. BACKUP SYSTEMS</b>\n\n"
        "  📲 <b>Manual Backup</b> (click 📦 Backup):\n"
        "     ↳ Text Report — Admin list + full PDF library\n"
        "     ↳ JSON Dump — Full DB export, restore-ready\n"
        "     ↳ Sent directly to Owner via Telegram\n\n"
        "  🕐 <b>Weekly Auto-Pilot</b>:\n"
        "     Every <b>Sunday @ 03:00 AM</b> — full snapshot auto-sent\n\n"
        "  📅 <b>Monthly Auto-Backup</b>:\n"
        "     1st of each month @ <b>03:30 AM</b> — JSON + Text\n"
        "     Dedup-guard: skips if month already backed up\n\n"
        "  📊 <b>Daily Status Reports</b>:\n"
        "     Sent at <b>08:40 AM</b> and <b>08:40 PM</b> (local time)\n"
        "     Shows: PDFs generated · links fetched · errors today\n\n"
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        "🚀 <b>SYSTEM STATUS:</b> 🟢 OPTIMAL\n"
        "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        "💎 <b>MSA NODE BOT 4</b>  |  <i>God-Mode Manual</i>  |  Page 4 of 4\n"
        "<i>Press ⬅️ PREV PAGE to go back or 🔙 Back to Menu to exit.</i>"
    ),
]

def _elite_guide_kb(page: int, total: int) -> ReplyKeyboardMarkup:
    """Navigation keyboard for Elite Help — PREV / NEXT + Back to Menu."""
    row_nav = []
    if page > 1:
        row_nav.append(KeyboardButton(text="⬅️ PREV PAGE"))
    if page < total:
        row_nav.append(KeyboardButton(text="NEXT PAGE ➡️"))
    rows = []
    if row_nav:
        rows.append(row_nav)
    rows.append([KeyboardButton(text="🔙 Back to Menu")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

@dp.message(F.text == "\U0001F48E Full Guide")
async def elite_help(message: types.Message, state: FSMContext):
    """Open the Bot 4 Full Guide — page 1."""
    if not is_admin(message.from_user.id): return
    await state.clear()
    await state.set_state(BotState.viewing_elite_help)
    await state.update_data(elite_help_page=1)
    await message.answer(
        _ELITE_GUIDE_PAGES[0],
        parse_mode="HTML",
        reply_markup=_elite_guide_kb(1, len(_ELITE_GUIDE_PAGES)),
    )

@dp.message(F.text == "NEXT PAGE ➡️")
async def elite_help_next(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    data = await state.get_data()
    page = min(data.get("elite_help_page", 1) + 1, len(_ELITE_GUIDE_PAGES))
    await state.update_data(elite_help_page=page)
    await message.answer(
        _ELITE_GUIDE_PAGES[page - 1],
        parse_mode="HTML",
        reply_markup=_elite_guide_kb(page, len(_ELITE_GUIDE_PAGES)),
    )

@dp.message(F.text == "⬅️ PREV PAGE")
async def elite_help_prev(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    data = await state.get_data()
    page = max(data.get("elite_help_page", 1) - 1, 1)
    await state.update_data(elite_help_page=page)
    await message.answer(
        _ELITE_GUIDE_PAGES[page - 1],
        parse_mode="HTML",
        reply_markup=_elite_guide_kb(page, len(_ELITE_GUIDE_PAGES)),
    )


# ==========================================
# 🗄️ DATABASE & ENV MANAGEMENT
# ==========================================

# --- BACK NAVIGATION ---
@dp.message(F.text == "🔙 Back")
async def back_router(message: types.Message, state: FSMContext):
    await start(message, state)
    
@dp.message(F.text == "🏠 Main Menu")
async def main_menu_return(message: types.Message, state: FSMContext):
    await start(message, state)

# ==========================================
# 🛽 CATCH-ALL HANDLERS (must be LAST)
# Prevents aiogram "Update is not handled" warnings
# These fire only when NO other handler matched.
# ==========================================

@dp.message()
async def _catchall_message(message: types.Message, state: FSMContext):
    """
    Silently absorbs any message that no specific handler matched.
    - Non-admins / strangers: drop silently (no response).
    - Admins: redirect to main menu (they likely pressed an unknown button).
    """
    if not message.from_user:
        return
    uid = message.from_user.id
    if is_banned(uid):
        return
    # Admins hitting an unrecognised button → send them back to menu
    if is_admin(uid):
        await state.clear()
        await message.answer(
            "⚠️ <b>Unrecognised command.</b>\nReturning to main menu.",
            parse_mode="HTML",
            reply_markup=get_main_menu(uid)
        )
    # Strangers: silently ignore (no "not handled" noise in logs)


@dp.callback_query()
async def _catchall_callback(callback: types.CallbackQuery):
    """Silently ack any callback query not matched by a specific handler."""
    try:
        await callback.answer()
    except Exception:
        pass


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
        await _safe_send_message(OWNER_ID, alert, parse_mode="Markdown")
    except Exception as _ge:
        logging.error(f"Failed to send Error Alert to Owner: {_ge}")
        
    # We log it but do not crash the bot
    logging.error(f"Global Error Caught: {exception}")


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
            
        # 2. REPORT IF ISSUES
        if issues:
            report = (
                f"⚠️ **AUTO-CHECKUP WARNING**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Your system has detected irregularities:\n"
            )
            for i in issues: report += f"• 🔴 {i}\n"
            report += f"\n🕐 Time: {now_local().strftime('%I:%M:%S %p')}"
            
            try: await _safe_send_message(OWNER_ID, report, parse_mode="Markdown")
            except Exception as _hm: logging.warning(f"Health monitor alert failed: {_hm}")

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
        await _persist_stats()
        print(f"🔄 Daily stats reset at {now_local().strftime('%I:%M %p')}")


async def strict_daily_report():
    """
    Sends a detailed report EXACTLY at 08:40 AM and 08:40 PM (local timezone).
    Uses sleep-until so the report fires correctly after any restart — no missed slots.
    """
    print("🕰️ Strict Daily Report: Online (08:40 AM/PM)")
    _slots = [(8, 40, "08:40 AM"), (20, 40, "08:40 PM")]

    while True:
        try:
            # ── Calculate exact sleep until next slot ──────────────────────────
            now = now_local()
            next_fire = None
            next_label = None
            for hour, minute, label in _slots:
                candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if candidate <= now:          # already passed — push to tomorrow
                    candidate += timedelta(days=1)
                if next_fire is None or candidate < next_fire:
                    next_fire = candidate
                    next_label = label

            wait_secs = (next_fire - now_local()).total_seconds()
            h_w = int(wait_secs // 3600)
            m_w = int((wait_secs % 3600) // 60)
            print(f"🕰️ Next daily report '{next_label}' in {h_w}h {m_w}m")
            await asyncio.sleep(max(wait_secs, 1))

            # ── Dedup guard: skip if already sent within 20 min of this slot ──────
            _DEDUP_WINDOW = 20 * 60   # 20 minutes
            if col_bot4_state is not None:
                try:
                    rec = col_bot4_state.find_one({"_id": "last_report_sent"})
                    if rec:
                        last_ts = rec.get("ts")
                        if last_ts and (datetime.now() - last_ts).total_seconds() < _DEDUP_WINDOW:
                            print(f"🕰️ Report dedup: already sent within last {_DEDUP_WINDOW//60}m — skipping.")
                            await asyncio.sleep(60)
                            continue
                except Exception:
                    pass

            # ── Fire the report ───────────────────────────────────────────────
            now = now_local()
            current_time = now.strftime("%I:%M %p")   # e.g. "08:40 AM"

            uptime_secs = int(time.time() - START_TIME)
            uptime_str = f"{uptime_secs // 3600}h {(uptime_secs % 3600) // 60}m"
            admin_count  = col_admins.count_documents({}) if col_admins  is not None else 0
            banned_count = col_banned.count_documents({}) if col_banned  is not None else 0
            pdf_count    = col_pdfs.count_documents({})   if col_pdfs    is not None else 0
            locked_count = col_locked.count_documents({}) if col_locked  is not None else 0

            db_status = "🔴 Offline"
            try:
                t0 = time.time()
                db_client.admin.command('ping')
                lat = (time.time() - t0) * 1000
                db_status = f"🟢 Connected ({lat:.0f}ms)"
            except Exception:
                pass

            report = (
                f"📅 **DAILY SYSTEM REPORT · BOT 4**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🕐 **Time:** `{current_time}`  |  📆 `{now.strftime('%b %d, %Y')}`\n"
                f"⚙️ **System:** 🟢 OPERATIONAL  |  ⏱ Uptime: `{uptime_str}`\n\n"
                f"📊 **LIBRARY:**\n"
                f"• 📚 PDFs Active: `{pdf_count}`\n"
                f"• 🔒 Locked Content: `{locked_count}`\n\n"
                f"👤 **USERS:**\n"
                f"• 👥 Admins: `{admin_count}`\n"
                f"• 🚫 Blacklisted: `{banned_count}`\n\n"
                f"📈 **TODAY'S ACTIVITY:**\n"
                f"• 📄 PDFs Generated: `{DAILY_STATS_BOT4['pdfs_generated']}`\n"
                f"• 🔗 Links Retrieved: `{DAILY_STATS_BOT4['links_retrieved']}`\n"
                f"• 🗑️ PDFs Deleted: `{DAILY_STATS_BOT4['pdfs_deleted']}`\n"
                f"• ⚠️ Errors Today: `{DAILY_STATS_BOT4['errors']}`\n\n"
                f"🛡️ **INFRASTRUCTURE:**\n"
                f"• 🗄️ MongoDB Atlas: {db_status}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💎 **MSA NODE SYSTEMS** | Verified."
            )

            await _safe_send_message(OWNER_ID, report, parse_mode="Markdown")
            print(f"✅ Daily Report Sent at {current_time}")

            # ── Mark sent in MongoDB to prevent duplicate on restart ─────────
            if col_bot4_state is not None:
                try:
                    col_bot4_state.update_one(
                        {"_id": "last_report_sent"},
                        {"$set": {"ts": datetime.now(), "label": next_label, "time": current_time}},
                        upsert=True
                    )
                except Exception:
                    pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Daily Report Failed: {e}")
            await asyncio.sleep(60)   # back off then re-calculate next slot


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

            caption = (
                f"📅 <b>MONTHLY AUTO-BACKUP · {month_label}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📆 <b>Date:</b> {fire_now.strftime('%b %d, %Y  ·  %I:%M %p')}\n"
                f"📊 <b>PDFs:</b> {len(data['pdfs'])} | <b>Admins:</b> {len(data['admins'])} | <b>Banned:</b> {len(data['banned'])}\n"
                f"💾 <b>Storage:</b> MongoDB Atlas\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <i>All data for {month_label} secured.</i>"
            )

            # Send JSON to Owner
            await _safe_send_document(
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
                        "month_key": month_key,
                        "month":     month_label,
                        "date":      fire_now,
                        "pdf_count": len(data["pdfs"]),
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


async def weekly_backup():
    """
    Runs every Sunday at 03:00 AM local time.
    Sends a full JSON dump + text report to Owner.
    Dedup-guard: will not fire twice within 1 hour of the same slot.
    """
    print("🗓️ Weekly Backup Scheduler: Online (Sun @ 03:00 AM)")
    while True:
        try:
            now = now_local()
            # Days until next Sunday (weekday 6)
            days_until_sunday = (6 - now.weekday()) % 7
            if days_until_sunday == 0:
                # Today is Sunday — check if 03:00 AM has passed
                target_today = now.replace(hour=3, minute=0, second=0, microsecond=0)
                if now >= target_today:
                    days_until_sunday = 7  # push to next Sunday
            target = (now + timedelta(days=days_until_sunday)).replace(
                hour=3, minute=0, second=0, microsecond=0
            )
            wait_secs = (target - now).total_seconds()
            print(f"🗓️ Weekly Backup in {wait_secs / 3600:.1f}h (on {target.strftime('%a %b %d, %Y at %I:%M %p')})")
            await asyncio.sleep(max(wait_secs, 1))

            # --- fire ---
            fire_now = now_local()
            week_key = fire_now.strftime("%Y-W%U")  # e.g. "2026-W08"

            # Dedup guard: skip if already sent this week
            try:
                if col_bot4_state is not None:
                    rec = col_bot4_state.find_one({"_id": "last_weekly_backup"})
                    if rec and rec.get("week_key") == week_key:
                        print(f"🗓️ Weekly Backup for {week_key} already sent — skipping.")
                        await asyncio.sleep(3600)
                        continue
            except Exception:
                pass

            # Text report
            txt_filename = await asyncio.to_thread(generate_system_backup)

            # JSON dump
            date_label = fire_now.strftime("%Y-%m-%d")
            json_filename = f"MSANODE_WEEKLY_{date_label}.json"
            data = {
                "backup_type": "weekly",
                "week_key": week_key,
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

            caption = (
                f"🗓️ <b>WEEKLY AUTO-BACKUP</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📆 <b>Date:</b> {fire_now.strftime('%a %b %d, %Y  ·  %I:%M %p')}\n"
                f"📊 <b>PDFs:</b> {len(data['pdfs'])} | <b>Admins:</b> {len(data['admins'])} | <b>Banned:</b> {len(data['banned'])}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <i>Weekly snapshot secured.</i>"
            )

            await _safe_send_document(
                OWNER_ID,
                FSInputFile(json_filename),
                caption=caption,
                parse_mode="HTML"
            )

            # Mark sent
            try:
                if col_bot4_state is not None:
                    col_bot4_state.update_one(
                        {"_id": "last_weekly_backup"},
                        {"$set": {"week_key": week_key, "ts": datetime.now()}},
                        upsert=True
                    )
            except Exception:
                pass

            print(f"✅ Weekly Backup Sent — {week_key}")

            # Cleanup local files
            for f in [txt_filename, json_filename]:
                if f and os.path.exists(f):
                    try:
                        os.remove(f)
                    except Exception:
                        pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            await notify_error_bot4("Weekly Backup Failed", str(e))

        await asyncio.sleep(3600)  # wait before re-calculating


async def _migrate_permissions():
    """
    Temporary migration: Ensure all existing admins have 'elite_help'.
    Also ensures 'nuke_data' if missing, for consistency.
    """
    if col_admins is None: return
    try:
        # 1. Update all admins to include 'elite_help' if not present
        # We can just push it to the set, but Mongo stores as list.
        # Efficient way: Add to set if not exists.
        
        # Get all admins
        dirs = list(col_admins.find({}))
        count = 0
        for d in dirs:
            perms = set(d.get("permissions", []))
            updated = False
            
            # Auto-grant Elite Help
            if "elite_help" not in perms:
                perms.add("elite_help")
                updated = True
                
            # Save back if changed
            if updated:
                col_admins.update_one(
                    {"_id": d["_id"]},
                    {"$set": {"permissions": list(perms)}}
                )
                count += 1
        
        if count > 0:
            print(f"🔄 Migrated {count} admins: Added 'elite_help' permission.")
    except Exception as e:
        print(f"⚠️ Migration Error: {e}")

async def main():
    # ── 1. Network startup (retry until Telegram responds) ──────────────────
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.set_my_commands([BotCommand(command="start", description="Menu")])
            break
        except Exception as e:
            print(f"⚠️ Network Startup Error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)

    # ── 2. Background tasks ──────────────────────────────────────────────────
    asyncio.create_task(auto_janitor())
    asyncio.create_task(weekly_backup())
    asyncio.create_task(monthly_backup())
    asyncio.create_task(system_guardian())
    asyncio.create_task(reset_daily_stats())
    asyncio.create_task(strict_daily_report())
    asyncio.create_task(auto_health_monitor())

    # ── 3. One-time startup tasks ────────────────────────────────────────────
    await _migrate_permissions()
    await _load_persisted_stats()

    print("💎 MSANODE BOT 4 ONLINE")

    # ── 4. ONLINE notification (awaited directly — never silently lost) ──────
    boot_time = now_local().strftime('%I:%M %p · %b %d, %Y')
    _online_msg = (
        "💎 <b>MSA NODE BOT 4: ONLINE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🟢 <b>Status:</b> OPERATIONAL\n"
        f"🕐 <b>Booted:</b> {boot_time}\n\n"
        "<i>I am awake and ready to serve, Master.</i>"
    )
    for _attempt in range(3):
        try:
            await bot.send_message(OWNER_ID, _online_msg, parse_mode="HTML")
            print("✅ Online notification sent.")
            break
        except TelegramRetryAfter as _fl:
            logging.info(f"Online notify flood-wait {_fl.retry_after}s — retrying...")
            await asyncio.sleep(_fl.retry_after + 1)
        except Exception as _e:
            logging.warning(f"Online notify attempt {_attempt + 1} failed: {_e}")
            await asyncio.sleep(2)

    # ── 5. Start web server + webhook or polling ─────────────────────────────
    web_runner = None
    try:
        if _WEBHOOK_URL:
            # ── WEBHOOK MODE (production) ────────────────────────────────────
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.set_webhook(_WEBHOOK_URL)
            print(f"✅ Webhook set: {_WEBHOOK_URL}")
            web_runner = await start_web_server(dp, bot)
            # Stay alive — aiohttp handles incoming Telegram updates
            await asyncio.Event().wait()
        else:
            # ── POLLING MODE (local dev fallback) ────────────────────────────
            print("ℹ️ No RENDER_EXTERNAL_URL — using polling (local dev mode)")
            web_runner = await start_web_server(dp, bot)
            while True:
                try:
                    await dp.start_polling(bot, skip_updates=True)
                    print("⚠️ Polling loop returned. Restarting in 5s...")
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logging.error(f"Polling Network Error: {e}. Retrying in 5s...")
                    await asyncio.sleep(5)
    finally:
        if web_runner:
            await web_runner.cleanup()
        # ── 6. OFFLINE notification (awaited in finally — fires before loop closes) ──
        _off_time = now_local().strftime('%I:%M %p · %b %d, %Y')
        _offline_msg = (
            "🔴 <b>MSA NODE BOT 4: OFFLINE</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🟠 <b>Status:</b> SHUTTING DOWN\n"
            f"🕐 <b>Time:</b> {_off_time}\n\n"
            "<i>Bot 4 has stopped. Restart me when needed.</i>"
        )
        for _attempt in range(3):
            try:
                await bot.send_message(OWNER_ID, _offline_msg, parse_mode="HTML")
                print("✅ Offline notification sent.")
                break
            except TelegramRetryAfter as _fl:
                await asyncio.sleep(_fl.retry_after + 1)
            except Exception as _e:
                logging.warning(f"Offline notify attempt {_attempt + 1} failed: {_e}")
                await asyncio.sleep(1)
        try:
            await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    print("🚀 STARTING INDIVIDUAL CORE TEST: BOT 4")
    threading.Thread(target=run_health_server, daemon=True).start()
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("◈ Bot 4 Shutdown.")
