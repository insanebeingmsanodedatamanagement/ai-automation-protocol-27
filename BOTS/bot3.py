import logging
import asyncio
import os
import sys
import psutil
import json
import traceback
import pickle
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import StateFilter
import pymongo
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
import re
import string
import random
from bson.objectid import ObjectId
import pytz
from logging.handlers import RotatingFileHandler
from aiohttp import web

# ==========================================
# ENTERPRISE CONFIGURATION
# ==========================================

# Bot Configuration
BOT_TOKEN = os.environ.get("BOT_9_TOKEN", os.environ.get("BOT_TOKEN"))
MONGO_URI = os.environ.get("MONGO_URI")
MASTER_ADMIN_ID = int(os.environ.get("MASTER_ADMIN_ID", 0))
OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# Global variable for health server cleanup
health_server_runner = None

# Database Configuration
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "MSANodeDB")
MONGO_MAX_POOL_SIZE = int(os.environ.get("MONGO_MAX_POOL_SIZE", 100))
MONGO_MIN_POOL_SIZE = int(os.environ.get("MONGO_MIN_POOL_SIZE", 10))
MONGO_CONNECT_TIMEOUT_MS = int(os.environ.get("MONGO_CONNECT_TIMEOUT_MS", 10000))

# Security Configuration
RATE_LIMIT_SPAM_THRESHOLD = int(os.environ.get("RATE_LIMIT_SPAM_THRESHOLD", 10))
RATE_LIMIT_SPAM_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_SPAM_WINDOW_SECONDS", 30))

# Auto-Healer Configuration
HEALTH_CHECK_INTERVAL = int(os.environ.get("HEALTH_CHECK_INTERVAL_SECONDS", 60))
HEALTH_CHECK_CRITICAL_THRESHOLD = int(os.environ.get("HEALTH_CHECK_CRITICAL_THRESHOLD", 3))
HEALTH_AUTO_RESTART = os.environ.get("HEALTH_AUTO_RESTART", "true").lower() == "true"
ERROR_NOTIFICATION_ENABLED = os.environ.get("ERROR_NOTIFICATION_ENABLED", "true").lower() == "true"
CRITICAL_ERROR_NOTIFY_IMMEDIATELY = os.environ.get("CRITICAL_ERROR_NOTIFY_IMMEDIATELY", "true").lower() == "true"

# Daily Reports Configuration
DAILY_REPORT_ENABLED = os.environ.get("DAILY_REPORT_ENABLED", "true").lower() == "true"
DAILY_REPORT_TIME_1 = os.environ.get("DAILY_REPORT_TIME_1", "08:40")
DAILY_REPORT_TIME_2 = os.environ.get("DAILY_REPORT_TIME_2", "20:40")
DAILY_REPORT_TIMEZONE = os.environ.get("DAILY_REPORT_TIMEZONE", "Asia/Kolkata")

# State Persistence Configuration
STATE_BACKUP_ENABLED = os.environ.get("STATE_BACKUP_ENABLED", "true").lower() == "true"
STATE_BACKUP_INTERVAL_MINUTES = int(os.environ.get("STATE_BACKUP_INTERVAL_MINUTES", 5))
STATE_BACKUP_LOCATION = os.environ.get("STATE_BACKUP_LOCATION", "./backups/state")
AUTO_RESUME_ON_STARTUP = os.environ.get("AUTO_RESUME_ON_STARTUP", "true").lower() == "true"

# Monitoring Configuration
TRACK_MEMORY_USAGE = os.environ.get("TRACK_MEMORY_USAGE", "true").lower() == "true"
TRACK_CPU_USAGE = os.environ.get("TRACK_CPU_USAGE", "true").lower() == "true"
ALERT_HIGH_MEMORY_MB = int(os.environ.get("ALERT_HIGH_MEMORY_MB", 500))
ALERT_HIGH_CPU_PERCENT = int(os.environ.get("ALERT_HIGH_CPU_PERCENT", 80))

# ==========================================
# ENTERPRISE LOGGING SETUP
# ==========================================

# Create logs directory if not exists
os.makedirs("logs", exist_ok=True)

# Main log handler with rotation
main_handler = RotatingFileHandler(
    "logs/bot9.log",
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
main_handler.setLevel(logging.INFO)

# Error log handler (separate file for errors)
error_handler = RotatingFileHandler(
    "logs/bot9_errors.log",
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
error_handler.setLevel(logging.ERROR)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p'
)
main_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[main_handler, error_handler, console_handler]
)
logger = logging.getLogger(__name__)

# ==========================================
# ENTERPRISE HEALTH MONITORING SYSTEM
# ==========================================

class HealthMonitor:
    """Enterprise-grade health monitoring and auto-healing system"""
    
    def __init__(self):
        self.health_checks_failed = 0
        self.last_health_check = datetime.now()
        self.error_count = 0
        self.warning_count = 0
        self.last_error_notification = None
        self.system_metrics = {
            "uptime_start": datetime.now(),
            "total_requests": 0,
            "total_errors": 0,
            "db_errors": 0,
            "api_errors": 0
        }
        self.is_healthy = True
        logger.info("✅ Health Monitor initialized")
    
    async def check_system_health(self):
        """Perform comprehensive system health check"""
        try:
            # Check memory usage
            memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            if memory_mb > ALERT_HIGH_MEMORY_MB:
                await self.send_alert(
                    "WARNING",
                    f"High Memory Usage: {memory_mb:.2f} MB (Threshold: {ALERT_HIGH_MEMORY_MB} MB)"
                )
            
            # Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > ALERT_HIGH_CPU_PERCENT:
                await self.send_alert(
                    "WARNING",
                    f"High CPU Usage: {cpu_percent}% (Threshold: {ALERT_HIGH_CPU_PERCENT}%)"
                )
            
            # Check database connection
            try:
                client.admin.command('ping')
                self.health_checks_failed = 0
                self.is_healthy = True
            except Exception as e:
                self.health_checks_failed += 1
                logger.error(f"Database health check failed: {e}")
                
                if self.health_checks_failed >= HEALTH_CHECK_CRITICAL_THRESHOLD:
                    await self.send_alert(
                        "CRITICAL",
                        f"Database connection failed {self.health_checks_failed} times! Attempting auto-heal..."
                    )
                    await self.auto_heal_database()
            
            self.last_health_check = datetime.now()
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            await self.send_error_notification("Health Check Failed", str(e), traceback.format_exc())
    
    async def auto_heal_database(self):
        """Attempt to auto-heal database connection"""
        try:
            logger.info("🔧 Attempting database auto-heal...")
            global client, db, col_pdfs, col_ig_content, col_logs, col_admins, col_banned_users, col_user_activity, col_settings, col_backups
            
            # Close existing connection
            try:
                client.close()
            except:
                pass
            
            # Reconnect
            client = pymongo.MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
                connectTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
                maxPoolSize=MONGO_MAX_POOL_SIZE,
                minPoolSize=MONGO_MIN_POOL_SIZE
            )
            db = client[MONGO_DB_NAME]
            
            # Reinitialize collections
            col_logs = db["bot9_logs"]
            col_pdfs = db["bot9_pdfs"]
            col_ig_content = db["bot9_ig_content"]
            col_settings = db["bot9_settings"]
            col_admins = db["bot9_admins"]
            col_banned_users = db["bot9_banned_users"]
            col_user_activity = db["bot9_user_activity"]
            col_backups = db["bot9_backups"]
            
            # Test connection
            client.admin.command('ping')
            
            self.health_checks_failed = 0
            self.is_healthy = True
            logger.info("✅ Database connection restored!")
            
            await self.send_alert("SUCCESS", "Database connection auto-healed successfully!")
            
        except Exception as e:
            logger.error(f"Auto-heal failed: {e}")
            await self.send_alert(
                "CRITICAL",
                f"Auto-heal FAILED! Manual intervention required!\nError: {str(e)}"
            )
    
    async def send_alert(self, level: str, message: str):
        """Send alert notification to admin"""
        try:
            if not ERROR_NOTIFICATION_ENABLED:
                return
            
            emoji_map = {
                "INFO": "ℹ️",
                "WARNING": "⚠️",
                "ERROR": "❌",
                "CRITICAL": "🚨",
                "SUCCESS": "✅"
            }
            
            emoji = emoji_map.get(level, "📢")
            timestamp = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
            
            alert_msg = f"{emoji} **BOT 9 HEALTH ALERT**\n\n"
            alert_msg += f"**Level:** {level}\n"
            alert_msg += f"**Time:** {timestamp}\n\n"
            alert_msg += f"**Message:**\n{message}\n\n"
            alert_msg += f"🤖 **Source:** Bot 9 Auto-Healer"
            
            await bot.send_message(MASTER_ADMIN_ID, alert_msg, parse_mode="Markdown")
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    
    async def send_error_notification(self, error_title: str, error_message: str, stack_trace: str = None):
        """Send instant error notification"""
        try:
            if not ERROR_NOTIFICATION_ENABLED:
                return
            
            # Rate limit error notifications (max 1 per minute for same error)
            now = datetime.now()
            if self.last_error_notification:
                time_diff = (now - self.last_error_notification).total_seconds()
                if time_diff < 60:
                    return
            
            self.last_error_notification = now
            self.error_count += 1
            
            timestamp = now.strftime("%Y-%m-%d %I:%M:%S %p")
            
            error_msg = f"🚨 **BOT 9 ERROR ALERT**\n\n"
            error_msg += f"**Error #{self.error_count}**\n"
            error_msg += f"**Time:** {timestamp}\n\n"
            error_msg += f"**Title:** {error_title}\n\n"
            error_msg += f"**Message:**\n`{error_message[:500]}`\n\n"
            
            if stack_trace and CRITICAL_ERROR_NOTIFY_IMMEDIATELY:
                error_msg += f"**Stack Trace:**\n```\n{stack_trace[:500]}\n```\n\n"
            
            error_msg += f"💡 **System Status:** {'Healthy' if self.is_healthy else 'Degraded'}\n"
            error_msg += f"📊 **Total Errors:** {self.error_count}"
            
            await bot.send_message(MASTER_ADMIN_ID, error_msg, parse_mode="Markdown")
            
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")
    
    async def log_system_metrics(self):
        """Log system metrics periodically"""
        try:
            uptime = datetime.now() - self.system_metrics["uptime_start"]
            memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            cpu_percent = psutil.cpu_percent(interval=1)
            
            logger.info(
                f"📊 System Metrics - Uptime: {uptime}, "
                f"Memory: {memory_mb:.2f}MB, CPU: {cpu_percent}%, "
                f"Requests: {self.system_metrics['total_requests']}, "
                f"Errors: {self.system_metrics['total_errors']}"
            )
        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")

# Initialize health monitor
health_monitor = HealthMonitor()

# ==========================================
# STATE PERSISTENCE SYSTEM
# ==========================================

class StatePersistence:
    """Persistent state management for bot recovery"""
    
    def __init__(self):
        self.state_file = os.path.join(STATE_BACKUP_LOCATION, "bot9_state.pkl")
        os.makedirs(STATE_BACKUP_LOCATION, exist_ok=True)
        logger.info("✅ State Persistence initialized")
    
    async def save_state(self):
        """Save bot state to disk"""
        try:
            if not STATE_BACKUP_ENABLED:
                return
            
            state_data = {
                "timestamp": datetime.now().isoformat(),
                "health_metrics": health_monitor.system_metrics,
                "error_count": health_monitor.error_count,
                "last_backup": datetime.now().isoformat()
            }
            
            with open(self.state_file, 'wb') as f:
                pickle.dump(state_data, f)
            
            logger.debug(f"State saved at {datetime.now()}")
            
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    async def load_state(self):
        """Load bot state from disk"""
        try:
            if not AUTO_RESUME_ON_STARTUP or not os.path.exists(self.state_file):
                return None
            
            with open(self.state_file, 'rb') as f:
                state_data = pickle.load(f)
            
            logger.info(f"✅ State restored from {state_data['timestamp']}")
            return state_data
            
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return None

# Initialize state persistence
state_persistence = StatePersistence()

# Helper to Log User Action
def log_user_action(user, action, details=None):
    """Log user action to file"""
    try:
        username = user.username or user.first_name or "Unknown"
        log_msg = f"User: {username} ({user.id}) | Action: {action}"
        if details:
            log_msg += f" | Details: {details}"
        logging.info(log_msg)
    except Exception as e:
        logging.error(f"Error logging action: {e}")

# --- Authorization and Ban Management ---

# Permission Constants matching Menu Buttons
PERMISSIONS = {
    "can_list": "📋 LIST",
    "can_add": "➕ ADD",
    "can_search": "🔍 SEARCH",
    "can_links": "🔗 LINKS",
    "can_analytics": "📊 ANALYTICS",
    "can_diagnosis": "🩺 DIAGNOSIS",
    "can_terminal": "🖥️ TERMINAL",
    "can_backup": "💾 BACKUP DATA",
    "can_manage_admins": "👥 ADMINS",
    "can_reset": "⚠️ RESET BOT DATA"
}

# Reverse map for easy lookup
PERMISSION_KEYS = {v: k for k, v in PERMISSIONS.items()}

# Default SAFE Permissions (Exclude dangerous features)
DEFAULT_SAFE_PERMISSIONS = [
    k for k in PERMISSIONS.keys() 
    if k not in ["can_manage_admins", "can_reset"]
]

# Role Definitions
ROLES = {
    "OWNER": list(PERMISSIONS.keys()), # Full Access
    "MANAGER": [
        "can_list", "can_add", "can_search", "can_links", 
        "can_analytics", "can_manage_admins", "can_backup" 
        # No Reset, No Diagnosis, No Terminal
    ],
    "ADMIN": [
        "can_list", "can_add", "can_search", "can_links",
        "can_analytics" 
        # No Admin Management
    ],
    "MODERATOR": [
        "can_list", "can_add", "can_search", "can_links"
        # No Admin/Analytics
    ],
    "SUPPORT": [
        "can_list", "can_search"
        # Read Only
    ]
}

def is_admin(user_id: int) -> bool:
    """Check if user is MASTER_ADMIN or in bot9_admins collection"""
    global MASTER_ADMIN_ID # Allow global update
    if user_id == MASTER_ADMIN_ID:
        return True
    
    admin = col_admins.find_one({"user_id": user_id})
    if admin:
        # CRITICAL FIX: Respect Lock Status
        if admin.get("is_locked", False):
            return False
            
        # Check if this admin is marked as OWNER in DB
        if admin.get("is_owner", False):
            # If DB says owner, but Env Var doesn't match, trust DB logic (Bot restart might reset Env)
            # But effectively this user IS an admin.
            return True
        return True
    return False

def has_permission(user_id: int, required_perm: str) -> bool:
    """
    Check if admin has specific permission.
    MASTER_ADMIN always has ALL permissions.
    """
    if user_id == MASTER_ADMIN_ID:
        return True
        
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        return False
        
    # If no permissions array exists, default to ALL ALLOWED (for backward compatibility/initial setup)
    # OR change to False if you want strict default deny. 
    # User requested: "if clicked permission please display available admin... selected menu buttons will be only available"
    # This implies we should default to Empty or All? 
    # Let's default to ALL permissions if the field is missing, so we don't break existing admins immediately.
    # The permission editor will allow revoking.
    current_perms = admin.get("permissions")
    
    if current_perms is None:
        return True # Default allow if not configured yet
        
    return required_perm in current_perms  # FIXED: Was required_permission

def is_banned(user_id: int) -> bool:
    """Check if user is banned"""
    if user_id == MASTER_ADMIN_ID or user_id == OWNER_ID:
        return False
    
    # Exempt all admins from bans (prevents auto-ban from locking them out)
    # To ban an admin, first remove them from admin list.
    if is_admin(user_id):
        return False
        
    banned = col_banned_users.find_one({"user_id": user_id})
    return banned is not None

def ban_user(user_id: int, user_name: str, username: str, reason: str):
    """Ban a user and log the action"""
    try:
        col_banned_users.insert_one({
            "user_id": user_id,
            "user_name": user_name,
            "username": username,
            "banned_by": "SYSTEM",
            "banned_at": datetime.now(),
            "reason": reason,
            "status": "banned"
        })
        logger.info(f"User {user_id} banned: {reason}")
    except Exception as e:
        logger.error(f"Failed to ban user {user_id}: {e}")

def format_datetime_12h(dt: datetime) -> str:
    """Format datetime to 12-hour AM/PM format"""
    if not dt:
        return "N/A"
    return dt.strftime("%b %d, %Y %I:%M %p")

async def check_spam_and_ban(user_id: int, user_name: str, username: str, action: str) -> tuple:
    """
    Check if user is spamming and auto-ban if threshold exceeded.
    Returns (was_banned: bool, attempt_count: int)
    Uses environment variables: RATE_LIMIT_SPAM_THRESHOLD, RATE_LIMIT_SPAM_WINDOW_SECONDS
    """
    from datetime import timedelta
    
    try:
        # Record this attempt
        col_user_activity.insert_one({
            "user_id": user_id,
            "timestamp": datetime.now(),
            "action": action
        })
        
        # Count attempts in configured window
        window_ago = datetime.now() - timedelta(seconds=RATE_LIMIT_SPAM_WINDOW_SECONDS)
        attempt_count = col_user_activity.count_documents({
            "user_id": user_id,
            "timestamp": {"$gte": window_ago}
        })
        
        # Auto-ban if threshold exceeded
        if attempt_count >= RATE_LIMIT_SPAM_THRESHOLD:
            # Check if already banned
            if not is_banned(user_id):
                reason = f"Automated: Spam detection ({attempt_count} unauthorized attempts in {RATE_LIMIT_SPAM_WINDOW_SECONDS} seconds)"
                ban_user(user_id, user_name, username, reason)
                
                # Notify admin about auto-ban
                await notify_admin_auto_ban(user_id, user_name, username, attempt_count)
                
                return True, attempt_count
        
        return False, attempt_count
        
    except Exception as e:
        logger.error(f"Error in spam check: {e}")
        await health_monitor.send_error_notification(
            "Spam Detection Error",
            str(e),
            traceback.format_exc()
        )
        return False, 0

async def log_unauthorized_access(user, action: str, attempt_count: int):
    """Log unauthorized access attempt and notify admin"""
    try:
        # Log to file
        log_user_action(user, f"UNAUTHORIZED ACCESS: {action}", f"Attempt #{attempt_count}")
        
        # Notify admin
        await notify_admin_unauthorized_access(user, action, attempt_count)
    except Exception as e:
        logger.error(f"Failed to log unauthorized access: {e}")

async def notify_admin_unauthorized_access(user, action: str, attempt_count: int):
    """Send unauthorized access report to master admin"""
    try:
        timestamp = format_datetime_12h(datetime.now())
        username = user.username or "No username"
        full_name = user.full_name or "Unknown"
        
        msg = (
            f"🚨 **UNAUTHORIZED ACCESS ATTEMPT**\n\n"
            f"👤 **User ID**: `{user.id}`\n"
            f"📝 **Username**: @{username}\n"
            f"👨 **Name**: {full_name}\n"
            f"🕐 **Time**: {timestamp}\n"
            f"🎯 **Action**: {action}\n"
            f"🔢 **Attempt**: #{attempt_count}\n\n"
            f"⚠️ **Status**: Access denied (non-admin)"
        )
        
        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Failed to notify admin about unauthorized access: {e}")

async def notify_admin_auto_ban(user_id: int, user_name: str, username: str, spam_count: int):
    """Notify admin about auto-ban"""
    try:
        timestamp = format_datetime_12h(datetime.now())
        
        msg = (
            f"🚫 **AUTO-BAN TRIGGERED**\n\n"
            f"👤 **User ID**: `{user_id}`\n"
            f"📝 **Username**: @{username or 'None'}\n"
            f"👨 **Name**: {user_name or 'Unknown'}\n"
            f"🕐 **Time**: {timestamp}\n"
            f"⚠️ **Reason**: Spam detected\n"
            f"📊 **Attempts**: {spam_count} in 30 seconds\n\n"
            f"🔇 User will receive NO responses (silent ban)"
        )
        
        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Failed to notify admin about auto-ban: {e}")

async def check_authorization(message: types.Message, action_name: str = "access bot", required_perm: str = None) -> bool:
    """
    Universal authorization check for all handlers.
    OPTIMIZED: Single DB query for admins.
    
    Checks:
    1. Master/Owner -> Bypass
    2. Fetch Admin Doc -> Check Lock, Update Info, Check Perms
    3. If Not Admin -> Check Ban, Check Spam/Log
    """
    user_id = message.from_user.id
    
    # 0. Master Admin / Owner Bypass (Always allowed, never banned)
    if user_id == MASTER_ADMIN_ID or user_id == OWNER_ID:
        return True
    
    # 1. Fetch Admin Doc ONCE
    admin_doc = col_admins.find_one({"user_id": user_id})

    # 2. Process Admin
    if admin_doc:
        # A. Check Lock Status
        if admin_doc.get("is_locked", False):
            # Locked admins cannot access anything.
            return False
            
        # B. Update Admin Info (Keep DB Fresh)
        # Check against current to minimize writes (optimization)
        current_name = admin_doc.get("full_name")
        current_username = admin_doc.get("username")
        
        if current_name != message.from_user.full_name or current_username != message.from_user.username:
            try:
                col_admins.update_one(
                    {"user_id": user_id},
                    {"$set": {
                        "full_name": message.from_user.full_name,
                        "username": message.from_user.username,
                        "last_active": datetime.now()
                    }}
                )
            except Exception as e:
                logger.error(f"Failed to update admin info: {e}")

        # C. Check Permission (if required)
        if required_perm:
            # If permissions not set, allow all (backward compatibility)
            perms = admin_doc.get("permissions")
            if perms is not None and required_perm not in perms:
                await message.answer("⛔ **ACCESS DENIED**\n\nYou do not have permission to access this feature.")
                logger.warning(f"Admin {user_id} denied access to {action_name} (Missing: {required_perm})")
                return False
        
        return True

    # 3. Process Non-Admin (If we are here, admin_doc is None)
    
    # A. Check ban status
    # We only check this for non-admins because admins are exempt in is_banned() anyway
    banned = col_banned_users.find_one({"user_id": user_id})
    if banned:
        return False  # No response at all for banned users

    # B. Non-admin unauthorized access logic
    was_banned, attempt_count = await check_spam_and_ban(
        user_id,
        message.from_user.full_name,
        message.from_user.username,
        action_name
    )
    
    if was_banned:
        return False  # Silent ban, no response
    
    # Log unauthorized access attempt
    await log_unauthorized_access(
        message.from_user,
        action_name,
        attempt_count
    )
    
    return False  # BLOCK - No access for non-admins

# ==========================================
# BOT AND DATABASE SETUP (ENTERPRISE)
# ==========================================

# Bot Setup
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Database Connection with Enterprise Configuration
try:
    print("🔌 Connecting to MongoDB...")
    client = pymongo.MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
        connectTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
        maxPoolSize=MONGO_MAX_POOL_SIZE,
        minPoolSize=MONGO_MIN_POOL_SIZE,
        retryWrites=True,
        retryReads=True
    )
    db = client[MONGO_DB_NAME]
    
    # Bot9 Management Collections
    col_logs = db["bot9_logs"]
    col_pdfs = db["bot9_pdfs"]
    col_ig_content = db["bot9_ig_content"]
    col_settings = db["bot9_settings"]
    col_admins = db["bot9_admins"]
    col_banned_users = db["bot9_banned_users"]
    col_user_activity = db["bot9_user_activity"]
    col_backups = db["bot9_backups"]  # Backup history collection
    
    # Test connection
    client.admin.command('ping')
    print("✅ Connected to MongoDB")
    print(f"   Database: {MONGO_DB_NAME}")
    print(f"   Connection Pool: {MONGO_MIN_POOL_SIZE}-{MONGO_MAX_POOL_SIZE}")
    
    # Create indexes for better performance (idempotent - skips if exists)
    def create_index_safe(collection, keys, **kwargs):
        """Helper to create index safely, ignoring if already exists"""
        try:
            collection.create_index(keys, **kwargs)
        except Exception as e:
            # Ignore if index already exists with different options
            if "already exists" not in str(e).lower() and "indexkeyspecsconflict" not in str(e).lower():
                raise
    
    try:
        print("🔍 Creating database indexes...")
        # Basic indexes with explicit names to avoid conflicts
        create_index_safe(col_pdfs, "index", unique=True, name="pdf_index_unique")
        create_index_safe(col_pdfs, "created_at", name="pdf_created_at")
        create_index_safe(col_pdfs, "msa_code", sparse=True, name="pdf_msa_code")
        create_index_safe(col_ig_content, "created_at", name="ig_created_at")
        create_index_safe(col_ig_content, "cc_number", unique=True, name="ig_cc_number_unique")
        create_index_safe(col_logs, "timestamp", name="log_timestamp")
        
        # Ban and activity tracking indexes (security)
        create_index_safe(col_banned_users, "user_id", unique=True, name="banned_user_id_unique")
        create_index_safe(col_user_activity, [("user_id", 1), ("timestamp", -1)], name="activity_user_time")
        create_index_safe(col_user_activity, [("timestamp", -1)], name="activity_timestamp")
        
        # Analytics performance indexes (for scalability with millions of records)
        create_index_safe(col_pdfs, [("clicks", -1)], sparse=True, name="pdf_clicks_desc")
        create_index_safe(col_pdfs, [("affiliate_clicks", -1)], sparse=True, name="pdf_aff_clicks_desc")
        create_index_safe(col_pdfs, [("ig_start_clicks", -1)], sparse=True, name="pdf_ig_clicks_desc")
        create_index_safe(col_pdfs, [("yt_start_clicks", -1)], sparse=True, name="pdf_yt_clicks_desc")
        create_index_safe(col_pdfs, [("yt_code_clicks", -1)], sparse=True, name="pdf_yt_code_clicks_desc")
        create_index_safe(col_ig_content, [("ig_cc_clicks", -1)], sparse=True, name="ig_cc_clicks_desc")
        
        # Compound indexes for filtered analytics queries
        create_index_safe(col_pdfs, [("affiliate_link", 1), ("affiliate_clicks", -1)], name="pdf_aff_link_clicks")
        create_index_safe(col_pdfs, [("ig_start_code", 1), ("ig_start_clicks", -1)], name="pdf_ig_code_clicks")
        create_index_safe(col_pdfs, [("yt_link", 1), ("yt_start_clicks", -1)], name="pdf_yt_link_clicks")
        create_index_safe(col_pdfs, [("msa_code", 1), ("yt_code_clicks", -1)], name="pdf_msa_yt_clicks")
        
        # Backup collection indexes
        create_index_safe(col_backups, "created_at", name="backup_created_at")
        create_index_safe(col_backups, "filename", name="backup_filename")
        
        # Admin collection indexes
        create_index_safe(col_admins, "user_id", unique=True, name="admin_user_id_unique")
        
        print("✅ Database indexes created (optimized for millions of records)")
    except Exception as idx_err:
        print(f"⚠️ Warning: Some indexes could not be created: {idx_err}")
        print("   Bot will continue, existing indexes will be used")
    
    # Initialize click tracking fields for existing documents (migration)
    try:
        print("🔄 Initializing click tracking fields...")
        # Update PDFs without click fields
        pdf_updated = col_pdfs.update_many(
            {"clicks": {"$exists": False}},
            {"$set": {
                "clicks": 0,
                "affiliate_clicks": 0,
                "ig_start_clicks": 0,
                "yt_start_clicks": 0,
                "yt_code_clicks": 0
            }}
        )
        
        # Update IG content without click fields
        ig_updated = col_ig_content.update_many(
            {"ig_cc_clicks": {"$exists": False}},
            {"$set": {"ig_cc_clicks": 0}}
        )
        
        print(f"✅ Click tracking initialized (PDFs: {pdf_updated.modified_count}, IG: {ig_updated.modified_count})")
    except Exception as migration_err:
        print(f"⚠️ Warning: Could not initialize click fields: {migration_err}")
    
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("✅ DATABASE READY FOR ENTERPRISE SCALE")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
except Exception as e:
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("❌ CRITICAL: DATABASE CONNECTION FAILED")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"Error: {e}")
    print(f"MongoDB URI: {MONGO_URI[:20]}..." if MONGO_URI else "MONGO_URI not set!")
    print("\n⚠️ Please check:")
    print("  1. MongoDB is running")
    print("  2. MONGO_URI in BOT9.env is correct")
    print("  3. Network connectivity")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    sys.exit(1)

# --- FSM States ---
class PDFStates(StatesGroup):
    waiting_for_add_name = State()
    waiting_for_add_link = State()
    waiting_for_edit_search = State()
    waiting_for_edit_field = State()
    waiting_for_edit_value = State()
    waiting_for_delete_search = State()
    waiting_for_delete_confirm = State()
    viewing_list = State()

class AffiliateStates(StatesGroup):
    waiting_for_pdf_selection = State()
    waiting_for_link = State()
    waiting_for_delete_confirm = State()
    viewing_list = State()

class AffiliateDeleteStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_confirm = State()

class MSACodeStates(StatesGroup):
    waiting_for_pdf_selection = State()
    waiting_for_code = State()
    viewing_list = State()
    
class MSACodeEditStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_new_code = State()
    
class MSACodeDeleteStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_confirm = State()

class YTStates(StatesGroup):
    waiting_for_title = State()
    waiting_for_link = State()
    waiting_for_pdf_selection = State()
    viewing_list = State()
    
class YTEditStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_field = State()
    waiting_for_value = State()
    
class YTDeleteStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_confirm = State()

class IGStates(StatesGroup):
    waiting_for_content_name = State()
    
class IGEditStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_new_name = State()
    
class IGDeleteStates(StatesGroup):
    waiting_for_selection = State()
    waiting_for_confirm = State()

class IGListStates(StatesGroup):
    viewing = State()

class IGAffiliateStates(StatesGroup):
    waiting_for_ig_selection = State()  # Select IG content for affiliate
    waiting_for_link = State()           # Enter affiliate link

class IGAffiliateEditStates(StatesGroup):
    waiting_for_selection = State()      # Select IG content to edit
    waiting_for_new_link = State()       # Enter new affiliate link

class IGAffiliateDeleteStates(StatesGroup):
    waiting_for_selection = State()      # Select IG content to delete affiliate
    waiting_for_confirm = State()        # Confirm deletion


class ListStates(StatesGroup):
    viewing_all = State()  # For viewing ALL PDFs
    viewing_ig = State()   # For viewing IG content

class SearchStates(StatesGroup):
    viewing_pdf_list = State()       # Viewing paginated PDF list
    waiting_for_pdf_input = State()  # Waiting for PDF index/name
    viewing_ig_list = State()        # Viewing paginated IG list
    waiting_for_ig_input = State()   # Waiting for IG index/CC code

class PDFActionStates(StatesGroup):
    waiting_for_action = State()


class ResetStates(StatesGroup):
    waiting_for_confirm_button = State()
    waiting_for_confirm_text = State()

class AnalyticsStates(StatesGroup):
    viewing_analytics = State()
    viewing_category = State()

class BackupStates(StatesGroup):
    viewing_backup_menu = State()

class AdminManagementStates(StatesGroup):
    waiting_for_new_admin_id = State()
    waiting_for_remove_admin_id = State()
    viewing_admin_list = State()
    waiting_for_ban_user_id = State()
    waiting_for_unban_user_id = State()

class AdminPermissionStates(StatesGroup):
    waiting_for_admin_selection = State()
    configuring_permissions = State()

class AdminRoleStates(StatesGroup):
    waiting_for_admin_selection = State()
    waiting_for_role_selection = State()
    waiting_for_owner_password = State() # For Ownership Transfer
    waiting_for_owner_confirm = State() # For Ownership Transfer Confirm

# --- Helpers ---
def get_cancel_keyboard():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ CANCEL")]], resize_keyboard=True)

async def reindex_all_pdfs():
    """
    Re-index all PDFs to have sequential indices starting from 1, with no gaps.
    This ensures proper ordering after deletions.
    """
    try:
        # Get all PDFs sorted by current index
        all_pdfs = list(col_pdfs.find().sort("index", 1))
        
        # Re-assign indices sequentially
        for new_index, pdf in enumerate(all_pdfs, start=1):
            if pdf["index"] != new_index:
                col_pdfs.update_one(
                    {"_id": pdf["_id"]},
                    {"$set": {"index": new_index}}
                )
        
        logger.info(f"Re-indexed {len(all_pdfs)} PDFs successfully")
        return len(all_pdfs)
    except Exception as e:
        logger.error(f"Error re-indexing PDFs: {e}")
        return 0

async def get_next_pdf_index():
    latest = col_pdfs.find_one(sort=[("index", -1)])
    return (latest["index"] + 1) if latest else 1

def validate_msa_code(code):
    """
    Validates MSA code format: MSA12345 (MSA + exactly 5 digits)
    Returns (is_valid: bool, error_msg: str)
    """
    import re
    if not code:
        return False, "⚠️ Code cannot be empty."
    
    # Check format: MSA followed by exactly 4 digits
    pattern = r'^MSA\d{4}$'
    if not re.match(pattern, code):
        return False, "⚠️ Invalid format. Use: MSA1234 (MSA + 4 digits)"
    
    return True, ""

def is_msa_code_duplicate(code, exclude_pdf_id=None):
    """
    Check if MSA code already exists in database
    exclude_pdf_id: ObjectId to exclude from check (for edit operations)
    """
    query = {"msa_code": code}
    if exclude_pdf_id:
        from bson.objectid import ObjectId
        query["_id"] = {"$ne": ObjectId(exclude_pdf_id)}
    
    return col_pdfs.find_one(query) is not None

def get_next_cc_code():
    """
    Generate next CC code (CC1, CC2, CC3...) with no gaps
    Finds the highest existing CC number and returns next
    """
    # Get all IG content sorted by CC code
    all_content = list(col_ig_content.find().sort("cc_number", 1))
    
    if not all_content:
        return "CC1", 1
    
    # Get the highest CC number
    highest = max(content['cc_number'] for content in all_content)
    next_number = highest + 1
    return f"CC{next_number}", next_number

def is_ig_name_duplicate(name, exclude_id=None):
    """
    Check if IG content name already exists
    exclude_id: ObjectId to exclude from check (for edit operations)
    """
    query = {"name": {"$regex": f"^{name}$", "$options": "i"}}  # Case-insensitive exact match
    if exclude_id:
        from bson.objectid import ObjectId
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    
    return col_ig_content.find_one(query) is not None

async def send_ig_list_view(message: types.Message, page=0, mode="list"):
    """
    Display paginated IG content list
    modes: 'list', 'edit', 'delete', 'ig_affiliate_select', 'ig_affiliate_edit', 'ig_affiliate_delete'
    """
    limit = 5  # Changed to 5 items per page
    skip = page * limit
    
    # Filter based on mode
    if mode == "ig_affiliate_select":
        # Show ONLY content that does NOT have an affiliate link
        query = {"$or": [{"affiliate_link": {"$exists": False}}, {"affiliate_link": ""}]}
    elif mode in ["ig_affiliate_edit", "ig_affiliate_delete"]:
        query = {"affiliate_link": {"$exists": True, "$ne": ""}}
    else:
        query = {}
    
    total = col_ig_content.count_documents(query)
    cursor = col_ig_content.find(query).sort("cc_number", 1).skip(skip).limit(limit)
    contents = list(cursor)
    
    # Header & Keyboard Setup
    if mode == "edit":
        title = "✏️ **EDIT IG CONTENT** - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "delete":
        title = "🗑️ **DELETE IG CONTENT** - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "ig_affiliate_select":
        title = "📎 **SELECT IG FOR AFFILIATE** - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "ig_affiliate_edit":
        title = "✏️ **EDIT AFFILIATE LINK** - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "ig_affiliate_delete":
        title = "🗑️ **DELETE AFFILIATE LINK** - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    else:
        title = "📸 **IG CONTENT LIST**"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO IG MENU")
    
    if not contents:
        msg = "⚠️ **No IG Content found.**\nAdd one first!"
        await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=[[cancel_btn]], resize_keyboard=True), parse_mode="Markdown")
        return
    
    text = f"{title} (Page {page+1})\nResult {skip+1}-{min(skip+len(contents), total)} of {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    for idx, content in enumerate(contents, start=1):
        display_index = skip + idx
        # Display index, CC code, and content name
        content_name = content.get('name', 'Unnamed')
        if len(content_name) > 25:
            content_name = content_name[:25] + "..."
        text += f"{display_index}. {content['cc_code']} - {content_name}"
        
        # Affiliate Status Logic
        has_affiliate = content.get("affiliate_link", "") != ""
        if mode in ["ig_affiliate_select", "ig_affiliate_edit", "ig_affiliate_delete"]:
             text += f" {'🔗' if has_affiliate else '⚠️'}"
        else:
             # Normal modes: Show explicit status
             status = "✅" if has_affiliate else "❌"
             text += f" | Aff: {status}"
        
        text += "\n"
    
    # Pagination
    buttons = []
    nav_prefix = "_IG" if mode == "list" else ""
    # Use appropriate prefix for other modes if needed, but 'list' is main one using state?
    # Actually ig_pagination_handler looks for "_IG" if mode="list".
    # For other modes (interactive), we might need logic?
    # Current code handles pagination via buttons?
    # Let's check logic for prev/next buttons.
    
    if page > 0: buttons.append(KeyboardButton(text=f"⬅️ PREV{nav_prefix} {page}"))
    if (skip + limit) < total: buttons.append(KeyboardButton(text=f"➡️ NEXT{nav_prefix} {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([cancel_btn])
    
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
             await message.answer(part, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")
    else:
        await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")

async def send_pdf_list_view(message: types.Message, page=0, mode="list"):
    """
    Helper to display paginated PDF list.
    modes: 'list', 'edit', 'delete', 'affiliate_select', 'affiliate_delete', 
           'msa_add_select', 'msa_edit_select', 'msa_delete', 'list_msa',
           'yt_add_select', 'yt_edit_select', 'yt_delete', 'list_yt'
    """
    limit = 5  # User requested 5 at a time
    skip = page * limit
    
    # query modification based on mode
    query = {}
    if mode == "affiliate_delete" or mode == "list_affiliate" or mode == "affiliate_edit_select":
        query = {"affiliate_link": {"$exists": True, "$ne": ""}}
    elif mode == "affiliate_add_select":
        # PDFs that DO NOT have an affiliate link (field missing, null, or empty string)
        query = {"$or": [
            {"affiliate_link": {"$exists": False}},
            {"affiliate_link": None},
            {"affiliate_link": ""}
        ]}
    elif mode == "msa_add_select":
        # PDFs that DO NOT have MSA code
        query = {"$or": [
            {"msa_code": {"$exists": False}},
            {"msa_code": None},
            {"msa_code": ""}
        ]}
    elif mode == "msa_edit_select" or mode == "msa_delete" or mode == "list_msa":
        # PDFs that HAVE MSA code
        query = {"msa_code": {"$exists": True, "$ne": ""}}
    elif mode == "yt_add_select":
        # PDFs that DO NOT have YT data
        query = {"$or": [
            {"yt_title": {"$exists": False}},
            {"yt_title": None},
            {"yt_title": ""}
        ]}
    elif mode == "yt_edit_select" or mode == "yt_delete" or mode == "list_yt":
        # PDFs that HAVE YT data
        query = {"yt_title": {"$exists": True, "$ne": ""}}

    total = col_pdfs.count_documents(query)
    
    cursor = col_pdfs.find(query).sort("index", 1).skip(skip).limit(limit)
    pdfs = list(cursor)
    
    # Header & Keyboard Setup
    if mode == "edit":
        title = "✏️ **EDIT PDF** - Select by Index or Name"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "delete":
        title = "🗑️ **DELETE PDF** - Select by Index or Name"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "affiliate_add_select":
        title = "💸 **ADD AFFILIATE LINK** - Select PDF (No Link)"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "affiliate_edit_select":
        title = "✏️ **EDIT AFFILIATE LINK** - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "affiliate_delete":
        title = "🗑️ **DELETE AFFILIATE LINK** - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "list_affiliate":
        title = "💸 **AFFILIATE LINKS LIST**"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO AFFILIATE MENU")
    elif mode == "msa_add_select":
        title = "🔑 **ADD MSA CODE** - Select PDF (No Code)"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "msa_edit_select":
        title = "✏️ **EDIT MSA CODE** - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "msa_delete":
        title = "🗑️ **DELETE MSA CODE** - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "list_msa":
        title = "🔑 **MSA CODES LIST**"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO CODE MENU")
    elif mode == "yt_add_select":
        title = "▶️ **ADD YT LINK** - Select PDF (No YT)"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "yt_edit_select":
        title = "✏️ **EDIT YT LINK** - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "yt_delete":
        title = "🗑️ **DELETE YT LINK** - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "list_yt":
        title = "▶️ **YT LINKS LIST**"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO YT MENU")
    else:
        title = "📂 **PDF LIST**"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO PDF MENU")

    if not pdfs:
        msg = f"📂 No PDFs found matching criteria.\nTotal: {total}"
        if mode == "affiliate_add_select":
            msg = "⚠️ **All existing PDFs already have Affiliate Links!**\nPlease add a new PDF first."
        elif mode == "affiliate_edit_select":
            msg = "⚠️ **No Affiliate Links found to edit.**\nAdd one first!"
        elif mode == "msa_add_select":
            msg = "⚠️ **All existing PDFs already have MSA Codes!**\nPlease add a new PDF first."
        elif mode == "msa_edit_select":
            msg = "⚠️ **No MSA Codes found to edit.**\nAdd one first!"
        elif mode == "msa_delete" or mode == "list_msa":
            msg = "⚠️ **No MSA Codes found.**\nAdd one first!"
        elif mode == "yt_add_select":
            msg = "⚠️ **All existing PDFs already have YT Links!**\nPlease add a new PDF first."
        elif mode == "yt_edit_select":
            msg = "⚠️ **No YT Links found to edit.**\nAdd one first!"
        elif mode == "yt_delete" or mode == "list_yt":
            msg = "⚠️ **No YT Links found.**\nAdd one first!"
            
        await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=[[cancel_btn]], resize_keyboard=True), parse_mode="Markdown")
        return

    text = f"{title} (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    
    # Use sequential numbering for list modes, actual index for others
    use_sequential = mode in ["list_affiliate", "list_msa", "list_yt", "yt_delete"]
    
    for idx, pdf in enumerate(pdfs, start=1):
        # Display index: sequential for list modes, actual for operation modes
        display_index = skip + idx if use_sequential else pdf['index']
        
        text += f"{display_index}. {pdf['name']}\n"
        text += f"🔗 Link: {pdf['link']}\n"
        
        # Show different fields based on mode
        if mode.startswith("yt_") or mode == "list_yt":
            # YT modes: Show ONLY Index, PDF Name, PDF Link (NO affiliate or MSA code)
            yt_title = pdf.get('yt_title', 'Not Set')
            yt_link = pdf.get('yt_link', 'Not Set')
            # Always show YT Title and Link (even if "Not Set")
            text += f"▶️ YT Title: {yt_title}\n"
            text += f"🔗 YT Link: {yt_link}\n\n"
        elif mode.startswith("msa_") or mode == "list_msa":
            aff_link = pdf.get('affiliate_link', 'Not Set')
            text += f"💸 AFF LINK: {aff_link}\n"
            
            # Show MSA Code if it exists
            msa_code = pdf.get('msa_code', 'Not Set')
            text += f"🔑 MSA CODE: {msa_code}\n\n"
        else:
            # Show Affiliate Link (Always, as requested)
            aff_link = pdf.get('affiliate_link', 'Not Set')
            text += f"💸 AFF LINK: {aff_link}\n\n"
    
    # Pagination
    buttons = []
    # Prefix mapping for different modes to handle callbacks/text correctly if needed
    # But since we use text-based handling, we just need unique text or state-based handling.
    # We will use "PREV" and "NEXT" generally, and the handlers will interpret based on state.
    # EXCEPT for strict list modes where there is no state.
    
    nav_prefix = ""
    if mode == "list_affiliate":
        nav_prefix = "_AFF"
    elif mode == "list_msa":
        nav_prefix = "_MSA"
    elif mode == "list_yt":
        nav_prefix = "_YT"
    
    if page > 0: buttons.append(KeyboardButton(text=f"⬅️ PREV{nav_prefix} {page}"))
    if (skip + limit) < total: buttons.append(KeyboardButton(text=f"➡️ NEXT{nav_prefix} {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([cancel_btn])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown", disable_web_page_preview=True)



# ... (Existing Keyboards and Handlers) ...

def get_main_menu(user_id: int):
    """Bot 9 Main Menu Structure - Dynamically Filtered"""
    
    # 1. Master Admin sees EVERYTHING
    if user_id == MASTER_ADMIN_ID:
        keyboard = [
            [KeyboardButton(text="📋 LIST"), KeyboardButton(text="➕ ADD")],
            [KeyboardButton(text="🔍 SEARCH"), KeyboardButton(text="🔗 LINKS")],
            [KeyboardButton(text="📊 ANALYTICS"), KeyboardButton(text="🩺 DIAGNOSIS")],
            [KeyboardButton(text="🖥️ TERMINAL"), KeyboardButton(text="💾 BACKUP DATA")],
            [KeyboardButton(text="👥 ADMINS"), KeyboardButton(text="⚠️ RESET BOT DATA")],
            [KeyboardButton(text="📚 BOT GUIDE")]
        ]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

    # 2. Check Admin Permissions
    admin = col_admins.find_one({"user_id": user_id})
    if not admin:
        # Fallback for non-admins (Access Control should block them anyway)
        return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📚 BOT GUIDE")]], resize_keyboard=True)
        
    perms = admin.get("permissions")
    
    # If permissions are NOT set (None) -> Default to SAFE ACCESS (No Admin/Reset)
    if perms is None:
        perms = DEFAULT_SAFE_PERMISSIONS
        
    # 3. Filter Buttons based on Permissions
    buttons = []
    
    # Define mapping and order
    # Row 1
    if "can_list" in perms: buttons.append("📋 LIST")
    if "can_add" in perms: buttons.append("➕ ADD")
    
    # Row 2
    if "can_search" in perms: buttons.append("🔍 SEARCH")
    if "can_links" in perms: buttons.append("🔗 LINKS")
    
    # Row 3
    if "can_analytics" in perms: buttons.append("📊 ANALYTICS")
    if "can_diagnosis" in perms: buttons.append("🩺 DIAGNOSIS")
    
    # Row 4
    if "can_terminal" in perms: buttons.append("🖥️ TERMINAL")
    if "can_backup" in perms: buttons.append("💾 BACKUP DATA")
    
    # Row 5 (Admins / Reset)
    if "can_manage_admins" in perms: buttons.append("👥 ADMINS")
    if "can_reset" in perms: buttons.append("⚠️ RESET BOT DATA")
    
    # Always add Guide
    buttons.append("📚 BOT GUIDE")
    
    # Build Keyboard (Two columns)
    keyboard = []
    row = []
    for btn_text in buttons:
        row.append(KeyboardButton(text=btn_text))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
        
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_add_menu():
    """Add Menu Structure"""
    keyboard = [
        [KeyboardButton(text="📄 PDF"), KeyboardButton(text="💸 AFFILIATE")],
        [KeyboardButton(text="🔑 CODE"), KeyboardButton(text="▶️ YT")],
        [KeyboardButton(text="📸 IG")],
        [KeyboardButton(text="⬅️ BACK TO MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_pdf_menu():
    """PDF Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD PDF"), KeyboardButton(text="✏️ EDIT PDF")],
        [KeyboardButton(text="🗑️ DELETE PDF"), KeyboardButton(text="📋 LIST PDF")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_affiliate_menu():
    """Affiliate Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD AFFILIATE"), KeyboardButton(text="✏️ EDIT AFFILIATE")],
        [KeyboardButton(text="🗑️ DELETE AFFILIATE"), KeyboardButton(text="📋 LIST AFFILIATE")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_code_menu():
    """Code Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD CODE"), KeyboardButton(text="✏️ EDIT CODE")],
        [KeyboardButton(text="🗑️ DELETE CODE"), KeyboardButton(text="📋 LIST CODE")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_yt_menu():
    """YT Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD YT LINK"), KeyboardButton(text="✏️ EDIT YT")],
        [KeyboardButton(text="🗑️ DELETE YT"), KeyboardButton(text="📋 LIST YT")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_links_menu():
    """Links Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="📑 ALL PDF"), KeyboardButton(text="📸 IG CC")],
        [KeyboardButton(text="🏠 HOME YT")],
        [KeyboardButton(text="⬅️ BACK TO MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_admin_config_menu():
    """Admin Configuration Menu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ NEW ADMIN"), KeyboardButton(text="➖ REMOVE ADMIN")],
        [KeyboardButton(text="🔐 PERMISSIONS"), KeyboardButton(text="👔 ROLES")],
        [KeyboardButton(text="🔒 LOCK/UNLOCK"), KeyboardButton(text="🚫 BAN CONFIG")],
        [KeyboardButton(text="📋 LIST ADMINS")],
        [KeyboardButton(text="🏠 MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_ban_config_menu():
    """Ban Configuration Menu Structure"""
    keyboard = [
        [KeyboardButton(text="🚫 BAN USER"), KeyboardButton(text="✅ UNBAN USER")],
        [KeyboardButton(text="📋 LIST BANNED")],
        [KeyboardButton(text="⬅️ BACK TO ADMIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_roles_menu():
    """Roles Menu Structure"""
    keyboard = [
        [KeyboardButton(text="👑 OWNER"), KeyboardButton(text="👨‍💼 MANAGER")],
        [KeyboardButton(text="👔 ADMIN"), KeyboardButton(text="🛡️ MODERATOR")],
        [KeyboardButton(text="👨‍💻 SUPPORT")],
        [KeyboardButton(text="❌ CANCEL")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_lock_menu():
    """Lock/Unlock Menu Structure"""
    keyboard = [
        [KeyboardButton(text="🔒 LOCK"), KeyboardButton(text="🔓 UNLOCK")],
        [KeyboardButton(text="❌ CANCEL")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_analytics_menu():
    """Analytics Menu Structure"""
    keyboard = [
        [KeyboardButton(text="📊 OVERVIEW")],
        [KeyboardButton(text="📄 PDF Clicks"), KeyboardButton(text="💸 Affiliate Clicks")],
        [KeyboardButton(text="📸 IG Start Clicks"), KeyboardButton(text="▶️ YT Start Clicks")],
        [KeyboardButton(text="📸 IG CC Start Clicks"), KeyboardButton(text="🔑 YT Code Start Clicks")],
        [KeyboardButton(text="⬅️ BACK TO MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_backup_menu():
    """Backup Menu Structure"""
    keyboard = [
        [KeyboardButton(text="💾 FULL BACKUP")],
        [KeyboardButton(text="📋 VIEW AS JSON"), KeyboardButton(text="📊 BACKUP STATS")],
        [KeyboardButton(text="⬅️ BACK TO MAIN MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# --- Helpers for Deep Linking ---
def generate_alphanumeric(length=8):
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))

def generate_digits(length=8):
    return "".join(random.choice(string.digits) for _ in range(length))

async def ensure_pdf_codes(pdf):
    """Ensure PDF has all start codes"""
    updates = {}
    if not pdf.get("ig_start_code"):
        updates["ig_start_code"] = generate_alphanumeric(8)
    if not pdf.get("yt_start_code"):
        updates["yt_start_code"] = generate_digits(8)
    if not pdf.get("aff_start_code"):
        updates["aff_start_code"] = generate_digits(8)
    if not pdf.get("orig_start_code"):
        updates["orig_start_code"] = generate_digits(8)
    
    if updates:
        col_pdfs.update_one({"_id": pdf["_id"]}, {"$set": updates})
        return {**pdf, **updates}
    return pdf

async def ensure_ig_cc_code(content):
    """Ensure IG content has start_code"""
    if not content.get("start_code"):
        code = generate_digits(8)
        col_ig_content.update_one({"_id": content["_id"]}, {"$set": {"start_code": code}})
        return {**content, "start_code": code}
    return content

async def get_home_yt_code():
    """Get or create Home YT code"""
    setting = col_settings.find_one({"key": "home_yt_code"})
    if not setting:
        code = generate_digits(8)
        col_settings.insert_one({"key": "home_yt_code", "value": code})
        return code
    return setting["value"]

def get_ig_menu():
    """IG Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD IG"), KeyboardButton(text="✏️ EDIT IG")],
        [KeyboardButton(text="🗑️ DELETE IG"), KeyboardButton(text="📎 ADD AFFILIATE")],
        [KeyboardButton(text="📋 LIST IG"), KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_ig_affiliate_menu():
    """IG Affiliate Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="📎 Add"), KeyboardButton(text="✏️ Edit")],
        [KeyboardButton(text="🗑️ Delete"), KeyboardButton(text="📋 List")],
        [KeyboardButton(text="◀️ Back")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


# --- Handlers ---

# --- GLOBAL PRIORITY HANDLER FOR RETURN BACK ---
@dp.message(F.text == "⬅️ RETURN BACK")
@dp.message(F.text.contains("BACK TO ADMIN MENU"))
async def global_return_back(message: types.Message, state: FSMContext):
    """Global handler for Return Back button to bypass any state issues"""
    # Authorization check
    if not await check_authorization(message, "Global Return Back", "can_manage_admins"):
        return
        
    await state.clear()
    await message.answer(
        "🔐 **Admin Management**\nSelect an option below:",
        reply_markup=get_admin_config_menu(),
        parse_mode="Markdown"
    )

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Start command handler - ADMIN ONLY"""
    user_id = message.from_user.id
    
    # 1. Check ban status - SILENT ignore
    if is_banned(user_id):
        return  # No response at all
    
    # 2. Check if user is admin
    if not is_admin(user_id):
        # Non-admin attempting to access bot9
        was_banned, attempt_count = await check_spam_and_ban(
            user_id,
            message.from_user.full_name,
            message.from_user.username,
            "/start command"
        )
        if was_banned:
            return  # Silent ban, no response
        
        # Log unauthorized access attempt
        await log_unauthorized_access(
            message.from_user,
            "/start command",
            attempt_count
        )
        return  # BLOCK - No access for non-admins
    
    # 3. Admin verified - Continue with normal handler logic
    log_user_action(message.from_user, "Started Bot")
    
    await message.answer(
        "🤖 **BOT 9 ONLINE**\n"
        "System Authorized. Accessing Mainframe...",
        reply_markup=get_main_menu(message.from_user.id),
        parse_mode="Markdown"
    )

@dp.message(F.text == "⬅️ BACK TO MAIN MENU")
async def back_to_main_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Back to Main Menu"):
        return
    await state.clear()
    await message.answer("🏠 Main Menu", reply_markup=get_main_menu(message.from_user.id))

@dp.message(F.text == "👥 ADMINS")
async def admins_handler(message: types.Message):
    """Show admin configuration menu"""
    if not await check_authorization(message, "Admins Menu"):
        return
    await message.answer(
        "👥 **ADMIN CONFIGURATION**\n\n"
        "Manage bot administrators and permissions:",
        reply_markup=get_admin_config_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🏠 MAIN MENU")
async def main_menu_from_admin_handler(message: types.Message, state: FSMContext):
    """Return to main menu from admin config"""
    if not await check_authorization(message, "Main Menu"):
        return
    await state.clear()
    await message.answer("🏠 Main Menu", reply_markup=get_main_menu(message.from_user.id))

@dp.message(F.text == "➕ NEW ADMIN")
async def new_admin_handler(message: types.Message, state: FSMContext):
    """Ask for new admin's user ID"""
    if not await check_authorization(message, "New Admin"):
        return
    await state.set_state(AdminManagementStates.waiting_for_new_admin_id)
    await message.answer(
        "➕ **ADD NEW ADMIN**\n\n"
        "Please send the **Telegram User ID** of the user you want to add as admin.\n\n"
        "Example: `123456789`",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")]
            ],
            resize_keyboard=True
        ),
        parse_mode="Markdown"
    )

@dp.message(AdminManagementStates.waiting_for_new_admin_id)
async def process_new_admin_id(message: types.Message, state: FSMContext):
    """Process and save new admin ID"""
    if message.text == "🏠 MAIN MENU":
        await state.clear()
        await message.answer("🏠 Main Menu", reply_markup=get_main_menu(message.from_user.id))
        return
    
    # Validate input
    if not message.text.isdigit():
        await message.answer(
            "⚠️ **Invalid Input**\n\n"
            "Please send a valid numeric Telegram User ID.",
            parse_mode="Markdown"
        )
        return
    
    new_admin_id = int(message.text)
    
    # Check if Banned
    if is_banned(new_admin_id):
        await message.answer(
            f"⛔ **User {new_admin_id} is BANNED.**\n\n"
            f"You cannot add a banned user as an Admin.\n"
            f"Please Unban them first from the Ban Menu.",
            reply_markup=get_admin_config_menu(),
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
    # Check for duplicates
    existing = col_admins.find_one({"user_id": new_admin_id})
    if existing:
        await message.answer(
            f"⚠️ **Admin Already Exists**\n\n"
            f"User ID `{new_admin_id}` is already an admin.",
            parse_mode="Markdown"
        )
        return
    
    # Save to database
    # Try to fetch user info
    admin_name = "Unknown"
    admin_username = "Unknown"
    
    try:
        user_chat = await bot.get_chat(new_admin_id)
        admin_name = user_chat.full_name or "Unknown"
        admin_username = user_chat.username or "Unknown"
    except Exception:
        logger.warning(f"Could not fetch info for new admin {new_admin_id}")

    col_admins.insert_one({
        "user_id": new_admin_id,
        "added_by": message.from_user.id,
        "added_at": datetime.now(),
        "permissions": DEFAULT_SAFE_PERMISSIONS, # Set safe defaults explicitly
        "full_name": admin_name,
        "username": admin_username,
        "is_locked": True # Locked by default
    })
    
    await state.clear()
    await message.answer(
        f"✅ **Admin Added Successfully!**\n\n"
        f"User ID: `{new_admin_id}`\n"
        f"Added by: {message.from_user.id}\n\n"
        f"⚠️ **NOTE: New Admins are LOCKED by default.**\n"
        f"Use the Lock Menu to unlock them.",
        reply_markup=get_admin_config_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "➖ REMOVE ADMIN")
async def remove_admin_handler(message: types.Message, state: FSMContext):
    """Show list of admins with pagination"""
    if not await check_authorization(message, "Remove Admin"):
        return
    admins = list(col_admins.find({}))
    
    if not admins:
        await message.answer(
            "⚠️ **No Admins Found**\n\n"
            "There are no admins to remove.\n"
            "Use **➕ NEW ADMIN** to add administrators.",
            parse_mode="Markdown"
        )
        return
    
    # Store current page in state
    await state.set_state(AdminManagementStates.viewing_admin_list)
    await state.update_data(page=0)
    
    # Show first page
    await show_admin_list_page(message, admins, page=0)

async def show_admin_list_page(message: types.Message, admins: list, page: int):
    """Display admin list with pagination"""
    ADMINS_PER_PAGE = 10
    total_pages = (len(admins) - 1) // ADMINS_PER_PAGE + 1
    start_idx = page * ADMINS_PER_PAGE
    end_idx = min(start_idx + ADMINS_PER_PAGE, len(admins))
    
    # Create list text and buttons
    admin_list_text = ""
    keyboard = []
    
    for i in range(start_idx, end_idx):
        admin = admins[i]
        user_id = admin.get("user_id")
        name = admin.get("full_name", "Unknown")
        is_locked = admin.get("is_locked", False)
        status_icon = "🔒" if is_locked else "🔓"
        
        # Add to text list
        admin_list_text += f"{i+1}. **{name}** (`{user_id}`) [{status_icon}]\n"
        
        # Add button
        btn_text = f"❌ Remove: {name} ({user_id})"
        keyboard.append([KeyboardButton(text=btn_text)])
    
    # Add navigation buttons if needed
    nav_buttons = []
    if page > 0:
        nav_buttons.append(KeyboardButton(text="⬅️ PREV"))
    if page < total_pages - 1:
        nav_buttons.append(KeyboardButton(text="➡️ NEXT"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    keyboard.append([KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")])
    
    await message.answer(
        f"➖ **REMOVE ADMIN**\n\n"
        f"Click on an admin to remove them:\n\n"
        f"{admin_list_text}\n"
        f"📊 Page {page + 1}/{total_pages} | Total: {len(admins)} admins",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )

@dp.message(AdminManagementStates.viewing_admin_list)
async def process_admin_removal(message: types.Message, state: FSMContext):
    """Handle admin removal or pagination"""
    if message.text == "🏠 MAIN MENU":
        await state.clear()
        await message.answer("🏠 Main Menu", reply_markup=get_main_menu(message.from_user.id))
        return
    
    data = await state.get_data()
    current_page = data.get("page", 0)
    admins = list(col_admins.find({}))
    
    # Handle pagination
    if message.text == "➡️ NEXT":
        await state.update_data(page=current_page + 1)
        await show_admin_list_page(message, admins, current_page + 1)
        return
    elif message.text == "⬅️ PREV":
        await state.update_data(page=current_page - 1)
        await show_admin_list_page(message, admins, current_page - 1)
        return
    
    # Handle admin removal
    target_id = None
    
    # Regex to extract ID from "❌ Remove: Name (ID)"
    import re
    match = re.search(r"Remove: .* \((\d+)\)$", message.text)
    
    if match: # New format
        target_id = int(match.group(1))
    elif message.text.startswith("❌ Remove Admin: "): # Old/Fallback format
        try:
            target_id = int(message.text.split(":")[-1].strip())
        except ValueError:
            target_id = None
            
    if target_id:
        try:
            # Remove from database
            result = col_admins.delete_one({"user_id": target_id})
            
            if result.deleted_count > 0:
                await state.clear()
                await message.answer(
                    f"✅ **Admin Removed**\n\n"
                    f"User ID `{target_id}` is no longer an admin.\n"
                    f"They cannot access Bot 9 anymore.",
                    reply_markup=get_admin_config_menu(),
                    parse_mode="Markdown"
                )
            else:
                await message.answer("⚠️ Admin not found in database.")
        except Exception as e:
            logger.error(f"Error removing admin: {e}")
            await message.answer("❌ Error removing admin.")
    else:
        await message.answer("⚠️ Invalid selection.")

@dp.message(F.text == "⬅️ BACK TO ADD MENU")
async def back_to_add_handler(message: types.Message):
    await message.answer("➕ **SELECT ADD COMPONENT:**", reply_markup=get_add_menu(), parse_mode="Markdown")

@dp.message(F.text == "➕ ADD")
async def add_menu_handler(message: types.Message):
    """Show Add Submenu"""
    if not await check_authorization(message, "Access Add Menu", "can_add"):
        return
    await message.answer(
        "➕ **SELECT ADD COMPONENT:**",
        reply_markup=get_add_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "📄 PDF")
async def pdf_menu_handler(message: types.Message):
    if not await check_authorization(message, "PDF Menu", "can_add"):
        return
    await message.answer("📄 **PDF MANAGEMENT**", reply_markup=get_pdf_menu(), parse_mode="Markdown")

def is_pdf_name_duplicate(name, exclude_id=None):
    """Check if PDF name already exists (case-insensitive). Returns conflicting PDF or None"""
    query = {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

def is_pdf_link_duplicate(link, exclude_id=None):
    """Check if PDF link already exists. Returns conflicting PDFw or None"""
    query = {"link": link}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

def is_affiliate_link_duplicate(link, exclude_id=None):
    """Check if Affiliate link already exists. Returns conflicting PDF or None"""
    query = {"affiliate_link": link}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

def is_yt_link_duplicate(link, exclude_id=None):
    """Check if YT link already exists. Returns conflicting PDF or None"""
    query = {"yt_link": link}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

def is_yt_title_duplicate(title, exclude_id=None):
    """Check if YT title already exists. Returns conflicting PDF or None"""
    query = {"yt_title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return col_pdfs.find_one(query)

# 3. PDF MANAGEMENT HANDLERS
# ---------------------------------------------------------------------------------------

# 1. ADD PDF
@dp.message(F.text == "➕ ADD PDF")
async def start_add_pdf(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add PDF", "can_add"):
        return
    await state.set_state(PDFStates.waiting_for_add_name)
    await message.answer("📄 **Enter PDF Name:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")

@dp.message(PDFStates.waiting_for_add_name)
async def process_add_pdf_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("📄 **PDF MANAGEMENT**", reply_markup=get_pdf_menu(), parse_mode="Markdown")
    
    name = message.text.strip()
    
    # Validation: Check duplicate name
    conflict_pdf = is_pdf_name_duplicate(name)
    if conflict_pdf:
        await message.answer(f"⚠️ **Name Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different name:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return
        
    await state.update_data(name=name)
    await state.set_state(PDFStates.waiting_for_add_link)
    await message.answer(f"✅ Name set to: **{name}**\n\n🔗 **Enter PDF Link:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")

@dp.message(PDFStates.waiting_for_add_link)
async def process_add_pdf_link(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("📄 **PDF MANAGEMENT**", reply_markup=get_pdf_menu(), parse_mode="Markdown")
    
    link = message.text.strip()
    
    # Validation: Check duplicate link
    conflict_pdf = is_pdf_link_duplicate(link)
    if conflict_pdf:
        await message.answer(f"⚠️ **Link Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return

    data = await state.get_data()
    name = data['name']
    
    # Validation (Basic)
    if "http" not in link and "t.me" not in link:
        await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
        return

    # Save to DB
    idx = await get_next_pdf_index()
    doc = {
        "index": idx,
        "name": name,
        "link": link,
        "created_at": datetime.now(),
        # Initialize click tracking fields
        "clicks": 0,
        "affiliate_clicks": 0,
        "ig_start_clicks": 0,
        "yt_start_clicks": 0,
        "yt_code_clicks": 0,
        # Last clicked timestamps
        "last_clicked_at": None,
        "last_affiliate_click": None,
        "last_ig_click": None,
        "last_yt_click": None,
        "last_yt_code_click": None
    }
    col_pdfs.insert_one(doc)
    
    # Log Action
    log_user_action(message.from_user, "Added PDF", f"Name: {name}, Index: {idx}")

    await state.clear()
    await message.answer(f"✅ **PDF Added!**\n\n🆔 Index: `{idx}`\n📄 Name: `{name}`\n🔗 Link: `{link}`", reply_markup=get_pdf_menu(), parse_mode="Markdown")

# 2. LIST PDF
@dp.message(F.text == "📋 LIST PDF")
async def list_pdfs(message: types.Message, state: FSMContext, page=0):
    if not await check_authorization(message, "List PDF", "can_list"):
        return
    await state.set_state(PDFStates.viewing_list)
    await send_pdf_list_view(message, page=page, mode="list")

@dp.message(F.text == "⬅️ BACK TO PDF MENU")
async def back_to_pdf_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("📄 **PDF MANAGEMENT**", reply_markup=get_pdf_menu(), parse_mode="Markdown")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV ") or m.text.startswith("➡️ NEXT ")))
async def pdf_pagination_handler(message: types.Message, state: FSMContext):
    # Determine mode based on state
    current_state = await state.get_state()
    mode = "list"
    if current_state == PDFStates.waiting_for_edit_search:
        mode = "edit"
    elif current_state == PDFStates.waiting_for_delete_search:
        mode = "delete"
    elif current_state == AffiliateStates.waiting_for_pdf_selection:
        mode = "affiliate_select"
    elif current_state == AffiliateDeleteStates.waiting_for_selection:
        mode = "affiliate_delete"
    elif current_state == MSACodeStates.waiting_for_pdf_selection:
        # Check selection_mode from state data
        data = await state.get_data()
        mode = data.get('selection_mode', 'msa_add_select')
    elif current_state == MSACodeEditStates.waiting_for_selection:
        mode = "msa_edit_select"
    elif current_state == MSACodeDeleteStates.waiting_for_selection:
        mode = "msa_delete"
    elif current_state == YTStates.waiting_for_pdf_selection:
        mode = "yt_add_select"
    elif current_state == YTEditStates.waiting_for_selection:
        mode = "yt_edit_select"
    elif current_state == YTDeleteStates.waiting_for_selection:
        mode = "yt_delete"

    try:
        page_str = message.text.split()[-1]
        page = int(page_str) - 1
        await send_pdf_list_view(message, page=page, mode=mode)
    except:
        await send_pdf_list_view(message, page=0, mode=mode)



# 3. EDIT PDF
@dp.message(F.text == "✏️ EDIT PDF")
async def start_edit_pdf(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit PDF", "can_add"):
        return
    await state.set_state(PDFStates.waiting_for_edit_search)
    await send_pdf_list_view(message, page=0, mode="edit")

@dp.message(PDFStates.waiting_for_edit_search)
async def process_edit_search(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())
    
    # Handle Pagination Interaction within Edit State
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        return await pdf_pagination_handler(message, state)
    
    query = message.text
    # Try Search by Index
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        # Search by Name (Text)
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return

    await state.update_data(edit_id=str(pdf["_id"]), current_name=pdf["name"], current_link=pdf["link"])
    
    # Show Edit Options
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📝 EDIT NAME"), KeyboardButton(text="🔗 EDIT LINK")],
        [KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(PDFStates.waiting_for_edit_field)
    await message.answer(
        f"📄 **PDF FOUND**\n"
        f"🆔 Index: `{pdf['index']}`\n"
        f"📛 Name: {pdf['name']}\n"
        f"🔗 Link: {pdf['link']}\n\n"
        "⬇️ **Select what to edit:**",
        reply_markup=kb,
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

@dp.message(PDFStates.waiting_for_edit_field)
async def process_edit_field(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())
    
    if message.text == "📝 EDIT NAME":
        await state.update_data(field="name")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ **Enter New Name:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
    elif message.text == "🔗 EDIT LINK":
        await state.update_data(field="link")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ **Enter New Link:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
    else:
        await message.answer("⚠️ Invalid Option.")

@dp.message(PDFStates.waiting_for_edit_value)
async def process_edit_value(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())
    
    data = await state.get_data()
    from bson.objectid import ObjectId
    
    field = data['field']
    new_value = message.text.strip()
    
    if field == "name":
        # Check if same as current
        if new_value.lower() == data['current_name'].lower():
            await message.answer(f"⚠️ **Same Name!**\nYou entered the exact same name.\nPlease enter a different name:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
            return

        # Check duplicate name (exclude current PDF)
        conflict_pdf = is_pdf_name_duplicate(new_value, exclude_id=data['edit_id'])
        if conflict_pdf:
            await message.answer(f"⚠️ **Name Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nTry another name:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
            return
            
        col_pdfs.update_one({"_id": ObjectId(data['edit_id'])}, {"$set": {"name": new_value}})
        msg = f"✅ **PDF Name Updated!**\nOld: {data['current_name']}\nNew: {new_value}"
        log_user_action(message.from_user, "Edited PDF Name", f"ID: {data['edit_id']}, New: {new_value}")
    
    elif field == "link":
        # Check if same as current
        if new_value == data['current_link']:
            await message.answer(f"⚠️ **Same Link!**\nYou entered the exact same link.\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
            return

        # Check duplicate link (exclude current PDF)
        conflict_pdf = is_pdf_link_duplicate(new_value, exclude_id=data['edit_id'])
        if conflict_pdf:
            await message.answer(f"⚠️ **Link Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nTry another link:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
            return
            
        # Basic Validation
        if "http" not in new_value and "t.me" not in new_value:
            await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
            return

        col_pdfs.update_one({"_id": ObjectId(data['edit_id'])}, {"$set": {"link": new_value}})
        msg = f"✅ **PDF Link Updated!**\nOld: {data['current_link']}\nNew: {new_value}"
        log_user_action(message.from_user, "Edited PDF Link", f"ID: {data['edit_id']}, New: {new_value}")
    else:
        msg = "⚠️ An unexpected error occurred."

    await state.clear()
    await message.answer(msg, reply_markup=get_pdf_menu(), parse_mode="Markdown")

# 4. DELETE PDF
@dp.message(F.text == "🗑️ DELETE PDF")
async def start_delete_pdf(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete PDF", "can_add"):
        return
    await state.set_state(PDFStates.waiting_for_delete_search)
    await send_pdf_list_view(message, page=0, mode="delete")

@dp.message(PDFStates.waiting_for_delete_search)
async def process_delete_search(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())
    
    # Handle Pagination Interaction within Delete State
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        return await pdf_pagination_handler(message, state)
    
    # Parse input - support comma-separated values
    raw_input = message.text.strip()
    queries = [q.strip() for q in raw_input.split(",")]
    
    # Find all matching PDFs and remove duplicates
    found_pdfs = []
    seen_ids = set()
    not_found = []
    
    for query in queries:
        if not query:  # Skip empty strings
            continue
            
        pdf = None
        if query.isdigit():
            pdf = col_pdfs.find_one({"index": int(query)})
        else:
            # Try exact name match first, then regex
            pdf = col_pdfs.find_one({"name": {"$regex": f"^{re.escape(query)}$", "$options": "i"}})
        
        if pdf:
            # Check for duplicates using _id
            pdf_id = str(pdf["_id"])
            if pdf_id not in seen_ids:
                seen_ids.add(pdf_id)
                found_pdfs.append(pdf)
        else:
            not_found.append(query)
    
    # Handle no results
    if not found_pdfs:
        msg = "❌ **No PDFs Found**\n\n"
        if not_found:
            msg += "Not found:\n" + "\n".join(f"• `{q}`" for q in not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return
    
    # Store delete IDs
    delete_ids = [str(pdf["_id"]) for pdf in found_pdfs]
    await state.update_data(delete_ids=delete_ids)
    
    # Build confirmation message
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(PDFStates.waiting_for_delete_confirm)
    
    # Show what will be deleted
    msg = f"⚠️ **CONFIRM BULK DELETION**\n\n"
    msg += f"📊 **Total to delete: {len(found_pdfs)} PDF(s)**\n\n"
    
    for idx, pdf in enumerate(found_pdfs, 1):
        msg += f"{idx}. `{pdf['index']}` - {pdf['name']}\n"
    
    if not_found:
        msg += f"\n⚠️ **Not Found ({len(not_found)}):**\n"
        msg += "\n".join(f"• `{q}`" for q in not_found[:5])  # Limit to 5
        if len(not_found) > 5:
            msg += f"\n...and {len(not_found) - 5} more"
    
    msg += "\n\n❓ Confirm deletion?"
    
    await message.answer(
        msg,
        reply_markup=kb,
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

@dp.message(PDFStates.waiting_for_delete_confirm)
async def process_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        from bson.objectid import ObjectId
        
        # Check if bulk delete (list of IDs) or single delete (single ID)
        delete_ids = data.get('delete_ids')
        
        if delete_ids:
            # Bulk delete
            object_ids = [ObjectId(id_str) for id_str in delete_ids]
            result = col_pdfs.delete_many({"_id": {"$in": object_ids}})
            deleted_count = result.deleted_count
            
            # Auto re-index remaining PDFs
            await reindex_all_pdfs()
            
            await state.clear()
            await message.answer(
                f"🗑️ **Bulk Deletion Complete**\n\n"
                f"✅ Successfully deleted **{deleted_count} PDF(s)**\n"
                f"📊 Indices automatically reorganized",
                reply_markup=get_pdf_menu(),
                parse_mode="Markdown"
            )
            
            # Log action
            log_user_action(message.from_user, "Bulk Delete PDFs", f"Deleted {deleted_count} PDFs")
        else:
            # Single delete (fallback for compatibility)
            delete_id = data.get('delete_id')
            if delete_id:
                col_pdfs.delete_one({"_id": ObjectId(delete_id)})
                
                # Auto re-index remaining PDFs
                await reindex_all_pdfs()
                
                await state.clear()
                await message.answer(
                    "🗑️ **PDF Deleted Successfully.**\n"
                    "📊 Indices automatically reorganized",
                    reply_markup=get_pdf_menu(),
                    parse_mode="Markdown"
                )
                log_user_action(message.from_user, "Delete PDF", f"ID: {delete_id}")
            else:
                await state.clear()
                await message.answer("❌ Error: No PDFs to delete.", reply_markup=get_pdf_menu())
    else:
        await state.clear()
        await message.answer("❌ Cancelled.", reply_markup=get_pdf_menu())

# ... (Previous Handlers) ...

# --- AFFILIATE HANDLERS ---

@dp.message(F.text == "💸 AFFILIATE")
async def affiliate_menu_handler(message: types.Message):
    await message.answer("💸 **AFFILIATE MANAGEMENT**", reply_markup=get_affiliate_menu(), parse_mode="Markdown")

# --- AFFILIATE HANDLERS ---

# 1. ADD / EDIT AFFILIATE
# Split Handlers

@dp.message(F.text == "➕ ADD AFFILIATE")
async def start_add_affiliate(message: types.Message, state: FSMContext):
    await state.set_state(AffiliateStates.waiting_for_pdf_selection)
    # Mode ensures we only show PDFs WITHOUT links
    await state.update_data(selection_mode="affiliate_add_select")
    await send_pdf_list_view(message, page=0, mode="affiliate_add_select")

@dp.message(F.text == "✏️ EDIT AFFILIATE")
async def start_edit_affiliate(message: types.Message, state: FSMContext):
    await state.set_state(AffiliateStates.waiting_for_pdf_selection)
    # Mode ensures we only show PDFs WITH links
    await state.update_data(selection_mode="affiliate_edit_select")
    await send_pdf_list_view(message, page=0, mode="affiliate_edit_select")

@dp.message(AffiliateStates.waiting_for_pdf_selection)
async def process_affiliate_pdf_selection(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_affiliate_menu())
    
    # Catch Back Button Here too just in case state is active
    if message.text == "⬅️ BACK TO AFFILIATE MENU":
        await state.clear()
        return await message.answer("💸 **AFFILIATE MANAGEMENT**", reply_markup=get_affiliate_menu(), parse_mode="Markdown")

    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
         try:
            page = int(message.text.split()[-1]) - 1
            data = await state.get_data()
            mode = data.get('selection_mode', 'affiliate_add_select')
            await send_pdf_list_view(message, page=page, mode=mode)
            return
         except: pass

    # Parse input - support comma-separated values for bulk selection
    raw_input = message.text.strip()
    queries = [q.strip() for q in raw_input.split(",")]
    
    # Get current mode to determine selection method
    current_data = await state.get_data()
    selection_mode = current_data.get('selection_mode', 'affiliate_add_select')
    use_sequential = selection_mode == 'affiliate_edit_select'  # Edit uses sequential numbering
    
    # For sequential selection (edit mode), get all PDFs matching the filter first
    all_filtered_pdfs = []
    if use_sequential:
        query_filter = {"affiliate_link": {"$exists": True, "$ne": ""}}
        all_filtered_pdfs = list(col_pdfs.find(query_filter).sort("index", 1))
    
    # Find all matching PDFs and remove duplicates
    found_pdfs = []
    seen_ids = set()
    not_found = []
    
    for query in queries:
        if not query:
            continue
            
        pdf = None
        if query.isdigit():
            if use_sequential:
                # Sequential selection - 1 means first item in the filtered list
                idx = int(query) - 1
                if 0 <= idx < len(all_filtered_pdfs):
                    pdf = all_filtered_pdfs[idx]
            else:
                # Direct index selection
                pdf = col_pdfs.find_one({"index": int(query)})
        else:
            pdf = col_pdfs.find_one({"name": {"$regex": f"^{re.escape(query)}$", "$options": "i"}})
        
        if pdf:
            # Check for duplicates
            pdf_id = str(pdf["_id"])
            if pdf_id not in seen_ids:
                seen_ids.add(pdf_id)
                found_pdfs.append(pdf)
        else:
            not_found.append(query)
    
    # Handle no results
    if not found_pdfs:
        msg = "❌ **No PDFs Found**\n\n"
        if not_found:
            msg += "Not found:\n" + "\n".join(f"• `{q}`" for q in not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return
    
    # Store selected PDF IDs and names for bulk operation
    pdf_ids = [str(pdf["_id"]) for pdf in found_pdfs]
    pdf_names = [pdf["name"] for pdf in found_pdfs]
    
    # For single selection, also store singular fields for compatibility
    if len(found_pdfs) == 1:
        await state.update_data(
            pdf_id=pdf_ids[0],
            pdf_name=pdf_names[0],
            pdf_ids=pdf_ids,
            pdf_names=pdf_names,
            is_bulk=False,
            current_aff=found_pdfs[0].get("affiliate_link", "None")
        )
    else:
        await state.update_data(
            pdf_ids=pdf_ids,
            pdf_names=pdf_names,
            is_bulk=True
        )
    
    await state.set_state(AffiliateStates.waiting_for_link)
    
    # Build confirmation message
    if len(found_pdfs) > 1:
        msg = f"💸 **MULTIPLE PDFs SELECTED ({len(found_pdfs)})**\n\n"
        for idx, pdf in enumerate(found_pdfs, 1):
            msg += f"{idx}. `{pdf['index']}` - {pdf['name']}\n"
        
        if not_found:
            msg += f"\n⚠️ **Not Found ({len(not_found)}):**\n"
            msg += "\n".join(f"• `{q}`" for q in not_found[:5])
            if len(not_found) > 5:
                msg += f"\n...and {len(not_found) - 5} more"
        
        msg += "\n\n📝 **Enter affiliate link to apply to ALL selected PDFs:**"
    else:
        # Single selection
        pdf = found_pdfs[0]
        current_aff = pdf.get("affiliate_link", "None")
        msg = (
            f"💸 **SELECTED PDF:**\n`{pdf['index']}`. {pdf['name']}\n"
            f"Current Affiliate Link: `{current_aff}`\n\n"
            "📝 **Enter new affiliate link:**"
        )
    
    await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")

@dp.message(AffiliateStates.waiting_for_link)
async def process_affiliate_link(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_affiliate_menu())
    
    link = message.text.strip()
    data = await state.get_data()
    
    # Basic Validation
    if "http" not in link and "t.me" not in link:
        await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
        return
    
    from bson.objectid import ObjectId
    
    # Check if bulk operation
    is_bulk = data.get('is_bulk', False)
    pdf_ids = data.get('pdf_ids', [])
    
    if is_bulk and pdf_ids:
        # Bulk assignment - apply same link to all PDFs
        object_ids = [ObjectId(id_str) for id_str in pdf_ids]
        result = col_pdfs.update_many(
            {"_id": {"$in": object_ids}},
            {"$set": {"affiliate_link": link}}
        )
        
        updated_count = result.modified_count
        pdf_names = data.get('pdf_names', [])
        
        await state.clear()
        await message.answer(
            f"✅ **Bulk Affiliate Link Assignment Complete!**\n\n"
            f"📊 Successfully set affiliate link for **{updated_count} PDF(s)**\n"
            f"🔗 Link: `{link}`",
            reply_markup=get_affiliate_menu(),
            parse_mode="Markdown"
        )
        
        # Log action
        log_user_action(message.from_user, "Bulk Add Affiliate", f"Set link for {updated_count} PDFs")
    else:
        # Single PDF assignment
        pdf_id = data.get('pdf_id')
        pdf_name = data.get('pdf_name', 'Unknown')
        
        # Check if same as current (only for single)
        current_aff = data.get('current_aff')
        if current_aff and link == current_aff:
            await message.answer(
                f"⚠️ **Same Link!**\nYou entered the exact same affiliate link.\nPlease enter a different link:",
                reply_markup=get_cancel_keyboard(),
                parse_mode="Markdown"
            )
            return
        
        col_pdfs.update_one(
            {"_id": ObjectId(pdf_id)},
            {"$set": {"affiliate_link": link}}
        )
        
        await state.clear()
        await message.answer(
            f"✅ **Affiliate Link Set for {pdf_name}!**",
            reply_markup=get_affiliate_menu(),
            parse_mode="Markdown"
        )

# 2. LIST AFFILIATE
@dp.message(F.text == "📋 LIST AFFILIATE")
async def list_affiliates_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "List Affiliates", "can_list"):
        return
    await state.set_state(AffiliateStates.viewing_list)
    await send_pdf_list_view(message, page=0, mode="list_affiliate")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_AFF") or m.text.startswith("➡️ NEXT_AFF")))
async def affiliate_pagination_handler(message: types.Message):
    try:
        page_str = message.text.split()[-1]
        page = int(page_str) - 1
        await send_pdf_list_view(message, page=page, mode="list_affiliate")
    except:
        await send_pdf_list_view(message, page=0, mode="list_affiliate")

@dp.message(F.text == "⬅️ BACK TO AFFILIATE MENU")
async def back_to_affiliate_menu(message: types.Message, state: FSMContext):
    await state.clear() # Clear any lingering state
    await message.answer("💸 **AFFILIATE MANAGEMENT**", reply_markup=get_affiliate_menu(), parse_mode="Markdown")

# 3. DELETE AFFILIATE

@dp.message(F.text == "🗑️ DELETE AFFILIATE")
async def start_delete_aff(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete Affiliate", "can_add"):
        return
    await state.set_state(AffiliateDeleteStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="affiliate_delete")

@dp.message(AffiliateDeleteStates.waiting_for_selection)
async def process_aff_delete_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_affiliate_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
         try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="affiliate_delete")
            return
         except: pass

    # Parse input - support comma-separated values for bulk deletion
    raw_input = message.text.strip()
    queries = [q.strip() for q in raw_input.split(",")]
    
    # Get all PDFs with affiliate links for sequential selection
    query_filter = {"affiliate_link": {"$exists": True, "$ne": ""}}
    all_filtered_pdfs = list(col_pdfs.find(query_filter).sort("index", 1))
    
    # Find all matching PDFs and remove duplicates
    found_pdfs = []
    seen_ids = set()
    not_found = []
    
    for query in queries:
        if not query:
            continue
            
        pdf = None
        if query.isdigit():
            # Sequential selection - 1 means first item in the filtered list
            idx = int(query) - 1
            if 0 <= idx < len(all_filtered_pdfs):
                pdf = all_filtered_pdfs[idx]
        else:
            pdf = col_pdfs.find_one({"name": {"$regex": f"^{re.escape(query)}$", "$options": "i"}})
        
        if pdf and pdf.get("affiliate_link"):
            # Check for duplicates
            pdf_id = str(pdf["_id"])
            if pdf_id not in seen_ids:
                seen_ids.add(pdf_id)
                found_pdfs.append(pdf)
        else:
            not_found.append(query)
    
    # Handle no results
    if not found_pdfs:
        msg = "❌ **No PDFs Found**\n\n"
        if not_found:
            msg += "Not found or no affiliate link:\n" + "\n".join(f"• `{q}`" for q in not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return
    
    # Store selected PDF IDs and names
    pdf_ids = [str(pdf["_id"]) for pdf in found_pdfs]
    pdf_names = [pdf["name"] for pdf in found_pdfs]
    
    # Store for both bulk and single
    if len(found_pdfs) == 1:
        await state.update_data(
            pdf_id=pdf_ids[0],
            pdf_name=pdf_names[0],
            pdf_ids=pdf_ids,
            pdf_names=pdf_names,
            is_bulk=False
        )
    else:
        await state.update_data(
            pdf_ids=pdf_ids,
            pdf_names=pdf_names,
            is_bulk=True
        )
    
    # Build confirmation message
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    await state.set_state(AffiliateDeleteStates.waiting_for_confirm)
    
    if len(found_pdfs) > 1:
        msg = f"⚠️ **CONFIRM BULK AFFILIATE DELETE**\n\n"
        msg += f"📊 **Total to delete: {len(found_pdfs)} affiliate link(s)**\n\n"
        for idx, pdf in enumerate(found_pdfs, 1):
            msg += f"{idx}. `{pdf['index']}` - {pdf['name']}\n"
        
        if not_found:
            msg += f"\n⚠️ **Not Found ({len(not_found)}):**\n"
            msg += "\n".join(f"• `{q}`" for q in not_found[:5])
            if len(not_found) > 5:
                msg += f"\n...and {len(not_found) - 5} more"
        
        msg += "\n\n❓ Remove affiliate links from all selected PDFs?"
    else:
        pdf = found_pdfs[0]
        msg = f"⚠️ Remove Affiliate Link from **{pdf['name']}**?"
    
    await message.answer(msg, reply_markup=kb, parse_mode="Markdown")

@dp.message(AffiliateDeleteStates.waiting_for_confirm)
async def process_aff_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        from bson.objectid import ObjectId
        
        # Check if bulk delete
        is_bulk = data.get('is_bulk', False)
        pdf_ids = data.get('pdf_ids', [])
        
        if is_bulk and pdf_ids:
            # Bulk delete - remove affiliate links from multiple PDFs
            object_ids = [ObjectId(id_str) for id_str in pdf_ids]
            result = col_pdfs.update_many(
                {"_id": {"$in": object_ids}},
                {"$unset": {"affiliate_link": ""}}
            )
            
            deleted_count = result.modified_count
            
            await state.clear()
            await message.answer(
                f"🗑️ **Bulk Affiliate Delete Complete!**\n\n"
                f"✅ Removed affiliate links from **{deleted_count} PDF(s)**",
                reply_markup=get_affiliate_menu(),
                parse_mode="Markdown"
            )
            
            # Log action
            log_user_action(message.from_user, "Bulk Delete Affiliate", f"Removed {deleted_count} affiliate links")
        else:
            # Single delete
            pdf_id = data.get('pdf_id')
            pdf_name = data.get('pdf_name', 'PDF')
            
            col_pdfs.update_one(
                {"_id": ObjectId(pdf_id)},
                {"$unset": {"affiliate_link": ""}}
            )
            
            await state.clear()
            await message.answer(
                f"🗑️ Affiliate Link Removed from **{pdf_name}**.",
                reply_markup=get_affiliate_menu(),
                parse_mode="Markdown"
            )
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_affiliate_menu())

@dp.message(F.text == "🔑 CODE")
async def code_menu_handler(message: types.Message):
    if not await check_authorization(message, "Code Menu", "can_add"):
        return
    await message.answer("🔑 **CODE MANAGEMENT**", reply_markup=get_code_menu(), parse_mode="Markdown")

# --- MSA CODE HANDLERS ---

# 1. ADD MSA CODE
@dp.message(F.text == "➕ ADD CODE")
async def start_add_msa_code(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add Code", "can_add"):
        return
    await state.set_state(MSACodeStates.waiting_for_pdf_selection)
    await state.update_data(selection_mode="msa_add_select")
    await send_pdf_list_view(message, page=0, mode="msa_add_select")

@dp.message(MSACodeStates.waiting_for_pdf_selection)
async def process_msa_code_pdf_selection(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_code_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            data = await state.get_data()
            mode = data.get('selection_mode', 'msa_add_select')
            await send_pdf_list_view(message, page=page, mode=mode)
            return
        except: pass

    query = message.text
    pdf = None
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    # Check if PDF already has MSA code (shouldn't happen if mode is correct, but double check)
    if pdf.get("msa_code"):
        await message.answer("⚠️ This PDF already has an MSA Code allocated.\nUse Edit to change it.", reply_markup=get_cancel_keyboard())
        return

    await state.update_data(pdf_id=str(pdf["_id"]), pdf_name=pdf["name"])
    await state.set_state(MSACodeStates.waiting_for_code)
    
    await message.answer(
        f"🔑 **SELECTED PDF:**\n`{pdf['index']}`. {pdf['name']}\n\n"
        "⌨️ **Enter MSA Code** (Format: MSA12345):",
        reply_markup=get_cancel_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(MSACodeStates.waiting_for_code)
async def process_msa_code(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_code_menu())
    
    code = message.text.strip().upper()
    
    # Validate format
    is_valid, error_msg = validate_msa_code(code)
    if not is_valid:
        await message.answer(error_msg, reply_markup=get_cancel_keyboard())
        return
    
    # Check for duplicates
    conflict_pdf = is_msa_code_duplicate(code)
    if conflict_pdf:
        await message.answer(f"⚠️ **MSA Code Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a unique code.", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return

    data = await state.get_data()
    from bson.objectid import ObjectId
    
    col_pdfs.update_one(
        {"_id": ObjectId(data['pdf_id'])},
        {"$set": {"msa_code": code}}
    )
    
    await state.clear()
    await message.answer(f"✅ **MSA Code `{code}` assigned to {data['pdf_name']}!**", reply_markup=get_code_menu(), parse_mode="Markdown")

# 2. EDIT MSA CODE
@dp.message(F.text == "✏️ EDIT CODE")
async def start_edit_msa_code(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit Code", "can_add"):
        return
    await state.set_state(MSACodeEditStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="msa_edit_select")

@dp.message(MSACodeEditStates.waiting_for_selection)
async def process_msa_edit_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_code_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="msa_edit_select")
            return
        except: pass

    query = message.text
    pdf = None
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found.", reply_markup=get_cancel_keyboard())
        return

    # Ensure it has an MSA code
    if not pdf.get("msa_code"):
        await message.answer("⚠️ This PDF does not have an MSA Code.\nUse Add instead.", reply_markup=get_cancel_keyboard())
        return

    await state.update_data(pdf_id=str(pdf["_id"]), pdf_name=pdf["name"], old_code=pdf["msa_code"])
    
    await state.set_state(MSACodeEditStates.waiting_for_new_code)
    await message.answer(
        f"✏️ **EDITING MSA CODE**\n"
        f"📄 PDF: {pdf['name']}\n"
        f"🔑 Current Code: `{pdf['msa_code']}`\n\n"
        "⌨️ **Enter New MSA Code** (Format: MSA12345):",
        reply_markup=get_cancel_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(MSACodeEditStates.waiting_for_new_code)
async def process_msa_edit_new_code(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_code_menu())
    
    code = message.text.strip().upper()
    
    # Validate format
    is_valid, error_msg = validate_msa_code(code)
    if not is_valid:
        await message.answer(error_msg, reply_markup=get_cancel_keyboard())
        return
    
    # Check if same as current
    data = await state.get_data()
    if code == data['old_code']:
        await message.answer(f"⚠️ **Same Code!**\nYou entered the exact same MSA code.\nPlease enter a different code:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return

    # Check for duplicates (exclude current PDF)
    conflict_pdf = is_msa_code_duplicate(code, exclude_id=data['pdf_id'])
    if conflict_pdf:
        await message.answer(f"⚠️ **MSA Code Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a unique code.", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return

    from bson.objectid import ObjectId
    
    col_pdfs.update_one(
        {"_id": ObjectId(data['pdf_id'])},
        {"$set": {"msa_code": code}}
    )
    
    old_code = data['old_code']
    pdf_name = data['pdf_name']
    
    await state.clear()
    await message.answer(
        f"✅ **MSA Code Updated for {pdf_name}!**\n\n"
        f"🔴 Old Code: `{old_code}`\n"
        f"🟢 New Code: `{code}`",
        reply_markup=get_code_menu(),
        parse_mode="Markdown"
    )

# 3. DELETE MSA CODE
@dp.message(F.text == "🗑️ DELETE CODE")
async def start_delete_msa_code(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete Code", "can_add"):
        return
    await state.set_state(MSACodeDeleteStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="msa_delete")

@dp.message(MSACodeDeleteStates.waiting_for_selection)
async def process_msa_delete_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_code_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="msa_delete")
            return
        except: pass

    query = message.text.strip()
    
    # Handle Bulk Selection (comma separated)
    queries = [q.strip() for q in query.split(",") if q.strip()]
    
    found_pdfs = []
    not_found = []
    no_code = []
    
    for q in queries:
        pdf_item = None
        if q.isdigit():
            pdf_item = col_pdfs.find_one({"index": int(q)})
        else:
            pdf_item = col_pdfs.find_one({"name": {"$regex": q, "$options": "i"}})
            
        if not pdf_item:
            not_found.append(q)
            continue
            
        if not pdf_item.get("msa_code"):
            no_code.append(pdf_item['name'])
            continue
            
        # Check for duplicates in selection
        if not any(p['_id'] == pdf_item['_id'] for p in found_pdfs):
            found_pdfs.append(pdf_item)
    
    if not found_pdfs:
        error_msg = "❌ No valid PDFs with MSA Codes found."
        if not_found: error_msg += f"\nNot Found: {', '.join(not_found)}"
        if no_code: error_msg += f"\nNo Code: {', '.join(no_code)}"
        await message.answer(error_msg, reply_markup=get_cancel_keyboard())
        return

    # Store for confirmation
    pdf_ids = [str(p["_id"]) for p in found_pdfs]
    
    await state.update_data(
        pdf_ids=pdf_ids,
        is_bulk=len(pdf_ids) > 1
    )
    
    # Build Confirmation Message
    msg = "⚠️ **CONFIRM BULK DELETION** ⚠️\n\n" if len(pdf_ids) > 1 else "⚠️ **CONFIRM DELETION**\n\n"
    msg += "You are about to remove MSA Codes from:\n"
    
    for p in found_pdfs:
        msg += f"• `{p['index']}`. **{p['name']}** (Code: `{p.get('msa_code', 'N/A')}`)\n"
        
    if not_found or no_code:
        msg += "\n⚠️ **SKIPPED ITEMS (Ignored):**\n"
        for q in not_found:
            msg += f"• `{q}`: Not Found\n"
        for name in no_code:
            msg += f"• `{name}`: No MSA Code assigned\n"
        
    msg += "\n**Are you sure?**"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(MSACodeDeleteStates.waiting_for_confirm)
    await message.answer(msg, reply_markup=kb, parse_mode="Markdown")

@dp.message(MSACodeDeleteStates.waiting_for_confirm)
async def process_msa_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        pdf_ids = data.get('pdf_ids', [])
        is_bulk = data.get('is_bulk', False)
        
        from bson.objectid import ObjectId
        
        # Perform Deletion
        if pdf_ids:
            object_ids = [ObjectId(pid) for pid in pdf_ids]
            result = col_pdfs.update_many(
                {"_id": {"$in": object_ids}},
                {"$unset": {"msa_code": ""}}
            )
            
            count = result.modified_count
            await state.clear()
            await message.answer(
                f"🗑️ **Deletion Complete**\nRemoved MSA Codes from {count} PDF(s).",
                reply_markup=get_code_menu(),
                parse_mode="Markdown"
            )
        else:
            await state.clear()
            await message.answer("⚠️ No PDFs selected.", reply_markup=get_code_menu())
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_code_menu())

# 4. LIST MSA CODES
@dp.message(F.text == "📋 LIST CODE")
async def list_msa_codes_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "List Code", "can_list"):
        return
    await state.set_state(MSACodeStates.viewing_list)
    await send_pdf_list_view(message, page=0, mode="list_msa")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_MSA") or m.text.startswith("➡️ NEXT_MSA")))
async def msa_pagination_handler(message: types.Message):
    try:
        page_str = message.text.split()[-1]
        page = int(page_str) - 1
        await send_pdf_list_view(message, page=page, mode="list_msa")
    except:
        await send_pdf_list_view(message, page=0, mode="list_msa")

@dp.message(F.text == "⬅️ BACK TO CODE MENU")
async def back_to_code_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("🔑 **CODE MANAGEMENT**", reply_markup=get_code_menu(), parse_mode="Markdown")


@dp.message(F.text == "▶️ YT")
async def yt_menu_handler(message: types.Message):
    if not await check_authorization(message, "YT Menu", "can_add"):
        return
    await message.answer("▶️ **YT MANAGEMENT**", reply_markup=get_yt_menu(), parse_mode="Markdown")

# --- YT HANDLERS ---

# 1. ADD YT
@dp.message(F.text == "➕ ADD YT LINK")
async def start_add_yt(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add YT", "can_add"):
        return
    await state.set_state(YTStates.waiting_for_pdf_selection)
    await send_pdf_list_view(message, page=0, mode="yt_add_select")

@dp.message(YTStates.waiting_for_pdf_selection)
async def process_yt_pdf_selection(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="yt_add_select")
            return
        except: pass

    query = message.text
    pdf = None
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    # Check if PDF already has YT data
    if pdf.get("yt_title") or pdf.get("yt_link"):
        await message.answer("⚠️ This PDF already has YT data.\nPlease add a new PDF first, or use Edit to modify.", reply_markup=get_cancel_keyboard())
        return

    # Store PDF info and ask for title
    await state.update_data(pdf_id=str(pdf["_id"]), pdf_name=pdf["name"])
    await state.set_state(YTStates.waiting_for_title)
    await message.answer(
        f"▶️ **Selected PDF:** {pdf['name']}\n\n"
        "⌨️ **Enter YouTube Video Title:**",
        reply_markup=get_cancel_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(YTStates.waiting_for_title)
async def process_yt_title(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    await state.update_data(yt_title=message.text.strip())
    
    # Check duplicate title
    conflict_pdf = is_yt_title_duplicate(message.text.strip())
    if conflict_pdf:
         await message.answer(f"⚠️ **YT Title Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different title:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
         return

    await state.set_state(YTStates.waiting_for_link)
    await message.answer("🔗 **Enter YouTube Short Link:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")

@dp.message(YTStates.waiting_for_link)
async def process_yt_link(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    # Basic validation
    link = message.text.strip()
    
    # Validation: Check duplicate YT link
    conflict_pdf = is_yt_link_duplicate(link)
    if conflict_pdf:
        await message.answer(f"⚠️ **YT Link Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return

    if "http" not in link and "youtu" not in link:
        await message.answer("⚠️ **Invalid YouTube Link.** Try again:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return
        
    data = await state.get_data()
    from bson.objectid import ObjectId
    
    col_pdfs.update_one(
        {"_id": ObjectId(data['pdf_id'])},
        {"$set": {"yt_title": data['yt_title'], "yt_link": link}}
    )
    
    await state.clear()
    await message.answer(
        f"✅ **YT Link added to {data['pdf_name']}!**\n\n"
        f"▶️ Title: {data['yt_title']}\n"
        f"🔗 Link: {link}",
        reply_markup=get_yt_menu(),
        parse_mode="Markdown"
    )

# 2. EDIT YT
@dp.message(F.text == "✏️ EDIT YT")
async def start_edit_yt(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit YT", "can_add"):
        return
    await state.set_state(YTEditStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="yt_edit_select")

@dp.message(YTEditStates.waiting_for_selection)
async def process_yt_edit_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="yt_edit_select")
            return
        except: pass

    query = message.text
    pdf = None
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found.", reply_markup=get_cancel_keyboard())
        return

    # Ensure it has YT data
    if not pdf.get("yt_title"):
        await message.answer("⚠️ This PDF does not have YT data.\nUse Add instead.", reply_markup=get_cancel_keyboard())
        return

    await state.update_data(pdf_id=str(pdf["_id"]), pdf_name=pdf["name"])
    
    # Show Edit Options
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="▶️ EDIT TITLE"), KeyboardButton(text="🔗 EDIT LINK")],
        [KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(YTEditStates.waiting_for_field)
    current_title = pdf.get('yt_title', 'Not Set')
    current_link = pdf.get('yt_link', 'Not Set')
    await state.update_data(current_yt_title=current_title, current_yt_link=current_link) # Store for comparison

    await state.set_state(YTEditStates.waiting_for_field)
    await message.answer(
        f"▶️ **YT DATA FOR: {pdf['name']}**\n"
        f"Title: {current_title}\n"
        f"Link: {current_link}\n\n"
        "⬇️ **Select what to edit:**",
        reply_markup=kb,
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

@dp.message(YTEditStates.waiting_for_field)
async def process_yt_edit_field(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    if message.text == "▶️ EDIT TITLE":
        await state.update_data(field="yt_title")
        await state.set_state(YTEditStates.waiting_for_value)
        await message.answer("⌨️ **Enter New Title:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
    elif message.text == "🔗 EDIT LINK":
        await state.update_data(field="yt_link")
        await state.set_state(YTEditStates.waiting_for_value)
        await message.answer("⌨️ **Enter New Link:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
    else:
        await message.answer("⚠️ Invalid Option.")

@dp.message(YTEditStates.waiting_for_value)
async def process_yt_edit_value(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
    
    data = await state.get_data()
    from bson.objectid import ObjectId
    new_value = message.text.strip()
    
    if data['field'] == "yt_link":
        pass # Allow same value update

        # Validation: Check duplicate YT link (exclude current PDF)
        conflict_pdf = is_yt_link_duplicate(new_value, exclude_id=data['pdf_id'])
        if conflict_pdf:
            await message.answer(f"⚠️ **YT Link Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
            return
            
        if "http" not in new_value and "youtu" not in new_value:
             await message.answer("⚠️ **Invalid YouTube Link.** Try again:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
             return

    elif data['field'] == "yt_title":
        pass # Allow same value update (no-op but good UX)

        # Validation: Check duplicate YT title (exclude current PDF)
        conflict_pdf = is_yt_title_duplicate(new_value, exclude_id=data['pdf_id'])
        if conflict_pdf:
            await message.answer(f"⚠️ **YT Title Already Exists!**\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different title:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
            return

    col_pdfs.update_one(
        {"_id": ObjectId(data['pdf_id'])},
        {"$set": {data['field']: new_value}}
    )
    
    await state.clear()
    field_name = "Title" if data['field'] == "yt_title" else "Link"
    await message.answer(f"✅ **YT {field_name} Updated for {data['pdf_name']}!**", reply_markup=get_yt_menu(), parse_mode="Markdown")

# 3. DELETE YT
@dp.message(F.text == "🗑️ DELETE YT")
async def start_delete_yt(message: types.Message, state: FSMContext):
    await state.set_state(YTDeleteStates.waiting_for_selection)
    await send_pdf_list_view(message, page=0, mode="yt_delete")

@dp.message(YTDeleteStates.waiting_for_selection)
async def process_yt_delete_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_yt_menu())
        
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_pdf_list_view(message, page=page, mode="yt_delete")
            return
        except: pass

    query = message.text.strip()
    
    # Handle Bulk Selection (comma separated)
    # RESOLVE SEQUENTIAL IDs (1, 2, 3...) to Actual PDFs in the "YT List"
    
    # 1. Fetch ALL PDFs that have YT data, sorted by index (same order as the view)
    yt_pdfs = list(col_pdfs.find({"yt_title": {"$exists": True, "$ne": ""}}).sort("index", 1))
    
    queries = [q.strip() for q in query.split(",") if q.strip()]
    
    found_pdfs = []
    not_found = []
    
    for q in queries:
        if q.isdigit():
            # Treat as SEQUENTIAL ID (1-based index in the list)
            seq_idx = int(q) - 1
            if 0 <= seq_idx < len(yt_pdfs):
                found_pdfs.append(yt_pdfs[seq_idx])
            else:
                not_found.append(q)
        else:
            # Fallback: Try regex search by name if not a digit? 
            # Or strict sequential? User asked for "1, 2, 3" which implies sequential.
            # Let's keep name match as backup but prioritize sequential
            matched_by_name = [p for p in yt_pdfs if q.lower() in p['name'].lower()]
            if matched_by_name:
                found_pdfs.extend(matched_by_name)
            else:
                not_found.append(q)

    # Remove duplicates
    unique_found = []
    seen_ids = set()
    for p in found_pdfs:
        if p['_id'] not in seen_ids:
            unique_found.append(p)
            seen_ids.add(p['_id'])
    found_pdfs = unique_found
    
    if not found_pdfs:
        error_msg = "❌ No valid PDFs selected."
        if not_found: error_msg += f"\nNot Found (in YT list): {', '.join(not_found)}"
        await message.answer(error_msg, reply_markup=get_cancel_keyboard())
        return

    # Store for confirmation
    pdf_ids = [str(p["_id"]) for p in found_pdfs]
    
    await state.update_data(
        pdf_ids=pdf_ids,
        is_bulk=len(pdf_ids) > 1
    )
    
    # Build Confirmation Message
    msg = "⚠️ **CONFIRM BULK DELETION** ⚠️\n\n" if len(pdf_ids) > 1 else "⚠️ **CONFIRM DELETION**\n\n"
    msg += "You are about to remove YT Data from:\n"
    
    for p in found_pdfs:
        # Show both sequential position? No, show actual Name and Title
        msg += f"• **{p['name']}** (YT: {p.get('yt_title', 'N/A')})\n"
        
    if not_found:
        msg += "\n⚠️ **SKIPPED ITEMS (Ignored):**\n"
        for q in not_found:
            msg += f"• `{q}`: Not valid sequential ID\n"
        
    msg += "\n**Are you sure?**"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(YTDeleteStates.waiting_for_confirm)
    await message.answer(msg, reply_markup=kb, parse_mode="Markdown")

@dp.message(YTDeleteStates.waiting_for_confirm)
async def process_yt_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        pdf_ids = data.get('pdf_ids', [])
        
        from bson.objectid import ObjectId
        
        # Perform Deletion
        if pdf_ids:
            object_ids = [ObjectId(pid) for pid in pdf_ids]
            result = col_pdfs.update_many(
                {"_id": {"$in": object_ids}},
                {"$unset": {"yt_title": "", "yt_link": ""}}
            )
            
            count = result.modified_count
            await state.clear()
            await message.answer(
                f"🗑️ **Deletion Complete**\nRemoved YT Data from {count} PDF(s).",
                reply_markup=get_yt_menu(),
                parse_mode="Markdown"
            )
        else:
            await state.clear()
            await message.answer("⚠️ No PDFs selected.", reply_markup=get_yt_menu())
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_yt_menu())

# 4. LIST YT
@dp.message(F.text == "📋 LIST YT")
async def list_yt_handler(message: types.Message, state: FSMContext):
    await state.set_state(YTStates.viewing_list)
    await send_pdf_list_view(message, page=0, mode="list_yt")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_YT") or m.text.startswith("➡️ NEXT_YT")))
async def yt_pagination_handler(message: types.Message):
    try:
        page_str = message.text.split()[-1]
        page = int(page_str) - 1
        await send_pdf_list_view(message, page=page, mode="list_yt")
    except:
        await send_pdf_list_view(message, page=0, mode="list_yt")

@dp.message(F.text == "⬅️ BACK TO YT MENU")
async def back_to_yt_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("▶️ **YT MANAGEMENT**", reply_markup=get_yt_menu(), parse_mode="Markdown")

@dp.message(F.text == "⬅️ BACK TO ADD MENU")
async def back_to_add_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("➕ **SELECT ADD COMPONENT:**", reply_markup=get_add_menu(), parse_mode="Markdown")

@dp.message(F.text == "🔗 LINKS")
async def links_menu_handler(message: types.Message):
    if not await check_authorization(message, "Links Menu", "can_list"):
        return
    await message.answer("🔗 **DEEP LINKS MANAGER**\nSelect a category to generate links:", reply_markup=get_links_menu(), parse_mode="Markdown")

@dp.message(F.text == "🏠 HOME YT")
async def home_yt_handler(message: types.Message):
    if not await check_authorization(message, "Home YT Link", "can_list"):
        return
    code = await get_home_yt_code()
    username = "msanodebot"
    
    link = f"https://t.me/{username}?start={code}_YTCODE"
    
    text = (
        "🏠 **HOME YT LINK**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 Link: `{link}`\n"
        f"🔑 Code: `{code}`\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        " This link is permanent and unique."
    )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "📸 IG CC")
async def ig_cc_links_handler(message: types.Message, page=0):
    if not await check_authorization(message, "IG CC Links", "can_list"):
        return
    limit = 5
    skip = page * limit
    
    total = col_ig_content.count_documents({})
    contents = list(col_ig_content.find().sort("cc_number", 1).skip(skip).limit(limit))
    
    if not contents and page == 0:
        await message.answer("⚠️ No IG CC Content found.", reply_markup=get_links_menu())
        return

    text = f"📸 **IG CC LINKS** (Page {page+1})\n━━━━━━━━━━━━━━━━━━━━\n\n"
    username = "msanodebot"
    
    for content in contents:
        # Ensure Code
        content = await ensure_ig_cc_code(content)
        code = content['start_code']
        cc_code = content['cc_code']
        
        link = f"https://t.me/{username}?start={code}_igcc_{cc_code}"
        
        text += (
            f"🆔 **{cc_code}**\n"
            f"🔗 `{link}`\n"
            f"🔑 Start Code: `{code}`\n"
            "────────────────────\n"
        )
    
    # Pagination Buttons
    buttons = []
    if page > 0: buttons.append(KeyboardButton(text=f"⬅️ PREV_IGLINK {page}"))
    if (skip + limit) < total: buttons.append(KeyboardButton(text=f"➡️ NEXT_IGLINK {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK TO LINKS MENU")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown", disable_web_page_preview=True)

@dp.message(F.text == "⬅️ BACK TO LINKS MENU")
async def back_to_links_menu(message: types.Message):
    await message.answer("🔗 **DEEP LINKS MANAGER**", reply_markup=get_links_menu(), parse_mode="Markdown")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_IGLINK") or m.text.startswith("➡️ NEXT_IGLINK")))
async def ig_link_pagination(message: types.Message):
    try:
        page = int(message.text.split()[-1]) - 1
        await ig_cc_links_handler(message, page=page)
    except:
        await message.answer("❌ Error navigating.")

@dp.message(F.text == "📑 ALL PDF")
async def all_pdf_links_handler(message: types.Message, page=0):
    if not await check_authorization(message, "All PDF Links", "can_list"):
        return
    limit = 5
    skip = page * limit
    
    total = col_pdfs.count_documents({})
    # Check if any PDFs exist
    if total == 0:
        await message.answer("⚠️ No PDFs found.", reply_markup=get_links_menu())
        return

    pdfs = list(col_pdfs.find().sort("index", 1).skip(skip).limit(limit))
    
    # Handle page out of bounds
    if not pdfs and page > 0:
        await message.answer("⚠️ End of list.", reply_markup=get_links_menu())
        return

    text = f"📑 **ALL PDF LINKS** (Page {page+1})\n━━━━━━━━━━━━━━━━━━━━\n\n"
    username = "msanodebot"
    
    for pdf in pdfs:
        # Check Completeness
        missing = []
        if not pdf.get('name'): missing.append("Name")
        if not pdf.get('link'): missing.append("Link")
        if not pdf.get('affiliate_link'): missing.append("Affiliate")
        if not pdf.get('msa_code'): missing.append("MSA Code")
        if not pdf.get('yt_title'): missing.append("YT Title")
        if not pdf.get('yt_link'): missing.append("YT Link")
        
        text += f"🆔 `{pdf['index']}`. **{pdf.get('name', 'Unknown')}**\n"
        
        if missing:
            text += f"⚠️ **Missing Details:** {', '.join(missing)}\n"
            text += "🚫 Links not generated. Please fill all fields.\n"
        else:
            # Generate Links
            pdf = await ensure_pdf_codes(pdf)
            ig_code = pdf['ig_start_code']
            yt_code = pdf['yt_start_code']
            aff_code = pdf['aff_start_code']
            orig_code = pdf['orig_start_code']
            
            # Sanitize Name (Alphanumeric + Underscore)
            sanitized_name = re.sub(r'[^a-zA-Z0-9]', '_', pdf['name'])
            sanitized_name = re.sub(r'_+', '_', sanitized_name).strip('_')
            
            ig_link = f"https://t.me/{username}?start={ig_code}_ig_{sanitized_name}"
            yt_link = f"https://t.me/{username}?start={yt_code}_yt_{sanitized_name}"
            aff_link = f"https://t.me/{username}?start={aff_code}_aff_{sanitized_name}"
            orig_link = f"https://t.me/{username}?start={orig_code}_orig_{sanitized_name}"
            
            text += (
                f"📸 **IG Link**: `{ig_link}`\n"
                f"   └ 🎟️ Code: `{ig_code}`\n\n"
                f"▶️ **YT Link**: `{yt_link}`\n"
                f"   └ 🎟️ Code: `{yt_code}`\n\n"
                f"🔐 **MSA Code**: `{pdf['msa_code']}`\n"
            )
        text += "────────────────────\n"
    
    # Pagination
    buttons = []
    if page > 0: buttons.append(KeyboardButton(text=f"⬅️ PREV_PDFLINK {page}"))
    if (skip + limit) < total: buttons.append(KeyboardButton(text=f"➡️ NEXT_PDFLINK {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK TO LINKS MENU")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown", disable_web_page_preview=True)

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_PDFLINK") or m.text.startswith("➡️ NEXT_PDFLINK")))
async def pdf_link_pagination(message: types.Message):
    try:
        page = int(message.text.split()[-1]) - 1
        await all_pdf_links_handler(message, page=page)
    except:
        await message.answer("❌ Error navigating.")

@dp.message(F.text == "⬅️ BACK TO YT MENU")
@dp.message(F.text == "📸 IG")
async def ig_menu_handler(message: types.Message):
    if not await check_authorization(message, "IG Menu", "can_add"):
        return
    await message.answer("📸 **IG MANAGEMENT**", reply_markup=get_ig_menu(), parse_mode="Markdown")

# --- IG HANDLERS ---

# 1. ADD IG
@dp.message(F.text == "➕ ADD IG")
async def start_add_ig(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add IG", "can_add"):
        return
    await state.set_state(IGStates.waiting_for_content_name)
    await message.answer("📝 **Enter IG Content:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")

@dp.message(IGStates.waiting_for_content_name)
async def process_ig_content_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_menu())
    
    content_name = message.text.strip()
    
    # Check for duplicate name
    if is_ig_name_duplicate(content_name):
        await message.answer("⚠️ **Content name already exists!**\nPlease use a different name.", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return
    
    # Auto-generate CC code
    cc_code, cc_number = get_next_cc_code()
    
    # Save to database
    from datetime import datetime
    doc = {
        "cc_code": cc_code,
        "cc_number": cc_number,
        "name": content_name,
        "created_at": datetime.now(),
        # Initialize click tracking field
        "ig_cc_clicks": 0,
        "last_ig_cc_click": None
    }
    col_ig_content.insert_one(doc)
    
    await state.clear()
    await message.answer(
        f"✅ **IG Content Added!**\n\n"
        f"🆔 Code: **{cc_code}**\n"
        f"📝 Name: {content_name}",
        reply_markup=get_ig_menu(),
        parse_mode="Markdown"
    )

# 2. EDIT IG
@dp.message(F.text == "✏️ EDIT IG")
async def start_edit_ig(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit IG", "can_add"):
        return
    await state.set_state(IGEditStates.waiting_for_selection)
    await send_ig_list_view(message, page=0, mode="edit")

@dp.message(IGEditStates.waiting_for_selection)
async def process_ig_edit_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="edit")
            return
        except: pass
    
    query = message.text.strip()
    content = None
    display_index = None
    
    # Try by index (display index)
    if query.isdigit():
        idx = int(query) - 1  # Convert to 0-based
        all_contents = list(col_ig_content.find().sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
            display_index = int(query)
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({"cc_code": {"$regex": f"^{query}$", "$options": "i"}})
        if content:
            # Find display index
            all_contents = list(col_ig_content.find().sort("cc_number", 1))
            for idx, c in enumerate(all_contents, start=1):
                if c['_id'] == content['_id']:
                    display_index = idx
                    break
    
    if not content:
        await message.answer("❌ Content Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(content_id=str(content["_id"]), old_name=content["name"], cc_code=content["cc_code"])
    
    # Display ONLY the selected item with FULL content
    text = f"✅ **SELECTED IG CONTENT**\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"`{display_index}`. **{content['cc_code']}**\n"
    text += f"📝 Full Content:\n{content['name']}\n"
    
    await state.set_state(IGEditStates.waiting_for_new_name)
    await message.answer(
        text + "\n━━━━━━━━━━━━━━━━━━━━\n⌨️ **Enter New Content:**",
        reply_markup=get_cancel_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(IGEditStates.waiting_for_new_name)
async def process_ig_edit_new_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_menu())
    
    new_name = message.text.strip()
    data = await state.get_data()
    
    # Check for duplicate name (excluding current content)
    if is_ig_name_duplicate(new_name, exclude_id=data['content_id']):
        await message.answer("⚠️ **Content name already exists!**\nPlease use a different name.", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        return
    
    from bson.objectid import ObjectId
    col_ig_content.update_one(
        {"_id": ObjectId(data['content_id'])},
        {"$set": {"name": new_name}}
    )
    
    await state.clear()
    await message.answer(
        f"✅ **IG Content Updated!**\n\n"
        f"🆔 Code: {data['cc_code']}\n"
        f"📝 New Name: {new_name}",
        reply_markup=get_ig_menu(),
        parse_mode="Markdown"
    )

# 3. DELETE IG
@dp.message(F.text == "🗑️ DELETE IG")
async def start_delete_ig(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete IG", "can_add"):
        return
    await state.set_state(IGDeleteStates.waiting_for_selection)
    await send_ig_list_view(message, page=0, mode="delete")

@dp.message(IGDeleteStates.waiting_for_selection)
async def process_ig_delete_select(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="delete")
            return
        except: pass
    
    raw_input = message.text.strip()
    queries = [q.strip() for q in raw_input.split(",")]
    
    # Get all contents sorted by cc_number for sequential resolution
    all_contents = list(col_ig_content.find().sort("cc_number", 1))
    
    found_contents = []
    seen_ids = set()
    not_found = []
    
    for q in queries:
        if not q: continue
        
        content = None
        if q.isdigit():
            # Sequential selection
            idx = int(q) - 1
            if 0 <= idx < len(all_contents):
                content = all_contents[idx]
        elif q.upper().startswith("CC"):
            # CC Code match
            content = next((c for c in all_contents if c['cc_code'].upper() == q.upper()), None)
            
        if content:
            cid = str(content["_id"])
            if cid not in seen_ids:
                seen_ids.add(cid)
                found_contents.append(content)
        else:
            not_found.append(q)
            
    if not found_contents:
        msg = "❌ **No Content Found**"
        if not_found:
             msg += "\nNot found: " + ", ".join(not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard())
        return
        
    # Store IDs
    delete_ids = [str(c["_id"]) for c in found_contents]
    
    await state.update_data(delete_ids=delete_ids)
    
    # Confirmation Message
    msg = f"⚠️ **CONFIRM BULK DELETION ({len(found_contents)})** ⚠️\n\n"
    for c in found_contents:
        msg += f"• **{c['cc_code']}** - {c['name']}\n"
        
    if not_found:
        msg += f"\n⚠️ Skipped: {', '.join(not_found)}\n"
        
    msg += "\n**Are you sure?**"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    await state.set_state(IGDeleteStates.waiting_for_confirm)
    await message.answer(msg, reply_markup=kb, parse_mode="Markdown")

@dp.message(IGDeleteStates.waiting_for_confirm)
async def process_ig_delete_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ CONFIRM DELETE":
        data = await state.get_data()
        delete_ids = data.get('delete_ids', [])
        
        from bson.objectid import ObjectId
        if delete_ids:
            object_ids = [ObjectId(uid) for uid in delete_ids]
            result = col_ig_content.delete_many({"_id": {"$in": object_ids}})
            count = result.deleted_count
            
            await state.clear()
            await message.answer(
                f"🗑️ **Deleted {count} IG Content(s)!**",
                reply_markup=get_ig_menu(),
                parse_mode="Markdown"
            )
        else:
             await state.clear()
             await message.answer("❌ Error: No content selected.", reply_markup=get_ig_menu())
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_ig_menu())

# 4. LIST IG
@dp.message(F.text == "📋 LIST IG")
async def list_ig_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "List IG", "can_list"):
        return
    await state.set_state(IGListStates.viewing)
    await send_ig_list_view(message, page=0, mode="list")

@dp.message(IGListStates.viewing)
async def process_ig_list_view(message: types.Message, state: FSMContext):
    # Handle BACK buttons first
    if message.text == "⬅️ BACK TO IG MENU":
        await state.clear()
        return await message.answer("📸 **IG MANAGEMENT**", reply_markup=get_ig_menu(), parse_mode="Markdown")
    
    if message.text == "⬅️ BACK TO LIST":
        await send_ig_list_view(message, page=0, mode="list")
        return
    
    # Handle Pagination
    if message.text and (message.text.startswith("⬅️ PREV_IG") or message.text.startswith("➡️ NEXT_IG")):
        try:
            page_str = message.text.split()[-1]
            page = int(page_str) - 1
            await send_ig_list_view(message, page=page, mode="list")
            return
        except:
            await send_ig_list_view(message, page=0, mode="list")
            return
    
    query = message.text.strip()
    content = None
    display_index = None
    
    # Try by index (display index)
    if query.isdigit():
        idx = int(query) - 1  # Convert to 0-based
        all_contents = list(col_ig_content.find().sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
            display_index = int(query)
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({"cc_code": {"$regex": f"^{query}$", "$options": "i"}})
        if content:
            # Find display index
            all_contents = list(col_ig_content.find().sort("cc_number", 1))
            for idx, c in enumerate(all_contents, start=1):
                if c['_id'] == content['_id']:
                    display_index = idx
                    break
    
    if not content:
        await message.answer("❌ Not Found. Try again or go back.", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ BACK TO IG MENU")]], resize_keyboard=True))
        return
    
    # Display the selected content
    text = f"✅ **VIEWING IG CONTENT**\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"{display_index}. {content['cc_code']}\n\n"
    text += f"📝 **Full Content:**\n{content['name']}\n"
    text += "━━━━━━━━━━━━━━━━━━━━"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⬅️ BACK TO LIST")],
        [KeyboardButton(text="⬅️ BACK TO IG MENU")]
    ], resize_keyboard=True)
    
    await message.answer(text, reply_markup=kb, parse_mode="Markdown")

@dp.message(F.text == "⬅️ BACK TO LIST")
async def back_to_ig_list(message: types.Message, state: FSMContext):
    await state.set_state(IGListStates.viewing)
    await send_ig_list_view(message, page=0, mode="list")

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_IG ") or m.text.startswith("➡️ NEXT_IG ")))
async def ig_pagination_handler(message: types.Message, state: FSMContext):
    # Check if we're in list viewing state
    current_state = await state.get_state()
    if current_state == IGListStates.viewing:
        try:
            page_str = message.text.split()[-1]
            page = int(page_str) - 1
            await send_ig_list_view(message, page=page, mode="list")
        except:
            await send_ig_list_view(message, page=0, mode="list")
    else:
        # Fallback for non-state pagination (shouldn't happen now)
        try:
            page_str = message.text.split()[-1]
            page = int(page_str) - 1
            await send_ig_list_view(message, page=page, mode="list")
        except:
            await send_ig_list_view(message, page=0, mode="list")

@dp.message(F.text == "⬅️ BACK TO IG MENU")
async def back_to_ig_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("📸 **IG MANAGEMENT**", reply_markup=get_ig_menu(), parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════════════════════
# COMPREHENSIVE LIST - Shows ALL Data (PDFs + IG Content)
# ═══════════════════════════════════════════════════════════════════════════

@dp.message(F.text == "📋 LIST")
async def comprehensive_list_handler(message: types.Message, state: FSMContext):
    """Show menu to choose between ALL (PDFs) or IG CC"""
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📚 ALL"), KeyboardButton(text="📸 IG CONTENT")],
        [KeyboardButton(text="⬅️ BACK")]
    ], resize_keyboard=True)
    await message.answer("📋 **SELECT VIEW:**", reply_markup=kb, parse_mode="Markdown")

@dp.message(F.text == "📚 ALL")
async def list_all_pdfs(message: types.Message, state: FSMContext):
    """Show all PDFs with pagination (5 per page)"""
    await state.set_state(ListStates.viewing_all)
    await send_all_pdfs_view(message, page=0)

@dp.message(F.text == "📸 IG CONTENT")
async def list_ig_content(message: types.Message, state: FSMContext):
    """Show all IG content with pagination (10 per page) - from LIST menu"""
    await state.set_state(ListStates.viewing_ig)
    await send_all_ig_view(message, page=0)

@dp.message(F.text == "⬅️ BACK")
async def back_from_list_menu(message: types.Message, state: FSMContext):
    """Handle BACK from LIST selection menu or viewing states"""
    current_state = await state.get_state()
    
    # If viewing ALL PDFs or IG CC, go back to LIST selection menu
    if current_state in [ListStates.viewing_all, ListStates.viewing_ig]:
        await state.clear()
        return await comprehensive_list_handler(message, state)
    
    # Otherwise, go back to main menu (from LIST selection menu itself)
    await state.clear()
    await message.answer("📋 **Main Menu**", reply_markup=get_main_menu(message.from_user.id))


async def send_all_pdfs_view(message: types.Message, page=0):
    """Display paginated PDF list (5 per page)"""
    limit = 5
    skip = page * limit
    
    total = col_pdfs.count_documents({})
    pdfs = list(col_pdfs.find().sort("index", 1).skip(skip).limit(limit))
    
    if not pdfs:
        await message.answer("⚠️ No PDFs found", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
        return
    
    text = f"📚 **PDF DATA** (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for pdf in pdfs:
        text += f"{pdf['index']}. **{pdf['name']}**\n"
        text += f"🔗 Link: {pdf['link']}\n"
        text += f"💸 AFF: {pdf.get('affiliate_link', 'Not Set')}\n"
        text += f"▶️ YT Title: {pdf.get('yt_title', 'Not Set')}\n"
        text += f"🔗 YT Link: {pdf.get('yt_link', 'Not Set')}\n"
        text += f"🔑 MSA: {pdf.get('msa_code', 'Not Set')}\n\n"
    
    # Pagination buttons
    buttons = []
    if page > 0:
        buttons.append(KeyboardButton(text=f"⬅️ PREV_ALL {page}"))
    if (skip + limit) < total:
        buttons.append(KeyboardButton(text=f"➡️ NEXT_ALL {page+2}"))
    
    keyboard = []
    if buttons:
        keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")

async def send_all_ig_view(message: types.Message, page=0):
    """Display paginated IG content list (10 per page)"""
    limit = 10
    skip = page * limit
    
    total = col_ig_content.count_documents({})
    contents = list(col_ig_content.find().sort("cc_number", 1).skip(skip).limit(limit))
    
    if not contents:
        await message.answer("⚠️ No IG Content found", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
        return
    
    text = f"📸 **IG CONTENT** (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for idx, content in enumerate(contents, start=1):
        display_index = skip + idx
        text += f"{display_index}. {content['cc_code']}\n"
        # Show preview (50 chars)
        preview = content['name']
        if len(preview) > 50:
            preview = preview[:50] + "..."
        text += f"📝 {preview}\n\n"
    
    # Pagination buttons
    buttons = []
    if page > 0:
        buttons.append(KeyboardButton(text=f"⬅️ PREV_IGCC {page}"))
    if (skip + limit) < total:
        buttons.append(KeyboardButton(text=f"➡️ NEXT_IGCC {page+2}"))
    
    keyboard = []
    if buttons:
        keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")

# Handle pagination for ALL PDFs
@dp.message(ListStates.viewing_all)
async def handle_all_pdfs_pagination(message: types.Message, state: FSMContext):
    if message.text and (message.text.startswith("⬅️ PREV_ALL") or message.text.startswith("➡️ NEXT_ALL")):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_all_pdfs_view(message, page=page)
        except:
            await send_all_pdfs_view(message, page=0)

# Handle pagination for IG CC
@dp.message(ListStates.viewing_ig)
async def handle_ig_cc_pagination(message: types.Message, state: FSMContext):
    if message.text and (message.text.startswith("⬅️ PREV_IGCC") or message.text.startswith("➡️ NEXT_IGCC")):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_all_ig_view(message, page=page)
        except:
            await send_all_ig_view(message, page=0)


# ═════════════════════════════════════════════════════════════════════
# SEARCH - PDF or IG CC with detailed info
# ═════════════════════════════════════════════════════════════════════

@dp.message(F.text == "🔍 SEARCH")
async def search_menu_handler(message: types.Message):
    if not await check_authorization(message, "Search Menu", "can_list"):
        return
    """Show search menu with PDF/IG CC options"""
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔍 SEARCH PDF"), KeyboardButton(text="🔍 SEARCH IG CC")],
        [KeyboardButton(text="⬅️ BACK")]
    ], resize_keyboard=True)
    await message.answer("🔍 **SELECT SEARCH TYPE:**", reply_markup=kb, parse_mode="Markdown")

@dp.message(F.text == "🔍 SEARCH PDF")
async def search_pdf_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Search PDF", "can_list"):
        return
    """Show available PDFs with pagination"""
    await state.set_state(SearchStates.viewing_pdf_list)
    await send_search_pdf_list(message, page=0)

async def send_search_pdf_list(message: types.Message, page=0):
    """Display paginated PDF list for search (5 per page)"""
    limit = 5
    skip = page * limit
    
    total = col_pdfs.count_documents({})
    pdfs = list(col_pdfs.find().sort("index", 1).skip(skip).limit(limit))
    
    if not pdfs:
        await message.answer("⚠️ No PDFs found", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
        return
    
    text = f"📚 **AVAILABLE PDFs** (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    for pdf in pdfs:
        text += f"{pdf['index']}. {pdf['name']}\n"
        text += f"🔗 {pdf['link']}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n⌨️ **Enter PDF Index or Name:**"
    
    # Pagination buttons
    buttons = []
    if page > 0:
        buttons.append(KeyboardButton(text=f"⬅️ PREV_SPDF {page}"))
    if (skip + limit) < total:
        buttons.append(KeyboardButton(text=f"➡️ NEXT_SPDF {page+2}"))
    
    keyboard = []
    if buttons:
        keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")

@dp.message(SearchStates.viewing_pdf_list)
async def handle_search_pdf_list(message: types.Message, state: FSMContext):
    """Handle pagination and input in PDF search list"""
    if message.text == "⬅️ BACK":
        await state.clear()
        return await search_menu_handler(message)
    
    if message.text == "❌ CANCEL":
        await state.clear()
        return await search_menu_handler(message)
    
    # Handle pagination
    if message.text and (message.text.startswith("⬅️ PREV_SPDF") or message.text.startswith("➡️ NEXT_SPDF")):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_search_pdf_list(message, page=page)
            return
        except:
            await send_search_pdf_list(message, page=0)
            return
    
    # Process search input
    await state.set_state(SearchStates.waiting_for_pdf_input)
    await process_pdf_search(message, state)

@dp.message(SearchStates.waiting_for_pdf_input)
async def process_pdf_search(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await search_menu_handler(message)
    
    query = message.text.strip()
    pdf = None
    
    # Try by index
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    # Try by name
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": f"^{query}$", "$options": "i"}})
    
    if not pdf:
        await message.answer("❌ PDF Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    # Format creation time
    from datetime import datetime
    creation_time = pdf.get('created_at', datetime.now())
    time_12h = creation_time.strftime("%I:%M %p")
    date_str = creation_time.strftime("%A, %B %d, %Y")
    
    # Build detailed info
    text = f"📄 **PDF DETAILS**\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🆔 Index: {pdf['index']}\n"
    text += f"📛 Name: {pdf['name']}\n"
    text += f"🔗 Link: {pdf['link']}\n"
    text += f"💸 Affiliate: {pdf.get('affiliate_link', 'Not Set')}\n"
    text += f"▶️ YT Title: {pdf.get('yt_title', 'Not Set')}\n"
    text += f"🔗 YT Link: {pdf.get('yt_link', 'Not Set')}\n"
    text += f"🔑 MSA Code: {pdf.get('msa_code', 'Not Set')}\n"
    text += f"📅 Created: {date_str}\n"
    text += f"🕐 Time: {time_12h}"
    
    # Keep state active for continuous search
    await state.set_state(SearchStates.waiting_for_pdf_input)
    
    # Add input prompt
    text += "\n\n━━━━━━━━━━━━━━━━━━━━\n⌨️ **Enter another PDF Index or Name to Search:**"
    
    # Auto-split if message exceeds Telegram's 4096 character limit
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for idx, part in enumerate(parts):
            if idx == len(parts) - 1:  # Last part gets the keyboard
                await message.answer(part, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
            else:
                await message.answer(part, parse_mode="Markdown")
    else:
        await message.answer(text, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "🔍 SEARCH IG CC")
async def search_ig_start(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Search IG", "can_list"):
        return
    """Show available IG content with pagination"""
    await state.set_state(SearchStates.viewing_ig_list)
    await send_search_ig_list(message, page=0)

async def send_search_ig_list(message: types.Message, page=0):
    """Display paginated IG content list for search (10 per page)"""
    limit = 10
    skip = page * limit
    
    total = col_ig_content.count_documents({})
    contents = list(col_ig_content.find().sort("cc_number", 1).skip(skip).limit(limit))
    
    if not contents:
        await message.answer("⚠️ No IG Content found", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ BACK")]], resize_keyboard=True))
        return
    
    text = f"📸 **AVAILABLE IG CONTENT** (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    for idx, content in enumerate(contents, start=1):
        display_idx = skip + idx
        text += f"{display_idx}. {content['cc_code']}\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n⌨️ **Enter Index or CC Code:**"
    
    # Pagination buttons
    buttons = []
    if page > 0:
        buttons.append(KeyboardButton(text=f"⬅️ PREV_SIG {page}"))
    if (skip + limit) < total:
        buttons.append(KeyboardButton(text=f"➡️ NEXT_SIG {page+2}"))
    
    keyboard = []
    if buttons:
        keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK"), KeyboardButton(text="❌ CANCEL")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")

@dp.message(SearchStates.viewing_ig_list)
async def handle_search_ig_list(message: types.Message, state: FSMContext):
    """Handle pagination and input in IG search list"""
    if message.text == "⬅️ BACK":
        await state.clear()
        return await search_menu_handler(message)
    
    if message.text == "❌ CANCEL":
        await state.clear()
        return await search_menu_handler(message)
    
    # Handle pagination
    if message.text and (message.text.startswith("⬅️ PREV_SIG") or message.text.startswith("➡️ NEXT_SIG")):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_search_ig_list(message, page=page)
            return
        except:
            await send_search_ig_list(message, page=0)
            return
    
    # Process search input
    await state.set_state(SearchStates.waiting_for_ig_input)
    await process_ig_search(message, state)

@dp.message(SearchStates.waiting_for_ig_input)
async def process_ig_search(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await search_menu_handler(message)
    
    query = message.text.strip()
    content = None
    
    # Try by index
    if query.isdigit():
        idx = int(query) - 1
        all_contents = list(col_ig_content.find().sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({"cc_code": {"$regex": f"^{query}$", "$options": "i"}})
    
    if not content:
        await message.answer("❌ IG Content Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    # Format creation time
    from datetime import datetime
    creation_time = content.get('created_at', datetime.now())
    time_12h = creation_time.strftime("%I:%M %p")
    date_str = creation_time.strftime("%A, %B %d, %Y")
    
    # Build detailed info
    text = f"📸 **IG CONTENT DETAILS**\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🆔 Code: {content['cc_code']}\n"
    text += f"📝 Content:\n{content['name']}\n\n"
    
    # Add affiliate link if exists
    if content.get('affiliate_link'):
        text += f"🔗 Affiliate: {content['affiliate_link']}\n"
    
    text += f"📅 Created: {date_str}\n"
    text += f"🕐 Time: {time_12h}"
    
    # Keep state active for continuous search
    await state.set_state(SearchStates.waiting_for_ig_input)
    
    # Add input prompt
    text += "\n\n━━━━━━━━━━━━━━━━━━━━\n⌨️ **Enter another Index or CC Code to Search:**"
    
    # Auto-split if message exceeds Telegram's 4096 character limit
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for idx, part in enumerate(parts):
            if idx == len(parts) - 1:  # Last part gets the keyboard
                await message.answer(part, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
            else:
                await message.answer(part, parse_mode="Markdown")
    else:
        await message.answer(text, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")


@dp.message(F.text == "🩺 DIAGNOSIS")
async def diagnosis_handler(message: types.Message):
    if not await check_authorization(message, "System Diagnosis", "can_view_analytics"):
        return
    """Comprehensive System Health Check & Diagnosis"""
    status_msg = await message.answer("🔍 **Running Complete System Diagnosis...**\n\n⏳ This may take a moment...", parse_mode="Markdown")
    
    issues = []
    warnings = []
    checks_passed = 0
    total_checks = 0
    
    # --- 1. DATABASE CONNECTION CHECK ---
    total_checks += 1
    try:
        start_t = datetime.now()
        client.admin.command('ping')
        ping_ms = (datetime.now() - start_t).microseconds / 1000
        
        if ping_ms > 100:
            warnings.append(f"⚠️ Database latency high: {ping_ms:.1f}ms (>100ms)")
        else:
            checks_passed += 1
            
        # Test write operation
        test_doc = {"test": True, "timestamp": datetime.now()}
        col_logs.insert_one(test_doc)
        col_logs.delete_one({"_id": test_doc["_id"]})
        checks_passed += 1
        total_checks += 1
        
    except Exception as e:
        issues.append(f"❌ Database Connection: {str(e)}")
        total_checks += 1
    
    # --- 2. COLLECTION INTEGRITY CHECK ---
    collections_to_check = {
        "bot9_pdfs": col_pdfs,
        "bot9_ig_content": col_ig_content,
        "bot9_logs": col_logs,
        "bot9_settings": col_settings
    }
    
    for coll_name, coll in collections_to_check.items():
        total_checks += 1
        try:
            count = coll.count_documents({})
            if count >= 0:
                checks_passed += 1
        except Exception as e:
            issues.append(f"❌ Collection '{coll_name}': {str(e)}")
    
    # --- 3. DATA INTEGRITY CHECK ---
    total_checks += 1
    try:
        # Check PDFs for missing critical fields
        pdfs_no_index = col_pdfs.count_documents({"index": {"$exists": False}})
        pdfs_no_name = col_pdfs.count_documents({"name": {"$exists": False}})
        pdfs_no_link = col_pdfs.count_documents({"link": {"$exists": False}})
        
        if pdfs_no_index > 0:
            issues.append(f"❌ {pdfs_no_index} PDFs missing 'index' field")
        if pdfs_no_name > 0:
            issues.append(f"❌ {pdfs_no_name} PDFs missing 'name' field")
        if pdfs_no_link > 0:
            issues.append(f"❌ {pdfs_no_link} PDFs missing 'link' field")
            
        if pdfs_no_index == 0 and pdfs_no_name == 0 and pdfs_no_link == 0:
            checks_passed += 1
            
    except Exception as e:
        issues.append(f"❌ Data Integrity Check: {str(e)}")
    
    # --- 4. DUPLICATE DETECTION ---
    total_checks += 1
    try:
        # Check for duplicate MSA codes
        pipeline = [
            {"$match": {"msa_code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$msa_code", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        duplicates = list(col_pdfs.aggregate(pipeline))
        
        if len(duplicates) > 0:
            issues.append(f"❌ Found {len(duplicates)} duplicate MSA codes")
            for dup in duplicates[:3]:  # Show first 3
                issues.append(f"   • Code '{dup['_id']}' used {dup['count']} times")
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Duplicate Check: {str(e)}")
    
    # --- 5. INDEX VERIFICATION ---
    total_checks += 1
    try:
        indexes = col_pdfs.list_indexes()
        index_names = [idx['name'] for idx in indexes]
        
        required_indexes = ['index_1', 'created_at_1', 'msa_code_1']
        missing_indexes = [idx for idx in required_indexes if idx not in index_names]
        
        if missing_indexes:
            warnings.append(f"⚠️ Missing indexes: {', '.join(missing_indexes)}")
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Index Check: {str(e)}")
    
    # --- 6. STORAGE CHECK ---
    total_checks += 1
    try:
        db_stats = db.command("dbStats")
        db_size_mb = db_stats.get("dataSize", 0) / (1024 * 1024)
        storage_limit = 512  # MB
        
        if db_size_mb > storage_limit * 0.9:
            issues.append(f"❌ Database nearly full: {db_size_mb:.2f}MB / {storage_limit}MB")
        elif db_size_mb > storage_limit * 0.7:
            warnings.append(f"⚠️ Database usage high: {db_size_mb:.2f}MB / {storage_limit}MB")
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Storage Check: {str(e)}")
    
    # --- 7. FILE SYSTEM CHECK ---
    total_checks += 1
    try:
        log_file = "bot9.log"
        if os.path.exists(log_file):
            log_size = os.path.getsize(log_file) / (1024 * 1024)  # MB
            if log_size > 100:
                warnings.append(f"⚠️ Log file large: {log_size:.2f}MB (consider rotation)")
            else:
                checks_passed += 1
        else:
            warnings.append("⚠️ Log file not found")
            
    except Exception as e:
        warnings.append(f"⚠️ File System Check: {str(e)}")
    
    # --- 8. SYSTEM RESOURCES CHECK ---
    total_checks += 1
    try:
        cpu_percent = psutil.cpu_percent(interval=0.5)
        memory = psutil.Process().memory_info()
        memory_mb = memory.rss / (1024 * 1024)
        
        if cpu_percent > 80:
            warnings.append(f"⚠️ High CPU usage: {cpu_percent:.1f}%")
        if memory_mb > 500:
            warnings.append(f"⚠️ High memory usage: {memory_mb:.2f}MB")
            
        if cpu_percent <= 80 and memory_mb <= 500:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Resource Check: {str(e)}")
    
    # --- 9. CONFIGURATION CHECK ---
    total_checks += 1
    try:
        config_ok = True
        if not BOT_TOKEN:
            issues.append("❌ BOT_TOKEN not configured")
            config_ok = False
        if not MONGO_URI:
            issues.append("❌ MONGO_URI not configured")
            config_ok = False
        if not MASTER_ADMIN_ID or MASTER_ADMIN_ID == 0:
            warnings.append("⚠️ MASTER_ADMIN_ID not configured")
            config_ok = False
            
        if config_ok:
            checks_passed += 1
            
    except Exception as e:
        issues.append(f"❌ Config Check: {str(e)}")
    
    # --- 10. DATA CONSISTENCY CHECK ---
    total_checks += 1
    try:
        # Check for orphaned data
        total_pdfs = col_pdfs.count_documents({})
        pdfs_with_codes = col_pdfs.count_documents({"msa_code": {"$exists": True, "$ne": "", "$ne": None}})
        
        # Verify index sequence
        if total_pdfs > 0:
            highest_index = col_pdfs.find_one(sort=[("index", -1)])
            if highest_index:
                expected_max = highest_index.get("index", 0)
                if expected_max > total_pdfs + 100:  # Allow some gaps
                    warnings.append(f"⚠️ Index gaps detected (max: {expected_max}, count: {total_pdfs})")
                else:
                    checks_passed += 1
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Consistency Check: {str(e)}")
    
    # --- 11. CLICK TRACKING FIELDS CHECK ---
    total_checks += 1
    try:
        # Check for PDFs missing click tracking fields
        pdfs_no_ig_clicks = col_pdfs.count_documents({"ig_start_clicks": {"$exists": False}})
        pdfs_no_yt_clicks = col_pdfs.count_documents({"yt_start_clicks": {"$exists": False}})
        pdfs_no_total_clicks = col_pdfs.count_documents({"clicks": {"$exists": False}})
        
        if pdfs_no_ig_clicks > 0 or pdfs_no_yt_clicks > 0 or pdfs_no_total_clicks > 0:
            warnings.append(f"⚠️ {max(pdfs_no_ig_clicks, pdfs_no_yt_clicks, pdfs_no_total_clicks)} PDFs missing click tracking fields")
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Click Tracking Check: {str(e)}")
    
    # --- 12. DEEP LINK START CODES CHECK ---
    total_checks += 1
    try:
        # Check for missing start codes
        pdfs_no_ig_code = col_pdfs.count_documents({"ig_start_code": {"$exists": False}})
        pdfs_no_yt_code = col_pdfs.count_documents({"yt_start_code": {"$exists": False}})
        
        # Check for duplicate start codes
        ig_code_pipeline = [
            {"$match": {"ig_start_code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$ig_start_code", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        yt_code_pipeline = [
            {"$match": {"yt_start_code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$yt_start_code", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        
        dup_ig_codes = list(col_pdfs.aggregate(ig_code_pipeline))
        dup_yt_codes = list(col_pdfs.aggregate(yt_code_pipeline))
        
        if pdfs_no_ig_code > 0 or pdfs_no_yt_code > 0:
            warnings.append(f"⚠️ {max(pdfs_no_ig_code, pdfs_no_yt_code)} PDFs missing deep link codes")
        if len(dup_ig_codes) > 0:
            issues.append(f"❌ Found {len(dup_ig_codes)} duplicate IG start codes")
        if len(dup_yt_codes) > 0:
            issues.append(f"❌ Found {len(dup_yt_codes)} duplicate YT start codes")
            
        if pdfs_no_ig_code == 0 and pdfs_no_yt_code == 0 and len(dup_ig_codes) == 0 and len(dup_yt_codes) == 0:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Deep Link Codes Check: {str(e)}")
    
    # --- 13. IG CONTENT VALIDATION ---
    total_checks += 1
    try:
        total_ig = col_ig_content.count_documents({})
        
        # Check for missing critical IG fields
        ig_no_cc = col_ig_content.count_documents({"cc_code": {"$exists": False}})
        ig_no_name = col_ig_content.count_documents({"name": {"$exists": False}})
        ig_no_start_code = col_ig_content.count_documents({"start_code": {"$exists": False}})
        
        # Check for duplicate CC codes
        cc_code_pipeline = [
            {"$match": {"cc_code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$cc_code", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        dup_cc_codes = list(col_ig_content.aggregate(cc_code_pipeline))
        
        if ig_no_cc > 0 or ig_no_name > 0 or ig_no_start_code > 0:
            issues.append(f"❌ {max(ig_no_cc, ig_no_name, ig_no_start_code)} IG items missing critical fields")
        if len(dup_cc_codes) > 0:
            issues.append(f"❌ Found {len(dup_cc_codes)} duplicate CC codes in IG content")
            
        if ig_no_cc == 0 and ig_no_name == 0 and ig_no_start_code == 0 and len(dup_cc_codes) == 0:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ IG Content Check: {str(e)}")
    
    # --- 14. AFFILIATE LINK INTEGRITY ---
    total_checks += 1
    try:
        # Count PDFs with affiliate links
        pdfs_with_affiliate = col_pdfs.count_documents({"affiliate_link": {"$exists": True, "$ne": "", "$ne": None}})
        ig_with_affiliate = col_ig_content.count_documents({"affiliate_link": {"$exists": True, "$ne": "", "$ne": None}})
        
        # Check for broken/invalid affiliate URLs
        pdfs_invalid_aff = 0
        for pdf in col_pdfs.find({"affiliate_link": {"$exists": True, "$ne": "", "$ne": None}}, {"affiliate_link": 1}):
            aff_link = pdf.get("affiliate_link", "")
            if not aff_link.startswith("http://") and not aff_link.startswith("https://"):
                pdfs_invalid_aff += 1
        
        if pdfs_invalid_aff > 0:
            warnings.append(f"⚠️ {pdfs_invalid_aff} PDFs have invalid affiliate URL format")
        else:
            checks_passed += 1
            
    except Exception as e:
        warnings.append(f"⚠️ Affiliate Links Check: {str(e)}")

    
    # --- GENERATE REPORT ---
    health_score = (checks_passed / total_checks * 100) if total_checks > 0 else 0
    
    # Determine health status
    if health_score >= 95 and len(issues) == 0:
        status_emoji = "🟢"
        status_text = "EXCELLENT"
        status_msg_text = "All systems operating perfectly!"
    elif health_score >= 80 and len(issues) == 0:
        status_emoji = "🟡"
        status_text = "GOOD"
        status_msg_text = "System healthy with minor warnings"
    elif health_score >= 60:
        status_emoji = "🟠"
        status_text = "FAIR"
        status_msg_text = "Some issues detected, review recommended"
    else:
        status_emoji = "🔴"
        status_text = "CRITICAL"
        status_msg_text = "Immediate attention required!"
    
    # Build detailed report
    report = f"""
🩺 **SYSTEM DIAGNOSIS REPORT**
━━━━━━━━━━━━━━━━━━━━━━━━

{status_emoji} **HEALTH STATUS: {status_text}**
{status_msg_text}

**📊 CHECKS SUMMARY**
• Total Checks: `{total_checks}`
• Passed: `{checks_passed}` ✅
• Warnings: `{len(warnings)}` ⚠️
• Critical: `{len(issues)}` ❌

**🎯 HEALTH SCORE**
{status_emoji} **{health_score:.1f}%**
"""

    # Add critical issues
    if issues:
        report += "\n**❌ CRITICAL ISSUES:**\n"
        for issue in issues:
            report += f"{issue}\n"
    
    # Add warnings
    if warnings:
        report += "\n**⚠️ WARNINGS:**\n"
        for warning in warnings[:5]:  # Limit to 5
            report += f"{warning}\n"
        if len(warnings) > 5:
            report += f"_...and {len(warnings) - 5} more warnings_\n"
    
    # Add all clear message
    if not issues and not warnings:
        report += "\n**✅ ALL CHECKS PASSED**\n"
        report += "• Database: Healthy\n"
        report += "• Collections: Valid\n"
        report += "• Data Integrity: Perfect\n"
        report += "• No Duplicates: Verified\n"
        report += "• Indexes: Optimal\n"
        report += "• Storage: Sufficient\n"
        report += "• Logs: Normal\n"
        report += "• Resources: Optimal\n"
        report += "• Configuration: Complete\n"
        report += "• Consistency: Validated\n"
        report += "\n🎉 **System is running flawlessly!**\n"
    
    # Add recommendations
    if issues or warnings:
        report += "\n**💡 RECOMMENDATIONS:**\n"
        if len(issues) > 0:
            report += "• Address critical issues immediately\n"
        if any("duplicate" in str(w).lower() for w in warnings + issues):
            report += "• Run duplicate cleanup\n"
        if any("storage" in str(w).lower() or "database" in str(w).lower() for w in warnings):
            report += "• Consider archiving old data\n"
        if any("log" in str(w).lower() for w in warnings):
            report += "• Rotate log files\n"
        if any("index" in str(w).lower() for w in warnings):
            report += "• Rebuild database indexes\n"
    
    report += f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"_Diagnostic completed at {datetime.now().strftime('%I:%M:%S %p')}_"
    
    await status_msg.edit_text(report, parse_mode="Markdown")
    log_user_action(message.from_user, "Ran System Diagnosis", f"Score: {health_score:.1f}%")

def get_recent_logs(lines_count=30):
    """Refactored log reader"""
    log_file = "bot9.log"
    if not os.path.exists(log_file):
        return "⚠️ No logs found yet."
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            content = "".join(lines[-lines_count:])
            if len(content) > 2000:
                content = content[-2000:]
                content = "..." + content
            return content.replace("`", "'") if content.strip() else "No recent logs."
    except Exception as e:
        return f"Error reading logs: {e}"

@dp.message(F.text == "🖥️ TERMINAL")
async def terminal_handler(message: types.Message):
    if not await check_authorization(message, "Terminal", "can_view_analytics"):
        return
    log_user_action(message.from_user, "Viewed Terminal")
    logs = get_recent_logs()
    text = f"🖥️ **LIVE TERMINAL OUTPUT**\n━━━━━━━━━━━━━━━━━━━━\n```\n{logs}\n```\n━━━━━━━━━━━━━━━━━━━━"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 REFRESH", callback_data="refresh_terminal")]])
    await message.answer(text, parse_mode="Markdown", reply_markup=kb)

@dp.callback_query(F.data == "refresh_terminal")
async def refresh_terminal_callback(callback: types.CallbackQuery):
    # Short circuit for permission check on callback
    if not await check_authorization(callback.message, "Refresh Terminal", "can_view_analytics"):
         await callback.answer("⛔ Access Denied", show_alert=True)
         return
    logs = get_recent_logs()
    text = f"🖥️ **LIVE TERMINAL OUTPUT**\n━━━━━━━━━━━━━━━━━━━━\n```\n{logs}\n```\n━━━━━━━━━━━━━━━━━━━━"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 REFRESH", callback_data="refresh_terminal")]])
    
    try:
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        pass # Content identical or message not modified
    
    await callback.answer("Refreshing logs...")

@dp.message(F.text == "📚 BOT GUIDE")
async def guide_handler(message: types.Message):
    await message.answer(
        "📚 **BOT 9 GUIDE**\n\n"
        "This bot manages:\n"
        "- PDF Management\n"
        "- IG Content Management\n"
        "- Link Generation\n"
        "- Click Analytics\n\n"
        "Use the main menu to navigate.",
        reply_markup=get_main_menu(message.from_user.id),
        parse_mode="Markdown"
    )

# ==========================================
# 📊 ANALYTICS HANDLERS
# ==========================================

@dp.message(F.text == "📊 ANALYTICS")
async def analytics_menu_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Analytics Menu", "can_view_analytics"):
        return
    """Show Analytics Menu"""
    await state.set_state(AnalyticsStates.viewing_analytics)
    await message.answer(
        "📊 **ANALYTICS DASHBOARD**\n\n"
        "Select a category to view detailed analytics:",
        reply_markup=get_analytics_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "📊 OVERVIEW")
async def analytics_overview_handler(message: types.Message):
    if not await check_authorization(message, "Analytics Overview", "can_view_analytics"):
        return
    """Show comprehensive analytics overview"""
    
    # Gather all stats efficiently using aggregation
    total_pdfs = col_pdfs.count_documents({"link": {"$exists": True}})
    total_ig_content = col_ig_content.count_documents({"cc_code": {"$exists": True}})
    
    # Use aggregation pipeline for efficient click totals (single query)
    pdf_stats = list(col_pdfs.aggregate([
        {"$group": {
            "_id": None,
            "pdf_clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
            "aff_clicks": {"$sum": {"$ifNull": ["$affiliate_clicks", 0]}},
            "ig_clicks": {"$sum": {"$ifNull": ["$ig_start_clicks", 0]}},
            "yt_clicks": {"$sum": {"$ifNull": ["$yt_start_clicks", 0]}},
            "yt_code_clicks": {"$sum": {"$ifNull": ["$yt_code_clicks", 0]}}
        }}
    ]))
    
    ig_stats = list(col_ig_content.aggregate([
        {"$group": {
            "_id": None,
            "ig_cc_clicks": {"$sum": {"$ifNull": ["$ig_cc_clicks", 0]}}
        }}
    ]))
    
    # Extract values (default to 0 if no data)
    pdf_clicks = pdf_stats[0].get("pdf_clicks", 0) if pdf_stats else 0
    aff_clicks = pdf_stats[0].get("aff_clicks", 0) if pdf_stats else 0
    ig_clicks = pdf_stats[0].get("ig_clicks", 0) if pdf_stats else 0
    yt_clicks = pdf_stats[0].get("yt_clicks", 0) if pdf_stats else 0
    yt_code_clicks = pdf_stats[0].get("yt_code_clicks", 0) if pdf_stats else 0
    ig_cc_clicks = ig_stats[0].get("ig_cc_clicks", 0) if ig_stats else 0
    
    total_clicks = pdf_clicks + aff_clicks + ig_clicks + yt_clicks + ig_cc_clicks + yt_code_clicks
    
    # Configuration status
    pdfs_with_affiliate = col_pdfs.count_documents({"affiliate_link": {"$exists": True, "$ne": ""}})
    pdfs_with_ig = col_pdfs.count_documents({"ig_start_code": {"$exists": True, "$ne": ""}})
    pdfs_with_yt = col_pdfs.count_documents({"yt_link": {"$exists": True, "$ne": ""}})
    pdfs_with_msa = col_pdfs.count_documents({"msa_code": {"$exists": True, "$ne": ""}})
    
    # Top 5 performers overall using optimized aggregation
    all_items = []
    
    # Get top PDFs (only fetch name and clicks fields)
    for pdf in col_pdfs.find(
        {"link": {"$exists": True}, "clicks": {"$gt": 0}},
        {"name": 1, "clicks": 1, "_id": 0}
    ).sort("clicks", -1).limit(20):
        all_items.append({"name": pdf.get("name", "Unnamed"), "clicks": pdf.get("clicks", 0), "type": "📄 PDF"})
    
    # Get top Affiliates (only fetch name and affiliate_clicks fields)
    for pdf in col_pdfs.find(
        {"affiliate_link": {"$exists": True, "$ne": ""}, "affiliate_clicks": {"$gt": 0}},
        {"name": 1, "affiliate_clicks": 1, "_id": 0}
    ).sort("affiliate_clicks", -1).limit(20):
        all_items.append({"name": pdf.get("name", "Unnamed"), "clicks": pdf.get("affiliate_clicks", 0), "type": "💸 Affiliate"})
    
    # Get top IG CC (only fetch name and ig_cc_clicks fields)
    for ig in col_ig_content.find(
        {"ig_cc_clicks": {"$gt": 0}},
        {"name": 1, "ig_cc_clicks": 1, "_id": 0}
    ).sort("ig_cc_clicks", -1).limit(20):
        all_items.append({"name": ig.get("name", "Unnamed"), "clicks": ig.get("ig_cc_clicks", 0), "type": "📸 IG CC"})
    
    # Sort all items by clicks and get top 5
    all_items.sort(key=lambda x: x["clicks"], reverse=True)
    top_5 = all_items[:5]
    
    # Build overview message
    text = "📊 **ANALYTICS OVERVIEW**\n"
    text += "═══════════════════════\n\n"
    
    text += f"📈 **TOTAL CLICKS:** {total_clicks:,}\n\n"
    
    text += "**📊 Clicks by Category:**\n"
    text += f"├ 📄 PDFs: {pdf_clicks:,}\n"
    text += f"├ 💸 Affiliates: {aff_clicks:,}\n"
    text += f"├ 📸 IG Start: {ig_clicks:,}\n"
    text += f"├ ▶️ YT Start: {yt_clicks:,}\n"
    text += f"├ 📸 IG CC: {ig_cc_clicks:,}\n"
    text += f"└ 🔑 YT Code: {yt_code_clicks:,}\n\n"
    
    text += "**📚 Content Library:**\n"
    text += f"├ Total PDFs: {total_pdfs}\n"
    text += f"├ IG Content: {total_ig_content}\n"
    text += f"├ With Affiliates: {pdfs_with_affiliate}\n"
    text += f"├ With IG Codes: {pdfs_with_ig}\n"
    text += f"├ With YT Links: {pdfs_with_yt}\n"
    text += f"└ With MSA Codes: {pdfs_with_msa}\n\n"
    
    if top_5:
        text += "🏆 **TOP 5 PERFORMERS:**\n"
        for idx, item in enumerate(top_5, 1):
            text += f"{idx}. {item['type']} **{item['name']}** - {item['clicks']:,} clicks\n"
        text += "\n"
    else:
        text += "📭 No clicks recorded yet.\n\n"
    
    # Performance indicators
    if total_pdfs > 0:
        complete_pdfs = col_pdfs.count_documents({
            "link": {"$exists": True},
            "affiliate_link": {"$exists": True, "$ne": ""},
            "ig_start_code": {"$exists": True, "$ne": ""},
            "yt_link": {"$exists": True, "$ne": ""},
            "msa_code": {"$exists": True, "$ne": ""}
        })
        completion_rate = (complete_pdfs / total_pdfs * 100) if total_pdfs > 0 else 0
        text += f"✅ **Setup Completion:** {completion_rate:.1f}% ({complete_pdfs}/{total_pdfs} fully configured)\n"
    
    text += "\n═══════════════════════\n"
    text += "💡 Select a category below for detailed analytics."
    
    await message.answer(
        text,
        reply_markup=get_analytics_menu(),
        parse_mode="Markdown"
    )

async def send_analytics_view(message: types.Message, category: str, page: int = 0):
    """Display top clicked items for a category with pagination"""
    items_per_page = 10
    skip = page * items_per_page
    
    # Determine collection, fields, and query based on category
    if category == "pdf":
        collection = col_pdfs
        title = "📄 TOP CLICKED PDFs"
        name_field = "name"
        click_field = "clicks"
        # Show all PDFs that have a link configured
        query = {"link": {"$exists": True}}
    elif category == "affiliate":
        collection = col_pdfs
        title = "💸 TOP CLICKED AFFILIATES"
        name_field = "name"
        click_field = "affiliate_clicks"
        # Show only PDFs that have affiliate link configured
        query = {"affiliate_link": {"$exists": True, "$ne": ""}}
    elif category == "ig_start":
        collection = col_pdfs
        title = "📸 TOP CLICKED IG START LINKS"
        name_field = "name"
        click_field = "ig_start_clicks"
        # Show only PDFs that have IG start code configured
        query = {"ig_start_code": {"$exists": True, "$ne": ""}}
    elif category == "yt_start":
        collection = col_pdfs
        title = "▶️ TOP CLICKED YT START LINKS"
        name_field = "name"
        click_field = "yt_start_clicks"
        # Show only PDFs that have YT link configured
        query = {"yt_link": {"$exists": True, "$ne": ""}}
    elif category == "ig_cc_start":
        collection = col_ig_content
        title = "📸 TOP CLICKED IG CC START LINKS"
        name_field = "name"
        click_field = "ig_cc_clicks"
        # Show all IG content (all have CC codes)
        query = {"cc_code": {"$exists": True}}
    elif category == "yt_code_start":
        collection = col_pdfs
        title = "🔑 TOP CLICKED YT CODE START LINKS"
        name_field = "name"
        click_field = "yt_code_clicks"
        # Show only PDFs that have MSA code configured
        query = {"msa_code": {"$exists": True, "$ne": ""}}
    else:
        await message.answer("⚠️ Invalid category")
        return
    
    # Count total items matching criteria
    total_items = collection.count_documents(query)
    
    if total_items == 0:
        # Determine empty message based on category
        if category == "affiliate":
            empty_msg = "No PDFs with affiliate links configured yet."
        elif category == "ig_start":
            empty_msg = "No PDFs with IG start codes configured yet."
        elif category == "yt_start":
            empty_msg = "No PDFs with YT links configured yet."
        elif category == "yt_code_start":
            empty_msg = "No PDFs with MSA codes configured yet."
        elif category == "ig_cc_start":
            empty_msg = "No IG content configured yet."
        else:
            empty_msg = "No items configured yet."
            
        await message.answer(
            f"{title}\n\n"
            f"📭 {empty_msg}",
            reply_markup=get_analytics_menu(),
            parse_mode="Markdown"
        )
        return
    
    # Fetch top items for current page with field projection (only needed fields)
    projection = {name_field: 1, click_field: 1, "index": 1, "cc_code": 1, "_id": 0}
    if category in ["pdf", "affiliate", "ig_start", "yt_start", "yt_code_start"]:
        projection["link"] = 1
        projection["affiliate_link"] = 1
        projection["ig_start_code"] = 1
        projection["yt_link"] = 1
        projection["msa_code"] = 1
    
    # Add timestamp fields for last clicked info
    if category == "pdf":
        projection["last_clicked_at"] = 1
    elif category == "affiliate":
        projection["last_affiliate_click"] = 1
    elif category == "ig_start":
        projection["last_ig_click"] = 1
    elif category == "yt_start":
        projection["last_yt_click"] = 1
    elif category == "ig_cc_start":
        projection["last_ig_cc_click"] = 1
    elif category == "yt_code_start":
        projection["last_yt_code_click"] = 1
    
    items = list(collection.find(query, projection).sort(click_field, -1).skip(skip).limit(items_per_page))
    
    if not items:
        await message.answer(
            "⚠️ No more items on this page.",
            reply_markup=get_analytics_menu()
        )
        return
    
    # Build display text
    text = f"{title}\n"
    text += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for idx, item in enumerate(items, start=skip + 1):
        item_name = item.get(name_field, "Unnamed")
        clicks = item.get(click_field, 0)
        
        # Get last clicked timestamp
        last_click_field = {
            "clicks": "last_clicked_at",
            "affiliate_clicks": "last_affiliate_click",
            "ig_start_clicks": "last_ig_click",
            "yt_start_clicks": "last_yt_click",
            "ig_cc_clicks": "last_ig_cc_click",
            "yt_code_clicks": "last_yt_code_click"
        }.get(click_field, "last_clicked_at")
        
        last_clicked = item.get(last_click_field)
        
        # Performance indicator
        if clicks == 0:
            indicator = "⚪"
        elif clicks < 10:
            indicator = "🟡"
        elif clicks < 50:
            indicator = "🟢"
        elif clicks < 100:
            indicator = "🔵"
        else:
            indicator = "🔥"
        
        text += f"{idx}. {indicator} **{item_name}**\n"
        text += f"   🔢 Clicks: **{clicks:,}**"
        
        if last_clicked:
            from datetime import datetime, timedelta
            now = datetime.now()
            time_diff = now - last_clicked
            
            if time_diff.days > 0:
                time_ago = f"{time_diff.days}d ago"
            elif time_diff.seconds >= 3600:
                time_ago = f"{time_diff.seconds // 3600}h ago"
            elif time_diff.seconds >= 60:
                time_ago = f"{time_diff.seconds // 60}m ago"
            else:
                time_ago = "just now"
            
            text += f" | 🕐 {time_ago}"
        elif clicks > 0:
            text += f" | 🕐 timestamp missing"
        
        text += "\n\n"
    
    text += f"━━━━━━━━━━━━━━━━━━━━\n"
    text += f"📊 Showing {skip + 1}-{skip + len(items)} of {total_items} items\n"
    
    # Pagination buttons
    keyboard = []
    nav_row = []
    
    if page > 0:
        nav_row.append(KeyboardButton(text=f"⬅️ PREV ({category})"))
    
    if skip + items_per_page < total_items:
        nav_row.append(KeyboardButton(text=f"➡️ NEXT ({category})"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([KeyboardButton(text="⬅️ BACK TO ANALYTICS")])
    
    await message.answer(
        text,
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )

@dp.message(F.text == "📄 PDF Clicks")
async def pdf_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="pdf", analytics_page=0)
    await send_analytics_view(message, "pdf", 0)

@dp.message(F.text == "💸 Affiliate Clicks")
async def affiliate_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="affiliate", analytics_page=0)
    await send_analytics_view(message, "affiliate", 0)

@dp.message(F.text == "📸 IG Start Clicks")
async def ig_start_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="ig_start", analytics_page=0)
    await send_analytics_view(message, "ig_start", 0)

@dp.message(F.text == "▶️ YT Start Clicks")
async def yt_start_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="yt_start", analytics_page=0)
    await send_analytics_view(message, "yt_start", 0)

@dp.message(F.text == "📸 IG CC Start Clicks")
async def ig_cc_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="ig_cc_start", analytics_page=0)
    await send_analytics_view(message, "ig_cc_start", 0)

@dp.message(F.text == "🔑 YT Code Start Clicks")
async def yt_code_clicks_handler(message: types.Message, state: FSMContext):
    await state.update_data(analytics_category="yt_code_start", analytics_page=0)
    await send_analytics_view(message, "yt_code_start", 0)

@dp.message(F.text.startswith("⬅️ PREV ("))
async def analytics_prev_handler(message: types.Message, state: FSMContext):
    """Handle previous page in analytics"""
    data = await state.get_data()
    category = data.get("analytics_category")
    current_page = data.get("analytics_page", 0)
    
    if current_page > 0:
        new_page = current_page - 1
        await state.update_data(analytics_page=new_page)
        await send_analytics_view(message, category, new_page)
    else:
        await message.answer("⚠️ Already on first page.")

@dp.message(F.text.startswith("➡️ NEXT ("))
async def analytics_next_handler(message: types.Message, state: FSMContext):
    """Handle next page in analytics"""
    data = await state.get_data()
    category = data.get("analytics_category")
    current_page = data.get("analytics_page", 0)
    
    new_page = current_page + 1
    await state.update_data(analytics_page=new_page)
    await send_analytics_view(message, category, new_page)

@dp.message(F.text == "⬅️ BACK TO ANALYTICS")
async def back_to_analytics_handler(message: types.Message, state: FSMContext):
    """Return to analytics menu"""
    await state.set_state(AnalyticsStates.viewing_analytics)
    await message.answer(
        "📊 **ANALYTICS DASHBOARD**",
        reply_markup=get_analytics_menu(),
        parse_mode="Markdown"
    )

# --- DATA RESET HANDLER ---
@dp.message(F.text == "⚠️ RESET BOT DATA")
async def start_reset_data(message: types.Message, state: FSMContext):
    # Security Check
    if message.from_user.id != MASTER_ADMIN_ID:
        await message.answer("⛔ **ACCESS DENIED.** Only the Master Admin can perform this action.")
        return

    await state.set_state(ResetStates.waiting_for_confirm_button)
    
    keyboard = [
        [KeyboardButton(text="🔴 CONFIRM RESET")],
        [KeyboardButton(text="❌ CANCEL")]
    ]
    await message.answer(
        "⚠️ **DANGER ZONE** ⚠️\n\n"
        "You have requested to **RESET ALL BOT DATA**.\n"
        "This will permanently delete:\n"
        "- All PDFs and Links\n"
        "- All IG Content\n"
        "- All Logs and Settings\n\n"
        "**THIS ACTION CANNOT BE UNDONE.**\n\n"
        "Please confirm using the button below:",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )

@dp.message(ResetStates.waiting_for_confirm_button)
async def process_reset_step1(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("✅ Reset Cancelled.", reply_markup=get_main_menu(message.from_user.id))
    
    if message.text == "🔴 CONFIRM RESET":
        await state.set_state(ResetStates.waiting_for_confirm_text)
        
        keyboard = [[KeyboardButton(text="❌ CANCEL")]]
        
        await message.answer(
            "🛑 **FINAL CONFIRMATION REQUIRED** 🛑\n\n"
            "To execute the reset protocol, type **CONFIRM** below.\n"
            "Any other input (or clicking Cancel) will stop the operation.",
            reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
    else:
        await message.answer("⚠️ Please select an option.")

@dp.message(ResetStates.waiting_for_confirm_text)
async def process_reset_final(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("✅ Reset Cancelled.", reply_markup=get_main_menu(message.from_user.id))
        
    if message.text.upper() == "CONFIRM":
        msg = await message.answer("🧨 **INITIATING SYSTEM WIPE...**", reply_markup=types.ReplyKeyboardRemove())
        
        try:
            # 1. Delete Collections
            db.drop_collection("bot9_pdfs")
            db.drop_collection("bot9_ig_content")
            db.drop_collection("bot9_logs")
            db.drop_collection("bot9_settings")
            db.drop_collection("bot9_backups")  # Added backup metadata collection
            
            # 2. Delete Log File
            log_file = "bot9.log"
            if os.path.exists(log_file):
                # Close logger handlers first? Python logging might lock file.
                # We can try truncating it instead of deleting if locked.
                with open(log_file, "w"):
                    pass
            
            # 3. Delete Backup Files
            backup_dir = "backups"
            if os.path.exists(backup_dir):
                import shutil
                shutil.rmtree(backup_dir)
                logger.info("✅ Deleted all backup files")
            
            # Log Action (to new log file)
            log_user_action(message.from_user, "PERFORMED FULL SYSTEM RESET")
            
            await message.answer(
                "✅ **SYSTEM RESET COMPLETE**\n"
                "All data has been eradicated.\n"
                "System is clean.",
                reply_markup=get_main_menu(message.from_user.id),
                parse_mode="Markdown"
            )
        except Exception as e:
            await message.answer(f"❌ **RESET FAILED:** {e}", reply_markup=get_main_menu(message.from_user.id))
            
        await state.clear()
    else:
        await state.clear()
        await message.answer("✅ Reset Cancelled. Input did not match 'CONFIRM'.", reply_markup=get_main_menu(message.from_user.id))

# 5. IG AFFILIATE MANAGEMENT HANDLERS
# ==========================================

# Main Affiliate Menu Handler
@dp.message(F.text == "📎 ADD AFFILIATE")
async def ig_affiliate_menu_handler(message: types.Message):
    if not await check_authorization(message, "IG Affiliate Menu", "can_add"):
        return
    """Show IG Affiliate Submenu"""
    await message.answer(
        "📎 **IG AFFILIATE MANAGEMENT**\n\nSelect an option:",
        reply_markup=get_ig_affiliate_menu(),
        parse_mode="Markdown"
    )

# Back button from affiliate submenu to IG menu
@dp.message(F.text == "◀️ Back")
async def ig_affiliate_back_handler(message: types.Message, state: FSMContext):
    """Return from affiliate menu to IG menu"""
    await state.clear()
    await message.answer("📸 **IG CODE MANAGEMENT**", reply_markup=get_ig_menu(), parse_mode="Markdown")

# 5a. ADD AFFILIATE TO IG CONTENT
@dp.message(F.text == "📎 Add")
async def start_add_ig_affiliate(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add IG Affiliate", "can_add"):
        return
    """Start Add Affiliate flow"""
    await state.set_state(IGAffiliateStates.waiting_for_ig_selection)
    await send_ig_list_view(message, page=0, mode="ig_affiliate_select")

@dp.message(IGAffiliateStates.waiting_for_ig_selection)
async def process_ig_affiliate_selection(message: types.Message, state: FSMContext):
    """Process IG selection for adding affiliate"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="ig_affiliate_select")
            return
        except: pass
    
    raw_input = message.text.strip()
    queries = [q.strip() for q in raw_input.split(",")]
    
    # Get all contents sorted by cc_number for sequential resolution
    all_contents = list(col_ig_content.find().sort("cc_number", 1))
    
    found_contents = []
    seen_ids = set()
    not_found = []
    
    for q in queries:
        if not q: continue
        
        content = None
        if q.isdigit():
            # Sequential selection
            idx = int(q) - 1
            if 0 <= idx < len(all_contents):
                content = all_contents[idx]
        elif q.upper().startswith("CC"):
            # CC Code match
            content = next((c for c in all_contents if c['cc_code'].upper() == q.upper()), None)
            
        if content:
            cid = str(content["_id"])
            if cid not in seen_ids:
                seen_ids.add(cid)
                found_contents.append(content)
        else:
            not_found.append(q)
            
    if not found_contents:
        msg = "❌ **No Content Found**"
        if not_found:
             msg += "\nNot found: " + ", ".join(not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard())
        return
    
    # Store IDs
    affiliate_ids = [str(c["_id"]) for c in found_contents]
    
    await state.update_data(affiliate_ids=affiliate_ids)
    await state.set_state(IGAffiliateStates.waiting_for_link)
    
    msg = f"✅ **Selected {len(found_contents)} IG items for Affiliate Link:**\n\n"
    for c in found_contents:
        msg += f"• {c['cc_code']} - {c['name']}\n"
    
    msg += "\n🔗 **Enter Affiliate Link (applies to ALL above):**"
    
    await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")

@dp.message(IGAffiliateStates.waiting_for_link)
async def process_ig_affiliate_link(message: types.Message, state: FSMContext):
    """Process affiliate link input"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    link = message.text.strip()
    
    # Basic validation
    if "http" not in link:
        await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
        return
    
    data = await state.get_data()
    from bson.objectid import ObjectId
    
    affiliate_ids = data.get('affiliate_ids', [])
    
    if affiliate_ids:
        object_ids = [ObjectId(uid) for uid in affiliate_ids]
        col_ig_content.update_many(
            {"_id": {"$in": object_ids}},
            {"$set": {"affiliate_link": link}}
        )
        
        log_user_action(message.from_user, "Bulk Added IG Affiliate", f"Count: {len(affiliate_ids)}")
        
        await state.clear()
        await message.answer(
            f"✅ **Bulk Affiliate Link Applied!**\n\n"
            f"🔗 Link: `{link}`\n"
            f"📊 Applied to {len(affiliate_ids)} items.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="Markdown"
        )
    else:
        await state.clear()
        await message.answer("❌ Error: No items selected.", reply_markup=get_ig_affiliate_menu())

# 5b. EDIT IG AFFILIATE
@dp.message(F.text == "✏️ Edit")
async def start_edit_ig_affiliate(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Edit IG Affiliate", "can_add"):
        return
    """Start Edit Affiliate flow"""
    # Check if any IG content has affiliate links
    count = col_ig_content.count_documents({"affiliate_link": {"$exists": True, "$ne": ""}})
    if count == 0:
        return await message.answer(
            "⚠️ **No affiliate links found!**\n\nAdd an affiliate link first.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="Markdown"
        )
    
    await state.set_state(IGAffiliateEditStates.waiting_for_selection)
    await send_ig_list_view(message, page=0, mode="ig_affiliate_edit")

@dp.message(IGAffiliateEditStates.waiting_for_selection)
async def process_ig_affiliate_edit_selection(message: types.Message, state: FSMContext):
    """Process IG selection for editing affiliate"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="ig_affiliate_edit")
            return
        except: pass
    
    query = message.text.strip()
    content = None
    
    # Try by index (display index)
    if query.isdigit():
        idx = int(query) - 1
        all_contents = list(col_ig_content.find({"affiliate_link": {"$exists": True, "$ne": ""}}).sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({
            "cc_code": {"$regex": f"^{query}$", "$options": "i"},
            "affiliate_link": {"$exists": True, "$ne": ""}
        })
    
    if not content:
        await message.answer("❌ Content Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(
        content_id=str(content["_id"]),
        cc_code=content["cc_code"],
        name=content["name"],
        old_link=content.get("affiliate_link", "")
    )
    await state.set_state(IGAffiliateEditStates.waiting_for_new_link)
    
    await message.answer(
        f"✅ **Selected:** {content['cc_code']} - {content['name']}\n\n"
        f"📎 Current Link: {content.get('affiliate_link', 'N/A')}\n\n"
        f"🔗 **Enter New Affiliate Link:**",
        reply_markup=get_cancel_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(IGAffiliateEditStates.waiting_for_new_link)
async def process_ig_affiliate_edit_link(message: types.Message, state: FSMContext):
    """Process new affiliate link"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    link = message.text.strip()
    
    # Basic validation
    if "http" not in link:
        await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
        return
    
    data = await state.get_data()
    
    # Check if link is same as old link
    if link == data.get('old_link'):
        await message.answer("⚠️ **Link is identical to current link.**\nNo changes made.", reply_markup=get_ig_affiliate_menu())
        await state.clear()
        return

    from bson.objectid import ObjectId
    
    # Update affiliate link
    col_ig_content.update_one(
        {"_id": ObjectId(data['content_id'])},
        {"$set": {"affiliate_link": link}}
    )
    
    log_user_action(message.from_user, "Edited IG Affiliate", f"Code: {data['cc_code']}")
    
    await state.clear()
    await message.answer(
        f"✅ **Affiliate Link Updated!**\n\n"
        f"🆔 Code: {data['cc_code']}\n"
        f"📝 Content: {data['name']}\n"
        f"🔗 New Link: {link}",
        reply_markup=get_ig_affiliate_menu(),
        parse_mode="Markdown"
    )

# 5c. DELETE IG AFFILIATE
@dp.message(F.text == "🗑️ Delete")
async def start_delete_ig_affiliate(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Delete IG Affiliate", "can_add"):
        return
    """Start Delete Affiliate flow"""
    # Check if any IG content has affiliate links
    count = col_ig_content.count_documents({"affiliate_link": {"$exists": True, "$ne": ""}})
    if count == 0:
        return await message.answer(
            "⚠️ **No affiliate links found!**\n\nNothing to delete.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="Markdown"
        )
    
    await state.set_state(IGAffiliateDeleteStates.waiting_for_selection)
    await send_ig_list_view(message, page=0, mode="ig_affiliate_delete")

@dp.message(IGAffiliateDeleteStates.waiting_for_selection)
async def process_ig_affiliate_delete_selection(message: types.Message, state: FSMContext):
    """Process IG selection for deleting affiliate"""
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_affiliate_menu())
    
    # Handle Pagination
    if message.text.startswith("⬅️ PREV") or message.text.startswith("➡️ NEXT"):
        try:
            page = int(message.text.split()[-1]) - 1
            await send_ig_list_view(message, page=page, mode="ig_affiliate_delete")
            return
        except: pass
    
    query = message.text.strip()
    content = None
    
    # Try by index (display index)
    if query.isdigit():
        idx = int(query) - 1
        all_contents = list(col_ig_content.find({"affiliate_link": {"$exists": True, "$ne": ""}}).sort("cc_number", 1))
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
    # Try by CC code
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({
            "cc_code": {"$regex": f"^{query}$", "$options": "i"},
            "affiliate_link": {"$exists": True, "$ne": ""}
        })
    
    if not content:
        await message.answer("❌ Content Not Found. Try again or Cancel.", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(
        content_id=str(content["_id"]),
        cc_code=content["cc_code"],
        name=content["name"],
        affiliate_link=content.get("affiliate_link", "")
    )
    await state.set_state(IGAffiliateDeleteStates.waiting_for_confirm)
    
    keyboard = [[KeyboardButton(text="✅ CONFIRM"), KeyboardButton(text="❌ CANCEL")]]
    confirm_kb = ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    await message.answer(
        f"⚠️ **CONFIRM DELETE AFFILIATE**\n\n"
        f"🆔 Code: {content['cc_code']}\n"
        f"📝 Content: {content['name']}\n"
        f"🔗 Link: {content.get('affiliate_link', '')}\n\n"
        f"Are you sure?",
        reply_markup=confirm_kb,
        parse_mode="Markdown"
    )

@dp.message(IGAffiliateDeleteStates.waiting_for_confirm)
async def process_ig_affiliate_delete_confirm(message: types.Message, state: FSMContext):
    """Process delete confirmation"""
    if message.text == "✅ CONFIRM":
        data = await state.get_data()
        from bson.objectid import ObjectId
        
        # Remove affiliate link from IG content
        col_ig_content.update_one(
            {"_id": ObjectId(data['content_id'])},
            {"$unset": {"affiliate_link": ""}}
        )
        
        log_user_action(message.from_user, "Deleted IG Affiliate", f"Code: {data['cc_code']}")
        
        await state.clear()
        await message.answer(
            f"🗑️ **Affiliate Link Deleted!**\n\n"
            f"🆔 Code: {data['cc_code']}\n"
            f"📝 Content: {data['name']}",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="Markdown"
        )
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_ig_affiliate_menu())

# 5d. LIST IG AFFILIATES
# 5d. PAGINATED IG AFFILIATE LIST
async def send_ig_affiliate_list_view_text(message: types.Message, page=0):
    """Helper to send paginated text list of affiliates"""
    limit = 5  # Limit 5 items per page as requested
    skip = page * limit
    
    query = {"affiliate_link": {"$exists": True, "$ne": ""}}
    total = col_ig_content.count_documents(query)
    contents = list(col_ig_content.find(query).sort("cc_number", 1).skip(skip).limit(limit))
    
    if not contents and page == 0:
        return await message.answer(
            "⚠️ **No affiliate links found!**\n\nAdd an affiliate link first.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="Markdown"
        )
    
    text = f"📋 **IG CONTENT WITH AFFILIATE LINKS (Page {page+1}):**\nResult {skip+1}-{min(skip+len(contents), total)} of {total}\n━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for idx, content in enumerate(contents, start=skip+1):
        text += f"{idx}. **{content['cc_code']}**\n"
        text += f"   🔗 {content.get('affiliate_link', 'N/A')}\n\n"
    
    text += f"━━━━━━━━━━━━━━━━━━━━\nTotal: **{total}** affiliate link(s)"
    
    # Pagination Keyboard
    buttons = []
    if page > 0: 
        buttons.append(KeyboardButton(text=f"⬅️ PREV_AFF {page}"))
    if (skip + limit) < total: 
        buttons.append(KeyboardButton(text=f"➡️ NEXT_AFF {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="◀️ Back")]) # Navigate back to affiliate menu
    
    size_mb = sys.getsizeof(text) # Basic size check
    if len(text) > 4000:
        # Split logic if dangerously huge (fallback)
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
             await message.answer(part, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")
    else:
        await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")

@dp.message(F.text == "📋 List")
async def list_ig_affiliates_handler(message: types.Message):
    if not await check_authorization(message, "List IG Affiliates", "can_list"):
        return
    """List all IG content with affiliate links"""
    await send_ig_affiliate_list_view_text(message, page=0)

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_AFF") or m.text.startswith("➡️ NEXT_AFF")))
async def ig_affiliate_pagination_handler(message: types.Message):
    """Handle pagination for affiliate text list"""
    try:
        page = int(message.text.split()[-1]) - 1
        await send_ig_affiliate_list_view_text(message, page)
    except:
        await send_ig_affiliate_list_view_text(message, 0)

# END OF IG AFFILIATE MANAGEMENT HANDLERS


# --- Placeholders ---

@dp.message(F.text.in_({"📋 LIST", "🔍 SEARCH", "🔗 LINKS"}))
async def not_implemented_handler(message: types.Message):
    """Handler for main menu features not yet implemented"""
    await message.answer("🚧 This feature is coming soon!")

# ==========================================
# 💾 BACKUP SYSTEM
# ==========================================

# Backup collection for metadata
col_backups = db["bot9_backups"]

def get_month_year_name():
    """Get current month and year in format: 2026_February"""
    now = datetime.now()
    month_name = now.strftime("%B")  # Full month name
    year = now.year
    return f"{year}_{month_name}"

async def create_backup_file(auto=False):
    """
    Create compressed backup file with all data.
    Returns: (success: bool, filepath: str, metadata: dict)
    """
    import zipfile
    import json
    import tempfile
    
    try:
        # Create backups directory
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        
        # Get month/year naming
        month_year = get_month_year_name()
        filename = f"Backup_{month_year}.zip"
        filepath = os.path.join(backup_dir, filename)
        
        # Check if backup already exists for this month
        if os.path.exists(filepath) and auto:
            logger.info(f"Backup for {month_year} already exists, skipping auto-backup")
            return True, filepath, None
        
        # Collect all data
        all_pdfs = list(col_pdfs.find({}))
        all_ig_content = list(col_ig_content.find({}))
        
        # Convert ObjectId and datetime to string for JSON serialization
        for pdf in all_pdfs:
            if '_id' in pdf:
                pdf['_id'] = str(pdf['_id'])
            for field in ['created_at', 'last_clicked_at', 'last_affiliate_click', 'last_ig_click', 'last_yt_click', 'last_yt_code_click']:
                if field in pdf and isinstance(pdf[field], datetime):
                    pdf[field] = pdf[field].strftime("%Y-%m-%d %I:%M:%S %p")
        
        for ig in all_ig_content:
            if '_id' in ig:
                ig['_id'] = str(ig['_id'])
            for field in ['created_at', 'last_ig_cc_click']:
                if field in ig and isinstance(ig[field], datetime):
                    ig[field] = ig[field].strftime("%Y-%m-%d %I:%M:%S %p")
        
        # Calculate statistics
        total_clicks = sum(p.get('clicks', 0) for p in all_pdfs)
        total_ig_clicks = sum(p.get('ig_start_clicks', 0) for p in all_pdfs)
        total_yt_clicks = sum(p.get('yt_start_clicks', 0) for p in all_pdfs)
        total_ig_cc_clicks = sum(ig.get('ig_cc_clicks', 0) for ig in all_ig_content)
        
        # Create metadata
        metadata = {
            "backup_type": "auto" if auto else "manual",
            "created_at": datetime.now(),
            "month": datetime.now().strftime("%B"),
            "year": datetime.now().year,
            "filename": filename,
            "pdfs_count": len(all_pdfs),
            "ig_count": len(all_ig_content),
            "total_clicks": total_clicks,
            "total_ig_clicks": total_ig_clicks,
            "total_yt_clicks": total_yt_clicks,
            "total_ig_cc_clicks": total_ig_cc_clicks
        }
        
        # Create temporary JSON files
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_pdfs:
            json.dump(all_pdfs, temp_pdfs, indent=2, default=str)
            temp_pdfs_path = temp_pdfs.name
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_ig:
            json.dump(all_ig_content, temp_ig, indent=2, default=str)
            temp_ig_path = temp_ig.name
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_meta:
            meta_for_file = metadata.copy()
            meta_for_file['created_at'] = meta_for_file['created_at'].strftime("%Y-%m-%d %I:%M:%S %p")
            json.dump(meta_for_file, temp_meta, indent=2)
            temp_meta_path = temp_meta.name
        
        # Create ZIP file with compression
        with zipfile.ZipFile(filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(temp_pdfs_path, "pdfs.json")
            zipf.write(temp_ig_path, "ig_content.json")
            zipf.write(temp_meta_path, "metadata.json")
        
        # Clean up temp files
        os.remove(temp_pdfs_path)
        os.remove(temp_ig_path)
        os.remove(temp_meta_path)
        
        # Get file size
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        metadata['file_size_mb'] = round(file_size_mb, 2)
        
        # Save metadata to database
        col_backups.insert_one(metadata)
        
        logger.info(f"✅ Backup created: {filename} ({file_size_mb:.2f} MB)")
        
        return True, filepath, metadata
        
    except Exception as e:
        logger.error(f"❌ Backup creation failed: {e}")
        return False, None, None

# NOTE: Auto-cleanup removed - all backups are kept permanently for data integrity
# This follows industry best practices where critical data is archived indefinitely

async def auto_backup_task():
    """Background task that creates monthly backups automatically"""
    while True:
        try:
            now = datetime.now()
            
            # Run on 1st of month at 2 AM
            if now.day == 1 and now.hour == 2:
                logger.info("🔄 Starting auto-backup...")
                success, filepath, metadata = await create_backup_file(auto=True)
                
                if success and metadata:
                    logger.info(f"✅ Auto-backup completed: {metadata['filename']}")
                    
                    # Notify master admin of successful backup
                    try:
                        await bot.send_message(
                            MASTER_ADMIN_ID,
                            f"✅ **AUTO-BACKUP SUCCESSFUL**\n\n"
                            f"📦 File: `{metadata['filename']}`\n"
                            f"💾 Size: {metadata['file_size_mb']:.2f} MB\n"
                            f"📊 PDFs: {metadata['pdfs_count']} | IG: {metadata['ig_count']}\n"
                            f"🕐 Time: {now.strftime('%I:%M %p')}",
                            parse_mode="Markdown"
                        )
                    except:
                        pass
                else:
                    # CRITICAL: Notify admin of backup failure
                    try:
                        await bot.send_message(
                            MASTER_ADMIN_ID,
                            f"🚨 **AUTO-BACKUP FAILED!**\n\n"
                            f"⚠️ The scheduled monthly backup could not be created.\n"
                            f"📅 Date: {now.strftime('%B %d, %Y')}\n"
                            f"🕐 Time: {now.strftime('%I:%M %p')}\n\n"
                            f"Please check the system immediately!",
                            parse_mode="Markdown"
                        )
                    except:
                        logger.error("Could not notify admin of backup failure!")
                
                # Sleep for 2 hours to avoid re-triggering
                await asyncio.sleep(7200)
            else:
                # Check every hour
                await asyncio.sleep(3600)
                
        except Exception as e:
            logger.error(f"❌ Auto-backup task error: {e}")
            
            # CRITICAL: Notify admin of system error
            try:
                await bot.send_message(
                    MASTER_ADMIN_ID,
                    f"🚨 **BACKUP SYSTEM ERROR!**\n\n"
                    f"❌ Error: `{str(e)}`\n"
                    f"🕐 Time: {datetime.now().strftime('%I:%M %p')}\n\n"
                    f"The auto-backup system encountered an error.",
                    parse_mode="Markdown"
                )
            except:
                logger.error("Could not notify admin of backup system error!")
            
            await asyncio.sleep(3600)  # Wait an hour before retrying

@dp.message(F.text == "💾 BACKUP DATA")
async def backup_menu_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Backup Menu", "can_manage_admins"):
        return
    """Show backup menu"""
    await state.set_state(BackupStates.viewing_backup_menu)
    await message.answer(
        "💾 **BACKUP & EXPORT**\n\n"
        "Choose a backup option:\n\n"
        "💾 **FULL BACKUP** - Export all data with timestamps\n"
        "📋 **VIEW AS JSON** - See backup in JSON format\n"
        "📊 **BACKUP STATS** - View database statistics\n"
        "📜 **BACKUP HISTORY** - View all monthly backup reports\n\n"
        "Select an option:",
        reply_markup=get_backup_menu(),
        parse_mode="Markdown"
    )

@dp.message(F.text == "💾 FULL BACKUP")
async def full_backup_handler(message: types.Message):
    if not await check_authorization(message, "Full Backup", "can_manage_admins"):
        return
    """Create full backup of all data"""
    try:
        # Show processing message
        processing_msg = await message.answer("⏳ Creating compressed backup file...")
        
        # Create backup file
        success, filepath, metadata = await create_backup_file(auto=False)
        
        # Delete processing message
        await processing_msg.delete()
        
        if not success:
            await message.answer("❌ Backup failed. Please try again later.")
            
            # CRITICAL: Notify master admin of manual backup failure
            try:
                await bot.send_message(
                    MASTER_ADMIN_ID,
                    f"🚨 **MANUAL BACKUP FAILED!**\n\n"
                    f"⚠️ User: {message.from_user.first_name or 'Unknown'} (ID: {message.from_user.id})\n"
                    f"📅 Date: {datetime.now().strftime('%B %d, %Y')}\n"
                    f"🕐 Time: {datetime.now().strftime('%I:%M %p')}\n\n"
                    f"Please investigate the backup system!",
                    parse_mode="Markdown"
                )
            except:
                logger.error("Could not notify admin of manual backup failure!")
            
            return
        
        # Get timestamp
        now = datetime.now()
        timestamp_12h = now.strftime("%Y-%m-%d %I:%M:%S %p")
        
        # Build backup summary
        backup_text = f"✅ **BACKUP CREATED SUCCESSFULLY!**\n"
        backup_text += f"═══════════════════════════\n\n"
        backup_text += f"📦 **File:** `{metadata['filename']}`\n"
        backup_text += f"💾 **Size:** {metadata['file_size_mb']:.2f} MB\n"
        backup_text += f"🕐 **Created:** {timestamp_12h}\n\n"
        
        backup_text += f"📊 **DATA SUMMARY:**\n"
        backup_text += f"├ 📄 PDFs: {metadata['pdfs_count']}\n"
        backup_text += f"└ 📸 IG Content: {metadata['ig_count']}\n\n"
        
        backup_text += f"🎯 **CLICK STATISTICS:**\n"
        backup_text += f"├ Total Clicks: {metadata['total_clicks']:,}\n"
        backup_text += f"├ YT Clicks: {metadata['total_yt_clicks']:,}\n"
        backup_text += f"└ IGCC Clicks: {metadata['total_ig_cc_clicks']:,}\n\n"
        
        backup_text += f"═══════════════════════════\n"
        backup_text += f"💡 **Backup Location:**\n`backups/{metadata['filename']}`\n\n"
        backup_text += f"🔒 Data is compressed and saved securely!"
        
        await message.answer(backup_text, parse_mode="Markdown")
        
        # Log the backup action
        log_user_action(message.from_user, "FULL_BACKUP", f"Created {metadata['filename']} ({metadata['file_size_mb']:.2f} MB)")
        
    except Exception as e:
        logger.error(f"Backup error: {e}")
        await message.answer("❌ Backup failed. Please try again later.")
        
        # CRITICAL: Notify master admin of backup exception
        try:
            await bot.send_message(
                MASTER_ADMIN_ID,
                f"🚨 **BACKUP EXCEPTION!**\n\n"
                f"❌ Error: `{str(e)}`\n"
                f"👤 User: {message.from_user.first_name or 'Unknown'} (ID: {message.from_user.id})\n"
                f"🕐 Time: {datetime.now().strftime('%I:%M %p')}\n\n"
                f"Check the backup system immediately!",
                parse_mode="Markdown"
            )
        except:
            logger.error("Could not notify admin of backup exception!")



@dp.message(F.text == "📋 VIEW AS JSON")
async def view_json_backup_handler(message: types.Message):
    if not await check_authorization(message, "View JSON Backup", "can_manage_admins"):
        return
    """Export backup as JSON format"""
    try:
        # Show processing message
        processing_msg = await message.answer("⏳ Generating JSON backup...")
        
        # Get current timestamp
        now = datetime.now()
        timestamp_12h = now.strftime("%Y-%m-%d %I:%M:%S %p")
        filename_timestamp = now.strftime("%Y-%m-%d_%I-%M-%S_%p")
        
        # Collect all data
        all_pdfs = list(col_pdfs.find({}))
        all_ig_content = list(col_ig_content.find({}))
        
        # Convert ObjectId and datetime to strings
        import json
        for pdf in all_pdfs:
            if '_id' in pdf:
                pdf['_id'] = str(pdf['_id'])
            for field in ['created_at', 'last_clicked_at', 'last_affiliate_click', 'last_ig_click', 'last_yt_click', 'last_yt_code_click']:
                if field in pdf and isinstance(pdf[field], datetime):
                    pdf[field] = pdf[field].strftime("%Y-%m-%d %I:%M:%S %p")
        
        for ig in all_ig_content:
            if '_id' in ig:
                ig['_id'] = str(ig['_id'])
            for field in ['created_at', 'last_ig_cc_click']:
                if field in ig and isinstance(ig[field], datetime):
                    ig[field] = ig[field].strftime("%Y-%m-%d %I:%M:%S %p")
        
        # Create JSON structure
        backup_data = {
            "backup_timestamp": timestamp_12h,
            "backup_timezone": "Local Time",
            "total_pdfs": len(all_pdfs),
            "total_ig_content": len(all_ig_content),
            "pdfs": all_pdfs,
            "ig_content": all_ig_content
        }
        
        # Convert to JSON string with pretty formatting
        json_output = json.dumps(backup_data, indent=2, ensure_ascii=False)
        
        # Save to file
        os.makedirs('backups', exist_ok=True)
        filename = f"backups/backup_{filename_timestamp}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(json_output)
        
        # Delete processing message
        await processing_msg.delete()
        
        # Send file to user
        await message.answer_document(
            types.FSInputFile(filename),
            caption=f"📋 **JSON BACKUP**\n\n"
                   f"🕐 Time: {timestamp_12h}\n"
                   f"📊 PDFs: {len(all_pdfs)}\n"
                   f"📸 IG Content: {len(all_ig_content)}\n\n"
                   f"✅ Backup exported successfully!",
            parse_mode="Markdown"
        )
        
        # Log the action
        log_user_action(message.from_user, "JSON_BACKUP", f"Exported as {filename}")
        
    except Exception as e:
        logger.error(f"JSON backup error: {e}")
        # Use plain text for error message to avoid markdown parsing issues
        await message.answer("❌ JSON backup failed. Please try again later.")

@dp.message(F.text == "📊 BACKUP STATS")
async def backup_stats_handler(message: types.Message):
    if not await check_authorization(message, "Backup Stats", "can_manage_users"):
        return
    """Show database statistics"""
    try:
        # Get collection stats
        pdf_count = col_pdfs.count_documents({})
        ig_count = col_ig_content.count_documents({})
        
        # Get database size (approximate)
        db_stats = db.command("dbstats")
        db_size_mb = db_stats.get("dataSize", 0) / (1024 * 1024)  # Convert to MB
        
        # Get collection details
        pdf_stats = db.command("collStats", "bot9_pdfs")
        ig_stats = db.command("collStats", "bot9_ig_content")
        
        pdf_size_mb = pdf_stats.get("size", 0) / (1024 * 1024)
        ig_size_mb = ig_stats.get("size", 0) / (1024 * 1024)
        
        # Build stats message
        stats_text = f"📊 **DATABASE STATISTICS**\n"
        stats_text += f"═══════════════════════════\n\n"
        
        stats_text += f"💾 **STORAGE:**\n"
        stats_text += f"├ Total DB Size: {db_size_mb:.2f} MB\n"
        stats_text += f"├ PDFs Collection: {pdf_size_mb:.2f} MB\n"
        stats_text += f"└ IG Collection: {ig_size_mb:.2f} MB\n\n"
        
        stats_text += f"📁 **COLLECTIONS:**\n"
        stats_text += f"├ `bot9_pdfs`: {pdf_count:,} documents\n"
        stats_text += f"└ `bot9_ig_content`: {ig_count:,} documents\n\n"
        
        # Index information
        pdf_indexes = col_pdfs.list_indexes()
        ig_indexes = col_ig_content.list_indexes()
        
        pdf_index_count = sum(1 for _ in pdf_indexes)
        ig_index_count = sum(1 for _ in ig_indexes)
        
        stats_text += f"🔍 **INDEXES:**\n"
        stats_text += f"├ PDFs: {pdf_index_count} indexes\n"
        stats_text += f"└ IG Content: {ig_index_count} indexes\n\n"
        
        # Recent activity
        recent_pdfs = col_pdfs.count_documents({"created_at": {"$gte": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)}})
        recent_ig = col_ig_content.count_documents({"created_at": {"$gte": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)}})
        
        stats_text += f"📈 **TODAY'S ACTIVITY:**\n"
        stats_text += f"├ New PDFs: {recent_pdfs}\n"
        stats_text += f"└ New IG Content: {recent_ig}\n\n"
        
        stats_text += f"═══════════════════════════\n"
        stats_text += f"🕐 **Updated:** {datetime.now().strftime('%I:%M:%S %p')}"
        
        await message.answer(stats_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        # Use plain text for error message to avoid markdown parsing issues
        await message.answer(f"❌ Failed to retrieve stats. Please try again later.")

@dp.message(F.text == "📜 BACKUP HISTORY")
async def backup_history_handler(message: types.Message):
    """Show complete backup history with all monthly reports"""
    if not await check_authorization(message, "View Backup History", "can_manage_users"):
        return
    
    try:
        # Get all backups sorted by creation date (newest first)
        all_backups = list(col_backups.find().sort("created_at", -1))
        
        if not all_backups:
            await message.answer(
                "📜 **BACKUP HISTORY**\n\n"
                "No backups found in the system.\n\n"
                "💡 Use **💾 FULL BACKUP** to create your first backup!",
                parse_mode="Markdown"
            )
            return
        
        # Build history report
        history = "📜 **BACKUP HISTORY REPORT**\n"
        history += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        history += f"📊 **Total Backups:** {len(all_backups)}\n\n"
        
        # Group by year for better organization
        backups_by_year = {}
        for backup in all_backups:
            year = backup.get('year', datetime.now().year)
            if year not in backups_by_year:
                backups_by_year[year] = []
            backups_by_year[year].append(backup)
        
        # Display backups grouped by year
        for year in sorted(backups_by_year.keys(), reverse=True):
            history += f"📅 **{year}**\n"
            history += "═" * 30 + "\n"
            
            for backup in backups_by_year[year]:
                month = backup.get('month', 'Unknown')
                filename = backup.get('filename', 'Unknown')
                size_mb = backup.get('file_size_mb', 0)
                backup_type = backup.get('backup_type', 'manual')
                created_at = backup.get('created_at')
                
                # Format timestamp
                if created_at:
                    if isinstance(created_at, str):
                        time_str = created_at
                    else:
                        time_str = created_at.strftime("%b %d, %I:%M %p")
                else:
                    time_str = "Unknown"
                
                # Type emoji
                type_emoji = "🔄" if backup_type == "auto" else "👤"
                
                history += f"\n{type_emoji} **{month}**\n"
                history += f"├ File: `{filename}`\n"
                history += f"├ Size: {size_mb:.2f} MB\n"
                history += f"├ PDFs: {backup.get('pdfs_count', 0)} | IG: {backup.get('ig_count', 0)}\n"
                history += f"├ Clicks: {backup.get('total_clicks', 0):,}\n"
                history += f"└ Created: {time_str}\n"
            
            history += "\n"
        
        history += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        history += "🔄 = Auto-backup | 👤 = Manual backup\n\n"
        history += "💡 All backups are stored permanently"
        
        await message.answer(history, parse_mode="Markdown")
        log_user_action(message.from_user, "VIEW_BACKUP_HISTORY", f"Viewed {len(all_backups)} backups")
        
    except Exception as e:
        logger.error(f"Backup history error: {e}")
        await message.answer("❌ Failed to load backup history. Please try again later.")


# ==========================================
# ADMIN MANAGEMENT HANDLERS
# ==========================================

@dp.message(F.text == "👥 ADMINS")
async def admin_menu_handler(message: types.Message):
    """Show Admin Management Menu"""
    if not await check_authorization(message, "Access Admin Menu", "can_manage_admins"):
        return
    
    await message.answer("🔐 **Admin Management**\nSelect an option below:", reply_markup=get_admin_config_menu(), parse_mode="Markdown")

@dp.message(F.text == "📋 LIST ADMINS")
async def list_admins_handler(message: types.Message, state: FSMContext):
    """List all admins from database with pagination"""
    if not await check_authorization(message, "List Admins", "can_manage_admins"):
        return
    
    # Reset page to 0
    await state.update_data(admin_page=0)
    await state.set_state(AdminManagementStates.viewing_admin_list)
    await send_admin_list_view(message, page=0)

async def send_admin_list_view(message: types.Message, page: int = 0):
    """Display paginated list of admins"""
    ADMINS_PER_PAGE = 5
    skip = page * ADMINS_PER_PAGE

    try:
        total_admins = col_admins.count_documents({})
        admins = list(col_admins.find({}).skip(skip).limit(ADMINS_PER_PAGE))

        if not admins and page == 0:
            # Build keyboard with just back button
            kb = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="⬅️ BACK TO ADMIN MENU")]],
                resize_keyboard=True
            )
            await message.answer("📋 **Admin List**\n\nNo admins found in the database.", reply_markup=kb, parse_mode="Markdown")
            return

        # Build message
        total_pages = max(1, (total_admins + ADMINS_PER_PAGE - 1) // ADMINS_PER_PAGE)
        text = f"📋 **Admin List** — Page {page + 1}/{total_pages} ({total_admins} total)\n"
        text += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for i, admin in enumerate(admins, start=skip + 1):
            uid = admin.get("user_id", "?")
            name = admin.get("full_name", "Unknown")
            username = admin.get("username", "")
            perms = admin.get("permissions", [])
            perm_count = len(perms) if perms else 0
            is_locked = admin.get("is_locked", False)
            status_str = "[🔒 LOCKED]" if is_locked else "[🔓 ACTIVE]"

            username_str = f"@{username}" if username and username != "Unknown" else "No username"
            text += f"**{i}.** `{uid}` — {name} {status_str}\n"
            text += f"   {username_str} | 🔑 {perm_count} permissions\n\n"

        text += "━━━━━━━━━━━━━━━━━━━━━━━━"

        # Build navigation keyboard
        nav_buttons = []
        if page > 0:
            nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
        if (skip + ADMINS_PER_PAGE) < total_admins:
            nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))

        keyboard_rows = []
        if nav_buttons:
            keyboard_rows.append(nav_buttons)

        keyboard_rows.append([KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")])

        kb = ReplyKeyboardMarkup(keyboard=keyboard_rows, resize_keyboard=True)
        await message.answer(text, reply_markup=kb, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error in send_admin_list_view: {e}")
        await message.answer(f"❌ Error loading admin list: {e}")

@dp.message(AdminManagementStates.viewing_admin_list, F.text == "➡️ NEXT ADMINS")
async def next_admin_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("admin_page", 0) + 1
    await state.update_data(admin_page=page)
    await send_admin_list_view(message, page)

@dp.message(AdminManagementStates.viewing_admin_list, F.text == "⬅️ PREV ADMINS")
async def prev_admin_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(0, data.get("admin_page", 0) - 1)
    await state.update_data(admin_page=page)
    await send_admin_list_view(message, page)

@dp.message(F.text.contains("BACK"))
@dp.message(F.text == "⬅️ RETURN BACK")
async def admin_list_back(message: types.Message, state: FSMContext):
    """Return to Admin Management menu from list view"""
    if not await check_authorization(message, "Admin Menu", "can_manage_admins"):
        return
    await state.clear()
    await message.answer(
        "🔐 **Admin Management**\nSelect an option below:",
        reply_markup=get_admin_config_menu(),
        parse_mode="Markdown"
    )



# ==========================================
# BAN CONFIGURATION HANDLERS
# ==========================================

@dp.message(F.text == "🏠 MAIN MENU")
async def main_menu_handler(message: types.Message, state: FSMContext):
    """Return to Main Menu (globally available for admins)"""
    if not await check_authorization(message, "Main Menu"):
        return
        
    await state.clear()
    await message.answer(
        "👋 **Welcome Back!**\nSelect an option from the menu below:",
        reply_markup=get_main_menu(message.from_user.id),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🚫 BAN CONFIG")
async def ban_config_menu_handler(message: types.Message):
    """Show Ban Configuration Menu"""
    if not await check_authorization(message, "Access Ban Config", "can_manage_admins"):
        return
    await message.answer("🚫 **BAN CONFIGURATION**\nSelect an option below:", reply_markup=get_ban_config_menu(), parse_mode="Markdown")

@dp.message(F.text == "⬅️ BACK TO ADMIN MENU")
async def back_to_admin_menu_handler(message: types.Message):
    """Return to Admin Menu"""
    if not await check_authorization(message, "Back to Admin Menu", "can_manage_admins"):
        return
    await message.answer("👥 **ADMIN CONFIGURATION**", reply_markup=get_admin_config_menu(), parse_mode="Markdown")

@dp.message(F.text == "🚫 BAN USER")
async def ban_user_start(message: types.Message, state: FSMContext):
    """Start ban user flow"""
    if not await check_authorization(message, "Ban User", "can_manage_admins"):
        return
    await state.set_state(AdminManagementStates.waiting_for_ban_user_id)
    await message.answer(
        "🚫 **BAN USER**\n\n"
        "Please enter the **Telegram User ID** of the user to ban.\n"
        "They will be blocked from accessing the bot.",
        reply_markup=get_cancel_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(AdminManagementStates.waiting_for_ban_user_id)
async def ban_user_process_id(message: types.Message, state: FSMContext):
    """Process ban user ID"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_ban_config_menu())
        return

    try:
        if not message.text.isdigit():
            await message.answer("⚠️ Invalid ID. Please enter a numeric User ID.", reply_markup=get_cancel_keyboard())
            return
            
        ban_id = int(message.text)
        
        # Prevent banning Admins
        if is_admin(ban_id):
            await message.answer(
                "⛔ **ERROR**\n\nYou cannot ban an Admin!\nRemove them from admins first.", 
                reply_markup=get_ban_config_menu(),
                parse_mode="Markdown"
            )
            await state.clear()
            return
            
        # Check if already banned
        if is_banned(ban_id):
            await message.answer(
                f"⚠️ **User {ban_id} is already banned!**", 
                reply_markup=get_ban_config_menu(),
                parse_mode="Markdown"
            )
            await state.clear()
            return
            
        # Ban User
        reason = f"Manual Ban by Admin {message.from_user.id}"
        ban_user(ban_id, "Unknown", "Unknown", reason) # Helper function handles logging
        
        await state.clear()
        await message.answer(
            f"✅ **SUCCESS!**\n\nUser `{ban_id}` has been BANNED.",
            reply_markup=get_ban_config_menu(),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await message.answer(f"❌ Error banning user: {e}", reply_markup=get_ban_config_menu())
        await state.clear()

@dp.message(F.text == "✅ UNBAN USER")
async def unban_user_start(message: types.Message, state: FSMContext):
    """Start unban user flow"""
    if not await check_authorization(message, "Unban User", "can_manage_admins"):
        return
    await state.set_state(AdminManagementStates.waiting_for_unban_user_id)
    await message.answer(
        "✅ **UNBAN USER**\n\n"
        "Please enter the **Telegram User ID** of the user to unban.",
        reply_markup=get_cancel_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(AdminManagementStates.waiting_for_unban_user_id)
async def unban_user_process_id(message: types.Message, state: FSMContext):
    """Process unban user ID"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_ban_config_menu())
        return

    try:
        if not message.text.isdigit():
            await message.answer("⚠️ Invalid ID. Please enter a numeric User ID.", reply_markup=get_cancel_keyboard())
            return
            
        unban_id = int(message.text)
        
        # Check if banned (Check DB directly to allow unbanning Exempt users too)
        banned_doc = col_banned_users.find_one({"user_id": unban_id})
        if not banned_doc:
            await message.answer(
                f"⚠️ **User {unban_id} is NOT found in ban list.**", 
                reply_markup=get_ban_config_menu(),
                parse_mode="Markdown"
            )
            await state.clear()
            return
            
        # Unban User
        col_banned_users.delete_one({"user_id": unban_id})
        logger.info(f"User {unban_id} unbanned by Admin {message.from_user.id}")
        
        await state.clear()
        await message.answer(
            f"✅ **SUCCESS!**\n\nUser `{unban_id}` has been UNBANNED.",
            reply_markup=get_ban_config_menu(),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await message.answer(f"❌ Error unbanning user: {e}", reply_markup=get_ban_config_menu())
        await state.clear()

@dp.message(F.text == "📋 LIST BANNED")
async def list_banned_handler(message: types.Message):
    """List all banned users with details"""
    if not await check_authorization(message, "List Banned", "can_manage_admins"):
        return
    
    banned_users = list(col_banned_users.find({}))
    
    if not banned_users:
        await message.answer("⚠️ **No banned users found.**", reply_markup=get_ban_config_menu(), parse_mode="Markdown")
        return
        
    msg = "🚫 **BANNED USERS LIST**\n━━━━━━━━━━━━━━━━━━━━\n"
    count = 0
    for user in banned_users:
        count += 1
        uid = user.get("user_id")
        reason = user.get("reason", "No reason provided")
        
        # Format Date
        date_val = user.get("banned_at", "Unknown")
        date_str = "Unknown"
        if isinstance(date_val, datetime):
            date_str = date_val.strftime("%Y-%m-%d %I:%M %p") # Date + Time (AM/PM)
        elif isinstance(date_val, str):
            date_str = date_val
            
        # Get Name (if saved)
        name = user.get("full_name", "Unknown")
        
        msg += (
            f"{count}. **{name}** (`{uid}`)\n"
            f"   📝 Reason: {reason}\n"
            f"   📅 Time: {date_str}\n\n"
        )
        
    if len(msg) > 4000:
        msg = msg[:4000] + "\n...(truncated)"
        
    await message.answer(msg, reply_markup=get_ban_config_menu(), parse_mode="Markdown")

# ==========================================
# ROLE MANAGEMENT HANDLERS
# ==========================================

@dp.message(F.text == "👔 ROLES")
@dp.message(F.text == "🔒 LOCK/UNLOCK")
async def roles_menu_handler(message: types.Message, state: FSMContext):
    """Show list of admins to select for Role Assignment or Lock/Unlock"""
    if not await check_authorization(message, "Manage Roles", "can_manage_admins"):
        return
        
    # Check if admins exist
    if col_admins.count_documents({}) == 0:
        await message.answer("⚠️ No admins found.", reply_markup=get_admin_config_menu())
        return

    # Determine Mode
    mode = "roles"
    if message.text == "🔒 LOCK/UNLOCK":
        mode = "lock"
    
    await state.update_data(role_menu_mode=mode)
        
    await state.set_state(AdminRoleStates.waiting_for_admin_selection)
    await state.update_data(role_admin_page=0)
    await send_role_admin_list(message, 0, mode)

async def send_role_admin_list(message: types.Message, page: int, mode: str = "roles"):
    """Helper to send paginated admin list for role selection"""
    ITEMS_PER_PAGE = 10
    admins = list(col_admins.find({}).sort("added_at", 1))
    total_admins = len(admins)
    total_pages = (total_admins + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    
    start = page * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE
    current_admins = admins[start:end]
    
    # Build Keyboard & Text List
    keyboard = []
    admin_list_text = ""
    
    # Admin Buttons (2 per row)
    row = []
    for i, admin in enumerate(current_admins):
        user_id = admin.get("user_id")
        name = admin.get("full_name", "Unknown")
        is_locked = admin.get("is_locked", False)
        
        status_str = "[🔒 LOCKED]" if is_locked else "[🔓 ACTIVE]"
        
        # Add to text list
        global_idx = start + i + 1
        admin_list_text += f"{global_idx}. **{name}** (`{user_id}`) {status_str}\n"
        
        # Button Format: "👤 Name (ID)"
        btn_text = f"👤 {name} ({user_id})"
        row.append(KeyboardButton(text=btn_text))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
        
    # Pagination Buttons
    nav_row = []
    if page > 0:
        nav_row.append(KeyboardButton(text="⬅️ PREV ROLES"))
    if page < total_pages - 1:
        nav_row.append(KeyboardButton(text="➡️ NEXT ROLES"))
    if nav_row:
        keyboard.append(nav_row)
        
    # Standard Controls
    keyboard.append([KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")])
    
    await message.answer(
        f"👔 **SELECT ADMIN TO {'MODIFY ROLE' if mode == 'roles' else 'LOCK/UNLOCK'}**\n\n"
        f"Select an admin from the list below:\n\n"
        f"{admin_list_text}\n"
        f"Page {page + 1}/{total_pages}",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )

@dp.message(AdminRoleStates.waiting_for_admin_selection, F.text == "➡️ NEXT ROLES")
async def next_role_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("role_admin_page", 0) + 1
    mode = data.get("role_menu_mode", "roles")
    await state.update_data(role_admin_page=page)
    await send_role_admin_list(message, page, mode)

@dp.message(AdminRoleStates.waiting_for_admin_selection, F.text == "⬅️ PREV ROLES")
async def prev_role_page(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(0, data.get("role_admin_page", 0) - 1)
    mode = data.get("role_menu_mode", "roles")
    await state.update_data(role_admin_page=page)
    await send_role_admin_list(message, page, mode)

@dp.message(AdminRoleStates.waiting_for_admin_selection)
async def role_admin_selected(message: types.Message, state: FSMContext):
    """Admin selected for role"""
    text = message.text
    
    if text == "❌ CANCEL" or text == "⬅️ RETURN BACK":
        await state.clear()
        await message.answer("↩️ Returned to Admin menu.", reply_markup=get_admin_config_menu())
        return

    # Try to extract ID from button text "Name (ID)"
    # Regex to find digits inside parentheses at the end of string
    import re
    match = re.search(r"\((\d+)\)$", text)
    
    target_admin_id = None
    if match:
        target_admin_id = int(match.group(1))
    elif text.isdigit():
        target_admin_id = int(text)
    else:
        await message.answer("⚠️ Invalid selection. Please click a user button.", reply_markup=get_cancel_keyboard())
        return
        
    # Verify admin exists (Direct DB check to allow managing locked admins)
    # is_admin() returns False for locked admins, so we can't use it here.
    admin_doc = col_admins.find_one({"user_id": target_admin_id})
    if not admin_doc and target_admin_id != MASTER_ADMIN_ID:
        await message.answer(f"⚠️ User {target_admin_id} is not an admin.", reply_markup=get_admin_config_menu())
        await state.clear()
        return
        
    # Prevent modifying Master Admin
    if target_admin_id == MASTER_ADMIN_ID:
        await message.answer("⛔ You cannot modify the Master Admin's role.", reply_markup=get_admin_config_menu())
        await state.clear()
        return
        
    # Store target admin ID
    await state.update_data(target_admin_id=target_admin_id)
    await state.set_state(AdminRoleStates.waiting_for_role_selection)
    
    admin_name = "Admin"
    admin_doc = col_admins.find_one({"user_id": target_admin_id})
    is_locked = False
    if admin_doc:
        admin_name = admin_doc.get("full_name", "Admin")
        is_locked = admin_doc.get("is_locked", False)

    # Determine Menu based on Mode
    data = await state.get_data()
    mode = data.get("role_menu_mode", "roles")
    
    target_menu = get_roles_menu()
    msg_text = f"👔 **SELECT ROLE FOR {admin_name}** (`{target_admin_id}`)\n\nChoose a role to apply permissions:"
    
    if mode == "lock":
        target_menu = get_lock_menu()
        status_text = "🔒 **LOCKED**" if is_locked else "🔓 **UNLOCKED**"
        msg_text = (
            f"🔒 **LOCK MANAGEMENT FOR {admin_name}** (`{target_admin_id}`)\n\n"
            f"Current Status: {status_text}\n\n"
            f"Select action:"
        )

    await message.answer(
        msg_text,
        reply_markup=target_menu,
        parse_mode="Markdown"
    )

@dp.message(AdminRoleStates.waiting_for_role_selection)
async def role_selected_process(message: types.Message, state: FSMContext):
    """Apply selected role"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_admin_config_menu())
        return

    selected_role = message.text
    
    data = await state.get_data()
    target_admin_id = data.get("target_admin_id")
    
    role_key = None
    if "OWNER" in selected_role: 
        # Trigger Ownership Transfer Flow
        await state.update_data(target_role="OWNER")
        await state.set_state(AdminRoleStates.waiting_for_owner_password)
        await message.answer(
            "🔐 **SECURITY CHECK**\n\n"
            "Resetting Ownership requires a password.\n"
            "Please enter the **Owner Password**:",
            reply_markup=get_cancel_keyboard(),
            parse_mode="Markdown"
        )
        return

    elif "MANAGER" in selected_role: role_key = "MANAGER"
    elif "ADMIN" in selected_role: role_key = "ADMIN"
    elif "MODERATOR" in selected_role: role_key = "MODERATOR"
    elif "SUPPORT" in selected_role: role_key = "SUPPORT"
    
    # Handle Lock/Unlock
    elif "🔒 LOCK" in selected_role:
        # Check if already locked
        admin_doc = col_admins.find_one({"user_id": target_admin_id})
        if admin_doc and admin_doc.get("is_locked", False):
            await message.answer(f"⚠️ **Admin {target_admin_id} is ALREADY LOCKED.**", reply_markup=get_admin_config_menu())
            await state.clear()
            return

        col_admins.update_one({"user_id": target_admin_id}, {"$set": {"is_locked": True}})
        log_user_action(message.from_user, "ADMIN LOCKED", f"Locked {target_admin_id}")
        await state.clear()
        await message.answer(
            f"🔒 **ADMIN LOCKED**\n\nUser `{target_admin_id}` has been locked.\nThey have NO access.",
            reply_markup=get_admin_config_menu(), parse_mode="Markdown"
        )
        return
        
    elif "🔓 UNLOCK" in selected_role:
        # Check if already unlocked
        admin_doc = col_admins.find_one({"user_id": target_admin_id})
        if admin_doc and not admin_doc.get("is_locked", False):
            await message.answer(f"⚠️ **Admin {target_admin_id} is ALREADY UNLOCKED.**", reply_markup=get_admin_config_menu())
            await state.clear()
            return

        col_admins.update_one({"user_id": target_admin_id}, {"$set": {"is_locked": False}})
        log_user_action(message.from_user, "ADMIN UNLOCKED", f"Unlocked {target_admin_id}")
        await state.clear()
        await message.answer(
            f"🔓 **ADMIN UNLOCKED**\n\nUser `{target_admin_id}` has been unlocked.\nPermissions restored.",
            reply_markup=get_admin_config_menu(), parse_mode="Markdown"
        )
        return
    
    if not role_key:
        await message.answer("⚠️ Invalid Role. Please select from keyboard.", reply_markup=get_roles_menu())
        return
        
    data = await state.get_data()
    target_admin_id = data.get("target_admin_id")
    
    # Get Permissions
    new_perms = ROLES.get(role_key)
    
    # Update DB
    col_admins.update_one(
        {"user_id": target_admin_id},
        {"$set": {"permissions": new_perms}}
    )
    
    # LOG
    log_user_action(message.from_user, "ROLE UPDATE", f"Set {target_admin_id} to {role_key}")
    
    # Notify Target Admin (Premium Message)
    try:
        caps_list = []
        # OWNER handled separately
        if role_key == "MANAGER": caps_list = ["• Manage Admins", "• Manage Content", "• View Analytics"]
        elif role_key == "ADMIN": caps_list = ["• Manage Content", "• Manage Links", "• View Analytics"]
        elif role_key == "MODERATOR": caps_list = ["• Add/Edit Content", "• Search Database"]
        elif role_key == "SUPPORT": caps_list = ["• View Content", "• Search Only"]
        
        caps_str = "\n".join(caps_list)
        
        await bot.send_message(
            target_admin_id,
            f"🌟 **PROMOTION GRANTED**\n\n"
            f"Dear Admin,\n\n"
            f"You have been promoted to **{role_key}**.\n"
            f"Your new capabilities include:\n"
            f"{caps_str}\n\n"
            f"*Access granted by {message.from_user.full_name}*.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Failed to notify admin {target_admin_id} of role change: {e}")
        
    await state.clear()
    await message.answer(
        f"✅ **SUCCESS!**\n\n"
        f"User `{target_admin_id}` is now **{role_key}**.\n"
        f"Permissions updated.",
        reply_markup=get_admin_config_menu(),
        parse_mode="Markdown"
    )

@dp.message(AdminRoleStates.waiting_for_owner_password)
async def process_owner_password(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        # Return to Role Selection state instead of clearing
        await state.set_state(AdminRoleStates.waiting_for_role_selection)
        await message.answer("❌ Cancelled.", reply_markup=get_roles_menu())
        return

    password = message.text.strip()
    if password == "99insanebeing45":
        # Check permissions - Only Current Owner can do this?
        # Actually any admin with 'can_manage_admins' can access Roles menu, 
        # BUT only valid password holders (Owner) should know this.
        
        await state.set_state(AdminRoleStates.waiting_for_owner_confirm)
        
        data = await state.get_data()
        target_admin_id = data.get("target_admin_id")
        
        await message.answer(
            f"⚠️ **CRITICAL WARNING** ⚠️\n\n"
            f"You are about to transfer **OWNERSHIP** to `{target_admin_id}`.\n"
            f"This action is **IRREVERSIBLE** via the bot.\n"
            f"You will lose your Owner privileges.\n\n"
            f"Are you sure?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ YES, TRANSFER OWNERSHIP"), KeyboardButton(text="❌ CANCEL")]],
                resize_keyboard=True
            ),
            parse_mode="Markdown"
        )
    else:
        await message.answer("⛔ **Incorrect Password.** Access Denied.", reply_markup=get_roles_menu())
        await state.clear()

@dp.message(AdminRoleStates.waiting_for_owner_confirm)
async def process_owner_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ YES, TRANSFER OWNERSHIP":
        data = await state.get_data()
        target_admin_id = data.get("target_admin_id")
        current_owner_id = message.from_user.id
        
        global MASTER_ADMIN_ID
        
        # 1. Promote New Owner
        col_admins.update_one(
            {"user_id": target_admin_id},
            {"$set": {
                "permissions": list(PERMISSIONS.keys()),
                "is_owner": True # Persistent Flag
            }}
        )
        
        # 2. Demote Old Owner (to Manager)
        # Note: If current_owner_id is the Environ Var ID, we can't 'remove' env var.
        # But we can update DB to say 'role: manager'.
        if current_owner_id != target_admin_id: # Self-promotion check
            col_admins.update_one(
                {"user_id": current_owner_id},
                {"$set": {
                    "permissions": ROLES["MANAGER"],
                    "is_owner": False
                }}
            )
        
        # 3. Update Global Cache
        MASTER_ADMIN_ID = target_admin_id
        
        # Log
        log_user_action(message.from_user, "OWNERSHIP TRANSFER", f"New Owner: {target_admin_id}")
        
        # Notify Steps
        try:
             await bot.send_message(
                target_admin_id,
                f"👑 **ALL HAIL THE NEW OWNER!**\n\n"
                f"You have been granted **OWNERSHIP** of this bot.\n"
                f"You now have absolute power.\n\n"
                f"*Transfer authorized by previous owner.*",
                parse_mode="Markdown"
            )
        except: pass
        
        await state.clear()
        await message.answer(
            f"✅ **OWNERSHIP TRANSFERRED!**\n\n"
            f"New Owner: `{target_admin_id}`\n"
            f"You are now a **MANAGER**.\n"
            f"Please restart the bot for full effect.",
            reply_markup=get_main_menu(current_owner_id),
            parse_mode="Markdown"
        )
        
    else:
        # Return to Role Selection state
        await state.set_state(AdminRoleStates.waiting_for_role_selection)
        await message.answer("❌ Transfer Cancelled.", reply_markup=get_roles_menu())

# ==========================================
# ADMIN PERMISSION HANDLERS
# ==========================================

@dp.message(F.text == "🔐 PERMISSIONS")
async def permissions_menu_handler(message: types.Message, state: FSMContext):
    """Show Permission Management - Select Admin"""
    if not await check_authorization(message, "Access Permissions"):
        return
    
    # Check if Master Admin (Only Master can manage permissions)
    if message.from_user.id != MASTER_ADMIN_ID:
        await message.answer("⛔ **ACCESS DENIED**\n\nOnly the Master Admin can manage permissions.")
        return

    # List admins to select
    admins = list(col_admins.find().sort("added_at", 1))
    
    if not admins:
        await message.answer("⚠️ **No additional admins found.**\nAdd admins first to configure permissions.", reply_markup=get_admin_config_menu())
        return
        
    await state.set_state(AdminPermissionStates.waiting_for_admin_selection)
    
    msg = "🔐 **MANAGE PERMISSIONS**\n\nSelect an admin to configure:\n"
    keyboard = []
    
    for admin in admins:
        user_id = admin['user_id']
        name = admin.get('full_name', 'Unknown')
        username = f"(@{admin.get('username')})" if admin.get('username') else ""
        keyboard.append([KeyboardButton(text=str(user_id))]) # Send ID as text
        msg += f"• `{user_id}`: {name} {username}\n"
        
    keyboard.append([KeyboardButton(text="❌ CANCEL")])
    
    await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")

@dp.message(AdminPermissionStates.waiting_for_admin_selection)
async def permission_admin_selected(message: types.Message, state: FSMContext):
    """Admin Selected - Show Permission Toggles"""
    if message.text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_admin_config_menu())
        return
        
    if not message.text.isdigit():
        await message.answer("⚠️ Invalid ID. Please select a valid User ID.", reply_markup=get_cancel_keyboard())
        return
        
    target_id = int(message.text)
    admin = col_admins.find_one({"user_id": target_id})
    
    if not admin:
        await message.answer("⚠️ Admin not found.", reply_markup=get_admin_config_menu())
        await state.clear()
        return
        
    # Get current permissions (Default to ALL if not set)
    current_perms = admin.get("permissions")
    if current_perms is None:
        current_perms = [p for p in DEFAULT_SAFE_PERMISSIONS] # Clone safe defaults
        
    # Save partial state
    await state.update_data(target_admin_id=target_id, current_perms=current_perms)
    await state.set_state(AdminPermissionStates.configuring_permissions)
    
    # Show toggles via Inline Keyboard (better for toggling)
    await send_permission_toggles(message, target_id, current_perms, admin.get("full_name", "Admin"))

async def send_permission_toggles(message: types.Message, target_id: int, current_perms: list, admin_name: str):
    """Helper to send/update permission toggle UI (Reply Keyboard)"""
    
    text = f"🔐 **CONFIGURING: {admin_name}** (`{target_id}`)\n\n"
    text += "Use the buttons below to toggle permissions.\n"
    text += "✅ = Allowed | ❌ = Denied\n\n"
    text += "Click **💾 SAVE CHANGES** to save and exit."
    
    # Build Reply Keyboard
    keyboard = []
    
    # Permission Buttons (2 per row)
    row = []
    for perm_key, btn_text in PERMISSIONS.items():
        is_allowed = perm_key in current_perms
        status_icon = "✅" if is_allowed else "❌"
        # Button Text: "✅ 👥 ADMINS"
        row.append(KeyboardButton(text=f"{status_icon} {btn_text}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
        
    # Actions
    keyboard.append([KeyboardButton(text="✅ SELECT ALL"), KeyboardButton(text="❌ REVOKE ALL")])
    keyboard.append([KeyboardButton(text="💾 SAVE CHANGES"), KeyboardButton(text="❌ CANCEL")])
    
    # Send message with ReplyKeyboard
    # Note: We rely on ReplyKeyboardMarkup to persistent the buttons until state is cleared
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="Markdown")

@dp.message(AdminPermissionStates.configuring_permissions)
async def permission_message_handler(message: types.Message, state: FSMContext):
    """Handle permission toggles via Text Messages"""
    data = await state.get_data()
    current_perms = data.get("current_perms", [])
    target_id = data.get("target_admin_id")
    
    text = message.text
    
    if text == "❌ CANCEL":
        await state.clear()
        await message.answer("❌ Operation cancelled.", reply_markup=get_admin_config_menu())
        return

    elif text == "💾 SAVE CHANGES":
        col_admins.update_one(
            {"user_id": target_id},
            {"$set": {"permissions": current_perms}}
        )
        await state.clear()
        await message.answer(f"✅ **PERMISSIONS SAVED** for Admin `{target_id}`", reply_markup=get_admin_config_menu(), parse_mode="Markdown")
        return

    elif text == "✅ SELECT ALL":
        current_perms = [p for p in DEFAULT_SAFE_PERMISSIONS] # Only Select Safe ones
        # Feedback message
        await message.answer("✅ **Safe permissions selected.**\n(Dangerous features must be toggled manually)")

    elif text == "❌ REVOKE ALL":
        current_perms = []
        # Feedback message
        await message.answer("❌ **All permissions revoked.**")

    else:
        # Check if it's a toggle button
        # Format: "✅ [NAME]" or "❌ [NAME]"
        # We need to find which permission key matches
        found_key = None
        for key, name in PERMISSIONS.items():
            if name in text: # "👥 ADMINS" in "✅ 👥 ADMINS"
                found_key = key
                break
        
        if found_key:
            if found_key in current_perms:
                current_perms.remove(found_key)
            else:
                current_perms.append(found_key)
        else:
            # Unknown input - ignore or show error
            await message.answer("⚠️ Invalid option. Please use the buttons.")
            return 

    # Update state
    await state.update_data(current_perms=current_perms)
    
    # Re-send menu to update button states
    admin = col_admins.find_one({"user_id": target_id})
    admin_name = admin.get("full_name", "Admin") if admin else "Admin"
    await send_permission_toggles(message, target_id, current_perms, admin_name)

# ==========================================
# GENERAL HANDLERS (No Admin Features)
# ==========================================





# --- Smart PDF Selection ---

@dp.message(lambda m: m.text and (m.text.isdigit() or len(m.text) > 2)) 
async def smart_pdf_selection_handler(message: types.Message, state: FSMContext):
    """Catches text that might be a PDF Index or Name"""
    
    # Ignore if in a specific state already (handled by FSM)
    current_state = await state.get_state()
    if current_state is not None:
        return

    query = message.text
    pdf = None
    
    if query.isdigit():
        pdf = col_pdfs.find_one({"index": int(query)})
    else:
        pdf = col_pdfs.find_one({"name": {"$regex": query, "$options": "i"}})
    
    if not pdf:
        # Pass through to debug logger if not found
        # We invoke the debug handler explicitly if we want, or just let it fall through
        # But since we are catching it here, we must decide.
        # If it looks like a command (starts with /), let it pass.
        if message.text.startswith("/"): return 
        # Otherwise, treat as "Unknown Command"
        print(f"⚠️ UNHANDLED MESSAGE: '{message.text}'")
        await message.answer(f"⚠️ Unhandled command: {message.text}\nPlease run /start to update your menu.")
        return

    # PDF Found - Show Actions
    await state.update_data(edit_id=str(pdf["_id"]), current_name=pdf["name"], current_link=pdf["link"])
    await state.set_state(PDFActionStates.waiting_for_action)
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📝 EDIT NAME"), KeyboardButton(text="🔗 EDIT LINK")],
        [KeyboardButton(text="🗑️ DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await message.answer(
        f"📄 **SELECTED PDF**\n"
        f"🆔 Index: `{pdf['index']}`\n"
        f"📛 Name: {pdf['name']}\n"
        f"🔗 Link: {pdf['link']}\n\n"
        "⬇️ **Select Action:**",
        reply_markup=kb,
        parse_mode="Markdown"
    )

@dp.message(PDFActionStates.waiting_for_action)
async def process_pdf_action(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Selection Cancelled.", reply_markup=get_pdf_menu())
    
    if message.text == "📝 EDIT NAME":
        await state.update_data(field="name")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ **Enter New Name:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        
    elif message.text == "🔗 EDIT LINK":
        await state.update_data(field="link")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ **Enter New Link:**", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        
    elif message.text == "🗑️ DELETE":
        # Transition to delete confirm
        data = await state.get_data()
        await state.update_data(delete_id=data['edit_id']) # Reuse ID
        
        kb = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
        ], resize_keyboard=True)
        
        await state.set_state(PDFStates.waiting_for_delete_confirm)
        await message.answer(
            f"⚠️ **CONFIRM DELETION**\n\nAre you sure you want to delete this PDF?",
            reply_markup=kb,
            parse_mode="Markdown"
        )
    else:
         await message.answer("⚠️ Invalid Option. Choose from the buttons.", reply_markup=get_cancel_keyboard())

# ==========================================
# 📊 ANALYTICS HANDLER
# ==========================================

@dp.message(F.text.in_(["📊 ANALYTICS", "📊 Analytics"]))
async def analytics_handler(message: types.Message, state: FSMContext):
    """Display analytics dashboard with PDF click stats and user metrics"""
    if not await check_authorization(message, "View Analytics", "can_analytics"):
        return
    
    await state.clear()
    
    # Get PDF analytics
    total_pdfs = col_pdfs.count_documents({})
    total_ig_content = col_ig_content.count_documents({})
    
    # Get user tracking stats
    user_stats_pipeline = [
        {"$group": {"_id": "$source", "count": {"$sum": 1}}}
    ]
    user_stats_raw = list(db["bot10_user_tracking"].aggregate(user_stats_pipeline))
    user_stats = {stat["_id"]: stat["count"] for stat in user_stats_raw if stat["_id"]}
    total_users = sum(user_stats.values())
    
    # Get top performing PDFs (sort by total clicks)
    top_pdfs = list(col_pdfs.find().sort("clicks", -1).limit(5))
    
    # Get recent activity
    last_ig_pdf = col_pdfs.find_one({"last_ig_click": {"$exists": True}}, sort=[("last_ig_click", -1)])
    last_yt_pdf = col_pdfs.find_one({"last_yt_click": {"$exists": True}}, sort=[("last_yt_click", -1)])
    last_igcc = col_ig_content.find_one({"last_ig_cc_click": {"$exists": True}}, sort=[("last_ig_cc_click", -1)])
    
    # Build analytics message
    msg = "📊 **ANALYTICS DASHBOARD**\\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\\n\\n"
    
    # Overview
    msg += f"📈 **OVERVIEW**\\n"
    msg += f"• Total PDFs: {total_pdfs}\\n"
    msg += f"• Total IG Content: {total_ig_content}\\n"
    msg += f"• Total Users: {total_users}\\n\\n"
    
    # Top PDFs
    if top_pdfs:
        msg += "🔥 **TOP PERFORMING PDFs** (by clicks)\\n"
        for i, pdf in enumerate(top_pdfs, 1):
            clicks = pdf.get('clicks', 0)
            ig_clicks = pdf.get('ig_start_clicks', 0)
            yt_clicks = pdf.get('yt_start_clicks', 0)
            name = pdf.get('name', 'Unnamed')
            if len(name) > 25:
                name = name[:25] + "..."
            msg += f"{i}. {name} - {clicks} clicks\\n"
            msg += f"   └ IG: {ig_clicks} | YT: {yt_clicks}\\n"
        msg += "\\n"
    
    # User sources
    if user_stats:
        msg += "🎯 **USER SOURCES**\\n"
        for source in ["IG", "YT", "IGCC"]:
            count = user_stats.get(source, 0)
            pct = (count / total_users * 100) if total_users > 0 else 0
            msg += f"• {source}: {count} users ({pct:.0f}%)\\n"
        msg += "\\n"
    
    # Recent activity
    msg += "⏰ **RECENT ACTIVITY**\\n"
    from datetime import datetime, timedelta
    now = datetime.now()
    
    if last_ig_pdf and last_ig_pdf.get('last_ig_click'):
        time_diff = now - last_ig_pdf['last_ig_click']
        if time_diff < timedelta(hours=1):
            time_str = f"{int(time_diff.total_seconds() / 60)} mins ago"
        elif time_diff < timedelta(days=1):
            time_str = f"{int(time_diff.total_seconds() / 3600)} hours ago"
        else:
            time_str = last_ig_pdf['last_ig_click'].strftime("%b %d, %I:%M %p")
        msg += f"• Last IG click: {time_str} (PDF #{last_ig_pdf.get('index', '?')})\\n"
    
    if last_yt_pdf and last_yt_pdf.get('last_yt_click'):
        time_diff = now - last_yt_pdf['last_yt_click']
        if time_diff < timedelta(hours=1):
            time_str = f"{int(time_diff.total_seconds() / 60)} mins ago"
        elif time_diff < timedelta(days=1):
            time_str = f"{int(time_diff.total_seconds() / 3600)} hours ago"
        else:
            time_str = last_yt_pdf['last_yt_click'].strftime("%b %d, %I:%M %p")
        msg += f"• Last YT click: {time_str} (PDF #{last_yt_pdf.get('index', '?')})\\n"
    
    if last_igcc and last_igcc.get('last_ig_cc_click'):
        time_diff = now - last_igcc['last_ig_cc_click']
        if time_diff < timedelta(hours=1):
            time_str = f"{int(time_diff.total_seconds() / 60)} mins ago"
        elif time_diff < timedelta(days=1):
            time_str = f"{int(time_diff.total_seconds() / 3600)} hours ago"
        else:
            time_str = last_igcc['last_ig_cc_click'].strftime("%b %d, %I:%M %p")
        msg += f"• Last IGCC click: {time_str} ({last_igcc.get('cc_code', '?')})\\n"
    
    await message.answer(msg, reply_markup=get_main_menu(), parse_mode="Markdown")


# --- General Handlers (Catch-all for buttons outside FSM states) ---
@dp.message(F.text == "❌ CANCEL")
async def general_cancel_handler(message: types.Message, state: FSMContext):
    """Handles cancel button clicks when not in a specific state"""
    if not await check_authorization(message, "Cancel button"):
        return
    await state.clear()
    await message.answer("❌ Operation cancelled.", reply_markup=get_main_menu())


# --- Debug Handler - Catch All with Authorization ---
@dp.message()
async def debug_catch_all(message: types.Message):
    # Apply authorization check
    if not await check_authorization(message, f"message: {message.text or 'media'}"):
        return
    
    print(f"⚠️ UNHANDLED MESSAGE: '{message.text}'")
    await message.answer(f"⚠️ Unhandled command: {message.text}\nPlease run /start to update your menu.")

# --- Main Execution ---

async def check_and_create_missed_backup():
    """Check if current month's backup exists, create it if missing"""
    try:
        month_year = get_month_year_name()
        filename = f"Backup_{month_year}.zip"
        
        # Check if backup exists in database
        existing_backup = col_backups.find_one({"filename": filename})
        
        if not existing_backup:
            logger.info(f"⚠️ No backup found for {month_year}, creating now...")
            success, filepath, metadata = await create_backup_file(auto=True)
            
            if success and metadata:
                logger.info(f"✅ Startup backup created: {metadata['filename']}")
                
                # Notify master admin
                try:
                    await bot.send_message(
                        MASTER_ADMIN_ID,
                        f"📦 **STARTUP BACKUP CREATED**\n\n"
                        f"The bot detected no backup for {month_year}.\n"
                        f"✅ Created: `{metadata['filename']}`\n"
                        f"💾 Size: {metadata['file_size_mb']:.2f} MB\n\n"
                        f"This ensures no monthly backup is missed!",
                        parse_mode="Markdown"
                    )
                except:
                    pass
            else:
                logger.error(f"❌ Failed to create startup backup for {month_year}")
        else:
            logger.info(f"✅ Backup for {month_year} already exists")
            
    except Exception as e:
        logger.error(f"❌ Startup backup check failed: {e}")

# ==========================================
# DAILY REPORT SYSTEM
# ==========================================

async def generate_daily_report():
    """Generate comprehensive daily report"""
    try:
        logger.info("📊 Generating daily report...")
        
        # Get timezone
        tz = pytz.timezone(DAILY_REPORT_TIMEZONE)
        now = datetime.now(tz)
        timestamp = now.strftime("%B %d, %Y %I:%M %p")
        
        # Get statistics
        total_pdfs = col_pdfs.count_documents({})
        total_ig_content = col_ig_content.count_documents({})
        total_admins = col_admins.count_documents({})
        total_banned = col_banned_users.count_documents({})
        
        # Get today's activity
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        pdfs_added_today = col_pdfs.count_documents({"created_at": {"$gte": today_start.replace(tzinfo=None)}})
        ig_added_today = col_ig_content.count_documents({"created_at": {"$gte": today_start.replace(tzinfo=None)}})
        
        # Get click statistics (last 24 hours)
        yesterday = now - timedelta(days=1)
        total_clicks_24h = 0
        pdf_clicks_24h = 0
        ig_cc_clicks_24h = 0
        yt_clicks_24h = 0
        
        # Count clicks from PDFs in last 24 hours
        for pdf in col_pdfs.find({}):
            if pdf.get('last_clicked_at') and pdf['last_clicked_at'] >= yesterday.replace(tzinfo=None):
                total_clicks_24h += pdf.get('clicks', 0)
            if pdf.get('last_affiliate_click') and pdf['last_affiliate_click'] >= yesterday.replace(tzinfo=None):
                pdf_clicks_24h += pdf.get('affiliate_clicks', 0)
            if pdf.get('last_yt_click') and pdf['last_yt_click'] >= yesterday.replace(tzinfo=None):
                yt_clicks_24h += pdf.get('yt_start_clicks', 0)
        
        # Count IG CC clicks in last 24 hours
        for ig in col_ig_content.find({}):
            if ig.get('last_ig_cc_click') and ig['last_ig_cc_click'] >= yesterday.replace(tzinfo=None):
                ig_cc_clicks_24h += ig.get('ig_cc_clicks', 0)
        
        # Get system metrics
        uptime = now - health_monitor.system_metrics["uptime_start"]
        uptime_str = f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds // 60) % 60}m"
        memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Get top performing content
        top_pdfs = list(col_pdfs.find({}).sort("clicks", -1).limit(5))
        top_ig = list(col_ig_content.find({}).sort("ig_cc_clicks", -1).limit(5))
        
        # Build report
        report = f"📊 **BOT 9 DAILY REPORT**\n"
        report += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        report += f"📅 **Date:** {timestamp}\n"
        report += f"⏰ **Report Type:** {'Morning' if now.hour < 12 else 'Evening'} Report\n\n"
        
        report += f"📈 **DATABASE OVERVIEW**\n"
        report += f"├ Total PDFs: {total_pdfs}\n"
        report += f"├ Total IG Content: {total_ig_content}\n"
        report += f"├ Total Admins: {total_admins}\n"
        report += f"└ Banned Users: {total_banned}\n\n"
        
        report += f"🆕 **TODAY'S ADDITIONS**\n"
        report += f"├ New PDFs: {pdfs_added_today}\n"
        report += f"└ New IG Content: {ig_added_today}\n\n"
        
        report += f"📊 **LAST 24 HOURS ACTIVITY**\n"
        report += f"├ Total Interactions: {total_clicks_24h}\n"
        report += f"├ PDF Affiliate Clicks: {pdf_clicks_24h}\n"
        report += f"├ YT Link Clicks: {yt_clicks_24h}\n"
        report += f"└ IG CC Clicks: {ig_cc_clicks_24h}\n\n"
        
        if top_pdfs:
            report += f"🔥 **TOP 5 PERFORMING PDFs**\n"
            for i, pdf in enumerate(top_pdfs, 1):
                name = pdf.get('name', 'Unnamed')
                if len(name) > 30:
                    name = name[:30] + "..."
                clicks = pdf.get('clicks', 0)
                report += f"{i}. {name} - {clicks} clicks\n"
            report += "\n"
        
        if top_ig:
            report += f"📸 **TOP 5 PERFORMING IG CONTENT**\n"
            for i, ig in enumerate(top_ig, 1):
                name = ig.get('name', 'Unnamed')
                if len(name) > 30:
                    name = name[:30] + "..."
                clicks = ig.get('ig_cc_clicks', 0)
                report += f"{i}. {name} - {clicks} clicks\n"
            report += "\n"
        
        report += f"🖥️ **SYSTEM HEALTH**\n"
        report += f"├ Uptime: {uptime_str}\n"
        report += f"├ Memory Usage: {memory_mb:.2f} MB\n"
        report += f"├ CPU Usage: {cpu_percent}%\n"
        report += f"├ Total Errors (Since Start): {health_monitor.error_count}\n"
        report += f"├ Health Checks Failed: {health_monitor.health_checks_failed}\n"
        report += f"└ Status: {'✅ Healthy' if health_monitor.is_healthy else '⚠️ Degraded'}\n\n"
        
        report += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        report += f"🤖 **Bot 9 Enterprise Monitoring System**\n"
        report += f"📌 Next report at {DAILY_REPORT_TIME_2 if now.hour < 12 else '08:40 AM tomorrow'}"
        
        # Send report
        await bot.send_message(MASTER_ADMIN_ID, report, parse_mode="Markdown")
        logger.info("✅ Daily report sent successfully")
        
    except Exception as e:
        logger.error(f"Failed to generate daily report: {e}")
        await health_monitor.send_error_notification(
            "Daily Report Generation Failed",
            str(e),
            traceback.format_exc()
        )

async def daily_report_task():
    """Background task for scheduled daily reports"""
    if not DAILY_REPORT_ENABLED:
        logger.info("Daily reports disabled")
        return
    
    logger.info(f"✅ Daily report task started (Times: {DAILY_REPORT_TIME_1}, {DAILY_REPORT_TIME_2})")
    
    while True:
        try:
            # Get current time in configured timezone
            tz = pytz.timezone(DAILY_REPORT_TIMEZONE)
            now = datetime.now(tz)
            current_time = now.strftime("%H:%M")
            
            # Check if it's time for report
            if current_time == DAILY_REPORT_TIME_1 or current_time == DAILY_REPORT_TIME_2:
                await generate_daily_report()
                # Sleep for 61 seconds to avoid sending multiple times in the same minute
                await asyncio.sleep(61)
            else:
                # Check every 30 seconds
                await asyncio.sleep(30)
                
        except Exception as e:
            logger.error(f"Daily report task error: {e}")
            await asyncio.sleep(60)

# ==========================================
# HEALTH MONITORING BACKGROUND TASK
# ==========================================

async def health_monitoring_task():
    """Background task for continuous health monitoring"""
    logger.info("✅ Health monitoring task started")
    
    while True:
        try:
            await health_monitor.check_system_health()
            await health_monitor.log_system_metrics()
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"Health monitoring task error: {e}")
            await asyncio.sleep(60)

# ==========================================
# STATE PERSISTENCE BACKGROUND TASK
# ==========================================

async def state_persistence_task():
    """Background task for periodic state backup"""
    if not STATE_BACKUP_ENABLED:
        logger.info("State persistence disabled")
        return
    
    logger.info(f"✅ State persistence task started (Interval: {STATE_BACKUP_INTERVAL_MINUTES} min)")
    
    while True:
        try:
            await state_persistence.save_state()
            await asyncio.sleep(STATE_BACKUP_INTERVAL_MINUTES * 60)
        except Exception as e:
            logger.error(f"State persistence task error: {e}")
            await asyncio.sleep(60)

async def health_check_endpoint(request):
    """Health check endpoint for uptime monitoring and hosting platforms"""
    try:
        uptime = datetime.now() - health_monitor.system_metrics["uptime_start"]
        memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        
        return web.json_response({
            "status": "healthy",
            "bot": "Bot 9 Enterprise",
            "timestamp": datetime.now().isoformat(),
            "uptime_seconds": int(uptime.total_seconds()),
            "uptime_formatted": str(uptime),
            "memory_mb": round(memory_mb, 2),
            "total_requests": health_monitor.system_metrics["total_requests"],
            "total_errors": health_monitor.system_metrics["total_errors"],
            "is_healthy": health_monitor.is_healthy
        })
    except Exception as e:
        logger.error(f"Health check endpoint error: {e}")
        return web.json_response({
            "status": "error",
            "error": str(e)
        }, status=500)

async def start_health_server():
    """Start health check web server for Render/Railway/Fly.io"""
    global health_server_runner
    try:
        app = web.Application()
        app.router.add_get('/health', health_check_endpoint)
        app.router.add_get('/', health_check_endpoint)  # Root also works
        
        runner = web.AppRunner(app)
        await runner.setup()
        health_server_runner = runner  # Store for cleanup
        
        # Use PORT from environment (Render/Railway provide this)
        port = int(os.environ.get('PORT', 8080))
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        
        logger.info(f"✅ Health check server started on port {port}")
        print(f"  ✅ Health endpoint: http://0.0.0.0:{port}/health")
        
    except Exception as e:
        logger.error(f"Failed to start health server: {e}")
        print(f"  ⚠️ Health server failed: {e}")

async def cleanup_on_shutdown():
    """Cleanup resources on bot shutdown to prevent aiohttp warnings"""
    print("\n🔄 Shutting down gracefully...")
    
    try:
        # Close bot session (prevents aiohttp unclosed session warnings)
        await bot.session.close()
        print("✅ Bot session closed")
    except Exception as e:
        logger.error(f"Error closing bot session: {e}")
    
    try:
        # Close health server
        global health_server_runner
        if health_server_runner:
            await health_server_runner.cleanup()
            print("✅ Health server closed")
    except Exception as e:
        logger.error(f"Error closing health server: {e}")
    
    try:
        # Save final state
        if STATE_BACKUP_ENABLED:
            await state_persistence.save_state({})
            print("✅ Final state saved")
    except Exception as e:
        logger.error(f"Error saving final state: {e}")
    
    print("👋 Shutdown complete!\n")

async def main():
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("🚀 BOT 9 ENTERPRISE EDITION STARTING...")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    # Start health check server (for hosting platforms and UptimeRobot)
    asyncio.create_task(start_health_server())
    
    # Load previous state if enabled
    if AUTO_RESUME_ON_STARTUP:
        print("📂 Loading previous state...")
        state_data = await state_persistence.load_state()
        if state_data:
            print(f"✅ State restored from {state_data['timestamp']}")
        else:
            print("ℹ️ No previous state found (fresh start)")
    
    # Start background tasks
    print("\n🔧 Starting background services...")
    
    asyncio.create_task(auto_backup_task())
    print("  ✅ Auto-backup task (Monthly at 2 AM)")
    
    asyncio.create_task(health_monitoring_task())
    print(f"  ✅ Health monitoring ({HEALTH_CHECK_INTERVAL}s interval)")
    
    asyncio.create_task(daily_report_task())
    print(f"  ✅ Daily reports ({DAILY_REPORT_TIME_1} & {DAILY_REPORT_TIME_2})")
    
    asyncio.create_task(state_persistence_task())
    print(f"  ✅ State persistence ({STATE_BACKUP_INTERVAL_MINUTES} min interval)")
    
    # Check if current month's backup exists (persistence check)
    print("\n💾 Checking backup status...")
    await check_and_create_missed_backup()
    
    # Send startup notification
    if MASTER_ADMIN_ID and MASTER_ADMIN_ID != 0:
        try:
            startup_msg = (
                "🚀 **BOT 9 ENTERPRISE EDITION**\n\n"
                "✅ **Status:** ONLINE\n"
                f"📅 **Started:** {datetime.now().strftime('%B %d, %Y %I:%M %p')}\n\n"
                "🔧 **Active Systems:**\n"
                "├ Auto-Healer: ✅ Active\n"
                "├ Health Monitor: ✅ Active\n"
                "├ Daily Reports: ✅ Active\n"
                "├ State Persistence: ✅ Active\n"
                "└ Auto Backup: ✅ Active\n\n"
                "🛡️ **Security:** Enterprise Level\n"
                "⚡ **Ready to Handle:** Millions of Requests\n\n"
                "All systems operational! 🎯"
            )
            await bot.send_message(MASTER_ADMIN_ID, startup_msg, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")
    else:
        print("⚠️  WARNING: MASTER_ADMIN_ID is 0 - update BOT9.env with your Telegram user ID")
        print("   Get your ID from: @userinfobot on Telegram")
    
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("✅ BOT 9 IS NOW ONLINE AND READY!")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
    
    # Start bot polling (THIS IS CRITICAL - without this, bot won't respond to messages)
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        # Cleanup on shutdown
        await cleanup_on_shutdown()


# ==========================================
# 🚀 APPLICATION ENTRY POINT
# ==========================================
if __name__ == "__main__":
    try:
        # Validate required environment variables before starting
        required_vars = ["BOT_9_TOKEN", "MONGO_URI", "MASTER_ADMIN_ID"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            print("❌ ERROR: Missing required environment variables:")
            for var in missing_vars:
                print(f"   - {var}")
            print("\n📝 Please set these variables in:")
            print("   - Local: Create .env file (copy from BOT9.env)")
            print("   - Render: Add in Environment section")
            print("   - See RENDER_ENV_VARIABLES.txt for details")
            sys.exit(1)
        
        # Run the bot
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\n⚠️  Bot stopped by user (Ctrl+C)")
        print("👋 Goodbye!")
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: Bot crashed!")
        print(f"Error: {e}")
        print(f"\nTraceback:")
        traceback.print_exc()
        print("\n📝 Check bot9_errors.log for details")
        sys.exit(1)

