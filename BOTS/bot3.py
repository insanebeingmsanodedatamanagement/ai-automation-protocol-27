import logging
import asyncio
import os
import sys

# Force UTF-8 output — prevents UnicodeEncodeError on Windows cp1252 console
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psutil
import json
import traceback
import pickle
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.types import ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import pymongo
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
import re
import string
import random
from bson.objectid import ObjectId
import pytz
from zoneinfo import ZoneInfo
from logging.handlers import RotatingFileHandler
from aiohttp import web
import html as _html

# Load environment variables from BOT9.env

# ==========================================
# ENTERPRISE CONFIGURATION
# ==========================================

# Bot Configuration
BOT_TOKEN = os.environ.get("BOT_9_TOKEN", os.environ.get("BOT_TOKEN"))
BOT_USERNAME = os.environ.get("BOT_USERNAME", "msanodebot")  # Bot's @username for generating t.me links
MONGO_URI = os.environ.get("MONGO_URI")
MASTER_ADMIN_ID = int(os.environ.get("MASTER_ADMIN_ID", 0))
OWNER_ID = int(os.iron.get("OWNER_ID", 0))

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
OWNER_PASSWORD = os.environ.get("OWNER_PASSWORD", "change_this_password_immediately")  # Password for ownership transfer
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")   # Set on Render; never hardcode here

# In-memory set of owner IDs that have completed password auth this session
_admin_authenticated: set = set()

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

# ---- Local timezone helper ----
try:
    _BOT9_TZ = ZoneInfo(DAILY_REPORT_TIMEZONE)
except Exception:
    _BOT9_TZ = ZoneInfo("Asia/Kolkata")

def now_local() -> datetime:
    """Return current time as a naive datetime in the configured local timezone."""
    return datetime.now(_BOT9_TZ).replace(tzinfo=None)
# --------------------------------

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
    
    # Per-level alert cooldown (seconds): how long to suppress duplicate alerts of the same type
    ALERT_COOLDOWNS = {
        "SUCCESS":  0,      # Never suppressed
        "INFO":     300,    # 5 min between same INFO alert
        "WARNING":  1800,   # 30 min between same WARNING alert
        "ERROR":    600,    # 10 min between same ERROR alert
        "CRITICAL": 120,    # 2 min between same CRITICAL alert
    }
    # Consecutive high readings required before firing a WARNING (avoids transient spikes)
    CPU_SUSTAINED_THRESHOLD = 3   # 3 × 60 s = 3 minutes sustained
    MEM_SUSTAINED_THRESHOLD = 2   # 2 × 60 s = 2 minutes sustained

    def __init__(self):
        self.health_checks_failed = 0
        self.last_health_check = now_local()
        self.error_count = 0
        self.warning_count = 0
        self.last_error_notification = None
        # Cooldown tracking: {alert_key: last_sent_datetime}
        self.last_alert_sent: dict = {}
        # Consecutive high-resource counters
        self.consecutive_cpu_high: int = 0
        self.consecutive_mem_high: int = 0
        self.system_metrics = {
            "uptime_start": now_local(),
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
                self.consecutive_mem_high += 1
                if self.consecutive_mem_high >= self.MEM_SUSTAINED_THRESHOLD:
                    await self.send_alert(
                        "WARNING",
                        f"High Memory Usage: {memory_mb:.2f} MB (Threshold: {ALERT_HIGH_MEMORY_MB} MB)"
                    )
            else:
                self.consecutive_mem_high = 0
            
            # Check CPU usage — only alert after N consecutive high readings (sustained spike)
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > ALERT_HIGH_CPU_PERCENT:
                self.consecutive_cpu_high += 1
                if self.consecutive_cpu_high >= self.CPU_SUSTAINED_THRESHOLD:
                    await self.send_alert(
                        "WARNING",
                        f"High CPU Usage: {cpu_percent:.1f}% (Threshold: {ALERT_HIGH_CPU_PERCENT}%)"
                    )
            else:
                self.consecutive_cpu_high = 0
            
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
            
            self.last_health_check = now_local()
            
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
        """Send alert notification to admin, with per-type cooldown to prevent spam"""
        try:
            if not ERROR_NOTIFICATION_ENABLED:
                return

            # --- Cooldown / deduplication ---
            cooldown_secs = self.ALERT_COOLDOWNS.get(level, 1800)
            if cooldown_secs > 0:
                # Use first 80 chars of message as part of key so the same alert type
                # is deduplicated but different messages of the same level still fire
                alert_key = f"{level}:{message[:80]}"
                last_sent = self.last_alert_sent.get(alert_key)
                if last_sent:
                    elapsed = (now_local() - last_sent).total_seconds()
                    if elapsed < cooldown_secs:
                        logger.debug(
                            f"[HealthMonitor] Suppressing {level} alert (cooldown {cooldown_secs - elapsed:.0f}s left)"
                        )
                        return
                self.last_alert_sent[alert_key] = now_local()
            # --- end cooldown ---

            emoji_map = {
                "INFO": "ℹ️",
                "WARNING": "⚠️",
                "ERROR": "❌",
                "CRITICAL": "🚨",
                "SUCCESS": "✅"
            }
            
            emoji = emoji_map.get(level, "📢")
            timestamp = now_local().strftime("%Y-%m-%d %I:%M:%S %p")
            
            alert_msg = f"{emoji} <b>BOT 9 HEALTH ALERT</b>\n\n"
            alert_msg += f"<b>Level:</b> {level}\n"
            alert_msg += f"<b>Time:</b> {timestamp}\n\n"
            alert_msg += f"<b>Message:</b>\n{message}\n\n"
            alert_msg += f"🤖 <b>Source:</b> Bot 9 Auto-Healer"
            
            await bot.send_message(MASTER_ADMIN_ID, alert_msg, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    
    async def send_error_notification(self, error_title: str, error_message: str, stack_trace: str = None):
        """Send instant error notification"""
        try:
            if not ERROR_NOTIFICATION_ENABLED:
                return
            
            # Rate limit error notifications (max 1 per minute for same error)
            now = now_local()
            if self.last_error_notification:
                time_diff = (now - self.last_error_notification).total_seconds()
                if time_diff < 60:
                    return
            
            self.last_error_notification = now
            self.error_count += 1
            
            timestamp = now.strftime("%Y-%m-%d %I:%M:%S %p")
            
            error_msg = f"🚨 <b>BOT 9 ERROR ALERT</b>\n\n"
            error_msg += f"<b>Error #{self.error_count}</b>\n"
            error_msg += f"<b>Time:</b> {timestamp}\n\n"
            error_msg += f"<b>Title:</b> {error_title}\n\n"
            error_msg += f"<b>Message:</b>\n`{error_message[:500]}`\n\n"
            
            if stack_trace and CRITICAL_ERROR_NOTIFY_IMMEDIATELY:
                error_msg += f"<b>Stack Trace:</b>\n```\n{stack_trace[:500]}\n```\n\n"
            
            error_msg += f"💡 <b>System Status:</b> {'Healthy' if self.is_healthy else 'Degraded'}\n"
            error_msg += f"📊 <b>Total Errors:</b> {self.error_count}"
            
            await bot.send_message(MASTER_ADMIN_ID, error_msg, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")
    
    async def log_system_metrics(self):
        """Log system metrics periodically"""
        try:
            uptime = now_local() - self.system_metrics["uptime_start"]
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
                "timestamp": now_local().isoformat(),
                "health_metrics": health_monitor.system_metrics,
                "error_count": health_monitor.error_count,
                "last_backup": now_local().isoformat()
            }
            
            with open(self.state_file, 'wb') as f:
                pickle.dump(state_data, f)
            
            logger.debug(f"State saved at {now_local()}")
            
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
            "banned_at": now_local(),
            "reason": reason,
            "status": "banned"
        })
        logger.info(f"User {user_id} banned: {reason}")
    except Exception as e:
        logger.error(f"Failed to ban user {user_id}: {e}")

def format_datetime_12h(dt: datetime) -> str:
    """Format datetime to 12-hour AM/PM format in local timezone"""
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
            "timestamp": now_local(),
            "action": action
        })
        
        # Count attempts in configured window
        window_ago = now_local() - timedelta(seconds=RATE_LIMIT_SPAM_WINDOW_SECONDS)
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
        timestamp = format_datetime_12h(now_local())
        username = user.username or "No username"
        full_name = user.full_name or "Unknown"
        
        msg = (
            f"🚨 <b>UNAUTHORIZED ACCESS ATTEMPT</b>\n\n"
            f"👤 <b>User ID</b>: `{user.id}`\n"
            f"📝 <b>Username</b>: @{username}\n"
            f"👨 <b>Name</b>: {full_name}\n"
            f"🕐 <b>Time</b>: {timestamp}\n"
            f"🎯 <b>Action</b>: {action}\n"
            f"🔢 <b>Attempt</b>: #{attempt_count}\n\n"
            f"⚠️ <b>Status</b>: Access denied (non-admin)"
        )
        
        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to notify admin about unauthorized access: {e}")

async def notify_admin_auto_ban(user_id: int, user_name: str, username: str, spam_count: int):
    """Notify admin about auto-ban"""
    try:
        timestamp = format_datetime_12h(now_local())
        
        msg = (
            f"🚫 <b>AUTO-BAN TRIGGERED</b>\n\n"
            f"👤 <b>User ID</b>: `{user_id}`\n"
            f"📝 <b>Username</b>: @{username or 'None'}\n"
            f"👨 <b>Name</b>: {user_name or 'Unknown'}\n"
            f"🕐 <b>Time</b>: {timestamp}\n"
            f"⚠️ <b>Reason</b>: Spam detected\n"
            f"📊 <b>Attempts</b>: {spam_count} in 30 seconds\n\n"
            f"🔇 User will receive NO responses (silent ban)"
        )
        
        await bot.send_message(MASTER_ADMIN_ID, msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to notify admin about auto-ban: {e}")

async def check_authorization_user(user: types.User, message: types.Message, action_name: str = "access bot", required_perm: str = None) -> bool:
    """
    Same as check_authorization but accepts a User object directly.
    Use this for callback handlers where message.from_user is the bot itself.
    """
    # Temporarily swap message.from_user by delegating with user_id override
    user_id = user.id

    # 0. Master Admin / Owner Bypass
    if user_id == MASTER_ADMIN_ID or user_id == OWNER_ID:
        return True

    admin_doc = col_admins.find_one({"user_id": user_id})
    if admin_doc:
        if admin_doc.get("is_locked", False):
            return False
        if required_perm:
            perms = admin_doc.get("permissions")
            if perms is not None and required_perm not in perms:
                await message.answer("⛔ <b>ACCESS DENIED</b>\n\nYou do not have permission to access this feature.", parse_mode="HTML")
                return False
        return True

    banned = col_banned_users.find_one({"user_id": user_id})
    if banned:
        return False

    was_banned, attempt_count = await check_spam_and_ban(user_id, user.full_name, user.username, action_name)
    if was_banned:
        return False

    await log_unauthorized_access(user, action_name, attempt_count)
    return False

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
                        "last_active": now_local()
                    }}
                )
            except Exception as e:
                logger.error(f"Failed to update admin info: {e}")

        # C. Check Permission (if required)
        if required_perm:
            # If permissions not set, allow all (backward compatibility)
            perms = admin_doc.get("permissions")
            if perms is not None and required_perm not in perms:
                await message.answer("⛔ <b>ACCESS DENIED</b>\n\nYou do not have permission to access this feature.", parse_mode="HTML")
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

class TutorialPKStates(StatesGroup):
    waiting_for_link = State()           # ADD: waiting for YT link to save
    waiting_for_edit_link = State()      # EDIT: waiting for new/updated YT link
    waiting_for_delete_confirm = State() # DELETE: waiting for CONFIRM keyword


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
    waiting_for_owner_second_confirm = State() # For Double Confirmation

class AdminAuthStates(StatesGroup):
    """Password gate states — used once per session when owner sends /start"""
    pw_first  = State()   # First password entry
    pw_second = State()   # Confirmation entry

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

def generate_unique_msa_code():
    """Generates a random MSAXXXX code and ensures it's completely unique in the DB."""
    import random
    while True:
        code = f"MSA{random.randint(1000, 9999)}"
        if not is_msa_code_duplicate(code):
            return code

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
        title = "✏️ <b>EDIT IG CONTENT</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "delete":
        title = "🗑️ <b>DELETE IG CONTENT</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "ig_affiliate_select":
        title = "📎 <b>SELECT IG FOR AFFILIATE</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "ig_affiliate_edit":
        title = "✏️ <b>EDIT AFFILIATE LINK</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "ig_affiliate_delete":
        title = "🗑️ <b>DELETE AFFILIATE LINK</b> - Select by Index or CC Code"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    else:
        title = "📸 <b>IG CONTENT LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO IG MENU")
    
    if not contents:
        msg = "⚠️ <b>No IG Content found.</b>\nAdd one first!"
        await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=[[cancel_btn]], resize_keyboard=True), parse_mode="HTML")
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
             await message.answer(part, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

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
        # Since all PDFs now auto-generate MSA codes, we display ALL PDFs so users can Replace/Override them.
        query = {}
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
        title = "✏️ <b>EDIT PDF</b> - Select by Index or Name"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "delete":
        title = "🗑️ <b>DELETE PDF</b> - Select by Index or Name"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "affiliate_add_select":
        title = "💸 <b>ADD AFFILIATE LINK</b> - Select PDF (No Link)"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "affiliate_edit_select":
        title = "✏️ <b>EDIT AFFILIATE LINK</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "affiliate_delete":
        title = "🗑️ <b>DELETE AFFILIATE LINK</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "list_affiliate":
        title = "💸 <b>AFFILIATE LINKS LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO AFFILIATE MENU")
    elif mode == "msa_add_select":
        title = "🔑 <b>REPLACE MSA CODE</b> - Select PDF to Override"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "msa_edit_select":
        title = "✏️ <b>EDIT MSA CODE</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "msa_delete":
        title = "🗑️ <b>DELETE MSA CODE</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "list_msa":
        title = "🔑 <b>MSA CODES LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO CODE MENU")
    elif mode == "yt_add_select":
        title = "▶️ <b>ADD YT LINK</b> - Select PDF (No YT)"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "yt_add_select":
        title = "▶️ <b>ADD YT LINK</b> - Select PDF (No YT)"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "yt_edit_select":
        title = "✏️ <b>EDIT YT LINK</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "yt_delete":
        title = "🗑️ <b>DELETE YT LINK</b> - Select PDF"
        cancel_btn = KeyboardButton(text="❌ CANCEL")
    elif mode == "list_yt":
        title = "▶️ <b>YT LINKS LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO YT MENU")
    else:
        title = "📂 <b>PDF LIST</b>"
        cancel_btn = KeyboardButton(text="⬅️ BACK TO PDF MENU")

    if not pdfs:
        msg = f"📂 No PDFs found matching criteria.\nTotal: {total}"
        if mode == "affiliate_add_select":
            msg = "⚠️ <b>All existing PDFs already have Affiliate Links!</b>\nPlease add a new PDF first."
        elif mode == "affiliate_edit_select":
            msg = "⚠️ <b>No Affiliate Links found to edit.</b>\nAdd one first!"
        elif mode == "msa_add_select":
            msg = "⚠️ <b>All existing PDFs already have MSA Codes!</b>\nPlease add a new PDF first."
        elif mode == "msa_edit_select":
            msg = "⚠️ <b>No MSA Codes found to edit.</b>\nAdd one first!"
        elif mode == "msa_delete" or mode == "list_msa":
            msg = "⚠️ <b>No MSA Codes found.</b>\nAdd one first!"
        elif mode == "yt_add_select":
            msg = "⚠️ <b>All existing PDFs already have YT Links!</b>\nPlease add a new PDF first."
        elif mode == "yt_edit_select":
            msg = "⚠️ <b>No YT Links found to edit.</b>\nAdd one first!"
        elif mode == "yt_delete" or mode == "list_yt":
            msg = "⚠️ <b>No YT Links found.</b>\nAdd one first!"
            
        await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=[[cancel_btn]], resize_keyboard=True), parse_mode="HTML")
        return

    text = f"<b>{title} (Page {page+1})</b>\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    
    # Use sequential numbering for list modes, actual index for others
    use_sequential = mode in ["list_affiliate", "list_msa", "list_yt", "yt_delete"]
    
    for idx, pdf in enumerate(pdfs, start=1):
        # Display index: sequential for list modes, actual for operation modes
        display_index = skip + idx if use_sequential else pdf['index']
        
        clean_name = pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
        text += f"<b>{display_index}.</b> <code>{clean_name}</code>\n"
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
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML", disable_web_page_preview=True)



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
        [KeyboardButton(text="📸 IG"), KeyboardButton(text="🎬 TUTORIAL")],
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
        [KeyboardButton(text="✏️ EDIT CODE"), KeyboardButton(text="🗑️ DELETE CODE")],
        [KeyboardButton(text="📋 LIST CODE"), KeyboardButton(text="⬅️ BACK TO ADD MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_yt_menu():
    """YT Submenu Structure"""
    keyboard = [
        [KeyboardButton(text="➕ ADD YT LINK"), KeyboardButton(text="✏️ EDIT YT LINK")],
        [KeyboardButton(text="🗑️ DELETE YT LINK"), KeyboardButton(text="📋 LIST YT LINK")],
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
        [KeyboardButton(text="👑 OWNER")],
        [KeyboardButton(text="👨‍💼 MANAGER"), KeyboardButton(text="👔 ADMIN")],
        [KeyboardButton(text="🛡️ MODERATOR"), KeyboardButton(text="👨‍💻 SUPPORT")],
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
        [KeyboardButton(text="🆔 MSA ID POOL")],
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

def get_tutorial_pk_menu():
    """Tutorial Submenu — universal tutorial link management."""
    keyboard = [
        [KeyboardButton(text="➕ ADD TUTORIAL"), KeyboardButton(text="✏️ EDIT TUTORIAL")],
        [KeyboardButton(text="🗑️ DELETE TUTORIAL"), KeyboardButton(text="📋 LIST TUTORIAL")],
        [KeyboardButton(text="⬅️ BACK TO ADD MENU")]
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
        "🔐 <b>Admin Management</b>\nSelect an option below:",
        reply_markup=get_admin_config_menu(),
        parse_mode="HTML"
    )

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
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
        "🤖 <b>BOT 9 ONLINE</b>\n"
        "System Authorized. Accessing Mainframe...",
        reply_markup=get_main_menu(message.from_user.id),
        parse_mode="HTML"
    )


# ──────────────────────────────────────────────────────────────────────────────
# 🔐 ADMIN PASSWORD GATE (owner only, once per session, double confirmation)
# ──────────────────────────────────────────────────────────────────────────────

@dp.message(AdminAuthStates.pw_first)
async def admin_pw_first(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text and message.text.strip() in ("❌ CANCEL", "❌ Cancel"):
        await state.clear()
        await message.answer("❌ Authentication cancelled.", reply_markup=ReplyKeyboardRemove())
        return
    try: await message.delete()
    except: pass
    data = await state.get_data()
    attempts = data.get("pw_attempts", 0)
    if not ADMIN_PASSWORD:
        _admin_authenticated.add(user_id)
        await state.clear()
        await cmd_start(message, state)
        return
    if message.text == ADMIN_PASSWORD:
        await state.update_data(pw_first_ok=True, pw_attempts=0)
        await state.set_state(AdminAuthStates.pw_second)
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


@dp.message(AdminAuthStates.pw_second)
async def admin_pw_second(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text and message.text.strip() in ("❌ CANCEL", "❌ Cancel"):
        # Cancel = skip auth for this session (owner ID already verified)
        _admin_authenticated.add(user_id)
        await state.clear()
        await cmd_start(message, state)
        return
    try: await message.delete()
    except: pass
    if message.text == ADMIN_PASSWORD:
        _admin_authenticated.add(user_id)
        await state.clear()
        await cmd_start(message, state)
    else:
        await state.clear()
        await message.answer(
            "❌ Passwords did not match. Authentication failed.\n\nUse /start to try again.",
            reply_markup=ReplyKeyboardRemove(),
        )


@dp.message(F.text == "⬅️ BACK TO MAIN MENU")
async def back_to_main_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Back to Main Menu"):
        return
    await state.clear()
    await message.answer("🏠 Main Menu", reply_markup=get_main_menu(message.from_user.id))

@dp.message(F.text == "🏠 MAIN MENU")
async def main_menu_from_admin_handler(message: types.Message, state: FSMContext):
    """Return to Main Menu (globally available for admins)"""
    if not await check_authorization(message, "Main Menu"):
        return
    await state.clear()
    await message.answer(
        "👋 <b>Welcome Back!</b>\nSelect an option from the menu below:",
        reply_markup=get_main_menu(message.from_user.id),
        parse_mode="HTML"
    )

@dp.message(F.text == "➕ NEW ADMIN")
async def new_admin_handler(message: types.Message, state: FSMContext):
    """Ask for new admin's user ID"""
    if not await check_authorization(message, "New Admin"):
        return
    await state.set_state(AdminManagementStates.waiting_for_new_admin_id)
    await message.answer(
        "➕ <b>ADD NEW ADMIN</b>\n\n"
        "Please send the <b>Telegram User ID</b> of the user you want to add as admin.\n\n"
        "Example: `123456789`",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")]
            ],
            resize_keyboard=True
        ),
        parse_mode="HTML"
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
            "⚠️ <b>Invalid Input</b>\n\n"
            "Please send a valid numeric Telegram User ID.",
            parse_mode="HTML"
        )
        return
    
    new_admin_id = int(message.text)
    
    # Check if Banned
    if is_banned(new_admin_id):
        await message.answer(
            f"⛔ <b>ACTION DENIED</b>\n\n"
            f"User `{new_admin_id}` is currently <b>BANNED</b>.\n"
            f"You cannot add a banned user as an Admin.\n\n"
            f"<i>Please unban them first from the Ban Config menu.</i>",
            reply_markup=get_admin_config_menu(),
            parse_mode="HTML"
        )
        await state.clear()
        return
    
    # Check for duplicates
    existing = col_admins.find_one({"user_id": new_admin_id})
    if existing:
        await message.answer(
            f"⚠️ <b>Admin Already Exists</b>\n\n"
            f"User ID `{new_admin_id}` is already an admin.",
            parse_mode="HTML"
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
        "added_at": now_local(),
        "permissions": [],      # LOCKED by default — NO permissions until unlocked by owner
        "full_name": admin_name,
        "username": admin_username,
        "is_locked": True       # Must be explicitly unlocked before they can use bot
    })
    
    await state.clear()
    await message.answer(
        f"✅ <b>Admin Added Successfully!</b>\n\n"
        f"User ID: `{new_admin_id}`\n"
        f"Added by: {message.from_user.id}\n\n"
        f"⚠️ <b>NOTE: New Admins are LOCKED by default.</b>\n"
        f"Use the Lock Menu to unlock them.",
        reply_markup=get_admin_config_menu(),
        parse_mode="HTML"
    )

@dp.message(F.text == "➖ REMOVE ADMIN")
async def remove_admin_handler(message: types.Message, state: FSMContext):
    """Show list of admins with pagination"""
    if not await check_authorization(message, "Remove Admin"):
        return
    # Exclude Master Admin
    admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}))
    
    if not admins:
        await message.answer(
            "⚠️ <b>No Other Admins Found</b>\n\n"
            "There are no admins to remove.\n"
            "Use <b>➕ NEW ADMIN</b> to add administrators.",
            parse_mode="HTML"
        )
        return
    
    # Store current page in state
    await state.set_state(AdminManagementStates.viewing_admin_list)
    await state.update_data(page=0)
    
    # Show first page
    await show_admin_list_page(message, admins, page=0)

async def show_admin_list_page(message: types.Message, admins: list, page: int):
    """Display admin list with pagination"""
    ADMINS_PER_PAGE = 5
    total_pages = max(1, (len(admins) + ADMINS_PER_PAGE - 1) // ADMINS_PER_PAGE)
    
    # Cap page just in case
    page = min(page, max(0, total_pages - 1))
    
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
        admin_list_text += f"{i+1}. <b>{name}</b> (`{user_id}`) [{status_icon}]\n"
        
        # Add button
        btn_text = f"❌ Remove: {name} ({user_id})"
        keyboard.append([KeyboardButton(text=btn_text)])
    
    # Add navigation buttons if needed
    nav_buttons = []
    if page > 0:
        nav_buttons.append(KeyboardButton(text="⬅️ PREV ADMINS"))
    if page < total_pages - 1:
        nav_buttons.append(KeyboardButton(text="➡️ NEXT ADMINS"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    keyboard.append([KeyboardButton(text="⬅️ RETURN BACK"), KeyboardButton(text="🏠 MAIN MENU")])
    
    await message.answer(
        f"➖ <b>REMOVE ADMIN</b>\n\n"
        f"Click on an admin to remove them:\n\n"
        f"{admin_list_text}\n"
        f"📊 Page {page + 1}/{total_pages} | Total: {len(admins)} admins",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="HTML"
    )

@dp.message(AdminManagementStates.viewing_admin_list)
async def process_admin_removal(message: types.Message, state: FSMContext):
    """Handle admin removal or pagination"""
    if message.text == "🏠 MAIN MENU":
        await state.clear()
        await message.answer("🏠 Main Menu", reply_markup=get_main_menu(message.from_user.id))
        return
    elif message.text in ["⬅️ RETURN BACK", "⬅️ BACK TO ADMIN MENU", "/cancel"]:
        await state.clear()
        await message.answer("⚙️ <b>Admin Management Menu</b>", reply_markup=get_admin_config_menu(), parse_mode="HTML")
        return
        
    data = await state.get_data()
    current_page = data.get("page", 0)
    admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}))
    
    # Handle pagination
    if message.text == "➡️ NEXT ADMINS":
        await state.update_data(page=current_page + 1)
        await show_admin_list_page(message, admins, current_page + 1)
        return
    elif message.text == "⬅️ PREV ADMINS":
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
        # Extra safety check to prevent removing Master Admin
        if target_id == MASTER_ADMIN_ID:
            await message.answer("🚫 <b>You cannot remove the Master Admin.</b>", parse_mode="HTML")
            return
            
        try:
            # Remove from database
            result = col_admins.delete_one({"user_id": target_id})
            
            if result.deleted_count > 0:
                await state.clear()
                await message.answer(
                    f"✅ <b>Admin Removed</b>\n\n"
                    f"User ID `{target_id}` is no longer an admin.\n"
                    f"They cannot access Bot 9 anymore.",
                    reply_markup=get_admin_config_menu(),
                    parse_mode="HTML"
                )
            else:
                await message.answer("⚠️ Admin not found in database.")
        except Exception as e:
            logger.error(f"Error removing admin: {e}")
            await message.answer("❌ Error removing admin.")
    else:
        await message.answer("⚠️ Invalid selection.")

@dp.message(F.text == "⬅️ BACK TO ADD MENU")
async def back_to_add_handler(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("➕ <b>SELECT ADD COMPONENT:</b>", reply_markup=get_add_menu(), parse_mode="HTML")

@dp.message(F.text == "➕ ADD")
async def add_menu_handler(message: types.Message):
    """Show Add Submenu"""
    if not await check_authorization(message, "Access Add Menu", "can_add"):
        return
    await message.answer(
        "➕ <b>SELECT ADD COMPONENT:</b>",
        reply_markup=get_add_menu(),
        parse_mode="HTML"
    )

@dp.message(F.text == "📄 PDF")
async def pdf_menu_handler(message: types.Message):
    if not await check_authorization(message, "PDF Menu", "can_add"):
        return
    await message.answer("📄 <b>PDF MANAGEMENT</b>", reply_markup=get_pdf_menu(), parse_mode="HTML")

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
    await message.answer("📄 <b>Enter PDF Name:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(PDFStates.waiting_for_add_name)
async def process_add_pdf_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("📄 <b>PDF MANAGEMENT</b>", reply_markup=get_pdf_menu(), parse_mode="HTML")
    
    name = message.text.strip()
    
    # Validation: Check duplicate name
    conflict_pdf = is_pdf_name_duplicate(name)
    if conflict_pdf:
        await message.answer(f"⚠️ <b>Name Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different name:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
        
    await state.update_data(name=name)
    await state.set_state(PDFStates.waiting_for_add_link)
    await message.answer(f"✅ Name set to: <b>{name}</b>\n\n🔗 <b>Enter PDF Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(PDFStates.waiting_for_add_link)
async def process_add_pdf_link(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("📄 <b>PDF MANAGEMENT</b>", reply_markup=get_pdf_menu(), parse_mode="HTML")
    
    link = message.text.strip()
    
    # Validation: Check duplicate link
    conflict_pdf = is_pdf_link_duplicate(link)
    if conflict_pdf:
        await message.answer(f"⚠️ <b>Link Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
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
        "created_at": now_local(),
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
        "last_yt_code_click": None,
        "msa_code": generate_unique_msa_code()
    }
    col_pdfs.insert_one(doc)
    
    # Log Action
    log_user_action(message.from_user, "Added PDF", f"Name: {name}, Index: {idx}")

    await state.clear()
    await message.answer(f"✅ <b>PDF Added!</b>\n\n🆔 Index: `{idx}`\n📄 Name: `{name}`\n🔗 Link: `{link}`", reply_markup=get_pdf_menu(), parse_mode="HTML")

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
    await message.answer("📄 <b>PDF MANAGEMENT</b>", reply_markup=get_pdf_menu(), parse_mode="HTML")

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
    clean_name = pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
    await message.answer(
        f"📄 <b>PDF FOUND</b>\n"
        f"🆔 Index: <code>{pdf['index']}</code>\n"
        f"📛 Name: {clean_name}\n"
        f"🔗 Link: {pdf['link']}\n\n"
        "⬇️ <b>Select what to edit:</b>",
        reply_markup=kb,
        parse_mode="HTML",
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
        await message.answer("⌨️ <b>Enter New Name:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
    elif message.text == "🔗 EDIT LINK":
        await state.update_data(field="link")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ <b>Enter New Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
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
            await message.answer(f"⚠️ <b>Same Name!</b>\nYou entered the exact same name.\nPlease enter a different name:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return

        # Check duplicate name (exclude current PDF)
        conflict_pdf = is_pdf_name_duplicate(new_value, exclude_id=data['edit_id'])
        if conflict_pdf:
            clean_name = conflict_pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
            await message.answer(f"⚠️ <b>Name Already Exists!</b>\nUsed by:\n🆔 Index: <code>{conflict_pdf['index']}</code>\n📄 Name: <code>{clean_name}</code>\n\nTry another name:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return
            
        col_pdfs.update_one({"_id": ObjectId(data['edit_id'])}, {"$set": {"name": new_value}})
        msg = f"✅ <b>PDF Name Updated!</b>\nOld: {data['current_name']}\nNew: {new_value}"
        log_user_action(message.from_user, "Edited PDF Name", f"ID: {data['edit_id']}, New: {new_value}")
    
    elif field == "link":
        # Check if same as current
        if new_value == data['current_link']:
            await message.answer(f"⚠️ <b>Same Link!</b>\nYou entered the exact same link.\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return

        # Check duplicate link (exclude current PDF)
        conflict_pdf = is_pdf_link_duplicate(new_value, exclude_id=data['edit_id'])
        if conflict_pdf:
            clean_name = conflict_pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
            await message.answer(f"⚠️ <b>Link Already Exists!</b>\nUsed by:\n🆔 Index: <code>{conflict_pdf['index']}</code>\n📄 Name: <code>{clean_name}</code>\n\nTry another link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return
            
        # Basic Validation
        if "http" not in new_value and "t.me" not in new_value:
            await message.answer("⚠️ Invalid Link. Please enter a valid URL.", reply_markup=get_cancel_keyboard())
            return

        col_pdfs.update_one({"_id": ObjectId(data['edit_id'])}, {"$set": {"link": new_value}})
        msg = f"✅ <b>PDF Link Updated!</b>\nOld: {data['current_link']}\nNew: {new_value}"
        log_user_action(message.from_user, "Edited PDF Link", f"ID: {data['edit_id']}, New: {new_value}")
    else:
        msg = "⚠️ An unexpected error occurred."

    await state.clear()
    await message.answer(msg, reply_markup=get_pdf_menu(), parse_mode="HTML")

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
        msg = "❌ <b>No PDFs Found</b>\n\n"
        if not_found:
            msg += "Not found:\n" + "\n".join(f"• `{q}`" for q in not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
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
    msg = f"⚠️ <b>CONFIRM BULK DELETION</b>\n\n"
    msg += f"📊 <b>Total to delete: {len(found_pdfs)} PDF(s)</b>\n\n"
    
    for idx, pdf in enumerate(found_pdfs, 1):
        clean_name = pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
        msg += f"{idx}. <code>{pdf['index']}</code> - {clean_name}\n"
    
    if not_found:
        msg += f"\n⚠️ <b>Not Found ({len(not_found)}):</b>\n"
        msg += "\n".join(f"• <code>{q}</code>" for q in not_found[:5])  # Limit to 5
        if len(not_found) > 5:
            msg += f"\n...and {len(not_found) - 5} more"
    
    msg += "\n\n❓ Confirm deletion?"
    
    await message.answer(
        msg,
        reply_markup=kb,
        parse_mode="HTML",
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
                f"🗑️ <b>Bulk Deletion Complete</b>\n\n"
                f"✅ Successfully deleted <b>{deleted_count} PDF(s)</b>\n"
                f"📊 Indices automatically reorganized",
                reply_markup=get_pdf_menu(),
                parse_mode="HTML"
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
                    "🗑️ <b>PDF Deleted Successfully.</b>\n"
                    "📊 Indices automatically reorganized",
                    reply_markup=get_pdf_menu(),
                    parse_mode="HTML"
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
    await message.answer("💸 <b>AFFILIATE MANAGEMENT</b>", reply_markup=get_affiliate_menu(), parse_mode="HTML")

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
        return await message.answer("💸 <b>AFFILIATE MANAGEMENT</b>", reply_markup=get_affiliate_menu(), parse_mode="HTML")

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
        msg = "❌ <b>No PDFs Found</b>\n\n"
        if not_found:
            msg += "Not found:\n" + "\n".join(f"• `{q}`" for q in not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
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
        msg = f"💸 <b>MULTIPLE PDFs SELECTED ({len(found_pdfs)})</b>\n\n"
        for idx, pdf in enumerate(found_pdfs, 1):
            msg += f"{idx}. `{pdf['index']}` - {pdf['name']}\n"
        
        if not_found:
            msg += f"\n⚠️ <b>Not Found ({len(not_found)}):</b>\n"
            msg += "\n".join(f"• `{q}`" for q in not_found[:5])
            if len(not_found) > 5:
                msg += f"\n...and {len(not_found) - 5} more"
        
        msg += "\n\n📝 <b>Enter affiliate link to apply to ALL selected PDFs:</b>"
    else:
        # Single selection
        pdf = found_pdfs[0]
        current_aff = pdf.get("affiliate_link", "None")
        msg = (
            f"💸 <b>SELECTED PDF:</b>\n`{pdf['index']}`. {pdf['name']}\n"
            f"Current Affiliate Link: `{current_aff}`\n\n"
            "📝 <b>Enter new affiliate link:</b>"
        )
    
    await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")

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
            f"✅ <b>Bulk Affiliate Link Assignment Complete!</b>\n\n"
            f"📊 Successfully set affiliate link for <b>{updated_count} PDF(s)</b>\n"
            f"🔗 Link: `{link}`",
            reply_markup=get_affiliate_menu(),
            parse_mode="HTML"
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
                f"⚠️ <b>Same Link!</b>\nYou entered the exact same affiliate link.\nPlease enter a different link:",
                reply_markup=get_cancel_keyboard(),
                parse_mode="HTML"
            )
            return
        
        col_pdfs.update_one(
            {"_id": ObjectId(pdf_id)},
            {"$set": {"affiliate_link": link}}
        )
        
        await state.clear()
        await message.answer(
            f"✅ <b>Affiliate Link Set for {pdf_name}!</b>",
            reply_markup=get_affiliate_menu(),
            parse_mode="HTML"
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
    await message.answer("💸 <b>AFFILIATE MANAGEMENT</b>", reply_markup=get_affiliate_menu(), parse_mode="HTML")

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
        msg = "❌ <b>No PDFs Found</b>\n\n"
        if not_found:
            msg += "Not found or no affiliate link:\n" + "\n".join(f"• `{q}`" for q in not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
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
        msg = f"⚠️ <b>CONFIRM BULK AFFILIATE DELETE</b>\n\n"
        msg += f"📊 <b>Total to delete: {len(found_pdfs)} affiliate link(s)</b>\n\n"
        for idx, pdf in enumerate(found_pdfs, 1):
            msg += f"{idx}. `{pdf['index']}` - {pdf['name']}\n"
        
        if not_found:
            msg += f"\n⚠️ <b>Not Found ({len(not_found)}):</b>\n"
            msg += "\n".join(f"• `{q}`" for q in not_found[:5])
            if len(not_found) > 5:
                msg += f"\n...and {len(not_found) - 5} more"
        
        msg += "\n\n❓ Remove affiliate links from all selected PDFs?"
    else:
        pdf = found_pdfs[0]
        msg = f"⚠️ Remove Affiliate Link from <b>{pdf['name']}</b>?"
    
    await message.answer(msg, reply_markup=kb, parse_mode="HTML")

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
                f"🗑️ <b>Bulk Affiliate Delete Complete!</b>\n\n"
                f"✅ Removed affiliate links from <b>{deleted_count} PDF(s)</b>",
                reply_markup=get_affiliate_menu(),
                parse_mode="HTML"
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
                f"🗑️ Affiliate Link Removed from <b>{pdf_name}</b>.",
                reply_markup=get_affiliate_menu(),
                parse_mode="HTML"
            )
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_affiliate_menu())

@dp.message(F.text == "🔑 CODE")
async def code_menu_handler(message: types.Message):
    if not await check_authorization(message, "Code Menu", "can_add"):
        return
    await message.answer("🔑 <b>CODE MANAGEMENT</b>", reply_markup=get_code_menu(), parse_mode="HTML")

# --- MSA CODE HANDLERS ---

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
        f"✏️ <b>EDITING MSA CODE</b>\n"
        f"📄 PDF: {pdf['name']}\n"
        f"🔑 Current Code: `{pdf['msa_code']}`\n\n"
        "⌨️ <b>Enter New MSA Code</b> (Format: MSA12345):",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
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
        await message.answer(f"⚠️ <b>Same Code!</b>\nYou entered the exact same MSA code.\nPlease enter a different code:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return

    # Check for duplicates (exclude current PDF)
    conflict_pdf = is_msa_code_duplicate(code, exclude_pdf_id=data['pdf_id'])
    if conflict_pdf:
        clean_name = conflict_pdf['name'].replace('<', '&lt;').replace('>', '&gt;')
        await message.answer(f"⚠️ <b>MSA Code Already Exists!</b>\nUsed by:\n🆔 Index: <code>{conflict_pdf['index']}</code>\n📄 Name: <code>{clean_name}</code>\n\nPlease enter a unique code.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
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
        f"✅ <b>MSA Code Updated for {pdf_name}!</b>\n\n"
        f"🔴 Old Code: `{old_code}`\n"
        f"🟢 New Code: `{code}`",
        reply_markup=get_code_menu(),
        parse_mode="HTML"
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
    msg = "⚠️ <b>CONFIRM BULK DELETION</b> ⚠️\n\n" if len(pdf_ids) > 1 else "⚠️ <b>CONFIRM DELETION</b>\n\n"
    msg += "You are about to remove MSA Codes from:\n"
    
    for p in found_pdfs:
        msg += f"• `{p['index']}`. <b>{p['name']}</b> (Code: `{p.get('msa_code', 'N/A')}`)\n"
        
    if not_found or no_code:
        msg += "\n⚠️ <b>SKIPPED ITEMS (Ignored):</b>\n"
        for q in not_found:
            msg += f"• `{q}`: Not Found\n"
        for name in no_code:
            msg += f"• `{name}`: No MSA Code assigned\n"
        
    msg += "\n<b>Are you sure?</b>"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(MSACodeDeleteStates.waiting_for_confirm)
    await message.answer(msg, reply_markup=kb, parse_mode="HTML")

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
                f"🗑️ <b>Deletion Complete</b>\nRemoved MSA Codes from {count} PDF(s).",
                reply_markup=get_code_menu(),
                parse_mode="HTML"
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
    await message.answer("🔑 <b>CODE MANAGEMENT</b>", reply_markup=get_code_menu(), parse_mode="HTML")


@dp.message(F.text == "▶️ YT")
async def yt_menu_handler(message: types.Message):
    if not await check_authorization(message, "YT Menu", "can_add"):
        return
    await message.answer("▶️ <b>YT MANAGEMENT</b>", reply_markup=get_yt_menu(), parse_mode="HTML")

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
        f"▶️ <b>Selected PDF:</b> {pdf['name']}\n\n"
        "⌨️ <b>Enter YouTube Video Title:</b>",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
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
         await message.answer(f"⚠️ <b>YT Title Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different title:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
         return

    await state.set_state(YTStates.waiting_for_link)
    await message.answer("🔗 <b>Enter YouTube Short Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

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
        await message.answer(f"⚠️ <b>YT Link Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return

    if "http" not in link and "youtu" not in link:
        await message.answer("⚠️ <b>Invalid YouTube Link.</b> Try again:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
        
    data = await state.get_data()
    from bson.objectid import ObjectId
    
    col_pdfs.update_one(
        {"_id": ObjectId(data['pdf_id'])},
        {"$set": {"yt_title": data['yt_title'], "yt_link": link}}
    )
    
    await state.clear()
    await message.answer(
        f"✅ <b>YT Link added to {data['pdf_name']}!</b>\n\n"
        f"▶️ Title: {data['yt_title']}\n"
        f"🔗 Link: {link}",
        reply_markup=get_yt_menu(),
        parse_mode="HTML"
    )

# 2. EDIT YT
@dp.message(F.text == "✏️ EDIT YT LINK")
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
        f"▶️ <b>YT DATA FOR: {pdf['name']}</b>\n"
        f"Title: {current_title}\n"
        f"Link: {current_link}\n\n"
        "⬇️ <b>Select what to edit:</b>",
        reply_markup=kb,
        parse_mode="HTML",
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
        await message.answer("⌨️ <b>Enter New Title:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
    elif message.text == "🔗 EDIT LINK":
        await state.update_data(field="yt_link")
        await state.set_state(YTEditStates.waiting_for_value)
        await message.answer("⌨️ <b>Enter New Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
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
            await message.answer(f"⚠️ <b>YT Link Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different link:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return
            
        if "http" not in new_value and "youtu" not in new_value:
             await message.answer("⚠️ <b>Invalid YouTube Link.</b> Try again:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
             return

    elif data['field'] == "yt_title":
        pass # Allow same value update (no-op but good UX)

        # Validation: Check duplicate YT title (exclude current PDF)
        conflict_pdf = is_yt_title_duplicate(new_value, exclude_id=data['pdf_id'])
        if conflict_pdf:
            await message.answer(f"⚠️ <b>YT Title Already Exists!</b>\nUsed by:\n🆔 Index: `{conflict_pdf['index']}`\n📄 Name: `{conflict_pdf['name']}`\n\nPlease enter a different title:", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            return

    col_pdfs.update_one(
        {"_id": ObjectId(data['pdf_id'])},
        {"$set": {data['field']: new_value}}
    )
    
    await state.clear()
    field_name = "Title" if data['field'] == "yt_title" else "Link"
    await message.answer(f"✅ <b>YT {field_name} Updated for {data['pdf_name']}!</b>", reply_markup=get_yt_menu(), parse_mode="HTML")

# 3. DELETE YT
@dp.message(F.text == "🗑️ DELETE YT LINK")
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
    msg = "⚠️ <b>CONFIRM BULK DELETION</b> ⚠️\n\n" if len(pdf_ids) > 1 else "⚠️ <b>CONFIRM DELETION</b>\n\n"
    msg += "You are about to remove YT Data from:\n"
    
    for p in found_pdfs:
        # Show both sequential position? No, show actual Name and Title
        msg += f"• <b>{p['name']}</b> (YT: {p.get('yt_title', 'N/A')})\n"
        
    if not_found:
        msg += "\n⚠️ <b>SKIPPED ITEMS (Ignored):</b>\n"
        for q in not_found:
            msg += f"• `{q}`: Not valid sequential ID\n"
        
    msg += "\n<b>Are you sure?</b>"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    
    await state.set_state(YTDeleteStates.waiting_for_confirm)
    await message.answer(msg, reply_markup=kb, parse_mode="HTML")

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
                f"🗑️ <b>Deletion Complete</b>\nRemoved YT Data from {count} PDF(s).",
                reply_markup=get_yt_menu(),
                parse_mode="HTML"
            )
        else:
            await state.clear()
            await message.answer("⚠️ No PDFs selected.", reply_markup=get_yt_menu())
    else:
        await state.clear()
        await message.answer("❌ Cancelled", reply_markup=get_yt_menu())

# 4. LIST YT
@dp.message(F.text == "📋 LIST YT LINK")
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
    await message.answer("▶️ <b>YT MANAGEMENT</b>", reply_markup=get_yt_menu(), parse_mode="HTML")

@dp.message(F.text == "🔗 LINKS")
async def links_menu_handler(message: types.Message):
    if not await check_authorization(message, "Links Menu", "can_list"):
        return
    await message.answer("🔗 <b>DEEP LINKS MANAGER</b>\nSelect a category to generate links:", reply_markup=get_links_menu(), parse_mode="HTML")

@dp.message(F.text == "🏠 HOME YT")
async def home_yt_handler(message: types.Message):
    if not await check_authorization(message, "Home YT Link", "can_list"):
        return
    code = await get_home_yt_code()
    username = BOT_USERNAME
    
    link = f"https://t.me/{username}?start={code}_YTCODE"
    
    text = (
        "🏠 <b>HOME YT LINK</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 Link: <code>{link}</code>\n"
        f"🔑 Code: <code>{code}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        " This link is permanent and unique."
    )
    await message.answer(text, parse_mode="HTML")

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

    text = f"📸 <b>IG CC LINKS</b> (Page {page+1})\n━━━━━━━━━━━━━━━━━━━━\n\n"
    username = BOT_USERNAME
    
    for content in contents:
        # Ensure Code
        content = await ensure_ig_cc_code(content)
        code = content['start_code']
        cc_code = content['cc_code']
        
        link = f"https://t.me/{username}?start={code}_igcc_{cc_code}"
        
        text += (
            f"🆔 <b>{cc_code}</b>\n"
            f"🔗 <code>{link}</code>\n"
            f"🔑 Start Code: <code>{code}</code>\n"
            "────────────────────\n"
        )
    
    # Pagination Buttons
    buttons = []
    if page > 0: buttons.append(KeyboardButton(text=f"⬅️ PREV_IGLINK {page}"))
    if (skip + limit) < total: buttons.append(KeyboardButton(text=f"➡️ NEXT_IGLINK {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK TO LINKS MENU")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML", disable_web_page_preview=True)

@dp.message(F.text == "⬅️ BACK TO LINKS MENU")
async def back_to_links_menu(message: types.Message):
    await message.answer("🔗 <b>DEEP LINKS MANAGER</b>", reply_markup=get_links_menu(), parse_mode="HTML")

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

    text = f"📑 <b>ALL PDF LINKS</b> (Page {page+1})\n━━━━━━━━━━━━━━━━━━━━\n\n"
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
        
        text += f"🆔 `{pdf['index']}`. <b>{pdf.get('name', 'Unknown')}</b>\n"
        
        if missing:
            text += f"⚠️ <b>Missing Details:</b> {', '.join(missing)}\n"
            text += "🚫 Links not generated. Please fill all fields.\n"
        else:
            # Generate Links
            pdf = await ensure_pdf_codes(pdf)
            ig_code = pdf['ig_start_code']
            yt_code = pdf['yt_start_code']
            aff_code = pdf['aff_start_code']
            orig_code = pdf['orig_start_code']
            username = BOT_USERNAME  # Use environment variable for bot username
            
            # Sanitize Name (Alphanumeric + Underscore)
            sanitized_name = re.sub(r'[^a-zA-Z0-9]', '_', pdf['name'])
            sanitized_name = re.sub(r'_+', '_', sanitized_name).strip('_')
            
            ig_link = f"https://t.me/{username}?start={ig_code}_ig_{sanitized_name}"
            yt_link = f"https://t.me/{username}?start={yt_code}_yt_{sanitized_name}"
            aff_link = f"https://t.me/{username}?start={aff_code}_aff_{sanitized_name}"
            orig_link = f"https://t.me/{username}?start={orig_code}_orig_{sanitized_name}"
            
            text += (
                f"📸 <b>IG Link</b>: <code>{ig_link}</code>\n"
                f"   └ 🎟️ Code: <code>{ig_code}</code>\n\n"
                f"▶️ <b>YT Link</b>: <code>{yt_link}</code>\n"
                f"   └ 🎟️ Code: <code>{yt_code}</code>\n\n"
                f"🔐 <b>MSA Code</b>: <code>{pdf['msa_code']}</code>\n"
            )
        text += "────────────────────\n"
    
    # Pagination
    buttons = []
    if page > 0: buttons.append(KeyboardButton(text=f"⬅️ PREV_PDFLINK {page}"))
    if (skip + limit) < total: buttons.append(KeyboardButton(text=f"➡️ NEXT_PDFLINK {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="⬅️ BACK TO LINKS MENU")])
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML", disable_web_page_preview=True)

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_PDFLINK") or m.text.startswith("➡️ NEXT_PDFLINK")))
async def pdf_link_pagination(message: types.Message):
    try:
        page = int(message.text.split()[-1]) - 1
        await all_pdf_links_handler(message, page=page)
    except:
        await message.answer("❌ Error navigating.")

@dp.message(F.text == "📸 IG")
async def ig_menu_handler(message: types.Message):
    if not await check_authorization(message, "IG Menu", "can_add"):
        return
    await message.answer("📸 <b>IG MANAGEMENT</b>", reply_markup=get_ig_menu(), parse_mode="HTML")

# --- IG HANDLERS ---

# 1. ADD IG
@dp.message(F.text == "➕ ADD IG")
async def start_add_ig(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Add IG", "can_add"):
        return
    await state.set_state(IGStates.waiting_for_content_name)
    await message.answer("📝 <b>Enter IG Content:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")

@dp.message(IGStates.waiting_for_content_name)
async def process_ig_content_name(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Cancelled.", reply_markup=get_ig_menu())
    
    content_name = message.text.strip()
    
    # Check for duplicate name
    if is_ig_name_duplicate(content_name):
        await message.answer("⚠️ <b>Content name already exists!</b>\nPlease use a different name.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    
    # Auto-generate CC code
    cc_code, cc_number = get_next_cc_code()
    
    # Save to database
    from datetime import datetime
    doc = {
        "cc_code": cc_code,
        "cc_number": cc_number,
        "name": content_name,
        "created_at": now_local(),
        # Initialize click tracking field
        "ig_cc_clicks": 0,
        "last_ig_cc_click": None
    }
    col_ig_content.insert_one(doc)
    
    await state.clear()
    await message.answer(
        f"✅ <b>IG Content Added!</b>\n\n"
        f"🆔 Code: <b>{cc_code}</b>\n"
        f"📝 Name: {content_name}",
        reply_markup=get_ig_menu(),
        parse_mode="HTML"
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
    text = f"✅ <b>SELECTED IG CONTENT</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"`{display_index}`. <b>{content['cc_code']}</b>\n"
    text += f"📝 Full Content:\n{content['name']}\n"
    
    await state.set_state(IGEditStates.waiting_for_new_name)
    await message.answer(
        text + "\n━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter New Content:</b>",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
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
        await message.answer("⚠️ <b>Content name already exists!</b>\nPlease use a different name.", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    
    from bson.objectid import ObjectId
    col_ig_content.update_one(
        {"_id": ObjectId(data['content_id'])},
        {"$set": {"name": new_name}}
    )
    
    await state.clear()
    await message.answer(
        f"✅ <b>IG Content Updated!</b>\n\n"
        f"🆔 Code: {data['cc_code']}\n"
        f"📝 New Name: {new_name}",
        reply_markup=get_ig_menu(),
        parse_mode="HTML"
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
        msg = "❌ <b>No Content Found</b>"
        if not_found:
             msg += "\nNot found: " + ", ".join(not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
        
    # Store IDs
    delete_ids = [str(c["_id"]) for c in found_contents]
    
    await state.update_data(delete_ids=delete_ids)
    
    # Confirmation Message
    msg = f"⚠️ <b>CONFIRM BULK DELETION ({len(found_contents)})</b> ⚠️\n\n"
    for c in found_contents:
        msg += f"• <b>{c['cc_code']}</b> - {c['name']}\n"
        
    if not_found:
        msg += f"\n⚠️ Skipped: {', '.join(not_found)}\n"
        
    msg += "\n<b>Are you sure?</b>"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
    ], resize_keyboard=True)
    await state.set_state(IGDeleteStates.waiting_for_confirm)
    await message.answer(msg, reply_markup=kb, parse_mode="HTML")

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
                f"🗑️ <b>Deleted {count} IG Content(s)!</b>",
                reply_markup=get_ig_menu(),
                parse_mode="HTML"
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
        return await message.answer("📸 <b>IG MANAGEMENT</b>", reply_markup=get_ig_menu(), parse_mode="HTML")
    
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
    text = f"✅ <b>VIEWING IG CONTENT</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"{display_index}. {content['cc_code']}\n\n"
    text += f"📝 <b>Full Content:</b>\n{content['name']}\n"
    text += "━━━━━━━━━━━━━━━━━━━━"
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⬅️ BACK TO LIST")],
        [KeyboardButton(text="⬅️ BACK TO IG MENU")]
    ], resize_keyboard=True)
    
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

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
    await message.answer("📸 <b>IG MANAGEMENT</b>", reply_markup=get_ig_menu(), parse_mode="HTML")


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
    await message.answer("📋 <b>SELECT VIEW:</b>", reply_markup=kb, parse_mode="HTML")

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
    await message.answer("📋 <b>Main Menu</b>", reply_markup=get_main_menu(message.from_user.id), parse_mode="HTML")


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
    
    text = f"📚 <b>PDF DATA</b> (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for pdf in pdfs:
        text += f"{pdf['index']}. <b>{pdf['name']}</b>\n"
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
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

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
    
    text = f"📸 <b>IG CONTENT</b> (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    
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
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

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
    text = message.text or ""

    # Pagination buttons
    if text.startswith("⬅️ PREV_IGCC") or text.startswith("➡️ NEXT_IGCC"):
        try:
            page = int(text.split()[-1]) - 1
            await send_all_ig_view(message, page=page)
        except:
            await send_all_ig_view(message, page=0)
        return

    # BACK button — go to list selection menu
    if text == "⬅️ BACK":
        await state.clear()
        await comprehensive_list_handler(message, state)
        return

    # ─── Full Detail View: index number or CC code ───────────────────────
    query = text.strip()
    if not query:
        return

    content = None
    display_index = None
    all_contents = list(col_ig_content.find().sort("cc_number", 1))

    if query.isdigit():
        idx = int(query) - 1
        if 0 <= idx < len(all_contents):
            content = all_contents[idx]
            display_index = int(query)
    elif query.upper().startswith("CC"):
        content = col_ig_content.find_one({"cc_code": {"$regex": f"^{query}$", "$options": "i"}})
        if content:
            for i, c in enumerate(all_contents, start=1):
                if c["_id"] == content["_id"]:
                    display_index = i
                    break

    if not content:
        await message.answer(
            "❌ <b>Not Found</b>\n\nSend an index number (e.g. `3`) or CC code (e.g. `CC3`) to view full details.\nOr press ⬅️ BACK.",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="⬅️ BACK")]],
                resize_keyboard=True
            )
        )
        return

    # Format full detail
    name = content.get("name", "N/A")
    aff_link = content.get("affiliate_link", "Not Set")
    start_code = content.get("start_code", "Not Set")
    ig_cc_clicks = content.get("ig_cc_clicks", 0)
    created_at = content.get("created_at")
    last_click = content.get("last_ig_cc_click")
    cc_number = content.get("cc_number", "?")

    created_str = created_at.strftime("%b %d, %Y  %I:%M %p") if isinstance(created_at, datetime) else str(created_at) if created_at else "N/A"
    last_click_str = last_click.strftime("%b %d, %Y  %I:%M %p") if isinstance(last_click, datetime) else str(last_click) if last_click else "Never"

    detail = (
        f"📸 <b>IG CONTENT — FULL DETAIL</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>#{display_index}  {content['cc_code']}</b>\n\n"
        f"📝 <b>Name / Content:</b>\n{name}\n\n"
        f"💸 <b>Affiliate Link:</b>\n{aff_link}\n\n"
        f"🔗 <b>Start Code:</b> `{start_code}`\n"
        f"📊 <b>IG CC Clicks:</b> `{ig_cc_clicks:,}`\n"
        f"🔢 <b>CC Number:</b> `{cc_number}`\n\n"
        f"📅 <b>Created:</b> {created_str}\n"
        f"🖱️ <b>Last Click:</b> {last_click_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⬅️ BACK TO IG LIST")],
        [KeyboardButton(text="⬅️ BACK")]
    ], resize_keyboard=True)
    await message.answer(detail, parse_mode="HTML", reply_markup=kb)

@dp.message(ListStates.viewing_ig, F.text == "⬅️ BACK TO IG LIST")
async def return_to_ig_list_from_detail(message: types.Message, state: FSMContext):
    """Return to IG content list from detail view"""
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
    await message.answer("🔍 <b>SELECT SEARCH TYPE:</b>", reply_markup=kb, parse_mode="HTML")

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
    
    text = f"📚 <b>AVAILABLE PDFs</b> (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    for pdf in pdfs:
        text += f"{pdf['index']}. {pdf['name']}\n"
        text += f"🔗 {pdf['link']}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter PDF Index or Name:</b>"
    
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
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

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
    creation_time = pdf.get('created_at', now_local())
    time_12h = creation_time.strftime("%I:%M %p")
    date_str = creation_time.strftime("%A, %B %d, %Y")
    
    # Build detailed info
    text = f"📄 <b>PDF DETAILS</b>\n━━━━━━━━━━━━━━━━━━━━\n"
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
    text += "\n\n━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter another PDF Index or Name to Search:</b>"
    
    # Auto-split if message exceeds Telegram's 4096 character limit
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for idx, part in enumerate(parts):
            if idx == len(parts) - 1:  # Last part gets the keyboard
                await message.answer(part, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            else:
                await message.answer(part, parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=get_cancel_keyboard(), parse_mode="HTML")

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
    
    text = f"📸 <b>AVAILABLE IG CONTENT</b> (Page {page+1})\nTotal: {total}\n━━━━━━━━━━━━━━━━━━━━\n"
    for idx, content in enumerate(contents, start=1):
        display_idx = skip + idx
        text += f"{display_idx}. {content['cc_code']}\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter Index or CC Code:</b>"
    
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
    
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

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
    creation_time = content.get('created_at', now_local())
    time_12h = creation_time.strftime("%I:%M %p")
    date_str = creation_time.strftime("%A, %B %d, %Y")
    
    # Build detailed info
    text = f"📸 <b>IG CONTENT DETAILS</b>\n━━━━━━━━━━━━━━━━━━━━\n"
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
    text += "\n\n━━━━━━━━━━━━━━━━━━━━\n⌨️ <b>Enter another Index or CC Code to Search:</b>"
    
    # Auto-split if message exceeds Telegram's 4096 character limit
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for idx, part in enumerate(parts):
            if idx == len(parts) - 1:  # Last part gets the keyboard
                await message.answer(part, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
            else:
                await message.answer(part, parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=get_cancel_keyboard(), parse_mode="HTML")


@dp.message(F.text == "🩺 DIAGNOSIS")
async def diagnosis_handler(message: types.Message):
    if not await check_authorization(message, "System Diagnosis", "can_view_analytics"):
        return
    """Comprehensive System Health Check & Diagnosis"""
    status_msg = await message.answer("🔍 <b>Running Complete System Diagnosis...</b>\n\n⏳ This may take a moment...", parse_mode="HTML")
    
    issues = []
    warnings = []
    checks_passed = 0
    total_checks = 0
    
    # --- 1. DATABASE CONNECTION CHECK ---
    total_checks += 1
    try:
        start_t = now_local()
        client.admin.command('ping')
        ping_ms = (now_local() - start_t).microseconds / 1000
        
        if ping_ms > 100:
            warnings.append(f"⚠️ Database latency high: {ping_ms:.1f}ms (>100ms)")
        else:
            checks_passed += 1
            
        # Test write operation
        test_doc = {"test": True, "timestamp": now_local()}
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
            # Auto-create missing indexes
            for idx in missing_indexes:
                field = idx.replace('_1', '')
                try:
                    col_pdfs.create_index([(field, 1)])
                except:
                    pass
            warnings.append(f"ℹ️ Auto-rebuilt missing indexes: {', '.join(missing_indexes)}")
            checks_passed += 1
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

    # --- 15. ADMIN STATUS CHECK ---
    total_checks += 1
    try:
        total_admins = col_admins.count_documents({})
        active_admins = col_admins.count_documents({"is_locked": False})
        locked_admins = col_admins.count_documents({"is_locked": True})
        if total_admins == 0:
            warnings.append("⚠️ No admins configured (only master admin has access)")
        else:
            checks_passed += 1
        # summarize in report as info
        extra_admin_info = f"👥 Admins: {total_admins} total — {active_admins} active, {locked_admins} locked"
        warnings.append(f"ℹ️ {extra_admin_info}")  # informational, not a warning
    except Exception as e:
        warnings.append(f"⚠️ Admin Check: {str(e)}")

    # --- 16. BACKUP HEALTH CHECK ---
    total_checks += 1
    try:
        last_backup = col_backups.find_one(sort=[("created_at", -1)])
        if last_backup:
            last_bk_time = last_backup.get("created_at")
            if last_bk_time:
                delta = now_local() - last_bk_time
                days_ago = delta.days
                if days_ago > 35:
                    warnings.append(f"⚠️ Last backup was {days_ago} days ago — consider creating a new backup")
                else:
                    checks_passed += 1
                bk_time_str = last_bk_time.strftime("%b %d, %Y  %I:%M %p") if hasattr(last_bk_time, 'strftime') else str(last_bk_time)
                warnings.append(f"ℹ️ Last backup: {bk_time_str} "
                                 f"({last_backup.get('filename', 'unknown')}, "
                                 f"{last_backup.get('file_size_mb', 0):.2f} MB)")
            else:
                warnings.append("⚠️ Last backup has no timestamp")
        else:
            warnings.append("⚠️ No backups found — create a backup via 💾 BACKUP DATA")
    except Exception as e:
        warnings.append(f"⚠️ Backup Health Check: {str(e)}")

    # --- 17. USER SOURCE TRACKING CHECK ---
    total_checks += 1
    try:
        tracking_col = db["bot10_user_tracking"]
        tracked_total = tracking_col.count_documents({})
        with_source = tracking_col.count_documents({"source": {"$exists": True}})
        dedup_col = db["bot9_user_activity"]
        dedup_records = dedup_col.count_documents({})

        source_pipeline = [
            {"$group": {"_id": "$source", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        source_dist = {doc["_id"]: doc["count"] for doc in tracking_col.aggregate(source_pipeline)}
        src_summary = ", ".join([f"{k}: {v}" for k, v in source_dist.items()]) if source_dist else "none"

        checks_passed += 1
        warnings.append(
            f"ℹ️ Source tracking: {tracked_total} users tracked, {with_source} with source locked. "
            f"Distribution: [{src_summary}]. Dedup records: {dedup_records:,}"
        )
    except Exception as e:
        warnings.append(f"⚠️ Source Tracking Check: {str(e)}")

    
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
🩺 <b>SYSTEM DIAGNOSIS REPORT</b>
━━━━━━━━━━━━━━━━━━━━━━━━

{status_emoji} <b>HEALTH STATUS: {status_text}</b>
{status_msg_text}

<b>📊 CHECKS SUMMARY</b>
• Total Checks: `{total_checks}`
• Passed: `{checks_passed}` ✅
• Warnings: `{len(warnings)}` ⚠️
• Critical: `{len(issues)}` ❌

<b>🎯 HEALTH SCORE</b>
{status_emoji} <b>{health_score:.1f}%</b>
"""

    # Add critical issues
    if issues:
        report += "\n<b>❌ CRITICAL ISSUES:</b>\n"
        for issue in issues:
            report += f"{issue}\n"
    
    # Add warnings
    if warnings:
        report += "\n<b>⚠️ WARNINGS:</b>\n"
        for warning in warnings[:5]:  # Limit to 5
            report += f"{warning}\n"
        if len(warnings) > 5:
            report += f"_...and {len(warnings) - 5} more warnings_\n"
    
    # Add all clear message
    if not issues and not warnings:
        report += "\n<b>✅ ALL CHECKS PASSED</b>\n"
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
        report += "\n🎉 <b>System is running flawlessly!</b>\n"
    
    # Add recommendations
    # Add recommendations (ignoring informational ℹ️ messages)
    real_warnings = [w for w in warnings if not str(w).startswith("ℹ️")]
    recs = ""
    if len(issues) > 0:
        recs += "• Address critical issues immediately\n"
    if any("duplicate" in str(w).lower() for w in real_warnings + issues):
        recs += "• Run duplicate cleanup\n"
    if any("storage" in str(w).lower() or "database" in str(w).lower() for w in real_warnings):
        recs += "• Consider archiving old data\n"
    if any("log" in str(w).lower() for w in real_warnings):
        recs += "• Rotate log files\n"
    if any("index" in str(w).lower() and "rebuilt" not in str(w).lower() for w in real_warnings):
        recs += "• Rebuild database indexes\n"

    if recs:
        report += "\n<b>💡 RECOMMENDATIONS:</b>\n" + recs
    
    report += f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"_Diagnostic completed at {now_local().strftime('%I:%M:%S %p')}_"
    
    await status_msg.edit_text(report, parse_mode="HTML")
    log_user_action(message.from_user, "Ran System Diagnosis", f"Score: {health_score:.1f}%")

def get_recent_logs(lines_count=30):
    """Refactored log reader"""
    # Logs are written to logs/bot9.log by the RotatingFileHandler
    log_file = "logs/bot9.log"
    if not os.path.exists(log_file):
        # Fallback: try Render's stdout capture via /proc/1/fd/1 is not readable,
        # so we read from the rotating log file only.
        return "⚠️ No logs found yet. (Log file not created - bot may have just started)"
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            content = "".join(lines[-lines_count:])
            if len(content) > 3500:
                content = content[-3500:]
                content = "..." + content
            import html
            return html.escape(content) if content.strip() else "No recent logs."
    except Exception as e:
        return f"Error reading logs: {e}"

@dp.message(F.text == "🖥️ TERMINAL")
async def terminal_handler(message: types.Message):
    if not await check_authorization(message, "Terminal", "can_view_analytics"):
        return
    log_user_action(message.from_user, "Viewed Terminal")
    logs = get_recent_logs(lines_count=40)
    text = f"🖥️ <b>LIVE TERMINAL OUTPUT</b>\n━━━━━━━━━━━━━━━━━━━━\n<pre><code class=\"language-python\">{logs}</code></pre>\n━━━━━━━━━━━━━━━━━━━━"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 REFRESH", callback_data="refresh_terminal")]])
    await message.answer(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data == "refresh_terminal")
async def refresh_terminal_callback(callback: types.CallbackQuery):
    # Use check_authorization_user so we check the human who clicked (callback.from_user),
    # NOT callback.message.from_user which points to the bot itself.
    if not await check_authorization_user(callback.from_user, callback.message, "Refresh Terminal", "can_view_analytics"):
         await callback.answer("⛔ Access Denied", show_alert=True)
         return
    logs = get_recent_logs(lines_count=40)
    text = f"🖥️ <b>LIVE TERMINAL OUTPUT</b>\n━━━━━━━━━━━━━━━━━━━━\n<pre><code class=\"language-python\">{logs}</code></pre>\n━━━━━━━━━━━━━━━━━━━━"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 REFRESH", callback_data="refresh_terminal")]])
    
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass # Content identical or message not modified
    
    await callback.answer("Refreshing logs...")

GUIDE_PAGES = [
    # ── PAGE 1 ── Overview + Main Menu buttons
    (
        "📚 <b>BOT 9 — COMPLETE GUIDE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📄 *Page 1 / 4 — Overview & Main Menu*\n\n"

        "🤖 <b>WHAT IS BOT 9?</b>\n"
        "Bot 9 is the <b>content management & analytics hub</b>.\n"
        "It stores PDFs, IG content, affiliate links, YT links, and\n"
        "generates unique tracking links for Bot 8 users.\n\n"

        "🏠 <b>MAIN MENU BUTTONS</b>\n"
        "┌─────────────────────────────\n"
        "│ 📋 LIST       — Browse all stored content\n"
        "│ ➕ ADD        — Add new content (PDF/IG/YT/Code)\n"
        "│ 🔍 SEARCH     — Search content by keyword/code\n"
        "│ 🔗 LINKS      — Generate & view tracking links\n"
        "│ 📊 ANALYTICS  — View click stats & performance\n"
        "│ 🩺 DIAGNOSIS  — System health & DB diagnostics\n"
        "│ 🖥️ TERMINAL    — Run shell commands (Master only)\n"
        "│ 💾 BACKUP DATA — Export/backup the database\n"
        "│ 👥 ADMINS     — Manage admin accounts\n"
        "│ ⚠️ RESET BOT DATA — Wipe data (Master only)\n"
        "│ 📚 BOT GUIDE  — This guide\n"
        "└─────────────────────────────\n\n"

        "🔐 <b>ACCESS LEVELS</b>\n"
        "• <b>Master Admin</b> — Full access to all features\n"
        "• <b>Admin</b> — Access based on assigned permissions\n"
        "• <b>Unauthorized</b> — Blocked, only sees Bot Guide\n\n"

        "⬇️ *Use the buttons below to navigate pages*"
    ),

    # ── PAGE 2 ── ADD / LIST / SEARCH / LINKS
    (
        "📚 <b>BOT 9 — COMPLETE GUIDE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📄 *Page 2 / 4 — Content Management*\n\n"

        "➕ <b>ADD MENU</b> *(Add new content)*\n"
        "├ 📄 <b>PDF</b> — Add / Edit / Delete / List PDFs\n"
        "│   └ Each PDF gets a unique link for Bot 8 users\n"
        "│   └ Supports: name, link, MSA code, IG code, YT link\n"
        "├ 💸 <b>AFFILIATE</b> — Manage affiliate links per PDF\n"
        "│   └ Add / Edit / Delete / List affiliate links\n"
        "│   └ Tracks affiliate clicks separately\n"
        "├ 🔑 <b>CODE</b> — YT Code management\n"
        "│   └ Add / Edit / Delete / List YT access codes\n"
        "│   └ Used for YTCODE tracking links\n"
        "├ ▶️ <b>YT</b> — YouTube link management\n"
        "│   └ Add / Edit / Delete / List YT links\n"
        "│   └ Links YT content to PDFs for tracking\n"
        "└ 📸 <b>IG</b> — Instagram content management\n"
        "    └ Add / Edit / Delete / List IG content\n"
        "    └ Supports IG CC codes & click tracking\n\n"

        "📋 <b>LIST MENU</b> *(Browse stored content)*\n"
        "├ 📚 ALL      — Show all PDFs with full details\n"
        "├ 📸 IG CONTENT — Show all IG content\n"
        "└ Paginated with ⬅️ PREV / NEXT ➡️ buttons\n\n"

        "🔍 <b>SEARCH MENU</b> *(Find content fast)*\n"
        "├ 🔍 SEARCH PDF    — Search PDFs by name/code\n"
        "└ 🔍 SEARCH IG CC  — Search IG content by code\n\n"

        "🔗 <b>LINKS MENU</b> *(Generate tracking links)*\n"
        "├ 🏠 HOME YT   — YT homepage tracking link\n"
        "├ 📑 ALL PDF   — Direct PDF tracking links\n"
        "├ 📸 IG CC     — IG CC tracking links\n"
        "└ All links auto-route users through Bot 8"
    ),

    # ── PAGE 3 ── Analytics / Diagnosis / Backup / Terminal
    (
        "📚 <b>BOT 9 — COMPLETE GUIDE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📄 *Page 3 / 4 — Analytics, Diagnosis & Tools*\n\n"

        "📊 <b>ANALYTICS MENU</b> *(Click tracking & stats)*\n"
        "├ 📊 OVERVIEW         — Full dashboard: total clicks,\n"
        "│                        top performers, content counts\n"
        "├ 📄 PDF Clicks        — Per-PDF click breakdown\n"
        "├ 💸 Affiliate Clicks  — Per-affiliate click stats\n"
        "├ 📸 IG Start Clicks   — IG start link clicks\n"
        "├ ▶️ YT Start Clicks   — YT start link clicks\n"
        "├ 📸 IG CC Start Clicks— IG CC link clicks\n"
        "└ 🔑 YT Code Clicks   — YT Code link clicks\n\n"

        "🩺 <b>DIAGNOSIS MENU</b> *(System health checks)*\n"
        "├ Checks MongoDB connection & collection sizes\n"
        "├ Detects orphaned records & broken references\n"
        "├ Reports missing MSA codes, empty fields\n"
        "└ Validates PDF links & IG content integrity\n\n"

        "💾 <b>BACKUP MENU</b> *(Data safety tools)*\n"
        "├ 💾 FULL BACKUP      — Export entire DB to JSON file\n"
        "├ 📋 VIEW AS JSON     — Preview backup in chat\n"
        "├ 📊 BACKUP STATS     — Show DB collection sizes\n"
        "└ 📜 BACKUP HISTORY   — View past backup records\n\n"

        "🖥️ <b>TERMINAL</b> *(Master Admin only)*\n"
        "├ Run any shell command directly from Telegram\n"
        "├ Output streamed back to chat\n"
        "└ Use with caution — no restrictions applied\n\n"

        "⚠️ <b>RESET BOT DATA</b> *(Master Admin only)*\n"
        "└ Wipes selected collections — irreversible!\n"
        "   Requires double confirmation before executing"
    ),

    # ── PAGE 4 ── Admins / Permissions / Ban / Roles
    (
        "📚 <b>BOT 9 — COMPLETE GUIDE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📄 *Page 4 / 4 — Admin & Permission System*\n\n"

        "👥 <b>ADMINS MENU</b> *(Manage admin accounts)*\n"
        "├ ➕ NEW ADMIN     — Add a new admin by user ID\n"
        "├ ➖ REMOVE ADMIN  — Remove an admin\n"
        "├ 📋 LIST ADMINS   — Show all admins with roles\n"
        "├ 🔐 PERMISSIONS   — Set per-admin permissions\n"
        "├ 👔 ROLES         — Assign role presets\n"
        "├ 🔒 LOCK/UNLOCK   — Temporarily disable an admin\n"
        "└ 🚫 BAN CONFIG    — Ban/unban users from Bot 8\n\n"

        "🔐 <b>PERMISSION FLAGS</b> *(Per-admin access control)*\n"
        "├ can_list         — View content lists\n"
        "├ can_add          — Add/edit/delete content\n"
        "├ can_search       — Use search feature\n"
        "├ can_links        — Access link generator\n"
        "├ can_analytics    — View analytics data\n"
        "├ can_diagnosis    — Run system diagnostics\n"
        "├ can_terminal     — Use terminal (⚠️ powerful)\n"
        "├ can_backup       — Access backup tools\n"
        "├ can_manage_admins— Add/remove other admins\n"
        "└ can_reset        — Reset bot data (⚠️ dangerous)\n\n"

        "🚫 <b>BAN SYSTEM</b>\n"
        "├ 🚫 BAN USER    — Block a user from Bot 8\n"
        "├ ✅ UNBAN USER  — Remove a ban\n"
        "└ 📋 LIST BANNED — See all currently banned users\n\n"

        "📎 <b>ADD AFFILIATE</b> *(Quick inline affiliate tool)*\n"
        "└ Shortcut to attach affiliate links to PDFs\n\n"

        "💡 <b>TIPS</b>\n"
        "• All actions are logged to console\n"
        "• Unauthorized access is auto-blocked & logged\n"
        "• Bot 9 feeds content to Bot 8 in real-time\n"
        "• Back buttons always available to navigate safely"
    ),
]

def get_guide_nav_keyboard(page: int) -> ReplyKeyboardMarkup:
    """Navigation keyboard for bot guide pages"""
    total = len(GUIDE_PAGES)
    row = []
    if page > 0:
        row.append(KeyboardButton(text=f"⬅️ GUIDE PREV"))
    if page < total - 1:
        row.append(KeyboardButton(text=f"GUIDE NEXT ➡️"))
    keyboard = []
    if row:
        keyboard.append(row)
    keyboard.append([KeyboardButton(text="🏠 MAIN MENU")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

@dp.message(F.text == "📚 BOT GUIDE")
async def guide_handler(message: types.Message, state: FSMContext):
    await state.update_data(guide_page=0)
    await message.answer(
        GUIDE_PAGES[0],
        reply_markup=get_guide_nav_keyboard(0),
        parse_mode="HTML"
    )

@dp.message(F.text == "GUIDE NEXT ➡️")
async def guide_next_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("guide_page", 0)
    page = min(page + 1, len(GUIDE_PAGES) - 1)
    await state.update_data(guide_page=page)
    await message.answer(
        GUIDE_PAGES[page],
        reply_markup=get_guide_nav_keyboard(page),
        parse_mode="HTML"
    )

@dp.message(F.text == "⬅️ GUIDE PREV")
async def guide_prev_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = data.get("guide_page", 0)
    page = max(page - 1, 0)
    await state.update_data(guide_page=page)
    await message.answer(
        GUIDE_PAGES[page],
        reply_markup=get_guide_nav_keyboard(page),
        parse_mode="HTML"
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
        "📊 <b>ANALYTICS DASHBOARD</b>\n\n"
        "Select a category to view detailed analytics:",
        reply_markup=get_analytics_menu(),
        parse_mode="HTML"
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
    
    # ── Source tracking from bot10_user_tracking (permanent first-source lock) ──
    try:
        tracking_col = db["bot10_user_tracking"]
        source_pipeline = [
            {"$group": {"_id": "$source", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        source_counts = {doc["_id"]: doc["count"] for doc in tracking_col.aggregate(source_pipeline)}
        total_tracked_users = tracking_col.count_documents({})
        src_ig      = source_counts.get("IG", 0)
        src_yt      = source_counts.get("YT", 0)
        src_igcc    = source_counts.get("IGCC", 0)
        src_ytcode  = source_counts.get("YTCODE", 0)
        src_other   = total_tracked_users - src_ig - src_yt - src_igcc - src_ytcode
    except Exception:
        total_tracked_users = src_ig = src_yt = src_igcc = src_ytcode = src_other = 0

    # Build overview message
    text = "📊 <b>ANALYTICS OVERVIEW</b>\n"
    text += "═══════════════════════\n\n"
    
    text += f"📈 <b>TOTAL CLICKS:</b> {total_clicks:,}\n\n"
    
    text += "<b>📊 Clicks by Category:</b>\n"
    text += f"├ 📄 PDFs: {pdf_clicks:,}\n"
    text += f"├ 💸 Affiliates: {aff_clicks:,}\n"
    text += f"├ 📸 IG Start: {ig_clicks:,}\n"
    text += f"├ ▶️ YT Start: {yt_clicks:,}\n"
    text += f"├ 📸 IG CC: {ig_cc_clicks:,}\n"
    text += f"└ 🔑 YT Code: {yt_code_clicks:,}\n\n"

    text += "<b>📡 TRAFFIC SOURCES (Unique Users — Permanently Locked):</b>\n"
    text += f"├ 👥 Total Tracked Users: {total_tracked_users:,}\n"
    text += f"├ 📸 IG Start: {src_ig:,} users\n"
    text += f"├ ▶️ YT Start: {src_yt:,} users\n"
    text += f"├ 📸 IG CC: {src_igcc:,} users\n"
    text += f"├ 🔑 YT Code: {src_ytcode:,} users\n"
    if src_other > 0:
        text += f"└ ❓ Other: {src_other:,} users\n\n"
    else:
        text += f"└ _(Each user's source is locked on first click — never changes)_\n\n"

    text += "<b>📚 Content Library:</b>\n"
    text += f"├ Total PDFs: {total_pdfs}\n"
    text += f"├ IG Content: {total_ig_content}\n"
    text += f"├ With Affiliates: {pdfs_with_affiliate}\n"
    text += f"├ With IG Codes: {pdfs_with_ig}\n"
    text += f"├ With YT Links: {pdfs_with_yt}\n"
    text += f"└ With MSA Codes: {pdfs_with_msa}\n\n"
    
    if top_5:
        text += "🏆 <b>TOP 5 PERFORMERS:</b>\n"
        for idx, item in enumerate(top_5, 1):
            text += f"{idx}. {item['type']} <b>{item['name']}</b> - {item['clicks']:,} clicks\n"
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
        text += f"✅ <b>Setup Completion:</b> {completion_rate:.1f}% ({complete_pdfs}/{total_pdfs} fully configured)\n"
    
    text += "\n═══════════════════════\n"
    text += "💡 Select a category below for detailed analytics."
    
    await message.answer(
        text,
        reply_markup=get_analytics_menu(),
        parse_mode="HTML"
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
            parse_mode="HTML"
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
        # For IG CC: show only cc_code (not full content text)
        if category == "ig_cc_start":
            item_name = item.get("cc_code", "Unknown")
        else:
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
        
        text += f"{idx}. {indicator} <b>{item_name}</b>\n"
        text += f"   🔢 Clicks: <b>{clicks:,}</b>"
        
        if last_clicked:
            from datetime import datetime, timedelta
            now = now_local()
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

    reply_kb = ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

    if len(text) > 4000:
        # Split into chunks, last chunk gets keyboard
        parts = []
        chunk = ""
        for line in text.split("\n"):
            if len(chunk) + len(line) + 1 > 4000:
                parts.append(chunk)
                chunk = line + "\n"
            else:
                chunk += line + "\n"
        if chunk:
            parts.append(chunk)
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                await message.answer(part, reply_markup=reply_kb, parse_mode="HTML")
            else:
                await message.answer(part, parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=reply_kb, parse_mode="HTML")

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
        "📊 <b>ANALYTICS DASHBOARD</b>",
        reply_markup=get_analytics_menu(),
        parse_mode="HTML"
    )

@dp.message(F.text == "🆔 MSA ID POOL")
async def msa_id_pool_handler(message: types.Message):
    """Show MSA Node ID pool usage with progress bar"""
    if not await check_authorization(message, "MSA ID Pool", "can_view_analytics"):
        return
    try:
        # MSA IDs live in MSANODEDATA db (shared with bot8/bot10)
        msa_col = client["MSANODEDATA"]["msa_ids"]
        total_allocated = msa_col.count_documents({})
        total_retired = msa_col.count_documents({"retired": True})
        active_members = total_allocated - total_retired

        TOTAL_POOL = 900_000_000  # 100,000,000 – 999,999,999
        available = TOTAL_POOL - total_allocated
        pct_used = total_allocated / TOTAL_POOL * 100
        filled = round(pct_used / 5)  # 20-block bar (each block = 5%)
        bar = "█" * filled + "░" * (20 - filled)

        if pct_used > 90:
            risk = "🔴 CRITICAL"
        elif pct_used > 50:
            risk = "🟠 HIGH"
        elif pct_used > 20:
            risk = "🟡 MODERATE"
        else:
            risk = "🟢 ABUNDANT"

        text = (
            "🆔 <b>MSA NODE ID POOL STATUS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 <b>Total Pool:</b> 900,000,000 IDs\n"
            f"✅ <b>Active Members:</b> {active_members:,}\n"
            f"🗄️ <b>Retired IDs (reserved):</b> {total_retired:,}\n"
            f"🔢 <b>Total Used (active+retired):</b> {total_allocated:,}\n"
            f"🟢 <b>Available:</b> {available:,}\n\n"
            f"📈 <b>Usage Bar:</b>\n`[{bar}]`\n"
            f"`{pct_used:.6f}%` used \u2014 {risk}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 {now_local().strftime('%B %d, %Y  %I:%M:%S %p')}"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=get_analytics_menu())
    except Exception as e:
        await message.answer(
            f"❌ MSA Pool check failed: `{str(e)[:150]}`",
            parse_mode="HTML",
            reply_markup=get_analytics_menu()
        )

# --- DATA RESET HANDLER ---
@dp.message(F.text == "⚠️ RESET BOT DATA")
async def start_reset_data(message: types.Message, state: FSMContext):
    # Security Check — Master Admin only
    if message.from_user.id != MASTER_ADMIN_ID:
        await message.answer("⛔ <b>ACCESS DENIED.</b> Only the Master Admin can perform this action.", parse_mode="HTML")
        return

    await state.set_state(ResetStates.waiting_for_confirm_button)
    
    keyboard = [
        [KeyboardButton(text="🔴 CONFIRM RESET")],
        [KeyboardButton(text="❌ CANCEL")]
    ]
    await message.answer(
        "⚠️ <b>DANGER ZONE — FULL SYSTEM WIPE</b> ⚠️\n\n"
        "You have requested to <b>RESET ALL BOT DATA</b>.\n\n"
        "This will permanently delete <b>EVERY SINGLE THING</b> from the database:\n"
        "• All PDFs and Links\n"
        "• All IG Content\n"
        "• All Logs and Settings\n"
        "• All Admins (except your master account)\n"
        "• All Banned Users\n"
        "• All User Activity & Click Dedup Records\n"
        "• All Backup Records\n\n"
        "🔴 <b>THIS ACTION CANNOT BE UNDONE.</b>\n\n"
        "<b>STEP 1 OF 2 — Click the button to proceed:</b>",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="HTML"
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
            "🛑 <b>STEP 2 OF 2 — FINAL CONFIRMATION</b> 🛑\n\n"
            "To execute the <b>COMPLETE SYSTEM WIPE</b>, type the word below exactly:\n\n"
            "```\nCONFIRM\n```\n\n"
            "Any other input (or ❌ CANCEL) will abort the operation.",
            reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
            parse_mode="HTML"
        )
    else:
        await message.answer("⚠️ Please select an option.")

@dp.message(ResetStates.waiting_for_confirm_text)
async def process_reset_final(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("✅ Reset Cancelled.", reply_markup=get_main_menu(message.from_user.id))
        
    if message.text.upper() == "CONFIRM":
        msg = await message.answer("🧨 <b>INITIATING COMPLETE SYSTEM WIPE...</b>", reply_markup=types.ReplyKeyboardRemove(), parse_mode="HTML")
        
        try:
            # 1. Drop ALL bot9 collections
            collections_to_wipe = [
                "bot9_pdfs",
                "bot9_ig_content",
                "bot9_logs",
                "bot9_settings",
                "bot9_backups",
                "bot9_admins",
                "bot9_banned_users",
                "bot9_user_activity",
                "bot9_state",
            ]
            wiped = []
            for coll_name in collections_to_wipe:
                db.drop_collection(coll_name)
                wiped.append(coll_name)

            # 2. Truncate log file if exists
            for log_file in ["bot9.log", "logs/bot9.log"]:
                if os.path.exists(log_file):
                    with open(log_file, "w"):
                        pass

            # 3. Delete local backup files
            backup_dir = "backups"
            if os.path.exists(backup_dir):
                import shutil
                shutil.rmtree(backup_dir)

            # 4. Re-seed master admin so the bot stays usable
            try:
                col_admins.insert_one({
                    "user_id": MASTER_ADMIN_ID,
                    "is_owner": True,
                    "is_locked": False,
                    "permissions": list(PERMISSIONS.keys()),
                    "full_name": "Master Admin",
                    "username": "owner",
                    "added_at": now_local(),
                })
            except Exception:
                pass  # Already exists is fine

            wiped_str = "\n".join([f"• `{c}`" for c in wiped])
            await message.answer(
                f"✅ <b>SYSTEM RESET COMPLETE</b>\n\n"
                f"🗑 <b>Wiped collections:</b>\n{wiped_str}\n\n"
                f"🔄 Master Admin account re-seeded.\n"
                f"🤖 System is clean and ready.",
                reply_markup=get_main_menu(message.from_user.id),
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"❌ <b>RESET FAILED:</b> `{e}`", reply_markup=get_main_menu(message.from_user.id), parse_mode="HTML")
            
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
        "📎 <b>IG AFFILIATE MANAGEMENT</b>\n\nSelect an option:",
        reply_markup=get_ig_affiliate_menu(),
        parse_mode="HTML"
    )

# Back button from affiliate submenu to IG menu
@dp.message(F.text == "◀️ Back")
async def ig_affiliate_back_handler(message: types.Message, state: FSMContext):
    """Return from affiliate menu to IG menu"""
    await state.clear()
    await message.answer("📸 <b>IG CODE MANAGEMENT</b>", reply_markup=get_ig_menu(), parse_mode="HTML")

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
        msg = "❌ <b>No Content Found</b>"
        if not_found:
             msg += "\nNot found: " + ", ".join(not_found)
        await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        return
    
    # Store IDs
    affiliate_ids = [str(c["_id"]) for c in found_contents]
    
    await state.update_data(affiliate_ids=affiliate_ids)
    await state.set_state(IGAffiliateStates.waiting_for_link)
    
    msg = f"✅ <b>Selected {len(found_contents)} IG items for Affiliate Link:</b>\n\n"
    for c in found_contents:
        msg += f"• {c['cc_code']} - {c['name']}\n"
    
    msg += "\n🔗 <b>Enter Affiliate Link (applies to ALL above):</b>"
    
    await message.answer(msg, reply_markup=get_cancel_keyboard(), parse_mode="HTML")

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
            f"✅ <b>Bulk Affiliate Link Applied!</b>\n\n"
            f"🔗 Link: `{link}`\n"
            f"📊 Applied to {len(affiliate_ids)} items.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
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
            "⚠️ <b>No affiliate links found!</b>\n\nAdd an affiliate link first.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
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
        f"✅ <b>Selected:</b> {content['cc_code']} - {content['name']}\n\n"
        f"📎 Current Link: {content.get('affiliate_link', 'N/A')}\n\n"
        f"🔗 <b>Enter New Affiliate Link:</b>",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
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
        await message.answer("⚠️ <b>Link is identical to current link.</b>\nNo changes made.", reply_markup=get_ig_affiliate_menu(), parse_mode="HTML")
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
        f"✅ <b>Affiliate Link Updated!</b>\n\n"
        f"🆔 Code: {data['cc_code']}\n"
        f"📝 Content: {data['name']}\n"
        f"🔗 New Link: {link}",
        reply_markup=get_ig_affiliate_menu(),
        parse_mode="HTML"
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
            "⚠️ <b>No affiliate links found!</b>\n\nNothing to delete.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
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
        f"⚠️ <b>CONFIRM DELETE AFFILIATE</b>\n\n"
        f"🆔 Code: {content['cc_code']}\n"
        f"📝 Content: {content['name']}\n"
        f"🔗 Link: {content.get('affiliate_link', '')}\n\n"
        f"Are you sure?",
        reply_markup=confirm_kb,
        parse_mode="HTML"
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
            f"🗑️ <b>Affiliate Link Deleted!</b>\n\n"
            f"🆔 Code: {data['cc_code']}\n"
            f"📝 Content: {data['name']}",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
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
            "⚠️ <b>No affiliate links found!</b>\n\nAdd an affiliate link first.",
            reply_markup=get_ig_affiliate_menu(),
            parse_mode="HTML"
        )
    
    text = f"📋 <b>IG CONTENT WITH AFFILIATE LINKS (Page {page+1}):</b>\nResult {skip+1}-{min(skip+len(contents), total)} of {total}\n━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for idx, content in enumerate(contents, start=skip+1):
        text += f"{idx}. <b>{content['cc_code']}</b>\n"
        text += f"   🔗 {content.get('affiliate_link', 'N/A')}\n\n"
    
    text += f"━━━━━━━━━━━━━━━━━━━━\nTotal: <b>{total}</b> affiliate link(s)"
    
    # Pagination Keyboard
    buttons = []
    if page > 0: 
        buttons.append(KeyboardButton(text=f"⬅️ PREV_IGAFF {page}"))
    if (skip + limit) < total: 
        buttons.append(KeyboardButton(text=f"➡️ NEXT_IGAFF {page+2}"))
    
    keyboard = []
    if buttons: keyboard.append(buttons)
    keyboard.append([KeyboardButton(text="◀️ Back")]) # Navigate back to affiliate menu
    
    size_mb = sys.getsizeof(text) # Basic size check
    if len(text) > 4000:
        # Split logic if dangerously huge (fallback)
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
             await message.answer(part, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

@dp.message(F.text == "📋 List")
async def list_ig_affiliates_handler(message: types.Message):
    if not await check_authorization(message, "List IG Affiliates", "can_list"):
        return
    """List all IG content with affiliate links"""
    await send_ig_affiliate_list_view_text(message, page=0)

@dp.message(lambda m: m.text and (m.text.startswith("⬅️ PREV_IGAFF") or m.text.startswith("➡️ NEXT_IGAFF")))
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
    now = now_local()
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
        now_ts = now_local()
        metadata = {
            "backup_type": "auto" if auto else "manual",
            "created_at": now_ts,
            "month": now_ts.strftime("%B"),
            "month_num": now_ts.month,            # numeric for sorting (1-12)
            "year": now_ts.year,
            "backup_key": f"{now_ts.year}/{now_ts.month:02d}",   # e.g. "2026/01"
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
            now = now_local()
            
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
                            f"✅ <b>AUTO-BACKUP SUCCESSFUL</b>\n\n"
                            f"📦 File: `{metadata['filename']}`\n"
                            f"💾 Size: {metadata['file_size_mb']:.2f} MB\n"
                            f"📊 PDFs: {metadata['pdfs_count']} | IG: {metadata['ig_count']}\n"
                            f"🕐 Time: {now.strftime('%I:%M %p')}",
                            parse_mode="HTML"
                        )
                    except:
                        pass
                else:
                    # CRITICAL: Notify admin of backup failure
                    try:
                        await bot.send_message(
                            MASTER_ADMIN_ID,
                            f"🚨 <b>AUTO-BACKUP FAILED!</b>\n\n"
                            f"⚠️ The scheduled monthly backup could not be created.\n"
                            f"📅 Date: {now.strftime('%B %d, %Y')}\n"
                            f"🕐 Time: {now.strftime('%I:%M %p')}\n\n"
                            f"Please check the system immediately!",
                            parse_mode="HTML"
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
                    f"🚨 <b>BACKUP SYSTEM ERROR!</b>\n\n"
                    f"❌ Error: `{str(e)}`\n"
                    f"🕐 Time: {now_local().strftime('%I:%M %p')}\n\n"
                    f"The auto-backup system encountered an error.",
                    parse_mode="HTML"
                )
            except:
                logger.error("Could not notify admin of backup system error!")
            
            await asyncio.sleep(3600)  # Wait an hour before retrying

@dp.message(F.text == "💾 BACKUP DATA", StateFilter(None))
async def backup_menu_handler(message: types.Message, state: FSMContext):
    if not await check_authorization(message, "Backup Menu", "can_manage_admins"):
        return
    """Show backup menu"""
    await state.set_state(BackupStates.viewing_backup_menu)
    await message.answer(
        "💾 <b>BACKUP & EXPORT</b>\n\n"
        "Choose a backup option:\n\n"
        "💾 <b>FULL BACKUP</b> - Export all data with timestamps\n"
        "📋 <b>VIEW AS JSON</b> - See backup in JSON format\n"
        "📊 <b>BACKUP STATS</b> - View database statistics\n"
        "📜 <b>BACKUP HISTORY</b> - View all monthly backup reports\n\n"
        "Select an option:",
        reply_markup=get_backup_menu(),
        parse_mode="HTML"
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
                    f"🚨 <b>MANUAL BACKUP FAILED!</b>\n\n"
                    f"⚠️ User: {message.from_user.first_name or 'Unknown'} (ID: {message.from_user.id})\n"
                    f"📅 Date: {now_local().strftime('%B %d, %Y')}\n"
                    f"🕐 Time: {now_local().strftime('%I:%M %p')}\n\n"
                    f"Please investigate the backup system!",
                    parse_mode="HTML"
                )
            except:
                logger.error("Could not notify admin of manual backup failure!")
            
            return
        
        # Get timestamp
        now = now_local()
        timestamp_12h = now.strftime("%Y-%m-%d %I:%M:%S %p")
        
        # Build backup summary
        backup_text = f"✅ <b>BACKUP CREATED SUCCESSFULLY!</b>\n"
        backup_text += f"═══════════════════════════\n\n"
        backup_text += f"📦 <b>File:</b> `{metadata['filename']}`\n"
        backup_text += f"💾 <b>Size:</b> {metadata['file_size_mb']:.2f} MB\n"
        backup_text += f"🕐 <b>Created:</b> {timestamp_12h}\n\n"
        
        backup_text += f"📊 <b>DATA SUMMARY:</b>\n"
        backup_text += f"├ 📄 PDFs: {metadata['pdfs_count']}\n"
        backup_text += f"└ 📸 IG Content: {metadata['ig_count']}\n\n"
        
        backup_text += f"🎯 <b>CLICK STATISTICS:</b>\n"
        backup_text += f"├ Total Clicks: {metadata['total_clicks']:,}\n"
        backup_text += f"├ YT Clicks: {metadata['total_yt_clicks']:,}\n"
        backup_text += f"└ IGCC Clicks: {metadata['total_ig_cc_clicks']:,}\n\n"
        
        backup_text += f"═══════════════════════════\n"
        backup_text += f"💡 <b>Backup Location:</b>\n`backups/{metadata['filename']}`\n\n"
        backup_text += f"🔒 Data is compressed and saved securely!"
        
        await message.answer(backup_text, parse_mode="HTML")
        
        # Log the backup action
        log_user_action(message.from_user, "FULL_BACKUP", f"Created {metadata['filename']} ({metadata['file_size_mb']:.2f} MB)")
        
    except Exception as e:
        logger.error(f"Backup error: {e}")
        await message.answer("❌ Backup failed. Please try again later.")
        
        # CRITICAL: Notify master admin of backup exception
        try:
            await bot.send_message(
                MASTER_ADMIN_ID,
                f"🚨 <b>BACKUP EXCEPTION!</b>\n\n"
                f"❌ Error: `{str(e)}`\n"
                f"👤 User: {message.from_user.first_name or 'Unknown'} (ID: {message.from_user.id})\n"
                f"🕐 Time: {now_local().strftime('%I:%M %p')}\n\n"
                f"Check the backup system immediately!",
                parse_mode="HTML"
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
        now = now_local()
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
            caption=f"📋 <b>JSON BACKUP</b>\n\n"
                   f"🕐 Time: {timestamp_12h}\n"
                   f"📊 PDFs: {len(all_pdfs)}\n"
                   f"📸 IG Content: {len(all_ig_content)}\n\n"
                   f"✅ Backup exported successfully!",
            parse_mode="HTML"
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
        stats_text = f"📊 <b>DATABASE STATISTICS</b>\n"
        stats_text += f"═══════════════════════════\n\n"
        
        stats_text += f"💾 <b>STORAGE:</b>\n"
        stats_text += f"├ Total DB Size: {db_size_mb:.2f} MB\n"
        stats_text += f"├ PDFs Collection: {pdf_size_mb:.2f} MB\n"
        stats_text += f"└ IG Collection: {ig_size_mb:.2f} MB\n\n"
        
        stats_text += f"📁 <b>COLLECTIONS:</b>\n"
        stats_text += f"├ `bot9_pdfs`: {pdf_count:,} documents\n"
        stats_text += f"└ `bot9_ig_content`: {ig_count:,} documents\n\n"
        
        # Index information
        pdf_indexes = col_pdfs.list_indexes()
        ig_indexes = col_ig_content.list_indexes()
        
        pdf_index_count = sum(1 for _ in pdf_indexes)
        ig_index_count = sum(1 for _ in ig_indexes)
        
        stats_text += f"🔍 <b>INDEXES:</b>\n"
        stats_text += f"├ PDFs: {pdf_index_count} indexes\n"
        stats_text += f"└ IG Content: {ig_index_count} indexes\n\n"
        
        # Recent activity
        recent_pdfs = col_pdfs.count_documents({"created_at": {"$gte": now_local().replace(hour=0, minute=0, second=0, microsecond=0)}})
        recent_ig = col_ig_content.count_documents({"created_at": {"$gte": now_local().replace(hour=0, minute=0, second=0, microsecond=0)}})
        
        stats_text += f"📈 <b>TODAY'S ACTIVITY:</b>\n"
        stats_text += f"├ New PDFs: {recent_pdfs}\n"
        stats_text += f"└ New IG Content: {recent_ig}\n\n"
        
        stats_text += f"═══════════════════════════\n"
        stats_text += f"🕐 <b>Updated:</b> {now_local().strftime('%I:%M:%S %p')}"
        
        await message.answer(stats_text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        # Use plain text for error message to avoid markdown parsing issues
        await message.answer(f"❌ Failed to retrieve stats. Please try again later.")

@dp.message(F.text == "📜 BACKUP HISTORY")
async def backup_history_handler(message: types.Message):
    """Show complete backup history grouped by Year → Month with MongoDB storage info"""
    if not await check_authorization(message, "View Backup History", "can_manage_users"):
        return
    
    try:
        # Get all backups sorted newest first
        all_backups = list(col_backups.find().sort([("year", -1), ("month_num", -1), ("created_at", -1)]))
        
        if not all_backups:
            await message.answer(
                "📜 <b>BACKUP HISTORY</b>\n\n"
                "No backups found in the system.\n\n"
                "💡 Use <b>💾 FULL BACKUP</b> to create your first backup!\n\n"
                f"📦 <b>Storage Location:</b>\n"
                f"Database: `{MONGO_DB_NAME}`\n"
                f"Collection: `bot9_backups`\n"
                f"Key structure: `year / month_num / backup_key`",
                parse_mode="HTML"
            )
            return
        
        # Group by year → then month_num within year
        from collections import defaultdict
        by_year_month: dict = defaultdict(lambda: defaultdict(list))
        for backup in all_backups:
            year = backup.get("year", now_local().year)
            month_num = backup.get("month_num", now_local().month)
            by_year_month[year][month_num].append(backup)

        history = "📜 <b>BACKUP HISTORY</b>\n"
        history += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        history += f"📊 <b>Total Backups:</b> {len(all_backups)}\n"
        history += f"📦 <b>Stored in:</b> `{MONGO_DB_NAME}` → collection `bot9_backups`\n\n"

        for year in sorted(by_year_month.keys(), reverse=True):
            history += f"╔═══════════════════\n"
            history += f"║ 📅 <b>{year}</b>\n"
            history += f"╚═══════════════════\n"

            months_in_year = by_year_month[year]
            for month_num in sorted(months_in_year.keys(), reverse=True):
                backups_in_month = months_in_year[month_num]
                first = backups_in_month[0]
                month_name = first.get("month", f"Month {month_num}")
                backup_key = first.get("backup_key", f"{year}/{month_num:02d}")

                history += f"\n  📁 <b>{month_name} {year}</b>\n"
                history += f"  DB path: `{MONGO_DB_NAME}.bot9_backups`  key: `{backup_key}`\n"
                history += "  " + "─" * 30 + "\n"

                for backup in backups_in_month:
                    filename  = backup.get("filename", "unknown")
                    size_mb   = backup.get("file_size_mb", 0)
                    bk_type   = backup.get("backup_type", "manual")
                    created_at = backup.get("created_at")

                    if created_at and hasattr(created_at, "strftime"):
                        time_str = created_at.strftime("%b %d, %Y  %I:%M %p")
                    else:
                        time_str = str(created_at) if created_at else "Unknown"

                    type_emoji = "🔄 Auto" if bk_type == "auto" else "👤 Manual"

                    history += f"  {type_emoji}\n"
                    history += f"  ├ 🗂 File: `{filename}`\n"
                    history += f"  ├ 💾 Size: {size_mb:.2f} MB\n"
                    history += f"  ├ 📄 PDFs: {backup.get('pdfs_count', 0)}  |  📸 IG: {backup.get('ig_count', 0)}\n"
                    history += f"  ├ 🖱 Clicks: {backup.get('total_clicks', 0):,}\n"
                    history += f"  └ 🕐 Created: {time_str}\n\n"

            history += "\n"

        history += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        history += "🔄 = Auto-backup  |  👤 = Manual backup\n"
        history += "💡 All backups are stored permanently in MongoDB"

        # Split if too long for Telegram (4096 chars limit)
        if len(history) > 4000:
            chunks = [history[i:i+4000] for i in range(0, len(history), 4000)]
            for chunk in chunks:
                await message.answer(chunk, parse_mode="HTML")
        else:
            await message.answer(history, parse_mode="HTML")

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
    
    await message.answer("🔐 <b>Admin Management</b>\nSelect an option below:", reply_markup=get_admin_config_menu(), parse_mode="HTML")

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
        total_admins = col_admins.count_documents({"user_id": {"$ne": MASTER_ADMIN_ID}})
        admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}).skip(skip).limit(ADMINS_PER_PAGE))

        if not admins and page == 0:
            # Build keyboard with just back button
            kb = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="⬅️ BACK TO ADMIN MENU")]],
                resize_keyboard=True
            )
            await message.answer("📋 <b>Admin List</b>\n\nNo other admins found in the database.", reply_markup=kb, parse_mode="HTML")
            return

        # Build message
        total_pages = max(1, (total_admins + ADMINS_PER_PAGE - 1) // ADMINS_PER_PAGE)
        text = f"📋 <b>Admin List</b> — Page {page + 1}/{total_pages} ({total_admins} total)\n"
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
            
            # Use same format but separate lines carefully
            text += f"<b>{i}.</b> `{uid}` — {name} {status_str}\n"
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
        await message.answer(text, reply_markup=kb, parse_mode="HTML")

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
async def admin_list_back(message: types.Message, state: FSMContext):
    """Return to Admin Management menu from list view"""
    if not await check_authorization(message, "Admin Menu", "can_manage_admins"):
        return
    await state.clear()
    await message.answer(
        "🔐 <b>Admin Management</b>\nSelect an option below:",
        reply_markup=get_admin_config_menu(),
        parse_mode="HTML"
    )



# ==========================================
# BAN CONFIGURATION HANDLERS
# ==========================================

@dp.message(F.text == "🚫 BAN CONFIG")
async def ban_config_menu_handler(message: types.Message):
    """Show Ban Configuration Menu"""
    if not await check_authorization(message, "Access Ban Config", "can_manage_admins"):
        return
    await message.answer("🚫 <b>BAN CONFIGURATION</b>\nSelect an option below:", reply_markup=get_ban_config_menu(), parse_mode="HTML")

@dp.message(F.text == "⬅️ BACK TO ADMIN MENU")
async def back_to_admin_menu_handler(message: types.Message):
    """Return to Admin Menu"""
    if not await check_authorization(message, "Back to Admin Menu", "can_manage_admins"):
        return
    await message.answer("👥 <b>ADMIN CONFIGURATION</b>", reply_markup=get_admin_config_menu(), parse_mode="HTML")

@dp.message(F.text == "🚫 BAN USER")
async def ban_user_start(message: types.Message, state: FSMContext):
    """Start ban user flow"""
    if not await check_authorization(message, "Ban User", "can_manage_admins"):
        return
    await state.set_state(AdminManagementStates.waiting_for_ban_user_id)
    await message.answer(
        "🚫 <b>BAN USER</b>\n\n"
        "Please enter the <b>Telegram User ID</b> of the user to ban.\n"
        "They will be blocked from accessing the bot.",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
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
                "⛔ <b>ACTION DENIED</b>\n\n"
                "You cannot ban an active Admin!\n"
                "<i>Please remove them from the Admin list first.</i>", 
                reply_markup=get_ban_config_menu(),
                parse_mode="HTML"
            )
            await state.clear()
            return
            
        # Check if already banned
        if is_banned(ban_id):
            await message.answer(
                f"⚠️ <b>User `{ban_id}` is already banned!</b>", 
                reply_markup=get_ban_config_menu(),
                parse_mode="HTML"
            )
            await state.clear()
            return
            
        # Ban User
        reason = f"Manual Ban by Admin {message.from_user.id}"
        ban_user(ban_id, "Unknown", "Unknown", reason) # Helper function handles logging
        
        await state.clear()
        await message.answer(
            f"✅ <b>SUCCESS!</b>\n\nUser `{ban_id}` has been BANNED.",
            reply_markup=get_ban_config_menu(),
            parse_mode="HTML"
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
        "✅ <b>UNBAN USER</b>\n\n"
        "Please enter the <b>Telegram User ID</b> of the user to unban.",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
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
                f"⚠️ <b>User {unban_id} is NOT found in ban list.</b>", 
                reply_markup=get_ban_config_menu(),
                parse_mode="HTML"
            )
            await state.clear()
            return
            
        # Unban User
        col_banned_users.delete_one({"user_id": unban_id})
        logger.info(f"User {unban_id} unbanned by Admin {message.from_user.id}")
        
        await state.clear()
        await message.answer(
            f"✅ <b>SUCCESS!</b>\n\nUser `{unban_id}` has been UNBANNED.",
            reply_markup=get_ban_config_menu(),
            parse_mode="HTML"
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
        await message.answer("⚠️ <b>No banned users found.</b>", reply_markup=get_ban_config_menu(), parse_mode="HTML")
        return
        
    msg = "🚫 <b>BANNED USERS LIST</b>\n━━━━━━━━━━━━━━━━━━━━\n"
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
            f"{count}. <b>{name}</b> (`{uid}`)\n"
            f"   📝 Reason: {reason}\n"
            f"   📅 Time: {date_str}\n\n"
        )
        
    if len(msg) > 4000:
        msg = msg[:4000] + "\n...(truncated)"
        
    await message.answer(msg, reply_markup=get_ban_config_menu(), parse_mode="HTML")

# ==========================================
# ROLE MANAGEMENT HANDLERS
# ==========================================

@dp.message(F.text == "👔 ROLES")
@dp.message(F.text == "🔒 LOCK/UNLOCK")
async def roles_menu_handler(message: types.Message, state: FSMContext):
    """Show list of admins to select for Role Assignment or Lock/Unlock"""
    if not await check_authorization(message, "Manage Roles", "can_manage_admins"):
        return
        
    # Check if admins exist excluding Master Admin
    if col_admins.count_documents({"user_id": {"$ne": MASTER_ADMIN_ID}}) == 0:
        await message.answer("⚠️ No other admins found.", reply_markup=get_admin_config_menu())
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
    """Helper to send paginated admin list for role selection or lock/unlock"""
    ITEMS_PER_PAGE = 10
    admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}).sort("added_at", 1))
    total_admins = len(admins)
    total_pages = max(1, (total_admins + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    
    # Cap page just in case
    page = min(page, max(0, total_pages - 1))
    
    start = page * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, total_admins)
    current_admins = admins[start:end]
    
    # Build Keyboard & Text List
    keyboard = []
    admin_list_text = ""
    
    # Admin Buttons (2 per row)
    row = []
    for i, admin in enumerate(current_admins):
        user_id = admin.get("user_id")
        
        # Smartly extract Name, fallback to Username, then User ID
        name = admin.get("full_name")
        if not name or name == str(user_id):
            name = admin.get("username")
            if not name:
                name = "Admin"
                
        is_locked = admin.get("is_locked", False)
        status_str = "[🔒 LOCKED]" if is_locked else "[🔓 ACTIVE]"
        
        # Add to text list
        global_idx = start + i + 1
        admin_list_text += f"{global_idx}. <b>{name}</b> (`{user_id}`) {status_str}\n"
        
        # Button Format changes based on mode
        if mode == "lock":
            icon = "🔒" if is_locked else "🔓"
            btn_text = f"{icon} {name} [{user_id}]"
        else:
            btn_text = f"👤 {name} [{user_id}]"
            
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
    
    header = "LOCK/UNLOCK" if mode == "lock" else "MODIFY ROLE"
    action = "toggle lock status" if mode == "lock" else "modify their role"
    
    await message.answer(
        f"👔 <b>SELECT ADMIN TO {header}</b>\n\n"
        f"Select an admin from the list below to {action}:\n\n"
        f"{admin_list_text}\n"
        f"Page {page + 1}/{total_pages}",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
        parse_mode="HTML"
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
    """Admin selected for role or lock toggle"""
    text = message.text
    
    if text == "❌ CANCEL" or text == "⬅️ RETURN BACK":
        await state.clear()
        await message.answer("↩️ Returned to Admin menu.", reply_markup=get_admin_config_menu())
        return

    # Try to extract ID from button text "Name [ID]" or "(ID)" for legacy
    import re
    match = re.search(r"\[(\d+)\]$", text)
    if not match:
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
    admin_doc = col_admins.find_one({"user_id": target_admin_id})
    if not admin_doc and target_admin_id != MASTER_ADMIN_ID:
        await message.answer(f"⚠️ User {target_admin_id} is not an admin.", reply_markup=get_admin_config_menu())
        await state.clear()
        return
        
    # Prevent modifying Master Admin
    if target_admin_id == MASTER_ADMIN_ID:
        await message.answer("⛔ You cannot modify the Master Admin.", reply_markup=get_admin_config_menu())
        await state.clear()
        return

    admin_name = admin_doc.get("full_name", "Admin")
    is_locked = admin_doc.get("is_locked", False)
    
    # Determine Menu based on Mode
    data = await state.get_data()
    mode = data.get("role_menu_mode", "roles")
    current_page = data.get("role_admin_page", 0)
    
    if mode == "lock":
        # Instantly toggle lock status and stay on the same paginated keyboard
        new_lock_state = not is_locked
        col_admins.update_one({"user_id": target_admin_id}, {"$set": {"is_locked": new_lock_state}})
        
        status_text = "LOCKED (Inactive)" if new_lock_state else "UNLOCKED (Active)"
        icon = "🔒" if new_lock_state else "🔓"
        
        log_user_action(message.from_user, f"{icon} ADMIN STATUS CHANGED", f"Set {target_admin_id} to {status_text}")
        
        await message.answer(
            f"✅ <b>STATUS UPDATED</b>\n\n"
            f"👤 Admin: {admin_name} (`{target_admin_id}`)\n"
            f"{icon} Status: <b>{status_text}</b>",
            parse_mode="HTML"
        )
        
        # Refresh the active page
        await send_role_admin_list(message, current_page, mode)
        return
        
    # Standard role assignment flow (mode == "roles")
    await state.update_data(target_admin_id=target_admin_id)
    await state.set_state(AdminRoleStates.waiting_for_role_selection)
    
    target_menu = get_roles_menu()
    msg_text = f"👔 <b>SELECT ROLE FOR {admin_name}</b> (`{target_admin_id}`)\n\nChoose a role to apply permissions:"
    
    await message.answer(
        msg_text,
        reply_markup=target_menu,
        parse_mode="HTML"
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
            "🔐 <b>SECURITY CHECK</b>\n\n"
            "Resetting Ownership requires a password.\n"
            "Please enter the <b>Owner Password</b>:",
            reply_markup=get_cancel_keyboard(),
            parse_mode="HTML"
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
            await message.answer(f"⚠️ <b>Admin {target_admin_id} is ALREADY LOCKED.</b>", reply_markup=get_admin_config_menu(), parse_mode="HTML")
            await state.clear()
            return

        col_admins.update_one({"user_id": target_admin_id}, {"$set": {"is_locked": True}})
        log_user_action(message.from_user, "ADMIN LOCKED", f"Locked {target_admin_id}")
        await state.clear()
        await message.answer(
            f"🔒 <b>ADMIN LOCKED</b>\n\nUser `{target_admin_id}` has been locked.\nThey have NO access.",
            reply_markup=get_admin_config_menu(), parse_mode="HTML"
        )
        return
        
    elif "🔓 UNLOCK" in selected_role:
        # Check if already unlocked
        admin_doc = col_admins.find_one({"user_id": target_admin_id})
        if admin_doc and not admin_doc.get("is_locked", False):
            await message.answer(f"⚠️ <b>Admin {target_admin_id} is ALREADY UNLOCKED.</b>", reply_markup=get_admin_config_menu(), parse_mode="HTML")
            await state.clear()
            return

        col_admins.update_one({"user_id": target_admin_id}, {"$set": {"is_locked": False}})
        log_user_action(message.from_user, "ADMIN UNLOCKED", f"Unlocked {target_admin_id}")
        
        # Send Role Message on Unlock
        if admin_doc:
            perms = admin_doc.get("permissions", [])
            # Determine role from perms
            detected_role = "SUPPORT"
            for r_name, r_perms in ROLES.items():
                if set(r_perms) == set(perms):
                    detected_role = r_name
                    break
            
            try:
                caps_list = []
                if detected_role == "OWNER": caps_list = ["• Absolute Power", "• Manage Everything"]
                elif detected_role == "MANAGER": caps_list = ["• Manage Admins", "• Manage Content", "• View Analytics"]
                elif detected_role == "ADMIN": caps_list = ["• Manage Content", "• Manage Links", "• View Analytics"]
                elif detected_role == "MODERATOR": caps_list = ["• Add/Edit Content", "• Search Database"]
                elif detected_role == "SUPPORT": caps_list = ["• View Content", "• Search Only"]
                
                caps_str = "\n".join(caps_list)
                
                await bot.send_message(
                    target_admin_id,
                    f"🌟 <b>ACCESS RESTORED</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n\n"
                    f"<b>Dear Administrator,</b>\n\n"
                    f"Your account status has been officially <b>UNLOCKED</b>.\n\n"
                    f"You are currently designated as a <b>{detected_role}</b>. "
                    f"Your authorized system capabilities are outlined below:\n\n"
                    f"{caps_str}\n\n"
                    f"<i>Access restored and authorized by {message.from_user.full_name}.</i>",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Failed to notify admin {target_admin_id} of unlock: {e}")

        await state.clear()
        await message.answer(
            f"🔓 <b>ADMIN UNLOCKED</b>\n\nUser `{target_admin_id}` has been unlocked.\nPermissions restored.",
            reply_markup=get_admin_config_menu(), parse_mode="HTML"
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
    
    # Notify Target Admin (Premium Message) ONLY if not locked
    admin_doc = col_admins.find_one({"user_id": target_admin_id})
    if admin_doc and not admin_doc.get("is_locked", False) and "UNLOCK" not in selected_role:
        try:
            caps_list = []
            if role_key == "MANAGER": caps_list = ["• Manage Admins", "• Manage Content", "• View Analytics"]
            elif role_key == "ADMIN": caps_list = ["• Manage Content", "• Manage Links", "• View Analytics"]
            elif role_key == "MODERATOR": caps_list = ["• Add/Edit Content", "• Search Database"]
            elif role_key == "SUPPORT": caps_list = ["• View Content", "• Search Only"]
            
            caps_str = "\n".join(caps_list)
            
            await bot.send_message(
                target_admin_id,
                f"🌟 <b>PROMOTION GRANTED</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"<b>Dear Administrator,</b>\n\n"
                f"Your account has been officially elevated to <b>{role_key}</b> status.\n\n"
                f"Your new authorized system capabilities are outlined below:\n\n"
                f"{caps_str}\n\n"
                f"<i>Access granted and authorized by {message.from_user.full_name}.</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {target_admin_id} of role change: {e}")
        
    await state.clear()
    await message.answer(
        f"✅ <b>SUCCESS!</b>\n\n"
        f"User `{target_admin_id}` is now <b>{role_key}</b>.\n"
        f"Permissions updated.",
        reply_markup=get_admin_config_menu(),
        parse_mode="HTML"
    )

@dp.message(AdminRoleStates.waiting_for_owner_password)
async def process_owner_password(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        # Return to Role Selection state instead of clearing
        await state.set_state(AdminRoleStates.waiting_for_role_selection)
        await message.answer("❌ Cancelled.", reply_markup=get_roles_menu())
        return

    password = message.text.strip()
    if password == OWNER_PASSWORD:
        # Check permissions — only the owner (who knows OWNER_PASSWORD) can confirm ownership transfer
        await state.set_state(AdminRoleStates.waiting_for_owner_confirm)
        
        data = await state.get_data()
        target_admin_id = data.get("target_admin_id")
        
        await message.answer(
            f"⚠️ <b>CRITICAL WARNING</b> ⚠️\n\n"
            f"You are about to transfer <b>OWNERSHIP</b> to `{target_admin_id}`.\n"
            f"This action is <b>IRREVERSIBLE</b> via the bot.\n"
            f"You will lose your Owner privileges.\n\n"
            f"Are you sure?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ YES, TRANSFER OWNERSHIP"), KeyboardButton(text="❌ CANCEL")]],
                resize_keyboard=True
            ),
            parse_mode="HTML"
        )
    else:
        await message.answer("⛔ <b>Incorrect Password.</b> Access Denied.", reply_markup=get_roles_menu(), parse_mode="HTML")
        await state.clear()

@dp.message(AdminRoleStates.waiting_for_owner_confirm)
async def process_owner_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ YES, TRANSFER OWNERSHIP":
        await state.set_state(AdminRoleStates.waiting_for_owner_second_confirm)
        await message.answer(
            f"⚠️ <b>FINAL CONFIRMATION</b> ⚠️\n\n"
            f"This is your last warning! Transferring ownership is permanent and you will become a manager.\n"
            f"Are you ABSOLUTELY sure?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ YES, I AM SURE"), KeyboardButton(text="❌ CANCEL")]],
                resize_keyboard=True
            ),
            parse_mode="HTML"
        )
    else:
        # Return to Role Selection state
        await state.set_state(AdminRoleStates.waiting_for_role_selection)
        await message.answer("❌ Transfer Cancelled.", reply_markup=get_roles_menu())

@dp.message(AdminRoleStates.waiting_for_owner_second_confirm)
async def process_owner_second_confirm(message: types.Message, state: FSMContext):
    if message.text == "✅ YES, I AM SURE":
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
        if current_owner_id != target_admin_id: # Self-promotion check
            col_admins.update_one(
                {"user_id": current_owner_id},
                {"$set": {
                    "permissions": ROLES["MANAGER"],
                    "is_owner": False
                }}
            )
        
        # 3. Update Global Cache & .env permanently
        MASTER_ADMIN_ID = target_admin_id
        try:
            with open("BOT9.env", "r", encoding="utf-8") as f:
                env_data = f.read()
            # Replace MASTER_ADMIN_ID correctly
            if "MASTER_ADMIN_ID=" in env_data:
                env_data = re.sub(r"MASTER_ADMIN_ID=.*", f"MASTER_ADMIN_ID={target_admin_id}", env_data)
            else:
                env_data += f"\\nMASTER_ADMIN_ID={target_admin_id}\\n"
            with open("BOT9.env", "w", encoding="utf-8") as f:
                f.write(env_data)
        except Exception as e:
            logger.error(f"Failed to update BOT9.env: {e}")
            
        # Log
        log_user_action(message.from_user, "OWNERSHIP TRANSFER", f"New Owner: {target_admin_id}")
        
        # Notify Steps
        try:
             await bot.send_message(
                target_admin_id,
                f"👑 <b>ALL HAIL THE NEW OWNER!</b>\\n\\n"
                f"You have been granted <b>OWNERSHIP</b> of this bot.\\n"
                f"You now have absolute power.\\n\\n"
                f"*Transfer authorized by previous owner.*",
                parse_mode="HTML"
            )
        except: pass
        
        await state.clear()
        await message.answer(
            f"✅ <b>OWNERSHIP TRANSFERRED!</b>\\n\\n"
            f"New Owner: `{target_admin_id}`\\n"
            f"You are now a <b>MANAGER</b>.\\n"
            f"Please restart the bot for full effect.",
            reply_markup=get_main_menu(current_owner_id),
            parse_mode="HTML"
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
        await message.answer("⛔ <b>ACCESS DENIED</b>\n\nOnly the Master Admin can manage permissions.", parse_mode="HTML")
        return

    # List admins to select (EXCLUDE MASTER ADMIN)
    admins = list(col_admins.find({"user_id": {"$ne": MASTER_ADMIN_ID}}).sort("added_at", 1))
    
    if not admins:
        await message.answer("⚠️ <b>No additional admins found.</b>\nAdd admins first to configure permissions.", reply_markup=get_admin_config_menu(), parse_mode="HTML")
        return
        
    await state.set_state(AdminPermissionStates.waiting_for_admin_selection)
    
    msg = "🔐 <b>MANAGE PERMISSIONS</b>\n\nSelect an admin to configure:\n"
    keyboard = []
    
    for admin in admins:
        user_id = admin['user_id']
        name = admin.get('full_name', 'Unknown')
        username = f"(@{admin.get('username')})" if admin.get('username') else ""
        keyboard.append([KeyboardButton(text=str(user_id))]) # Send ID as text
        msg += f"• `{user_id}`: {name} {username}\n"
        
    keyboard.append([KeyboardButton(text="❌ CANCEL")])
    
    await message.answer(msg, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

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
        
    # Get current permissions (Default to None so we can initialize to empty as requested)
    current_perms = admin.get("permissions")
    if current_perms is None:
        current_perms = [] # Start blank so you explicitly grant what is needed
        
    # Save partial state
    await state.update_data(target_admin_id=target_id, current_perms=current_perms)
    await state.set_state(AdminPermissionStates.configuring_permissions)
    
    # Show toggles via Inline Keyboard (better for toggling)
    await send_permission_toggles(message, target_id, current_perms, admin.get("full_name", "Admin"))

async def send_permission_toggles(message: types.Message, target_id: int, current_perms: list, admin_name: str):
    """Helper to send/update permission toggle UI (Reply Keyboard)"""
    
    text = f"🔐 <b>CONFIGURING: {admin_name}</b> (`{target_id}`)\n\n"
    text += "Use the buttons below to toggle permissions.\n"
    text += "✅ = Allowed | ❌ = Denied\n\n"
    text += "Click <b>💾 SAVE CHANGES</b> to save and exit."
    
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
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True), parse_mode="HTML")

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
        await message.answer(f"✅ <b>PERMISSIONS SAVED</b> for Admin `{target_id}`", reply_markup=get_admin_config_menu(), parse_mode="HTML")
        return

    elif text == "✅ SELECT ALL":
        current_perms = [p for p in DEFAULT_SAFE_PERMISSIONS] # Only Select Safe ones
        # Feedback message
        await message.answer("✅ <b>Safe permissions selected.</b>\n(Dangerous features must be toggled manually)", parse_mode="HTML")

    elif text == "❌ REVOKE ALL":
        current_perms = []
        # Feedback message
        await message.answer("❌ <b>All permissions revoked.</b>", parse_mode="HTML")

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
                        f"📦 <b>STARTUP BACKUP CREATED</b>\n\n"
                        f"The bot detected no backup for {month_year}.\n"
                        f"✅ Created: `{metadata['filename']}`\n"
                        f"💾 Size: {metadata['file_size_mb']:.2f} MB\n\n"
                        f"This ensures no monthly backup is missed!",
                        parse_mode="HTML"
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
        
        # Use now_local() so arithmetic with uptime_start (also naive) works correctly
        now = now_local()
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
        _uptime_start = health_monitor.system_metrics["uptime_start"]
        _uptime_start = _uptime_start.replace(tzinfo=None) if _uptime_start.tzinfo else _uptime_start
        _now_naive = now.replace(tzinfo=None) if now.tzinfo else now
        uptime = _now_naive - _uptime_start
        uptime_str = f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds // 60) % 60}m"
        memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Get top performing content
        top_pdfs = list(col_pdfs.find({}).sort("clicks", -1).limit(5))
        top_ig = list(col_ig_content.find({}).sort("ig_cc_clicks", -1).limit(5))
        
        # Build report
        report = f"📊 <b>BOT 9 DAILY REPORT</b>\n"
        report += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        report += f"📅 <b>Date:</b> {timestamp}\n"
        report += f"⏰ <b>Report Type:</b> {'Morning' if now.hour < 12 else 'Evening'} Report\n\n"
        
        report += f"📈 <b>DATABASE OVERVIEW</b>\n"
        report += f"├ Total PDFs: {total_pdfs}\n"
        report += f"├ Total IG Content: {total_ig_content}\n"
        report += f"├ Total Admins: {total_admins}\n"
        report += f"└ Banned Users: {total_banned}\n\n"
        
        report += f"🆕 <b>TODAY'S ADDITIONS</b>\n"
        report += f"├ New PDFs: {pdfs_added_today}\n"
        report += f"└ New IG Content: {ig_added_today}\n\n"
        
        report += f"📊 <b>LAST 24 HOURS ACTIVITY</b>\n"
        report += f"├ Total Interactions: {total_clicks_24h}\n"
        report += f"├ PDF Affiliate Clicks: {pdf_clicks_24h}\n"
        report += f"├ YT Link Clicks: {yt_clicks_24h}\n"
        report += f"└ IG CC Clicks: {ig_cc_clicks_24h}\n\n"
        
        if top_pdfs:
            report += f"🔥 <b>TOP 5 PERFORMING PDFs</b>\n"
            for i, pdf in enumerate(top_pdfs, 1):
                name = pdf.get('name', 'Unnamed')
                if len(name) > 30:
                    name = name[:30] + "..."
                clicks = pdf.get('clicks', 0)
                report += f"{i}. {name} - {clicks} clicks\n"
            report += "\n"
        
        if top_ig:
            report += f"📸 <b>TOP 5 PERFORMING IG CONTENT</b>\n"
            for i, ig in enumerate(top_ig, 1):
                name = ig.get('name', 'Unnamed')
                if len(name) > 30:
                    name = name[:30] + "..."
                clicks = ig.get('ig_cc_clicks', 0)
                report += f"{i}. {name} - {clicks} clicks\n"
            report += "\n"
        
        report += f"🖥️ <b>SYSTEM HEALTH</b>\n"
        report += f"├ Uptime: {uptime_str}\n"
        report += f"├ Memory Usage: {memory_mb:.2f} MB\n"
        report += f"├ CPU Usage: {cpu_percent}%\n"
        report += f"├ Total Errors (Since Start): {health_monitor.error_count}\n"
        report += f"├ Health Checks Failed: {health_monitor.health_checks_failed}\n"
        report += f"└ Status: {'✅ Healthy' if health_monitor.is_healthy else '⚠️ Degraded'}\n\n"
        
        report += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        report += f"🤖 <b>Bot 9 Enterprise Monitoring System</b>\n"
        _next_lbl = "08:40 PM" if now.hour < 12 else "08:40 AM (tomorrow)"
        report += f"📌 Next report at {_next_lbl}"
        
        # Send report
        await bot.send_message(MASTER_ADMIN_ID, report, parse_mode="HTML")
        logger.info("✅ Daily report sent successfully")
        
    except Exception as e:
        logger.error(f"Failed to generate daily report: {e}")
        await health_monitor.send_error_notification(
            "Daily Report Generation Failed",
            str(e),
            traceback.format_exc()
        )

async def daily_report_task():
    """Background task for scheduled daily reports — sleep-until exact times."""
    if not DAILY_REPORT_ENABLED:
        logger.info("Daily reports disabled")
        return

    # Parse configured time strings into (hour, minute) tuples
    _slots: list = []
    for t_str in [DAILY_REPORT_TIME_1, DAILY_REPORT_TIME_2]:
        try:
            _h, _m = map(int, t_str.split(':'))
            _slots.append((_h, _m))
        except Exception:
            logger.warning(f"Invalid report time '{t_str}' — skipping")
    if not _slots:
        logger.error("No valid daily report times configured")
        return

    logger.info(f"✅ Daily report task started (Times: {DAILY_REPORT_TIME_1}, {DAILY_REPORT_TIME_2})")

    while True:
        try:
            # ── Calculate exact sleep until next slot ──────────────────────
            now = now_local()
            next_fire = None
            for hour, minute in _slots:
                candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if candidate <= now:
                    candidate += timedelta(days=1)
                if next_fire is None or candidate < next_fire:
                    next_fire = candidate

            wait_secs = (next_fire - now_local()).total_seconds()
            h_w = int(wait_secs // 3600)
            m_w = int((wait_secs % 3600) // 60)
            logger.info(f"📊 Next daily report in {h_w}h {m_w}m")
            await asyncio.sleep(max(wait_secs, 1))

            await generate_daily_report()

        except asyncio.CancelledError:
            break
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
        uptime = now_local() - health_monitor.system_metrics["uptime_start"]
        memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        
        return web.json_response({
            "status": "healthy",
            "bot": "Bot 9 Enterprise",
            "timestamp": now_local().isoformat(),
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

# ==========================================
# 🎬 TUTORIAL PK — Universal tutorial link for ALL Bot8 users
# Stored in db["bot9_tutorials"] with type="PK"
# Delivered to Bot8 users on empty /start (no referral payload)
# ==========================================

@dp.message(F.text == "🎬 TUTORIAL")
async def tutorial_pk_menu_handler(message: types.Message, state: FSMContext):
    """Open Tutorial management submenu."""
    if not await check_authorization(message, "Tutorial Menu", "can_add"):
        return
    await state.clear()
    await message.answer(
        "🎬 <b>TUTORIAL</b>\n\n"
        "Manage the <b>universal tutorial link</b> shown to every Bot8 member\n"
        "on their empty start and inside the Agent Guide.\n\n"
        "One link — one message — delivered to every member automatically.",
        reply_markup=get_tutorial_pk_menu(),
        parse_mode="HTML"
    )


@dp.message(F.text == "➕ ADD TUTORIAL")
async def tutorial_pk_add(message: types.Message, state: FSMContext):
    """Start ADD flow — ask admin for the YouTube tutorial link."""
    if not await check_authorization(message, "Add Tutorial", "can_add"):
        return
    existing = db["bot9_tutorials"].find_one({"type": "PK"})
    if existing and existing.get("link"):
        safe_link = _html.escape(existing["link"])
        await message.answer(
            f"⚠️ <b>A Tutorial link is already set:</b>\n\n"
            f"<code>{safe_link}</code>\n\n"
            "Use <b>✏️ EDIT TUTORIAL</b> to update it, or <b>🗑️ DELETE TUTORIAL</b> to remove it first.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
        return
    await state.set_state(TutorialPKStates.waiting_for_link)
    await message.answer(
        "🔗 <b>SEND THE YOUTUBE TUTORIAL LINK</b>\n\n"
        "This link will be delivered to <b>all Bot8 users</b> when they start\n"
        "with no referral — as a premium tutorial message with an inline button.\n\n"
        "• Must be a valid URL starting with <code>https://</code>\n"
        "• Shown as a button — never as raw text\n\n"
        "Send the link now, or type <code>CANCEL</code> to abort.",
        parse_mode="HTML"
    )


@dp.message(TutorialPKStates.waiting_for_link)
async def tutorial_pk_save_link(message: types.Message, state: FSMContext):
    """Save the new PK tutorial link to the database."""
    if not await check_authorization(message, "Save Tutorial PK", "can_add"):
        await state.clear()
        return
    text = message.text.strip()
    if text.upper() == "CANCEL":
        await state.clear()
        await message.answer("❌ Cancelled.", reply_markup=get_tutorial_pk_menu(), parse_mode="HTML")
        return
    if not re.match(r"^https?://", text):
        await message.answer(
            "❌ <b>Invalid URL.</b> Please send a valid link starting with <code>https://</code>",
            parse_mode="HTML"
        )
        return
    db["bot9_tutorials"].update_one(
        {"type": "PK"},
        {"$set": {"type": "PK", "link": text, "updated_at": datetime.now()}},
        upsert=True
    )
    safe_link = _html.escape(text)
    await state.clear()
    await message.answer(
        f"✅ <b>TUTORIAL SAVED</b>\n\n"
        f"<b>Link:</b> <code>{safe_link}</code>\n\n"
        "All Bot8 users will now see this tutorial on their next empty start and in the Agent Guide.",
        reply_markup=get_tutorial_pk_menu(),
        parse_mode="HTML"
    )


@dp.message(F.text == "✏️ EDIT TUTORIAL")
async def tutorial_pk_edit(message: types.Message, state: FSMContext):
    """Start EDIT flow — shows current link and asks for replacement."""
    if not await check_authorization(message, "Edit Tutorial", "can_add"):
        return
    existing = db["bot9_tutorials"].find_one({"type": "PK"})
    if not existing or not existing.get("link"):
        await message.answer(
            "⚠️ <b>No Tutorial link set yet.</b>\n\nUse <b>➕ ADD TUTORIAL</b> to add one first.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
        return
    safe_link = _html.escape(existing["link"])
    await state.set_state(TutorialPKStates.waiting_for_edit_link)
    await message.answer(
        f"✏️ <b>EDIT TUTORIAL LINK</b>\n\n"
        f"<b>Current link:</b>\n<code>{safe_link}</code>\n\n"
        "Send the new YouTube link, or type <code>CANCEL</code> to abort.",
        parse_mode="HTML"
    )


@dp.message(TutorialPKStates.waiting_for_edit_link)
async def tutorial_pk_save_edit(message: types.Message, state: FSMContext):
    """Apply the updated PK tutorial link."""
    if not await check_authorization(message, "Save Edit Tutorial PK", "can_add"):
        await state.clear()
        return
    text = message.text.strip()
    if text.upper() == "CANCEL":
        await state.clear()
        await message.answer("❌ Cancelled.", reply_markup=get_tutorial_pk_menu(), parse_mode="HTML")
        return
    if not re.match(r"^https?://", text):
        await message.answer(
            "❌ <b>Invalid URL.</b> Please send a valid link starting with <code>https://</code>",
            parse_mode="HTML"
        )
        return
    db["bot9_tutorials"].update_one(
        {"type": "PK"},
        {"$set": {"link": text, "updated_at": datetime.now()}},
        upsert=True
    )
    safe_link = _html.escape(text)
    await state.clear()
    await message.answer(
        f"✅ <b>TUTORIAL UPDATED</b>\n\n"
        f"<b>New link:</b> <code>{safe_link}</code>\n\n"
        "All Bot8 users will now receive this updated tutorial on their next empty start and in the Agent Guide.",
        reply_markup=get_tutorial_pk_menu(),
        parse_mode="HTML"
    )


@dp.message(F.text == "🗑️ DELETE TUTORIAL")
async def tutorial_pk_delete(message: types.Message, state: FSMContext):
    """Ask for confirmation before deleting tutorial link."""
    if not await check_authorization(message, "Delete Tutorial", "can_add"):
        return
    existing = db["bot9_tutorials"].find_one({"type": "PK"})
    if not existing or not existing.get("link"):
        await message.answer(
            "⚠️ <b>No Tutorial link to delete.</b>",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
        return
    safe_link = _html.escape(existing["link"])
    await state.set_state(TutorialPKStates.waiting_for_delete_confirm)
    await message.answer(
        f"🗑️ <b>DELETE TUTORIAL?</b>\n\n"
        f"<b>Current link:</b>\n<code>{safe_link}</code>\n\n"
        "Type <code>CONFIRM</code> to delete permanently, or <code>CANCEL</code> to abort.",
        parse_mode="HTML"
    )


@dp.message(TutorialPKStates.waiting_for_delete_confirm)
async def tutorial_pk_confirm_delete(message: types.Message, state: FSMContext):
    """Execute deletion after CONFIRM keyword received."""
    if not await check_authorization(message, "Confirm Delete Tutorial PK", "can_add"):
        await state.clear()
        return
    text = message.text.strip().upper()
    if text == "CONFIRM":
        db["bot9_tutorials"].delete_one({"type": "PK"})
        await state.clear()
        await message.answer(
            "✅ <b>TUTORIAL DELETED.</b>\n\n"
            "Bot8 users will now see a professional 'coming soon' message "
            "until a new link is added.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
    elif text == "CANCEL":
        await state.clear()
        await message.answer("❌ Deletion cancelled.", reply_markup=get_tutorial_pk_menu(), parse_mode="HTML")
    else:
        await message.answer(
            "⚠️ Type exactly <code>CONFIRM</code> to delete or <code>CANCEL</code> to abort.",
            parse_mode="HTML"
        )


@dp.message(F.text == "📋 LIST TUTORIAL")
async def tutorial_pk_list(message: types.Message, state: FSMContext):
    """Display the currently stored tutorial link."""
    if not await check_authorization(message, "List Tutorial", "can_add"):
        return
    await state.clear()
    existing = db["bot9_tutorials"].find_one({"type": "PK"})
    if existing and existing.get("link"):
        safe_link = _html.escape(existing["link"])
        updated = existing.get("updated_at")
        updated_str = updated.strftime("%B %d, %Y — %I:%M %p") if updated else "Unknown"
        await message.answer(
            f"📋 <b>TUTORIAL LINK</b>\n\n"
            f"<b>Status:</b> ✅ Active\n"
            f"<b>Scope:</b> Universal — all Bot8 users (empty start + Agent Guide)\n"
            f"<b>Last updated:</b> {updated_str}\n\n"
            f"<b>Link:</b>\n<code>{safe_link}</code>",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "📋 <b>TUTORIAL STATUS</b>\n\n"
            "❌ <b>No link set yet.</b>\n\n"
            "Use <b>➕ ADD TUTORIAL</b> to add a link — it will be\n"
            "sent to Bot8 users as a premium framed message with a watch button.",
            reply_markup=get_tutorial_pk_menu(),
            parse_mode="HTML"
        )

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
        f"📄 <b>SELECTED PDF</b>\n"
        f"🆔 Index: `{pdf['index']}`\n"
        f"📛 Name: {pdf['name']}\n"
        f"🔗 Link: {pdf['link']}\n\n"
        "⬇️ <b>Select Action:</b>",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.message(PDFActionStates.waiting_for_action)
async def process_pdf_action(message: types.Message, state: FSMContext):
    if message.text == "❌ CANCEL":
        await state.clear()
        return await message.answer("❌ Selection Cancelled.", reply_markup=get_pdf_menu())
    
    if message.text == "📝 EDIT NAME":
        await state.update_data(field="name")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ <b>Enter New Name:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        
    elif message.text == "🔗 EDIT LINK":
        await state.update_data(field="link")
        await state.set_state(PDFStates.waiting_for_edit_value)
        await message.answer("⌨️ <b>Enter New Link:</b>", reply_markup=get_cancel_keyboard(), parse_mode="HTML")
        
    elif message.text == "🗑️ DELETE":
        # Transition to delete confirm
        data = await state.get_data()
        await state.update_data(delete_id=data['edit_id']) # Reuse ID
        
        kb = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="✅ CONFIRM DELETE"), KeyboardButton(text="❌ CANCEL")]
        ], resize_keyboard=True)
        
        await state.set_state(PDFStates.waiting_for_delete_confirm)
        await message.answer(
            f"⚠️ <b>CONFIRM DELETION</b>\n\nAre you sure you want to delete this PDF?",
            reply_markup=kb,
            parse_mode="HTML"
        )
    else:
         await message.answer("⚠️ Invalid Option. Choose from the buttons.", reply_markup=get_cancel_keyboard())

# ==========================================
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

    # 🔴 SHUTDOWN NOTIFICATION TO OWNER
    try:
        if MASTER_ADMIN_ID and MASTER_ADMIN_ID != 0:
            uptime = now_local() - health_monitor.system_metrics["uptime_start"]
            h = int(uptime.total_seconds() // 3600)
            m = int((uptime.total_seconds() % 3600) // 60)
            await bot.send_message(
                MASTER_ADMIN_ID,
                f"🔴 <b>BOT 9 — OFFLINE</b>\n\n"
                f"<b>Status:</b> Shutting down\n"
                f"<b>Uptime:</b> {h}h {m}m\n"
                f"<b>Errors:</b> {health_monitor.error_count}\n"
                f"<b>Warnings:</b> {health_monitor.warning_count}\n\n"
                f"<b>Time:</b> {now_local().strftime('%B %d, %Y — %I:%M:%S %p')}\n\n"
                f"_Bot 9 has stopped. It will resume when restarted._",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Failed to send shutdown notification: {e}")

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
                "🚀 <b>BOT 9 ENTERPRISE EDITION</b>\n\n"
                "✅ <b>Status:</b> ONLINE\n"
                f"📅 <b>Started:</b> {now_local().strftime('%B %d, %Y %I:%M %p')}\n\n"
                "🔧 <b>Active Systems:</b>\n"
                "├ Auto-Healer: ✅ Active\n"
                "├ Health Monitor: ✅ Active\n"
                "├ Daily Reports: ✅ Active\n"
                "├ State Persistence: ✅ Active\n"
                "└ Auto Backup: ✅ Active\n\n"
                "🛡️ <b>Security:</b> Enterprise Level\n"
                "⚡ <b>Ready to Handle:</b> Millions of Requests\n\n"
                "All systems operational! 🎯"
            )
            await bot.send_message(MASTER_ADMIN_ID, startup_msg, parse_mode="HTML")
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
