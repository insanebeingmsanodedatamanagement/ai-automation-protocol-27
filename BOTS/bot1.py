import asyncio
import logging
import random
import html
import threading
import time
import sys
from datetime import timedelta
from aiohttp import web
import pymongo
import os
import io
import pytz
from datetime import datetime

# Load environment variables from .env file
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, CommandObject, ChatMemberUpdatedFilter, LEAVE_TRANSITION, JOIN_TRANSITION, Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile, ChatMemberUpdated
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ==========================================
# 📜 TERMS & CONDITIONS STATE
# ==========================================
class TermsState(StatesGroup):
    waiting_for_acceptance = State()
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramConflictError, TelegramForbiddenError
from aiogram.client.session.aiohttp import AiohttpSession

# ==========================================
# ⚡ CONFIGURATION (GHOST PROTOCOL)
# ==========================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
ADMIN_LOG_CHANNEL = os.getenv("ADMIN_LOG_CHANNEL")
REVIEW_LOG_CHANNEL = os.getenv("REVIEW_LOG_CHANNEL")
SUPPORT_CHANNEL_ID = os.getenv("SUPPORT_CHANNEL_ID")
BAN_CHANNEL_ID = -1003575487367  # Ban notifications channel (legacy)
BAN_REPORT_CHANNEL_ID = int(os.getenv("BAN_REPORT_CHANNEL_ID", -1003575487367))  # New ban report channel
APPEAL_CHANNEL_ID = int(os.getenv("APPEAL_CHANNEL_ID", -1003354981499))  # Ban appeal channel

try:
    OWNER_ID = int(os.getenv("OWNER_ID", 0))
    CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
except (TypeError, ValueError):
    OWNER_ID = 0
    CHANNEL_ID = 0

CHANNEL_LINK = os.getenv("CHANNEL_LINK") 
BOT_USERNAME = os.getenv("BOT_USERNAME")
YOUTUBE_LINK = os.getenv("YOUTUBE_LINK") 
INSTAGRAM_LINK = os.getenv("INSTAGRAM_LINK") 

if not BOT_TOKEN or not MONGO_URI or not OWNER_ID:
    print("❌ CRITICAL ERROR: Environment variables missing!")
    sys.exit(1)

IST = pytz.timezone('Asia/Kolkata')

# ==========================================
# ⏱️ TIMING CONFIGURATION - EXACT VALUES
# ==========================================
# All timings are precisely defined for accurate cooldowns and spam protection

# Review System Timing
REVIEW_COOLDOWN_DAYS = 7  # Default: Users can review once every 7 days (1 week)
REVIEW_COOLDOWN_SECONDS = 604800  # Default: Exactly 7 days in seconds (7 * 24 * 60 * 60)

def get_review_cooldown_days():
    """Get current review cooldown days from settings (dynamically updated by admin)"""
    try:
        cooldown_setting = col_settings.find_one({"setting": "review_cooldown_days"})
        return cooldown_setting.get("value", 7) if cooldown_setting else 7
    except:
        return 7  # Fallback to default

def get_review_cooldown_seconds():
    """Get review cooldown in seconds (dynamically calculated from settings)"""
    return get_review_cooldown_days() * 24 * 60 * 60

# Anti-Spam Timing
ANTI_SPAM_FREEZE_SECONDS = 2.0  # Exactly 2.0 seconds freeze between review button clicks

# Rate Limiting Timing (defined below near usage)
# - RATE_LIMIT_WINDOW_SECONDS = 60 (exactly 60 seconds = 1 minute window)
# - MAX_REQUESTS_PER_MINUTE = 10 (maximum 10 requests per 60-second window)

# Spam Cooldown Levels (defined below)
# - Level 1: 30 seconds
# - Level 2: 60 seconds (1 minute)
# - Level 3: 180 seconds (3 minutes)
# - Level 4: 300 seconds (5 minutes)
# - Level 5+: 600 seconds (10 minutes)

# Cache Timing (defined below)
# - CACHE_DURATION = 300 seconds (5 minutes)
# ==========================================

# 🧠 PSYCHOLOGY: ALPHA CROSS-PROMOTION TITLES
# (Restored from your clickbait data but used as dynamic sync triggers)
# 🧠 PSYCHOLOGY: 50 SUPREME ALPHA TITLES
ALPHA_TITLES = [
    "🔥 **This Strategy is Breaking the Internet.** Check out more videos now! Hurry up!",
    "🚀 **How to 10x Your Results Overnight.** Check out more videos now! Hurry up!",
    "🤫 **The Secret Loophole Nobody Talks About.** Check out more videos now! Hurry up!",
    "⚠️ **URGENT: Watch Before It's Deleted.** Check out more videos now! Hurry up!",
    "💎 **Found: The 'Cheat Code' for Success.** Check out more videos now! Hurry up!",
    "🤯 **I Can't Believe This Actually Works.** Check out more videos now! Hurry up!",
    "🔓 **Unlocking The Forbidden Strategy.** Check out more videos now! Hurry up!",
    "🕵️ **Leaked: What The Pros Are Using.** Check out more videos now! Hurry up!",
    "👑 **Become The Authority In Your Niche.** Check out more videos now! Hurry up!",
    "🎯 **The Exact Blueprint I Used.** Check out more videos now! Hurry up!",
    "💸 **The Passive Income Machine Revealed.** Check out more videos now! Hurry up!",
    "📉 **Stop Losing Money with Old Methods.** Check out more videos now! Hurry up!",
    "⚡ **Zero to Viral in 24 Hours.** Check out more videos now! Hurry up!",
    "🔮 **The Future of Automation is Here.** Check out more videos now! Hurry up!",
    "🛠️ **The Only Tool You Will Ever Need.** Check out more videos now! Hurry up!",
    "🚫 **Don't Ignore This Wealth Warning.** Check out more videos now! Hurry up!",
    "🌪️ **Industry Disruptor: The New Meta.** Check out more videos now! Hurry up!",
    "💡 **Genius Hack for Content Creators.** Check out more videos now! Hurry up!",
    "🏆 **Join the Top 1% with This Secret.** Check out more videos now! Hurry up!",
    "🛑 **Stop Scrolling and Watch This.** Check out more videos now! Hurry up!",
    "🧠 **Psychological Triggers for Sales.** Check out more videos now! Hurry up!",
    "🎁 **Free Value Explosion Inside.** Check out more videos now! Hurry up!",
    "🔥 **Hot: The Trending Viral Loop.** Check out more videos now! Hurry up!",
    "🗝️ **Key to the MSANode Kingdom.** Check out more videos now! Hurry up!",
    "📢 **Major Announcement: The Shift.** Check out more videos now! Hurry up!",
    "🧪 **Proven Results: No Fluff.** Check out more videos now! Hurry up!",
    "🌍 **Global Operatives Are Scaling.** Check out more videos now! Hurry up!",
    "🧩 **The Missing Piece of Your Empire.** Check out more videos now! Hurry up!",
    "⚡ **Fast-Track Your Breakthrough.** Check out more videos now! Hurry up!",
    "🌑 **The Dark Side of Digital Success.** Check out more videos now! Hurry up!",
    "☀️ **A New Era of Automation.** Check out more videos now! Hurry up!",
    "🎭 **The Truth Behind Big Profits.** Check out more videos now! Hurry up!",
    "🎰 **Winning Every Time with Logic.** Check out more videos now! Hurry up!",
    "🧨 **Explosive Growth Formula.** Check out more videos now! Hurry up!",
    "🌊 **The Wave of Success is Coming.** Check out more videos now! Hurry up!",
    "🏔️ **Peak Performance Masterclass.** Check out more videos now! Hurry up!",
    "⚓ **Anchor Your Wealth with This.** Check out more videos now! Hurry up!",
    "🛰️ **High-Level Intelligence Report.** Check out more videos now! Hurry up!",
    "🛡️ **Protect Your Business from Failure.** Check out more videos now! Hurry up!",
    "⚔️ **The Competitive Edge You Need.** Check out more videos now! Hurry up!",
    "🌋 **Erupt into the Mainstream.** Check out more videos now! Hurry up!",
    "💎 **Polishing Your Skills to Diamond.** Check out more videos now! Hurry up!",
    "🛸 **Out of This World Results.** Check out more videos now! Hurry up!",
    "🌌 **Infinite Scaling Possibilities.** Check out more videos now! Hurry up!",
    "🧬 **The DNA of a Successful Bot.** Check out more videos now! Hurry up!",
    "🔋 **Recharge Your Motivation Now.** Check out more videos now! Hurry up!",
    "📡 **Transmission Signal: SUCCESS.** Check out more videos now! Hurry up!",
    "🌠 **Shoot for the Stars with This.** Check out more videos now! Hurry up!",
    "🔥 **Burn the Competition Away.** Check out more videos now! Hurry up!",
    "🏰 **Build an Unbreakable Empire.** Check out more videos now! Hurry up!"
]

# 🧠 PSYCHOLOGY: 20 SUPREME PURCHASE-DRIVEN AI TRIGGERS
# Persona: Master Sadiq’s Private Stack / Focus: ROI & Commission Conversion
# Strategy: Framing the AI tool as the "Mandatory Engine" to the "PDF Blueprint"
AFFILIATE_TRIGGERS = [
    "🤫 **COMMANDER'S SECRET:** The PDF I sent is the map, but this AI is the actual engine. Without it, you are just walking. It costs a few dollars to activate, but it earns that back in the first hour of automation. Click below and claim it fast before it's gone!",
    "⚠️ **ROI ALERT:** I manually verified this AI tool. Don't look at the price as a cost—it’s a 'Business Asset' that replaces 3 employees. If you want my results, you need my engine. Click below and claim it fast before it's gone!",
    "💡 **INSIDER ALPHA:** You can't scale a 6-figure empire with free tools. I personally pay for this AI because it handles the server-side overrides that manual work can't touch. Click below and claim it fast before it's gone!",
    "🛡️ **THE INFRASTRUCTURE:** This AI tool is exactly why my bot accounts never get banned. It’s worth every penny to protect your business from being wiped out. Click below and claim it fast before it's gone!",
    "🔥 **THE MISSING PIECE:** Most recruits hesitate to invest in the software and they fail. Don't be 'most people'. Secure the engine that makes the blueprint actually work. Click below and claim it fast before it's gone!",
    "🎁 **MSANODE OVERRIDE:** I secured a massive discount for our inner circle. You save 50% on the purchase today, and the AI starts printing your content immediately. Click below and claim it fast before it's gone!",
    "⚡ **HYPER-DRIVE:** Stop working for the competition. Purchase this AI engine once and let it run your life on complete autopilot while you sleep. Click below and claim it fast before it's gone!",
    "📈 **VERIFIED REVENUE:** My top operatives who purchased this AI reached profit 10x faster than those trying to do it for free. Statistics don't lie—buy the engine. Click below and claim it fast before it's gone!",
    "🎯 **SURGICAL EXECUTION:** Guessing is the most expensive mistake you can make. Buying this AI tool is a surgical move to guarantee your breakthrough. Click below and claim it fast before it's gone!",
    "🗝️ **THE MASTER KEY:** Without this tool, you're just reading a document. With it, you're building a dynasty. Own the engine that I use daily. Click below and claim it fast before it's gone!",
    "🛑 **URGENT:** The private gateway to this AI cloud is closing soon. Secure your lifetime license now before the public price doubles. Click below and claim it fast before it's gone!",
    "🧪 **ELITE PERFORMANCE:** I tested this AI-voice cloud myself. It’s the highest ROI asset in my entire stack. It pays for itself in 48 hours. Click below and claim it fast before it's gone!",
    "🕵️ **THE 0.1% EDGE:** This AI operates under the radar using private residential proxies. It’s the only tool I trust to scale without being noticed. Click below and claim it fast before it's gone!",
    "💸 **ASSET ACQUIRED:** Think of this purchase as an investment in your future. It has already paid for itself for every operative who activated it today. Click below and claim it fast before it's gone!",
    "🌪️ **META DISRUPTOR:** The game has shifted to paid AI automation. This software is your only way to stay relevant and profitable in 2026. Click below and claim it fast before it's gone!",
    "🔓 **FORBIDDEN GATEWAY:** This link bypasses the standard $97/mo fee for a one-time purchase. It’s a private MSANode-only deal. Click below and claim it fast before it's gone!",
    "🛰️ **SATELLITE INTEL:** My tech contacts leaked this AI tool to me. It automatically scrapes and recreates the highest-performing content while you relax. Click below and claim it fast before it's gone!",
    "⚔️ **COMPETITIVE KILLER:** While others are struggling with manual labor, this AI is running your empire 24/7. Don't let others out-work you. Click below and claim it fast before it's gone!",
    "🧬 **SUCCESS DNA:** This AI tool is the foundation of my wealth strategy. Without purchasing it, the system simply does not scale. Click below and claim it fast before it's gone!",
    "🌑 **THE DARK HORSE:** This AI multiplier is my hidden weapon. You’ve seen my viral results; this software is the engine that drives them. Click below and claim it fast before it's gone!"
]

# ==========================================
# 🛠 SYSTEM SETUP (IRON DOME)
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress MongoDB background task errors (network timeouts are expected)
logging.getLogger('pymongo').setLevel(logging.CRITICAL)  # Only show critical errors
logging.getLogger('pymongo.serverSelection').setLevel(logging.CRITICAL)
logging.getLogger('pymongo.topology').setLevel(logging.CRITICAL)
logging.getLogger('pymongo.connection').setLevel(logging.CRITICAL)

# Configure aiohttp session with proper timeouts for Windows
# Note: Using higher timeout value to prevent Windows semaphore timeout errors
session = AiohttpSession(
    timeout=120.0  # 120 seconds total timeout (Windows needs higher values)
)

bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher(storage=MemoryStorage())

# Manager Bot connection (for admin notifications)
MANAGER_BOT_TOKEN = os.getenv("MANAGER_BOT_TOKEN")
manager_bot = Bot(token=MANAGER_BOT_TOKEN, session=session) if MANAGER_BOT_TOKEN else None

# ==========================================
# 📊 ENTERPRISE HEALTH MONITORING
# ==========================================

health_metrics = {
    'start_time': time.time(),
    'total_requests': 0,
    'failed_requests': 0,
    'db_operations': 0,
    'db_failures': 0,
    'last_health_check': time.time()
}

# Admin notification helper
async def notify_admins(message_text: str):
    """Send notification to owner about new reviews/tickets"""
    if manager_bot and OWNER_ID:
        try:
            await manager_bot.send_message(OWNER_ID, message_text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed to notify admin: {e}")

# --- RENDER PORT BINDER ---
async def handle_health(request):
    return web.Response(text="MSANODE GATEWAY HUB IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        
        # Try to find an available port starting from 10000
        port = int(os.environ.get("PORT", 10000))
        max_attempts = 10
        for attempt in range(max_attempts):
            try:
                web.run_app(app, host='0.0.0.0', port=port + attempt, handle_signals=False, print=None)
                break
            except OSError as port_error:
                if attempt == max_attempts - 1:
                    # Silently fail after all attempts - not critical for bot operation
                    pass
                continue
    except Exception:
        # Health server is optional - silently skip if it fails
        pass

# --- BAN FEATURE CHECKER ---
def is_user_banned_from_feature(user_id: int, feature_name: str) -> bool:
    """Check if a specific feature is banned for user by user_id."""
    try:
        # Get user data from banned collection
        ban_record = col_banned.find_one({"user_id": str(user_id)})
        if not ban_record:
            return False
        
        banned_features = ban_record.get("banned_features", [])
        return feature_name in banned_features
    except Exception as e:
        print(f"Error checking feature ban for user {user_id}: {e}")
        return False

def is_feature_banned(user_data, feature_name):
    """Check if a specific feature is banned for a user."""
    try:
        if user_data is None:
            return False
        
        # Check if user is permanently banned
        is_banned = user_data.get('is_banned', False)
        if is_banned:
            # Check if it's a temporary ban and if it has expired
            temp_ban_end = user_data.get('temp_ban_end')
            if temp_ban_end:
                ist_timezone = pytz.timezone('Asia/Kolkata')
                current_time = datetime.now(ist_timezone)
                if temp_ban_end > current_time:
                    # Temp ban is still active, check banned features
                    banned_features = user_data.get('banned_features', [])
                    return feature_name in banned_features
                else:
                    # Temp ban expired, automatically unban
                    col_users.update_one(
                        {'user_id': user_data['user_id']},
                        {
                            '$unset': {
                                'is_banned': 1,
                                'ban_reason': 1,
                                'temp_ban_end': 1,
                                'banned_features': 1,
                                'ban_timestamp': 1
                            }
                        }
                    )
                    return False
            else:
                # Permanent ban, check banned features
                banned_features = user_data.get('banned_features', [])
                return feature_name in banned_features
        
        return False
        
    except Exception as e:
        print(f"Error checking feature ban: {e}")
        return False

async def send_feature_ban_message(message: types.Message, feature_name: str, user_data: dict):
    """Send appropriate ban message for a specific feature."""
    try:
        ist_timezone = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist_timezone)
        
        ban_reason = user_data.get('ban_reason', 'Violation of terms')
        temp_ban_end = user_data.get('temp_ban_end')
        
        if temp_ban_end and temp_ban_end > current_time:
            # Temporary ban
            time_left = temp_ban_end - current_time
            days = time_left.days
            hours, remainder = divmod(time_left.seconds, 3600)
            minutes, _ = divmod(remainder, 60)
            
            ban_text = f"🚫 **{feature_name.title()} Feature Temporarily Banned**\n\n"
            ban_text += f"**Reason:** {ban_reason}\n\n"
            ban_text += f"**Time Remaining:** {days} days, {hours} hours, {minutes} minutes\n\n"
            
            # Check if other features are unbanned
            banned_features = user_data.get('banned_features', [])
            all_features = ['downloads', 'reviews', 'support', 'search']
            unbanned_features = [f for f in all_features if f not in banned_features]
            
            if unbanned_features:
                ban_text += f"**✅ Available Features:** {', '.join(unbanned_features).title()}\n\n"
            
            ban_text += "Please wait for the ban period to end or contact support."
        else:
            # Permanent feature ban
            ban_text = f"🚫 **{feature_name.title()} Feature Permanently Banned**\n\n"
            ban_text += f"**Reason:** {ban_reason}\n\n"
            ban_text += "Contact support for more information."
        
        await message.answer(ban_text, parse_mode="Markdown")
        
    except Exception as e:
        await message.answer(f"❌ {feature_name.title()} feature is currently unavailable.")
        print(f"Error sending feature ban message: {e}")

# --- MONGODB CONNECTION (ENTERPRISE-SCALE) ---
try:
    client = pymongo.MongoClient(
        MONGO_URI,
        maxPoolSize=100,  # 🏢 ENTERPRISE: Handle 100 concurrent connections
        minPoolSize=10,   # 🏢 ENTERPRISE: Always maintain 10 warm connections
        maxIdleTimeMS=45000,  # Exactly 45 seconds (45,000ms) idle timeout for better connection reuse
        serverSelectionTimeoutMS=10000,  # Exactly 10 seconds (10,000ms) server selection timeout
        connectTimeoutMS=20000,  # Exactly 20 seconds (20,000ms) connection timeout
        socketTimeoutMS=20000,   # Exactly 20 seconds (20,000ms) socket timeout
        retryWrites=True,
        retryReads=True,
        w='majority',
        journal=True,  # 🏢 ENTERPRISE: Ensure writes are journaled for data safety
        tlsAllowInvalidCertificates=False,  # Ensure proper SSL
        directConnection=False  # Use replica set routing
    )
    db = client["MSANodeDB"]
    col_users = db["user_logs"]
    col_active = db["active_content"]
    col_viral = db["viral_videos"]
    col_reels = db["viral_reels"]
    col_settings = db["settings"] 
    col_banned = db["banned_users"]
    col_ban_history = db["ban_history"]
    col_appeals = db["ban_appeals"]  # New collection for ban appeals
    col_reviews = db["user_reviews"]
    col_user_counter = db["user_counter"]  # For tracking next MSA ID
    col_terms = db["terms_acceptance"]  # For tracking terms & conditions acceptance
    
    print("[OK] GATEWAY DATA CORE: CONNECTED")
    
    # Initialize counter if not exists
    if not col_user_counter.find_one({"_id": "msa_counter"}):
        col_user_counter.insert_one({"_id": "msa_counter", "current": 0})
        print("[OK] USER ID COUNTER INITIALIZED")
    
    # Assign IDs to existing users without msa_id (using gap-filling logic)
    existing_users_without_id = col_users.find({"msa_id": {"$exists": False}})
    count_assigned = 0
    
    # First, get all existing MSA IDs to find gaps
    existing_users_with_id = col_users.find({"msa_id": {"$exists": True}}, {"msa_id": 1})
    used_ids = set()
    for user in existing_users_with_id:
        msa_id = user.get("msa_id", "")
        if msa_id and msa_id.startswith("MSA"):
            try:
                num = int(msa_id.replace("MSA", ""))
                used_ids.add(num)
            except ValueError:
                continue
    
    # Assign IDs filling gaps first
    next_id = 1
    for user in existing_users_without_id:
        # Find next available ID
        while next_id in used_ids:
            next_id += 1
        
        new_id = f"MSA{next_id}"
        col_users.update_one(
            {"_id": user["_id"]},
            {"$set": {"msa_id": new_id}}
        )
        used_ids.add(next_id)
        count_assigned += 1
        next_id += 1
    
    if count_assigned > 0:
        print(f"[OK] ASSIGNED {count_assigned} MSA IDs (GAP-FILLING ENABLED)")
        # Update counter to highest ID
        if used_ids:
            col_user_counter.update_one(
                {"_id": "msa_counter"},
                {"$set": {"current": max(used_ids)}}
            )
    
    # 🏢 ENTERPRISE: Create compound indexes for optimal performance with millions of records
    # Check existing indexes first to avoid conflicts
    def safe_create_index(collection, keys, **kwargs):
        """Create index only if it doesn't exist with same specs"""
        try:
            existing_indexes = collection.index_information()
            # Generate expected index name
            if isinstance(keys, list):
                index_name = "_".join([f"{k}_{v}" for k, v in keys])
            else:
                index_name = f"{keys}_1"
            
            # Check if index already exists
            if index_name not in existing_indexes:
                collection.create_index(keys, **kwargs)
                return True
            return False  # Already exists
        except Exception as e:
            # Silently ignore if index exists with different options
            if "IndexOptionsConflict" in str(e) or "IndexKeySpecsConflict" in str(e):
                return False
            raise e
    
    try:
        created_count = 0
        created_count += 1 if safe_create_index(col_users, [("user_id", pymongo.ASCENDING)], unique=True, background=True) else 0
        created_count += 1 if safe_create_index(col_users, [("msa_id", pymongo.ASCENDING)], unique=True, sparse=True, background=True) else 0
        created_count += 1 if safe_create_index(col_users, [("status", pymongo.ASCENDING), ("last_active", pymongo.DESCENDING)], background=True) else 0
        created_count += 1 if safe_create_index(col_users, [("source", pymongo.ASCENDING), ("status", pymongo.ASCENDING)], background=True) else 0
        created_count += 1 if safe_create_index(col_banned, [("user_id", pymongo.ASCENDING)], background=True) else 0
        created_count += 1 if safe_create_index(col_banned, [("ban_type", pymongo.ASCENDING), ("ban_until", pymongo.ASCENDING)], background=True) else 0
        created_count += 1 if safe_create_index(col_reviews, [("user_id", pymongo.ASCENDING), ("timestamp", pymongo.DESCENDING)], background=True) else 0
        created_count += 1 if safe_create_index(col_appeals, [("user_id", pymongo.ASCENDING), ("status", pymongo.ASCENDING)], background=True) else 0
        created_count += 1 if safe_create_index(col_terms, [("user_id", pymongo.ASCENDING)], unique=True, background=True) else 0
        
        if created_count > 0:
            print(f"[OK] CREATED {created_count} NEW ENTERPRISE INDEXES")
        else:
            print("[OK] ENTERPRISE INDEXES ALREADY EXIST - OPTIMIZED FOR LAKHS OF USERS")
    except Exception as idx_err:
        print(f"[WARN] Index configuration issue: {idx_err}")
    
except Exception as e:
    print(f"[ERROR] DATABASE OFFLINE: {e}")
    sys.exit(1)

# ==========================================
# 🏢 ENTERPRISE CIRCUIT BREAKER PATTERN
# ==========================================
db_circuit_breaker = {
    "failure_count": 0,
    "last_failure_time": 0,
    "is_open": False,
    "open_until": 0
}
CIRCUIT_BREAKER_THRESHOLD = 5  # Open circuit after 5 consecutive failures
CIRCUIT_BREAKER_TIMEOUT = 30  # Keep circuit open for exactly 30 seconds

def check_db_circuit():
    """Check if database circuit breaker allows operations"""
    now = time.time()
    if db_circuit_breaker["is_open"]:
        if now >= db_circuit_breaker["open_until"]:
            # Reset circuit breaker after timeout
            db_circuit_breaker["is_open"] = False
            db_circuit_breaker["failure_count"] = 0
            print("[OK] CIRCUIT BREAKER RESET - ATTEMPTING RECONNECTION")
            return True
        return False
    return True

def record_db_failure():
    """Record database failure and potentially open circuit breaker"""
    db_circuit_breaker["failure_count"] += 1
    db_circuit_breaker["last_failure_time"] = time.time()
    
    if db_circuit_breaker["failure_count"] >= CIRCUIT_BREAKER_THRESHOLD:
        db_circuit_breaker["is_open"] = True
        db_circuit_breaker["open_until"] = time.time() + CIRCUIT_BREAKER_TIMEOUT
        print(f"[CRITICAL] CIRCUIT BREAKER OPENED - DB OPERATIONS SUSPENDED FOR {CIRCUIT_BREAKER_TIMEOUT}s")

def record_db_success():
    """Record successful database operation"""
    if db_circuit_breaker["failure_count"] > 0:
        db_circuit_breaker["failure_count"] = max(0, db_circuit_breaker["failure_count"] - 1)

async def safe_db_operation(operation, *args, **kwargs):
    """Execute database operation with circuit breaker protection"""
    if not check_db_circuit():
        raise Exception("Circuit breaker open - database temporarily unavailable")
    
    try:
        result = operation(*args, **kwargs)
        record_db_success()
        return result
    except Exception as e:
        record_db_failure()
        raise e

# ==========================================
# 🏢 ENTERPRISE MEMORY MANAGEMENT
# ==========================================
MAX_CACHE_SIZE = 10000  # Maximum entries in memory caches
CACHE_CLEANUP_AGE = 3600  # Remove entries older than exactly 1 hour (3600 seconds)

def cleanup_memory_caches():
    """Clean up old entries from memory caches to prevent memory bloat with lakhs of users"""
    now = time.time()
    cleanup_count = 0
    
    # Clean user_last_action cache
    for user_id in list(user_last_action.keys()):
        if now - user_last_action[user_id] > CACHE_CLEANUP_AGE:
            del user_last_action[user_id]
            cleanup_count += 1
    
    # Clean start_command_tracker if too large
    if len(start_command_tracker) > MAX_CACHE_SIZE:
        # Keep only recent entries
        sorted_entries = sorted(start_command_tracker.items(), key=lambda x: x[1], reverse=True)
        start_command_tracker.clear()
        start_command_tracker.update(dict(sorted_entries[:MAX_CACHE_SIZE // 2]))
        cleanup_count += len(sorted_entries) - (MAX_CACHE_SIZE // 2)
    
    # Clean sync_cooldown if too large
    if len(sync_cooldown) > MAX_CACHE_SIZE:
        for user_id in list(sync_cooldown.keys()):
            if now - sync_cooldown[user_id] > CACHE_CLEANUP_AGE:
                del sync_cooldown[user_id]
                cleanup_count += 1
    
    if cleanup_count > 0:
        print(f"[OK] MEMORY CLEANUP: REMOVED {cleanup_count} OLD CACHE ENTRIES")

# ==========================================
# 🏢 ENTERPRISE HEALTH MONITORING
# ==========================================
async def enterprise_health_check():
    """Periodic health check for enterprise monitoring"""
    while True:
        try:
            await asyncio.sleep(300)  # Check every exactly 5 minutes (300 seconds)
            
            # Check database connectivity
            try:
                client.admin.command('ping')
                db_status = "✅ HEALTHY"
            except:
                db_status = "❌ UNHEALTHY"
                print("[CRITICAL] DATABASE HEALTH CHECK FAILED")
            
            # Check circuit breaker status
            circuit_status = "🔴 OPEN" if db_circuit_breaker["is_open"] else "🟢 CLOSED"
            
            # Memory cleanup
            cleanup_memory_caches()
            
            # Calculate error rate (from panic protocol)
            now = time.time()
            recent_errors = [t for t in error_timestamps if now - t < 300]  # Last 5 minutes
            error_rate = len(recent_errors) / 300 * 100  # Errors per second * 100
            
            print(f"[HEALTH] DB: {db_status} | Circuit: {circuit_status} | Error Rate: {error_rate:.2f}% | Cache Size: {len(user_last_action)}")
            
            if error_rate > 5:  # More than 5% error rate
                print(f"[WARNING] HIGH ERROR RATE DETECTED: {error_rate:.2f}%")
                
        except Exception as e:
            print(f"[ERROR] Health check failed: {e}")

# ==========================================
# 🚀 SCALABILITY OPTIMIZATIONS
# ==========================================

# Cache system for frequently accessed data
review_count_cache = {"count": 0, "last_updated": None}
CACHE_DURATION = 300  # Exactly 300 seconds (5 minutes) cache duration

def get_cached_review_count():
    """Get total review count with caching to avoid heavy DB queries"""
    now = time.time()
    cache = review_count_cache
    
    # Return cached value if still valid
    if cache["last_updated"] and (now - cache["last_updated"]) < CACHE_DURATION:
        return cache["count"]
    
    # Update cache
    try:
        # Use estimated_document_count for better performance on large collections
        count = col_reviews.estimated_document_count()
        cache["count"] = count
        cache["last_updated"] = now
        return count
    except:
        # Fallback to last known value
        return cache["count"]

def invalidate_review_cache():
    """Invalidate cache when new review is added"""
    review_count_cache["last_updated"] = None

# Global rate limiter per user (prevents system overload)
user_request_tracker = {}  # {user_id: [timestamp1, timestamp2, ...]}
MAX_REQUESTS_PER_MINUTE = 10  # Maximum 10 review attempts per minute per user
RATE_LIMIT_WINDOW_SECONDS = 60  # Exactly 60 seconds (1 minute) rate limit window

def check_rate_limit(user_id: int) -> tuple[bool, str]:
    """Check if user exceeds rate limit. Returns (is_blocked, message)"""
    now = time.time()
    
    # Clean old entries (older than exactly 1 minute = 60 seconds)
    if user_id in user_request_tracker:
        user_request_tracker[user_id] = [
            ts for ts in user_request_tracker[user_id] 
            if now - ts < RATE_LIMIT_WINDOW_SECONDS
        ]
    else:
        user_request_tracker[user_id] = []
    
    # Check request count
    request_count = len(user_request_tracker[user_id])
    
    if request_count >= MAX_REQUESTS_PER_MINUTE:
        return (True, 
            f"🚫 **RATE LIMIT EXCEEDED**\n\n"
            f"⚠️ Too many requests! You've made {request_count} attempts in 1 minute.\n\n"
            f"🛡️ **Anti-Spam Protection:**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Maximum: {MAX_REQUESTS_PER_MINUTE} requests per minute\n"
            f"Wait: {RATE_LIMIT_WINDOW_SECONDS} seconds before trying again\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⏰ Please wait and try again shortly.\n"
            f"🛡️ This protects the MSA NODE system from overload.")
    
    # Track this request
    user_request_tracker[user_id].append(now)
    return (False, "")

# ==========================================
# 🛡️ INTELLIGENCE HELPERS (UNREDUCED)
# ==========================================

# Review FSM States
class ReviewState(StatesGroup):
    waiting_for_stars = State()
    waiting_for_text = State()
    waiting_for_confirmation = State()
    processing = State()

# Customer Support FSM States
class SupportState(StatesGroup):
    selecting_issue = State()  # Selecting from predefined issues
    waiting_for_message = State()  # Typing custom message
    processing = State()  # Anti-spam protection

# Ban Appeal FSM States
class AppealState(StatesGroup):
    waiting_for_appeal_message = State()
    processing = State()  # Anti-spam protection

# Advanced Anti-Spam Protection System
user_last_action = {}  # Track last action timestamp
user_spam_count = {}   # Track spam violations
user_cooldown_until = {}  # Track cooldown end time
sync_cooldown = {}  # Track last sync time for review status updates
user_social_clicks = {}  # Track which social media buttons user clicked {user_id: {'ig': bool, 'yt': bool}}
start_command_tracker = {}  # Track /start command spam {user_id: [timestamps]}
user_support_pending = {}  # Track pending support requests {user_id: {'message': str, 'timestamp': float, 'channel_msg_id': int}}
user_support_clicks = {}  # Track customer support button clicks {user_id: [timestamps]}
user_support_cooldown = {}  # Track cooldown after resolution {user_id: timestamp_when_can_submit_again}
user_support_history = {}  # Track support request history {user_id: [timestamps]}
user_fake_attempts = {}  # Track fake/spam message attempts {user_id: count}
user_template_views = {}  # Track template solution views {user_id: [timestamps]}
user_template_spam = {}  # Track template spam clicks {user_id: count}
user_guide_views = {}  # Track guide section views {user_id: [timestamps]}
user_guide_spam = {}  # Track guide rapid clicking {user_id: count}
error_timestamps = []  # Track error timestamps for health monitoring

# ==========================================
# 🛡️ FINAL SECURITY CONFIGURATION
# ==========================================
# Multi-layer protection system with exact timings

# Support System Configuration - EXACT TIMINGS
SUPPORT_COOLDOWN_AFTER_RESOLVE = 3600  # Exactly 3600 seconds (1 hour) cooldown after resolution
SUPPORT_DAILY_LIMIT = 3  # Maximum 3 support requests per 24 hours
SUPPORT_HISTORY_WINDOW = 86400  # Exactly 86400 seconds (24 hours) for tracking history
TEMPLATE_VIEW_LIMIT = 15  # Maximum template views before warning
TEMPLATE_SPAM_FREEZE = 5  # Freeze after 5 rapid clicks

# Cooldown durations (in seconds) for progressive punishment - EXACT TIMINGS
COOLDOWN_LEVELS = {
    1: 30,    # First offense: exactly 30 seconds
    2: 60,    # Second offense: exactly 60 seconds (1 minute)
    3: 180,   # Third offense: exactly 180 seconds (3 minutes)
    4: 300,   # Fourth offense: exactly 300 seconds (5 minutes)
    5: 600,   # Fifth+ offense: exactly 600 seconds (10 minutes)
}

# Security Limits
MAX_MESSAGE_LENGTH = 4096  # Telegram message limit
MAX_REVIEW_TEXT_LENGTH = 1000  # Maximum review text length
MIN_REVIEW_TEXT_LENGTH = 10  # Minimum review text length
MAX_SUPPORT_MESSAGE_LENGTH = 1500  # Maximum support message length
MAX_USERNAME_LENGTH = 50  # Maximum username display length
MAX_FAILED_ATTEMPTS = 5  # Maximum failed attempts before temporary ban
FAILED_ATTEMPT_WINDOW = 300  # 5 minutes window for failed attempts
GLOBAL_BAN_THRESHOLD = 10  # Permanent ban after 10 violations

# IP/User tracking for advanced security
user_failed_attempts = {}  # Track failed/invalid attempts {user_id: count}
user_violation_count = {}  # Track total violations {user_id: count}
user_last_violation = {}  # Track last violation time {user_id: timestamp}
# ==========================================

# Support button spam protection
SUPPORT_CLICK_WINDOW = 10  # seconds
SUPPORT_MAX_CLICKS = 3  # max clicks in window before warning

# Guide system spam protection
GUIDE_VIEW_LIMIT = 20  # Maximum guide views before warning
GUIDE_SPAM_FREEZE = 5  # Freeze after 5 rapid clicks
GUIDE_CLICK_WINDOW = 10  # seconds for rapid click detection
SUPPORT_BAN_CLICKS = 5  # clicks to trigger permanent ban

# ==========================================
# 🛑 THE TOTAL BLACKLIST (MAXIMUM PROTECTION)
# ==========================================
import re

# ==========================================
# 🛑 THE NUCLEAR BLACKLIST (TIER-6 SECURITY)
# ==========================================
SPAM_KEYWORDS = {
    # --- Greetings & Casual Noise (English & Romanized Hindi/Urdu) ---
    'hi', 'hello', 'hey', 'hii', 'hiii', 'helloo', 'helo', 'hellow', 'hlo', 'hlw',
    'yo', 'sup', 'wassup', 'watsup', 'yoo', 'yooo', 'heyya', 'hiyah', 'hola', 'salam',
    'namaste', 'morning', 'night', 'evening', 'buddy', 'friend', 'sir', 'bro', 'bruh',
    'bruv', 'dude', 'man', 'guys', 'everyone', 'bhai', 'bhaiya', 'yaar', 'yr', 'ji',
    
    # --- Standalone Gratitude (Belongs in Reviews) ---
    'thanks', 'thx', 'thank', 'thankyou', 'tq', 'ty', 'thnx', 'thnk', 'appreciate',
    'grateful', 'bless', 'blessing', 'god bless', 'respect', 'tysm', 'tyvm', 'shukriya',
    
    # --- Testing & Probing ---
    'test', 'testing', 'tests', 'tst', 'check', 'checking', 'chk', 'chck', 'live',
    'active', 'online', 'work', 'working', 'works', 'does it work', 'are you there',
    'u there', 'anybody', 'anyone', 'hello there', 'hi there', 'ping', 'pong', 'echo',
    
    # --- Nonsense & Emotional Fillers ---
    'lol', 'lmao', 'haha', 'hehe', 'lmfao', 'rofl', 'xd', 'wow', 'cool', 'nice',
    'great', 'amazing', 'hmm', 'hmmm', 'huh', 'eh', 'oh', 'woww', 'pff', 'meh',
    'sad', 'happy', 'good', 'bad', 'okies', 'okie', 'yah', 'yeah', 'yea', 'mast',
    
    # --- Single/Double Letter Noise ---
    'k', 'kk', 'done', 'yes', 'no', 'yep', 'nope', 'ya', 'nah', 'ok', 'okay',
    'wait', 'stop', 'go', 'stfu', 'pls', 'plz', 'please', 'kindly', 'suno',
    
    # --- Identity & AI Baiting ---
    'bot', 'ai', 'robot', 'are you real', 'who are you', 'are you bot', 'gemini',
    'gpt', 'chatgpt', 'openai', 'system', 'admin', 'owner', 'manager', 'creator',
    'developer', 'who made you', 'how do you work', 'fake bot', 'scam', 'fraud',
    
    # --- Vague Demands (No Context) ---
    'help', 'helpme', 'help me', 'fast', 'urgent', 'now', 'today', 'immediately',
    'asap', 'hurry', 'quickly', 'send', 'give', 'show', 'tell', 'want', 'need',
    'i need', 'give me', 'send me', 'send link', 'open', 'unlock', 'dikhao', 'do',
    
    # --- Indecisive & Vague ---
    'random', 'anything', 'idk', 'nothing', 'whatever', 'dunno', 'maybe', 'perhaps',
    'idrc', 'something', 'just', 'only', 'mere', 'basically'
}
async def smart_protection_check(text):
    """Tier-6 Heuristic Analysis for Message Integrity."""
    t = text.lower().strip()
    
    # A. Length Barrier (Minimum 4 words for technical reports)
    if len(t.split()) < 4:
        return True, "Transmission too short. Minimum 4 words required for technical audit."

    # B. Repetitive Character Detection (e.g., "heyyyyyy", "!!!!!!!!!")
    if re.search(r'(.)\1{3,}', t):
        return True, "Pattern violation: Excessive character repetition detected."

    # C. Emoji-Only or Excessive Emoji Check
    emoji_count = len(re.findall(r'[^\w\s,.]', t))
    if emoji_count > 3:
        return True, "Pattern violation: Excessive symbolic noise (emojis) detected."

    # D. All-Caps Aggression
    if text.isupper() and len(text) > 5:
        return True, "Protocol violation: High-decibel transmission (All-Caps) detected."

    # E. Keyword Blacklist Check
    words = t.split()
    if any(word in SPAM_KEYWORDS for word in words):
        return True, "Transmission contains restricted non-technical keywords."

    return False, ""
def is_fake_support_message(text: str) -> tuple[bool, str]:
    """Check if message is fake/spam. Returns (is_fake, reason)"""
    text_lower = text.lower().strip()
    
    # Check if entire message is just blacklisted keyword(s)
    words = text_lower.split()
    if len(words) <= 3:  # Short messages are suspicious
        # Check if all words are spam keywords
        if all(word in SPAM_KEYWORDS for word in words):
            return (True, "Message contains only greeting/test words without actual issue description")
    
    # Check for exact match with common spam phrases
    for phrase in SPAM_KEYWORDS:
        if text_lower == phrase:
            return (True, f"Message is just '{phrase}' without describing any issue")
    
    # Count meaningful words (exclude very common words)
    common_fillers = {'the', 'a', 'an', 'is', 'am', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'should', 'could', 'can', 'may', 'might', 'must', 'i', 'you', 'we', 'they', 'he', 'she', 'it', 'my', 'your', 'our', 'their', 'this', 'that', 'these', 'those'}
    meaningful_words = [w for w in words if len(w) > 2 and w not in common_fillers and w not in SPAM_KEYWORDS]
    
    # If less than 3 meaningful words, likely fake
    if len(meaningful_words) < 3:
        return (True, "Message does not contain enough meaningful information about the issue")
    
    # Check if message is too generic
    generic_starters = ['hi', 'hello', 'hey', 'test']
    if len(words) <= 5 and any(text_lower.startswith(starter) for starter in generic_starters):
        return (True, "Message appears to be a test or greeting without actual issue")
    
    return (False, "")

def get_cooldown_duration(spam_count: int) -> int:
    """Get cooldown duration based on spam count"""
    if spam_count >= 5:
        return COOLDOWN_LEVELS[5]
    return COOLDOWN_LEVELS.get(spam_count, 30)

def check_support_eligibility(user_id: str) -> tuple[bool, str]:
    """Check if user can submit support request. Returns (can_submit, reason_if_blocked)"""
    current_time = time.time()
    
    # Check cooldown after resolution
    if user_id in user_support_cooldown:
        cooldown_end = user_support_cooldown[user_id]
        if current_time < cooldown_end:
            remaining = int(cooldown_end - current_time)
            time_str = format_time_remaining(remaining)
            return (False, 
                f"⏰ **COOLDOWN ACTIVE**\n\n"
                f"🛡️ You can submit a new support request in:\n"
                f"⏱️ **{time_str}**\n\n"
                f"💡 This cooldown prevents spam and ensures quality support.\n\n"
                f"⚠️ Reason: Recent support request was resolved\n"
                f"🔄 Cooldown: {SUPPORT_COOLDOWN_AFTER_RESOLVE // 60} minutes after resolution")
    
    # Check daily limit
    if user_id not in user_support_history:
        user_support_history[user_id] = []
    
    # Clean old history (older than 24 hours)
    user_support_history[user_id] = [
        ts for ts in user_support_history[user_id]
        if current_time - ts < SUPPORT_HISTORY_WINDOW
    ]
    
    request_count = len(user_support_history[user_id])
    if request_count >= SUPPORT_DAILY_LIMIT:
        oldest_request = min(user_support_history[user_id])
        reset_time = oldest_request + SUPPORT_HISTORY_WINDOW
        remaining = int(reset_time - current_time)
        hours = remaining // 3600
        minutes = (remaining % 3600) // 60
        
        return (False,
            f"🚫 **DAILY LIMIT REACHED**\n\n"
            f"⚠️ You've reached the maximum support requests\n"
            f"📊 Limit: {SUPPORT_DAILY_LIMIT} requests per 24 hours\n"
            f"📈 Current: {request_count}/{SUPPORT_DAILY_LIMIT}\n\n"
            f"⏰ **Resets in:** {hours}h {minutes}m\n\n"
            f"💡 This limit prevents spam and ensures quality support.\n"
            f"🛡️ Legitimate issues are prioritized.")
    
    return (True, "")

def format_time_remaining(seconds: int) -> str:
    """Format seconds into readable time string"""
    if seconds >= 60:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs}s" if secs > 0 else f"{minutes}m"
    return f"{seconds}s"

def check_review_cooldown(user_id: str) -> tuple[bool, int, str]:
    """Check if user can submit a review based on last review date. Returns (is_blocked, remaining_seconds, message)"""
    try:
        # Find user's last review
        last_review = col_reviews.find_one(
            {"user_id": user_id},
            sort=[("timestamp", -1)]  # Get most recent review
        )
        
        if not last_review:
            return (False, 0, "")  # No previous review, allow
        
        # Check if admin has reset cooldown for this user
        if last_review.get("cooldown_reset", False):
            return (False, 0, "")  # Cooldown was reset, allow review
        
        # Calculate time since last review
        last_review_time = last_review.get("timestamp")
        if not last_review_time:
            return (False, 0, "")
        
        # Ensure last_review_time is timezone-aware (convert if naive)
        if last_review_time.tzinfo is None:
            last_review_time = IST.localize(last_review_time)
        
        now_ist = datetime.now(IST)
        time_diff = now_ist - last_review_time
        
        # Check if cooldown period has passed (dynamic cooldown from settings)
        elapsed_seconds = time_diff.total_seconds()
        cooldown_seconds = get_review_cooldown_seconds()
        
        if elapsed_seconds < cooldown_seconds:
            # Still in cooldown - calculate exact remaining time
            remaining_seconds = int(cooldown_seconds - elapsed_seconds + 0.5)  # Round to nearest second
            
            # Calculate days, hours, minutes remaining
            days_left = remaining_seconds // (24 * 60 * 60)
            hours_left = (remaining_seconds % (24 * 60 * 60)) // (60 * 60)
            minutes_left = (remaining_seconds % (60 * 60)) // 60
            
            # Format time remaining
            if days_left > 0:
                time_str = f"{days_left}d {hours_left}h {minutes_left}m"
            elif hours_left > 0:
                time_str = f"{hours_left}h {minutes_left}m"
            else:
                time_str = f"{minutes_left}m"
            
            # Calculate when user can review again (dynamic cooldown from settings)
            next_review_time = last_review_time + timedelta(seconds=cooldown_seconds)
            next_review_str = next_review_time.strftime("%d-%m-%Y %I:%M %p")
            next_review_date_only = next_review_time.strftime("%d %B %Y")
            next_review_time_only = next_review_time.strftime("%I:%M %p")
            
            # Format last review date
            last_review_str = last_review_time.strftime("%d-%m-%Y %I:%M %p")
            
            # Get last rating for display
            last_rating = last_review.get("rating", 0)
            star_display = "⭐" * last_rating
            rating_bar = "★" * last_rating + "☆" * (5 - last_rating)
            
            # Create visual cooldown bar (showing time remaining - 100% to 0%)
            percentage_complete = 100 - int((elapsed_seconds / REVIEW_COOLDOWN_SECONDS) * 100)  # Reverse: 100% → 0%
            filled_blocks = int((percentage_complete / 100) * 20)
            empty_blocks = 20 - filled_blocks
            progress_bar = "█" * filled_blocks + "░" * empty_blocks
            
            msg = (
                f"⏰ **REVIEW COOLDOWN ACTIVE**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ Your last review was successfully submitted!\n\n"
                f"📊 **YOUR LAST REVIEW:**\n"
                f"📅 {last_review_str}\n"
                f"⭐ Rating: {star_display} **{last_rating}/5**\n"
                f"📈 [{rating_bar}]\n\n"
                f"⏳ **COOLDOWN STATUS:**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"[{progress_bar}] {percentage_complete}%\n\n"
                f"⏱️ **Time Remaining:** `{time_str}`\n"
                f"🔓 **Next Review:** {next_review_date_only} at {next_review_time_only}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🛡️ Quality control active to ensure meaningful feedback.\n"
                f"🙏 Thank you for your patience!"
            )
            
            return (True, remaining_seconds, msg)
        
        return (False, 0, "")  # Cooldown passed, allow review
        
    except Exception as e:
        logger.error(f"Error checking review cooldown: {e}")
        return (False, 0, "")  # On error, allow review (fail-safe)

def check_spam_and_cooldown(user_id: int, current_time: float) -> tuple[bool, int, str]:
    """Check if user is spamming or in cooldown. Returns (is_blocked, remaining_seconds, message)"""
    # Check if user is in cooldown period
    if user_id in user_cooldown_until:
        cooldown_end = user_cooldown_until[user_id]
        if current_time < cooldown_end:
            remaining = int(cooldown_end - current_time + 0.5)  # Round to nearest second for accuracy
            time_str = format_time_remaining(remaining)
            spam_level = user_spam_count.get(user_id, 1)
            
            # Progressive professional messages
            if spam_level == 1:
                msg = f"⏳ **Brief Cooldown Active**\n\n🔒 Please wait a moment before trying again.\n⏰ **Time Remaining:** `{time_str}`\n\n💡 _Quality takes time. We appreciate your patience!_"
            elif spam_level == 2:
                msg = f"⏸️ **Temporary Pause**\n\n🔒 Multiple rapid attempts detected.\n⏰ **Cooldown Duration:** `{time_str}`\n\n✨ _Take a moment to breathe. We'll be ready when you are!_"
            elif spam_level == 3:
                msg = f"⌛ **Extended Break Required**\n\n🛡️ Our system detected unusual activity.\n⏰ **Time Remaining:** `{time_str}`\n\n🌟 _This brief pause protects our premium service quality._"
            else:
                msg = f"🌟 **Service Protection Active**\n\n🛡️ **Cooldown Period:** `{time_str}`\n\n💬 _We maintain strict quality standards to provide the best experience._\n✨ _Thank you for understanding and respecting our system._"
            
            return (True, remaining, msg)
        else:
            # Cooldown expired - FULLY RESET spam tracking to give user a clean slate
            if user_id in user_spam_count:
                del user_spam_count[user_id]  # Completely clear spam count
            if user_id in user_last_action:
                del user_last_action[user_id]  # Clear last action timestamp
            del user_cooldown_until[user_id]  # Remove cooldown entry
    
    # Check for rapid clicking (spam detection) - exact anti-spam freeze threshold
    if user_id in user_last_action:
        time_diff = current_time - user_last_action[user_id]
        if time_diff < ANTI_SPAM_FREEZE_SECONDS:  # Exactly less than 2.0 seconds = spam
            # Increment spam count
            spam_count = user_spam_count.get(user_id, 0) + 1
            user_spam_count[user_id] = spam_count
            
            # Apply exact cooldown duration
            cooldown_duration = get_cooldown_duration(spam_count)
            cooldown_end_time = current_time + float(cooldown_duration)  # Ensure float precision
            user_cooldown_until[user_id] = cooldown_end_time
            
            # Update last action timestamp to current time to prevent re-triggering
            user_last_action[user_id] = current_time
            
            time_str = format_time_remaining(cooldown_duration)
            unfreeze_time = datetime.fromtimestamp(cooldown_end_time, IST).strftime('%I:%M %p')
            msg = f"⏸️ **Quick Pause**\n\n👋 Hey! You're clicking too fast.\n⏰ **Brief Cooldown:** `{time_str}`\n🕓 **Available At:** {unfreeze_time}\n\n✨ _Our premium system needs a moment to process._\n💡 _Relax and try again in a few seconds!_"
            
            return (True, cooldown_duration, msg)
    
    return (False, 0, "")

# ==========================================
# 🛡️ ADVANCED SECURITY FUNCTIONS
# ==========================================

def sanitize_text(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> str:
    """Sanitize user input to prevent injection and limit length"""
    if not text:
        return ""
    
    # Remove potentially dangerous characters
    text = text.strip()
    
    # Limit length
    if len(text) > max_length:
        text = text[:max_length]
    
    # Remove control characters except newlines
    text = ''.join(char for char in text if char == '\n' or not char.isprintable() or char.isprintable())
    
    return text

def is_user_completely_banned(user_id: int) -> tuple[bool, dict, str]:
    """Check if user is completely banned from using bot. Returns (is_banned, ban_record, ban_message)"""
    try:
        ban_record = col_banned.find_one({"user_id": str(user_id)})
        if not ban_record:
            return (False, None, "")
        
        ban_type = ban_record.get("ban_type", "permanent")
        ban_until = ban_record.get("ban_until")
        banned_features = ban_record.get("banned_features", [])
        
        # Check if temporary ban has expired
        if ban_type == "temporary" and ban_until:
            if isinstance(ban_until, datetime):
                if datetime.now(IST) >= ban_until:
                    # Ban expired, remove it
                    col_banned.delete_one({"user_id": str(user_id)})
                    col_users.update_one({"user_id": str(user_id)}, {"$set": {"status": "active"}})
                    return (False, None, "")
        
        # User is banned - check if it's a complete ban (all features banned)
        if len(banned_features) >= 4 or ban_type == "permanent":
            # Complete ban - generate message
            custom_reason = ban_record.get("reason", "Violation of terms of service")
            banned_at = ban_record.get("banned_at", "Unknown")
            banned_by = ban_record.get("banned_by", "Admin")
            banned_from = ban_record.get("banned_from", "Multiple violations")
            violation_type = ban_record.get("violation_type", "Spam/Abuse")
            msa_id = ban_record.get("msa_id", "UNKNOWN")
            
            if isinstance(banned_at, datetime):
                ban_date = banned_at.strftime("%d %b %Y, %I:%M %p")
            else:
                ban_date = "Unknown Date"
            
            ban_msg = (
                f"🚫 **ACCESS COMPLETELY DENIED**\n\n"
                f"⛔ You are permanently banned from using this bot.\n\n"
                f"**📋 Ban Details:**\n"
                f"• MSA ID: {msa_id}\n"
                f"• Reason: {custom_reason}\n"
                f"• Violation Type: {violation_type}\n"
                f"• Banned From: {banned_from}\n"
                f"• Banned On: {ban_date}\n"
                f"• Banned By: {banned_by.replace('System (Auto)', 'MSANode Security Agent')}\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"❌ **ALL FEATURES DISABLED**\n"
                f"• Dashboard: Blocked\n"
                f"• Reviews: Blocked\n"
                f"• Support: Blocked\n"
                f"• Guide: Blocked\n"
                f"• FAQ: Blocked\n\n"
                f"🔒 You cannot access any bot features.\n"
                f"💬 Contact admin if this is a mistake."
            )
            
            return (True, ban_record, ban_msg)
        
        # Partial ban (some features banned)
        return (False, ban_record, "")
        
    except Exception as e:
        logger.error(f"Error checking ban status: {e}")
        return (False, None, "")

def check_security_violation(user_id: int) -> tuple[bool, str]:
    """Check if user has violated security policies. Returns (is_banned, reason)"""
    current_time = time.time()
    
    # Check failed attempts in window
    if user_id in user_failed_attempts:
        # Clean old attempts
        if user_id in user_last_violation:
            if current_time - user_last_violation[user_id] > FAILED_ATTEMPT_WINDOW:
                user_failed_attempts[user_id] = 0
        
        # Check threshold
        if user_failed_attempts[user_id] >= MAX_FAILED_ATTEMPTS:
            return (True, "Too many failed attempts. Temporarily restricted.")
    
    # Check global violation count
    if user_id in user_violation_count:
        if user_violation_count[user_id] >= GLOBAL_BAN_THRESHOLD:
            return (True, "Multiple policy violations. Account permanently restricted.")
    
    return (False, "")

async def send_ban_report(user_id: int, reason: str, violation_type: str, banned_from: str, banned_by: str = "System"):
    """Send detailed ban report to BAN_REPORT_CHANNEL_ID"""
    try:
        # Get complete user info
        user_doc = col_users.find_one({"user_id": str(user_id)})
        if user_doc:
            user_name = user_doc.get("first_name", "Unknown")
            msa_id = user_doc.get("msa_id", "UNKNOWN")
            username = user_doc.get("username", "No Username")
            join_date = user_doc.get("first_seen")
            last_active = user_doc.get("last_active")
        else:
            user_name = "Unknown"
            msa_id = "UNKNOWN"
            username = "No Username"
            join_date = None
            last_active = None
        
        now_str = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
        
        # Build detailed report
        report = (
            f"🚨 **USER BANNED - DETAILED REPORT** 🚨\n"
            f"═══════════════════════════════════\n\n"
            f"**👤 USER INFORMATION:**\n"
            f"• Name: {user_name}\n"
            f"• Telegram ID: `{user_id}`\n"
            f"• MSA ID: {msa_id}\n"
            f"• Username: @{username}\n\n"
            f"**⚠️ BAN DETAILS:**\n"
            f"• Reason: {reason}\n"
            f"• Violation Type: {violation_type}\n"
            f"• Banned From: {banned_from}\n"
            f"• Banned By: {banned_by.replace('System (Auto)', 'MSANode Security Agent')}\n"
            f"• Ban Date: {now_str}\n"
            f"• Ban Type: PERMANENT\n\n"
            f"**📊 USER ACTIVITY:**\n"
        )
        
        if join_date:
            if isinstance(join_date, datetime):
                join_str = join_date.strftime("%d-%m-%Y")
            else:
                join_str = str(join_date)
            report += f"• Joined: {join_str}\n"
        
        if last_active:
            if isinstance(last_active, datetime):
                last_str = last_active.strftime("%d-%m-%Y %I:%M %p")
            else:
                last_str = str(last_active)
            report += f"• Last Active: {last_str}\n"
        
        report += (
            f"\n**🔒 BLOCKED FEATURES:**\n"
            f"• ❌ Dashboard\n"
            f"• ❌ Reviews\n"
            f"• ❌ Customer Support\n"
            f"• ❌ Guide/How to Use\n"
            f"• ❌ FAQ/Help\n\n"
            f"═══════════════════════════════════\n"
            f"Status: 🔴 **PERMANENTLY BANNED**\n"
            f"═══════════════════════════════════"
        )
        
        # Send to ban report channel
        if BAN_REPORT_CHANNEL_ID:
            await bot.send_message(
                chat_id=BAN_REPORT_CHANNEL_ID,
                text=report,
                parse_mode="Markdown"
            )
            logger.info(f"Ban report sent to channel for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to send ban report: {e}")

def record_violation(user_id: int, violation_type: str = "general"):
    """Record a security violation for a user"""
    current_time = time.time()
    
    # Increment counters
    user_failed_attempts[user_id] = user_failed_attempts.get(user_id, 0) + 1
    user_violation_count[user_id] = user_violation_count.get(user_id, 0) + 1
    user_last_violation[user_id] = current_time
    
    # Log violation
    logger.warning(f"Security violation by user {user_id}: {violation_type} (Total: {user_violation_count[user_id]})")
    
    # Auto-ban if threshold reached
    if user_violation_count[user_id] >= GLOBAL_BAN_THRESHOLD:
        try:
            # Get user info
            user_doc = col_users.find_one({"user_id": str(user_id)})
            user_name = user_doc.get("first_name", "Unknown User") if user_doc else "Unknown User"
            msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
            username = user_doc.get("username", "No Username") if user_doc else "No Username"
            
            # Ban in database with complete information
            col_banned.insert_one({
                "user_id": str(user_id),
                "msa_id": msa_id,
                "username": username,
                "user_name": user_name,
                "reason": f"Automatic ban: {GLOBAL_BAN_THRESHOLD} security violations",
                "timestamp": datetime.now(IST),
                "violation_type": violation_type,
                "banned_from": "Multiple violations",
                "permanent": True,
                "banned_at": datetime.now(IST),
                "banned_by": "MSANode Security Agent",
                "ban_type": "permanent",
                "ban_until": None,
                "banned_features": ["downloads", "reviews", "support", "search", "dashboard", "guide", "faq"]
            })
            
            # Log to ban history
            col_ban_history.insert_one({
                "user_id": str(user_id),
                "msa_id": msa_id,
                "username": username,
                "user_name": user_name,
                "action_type": "auto_ban",
                "admin_name": "MSANode Security Agent",
                "reason": f"Automatic ban: {GLOBAL_BAN_THRESHOLD} security violations",
                "ban_type": "permanent",
                "ban_until": None,
                "banned_features": ["downloads", "reviews", "support", "search", "dashboard", "guide", "faq"],
                "banned_from": "Multiple violations",
                "violation_type": violation_type,
                "timestamp": datetime.now(IST)
            })
            
            # Send detailed ban report to BAN_REPORT_CHANNEL_ID
            asyncio.create_task(send_ban_report(
                user_id=user_id,
                reason=f"Automatic ban: {GLOBAL_BAN_THRESHOLD} security violations",
                violation_type=violation_type,
                banned_from="Multiple violations",
                banned_by="MSANode Security Agent"
            ))
            
            # Send notification to legacy ban channel (if different)
            if BAN_CHANNEL_ID and BAN_CHANNEL_ID != BAN_REPORT_CHANNEL_ID:
                try:
                    now_str = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
                    channel_msg = (
                        f"🚨 **AUTO-BAN TRIGGERED**\n"
                        f"═══════════════════════\n\n"
                        f"👤 User: {user_name}\n"
                        f"🆔 Telegram ID: {user_id}\n"
                        f"🏷️ MSA ID: {msa_id}\n"
                        f"👤 Username: @{username}\n"
                        f"🤖 Banned By: System (Auto)\n"
                        f"📅 Date: {now_str}\n\n"
                        f"⚠️ Violation Type: {violation_type}\n"
                        f"📝 Reason: {GLOBAL_BAN_THRESHOLD} Security Violations\n\n"
                        f"═══════════════════════\n"
                        f"Status: 🔒 PERMANENT (AUTO)"
                    )
                    asyncio.create_task(bot.send_message(
                        chat_id=BAN_CHANNEL_ID,
                        text=channel_msg,
                        parse_mode="Markdown"
                    ))
                except Exception as e:
                    logger.error(f"Failed to send auto-ban notification: {e}")
            
            logger.critical(f"🚨 AUTO-BAN: User {user_id} permanently banned for {GLOBAL_BAN_THRESHOLD} violations")
        except Exception as e:
            logger.error(f"Failed to auto-ban user {user_id}: {e}")

def validate_review_text(text: str) -> tuple[bool, str]:
    """Validate review text meets requirements. Returns (is_valid, error_message)"""
    if not text or not text.strip():
        return (False, "Review text cannot be empty.")
    
    text = text.strip()
    
    if len(text) < MIN_REVIEW_TEXT_LENGTH:
        return (False, f"Review too short. Minimum {MIN_REVIEW_TEXT_LENGTH} characters required.")
    
    if len(text) > MAX_REVIEW_TEXT_LENGTH:
        return (False, f"Review too long. Maximum {MAX_REVIEW_TEXT_LENGTH} characters allowed.")
    
    # Check for spam patterns
    if text.count('\n') > 20:
        return (False, "Too many line breaks. Please write normally.")
    
    # Check for repeated characters
    for char in set(text):
        if text.count(char * 10) > 0:  # 10 repeated characters
            return (False, "Spam detected. Please write meaningful feedback.")
    
    return (True, "")

def validate_support_message(text: str) -> tuple[bool, str]:
    """Validate support message. Returns (is_valid, error_message)"""
    if not text or not text.strip():
        return (False, "Support message cannot be empty.")
    
    text = text.strip()
    
    if len(text) < 5:
        return (False, "Message too short. Please describe your issue.")
    
    if len(text) > MAX_SUPPORT_MESSAGE_LENGTH:
        return (False, f"Message too long. Maximum {MAX_SUPPORT_MESSAGE_LENGTH} characters allowed.")
    
    return (True, "")

# ==========================================

# Keyboard Builders
def get_main_keyboard(user_id: int = None):
    """Main menu with DASHBOARD, REVIEW, CUSTOMER SUPPORT, FAQ, and GUIDE buttons
    For banned users: Shows ONLY Appeal Ban button"""
    
    # Check if user is completely banned
    if user_id:
        is_banned, ban_record, ban_msg = is_user_completely_banned(user_id)
        if is_banned:
            # Return keyboard with ONLY Appeal Ban button for banned users
            keyboard = [
                [KeyboardButton(text="🔔 APPEAL BAN")]
            ]
            return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)
    
    keyboard = [
        [KeyboardButton(text="📊 DASHBOARD")],
    ]
    
    # Check if review system is enabled (default to True if setting doesn't exist)
    try:
        reviews_setting = col_settings.find_one({"setting": "reviews_enabled"})
        
        if reviews_setting:
            reviews_enabled = reviews_setting.get("value", True)
        else:
            reviews_enabled = True
        
        # Only add review button if explicitly enabled (not False, not "false", not 0)
        if reviews_enabled != False and reviews_enabled != "false" and reviews_enabled != 0:
            keyboard.append([KeyboardButton(text="⭐ REVIEW")])
    except:
        # On error, show review button (fail open)
        keyboard.append([KeyboardButton(text="⭐ REVIEW")])
    
    keyboard.extend([
        [KeyboardButton(text="💬 CUSTOMER SUPPORT")],
        [KeyboardButton(text="❓ FAQ / HELP")],
        [KeyboardButton(text="📚 GUIDE / HOW TO USE")],
        [KeyboardButton(text="📜 RULES & REGULATIONS")]
    ])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_star_keyboard():
    """Colorful professional star rating keyboard - respects minimum rating setting"""
    # Get minimum rating from settings (default: 1)
    try:
        min_rating_setting = col_settings.find_one({"setting": "review_min_rating"})
        min_rating = min_rating_setting.get("value", 1) if min_rating_setting else 1
    except:
        min_rating = 1  # Fallback to allow all ratings
    
    # Star options with emojis
    star_options = {
        1: "⭐️ 1 STAR",
        2: "⭐️⭐️ 2 STARS",
        3: "⭐️⭐️⭐️ 3 STARS",
        4: "⭐️⭐️⭐️⭐️ 4 STARS",
        5: "⭐️⭐️⭐️⭐️⭐️ 5 STARS"
    }
    
    # Build keyboard with only allowed ratings
    keyboard = []
    row = []
    for rating in range(min_rating, 6):  # min_rating to 5 stars
        row.append(KeyboardButton(text=star_options[rating]))
        if len(row) == 2:  # 2 buttons per row
            keyboard.append(row)
            row = []
    
    # Add remaining button if odd number
    if row:
        keyboard.append(row)
    
    # Add back button
    keyboard.append([KeyboardButton(text="🔙 BACK")])
    
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

def get_cancel_keyboard():
    """Cancel button for text input"""
    keyboard = [[KeyboardButton(text="❌ CANCEL")]]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

def get_dashboard_actions_keyboard(has_pending_ticket: bool = False):
    """Dashboard action buttons with conditional cancel ticket option"""
    keyboard = []
    
    # Add history buttons
    keyboard.append([KeyboardButton(text="📜 MY REVIEWS"), KeyboardButton(text="🎫 MY TICKETS")])
    keyboard.append([KeyboardButton(text="📊 MY STATS")])
    
    if has_pending_ticket:
        keyboard.append([KeyboardButton(text="🚫 CANCEL MY TICKET")])
    
    keyboard.append([KeyboardButton(text="🔄 REFRESH STATUS")])
    keyboard.append([KeyboardButton(text="🏠 BACK TO MAIN MENU")])
    
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_support_issues_keyboard():
    """Common support issues template keyboard"""
    keyboard = [
        [KeyboardButton(text="📄 PDF/Link Not Working")],
        [KeyboardButton(text="🤖 Bot Not Responding")],
        [KeyboardButton(text="⭐ Review Issue")],
        [KeyboardButton(text="🔗 Access/Channel Problem")],
        [KeyboardButton(text="❓ Content Question")],
        [KeyboardButton(text="⚙️ Account/Settings Help")],
        [KeyboardButton(text="✍️ Other Issue (Type Custom Message)")],
        [KeyboardButton(text="❌ CANCEL")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

def get_faq_keyboard():
    """FAQ topics keyboard"""
    keyboard = [
        [KeyboardButton(text="❓ How to submit a review?")],
        [KeyboardButton(text="⏰ Why can't I review again?")],
        [KeyboardButton(text="📱 How to contact support?")],
        [KeyboardButton(text="🔒 Why is my review not showing?")],
        [KeyboardButton(text="⚖️ What are the spam protections?")],
        [KeyboardButton(text="📊 How to check my dashboard?")],
        [KeyboardButton(text="🏠 Back to Main Menu")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_solution_feedback_keyboard():
    """Feedback keyboard after showing solution"""
    keyboard = [
        [KeyboardButton(text="✅ SOLVED - Thank You!")],
        [KeyboardButton(text="❌ NOT SOLVED - Need More Help")],
        [KeyboardButton(text="🔄 Check Other Solutions")],
        [KeyboardButton(text="💬 Contact Support Team")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

def get_guide_main_keyboard():
    """Main guide menu keyboard"""
    keyboard = [
        [KeyboardButton(text="📚 Support System Guide")],
        [KeyboardButton(text="⭐ Review System Guide")],
        [KeyboardButton(text="🛡️ Anti-Spam Protection Info")],
        [KeyboardButton(text="⚡ Commands & Features")],
        [KeyboardButton(text="🎯 Premium Features")],
        [KeyboardButton(text="❓ FAQ - Common Questions")],
        [KeyboardButton(text="🏠 Back to Main Menu")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

def get_confirmation_keyboard():
    """Confirmation buttons for review submission"""
    keyboard = [
        [KeyboardButton(text="✅ CONFIRM & SEND")],
        [KeyboardButton(text="❌ CANCEL")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

async def send_review_to_channel(user_id: str, name: str, username: str, rating: int, feedback: str, timestamp: str, is_update: bool = False):
    """Send or update review report in private review channel"""
    if not REVIEW_LOG_CHANNEL:
        return None
    
    try:
        # Get user's MSA ID
        user_doc = col_users.find_one({"user_id": user_id})
        msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
        
        # Calculate cooldown (dynamic from settings)
        now_ist = datetime.now(IST)
        cooldown_seconds = get_review_cooldown_seconds()
        next_review_date = (now_ist + timedelta(seconds=cooldown_seconds)).strftime("%d-%m-%Y %I:%M %p")
        
        # Star rating bar and display
        rating_bar = "★" * rating + "☆" * (5 - rating)
        star_emoji = "⭐" * rating
        
        # Status indicator based on whether this is update or new
        status_badge = "🔄 UPDATED" if is_update else "🆕 NEW"
        update_info = f"\n🔄 **LAST UPDATED:** {timestamp}" if is_update else ""
        
        # For NEW reviews: cooldown is 100% (just submitted, full cooldown active)
        # For UPDATES: keep 100% because user is submitting again (cooldown resets)
        percentage_remaining = 100
        filled_blocks = 20
        empty_blocks = 0
        cooldown_bar = "█" * filled_blocks + "░" * empty_blocks
        
        # Escape markdown special characters in feedback to prevent parsing errors
        feedback_escaped = feedback.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
        
        report = (
            f"🌟 **SYSTEM PERFORMANCE REVIEW** {status_badge}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 **OPERATIVE:** {name.upper()}\n"
            f"🆔 **MSA ID:** `{msa_id}`\n"
            f"📱 **TELEGRAM ID:** `{user_id}`\n"
            f"🌐 **USERNAME:** {username}\n\n"
            f"📊 **RATING:** {star_emoji} **{rating}/5**\n"
            f"📈 **BAR:** {rating_bar}\n\n"
            f"💬 **FEEDBACK:**\n{feedback_escaped}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ **VAULT STATUS:** 🟢 ACTIVE\n"
            f"⏳ **COOLDOWN:** {cooldown_bar} {percentage_remaining}%\n"
            f"🔓 **NEXT REVIEW:** {next_review_date}\n"
            f"⏰ **SUBMITTED:** {timestamp}{update_info}"
        )
        
        # Create inline keyboard with SYNC button
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        sync_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 SYNC STATUS", callback_data=f"sync_review_{user_id}")]
        ])
        
        # Check if user has existing review message in channel
        existing_review = col_reviews.find_one(
            {"user_id": user_id}
        )
        
        if existing_review:
            logger.info(f"📋 Found existing review for user {user_id}: has channel_msg_id={existing_review.get('channel_message_id') is not None}")
        
        if existing_review and existing_review.get("channel_message_id"):
            # ALWAYS try to EDIT existing message (prevents duplicates)
            existing_msg_id = existing_review["channel_message_id"]
            logger.info(f"🔄 Attempting to edit existing message {existing_msg_id} for user {user_id}")
            try:
                await bot.edit_message_text(
                    chat_id=REVIEW_LOG_CHANNEL,
                    message_id=existing_msg_id,
                    text=report,
                    parse_mode="Markdown",
                    reply_markup=sync_keyboard
                )
                logger.info(f"✏️ Successfully edited message {existing_msg_id} for user {user_id}")
                return existing_msg_id  # Return same message ID
            except TelegramBadRequest as edit_error:
                # Message might be deleted or too old, send new one
                logger.warning(f"⚠️ Cannot edit message {existing_msg_id} (likely deleted): {edit_error}")
            except Exception as edit_error:
                logger.error(f"❌ Failed to edit message {existing_msg_id}: {edit_error}")
        else:
            logger.info(f"📝 No existing message found for user {user_id}, will send new one")
        
        # SEND new message (first-time reviewer or edit failed)
        sent_message = await bot.send_message(
            REVIEW_LOG_CHANNEL, 
            report, 
            parse_mode="Markdown",
            reply_markup=sync_keyboard
        )
        logger.info(f"📤 Sent new review message for user {user_id}")
        return sent_message.message_id
        
    except Exception as e:
        logger.error(f"Error in send_review_to_channel: {e}")
        return None

async def send_admin_report(text: str):
    """Sends one-time detailed dossier to private channel."""
    if ADMIN_LOG_CHANNEL:
        try:
            await bot.send_message(ADMIN_LOG_CHANNEL, f"📡 **MSANODE INTELLIGENCE DOSSIER**\n────────────────────\n{text}", parse_mode="Markdown")
        except: pass

def get_next_msa_id():
    """Generate next unique MSA ID - reuses deleted IDs (MSA1, MSA2, etc.) before incrementing"""
    try:
        # Get all existing MSA IDs from users collection
        existing_users = col_users.find({"msa_id": {"$exists": True}}, {"msa_id": 1})
        existing_ids = set()
        
        for user in existing_users:
            msa_id = user.get("msa_id", "")
            if msa_id and msa_id.startswith("MSA"):
                try:
                    # Extract numeric part (e.g., "MSA5" -> 5)
                    num = int(msa_id.replace("MSA", ""))
                    existing_ids.add(num)
                except ValueError:
                    continue
        
        # Find the lowest available ID starting from 1
        next_id = 1
        while next_id in existing_ids:
            next_id += 1
        
        # Update counter to highest ID seen (for consistency)
        current_counter = col_user_counter.find_one({"_id": "msa_counter"})
        if current_counter:
            max_id = max(existing_ids) if existing_ids else 0
            if next_id > max_id:
                # We're assigning a new highest ID, update counter
                col_user_counter.update_one(
                    {"_id": "msa_counter"},
                    {"$set": {"current": next_id}}
                )
        
        return f"MSA{next_id}"
        
    except Exception as e:
        logger.error(f"Error generating MSA ID: {e}")
        # Fallback to counter-based ID
        try:
            counter_doc = col_user_counter.find_one_and_update(
                {"_id": "msa_counter"},
                {"$inc": {"current": 1}},
                return_document=True
            )
            return f"MSA{counter_doc['current']}"
        except:
            # Last resort: timestamp-based ID
            return f"MSA{int(time.time())}"

async def is_member(user_id):
    """Verifies user is inside MSANode Telegram Channel."""
    try:
        status = await bot.get_chat_member(CHANNEL_ID, user_id)
        return status.status in ['member', 'administrator', 'creator']
    except: return False

def has_accepted_terms(user_id: int) -> bool:
    """Check if user has accepted terms and conditions"""
    try:
        terms_record = col_terms.find_one({"user_id": str(user_id)})
        return terms_record is not None and terms_record.get("accepted", False)
    except:
        return False

async def log_user(user: types.User, source: str):
    """Identity Engine: Returns NEW or RETURNING with IST format."""
    now_str = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
    u_id = str(user.id)
    u_name = f"@{user.username}" if user.username else "None"
    
    try:
        existing = col_users.find_one({"user_id": u_id})
        if not existing:
            # Generate unique MSA ID for new user
            msa_id = get_next_msa_id()
            col_users.insert_one({
                "first_name": user.first_name,
                "username": u_name,
                "user_id": u_id,
                "msa_id": msa_id,
                "last_active": now_str,
                "joined_date": now_str,
                "source": source,
                "status": "Active",
                "has_reported": False,
                "terms_accepted": False  # Track terms acceptance in user doc
            })
            logger.info(f"✅ NEW USER REGISTERED: {msa_id} (Telegram ID: {u_id})")
            return "NEW"
        else:
            upd = {"last_active": now_str, "status": "Active"}
            if existing.get("source") == "Unknown" and source != "Unknown": upd["source"] = source
            # Ensure existing user has MSA ID (fallback for migration)
            if not existing.get("msa_id"):
                upd["msa_id"] = get_next_msa_id()
                logger.info(f"✅ ASSIGNED ID TO EXISTING USER: {upd['msa_id']} (Telegram ID: {u_id})")
            col_users.update_one({"user_id": u_id}, {"$set": upd})
            return "RETURNING"
    except Exception as e: 
        logger.error(f"Log Error: {e}")
        return "ERROR"

async def get_content(code: str):
    try:
        doc = col_active.find_one({"code": code.upper()})
        if doc:
            aff_text = doc.get("aff_text") or random.choice(AFFILIATE_TRIGGERS)
            return {"main_link": doc.get("pdf_link"), "aff_link": doc.get("aff_link"), "aff_text": aff_text}
    except: return None
    return None

# ==========================================
# 🚨 RETENTION WATCHDOG
# ==========================================
@dp.chat_member(ChatMemberUpdatedFilter(LEAVE_TRANSITION))
async def on_user_leave(event: ChatMemberUpdated):
    if event.chat.id != CHANNEL_ID: return
    user = event.new_chat_member.user
    
    # Update Database Status
    col_users.update_one({"user_id": str(user.id)}, {"$set": {"status": "LEFT"}})
    
    try:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="� REJOIN VAULT", url=CHANNEL_LINK)]])
        
        sent_msg = await bot.send_message(
            user.id, 
            f"⚠️ **{user.first_name}, Your Access Has Been Revoked**\n\n"
            f"You've left the MSANode Vault. All premium blueprints and exclusive content are now locked.\n\n"
            f"**💎 What You're Missing:**\n"
            f"➤ Premium automation guides\n"
            f"➤ Exclusive strategy blueprints\n"
            f"➤ Direct access to tools & resources\n"
            f"➤ Priority support and updates\n\n"
            f"**🔓 Rejoin now to restore full access instantly.**",
            reply_markup=kb
        )
        # Store message ID so we can delete it when they rejoin
        col_users.update_one(
            {"user_id": str(user.id)}, 
            {"$set": {"leave_message_id": sent_msg.message_id}}
        )
    except Exception:
        pass # Prevents crash if user blocks bot

@dp.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def on_user_join(event: ChatMemberUpdated):
    if event.chat.id != CHANNEL_ID: return
    user = event.new_chat_member.user
    
    # Restore Database Status
    col_users.update_one({"user_id": str(user.id)}, {"$set": {"status": "Active"}})
    
    try:
        # Delete the "rejoin" message if it exists
        user_doc = col_users.find_one({"user_id": str(user.id)})
        if user_doc and user_doc.get("leave_message_id"):
            try:
                await bot.delete_message(user.id, user_doc["leave_message_id"])
                # Remove the stored message ID
                col_users.update_one(
                    {"user_id": str(user.id)}, 
                    {"$unset": {"leave_message_id": ""}}
                )
            except:
                pass  # Message might already be deleted
        
        # Premium animation sequence for rejoining
        loading = await bot.send_message(user.id, "🔍 **Verifying Membership...**")
        await asyncio.sleep(0.8)
        await loading.edit_text("🔐 **Restoring Access Privileges...**")
        await asyncio.sleep(0.7)
        await loading.edit_text("✅ **Access Restored Successfully!**")
        await asyncio.sleep(0.6)
        await loading.delete()
        
        await bot.send_message(
            user.id, 
            f"🎉 **Welcome Back, {user.first_name}!**\n\n"
            f"Your membership has been successfully restored. You now have full access to:\n\n"
            f"✅ All premium blueprints\n"
            f"✅ Exclusive automation guides\n"
            f"✅ Direct tool access\n"
            f"✅ Priority updates\n\n"
            f"💼 **Ready to continue your journey. Check pinned comments for latest content!**"
        )
    except Exception: 
        pass

# ==========================================
# ⭐ REVIEW SYSTEM
# ==========================================

@dp.message(F.text == "⭐ REVIEW")
async def start_review(message: types.Message, state: FSMContext):
    """Start the review process with animations and anti-spam protection"""
    user_id = message.from_user.id
    current_time = time.time()
    
    # CRITICAL: Check if user is completely banned FIRST
    is_banned, ban_record, ban_msg = is_user_completely_banned(user_id)
    if is_banned:
        try:
            await message.answer(ban_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    # CRITICAL: Check if user has accepted terms & conditions
    if not has_accepted_terms(user_id):
        await message.answer(
            "⚠️ **Terms & Conditions Required**\n\n"
            f"{message.from_user.first_name}, you must accept our Terms & Conditions before using any bot features.\n\n"
            "📜 Please accept the terms to continue.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✅ I Accept the Terms & Conditions")],
                    [KeyboardButton(text="❌ I Reject")]
                ],
                resize_keyboard=True,
                one_time_keyboard=False
            ),
            parse_mode="Markdown"
        )
        return
    
    # Check if review system is enabled (CRITICAL CHECK)
    try:
        reviews_setting = col_settings.find_one({"setting": "reviews_enabled"})
        
        # If setting exists, check its value. If not exists, default to True (enabled)
        if reviews_setting:
            reviews_enabled = reviews_setting.get("value", True)
        else:
            reviews_enabled = True
        
        # If reviews are disabled, block access immediately
        if reviews_enabled == False or reviews_enabled == "false" or reviews_enabled == 0:
            await message.answer(
                "⚠️ **Review System Currently Disabled**\n\n"
                "The review feature has been temporarily disabled by the admin.\n"
                "Please try again later! 🙏",
                parse_mode="Markdown"
            )
            logger.info(f"❌ User {user_id} tried to access disabled review system")
            return
    except Exception as e:
        logger.error(f"Error checking review settings: {e}")
        # On error, allow review (fail open)
        pass
    
    # Check if review feature is banned for this user
    user_data = col_users.find_one({"user_id": str(user_id)})
    if is_feature_banned(user_data, 'reviews'):
        await send_feature_ban_message(message, 'reviews', user_data)
        return
    
    # CRITICAL: Anti-spam freeze - prevent multiple message spam on button clicks
    # Check if user clicked too fast (within exactly 2.0 seconds of last click)
    if user_id in user_last_action:
        time_since_last = current_time - user_last_action[user_id]
        if time_since_last < ANTI_SPAM_FREEZE_SECONDS:  # Anti-spam freeze timer
            # Silently ignore spam clicks - don't send any message
            return
    
    # FIRST: System-level rate limiting (prevents overload from lakhs of users)
    is_rate_limited, rate_limit_msg = check_rate_limit(user_id)
    if is_rate_limited:
        await message.answer(rate_limit_msg, parse_mode="Markdown")
        return
    
    # SECOND: Check 7-day review cooldown from database
    is_cooldown, cooldown_remaining, cooldown_msg = check_review_cooldown(str(user_id))
    if is_cooldown:
        await message.answer(cooldown_msg, parse_mode="Markdown")
        return
    
    # THIRD: Check for spam/rapid clicking protection (progressive bans)
    is_blocked, remaining_time, block_msg = check_spam_and_cooldown(user_id, current_time)
    if is_blocked:
        await message.answer(block_msg, parse_mode="Markdown")
        return
    
    # ALL CHECKS PASSED - Update timestamp now to prevent spam in next attempts
    user_last_action[user_id] = current_time
    
    # Clear any existing state - REFRESH/RESTART from beginning
    await state.clear()
    
    # Set processing state to block spam
    await state.set_state(ReviewState.processing)
    
    # Enhanced premium animation sequence with progress bars
    msg = await message.answer("┏━━━━━━━━━━━━━━━━━━━┓\n┃ 🎯 **Review Portal** ┃\n┗━━━━━━━━━━━━━━━━━━━┛\n\n⏳ *Initializing...*\n░░░░░░░░░░ 0%", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await msg.edit_text("┏━━━━━━━━━━━━━━━━━━━┓\n┃ 🔐 **Review Portal** ┃\n┗━━━━━━━━━━━━━━━━━━━┛\n\n🔍 *Authenticating user...*\n▰▰▱▱▱▱▱▱▱▱ 25%", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await msg.edit_text("┏━━━━━━━━━━━━━━━━━━━┓\n┃ 🌟 **Review Portal** ┃\n┗━━━━━━━━━━━━━━━━━━━┛\n\n⚡ *Loading interface...*\n▰▰▰▰▰▱▱▱▱▱ 50%", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await msg.edit_text("┏━━━━━━━━━━━━━━━━━━━┓\n┃ ✨ **Review Portal** ┃\n┗━━━━━━━━━━━━━━━━━━━┛\n\n🎨 *Preparing stars...*\n▰▰▰▰▰▰▰▱▱▱ 75%", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await msg.edit_text("┏━━━━━━━━━━━━━━━━━━━┓\n┃ 💎 **Review Portal** ┃\n┗━━━━━━━━━━━━━━━━━━━┛\n\n✅ *System Ready!*\n▰▰▰▰▰▰▰▰▰▰ 100%", parse_mode="Markdown")
    await asyncio.sleep(0.2)
    await msg.edit_text("┏━━━━━━━━━━━━━━━━━━━┓\n┃ 🎊 **Welcome!** 🎊 ┃\n┗━━━━━━━━━━━━━━━━━━━┛\n\n🚀 *Launching...*", parse_mode="Markdown")
    await asyncio.sleep(0.25)
    await msg.delete()
    
    # Check if user is a milestone reviewer (using cached count)
    total_reviews = get_cached_review_count()
    milestone_bonus = ""
    if total_reviews + 1 in [1, 10, 25, 50, 100, 250, 500, 1000]:
        milestone_bonus = f"\n\n🎁 **SPECIAL:** You're reviewer #{total_reviews + 1}! Bonus reward inside! 🎁"
    
    await message.answer(
        f"✨ **Welcome, {message.from_user.first_name}!** ✨\n\n"
        f"╔═══════════════════════════════╗\n"
        f"   🌟 **PREMIUM REVIEW SYSTEM** 🌟\n"
        f"╚═══════════════════════════════╝\n\n"
        f"💎 _Your voice shapes our excellence!_\n\n"
        f"🎁 **EXCLUSIVE REWARDS:**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⭐⭐⭐⭐⭐ = 🎊 **Premium Bonus**\n"
        f"⭐⭐⭐⭐ = 💝 **VIP Recognition**\n"
        f"⭐⭐⭐ = 🙏 **Valued Contributor**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{milestone_bonus}\n\n"
        f"⭐ **How was your experience?**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⭐ = Needs Improvement\n"
        f"⭐⭐⭐ = Satisfactory\n"
        f"⭐⭐⭐⭐⭐ = Exceptional\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👇 _Select your rating:_",
        reply_markup=get_star_keyboard()
    )
    await state.set_state(ReviewState.waiting_for_stars)

@dp.message(F.text.in_(["🔙 BACK", "❌ CANCEL"]))
async def cancel_review(message: types.Message, state: FSMContext):
    """Cancel review and return to main menu"""
    user_id = str(message.from_user.id)
    current_time = time.time()
    
    # Ban protection check
    if col_banned.find_one({"user_id": user_id}):
        return
    
    current_state = await state.get_state()
    
    # Handle SupportState cancel
    if current_state and "SupportState" in str(current_state):
        logger.info(f"Cancel button clicked by user {user_id} in support state")
        await state.clear()
        await message.answer(
            "Cancelled\n\n"
            "Support request cancelled.\n"
            "Returning to main menu...",
            reply_markup=get_main_keyboard()
        )
        return
    
    # Only handle review if in review state
    if not current_state or "ReviewState" not in str(current_state):
        return
    
    # Anti-spam check
    is_blocked, remaining, spam_msg = check_spam_and_cooldown(user_id, current_time)
    if is_blocked:
        try:
            await message.answer(spam_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    # Update last action
    user_last_action[user_id] = current_time
    
    try:
        msg = await message.answer("Cancelling review...", parse_mode="Markdown")
        await asyncio.sleep(0.3)
        await msg.edit_text("Review cancelled successfully!", parse_mode="Markdown")
        await asyncio.sleep(0.4)
        await msg.delete()
        
        await state.clear()
        await message.answer(
            f"✨ **Review Cancelled Successfully**\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💭 No worries! You can return anytime.\n"
            f"🔄 Your feedback is always welcome.\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👉 Click **⭐ REVIEW** whenever you're ready!",
            reply_markup=get_main_keyboard()
        )
    except TelegramForbiddenError:
        logger.warning(f"User {message.from_user.id} blocked the bot")
    except Exception as e:
        logger.error(f"Error in cancel_review: {e}")

@dp.message(F.text.in_(["⭐️ 1 STAR", "⭐️⭐️ 2 STARS", "⭐️⭐️⭐️ 3 STARS", "⭐️⭐️⭐️⭐️ 4 STARS", "⭐️⭐️⭐️⭐️⭐️ 5 STARS"]), ReviewState.waiting_for_stars)
async def handle_star_selection(message: types.Message, state: FSMContext):
    """Handle star rating selection with minimum rating validation"""
    user_id = message.from_user.id
    current_time = time.time()
    
    # Anti-spam protection with cooldown system
    is_blocked, remaining_time, block_msg = check_spam_and_cooldown(user_id, current_time)
    if is_blocked:
        await message.answer(block_msg, parse_mode="Markdown")
        return
    
    user_last_action[user_id] = current_time
    
    # Extract rating number from text
    rating_map = {
        "⭐️ 1 STAR": 1,
        "⭐️⭐️ 2 STARS": 2,
        "⭐️⭐️⭐️ 3 STARS": 3,
        "⭐️⭐️⭐️⭐️ 4 STARS": 4,
        "⭐️⭐️⭐️⭐️⭐️ 5 STARS": 5
    }
    rating = rating_map.get(message.text)
    
    # Validate against minimum rating setting
    try:
        min_rating_setting = col_settings.find_one({"setting": "review_min_rating"})
        min_rating = min_rating_setting.get("value", 1) if min_rating_setting else 1
        
        if rating < min_rating:
            await message.answer(
                f"❌ **RATING NOT ALLOWED**\n\n"
                f"Minimum rating allowed: {min_rating}⭐\n"
                f"Your selection: {rating}⭐\n\n"
                f"Please select {min_rating} stars or higher.",
                reply_markup=get_star_keyboard()
            )
            return
    except Exception as e:
        logger.error(f"Error checking minimum rating: {e}")
    
    # Anti-spam: Set processing state immediately
    await state.set_state(ReviewState.processing)
    
    # Store rating in state
    await state.update_data(rating=rating)
    
    # CRITICAL: Set state to waiting_for_text IMMEDIATELY to prevent race condition
    # This ensures fast typers don't get stuck in wrong state
    await state.set_state(ReviewState.waiting_for_text)
    
    # Premium animation with rating-specific effects
    if rating == 5:
        msg = await message.answer("🌟 **5-Star Excellence!**")
        await asyncio.sleep(0.15)
        await msg.edit_text("✨ **Preparing Rewards...**")
        await asyncio.sleep(0.15)
    elif rating == 4:
        msg = await message.answer("💎 **4-Star Quality!**")
        await asyncio.sleep(0.15)
        await msg.edit_text("💝 **Thank You!**")
        await asyncio.sleep(0.15)
    else:
        msg = await message.answer("⭐ **Processing Rating...**")
        await asyncio.sleep(0.15)
        await msg.edit_text("✨ **Analyzing...**")
        await asyncio.sleep(0.15)
    await msg.delete()
    
    # Premium animations for each rating level
    animations = {
        1: [("😔 **Recording feedback...**", 0.15),
            ("💾 **Saved**", 0.1)],
        2: [("📊 **Recording rating...**", 0.15),
            ("💾 **Saved**", 0.1)],
        3: [("👍 **Good rating!**", 0.15),
            ("💾 **Saved**", 0.1)],
        4: [("🚀 **Excellent!**", 0.15),
            ("💎 **Saved**", 0.1)],
        5: [("🎊 **Outstanding!**", 0.15),
            ("✨ **Saved**", 0.1)]
    }
    
    # Run animation sequence
    msg = await message.answer(animations[rating][0][0])
    for i in range(1, len(animations[rating])):
        await asyncio.sleep(animations[rating][i-1][1])
        await msg.edit_text(animations[rating][i][0])
    await asyncio.sleep(0.1)
    await msg.delete()
    
    # Premium LARGE animated star display with bigger stars
    large_star_displays = {
        1: "⭐",
        2: "⭐ ⭐",
        3: "⭐ ⭐ ⭐",
        4: "⭐ ⭐ ⭐ ⭐",
        5: "⭐ ⭐ ⭐ ⭐ ⭐"
    }
    
    # Fast animated star reveal with bigger display
    star_msg = await message.answer("✨")
    await asyncio.sleep(0.1)
    await star_msg.edit_text(f"✨\n\n      ⭐")
    await asyncio.sleep(0.08)
    if rating >= 2:
        await star_msg.edit_text(f"✨\n\n      ⭐ ⭐")
        await asyncio.sleep(0.08)
    if rating >= 3:
        await star_msg.edit_text(f"✨\n\n      ⭐ ⭐ ⭐")
        await asyncio.sleep(0.08)
    if rating >= 4:
        await star_msg.edit_text(f"✨\n\n      ⭐ ⭐ ⭐ ⭐")
        await asyncio.sleep(0.08)
    if rating >= 5:
        await star_msg.edit_text(f"✨\n\n      ⭐ ⭐ ⭐ ⭐ ⭐")
        await asyncio.sleep(0.1)
    await star_msg.delete()
    
    # Premium rating-based response with LARGE stars
    responses = {
        1: "😔 **We sincerely apologize for the experience.**\n\nYour satisfaction is our highest priority. Please share what went wrong so we can make it right immediately.",
        2: "⚠️ **We know we can do much better!**\n\nHelp us understand what disappointed you. Your insights directly drive our improvement initiatives.",
        3: "👍 **Good foundation, aiming for excellence!**\n\nWhat would elevate your experience to 5 stars? We're actively listening and ready to implement changes.",
        4: "🚀 **Almost perfect - thank you!**\n\nYou're satisfied, but we aim for absolute excellence. What final touch would make it flawless?",
        5: "🔥 **Outstanding! We're honored!**\n\nWe're thrilled by your 5-star rating! Share what made your experience exceptional so we can replicate it."
    }
    
    await message.answer(
        f"╔═════════════════════════╗\n"
        f"      ⭐ **RATING CONFIRMED** ⭐\n"
        f"╚═════════════════════════╝\n\n"
        f"**YOUR RATING:**\n\n"
        f"      {large_star_displays[rating]}\n\n"
        f"      **{rating} OUT OF 5 STARS**\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{responses[rating]}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 **Please write your detailed feedback below:**\n\n"
        f"📏 **Character Limit:** 5-1000 characters\n"
        f"✅ **Guidelines:** Use meaningful words, no spam\n\n"
        f"💭 Share your honest thoughts and suggestions!\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━",
        reply_markup=get_cancel_keyboard()
    )

@dp.message(ReviewState.waiting_for_stars)
async def enforce_star_button(message: types.Message):
    """Enforce use of star buttons"""
    # Ban protection check
    if col_banned.find_one({"user_id": str(message.from_user.id)}):
        return
    
    try:
        await message.answer(
            "⚠️ **Please Use the Star Buttons**\n\n"
            "📍 Click one of the colorful star buttons above (1-5 stars) to rate your experience.\n\n"
            "💡 Text input is not accepted for ratings."
        )
    except TelegramForbiddenError:
        logger.warning(f"User {message.from_user.id} blocked the bot")
    except Exception as e:
        logger.error(f"Error in enforce_star_button: {e}")

@dp.message(ReviewState.waiting_for_text)
async def handle_review_text(message: types.Message, state: FSMContext):
    """Handle review text input with comprehensive validation"""
    # Skip if cancel button
    if message.text in ["❌ CANCEL", "🔙 BACK"]:
        return
    
    user_id = message.from_user.id
    current_time = time.time()
    
    # Security check - Check for violations first
    is_banned, ban_reason = check_security_violation(user_id)
    if is_banned:
        await message.answer(
            f"🚫 **Access Restricted**\n\n{ban_reason}\n\nContact support if you believe this is an error.",
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
    # Anti-spam protection
    is_blocked, remaining_time, block_msg = check_spam_and_cooldown(user_id, current_time)
    if is_blocked:
        await message.answer(block_msg, parse_mode="Markdown")
        return
    
    user_last_action[user_id] = current_time
    
    # Anti-spam: Set processing state immediately
    await state.set_state(ReviewState.processing)
    
    # Get and sanitize the text
    feedback_text = sanitize_text(message.text.strip() if message.text else "", MAX_REVIEW_TEXT_LENGTH)
    
    # Validate review text using security function
    is_valid, error_msg = validate_review_text(feedback_text)
    if not is_valid:
        record_violation(user_id, "invalid_review_text")
        await message.answer(
            f"⚠️ **Invalid Review**\n\n{error_msg}\n\n💡 Please write a meaningful review.",
            reply_markup=get_cancel_keyboard(),
            parse_mode="Markdown"
        )
        await state.set_state(ReviewState.waiting_for_text)
        return
    
    # Validation passed - continue with old spam check for backward compatibility
    # Validation 3: Spam/repeated characters (like "aaaaaaa")
    def is_spam_text(text: str) -> bool:
        """Check if text is spam (repeated chars)"""
        if len(text) < 10:
            return False
        
        # Check for excessive character repetition
        unique_chars = len(set(text.lower().replace(" ", "")))
        total_chars = len(text.replace(" ", ""))
        
        # If more than 70% are the same character, it's spam
        if total_chars > 0 and unique_chars / total_chars < 0.3:
            return True
        
        return False
    
    if is_spam_text(feedback_text):
        await message.answer(
            "⚠️ **Invalid Feedback Format!**\n\n"
            "🚫 Your message appears to contain spam or repeated characters.\n\n"
            "✅ **Please provide meaningful feedback:**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "• Use proper words and sentences\n"
            "• Share specific details\n"
            "• Avoid repetitive characters\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "💡 Quality feedback helps us serve you better!",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(ReviewState.waiting_for_text)
        return
    
    # All validations passed - save feedback
    await state.update_data(feedback=feedback_text)
    data = await state.get_data()
    
    # Fast processing animation
    msg = await message.answer("💾 **Processing your feedback...**")
    await asyncio.sleep(0.12)
    await msg.edit_text("✨ **Generating preview...**")
    await asyncio.sleep(0.12)
    await msg.delete()
    
    # Show premium preview with bigger stars
    large_star_displays = {
        1: "⭐",
        2: "⭐ ⭐",
        3: "⭐ ⭐ ⭐",
        4: "⭐ ⭐ ⭐ ⭐",
        5: "⭐ ⭐ ⭐ ⭐ ⭐"
    }
    
    await message.answer(
        f"╔═══════════════════════════╗\n"
        f"      📋 **REVIEW PREVIEW** 📋\n"
        f"╚═══════════════════════════╝\n\n"
        f"**YOUR RATING:**\n"
        f"      {large_star_displays[data['rating']]}\n"
        f"      **{data['rating']} OUT OF 5 STARS**\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"**YOUR FEEDBACK:**\n"
        f"_{data['feedback']}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ **Ready to submit?**\n"
        f"Click '✅ CONFIRM & SEND' to submit your review!",
        reply_markup=get_confirmation_keyboard()
    )
    await state.set_state(ReviewState.waiting_for_confirmation)

@dp.message(F.text == "✅ CONFIRM & SEND", ReviewState.waiting_for_confirmation)
async def confirm_and_send_review(message: types.Message, state: FSMContext):
    """Finalize and send review to channel"""
    user_id = message.from_user.id
    current_time = time.time()
    
    # Anti-spam protection
    is_blocked, remaining_time, block_msg = check_spam_and_cooldown(user_id, current_time)
    if is_blocked:
        await message.answer(block_msg, parse_mode="Markdown")
        return
    
    user_last_action[user_id] = current_time
    
    # Anti-spam: Set processing state immediately
    await state.set_state(ReviewState.processing)
    
    data = await state.get_data()
    
    # Premium submission animation sequence
    msg = await message.answer("┌─────────────────────┐\n│ ⏳ **Submitting...** │\n└─────────────────────┘\n\n🔄 *Processing review...*\n░░░░░░░░░░ 0%")
    await asyncio.sleep(0.15)
    await msg.edit_text("┌─────────────────────┐\n│ 📡 **Submitting...** │\n└─────────────────────┘\n\n📤 *Transmitting data...*\n▰▰▰▰▰▱▱▱▱▱ 50%")
    await asyncio.sleep(0.15)
    await msg.edit_text("┌─────────────────────┐\n│ ✨ **Submitting...** │\n└─────────────────────┘\n\n💾 *Saving to database...*\n▰▰▰▰▰▰▰▰▱▱ 85%")
    await asyncio.sleep(0.15)
    await msg.edit_text("┌─────────────────────┐\n│ ✅ **Complete!** 🎉 │\n└─────────────────────┘\n\n🎊 *Review submitted!*\n▰▰▰▰▰▰▰▰▰▰ 100%")
    await asyncio.sleep(0.2)
    await msg.delete()
    
    # Prepare data
    user_id = str(message.from_user.id)
    name = message.from_user.first_name
    username = f"@{message.from_user.username}" if message.from_user.username else "N/A"
    timestamp = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
    current_time_ist = datetime.now(IST)
    
    # Check if user has previous review
    existing_review = col_reviews.find_one(
        {"user_id": user_id},
        sort=[("timestamp", -1)]
    )
    is_update = existing_review is not None
    
    # Send/update in private channel first (to get message_id)
    try:
        channel_message_id = await send_review_to_channel(
            user_id=user_id,
            name=name,
            username=username,
            rating=data['rating'],
            feedback=data['feedback'],
            timestamp=timestamp,
            is_update=is_update
        )
    except Exception as channel_error:
        logger.error(f"Failed to send to review channel: {channel_error}")
        channel_message_id = None  # Continue without channel message
    
    # Save to database - use INSERT for new, preserving history
    try:
        review_doc = {
            "user_id": user_id,
            "name": name,
            "username": username,
            "rating": data['rating'],
            "feedback": data['feedback'],
            "timestamp": current_time_ist,
            "date": timestamp,
            "channel_message_id": channel_message_id,
            "is_update": is_update,
            "cooldown_reset": False,  # Clear reset flag on new submission
            "submission_count": (existing_review.get("submission_count", 0) + 1) if existing_review else 1
        }
        
        # Replace old review with new one (upsert by user_id) - no duplicates per user
        col_reviews.replace_one(
            {"user_id": user_id},
            review_doc,
            upsert=True
        )
        
        # Invalidate cache after new review
        invalidate_review_cache()
        
        logger.info(f"💾 Review saved for user {user_id} (Type: {'Update' if is_update else 'New'}, Count: {review_doc['submission_count']})")
        
        # No notifications to Bot2 - admins can check reviews directly in Bot2
        
    except Exception as e:
        logger.error(f"Error saving review to DB: {e}")
    
    # Thank user with cooldown info and incentives
    await state.clear()
    
    # Calculate next review date (dynamic cooldown from settings)
    cooldown_seconds = get_review_cooldown_seconds()
    next_review_time = current_time_ist + timedelta(seconds=cooldown_seconds)
    next_review_formatted = next_review_time.strftime("%d %B %Y at %I:%M %p")
    
    status_text = "updated and resubmitted" if is_update else "successfully submitted"
    rating = data['rating']
    
    # Check milestone (using cached count for performance)
    total_reviews = get_cached_review_count()
    is_milestone = total_reviews in [1, 10, 25, 50, 100, 250, 500, 1000]
    
    # Generate reward message based on rating
    reward_msg = ""
    if rating == 5:
        reward_msg = (
            f"\n\n🎁 **EXCLUSIVE 5-STAR BONUS:**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✨ Priority Support Access\n"
            f"💎 Premium User Badge Unlocked\n"
            f"🌟 Early Access to New Features\n"
            f"🎯 Special Recognition in Community\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
    elif rating == 4:
        reward_msg = (
            f"\n\n💝 **4-STAR APPRECIATION:**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🙏 Thank you for your valuable feedback!\n"
            f"🎖️ Premium Member Recognition\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
    elif rating == 3:
        reward_msg = (
            f"\n\n🙏 **FEEDBACK BADGE EARNED:**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💬 Your constructive feedback helps us improve!\n"
            f"🎯 We're working to serve you better\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
    
    milestone_msg = ""
    if is_milestone:
        milestone_msg = (
            f"\n\n🏆 **MILESTONE ACHIEVEMENT!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎊 You're Reviewer #{total_reviews}!\n"
            f"🎁 Special Milestone Reward Unlocked\n"
            f"👑 Exclusive VIP Recognition\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
    
    # Calculate progress bar (starts at 100% when just submitted)
    progress_bar = "█" * 20  # Full bar initially
    percentage_complete = 100  # 100% remaining (full cooldown)
    
    await message.answer(
        f"╔═══════════════════════════╗\n"
        f"║ {'🎊' if rating >= 4 else '🎉'} **Thank You, {name}!** {'🎊' if rating >= 4 else '🎉'} ║\n"
        f"╚═══════════════════════════╝\n\n"
        f"✅ Your review has been **{status_text}**!\n\n"
        f"┏━━━━━━━━━━━━━━━━━━━━━━━┓\n"
        f"┃  📊 **YOUR REVIEW**    ┃\n"
        f"┗━━━━━━━━━━━━━━━━━━━━━━━┛\n"
        f"📅 **Date:** {timestamp}\n"
        f"⭐ **Rating:** {'⭐' * rating} **{rating}/5**\n"
        f"📊 **Score:** [{'★' * rating}{'☆' * (5 - rating)}]\n\n"
        f"┏━━━━━━━━━━━━━━━━━━━━━━━┓\n"
        f"┃ ⏳ **COOLDOWN STATUS** ┃\n"
        f"┗━━━━━━━━━━━━━━━━━━━━━━━┛\n"
        f"🔄 **Progress:**\n[{progress_bar}] {percentage_complete}%\n\n"
        f"⏱️ **Time Remaining:** `{REVIEW_COOLDOWN_DAYS} days`\n"
        f"🔓 **Next Review:** {next_review_formatted}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        f"{reward_msg}"
        f"{milestone_msg}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🛡️ *Quality control ensures meaningful feedback*\n"
        f"🙏 *Thank you for your time and honesty!*\n\n"
        f"🚀 **Continue Your Premium Journey!**",
        reply_markup=get_main_keyboard()
    )

@dp.message(ReviewState.waiting_for_confirmation)
async def enforce_confirmation(message: types.Message):
    """Enforce use of confirmation buttons"""
    # Ban protection
    if col_banned.find_one({"user_id": str(message.from_user.id)}):
        return
    
    try:
        await message.answer(
            "⚠️ **Please use the buttons above!**\n\n"
            "Click '✅ CONFIRM & SEND' to submit\n"
            "or '❌ CANCEL' to go back"
        )
    except TelegramForbiddenError:
        logger.warning(f"User {message.from_user.id} blocked the bot")
    except Exception as e:
        logger.error(f"Error in enforce_confirmation: {e}")

# ==========================================
# � CHANNEL SYNC HANDLER
# ==========================================

@dp.callback_query(lambda c: c.data and c.data.startswith("sync_review_"))
async def sync_review_status(callback: types.CallbackQuery):
    """Sync/refresh cooldown status for channel review report"""
    try:
        # Extract user_id from callback data
        user_id = callback.data.replace("sync_review_", "")
        
        # Anti-spam: Check last sync time
        current_time = time.time()
        last_sync = sync_cooldown.get(user_id, 0)
        
        if current_time - last_sync < ANTI_SPAM_FREEZE_SECONDS:
            await callback.answer("⏳ Please wait a moment before syncing again.", show_alert=False)
            return
        
        # Update last sync time
        sync_cooldown[user_id] = current_time
        
        # Show loading state
        await callback.answer("🔄 Syncing status...", show_alert=False)
        
        # Fetch review from database
        review = col_reviews.find_one({"user_id": user_id}, sort=[("timestamp", -1)])
        
        if not review:
            await callback.answer("❌ Review not found in database", show_alert=True)
            return
        
        # Get review details
        name = review.get("name", "Unknown")
        username = review.get("username", "N/A")
        rating = review.get("rating", 0)
        feedback = review.get("feedback", "No feedback")
        submitted_time = review.get("timestamp")
        
        # Ensure submitted_time is timezone-aware
        if submitted_time and submitted_time.tzinfo is None:
            submitted_time = IST.localize(submitted_time)
        
        # Calculate current cooldown status (dynamic from settings)
        now_ist = datetime.now(IST)
        cooldown_seconds = get_review_cooldown_seconds()
        cooldown_days = get_review_cooldown_days()
        next_review_date = (submitted_time + timedelta(seconds=cooldown_seconds)).strftime("%d-%m-%Y %I:%M %p")
        timestamp_str = submitted_time.strftime("%d-%m-%Y %I:%M %p")
        
        # Calculate elapsed time and cooldown bar
        elapsed_seconds = (now_ist - submitted_time).total_seconds()
        total_cooldown = cooldown_days * 24 * 60 * 60
        percentage_remaining = max(0, 100 - int((elapsed_seconds / total_cooldown) * 100))
        
        filled_blocks = int((percentage_remaining / 100) * 20)
        empty_blocks = 20 - filled_blocks
        cooldown_bar = "█" * filled_blocks + "░" * empty_blocks
        
        # Star rating display
        rating_bar = "★" * rating + "☆" * (5 - rating)
        star_emoji = "⭐" * rating
        
        # Build updated report
        status_badge = "🔄 SYNCED"
        sync_time = now_ist.strftime("%d-%m-%Y %I:%M %p")
        
        report = (
            f"🌟 **SYSTEM PERFORMANCE REVIEW** {status_badge}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 **OPERATIVE:** {name.upper()}\n"
            f"🆔 **USER ID:** `{user_id}`\n"
            f"🌐 **USERNAME:** {username}\n\n"
            f"📊 **RATING:** {star_emoji} **{rating}/5**\n"
            f"📈 **BAR:** [{rating_bar}]\n\n"
            f"💬 **FEEDBACK:**\n_{feedback}_\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ **VAULT STATUS:** 🟢 ACTIVE\n"
            f"⏳ **COOLDOWN:** [{cooldown_bar}] {percentage_remaining}%\n"
            f"🔓 **NEXT REVIEW:** {next_review_date}\n"
            f"⏰ **SUBMITTED:** {timestamp_str}\n"
            f"🔄 **LAST SYNCED:** {sync_time}"
        )
        
        # Create keyboard with sync button
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        sync_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 SYNC STATUS", callback_data=f"sync_review_{user_id}")]
        ])
        
        # Update the message
        await callback.message.edit_text(
            text=report,
            parse_mode="Markdown",
            reply_markup=sync_keyboard
        )
        
        await callback.answer(f"✅ Synced! Cooldown: {percentage_remaining}%", show_alert=True)
        logger.info(f"🔄 Synced review status for user {user_id} - {percentage_remaining}% remaining")
        
    except Exception as e:
        logger.error(f"Error syncing review status: {e}")
        await callback.answer("❌ Failed to sync status", show_alert=True)

# ==========================================
# �🛡️ REVIEW SAFETY HANDLERS
# ==========================================

@dp.message(ReviewState.waiting_for_stars, F.content_type.in_(['photo', 'video', 'document', 'sticker', 'animation', 'voice', 'video_note', 'audio']))
async def handle_media_in_star_selection(message: types.Message):
    """Handle media messages during star selection"""
    # Ban protection
    if col_banned.find_one({"user_id": str(message.from_user.id)}):
        return
    
    try:
        await message.answer(
            "⚠️ **Invalid Input Type**\n\n"
            "👆 Please select your rating using the **star buttons above**!\n\n"
            "🚫 Media files are not accepted at this stage.\n\n"
            "⭐ Click one of: 1-5 STARS"
        )
    except TelegramForbiddenError:
        logger.warning(f"User {message.from_user.id} blocked the bot")
    except Exception as e:
        logger.error(f"Error in handle_media_in_star_selection: {e}")

@dp.message(ReviewState.waiting_for_text, F.content_type.in_(['photo', 'video', 'document', 'sticker', 'animation', 'voice', 'video_note', 'audio']))
async def handle_media_in_text_input(message: types.Message):
    """Handle media messages during text input"""
    # Ban protection
    if col_banned.find_one({"user_id": str(message.from_user.id)}):
        return
    
    try:
        await message.answer(
            "⚠️ **Text Feedback Required**\n\n"
            "📝 Please write your feedback as **text message** only.\n\n"
            "🚫 We cannot accept:\n"
            "• Photos/Videos\n"
            "• Stickers/GIFs\n"
            "• Voice messages\n"
            "• Files/Documents\n\n"
            "✅ Type your review in text format (5-1000 characters)",
            reply_markup=get_cancel_keyboard()
        )
    except TelegramForbiddenError:
        logger.warning(f"User {message.from_user.id} blocked the bot")
    except Exception as e:
        logger.error(f"Error in handle_media_in_text_input: {e}")

@dp.message(ReviewState.waiting_for_confirmation, F.content_type.in_(['photo', 'video', 'document', 'sticker', 'animation', 'voice', 'video_note', 'audio']))
async def handle_media_in_confirmation(message: types.Message):
    """Handle media messages during confirmation"""
    # Ban protection
    if col_banned.find_one({"user_id": str(message.from_user.id)}):
        return
    
    try:
        await message.answer(
            "⚠️ **Button Action Required**\n\n"
            "👆 Please use the confirmation buttons above!\n\n"
            "✅ Click **CONFIRM & SEND** to submit\n"
            "❌ Click **CANCEL** to go back\n\n"
            "🚫 Media messages not accepted here."
        )
    except TelegramForbiddenError:
        logger.warning(f"User {message.from_user.id} blocked the bot")
    except Exception as e:
        logger.error(f"Error in handle_media_in_confirmation: {e}")

# ==========================================
# � GUIDE / HOW TO USE BUTTON HANDLER
# ==========================================

@dp.message(F.text == "📚 GUIDE / HOW TO USE")
async def handle_guide_button(message: types.Message):
    """Handle guide button click from main menu"""
    user_id = message.from_user.id
    
    # CRITICAL: Check if user is completely banned FIRST
    is_banned, ban_record, ban_msg = is_user_completely_banned(user_id)
    if is_banned:
        try:
            await message.answer(ban_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    # Ban check (legacy system - will be removed by new check above)
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    # Anti-spam protection with permanent ban for severe violations
    current_time = time.time()
    if user_id not in user_guide_views:
        user_guide_views[user_id] = []
    
    # Clean old timestamps
    user_guide_views[user_id] = [
        ts for ts in user_guide_views[user_id] 
        if current_time - ts < GUIDE_CLICK_WINDOW
    ]
    
    # Check for rapid clicking
    recent_views = len(user_guide_views[user_id])
    if recent_views >= 3:
        user_guide_spam[user_id] = user_guide_spam.get(user_id, 0) + 1
        
        # Severe spamming (10+ violations) = permanent ban
        if user_guide_spam[user_id] >= 10:
            # Get user info
            user_doc = col_users.find_one({"user_id": str(user_id)})
            msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
            user_name = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
            username = user_doc.get("username", "No Username") if user_doc else "No Username"
            
            # Permanent ban
            col_banned.insert_one({
                "user_id": str(user_id),
                "msa_id": msa_id,
                "username": username,
                "user_name": user_name,
                "reason": f"Guide Button Spam - {user_guide_spam[user_id]} violations",
                "violation_type": "Guide Button Spam",
                "banned_from": "Guide Button",
                "banned_at": datetime.now(IST),
                "banned_by": "MSANode Security Agent",
                "ban_type": "permanent",
                "ban_until": None,
                "banned_features": ["downloads", "reviews", "support", "search", "dashboard", "guide", "faq"]
            })
            
            # Send ban report
            asyncio.create_task(send_ban_report(
                user_id=user_id,
                reason=f"Guide Button Spam - {user_guide_spam[user_id]} violations",
                violation_type="Guide Button Spam",
                banned_from="Guide Button",
                banned_by="MSANode Security Agent"
            ))
            
            try:
                await message.answer(
                    f"🚫 **PERMANENTLY BANNED**\n\n"
                    f"❌ You have been banned for excessive spam\n\n"
                    f"**📋 Your Details:**\n"
                    f"• MSA ID: {msa_id}\n\n"
                    f"⚠️ Reason: Guide Button Spam\n\n"
                    f"💀 This ban is permanent and irreversible.\n"
                    f"💬 Use Customer Support below to appeal.",
                    parse_mode="Markdown",
                    reply_markup=get_main_keyboard(user_id)
                )
            except:
                pass
            return
        
        if user_guide_spam[user_id] >= GUIDE_SPAM_FREEZE:
            try:
                await message.answer(
                    "🚫 **GUIDE ACCESS FROZEN**\n\n"
                    f"⚠️ You've been clicking too rapidly!\n\n"
                    f"⏸️ **Freeze Duration:** 30 seconds\n"
                    f"📊 **Spam Count:** {user_guide_spam[user_id]}\n\n"
                    "💡 Please use the guide normally without spamming.",
                    parse_mode="Markdown"
                )
            except:
                pass
            await asyncio.sleep(30)
            user_guide_spam[user_id] = 0
            return
    
    # Add current view
    user_guide_views[user_id].append(current_time)
    
    # ULTRA PREMIUM 10-STAGE ANIMATION
    loading = await message.answer("⚡ **INITIALIZING GUIDE SYSTEM...**", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🔐 **[▓░░░░░░░░░] 10%** - Authenticating Access...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🔍 **[▓▓░░░░░░░░] 20%** - Scanning Features Database...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("📡 **[▓▓▓░░░░░░░] 30%** - Connecting to Documentation Server...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("📖 **[▓▓▓▓░░░░░░] 40%** - Compiling User Guides...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("⚙️ **[▓▓▓▓▓░░░░░] 50%** - Processing Instructions...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("✨ **[▓▓▓▓▓▓░░░░] 60%** - Formatting Content Layout...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🎨 **[▓▓▓▓▓▓▓░░░] 70%** - Applying Premium Styling...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🎯 **[▓▓▓▓▓▓▓▓░░] 80%** - Preparing Interactive Menu...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🚀 **[▓▓▓▓▓▓▓▓▓░] 90%** - Finalizing User Experience...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("💎 **[▓▓▓▓▓▓▓▓▓▓] 100%** - ✅ GUIDE SYSTEM READY!", parse_mode="Markdown")
    await asyncio.sleep(0.2)
    await loading.delete()
    
    # Get user info
    user_doc = col_users.find_one({"user_id": str(user_id)})
    msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
    user_name = user_doc.get("first_name", "User") if user_doc else "User"
    
    guide_message = (
        "📚 **MSANode AGENT - Complete User Guide**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 **Welcome, {user_name}!**\n"
        f"🏷️ **Your MSA ID:** `{msa_id}`\n\n"
        "🎯 **Learn How To Use Each Feature:**\n\n"
        "This guide explains every button and feature in detail.\n"
        "Choose any topic below to get step-by-step instructions!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "**📖 Available Sections:**\n\n"
        "📚 **Support System** - Complete guide on getting help\n"
        "⭐ **Review System** - How reviews work & tips\n"
        "🛡️ **Anti-Spam Info** - All protections explained\n"
        "⚡ **Buttons** - Every button explained\n"
        "💎 **Premium Features** - Advanced functionality\n"
        "❓ **FAQ** - 15 frequently asked questions\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 **Pro Tip:** Select any button below to start learning!\n\n"
        "Each section includes detailed explanations, examples,\n"
        "and everything you need to master the bot!"
    )
    
    try:
        await message.answer(
            guide_message,
            reply_markup=get_guide_main_keyboard(),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error sending guide menu: {e}")


# ==========================================
# 📜 RULES & REGULATIONS SYSTEM
# ==========================================

@dp.message(F.text == "📜 RULES & REGULATIONS")
async def cmd_rules(message: types.Message):
    """Display comprehensive rules and regulations with user profile"""
    user_id = message.from_user.id
    
    # Check if user is completely banned
    is_banned, ban_record, ban_msg = is_user_completely_banned(user_id)
    if is_banned:
        try:
            await message.answer(ban_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    # Check if feature is banned
    if is_user_banned_from_feature(user_id, "dashboard"):
        feature_name = "rules"
        try:
            await message.answer(
                f"🚫 **Feature Restricted**\n\n"
                f"You are currently restricted from accessing {feature_name}.\n\n"
                f"💡 Contact support if you believe this is an error.",
                parse_mode="Markdown"
            )
        except Exception as e:
            await message.answer(f"❌ {feature_name.title()} feature is currently unavailable.")
            print(f"Error sending feature ban message: {e}")
        return
    
    # Fetch user from database to check if they exist
    user_doc = col_users.find_one({"user_id": str(user_id)})
    
    if user_doc:
        msa_id = user_doc.get("msa_id", "Not Assigned")
        join_date = user_doc.get("timestamp", "Unknown")
        if isinstance(join_date, datetime):
            join_date = join_date.strftime("%d %b %Y")
        status = user_doc.get("status", "active")
    else:
        msa_id = "Not Assigned"
        join_date = "Today"
        status = "new"
    
    # Loading animation
    loading = await message.answer("📜 **Loading Rules & Regulations...**")
    await asyncio.sleep(0.6)
    await loading.edit_text("📋 **Fetching Guidelines from Database...**")
    await asyncio.sleep(0.5)
    await loading.delete()
    
    rules_message = (
        f"╔══════════════════════════╗\n"
        f"║   📜 **MSANode AGENT PROTOCOLS** ║\n"
        f"╚══════════════════════════╝\n\n"
        f"👤 **Operative Dossier:**\n"
        f"• MSA ID: `{msa_id}`\n"
        f"• Clearance: {status.upper()}\n"
        f"• Active Since: {join_date}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"**1️⃣ OPERATIVE ETHICS**\n"
        f"• Maintain professional conduct within the Syndicate.\n"
        f"• Zero tolerance for harassment or unauthorized disruption.\n"
        f"• Use all assets and blueprints for their intended purpose only.\n\n"
        f"**2️⃣ TRANSMISSION EFFICIENCY**\n"
        f"• Avoid redundant command execution (Spam).\n"
        f"• Systematic flooding of the /start command results in immediate revocation of access.\n"
        f"• Respect the bandwidth of the Vault Core.\n\n"
        f"**3️⃣ INTELLIGENCE AUDITS**\n"
        f"• Provide honest and objective ratings for all blueprints.\n"
        f"• Fake or manipulative feedback is a violation of Syndicate trust.\n\n"
        f"**4️⃣ PRIORITY SUPPORT LINE**\n"
        f"• Use Customer Support for critical technical failures only.\n"
        f"• Protocol allows for only one active support ticket per operative.\n"
        f"• Patience is required; intelligence reports are processed in order of priority.\n\n"
        f"**5️⃣ ACCESS CONTROL & SECURITY**\n"
        f"• Permanent membership in the Telegram Vault is mandatory.\n"
        f"• Revoking your membership will trigger an automatic lockout from all blueprints.\n"
        f"• Social synchronization (IG/YT) is required to maintain clearance.\n\n"
        f"**6️⃣ DATA PRIVACY**\n"
        f"• Your activity logs are encrypted and stored within our secure data core.\n"
        f"• Your MSA ID is your unique identifier; keep your terminal secure.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"**✅ BY PROCEEDING, YOU ACKNOWLEDGE:**\n"
        f"• Full compliance with the protocols stated above.\n"
        f"• Admin decisions regarding access revocation are final.\n\n"
        f"**⚠️ DISCIPLINARY ACTIONS:**\n"
        f"• Protocol Breach 1: Official Warning\n"
        f"• Protocol Breach 2: Temporary Terminal Suspension\n"
        f"• Protocol Breach 3: Permanent Revocation of Syndicate Status\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 **Note:** These protocols ensure the stability and exclusivity of the MSANode ecosystem.\n\n"
        f"🤝 **Thank you for your professionalism, {message.from_user.first_name}.**\n\n"
        f"🎯 **Protocols Updated:** 2026 by MSA NODE "
    )
    await message.answer(rules_message, parse_mode="Markdown")
    
    # Log the rules view
    if user_doc:
        try:
            col_users.update_one(
                {"user_id": str(user_id)},
                {
                    "$set": {"last_rules_view": datetime.now(IST)},
                    "$inc": {"rules_view_count": 1}
                }
            )
        except Exception as e:
            print(f"Error logging rules view: {e}")


# ==========================================
# ❓ FAQ SYSTEM
# ==========================================

@dp.message(F.text == "❓ FAQ / HELP")
async def handle_faq(message: types.Message):
    """Handle FAQ button - show frequently asked questions"""
    user_id = message.from_user.id
    
    # CRITICAL: Check if user is completely banned FIRST
    is_banned, ban_record, ban_msg = is_user_completely_banned(user_id)
    if is_banned:
        try:
            await message.answer(ban_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    # Ban check (legacy system - will be removed by new check above)
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    faq_intro = (
        "❓ **Frequently Asked Questions**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💡 **Quick Answers to Common Questions!**\n\n"
        "Select any question below to get an instant answer.\n"
        "These are the most common questions users ask.\n\n"
        "👇 **Choose a topic:**"
    )
    
    try:
        await message.answer(
            faq_intro,
            reply_markup=get_faq_keyboard(),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error sending FAQ menu: {e}")

@dp.message(F.text == "❓ How to submit a review?")
async def faq_submit_review(message: types.Message):
    """FAQ: How to submit a review"""
    answer = (
        "⭐ **How to Submit a Review?**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Step-by-Step:**\n\n"
        "1️⃣ Click the **⭐ REVIEW** button from main menu\n"
        "2️⃣ Select your rating (1-5 stars)\n"
        "3️⃣ Write your honest feedback (minimum 10 characters)\n"
        "4️⃣ Confirm submission\n"
        "5️⃣ Done! Your review is submitted ✅\n\n"
        "**Important Notes:**\n"
        "• Reviews are posted to admin channel\n"
        "• You can review once every 7 days\n"
        "• Minimum 10 characters required\n"
        "• Be honest and constructive!\n\n"
        "💡 **Tip:** Quality reviews help improve the service!"
    )
    await message.answer(answer, reply_markup=get_faq_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "⏰ Why can't I review again?")
async def faq_cooldown(message: types.Message):
    """FAQ: Review cooldown explanation"""
    answer = (
        "⏰ **Review Cooldown Explained**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Cooldown Period:** Exactly 7 days (604,800 seconds)\n\n"
        "**Why 7 Days?**\n"
        "• Prevents spam and abuse\n"
        "• Ensures quality feedback\n"
        "• Gives you time to experience more\n"
        "• Protects bot from bans\n\n"
        "**What You'll See:**\n"
        "• Exact time remaining (days, hours, minutes)\n"
        "• Progress bar showing cooldown status\n"
        "• Next available review date & time\n\n"
        "**After 7 Days:**\n"
        "✅ Your cooldown automatically resets\n"
        "✅ You can submit a new review\n"
        "✅ Previous review stays in history\n\n"
        "💡 **Tip:** Check dashboard to see cooldown status anytime!"
    )
    await message.answer(answer, reply_markup=get_faq_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "📱 How to contact support?")
async def faq_contact_support(message: types.Message):
    """FAQ: How to contact support"""
    answer = (
        "📱 **How to Contact Support?**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Quick Steps:**\n\n"
        "1️⃣ Click **💬 CUSTOMER SUPPORT** from main menu\n"
        "2️⃣ Choose issue type from templates OR\n"
        "3️⃣ Select \"✍️ Other Issue\" to write custom message\n"
        "4️⃣ Your ticket goes directly to admin\n"
        "5️⃣ Admin will reply in support channel\n\n"
        "**Issue Templates:**\n"
        "📄 PDF/Link Not Working\n"
        "🤖 Bot Not Responding\n"
        "⭐ Review Issue\n"
        "🔗 Access/Channel Problem\n"
        "❓ Content Question\n"
        "⚙️ Account/Settings Help\n\n"
        "**Response Time:**\n"
        "⚡ Usually within 24 hours\n"
        "🔔 You'll get notification when admin replies\n\n"
        "💡 **Tip:** Use templates for faster resolution!"
    )
    await message.answer(answer, reply_markup=get_faq_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "🔒 Why is my review not showing?")
async def faq_review_not_showing(message: types.Message):
    """FAQ: Why review not showing"""
    answer = (
        "🔒 **Why Is My Review Not Showing?**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Possible Reasons:**\n\n"
        "1️⃣ **Still in Cooldown**\n"
        "   • Check if setted or 7 days have passed\n"
        "   • View exact time in dashboard\n\n"
        "2️⃣ **Review Too Short**\n"
        "   • Minimum 10 characters required\n"
        "   • Write more detailed feedback\n\n"
        "3️⃣ **Spam Detection**\n"
        "   • Don't click too fast\n"
        "   • Wait for cooldown to expire\n\n"
        "4️⃣ **Already Submitted**\n"
        "   • Can only review once per 7 or setted days\n"
        "   • Check dashboard for last review\n\n"
        "**Where Reviews Go:**\n"
        "✅ Admin review channel (for moderation)\n"
        "✅ Database (permanent record)\n"
        "✅ Your profile history\n\n"
        "💡 **Tip:** Click 📊 DASHBOARD to check your review status!"
    )
    await message.answer(answer, reply_markup=get_faq_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "⚖️ What are the spam protections?")
async def faq_spam_protections(message: types.Message):
    """FAQ: Spam protection explanation"""
    answer = (
        "⚖️ **Spam Protection System**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Multi-Layer Protection:**\n\n"
        "🛡️ **1. Anti-Spam Freeze**\n"
        "• Exactly 2.0 seconds between review clicks\n"
        "• Prevents accidental spam\n"
        "• Silent protection (no messages)\n\n"
        "⚠️ **2. Progressive Cooldowns**\n"
        "• 1st offense: 30 seconds\n"
        "• 2nd offense: 60 seconds (1 min)\n"
        "• 3rd offense: 180 seconds (3 min)\n"
        "• 4th offense: 300 seconds (5 min)\n"
        "• 5th+ offense: 600 seconds (10 min)\n\n"
        "🚫 **3. Rate Limiting**\n"
        "• Maximum 10 requests per 60 seconds\n"
        "• Protects server from overload\n"
        "• Fair usage for all users\n\n"
        "⏰ **4. Review Cooldown**\n"
        "• Exactly 7 days (604,800 seconds) or setted days \n"
        "• Cannot be bypassed\n"
        "• Automatic reset after period\n\n"
        "**Why These Protections?**\n"
        "✅ Prevents bot bans from Telegram\n"
        "✅ Ensures quality over quantity\n"
        "✅ Fair system for all users\n"
        "✅ Protects database from abuse\n\n"
        "💡 **Tip:** Normal usage = No problems!"
    )
    await message.answer(answer, reply_markup=get_faq_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "📊 How to check my dashboard?")
async def faq_dashboard(message: types.Message):
    """FAQ: Dashboard explanation"""
    answer = (
        "📊 **Your Dashboard Guide**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Access Dashboard:**\n"
        "Click **📊 DASHBOARD** button from main menu\n\n"
        "**What You'll See:**\n\n"
        "👤 **Profile Info:**\n"
        "• Your MSA User ID\n"
        "• Username\n\n"
        "📰 **News & Updates:**\n"
        "• Pending support tickets\n"
        "• Review status\n"
        "• Important announcements\n\n"
        "⏱️ **Support Ticket Status:**\n"
        "• Pending ticket details\n"
        "• How long it's been waiting\n"
        "• Response time tracking\n\n"
        "**Dashboard Actions:**\n"
        "🔄 REFRESH STATUS - Update latest info\n"
        "🚫 CANCEL MY TICKET - Cancel pending support\n"
        "🏠 BACK TO MAIN MENU - Return to main\n\n"
        "**Why Use Dashboard?**\n"
        "✅ See all your activity in one place\n"
        "✅ Track ticket progress\n"
        "✅ Check review cooldown\n"
        "✅ Stay updated on important news\n\n"
        "💡 **Tip:** Refresh frequently to see latest updates!"
    )
    await message.answer(answer, reply_markup=get_faq_keyboard(), parse_mode="Markdown")


# ==========================================
# 📊 USER DASHBOARD SYSTEM
# ==========================================

@dp.message(F.text == "📊 DASHBOARD")
async def show_dashboard(message: types.Message, state: FSMContext):
    """Display simple user dashboard with ID, username, and pending items"""
    user_id = str(message.from_user.id)
    
    # CRITICAL: Check if user is completely banned
    is_banned, ban_record, ban_msg = is_user_completely_banned(int(user_id))
    if is_banned:
        try:
            await message.answer(ban_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    # CRITICAL: Check if user has accepted terms & conditions
    if not has_accepted_terms(int(user_id)):
        await message.answer(
            "⚠️ **Terms & Conditions Required**\n\n"
            f"{message.from_user.first_name}, you must accept our Terms & Conditions before using any bot features.\n\n"
            "📜 Please accept the terms to continue.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✅ I Accept the Terms & Conditions")],
                    [KeyboardButton(text="❌ I Reject")]
                ],
                resize_keyboard=True,
                one_time_keyboard=False
            ),
            parse_mode="Markdown"
        )
        return
    
    # Only basic rate limiting for navigation (no progressive spam bans)
    is_rate_limited, rate_msg = check_rate_limit(user_id)
    if is_rate_limited:
        await message.answer(rate_msg, parse_mode="Markdown")
        return
    
    # Get user data
    user_doc = col_users.find_one({"user_id": user_id})
    
    if not user_doc:
        await message.answer(
            "Profile not found. Please use /start first.",
            reply_markup=get_main_keyboard()
        )
        return
    
    # Get basic info
    msa_id = user_doc.get("msa_id", "UNKNOWN")
    username = user_doc.get("username", "No Username")
    
    # Check for pending items
    pending_items = []
    
    # Check review status
    user_review = col_reviews.find_one({"user_id": user_id})
    if not user_review:
        pending_items.append("- Review not submitted yet")
    
    # Check pending support ticket - verify both in-memory and database
    pending_ticket = user_support_pending.get(user_id)
    db_support_status = user_doc.get("support_status") if user_doc else None
    
    # Clear in-memory if database shows resolved/responded
    if db_support_status in ["resolved", "responded"] and pending_ticket:
        user_support_pending[user_id]['status'] = 'cleared'
        pending_ticket = None  # Don't show as pending
    
    if pending_ticket and pending_ticket.get('status') == 'pending':
        ticket_msg = pending_ticket.get('message', '')[:40]
        ticket_time = pending_ticket.get('timestamp', time.time())
        elapsed_seconds = int(time.time() - ticket_time)
        
        # Calculate wait time
        hours = elapsed_seconds // 3600
        minutes = (elapsed_seconds % 3600) // 60
        
        if hours > 0:
            wait_time = f"{hours}h {minutes}m"
        else:
            wait_time = f"{minutes}m"
        
        # Check if admin replied
        reply_count = pending_ticket.get('reply_count', 0)
        if reply_count > 0:
            pending_items.append(f"- 💬 Support: {ticket_msg}... (Admin replied {reply_count}x, waiting {wait_time})")
        else:
            pending_items.append(f"- ⏳ Support: {ticket_msg}... (Waiting {wait_time})")
    
    # Build simple dashboard
    # Check maintenance status
    maint = col_settings.find_one({"setting": "maintenance"})
    is_maintenance = maint and maint.get("value")
    
    if is_maintenance:
        agent_status = "🔴 Offline (Maintenance)"
        heartbeat = "💔 Paused"
    else:
        agent_status = "🟢 Online"
        heartbeat = "💚 Live • Breathing"
    
    dashboard_msg = (
        "╔═════════════════════════╗\n"
        "║  📊 **YOUR DASHBOARD**  ║\n"
        "╚═════════════════════════╝\n\n"
        f"**MSANODE AGENT:** {agent_status}\n"
        f"**Heartbeat:** {heartbeat}\n"
        "⚡ **Status:** Active\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"**👤 Profile Information:**\n"
        f"• MSA ID: `{msa_id}`\n"
        f"• Username: {username}\n\n"
    )
    
    if pending_items:
        dashboard_msg += "**📋 PENDING UPDATES:**\n"
        dashboard_msg += "\n".join(pending_items)
    else:
        dashboard_msg += "**📰 NEWS & UPDATES:**\n"
        dashboard_msg += "✅ No new updates at this time.\n"
    
    dashboard_msg += "\n━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Only show cancel button if truly pending (not resolved in database)
    has_active_ticket = (pending_ticket and pending_ticket.get('status') == 'pending' and 
                         db_support_status not in ["resolved", "responded"])
    
    await message.answer(
        dashboard_msg,
        reply_markup=get_dashboard_actions_keyboard(has_pending_ticket=has_active_ticket),
        parse_mode="Markdown"
    )

@dp.message(F.text == "🚫 CANCEL MY TICKET")
async def cancel_ticket(message: types.Message, state: FSMContext):
    """Allow user to cancel their pending support ticket"""
    user_id = str(message.from_user.id)
    
    # Only basic rate limiting for ticket actions (no progressive spam bans)
    is_rate_limited, rate_msg = check_rate_limit(user_id)
    if is_rate_limited:
        await message.answer(rate_msg, parse_mode="Markdown")
        return
    
    # Check if user has pending ticket
    pending_ticket = user_support_pending.get(user_id)
    
    if not pending_ticket or pending_ticket.get('status') != 'pending':
        await message.answer(
            "No Pending Ticket\n\n"
            "You don't have any active support requests to cancel.\n\n"
            "Use the Dashboard to check your status anytime!",
            reply_markup=get_main_keyboard()
        )
        return
    
    # Simple cancellation message
    loading = await message.answer("Processing cancellation...")
    await asyncio.sleep(0.3)
    await loading.delete()
    
    # Get ticket details
    ticket_id = pending_ticket.get('channel_msg_id', 'N/A')
    ticket_time = pending_ticket.get('timestamp', time.time())
    ticket_datetime = datetime.fromtimestamp(ticket_time, IST)
    submitted_at = ticket_datetime.strftime("%d-%m-%Y %I:%M %p")
    
    # Update support channel message
    try:
        if SUPPORT_CHANNEL_ID and ticket_id != 'N/A':
            cancel_update = (
                "TICKET CANCELLED BY USER\n"
                "=====================\n\n"
                f"User: {pending_ticket.get('user_name', 'Unknown')}\n"
                f"TELEGRAM ID: {user_id}\n"
                f"Submitted: {submitted_at}\n"
                f"Cancelled: {datetime.now(IST).strftime('%d-%m-%Y %I:%M %p')}\n\n"
                "=====================\n"
                "Original Request:\n\n"
                f"{pending_ticket.get('message', 'N/A')}\n\n"
                "=====================\n"
                "Status: CANCELLED BY USER\n"
                "====================="
            )
            
            await bot.edit_message_text(
                text=cancel_update,
                chat_id=SUPPORT_CHANNEL_ID,
                message_id=ticket_id
            )
    except Exception as e:
        logger.error(f"Error updating cancelled ticket in channel: {e}")
    
    # Mark as cancelled (not pending)
    user_support_pending[user_id]['status'] = 'cancelled'
    
    # Confirmation to user
    await message.answer(
        "TICKET CANCELLED SUCCESSFULLY\n\n"
        f"Ticket ID: #{ticket_id}\n"
        f"Was Submitted: {submitted_at}\n"
        f"Cancelled At: {datetime.now(IST).strftime('%I:%M %p')}\n\n"
        "=====================\n"
        "Your support request has been withdrawn.\n"
        "You can submit a new request anytime.\n\n"
        "=====================",
        reply_markup=get_main_keyboard()
    )
    
    logger.info(f"User {user_id} cancelled support ticket #{ticket_id}")

@dp.message(F.text == "🔄 REFRESH STATUS")
async def refresh_dashboard(message: types.Message, state: FSMContext):
    """Refresh dashboard to show latest status"""
    user_id = str(message.from_user.id)
    
    # Only basic rate limiting for refresh (no progressive spam bans)
    is_rate_limited, rate_msg = check_rate_limit(user_id)
    if is_rate_limited:
        try:
            await message.answer(rate_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    # Quick refresh animation
    loading = await message.answer("🔄 Refreshing...")
    await asyncio.sleep(0.1)
    await loading.edit_text("📡 Fetching Latest Data...")
    await asyncio.sleep(0.1)
    await loading.edit_text("✅ Updated!")
    await asyncio.sleep(0.1)
    await loading.delete()
    
    # Show dashboard again
    await show_dashboard(message, state)

@dp.message(F.text == "🏠 BACK TO MAIN MENU")
async def back_to_main_from_dashboard(message: types.Message, state: FSMContext):
    """Return to main menu from dashboard - no spam check needed for navigation"""
    await message.answer(
        "🏠 Returned to Main Menu\n\nWelcome back! Choose an option:",
        reply_markup=get_main_keyboard()
    )

# ==========================================
# �💬 CUSTOMER SUPPORT SYSTEM
# ==========================================

@dp.message(F.text == "💬 CUSTOMER SUPPORT")
async def start_customer_support(message: types.Message, state: FSMContext):
    """Handle customer support button click with spam protection
    Note: Banned users CANNOT use this - they must use Appeal Ban button"""
    user_id = str(message.from_user.id)
    current_time = time.time()
    
    # CRITICAL: Check if user is completely banned - block them from support
    is_banned, ban_record, ban_msg = is_user_completely_banned(int(user_id))
    if is_banned:
        try:
            await message.answer(
                f"⛔ **CUSTOMER SUPPORT NOT AVAILABLE**\n\n"
                f"You are currently banned from using this bot.\n\n"
                f"🔔 **To appeal your ban:**\n"
                f"Use the 🔔 **APPEAL BAN** button below.\n\n"
                f"💬 Customer Support is only for active users.",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(int(user_id))
            )
        except:
            pass
        return
    
    # CRITICAL: Check if user has accepted terms & conditions
    if not has_accepted_terms(int(user_id)):
        await message.answer(
            "⚠️ **Terms & Conditions Required**\n\n"
            f"{message.from_user.first_name}, you must accept our Terms & Conditions before using any bot features.\n\n"
            "📜 Please accept the terms to continue.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✅ I Accept the Terms & Conditions")],
                    [KeyboardButton(text="❌ I Reject")]
                ],
                resize_keyboard=True,
                one_time_keyboard=False
            ),
            parse_mode="Markdown"
        )
        return
    
    # Check if support feature is banned for this user (partial ban)
    user_data = col_users.find_one({"user_id": user_id})
    if is_feature_banned(user_data, 'support'):
        await send_feature_ban_message(message, 'support', user_data)
        return
    
    # Ban check (legacy system)
    ban_record = col_banned.find_one({"user_id": user_id})
    if ban_record:
        try:
            await message.answer(
                "🚫 **PERMANENTLY BANNED**\n\n"
                "❌ You have been banned from using this bot\n"
                "⚠️ Reason: Spam/Abuse Detection\n\n"
                "💀 This action is permanent and irreversible.",
                parse_mode="Markdown"
            )
        except:
            pass
        return
    
    # Support button spam protection
    if user_id not in user_support_clicks:
        user_support_clicks[user_id] = []
    
    # Clean old clicks outside the time window
    user_support_clicks[user_id] = [
        click_time for click_time in user_support_clicks[user_id]
        if current_time - click_time < SUPPORT_CLICK_WINDOW
    ]
    
    # Add current click
    user_support_clicks[user_id].append(current_time)
    click_count = len(user_support_clicks[user_id])
    
    # Progressive spam detection
    if click_count >= SUPPORT_BAN_CLICKS:
        # Get user info including MSA ID
        user_doc = col_users.find_one({"user_id": user_id})
        msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
        user_name = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
        username = user_doc.get("username", "No Username") if user_doc else "No Username"
        
        # PERMANENT BAN with complete information
        col_banned.insert_one({
            "user_id": user_id,
            "msa_id": msa_id,
            "username": username,
            "user_name": user_name,
            "reason": "Customer Support Button Spam - Excessive rapid clicking",
            "violation_type": "Support Button Spam",
            "banned_from": "Customer Support Button",
            "banned_at": datetime.now(IST),
            "banned_by": "MSANode Security Agent",
            "ban_type": "permanent",
            "ban_until": None,
            "click_count": click_count,
            "banned_features": ["downloads", "reviews", "support", "search", "dashboard", "guide", "faq"]
        })
        
        # Log to ban history
        col_ban_history.insert_one({
            "user_id": user_id,
            "msa_id": msa_id,
            "username": username,
            "user_name": user_name,
            "action_type": "auto_ban",
            "admin_name": "MSANode Security Agent",
            "reason": f"Customer Support Button Spam - {click_count} rapid clicks",
            "ban_type": "permanent",
            "ban_until": None,
            "banned_features": ["downloads", "reviews", "support", "search", "dashboard", "guide", "faq"],
            "banned_from": "Customer Support Button",
            "violation_type": "Support Button Spam",
            "timestamp": datetime.now(IST)
        })
        
        # Send detailed ban report
        asyncio.create_task(send_ban_report(
            user_id=int(user_id),
            reason=f"Customer Support Button Spam - {click_count} rapid clicks",
            violation_type="Support Button Spam",
            banned_from="Customer Support Button",
            banned_by="MSANode Security Agent"
        ))
        
        try:
            await message.answer(
                f"🚫 **PERMANENTLY BANNED**\n\n"
                f"❌ You have been automatically banned\n\n"
                f"**📋 Your Details:**\n"
                f"• MSA ID: {msa_id}\n\n"
                f"⚠️ Reason: Excessive spam detected\n"
                f"📊 Violations: {click_count} rapid clicks\n\n"
                f"💀 **This ban is permanent and cannot be reversed.**\n\n"
                f"⚠️ Please respect bot usage guidelines.\n"
                f"💬 Use Customer Support below to appeal.",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(int(user_id))
            )
        except:
            pass
        
        logger.warning(f"🚫 User {user_id} (MSA: {msa_id}) PERMANENTLY BANNED for support button spam ({click_count} clicks)")
        return
    
    elif click_count >= SUPPORT_MAX_CLICKS:
        # WARNING - Freeze for 15 seconds
        freeze_until = current_time + 15
        remaining = 15
        
        try:
            await message.answer(
                "⚠️ **SPAM DETECTED - COOLDOWN ACTIVE**\n\n"
                f"🛑 You are clicking too fast!\n"
                f"⏰ Frozen for: {remaining} seconds\n"
                f"📊 Violations: {click_count}/{SUPPORT_BAN_CLICKS}\n\n"
                "⚠️ **Warning:** One more violation = Permanent Ban\n\n"
                "💡 *Please wait before trying again.*",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard()
            )
        except:
            pass
        
        logger.warning(f"⚠️ User {user_id} frozen for support spam ({click_count} clicks)")
        return
    
    # Check eligibility (cooldown and daily limits)
    can_submit, block_message = check_support_eligibility(user_id)
    if not can_submit:
        try:
            await message.answer(
                block_message,
                parse_mode="Markdown",
                reply_markup=get_main_keyboard()
            )
        except:
            pass
        return
    
    # Check if user has PENDING support request (check both database and memory)
    # First check database status
    user_doc = col_users.find_one({"user_id": user_id})
    db_support_status = user_doc.get("support_status") if user_doc else None
    
    # If database shows resolved, clear the in-memory pending status
    if db_support_status == "resolved" and user_id in user_support_pending:
        user_support_pending[user_id]['status'] = 'cleared'
    
    # Now check if still pending
    if user_id in user_support_pending and user_support_pending[user_id].get('status') == 'pending':
        # Double check database to ensure it's actually pending
        if db_support_status in ["resolved", "responded"]:
            # Database shows resolved but memory was outdated - clear it
            user_support_pending[user_id]['status'] = 'cleared'
        else:
            # Truly pending - show pending message
            pending_data = user_support_pending[user_id]
            pending_time = datetime.fromtimestamp(pending_data['timestamp'], IST).strftime("%d-%m-%Y %I:%M %p")
            
            try:
                await message.answer(
                    "⏳ **Support Request PENDING**\n\n"
                    f"📩 You already have a pending request\n"
                    f"🕐 Submitted: {pending_time}\n"
                    f"📊 Status: ⏳ PENDING\n\n"
                    "✨ Our team will respond soon!\n"
                    "⏰ Please wait for resolution before submitting another request.\n\n"
                    "💎 *Thank you for your patience!*",
                    parse_mode="Markdown",
                    reply_markup=get_main_keyboard()
                )
            except Exception as e:
                logger.error(f"Error showing pending message: {e}")
            return
    
    # Ask for support message with premium animation
    try:
        msg = await message.answer("💬 **Customer Support**", parse_mode="Markdown")
        await asyncio.sleep(0.12)
        await msg.edit_text("💬 **Customer Support**\n🔍 *Initializing...*", parse_mode="Markdown")
        await asyncio.sleep(0.12)
        await msg.edit_text("💬 **Customer Support**\n⚙️ *Loading Interface...*", parse_mode="Markdown")
        await asyncio.sleep(0.12)
        await msg.edit_text("💬 **Customer Support**\n🎯 *Preparing Options...*", parse_mode="Markdown")
        await asyncio.sleep(0.12)
        await msg.edit_text("💬 **Customer Support**\n✨ *Almost Ready...*", parse_mode="Markdown")
        await asyncio.sleep(0.12)
        
        # Delete the animation message and send issue selection
        try:
            await msg.delete()
        except:
            pass
        
        await message.answer(
            "💬 **Customer Support**\n\n"
            "🎯 **Choose an Option Below:**\n\n"
            "📚 **Self-Help Solutions** (Instant!)\n"
            "   • View common issue solutions\n"
            "   • No waiting - solve it yourself!\n"
            "   • Free unlimited access\n\n"
            "📝 **Custom Support Request**\n"
            "   • For unique/complex issues\n"
            "   • Direct message to support team\n"
            "   • Response within 24 hours\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💡 **Tip:** Try self-help first - 80% of issues are solved instantly!\n\n"
            "Choose the option that best describes your problem,\n"
            "or select 'Other Issue' to type a custom message.\n\n"
            "💡 **Using templates helps us:**\n"
            "   • Respond faster\n"
            "   • Understand your issue better\n"
            "   • Provide accurate solutions\n\n"
            "⬇️ **Select an option below:**",
            parse_mode="Markdown",
            reply_markup=get_support_issues_keyboard()
        )
        await state.set_state(SupportState.selecting_issue)
    except Exception as e:
        logger.error(f"Error starting customer support: {e}")

# ==========================================
# � APPEAL BAN SYSTEM
# ==========================================

@dp.message(F.text == "🔔 APPEAL BAN")
async def start_appeal_process(message: types.Message, state: FSMContext):
    """Handle appeal ban button - only for banned users"""
    user_id = str(message.from_user.id)
    current_time = time.time()
    
    # Check if user is actually banned
    is_banned, ban_record, ban_msg = is_user_completely_banned(int(user_id))
    if not is_banned:
        try:
            await message.answer(
                "✅ **NO BAN FOUND**\n\n"
                "You are not currently banned.\n"
                "This button is only for banned users.\n\n"
                "📱 Use the main menu to access features.",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(int(user_id))
            )
        except:
            pass
        return
    
    # Check if user has already appealed recently (spam protection)
    existing_appeal = col_appeals.find_one({
        "user_id": user_id,
        "status": "pending"
    })
    
    if existing_appeal:
        appeal_time = existing_appeal.get("appeal_date")
        appeal_time_str = appeal_time.strftime("%d-%m-%Y %I:%M %p") if appeal_time else "Unknown"
        
        try:
            await message.answer(
                "⏳ **APPEAL ALREADY SUBMITTED**\n\n"
                f"📩 You have a pending appeal\n"
                f"🕐 Submitted: {appeal_time_str}\n"
                f"📊 Status: ⏳ PENDING REVIEW\n\n"
                "✨ Our team will review your appeal soon!\n"
                "⏰ Please wait for response before submitting another.\n\n"
                "💎 *Thank you for your patience!*",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(int(user_id))
            )
        except:
            pass
        return
    
    # Check last appeal time (24 hour limit)
    last_appeal = col_appeals.find_one(
        {"user_id": user_id},
        sort=[("appeal_date", -1)]
    )
    
    if last_appeal:
        last_appeal_time = last_appeal.get("appeal_date")
        if last_appeal_time:
            # Make sure last_appeal_time is timezone-aware
            if isinstance(last_appeal_time, datetime):
                if last_appeal_time.tzinfo is None:
                    last_appeal_time = IST.localize(last_appeal_time)
            else:
                # If it's a string, try to parse it
                try:
                    last_appeal_time = datetime.fromisoformat(str(last_appeal_time))
                    if last_appeal_time.tzinfo is None:
                        last_appeal_time = IST.localize(last_appeal_time)
                except:
                    last_appeal_time = None
            
            if last_appeal_time:
                time_since_appeal = (datetime.now(IST) - last_appeal_time).total_seconds()
                if time_since_appeal < 86400:  # 24 hours
                    hours_remaining = int((86400 - time_since_appeal) / 3600)
                    try:
                        await message.answer(
                            f"⏰ **APPEAL COOLDOWN ACTIVE**\n\n"
                            f"You can submit another appeal in:\n"
                            f"⏳ {hours_remaining} hours\n\n"
                            f"⚠️ Limit: 1 appeal per 24 hours\n\n"
                            f"💡 Please wait before trying again.",
                            parse_mode="Markdown",
                            reply_markup=get_main_keyboard(int(user_id))
                        )
                    except:
                        pass
                    return
    
    # Get user and ban details
    user_doc = col_users.find_one({"user_id": user_id})
    msa_id = ban_record.get("msa_id", "UNKNOWN")
    username = ban_record.get("username", "No Username")
    ban_reason = ban_record.get("reason", "No reason provided")
    banned_by = ban_record.get("banned_by", "System")
    banned_at = ban_record.get("banned_at")
    banned_at_str = banned_at.strftime("%d-%m-%Y %I:%M %p") if banned_at else "Unknown"
    
    # Show ban details and request appeal message
    try:
        await message.answer(
            "🔔 **BAN APPEAL SYSTEM**\n\n"
            "📋 **Your Ban Details:**\n"
            f"• MSA ID: `{msa_id}`\n"
            f"• Username: @{username}\n"
            f"• Banned By: {banned_by}\n"
            f"• Date: {banned_at_str}\n"
            f"• Reason: {ban_reason}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📝 **How to Appeal:**\n"
            "1. Write a clear explanation\n"
            "2. Explain why you should be unbanned\n"
            "3. Promise to follow rules\n\n"
            "💡 **Tips for Success:**\n"
            "   • Be honest and respectful\n"
            "   • Acknowledge your mistake\n"
            "   • Show you understand the rules\n\n"
            "⚠️ **Important:**\n"
            "   • You can appeal once per 24 hours\n"
            "   • Admin will review your appeal\n"
            "   • Response may take up to 24 hours\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "✍️ **Type your appeal message below:**",
            parse_mode="Markdown",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(AppealState.waiting_for_appeal_message)
    except Exception as e:
        logger.error(f"Error starting appeal: {e}")


@dp.message(AppealState.waiting_for_appeal_message)
async def process_appeal_message(message: types.Message, state: FSMContext):
    """Process the appeal message from banned user"""
    user_id = str(message.from_user.id)
    appeal_text = message.text
    
    # Validate appeal message
    if not appeal_text or len(appeal_text.strip()) < 10:
        try:
            await message.answer(
                "❌ **APPEAL TOO SHORT**\n\n"
                "Your appeal must be at least 10 characters.\n"
                "Please provide a clear explanation.\n\n"
                "✍️ Try again:",
                parse_mode="Markdown"
            )
        except:
            pass
        return
    
    if len(appeal_text) > 1000:
        try:
            await message.answer(
                "❌ **APPEAL TOO LONG**\n\n"
                "Maximum length: 1000 characters\n"
                "Please shorten your message.\n\n"
                "✍️ Try again:",
                parse_mode="Markdown"
            )
        except:
            pass
        return
    
    # Get user details
    is_banned, ban_record, _ = is_user_completely_banned(int(user_id))
    if not is_banned:
        await state.clear()
        try:
            await message.answer(
                "✅ You are no longer banned!",
                reply_markup=get_main_keyboard(int(user_id))
            )
        except:
            pass
        return
    
    user_doc = col_users.find_one({"user_id": user_id})
    msa_id = ban_record.get("msa_id", "UNKNOWN")
    username = ban_record.get("username", "No Username")
    user_name = ban_record.get("user_name", "Unknown")
    ban_reason = ban_record.get("reason", "No reason provided")
    banned_by = ban_record.get("banned_by", "System")
    banned_at = ban_record.get("banned_at")
    
    # Store appeal in database
    appeal_doc = {
        "user_id": user_id,
        "msa_id": msa_id,
        "username": username,
        "user_name": user_name,
        "appeal_text": appeal_text,
        "ban_reason": ban_reason,
        "banned_by": banned_by,
        "banned_at": banned_at,
        "appeal_date": datetime.now(IST),
        "status": "pending",
        "reviewed_by": None,
        "review_date": None,
        "response": None
    }
    
    try:
        col_appeals.insert_one(appeal_doc)
    except Exception as e:
        logger.error(f"Error storing appeal: {e}")
        try:
            await message.answer(
                "❌ **ERROR**\n\nFailed to submit appeal. Please try again later.",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(int(user_id))
            )
        except:
            pass
        await state.clear()
        return
    
    # Send appeal to appeal channel with inline buttons
    banned_at_str = banned_at.strftime("%d-%m-%Y %I:%M %p") if banned_at else "Unknown"
    appeal_time_str = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
    
    appeal_message = (
        "🔔 **NEW BAN APPEAL**\n\n"
        "👤 **User Information:**\n"
        f"• MSA ID: `{msa_id}`\n"
        f"• User ID: `{user_id}`\n"
        f"• Username: @{username}\n"
        f"• Name: {user_name}\n\n"
        "🚫 **Ban Information:**\n"
        f"• Reason: {ban_reason}\n"
        f"• Banned By: {banned_by}\n"
        f"• Banned Date: {banned_at_str}\n\n"
        "📝 **Appeal Message:**\n"
        f"{appeal_text}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Appeal Time: {appeal_time_str}\n"
        f"📊 Status: ⏳ PENDING REVIEW"
    )
    
    # Send appeal to channel (no buttons - manage in bot2)
    try:
        channel_msg = await bot.send_message(
            chat_id=APPEAL_CHANNEL_ID,
            text=appeal_message,
            parse_mode="Markdown"
        )
        # Store message ID and original text for later status updates
        col_appeals.update_one(
            {"user_id": user_id, "status": "pending"},
            {
                "$set": {
                    "channel_message_id": channel_msg.message_id,
                    "original_text": appeal_message
                }
            }
        )
    except Exception as e:
        logger.error(f"Error sending appeal to channel: {e}")
    
    # Premium confirmation animation
    await state.clear()
    
    # Show premium processing animation
    processing = await message.answer("⚡ **Processing Appeal...**", parse_mode="Markdown")
    await asyncio.sleep(0.1)
    await processing.edit_text("📝 **[▓░░░░] 20% - Validating Details...**", parse_mode="Markdown")
    await asyncio.sleep(0.1)
    await processing.edit_text("📤 **[▓▓░░░] 40% - Sending to Admin Team...**", parse_mode="Markdown")
    await asyncio.sleep(0.1)
    await processing.edit_text("🔔 **[▓▓▓░░] 60% - Notifying Admins...**", parse_mode="Markdown")
    await asyncio.sleep(0.1)
    await processing.edit_text("✨ **[▓▓▓▓░] 80% - Finalizing Submission...**", parse_mode="Markdown")
    await asyncio.sleep(0.1)
    await processing.edit_text("✅ **[▓▓▓▓▓] 100% - APPEAL SUBMITTED!**", parse_mode="Markdown")
    await asyncio.sleep(0.2)
    await processing.delete()
    
    try:
        await message.answer(
            "✅ **APPEAL SUBMITTED SUCCESSFULLY**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📩 **Your appeal has been sent to the admin team**\n\n"
            f"🆔 Appeal ID: `{msa_id}`\n"
            f"🕐 Submitted: {appeal_time_str}\n"
            f"📊 Status: ⏳ **PENDING REVIEW**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "✨ **What happens next:**\n"
            "   • 👀 Admin will review your case\n"
            "   • 📬 You'll be notified of the decision\n"
            "   • ⏰ Response time: Up to 24 hours\n\n"
            "⚠️ **Important Guidelines:**\n"
            "   • ❌ Do NOT submit multiple appeals\n"
            "   • ⏸️ Wait patiently for admin response\n"
            "   • Follow bot rules if unbanned\n\n"
            "💎 *Thank you for your patience!*",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(int(user_id))
        )
    except Exception as e:
        logger.error(f"Error sending confirmation: {e}")


# ==========================================
# �📚 GUIDE SECTION HANDLERS
# ==========================================

@dp.message(F.text == "📚 Support System Guide")
async def guide_support_system(message: types.Message):
    """Show complete support system guide"""
    user_id = message.from_user.id
    
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    # ULTRA PREMIUM 7-STAGE ANIMATION
    loading = await message.answer("⚡ **ACCESSING SUPPORT GUIDE...**", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🔍 **[▓░░░░░░] 14%** - Scanning Support Protocols...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("📡 **[▓▓░░░░░] 29%** - Loading Template Database...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("📖 **[▓▓▓░░░░] 43%** - Compiling Instructions...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("⚙️ **[▓▓▓▓░░░] 57%** - Processing Guidelines...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("✨ **[▓▓▓▓▓░░] 71%** - Formatting Content...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🎯 **[▓▓▓▓▓▓░] 86%** - Finalizing Guide...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("💎 **[▓▓▓▓▓▓▓] 100%** - ✅ SUPPORT GUIDE READY!", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await loading.delete()
    
    guide_text = (
        "📚 **SUPPORT SYSTEM - COMPLETE GUIDE**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎯 **How To Get Support:**\n\n"
        "**1️⃣ START SUPPORT REQUEST**\n"
        "   • Click \"💬 Customer Support\" button\n"
        "**2️⃣ CHOOSE ISSUE TYPE**\n"
        "   You'll see 6 common issue templates:\n"
        "   • 📄 PDF/Link Not Working\n"
        "   • 🤖 Bot Not Responding\n"
        "   • ⭐ Review Issue\n"
        "   • 🔗 Access/Channel Problem\n"
        "   • ❓ Content Question\n"
        "   • ⚙️ Account/Settings Help\n\n"
        "**3️⃣ VIEW SOLUTION**\n"
        "   • You'll see detailed troubleshooting steps\n"
        "   • Try each step carefully\n"
        "   • Most issues are solved instantly!\n\n"
        "**4️⃣ AFTER SOLUTION**\n"
        "   Four options appear:\n"
        "   ✅ **SOLVED** - Issue fixed? Mark as solved!\n"
        "   ❌ **NOT SOLVED** - Need more help? Escalates to team\n"
        "   🔄 **Check Other** - Browse other solutions\n"
        "   💬 **Contact Support** - Talk to direct support\n\n"
        "**5️⃣ CUSTOM MESSAGE (Optional)**\n"
        "   • If no matching support available!!!\n"
        "   • Click \"✍️ Other Issue\"\n"
        "   • Describe your problem (minimum 10 characters)\n"
        "   • Goes directly to support team\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🛡️ **ANTI-SPAM PROTECTION:**\n\n"
        "• Maximum 3 requests per 24 hours\n"
        "• 1 hour cooldown after resolution\n"
        "• Template browsing is unlimited\n"
        "• Only custom messages count toward limit\n\n"
        "⏱️ **RESPONSE TIME:**\n"
        "• Self-help solutions: Instant\n"
        "• Support team: Usually within 1-72 hours\n\n"
        "💡 **PRO TIP:** Always try template solutions first!\n"
        "   Most issues are solved instantly without waiting!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🔙 Use the keyboard below to explore other guides!"
    )
    
    try:
        await message.answer(guide_text, reply_markup=get_guide_main_keyboard(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error in support guide: {e}")

@dp.message(F.text == "⭐ Review System Guide")
async def guide_review_system(message: types.Message):
    """Show review system guide"""
    user_id = message.from_user.id
    
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    # ULTRA PREMIUM 7-STAGE ANIMATION
    loading = await message.answer("⚡ **ACCESSING REVIEW GUIDE...**", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("⭐ **[▓░░░░░░] 14%** - Loading Review System...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("📝 **[▓▓░░░░░] 29%** - Fetching Guidelines...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🔍 **[▓▓▓░░░░] 43%** - Compiling Best Practices...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("✨ **[▓▓▓▓░░░] 57%** - Processing Examples...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🎨 **[▓▓▓▓▓░░] 71%** - Formatting Layout...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🎯 **[▓▓▓▓▓▓░] 86%** - Finalizing Guide...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("💎 **[▓▓▓▓▓▓▓] 100%** - ✅ REVIEW GUIDE READY!", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await loading.delete()
    
    guide_text = (
        "⭐ **REVIEW SYSTEM - COMPLETE GUIDE**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📝 **How To Leave A Review:**\n\n"
        "**1️⃣ START REVIEW**\n"
        "   • Click \"⭐ Leave Your Review\" button\n"
        "**2️⃣ CHOOSE RATING**\n"
        "   • Select 1-5 stars\n"
        f"⭐         • **DISAPPOINTING** (Not what I expected)\n"
        f"⭐⭐       • **UNSATISFACTORY** (Needs more value)\n"
        f"⭐⭐⭐     • **DECENT** (It's okay, but could be better)\n"
        f"⭐⭐⭐⭐   • **IMPRESSIVE** (High-quality and helpful)\n"
        f"⭐⭐⭐⭐⭐ • **EXCEPTIONAL** (Exceeded all my expectations)\n\n"
        "**3️⃣ WRITE FEEDBACK**\n"
        "   • Minimum 10 characters required\n"
        "   • Maximum 500 characters\n"
        "   • Be honest and detailed!\n\n"
        "**4️⃣ CONFIRM & SEND**\n"
        "   • Review your message\n"
        "   • Click \"✅ CONFIRM & SEND\"\n"
        "   • Or \"❌ CANCEL\" to restart\n\n"
        "**5️⃣ PUBLISHED!**\n"
        "   • Your review is now live and submitted\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🔄 **UPDATE EXISTING REVIEW:**\n\n"
        "   • Already reviewed? No problem!\n"
        "   • Submit a new review again\n"
        "⏱️ **COOLDOWN SYSTEM:**\n\n"
        "   • **After Submission:** setted up cooldown days applied by MSANode AGENT\n"
        "   • **Updates:** Reset cooldown to setted up days\n"
        "   • **Purpose:** Prevent spam reviews\n\n"
        "🎯 **REVIEW QUALITY TIPS:**\n\n"
        "   ✅ **Good Reviews:**\n"
        "   • Specific details about experience\n"
        "   • Honest opinions\n"
        "   • Constructive feedback\n"
        "   • Clear and well-written\n\n"
        "   ❌ **Bad Reviews (Will Be Rejected):**\n"
        "   • Just emoji or symbols\n"
        "   • Too short (under 10 chars)\n"
        "   • Spam or nonsense\n"
        "   • Offensive language\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 **WHY LEAVE A REVIEW?**\n\n"
        "   • Help us improve\n"
        "   • Guide other users\n"
        "   • Show appreciation\n"
        "   • Build community trust in MSANode Family\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
          "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ **WHAT GETS YOU BANNED:**\n\n"
        "   🚫 Excessive button spam\n"
        "   🚫 Command flooding (/start abuse)\n"
        "   🚫 Fake/spam messages repeatedly\n"
        "   🚫 Attempting to bypass cooldowns\n"
        "   🚫 Abusive behavior\n\n"
        "✅ **HOW TO AVOID ISSUES:**\n\n"
        "   ✔️ Use bot normally\n"
        "   ✔️ Wait for cooldowns to expire\n"
        "   ✔️ Don't spam buttons/commands\n"
        "   ✔️ Write meaningful messages\n"
        "   ✔️ Respect limits\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 **GOT BANNED BY MISTAKE?**\n\n"
        "   • Contact support team\n"
        "   • Explain situation calmly\n"
        "   • Admin can unban with /unban command\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🔙 Use the keyboard below to explore other guides!"
    )
    
    try:
        await message.answer(guide_text, reply_markup=get_guide_main_keyboard(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error in review guide: {e}")

@dp.message(F.text == "🛡️ Anti-Spam Protection Info")
async def guide_antispam(message: types.Message):
    """Show anti-spam protection information"""
    user_id = message.from_user.id
    
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    # ULTRA PREMIUM 7-STAGE ANIMATION
    loading = await message.answer("⚡ **ACCESSING SECURITY INFO...**", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🔐 **[▓░░░░░░] 14%** - Initializing Security Scan...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🛡️ **[▓▓░░░░░] 29%** - Loading Protection Systems...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🔍 **[▓▓▓░░░░] 43%** - Analyzing Protections...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("⚙️ **[▓▓▓▓░░░] 57%** - Compiling Security Docs...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("✨ **[▓▓▓▓▓░░] 71%** - Formatting Information...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🎯 **[▓▓▓▓▓▓░] 86%** - Preparing Display...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("💎 **[▓▓▓▓▓▓▓] 100%** - ✅ SECURITY INFO COMPLETE!", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await loading.delete()
    
    guide_text = (
        "🛡️ **ANTI-SPAM PROTECTION - EXPLAINED**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎯 **Why We Have Anti-Spam:**\n\n"
        "   • Ensures fair access for all users\n"
        "   • Prevents bot abuse\n"
        "   • Maintains service quality\n"
        "   • Protects support team from overload\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🔒 **PROTECTION SYSTEMS:**\n\n"
        "**1️⃣ PROGRESSIVE COOLDOWNS**\n"
        "   If you spam buttons:\n"
        "   • 1st offense: 30 seconds\n"
        "   • 2nd offense: 1 minute\n"
        "   • 3rd offense: 3 minutes\n"
        "   • 4th offense: 5 minutes\n"
        "   • 5th+ offense: 10 minutes\n\n"
        "**2️⃣ SUPPORT REQUEST LIMITS**\n"
        "   • Maximum 3 requests per 24 hours\n"
        "   • Only CUSTOM messages count\n"
        "   • Template browsing = unlimited\n"
        "   • 1 hour cooldown after resolution\n\n"
        "**3️⃣ REVIEW COOLDOWN**\n"
        "   • setted days between reviews\n"
        "   • Prevents review spam\n"
        "   • Updates allowed (resets cooldown)\n\n"
        "**4️⃣ RAPID CLICK DETECTION**\n"
        "   • Detects 3+ clicks in 10 seconds\n"
        "   • Triggers warnings\n"
        "   • Can freeze access temporarily\n\n"
        "**5️⃣ FAKE MESSAGE DETECTION**\n"
        "   • Messages under 10 characters rejected\n"
        "   • Spam-like patterns detected\n"
        "   • Multiple violations = ban\n\n"
        "**6️⃣ COMMAND SPAM PROTECTION**\n"
        "   • /start spam = instant ban\n"
        "   • 5 /start commands in 60 seconds = permanent ban\n"
        "   • 3-4 buttons at once = warning\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ **WHAT GETS YOU BANNED:**\n\n"
        "   🚫 Excessive button spam\n"
        "   🚫 Command flooding (/start abuse)\n"
        "   🚫 Fake/spam messages repeatedly\n"
        "   🚫 Attempting to bypass cooldowns\n"
        "   🚫 Abusive behavior\n\n"
        "✅ **HOW TO AVOID ISSUES:**\n\n"
        "   ✔️ Use bot normally\n"
        "   ✔️ Wait for cooldowns to expire\n"
        "   ✔️ Don't spam buttons/commands\n"
        "   ✔️ Write meaningful messages\n"
        "   ✔️ Respect limits\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 **GOT BANNED BY MISTAKE?**\n\n"
        "   • Contact support team\n"
        "   • Explain situation calmly\n"
        "   • Admin can unban with /unban command\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🔙 Use the keyboard below to explore other guides!"
    )
    
    try:
        await message.answer(guide_text, reply_markup=get_guide_main_keyboard(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error in antispam guide: {e}")

@dp.message(F.text == "⚡ Commands & Features")
async def guide_commands(message: types.Message):
    """Show all commands and features"""
    user_id = message.from_user.id
    
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    # ULTRA PREMIUM 7-STAGE ANIMATION
    loading = await message.answer("⚡ **LOADING COMMANDS LIST...**", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("⚙️ **[▓░░░░░░] 14%** - Scanning Command Registry...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("📑 **[▓▓░░░░░] 29%** - Loading Feature Database...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🔍 **[▓▓▓░░░░] 43%** - Compiling Features...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("📖 **[▓▓▓▓░░░] 57%** - Processing Documentation...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("✨ **[▓▓▓▓▓░░] 71%** - Formatting Command List...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🎯 **[▓▓▓▓▓▓░] 86%** - Finalizing Guide...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("💎 **[▓▓▓▓▓▓▓] 100%** - ✅ COMMANDS LIST READY!", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await loading.delete()
    
    guide_text = (
      f"🚀 **BUTTONS & COMMAND SESSION**\n"
        f"• 🛠 /start - Initialize terminal and access the main dashboard.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 **SYSTEM MODULES**\n\n"
        f"🏠 **DASHBOARD**\n"
        f"• Start bot & show main menu\n\n"
        f"⭐ **REVIEW**\n"
        f"• Open this guide system\n"
        f"• Share your feedback & rate 1-5 stars\n"
        f"• Update existing reviews\n\n"
        f"🛠 **CUSTOMER SUPPORT**\n"
        f"• Request customer support\n"
        f"• Get help with issues & browse 6 solution templates\n"
        f"• Direct contact with support team\n\n"
        f"❓ **FAQ / HELP**\n"
        f"• Most Fequently Asked Questions\n\n"
        f"📚 **GUIDE / HOW TO USE**\n"
        f"• Check your account status\n"
        f"• Complete documentation & step-by-step tutorials\n"
        f"• Detailed feature explanations\n\n"
        f"📜 **RULES & REGULATIONS**\n"
        f"• Rules And Regulations Of MSANode AGENT\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        "✨ **SPECIAL FEATURES:**\n\n"
        "   🎯 Self-Help Solutions - 6 templates\n"
        "   🛡️ Multi-Layer Spam Protection\n"
        "   ⏱️ Smart Cooldown System\n"
        "   📊 Real-Time Status Tracking\n"
        "   🔄 Review Update System\n"
        "   💬 Direct Support Channel\n"
        "   🎨 Premium UI/UX Design\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 **PRO TIPS:**\n\n"
        "   • Save your MSA ID for reference\n"
        "   • Use templates before custom support\n"
        "   • Don't spam to avoid cooldowns\n"
        "   • Leave honest reviews to help us\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🔙 Use the keyboard below to explore other guides!"
    )
    
    try:
        await message.answer(guide_text, reply_markup=get_guide_main_keyboard(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error in commands guide: {e}")

@dp.message(F.text == "💎 Premium Features")
async def guide_premium(message: types.Message):
    """Show premium features"""
    user_id = message.from_user.id
    
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    # ULTRA PREMIUM 7-STAGE ANIMATION
    loading = await message.answer("⚡ **UNLOCKING PREMIUM FEATURES...**", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🔐 **[▓░░░░░░] 14%** - Authenticating Premium Access...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("💎 **[▓▓░░░░░] 29%** - Loading Premium Database...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("✨ **[▓▓▓░░░░] 43%** - Analyzing Premium...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🎨 **[▓▓▓▓░░░] 57%** - Compiling Feature List...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🚀 **[▓▓▓▓▓░░] 71%** - Processing Advanced Features...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🎯 **[▓▓▓▓▓▓░] 86%** - Unlocked!", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("💎 **[▓▓▓▓▓▓▓] 100%** - ✅ PREMIUM GUIDE READY!", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await loading.delete()
    
    guide_text = (
        f"💎 **PREMIUM CAPABILITIES**\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✨ **What defines the MSANode experience:**\n\n"
        f"**1️⃣ INTERACTIVE VISUALS**\n"
        f"   • Real-time progress bars and sync-loading.\n"
        f"   • High-fidelity transitions between modules.\n"
        f"   • A clean, cinematic UI optimized for future.\n\n"
        f"**2️⃣ INSTANT RESOLUTION SYSTEM**\n"
        f"   • 6 pre-configured intelligence templates.\n"
        f"   • Step-by-step guidance for every blueprint type.\n"
        f"   • Solve anomalies instantly without waiting for command.\n\n"
        f"**3️⃣ IRON DOME SECURITY**\n"
        f"   • Personalized protection for your terminal access.\n"
        f"   • Intelligent safeguard against accidental pings.\n"
        f"   • Multi-layer security to keep the vault stable.\n\n"
        f"**4️⃣ PRIORITY COMMUNICATION**\n"
        f"   • Direct synchronization with MSANode Intelligence.\n"
        f"   • Your feedback is analyzed and logged immediately.\n"
        f"   • Personalized response paths based on your input.\n\n"
        f"**5️⃣ LIVE STATUS UPDATES**\n"
        f"   • Instant alerts when a task is completed.\n"
        f"   • Real-time tracking of your clearance level.\n"
        f"   • Transparent reporting on your Syndicate activity.\n\n"
        f"**6️⃣ REPUTATION MANAGEMENT**\n"
        f"   • Ability to refine and update your audits anytime.\n"
        f"   • Quality-controlled feedback to ensure high standards.\n"
        f"   • Direct influence on upcoming blueprint releases.\n\n"
        f"**7️⃣ MSANODE PASSPORT (ID)**\n"
        f"   • A unique, permanent signature for your account.\n"
        f"   • Professional tracking for elite support handling.\n"
        f"   • Your key to the entire MSANode ecosystem.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 **ELITE BENEFITS:**\n\n"
        f"   ✅ Sophisticated and smooth user journey\n"
        f"   ✅ Zero-delay problem solving\n"
        f"   ✅ Secure and stable operational environment\n"
        f"   ✅ Direct influence on Syndicate growth\n"
        f"   ✅ Modern, high-authority terminal design\n"
        f"   ✅ 24/7 Access to high-tier intelligence\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔙 *Use the keyboard below to navigate other sectors.*"
    )
    try:
        await message.answer(guide_text, reply_markup=get_guide_main_keyboard(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error in premium guide: {e}")

@dp.message(F.text == "❓ FAQ - Common Questions")
async def guide_faq(message: types.Message):
    """Show frequently asked questions"""
    user_id = message.from_user.id
    
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    # ULTRA PREMIUM 7-STAGE ANIMATION
    loading = await message.answer("⚡ **LOADING FAQ DATABASE...**", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🔍 **[▓░░░░░░] 14%** - Scanning Question Database...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("📚 **[▓▓░░░░░] 29%** - Loading Common Questions...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("📖 **[▓▓▓░░░░] 43%** - Compiling Questions...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("⚙️ **[▓▓▓▓░░░] 57%** - Processing Answers...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("✨ **[▓▓▓▓▓░░] 71%** - Formatting FAQ...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("🎯 **[▓▓▓▓▓▓░] 86%** - Finalizing Display...", parse_mode="Markdown")
    await asyncio.sleep(0.07)
    await loading.edit_text("💎 **[▓▓▓▓▓▓▓] 100%** - ✅ FAQ READY!", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await loading.delete()
    
    guide_text = (
        "❓ **FREQUENTLY ASKED QUESTIONS**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Q1: What is MSA ID?**\n"
        "A: Your unique identifier in the system.\n"
        "   Format: MSA1, MSA2, MSA3, etc.\n"
        "   Use it for support tickets.\n\n"
        "**Q2: How do I get support?**\n"
        "A: Click \"💬 Customer Support\" → Choose issue template\n"
        "   → Try solution → If not solved, contact team.\n\n"
        "**Q3: Why can't I submit support request?**\n"
        "A: Common reasons:\n"
        "   • Reached 3 requests/24 hours limit\n"
        "   • In cooldown after resolution (1 hour)\n"
        "   • Message too short (min 10 chars)\n"
        "   • Pending request already exists\n\n"
        "**Q4: How do I update my review?**\n"
        "A: Just submit a new review!\n"
        "   Your old review will be updated automatically.\n"
        "   Note: Cooldown resets to 7 days or setted days.\n\n"
        "**Q5: What's the difference between templates and custom?**\n"
        "A: Templates: Self-help solutions, instant, unlimited\n"
        "   Custom: Goes to support team, counts toward limits\n\n"
        "**Q6: Why am I in cooldown?**\n"
        "A: You triggered anti-spam protection:\n"
        "   • Button/command spam\n"
        "   • Rapid clicking\n"
        "   • Multiple requests\n"
        "   Wait for cooldown to expire.\n\n"
        "**Q7: How to check my status?**\n"
        "A: Click \"DASHBOARD\" then Click \"📊 STATUS\" \n"
        "   Shows: MSA ID, membership, pending requests, etc.\n\n"
        "**Q8: Can I cancel a support request?**\n"
        "A: While typing: Click \"❌ CANCEL\" button or DASHBOARD press \"❌ CANCEL\" button \n"
        "   After submission: No, wait for admin resolution\n\n"
        "**Q9: How long until support responds?**\n"
        "A: Usually within 1-24 hours\n"
        "   Self-help templates: Instant!\n\n"
        "**Q10: What happens if I spam?**\n"
        "A: Progressive cooldowns:\n"
        "   1st: 30 sec → 2nd: 1 min → 3rd: 3 min\n"
        "   4th: 5 min → 5th+: 10 min\n"
        "   Severe spam: Permanent ban\n\n"
        "**Q11: Why was I banned?**\n"
        "A: Reasons:\n"
        "   • Excessive spam\n"
        "   • Command flooding (/start)\n"
        "   • Fake/abusive messages\n"
        "   Contact admin to appeal.\n\n"
        "**Q12: Can I use bot without joining channel?**\n"
        "A: No, MSAnode VAULT membership is required.\n"
        "   You'll be prompted to join automatically.\n\n"
        "**Q13: Template solutions don't help. What now?**\n"
        "A: After trying template:\n"
        "   Click \"❌ NOT SOLVED\" or \"💬 Contact Support\"\n"
        "   This escalates to human support team.\n\n"
        "**Q14: How often can I leave reviews?**\n"
        "A: Every 7 days or setted days \n"
        "   Or update existing review (resets cooldown)\n\n"
        "**Q15: What's the ❌ CANCEL button for?**\n"
        "A: Stops current action immediately\n"
        "   Works in support, review, any text input state\n"
        "   Returns you to main menu\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 **Still have questions?**\n"
        "   Use \"💬 Customer Support\" to ask!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🔙 Use the keyboard below to explore other guides!"
    )
    
    try:
        await message.answer(guide_text, reply_markup=get_guide_main_keyboard(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error in FAQ guide: {e}")

@dp.message(F.text == "🏠 Back to Main Menu")
async def guide_back_to_main(message: types.Message):
    """Return to main menu from guide"""
    user_id = message.from_user.id
    
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    try:
        await message.answer(
            "🏠 **Returned to Main Menu**\n\n"
            "💡 Use the buttons below to navigate:",
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error returning to main: {e}")

@dp.message(SupportState.selecting_issue)
async def handle_issue_selection(message: types.Message, state: FSMContext):
    """Handle user's issue selection from template - Show solutions first"""
    user_id = str(message.from_user.id)
    logger.info(f"Handler called for user {user_id}, message: {message.text}")
    
    # PRIORITY 1: Handle cancel FIRST before anything else
    if message.text == "❌ CANCEL":
        logger.info(f"Cancel button clicked by user {user_id} in support selection")
        try:
            await state.clear()
            await message.answer(
                "Cancelled\n\n"
                "Support request cancelled.\n"
                "Returning to main menu...",
                reply_markup=get_main_keyboard()
            )
            logger.info(f"Successfully cancelled support for user {user_id}")
        except Exception as e:
            logger.error(f"Error cancelling support selection: {e}")
            try:
                await message.answer(
                    "Cancelled",
                    reply_markup=get_main_keyboard()
                )
            except:
                pass
        return
    
    # Ban check
    if col_banned.find_one({"user_id": user_id}):
        await state.clear()
        return
    
    # Check if user wants to type custom message
    if message.text and message.text == "✍️ Other Issue (Type Custom Message)":
        try:
            await message.answer(
                "Custom Support Message\n\n"
                "Please describe your issue:\n\n"
                "Type your message below\n"
                "Min: 10 characters\n"
                "Max: 500 characters\n\n"
                "Important Guidelines:\n"
                "   - Be clear and concise\n"
                "   - No spam or repetitive text\n"
                "   - Professional language only\n\n"
                "This will be sent to our support team!",
                reply_markup=get_cancel_keyboard()
            )
            await state.set_state(SupportState.waiting_for_message)
        except Exception as e:
            logger.error(f"Error showing custom message prompt: {e}")
        return
    
    # Anti-spam check using global cooldown system
    current_time = time.time()
    is_blocked, remaining, spam_msg = check_spam_and_cooldown(user_id, current_time)
    if is_blocked:
        try:
            await message.answer(spam_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    # Update last action
    user_last_action[user_id] = current_time
    
    # Check for rapid clicking (more than 3 in 10 seconds)
    if user_id not in user_template_views:
        user_template_views[user_id] = []
    
    # Clean old views (older than 5 minutes)
    user_template_views[user_id] = [t for t in user_template_views[user_id] if current_time - t < 300]
    
    recent_views = [t for t in user_template_views[user_id] if current_time - t < 10]
    if len(recent_views) >= 3:
        user_template_spam[user_id] = user_template_spam.get(user_id, 0) + 1
        
        if user_template_spam[user_id] >= TEMPLATE_SPAM_FREEZE:
            try:
                await message.answer(
                    "⚠️ **ANTI-SPAM PROTECTION**\n\n"
                    "🚫 You're clicking too fast!\n"
                    "⏸️ **Account temporarily frozen**\n\n"
                    "⏰ Please wait 30 seconds before trying again.\n\n"
                    "⚠️ Continued spam may result in a ban.",
                    parse_mode="Markdown",
                    reply_markup=get_main_keyboard()
                )
            except:
                pass
            await state.clear()
            await asyncio.sleep(30)
            user_template_spam[user_id] = 0
            return
        
        try:
            await message.answer(
                f"⚠️ **Slow Down!**\n\n"
                f"🛑 Warning {user_template_spam[user_id]}/{TEMPLATE_SPAM_FREEZE}\n"
                "Please wait a moment between selections.",
                parse_mode="Markdown"
            )
        except:
            pass
        await asyncio.sleep(3)
        return
    
    # Check total views limit
    if len(user_template_views[user_id]) >= TEMPLATE_VIEW_LIMIT:
        try:
            await message.answer(
                "⚠️ **Too Many Views**\n\n"
                "You've checked many solutions already.\n"
                "💬 Please use **'Other Issue'** to contact our support team directly.",
                parse_mode="Markdown",
                reply_markup=get_support_issues_keyboard()
            )
        except:
            pass
        return
    
    # Track this view
    user_template_views[user_id].append(current_time)
    
    # Solution database for each issue type
    solutions = {
        "📄 PDF/Link Not Working": (
            "📄 **PDF/Link Troubleshooting**\n\n"
            "🔍 **Common Solutions:**\n\n"
            "1️⃣ **Check Your Internet**\n"
            "   • Refresh your connection\n"
            "   • Try switching WiFi/Mobile data\n\n"
            "2️⃣ **Clear Browser Cache**\n"
            "   • Close and reopen browser\n"
            "   • Try incognito/private mode\n\n"
            "3️⃣ **Try Different Browser**\n"
            "   • Chrome, Firefox, or Safari\n"
            "   • Update to latest version\n\n"
            "4️⃣ **Check Link Again**\n"
            "   • Copy the full link\n"
            "   • Make sure no characters are cut off\n\n"
            "5️⃣ **Download Instead of View**\n"
            "   • Right-click → Save As\n"
            "   • Open with PDF reader app\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💡 **Still not working?** Try restarting your device!"
        ),
        "🤖 Bot Not Responding": (
            "🤖 **Bot Response Issues?**\n\n"
            "If the bot isn't responding, try these quick fixes:\n\n"
            "**1️⃣ Restart the Bot**\n"
            "   • Use the buttons below to navigate\n"
            "   • Tap 📊 DASHBOARD to refresh\n\n"
            "**2️⃣ Check Bot Status**\n"
            "   • Bot might be under maintenance\n"
            "   • Wait 2-3 minutes and retry\n\n"
            "**3️⃣ Clear & Restart**\n"
            "   • Delete this conversation\n"
            "   • Start fresh using your original link\n\n"
            "**4️⃣ Restart Telegram**\n"
            "   • Close app completely\n"
            "   • Clear from recent apps\n"
            "   • Reopen and try again\n\n"
            "**5️⃣ Verify Membership**\n"
            "   • Ensure you're still in the Telegram Vault\n"
            "   • Re-join if you left the channel\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💡 **Pro Tip:** Keep Telegram updated to the latest version for best performance!\n\n"
        ),
        "⭐ Review Issue": (
            "⭐ **Review System Help**\n\n"
            "🔍 **Solutions:**\n\n"
            "1️⃣ **Can't Submit Review?**\n"
            "   • Check if you've reviewed before\n"
            "   • Each user can only review once\n"
            "   • Updates are allowed anytime\n\n"
            "2️⃣ **Rating Not Saving?**\n"
            "   • Select stars using buttons only\n"
            "   • Don't type the rating\n"
            "   • Wait for confirmation message\n\n"
            "3️⃣ **Text Too Long/Short?**\n"
            "   • Minimum: 10 characters\n"
            "   • Maximum: 500 characters\n"
            "   • Keep it clear and concise\n\n"
            "4️⃣ **Want to Update Review?**\n"
            "   • Just submit a new one\n"
            "   • Old review will be replaced\n"
            "   • All ratings are welcome!\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💡 **Tip:** Be honest and constructive!"
        ),
        "🔗 Access/Channel Problem": (
            "🔗 **Channel Access Solutions**\n\n"
            "🔍 **Try These Steps:**\n\n"
            "1️⃣ **Join/Rejoin Channel**\n"
            "   • Click the channel link again\n"
            "   • Make sure you clicked JOIN\n"
            "   • Check if you're still a member\n\n"
            "2️⃣ **Complete Verification**\n"
            "   • Follow all verification steps\n"
            "   • Click all required buttons\n"
            "   • Wait for confirmation\n\n"
            "3️⃣ **Check Requirements**\n"
            "   • Some content needs prerequisites\n"
            "   • Read the access requirements\n"
            "   • Complete all tasks\n\n"
            "4️⃣ **Link Expired?**\n"
            "   • Request a new link\n"
            "   • Contact channel admin\n"
            "   • Check announcements\n\n"
            "5️⃣ **Permission Issues**\n"
            "   • You might be restricted\n"
            "   • Check with channel owner\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💡 **Note:** Some channels have waiting periods!"
        ),
        "❓ Content Question": (
            "❓ **Content Help**\n\n"
            "🔍 **Common Questions:**\n\n"
            "1️⃣ **Finding Specific Content**\n"
            "   • Use channel search feature\n"
            "   • Check pinned messages\n"
            "   • Browse by date/category\n\n"
            "2️⃣ **Content Quality**\n"
            "   • All materials are verified\n"
            "   • Updated regularly\n"
            "   • Report broken content\n\n"
            "3️⃣ **Download Issues**\n"
            "   • Check your storage space\n"
            "   • Try smaller file sizes\n"
            "   • Use download manager\n\n"
            "4️⃣ **Understanding Materials**\n"
            "   • Read instructions carefully\n"
            "   • Check for README files\n"
            "   • Look for video guides\n\n"
            "5️⃣ **Missing Content**\n"
            "   • Content may be time-limited\n"
            "   • Check announcements\n"
            "   • Ask support \n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💡 **Tip:** Save important content immediately!"
        ),
        "⚙️ Account/Settings Help": (
            "⚙️ **Account & Settings**\n\n"
            "🔍 **Solutions:**\n\n"
            "1️⃣ **Profile Issues**\n"
            "   • Check Telegram settings\n"
            "   • Update profile picture\n"
            "   • Verify username\n\n"
            "2️⃣ **Notification Problems**\n"
            "   • Enable Telegram notifications\n"
            "   • Check phone settings\n"
            "   • Unmute the bot\n\n"
            "3️⃣ **Language Settings**\n"
            "   • Use /settings command\n"
            "   • Change Telegram language\n"
            "   • Restart for changes\n\n"
            "4️⃣ **Privacy Settings**\n"
            "   • Check Telegram privacy\n"
            "   • Allow messages from bots\n"
            "   • Enable profile visibility\n\n"
            "5️⃣ **Reset Account**\n"
            "   • Type /start to reset\n"
            "   • Clear bot data\n"
            "   • Re-verify if needed\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💡 **Note:** Never share your account details!"
        )
    }
    
    # Check if it's a template issue
    if message.text in solutions:
        try:
            # Show solution
            await message.answer(
                solutions[message.text],
                parse_mode="Markdown"
            )
            
            await asyncio.sleep(0.5)
            
            # Ask for feedback
            await message.answer(
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "📊 **Was this helpful?**\n\n"
                "Please select one option below:",
                parse_mode="Markdown",
                reply_markup=get_solution_feedback_keyboard()
            )
            
            # Stay in selecting_issue state to handle feedback
        except Exception as e:
            logger.error(f"Error showing solution: {e}")
        return
    
    # Handle feedback responses
    if message.text == "✅ SOLVED - Thank You!":
        try:
            # Premium animation sequence
            anim_msg = await message.answer("🔄 **Processing...**", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("✨ **Analyzing Response...**\n▓░░░░░░░░░ 10%", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("📊 **Recording Success...**\n▓▓▓▓▓░░░░░ 50%", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("🎉 **Finalizing...**\n▓▓▓▓▓▓▓▓▓░ 90%", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("✅ **Complete!**\n▓▓▓▓▓▓▓▓▓▓ 100%", parse_mode="Markdown")
            await asyncio.sleep(0.3)
            
            try:
                await anim_msg.delete()
            except:
                pass
            
            # Get user info
            user_name = message.from_user.first_name or "User"
            username = f"@{message.from_user.username}" if message.from_user.username else "No username"
            user_doc = col_users.find_one({"user_id": user_id})
            msa_id = user_doc.get('msa_id', 'N/A') if user_doc else 'N/A'
            timestamp = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
            
            # Send success report to channel
            success_report = (
                "✅ **SELF-HELP SUCCESS**\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 **User:** {user_name}\n"
                f"🔖 **MSA ID:** `{msa_id}`\n"
                f"🆔 **TELEGRAM ID:** `{user_id}`\n"
                f"📱 **Username:** {username}\n"
                f"🕐 **Time:** {timestamp}\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "💬 **Status:** User resolved issue using self-help guide\n"
                "📊 **Result:** No support ticket needed\n"
                "✅ **Outcome:** Issue resolved independently\n"
                "━━━━━━━━━━━━━━━━━━━━━"
            )
            
            if SUPPORT_CHANNEL_ID:
                await bot.send_message(
                    SUPPORT_CHANNEL_ID,
                    success_report,
                    parse_mode="Markdown"
                )
            
            # Premium success message to user
            await message.answer(
                "🎉🎊 **ISSUE RESOLVED!** 🎊🎉\n\n"
                "✅ **Excellent!** We're thrilled we could help!\n\n"
                "💪 **You solved it using our self-help guide!**\n"
                "✨ This is the fastest way to get help.\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "💎 **Thank you for:**\n"
                "   • Using our resources\n"
                "   • Saving support time\n"
                "   • Being self-sufficient\n\n"
                "🚀 **Need help again?** Just tap Support anytime!\n\n"
                "🔙 Returning to main menu...",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard()
            )
            
            await state.clear()
            logger.info(f"✅ User {user_id} (MSA: {msa_id}) resolved issue via self-help")
            
        except Exception as e:
            logger.error(f"Error handling solved feedback: {e}")
            await state.clear()
            try:
                await message.answer(
                    "✅ **Thank you!**\n\n"
                    "We're glad we could help!\n\n"
                    "🔙 Returning to main menu...",
                    parse_mode="Markdown",
                    reply_markup=get_main_keyboard()
                )
            except:
                pass
        return
    
    elif message.text == "❌ NOT SOLVED - Need More Help":
        try:
            # Animation for escalation
            anim_msg = await message.answer("🔄 **Processing...**", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("📄 **Preparing Support Form...**\n▓░░░░░░░░░ 10%", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("👥 **Connecting to Team...**\n▓▓▓▓▓░░░░░ 50%", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("✨ **Ready!**\n▓▓▓▓▓▓▓▓▓▓ 100%", parse_mode="Markdown")
            await asyncio.sleep(0.3)
            
            try:
                await anim_msg.delete()
            except:
                pass
            
            await message.answer(
                "💬 **Personal Support Requested**\n\n"
                "👨‍💼 Our team is ready to help you!\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "📝 **Please describe your specific issue:**\n\n"
                "✍️ Type your message below\n"
                "📊 Min: 10 characters\n"
                "📊 Max: 500 characters\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "💎 **What happens next:**\n"
                "   1️⃣ Your message goes to our team\n"
                "   2️⃣ We analyze your specific issue\n"
                "   3️⃣ You get a personalized response\n\n"
                "⏰ *Response time: Usually within 24 hours*",
                parse_mode="Markdown",
                reply_markup=get_cancel_keyboard()
            )
            await state.set_state(SupportState.waiting_for_message)
            
        except Exception as e:
            logger.error(f"Error showing custom message prompt: {e}")
        return
    
    elif message.text == "🔄 Check Other Solutions":
        try:
            # Animation for browsing
            anim_msg = await message.answer("🔄 **Loading...**", parse_mode="Markdown")
            await asyncio.sleep(0.12)
            await anim_msg.edit_text("📚 **Loading Solutions Library...**", parse_mode="Markdown")
            await asyncio.sleep(0.12)
            await anim_msg.edit_text("✨ **Preparing Options...**", parse_mode="Markdown")
            await asyncio.sleep(0.12)
            await anim_msg.edit_text("🎯 **Ready!**", parse_mode="Markdown")
            await asyncio.sleep(0.2)
            
            try:
                await anim_msg.delete()
            except:
                pass
            
            await message.answer(
                "📚 **Browse Self-Help Solutions**\n\n"
                "🎯 Select another issue to view solutions:\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "💡 **Did you know?**\n"
                "   • 80% of issues solved instantly\n"
                "   • No waiting for support team\n"
                "   • Available 24/7\n\n"
                "💬 **Need personalized help?**\n"
                "   Use '✍️ Other Issue' option below",
                parse_mode="Markdown",
                reply_markup=get_support_issues_keyboard()
            )
            
        except Exception as e:
            logger.error(f"Error showing templates again: {e}")
        return
    
    elif message.text == "💬 Contact Support Team":
        try:
            # Premium connection animation
            anim_msg = await message.answer("🔄 **Connecting...**", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("📞 **Calling Support Team...**\n▓░░░░░░░░░ 10%", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("👥 **Team Notified...**\n▓▓▓▓▓░░░░░ 50%", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("📝 **Preparing Form...**\n▓▓▓▓▓▓▓▓░░ 80%", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await anim_msg.edit_text("✅ **Connected!**\n▓▓▓▓▓▓▓▓▓▓ 100%", parse_mode="Markdown")
            await asyncio.sleep(0.3)
            
            try:
                await anim_msg.delete()
            except:
                pass
            
            await message.answer(
                "💬 **Direct Support Line**\n\n"
                "👨‍💻 You're now connected to our support team!\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "📝 **Please describe your issue:**\n\n"
                "✍️ Type your message below\n"
                "📊 Min: 10 characters\n"
                "📊 Max: 500 characters\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "⚠️ **Guidelines:**\n"
                "   • Be clear and specific\n"
                "   • Mention what you've tried\n"
                "   • Professional language only\n"
                "   • Include relevant details\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "💎 **What to expect:**\n"
                "   • Your message goes to experts\n"
                "   • Personalized solution\n"
                "   • Response within 24 hours\n\n"
                "✨ *We're here to help!*",
                parse_mode="Markdown",
                reply_markup=get_cancel_keyboard()
            )
            await state.set_state(SupportState.waiting_for_message)
            
        except Exception as e:
            logger.error(f"Error showing contact support: {e}")
        return
    
    else:
        # Invalid selection
        try:
            await message.answer(
                "⚠️ **Invalid Selection**\n\n"
                "Please use the buttons to select an option.",
                parse_mode="Markdown",
                reply_markup=get_support_issues_keyboard()
            )
        except:
            pass
        return

async def process_support_submission(message: types.Message, state: FSMContext, support_text: str, display_text: str = None):
    """Process and submit support request (shared logic for template and custom messages)"""
    user_id = str(message.from_user.id)
    
    if display_text is None:
        display_text = support_text
    
    # Send to support channel with animation
    try:
        progress_msg = await message.answer("📝 **Validating Request...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await progress_msg.edit_text("🔍 **Analyzing Message...**\n▓░░░░░░░░░ 10%", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await progress_msg.edit_text("⚡ **Processing Content...**\n▓▓▓░░░░░░░ 30%", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await progress_msg.edit_text("🎯 **Preparing Report...**\n▓▓▓▓▓░░░░░ 50%", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await progress_msg.edit_text("📡 **Connecting to Support...**\n▓▓▓▓▓▓▓░░░ 70%", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await progress_msg.edit_text("✉️ **Sending Message...**\n▓▓▓▓▓▓▓▓▓░ 90%", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        
        # Prepare support report
        user_data = col_users.find_one({"user_id": user_id})
        user_name = user_data.get('full_name', 'Unknown') if user_data else 'Unknown'
        msa_id = user_data.get('msa_id', 'UNKNOWN') if user_data else 'UNKNOWN'
        username = f"@{message.from_user.username}" if message.from_user.username else "No Username"
        timestamp = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
        
        # Check if user has ANY existing support record (pending OR cleared)
        existing_request = user_support_pending.get(user_id)
        
        # Block only if PENDING (not cleared)
        if existing_request and existing_request.get('status') == 'pending':
            await state.clear()
            try:
                await progress_msg.delete()
            except:
                pass
            try:
                await message.answer(
                    "⚠️ **Duplicate Blocked**\n\n"
                    "🚫 You already have a pending request\n\n"
                    "💡 Please wait for resolution.",
                    parse_mode="Markdown",
                    reply_markup=get_main_keyboard()
                )
            except:
                pass
            return
        
        support_report = (
            "🆘 **SUPPORT REQUEST**\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 **User:** {user_name}\n"
            f"🏷️ **MSA ID:** `{msa_id}`\n"
            f"🆔 **TELEGRAM ID:** `{user_id}`\n"
            f"📱 **Username:** {username}\n"
            f"🕐 **Submitted:** {timestamp}\n"
            f"📊 **Status:** ⏳ PENDING\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💬 **MESSAGE:**\n\n"
            f"{support_text}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 **Contact:** tg://user?id={message.from_user.id}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💡 *Reply /resolve {msa_id} or /resolve {user_id} to mark as cleared*"
        )
        
        # Send to support channel
        if not SUPPORT_CHANNEL_ID:
            await progress_msg.edit_text(
                "⚠️ **Configuration Error**\n\n"
                "Support channel not configured.\n"
                "Please contact administrator.",
                parse_mode="Markdown"
            )
            await state.clear()
            return
        
        # Edit existing message if user has record, otherwise send new
        if existing_request and existing_request.get('channel_msg_id'):
            try:
                # Edit existing message (same message, new content)
                await bot.edit_message_text(
                    text=support_report,
                    chat_id=SUPPORT_CHANNEL_ID,
                    message_id=existing_request['channel_msg_id'],
                    parse_mode="Markdown"
                )
                channel_msg_id = existing_request['channel_msg_id']
                logger.info(f"✅ Edited existing support message for user {user_id}")
            except Exception as e:
                # If edit fails, send new message
                logger.error(f"Failed to edit existing message: {e}")
                channel_msg = await bot.send_message(
                    SUPPORT_CHANNEL_ID,
                    support_report,
                    parse_mode="Markdown"
                )
                channel_msg_id = channel_msg.message_id
        else:
            # Send new message for first-time user
            channel_msg = await bot.send_message(
                SUPPORT_CHANNEL_ID,
                support_report,
                parse_mode="Markdown"
            )
            channel_msg_id = channel_msg.message_id
            logger.info(f"📨 Sent new support message for user {user_id}")
        
        # Mark as pending in memory AND database
        user_support_pending[user_id] = {
            'message': support_text,
            'timestamp': time.time(),
            'channel_msg_id': channel_msg_id,
            'status': 'pending',
            'user_name': user_name,
            'username': username
        }
        
        # Save to database for bot2 access
        col_users.update_one(
            {"user_id": user_id},
            {"$set": {
                "support_status": "open",
                "support_issue": support_text,
                "support_timestamp": datetime.now(IST),
                "support_channel_msg_id": channel_msg_id
            }}
        )
        
        # Track in history for daily limit
        if user_id not in user_support_history:
            user_support_history[user_id] = []
        user_support_history[user_id].append(time.time())
        
        # Support ticket notification sent to channel only (not to bot2 admins)
        
        # Confirm to user with premium animation and status bar
        await progress_msg.edit_text("✅ **Message Delivered!**\n▓▓▓▓▓▓▓▓▓▓ 100%", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await progress_msg.edit_text("🎊 **Success!**\n✨ *Generating Report...*", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await progress_msg.edit_text("💫 **Finalizing...**\n🎯 *Creating Status Dashboard...*", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        
        # Delete progress message and send final confirmation with status bar
        try:
            await progress_msg.delete()
        except:
            pass
        
        # Create status bar
        status_bar = "█████░░░░░░░░░░░"  # 33% - Submitted
        
        await message.answer(
            "✅ **Message Sent Successfully!**\n\n"
            "📩 Your support request has been received\n"
            "👥 Our team will review it shortly\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📊 **REQUEST STATUS**\n"
            f"Progress: [{status_bar}] 33%\n\n"
            "⏰ **Status:** ⏳ Pending Review\n"
            f"🕐 **Submitted:** {timestamp}\n"
            f"🆔 **Request ID:** #{channel_msg_id}\n"
            f"📝 **Message Length:** {len(support_text)} chars\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "💎 *Thank you for contacting us!*\n"
            "✨ We'll get back to you as soon as possible.\n\n"
            "⚠️ **Note:** You can submit a new request only after this one is resolved.",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard()
        )
        
        await state.clear()
        logger.info(f"Support request from user {user_id} sent to channel")
        
    except Exception as e:
        logger.error(f"Error sending support message: {e}")
        try:
            await message.answer(
                "⚠️ **Error**\n\n"
                "Failed to send support request.\n"
                "Please try again later.",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard()
            )
        except:
            pass
        await state.clear()

@dp.message(SupportState.waiting_for_message)
async def receive_support_message(message: types.Message, state: FSMContext):
    """Process user's custom support message"""
    user_id = str(message.from_user.id)
    
    # PRIORITY 1: Handle cancel FIRST before any other checks
    if message.text and message.text == "❌ CANCEL":
        await state.clear()
        try:
            await message.answer(
                "Cancelled\n\n"
                "Support request cancelled.\n"
                "Returning to main menu...",
                reply_markup=get_main_keyboard()
            )
        except Exception as e:
            logger.error(f"Error cancelling support: {e}")
        return
    
    # Ban check
    if col_banned.find_one({"user_id": user_id}):
        await state.clear()
        return
    
    # Check eligibility (cooldown and daily limits)
    can_submit, block_message = check_support_eligibility(user_id)
    if not can_submit:
        await state.clear()
        try:
            await message.answer(
                block_message,
                parse_mode="Markdown",
                reply_markup=get_main_keyboard()
            )
        except:
            pass
        return
    
    # CRITICAL: Block if user already has pending request (check database too)
    user_doc = col_users.find_one({"user_id": user_id})
    db_support_status = user_doc.get("support_status") if user_doc else None
    
    # If database shows resolved, clear the in-memory pending status
    if db_support_status in ["resolved", "responded"] and user_id in user_support_pending:
        user_support_pending[user_id]['status'] = 'cleared'
    
    if user_id in user_support_pending and user_support_pending[user_id].get('status') == 'pending':
        # Double check database
        if db_support_status not in ["resolved", "responded"]:
            # Truly pending
            await state.clear()
            pending_time = datetime.fromtimestamp(user_support_pending[user_id]['timestamp'], IST).strftime("%d-%m-%Y %I:%M %p")
            try:
                await message.answer(
                    "⚠️ **Duplicate Request Blocked**\n\n"
                    "🚫 You already have a pending support request\n"
                    f"🕐 Submitted: {pending_time}\n\n"
                    "⏰ Please wait for resolution before submitting another request.\n\n"
                    "💎 *Returning to main menu...*",
                    parse_mode="Markdown",
                    reply_markup=get_main_keyboard()
                )
            except:
                pass
            return
    
    # Validate message
    if not message.text:
        try:
            await message.answer(
                "⚠️ **Text Only**\n\n"
                "📝 Please send a text message describing your issue.\n"
                "🚫 Media files are not accepted.",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Error sending text-only message: {e}")
        return
    
    support_text = message.text.strip()
    
    # Check for fake/test messages FIRST (before other validations)
    is_fake, fake_reason = is_fake_support_message(support_text)
    if is_fake:
        # Track fake attempts
        if user_id not in user_fake_attempts:
            user_fake_attempts[user_id] = 0
        user_fake_attempts[user_id] += 1
        attempt_count = user_fake_attempts[user_id]
        
        # Progressive punishment for repeated fake attempts
        if attempt_count >= 5:
            # Get user info for ban record
            user_doc = col_users.find_one({"user_id": user_id})
            msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
            user_name = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
            
            # Permanent ban for persistent fake message spam
            col_banned.insert_one({
                "user_id": user_id,
                "msa_id": msa_id,
                "user_name": user_name,
                "reason": "Repeated fake/spam support messages",
                "banned_at": datetime.now(IST),
                "fake_attempts": attempt_count
            })
            try:
                await message.answer(
                    "🚫 **PERMANENTLY BANNED**\n\n"
                    "❌ Your account has been banned\n"
                    "⚠️ Reason: Repeated spam/fake support requests\n"
                    f"📊 Violations: {attempt_count} fake messages\n\n"
                    "💀 **This ban is permanent.**\n\n"
                    "⚠️ The support system is for legitimate issues only.",
                    parse_mode="Markdown"
                )
            except:
                pass
            await state.clear()
            logger.warning(f"🚫 Banned user {user_id} (MSA: {msa_id}) for {attempt_count} fake support attempts")
            return
        
        # Warning message with attempt counter
        warning_level = ""
        if attempt_count >= 3:
            warning_level = f"\n\n⚠️ **FINAL WARNING:** {attempt_count}/5 violations\n🚨 {5 - attempt_count} more fake attempts = Permanent Ban"
        elif attempt_count >= 2:
            warning_level = f"\n\n⚠️ Warning: {attempt_count}/5 violations\n💡 Continued spam will result in a ban"
        
        try:
            await message.answer(
                "🚫 **INVALID SUPPORT REQUEST**\n\n"
                "⚠️ Your message was rejected\n"
                f"📋 Reason: {fake_reason}\n\n"
                "📝 **Please provide:**\n"
                "   • Clear description of your issue\n"
                "   • What you were trying to do\n"
                "   • What went wrong\n"
                "   • Any error messages received\n\n"
                "💡 **Examples of good requests:**\n"
                "   ✅ \"I'm not receiving the PDF after clicking the link\"\n"
                "   ✅ \"The bot is not responding to my commands\"\n"
                "   ✅ \"I need help changing my account settings\"\n\n"
                "❌ **Examples of bad requests:**\n"
                "   ⛔ \"hi\"\n"
                "   ⛔ \"test\"\n"
                "   ⛔ \"hello there\"\n\n"
                f"🛡️ This filter prevents spam and ensures quality support.{warning_level}",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard()
            )
        except Exception as e:
            logger.error(f"Error sending fake message rejection: {e}")
        await state.clear()
        logger.info(f"⚠️ Rejected fake support message from user {user_id} (attempt {attempt_count}/5)")
        return
    
    # Enhanced spam detection
    # Check for repetitive characters (e.g., "aaaaaaa", "!!!!!!!")
    repetitive_pattern = any(char * 5 in support_text for char in set(support_text))
    
    # Check for too many special characters
    special_char_count = sum(1 for char in support_text if not char.isalnum() and not char.isspace())
    special_char_ratio = special_char_count / len(support_text) if len(support_text) > 0 else 0
    
    # Check for excessive caps
    caps_count = sum(1 for char in support_text if char.isupper())
    caps_ratio = caps_count / len(support_text) if len(support_text) > 0 else 0
    
    # Spam validation
    if repetitive_pattern:
        try:
            await message.answer(
                "⚠️ **Spam Detected**\n\n"
                "🚫 Your message contains repetitive patterns\n"
                "📝 Please write a proper, clear message\n\n"
                "💡 *Tip: Describe your issue naturally*",
                parse_mode="Markdown"
            )
        except:
            pass
        return
    
    if special_char_ratio > 0.3:
        try:
            await message.answer(
                "⚠️ **Invalid Message Format**\n\n"
                "🚫 Too many special characters detected\n"
                "📝 Please use normal text to describe your issue\n\n"
                "💡 *Tip: Use clear, simple language*",
                parse_mode="Markdown"
            )
        except:
            pass
        return
    
    if caps_ratio > 0.7 and len(support_text) > 20:
        try:
            await message.answer(
                "⚠️ **Excessive Caps Lock**\n\n"
                "🚫 Please don't use ALL CAPS\n"
                "📝 Write normally for better assistance\n\n"
                "💡 *Tip: Use normal capitalization*",
                parse_mode="Markdown"
            )
        except:
            pass
        return
    
    # Check length
    if len(support_text) < 10:
        try:
            await message.answer(
                "⚠️ **Message Too Short**\n\n"
                "📝 Please provide more details (minimum 10 characters)\n"
                "✍️ Help us understand your issue better!",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Error sending too-short message: {e}")
        return
    
    if len(support_text) > 500:
        try:
            await message.answer(
                "⚠️ **Message Too Long**\n\n"
                "📝 Please keep your message under 500 characters\n"
                f"📊 Current length: {len(support_text)} characters\n\n"
                "💡 *Tip: Be concise and specific*",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Error sending too-long message: {e}")
        return
    
    # All validations passed - submit the support request
    await process_support_submission(message, state, support_text)

# Handler for media files in support (reject them)
@dp.message(SupportState.waiting_for_message, F.content_type.in_(['photo', 'video', 'document', 'sticker', 'animation', 'voice', 'video_note', 'audio']))
async def handle_media_in_support(message: types.Message, state: FSMContext):
    """Handle media messages in support state"""
    if col_banned.find_one({"user_id": str(message.from_user.id)}):
        return
    
    try:
        await message.answer(
            "⚠️ **Text Only**\n\n"
            "📝 Please send a text message describing your issue\n"
            "🚫 Media files are not accepted in support requests\n\n"
            "✍️ Type your message below:\n\n"
            "💡 Or click '❌ CANCEL' to exit",
            parse_mode="Markdown",
            reply_markup=get_cancel_keyboard()
        )
    except Exception as e:
        logger.error(f"Error handling media in support: {e}")

# ==========================================
# 🤖 ADMIN COMMANDS
# ==========================================

@dp.message(Command("resetcooldown"))
async def cmd_reset_cooldown(message: types.Message, command: CommandObject):
    """Admin command to reset a user's review cooldown"""
    # Check if user is admin/owner
    if message.from_user.id != OWNER_ID:
        try:
            await message.answer("❌ **ACCESS DENIED**\n\n🛡️ This command is restricted to administrators only.")
        except:
            pass
        return
    
    # Get user ID from command
    args = command.args
    if not args:
        try:
            await message.answer(
                "⚠️ **Invalid Usage**\n\n"
                "📝 **Correct Format:**\n"
                "`/resetcooldown <user_id>`\n\n"
                "**Example:**\n"
                "`/resetcooldown 123456789`"
            )
        except:
            pass
        return
    
    target_user_id = args.strip()
    
    try:
        # Premium admin processing animation
        processing = await message.answer("⚡ **Admin Command Initiated...**")
        await asyncio.sleep(0.1)
        await processing.edit_text("🔍 **[░░░░░] 20% - Scanning Database...**")
        await asyncio.sleep(0.1)
        await processing.edit_text("📊 **[██░░░] 40% - Locating User Records...**")
        await asyncio.sleep(0.1)
        await processing.edit_text("⚙️ **[███░░] 60% - Processing Request...**")
        await asyncio.sleep(0.1)
        await processing.edit_text("🎯 **[████░] 80% - Applying Changes...**")
        await asyncio.sleep(0.1)
        await processing.edit_text("✅ **[█████] 100% - Operation Complete!**")
        await asyncio.sleep(0.15)
        await processing.delete()
        
        # Find user's last review
        last_review = col_reviews.find_one(
            {"user_id": target_user_id},
            sort=[("timestamp", -1)]
        )
        
        if not last_review:
            await processing.delete()
            await message.answer(
                f"❌ **User Not Found**\n\n"
                f"🔍 No review found for user ID: `{target_user_id}`\n\n"
                f"💡 Make sure the user has submitted at least one review."
            )
            return
        
        await processing.edit_text("⚙️ **Processing Reset...**")
        await asyncio.sleep(0.5)
        
        # Get user info
        user_name = last_review.get("name", "Unknown")
        username = last_review.get("username", "N/A")
        last_rating = last_review.get("rating", 0)
        channel_message_id = last_review.get("channel_message_id")
        
        # Calculate current cooldown info
        last_review_time = last_review.get("timestamp")
        if last_review_time.tzinfo is None:
            last_review_time = IST.localize(last_review_time)
        
        now_ist = datetime.now(IST)
        time_since_review = now_ist - last_review_time
        days_since = time_since_review.days
        hours_since = time_since_review.seconds // 3600
        
        # Instead of deleting, update the review to mark cooldown as reset
        # This preserves the channel_message_id so future reviews edit the same message
        update_result = col_reviews.update_one(
            {"_id": last_review["_id"]},
            {
                "$set": {
                    "cooldown_reset": True,
                    "reset_by": "admin",
                    "reset_at": now_ist,
                    "reset_admin_id": message.from_user.id
                }
            }
        )
        
        # Delete any older duplicate reviews (keep only the most recent one)
        col_reviews.delete_many({
            "user_id": target_user_id,
            "_id": {"$ne": last_review["_id"]}  # Keep the most recent, delete others
        })
        
        await processing.edit_text("✅ **Reset Complete!**")
        await asyncio.sleep(0.4)
        await processing.delete()
        
        logger.info(f"🔓 Cooldown reset for user {target_user_id}, preserving channel message link")
        
        # Update channel message to show RESET status with 0% progress
        if channel_message_id and REVIEW_LOG_CHANNEL:
            try:
                star_emoji = "⭐" * last_rating
                rating_bar = "★" * last_rating + "☆" * (5 - last_rating)
                reset_timestamp = now_ist.strftime("%d-%m-%Y %I:%M %p")
                
                reset_report = (
                    f"🌟 **SYSTEM PERFORMANCE REVIEW** 🔄 UPDATED\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👤 **OPERATIVE:** {user_name.upper()}\n"
                    f"🆔 **USER ID:** `{target_user_id}`\n"
                    f"🌐 **USERNAME:** {username}\n\n"
                    f"📊 **LAST RATING:** {star_emoji} **{last_rating}/5**\n"
                    f"📈 **BAR:** [{rating_bar}]\n\n"
                    f"💬 **LAST FEEDBACK:**\n_{last_review.get('feedback', 'N/A')}_\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚙️ **VAULT STATUS:** 🟢 ACTIVE\n"
                    f"⏳ **COOLDOWN:** [░░░░░░░░░░░░░░░░░░░░] 0% ✅ **RESET**\n"
                    f"🔓 **STATUS:** READY FOR NEW REVIEW\n"
                    f"🔄 **RESET BY:** Admin\n"
                    f"⏰ **RESET AT:** {reset_timestamp}"
                )
                
                await bot.edit_message_text(
                    chat_id=REVIEW_LOG_CHANNEL,
                    message_id=channel_message_id,
                    text=reset_report,
                    parse_mode="Markdown"
                )
                logger.info(f"✅ Updated channel message to show RESET status for user {target_user_id}")
            except Exception as e:
                logger.error(f"Failed to update channel message: {e}")
        
        # Send success message to admin
        await message.answer(
            f"✅ **Cooldown Reset Successfully!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 **User:** {user_name}\n"
            f"🆔 **ID:** `{target_user_id}`\n"
            f"🌐 **Username:** {username}\n\n"
            f"📊 **Previous Review:**\n"
            f"   ⭐ **Rating:** {last_rating}/5\n"
            f"   📅 **Date:** {days_since}d {hours_since}h ago\n\n"
            f"🔓 **Status:** Cooldown bypassed - user can review now!\n"
            f"📝 **Channel Message:** Updated to show 0% cooldown\n"
            f"🔄 **Smart Tracking:** Message ID preserved for future edits\n\n"
            f"💡 Next review will update the same message (no duplicates)."
        ) 
        
        logger.info(f"🔓 Admin {message.from_user.id} reset cooldown for user {target_user_id}")
        
    except Exception as e:
        logger.error(f"Error in resetcooldown command: {e}")
        try:
            if 'processing' in locals():
                await processing.delete()
        except:
            pass
        try:
            await message.answer(
                f"❌ **Error Occurred**\n\n"
                f"⚠️ Failed to reset cooldown: {str(e)}\n\n"
                f"💡 Please check the user ID and try again."
            )
        except:
            pass

# ==========================================
# 📚 GUIDE SYSTEM - HOW TO USE
# ==========================================

@dp.message(Command("guide"))
async def cmd_guide(message: types.Message):
    """Show comprehensive guide/how to use menu"""
    user_id = message.from_user.id
    
    # Ban check
    if col_banned.find_one({"user_id": str(user_id)}):
        return
    
    # Anti-spam protection
    current_time = time.time()
    if user_id not in user_guide_views:
        user_guide_views[user_id] = []
    
    # Clean old timestamps
    user_guide_views[user_id] = [
        ts for ts in user_guide_views[user_id] 
        if current_time - ts < GUIDE_CLICK_WINDOW
    ]
    
    # Check for rapid clicking
    recent_views = len(user_guide_views[user_id])
    if recent_views >= 3:
        user_guide_spam[user_id] = user_guide_spam.get(user_id, 0) + 1
        
        if user_guide_spam[user_id] >= GUIDE_SPAM_FREEZE:
            try:
                await message.answer(
                    "🚫 **GUIDE ACCESS FROZEN**\n\n"
                    f"⚠️ You've been clicking too rapidly!\n\n"
                    f"⏸️ **Freeze Duration:** 30 seconds\n"
                    f"📊 **Spam Count:** {user_guide_spam[user_id]}\n\n"
                    "💡 Please use the guide normally without spamming.",
                    parse_mode="Markdown"
                )
            except:
                pass
            await asyncio.sleep(30)
            user_guide_spam[user_id] = 0
            return
    
    # Add current view
    user_guide_views[user_id].append(current_time)
    
    # ULTRA PREMIUM 10-STAGE ANIMATION
    loading = await message.answer("⚡ **INITIALIZING GUIDE SYSTEM...**", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🔐 **[▓░░░░░░░░░] 10%** - Authenticating Access...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🔍 **[▓▓░░░░░░░░] 20%** - Scanning Features Database...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("📡 **[▓▓▓░░░░░░░] 30%** - Connecting to Documentation Server...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("📖 **[▓▓▓▓░░░░░░] 40%** - Compiling User Guides...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("⚙️ **[▓▓▓▓▓░░░░░] 50%** - Processing Instructions...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("✨ **[▓▓▓▓▓▓░░░░] 60%** - Formatting Content Layout...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🎨 **[▓▓▓▓▓▓▓░░░] 70%** - Applying Premium Styling...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🎯 **[▓▓▓▓▓▓▓▓░░] 80%** - Preparing Interactive Menu...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("🚀 **[▓▓▓▓▓▓▓▓▓░] 90%** - Finalizing User Experience...", parse_mode="Markdown")
    await asyncio.sleep(0.08)
    await loading.edit_text("💎 **[▓▓▓▓▓▓▓▓▓▓] 100%** - ✅ GUIDE SYSTEM READY!", parse_mode="Markdown")
    await asyncio.sleep(0.2)
    await loading.delete()
    
    # Get user info
    user_doc = col_users.find_one({"user_id": str(user_id)})
    msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
    user_name = user_doc.get("first_name", "User") if user_doc else "User"
    
    guide_message = (
        "📚 **MSANode Bot - Complete Guide**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 **Welcome, {user_name}!**\n"
        f"🏷️ **Your MSA ID:** `{msa_id}`\n\n"
        "🎯 **What would you like to learn?**\n\n"
        "Select any topic below to get detailed instructions:\n\n"
        "📚 **Support System** - How to get help\n"
        "⭐ **Review System** - How to leave reviews\n"
        "🛡️ **Anti-Spam** - Protection features explained\n"
        "⚡ **Commands** - All available commands\n"
        "💎 **Premium Features** - Advanced functionality\n"
        "❓ **FAQ** - Frequently asked questions\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 **Tip:** Choose any button below to start!"
    )
    
    try:
        await message.answer(
            guide_message,
            reply_markup=get_guide_main_keyboard(),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error sending guide menu: {e}")

@dp.message(Command("resolve"))
async def cmd_resolve_support(message: types.Message, command: CommandObject):
    """Admin command to mark support request as resolved"""
    # Check if user is admin/owner
    if message.from_user.id != OWNER_ID:
        try:
            await message.answer("❌ **ACCESS DENIED**\n\n🛡️ This command is restricted to administrators only.")
        except:
            pass
        return
    
    # Get user ID from command
    args = command.args
    if not args:
        try:
            await message.answer(
                "⚠️ **Invalid Usage**\n\n"
                "📝 **Correct Format:**\n"
                "`/resolve <msa_id or telegram_id>`\n\n"
                "**Examples:**\n"
                "`/resolve MSA1`\n"
                "`/resolve 123456789`",
                parse_mode="Markdown"
            )
        except:
            pass
        return
    
    identifier = args.strip()
    
    # Check if it's MSA ID (starts with MSA) or Telegram ID (numeric)
    target_user_id = None
    if identifier.upper().startswith("MSA"):
        # Look up user by MSA ID
        user_doc = col_users.find_one({"msa_id": identifier.upper()})
        if user_doc:
            target_user_id = user_doc["user_id"]
        else:
            try:
                await message.answer(
                    f"❌ **User Not Found**\n\n"
                    f"🔍 No user found with MSA ID: `{identifier.upper()}`\n\n"
                    f"💡 Please verify the ID and try again.",
                    parse_mode="Markdown"
                )
            except:
                pass
            return
    else:
        # Assume it's a Telegram ID
        target_user_id = identifier
    
    try:
        # Check if user has pending support request
        if target_user_id not in user_support_pending:
            await message.answer(
                f"❌ **No Pending Request**\n\n"
                f"🔍 User ID `{target_user_id}` has no pending support requests.\n\n"
                f"💡 User may have already been resolved or never submitted a request.",
                parse_mode="Markdown"
            )
            return
        
        # Premium admin processing animation
        processing = await message.answer("⚡ **Admin Resolution Started...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await processing.edit_text("🔍 **[░░░░░] 20% - Loading Request...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await processing.edit_text("📊 **[██░░░] 40% - Verifying Details...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await processing.edit_text("⚙️ **[███░░] 60% - Processing Resolution...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await processing.edit_text("🎯 **[████░] 80% - Updating Records...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await processing.edit_text("✅ **[█████] 100% - Resolution Complete!**", parse_mode="Markdown")
        await asyncio.sleep(0.15)
        await processing.delete()
        
        # Get support request details
        support_data = user_support_pending[target_user_id]
        channel_msg_id = support_data.get('channel_msg_id')
        user_message = support_data.get('message', 'N/A')
        user_name = support_data.get('user_name', 'Unknown')
        username = support_data.get('username', 'No Username')
        submitted_time = datetime.fromtimestamp(support_data['timestamp'], IST).strftime("%d-%m-%Y %I:%M %p")
        resolved_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
        
        # Get MSA ID
        user_doc = col_users.find_one({"user_id": target_user_id})
        msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
        
        # Update channel message to show CLEARED status
        if channel_msg_id and SUPPORT_CHANNEL_ID:
            try:
                resolved_report = (
                    "✅ **SUPPORT REQUEST - CLEARED**\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"👤 **User:** {user_name}\n"
                    f"🏷️ **MSA ID:** `{msa_id}`\n"
                    f"🆔 **TELEGRAM ID:** `{target_user_id}`\n"
                    f"📱 **Username:** {username}\n"
                    f"🕐 **Submitted:** {submitted_time}\n"
                    f"✅ **Resolved:** {resolved_time}\n"
                    f"📊 **Status:** ✅ CLEARED\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n"
                    "💬 **MESSAGE:**\n\n"
                    f"{user_message}\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔗 **Contact:** tg://user?id={target_user_id}\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"✅ *Resolved by admin at {resolved_time}*"
                )
                
                # Try to edit existing message
                try:
                    await bot.edit_message_text(
                        text=resolved_report,
                        chat_id=SUPPORT_CHANNEL_ID,
                        message_id=channel_msg_id,
                        parse_mode="Markdown"
                    )
                    logger.info(f"✅ Updated support channel message #{channel_msg_id}")
                except Exception as edit_error:
                    # If edit fails (message deleted/not found), send new message
                    logger.warning(f"Failed to edit message #{channel_msg_id}: {edit_error}")
                    logger.info("Sending new resolution message instead...")
                    try:
                        await bot.send_message(
                            SUPPORT_CHANNEL_ID,
                            resolved_report,
                            parse_mode="Markdown"
                        )
                        logger.info(f"✅ Sent new resolution message to support channel")
                    except Exception as send_error:
                        logger.error(f"Failed to send new message: {send_error}")
                        
            except Exception as e:
                logger.error(f"Error updating support channel: {e}")
        
        # Mark as cleared (DON'T DELETE - keep for future edits)
        user_support_pending[target_user_id]['status'] = 'cleared'
        user_support_pending[target_user_id]['resolved_time'] = time.time()
        
        # Update database
        col_users.update_one(
            {"user_id": target_user_id},
            {"$set": {
                "support_status": "resolved",
                "support_resolved_at": datetime.now(IST)
            }}
        )
        
        # Set cooldown for next support request (1 hour from now)
        cooldown_end = time.time() + SUPPORT_COOLDOWN_AFTER_RESOLVE
        user_support_cooldown[target_user_id] = cooldown_end
        
        await processing.edit_text("✅ **Request Cleared!**", parse_mode="Markdown")
        await asyncio.sleep(0.3)
        await processing.delete()
        
        # Send notification to the user that their issue is resolved
        try:
            user_notification = (
                "✅ **ISSUE RESOLVED!**\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🎉 **Good News, {user_name}!**\n\n"
                f"Your support request has been successfully resolved by our team!\n\n"
                f"📅 **Resolved on:** {resolved_time}\n"
                f"🏷️ **Your MSA ID:** `{msa_id}`\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💬 **Your Request:**\n{user_message[:150]}{'...' if len(user_message) > 150 else ''}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✨ **Next Steps:**\n"
                f"• Your issue is now closed\n"
                f"• You can submit new requests anytime\n"
                f"• Use /help for assistance\n\n"
                f"💎 Thank you for your patience!\n"
                f"🙏 We're here to help you succeed!"
            )
            await bot.send_message(
                chat_id=int(target_user_id),
                text=user_notification,
                parse_mode="Markdown"
            )
            logger.info(f"✅ Sent resolution notification to user {target_user_id}")
        except Exception as notify_error:
            logger.warning(f"⚠️ Failed to send resolution notification to user {target_user_id}: {notify_error}")
            # Don't fail the entire resolution if user notification fails
        
        # Send success message to admin
        await message.answer(
            f"✅ **Support Request Resolved!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 **User:** {user_name}\n"
            f"🏷️ **MSA ID:** `{msa_id}`\n"
            f"🆔 **TELEGRAM ID:** `{target_user_id}`\n"
            f"📱 **Username:** {username}\n\n"
            f"💬 **Request:** {user_message[:100]}{'...' if len(user_message) > 100 else ''}\n\n"
            f"🕐 **Submitted:** {submitted_time}\n"
            f"✅ **Resolved:** {resolved_time}\n\n"
            f"💎 User can now submit new support requests!\n"
            f"📨 User has been notified of the resolution.",
            parse_mode="Markdown"
        )
        
        logger.info(f"✅ Admin {message.from_user.id} resolved support request for user {target_user_id}")
        
    except Exception as e:
        logger.error(f"Error in resolve command: {e}")
        try:
            if 'processing' in locals():
                await processing.delete()
        except:
            pass
        try:
            # More helpful error message
            error_msg = str(e)
            if "message to edit not found" in error_msg.lower():
                await message.answer(
                    f"⚠️ **Channel Message Not Found**\n\n"
                    f"✅ **Request still marked as resolved!**\n\n"
                    f"📝 **What happened:**\n"
                    f"The original support message in the channel was deleted or not found.\n\n"
                    f"💡 **Result:**\n"
                    f"• User's request is resolved\n"
                    f"• Cooldown applied (1 hour)\n"
                    f"• User can submit new requests after cooldown\n\n"
                    f"🔍 **User ID:** `{target_user_id}`\n"
                    f"✅ Resolution completed successfully despite channel message error.",
                    parse_mode="Markdown"
                )
            else:
                await message.answer(
                    f"❌ **Error Occurred**\n\n"
                    f"⚠️ Failed to resolve request: {error_msg}\n\n"
                    f"💡 Please check the user ID and try again.",
                    parse_mode="Markdown"
                )
        except:
            pass

@dp.message(Command("reply"))
async def cmd_reply_to_ticket(message: types.Message, command: CommandObject):
    """Admin command to reply to user's support ticket
    Usage: /reply <user_id> <message>
    """
    # Check if user is admin/owner
    if message.from_user.id != OWNER_ID:
        try:
            await message.answer("❌ **ACCESS DENIED**\n\n🛡️ This command is restricted to administrators only.")
        except:
            pass
        return
    
    # Get arguments
    args = command.args
    if not args:
        await message.answer(
            "❌ **Invalid Usage**\n\n"
            "**Correct format:**\n"
            "`/reply <user_id> <your message>`\n\n"
            "**Example:**\n"
            "`/reply 123456789 Your issue has been investigated. Here's the solution...`\n\n"
            "💡 The user will receive your message as a notification!",
            parse_mode="Markdown"
        )
        return
    
    # Parse user ID and message
    try:
        parts = args.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer(
                "❌ **Missing Message**\n\n"
                "Please include your reply message after the user ID.\n\n"
                "**Example:**\n"
                "`/reply 123456789 Your issue has been resolved!`",
                parse_mode="Markdown"
            )
            return
        
        target_user_id = parts[0]
        admin_reply = parts[1]
        
        # Verify user exists
        if target_user_id not in user_support_pending:
            await message.answer(
                f"⚠️ **User Not Found**\n\n"
                f"User ID `{target_user_id}` doesn't have any pending support tickets.\n\n"
                f"💡 They may have already been resolved or cancelled.",
                parse_mode="Markdown"
            )
            return
        
        # Get user details
        support_data = user_support_pending[target_user_id]
        user_message = support_data.get('message', 'N/A')
        user_name = support_data.get('user_name', 'User')
        username = support_data.get('username', 'N/A')
        msa_id = "N/A"
        
        # Get MSA ID from database
        user_doc = col_users.find_one({"user_id": target_user_id})
        if user_doc:
            msa_id = user_doc.get('msa_id', 'N/A')
        
        processing = await message.answer("📤 **Sending Reply...**", parse_mode="Markdown")
        
        # Send reply notification to user
        try:
            reply_notification = (
                "💬 **ADMIN REPLY RECEIVED!**\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👋 **Hello {user_name}!**\n\n"
                f"Our admin team has replied to your support request:\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"**📝 Your Request:**\n{user_message[:150]}{'...' if len(user_message) > 150 else ''}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"**💬 Admin Reply:**\n{admin_reply}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✨ **What to do next:**\n"
                f"• Read the reply carefully\n"
                f"• Follow any instructions given\n"
                f"• If issue is resolved, great!\n"
                f"• If you need more help, submit a new ticket\n\n"
                f"🙏 Thank you for your patience!"
            )
            
            await bot.send_message(
                int(target_user_id),
                reply_notification,
                parse_mode="Markdown"
            )
            
            await processing.edit_text("✅ **Reply Sent!**", parse_mode="Markdown")
            await asyncio.sleep(0.3)
            await processing.delete()
            
            # Update ticket with reply timestamp
            user_support_pending[target_user_id]['last_reply'] = time.time()
            user_support_pending[target_user_id]['reply_count'] = user_support_pending[target_user_id].get('reply_count', 0) + 1
            
            # Send confirmation to admin
            reply_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
            await message.answer(
                f"✅ **Reply Delivered Successfully!**\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 **User:** {user_name}\n"
                f"🏷️ **MSA ID:** `{msa_id}`\n"
                f"🆔 **Telegram ID:** `{target_user_id}`\n"
                f"📱 **Username:** {username}\n\n"
                f"💬 **Your Reply:** {admin_reply[:100]}{'...' if len(admin_reply) > 100 else ''}\n\n"
                f"🕐 **Sent At:** {reply_time}\n"
                f"📊 **Total Replies:** {user_support_pending[target_user_id].get('reply_count', 1)}\n\n"
                f"💡 User has been notified and can read your reply now!\n"
                f"✅ Use `/resolve {target_user_id}` when issue is fully resolved.",
                parse_mode="Markdown"
            )
            
            logger.info(f"✅ Admin {message.from_user.id} replied to user {target_user_id}")
            
        except Exception as send_error:
            await processing.delete()
            await message.answer(
                f"❌ **Failed to Send Reply**\n\n"
                f"⚠️ Error: {str(send_error)}\n\n"
                f"**Possible reasons:**\n"
                f"• User blocked the bot\n"
                f"• User deleted their account\n"
                f"• Invalid user ID\n\n"
                f"💡 You can still resolve the ticket with `/resolve {target_user_id}`",
                parse_mode="Markdown"
            )
            logger.error(f"Failed to send reply to user {target_user_id}: {send_error}")
            
    except Exception as e:
        logger.error(f"Error in reply command: {e}")
        try:
            if 'processing' in locals():
                await processing.delete()
        except:
            pass
        await message.answer(
            f"❌ **Error Processing Reply**\n\n"
            f"⚠️ {str(e)}\n\n"
            f"💡 Check the command format and try again.",
            parse_mode="Markdown"
        )

@dp.message(Command("unban"))
async def cmd_unban_user(message: types.Message, command: CommandObject):
    """Admin command to unban a permanently banned user"""
    # Check if user is admin/owner
    if message.from_user.id != OWNER_ID:
        try:
            await message.answer("❌ **ACCESS DENIED**\n\n🛡️ This command is restricted to administrators only.")
        except:
            pass
        return
    
    # Get user ID from command
    args = command.args
    if not args:
        try:
            await message.answer(
                "⚠️ **Invalid Usage**\n\n"
                "📝 **Correct Format:**\n"
                "`/unban <msa_id or telegram_id>`\n\n"
                "**Examples:**\n"
                "`/unban MSA1`\n"
                "`/unban 123456789`",
                parse_mode="Markdown"
            )
        except:
            pass
        return
    
    identifier = args.strip()
    
    # Check if it's MSA ID (starts with MSA) or Telegram ID (numeric)
    target_user_id = None
    if identifier.upper().startswith("MSA"):
        # Look up user by MSA ID
        user_doc = col_users.find_one({"msa_id": identifier.upper()})
        if user_doc:
            target_user_id = user_doc["user_id"]
        else:
            try:
                await message.answer(
                    f"❌ **User Not Found**\n\n"
                    f"🔍 No user found with MSA ID: `{identifier.upper()}`\n\n"
                    f"💡 Please verify the ID and try again.",
                    parse_mode="Markdown"
                )
            except:
                pass
            return
    else:
        # Assume it's a Telegram ID
        target_user_id = identifier
    
    try:
        # Check if user is actually banned
        ban_record = col_banned.find_one({"user_id": target_user_id})
        if not ban_record:
            await message.answer(
                f"❌ **User Not Banned**\n\n"
                f"🔍 User ID `{target_user_id}` is not in the banned list.\n\n"
                f"💡 User is already free to use the bot.",
                parse_mode="Markdown"
            )
            return
        
        # Premium admin processing animation
        processing = await message.answer("⚡ **Admin Unban Initiated...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await processing.edit_text("🔍 **[░░░░░] 20% - Scanning Ban Records...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await processing.edit_text("📊 **[██░░░] 40% - Verifying Ban Details...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        await processing.edit_text("🗑️ **[███░░] 60% - Removing Ban Record...**", parse_mode="Markdown")
        await asyncio.sleep(0.1)
        
        # Get ban details
        ban_reason = ban_record.get('reason', 'Unknown')
        ban_date = ban_record.get('banned_at', 'Unknown')
        if isinstance(ban_date, datetime):
            ban_date_str = ban_date.strftime("%d-%m-%Y %I:%M %p")
        else:
            ban_date_str = str(ban_date)
        
        # Get user details including MSA ID
        user_doc = col_users.find_one({"user_id": target_user_id})
        msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
        user_name = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
        
        # Remove from banned collection
        result = col_banned.delete_one({"user_id": target_user_id})
        
        if result.deleted_count > 0:
            await processing.edit_text("🎯 **[████░] 80% - Restoring Access...**", parse_mode="Markdown")
            await asyncio.sleep(0.1)
            await processing.edit_text("🎉 **[█████] 100% - Unban Complete!**", parse_mode="Markdown")
            await asyncio.sleep(0.15)
            await processing.delete()
            
            # Send success message to admin
            await message.answer(
                f"✅ **User Successfully Unbanned!**\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 **User:** {user_name}\n"
                f"🏷️ **MSA ID:** `{msa_id}`\n"
                f"🆔 **TELEGRAM ID:** `{target_user_id}`\n"
                f"⚠️ **Original Ban Reason:** {ban_reason}\n"
                f"📅 **Banned On:** {ban_date_str}\n"
                f"🔓 **Processed By:** MSA NODE AGENT\n"
                f"⏰ **Unbanned At:** {datetime.now(IST).strftime('%d-%m-%Y %I:%M %p')}\n\n"
                f"💎 **User can now use the bot normally!**\n"
                f"🔄 All bot features restored for this user.",
                parse_mode="Markdown"
            )
            
            logger.info(f"🔓 Admin {message.from_user.id} unbanned user {target_user_id}")
        else:
            await processing.delete()
            await message.answer(
                f"❌ **Unban Failed**\n\n"
                f"⚠️ Could not remove user from ban list.\n\n"
                f"💡 Please try again or check the user ID.",
                parse_mode="Markdown"
            )
            
    except Exception as e:
        logger.error(f"Error in unban command: {e}")
        try:
            if 'processing' in locals():
                await processing.delete()
        except:
            pass
        try:
            await message.answer(
                f"❌ **Error Occurred**\n\n"
                f"⚠️ Failed to unban user: {str(e)}\n\n"
                f"💡 Please check the user ID and try again.",
                parse_mode="Markdown"
            )
        except:
            pass

@dp.message(Command("userinfo"))
async def cmd_user_info(message: types.Message, command: CommandObject):
    """Admin command to get detailed user information by MSA ID or Telegram ID"""
    # Check if user is admin/owner
    if message.from_user.id != OWNER_ID:
        try:
            await message.answer("❌ **ACCESS DENIED**\n\n🛡️ This command is restricted to administrators only.")
        except:
            pass
        return
    
    # Get user identifier from command
    args = command.args
    if not args:
        try:
            await message.answer(
                "⚠️ **Invalid Usage**\n\n"
                "📝 **Correct Format:**\n"
                "`/userinfo <msa_id or telegram_id>`\n\n"
                "**Examples:**\n"
                "`/userinfo MSA1`\n"
                "`/userinfo 123456789`",
                parse_mode="Markdown"
            )
        except:
            pass
        return
    
    identifier = args.strip()
    
    try:
        # Look up user by MSA ID or Telegram ID
        user_doc = None
        if identifier.upper().startswith("MSA"):
            user_doc = col_users.find_one({"msa_id": identifier.upper()})
        else:
            user_doc = col_users.find_one({"user_id": identifier})
        
        if not user_doc:
            await message.answer(
                f"❌ **User Not Found**\n\n"
                f"🔍 No user found with identifier: `{identifier}`\n\n"
                f"💡 Please verify the MSA ID or Telegram ID and try again.",
                parse_mode="Markdown"
            )
            return
        
        # Extract user information
        msa_id = user_doc.get("msa_id", "UNKNOWN")
        telegram_id = user_doc.get("user_id", "UNKNOWN")
        first_name = user_doc.get("first_name", "Unknown")
        username = user_doc.get("username", "No Username")
        status = user_doc.get("status", "Unknown")
        source = user_doc.get("source", "Unknown")
        joined_date = user_doc.get("joined_date", "Unknown")
        last_active = user_doc.get("last_active", "Unknown")
        has_reported = user_doc.get("has_reported", False)
        
        # Check if user has reviews
        review_count = col_reviews.count_documents({"user_id": telegram_id})
        latest_review = col_reviews.find_one(
            {"user_id": telegram_id},
            sort=[("timestamp", -1)]
        )
        
        review_info = "No reviews"
        if latest_review:
            rating = latest_review.get("rating", "N/A")
            review_date = latest_review.get("submitted_at", "Unknown")
            review_info = f"⭐ {rating}/5 (Last: {review_date})"
        
        # Check if user has support tickets
        support_status = "No pending tickets"
        if telegram_id in user_support_pending:
            support_data = user_support_pending[telegram_id]
            if support_data.get('status') == 'pending':
                support_time = datetime.fromtimestamp(support_data['timestamp'], IST).strftime("%d-%m-%Y %I:%M %p")
                support_status = f"⏳ Pending (Since: {support_time})"
            else:
                support_status = "✅ Resolved"
        
        # Check support cooldown
        cooldown_status = "No cooldown"
        if telegram_id in user_support_cooldown:
            cooldown_end = user_support_cooldown[telegram_id]
            if time.time() < cooldown_end:
                cooldown_time = datetime.fromtimestamp(cooldown_end, IST).strftime("%d-%m-%Y %I:%M %p")
                remaining = int(cooldown_end - time.time())
                cooldown_status = f"⏰ Active (Ends: {cooldown_time}, {remaining // 60}m left)"
        
        # Check support history (24h)
        support_24h_count = 0
        if telegram_id in user_support_history:
            current_time = time.time()
            support_24h_count = len([
                ts for ts in user_support_history[telegram_id]
                if current_time - ts < SUPPORT_HISTORY_WINDOW
            ])
        
        # Check fake message attempts
        fake_attempts = user_fake_attempts.get(telegram_id, 0)
        fake_status = f"✅ Clean ({fake_attempts}/5)" if fake_attempts == 0 else f"⚠️ {fake_attempts}/5 violations"
        if fake_attempts >= 3:
            fake_status = f"🚨 {fake_attempts}/5 violations (DANGER)"
        
        # Check ban status
        ban_status = "✅ Not Banned"
        ban_record = col_banned.find_one({"user_id": telegram_id})
        if ban_record:
            ban_reason = ban_record.get("reason", "Unknown")
            ban_date = ban_record.get("banned_at", "Unknown")
            if isinstance(ban_date, datetime):
                ban_date_str = ban_date.strftime("%d-%m-%Y %I:%M %p")
            else:
                ban_date_str = str(ban_date)
            ban_status = f"🚫 BANNED\n   Reason: {ban_reason}\n   Date: {ban_date_str}"
        
        # Status emoji
        status_emoji = "🟢" if status == "Active" else "🔴"
        
        # Send detailed user report
        await message.answer(
            f"📋 **USER INFORMATION REPORT**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**👤 IDENTITY:**\n"
            f"   MSA ID: `{msa_id}`\n"
            f"   Telegram ID: `{telegram_id}`\n"
            f"   Name: {first_name}\n"
            f"   Username: {username}\n"
            f"   Direct: tg://user?id={telegram_id}\n\n"
            f"**📊 STATUS:**\n"
            f"   Account: {status_emoji} {status}\n"
            f"   Source: {source}\n"
            f"   Ban Status: {ban_status}\n\n"
            f"**📅 ACTIVITY:**\n"
            f"   Joined: {joined_date}\n"
            f"   Last Active: {last_active}\n"
            f"   Admin Reported: {'Yes' if has_reported else 'No'}\n\n"
            f"**💬 ENGAGEMENT:**\n"
            f"   Reviews: {review_count} total\n"
            f"   Latest: {review_info}\n"
            f"   Support: {support_status}\n"
            f"   Cooldown: {cooldown_status}\n"
            f"   24h Requests: {support_24h_count}/{SUPPORT_DAILY_LIMIT}\n"
            f"   Fake Attempts: {fake_status}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✨ **All user details retrieved successfully!**",
            parse_mode="Markdown"
        )
        
        logger.info(f"📋 Admin {message.from_user.id} viewed info for user {msa_id} ({telegram_id})")
        
    except Exception as e:
        logger.error(f"Error in userinfo command: {e}")
        try:
            await message.answer(
                f"❌ **Error Occurred**\n\n"
                f"⚠️ Failed to retrieve user info: {str(e)}\n\n"
                f"💡 Please check the identifier and try again.",
                parse_mode="Markdown"
            )
        except:
            pass

# ==========================================
# 🤖 BOT LOGIC: MSANODE INTELLIGENCE HUB
# ==========================================

@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    
    # 0. Check if user was previously banned but is now unbanned
    user_doc = col_users.find_one({"user_id": str(user_id)})
    if user_doc:
        was_unbanned = user_doc.get("was_unbanned", False)
        previous_ban_reason = user_doc.get("previous_ban_reason")
        unbanned_at = user_doc.get("unbanned_at")
        unbanned_by = user_doc.get("unbanned_by", "Admin")
        
        if was_unbanned:
            # User was previously banned but unbanned - show warning
            if isinstance(unbanned_at, datetime):
                unban_date = unbanned_at.strftime("%d %b %Y, %I:%M %p")
            else:
                unban_date = "Recently"
            
            warning_msg = (
                f"⚠️ **SECOND CHANCE GRANTED** ⚠️\n\n"
                f"🔓 Your ban has been lifted by **MSA NODE AGENT**.\n\n"
                f"**📋 Your Details:**\n"
            )
            
            if user_doc.get("msa_id"):
                warning_msg += f"• MSA ID: {user_doc.get('msa_id')}\n"
            
            if previous_ban_reason:
                warning_msg += f"\n**📜 Previous Ban Reason:**\n{previous_ban_reason}\n"
            
            warning_msg += (
                f"\n**🔓 Unbanned:** {unban_date}\n"
                f"**👮 Processed By:** MSA NODE AGENT\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ **FINAL WARNING** ⚠️\n\n"
                f"• This is your LAST CHANCE\n"
                f"• Do NOT repeat the same violations\n"
                f"• Follow bot usage guidelines strictly\n"
                f"• Any spam/abuse = PERMANENT BAN (no appeal)\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"✅ You now have full access to all features.\n"
                f"👉 Use the bot responsibly!"
            )
            
            try:
                await message.answer(warning_msg, parse_mode="Markdown")
                await asyncio.sleep(2)
            except:
                pass
            
            # Clear the unbanned flag so we don't show this message again
            col_users.update_one(
                {"user_id": str(user_id)},
                {"$set": {"was_unbanned": False}}
            )
            
            # Continue to normal flow below
    
    # 1. Ban Protection with custom message and temporary ban check
    ban_record = col_banned.find_one({"user_id": str(user_id)})
    if ban_record:
        ban_type = ban_record.get("ban_type", "permanent")
        ban_until = ban_record.get("ban_until")
        banned_features = ban_record.get("banned_features", [])
        
        # Check if temporary ban has expired
        if ban_type == "temporary" and ban_until:
            if isinstance(ban_until, datetime):
                if datetime.now(IST) >= ban_until:
                    # Ban expired, remove from banned list and restore access
                    col_banned.delete_one({"user_id": str(user_id)})
                    col_users.update_one({"user_id": str(user_id)}, {"$set": {"status": "active"}})
                    
                    # Show welcome back animation with feature status
                    welcome = await message.answer("🔓 **Checking ban status...**")
                    await asyncio.sleep(0.8)
                    await welcome.edit_text("🔓 **Checking ban status...**\n✅ *Temporary ban expired!*")
                    await asyncio.sleep(0.8)
                    
                    # Check if any features were selectively unbanned
                    if len(banned_features) < 4:  # Some features were unbanned during ban
                        unbanned_features = [f for f in ["downloads", "reviews", "support", "search"] if f not in banned_features]
                        if unbanned_features:
                            feature_list = ", ".join([f.title() for f in unbanned_features])
                            await welcome.edit_text(
                                f"🎉 **Welcome back!**\n"
                                f"✨ *Your access has been restored.*\n\n"
                                f"🔓 **Previously Unbanned Features:**\n{feature_list}\n\n"
                                f"💫 *All features are now fully available!*"
                            )
                        else:
                            await welcome.edit_text("🎉 **Welcome back!**\n✨ *Your access has been restored.*")
                    else:
                        await welcome.edit_text("🎉 **Welcome back!**\n✨ *Your access has been restored.*")
                    
                    await asyncio.sleep(2.5)
                    await welcome.delete()
                    # Continue to normal start flow
                else:
                    # Still banned - show remaining time and feature status
                    time_left = ban_until - datetime.now(IST)
                    days = time_left.days
                    hours = time_left.seconds // 3600
                    minutes = (time_left.seconds % 3600) // 60
                    
                    unban_date = ban_until.strftime("%d %b %Y, %I:%M %p IST")
                    
                    custom_reason = ban_record.get("reason")
                    banned_at = ban_record.get("banned_at", "Unknown")
                    banned_by = ban_record.get("banned_by", "Admin")
                    msa_id_temp = ban_record.get("msa_id", "UNKNOWN")
                    
                    if isinstance(banned_at, datetime):
                        ban_date = banned_at.strftime("%d %b %Y, %I:%M %p")
                    else:
                        ban_date = "Unknown Date"
                    
                    # Check for feature unbans
                    if len(banned_features) < 4:
                        unbanned_features = [f for f in ["downloads", "reviews", "support", "search"] if f not in banned_features]
                        feature_list = ", ".join([f.title() for f in unbanned_features])
                        feature_status = f"\n\n🔓 **Partially Unbanned Features:**\n{feature_list}\n💡 *These will be available when your ban expires.*"
                    else:
                        feature_status = ""
                    
                    if custom_reason:
                        ban_msg = (
                            f"⏰ **TEMPORARY BAN ACTIVE**\n\n"
                            f"⛔ Your account is temporarily banned.\n\n"
                            f"**📋 Ban Details:**\n"
                            f"• MSA ID: {msa_id_temp}\n"
                            f"• Reason: {custom_reason}\n\n"
                            f"**⏳ Time Remaining:**\n"
                            f"  • {days} days, {hours} hours, {minutes} minutes\n\n"
                            f"**🔓 Auto-Unban:** {unban_date}\n"
                            f"**📅 Banned On:** {ban_date}\n"
                            f"**👮 Banned By:** {banned_by}{feature_status}\n\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"⏸️ Please wait until the ban period ends.\n"
                            f"💬 Contact support if you believe this is a mistake."
                        )
                    else:
                        ban_msg = (
                            f"⏰ **TEMPORARY BAN ACTIVE**\n\n"
                            f"⛔ Your account is temporarily banned.\n\n"
                            f"**📋 Your Details:**\n"
                            f"• MSA ID: {msa_id_temp}\n\n"
                            f"**⏳ Time Remaining:**\n"
                            f"  • {days} days, {hours} hours, {minutes} minutes\n\n"
                            f"**🔓 Auto-Unban:** {unban_date}\n"
                            f"**📅 Banned On:** {ban_date}\n"
                            f"**👮 Banned By:** {banned_by}{feature_status}\n\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"This action was taken due to a violation of our terms of service.\n\n"
                            f"⏸️ Access will be automatically restored after the ban period.\n"
                            f"💬 If you believe this is an error, please contact the administrator."
                        )
                    
                    try:
                        await message.answer(ban_msg, reply_markup=get_main_keyboard(user_id))
                    except:
                        pass
                    return
            else:
                # Invalid datetime, treat as permanent
                pass
        
        # Permanent ban or invalid temporary ban
        if ban_type == "permanent" or not ban_until:
            custom_reason = ban_record.get("reason")
            banned_at = ban_record.get("banned_at", "Unknown")
            banned_by = ban_record.get("banned_by", "Admin")
            msa_id_perm = ban_record.get("msa_id", "UNKNOWN")
            
            if isinstance(banned_at, datetime):
                ban_date = banned_at.strftime("%d %b %Y, %I:%M %p")
            else:
                ban_date = "Unknown Date"
            
            # Check for feature unbans in permanent ban
            if len(banned_features) < 4:
                unbanned_features = [f for f in ["downloads", "reviews", "support", "search"] if f not in banned_features]
                feature_list = ", ".join([f.title() for f in unbanned_features])
                feature_status = f"\n\n🔓 **Partially Unbanned Features:**\n{feature_list}\n💡 *Admin has restored some of your features.*\n⚠️ *However, you are still banned from using the bot.*"
            else:
                feature_status = ""
            
            if custom_reason:
                ban_msg = (
                    f"🚫 **ACCESS DENIED**\n\n"
                    f"⛔ Your account has been permanently banned.\n\n"
                    f"**📋 Ban Details:**\n"
                    f"• MSA ID: {msa_id_perm}\n"
                    f"• Reason: {custom_reason}\n\n"
                    f"**📅 Banned On:** {ban_date}\n"
                    f"**👮 Banned By:** {banned_by}{feature_status}\n\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"❌ You can no longer access this bot.\n"
                    f"💬 Contact support if you believe this is a mistake."
                )
            else:
                ban_msg = (
                    f"🚫 **ACCESS DENIED**\n\n"
                    f"⛔ Your account has been banned from using this bot.\n\n"
                    f"**📋 Your Details:**\n"
                    f"• MSA ID: {msa_id_perm}\n\n"
                    f"**📅 Banned On:** {ban_date}\n"
                    f"**👮 Banned By:** {banned_by}{feature_status}\n\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"**🔒 Status:** Permanent Ban\n\n"
                    f"This action was taken due to a violation of our terms of service.\n\n"
                    f"❌ You are no longer permitted to use this bot.\n"
                    f"💬 If you believe this is an error, please contact the administrator."
                )
            
            try:
                await message.answer(ban_msg, reply_markup=get_main_keyboard(user_id))
            except:
                pass
            return
    
    # 2. Anti-Spam Protection for /start command
    current_time = time.time()
    if user_id not in start_command_tracker:
        start_command_tracker[user_id] = []
    
    # Clean old timestamps (older than 60 seconds)
    start_command_tracker[user_id] = [
        ts for ts in start_command_tracker[user_id] 
        if current_time - ts < 60
    ]
    
    # Check spam threshold
    start_count = len(start_command_tracker[user_id])
    
    if start_count >= 5:  # 5+ /start commands in 60 seconds = spam
        # Get user info including MSA ID
        user_doc = col_users.find_one({"user_id": str(user_id)})
        msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
        user_name = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
        username = user_doc.get("username", "No Username") if user_doc else "No Username"
        
        # Permanent ban for severe spam with complete information
        col_banned.insert_one({
            "user_id": str(user_id),
            "msa_id": msa_id,
            "username": username,
            "user_name": user_name,
            "reason": f"Spamming /start command - {start_count} attempts in 60 seconds",
            "violation_type": "Start Command Spam",
            "banned_from": "/start command",
            "banned_at": datetime.now(IST),
            "banned_by": "MSANode Security Agent",
            "ban_type": "permanent",
            "ban_until": None,
            "banned_features": ["downloads", "reviews", "support", "search", "dashboard", "guide", "faq"]
        })
        
        # Log to ban history
        col_ban_history.insert_one({
            "user_id": str(user_id),
            "msa_id": msa_id,
            "username": username,
            "user_name": user_name,
            "action_type": "auto_ban",
            "admin_name": "MSANode Security Agent",
            "reason": f"Spamming /start command - {start_count} attempts",
            "ban_type": "permanent",
            "ban_until": None,
            "banned_features": ["downloads", "reviews", "support", "search", "dashboard", "guide", "faq"],
            "banned_from": "/start command",
            "violation_type": "Start Command Spam",
            "timestamp": datetime.now(IST)
        })
        
        # Send detailed ban report
        asyncio.create_task(send_ban_report(
            user_id=user_id,
            reason=f"Spamming /start command - {start_count} attempts in 60 seconds",
            violation_type="Start Command Spam",
            banned_from="/start command",
            banned_by="MSANode Security Agent"
        ))
        
        try:
            await message.answer(
                f"🚫 **ACCESS PERMANENTLY DENIED**\n\n"
                f"⛔ Your account has been banned for spamming the bot.\n\n"
                f"**📋 Your Details:**\n"
                f"• MSA ID: {msa_id}\n\n"
                f"**Reason:** Excessive /start command abuse\n"
                f"**Status:** Permanent Ban\n\n"
                f"❌ You can no longer access this bot.\n"
                f"💬 Use Customer Support below to appeal.",
                reply_markup=get_main_keyboard(user_id)
            )
        except:
            pass
        logger.warning(f"🚫 Banned user {user_id} (MSA: {msa_id}) for /start spam")
        return
    
    elif start_count >= 3:  # 3-4 /start commands = warning
        try:
            await message.answer(
                "⚠️ **SPAM WARNING**\n\n"
                f"🛑 You have sent /start **{start_count}** times in the last minute.\n\n"
                "**⏸️ Please slow down!**\n"
                f"➤ {5 - start_count} more attempts will result in a permanent ban.\n\n"
                "💡 Use the bot normally without spamming."
            )
        except:
            pass
        await asyncio.sleep(2)  # Force 2-second delay
    
    # Add current timestamp
    start_command_tracker[user_id].append(current_time)
    
    # 3. Maintenance Check - PREMIUM VERSION
    maint = col_settings.find_one({"setting": "maintenance"})
    if maint and maint.get("value"):
        # Premium maintenance animation
        maint_msg = await message.answer("🔍 **Checking System Status...**", parse_mode="Markdown")
        await asyncio.sleep(0.4)
        await maint_msg.edit_text("⚙️ **[░░░░░] 20% - Scanning Servers...**", parse_mode="Markdown")
        await asyncio.sleep(0.3)
        await maint_msg.edit_text("🔧 **[██░░░] 40% - Verifying Systems...**", parse_mode="Markdown")
        await asyncio.sleep(0.3)
        await maint_msg.edit_text("🛠️ **[████░] 80% - Status Retrieved...**", parse_mode="Markdown")
        await asyncio.sleep(0.3)
        await maint_msg.edit_text("⚠️ **[█████] 100% - Maintenance Detected**", parse_mode="Markdown")
        await asyncio.sleep(0.5)
        await maint_msg.delete()
        
        # Get maintenance details
        maint_reason = maint.get("reason", "System upgrades in progress")
        maint_eta = maint.get("eta", "Soon")
        maint_started = maint.get("started_at", "Unknown")
        
        try:
            await message.answer(
                "╔══════════════════════════╗\n"
                "║  🔴 **MSANODE AGENT**    ║\n"
                "║     **● OFFLINE**        ║\n"
                "╚══════════════════════════╝\n\n"
                "🚧 **SYSTEM MAINTENANCE IN PROGRESS**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"**📋 Status:** Under Maintenance\n"
                f"**🔧 Reason:** {maint_reason}\n"
                f"**⏰ Started:** {maint_started}\n"
                f"**🕐 ETA:** {maint_eta}\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "**⚠️ ALL SERVICES TEMPORARILY UNAVAILABLE:**\n"
                "• Content downloads\n"
                "• Review system\n"
                "• Customer support\n"
                "• Dashboard access\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "💡 **We'll notify you when the system is back online!**\n\n"
                "🔔 **Stay tuned for updates.**\n"
                "Thank you for your patience! 🙏",
                parse_mode="Markdown"
            )
        except:
            pass
        return 

    # 3.5. Terms & Conditions Check - Must be accepted before using bot
    if not has_accepted_terms(user_id):
        # User hasn't accepted terms yet - show terms and require acceptance
        terms_kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✅ I Accept the Terms & Conditions")],
                [KeyboardButton(text="❌ I Reject")]
            ],
            resize_keyboard=True,
            one_time_keyboard=False
        )
        
        terms_message = (
            f"**Welcome, {message.from_user.first_name}!** 🎉\n\n"
            f"╔════════════════════════════╗\n"
            f"║  📜 **TERMS & CONDITIONS**  ║\n"
            f"╚════════════════════════════╝\n\n"
            f"⚠️ **IMPORTANT: You must read and accept our terms to use this bot.**\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**1️⃣ GENERAL CONDUCT**\n"
            f"• Be respectful to all users and staff\n"
            f"• No harassment, hate speech, or abuse\n"
            f"• Use the bot for intended purposes only\n"
            f"• Follow all commands and guidelines\n\n"
            f"**2️⃣ SPAM PREVENTION**\n"
            f"• Do NOT spam commands repeatedly\n"
            f"• Limit: 10 actions per minute\n"
            f"• Spamming = Automatic permanent ban\n"
            f"• No flooding in reviews or support\n\n"
            f"**3️⃣ REVIEWS & FEEDBACK**\n"
            f"• Provide honest, constructive reviews\n"
            f"• No fake, abusive, or spam reviews\n"
            f"• Minimum rating rules apply\n"
            f"• Review system can be disabled anytime\n\n"
            f"**4️⃣ CUSTOMER SUPPORT**\n"
            f"• Use support for legitimate issues only\n"
            f"• Be patient, we respond ASAP\n"
            f"• No spam or abuse in support tickets\n"
            f"• One active ticket at a time\n\n"
            f"**5️⃣ BANS & PENALTIES**\n"
            f"• Violations result in temporary or permanent bans\n"
            f"• Banned users can appeal via 🔔 APPEAL BAN\n"
            f"• Repeat offenders get permanent bans\n"
            f"• Admin decisions are final\n\n"
            f"**6️⃣ CONTENT ACCESS**\n"
            f"• Membership in Telegram Vault required\n"
            f"• Leaving channel = Access revoked\n"
            f"• Social media verification required\n"
            f"• Premium content via pinned comments\n\n"
            f"**7️⃣ DATA & PRIVACY**\n"
            f"• Your data is stored securely\n"
            f"• MSA ID assigned for tracking\n"
            f"• Activity logs maintained for security\n"
            f"• No data shared with third parties\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**✅ BY ACCEPTING, YOU AGREE TO:**\n"
            f"• Follow all rules above\n"
            f"• Accept admin moderation decisions\n"
            f"• Respect the community guidelines\n"
            f"• Use the bot responsibly\n\n"
            f"❌ **BY REJECTING:**\n"
            f"• You will NOT be able to use this bot\n"
            f"• All features will remain locked\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**👇 Please make your choice below:**"
        )
        
        try:
            await message.answer(terms_message, reply_markup=terms_kb, parse_mode="Markdown")
        except:
            pass
        return

    # 4. Parse Arg/Source & Log
    raw_arg = command.args
    source = "Unknown"; payload = None
    if raw_arg:
        if raw_arg.startswith("ig_"): source = "Instagram"; payload = raw_arg.replace("ig_", "")
        elif raw_arg.startswith("yt_"): source = "YouTube"; payload = raw_arg.replace("yt_", "")
        else: payload = raw_arg
    
    u_status = await log_user(message.from_user, source)

    # 6. PREMIUM VERIFICATION ANIMATION
    load = await message.answer("╔════════════════════╗\n║   🎯 **MSANode**   ║\n║  **Security Hub**  ║\n╚════════════════════╝\n\n🔄 *Initializing...*", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await load.edit_text("╔════════════════════╗\n║   🎯 **MSANode**   ║\n║  **Security Hub**  ║\n╚════════════════════╝\n\n🔐 *Scanning credentials...*\n▰▰▰▱▱▱▱▱▱▱ 30%", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await load.edit_text("╔════════════════════╗\n║   🛰️ **MSANode**   ║\n║  **Vault Network** ║\n╚════════════════════╝\n\n📡 *Establishing secure link...*\n▰▰▰▰▰▰▱▱▱▱ 60%", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await load.edit_text("╔════════════════════╗\n║   ⚡ **MSANode**   ║\n║ **Authentication** ║\n╚════════════════════╝\n\n🔓 *Verifying permissions...*\n▰▰▰▰▰▰▰▰▱▱ 85%", parse_mode="Markdown")
    await asyncio.sleep(0.15)
    await load.edit_text("╔════════════════════╗\n║   💎 **MSANode**   ║\n║  **Premium Hub**   ║\n╚════════════════════╝\n\n✨ *Access Granted!*\n▰▰▰▰▰▰▰▰▰▰ 100%", parse_mode="Markdown")
    await asyncio.sleep(0.2)
    
    # Show MSANODE AGENT status for unrestricted users
    try:
        await load.delete()
    except:
        pass
    
    # Display MSANODE AGENT LIVE status with breathing animation
    agent_msg = await message.answer(
        "╔════════════════════════╗\n"
        "║   ⚪ **MSANODE AGENT**   ║\n"
        "║      **○ Starting...**   ║\n"
        "╚════════════════════════╝",
        parse_mode="Markdown"
    )
    
    # Breathing animation - 4 stages
    await asyncio.sleep(0.4)
    await agent_msg.edit_text(
        "╔════════════════════════╗\n"
        "║   🔵 **MSANODE AGENT**   ║\n"
        "║      **◐ Initializing**  ║\n"
        "╚════════════════════════╝",
        parse_mode="Markdown"
    )
    
    await asyncio.sleep(0.4)
    await agent_msg.edit_text(
        "╔════════════════════════╗\n"
        "║   🟡 **MSANODE AGENT**   ║\n"
        "║      **◑ Connecting...**  ║\n"
        "╚════════════════════════╝",
        parse_mode="Markdown"
    )
    
    await asyncio.sleep(0.4)
    await agent_msg.edit_text(
        "╔════════════════════════╗\n"
        "║   🟢 **MSANODE AGENT**   ║\n"
        "║      **● ACTIVE**        ║\n"
        "╚════════════════════════╝\n\n"
        "✅ **System Status:** Operational\n"
        "🔐 **Security:** Active\n"
        "⚡ **Response Time:** <1ms\n"
        "💚 **Heartbeat:** Live",
        parse_mode="Markdown"
    )
    
    await asyncio.sleep(1.2)
    try:
        await agent_msg.delete()
    except:
        pass

    # 7. Membership Gate with Social Media Tracking
    if not await is_member(message.from_user.id):
        # Check if user is RETURNING (was member before but left) or NEW
        user_doc = col_users.find_one({"user_id": str(user_id)})
        is_returning_user = user_doc is not None  # If user exists in DB, they were here before
        
        if is_returning_user:
            # RETURNING USER WHO LEFT - Force them to rejoin!
            rejoin_kb = InlineKeyboardBuilder()
            rejoin_kb.row(InlineKeyboardButton(text="🚀 REJOIN TELEGRAM VAULT", url=CHANNEL_LINK))
            
            rejoin_msg = await message.answer(
                f"⚠️ **ACCESS RESTRICTED, {message.from_user.first_name}!**\n\n"
                f"🔒 **Vault Membership Required**\n\n"
                f"You left the MSANode Telegram Vault. To regain access to premium content, you must rejoin.\n\n"
                f"**🚨 Important:**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"• All premium features disabled\n"
                f"• Content access blocked\n"
                f"• Rejoin required to continue\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👇 **Rejoin now to restore access:**",
                reply_markup=rejoin_kb.as_markup()
            )
            # Store message ID for auto-deletion when user rejoins
            if user_id not in user_social_clicks:
                user_social_clicks[user_id] = {}
            user_social_clicks[user_id]['rejoin_msg_id'] = rejoin_msg.message_id
            return  # Block access until they rejoin
        
        # NEW USER - Show verification flow
        # Track source for verification (internal only)
        if user_id not in user_social_clicks:
            user_social_clicks[user_id] = {'ig': False, 'yt': False, 'source': source}
        
        kb = InlineKeyboardBuilder()
        
        # Conditionally show social media buttons based on source
        if source == "Instagram":
            # From Instagram → Show ONLY YouTube + Telegram
            kb.row(InlineKeyboardButton(text="▶️ Subscribe to Unlock", callback_data=f"social_yt"))
            kb.row(InlineKeyboardButton(text="🚀 Join Telegram Vault", url=CHANNEL_LINK))
            kb.row(InlineKeyboardButton(text="✅ I HAVE COMPLETED", callback_data=f"verify_{source}_{raw_arg or 'none'}"))
            
            verification_msg = (
                f"**Welcome, {message.from_user.first_name}!** 🎯\n\n"
                f"🔐 **Premium Access Requirements**\n\n"
                f"To unlock exclusive premium content, complete these requirements:\n\n"
                f"**1️⃣ YouTube Subscription**\n"
                f"   ▶️ Subscribe to our YouTube channel\n"
                f"   💡 Access premium video strategies\n\n"
                f"**2️⃣ Telegram Vault Membership**\n"
                f"   🚀 Join our exclusive Telegram community\n"
                f"   💬 Get instant updates & support\n\n"
                f"**3️⃣ Verification**\n"
                f"   ✅ Click 'I HAVE COMPLETED' when done\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💎 **What You'll Unlock:**\n"
                f"• Premium automation blueprints\n"
                f"• Exclusive tools & resources\n"
                f"• Priority support access\n"
                f"• Advanced strategies\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"⚡ Complete both requirements to begin!"
            )
            
        elif source == "YouTube":
            # From YouTube → Show ONLY Instagram + Telegram
            kb.row(InlineKeyboardButton(text="📸 Follow to Unlock", callback_data=f"social_ig"))
            kb.row(InlineKeyboardButton(text="🚀 Join Telegram Vault", url=CHANNEL_LINK))
            kb.row(InlineKeyboardButton(text="✅ I HAVE COMPLETED", callback_data=f"verify_{source}_{raw_arg or 'none'}"))
            
            verification_msg = (
                f"**Welcome, {message.from_user.first_name}!** 🎯\n\n"
                f"🔐 **Premium Access Requirements**\n\n"
                f"To unlock exclusive premium content, complete these requirements:\n\n"
                f"**1️⃣ Instagram Follow**\n"
                f"   📸 Follow our Instagram profile\n"
                f"   ✨ Get daily automation tips\n\n"
                f"**2️⃣ Telegram Vault Membership**\n"
                f"   🚀 Join our exclusive Telegram community\n"
                f"   💬 Get instant updates & support\n\n"
                f"**3️⃣ Verification**\n"
                f"   ✅ Click 'I HAVE COMPLETED' when done\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💎 **What You'll Unlock:**\n"
                f"• Premium automation blueprints\n"
                f"• Exclusive tools & resources\n"
                f"• Priority support access\n"
                f"• Advanced strategies\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"⚡ Complete both requirements to begin!"
            )
            
        else:
            # Unknown source - Don't show verification, send to pinned comments instead
            try:
                await load.delete()
            except:
                pass
            
            kb_unknown = InlineKeyboardBuilder()
            kb_unknown.row(InlineKeyboardButton(text="▶️ YouTube Channel", url=YOUTUBE_LINK))
            kb_unknown.row(InlineKeyboardButton(text="📸 Instagram Profile", url=INSTAGRAM_LINK))
            
            await message.answer(
                f"**Hello, {message.from_user.first_name}!** 👋\n\n"
                f"I noticed you started the bot directly.\n\n"
                f"**📍 To Access Premium Content:**\n"
                f"➤ Visit my **YouTube** or **Instagram**\n"
                f"➤ Check the **PINNED COMMENT** on recent posts\n"
                f"➤ Use the exclusive link from there\n\n"
                f"💎 Each pinned comment contains direct access to premium guides and strategies.\n\n"
                f"🎯 Visit my channels below:",
                reply_markup=kb_unknown.as_markup()
            )
            await asyncio.sleep(0.3)
            await message.answer("💼 **Explore the channels and return with a link!**", reply_markup=get_main_keyboard())
            return
        
        # Store verification message ID for potential auto-deletion
        verify_msg = await message.answer(verification_msg, reply_markup=kb.as_markup())
        user_social_clicks[user_id]['verify_msg_id'] = verify_msg.message_id
        return

    # 8. Core Delivery Logic - User is in Telegram vault
    # Delete rejoin message if it exists (returning user who just rejoined)
    if user_id in user_social_clicks and 'rejoin_msg_id' in user_social_clicks[user_id]:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=user_social_clicks[user_id]['rejoin_msg_id'])
        except:
            pass
        # Clean up tracking
        if user_id in user_social_clicks:
            del user_social_clicks[user_id]
    
    try:
        await load.delete()
    except:
        pass
    if payload:
        # User requested a specific M-Code
        if u_status == "NEW":
            # Premium animations for new user with content
            loading = await message.answer("⚡ **Initializing Access Protocol...**")
            await asyncio.sleep(0.8)
            await loading.edit_text("🔐 **Establishing Secure Connection...**")
            await asyncio.sleep(0.7)
            await loading.edit_text("✅ **Access Granted. Welcome Aboard!**")
            await asyncio.sleep(0.5)
            await loading.delete()
            
            await message.answer(
                f"**Welcome, {message.from_user.first_name}!** 🎯\n\n"
                f"Your access has been successfully activated. Preparing your exclusive content...",
                reply_markup=get_main_keyboard()
            )
        else:
            await message.answer(
                f"**Welcome Back, {message.from_user.first_name}.** ✅\n\n"
                f"Access verified. Loading your requested blueprint...",
                reply_markup=get_main_keyboard()
            )
        await deliver_content(message, payload, source)
    else:
        # No payload - check if NEW or RETURNING user
        if u_status == "NEW":
            # NEW user without link - Show Rules & Regulations first
            loading = await message.answer("🔍 **Scanning Access Level...**")
            await asyncio.sleep(0.8)
            await loading.edit_text("📊 **Analyzing Entry Point...**")
            await asyncio.sleep(0.7)
            await loading.edit_text("✅ **New User Detected - Loading Guidelines**")
            await asyncio.sleep(0.6)
            await loading.delete()
            
            # Show Rules & Regulations to new users
            rules_msg = (
                f"**Welcome, {message.from_user.first_name}!** 🎉\n\n"
                f"╔════════════════════════╗\n"
                f"║  📜 **RULES & REGULATIONS** ║\n"
                f"╚════════════════════════╝\n\n"
                f"**⚠️ IMPORTANT: Please Read Carefully**\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"**1️⃣ GENERAL CONDUCT**\n"
                f"• Be respectful to all users and staff\n"
                f"• No harassment, hate speech, or abuse\n"
                f"• Use the bot for intended purposes only\n"
                f"• Follow all commands and guidelines\n\n"
                f"**2️⃣ SPAM PREVENTION**\n"
                f"• Do NOT spam commands repeatedly\n"
                f"• Limit: 10 actions per minute\n"
                f"• Spamming = Automatic permanent ban\n"
                f"• No flooding in reviews or support\n\n"
                f"**3️⃣ REVIEWS & FEEDBACK**\n"
                f"• Provide honest, constructive reviews\n"
                f"• No fake, abusive, or spam reviews\n"
                f"• Minimum rating rules apply\n"
                f"• Review system can be disabled anytime\n\n"
                f"**4️⃣ CUSTOMER SUPPORT**\n"
                f"• Use support for legitimate issues only\n"
                f"• Be patient, we respond ASAP\n"
                f"• No spam or abuse in support tickets\n"
                f"• One active ticket at a time\n\n"
                f"**5️⃣ BANS & PENALTIES**\n"
                f"• Violations result in temporary or permanent bans\n"
                f"• Banned users can appeal via 🔔 APPEAL BAN\n"
                f"• Repeat offenders get permanent bans\n"
                f"• Admin decisions are final\n\n"
                f"**6️⃣ CONTENT ACCESS**\n"
                f"• Membership in Telegram Vault required\n"
                f"• Leaving channel = Access revoked\n"
                f"• Social media verification required\n"
                f"• Premium content via pinned comments\n\n"
                f"**7️⃣ DATA & PRIVACY**\n"
                f"• Your data is stored securely\n"
                f"• MSA ID assigned for tracking\n"
                f"• Activity logs maintained for security\n"
                f"• No data shared with third parties\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"**✅ BY USING THIS BOT, YOU AGREE TO:**\n"
                f"• Follow all rules above\n"
                f"• Accept admin moderation decisions\n"
                f"• Respect the community guidelines\n"
                f"• Use the bot responsibly\n\n"
                f"❌ **Violations = Immediate Action**\n"
                f"💡 **Questions? Use** 💬 CUSTOMER SUPPORT\n\n"
                f"🎯 **Now you're ready to explore!**"
            )
            await message.answer(rules_msg, parse_mode="Markdown")
            await asyncio.sleep(1.0)
            
            kb = InlineKeyboardBuilder()
            kb.row(InlineKeyboardButton(text="▶️ YouTube Channel", url=YOUTUBE_LINK))
            kb.row(InlineKeyboardButton(text="📸 Instagram Profile", url=INSTAGRAM_LINK))
            
            await message.answer(
                f"**Hello, {message.from_user.first_name}!** 👋\n\n"
                f"I noticed you started without an access link.\n\n"
                f"**📍 To Access Premium Blueprints:**\n"
                f"➤ Visit my **YouTube** or **Instagram**\n"
                f"➤ Open any recent post or video\n"
                f"➤ Look for the **PINNED COMMENT** at the top\n"
                f"➤ Click the exclusive link inside\n\n"
                f"💎 **Each pinned comment contains direct access to premium guides, tools, and strategies.**\n\n"
                f"🎯 Start exploring below:",
                reply_markup=kb.as_markup()
            )
            await asyncio.sleep(0.3)
            await message.answer("💼 **Ready when you are.**", reply_markup=get_main_keyboard())
        else:
            # Returning user without link
            kb = InlineKeyboardBuilder()
            kb.row(InlineKeyboardButton(text="▶️ YouTube", url=YOUTUBE_LINK))
            kb.row(InlineKeyboardButton(text="📸 Instagram", url=INSTAGRAM_LINK))
            
            await message.answer(
                f"**Welcome Back, {message.from_user.first_name}.** ✅\n\n"
                f"To access new blueprints and premium content, check the **PINNED COMMENTS** on my latest posts.\n\n"
                f"🎯 Every pinned comment has exclusive access links.",
                reply_markup=kb.as_markup()
            )
            await asyncio.sleep(0.3)
            await message.answer("💼 **Your Dashboard:**", reply_markup=get_main_keyboard())

# ==========================================
# 🔐 SOCIAL MEDIA TRACKING CALLBACKS
# ==========================================

@dp.callback_query(F.data.startswith("social_"))
async def track_social_click(callback: types.CallbackQuery):
    """Track when user clicks social media buttons and open URL"""
    try:
        platform = callback.data.split("_")[1]  # 'ig' or 'yt'
        user_id = callback.from_user.id
        
        # Initialize tracking if not exists
        if user_id not in user_social_clicks:
            user_social_clicks[user_id] = {'ig': False, 'yt': False}
        
        # Mark this platform as clicked
        user_social_clicks[user_id][platform] = True
        
        # Determine URL and platform name
        if platform == "yt":
            url = YOUTUBE_LINK
            platform_name = "YouTube"
            icon = "▶️"
            action = "Subscribe"
        else:
            url = INSTAGRAM_LINK
            platform_name = "Instagram"
            icon = "📸"
            action = "Follow"
        
        # Premium animation sequence
        await callback.answer(f"🎯 Connecting to {platform_name}...", show_alert=False)
        
        # Multi-step premium animation
        loading = await callback.message.answer(f"⚡ **Initializing Connection...**")
        await asyncio.sleep(0.12)
        await loading.edit_text(f"🔍 **Scanning {platform_name} Network...**")
        await asyncio.sleep(0.12)
        await loading.edit_text(f"🌐 **[░░░░░] 20% - Establishing Link...**")
        await asyncio.sleep(0.12)
        await loading.edit_text(f"🔗 **[██░░░] 40% - Authenticating...**")
        await asyncio.sleep(0.12)
        await loading.edit_text(f"✅ **[████░] 80% - Connection Secure...**")
        await asyncio.sleep(0.12)
        await loading.edit_text(f"🎉 **[█████] 100% - Ready to Redirect!**")
        await asyncio.sleep(0.15)
        await loading.delete()
        
        # Send URL with premium message
        await callback.message.answer(
            f"{icon} **{platform_name} Channel**\n\n"
            f"Click below to {action.lower()} on {platform_name}:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"{action} on {platform_name}", url=url)]
            ])
        )
        
        # Send progress reminder after delay
        await asyncio.sleep(1.5)
        await callback.message.answer(
            f"✅ **Step 1 Complete**\n\n"
            f"After you {action.lower()} on {platform_name}:\n"
            f"➤ Return here\n"
            f"➤ Join the Telegram Channel\n"
            f"➤ Click 'I HAVE COMPLETED'\n\n"
            f"💎 Almost there!"
        )
        
        logger.info(f"User {user_id} clicked {platform_name} button")
    except Exception as e:
        logger.error(f"Error in track_social_click: {e}")
        await callback.answer("✅ Opened!", show_alert=False)

@dp.callback_query(F.data.startswith("verify_"))
async def verify_all_requirements(callback: types.CallbackQuery):
    """Verify user completed all requirements: clicked social media + joined Telegram"""
    user_id = callback.from_user.id
    
    # Ban protection
    if col_banned.find_one({"user_id": str(user_id)}):
        await callback.answer("❌ Access Denied", show_alert=True)
        return
    
    try:
        parts = callback.data.split("_")
        source = parts[1] if len(parts) > 1 else "Unknown"
        raw_arg = parts[2] if len(parts) > 2 else "none"
        
        # Auto-mark social click based on source (user came from that platform, so they're already there)
        if user_id not in user_social_clicks:
            user_social_clicks[user_id] = {'ig': False, 'yt': False}
        
        # If user came from Instagram, auto-track that (they must have IG already)
        if source == "Instagram":
            user_social_clicks[user_id]['ig'] = True
        # If user came from YouTube, auto-track that (they must have YT already)
        elif source == "YouTube":
            user_social_clicks[user_id]['yt'] = True
        
        # Check social media clicks
        social_clicks = user_social_clicks.get(user_id, {'ig': False, 'yt': False})
        clicked_ig = social_clicks.get('ig', False)
        clicked_yt = social_clicks.get('yt', False)
        
        # Check Telegram membership
        is_telegram_member = await is_member(user_id)
        
        # Validation logic based on source
        if source == "Instagram":
            # From Instagram: Must click YouTube button + Join Telegram
            if not clicked_yt:
                await callback.answer(
                    "📺 YOUTUBE REQUIRED\n\n"
                    "✨ Please subscribe to YouTube\n"
                    "Tap the YouTube button above",
                    show_alert=True
                )
                return
        elif source == "YouTube":
            # From YouTube: Must click Instagram button + Join Telegram
            if not clicked_ig:
                await callback.answer(
                    "📸 INSTAGRAM REQUIRED\n\n"
                    "✨ Please follow on Instagram\n"
                    "Tap the Instagram button above",
                    show_alert=True
                )
                return
        else:
            # Unknown source - shouldn't reach here due to early return
            await callback.answer(
                "💎 PREMIUM ACCESS\n\n"
                "Use link from pinned comments",
                show_alert=True
            )
            return
        
        if not is_telegram_member:
            # User didn't join Telegram
            platform_name = "YouTube" if source == "Instagram" else "Instagram"
            
            await callback.answer(
                "💎 ONE MORE STEP\n\n"
                f"✅ {platform_name} complete!\n\n"
                f"🚀 Please join Telegram Vault\n"
                f"Then tap 'I HAVE COMPLETED'",
                show_alert=True
            )
            return
        
        # ALL REQUIREMENTS MET! ✅
        # Premium verification animation sequence
        loading = await callback.message.answer("⚡ **Initiating Verification...**")
        await asyncio.sleep(0.1)
        await loading.edit_text("🔍 **[░░░░░] 20% - Scanning Requirements...**")
        await asyncio.sleep(0.1)
        
        # Show appropriate platform verification based on source
        platform_verified = "Instagram" if source == "YouTube" else "YouTube"
        await loading.edit_text(f"✨ **[██░░░] 40% - {platform_verified} Verified!**")
        await asyncio.sleep(0.1)
        await loading.edit_text("💎 **[███░░] 60% - Telegram Verified!**")
        await asyncio.sleep(0.1)
        await loading.edit_text("🎯 **[████░] 80% - Processing Access...**")
        await asyncio.sleep(0.1)
        await loading.edit_text("🌟 **[█████] 100% - VAULT UNLOCKED!**")
        await asyncio.sleep(0.4)
        await loading.edit_text("🎉 **Access Granted!**")
        await asyncio.sleep(0.5)
        
        # Delete verification message (stored earlier)
        if user_id in user_social_clicks and 'verify_msg_id' in user_social_clicks[user_id]:
            try:
                await bot.delete_message(chat_id=callback.message.chat.id, message_id=user_social_clicks[user_id]['verify_msg_id'])
            except:
                pass
        
        # Delete original callback message if different
        try:
            await callback.message.delete()
        except:
            pass
        
        try:
            await loading.delete()
        except:
            pass
        
        # Premium welcome animation
        welcome_msg = await callback.message.answer("🎯 **Preparing Welcome...**")
        await asyncio.sleep(0.1)
        await welcome_msg.edit_text("⚡ **[░░░░░] 20% - Creating Profile...**")
        await asyncio.sleep(0.1)
        await welcome_msg.edit_text("💎 **[██░░░] 40% - Unlocking Vault...**")
        await asyncio.sleep(0.1)
        await welcome_msg.edit_text("🌟 **[████░] 80% - Loading Resources...**")
        await asyncio.sleep(0.1)
        await welcome_msg.edit_text("🎉 **[█████] 100% - Welcome Ready!**")
        await asyncio.sleep(0.15)
        await welcome_msg.delete()
        
        # Premium success message
        await callback.message.answer(
            f"🎉 **Welcome to MSANode Vault, {callback.from_user.first_name}!**\n\n"
            f"✅ **Verification Complete**\n\n"
            f"You now have full access to:\n"
            f"💎 Premium automation guides\n"
            f"💎 Exclusive strategies & blueprints\n"
            f"💎 Advanced tools & resources\n"
            f"💎 Priority support\n\n"
            f"🚀 **Let's get started with your journey!**",
            reply_markup=get_main_keyboard()
        )
        
        # Clear social clicks tracking (no longer needed)
        if user_id in user_social_clicks:
            del user_social_clicks[user_id]
        
        # Deliver content if user came with a code
        if raw_arg != "none":
            src = "Instagram" if raw_arg.startswith("ig_") else "YouTube"
            await deliver_content(callback.message, raw_arg.replace("ig_", "").replace("yt_", ""), src, "NEW")
        else:
            await callback.message.answer("✨ **Welcome to MSANode Vault!** Check pinned comments on social media for exclusive access links.")
    
    except TelegramForbiddenError:
        logger.warning(f"User {user_id} blocked the bot")
    except Exception as e:
        logger.error(f"Error in verify_all_requirements: {e}")
        try:
            await callback.answer("⚠️ An error occurred. Please try again.", show_alert=True)
        except:
            pass

@dp.callback_query(F.data.startswith("check_"))
async def check_join(callback: types.CallbackQuery):
    """Legacy callback for backwards compatibility"""
    # Redirect to new verify system
    user_id = callback.from_user.id
    raw_arg = callback.data.split("_", 1)[1] if "_" in callback.data else "none"
    
    # Initialize social clicks as already done (legacy users)
    if user_id not in user_social_clicks:
        user_social_clicks[user_id] = {'ig': True, 'yt': True}  # Assume completed for old links
    
    # Call new verify function
    callback.data = f"verify_Unknown_{raw_arg}"
    await verify_all_requirements(callback)

# ==========================================
# 📜 TERMS & CONDITIONS ACCEPTANCE HANDLERS
# ==========================================

@dp.message(F.text == "✅ I Accept the Terms & Conditions")
async def handle_terms_accept(message: types.Message):
    """Handle user acceptance of terms and conditions"""
    user_id = message.from_user.id
    
    # Check if already accepted
    if has_accepted_terms(user_id):
        await message.answer(
            "✅ **You have already accepted the Terms & Conditions.**\n\n"
            "You have full access to all bot features!",
            reply_markup=get_main_keyboard()
        )
        return
    
    # Record acceptance in database
    try:
        now = datetime.now(IST)
        now_str = now.strftime("%d-%m-%Y %I:%M %p")
        
        # Insert into terms collection
        col_terms.insert_one({
            "user_id": str(user_id),
            "accepted": True,
            "accepted_at": now,
            "accepted_at_str": now_str,
            "first_name": message.from_user.first_name,
            "username": f"@{message.from_user.username}" if message.from_user.username else "None"
        })
        
        # Update user document
        col_users.update_one(
            {"user_id": str(user_id)},
            {"$set": {"terms_accepted": True, "terms_accepted_at": now_str}}
        )
        
        # Show acceptance animation
        loading = await message.answer("⚡ **Processing Your Acceptance...**")
        await asyncio.sleep(0.15)
        await loading.edit_text("📝 **[░░░░░] 20% - Recording Decision...**")
        await asyncio.sleep(0.15)
        await loading.edit_text("🔐 **[██░░░] 40% - Verifying Agreement...**")
        await asyncio.sleep(0.15)
        await loading.edit_text("✅ **[████░] 80% - Activating Access...**")
        await asyncio.sleep(0.15)
        await loading.edit_text("🎉 **[█████] 100% - Complete!**")
        await asyncio.sleep(0.3)
        await loading.delete()
        
        # Welcome message with full access
        await message.answer(
            f"🎉 **Thank you, {message.from_user.first_name}!**\n\n"
            f"✅ **Terms & Conditions Accepted**\n\n"
            f"You now have full access to all bot features:\n"
            f"💎 Premium content downloads\n"
            f"💎 Review system\n"
            f"💎 Customer support\n"
            f"💎 Search & vault features\n"
            f"💎 All bot commands\n\n"
            f"🚀 **Welcome to MSANode!** Use the menu below to get started.",
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown"
        )
        
        # Log acceptance
        user_doc = col_users.find_one({"user_id": str(user_id)})
        msa_id = user_doc.get("msa_id", "UNKNOWN") if user_doc else "UNKNOWN"
        logger.info(f"✅ User {msa_id} ({user_id}) accepted Terms & Conditions")
        
    except Exception as e:
        logger.error(f"Error recording terms acceptance: {e}")
        await message.answer(
            "⚠️ **An error occurred while processing your acceptance.**\n\n"
            "Please try again by clicking the Accept button.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✅ I Accept the Terms & Conditions")],
                    [KeyboardButton(text="❌ I Reject")]
                ],
                resize_keyboard=True,
                one_time_keyboard=False
            )
        )

@dp.message(F.text == "❌ I Reject")
async def handle_terms_reject(message: types.Message):
    """Handle user rejection of terms and conditions"""
    user_id = message.from_user.id
    
    # Keep showing the same keyboard - don't let them proceed
    terms_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ I Accept the Terms & Conditions")],
            [KeyboardButton(text="❌ I Reject")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    
    await message.answer(
        f"❌ **Terms & Conditions Rejected**\n\n"
        f"**{message.from_user.first_name}, you must accept our Terms & Conditions to use this bot.**\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ **Without acceptance:**\n"
        f"• You cannot access any bot features\n"
        f"• All commands will be blocked\n"
        f"• No content downloads available\n"
        f"• No support or reviews access\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 **To continue:**\n"
        f"➤ Read the terms carefully\n"
        f"➤ Click '✅ I Accept' button below\n\n"
        f"⏸️ **You can't proceed until you accept the terms.**",
        reply_markup=terms_kb,
        parse_mode="Markdown"
    )
    
    logger.info(f"❌ User {user_id} rejected Terms & Conditions")

async def deliver_content(message: types.Message, payload: str, source: str, u_status: str = "UNKNOWN"):
    u_id = str(message.chat.id)
    name = message.chat.first_name or "Operative"
    u_name = f"@{message.chat.username}" if message.chat.username else "None"
    
    # Check if download feature is banned for this user
    user_data = col_users.find_one({"user_id": u_id})
    if is_feature_banned(user_data, 'downloads'):
        await send_feature_ban_message(message, 'downloads', user_data)
        return
    
    data = await get_content(payload)
    
    if not data: 
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🔴 YouTube", url=YOUTUBE_LINK))
        kb.row(InlineKeyboardButton(text="📸 Instagram", url=INSTAGRAM_LINK))
        await message.answer(
            f"❌ **Link Broken or Invalid!**\n\n"
            f"The link you used appears to be damaged or expired.\n\n"
            f"**🔍 How to fix:**\n"
            f"➤ Go to my **YouTube** or **Instagram**\n"
            f"➤ Find the **PINNED COMMENT** on latest post\n"
            f"➤ Click the fresh link from there and try again\n\n"
            f"✨ The pinned comment always has the working link!",
            reply_markup=kb.as_markup()
        )
        return
    
    # --- INTELLIGENCE DOSSIER (ONE-TIME REPORT - NO DUPLICATES) ---
    # Only report NEW users who haven't been reported yet
    doc = col_users.find_one({"user_id": u_id})
    if doc and not doc.get("has_reported", False) and u_status == "NEW":
        rep_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
        msa_id = doc.get("msa_id", "UNKNOWN")
        dossier = (
            f"👤 **NEW RECRUIT CAPTURED**\n"
            f"**MSA ID:** `{msa_id}`\n"
            f"**Name:** {name}\n"
            f"**User:** {u_name}\n"
            f"**Telegram ID:** `{u_id}`\n"
            f"**Source:** {source}\n"
            f"**M-Code:** `{payload}`\n"
            f"**PDF:** {data['main_link']}\n"
            f"**Time:** {rep_time}"
        )
        await send_admin_report(dossier)
        # Mark as reported to prevent duplicate reports
        col_users.update_one({"user_id": u_id}, {"$set": {"has_reported": True}})

    # 1. BLUEPRINT DELIVERY
    await message.answer(f"**Transmission Successful.** 🔓\n\nBlueprint ready:\n{data['main_link']}")

    # 2. AFFILIATE (1.5s DELAY)
    if data['aff_link'] and len(data['aff_link']) > 5:
        await asyncio.sleep(1.5)
        kb_aff = InlineKeyboardBuilder().button(text="� ACCESS PREMIUM RESOURCE", url=data['aff_link']).as_markup()
        await message.answer(f"💼 **Exclusive Tool Recommendation**\n\n{data['aff_text']}", reply_markup=kb_aff)

    # 3. CROSS-SYNC ENGINE (1.5s DELAY + ALPHA TITLES)
    await asyncio.sleep(1.5)
    title = random.choice(ALPHA_TITLES)
    
    if source == "YouTube":
        reel = list(col_reels.aggregate([{"$sample": {"size": 1}}]))
        kb = InlineKeyboardBuilder()
        msg = f"⚡ **{name}, Your Strategy Continues on Instagram**\n\nThis guide is just the foundation. For daily automation insights and advanced tactics, follow my Instagram:"
        if reel:
            msg += f"\n\n{title}\n{reel[0].get('desc', 'Premium Content Available')}"
            kb.row(InlineKeyboardButton(text="📸 VIEW PREMIUM CONTENT", url=reel[0]['link']))
        else: kb.row(InlineKeyboardButton(text="📸 FOLLOW FOR DAILY INSIGHTS", url=INSTAGRAM_LINK))
        kb.row(InlineKeyboardButton(text="▶️ SUBSCRIBE ON YOUTUBE", url=YOUTUBE_LINK))
        await message.answer(msg, reply_markup=kb.as_markup())
    else:
        video = list(col_viral.aggregate([{"$sample": {"size": 1}}]))
        kb = InlineKeyboardBuilder()
        msg = f"🔥 **{name}, Dive Deeper on YouTube**\n\nThis Instagram guide is your starting point. For comprehensive strategies and in-depth breakdowns, my YouTube channel delivers the full blueprint:"
        if video:
            msg += f"\n\n{title}\n{video[0].get('desc', 'Complete Strategy Available')}"
            kb.row(InlineKeyboardButton(text="▶️ WATCH FULL BREAKDOWN", url=video[0]['link']))
        else: kb.row(InlineKeyboardButton(text="▶️ SUBSCRIBE FOR IN-DEPTH CONTENT", url=YOUTUBE_LINK))
        kb.row(InlineKeyboardButton(text="📸 FOLLOW ON INSTAGRAM", url=INSTAGRAM_LINK))
        await message.answer(msg, reply_markup=kb.as_markup())

# ==========================================
# USER HISTORY HANDLERS
# ==========================================
@dp.message(F.text == "📜 MY REVIEWS")
async def show_my_reviews(message: types.Message):
    """Show user's review history"""
    user_id = str(message.from_user.id)
    is_banned, ban_record, ban_msg = is_user_completely_banned(int(user_id))
    if is_banned:
        try:
            await message.answer(ban_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    reviews = list(col_reviews.find({"user_id": user_id}).sort("timestamp", -1))
    
    if not reviews:
        await message.answer(
            "📜 **MY REVIEW HISTORY**\n\n"
            "❌ You haven't submitted any reviews yet.\n\n"
            "💡 **Tip:** Use the ⭐ REVIEW button to share your experience!",
            parse_mode="Markdown"
        )
        return
    
    msg = "📜 **MY REVIEW HISTORY**\n" + "="*35 + "\n\n"
    msg += f"📊 **Total Reviews:** {len(reviews)}\n\n"
    
    for idx, review in enumerate(reviews[:10], 1):
        rating = review.get("rating", 0)
        stars = "⭐" * rating
        review_text = review.get("review", "No text")[:100]
        timestamp = review.get("timestamp")
        
        if timestamp:
            if isinstance(timestamp, datetime):
                if timestamp.tzinfo is None:
                    timestamp = IST.localize(timestamp)
            else:
                try:
                    timestamp = datetime.fromisoformat(str(timestamp))
                    if timestamp.tzinfo is None:
                        timestamp = IST.localize(timestamp)
                except:
                    timestamp = None
            time_str = timestamp.strftime("%d %b %Y, %I:%M %p") if timestamp else "Unknown"
        else:
            time_str = "Unknown"
        
        msg += f"**#{idx}** {stars} ({rating}/5)\n"
        msg += f"📅 {time_str}\n"
        msg += f"💬 \"{review_text}\"\n"
        msg += "-"*35 + "\n\n"
    
    if len(reviews) > 10:
        msg += f"_...and {len(reviews) - 10} more reviews_\n"
    
    await message.answer(msg, parse_mode="Markdown")

@dp.message(F.text == "🎫 MY TICKETS")
async def show_my_tickets(message: types.Message):
    """Show user's support ticket history"""
    user_id = str(message.from_user.id)
    is_banned, ban_record, ban_msg = is_user_completely_banned(int(user_id))
    if is_banned:
        try:
            await message.answer(ban_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    user_doc = col_users.find_one({"user_id": user_id})
    if not user_doc:
        await message.answer(
            "🎫 **MY SUPPORT TICKETS**\n\n"
            "❌ No ticket history found.\n\n"
            "💡 **Tip:** Use 💬 CUSTOMER SUPPORT to get help!",
            parse_mode="Markdown"
        )
        return
    
    support_history = user_doc.get("support_history", [])
    current_ticket = user_support_pending.get(user_id)
    
    msg = "🎫 **MY SUPPORT TICKETS**\n" + "="*35 + "\n\n"
    
    if current_ticket and current_ticket.get('status') == 'pending':
        ticket_msg = current_ticket.get('message', '')[:80]
        ticket_time = current_ticket.get('timestamp', time.time())
        elapsed = int(time.time() - ticket_time)
        hours = elapsed // 3600
        minutes = (elapsed % 3600) // 60
        msg += f"🟡 **CURRENT TICKET (Pending)**\n"
        msg += f"💬 \"{ticket_msg}\"\n"
        msg += f"⏳ Waiting: {hours}h {minutes}m\n"
        msg += "-"*35 + "\n\n"
    
    if support_history:
        msg += f"📊 **Past Tickets:** {len(support_history)}\n\n"
        for idx, ticket in enumerate(support_history[-10:], 1):
            issue = ticket.get("issue", "Unknown")[:60]
            status = ticket.get("status", "unknown")
            status_icon = "✅" if status == "resolved" else ("💬" if status == "responded" else "❓")
            timestamp = ticket.get("timestamp", "Unknown")
            msg += f"**#{idx}** {status_icon} {status.title()}\n"
            msg += f"💬 \"{issue}\"\n"
            msg += f"📅 {timestamp}\n"
            msg += "-"*35 + "\n\n"
        
        if len(support_history) > 10:
            msg += f"_...and {len(support_history) - 10} more tickets_\n"
    else:
        msg += "❌ No past tickets found.\n\n"
    
    msg += "\n💡 Need help? Use 💬 CUSTOMER SUPPORT!"
    await message.answer(msg, parse_mode="Markdown")

@dp.message(F.text == "📊 MY STATS")
async def show_my_stats(message: types.Message):
    """Show user's comprehensive statistics"""
    user_id = str(message.from_user.id)
    is_banned, ban_record, ban_msg = is_user_completely_banned(int(user_id))
    if is_banned:
        try:
            await message.answer(ban_msg, parse_mode="Markdown")
        except:
            pass
        return
    
    user_doc = col_users.find_one({"user_id": user_id})
    if not user_doc:
        await message.answer("❌ Profile not found. Use /start first.", parse_mode="Markdown")
        return
    
    msa_id = user_doc.get("msa_id", "UNKNOWN")
    username = user_doc.get("username", "No Username")
    joined_date = user_doc.get("joined_date", "Unknown")
    
    review_count = col_reviews.count_documents({"user_id": user_id})
    reviews = list(col_reviews.find({"user_id": user_id}))
    if reviews:
        ratings = [r.get("rating", 0) for r in reviews if r.get("rating")]
        avg_rating = sum(ratings) / len(ratings) if ratings else 0
    else:
        avg_rating = 0
    
    support_history = user_doc.get("support_history", [])
    ticket_count = len(support_history)
    resolved_tickets = len([t for t in support_history if t.get("status") == "resolved"])
    
    ban_history_count = col_ban_history.count_documents({"user_id": user_id})
    
    msg = "📊 **MY STATISTICS**\n" + "="*35 + "\n\n"
    msg += f"👤 **PROFILE INFO**\n"
    msg += f"🆔 MSA ID: `{msa_id}`\n"
    msg += f"👤 Username: @{username}\n"
    msg += f"📅 Member Since: {joined_date}\n\n"
    
    msg += f"⭐ **REVIEW ACTIVITY**\n"
    msg += f"📝 Total Reviews: {review_count}\n"
    if review_count > 0:
        msg += f"⭐ Average Rating: {avg_rating:.1f}/5.0\n"
        msg += f"📈 Rating: {'★' * int(avg_rating)}{'☆' * (5 - int(avg_rating))}\n"
    msg += "\n"
    
    msg += f"🎫 **SUPPORT ACTIVITY**\n"
    msg += f"💬 Total Tickets: {ticket_count}\n"
    if ticket_count > 0:
        resolution_rate = (resolved_tickets / ticket_count * 100) if ticket_count > 0 else 0
        msg += f"✅ Resolved: {resolved_tickets}\n"
        msg += f"📊 Resolution Rate: {resolution_rate:.0f}%\n"
    msg += "\n"
    
    msg += "🛡️ **ACCOUNT STATUS**\n"
    if ban_history_count > 0:
        msg += f"⚠️ Warnings: {ban_history_count}\n"
    else:
        msg += "✅ Clean Record\n"
    
    msg += "\n" + "="*35 + "\n💎 Keep up the great activity!"
    await message.answer(msg, parse_mode="Markdown")

# ==========================================
# �️ CATCH-ALL HANDLER (MUST BE LAST)
# ==========================================
@dp.message()
async def handle_unhandled_messages(message: types.Message):
    """Catch all unhandled messages to prevent 'not handled' errors"""
    user_id = str(message.from_user.id)
    
    # Ban check
    if col_banned.find_one({"user_id": user_id}):
        return
    
    # Premium guidance animation
    guiding = await message.answer("🎯 **Analyzing Input...**")
    await asyncio.sleep(0.1)
    await guiding.edit_text("💡 **[██░░░] 40% - Checking Options...**")
    await asyncio.sleep(0.1)
    await guiding.edit_text("🌟 **[████░] 80% - Preparing Menu...**")
    await asyncio.sleep(0.1)
    await guiding.edit_text("✨ **[█████] 100% - Menu Ready!**")
    await asyncio.sleep(0.15)
    await guiding.delete()
    
    # Premium menu redirect
    try:
        await message.answer(
            "💎 **Use the premium menu below**\n\n"
            "📊 **DASHBOARD** - View your profile & status\n"
            "⭐ **REVIEW** - Share your feedback\n"
            "💬 **CUSTOMER SUPPORT** - Get help\n"
            "❓ **FAQ / HELP** - Common questions\n"
            "📚 **GUIDE / HOW TO USE** - Learn how to use\n"
            "📜 **RULES & REGULATIONS** - View bot rules\n\n"
            "🚀 **Navigate using the buttons for the best experience!**",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in catch-all handler: {e}")

# ==========================================
# 🚀 NUCLEAR SHIELD
# ==========================================
async def main():
    try: await bot.delete_webhook(drop_pending_updates=True)
    except: pass
    
    # 🏢 ENTERPRISE: Start health monitoring in background
    asyncio.create_task(enterprise_health_check())
    print("[OK] ENTERPRISE HEALTH MONITORING STARTED")
    
    print(f"✅ MSANODE GATEWAY ONLINE - ENTERPRISE MODE (LAKHS-READY)")
    # Configure polling with proper timeout settings for Windows
    await dp.start_polling(
        bot,
        skip_updates=True,
        timeout=20,  # Polling timeout in seconds
        relax=0.1,   # Delay between iterations
        fast=True    # Use fast polling mode
    )

if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    loop_count = 0
    
    while True:
        loop_count += 1
        try:
            # Use asyncio.run() which properly manages the event loop lifecycle
            asyncio.run(main())
            
        except TelegramConflictError:
            print("💀 GHOST DETECTED! Waiting 20s...")
            time.sleep(20)
        except KeyboardInterrupt:
            print("🛑 Bot stopped by user")
            break
        except Exception as e:
            print(f"⚠️ Error (attempt {loop_count}): {e}")
            time.sleep(5)  # Wait before retry
 
