import asyncio
import functools
import logging
import os
import pymongo
import random
import re
import string
import time
import traceback
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from aiohttp import web as aiohttp_web
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter, TelegramNetworkError

# Load environment variables
load_dotenv()

# ==========================================
# ⚡ CONFIGURATION  — all values from env vars
# ==========================================
BOT_TOKEN = os.getenv("BOT_8_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "MSANodeDB")  # MongoDB database name
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))           # Vault channel numeric ID
CHANNEL_LINK = os.getenv("CHANNEL_LINK")               # Telegram vault invite link
YOUTUBE_LINK = os.getenv("YOUTUBE_LINK", "")
INSTAGRAM_LINK = os.getenv("INSTAGRAM_LINK", "")
REVIEW_LOG_CHANNEL = int(os.getenv("REVIEW_LOG_CHANNEL", 0))   # Support ticket log channel
# Fallback link shown to users when no content link is stored in DB
BOT_FALLBACK_LINK = os.getenv("BOT_FALLBACK_LINK", "https://t.me/msanodebot")
# Render web-service health check port (Render sets PORT automatically)
PORT = int(os.getenv("PORT", 8080))

# ==========================================
# ⚠️ STARTUP VALIDATION - Fail fast
# ==========================================
_REQUIRED_ENV = {
    "BOT_8_TOKEN": BOT_TOKEN,
    "MONGO_URI": MONGO_URI,
    "OWNER_ID": os.getenv("OWNER_ID"),
    "CHANNEL_ID": os.getenv("CHANNEL_ID"),
    "CHANNEL_LINK": CHANNEL_LINK,
}
_missing = [k for k, v in _REQUIRED_ENV.items() if not v]
if _missing:
    print(f"ERROR: Missing required env vars: {', '.join(_missing)}")
    sys.exit(1)

# ==========================================
# 🕐 TIMEZONE CONFIGURATION
# ==========================================
# Set your timezone here — used for 8:40 AM/PM daily reports
REPORT_TIMEZONE = os.getenv("REPORT_TIMEZONE", "Asia/Kolkata")  # Change via env var
try:
    TZ = ZoneInfo(REPORT_TIMEZONE)
except Exception:
    TZ = ZoneInfo("Asia/Kolkata")
    logging.warning(f"Invalid REPORT_TIMEZONE '{REPORT_TIMEZONE}', falling back to Asia/Kolkata")

def now_local() -> datetime:
    """Return current time as a naive datetime in the configured local timezone."""
    return datetime.now(TZ).replace(tzinfo=None)

# Daily report times (24h format)
REPORT_HOUR_AM = 8   # 8 AM
REPORT_MIN_AM = 40   # :40
REPORT_HOUR_PM = 20  # 8 PM
REPORT_MIN_PM = 40   # :40

# ==========================================
# ⏱️ TIMING CONSTANTS (Animation speeds)
# ==========================================
ANIM_FAST = 0.2      # Fast animations
ANIM_MEDIUM = 0.3    # Medium animations
ANIM_SLOW = 0.5      # Slow animations
ANIM_PAUSE = 0.4     # Pause between sections
ANIM_DELAY = 1.0     # Long delay before delete

# ==========================================
# 🛡️ ANTI-SPAM SYSTEM
# ==========================================
# Track users currently processing commands (prevents spam)
user_processing: dict[int, str] = {}  # {user_id: "command_name"}

# Rate limiting: Track last command time per user (prevents flood bans)
user_last_command: dict[int, float] = {}  # {user_id: timestamp}
COMMAND_COOLDOWN = 2.0  # seconds between commands (prevents Telegram FloodWait)

# ==========================================
# 🧊 PROGRESSIVE AUTO-FREEZE SYSTEM
# ==========================================
# Freeze durations per offense level (seconds)
_FREEZE_LEVELS  = [30, 90, 300, 900]   # 30s → 1m30s → 5m → 15m
_FREEZE_WINDOW  = 4.0   # sliding window (seconds) — lenient for slow internet
_FREEZE_TRIGGER = 5     # rapid taps within window needed to trip first freeze
_FREEZE_DECAY   = 600   # seconds of clean behavior before offense count resets

# Per-user state: {user_id: {offense, frozen_until, taps, window_start}}
_freeze_tracker: dict[int, dict] = {}

# ==========================================
# 🛠 SYSTEM SETUP
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# Suppress noisy pymongo background pool/network warnings (auto-recovered by pymongo itself)
logging.getLogger("pymongo.client").setLevel(logging.CRITICAL)
logging.getLogger("pymongo.pool").setLevel(logging.CRITICAL)
logging.getLogger("pymongo.topology").setLevel(logging.CRITICAL)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ==========================================
# 🖥️ BOT 8 LIVE TERMINAL MIDDLEWARE
# Logs every user interaction to MongoDB — visible in Bot 10 Terminal from Render
# ==========================================
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from typing import Callable, Dict, Any, Awaitable

class Bot8TerminalMiddleware(BaseMiddleware):
    """Intercepts every message and logs it to shared MongoDB live_terminal_logs collection."""
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # Only log Message events with text
        msg = getattr(event, 'message', event) if not hasattr(event, 'text') else event
        user = getattr(msg, 'from_user', None)
        text = getattr(msg, 'text', None) or getattr(msg, 'caption', None) or "[media]"
        if user and user.id:
            try:
                # Trim long messages
                display_text = text[:60] + "..." if len(text) > 60 else text
                # We call log_to_terminal only after DB is ready (guarded by try/except inside)
                log_to_terminal(
                    action_type=f"MSG from {user.full_name or user.id}",
                    user_id=user.id,
                    details=display_text
                )
            except Exception:
                pass
        return await handler(event, data)
# ==========================================
health_stats = {
    "errors_caught": 0,
    "auto_healed": 0,
    "owner_notified": 0,
    "last_error": None,
    "last_error_msg": "",
    "bot_start_time": datetime.now(TZ),
    "db_reconnects": 0,
    "reports_sent": 0,
}

# ==========================================
# 📊 DATABASE CONNECTION  (Enterprise Pool)
# ==========================================
try:
    client = pymongo.MongoClient(
        MONGO_URI,
        maxPoolSize=50,          # Up to 50 concurrent connections
        minPoolSize=5,           # Keep 5 always alive
        maxIdleTimeMS=30000,     # Close idle connections after 30 s
        serverSelectionTimeoutMS=5000,  # Fail fast on unavailable
        connectTimeoutMS=10000,
        socketTimeoutMS=30000,
        retryWrites=True,
        retryReads=True,
        w="majority",            # Write concern – durable
    )
    db = client[MONGO_DB_NAME]
    col_user_verification = db["user_verification"]
    col_msa_ids = db["msa_ids"]  # Collection for MSA+ ID tracking
    col_pdfs = db["bot9_pdfs"]   # Collection for PDF/Affiliate/YT data
    col_ig_content = db["bot9_ig_content"] # IG Content Collection
    col_support_tickets = db["support_tickets"]  # Collection for support ticket tracking
    col_banned_users = db["banned_users"]  # Collection for banned users (managed by Bot 10)
    col_suspended_features = db["suspended_features"]  # Collection for suspended features (managed by Bot 10)
    col_bot8_settings = db["bot8_settings"]  # Bot 8 global settings (Maintenance Mode)
    col_live_logs = db["live_terminal_logs"]  # Shared live logs for Bot 10 terminal (Render-safe)
    col_bot8_backups = db["bot8_backups"]     # Bot 8 auto-backups (12h, cloud-safe)
    col_broadcasts = db["bot10_broadcasts"]    # Broadcasts sent via Bot 10 (read-only here)
    logger.info("✅ MongoDB connected successfully")
    
    # ==========================================
    # 🔍 CREATE DATABASE INDEXES (Performance)
    # ==========================================
    try:
        col_user_verification.create_index("user_id", unique=True)
        col_msa_ids.create_index("user_id", unique=True)
        col_msa_ids.create_index("msa_number")
        col_pdfs.create_index("ig_start_code")
        col_pdfs.create_index("yt_start_code")
        col_pdfs.create_index("index")
        col_ig_content.create_index("cc_code")
        col_ig_content.create_index("start_code")
        col_support_tickets.create_index("user_id")
        col_support_tickets.create_index("status")
        # Enterprise extra indexes
        col_banned_users.create_index("user_id", unique=True)
        col_banned_users.create_index("ban_expires")  # TTL hint only
        col_support_tickets.create_index([("user_id", 1), ("status", 1)])
        col_support_tickets.create_index("created_at")
        col_support_tickets.create_index("resolved_at")
        db["bot10_user_tracking"].create_index("user_id", unique=True)
        db["bot8_state_persistence"].create_index("key", unique=True)
        col_bot8_backups.create_index([("backup_date", -1)])
        col_bot8_backups.create_index([("backup_type", 1)])
        logger.info("✅ Database indexes created/verified")
    except Exception as idx_error:
        logger.warning(f"⚠️ Index creation warning: {idx_error}")

except Exception as e:
    logger.error(f"❌ MongoDB connection failed: {e}")
    sys.exit(1)

# ==========================================
# 🖥️ LIVE TERMINAL LOGGER (shared with Bot 10)
# ==========================================
_BOT8_LOG_MAX = 100  # Keep last 100 bot8 logs in MongoDB

def log_to_terminal(action_type: str, user_id: int, details: str = ""):
    """Write a log entry to the shared live_terminal_logs collection so Bot 10 can display it live."""
    try:
        timestamp = now_local().strftime('%I:%M:%S %p')
        col_live_logs.insert_one({
            "timestamp": timestamp,
            "created_at": now_local(),
            "bot": "bot8",
            "action": action_type,
            "user_id": user_id,
            "details": details,
        })
        # Trim: keep newest _BOT8_LOG_MAX entries for bot8
        count = col_live_logs.count_documents({"bot": "bot8"})
        if count > _BOT8_LOG_MAX:
            oldest = list(col_live_logs.find({"bot": "bot8"}, {"_id": 1}).sort("created_at", 1).limit(count - _BOT8_LOG_MAX))
            if oldest:
                col_live_logs.delete_many({"_id": {"$in": [d["_id"] for d in oldest]}})
    except Exception:
        pass  # Never let logging crash the bot

# ==========================================
# 🔐 VERIFICATION FUNCTIONS
# ==========================================

# ==========================================
# USER SOURCE TRACKING (permanent first-source lock)
# ==========================================
def track_user_source(user_id: int, source: str, username: str, first_name: str, msa_id: str):
    """
    Record traffic source PERMANENTLY on first start only.
    - New user: inserts full record including source.
    - Returning user without source: adds source field only.
    - Returning user with source: only updates last_start and msa_id. Source is NEVER changed.
    """
    try:
        col = db["bot10_user_tracking"]
        existing = col.find_one({"user_id": user_id}, {"source": 1})
        if existing is None:
            # Brand new user — insert full record with source
            col.insert_one({
                "user_id": user_id,
                "source": source,
                "first_start": now_local(),
                "username": username,
                "first_name": first_name,
                "msa_id": msa_id,
                "last_start": now_local(),
            })
        elif "source" not in existing:
            # Existing user but source was never recorded — set it now (once only)
            col.update_one(
                {"user_id": user_id},
                {"$set": {
                    "source": source,
                    "first_start": now_local(),
                    "last_start": now_local(),
                    "msa_id": msa_id,
                }}
            )
        else:
            # Returning user WITH source — only update mutable fields, source stays
            col.update_one(
                {"user_id": user_id},
                {"$set": {"last_start": now_local(), "msa_id": msa_id}}
            )
    except Exception as e:
        logger.error(f"Warning: track_user_source failed: {e}")
async def check_channel_membership(user_id: int) -> bool:
    """Check if user is a member of the vault channel"""
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        return False

class SearchCodeStates(StatesGroup):
    waiting_for_code = State()
    # Context flags for search flow
    is_yt_flow = State() # If True, user came from YT link (force IG content)

class SupportStates(StatesGroup):
    waiting_for_issue = State()  # Waiting for user to describe their issue

class GuideStates(StatesGroup):
    viewing_bot8 = State()  # paginated Agent Guide

# ==========================================
# 🛡️ TICKET VALIDATION & FILTERS — ENTERPRISE GRADE
# ==========================================

# ---------------------------------------------------------------------------
# PROFANITY DATABASE
# Multi-layer: exact words, leetspeak/substitutions, phrase patterns, threats,
# scam patterns, and hate speech — all checked independently.
# ---------------------------------------------------------------------------

# Layer 1 — Core profanity (English, common)
_PROFANITY_CORE = {
    "fuck", "fucking", "fucked", "fucker", "fucks", "fuckin", "fuckoff",
    "shit", "shitting", "shitty", "bullshit", "horseshit", "shithead", "shitstorm",
    "bitch", "bitches", "bitchy", "son of a bitch",
    "asshole", "ass", "asses", "arsehole", "arse",
    "bastard", "bastards",
    "crap", "crappy",
    "piss", "pissed", "pissing",
    "dick", "dicks", "dickhead", "dickface",
    "cock", "cocks", "cocksucker", "cockhead",
    "pussy", "pussies",
    "slut", "sluts", "slutty",
    "whore", "whores", "whorish",
    "cunt", "cunts",
    "twat", "twats",
    "motherfucker", "motherfucking", "mf",
    "dipshit", "dumbass", "dumbfuck", "jackass", "numbnuts",
    "prick", "pricks",
    "douche", "douchebag", "douchebags",
    "wanker", "wankers", "tosser", "tossers",
    "skank", "skanky",
    "bimbo", "bimbos",
    "moron", "morons", "idiot", "idiots", "imbecile",
    "shitface", "shitbag", "cumshot", "cumface",
    "jerkoff", "jerk off", "jackoff", "jack off",
    "asshat", "asswipe", "assfuck",
    "fuckface", "fuckwit", "fuckhead", "fuckboy",
    "clusterfuck", "mindfuck",
    "dumbshit", "dumb shit", "holy shit",
}

# Layer 2 — Hate speech / slurs
_HATE_SPEECH = {
    "nigger", "nigga", "niggas", "niggers",
    "fag", "faggot", "faggots", "fags",
    "retard", "retarded", "retards",
    "spic", "spics", "wetback", "wetbacks",
    "chink", "chinks", "gook", "gooks",
    "kike", "kikes",
    "tranny", "trannies",
    "dyke", "dykes",
    "cracker", "crackers",
    "coon", "coons",
    "towelhead", "sandnigger",
    "zipperhead",
    "raghead",
    "beaner", "beaners",
    "gringo", "gringos",
    "honky", "honkies",
    "jap", "japs",
    "nazi", "nazis",
    "white trash",
}

# Layer 3 — Sexual content / adult material
_SEXUAL_CONTENT = {
    "porn", "porno", "pornography", "pornographic",
    "nude", "nudes", "nudity",
    "naked", "nakedpics",
    "xxx", "x-rated",
    "dildo", "dildos",
    "vibrator", "vibrators",
    "blowjob", "blow job", "handjob", "hand job",
    "cumming", "cum", "cumslut",
    "orgasm", "orgasms",
    "masturbate", "masturbation", "masturbating",
    "erection", "erotic",
    "hentai",
    "onlyfans", "only fans",
    "sexting", "sext",
    "stripclub", "strip club",
    "hooker", "hookers", "escort", "prostitute", "prostitution",
}

# Layer 4 — Threats and violence
_THREATS = {
    "kill yourself", "kys", "go kill yourself",
    "kill you", "i will kill", "gonna kill", "going to kill",
    "i will hurt", "gonna hurt", "going to hurt",
    "beat you up", "beat your ass",
    "shoot you", "stab you", "i will stab",
    "bomb", "bombing", "blow up", "blowing up",
    "die", "you should die", "hope you die",
    "suicide", "hang yourself", "slit your wrists",
    "murder", "murdering", "gonna murder",
    "attack you", "come for you", "find you",
    "i know where you live", "dox you", "doxxed",
    "ddos", "hack you", "hacking you",
}

# Layer 5 — Scam / phishing / spam
_SCAM_PATTERNS = {
    "free money", "free cash", "free bitcoin", "free crypto",
    "click here", "click this link", "click the link",
    "bit.ly", "tinyurl", "shorturl", "is.gd", "t.co/",
    "get rich", "get rich quick", "earn money fast",
    "make money fast", "make $", "make dollars",
    "investment opportunity", "guaranteed profit", "guaranteed returns",
    "binary options", "forex signals", "crypto signals",
    "send me money", "send me btc", "send bitcoin",
    "wire transfer", "western union", "moneygram",
    "nigerian prince", "lottery winner", "you've won",
    "claim your prize", "congratulations you won",
    "account suspended", "verify your account now",
    "your account will be deleted",
    "whatsapp me", "telegram me at", "contact me on",
    "100% safe", "100% legit", "zero risk",
    "passive income", "work from home earn",
    "mlm", "pyramid scheme", "ponzi",
    "cheap followers", "buy followers", "buy likes",
}

# Layer 6 — Leetspeak / character substitution variants
# These are checked after normalizing the text (see _normalize below)
_LEET_NORMALIZED = {
    # These are canonical forms; normalizer converts leet → plain before matching
    "fuck", "shit", "bitch", "ass", "dick", "cock", "cunt", "piss",
    "nigger", "nigga", "faggot", "retard",
    "porn", "sex", "nude",
}

# Combined master set for quick single-pass check
BAD_WORDS: set[str] = (
    _PROFANITY_CORE
    | _HATE_SPEECH
    | _SEXUAL_CONTENT
    | _THREATS
    | _SCAM_PATTERNS
)

# ---------------------------------------------------------------------------
# NORMALIZATION — converts leet/unicode tricks to plain ASCII before matching
# Handles: 4→a, 3→e, 1→i/l, 0→o, 5→s, 7→t, @→a, $→s, +→t, etc.
# ---------------------------------------------------------------------------
_LEET_MAP = str.maketrans({
    '4': 'a', '@': 'a', 'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a',
    '3': 'e', 'é': 'e', 'è': 'e', 'ë': 'e',
    '1': 'i', '!': 'i', 'í': 'i', 'ì': 'i', 'î': 'i',
    '0': 'o', 'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o',
    '5': 's', '$': 's',
    '7': 't', '+': 't',
    '6': 'g',
    '8': 'b',
    '9': 'p',
    'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u',
    'ç': 'c',
    'ñ': 'n',
    'ý': 'y',
    'ß': 'ss',
    # Zero-width / invisible chars
    '\u200b': '', '\u200c': '', '\u200d': '', '\ufeff': '',
    # Common obfuscation
    '|': 'i', '(': 'c',
})

def _normalize(text: str) -> str:
    """Normalize text: lowercase, leet decode, collapse repeated chars, strip spaces between letters."""
    t = text.lower()
    t = t.translate(_LEET_MAP)
    # Remove zero-width spaces and soft hyphens
    t = re.sub(r'[\u00ad\u200b-\u200d\ufeff]', '', t)
    # Collapse 3+ repeated same characters → 2 (catches "fuuuuck" → "fuuck" → still matches "fuck")
    t = re.sub(r'(.)\1{2,}', r'\1\1', t)
    # Remove spaces/dots/dashes used to obfuscate (f.u.c.k, f-u-c-k, f u c k)
    t_nospace = re.sub(r'(?<=[a-z])[\s.\-_*#]{1,3}(?=[a-z])', '', t)
    return t_nospace

# Maximum safe message length for Telegram
MAX_TICKET_LENGTH = 4000
MIN_TICKET_LENGTH = 20  # Raised: 10 chars is too little to be a real support message

# ---------------------------------------------------------------------------
# PROFANITY DETECTION — multi-layer
# ---------------------------------------------------------------------------
def contains_profanity(text: str) -> tuple[bool, list]:
    """
    Multi-layer profanity check:
    1. Direct match on original lowercase
    2. Leet/unicode-normalized match
    3. Phrase-level match (for multi-word patterns like "kill yourself")

    Returns (has_profanity: bool, found_terms: list)
    """
    original_lower = text.lower()
    normalized = _normalize(text)
    found = []

    for term in BAD_WORDS:
        term_lower = term.lower()
        term_norm  = _normalize(term)

        # Multi-word phrases: substring match (no word boundary needed)
        if " " in term_lower:
            if term_lower in original_lower or term_norm in normalized:
                found.append(term)
            continue

        # Single words: word-boundary match on both original and normalized
        pattern_orig = r'(?<![a-z])' + re.escape(term_lower) + r'(?![a-z])'
        pattern_norm = r'(?<![a-z])' + re.escape(term_norm)  + r'(?![a-z])'

        if re.search(pattern_orig, original_lower):
            found.append(term)
        elif re.search(pattern_norm, normalized):
            found.append(term)

    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped = []
    for w in found:
        if w not in seen:
            seen.add(w)
            deduped.append(w)

    return (len(deduped) > 0, deduped)


# ---------------------------------------------------------------------------
# SPAM / GIBBERISH DETECTION
# ---------------------------------------------------------------------------
def is_spam_or_gibberish(text: str) -> tuple[bool, str]:
    """
    Multi-signal spam and gibberish detector.
    Returns (is_spam: bool, reason: str)
    """
    stripped = text.strip()
    words = stripped.split()
    total_chars = len(stripped)

    # --- 1. Excessive repeated characters (aaaaaaa, !!!!!, hahahahaha) ---
    if re.search(r'(.)\1{5,}', stripped):
        return (True, "Excessive repeated characters — looks like spam")

    # --- 2. Entire message is a single repeated word ---
    if len(words) >= 4:
        unique_words = set(w.lower() for w in words)
        if len(unique_words) == 1:
            return (True, "Single word repeated over and over")

    # --- 3. Excessive ALL-CAPS (>65% uppercase, ignoring spaces/punctuation) ---
    if total_chars > 10:
        letters = [c for c in stripped if c.isalpha()]
        if letters:
            caps_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
            if caps_ratio > 0.65:
                return (True, "Excessive caps — please write normally")

    # --- 4. Excessive special characters (>35%) ---
    if total_chars > 0:
        special = sum(1 for c in stripped if not c.isalnum() and not c.isspace())
        if special / total_chars > 0.35:
            return (True, "Too many special characters")

    # --- 5. Excessive emojis (>40% of characters are emoji/non-BMP) ---
    if total_chars > 0:
        emoji_chars = sum(1 for c in stripped if ord(c) > 0x1F000)
        if emoji_chars / total_chars > 0.40:
            return (True, "Too many emojis — describe your issue in words")

    # --- 6. Very short words dominate (random noise: "lol ok hi ya oh") ---
    if len(words) > 6:
        short = [w for w in words if len(w.strip('.,!?')) <= 3]
        if len(short) / len(words) > 0.75:
            return (True, "Message is mostly very short/meaningless words")

    # --- 7. Keyboard mashing patterns ---
    keyboard_rows = [
        "qwertyuiop", "asdfghjkl", "zxcvbnm",
        "qazwsx", "wsxedc", "edcrfv", "rfvtgb", "tgbyhn", "yhnujm",
        "1234567890", "0987654321",
        "qwerty", "azerty", "dvorak",
    ]
    t_nospace = stripped.lower().replace(" ", "")
    for kp in keyboard_rows:
        if len(kp) >= 6 and (kp in t_nospace or kp[::-1] in t_nospace):
            return (True, "Keyboard mashing detected")

    # --- 8. No vowels in a long stretch (pure consonant gibberish: "jksdfjkl") ---
    # Check each word individually so real abbreviations (e.g. "lol") don't trigger
    if total_chars > 20:
        long_words = [w for w in words if len(w) > 6]
        for w in long_words:
            w_alpha = re.sub(r'[^a-z]', '', w.lower())
            if len(w_alpha) > 6:
                vowels = sum(1 for c in w_alpha if c in 'aeiou')
                if vowels == 0:
                    return (True, f"Gibberish word detected: '{w}'")

    # --- 9. URL/link injection (phishing, external links not from Telegram) ---
    url_pattern = re.compile(
        r'(https?://|www\.)'                          # http(s):// or www.
        r'(?!t\.me|telegram\.(me|org|dog))'           # exclude Telegram itself
        r'[^\s]{4,}',
        re.IGNORECASE
    )
    if url_pattern.search(stripped):
        return (True, "External links are not allowed in support messages")

    # --- 10. Phone number injection (scam bait: share your number privately) ---
    phone_pattern = re.compile(
        r'(\+?[0-9]{1,3}[\s\-.]?)?'                  # optional country code
        r'(\(?\d{3}\)?[\s\-.]?)'                      # area code
        r'\d{3}[\s\-.]?\d{4,}',                       # local number
        re.IGNORECASE
    )
    if phone_pattern.search(stripped):
        return (True, "Phone numbers are not allowed in support messages")

    # --- 11. Email injection ---
    email_pattern = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
    if email_pattern.search(stripped):
        return (True, "Email addresses are not allowed in support messages")

    # --- 12. Excessive number blocks (card numbers, account numbers) ---
    # 12+ consecutive digits → likely a card/account number
    if re.search(r'\d{12,}', stripped.replace(' ', '').replace('-', '')):
        return (True, "Long number sequences are not allowed — do not share financial data")

    # --- 13. Completely non-meaningful (only punctuation/numbers, no real words) ---
    alpha_chars = sum(1 for c in stripped if c.isalpha())
    if total_chars > 10 and alpha_chars / total_chars < 0.25:
        return (True, "Message has almost no readable text")

    return (False, "")


# ---------------------------------------------------------------------------
# RATE LIMITING for support tickets (per-user submission throttle)
# ---------------------------------------------------------------------------
# Track last ticket submission time per user_id: {user_id: datetime}
_ticket_rate_limit: dict[int, datetime] = {}
TICKET_SUBMIT_COOLDOWN_SECONDS = 120  # 2 min between submissions (prevents ticket flooding)
TICKET_DAILY_LIMIT = 5                # Max tickets per user per 24 h
_ticket_daily_counts: dict[int, list] = {}  # {user_id: [datetime, ...]}

def check_ticket_rate_limit(user_id: int) -> tuple[bool, str]:
    """
    Returns (allowed: bool, error_msg: str).
    Enforces per-user cooldown and daily submission cap.
    """
    now = now_local()

    # Cooldown check
    last = _ticket_rate_limit.get(user_id)
    if last:
        elapsed = (now - last).total_seconds()
        if elapsed < TICKET_SUBMIT_COOLDOWN_SECONDS:
            wait = int(TICKET_SUBMIT_COOLDOWN_SECONDS - elapsed)
            return (False, f"⏳ **PLEASE WAIT**\n\nYou submitted a ticket recently.\nYou can send another in **{wait} seconds**.\n\n_Cooldown prevents flooding our support queue._")

    # Daily cap check — prune entries older than 24 h
    history = _ticket_daily_counts.get(user_id, [])
    history = [t for t in history if (now - t).total_seconds() < 86400]
    _ticket_daily_counts[user_id] = history

    if len(history) >= TICKET_DAILY_LIMIT:
        return (False, f"🚫 **DAILY TICKET LIMIT REACHED**\n\nYou have submitted **{TICKET_DAILY_LIMIT} tickets** in the last 24 hours.\n\nPlease wait before submitting more.\n\n_This limit prevents abuse of our support system._")

    return (True, "")

def record_ticket_submission(user_id: int):
    """Record a successful ticket submission for rate limiting."""
    now = now_local()
    _ticket_rate_limit[user_id] = now
    history = _ticket_daily_counts.get(user_id, [])
    history.append(now)
    _ticket_daily_counts[user_id] = history


# ---------------------------------------------------------------------------
# MASTER VALIDATION — called before every ticket submission
# ---------------------------------------------------------------------------
def validate_ticket_content(text: str, user_name: str = "User") -> tuple[bool, str]:
    """
    Full multi-layer ticket content validation.
    Order: length → profanity → spam/gibberish
    Returns (is_valid: bool, rejection_message: str)
    """
    # 1. Length checks
    if len(text) < MIN_TICKET_LENGTH:
        return (False,
            f"⚠️ **MESSAGE TOO SHORT**\n\n"
            f"{user_name}, please describe your issue in more detail.\n\n"
            f"• Minimum: **{MIN_TICKET_LENGTH} characters**\n"
            f"• Your message: **{len(text)} characters**\n\n"
            f"_Include what happened, when it happened, and what you need help with._"
        )

    if len(text) > MAX_TICKET_LENGTH:
        return (False,
            f"⚠️ **MESSAGE TOO LONG**\n\n"
            f"{user_name}, your message is too long.\n\n"
            f"• Maximum: **{MAX_TICKET_LENGTH} characters**\n"
            f"• Your message: **{len(text)} characters**\n\n"
            f"_Please shorten your message and focus on the key issue._"
        )

    # 2. Profanity / hate speech / threat check
    has_profanity, found_terms = contains_profanity(text)
    if has_profanity:
        # Censor found terms to not expose the full list in messages
        display = ", ".join([f"`{'*' * len(w)}`" for w in found_terms[:3]])
        return (False,
            f"🚫 **INAPPROPRIATE CONTENT DETECTED**\n\n"
            f"{user_name}, your message was blocked by our content filter.\n\n"
            f"**Reason:** Offensive, hateful, or threatening language detected.\n\n"
            f"⚠️ Please rewrite your message respectfully.\n\n"
            f"_Repeated violations may result in your support access being restricted._"
        )

    # 3. Spam / gibberish / injection check
    is_spam, reason = is_spam_or_gibberish(text)
    if is_spam:
        return (False,
            f"🚫 **MESSAGE REJECTED**\n\n"
            f"{user_name}, your message was flagged as invalid.\n\n"
            f"**Reason:** {reason}\n\n"
            f"⚠️ Please write a clear, genuine description of your issue.\n\n"
            f"_Our system requires real, readable support messages._"
        )

    return (True, "")

def get_user_verification_status(user_id: int) -> dict:
    """Get user verification status from database"""
    user_data = col_user_verification.find_one({"user_id": user_id})
    if not user_data:
        # Create new record for new user
        user_data = {
            "user_id": user_id,
            "vault_joined": False,
            "verified": False,
            "ever_verified": False,  # Track if user was EVER verified (for old user detection)
            "verification_msg_id": None,  # Store verification message ID for deletion
            "rejoin_msg_id": None,  # Store rejoin message ID for deletion when user rejoins
            "first_start": now_local()
        }
        col_user_verification.insert_one(user_data)
    return user_data

def update_verification_status(user_id: int, **kwargs):
    """Update user verification fields (prevents duplicates with upsert)"""
    col_user_verification.update_one(
        {"user_id": user_id},
        {"$set": kwargs},
        upsert=True  # Create if doesn't exist, update if exists
    )

# ==========================================
# 🛑 MAINTENANCE MODE CHECK
# ==========================================
async def check_maintenance_mode(message: types.Message) -> bool:
    """
    Check if maintenance mode is enabled.
    Returns True if maintenance is ON and user should be blocked.
    """
    try:
        # 1. Check if user is MASTER_ADMIN (Bypass)
        user_id = message.from_user.id
        if user_id == OWNER_ID:  # Owner can always access
            return False
            
        # 2. Check Settings
        settings = col_bot8_settings.find_one({"setting": "maintenance_mode"})
        if settings and settings.get("value", False):
            # Maintenance is ON
            try:
                user_name = message.from_user.first_name or "Valued Member"
                maintenance_msg = settings.get("maintenance_message", "")
                eta = settings.get("eta", "")
                
                # Build premium personal maintenance message
                msg_lines = [
                    f"👤 **Dear {user_name},**\n",
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                    "🔧 **MSA NODE AGENT — SYSTEM UPGRADE**\n",
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n",
                    "Your MSA Node Agent is currently undergoing a **premium infrastructure upgrade** to deliver you an even more powerful experience.\n\n",
                    "📌 **THIS IS A SCHEDULED UPGRADE**\n",
                    "All services are temporarily paused while our systems evolve.\n\n",
                    "🚫 **During Upgrade:**\n",
                    "• Start links are not active\n",
                    "• All bot features are paused\n",
                    "• No new sessions can begin\n\n",
                ]
                
                if maintenance_msg:
                    msg_lines.append(f"💬 **Message from Admin:**\n_{maintenance_msg}_\n\n")
                
                if eta:
                    msg_lines.append(f"⏳ **Estimated Return:** {eta}\n\n")
                else:
                    msg_lines.append("⏳ **Status:** Coming back online very soon.\n\n")
                
                msg_lines += [
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n",
                    "Thank you for your patience, {user_name}. The upgrade ensures you receive the **best possible service**.\n\n",
                    "_— MSA Node Systems_"
                ]
                
                final_msg = "".join(msg_lines).replace("{user_name}", user_name)
                
                await message.answer(final_msg, parse_mode="Markdown")
                logger.info(f"🚫 Maintenance Block: User {user_id} blocked.")
            except Exception as e:
                logger.error(f"Error sending maintenance message: {e}")
            return True
        
        return False
    except Exception as e:
        logger.error(f"Error checking maintenance mode: {e}")
        return False

# ==========================================
# 🛡️ ANTI-SPAM & UTILITY FUNCTIONS
# ==========================================
async def safe_delete_message(message: types.Message):
    """Safely delete a message without raising exceptions"""
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"Could not delete message: {e}")
        pass

def is_user_processing(user_id: int) -> bool:
    """Check if user is currently processing a command"""
    return user_id in user_processing

def set_user_processing(user_id: int, command: str):
    """Mark user as processing a command"""
    user_processing[user_id] = command
    logger.debug(f"User {user_id} started processing: {command}")

def clear_user_processing(user_id: int):
    """Clear user's processing state"""
    if user_id in user_processing:
        command = user_processing.pop(user_id)
        logger.debug(f"User {user_id} finished processing: {command}")

def rate_limit(cooldown: float = COMMAND_COOLDOWN):
    """Decorator to enforce cooldown between commands (prevents Telegram FloodWait bans)"""
    def decorator(handler):
        @functools.wraps(handler)
        
        async def wrapper(message: types.Message, *args, **kwargs):
            user_id = message.from_user.id
            now = time.time()
            last_time = user_last_command.get(user_id, 0)
            
            # Check if user is within cooldown period
            time_since_last = now - last_time
            if time_since_last < cooldown:
                remaining = cooldown - time_since_last
                logger.warning(f"RATE LIMIT: User {user_id} too fast ({remaining:.1f}s remaining)")
                # Silently ignore - prevents spam from triggering more messages
                return
            
            # Update last command time
            user_last_command[user_id] = now
            
            # Execute the handler
            return await handler(message, *args, **kwargs)
        
        return wrapper
    return decorator

def anti_spam(command_name: str):
    """Decorator to prevent command spam - blocks if user is already processing"""
    def decorator(handler):
        @functools.wraps(handler)
        async def wrapper(message: types.Message, *args, **kwargs):
            user_id = message.from_user.id
            
            # Check if user is already processing
            if is_user_processing(user_id):
                current_command = user_processing.get(user_id, "unknown")
                logger.warning(f"SPAM BLOCKED: User {user_id} tried '{command_name}' while processing '{current_command}'")
                # Silently ignore - don't send warning message to avoid spam
                return
            
            # Mark as processing
            set_user_processing(user_id, command_name)
            
            try:
                # Execute the actual handler
                await handler(message, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {command_name} for user {user_id}: {e}")
            finally:
                # Always clear processing state
                clear_user_processing(user_id)
        
        return wrapper
    return decorator

# ==========================================
# 🧊 FREEZE ENGINE FUNCTIONS
# ==========================================
def _record_spam_tap(user_id: int) -> tuple[bool, int]:
    """
    Record one tap for this user.
    Returns (triggered_new_freeze: bool, freeze_seconds: int).
    Lenient: needs _FREEZE_TRIGGER taps inside _FREEZE_WINDOW seconds.
    """
    now = time.time()
    state = _freeze_tracker.setdefault(user_id, {
        "offense": 0, "frozen_until": 0.0,
        "taps": 0, "window_start": now, "last_tap": now
    })

    # If already frozen just return
    if now < state["frozen_until"]:
        return False, 0

    # Decay offense count if user was clean for _FREEZE_DECAY seconds
    if now - state.get("last_tap", now) > _FREEZE_DECAY:
        state["offense"] = 0

    state["last_tap"] = now

    # Sliding window: reset tap counter when window expires
    if now - state["window_start"] > _FREEZE_WINDOW:
        state["taps"] = 1
        state["window_start"] = now
    else:
        state["taps"] += 1

    # Check if threshold crossed
    if state["taps"] >= _FREEZE_TRIGGER:
        level   = min(state["offense"], len(_FREEZE_LEVELS) - 1)
        secs    = _FREEZE_LEVELS[level]
        state["frozen_until"]  = now + secs
        state["offense"]       = min(state["offense"] + 1, len(_FREEZE_LEVELS))
        state["taps"]          = 0          # reset tap window after freeze
        state["window_start"]  = now
        return True, secs   # ← freeze was triggered

    return False, 0


async def _check_freeze(message: types.Message) -> bool:
    """
    Call at the top of every user handler.
    Records the tap.  If the user is currently frozen → sends a 12h-format
    warning and returns True (handler should return immediately).
    If a new freeze is triggered → also sends warning + notifies owner.
    Returns False when the user is clear to proceed.
    """
    user_id  = message.from_user.id
    now      = time.time()

    # Check if already frozen (without recording a new tap)
    state = _freeze_tracker.get(user_id, {})
    if now < state.get("frozen_until", 0):
        remaining = int(state["frozen_until"] - now)
        mins, secs = divmod(remaining, 60)
        time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
        # Calculate unfreeze clock in 12h format
        unfreeze_dt = datetime.fromtimestamp(state["frozen_until"])
        try:
            import pytz
            tz_name = os.getenv("REPORT_TIMEZONE", "Asia/Kolkata")
            tz = pytz.timezone(tz_name)
            unfreeze_dt = datetime.now(tz) + __import__("datetime").timedelta(seconds=remaining)
        except Exception:
            pass
        unfreeze_str = unfreeze_dt.strftime("%I:%M %p")
        offense = state.get("offense", 1)
        level_label = ["1st", "2nd", "3rd", "4th"][min(offense - 1, 3)]
        try:
            await message.answer(
                f"🧊 <b>You are temporarily frozen.</b>\n\n"
                f"Rapid button presses detected — please slow down.\n\n"
                f"⏳ <b>Unfreeze in:</b> {time_str}  (at {unfreeze_str})\n"
                f"⚠️ <b>Offense level:</b> {level_label}\n\n"
                f"<i>All features are paused during freeze.\n"
                f"Internet lag? No worries — freeze times reset after 10 min of normal use.</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return True  # ← caller should return

    # Record the tap and check if this triggers a new freeze
    triggered, freeze_secs = _record_spam_tap(user_id)
    if triggered:
        state  = _freeze_tracker[user_id]
        offense = state.get("offense", 1)
        level_label = ["1st", "2nd", "3rd", "4th"][min(offense - 1, 3)]
        mins, secs = divmod(freeze_secs, 60)
        time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
        unfreeze_dt = datetime.fromtimestamp(time.time() + freeze_secs)
        try:
            import pytz
            tz_name = os.getenv("REPORT_TIMEZONE", "Asia/Kolkata")
            tz = pytz.timezone(tz_name)
            unfreeze_dt = datetime.now(tz) + __import__("datetime").timedelta(seconds=freeze_secs)
        except Exception:
            pass
        unfreeze_str = unfreeze_dt.strftime("%I:%M %p")
        logger.warning(f"🧊 FREEZE: User {user_id} frozen for {freeze_secs}s (offense #{offense})")
        try:
            await message.answer(
                f"🧊 <b>Auto-Freeze Activated!</b>\n\n"
                f"Too many rapid button presses detected.\n\n"
                f"⏳ <b>Frozen for:</b> {time_str}  (until {unfreeze_str})\n"
                f"⚠️ <b>Offense level:</b> {level_label} — each repeat increases freeze time.\n\n"
                f"<i>All features are paused during freeze.\n"
                f"Slow internet? No worry — 5+ taps in 4s needed to trigger. "
                f"After 10 min of normal use the count resets completely.</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass
        # Notify owner on 3rd+ offense
        if offense >= 3:
            try:
                user_mention = f"@{message.from_user.username}" if message.from_user.username else f"ID {user_id}"
                await bot.send_message(
                    OWNER_ID,
                    f"🧊 <b>REPEAT SPAMMER — {level_label} OFFENSE</b>\n\n"
                    f"User: {user_mention} (ID: <code>{user_id}</code>)\n"
                    f"Frozen for: {time_str}\n"
                    f"Total offenses: {offense}\n\n"
                    f"<i>Not banned — progressive freeze only.</i>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        return True  # ← caller should return

    return False  # ← user is clean, proceed

# ==========================================
# 📦 CONTENT PACKS (20 Items Each)
# ==========================================
CONTENT_PACKS = {
    "IGCCC_CODES": [
        "👁️ **THE GLIMPSE:** You just saw a glimpse. The full picture is bigger. Grab out soon more content and free things. Check out YT MSA CODE.",
        "⚡ **CHARGE:** That content was a spark. The fire is elsewhere. Check out more kind or check out YT MSA CODE.",
        "🧩 **MISSING PIECE:** You have one piece. The puzzle is unsolved. Grab out soon more content and free things. Check out YT MSA CODE.",
        "🌊 **DEEP DIVE:** Don't stay on the surface. Dive deeper. Check out more kind or check out YT MSA CODE.",
        "🗝️ **UNLOCK:** The door is ajar. Push it open. Grab out more content and free things. Check out YT MSA CODE.",
        "🚀 **BOOST:** That was just fuel. The engine is waiting. Check out more kind or check out YT MSA CODE.",
        "💎 **HIDDEN GEMS:** The best content is not public. It is hidden. Grab out soon more content and free things. Check out YT MSA CODE.",
        "📡 **SIGNAL:** You received the signal. Now trace the source. Check out more kind or check out YT MSA CODE.",
        "🧠 **INTEL:** That was data. We have wisdom. Grab out soon more content and free things. Check out YT MSA CODE.",
        "🔒 **ACCESS:** You are at the gate. The key is inside. Check out more kind or check out YT MSA CODE.",
        "🌐 **NETWORK:** One post is a dot. The network is a line. Connect. Grab out soon more content and free things. Check out YT MSA CODE.",
        "💼 **ASSET:** You viewed an asset. Now own the vault. Check out more kind or check out YT MSA CODE.",
        "🧬 **DNA:** That was a sample. The organism is alive. Grab out soon more content and free things. Check out YT MSA CODE.",
        "🔌 **PLUG IN:** You are running on battery. Plug into the grid. Check out more kind or check out YT MSA CODE.",
        "🐺 **THE PACK:** You saw the lone wolf. Meet the pack. Grab out soon more content and free things. Check out YT MSA CODE.",
        "🦅 **ALTITUDE:** You are on the ground. Fly higher. Check out more kind or check out YT MSA CODE.",
        "⚔️ **WEAPON:** That was a shield. Get the sword. Grab out soon more content and free things. Check out YT MSA CODE.",
        "🛡️ **DEFENSE:** You are exposed. Get armor. Check out more kind or check out YT MSA CODE.",
        "🩸 **BLOOD:** It is in the veins. The heart is beating. Grab out soon more content and free things. Check out YT MSA CODE.",
        "🌪️ **STORM:** That was a breeze. The storm is coming. Grab out soon more content and free things. Check out YT MSA CODE."
    ],
    "PDF_TITLES": [
        "📫 **DELIVERED:** The Asset is in your inbox, {name}. It is a clear, step-by-step blueprint. Open it and execute.",
        "🗺️ **THE MAP:** You have the map, {name}. It is easy to read. Follow the path. The destination is clear.",
        "✅ **CONFIRMED:** The Transfer is complete, {name}. The PDF is a simplified guide. No fluff. Just action.",
        "📘 **THE BLUEPRINT:** This is not a complex theory, {name}. It is a practical blueprint. Build exactly what you see.",
        "⚡ **QUICK START:** The file is designed for speed, {name}. Read it in 10 minutes. Execute it today. Fast results.",
        "🗝️ **MASTER KEY:** You hold the key, {name}. It fits the lock perfectly. Turn it. Open the door.",
        "🧠 **CLARITY:** Confusion is the enemy, {name}. This PDF is the weapon. It cuts through the noise. Get clarity.",
        "🏗️ **FOUNDATION:** Everything starts here, {name}. The PDF lays the foundation. It is solid. Build on it.",
        "🛡️ **ARMOR:** The world is chaotic, {name}. This document is your armor. Put it on. You are protected.",
        "🧭 **COMPASS:** You were lost, {name}. Now you have a compass. It points North. Follow the direction.",
        "🩸 **THE PACT:** We have a deal, {name}. I give you the strategy. You give me the work. The contract is in the file.",
        "🔋 **POWER SOURCE:** This is not just text, {name}. It is a battery. Plug in. Charge your systems.",
        "🕶️ **VISION:** You were blind to the opportunity, {name}. Now you see. The PDF gives you 20/20 vision.",
        "🧬 **THE CODE:** Success has a code, {name}. You just downloaded it. It is readable. Decrypt your future.",
        "🎓 **THE LESSON:** School taught you to memorize, {name}. This PDF teaches you to think. Learn the real lesson.",
        "💼 **PROFESSIONAL:** This is high-level intel, {name}. Treat it with respect. Execute like a professional.",
        "🚦 **GREEN LIGHT:** You have the green light, {name}. The instructions are simple. Go. Do not stop.",
        "🧩 **SOLVED:** The puzzle is solved, {name}. The PDF shows you the picture. Put the pieces together.",
        "💎 **THE GEM:** You dug for it, {name}. Now polish it. The value is in your hands. Don't drop it.",
        "🚀 **LAUNCH:** The checklist is complete, {name}. The systems are go. Launch the mission."
    ],
    "PDF_BUTTONS": [
        "📂 OPEN BLUEPRINT", "🔓 UNLOCK ASSET", "👁️ SEE TRUTH", "🎒 GRAB BAG",
        "📦 UNBOX PACKAGE", "🗝️ USE KEY", "👓 VIEW EVIDENCE",
        "🤝 SECURE DEAL", "✊ SEIZE CHANCE", "📄 READ FILE", "🧱 BREAK WALL",
        "🔦 REVEAL INTEL", "💵 CLAIM BOUNTY", "📥 GET DOWNLOAD", "💼 OPEN BRIEFCASE",
        "🔐 DECRYPT FILE", "🔭 SCOUT TARGET", "🎣 HOOK PRIZE", "💿 SAVE ASSET",
        "🗄️ ACCESS ARCHIVE", "🚪 ENTER ROOM", "🔬 INSPECT DATA", "🕯️ SEE LIGHT",
        "⚒️ FORGE KEY", "🗡️ EQUIP WEAPON", "🩸 TAKE OATH",
        "💠 CLAIM GEM"
    ],
    "PDF_FOOTERS": [
        "⚠️ Authorized for {name} only", "🔒 Status: CLASSIFIED | User: {name}", "⏱️ Time: NOW | Mission: GO",
        "🕶️ Mode: GHOST | Trace: NONE", "🩸 Pact: SEALED | {name}", "🧾 Receipt: VALID | Item: BLUEPRINT",
        "🛡️ Protection: ACTIVE | {name}", "🧬 DNA Match: {name} | CONFIRMED", "🔋 Battery: FULL | {name}: READY",
        "🧊 Temperature: COLD | Fear: ZERO", "🐺 Pack: ONE | Leader: {name}", "🦅 Altitude: HIGH | View: CLEAR",
        "🗝️ Access: GRANTED | Level: MAX", "🚫 Leaks: ZERO | Trust: 100%", "🧠 Firmware: UPDATED | {name}",
        "🏹 Target: LOCKED | Shot: YOURS", "⚖️ Judge: YOU | Verdict: WIN", "🌪️ Storm: WEATHERED | Path: CLEAR",
        "🧱 Wall: BROKEN | Path: OPEN", "🔦 Light: ON | Shadow: GONE", "💊 Matrix: EXIT | Reality: ENTER",
        "💉 Dose: TRUTH | Patient: {name}", "🧩 Puzzle: SOLVED | Reward: CLAIMED", "🏆 Rank: ELITE | Player: {name}",
        "🎫 Ticket: PUNCHED | Ride: START", "🎬 Scene: ONE | Action: {name}", "🎤 Mic: ON | Stage: YOURS",
        "🥊 Corner: BLUE | Fighter: {name}", "🚦 Light: GREEN | Pedal: DOWN", "🏁 Flag: WAVED | Winner: {name}"
    ],
    "AFFILIATE_TITLES": [
        "🤖 **THE WORKFLOW:** I used to pay a VA $1,500/month to run my Twitter. Now I pay this AI tool $29/month to do it better. That is a $17,000/year raise. Click to give yourself a raise.",
        "💸 **ROI ALERT:** This isn't an expense, it's an investment. If you buy a $40 tool and it makes you one $50 sale, everything after that is infinite ROI. Do not be cheap with your future.",
        "🚀 **SPEED:** Speed is the only advantage you have against big corporations. They have meetings; you have this AI. While they talk, you build. Get the tool and start building.",
        "💰 **ASSET BUILDING:** Stop looking for 'gigs' and start building 'assets'. An automated social media channel is an asset that pays you while you sleep. This is the engine for that asset.",
        "🧬 **CLONE YOURSELF:** You are limited by 24 hours in a day. This AI is not. It clones your tone, your ideas, and your output. It’s the only ethical way to clone yourself. Start cloning.",
        "📈 **COMPOUND RESULTS:** Content compounds. One video does nothing. 100 videos change your life. This tool ensures you actually post the 100 videos without burning out. Start compounding.",
        "🏦 **THE MATH:** A $20 tool that saves you 20 hours is paying you $100/hour to use it (assuming your time is worth $100). If you don't buy it, you are losing money. Do the math.",
        "💎 **HIDDEN GEM:** Most 'AI tools' are just ChatGPT wrappers. This one is different. It’s a full-stack automation suite that actually executes tasks. I only share the real ones. Get it.",
        "🧾 **EXPENSE IT:** If you have a business, this is a write-off. If you don't have a business, this is how you start one. It costs less than a lunch. Stop overthinking.",
        "🏗️ **FOUNDATION:** You wouldn't build a house without a foundation. Don't try to build a content empire without an automation foundation. This software is the concrete.",
        "🧠 **PSYCHOLOGY:** Humans are wired to trust consistency. If you post every day, you win trust. But humans are inconsistent. This AI solves the human flaw. Be consistent.",
        "⚡ **FRICTION:** The reason you haven't started is 'friction'. Creating is hard. This tool removes the friction. One click, one piece of content. Remove the barrier.",
        "🕵️ **SECRET ADVANTAGE:** The top 1% of creators aren't working 100x harder than you. They just have better levers. This tool is a lever. Pull it.",
        "📝 **WRITING HACK:** I hate writing emails. So I stopped. I trained this AI to write exactly like me, and now it sends 1000 emails a week. My open rates went UP. Try it.",
        "🎨 **NO SKILL NEEDED:** You don't need to be a designer, a writer, or a coder. You just need to be smart enough to use this tool. It bridges the skill gap. Cross the bridge.",
        "🧹 **AUTOMATE THE BORING:** Life is too short to do boring work. Data entry, scheduling, formatting... let the robot do it. You focus on the strategy. Reclaim your life.",
        "🚿 **PASSIVE INCOME:** Everyone says they want passive income, but they do manual work. That is active income. To get passive results, you need active robots. Here is your robot.",
        "⚙️ **SYSTEM:** You fall to the level of your systems. If your system is 'I'll do it when I feel like it', you will fail. If your system is this AI, you will succeed. Upgrade your system.",
        "📅 **CONSISTENCY:** Motivation gets you started. Habit keeps you going. Automation keeps you going even when you quit. This is your insurance policy against quitting.",
        "📂 **DIGITAL REAL ESTATE:** Every piece of content you post is a digital brick. This tool lays bricks 24/7. Build your mansion while you sleep.",
        "😨 **THE WARNING:** I've seen it happen. People wait too long, the algorithm changes, and the opportunity is gone. This tool is working *right now*. Don't wait for it to break.",
        "🦖 **DINOSAUR:** In 5 years, running a manual business will be like riding a horse to work. Cute, but slow. Don't be a dinosaur. Get the car (AI).",
        "📉 **INFLATION:** The cost of living is going up. Your income needs to go up faster. Manual work can't keep up. Scalable AI income is the only hedge. Protect yourself.",
        "🚫 **DON'T GET LEFT BEHIND:** Your competitors are reading this right now. Half of them will click. Half won't. The half that click will beat you. Which half are you in?",
        "⚠️ **PRICE HIKE:** Software companies always raise prices once they get popular. Lock in your legacy pricing now before they 2x the monthly cost. Secure the bag.",
        "🛑 **STOP SCROLLING:** You have been scrolling for 20 minutes. That gave you $0. If you spent that 20 minutes setting up this tool, you'd be building an asset. Switch modes.",
        "⏳ **TIME IS MONEY:** Every hour you spend doing manual work is an hour you just sold for $0. Stop giving away your inventory. Automate the work.",
        "🌪️ **THE WAVE:** AI is a tidal wave. You can surf it or you can drown. This tool is your surfboard. Get on the board.",
        "👋 **FIRE YOUR BOSS:** The only way to fire your boss is to replace your salary. You can't do that with a side hustle that takes 10 hours a day. You need automation. Start here.",
        "🤜 **PUNCH BACK:** The economy is punching you in the face. Punch back. Build a revenue stream that isn't dependent on a paycheck. This is your weapon.",
        "🧪 **TESTED BY ME:** I don't recommend junk. I personally use this for my main channel. If it breaks, I lose money. It hasn't broken. That's my endorsement.",
        "📊 **RESULTS:** I showed this to a student last week. He set it up in 20 minutes. Today he sent me a screenshot of his first commission. It works fast. Try it.",
        "👨🔬 **THE LAB:** I spend $1,000s testing tools so you don't have to. I filtered out the trash. This is the one that survived. It's the best in class.",
        "🔬 **VETTED:** I don't share garbage. I vet everything. This passed every test. Trust my process, {name}."
    ],
    "AFFILIATE_FOOTERS": [
        "Click now or regret later, {name}.",
        "Every second you wait is revenue lost, {name}.",
        "This is the sign you were looking for, {name}.",
        "Don't let fear decide your future, {name}.",
        "You'll either click this or watch someone else win with it, {name}.",
        "The best time was yesterday. The second best time is now, {name}.",
        "Hesitation is expensive, {name}.",
        "Winners click. Losers scroll.",
        "This is your edge, {name}. Use it.",
        "Success leaves clues. This is one of them.",
        "You already know you need this, {name}.",
        "Investment, not expense. Get it {name}.",
        "While you think, others act. Don't be late {name}.",
        "Your competition just clicked. Now it's your turn {name}.",
        "Courage is clicking even when you're scared, {name}.",
        "This tool pays for itself on day one {name}.",
        "Stop planning. Start building.",
        "The opportunity is here. The decision is yours, {name}.",
        "You can afford this. You can't afford to skip it.",
        "One click. Infinite upside. Zero excuses."
    ],
    "AFFILIATE_TITLES_EXTRA": [
        "✅ **VERIFIED:** Beware of fake AI tools. There are scams out there. This link is the verified official site for the tool I use. Stay safe. Use this link.",
        "📜 **MY STACK:** People ask me 'What is your tech stack?'. This is the foundation of it. Without this, my business collapses. That is how important it is.",
        "👨🏫 **LESSON:** The wealthy buy time. The poor sell time. For $29, you are buying 100 hours of time. That is the best trade you will ever make.",
        "🏆 **WINNER:** Winners make decisions quickly. Losers overthink until the opportunity is gone. Be a winner. Make the decision. Click the link.",
        "🥇 **TOP TIER:** There are free tools and there are paid tools. Free tools cost you time. Paid tools make you money. Upgrade to the top tier.",
        "🤝 **TRUST ME:** If you trust my content, trust my recommendation. I would not risk my reputation for a few dollars. This tool is legitimate power.",
        "🗣️ **FINAL WORD:** You can keep doing it the hard way, and I will respect the hustle. But if you want the smart way, the wealthy way... click the button."
    ],
    "AFFILIATE_BUTTONS": [
        "💸 CLAIM YOUR EDGE",
        "🚀 ACTIVATE NOW",
        "🛠️ GRAB THE TOOL",
        "⚡ GET INSTANT ACCESS",
        "🤖 UNLOCK AUTOMATION",
        "📈 START EARNING TODAY",
        "🏗️ BUILD YOUR EMPIRE",
        "💎 SECURE THE GEM",
        "🧱 LAY YOUR FOUNDATION",
        "⏳ STOP WASTING TIME",
        "🔥 IGNITE YOUR GROWTH",
        "💰 CLAIM FREE TRIAL",
        "🎯 HIT YOUR TARGET",
        "🔓 UNLOCK FULL POWER",
        "⚙️ AUTOMATE EVERYTHING",
        "🏆 JOIN THE WINNERS",
        "🎁 REDEEM YOUR BONUS",
        "💪 GAIN THE ADVANTAGE",
        "🌟 ACCESS PREMIUM NOW",
        "✅ YES, I WANT THIS"
    ],
    "YT_VIDEO_TITLES": [
        "👁️ **THE SOURCE:** You have seen the clips on Instagram, {name}. Now go to the source. The Main Channel has the full picture. Explore it.",
        "📡 **MAIN FREQUENCY:** Instagram is for updates, {name}. YouTube is for the broadcast. Tune into the main frequency on the Channel.",
        "🧠 **THE ARCHIVE:** You are only seeing the surface on Instagram, {name}. The YouTube Channel is the archive. Go deep.",
        "🏗️ **HEADQUARTERS:** Instagram is the outpost, {name}. YouTube is Headquarters. Report to HQ for the full briefing.",
        "🌊 **DEEP DIVE:** Instagram is the shallow end, {name}. YouTube is the deep ocean. Dive into the Main Channel.",
        "📚 **THE LIBRARY:** You read the headlines on Insta, {name}. Read the book on YouTube. The Channel holds the knowledge.",
        "⚡ **FULL POWER:** Instagram is 10% power, {name}. YouTube is 100%. Switch to the Main Channel for full voltage.",
        "🔥 **UNCENSORED:** We are limited on Instagram, {name}. We are unleashed on YouTube. Watch the uncensored strategies on the Channel.",
        "🔐 **THE VAULT:** The gems are on Instagram, {name}. The gold bars are on YouTube. Enter the vault on the Main Channel.",
        "🧬 **ORIGIN STORY:** You know the brand from Instagram, {name}. Learn the philosophy on YouTube. Watch the Main Channel.",
        "🕸️ **THE NETWORK:** Instagram is the web, {name}. YouTube is the spider. Come to the center of the network.",
        "🎓 **HIGHER LEARNING:** Instagram is recess, {name}. YouTube is class. School is in session on the Main Channel.",
        "🛫 **LAUNCHPAD:** You are taxiing on Instagram, {name}. Take off on YouTube. The Main Channel is the runway.",
        "🔭 **BIGGER PICTURE:** Expand your view, {name}. Instagram is a keyhole. YouTube is the door. Open it.",
        "🗺️ **EXPEDITION:** The journey starts on Insta, {name}. The expedition happens on YouTube. Join the trek on the Channel.",
        "🥊 **HEAVYWEIGHT:** Instagram is sparring, {name}. YouTube is the title fight. Step into the ring on the Main Channel.",
        "🎹 **FULL SYMPHONY:** You heard the notes on Insta, {name}. Hear the symphony on YouTube. Listen to the Main Channel.",
        "🍳 **THE KITCHEN:** You saw the meal on Instagram, {name}. See how it's cooked on YouTube. Enter the kitchen.",
        "🏎️ **FULL THROTTLE:** You are cruising on Insta, {name}. Race on YouTube. Hit the gas on the Main Channel.",
        "🌎 **THE UNIVERSE:** You are in orbit on Instagram, {name}. Land on the planet on YouTube. Explore the ecosystem."
    ],
    "YT_CODES_BUTTONS": [
        "📺 EXPLORE CHANNEL",
        "📺 VISIT MAIN HUB",
        "📺 ACCESS ARCHIVE",
        "📺 ENTER THE VAULT",
        "📺 JOIN THE NETWORK",
        "📺 SEE FULL PICTURE",
        "📺 GO TO SOURCE",
        "📺 UNLOCK CHANNEL",
        "📺 VIEW ALL INTEL",
        "📺 OPEN MAIN FEED"
    ],
    "IG_VIDEO_TITLES": [
        "➕ **GET MORE:** You liked the video, {name}? There is so much more on Instagram. Get the full experience.",
        "🤝 **CONNECT:** You watched the content, {name}. Now connect with the man behind it. I am on Instagram.",
        "🏠 **THE HOUSE:** YouTube is the front yard, {name}. Instagram is the living room. Come inside the house.",
        "🔥 **THE ENERGY:** YouTube is information, {name}. Instagram is energy. Come feel the vibe.",
        "🧬 **FULL CIRCLE:** You have the lesson, {name}. Now get the lifestyle. Use Instagram to complete the circle.",
        "🫂 **THE FAMILY:** YouTube is for everyone, {name}. Instagram is for the family. Join the brotherhood.",
        "📸 **UNFILTERED:** YouTube is polished, {name}. Instagram is raw. See the real me.",
        "🧠 **INSIDE MY HEAD:** I share my daily thoughts on Instagram, {name}. Get inside my head. Learn how I think.",
        "❤️ **PASSION:** You see the work on YouTube, {name}. Feel the passion on Instagram. It hits different.",
        "🆙 **LEVEL UP:** You want more? I give more on Instagram, {name}. Level up your access.",
        "🎁 **BONUS:** The video was just the start, {name}. The bonus content is waiting on Instagram. Go get it.",
        "🗣️ **CONVERSATION:** YouTube is a speech, {name}. Instagram is a conversation. Let's talk.",
        "👀 **CLOSER LOOK:** Get a closer look at the operation, {name}. Instagram zooms in. See the details.",
        "🛡️ **MY CIRCLE:** See who I hang with on Instagram, {name}. You are the average of your circle. Check mine.",
        "💎 **MORE GEMS:** I drop daily gems on various topics, {name}. Don't miss the free game on Instagram.",
        "🚀 **THE RIDE:** Come along for the ride, {name}. I document the journey on Instagram. Be a passenger.",
        "🚪 **BACKSTAGE:** You saw the show on YouTube, {name}. Come backstage on Instagram. Meet the team.",
        "🔌 **PLUG IN:** YouTube is the device, {name}. Instagram is the outlet. Plug in for power.",
        "🌊 **IMMERSE:** Don't just watch, {name}. Immerse yourself. Instagram surrounds you with the mindset.",
        "🔑 **ACCESS GRANTED:** I am giving you access to my daily life, {name}. Accept the invite on Instagram."
    ],
    "IG_CODES_BUTTONS": [
        "📸 SEE THE REALITY",
        "📸 JOIN THE NETWORK",
        "📸 WATCH EXECUTION",
        "📸 SEE DAILY OPS",
        "📸 VERIFY RESULTS",
        "📸 CHECK THE FIELD",
        "📸 FOLLOW THE MAN",
        "📸 VIEW LIFESTYLE",
        "📸 ACCESS EVIDENCE",
        "📸 ENTER THE LAB"
    ],
    "IG_VIDEO_FOOTERS": [
        "Don't overthink it, {name}. Just click and see.",
        "This is where the conversation happens.",
        "The door is open, {name}. Step in.",
        "Stop reading. Start following.",
        "You'll regret not clicking, {name}.",
        "The network is waiting for you.",
        "One click separates you from the next level, {name}.",
        "Follow now. Thank yourself later.",
        "You're already here. Might as well commit.",
        "This isn't spam, {name}. This is opportunity.",
        "Everyone who follows, grows. Simple math.",
        "The proof is in the feed, {name}.",
        "You clicked on the Blueprint. Now click on this, {name}.",
        "Instagram is where I live, {name}. Come visit.",
        "Don't let your fear of commitment stop your growth.",
        "You got this far, {name}. Finish the job.",
        "The people who follow, succeed. Facts.",
        "This is the missing piece, {name}.",
        "Access denied until you follow.",
        "If you're serious, you'll click. If not, you won't."
    ],
    "MSACODE": [
        "🔍 **THE SOURCE:** {name}, YouTube holds the **MSA CODES**. Instagram holds the **INTEL**. You need both to survive.",
        "🗝️ **KEYS & MAPS:** The Keys (**MSA CODES**) are in the YouTube briefings, {name}. The Map is on Instagram. Don't get lost.",
        "💎 **DOUBLE THREAT:** {name}, Watch YouTube for the **MSA CODES**. Follow Instagram for the **STRATEGY**. Master both.",
        "📡 **SIGNAL:** YouTube transmits the **MSA CODES**, {name}. Instagram transmits the **CULTURE**. Tune into both frequencies.",
        "🛑 **MISSING DATA:** {name}, If you only have the PDF, you have 10%. YouTube has the **MSA CODES** (40%). Instagram has the rest.",
        "🐺 **HUNTING GROUNDS:** We drop **MSA CODES** in YouTube videos, {name}. We drop **STATUS** on Instagram. Hunt everywhere.",
        "👁️ **ALWAYS WATCHING:** Did you miss the **MSA CODE** in the last video, {name}? YouTube has it. Instagram shows you how to use it.",
        "⚡ **POWER SUPPLY:** {name}, YouTube is the **GENERATOR** (MSA CODES). Instagram is the **BATTERY** (Energy). Plug into both.",
        "🧠 **FULL ACCESS:** You want more **MSA CODES**, {name}? Go to YouTube. You want the network? Go to Instagram. Full access requires both.",
        "📦 **THE DROP:** The Asset is here, {name}. The **MSA CODE** to open the next one is on YouTube. The **MISSION** is on Instagram.",
        "🔐 **TWO KEYS:** Success requires two keys, {name}. One (**MSA CODE**) is hidden in our YouTube videos. The other is on our Instagram feed.",
        "🌐 **THE SYSTEM:** The System distributes **MSA CODES** via YouTube and **ORDERS** via Instagram. Follow the System, {name}.",
        "🧬 **DNA:** The DNA of success, {name}: **MSA CODES** (YouTube) + **NETWORK** (Instagram). Do not separate them.",
        "🕵️ **CLUES:** {name}, We hid the last **MSA CODE** in a YouTube frame. We posted the clue on Instagram. Play the game.",
        "🏆 **THE PRIZE:** The prize is locked, {name}. YouTube has the **MSA CODE**. Instagram shows you the path to the vault.",
        "🔌 **DISCONNECTED:** Without YouTube, you miss the **MSA CODES**. Without Instagram, you miss the **SIGNAL**. Reconnect, {name}.",
        "📢 **BRIEFING:** The Mission Briefing is on YouTube (grab the **MSA CODE**), {name}. The Debrief is on Instagram. Report in.",
        "⏳ **COUNTDOWN:** The next **MSA CODE** drops on YouTube soon, {name}. Instagram will notify you. Be ready.",
        "🤝 **THE DEAL:** You watch YouTube for **MSA CODES**, {name}. You follow Instagram for **POWER**. That is the deal.",
        "🚪 **DUAL ENTRY:** One door opens with an **MSA CODE** (YouTube). The other opens with reputation (Instagram). Enter, {name}.",
        "🔦 **SEARCH PARTY:** {name}, the search is on. **MSA CODES** are hidden on YouTube. **CLUES** are on Instagram.",
        "💼 **THE BRIEFCASE:** The briefcase is locked, {name}. Combination is an **MSA CODE** (YouTube). Location is Instagram.",
        "🚁 **EXTRACTION:** Extraction point set, {name}. Ticket is an **MSA CODE** (YouTube). Route is on Instagram.",
        "📡 **FREQUENCY:** {name}, you are on the wrong frequency. Tune to YouTube for **MSA CODES**. Instagram for **ORDERS**.",
        "🧱 **THE WALL:** Hit a wall, {name}? Break it with an **MSA CODE** from YouTube. Build a bridge on Instagram.",
        "💊 **RED PILL:** The Red Pill is the **MSA CODE** (YouTube). The rabbit hole is Instagram. Wake up, {name}.",
        "🕰️ **TIK TOK:** Time is running out, {name}. Grab the **MSA CODE** from YouTube before the clock stops. Updates on Instagram.",
        "🗺️ **COMPASS:** You are lost, {name}. YouTube is your North (**MSA CODES**). Instagram is your map.",
        "⚖️ **JUDGMENT:** You are being judged, {name}. Evidence: **MSA CODES** (YouTube). Verdict: Instagram.",
        "🌪️ **CHAOS:** Control the chaos, {name}. Structure comes from **MSA CODES** (YouTube). Power comes from Instagram.",
        "🔑 **MASTER KEY:** There is a master key, {name}. It's an **MSA CODE** on YouTube. The door is on Instagram.",
        "👁️‍🗨️ **VISION:** Clear your vision, {name}. See the **MSA CODE** on YouTube. See the future on Instagram.",
        "🩸 **BLOODLINE:** It's in the blood, {name}. **MSA CODES** (YouTube) are the DNA. The Network (Instagram) is the family.",
        "🛡️ **SHIELD:** Shields up, {name}. Armor yourself with **MSA CODES** (YouTube). Stand your ground on Instagram.",
        "⚔️ **SWORD:** Strike first, {name}. Weapon: **MSA CODE** (YouTube). Battleground: Instagram.",
        "👑 **CROWN:** Heavy is the head, {name}. Earn the crown with **MSA CODES** (YouTube). Wear it on Instagram.",
        "🦁 **ROAR:** Silence the lambs, {name}. Roar with an **MSA CODE** (YouTube). Lead the pride on Instagram.",
        "🦅 **ALTITUDE:** Fly higher, {name}. Fuel: **MSA CODES** (YouTube). Airspace: Instagram.",
        "🌑 **ECLIPSE:** Overshadow them, {name}. Light: **MSA CODE** (YouTube). Shadow: Instagram.",
        "🚀 **IGNITION:** 3, 2, 1... Launch, {name}. Ignition code is an **MSA CODE** (YouTube). Orbit is Instagram."
    ],
    "MSACODE_BUTTONS": [
        ("📺 ACQUIRE TARGET", "📸 CONFIRM KILL"),
        ("📺 ANALYZE SIGNAL", "📸 JOIN NETWORK"),
        ("📺 WATCH BRIEFING", "📸 REPORT STATUS"),
        ("📺 DECRYPT VIDEO", "📸 ACCESS COMMS"),
        ("📺 UNLOCK SYSTEM", "📸 ENTER PROTOCOL"),
        ("📺 VIEW EVIDENCE", "📸 VERIFY SOURCE"),
        ("📺 OPEN CHANNEL", "📸 ESTABLISH LINK"),
        ("📺 GRAB BLUPRINT", "📸 JOIN DYNASTY"),
        ("📺 INITIATE PLAN", "📸 EXECUTE ORDER"),
        ("📺 ACCESS ARCHIVE", "📸 CHECK RANK"),
        ("📺 CLAIM ASSET", "📸 VERIFY ID"),
        ("📺 START MISSION", "📸 JOIN SQUAD"),
        ("📺 DECODE INTEL", "📸 READ DOSSIER"),
        ("📺 OPEN VAULT", "📸 ENTER GATE"),
        ("📺 GET STRATEGY", "📸 SEE TACTICS"),
        ("📺 DOWNLOAD KEY", "📸 UPLOAD STATUS"),
        ("📺 ACTIVATE", "📸 DEPLOY"),
        ("📺 WATCH FOOTAGE", "📸 SEE PROOF"),
        ("📺 ENTER MATRIX", "📸 JOIN REALITY"),
        ("📺 UNLOCK GATE", "📸 ACCESS CITY"),
        ("📺 RETRIEVE CODE", "📸 CONFIRM ENTRY"),
        ("📺 SECURE ASSET", "📸 JOIN FACTION"),
        ("📺 WATCH INTEL", "📸 READ REPORT"),
        ("📺 GET PASSWORD", "📸 ENTER CONSOLE"),
        ("📺 ACCESS MAIN", "📸 JOIN CHANNEL"),
        ("📺 VIEW SOURCE", "📸 VERIFY OATH"),
        ("📺 OPEN FILE", "📸 READ MEMO"),
        ("📺 GET CLEARANCE", "📸 JOIN ELITE"),
        ("📺 UNLOCK POWER", "📸 GAIN STATUS"),
        ("📺 VIEW CODES", "📸 SEE NETWORK"),
        ("📺 START DOWNLOAD", "📸 START UPLOAD"),
        ("📺 GET BRIEFING", "📸 VERIFY RANK"),
        ("📺 ACCESS TERMINAL", "📸 JOIN SERVER"),
        ("📺 WATCH VIDEO", "📸 SEE EVIDENCE"),
        ("📺 GRAB CODE", "📸 JOIN TEAM"),
        ("📺 ENTER CODE", "📸 ENTER WORLD"),
        ("📺 UNLOCK NOW", "📸 JOIN NOW"),
        ("📺 ACCESS KEY", "📸 ACCESS HUB"),
        ("📺 VIEW MAP", "📸 FIND PATH"),
        ("📺 GET COORDINATES", "📸 JOIN LOCATION"),
        ("📺 START ENGINE", "📸 JOIN CONVOY"),
        ("📺 LOAD PROGRAM", "📸 RUN SYSTEM"),
        ("📺 EXECUTE CODE", "📸 CONFIRM KILL"),
        ("📺 ACCESS DATABASE", "📸 READ LOGS"),
        ("📺 GET CREDENTIALS", "📸 VERIFY PASS"),
        ("📺 OPEN PORTAL", "📸 ENTER REALM"),
        ("📺 START SEQUENCE", "📸 JOIN OPS"),
        ("📺 UNLOCK POWER", "📸 CLAIM THRONE"),
        ("📺 ACCESS REWARD", "📸 RANK UP"),
        ("📺 FINAL STEP", "📸 COMPLETE MISSION")
    ],
    "MSACODE_FOOTERS": [
        "🛡️ Clearance: VAULT | Status: VERIFIED",
        "👁️ Surveillance: ACTIVE | Trace: SECURE",
        "⚡ Connection: ENCRYPTED | Uplink: STABLE",
        "🔒 Security Level: MAX | User: {name}",
        "🕶️ Mode: GHOST | Access: GRANTED",
        "🧬 Identity: CONFIRMED | Phase: ACTIVE",
        "📡 Signal: STRONG | Protocol: OMEGA",
        "🗝️ Keys: ALLOCATED | Session: SECURE",
        "🩸 Oath: BOUND | Loyalty: VERIFIED",
        "🏛️ Network: PRIVATE | Entry: AUTHORIZED",
        " Zone: RESTRICTED | Pass: VALID",
        "🧪 Lab: SECURE | Test: PASSED",
        "🧹 Area: CLEAN | Threat: NULL",
        "🧗 Altitude: HIGH | Air: THIN",
        "⚓ Anchor: LIFTED | Sail: SET",
        "🥊 Fight: WON | Belt: HELD",
        "🏁 Race: OVER | Winner: {name}",
        "🐺 Pack: ALPHA | Hunt: ON",
        "🦅 View: EAGLE | Eyes: SHARP",
        "🕯️ Flame: LIT | Shadow: CAST",
        "🗡️ Blade: SHARP | Cut: DEEP",
        "🏆 Trophy: WON | Shelf: FULL",
        "👻 Mode: STEALTH | Noise: ZERO",
        "🚫 Mercy: NONE | Win: ALL",
        "🔋 Battery: 100% | Charge: HOLDING",
        "🤖 Bot: ACTIVE | AI: ONLINE",
        "💸 Asset: SECURE | Value: HIGH",
        "🏗️ Build: COMPLETE | Foundation: SOLID",
        "🧠 Mind: FOCUSED | Vision: CLEAR",
        "🌪️ Force: GALE | Path: DESTRUCTIVE",
        "🌊 Wave: RIDING | Surf: UP",
        "🔥 Heat: MAX | Burn: CONTROLLED",
        "❄️ Ice: COLD | Veins: FROZEN",
        "☁️ Cloud: UPLINK | Sync: DONE",
        "🌞 Dawn: BREAKING | Rise: NOW",
        "🌚 Night: OPS | Cover: DARK",
        "⭐ Star: RISING | Shine: BRIGHT",
        "🌀 Vortex: OPEN | Pull: STRONG"
    ],
    "MSACODE_INVALID": [
        "❌ **IMPOSSIBLE:** That MSA CODE does not exist, {name}. You are guessing. Stop guessing. Click below to get the real MSA CODE.",
        "🚫 **ACCESS DENIED:** We checked, {name}. That MSA CODE is wrong. You skipped the briefing. Click below to watch the video.",
        "⚠️ **WARNING:** Invalid input detected, {name}. Do not waste the system's time. Click below to retrieve the correct MSA CODE.",
        "🛑 **STOP:** You are trying to take shortcuts, {name}. There are no shortcuts. Click below to get the real MSA CODE.",
        "📉 **FAILURE:** You missed the MSA CODE, {name}. It was on the screen. Click below and go find it.",
        "🔒 **LOCKED:** The door remains shut, {name}. You do not have the key. The key is in the video. Click below.",
        "📵 **NO SIGNAL:** Your MSA CODE is noise, {name}. We need the signal. Click below to connect to the source.",
        "🧩 **MISSING PIECE:** You are trying to solve the puzzle without the pieces, {name}. Click below to get the piece.",
        "📉 **ERROR 404:** MSA CODE not found, {name}. Strategy: Click below. Watch. Return.",
        "👀 **BLIND:** You are flying blind, {name}. The coordinates are in the briefing. Click below to see.",
        "🧱 **WALL:** You hit a wall, {name}. Break it with the correct MSA CODE. Click below to find the hammer.",
        "🕸️ **TRAP:** You fell into the trap of laziness, {name}. Climb out. Click below to do the work.",
        "⚖️ **JUDGMENT:** The system judges your MSA CODE: INVALID. Appeal by clicking below, {name}.",
        "⏳ **TIME WASTED:** You just wasted time guessing, {name}. Stop. Click below to get the answer.",
        "🔌 **UNPLUGGED:** You are not connected, {name}. Click below to connect to the source.",
        "🔦 **DARKNESS:** You are in the dark, {name}. Turn on the light. Click below to find the switch.",
        "🗑️ **TRASH:** That MSA CODE is garbage data, {name}. Give us gold. Click below to find the gold.",
        "🚩 **FLAGGED:** Your attempt has been flagged as incorrect, {name}. Correct your course. Click below.",
        "📉 **DECLINED:** Your transaction was declined, {name}. Insufficient knowledge. Click below to deposit knowledge.",
        "🚪 **WRONG DOOR:** That key doesn't fit, {name}. Click below to find the right key.",
        "🔇 **SILENCE:** The system is silent, {name}. Your MSA CODE did not wake it up. Click below to find the voice.",
        "👻 **GHOST:** You are chasing ghosts, {name}. That MSA CODE is dead. Click below to find the living MSA CODE.",
        "🌪️ **MIRAGE:** That MSA CODE is a mirage, {name}. It looks real, but it's not. Click below to find the oasis.",
        "🕸️ **VOID:** You entered the void, {name}. There is nothing here. Click below to find the substance.",
        "⚡ **STATIC:** All we hear is static, {name}. Tune your frequency. Click below to find the signal.",
        "🐛 **GLITCH:** You caused a glitch in the matrix, {name}. That MSA CODE is a bug. Click below to fix the MSA CODE.",
        "🛑 **HALT:** Security protocol engaged, {name}. MSA CODE unrecognized. Click below to clear your status.",
        "🧊 **FROZEN:** Your progress is frozen, {name}. That MSA CODE is ice. Click below to find the fire.",
        "🎭 **MASK:** That MSA CODE is wearing a mask, {name}. Take it off. Click below to find the face.",
        "🕰️ **ECHO:** You are just an echo, {name}. We need the source. Click below to become the source."
    ]
}

# ==========================================
# 🆔 MSA+ ID ALLOCATION SYSTEM
# ==========================================

def get_next_msa_id() -> tuple[str, int]:
    """Get the next available MSA+ ID by finding gaps in sequence"""
    # Get all allocated MSA numbers sorted ascending
    allocated_ids = list(col_msa_ids.find({}, {"msa_number": 1}).sort("msa_number", 1))
    allocated_numbers = [doc["msa_number"] for doc in allocated_ids]
    
    if not allocated_numbers:
        # First user ever
        next_number = 0
    else:
        # Find first gap in sequence
        next_number = None
        for i in range(len(allocated_numbers)):
            if allocated_numbers[i] != i:
                next_number = i
                break
        
        # If no gap found, use next sequential number
        if next_number is None:
            next_number = len(allocated_numbers)
    
    # Format as MSA000000000 (9 digits)
    msa_id = f"MSA{next_number:09d}"
    return msa_id, next_number

def allocate_msa_id(user_id: int, username: str, first_name: str) -> str:
    """Allocate MSA+ ID to a user (prevents duplicates)"""
    # Check if user already has an MSA+ ID
    existing = col_msa_ids.find_one({"user_id": user_id})
    if existing:
        logger.info(f"User {user_id} already has MSA+ ID: {existing['msa_id']}")
        return existing['msa_id']
    
    # Get next available ID
    msa_id, msa_number = get_next_msa_id()
    
    # Insert into database
    col_msa_ids.insert_one({
        "user_id": user_id,
        "msa_id": msa_id,
        "msa_number": msa_number,
        "assigned_at": now_local(),
        "username": username,
        "first_name": first_name
    })
    
    # Update user verification record
    update_verification_status(user_id, msa_id=msa_id)
    
    logger.info(f"Allocated {msa_id} to user {user_id} ({first_name})")
    return msa_id

def get_user_msa_id(user_id: int) -> str | None:
    """Get user's MSA+ ID from database"""
    msa_record = col_msa_ids.find_one({"user_id": user_id})
    if msa_record:
        return msa_record['msa_id']
    return None

def get_verification_keyboard(user_id: int, user_data: dict, show_all: bool = True) -> InlineKeyboardMarkup:
    """Create inline keyboard - All 3 are URL buttons, no callbacks"""
    if show_all:
        # For NEW users - show all 3 buttons (all caps)
        keyboard = [
            [InlineKeyboardButton(text="📺 YOUTUBE", url=YOUTUBE_LINK)],
            [InlineKeyboardButton(text="📸 INSTAGRAM", url=INSTAGRAM_LINK)],
            [InlineKeyboardButton(text="📢 TELEGRAM VAULT", url=CHANNEL_LINK)]
        ]
    else:
        # For OLD users who left - show ONLY rejoin button (all caps)
        keyboard = [
            [InlineKeyboardButton(text="📢 REJOIN VAULT", url=CHANNEL_LINK)]
        ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ==========================================
# 🔒 VAULT ACCESS CONTROL MIDDLEWARE
# ==========================================

async def check_if_banned(user_id: int) -> dict | None:
    """Check if user is banned. Returns ban doc if banned, None otherwise. Auto-unbans expired temporary bans."""
    try:
        # Only check bans that apply to Bot 8 (exclude bans scoped to bot10 admin panel only)
        ban_doc = col_banned_users.find_one({"user_id": user_id, "scope": {"$ne": "bot10"}})
        
        if ban_doc:
            # Check if it's a temporary ban that has expired
            if ban_doc.get('ban_type') == 'temporary' and ban_doc.get('ban_expires'):
                if now_local() > ban_doc['ban_expires']:
                    # Temporary ban has expired - auto-unban
                    col_banned_users.delete_one({"user_id": user_id})
                    logger.info(f"Auto-unbanned user {user_id} - temporary ban expired")
                    return None  # User is no longer banned
            
            # Ban is still active
            return ban_doc
        
        return None
    except Exception as e:
        logger.error(f"Ban check failed for user {user_id}: {e}")
        return None

async def require_vault_access(handler_func):
    """Decorator to ensure user is in vault before accessing any feature"""
    async def wrapper(message_or_callback):
        # Get user ID
        if hasattr(message_or_callback, 'from_user'):
            user_id = message_or_callback.from_user.id
            user_name = message_or_callback.from_user.first_name or "User"
        else:
            return
        
        # Check if user is in vault (real-time check)
        is_in_vault = await check_channel_membership(user_id)
        
        if not is_in_vault:
            # User not in vault - block access and show rejoin message
            user_data = get_user_verification_status(user_id)
            was_ever_verified = user_data.get('ever_verified', False)
            
            if was_ever_verified:
                # Old user who left
                await message_or_callback.answer(
                    f"🔒 **{user_name}, YOU LEFT THE VAULT**\n\n"
                    f"You had access. You gave it up.\n"
                    f"Now the system won't let you in.\n\n"
                    f"**You know the drill:**\n"
                    f"No vault = No features. No exceptions.\n\n"
                    f"💎 **Get back in. Restore your status.**",
                    reply_markup=get_verification_keyboard(user_id, user_data, show_all=False),
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                # New user who never joined
                await message_or_callback.answer(
                    f"🔒 **{user_name}, ACCESS LOCKED**\n\n"
                    f"The **MSA NODE Vault** is not optional.\n"
                    f"It's the gateway. It's the requirement.\n\n"
                    f"You want the tools? Join the vault.\n"
                    f"You want the content? Join the vault.\n"
                    f"You want to compete? Join the vault.\n\n"
                    f"✨ **Join now. Unlock everything.**",
                    reply_markup=get_verification_keyboard(user_id, user_data, show_all=True),
                    parse_mode=ParseMode.MARKDOWN
                )
            return
        
        # User is in vault - allow access
        await handler_func(message_or_callback)
    
    return wrapper

# ==========================================
# 📋 MENU KEYBOARDS
# ==========================================
def get_main_menu():
    """Create the main menu keyboard with 5 buttons"""
    keyboard = [
        [KeyboardButton(text="📊 DASHBOARD")],
        [KeyboardButton(text="🔍 SEARCH CODE")],
        [KeyboardButton(text="📜 RULES")],
        [KeyboardButton(text="📖 AGENT GUIDE")],
        [KeyboardButton(text="📞 SUPPORT")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_user_menu(user_id: int):
    """Create menu based on user's ban/suspension status"""
    from aiogram.types import ReplyKeyboardRemove
    
    # Check if user is banned (only bans that apply to Bot 8, not bot10-only admin bans)
    ban_doc = col_banned_users.find_one({"user_id": user_id, "scope": {"$ne": "bot10"}})
    
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        
        # Temporary ban: Show only SUPPORT button
        if ban_type == "temporary":
            keyboard = [[KeyboardButton(text="📞 SUPPORT")]]
            return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
        
        # Permanent ban: Hide all buttons (no keyboard)
        else:
            return ReplyKeyboardRemove()
    
    # Check suspended features
    suspend_doc = col_suspended_features.find_one({"user_id": user_id})
    
    if suspend_doc:
        suspended = suspend_doc.get("suspended_features", [])
        
        # Build menu excluding suspended features
        keyboard = []
        
        if "DASHBOARD" not in suspended:
            keyboard.append([KeyboardButton(text="📊 DASHBOARD")])
        if "SEARCH_CODE" not in suspended:
            keyboard.append([KeyboardButton(text="🔍 SEARCH CODE")])
        if "RULES" not in suspended:
            keyboard.append([KeyboardButton(text="📜 RULES")])
        if "GUIDE" not in suspended:
            keyboard.append([KeyboardButton(text="📖 AGENT GUIDE")])
        
        # Always show SUPPORT
        keyboard.append([KeyboardButton(text="📞 SUPPORT")])
        
        if keyboard:
            return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
        else:
            # If all features suspended, show only support
            keyboard = [[KeyboardButton(text="📞 SUPPORT")]]
            return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    
    # No restrictions: return full menu
    return get_main_menu()

def get_banned_user_keyboard(ban_type="permanent"):
    """Create keyboard for banned users based on ban type"""
    from aiogram.types import ReplyKeyboardRemove
    
    if ban_type == "temporary":
        # Temporary ban: Show only SUPPORT button
        keyboard = [[KeyboardButton(text="📞 SUPPORT")]]
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
    else:
        # Permanent ban: No buttons at all
        return ReplyKeyboardRemove()

def get_support_menu():
    """Create the support menu with issue categories"""
    keyboard = [
        [KeyboardButton(text="📄 PDF/LINK ISSUES")],
        [KeyboardButton(text="🔧 TROUBLESHOOTING")],
        [KeyboardButton(text="❓ OTHER ISSUES")],
        [KeyboardButton(text="🎫 RAISE A TICKET")],
        [KeyboardButton(text="🔙 BACK TO MENU")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_resolution_keyboard():
    """Create resolution keyboard after showing help"""
    keyboard = [
        [KeyboardButton(text="✅ RESOLVED")],
        [KeyboardButton(text="🔍 CHECK OTHER")],
        [KeyboardButton(text="🎫 RAISE A TICKET")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# ==========================================
# 🎬 HANDLERS
# ==========================================

# ==========================================
# 🧩 DYNAMIC PAYLOAD PARSING
# ==========================================
def parse_start_payload(payload: str):
    """
    Parse the start payload with strict validation and partial matching.
    Returns a dict with 'status' and 'data'.
    """
    if not payload:
        return {"status": "invalid", "data": None}

    # 1. Try Exact Match
    # Format: CODE_source_NAME (e.g., cGdBXAN9_ig_PF2)
    match = re.search(r"^(.+)_(ig|yt)_(.+)$", payload)
    if match:
        return {
            "status": "valid",
            "data": {
                "code": match.group(1),
                "source": match.group(2).lower(),
                "pdf_name": match.group(3)
            }
        }

    # 2. Try YT Code Prompt Match
    # Format: CODE_YTCODE (e.g., 80919449_YTCODE)
    match_yt = re.search(r"^(.+)_YTCODE$", payload)
    if match_yt:
        return {
            "status": "yt_code_prompt",
            "data": {
                "user_code": match_yt.group(1)
            }
        }
    
    # 3. Try IGCC Deep Link Match
    # Format: USERID_igcc_CCCODE (e.g. 84797415_igcc_CC1)
    match_igcc = re.search(r"^(.+)_igcc_(.+)$", payload)
    if match_igcc:
        return {
            "status": "igcc_deep_link",
            "data": {
                "user_id_ref": match_igcc.group(1),
                "cc_code": match_igcc.group(2)
            }
        }

    # 4. Try Partial/Broken Match (Source Detection)
    if "_ig_" in payload.lower():
        return {"status": "broken_ig", "data": None}
    
    if "_yt_" in payload.lower():
        return {"status": "broken_yt", "data": None}
        
    if "ytcode" in payload.lower():
        return {"status": "broken_yt_prompt", "data": None}

    return {"status": "invalid", "data": payload}

def generate_alphanumeric(length=8):
    """Generate random alphanumeric code"""
    return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def generate_digits(length=8):
    """Generate random digit code"""
    return "".join(random.choice(string.digits) for _ in range(length))

async def ensure_pdf_codes(pdf):
    """Ensure PDF has all start codes - creates them if missing"""
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

async def show_access_denied_animation(message: types.Message, user_id: int, payload: str = "", expected: str = ""):
    """Reusable ACCESS DENIED animation and message"""
    # 🎬 ANIMATION: ACCESS DENIED
    msg = await message.answer("🚫")
    await asyncio.sleep(ANIM_MEDIUM)
    await msg.edit_text("🚫 **SYSTEM ALERT**", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_SLOW)
    await msg.edit_text("🔒 **SECURITY BREACH DETECTED**", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_SLOW)
    await safe_delete_message(msg)

    # Error message
    error_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 INSTAGRAM", url=INSTAGRAM_LINK)],
        [InlineKeyboardButton(text="📺 YOUTUBE", url=YOUTUBE_LINK)]
    ])
    await message.answer(
        f"⚠️ **ACCESS DENIED: INVALID LINK**\n\n"
        f"The link you provided is **unrecognized** by the Agent.\n"
        f"Please obtain the **CORRECT LINK** from our official channels:\n\n"
        f"📸 **Instagram**: For exclusive Deep Links.\n"
        f"📺 **YouTube**: For Video Access.\n\n"
        f"OR enter a valid **MSA CODE** manually.\n\n"
        f"💬 Need help? Check vault announcements",
        reply_markup=error_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Log security breach
    if payload and expected:
        logger.warning(f"SECURITY BREACH: User {user_id} tried payload '{payload}' but expected '{expected}'")
    else:
        logger.warning(f"SECURITY BREACH: User {user_id} tried invalid link")

def get_pdf_content(index: int):
    """Fetch PDF content by index from bot9_pdfs collection"""
    return col_pdfs.find_one({"index": index})

# ==========================================
# 🎬 HANDLERS
# ==========================================

@dp.message(CommandStart())
@rate_limit(cooldown=2.0)
@anti_spam("start")
async def cmd_start(message: types.Message, state: FSMContext):
    
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    user_id = message.from_user.id
    user_name = message.from_user.first_name or "User"
    
    # ==========================================
    # 🚫 BAN CHECK - Highest Priority
    # ==========================================
    ban_doc = await check_if_banned(user_id)
    if ban_doc:
        banned_at = ban_doc.get("banned_at", now_local())
        ban_type = ban_doc.get("ban_type", "permanent")
        
        # Build ban message based on type
        if ban_type == "temporary" and ban_doc.get("ban_expires"):
            ban_expires = ban_doc["ban_expires"]
            time_diff = ban_expires - now_local()
            
            # Calculate time remaining
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
            
            # Calculate progress bar
            ban_duration_hours = ban_doc.get("ban_duration_hours", 24)
            total_seconds = ban_duration_hours * 3600
            elapsed_seconds = total_seconds - time_diff.total_seconds()
            progress_percentage = max(0, min(100, (elapsed_seconds / total_seconds) * 100))
            
            # Generate progress bar (20 blocks)
            filled = int((progress_percentage / 100) * 20)
            empty = 20 - filled
            progress_bar = "▰" * filled + "▱" * empty
            
            ban_message = (
                "⏰ **TEMPORARY RESTRICTION**\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Hi {user_name}, your account access is temporarily limited.\n\n"
                f"🕐 **Ban Start:** {banned_at.strftime('%b %d at %I:%M %p')}\n"
                f"🕐 **Ban Expires:** {ban_expires.strftime('%b %d at %I:%M %p')}\n"
                f"⏳ **Time Remaining:** {time_remaining}\n\n"
                f"**Ban Progress**\n"
                f"`[{progress_bar}]` {progress_percentage:.0f}%\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ **Auto-Unban:** Your access will be automatically restored when the timer expires.\n\n"
                f"⚠️ **Support:** You can use **📞 SUPPORT** to contact us.\n\n"
                f"📋 **Note:** Review community guidelines to avoid future restrictions."
            )
        else:
            ban_message = (
                "🚫 **ACCOUNT PERMANENTLY BANNED**\n\n"
                f"Hi {user_name}, your account has been permanently banned.\n\n"
                f"🕐 **Banned:** {banned_at.strftime('%b %d, %Y at %I:%M:%S %p')}\n\n"
                "⚠️ **All features and buttons are disabled.**\n"
                "This action is permanent."
            )
        
        await message.answer(
            ban_message,
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"🚫 Banned user {user_id} ({ban_type}) attempted to access bot")
        return
    
    args = message.text.split()
    payload = args[1] if len(args) > 1 else None
    
    # Check for Dynamic Payload (Priority)
    parse_result = parse_start_payload(payload)
    
    if parse_result["status"] == "valid":
        parsed_data = parse_result["data"]
        input_code = parsed_data.get("code", "")
        source = parsed_data['source'] # 'ig' or 'yt'
        
        # 1. Fetch Content by CODE (not by index)
        # Determine which DB field to check based on source
        if source == "ig":
            pdf_data = col_pdfs.find_one({"ig_start_code": input_code})
        elif source == "yt":
            pdf_data = col_pdfs.find_one({"yt_start_code": input_code})
        else:
            pdf_data = None
        
        if pdf_data:
            # ✅ CODE FOUND - Now VALIDATE ALL REQUIRED FIELDS
            # 🔒 STRICT FIELD VALIDATION
            # Check if PDF has ALL required data before allowing access
            
            missing_fields = []
            
            # Check for Affiliate Link
            if not pdf_data.get('affiliate_link'):
                missing_fields.append("Affiliate Link")
            
            # Check for YT Title
            if not pdf_data.get('yt_title'):
                missing_fields.append("YouTube Title")
            
            # Check for YT Link
            if not pdf_data.get('yt_link'):
                missing_fields.append("YouTube Link")
            
            # Check for MSA Code
            if not pdf_data.get('msa_code'):
                missing_fields.append("MSA Code")
            
            # If any field is missing, deny access
            if missing_fields:
                error_msg = (
                    "⚠️ **LINK INVALID**\n\n"
                    f"{user_name}, this content is no longer available because required information is missing:\n\n"
                )
                for field in missing_fields:
                    error_msg += f"• {field}\n"
                
                error_msg += (
                    "\n━━━━━━━━━━━━━━━━━━━━\n\n"
                    "🛠️ **Status:** This link has been disabled.\n\n"
                    "📞 **Support:** Use the Menu button to access support if you need assistance."
                )
                
                logger.warning(f"🚫 Deep link denied for user {user_id}: Missing fields {missing_fields} for PDF '{pdf_data.get('name')}'")
                
                await message.answer(
                    error_msg,
                    reply_markup=get_main_menu(),
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # ✅ ALL REQUIRED FIELDS PRESENT - Now validate FULL payload structure
            # 🔒 STRICT FULL LINK VALIDATION
            # Reconstruct the expected payload and compare with input
            
            # Sanitize PDF name (same logic as bot9.py)
            pdf_name = pdf_data.get("name", "")
            sanitized_name = re.sub(r'[^a-zA-Z0-9]', '_', pdf_name)
            sanitized_name = re.sub(r'_+', '_', sanitized_name).strip('_')
            
            # Build expected payload
            expected_payload = f"{input_code}_{source}_{sanitized_name}"
            
            # STRICT COMPARISON: Must match EXACTLY
            if payload != expected_payload:
                # 🚫 INVALID LINK (Tampered suffix/structure)
                await show_access_denied_animation(message, user_id, payload, expected_payload)
                return
            
            # ✅ FULL VALIDATION PASSED
              # 📊 TRACK CLICK ANALYTICS
            try:
                # Get or allocate MSA+ ID for user
                username = message.from_user.username or "unknown"
                first_name = message.from_user.first_name or "User"
                msa_id = get_user_msa_id(user_id)
                if not msa_id:
                    msa_id = allocate_msa_id(user_id, username, first_name)
                
                if source == "ig":
                    # Increment IG start clicks counter and update timestamp
                    col_pdfs.update_one(
                        {"_id": pdf_data["_id"]},
                        {
                            "$inc": {"ig_start_clicks": 1, "clicks": 1},
                            "$set": {"last_ig_click": now_local(), "last_clicked_at": now_local()}
                        }
                    )
                    # Track user source permanently (first start only — never overwritten)
                    track_user_source(user_id, "IG", username, first_name, msa_id)
                elif source == "yt":
                    # Increment YT start clicks counter and update timestamp
                    col_pdfs.update_one(
                        {"_id": pdf_data["_id"]},
                        {
                            "$inc": {"yt_start_clicks": 1, "clicks": 1},
                            "$set": {"last_yt_click": now_local(), "last_clicked_at": now_local()}
                        }
                    )
                    # Track user source permanently (first start only — never overwritten)
                    track_user_source(user_id, "YT", username, first_name, msa_id)
                logger.info(f"📊 Analytics: User {user_id} clicked {source.upper()} link for PDF '{pdf_data.get('name')}'")
            except Exception as analytics_err:
                logger.error(f"⚠️ Analytics tracking failed: {analytics_err}")
            
            # ==========================================
            # 🔒 VAULT ACCESS CHECK — Block non-members
            # ==========================================
            is_in_vault = await check_channel_membership(user_id)
            if not is_in_vault:
                user_data = get_user_verification_status(user_id)
                was_ever_verified = user_data.get('ever_verified', False)
                vault_kb = get_verification_keyboard(user_id, user_data, show_all=not was_ever_verified)
                if was_ever_verified:
                    vault_msg = (
                        f"🔐 **{user_name}, THE VAULT IS CLOSED TO YOU**\n\n"
                        f"You clicked the link. The content is right there.\n"
                        f"But the system doesn't deliver to those who walked out.\n\n"
                        f"**You left the Vault.**\n"
                        f"That means you left your privileges at the door.\n\n"
                        f"💎 **One action separates you from everything:**\n"
                        f"Rejoin the Vault → Unlock full delivery. Instantly.\n\n"
                        f"*The content waits. The clock doesn't.*"
                    )
                else:
                    vault_msg = (
                        f"🔒 **{user_name}, ACCESS LOCKED**\n\n"
                        f"You found the link. You even clicked it.\n"
                        f"That tells us you're serious.\n\n"
                        f"**But the system only delivers to Vault members.**\n"
                        f"No vault = No content. No exceptions.\n\n"
                        f"✨ **The fix is simple:**\n"
                        f"Join the Vault → Come back → Get everything."
                    )
                await message.answer(
                    vault_msg,
                    reply_markup=vault_kb,
                    parse_mode=ParseMode.MARKDOWN
                )
                await message.answer(
                    "🔒 Menu locked until you rejoin the Vault.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return

            # =================================================================================
            # 🚀 EXACT SEARCH CODE DELIVERY FORMAT (Dynamic Cross-Platform)
            # =================================================================================
            
            # 1. PREPARE CONTENT
            first_name = message.from_user.first_name
            
            # PDF Title
            pdf_title_template = random.choice(CONTENT_PACKS["PDF_TITLES"])
            try:
                pdf_title_text = pdf_title_template.format(name=first_name)
            except:
                pdf_title_text = pdf_title_template
            
            # Affiliate Title
            aff_title_text = random.choice(CONTENT_PACKS["AFFILIATE_TITLES"])
            
            # Dynamic Cross-Platform Logic for Text AND Final Button
            if source == 'ig':
                # IG -> YT (Use YT_VIDEO_TITLES for text, YT_CODES_BUTTONS for action)
                msa_code_template = random.choice(CONTENT_PACKS["YT_VIDEO_TITLES"])
                target_btn_text = random.choice(CONTENT_PACKS["YT_CODES_BUTTONS"])
                target_link = YOUTUBE_LINK
                footer_suffix = "| Source: IG -> YT" 
                
            elif source == 'yt':
                # YT -> IG (Use IG_VIDEO_TITLES for text, IG_CODES_BUTTONS for action)
                msa_code_template = random.choice(CONTENT_PACKS["IG_VIDEO_TITLES"])
                target_btn_text = random.choice(CONTENT_PACKS["IG_CODES_BUTTONS"])
                target_link = INSTAGRAM_LINK
                footer_suffix = "| Source: YT -> IG"
                
            else:
                # Fallback (legacy/unknown)
                msa_code_template = random.choice(CONTENT_PACKS["MSACODE"])
                target_btn_text = "📢 JOIN VAULT"
                target_link = CHANNEL_LINK
                footer_suffix = ""

            # Format the selected MSA Code text
            try:
                msa_code_text = msa_code_template.format(name=first_name)
            except:
                msa_code_text = msa_code_template
                
            # Links
            pdf_link = pdf_data.get("link", BOT_FALLBACK_LINK)
            affiliate_link = pdf_data.get("affiliate_link", BOT_FALLBACK_LINK)

            # 🎬 ANIMATION: DECRYPTION
            msg = await message.answer("◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text("◻️ ◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text("◻️ ◻️ ◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text(f"📸 **CONNECTING SOURCE...**", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_PAUSE)
            await msg.edit_text(f"🔓 **DECRYPTING ASSET...**", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_PAUSE)
            await msg.edit_text(f"✅ **IDENTITY CONFIRMED: {first_name}**\n\n`Secure Delivery In Progress...`", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_DELAY)
            await safe_delete_message(msg)

            # ---------------------------------------------------------
            # 1️⃣ MESSAGE 1: PDF DELIVERY
            # ---------------------------------------------------------
            pdf_btn_text = random.choice(CONTENT_PACKS["PDF_BUTTONS"])
            pdf_footer_template = random.choice(CONTENT_PACKS["PDF_FOOTERS"])
            try:
                pdf_footer_text = pdf_footer_template.format(name=first_name)
            except:
                pdf_footer_text = pdf_footer_template
                
            pdf_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=pdf_btn_text, url=pdf_link)]])
            await message.answer(
                f"{pdf_title_text}\n\n`{pdf_footer_text}`",
                reply_markup=pdf_kb,
                parse_mode=ParseMode.MARKDOWN
            )

            # ⏳ DOT ANIMATION 1
            wait_msg = await message.answer("▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await safe_delete_message(wait_msg)

            # ---------------------------------------------------------
            # 2️⃣ MESSAGE 2: AFFILIATE OPPORTUNITY
            # ---------------------------------------------------------
            if affiliate_link:
                # Select Random Affiliate Footer
                aff_footer_template = random.choice(CONTENT_PACKS["AFFILIATE_FOOTERS"])
                try:
                    aff_footer_text = aff_footer_template.format(name=first_name)
                except:
                    aff_footer_text = aff_footer_template

                aff_btn_text = random.choice(CONTENT_PACKS["AFFILIATE_BUTTONS"])
                aff_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=aff_btn_text, url=affiliate_link)]])
                await message.answer(
                    f"{aff_title_text}\n\n━━━━━━━━━━━━━━━━\n`{aff_footer_text}`",
                    reply_markup=aff_kb,
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # ⏳ DOT ANIMATION 2
                wait_msg = await message.answer("▪️")
                await asyncio.sleep(ANIM_MEDIUM)
                await wait_msg.edit_text("▪️▪️")
                await asyncio.sleep(ANIM_MEDIUM)
                await wait_msg.edit_text("▪️▪️▪️")
                await asyncio.sleep(ANIM_MEDIUM)
                await safe_delete_message(wait_msg)

            # ---------------------------------------------------------
            # 3️⃣ MESSAGE 3: NETWORK / CROSS-PLATFORM
            # ---------------------------------------------------------
            # Select Random Affiliate Footer
            aff_footer_template = random.choice(CONTENT_PACKS["AFFILIATE_FOOTERS"])
            try:
                base_footer = aff_footer_template.format(name=first_name)
            except:
                base_footer = aff_footer_template
            
            final_footer = base_footer 

            # Select Random Affiliate Button
            aff_btn_text = random.choice(CONTENT_PACKS["AFFILIATE_BUTTONS"])
            if not affiliate_link or len(affiliate_link) < 5: 
                affiliate_link = BOT_FALLBACK_LINK

            network_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=target_btn_text, url=target_link)],
                [InlineKeyboardButton(text=aff_btn_text, url=affiliate_link)]
            ])
            
            await message.answer(
                f"{msa_code_text}\n\n`{final_footer}`",
                reply_markup=network_kb,
                parse_mode=ParseMode.MARKDOWN
            )
            
            logger.info(f"User {user_id} triggered dynamic start: Source={source}, Code={input_code}")
            return
        else:
            # 🚫 PDF NOT FOUND - Invalid Code
            await show_access_denied_animation(message, user_id)
            return

    # 🚫 ERROR HANDLING: BROKEN IG LINK
    elif parse_result["status"] == "broken_ig":
        await show_access_denied_animation(message, user_id)
        return

    # 🚫 ERROR HANDLING: BROKEN YT LINK
    elif parse_result["status"] == "broken_yt":
        await show_access_denied_animation(message, user_id)
        return

    # 🚫 ERROR HANDLING: INVALID / UNKNOWN SOURCE
    elif parse_result["status"] == "invalid" and payload:
        await show_access_denied_animation(message, user_id)
        return

    # 🎥 NEW FLOW: YT CODE PROMPT (Force MSA Code Entry)
    elif parse_result["status"] == "yt_code_prompt":
        # 🎬 ANIMATION: SOURCE VALIDATION
        msg = await message.answer("📡")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("📡 **CONNECTING TO SOURCE...**", parse_mode=ParseMode.MARKDOWN)
        await asyncio.sleep(ANIM_SLOW)
        await msg.edit_text("🔒 **SECURE CONNECTION ESTABLISHED**", parse_mode=ParseMode.MARKDOWN)
        await asyncio.sleep(ANIM_SLOW)
        await safe_delete_message(msg)

        # Prompt for MSA Code with Cancel button
        first_name = message.from_user.first_name
        
        cancel_kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ CANCEL")]],
            resize_keyboard=True,
            one_time_keyboard=False
        )
        
        await message.answer(
            f"🔒 **AUTHENTICATION REQUIRED**\n\n{first_name}, the agent is waiting.\nEnter your **MSA CODE** to decrypt the asset.\n\n*Precision is key.*\n\n`ENTER MSA CODE BELOW:`\n\n⚪️ _Press 'CANCEL' to cancel this operation._",
            reply_markup=cancel_kb,
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Set state to waiting for code
        await state.set_state(SearchCodeStates.waiting_for_code)
        # Set context flag: User came from YT, so we treat them as a YT source user
        await state.update_data(is_yt_flow=True)
        return
    
    # 🚫 ERROR HANDLING: BROKEN YT CODE PROMPT
    elif parse_result["status"] == "broken_yt_prompt":
        # Log the specific broken payload
        logger.warning(f"BROKEN YT PROMPT from {user_id}: {payload}")

        # 🎬 ANIMATION: ERROR DETECTION
        msg = await message.answer("⚠️")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("⚠️ **DETECTING ERROR...**", parse_mode=ParseMode.MARKDOWN)
        await asyncio.sleep(ANIM_SLOW)
        await msg.edit_text("⚙️ **BYPASSING SECURITY...**")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("⚡ **PROXY CONNECTION ESTABLISHED...**")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("🔍 **SEARCHING DATABASE...**")
        await asyncio.sleep(ANIM_MEDIUM)
        await msg.edit_text("⛔ **ERROR: ENCRYPTION KEY INVALID**")
        await asyncio.sleep(ANIM_SLOW)
        await msg.edit_text("⚠️ **ACCESS DENIED**")
        await asyncio.sleep(ANIM_SLOW)
        await safe_delete_message(msg)

        # Select Random Affiliate Footer
        aff_footer_template = random.choice(CONTENT_PACKS["AFFILIATE_FOOTERS"])
        try:
            error_footer = aff_footer_template.format(name=message.from_user.first_name)
        except:
            error_footer = aff_footer_template

        error_msg = (
            f"⚠️ **ACCESS DENIED: LINK FRACTURED**\n\n"
            f"The Neural Link you attempted to access is **INVALID**.\n"
            f"The agent cannot verify the requested Asset.\n\n"
            f"**DIAGNOSTIC:**\n"
            f"• Check the characters in your link.\n"
            f"• Ensure no digits are missing.\n"
            f"• Verify the source of your intelligence.\n\n"
            f"**PROTOCOL:**\n"
            f"Re-examine your data. Correct the vector. Execute again.\n\n"
            f"💬 Still stuck? Ask in vault channel\n\n"
            f"`{error_footer}`"
        )

        # Select Random Affiliate Button
        aff_btn_text = random.choice(CONTENT_PACKS["AFFILIATE_BUTTONS"])
        aff_link = BOT_FALLBACK_LINK

        error_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📸 GET CORRECT LINK", url=INSTAGRAM_LINK)]
        ])
        
        await message.answer(error_msg, reply_markup=error_kb, parse_mode=ParseMode.MARKDOWN)
        return
    
    # 📸 NEW FLOW: IGCC DEEP LINK (Instant Content + Upsell)
    elif parse_result["status"] == "igcc_deep_link":
        parsed_data = parse_result["data"]
        cc_code = parsed_data["cc_code"]
        user_id_ref = parsed_data["user_id_ref"]
        
        # Fetch Content
        ig_content = col_ig_content.find_one({"cc_code": cc_code})
        
        if ig_content:
            # ✅ ENSURE CODE EXISTS - Auto-generate if missing
            ig_content = await ensure_ig_cc_code(ig_content)
            
            # 🔒 STRICT FULL LINK VALIDATION
            # Reconstruct expected payload and compare
            db_start_code = ig_content.get("start_code", "")
            expected_payload = f"{db_start_code}_igcc_{cc_code}"
            
            # STRICT COMPARISON: Must match EXACTLY
            if not db_start_code or payload != expected_payload:
                # 🚫 INVALID LINK (Tampered or Mismatch)
                await show_access_denied_animation(message, user_id, payload, expected_payload)
                return
            
            # ✅ VALIDATION PASSED - Continue with content delivery
            # 📊 TRACK CLICK ANALYTICS
            user_name = message.from_user.first_name or "User"  # FIX: define user_name here
            try:
                # Get or allocate MSA+ ID for user
                username = message.from_user.username or "unknown"
                first_name = message.from_user.first_name or "User"
                msa_id = get_user_msa_id(user_id)
                if not msa_id:
                    msa_id = allocate_msa_id(user_id, username, first_name)
                
                # Increment IG CC clicks counter and update timestamp
                col_ig_content.update_one(
                    {"_id": ig_content["_id"]},
                    {
                        "$inc": {"ig_cc_clicks": 1},
                        "$set": {"last_ig_cc_click": now_local()}
                    }
                )
                
                # Track user source permanently (first start only — never overwritten)
                track_user_source(user_id, "IGCC", username, first_name, msa_id)
                
                logger.info(f"📊 Analytics: User {user_id} clicked IGCC link for '{ig_content.get('name')}'")
            except Exception as analytics_err:
                logger.error(f"⚠️ Analytics tracking failed: {analytics_err}")
            
            # ==========================================
            # 🔒 VAULT ACCESS CHECK — Block non-members
            # ==========================================
            is_in_vault = await check_channel_membership(user_id)
            if not is_in_vault:
                user_data = get_user_verification_status(user_id)
                was_ever_verified = user_data.get('ever_verified', False)
                vault_kb = get_verification_keyboard(user_id, user_data, show_all=not was_ever_verified)
                if was_ever_verified:
                    vault_msg = (
                        f"🔐 **{user_name}, THE VAULT IS CLOSED TO YOU**\n\n"
                        f"You clicked the link. The content is right there.\n"
                        f"But the system doesn't deliver to those who walked out.\n\n"
                        f"**You left the Vault.**\n"
                        f"That means you left your privileges at the door.\n\n"
                        f"💎 **One action separates you from everything:**\n"
                        f"Rejoin the Vault → Unlock full delivery. Instantly.\n\n"
                        f"*The content waits. The clock doesn't.*"
                    )
                else:
                    vault_msg = (
                        f"🔒 **{user_name}, ACCESS LOCKED**\n\n"
                        f"You found the link. You even clicked it.\n"
                        f"That tells us you're serious.\n\n"
                        f"**But the system only delivers to Vault members.**\n"
                        f"No vault = No content. No exceptions.\n\n"
                        f"✨ **The fix is simple:**\n"
                        f"Join the Vault → Come back → Get everything."
                    )
                await message.answer(
                    vault_msg,
                    reply_markup=vault_kb,
                    parse_mode=ParseMode.MARKDOWN
                )
                await message.answer(
                    "🔒 Menu locked until you rejoin the Vault.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return

            # 🎬 ANIMATION: ACCESSING CONTENT
            msg = await message.answer("◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text("◻️ ◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text("◻️ ◻️ ◻️")
            await asyncio.sleep(ANIM_FAST)
            await msg.edit_text(f"📸 **CONNECTING TO SOURCE...**", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_PAUSE)
            await msg.edit_text(f"🔓 **ACCESSING CONTENT...**", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(ANIM_PAUSE)
            await safe_delete_message(msg)

            # ---------------------------------------------------------
            # 1️⃣ MESSAGE 1: CONTENT DELIVERY
            # ---------------------------------------------------------
            await message.answer(f"{ig_content['name']}", parse_mode="Markdown")
            
            # ⏳ DOT ANIMATION 1
            wait_msg = await message.answer("▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await safe_delete_message(wait_msg)

            # ---------------------------------------------------------
            # 2️⃣ MESSAGE 2: AFFILIATE UPSELL
            # ---------------------------------------------------------
            # msg_text = random.choice(CONTENT_PACKS["IGCCC_CODES"]) # Removed per user request
            title_text = random.choice(CONTENT_PACKS["AFFILIATE_TITLES"])
            footer_template = random.choice(CONTENT_PACKS["AFFILIATE_FOOTERS"])
            try:
                footer_text = footer_template.format(name=user_name)
            except:
                footer_text = footer_template
                
            aff_msg = f"{title_text}\n\n`{footer_text}`"
            
            # Button: Affiliate Link Only
            aff_link = ig_content.get("affiliate_link", BOT_FALLBACK_LINK)
            if not aff_link: aff_link = BOT_FALLBACK_LINK
            
            # Helper to clean link (if somehow it's empty string)
            if len(aff_link) < 5: aff_link = BOT_FALLBACK_LINK

            aff_btn_text = random.choice(CONTENT_PACKS["AFFILIATE_BUTTONS"])
            kb_aff = [[InlineKeyboardButton(text=aff_btn_text, url=aff_link)]]
            
            await message.answer(aff_msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_aff), parse_mode="Markdown")
            
            # ⏳ DOT ANIMATION 2
            wait_msg = await message.answer("▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await wait_msg.edit_text("▪️▪️▪️")
            await asyncio.sleep(ANIM_MEDIUM)
            await safe_delete_message(wait_msg)

            # ---------------------------------------------------------
            # 3️⃣ MESSAGE 3: NETWORK CONNECTION (IG + YT)
            # ---------------------------------------------------------
            # Static Psychological "System" Message
            network_msg = (
                f"📡 **SYSTEM STATUS: ASSET SECURED**\n\n"
                f"{user_name}, the tool is in your hands.\n"
                f"But a tool without a master is just metal.\n\n"
                f"You are here to build an **EMPIRE**, not a hobby.\n"
                f"We provide the blueprints. You provide the labor.\n\n"
                f"📺 **YouTube**: THE BLUEPRINT (Strategy & Execution).\n"
                f"📸 **Instagram**: THE NETWORK (Connections & Alpha).\n\n"
                f"The game is rigged. We are teaching you how to play.\n"
                f"**🚀 GET IN THE GAME NOW, {user_name}. Before it's too late.**"
            )
            
            # Select Random Affiliate Footer
            aff_footer_template = random.choice(CONTENT_PACKS["AFFILIATE_FOOTERS"])
            try:
                network_footer = aff_footer_template.format(name=user_name)
            except:
                network_footer = aff_footer_template
            
            final_network_msg = f"{network_msg}\n\n`{network_footer}`"
            
            # Select Random Affiliate Button & Link
            aff_btn_text = random.choice(CONTENT_PACKS["AFFILIATE_BUTTONS"])
            aff_link = ig_content.get("affiliate_link", BOT_FALLBACK_LINK)
            if not aff_link or len(aff_link) < 5: aff_link = BOT_FALLBACK_LINK

            kb_network = [
                [
                    InlineKeyboardButton(text="📸 EXPLORE MORE IG", url=INSTAGRAM_LINK),
                    InlineKeyboardButton(text="▶️ EXPLORE MORE YT", url=YOUTUBE_LINK)
                ]
            ]
            
            await message.answer(final_network_msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_network), parse_mode="Markdown")

            logger.info(f"User {user_id} triggered IGCC deep link for {cc_code}")
            return
        else:
            # 🚫 IG Content not found
            await show_access_denied_animation(message, user_id)
            return

    # Fallback to Standard Flow (Welcome / Verification)
    
    # Always show animation first for everyone
    # Step 1: Initial box
    msg = await message.answer("◻️")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Step 2: Loading boxes
    await msg.edit_text("◻️ ◻️")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Step 3: Full boxes
    await msg.edit_text("◻️ ◻️ ◻️")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Step 4: System activation
    await msg.edit_text("🔒 **AUTHENTICATING**\n\n`Verifying identity...`", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_SLOW)
    
    # Step 5: Identity confirmed
    await msg.edit_text(f"✅ **VERIFIED**\n\n`Welcome, {user_name}`", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_SLOW)
    
    # Step 6: Interface loading
    await msg.edit_text("⚙️ **INITIALIZING**\n\n`Loading workspace...`", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_PAUSE)
    
    # Now check verification status
    user_data = get_user_verification_status(user_id)
    
    # ALWAYS check if user is in vault channel (real-time check)
    is_in_vault = await check_channel_membership(user_id)
    
    # Update vault status in database based on real-time check
    update_verification_status(user_id, vault_joined=is_in_vault)
    
    # Check if user was EVER verified before (old user detection)
    was_ever_verified = user_data.get('ever_verified', False)
    
    # Verification = Only vault membership (no YT/IG tracking)
    all_verified = is_in_vault
    
    # If not verified (not in vault) AND this is a NEW user (never verified before)
    if not all_verified and not was_ever_verified:
        join_text = f"""
✨ **{user_name}, Welcome to Your New Journey!**

You've just taken the first step into something **extraordinary**. The MSA NODE Family isn't just a community—it's a movement of **visionaries, creators, and leaders** shaping the future.

━━━━━━━━━━━━━━━━━━━━

**🌟 Your Gateway to Excellence:**

📺 **YouTube** → Master strategies that move markets
📸 **Instagram** → Exclusive insights & real-time updates  
📢 **Telegram Vault** → Your **VIP access pass** to premium content

━━━━━━━━━━━━━━━━━━━━

**💎 Here's What Happens Next:**

1️⃣ Tap **📢 Telegram Vault** below
2️⃣ Step into our exclusive inner circle
3️⃣ **Instant verification** → We'll roll out the red carpet!

**🚀 Your transformation starts now. Are you ready?**
"""
        verification_msg = await msg.edit_text(
            join_text,
            reply_markup=get_verification_keyboard(user_id, user_data),
            parse_mode=ParseMode.MARKDOWN
        )
        # Hide menu keyboard for non-vault users
        await message.answer(
            "🔒 No access to menu and features",
            reply_markup=ReplyKeyboardRemove()
        )
        # Store verification message ID for later deletion
        update_verification_status(user_id, verification_msg_id=verification_msg.message_id)
        return
    
    # If not verified but WAS verified before (old user who left), just tell them to rejoin
    if not all_verified and was_ever_verified:
        await msg.edit_text(
            f"👋 **{user_name}, We've Missed You!**\n\nYour seat in the Telegram Vault is still reserved, waiting for your return.\n\n💎 **Everything you left behind?** Still yours.\n🎯 **Your community?** Still here for you.\n\n**One tap. Full access restored. Welcome home.**",
            reply_markup=get_verification_keyboard(user_id, user_data, show_all=False),
            parse_mode=ParseMode.MARKDOWN
        )
        # Hide menu keyboard for old users too
        await message.answer(
            "🔒 No access to menu and features",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    
    # User is verified - show welcome interface
    # Mark as verified if not already
    if not user_data.get('verified'):
        update_verification_status(user_id, verified=True)
    
    # Final: Enhanced premium interface with ONLINE status
    welcome_text = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━
   🤖 **MSA NODE AGENT**
   🟢 **SYSTEM ONLINE**
━━━━━━━━━━━━━━━━━━━━━━━━━━━

Welcome back, **{user_name}** 👋

⭐ **PREMIUM ACCESS GRANTED**
━━━━━━━━━━━━━━━━━━━━━━━━━━━

📍 **YOUR STATUS**

✅ **Verified Member**
🔓 **Full Access Enabled**
💎 **MSA NODE Elite**
🆔 **Member ID**: Active

━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 **AGENT SERVICES**

📊 **DASHBOARD**
   Your profile, stats & live announcements

🔍 **SEARCH CODE**
   Unlock content with your MSA CODES

📜 **RULES**
   Community code of conduct

📖 **AGENT GUIDE**
   Full manual — how to use MSA NODE Agent

━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 **QUICK START:**
• Use the menu buttons below
• Tap links from videos — content auto-delivers
• Enter codes manually via **SEARCH CODE**
• Check **DASHBOARD** for your live stats

━━━━━━━━━━━━━━━━━━━━━━━━━━━

🛡️ **Secure** • ⚡ **Fast** • 🎯 **Reliable**

📞 Need help? Use **SUPPORT** anytime

_Select a service from the menu ⬇️_
"""
    
    await safe_delete_message(msg)
    await message.answer(
        welcome_text,
        reply_markup=get_user_menu(user_id),
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Check for deep link payload (Legacy check or fallback)
    if payload == "80919449_YTCODE":
        # Track user source for bot10 broadcasts
        try:
            # Get or allocate MSA+ ID for user
            username = message.from_user.username or "unknown"
            first_name = message.from_user.first_name or "User"
            msa_id = get_user_msa_id(user_id)
            if not msa_id:
                msa_id = allocate_msa_id(user_id, username, first_name)
            
            # Track user source permanently (first start only — never overwritten)
            track_user_source(user_id, "YTCODE", username, first_name, msa_id)
        except Exception as track_err:
            logger.error(f"⚠️ Bot10 user tracking failed: {track_err}")
        
        # Auto-trigger Search Code prompt
        await asyncio.sleep(ANIM_SLOW)
        await message.answer(
            "🔑 **ENTER MSA CODE**\n\nTo access the Blueprint, please type the unique **MSA CODE** from the video below.\n\n`Example: MSA001`",
            parse_mode=ParseMode.MARKDOWN
        )
        await state.set_state(SearchCodeStates.waiting_for_code)
        logger.info(f"User {user_id} triggered via YTCODE deep link")
    
    logger.info(f"User {user_id} started with premium interface")

# ==========================================
# 🎉 AUTO-WELCOME ON VAULT JOIN
# ==========================================

@dp.chat_member()
async def handle_vault_join(event: ChatMemberUpdated):
    """Detect when user joins vault and auto-send welcome message"""
    # Check if this is the vault channel
    if event.chat.id != CHANNEL_ID:
        return
    
    # Check if user joined (status changed from not member to member)
    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status
    
    # Detect join: was not in channel, now in channel
    if old_status in ["left", "kicked"] and new_status in ["member", "administrator", "creator"]:
        user_id = event.from_user.id
        user_name = event.from_user.first_name or "User"

        # ==========================================
        # 🛑 MAINTENANCE MODE CHECK (Chat Member)
        # ==========================================
        try:
            settings = col_bot8_settings.find_one({"setting": "maintenance_mode"})
            if settings and settings.get("value", False) and user_id != OWNER_ID:
                # Maintenance is ON — update DB status but skip welcome messages
                update_verification_status(user_id, vault_joined=True, verified=True, ever_verified=True, rejoin_msg_id=None)
                username = event.from_user.username or "unknown"
                allocate_msa_id(user_id, username, user_name)
                try:
                    await bot.send_message(
                        user_id,
                        f"👤 **Dear {user_name},**\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "🔧 **MSA NODE AGENT — SYSTEM UPGRADE**\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "You've successfully joined the vault! 🎉\n\n"
                        "However, the Agent is currently undergoing a **premium infrastructure upgrade**. "
                        "Your membership is saved — just come back once we're online.\n\n"
                        "⏳ **Status:** Coming back online very soon.\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "Thank you for your patience.\n\n"
                        "_— MSA Node Systems_",
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    logger.error(f"Failed to send maintenance message to vault joiner {user_id}: {e}")
                logger.info(f"🛑 Maintenance: Vault join by {user_id} — saved but no welcome sent.")
                return
        except Exception as e:
            logger.error(f"Error checking maintenance mode in vault join handler: {e}")
        
        # Get user data to check for message IDs
        user_data = get_user_verification_status(user_id)
        verification_msg_id = user_data.get('verification_msg_id')
        rejoin_msg_id = user_data.get('rejoin_msg_id')
        
        # Update verification status and mark as EVER verified (for old user detection)
        update_verification_status(user_id, vault_joined=True, verified=True, ever_verified=True, rejoin_msg_id=None)
        
        # Allocate MSA+ ID if not already assigned
        username = event.from_user.username or "unknown"
        msa_id = allocate_msa_id(user_id, username, user_name)
        
        # Delete the verification message if it exists
        if verification_msg_id:
            try:
                await bot.delete_message(user_id, verification_msg_id)
                logger.info(f"Deleted verification message {verification_msg_id} for user {user_id}")
            except Exception as e:
                logger.error(f"Failed to delete verification message: {e}")
        
        # Delete the rejoin message if it exists (user rejoined after leaving)
        if rejoin_msg_id:
            try:
                await bot.delete_message(user_id, rejoin_msg_id)
                logger.info(f"Deleted rejoin message {rejoin_msg_id} for user {user_id}")
            except Exception as e:
                logger.error(f"Failed to delete verification message: {e}")
        
        # Send welcome message to user's DM
        try:
            await bot.send_message(
                user_id,
                f"🎉 **{user_name}, You're In!**\n\n✨ **Verification Complete** → Your journey begins this very moment.\n\n━━━━━━━━━━━━━━━━━━━━\n\n🆔 **Your MSA+ ID**: `{msa_id}`\n💎 **Premium Access**: Unlocked\n🏆 **Elite Community**: You're now among the visionaries\n🚀 **Exclusive Content**: At your fingertips\n\n**Your dashboard awaits.**\n\n━━━━━━━━━━━━━━━━━━━━\n\n*Welcome home, {user_name}. This is where legends are made.* ⚡",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Send menu keyboard immediately
            await bot.send_message(
                user_id,
                "👇 **Select a service from the menu below to begin: Just say a word!**",
                reply_markup=get_user_menu(user_id),
                parse_mode=ParseMode.MARKDOWN
            )
            
            logger.info(f"Auto-welcomed user {user_id} after vault join")
        except Exception as e:
            logger.error(f"Failed to send welcome message to {user_id}: {e}")
    
    # Detect leave: was in channel, now not in channel
    elif old_status in ["member", "administrator", "creator"] and new_status in ["left", "kicked"]:
        user_id = event.from_user.id
        user_name = event.from_user.first_name or "User"
        
        # Check if user exists in database (not permanently deleted)
        existing_user = col_user_verification.find_one({"user_id": user_id})
        
        if not existing_user:
            # User was permanently deleted - don't send any message or update anything
            logger.info(f"User {user_id} left vault but was permanently deleted - no action taken")
            return
        
        # Update status - user left vault
        update_verification_status(user_id, vault_joined=False, verified=False)
        
        # Get user data for keyboard
        user_data = get_user_verification_status(user_id)
        
        # Send instant rejoin message with button and store message ID
        try:
            rejoin_msg = await bot.send_message(
                user_id,
                f"💫 **{user_name}, Your Journey Paused**\n\nWe see you've stepped away from the Vault. Life happens—we understand.\n\n💎 **Here's the thing:** Your spot? Still reserved.\n🎯 **Your community?** Still rooting for you.\n\n**When you're ready to return, we'll be right here.** One click brings you back.\n\n*No pressure. Just opportunity.* ✨",
                reply_markup=get_verification_keyboard(user_id, user_data, show_all=False),
                parse_mode=ParseMode.MARKDOWN
            )
            # Store rejoin message ID for deletion when user rejoins
            update_verification_status(user_id, rejoin_msg_id=rejoin_msg.message_id)
            logger.info(f"Sent rejoin message {rejoin_msg.message_id} to user {user_id} who left vault")
        except Exception as e:
            logger.error(f"Failed to send rejoin message to {user_id}: {e}")



# ==========================================
# 📢 ANNOUNCEMENT HELPERS (reads bot10_broadcasts)
# ==========================================

_ANN_PREVIEW_CHARS = 160   # max preview chars per broadcast
_DASH_CHAR_LIMIT   = 3700  # safe buffer below Telegram’s 4096-char cap

def _build_announcement_section() -> str:
    """
    Returns the ANNOUNCEMENTS section text.
    Always shows the 3 most recent broadcasts only (no pagination).
    """
    try:
        total = col_broadcasts.count_documents({})
        if total == 0:
            return (
                "📢 **ANNOUNCEMENTS**\n"
                "―――――――――――――――――\n\n"
                "🔔 _No announcements yet._\n"
                "_Stay tuned for exclusive content!_"
            )

        broadcasts = list(
            col_broadcasts.find({})
            .sort("index", -1)   # newest first
            .limit(3)
        )

        lines = [
            f"📢 **ANNOUNCEMENTS** _· Latest {len(broadcasts)} of {total}_",
            "―――――――――――――――――",
        ]

        badge = ["01", "02", "03"]

        for i, b in enumerate(broadcasts):
            created_at = b.get("created_at")
            raw_text   = (b.get("message_text") or "").strip()
            media_type = b.get("media_type", "")

            date_str = (
                created_at.strftime("%b %d, %Y  ·  %I:%M %p")
                if created_at else "—"
            )

            if raw_text:
                preview = raw_text[:_ANN_PREVIEW_CHARS]
                if len(raw_text) > _ANN_PREVIEW_CHARS:
                    # trim to last full word
                    preview = preview.rsplit(" ", 1)[0] + "…"
            elif media_type:
                preview = f"📎 _[{media_type.capitalize()} content]_"
            else:
                preview = "_[No preview available]_"

            # NEW badge for broadcasts within last 48 h
            is_new = False
            if created_at:
                try:
                    from datetime import timezone
                    age = now_local() - created_at.replace(
                        tzinfo=created_at.tzinfo or timezone.utc
                    )
                    is_new = age.total_seconds() < 172800  # 48 h
                except Exception:
                    pass

            new_tag = " 🆕" if is_new else ""

            block = (
                f"🔘 **#{badge[i]}**{new_tag}\n"
                f"{preview}\n"
                f"🕐 _{date_str}_"
            )
            lines.append(block)
            if i < len(broadcasts) - 1:
                lines.append("╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌")

        return "\n".join(lines)

    except Exception as _e:
        logger.error(f"_build_announcement_section error: {_e}")
        return (
            "📢 **ANNOUNCEMENTS**\n"
            "⚠️ _Could not load announcements._"
        )


def _build_dashboard_text(user_name, display_msa_id, member_since, ann_text) -> str:
    """Assemble the full dashboard message."""
    return (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"   📊 **YOUR DASHBOARD**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**PROFILE INFORMATION**\n\n"
        f"👤 **Name:** {user_name}\n"
        f"🆔 **MSA+ ID:** `{display_msa_id}`\n"
        f"📅 **Member Since:** {member_since}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**ACCOUNT STATUS**\n\n"
        f"✅ **Verification:** Confirmed\n"
        f"🏆 **Membership:** Premium Active\n"
        f"⭐ **Access Level:** Full Access\n"
        f"🌐 **Network:** MSA NODE Elite\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**AGENT INFO**\n\n"
        f"🤖 **Agent:** MSA NODE Agent\n"
        f"🛡️ **Security:** Encrypted\n"
        f"⚡ **Response:** Real-time\n"
        f"🔄 **Updates:** Automatic\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{ann_text}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💡 **TIP:** Use **SEARCH CODE** to access\n"
        f"vault content from videos instantly.\n\n"
        f"📞 **Need help?** Open a **SUPPORT** ticket\n\n"
        f"💎 *MSA NODE Agent — Your Exclusive Gateway*"
    )


# ==========================================
#  MENU HANDLERS
# ==========================================

@dp.message(F.text == "📊 DASHBOARD")
@rate_limit(3.0)  # 3 second cooldown for dashboard
@anti_spam("dashboard")
async def dashboard(message: types.Message):
    """Handle Dashboard button"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check suspended features
    suspend_doc = col_suspended_features.find_one({"user_id": message.from_user.id})
    if suspend_doc and "DASHBOARD" in suspend_doc.get("suspended_features", []):
        await message.answer(
            "⚠️ **FEATURE SUSPENDED**\n\nDashboard access has been suspended for your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        await message.answer(
            f"🔒 **{user_name}, ACCESS DENIED**\n\n"
            f"You walked away from the **MSA NODE Vault**.\n"
            f"That means you walked away from your dashboard.\n\n"
            f"The system doesn't reward hesitation.\n"
            f"Every second you're out, you're losing visibility on your progress.\n\n"
            f"**The choice is simple:**\n"
            f"• Stay out → Stay blind.\n"
            f"• Get back in → Get back to work.\n\n"
            f"💎 **Rejoin the Vault. Reclaim your access.**",
            reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    
    
    # Get user's MSA+ ID before animation
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "User"
    msa_id = get_user_msa_id(user_id)
    
    # 🎬 DASHBOARD ANIMATION
    msg = await message.answer("⏳ Accessing User Database...")
    await asyncio.sleep(ANIM_FAST)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Accessing User Database...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text(f"🔐 Verifying Identity...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await msg.edit_text("📊 Loading Profile Stats...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Get Member Since date
    member_since = "Unknown"
    msa_record = col_msa_ids.find_one({"user_id": user_id})
    if msa_record and "assigned_at" in msa_record:
        member_since = msa_record["assigned_at"].strftime("%B %Y")
    else:
        # Fallback to first_start
        user_data = col_user_verification.find_one({"user_id": user_id})
        if user_data and "first_start" in user_data:
             member_since = user_data["first_start"].strftime("%B %Y")

    # Format MSA ID (remove + if present for display)
    display_msa_id = msa_id.replace("+", "") if msa_id else 'Not Assigned'

    # Build announcement section (always latest 3 only)
    ann_text = _build_announcement_section()
    dashboard_text = _build_dashboard_text(user_name, display_msa_id, member_since, ann_text)

    # Character-limit guard: trim announcement block if needed
    if len(dashboard_text) > _DASH_CHAR_LIMIT:
        # Progressively trim preview until it fits
        for cap in [120, 80, 40]:
            trimmed = col_broadcasts.find({}).sort("index", -1).limit(3)
            short_lines = []
            for b in trimmed:
                raw = (b.get("message_text") or "").strip()
                short_lines.append(raw[:cap] + ("…" if len(raw) > cap else ""))
            if short_lines:
                ann_text = (
                    "📢 **ANNOUNCEMENTS** _· Latest 3_\n"
                    "―――――――――――――――――\n"
                    + "\n╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌\n".join(short_lines)
                )
            dashboard_text = _build_dashboard_text(user_name, display_msa_id, member_since, ann_text)
            if len(dashboard_text) <= _DASH_CHAR_LIMIT:
                break

    await msg.edit_text(
        dashboard_text,
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} accessed Dashboard")

# ==========================================
# 🚫 CANCEL SEARCH HANDLER
# ==========================================
@dp.message(F.text == "❌ CANCEL")
@rate_limit(1.0)  # 1 second cooldown for cancel
async def cancel_search_handler(message: types.Message, state: FSMContext):
    """Handle cancel button in search flow"""
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Animation: Aborting operation
    msg = await message.answer("⚠️")
    await asyncio.sleep(ANIM_MEDIUM)
    await msg.edit_text("⚠️ **ABORTING...**", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_PAUSE)
    await msg.edit_text("🔓 **UNLOCKING SESSION...**", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_PAUSE)
    await safe_delete_message(msg)
    
    await state.clear()
    await message.answer(
        "❌ **SEARCH CANCELLED**\n\n`Operation aborted. Returning to main menu...`",
        reply_markup=get_user_menu(message.from_user.id),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} cancelled search")

@dp.message(F.text == "🔍 SEARCH CODE")
@rate_limit(2.0)  # 2 second cooldown for search
@anti_spam("search")
async def search(message: types.Message, state: FSMContext):
    """Handle Search button"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check suspended features
    suspend_doc = col_suspended_features.find_one({"user_id": message.from_user.id})
    if suspend_doc and "SEARCH_CODE" in suspend_doc.get("suspended_features", []):
        await message.answer(
            "⚠️ **FEATURE SUSPENDED**\n\nSearch Code access has been suspended for your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        await message.answer(
            f"🔒 **{user_name}, SEARCH IS BLOCKED**\n\n"
            f"You can't search for codes if you're not in the **Vault**.\n"
            f"The system protects its assets.\n\n"
            f"**Want to search?**\n"
            f"Get in the vault. It's that simple.\n\n"
            f"💎 **Rejoin. Unlock Search.**",
            reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # 🎬 CYBER LOADING ANIMATION
    msg = await message.answer("📡 Establishing Secure Uplink...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Establishing Secure Uplink...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("🔍 Initializing Code Search Protocol...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await safe_delete_message(msg)
    first_name = message.from_user.first_name
    
    # Add cancel button
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    
    await message.answer(
        f"🔒 **AUTHENTICATION REQUIRED**\n\n{first_name}, the agent is waiting.\nEnter your **MSA CODE** to decrypt the asset.\n\n*Precision is key.*\n\n`ENTER MSA CODE BELOW:`\n\n⚪️ _Reply 'CANCEL' to cancel this operation._",
        reply_markup=cancel_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    await state.set_state(SearchCodeStates.waiting_for_code)
    logger.info(f"User {message.from_user.id} initiated Search Code")

@dp.message(SearchCodeStates.waiting_for_code)
@rate_limit(1.5)
@anti_spam("process_search")
async def process_search_code(message: types.Message, state: FSMContext):
    """Process the MSA code input"""
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        await state.clear()
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await state.clear()
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    code = message.text.strip()
    
    # 🎬 CYBER LOADING ANIMATION (Common for all)
    msg = await message.answer("📡 Establishing Secure Uplink...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Establishing Secure Uplink...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("🔍 Verifying MSA CODE...")
    await asyncio.sleep(ANIM_SLOW)
    
    # 🔍 DATABASE QUERY (Case-insensitive)
    # Using regex for case-insensitive match on 'msa_code'
    pdf_doc = col_pdfs.find_one({"msa_code": {"$regex": f"^{code}$", "$options": "i"}})
    
    # Check if code exists
    if not pdf_doc:
        # ❌ INVALID CODE HANDLER
        await msg.edit_text("🚫 ACCESS DENIED")
        await asyncio.sleep(ANIM_SLOW)   
        await safe_delete_message(msg)
        
        # Get state data to check context
        state_data = await state.get_data()
        is_yt_flow = state_data.get("is_yt_flow", False)
        
        # Personalize error message
        first_name = message.from_user.first_name
        
        if is_yt_flow:
            # YT Flow: Dedicated professional error message (NOT from MSACODE_INVALID)
            error_msg = f"⚠️ **INCORRECT CODE**\n\n{first_name}, that MSA CODE does not match our records.\n\n🎯 **The correct code is waiting for you in the video.**\n\nReturn to the source. Watch carefully. Try again.\n\n`Click below or enter the correct code:`\n\n⚪️ _Click 'CANCEL' to cancel._"
            
            retry_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📺 GET CORRECT CODE", url=YOUTUBE_LINK)]
            ])
            
            await message.answer(
                f"{error_msg}",
                reply_markup=retry_kb,
                parse_mode=ParseMode.MARKDOWN
            )
            # DO NOT clear state - keep asking until correct or canceled
            return
        else:
            # Manual Flow: Same dedicated error message (NOT from MSACODE_INVALID)
            error_msg = f"⚠️ **INCORRECT CODE**\n\n{first_name}, that MSA CODE does not match our records.\n\n🎯 **The correct code is waiting for you in the video.**\n\nReturn to the source. Watch carefully. Try again.\n\n`Click below or enter the correct code:`\n\n⚪️ _Click 'CANCEL' to cancel._"
            
            retry_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📺 GET CORRECT CODE", url=YOUTUBE_LINK)]
            ])
            
            await message.answer(
                f"{error_msg}",
                reply_markup=retry_kb,
                parse_mode=ParseMode.MARKDOWN
            )
            # DO NOT clear state - keep asking until correct or canceled
            return

    # ✅ VALID CODE HANDLER
    await msg.edit_text("🔐 Decrypting Access Code...")
    await asyncio.sleep(ANIM_SLOW)
    
    # 📊 TRACK CLICK ANALYTICS for YT Code clicks
    try:
        # Increment YT code clicks counter and update timestamp
        col_pdfs.update_one(
            {"_id": pdf_doc["_id"]},
            {
                "$inc": {"yt_code_clicks": 1, "clicks": 1},
                "$set": {"last_yt_code_click": now_local(), "last_clicked_at": now_local()}
            }
        )
        logger.info(f"📊 Analytics: User {message.from_user.id} entered YT code for PDF '{pdf_doc.get('name')}'")
    except Exception as analytics_err:
        logger.error(f"⚠️ Analytics tracking failed: {analytics_err}")
    
    # Personalize the success message
    first_name = message.from_user.first_name
    await msg.edit_text(f"✅ **IDENTITY CONFIRMED: {first_name}**\n\n`Secure Delivery In Progress...`", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(ANIM_DELAY) # Slightly longer to let them see their name
    
    # Delete loading message to clean up
    await safe_delete_message(msg)

    # Get state data to check context
    state_data = await state.get_data()
    is_yt_flow = state_data.get("is_yt_flow", False)

    # DYNAMIC CONTENT SELECTION BASED ON CONTEXT
    if is_yt_flow:
        # User came from YT -> Treat as YT Source -> Show IG Titles/Buttons (Cross-pollinate)
        # 1. PDF Title: Standard
        pdf_title_template = random.choice(CONTENT_PACKS["PDF_TITLES"])
        
        # 2. Affiliate Title: Standard
        aff_title_text = random.choice(CONTENT_PACKS["AFFILIATE_TITLES"])
        
        # 3. Network Message: FORCE IG CONTENT
        # Use IG Video Titles (since they are watching on YT, we sell them on IG)
        msa_code_template = random.choice(CONTENT_PACKS["IG_VIDEO_TITLES"])
        
        # Use IG Buttons (Force them to IG)
        # We need a list of just IG buttons to pick from
        network_btn_text = random.choice(CONTENT_PACKS["IG_CODES_BUTTONS"])
        network_url = INSTAGRAM_LINK
        
    else:
        # Standard Manual Entry -> Randomize or Standard Logic
        # For now, keep existing random logic or define a "Neutral" flow?
        # Let's keep existing random mix for manual entry
        pdf_title_template = random.choice(CONTENT_PACKS["PDF_TITLES"])
        aff_title_text = random.choice(CONTENT_PACKS["AFFILIATE_TITLES"])
        msa_code_template = random.choice(CONTENT_PACKS["MSACODE"])
        network_btn_text = None # Will use dual buttons below

    # Format Titles
    try:
        pdf_title_text = pdf_title_template.format(name=first_name)
    except:
        pdf_title_text = pdf_title_template
        
    try:
        if is_yt_flow:
             msa_code_text = msa_code_template.format(name=first_name)
        else:
             msa_code_text = msa_code_template.format(name=first_name)
    except:
        msa_code_text = msa_code_template
    
    # Retrieve Links from DB
    pdf_link = pdf_doc.get("link", BOT_FALLBACK_LINK)
    affiliate_link = pdf_doc.get("affiliate_link", BOT_FALLBACK_LINK)

    # 1️⃣ SEND PDF MESSAGE (Standard)
    # ... (same as before) ...
    pdf_btn_text = random.choice(CONTENT_PACKS["PDF_BUTTONS"])
    pdf_footer_template = random.choice(CONTENT_PACKS["PDF_FOOTERS"])
    try:
        pdf_footer_text = pdf_footer_template.format(name=first_name)
    except:
        pdf_footer_text = pdf_footer_template
        
    pdf_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=pdf_btn_text, url=pdf_link)]])
    await message.answer(
        f"{pdf_title_text}\n\n`{pdf_footer_text}`",
        reply_markup=pdf_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # ⏳ SEQUENCE DOT ANIMATION 1
    wait_msg = await message.answer("▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(wait_msg)

    # 2️⃣ SEND AFFILIATE MESSAGE with Footer
    # Select random footer
    aff_footer_template = random.choice(CONTENT_PACKS["AFFILIATE_FOOTERS"])
    try:
        aff_footer_text = aff_footer_template.format(name=first_name)
    except:
        aff_footer_text = aff_footer_template
    
    aff_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💰 ACCESS OPPORTUNITY", url=affiliate_link)]])
    await message.answer(
        f"{aff_title_text}\n\n━━━━━━━━━━━━━━━━\n`{aff_footer_text}`",
        reply_markup=aff_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # ⏳ SEQUENCE DOT ANIMATION 2
    wait_msg = await message.answer("▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await wait_msg.edit_text("▪️▪️▪️")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(wait_msg)

    # 3️⃣ SEND NETWORK MESSAGE (Context-Aware)
    
    if is_yt_flow:
        # YT Flow: Show single button to IG with footer
        network_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=network_btn_text, url=network_url)]
        ])
        # Add random IG Video Footer
        ig_footer_template = random.choice(CONTENT_PACKS["IG_VIDEO_FOOTERS"])
        try:
            ig_footer_text = ig_footer_template.format(name=first_name)
        except:
            ig_footer_text = ig_footer_template
        msa_code_text += f"\n\n━━━━━━━━━━━━━━━━\n`{ig_footer_text}`"
    else:
        # Standard Flow: Dual Buttons (YT + IG)
        yt_btn_text_std, ig_btn_text_std = random.choice(CONTENT_PACKS["MSACODE_BUTTONS"])
        footer_template_std = random.choice(CONTENT_PACKS["MSACODE_FOOTERS"])
        try:
             footer_text_std = footer_template_std.format(name=first_name)
        except:
             footer_text_std = footer_template_std

        network_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=yt_btn_text_std, url=YOUTUBE_LINK)],
            [InlineKeyboardButton(text=ig_btn_text_std, url=INSTAGRAM_LINK)]
        ])
        # Append footer for standard flow if needed
        msa_code_text += f"\n\n━━━━━━━━━━━━━━━━\n`{footer_text_std}`"
    
    await message.answer(
        f"{msa_code_text}",
        reply_markup=network_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Log success
    logger.info(f"User {message.from_user.id} accessed content for code {code} (Index: {pdf_doc.get('index')}) | Context: {'YT Flow' if is_yt_flow else 'Manual'}")
    
    # DO NOT clear state - keep loop active
    # Re-prompt for another MSA CODE
    await asyncio.sleep(ANIM_DELAY)  # Brief pause after content delivery
    
    await message.answer(
        f"🔒 **AUTHENTICATION REQUIRED**\n\n{first_name}, the agent is waiting.\nEnter your **MSA CODE** to decrypt the asset.\n\n*Precision is key.*\n\n`ENTER MSA CODE BELOW:`\n\n⚪️ _Reply 'CANCEL' to cancel this operation._",
        parse_mode=ParseMode.MARKDOWN
    )
    # State remains active - user can enter another code or cancel

@dp.message(F.text == "📜 RULES")
@rate_limit(3.0)  # 3 second cooldown for rules
@anti_spam("rules")
async def rules_regulations(message: types.Message):
    """Handle Rules button"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check suspended features
    suspend_doc = col_suspended_features.find_one({"user_id": message.from_user.id})
    if suspend_doc and "RULES" in suspend_doc.get("suspended_features", []):
        await message.answer(
            "⚠️ **FEATURE SUSPENDED**\n\nRules access has been suspended for your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        await message.answer(
            f"🔒 **{user_name}, RULES ARE VAULT-ONLY**\n\n"
            f"The rules aren't public. They're protected.\n"
            f"Only vault members see the blueprint.\n\n"
            f"**You want the rules?**\n"
            f"Earn them. Join the vault.\n\n"
            f"💎 **Rejoin. See the system.**",
            reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # 🎬 RULES ANIMATION
    msg = await message.answer("⚖️ Accessing Protocol Database...")
    await asyncio.sleep(ANIM_FAST)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Accessing Protocol Database...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("📜 Verifying Community Guidelines...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    rules_text = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━
  📜  **MSA NODE AGENT — RULES**
━━━━━━━━━━━━━━━━━━━━━━━━━━━

These rules apply to every member who accesses the vault through **MSA NODE Agent**. Read them carefully — your continued access depends on it.

━━━━━━━━━━━━━━━━━━━━━━━━━━━

**⚖️ CORE PRINCIPLES**

1️⃣ **Respect & Professionalism**
   • Treat all members and admins with respect
   • Harassment, hate speech, and toxic behaviour are strictly prohibited
   • Keep all communication professional and constructive

2️⃣ **Vault Content Integrity**
   • Do NOT share vault content outside the community
   • Do NOT redistribute or re-upload MSA CODES or blueprints
   • All materials are exclusively for verified vault members

3️⃣ **Privacy & Account Security**
   • Never share your **MSA+ ID** with anyone
   • Do not disclose personal information of other members
   • Report any suspicious activity to an admin immediately

4️⃣ **Agent Usage Rules**
   • Do NOT spam buttons, commands, or messages
   • Do NOT attempt to exploit or reverse-engineer **MSA NODE Agent**
   • Use all features as intended — one action at a time
   • Respect cooldowns and rate limits at all times

5️⃣ **Identity & Honesty**
   • Do not impersonate admins, other members, or **MSA NODE Agent**
   • Do not use fake accounts to bypass restrictions
   • Any form of manipulation or deception leads to immediate ban

━━━━━━━━━━━━━━━━━━━━━━━━━━━

**🚨 VIOLATION CONSEQUENCES**

⚡ **Strike 1** — Account freeze (automatic cooldown applies)
⛔ **Strike 2** — Temporary suspension from all features
🔴 **Strike 3** — Permanent ban from **MSA NODE Agent**

> Severe violations (scamming, hacking attempts, data abuse) result in an instant permanent ban — no warnings.

━━━━━━━━━━━━━━━━━━━━━━━━━━━

**📩 APPEALS & QUESTIONS**
Open a support ticket via **📞 SUPPORT** if you believe an action was made in error. Provide your **MSA+ ID** and a clear explanation.

━━━━━━━━━━━━━━━━━━━━━━━━━━━

💎 **MSA NODE Agent**  |  _Your Exclusive Gateway_
_By using this agent, you agree to all the rules above._
_Rules are subject to updates without prior notice._
"""
    
    await msg.edit_text(
        rules_text,
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} viewed Rules")


# ──────────────────────────────────────────────────────────────
# 📚 GUIDE SYSTEM — two-choice selector + paginated user guide
# ──────────────────────────────────────────────────────────────

_AGENT_GUIDE_PAGES = [
    # Page 1 / 2 — DASHBOARD + SEARCH CODE + RULES + SUPPORT
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📖  **MSA NODE AGENT GUIDE**  ·  Page 1 / 2\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Welcome to **MSA NODE Agent** — your secure vault gateway.\n"
        "This guide covers everything you need to use it with ease.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📊 **DASHBOARD**\n"
        "Your personal hub inside the vault.\n"
        "  ├─ View your unique **MSA+ ID** & join date\n"
        "  ├─ Check your verification & membership status\n"
        "  ├─ See live announcements from the vault team\n"
        "  └─ Full access is granted once verified ✅\n\n"
        "🔍 **SEARCH CODE**\n"
        "Unlock exclusive vault content using **MSA CODES**.\n\n"
        "_Method 1 — Direct Link (Recommended)_\n"
        "  ① Watch a video on YouTube or Instagram\n"
        "  ② Find the special **MSA NODE Agent** link\n"
        "  ③ Tap it — Telegram opens automatically\n"
        "  ④ Your content is delivered instantly ✅\n\n"
        "_Method 2 — Manual Entry_\n"
        "  ① Press the **🔍 SEARCH CODE** button\n"
        "  ② Type your code exactly (e.g. `MSA001`)\n"
        "  ③ Receive your exclusive blueprint 📦\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📜 **RULES**\n"
        "  View the full community code of conduct.\n"
        "  Read carefully — vault access depends on it.\n\n"
        "📞 **SUPPORT**\n"
        "  Need help? Open a support ticket directly.\n"
        "  Describe your issue clearly for faster resolution.\n"
        "  ⚠️ Only one active ticket is allowed at a time.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 **MSA NODE Agent**  |  _Your Exclusive Gateway_"
    ),
    # Page 2 / 2 — PRO TIPS + TROUBLESHOOTING
    (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "  📖  **MSA NODE AGENT GUIDE**  ·  Page 2 / 2\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📖 **AGENT GUIDE**\n"
        "  You're reading it — this is your full manual.\n"
        "  Use PREV / NEXT to navigate between pages.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "✨ **PRO TIPS**\n"
        "  • Watch all videos to collect new **MSA CODES**\n"
        "  • Check **DASHBOARD** often for live announcements\n"
        "  • Never share your **MSA+ ID** with anyone\n"
        "  • Do not redistribute vault content outside\n"
        "  • Stay active — idle accounts may lose access\n\n"
        "🆘 **QUICK FIXES**\n"
        "  ❌ _Link not working?_\n"
        "     → Make sure the link hasn't been modified\n"
        "     → Confirm your vault membership is active\n\n"
        "  ⏳ _Agent slow to respond?_\n"
        "     → Wait 2–3 seconds between actions\n"
        "     → Avoid pressing buttons repeatedly\n\n"
        "  🚫 _Access denied or locked out?_\n"
        "     → Rejoin the vault channel\n"
        "     → Run /start to re-verify your account\n\n"
        "  🎫 _Need human support?_\n"
        "     → Press **📞 SUPPORT** to open a ticket\n"
        "     → An admin will respond shortly\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💎 **MSA NODE Agent**  |  _Your Exclusive Gateway_"
    ),
]

def _agent_guide_kb(page: int, total: int) -> ReplyKeyboardMarkup:
    """Navigation keyboard for MSA NODE Agent Guide — PREV / NEXT / HOME only."""
    row_nav = []
    if page > 1:
        row_nav.append(KeyboardButton(text="⬅️ PREV"))
    if page < total:
        row_nav.append(KeyboardButton(text="NEXT ➡️"))
    rows = []
    if row_nav:
        rows.append(row_nav)
    rows.append([KeyboardButton(text="🏠 MAIN MENU")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

@dp.message(F.text == "📖 AGENT GUIDE")
@rate_limit(3.0)
@anti_spam("guide")
async def guide(message: types.Message, state: FSMContext):
    """Open MSA NODE Agent Guide — goes straight to page 1 for users."""
    if await _check_freeze(message): return
    if await check_maintenance_mode(message):
        return

    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    suspend_doc = col_suspended_features.find_one({"user_id": message.from_user.id})
    if suspend_doc and "GUIDE" in suspend_doc.get("suspended_features", []):
        await message.answer(
            "⚠️ **FEATURE SUSPENDED**\n\nGuide access has been suspended for your account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        await message.answer(
            f"🔒 **{user_name}, GUIDE IS LOCKED**\n\n"
            f"The **Guide** is vault-exclusive.\n\n"
            f"💎 **Rejoin to unlock it.**",
            reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # 🎬 GUIDE BOOT ANIMATION
    msg = await message.answer("📡 Accessing Agent Manual...")
    await asyncio.sleep(ANIM_FAST)
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Decrypting Agent Manual...")
        await asyncio.sleep(0.07)
    await msg.edit_text("📖 Loading Page 1...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)

    page = 1
    await state.set_state(GuideStates.viewing_bot8)
    await state.update_data(guide_page=page)
    await message.answer(
        _AGENT_GUIDE_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_agent_guide_kb(page, len(_AGENT_GUIDE_PAGES)),
    )
    logger.info(f"User {message.from_user.id} opened Agent Guide page 1")

@dp.message(GuideStates.viewing_bot8, F.text == "NEXT ➡️")
async def guide_bot8_next(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = min(data.get("guide_page", 1) + 1, len(_AGENT_GUIDE_PAGES))
    await state.update_data(guide_page=page)
    msg = await message.answer("⏩ Loading next page...")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text(f"📖 Page {page} / {len(_AGENT_GUIDE_PAGES)}")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    await message.answer(
        _AGENT_GUIDE_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_agent_guide_kb(page, len(_AGENT_GUIDE_PAGES)),
    )

@dp.message(GuideStates.viewing_bot8, F.text == "⬅️ PREV")
async def guide_bot8_prev(message: types.Message, state: FSMContext):
    data = await state.get_data()
    page = max(data.get("guide_page", 1) - 1, 1)
    await state.update_data(guide_page=page)
    msg = await message.answer("⏪ Going back...")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text(f"📖 Page {page} / {len(_AGENT_GUIDE_PAGES)}")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    await message.answer(
        _AGENT_GUIDE_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_agent_guide_kb(page, len(_AGENT_GUIDE_PAGES)),
    )

@dp.message(F.text == "📚 GUIDE MENU")
async def guide_legacy_menu_btn(message: types.Message, state: FSMContext):
    """Legacy GUIDE MENU button — shows Agent Guide page 1."""
    msg = await message.answer("📡 Accessing Agent Manual...")
    await asyncio.sleep(ANIM_FAST)
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Decrypting Agent Manual...")
        await asyncio.sleep(0.07)
    await msg.edit_text("📖 Loading Page 1...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    page = 1
    await state.set_state(GuideStates.viewing_bot8)
    await state.update_data(guide_page=page)
    await message.answer(
        _AGENT_GUIDE_PAGES[page - 1],
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_agent_guide_kb(page, len(_AGENT_GUIDE_PAGES)),
    )

@dp.message(F.text == "🏠 MAIN MENU")
async def guide_back_to_main_bot8(message: types.Message, state: FSMContext):
    """Return to main menu, clearing any guide state (bot8)."""
    await state.clear()
    user_id = message.from_user.id
    await message.answer(
        "📋 **Main Menu**\n\nSelect an option:",
        reply_markup=get_user_menu(user_id),
        parse_mode=ParseMode.MARKDOWN,
    )
    logger.info(f"User {user_id} returned to main menu from guide")



@dp.message(Command("menu"))
@rate_limit(2.0)  # 2 second cooldown for menu command
async def cmd_menu(message: types.Message):
    """Show the main menu"""
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Ban check
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **ACCESS DENIED**\n\nYou are banned from using MSA NODE Agent.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    await message.answer(
        "📋 **Main Menu**\n\nSelect an option:",
        reply_markup=get_user_menu(message.from_user.id),
        parse_mode=ParseMode.MARKDOWN
    )

# ==========================================
# 📞 SUPPORT SYSTEM
# ==========================================

@dp.message(F.text == "📞 SUPPORT")
@rate_limit(2.0)  # 2 second cooldown
@anti_spam("support")
async def support_menu(message: types.Message, state: FSMContext):
    """Handle Support button - show support options"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check if user is banned - allow support access for banned users
    ban_doc = await check_if_banned(message.from_user.id)
    is_banned = ban_doc is not None
    
    # Check vault access (skip for banned users)
    if not is_banned:
        is_in_vault = await check_channel_membership(message.from_user.id)
    else:
        is_in_vault = True  # Allow banned users to bypass vault check for support
    
    if not is_in_vault:
        user_data = get_user_verification_status(message.from_user.id)
        was_ever_verified = user_data.get('ever_verified', False)
        user_name = message.from_user.first_name or "User"
        await message.answer(
            f"🔒 **{user_name}, SUPPORT IS VAULT-ONLY**\n\n"
            f"Support is for **verified members only**.\n"
            f"You need access to get help.\n\n"
            f"**Join the vault first.**\n\n"
            f"💎 **Rejoin. Get Support.**",
            reply_markup=get_verification_keyboard(message.from_user.id, user_data, show_all=not was_ever_verified),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Clear any existing state
    await state.clear()
    
    # 🎬 SUPPORT ANIMATION
    msg = await message.answer("🔌 Connecting to Support...")
    await asyncio.sleep(ANIM_FAST)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Connecting to Support...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("📞 Opening Support Center...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    
    first_name = message.from_user.first_name or "Member"
    
    support_text = f"""
📞 **SUPPORT CENTER**
━━━━━━━━━━━━━━━━━━━━━

Welcome, **{first_name}**! 👋

**Select your issue category:**

📄 **PDF/LINK ISSUES**
   Problems with PDFs, links, codes

🔧 **TROUBLESHOOTING**
   Bot performance, errors, bugs

❓ **OTHER ISSUES**
   General questions & help

🎫 **RAISE A TICKET**
   Submit issue to admin team

🔙 **BACK TO MENU**
   Return to main menu

━━━━━━━━━━━━━━━━━━━━━

💡 **Tip:** Check categories first for instant solutions!
"""
    
    await message.answer(
        support_text,
        reply_markup=get_support_menu(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} opened Support Center")

@dp.message(F.text == "📄 PDF/LINK ISSUES")
@rate_limit(2.0)
@anti_spam("pdf_issues")
async def pdf_link_issues_handler(message: types.Message):
    """Handle PDF/Link Issues category"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        await message.answer(
            "🔒 **ACCESS DENIED**\n\nJoin the vault to access support.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Get user info for personalization
    first_name = message.from_user.first_name or "Member"
    
    # 🎬 PREMIUM SUPPORT ANIMATION
    msg = await message.answer("🔎 Analyzing your issue...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await msg.edit_text(f"📄 **Loading PDF/Link Solutions for {first_name}...**")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Preparing Solutions...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("✅ **Solutions Ready!**")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(msg)
    
    help_text = f"""
📄 **PDF & LINK ISSUES**
━━━━━━━━━━━━━━━━━━━━━━━━━━

👋 **{first_name}, I'm here to help with your PDF/Link issues.**

**🔍 COMMON PROBLEMS & SOLUTIONS:**

**Problem 1: Link Not Working**
`Solution:`
• Verify you are in the vault channel
• Don't modify or edit the link
• Wait 2-3 seconds and try again
• Clear Telegram cache and retry

**Problem 2: PDF Not Opening**
`Solution:`
• Check your internet connection
• Update Telegram app to latest version
• Try opening in external browser
• Download and open in PDF reader

**Problem 3: MSA CODE Invalid**
`Solution:`
• Check spelling carefully (case sensitive)
• Ensure you copied the full code
• Code must match video/post source
• Try manual entry instead of paste

**Problem 4: Content Not Delivered**
`Solution:`
• Wait 5-10 seconds (processing time)
• Check if bot sent multiple messages
• Don't spam the button
• Use /start to reset bot

**Problem 5: Google Drive Access Denied**
`Solution:`
• Link opens automatically in Drive
• Make sure you're logged into Google
• Try incognito/private mode
• Request access if prompted

━━━━━━━━━━━━━━━━━━━━━━━━━━

💬 **{first_name}, did any of these solutions work for you?**

✅ If your issue is resolved, click **RESOLVED**
🔍 Need to check other categories? Click **CHECK OTHER**
🎫 Still need help? Click **RAISE A TICKET** to reach admin

*I'm here to help!*
"""
    
    await message.answer(
        help_text,
        reply_markup=get_resolution_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} viewed PDF/Link Issues")

@dp.message(F.text == "🔧 TROUBLESHOOTING")
@rate_limit(2.0)
@anti_spam("troubleshooting")
async def troubleshooting_handler(message: types.Message):
    """Handle Troubleshooting category"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        await message.answer(
            "🔒 **ACCESS DENIED**\n\nJoin the vault to access support.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Get user info for personalization
    first_name = message.from_user.first_name or "Member"
    
    # 🎬 PREMIUM SUPPORT ANIMATION
    msg = await message.answer("⚙️ Running diagnostics...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await msg.edit_text(f"🔧 **Analyzing Bot Performance for {first_name}...**")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Scanning System...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("✅ **Diagnostics Complete!**")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(msg)
    
    help_text = f"""
🔧 **TROUBLESHOOTING GUIDE**
━━━━━━━━━━━━━━━━━━━━━━━━━━

👋 **{first_name}, let's fix your technical issues together.**

**⚡ PERFORMANCE ISSUES:**

**Issue: Bot is Slow/Laggy**
`Solution:`
• Wait 2-3 seconds between commands
• Don't spam buttons repeatedly
• Check your network connection
• Restart Telegram app
• Clear Telegram cache

**Issue: Commands Not Working**
`Solution:`
• Use /start to reset the agent
• Check vault membership status
• Wait for animations to complete
• Don't send multiple commands at once

**Issue: Stuck in Search Mode**
`Solution:`
• Click ❌ CANCEL button
• Send /start command
• Wait 10 seconds before retrying

**Issue: Menu Buttons Missing**
`Solution:`
• Send /menu command
• Restart Telegram app
• Use /start to reload interface

━━━━━━━━━━━━━━━━━━━━━━━━━━

**🚨 ERROR MESSAGES:**

**"Access Denied"**
• Join the vault channel first
• Verify membership status
• Wait 10 seconds after joining

**"Invalid Code"**
• Check code spelling
• Ensure exact match from source
• Try uppercase/lowercase variants

**"Rate Limited"**
• You clicked too fast
• Wait 2-3 seconds
• Prevents Telegram ban

━━━━━━━━━━━━━━━━━━━━━━━━━━

**💡 BEST PRACTICES:**

✅ Wait for bot responses
✅ Follow on-screen instructions
✅ One command at a time
✅ Keep Telegram updated
✅ Stable internet connection

━━━━━━━━━━━━━━━━━━━━━━━━━━

💬 **{first_name}, were you able to fix the issue?**

✅ Problem solved? Click **RESOLVED**
🔍 Want to explore other solutions? Click **CHECK OTHER**
🎫 Need direct admin support? Click **RAISE A TICKET**

*We're committed to getting you back on track!*
"""
    
    await message.answer(
        help_text,
        reply_markup=get_resolution_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} viewed Troubleshooting")

@dp.message(F.text == "❓ OTHER ISSUES")
@rate_limit(2.0)
@anti_spam("other_issues")
async def other_issues_handler(message: types.Message):
    """Handle Other Issues category"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        await message.answer(
            "🔒 **ACCESS DENIED**\n\nJoin the vault to access support.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Get user info for personalization
    first_name = message.from_user.first_name or "Member"
    
    # 🎬 PREMIUM SUPPORT ANIMATION
    msg = await message.answer("📚 Accessing knowledge base...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await msg.edit_text(f"❓ **Finding Answers for {first_name}...**")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Searching Database...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("✅ **Information Retrieved!**")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(msg)
    
    help_text = f"""
❓ **OTHER QUESTIONS & HELP**
━━━━━━━━━━━━━━━━━━━━━━━━━━

👋 **{first_name}, I have answers to your general questions.**

**📚 GENERAL INFORMATION:**

**Q: How do I access content?**
`A:` Click links from videos or use SEARCH CODE with MSA CODES.

**Q: Where do I find MSA CODES?**
`A:` MSA CODES are shown in YouTube videos and Instagram posts.

**Q: How to use SEARCH CODE?**
`A:` Click 🔍 SEARCH CODE → Enter MSA CODE → Receive content

**Q: What is MSA+ ID?**
`A:` Your unique member identification number. View in DASHBOARD.

**Q: Can I share content?**
`A:` No. All vault content is exclusive for members only.

━━━━━━━━━━━━━━━━━━━━━━━━━━

**🔐 ACCOUNT & ACCESS:**

**Q: I left vault, what happens?**
`A:` Access revoked immediately. Rejoin to restore full access.

**Q: Can I rejoin after leaving?**
`A:` Yes. Rejoin vault channel to restore access instantly.

**Q: How to check my status?**
`A:` Use 📊 DASHBOARD to view your profile and membership info.

━━━━━━━━━━━━━━━━━━━━━━━━━━

**📱 PLATFORM SUPPORT:**

**Q: Does bot work on mobile?**
`A:` Yes. Fully optimized for mobile and desktop.

**Q: Which Telegram version?**
`A:` Works on all: Official app, Web, Desktop.

**Q: Need special permissions?**
`A:` Only vault channel membership required.

━━━━━━━━━━━━━━━━━━━━━━━━━━

**📖 RESOURCES:**

• Check 📚 GUIDE for complete manual
• Review 📜 RULES for community guidelines
• Visit vault for announcements

━━━━━━━━━━━━━━━━━━━━━━━━━━

💬 **{first_name}, did you find what you were looking for?**

✅ Got your answer? Click **RESOLVED**
🔍 Need to check other sections? Click **CHECK OTHER**
🎫 Have a specific question for admin? Click **RAISE A TICKET**

*Always happy to help!*
"""
    
    await message.answer(
        help_text,
        reply_markup=get_resolution_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} viewed Other Issues")

@dp.message(F.text == "✅ RESOLVED")
@rate_limit(cooldown=1.0)
@anti_spam("resolved")
async def resolved_handler(message: types.Message):
    
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    first_name = message.from_user.first_name or "Member"
    
    # 🎬 SUCCESS ANIMATION
    msg = await message.answer("✨")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text("✨ ✨")
    await asyncio.sleep(ANIM_FAST)
    await msg.edit_text("✨ ✨ ✨")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(msg)
    
    await message.answer(
        f"✅ **EXCELLENT, {first_name}!**\n\n"
        f"I'm glad we could resolve your issue together!\n\n"
        f"💎 **You're all set now.**\n\n"
        f"If you ever need support again, I'm here 24/7.\n"
        f"Just click **📞 SUPPORT** anytime.\n\n"
        f"`Returning to main menu...`",
        reply_markup=get_user_menu(message.from_user.id),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} marked issue as resolved")

@dp.message(F.text == "🔍 CHECK OTHER")
@rate_limit(1.5)
@anti_spam("check_other")
async def check_other_handler(message: types.Message):
    """Handle Check Other button"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    first_name = message.from_user.first_name or "Member"
    
    # 🎬 TRANSITION ANIMATION
    msg = await message.answer("🔄 Switching categories...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    
    await message.answer(
        f"🔍 **BROWSE OTHER SOLUTIONS, {first_name}**\n\n"
        f"Let's explore other support categories to find what you need.\n\n"
        f"**Select another category below:**",
        reply_markup=get_support_menu(),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} checking other categories")

@dp.message(F.text == "🎫 RAISE A TICKET")
@rate_limit(2.0)
@anti_spam("raise_ticket")
async def raise_ticket_handler(message: types.Message, state: FSMContext):
    """Handle Raise a Ticket button - check for existing ticket first"""
    if await _check_freeze(message): return
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        return

    # Check vault access
    is_in_vault = await check_channel_membership(message.from_user.id)
    if not is_in_vault:
        await message.answer(
            "🔒 **ACCESS DENIED**\n\nJoin the vault to access support.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    user_id = message.from_user.id
    
    # Check if user has an open ticket
    existing_ticket = col_support_tickets.find_one({
        "user_id": user_id,
        "status": "open"
    })
    
    if existing_ticket:
        # User already has an open ticket - show lock message
        first_name = message.from_user.first_name or "Member"
        ticket_date = existing_ticket.get('created_at', now_local())
        date_str = ticket_date.strftime("%B %d, %Y at %I:%M %p")
        
        # 🎬 LOCK ANIMATION
        msg = await message.answer("🔒 Checking ticket status...")
        await asyncio.sleep(ANIM_MEDIUM)
        await safe_delete_message(msg)
        
        await message.answer(
            f"🔒 **{first_name}, YOU HAVE AN ACTIVE TICKET**\n\n"
            f"I see you already submitted a support request.\n\n"
            f"📋 **Your Current Ticket:**\n"
            f"• **Submitted:** {date_str}\n"
            f"• **Status:** ⏳ Pending Admin Review\n\n"
            f"⚠️ **{first_name}, you cannot submit another ticket until:**\n"
            f"• Admin reviews your current ticket\n"
            f"• Admin responds to your issue\n"
            f"• Current ticket is marked as resolved\n\n"
            f"💡 **Good news:** Admin typically responds within 24-48 hours!\n\n"
            f"`{first_name}, please wait for admin response. You'll be notified!`",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"User {user_id} tried to submit ticket while one is open")
        return
    
    first_name = message.from_user.first_name or "Member"
    
    # 🎬 TICKET PREPARATION ANIMATION
    msg = await message.answer("🎫 Preparing ticket form...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    await msg.edit_text(f"📝 **Setting up for {first_name}...**")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    
    # Add cancel button
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ CANCEL")]],
        resize_keyboard=True
    )
    
    await message.answer(
        f"🎫 **TICKET SUBMISSION FORM**\n\n"
        f"👋 **{first_name}, I'm listening.**\n\n"
        f"Please describe your issue in detail so our admin team can help you effectively.\n\n"
        f"**💡 Include these details:**\n"
        f"• Clear description of what's wrong\n"
        f"• Steps you took before the issue\n"
        f"• Any error messages you received\n\n"
        f"**📎 You can also attach:**\n"
        f"• 📷 Screenshots (images)\n"
        f"• 🎥 Screen recordings (videos)\n"
        f"• 📝 Text description (with or without media)\n\n"
        f"**✅ Requirements:**\n"
        f"• Minimum {MIN_TICKET_LENGTH} characters for text\n"
        f"• Maximum {MAX_TICKET_LENGTH} characters\n"
        f"• Professional language (no profanity)\n"
        f"• Clear communication (no spam/gibberish)\n\n"
        f"**{first_name}, type your message or send media below:**\n\n"
        f"⚪️ _Click '❌ CANCEL' if you changed your mind._",
        reply_markup=cancel_kb,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Set state to wait for issue description
    await state.set_state(SupportStates.waiting_for_issue)
    logger.info(f"User {message.from_user.id} started ticket submission")

@dp.message(SupportStates.waiting_for_issue)
@rate_limit(1.5)
@anti_spam("submit_ticket")
async def process_ticket_submission(message: types.Message, state: FSMContext):
    """Process the ticket submission with text/photo/video and comprehensive validation"""
    # Check Maintenance Mode
    if await check_maintenance_mode(message):
        await state.clear()
        return

    # Get user info for personalization
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "Member"
    
    # Determine content type and extract text
    has_photo = message.photo is not None
    has_video = message.video is not None
    issue_text = (message.caption or message.text or "").strip()
    
    # Check if user canceled
    if issue_text.upper() == "CANCEL" or issue_text == "❌ CANCEL":
        await state.clear()
        await message.answer(
            "❌ **TICKET CANCELLED**\n\n`Returning to support menu...`",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Validate if there's any content at all
    if not issue_text and not has_photo and not has_video:
        await message.answer(
            f"⚠️ **NO CONTENT DETECTED**\n\n"
            f"{user_name}, please send either:\n"
            f"• Text description of your issue\n"
            f"• Screenshot/image with caption\n"
            f"• Video with description\n\n"
            f"`Try again or click ❌ CANCEL`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # If media without caption, require minimum description
    if (has_photo or has_video) and len(issue_text) == 0:
        await message.answer(
            f"⚠️ **CAPTION REQUIRED**\n\n"
            f"{user_name}, please add a description to your media:\n"
            f"• Explain what the image/video shows\n"
            f"• Describe the problem clearly\n\n"
            f"📝 **How to add caption:**\n"
            f"1. Long press the media\n"
            f"2. Add text description\n"
            f"3. Send again\n\n"
            f"`Try again or click ❌ CANCEL`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Comprehensive validation on text content
    is_valid, error_msg = validate_ticket_content(issue_text, user_name)
    if not is_valid:
        await message.answer(error_msg, parse_mode=ParseMode.MARKDOWN)
        return

    # Rate limit check — prevent ticket flooding
    rate_ok, rate_msg = check_ticket_rate_limit(user_id)
    if not rate_ok:
        await message.answer(rate_msg, parse_mode=ParseMode.MARKDOWN)
        return
    
    # 🎬 SUBMISSION ANIMATION
    msg = await message.answer("📡 Submitting Ticket...")
    await asyncio.sleep(ANIM_MEDIUM)
    
    # Cyber Bar effect
    steps = ["▱▱▱▱▱", "▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    for step in steps:
        await msg.edit_text(f"[{step}] Submitting Ticket...")
        await asyncio.sleep(0.1)
    
    await msg.edit_text("✅ Ticket Submitted Successfully!")
    await asyncio.sleep(ANIM_SLOW)
    await safe_delete_message(msg)
    
    # Get additional user info
    username = f"@{message.from_user.username}" if message.from_user.username else "No Username"
    
    # Get MSA+ ID
    msa_id = get_user_msa_id(user_id)
    display_msa_id = msa_id.replace("+", "") if msa_id else "Not Assigned"
    
    # Get current date/time in 12-hour format
    now = now_local()
    date_str = now.strftime("%B %d, %Y")  # e.g., "February 12, 2026"
    time_str = now.strftime("%I:%M %p")   # e.g., "03:45 PM"
    
    # Determine ticket type
    ticket_type = "Text Only"
    if has_photo and has_video:
        ticket_type = "Text + Photo + Video"
    elif has_photo:
        ticket_type = "Text + Photo 📷"
    elif has_video:
        ticket_type = "Text + Video 🎥"
    
    # Create ticket message for admin channel
    ticket_msg = f"""
🎫 **NEW SUPPORT TICKET**
━━━━━━━━━━━━━━━━━━━━━━━━

📅 **Date:** {date_str}
🕐 **Time:** {time_str}
📋 **Type:** {ticket_type}

👤 **USER INFORMATION**
━━━━━━━━━━━━━━━━━━━━━━━━

**Name:** {user_name}
**Username:** {username}
**User ID:** `{user_id}`
**MSA+ ID:** `{display_msa_id}`

🔍 **ISSUE DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━

{issue_text}

━━━━━━━━━━━━━━━━━━━━━━━━

⚡ **STATUS:** Open
🤖 **Source:** MSA NODE Bot
✅ **Validated:** Passed all filters

💡 **Admin Actions:**
• Reply directly to user: [Contact User](tg://user?id={user_id})
• Mark as resolved: `/resolve {user_id}`
"""
    
    # Send ticket to admin channel with media if present
    try:
        # Send media first if present, then text ticket message
        if has_photo:
            # Get the largest photo
            photo = message.photo[-1]
            await bot.send_photo(
                REVIEW_LOG_CHANNEL,
                photo.file_id,
                caption="📷 **TICKET SCREENSHOT**\n\n_See full ticket details below_",
                parse_mode=ParseMode.MARKDOWN
            )
        
        if has_video:
            await bot.send_video(
                REVIEW_LOG_CHANNEL,
                message.video.file_id,
                caption="🎥 **TICKET VIDEO**\n\n_See full ticket details below_",
                parse_mode=ParseMode.MARKDOWN
            )
        
        # Send main ticket message and store message_id
        channel_msg = await bot.send_message(
            REVIEW_LOG_CHANNEL,
            ticket_msg,
            parse_mode=ParseMode.MARKDOWN
        )
        channel_message_id = channel_msg.message_id
        logger.info(f"✅ Ticket submitted by user {user_id} to channel {REVIEW_LOG_CHANNEL} (Type: {ticket_type}, Msg ID: {channel_message_id})")
    except Exception as e:
        logger.error(f"❌ Failed to send ticket to admin channel: {e}")
        await message.answer(
            "❌ **SUBMISSION FAILED**\n\n"
            "Could not submit your ticket. Please try again later.",
            reply_markup=get_support_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
        await state.clear()
        return
    
    # Count previous tickets for this user
    previous_tickets_count = col_support_tickets.count_documents({"user_id": user_id})
    support_count = previous_tickets_count + 1  # Current ticket number
    
    # Store ticket in database (LOCK SYSTEM)
    ticket_record = {
        "user_id": user_id,
        "user_name": user_name,
        "username": message.from_user.username or "none",
        "msa_id": display_msa_id,
        "issue_text": issue_text,
        "has_photo": has_photo,
        "has_video": has_video,
        "ticket_type": ticket_type,
        "character_count": len(issue_text),
        "status": "open",  # open, resolved
        "created_at": now,
        "resolved_at": None,
        "channel_message_id": channel_message_id,  # Store for editing later
        "support_count": support_count  # Track ticket number for this user
    }
    col_support_tickets.insert_one(ticket_record)
    logger.info(f"Ticket record created for user {user_id} in database (Support #{support_count})")

    # Record submission for rate limiting
    record_ticket_submission(user_id)
    
    # Clear state
    await state.clear()
    
    # 🎬 SUCCESS CONFIRMATION ANIMATION
    success_msg = await message.answer("✨")
    await asyncio.sleep(ANIM_FAST)
    await success_msg.edit_text("✨ ✅")
    await asyncio.sleep(ANIM_FAST)
    await success_msg.edit_text("✨ ✅ ✨")
    await asyncio.sleep(ANIM_FAST)
    await safe_delete_message(success_msg)
    
    # Build media confirmation part
    media_info = ""
    if has_photo:
        media_info += "• 📷 Screenshot attached\n"
    if has_video:
        media_info += "• 🎥 Video attached\n"
    
    # Confirm to user with personalization
    await message.answer(
        f"✅ **TICKET SUBMITTED SUCCESSFULLY!**\n\n"
        f"👋 **{user_name}, I've forwarded your issue to our admin team.**\n\n"
        f"📋 **Your Ticket Information:**\n"
        f"• **Submitted:** {date_str} at {time_str}\n"
        f"• **Type:** {ticket_type}\n"
        f"{media_info}"
        f"• **Characters:** {len(issue_text)}/{MAX_TICKET_LENGTH}\n"
        f"• **Status:** ⏳ Awaiting Admin Review\n"
        f"• **Ticket Priority:** Normal\n\n"
        f"🔔 **What happens next, {user_name}?**\n"
        f"• Admin will carefully review your ticket\n"
        f"• You'll receive a personal response via DM\n"
        f"• Expected response: 24-48 hours\n"
        f"• You'll be notified as soon as admin responds\n\n"
        f"🔒 **Important:** You cannot submit another ticket until admin resolves this one.\n\n"
        f"💡 **{user_name}, thank you for your patience!**\n"
        f"We're committed to solving your issue.\n\n"
        f"`Returning to support menu...`",
        reply_markup=get_support_menu(),
        parse_mode=ParseMode.MARKDOWN
    )
    
    logger.info(f"User {user_id} ticket confirmed")

@dp.message(Command("resolve"))
@rate_limit(10.0)  # Strict 10 second cooldown for admin command
async def cmd_resolve_ticket(message: types.Message):
    """Resolve a user's ticket (Admin only command - strict rate limit)"""  
    # Only owner/admin can use this
    if message.from_user.id != OWNER_ID:
        return
    
    try:
        # Parse command: /resolve <user_id>
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                "**Usage:** `/resolve <user_id>`\n\nExample: `/resolve 123456789`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        target_user_id = int(parts[1])
        
        # Find and update ticket
        result = col_support_tickets.update_one(
            {"user_id": target_user_id, "status": "open"},
            {"$set": {"status": "resolved", "resolved_at": now_local()}}
        )
        
        if result.modified_count > 0:
            await message.answer(
                f"✅ **Ticket Resolved**\n\n"
                f"**User ID:** `{target_user_id}`\n\n"
                f"User can now submit new tickets.",
                parse_mode=ParseMode.MARKDOWN
            )
            logger.info(f"Admin {message.from_user.id} resolved ticket for user {target_user_id}")
            
            # Notify user their ticket is resolved
            try:
                await bot.send_message(
                    target_user_id,
                    "✅ **TICKET RESOLVED**\n\n"
                    "Your support ticket has been reviewed and resolved by admin.\n\n"
                    "You can now submit new tickets if needed.\n\n"
                    "Thank you for your patience!",
                    parse_mode=ParseMode.MARKDOWN
                )
            except:
                pass  # User might have blocked bot
        else:
            await message.answer(
                f"❌ **No Open Ticket**\n\n"
                f"User `{target_user_id}` has no open tickets.",
                parse_mode=ParseMode.MARKDOWN
            )
    except ValueError:
        await message.answer(
            "❌ **Invalid User ID**\n\nProvide a valid numeric user ID.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await message.answer(f"❌ **Error:** {str(e)}", parse_mode=ParseMode.MARKDOWN)
        logger.error(f"Error resolving ticket: {e}")

@dp.message(F.text == "🔙 BACK TO MENU")
@rate_limit(1.5)
@anti_spam("back_menu")
async def back_to_menu_handler(message: types.Message, state: FSMContext):
    """Handle Back to Menu button"""
    # Check Maintenance Mode - still allow nav back to menu during maintenance
    # but show maintenance screen (user can't do anything anyway)
    if await check_maintenance_mode(message):
        await state.clear()
        return

    # Clear any state
    await state.clear()
    
    # Check if user is banned
    ban_doc = await check_if_banned(message.from_user.id)
    if ban_doc:
        ban_type = ban_doc.get("ban_type", "permanent")
        await message.answer(
            "🚫 **BANNED USER**\n\nYou are banned from using bot features.",
            reply_markup=get_banned_user_keyboard(ban_type),
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    first_name = message.from_user.first_name or "Member"
    
    # 🎬 TRANSITION ANIMATION
    msg = await message.answer("🔄 Returning to main menu...")
    await asyncio.sleep(ANIM_MEDIUM)
    await safe_delete_message(msg)
    
    await message.answer(
        f"📋 **MAIN MENU**\n\n"
        f"Welcome back, **{first_name}**! 👋\n\n"
        f"You've returned to the main control center.\n"
        f"Select any service below to continue.",
        reply_markup=get_user_menu(message.from_user.id),
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info(f"User {message.from_user.id} returned to main menu")

@dp.message(Command("delete"))
@rate_limit(5.0)  # 5 second cooldown for delete command (admin only)
async def cmd_delete_user(message: types.Message):
    """Delete user verification data (Owner only - for testing)"""
    # Only owner can use this command
    if message.from_user.id != OWNER_ID:
        await message.answer("❌ This command is only for the owner.", parse_mode=ParseMode.MARKDOWN)
        return
    
    # Get user ID from command
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                "**Usage:** `/delete <user_id>`\n\nExample: `/delete 123456789`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        target_user_id = int(parts[1])
        
        # Get MSA+ ID before deletion (for confirmation message)
        msa_record = col_msa_ids.find_one({"user_id": target_user_id})
        deleted_msa_id = msa_record['msa_id'] if msa_record else None
        
        # Delete from both collections
        result_verification = col_user_verification.delete_one({"user_id": target_user_id})
        result_msa = col_msa_ids.delete_one({"user_id": target_user_id})
        
        if result_verification.deleted_count > 0 or result_msa.deleted_count > 0:
            msa_info = f"\n🆔 **MSA+ ID Deleted**: `{deleted_msa_id}`" if deleted_msa_id else ""
            await message.answer(
                f"✅ **User Deleted**\n\n**User ID:** `{target_user_id}`{msa_info}\n\nVerification data has been removed from database.\n\nThis user will be treated as a new user on next /start.\n\n🔄 **Note**: The MSA+ ID `{deleted_msa_id}` is now available for reassignment.",
                parse_mode=ParseMode.MARKDOWN
            )
            logger.info(f"Owner {message.from_user.id} deleted user {target_user_id} (MSA+ ID: {deleted_msa_id}) from database")
        else:
            await message.answer(
                f"❌ **User Not Found**\n\n**User ID:** `{target_user_id}`\n\nNo verification data found in database.",
                parse_mode=ParseMode.MARKDOWN
            )
    except ValueError:
        await message.answer(
            "❌ **Invalid User ID**\n\nPlease provide a valid numeric user ID.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await message.answer(f"❌ **Error:** {str(e)}", parse_mode=ParseMode.MARKDOWN)
        logger.error(f"Error in delete command: {e}")

# NOTE: /resolve is defined earlier (once). Duplicate removed.

@dp.message(Command("ticket_stats"))
@rate_limit(5.0)
async def cmd_ticket_stats(message: types.Message):
    """Display ticket statistics (Admin only command)"""
    # Only owner/admin can use this
    if message.from_user.id != OWNER_ID:
        return
    
    try:
        # Count tickets by status
        open_count = col_support_tickets.count_documents({"status": "open"})
        resolved_count = col_support_tickets.count_documents({"status": "resolved"})
        archived_count = col_support_tickets.count_documents({"status": "archived"})
        total_count = open_count + resolved_count + archived_count
        
        # Get recent tickets (last 24 hours)
        yesterday = now_local() - timedelta(days=1)
        recent_count = col_support_tickets.count_documents({
            "created_at": {"$gte": yesterday}
        })
        
        # Get tickets to be archived soon (resolved > 6 days ago)
        expire_soon_date = now_local() - timedelta(days=TICKET_EXPIRE_DAYS - 1)
        expire_date = now_local() - timedelta(days=TICKET_EXPIRE_DAYS)
        expire_soon_count = col_support_tickets.count_documents({
            "status": "resolved",
            "resolved_at": {"$gte": expire_date, "$lt": expire_soon_date}
        })
        
        await message.answer(
            f"📊 **SUPPORT TICKET STATISTICS**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**📋 Overall Status:**\n"
            f"• Total Tickets: `{total_count}`\n"
            f"• 🔴 Open: `{open_count}`\n"
            f"• 🟢 Resolved: `{resolved_count}`\n"
            f"• 🗄️ Archived: `{archived_count}`\n\n"
            f"**📅 Recent Activity:**\n"
            f"• Last 24 Hours: `{recent_count}` new tickets\n\n"
            f"**🗑️ Auto-Archive System:**\n"
            f"• Archive After: `{TICKET_EXPIRE_DAYS} days`\n"
            f"• Expiring Soon: `{expire_soon_count}` tickets\n"
            f"• Status: ✅ Active\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_Auto-cleanup runs every 24 hours_",
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Admin {message.from_user.id} viewed ticket statistics")
        
    except Exception as e:
        await message.answer(f"❌ **Error:** {str(e)}", parse_mode=ParseMode.MARKDOWN)
        logger.error(f"Error in ticket stats: {e}")

@dp.message(Command("health"))
@rate_limit(5.0)
async def cmd_bot_health(message: types.Message):
    """Display bot health status (Owner only command)"""
    # Only owner can use this
    if message.from_user.id != OWNER_ID:
        return
    
    try:
        # Calculate uptime
        uptime = now_local() - health_stats["bot_start_time"]
        days = int(uptime.total_seconds() // 86400)
        hours = int((uptime.total_seconds() % 86400) // 3600)
        minutes = int((uptime.total_seconds() % 3600) // 60)
        
        # Check database status
        db_status = "❌ OFFLINE"
        try:
            client.admin.command('ping')
            db_status = "✅ ONLINE"
        except:
            pass
        
        # Check bot status
        bot_status = "❌ ERROR"
        try:
            me = await bot.get_me()
            bot_status = f"✅ ONLINE (@{me.username})"
        except:
            pass
        
        # Last error info
        last_error_info = "None"
        if health_stats["last_error"]:
            time_since = now_local() - health_stats["last_error"]
            mins_ago = int(time_since.total_seconds() // 60)
            last_error_info = f"{mins_ago} minutes ago"
        
        # Calculate success rate
        total_errors = health_stats["errors_caught"]
        healed = health_stats["auto_healed"]
        success_rate = (healed / total_errors * 100) if total_errors > 0 else 100
        
        await message.answer(
            f"🏥 **BOT HEALTH STATUS**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**⚡ System Status:**\n"
            f"• Bot: {bot_status}\n"
            f"• Database: {db_status}\n"
            f"• Health Monitor: ✅ Active\n"
            f"• Auto-Healer: ✅ Active\n\n"
            f"**⏱️ Uptime:**\n"
            f"• Running: {days}d {hours}h {minutes}m\n"
            f"• Started: {health_stats['bot_start_time'].strftime('%b %d, %I:%M %p')}\n\n"
            f"**📊 Error Statistics:**\n"
            f"• Total Caught: `{total_errors}`\n"
            f"• Auto-Healed: `{healed}`\n"
            f"• Manual Fixes: `{total_errors - healed}`\n"
            f"• Success Rate: `{success_rate:.1f}%`\n"
            f"• Owner Alerts: `{health_stats['owner_notified']}`\n\n"
            f"**🕐 Last Error:**\n"
            f"• {last_error_info}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_Health checks run automatically every hour_",
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Owner {message.from_user.id} checked bot health")
        
    except Exception as e:
        await message.answer(f"❌ **Error:** {str(e)}", parse_mode=ParseMode.MARKDOWN)
        logger.error(f"Error in health command: {e}")

# ==========================================
# 🏥 ENTERPRISE AUTO-HEALER & HEALTH SYSTEM
# ==========================================

# NOTE: health_stats is defined early at top of file.

# Exponential backoff: wait 1s, 2s, 4s, 8s, 16s (max)
_BACKOFF_BASE = 1
_BACKOFF_MAX = 16
_MAX_HEAL_RETRIES = 5

# Per-alert cooldown tracker to prevent notification spam:
# Format: {"{severity}:{error_type}": last_sent_datetime}
_last_owner_alert: dict = {}
# Cooldown seconds per severity level
_NOTIFY_COOLDOWNS = {"WARNING": 1800, "ERROR": 600, "CRITICAL": 120}

async def notify_owner(error_type: str, error_msg: str, severity: str = "CRITICAL", auto_healed: bool = False):
    """Instantly notify owner of errors via Telegram with full context.

    Severity levels: WARNING | ERROR | CRITICAL
    Duplicate alerts of the same type+severity are suppressed within the cooldown window.
    """
    try:
        # --- Cooldown / deduplication ---
        cooldown = _NOTIFY_COOLDOWNS.get(severity, 600)
        alert_key = f"{severity}:{error_type}"
        last_sent = _last_owner_alert.get(alert_key)
        if last_sent:
            elapsed = (datetime.now(TZ) - last_sent).total_seconds()
            if elapsed < cooldown:
                logger.debug(f"[notify_owner] Suppressing {severity} alert '{error_type}' ({cooldown - elapsed:.0f}s left)")
                return
        _last_owner_alert[alert_key] = datetime.now(TZ)
        # --- end cooldown ---

        health_stats["owner_notified"] += 1

        emoji_map = {"CRITICAL": "🔴", "ERROR": "🟠", "WARNING": "🟡"}
        emoji = emoji_map.get(severity, "🟡")
        heal_status = "✅ AUTO-HEALED" if auto_healed else "❌ MANUAL FIX NEEDED"

        now_tz = datetime.now(TZ)
        uptime = now_tz - health_stats["bot_start_time"]
        hours = int(uptime.total_seconds() // 3600)
        minutes = int((uptime.total_seconds() % 3600) // 60)

        # Truncate error for Telegram (4096 char limit)
        safe_error = str(error_msg)[:600].replace("`", "'")

        notification = (
            f"{emoji} **BOT 8 — HEALTH ALERT**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**Severity:** `{severity}`\n"
            f"**Type:** `{error_type}`\n"
            f"**Status:** {heal_status}\n\n"
            f"**Error Details:**\n"
            f"```\n{safe_error}\n```\n\n"
            f"**Bot Statistics:**\n"
            f"• Uptime: {hours}h {minutes}m\n"
            f"• Errors Caught: {health_stats['errors_caught']}\n"
            f"• Auto-Healed: {health_stats['auto_healed']}\n"
            f"• Owner Alerts: {health_stats['owner_notified']}\n"
            f"• DB Reconnects: {health_stats['db_reconnects']}\n\n"
            f"**Timestamp:** {now_tz.strftime('%B %d, %Y — %I:%M:%S %p %Z')}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 MSA NODE Bot 8 — Health Monitor"
        )

        await bot.send_message(OWNER_ID, notification, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"📢 Owner notified of {severity}: {error_type}")

    except Exception as e:
        logger.error(f"❌ Failed to notify owner: {e}")


async def auto_heal(error_type: str, error: Exception, context: dict = None) -> bool:
    """Attempt automatic healing with exponential backoff retry.

    Returns True if healing succeeded.
    """
    error_str = str(error).lower()
    tb = traceback.format_exc()

    for attempt in range(1, _MAX_HEAL_RETRIES + 1):
        wait = min(_BACKOFF_BASE * (2 ** (attempt - 1)), _BACKOFF_MAX)
        try:
            logger.warning(f"🏥 Auto-heal attempt {attempt}/{_MAX_HEAL_RETRIES} for: {error_type}")

            # ── Database / MongoDB ──────────────────────────────────
            if any(k in error_str for k in ("mongo", "database", "pymongo", "serverselection")):
                logger.info("🔌 Attempting database reconnection...")
                client.admin.command('ping')
                logger.info("✅ Database connection restored!")
                health_stats["auto_healed"] += 1
                health_stats["db_reconnects"] += 1
                return True

            # ── Telegram FloodWait / RetryAfter ─────────────────────
            if isinstance(error, TelegramRetryAfter):
                retry_after = error.retry_after + 1
                logger.info(f"⏳ Telegram FloodWait: sleeping {retry_after}s")
                await asyncio.sleep(retry_after)
                health_stats["auto_healed"] += 1
                return True

            # ── Generic timeout / network ───────────────────────────
            if any(k in error_str for k in ("timeout", "timed out", "read timeout")):
                logger.info(f"⏱️ Timeout — waiting {wait}s before retry")
                await asyncio.sleep(wait)
                health_stats["auto_healed"] += 1
                return True

            if any(k in error_str for k in ("connection", "network", "socket", "eof", "ssl")):
                logger.info(f"🔄 Network error — waiting {wait}s")
                await asyncio.sleep(wait)
                health_stats["auto_healed"] += 1
                return True

            # ── Rate-limit (non-Telegram) ──────────────────────────
            if "rate limit" in error_str:
                logger.info(f"🚦 Rate limit — waiting {wait}s")
                await asyncio.sleep(wait)
                health_stats["auto_healed"] += 1
                return True

            # ── Unknown ─────────────────────────────────────────────
            logger.warning(f"❓ Unknown error type — cannot auto-heal: {error_type}")
            return False

        except Exception as heal_err:
            logger.error(f"❌ Healing attempt {attempt} failed: {heal_err}")
            if attempt < _MAX_HEAL_RETRIES:
                await asyncio.sleep(wait)

    logger.error(f"💀 All {_MAX_HEAL_RETRIES} healing attempts exhausted for: {error_type}")
    return False


async def health_monitor():
    """Background task: ping DB + bot every hour, alert owner on failure."""
    while True:
        try:
            await asyncio.sleep(3600)  # Every hour

            # ── DB ping ──────────────────────────────────────────────
            try:
                client.admin.command('ping')
                logger.info("✅ Hourly health check: Database OK")
            except Exception as e:
                logger.error(f"❌ Hourly health check: DB FAILED — {e}")
                healed = await auto_heal("DB Health Check", e)
                await notify_owner("Database Health Check", str(e), "ERROR" if healed else "CRITICAL", healed)

            # ── Bot API ping ──────────────────────────────────────────
            try:
                me = await bot.get_me()
                logger.info(f"✅ Hourly health check: Bot OK (@{me.username})")
            except Exception as e:
                logger.error(f"❌ Hourly health check: Bot API FAILED — {e}")
                healed = await auto_heal("Bot API Check", e)
                await notify_owner("Bot API Connection", str(e), "CRITICAL", healed)

        except Exception as e:
            logger.error(f"❌ Health monitor loop error: {e}")


async def global_error_handler(update: types.Update, exception: Exception):
    """Catch ALL unhandled errors from dispatcher and attempt auto-healing."""
    try:
        health_stats["errors_caught"] += 1
        health_stats["last_error"] = datetime.now(TZ)
        health_stats["last_error_msg"] = str(exception)[:200]

        error_type = type(exception).__name__
        error_msg = str(exception)
        tb = traceback.format_exc()

        logger.error(f"❌ Unhandled {error_type}: {error_msg}\n{tb[:800]}")

        # Skip logging of harmless Telegram errors
        if isinstance(exception, TelegramAPIError):
            if "message is not modified" in error_msg.lower():
                return True  # Harmless, don't alert owner
            if "message to delete not found" in error_msg.lower():
                return True

        # Attempt healing
        healed = await auto_heal(error_type, exception, {"update": update})

        # Severity determination
        if isinstance(exception, TelegramRetryAfter):
            severity = "WARNING"
        elif "critical" in error_msg.lower() or "fatal" in error_msg.lower():
            severity = "CRITICAL"
        elif healed:
            severity = "WARNING"
        else:
            severity = "ERROR"

        # Always alert owner (even for auto-healed errors) unless WARNING
        if severity != "WARNING" or not healed:
            await notify_owner(error_type, f"{error_msg}\n\nTraceback:\n{tb[:400]}", severity, healed)

        logger.info(f"🏥 Error handled — Auto-healed: {healed}")
        return True

    except Exception as e:
        logger.critical(f"💥 Error handler itself crashed: {e}")
        try:
            await bot.send_message(
                OWNER_ID,
                f"🔴🔴🔴 **CRITICAL — ERROR HANDLER CRASHED**\n\n```{str(e)[:300]}```",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass
        return False


# ==========================================
# 🗑️ AUTO-EXPIRE TICKETS SYSTEM
# ==========================================

TICKET_EXPIRE_DAYS = int(os.getenv("TICKET_EXPIRE_DAYS", 7))  # Days after resolution to auto-archive

async def auto_expire_tickets():
    """Background task: archive old resolved tickets every 24 hours."""
    while True:
        try:
            await asyncio.sleep(86400)  # 24 hours

            expire_date = now_local() - timedelta(days=TICKET_EXPIRE_DAYS)
            old_tickets = list(col_support_tickets.find({
                "status": "resolved",
                "resolved_at": {"$lt": expire_date}
            }))

            archived_count = 0
            for ticket in old_tickets:
                col_support_tickets.update_one(
                    {"_id": ticket["_id"]},
                    {"$set": {"status": "archived", "archived_at": now_local()}}
                )
                archived_count += 1

            if archived_count > 0:
                logger.info(f"🗑️ Auto-archived {archived_count} resolved tickets (>{TICKET_EXPIRE_DAYS} days old)")
            else:
                logger.info("✅ Ticket cleanup: nothing to archive")

        except Exception as e:
            logger.error(f"❌ Auto-expire tickets error: {e}")


# ==========================================
# 📊 TWICE-DAILY REPORT SYSTEM (8:40 AM & PM)
# ==========================================

async def _build_daily_report(period: str) -> str:
    """Build a comprehensive report string for owner."""
    now_tz = datetime.now(TZ)
    uptime = now_tz - health_stats["bot_start_time"]
    days = int(uptime.total_seconds() // 86400)
    hours = int((uptime.total_seconds() % 86400) // 3600)
    minutes = int((uptime.total_seconds() % 3600) // 60)

    # ── DB Stats (run in executor to avoid blocking) ─────────────
    loop = asyncio.get_event_loop()

    def _get_stats():
        total_users = col_user_verification.count_documents({})
        verified_users = col_user_verification.count_documents({"verified": True})
        total_msa_ids = col_msa_ids.count_documents({})
        open_tickets = col_support_tickets.count_documents({"status": "open"})
        resolved_tickets = col_support_tickets.count_documents({"status": "resolved"})
        archived_tickets = col_support_tickets.count_documents({"status": "archived"})
        banned_users = col_banned_users.count_documents({})
        total_pdfs = col_pdfs.count_documents({})
        total_ig_content = col_ig_content.count_documents({})

        # New users today
        today_start = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
        new_today = col_user_verification.count_documents({
            "first_start": {"$gte": today_start}
        })

        # Clicks today
        total_clicks_today = (
            col_pdfs.aggregate([
                {"$group": {"_id": None, "total": {"$sum": "$clicks"}}}
            ])
        )
        clicks_sum = 0
        for c in total_clicks_today:
            clicks_sum = c.get("total", 0)

        # DB ping
        try:
            client.admin.command('ping')
            db_status_str = "✅ ONLINE"
        except Exception:
            db_status_str = "❌ OFFLINE"

        return {
            "total_users": total_users,
            "verified_users": verified_users,
            "total_msa_ids": total_msa_ids,
            "open_tickets": open_tickets,
            "resolved_tickets": resolved_tickets,
            "archived_tickets": archived_tickets,
            "banned_users": banned_users,
            "total_pdfs": total_pdfs,
            "total_ig_content": total_ig_content,
            "new_today": new_today,
            "clicks_sum": clicks_sum,
            "db_status": db_status_str,
        }

    stats = await loop.run_in_executor(None, _get_stats)

    # ── Build success-rate ──────────────────────────────────────
    total_errors = health_stats["errors_caught"]
    healed = health_stats["auto_healed"]
    success_rate = (healed / total_errors * 100) if total_errors > 0 else 100.0

    report = (
        f"📊 **BOT 8 — {period} REPORT**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🕐 **Time:** {now_tz.strftime('%I:%M %p')} {REPORT_TIMEZONE}\n"
        f"📅 **Date:** {now_tz.strftime('%B %d, %Y')}\n\n"
        f"━━ ⏱️ UPTIME ━━\n"
        f"• Running since: {health_stats['bot_start_time'].strftime('%b %d, %I:%M %p')}\n"
        f"• Total uptime: {days}d {hours}h {minutes}m\n\n"
        f"━━ 👥 USERS ━━\n"
        f"• Total registered: `{stats['total_users']}`\n"
        f"• Verified (vault): `{stats['verified_users']}`\n"
        f"• MSA+ IDs assigned: `{stats['total_msa_ids']}`\n"
        f"• New today: `{stats['new_today']}`\n"
        f"• Banned: `{stats['banned_users']}`\n\n"
        f"━━ 📦 CONTENT ━━\n"
        f"• PDFs in DB: `{stats['total_pdfs']}`\n"
        f"• IG Content: `{stats['total_ig_content']}`\n"
        f"• Total content clicks: `{stats['clicks_sum']}`\n\n"
        f"━━ 🎫 SUPPORT TICKETS ━━\n"
        f"• Open: `{stats['open_tickets']}`\n"
        f"• Resolved: `{stats['resolved_tickets']}`\n"
        f"• Archived: `{stats['archived_tickets']}`\n\n"
        f"━━ 🏥 HEALTH ━━\n"
        f"• Database: {stats['db_status']}\n"
        f"• Errors caught: `{total_errors}`\n"
        f"• Auto-healed: `{healed}`\n"
        f"• Heal success rate: `{success_rate:.1f}%`\n"
        f"• DB reconnects: `{health_stats['db_reconnects']}`\n"
        f"• Owner alerts sent: `{health_stats['owner_notified']}`\n"
        f"• Reports sent: `{health_stats['reports_sent']}`\n"
        f"• Last error: {health_stats['last_error'].strftime('%I:%M %p') if health_stats['last_error'] else 'None'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 MSA NODE Bot 8 — Auto-Report"
    )
    return report


async def send_daily_report(period: str):
    """Send a report to owner. period = 'MORNING' or 'EVENING'."""
    try:
        report = await _build_daily_report(period)
        await bot.send_message(OWNER_ID, report, parse_mode=ParseMode.MARKDOWN)
        health_stats["reports_sent"] += 1
        logger.info(f"📊 {period} report sent to owner")
    except Exception as e:
        logger.error(f"❌ Failed to send {period} report: {e}")


async def daily_report_scheduler():
    """Background task: fire reports at 8:40 AM and 8:40 PM (owner's timezone)."""
    logger.info(f"📅 Daily report scheduler started (timezone: {REPORT_TIMEZONE})")
    report_times = [
        (REPORT_HOUR_AM, REPORT_MIN_AM, "MORNING (8:40 AM)"),
        (REPORT_HOUR_PM, REPORT_MIN_PM, "EVENING (8:40 PM)"),
    ]

    while True:
        try:
            now = datetime.now(TZ)
            # Find next report time
            next_fire = None
            next_label = None
            for hour, minute, label in report_times:
                candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if candidate <= now:
                    candidate += timedelta(days=1)
                if next_fire is None or candidate < next_fire:
                    next_fire = candidate
                    next_label = label

            wait_secs = (next_fire - datetime.now(TZ)).total_seconds()
            logger.info(f"📅 Next report '{next_label}' in {int(wait_secs // 3600)}h {int((wait_secs % 3600) // 60)}m")

            await asyncio.sleep(max(wait_secs, 1))
            await send_daily_report(next_label)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Daily report scheduler error: {e}")
            await asyncio.sleep(60)  # Back off 1 min on error


# ==========================================
# 💾 STATE PERSISTENCE — Remember on restart
# ==========================================

def save_bot_state(key: str, value: dict):
    """Persist a key-value state to MongoDB so it survives restarts."""
    try:
        db["bot8_state_persistence"].update_one(
            {"key": key},
            {"$set": {"key": key, "value": value, "updated_at": now_local()}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"❌ Failed to save bot state '{key}': {e}")


def load_bot_state(key: str) -> dict:
    """Load a persisted state from MongoDB. Returns {} if not found."""
    try:
        doc = db["bot8_state_persistence"].find_one({"key": key})
        if doc:
            return doc.get("value", {})
    except Exception as e:
        logger.error(f"❌ Failed to load bot state '{key}': {e}")
    return {}


def restore_health_stats_from_db():
    """Restore cumulative health_stats counters from last run."""
    saved = load_bot_state("health_stats_cumulative")
    if saved:
        # Restore cumulative counters (but reset session-specific ones)
        health_stats["errors_caught"] = saved.get("errors_caught", 0)
        health_stats["auto_healed"] = saved.get("auto_healed", 0)
        health_stats["owner_notified"] = saved.get("owner_notified", 0)
        health_stats["db_reconnects"] = saved.get("db_reconnects", 0)
        health_stats["reports_sent"] = saved.get("reports_sent", 0)
        logger.info(f"💾 Health stats restored from DB (errors: {health_stats['errors_caught']}, healed: {health_stats['auto_healed']})")


# ==========================================
# 💾 AUTO-BACKUP SYSTEM — Bot 8 (every 12 hours)
# ==========================================
_BOT8_LAST_BACKUP_KEY = "bot8_last_auto_backup"

async def auto_backup_bot8():
    """Run a full Bot 8 data backup every 12 hours into bot8_backups collection."""
    while True:
        try:
            now = now_local()

            # AM / PM label for display
            period = "AM" if now.hour < 12 else "PM"
            timestamp_label = now.strftime(f"%B %d, %Y — %I:%M {period}")  # e.g. February 18, 2026 — 08:40 AM
            timestamp_key  = now.strftime("%Y-%m-%d_%I-%M-%S_") + period   # file-safe key
            window_key     = now.strftime("%Y-%m-%d_") + period             # e.g. "2026-02-19_AM"

            # ✅ Dedup: skip if a backup for this 12 h window already exists
            if col_bot8_backups.count_documents({"window_key": window_key}) > 0:
                logger.info(f"⚠️  Bot8 auto-backup SKIPPED — window {window_key} already stored")
                await asyncio.sleep(12 * 3600)
                continue

            logger.info(f"💾 BOT 8 AUTO-BACKUP STARTING — {timestamp_label}")

            collections_to_backup = [
                ("user_verification",  col_user_verification),
                ("msa_ids",            col_msa_ids),
                ("bot9_pdfs",          col_pdfs),
                ("bot9_ig_content",    col_ig_content),
                ("support_tickets",    col_support_tickets),
                ("banned_users",       col_banned_users),
                ("suspended_features", col_suspended_features),
            ]

            collection_counts = {}
            total_records = 0
            BATCH_SIZE = 5000

            start_time = now_local()
            for col_name, collection in collections_to_backup:
                try:
                    count = collection.count_documents({})
                    collection_counts[col_name] = count
                    total_records += count
                except Exception as ce:
                    logger.warning(f"⚠️ Bot8 backup — could not count {col_name}: {ce}")
                    collection_counts[col_name] = 0

            processing_time = (now_local() - start_time).total_seconds()

            backup_summary = {
                "bot":              "bot8",
                "backup_date":     now,
                "backup_type":     "automatic_12h",
                "timestamp":       timestamp_key,
                "timestamp_label": timestamp_label,
                "window_key":      now.strftime("%Y-%m-%d_") + period,  # e.g. "2026-02-19_AM"
                "period":          period,              # "AM" or "PM"
                "year":            now.year,
                "month":           now.strftime("%B"),  # e.g. "February"
                "day":             now.day,
                "hour_12":         now.strftime("%I").lstrip("0") or "12",  # 12-h no leading zero
                "minute":          now.strftime("%M"),
                "total_records":   total_records,
                "collection_counts": collection_counts,
                "processing_time": processing_time,
            }

            col_bot8_backups.insert_one(backup_summary)

            # Keep last 60 backups (30 days × 2/day)
            backup_count = col_bot8_backups.count_documents({})
            if backup_count > 60:
                old = list(col_bot8_backups.find({}).sort("backup_date", 1).limit(backup_count - 60))
                col_bot8_backups.delete_many({"_id": {"$in": [b["_id"] for b in old]}})

            logger.info(
                f"✅ Bot 8 auto-backup done — {total_records:,} records | "
                f"{processing_time:.2f}s | Period: {period} | Kept ≤60 backups"
            )

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Bot 8 auto-backup error: {e}")

        # Sleep exactly 12 hours then run again
        await asyncio.sleep(12 * 3600)


async def periodic_state_saver():
    """Save health_stats to DB every 5 minutes so restarts don't lose counts."""
    while True:
        try:
            await asyncio.sleep(300)  # Every 5 minutes
            save_bot_state("health_stats_cumulative", {
                "errors_caught": health_stats["errors_caught"],
                "auto_healed": health_stats["auto_healed"],
                "owner_notified": health_stats["owner_notified"],
                "db_reconnects": health_stats["db_reconnects"],
                "reports_sent": health_stats["reports_sent"],
                "last_saved": now_local().isoformat(),
            })
            logger.debug("💾 Health stats auto-saved to DB")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ State saver error: {e}")


# ==========================================
# 🌐 RENDER HEALTH CHECK WEB SERVER
# Render requires a web service to respond on $PORT — this lightweight
# aiohttp server satisfies that requirement alongside the bot polling.
# ==========================================

async def _health_handler(request: aiohttp_web.Request) -> aiohttp_web.Response:
    """Health check endpoint — Render pings this to confirm the service is alive."""
    uptime = datetime.now(TZ) - health_stats["bot_start_time"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)
    return aiohttp_web.json_response({
        "status": "ok",
        "bot": "MSA NODE Agent",
        "uptime": f"{h}h {m}m",
        "errors_caught": health_stats["errors_caught"],
        "auto_healed": health_stats["auto_healed"],
    })


async def start_health_server():
    """Start the lightweight aiohttp web server for Render health checks.

    Only starts when PORT is explicitly set in the environment (i.e. running on
    Render).  On local dev the env var is absent so we skip binding entirely,
    avoiding WinError 10048 / EADDRINUSE clashes.
    """
    if "PORT" not in os.environ:
        logger.info("🌐 Health server skipped (PORT not set — local dev mode)")
        return None
    app = aiohttp_web.Application()
    app.router.add_get("/health", _health_handler)
    app.router.add_get("/", _health_handler)  # Render also checks root
    runner = aiohttp_web.AppRunner(app)
    await runner.setup()
    site = aiohttp_web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌐 Health check server listening on port {PORT}")
    return runner


# ==========================================
# 🚀 MAIN FUNCTION — Enterprise Launch
# ==========================================

async def main():
    """Start Bot 8 with all enterprise background tasks."""
    tasks = []
    health_runner = None

    try:
        logger.info("🚀 MSA NODE AGENT Bot 8 — Enterprise startup...")

        # ── Restore persisted state ──────────────────────────────
        health_stats["bot_start_time"] = datetime.now(TZ)
        restore_health_stats_from_db()

        # ── Register global error handler ────────────────────────
        dp.errors.register(global_error_handler)
        logger.info("🏥 Global error handler + auto-healer registered")

        # ── Register live terminal middleware ────────────────────
        dp.message.middleware(Bot8TerminalMiddleware())
        log_to_terminal("STARTUP", 0, "Bot 8 online — live terminal active")
        logger.info("🖥️ Live terminal middleware registered (logs visible in Bot 10)")

        # ── Start Render health check web server ─────────────────
        health_runner = await start_health_server()

        # ── Start background tasks ───────────────────────────────
        tasks = [
            asyncio.create_task(health_monitor(),          name="health_monitor"),
            asyncio.create_task(auto_expire_tickets(),     name="ticket_archiver"),
            asyncio.create_task(daily_report_scheduler(),  name="daily_reports"),
            asyncio.create_task(periodic_state_saver(),    name="state_saver"),
            asyncio.create_task(auto_backup_bot8(),        name="auto_backup"),
        ]
        logger.info(f"✅ {len(tasks)} background tasks started: {[t.get_name() for t in tasks]}")

        # ── Startup notification to owner ────────────────────────
        try:
            now_tz = datetime.now(TZ)
            saved = load_bot_state("health_stats_cumulative")
            continued_from = saved.get("last_saved", "N/A")
            await bot.send_message(
                OWNER_ID,
                f"✅ <b>BOT 8 — ONLINE &amp; READY</b>\n\n"
                f"🏥 Auto-Healer: ✅ Active\n"
                f"💊 Health Monitor: ✅ Running (hourly)\n"
                f"📊 Daily Reports: ✅ Scheduled (8:40 AM &amp; PM {REPORT_TIMEZONE})\n"
                f"🗑️ Ticket Archiver: ✅ Active\n"
                f"💾 State Persistence: ✅ Enabled\n"
                f"🗄️ Auto-Backup: ✅ Every 12h — bot8_backups\n\n"
                f"<b>Started:</b> {now_tz.strftime('%B %d, %Y — %I:%M:%S %p %Z')}\n"
                f"<b>Continued from save:</b> {continued_from}\n\n"
                f"<i>All systems operational — Scaling ready</i>",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning(f"Could not send startup notification: {e}")

        # ── Start polling ────────────────────────────────────────
        logger.info("🔄 Starting polling...")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

    except Exception as e:
        logger.critical(f"💥 Fatal startup error: {e}\n{traceback.format_exc()}")
        try:
            await notify_owner("Bot 8 Startup FATAL", f"{e}\n{traceback.format_exc()[:500]}", "CRITICAL", False)
        except Exception:
            pass
        raise

    finally:
        # ── Save final state before exit ─────────────────────────
        try:
            save_bot_state("health_stats_cumulative", {
                "errors_caught": health_stats["errors_caught"],
                "auto_healed": health_stats["auto_healed"],
                "owner_notified": health_stats["owner_notified"],
                "db_reconnects": health_stats["db_reconnects"],
                "reports_sent": health_stats["reports_sent"],
                "last_saved": now_local().isoformat(),
            })
            logger.info("💾 Final state saved to DB")
        except Exception:
            pass

        # ── Cancel all background tasks ──────────────────────────
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"🛑 Task '{task.get_name()}' stopped")

        # ── Shutdown notification ────────────────────────────────
        try:
            now_tz = datetime.now(TZ)
            uptime = now_tz - health_stats["bot_start_time"]
            h = int(uptime.total_seconds() // 3600)
            m = int((uptime.total_seconds() % 3600) // 60)
            await bot.send_message(
                OWNER_ID,
                f"🛑 **BOT 8 — SHUTDOWN**\n\n"
                f"**Uptime this session:** {h}h {m}m\n"
                f"**Errors Caught:** {health_stats['errors_caught']}\n"
                f"**Auto-Healed:** {health_stats['auto_healed']}\n"
                f"**Owner Alerts:** {health_stats['owner_notified']}\n"
                f"**Reports Sent:** {health_stats['reports_sent']}\n\n"
                f"**Shutdown at:** {now_tz.strftime('%B %d, %Y — %I:%M:%S %p %Z')}\n\n"
                f"_State persisted. Will resume counts on restart._",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

        try:
            await bot.session.close()
        except Exception:
            pass

        # ── Stop health check web server ─────────────────────────
        if health_runner:
            try:
                await health_runner.cleanup()
                logger.info("🌐 Health check server stopped")
            except Exception:
                pass

        logger.info("✅ Bot 8 shutdown complete")


# ==========================================
# 🏁 ENTRY POINT — With auto-restart wrapper
# ==========================================

if __name__ == "__main__":
    _restart_delay = 5  # seconds between restarts
    while True:
        try:
            asyncio.run(main())
            # main() only returns on clean shutdown → don't restart
            logger.info("✅ Clean shutdown. Exiting.")
            break
        except KeyboardInterrupt:
            logger.info("⚠️ Bot stopped by user (Ctrl+C)")
            break
        except SystemExit:
            logger.info("⚠️ SystemExit received. Stopping.")
            break
        except Exception as e:
            logger.critical(f"💥 Unhandled top-level crash: {e}\n{traceback.format_exc()}")
            logger.info(f"♻️ Auto-restarting in {_restart_delay} seconds...")
            # Exponential backoff on crash (cap at 60 s)
            time.sleep(_restart_delay)
            _restart_delay = min(_restart_delay * 2, 60)
