import asyncio
import logging
import random
import html
import threading
from aiohttp import web
import uuid
import re
import os
import aiohttp
import pytz
import json
import io
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Any
import psutil
from aiogram.filters import Command, StateFilter, or_f
import time
import google.generativeai as genai
from aiogram.filters import or_f
import pymongo

# Track when the bot was launched
START_TIME = time.time()
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError

# ==========================================
# ⚡ CONFIGURATION (SECURED)
# ==========================================
BOT_TOKEN = os.getenv("BOT_5_TOKEN")
GEMINI_KEY = os.getenv("GEMINI_KEY")
MONGO_URI = os.getenv("MONGO_URI")

# IDENTITY HIDDEN: Pulled from Environment. No plain text IDs remain.
OWNER_ID_RAW = os.getenv("MASTER_ADMIN_ID")
CHANNEL_ID_RAW = os.getenv("MAIN_CHANNEL_ID")
LOG_CHANNEL_ID_RAW = os.getenv("LOG_CHANNEL_ID")

if not all([BOT_TOKEN, GEMINI_KEY, MONGO_URI, OWNER_ID_RAW, CHANNEL_ID_RAW, LOG_CHANNEL_ID_RAW]):
    print("❌ Bot 5 Error: Missing AI or Bot credentials in Render Environment!")
    import sys
    sys.exit(1)

OWNER_ID = int(OWNER_ID_RAW)
CHANNEL_ID = int(CHANNEL_ID_RAW)
LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_RAW)

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Kolkata'))
# MongoDB Connection
db_client = None
db = None
col_system_stats = None
col_api_ledger = None
col_vault = None
col_schedules = None
col_settings = None
MODEL_POOL = ["gemini-2.5-flash", "gemini-2.5-pro","gemini-2.5-flash-preview-09-2025","gemini-2.5-flash-lite","gemini-2.5-flash-lite-preview-09-2025"]
API_USAGE_COUNT = 0
CONSOLE_LOGS = [] # Required for your terminal_viewer
PENDING_APPROVALS = {} # Required for your scheduling logic
model = None
# ==========================================
# 🛠 SETUP
# ==========================================
def connect_db():
    global db_client, db, col_system_stats, col_api_ledger, col_vault, col_schedules, col_settings
    try:
        db_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = db_client["MSANodeDB"]
        col_system_stats = db["system_stats"]
        col_api_ledger = db["api_ledger"]
        col_vault = db["vault"]
        col_schedules = db["schedules"]
        col_settings = db["settings"]
        db_client.server_info()
        logging.info("MongoDB connected successfully")
        return True
    except Exception as e:
        logging.error(f"MongoDB Connect Error: {e}")
        return False

connect_db()
# Global variables for bot state
CURRENT_MODEL_INDEX = 0 
#1. State Definition (Must be above handlers)
class APIState(StatesGroup):
    waiting_api = State()

@dp.message(or_f(F.text.contains("API"), Command("api")), StateFilter("*"))
async def api_management(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return

    # 1. Clear any old state before starting
    await state.clear()

    global GEMINI_KEY
    key_hash = GEMINI_KEY[-8:]

    ledger = col_api_ledger.find_one({"key_hash": key_hash})
    current_usage = ledger.get("usage_count", 0) if ledger else 0

    # Inline Keyboard with Cancel/Back button
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 CANCEL / BACK", callback_data="cancel_api")]
    ])
    
    await message.answer(
        "📊 <b>API TELEMETRY REPORT</b>\n"
        "────────────────────\n"
        f"KEY: <code>****{key_hash}</code>\n"
        f"LIFETIME USAGE: <code>{current_usage} Requests</code>\n"
        f"EST. QUOTA: <code>Free Tier (~1.5k/day)</code>\n"
        "────────────────────\n"
        "📥 <b>Enter NEW Key or press BACK:</b>",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )
    await state.set_state(APIState.waiting_api)
# ==========================================
# 🔑 API HOT-SWAP (GHOST ROTATION)
# ==========================================
@dp.callback_query(F.data == "cancel_api")
async def cancel_handler(cb: types.CallbackQuery, state: FSMContext):
    """Resets the bot state and returns to menu."""
    await state.clear()
    await cb.message.edit_text("<b>🔙 NAVIGATION RESET.</b>\nSystem on Standby.", parse_mode=ParseMode.HTML)
    await cb.answer("State Cleared")
@dp.message(or_f(F.text.contains("API"), Command("api")), StateFilter("*"))
async def api_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    global GEMINI_KEY
    await state.clear()
    
    # Mask the key so it's safe if someone is looking over your shoulder
    masked_key = f"{GEMINI_KEY[:8]}****{GEMINI_KEY[-4:]}"
    
    await message.answer(
        "🔑 <b>API MANAGEMENT PROTOCOL</b>\n"
        f"CURRENT KEY: <code>{masked_key}</code>\n"
        "────────────────────\n"
        "📥 <b>Enter the NEW Gemini API Key:</b>\n"
        "<i>Note: This will re-initialize the AI Engine immediately.</i>",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(APIState.waiting_api)

@dp.message(APIState.waiting_api)
async def api_update(message: types.Message, state: FSMContext):
    global GEMINI_KEY, model
    new_key = message.text.strip()
    
    # Validation: Gemini keys usually start with 'AIza'
    if not new_key.startswith("AIza") or len(new_key) < 20:
        await message.answer("❌ <b>INVALID KEY:</b> That does not look like a valid Gemini API Key.")
        return

    try:
        # 1. Update Global Variable
        GEMINI_KEY = new_key
        
        # 2. Re-configure the Library
        genai.configure(api_key=GEMINI_KEY)
        
        # 3. Reset the Model instance
        # This forces generate_content() to re-build the model with the new key
        model = None 
        
        await message.answer(
            "🚀 <b>API ROTATION SUCCESSFUL</b>\n"
            "The Ghost Infrastructure is now utilizing the new credentials.\n"
            "────────────────────\n"
            "<b>STATUS:</b> Operational",
            parse_mode=ParseMode.HTML
        )
        console_out("System: API Key Rotated.")
        
    except Exception as e:
        await message.answer(f"❌ <b>RE-INIT FAILED:</b> {html.escape(str(e))}")
    
    await state.clear()
# ==========================================
# VIRTUAL CONSOLE STORAGE (MUST BE AT TOP)
# ==========================================

def console_out(text):
    global CONSOLE_LOGS
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {text}"
    CONSOLE_LOGS.append(entry)
    if len(CONSOLE_LOGS) > 12: 
        CONSOLE_LOGS.pop(0)
    logging.info(text)

async def get_api_usage_safe():
    try:
        stats = col_system_stats.find_one({"_id": 1})
        if not stats:
            col_system_stats.insert_one({
                "_id": 1,
                "api_total": 0,
                "last_reset": datetime.now()
            })
            return 0
        return stats.get("api_total", 0)
    except Exception:
        return 0

async def increment_api_count(api_key):
    """Increments the local persistent counter for the current key."""
    key_hash = api_key[-8:] # Use last 8 chars as a unique identifier
    try:
        # Try to find existing record
        existing = col_api_ledger.find_one({"key_hash": key_hash})

        if not existing:
            # Create new record if this key hasn't been used before
            col_api_ledger.insert_one({
                "key_hash": key_hash,
                "usage_count": 1
            })
            return 1
        else:
            # Increment existing record
            new_count = existing.get("usage_count", 0) + 1
            col_api_ledger.update_one(
                {"key_hash": key_hash},
                {"$set": {"usage_count": new_count}}
            )
            return new_count
    except Exception as e:
        logging.error(f"Error incrementing API count: {e}")
        return 0

@dp.message(F.text.contains("TERMINAL"), StateFilter("*"))
async def terminal_viewer(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    try:
        await state.clear()
        uptime_seconds = int(time.time() - START_TIME)
        uptime_str = str(timedelta(seconds=uptime_seconds))
        cpu = psutil.cpu_percent()
        ram = psutil.virtual_memory().percent
        current_api = await get_api_usage_safe()
        active_jobs = len(scheduler.get_jobs())
        log_feed = "\n".join(CONSOLE_LOGS) if CONSOLE_LOGS else "System Standby: No events logged yet."
        
        text = (
            "<b>◈ MSANODE REMOTE TERMINAL ◈</b>\n"
            "────────────────────\n"
            f"STATUS: ACTIVE | UPTIME: {uptime_str}\n"
            f"CPU: {cpu}% | RAM: {ram}%\n"
            f"DATABASE: CONNECTED | JOBS: {active_jobs}\n"
            f"API USAGE: {current_api}/1500\n"
            "────────────────────\n"
            "LIVE FEED:\n"
            f"<code>{log_feed}</code>\n"
            "────────────────────"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="REFRESH", callback_data="refresh_term")]])
        await message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"TERMINAL ERROR: {str(e)}")

@dp.callback_query(F.data == "refresh_term")
async def refresh_terminal(cb: types.CallbackQuery, state: FSMContext):
    uptime_seconds = int(time.time() - START_TIME)
    uptime_str = str(timedelta(seconds=uptime_seconds))
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    current_api = await get_api_usage_safe()
    active_jobs = len(scheduler.get_jobs())
    log_feed = "\n".join(CONSOLE_LOGS) if CONSOLE_LOGS else "Awaiting system events..."

    new_text = (
        "<b>◈ MSANODE REMOTE TERMINAL ◈</b>\n"
        "────────────────────\n"
        f"STATUS: ACTIVE | UPTIME: {uptime_str}\n"
        f"CPU: {cpu}% | RAM: {ram}%\n"
        f"DATABASE: CONNECTED | JOBS: {active_jobs}\n"
        f"API USAGE: {current_api}/1500\n"
        "────────────────────\n"
        "<b>LIVE FEED:</b>\n"
        f"<code>{log_feed}</code>\n"
        "────────────────────"
    )
    try:
        await cb.message.edit_text(new_text, reply_markup=cb.message.reply_markup, parse_mode=ParseMode.HTML)
        await cb.answer("Terminal Synchronized.")
    except:
        await cb.answer()

# ==========================================
# 🧠 ORACLE PROMPT ENGINE (PROJECT CHIMERA)
# ==========================================
def get_system_prompt():
    return """
    ACT AS: 'MSANODE OVERLORD'. 
    GOAL: Deliver an 'Unfair Advantage' resource (AI side hustles/Arbitrage/Tactical Tech).
    TONE: Exclusive, Urgent, Technical, Military-Grade Scarcity.
    
    STRICT CONSTRAINTS:
    - COLD START: Begin IMMEDIATELY with '━━━━━━━━━━━━━━━━━━━━' followed by the '🚨 OPERATION' header.
    - NO PRE-TEXT: Never explain your mandate, never use disclaimers, and never say "I cannot generate viral content." 
    - DIRECT LINKS: Provide REAL, EXTERNAL HTTPS LINKS to the actual tools (e.g., chain.link, openai.com, ankr.com).
    - NO BRANDING: Do not create fake msanode.net links. Provide the source tools directly.
    - FORMATTING: NO EMOJIS in body text. Emojis allowed ONLY in headers.
    - NO AI FILLER.

    STRUCTURE:
    ━━━━━━━━━━━━━━━━━━━━
    🚨 OPERATION: [CAPITALIZED TITLE] 🚨
    ━━━━━━━━━━━━━━━━━━━━
    🧠 THE ADVANTAGE: [Explain arbitrage/logic/side-hustle]
    ⚠️ RESTRICTED TOOLKIT:
    • 1. [Real Tool Name]: [Specific Benefit] (Link: [Direct URL])
    • 2. [Real Tool Name]: [Specific Benefit] (Link: [Direct URL])
    • 3. [Real Tool Name]: [Specific Benefit] (Link: [Direct URL])
    ⚡ EXECUTION PROTOCOL: [Direct technical steps to earn/deploy]
    👑 MSA NODE DIRECTIVE: "Family: Execute. Action is currency. Hurry Up !!! .Claim Free Rewards Now"
    ━━━━━━━━━━━━━━━━━━━━
    """

genai.configure(api_key=GEMINI_KEY)

async def generate_content(prompt="Generate a viral AI side hustle reel script"):
    global model, API_USAGE_COUNT
    if API_USAGE_COUNT >= 1500:
        return "⚠️ API Limit Reached", "Limit"

    try:
        if model is None:
            model = genai.GenerativeModel(
                model_name=MODEL_POOL[CURRENT_MODEL_INDEX],
                system_instruction=get_system_prompt()
            )
            console_out(f"Oracle Online: {MODEL_POOL[CURRENT_MODEL_INDEX]}")

        response = await asyncio.to_thread(model.generate_content, prompt[:500])
        raw_text = response.text if response else "No response"
        # Project Chimera Stability Shield: HTML Escaping
        clean_content = html.escape(raw_text)[:3500] 
        
        API_USAGE_COUNT += 1
        await increment_api_count(GEMINI_KEY)
        return clean_content, "AI Directive"

    except Exception as e:
        err = str(e)
        console_out(f"CRITICAL GEN ERROR: {err}")
        return f"System Error: {html.escape(err)[:100]}", "Error"

async def alchemy_transform(raw_text):
    try:
        prompt = get_system_prompt() + f"\n\nINPUT DATA:\n{raw_text}\n\nINSTRUCTION: Rewrite into MSANODE Protocol."
        resp = await model.generate_content_async(prompt)
        return re.sub(r"^(Here is|Sure).*?\n", "", resp.text, flags=re.IGNORECASE).strip()
    except: return "⚠️ Alchemy Failed."
# --- RENDER PORT BINDER (SHIELD) ---
async def handle_health(request):
    return web.Response(text="CORE 5 (AI SINGULARITY) IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")
# ==========================================
# 📡 UTILITY LOGIC
# ==========================================

async def get_next_x_code(prefix="X"):
    """Generates sequential codes: X1, X2, X3... across breaches and schedules."""
    try:
        # Count documents in both vault and schedules to get a global sequential number
        vault_count = col_vault.count_documents({})
        sched_count = col_schedules.count_documents({})
        return f"{prefix}{vault_count + sched_count + 1}"
    except Exception as e:
        logging.error(f"X-Code Gen Error: {e}")
        return f"{prefix}{random.randint(100, 999)}"
REWARD_POOL = [
    "GitHub Student Pack ($200k in Premium Infrastructure)",
    "Top 7 AI Tools that make ChatGPT look like a Toy",
    "Google's Hidden Professional Cybersecurity Certification",
    

]

async def safe_send_message(chat_id, text, reply_markup=None):
    try:
        return await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except TelegramBadRequest:
        clean_text = html.escape(text).replace("*", "").replace("`", "").replace("_", "")
        return await bot.send_message(chat_id, clean_text, parse_mode=None, reply_markup=reply_markup)
    except TelegramNetworkError:
        await asyncio.sleep(2)
        try: return await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except: return None
# ==========================================
# [!] FSM STATES (MUST BE AT THE TOP)
# ==========================================

class ScheduleState(StatesGroup):
    waiting_time = State()
    waiting_month = State()
    waiting_year = State()
    selecting_days = State()

class BreachState(StatesGroup):
    selecting_mode = State()
    waiting_topic = State()
    waiting_reaction_count = State()

class EditState(StatesGroup):
    waiting_id = State()
    waiting_text = State()

class UnsendState(StatesGroup):
    waiting_id = State()

class HurryState(StatesGroup):
    waiting_code = State()
    waiting_duration = State()

class EngagementState(StatesGroup):
    waiting_code = State()
    waiting_count = State()

class BroadcastState(StatesGroup):
    waiting_msg = State()
## ==========================================
# 🎯 ENGAGEMENT CONTROL (PRIORITY ANCHORED)
# ==========================================

# Handler for "🎯 ENGAGEMENT", "ENGAGEMENT", or "/engagement"
@dp.message(or_f(F.text.contains("ENGAGEMENT"), Command("engagement")), StateFilter("*"))
async def engagement_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # Critical: Flush previous states
    await state.clear()
    
    await message.answer(
        "🎯 <b>ENGAGEMENT GATING ACTIVATED</b>\n"
        "Enter the <b>M-Code</b> to update reaction targets:", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(EngagementState.waiting_code)

@dp.message(EngagementState.waiting_code)
async def engagement_id_received(message: types.Message, state: FSMContext):
    m_code = message.text.upper().strip()

    try:
        # Verify the M-Code exists in our Vault
        entry = col_vault.find_one({"m_code": m_code})

        if entry:
            await state.update_data(target_code=m_code, msg_id=entry.get("msg_id"))
            await message.answer(
                f"✅ <b>ENTRY FOUND:</b> <code>{m_code}</code>\n"
                f"Current Lock: <code>{entry.get('reaction_lock', 0)}x</code> 🔥 reactions.\n\n"
                "📥 <b>Enter the NEW target reaction count (0 to remove lock):</b>",
                parse_mode=ParseMode.HTML
            )
            await state.set_state(EngagementState.waiting_count)
        else:
            await message.answer(f"❌ <b>ERROR:</b> M-Code <code>{m_code}</code> not found.")
    except Exception as e:
        logging.error(f"Error verifying M-code: {e}")
        await message.answer("❌ <b>DATABASE ERROR:</b> Could not verify M-code.")
        await state.clear()

@dp.message(EngagementState.waiting_count)
async def engagement_exec(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("⚠️ <b>INVALID INPUT:</b> Enter a numerical value.")
        return

    new_count = int(message.text)
    data = await state.get_data()
    m_code = data['target_code']
    msg_id = data['msg_id']

    try:
        # 1. Update Database
        col_vault.update_one(
            {"m_code": m_code},
            {"$set": {
                "reaction_lock": new_count,
                "is_unlocked": (new_count == 0),
                "last_verified": datetime.now()
            }}
        )

        # 2. Synchronize Telegram UI
        # We refresh the buttons on the actual channel post immediately
        await bot.edit_message_reply_markup(
            chat_id=CHANNEL_ID,
            message_id=msg_id,
            reply_markup=get_engagement_markup(m_code, lock=new_count, unlocked=(new_count == 0))
        )

        await message.answer(
            f"🚀 <b>GATING UPDATED:</b> <code>{m_code}</code>\n"
            f"New Threshold: <code>{new_count}x</code> 🔥",
            parse_mode=ParseMode.HTML
        )
        console_out(f"Gating Reset: {m_code} set to {new_count}")

    except Exception as e:
        await message.answer(f"❌ <b>SYNC FAILED:</b> {html.escape(str(e))}")

    await state.clear()
    # ==========================================
# 📡 UI HELPERS (MUST BE DEFINED EARLY)
# ==========================================

def get_engagement_markup(m_code, lock=0, unlocked=False):
    """
    Generates the reaction gating buttons.
    This function must be defined BEFORE any handlers call it.
    """
    if lock > 0 and not unlocked:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔒 UNLOCK AT {lock}x 🔥 REACTIONS", callback_data=f"lockmsg_{m_code}")]
        ])
    
    # Default state if lock is 0 or already unlocked
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔓 UNLOCK SECRET BONUS HACK", callback_data=f"reveal_{m_code}")]
    ])
# ==========================================
# 🕹️ FSM STATES
# ==========================================

class ScheduleState(StatesGroup):
    waiting_time = State(); waiting_month = State(); waiting_year = State(); selecting_days = State()
class BreachState(StatesGroup):
    selecting_mode = State(); waiting_topic = State(); waiting_reaction_count = State()
class EditState(StatesGroup):
    waiting_id = State(); waiting_text = State()
class UnsendState(StatesGroup):
    waiting_id = State()
class HurryState(StatesGroup):
    waiting_code = State(); waiting_duration = State()
class EngagementState(StatesGroup):
    waiting_code = State(); waiting_count = State()
class BroadcastState(StatesGroup):
    waiting_msg = State()

# ==========================================
# 🛡️ SYSTEM TASKS & ACTIVE LISTENER
# ==========================================
# ==========================================
# 🪤 REACTION LOCK INTERFACE HANDLERS
# ==========================================

@dp.callback_query(F.data.startswith("reveal_"))
async def reveal_secret(cb: types.CallbackQuery):
    """
    Triggers when the 'UNLOCK SECRET BONUS HACK' button is clicked.
    Provides verified engineering/AI resources.
    """
    hacks = [
        "◈ VPN Bypass: Protocol Verified.", 
        "◈ EDU Email: Access Granted.", 
        "◈ Archive Script: Script mirror active.",
        "◈ Premium Repo: Branch decrypted."
    ]
    # Sends a private alert to the user who clicked
    await cb.answer(random.choice(hacks), show_alert=True)

@dp.callback_query(F.data.startswith("lockmsg_"))
async def lock_alert(cb: types.CallbackQuery):
    """
    Triggers when the locked button is clicked.
    Informs the user about the remaining requirement.
    """
    await cb.answer(
        "◈ ACCESS RESTRICTED ◈\n"
        "Requirement: Reach the 🔥 reaction target to unlock this intelligence.", 
        show_alert=True
    )

# ==========================================
# 📊 UNIVERSAL REACTION LISTENER (COUNT-ONLY)
# ==========================================

@dp.message_reaction()
async def reaction_listener(reaction: types.MessageReactionUpdated):
    """
    Counts ANY reaction emoji. Once the total count across
    all emojis hits the target, the Vault unlocks.
    """
    try:
        # Search the Vault for this message ID
        entry = col_vault.find_one({
            "msg_id": reaction.message_id,
            "is_unlocked": False
        })

        # If entry is found and a lock exists
        if entry and entry.get("reaction_lock", 0) > 0:
            # Calculate Total Count across all emoji types
            total_reactions = 0
            for r in reaction.new_reaction:
                # This counts the total number of people who reacted
                # Telegram provides a list of reaction types and their counts
                # For channels, we sum the totals provided in the update
                total_reactions += 1 # Standard count per reactor

            # Check if we hit the goal
            if total_reactions >= entry.get("reaction_lock", 0):
                # 1. Update Database Status
                col_vault.update_one(
                    {"m_code": entry.get("m_code")},
                    {"$set": {"is_unlocked": True}}
                )

                # 2. Update Channel UI
                await bot.edit_message_reply_markup(
                    chat_id=CHANNEL_ID,
                    message_id=entry.get("msg_id"),
                    reply_markup=get_engagement_markup(entry.m_code, unlocked=True)
                )
                
                # 3. Notification of Breach
                await bot.send_message(
                    LOG_CHANNEL_ID,
                    f"🔓 <b>VAULT UNLOCKED:</b> <code>{entry.get('m_code')}</code>\n"
                    f"Threshold of <code>{entry.get('reaction_lock', 0)}</code> reactions reached."
                )
    except Exception as e:
        logging.error(f"Error in reaction listener: {e}")
    """
    Asynchronous link validation to ensure intelligence assets are live.
    """
    urls = re.findall(r'(https?://[^\s)]+)', text)
    invalid = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                async with session.get(url, timeout=5) as resp:
                    if resp.status >= 400: 
                        invalid.append(url)
            except Exception:
                invalid.append(url)
    return invalid

async def self_healing_audit():
    """
    Periodic deep-scan of vault integrity.
    """
    try:
        # Get recent vault entries from MongoDB
        recent_entries = list(col_vault.find().sort("created_at", -1).limit(50))
        report = "🛡️ <b>DAILY HEALING REPORT:</b>\n"
        found = False

        for entry in recent_entries:
            bad = await validate_links(entry.get("content", ""))
            if bad:
                report += f"❌ <code>{entry.get('m_code')}</code>: {bad}\n"
                found = True

        if found:
            await bot.send_message(OWNER_ID, report, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Self-healing audit error: {e}")

async def hourly_heartbeat():
    """
    Ensures the bot and database connection remain persistent.
    """
    try:
        # Test MongoDB connection
        col_vault.find_one({}, limit=1)

        # Pull model engine info if available
        curr_eng = MODEL_POOL[CURRENT_MODEL_INDEX] if 'MODEL_POOL' in globals() else "Active"

        await bot.send_message(
            LOG_CHANNEL_ID,
            f"💓 <b>HEARTBEAT:</b> Nominal | API: {API_USAGE_COUNT}/1500 | Engine: {curr_eng}",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await bot.send_message(
            OWNER_ID,
            f"🚨 <b>SYSTEM ERROR:</b> {html.escape(str(e))}",
            parse_mode=ParseMode.HTML
        )

# ==========================================
# 🗑 UNSEND PROTOCOL (DELETION)
# ==========================================

@dp.message(F.text == "🗑 UNSEND", StateFilter("*"))
@dp.message(Command("unsend"))
async def unsend_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer(
        "🗑 <b>UNSEND INITIATED</b>\nEnter the M-Code to scrub from existence:", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(UnsendState.waiting_id)

@dp.message(UnsendState.waiting_id)
async def unsend_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    m_code = message.text.upper()
    
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(VaultEntry).where(VaultEntry.m_code == m_code))
        entry = res.scalar_one_or_none()
        
        if entry:
            try:
                await bot.delete_message(CHANNEL_ID, entry.msg_id)
                telegram_status = "Scrubbed from Channel"
            except Exception:
                telegram_status = "Channel deletion failed (too old)"
            
            await session.execute(delete(VaultEntry).where(VaultEntry.m_code == m_code))
            await session.commit()
            
            await message.answer(
                f"✅ <b>OPERATION COMPLETE</b>\nID: <code>{m_code}</code>\nStatus: {telegram_status} and Database.", 
                parse_mode=ParseMode.HTML
            )
        else:
            await message.answer(
                f"❌ <b>ERROR:</b> M-Code <code>{m_code}</code> not found.", 
                parse_mode=ParseMode.HTML
            )
    await state.clear()
# ==========================================
# 🔘 COMMANDS & GUIDE
# ==========================================
@dp.message(Command("start"), StateFilter("*"))
async def start_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # Critical: Reset any stuck states from previous errors
    await state.clear()
# Reorganized Keyboard: Tactical Command v15.0
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")], 
        [KeyboardButton(text="✏️ EDIT"), KeyboardButton(text="🗑 UNSEND")],
        [KeyboardButton(text="📋 LIST"), KeyboardButton(text="🎯 ENGAGEMENT")],
        [KeyboardButton(text="📢 BROADCAST"), KeyboardButton(text="🔑 API")], # Added API Hot-Swap
        [KeyboardButton(text="⚙️ MODEL"), KeyboardButton(text="📊 AUDIT")], 
        [KeyboardButton(text="📟 TERMINAL"), KeyboardButton(text="❓ GUIDE")],
        [KeyboardButton(text="🛑 PANIC")]
    ], resize_keyboard=True)
    
    # Log the access to the internal terminal
    if 'console_out' in globals():
        console_out("Master Sadiq initialized Command Center")

    await message.answer(
        "💎 <b>APEX SINGULARITY v5.0</b>\n"
        "────────────────────\n"
        "Master Sadiq, the system is fully synchronized.\n"
        "All nodes active. Awaiting your directive.", 
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )

@dp.message(F.text == "❓ GUIDE" or Command("guide"))
async def help_guide(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    
    guide = (
        "💎 <b>APEX OVERLORD SINGULARITY: TECHNICAL MANUAL</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔥 <b>OFFENSIVE: BREACH PROTOCOL</b>\n"
        "• <code>/breach</code>: Generates high-value toolkit leaks.\n"
        "• <b>AUTO:</b> AI selects resource from the elite reward pool.\n"
        "• <b>MANUAL:</b> AI generates technical content based on your niche.\n"
        "• <b>REACTION LOCK:</b> Force engagement by locking bonus hacks.\n\n"
        
        "🗓 <b>LOGISTICS: SCHEDULE SUBSYSTEM</b>\n"
        "• <code>/schedule</code>: Set precision fire times (HH:MM AM/PM).\n"
        "• <b>GUARDED FIRE:</b> T-60 notification sends you the draft 60 minutes before fire.\n\n"
        
        "📢 <b>SYNDICATE: GLOBAL BROADCAST</b>\n"
        "• <code>/broadcast</code>: Send military-formatted alerts to the entire Family.\n\n"
        
        "🎯 <b>GATING: ENGAGEMENT CONTROL</b>\n"
        "• <code>/engagement</code>: Retroactively set or update reaction targets.\n\n"
        
        "⚙️ <b>INTELLIGENCE: MODEL MANAGEMENT</b>\n"
        "• <code>/model</code>: Monitor Gemini API usage (1,500 limit). Swap engines live.\n\n"
        
        "🛡 <b>DEFENSIVE: AUDIT & SELF-HEAL</b>\n"
        "• <code>/audit</code>: Deep scan of database and system health.\n"
        "• <b>HEARTBEAT:</b> Hourly status checks in the Log Channel.\n"
        "• <b>SELF-HEAL:</b> Dead URL audit at midnight.\n\n"
        
        "🧪 <b>TRANSMUTATION: ALCHEMY ENGINE</b>\n"
        "• <b>AUTO-TRANSMUTE:</b> Forward text to bot for MSANODE Protocol rewrite.\n\n"
        
        "📦 <b>UTILITY: VAULT COMMANDS</b>\n"
        "• <code>/list</code>: View ID-locked inventory.\n"
        "• <code>/backup</code>: Instant SQLite database export.\n"
        "• <code>/edit</code>: Remote text correction via M-Code.\n"
        "• <code>/unsend</code>: Permanent deletion of a leak.\n"
        "• <code>/hurry</code>: FOMO countdown injection.\n"
        "• <code>/panic</code>: Emergency kill-switch for all tasks.\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👑 <b>MASTER SADIQ DIRECTIVE:</b> Execute with precision."
    )
    await message.answer(guide, parse_mode=ParseMode.HTML)

@dp.message(F.text == "⚙️ MODEL" or Command("usage"))
async def model_info(message: types.Message):
    # Fixed: Referenced MODEL_POOL correctly
    curr_mod = MODEL_POOL[CURRENT_MODEL_INDEX] if 'MODEL_POOL' in globals() else "Synchronizing"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 SWAP ENGINE", callback_data="swap_engine")],
        [InlineKeyboardButton(text="📊 USAGE STATS", callback_data="api_usage")]
    ])
    await message.answer(f"⚙️ <b>ENGINE:</b> <code>{curr_mod}</code>\n💎 <b>USAGE:</b> {API_USAGE_COUNT}/1500", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "api_usage")
async def api_usage_cb(cb: types.CallbackQuery):
    await cb.answer(f"Consumed: {API_USAGE_COUNT} | Left: {1500 - API_USAGE_COUNT}", show_alert=True)

@dp.callback_query(F.data == "swap_engine")
async def swap_engine_cb(cb: types.CallbackQuery):
    kb_list = []
    for i, m in enumerate(MODEL_POOL):
        kb_list.append([InlineKeyboardButton(text=f"⚙️ {m}", callback_data=f"selmod_{i}")])
    
    # Add the "ADD MODE" button as requested
    kb_list.append([InlineKeyboardButton(text="➕ ADD CUSTOM MODE", callback_data="add_custom_mode")])
    kb_list.append([InlineKeyboardButton(text="🔙 BACK", callback_data="cancel_api")])
    
    await cb.message.edit_text("🎯 <b>ENGINE SELECTION PROTOCOL:</b>", 
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list), 
                               parse_mode=ParseMode.HTML)
@dp.callback_query(F.data.startswith("selmod_"))
async def sel_model_exec(cb: types.CallbackQuery):
    global CURRENT_MODEL_INDEX, model
    idx = int(cb.data.split("_")[1])
    CURRENT_MODEL_INDEX = idx
    # Hard-locked synchronization with Overlord persona
    model = genai.GenerativeModel(
        model_name=MODEL_POOL[CURRENT_MODEL_INDEX],
        system_instruction=get_system_prompt()
    )
    await cb.message.edit_text(f"✅ <b>ENGINE UPDATED:</b> <code>{MODEL_POOL[idx]}</code>", parse_mode=ParseMode.HTML)

@dp.message(F.text == "📦 BACKUP" or Command("backup"))
async def backup_mirror(message: types.Message):
    try:
        # Get all vault entries from MongoDB
        vault_entries = list(col_vault.find({}, {"_id": 0}))  # Exclude MongoDB _id field
        data = [{
            "m_code": entry.get("m_code"),
            "topic": entry.get("topic"),
            "content": entry.get("content"),
            "lock": entry.get("reaction_lock", 0)
        } for entry in vault_entries]

        json_file = io.BytesIO(json.dumps(data, indent=4).encode())
        await message.answer_document(
            BufferedInputFile(json_file.getvalue(), filename="vault_backup.json"),
            caption="🔒 <b>BACKUP MIRROR SECURED.</b>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await message.answer(f"❌ <b>BACKUP FAILED:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

# ==========================================
# 🗄️ DATABASE COLLECTIONS (MongoDB)
# ==========================================
# Collections are automatically created when first used
# No need for explicit schema definitions like SQLAlchemy
@dp.callback_query(F.data.startswith("del_x_"))
async def delete_record_surgical(cb: types.CallbackQuery):
    code = cb.data.split("_")[2]
    
    # Delete from Vault
    res = col_vault.delete_one({"m_code": code})
    # Also delete from Schedules if it's there
    col_schedules.delete_one({"m_code": code})
    
    if res.deleted_count > 0:
        await cb.answer(f"🗑️ Record {code} Purged.", show_alert=True)
        await broadcast_audit("SURGICAL_DELETE", code, "Entry wiped from database by Owner.")
        await cb.message.delete()
    else:
        await cb.answer("❌ Error: Record not found.")
# ==========================================
# 🔄 GLOBAL API COUNTER (MongoDB)
# ==========================================
async def get_api_usage():
    try:
        stats = col_system_stats.find_one({"_id": 1})
        if not stats:
            # Initialize if first time
            col_system_stats.insert_one({
                "_id": 1,
                "api_total": 0,
                "last_reset": datetime.now()
            })
            return 0
        return stats.get("api_total", 0)
    except Exception as e:
        logging.error(f"Error getting API usage: {e}")
        return 0
    
# ==========================================
# 🔥 BREACH (STABLE v6.0 - HARD LOCKED)
# ==========================================
@dp.message(F.text == "🔥 BREACH")
async def breach_start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 AUTO", callback_data="breach_auto"), 
         InlineKeyboardButton(text="📝 MANUAL", callback_data="breach_manual")]
    ])
    await message.answer("🧨 <b>BREACH INITIALIZED</b>\nSelect generation mode:", reply_markup=kb, parse_mode=ParseMode.HTML)
    await state.set_state("SELECTING_MODE")

@dp.callback_query(F.data.startswith("breach_"))
async def breach_mode_select(cb: types.CallbackQuery, state: FSMContext):
    mode = cb.data.split("_")[1]
    if mode == "manual":
        await cb.message.edit_text("🎯 <b>TARGET:</b> Enter your niche/topic:", parse_mode=ParseMode.HTML)
        await state.set_state("WAITING_TOPIC")
    else:
        await cb.message.edit_text("🔍 <b>SYNTHESIZING...</b>", parse_mode=ParseMode.HTML)
        content, topic = await generate_content()
        await state.update_data(content=content, topic=topic)
        await cb.message.answer("🔥 <b>REACTION LOCK:</b> Enter target count (0 to skip):", parse_mode=ParseMode.HTML)
        await state.set_state("WAITING_REACTION_COUNT")

@dp.message(StateFilter("WAITING_TOPIC"))
async def breach_manual_topic(message: types.Message, state: FSMContext):
    await message.answer("🔍 <b>SYNTHESIZING...</b>", parse_mode=ParseMode.HTML)
    content, topic = await generate_content(message.text)
    await state.update_data(content=content, topic=topic)
    await message.answer("🔥 <b>REACTION LOCK:</b> Enter target count (0 to skip):", parse_mode=ParseMode.HTML)
    await state.set_state("WAITING_REACTION_COUNT")

@dp.message(StateFilter("WAITING_REACTION_COUNT"))
async def breach_final_count(message: types.Message, state: FSMContext):
    raw_text = message.text.strip()
    if not raw_text.isdigit():
        await message.answer("⚠️ <b>Numbers only.</b>", parse_mode=ParseMode.HTML)
        return

    count = int(raw_text)
    await state.update_data(reaction_lock=count)
    data = await state.get_data()
    
    preview = (
        f"<b>📑 PREVIEW (Lock: {count}x🔥)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{data.get('content', 'No Content')}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 FIRE CONFIRMED", callback_data="fire_final")]
    ])
    
    await message.answer(preview, reply_markup=kb, parse_mode=ParseMode.HTML)
    await state.set_state("BREACH_PREVIEW_STATE")

# ==========================================
# 🔥 BREACH EXECUTION (MIRROR DEPTH v13.0)
# ==========================================

@dp.callback_query(F.data == "fire_final")
async def fire_final(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    
    # 1. Sequential Identity Generation
    m_code = await get_next_m_code() 
    
    # 2. Temporal Precision Capture
    now = datetime.now()
    fire_time = now.strftime("%I:%M:%S %p")
    fire_date = now.strftime("%d-%m-%Y")
    
    try:
        # 3. Public Deployment (Main Channel)
        vault_msg = await bot.send_message(
            CHANNEL_ID, 
            data['content'], 
            parse_mode=ParseMode.HTML, 
            reply_markup=get_engagement_markup(m_code, data['reaction_lock'])
        )
        
        # 4. Persistence to MongoDB Ledger
        col_vault.insert_one({
            "m_code": m_code,
            "msg_id": vault_msg.message_id,
            "topic": data['topic'],
            "content": data['content'],
            "reaction_lock": data['reaction_lock'],
            "is_unlocked": False,
            "created_at": now,
            "last_verified": now
        })
            
        # 5. FULL MIRROR TO PRIVATE LOG CHANNEL (The Fix)
        # This sends the EXACT Vault content followed by technical metadata
        log_payload = (
            f"{data['content']}\n\n"
            f"────────────────────\n"
            f"📊 <b>DEPLOYMENT METADATA</b>\n"
            f"CODE: <code>{m_code}</code>\n"
            f"TIME: <code>{fire_time}</code>\n"
            f"DATE: <code>{fire_date}</code>\n"
            f"GATING: <code>{data['reaction_lock']}x</code> Reactions\n"
            f"STATUS: <b>VERIFIED BREACH</b>\n"
            f"────────────────────"
        )
        
        await bot.send_message(
            LOG_CHANNEL_ID, 
            log_payload, 
            parse_mode=ParseMode.HTML
        )
        
        # 6. Command UI Update
        await cb.message.edit_text(
            f"🚀 <b>BREACH SUCCESSFUL</b>\n"
            f"Identity: <code>{m_code}</code>\n"
            f"Timestamp: <code>{fire_time}</code>\n"
            f"Mirrored to Command Center.", 
            parse_mode=ParseMode.HTML
        )
        
        console_out(f"Protocol {m_code} mirrored at {fire_time}")
        await state.clear()
        
    except Exception as e:
        error_info = html.escape(str(e))
        await cb.message.answer(f"❌ <b>MIRROR FAILURE:</b> <code>{error_info}</code>")
        console_out(f"Mirror Error: {error_info}")
# ==========================================
# ==========================================
# 📋 LIST / AUDIT (STATE INDEPENDENT)
# ==========================================
@dp.message(F.text == "📋 LIST", StateFilter("*"))
async def list_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    # This line clears any "stuck" state automatically
    await state.clear()
    try:
        # Get all vault entries from MongoDB, sorted by creation date
        entries = list(col_vault.find().sort("created_at", -1))
        # Stability Shield: HTML Formatting for clean terminal aesthetics
        rep = "<b>📋 INVENTORY</b>\n" + "\n".join([f"🆔 <code>{entry.get('m_code')}</code> | 🔥 {entry.get('reaction_lock', 0)}x" for entry in entries])
        await message.answer(rep if entries else "📭 Empty.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"❌ <b>LIST ERROR:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

@dp.message(F.text == "📊 AUDIT", StateFilter("*"))
async def audit_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    # Clear state so you can use the bot again immediately
    await state.clear()
    try:
        total = col_vault.count_documents({})
        # Pull real-time API usage from global counter
        await message.answer(f"📊 <b>AUDIT:</b> {total} entries. API Usage: {API_USAGE_COUNT}/1500.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"❌ <b>AUDIT ERROR:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

# ==========================================
# 🗓 SCHEDULE HELPERS (DEFINED FIRST)
# ==========================================

async def get_days_kb(selected):
    """Generates the dynamic days-selection keyboard with Ticks/Crosses."""
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    buttons = []; row = []
    for i, d in enumerate(days):
        # Tick for selected, Cross for unselected
        text = f"✅ {d}" if i in selected else f"❌ {d}"
        row.append(InlineKeyboardButton(text=text, callback_data=f"toggle_{i}"))
        if len(row) == 3: buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(text="📥 CONFIRM DAYS", callback_data="days_done")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def show_days_keyboard(message, selected):
    """Initializes the days menu."""
    kb = await get_days_kb(selected)
    await message.answer(
        "📅 <b>SELECT DEPLOYMENT DAYS</b>\n"
        "Toggle the days for recurring fire:", 
        reply_markup=kb, 
        parse_mode=ParseMode.HTML
    )

# ==========================================
# 🗓 SCHEDULE HANDLERS (PRIORITY ANCHORED)
# ==========================================

@dp.message(or_f(F.text.contains("SCHEDULE"), Command("schedule")), StateFilter("*"))
async def schedule_start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer("🕒 <b>Enter Fire Time (e.g., 04:08 PM):</b>", parse_mode=ParseMode.HTML)
    await state.set_state(ScheduleState.waiting_time)

@dp.message(ScheduleState.waiting_time)
async def sched_time(message: types.Message, state: FSMContext):
    try:
        t_str = message.text.upper().replace(".", "").strip()
        datetime.strptime(t_str, "%I:%M %p")
        await state.update_data(time=t_str)
        await message.answer(f"✅ <b>TIME SECURED:</b> <code>{t_str}</code>\n📅 <b>Enter Month (1-12):</b>", parse_mode=ParseMode.HTML)
        await state.set_state(ScheduleState.waiting_month)
    except:
        await message.answer("⚠️ <b>FORMAT ERROR:</b> Use HH:MM AM/PM (e.g., 10:55 PM)")

@dp.message(ScheduleState.waiting_month)
async def sched_month(message: types.Message, state: FSMContext):
    try:
        val = int(message.text)
        if not (1 <= val <= 12): raise ValueError
        await state.update_data(month=val)
        await message.answer("📅 <b>Enter Year (e.g., 2026):</b>", parse_mode=ParseMode.HTML)
        await state.set_state(ScheduleState.waiting_year)
    except:
        await message.answer("❌ <b>ERROR:</b> Enter a valid month number (1-12).")

@dp.message(ScheduleState.waiting_year)
async def sched_year(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ <b>ERROR:</b> Enter numerical year.")
        return
    await state.update_data(year=int(message.text), selected_days=[])
    await show_days_keyboard(message, [])
    await state.set_state(ScheduleState.selecting_days)

@dp.callback_query(F.data.startswith("toggle_"), ScheduleState.selecting_days)
async def toggle_day(cb: types.CallbackQuery, state: FSMContext):
    idx = int(cb.data.split("_")[1])
    data = await state.get_data()
    sel = data.get("selected_days", [])
    if idx in sel: sel.remove(idx)
    else: sel.append(idx)
    await state.update_data(selected_days=sel)
    await cb.message.edit_reply_markup(reply_markup=await get_days_kb(sel))

@dp.callback_query(F.data == "days_done", ScheduleState.selecting_days)
async def days_finished(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 LOCK PROTOCOL", callback_data="sched_lock")]])
    await cb.message.edit_text(
        f"📋 <b>SCHEDULE SUMMARY</b>\n"
        f"🕒 Time: <code>{data['time']}</code>\n"
        f"🗓 Days index: <code>{data['selected_days']}</code>", 
        reply_markup=kb, 
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data == "sched_lock")
async def sched_lock(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    
    # [!] CHANGE: Ensure this also uses sequential numbering
    m_code = await get_next_m_code()
    
    # ... rest of your cron/scheduling logic ...
    day_map = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    cron_days = ",".join([day_map[i] for i in data['selected_days']])
    
    dt_fire = datetime.strptime(data['time'], "%I:%M %p")
    now = datetime.now()
    fire_today = now.replace(hour=dt_fire.hour, minute=dt_fire.minute, second=0, microsecond=0)
    time_diff = (fire_today - now).total_seconds() / 60

    # 1. SETUP RECURRING JOBS
    review_hour = dt_fire.hour - 1 if dt_fire.hour > 0 else 23
    scheduler.add_job(trigger_review, CronTrigger(day_of_week=cron_days, hour=review_hour, minute=dt_fire.minute), args=[m_code, data['time']])
    scheduler.add_job(execute_guarded_fire, CronTrigger(day_of_week=cron_days, hour=dt_fire.hour, minute=dt_fire.minute), args=[m_code])

    # 2. HYBRID INTELLIGENT LOGIC
    today_short = now.strftime("%a").lower()
    if today_short in cron_days and 0 < time_diff <= 60:
        # PATH A: FIRE AT SCHEDULED TIME (NO PERMISSION NEEDED)
        # Store in PENDING with 'confirmed' already True
        content, topic = await generate_content()
        PENDING_APPROVALS[m_code] = {"content": content, "topic": topic, "confirmed": True, "target": data['time']}
        await cb.message.edit_text(f"⚡ <b>DIRECT FIRE ARMED:</b> Window under 60m. Bot will fire at <code>{data['time']}</code> automatically.", parse_mode=ParseMode.HTML)
    else:
        # PATH B: GUARDED (CONFIRMATION REQUIRED AT T-60)
        await cb.message.edit_text(f"💎 <b>PROTOCOL SECURED:</b> I will ask for confirmation 60m before <code>{data['time']}</code>.", parse_mode=ParseMode.HTML)
    
    await state.clear()

# ==========================================
# 🚀 BACKGROUND EXECUTION (INTELLIGENCE)
# ==========================================
# --- STEP 1: Add to your PENDING_APPROVALS logic ---
async def trigger_review(m_code, target_time):
    """Fires exactly 60m before target."""
    content, topic = await generate_content()
    
    # Check Integrity (Does AI content look okay?)
    integrity = "PASSED" if len(content) > 100 else "FAILED"
    
    PENDING_APPROVALS[m_code] = {
        "content": content, 
        "topic": topic, 
        "confirmed": False, 
        "target": target_time,
        "integrity": integrity
    }
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 CONFIRM FIRE", callback_data=f"arm_{m_code}")],
        [InlineKeyboardButton(text="🗑️ ABORT / DELETE", callback_data=f"del_x_{m_code}")]
    ])
    
    await bot.send_message(OWNER_ID, 
        f"⏳ <b>PRE-FLIGHT CHECK (T-60m): {m_code}</b>\n"
        f"Fire scheduled at: <code>{target_time}</code>\n"
        f"Integrity Check: <b>{integrity}</b>\n"
        f"────────────────────\n\n{content}", 
        reply_markup=kb, parse_mode=ParseMode.HTML)

# --- STEP 2: The T-0 Execution Logic ---
async def execute_guarded_fire(m_code):
    """The Precision Trigger at T-0."""
    if m_code in PENDING_APPROVALS:
        task = PENDING_APPROVALS[m_code]
        
        # AUTO-FIRE LOGIC: Fire if confirmed OR if integrity passed but I missed the button
        should_fire = task["confirmed"] or (task["integrity"] == "PASSED")
        
        if should_fire:
            # Deploy to public channel
            vault_msg = await bot.send_message(CHANNEL_ID, task['content'], 
                                               reply_markup=get_engagement_markup(m_code))
            
            # Sync to Database
            col_vault.insert_one({
                "m_code": m_code, "msg_id": vault_msg.message_id, "content": task['content'],
                "is_unlocked": False, "created_at": datetime.now(), "clicks": 0
            })
            
            # Audit Mirror
            await broadcast_audit("SCHEDULED_FIRE", m_code, "Auto-fired via Fail-Safe Protocol")
        else:
            await bot.send_message(OWNER_ID, f"❌ <b>ABORTED:</b> {m_code} failed integrity/confirmation.")
            await broadcast_audit("FIRE_ABORTED", m_code, "System Integrity Check Failed.")
            
        del PENDING_APPROVALS[m_code]
# ==========================================
# ✏️ REMOTE EDIT (REINFORCED PRIORITY)
# ==========================================

# Using or_f to catch "✏️ EDIT", "EDIT", or "/edit"
@dp.message(or_f(F.text.contains("EDIT"), Command("edit")), StateFilter("*"))
async def edit_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # Critical: Flush any stuck states from previous protocols
    await state.clear()
    
    await message.answer(
        "📝 <b>EDIT MODE ACTIVATED</b>\n"
        "Enter the <b>M-Code</b> of the post to modify (e.g., M1):", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(EditState.waiting_id)

@dp.message(EditState.waiting_id)
async def edit_id_received(message: types.Message, state: FSMContext):
    # Standardize input
    m_code = message.text.upper().strip()

    try:
        # Check database before proceeding
        entry = col_vault.find_one({"m_code": m_code})

        if entry:
            await state.update_data(edit_code=m_code, msg_id=entry.get("msg_id"))
            await message.answer(
                f"🔍 <b>ENTRY FOUND:</b> <code>{m_code}</code>\n"
                f"────────────────────\n"
                f"<b>CURRENT CONTENT:</b>\n"
                f"<code>{html.escape(entry.get('content', '')[:150])}...</code>\n"
                f"────────────────────\n"
                f"📥 <b>Enter the NEW content for this post:</b>",
                parse_mode=ParseMode.HTML
            )
            await state.set_state(EditState.waiting_text)
        else:
            await message.answer(f"❌ <b>ERROR:</b> M-Code <code>{m_code}</code> not found in Vault.")
            await state.clear()
    except Exception as e:
        await message.answer(f"❌ <b>DATABASE ERROR:</b> {html.escape(str(e))}")
        await state.clear()

@dp.message(EditState.waiting_text)
async def edit_exec(message: types.Message, state: FSMContext):
    data = await state.get_data()
    m_code = data['edit_code']
    msg_id = data['msg_id']
    new_content = message.text

    try:
        # 1. Update the physical message in the Telegram Channel
        # We preserve the original reaction lock buttons
        await bot.edit_message_text(
            text=new_content,
            chat_id=CHANNEL_ID,
            message_id=msg_id,
            parse_mode=ParseMode.HTML,
            reply_markup=get_engagement_markup(m_code)
        )

        # 2. Update MongoDB
        col_vault.update_one(
            {"m_code": m_code},
            {"$set": {"content": new_content}}
        )

        await message.answer(f"🚀 <b>SUCCESS:</b> Intelligence <code>{m_code}</code> updated in channel and database.")
        console_out(f"System Edit: {m_code} transmuted.")

    except Exception as e:
        # Error handling for messages older than 48 hours
        await message.answer(f"❌ <b>EDIT FAILED:</b> {html.escape(str(e))}")

    await state.clear()
# ==========================================
# [!] BROADCAST LOGIC (PRIORITY ANCHORED)
# ==========================================

@dp.message(or_f(F.text.contains("BROADCAST"), Command("broadcast")), StateFilter("*"))
async def broadcast_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # 1. Clear any stuck states
    await state.clear() 
    
    # 2. Set state immediately
    await state.set_state(BroadcastState.waiting_msg)
    
    await message.answer(
        "<b>[-] SYNDICATE BROADCAST</b>\n"
        "Enter your directive for the Family:", 
        parse_mode=ParseMode.HTML
    )

# CRITICAL: This handler MUST come before any general text handlers
# ==========================================
# 📢 SYNDICATE BROADCAST (TELEMETRY SYNCED)
# ==========================================
async def broadcast_audit(action: str, code: str, details: str = "N/A"):
    """Full-Spectrum Audit mirroring to Private Channel."""
    log_text = (
        f"🛡️ <b>EMPIRE AUDIT ENGINE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔧 <b>ACTION:</b> {action}\n"
        f"🆔 <b>UNIQUE CODE:</b> <code>{code}</code>\n"
        f"📝 <b>DETAILS:</b> {details}\n"
        f"👤 <b>OPERATOR:</b> Master Sadiq\n"
        f"📅 <b>TIMESTAMP:</b> {get_current_time()}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    # Send to the Private Log Channel defined in your config
    await bot.send_message(LOG_CHANNEL_ID, log_text, parse_mode=ParseMode.HTML)
    # Also log to internal console
    console_out(f"Audit: {action} | {code}")
@dp.message(BroadcastState.waiting_msg)
async def broadcast_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # 1. Check for Cancellation
    if message.text in ["🛑 PANIC", "/cancel"]:
        await state.clear()
        await message.answer("<b>[!] BROADCAST ABORTED.</b>", parse_mode=ParseMode.HTML)
        return

    # 2. Construct Technical Template
    # We wrap your input in the Syndicate styling
    formatted_payload = (
        "<b>◈ MSANODE SYNDICATE ◈</b>\n"
        "────────────────────\n\n"
        f"{html.escape(message.text)}\n\n"
        "────────────────────\n"
        "<b>DIRECTIVE FROM MASTER SADIQ</b>\n"
        "<i>\"Family: Execute with precision. Action is our only currency.\"</i>\n"
        "────────────────────"
    )
    
    # 3. Public Deployment
    try:
        # Use our safe_send_message protocol
        sent_msg = await safe_send_message(CHANNEL_ID, formatted_payload)
        
        if sent_msg:
            # Attempt to Pin the Directive
            try:
                await bot.pin_chat_message(CHANNEL_ID, sent_msg.message_id)
                pin_status = "SENT AND PINNED"
            except:
                pin_status = "SENT (PIN FAILED)"
            
            # 4. MIRROR TO PRIVATE LOG CHANNEL (Fixed & Unified)
            # This ensures your command center tracks the global broadcast
            await bot.send_message(
                LOG_CHANNEL_ID, 
                f"📢 <b>GLOBAL BROADCAST MIRROR</b>\n"
                f"Status: {pin_status}\n"
                f"────────────────────\n"
                f"{formatted_payload}",
                parse_mode=ParseMode.HTML
            )
            
            await message.answer(f"<b>[+] DIRECTIVE {pin_status}.</b>", parse_mode=ParseMode.HTML)
            console_out(f"Global Broadcast: {pin_status}")
            
        else:
            await message.answer("<b>[!] ERROR:</b> Public deployment failed.")
            
    except Exception as e:
        await message.answer(f"<b>[!] CRITICAL:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)
    
    # Finalize and return to Standby
    await state.clear()

# ==========================================
# [!] UNSEND PROTOCOL (SCRUB DELETION)
# ==========================================
@dp.message(F.text == "UNSEND", StateFilter("*"))
@dp.message(Command("unsend"), StateFilter("*"))
async def unsend_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer("[-] UNSEND INITIATED\nEnter the M-Code to scrub from existence:")
    await state.set_state(UnsendState.waiting_id)

@dp.message(UnsendState.waiting_id)
async def unsend_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    m_code = message.text.upper()

    try:
        entry = col_vault.find_one({"m_code": m_code})

        if entry:
            try:
                await bot.delete_message(CHANNEL_ID, entry.get("msg_id"))
                t_status = "Scrubbed from Channel"
            except Exception:
                t_status = "Telegram scrub failed (Message may be too old)"

            col_vault.delete_one({"m_code": m_code})

            await message.answer(f"<b>[+] SCRUB COMPLETE</b>\nID: <code>{m_code}</code>\nStatus: {t_status} and Database.", parse_mode=ParseMode.HTML)
        else:
            await message.answer(f"<b>[!] NOT FOUND:</b> <code>{m_code}</code> is not in the system.", parse_mode=ParseMode.HTML)

    except Exception as e:
        await message.answer(f"<b>[!] SCRUB ERROR:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

    await state.clear()

# ==========================================
# [!] ALCHEMY (UNTOUCHED foundation)
@dp.message(F.text & F.forward_from_chat)
async def alchemy_engine(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    await message.answer("[-] Alchemy: Transmuting intelligence...")
    content = await alchemy_transform(message.text)
    # Transmuted content delivered via HTML stability
    await bot.send_message(OWNER_ID, f"<b>[+] TRANSFORMED:</b>\n\n{content}", parse_mode=ParseMode.HTML)
# ==========================================
# 🚨 UNBLOCKABLE EMERGENCY OVERRIDE
# ==========================================
@dp.message(StateFilter("*"), lambda m: m.text and "PANIC" in m.text.upper())
@dp.message(Command("cancel"), StateFilter("*"))
async def global_panic_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return

    # 1. Force clear all stuck AI processes or states
    await state.clear()
    
    # 2. Re-create the menu manually (No function needed)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📄 Generate PDF"), KeyboardButton(text="🔗 Get Link")],
            [KeyboardButton(text="📋 Show Library"), KeyboardButton(text="📊 Storage Info")],
            [KeyboardButton(text="🗑 Remove PDF"), KeyboardButton(text="💎 Elite Help")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        "🚨 <b>SYSTEM-WIDE RESET SUCCESSFUL</b>\n"
        "────────────────────\n"
        "• State Memory: <b>PURGED</b>\n"
        "• AI Logic: <b>STANDBY</b>\n"
        "────────────────────\n"
        "Infrastructure restored to Home Protocol.",
        reply_markup=kb,
        parse_mode="HTML"
    )
    
    print(f"◈ ALERT: Panic Reset executed via sh.py")
# ==========================================
# [!] MAIN LOOP
# ==========================================
async def main():
    # MongoDB connection is already established in connect_db()

    # Scheduler Synchronization
    scheduler.add_job(hourly_heartbeat, 'interval', hours=1)
    scheduler.add_job(self_healing_audit, 'cron', hour=0, minute=0)
    scheduler.start()

    print("◈ SINGULARITY APEX ONLINE")

    try:
        await bot.send_message(OWNER_ID, "<b>[+] Singularity Online. Persistent & Failover Active.</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"Startup notification failed: {e}")

    # Polling Loop with Fail-Safe Sleep
    while True:
        try:
            await dp.start_polling(bot)
        except Exception as e:
            print(f"Polling error: {e}")
            await asyncio.sleep(5)
# --- RENDER PORT BINDER (SHIELD) ---
async def handle_health(request):
    return web.Response(text="CORE 5 (AI SINGULARITY) IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")
# ==========================================
# 🚀 THE SUPREME STARTUP (THE FIX)
# ==========================================
if __name__ == "__main__":
    print("🚀 STARTING INDIVIDUAL CORE TEST: BOT 5 (AI SINGULARITY)")
    
    # 1. Start the Health Server in a background thread
    # This stops the "No open ports detected" error immediately
    threading.Thread(target=run_health_server, daemon=True).start()
    
    # 2. Launch the AI Singularity main loop
    try:
        # Buffer to allow port binding to stabilize
        time.sleep(2) 
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("◈ Bot 5 Shutdown.")
    except Exception as e:
        # This will show us EXACTLY why the bot crashed in Render logs
        print(f"💥 CRITICAL STARTUP ERROR: {e}")
        import traceback
        traceback.print_exc()


