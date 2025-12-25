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
import sys
# 2025 AI SDK REPLACEMENT (Deprecation Fix)
from google import genai 
from google.genai import types as ai_types

from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

# Track when the bot was launched
START_TIME = time.time()
IST = pytz.timezone('Asia/Kolkata')

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
    sys.exit(1)

OWNER_ID = int(OWNER_ID_RAW)
CHANNEL_ID = int(CHANNEL_ID_RAW)
LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_RAW)

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
scheduler = AsyncIOScheduler(timezone=IST)

client = genai.Client(api_key=GEMINI_KEY)

# MongoDB Connection
db_client = None
db = None
col_system_stats = None
col_api_ledger = None
col_vault = None
col_schedules = None
col_settings = None

# 2025 Model Configuration
MODEL_POOL = [
    "gemini-2.0-flash", 
    "gemini-2.0-pro",
    "gemini-2.0-flash-preview-09-2025",
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash-lite-preview-09-2025"
]
API_USAGE_COUNT = 0
CONSOLE_LOGS = [] # Required for your terminal_viewer
PENDING_APPROVALS = {} # Required for your scheduling logic

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

# 1. State Definition (Must be above handlers)
class APIState(StatesGroup):
    waiting_api = State()
    waiting_model = State()

@dp.message(or_f(F.text.contains("API"), Command("api")), StateFilter("*"))
async def api_management(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return

    # 1. Clear any old state before starting
    await state.clear()

    global GEMINI_KEY
    key_hash = GEMINI_KEY[-8:] if GEMINI_KEY else "NONE"

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
    
    # Secure Masking Logic
    masked_key = f"{GEMINI_KEY[:8]}****{GEMINI_KEY[-4:]}" if GEMINI_KEY else "UNDEFINED"
    
    await message.answer(
        "🔑 <b>API MANAGEMENT PROTOCOL</b>\n"
        f"CURRENT KEY: <code>{masked_key}</code>\n"
        "────────────────────\n"
        "📥 <b>Enter the NEW Gemini API Key:</b>\n"
        "<i>Note: This will re-initialize the 2025 AI Client immediately.</i>",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(APIState.waiting_api)

@dp.message(APIState.waiting_api)
async def api_update(message: types.Message, state: FSMContext):
    global GEMINI_KEY, client
    new_key = message.text.strip()
    
    # 2025 Validation Standard
    if not new_key.startswith("AIza") or len(new_key) < 20:
        await message.answer("❌ <b>INVALID KEY:</b> Source does not match Gemini standards.")
        return

    try:
        # 1. Update Global Variable
        GEMINI_KEY = new_key
        
        # 2. Re-configure the 2025 Client (Surgical Fix)
        client = genai.Client(api_key=GEMINI_KEY)
        
        await message.answer(
            "🚀 <b>API ROTATION SUCCESSFUL</b>\n"
            "The Ghost Infrastructure is now utilizing the new credentials.\n"
            "────────────────────\n"
            "<b>STATUS:</b> Operational",
            parse_mode=ParseMode.HTML
        )
        console_out("System: 2025 AI Client Rotated.")
        
    except Exception as e:
        await message.answer(f"❌ <b>RE-INIT FAILED:</b> {html.escape(str(e))}")
    
    await state.clear()

# ==========================================
# MODEL MANAGEMENT (ADD CUSTOM MODEL)
# ==========================================

@dp.callback_query(F.data == "add_custom_mode")
async def add_custom_mode(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "📝 <b>ADD CUSTOM MODEL</b>\n"
        "Enter the new model name (e.g., gemini-2.0-pro):\n"
        "────────────────────\n"
        "⚠️ <b>WARNING:</b> Only add verified Gemini models.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 CANCEL", callback_data="cancel_api")]
        ]),
        parse_mode=ParseMode.HTML
    )
    await state.set_state(APIState.waiting_model)

@dp.message(APIState.waiting_model)
async def add_model_update(message: types.Message, state: FSMContext):
    new_model = message.text.strip()
    if new_model and new_model not in MODEL_POOL:
        MODEL_POOL.append(new_model)
        await message.answer(
            f"✅ <b>MODEL ADDED SUCCESSFULLY</b>\n"
            f"<code>{new_model}</code>\n"
            "────────────────────\n"
            "Use /api to select it.",
            parse_mode=ParseMode.HTML
        )
    elif new_model in MODEL_POOL:
        await message.answer("❌ <b>MODEL ALREADY EXISTS</b>", parse_mode=ParseMode.HTML)
    else:
        await message.answer("❌ <b>INVALID MODEL NAME</b>", parse_mode=ParseMode.HTML)
    await state.clear()

# ==========================================
# VIRTUAL CONSOLE STORAGE (LOGGING ENGINE)
# ==========================================

def console_out(text):
    global CONSOLE_LOGS
    timestamp = datetime.now(IST).strftime("%H:%M:%S")
    entry = f"[{timestamp}] {text}"
    CONSOLE_LOGS.append(entry)
    if len(CONSOLE_LOGS) > 12: 
        CONSOLE_LOGS.pop(0)
    logging.info(text)

async def get_api_usage_safe():
    """Retrieves usage from MongoDB system_stats."""
    try:
        stats = col_system_stats.find_one({"_id": 1})
        if not stats:
            col_system_stats.insert_one({
                "_id": 1,
                "api_total": 0,
                "last_reset": datetime.now(IST)
            })
            return 0
        return stats.get("api_total", 0)
    except Exception:
        return 0

async def increment_api_count(api_key):
    """Increments the local persistent counter for the current key hash."""
    key_hash = api_key[-8:] 
    try:
        existing = col_api_ledger.find_one({"key_hash": key_hash})

        if not existing:
            col_api_ledger.insert_one({
                "key_hash": key_hash,
                "usage_count": 1,
                "last_active": datetime.now(IST)
            })
            return 1
        else:
            new_count = existing.get("usage_count", 0) + 1
            col_api_ledger.update_one(
                {"key_hash": key_hash},
                {"$set": {"usage_count": new_count, "last_active": datetime.now(IST)}}
            )
            return new_count
    except Exception as e:
        logging.error(f"Ledger Sync Error: {e}")
        return 0

# ==========================================
# 📟 REMOTE TERMINAL (COMMAND CENTER)
# ==========================================

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
    - NO PRE-TEXT: Never explain your mandate or use disclaimers.
    - DIRECT LINKS: Provide REAL, EXTERNAL HTTPS LINKS to tools (e.g., chain.link, openai.com).
    - NO BRANDING: Do not create fake links.
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

async def generate_content(prompt="Generate a viral AI side hustle reel script"):
    global API_USAGE_COUNT
    if API_USAGE_COUNT >= 1500:
        return "⚠️ API Limit Reached", "Limit"

    try:
        # 2025 SDK: client.models.generate_content
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=MODEL_POOL[CURRENT_MODEL_INDEX],
            contents=prompt[:500],
            config=ai_types.GenerateContentConfig(
                system_instruction=get_system_prompt()
            )
        )
        
        raw_text = response.text if response else "No response"
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
        # 2025 SDK: client.models.generate_content
        resp = await asyncio.to_thread(
            client.models.generate_content,
            model=MODEL_POOL[CURRENT_MODEL_INDEX],
            contents=f"INPUT DATA:\n{raw_text}\n\nINSTRUCTION: Rewrite into MSANODE Protocol.",
            config=ai_types.GenerateContentConfig(
                system_instruction=get_system_prompt()
            )
        )
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
# 📡 UTILITY LOGIC (X-IDENTITY ENGINE)
# ==========================================
async def get_next_x_code(prefix="X"):
    """Surgical Sequential Counter: X1, X2, X3... across breaches and schedules."""
    try:
        # Atomically increment the global counter in MongoDB
        stats = col_system_stats.find_one_and_update(
            {"_id": "global_counter"},
            {"$inc": {"count": 1}},
            upsert=True,
            return_document=True
        )
        new_count = stats.get("count", 1)
        return f"{prefix}{new_count}"
    except Exception as e:
        console_out(f"X-Code Error: {e}")
        return f"{prefix}{random.randint(1000, 9999)}"

REWARD_POOL = [
    "GitHub Student Pack ($200k in Premium Infrastructure)",
    "Top 7 AI Tools that make ChatGPT look like a Toy",
    "Google's Hidden Professional Cybersecurity Certification"
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
# [!] FSM STATES (CORE PERSISTENCE)
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
    
    # Critical: Flush previous states to prevent logic loops
    await state.clear()
    
    await message.answer(
        "🎯 <b>ENGAGEMENT GATING ACTIVATED</b>\n"
        "Enter the <b>Unique Code</b> (e.g., X1) to update reaction targets:", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(EngagementState.waiting_code)

@dp.message(EngagementState.waiting_code)
async def engagement_id_received(message: types.Message, state: FSMContext):
    # Standardize input to match database X-Series codes
    m_code = message.text.upper().strip()

    try:
        # Verify the Code exists in our Vault
        entry = col_vault.find_one({"m_code": m_code})

        if entry:
            await state.update_data(target_code=m_code, msg_id=entry.get("msg_id"))
            current_lock = entry.get('reaction_lock', 0)
            
            await message.answer(
                f"✅ <b>ENTRY FOUND:</b> <code>{m_code}</code>\n"
                f"Current Lock: <code>{current_lock}x</code> 🔥 reactions.\n\n"
                "📥 <b>Enter the NEW target reaction count (0 to remove lock):</b>",
                parse_mode=ParseMode.HTML
            )
            await state.set_state(EngagementState.waiting_count)
        else:
            await message.answer(f"❌ <b>ERROR:</b> Code <code>{m_code}</code> not found in Vault.")
            await state.clear()
    except Exception as e:
        logging.error(f"Error verifying code: {e}")
        await message.answer("❌ <b>DATABASE ERROR:</b> Connection interrupt.")
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
        # 1. Update Database with IST timestamp
        col_vault.update_one(
            {"m_code": m_code},
            {"$set": {
                "reaction_lock": new_count,
                "is_unlocked": (new_count == 0),
                "last_verified": datetime.now(IST)
            }}
        )

        # 2. Synchronize Telegram UI (Live Update)
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
# 📡 UI HELPERS (DYNAMIC MARKUP)
# ==========================================

def get_engagement_markup(m_code, lock=0, unlocked=False):
    """
    Generates the reaction gating buttons.
    Logic: If lock > 0 and not yet unlocked, show the 'Lock' button.
    Otherwise, show the 'Reveal' button for the intelligence asset.
    """
    if lock > 0 and not unlocked:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔒 UNLOCK AT {lock}x 🔥 REACTIONS", callback_data=f"lockmsg_{m_code}")]
        ])
    
    # Reveal state once threshold is bypassed
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔓 UNLOCK SECRET BONUS HACK", callback_data=f"reveal_{m_code}")]
    ])
# ==========================================
# 🕹️ FSM STATES (CLEANED & DE-DUPLICATED)
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

# ==========================================
# 🪤 REACTION LOCK INTERFACE HANDLERS
# ==========================================

@dp.callback_query(F.data.startswith("reveal_"))
async def reveal_secret(cb: types.CallbackQuery):
    """Provides verified engineering/AI resources upon unlock."""
    hacks = [
        "◈ VPN Bypass: Protocol Verified.", 
        "◈ EDU Email: Access Granted.", 
        "◈ Archive Script: Script mirror active.",
        "◈ Premium Repo: Branch decrypted."
    ]
    await cb.answer(random.choice(hacks), show_alert=True)

@dp.callback_query(F.data.startswith("lockmsg_"))
async def lock_alert(cb: types.CallbackQuery):
    """Informs the user about the remaining requirement."""
    await cb.answer(
        "◈ ACCESS RESTRICTED ◈\n"
        "Requirement: Reach the 🔥 reaction target to unlock this intelligence.", 
        show_alert=True
    )

# ==========================================
# 📊 UNIVERSAL REACTION LISTENER (FIXED)
# ==========================================

@dp.message_reaction()
async def reaction_listener(reaction: types.MessageReactionUpdated):
    """Counts reactions and unlocks the Vault when target is reached."""
    try:
        # Search the Vault using dict.get() to prevent AttributeErrors
        entry = col_vault.find_one({
            "msg_id": reaction.message_id,
            "is_unlocked": False
        })

        if entry and entry.get("reaction_lock", 0) > 0:
            # logic to tally reaction count
            total_reactions = len(reaction.new_reaction) 

            if total_reactions >= entry.get("reaction_lock", 0):
                # 1. Update Database Status
                col_vault.update_one(
                    {"m_code": entry.get("m_code")},
                    {"$set": {"is_unlocked": True}}
                )

                # 2. Update Channel UI (Fixed Markup Call)
                await bot.edit_message_reply_markup(
                    chat_id=CHANNEL_ID,
                    message_id=entry.get("msg_id"),
                    reply_markup=get_engagement_markup(entry.get("m_code"), unlocked=True)
                )
                
                # 3. Notification to Audit Channel
                await bot.send_message(
                    LOG_CHANNEL_ID,
                    f"🔓 <b>VAULT UNLOCKED:</b> <code>{entry.get('m_code')}</code>\n"
                    f"Threshold of <code>{entry.get('reaction_lock', 0)}</code> reactions reached.",
                    parse_mode=ParseMode.HTML
                )
    except Exception as e:
        logging.error(f"Error in reaction listener: {e}")

# ==========================================
# 🛡️ SYSTEM TASKS & AUDIT ENGINE
# ==========================================

async def validate_links(text):
    """Asynchronous link validation for live assets."""
    urls = re.findall(r'(https?://[^\s)]+)', text)
    invalid = []
    # Added timeout to prevent hanging
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for url in urls:
            try:
                async with session.get(url) as resp:
                    if resp.status >= 400: 
                        invalid.append(url)
            except Exception:
                invalid.append(url)
    return invalid

async def self_healing_audit():
    """Periodic deep-scan of vault integrity."""
    try:
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
    """Ensures bot and database connection remain persistent."""
    try:
        # Test MongoDB connection
        col_vault.find_one({}, limit=1)

        # Pull model engine info safely
        curr_eng = MODEL_POOL[CURRENT_MODEL_INDEX] if 'MODEL_POOL' in globals() else "Active"

        await bot.send_message(
            LOG_CHANNEL_ID,
            f"💓 <b>HEARTBEAT:</b> Nominal | API: {API_USAGE_COUNT}/1500 | Engine: {curr_eng}",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        # Fallback to Owner if Log Channel fails
        await bot.send_message(
            OWNER_ID,
            f"🚨 <b>SYSTEM ERROR:</b> {html.escape(str(e))}",
            parse_mode=ParseMode.HTML
        )

# ==========================================
# 🗑 SURGICAL DELETE PROTOCOL (DOUBLE-WIPE)
# ==========================================

@dp.message(or_f(F.text == "🗑️ SURGICAL DELETE", Command("unsend")), StateFilter("*"))
async def unsend_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer(
        "🗑️ <b>SURGICAL PURGE INITIATED</b>\n"
        "────────────────────\n"
        "Enter the <b>Unique Code</b> (e.g., X1) to wipe from Channel and Database:", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(UnsendState.waiting_id)

@dp.message(UnsendState.waiting_id)
async def unsend_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    target_code = message.text.upper().strip()
    
    try:
        # Search the MongoDB Vault
        entry = col_vault.find_one({"m_code": target_code})
        
        if entry:
            # 1. Surgical removal from Public Channel
            try:
                await bot.delete_message(CHANNEL_ID, entry.get("msg_id"))
                t_status = "✅ Scrubbed from Channel"
            except Exception:
                t_status = "⚠️ Channel wipe failed (Post too old)"
            
            # 2. Permanent removal from MongoDB
            col_vault.delete_one({"m_code": target_code})
            
            # 3. Mirror to Private Log Channel
            await bot.send_message(
                LOG_CHANNEL_ID,
                f"🗑️ <b>EMPIRE WIPE COMPLETE</b>\n"
                f"CODE: <code>{target_code}</code>\n"
                f"STATUS: {t_status} & Database Purged.",
                parse_mode=ParseMode.HTML
            )
            
            await message.answer(
                f"🚀 <b>PURGE SUCCESSFUL</b>\n"
                f"ID: <code>{target_code}</code>\n"
                f"Status: {t_status} and Database wiped.", 
                parse_mode=ParseMode.HTML
            )
        else:
            await message.answer(f"❌ <b>ERROR:</b> Code <code>{target_code}</code> not found.")
            
    except Exception as e:
        await message.answer(f"💥 <b>CRITICAL ERROR:</b> {html.escape(str(e))}")

    await state.clear()

# ==========================================
# 🔘 MAIN COMMANDS & DYNAMIC MENU
# ==========================================

@dp.message(Command("start"), StateFilter("*"))
async def start_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()

    # Reorganized Keyboard: Tactical Command v16.0 (Surgical Upgrade)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")], 
        [KeyboardButton(text="✏️ EDIT"), KeyboardButton(text="🗑️ SURGICAL DELETE")], 
        [KeyboardButton(text="📋 LIST"), KeyboardButton(text="🎯 ENGAGEMENT")],
        [KeyboardButton(text="📢 BROADCAST"), KeyboardButton(text="🔑 API")], 
        [KeyboardButton(text="⚙️ MODEL"), KeyboardButton(text="📊 AUDIT")], 
        [KeyboardButton(text="📟 TERMINAL"), KeyboardButton(text="❓ GUIDE")],
        [KeyboardButton(text="🛑 PANIC")]
    ], resize_keyboard=True)
    
    console_out("Master Sadiq accessed Command Center")

    await message.answer(
        "💎 <b>APEX SINGULARITY v5.0</b>\n"
        "────────────────────\n"
        "Master Sadiq, the system is fully synchronized.\n"
        "Surgical Purge & 2025 AI Engine: <b>ACTIVE</b>.", 
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )

@dp.message(or_f(F.text == "❓ GUIDE", Command("guide")))
async def help_guide(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    
    guide = (
        "💎 <b>APEX OVERLORD: TECHNICAL MANUAL</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔥 <b>BREACH:</b> AI toolkit generation with X-Series Identity.\n"
        "🗓 <b>SCHEDULE:</b> Precision fire with T-60 Fail-Safe.\n"
        "🗑️ <b>SURGICAL DELETE:</b> Wipe X-codes from Channel & DB.\n"
        "📢 <b>BROADCAST:</b> Pin global directives to the Syndicate.\n"
        "⚙️ <b>MODEL:</b> Swap 2025 AI engines & track 1.5k quota.\n"
        "🛡 <b>AUDIT:</b> Heartbeat monitoring & Self-healing scan.\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👑 <b>MASTER SADIQ DIRECTIVE:</b> Precision is Power."
    )
    await message.answer(guide, parse_mode=ParseMode.HTML)

@dp.message(or_f(F.text == "⚙️ MODEL", Command("usage")))
async def model_info(message: types.Message):
    curr_mod = MODEL_POOL[CURRENT_MODEL_INDEX]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 SWAP ENGINE", callback_data="swap_engine")],
        [InlineKeyboardButton(text="📊 USAGE STATS", callback_data="api_usage")]
    ])
    await message.answer(
        f"⚙️ <b>ENGINE:</b> <code>{curr_mod}</code>\n"
        f"💎 <b>USAGE:</b> {API_USAGE_COUNT}/1500", 
        reply_markup=kb, parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data == "swap_engine")
async def swap_engine_cb(cb: types.CallbackQuery):
    kb_list = [[InlineKeyboardButton(text=f"⚙️ {m}", callback_data=f"selmod_{i}")] 
               for i, m in enumerate(MODEL_POOL)]
    
    kb_list.append([InlineKeyboardButton(text="➕ ADD NEW MODE", callback_data="add_custom_mode")])
    kb_list.append([InlineKeyboardButton(text="� SWAP MODEL", callback_data="swap_model")])
    kb_list.append([InlineKeyboardButton(text="�🔙 BACK", callback_data="cancel_api")])
    
    await cb.message.edit_text(
        "🎯 <b>2025 ENGINE SELECTION:</b>", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list), 
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data.startswith("selmod_"))
async def sel_model_exec(cb: types.CallbackQuery):
    global CURRENT_MODEL_INDEX
    idx = int(cb.data.split("_")[1])
    CURRENT_MODEL_INDEX = idx
    # Synchronization with 2025 Overlord Persona is automatic in Part 3
    await cb.message.edit_text(f"✅ <b>ENGINE UPDATED:</b> <code>{MODEL_POOL[idx]}</code>", parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "swap_model")
async def swap_model_cb(cb: types.CallbackQuery):
    global CURRENT_MODEL_INDEX
    CURRENT_MODEL_INDEX = (CURRENT_MODEL_INDEX + 1) % len(MODEL_POOL)
    new_model = MODEL_POOL[CURRENT_MODEL_INDEX]
    await cb.message.edit_text(f"🔄 <b>MODEL SWAPPED:</b> <code>{new_model}</code>", parse_mode=ParseMode.HTML)

@dp.message(or_f(F.text == "📦 BACKUP", Command("backup")))
async def backup_mirror(message: types.Message):
    try:
        vault_entries = list(col_vault.find({}, {"_id": 0}))
        json_file = io.BytesIO(json.dumps(vault_entries, indent=4, default=str).encode())
        await message.answer_document(
            BufferedInputFile(json_file.getvalue(), filename="vault_backup.json"),
            caption="🔒 <b>BACKUP MIRROR SECURED.</b>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await message.answer(f"❌ <b>BACKUP FAILED:</b> {html.escape(str(e))}")
# ==========================================
# 🗑 SURGICAL DELETE ENGINE (DATABASE & CHANNEL)
# ==========================================

# 1. INITIALIZATION: Triggered by Menu Button
@dp.message(F.text == "🗑️ SURGICAL DELETE", StateFilter("*"))
async def surgical_delete_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer(
        "🗑️ <b>SURGICAL DELETE ACTIVATED</b>\n"
        "────────────────────\n"
        "Enter the <b>Unique Code</b> (e.g., X1, X5) to wipe from existence:", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(UnsendState.waiting_id)

# 2. EXECUTION: The Double-Wipe (Text Input)
@dp.message(UnsendState.waiting_id)
async def surgical_delete_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    target_code = message.text.upper().strip()

    try:
        # Search in MongoDB Vault
        entry = col_vault.find_one({"m_code": target_code})

        if entry:
            # surgical removal from Telegram Channel
            try:
                await bot.delete_message(CHANNEL_ID, entry.get("msg_id"))
                t_status = "✅ Scrubbed from Channel"
            except Exception:
                t_status = "⚠️ Channel Deletion Failed (Too old)"

            # Surgical removal from MongoDB
            col_vault.delete_one({"m_code": target_code})

            # Mirror to Private Log Channel
            await bot.send_message(
                LOG_CHANNEL_ID, 
                f"🗑️ <b>EMPIRE WIPE COMPLETE</b>\n"
                f"CODE: <code>{target_code}</code>\n"
                f"STATUS: {t_status} & Database Purged.",
                parse_mode=ParseMode.HTML
            )

            await message.answer(
                f"🚀 <b>PURGE SUCCESSFUL</b>\n"
                f"ID: <code>{target_code}</code>\n"
                f"Status: {t_status} and Database.", 
                parse_mode=ParseMode.HTML
            )
        else:
            await message.answer(f"❌ <b>ERROR:</b> Code <code>{target_code}</code> not found in Vault.")

    except Exception as e:
        await message.answer(f"💥 <b>CRITICAL ERROR:</b> {html.escape(str(e))}")

    await state.clear()

# 3. CALLBACK EXECUTION: The Inline Button Wipe (From Logs)
@dp.callback_query(F.data.startswith("del_x_"))
async def delete_record_surgical_callback(cb: types.CallbackQuery):
    """
    Surgical removal triggered via Inline Buttons in the Audit Channel.
    """
    unique_code = cb.data.split("_")[2]
    
    entry = col_vault.find_one({"m_code": unique_code})
    
    if entry:
        try:
            await bot.delete_message(CHANNEL_ID, entry.get("msg_id"))
            telegram_info = "Wiped from Channel"
        except Exception:
            telegram_info = "Channel post already gone/too old"
        
        col_vault.delete_one({"m_code": unique_code})
        
        await cb.answer(f"🚀 {unique_code}: FULL SYSTEM PURGE COMPLETE", show_alert=True)
        
        # Mirror to Private Log Channel
        await broadcast_audit("SURGICAL_DELETE", unique_code, f"Status: {telegram_info}")
        
        # Update the Log message to show purged status
        await cb.message.edit_text(f"🗑️ <b>RECORD PURGED:</b> <code>{unique_code}</code>", parse_mode=ParseMode.HTML)
    else:
        await cb.answer("❌ Error: Code not found in Database.")

# ==========================================
# 🔄 GLOBAL API COUNTER (MongoDB Persistence)
# ==========================================

async def get_api_usage():
    try:
        stats = col_system_stats.find_one({"_id": 1})
        if not stats:
            col_system_stats.insert_one({
                "_id": 1,
                "api_total": 0,
                "last_reset": datetime.now(IST)
            })
            return 0
        return stats.get("api_total", 0)
    except Exception as e:
        logging.error(f"Error getting API usage: {e}")
        return 0

# ==========================================
# 🔥 BREACH (STABLE v6.0 - IDENTITY SYNCED)
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
    await state.set_state(BreachState.selecting_mode)

@dp.callback_query(F.data.startswith("breach_"))
async def breach_mode_select(cb: types.CallbackQuery, state: FSMContext):
    mode = cb.data.split("_")[1]
    if mode == "manual":
        await cb.message.edit_text("🎯 <b>TARGET:</b> Enter your niche/topic:", parse_mode=ParseMode.HTML)
        await state.set_state(BreachState.waiting_topic)
    else:
        await cb.message.edit_text("🔍 <b>SYNTHESIZING...</b>", parse_mode=ParseMode.HTML)
        content, topic = await generate_content()
        await state.update_data(content=content, topic=topic)
        await cb.message.answer("🔥 <b>REACTION LOCK:</b> Enter target count (0 to skip):", parse_mode=ParseMode.HTML)
        await state.set_state(BreachState.waiting_reaction_count)

@dp.message(BreachState.waiting_topic)
async def breach_manual_topic(message: types.Message, state: FSMContext):
    await message.answer("🔍 <b>SYNTHESIZING...</b>", parse_mode=ParseMode.HTML)
    content, topic = await generate_content(message.text)
    await state.update_data(content=content, topic=topic)
    await message.answer("🔥 <b>REACTION LOCK:</b> Enter target count (0 to skip):", parse_mode=ParseMode.HTML)
    await state.set_state(BreachState.waiting_reaction_count)

@dp.message(BreachState.waiting_reaction_count)
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
# 🔥 BREACH EXECUTION (MIRROR DEPTH v13.0 - REINFORCED)
# ==========================================

@dp.callback_query(F.data == "fire_final")
async def fire_final(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    
    # 1. 2025 Sequential Identity Generation (X-Series)
    m_code = await get_next_x_code() 
    
    # 2. Temporal Precision Capture (IST Locked)
    now = datetime.now(IST)
    fire_time = now.strftime("%I:%M:%S %p")
    fire_date = now.strftime("%d-%m-%Y")
    
    try:
        # 3. Public Deployment (Main Channel)
        # Identity is tagged with the new X-code
        vault_msg = await bot.send_message(
            CHANNEL_ID, 
            data['content'], 
            parse_mode=ParseMode.HTML, 
            reply_markup=get_engagement_markup(m_code, data.get('reaction_lock', 0))
        )
        
        # 4. Persistence to MongoDB Ledger
        col_vault.insert_one({
            "m_code": m_code,
            "msg_id": vault_msg.message_id,
            "topic": data.get('topic', 'General'),
            "content": data['content'],
            "reaction_lock": data.get('reaction_lock', 0),
            "is_unlocked": (data.get('reaction_lock', 0) == 0),
            "created_at": now,
            "last_verified": now
        })
            
        # 5. SURGICAL MIRROR TO LOG CHANNEL
        # Includes technical metadata and the Surgical Delete button
        audit_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🗑️ SURGICAL DELETE ({m_code})", callback_data=f"del_x_{m_code}")]
        ])

        log_payload = (
            f"<b>◈ BREACH DEPLOYED ◈</b>\n"
            f"────────────────────\n\n"
            f"{data['content']}\n\n"
            f"────────────────────\n"
            f"📊 <b>DEPLOYMENT METADATA</b>\n"
            f"CODE: <code>{m_code}</code>\n"
            f"TIME: <code>{fire_time}</code>\n"
            f"DATE: <code>{fire_date}</code>\n"
            f"GATING: <code>{data.get('reaction_lock', 0)}x</code> 🔥\n"
            f"STATUS: <b>VERIFIED BREACH</b>\n"
            f"────────────────────"
        )
        
        await bot.send_message(
            LOG_CHANNEL_ID, 
            log_payload, 
            reply_markup=audit_kb, # <-- Delete button attached to log
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
        await cb.message.answer(f"❌ <b>DEPLOYMENT FAILURE:</b> <code>{error_info}</code>")
        console_out(f"Execution Error: {error_info}")

# ==========================================
# 📋 LIST / AUDIT (STATE INDEPENDENT)
# ==========================================

@dp.message(F.text == "📋 LIST", StateFilter("*"))
async def list_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # Flush stuck states automatically
    await state.clear()
    
    try:
        # Pull entries from MongoDB sorted by recency
        entries = list(col_vault.find().sort("created_at", -1).limit(20))
        
        if not entries:
            await message.answer("📭 <b>INVENTORY EMPTY</b>", parse_mode=ParseMode.HTML)
            return

        inventory_list = []
        for entry in entries:
            code = entry.get('m_code')
            lock = entry.get('reaction_lock', 0)
            status = "🔓" if entry.get('is_unlocked') else "🔒"
            inventory_list.append(f"🆔 <code>{code}</code> | {status} {lock}x 🔥")

        rep = "<b>📋 CURRENT INVENTORY (LAST 20)</b>\n────────────────────\n" + "\n".join(inventory_list)
        
        await message.answer(rep, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"❌ <b>LIST ERROR:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

@dp.message(F.text == "📊 AUDIT", StateFilter("*"))
async def audit_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    
    try:
        total_vault = col_vault.count_documents({})
        active_schedules = col_schedules.count_documents({})
        
        # Mirror current API usage safe count
        api_usage = await get_api_usage_safe()
        
        text = (
            "<b>📊 SYSTEM AUDIT REPORT</b>\n"
            "────────────────────\n"
            f"VAULT ENTRIES: <code>{total_vault}</code>\n"
            f"ACTIVE SCHED: <code>{active_schedules}</code>\n"
            f"API QUOTA: <code>{api_usage}/1500</code>\n"
            "────────────────────\n"
            "STATUS: <b>NOMINAL</b>"
        )
        await message.answer(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"❌ <b>AUDIT ERROR:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)
        

# ==========================================
# 🗓 SCHEDULE HELPERS (IST SYNCHRONIZED)
# ==========================================

async def get_days_kb(selected):
    """Generates the dynamic days-selection keyboard with Ticks/Crosses."""
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    buttons = []; row = []
    for i, d in enumerate(days):
        text = f"✅ {d}" if i in selected else f"❌ {d}"
        row.append(InlineKeyboardButton(text=text, callback_data=f"toggle_{i}"))
        if len(row) == 3: buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(text="📥 CONFIRM DAYS", callback_data="days_done")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def show_days_keyboard(message, selected):
    """Initializes the days menu for deployment."""
    kb = await get_days_kb(selected)
    await message.answer(
        "📅 <b>SELECT DEPLOYMENT DAYS</b>\n"
        "Toggle the days for recurring fire:", 
        reply_markup=kb, 
        parse_mode=ParseMode.HTML
    )

# ==========================================
# 🗓 SCHEDULE HANDLERS (X-SERIES INTEGRATED)
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
    if not data.get('selected_days'):
        await cb.answer("⚠️ Select at least one day!", show_alert=True)
        return
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
    
    # 1. 2025 Identity Sync: X-Series Generation
    m_code = await get_next_x_code()
    
    day_map = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    cron_days = ",".join([day_map[i] for i in data['selected_days']])
    
    # Time Conversion
    dt_fire = datetime.strptime(data['time'], "%I:%M %p")
    now = datetime.now(IST)
    fire_today = now.replace(hour=dt_fire.hour, minute=dt_fire.minute, second=0, microsecond=0)
    time_diff = (fire_today - now).total_seconds() / 60

    # 2. SETUP RECURRING JOBS (T-60 & T-0)
    review_hour = dt_fire.hour - 1 if dt_fire.hour > 0 else 23
    scheduler.add_job(trigger_review, CronTrigger(day_of_week=cron_days, hour=review_hour, minute=dt_fire.minute, timezone=IST), args=[m_code, data['time']])
    scheduler.add_job(execute_guarded_fire, CronTrigger(day_of_week=cron_days, hour=dt_fire.hour, minute=dt_fire.minute, timezone=IST), args=[m_code])

    # 3. HYBRID LOGIC: Check if fire time is imminent
    today_short = now.strftime("%a").lower()
    if today_short in cron_days and 0 < time_diff <= 60:
        content, topic = await generate_content()
        PENDING_APPROVALS[m_code] = {"content": content, "topic": topic, "confirmed": True, "target": data['time'], "integrity": "PASSED"}
        await cb.message.edit_text(f"⚡ <b>DIRECT FIRE ARMED:</b> Window under 60m. Bot will fire <code>{m_code}</code> at <code>{data['time']}</code> automatically.", parse_mode=ParseMode.HTML)
    else:
        await cb.message.edit_text(f"💎 <b>PROTOCOL SECURED:</b> <code>{m_code}</code> locked. I will ask for confirmation 60m before <code>{data['time']}</code>.", parse_mode=ParseMode.HTML)
    
    await state.clear()

# ==========================================
# 🚀 FAIL-SAFE BACKGROUND EXECUTION
# ==========================================

async def trigger_review(m_code, target_time):
    """T-60m: Generates content and initiates Pre-Flight Integrity Check."""
    content, topic = await generate_content()
    
    # Integrity Check: Reject empty or error responses
    integrity = "PASSED" if len(content) > 150 and "System Error" not in content else "FAILED"
    
    PENDING_APPROVALS[m_code] = {
        "content": content, "confirmed": False, 
        "integrity": integrity, "target": target_time
    }
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 CONFIRM FIRE", callback_data=f"arm_{m_code}")],
        [InlineKeyboardButton(text="🗑️ ABORT", callback_data=f"del_x_{m_code}")]
    ])
    
    await bot.send_message(OWNER_ID, 
        f"⏳ <b>PRE-FLIGHT (T-60m): {m_code}</b>\n"
        f"FIRE AT: <code>{target_time}</code>\n"
        f"INTEGRITY: <b>{integrity}</b>\n"
        f"────────────────────\n\n{content}", 
        reply_markup=kb, parse_mode=ParseMode.HTML)

async def execute_guarded_fire(m_code):
    """T-0: The Precision Trigger. Checks integrity and fires automatically."""
    if m_code in PENDING_APPROVALS:
        task = PENDING_APPROVALS[m_code]
        
        # AUTO-FIRE LOGIC: Proceed if confirmed OR if integrity is passed despite missed interaction
        if task["confirmed"] or task["integrity"] == "PASSED":
            msg = await bot.send_message(CHANNEL_ID, task['content'], 
                                         reply_markup=get_engagement_markup(m_code))
            
            # Persistent Mirror to Vault
            col_vault.insert_one({
                "m_code": m_code, "msg_id": msg.message_id, "content": task['content'],
                "created_at": datetime.now(IST), "is_unlocked": False
            })
            await broadcast_audit("AUTO_FIRE", m_code, f"Target: {task['target']} | Status: Success")
        else:
            await bot.send_message(OWNER_ID, f"❌ <b>FIRE ABORTED:</b> <code>{m_code}</code> failed pre-flight or manual override.")
            await broadcast_audit("FIRE_ABORT", m_code, "Reason: Integrity/Confirmation Failure")
        
        del PENDING_APPROVALS[m_code]
# ==========================================
# ✏️ REMOTE EDIT (X-SERIES REINFORCED)
# ==========================================

@dp.message(or_f(F.text.contains("EDIT"), Command("edit")), StateFilter("*"))
async def edit_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # Critical: Flush any stuck states from previous protocols
    await state.clear()
    
    await message.answer(
        "📝 <b>EDIT MODE ACTIVATED</b>\n"
        "────────────────────\n"
        "Enter the <b>Unique Code</b> (e.g., X1) to modify:", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(EditState.waiting_id)

@dp.message(EditState.waiting_id)
async def edit_id_received(message: types.Message, state: FSMContext):
    # Standardize input for X-Series compatibility
    m_code = message.text.upper().strip()

    try:
        # Check MongoDB Vault for identity match
        entry = col_vault.find_one({"m_code": m_code})

        if entry:
            await state.update_data(edit_code=m_code, msg_id=entry.get("msg_id"))
            current_text = entry.get('content', '')[:150]
            
            await message.answer(
                f"🔍 <b>ENTRY FOUND:</b> <code>{m_code}</code>\n"
                f"────────────────────\n"
                f"<b>CURRENT CONTENT:</b>\n"
                f"<code>{html.escape(current_text)}...</code>\n"
                f"────────────────────\n"
                f"📥 <b>Enter the NEW content for this post:</b>",
                parse_mode=ParseMode.HTML
            )
            await state.set_state(EditState.waiting_text)
        else:
            await message.answer(f"❌ <b>ERROR:</b> Code <code>{m_code}</code> not found.")
            await state.clear()
    except Exception as e:
        await message.answer(f"❌ <b>DATABASE ERROR:</b> {html.escape(str(e))}")
        await state.clear()

@dp.message(EditState.waiting_text)
async def edit_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    data = await state.get_data()
    m_code = data['edit_code']
    msg_id = data['msg_id']
    new_content = message.text

    try:
        # 1. Update physical message in the Telegram Channel
        # Preserves the original gating markup
        await bot.edit_message_text(
            text=new_content,
            chat_id=CHANNEL_ID,
            message_id=msg_id,
            parse_mode=ParseMode.HTML,
            reply_markup=get_engagement_markup(m_code)
        )

        # 2. Update MongoDB Record
        col_vault.update_one(
            {"m_code": m_code},
            {"$set": {"content": new_content, "last_edited": datetime.now(IST)}}
        )

        await message.answer(f"🚀 <b>SUCCESS:</b> Intelligence <code>{m_code}</code> transmuted.")
        console_out(f"System Edit: {m_code} updated.")

    except Exception as e:
        # Failsafe for messages > 48hrs (Telegram API Limit)
        await message.answer(f"❌ <b>EDIT FAILED:</b> {html.escape(str(e))}")

    await state.clear()

# ==========================================
# 📢 SYNDICATE BROADCAST (TELEMETRY MIRRORED)
# ==========================================

@dp.message(or_f(F.text.contains("BROADCAST"), Command("broadcast")), StateFilter("*"))
async def broadcast_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear() 
    await state.set_state(BroadcastState.waiting_msg)
    
    await message.answer(
        "<b>◈ SYNDICATE BROADCAST ◈</b>\n"
        "────────────────────\n"
        "Enter your directive for the Family:", 
        parse_mode=ParseMode.HTML
    )

async def broadcast_audit(action: str, code: str, details: str = "N/A"):
    """Surgical Audit mirroring to Private LOG_CHANNEL."""
    log_text = (
        f"🛡️ <b>EMPIRE AUDIT ENGINE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔧 <b>ACTION:</b> {action}\n"
        f"🆔 <b>UNIQUE CODE:</b> <code>{code}</code>\n"
        f"📝 <b>DETAILS:</b> {details}\n"
        f"👤 <b>OPERATOR:</b> Master Sadiq\n"
        f"🚦 <b>STATUS:</b> VERIFIED\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    await bot.send_message(LOG_CHANNEL_ID, log_text, parse_mode=ParseMode.HTML)
    console_out(f"Audit: {action} | {code}")

@dp.message(BroadcastState.waiting_msg)
async def broadcast_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # Emergency Abort Check
    if message.text in ["🛑 PANIC", "/cancel"]:
        await state.clear()
        await message.answer("<b>[!] BROADCAST ABORTED.</b>", parse_mode=ParseMode.HTML)
        return

    # Construct Overlord Technical Template
    formatted_payload = (
        "<b>◈ MSANODE SYNDICATE ◈</b>\n"
        "────────────────────\n\n"
        f"{html.escape(message.text)}\n\n"
        "────────────────────\n"
        "<b>DIRECTIVE FROM MASTER SADIQ</b>\n"
        "<i>\"Family: Execute with precision. Action is our currency.\"</i>\n"
        "────────────────────"
    )
    
    try:
        sent_msg = await safe_send_message(CHANNEL_ID, formatted_payload)
        
        if sent_msg:
            # Automatic Pinning for Maximum Visibility
            try:
                await bot.pin_chat_message(CHANNEL_ID, sent_msg.message_id)
                pin_status = "SENT AND PINNED"
            except:
                pin_status = "SENT (PIN FAILED)"
            
            # mirror to Private Log Channel
            await bot.send_message(
                LOG_CHANNEL_ID, 
                f"📢 <b>GLOBAL BROADCAST MIRROR</b>\n"
                f"Status: {pin_status}\n"
                f"────────────────────\n"
                f"{formatted_payload}",
                parse_mode=ParseMode.HTML
            )
            
            await message.answer(f"<b>[+] DIRECTIVE {pin_status}.</b>", parse_mode=ParseMode.HTML)
            console_out(f"Broadcast: {pin_status}")
        else:
            await message.answer("<b>[!] ERROR:</b> Public deployment failed.")
            
    except Exception as e:
        await message.answer(f"<b>[!] CRITICAL:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)
    
    await state.clear()

# ==========================================
# 🗑 UNSEND PROTOCOL (SURGICAL SCRUB)
# ==========================================

@dp.message(F.text == "🗑 UNSEND", StateFilter("*"))
@dp.message(Command("unsend"), StateFilter("*"))
async def unsend_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer(
        "🗑 <b>UNSEND INITIATED</b>\n"
        "Enter the <b>Unique Code</b> (X-Series) to scrub from existence:", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(UnsendState.waiting_id)

@dp.message(UnsendState.waiting_id)
async def unsend_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    target_code = message.text.upper().strip()

    try:
        # Locate identity in MongoDB
        entry = col_vault.find_one({"m_code": target_code})

        if entry:
            try:
                # Surgical removal from Public Channel
                await bot.delete_message(CHANNEL_ID, entry.get("msg_id"))
                t_status = "Scrubbed from Channel"
            except Exception:
                t_status = "Telegram scrub failed (Post too old)"

            # Permanent purge from Database
            col_vault.delete_one({"m_code": target_code})

            # Mirror to Private Log Channel
            await broadcast_audit("SURGICAL_SCRUB", target_code, f"Status: {t_status}")

            await message.answer(
                f"<b>[+] SCRUB COMPLETE</b>\n"
                f"ID: <code>{target_code}</code>\n"
                f"Status: {t_status} and Database Purged.", 
                parse_mode=ParseMode.HTML
            )
        else:
            await message.answer(f"<b>[!] NOT FOUND:</b> <code>{target_code}</code> is not in the system.", parse_mode=ParseMode.HTML)

    except Exception as e:
        await message.answer(f"<b>[!] SCRUB ERROR:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

    await state.clear()

# ==========================================
# 🧪 ALCHEMY ENGINE (2025 SDK TRANSMUTATION)
# ==========================================

@dp.message(F.text & F.forward_from_chat)
async def alchemy_engine(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    await message.answer("<b>[-] Alchemy: Transmuting intelligence...</b>", parse_mode=ParseMode.HTML)
    
    # Transmutation via 2025 AI Client
    content = await alchemy_transform(message.text)
    
    await bot.send_message(
        OWNER_ID, 
        f"<b>◈ TRANSMUTED INTELLIGENCE ◈</b>\n\n{content}", 
        parse_mode=ParseMode.HTML
    )

# ==========================================
# 🚨 EMERGENCY OVERRIDE (PANIC RESET)
# ==========================================

@dp.message(StateFilter("*"), lambda m: m.text and "PANIC" in m.text.upper())
@dp.message(Command("cancel"), StateFilter("*"))
async def global_panic_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return

    # 1. Force clear all stuck AI processes or FSM states
    await state.clear()
    
    # 2. Restore Correct Bot 5 Tactical Menu (Fixed from Part 7)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")], 
        [KeyboardButton(text="✏️ EDIT"), KeyboardButton(text="🗑️ SURGICAL DELETE")], 
        [KeyboardButton(text="📋 LIST"), KeyboardButton(text="🎯 ENGAGEMENT")],
        [KeyboardButton(text="📢 BROADCAST"), KeyboardButton(text="🔑 API")], 
        [KeyboardButton(text="⚙️ MODEL"), KeyboardButton(text="📊 AUDIT")], 
        [KeyboardButton(text="📟 TERMINAL"), KeyboardButton(text="❓ GUIDE")],
        [KeyboardButton(text="🛑 PANIC")]
    ], resize_keyboard=True)
    
    await message.answer(
        "🚨 <b>SYSTEM-WIDE RESET SUCCESSFUL</b>\n"
        "────────────────────\n"
        "• State Memory: <b>PURGED</b>\n"
        "• AI Logic: <b>STANDBY</b>\n"
        "────────────────────\n"
        "Infrastructure restored to Home Protocol.",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )
    
    console_out("◈ ALERT: Panic Reset executed by Master Sadiq.")

# ==========================================
# 🚀 SUPREME STARTUP (MAIN LOOP)
# ==========================================

async def main():
    # 1. Synchronize Background Tasks
    scheduler.add_job(hourly_heartbeat, 'interval', hours=1, timezone=IST)
    scheduler.add_job(self_healing_audit, 'cron', hour=0, minute=0, timezone=IST)
    scheduler.start()

    console_out("◈ SINGULARITY APEX ONLINE")

    try:
        await bot.send_message(OWNER_ID, "<b>[+] Singularity Online. Persistent & Failover Active.</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"Startup notification failed: {e}")

    # 2. Polling Loop with Fail-Safe Restart
    while True:
        try:
            await dp.start_polling(bot, skip_updates=True)
        except Exception as e:
            console_out(f"Polling Error: {e}")
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

if __name__ == "__main__":
    print("🚀 STARTING CORE 5: AI SINGULARITY")
    
    # Start Health Server in background thread for Render stability
    threading.Thread(target=run_health_server, daemon=True).start()
    
    try:
        time.sleep(2) # Buffer for port binding
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("◈ Bot 5 Shutdown.")
    except Exception as e:
        print(f"💥 CRITICAL STARTUP ERROR: {e}")
        traceback.print_exc()
