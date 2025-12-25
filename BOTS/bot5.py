import asyncio, logging, random, html, threading, os, sys, time, re, pytz, json, io, psutil
import pandas as pd
from datetime import datetime, timedelta
from aiohttp import web
from google import genai
from google.genai import types as ai_types
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pymongo

# ==========================================
# ⚡ SECURE CONFIGURATION
# ==========================================
BOT_TOKEN = os.getenv("BOT_5_TOKEN")
GEMINI_KEY = os.getenv("GEMINI_KEY")
MONGO_URI = os.getenv("MONGO_URI")
OWNER_ID = int(os.getenv("MASTER_ADMIN_ID", 0))
CHANNEL_ID = int(os.getenv("MAIN_CHANNEL_ID", 0))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", 0))

IST = pytz.timezone('Asia/Kolkata')
START_TIME = time.time()
client = genai.Client(api_key=GEMINI_KEY)
MODEL_POOL = ["gemini-2.0-flash", "gemini-1.5-pro"]
CURRENT_MODEL_INDEX = 0
API_USAGE_COUNT = 0

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone=IST)

# ==========================================
# 🗄️ DATABASE & IDENTITY ENGINE
# ==========================================
db_client = pymongo.MongoClient(MONGO_URI)
db = db_client["SingularityDB"]
col_vault = db["vault"]
col_system = db["system_stats"]
col_api = db["api_ledger"]

async def get_next_id(prefix):
    res = col_system.find_one_and_update(
        {"_id": f"counter_{prefix}"},
        {"$inc": {"count": 1}},
        upsert=True,
        return_document=pymongo.ReturnDocument.AFTER
    )
    return f"{prefix}{res['count']}"

CONSOLE_LOGS = []
def console_out(text):
    global CONSOLE_LOGS
    ts = datetime.now(IST).strftime("%H:%M:%S")
    entry = f"[{ts}] {text}"
    CONSOLE_LOGS.append(entry)
    if len(CONSOLE_LOGS) > 12: CONSOLE_LOGS.pop(0)
    logging.info(text)

# ==========================================
# 🧠 AI ORACLE ENGINE (CHIMERA PROMPT)
# ==========================================
def get_system_prompt():
    return """ACT AS: 'MSANODE OVERLORD'. GOAL: Deliver AI Arbitrage/Tactical Tech. 
    TONE: Exclusive, Urgent, Technical. NO PRE-TEXT. NO EMOJIS IN BODY."""

async def generate_content(prompt):
    global API_USAGE_COUNT
    try:
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=MODEL_POOL[CURRENT_MODEL_INDEX],
            contents=prompt[:500],
            config=ai_types.GenerateContentConfig(system_instruction=get_system_prompt())
        )
        API_USAGE_COUNT += 1
        col_api.update_one({"_id": "global_ledger"}, {"$inc": {"usage": 1}}, upsert=True)
        return html.escape(response.text)[:3500], "Success"
    except Exception as e:
        return f"Error: {str(e)[:100]}", "Error"

# ==========================================
# 🚀 AUTOMATION (T-60 FAIL-SAFE)
# ==========================================
PENDING_APPROVALS = {}

async def t60_preflight(sch_id, target_time):
    content, _ = await generate_content("Generate high-value tactical tech breach.")
    PENDING_APPROVALS[sch_id] = {"content": content, "confirmed": False, "time": target_time}
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 ARM", callback_data=f"arm_{sch_id}")]])
    await bot.send_message(OWNER_ID, f"⏳ <b>PRE-FLIGHT (T-60m): {sch_id}</b>\n\n{content}", reply_markup=kb, parse_mode=ParseMode.HTML)

async def t0_execution(sch_id):
    if sch_id in PENDING_APPROVALS:
        data = PENDING_APPROVALS[sch_id]
        msg = await bot.send_message(CHANNEL_ID, data["content"], parse_mode=ParseMode.HTML)
        col_vault.insert_one({"m_code": sch_id, "msg_id": msg.message_id, "content": data["content"], "created_at": datetime.now(IST)})
        await bot.send_message(LOG_CHANNEL_ID, f"🚀 <b>AUTO-FIRE SUCCESS:</b> {sch_id}")
        del PENDING_APPROVALS[sch_id]

# ==========================================
# 🕹️ UI COMMANDS & INTERFACES
# ==========================================
class SingularityState(StatesGroup):
    waiting_topic = State(); waiting_edit_id = State(); waiting_edit_text = State()
    waiting_delete_id = State(); waiting_broadcast = State(); waiting_time = State()

@dp.message(Command("start"), StateFilter("*"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")],
        [KeyboardButton(text="✏️ EDIT"), KeyboardButton(text="🗑 DELETE")],
        [KeyboardButton(text="📋 LIST"), KeyboardButton(text="📟 TERMINAL")],
        [KeyboardButton(text="📥 EXPORT"), KeyboardButton(text="🛡 AUDIT")]
    ], resize_keyboard=True)
    await message.answer("💎 <b>SINGULARITY V5.0</b>\nAwaiting Directive.", reply_markup=kb, parse_mode=ParseMode.HTML)

# ==========================================
# 🔥 BREACH & 📋 LIST & 🗑 DELETE
# ==========================================
@dp.message(F.text == "🔥 BREACH")
async def breach_init(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🤖 AUTO", callback_data="brauto")]])
    await message.answer("🔥 <b>BREACH:</b> Select Mode", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "brauto")
async def br_auto(cb: types.CallbackQuery, state: FSMContext):
    content, _ = await generate_content("Generate viral AI arbitrage.")
    await state.update_data(content=content)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 FIRE", callback_data="brfire")]])
    await cb.message.answer(f"📑 <b>PREVIEW:</b>\n\n{content}", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "brfire")
async def br_fire(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    code = await get_next_id("BR")
    await bot.send_message(CHANNEL_ID, data['content'], parse_mode=ParseMode.HTML)
    await cb.message.edit_text(f"🚀 <b>DEPLOYED:</b> <code>{code}</code>")
    await state.clear()

@dp.message(F.text == "📋 LIST")
async def cmd_list(message: types.Message):
    entries = list(col_vault.find().sort("created_at", -1).limit(5))
    res = "<b>📋 RECENT VAULT:</b>\n" + "\n".join([f"• <code>{e['m_code']}</code>" for e in entries])
    await message.answer(res, parse_mode=ParseMode.HTML)

# ==========================================
# 📡 RENDER FREE TIER GHOST PORT (THE KILLER FIX)
# ==========================================
async def start_health_server():
    """Satisfies Render's port requirement instantly on 0.0.0.0."""
    try:
        app = web.Application()
        app.router.add_get('/', lambda r: web.Response(text="ALIVE"))
        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.getenv("PORT", 10000))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        print(f"✅ GHOST PORT BOUND: {port}")
    except Exception as e: print(f"Port Error: {e}")

# ==========================================
# 🚀 SUPREME BOOTLOADER
# ==========================================
async def main():
    try:
        # STEP 1: BIND PORT IMMEDIATELY (Do not wait for anything else)
        await start_health_server()
        
        # STEP 2: SYNC DATABASE
        global API_USAGE_COUNT
        ledger = col_api.find_one({"_id": "global_ledger"})
        API_USAGE_COUNT = ledger.get("usage", 0) if ledger else 0
        
        # STEP 3: START BOT
        scheduler.start()
        await bot.send_message(OWNER_ID, "💎 <b>SINGULARITY V5 LIVE</b>")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e: print(f"💥 FATAL: {e}")

if __name__ == "__main__":
    asyncio.run(main())
