import asyncio, os, html, time, pytz, logging, random, io, psutil
from datetime import datetime, timedelta
from aiohttp import web
from google import genai
from google.genai import types as ai_types
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
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

IST = pytz.timezone('Asia/Kolkata')
client = genai.Client(api_key=GEMINI_KEY)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone=IST)

db_client = pymongo.MongoClient(MONGO_URI)
db = db_client["Singularity_V5_Final"]
col_vault = db["vault"]
col_system = db["system_stats"]
col_api = db["api_ledger"]

# Models set specifically to Gemini 2.0 series
MODEL_POOL = ["gemini-2.5-flash", "gemini-2.5-pro"]
CURRENT_MODEL_INDEX = 0
API_USAGE_COUNT = 0
PENDING_FIRE = {}

# ==========================================
# 🌐 PROMPT PACKS
# ==========================================
CLOUD_PROMPT_PACK = [
    "Generate a viral cloud computing arbitrage opportunity with real tools and links.",
    "Create an unfair advantage guide for cloud storage hacks and side hustles.",
    "Design a tactical cloud deployment strategy for AI-powered businesses.",
    "Uncover hidden cloud cost-saving techniques with external resources.",
    "Build a scalable cloud infrastructure blueprint for passive income streams.",
    "Explore cloud-based arbitrage plays using cutting-edge technologies.",
    "Craft a cloud migration masterplan for maximum efficiency and profits.",
    "Reveal cloud security exploits and defensive strategies with real links.",
    "Develop a cloud-native app idea for viral monetization.",
    "Analyze cloud market trends for predictive arbitrage opportunities."
]

# --- TELEMETRY HELPERS ---
def console_out(text):
    print(f"[{datetime.now(IST).strftime('%H:%M:%S')}] {text}")

async def increment_api_count_in_db():
    try:
        col_api.update_one({"_id": "global_ledger"}, {"$inc": {"usage": 1}}, upsert=True)
    except Exception as e:
        console_out(f"Ledger Sync Error: {e}")

# ==========================================
# 🧠 ORACLE PROMPT ENGINE (CHIMERA PROTOCOL)
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

async def generate_content(prompt):
    global API_USAGE_COUNT
    if API_USAGE_COUNT >= 1500:
        return "⚠️ <b>CRITICAL:</b> API Limit Reached.", "Limit"
    try:
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=MODEL_POOL[CURRENT_MODEL_INDEX],
            contents=prompt[:500],
            config=ai_types.GenerateContentConfig(system_instruction=get_system_prompt())
        )
        raw_text = response.text if response else "No response."
        clean_content = html.escape(raw_text)[:3500] 
        API_USAGE_COUNT += 1
        await increment_api_count_in_db() 
        return clean_content, "AI Directive"
    except Exception as e:
        console_out(f"GEN ERROR: {e}")
        return f"Error: {html.escape(str(e))[:100]}", "Error"

# ==========================================
# 📡 HARDCODED PORT BINDER (RENDER SHIELD)
# ==========================================
async def start_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', lambda r: web.Response(text="SINGULARITY_V5_LIVE"))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 10000)
        await site.start()
        console_out("PORT 10000 BOUND.")
    except Exception as e: console_out(f"PORT ERROR: {e}")

# ==========================================
# 🕹️ STATE MACHINE & UI
# ==========================================
class SingularityState(StatesGroup):
    waiting_topic = State()
    waiting_sched_time = State()
    waiting_sched_month = State()
    waiting_sched_year = State()
    selecting_days = State()
    waiting_new_api = State()
    waiting_broadcast = State()

async def get_days_kb(selected):
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    row1 = [InlineKeyboardButton(text=f"{'✅' if i in selected else '❌'} {d}", callback_data=f"toggle_{i}") for i, d in enumerate(days[:4])]
    row2 = [InlineKeyboardButton(text=f"{'✅' if i+4 in selected else '❌'} {d}", callback_data=f"toggle_{i+4}") for i, d in enumerate(days[4:])]
    return InlineKeyboardMarkup(inline_keyboard=[row1, row2, [InlineKeyboardButton(text="📥 LOCK PROTOCOL", callback_data="lock_sched")]])

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")],
        [KeyboardButton(text="⚙️ MODELS"), KeyboardButton(text="🔑 API")],
        [KeyboardButton(text="🛡 SCAN"), KeyboardButton(text="📢 BROADCAST")]
    ], resize_keyboard=True)
    await message.answer("💎 <b>SINGULARITY V5.0 LIVE</b>", reply_markup=kb, parse_mode=ParseMode.HTML)

# --- BUTTON 1: BREACH ---
@dp.message(F.text == "🔥 BREACH")
async def breach_menu(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🤖 AUTO", callback_data="brauto"), InlineKeyboardButton(text="✍️ MANUAL", callback_data="brmanual")]])
    await message.answer("🔥 <b>BREACH:</b> Select Mode", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "brauto")
async def br_auto(cb: types.CallbackQuery, state: FSMContext):
    target_prompt = random.choice(CLOUD_PROMPT_PACK)
    content, _ = await generate_content(target_prompt)
    await state.update_data(c=content)
    await cb.message.answer(f"📑 <b>PREVIEW:</b>\n\n{content}\n\n<b>FIRE?</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 FIRE", callback_data="fire")]]), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "brmanual")
async def br_manual(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(SingularityState.waiting_topic)
    await cb.message.answer("🎯 Enter Target Topic:")

@dp.message(SingularityState.waiting_topic)
async def topic_res(message: types.Message, state: FSMContext):
    content, _ = await generate_content(f"Topic: {message.text}")
    await state.update_data(c=content)
    await message.answer(f"📑 <b>PREVIEW:</b>\n\n{content}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 FIRE", callback_data="fire")]]), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "fire")
async def fire_exec(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await bot.send_message(CHANNEL_ID, data['c'], parse_mode=ParseMode.HTML)
    await cb.message.edit_text("🚀 <b>DEPLOYED.</b>")
    await state.clear()

# --- BUTTON 2: SCHEDULE ---
async def t60_preflight(job_id, fire_time):
    content, _ = await generate_content(random.choice(CLOUD_PROMPT_PACK))
    PENDING_FIRE[job_id] = {"content": content, "fired": False}
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 FIRE NOW", callback_data=f"confirm_{job_id}")]])
    await bot.send_message(OWNER_ID, f"⏳ <b>T-60 REMINDER:</b> {job_id}\nTarget: {fire_time}\n\n{content}", reply_markup=kb, parse_mode=ParseMode.HTML)

async def t0_execution(job_id):
    if job_id in PENDING_FIRE and not PENDING_FIRE[job_id]["fired"]:
        await bot.send_message(CHANNEL_ID, PENDING_FIRE[job_id]["content"], parse_mode=ParseMode.HTML)
        PENDING_FIRE[job_id]["fired"] = True
    if job_id in PENDING_FIRE: del PENDING_FIRE[job_id]

@dp.message(F.text == "🗓 SCHEDULE")
async def sched_start(message: types.Message, state: FSMContext):
    await state.set_state(SingularityState.waiting_sched_time)
    await message.answer("🕒 <b>TIME:</b> (e.g. 03:00 PM):")

@dp.message(SingularityState.waiting_sched_time)
async def sched_time(message: types.Message, state: FSMContext):
    await state.update_data(time=message.text.upper())
    await state.set_state(SingularityState.waiting_sched_month)
    await message.answer("📅 <b>MONTH:</b> (1-12):")

@dp.message(SingularityState.waiting_sched_month)
async def sched_month(message: types.Message, state: FSMContext):
    await state.update_data(month=message.text)
    await state.set_state(SingularityState.waiting_sched_year)
    await message.answer("📅 <b>YEAR:</b> (e.g. 2025):")

@dp.message(SingularityState.waiting_sched_year)
async def sched_year(message: types.Message, state: FSMContext):
    await state.update_data(year=message.text, selected_days=[])
    kb = await get_days_kb([])
    await message.answer("🗓 <b>SELECT DAYS:</b>", reply_markup=kb)
    await state.set_state(SingularityState.selecting_days)

@dp.callback_query(F.data.startswith("toggle_"))
async def toggle_day(cb: types.CallbackQuery, state: FSMContext):
    day = int(cb.data.split("_")[1])
    data = await state.get_data()
    sel = data['selected_days']
    if day in sel: sel.remove(day)
    else: sel.append(day)
    await state.update_data(selected_days=sel)
    await cb.message.edit_reply_markup(reply_markup=await get_days_kb(sel))

@dp.callback_query(F.data == "lock_sched")
async def lock_sched(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    sch_id = f"SCH_{random.randint(100,999)}"
    day_names = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    cron_days = ",".join([day_names[i] for i in data['selected_days']])
    t_obj = datetime.strptime(data['time'], "%I:%M %p")
    scheduler.add_job(t60_preflight, CronTrigger(day_of_week=cron_days, hour=(t_obj.hour-1)%24, minute=t_obj.minute), args=[sch_id, data['time']])
    scheduler.add_job(t0_execution, CronTrigger(day_of_week=cron_days, hour=t_obj.hour, minute=t_obj.minute), args=[sch_id])
    await cb.message.edit_text(f"🔒 <b>LOCKED: {sch_id}</b>\nTime: {data['time']}\nDays: {cron_days}")
    await state.clear()

# --- BUTTON 3: MODELS ---
@dp.message(F.text == "⚙️ MODELS")
async def model_menu(message: types.Message):
    global CURRENT_MODEL_INDEX
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 SWAP", callback_data="mod_swap")]])
    await message.answer(f"⚙️ <b>ACTIVE:</b> <code>{MODEL_POOL[CURRENT_MODEL_INDEX]}</code>", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "mod_swap")
async def mod_swap(cb: types.CallbackQuery):
    global CURRENT_MODEL_INDEX
    CURRENT_MODEL_INDEX = (CURRENT_MODEL_INDEX + 1) % len(MODEL_POOL)
    await cb.message.edit_text(f"✅ <b>ENGINE:</b> <code>{MODEL_POOL[CURRENT_MODEL_INDEX]}</code>")

# --- BUTTON 4: API ---
@dp.message(F.text == "🔑 API")
async def api_menu(message: types.Message):
    masked = f"{GEMINI_KEY[:6]}****{GEMINI_KEY[-4:]}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 CHANGE API", callback_data="api_change")]])
    await message.answer(f"🔑 <b>API STATUS:</b> ACTIVE\nKey: <code>{masked}</code>", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "api_change")
async def api_change(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(SingularityState.waiting_new_api)
    await cb.message.answer("📥 Send New Gemini API Key:")

@dp.message(SingularityState.waiting_new_api)
async def api_save(message: types.Message, state: FSMContext):
    global GEMINI_KEY, client
    GEMINI_KEY = message.text.strip()
    client = genai.Client(api_key=GEMINI_KEY)
    await message.answer("🚀 <b>API KEY UPDATED.</b>")
    await state.clear()

# --- BUTTON 5: SCAN ---
@dp.message(F.text == "🛡 SCAN")
async def cmd_scan(message: types.Message):
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    await message.answer(f"🛡 <b>SYSTEM SCAN</b>\nCPU: {cpu}%\nRAM: {ram}%\nDB: NOMINAL\nPORT: 10000 ACTIVE\nAPI QUOTA: {API_USAGE_COUNT}/1500", parse_mode=ParseMode.HTML)

# --- BUTTON 6: BROADCAST ---
@dp.message(F.text == "📢 BROADCAST")
async def broad_init(message: types.Message, state: FSMContext):
    await state.set_state(SingularityState.waiting_broadcast)
    await message.answer("📢 <b>BROADCAST:</b> Enter Directive (Auto-Pinned):")

@dp.message(SingularityState.waiting_broadcast)
async def broad_exec(message: types.Message, state: FSMContext):
    fmt = (f"<b>◈ SYNDICATE DIRECTIVE ◈</b>\n"
           f"────────────────────\n\n"
           f"{html.escape(message.text)}\n\n"
           f"────────────────────")
    msg = await bot.send_message(CHANNEL_ID, fmt, parse_mode=ParseMode.HTML)
    try: await bot.pin_chat_message(CHANNEL_ID, msg.message_id)
    except: pass
    await message.answer("🚀 <b>BROADCAST DEPLOYED & PINNED.</b>")
    await state.clear()

@dp.callback_query(F.data.startswith("confirm_"))
async def manual_fire_confirm(cb: types.CallbackQuery):
    job_id = cb.data.split("_")[1]
    if job_id in PENDING_FIRE and not PENDING_FIRE[job_id]["fired"]:
        await bot.send_message(CHANNEL_ID, PENDING_FIRE[job_id]["content"], parse_mode=ParseMode.HTML)
        PENDING_FIRE[job_id]["fired"] = True
        await cb.message.edit_text("🚀 <b>MANUAL FIRE SUCCESSFUL.</b>")
    await cb.answer()

# ==========================================
# 🚀 BOOTLOADER
# ==========================================
async def main():
    global API_USAGE_COUNT
    await start_health_server()
    ledger = col_api.find_one({"_id": "global_ledger"})
    API_USAGE_COUNT = ledger.get("usage", 0) if ledger else 0
    scheduler.start()
    await bot.send_message(OWNER_ID, "💎 <b>APEX SINGULARITY v5.0 ONLINE</b>")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
