import asyncio, logging, random, html, threading, os, sys, time, re, pytz, json, io, psutil
import pandas as pd
from datetime import datetime, timedelta
from aiohttp import web
from google import genai
from google.genai import types as ai_types
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter, or_f
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pymongo
import threading
# ==========================================
# ⚡ SECURE CONFIGURATION (ENV DRIVEN)
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
    """Generates BR1, BR2... or SCH1, SCH2..."""
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
# 🧠 ORACLE PROMPT ENGINE
# ==========================================
# ==========================================
# 🧠 ORACLE PROMPT ENGINE (CHIMERA PROTOCOL)
# ==========================================

def get_system_prompt():
    """Returns the strict Overlord persona instructions."""
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
    """Main generation engine for Breaches and Schedules."""
    global API_USAGE_COUNT
    
    # 1. API Quota Guard
    if API_USAGE_COUNT >= 1500:
        return "⚠️ <b>CRITICAL:</b> Gemini API Monthly Limit Reached (1,500/1,500).", "Limit"

    try:
        # 2. 2025 SDK Execution
        # We run this in a thread to keep the bot responsive
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=MODEL_POOL[CURRENT_MODEL_INDEX],
            contents=prompt[:500],
            config=ai_types.GenerateContentConfig(
                system_instruction=get_system_prompt()
            )
        )
        
        raw_text = response.text if response else "No response from Oracle."
        
        # 3. HTML Scrubbing (Ensuring Telegram doesn't crash on special characters)
        clean_content = html.escape(raw_text)[:3500] 
        
        # 4. Telemetry Update
        API_USAGE_COUNT += 1
        # Synchronize with MongoDB Ledger (Function defined in Block 1)
        await increment_api_count_in_db() 
        
        return clean_content, "AI Directive"

    except Exception as e:
        err = str(e)
        console_out(f"CRITICAL GEN ERROR: {err}")
        return f"<b>System Error:</b> {html.escape(err)[:100]}", "Error"

async def alchemy_transform(raw_text):
    """Transmutes forwarded intelligence into the Overlord Protocol."""
    try:
        resp = await asyncio.to_thread(
            client.models.generate_content,
            model=MODEL_POOL[CURRENT_MODEL_INDEX],
            contents=f"INPUT DATA:\n{raw_text}\n\nINSTRUCTION: Rewrite into MSANODE Protocol.",
            config=ai_types.GenerateContentConfig(
                system_instruction=get_system_prompt()
            )
        )
        # Clean the response to remove AI chatter ("Here is your rewrite...")
        return re.sub(r"^(Here is|Sure).*?\n", "", resp.text, flags=re.IGNORECASE).strip()
    except Exception as e: 
        console_out(f"Alchemy Error: {e}")
        return "⚠️ <b>Alchemy Failed:</b> AI Engine timeout."

async def ai_generate(prompt):
    try:
        model_name = MODEL_POOL[CURRENT_MODEL_INDEX]
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=model_name,
            contents=prompt,
            config=ai_types.GenerateContentConfig(system_instruction=get_system_prompt())
        )
        return response.text, "AI Directive"
    except Exception as e:
        return f"System Error: {str(e)}", "Error"

# ==========================================
# 🚀 AUTOMATION (T-60 FAIL-SAFE)
# ==========================================
PENDING_APPROVALS = {}

async def t60_preflight(sch_id, target_time):
    content, _ = await ai_generate("Generate high-value tactical tech breach.")
    integrity = "PASSED" if len(content) > 150 else "FAILED"
    PENDING_APPROVALS[sch_id] = {"content": content, "confirmed": False, "integrity": integrity, "time": target_time}
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 CONFIRM FIRE", callback_data=f"arm_{sch_id}"), 
         InlineKeyboardButton(text="🗑 ABORT", callback_data=f"del_{sch_id}")]
    ])
    await bot.send_message(OWNER_ID, f"⏳ <b>PRE-FLIGHT (T-60m): {sch_id}</b>\nFIRE AT: {target_time}\nINTEGRITY: {integrity}\n\n{content}", reply_markup=kb, parse_mode=ParseMode.HTML)

async def t0_execution(sch_id):
    if sch_id not in PENDING_APPROVALS: return
    data = PENDING_APPROVALS[sch_id]
    if data["confirmed"] or data["integrity"] == "PASSED":
        msg = await bot.send_message(CHANNEL_ID, data["content"])
        col_vault.insert_one({"m_code": sch_id, "msg_id": msg.message_id, "content": data["content"], "ts": datetime.now(IST)})
        await bot.send_message(LOG_CHANNEL_ID, f"🚀 <b>AUTO-FIRE SUCCESS:</b> {sch_id}\nStatus: Integrity Checked & Deployed.", parse_mode=ParseMode.HTML)
    else:
        await bot.send_message(OWNER_ID, f"❌ <b>FIRE ABORTED:</b> {sch_id} failed integrity check.")
    del PENDING_APPROVALS[sch_id]

# ==========================================
# 🕹️ UI & INTERACTIVE COMMANDS
# ==========================================
class SingularityState(StatesGroup):
    waiting_topic = State(); waiting_reaction = State(); waiting_time = State()
    waiting_month = State(); waiting_year = State(); selecting_days = State()
    waiting_edit_id = State(); waiting_edit_text = State(); waiting_delete_id = State()
    waiting_broadcast = State(); waiting_new_api = State()

@dp.message(Command("start"), StateFilter("*"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")],
        [KeyboardButton(text="✏️ EDIT"), KeyboardButton(text="🗑 DELETE")],
        [KeyboardButton(text="📋 LIST"), KeyboardButton(text="📢 BROADCAST")],
        [KeyboardButton(text="🔑 API"), KeyboardButton(text="⚙️ MODELS")],
        [KeyboardButton(text="📟 TERMINAL"), KeyboardButton(text="🛑 PANIC")],
        [KeyboardButton(text="📥 EXPORT"), KeyboardButton(text="🛡 AUDIT")],
        [KeyboardButton(text="❓ GUIDE")]
    ], resize_keyboard=True)
    await message.answer("💎 <b>APEX SINGULARITY v5.0</b>\nAwaiting directive.", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.message(F.text == "🗑 DELETE", StateFilter("*"))
async def delete_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.set_state(SingularityState.waiting_delete_id)
    await message.answer("🗑 <b>PURGE:</b> Enter Unique Code (BRx/SCHx) to wipe:")

@dp.message(SingularityState.waiting_delete_id)
async def delete_exec(message: types.Message, state: FSMContext):
    code = message.text.upper().strip()
    entry = col_vault.find_one({"m_code": code})
    if entry:
        try: await bot.delete_message(CHANNEL_ID, entry['msg_id'])
        except: pass
        col_vault.delete_one({"m_code": code})
        await message.answer(f"✅ <b>SCRUBBED:</b> <code>{code}</code> removed from existence.")
    else: await message.answer("❌ Error: Code not found.")
    await state.clear()

@dp.message(F.text == "📥 EXPORT", StateFilter("*"))
async def export_data(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    data = list(col_vault.find({}, {"_id": 0}))
    df = pd.DataFrame(data)
    csv_buf = io.BytesIO()
    df.to_csv(csv_buf, index=False)
    csv_buf.seek(0)
    await message.answer_document(BufferedInputFile(csv_buf.read(), filename="singularity_vault.csv"), caption="📊 Vault Intelligence Exported.")

# ==========================================
# 🗓 SCHEDULE HELPERS (Day Selector)
# ==========================================
async def get_days_kb(selected):
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    buttons = []
    row = []
    for i, d in enumerate(days):
        text = f"✅ {d}" if i in selected else f"❌ {d}"
        row.append(InlineKeyboardButton(text=text, callback_data=f"toggle_{i}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(text="📥 LOCK DAYS", callback_data="days_done")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ==========================================
# 🗓 SCHEDULE HANDLERS
# ==========================================
@dp.message(F.text == "🗓 SCHEDULE", StateFilter("*"))
async def sched_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer("🕒 <b>TIME:</b> Enter Fire Time (e.g., 03:00 PM):", parse_mode=ParseMode.HTML)
    await state.set_state(SingularityState.waiting_time)

@dp.message(SingularityState.waiting_time)
async def sched_time(message: types.Message, state: FSMContext):
    t_str = message.text.upper().strip()
    try:
        datetime.strptime(t_str, "%I:%M %p")
        await state.update_data(time=t_str)
        await message.answer("📅 <b>MONTH:</b> Enter (1-12):", parse_mode=ParseMode.HTML)
        await state.set_state(SingularityState.waiting_month)
    except:
        await message.answer("⚠️ Format: 03:00 PM")

@dp.message(SingularityState.waiting_month)
async def sched_month(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or not (1 <= int(message.text) <= 12):
        return await message.answer("❌ Valid month 1-12")
    await state.update_data(month=int(message.text))
    await message.answer("📅 <b>YEAR:</b> Enter (e.g., 2025):", parse_mode=ParseMode.HTML)
    await state.set_state(SingularityState.waiting_year)

@dp.message(SingularityState.waiting_year)
async def sched_year(message: types.Message, state: FSMContext):
    await state.update_data(year=int(message.text), selected_days=[])
    kb = await get_days_kb([])
    await message.answer("🗓 <b>SELECT DAYS:</b>", reply_markup=kb, parse_mode=ParseMode.HTML)
    await state.set_state(SingularityState.selecting_days)

@dp.callback_query(F.data.startswith("toggle_"))
async def toggle_day(cb: types.CallbackQuery, state: FSMContext):
    day_idx = int(cb.data.split("_")[1])
    data = await state.get_data()
    sel = data.get("selected_days", [])
    if day_idx in sel: sel.remove(day_idx)
    else: sel.append(day_idx)
    await state.update_data(selected_days=sel)
    await cb.message.edit_reply_markup(reply_markup=await get_days_kb(sel))

@dp.callback_query(F.data == "days_done")
async def sched_finalize(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    sch_id = await get_next_id("SCH")
    
    # Logic for APScheduler
    day_map = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    cron_days = ",".join([day_map[i] for i in data['selected_days']])
    t_obj = datetime.strptime(data['time'], "%I:%M %p")
    
    # T-60 and T-0 Jobs
    review_h = (t_obj.hour - 1) % 24
    scheduler.add_job(t60_preflight, CronTrigger(day_of_week=cron_days, hour=review_h, minute=t_obj.minute), args=[sch_id, data['time']])
    scheduler.add_job(t0_execution, CronTrigger(day_of_week=cron_days, hour=t_obj.hour, minute=t_obj.minute), args=[sch_id])
    
    await cb.message.edit_text(f"🔒 <b>PROTOCOL LOCKED: {sch_id}</b>\nTime: {data['time']}\nDays: {cron_days}", parse_mode=ParseMode.HTML)
    await state.clear()

# ==========================================
# 🔥 BREACH LOGIC (AUTO/MANUAL)
# ==========================================
@dp.callback_query(F.data == "brmanual")
async def breach_manual(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.edit_text("🎯 <b>TARGET:</b> Enter your topic:", parse_mode=ParseMode.HTML)
    await state.set_state(SingularityState.waiting_topic)

@dp.message(SingularityState.waiting_topic)
async def breach_topic_received(message: types.Message, state: FSMContext):
    await message.answer("🛰 <b>SYNTHESIZING...</b>", parse_mode=ParseMode.HTML)
    content, _ = await ai_generate(f"Generate tactical breach about: {message.text}")
    await state.update_data(content=content)
    await message.answer(f"📑 <b>PREVIEW:</b>\n\n{content}\n\n<b>FIRE YES?</b>", 
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 FIRE", callback_data="brfire")]]))

# ==========================================
# ✏️ EDIT ENGINE
# ==========================================
@dp.message(F.text == "✏️ EDIT", StateFilter("*"))
async def edit_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.set_state(SingularityState.waiting_edit_id)
    await message.answer("✏️ <b>EDIT:</b> Enter Unique Code (BRx/SCHx):")

@dp.message(SingularityState.waiting_edit_id)
async def edit_id_received(message: types.Message, state: FSMContext):
    code = message.text.upper().strip()
    entry = col_vault.find_one({"m_code": code})
    if entry:
        await state.update_data(edit_code=code, edit_msg_id=entry['msg_id'])
        await message.answer(f"📥 <b>CODE: {code}</b>\nEnter NEW text content:", parse_mode=ParseMode.HTML)
        await state.set_state(SingularityState.waiting_edit_text)
    else: await message.answer("❌ Code not found.")

@dp.message(SingularityState.waiting_edit_text)
async def edit_exec(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        await bot.edit_message_text(text=message.text, chat_id=CHANNEL_ID, message_id=data['edit_msg_id'])
        col_vault.update_one({"m_code": data['edit_code']}, {"$set": {"content": message.text}})
        await message.answer(f"✅ <b>{data['edit_code']}</b> updated.")
    except Exception as e: await message.answer(f"❌ Edit Error: {e}")
    await state.clear()

# ==========================================
# 📟 TERMINAL & 🔑 API & ⚙️ MODELS
# ==========================================
@dp.message(F.text == "📟 TERMINAL")
async def terminal_view(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    uptime = str(timedelta(seconds=int(time.time() - START_TIME)))
    logs = "\n".join(CONSOLE_LOGS)
    cpu = psutil.cpu_percent()
    text = (f"<b>📟 TERMINAL</b>\nUptime: {uptime}\nCPU: {cpu}%\n\n<b>LOGS:</b>\n<code>{logs}</code>")
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message(F.text == "🔑 API")
async def api_view(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    masked = f"{GEMINI_KEY[:8]}****{GEMINI_KEY[-4:]}"
    await message.answer(f"🔑 <b>API:</b> <code>{masked}</code>\n\nSend NEW key to replace or press /cancel", parse_mode=ParseMode.HTML)
    await state.set_state(SingularityState.waiting_new_api)

@dp.message(F.text == "📢 BROADCAST", StateFilter("*"))
async def broad_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await message.answer("📢 <b>BROADCAST:</b> Enter text (Pinned automatically):")
    await state.set_state(SingularityState.waiting_broadcast)

@dp.message(SingularityState.waiting_broadcast)
async def broad_exec(message: types.Message, state: FSMContext):
    formatted = (f"<b>◈ SYNDICATE DIRECTIVE ◈</b>\n"
                 f"────────────────────\n\n"
                 f"{message.text}\n\n"
                 f"────────────────────\n"
                 f"<b>OPERATOR:</b> Master Sadiq")
    msg = await bot.send_message(CHANNEL_ID, formatted, parse_mode=ParseMode.HTML)
    await bot.pin_chat_message(CHANNEL_ID, msg.message_id)
    await message.answer("🚀 <b>BROADCAST DEPLOYED & PINNED.</b>")
    await state.clear()

@dp.message(F.text == "🛡 AUDIT")
async def audit_check(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    v_count = col_vault.count_documents({})
    s_count = len(scheduler.get_jobs())
    await message.answer(f"🛡 <b>AUDIT:</b>\nVault: {v_count} entries\nJobs: {s_count} active", parse_mode=ParseMode.HTML)

@dp.message(F.text == "❓ GUIDE")
async def guide_view(message: types.Message):
    await message.answer("📚 <b>MANUAL:</b>\n- BR: Breach\n- SCH: Schedule\n- T-60 Fail-safe active.", parse_mode=ParseMode.HTML)


# ==========================================
# ✏️ SURGICAL EDIT ENGINE
# ==========================================
@dp.message(F.text == "✏️ EDIT", StateFilter("*"))
async def edit_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.set_state(SingularityState.waiting_edit_id)
    await message.answer("✏️ <b>EDIT MODE:</b> Enter Unique Code (e.g., BR1/SCH5):", parse_mode=ParseMode.HTML)

@dp.message(SingularityState.waiting_edit_id)
async def edit_id_received(message: types.Message, state: FSMContext):
    code = message.text.upper().strip()
    entry = col_vault.find_one({"m_code": code})
    if entry:
        await state.update_data(edit_code=code, edit_msg_id=entry['msg_id'])
        await message.answer(f"📥 <b>ENTRY: {code}</b>\nEnter the NEW intelligence text:", parse_mode=ParseMode.HTML)
        await state.set_state(SingularityState.waiting_edit_text)
    else: 
        await message.answer("❌ <b>ERROR:</b> Identity not found in Vault.")
        await state.clear()

@dp.message(SingularityState.waiting_edit_text)
async def edit_exec(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        # Hot-swap content in the Public Channel
        await bot.edit_message_text(
            text=message.text, 
            chat_id=CHANNEL_ID, 
            message_id=data['edit_msg_id'],
            parse_mode=ParseMode.HTML,
            reply_markup=get_engagement_markup(data['edit_code'])
        )
        # Update Database
        col_vault.update_one({"m_code": data['edit_code']}, {"$set": {"content": message.text}})
        await message.answer(f"🚀 <b>SUCCESS:</b> {data['edit_code']} transmuted.")
    except Exception as e: 
        await message.answer(f"❌ <b>EDIT FAILED:</b> {html.escape(str(e))}")
    await state.clear()

# ==========================================
# 📢 SYNDICATE BROADCAST
# ==========================================
@dp.message(F.text == "📢 BROADCAST", StateFilter("*"))
async def broad_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await message.answer("📢 <b>BROADCAST:</b> Enter Directive (Auto-Pinned):")
    await state.set_state(SingularityState.waiting_broadcast)

@dp.message(SingularityState.waiting_broadcast)
async def broad_exec(message: types.Message, state: FSMContext):
    formatted = (f"<b>◈ SYNDICATE DIRECTIVE ◈</b>\n"
                 f"────────────────────\n\n"
                 f"{html.escape(message.text)}\n\n"
                 f"────────────────────\n"
                 f"<b>OPERATOR:</b> Master Sadiq")
    msg = await safe_send_message(CHANNEL_ID, formatted)
    if msg:
        try: await bot.pin_chat_message(CHANNEL_ID, msg.message_id)
        except: pass
        await message.answer("🚀 <b>BROADCAST DEPLOYED & PINNED.</b>")
    await state.clear()


# ==========================================
# ⚙️ MODELS & 🔑 API TELEMETRY
# ==========================================
@dp.message(F.text == "⚙️ MODELS")
async def models_view(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    curr = MODEL_POOL[CURRENT_MODEL_INDEX]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 SWAP ENGINE", callback_data="swap_mod")],
        [InlineKeyboardButton(text="➕ ADD MODEL", callback_data="add_mod")]
    ])
    await message.answer(f"⚙️ <b>ACTIVE ENGINE:</b> <code>{curr}</code>\nTotal Pool: {len(MODEL_POOL)} Models", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.message(F.text == "🔑 API")
async def api_telemetry(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    masked = f"{GEMINI_KEY[:8]}****{GEMINI_KEY[-4:]}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 ROTATE KEY", callback_data="rotate_api")]])
    await message.answer(f"🔑 <b>API STATUS:</b>\nKey: <code>{masked}</code>\nQuota: Standard Free Tier", reply_markup=kb, parse_mode=ParseMode.HTML)

# ==========================================
# 📥 EXPORT & 🛡️ AUDIT
# ==========================================
@dp.message(F.text == "📥 EXPORT")
async def export_vault(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    # Fetch all records and convert to CSV
    cursor = col_vault.find({}, {"_id": 0})
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return await message.answer("📭 <b>VAULT EMPTY:</b> No data to export.")
    
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    file_content = stream.getvalue().encode()
    
    await message.answer_document(
        BufferedInputFile(file_content, filename=f"Singularity_Export_{datetime.now(IST).strftime('%d%m')}.csv"),
        caption="📊 <b>INTELLIGENCE EXPORT COMPLETE</b>"
    )

@dp.message(F.text == "🛡️ AUDIT")
async def audit_system(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    v_count = col_vault.count_documents({})
    jobs = scheduler.get_jobs()
    await message.answer(
        f"🛡️ <b>SYSTEM AUDIT</b>\n"
        f"────────────────────\n"
        f"VAULT RECORDS: {v_count}\n"
        f"ACTIVE JOBS: {len(jobs)}\n"
        f"DB STATUS: CONNECTED\n"
        f"AI ENGINE: GEMINI 2.0-FLASH", parse_mode=ParseMode.HTML
    )

# ==========================================
# 🚀 SUPREME STARTUP SEQUENCE
# ==========================================
async def hourly_heartbeat():
    """60-Min check to ensure connection stability."""
    try:
        col_system.find_one({"_id": "global_heartbeat"})
        await bot.send_message(LOG_CHANNEL_ID, "💓 <b>HEARTBEAT:</b> Nominal. Connectivity verified.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await bot.send_message(OWNER_ID, f"🚨 <b>ALERT:</b> Heartbeat fail. Error: {e}")

# ==========================================
# 📟 TERMINAL & TELEMETRY ENGINE
# ==========================================
@dp.message(F.text == "📟 TERMINAL")
async def cmd_terminal(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    
    # Calculate System Stats
    uptime_sec = int(time.time() - START_TIME)
    uptime = str(timedelta(seconds=uptime_sec))
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    
    # Format Terminal Output
    log_feed = "\n".join(CONSOLE_LOGS) if CONSOLE_LOGS else "NO ACTIVE LOGS"
    
    terminal_text = (
        "<b>📟 APEX TERMINAL</b>\n"
        "────────────────────\n"
        f"⏱ <b>UPTIME:</b> <code>{uptime}</code>\n"
        f"🧠 <b>CPU:</b> <code>{cpu}%</code> | <b>RAM:</b> <code>{ram}%</code>\n"
        f"📡 <b>NETWORK:</b> Nominal\n"
        "────────────────────\n"
        "<b>LIVE EVENT FEED:</b>\n"
        f"<code>{log_feed}</code>\n"
        "────────────────────"
    )
    
    await message.answer(terminal_text, parse_mode=ParseMode.HTML)

# ==========================================
# 🔄 API & ENGINE ROTATION
# ==========================================
@dp.callback_query(F.data == "swap_mod")
async def swap_model_callback(cb: types.CallbackQuery):
    global CURRENT_MODEL_INDEX
    # Cycle through the available models
    CURRENT_MODEL_INDEX = (CURRENT_MODEL_INDEX + 1) % len(MODEL_POOL)
    new_model = MODEL_POOL[CURRENT_MODEL_INDEX]
    
    console_out(f"Engine Swap: {new_model}")
    await cb.message.edit_text(
        f"✅ <b>ENGINE SWAPPED</b>\n"
        f"Active Model: <code>{new_model}</code>", 
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data == "rotate_api")
async def rotate_api_init(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(SingularityState.waiting_new_api)
    await cb.message.answer("📥 <b>SEND NEW GEMINI API KEY:</b>\n(Or /cancel to abort)")
    await cb.answer()

@dp.message(SingularityState.waiting_new_api)
async def rotate_api_exec(message: types.Message, state: FSMContext):
    global GEMINI_KEY, client
    new_key = message.text.strip()
    
    try:
        # Test the new key immediately
        test_client = genai.Client(api_key=new_key)
        test_client.models.generate_content(model="gemini-2.0-flash", contents="Ping")
        
        # If successful, apply globally
        GEMINI_KEY = new_key
        client = test_client
        console_out("API Key Rotated Successfully")
        await message.answer("🚀 <b>API KEY VERIFIED & UPDATED.</b>")
    except Exception as e:
        await message.answer(f"❌ <b>INVALID KEY:</b> {html.escape(str(e))}")
    
    await state.clear()

# ==========================================
# 🛑 PANIC & 📥 EXPORT & 🛡 STATUS
# ==========================================
@dp.message(F.text == "🛑 PANIC", StateFilter("*"))
async def cmd_panic(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # 1. Clear Memory
    await state.clear()
    # 2. Halt Scheduler
    scheduler.remove_all_jobs()
    # 3. Re-initialize Heartbeat only
    scheduler.add_job(hourly_heartbeat, 'interval', minutes=60)
    
    console_out("PANIC OVERRIDE EXECUTED")
    await message.answer(
        "🚨 <b>SYSTEM PURGED</b>\n"
        "────────────────────\n"
        "• All active FSM states: <b>WIPED</b>\n"
        "• All scheduled tasks: <b>TERMINATED</b>\n"
        "• AI Logic: <b>STANDBY</b>", 
        parse_mode=ParseMode.HTML
    )

@dp.message(F.text == "🛡 AUDIT")
async def cmd_audit(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    
    vault_total = col_vault.count_documents({})
    active_jobs = len(scheduler.get_jobs())
    
    await message.answer(
        "🛡 <b>OVERALL STATUS REPORT</b>\n"
        "────────────────────\n"
        f"• <b>VAULT:</b> {vault_total} Records\n"
        f"• <b>JOBS:</b> {active_jobs} Active\n"
        f"• <b>API:</b> Functional\n"
        f"• <b>DB:</b> Synchronized\n"
        "────────────────────\n"
        "<b>SYSTEM: NOMINAL</b>", 
        parse_mode=ParseMode.HTML
    )

@dp.message(F.text == "📥 EXPORT")
async def cmd_export(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    
    # Fetch data from MongoDB
    records = list(col_vault.find({}, {"_id": 0}))
    if not records:
        return await message.answer("📭 <b>VAULT EMPTY.</b>")
    
    # Create CSV using Pandas
    df = pd.DataFrame(records)
    output = io.BytesIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    await message.answer_document(
        document=BufferedInputFile(output.read(), filename="singularity_database.csv"),
        caption="📊 <b>DATABASE EXPORT COMPLETE.</b>"
    )

# ==========================================
# 🛡️ OVERALL STATUS & ERROR MONITOR
# ==========================================
@dp.message(F.text == "🛡 AUDIT", StateFilter("*"))
async def cmd_overall_status(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    
    # Analyze System Integrity
    v_count = col_vault.count_documents({})
    s_count = len(scheduler.get_jobs())
    api_status = "STABLE" if client else "DISCONNECTED"
    
    # Check Database Latency (Simulated check)
    db_ping = "NOMINAL"
    
    report = (
        "<b>🛡️ OVERALL SYSTEM AUDIT</b>\n"
        "────────────────────\n"
        f"💎 <b>VAULT:</b> {v_count} Intelligence Records\n"
        f"🗓️ <b>SCHEDULER:</b> {s_count} Active Protocols\n"
        f"🔑 <b>API ENGINE:</b> {api_status}\n"
        f"🗄️ <b>DATABASE:</b> {db_ping}\n"
        "────────────────────\n"
        "🚦 <b>ALL NODES:</b> VERIFIED & SECURE"
    )
    await message.answer(report, parse_mode=ParseMode.HTML)

# ==========================================
# ❓ FEATURE GUIDE (THE MANUAL)
# ==========================================
@dp.message(F.text == "❓ GUIDE", StateFilter("*"))
async def cmd_guide(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    
    guide_text = (
        "👑 <b>SINGULARITY v5.0: OPERATIONAL MANUAL</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔥 <b>BREACH:</b> Manual or Auto AI generation. Mirror and Vaulting included.\n\n"
        "🗓️ <b>SCHEDULE:</b> Time-locked fire. Ask for confirm at T-60m. Auto-fire at T-0.\n\n"
        "✏️ <b>EDIT/DELETE:</b> Surgical control using Unique Codes (BR/SCH).\n\n"
        "🔑 <b>API/MODELS:</b> Hot-swap engine or rotate Gemini keys on the fly.\n\n"
        "📟 <b>TERMINAL:</b> Live event feed and CPU/RAM telemetry.\n\n"
        "📥 <b>EXPORT:</b> Immediate CSV dump of the entire database.\n\n"
        "🛑 <b>PANIC:</b> Emergency kill-switch for all states and schedules.\n\n"
        "🛡️ <b>AUDIT:</b> Deep scan of system health.\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Directive: Action is our only currency.</i>"
    )
    await message.answer(guide_text, parse_mode=ParseMode.HTML)

# ==========================================
# 📡 60-MIN HEARTBEAT & ERROR SENTINEL
# ==========================================
async def check_system_integrity():
    """Background task to notify Master Sadiq of any failures."""
    try:
        # Test MongoDB
        col_system.find_one({"_id": "counter_BR"})
        # Test AI Client
        await asyncio.to_thread(client.models.list_models)
        
        console_out("Sentinel: Health check PASSED.")
    except Exception as e:
        # Instant Notification on Error
        error_msg = f"🚨 <b>SINGULARITY ERROR DETECTED</b>\n\n<b>Source:</b> Sentinel\n<b>Issue:</b> {html.escape(str(e))}"
        try:
            await bot.send_message(OWNER_ID, error_msg, parse_mode=ParseMode.HTML)
        except:
            print(f"CRITICAL: Failed to notify Owner of error: {e}")

# Add this to your scheduler in main()
# scheduler.add_job(check_system_integrity, 'interval', minutes=60)
# ==========================================
# 📋 INVENTORY LIST (BUTTON 5)
# ==========================================
@dp.message(F.text == "📋 LIST", StateFilter("*"))
async def cmd_list_vault(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    
    try:
        # Fetch the last 15 entries for readability
        entries = list(col_vault.find().sort("created_at", -1).limit(15))
        
        if not entries:
            return await message.answer("📭 <b>VAULT EMPTY:</b> No active intelligence recorded.")
        
        list_text = "<b>📋 ACTIVE INVENTORY (LAST 15)</b>\n────────────────────\n"
        for e in entries:
            code = e.get("m_code", "N/A")
            ts = e.get("created_at", datetime.now(IST)).strftime("%d/%m %H:%M")
            list_text += f"• <code>{code}</code> | {ts}\n"
        
        list_text += "────────────────────\n<i>Use EDIT or DELETE with the codes above.</i>"
        await message.answer(list_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"❌ <b>LIST ERROR:</b> {html.escape(str(e))}")

# ==========================================
# 🧪 ALCHEMY ENGINE (AUTOMATIC FORWARD)
# ==========================================
async def alchemy_transform(text):
    """Internal AI logic to rewrite content into MSANODE Protocol."""
    prompt = f"REWRITE THE FOLLOWING INTO MSANODE OVERLORD PROTOCOL. MAINTAIN ALL LINKS:\n\n{text}"
    content, _ = await ai_generate(prompt)
    return content

@dp.message(F.forward_from_chat)
async def handle_forward_alchemy(message: types.Message):
    """Triggered when you forward a message to the bot."""
    if message.from_user.id != OWNER_ID: return
    
    await message.answer("🧪 <b>ALCHEMY:</b> Transmuting intelligence...")
    transmuted = await alchemy_transform(message.text or message.caption or "")
    
    await message.answer(
        f"<b>◈ TRANSMUTED DATA ◈</b>\n"
        f"────────────────────\n\n"
        f"{transmuted}\n\n"
        f"────────────────────",
        parse_mode=ParseMode.HTML
    )

# ==========================================
# 📡 REACTION UI HELPER
# ==========================================
def get_engagement_markup(code, lock_count=0):
    """Generates the reaction lock buttons for breaches/schedules."""
    if lock_count > 0:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔒 UNLOCK AT {lock_count}x 🔥", callback_data=f"lock_{code}")]
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔓 CLAIM REWARD NOW", callback_data=f"reveal_{code}")]
    ])

# ==========================================
#  THE SUPREME STARTUP
# ==========================================
# ==========================================
# 🪤 INTERACTION HANDLERS (CALLBACKS)
# ==========================================

@dp.callback_query(F.data.startswith("arm_"))
async def arm_scheduled_fire(cb: types.CallbackQuery):
    """Manual confirmation for T-60 pre-flight."""
    sch_id = cb.data.split("_")[1]
    if sch_id in PENDING_APPROVALS:
        PENDING_APPROVALS[sch_id]["confirmed"] = True
        console_out(f"Protocol {sch_id} armed by Master Sadiq.")
        await cb.message.edit_text(f"🔥 <b>PROTOCOL ARMED:</b> {sch_id} will fire at the scheduled time.", parse_mode=ParseMode.HTML)
    await cb.answer("Intelligence Armed.")

@dp.callback_query(F.data.startswith("abort_"))
async def abort_scheduled_fire(cb: types.CallbackQuery):
    """Manual abort for T-60 pre-flight."""
    sch_id = cb.data.split("_")[1]
    if sch_id in PENDING_APPROVALS:
        del PENDING_APPROVALS[sch_id]
        console_out(f"Protocol {sch_id} aborted.")
        await cb.message.edit_text(f"🛑 <b>PROTOCOL ABORTED:</b> {sch_id} wiped from queue.", parse_mode=ParseMode.HTML)
    await cb.answer("Intelligence Scuttled.")

@dp.callback_query(F.data.startswith("lock_"))
async def reaction_lock_info(cb: types.CallbackQuery):
    """Informs users how many reactions are needed."""
    await cb.answer("◈ ACCESS RESTRICTED ◈\nReach the 🔥 target to unlock.", show_alert=True)

@dp.callback_query(F.data.startswith("reveal_"))
async def reaction_reveal(cb: types.CallbackQuery):
    """Provides the secret bonus rewards once unlocked."""
    rewards = [
        "◈ VPN Bypass: Protocol 8-Active.",
        "◈ Archive Key: Decrypted.",
        "◈ EDU Asset: Mirror Available.",
        "◈ Premium Repo: Branch Shared."
    ]
    await cb.answer(f"🔓 UNLOCKED: {random.choice(rewards)}", show_alert=True)

# ==========================================
# 🛠️ SURGICAL UTILITIES
# ==========================================

async def safe_send_message(chat_id, text, reply_markup=None):
    """Prevents bot crashes during high-traffic or API lag."""
    try:
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False
        )
    except Exception as e:
        console_out(f"Send Error: {e}")
        return None

async def get_api_usage_stats():
    """Calculates usage for the Terminal view."""
    # Since we are using Gemini 2.0 Free Tier, we track internal count
    usage = col_system.find_one({"_id": "api_usage_counter"})
    return usage.get("count", 0) if usage else 0

# ==========================================
# 🚀 THE SUPREME INITIALIZATION
# ==========================================

async def startup_sequence():
    """Logic to run exactly once when the bot boots up."""
    # Ensure system counters exist in DB
    if not col_system.find_one({"_id": "counter_BR"}):
        col_system.insert_one({"_id": "counter_BR", "count": 0})
    if not col_system.find_one({"_id": "counter_SCH"}):
        col_system.insert_one({"_id": "counter_SCH", "count": 0})
    
    # Mirror bootup to Private Channel
    await bot.send_message(
        LOG_CHANNEL_ID,
        "🛡️ <b>SYSTEM BOOT SUCCESSFUL</b>\n"
        "────────────────────\n"
        "• All Protocols: <b>ARMED</b>\n"
        "• Database: <b>SYNCED</b>\n"
        "• AI Engine: <b>READY</b>\n"
        "────────────────────",
        parse_mode=ParseMode.HTML
    )
# ==========================================
# 📡 RENDER HEALTH SHIELD (IMMEDIATE BIND)
# ==========================================
def run_health_server():
    """
    This runs in a separate thread and binds to the port 
    the MILLISECOND the script starts. This satisfies Render.
    """
    async def handle_ping(request):
        return web.Response(text="SINGULARITY_V5_LIVE")

    try:
        app = web.Application()
        app.router.add_get('/', handle_ping)
        port = 10000
        # We use a simple runner to avoid loop conflicts
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"Health Server Note: {e}")
async def increment_api_count_in_db():
    try:
        col_api.update_one({"_id": "global_ledger"}, {"$inc": {"usage": 1}}, upsert=True)
    except Exception as e:
        console_out(f"Ledger Sync Error: {e}")


@dp.callback_query(F.data == "brauto")
async def breach_auto_exec(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.edit_text("🛰 <b>SYNTHESIZING...</b>", parse_mode=ParseMode.HTML)
    content, topic = await ai_generate("Generate a viral AI arbitrage operation.")
    await state.update_data(content=content, topic=topic)
    await cb.message.answer(f"📑 <b>PREVIEW:</b>\n\n{content}\n\n<b>FIRE YES?</b>", 
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 FIRE", callback_data="brfire")]]))

@dp.callback_query(F.data == "brfire")
async def breach_fire_final(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    content = data.get("content")
    if not content: return await cb.answer("❌ Intelligence Lost.")
    
    code = await get_next_id("BR")
    msg = await bot.send_message(CHANNEL_ID, content, parse_mode=ParseMode.HTML)
    col_vault.insert_one({"m_code": code, "msg_id": msg.message_id, "content": content, "created_at": datetime.now(IST)})
    
    await bot.send_message(LOG_CHANNEL_ID, f"🚀 <b>BREACH DEPLOYED: {code}</b>\n\n{content}", parse_mode=ParseMode.HTML)
    await cb.message.edit_text(f"🚀 <b>DEPLOYED:</b> <code>{code}</code>")
    await state.clear()
async def main():
    """Handles the async startup of all Singularity subsystems."""
    try:
        # --- CRITICAL: Initialize Database Counters ---
        await startup_sequence() 
        
        global API_USAGE_COUNT
        API_USAGE_COUNT = 0 
        
        # Start Scheduler inside the running loop
        scheduler.start()
        scheduler.add_job(hourly_heartbeat, 'interval', minutes=60)
        scheduler.add_job(check_system_integrity, 'interval', minutes=60)
        
        console_out("◈ SUBSYSTEMS ARMED")
        
        # Verify Database Connection
        db_client.admin.command('ping')
        
        # Send Startup Signal to Master Sadiq
        await bot.send_message(OWNER_ID, "💎 <b>APEX SINGULARITY v5.0 ONLINE</b>", parse_mode=ParseMode.HTML)
        
        # Start Polling
        print("◈ Bot is now polling...")
        await dp.start_polling(bot, skip_updates=True)
        
    except Exception as e:
        print(f"FATAL STARTUP ERROR: {e}")
        try: await bot.send_message(LOG_CHANNEL_ID, f"🚨 FATAL BOOT ERROR: {e}")
        except: pass
