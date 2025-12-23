import asyncio
import logging
import random
import html
import threading
import time
import sys
from aiohttp import web
import pymongo
import os
import io
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, CommandObject, ChatMemberUpdatedFilter, LEAVE_TRANSITION, JOIN_TRANSITION, Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile, ChatMemberUpdated
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramConflictError, TelegramForbiddenError

# ==========================================
# ⚡ MSANODE CONFIGURATION (ENVIRONMENT ONLY)
# ==========================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
ADMIN_LOG_CHANNEL = os.getenv("ADMIN_LOG_CHANNEL")

# Pull IDs as Integers safely
try:
    OWNER_ID = int(os.getenv("OWNER_ID", 0))
    CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
except (TypeError, ValueError):
    OWNER_ID = 0
    CHANNEL_ID = 0

# Links & Branding
CHANNEL_LINK = os.getenv("CHANNEL_LINK") 
BOT_USERNAME = os.getenv("BOT_USERNAME")
YOUTUBE_LINK = os.getenv("YOUTUBE_LINK") 
INSTAGRAM_LINK = os.getenv("INSTAGRAM_LINK") 

if not BOT_TOKEN or not MONGO_URI or not OWNER_ID:
    print("❌ CRITICAL ERROR: Mandatory Environment Variables (BOT_TOKEN, MONGO_URI, OWNER_ID) missing!")
    sys.exit(1)

# 🧠 PSYCHOLOGY: MSANODE Alpha Titles
CLICKBAIT_TITLES = [
    "🔥 **This Strategy is Breaking the Internet**",
    "🚀 **How to 10x Your Results Overnight**",
    "💰 **Watch This Before It Gets Deleted**",
    "🧠 **The 1% Are Hiding This From You**",
    "🤫 **The Secret Loophole Nobody Talks About**",
    "⚠️ **URGENT: This Changes Everything**",
    "💀 **Why Most People Fail (Don't Be Them)**",
    "💎 **Found: The 'Cheat Code' for Success**",
    "🤯 **I Can't Believe This Actually Works**",
    "📉 **Is This The End of Traditional Methods?**",
    "🔓 **Unlocking The Forbidden Strategy**",
    "⚡ **From 0 to 100: The Fast Track**",
    "🕵️ **Leaked: What The Pros Are Using**",
    "🔮 **Predicting The Next Big Trend**",
    "💸 **Passive Income: The Real Truth**",
    "🛠️ **The Tool That Replaces Hard Work**",
    "🚫 **Don't Ignore This Warning**",
    "👑 **Become The Authority In Your Niche**",
    "🌪️ **This Will Disrupt The Entire Industry**",
    "🎯 **The Exact Blueprint I Used**"
]

# 🧠 PSYCHOLOGY: MSANODE Affiliate Triggers
AFFILIATE_TRIGGERS = [
    "🤖 **NEW AI ALERT:** This tool is going viral right now.",
    "⚠️ **URGENT:** 90% of people are missing this opportunity.",
    "🎁 **SURPRISE BONUS:** We unlocked a secret tool for you.",
    "🔥 **HIGH DEMAND:** Automate the hard work. Try it free.",
    "⚡ **SPEED RUN:** Want results faster? Use this.",
    "💎 **HIDDEN GEM:** Top creators use this quietly.",
    "🚀 **BOOST:** Give yourself an unfair advantage.",
    "🤫 **CONFIDENTIAL:** I shouldn't be sharing this.",
    "⏳ **LIMITED TIME:** This offer might expire soon.",
    "💡 **SMART MOVE:** Work smarter, not harder.",
    "🔑 **ACCESS GRANTED:** Your private invite is here.",
    "📈 **GROWTH HACK:** The shortcut you've been looking for.",
    "🏆 **WINNER'S CIRCLE:** Join the elite users.",
    "🛑 **STOP WAITING:** Start seeing results today.",
    "👀 **SNEAK PEEK:** See what the hype is about.",
    "🧪 **PROVEN:** Tested and verified results.",
    "🌪️ **GAME CHANGER:** This disrupts everything.",
    "🛡️ **SECURE:** The safe way to scale up.",
    "🎯 **PRECISE:** Hit your goals with AI precision.",
    "💰 **PROFITABLE:** The ROI on this is insane."
]

# ==========================================
# 📝 FSM STATES (ADMIN FLOWS)
# ==========================================
class VaultState(StatesGroup):
    waiting_code = State()
    waiting_pdf = State()
    waiting_aff_link = State()
    waiting_aff_text = State()

class BroadcastState(StatesGroup):
    waiting_msg = State()

# ==========================================
# 🛠 SYSTEM SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# --- RENDER PORT BINDER ---
async def handle_health(request):
    return web.Response(text="MSANODE SUPREME COMMANDER IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")

# --- MONGODB CONNECTION ---
print("🔄 Connecting to MSANODE Database...")
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["MSANodeDB"]
    col_users = db["user_logs"]
    col_active = db["active_content"]
    col_viral = db["viral_videos"]
    col_reels = db["viral_reels"]
    col_settings = db["settings"] 
    col_banned = db["banned_users"] 
    print(f"✅ SUCCESSFULLY CONNECTED TO MSANODE MONGODB")
except Exception as e:
    print(f"❌ CRITICAL CONNECTION ERROR: {e}")
    sys.exit(1)

# --- HELPERS ---

async def send_admin_report(text: str):
    """Sends real-time logs to the private MSANODE Admin Channel."""
    if ADMIN_LOG_CHANNEL:
        try:
            await bot.send_message(
                ADMIN_LOG_CHANNEL, 
                f"📡 **MSANODE LIVE REPORT**\n────────────────────\n{text}", 
                parse_mode="Markdown"
            )
        except Exception as e:
            print(f"⚠️ Admin Report Failed: {e}")

async def check_maintenance():
    try:
        setting = col_settings.find_one({"setting": "maintenance"})
        if setting and setting.get("value") == True:
            return True
    except: pass
    return False

async def is_banned(user_id):
    try:
        user = col_banned.find_one({"user_id": str(user_id)})
        return user is not None
    except: return False

async def log_user(user: types.User, source: str):
    """Detailed Psychological Logging for MSANODE."""
    now_str = datetime.now().strftime("%d-%m-%Y %I:%M %p")
    user_id = str(user.id)
    username = f"@{user.username}" if user.username else "None"
    
    entry = {
        "first_name": user.first_name,
        "username": username,
        "user_id": user_id,
        "last_active": now_str,
        "status": "Active"
    }

    try:
        existing = col_users.find_one({"user_id": user_id})
        if not existing:
            entry["joined_date"] = now_str
            entry["interaction_count"] = 1
            entry["source"] = source
            col_users.insert_one(entry)
            await send_admin_report(f"👤 **NEW RECRUIT**\n**Name:** {user.first_name}\n**User:** {username}\n**ID:** `{user_id}`\n**Source:** {source}\n**Time:** {now_str}")
        else:
            update_fields = {"last_active": now_str, "status": "Active"}
            if existing.get("source") in ["Unknown", None, "Direct"]:
                update_fields["source"] = source
                
            col_users.update_one({"user_id": user_id}, {"$set": update_fields, "$inc": {"interaction_count": 1}})
    except Exception as e: 
        print(f"❌ LOG ERROR: {e}")

async def get_content(code: str):
    try:
        doc = col_active.find_one({"code": code.upper()})
        if doc:
            aff_text = doc.get("aff_text")
            if not aff_text or len(aff_text) < 5:
                aff_text = random.choice(AFFILIATE_TRIGGERS)
            return {"main_link": doc.get("pdf_link"), "aff_link": doc.get("aff_link"), "aff_text": aff_text}
    except: return None
    return None

# ==========================================
# 📢 ADMIN: BROADCAST SYSTEM
# ==========================================

@dp.message(Command("broadcast"))
async def start_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await message.answer("📣 **MSANODE BROADCAST SYSTEM**\n\nSend the message (Text/Photo/Video) you want to blast to all users in the database:")
    await state.set_state(BroadcastState.waiting_msg)

@dp.message(BroadcastState.waiting_msg)
async def perform_broadcast(message: types.Message, state: FSMContext):
    users = list(col_users.find({}, {"user_id": 1}))
    count, blocked = 0, 0
    await message.answer(f"🚀 **Targeting {len(users)} users... Stand by.**")
    
    for u in users:
        try:
            await bot.copy_message(chat_id=u['user_id'], from_chat_id=message.chat.id, message_id=message.message_id)
            count += 1
            await asyncio.sleep(0.05) 
        except (TelegramForbiddenError, TelegramBadRequest):
            blocked += 1
            col_users.update_one({"user_id": u['user_id']}, {"$set": {"status": "BLOCKED"}})
            
    await message.answer(f"✅ **Mission Complete**\n────────────────────\n📦 **Delivered:** {count}\n🚫 **Blocked/Dead:** {blocked}")
    await send_admin_report(f"📢 **BROADCAST SENT**\n**Total Target:** {len(users)}\n**Success:** {count}\n**Failure:** {blocked}")
    await state.clear()

# ==========================================
# 🔑 ADMIN: VAULT MANAGEMENT (ADD)
# ==========================================

@dp.message(Command("add"), StateFilter("*"))
async def add_start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer("🛠 **MSANODE VAULT ADD PROTOCOL**\n────────────────────\n📥 **Step 1:** Enter the **M-Code** (e.g. M101):")
    await state.set_state(VaultState.waiting_code)

@dp.message(VaultState.waiting_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.upper().strip()
    await state.update_data(code=code)
    await message.answer(f"✅ Code `{code}` Locked.\n────────────────────\n🔗 **Step 2:** Paste the **PDF Link**:")
    await state.set_state(VaultState.waiting_pdf)

@dp.message(VaultState.waiting_pdf)
async def process_pdf(message: types.Message, state: FSMContext):
    await state.update_data(pdf_link=message.text.strip())
    await message.answer("💸 **Step 3:** Paste the **Affiliate Link** (or type 'none'):")
    await state.set_state(VaultState.waiting_aff_link)

@dp.message(VaultState.waiting_aff_link)
async def process_aff(message: types.Message, state: FSMContext):
    await state.update_data(aff_link=message.text.strip())
    await message.answer("📝 **Step 4:** Enter the **Affiliate CTA Text** (Psychology trigger):")
    await state.set_state(VaultState.waiting_aff_text)

@dp.message(VaultState.waiting_aff_text)
async def process_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    aff_link = data['aff_link'] if data['aff_link'].lower() != 'none' else ""
    
    col_active.update_one(
        {"code": data['code']},
        {"$set": {
            "pdf_link": data['pdf_link'],
            "aff_link": aff_link,
            "aff_text": message.text.strip(),
            "created_at": datetime.now()
        }}, upsert=True
    )
    await message.answer(f"🚀 **MSANODE VAULT UPDATED**\nCode `{data['code']}` is now LIVE and active.")
    await state.clear()

# ==========================================
# 🚨 WATCHDOG: PSYCHOLOGICAL RETENTION
# ==========================================

@dp.chat_member(ChatMemberUpdatedFilter(LEAVE_TRANSITION))
async def on_user_leave(event: ChatMemberUpdated):
    if event.chat.id != CHANNEL_ID: return
    user = event.new_chat_member.user
    await log_user(user, "LEFT_CHANNEL")
    await send_admin_report(f"📉 **USER LEFT VAULT**\n**Name:** {user.first_name}\n**ID:** `{user.id}`")
    try:
        await bot.send_message(user.id, f"⚠️ **Wait, {user.first_name}...**\n\nYou just disconnected from the MSANODE Alpha Vault. Most people quit right before the breakthrough. Don't be 'most people'.", 
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Re-Join The 1%", url=CHANNEL_LINK)]]))
    except: pass

@dp.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def on_user_join(event: ChatMemberUpdated):
    if event.chat.id != CHANNEL_ID: return
    user = event.new_chat_member.user
    await log_user(user, "JOINED_CHANNEL")
    await send_admin_report(f"📈 **USER JOINED VAULT**\n**Name:** {user.first_name}\n**ID:** `{user.id}`")
    try: await bot.send_message(user.id, f"🤝 **Welcome back to MSANODE, {user.first_name}.**\n\nYour commitment to the grind is noted. The vault is open.")
    except: pass

# ==========================================
# 🤖 BOT LOGIC: THE MSANODE FLOW
# ==========================================

@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    if await is_banned(message.from_user.id): return
    if await check_maintenance():
        await message.answer("🚧 **UPGRADING MSANODE...** Give us 5 minutes.")
        return 

    raw_arg = command.args
    source = "Direct" 
    payload = None

    if raw_arg:
        if raw_arg.startswith("ig_"): source = "Instagram"; payload = raw_arg.replace("ig_", "")
        elif raw_arg.startswith("yt_"): source = "YouTube"; payload = raw_arg.replace("yt_", "")
        else: payload = raw_arg
    
    await log_user(message.from_user, source)

    # --- GATEKEEPER: CROSS PROMOTION LOGIC ---
    try:
        user_status = await bot.get_chat_member(CHANNEL_ID, message.from_user.id)
        if user_status.status in ['left', 'kicked', 'restricted']: raise Exception("Not Member")
    except Exception:
        kb = InlineKeyboardBuilder()
        # Cross Promo: If from IG, force YT. If from YT, force IG.
        if source == "Instagram":
            kb.row(InlineKeyboardButton(text="🔴 Subscribe on YouTube", url=YOUTUBE_LINK))
            kb.row(InlineKeyboardButton(text="🚀 Join MSANODE Telegram", url=CHANNEL_LINK))
        else:
            kb.row(InlineKeyboardButton(text="📸 Follow on Instagram", url=INSTAGRAM_LINK))
            kb.row(InlineKeyboardButton(text="🚀 Join MSANODE Telegram", url=CHANNEL_LINK))
            
        kb.row(InlineKeyboardButton(text="✅ I HAVE JOINED BOTH", callback_data=f"check_{raw_arg or 'none'}"))
        
        await message.answer(
            f"**Yo {message.from_user.first_name}! Welcome to MSANODE.** 👋\n\nTo unlock my private blueprints, you must be part of the community on all platforms. Commitment is the price of success.",
            reply_markup=kb.as_markup()
        )
        return

    if not payload:
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🔴 YouTube Channel", url=YOUTUBE_LINK), InlineKeyboardButton(text="📸 Instagram Page", url=INSTAGRAM_LINK))
        await message.answer(f"**Welcome to the MSANODE Hub, {message.from_user.first_name}! 👋**\n\nYou are in. To get a specific guide, use the M-Code from my latest video.", reply_markup=kb.as_markup())
        return

    await deliver_content(message, payload, source)

@dp.callback_query(F.data.startswith("check_"))
async def check_join(callback: types.CallbackQuery):
    raw_arg = callback.data.split("_", 1)[1]
    try:
        user_status = await bot.get_chat_member(CHANNEL_ID, callback.from_user.id)
        if user_status.status in ['left', 'kicked', 'restricted']:
            await callback.answer("❌ You haven't joined the MSANODE channel yet!", show_alert=True)
            return
        
        try: await callback.message.delete()
        except: pass
        
        if raw_arg != "none":
            source = "Instagram" if raw_arg.startswith("ig_") else "YouTube"
            payload = raw_arg.replace("ig_", "").replace("yt_", "")
            await deliver_content(callback.message, payload, source)
        else:
            await callback.message.answer("✅ **Access Restored.** Welcome back to MSANODE.")
    except:
        await callback.answer("❌ Verification failed. Join the channel first!", show_alert=True)

async def deliver_content(message: types.Message, payload: str, source: str):
    data = await get_content(payload)
    name = message.chat.first_name if message.chat.first_name else "User"
    
    if not data: 
        await message.answer(f"❌ **Error:** Code `{payload}` not found in the MSANODE Data Core.")
        return
    
    # 1. THE PDF DELIVERY
    await message.answer(f"**Access Granted, {name}.** 🔓\n\nHere is your requested MSANODE blueprint:\n{data['main_link']}")
    await send_admin_report(f"📦 **BLUEPRINT DELIVERED**\n**User:** {name}\n**Code:** `{payload}`\n**Source:** {source}")

    # 2. THE PSYCHOLOGICAL AFFILIATE (DELAYED)
    if data['aff_link'] and len(data['aff_link']) > 5:
        await asyncio.sleep(1.5)
        kb_aff = InlineKeyboardBuilder().button(text="🚀 UNLOCK THE ENGINE", url=data['aff_link'])
        await message.answer(f"**Wait, {name}...** 🤫\n\n{data['aff_text']}", reply_markup=kb_aff.as_markup())
        await send_admin_report(f"💰 **AFFILIATE LINK CLICKED**\n**User:** {name}\n**Link:** {data['aff_link']}")

    # 3. THE CROSS-PLATFORM PSYCHOLOGY
    await asyncio.sleep(1.5)
    if source == "Instagram":
        pipeline = [{"$sample": {"size": 1}}]
        video = list(col_viral.aggregate(pipeline))
        if video:
            kb_cross = InlineKeyboardBuilder().button(text="▶️ WATCH FULL STRATEGY", url=video[0]['link'])
            await message.answer(f"🔥 **Deep Dive Needed?**\n\nI just dropped a breakdown on YouTube:\n{video[0].get('desc', 'Check this out!')}", reply_markup=kb_cross.as_markup())
    else: 
        pipeline = [{"$sample": {"size": 1}}]
        reel = list(col_reels.aggregate(pipeline))
        if reel:
            kb_cross = InlineKeyboardBuilder().button(text="📸 WATCH QUICK HACK", url=reel[0]['link'])
            await message.answer(f"⚡ **Need it in 60s?**\n\nCheck the fast version on Instagram:\n{reel[0].get('desc', 'Check this out!')}", reply_markup=kb_cross.as_markup())

# ==========================================
# 🚀 THE SUPREME RESTART
# ==========================================

async def main():
    print(f"✅ MSANODE Bot Online...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    while True:
        try:
            asyncio.run(main())
        except TelegramConflictError:
            time.sleep(5)
        except Exception as e:
            print(f"⚠️ System Error: {e}")
            time.sleep(10)
