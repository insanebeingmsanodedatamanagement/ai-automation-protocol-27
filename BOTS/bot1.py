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
import pytz
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
# ⚡ CONFIGURATION (GHOST PROTOCOL)
# ==========================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
ADMIN_LOG_CHANNEL = os.getenv("ADMIN_LOG_CHANNEL")

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
    print("❌ CRITICAL ERROR: Mandatory Environment Variables missing!")
    sys.exit(1)

# Timezone Synchronization
IST = pytz.timezone('Asia/Kolkata')

# 🧠 PSYCHOLOGY: Affiliate Triggers (Fallback)
AFFILIATE_TRIGGERS = [
    "🤖 **NEW AI ALERT:** This tool is going viral right now.",
    "⚠️ **URGENT:** 90% of people are missing this opportunity.",
    "🎁 **SURPRISE BONUS:** We unlocked a secret tool for you.",
    "🔥 **HIGH DEMAND:** Automate the hard work. Try it free.",
    "⚡ **SPEED RUN:** Want results faster? Use this.",
    "📈 **GROWTH HACK:** The shortcut you've been looking for."
]

# ==========================================
# 🛠 SYSTEM SETUP (IRON DOME)
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# --- RENDER PORT BINDER (HEALTH SHIELD) ---
async def handle_health(request):
    return web.Response(text="MSANODE GATEWAY IS ONLINE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")

# --- MONGODB CONNECTION ---
print("🔄 Connecting Gateway to MSANode Database...")
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["MSANodeDB"]
    col_users = db["user_logs"]
    col_active = db["active_content"]
    col_viral = db["viral_videos"]
    col_reels = db["viral_reels"]
    col_settings = db["settings"] 
    col_banned = db["banned_users"] 
    print(f"✅ GATEWAY DATA CORE: CONNECTED")
except Exception as e:
    print(f"❌ DATABASE OFFLINE: {e}")
    sys.exit(1)

# ==========================================
# 🛡️ GATEWAY INTELLIGENCE HELPERS
# ==========================================

async def send_admin_report(text: str):
    """Sends one-time detailed dossier to the private MSANode Admin channel."""
    if ADMIN_LOG_CHANNEL:
        try:
            await bot.send_message(
                ADMIN_LOG_CHANNEL, 
                f"📡 **MSANODE INTELLIGENCE DOSSIER**\n────────────────────\n{text}", 
                parse_mode="Markdown"
            )
        except Exception as e:
            print(f"⚠️ Intel Report Failed: {e}")

async def check_maintenance():
    try:
        setting = col_settings.find_one({"setting": "maintenance"})
        return setting and setting.get("value") == True
    except: return False

async def is_banned(user_id):
    try:
        return col_banned.find_one({"user_id": str(user_id)}) is not None
    except: return False

async def is_member(user_id):
    """Strictly verifies if user is inside the MSANode Telegram."""
    try:
        status = await bot.get_chat_member(CHANNEL_ID, user_id)
        return status.status in ['member', 'administrator', 'creator']
    except: return False

async def log_user(user: types.User, source: str):
    """Handles Identity Management and Silent Updates."""
    # Master Sadiq Format: DD-MM-YYYY 11:00 PM
    now_str = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
    user_id = str(user.id)
    username = f"@{user.username}" if user.username else "None"
    
    try:
        existing = col_users.find_one({"user_id": user_id})
        if not existing:
            col_users.insert_one({
                "first_name": user.first_name,
                "username": username,
                "user_id": user_id,
                "last_active": now_str,
                "joined_date": now_str,
                "source": source,
                "status": "Active",
                "has_reported": False # Dossier flag
            })
            return "NEW"
        else:
            # Silent Database Update for Returning Operatives
            update_fields = {"last_active": now_str, "status": "Active"}
            # Upgrade source if it was Unknown but now is YT/IG
            if existing.get("source") == "Unknown" and source != "Unknown":
                update_fields["source"] = source
            col_users.update_one({"user_id": user_id}, {"$set": update_fields})
            return "RETURNING"
    except Exception as e: 
        print(f"❌ LOG ERROR: {e}")
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
    col_users.update_one({"user_id": str(user.id)}, {"$set": {"status": "LEFT"}})
    try:
        await bot.send_message(user.id, f"⚠️ **Connection Broken, {user.first_name}...**\n\nYou left the MSANode Core. Access is now locked. Re-join to continue.", 
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Re-Join MSANode", url=CHANNEL_LINK)]]))
    except: pass

@dp.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def on_user_join(event: ChatMemberUpdated):
    if event.chat.id != CHANNEL_ID: return
    user = event.new_chat_member.user
    col_users.update_one({"user_id": str(user.id)}, {"$set": {"status": "Active"}})
    try: await bot.send_message(user.id, f"🤝 **Clearance Restored, {user.first_name}.**")
    except: pass

# ==========================================
# 🤖 BOT LOGIC: THE MSANODE GATEWAY
# ==========================================

@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    if await is_banned(message.from_user.id): return
    if await check_maintenance():
        await message.answer("🚧 **UPGRADING CORE...** We will be back soon.")
        return 

    raw_arg = command.args
    source = "Unknown"; payload = None
    if raw_arg:
        if raw_arg.startswith("ig_"): source = "Instagram"; payload = raw_arg.replace("ig_", "")
        elif raw_arg.startswith("yt_"): source = "YouTube"; payload = raw_arg.replace("yt_", "")
        else: payload = raw_arg
    
    user_status = await log_user(message.from_user, source)

    # STRICT MEMBERSHIP CHECK
    if not await is_member(message.from_user.id):
        kb = InlineKeyboardBuilder()
        if source == "YouTube": kb.row(InlineKeyboardButton(text="📸 Follow on Instagram", url=INSTAGRAM_LINK))
        else: kb.row(InlineKeyboardButton(text="🔴 Subscribe on YouTube", url=YOUTUBE_LINK))
        kb.row(InlineKeyboardButton(text="🚀 Join MSANode Telegram", url=CHANNEL_LINK))
        kb.row(InlineKeyboardButton(text="✅ I HAVE JOINED ALL", callback_data=f"check_{raw_arg or 'none'}"))
        
        await message.answer(
            f"**Identity Verification Required, {message.from_user.first_name}.** ✋\n\nThe MSANode Data Core is reserved for active members. Synchronize below to unlock clearance:",
            reply_markup=kb.as_markup()
        )
        return

    # BRANDED PSYCHOLOGICAL WELCOME
    if user_status == "NEW":
        await message.answer(f"**Welcome Recruit, {message.from_user.first_name}.** 👋\n\nConnection established with MSANODE. You are successfully part of the family. Ready for execution?")
    else:
        await message.answer(f"**Clearance Verified, Operative {message.from_user.first_name}.** ✅\n\nWelcome back to the MSANODE Core. Re-syncing requested data...")

    if not payload:
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🔴 YouTube Channel", url=YOUTUBE_LINK), InlineKeyboardButton(text="📸 Instagram Page", url=INSTAGRAM_LINK))
        await message.answer(f"**HELLO! MSANODE AGENT ONLINE.**\n\nTo unlock a blueprint, use the link from the pinned comments of my latest content.", reply_markup=kb.as_markup())
        return

    await deliver_content(message, payload, source)

@dp.callback_query(F.data.startswith("check_"))
async def check_join(callback: types.CallbackQuery):
    raw_arg = callback.data.split("_", 1)[1]
    if not await is_member(callback.from_user.id):
        await callback.answer("❌ Error: Membership not detected in MSANode Vault.", show_alert=True)
        return

    try: await callback.message.delete()
    except: pass
    await callback.message.answer(f"**Clearance Granted, {callback.from_user.first_name}.** ✅")
    
    if raw_arg != "none":
        source = "Instagram" if raw_arg.startswith("ig_") else "YouTube"
        payload = raw_arg.replace("ig_", "").replace("yt_", "")
        await deliver_content(callback.message, payload, source)
    else:
        await callback.message.answer("✅ **Access Restored.** Welcome back.")

async def deliver_content(message: types.Message, payload: str, source: str):
    data = await get_content(payload)
    user_id = str(message.chat.id)
    name = message.chat.first_name or "Operative"
    username = f"@{message.chat.username}" if message.chat.username else "None"
    
    if not data: 
        await message.answer(f"❌ **Data Error:** Code `{payload}` is invalid or expired.")
        return
    
    # 1. 📂 THE DELIVERY
    await message.answer(f"**Transmission Successful.** 🔓\n\nYour requested MSANode Blueprint is ready:\n{data['main_link']}")

    # --- ONE-TIME ADMIN INTELLIGENCE DOSSIER ---
    # Trigger: If user exists in DB but hasn't been reported to the admin channel yet
    user_doc = col_users.find_one({"user_id": user_id})
    if user_doc and not user_doc.get("has_reported", False):
        report_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
        dossier = (
            f"👤 **NEW RECRUIT CAPTURED**\n"
            f"**Name:** {name}\n"
            f"**User:** {username}\n"
            f"**ID:** `{user_id}`\n"
            f"**Source:** {source}\n"
            f"**M-Code Used:** `{payload}`\n"
            f"**PDF Sent:** {data['main_link']}\n"
            f"**Affiliate Sent:** {data['aff_link'] if data['aff_link'] else 'None'}\n"
            f"**Time:** {report_time}"
        )
        await send_admin_report(dossier)
        col_users.update_one({"user_id": user_id}, {"$set": {"has_reported": True}})

    # 2. 💸 THE PSYCHOLOGICAL AFFILIATE (DELAYED)
    if data['aff_link'] and len(data['aff_link']) > 5:
        await asyncio.sleep(1.5)
        kb_aff = InlineKeyboardBuilder().button(text="🚀 UNLOCK THE ENGINE", url=data['aff_link'])
        await message.answer(f"🤫 **Wait, one more tool for the army...**\n\n{data['aff_text']}", reply_markup=kb_aff.as_markup())

    # 3. 🔄 THE CROSS-SYNC (INTELLIGENT GROWTH)
    await asyncio.sleep(1.5)
    if source == "YouTube":
        # Coming from YouTube? Suggest mobile/short-form alpha on IG
        reel = list(col_reels.aggregate([{"$sample": {"size": 1}}]))
        kb_sync = InlineKeyboardBuilder()
        msg = f"⚡ **Maximize Your Edge, {name}.**\n\nYou've seen the deep dive, but I drop daily automation hacks on Instagram. Check this out:"
        if reel: kb_sync.row(InlineKeyboardButton(text="📸 WATCH DAILY ALPHA", url=reel[0]['link']))
        else: kb_sync.row(InlineKeyboardButton(text="📸 FOLLOW INSTAGRAM", url=INSTAGRAM_LINK))
        kb_sync.row(InlineKeyboardButton(text="▶️ STAY TUNED ON YOUTUBE", url=YOUTUBE_LINK))
        await message.answer(msg, reply_markup=kb_sync.as_markup())
    else:
        # Coming from Instagram? Suggest deep strategy on YouTube
        video = list(col_viral.aggregate([{"$sample": {"size": 1}}]))
        kb_sync = InlineKeyboardBuilder()
        msg = f"🔥 **Go Beyond the Surface, {name}.**\n\nInstagram is for speed, but YouTube is for the real money. I just dropped a deep strategy breakdown:"
        if video: kb_sync.row(InlineKeyboardButton(text="▶️ WATCH FULL STRATEGY", url=video[0]['link']))
        else: kb_sync.row(InlineKeyboardButton(text="▶️ SUBSCRIBE YOUTUBE", url=YOUTUBE_LINK))
        kb_sync.row(InlineKeyboardButton(text="📸 STAY TUNED ON INSTA", url=INSTAGRAM_LINK))
        await message.answer(msg, reply_markup=kb_sync.as_markup())

# ==========================================
# 🚀 MSANODE NUCLEAR GHOST-KILLER RESTART
# ==========================================

async def main():
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        print("🛠 Purging sessions... MSANode Gateway Shield Active.")
        await asyncio.sleep(2)
    except Exception as e:
        print(f"⚠️ Webhook Purge Note: {e}")
    print(f"✅ MSANODE GATEWAY HUB ONLINE. Monitoring for Ghost Instances...")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    # Start the Health Server (Essential for Render Uptime)
    threading.Thread(target=run_health_server, daemon=True).start()
    
    while True:
        try:
            asyncio.run(main())
        except TelegramConflictError:
            # NUCLEAR OPTION: If conflict error 409, wait 20s to kill the competing bot
            print("💀 GHOST DETECTED! Waiting 20 seconds to force-kill the competing instance...")
            time.sleep(20)
        except Exception as e:
            print(f"⚠️ System Error: {e}")
            time.sleep(15)
