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
    print("❌ CRITICAL ERROR: Environment variables missing!")
    sys.exit(1)

IST = pytz.timezone('Asia/Kolkata')

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

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# --- RENDER PORT BINDER ---
async def handle_health(request):
    return web.Response(text="MSANODE GATEWAY HUB IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")

# --- MONGODB CONNECTION ---
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
# 🛡️ INTELLIGENCE HELPERS (UNREDUCED)
# ==========================================

async def send_admin_report(text: str):
    """Sends one-time detailed dossier to private channel."""
    if ADMIN_LOG_CHANNEL:
        try:
            await bot.send_message(ADMIN_LOG_CHANNEL, f"📡 **MSANODE INTELLIGENCE DOSSIER**\n────────────────────\n{text}", parse_mode="Markdown")
        except: pass

async def is_member(user_id):
    """Verifies user is inside MSANode Telegram Channel."""
    try:
        status = await bot.get_chat_member(CHANNEL_ID, user_id)
        return status.status in ['member', 'administrator', 'creator']
    except: return False

async def log_user(user: types.User, source: str):
    """Identity Engine: Returns NEW or RETURNING with IST format."""
    now_str = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
    u_id = str(user.id)
    u_name = f"@{user.username}" if user.username else "None"
    
    try:
        existing = col_users.find_one({"user_id": u_id})
        if not existing:
            col_users.insert_one({
                "first_name": user.first_name,
                "username": u_name,
                "user_id": u_id,
                "last_active": now_str,
                "joined_date": now_str,
                "source": source,
                "status": "Active",
                "has_reported": False 
            })
            return "NEW"
        else:
            upd = {"last_active": now_str, "status": "Active"}
            if existing.get("source") == "Unknown" and source != "Unknown": upd["source"] = source
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
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Re-Join MSANode", url=CHANNEL_LINK)]])
        # Corrected Indentation & Syntax
        await bot.send_message(
            user.id, 
            f"⚠️ **Wait, {user.first_name}...**\n\nYou left the MSANode VAULT. Access to blueprints is now locked. Kindly Re-Join To MSANODE VAULT Membership To Unlock!!!", 
            reply_markup=kb
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
        await bot.send_message(
            user.id, 
            f"🤝 **Membership Restored, {user.first_name}.** Welcome Back To The MSANODE Family"
        )
    except Exception: 
        pass

# ==========================================
# 🤖 BOT LOGIC: MSANODE INTELLIGENCE HUB
# ==========================================

@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    # 1. Protection Checks
    if col_banned.find_one({"user_id": str(message.from_user.id)}): return
    maint = col_settings.find_one({"setting": "maintenance"})
    if maint and maint.get("value"):
        await message.answer("🚧 **UPGRADING IN PROCESS...** We will be back soon.")
        return 

    # 2. Parse Arg/Source & Log
    raw_arg = command.args
    source = "Unknown"; payload = None
    if raw_arg:
        if raw_arg.startswith("ig_"): source = "Instagram"; payload = raw_arg.replace("ig_", "")
        elif raw_arg.startswith("yt_"): source = "YouTube"; payload = raw_arg.replace("yt_", "")
        else: payload = raw_arg
    
    u_status = await log_user(message.from_user, source)

    # 3. VERIFICATION ANIMATION
    load = await message.answer("🔍 **Verifying Clearance...**")
    await asyncio.sleep(0.7)
    await load.edit_text("🛰 **Accessing MSANode Data Core...**")
    await asyncio.sleep(0.5)

    # 4. Membership Gate
    if not await is_member(message.from_user.id):
        await load.delete()
        kb = InlineKeyboardBuilder()
        if source == "YouTube": kb.row(InlineKeyboardButton(text="📸 Follow Instagram", url=INSTAGRAM_LINK))
        else: kb.row(InlineKeyboardButton(text="🔴 Subscribe YouTube", url=YOUTUBE_LINK))
        kb.row(InlineKeyboardButton(text="🚀 Join MSANode Telegram", url=CHANNEL_LINK))
        kb.row(InlineKeyboardButton(text="✅ I HAVE JOINED ALL", callback_data=f"check_{raw_arg or 'none'}"))
        await message.answer(f"**Verification Required, {message.from_user.first_name}.** ✋\n\nTo unlock blueprints, you must unlock ** MEMBER ** of the MSANode Family click below now :", reply_markup=kb.as_markup())
        return

    # 5. Core Delivery Logic
    await load.delete()
    if payload:
        # User requested a specific M-Code
        if u_status == "NEW":
            await message.answer(f"**Welcome Recruit, {message.from_user.first_name}!** 👋\n\nMembership established. You are part of MSANODE VAULT now. Executing payload...")
        else:
            await message.answer(f"**Clearance Verified, {message.from_user.first_name}.** ✅\n\nWelcome back to the MSANODE Vault. Loading blueprints...")
        await deliver_content(message, payload, source)
    else:
        # Returning user or just started without M-Code
        kb = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔴 YouTube", url=YOUTUBE_LINK), InlineKeyboardButton(text="📸 Instagram", url=INSTAGRAM_LINK))
        await message.answer(
            f"**OPERATIVE {message.from_user.first_name}, YOU ARE IN THE VAULT.** ✅\n\n"
            f"You are already registered. To unlock more guides and direct links , use the links from the **PINNED COMMENTS** of my latest content. visit it now",
            reply_markup=kb.as_markup()
        )

@dp.callback_query(F.data.startswith("check_"))
async def check_join(callback: types.CallbackQuery):
    raw_arg = callback.data.split("_", 1)[1]
    if not await is_member(callback.from_user.id):
        await callback.answer("❌ Verification Failed: Membership not detected.", show_alert=True)
        return
    try: await callback.message.delete()
    except: pass
    await callback.message.answer(f"**Clearance Granted.** ✅")
    if raw_arg != "none":
        src = "Instagram" if raw_arg.startswith("ig_") else "YouTube"
        await deliver_content(callback.message, raw_arg.replace("ig_", "").replace("yt_", ""), src)
    else:
        await callback.message.answer("✅ **Access Restored.** Check pinned comments & for new **LINKS**.")

async def deliver_content(message: types.Message, payload: str, source: str):
    data = await get_content(payload)
    u_id = str(message.chat.id)
    name = message.chat.first_name or "Operative"
    u_name = f"@{message.chat.username}" if message.chat.username else "None"
    
    if not data: 
        await message.answer(f"❌ **Error:** Code `{payload}` invalid.")
        return
    
    # --- INTELLIGENCE DOSSIER (ONE-TIME REPORT) ---
    doc = col_users.find_one({"user_id": u_id})
    if doc and not doc.get("has_reported", False):
        rep_time = datetime.now(IST).strftime("%d-%m-%Y %I:%M %p")
        dossier = (
            f"👤 **NEW RECRUIT CAPTURED**\n"
            f"**Name:** {name}\n"
            f"**User:** {u_name}\n"
            f"**ID:** `{u_id}`\n"
            f"**Source:** {source}\n"
            f"**M-Code:** `{payload}`\n"
            f"**PDF:** {data['main_link']}\n"
            f"**Time:** {rep_time}"
        )
        await send_admin_report(dossier)
        col_users.update_one({"user_id": u_id}, {"$set": {"has_reported": True}})

    # 1. BLUEPRINT DELIVERY
    await message.answer(f"**Transmission Successful.** 🔓\n\nBlueprint ready:\n{data['main_link']}")

    # 2. AFFILIATE (1.5s DELAY)
    if data['aff_link'] and len(data['aff_link']) > 5:
        await asyncio.sleep(1.5)
        kb_aff = InlineKeyboardBuilder().button(text="🚀 UNLOCK ENGINE", url=data['aff_link']).as_markup()
        await message.answer(f"🤫 **One more tool for the army...**\n\n{data['aff_text']}", reply_markup=kb_aff)

    # 3. CROSS-SYNC ENGINE (1.5s DELAY + ALPHA TITLES)
    await asyncio.sleep(1.5)
    title = random.choice(ALPHA_TITLES)
    
    if source == "YouTube":
        reel = list(col_reels.aggregate([{"$sample": {"size": 1}}]))
        kb = InlineKeyboardBuilder()
        msg = f"⚡ **Maximize Edge, {name}.**\n\nYou've seen the deep strategy, but I drop daily automation on Instagram. Join elite there:"
        if reel:
            msg += f"\n\n{title}\n{reel[0].get('desc', 'Check Daily Alpha')}"
            kb.row(InlineKeyboardButton(text="📸 WATCH MORE", url=reel[0]['link']))
        else: kb.row(InlineKeyboardButton(text="📸 FOLLOW INSTAGRAM", url=INSTAGRAM_LINK))
        kb.row(InlineKeyboardButton(text="▶️ STAY TUNED ON YOUTUBE", url=YOUTUBE_LINK))
        await message.answer(msg, reply_markup=kb.as_markup())
    else:
        video = list(col_viral.aggregate([{"$sample": {"size": 1}}]))
        kb = InlineKeyboardBuilder()
        msg = f"🔥 **Go Beyond the Surface, {name}.**\n\nInstagram is for speed, but YouTube is for real money. I just dropped a breakdown you can't miss:"
        if video:
            msg += f"\n\n{title}\n{video[0].get('desc', 'Full Strategy Reveal')}"
            kb.row(InlineKeyboardButton(text="▶️ WATCH FULL STRATEGY", url=video[0]['link']))
        else: kb.row(InlineKeyboardButton(text="▶️ SUBSCRIBE YOUTUBE", url=YOUTUBE_LINK))
        kb.row(InlineKeyboardButton(text="📸 STAY TUNED ON INSTA", url=INSTAGRAM_LINK))
        await message.answer(msg, reply_markup=kb.as_markup())

# ==========================================
# 🚀 NUCLEAR SHIELD
# ==========================================
async def main():
    try: await bot.delete_webhook(drop_pending_updates=True)
    except: pass
    print(f"✅ MSANODE GATEWAY ONLINE.")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    while True:
        try: asyncio.run(main())
        except TelegramConflictError:
            print("💀 GHOST DETECTED! Waiting 20s...")
            time.sleep(20)
        except Exception as e:
            print(f"⚠️ Error: {e}")
            time.sleep(15)


