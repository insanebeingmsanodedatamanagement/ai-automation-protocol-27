import asyncio
import logging
import random
import threading
from aiohttp import web
import pymongo
import os
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, CommandObject, ChatMemberUpdatedFilter, LEAVE_TRANSITION, JOIN_TRANSITION
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# ==========================================
# ⚡ CONFIGURATION
# ==========================================
# ⚠️ REPLACE THESE WITH YOUR REAL KEYS
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

if not BOT_TOKEN or not MONGO_URI:
    print("❌ ERROR: Environment Variables not found! Check Render settings.")

CHANNEL_ID = -1003480585973 
CHANNEL_LINK = "https://t.me/msanode" 
YOUTUBE_LINK = "http://www.youtube.com/@msanode" 
INSTAGRAM_LINK = "https://www.instagram.com/msanode" 

# 🧠 PSYCHOLOGY: Titles for YouTube Videos (Fallback)
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

# 🧠 PSYCHOLOGY: Affiliate Triggers
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
# 🛠 SYSTEM SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
# --- RENDER PORT BINDER ---
async def handle_health(request):
    return web.Response(text="BOT 1 (HUMAN EDITION) IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")
# --- MONGODB CONNECTION ---
print("🔄 Connecting to Database...")
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["MSANodeDB"]
    
    # Collections
    col_users = db["user_logs"]
    col_active = db["active_content"]
    col_viral = db["viral_videos"]
    col_reels = db["viral_reels"]
    col_settings = db["settings"] # For Maintenance Mode
    col_banned = db["banned_users"] # For Ban System

    print(f"✅ SUCCESSFULLY CONNECTED TO MONGODB")
except Exception as e:
    print(f"❌ CRITICAL CONNECTION ERROR: {e}")
    exit()

# --- HELPERS ---

async def check_maintenance():
    """Checks MongoDB for maintenance flag."""
    try:
        # Check if a setting exists where maintenance is True
        setting = col_settings.find_one({"setting": "maintenance"})
        if setting and setting.get("value") == True:
            return True
    except: pass
    return False

async def is_banned(user_id):
    """Checks if user ID is in the ban list."""
    try:
        user = col_banned.find_one({"user_id": str(user_id)})
        return user is not None
    except: return False

async def log_user(user: types.User, source: str):
    """Logs user to MongoDB efficiently."""
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
        # 1. Check if user exists to handle "Joined Date" and "Source"
        existing = col_users.find_one({"user_id": user_id})
        
        if not existing:
            # NEW USER
            entry["joined_date"] = now_str
            entry["interaction_count"] = 1
            entry["source"] = source
            col_users.insert_one(entry)
            print(f"✅ NEW USER: {user.first_name}")
        else:
            # EXISTING USER - Update logs
            update_fields = {
                "last_active": now_str,
                "first_name": user.first_name, # Update name if changed
                "username": username,
                "status": "Active"
            }
            
            # Update source only if it was Unknown or direct channel join
            if existing.get("source") in ["Unknown", None] and "CHANNEL" not in source:
                update_fields["source"] = source
                
            col_users.update_one(
                {"user_id": user_id},
                {
                    "$set": update_fields,
                    "$inc": {"interaction_count": 1} # Increment count
                }
            )
            
            # Handle Leave/Join Status
            if source == "LEFT_CHANNEL":
                col_users.update_one({"user_id": user_id}, {"$set": {"status": "LEFT"}})
            elif source == "JOINED_CHANNEL":
                col_users.update_one({"user_id": user_id}, {"$set": {"status": "Active"}})

    except Exception as e: 
        print(f"❌ LOG ERROR: {e}")

async def get_content(code: str):
    """Fetches content from MongoDB."""
    try:
        # Instant lookup
        doc = col_active.find_one({"code": code})
        if doc:
            # Use specific affiliate text if saved, else random trigger
            aff_text = doc.get("aff_text")
            if not aff_text or len(aff_text) < 5:
                aff_text = random.choice(AFFILIATE_TRIGGERS)
                
            return {
                "main_link": doc.get("pdf_link"), 
                "aff_link": doc.get("aff_link"), 
                "aff_text": aff_text
            }
    except: return None
    return None

async def get_viral_video():
    """Gets a random YouTube video using MongoDB Aggregation."""
    try:
        # Get 1 random document
        pipeline = [{"$sample": {"size": 1}}]
        result = list(col_viral.aggregate(pipeline))
        
        if result:
            video = result[0]
            title = video.get("desc")
            if not title or len(title) < 5:
                title = random.choice(CLICKBAIT_TITLES)
            return {"link": video.get("link"), "title": title}
    except: return None
    return None

async def get_viral_reel():
    """Gets a random Insta Reel using MongoDB Aggregation."""
    try:
        pipeline = [{"$sample": {"size": 1}}]
        result = list(col_reels.aggregate(pipeline))
        
        if result:
            reel = result[0]
            desc = reel.get("desc")
            if not desc or len(desc) < 5:
                desc = "🔥 Watch this strategy!"
            return {"link": reel.get("link"), "desc": desc}
    except: return None
    return None

# ==========================================
# 🚨 WATCHDOG
# ==========================================
@dp.chat_member(ChatMemberUpdatedFilter(LEAVE_TRANSITION))
async def on_user_leave(event: ChatMemberUpdated):
    if event.chat.id != CHANNEL_ID: return
    user = event.new_chat_member.user
    await log_user(user, "LEFT_CHANNEL")
    try:
        await bot.send_message(user.id, f"⚠️ **Wait, {user.first_name}... are you leaving?**\n\nYou're walking away from the Vault. If you leave, you lose access to all future drops.\n\nDon't make that mistake.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Re-Join The Family", url=CHANNEL_LINK)]]))
    except: pass

@dp.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def on_user_join(event: ChatMemberUpdated):
    if event.chat.id != CHANNEL_ID: return
    user = event.new_chat_member.user
    await log_user(user, "JOINED_CHANNEL")
    try: await bot.send_message(user.id, f"🤝 **Smart move, {user.first_name}.**\n\nYou're back in the inner our private vault. Access granted.")
    except: pass

# ==========================================
# 🤖 BOT LOGIC
# ==========================================

@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    
    # 🚫 BAN CHECK (Priority 1)
    if await is_banned(message.from_user.id):
        await message.answer("🚫 **Access Denied**\n\nI'm sorry, but we had to block your access.")
        return

    # 🛑 MAINTENANCE CHECK (Priority 2)
    if await check_maintenance():
        await message.answer(
            "🚧 **UPGRADING THE VAULT...** 🚧\n\n"
            f"Yo {message.from_user.first_name}, I'm adding some new servers to handle the load.\n"
            "Give me 10 minutes. I promise it's worth the wait."
        )
        return 

    raw_arg = command.args
    source = "YouTube" 
    payload = None

    if raw_arg:
        if raw_arg.startswith("ig_"):
            source = "Instagram"
            payload = raw_arg.replace("ig_", "")
        elif raw_arg.startswith("yt_"):
            source = "YouTube"
            payload = raw_arg.replace("yt_", "")
        else:
            source = "YouTube"
            payload = raw_arg
    
    name = message.from_user.first_name

    # --- CHECK MEMBERSHIP ---
    try:
        user_status = await bot.get_chat_member(CHANNEL_ID, message.from_user.id)
        if user_status.status in ['left', 'kicked', 'restricted']: 
            raise Exception("Not Member")
    except Exception:
        kb = InlineKeyboardBuilder()
        if payload:
            if source == "Instagram":
                kb.row(InlineKeyboardButton(text="📸 Follow on Insta", url=INSTAGRAM_LINK))
                kb.row(InlineKeyboardButton(text="🔴 Sub on YouTube", url=YOUTUBE_LINK))
            else:
                kb.row(InlineKeyboardButton(text="🔴 Sub on YouTube", url=YOUTUBE_LINK))
                kb.row(InlineKeyboardButton(text="📸 Follow on Insta", url=INSTAGRAM_LINK))
            kb.row(InlineKeyboardButton(text="🚀 Join Telegram Family", url=CHANNEL_LINK))
            kb.row(InlineKeyboardButton(text="✅ I Have Joined", callback_data=f"check_{raw_arg}"))
            
            await message.answer(f"**Hold up, {name}!** ✋\n\nI want to give you this file, but you need to be in the family first.\n\n👇 **Join below, then click 'I Have Joined' so I can unlock it for you:**", reply_markup=kb.as_markup())
            return
        else:
            kb.row(InlineKeyboardButton(text="🚀 Re-Join Family Channel", url=CHANNEL_LINK))
            kb.row(InlineKeyboardButton(text="✅ Restore Access", callback_data="check_none"))
            await message.answer(f"⚠️ **Access Paused.**\n\n{name}, it looks like you're not in the channel anymore. Jump back in and I'll unlock everything.", reply_markup=kb.as_markup())
            return

    # --- PROCEED ---
    await log_user(message.from_user, source)

    if not payload:
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🔴 YouTube Channel", url=YOUTUBE_LINK))
        kb.row(InlineKeyboardButton(text="📸 Instagram Page", url=INSTAGRAM_LINK))
        await message.answer(f"**Yo {name}! 👋**\n\nYou're inside the Vault, but I don't know which file you want yet.\n\nGo grab a **LINK** from one of my videos and click it to receieve your specific guide , or click a link from the description.", reply_markup=kb.as_markup())
        return

    await deliver_content(message, payload, source)

@dp.callback_query(F.data.startswith("check_"))
async def check_join(callback: types.CallbackQuery):
    
    # 🚫 BAN CHECK
    if await is_banned(callback.from_user.id):
        await callback.answer("🚫 You are banned.", show_alert=True)
        return

    # 🛑 MAINTENANCE CHECK
    if await check_maintenance():
        await callback.answer("🚧 Maintenance Mode is ON. Try again later.", show_alert=True)
        return

    try: raw_arg = callback.data.split("_", 1)[1]
    except: raw_arg = "none"
    
    source = "YouTube" 
    payload = None
    
    if raw_arg != "none":
        payload = raw_arg
        if raw_arg.startswith("ig_"): 
            source = "Instagram"
            payload = raw_arg.replace("ig_", "")
        elif raw_arg.startswith("yt_"): 
            source = "YouTube"
            payload = raw_arg.replace("yt_", "")
        else:
            source = "YouTube"
            payload = raw_arg

    try: await callback.message.edit_text("🔄 *Verifying membership...*"); 
    except: pass
    await asyncio.sleep(1.0)
    
    try:
        user_status = await bot.get_chat_member(CHANNEL_ID, callback.from_user.id)
        if user_status.status in ['left', 'kicked', 'restricted']: raise Exception("Not Member")
        
        await log_user(callback.from_user, source)
        
        if payload:
            await deliver_content(callback.message, payload, source)
        else:
            await callback.message.answer(f"**You're in, {callback.from_user.first_name}!** ✅\n\nWelcome back to the Vault.")
    except:
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🚀 Join Telegram Family", url=CHANNEL_LINK))
        kb.row(InlineKeyboardButton(text="🔄 Try Again", callback_data=f"check_{raw_arg}"))
        try: await callback.message.edit_text(f"❌ **Still can't see you, {callback.from_user.first_name}!**\n\nMake sure you join the channel first, then click the button.", reply_markup=kb.as_markup())
        except TelegramBadRequest: pass

async def deliver_content(message: types.Message, payload: str, source: str):
    data = await get_content(payload)
    name = message.chat.first_name 
    
    if not data: 
        await message.answer(f"❌ **Hmm... Code `{payload}` isn't working.**\n\nDouble check it, or maybe there is some problem with file. {name}, try grabbing a fresh code from my latest video.")
        return
    
    try: await message.delete()
    except: pass
    
    # 
    await message.answer(f"**Access Granted, {name}.** 🔓\n\nI kept this safe for you. Here is the file you wanted:\n{data['main_link']}")
    
    if data['aff_link'] and len(data['aff_link']) > 5:
        await asyncio.sleep(1.5)
        kb_aff = InlineKeyboardBuilder()
        kb_aff.button(text="🚀 UNLOCK THE TOOL", url=data['aff_link'])
        await message.answer(f"👀 **Wait, I have one more thing...**\n\n{data['aff_text']}\n\nDon't share this with everyone.", reply_markup=kb_aff.as_markup())

    await asyncio.sleep(1.5)
    
    if source == "Instagram":
        video = await get_viral_video()
        if video:
            kb_cross = InlineKeyboardBuilder()
            kb_cross.button(text="▶️ WATCH VIDEO", url=video['link'])
            await message.answer(f"🔥 **Go Deeper:**\n\nI broke this down fully on YouTube. If you want the real detailed strategy, watch this:\n{video['title']}", reply_markup=kb_cross.as_markup())
            
    elif source == "YouTube" or source == "Direct":
        reel = await get_viral_reel()
        if reel:
            kb_cross = InlineKeyboardBuilder()
            kb_cross.button(text="📸 WATCH REEL", url=reel['link'])
            await message.answer(f"⚡ **Quick Hack:**\n\nI dropped a 60-second version of this on Insta. Check it out:\n{reel['desc']}", reply_markup=kb_cross.as_markup())
        else:
            kb_cross = InlineKeyboardBuilder()
            kb_cross.button(text="📸 FOLLOW INSTA", url=INSTAGRAM_LINK)
            await message.answer("⚡ **Daily Hacks:**\n\nI drop daily alpha on Instagram. Don't miss it.", reply_markup=kb_cross.as_markup())

async def main():
    print("✅ User Bot (Human Edition) is Online...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    print("🚀 STARTING INDIVIDUAL CORE TEST: BOT 1")
    
    # 1. Start Health Server in background thread (Fixes Render Port error)
    threading.Thread(target=run_health_server, daemon=True).start()
    
    # 2. Run the Bot
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Bot 1 Stopped")

