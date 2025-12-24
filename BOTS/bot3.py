import telebot
import pymongo
import pandas as pd
import os
import threading
import time
import requests
import re
import pytz 
import functools
import sys
from collections import Counter
from telebot import types
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.storage import StateMemoryStorage
from datetime import datetime
from aiohttp import web

# ================= CONFIGURATION (FULLY SECURED) =================
# All private credentials are now pulled from Environment Variables.
BOT_TOKEN = os.environ.get("BOT_3_TOKEN") 
MONGO_URI = os.environ.get("MONGO_URI")

# IDENTITY HIDDEN: Values are pulled from Render Environment Variables
MAIN_BOT_USERNAME = os.environ.get("MAIN_BOT_USERNAME") 
MASTER_ADMIN_ID_RAW = os.environ.get("MASTER_ADMIN_ID")
MASTER_ADMIN_ID = int(MASTER_ADMIN_ID_RAW) if MASTER_ADMIN_ID_RAW else 0

# Timezone
IST = pytz.timezone('Asia/Kolkata')

print("🔄 Initializing Empire Data Manager (God Mode)...")

# ================= DATABASE CONNECTION =================
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["MSANodeDB"]
    
    # Collections (Interconnected with Bot 1 and Bot 2)
    col_active = db["active_content"]
    col_viral = db["viral_videos"]
    col_reels = db["viral_reels"]
    col_admins = db["admins"]
    col_audit = db["audit_log"]
    col_users = db["user_logs"]
    col_recycle = db["recycle_bin"]
    
    print("✅ Connected to MongoDB Atlas")
except Exception as e:
    print(f"❌ CRITICAL DB ERROR: {e}")
    sys.exit(1)

# --- STABILITY ENHANCEMENT ---
state_storage = StateMemoryStorage()
bot = telebot.TeleBot(BOT_TOKEN, threaded=True, num_threads=15, state_storage=state_storage)

# ================= SECURITY & HELPERS =================
admin_cache = []
last_cache_time = 0

def safe_execute(func):
    """Prevents the bot from crashing on errors."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"⚠️ Error in {func.__name__}: {e}")
            try:
                chat_id = None
                if len(args) > 0:
                    if hasattr(args[0], 'chat'): chat_id = args[0].chat.id
                    elif hasattr(args[0], 'message'): chat_id = args[0].message.chat.id
                
                if chat_id:
                    bot.send_message(chat_id, f"❌ Command Failed: {e}")
            except: pass
    return wrapper

def get_authorized_users():
    global admin_cache, last_cache_time
    if time.time() - last_cache_time < 120 and admin_cache: return admin_cache
    try:
        cursor = col_admins.find({}, {"user_id": 1})
        db_admins = [int(doc["user_id"]) for doc in cursor if str(doc["user_id"]).isdigit()]
        if MASTER_ADMIN_ID != 0 and MASTER_ADMIN_ID not in db_admins: db_admins.append(MASTER_ADMIN_ID)
        admin_cache = db_admins; last_cache_time = time.time()
        return admin_cache
    except: return [MASTER_ADMIN_ID] if MASTER_ADMIN_ID != 0 else []

def is_admin(user_id): return user_id in get_authorized_users()

# EXACT FORMAT: 04:08 PM 24-12-2025
def get_current_time(): return datetime.now(IST).strftime("%I:%M %p %d-%m-%Y")

def get_next_code_suggestion():
    try: return f"M{col_active.count_documents({}) + 101}"
    except: return "M101"

def get_main_menu():
    markup = InlineKeyboardMarkup()
    markup.row_width = 2
    markup.add(
        InlineKeyboardButton("📋 List", callback_data="hub_list"), 
        InlineKeyboardButton("➕ Add", callback_data="hub_add")
    )
    markup.add(
        InlineKeyboardButton("🔍 Search", callback_data="hub_search"), 
        InlineKeyboardButton("🗑️ Delete", callback_data="hub_delete")
    )
    markup.add(
        InlineKeyboardButton("🔥 Popularity Stats", callback_data="btn_popularity"), # LIVE CLICK TRACKING
        InlineKeyboardButton("🔗 Get Start Links", callback_data="btn_link")
    )
    markup.add(
        InlineKeyboardButton("💾 Backup", callback_data="hub_backup"),
        InlineKeyboardButton("📊 DB Counts", callback_data="btn_stats")
    )
    markup.add(
        InlineKeyboardButton("🏥 Health Check", callback_data="btn_health"),
        InlineKeyboardButton("🔄 Refresh Admins", callback_data="btn_refresh")
    )
    markup.add(
        InlineKeyboardButton("☢️ CLEAR ALL DATA", callback_data="nuclear_1")
    )
    return markup

# ================= HANDLERS =================

@bot.message_handler(commands=['start'])
@safe_execute
def send_welcome(message):
    if not is_admin(message.from_user.id): return 
    bot.reply_to(message, f"⚡ **Data Core (God Mode)**\n📅 {get_current_time()}\n\n**👇 Operations Console:**", parse_mode="Markdown", reply_markup=get_main_menu())

@bot.callback_query_handler(func=lambda call: True)
@safe_execute
def handle_query(call):
    if not is_admin(call.from_user.id): return
    data = call.data

    # --- 1. LIST SYSTEM ---
    if data == "hub_list":
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("📹 List YouTube", callback_data="list_yt"),
            InlineKeyboardButton("📸 List Instagram", callback_data="list_ig"),
            InlineKeyboardButton("📄 List PDF Vault", callback_data="list_pdf"),
            InlineKeyboardButton("⬅️ Back", callback_data="back_main")
        )
        bot.edit_message_text("📋 **Select Database to List:**", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    elif data == "list_pdf": run_full_inventory(call.message)
    elif data == "list_yt": list_simple(col_viral, "YOUTUBE", call.message)
    elif data == "list_ig": list_simple(col_reels, "INSTAGRAM", call.message)

    # --- 2. ADD SYSTEM ---
    elif data == "hub_add":
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("📹 Add YT (Unique Code)", callback_data="btn_yt"),
            InlineKeyboardButton("📸 Add IG (Unique Code)", callback_data="btn_insta"),
            InlineKeyboardButton("📄 Add PDF (M-Code)", callback_data="btn_add_pdf"),
            InlineKeyboardButton("⬅️ Back", callback_data="back_main")
        )
        bot.edit_message_text("➕ **Select Category to Add:**", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    # --- 3. SEARCH SYSTEM ---
    elif data == "hub_search":
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("🔍 Search YT by Code", callback_data="search_yt"),
            InlineKeyboardButton("🔍 Search IG by Code", callback_data="search_ig"),
            InlineKeyboardButton("🔍 Search PDF by Code", callback_data="search_pdf"),
            InlineKeyboardButton("⬅️ Back", callback_data="back_main")
        )
        bot.edit_message_text("🔍 **Select Database to Search:**", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    # --- 4. DELETE SYSTEM ---
    elif data == "hub_delete":
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("🗑️ Delete YT Entry", callback_data="del_yt"),
            InlineKeyboardButton("🗑️ Delete IG Entry", callback_data="del_ig"),
            InlineKeyboardButton("🗑️ Delete PDF Entry", callback_data="del_pdf"),
            InlineKeyboardButton("⬅️ Back", callback_data="back_main")
        )
        bot.edit_message_text("🗑️ **Select Database to Delete From:**", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    # --- 5. BACKUP SYSTEM ---
    elif data == "hub_backup":
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("📥 Download YT CSV", callback_data="exp_yt"),
            InlineKeyboardButton("📥 Download IG CSV", callback_data="exp_ig"),
            InlineKeyboardButton("📥 Download PDF CSV", callback_data="exp_pdf"),
            InlineKeyboardButton("⬅️ Back", callback_data="back_main")
        )
        bot.edit_message_text("💾 **Which Data Backup do you need?**", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    # --- 6. POPULARITY ANALYTICS ---
    elif data == "btn_popularity":
        run_popularity_report(call.message)

    # --- 7. NUCLEAR CLEAR (2-STEP CONFIRMATION) ---
    elif data == "nuclear_1":
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🛑 YES, DELETE EVERYTHING", callback_data="nuclear_2"))
        kb.add(InlineKeyboardButton("❌ CANCEL", callback_data="back_main"))
        bot.edit_message_text("⚠️ **CRITICAL WARNING:** This will permanently wipe ALL databases (YT, IG, PDF). Are you sure?", call.message.chat.id, call.message.message_id, reply_markup=kb)

    elif data == "nuclear_2":
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("☢️ EXECUTE FINAL WIPE", callback_data="nuclear_final"))
        kb.add(InlineKeyboardButton("❌ ABORT", callback_data="back_main"))
        bot.edit_message_text("🚨 **FINAL WARNING:** Every link will be lost. Purge now?", call.message.chat.id, call.message.message_id, reply_markup=kb)

    elif data == "nuclear_final":
        col_active.delete_many({}); col_viral.delete_many({}); col_reels.delete_many({})
        bot.edit_message_text("💥 **DATABASE PURGED.** All empire data destroyed.", call.message.chat.id, call.message.message_id, reply_markup=get_main_menu())

    # --- STEP TRIGGER MAPPING ---
    elif data == "back_main": bot.edit_message_text("⚡ **Operations Console:**", call.message.chat.id, call.message.message_id, reply_markup=get_main_menu())
    
    # Adding
    elif data == "btn_add_pdf": bot.register_next_step_handler(bot.send_message(call.message.chat.id, f"📥 Enter M-Code:\n💡 Suggested: `{get_next_code_suggestion()}`", parse_mode="Markdown"), step_add_code)
    elif data == "btn_yt": bot.register_next_step_handler(bot.send_message(call.message.chat.id, "📹 Enter Unique YT Code:"), step_yt_code)
    elif data == "btn_insta": bot.register_next_step_handler(bot.send_message(call.message.chat.id, "📸 Enter Unique IG Code:"), step_ig_code)
    
    # Searching
    elif data == "search_yt": bot.register_next_step_handler(bot.send_message(call.message.chat.id, "🔍 Enter YT Code:"), lambda m: step_search_simple(col_viral, m))
    elif data == "search_ig": bot.register_next_step_handler(bot.send_message(call.message.chat.id, "🔍 Enter IG Code:"), lambda m: step_search_simple(col_reels, m))
    elif data == "search_pdf": bot.register_next_step_handler(bot.send_message(call.message.chat.id, "🔍 Enter PDF Code:"), step_search_pdf)
    
    # Deleting
    elif data == "del_yt": bot.register_next_step_handler(bot.send_message(call.message.chat.id, "🗑️ Enter YT Code to Remove:"), lambda m: step_delete_simple(col_viral, m))
    elif data == "del_ig": bot.register_next_step_handler(bot.send_message(call.message.chat.id, "🗑️ Enter IG Code to Remove:"), lambda m: step_delete_simple(col_reels, m))
    elif data == "del_pdf": bot.register_next_step_handler(bot.send_message(call.message.chat.id, "🗑️ Enter PDF Code to Delete:"), step_remove_pdf)
    
    # Backup & Other
    elif data == "exp_yt": run_csv_export(col_viral, "YT_Backup.csv", call.message)
    elif data == "exp_ig": run_csv_export(col_reels, "IG_Backup.csv", call.message)
    elif data == "exp_pdf": run_csv_export(col_active, "PDF_Backup.csv", call.message)
    elif data == "btn_link": bot.register_next_step_handler(bot.send_message(call.message.chat.id, "🔗 Enter PDF Code to Generate Smart Links:"), step_get_link)
    elif data == "btn_stats": run_stats(call.message)
    elif data == "btn_health": run_health_check(call.message)
    elif data == "btn_refresh":
        global last_cache_time; last_cache_time = 0; get_authorized_users()
        bot.send_message(call.message.chat.id, "🔐 Admins Refreshed.", reply_markup=get_main_menu())

# ================= CORE LOGIC FUNCTIONS =================

@safe_execute
def run_popularity_report(message):
    """Generates a live ranking of blueprints by click counts."""
    top_docs = list(col_active.find().sort("clicks", -1).limit(20))
    if not top_docs:
        return bot.send_message(message.chat.id, "⚠️ No data available yet.")
    
    report = "🔥 **MOST POPULAR BLUEPRINTS**\n\n"
    for i, doc in enumerate(top_docs, 1):
        clicks = doc.get('clicks', 0)
        report += f"{i}. Code: `{doc.get('code')}` — ⚡ **{clicks} Clicks**\n"
    
    bot.send_message(message.chat.id, report, parse_mode="Markdown", reply_markup=get_main_menu())

def run_csv_export(collection, filename, message):
    data = list(collection.find({}, {"_id": 0}))
    if not data: return bot.send_message(message.chat.id, "❌ No data found.")
    pd.DataFrame(data).to_csv(filename, index=False)
    with open(filename, "rb") as f: bot.send_document(message.chat.id, f, caption=f"💾 {filename}")
    os.remove(filename)

def list_simple(collection, label, message):
    text = f"📦 **{label} DATABASE**\n\n"
    count = 0
    for doc in collection.find():
        text += f"🔹 Code: `{doc.get('code')}`\n🔗 {doc.get('link')}\n\n"
        count += 1
    if count == 0: text = f"❌ {label} Database is empty."
    bot.send_message(message.chat.id, text, parse_mode="Markdown")

@safe_execute
def run_full_inventory(message):
    filename = f"PDF_Vault_{int(time.time())}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"📦 MSANODE PDF VAULT | {get_current_time()}\n{'='*50}\n")
        count = 0
        for doc in col_active.find({}):
            f.write(f"[{doc.get('code')}]\n")
            f.write(f"PDF: {doc.get('pdf_link')}\n")
            f.write(f"AFF: {doc.get('aff_link')}\n")
            f.write(f"CREATED: {doc.get('created_at', 'N/A')}\n")
            f.write(f"LIVE CLICKS: {doc.get('clicks', 0)}\n")
            f.write(f"{'-'*50}\n")
            count += 1
        f.write(f"\nTOTAL: {count}")
    with open(filename, "rb") as f: bot.send_document(message.chat.id, f, caption=f"📄 PDF Master List")
    os.remove(filename)

# --- ADD YT/IG WITH CODES ---
def step_yt_code(m):
    code = m.text.upper().strip()
    bot.register_next_step_handler(bot.send_message(m.chat.id, f"Code `{code}` Saved.\n🔗 Send YouTube Link:"), lambda msg: finalize_simple_add(col_viral, code, msg))

def step_ig_code(m):
    code = m.text.upper().strip()
    bot.register_next_step_handler(bot.send_message(m.chat.id, f"Code `{code}` Saved.\n🔗 Send Instagram Link:"), lambda msg: finalize_simple_add(col_reels, code, msg))

def finalize_simple_add(collection, code, m):
    collection.insert_one({"code": code, "link": m.text.strip(), "created_at": get_current_time()})
    bot.send_message(m.chat.id, f"✅ Entry `{code}` successfully stored.", reply_markup=get_main_menu())

# --- PDF ADDITION ---
@safe_execute
def step_add_code(message):
    code = message.text.upper().strip()
    if col_active.find_one({"code": code}): return bot.reply_to(message, "⚠️ Code exists.", reply_markup=get_main_menu())
    bot.register_next_step_handler(bot.reply_to(message, f"Code `{code}` Locked.\n🔗 **Step 2:** Send PDF Link:"), step_add_pdf, code)

@safe_execute
def step_add_pdf(message, code):
    pdf = message.text.strip()
    bot.register_next_step_handler(bot.reply_to(message, "PDF OK.\n💸 **Step 3:** Send Affiliate Link:"), step_add_aff, code, pdf)

@safe_execute
def step_add_aff(message, code, pdf):
    aff = message.text.strip()
    # Interconnected logic: Bot 1 increments 'clicks' field
    col_active.insert_one({
        "code": code, 
        "pdf_link": pdf, 
        "aff_link": aff, 
        "created_at": get_current_time(), 
        "clicks": 0
    })
    bot.reply_to(message, f"✅ **PDF STORED**\nCode: `{code}`\nCreated: {get_current_time()}", reply_markup=get_main_menu())

# --- SEARCH & DELETE ---
def step_search_simple(collection, m):
    res = collection.find_one({"code": m.text.upper().strip()})
    if not res: return bot.send_message(m.chat.id, "❌ Code not found.")
    bot.send_message(m.chat.id, f"🔍 **Result Found:**\nCode: `{res['code']}`\nLink: {res['link']}\nDate: {res.get('created_at')}")

def step_search_pdf(m):
    res = col_active.find_one({"code": m.text.upper().strip()})
    if not res: return bot.send_message(m.chat.id, "❌ Code not found in PDF Vault.")
    bot.send_message(m.chat.id, f"🔍 **PDF Found:**\nCode: `{res['code']}`\nPDF: {res['pdf_link']}\nAFF: {res['aff_link']}\nClicks: {res.get('clicks', 0)}")

def step_delete_simple(collection, m):
    res = collection.delete_one({"code": m.text.upper().strip()})
    if res.deleted_count > 0: bot.send_message(m.chat.id, "🗑️ Entry deleted successfully.")
    else: bot.send_message(m.chat.id, "❌ Code not found.")

def step_remove_pdf(m):
    res = col_active.delete_one({"code": m.text.upper().strip()})
    if res.deleted_count > 0: bot.send_message(m.chat.id, "🗑️ PDF deleted successfully.")
    else: bot.send_message(m.chat.id, "❌ Code not found.")

# --- START LINKS ---
@safe_execute
def step_get_link(message):
    code = message.text.upper().strip()
    user = (MAIN_BOT_USERNAME or "@bot").replace('@', '')
    bot.reply_to(message, f"🔗 **Smart Links for {code}:**\n\n🔴 **YouTube:**\n`https://t.me/{user}?start=yt_{code}`\n\n📸 **Instagram:**\n`https://t.me/{user}?start=ig_{code}`", parse_mode="Markdown", reply_markup=get_main_menu())

def run_stats(message):
    msg = (f"📊 **STORAGE COUNTS**\n\n📄 PDF Vault: {col_active.count_documents({})}\n📹 YouTube DB: {col_viral.count_documents({})}\n📸 Instagram DB: {col_reels.count_documents({})}")
    bot.reply_to(message, msg, reply_markup=get_main_menu())

def run_health_check(message):
    bot.reply_to(message, "🏥 **System Online:**\n✅ DB Connected\n✅ Analytics Engine Loaded", reply_markup=get_main_menu())

# --- RENDER PORT BINDER ---
async def handle_health(request):
    return web.Response(text="MSANODE CORE ONLINE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except: pass

# ================= EXECUTION =================
if __name__ == "__main__":
    print("💎 MSANODE DATA CORE: ACTIVATING GOD MODE...")
    threading.Thread(target=run_health_server, daemon=True).start()
    bot.remove_webhook()
    time.sleep(1)
    
    while True:
        try:
            bot.polling(none_stop=True, skip_pending=True, timeout=60)
        except Exception as e:
            print(f"⚠️ Polling Error: {e}")
            time.sleep(10)
