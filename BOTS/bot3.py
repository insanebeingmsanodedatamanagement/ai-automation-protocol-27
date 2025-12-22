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

# ================= CONFIGURATION =================
# These pull from the Render Environment Variables we will set next
BOT_TOKEN = os.getenv("BOT_3_TOKEN") 
MONGO_URI = os.getenv("MONGO_URI")

if not BOT_TOKEN or not MONGO_URI:
    print("❌ SECURITY ALERT: Bot 3 keys missing from Environment!")

MAIN_BOT_USERNAME = "@msanodedatamanagerbot" 
MASTER_ADMIN_ID = 6988593629 

# Timezone
IST = pytz.timezone('Asia/Kolkata')

print("🔄 Initializing Empire Data Manager (God Mode)...")

# ================= DATABASE CONNECTION =================
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["MSANNodeDB"]
    
    # Collections
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

# --- STABILITY ENHANCEMENT: Memory Storage & Threading ---
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
                # Optimized error reporting
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
        if MASTER_ADMIN_ID not in db_admins: db_admins.append(MASTER_ADMIN_ID)
        admin_cache = db_admins; last_cache_time = time.time()
        return admin_cache
    except: return [MASTER_ADMIN_ID]

def is_admin(user_id): return user_id in get_authorized_users()

def get_current_time(): return datetime.now(IST).strftime("%d/%m/%Y %I:%M %p")

def log_audit(action, user, details, code="N/A"):
    try: col_audit.insert_one({"timestamp": get_current_time(), "user": str(user), "action": action, "code": str(code), "details": str(details)})
    except: pass

def get_next_code_suggestion():
    try: return f"VIDEO{col_active.count_documents({}) + 1}"
    except: return "VIDEO1"

def get_storage_usage():
    try:
        stats = db.command("dbstats")
        used_mb = round(stats.get('storageSize', 0) / (1024 * 1024), 2)
        percent = round((stats.get('storageSize', 0) / (512 * 1024 * 1024)) * 100, 2)
        return used_mb, percent
    except: return 0, 0

def get_main_menu():
    markup = InlineKeyboardMarkup()
    markup.row_width = 2
    # Content
    markup.add(InlineKeyboardButton("➕ Add Content", callback_data="btn_add"), InlineKeyboardButton("🔗 Get Smart Links", callback_data="btn_link"))
    # Reports
    markup.add(InlineKeyboardButton("📦 Full Inventory", callback_data="btn_full_list"), InlineKeyboardButton("💾 Check Storage", callback_data="btn_storage"))
    # Media
    markup.add(InlineKeyboardButton("🎬 Add YT Video", callback_data="btn_yt"), InlineKeyboardButton("📸 Add IG Reel", callback_data="btn_insta"))
    # Edit/Search
    markup.add(InlineKeyboardButton("🔍 Search", callback_data="btn_search"), InlineKeyboardButton("✏️ Edit", callback_data="btn_edit"))
    # Maintenance
    markup.add(InlineKeyboardButton("🗑️ Remove", callback_data="btn_remove"), InlineKeyboardButton("🧹 Deep Clean", callback_data="btn_clean"))
    # Stats
    markup.add(InlineKeyboardButton("🏥 Health Check", callback_data="btn_health"), InlineKeyboardButton("📈 Traffic Stats", callback_data="btn_traffic"))
    # Admin
    markup.add(InlineKeyboardButton("📊 DB Counts", callback_data="btn_stats"), InlineKeyboardButton("📂 Export CSV", callback_data="btn_export"))
    markup.add(InlineKeyboardButton("🔄 Refresh Admins", callback_data="btn_refresh"))
    return markup

# ================= HANDLERS =================
@bot.message_handler(commands=['start'])
@safe_execute
def send_welcome(message):
    if not is_admin(message.from_user.id): return 
    bot.reply_to(message, f"⚡ **Data Manager (God Mode)**\n📅 {get_current_time()}\n\n**👇 Operations Console:**", parse_mode="Markdown", reply_markup=get_main_menu())

@bot.message_handler(content_types=['document'])
@safe_execute
def handle_docs(message):
    if not is_admin(message.from_user.id): return
    file_name = message.document.file_name
    if not file_name.endswith(('.csv', '.xlsx')): return bot.reply_to(message, "❌ .csv or .xlsx only.")
    
    bot.reply_to(message, "📥 **Importing...**")
    file_info = bot.get_file(message.document.file_id)
    downloaded_file = bot.download_file(file_info.file_path)
    save_path = f"temp_{int(time.time())}.{'xlsx' if file_name.endswith('.xlsx') else 'csv'}"
    
    with open(save_path, 'wb') as new_file: new_file.write(downloaded_file)
    df = pd.read_excel(save_path) if save_path.endswith('.xlsx') else pd.read_csv(save_path)
    
    if len(df.columns) < 3: os.remove(save_path); return bot.reply_to(message, "❌ Columns must be: Code, PDF, Affiliate")
    
    data_list = [{"code": str(row[0]), "pdf_link": str(row[1]), "aff_link": str(row[2])} for index, row in df.iterrows()]
    if data_list: col_active.insert_many(data_list)
    
    log_audit("BULK_IMPORT", message.from_user.first_name, f"Imported {len(data_list)}")
    bot.reply_to(message, f"✅ Imported {len(data_list)} items.", reply_markup=get_main_menu())
    os.remove(save_path)

@bot.callback_query_handler(func=lambda call: True)
@safe_execute
def handle_query(call):
    # --- CRITICAL FIX: STOP LOADING SPINNER IMMEDIATELY ---
    bot.answer_callback_query(call.id)
    print(f"💎 BUTTON ACTION: {call.data}")

    if not is_admin(call.from_user.id): return bot.send_message(call.message.chat.id, "⛔ Access Denied")
    data = call.data

    if data.startswith("edit:"):
        parts = data.split(":"); field, code = parts[1], parts[2]
        msg = bot.send_message(call.message.chat.id, f"✏️ Editing **{code}**\nSend NEW **{field.upper()}**:", parse_mode="Markdown")
        bot.register_next_step_handler(msg, step_process_edit, field, code)
        return

    # Routing
    if data == "btn_add":
        msg = bot.send_message(call.message.chat.id, f"Enter Code:\n💡 Sug: `{get_next_code_suggestion()}`", parse_mode="Markdown")
        bot.register_next_step_handler(msg, step_add_code)
    
    elif data == "btn_link": 
        msg = bot.send_message(call.message.chat.id, "🔗 **Enter Code to Generate Links:**", parse_mode="Markdown")
        bot.register_next_step_handler(msg, step_get_link)

    elif data == "btn_storage": 
        used, perc = get_storage_usage()
        bar = "█" * int(perc/10) + "░" * (10 - int(perc/10))
        bot.send_message(call.message.chat.id, f"💾 **STORAGE**\n📦 Used: {used} MB\n📊 `{bar}` {perc}%\n🛑 Limit: 512 MB", parse_mode="Markdown", reply_markup=get_main_menu())

    elif data == "btn_full_list": 
        bot.send_message(call.message.chat.id, "📦 **Generating Report...**")
        run_full_inventory(call.message)

    elif data == "btn_search":
        msg = bot.send_message(call.message.chat.id, "🔍 Enter Code:")
        bot.register_next_step_handler(msg, step_search_code)

    elif data == "btn_edit":
        msg = bot.send_message(call.message.chat.id, "✏️ Enter Code:")
        bot.register_next_step_handler(msg, step_edit_start)

    elif data == "btn_remove":
        msg = bot.send_message(call.message.chat.id, "🗑️ Enter Code to Remove:")
        bot.register_next_step_handler(msg, step_remove_code)

    elif data == "btn_yt":
        msg = bot.send_message(call.message.chat.id, "🎬 Enter YT Link:")
        bot.register_next_step_handler(msg, step_yt_link)

    elif data == "btn_insta":
        msg = bot.send_message(call.message.chat.id, "📸 Enter Reel Link:")
        bot.register_next_step_handler(msg, step_insta_link)

    elif data == "btn_traffic":
        run_traffic_analysis(call.message)
    elif data == "btn_clean":
        run_deep_clean(call.message)
    elif data == "btn_health":
        bot.send_message(call.message.chat.id, "🏥 Scanning...")
        run_health_check(call.message)
    elif data == "btn_export":
        run_export(call.message)
    elif data == "btn_stats":
        run_stats(call.message)
    elif data == "btn_refresh":
        global last_cache_time; last_cache_time = 0; get_authorized_users()
        bot.send_message(call.message.chat.id, "🔐 Admins Refreshed.", reply_markup=get_main_menu())

# ================= LOGIC FUNCTIONS =================
@safe_execute
def run_full_inventory(message):
    filename = f"Inventory_{int(time.time())}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"📦 INVENTORY | {get_current_time()}\n{'='*40}\n")
        count = 0
        for doc in col_active.find({}):
            f.write(f"[{doc.get('code')}]\nPDF: {doc.get('pdf_link')}\nAFF: {doc.get('aff_link')}\n{'-'*40}\n")
            count += 1
        f.write(f"\nTOTAL: {count}")
    with open(filename, "rb") as f: bot.send_document(message.chat.id, f, caption=f"📦 Full List ({count} items)")
    os.remove(filename)

@safe_execute
def run_traffic_analysis(message):
    res = list(col_users.aggregate([{"$group": {"_id": "$source", "count": {"$sum": 1}}}]))
    if not res: return bot.reply_to(message, "⚠️ No Data.", reply_markup=get_main_menu())
    total = sum([r['count'] for r in res])
    rep = "📈 **SOURCES**\n"
    for r in res: rep += f"🔹 {r['_id']}: {r['count']} ({round(r['count']/(total or 1)*100,1)}%)\n"
    bot.reply_to(message, rep, parse_mode="Markdown", reply_markup=get_main_menu())

@safe_execute
def run_stats(message):
    msg = (f"📊 **STATS**\n✅ Active: {col_active.count_documents({})}\n🎬 Viral: {col_viral.count_documents({})}\n"
            f"📸 Reels: {col_reels.count_documents({})}\n🗑️ Bin: {col_recycle.count_documents({})}")
    bot.reply_to(message, msg, reply_markup=get_main_menu(), parse_mode="Markdown")

@safe_execute
def run_deep_clean(message):
    res = col_active.delete_many({"$or": [{"code": ""}, {"pdf_link": ""}]})
    bot.reply_to(message, f"🧹 Cleaned {res.deleted_count} junk items.", reply_markup=get_main_menu())

@safe_execute
def run_health_check(message):
    issues = 0; report = "🏥 **Report:**\n"
    for doc in col_active.find({}).limit(100):
        if not str(doc.get("pdf_link", "")).startswith("http"):
            report += f"⚠️ {doc.get('code')}: Invalid URL\n"; issues += 1
    if issues == 0: bot.reply_to(message, "✅ Healthy.", reply_markup=get_main_menu())
    else: bot.reply_to(message, report, reply_markup=get_main_menu())

@safe_execute
def run_export(message):
    df = pd.DataFrame(list(col_active.find({}, {"_id": 0})))
    df.to_csv("backup.csv", index=False)
    with open("backup.csv", "rb") as f: bot.send_document(message.chat.id, f, caption="Backup")
    os.remove("backup.csv")

# ================= STEPS =================
@safe_execute
def step_add_code(message):
    code = message.text.strip()
    if col_active.find_one({"code": code}): return bot.reply_to(message, "⚠️ Exists.", reply_markup=get_main_menu())
    msg = bot.reply_to(message, f"Code: `{code}`\nSend PDF Link:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, step_add_pdf, code)

@safe_execute
def step_add_pdf(message, code):
    pdf = message.text.strip()
    msg = bot.reply_to(message, "Send Aff Link:")
    bot.register_next_step_handler(msg, step_add_aff, code, pdf)

@safe_execute
def step_add_aff(message, code, pdf):
    aff = message.text.strip()
    col_active.insert_one({"code": code, "pdf_link": pdf, "aff_link": aff})
    log_audit("ADDED", message.from_user.username or message.from_user.id, code)
    bot.reply_to(message, "✅ Added.", reply_markup=get_main_menu())

@safe_execute
def step_get_link(message):
    code = message.text.strip()
    base = MAIN_BOT_USERNAME.replace('@', '')
    bot.reply_to(message, f"🔗 **Links for {code}**\n\n🔴 `https://t.me/{base}?start=yt_{code}`\n📸 `https://t.me/{base}?start=ig_{code}`", parse_mode="Markdown", reply_markup=get_main_menu())

@safe_execute
def step_search_code(message):
    res = col_active.find_one({"code": message.text.strip()})
    if not res: return bot.reply_to(message, "❌ Not Found.", reply_markup=get_main_menu())
    bot.reply_to(message, f"🔍 **{res['code']}**\n📄 {res['pdf_link']}\n💰 {res['aff_link']}", reply_markup=get_main_menu())

@safe_execute
def step_edit_start(message):
    code = message.text.strip()
    if not col_active.find_one({"code": code}): return bot.reply_to(message, "❌ Not Found.")
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("Edit PDF", callback_data=f"edit:pdf_link:{code}"), InlineKeyboardButton("Edit Aff", callback_data=f"edit:aff_link:{code}"))
    bot.reply_to(message, f"Edit **{code}**:", parse_mode="Markdown", reply_markup=markup)

@safe_execute
def step_process_edit(message, field, code):
    col_active.update_one({"code": code}, {"$set": {field: message.text.strip()}})
    bot.reply_to(message, "✅ Updated.", reply_markup=get_main_menu())

@safe_execute
def step_remove_code(message):
    code = message.text.strip()
    res = col_active.find_one_and_delete({"code": code})
    if res:
        del res["_id"]; res["deleted_at"] = get_current_time()
        col_recycle.insert_one(res)
        bot.reply_to(message, "🗑️ Deleted.", reply_markup=get_main_menu())
    else: bot.reply_to(message, "❌ Not Found.")

def step_yt_link(m): bot.register_next_step_handler(bot.reply_to(m, "Title:"), lambda msg: (col_viral.insert_one({"link": m.text, "desc": msg.text}), bot.reply_to(msg, "✅ Saved.")))
def step_insta_link(m): bot.register_next_step_handler(bot.reply_to(m, "Desc:"), lambda msg: (col_reels.insert_one({"link": m.text, "desc": msg.text}), bot.reply_to(msg, "✅ Saved.")))
# --- RENDER PORT BINDER ---
async def handle_health(request):
    return web.Response(text="BOT 3 IS ONLINE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")
# ================= THE SUPREME RESTART =================
if __name__ == "__main__":
    print("💎 MSANODE DATA CORE: ACTIVATING GOD MODE...")
    
    # 1. Start the Health Server for Render (Stops the "No open ports" error)
    threading.Thread(target=run_health_server, daemon=True).start()
    
    # 2. Kill Ghost Sessions
    bot.remove_webhook()
    time.sleep(2)
    
    # 3. High-Stability Polling Loop
    while True:
        try:
            print("📡 Connection established. Monitoring Buttons...")
            bot.polling(none_stop=True, skip_pending=True, timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"⚠️ Conflict/Error: {e}. Reconnecting in 10s...")
            time.sleep(10)

