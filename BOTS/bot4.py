import asyncio
import logging
import os
import io
import pickle
import pymongo
import re
import threading
from aiohttp import web
import shutil
import base64
import sys
import socket
import time
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import FSInputFile, ReplyKeyboardMarkup, KeyboardButton, BotCommand
from aiogram.utils.keyboard import ReplyKeyboardBuilder

# ReportLab & Google Imports
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.colors import Color, gray
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import inch
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ==========================================
# ⚡ CONFIGURATION (SECURED GOD-MODE)
# ==========================================
# All sensitive IDs are now pulled from Render Environment Variables.
BOT_TOKEN = os.getenv("BOT_4_TOKEN") 
MONGO_URI = os.getenv("MONGO_URI") 

# HIDDEN IDENTITY: No plain text ID or Folder link remains.
OWNER_ID = int(os.environ.get("MASTER_ADMIN_ID", 0)) 
PARENT_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")

# System File Mapping
CREDENTIALS_FILE = os.environ.get("CRED_FILE_NAME", 'credentials.json')
TOKEN_FILE = os.environ.get("TOKEN_FILE_NAME", 'token.pickle')

START_TIME = time.time() 

if not BOT_TOKEN or not PARENT_FOLDER_ID:
    print("❌ CRITICAL ERROR: Bot 4 Security credentials missing!")

# ==========================================
# 🛠 SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
col_pdfs = None
db_client = None
# --- RENDER SECRET INJECTION ---
def prepare_secrets():
    """Moves Google secrets from Render /etc/secrets to local folder."""
    targets = {"token.pickle.base64": "token.pickle", "credentials.json": "credentials.json"}
    search_paths = ["/etc/secrets", "..", "."]
    
    for src, target in targets.items():
        for path in search_paths:
            full_src = os.path.join(path, src)
            if os.path.exists(full_src):
                if ".base64" in src:
                    with open(full_src, "r") as f:
                        binary = base64.b64decode(f.read().strip())
                    with open(target, "wb") as f: f.write(binary)
                else:
                    shutil.copy(full_src, target)
                print(f"✅ Secret Injected: {target}")
                break

# --- RENDER PORT BINDER ---
async def handle_health(request):
    return web.Response(text="CORE 4 (PDF INFRASTRUCTURE) IS ACTIVE")

def run_health_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_health)
        port = int(os.environ.get("PORT", 10000))
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"📡 Health Server Note: {e}")
def connect_db():
    global col_pdfs, db_client
    try:
        db_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = db_client["MSANodeDB"]
        col_pdfs = db["pdf_library"]
        db_client.server_info()
        return True
    except Exception as e:
        logging.error(f"DB Connect Error: {e}")
        return False

connect_db()
class BotState(StatesGroup):
    waiting_for_code = State()
    processing_script = State()
    fetching_link = State()
    deleting_pdf = State()
    confirm_overwrite = State()  # Add this line

def get_main_menu():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📄 Generate PDF"), KeyboardButton(text="🔗 Get Link"))
    builder.row(KeyboardButton(text="📋 Show Library"), KeyboardButton(text="📊 Storage Info"))
    builder.row(KeyboardButton(text="🗑 Remove PDF"), KeyboardButton(text="💎 Elite Help"))
    return builder.as_markup(resize_keyboard=True)

# ==========================================
# 📊 VISUAL ANALYTICS
# ==========================================

def generate_progress_bar(percentage):
    """Creates a visual progress bar for Telegram."""
    filled_length = int(percentage // 10)
    bar = "▓" * filled_length + "░" * (10 - filled_length)
    return f"|{bar}| {percentage:.1f}%"

# ==========================================
# 🚀 AUTOMATION TASKS
# ==========================================

async def hourly_pulse():
    while True:
        await asyncio.sleep(3600)
        try:
            db_client.server_info()
            await bot.send_message(OWNER_ID, "💓 **PULSE:** All systems ready, Master Sadiq.")
        except: connect_db()

async def daily_briefing():
    while True:
        now = datetime.now()
        target = now.replace(hour=8, minute=40, second=0, microsecond=0)
        if now >= target: target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            count = col_pdfs.count_documents({})
            await bot.send_message(OWNER_ID, f"☀️ **8:40 AM REPORT**\n\nMaster Sadiq, the system is clean and operational. Current Library: `{count}` guides.")
        except: pass

async def system_guardian():
    while True:
        try:
            db_client.server_info()
            get_drive_service()
        except: connect_db()
        await asyncio.sleep(1800)

async def auto_janitor():
    while True:
        await asyncio.sleep(86400)
        for file in os.listdir():
            if file.endswith(".pdf"):
                try: os.remove(file)
                except: pass

async def weekly_backup():
    while True:
        now = datetime.now()
        days_until_sunday = (6 - now.weekday()) % 7
        if days_until_sunday == 0 and now.hour >= 3: days_until_sunday = 7
        target = now.replace(hour=3, minute=0, second=0, microsecond=0) + timedelta(days=days_until_sunday)
        await asyncio.sleep((target - now).total_seconds())
        try:
            docs = list(col_pdfs.find().sort("_id", -1))
            if docs:
                content = f"🛡 MASTER BACKUP [{now.strftime('%d/%m/%Y')}]\n" + "="*35 + "\n"
                for d in docs: content += f"CODE: {d.get('code')} | LINK: {d.get('link')}\n"
                with open("backup.txt", "w") as f: f.write(content)
                await bot.send_document(OWNER_ID, FSInputFile("backup.txt"), caption="🛡 **Master Backup for Master Sadiq.**")
                os.remove("backup.txt")
        except: pass

# ==========================================
# 🧠 PDF & DRIVE LOGIC
# ==========================================

def draw_canvas_extras(canvas, doc):
    canvas.saveState()
    canvas.translate(letter[0]/2, letter[1]/2); canvas.rotate(45); canvas.setFillColor(Color(0,0,0,alpha=0.08)) 
    canvas.setFont("Helvetica-Bold", 70); canvas.drawCentredString(0, 0, "MSANODE"); canvas.restoreState()
    canvas.saveState(); canvas.setFont("Helvetica", 9); canvas.setFillColor(gray)
    canvas.drawRightString(letter[0]-0.75*inch, 0.5*inch, f"MSANODE OFFICIAL GUIDE | Page {doc.page}"); canvas.restoreState()

def create_goldmine_pdf(text, filename):
    t = re.compile(r'[^\x00-\x7F]+').sub('', text)
    t = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', t)
    t = re.sub(r'__(.*?)__', r'<u>\1</u>', t)
    doc = SimpleDocTemplate(filename, pagesize=letter, leftMargin=0.75*inch, rightMargin=0.75*inch, topMargin=0.75*inch, bottomMargin=0.75*inch)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='GB', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=11, leading=16, alignment=4, spaceAfter=12))
    story = [Paragraph(p.strip().replace('\n', ' '), styles['GB']) for p in t.split('\n\n') if p.strip()]
    doc.build(story, onFirstPage=draw_canvas_extras, onLaterPages=draw_canvas_extras)

# ==========================================
# 🧠 SECURE DRIVE SERVICE (FIXED)
# ==========================================
def get_drive_service():
    creds = None
    # Check if token exists
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as t:
            creds = pickle.load(t)
            
    # If no valid creds, we cannot re-auth on Render
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(TOKEN_FILE, 'wb') as t:
                    pickle.dump(creds, t)
            except Exception as e:
                # This is where the 'invalid_grant' happens
                raise Exception("TOKEN_EXPIRED: Master Sadiq, your Drive Token is dead. Re-auth locally.")
        else:
            raise Exception("AUTH_MISSING: Credentials not found or invalid.")
            
    return build('drive', 'v3', credentials=creds)
def upload_to_drive(filename):
    service = get_drive_service()
    media = MediaIoBaseUpload(io.FileIO(filename, 'rb'), mimetype='application/pdf')
    file = service.files().create(body={'name': filename, 'parents': [PARENT_FOLDER_ID]}, media_body=media, fields='id, webViewLink').execute()
    service.permissions().create(fileId=file.get('id'), body={'type': 'anyone', 'role': 'reader'}).execute()
    return file.get('webViewLink')

# ==========================================
# 🤖 HANDLERS
# ==========================================
@dp.message(BotState.confirm_overwrite)
async def handle_overwrite_decision(message: types.Message, state: FSMContext):
    if message.text == "❌ NEW CODE":
        await message.answer("🔄 **Enter a different Project Code:**")
        await state.set_state(BotState.waiting_for_code)
        return
        
    if message.text == "✅ OVERWRITE":
        data = await state.get_data()
        code = data.get('pending_code')
        await state.update_data(code=code)
        await message.answer(
            f"🚀 **Overwriting `{code}`.**\n"
            "Master Sadiq, paste the new script contents:",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
        )
        await state.set_state(BotState.processing_script)
    else:
        await message.answer("Please use the buttons: ✅ OVERWRITE or ❌ NEW CODE")
@dp.message(Command("start"))
@dp.message(F.text == "🔙 Back to Menu")
async def start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer("💎 **MSANODE BOT 4**\nAt your service, Master Sadiq.", reply_markup=get_main_menu())

@dp.message(F.text == "📊 Storage Info")
# ==========================================
# 📊 STORAGE HANDLER (SECURED)
# ==========================================
@dp.message(F.text == "📊 Storage Info")
async def storage_info(message: types.Message):
    try:
        # 1. MongoDB Logic (Always Works)
        stats = db_client["MSANodeDB"].command("collstats", "pdf_library")
        m_count = stats.get('count', 0)
        m_used = stats.get('size', 0) / (1024 * 1024)
        m_limit = 512.0
        m_perc = (m_used / m_limit) * 100
        
        # 2. Drive Logic (Handles the Token Error)
        try:
            service = get_drive_service()
            about = service.about().get(fields="storageQuota").execute()
            quota = about.get('storageQuota', {})
            d_limit = int(quota.get('limit')) / (1024**3)
            d_used = int(quota.get('usage')) / (1024**3)
            d_perc = (d_used / d_limit) * 100
            drive_report = (
                f"☁️ **Google Drive (Files)**\n"
                f"Used: `{d_used:.2f} GB` / `{d_limit:.0f} GB`\n"
                f"`{generate_progress_bar(d_perc)}`"
            )
        except Exception as drive_err:
            logging.error(f"Drive Storage Error: {drive_err}")
            drive_report = "⚠️ **Drive Access Error:** Token Expired. Please re-authenticate locally and update Render."

        msg = (
            f"📊 **MASTER SADIQ'S STORAGE CENTER**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🍃 **MongoDB (Metadata)**\n"
            f"Records: `{m_count}`\n"
            f"Used: `{m_used:.2f} MB` / `{m_limit} MB`\n"
            f"`{generate_progress_bar(m_perc)}`\n\n"
            f"{drive_report}\n\n"
            f"✅ **System Status: Checking...**"
        )
        await message.answer(msg)
    except Exception as e:
        await message.answer(f"❌ Analytics Engine Failure: `{e}`")
@dp.message(F.text == "📄 Generate PDF")
async def gen_btn(message: types.Message, state: FSMContext):
    await state.update_data(raw_script="")
    await message.answer("📝 **Master Sadiq, enter Project Code:**", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True))
    await state.set_state(BotState.waiting_for_code)


# ==========================================
# 🛑 STEP 1: CODE INPUT & DUPLICATE CHECK
# ==========================================
@dp.message(BotState.waiting_for_code)
async def code_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    # Clean input and search MongoDB
    code = message.text.strip().upper()
    exists = col_pdfs.find_one({"code": code})
    
    if exists:
        # Save the code to memory while we ask for permission
        await state.update_data(pending_code=code)
        
        # Build the decision buttons
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="✅ OVERWRITE"), KeyboardButton(text="❌ NEW CODE"))
        
        await message.answer(
            f"⚠️ **ALERT:** Project `{code}` already exists in the Vault.\n"
            "Do you want to replace the old version with this new one?",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(BotState.confirm_overwrite)
        return

    # If it's a new code, proceed as normal
    await state.update_data(code=code)
    await message.answer(
        f"🖋 **Code `{code}` Registered.**\n"
        "Master Sadiq, paste your script now:",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
    )
    await state.set_state(BotState.processing_script)

# ==========================================
# 🛑 STEP 2: DECISION HANDLER
# ==========================================
@dp.message(BotState.confirm_overwrite)
async def handle_overwrite_decision(message: types.Message, state: FSMContext):
    if message.text == "❌ NEW CODE":
        await message.answer("🔄 **Enter a different Project Code:**")
        await state.set_state(BotState.waiting_for_code)
        return
        
    if message.text == "✅ OVERWRITE":
        data = await state.get_data()
        code = data.get('pending_code')
        await state.update_data(code=code) # Move the pending code to the active code slot
        await message.answer(
            f"🚀 **Overwriting `{code}`.**\n"
            "Master Sadiq, paste the new script contents:",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
        )
        await state.set_state(BotState.processing_script)
    else:
        # Fallback if they type something else
        await message.answer("Please use the buttons: ✅ OVERWRITE or ❌ NEW CODE")

@dp.message(BotState.processing_script, F.text)
async def merge_script(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    data = await state.get_data()
    updated = data.get('raw_script', '') + "\n\n" + message.text
    await state.update_data(raw_script=updated)
    
    # Check if a task is already running to prevent PermissionError spam
    if not data.get('timer_active'):
        await state.update_data(timer_active=True)
        
        async def auto_finish(uid, st):
            await asyncio.sleep(5) # Set to 5 seconds to give you time to paste everything
            await finalize_pdf(uid, st)
            
        asyncio.create_task(auto_finish(message.from_user.id, state))

async def finalize_pdf(user_id, state):
    data = await state.get_data()
    code, script = data.get('code'), data.get('raw_script', '').strip()
    if not script or not code: return
    
    msg = await bot.send_message(user_id, "💎 **Master Sadiq, building your guide...**")
    filename = f"{code}.pdf"
    
    try:
        # 1. Generate and Upload
        await asyncio.to_thread(create_goldmine_pdf, script, filename)
        link = await asyncio.to_thread(upload_to_drive, filename)
        
        # 2. DATABASE SYNC (Fixes Duplicate/Ghost Issue)
        # Delete any existing entry with this code before adding the new one
        col_pdfs.delete_many({"code": code}) 
        col_pdfs.insert_one({
            "code": code, 
            "link": link, 
            "timestamp": datetime.now()
        })
        
        # 3. Send to User
        await bot.send_document(
            user_id, 
            FSInputFile(filename), 
            caption=f"✅ **READY**\nCode: `{code}`\n🔗 **Link:** {link}"
        )
        
        # 4. WINDOWS SAFE CLEANUP (Fixes PermissionError)
        await asyncio.sleep(2) # Buffer for Windows file lock
        if os.path.exists(filename):
            try:
                os.remove(filename)
                print(f"◈ System: {filename} purged successfully.")
            except PermissionError:
                await asyncio.sleep(3)
                try:
                    os.remove(filename)
                except:
                    print(f"◈ Warning: {filename} locked by system. Janitor will clear it later.")
                
    except Exception as e: 
        await bot.send_message(user_id, f"❌ Error: `{e}`")
    
    # Ensure timer flag is reset and state is cleared
    await state.clear()
# ==========================================
# 📋 LIBRARY & MANAGEMENT HANDLERS
# ==========================================

@dp.message(F.text == "📋 Show Library")
async def list_library(message: types.Message):
    # Sort by most recent first
    docs = list(col_pdfs.find().sort("timestamp", -1))
    
    if not docs: 
        return await message.answer("📭 Library empty, Master Sadiq.")
    
    seen_codes = set()
    res = ["📋 **LIBRARY INDEX (SYNCED)**", "━━━━━━━━━━━━━━━━━━━━"]
    count = 1
    
    for d in docs:
        code = d.get('code')
        # Only list unique codes to prevent duplicate clutter
        if code and code not in seen_codes:
            timestamp = d.get('timestamp', datetime.now()).strftime('%d/%m')
            res.append(f"{count}. `{code}` — [{timestamp}]")
            seen_codes.add(code)
            count += 1
            if count > 25: break # Limit list length for Telegram
            
    res.append("━━━━━━━━━━━━━━━━━━━━")
    res.append("💎 *System: God-Mode filtered entries.*")
    await message.answer("\n".join(res), parse_mode="Markdown")

@dp.message(F.text == "🔗 Get Link")
async def link_btn(message: types.Message, state: FSMContext):
    await message.answer("🔍 Code, Master Sadiq:", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True))
    await state.set_state(BotState.fetching_link)

@dp.message(BotState.fetching_link)
async def fetch_link(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    code_query = message.text.strip().upper()
    # Pull the most recent entry for this code
    doc = col_pdfs.find_one({"code": code_query}, sort=[("timestamp", -1)])
    
    if doc: 
        await message.answer(f"✅ **RESOURCE FOUND**\nCode: `{doc.get('code')}`\n🔗 **Link:** {doc.get('link')}")
    else: 
        await message.answer(f"❌ `{code_query}` not found in index, Master Sadiq.")
    await state.clear()

@dp.message(F.text == "🗑 Remove PDF")
async def remove_btn(message: types.Message, state: FSMContext):
    await message.answer("🗑 Code to Delete:", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True))
    await state.set_state(BotState.deleting_pdf)

@dp.message(BotState.deleting_pdf)
async def delete_exec(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    code_query = message.text.strip().upper()
    # Delete all instances of this code to ensure clean library
    res = col_pdfs.delete_many({"code": code_query})
    
    if res.deleted_count > 0:
        await message.answer(f"🗑 **DELETED:** `{code_query}`\nRecords purged: {res.deleted_count}")
    else: 
        await message.answer(f"❌ No records found for `{code_query}`.")
    await state.clear()

@dp.message(F.text == "💎 Elite Help")
async def help_feature(message: types.Message):
    help_text = (
        "💎 <b>MSANODE BOT 4: OPERATIONAL MANUAL</b>\n"
        "<i>High-Performance PDF Infrastructure v4.0</i>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        
        "📑 <b>1. GENERATION ENGINE</b>\n"
        "• <b>📄 Generate PDF:</b> Initiates the build sequence. Enter a unique Project Code (e.g., P1) followed by your content.\n"
        "• <b>Overload Protection:</b> You can paste long scripts in multiple messages. The bot waits 5 seconds after your last paste to finalize.\n"
        "• <b>Overwrite Guard:</b> If a code exists, the bot will ask for permission before replacing data.\n\n"
        
        "🔍 <b>2. RETRIEVAL & MANAGEMENT</b>\n"
        "• <b>🔗 Get Link:</b> Instantly fetches the Google Drive URL for any Project Code in your vault.\n"
        "• <b>📋 Show Library:</b> Displays the last 25 unique projects with their generation dates.\n"
        "• <b>🗑 Remove PDF:</b> Permanently purges metadata from MongoDB. <i>Note: This does not delete the file from Drive for security.</i>\n\n"
        
        "📊 <b>3. SYSTEM ANALYTICS</b>\n"
        "• <b>📊 Storage Info:</b> Live tracking of your MongoDB records and Google Drive quota.\n"
        "• <b>💓 Pulse:</b> An automated hourly heartbeat to ensure database connectivity.\n\n"
        
        "🛡 <b>4. AUTOMATED PROTOCOLS</b>\n"
        "• <b>Monthly Vaults:</b> Files are auto-organized into folders by month (e.g., DEC_2025).\n"
        "• <b>Janitor:</b> Local storage is wiped every 24 hours to keep the server lightweight.\n"
        "• <b>Guardian:</b> Every Sunday at 3:00 AM, a full library backup (.txt) is sent to you.\n"
        "• <b>Daily Brief:</b> Every morning at 8:40 AM, a status report is delivered.\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "✅ <b>ALL SYSTEMS: OPTIMAL</b>"
    )
    
    await message.answer(help_text, parse_mode="HTML")
def upload_to_drive(filename):
    service = get_drive_service()
    
    # 1. GENERATE DYNAMIC FOLDER NAME
    month_name = datetime.now().strftime('%b_%Y').upper() # e.g., DEC_2025
    folder_name = f"{month_name}_GUIDES"
    
    # 2. CHECK IF FOLDER EXISTS
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and '{PARENT_FOLDER_ID}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    folders = results.get('files', [])
    
    if folders:
        target_folder_id = folders[0]['id']
    else:
        # 3. CREATE FOLDER IF NOT FOUND
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [PARENT_FOLDER_ID]
        }
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        target_folder_id = folder.get('id')
        print(f"◈ System: Created new monthly vault: {folder_name}")

    # 4. UPLOAD FILE TO THE TARGET FOLDER
    media = MediaIoBaseUpload(io.FileIO(filename, 'rb'), mimetype='application/pdf')
    file_metadata = {'name': filename, 'parents': [target_folder_id]}
    file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
    
    # Set public permissions
    service.permissions().create(fileId=file.get('id'), body={'type': 'anyone', 'role': 'reader'}).execute()
    
    return file.get('webViewLink')
# ==========================================
# 🚀 CORE INITIALIZATION
# ==========================================

async def main():
    # Force reset of old stuck sessions
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_my_commands([BotCommand(command="start", description="Menu")])
    
    # Initialize Automated Background Tasks
    asyncio.create_task(auto_janitor())
    asyncio.create_task(weekly_backup())
    asyncio.create_task(system_guardian())
    asyncio.create_task(daily_briefing())
    asyncio.create_task(hourly_pulse())
    
    print("💎 MSANODE BOT 4 ONLINE")
    
    try: 
        await bot.send_message(OWNER_ID, "🚀 **God Mode Online.**\nInfrastructure reset and ready, Master Sadiq.", parse_mode="HTML")
    except Exception as e:
        print(f"Startup notify failed: {e}")

    # Polling Loop with Safety Shield
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    print("🚀 STARTING INDIVIDUAL CORE TEST: BOT 4")
    
    # 1. Prepare Google Drive Secrets first
    prepare_secrets()
    
    # 2. Start Health Server in background
    threading.Thread(target=run_health_server, daemon=True).start()
    
    # 3. Launch Bot 4
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("◈ Bot 4 Shutdown.")



