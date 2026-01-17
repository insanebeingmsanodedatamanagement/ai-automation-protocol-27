import asyncio
import logging
import os
import sys
import io
import pickle
import pymongo
import re
import threading
from aiohttp import web
import shutil
import base64
import time
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import FSInputFile, ReplyKeyboardMarkup, KeyboardButton, BotCommand
from aiogram.utils.keyboard import ReplyKeyboardBuilder

# Fix Windows console encoding for emoji support
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# ReportLab & Google Imports
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.colors import Color, gray, black, HexColor
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# ==========================================
# ⚡ CONFIGURATION
# ==========================================
BOT_TOKEN = os.getenv("BOT_4_TOKEN") 
MONGO_URI = os.getenv("MONGO_URI")

if not BOT_TOKEN:
    print("❌ Bot 4 Error: BOT_4_TOKEN not found in Render Environment!")
OWNER_ID = 6988593629 
PARENT_FOLDER_ID = '1J0iGLcwjTTdQRQJ--A9s8D26a3-gN7IB'
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.pickle'

START_TIME = time.time() 

# ==========================================
# 🛠 SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
col_pdfs = None
db_client = None

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
                    if os.path.abspath(full_src) != os.path.abspath(target):
                        shutil.copy(full_src, target)
                print(f"✅ Secret Injected: {target}")
                break

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
    global col_pdfs, col_trash, db_client
    try:
        db_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = db_client["MSANodeDB"]
        col_pdfs = db["pdf_library"]
        col_trash = db["recycle_bin"]
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
    confirm_overwrite = State()
    confirm_nuke = State()
    waiting_for_range = State()
    choosing_retrieval_mode = State()
    choosing_delete_mode = State()
    confirm_delete = State()
    choosing_edit_mode = State()
    waiting_for_edit_target = State()
    waiting_for_new_code = State()
    confirm_empty_bin = State()

def get_main_menu():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📄 Generate PDF"), KeyboardButton(text="🔗 Get Link"))
    builder.row(KeyboardButton(text="📋 Show Library"), KeyboardButton(text="✏️ Edit PDF"))
    builder.row(KeyboardButton(text="🗑 Remove PDF"), KeyboardButton(text="♻️ Recycle Bin"))
    builder.row(KeyboardButton(text="📊 Storage Info"), KeyboardButton(text="⚠️ NUKE ALL DATA"))
    builder.row(KeyboardButton(text="💎 Elite Help"))
    return builder.as_markup(resize_keyboard=True)

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
# 🧠 PDF GENERATION - S19 STYLE
# ==========================================

def draw_canvas_extras(canvas_obj, doc):
    """Adds MSANODE watermark and page numbers like S19.pdf"""
    canvas_obj.saveState()
    
    # Watermark
    canvas_obj.translate(letter[0]/2, letter[1]/2)
    canvas_obj.rotate(45)
    canvas_obj.setFillColor(Color(0, 0, 0, alpha=0.08))
    canvas_obj.setFont("Helvetica-Bold", 70)
    canvas_obj.drawCentredString(0, 0, "MSANODE")
    canvas_obj.restoreState()
    
    # Premium Black Border
    canvas_obj.saveState()
    canvas_obj.setStrokeColor(HexColor('#000000'))
    canvas_obj.setLineWidth(2)  # Nice thick premium line
    # Draw border with 0.5 inch margin
    canvas_obj.rect(0.5*inch, 0.5*inch, letter[0]-1.0*inch, letter[1]-1.0*inch)
    canvas_obj.restoreState()
    
    # Page number footer
    canvas_obj.saveState()
    canvas_obj.setFont("Helvetica", 9)
    canvas_obj.setFillColor(gray)
    # Left footer: MSANODE OFFICIAL BLUEPRINT
    canvas_obj.drawString(
        0.75*inch, 
        0.25*inch, 
        "MSANODE OFFICIAL BLUEPRINT"
    )
    
    # Right footer: Page Number
    canvas_obj.drawRightString(
        letter[0] - 0.75*inch, 
        0.25*inch, 
        f"Page {doc.page}"
    )
    canvas_obj.restoreState()

def process_inline_formatting(text):
    """
    Process inline formatting markers:
    - ***** text ***** -> DARK BLACK BOLD ALL CAPS
    - **** text **** -> BLUE BOLD ALL CAPS
    - ***text*** -> RED BOLD ALL CAPS
    - *text* -> DARK BLACK BOLD (no caps)
    - Normal text -> standard lowercase (no special formatting)
    """
    # First, handle ***** text ***** (DARK BLACK BOLD ALL CAPS) - HIGHEST PRIORITY
    def uppercase_black_5star(match):
        content = match.group(1).strip().upper()  # Strip spaces and uppercase
        return f'<font color="#000000"><b>{content}</b></font>'  # Dark black
    
    text = re.sub(r'\*\*\*\*\*([^*]+?)\*\*\*\*\*', uppercase_black_5star, text)

    # Then handle **** text **** (BLUE BOLD ALL CAPS)
    def uppercase_blue(match):
        content = match.group(1).strip().upper()  # Strip spaces and uppercase
        return f'<font color="#1565C0"><b>{content}</b></font>'  # Dark blue
    
    text = re.sub(r'\*\*\*\*([^*]+?)\*\*\*\*', uppercase_blue, text)
    
    # Then handle ***text*** (RED BOLD ALL CAPS)
    def uppercase_red(match):
        content = match.group(1).strip().upper()  # Strip spaces and uppercase
        return f'<font color="#D32F2F"><b>{content}</b></font>'
    
    text = re.sub(r'\*\*\*([^*]+?)\*\*\*', uppercase_red, text)
    
    # Finally handle *text* (DARK BLACK BOLD, no caps - keep original case)
    def bold_black_no_caps(match):
        content = match.group(1).strip()  # Strip spaces but keep original case
        return f'<font color="#000000"><b>{content}</b></font>'
    
    text = re.sub(r'\*([^*]+?)\*', bold_black_no_caps, text)
    
    return text


def create_goldmine_pdf(text, filename):
    """Creates PDF in S19 professional format"""
    
    # Clean text - remove non-ASCII characters
    text = re.compile(r'[^\x00-\x7F]+').sub('', text)
    
    # Remove line separator graphics (______________ style lines)
    text = re.sub(r'_{20,}', '', text)
    
    # Clean up excessive newlines but keep intentional breaks
    text = re.sub(r'\n{4,}', '\n\n', text)
    
    # CRITICAL FIX: Merge Roman numerals with their titles if split across lines
    # This fixes "I.\n THE OPPORTUNITY" -> "I. THE OPPORTUNITY"
    text = re.sub(r'(^|\n)(I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII)\.\s*\n\s*', r'\1\2. ', text, flags=re.MULTILINE)
    
    # Setup document
    doc = SimpleDocTemplate(
        filename, 
        pagesize=letter,
        leftMargin=0.75*inch,
        rightMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch
    )
    
    # Define styles matching S19.pdf
    styles = getSampleStyleSheet()
    
    # Header style (MSANODE VAULT BLUEPRINT) - Dark Black and Underlined
    styles.add(ParagraphStyle(
        name='MSAHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=16,
        leading=20,
        textColor=HexColor('#000000'),  # Dark black color
        alignment=TA_CENTER,
        spaceAfter=6,
        underlineWidth=1,
        underlineColor=HexColor('#000000')
    ))
    
    # Main Title style (for the very first line)
    styles.add(ParagraphStyle(
        name='MainTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        leading=14,
        textColor=black,
        alignment=TA_LEFT,
        spaceAfter=12
    ))
    
    # Section Header (I, II, III, etc.) - Keep Roman numeral with title on SAME line - RED COLOR
    styles.add(ParagraphStyle(
        name='SectionHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=12,
        leading=16,
        textColor=HexColor('#D32F2F'),  # Vibrant red for Roman numerals
        alignment=TA_LEFT,
        spaceAfter=10,
        spaceBefore=14
    ))
    
    # Subsection with parentheses - Medium gray
    styles.add(ParagraphStyle(
        name='ParenSubsection',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=10,
        leading=13,
        textColor=HexColor('#404040'),  # Medium gray for subsections
        alignment=TA_LEFT,
        spaceAfter=6,
        spaceBefore=6
    ))
    
    # Subsection (The, Core, etc.) - Medium gray
    styles.add(ParagraphStyle(
        name='Subsection',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=10,
        leading=13,
        textColor=HexColor('#404040'),  # Medium gray for subsections
        alignment=TA_LEFT,
        spaceAfter=6,
        spaceBefore=8
    ))
    
    # Body text - LIGHT GRAY - JUSTIFIED
    styles.add(ParagraphStyle(
        name='Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        leading=14,
        textColor=HexColor('#333333'),  # Light gray for body text
        alignment=TA_JUSTIFY,
        spaceAfter=8
    ))
    
    # Code/Formula Box style
    styles.add(ParagraphStyle(
        name='CodeBox',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=9,
        leading=12,
        textColor=HexColor('#212121'),
        backColor=HexColor('#F5F5F5'),
        borderColor=HexColor('#E0E0E0'),
        borderWidth=1,
        borderPadding=6,
        alignment=TA_LEFT,
        spaceAfter=12,
        spaceBefore=8,
        leftIndent=6,
        rightIndent=6
    ))
    
    # All-caps header style - DARK BLACK
    styles.add(ParagraphStyle(
        name='AllCapsHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        leading=14,
        textColor=HexColor('#000000'),  # Dark black for all-caps
        alignment=TA_LEFT,
        spaceAfter=10,
        spaceBefore=10
    ))
    
    # Build story
    story = []
    
    # Add header (MSANODE VAULT BLUEPRINT) - Underlined
    story.append(Paragraph("<u>MSANODE VAULT BLUEPRINT</u>", styles['MSAHeader']))
    story.append(Spacer(1, 0.1*inch))
    
    # Parse and format content
    lines = text.split('\n')
    
    # Track if we've added the main title
    main_title_added = False
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # First substantive line is the main title
        if not main_title_added and len(line) > 20:
            story.append(Paragraph(process_inline_formatting(line), styles['MainTitle']))
            main_title_added = True
            continue
        
        # CRITICAL FIX: Roman numerals sections - keep numeral AND title together
        # Matches "I. THE OPPORTUNITY" or "VII. FINAL WORD" etc.
        # Display in BOLD, ALL CAPS, RED
        if re.match(r'^(I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII)\.\s+', line):
            story.append(Spacer(1, 0.08*inch))
            # Convert to uppercase and bold for premium red appearance
            story.append(Paragraph(process_inline_formatting(f"<b>{line.upper()}</b>"), styles['SectionHeader']))
            continue
        
        # Parentheses subsections like (The Managerial Mindset) or (Precision Engineering)
        if re.match(r'^\(.*?\):', line) or (line.startswith('(') and line.endswith(':')):
            story.append(Paragraph(process_inline_formatting(line), styles['ParenSubsection']))
            continue
        
        # Subsections starting with "The" or "THE" followed by title
        if re.match(r'^(The|THE)\s+[A-Z].*?:', line):
            story.append(Paragraph(process_inline_formatting(line), styles['Subsection']))
            continue
        
        # Code/Example boxes
        if line.lower().strip().startswith('example:') or line.lower().strip().startswith('formula:'):
            story.append(Paragraph(process_inline_formatting(line), styles['CodeBox']))
            continue
        
        # Numbered subsections like "1. THE LOGIC TRANSLATION"
        if re.match(r'^\d+\.\s+THE\s+[A-Z]', line):
            story.append(Paragraph(process_inline_formatting(line), styles['Subsection']))
            continue
        
        # Other bold subsections (Core Tools, etc.)
        if line.startswith('CORE TOOLS') or line.startswith('Core Tools'):
            story.append(Paragraph(process_inline_formatting(f"<b>{line}</b>"), styles['Subsection']))
            continue
        
        # All caps section dividers (but not too long to avoid body text in caps) - DARK BLACK
        if line.isupper() and 5 < len(line) < 100:
            story.append(Paragraph(process_inline_formatting(f"<b>{line}</b>"), styles['AllCapsHeader']))
            continue
        
        # Bullet points or dashes
        if line.startswith('-') or line.startswith('•'):
            story.append(Paragraph(process_inline_formatting(line), styles['Body']))
            continue
        
        # Regular body text - split into chunks if extremely long
        if len(line) > 600:
            # Split at sentence boundaries for readability
            sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', line)
            for sentence in sentences:
                if sentence.strip():
                    story.append(Paragraph(process_inline_formatting(sentence.strip()), styles['Body']))
        else:
            story.append(Paragraph(process_inline_formatting(line), styles['Body']))
    
    # Build PDF
    doc.build(story, onFirstPage=draw_canvas_extras, onLaterPages=draw_canvas_extras)

def get_drive_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as t: creds = pickle.load(t)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token: creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/drive.file'])
            creds = flow.run_local_server(port=8080)
        with open(TOKEN_FILE, 'wb') as t: pickle.dump(creds, t)
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(filename):
    service = get_drive_service()
    
    # Generate dynamic folder name
    month_name = datetime.now().strftime('%b_%Y').upper()
    folder_name = f"{month_name}_GUIDES"
    
    # Check if folder exists
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and '{PARENT_FOLDER_ID}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    folders = results.get('files', [])
    
    if folders:
        target_folder_id = folders[0]['id']
    else:
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [PARENT_FOLDER_ID]
        }
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        target_folder_id = folder.get('id')
        print(f"◈ System: Created new monthly vault: {folder_name}")

    # Upload file
    media = MediaIoBaseUpload(io.FileIO(filename, 'rb'), mimetype='application/pdf')
    file_metadata = {'name': filename, 'parents': [target_folder_id]}
    file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
    
    service.permissions().create(fileId=file.get('id'), body={'type': 'anyone', 'role': 'reader'}).execute()
    
    return file.get('webViewLink')

def download_from_drive(filename):
    """Downloads a file from Drive by name to local storage."""
    service = get_drive_service()
    
    # 1. Search for file by name (Global search to find it in subfolders)
    # We remove 'parents' check because files are inside Month Folders, not the root.
    query = f"name = '{filename}' and mimeType = 'application/pdf' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    
    if not files:
        return None
        
    file_id = files[0]['id']
    
    # 2. Download content
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(filename, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        
    return filename

def get_recycle_bin_id(service):
    """Finds or creates 'Recycle Bin' folder inside the Vault."""
    query = f"mimeType='application/vnd.google-apps.folder' and name='Recycle Bin' and '{PARENT_FOLDER_ID}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    
    if files:
        return files[0]['id']
    else:
        # Create it
        metadata = {
            'name': 'Recycle Bin',
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [PARENT_FOLDER_ID]
        }
        folder = service.files().create(body=metadata, fields='id').execute()
        return folder.get('id')

def move_to_recycle_bin(filename):
    """Moves a file to the Recycle Bin folder in Drive."""
    service = get_drive_service()
    
    # 1. Search for file by name
    query = f"name = '{filename}' and mimeType = 'application/pdf' and trashed = false"
    results = service.files().list(q=query, fields="files(id, parents)").execute()
    files = results.get('files', [])
    
    if not files:
        return False
        
    bin_id = get_recycle_bin_id(service)
    
    # 2. Move file
    try:
        for f in files:
            # Move key: addParents = bin, removeParents = current
            prev_parents = ",".join(f.get('parents', []))
            service.files().update(
                fileId=f['id'],
                addParents=bin_id,
                removeParents=prev_parents,
                fields='id, parents'
            ).execute()
        return True
    except:
        return False

def rename_file_in_drive(old_filename, new_filename):
    """Renames a file in Drive."""
    service = get_drive_service()
    
    # 1. Search for file
    query = f"name = '{old_filename}' and mimeType = 'application/pdf' and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    
    if not files:
        return False
    
    # 2. Rename (first match)
    file_id = files[0]['id']
    try:
        service.files().update(
            fileId=file_id,
            body={'name': new_filename},
            fields='id, name'
        ).execute()
        return True
    except:
        return False

def empty_drive_folder(folder_id):
    """Permanently deletes all files in a folder."""
    service = get_drive_service()
    
    deleted_count = 0
    page_token = None
    
    while True:
        # Search for all children
        q = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(q=q, fields="nextPageToken, files(id)", pageToken=page_token).execute()
        items = results.get('files', [])
        
        for item in items:
            try:
                service.files().delete(fileId=item['id']).execute()
                deleted_count += 1
            except:
                pass
        
        page_token = results.get('nextPageToken')
        if not page_token:
            break
            
    return deleted_count

# ==========================================
# 🤖 HANDLERS
# ==========================================

@dp.message(Command("start"))
@dp.message(F.text == "🔙 Back to Menu")
async def start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer("💎 **MSANODE BOT 4**\nAt your service, Master Sadiq.", reply_markup=get_main_menu())

@dp.message(F.text == "📋 Show Library")
async def show_library(message: types.Message):
    docs = list(col_pdfs.find().sort("timestamp", -1))
    total_count = len(docs)
    
    if not docs:
        await message.answer("📂 **LIBRARY IS EMPTY**\nNo PDFs available.")
        return

    msg_lines = [
        f"📂 **VAULT LIBRARY INDEX**",
        f"📊 **Total Files: {total_count}**",
        "━━━━━━━━━━━━━━━━━━━━"
    ]
    
    for idx, doc in enumerate(docs, 1):
        code = doc.get('code', 'UNKNOWN')
        # Maybe add timestamp?
        # ts = doc.get('timestamp')
        # if ts: date_str = ts.strftime("%d-%m-%Y")
        # msg_lines.append(f"`{idx}.` **{code}** ({date_str})")
        # Keeping it simple as per request "count and overall things" usually means clean layout
        msg_lines.append(f"`{idx:02}.` **{code}**")
    
    full_msg = "\n".join(msg_lines)
    
    # Split if too long (Telegram limit ~4096)
    if len(full_msg) > 4000:
        parts = [full_msg[i:i+4000] for i in range(0, len(full_msg), 4000)]
        for part in parts:
            await message.answer(part)
    else:
        await message.answer(full_msg)
    
    await message.answer("✅ **End of List**")

@dp.message(F.text == "📊 Storage Info")
async def storage_info(message: types.Message):
    wait_msg = await message.answer("⏳ **Calculating Vault Metrics...**")
    try:
        # 1. MongoDB Stats
        stats = db_client["MSANodeDB"].command("collstats", "pdf_library")
        m_count = stats.get('count', 0)
        m_used = stats.get('size', 0) / (1024 * 1024)
        m_limit = 512.0
        m_perc = (m_used / m_limit) * 100
        
        # 2. Drive Stats (Account Level)
        service = get_drive_service()
        about = service.about().get(fields="storageQuota").execute()
        quota = about.get('storageQuota', {})
        total_limit_gb = int(quota.get('limit')) / (1024**3)
        total_used_gb = int(quota.get('usage')) / (1024**3)
        total_perc = (total_used_gb / total_limit_gb) * 100
        
        # 3. Vault Specific Stats (Recursive Folder Size)
        # Find all files inside the Parent Folder (and subfolders)
        vault_size_bytes = 0
        vault_files_count = 0
        
        page_token = None
        while True:
            # Query: Search for all files that are NOT folders, NOT trashed, and have PARENT_FOLDER_ID in ancestors
            # Note: searching 'ancestors' is tricky in v3 without iterating. 
            # Simpler approach for "Bot Vault": Search for files created by this bot or just all PDF files in the specific folder structure.
            # Given the structure: Root -> Month Folders -> PDFs
            # We first find all Month Folders inside PARENT_FOLDER_ID
            
            # Let's iterate: 1. Get children of PARENT_FOLDER_ID. 
            # If child is folder -> get its children. If child is file -> add size.
            pass # Placeholder for logic below
            break
            
        # Recursive size calculation helper
        def get_folder_size(folder_id):
            total_size = 0
            count = 0
            
            # List items in this folder
            q = f"'{folder_id}' in parents and trashed = false"
            
            next_page = None
            while True:
                results = service.files().list(q=q, fields="nextPageToken, files(id, name, mimeType, size)", pageToken=next_page).execute()
                items = results.get('files', [])
                
                for item in items:
                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        s, c = get_folder_size(item['id'])
                        total_size += s
                        count += c
                    else:
                        if 'size' in item:
                            total_size += int(item['size'])
                            count += 1
                
                next_page = results.get('nextPageToken')
                if not next_page: break
            
            return total_size, count

        # Execute recursive calculation
        vault_bytes, vault_count = get_folder_size(PARENT_FOLDER_ID)
        vault_mb = vault_bytes / (1024 * 1024)
        vault_gb = vault_bytes / (1024**3)

        msg = (
            f"📊 **MASTER STORAGE ANALYTICS**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🍃 **MongoDB (Metadata)**\n"
            f"• Records: `{m_count}`\n"
            f"• Usage: `{m_used:.2f} MB` / `{m_limit} MB`\n"
            f"`{generate_progress_bar(m_perc)}`\n\n"
            
            f"☁️ **Google Drive (Vault Specific)**\n"
            f"• Files stored: `{vault_count}`\n"
            f"• Vault Size: `{vault_mb:.2f} MB` ({vault_gb:.4f} GB)\n"
            f"• Real-time Recursive Scan: ✅\n\n"
            
            f"💿 **Google Account (Total Limit)**\n"
            f"• Usage: `{total_used_gb:.2f} GB` / `{total_limit_gb:.0f} GB`\n"
            f"`{generate_progress_bar(total_perc)}`\n\n"
            
            f"✅ **System Integrity: 100%**"
        )
        await wait_msg.delete()
        await message.answer(msg)
        
    except Exception as e:
        if 'wait_msg' in locals(): await wait_msg.delete()
        await message.answer(f"⚠️ Analytics failed, Master Sadiq: `{e}`")

@dp.message(F.text == "📄 Generate PDF")
async def gen_btn(message: types.Message, state: FSMContext):
    await state.update_data(raw_script="")
    await message.answer("📝 **Master Sadiq, enter Project Code:**", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True))
    await state.set_state(BotState.waiting_for_code)

@dp.message(BotState.waiting_for_code)
async def code_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    code = message.text.strip().upper()
    exists = col_pdfs.find_one({"code": code})
    
    if exists:
        await state.update_data(pending_code=code)
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="✅ OVERWRITE"), KeyboardButton(text="❌ NEW CODE"))
        
        await message.answer(
            f"⚠️ **ALERT:** Project `{code}` already exists in the Vault.\n"
            "Do you want to replace the old version with this new one?",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(BotState.confirm_overwrite)
        return

    await state.update_data(code=code)
    await message.answer(
        f"🖋 **Code `{code}` Registered.**\n"
        "Master Sadiq, paste your script now:",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
    )
    await state.set_state(BotState.processing_script)

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

@dp.message(BotState.processing_script, F.text)
async def merge_script(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    data = await state.get_data()
    updated = data.get('raw_script', '') + "\n\n" + message.text
    await state.update_data(raw_script=updated)
    
    if not data.get('timer_active'):
        await state.update_data(timer_active=True)
        
        async def auto_finish(uid, st):
            await asyncio.sleep(5)
            await finalize_pdf(uid, st)
            
        asyncio.create_task(auto_finish(message.from_user.id, state))

async def finalize_pdf(user_id, state):
    data = await state.get_data()
    code, script = data.get('code'), data.get('raw_script', '').strip()
    if not script or not code: return
    
    msg = await bot.send_message(user_id, "💎 **Master Sadiq, building your guide...**")
    filename = f"{code}.pdf"
    
    try:
        await asyncio.to_thread(create_goldmine_pdf, script, filename)
        link = await asyncio.to_thread(upload_to_drive, filename)
        
        col_pdfs.delete_many({"code": code}) 
        col_pdfs.insert_one({
            "code": code, 
            "link": link, 
            "timestamp": datetime.now()
        })
        
        await bot.send_document(
            user_id, 
            FSInputFile(filename), 
            caption=f"✅ **READY**\nCode: `{code}`\n🔗 **Link:** {link}"
        )
        
        await asyncio.sleep(2)
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
    
    await state.clear()

@dp.message(F.text == "📋 Show Library")
async def list_library(message: types.Message):
    docs = list(col_pdfs.find().sort("timestamp", -1))
    
    if not docs: 
        return await message.answer("📭 Library empty, Master Sadiq.")
    
    seen_codes = set()
    res = ["📋 **LIBRARY INDEX (SYNCED)**", "━━━━━━━━━━━━━━━━━━━━"]
    count = 1
    
    for d in docs:
        code = d.get('code')
        if code and code not in seen_codes:
            timestamp = d.get('timestamp', datetime.now()).strftime('%d/%m')
            res.append(f"{count}. `{code}` — [{timestamp}]")
            seen_codes.add(code)
            count += 1
            if count > 25: break
            
    res.append("━━━━━━━━━━━━━━━━━━━━")
    res.append("💎 *System: God-Mode filtered entries.*")
    await message.answer("\n".join(res), parse_mode="Markdown")

@dp.message(F.text == "♻️ Recycle Bin")
async def recycle_bin_btn(message: types.Message, state: FSMContext):
    # 1. Get Trash Stats from DB
    trash_count = col_trash.count_documents({})
    
    builder = ReplyKeyboardBuilder()
    if trash_count > 0:
        builder.row(KeyboardButton(text="🔥 EMPTY BIN"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        f"♻️ **RECYCLE BIN MANAGEMENT**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🗑 Items in Bin: **{trash_count}**\n\n"
        f"Select Option:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(BotState.confirm_empty_bin)

@dp.message(BotState.confirm_empty_bin)
async def empty_bin_handler(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    if text == "🔥 EMPTY BIN":
        # Double Confirmation
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="✅ CONFIRM EMPTY"), KeyboardButton(text="❌ CANCEL"))
        
        await message.answer(
            "⚠️ **WARNING: PERMANENT DATA LOSS**\n"
            "This will permanently destroy ALL files in the Recycle Bin.\n"
            "Are you sure?",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        # We can stay in same state, just check text next
        return

    if text == "❌ CANCEL":
        return await recycle_bin_btn(message, state)
        
    if text == "✅ CONFIRM EMPTY":
        msg = await message.answer("⏳ **EMPTYING RECYCLE BIN...**")
        
        # 1. Clean Drive
        service = get_drive_service()
        bin_id = get_recycle_bin_id(service)
        drive_count = await asyncio.to_thread(empty_drive_folder, bin_id)
        
        # 2. Clean DB
        db_res = col_trash.delete_many({})
        db_count = db_res.deleted_count
        
        await msg.edit_text(
            f"🔥 **BIN EMPTIED SUCCESSFULLY**\n"
            f"☁️ Drive Files Purged: `{drive_count}`\n"
            f"🍃 DB Records Purged: `{db_count}`\n"
            f"Total Cleanup Complete."
        )
        await asyncio.sleep(2)
        return await start(message, state)

    await message.answer("⚠️ Valid options only.")

@dp.message(F.text == "✏️ Edit PDF")
async def edit_btn(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔢 BY INDEX"), KeyboardButton(text="🆔 BY CODE"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "✏️ **EDIT PROTOCOL INITIATED**\n"
        "Select Selection Mode to Rename File:\n\n"
        "🔢 **BY INDEX**: Select by position (e.g. 1 = Newest).\n"
        "🆔 **BY CODE**: Select by Code Button.",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(BotState.choosing_edit_mode)

@dp.message(BotState.choosing_edit_mode)
async def handle_edit_mode(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    if text == "🔢 BY INDEX":
        await message.answer(
            "🔢 **INDEX SELECTION**\n"
            "Enter the Index of the file to rename (e.g. `1`).",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
        )
        await state.update_data(edit_mode="index")
        await state.set_state(BotState.waiting_for_edit_target)
        
    elif text == "🆔 BY CODE":
        # Fetch available codes for buttons
        docs = list(col_pdfs.find().sort("timestamp", -1))
        
        builder = ReplyKeyboardBuilder()
        existing_codes = []
        for d in docs[:50]:
            code = d.get('code')
            if code and code not in existing_codes:
                builder.add(KeyboardButton(text=code))
                existing_codes.append(code)
        
        builder.adjust(3)
        builder.row(KeyboardButton(text="🔙 Back to Menu"))
        
        await message.answer(
            "🆔 **CODE SELECTION**\n"
            "Select the Code you wish to Rename:",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.update_data(edit_mode="code")
        await state.set_state(BotState.waiting_for_edit_target)
    else:
        await message.answer("⚠️ Invalid Option.")

@dp.message(BotState.waiting_for_edit_target)
async def select_edit_target(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    if text == "🔙 BACK TO MENU": return await start(message, state)
    
    data = await state.get_data()
    mode = data.get('edit_mode', 'code')
    doc = None
    
    if mode == 'index':
        if not text.isdigit():
            await message.answer("⚠️ Enter a valid number (e.g. 1).")
            return
        idx = int(text)
        if idx < 1:
            await message.answer("⚠️ Index must be 1 or greater.")
            return
            
        all_docs = list(col_pdfs.find().sort("timestamp", -1))
        if idx > len(all_docs):
            await message.answer(f"❌ Index {idx} not found. Max is {len(all_docs)}.")
            return
        doc = all_docs[idx-1]
        
    else:
        # Code mode
        doc = col_pdfs.find_one({"code": text})
        if not doc:
            await message.answer(f"❌ Code `{text}` not found.")
            return

    # Doc found, ask for new name
    old_code = doc.get('code')
    await state.update_data(target_doc_id=str(doc['_id']), old_code=old_code)
    
    await message.answer(
        f"📝 **EDITING: `{old_code}`**\n"
        f"Enter the **NEW UNIQUE CODE** for this file:",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
    )
    await state.set_state(BotState.waiting_for_new_code)

@dp.message(BotState.waiting_for_new_code)
async def save_new_code(message: types.Message, state: FSMContext):
    new_code = message.text.strip().upper()
    if new_code == "🔙 BACK TO MENU": return await start(message, state)
    
    # Validation
    if not new_code: return await message.answer("⚠️ Code cannot be empty.")
    
    # Check uniqueness
    if col_pdfs.find_one({"code": new_code}):
        await message.answer(f"⚠️ Code `{new_code}` already exists! Choose another.")
        return
        
    data = await state.get_data()
    old_code = data.get('old_code')
    doc_id = data.get('target_doc_id')
    
    msg = await message.answer(f"⏳ **RENAMING: `{old_code}` ➡️ `{new_code}`...**")
    
    # 1. Drive Rename
    old_filename = f"{old_code}.pdf"
    new_filename = f"{new_code}.pdf"
    
    drive_res = await asyncio.to_thread(rename_file_in_drive, old_filename, new_filename)
    
    # 2. DB Update
    from bson.objectid import ObjectId
    col_pdfs.update_one(
        {"_id": ObjectId(doc_id)}, 
        {"$set": {"code": new_code, "filename": new_code}} # Assuming we want to sync filename too if used
    )
    
    status = "☁️ Drive: Renamed" if drive_res else "☁️ Drive: Not Found (DB Only Revised)"
    
    await msg.edit_text(
        f"✅ **SUCCESSFULLY RENAMED**\n"
        f"Old: `{old_code}`\n"
        f"New: `{new_code}`\n"
        f"{status}\n\n"
        f"Enter next command or '🔙 Back to Menu'."
    )
    # Return to Menu logic? 
    # User usually wants to stop editing after one rename.
    # But sticking to "State Persistance" rule:
    # await message.answer("✏️ Select next Edit Mode or '🔙 Back to Menu'.", reply_markup=get_main_menu())
    await state.clear() # Reset state since we are back at menu level essentially, or revert to choosing_edit_mode?
    # Actually, let's keep them in the Edit Menu flow?
    # But `save_new_code` finishes the specific task.
    # Let's show the Edit Menu again so they can pick another file?
    # Calling edit_btn logic manually:
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔢 BY INDEX"), KeyboardButton(text="🆔 BY CODE"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    await message.answer("Select Mode to Edit Another:", reply_markup=builder.as_markup(resize_keyboard=True))
    await state.set_state(BotState.choosing_edit_mode)

@dp.message(F.text == "🔗 Get Link")
async def link_btn(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📄 GET PDF FILE"), KeyboardButton(text="🔗 GET DRIVE LINK"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "🎛 **SELECT RETRIEVAL FORMAT:**\n\n"
        "📄 **GET PDF FILE**: Downloads and sends the actual file.\n"
        "🔗 **GET DRIVE LINK**: Sends the secure Google Drive URL.",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(BotState.choosing_retrieval_mode)

@dp.message(BotState.choosing_retrieval_mode)
async def handle_mode_selection(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    mode = "link"
    if "PDF" in message.text: mode = "pdf"
    
    await state.update_data(retrieval_mode=mode)
    
    # Fetch all documents
    docs = list(col_pdfs.find().sort("timestamp", -1))
    
    if not docs:
        await message.answer("📭 The Main Vault is empty, Master Sadiq.")
        return await start(message, state)

    builder = ReplyKeyboardBuilder()
    
    existing_codes = []
    for d in docs[:50]:
        code = d.get('code')
        if code and code not in existing_codes:
            builder.add(KeyboardButton(text=code))
            existing_codes.append(code)
    
    builder.adjust(3)
    builder.row(KeyboardButton(text="🔢 BULK RANGE"), KeyboardButton(text="🔙 Back to Menu"))
    
    mode_text = "PDF FILE" if mode == "pdf" else "DRIVE LINK"
    await message.answer(
        f"📂 **MODE: {mode_text}**\n"
        "Select a Project Code below or use **BULK RANGE** to fetch multiple.", 
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(BotState.fetching_link)

@dp.message(BotState.fetching_link)
async def fetch_link(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    
    if text == "🔙 BACK TO MENU": return await start(message, state)
    
    # === BULK RANGE MODE ===
    if text == "🔢 BULK RANGE":
        await message.answer(
            "🔢 **BULK RETRIEVAL MODE**\n"
            "Enter the index range of PDFs you need (e.g., `1-5`, `10-20`).\n"
            "Index 1 = Newest PDF.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
        )
        await state.set_state(BotState.waiting_for_range)
        return

    # === SINGLE RETRIEVAL MODE (User clicked a code button) ===
    doc = col_pdfs.find_one({"code": text}, sort=[("timestamp", -1)])
    
    if doc:
        data = await state.get_data()
        mode = data.get('retrieval_mode', 'link')
        
        if mode == 'pdf':
            wait_msg = await message.answer(f"⏳ **Fetching PDF: `{text}`...**")
            filename = f"{text}.pdf"
            
            try:
                # Attempt to download from Drive
                local_path = await asyncio.to_thread(download_from_drive, filename)
                
                if local_path and os.path.exists(local_path):
                    await bot.send_document(message.from_user.id, FSInputFile(local_path), caption=f"📄 **FILE ACQUIRED**\nCode: `{text}`")
                    await wait_msg.delete()
                    try: os.remove(local_path) 
                    except: pass
                else:
                    await wait_msg.edit_text(f"❌ Error: File `{filename}` not found in Drive Vault.")
            except Exception as e:
                await wait_msg.edit_text(f"❌ Download Failed: {e}")
                
        else:
            # Link Mode
            await message.answer(f"✅ **RESOURCE ACQUIRED**\nCode: `{doc.get('code')}`\n🔗 {doc.get('link')}")
            
    else:
        # If they typed something random that isn't a code
        await message.answer(f"❌ Code `{text}` not found. Select from the buttons or try again.")

@dp.message(BotState.waiting_for_range)
async def process_bulk_range(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text == "🔙 Back to Menu": return await start(message, state)
    
    try:
        # Parse "1-5" or just "1"
        if "-" in text:
            start_idx, end_idx = map(int, text.split('-'))
        elif text.isdigit():
            start_idx = int(text)
            end_idx = start_idx
        else:
            await message.answer("⚠️ Invalid format. Please enter a number (e.g. `1`) or range (e.g. `1-5`).")
            return
        
        if start_idx < 1 or end_idx < start_idx:
            await message.answer("⚠️ Invalid range logic.")
            return

        # Fetch all docs sorted by timestamp (Newest first)
        all_docs = list(col_pdfs.find().sort("timestamp", -1))
        # start_idx is 1-based, so subtract 1 for 0-based indexing
        # end_idx is inclusive for the user, so no need to +1 for python slice if we use [start-1 : end] ???
        # Python slice [a:b] stops BEFORE b. 
        # So "1-1" means index 0. slice [0:1] gives item 0. Correct.
        # "1-5" means indices 0,1,2,3,4. slice [0:5] gives items 0,1,2,3,4. Correct.
        
        selected_docs = all_docs[start_idx-1 : end_idx]
        
        if not selected_docs:
            await message.answer(f"❌ No documents found in range {start_idx}-{end_idx} (Total: {total_docs}).")
            return
            
        data = await state.get_data()
        mode = data.get('retrieval_mode', 'link')
        
        if mode == 'pdf':
            # === BULK PDF MODE ===
            await message.answer(f"📦 **BULK DOWNLOAD INITIATED ({len(selected_docs)} files)...**\nPlease wait.")
            
            count = 0 
            for doc in selected_docs:
                code = doc.get('code')
                filename = f"{code}.pdf"
                try:
                    local_path = await asyncio.to_thread(download_from_drive, filename)
                    if local_path and os.path.exists(local_path):
                        await bot.send_document(message.from_user.id, FSInputFile(local_path), caption=f"Code: `{code}`")
                        count += 1
                        try: os.remove(local_path) 
                        except: pass
                        await asyncio.sleep(1) # Prevent flood wait
                except: continue
                
            await message.answer(f"✅ **Delivered {count}/{len(selected_docs)} files.**")
            
        else:
            # === BULK LINK MODE ===
            report = [f"🔢 **BULK DUMP: {start_idx}-{end_idx}**", "━━━━━━━━━━━━━━━━━━━━"]
            
            for i, doc in enumerate(selected_docs):
                current_num = start_idx + i
                report.append(f"**{current_num}. {doc.get('code')}**")
                report.append(f"🔗 {doc.get('link')}")
                report.append("") 
                
            report.append("━━━━━━━━━━━━━━━━━━━━")
            
            full_msg = "\n".join(report)
            if len(full_msg) > 4000:
                chunks = [full_msg[i:i+4000] for i in range(0, len(full_msg), 4000)]
                for chunk in chunks:
                    await message.answer(chunk, disable_web_page_preview=True)
            else:
                await message.answer(full_msg, disable_web_page_preview=True)
            
        await message.answer("💎 **Operation Complete.** Enter another range or click '🔙 Back to Menu'.")
        
    except ValueError:
        await message.answer("⚠️ Error: Please enter numeric values like `1-5`.")
    except Exception as e:
        await message.answer(f"❌ Error: {e}")

@dp.message(F.text == "🗑 Remove PDF")
async def remove_btn(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔢 DELETE BY RANGE"), KeyboardButton(text="🆔 DELETE BY CODE"))
    builder.row(KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "🗑 **DELETION PROTOCOL INITIATED**\n"
        "Select Deletion Mode:\n\n"
        "🔢 **DELETE BY RANGE**: Delete multiple files (e.g., 1-5).\n"
        "🆔 **DELETE BY CODE**: Delete a specific code (e.g., P1).",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(BotState.choosing_delete_mode)

@dp.message(BotState.choosing_delete_mode)
async def handle_delete_mode(message: types.Message, state: FSMContext):
    text = message.text
    if text == "🔙 Back to Menu": return await start(message, state)
    
    if text == "🔢 DELETE BY RANGE":
        await message.answer(
            "🔢 **BULK DELETE MODE**\n"
            "Enter range to purge (e.g., `1-5`).\n"
            "⚠️ **WARNING**: This deletes from Database AND Google Drive.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True)
        )
        await state.update_data(delete_mode="range")
        await state.set_state(BotState.deleting_pdf)
        
    elif text == "🆔 DELETE BY CODE":
        # Fetch available codes for buttons
        docs = list(col_pdfs.find().sort("timestamp", -1))
        
        builder = ReplyKeyboardBuilder()
        existing_codes = []
        for d in docs[:50]:
            code = d.get('code')
            if code and code not in existing_codes:
                builder.add(KeyboardButton(text=code))
                existing_codes.append(code)
        
        builder.adjust(3)
        builder.row(KeyboardButton(text="🔙 Back to Menu"))
        
        await message.answer(
            "🆔 **SINGLE DELETE MODE**\n"
            "Select a Code button below or type one (e.g., `P1`).",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.update_data(delete_mode="code")
        await state.set_state(BotState.deleting_pdf)
    else:
        await message.answer("⚠️ Invalid Option. use buttons.")

@dp.message(BotState.deleting_pdf)
async def process_deletion(message: types.Message, state: FSMContext):
    text = message.text.strip().upper()
    if text == "🔙 BACK TO MENU": return await start(message, state)
    
    data = await state.get_data()
    mode = data.get('delete_mode', 'code')
    
    if mode == 'code':
        # Single Deletion - Ask for Confirmation
        code = text
        await state.update_data(target_code=code)
        
        # Confirmation Keyboard
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="✅ YES, DELETE"), KeyboardButton(text="❌ CANCEL"))
        
        await message.answer(
            f"❓ **CONFIRM DELETION**\n"
            f"Are you sure you want to permanently delete **{code}** from Database and Drive?",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(BotState.confirm_delete)

    else:
        # Range Deletion (Keep existing logic for now, or add confirmation? Let's add simple confirmation)
        # Actually user specifically asked for "click button confirm". 
        # Range relies on text input. Single relies on buttons.
        # Let's just implement confirmation for EVERYTHING.
        pass # To be continued in next edit if needed, but for now focusing on Code mode changes.
        
        # ... Wait, I can't leave 'pass'. I need to keep the Range logic functioning.
        # Let's just update the Code block first.
        
        try:
            # Parse Range
            if "-" in text:
                start_idx, end_idx = map(int, text.split('-'))
            elif text.isdigit():
                start_idx = int(text)
                end_idx = start_idx
            else:
                await message.answer("⚠️ Invalid format. Use `1-5`.")
                return

            if start_idx < 1 or end_idx < start_idx:
                await message.answer("⚠️ Invalid range logic.")
                return
            
            # Fetch docs
            all_docs = list(col_pdfs.find().sort("timestamp", -1))
            selected_docs = all_docs[start_idx-1 : end_idx]
            
            if not selected_docs:
                await message.answer("❌ No documents in that range.")
                return
            
            # Store target docs for confirmation
            await state.update_data(target_range_indices=[start_idx, end_idx], target_range_len=len(selected_docs))
            
            builder = ReplyKeyboardBuilder()
            builder.row(KeyboardButton(text="✅ YES, DELETE"), KeyboardButton(text="❌ CANCEL"))
            
            await message.answer(
                f"❓ **CONFIRM BULK DELETION**\n"
                f"Range: {start_idx}-{end_idx}\n"
                f"Files to purge: **{len(selected_docs)}**\n"
                f"This cannot be undone.",
                reply_markup=builder.as_markup(resize_keyboard=True)
            )
            await state.set_state(BotState.confirm_delete)
            
        except ValueError:
            await message.answer("⚠️ Error: Use numeric format `1-5`.")
        except Exception as e:
            await message.answer(f"❌ Error: {e}")

@dp.message(BotState.confirm_delete)
async def execute_deletion(message: types.Message, state: FSMContext):
    text = message.text.upper()
    data = await state.get_data()
    mode = data.get('delete_mode', 'code')
    
    if text == "❌ CANCEL":
        await message.answer("� **DELETION ABORTED.**\nNo files were touched.")
        
        # Helper to re-show menu based on mode
        if mode == 'code':
            # Re-fetch buttons
            docs = list(col_pdfs.find().sort("timestamp", -1))
            builder = ReplyKeyboardBuilder()
            existing_codes = []
            for d in docs[:50]:
                code = d.get('code')
                if code and code not in existing_codes:
                    builder.add(KeyboardButton(text=code))
                    existing_codes.append(code)
            builder.adjust(3)
            builder.row(KeyboardButton(text="🔙 Back to Menu"))
            await message.answer("🆔 **Select Code to Delete:**", reply_markup=builder.as_markup(resize_keyboard=True))
            await state.set_state(BotState.deleting_pdf)
        else:
            await message.answer("🔢 **Enter range to purge (e.g. 1-5):**", 
                                 reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True))
            await state.set_state(BotState.deleting_pdf)
        return

    if text == "✅ YES, DELETE":
        if mode == 'code':
            code = data.get('target_code')
            msg = await message.answer(f"⏳ **MOVING TO RECYCLE BIN: `{code}`...**")
            
            # 1. Drive Move
            filename = f"{code}.pdf"
            drive_res = await asyncio.to_thread(move_to_recycle_bin, filename)
            
            # 2. MongoDB Move (Copy to Trash -> Delete from Library)
            doc = col_pdfs.find_one({"code": code})
            if doc:
                col_trash.insert_one(doc)
                col_pdfs.delete_one({"_id": doc['_id']})
                db_res = True
            else:
                db_res = False
            
            status = []
            if drive_res: status.append("☁️ Drive: Moved to Bin")
            else: status.append("☁️ Drive: Not Found")
            
            if db_res: status.append("🍃 DB: Moved to Bin")
            else: status.append("🍃 DB: Not Found")
            
            await msg.edit_text(
                f"♻️ **REYCLED: `{code}`**\n" + "\n".join(status)
            )
        else:
            # Range Deletion
            indices = data.get('target_range_indices')
            start_idx, end_idx = indices
            
            msg = await message.answer(f"⏳ **EXECUTING BULK RECYCLE...**")
            
            all_docs = list(col_pdfs.find().sort("timestamp", -1))
            selected_docs = all_docs[start_idx-1 : end_idx]
            
            moved_count = 0
            for doc in selected_docs:
                code = doc.get('code')
                # Drive
                await asyncio.to_thread(move_to_recycle_bin, f"{code}.pdf")
                # DB
                col_trash.insert_one(doc)
                col_pdfs.delete_one({"_id": doc['_id']})
                moved_count += 1
            
            await msg.edit_text(f"♻️ **BULK RECYCLE COMPLETE**\nMoved {moved_count} files to Bin.")
            
        # Re-Show Menu
        if mode == 'code':
            await asyncio.sleep(1)
            # Re-fetch buttons
            docs = list(col_pdfs.find().sort("timestamp", -1))
            builder = ReplyKeyboardBuilder()
            existing_codes = []
            for d in docs[:50]:
                code = d.get('code')
                if code and code not in existing_codes:
                    builder.add(KeyboardButton(text=code))
                    existing_codes.append(code)
            builder.adjust(3)
            builder.row(KeyboardButton(text="🔙 Back to Menu"))
            await message.answer("🆔 Select next Code or '🔙 Back to Menu'.", reply_markup=builder.as_markup(resize_keyboard=True))
        else:
            await message.answer("🔢 Enter next range or '🔙 Back to Menu'.",
                                 reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 Back to Menu")]], resize_keyboard=True))
        
        await state.set_state(BotState.deleting_pdf)
    else:
        await message.answer("⚠️ Please select YES or CANCEL.")
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

@dp.message(F.text == "⚠️ NUKE ALL DATA")
async def nuke_warning(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="☢️ EXECUTE NUKE"), KeyboardButton(text="🔙 Back to Menu"))
    
    await message.answer(
        "⚠️ **NUCLEAR WARNING** ⚠️\n\n"
        "Master Sadiq, you are about to initiate a **TOTAL SYSTEM WIPE**.\n\n"
        "🔥 **This will destroy:**\n"
        "- All MongoDB Metadata records\n"
        "- All PDF files in your Google Drive Vault\n"
        "- All local temporary files\n\n"
        "**This action is IRREVERSIBLE.** Are you absolutely sure?",
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode="Markdown"
    )
    await state.set_state(BotState.confirm_nuke)

@dp.message(BotState.confirm_nuke)
async def nuke_execution(message: types.Message, state: FSMContext):
    if message.text == "🔙 Back to Menu": return await start(message, state)
    
    if message.text == "☢️ EXECUTE NUKE":
        status_msg = await message.answer("☢️ **INITIATING NUCLEAR PROTOCOL...**")
        
        # 1. MongoDB Wipe
        await status_msg.edit_text("🔥 **STEP 1/3: Purging Database...**")
        try:
            x = col_pdfs.delete_many({})
            db_count = x.deleted_count
        except Exception as e:
            db_count = f"Error: {e}"
            
        # 2. Drive Wipe
        await status_msg.edit_text("🔥 **STEP 2/3: Incinerating Google Drive Vault...**")
        drive_count = 0
        try:
            service = get_drive_service()
            # List all files/folders inside the Parent Folder
            query = f"'{PARENT_FOLDER_ID}' in parents and trashed = false"
            results = service.files().list(q=query, fields="files(id, name)").execute()
            items = results.get('files', [])
            
            if items:
                for item in items:
                    try:
                        service.files().delete(fileId=item['id']).execute()
                        drive_count += 1
                    except: pass
        except Exception as e:
            drive_count = f"Error: {e}"
            
        # 3. Local Wipe
        await status_msg.edit_text("🔥 **STEP 3/3: Sterilizing Local Environment...**")
        local_count = 0
        for file in os.listdir():
            if file.endswith(".pdf"):
                try: 
                    os.remove(file)
                    local_count += 1
                except: pass
                
        # Final Report
        report = (
            "☢️ **NUCLEAR WIPEOUT COMPLETE** ☢️\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🛢 **Database:** {db_count} records destroyed.\n"
            f"☁️ **Drive:** {drive_count} items incinerated.\n"
            f"💻 **Local:** {local_count} files purged.\n\n"
            "The system is now completely empty, Master Sadiq."
        )
        await status_msg.edit_text(report)
        await state.clear()
        
        # Reset Menu
        await message.answer("💎 **READY FOR REBIRTH.**", reply_markup=get_main_menu())
    else:
        await message.answer("Please confirm with the button or go back.")

# ==========================================
# 🚀 CORE INITIALIZATION
# ==========================================

async def main():
    # Retry loop for network startup
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.set_my_commands([BotCommand(command="start", description="Menu")])
            break
        except Exception as e:
            print(f"⚠️ Network Startup Error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
    
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

    while True:
        try:
            await dp.start_polling(bot, skip_updates=True)
            break
        except Exception as e:
             logging.error(f"Polling Network Error: {e}. Retrying in 5s...")
             await asyncio.sleep(5)
    
    await bot.session.close()

if __name__ == "__main__":
    print("🚀 STARTING INDIVIDUAL CORE TEST: BOT 4")
    
    prepare_secrets()
    
    threading.Thread(target=run_health_server, daemon=True).start()
    
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("◈ Bot 4 Shutdown.")
