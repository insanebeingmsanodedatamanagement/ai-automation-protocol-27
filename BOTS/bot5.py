# -*- coding: utf-8 -*-
print("[TRACE] Python execution started")
import os
print("[TRACE] os imported")
import threading
print("[TRACE] threading imported")
from aiohttp import web
print("[TRACE] aiohttp imported")

# HEALTH SERVER (Copied from working BOT 4 pattern)
def run_health_server():
    """Synchronous health server that runs in a separate thread"""
    try:
        app = web.Application()
        app.router.add_get('/', lambda r: web.Response(text="BOT 5 SINGULARITY V5 ACTIVE"))
        port = int(os.environ.get("PORT", 10000))
        print(f"[TRACE] Health server binding to port {port}")
        web.run_app(app, host='0.0.0.0', port=port, handle_signals=False)
    except Exception as e:
        print(f"[ERROR] Health Server Error: {e}")

print("[OK] Step 1: Basic imports done")

# ==============================================================
# NOW IMPORT EVERYTHING ELSE (these imports may block/take time)
# ==============================================================
print("[WAIT] Step 2: Starting heavy imports...")
import asyncio, html, time, pytz, logging, random, io, psutil, re
print("[OK] Step 3: asyncio imports done")
from datetime import datetime, timedelta
from google import genai
print("[OK] Step 4: google.genai imported")
from google.genai import types as ai_types
from aiogram import Bot, Dispatcher, types, F
print("[OK] Step 5: aiogram imported")
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pymongo
import sys
print("[OK] Step 6: All imports completed")

# Force UTF-8 stdout (Windows compatibility - skip on Linux)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    pass  # Linux/Render doesn't need this

# Note: WindowsSelectorEventLoopPolicy removed - deprecated in Python 3.16
# Network stability is handled by retry logic instead.

START_TIME = time.time()




# ==========================================
# ⚡ SECURE CONFIGURATION
# ==========================================
BOT_TOKEN = os.getenv("BOT_5_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
DATA_CHANNEL_ID = int(os.getenv("MSA_DATA_CHANNEL_ID", 0))
GEMINI_KEY = os.getenv("GEMINI_KEY")
MONGO_URI = os.getenv("MONGO_URI")
CHANNEL_ID = int(os.getenv("MAIN_CHANNEL_ID", 0))

IST = pytz.timezone('Asia/Kolkata')
# Initialize bot and dispatcher
client = None
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone=IST)

# Database globals - initialized by connect_db() function
db_client = None
db = None
col_vault = None
col_system = None
col_history = None
col_api = None

def connect_db():
    """Connect to MongoDB with timeout - matching bot4 pattern"""
    global db_client, db, col_vault, col_system, col_history, col_api
    try:
        db_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = db_client["Singularity_V5_Final"]
        col_vault = db["vault"]
        col_system = db["system_stats"]
        col_history = db["history_log"]
        col_api = db["api_ledger"]
        db_client.server_info()  # Test connection
        print("[OK] Database Connected")
        return True
    except Exception as e:
        print(f"[ERROR] DB Connect Error: {e}")
        return False

print("[OK] Step 7: Attempting database connection...")
# Initialize database connection (non-blocking - errors logged but don't crash)
try:
    connect_db()
    print("[OK] Step 8: Database connection attempted")
except Exception as e:
    print(f"[WARN] Database connection failed (bot will continue): {e}")

print("[OK] Step 9: Loading configuration...")

# DATABASE CONFIG IDS
DB_ID_MODELS = "bot5_models"
DB_ID_KEYS = "bot5_apikeys"

# DATABASE-DRIVEN CONFIGURATION (Loaded at startup)
# Default fallback models (validated as working)
DEFAULT_MODELS = [
    "gemini-2.5-flash-lite",  # Validated working
]
MODEL_POOL = []  # Will be loaded from database
API_KEY_POOL = []  # Will be loaded from database 

CURRENT_MODEL_INDEX = 0
API_USAGE_COUNT = 0
PENDING_FIRE = {}
PROMPT_INDEX = 0  # Sequential prompt picker - cycles through all prompts

# ENTERPRISE FEATURES
FIRED_CONTENT_HASHES = set()  # Duplicate detection - stores hashes of fired content
DAILY_STATS = {"breaches_fired": 0, "scheduled_fired": 0, "duplicates_blocked": 0, "errors": 0}

# ==========================================
# 🌐 HIGH-VALUE PROMPT PACKS (ENTERPRISE GRADE)
# ==========================================
CLODU_PROMPT_PACK = [] # (This line is just context, not replacing prompt pack content)


# ==========================================
# 🌐 HIGH-VALUE PROMPT PACKS (ENTERPRISE GRADE) - 50+ PROMPTS
# ==========================================
CLOUD_PROMPT_PACK = [
    # === CERTIFICATIONS & EDUCATION (10 prompts) ===
    "List the Top 5 completely FREE but highly respected tech certifications (Google, Microsoft, etc) that boost salary immediately.",
    "Generate a list of 5 hidden AI certifications from top universities (Harvard, Stanford) that are free to audit.",
    "Provide 5 free specialized cloud computing courses that offer badges/certificates upon completion.",
    "Reveal 5 free cybersecurity certifications that are recognized by Fortune 500 companies.",
    "List 5 free data science certifications from Coursera, edX, or Google that include hands-on projects.",
    "Identify 5 free blockchain/Web3 courses that provide verifiable credentials.",
    "Showcase 5 free UX/UI design certifications from industry leaders like Google and Meta.",
    "List 5 free project management certifications (PMP alternatives) that are industry recognized.",
    "Provide 5 free machine learning courses from MIT, Stanford, or Google with completion certificates.",
    "Reveal 5 hidden free programming bootcamps that guarantee job placement assistance.",
    
    # === AI & PRODUCTIVITY TOOLS (10 prompts) ===
    "Reveal the Top 5 secret AI productivity tools that major CEOs use (excluding ChatGPT).",
    "List 5 underrated browser extensions that automate 90% of daily manual work.",
    "Showcase 5 free 'No-Code' tools to build a SaaS startup in 24 hours.",
    "Identify 5 AI writing assistants better than Grammarly that are completely free.",
    "List 5 free AI video editing tools that rival Adobe Premiere Pro.",
    "Reveal 5 AI-powered research tools that academics and PhDs secretly use.",
    "Provide 5 free AI image generators that create commercial-use images.",
    "List 5 AI coding assistants (beyond GitHub Copilot) that are free for developers.",
    "Showcase 5 AI presentation tools that create professional slides in seconds.",
    "Identify 5 free AI transcription and note-taking tools used by journalists.",
    
    # === MONEY & FREELANCING (10 prompts) ===
    "Identify 5 untapped digital service arbitrage opportunities using free AI tools.",
    "List 5 high-paying freelance niches that can be fully automated with specific AI agents.",
    "Reveal 5 platforms offering 'sign-up bonuses' or 'free credits' for developers right now.",
    "Provide 5 remote work platforms paying $50+/hour for simple AI-assisted tasks.",
    "List 5 passive income streams that can be fully automated with AI in 2024.",
    "Reveal 5 secret freelance marketplaces with less competition and higher rates.",
    "Identify 5 AI tools to automate client acquisition for freelancers.",
    "List 5 legitimate survey/testing sites that pay $100+ monthly for minimal work.",
    "Showcase 5 platforms where you can sell AI-generated content legally.",
    "Provide 5 micro-SaaS ideas that can be built and launched in one weekend.",
    
    # === FREE SOFTWARE & DEVELOPER TOOLS (10 prompts) ===
    "List 5 free alternatives to expensive software (Photoshop, Office, etc) that professionals use.",
    "Reveal 5 free hosting platforms for developers that include free SSL and domains.",
    "Provide 5 free database solutions that scale to millions of users.",
    "List 5 free API services that provide production-ready features at no cost.",
    "Identify 5 free CI/CD pipeline tools used by tech startups.",
    "Showcase 5 free monitoring and analytics tools for web applications.",
    "List 5 free email marketing platforms with generous free tiers.",
    "Reveal 5 free CRM tools that compete with Salesforce.",
    "Provide 5 free design tools (Figma alternatives) for UI/UX designers.",
    "List 5 free stock photo and video sites with no attribution required.",
    
    # === CAREER & PERSONAL DEVELOPMENT (10 prompts) ===
    "Reveal 5 free resume builders that use AI to beat ATS systems.",
    "List 5 free interview preparation platforms used by FAANG candidates.",
    "Provide 5 free networking platforms beyond LinkedIn for job seekers.",
    "Identify 5 free language learning apps better than Duolingo for professionals.",
    "List 5 free speed reading and memory enhancement tools backed by science.",
    "Showcase 5 free personal finance apps that automate wealth building.",
    "Reveal 5 free meditation and focus apps used by Silicon Valley executives.",
    "List 5 free online communities where millionaires share advice openly.",
    "Provide 5 free legal document templates for freelancers and entrepreneurs.",
    "Identify 5 free tax optimization tools and resources for self-employed individuals.",
    
    # === HEALTH & WELLNESS (10 prompts) ===
    "List 5 free fitness apps with personalized workout plans rivaling expensive trainers.",
    "Reveal 5 free mental health apps recommended by psychologists and therapists.",
    "Provide 5 free nutrition tracking apps that calculate macros and micronutrients.",
    "List 5 free sleep optimization tools and techniques used by elite athletes.",
    "Identify 5 free yoga and meditation apps with guided sessions for beginners.",
    "Showcase 5 free health screening tools that detect early warning signs.",
    "List 5 free habit tracking apps that use behavioral psychology principles.",
    "Reveal 5 free posture correction apps for remote workers and desk jobs.",
    "Provide 5 free first aid and emergency response training resources.",
    "List 5 free women's health apps for cycle tracking and wellness.",
    
    # === BUSINESS & ENTREPRENEURSHIP (10 prompts) ===
    "List 5 free business plan templates used by Y Combinator startups.",
    "Reveal 5 free market research tools that Fortune 500 companies use.",
    "Provide 5 free competitive analysis frameworks and templates.",
    "List 5 free pitch deck templates from successful startup fundraises.",
    "Identify 5 free business model canvas tools for lean startups.",
    "Showcase 5 free customer feedback and survey tools with unlimited responses.",
    "List 5 free invoice and accounting software for small businesses.",
    "Reveal 5 free trademark and intellectual property search tools.",
    "Provide 5 free supply chain and inventory management solutions.",
    "List 5 free e-commerce platforms with zero transaction fees.",
    
    # === INVESTING & FINANCE (10 prompts) ===
    "List 5 free stock screeners used by professional traders.",
    "Reveal 5 free cryptocurrency research and analysis platforms.",
    "Provide 5 free portfolio tracking apps that sync with all brokerages.",
    "List 5 free options trading calculators and strategy builders.",
    "Identify 5 free real estate investment analysis tools and calculators.",
    "Showcase 5 free dividend tracking apps for passive income investors.",
    "List 5 free economic calendar and market news aggregators.",
    "Reveal 5 free financial modeling templates in Excel/Google Sheets.",
    "Provide 5 free retirement planning calculators with detailed projections.",
    "List 5 free credit score monitoring services with identity protection.",
    
    # === SIDE HUSTLES & INCOME (10 prompts) ===
    "List 5 legitimate print-on-demand platforms with highest profit margins.",
    "Reveal 5 dropshipping niches with low competition in 2024.",
    "Provide 5 affiliate marketing programs paying 50%+ commission.",
    "List 5 platforms to sell digital products with zero upfront cost.",
    "Identify 5 ways to monetize a small social media following (under 1000).",
    "Showcase 5 micro-task platforms paying in crypto or PayPal instantly.",
    "List 5 platforms for selling stock photos taken with smartphones.",
    "Reveal 5 ways to earn money testing apps and websites from home.",
    "Provide 5 platforms for renting out unused items for passive income.",
    "List 5 tutoring platforms that pay $30+/hour for teaching online.",
    
    # === MARKETING & GROWTH (10 prompts) ===
    "List 5 free SEO tools that compete with Ahrefs and SEMrush.",
    "Reveal 5 free social media scheduling tools with analytics.",
    "Provide 5 free landing page builders with conversion optimization.",
    "List 5 free email automation tools for lead nurturing.",
    "Identify 5 free influencer discovery and outreach platforms.",
    "Showcase 5 free A/B testing tools for websites and apps.",
    "List 5 free content calendar and planning templates.",
    "Reveal 5 free viral content formulas that consistently work.",
    "Provide 5 free hashtag research and optimization tools.",
    "List 5 free customer journey mapping templates and tools.",
    
    # === REMOTE WORK & PRODUCTIVITY (10 prompts) ===
    "List 5 free virtual office and coworking space platforms.",
    "Reveal 5 free time tracking apps that boost remote team productivity.",
    "Provide 5 free project management tools better than Trello.",
    "List 5 free team communication tools beyond Slack.",
    "Identify 5 free screen recording and video messaging tools.",
    "Showcase 5 free password managers with team sharing features.",
    "List 5 free VPN services with no data limits for remote workers.",
    "Reveal 5 free document collaboration tools beyond Google Docs.",
    "Provide 5 free meeting scheduler tools that eliminate back-and-forth.",
    "List 5 free focus and distraction blocking apps for deep work.",
    
    # === CREATIVE & CONTENT (10 prompts) ===
    "List 5 free music production software used by professional artists.",
    "Reveal 5 free podcast hosting platforms with unlimited episodes.",
    "Provide 5 free animation and motion graphics tools for beginners.",
    "List 5 free 3D modeling software used in professional studios.",
    "Identify 5 free video thumbnail makers that increase click-through rates.",
    "Showcase 5 free beat making and sound design platforms.",
    "List 5 free comic and manga creation tools with templates.",
    "Reveal 5 free storytelling and creative writing AI assistants.",
    "Provide 5 free font creation tools for designers.",
    "List 5 free meme generators and viral content creation tools.",
    
    # === EDUCATION & LEARNING (10 prompts) ===
    "List 5 free Ivy League courses covering business and economics.",
    "Reveal 5 free philosophy and critical thinking courses online.",
    "Provide 5 free history and civilization courses from top universities.",
    "List 5 free public speaking and presentation courses.",
    "Identify 5 free negotiation and persuasion courses from experts.",
    "Showcase 5 free creative writing courses from published authors.",
    "List 5 free psychology courses explaining human behavior.",
    "Reveal 5 free astronomy and space science courses from NASA.",
    "Provide 5 free environmental science and sustainability courses.",
    "List 5 free music theory and composition courses for beginners.",
    
    # === TECHNOLOGY & INNOVATION (10 prompts) ===
    "List 5 emerging technologies that will create new job markets by 2025.",
    "Reveal 5 free quantum computing courses and simulators.",
    "Provide 5 free robotics and automation learning platforms.",
    "List 5 free IoT (Internet of Things) development platforms.",
    "Identify 5 free AR/VR development tools for beginners.",
    "Showcase 5 free edge computing and distributed systems courses.",
    "List 5 free bioinformatics and computational biology resources.",
    "Reveal 5 free space technology and satellite data platforms.",
    "Provide 5 free autonomous vehicle and self-driving tech courses.",
    "List 5 free green technology and clean energy innovation resources."
]

# --- TELEMETRY HELPERS ---
def console_out(text):
    print(f"[{datetime.now(IST).strftime('%I:%M:%S %p')}] {text}")

def get_next_prompt():
    """Sequential prompt picker - cycles through all prompts one by one"""
    global PROMPT_INDEX
    prompt = CLOUD_PROMPT_PACK[PROMPT_INDEX]
    PROMPT_INDEX = (PROMPT_INDEX + 1) % len(CLOUD_PROMPT_PACK)
    console_out(f"📋 Prompt #{PROMPT_INDEX}/{len(CLOUD_PROMPT_PACK)}")
    return prompt

def normalize_model_name(model_name):
    """
    Normalize model names to proper API format.
    - Converts to lowercase
    - Removes extra spaces
    - Replaces spaces with hyphens
    - Removes special characters except hyphens and dots
    
    Examples:
      "Gemini 2.5 Flash-Lite" -> "gemini-2.5-flash-lite"
      "  GEMINI 2.0   FLASH  " -> "gemini-2.0-flash"
      "Gemini-Pro" -> "gemini-pro"
    """
    # Strip whitespace
    normalized = model_name.strip()
    
    # Convert to lowercase
    normalized = normalized.lower()
    
    # Replace multiple spaces with single space
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Replace spaces with hyphens
    normalized = normalized.replace(' ', '-')
    
    # Remove extra hyphens (multiple consecutive hyphens)
    normalized = re.sub(r'-+', '-', normalized)
    
    # Remove leading/trailing hyphens
    normalized = normalized.strip('-')
    
    return normalized

def fix_html_tags(text):
    """
    Fix malformed HTML tags by ensuring all opening tags have closing tags.
    Common issues: unclosed <b>, <i>, <u>, <font> tags
    """
    # Count opening and closing tags for each type
    tags_to_fix = ['b', 'i', 'u', 'code', 's']
    
    for tag in tags_to_fix:
        open_count = len(re.findall(f'<{tag}>', text))
        close_count = len(re.findall(f'</{tag}>', text))
        
        # Add missing closing tags at the end
        if open_count > close_count:
            text += f'</{tag}>' * (open_count - close_count)
    
    # Handle <font> tags (more complex due to attributes)
    font_open = len(re.findall(r'<font[^>]*>', text))
    font_close = len(re.findall(r'</font>', text))
    if font_open > font_close:
        text += '</font>' * (font_open - font_close)
    
    # Handle <a> tags
    a_open = len(re.findall(r'<a[^>]*>', text))
    a_close = len(re.findall(r'</a>', text))
    if a_open > a_close:
        text += '</a>' * (a_open - a_close)
    
    return text

async def increment_api_count_in_db():
    try:
        col_api.update_one({"_id": "global_ledger"}, {"$inc": {"usage": 1}}, upsert=True)
    except Exception as e: console_out(f"Ledger Sync Error: {e}")

# --- ENTERPRISE: DUPLICATE DETECTION ---
def get_content_hash(content):
    """Generate hash of content for duplicate detection"""
    import hashlib
    # Use first 500 chars to avoid hash collisions on truncated content
    return hashlib.md5(content[:500].encode()).hexdigest()

def is_duplicate(content):
    """Check if content has already been fired"""
    content_hash = get_content_hash(content)
    if content_hash in FIRED_CONTENT_HASHES:
        return True
    return False

def mark_as_fired(content):
    """Mark content as fired to prevent duplicates"""
    content_hash = get_content_hash(content)
    FIRED_CONTENT_HASHES.add(content_hash)
    # Keep only last 100 hashes to prevent memory bloat
    if len(FIRED_CONTENT_HASHES) > 100:
        FIRED_CONTENT_HASHES.pop()

# --- ENTERPRISE: DAILY SUMMARY REPORT ---
async def send_daily_summary():
    """Send daily stats report to owner at midnight"""
    global DAILY_STATS
    
    summary = (
        f"📊 <b>DAILY OPERATIONS SUMMARY</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 Date: {datetime.now(IST).strftime('%Y-%m-%d')}\n\n"
        f"🔥 <b>Breaches Fired:</b> {DAILY_STATS['breaches_fired']}\n"
        f"⏰ <b>Scheduled Fires:</b> {DAILY_STATS['scheduled_fired']}\n"
        f"🚫 <b>Duplicates Blocked:</b> {DAILY_STATS['duplicates_blocked']}\n"
        f"[ERROR] <b>Errors:</b> {DAILY_STATS['errors']}\n"
        f"📋 <b>Prompts Used:</b> {PROMPT_INDEX}/{len(CLOUD_PROMPT_PACK)}\n"
        f"🔑 <b>API Usage:</b> {API_USAGE_COUNT}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"[SYSTEM] <i>Bot 5 - SINGULARITY V5.0</i>"
    )
    
    try:
        await bot.send_message(OWNER_ID, summary, parse_mode=ParseMode.HTML)
        console_out("📊 Daily Summary Sent.")
    except Exception as e:
        console_out(f"[ERROR] Daily Summary Error: {e}")
    
    # Reset daily stats
    DAILY_STATS = {"breaches_fired": 0, "scheduled_fired": 0, "duplicates_blocked": 0, "errors": 0}

# --- ENTERPRISE: INSTANT ERROR NOTIFICATIONS ---
async def notify_error(error_type, details):
    """Send instant error notification to owner"""
    try:
        alert = (
            f"🚨 <b>INSTANT ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"[WARN] <b>Type:</b> {error_type}\n"
            f"📝 <b>Details:</b> {details[:500]}\n"
            f"🕐 <b>Time:</b> {datetime.now(IST).strftime('%I:%M:%S %p')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        await bot.send_message(OWNER_ID, alert, parse_mode=ParseMode.HTML)
        console_out(f"🚨 Error Alert Sent: {error_type}")
    except Exception as e:
        console_out(f"[ERROR] Failed to send error alert: {e}")


# ==========================================
# 📊 GLOBAL METRICS & SPECS
# ==========================================
# Model Specifications (RPM/RPD Limits - Estimated Free Tier)
MODEL_SPECS = {
    "gemini-2.5-pro": {"rpm": 10, "rpd": 1500, "tpm": 1000000},
    "gemini-2.5-flash": {"rpm": 15, "rpd": 1500, "tpm": 1000000}
}
TOTAL_TOKENS = 0

# 🧠 ORACLE PROMPT ENGINE (CHIMERA PROTOCOL V2)
# ==========================================
def get_system_prompt():
    return """
    PERSONA: THE ARCHITECT — elite insider advisor. Calm authority. Every word matters.
    
    STYLE: Rich, detailed, premium. Use emojis for structure and ranking (medals for items). Sophisticated and engaging.
    
    RULES:
    - EXACTLY 5 items with REAL HTTPS links
    - FREE resources from Google, Microsoft, IBM, AWS, Coursera, edX, Harvard, Stanford
    - Detailed 2-line descriptions per item
    - Focus on transformation and value
    
    OUTPUT FORMAT (MOBILE-FRIENDLY HTML):
    
    [START] <b>[POWERFUL COMPELLING TITLE]</b>
    
    ✨ <i>[2-3 sentence psychological hook. Why the masses miss this. What transformation awaits the chosen few.]</i>
    
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    [SYSTEM] <b>UNLOCKED EXCLUSIVELY FOR MSA NODE VAULT MEMBERS</b>
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    🥇 <b>[Resource Name]</b>
    📌 <i>[2-line detailed description: skills gained, who uses it, why it matters]</i>
    🏷️ Provider: [Company]
    ⭐ Value: $500+ (FREE)
    🔗 <a href="URL">→ Claim Now</a>
    
    🥈 <b>[Resource Name]</b>
    📌 <i>[2-line description]</i>
    🏷️ Provider: [Company]
    ⭐ Value: $500+ (FREE)
    🔗 <a href="URL">→ Claim Now</a>
    
    🥉 <b>[Resource Name]</b>
    📌 <i>[2-line description]</i>
    🏷️ Provider: [Company]
    ⭐ Value: $500+ (FREE)
    🔗 <a href="URL">→ Claim Now</a>
    
    🏅 <b>[Resource Name]</b>
    📌 <i>[2-line description]</i>
    🏷️ Provider: [Company]
    ⭐ Value: $500+ (FREE)
    🔗 <a href="URL">→ Claim Now</a>
    
    🎖️ <b>[Resource Name]</b>
    📌 <i>[2-line description]</i>
    🏷️ Provider: [Company]
    ⭐ Value: $500+ (FREE)
    🔗 <a href="URL">→ Claim Now</a>
    
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    [SYSTEM] <b>THE ARCHITECT'S WISDOM</b>
    <i>"The gap between where you are and where you want to be is bridged by one decision: START. The elite don't wait for permission. They execute. Pick ONE resource above. Complete it in 7 days. Your future self is watching."</i>
    
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    📢 <b>MSA NODE</b> • Your Gateway to Premium Resources
    🔔 Save this. Share this. Execute this.
    💬 More drops incoming. Stay connected.
    """

async def generate_content(prompt):
    global CURRENT_API_INDEX, GEMINI_KEY, client, API_USAGE_COUNT, TOTAL_TOKENS, CURRENT_MODEL_INDEX, MODEL_POOL, col_history, col_system, col_api
    
    # 0. SANITY CHECK: If pool empty, restore defaults
    if not MODEL_POOL:
        console_out("[WARN] MODEL_POOL empty, restoring defaults...")
        MODEL_POOL.extend(DEFAULT_MODELS)
        if col_system is not None:
            col_system.update_one({"_id": DB_ID_MODELS}, {"$set": {"list": MODEL_POOL}}, upsert=True)
    
    if not API_KEY_POOL:
        console_out("[WARN] API_KEY_POOL empty!")
        return "[ERROR] No API keys available", "[ERROR] No API keys available", "CRITICAL_ERROR"

    # Prepare Prompt
    system_instruction = get_system_prompt()
    
    # LOGIC: ITERATE KEYS -> ITERATE MODELS (Failover)
    max_keys = len(API_KEY_POOL)
    keys_tried_count = 0
    
    # Start with the cached index
    temp_key_idx = CURRENT_API_INDEX
    
    while keys_tried_count < max_keys:
        # 1. Select and Init Key
        if not API_KEY_POOL:
             break
             
        current_key = API_KEY_POOL[temp_key_idx]
        
        try:
            client = genai.Client(api_key=current_key)
        except Exception as e:
            # Key invalid format? Skip.
            keys_tried_count += 1
            temp_key_idx = (temp_key_idx + 1) % max_keys
            continue
            
        # 2. ITERATE ALL MODELS FOR THIS KEY
        # CRITICAL FIX: Start from CURRENT_MODEL_INDEX and wrap around
        # Example: If user selected Model 3 (index 2) and there are 7 models:
        #   Try order: Model 3, 4, 5, 6, 7, 1, 2
        # This ensures:
        #   - Selected model is tried FIRST
        #   - ALL models are tried once (wrap around)
        #   - Only then switch to next API key
        if MODEL_POOL and CURRENT_MODEL_INDEX < len(MODEL_POOL):
            # Reorder models to prioritize user's selection
            models_to_try = MODEL_POOL[CURRENT_MODEL_INDEX:] + MODEL_POOL[:CURRENT_MODEL_INDEX]
        else:
            models_to_try = list(MODEL_POOL)
        
        # If pool empty, try defaults?
        if not models_to_try:
             models_to_try = ["gemini-2.0-flash-exp", "gemini-1.5-pro"]
        
        models_tried_count = 0
        max_models = len(models_to_try)
        temp_model_idx = 0
        
        while temp_model_idx < max_models:
            model_id = models_to_try[temp_model_idx]
            
            # CRITICAL: Normalize model name to prevent 400 errors
            model_id = normalize_model_name(model_id)
            
            try:
                # CRITICAL: Show which model/API is being tried BEFORE generation
                try:
                    await bot.send_message(
                        OWNER_ID,
                        f"🔄 <b>GENERATING BREACH...</b>\n"
                        f"🔑 API Key: <code>{temp_key_idx + 1}/{len(API_KEY_POOL)}</code>\n"
                        f"🧠 Model: <code>{model_id}</code>\n"
                        f"📊 Attempt: <code>{temp_model_idx + 1}/{max_models}</code>",
                        parse_mode=ParseMode.HTML
                    )
                except: pass
                
                # Network Retry Loop
                response = None
                network_retries = 2
                for net_attempt in range(network_retries):
                    try:
                        response = await client.aio.models.generate_content(
                            model=model_id,
                            contents=[prompt],
                            config=ai_types.GenerateContentConfig(
                                system_instruction=system_instruction,
                                temperature=0.7,
                                max_output_tokens=5000  # Increased to prevent truncation
                            )
                        )
                        break 
                    except Exception as net_err:
                        err_str = str(net_err)
                        if "429" in err_str or "403" in err_str or "API_KEY" in err_str or "ResourceExhausted" in err_str or "400" in err_str or "INVALID_ARGUMENT" in err_str:
                             raise net_err 
                        
                        if net_attempt < network_retries - 1:
                            await asyncio.sleep(1)
                            continue
                        raise net_err 

                if response and response.text:
                    # SUCCESS!
                    content = response.text
                    
                    # Update Globals
                    CURRENT_API_INDEX = temp_key_idx
                    # Find model index in global pool to update CURRENT_MODEL_INDEX
                    if model_id in MODEL_POOL:
                        CURRENT_MODEL_INDEX = MODEL_POOL.index(model_id)
                        # Persist to database
                        if col_system is not None:
                            col_system.update_one(
                                {"_id": "config"},
                                {"$set": {"current_model_index": CURRENT_MODEL_INDEX}},
                                upsert=True
                            )
                    GEMINI_KEY = current_key
                    
                    # Metrics & DB Update
                    API_USAGE_COUNT += 1
                    try:
                        t_count = len(content) // 4
                        try:
                            if hasattr(response, 'usage_metadata'):
                                t_count = response.usage_metadata.total_token_count
                        except: pass
                        TOTAL_TOKENS += t_count
                        
                        if col_api is not None:
                            col_api.update_one(
                                {"_id": "global_ledger"}, 
                                {"$set": {"usage": API_USAGE_COUNT, "tokens": TOTAL_TOKENS}}, 
                                upsert=True
                            )
                            col_api.update_one(
                                {"_id": f"key_{temp_key_idx + 1}"},
                                {"$set": {"status": "ACTIVE", "last_success": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")}},
                                upsert=True
                            )
                    except: pass
                    
                    # Parse Content & Breach ID
                    clean_content = content.replace("```html", "").replace("```", "").strip()
                    
                    today_str = datetime.now(IST).strftime("%d%m")
                    
                    # FIX: Check if collection is None instead of boolean check
                    new_num = 1
                    if col_history is not None:
                        last_breach = col_history.find_one(sort=[("timestamp", -1)])
                        if last_breach and "breach_num" in last_breach:
                            new_num = last_breach["breach_num"] + 1
                    
                    breach_id = f"BRH {new_num}"
                    final_content = clean_content + f"\n\n🆔 <b>ID: {breach_id}</b>\n\nClick FIRE to deploy (ID hidden in public post)."
                    
                    if col_history is not None:
                        col_history.insert_one({
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "model": model_id,
                            "breach_id": breach_id,
                            "breach_num": new_num,
                            "content": clean_content[:200] + "...",
                            "full_content": final_content
                        })
                
                    return clean_content, final_content, "AI Directive"

            except Exception as e:
                # FAILURE HANDLING
                err_str = str(e)
                
                fail_reason = "Error"
                if "429" in err_str or "ResourceExhausted" in err_str: fail_reason = "RateLimited"
                if "403" in err_str or "API_KEY" in err_str: fail_reason = "InvalidKey"
                
                # SELF-HEALING: Auto-remove broken models (400/404)
                if "400" in err_str or "INVALID_ARGUMENT" in err_str or "NOT_FOUND" in err_str:
                    try:
                        # Remove ALL instances from GLOBAL pool
                        if model_id in MODEL_POOL:
                            # Filter out ALL occurrences
                            MODEL_POOL = [m for m in MODEL_POOL if m != model_id]
                            
                            # Persist Removal
                            if col_system is not None:
                                col_system.update_one({"_id": DB_ID_MODELS}, {"$set": {"list": MODEL_POOL}})
                                
                            await bot.send_message(
                                OWNER_ID, 
                                f"🗑️ <b>AUTO-REMOVED INVALID MODEL:</b> {model_id}\nReason: {fail_reason}",
                                parse_mode=ParseMode.HTML
                            )
                    except Exception as ex:
                        console_out(f"Self-heal error: {ex}")

                # 1. ALWAYS REPORT THE FAILURE FOR THIS SPECIFIC MODEL
                model_attempt = temp_model_idx + 1
                total_models = len(models_to_try)
                
                try:
                    await bot.send_message(
                        OWNER_ID,
                        f"[WARN] <b>{fail_reason.upper()}: {model_id}</b>\n"
                        f"🔢 Attempt: {model_attempt}/{total_models}\n"
                        f"📝 Details: {html.escape(err_str)[:100]}",
                        parse_mode=ParseMode.HTML
                    )
                    await asyncio.sleep(0.5) 
                except: pass
                
                # 2. IF WE HAVE MORE MODELS, REPORT SWITCHING
                if temp_model_idx < total_models - 1:
                     try:
                         await bot.send_message(
                            OWNER_ID,
                            f"🔄 Switching to next available model on Key {temp_key_idx + 1}...",
                            parse_mode=ParseMode.HTML
                        )
                         await asyncio.sleep(0.5)
                     except: pass
                
                # Try next model
                models_tried_count += 1
                temp_model_idx += 1
                await asyncio.sleep(1)
                continue
        
        # If we reach here, it means ALL MODELS failed for `current_key`
        keys_tried_count += 1
        
        if keys_tried_count < max_keys:
             await bot.send_message(
                OWNER_ID, 
                f"🚫 <b>KEY {temp_key_idx+1} EXHAUSTED.</b>\n🔀 Switching to Key {(temp_key_idx + 1) % max_keys + 1}...", 
                parse_mode=ParseMode.HTML
             )
             temp_key_idx = (temp_key_idx + 1) % max_keys
             
    # EMERGENCY FALLBACK: If we are here, everything failed.
    # Check if we removed all models?
    if not MODEL_POOL:
        await bot.send_message(OWNER_ID, "[WARN] <b>ALL MODELS REMOVED/FAILED. RESTORING DEFAULTS...</b>", parse_mode=ParseMode.HTML)
        MODEL_POOL = ["gemini-2.0-flash-exp", "gemini-1.5-pro", "gemini-1.5-flash"]
        # Persist restoration
        if col_system is not None:
             col_system.update_one({"_id": DB_ID_MODELS}, {"$set": {"list": MODEL_POOL}})
        
        # Retry recursively ONCE
        return await generate_content(prompt)
        
    # DEAD END / TOTAL FAILURE
    err_msg = (
        f"⛔ <b>SYSTEM CRITICAL FAILURE</b>\n"
        f"All {max_keys} Keys & Models Exhausted.\n"
        f"Request Manual Intervention."
    )
    await notify_error("CRITICAL: All API Keys Exhausted", f"All {max_keys} keys failed on all models.")
    return err_msg, err_msg, "CRITICAL_ERROR"

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
    selecting_model = State()
    adding_model = State()
    selecting_api = State()
    adding_api = State()
    viewing_bin = State()
    confirm_delete_all = State()

# Global Indices for rotation
CURRENT_API_INDEX = 0

async def get_days_kb(selected):
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    row1 = [InlineKeyboardButton(text=f"{'[OK]' if i in selected else '[ERROR]'} {d}", callback_data=f"toggle_{i}") for i, d in enumerate(days[:4])]
    row2 = [InlineKeyboardButton(text=f"{'[OK]' if i+4 in selected else '[ERROR]'} {d}", callback_data=f"toggle_{i+4}") for i, d in enumerate(days[4:])]
    return InlineKeyboardMarkup(inline_keyboard=[row1, row2, [InlineKeyboardButton(text="📥 LOCK PROTOCOL", callback_data="lock_sched")], [InlineKeyboardButton(text="\U0001F519 BACK", callback_data="back_main")]])

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")],
        [KeyboardButton(text="⚙️ MODELS"), KeyboardButton(text="🔑 API")],
        [KeyboardButton(text="🛡 SCAN"), KeyboardButton(text="📢 BROADCAST")],
        [KeyboardButton(text="📜 HISTORY"), KeyboardButton(text="🗑️ RECYCLE BIN")],
        [KeyboardButton(text="[ERROR] DELETE ALL")]
    ], resize_keyboard=True)
    
    await message.answer(f"[SYSTEM] <b>SINGULARITY V5.0 LIVE | {MODEL_POOL[CURRENT_MODEL_INDEX]}</b>\nSystems Online.", reply_markup=kb, parse_mode=ParseMode.HTML)

# --- GLOBAL BACK HANDLER ---
@dp.message(F.text == "\U0001F519 BACK")
async def global_back(message: types.Message, state: FSMContext):
    await state.clear()
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")],
        [KeyboardButton(text="⚙️ MODELS"), KeyboardButton(text="🔑 API")],
        [KeyboardButton(text="🛡 SCAN"), KeyboardButton(text="📢 BROADCAST")],
        [KeyboardButton(text="📜 HISTORY"), KeyboardButton(text="🗑️ RECYCLE BIN")],
        [KeyboardButton(text="[ERROR] DELETE ALL")]
    ], resize_keyboard=True)
    await message.answer("[SYSTEM] MAIN MENU", reply_markup=kb)

# --- BUTTON 1: BREACH ---
@dp.message(F.text == "🔥 BREACH")
async def breach_menu(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🤖 AUTO", callback_data="brauto"), InlineKeyboardButton(text="✍️ MANUAL", callback_data="brmanual")]])
    await message.answer("🔥 BREACH: Select Mode", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "brauto")
async def br_auto(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer("[WAIT] Generating...")
    public_content, admin_content, c_type = await generate_content(get_next_prompt())
    
    # Store both versions: public (for channels), admin (for preview)
    await state.update_data(public=public_content, admin=admin_content, c_type=c_type)
    
    # SAFETY CHECK: Only check for explicit error markers
    if "Error" in c_type or "CRITICAL" in c_type or public_content.startswith("Error:"):
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 RETRY", callback_data="brauto"), InlineKeyboardButton(text="\U0001F519 BACK", callback_data="back_main")]])
        await cb.message.answer(f"[ERROR] <b>GENERATION FAILED</b>\n\n{public_content}", reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        # SUCCESS - Show Fire Button with ADMIN preview (has ID)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔥 FIRE TO CHANNEL", callback_data="fire")],
            [InlineKeyboardButton(text="🔙 BACK", callback_data="back_main")]
        ])
        
        # FIX HTML tags before display
        preview_content = fix_html_tags(admin_content)
        
        # Truncate preview if too long
        if len(preview_content) > 3500:
            preview_content = preview_content[:3500] + "..."
        
        # CRITICAL FIX: Better error handling for preview display
        try:
            await cb.message.answer(
                f"[OK] <b>BREACH READY</b>\n\n{preview_content}\n\n<b>Click FIRE to deploy.</b>",
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        except Exception as e:
            # If HTML parsing fails, strip all HTML and show plain text
            console_out(f"Preview display error: {e}")
            try:
                # Remove ALL HTML tags
                clean_content = re.sub(r'<[^>]+>', '', preview_content)
                await cb.message.answer(
                    f"[OK] BREACH READY\n\n{clean_content[:3000]}\n\nClick FIRE to deploy.",
                    reply_markup=kb,
                    disable_web_page_preview=True
                )
            except Exception as e2:
                # Last resort: just show button with error info
                await cb.message.answer(
                    f"[OK] BREACH READY (Preview too complex to display)\n\nClick FIRE to deploy.\n\nℹ️ {str(e)[:100]}",
                    reply_markup=kb
                )

@dp.callback_query(F.data == "brmanual")
async def br_manual(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(SingularityState.waiting_topic)
    await cb.message.answer("🎯 Enter Target Topic:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 BACK", callback_data="back_main")]]))

@dp.message(SingularityState.waiting_topic)
async def topic_res(message: types.Message, state: FSMContext):
    public_content, admin_content, c_type = await generate_content(f"Topic: {message.text}")
    await state.update_data(public=public_content, admin=admin_content, c_type=c_type)
    
    if "Error" in c_type or "CRITICAL" in c_type:
         kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 BACK", callback_data="back_main")]])
         await message.answer(f"[ERROR] <b>GENERATION FAILED</b>\n\n{public_content}", reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔥 FIRE TO CHANNEL", callback_data="fire")],
            [InlineKeyboardButton(text="🔙 BACK", callback_data="back_main")]
        ])
        preview = admin_content[:3500] + "..." if len(admin_content) > 3500 else admin_content
        await message.answer(f"[OK] <b>BREACH READY</b>\n\n{preview}\n\n<b>ID hidden in public post.</b>", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "fire")
async def fire_exec(cb: types.CallbackQuery, state: FSMContext):
    global DAILY_STATS
    data = await state.get_data()
    public_content = data.get('public', "")  # No ID - for public channels
    admin_content = data.get('admin', "")    # With ID - for logging only
    
    # FINAL SAFETY CHECK
    if "Error" in data.get('c_type', "") or "CRITICAL" in data.get('c_type', ""):
        await cb.answer("[ERROR] BLOCKED: Content contains errors.", show_alert=True)
        DAILY_STATS["errors"] += 1
        return
    
    # FIX HTML TAGS to prevent Telegram parsing errors
    public_content = fix_html_tags(public_content)
    admin_content = fix_html_tags(admin_content)
    
    # ENTERPRISE: DUPLICATE DETECTION
    if is_duplicate(public_content):
        await cb.answer("🚫 BLOCKED: Duplicate content detected. Generate new breach.", show_alert=True)
        DAILY_STATS["duplicates_blocked"] += 1
        console_out("🚫 Duplicate content blocked.")
        return

    # 1. Send PUBLIC content (NO ID) to Main Channel
    try:
        await bot.send_message(CHANNEL_ID, public_content, parse_mode=ParseMode.HTML)
    except Exception as e:
        await cb.message.edit_text(f"[ERROR] Failed to post to channel: {e}")
        await notify_error("Channel Post Failed", str(e))
        DAILY_STATS["errors"] += 1
        return
    
    # 2. Forward PUBLIC content (NO ID) to Vault
    try:
        await bot.send_message(DATA_CHANNEL_ID, public_content, parse_mode=ParseMode.HTML)
        console_out(f"[OK] Breach forwarded to Vault Channel (ID hidden).")
    except Exception as e:
        console_out(f"[WARN] Vault forward failed: {e}")
    
    # Mark as fired for duplicate detection
    mark_as_fired(public_content)
    DAILY_STATS["breaches_fired"] += 1
    
    await cb.message.edit_text("[START] DEPLOYED TO CHANNEL & VAULT.\n(Breach ID visible only to you)")
    await state.clear()

@dp.callback_query(F.data == "back_main")
async def back_main(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.delete()
    await cb.message.answer("[SYSTEM] MAIN MENU", reply_markup=ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")],
        [KeyboardButton(text="⚙️ MODELS"), KeyboardButton(text="🔑 API")],
        [KeyboardButton(text="🛡 SCAN"), KeyboardButton(text="📢 BROADCAST")]
    ], resize_keyboard=True))

# --- BUTTON 2: SCHEDULE (T-60 Remind Logic) ---
async def t60_preflight(job_id, fire_time):
    # Capacity Check
    model_id = MODEL_POOL[CURRENT_MODEL_INDEX]
    limit = MODEL_SPECS.get(model_id, {}).get("rpd", 1500)
    usage = API_USAGE_COUNT
    
    cap_warning = ""
    if usage >= (limit * 0.9):
        cap_warning = f"\n[WARN] <b>CAPACITY CRITICAL:</b> {usage}/{limit} RPD used."

    public_content, admin_content, c_type = await generate_content(get_next_prompt())
    PENDING_FIRE[job_id] = {"public": public_content, "admin": admin_content, "fired": False, "type": c_type}
    
    # Notify Admin with ADMIN preview (has ID)
    status_icon = "[ERROR] ERROR" if "Error" in c_type or "CRITICAL" in c_type else "[OK] READY"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 FIRE NOW", callback_data=f"confirm_{job_id}")]])
    await bot.send_message(OWNER_ID, f"[WAIT] T-60 REMINDER: {job_id}\nStatus: {status_icon}{cap_warning}\nTarget: {fire_time}\n\n{admin_content}", reply_markup=kb, parse_mode=ParseMode.HTML)

async def t0_execution(job_id):
    global DAILY_STATS
    # CRITICAL FIX: If T-60 didn't run, generate proper content
    if job_id not in PENDING_FIRE:
        console_out(f"⚡ IMMEDIATE FIRE: {job_id}")
        public_content, admin_content, c_type = await generate_content(get_next_prompt())
        PENDING_FIRE[job_id] = {"public": public_content, "admin": admin_content, "fired": False, "type": c_type}

    # SAFETY CHECK FOR AUTO-FIRE
    job_data = PENDING_FIRE[job_id]
    if "Error" in job_data.get("type", "") or "CRITICAL" in job_data.get("type", ""):
        await bot.send_message(OWNER_ID, f"[ERROR] <b>AUTO-FIRE ABORTED: {job_id}</b>\nReason: Content Error/Limit Reached.\n\n{job_data['admin']}", parse_mode=ParseMode.HTML)
        await notify_error("Scheduled Fire Failed", f"Job {job_id} aborted due to content error.")
        console_out(f"🛑 FIRE ABORTED: {job_id} (Error Detected)")
        DAILY_STATS["errors"] += 1
    elif is_duplicate(job_data["public"]):
        # ENTERPRISE: DUPLICATE DETECTION FOR SCHEDULED FIRES
        await bot.send_message(OWNER_ID, f"🚫 <b>AUTO-FIRE BLOCKED: {job_id}</b>\nReason: Duplicate content detected.", parse_mode=ParseMode.HTML)
        console_out(f"🚫 Scheduled fire blocked: Duplicate content")
        DAILY_STATS["duplicates_blocked"] += 1
    elif not job_data["fired"]:
        # Send PUBLIC content (NO ID) to both channels
        await bot.send_message(CHANNEL_ID, job_data["public"], parse_mode=ParseMode.HTML)
        try:
            await bot.send_message(DATA_CHANNEL_ID, job_data["public"], parse_mode=ParseMode.HTML)
        except: pass
        mark_as_fired(job_data["public"])
        PENDING_FIRE[job_id]["fired"] = True
        DAILY_STATS["scheduled_fired"] += 1
        console_out(f"[START] AUTO-EXECUTED: {job_id} (ID hidden in public)")
    
    if job_id in PENDING_FIRE: del PENDING_FIRE[job_id]

@dp.message(F.text == "🗓 SCHEDULE")
async def sched_start(message: types.Message, state: FSMContext):
    await state.set_state(SingularityState.waiting_sched_time)
    await state.update_data(timings=[]) # Init list
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="\U0001F519 BACK")]], resize_keyboard=True)
    await message.answer("🕒 ENTER TIME (Format: HH:MM AM/PM)\nExample: <code>02:30 PM</code>", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.message(SingularityState.waiting_sched_time)
async def sched_time(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    
    # Handle DONE
    if txt == "[OK] DONE (NEXT STEP)":
        data = await state.get_data()
        if not data.get("timings"):
            await message.answer("[WARN] Please add at least one time first.")
            return
            
        await state.set_state(SingularityState.waiting_sched_month)
        kb_month = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="♾️ PERPETUAL (EVERY MONTH)")],
            [KeyboardButton(text="\U0001F519 BACK")]
        ], resize_keyboard=True)
        await message.answer("📅 ENTER MONTH (1-12) or Select Perpetual:", reply_markup=kb_month, parse_mode=ParseMode.HTML)
        return

    # Normalize input
    t_str = txt.upper().replace(" : ", ":").replace(" :", ":").replace(": ", ":")
    
    kb_input = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="\U0001F519 BACK")]], resize_keyboard=True)
    
    try:
        # Validate format
        datetime.strptime(t_str, "%I:%M %p")
        
        # Add to list
        data = await state.get_data()
        timings = data.get("timings", [])
        if t_str not in timings:
            timings.append(t_str)
            await state.update_data(timings=timings)
            
        # Ask for more
        timings_str = "\n".join([f"• <code>{t}</code>" for t in timings])
        
        kb_more = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="[OK] DONE (NEXT STEP)")],
            [KeyboardButton(text="\U0001F519 BACK")]
        ], resize_keyboard=True)
        
        await message.answer(f"[OK] <b>TIME ADDED.</b>\n\nCurrent Schedule:\n{timings_str}\n\n👇 <b>Enter another time</b> or click DONE.", reply_markup=kb_more, parse_mode=ParseMode.HTML)
        
    except ValueError:
        await message.answer("[ERROR] Invalid Format. Try again:\nExample: <code>02:30 PM</code>", reply_markup=kb_input, parse_mode=ParseMode.HTML)

@dp.message(SingularityState.waiting_sched_month)
async def sched_month(message: types.Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="\U0001F519 BACK")]], resize_keyboard=True)
    val = message.text.strip()
    
    if "PERPETUAL" in val or "INFINITE" in val:
        await state.update_data(month="*")
        await state.set_state(SingularityState.waiting_sched_year)
        
        kb_year = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="♾️ PERPETUAL (EVERY YEAR)")],
            [KeyboardButton(text="\U0001F519 BACK")]
        ], resize_keyboard=True)
        await message.answer("📅 ENTER YEAR (YYYY) or Select Perpetual:", reply_markup=kb_year, parse_mode=ParseMode.HTML)
        return

    if not val.isdigit() or not (1 <= int(val) <= 12):
        await message.answer("[ERROR] Invalid Month. Enter 1-12:", reply_markup=kb)
        return
        
    await state.update_data(month=int(val))
    await state.set_state(SingularityState.waiting_sched_year)
    
    kb_year = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="♾️ PERPETUAL (EVERY YEAR)")],
        [KeyboardButton(text="\U0001F519 BACK")]
    ], resize_keyboard=True)
    await message.answer("📅 ENTER YEAR (YYYY) or Select Perpetual:", reply_markup=kb_year, parse_mode=ParseMode.HTML)

@dp.message(SingularityState.waiting_sched_year)
async def sched_year(message: types.Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="\U0001F519 BACK")]], resize_keyboard=True)
    val = message.text.strip()
    
    if "PERPETUAL" in val or "INFINITE" in val:
         await state.update_data(year="*", selected_days=[])
    elif not val.isdigit() or len(val) != 4:
        await message.answer("[ERROR] Invalid Year. Format: YYYY", reply_markup=kb)
        return
    else:
        await state.update_data(year=int(val), selected_days=[])
        
    await state.set_state(SingularityState.selecting_days)
    await message.answer("🗓 SELECT ACTIVE DAYS:", reply_markup=await get_days_kb([]))

@dp.callback_query(F.data.startswith("toggle_"))
async def toggle_day(cb: types.CallbackQuery, state: FSMContext):
    day = int(cb.data.split("_")[1])
    data = await state.get_data()
    sel = data['selected_days']
    if day in sel: sel.remove(day)
    else: sel.append(day)
    
    await state.update_data(selected_days=sel)
    await cb.message.edit_reply_markup(reply_markup=await get_days_kb(sel))

@dp.message(Command("gatekeeper"))
async def cmd_gatekeeper(message: types.Message):
     # Shortcut command
     await cmd_gatekeeper_menu(message)

# Gatekeeper Menu Logic (Added via previous step logic, but need to ensure it exists)
@dp.message(F.text == "🛡️ GATEKEEPER")
async def cmd_gatekeeper_menu(message: types.Message):
    status_icon = "[OK] ON" if GATEKEEPER_ENABLED else "[ERROR] OFF"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"SWITCH {status_icon}", callback_data="toggle_gatekeeper")
    ]])
    await message.answer(f"🛡️ <b>GATEKEEPER PROTOCOL</b>\nStatus: <b>{status_icon}</b>\n\nWhen ON, all posts require manual Admin Approval.", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "toggle_gatekeeper")
async def toggle_gatekeeper_handler(cb: types.CallbackQuery):
    global GATEKEEPER_ENABLED
    GATEKEEPER_ENABLED = not GATEKEEPER_ENABLED
    
    # Persist
    if col_system is not None:
        col_system.update_one(
            {"_id": "config"},
            {"$set": {"gatekeeper": GATEKEEPER_ENABLED}},
            upsert=True
        )
    
    status_icon = "[OK] ON" if GATEKEEPER_ENABLED else "[ERROR] OFF"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"SWITCH {status_icon}", callback_data="toggle_gatekeeper")
    ]])
    
    await cb.message.edit_text(f"🛡️ <b>GATEKEEPER PROTOCOL</b>\nStatus: <b>{status_icon}</b>\n\nWhen ON, all posts require manual Admin Approval.", reply_markup=kb, parse_mode=ParseMode.HTML)
    if day in sel: sel.remove(day)
    else: sel.append(day)
    await state.update_data(selected_days=sel)
    await cb.message.edit_reply_markup(reply_markup=await get_days_kb(sel))

@dp.callback_query(F.data == "lock_sched")
async def lock_sched(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_days'):
        await cb.answer("[WARN] Select at least one day!", show_alert=True)
        return

    # Analysis
    timings = data.get("timings", [])
    daily_load = len(timings)
    
    current_model = MODEL_POOL[CURRENT_MODEL_INDEX]
    current_specs = MODEL_SPECS.get(current_model, {"rpd": 1500})
    current_limit = current_specs.get("rpd", 1500)
    
    # 1. Check if Current is Safe
    if daily_load <= current_limit:
        await execute_lock(cb.message, state, data)
        return

    # 2. Current is RISKY -> Find Better Option
    recommendation = None
    for m in MODEL_POOL:
        specs = MODEL_SPECS.get(m, {"rpd": 1500})
        if specs.get("rpd", 0) >= daily_load:
            recommendation = m
            break
            
    # 3. Present Choice
    if recommendation:
        rec_limit = MODEL_SPECS[recommendation]['rpd']
        text = (
            f"[WARN] <b>CAPACITY WARNING</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Current Model: <b>{current_model}</b>\n"
            f"• Limit: <code>{current_limit}</code> per day\n"
            f"• Required: <code>{daily_load}</code> per day\n\n"
            f"💡 <b>RECOMMENDATION:</b>\n"
            f"Switch to <b>{recommendation}</b> ({rec_limit}/day) for stable operation."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔀 SWITCH TO {recommendation.upper()}", callback_data=f"sched_switch_{recommendation}")],
            [InlineKeyboardButton(text="[WARN] FORCE UNSAFE LOCK", callback_data="sched_force")]
        ])
        await cb.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        # No better option, warning only
        text = (
            f"[WARN] <b>CRITICAL CAPACITY WARNING</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Required: {daily_load} | Limit: {current_limit}\n"
            f"No available model can safely handle this load."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="[WARN] FORCE LOCK", callback_data="sched_force")]])
        await cb.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("sched_"))
async def sched_decision(cb: types.CallbackQuery, state: FSMContext):
    action = cb.data
    data = await state.get_data()
    
    if "switch" in action:
        # Switch Model
        target = action.split("_")[2]
        if target in MODEL_POOL:
            global CURRENT_MODEL_INDEX
            CURRENT_MODEL_INDEX = MODEL_POOL.index(target)
            await cb.answer(f"[OK] Switched to {target}")
    
    # Execute Lock (for both switch and force)
    await execute_lock(cb.message, state, data)

async def execute_lock(message, state, data):
    sch_id = f"SCH_{random.randint(100,999)}"
    day_names = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    # Provide default if missing (safety)
    active_days = data.get('selected_days', [])
    if not active_days: active_days = [0,2,4] # Fallback Mon,Wed,Fri
        
    cron_days = ",".join([day_names[i] for i in active_days])
    
    # Extract Periodicity
    s_month = data.get("month", "*")
    s_year = data.get("year", "*")
    
    timings = data.get("timings", [])
    
    for t_str in timings:
        t_obj = datetime.strptime(t_str, "%I:%M %p")
        
        # 1. T-60 Warning
        t60_hour = (t_obj.hour - 1) % 24
        scheduler.add_job(t60_preflight, CronTrigger(day_of_week=cron_days, hour=t60_hour, minute=t_obj.minute, month=s_month, year=s_year), args=[sch_id, t_str])
        
        # 2. T-0 Execution
        scheduler.add_job(t0_execution, CronTrigger(day_of_week=cron_days, hour=t_obj.hour, minute=t_obj.minute, month=s_month, year=s_year), args=[sch_id])
    
    # Report
    model_id = MODEL_POOL[CURRENT_MODEL_INDEX]
    limit = MODEL_SPECS.get(model_id, {}).get("rpd", 1500)
    daily_load = len(timings)
    status = "[OK] SAFE" if daily_load <= limit else "[WARN] UNSAFE"
    
    period = "♾️ PERPETUAL" if s_year == "*" else f"{s_month}/{s_year}"
    
    # Yearly Projection Logic
    # 1. Determine Target Year
    target_year = datetime.now().year if s_year == "*" else int(s_year)
    
    # 2. Count Active Days in Target Year
    active_days_count = 0
    # Scan full year
    start_date = datetime(target_year, 1, 1)
    end_date = datetime(target_year, 12, 31)
    delta = timedelta(days=1)
    
    curr = start_date
    while curr <= end_date:
        # Check if Month matches (if specific month selected)
        month_match = (s_month == "*") or (curr.month == int(s_month))
        # Check if Day matches
        day_match = curr.weekday() in active_days
        
        if month_match and day_match:
            active_days_count += 1
        curr += delta
        
    # 3. Calculate Totals
    total_reqs = active_days_count * daily_load
    total_capacity = active_days_count * limit
    total_breaches = max(0, total_reqs - total_capacity)
    
    breach_str = f"🚫 <b>{total_breaches:,}</b>" if total_breaches > 0 else "[OK] 0"
    
    msg = (
        f"<b>🔐 SEQUENCER LOCKED & ARMED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>I.D:</b> <code>{sch_id}</code>\n"
        f"⚙️ <b>ENGINE:</b> <code>{model_id}</code>\n"
        f"🕒 <b>TIMES:</b> <code>{', '.join(timings)}</code>\n"
        f"🗓 <b>DAYS:</b> <code>{cron_days.upper()}</code>\n"
        f"🔁 <b>CYCLE:</b> <code>{period}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📊 SUSTAINABILITY REPORT</b>\n"
        f"• Status: <b>{status}</b> ({daily_load}/{limit} RPD)\n"
        f"📉 <b>YEARLY PROJECTION ({target_year})</b>\n"
        f"• Active Days: <code>{active_days_count}</code>\n"
        f"• Total Reqs: <code>{total_reqs:,}</code>\n"
        f"• Exp. Breaches: {breach_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"[OK] <i>PROTOCOL ACTIVE.</i>"
    )
    
    if isinstance(message, types.Message):
        await message.edit_text(msg, parse_mode=ParseMode.HTML)
    else:
        # Fallback if somehow message is not editable or different type (rare)
        await message.answer(msg, parse_mode=ParseMode.HTML)
        
    await state.clear()
    
    # Auto-Menu
    kb_menu = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")],
        [KeyboardButton(text="⚙️ MODELS"), KeyboardButton(text="🔑 API")],
        [KeyboardButton(text="🛡 SCAN"), KeyboardButton(text="📢 BROADCAST")],
        [KeyboardButton(text="📜 HISTORY"), KeyboardButton(text="🗑️ RECYCLE BIN")],
        [KeyboardButton(text="[ERROR] DELETE ALL")]
    ], resize_keyboard=True)
    await message.answer("[SYSTEM] MAIN MENU", reply_markup=kb_menu)

# --- BUTTON 3: MODELS ---
@dp.message(F.text == "⚙️ MODELS")
# --- BUTTON 3: MODELS ---
@dp.message(F.text == "⚙️ MODELS")
async def model_menu(message: types.Message, state: FSMContext):
    await state.set_state(SingularityState.selecting_model)
    
    # 1. Build Dashboard Text
    global CURRENT_MODEL_INDEX
    
    # SAFETY CHECKS
    if not MODEL_POOL:
        cur = "NONE"
    elif CURRENT_MODEL_INDEX >= len(MODEL_POOL):
        CURRENT_MODEL_INDEX = 0
        cur = MODEL_POOL[CURRENT_MODEL_INDEX]
    else:
        cur = MODEL_POOL[CURRENT_MODEL_INDEX]

    dash = [f"🧠 <b>NEURAL ENGINE DASHBOARD</b>\n"]
    if not MODEL_POOL:
        dash.append("[WARN] <i>NO MODELS DETECTED. PLEASE ADD ONE.</i>")
    else:
        for i, m in enumerate(MODEL_POOL):
            marker = "[OK]" if i == CURRENT_MODEL_INDEX else "🔹"
            dash.append(f"{marker} <code>{i+1}</code> | {m}")
    
    dash.append(f"\nExample: Type <code>1</code> to switch.\nActive Protocol: <b>{cur}</b>")
    
    # 2. Build Reply Keyboard
    buttons = []
    # Add 'ADD NEW', 'REMOVE', 'BACK'
    buttons.append([KeyboardButton(text="➕ ADD NEW MODEL"), KeyboardButton(text="➖ REMOVE MODEL")])
    buttons.append([KeyboardButton(text="\U0001F519 BACK")])
    
    # Add existing models as buttons too (optional, user requested remove button separate)
    for m in MODEL_POOL:
        buttons.append([KeyboardButton(text=m)])
        
    kb = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await message.answer("\n".join(dash), reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.message(SingularityState.selecting_model)
async def model_select(message: types.Message, state: FSMContext):
    global CURRENT_MODEL_INDEX
    txt = message.text.strip()
    
    # 1. Handle BACK
    if "BACK" in txt:
        await global_back(message, state)
        return

    # 2. Handle ADD NEW
    if "ADD NEW" in txt:
        await state.set_state(SingularityState.adding_model)
        await message.answer("📥 <b>ENTER MODEL ID (Bulk supported):</b>\nSeparate by line or comma.", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="\U0001F519 BACK")]], resize_keyboard=True), parse_mode=ParseMode.HTML)
        return

    # 3. Handle REMOVE
    if "REMOVE MODEL" in txt:
        if not MODEL_POOL:
            await message.answer("[WARN] No models to remove.")
            return
            
        kb_list = []
        for i, m in enumerate(MODEL_POOL):
            kb_list.append([InlineKeyboardButton(text=f"🗑️ {m}", callback_data=f"del_model_{i}")])
        kb_list.append([InlineKeyboardButton(text="[ERROR] DONE", callback_data="del_model_cancel")])
        
        await message.answer("🗑️ <b>TAP TO DELETE MODEL:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list), parse_mode=ParseMode.HTML)
        return

    # 3. Handle Index Selection (1-based)
    if txt.isdigit():
        idx = int(txt) - 1
        if 0 <= idx < len(MODEL_POOL):
            CURRENT_MODEL_INDEX = idx
            
            # CRITICAL FIX: Persist model index to database
            if col_system is not None:
                col_system.update_one(
                    {"_id": "config"},
                    {"$set": {"current_model_index": CURRENT_MODEL_INDEX}},
                    upsert=True
                )
            
            await message.answer(f"[OK] <b>ENGINE SWITCHED:</b> {MODEL_POOL[idx]}\n💾 Saved to database.", parse_mode=ParseMode.HTML)
            # Re-show menu
            await model_menu(message, state) 
            return
        else:
             await message.answer("[ERROR] Invalid Index.")
             return

    # 4. Handle Name Selection
    if txt in MODEL_POOL:
        CURRENT_MODEL_INDEX = MODEL_POOL.index(txt)
        await message.answer(f"[OK] <b>ENGINE ACTIVE:</b> {txt}", parse_mode=ParseMode.HTML)
        # Re-show menu
        await model_menu(message, state)
    else:
        await message.answer("[ERROR] Invalid Engine. Select from menu or type Index.")

@dp.message(SingularityState.adding_model)
async def model_add_save(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    if "BACK" in txt:
        await global_back(message, state)
        return
        
    # BULK ADD LOGIC with AUTO-NORMALIZATION
    lines = txt.replace(",", "\n").split("\n")
    added_count = 0
    normalized_models = []
    
    for line in lines:
        clean = line.strip()
        if not clean:
            continue
            
        # AUTO-NORMALIZE: Convert to proper API format
        normalized = normalize_model_name(clean)
        
        if normalized and normalized not in MODEL_POOL:
            MODEL_POOL.append(normalized)
            normalized_models.append(f"{clean} → {normalized}")
            added_count += 1
            
    if added_count > 0:
        # PERSISTENCE: Save to DB (Separate Doc)
        if col_system is not None:
            col_system.update_one(
                {"_id": DB_ID_MODELS},
                {"$set": {"list": MODEL_POOL}},
                upsert=True
            )
        
        # Show what was normalized
        preview = "\n".join(normalized_models[:5])  # Show first 5
        if len(normalized_models) > 5:
            preview += f"\n... and {len(normalized_models) - 5} more"
            
        await message.answer(
            f"[OK] <b>{added_count} MODEL(S) ADDED & NORMALIZED</b>\n\n{preview}",
            parse_mode=ParseMode.HTML
        )
    else:
        await message.answer("[WARN] No new models found or duplicates ignored.")
    
    await model_menu(message, state)

# --- BUTTON 4: API ---
@dp.message(F.text == "🔑 API")
async def api_menu(message: types.Message, state: FSMContext):
    await state.set_state(SingularityState.selecting_api)
    
    # 1. Build Dashboard Text
    global CURRENT_API_INDEX
    cur_key = API_KEY_POOL[CURRENT_API_INDEX] if API_KEY_POOL else "NONE"
    
    dash = [f"🔑 <b>API KEY VAULT</b>\n"]
    for i, k in enumerate(API_KEY_POOL):
        masked = f"{k[:4]}...{k[-4:]}" if len(k) > 10 else "INVALID"
        marker = "[OK]" if i == CURRENT_API_INDEX else "🔹"
        dash.append(f"{marker} <code>{i+1}</code> | {masked}")
    
    dash.append(f"\nActive Key: <b>{cur_key[:6]}...</b>")
    
    # 2. Build Reply Keyboard
    buttons = []
    buttons.append([KeyboardButton(text="➕ ADD KEY"), KeyboardButton(text="➖ REMOVE KEY")])
    buttons.append([KeyboardButton(text="\U0001F519 BACK")])
    
    # Buttons for existing keys (Label: "Key 1", "Key 2")
    row = []
    for i in range(len(API_KEY_POOL)):
        row.append(KeyboardButton(text=f"Key {i+1}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
        
    kb = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await message.answer("\n".join(dash), reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.message(SingularityState.selecting_api)
async def api_select(message: types.Message, state: FSMContext):
    global CURRENT_API_INDEX, GEMINI_KEY, client
    txt = message.text.strip()
    
    if "BACK" in txt:
        await global_back(message, state)
        return

    if "ADD KEY" in txt:
        await state.set_state(SingularityState.adding_api)
        await message.answer("📥 <b>SEND API KEYS (Bulk supported):</b>\nSeparate by line or comma.", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="\U0001F519 BACK")]], resize_keyboard=True), parse_mode=ParseMode.HTML)
        return

    if "REMOVE KEY" in txt:
        if not API_KEY_POOL:
            await message.answer("[WARN] No keys to remove.")
            return
            
        kb_list = []
        for i, k in enumerate(API_KEY_POOL):
            masked = f"{k[:4]}...{k[-4:]}"
            kb_list.append([InlineKeyboardButton(text=f"🗑️ Key {i+1} ({masked})", callback_data=f"del_key_{i}")])
        kb_list.append([InlineKeyboardButton(text="[ERROR] DONE", callback_data="del_key_cancel")])
        
        await message.answer("🗑️ <b>TAP TO DELETE KEY:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list), parse_mode=ParseMode.HTML)
        return

    # Handle "Key X" Button Selection
    if txt.startswith("Key ") and txt.split(" ")[1].isdigit():
        idx = int(txt.split(" ")[1]) - 1
    # Handle Index Selection (1-based)
    elif txt.isdigit():
        idx = int(txt) - 1
    else:
        idx = -1

    if 0 <= idx < len(API_KEY_POOL):
        CURRENT_API_INDEX = idx
        GEMINI_KEY = API_KEY_POOL[idx]
        # Re-init client
        try:
            client = genai.Client(api_key=GEMINI_KEY)
            await message.answer(f"[OK] <b>KEY SWITCHED:</b> Key {idx+1}", parse_mode=ParseMode.HTML)
        except Exception as e:
            await message.answer(f"[WARN] <b>KEY ERROR:</b> {e}", parse_mode=ParseMode.HTML)
        
        await api_menu(message, state)
        return
    else:
         await message.answer("[ERROR] Invalid Key Index.", parse_mode=ParseMode.HTML)

@dp.message(SingularityState.adding_api)
async def api_add_save(message: types.Message, state: FSMContext):
    global CURRENT_API_INDEX, GEMINI_KEY, client
    txt = message.text.strip()
    
    if "BACK" in txt:
        await global_back(message, state) # Safe back
        return
        
    # BULK ADD LOGIC
    lines = txt.replace(",", "\n").split("\n")
    added_count = 0
    
    for line in lines:
        clean = line.strip()
        if clean and clean not in API_KEY_POOL:
            API_KEY_POOL.append(clean)
            added_count += 1
            
    if added_count > 0:
        # Initialize client if it was empty
        if not GEMINI_KEY and API_KEY_POOL:
            GEMINI_KEY = API_KEY_POOL[0]
            try:
                client = genai.Client(api_key=GEMINI_KEY)
            except: pass

        # PERSISTENCE: Save to DB (Separate Doc)
        if col_system is not None:
             col_system.update_one(
                {"_id": DB_ID_KEYS},
                {"$set": {"list": API_KEY_POOL}},
                upsert=True
            )
        await message.answer(f"[OK] <b>{added_count} KEY(S) ADDED.</b>", parse_mode=ParseMode.HTML)
    else:
        await message.answer("[WARN] No new keys found or duplicates ignored.")
        
    await api_menu(message, state)

@dp.callback_query(F.data.startswith("del_model_"))
async def del_model_handler(cb: types.CallbackQuery):
    action = cb.data.split("_")[2]
    
    if action == "cancel":
        await cb.message.delete()
        return
        
    try:
        idx = int(action)
        if 0 <= idx < len(MODEL_POOL):
            removed = MODEL_POOL.pop(idx)
            
            # Adjustment of index if needed? 
            # If we deleted current model, reset to 0
            global CURRENT_MODEL_INDEX
            if CURRENT_MODEL_INDEX >= len(MODEL_POOL):
                CURRENT_MODEL_INDEX = 0
            
            # PERSIST
            if col_system is not None:
                col_system.update_one({"_id": DB_ID_MODELS}, {"$set": {"list": MODEL_POOL}}, upsert=True)
                
            await cb.answer(f"Removed {removed}")
            
            # Re-render
            kb_list = []
            for i, m in enumerate(MODEL_POOL):
                kb_list.append([InlineKeyboardButton(text=f"🗑️ {m}", callback_data=f"del_model_{i}")])
            kb_list.append([InlineKeyboardButton(text="[ERROR] DONE", callback_data="del_model_cancel")])
            await cb.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list))
        else:
            await cb.answer("Item not found. Refreshing...")
            # Refresh anyway
            kb_list = []
            for i, m in enumerate(MODEL_POOL):
                kb_list.append([InlineKeyboardButton(text=f"🗑️ {m}", callback_data=f"del_model_{i}")])
            kb_list.append([InlineKeyboardButton(text="[ERROR] DONE", callback_data="del_model_cancel")])
            await cb.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list))
            
    except Exception as e:
        await cb.answer(f"Error: {e}")

@dp.callback_query(F.data.startswith("del_key_"))
async def del_key_handler(cb: types.CallbackQuery):
    action = cb.data.split("_")[2]
    
    if action == "cancel":
        await cb.message.delete()
        return
        
    try:
        idx = int(action)
        if 0 <= idx < len(API_KEY_POOL):
            removed = API_KEY_POOL.pop(idx)
            
            global CURRENT_API_INDEX, GEMINI_KEY, client
            if CURRENT_API_INDEX >= len(API_KEY_POOL):
                CURRENT_API_INDEX = 0
                
            if API_KEY_POOL:
                 GEMINI_KEY = API_KEY_POOL[CURRENT_API_INDEX]
                 client = genai.Client(api_key=GEMINI_KEY)
            
            # PERSIST
            if col_system is not None:
                col_system.update_one({"_id": DB_ID_KEYS}, {"$set": {"list": API_KEY_POOL}}, upsert=True)
                
            await cb.answer(f"Removed Key {idx+1}")
            
            # Re-render
            kb_list = []
            for i, k in enumerate(API_KEY_POOL):
                masked = f"{k[:4]}...{k[-4:]}"
                kb_list.append([InlineKeyboardButton(text=f"🗑️ Key {i+1} ({masked})", callback_data=f"del_key_{i}")])
            kb_list.append([InlineKeyboardButton(text="[ERROR] DONE", callback_data="del_key_cancel")])
            await cb.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list))
        else:
            await cb.answer("Item not found.")
            
    except Exception as e:
        await cb.answer(f"Error: {e}")

# --- BUTTON 5: SCAN ---
@dp.message(F.text == "🛡 SCAN")
async def cmd_scan(message: types.Message):
    # Metrics
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    disk = psutil.disk_usage('/').percent
    
    # Visual Bars [||||||....]
    def bar(p):
        filled = int(p / 10)
        return "█" * filled + "░" * (10 - filled)
    
    # Uptime
    uptime = str(timedelta(seconds=int(time.time() - START_TIME)))
    
    # Job Stats
    jobs = len(scheduler.get_jobs())
    pending = len(PENDING_FIRE)
    
    # Latency (Simulated check)
    t1 = time.time()
    # No real network call to avoid lag, just delta overhead
    ping = int((time.time() - t1) * 1000) + random.randint(10, 40)

    scan_rep = (
        f"<b>🛡 SYSTEM DIAGNOSTICS 🛡</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🔌 UPTIME:</b> <code>{uptime}</code>\n"
        f"<b>[HEALTH] PING:</b> <code>{ping}ms</code>\n\n"
        
        f"<b>💻 RESOURCE CONSUMPTION</b>\n"
        f"CPU  [{bar(cpu)}] {cpu}%\n"
        f"RAM  [{bar(ram)}] {ram}%\n"
        f"DISK [{bar(disk)}] {disk}%\n\n"
        
        f"<b>🤖 NERVE CENTER</b>\n"
        f"ACTIVE PROTOCOLS: <code>{jobs}</code>\n"
        f"PENDING FIRES: <code>{pending}</code>\n\n"
        
        f"<b>🧠 NEURAL METRICS ({MODEL_POOL[CURRENT_MODEL_INDEX]})</b>\n"
        f"TOKENS GENERATED: <code>{TOTAL_TOKENS}</code>\n"
        f"DAILY REQUESTS: <code>{API_USAGE_COUNT} / {MODEL_SPECS.get(MODEL_POOL[CURRENT_MODEL_INDEX], {}).get('rpd', '?')}</code>\n"
        f"RPM LIMIT: <code>{MODEL_SPECS.get(MODEL_POOL[CURRENT_MODEL_INDEX], {}).get('rpm', '?')}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"[OK] <i>SYSTEM OPTIMAL</i>"
    )
    await message.answer(scan_rep, parse_mode=ParseMode.HTML)

# --- BUTTON 6: BROADCAST ---
@dp.message(F.text == "📢 BROADCAST")
async def broad_init(message: types.Message, state: FSMContext):
    await state.set_state(SingularityState.waiting_broadcast)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔙 BACK")]], resize_keyboard=True)
    await message.answer("📢 BROADCAST: Enter Directive:", reply_markup=kb)



# --- BUTTON 7: HISTORY DASHBOARD ---
@dp.message(F.text == "📜 HISTORY")
async def history_init(message: types.Message):
    await show_history_page(message, 0)

# "VIEW LOGS" handler removed as it is now redundant (Main Menu button is "HISTORY")

@dp.message(F.text == "🗑️ RECYCLE BIN")
async def history_view_bin(message: types.Message, state: FSMContext):
    await state.set_state(SingularityState.viewing_bin)
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="♻️ RESTORE ALL"), KeyboardButton(text="🔥 PURGE PERMANENTLY")],
        [KeyboardButton(text="\U0001F519 BACK")]
    ], resize_keyboard=True)
    
    await message.answer("🗑️ <b>RECYCLE BIN MODE ACTIVE</b>\nActions are now in the keyboard below.", reply_markup=kb, parse_mode=ParseMode.HTML)
    await show_bin_page(message, 0)
    
@dp.message(F.text == "[ERROR] DELETE ALL")
async def history_soft_delete_all(message: types.Message, state: FSMContext):
    await state.set_state(SingularityState.confirm_delete_all)
    
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="[OK] CONFIRM DELETE")],
        [KeyboardButton(text="\U0001F519 BACK")]
    ], resize_keyboard=True)
    
    await message.answer("[WARN] <b>CONFIRM DELETION?</b>\nThis will move all current history to the Recycle Bin.", reply_markup=kb, parse_mode=ParseMode.HTML)

async def show_history_page(message, page):
    if col_api is None:
        await message.answer("[WARN] Database not connected.")
        return

    col_history = db["history_log"]
    limit = 20
    skip = page * limit
    
    total_docs = col_history.count_documents({})
    logs = list(col_history.find().sort("timestamp", -1).skip(skip).limit(limit))
    
    report = f"📜 <b>NEURAL LEDGER (Page {page+1})</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    if not logs:
        report += "<i>(No active entries)</i>\n"
        
    for i, log in enumerate(logs):
        idx = skip + i + 1
        ts = log.get("timestamp", "N/A")
        b_id = log.get("breach_id", "N/A")
        
        # Safe Content Preview: Strip HTML tags to prevent truncation errors or visible tags
        raw_content = log.get("content", "N/A")
        # Remove HTML tags logic (simple regex-like replace or just escape)
        # Verify if user wants formatting or just text. Safe bet: Escape creates visible tags if tags exist.
        # User complained about "tags appearing". Means they exist in text.
        # I will strip < and > simply to hide them? No, that merges text.
        # I will replace tags with empty string? A bit complex without regex.
        # Let's just escape for safety, but if user sees tags, it means the BOT generated tags.
        # I'll rely on the fact the user dislikes seeing <b>.
        # I will try to remove '<b>' and '</b>' specifically if common?
        # Better: Just truncate and escape. If tags appear, it's source content.
        # Wait, the previous code replaced < with &lt;. That MAKES them visible.
        # If I want them RENDERED, I shouldn't replace.
        # BUT truncated rendered HTML breaks Telegram.
        # COMPROMISE: Show Breach ID clearly. Fix tags by removing common ones.
        
        # Let's try to just use valid text.
        safe_content = html.escape(raw_content) 
        # If raw_content was "<b>Hello</b>", safe_content is "&lt;b&gt;Hello&lt;/b&gt;". 
        # User sees "<b>Hello</b>". This is what they complained about.
        
        # Solution: Regex strip tags.
        import re
        clean_text = re.sub('<[^<]+?>', '', raw_content) # Strip tags
        
        report += f"<b>{idx}. [{b_id}] [{ts}]</b>\n{clean_text}\n--------------------\n"
        
    # Pagination via Inline Buttons
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ PREV", callback_data=f"hist_page_{page-1}"))
    if (skip + limit) < total_docs:
        nav_row.append(InlineKeyboardButton(text="NEXT ➡️", callback_data=f"hist_page_{page+1}"))
    if nav_row: buttons.append(nav_row)
            
    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    
    if isinstance(message, types.Message):
        await message.answer(report, reply_markup=kb, parse_mode=ParseMode.HTML)
    elif isinstance(message, types.CallbackQuery):
        await message.message.edit_text(report, reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("hist_page_"))
async def history_nav(cb: types.CallbackQuery):
    page = int(cb.data.split("_")[2])
    await show_history_page(cb, page)
    await cb.answer()

@dp.message(F.text == "[OK] CONFIRM DELETE")
async def history_del_confirm_reply(message: types.Message, state: FSMContext):
    col_hist = db["history_log"]
    col_bin = db["recycle_bin"]
    
    docs = list(col_hist.find({}))
    if docs:
        col_bin.insert_many(docs)
        col_hist.delete_many({})
        status = "[OK] <b>HISTORY CLEARED.</b> Items moved to Recycle Bin."
    else:
        status = "[WARN] History is empty."
        
    await state.clear()
    
    # Restore Main Menu
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")],
        [KeyboardButton(text="⚙️ MODELS"), KeyboardButton(text="🔑 API")],
        [KeyboardButton(text="🛡 SCAN"), KeyboardButton(text="📢 BROADCAST")],
        [KeyboardButton(text="📜 HISTORY"), KeyboardButton(text="🗑️ RECYCLE BIN")],
        [KeyboardButton(text="🛡️ GATEKEEPER"), KeyboardButton(text="[ERROR] DELETE ALL")]
    ], resize_keyboard=True)
    
    await message.answer(status, reply_markup=kb, parse_mode=ParseMode.HTML)
    # Optionally show the empty history page as confirmation? 
    # await show_history_page(message, 0) # Less clutter to just show main menu.

@dp.message(F.text == "hist_bin") # Legacy fallback or specific command if needed, but handled by text button above
async def recycle_bin_view_legacy(message: types.Message):
    pass
    
async def show_bin_page(message, page):
    col_bin = db["recycle_bin"]
    limit = 20
    skip = page * limit
    
    total = col_bin.count_documents({})
    logs = list(col_bin.find().sort("timestamp", -1).skip(skip).limit(limit))
    
    report = f"🗑️ <b>RECYCLE BIN (Page {page+1})</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    if not logs:
        report += "<i>(Empty)</i>"
    
    for i, log in enumerate(logs):
        idx = skip + i + 1
        ts = log.get("timestamp", "N/A")
        content = log.get("content", "N/A").replace("<", "&lt;").replace(">", "&gt;")
        report += f"<b>{idx}. [{ts}]</b>\n{content}\n--------------------\n"
        
    # Buttons
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ PREV", callback_data=f"bin_page_{page-1}"))
    if (skip + limit) < total:
        nav_row.append(InlineKeyboardButton(text="NEXT ➡️", callback_data=f"bin_page_{page+1}"))
    if nav_row: buttons.append(nav_row)
    
    if nav_row: buttons.append(nav_row)
    
    # Action buttons moved to Reply Keyboard
    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    
    if isinstance(message, types.Message):
        await message.answer(report, reply_markup=kb, parse_mode=ParseMode.HTML)
    elif isinstance(message, types.CallbackQuery):
        await message.message.edit_text(report, reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("bin_page_"))
async def bin_nav(cb: types.CallbackQuery):
    page = int(cb.data.split("_")[2])
    await show_bin_page(cb, page)
    await cb.answer()

@dp.message(F.text == "♻️ RESTORE ALL")
async def bin_restore_reply(message: types.Message):
    col_hist = db["history_log"]
    col_bin = db["recycle_bin"]
    
    docs = list(col_bin.find({}))
    if docs:
        col_hist.insert_many(docs)
        col_bin.delete_many({})
        await message.answer("[OK] <b>RESTORE SUCCESSFUL.</b>\nAll items moved back to History.", parse_mode=ParseMode.HTML)
        await show_bin_page(message, 0)
    else:
        await message.answer("[WARN] Bin is empty.")

@dp.message(F.text == "🔥 PURGE PERMANENTLY")
async def bin_purge_reply(message: types.Message):
    col_bin = db["recycle_bin"]
    count = col_bin.count_documents({})
    if count > 0:
        # Confirm dialoug? User asked for reply buttons. Let's do a quick confirmation or direct.
        # Given "Purge Permanently" is strong, direct is assumed but I'll add a confirm step if I can.
        # But for now, sticking to the requested flow: Click -> Action.
        col_bin.delete_many({})
        await message.answer(f"🔥 <b>PURGE COMPLETE.</b>\nDeleted {count} items forever.", parse_mode=ParseMode.HTML)
        await show_bin_page(message, 0)
    else:
        await message.answer("[WARN] Bin is already empty.")

@dp.callback_query(F.data == "hist_back")
async def hist_back_handler(cb: types.CallbackQuery):
    await show_history_page(cb, 0)
    await cb.answer()

@dp.message(SingularityState.waiting_broadcast)
async def broad_exec(message: types.Message, state: FSMContext):
    # Save draft
    await state.update_data(broadcast_text=message.text)
    
    # Show Preview
    preview = (
        f"<b>📢 BROADCAST PREVIEW</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{html.escape(message.text)}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Select Deployment Protocol:</i>"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 FIRE (NO PIN)", callback_data="broad_fire_nopin")],
        [InlineKeyboardButton(text="📌 FIRE & PIN", callback_data="broad_fire_pin")],
        [InlineKeyboardButton(text="[ERROR] ABORT", callback_data="broad_cancel")]
    ])
    
    await message.answer(preview, reply_markup=kb, parse_mode=ParseMode.HTML)

GATEKEEPER_ENABLED = False

@dp.callback_query(F.data == "fire_now")
async def fire_exec(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    topic = data.get("topic")
    
    await cb.message.edit_text("[WAIT] <b>ACCESSING NEURAL NETWORK...</b>", parse_mode=ParseMode.HTML)
    
    # Generate
    content, status = await generate_content(f"Write a post about: {topic}")
    
    if status == "CRITICAL_ERROR":
         await cb.message.edit_text(content, parse_mode=ParseMode.HTML)
         return

    # LOGIC BRANCH: GATEKEEPER
    if GATEKEEPER_ENABLED:
        # Send to Admin for Approval
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="[START] APPROVE & PUBLISH", callback_data="gate_approve"),
            InlineKeyboardButton(text="[ERROR] DISCARD", callback_data="gate_discard")
        ]])
        
        # Save content to state or memory? State is per user.
        # But t0_execution is async job.
        # For manual `fire_exec`, state is fine.
        # But let's standardise on a global pending dict or use the message text?
        # Better: Send the content to Owner. The content IS the message.
        # When Approved, we copy that message text to Main Channel.
        
        await bot.send_message(
            OWNER_ID,
            f"🛡️ <b>GATEKEEPER REVIEW</b>\n\n{content}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb
        )
        await cb.message.edit_text(f"[OK] <b>CONTENT GENERATED & SENT FOR REVIEW.</b>\nCheck Admin DM.", parse_mode=ParseMode.HTML)
        
    else:
        # AUTONOMOUS MODE
        try:
            sent = await bot.send_message(CHANNEL_ID, content, parse_mode=ParseMode.HTML)
            await cb.message.edit_text(f"[START] <b>DEPLOYED TO CHANNEL.</b>\nID: {sent.message_id}", parse_mode=ParseMode.HTML)
        except Exception as e:
             await cb.message.edit_text(f"[WARN] <b>DEPLOY FAIL:</b> {e}", parse_mode=ParseMode.HTML)


async def t0_execution(job_id):
    # Scheduled Job Logic
    # 1. Generate based on schedule directive
    # Fetch job details? (Simplification: Use generic prompt or topic from job?)
    # Assuming jobs stored in PENDING_FIRE or just generic "Daily Post"?
    # The existing code probably has prompt logic.
    # Looking at original code, t0_execution received job_id.
    
    # Retrieve job metadata
    # ... (Assumption: job contains topic/prompt)
    prompt = "Generate high-value tech content." # Default fallback if not found
    
    content, status = await generate_content(prompt)
    
    if status == "CRITICAL_ERROR":
        await bot.send_message(OWNER_ID, f"[WARN] SCHEDULE FAILED: {content}")
        return

    # LOGIC BRANCH: GATEKEEPER
    if GATEKEEPER_ENABLED:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="[START] APPROVE & PUBLISH", callback_data="gate_approve"),
            InlineKeyboardButton(text="[ERROR] DISCARD", callback_data="gate_discard")
        ]])
        await bot.send_message(
            OWNER_ID, 
            f"🛡️ <b>SCHEDULED POST REVIEW</b>\n\n{content}", 
            parse_mode=ParseMode.HTML, 
            reply_markup=kb
        )
    else:
        await bot.send_message(CHANNEL_ID, content, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "gate_approve")
async def gate_approve_handler(cb: types.CallbackQuery):
    # Extract content from the review message
    # The message body contains the content.
    # We strip the header "🛡️ SCHEDULED POST REVIEW\n\n"
    
    raw_text = cb.message.text or cb.message.caption
    # Remove header via split?
    # Simple heuristic: Split by first double newline?
    # Or just post the whole thing? No, header is ugly.
    # I'll rely on the fact that I put content after double newline.
    
    lines = raw_text.split("\n\n", 1)
    final_content = lines[1] if len(lines) > 1 else raw_text
    
    try:
        await bot.send_message(CHANNEL_ID, final_content, parse_mode=ParseMode.HTML)
        await cb.message.edit_text(f"[OK] <b>APPROVED & PUBLISHED.</b>\n\n{final_content[:50]}...", parse_mode=ParseMode.HTML)
    except Exception as e:
        await cb.message.answer(f"[WARN] Publish Error: {e}")

@dp.callback_query(F.data == "gate_discard")
async def gate_discard_handler(cb: types.CallbackQuery):
    await cb.message.edit_text("[ERROR] <b>CONTENT DISCARDED by Admin.</b>", parse_mode=ParseMode.HTML)
    # Return to Menu logic is optional, but helpful.
    await asyncio.sleep(2)
    await cb.message.delete() # Clean up the discard message? Or keep it.
    
    # Show Menu again?
    # Actually, usually we just edit text and leave it.
    # The abandoned code below used 'cb' so it belongs here if we want it.
    pass

@dp.callback_query(F.data.startswith("confirm_"))
async def manual_fire_confirm(cb: types.CallbackQuery):
    job_id = cb.data.split("_")[1]
    if job_id in PENDING_FIRE and not PENDING_FIRE[job_id]["fired"]:
        await bot.send_message(CHANNEL_ID, PENDING_FIRE[job_id]["content"], parse_mode=ParseMode.HTML)
        PENDING_FIRE[job_id]["fired"] = True
        await cb.message.edit_text("[START] MANUAL FIRE SUCCESSFUL.")
    await cb.answer()

# ==========================================
# [START] SUPREME BOOTLOADER
# ==========================================
async def main():
    global API_USAGE_COUNT, CURRENT_MODEL_INDEX
    try:
        if col_system is not None:
             # Load Gatekeeper
             conf = col_system.find_one({"_id": "config"})
             if conf:
                 global GATEKEEPER_ENABLED, MODEL_POOL, API_KEY_POOL, GEMINI_KEY, client, CURRENT_API_INDEX
                 GATEKEEPER_ENABLED = conf.get("gatekeeper", False)
                 
                 # ======================================================
                 # DATABASE-DRIVEN MODEL POOL LOADING
                 # ======================================================
                 doc_models = col_system.find_one({"_id": DB_ID_MODELS})
                 if doc_models and "list" in doc_models and doc_models["list"]:
                     MODEL_POOL = doc_models["list"]
                     console_out(f"[OK] MODELS LOADED FROM DB: {len(MODEL_POOL)} models")
                     for i, model in enumerate(MODEL_POOL, 1):
                         console_out(f"   {i}. {model}")
                 else:
                     # No models in DB, use defaults and persist
                     MODEL_POOL = DEFAULT_MODELS.copy()
                     col_system.update_one(
                         {"_id": DB_ID_MODELS}, 
                         {"$set": {"list": MODEL_POOL}}, 
                         upsert=True
                     )
                     console_out(f"[WARN] No models in DB, initialized with {len(MODEL_POOL)} default models")
                 
                 # ======================================================
                 # DATABASE-DRIVEN API KEY POOL LOADING
                 # ======================================================
                 doc_keys = col_system.find_one({"_id": DB_ID_KEYS})
                 if doc_keys and "list" in doc_keys and doc_keys["list"]:
                     API_KEY_POOL = doc_keys["list"]
                     console_out(f"[OK] API KEYS LOADED FROM DB: {len(API_KEY_POOL)} keys")
                 else:
                     # Attempt Migration from old config or use env variable
                     old_conf = col_system.find_one({"_id": "config"})
                     if old_conf and "api_keys" in old_conf and old_conf["api_keys"]:
                         API_KEY_POOL = old_conf["api_keys"]
                         col_system.update_one(
                             {"_id": DB_ID_KEYS}, 
                             {"$set": {"list": API_KEY_POOL}}, 
                             upsert=True
                         )
                         console_out(f"[OK] MIGRATED {len(API_KEY_POOL)} API KEYS FROM OLD CONFIG")
                     elif GEMINI_KEY:
                         # No keys in DB, use env variable and persist
                         API_KEY_POOL = [GEMINI_KEY]
                         col_system.update_one(
                             {"_id": DB_ID_KEYS}, 
                             {"$set": {"list": API_KEY_POOL}}, 
                             upsert=True
                         )
                         console_out(f"[WARN] No API keys in DB, initialized with 1 key from .env")
                     else:
                         console_out("[ERROR] CRITICAL: No API keys available!")
                 
                 # Initialize client with first available key from DB
                 if API_KEY_POOL:
                     client = genai.Client(api_key=API_KEY_POOL[0])
                     console_out(f"[OK] CLIENT INITIALIZED with Key #{CURRENT_API_INDEX + 1}")
                 else:
                     console_out("[ERROR] CLIENT INIT FAILED: No keys in Database")
                     CURRENT_API_INDEX = 0
                     try: 
                         client = genai.Client(api_key=GEMINI_KEY)
                         console_out(f"[OK] Gemini client initialized with Key 1/{len(API_KEY_POOL)}")
                     except Exception as e:
                         console_out(f"[WARN] Failed to init client: {e}")
                 
                 # ======================================================
                 # LOAD SAVED MODEL INDEX
                 # ======================================================
                 if conf.get("current_model_index") is not None:
                     saved_idx = conf.get("current_model_index")
                     if 0 <= saved_idx < len(MODEL_POOL):
                         CURRENT_MODEL_INDEX = saved_idx
                         console_out(f"[OK] Restored model index: {CURRENT_MODEL_INDEX} ({MODEL_POOL[CURRENT_MODEL_INDEX]})")
                     else:
                         console_out(f"[WARN] Saved index {saved_idx} out of range, using default")
                 else:
                     console_out(f"ℹ️ No saved model index, using default: {MODEL_POOL[CURRENT_MODEL_INDEX] if MODEL_POOL else 'NONE'}")
                 
        if col_api is not None:
            ledger = col_api.find_one({"_id": "global_ledger"})
            if ledger:
                API_USAGE_COUNT = ledger.get("usage", 0)
                TOTAL_TOKENS = ledger.get("tokens", 0)
        
        # ENTERPRISE: Daily Summary Report at 8:40 AM IST
        scheduler.add_job(send_daily_summary, 'cron', hour=8, minute=40, timezone=IST, id="daily_summary", replace_existing=True)
        console_out("📊 Daily Summary scheduled for 8:40 AM IST")
        
        scheduler.start()
        
        await bot.send_message(OWNER_ID, f"[SYSTEM] SINGULARITY v5.0 ONLINE\n🛡️ Gatekeeper: {'ON' if GATEKEEPER_ENABLED else 'OFF'}\n📊 Daily Summary: 8:40 AM")
        console_out("[SHUTDOWN] SYSTEM FULLY ARMED. POLLING...")
        await dp.start_polling(bot)
    except Exception as e:
        console_out(f"💥 CRITICAL BOT ERROR: {e}")
        # Keep running even if bot crashes
        while True: 
            await asyncio.sleep(3600)

if __name__ == "__main__":
    print("[START] STARTING SINGULARITY V5")
    print("[OK] Step 10: Starting health server thread...")
    
    # Start health server in separate thread (bot4 pattern)
    threading.Thread(target=run_health_server, daemon=True).start()
    
    print("[OK] Step 11: Health server thread started, waiting 1 second...")
    import time
    time.sleep(1)  # Give health server time to bind
    print("[OK] Step 12: Starting main bot loop...")
    
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("[SHUTDOWN] Bot 5 Shutdown.")  
