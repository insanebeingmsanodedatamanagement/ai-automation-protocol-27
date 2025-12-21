import asyncio
import logging
import random
import html

import uuid
import re
import os
import aiohttp
import json
import io
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Any
import psutil
from aiogram.filters import Command, StateFilter, or_f
import time
import google.generativeai as genai
from aiogram.filters import or_f
import pymongo

# Track when the bot was launched
START_TIME = time.time()
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError

# ==========================================
# ⚡ CONFIGURATION
# ==========================================
BOT_TOKEN = os.getenv("BOT_5_TOKEN")
GEMINI_KEY = os.getenv("GEMINI_KEY")
MONGO_URI = os.getenv("MONGO_URI")

if not all([BOT_TOKEN, GEMINI_KEY, MONGO_URI]):
    print("❌ Bot 5 Error: Missing AI or Bot credentials in Render Environment!")
CHANNEL_ID = -1003480585973 
LOG_CHANNEL_ID = -1003689609198 
OWNER_ID = 6988593629 


bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
scheduler = AsyncIOScheduler()

# MongoDB Connection
db_client = None
db = None
col_system_stats = None
col_api_ledger = None
col_vault = None
col_schedules = None
col_settings = None

# ==========================================
# 🛠 SETUP
# ==========================================
def connect_db():
    global db_client, db, col_system_stats, col_api_ledger, col_vault, col_schedules, col_settings
    try:
        db_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = db_client["MSANodeDB"]
        col_system_stats = db["system_stats"]
        col_api_ledger = db["api_ledger"]
        col_vault = db["vault"]
        col_schedules = db["schedules"]
        col_settings = db["settings"]
        db_client.server_info()
        logging.info("MongoDB connected successfully")
        return True
    except Exception as e:
        logging.error(f"MongoDB Connect Error: {e}")
        return False

connect_db()
# Global variables for bot state
CURRENT_MODEL_INDEX = 0 
#1. State Definition (Must be above handlers)
class APIState(StatesGroup):
    waiting_api = State()

@dp.message(or_f(F.text.contains("API"), Command("api")), StateFilter("*"))
async def api_management(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return

    # 1. Clear any old state before starting
    await state.clear()

    global GEMINI_KEY
    key_hash = GEMINI_KEY[-8:]

    ledger = col_api_ledger.find_one({"key_hash": key_hash})
    current_usage = ledger.get("usage_count", 0) if ledger else 0

    # Inline Keyboard with Cancel/Back button
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 CANCEL / BACK", callback_data="cancel_api")]
    ])
    
    await message.answer(
        "📊 <b>API TELEMETRY REPORT</b>\n"
        "────────────────────\n"
        f"KEY: <code>****{key_hash}</code>\n"
        f"LIFETIME USAGE: <code>{current_usage} Requests</code>\n"
        f"EST. QUOTA: <code>Free Tier (~1.5k/day)</code>\n"
        "────────────────────\n"
        "📥 <b>Enter NEW Key or press BACK:</b>",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )
    await state.set_state(APIState.waiting_api)
# ==========================================
# 🔑 API HOT-SWAP (GHOST ROTATION)
# ==========================================
@dp.callback_query(F.data == "cancel_api")
async def cancel_handler(cb: types.CallbackQuery, state: FSMContext):
    """Resets the bot state and returns to menu."""
    await state.clear()
    await cb.message.edit_text("<b>🔙 NAVIGATION RESET.</b>\nSystem on Standby.", parse_mode=ParseMode.HTML)
    await cb.answer("State Cleared")
@dp.message(or_f(F.text.contains("API"), Command("api")), StateFilter("*"))
async def api_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    global GEMINI_KEY
    await state.clear()
    
    # Mask the key so it's safe if someone is looking over your shoulder
    masked_key = f"{GEMINI_KEY[:8]}****{GEMINI_KEY[-4:]}"
    
    await message.answer(
        "🔑 <b>API MANAGEMENT PROTOCOL</b>\n"
        f"CURRENT KEY: <code>{masked_key}</code>\n"
        "────────────────────\n"
        "📥 <b>Enter the NEW Gemini API Key:</b>\n"
        "<i>Note: This will re-initialize the AI Engine immediately.</i>",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(APIState.waiting_api)

@dp.message(APIState.waiting_api)
async def api_update(message: types.Message, state: FSMContext):
    global GEMINI_KEY, model
    new_key = message.text.strip()
    
    # Validation: Gemini keys usually start with 'AIza'
    if not new_key.startswith("AIza") or len(new_key) < 20:
        await message.answer("❌ <b>INVALID KEY:</b> That does not look like a valid Gemini API Key.")
        return

    try:
        # 1. Update Global Variable
        GEMINI_KEY = new_key
        
        # 2. Re-configure the Library
        genai.configure(api_key=GEMINI_KEY)
        
        # 3. Reset the Model instance
        # This forces generate_content() to re-build the model with the new key
        model = None 
        
        await message.answer(
            "🚀 <b>API ROTATION SUCCESSFUL</b>\n"
            "The Ghost Infrastructure is now utilizing the new credentials.\n"
            "────────────────────\n"
            "<b>STATUS:</b> Operational",
            parse_mode=ParseMode.HTML
        )
        console_out("System: API Key Rotated.")
        
    except Exception as e:
        await message.answer(f"❌ <b>RE-INIT FAILED:</b> {html.escape(str(e))}")
    
    await state.clear()
# ==========================================
# VIRTUAL CONSOLE STORAGE (MUST BE AT TOP)
# ==========================================

def console_out(text):
    global CONSOLE_LOGS
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {text}"
    CONSOLE_LOGS.append(entry)
    if len(CONSOLE_LOGS) > 12: 
        CONSOLE_LOGS.pop(0)
    logging.info(text)

async def get_api_usage_safe():
    try:
        stats = col_system_stats.find_one({"_id": 1})
        if not stats:
            col_system_stats.insert_one({
                "_id": 1,
                "api_total": 0,
                "last_reset": datetime.now()
            })
            return 0
        return stats.get("api_total", 0)
    except Exception:
        return 0

async def increment_api_count(api_key):
    """Increments the local persistent counter for the current key."""
    key_hash = api_key[-8:] # Use last 8 chars as a unique identifier
    try:
        # Try to find existing record
        existing = col_api_ledger.find_one({"key_hash": key_hash})

        if not existing:
            # Create new record if this key hasn't been used before
            col_api_ledger.insert_one({
                "key_hash": key_hash,
                "usage_count": 1
            })
            return 1
        else:
            # Increment existing record
            new_count = existing.get("usage_count", 0) + 1
            col_api_ledger.update_one(
                {"key_hash": key_hash},
                {"$set": {"usage_count": new_count}}
            )
            return new_count
    except Exception as e:
        logging.error(f"Error incrementing API count: {e}")
        return 0

@dp.message(F.text.contains("TERMINAL"), StateFilter("*"))
async def terminal_viewer(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    try:
        await state.clear()
        uptime_seconds = int(time.time() - START_TIME)
        uptime_str = str(timedelta(seconds=uptime_seconds))
        cpu = psutil.cpu_percent()
        ram = psutil.virtual_memory().percent
        current_api = await get_api_usage_safe()
        active_jobs = len(scheduler.get_jobs())
        log_feed = "\n".join(CONSOLE_LOGS) if CONSOLE_LOGS else "System Standby: No events logged yet."
        
        text = (
            "<b>◈ MSANODE REMOTE TERMINAL ◈</b>\n"
            "────────────────────\n"
            f"STATUS: ACTIVE | UPTIME: {uptime_str}\n"
            f"CPU: {cpu}% | RAM: {ram}%\n"
            f"DATABASE: CONNECTED | JOBS: {active_jobs}\n"
            f"API USAGE: {current_api}/1500\n"
            "────────────────────\n"
            "LIVE FEED:\n"
            f"<code>{log_feed}</code>\n"
            "────────────────────"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="REFRESH", callback_data="refresh_term")]])
        await message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"TERMINAL ERROR: {str(e)}")

@dp.callback_query(F.data == "refresh_term")
async def refresh_terminal(cb: types.CallbackQuery, state: FSMContext):
    uptime_seconds = int(time.time() - START_TIME)
    uptime_str = str(timedelta(seconds=uptime_seconds))
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    current_api = await get_api_usage_safe()
    active_jobs = len(scheduler.get_jobs())
    log_feed = "\n".join(CONSOLE_LOGS) if CONSOLE_LOGS else "Awaiting system events..."

    new_text = (
        "<b>◈ MSANODE REMOTE TERMINAL ◈</b>\n"
        "────────────────────\n"
        f"STATUS: ACTIVE | UPTIME: {uptime_str}\n"
        f"CPU: {cpu}% | RAM: {ram}%\n"
        f"DATABASE: CONNECTED | JOBS: {active_jobs}\n"
        f"API USAGE: {current_api}/1500\n"
        "────────────────────\n"
        "<b>LIVE FEED:</b>\n"
        f"<code>{log_feed}</code>\n"
        "────────────────────"
    )
    try:
        await cb.message.edit_text(new_text, reply_markup=cb.message.reply_markup, parse_mode=ParseMode.HTML)
        await cb.answer("Terminal Synchronized.")
    except:
        await cb.answer()

# ==========================================
# 🧠 ORACLE PROMPT ENGINE (PROJECT CHIMERA)
# ==========================================
def get_system_prompt():
    return """
    ACT AS: 'MSANODE OVERLORD'. 
    GOAL: Deliver an 'Unfair Advantage' resource (AI side hustles/Arbitrage/Tactical Tech).
    TONE: Exclusive, Urgent, Technical, Military-Grade Scarcity.
    
    STRICT CONSTRAINTS:
    - COLD START: Begin IMMEDIATELY with '━━━━━━━━━━━━━━━━━━━━' followed by the '🚨 OPERATION' header.
    - NO PRE-TEXT: Never explain your mandate, never use disclaimers, and never say "I cannot generate viral content." 
    - DIRECT LINKS: Provide REAL, EXTERNAL HTTPS LINKS to the actual tools (e.g., chain.link, openai.com, ankr.com).
    - NO BRANDING: Do not create fake msanode.net links. Provide the source tools directly.
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

genai.configure(api_key=GEMINI_KEY)

async def generate_content(prompt="Generate a viral AI side hustle reel script"):
    global model, API_USAGE_COUNT
    if API_USAGE_COUNT >= 1500:
        return "⚠️ API Limit Reached", "Limit"

    try:
        if model is None:
            model = genai.GenerativeModel(
                model_name=MODEL_POOL[CURRENT_MODEL_INDEX],
                system_instruction=get_system_prompt()
            )
            console_out(f"Oracle Online: {MODEL_POOL[CURRENT_MODEL_INDEX]}")

        response = await asyncio.to_thread(model.generate_content, prompt[:500])
        raw_text = response.text if response else "No response"
        # Project Chimera Stability Shield: HTML Escaping
        clean_content = html.escape(raw_text)[:3500] 
        
        API_USAGE_COUNT += 1
        await increment_api_count(GEMINI_KEY)
        return clean_content, "AI Directive"

    except Exception as e:
        err = str(e)
        console_out(f"CRITICAL GEN ERROR: {err}")
        return f"System Error: {html.escape(err)[:100]}", "Error"

async def alchemy_transform(raw_text):
    try:
        prompt = get_system_prompt() + f"\n\nINPUT DATA:\n{raw_text}\n\nINSTRUCTION: Rewrite into MSANODE Protocol."
        resp = await model.generate_content_async(prompt)
        return re.sub(r"^(Here is|Sure).*?\n", "", resp.text, flags=re.IGNORECASE).strip()
    except: return "⚠️ Alchemy Failed."

# ==========================================
# 📡 UTILITY LOGIC
# ==========================================

async def get_next_m_code():
    """Queries the database for the total number of entries and returns the next ID."""
    try:
        count = col_vault.count_documents({})
        return f"M{count + 1}"
    except Exception as e:
        logging.error(f"Error getting next M-code: {e}")
        return "M1"
REWARD_POOL = [
    "GitHub Student Pack ($200k in Premium Infrastructure)",
    "Top 7 AI Tools that make ChatGPT look like a Toy",
    "Google's Hidden Professional Cybersecurity Certification",
    "Microsoft Azure $100 Cloud Compute Credit Hack",
    "JetBrains All Products Professional IDE Suite for Students",
    "DigitalOcean production-ready Cloud Hosting Credits",
    "The 'Student Developer' Vault of Free API Keys",
    "AWS Educate: Premium ML and Cloud Infrastructure Credits",
    "Secret Canva Pro invitation links for verified EDU users",
    "Leaked List of Websites for Free Engineering Textbooks",
    "Notion Personal Pro Lifetime License for Students",
    "3 AI Search Engines that bypass standard Web Indexing",
     "Top 5 Free AI Tools for Civil and Mechanical Engineers (Live 2026)",
    "Hottest Free AI Automation Tools for CSE Students (GitHub Leaks)",
    "Professional AutoCAD and SolidWorks Free Alternative Rewards",
    "Elite AI Debugging Tools for Senior Software Engineers (Free)",
    "MATLAB and Wolfram Alpha Pro: Free Institutional Access Hacks",
    "Affinity Professional Design Suite: Lifetime Free Access Rewards",
    "Top 5 AI PCB Design and Circuit Simulation Tools (2026 Free)",
    "Industrial IoT and Robotics Simulation Pro Tools (Free Access)",
    "Leaked Free License for JetBrains All Products (2026 Student)",
    "High-End Fluid Dynamics and Thermal Analysis Tools (Free Tier)",
    "Trending AI Code Editors killing VS Code in 2026 (Free)",
    "Professional PLC Programming and SCADA Simulation Rewards",
    "Top 5 AI SQL and Database Architecture Tools (Zero Cost)",
    "Industrial Structural Analysis Pro Software Alternatives",
    "Elite VS Code Extension Packs for God-Level Productivity",
    "Hottest Free Tools for VR/AR Development and 3D Modeling",
    "Professional Game Engine Asset Packs (Unity/Unreal Free Rewards)",
    "Top 5 AI Testing and Quality Assurance Automation Tools",
    "Hidden Free Linux Distros for High-Performance Engineering",
    "Latest Free Tools for Quantum Computing Simulation and Research",

    # --- PROFESSIONAL CERTIFICATIONS AND REWARDS (20) ---
    "Verified Free Professional Certificates from Google and Microsoft",
    "Ivy League Free Course Rewards with Shareable Certificates",
    "Hidden Cybersecurity Certification Scholarships (Full Vouchers)",
    "Cloud Architect Professional Certification Training (Zero Cost)",
    "Project Management (PMP) Free Training and Certification Packs",
    "Medical and Bioinformatics Professional Certs (Free Access)",
    "Latest Free AI Ethics and Governance Certifications (2026)",
    "Meta and IBM Professional Certificate Rewards (Scholarship Links)",
    "Oracle Cloud Infrastructure (OCI) Free Certification Vouchers",
    "Salesforce Professional Administrator Training (Free Reward)",
    "AWS Certified Cloud Practitioner: Free Prep and Exam Hacks",
    "Cisco Networking Academy: Professional Badge Rewards (Free)",
    "Red Hat Enterprise Linux (RHEL) Free Training Certification",
    "HubSpot Academy: Industrial Marketing and Sales Certifications",
    "DeepLearning.AI: Free AI Specialization Access Protocols",
    "Blockchain and Smart Contract Developer Certs (Zero Cost)",
    "Data Analyst Professional Certifications: Free Voucher Leaks",
    "Full-Stack Web Dev Professional Certs (Industry Recognized)",
    "Advanced UI/UX Certification Rewards from Top Agencies",
    "Hottest Free Certificates for 5G and Telecom Engineering",

    # --- AI AGENTS, PROMPTS AND GOD-MODE (20) ---
    "God-Mode Prompt Packs for Academic Research and Exam Bypassing",
    "Advanced Prompt Engineering for AI Image and Video Generation",
    "Elite Act As Persona Prompts for Medical and Legal Research",
    "Agentic AI Workflow Prompts for Automating Complex Tasks",
    "Jailbreak-Style Productivity Prompts for Unfiltered AI Output",
    "Reasoning Model (O1/O3) Prompt Packs for Complex Logic Math",
    "Multi-Agent Orchestration Prompts for Auto-Building Startups",
    "Custom GPT Instruction Leaks for Bypassing Academic Filters",
    "Top 5 AI Agent Platforms for Personal Workflow Automation",
    "Hottest Prompt Injection Methods for Research Synthesis",
    "Professional Midjourney and DALL-E 3 Style Prompt Vaults",
    "AI Voice Cloning and Audio Engineering God-Mode Prompts",
    "Advanced SEO and Content Strategy Prompt Packs for 2026",
    "Prompt Engineering for Automated Python Script Generation",
    "Reasoning-Chain Prompts for Solving Advanced Physics and Calc",
    "Social Engineering and Psychology Act As Prompt Packs",
    "Automated Side-Hustle Prompts for AI-Driven Revenue",
    "Prompt Packs for Generating High-End Figma UI Components",
    "Hidden Prompts for Extracting Raw Data from Obscured Sites",
    "Elite LLM Fine-Tuning Prompts for Niche-Specific Intelligence",

    # --- FOUNDER, SAAS AND FREELANCE (20) ---
    "High-Ticket Freelance Automation Templates (Notion/Figma)",
    "SaaS Startup OS: Premium Notion Templates for AI Founders",
    "Hidden Free Developer Credits: $1000+ Vercel/AWS/GCP Rewards",
    "Premium API Access Leaks: Zero-Cost LLM Inference for Devs",
    "Shopify and E-commerce Premium Theme Leaks (Free for Founders)",
    "AWS Activate for AI: $100k+ Foundation Model Credits (2026)",
    "Vercel and Supabase Founder Rewards for Next-Gen AI Apps",
    "Hottest Free High-Performance VPS for Hosting AI Agents",
    "Industrial CRM and Lead Generation Templates (Free Access)",
    "Elite Pitch Deck and Investor Presentation Templates (Figma)",
    "Founder's Tech Stack: $10k+ in Free Tool Subsidies (Verified)",
    "Premium Stripe and Fintech Infrastructure Free Tiers for Startups",
    "Automation Gold Templates for Zapier and Make.com (Free)",
    "Hottest Free Tools for Finding High-Paying Remote Clients",
    "Zero-Cost Professional Email and Domain Methods for Startups",
    "Founder's Intellectual Property (IP) and Legal Template Packs",
    "Top 5 AI Tools for Automating Client Onboarding (Free)",
    "Premium GitHub Action Workflows for Auto-Deployment Leaks",
    "SaaS Landing Page Templates with Highest Conversion Rates",
    "Exclusive Access to Private Beta Founder Tools (Invites)",


    # --- ACADEMIC WEAPONRY AND CREATIVE ASSETS (20) ---
    "3 Secret AI Tools that write Plagiarism-Free Academic Papers",
    "Leaked Course Masterclasses from Stanford and MIT (Direct Access)",
    "Hottest Free Tools for Rapid Language Learning and Translation",
    "Top 5 AI Math and Physics Solvers for Elite STEM Performance",
    "The Anki God-Tier Deck Collection for Medical and Engineering",
    "NotebookLM Advanced Protocols for Research Paper Synthesis",
    "Perplexity Pro: 1-Year Reward Links for Academic Dominance",
    "Adobe Creative Cloud Alternatives: Pro Suite Rewards (Free)",
    "Premium UI/UX Design System Templates for Figma (Live Trends)",
    "Industrial-Grade Content Creation Templates for 2026 Growth",
    "Top 5 AI Music and SFX Generation Tools (Commercial License Free)",
    "Premium TradingView Indicator Leaks for Crypto and Forex",
    "Elite Financial Modeling and Excel Mastery Templates (Free)",
    "Bloomberg Terminal Alternatives: Professional Market Data Tools",
    "High-End Cybersecurity Lab Access (TryHackMe/HackTheBox Pro)",
    "3 Secret Websites for Free High-End Engineering Textbooks",
    "Elite Resume and Portfolio Templates that Beat ATS Systems",
    "Hottest Free Tools for Automated Video Editing and Subtitling",
    "Top 5 AI Web Scraping Tools for Mass Research Automation",
    "Leaked PDF Management Pro Suites for Academic Heavy-Lifters",

    # --- SECTOR 1: AGENTIC AI & AUTONOMOUS SYSTEMS (20) ---
    "Agentic AI Workflow Blueprints for Multi-Platform Automation",
    "Top 5 Micro-LLM Deployment Rewards for Edge Computing (2026)",
    "Hottest Free Autonomous Agent Platforms for Devs (Zero Cost)",
    "Elite Prompt Packs for GPT-5 Reasoning and Deep Logic",
    "Grok-4 Technical Nuance Prompting for Competitive Intelligence",
    "Reasoning Model (o1/o3) Protocols for Complex Engineering Math",
    "AI Agent Orchestration Templates for Self-Building Startups",
    "Hidden Free Credits for 2026 Agentic Infrastructure (AgentFlow)",
    "Ambient AI Smart Office Setup Hacks for Energy Efficiency",
    "Top 5 AI Tools for Real-Time Human-AI Interaction Design",
    "Neuro-morphic Computing Simulation Tools (Free Academic Access)",
    "Leaked API Credits for Fast-Track LLM Inference (2026 Tiers)",
    "Autonomous Content Arbitrage Systems for YouTube/TikTok (Free)",
    "Elite AI Debugging Protocols for Self-Correcting Codebases",
    "Hottest Prompt Injection Defense Frameworks for AI Founders",
    "Top 5 AI Personal Assistants for High-Stakes Schedule Management",
    "AI-Powered Energy Efficient Burner Design Protocols (Mechanical)",
    "Visual Inspection AI Systems for Hardware Quality Control (Free)",
    "Automated Threat Hunting Protocols using Agentic AI (Cyber)",
    "Top 5 AI Tools for Real-Time Multi-Language Voice Translation",

    # --- SECTOR 2: PROFESSIONAL CERTS & GOVERNANCE (20) ---
    "Verified Free AI Ethics and Governance Certificates (2026)",
    "Grow with Google: AI Essentials and Prompting Rewards (Free)",
    "Microsoft Azure AI Fundamentals (AZ-900) 2026 Voucher Leaks",
    "Okta Identity and Access Management (IAM) Professional Certs",
    "Post-Quantum Cryptography Professional Prep and Vouchers",
    "ESG and Carbon Management Professional Certification (Free)",
    "FinOps Certified Practitioner Rewards for Cloud Cost Control",
    "AI-Powered Clinical Documentation Professional Certs (Medical)",
    "LegalTech AI Compliance and Contract Reviewer Certifications",
    "Digital Immune System (DIS) Architecture Training (Free Access)",
    "NIST AI Risk Management Framework Professional Badge Rewards",
    "Elite Data Privacy and GDPR 2.0 Compliance Vouchers (2026)",
    "Top 5 Free Certifications for 6G and Edge Network Engineering",
    "Sustainable Finance Professional Cert Rewards (Global Vouchers)",
    "Google Professional Workspace Administrator Cert (2026 Leaks)",
    "Certified AI Product Manager (AICPM) Free Training Protocols",
    "Blockchain-Based Data Integrity Professional Certifications",
    "Top 5 Free Certificates for Generative AI in Healthcare",
    "Enterprise AI Governance Training for Executive Leadership",
    "Hottest Free Vouchers for Advanced Penetration Testing Labs",


    # --- SECTOR 3: FOUNDER & STARTUP ARBITRAGE (20) ---
    "2026 B2B SaaS Vertical ERP Management Templates (Free)",
    "AI-Driven Legal Contract Analyzer Frameworks (Founder Leaks)",
    "ESG Dashboard Templates for Carbon Tracking and Compliance",
    "AI-Led Interview Solutions for Rapid Founder Hiring (Free)",
    "Digital Banking Experience Design Templates (Plumery Style)",
    "Browser-Based Detection and Response (SquareX) Security Hacks",
    "Construction Quality AI Management Software (Academic Rewards)",
    "Real-Time Driver Behavior and Logistics Monitoring Protocols",
    "Compliance-as-a-Service (CaaS) Blueprints for Global Startups",
    "Zero-Trust Network Access (ZTNA) Model Implementation Labs",
    "Automated Tax Collaboration Platform Templates (Founder Grade)",
    "SaaS Landing Page Templates for 2026 High-Conversion Trends",
    "Hidden Startup Subsidies: $100k+ Foundation Model Credits",
    "Founder’s Intellectual Property (IP) Protection Protocol Packs",
    "Top 5 AI Tools for Automated Market Sentiment Analysis (2026)",
    "Hottest Free VPS for 2026 Global Edge Deployment (Vercel Leaks)",
    "Elite Pitch Deck and Financial Forecasting Templates (AI-Ready)",
    "SaaS Revenue Recognition and Automated Accounting Frameworks",
    "Founder’s Guide to Zero-Cost Global Payroll Infrastructure",
    "Top 5 AI Tools for Automating SaaS Customer Success (Free)",

    # --- SECTOR 4: ACADEMIC WEAPONRY & GPA HACKS (20) ---
    "NotebookLM Advanced Protocols for Research Synthesis (Elite)",
    "3 Secret AI Tools for Plagiarism-Free Academic Paper Generation",
    "Top 5 AI Math and Physics Solvers for 2026 STEM Performance",
    "The Anki God-Tier Deck Collection for Medical and CSE (2026)",
    "Perplexity Pro: 1-Year Academic Reward Links for Students",
    "Leaked MIT and Harvard Masterclasses (Direct Reward Access)",
    "Hottest Free Tools for Rapid Technical Language Mastery",
    "Elite LaTeX and Scientific Publishing Template Packs (Zero Cost)",
    "Zero-Cost High-End Presentation Tools for Research Defense",
    "Leaked PDF Management Pro Suites for Academic Heavy-Lifters",
    "SimScale Academic Tier: Free CFD and Thermal Analysis Leaks",
    "Fusion 360 Student: Industrial Generative Design Rewards",
    "Top 5 AI Study Assistants that bypass AI Content Detectors",
    "Hottest Free Tools for Automated Study Schedule Generation",
    "Elite Academic Citation and Referencing Automation Protocols",
    "Visual-Spatial Reasoning Prompt Packs for Design and Geometry",
    "3 Secret Websites for Free High-End Engineering Textbooks",
    "Elite Resume and Portfolio Templates that Beat 2026 ATS",
    "Hottest Free Tools for Automated Video Editing for Creators",
    "Top 5 AI Web Scraping Tools for Mass Academic Research",


    # --- 🏗 INDUSTRIAL & ENGINEERING WEAPONRY (20) ---
    "Professional AutoCAD & SolidWorks Free Alternative Rewards (2026)",
    "Hottest Free AI Automation Tools for CSE Students (GitHub Leaks)",
    "Top 5 Free AI Tools for Civil & Mechanical Engineers (Live 2026)",
    "Elite AI Debugging Tools for Senior Software Engineers (Free)",
    "MATLAB & Wolfram Alpha Pro: Free Institutional Access Hacks",
    "Affinity Professional Design Suite: Lifetime Free Access Protocol",
    "Top 5 AI PCB Design & Circuit Simulation Tools (2026 Free)",
    "Industrial IoT & Robotics Simulation Pro Tools (Free Access)",
    "Leaked Free License for JetBrains All Products (2026 Student)",
    "High-End Fluid Dynamics & Thermal Analysis Tools (Free Tier)",
    "Professional PLC Programming & SCADA Simulation (Verified Free)",
    "Top 5 AI SQL & Database Architecture Tools (Zero Cost)",
    "Industrial Structural Analysis Pro Software Alternatives",
    "Elite VS Code Extension Packs for God-Level Productivity",
    "Hottest Free Tools for VR/AR Development & 3D Modeling",
    "Professional Game Engine Asset Packs (Unity/Unreal Free Rewards)",
    "Top 5 AI Testing & Quality Assurance Automation Tools",
    "Hidden Free Linux Distros for High-Performance Engineering",
    "Latest Free Tools for Quantum Computing Simulation & Research",
    "Industrial Automation & Control Systems Free Professional Training",

    # --- 🎓 PROFESSIONAL CERTIFICATIONS & VOUCHERS (20) ---
    "Verified Free Professional Certificates from Google & Microsoft (2026)",
    "Ivy League Free Course Rewards with Shareable Certificates",
    "Hidden Cybersecurity Certification Scholarships (Full Exam Vouchers)",
    "Cloud Architect Professional Certification Training (Zero Cost)",
    "Project Management (PMP) 2026 Free Training & Certification Packs",
    "Medical & Bioinformatics Professional Certs (Free Access)",
    "Latest Free AI Ethics & Governance Certifications (High Demand)",
    "Meta & IBM Professional Certificate Rewards (Scholarship Links)",
    "Oracle Cloud Infrastructure (OCI) Free Certification Vouchers",
    "Salesforce Professional Administrator Training (Free Reward)",
    "AWS Certified Cloud Practitioner: Free Prep & Exam Hacks",
    "Cisco Networking Academy: Professional Badge Rewards (Free)",
    "Red Hat Enterprise Linux (RHEL) Free Training Certification",
    "HubSpot Academy: Industrial Marketing & Sales Certifications",
    "DeepLearning.AI: Free AI Specialization Access Protocols",
    "Blockchain & Smart Contract Developer Certs (Zero Cost)",
    "Data Analyst Professional Certifications: Free Voucher Leaks",
    "Full-Stack Web Dev Professional Certs (Industry Recognized)",
    "Advanced UI/UX Certification Rewards from Top Agencies",
    "Hottest Free Certificates for 5G & Telecom Engineering",

    # --- 🧠 AI AGENTS & PROMPT WEAPONRY (20) ---
    "God-Mode Prompt Packs for Academic Research & Exam Bypassing",
    "Advanced Prompt Engineering for AI Image & Video Generation",
    "Elite 'Act As' Persona Prompts for Medical & Legal Research",
    "Agentic AI Workflow Prompts for Automating Complex Tasks",
    "Jailbreak-Style Productivity Prompts for Unfiltered AI Output",
    "Reasoning Model (O1/O3) Prompt Packs for Complex Logic Math",
    "Multi-Agent Orchestration Prompts for Auto-Building Startups",
    "Custom GPT Instruction Leaks for Bypassing Academic Filters",
    "Top 5 AI Agent Platforms for Personal Workflow Automation",
    "Hottest Prompt Injection Methods for Research Synthesis",
    "Professional Midjourney & DALL-E 3 Style Prompt Vaults",
    "AI Voice Cloning & Audio Engineering God-Mode Prompts",
    "Advanced SEO & Content Strategy Prompt Packs for 2026",
    "Prompt Engineering for Automated Python Script Generation",
    "Reasoning-Chain Prompts for Solving Advanced Physics & Calc",
    "Social Engineering & Psychology 'Act As' Prompt Packs",
    "Automated Side-Hustle Prompts for AI-Driven Revenue",
    "Prompt Packs for Generating High-End Figma UI Components",
    "Hidden Prompts for Extracting Raw Data from Obscured Sites",
    "Elite LLM Fine-Tuning Prompts for Niche-Specific Intelligence",

    # --- 🚀 FOUNDER, SAAS & REVENUE ARBITRAGE (20) ---
    "High-Ticket Freelance Automation Templates (Notion/Figma)",
    "SaaS Startup OS: Premium Notion Templates for AI Founders",
    "Hidden Free Developer Credits: $1000+ Vercel/AWS/GCP Rewards",
    "Premium API Access Leaks: Zero-Cost LLM Inference for Devs",
    "Shopify & E-commerce Premium Theme Leaks (Free for Founders)",
    "AWS Activate for AI: $100k+ Foundation Model Credits (2026)",
    "Vercel & Supabase Founder Rewards for Next-Gen AI Apps",
    "Hottest Free High-Performance VPS for Hosting AI Agents",
    "Industrial CRM & Lead Generation Templates (Free Access)",
    "Elite Pitch Deck & Investor Presentation Templates (Figma)",
    "Founder's Tech Stack: $10k+ in Free Tool Subsidies (Verified)",
    "Premium Stripe & Fintech Infrastructure Free Tiers for Startups",
    "Automation Gold Templates for Zapier & Make.com (Free)",
    "Hottest Free Tools for Finding High-Paying Remote Clients",
    "Zero-Cost Professional Email & Domain Methods for Startups",
    "Founder's Intellectual Property (IP) & Legal Template Packs",
    "Top 5 AI Tools for Automating Client Onboarding (Free)",
    "Premium GitHub Action Workflows for Auto-Deployment Leaks",
    "SaaS Landing Page Templates with Highest Conversion Rates",
    "Exclusive Access to Private Beta Founder Tools (Invites)",

    # --- 📚 ACADEMIC WEAPONRY & SPECIALIZED NICHE (20) ---
    "3 Secret AI Tools that write Plagiarism-Free Academic Papers",
    "Leaked Course Masterclasses from Stanford & MIT (Direct Access)",
    "Hottest Free Tools for Rapid Language Learning & Translation",
    "Top 5 AI Math & Physics Solvers for Elite STEM Performance",
    "The 'Anki' God-Tier Deck Collection for Medical & Engineering",
    "NotebookLM Advanced Protocols for Research Paper Synthesis",
    "Perplexity Pro: 1-Year Reward Links for Academic Dominance",
    "Adobe Creative Cloud Alternatives: Pro Suite Rewards (Free)",
    "Premium UI/UX Design System Templates for Figma (Live Trends)",
    "Industrial-Grade Content Creation Templates for 2026 Growth",
    "Top 5 AI Music & SFX Generation Tools (Commercial License Free)",
    "Premium TradingView Indicator Leaks for Crypto & Forex",
    "Elite Financial Modeling & Excel Mastery Templates (Free)",
    "Bloomberg Terminal Alternatives: Professional Market Data Tools",
    "High-End Cybersecurity Lab Access (TryHackMe/HackTheBox Pro)",
    "3 Secret Websites for Free High-End Engineering Textbooks",
    "Elite Resume & Portfolio Templates that Beat ATS Systems",
    "Hottest Free Tools for Automated Video Editing & Subtitling",
    "Top 5 AI Web Scraping Tools for Mass Research Automation",
    "Leaked PDF Management Pro Suites for Academic Heavy-Lifters",


]

async def safe_send_message(chat_id, text, reply_markup=None):
    try:
        return await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except TelegramBadRequest:
        clean_text = html.escape(text).replace("*", "").replace("`", "").replace("_", "")
        return await bot.send_message(chat_id, clean_text, parse_mode=None, reply_markup=reply_markup)
    except TelegramNetworkError:
        await asyncio.sleep(2)
        try: return await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except: return None
# ==========================================
# [!] FSM STATES (MUST BE AT THE TOP)
# ==========================================

class ScheduleState(StatesGroup):
    waiting_time = State()
    waiting_month = State()
    waiting_year = State()
    selecting_days = State()

class BreachState(StatesGroup):
    selecting_mode = State()
    waiting_topic = State()
    waiting_reaction_count = State()

class EditState(StatesGroup):
    waiting_id = State()
    waiting_text = State()

class UnsendState(StatesGroup):
    waiting_id = State()

class HurryState(StatesGroup):
    waiting_code = State()
    waiting_duration = State()

class EngagementState(StatesGroup):
    waiting_code = State()
    waiting_count = State()

class BroadcastState(StatesGroup):
    waiting_msg = State()
## ==========================================
# 🎯 ENGAGEMENT CONTROL (PRIORITY ANCHORED)
# ==========================================

# Handler for "🎯 ENGAGEMENT", "ENGAGEMENT", or "/engagement"
@dp.message(or_f(F.text.contains("ENGAGEMENT"), Command("engagement")), StateFilter("*"))
async def engagement_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # Critical: Flush previous states
    await state.clear()
    
    await message.answer(
        "🎯 <b>ENGAGEMENT GATING ACTIVATED</b>\n"
        "Enter the <b>M-Code</b> to update reaction targets:", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(EngagementState.waiting_code)

@dp.message(EngagementState.waiting_code)
async def engagement_id_received(message: types.Message, state: FSMContext):
    m_code = message.text.upper().strip()

    try:
        # Verify the M-Code exists in our Vault
        entry = col_vault.find_one({"m_code": m_code})

        if entry:
            await state.update_data(target_code=m_code, msg_id=entry.get("msg_id"))
            await message.answer(
                f"✅ <b>ENTRY FOUND:</b> <code>{m_code}</code>\n"
                f"Current Lock: <code>{entry.get('reaction_lock', 0)}x</code> 🔥 reactions.\n\n"
                "📥 <b>Enter the NEW target reaction count (0 to remove lock):</b>",
                parse_mode=ParseMode.HTML
            )
            await state.set_state(EngagementState.waiting_count)
        else:
            await message.answer(f"❌ <b>ERROR:</b> M-Code <code>{m_code}</code> not found.")
    except Exception as e:
        logging.error(f"Error verifying M-code: {e}")
        await message.answer("❌ <b>DATABASE ERROR:</b> Could not verify M-code.")
        await state.clear()

@dp.message(EngagementState.waiting_count)
async def engagement_exec(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("⚠️ <b>INVALID INPUT:</b> Enter a numerical value.")
        return

    new_count = int(message.text)
    data = await state.get_data()
    m_code = data['target_code']
    msg_id = data['msg_id']

    try:
        # 1. Update Database
        col_vault.update_one(
            {"m_code": m_code},
            {"$set": {
                "reaction_lock": new_count,
                "is_unlocked": (new_count == 0),
                "last_verified": datetime.now()
            }}
        )

        # 2. Synchronize Telegram UI
        # We refresh the buttons on the actual channel post immediately
        await bot.edit_message_reply_markup(
            chat_id=CHANNEL_ID,
            message_id=msg_id,
            reply_markup=get_engagement_markup(m_code, lock=new_count, unlocked=(new_count == 0))
        )

        await message.answer(
            f"🚀 <b>GATING UPDATED:</b> <code>{m_code}</code>\n"
            f"New Threshold: <code>{new_count}x</code> 🔥",
            parse_mode=ParseMode.HTML
        )
        console_out(f"Gating Reset: {m_code} set to {new_count}")

    except Exception as e:
        await message.answer(f"❌ <b>SYNC FAILED:</b> {html.escape(str(e))}")

    await state.clear()
    # ==========================================
# 📡 UI HELPERS (MUST BE DEFINED EARLY)
# ==========================================

def get_engagement_markup(m_code, lock=0, unlocked=False):
    """
    Generates the reaction gating buttons.
    This function must be defined BEFORE any handlers call it.
    """
    if lock > 0 and not unlocked:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔒 UNLOCK AT {lock}x 🔥 REACTIONS", callback_data=f"lockmsg_{m_code}")]
        ])
    
    # Default state if lock is 0 or already unlocked
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔓 UNLOCK SECRET BONUS HACK", callback_data=f"reveal_{m_code}")]
    ])
# ==========================================
# 🕹️ FSM STATES
# ==========================================

class ScheduleState(StatesGroup):
    waiting_time = State(); waiting_month = State(); waiting_year = State(); selecting_days = State()
class BreachState(StatesGroup):
    selecting_mode = State(); waiting_topic = State(); waiting_reaction_count = State()
class EditState(StatesGroup):
    waiting_id = State(); waiting_text = State()
class UnsendState(StatesGroup):
    waiting_id = State()
class HurryState(StatesGroup):
    waiting_code = State(); waiting_duration = State()
class EngagementState(StatesGroup):
    waiting_code = State(); waiting_count = State()
class BroadcastState(StatesGroup):
    waiting_msg = State()

# ==========================================
# 🛡️ SYSTEM TASKS & ACTIVE LISTENER
# ==========================================
# ==========================================
# 🪤 REACTION LOCK INTERFACE HANDLERS
# ==========================================

@dp.callback_query(F.data.startswith("reveal_"))
async def reveal_secret(cb: types.CallbackQuery):
    """
    Triggers when the 'UNLOCK SECRET BONUS HACK' button is clicked.
    Provides verified engineering/AI resources.
    """
    hacks = [
        "◈ VPN Bypass: Protocol Verified.", 
        "◈ EDU Email: Access Granted.", 
        "◈ Archive Script: Script mirror active.",
        "◈ Premium Repo: Branch decrypted."
    ]
    # Sends a private alert to the user who clicked
    await cb.answer(random.choice(hacks), show_alert=True)

@dp.callback_query(F.data.startswith("lockmsg_"))
async def lock_alert(cb: types.CallbackQuery):
    """
    Triggers when the locked button is clicked.
    Informs the user about the remaining requirement.
    """
    await cb.answer(
        "◈ ACCESS RESTRICTED ◈\n"
        "Requirement: Reach the 🔥 reaction target to unlock this intelligence.", 
        show_alert=True
    )

# ==========================================
# 📊 UNIVERSAL REACTION LISTENER (COUNT-ONLY)
# ==========================================

@dp.message_reaction()
async def reaction_listener(reaction: types.MessageReactionUpdated):
    """
    Counts ANY reaction emoji. Once the total count across
    all emojis hits the target, the Vault unlocks.
    """
    try:
        # Search the Vault for this message ID
        entry = col_vault.find_one({
            "msg_id": reaction.message_id,
            "is_unlocked": False
        })

        # If entry is found and a lock exists
        if entry and entry.get("reaction_lock", 0) > 0:
            # Calculate Total Count across all emoji types
            total_reactions = 0
            for r in reaction.new_reaction:
                # This counts the total number of people who reacted
                # Telegram provides a list of reaction types and their counts
                # For channels, we sum the totals provided in the update
                total_reactions += 1 # Standard count per reactor

            # Check if we hit the goal
            if total_reactions >= entry.get("reaction_lock", 0):
                # 1. Update Database Status
                col_vault.update_one(
                    {"m_code": entry.get("m_code")},
                    {"$set": {"is_unlocked": True}}
                )

                # 2. Update Channel UI
                await bot.edit_message_reply_markup(
                    chat_id=CHANNEL_ID,
                    message_id=entry.get("msg_id"),
                    reply_markup=get_engagement_markup(entry.m_code, unlocked=True)
                )
                
                # 3. Notification of Breach
                await bot.send_message(
                    LOG_CHANNEL_ID,
                    f"🔓 <b>VAULT UNLOCKED:</b> <code>{entry.get('m_code')}</code>\n"
                    f"Threshold of <code>{entry.get('reaction_lock', 0)}</code> reactions reached."
                )
    except Exception as e:
        logging.error(f"Error in reaction listener: {e}")
    """
    Asynchronous link validation to ensure intelligence assets are live.
    """
    urls = re.findall(r'(https?://[^\s)]+)', text)
    invalid = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                async with session.get(url, timeout=5) as resp:
                    if resp.status >= 400: 
                        invalid.append(url)
            except Exception:
                invalid.append(url)
    return invalid

async def self_healing_audit():
    """
    Periodic deep-scan of vault integrity.
    """
    try:
        # Get recent vault entries from MongoDB
        recent_entries = list(col_vault.find().sort("created_at", -1).limit(50))
        report = "🛡️ <b>DAILY HEALING REPORT:</b>\n"
        found = False

        for entry in recent_entries:
            bad = await validate_links(entry.get("content", ""))
            if bad:
                report += f"❌ <code>{entry.get('m_code')}</code>: {bad}\n"
                found = True

        if found:
            await bot.send_message(OWNER_ID, report, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Self-healing audit error: {e}")

async def hourly_heartbeat():
    """
    Ensures the bot and database connection remain persistent.
    """
    try:
        # Test MongoDB connection
        col_vault.find_one({}, limit=1)

        # Pull model engine info if available
        curr_eng = MODEL_POOL[CURRENT_MODEL_INDEX] if 'MODEL_POOL' in globals() else "Active"

        await bot.send_message(
            LOG_CHANNEL_ID,
            f"💓 <b>HEARTBEAT:</b> Nominal | API: {API_USAGE_COUNT}/1500 | Engine: {curr_eng}",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await bot.send_message(
            OWNER_ID,
            f"🚨 <b>SYSTEM ERROR:</b> {html.escape(str(e))}",
            parse_mode=ParseMode.HTML
        )

# ==========================================
# 🗑 UNSEND PROTOCOL (DELETION)
# ==========================================

@dp.message(F.text == "🗑 UNSEND", StateFilter("*"))
@dp.message(Command("unsend"))
async def unsend_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer(
        "🗑 <b>UNSEND INITIATED</b>\nEnter the M-Code to scrub from existence:", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(UnsendState.waiting_id)

@dp.message(UnsendState.waiting_id)
async def unsend_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    m_code = message.text.upper()
    
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(VaultEntry).where(VaultEntry.m_code == m_code))
        entry = res.scalar_one_or_none()
        
        if entry:
            try:
                await bot.delete_message(CHANNEL_ID, entry.msg_id)
                telegram_status = "Scrubbed from Channel"
            except Exception:
                telegram_status = "Channel deletion failed (too old)"
            
            await session.execute(delete(VaultEntry).where(VaultEntry.m_code == m_code))
            await session.commit()
            
            await message.answer(
                f"✅ <b>OPERATION COMPLETE</b>\nID: <code>{m_code}</code>\nStatus: {telegram_status} and Database.", 
                parse_mode=ParseMode.HTML
            )
        else:
            await message.answer(
                f"❌ <b>ERROR:</b> M-Code <code>{m_code}</code> not found.", 
                parse_mode=ParseMode.HTML
            )
    await state.clear()
# ==========================================
# 🔘 COMMANDS & GUIDE
# ==========================================
@dp.message(Command("start"), StateFilter("*"))
async def start_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # Critical: Reset any stuck states from previous errors
    await state.clear()
# Reorganized Keyboard: Tactical Command v15.0
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔥 BREACH"), KeyboardButton(text="🗓 SCHEDULE")], 
        [KeyboardButton(text="✏️ EDIT"), KeyboardButton(text="🗑 UNSEND")],
        [KeyboardButton(text="📋 LIST"), KeyboardButton(text="🎯 ENGAGEMENT")],
        [KeyboardButton(text="📢 BROADCAST"), KeyboardButton(text="🔑 API")], # Added API Hot-Swap
        [KeyboardButton(text="⚙️ MODEL"), KeyboardButton(text="📊 AUDIT")], 
        [KeyboardButton(text="📟 TERMINAL"), KeyboardButton(text="❓ GUIDE")],
        [KeyboardButton(text="🛑 PANIC")]
    ], resize_keyboard=True)
    
    # Log the access to the internal terminal
    if 'console_out' in globals():
        console_out("Master Sadiq initialized Command Center")

    await message.answer(
        "💎 <b>APEX SINGULARITY v5.0</b>\n"
        "────────────────────\n"
        "Master Sadiq, the system is fully synchronized.\n"
        "All nodes active. Awaiting your directive.", 
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )

@dp.message(F.text == "❓ GUIDE" or Command("guide"))
async def help_guide(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    
    guide = (
        "💎 <b>APEX OVERLORD SINGULARITY: TECHNICAL MANUAL</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔥 <b>OFFENSIVE: BREACH PROTOCOL</b>\n"
        "• <code>/breach</code>: Generates high-value toolkit leaks.\n"
        "• <b>AUTO:</b> AI selects resource from the elite reward pool.\n"
        "• <b>MANUAL:</b> AI generates technical content based on your niche.\n"
        "• <b>REACTION LOCK:</b> Force engagement by locking bonus hacks.\n\n"
        
        "🗓 <b>LOGISTICS: SCHEDULE SUBSYSTEM</b>\n"
        "• <code>/schedule</code>: Set precision fire times (HH:MM AM/PM).\n"
        "• <b>GUARDED FIRE:</b> T-60 notification sends you the draft 60 minutes before fire.\n\n"
        
        "📢 <b>SYNDICATE: GLOBAL BROADCAST</b>\n"
        "• <code>/broadcast</code>: Send military-formatted alerts to the entire Family.\n\n"
        
        "🎯 <b>GATING: ENGAGEMENT CONTROL</b>\n"
        "• <code>/engagement</code>: Retroactively set or update reaction targets.\n\n"
        
        "⚙️ <b>INTELLIGENCE: MODEL MANAGEMENT</b>\n"
        "• <code>/model</code>: Monitor Gemini API usage (1,500 limit). Swap engines live.\n\n"
        
        "🛡 <b>DEFENSIVE: AUDIT & SELF-HEAL</b>\n"
        "• <code>/audit</code>: Deep scan of database and system health.\n"
        "• <b>HEARTBEAT:</b> Hourly status checks in the Log Channel.\n"
        "• <b>SELF-HEAL:</b> Dead URL audit at midnight.\n\n"
        
        "🧪 <b>TRANSMUTATION: ALCHEMY ENGINE</b>\n"
        "• <b>AUTO-TRANSMUTE:</b> Forward text to bot for MSANODE Protocol rewrite.\n\n"
        
        "📦 <b>UTILITY: VAULT COMMANDS</b>\n"
        "• <code>/list</code>: View ID-locked inventory.\n"
        "• <code>/backup</code>: Instant SQLite database export.\n"
        "• <code>/edit</code>: Remote text correction via M-Code.\n"
        "• <code>/unsend</code>: Permanent deletion of a leak.\n"
        "• <code>/hurry</code>: FOMO countdown injection.\n"
        "• <code>/panic</code>: Emergency kill-switch for all tasks.\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "👑 <b>MASTER SADIQ DIRECTIVE:</b> Execute with precision."
    )
    await message.answer(guide, parse_mode=ParseMode.HTML)

@dp.message(F.text == "⚙️ MODEL" or Command("usage"))
async def model_info(message: types.Message):
    # Fixed: Referenced MODEL_POOL correctly
    curr_mod = MODEL_POOL[CURRENT_MODEL_INDEX] if 'MODEL_POOL' in globals() else "Synchronizing"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 SWAP ENGINE", callback_data="swap_engine")],
        [InlineKeyboardButton(text="📊 USAGE STATS", callback_data="api_usage")]
    ])
    await message.answer(f"⚙️ <b>ENGINE:</b> <code>{curr_mod}</code>\n💎 <b>USAGE:</b> {API_USAGE_COUNT}/1500", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "api_usage")
async def api_usage_cb(cb: types.CallbackQuery):
    await cb.answer(f"Consumed: {API_USAGE_COUNT} | Left: {1500 - API_USAGE_COUNT}", show_alert=True)

@dp.callback_query(F.data == "swap_engine")
async def swap_engine_cb(cb: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=m, callback_data=f"selmod_{i}")] for i, m in enumerate(MODEL_POOL)])
    await cb.message.edit_text("🎯 <b>SELECT NEW ENGINE:</b>", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("selmod_"))
async def sel_model_exec(cb: types.CallbackQuery):
    global CURRENT_MODEL_INDEX, model
    idx = int(cb.data.split("_")[1])
    CURRENT_MODEL_INDEX = idx
    # Hard-locked synchronization with Overlord persona
    model = genai.GenerativeModel(
        model_name=MODEL_POOL[CURRENT_MODEL_INDEX],
        system_instruction=get_system_prompt()
    )
    await cb.message.edit_text(f"✅ <b>ENGINE UPDATED:</b> <code>{MODEL_POOL[idx]}</code>", parse_mode=ParseMode.HTML)

@dp.message(F.text == "📦 BACKUP" or Command("backup"))
async def backup_mirror(message: types.Message):
    try:
        # Get all vault entries from MongoDB
        vault_entries = list(col_vault.find({}, {"_id": 0}))  # Exclude MongoDB _id field
        data = [{
            "m_code": entry.get("m_code"),
            "topic": entry.get("topic"),
            "content": entry.get("content"),
            "lock": entry.get("reaction_lock", 0)
        } for entry in vault_entries]

        json_file = io.BytesIO(json.dumps(data, indent=4).encode())
        await message.answer_document(
            BufferedInputFile(json_file.getvalue(), filename="vault_backup.json"),
            caption="🔒 <b>BACKUP MIRROR SECURED.</b>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await message.answer(f"❌ <b>BACKUP FAILED:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

# ==========================================
# 🗄️ DATABASE COLLECTIONS (MongoDB)
# ==========================================
# Collections are automatically created when first used
# No need for explicit schema definitions like SQLAlchemy

# ==========================================
# 🔄 GLOBAL API COUNTER (MongoDB)
# ==========================================
async def get_api_usage():
    try:
        stats = col_system_stats.find_one({"_id": 1})
        if not stats:
            # Initialize if first time
            col_system_stats.insert_one({
                "_id": 1,
                "api_total": 0,
                "last_reset": datetime.now()
            })
            return 0
        return stats.get("api_total", 0)
    except Exception as e:
        logging.error(f"Error getting API usage: {e}")
        return 0
    
# ==========================================
# 🔥 BREACH (STABLE v6.0 - HARD LOCKED)
# ==========================================
@dp.message(F.text == "🔥 BREACH")
async def breach_start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 AUTO", callback_data="breach_auto"), 
         InlineKeyboardButton(text="📝 MANUAL", callback_data="breach_manual")]
    ])
    await message.answer("🧨 <b>BREACH INITIALIZED</b>\nSelect generation mode:", reply_markup=kb, parse_mode=ParseMode.HTML)
    await state.set_state("SELECTING_MODE")

@dp.callback_query(F.data.startswith("breach_"))
async def breach_mode_select(cb: types.CallbackQuery, state: FSMContext):
    mode = cb.data.split("_")[1]
    if mode == "manual":
        await cb.message.edit_text("🎯 <b>TARGET:</b> Enter your niche/topic:", parse_mode=ParseMode.HTML)
        await state.set_state("WAITING_TOPIC")
    else:
        await cb.message.edit_text("🔍 <b>SYNTHESIZING...</b>", parse_mode=ParseMode.HTML)
        content, topic = await generate_content()
        await state.update_data(content=content, topic=topic)
        await cb.message.answer("🔥 <b>REACTION LOCK:</b> Enter target count (0 to skip):", parse_mode=ParseMode.HTML)
        await state.set_state("WAITING_REACTION_COUNT")

@dp.message(StateFilter("WAITING_TOPIC"))
async def breach_manual_topic(message: types.Message, state: FSMContext):
    await message.answer("🔍 <b>SYNTHESIZING...</b>", parse_mode=ParseMode.HTML)
    content, topic = await generate_content(message.text)
    await state.update_data(content=content, topic=topic)
    await message.answer("🔥 <b>REACTION LOCK:</b> Enter target count (0 to skip):", parse_mode=ParseMode.HTML)
    await state.set_state("WAITING_REACTION_COUNT")

@dp.message(StateFilter("WAITING_REACTION_COUNT"))
async def breach_final_count(message: types.Message, state: FSMContext):
    raw_text = message.text.strip()
    if not raw_text.isdigit():
        await message.answer("⚠️ <b>Numbers only.</b>", parse_mode=ParseMode.HTML)
        return

    count = int(raw_text)
    await state.update_data(reaction_lock=count)
    data = await state.get_data()
    
    preview = (
        f"<b>📑 PREVIEW (Lock: {count}x🔥)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{data.get('content', 'No Content')}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 FIRE CONFIRMED", callback_data="fire_final")]
    ])
    
    await message.answer(preview, reply_markup=kb, parse_mode=ParseMode.HTML)
    await state.set_state("BREACH_PREVIEW_STATE")

# ==========================================
# 🔥 BREACH EXECUTION (MIRROR DEPTH v13.0)
# ==========================================

@dp.callback_query(F.data == "fire_final")
async def fire_final(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    
    # 1. Sequential Identity Generation
    m_code = await get_next_m_code() 
    
    # 2. Temporal Precision Capture
    now = datetime.now()
    fire_time = now.strftime("%I:%M:%S %p")
    fire_date = now.strftime("%d-%m-%Y")
    
    try:
        # 3. Public Deployment (Main Channel)
        vault_msg = await bot.send_message(
            CHANNEL_ID, 
            data['content'], 
            parse_mode=ParseMode.HTML, 
            reply_markup=get_engagement_markup(m_code, data['reaction_lock'])
        )
        
        # 4. Persistence to MongoDB Ledger
        col_vault.insert_one({
            "m_code": m_code,
            "msg_id": vault_msg.message_id,
            "topic": data['topic'],
            "content": data['content'],
            "reaction_lock": data['reaction_lock'],
            "is_unlocked": False,
            "created_at": now,
            "last_verified": now
        })
            
        # 5. FULL MIRROR TO PRIVATE LOG CHANNEL (The Fix)
        # This sends the EXACT Vault content followed by technical metadata
        log_payload = (
            f"{data['content']}\n\n"
            f"────────────────────\n"
            f"📊 <b>DEPLOYMENT METADATA</b>\n"
            f"CODE: <code>{m_code}</code>\n"
            f"TIME: <code>{fire_time}</code>\n"
            f"DATE: <code>{fire_date}</code>\n"
            f"GATING: <code>{data['reaction_lock']}x</code> Reactions\n"
            f"STATUS: <b>VERIFIED BREACH</b>\n"
            f"────────────────────"
        )
        
        await bot.send_message(
            LOG_CHANNEL_ID, 
            log_payload, 
            parse_mode=ParseMode.HTML
        )
        
        # 6. Command UI Update
        await cb.message.edit_text(
            f"🚀 <b>BREACH SUCCESSFUL</b>\n"
            f"Identity: <code>{m_code}</code>\n"
            f"Timestamp: <code>{fire_time}</code>\n"
            f"Mirrored to Command Center.", 
            parse_mode=ParseMode.HTML
        )
        
        console_out(f"Protocol {m_code} mirrored at {fire_time}")
        await state.clear()
        
    except Exception as e:
        error_info = html.escape(str(e))
        await cb.message.answer(f"❌ <b>MIRROR FAILURE:</b> <code>{error_info}</code>")
        console_out(f"Mirror Error: {error_info}")
# ==========================================
# ==========================================
# 📋 LIST / AUDIT (STATE INDEPENDENT)
# ==========================================
@dp.message(F.text == "📋 LIST", StateFilter("*"))
async def list_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    # This line clears any "stuck" state automatically
    await state.clear()
    try:
        # Get all vault entries from MongoDB, sorted by creation date
        entries = list(col_vault.find().sort("created_at", -1))
        # Stability Shield: HTML Formatting for clean terminal aesthetics
        rep = "<b>📋 INVENTORY</b>\n" + "\n".join([f"🆔 <code>{entry.get('m_code')}</code> | 🔥 {entry.get('reaction_lock', 0)}x" for entry in entries])
        await message.answer(rep if entries else "📭 Empty.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"❌ <b>LIST ERROR:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

@dp.message(F.text == "📊 AUDIT", StateFilter("*"))
async def audit_cmd(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    # Clear state so you can use the bot again immediately
    await state.clear()
    try:
        total = col_vault.count_documents({})
        # Pull real-time API usage from global counter
        await message.answer(f"📊 <b>AUDIT:</b> {total} entries. API Usage: {API_USAGE_COUNT}/1500.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"❌ <b>AUDIT ERROR:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

# ==========================================
# 🗓 SCHEDULE HELPERS (DEFINED FIRST)
# ==========================================

async def get_days_kb(selected):
    """Generates the dynamic days-selection keyboard with Ticks/Crosses."""
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    buttons = []; row = []
    for i, d in enumerate(days):
        # Tick for selected, Cross for unselected
        text = f"✅ {d}" if i in selected else f"❌ {d}"
        row.append(InlineKeyboardButton(text=text, callback_data=f"toggle_{i}"))
        if len(row) == 3: buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(text="📥 CONFIRM DAYS", callback_data="days_done")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def show_days_keyboard(message, selected):
    """Initializes the days menu."""
    kb = await get_days_kb(selected)
    await message.answer(
        "📅 <b>SELECT DEPLOYMENT DAYS</b>\n"
        "Toggle the days for recurring fire:", 
        reply_markup=kb, 
        parse_mode=ParseMode.HTML
    )

# ==========================================
# 🗓 SCHEDULE HANDLERS (PRIORITY ANCHORED)
# ==========================================

@dp.message(or_f(F.text.contains("SCHEDULE"), Command("schedule")), StateFilter("*"))
async def schedule_start(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer("🕒 <b>Enter Fire Time (e.g., 04:08 PM):</b>", parse_mode=ParseMode.HTML)
    await state.set_state(ScheduleState.waiting_time)

@dp.message(ScheduleState.waiting_time)
async def sched_time(message: types.Message, state: FSMContext):
    try:
        t_str = message.text.upper().replace(".", "").strip()
        datetime.strptime(t_str, "%I:%M %p")
        await state.update_data(time=t_str)
        await message.answer(f"✅ <b>TIME SECURED:</b> <code>{t_str}</code>\n📅 <b>Enter Month (1-12):</b>", parse_mode=ParseMode.HTML)
        await state.set_state(ScheduleState.waiting_month)
    except:
        await message.answer("⚠️ <b>FORMAT ERROR:</b> Use HH:MM AM/PM (e.g., 10:55 PM)")

@dp.message(ScheduleState.waiting_month)
async def sched_month(message: types.Message, state: FSMContext):
    try:
        val = int(message.text)
        if not (1 <= val <= 12): raise ValueError
        await state.update_data(month=val)
        await message.answer("📅 <b>Enter Year (e.g., 2026):</b>", parse_mode=ParseMode.HTML)
        await state.set_state(ScheduleState.waiting_year)
    except:
        await message.answer("❌ <b>ERROR:</b> Enter a valid month number (1-12).")

@dp.message(ScheduleState.waiting_year)
async def sched_year(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ <b>ERROR:</b> Enter numerical year.")
        return
    await state.update_data(year=int(message.text), selected_days=[])
    await show_days_keyboard(message, [])
    await state.set_state(ScheduleState.selecting_days)

@dp.callback_query(F.data.startswith("toggle_"), ScheduleState.selecting_days)
async def toggle_day(cb: types.CallbackQuery, state: FSMContext):
    idx = int(cb.data.split("_")[1])
    data = await state.get_data()
    sel = data.get("selected_days", [])
    if idx in sel: sel.remove(idx)
    else: sel.append(idx)
    await state.update_data(selected_days=sel)
    await cb.message.edit_reply_markup(reply_markup=await get_days_kb(sel))

@dp.callback_query(F.data == "days_done", ScheduleState.selecting_days)
async def days_finished(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 LOCK PROTOCOL", callback_data="sched_lock")]])
    await cb.message.edit_text(
        f"📋 <b>SCHEDULE SUMMARY</b>\n"
        f"🕒 Time: <code>{data['time']}</code>\n"
        f"🗓 Days index: <code>{data['selected_days']}</code>", 
        reply_markup=kb, 
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data == "sched_lock")
async def sched_lock(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    
    # [!] CHANGE: Ensure this also uses sequential numbering
    m_code = await get_next_m_code()
    
    # ... rest of your cron/scheduling logic ...
    day_map = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    cron_days = ",".join([day_map[i] for i in data['selected_days']])
    
    dt_fire = datetime.strptime(data['time'], "%I:%M %p")
    now = datetime.now()
    fire_today = now.replace(hour=dt_fire.hour, minute=dt_fire.minute, second=0, microsecond=0)
    time_diff = (fire_today - now).total_seconds() / 60

    # 1. SETUP RECURRING JOBS
    review_hour = dt_fire.hour - 1 if dt_fire.hour > 0 else 23
    scheduler.add_job(trigger_review, CronTrigger(day_of_week=cron_days, hour=review_hour, minute=dt_fire.minute), args=[m_code, data['time']])
    scheduler.add_job(execute_guarded_fire, CronTrigger(day_of_week=cron_days, hour=dt_fire.hour, minute=dt_fire.minute), args=[m_code])

    # 2. HYBRID INTELLIGENT LOGIC
    today_short = now.strftime("%a").lower()
    if today_short in cron_days and 0 < time_diff <= 60:
        # PATH A: FIRE AT SCHEDULED TIME (NO PERMISSION NEEDED)
        # Store in PENDING with 'confirmed' already True
        content, topic = await generate_content()
        PENDING_APPROVALS[m_code] = {"content": content, "topic": topic, "confirmed": True, "target": data['time']}
        await cb.message.edit_text(f"⚡ <b>DIRECT FIRE ARMED:</b> Window under 60m. Bot will fire at <code>{data['time']}</code> automatically.", parse_mode=ParseMode.HTML)
    else:
        # PATH B: GUARDED (CONFIRMATION REQUIRED AT T-60)
        await cb.message.edit_text(f"💎 <b>PROTOCOL SECURED:</b> I will ask for confirmation 60m before <code>{data['time']}</code>.", parse_mode=ParseMode.HTML)
    
    await state.clear()

# ==========================================
# 🚀 BACKGROUND EXECUTION (INTELLIGENCE)
# ==========================================

async def trigger_review(m_code, target_time):
    """Fires 60m before target. Generates and asks for permission."""
    content, topic = await generate_content()
    PENDING_APPROVALS[m_code] = {"content": content, "topic": topic, "confirmed": False, "target": target_time}
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 CONFIRM FIRE", callback_data=f"arm_{m_code}")]])
    await bot.send_message(OWNER_ID, f"⏳ <b>REVIEW (T-60m): {m_code}</b>\nFire scheduled at: <code>{target_time}</code>\n────────────────────\n\n{content}", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("arm_"))
async def arm_post(cb: types.CallbackQuery):
    """Sets flag to True. Execution job will handle the rest at scheduled time."""
    m_code = cb.data.split("_")[1]
    if m_code in PENDING_APPROVALS:
        PENDING_APPROVALS[m_code]["confirmed"] = True
        await cb.message.edit_text(f"✅ <b>POST ARMED:</b> Intelligence locked for <code>{PENDING_APPROVALS[m_code]['target']}</code>.", parse_mode=ParseMode.HTML)

async def execute_guarded_fire(m_code):
    """The Precision Trigger."""
    if m_code in PENDING_APPROVALS:
        data = PENDING_APPROVALS[m_code]
        if data.get("confirmed"):
            vault_msg = await bot.send_message(CHANNEL_ID, data['content'], parse_mode=ParseMode.HTML, reply_markup=get_engagement_markup(m_code))
            col_vault.insert_one({
                "m_code": m_code,
                "msg_id": vault_msg.message_id,
                "topic": data['topic'],
                "content": data['content'],
                "reaction_lock": 0,
                "is_unlocked": False,
                "created_at": datetime.now(),
                "last_verified": datetime.now()
            })
            await bot.send_message(LOG_CHANNEL_ID, f"📢 <b>DEPLOYED:</b> <code>{m_code}</code> fired successfully.")
        else:
            await bot.send_message(OWNER_ID, f"❌ <b>ABORTED:</b> <code>{m_code}</code> fire time reached but no confirmation was received.")
        del PENDING_APPROVALS[m_code]
# ==========================================
# ✏️ REMOTE EDIT (REINFORCED PRIORITY)
# ==========================================

# Using or_f to catch "✏️ EDIT", "EDIT", or "/edit"
@dp.message(or_f(F.text.contains("EDIT"), Command("edit")), StateFilter("*"))
async def edit_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # Critical: Flush any stuck states from previous protocols
    await state.clear()
    
    await message.answer(
        "📝 <b>EDIT MODE ACTIVATED</b>\n"
        "Enter the <b>M-Code</b> of the post to modify (e.g., M1):", 
        parse_mode=ParseMode.HTML
    )
    await state.set_state(EditState.waiting_id)

@dp.message(EditState.waiting_id)
async def edit_id_received(message: types.Message, state: FSMContext):
    # Standardize input
    m_code = message.text.upper().strip()

    try:
        # Check database before proceeding
        entry = col_vault.find_one({"m_code": m_code})

        if entry:
            await state.update_data(edit_code=m_code, msg_id=entry.get("msg_id"))
            await message.answer(
                f"🔍 <b>ENTRY FOUND:</b> <code>{m_code}</code>\n"
                f"────────────────────\n"
                f"<b>CURRENT CONTENT:</b>\n"
                f"<code>{html.escape(entry.get('content', '')[:150])}...</code>\n"
                f"────────────────────\n"
                f"📥 <b>Enter the NEW content for this post:</b>",
                parse_mode=ParseMode.HTML
            )
            await state.set_state(EditState.waiting_text)
        else:
            await message.answer(f"❌ <b>ERROR:</b> M-Code <code>{m_code}</code> not found in Vault.")
            await state.clear()
    except Exception as e:
        await message.answer(f"❌ <b>DATABASE ERROR:</b> {html.escape(str(e))}")
        await state.clear()

@dp.message(EditState.waiting_text)
async def edit_exec(message: types.Message, state: FSMContext):
    data = await state.get_data()
    m_code = data['edit_code']
    msg_id = data['msg_id']
    new_content = message.text

    try:
        # 1. Update the physical message in the Telegram Channel
        # We preserve the original reaction lock buttons
        await bot.edit_message_text(
            text=new_content,
            chat_id=CHANNEL_ID,
            message_id=msg_id,
            parse_mode=ParseMode.HTML,
            reply_markup=get_engagement_markup(m_code)
        )

        # 2. Update MongoDB
        col_vault.update_one(
            {"m_code": m_code},
            {"$set": {"content": new_content}}
        )

        await message.answer(f"🚀 <b>SUCCESS:</b> Intelligence <code>{m_code}</code> updated in channel and database.")
        console_out(f"System Edit: {m_code} transmuted.")

    except Exception as e:
        # Error handling for messages older than 48 hours
        await message.answer(f"❌ <b>EDIT FAILED:</b> {html.escape(str(e))}")

    await state.clear()
# ==========================================
# [!] BROADCAST LOGIC (PRIORITY ANCHORED)
# ==========================================

@dp.message(or_f(F.text.contains("BROADCAST"), Command("broadcast")), StateFilter("*"))
async def broadcast_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # 1. Clear any stuck states
    await state.clear() 
    
    # 2. Set state immediately
    await state.set_state(BroadcastState.waiting_msg)
    
    await message.answer(
        "<b>[-] SYNDICATE BROADCAST</b>\n"
        "Enter your directive for the Family:", 
        parse_mode=ParseMode.HTML
    )

# CRITICAL: This handler MUST come before any general text handlers
# ==========================================
# 📢 SYNDICATE BROADCAST (TELEMETRY SYNCED)
# ==========================================

@dp.message(BroadcastState.waiting_msg)
async def broadcast_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    
    # 1. Check for Cancellation
    if message.text in ["🛑 PANIC", "/cancel"]:
        await state.clear()
        await message.answer("<b>[!] BROADCAST ABORTED.</b>", parse_mode=ParseMode.HTML)
        return

    # 2. Construct Technical Template
    # We wrap your input in the Syndicate styling
    formatted_payload = (
        "<b>◈ MSANODE SYNDICATE ◈</b>\n"
        "────────────────────\n\n"
        f"{html.escape(message.text)}\n\n"
        "────────────────────\n"
        "<b>DIRECTIVE FROM MASTER SADIQ</b>\n"
        "<i>\"Family: Execute with precision. Action is our only currency.\"</i>\n"
        "────────────────────"
    )
    
    # 3. Public Deployment
    try:
        # Use our safe_send_message protocol
        sent_msg = await safe_send_message(CHANNEL_ID, formatted_payload)
        
        if sent_msg:
            # Attempt to Pin the Directive
            try:
                await bot.pin_chat_message(CHANNEL_ID, sent_msg.message_id)
                pin_status = "SENT AND PINNED"
            except:
                pin_status = "SENT (PIN FAILED)"
            
            # 4. MIRROR TO PRIVATE LOG CHANNEL (Fixed & Unified)
            # This ensures your command center tracks the global broadcast
            await bot.send_message(
                LOG_CHANNEL_ID, 
                f"📢 <b>GLOBAL BROADCAST MIRROR</b>\n"
                f"Status: {pin_status}\n"
                f"────────────────────\n"
                f"{formatted_payload}",
                parse_mode=ParseMode.HTML
            )
            
            await message.answer(f"<b>[+] DIRECTIVE {pin_status}.</b>", parse_mode=ParseMode.HTML)
            console_out(f"Global Broadcast: {pin_status}")
            
        else:
            await message.answer("<b>[!] ERROR:</b> Public deployment failed.")
            
    except Exception as e:
        await message.answer(f"<b>[!] CRITICAL:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)
    
    # Finalize and return to Standby
    await state.clear()

# ==========================================
# [!] UNSEND PROTOCOL (SCRUB DELETION)
# ==========================================
@dp.message(F.text == "UNSEND", StateFilter("*"))
@dp.message(Command("unsend"), StateFilter("*"))
async def unsend_init(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    await state.clear()
    await message.answer("[-] UNSEND INITIATED\nEnter the M-Code to scrub from existence:")
    await state.set_state(UnsendState.waiting_id)

@dp.message(UnsendState.waiting_id)
async def unsend_exec(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return
    m_code = message.text.upper()

    try:
        entry = col_vault.find_one({"m_code": m_code})

        if entry:
            try:
                await bot.delete_message(CHANNEL_ID, entry.get("msg_id"))
                t_status = "Scrubbed from Channel"
            except Exception:
                t_status = "Telegram scrub failed (Message may be too old)"

            col_vault.delete_one({"m_code": m_code})

            await message.answer(f"<b>[+] SCRUB COMPLETE</b>\nID: <code>{m_code}</code>\nStatus: {t_status} and Database.", parse_mode=ParseMode.HTML)
        else:
            await message.answer(f"<b>[!] NOT FOUND:</b> <code>{m_code}</code> is not in the system.", parse_mode=ParseMode.HTML)

    except Exception as e:
        await message.answer(f"<b>[!] SCRUB ERROR:</b> {html.escape(str(e))}", parse_mode=ParseMode.HTML)

    await state.clear()

# ==========================================
# [!] ALCHEMY (UNTOUCHED foundation)
@dp.message(F.text & F.forward_from_chat)
async def alchemy_engine(message: types.Message):
    if message.from_user.id != OWNER_ID: return
    await message.answer("[-] Alchemy: Transmuting intelligence...")
    content = await alchemy_transform(message.text)
    # Transmuted content delivered via HTML stability
    await bot.send_message(OWNER_ID, f"<b>[+] TRANSFORMED:</b>\n\n{content}", parse_mode=ParseMode.HTML)
# ==========================================
# 🚨 UNBLOCKABLE EMERGENCY OVERRIDE
# ==========================================
@dp.message(StateFilter("*"), lambda m: m.text and "PANIC" in m.text.upper())
@dp.message(Command("cancel"), StateFilter("*"))
async def global_panic_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != OWNER_ID: return

    # 1. Force clear all stuck AI processes or states
    await state.clear()
    
    # 2. Re-create the menu manually (No function needed)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📄 Generate PDF"), KeyboardButton(text="🔗 Get Link")],
            [KeyboardButton(text="📋 Show Library"), KeyboardButton(text="📊 Storage Info")],
            [KeyboardButton(text="🗑 Remove PDF"), KeyboardButton(text="💎 Elite Help")]
        ],
        resize_keyboard=True
    )
    
    await message.answer(
        "🚨 <b>SYSTEM-WIDE RESET SUCCESSFUL</b>\n"
        "────────────────────\n"
        "• State Memory: <b>PURGED</b>\n"
        "• AI Logic: <b>STANDBY</b>\n"
        "────────────────────\n"
        "Infrastructure restored to Home Protocol.",
        reply_markup=kb,
        parse_mode="HTML"
    )
    
    print(f"◈ ALERT: Panic Reset executed via sh.py")
# ==========================================
# [!] MAIN LOOP
# ==========================================
async def main():
    # MongoDB connection is already established in connect_db()

    # Scheduler Synchronization
    scheduler.add_job(hourly_heartbeat, 'interval', hours=1)
    scheduler.add_job(self_healing_audit, 'cron', hour=0, minute=0)
    scheduler.start()

    print("◈ SINGULARITY APEX ONLINE")

    try:
        await bot.send_message(OWNER_ID, "<b>[+] Singularity Online. Persistent & Failover Active.</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"Startup notification failed: {e}")

    # Polling Loop with Fail-Safe Sleep
    while True:
        try:
            await dp.start_polling(bot)
        except Exception as e:
            print(f"Polling error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":

    asyncio.run(main())
