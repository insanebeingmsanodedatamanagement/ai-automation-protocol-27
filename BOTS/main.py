import subprocess
import os
import sys
import asyncio
import shutil
import base64
from aiohttp import web

# Force printing to show up in Render logs immediately
def log(message):
    print(message, flush=True)

# ==========================================
# 🔧 HARDENED ENVIRONMENT PREPARATION
# ==========================================
def prepare_environment():
    """Locate secrets in Render root and move them into the BOTS execution folder"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Render's secret files are usually in the parent of the BOTS folder
    parent_dir = os.path.dirname(current_dir)

    log(f"--- 🛠️ STARTING HARDENED PREPARATION ---")
    log(f"Current Path: {current_dir}")
    log(f"Searching for secrets in: {parent_dir}")

    # 1. Handle token.pickle.base64
    pickle_source = os.path.join(parent_dir, "token.pickle.base64")
    pickle_target = os.path.join(current_dir, "token.pickle")

    if os.path.exists(pickle_source):
        try:
            with open(pickle_source, "r") as f:
                base64_data = f.read().strip()
                binary_data = base64.b64decode(base64_data)
            with open(pickle_target, "wb") as f:
                f.write(binary_data)
            log("✅ SUCCESS: token.pickle reconstructed")
        except Exception as e:
            log(f"❌ ERROR: Pickle reconstruction failed: {e}")
    else:
        log(f"⚠️ CRITICAL: token.pickle.base64 NOT FOUND at {pickle_source}")

    # 2. Handle JSON Secrets
    secrets = ["credentials.json", "service_account.json", "vault_final.json"]
    for secret in secrets:
        source_json = os.path.join(parent_dir, secret)
        target_json = os.path.join(current_dir, secret)
        
        if os.path.exists(source_json):
            try:
                shutil.copy(source_json, target_json)
                log(f"✅ SUCCESS: Injected {secret}")
            except Exception as e:
                log(f"❌ ERROR: Failed to copy {secret}: {e}")
        else:
            log(f"⚠️ MISSING: {secret} not found in parent directory")

# Run preparation before starting anything
prepare_environment()

# --- 1. RENDER HEALTH CHECK ---
async def handle(request):
    return web.Response(text="MSANODE SINGULARITY: ALL CORES ACTIVE")

async def start_server():
    port = int(os.environ.get("PORT", 10000))
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    log(f"📡 Master Port {port} Online")

# --- 2. THE MULTI-BOT ENGINE ---
async def run_bots():
    bot_files = ["bot1.py", "bot2.py", "bot3.py", "bot4.py", "bot5.py"]
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    log("🚀 MSANODE: Launching all 5 Cores...")

    for file in bot_files:
        file_path = os.path.join(current_dir, file)
        if os.path.exists(file_path):
            log(f"🔄 Attempting to start {file}...")
            # Use -u for unbuffered logs and pass current stdout/stderr to Render
            subprocess.Popen(
                [sys.executable, "-u", file],
                cwd=current_dir,
                stdout=sys.stdout,
                stderr=sys.stderr
            )
            log(f"✅ Process started for {file}")
            await asyncio.sleep(5)
        else:
            log(f"❌ CRITICAL: {file} not found at {file_path}")

async def main():
    await start_server()
    await run_bots()
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        log(f"💥 GLOBAL CRASH: {e}")
