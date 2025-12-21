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
# 🔧 ENVIRONMENT PREPARATION
# ==========================================
def prepare_environment():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # If Root Directory is 'BOTS', then secrets are in '..'
    # If Root Directory is blank, secrets are also in '..' relative to this file
    root_dir = os.path.dirname(current_dir)

    log(f"--- 🛠️  STARTING PREPARATION ---")
    log(f"Current Path: {current_dir}")
    log(f"Root Path: {root_dir}")

    # 1. Rebuild token.pickle
    source_pickle = os.path.join(root_dir, "token.pickle.base64")
    target_pickle = os.path.join(current_dir, "token.pickle")

    if os.path.exists(source_pickle):
        try:
            with open(source_pickle, "r") as f:
                base64_data = f.read().strip()
                binary_data = base64.b64decode(base64_data)
            with open(target_pickle, "wb") as f:
                f.write(binary_data)
            log("✅ SUCCESS: token.pickle reconstructed")
        except Exception as e:
            log(f"❌ ERROR: Pickle reconstruction failed: {e}")
    else:
        log(f"⚠️  CRITICAL: token.pickle.base64 NOT FOUND at {source_pickle}")

    # 2. Inject JSON secrets
    secrets = ["credentials.json", "service_account.json", "vault_final.json"]
    for secret in secrets:
        source_json = os.path.join(root_dir, secret)
        target_json = os.path.join(current_dir, secret)
        
        if os.path.exists(source_json):
            shutil.copy(source_json, target_json)
            log(f"✅ SUCCESS: Injected {secret}")
        else:
            log(f"⚠️  MISSING: {secret} not found in root")

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
            # We use 'bufsize=0' and connect pipes to see bot logs in real-time
            subprocess.Popen(
                [sys.executable, "-u", file], # -u forces unbuffered output in the bot itself
                cwd=current_dir,
                stdout=sys.stdout,
                stderr=sys.stderr
            )
            log(f"✅ Process started for {file}")
            await asyncio.sleep(5) # Give it 5 seconds to show any initial errors
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
