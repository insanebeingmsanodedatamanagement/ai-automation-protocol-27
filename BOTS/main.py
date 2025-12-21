import subprocess
import os
import sys
import asyncio
import shutil
import base64
from aiohttp import web

def log(message):
    print(message, flush=True)

# ==========================================
# 🔍 SEARCH & RESCUE PREPARATION
# ==========================================
def prepare_environment():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log(f"--- 🔍 GLOBAL SECRET SEARCH ---")
    
    targets = {
        "token.pickle.base64": "token.pickle",
        "credentials.json": "credentials.json",
        "service_account.json": "service_account.json",
        "vault_final.json": "vault_final.json"
    }

    # Potential Secret Locations on Render
    search_paths = [
        os.path.dirname(current_dir), # Parent (Root)
        "/etc/secrets",               # Default Render Secret Path
        current_dir                   # Current folder
    ]

    for secret_src, bot_target in targets.items():
        found = False
        target_path = os.path.join(current_dir, bot_target)
        
        for path in search_paths:
            full_src_path = os.path.join(path, secret_src)
            if os.path.exists(full_src_path):
                try:
                    if ".base64" in secret_src:
                        with open(full_src_path, "r") as f:
                            binary = base64.b64decode(f.read().strip())
                        with open(target_path, "wb") as f:
                            f.write(binary)
                    else:
                        shutil.copy(full_src_path, target_path)
                    log(f"✅ FOUND & INJECTED: {secret_src} from {path}")
                    found = True
                    break
                except Exception as e:
                    log(f"❌ ERROR processing {secret_src}: {e}")
        
        if not found:
            log(f"🚫 NOT FOUND: {secret_src} (Checked root, /etc/secrets, and BOTS)")

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
        if os.path.exists(os.path.join(current_dir, file)):
            subprocess.Popen(
                [sys.executable, "-u", file],
                cwd=current_dir,
                stdout=sys.stdout,
                stderr=sys.stderr
            )
            log(f"✅ Process started: {file}")
            await asyncio.sleep(4) 
        else:
            log(f"❌ Missing Core: {file}")

async def main():
    await start_server()
    await run_bots()
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    try: asyncio.run(main())
    except Exception as e: log(f"💥 CRASH: {e}")
