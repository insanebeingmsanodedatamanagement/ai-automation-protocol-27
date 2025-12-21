import subprocess
import os
import sys
import asyncio
import shutil
import base64
from aiohttp import web

# ==========================================
# 🔧 ENVIRONMENT PREPARATION (ABSOLUTE PATHS)
# ==========================================
def prepare_environment():
    """Ensure all secrets move from Root to the BOTS directory for execution"""
    # Absolute path of this script (main.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Render puts Secret Files in the Root (one level up from BOTS folder)
    root_dir = os.path.dirname(current_dir)

    print(f"📂 Execution Path: {current_dir}")
    print(f"📂 Secret Source: {root_dir}")

    # 1. Rebuild token.pickle from Base64 string
    source_pickle = os.path.join(root_dir, "token.pickle.base64")
    target_pickle = os.path.join(current_dir, "token.pickle")

    if os.path.exists(source_pickle):
        try:
            with open(source_pickle, "r") as f:
                base64_data = f.read().strip()
                binary_data = base64.b64decode(base64_data)
            with open(target_pickle, "wb") as f:
                f.write(binary_data)
            print("✅ token.pickle successfully reconstructed from Base64")
        except Exception as e:
            print(f"❌ Failed to reconstruct pickle: {e}")
    else:
        print(f"⚠️ token.pickle.base64 NOT found at {source_pickle}")

    # 2. Inject JSON secrets
    secrets = ["credentials.json", "service_account.json", "vault_final.json"]
    for secret in secrets:
        source_json = os.path.join(root_dir, secret)
        target_json = os.path.join(current_dir, secret)
        
        if os.path.exists(source_json):
            try:
                shutil.copy(source_json, target_json)
                print(f"✅ Injected {secret} into BOTS environment")
            except Exception as e:
                print(f"❌ Failed to copy {secret}: {e}")
        else:
            print(f"⚠️ Secret missing from Root: {secret}")

# Run preparation before starting the server
prepare_environment()

# --- 1. RENDER HEALTH CHECK ---
async def handle(request):
    return web.Response(text="MSANODE SINGULARITY: ALL CORES ACTIVE")

async def start_server():
    # Render expects the app to listen on the PORT variable
    port = int(os.environ.get("PORT", 10000))
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"📡 Master Port {port} Online")

# --- 2. THE MULTI-BOT ENGINE ---
async def run_bots():
    bot_files = ["bot1.py", "bot2.py", "bot3.py", "bot4.py", "bot5.py"]
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    print(f"🚀 MSANODE: Launching all 5 Cores...")

    for file in bot_files:
        file_path = os.path.join(current_dir, file)
        if os.path.exists(file_path):
            # stdout/stderr redirection is the ONLY way to see bot errors in Render logs
            # Removed creationflags (Windows only) for Linux compatibility
            subprocess.Popen(
                [sys.executable, file],
                cwd=current_dir,
                stdout=sys.stdout,
                stderr=sys.stderr
            )
            print(f"✅ Executing: {file}")
            await asyncio.sleep(3) # Delay to prevent CPU spikes
        else:
            print(f"❌ Core File Missing: {file_path}")

async def main():
    await start_server()
    await run_bots()
    # Continuous keep-alive loop
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("◈ Singularity Offline.")
