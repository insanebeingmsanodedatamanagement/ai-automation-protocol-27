import subprocess
import os
import sys
import asyncio
from aiohttp import web

# ==========================================
# 🔧 PICKLE RECONSTRUCTION FOR RENDER
# ==========================================
def rebuild_pickle():
    """Reconstruct token.pickle from base64 for Google API authentication"""
    source = "token.pickle.base64"
    target = "token.pickle"

    if os.path.exists(source) and not os.path.exists(target):
        try:
            import base64
            with open(source, "r") as f:
                base64_data = f.read().strip()  # Remove any whitespace
                binary_data = base64.b64decode(base64_data)

            with open(target, "wb") as f:
                f.write(binary_data)
            print("✅ token.pickle successfully reconstructed from Base64")
        except Exception as e:
            print(f"❌ Failed to reconstruct token.pickle: {e}")
    elif os.path.exists(target):
        print("✅ token.pickle already exists")
    else:
        print("⚠️  token.pickle.base64 not found - Google API may not work")

# Reconstruct pickle before starting anything
rebuild_pickle()

# --- 1. RENDER HEALTH CHECK ---
async def handle(request):
    return web.Response(text="MSANODE SINGULARITY: ALL CORES ACTIVE")

async def start_server():
    # Dynamically find the port for Render
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
    print("🚀 MSANODE: Launching all 5 Cores...")

    for file in bot_files:
        if os.path.exists(file):
            # This starts each bot in its own background process
            subprocess.Popen([sys.executable, file], shell=True, creationflags=subprocess.CREATE_NEW_CONSOLE)
            print(f"✅ Started: {file}")
            await asyncio.sleep(1) # Small delay to prevent CPU overload
        else:
            print(f"❌ File Missing: {file}")

async def main():
    await start_server()
    await run_bots()
    # Keeps the script alive forever
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("◈ Singularity Offline.")
