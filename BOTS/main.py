import subprocess
import os
import sys
import asyncio
import time
from aiohttp import web

# ==========================================
# 🛡️ THE UNIFIED HEALTH SHIELD
# ==========================================
async def handle_singularity(request):
    return web.Response(text="💎 MSANODE SINGULARITY: 5 CORES ACTIVE & PROTECTED")

async def start_health_server():
    app = web.Application()
    app.router.add_get('/', handle_singularity)
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"📡 Singularity Port {port} LOCKED. Shielding 750 Free Hours.")

# ==========================================
# 🚀 THE STAGGERED DEPLOYMENT
# ==========================================
async def launch_empire():
    # List of all your verified core files
    cores = ["bot1.py", "bot2.py", "bot3.py", "bot4.py", "bot5.py"]
    processes = []

    print("\n⚡ [PHASE 1] INITIALIZING EMPIRE SINGULARITY...")
    
    for file in cores:
        if os.path.exists(file):
            print(f"⌛ Activating Core: {file}...")
            
            # Start the bot as a background process
            # -u ensures logs show up in Render immediately
            p = subprocess.Popen([sys.executable, "-u", file])
            processes.append(p)
            
            print(f"✅ {file} is now LIVE in background.")
            
            # --- CRITICAL: THE STAGGER GAP ---
            # We wait 20 seconds between bots so the 1-CPU core doesn't explode
            # and to prevent "Conflict 409" (Telegram login overlaps).
            await asyncio.sleep(20) 
        else:
            print(f"❌ CRITICAL ERROR: File {file} not found!")

    print("\n💎 [PHASE 2] ALL NODES SYNCHRONIZED. GOD MODE ACTIVE.\n")
    return processes

# ==========================================
# 🔄 MAIN LOOP
# ==========================================
async def main():
    # 1. Open the Port Binder (Satisfies Render's health check)
    await start_health_server()
    
    # 2. Launch the 5 bots
    procs = await launch_empire()
    
    # 3. Monitor and Keep Alive
    while True:
        # Check every hour if bots are still alive
        for i, p in enumerate(procs):
            if p.poll() is not None:
                print(f"⚠️ Warning: Core {i+1} stopped. Restarting Singularity is recommended.")
        
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Singularity Shutting Down.")
    except Exception as e:
        print(f"💥 Singularity Crash: {e}")
