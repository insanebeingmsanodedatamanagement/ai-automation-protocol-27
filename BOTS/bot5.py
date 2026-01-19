#!/usr/bin/env python3
# Minimal test to verify Render can run Python
import sys
print("=" * 50)
print("MINIMAL TEST - PYTHON IS RUNNING")
print(f"Python version: {sys.version}")
print("=" * 50)

# Start a minimal web server
from aiohttp import web
import os

async def health(request):
    return web.Response(text="TEST SERVER ACTIVE")

app = web.Application()
app.router.add_get('/', health)

port = int(os.environ.get("PORT", 10000))
print(f"Starting test server on port {port}")
web.run_app(app, host='0.0.0.0', port=port)
