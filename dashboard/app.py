"""
dashboard/app.py — FastAPI dashboard server for Mazara SCADA monitoring.

Routes:
  GET /           → serves static/index.html
  GET /api/status → returns the latest snapshot from dashboard_data_{today}.json

Run with:
    python dashboard/app.py
    (or via uvicorn: uvicorn dashboard.app:app --host localhost --port 8080)
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# Add parent to sys.path so we can import analyze_site
DASHBOARD_DIR = Path(__file__).resolve().parent
ROOT = DASHBOARD_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import uvicorn
import socket
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Import analysis logic
from processor_watchdog_final import analyze_site

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DASHBOARD_DIR = Path(__file__).resolve().parent
STATIC_DIR = DASHBOARD_DIR / "static"
ROOT = DASHBOARD_DIR.parent
DATA_DIR = ROOT / "extracted_data"
USER_SETTINGS_PATH = ROOT / "user_settings.json"

STATIC_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Mazara SCADA Monitor", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# WebSocket Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

# Background Task for Data Push
async def data_broadcaster():
    last_mtime = 0
    while True:
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            json_path = DATA_DIR / f"dashboard_data_{today}.json"
            busy_path = ROOT / ".extraction_busy"
            if not hasattr(manager, "_logged_path"):
                print(f"[DASHBOARD] Monitoring busy flag at: {busy_path.absolute()}")
                manager._logged_path = True
            
            is_extracting = busy_path.exists()
            
            # Broadcast extraction status every tick regardless of data change
            await manager.broadcast({
                "type": "extraction_status", 
                "is_extracting": is_extracting
            })

            if json_path.exists():
                mtime = json_path.stat().st_mtime
                if mtime > last_mtime:
                    last_mtime = mtime
                    with open(json_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if data:
                        latest_key = sorted(data.keys())[-1]
                        latest_data = data[latest_key]
                        await manager.broadcast({"type": "data_update", "data": latest_data})
        except Exception:
            pass
        await asyncio.sleep(2)

@app.post("/api/extraction/trigger")
async def trigger_extraction():
    print("[DASHBOARD] Manual extraction trigger received!", flush=True)
    trigger_path = ROOT / ".trigger_extraction"
    busy_path = ROOT / ".extraction_busy"
    
    if busy_path.exists():
        return JSONResponse({"status": "error", "message": "Extraction already in progress."}, status_code=400)
    
    trigger_path.touch()
    return JSONResponse({"status": "success", "message": "Extraction triggered."})

@app.get("/api/extraction/status")
async def get_extraction_status():
    busy_path = ROOT / ".extraction_busy"
    return JSONResponse({"is_extracting": busy_path.exists()})

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(data_broadcaster())


@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial data immediately
        today = datetime.now().strftime("%Y-%m-%d")
        json_path = DATA_DIR / f"dashboard_data_{today}.json"
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data:
                latest_key = sorted(data.keys())[-1]
                await websocket.send_json({"type": "data_update", "data": data[latest_key]})
                
        # Send initial settings
        from processor_watchdog_final import load_user_settings
        settings = load_user_settings()
        await websocket.send_json({"type": "config_update", "data": settings})
        
        while True:
            # wait for messages from client (if needed)
            message = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/api/settings")
async def get_settings():
    from processor_watchdog_final import load_user_settings
    return JSONResponse(load_user_settings())


@app.post("/api/settings")
async def update_settings(request: Request, background_tasks: BackgroundTasks):
    try:
        new_settings = await request.json()
        
        # Define the background work
        async def apply_changes(settings: dict):
            try:
                # 1. Save to disk
                with open(USER_SETTINGS_PATH, "w", encoding="utf-8") as f:
                    json.dump(settings, f, indent=4)
                
                # 2. Broadcast to clients
                await manager.broadcast({"type": "config_update", "data": settings})
                
                # 3. Rescan
                today = datetime.now().strftime("%Y-%m-%d")
                analyze_site(today)
            except Exception as e:
                print(f"[!] Background settings error: {e}")

        # Queue the work and return immediately
        background_tasks.add_task(apply_changes, new_settings)
        return JSONResponse({"status": "success"})
        
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/forensic/rescan")
async def rescan():
    """Delete current today's JSON, clear error folders, and re-trigger analyze_site."""
    today = datetime.now().strftime("%Y-%m-%d")
    json_path = DATA_DIR / f"dashboard_data_{today}.json"
    root_errors = ROOT / "errors"
    vcom_screenshots = ROOT / "VCOM_Screenshots"

    try:
        # 1. Delete JSON if exists to force a fresh start
        if json_path.exists():
            json_path.unlink()
        
        # 2. Clear error screenshots
        for folder in [root_errors, vcom_screenshots]:
            if folder.exists():
                for f in folder.glob("*.png"):
                    try:
                        f.unlink()
                    except Exception:
                        pass

        # 3. Run analysis
        analyze_site(today)
        
        return JSONResponse({"status": "success", "message": f"Rescan completed for {today}. Errors cleared."})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/telegram/test")
async def test_telegram():
    """Send a test message with the detailed system upgrade summary."""
    from processor_watchdog_final import load_user_settings, send_telegram_notification
    try:
        settings = load_user_settings()
        msg = (
            "⚙️ *System Upgrade Applied*\n"
            "- Switched to ultra-fast CSV data ingestion (Excel dependency removed)\n"
            "- Fixed 9510m duration bug via clock-time analysis\n"
            "- Improved data deduplication logic\n"
            "- Optimized network share I/O performance"
        )
        send_telegram_notification(msg, settings)
        return JSONResponse({"status": "success", "message": "Test message sent to Telegram."})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# LLM Chat Endpoint
# ---------------------------------------------------------------------------
try:
    from llm_agent import ask_llm
except ImportError:
    ask_llm = None

@app.post("/api/chat")
async def chat_endpoint(request: Request):
    if not ask_llm:
        return JSONResponse({"status": "error", "message": "llm_agent module not found."}, status_code=500)
    
    try:
        body = await request.json()
        question = body.get("question")
        if not question:
            return JSONResponse({"status": "error", "message": "No question provided."}, status_code=400)
        
        # Load the latest state to pass as context
        today = datetime.now().strftime("%Y-%m-%d")
        json_path = DATA_DIR / f"dashboard_data_{today}.json"
        latest_data = None
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data:
                    latest_key = sorted(data.keys())[-1]
                    latest_data = data[latest_key]
        
        # Call the LLM
        answer = ask_llm(question, latest_data)
        return JSONResponse({"status": "success", "answer": answer})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_local_ip():
    """Try to get the primary LAN IP of this machine."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def setup_ngrok(token: str, port: int, user: str, psw: str):
    """Start an ngrok tunnel if a token is provided."""
    try:
        from pyngrok import ngrok, conf
        import subprocess, platform
        
        # Kill any lingering ngrok processes to prevent ERR_NGROK_334
        try:
            if platform.system() == "Windows":
                subprocess.run(["taskkill", "/F", "/IM", "ngrok.exe"], capture_output=True)
            else:
                subprocess.run(["pkill", "-9", "ngrok"], capture_output=True)
        except Exception:
            pass
            
        conf.get_default().auth_token = token
        
        # Configure the tunnel with Basic Auth for security
        auth = f"{user}:{psw}" if user and psw else None
        public_url = ngrok.connect(port, auth=auth).public_url
        return public_url
    except ImportError:
        return None
    except Exception as e:
        print(f"[!] Ngrok Error: {e}")
        return None

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from processor_watchdog_final import load_config
    cfg = load_config()
    
    port = 8080
    local_ip = get_local_ip()
    
    print("\n" + "="*60)
    print("    MAZARA DASHBOARD STARTING")
    print("="*60)
    print(f"[*] Local:   http://localhost:{port}")
    print(f"[*] Network: http://{local_ip}:{port}")
    
    # Try to start Ngrok
    ngrok_token = cfg.get("NGROK_AUTH_TOKEN")
    if ngrok_token and ngrok_token != "YOUR_TOKEN_HERE":
        print("[*] Starting Ngrok Tunnel...")
        ng_user = cfg.get("DASHBOARD_USER", "admin")
        ng_pass = cfg.get("DASHBOARD_PASS", "mazara2025")
        
        public_url = setup_ngrok(ngrok_token, port, ng_user, ng_pass)
        if public_url:
            print(f"[*] Remote Access (Public): {public_url}")
            print(f"[*] Security Policy: Basic Auth ({ng_user}:{ng_pass})")
        else:
            print("[!] Ngrok failed: Is 'pyngrok' installed? Run: pip install pyngrok")
    else:
        print("[!] No NGROK_AUTH_TOKEN found in config.json. Remote access via Ngrok is disabled.")
    
    print("="*60 + "\n")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="warning",
    )
