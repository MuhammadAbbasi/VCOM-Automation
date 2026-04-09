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
from fastapi import FastAPI
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

STATIC_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Mazara SCADA Monitor", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/api/status")
async def status():
    today = datetime.now().strftime("%Y-%m-%d")
    json_path = DATA_DIR / f"dashboard_data_{today}.json"

    if not json_path.exists():
        return JSONResponse({})

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return JSONResponse({})

    if not data:
        return JSONResponse({})

    # Return the most recent snapshot
    latest_key = sorted(data.keys())[-1]
    return JSONResponse(data[latest_key])


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
