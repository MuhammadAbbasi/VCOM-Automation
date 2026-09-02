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
import os
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
import logging
import secrets
import base64



# Suppress asyncio's noisy log for WebSocket keepalive ping timeouts.
# These fire when a browser tab goes idle — harmless, already handled in broadcast().
logging.getLogger("asyncio").setLevel(logging.CRITICAL)
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, Request, Depends, HTTPException, status
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

# Import analysis logic
from processor_watchdog_final import analyze_site

# Import plant map routes
try:
    from dashboard.plant_map_routes import router as plant_map_router
except ImportError:
    plant_map_router = None

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
from processor_watchdog_final import load_config

# Load System ID from config
SYSTEM_ID = "XXXXXXX"
try:
    cfg = load_config()
    system_url = cfg.get("SYSTEM_URL", "")
    import re
    m = re.search(r'systemId/(\d+)', system_url)
    if m:
        SYSTEM_ID = m.group(1)
except Exception:
    pass


def is_authenticated(request: Request) -> bool:
    """Check if request is authenticated via session cookie or Basic Auth."""
    session = request.cookies.get("get_session")
    if session == "authenticated":
        return True

    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Basic "):
        try:
            encoded_credentials = auth_header.split(" ", 1)[1]
            decoded = base64.b64decode(encoded_credentials).decode("utf-8")
            username, password = decoded.split(":", 1)
            cfg = load_config()
            expected_user = cfg.get("DASHBOARD_USER") or os.environ.get("DASHBOARD_USER", "admin")
            expected_pass = cfg.get("DASHBOARD_PASS") or os.environ.get("DASHBOARD_PASS", "")
            if secrets.compare_digest(username, expected_user) and secrets.compare_digest(password, expected_pass):
                return True
        except Exception:
            pass
    return False


def require_auth(request: Request) -> None:
    """FastAPI dependency: 401s unless is_authenticated() passes (session
    cookie OR Basic Auth) — unlike the old Basic-Auth-only verify_credentials,
    this matches what the /login cookie flow actually sets."""
    if not is_authenticated(request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

async def delayed_broadcaster_start():
    await asyncio.sleep(2)
    asyncio.create_task(data_broadcaster())

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(delayed_broadcaster_start())
    yield

from fastapi.middleware.gzip import GZipMiddleware

app = FastAPI(title="Mazara SCADA Monitor", docs_url=None, redoc_url=None, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include plant map routes
if plant_map_router:
    app.include_router(plant_map_router)

@app.middleware("http")
async def add_security_headers(request, call_next):
    # Protect sensitive static assets
    path = request.url.path.lower()
    if path.startswith("/static/"):
        public_assets = {
            "/static/login.html",
            "/static/get_landing.html",
            "/static/lucide.min.js",
            "/static/style.css"
        }
        if path not in public_assets:
            if not is_authenticated(request):
                return RedirectResponse(url="/login", status_code=307)

    response = await call_next(request)
    response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self' 'unsafe-inline' cdn.jsdelivr.net; style-src 'self' 'unsafe-inline' fonts.googleapis.com cdn.jsdelivr.net; font-src 'self' fonts.gstatic.com; img-src 'self' data:; connect-src 'self' ws: wss:;"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response

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
        dead = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)
        for conn in dead:
            self.disconnect(conn)

manager = ConnectionManager()

def fetch_broadcaster_data(today, link_status_path):
    link_info = {"status": "offline", "last_heartbeat": None}
    if link_status_path.exists():
        try:
            with open(link_status_path, "r") as f:
                link_info = json.load(f)
            last_heartbeat = link_info.get("last_heartbeat")
            if last_heartbeat:
                last_ts = datetime.fromisoformat(last_heartbeat)
                # Treat tracker MQTT link as connected only if the last heartbeat is within 1 hour.
                if (datetime.now() - last_ts).total_seconds() > 3600:
                    link_info["status"] = "stale"
        except Exception:
            pass

    latest_data = None
    trackers = None
    try:
        from db.db_manager import load_latest_snapshot, get_daily_sensor_history, get_all_tracker_status
        latest_data = load_latest_snapshot(today)
        if latest_data:
            latest_data["sensor_history"] = get_daily_sensor_history(today)
            latest_data["link_status"] = link_info
            trackers = get_all_tracker_status()
    except Exception as e:
        print(f"[DASHBOARD] Broadcast load error: {e}")
    return latest_data, trackers

# Background Task for Data Push
async def data_broadcaster():
    prev_is_extracting = False
    await asyncio.sleep(2)
    while True:
        try:
            if not manager.active_connections:
                await asyncio.sleep(3)
                continue

            today = datetime.now().strftime("%Y-%m-%d")
            busy_path = ROOT / ".extraction_busy"
            if not hasattr(manager, "_logged_path"):
                print(f"[DASHBOARD] Monitoring busy flag at: {busy_path.absolute()}")
                manager._logged_path = True

            is_extracting = busy_path.exists()

            # Detect extraction cycle completion and trigger a page reload
            if prev_is_extracting and not is_extracting:
                print("[DASHBOARD] Extraction cycle completed — broadcasting page reload.")
                await manager.broadcast({"type": "page_reload"})

            prev_is_extracting = is_extracting

            from db.vcom_status_helpers import get_vcom_status
            v_status = get_vcom_status()

            await manager.broadcast({
                "type": "extraction_status",
                "is_extracting": is_extracting,
                "vcom_status": v_status
            })

            link_status_path = ROOT / "db" / "link_status.json"
            latest_data, trackers = await asyncio.to_thread(fetch_broadcaster_data, today, link_status_path)

            if latest_data:
                latest_data["system_id"] = SYSTEM_ID
                await manager.broadcast({
                    "type": "data_update",
                    "data": latest_data,
                    "trackers": trackers,
                    "timestamp": datetime.now().isoformat()
                })
        except Exception as e:
            print(f"[DASHBOARD] Broadcast error: {e}")
        await asyncio.sleep(5)

@app.post("/api/extraction/trigger")
async def trigger_extraction(_: None = Depends(require_auth)):
    print("[DASHBOARD] Manual extraction trigger received!", flush=True)
    trigger_path = ROOT / ".trigger_extraction"
    busy_path = ROOT / ".extraction_busy"
    
    if busy_path.exists():
        return JSONResponse({"status": "error", "message": "Extraction already in progress."}, status_code=400)
    
    trigger_path.touch()
    return JSONResponse({"status": "success", "message": "Extraction triggered."})

@app.get("/api/extraction/status")
async def get_extraction_status(_: None = Depends(require_auth)):
    busy_path = ROOT / ".extraction_busy"
    from db.vcom_status_helpers import get_vcom_status
    return JSONResponse({
        "is_extracting": busy_path.exists(),
        "vcom_status": get_vcom_status()
    })

@app.get("/api/vcom/status")
async def api_vcom_status(_: None = Depends(require_auth)):
    from db.vcom_status_helpers import get_vcom_status
    return JSONResponse(get_vcom_status())

@app.get("/api/trackers/history")
async def api_tracker_history(request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        from db.db_manager import get_tracker_history_compact
        data = await asyncio.to_thread(get_tracker_history_compact)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# ---------------------------------------------------------------------------
# Page & Portal Routes
# ---------------------------------------------------------------------------
@app.get("/")
async def landing_page(request: Request):
    """Public GET (Green Energy Team) Landing Page or protected plant dashboard."""
    # Check X-Forwarded-Host first (preserving original host through Cloudflare Tunnel)
    host = request.headers.get("x-forwarded-host", "").lower()
    if not host:
        host = request.headers.get("host", "").lower()
        
    print(f"[DASHBOARD] Root request received. Host header: '{host}'", flush=True)
    if host.startswith("mazara."):
        if not is_authenticated(request):
            return RedirectResponse(url="https://monitoraggioget.it/login", status_code=307)
        return FileResponse(str(STATIC_DIR / "index.html"))
    return FileResponse(str(STATIC_DIR / "get_landing.html"))

@app.get("/login")
async def login_page():
    """Client Portal Login Page."""
    return FileResponse(str(STATIC_DIR / "login.html"))

@app.post("/api/auth/login")
async def process_login(request: Request, data: dict):
    """Authenticate Client Credentials."""
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()
    cfg = load_config()
    expected_user = cfg.get("DASHBOARD_USER") or os.environ.get("DASHBOARD_USER", "admin")
    expected_pass = cfg.get("DASHBOARD_PASS") or os.environ.get("DASHBOARD_PASS", "")

    if secrets.compare_digest(username, expected_user) and secrets.compare_digest(password, expected_pass):
        response = JSONResponse({"status": "success", "message": "Login successful"})
        host = request.headers.get("host", "").split(":")[0].lower()
        is_https = request.headers.get("x-forwarded-proto", "http").lower() == "https" or request.url.scheme == "https"
        cookie_domain = ".monitoraggioget.it" if "monitoraggioget.it" in host else None

        response.set_cookie(
            key="get_session", 
            value="authenticated", 
            max_age=86400, 
            httponly=True, 
            samesite="lax",
            secure=is_https,
            domain=cookie_domain
        )
        return response
    else:
        return JSONResponse({"status": "error", "message": "Invalid username or password"}, status_code=401)

@app.get("/logout")
async def logout(request: Request):
    """Logout client session."""
    response = RedirectResponse(url="/login")
    host = request.headers.get("host", "").split(":")[0].lower()
    cookie_domain = ".monitoraggioget.it" if "monitoraggioget.it" in host else None
    if cookie_domain:
        response.delete_cookie("get_session", domain=cookie_domain)
    response.delete_cookie("get_session")
    return response

@app.get("/plants")
async def plants_page(request: Request):
    """Monitored Assets Portfolio Portal (Protected)."""
    if not is_authenticated(request):
        return RedirectResponse(url="/login", status_code=307)
    return FileResponse(str(STATIC_DIR / "plants.html"))

@app.get("/dashboard")
async def scada_dashboard(request: Request):
    """Live SCADA Control Room Interface (Protected)."""
    if not is_authenticated(request):
        return RedirectResponse(url="/login", status_code=307)
    return FileResponse(str(STATIC_DIR / "index.html"))

@app.get("/api/plants/summary")
async def get_plants_summary(request: Request):
    """Get high-level summary telemetry for the Monitored Assets card."""
    if not is_authenticated(request):
        return JSONResponse({"status": "error", "message": "Unauthorized"}, status_code=401)
    try:
        from db.db_manager import load_latest_snapshot
        today = datetime.now().strftime("%Y-%m-%d")
        snapshot = await asyncio.to_thread(load_latest_snapshot, today)
        if snapshot:
            macro = snapshot.get("macro_health", {})
            mw = macro.get("total_ac_power_mw", 0.0) or 0.0
            act_p = mw * 1000.0  # Convert MW to kW
            mwh = macro.get("total_energy_mwh", 0.0) or 0.0
            daily_e = mwh * 1000.0  # Convert MWh to kWh
            online = macro.get("online", 36)
            total = macro.get("total_inverters", 36)
            return JSONResponse({
                "status": "success",
                "active_power": round(act_p, 1),
                "daily_energy": round(daily_e, 0),
                "inverters_online": online,
                "total_inverters": total
            })
    except Exception as e:
        print(f"[SUMMARY API ERROR] {e}")
        pass
    return JSONResponse({"status": "success", "active_power": 0.0, "daily_energy": 0.0, "inverters_online": 36, "total_inverters": 36})


def fetch_ws_initial_data(today, link_status_path):
    latest_data = None
    trackers = None
    link_info = {"status": "offline"}
    try:
        from db.db_manager import load_latest_snapshot, get_daily_sensor_history, get_all_tracker_status
        latest_data = load_latest_snapshot(today)
        if link_status_path.exists():
            with open(link_status_path, "r") as f:
                link_info = json.load(f)
        if latest_data:
            latest_data["sensor_history"] = get_daily_sensor_history(today)
            latest_data["link_status"] = link_info
        trackers = get_all_tracker_status()
    except Exception as e:
        print(f"[WS] Error in fetch_ws_initial_data: {e}")
    
    settings = None
    try:
        from processor_watchdog_final import load_user_settings
        settings = load_user_settings()
    except Exception as e:
        print(f"[WS] Error loading user settings: {e}")
        
    return latest_data, trackers, settings

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    session = websocket.cookies.get("get_session")
    host = websocket.headers.get("host", "").split(":")[0].lower()
    is_local = host in ("localhost", "127.0.0.1", "192.168.10.40")

    if session != "authenticated" and not is_local:
        await websocket.close(code=1008)
        return
    await manager.connect(websocket)
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        link_status_path = ROOT / "db" / "link_status.json"
        
        latest_data, trackers, settings = await asyncio.to_thread(fetch_ws_initial_data, today, link_status_path)
        
        if latest_data:
            latest_data["system_id"] = SYSTEM_ID
            await websocket.send_json({
                "type": "data_update", 
                "data": latest_data, 
                "trackers": trackers
            })
            
        if settings:
            await websocket.send_json({"type": "config_update", "data": settings})
        
        while True:
            # wait for messages from client (ping/pong keepalive)
            message = await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        manager.disconnect(websocket)


@app.get("/api/trackers")
async def get_trackers(_: None = Depends(require_auth)):
    from db.db_manager import get_all_tracker_status
    return await asyncio.to_thread(get_all_tracker_status)


@app.get("/api/status")
async def get_settings(_: None = Depends(require_auth)):
    from processor_watchdog_final import load_user_settings
    return JSONResponse(load_user_settings())

@app.get("/api/link_status")
async def get_link_status(_: None = Depends(require_auth)):
    status_file = ROOT / "db" / "link_status.json"
    if status_file.exists():
        with open(status_file, "r") as f:
            return json.load(f)
    return JSONResponse({"status": "offline", "message": "No heartbeat received yet."})


@app.post("/api/settings")
async def update_settings(request: Request, background_tasks: BackgroundTasks, _: None = Depends(require_auth)):
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
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, analyze_site, today)
            except Exception as e:
                print(f"[!] Background settings error: {e}")

        # Queue the work and return immediately
        background_tasks.add_task(apply_changes, new_settings)
        return JSONResponse({"status": "success"})
        
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/forensic/rescan")
async def rescan(_: None = Depends(require_auth)):
    """Delete current today's snapshots, clear error folders, and re-trigger analyze_site."""
    today = datetime.now().strftime("%Y-%m-%d")
    root_errors = ROOT / "errors"
    vcom_screenshots = ROOT / "VCOM_Screenshots"

    try:
        # 1. Delete DB snapshots for today
        try:
            from db.db_manager import delete_snapshots
            await asyncio.to_thread(delete_snapshots, today)
        except Exception:
            pass

        # 2. Also delete JSON file if it exists (legacy cleanup)
        json_path = DATA_DIR / f"dashboard_data_{today}.json"
        if json_path.exists():
            json_path.unlink()
        
        # 3. Clear error screenshots
        for folder in [root_errors, vcom_screenshots]:
            if folder.exists():
                for f in folder.glob("*.png"):
                    try:
                        f.unlink()
                    except Exception:
                        pass

        # 4. Run analysis
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, analyze_site, today)
        
        return JSONResponse({"status": "success", "message": f"Rescan completed for {today}. Errors cleared."})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/telegram/test")
async def test_telegram(_: None = Depends(require_auth)):
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
    from llm_agent import ask_agent as ask_llm
except ImportError:
    ask_llm = None

@app.post("/api/chat")
async def chat_endpoint(request: Request, _: None = Depends(require_auth)):
    if not ask_llm:
        return JSONResponse({"status": "error", "message": "llm_agent module not found."}, status_code=500)
    
    try:
        body = await request.json()
        question = body.get("question")
        if not question:
            return JSONResponse({"status": "error", "message": "No question provided."}, status_code=400)
        
        # Load the latest state from database
        today = datetime.now().strftime("%Y-%m-%d")
        latest_data = None
        try:
            from db.db_manager import load_latest_snapshot
            latest_data = load_latest_snapshot(today)
        except Exception:
            # Fallback: read from JSON
            json_path = DATA_DIR / f"dashboard_data_{today}.json"
            if json_path.exists():
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if data:
                        latest_key = sorted(data.keys())[-1]
                        latest_data = data[latest_key]
        
        # Call the LLM inside a background thread pool to prevent blocking the event loop
        answer = await asyncio.to_thread(ask_llm, question, latest_data, 1, None, None, "DASHBOARD_USER")
        return JSONResponse({"status": "success", "answer": answer})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# Analytics Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/analytics/config")
async def get_analytics_config(_: None = Depends(require_auth)):
    """Return available metrics and inverters for the analytics UI."""
    try:
        from db.db_manager import METRIC_TABLE_MAP, get_available_inverters, get_available_dates
        inverters = await asyncio.to_thread(get_available_inverters)
        dates = await asyncio.to_thread(get_available_dates)
        return JSONResponse({
            "metrics": list(METRIC_TABLE_MAP.keys()),
            "inverters": inverters,
            "available_dates": dates
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/analytics/data")
async def get_analytics_data(
    metric: str, 
    start: str, 
    end: str, 
    inverters: str = None, 
    _: None = Depends(require_auth)
):
    """Fetch historical data for charting."""
    try:
        from db.db_manager import get_metric_history
        
        inv_list = [i.strip() for i in inverters.split(",") if i and i.strip()] if inverters else None
        data = await asyncio.to_thread(get_metric_history, metric, start, end, inv_list)

        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/heatmap/data")
async def get_heatmap_data(
    date: str = None,
    metric: str = "ac",
    _: None = Depends(require_auth)
):
    """Return real database metrics mapped into a 15-minute 96-slot matrix for all 36 inverters."""
    try:
        from db.db_manager import get_heatmap_matrix, get_available_dates
        import datetime
        if not date:
            dates = await asyncio.to_thread(get_available_dates)
            date = dates[-1] if dates else datetime.date.today().isoformat()
        
        data = await asyncio.to_thread(get_heatmap_matrix, date, metric)
        return JSONResponse(data)
    except Exception as e:
        print(f"[DASHBOARD] Error fetching heatmap data: {e}")
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
    print(f"[*] Local:   http://localhost:{port}", flush=True)
    print(f"[*] Network: http://{local_ip}:{port}\n", flush=True)
    
    # Start Cloudflare Tunnel (Free, Unlimited Bandwidth) in background
    def run_tunnel_bg():
        import time
        time.sleep(2)
        try:
            from tunnel_manager import start_cloudflare_tunnel
            public_url = start_cloudflare_tunnel(port)
            if public_url:
                print(f"[*] Cloudflare Tunnel Public URL: {public_url}", flush=True)
            else:
                # Fallback to ngrok if cloudflared failed
                ngrok_token = cfg.get("NGROK_AUTH_TOKEN")
                if ngrok_token and ngrok_token != "YOUR_TOKEN_HERE":
                    setup_ngrok(ngrok_token, port, cfg.get("DASHBOARD_USER", "admin"), cfg.get("DASHBOARD_PASS", ""))
        except Exception as e:
            print(f"[!] Tunnel background error: {e}", flush=True)

    import threading
    threading.Thread(target=run_tunnel_bg, daemon=True).start()
    
    print("="*60 + "\n", flush=True)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="warning",
        http="h11",
        loop="asyncio",
        ws_ping_interval=60,
        ws_ping_timeout=30,
    )
