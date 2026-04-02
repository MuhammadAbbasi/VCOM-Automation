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
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Import analysis logic
from processor_watchdog import analyze_site

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
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        app,
        host="localhost",
        port=8080,
        log_level="info",
    )
