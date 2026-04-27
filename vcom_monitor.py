"""
vcom_monitor.py — VCOM Playwright scraper (sync).

Runs a continuous 10-minute extraction loop:
  1. Open a visible Chromium browser at 1450×900
  2. Login and navigate to the Valutazione evaluation dashboard
  3. Select the 36 target inverters
  4. For each of the 6 metrics: extract → append to daily Excel file
  5. Each metric has 2 retry attempts on failure
  6. Sleep 10 minutes, repeat
  7. Each cycle checks if the session is still alive; re-logs in if not

Run with:
    python vcom_monitor.py
"""

print("VCOM MONITOR STARTING...")

import logging
import os
import sys
import time
import json
import traceback
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Fix for Windows console encoding issues with emojis/special characters
if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        except Exception:
            pass

from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Paths & logging
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
ERRORS_DIR = ROOT / "errors"
LOG_PATH = ROOT / "monitoring.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EXTRACTION] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(LOG_PATH, maxBytes=1_000_000_000, backupCount=3, encoding="utf-8"),
    ],
)
logger = logging.getLogger("vcom_monitor")

# Add SQLite log handler for extraction logs
try:
    from db.db_manager import SQLiteLogHandler
    _sqlite_handler = SQLiteLogHandler(source_name="extraction")
    _sqlite_handler.setFormatter(logging.Formatter("%(asctime)s [EXTRACTION] %(levelname)s %(message)s"))
    logger.addHandler(_sqlite_handler)
except Exception:
    pass  # DB module may not be initialized yet

# ---------------------------------------------------------------------------
# Metric extractors
# ---------------------------------------------------------------------------
from extraction_code.base_monitor import login, select_inverters, export_metric, load_config, dismiss_popup
from extraction_code.pr_monitor import extract_pr
from extraction_code.potenza_ac_monitor import extract_potenza_ac
from extraction_code.corrente_dc_monitor import extract_corrente_dc
from extraction_code.resistenza_monitor import extract_resistenza
from extraction_code.temperatura_monitor import extract_temperatura
from extraction_code.irraggiamento_monitor import extract_irraggiamento
from extraction_code.potenza_attiva_monitor import extract_potenza_attiva

METRICS = [
    ("PR inverter", extract_pr),
    ("Potenza AC", extract_potenza_ac),
    ("Corrente DC", extract_corrente_dc),
    ("Resistenza di isolamento", extract_resistenza),
    ("Temperatura", extract_temperatura),
    ("Irraggiamento", extract_irraggiamento),
    ("Potenza attiva", extract_potenza_attiva),
]

# ---------------------------------------------------------------------------
# Main Logic
# ---------------------------------------------------------------------------

def run_extraction_cycle(page, cycle_count: int):
    cycle_start = time.time()
    logger.info(f"=== Starting Extraction Cycle #{cycle_count} ===")
    
    # 0. Quick session check
    try:
        if not page.locator('text=/Irraggiamento|Potenza AC/').first.is_visible(timeout=5000):
            logger.warning("Session likely expired. Re-logging...")
            login(page)
    except Exception as e:
        logger.warning(f"Session check failed ({e}). Attempting re-login anyway...")
        try:
            login(page)
        except Exception as login_err:
            logger.error(f"Re-login failed: {login_err}")
            return # Skip this cycle

    # 1. Select inverters
    select_inverters(page)

    # 2. Extract metrics
    metric_timings = {}
    for name, extractor in METRICS:
        metric_start = time.time()
        logger.info(f"Extracting: {name}")
        success = False
        for attempt in range(1, 4):  # Increased to 3 attempts
            try:
                df = extractor(page)
                if df is not None and not df.empty:
                    export_metric(df, name)
                    success = True
                    break
                else:
                    logger.warning(f"  Attempt {attempt} for {name} returned empty data.")
            except Exception as e:
                logger.warning(f"  Attempt {attempt} failed for {name}: {e}")
                if "detached from the DOM" in str(e) or "attached to the DOM" in str(e):
                    logger.info("  DOM detachment detected. Reloading page...")
                    page.reload()
                    page.wait_for_load_state("networkidle")
                    select_inverters(page) # Re-select after reload
                
                time.sleep(3)
        
        duration = time.time() - metric_start
        metric_timings[name] = duration
        if success:
            logger.info(f"  [OK] {name} extracted in {duration:.2f}s")
        else:
            logger.error(f"  [FAIL] {name} failed after {duration:.2f}s")

    total_duration = time.time() - cycle_start
    logger.info(f"=== Cycle #{cycle_count} Finished in {total_duration:.2f}s ===")
    
    # Optional: Detailed summary
    summary = " | ".join([f"{n}: {d:.1f}s" for n, d in metric_timings.items()])
    logger.info(f"Summary: {summary}")

def main() -> None:
    print("[EXTRACTION] Script started.", flush=True)
    ERRORS_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize databases
    try:
        from db.db_manager import init_databases
        init_databases()
        logger.info("Databases initialized.")
    except Exception as e:
        logger.warning(f"Could not initialize databases: {e}")

    logger.info("VCOM monitor starting...")

    trigger_path = ROOT / ".trigger_extraction"
    busy_path = ROOT / ".extraction_busy"

    try:
        with sync_playwright() as p:
            is_headless = os.environ.get("VCOM_HEADLESS", "false").lower() == "true"
            logger.info(f"Launching persistent browser (headless={is_headless})...")
            browser = p.chromium.launch(headless=is_headless)
            context = browser.new_context(viewport={"width": 1450, "height": 900})
            page = context.new_page()
            
            print("[EXTRACTION] Initial VCOM Login...", flush=True)
            login(page)
            
            cycle_count = 1
            while True:
                # Mark as busy as soon as we start
                busy_path.touch()
                
                try:
                    if page.is_closed():
                        logger.warning("Browser page was closed. Reopening...")
                        page = context.new_page()
                        login(page)

                    run_extraction_cycle(page, cycle_count)
                    
                except Exception as e:
                    logger.critical(f"FATAL Exception in cycle #{cycle_count}: {e}\n{traceback.format_exc()}")
                    print(f"[EXTRACTION] Fatal Error in cycle: {e}", flush=True)
                    # Simple recovery: try re-login if page is still alive
                    try:
                        if not page.is_closed():
                            login(page)
                    except:
                        pass

                # Mark as IDLE while sleeping
                if busy_path.exists(): busy_path.unlink()

                try:
                    from processor_watchdog_final import load_user_settings
                    settings = load_user_settings()
                    current_interval = settings.get("collection_interval", 15)
                except Exception:
                    current_interval = 15

                logger.info(f"Sleeping {current_interval} minutes until next cycle...")
                
                # INTERRUPTIBLE SLEEP
                sleep_seconds = int(current_interval * 60)
                for _ in range(sleep_seconds):
                    if trigger_path.exists():
                        print("[EXTRACTION] Manual trigger detected!", flush=True)
                        trigger_path.unlink()
                        break
                    time.sleep(1)
                
                cycle_count += 1

            browser.close()

    finally:
        # Final cleanup of busy flag
        if busy_path.exists():
            busy_path.unlink()
            logger.info("Removed busy flag on exit.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[EXTRACTION] Stopped by user.")
