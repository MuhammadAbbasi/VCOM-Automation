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

METRICS = [
    ("PR inverter", extract_pr),
    ("Potenza AC", extract_potenza_ac),
    ("Corrente DC", extract_corrente_dc),
    ("Resistenza di isolamento", extract_resistenza),
    ("Temperatura", extract_temperatura),
    ("Irraggiamento", extract_irraggiamento),
]

# ---------------------------------------------------------------------------
# Main Logic
# ---------------------------------------------------------------------------

def run_extraction_cycle(page, cycle_count: int):
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
    for name, extractor in METRICS:
        logger.info(f"Extracting: {name}")
        success = False
        for attempt in range(1, 3):
            try:
                df = extractor(page)
                if df is not None and not df.empty:
                    export_metric(df, name)
                    success = True
                    break
            except Exception as e:
                logger.warning(f"  Attempt {attempt} failed for {name}: {e}")
                time.sleep(2)
        
        if not success:
            logger.error(f"  {name} failed after 2 attempts.")

def main() -> None:
    print("[EXTRACTION] Script started.", flush=True)
    ERRORS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("VCOM monitor starting...")

    trigger_path = ROOT / ".trigger_extraction"
    busy_path = ROOT / ".extraction_busy"

    try:
        # Mark as busy as soon as we start
        busy_path.touch()
        logger.info(f"Sync flags: busy={busy_path}, trigger={trigger_path}")

        print("[EXTRACTION] Initializing Playwright...", flush=True)
        with sync_playwright() as p:
            is_headless = os.environ.get("VCOM_HEADLESS", "false").lower() == "true"
            print(f"[EXTRACTION] Launching Chromium (headless={is_headless})...", flush=True)
            browser = p.chromium.launch(headless=is_headless)
            context = browser.new_context(viewport={"width": 1450, "height": 900})
            page = context.new_page()

            try:
                print("[EXTRACTION] Attempting VCOM Login...", flush=True)
                login(page)
                print("[EXTRACTION] Login successful.", flush=True)

                cycle_count = 1
                while True:
                    # Refresh busy flag just in case
                    busy_path.touch()
                    
                    try:
                        run_extraction_cycle(page, cycle_count)
                    except Exception:
                        logger.critical(f"Unhandled error in cycle #{cycle_count}:\n{traceback.format_exc()}")
                        try:
                            ss_path = ERRORS_DIR / f"fatal_cycle{cycle_count}.png"
                            page.screenshot(path=str(ss_path))
                        except Exception:
                            pass

                    # Mark as IDLE while sleeping (important for manual trigger to work)
                    if busy_path.exists(): busy_path.unlink()

                    try:
                        from processor_watchdog_final import load_user_settings
                        settings = load_user_settings()
                        current_interval = settings.get("collection_interval", 15)
                    except Exception:
                        current_interval = 15

                    logger.info(f"Sleeping {current_interval} minutes until next cycle (or manual trigger)...")
                    
                    # 3. INTERRUPTIBLE SLEEP
                    sleep_seconds = int(current_interval * 60)
                    for _ in range(sleep_seconds):
                        if trigger_path.exists():
                            print("[EXTRACTION] Manual trigger detected!", flush=True)
                            logger.info("Manual trigger detected! Breaking sleep.")
                            trigger_path.unlink()
                            break
                        time.sleep(1)
                    
                    cycle_count += 1

            except KeyboardInterrupt:
                logger.info("Interrupted by user.")
            except Exception as e:
                logger.critical(f"FATAL Exception in main loop: {e}\n{traceback.format_exc()}")
            finally:
                print("[EXTRACTION] Closing browser...", flush=True)
                browser.close()

    finally:
        # Final cleanup of busy flag
        if busy_path.exists():
            busy_path.unlink()
            logger.info("Removed busy flag on exit.")

if __name__ == "__main__":
    main()
