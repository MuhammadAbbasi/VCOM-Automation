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
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger("vcom_monitor")

# ---------------------------------------------------------------------------
# Metric extractors
# ---------------------------------------------------------------------------
from extraction_code.base_monitor import login, select_inverters, export_metric, load_config
from extraction_code.pr_monitor import extract_pr
from extraction_code.potenza_ac_monitor import extract_potenza_ac
from extraction_code.corrente_dc_monitor import extract_corrente_dc, download_corrente_dc_csv
from extraction_code.resistenza_monitor import extract_resistenza
from extraction_code.temperatura_monitor import extract_temperatura
from extraction_code.irraggiamento_monitor import extract_irraggiamento

METRICS = [
    ("PR",                    extract_pr,             "PR"),
    ("Potenza AC",            extract_potenza_ac,     "Potenza_AC"),
    ("Corrente DC",           extract_corrente_dc,    "Corrente_DC"),
    ("Resistenza Isolamento", extract_resistenza,     "Resistenza_Isolamento"),
    ("Temperatura",           extract_temperatura,    "Temperatura"),
    ("Irraggiamento",         extract_irraggiamento,  "Irraggiamento"),
]

# Will dynamically fetch from user settings, defaults to 3
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# Session health check
# ---------------------------------------------------------------------------

def ensure_on_evaluation_page(page) -> None:
    """Check session is alive; click 'Valutazione' if visible, else re-login."""
    valutazione_selector = 'a[title="Valutazione"]'
    try:
        if page.locator(valutazione_selector).is_visible(timeout=5_000):
            page.locator(valutazione_selector).first.click()
            time.sleep(2)
        else:
            logger.warning("Session may have expired — re-logging in...")
            login(page)
    except Exception:
        logger.warning("Navigation check failed — attempting re-login...")
        login(page)


# ---------------------------------------------------------------------------
# Status reporting
# ---------------------------------------------------------------------------

def update_extraction_status(metric_prefix: str, status: str) -> None:
    """Save extraction status to a JSON file for the dashboard watchdog.
    Statuses: 'success', 'empty', 'failed'.
    """
    try:
        from extraction_code.base_monitor import DATA_DIR, today_str
        status_path = DATA_DIR / "extraction_status.json"
        
        data = {}
        if status_path.exists():
            try:
                with open(status_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        
        today = today_str()
        if today not in data:
            data[today] = {}
        
        data[today][metric_prefix] = {
            "status": status,
            "timestamp": datetime.now().isoformat(timespec="seconds")
        }
        
        with open(status_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to update extraction status: {e}")


# ---------------------------------------------------------------------------
# Single extraction cycle
# ---------------------------------------------------------------------------

def run_extraction_cycle(page, cycle_count: int) -> None:
    print(f"\n\n{'#' * 80}", flush=True)
    print(f"### [CYCLE #{cycle_count}] STARTING EXTRACTION @ {datetime.now().strftime('%H:%M:%S')}", flush=True)
    print(f"{'#' * 80}", flush=True)
    
    logger.info(f"=== Starting Cycle #{cycle_count} ===")

    # Ensure we're on the evaluation page and inverters are selected
    try:
        print(f"[*] Navigating: {load_config()['SYSTEM_URL']}...", flush=True)
        ensure_on_evaluation_page(page)
        print("[*] Selecting 36 Inverters...", flush=True)
        select_inverters(page)
    except Exception as e:
        print(f"[!] SETUP FAILED: {e}", flush=True)
        logger.error(f"Pre-extraction setup failed: {e}")
        return

    for label, extractor, prefix in METRICS:
        print(f"\n[>] EXTRACTING: {label}...", flush=True)
        df = None
        success = False
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"    (Attempt {attempt}/{MAX_RETRIES})...", flush=True)
                
                # Special fallback for Corrente DC on the 3rd attempt (after 2 failures)
                if label == "Corrente DC" and attempt == 3:
                     df = download_corrente_dc_csv(page)
                else:
                     df = extractor(page)
                
                if df is not None and not df.empty:
                    success = True
                    break
                else:
                    raise ValueError("Empty or None DataFrame returned")
                    
            except Exception as e:
                print(f"    (!!) Attempt {attempt} failed: {type(e).__name__}", flush=True)
                logger.error(f"[{label}] Attempt {attempt} failed:\n{traceback.format_exc()}")
                ERRORS_DIR.mkdir(parents=True, exist_ok=True)
                try:
                    page.screenshot(path=str(ERRORS_DIR / f"error_{label.replace(' ', '_')}_attempt1.png"))
                except Exception:
                    pass
                time.sleep(5)

        if not success:
            print(f"[!] {label}: ALL ATTEMPTS FAILED.", flush=True)
            update_extraction_status(prefix, "failed")
        elif df is None or df.empty:
            print(f"[?] {label}: NO DATA FOUND.", flush=True)
            update_extraction_status(prefix, "empty")
        else:
            print(f"[OK] {label}: Extracted {len(df)} rows.", flush=True)
            try:
                export_metric(df, prefix)
                update_extraction_status(prefix, "success")
            except Exception as export_err:
                logger.error(f"[{label}] Export failed (will retry next cycle): {export_err}")
                print(f"[!] {label}: EXPORT FAILED — {type(export_err).__name__}", flush=True)
                update_extraction_status(prefix, "failed")

    print(f"\n{'#' * 80}", flush=True)
    print(f"### [CYCLE #{cycle_count}] COMPLETED SUCCESSFULLY @ {datetime.now().strftime('%H:%M:%S')}", flush=True)
    print(f"{'#' * 80}\n", flush=True)
    logger.info(f"=== Cycle #{cycle_count} finished successfully ===")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("[EXTRACTION] Script started.", flush=True)
    ERRORS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("VCOM monitor starting...")

    print("[EXTRACTION] Initializing Playwright...", flush=True)
    with sync_playwright() as p:
        print("[EXTRACTION] Launching Chromium (visible)...", flush=True)
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1450, "height": 900})
        page = context.new_page()

        try:
            print("[EXTRACTION] Attempting VCOM Login...", flush=True)
            login(page)
            print("[EXTRACTION] Login successful.", flush=True)

            cycle_count = 1
            while True:
                try:
                    run_extraction_cycle(page, cycle_count)
                except Exception:
                    logger.critical(f"Unhandled error in cycle #{cycle_count}:\n{traceback.format_exc()}")
                    try:
                        page.screenshot(path=str(ERRORS_DIR / f"fatal_cycle{cycle_count}.png"))
                    except Exception:
                        pass

                try:
                    from processor_watchdog_final import load_user_settings
                    current_interval = load_user_settings().get("collection_interval", 15)
                except Exception:
                    current_interval = 15

                logger.info(f"Sleeping {current_interval} minutes until next cycle...")
                time.sleep(current_interval * 60)
                cycle_count += 1

        except KeyboardInterrupt:
            logger.info("Interrupted by user.")
        except Exception:
            logger.critical(f"FATAL:\n{traceback.format_exc()}")
            try:
                page.screenshot(path=str(ERRORS_DIR / "fatal_error.png"))
            except Exception:
                pass
        finally:
            logger.info("Closing browser.")
            browser.close()


if __name__ == "__main__":
    main()
