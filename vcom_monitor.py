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

def _is_on_evaluation_page(page) -> bool:
    """Return True if we are already on the VCOM evaluation/valutazione section."""
    try:
        url = page.url.lower()
        return "valutazione" in url or "evaluation" in url or "index/index" in url
    except Exception:
        return False


def _is_on_login_page(page) -> bool:
    """Return True if the browser is showing a login form."""
    try:
        return (
            page.locator('input#username:visible, input[type="password"]:visible').count() > 0
            or "login" in page.url.lower()
            or "auth" in page.url.lower()
        )
    except Exception:
        return False


def ensure_session(page) -> bool:
    """
    Check the current page state and act accordingly:
    - Already on evaluation page → do nothing, return True
    - On login page → do full login, return True
    - Anywhere else → navigate to evaluation, return True/False
    Does NOT re-submit credentials if the session is still valid.
    """
    try:
        if _is_on_evaluation_page(page):
            logger.info("Session OK — already on evaluation page.")
            return True

        if _is_on_login_page(page):
            logger.warning("Login page detected — re-authenticating...")
            login(page)
            return True

        # Unknown state: navigate to the evaluation URL
        logger.warning(f"Unexpected page ({page.url[:60]}) — navigating back...")
        cfg = load_config()
        page.goto(cfg["SYSTEM_URL"], timeout=60_000)
        page.wait_for_load_state("networkidle", timeout=30_000)

        if _is_on_login_page(page):
            login(page)

        return _is_on_evaluation_page(page)

    except Exception as e:
        logger.error(f"Session check error: {e}")
        return False


def run_extraction_cycle(page, cycle_count: int):
    cycle_start = time.time()
    logger.info(f"=== Starting Extraction Cycle #{cycle_count} ===")

    # 0. Ensure we are on the right page WITHOUT re-logging in if session is alive
    if not ensure_session(page):
        logger.error("Could not reach evaluation page — skipping cycle.")
        return

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

LAST_CYCLE_FILE = ROOT / "db" / "last_extraction.json"


def _read_last_cycle_time() -> datetime | None:
    """Read the timestamp of the last completed extraction cycle."""
    try:
        if LAST_CYCLE_FILE.exists():
            with open(LAST_CYCLE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            return datetime.fromisoformat(data["completed_at"])
    except Exception:
        pass
    return None


def _write_last_cycle_time():
    """Persist the current time as the last completed extraction cycle."""
    LAST_CYCLE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LAST_CYCLE_FILE, "w", encoding="utf-8") as f:
        json.dump({"completed_at": datetime.now().isoformat()}, f)


def _get_interval_minutes() -> int:
    try:
        from processor_watchdog_final import load_user_settings
        return int(load_user_settings().get("collection_interval", 15))
    except Exception:
        return 15


def _sleep_remaining(interval_minutes: int, trigger_path: Path) -> bool:
    """
    Sleep until the next cycle is due, checking once per second for a
    manual trigger file. Returns True if woken by trigger, False if normal.

    Crash-resistant: reads last_extraction.json so a restart mid-sleep
    resumes the remaining wait rather than running immediately.
    """
    last = _read_last_cycle_time()
    if last:
        elapsed = (datetime.now() - last).total_seconds()
        remaining = max(0, interval_minutes * 60 - int(elapsed))
    else:
        remaining = 0  # No record → run immediately

    if remaining > 0:
        logger.info(
            f"Resuming wait: {remaining}s left of {interval_minutes}-min interval "
            f"(last cycle was {int((datetime.now() - last).total_seconds())}s ago)"
        )
    else:
        logger.info("Interval elapsed — starting next cycle immediately.")
        return False

    for _ in range(remaining):
        if trigger_path.exists():
            logger.info("Manual trigger detected — starting cycle now.")
            trigger_path.unlink(missing_ok=True)
            return True
        time.sleep(1)

    return False


def main() -> None:
    print("[EXTRACTION] Script started.", flush=True)
    ERRORS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        from db.db_manager import init_databases
        init_databases()
        logger.info("Databases initialized.")
    except Exception as e:
        logger.warning(f"Could not initialize databases: {e}")

    logger.info("VCOM monitor starting...")

    trigger_path = ROOT / ".trigger_extraction"
    busy_path    = ROOT / ".extraction_busy"

    # Clean up a stale busy flag from a previous crash
    if busy_path.exists():
        age = (datetime.now() - datetime.fromtimestamp(busy_path.stat().st_mtime)).total_seconds()
        if age > 1800:
            busy_path.unlink()
            logger.warning("Removed stale .extraction_busy flag from previous crash.")

    try:
        with sync_playwright() as p:
            is_headless = os.environ.get("VCOM_HEADLESS", "false").lower() == "true"
            logger.info(f"Launching persistent browser (headless={is_headless})...")
            browser = p.chromium.launch(headless=is_headless)
            context = browser.new_context(viewport={"width": 1450, "height": 900})
            page = context.new_page()

            # Wait out remaining interval before first cycle (crash-resistant)
            interval = _get_interval_minutes()
            triggered = _sleep_remaining(interval, trigger_path)
            if not triggered:
                logger.info("Starting initial extraction cycle...")

            print("[EXTRACTION] Initial VCOM Login...", flush=True)
            try:
                login(page)
            except Exception as e:
                logger.error(f"Initial login failed: {e}. Will retry in main loop.")
                # Don't raise, let the while loop handle it
            
            cycle_count = 1
            while True:
                busy_path.touch()

                try:
                    if page.is_closed():
                        logger.warning("Browser page was closed. Reopening...")
                        page = context.new_page()
                        login(page)

                    run_extraction_cycle(page, cycle_count)
                    _write_last_cycle_time()  # ← persist completion timestamp

                except Exception as e:
                    logger.critical(
                        f"FATAL Exception in cycle #{cycle_count}: {e}\n{traceback.format_exc()}"
                    )
                    print(f"[EXTRACTION] Fatal Error in cycle: {e}", flush=True)
                    try:
                        if not page.is_closed():
                            login(page)
                    except Exception:
                        pass

                finally:
                    if busy_path.exists():
                        busy_path.unlink()

                # Sleep for the configured interval (crash-resistant)
                interval = _get_interval_minutes()
                logger.info(f"Cycle #{cycle_count} done. Sleeping {interval} min...")
                _sleep_remaining(interval, trigger_path)

                cycle_count += 1

            browser.close()

    finally:
        if busy_path.exists():
            busy_path.unlink()
            logger.info("Removed busy flag on exit.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[EXTRACTION] Stopped by user.")
