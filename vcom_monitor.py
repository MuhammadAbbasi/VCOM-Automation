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
import threading
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


from db.vcom_status_helpers import save_vcom_status, get_vcom_status

def check_and_update_vcom_health(page) -> bool:
    """
    Check if the current page displays a VCOM outage (HTTP 404, 502, 503, 504, or Nginx error page).
    Updates vcom_status.json and returns True if healthy, False if down.
    """
    try:
        if page.is_closed():
            save_vcom_status("down", 0, "Browser page is closed")
            return False

        url = page.url.lower()
        title = ""
        try:
            title = page.title().lower()
        except Exception:
            pass

        content_snippet = ""
        try:
            content_snippet = page.content()[:3000].lower()
        except Exception:
            pass

        # Error signatures for Nginx / HTTP 404 / 502 / 503 / 504
        is_404 = "404 not found" in title or "404 not found" in content_snippet
        is_502 = "502 bad gateway" in title or "502 bad gateway" in content_snippet
        is_503 = "503 service" in title or "503 service" in content_snippet
        is_504 = "504 gateway" in title or "504 gateway" in content_snippet
        is_nginx_err = "nginx" in content_snippet and ("404" in content_snippet or "error" in content_snippet or "502" in content_snippet or "503" in content_snippet or "not found" in content_snippet)

        if is_404 or is_502 or is_503 or is_504 or is_nginx_err:
            code = 404 if is_404 else (502 if is_502 else (503 if is_503 else (504 if is_504 else 404)))
            error_desc = f"HTTP {code} Not Found / Error (nginx)" if is_nginx_err or is_404 else f"HTTP {code} Error"
            msg = f"Portale VCOM non raggiungibile ({error_desc})"
            save_vcom_status("down", code, msg)
            logger.error(f"[VCOM OUTAGE DETECTED] {msg} — URL: {page.url}")
            return False

        # If we successfully reached VCOM or evaluation section
        if "meteocontrol" in url or "valutazione" in url or "evaluation" in url or _is_on_evaluation_page(page):
            save_vcom_status("online", 200, "")
            return True

    except Exception as e:
        logger.warning(f"Error during VCOM health check: {e}")

    return True


def ensure_session(page) -> bool:
    """
    Check the current page state and act accordingly:
    - Detect 404/50x/Nginx outage screens → update status to 'down' and return False
    - Already on evaluation page → do nothing, return True
    - On login page → do full login, return True
    - Anywhere else → navigate to evaluation, return True/False
    Does NOT re-submit credentials if the session is still valid.
    """
    try:
        if not check_and_update_vcom_health(page):
            return False

        if _is_on_evaluation_page(page):
            logger.info("Session OK — already on evaluation page.")
            save_vcom_status("online", 200, "")
            return True

        if _is_on_login_page(page):
            logger.warning("Login page detected — re-authenticating...")
            login(page)
            return check_and_update_vcom_health(page)

        # Unknown state: navigate to the evaluation URL
        logger.warning(f"Unexpected page ({page.url[:60]}) — navigating back...")
        cfg = load_config()
        response = page.goto(cfg["SYSTEM_URL"], timeout=60_000)
        page.wait_for_load_state("networkidle", timeout=30_000)

        if response and response.status >= 400:
            msg = f"HTTP {response.status} Error on navigation"
            save_vcom_status("down", response.status, msg)
            logger.error(f"[VCOM OUTAGE] HTTP {response.status} returned from {cfg['SYSTEM_URL']}")
            return False

        if not check_and_update_vcom_health(page):
            return False

        if _is_on_login_page(page):
            login(page)

        healthy = _is_on_evaluation_page(page) and check_and_update_vcom_health(page)
        if healthy:
            save_vcom_status("online", 200, "")
        return healthy

    except Exception as e:
        logger.error(f"Session check error: {e}")
        err_str = str(e).lower()
        if "err_connection" in err_str or "err_name_not_resolved" in err_str or "timeout" in err_str:
            save_vcom_status("down", 503, f"Impossibile connettersi a VCOM ({e})")
        else:
            check_and_update_vcom_health(page)
        return False


def save_attribute_link(name: str, url: str) -> None:
    """Save the browser URL for the given attribute along with a timestamp."""
    try:
        links_file = ROOT / "db" / "attribute_links.json"
        links_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing data
        data = {}
        if links_file.exists():
            try:
                with open(links_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                pass
                
        if name not in data:
            data[name] = []
            
        data[name].append({
            "timestamp": datetime.now().replace(microsecond=0).isoformat(),
            "url": url
        })
        
        # Keep only the last 50 entries to prevent infinite growth
        data[name] = data[name][-50:]
        
        with open(links_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        logger.info(f"  [LINK] Saved VCOM URL for '{name}': {url[:80]}...")
    except Exception as e:
        logger.error(f"  [LINK] Failed to save attribute link for '{name}': {e}")


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
                    try:
                        save_attribute_link(name, page.url)
                    except Exception as le:
                        logger.error(f"  Error calling save_attribute_link: {le}")
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


DAILY_RESTART_HOUR = 2  # 2 AM


def _should_daily_restart(start_date: str) -> bool:
    """True once the clock is at or past DAILY_RESTART_HOUR on a new calendar day."""
    now = datetime.now()
    return now.hour >= DAILY_RESTART_HOUR and now.strftime("%Y-%m-%d") != start_date


def _do_daily_restart() -> None:
    """Back up the database then exit so the orchestrator relaunches with a fresh browser."""
    logger.info("[DAILY RESTART] 2 AM daily restart triggered — backing up database...")
    try:
        from dashboard_doctor import backup_database
        backup_database()
        logger.info("[DAILY RESTART] Backup complete.")
    except Exception as e:
        logger.warning(f"[DAILY RESTART] Backup failed (continuing with restart): {e}")
    logger.info("[DAILY RESTART] Exiting — orchestrator will relaunch with fresh session.")
    sys.exit(0)


def _sleep_remaining(interval_minutes: int, trigger_path: Path, start_date: str = "") -> str:
    """
    Sleep until the next cycle is due, checking once per second for a
    manual trigger file or the 2 AM daily-restart condition.

    Returns: 'trigger' | 'restart' | 'normal'

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
        return "normal"

    for _ in range(remaining):
        if trigger_path.exists():
            logger.info("Manual trigger detected — starting cycle now.")
            trigger_path.unlink(missing_ok=True)
            return "trigger"
        if start_date and _should_daily_restart(start_date):
            return "restart"
        time.sleep(1)

    return "normal"


# ---------------------------------------------------------------------------
# Cycle Watchdog to prevent hangs
# ---------------------------------------------------------------------------
CYCLE_TIMEOUT_SECONDS = 900.0  # 15 minutes

def cycle_watchdog_trigger(cycle_num: int) -> None:
    logger.critical(
        f"[WATCHDOG] Extraction cycle #{cycle_num} has exceeded "
        f"{CYCLE_TIMEOUT_SECONDS / 60:.1f} minutes! Force-exiting process to trigger orchestrator restart..."
    )
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(1)


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
            # Use a persistent context to save login cookies/session
            user_data_dir = ROOT / "playwright_profile"

            # Track the calendar date this process was launched on.
            # Used to detect when a new day has started for the 2 AM restart.
            start_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"[DAILY RESTART] Process start date: {start_date}. Will restart after {DAILY_RESTART_HOUR:02d}:00.")

            # Wait out remaining interval before first cycle (crash-resistant)
            interval = _get_interval_minutes()
            wake_reason = _sleep_remaining(interval, trigger_path, start_date)
            if wake_reason == "restart":
                _do_daily_restart()
            elif wake_reason != "trigger":
                logger.info("Starting initial extraction cycle...")

            cycle_count = 1
            while True:
                busy_path.touch()

                # Start the watchdog timer for the current cycle (15 minutes limit)
                watchdog = threading.Timer(CYCLE_TIMEOUT_SECONDS, cycle_watchdog_trigger, args=[cycle_count])
                watchdog.daemon = True
                watchdog.start()

                context = None
                try:
                    logger.info(f"Launching fresh browser context for cycle #{cycle_count}...")
                    context = p.chromium.launch_persistent_context(
                        user_data_dir=str(user_data_dir),
                        headless=is_headless,
                        viewport={"width": 1450, "height": 900},
                        args=["--disable-dev-shm-usage"]
                    )
                    page = context.pages[0] if context.pages else context.new_page()

                    print("[EXTRACTION] Verifying VCOM Session...", flush=True)
                    ensure_session(page)
                    run_extraction_cycle(page, cycle_count)

                except Exception as e:
                    logger.critical(
                        f"FATAL Exception in cycle #{cycle_count}: {e}\n{traceback.format_exc()}"
                    )
                    print(f"[EXTRACTION] Fatal Error in cycle: {e}", flush=True)

                finally:
                    if context:
                        try:
                            context.close()
                            logger.info("Browser context closed after cycle to release RAM.")
                        except Exception:
                            pass
                    _write_last_cycle_time()  # ← persist completion/attempt timestamp
                    # Cancel the watchdog timer as the cycle has finished
                    watchdog.cancel()
                    if busy_path.exists():
                        busy_path.unlink()

                # Check for 2 AM daily restart before sleeping
                if _should_daily_restart(start_date):
                    _do_daily_restart()

                # Signal the watchdog immediately — write a trigger file to extracted_data/
                # so the watchdog's file observer wakes up without waiting for its fallback timer.
                try:
                    trigger_file = ROOT / "extracted_data" / "extraction.trigger"
                    trigger_file.parent.mkdir(parents=True, exist_ok=True)
                    trigger_file.write_text(datetime.now().isoformat())
                except Exception:
                    pass

                # Sleep for the configured interval (crash-resistant)
                interval = _get_interval_minutes()
                logger.info(f"Cycle #{cycle_count} done. Sleeping {interval} min (Chrome RAM released)...")
                if _sleep_remaining(interval, trigger_path, start_date) == "restart":
                    _do_daily_restart()

                cycle_count += 1

    finally:
        if busy_path.exists():
            busy_path.unlink()
            logger.info("Removed busy flag on exit.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[EXTRACTION] Stopped by user.")
