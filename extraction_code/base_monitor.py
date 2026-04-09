"""
base_monitor.py — Shared sync-Playwright helpers for VCOM metric extraction.

Ported from the working Automation/extraction_code/vcom_monitor.py pattern.
Uses sync_playwright (not async) to match the proven implementation.
"""

import json
import logging
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from zipfile import BadZipFile

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config.json"
DATA_DIR = ROOT / "extracted_data"
ERRORS_DIR = ROOT / "errors"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_timestamp_fetch() -> str:
    return datetime.now().strftime("%H:%M:%S")


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def parse_italian_number(s: str):
    """Convert Italian-formatted number string to float."""
    if not isinstance(s, str):
        return s
    s = s.strip()
    if not s or s in ("-", "—", "n/a", "N/A", "--"):
        return None
    try:
        return float(s.replace(".", "").replace(",", "."))
    except ValueError:
        return s


# ---------------------------------------------------------------------------
# Excel append
# ---------------------------------------------------------------------------

def append_df_to_excel(filename: str, df: pd.DataFrame, sheet_name: str = "Sheet1") -> None:
    """Append df to an existing Excel file, or create it if it doesn't exist.
    
    If the existing file is corrupted (Bad CRC / malformed XML — common on
    network shares), the corrupt file is backed up and a fresh one is created
    so that the extraction cycle can continue without crashing.
    """
    if not os.path.exists(filename):
        df.to_excel(filename, index=False, sheet_name=sheet_name)
        return

    try:
        with pd.ExcelWriter(filename, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
            write_header = sheet_name not in writer.sheets
            startrow = writer.sheets[sheet_name].max_row if not write_header else 0
            df.to_excel(writer, sheet_name=sheet_name, startrow=startrow, index=False, header=write_header)

    except (BadZipFile, Exception) as exc:
        # Catch corrupted-file errors: BadZipFile, XML ParseError, etc.
        exc_name = type(exc).__name__
        if isinstance(exc, BadZipFile) or "ParseError" in exc_name or "not well-formed" in str(exc):
            logger.warning(
                f"Corrupted Excel detected ({exc_name}): {filename} — "
                f"backing up and creating fresh file."
            )
            # Back up the corrupted file
            backup = filename + f".corrupt_{datetime.now().strftime('%H%M%S')}.bak"
            try:
                shutil.copy2(filename, backup)
                logger.info(f"Backed up corrupt file to {backup}")
            except Exception as copy_err:
                logger.warning(f"Could not back up corrupt file: {copy_err}")

            # Remove the corrupted file and write fresh
            try:
                os.remove(filename)
            except Exception:
                pass
            df.to_excel(filename, index=False, sheet_name=sheet_name)
            logger.info(f"Created fresh Excel: {filename}")
        else:
            raise  # Re-raise unexpected errors


def export_metric(df: pd.DataFrame, prefix: str) -> None:
    """Stamp with current time and append to the daily Excel file."""
    if df is None or df.empty:
        logger.warning(f"[{prefix}] Empty DataFrame — skipping export.")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    current_time = get_timestamp_fetch()

    if "Timestamp Fetch" not in df.columns:
        df.insert(0, "Timestamp Fetch", current_time)

    filepath = str(DATA_DIR / f"{prefix}_{today_str()}.xlsx")
    append_df_to_excel(filepath, df)
    logger.info(f"[OK] Appended {len(df)} rows -> {filepath}")


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

def login(page) -> None:
    """
    Authenticate to VCOM and navigate to the Valutazione (Evaluation) section.

    Steps (mirrored from the working Automation implementation):
      1. Go to SYSTEM_URL
      2. Fill username + password, click Login button
      3. Dismiss cookie banner
      4. Wait for 'Valutazione' link → click it
      5. Wait for 'Inverter' to confirm we're on the evaluation page
    """
    cfg = load_config()
    logger.info("Logging into VCOM meteocontrol...")
    page.goto(cfg["SYSTEM_URL"], timeout=60_000)

    # Fill login form
    page.wait_for_selector('input[type="text"]', timeout=30_000)
    page.fill('input[type="text"]', cfg["USERNAME"])
    page.fill('input[type="password"]', cfg["PASSWORD"])
    page.locator('button:has-text("Login"), button[type="submit"]').first.click()

    # Dismiss cookie banner if it appears
    try:
        if page.locator('button:has-text("Usa solo i cookie necessari")').is_visible(timeout=5_000):
            page.locator('button:has-text("Usa solo i cookie necessari")').click()
    except Exception:
        pass

    # After login VCOM lands on the Control Center main page.
    # Click the "Valutazione" tab to reach the evaluation/analysis section.
    logger.info("Navigating to Valutazione section...")
    page.wait_for_selector('text="Valutazione"', timeout=60_000)
    page.locator('text="Valutazione"').first.click()

    # Confirm we're on the right page
    try:
        page.wait_for_selector('text="Inverter"', timeout=30_000)
        logger.info("Successfully reached the Evaluation dashboard.")
    except Exception as e:
        logger.error(f"Could not confirm evaluation dashboard: {e}")
        ERRORS_DIR.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(ERRORS_DIR / "login_error.png"))


# ---------------------------------------------------------------------------
# Popup dismissal
# ---------------------------------------------------------------------------

def dismiss_popup(page) -> None:
    """Dismiss the 'Valori minimi non disponibili' popup (and any other Chiudi dialog).

    This popup appears when minute-resolution data is unavailable for the
    current component selection.  Clicking 'Chiudi' closes it and lets
    extraction continue with whatever resolution is available.
    """
    try:
        chiudi = page.locator('button:has-text("Chiudi"):visible')
        chiudi.first.wait_for(state="visible", timeout=4_000)
        chiudi.first.click()
        logger.info("Dismissed 'Chiudi' popup.")
        time.sleep(1)
    except Exception:
        pass  # No popup — that's fine
    logger.info("Out of dismiss_popup")


# ---------------------------------------------------------------------------
# Inverter selection
# ---------------------------------------------------------------------------

def select_inverters(page) -> None:
    """Ensure only the 36 target inverters are selected, excluding SunGrow."""
    cfg = load_config()
    logger.info("Selecting target inverters...")
    try:
        # Deselect all first for a clean slate
        if page.locator('button:has-text("Deseleziona tutto")').is_visible():
            page.locator('button:has-text("Deseleziona tutto")').click()
            time.sleep(1)

        for inv_id in cfg.get("INVERTER_IDS", []):
            cb = page.locator(f"input#checkbox-{inv_id}")
            if cb.is_visible():
                cb.check()

        # Ensure SunGrow is NOT checked
        sungrow_cb = page.locator('input[id*="Id27848313"]')
        if sungrow_cb.is_visible() and sungrow_cb.is_checked():
            sungrow_cb.uncheck()

        # Refresh chart after selection
        btn = page.locator('button:has-text("Aggiorna grafico"), button:has-text("Update chart")')
        if btn.is_visible():
            btn.click()
            time.sleep(2)

    except Exception as e:
        logger.warning(f"Inverter selection error: {e}")


# ---------------------------------------------------------------------------
# Shared per-metric helpers
# ---------------------------------------------------------------------------

def toggle_minute_values(page, metric_name: str) -> None:
    """Toggle 'Valori in minuti' ON if not already active.

    After clicking the toggle the platform may show a 'Valori minimi non
    disponibili' popup — dismiss_popup() handles that.
    """
    try:
        page.wait_for_selector('button[title="acceso"]:visible', timeout=10_000)
        acceso_btn = page.locator('button[title="acceso"]:visible').first
        cls = acceso_btn.get_attribute("class") or ""
        if "active" not in cls:
            logger.info(f"Toggling 'Valori in minuti' ON for {metric_name}...")
            print(f"Toggling 'Valori in minuti' ON for {metric_name}...")
            acceso_btn.click()
            dismiss_popup(page)  # handles Chiudi if it appears
            time.sleep(3)
    except Exception:
        logger.warning(f"Could not toggle 'Valori in minuti' for {metric_name}.")
    logger.info(f"Out of toggle_minute_values for {metric_name}")


def refresh_chart(page) -> None:
    """Click 'Aggiorna grafico' if visible."""
    try:
        btn = page.locator('button:has-text("Aggiorna grafico"), button:has-text("Update chart")')
        if btn.is_visible(timeout=3_000):
            btn.click()
            time.sleep(2)
    except Exception:
        pass
    logger.info("Out of refresh_chart")


def click_dati_tab(page, extra_wait: float = 0) -> None:
    """Switch to the 'Dati' (data table) tab using a simple text search.
    
    Retries up to 3 times to handle DOM-detachment errors that occur when
    the VCOM page re-renders while we're interacting with it.
    """
    logger.info("Locating 'Dati' tab button...")
    print("[*] Transitioning to 'Dati' (Data) view...", flush=True)
    
    max_attempts = 3
    last_err = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            # VCOM evaluation pages can be long
            page.evaluate("window.scrollTo(0, 450)")
            time.sleep(0.5)  # brief settle after scroll
            
            # Re-query the locator each attempt to avoid stale references
            tab = page.get_by_text("Dati", exact=True).last
            
            # Use wait_for first (doesn't require attached), then scroll
            tab.wait_for(state="visible", timeout=20_000)
            
            try:
                tab.scroll_into_view_if_needed()
            except Exception:
                # If scroll fails (detached DOM), wait and re-query
                time.sleep(1)
                tab = page.get_by_text("Dati", exact=True).last
                tab.wait_for(state="visible", timeout=10_000)
            
            # Check if already active
            parent_cls = tab.evaluate("el => el.parentElement ? el.parentElement.className : ''")
            if "active" in parent_cls or "selected" in parent_cls or "ui-tabs-active" in parent_cls:
                logger.info("'Dati' tab is already active.")
                print("[*] 'Dati' already active.", flush=True)
            else:
                tab.click()
                logger.info("Clicked 'Dati' tab.")
                print("[OK] 'Dati' tab clicked.", flush=True)
            
            # Wait for table rendering if requested
            if extra_wait > 0:
                logger.info(f"Waiting extra {extra_wait}s for table render...")
                time.sleep(extra_wait)
            
            return  # success

        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                logger.warning(f"click_dati_tab attempt {attempt} failed ({type(e).__name__}), retrying...")
                time.sleep(2)
            else:
                logger.error(f"Failed to click 'Dati' tab button after {max_attempts} attempts: {e}")
                print(f"[!] FAILED to find 'Dati' tab button: {type(e).__name__}", flush=True)
                ERRORS_DIR.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(ERRORS_DIR / "error_clicking_dati_final.png"))
                raise


def extract_infotab_table_js(page, metric_name: str, row_timeout: int = 20_000) -> pd.DataFrame:
    """Extract #infotab-data table via a single JavaScript call.

    Used for wide tables (e.g. Corrente DC with 808 string columns) where
    per-cell Playwright DOM traversal causes browser Out-of-Memory crashes.
    All data is serialised inside the browser in one evaluate() call and
    returned as a plain list-of-lists — no per-cell round trips.

    *row_timeout* controls how long to wait for the first table row to appear.
    Use a higher value (e.g. 30_000) for large tables like Corrente DC.
    """
    try:
        # Take a screenshot to see if the table is actually there
        logger.info(f"[Table Extraction] Checking state for {metric_name}...")
        ERRORS_DIR.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(ERRORS_DIR / f"debug_{metric_name.replace(' ', '_')}_pre_wait.png"))
        
        logger.info(f"Waiting for {metric_name} table rows (timeout={row_timeout}ms)...")
        page.locator("#infotab-data table tbody tr").first.wait_for(state="visible", timeout=row_timeout)
    except Exception:
        logger.warning(f"No data rows found for {metric_name} after waiting.")
        # Final screenshot of failure
        page.screenshot(path=str(ERRORS_DIR / f"debug_{metric_name.replace(' ', '_')}_no_rows.png"))
        return pd.DataFrame()

    logger.info(f"Evaluating JS extraction for {metric_name}...")
    result = page.evaluate("""() => {
        const table = document.querySelector('#infotab-data table');
        if (!table) return { headers: [], rows: [] };

        // Headers
        const thEls = Array.from(table.querySelectorAll('thead tr th'));
        const headers = thEls.map(th => th.innerText.trim());

        // Rows
        const trEls = Array.from(table.querySelectorAll('tbody tr'));
        const rows = trEls.map(tr =>
            Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim())
        );
        return { headers, rows };
    }""")

    raw_headers = result.get("headers", [])
    raw_rows = result.get("rows", [])

    # Filter SunGrow columns
    ignored = {i for i, h in enumerate(raw_headers) if "SunGrow" in h}
    headers = [h for i, h in enumerate(raw_headers) if i not in ignored]

    logger.info(f"{metric_name}: {len(headers)} columns, {len(raw_rows)} rows found")

    parsed_rows = []
    for row in raw_rows:
        filtered = [cell for i, cell in enumerate(row) if i not in ignored]
        converted = []
        for j, cell in enumerate(filtered):
            if j == 0:
                converted.append(cell)   # Ora — keep as string
            else:
                converted.append(parse_italian_number(cell))
        parsed_rows.append(converted)

    logger.info(f"Finished parsing {metric_name} data.")
    return pd.DataFrame(parsed_rows, columns=headers) if headers else pd.DataFrame(parsed_rows)


def extract_infotab_table(page, metric_name: str) -> pd.DataFrame:
    """Extract the standard #infotab-data table (used by 5 of 6 metrics)."""
    rows_locator = page.locator("#infotab-data table tbody tr")
    try:
        rows_locator.first.wait_for(state="visible", timeout=20_000)
    except Exception:
        logger.warning(f"No data rows found for {metric_name}.")
        return pd.DataFrame()

    # Headers
    headers_raw = [h.inner_text().strip() for h in page.locator("#infotab-data table thead tr th").all()]
    header_texts = []
    ignored_indices = set()
    for i, h in enumerate(headers_raw):
        if "SunGrow" in h:
            ignored_indices.add(i)
        else:
            header_texts.append(h)

    logger.info(f"{metric_name} headers: {header_texts[:5]}{'…' if len(header_texts) > 5 else ''}")

    # Rows
    results = []
    for row in rows_locator.all():
        cells = row.locator("td").all_inner_texts()
        filtered = [cells[i].strip() for i in range(len(cells)) if i not in ignored_indices]
        converted = []
        for j, cell in enumerate(filtered):
            if j == 0:  # Ora column — keep as string
                converted.append(cell)
            else:
                converted.append(parse_italian_number(cell))
        results.append(converted)

    return pd.DataFrame(results, columns=header_texts) if header_texts else pd.DataFrame(results)
