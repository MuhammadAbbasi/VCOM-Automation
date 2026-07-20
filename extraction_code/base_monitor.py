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
# CSV append
# ---------------------------------------------------------------------------

def write_df_to_csv(filename: str, df: pd.DataFrame) -> None:
    """Write df to a CSV file (overwriting existing)."""
    df.to_csv(filename, index=False, header=True, encoding="utf-8")


def export_metric(df: pd.DataFrame, prefix: str) -> None:
    """Stamp with current time and save to the SQLite database."""
    if df is None or df.empty:
        logger.warning(f"[{prefix}] Empty DataFrame — skipping export.")
        return

    # Ensure we have actual data/inverter columns (excluding time and metadata)
    data_cols = [c for c in df.columns if c not in ("Ora", "Timestamp Fetch", "_date", "date", "ora", "timestamp_fetch")]
    if not data_cols:
        logger.warning(f"[{prefix}] DataFrame contains only time/metadata columns — skipping export.")
        return

    current_time = get_timestamp_fetch()

    if "Timestamp Fetch" not in df.columns:
        df.insert(0, "Timestamp Fetch", current_time)

    # Save to database
    try:
        from db.db_manager import save_metric, save_extraction_status
        date_str = today_str()
        save_metric(df, prefix, date_str)
        logger.info(f"[OK] Saved {len(df)} rows -> DB ({prefix}, {date_str})")

        # Map the naming convention for extraction status tracking
        key_name = prefix.replace(" ", "_") if prefix in ["PR inverter", "Potenza AC", "Corrente DC", "Resistenza di isolamento", "Potenza attiva", "Irraggiamento", "Temperatura"] else prefix
        if prefix == "Resistenza di isolamento": key_name = "Resistenza_Isolamento"
        if prefix == "PR inverter": key_name = "PR"
        if prefix == "Potenza attiva": key_name = "Potenza_Attiva"
        logger.info(f"[STATUS] Saving extraction status: prefix='{prefix}' -> key_name='{key_name}'")
        save_extraction_status(date_str, key_name, "success")
    except Exception as e:
        logger.error(f"[DB] Failed to save {prefix} to database: {e}")
        # Fallback: write to CSV so data isn't lost
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        filepath = str(DATA_DIR / f"{prefix}_{today_str()}.csv")
        write_df_to_csv(filepath, df)
        logger.warning(f"[FALLBACK] Wrote CSV: {filepath}")
        # Still record the extraction status so the dashboard doesn't show PENDING
        try:
            from db.db_manager import save_extraction_status
            date_str = today_str()
            key_name = prefix.replace(" ", "_")
            if prefix == "Resistenza di isolamento": key_name = "Resistenza_Isolamento"
            if prefix == "PR inverter": key_name = "PR"
            if prefix == "Potenza attiva": key_name = "Potenza_Attiva"
            save_extraction_status(date_str, key_name, "error")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

def login(page) -> None:
    """
    Authenticate to VCOM and navigate to the Valutazione (Evaluation) section.

    Steps (updated for the new Keycloak login flow):
      1. Go to SYSTEM_URL
      2. Fill username (input#username)
      3. Click 'Continua' if needed or proceed to password
      4. Fill password (input#password)
      5. Click 'Accedi' (input#kc-login)
      6. Dismiss cookie banner
      7. Wait for 'Valutazione' link → click it
    """
    cfg = load_config()
    
    # Check if already logged in
    try:
        if page.locator('a[title="Valutazione"]').count() > 0 or "valutazione" in page.url.lower():
            logger.info("Already logged in. Skipping navigation to login page.")
        else:
            logger.info("Logging into VCOM meteocontrol...")
            page.goto(cfg["SYSTEM_URL"], timeout=60_000)
    except Exception:
        page.goto(cfg["SYSTEM_URL"], timeout=60_000)

    # Dismiss cookie banner early
    try:
        page.locator('button:has-text("Usa solo i cookie necessari"), button:has-text("Accetta tutti i cookie")').click(timeout=3_000)
    except Exception:
        pass

    try:
        # Check for legacy vs modern login
        page.wait_for_load_state("networkidle", timeout=30_000)
        
        # 1. Detect and Handle Legacy Login (Username & Password together)
        legacy_pass = page.locator('input[type="password"]:visible')
        if legacy_pass.count() > 0:
            logger.info("Detected legacy login page. Filling credentials...")
            page.locator('input[type="text"]:visible').first.fill(cfg["USERNAME"])
            page.locator('input[type="password"]:visible').first.fill(cfg["PASSWORD"])
            page.locator('button:has-text("Login"), button[type="submit"]').first.click()
        
        # 2. Detect and Handle Keycloak (Multi-step) flow
        # This might be the initial page or a redirect after the legacy check
        for _ in range(2): # Double check for transitions
            if page.locator('input#username:visible').count() > 0:
                logger.info("Handling Keycloak Username screen...")
                page.locator('input#username').fill(cfg["USERNAME"])
                page.press('input#username', "Enter")
                time.sleep(3)
            
            if page.locator('input#password:visible').count() > 0:
                logger.info("Handling Keycloak Password screen...")
                page.locator('input#password').fill(cfg["PASSWORD"])
                page.press('input#password', "Enter")
                break
            
            time.sleep(2)
        
        time.sleep(5)  # Global wait for redirect/auth

    except Exception as e:
        logger.error(f"Login form interaction failed: {e}")
        try:
            if not page.is_closed():
                ERRORS_DIR.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(ERRORS_DIR / "login_form_error.png"))
        except Exception:
            pass
        raise

    # Final cookie check after landing
    try:
        if not page.is_closed():
            # Sometimes a cookie bot overlay persists
            page.evaluate("""() => {
                try {
                    const btn = Array.from(document.querySelectorAll('button')).find(b => b.innerText.includes('Usa solo') || b.innerText.includes('Accetta'));
                    if (btn) btn.click();
                } catch(e) {}
            }""")
    except Exception:
        pass

    # After login VCOM lands on the dashboard.
    # Click the "Valutazione" tab to reach the evaluation/analysis section.
    logger.info("Navigating to Valutazione section...")
    valutazione_selector = 'a[title="Valutazione"]'
    
    # 1. First try direct navigation as it's the most reliable
    try:
        if "valutazione" not in page.url.lower():
            logger.info(f"Direct navigation to {cfg['SYSTEM_URL']}...")
            page.goto(cfg["SYSTEM_URL"], timeout=45_000)
            page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception as e:
        logger.warning(f"Direct navigation attempt failed: {e}")

    # 2. If not there, try the selector (might be in a menu or sidebar)
    if "valutazione" not in page.url.lower():
        try:
            # Check if we need to open the sidebar first (Barra laterale)
            sidebar_toggle = page.locator('button:has-text("Barra laterale"), .sidebar-toggle, .menu-toggle').first
            if sidebar_toggle.count() > 0 and not page.locator(valutazione_selector).is_visible():
                logger.info("Opening sidebar to find Valutazione link...")
                sidebar_toggle.click()
                time.sleep(1)

            page.wait_for_selector(valutazione_selector, timeout=15_000)
            page.locator(valutazione_selector).first.click()
        except Exception as e:
            logger.error(f"Failed to find Valutazione link via selector: {e}")
            if not page.is_closed():
                page.screenshot(path=str(ERRORS_DIR / "navigation_error.png"))
            # If we are already on evaluation (URL check), don't raise
            if "valutazione" not in page.url.lower():
                raise

    # Confirm we're on the right page
    try:
        page.wait_for_selector('text="Inverter"', timeout=30_000)
        logger.info("Successfully reached the Evaluation dashboard.")
    except Exception as e:
        logger.error(f"Could not confirm evaluation dashboard: {e}")
        if not page.is_closed():
            page.screenshot(path=str(ERRORS_DIR / "login_success_navigation_error.png"))


# ---------------------------------------------------------------------------
# Popup dismissal
# ---------------------------------------------------------------------------

def dismiss_popup(page) -> None:
    """Dismiss any 'Valori minimi non disponibili' or other blocking Bootstrap Vue modals.
    
    Previous approach (clicking the 'Chiudi' button) fails because Playwright's
    force-click does not trigger Bootstrap Vue's internal event handlers.
    Instead, we remove the modal and its backdrop directly from the DOM.
    """
    try:
        removed = page.evaluate("""() => {
            let removed = 0;
            // 1. Remove all visible modal backdrops
            document.querySelectorAll('.modal-backdrop').forEach(el => {
                el.remove();
                removed++;
            });
            // 2. Hide and remove all open Bootstrap Vue modals
            document.querySelectorAll('.modal.show, .modal.fade.show').forEach(el => {
                el.classList.remove('show');
                el.style.display = 'none';
                el.setAttribute('aria-hidden', 'true');
                el.removeAttribute('aria-modal');
                removed++;
            });
            // 3. Also handle the outer wrapper divs (missing-minute-values-modal)
            document.querySelectorAll('[class*="missing-minute-values-modal"]').forEach(el => {
                el.style.display = 'none';
                removed++;
            });
            // 4. Clean up body classes that lock scrolling
            document.body.classList.remove('modal-open');
            document.body.style.removeProperty('padding-right');
            document.body.style.overflow = '';
            return removed;
        }""")
        if removed > 0:
            logger.info(f"Dismissed popup by removing {removed} modal element(s) from DOM.")
            time.sleep(0.5)
    except Exception as e:
        logger.debug(f"Popup dismissal failed: {e}")


# ---------------------------------------------------------------------------
# Inverter selection
# ---------------------------------------------------------------------------

def select_inverters(page) -> None:
    """Ensure only the 36 target inverters are selected, excluding SunGrow."""
    cfg = load_config()
    logger.info("Selecting target inverters...")
    try:
        target_ids = cfg.get("INVERTER_IDS", [])

        # Navigate to 'PR inverter' tab to configure selection (it must be a component tab)
        logger.info("Navigating to 'PR inverter' tab to configure selection...")
        pr_link = page.locator('text=/^\\s*PR inverter\\s*$/i').first
        pr_link.wait_for(state="visible", timeout=20_000)
        time.sleep(1) # Settle
        pr_link.click(force=True)
        time.sleep(2)
        dismiss_popup(page)

        # Toggle 'Valori in minuti' ON to expose the selection panel
        toggle_minute_values(page, "PR inverter")

        # Wait for selection panel innerHTML to be populated (indicates dynamic loading finished)
        page.wait_for_function("""() => {
            const el = document.querySelector('#chartComponentSelection');
            return el && el.innerHTML.trim() !== '';
        }""", timeout=30_000)
        dismiss_popup(page)

        # 1. Expand selection panel if it is collapsed (hidden)
        container = page.locator("#chartComponentSelection")
        # Ensure it is attached/present in the DOM first
        container.wait_for(state="attached", timeout=20_000)
        if not container.is_visible():
            logger.info("Inverter selection panel is collapsed. Expanding it...")
            toggle_btn = page.locator("a#headingComponentSelection")
            if toggle_btn.count() > 0:
                toggle_btn.click()
                time.sleep(1.5) # wait for expansion animation
        
        # Now wait for it to be visible
        container.wait_for(state="visible", timeout=10_000)
        
        # 2. Check if all 36 inverters are already selected (their spans have 'component-colorized' class)
        all_inverters_selected = True
        for inv_id in target_ids:
            span = page.locator(f"span#{inv_id}")
            if span.count() > 0:
                classes = span.get_attribute("class") or ""
                if "component-colorized" not in classes:
                    all_inverters_selected = False
                    break
            else:
                all_inverters_selected = False
                break

        # 3. Check if SunGrow is checked (should NOT be checked).
        # We check both the input state and the span's class to be absolutely sure.
        sungrow_checked = False
        sungrow_cb = page.locator("input#checkbox-Id27848313")
        if sungrow_cb.count() > 0:
            if sungrow_cb.is_checked():
                sungrow_checked = True

        sungrow_span = page.locator("span#Id27848313")
        if sungrow_span.count() > 0:
            classes = sungrow_span.get_attribute("class") or ""
            if "component-colorized" in classes:
                sungrow_checked = True

        if all_inverters_selected and not sungrow_checked:
            logger.info("All 36 target inverters are already selected and SunGrow is deselected. Skipping selection steps.")
            return

        logger.info("Selection mismatch or other user changed selection. Performing check/uncheck steps...")

        # Deselect all first for a clean slate
        btn_deselect = page.locator('button.selectNone:visible, button:has-text("Deseleziona tutto"):visible').first
        if btn_deselect.count() > 0 and btn_deselect.is_visible():
            btn_deselect.click()
            time.sleep(1)

        # Select target inverters
        for inv_id in target_ids:
            cb = page.locator(f"input#checkbox-{inv_id}")
            if cb.count() > 0:
                cb.check(force=True) # Use force=True as labels often intercept clicks or inputs are styled opacity: 0

        # Ensure SunGrow is NOT checked
        if sungrow_cb.count() > 0 and sungrow_cb.is_checked():
            sungrow_cb.uncheck(force=True)

        # Refresh chart after selection - Use a specific selector to avoid strict mode violation
        btn = page.locator('#chartComponentSelection button:has-text("Aggiorna grafico"), #chartComponentSelection button:has-text("Update chart")').first
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
        acceso_locator = page.locator('button[title="acceso"]:visible')
        if acceso_locator.count() == 0:
            logger.info(f"No 'Valori in minuti' toggle found for {metric_name}. Skipping.")
            logger.info(f"Out of toggle_minute_values for {metric_name}")
            return

        acceso_btn = acceso_locator.first
        cls = acceso_btn.get_attribute("class") or ""
        if "active" not in cls:
            logger.info(f"Toggling 'Valori in minuti' ON for {metric_name}...")
            print(f"Toggling 'Valori in minuti' ON for {metric_name}...")
            acceso_btn.click()
            dismiss_popup(page)  # handles Chiudi if it appears
            time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not toggle 'Valori in minuti' for {metric_name}: {e}")
    logger.info(f"Out of toggle_minute_values for {metric_name}")


def refresh_chart(page) -> None:
    """Click 'Aggiorna grafico' if visible."""
    try:
        btn = page.locator('button:has-text("Aggiorna grafico"), button:has-text("Update chart")')
        if btn.is_visible():
            btn.click()
            time.sleep(1.5)
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
            # Clear any blocking modals before attempting to click
            dismiss_popup(page)
            
            # VCOM evaluation pages can be long
            page.evaluate("window.scrollTo(0, 450)")
            time.sleep(0.5)  # brief settle after scroll
            
            # Re-query the locator each attempt to avoid stale references
            # Robust exact-match regex locator for Dati/Data tab
            tab = page.locator('text=/^\\s*(Dati|Data)\\s*$/i').last
            
            # Use wait_for first (doesn't require attached), then scroll
            tab.wait_for(state="visible", timeout=10_000)
            
            try:
                tab.scroll_into_view_if_needed()
            except Exception:
                # If scroll fails (detached DOM), wait and re-query
                time.sleep(0.7)
                tab = page.locator('text=/^\\s*(Dati|Data)\\s*$/i').last
                tab.wait_for(state="visible", timeout=6_000)
            
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
                url = ''
                try:
                    url = page.url.lower()
                except Exception:
                    pass

                if "valutazione" in url or "evaluation" in url or "index/index" in url:
                    logger.warning(
                        f"click_dati_tab attempt {attempt} failed ({type(e).__name__}). Retrying on same page..."
                    )
                    time.sleep(2)
                else:
                    logger.warning(
                        f"click_dati_tab attempt {attempt} failed ({type(e).__name__}). Reloading page..."
                    )
                    try:
                        page.reload()
                        page.wait_for_load_state("networkidle", timeout=10_000)
                    except Exception:
                        pass
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


