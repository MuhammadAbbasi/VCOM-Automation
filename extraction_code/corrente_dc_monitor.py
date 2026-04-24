import logging
import time
import shutil
from pathlib import Path
import pandas as pd
from .base_monitor import (
    refresh_chart, click_dati_tab, extract_infotab_table_js,
    dismiss_popup, toggle_minute_values, DATA_DIR, ERRORS_DIR,
    get_timestamp_fetch, today_str, parse_italian_number
)

logger = logging.getLogger(__name__)


def _wait_for_chart(page, label: str) -> None:
    """Wait for the Corrente DC chart to fully render after tab click."""
    logger.info(f"Waiting for {label} chart to render (networkidle)...")
    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        logger.warning(f"{label} networkidle timeout (ignored)")
    time.sleep(4)


def _ensure_minute_values_on(page) -> None:
    """Make sure 'Valori in minuti' toggle is active (blue/acceso). If not, click it."""
    try:
        acceso = page.locator('button[title="acceso"]:visible').first
        if acceso.is_visible(timeout=5_000):
            cls = acceso.get_attribute("class") or ""
            if "active" not in cls:
                logger.info("Enabling 'Valori in minuti'...")
                acceso.click()
                dismiss_popup(page)
                time.sleep(3)
                try:
                    page.wait_for_load_state("networkidle", timeout=20_000)
                except Exception:
                    pass
                time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not ensure Valori in minuti is on: {e}")


def _download_chart_csv(page) -> pd.DataFrame | None:
    """
    Fallback: download the Corrente DC chart as CSV via the Highcharts
    hamburger menu (≡ → Scarica come file CSV).

    The SVG export button is a <g> element with title 'Menù contestuale diagramma'.
    Playwright's download API captures the file before the OS dialog appears.

    Returns a parsed DataFrame with the same structure as the table extraction,
    or None on failure.
    """
    logger.info("[DC Fallback] Attempting CSV download via Highcharts export menu...")

    try:
        # Find the export button by its SVG title
        export_btn = page.locator('g:has(> title):has-text("diagramma")').last
        if not export_btn.is_visible(timeout=5_000):
            logger.warning("[DC Fallback] Export button not found by text, trying JS click...")
            # Fallback: click the button by pixel coordinates found via JS
            coords = page.evaluate("""() => {
                for (const g of document.querySelectorAll('g')) {
                    const t = g.querySelector(':scope > title');
                    if (t && t.textContent.includes('diagramma')) {
                        const r = g.getBoundingClientRect();
                        return {x: r.x + r.width/2, y: r.y + r.height/2};
                    }
                }
                return null;
            }""")
            if not coords:
                logger.error("[DC Fallback] Could not locate Highcharts export button at all.")
                return None
            page.mouse.click(coords["x"], coords["y"])
        else:
            export_btn.click()

        time.sleep(1.5)  # Wait for dropdown to appear

        # Take screenshot to verify menu appeared
        ERRORS_DIR.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(ERRORS_DIR / "dc_fallback_menu_open.png"))

        # Click "Scarica come file CSV"
        csv_item = page.locator('text="Scarica come file CSV"').first
        csv_item.wait_for(state="visible", timeout=5_000)

        # Capture download
        with page.expect_download(timeout=30_000) as dl_info:
            csv_item.click()

        download = dl_info.value
        download_path = download.path()

        if not download_path:
            logger.error("[DC Fallback] Download path is None after download.")
            return None

        logger.info(f"[DC Fallback] Downloaded to temp: {download_path}")

        # Parse the downloaded CSV
        df = _parse_highcharts_csv(download_path)
        return df

    except Exception as e:
        logger.error(f"[DC Fallback] CSV download failed: {e}")
        try:
            page.screenshot(path=str(ERRORS_DIR / "dc_fallback_error.png"))
        except Exception:
            pass
        return None


def _parse_highcharts_csv(csv_path: str) -> pd.DataFrame:
    """
    Parse the Highcharts-exported CSV into a DataFrame matching the table
    extraction format. Highcharts CSVs use semicolons as separators by default
    and may use Italian number formatting (comma as decimal).

    NOTE: Highcharts often exports one series per time-row, resulting in a very
    sparse CSV (80k+ rows) with mostly NaNs. We densify it by grouping by time.
    """
    import io

    try:
        raw = Path(csv_path).read_text(encoding="utf-8-sig", errors="replace")
    except Exception:
        raw = Path(csv_path).read_text(encoding="latin-1", errors="replace")

    # Detect separator: Highcharts uses ";" but may also use ","
    lines = [l for l in raw.split("\n") if l.strip()]
    if not lines:
        return pd.DataFrame()
    
    first_line = lines[0]
    sep = ";" if ";" in first_line else ","

    df = pd.read_csv(io.StringIO(raw), sep=sep, header=0)
    if df.empty:
        return df

    # 1. Rename the first column to 'Ora' (X-axis/Time) to match table scraper
    time_col = df.columns[0]
    df.rename(columns={time_col: "Ora"}, inplace=True)

    # 2. Convert all numeric columns from Italian formatting
    # Skip the 'Ora' column if it contains non-numeric chars (like HH:MM)
    for col in df.columns:
        if col == "Ora":
            continue
        # Apply Italian number parsing to strings, but keep existing numbers
        df[col] = df[col].apply(lambda v: parse_italian_number(str(v)) if pd.notna(v) and not isinstance(v, (int, float)) else v)

    # 3. Densify: Highcharts CSV often has many rows for the same timestamp
    # because series are slightly unaligned or it exports each point as a new row.
    logger.info(f"[DC Fallback] Densifying sparse CSV ({len(df)} rows)...")
    
    # Round Ora to handle floating point jitter (3 decimals = ~3.6 seconds precision)
    df["Ora"] = df["Ora"].round(3)
    
    # We group by Ora and take the first non-null value for each MPPT
    df_dense = df.groupby("Ora", as_index=False).first()
    
    # Final cleanup: ensure we have a reasonable number of rows (e.g. 1440 for 24h/1min)
    logger.info(f"[DC Fallback] Extraction result: {len(df_dense)} rows, {len(df_dense.columns)} columns")
    return df_dense


def extract_corrente_dc(page) -> pd.DataFrame:
    """Extract Corrente DC (string-level) data from the VCOM Evaluation dashboard.

    Strategy:
      1. Click 'Corrente DC' tab and wait for chart to render.
      2. Ensure 'Valori in minuti' is ON.
      3. Attempt standard JS table extraction (up to 2 tries).
      4. If both tries fail or return empty, fall back to downloading
         the chart as CSV via the Highcharts hamburger export menu.
    """
    logger.info("--- Extracting Corrente DC Data ---")

    # ── Step 1: Navigate to tab ──────────────────────────────────────────────
    logger.info("Clicking 'Corrente DC' tab...")
    page.locator('text=/^\\s*Corrente DC\\s*$/i').first.click(force=True)
    time.sleep(3)
    dismiss_popup(page)

    # ── Step 2: Wait for chart + ensure minute values ────────────────────────
    _wait_for_chart(page, "Corrente DC")
    _ensure_minute_values_on(page)

    refresh_chart(page)
    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass
    time.sleep(3)

    # ── Step 3: Try standard table extraction (up to 2 attempts) ────────────
    for attempt in range(1, 3):
        logger.info(f"DC table extraction attempt {attempt}/2...")
        try:
            click_dati_tab(page, extra_wait=5)
            dismiss_popup(page)
            df = extract_infotab_table_js(page, "Corrente DC", row_timeout=30_000)

            if df is not None and not df.empty:
                logger.info(f"[OK] DC table extraction succeeded on attempt {attempt}.")
                return df
            else:
                logger.warning(f"DC table extraction attempt {attempt} returned empty DataFrame.")
        except Exception as e:
            logger.warning(f"DC table extraction attempt {attempt} raised error: {e}")

        if attempt < 2:
            # Go back to the chart view for the second attempt
            logger.info("Clicking Corrente DC tab again for retry...")
            page.locator('text=/^\\s*Corrente DC\\s*$/i').first.click(force=True)
            time.sleep(2)
            dismiss_popup(page)
            _wait_for_chart(page, "Corrente DC retry")
            _ensure_minute_values_on(page)

    # ── Step 4: CSV download fallback ────────────────────────────────────────
    logger.warning("[DC] Both table extraction attempts failed. Switching to CSV download fallback...")

    # Go back to chart view before downloading
    page.locator('text=/^\\s*Corrente DC\\s*$/i').first.click(force=True)
    time.sleep(2)
    dismiss_popup(page)
    _wait_for_chart(page, "Corrente DC (fallback)")
    _ensure_minute_values_on(page)

    df = _download_chart_csv(page)

    if df is not None and not df.empty:
        logger.info("[DC Fallback] CSV download extraction successful.")
        return df

    logger.error("[DC] All extraction methods failed. Returning empty DataFrame.")
    return pd.DataFrame()
