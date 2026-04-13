import logging
import time
import pandas as pd
from .base_monitor import refresh_chart, click_dati_tab, extract_infotab_table_js, dismiss_popup, toggle_minute_values, DATA_DIR, today_str

logger = logging.getLogger(__name__)


def extract_corrente_dc(page) -> pd.DataFrame:
    """Extract Corrente DC (string-level) data from the VCOM Evaluation dashboard.

    DC current has ~808 string columns — the chart takes much longer to render
    than other metrics, and 'Valori in minuti' is often unavailable for it.

    Strategy:
      - Wait for networkidle after the chart loads before switching to Dati
      - Extra 2s wait after clicking Dati for the large table to finish rendering
      - Use JavaScript extraction to avoid per-cell OOM crashes
    """
    logger.info("--- Extracting Corrente DC Data ---")

    logger.info("Clicking 'Corrente DC' tab...")
    page.locator('text="Corrente DC"').first.click()
    time.sleep(3)
    dismiss_popup(page)
    
    toggle_minute_values(page, "Corrente DC")

    logger.info("Waiting for Corrente DC chart/network to settle...")
    # Wait for the chart to finish loading before doing anything else
    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        logger.warning("Corrente DC networkidle timeout (ignored)")

    refresh_chart(page)

    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    logger.info("Switching to Dati tab for Corrente DC (large table wait)...")
    click_dati_tab(page, extra_wait=2)
    dismiss_popup(page)   # popup can reappear after switching tabs

    logger.info("Extracting Corrente DC table via JS (high timeout)...")
    return extract_infotab_table_js(page, "Corrente DC", row_timeout=30_000)


def download_corrente_dc_csv(page) -> pd.DataFrame:
    """Fallback method: Downloads CSV from Highcharts context menu."""
    logger.info("--- FALLBACK: Downloading Corrente DC CSV ---")
    
    # Ensure tab is active
    page.locator('text="Corrente DC"').first.click()
    time.sleep(2)
    dismiss_popup(page)
    
    # Handle minute values
    toggle_minute_values(page, "Corrente DC")
    refresh_chart(page)
    time.sleep(5) # Wait for chart to render
    
    # Highcharts context menu button is usually at the top right of the svg container
    menu_btn = page.locator('.highcharts-contextbutton, .highcharts-button-symbol').first
    try:
        menu_btn.wait_for(state="visible", timeout=10_000)
        menu_btn.click()
        logger.info("Context menu opened.")
    except Exception as e:
        logger.error(f"Failed to open Highcharts context menu: {e}")
        return None
    
    # Look for the download option
    try:
        with page.expect_download(timeout=30_000) as download_info:
            # The text in the screenshot is "Scarica come file CSV"
            page.locator('text="Scarica come file CSV"').first.click()
        
        download = download_info.value
        
        # Save directly to our extracted_data directory
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        dest_path = str(DATA_DIR / f"Corrente_DC_{today_str()}_raw.csv")
        download.save_as(dest_path)
        logger.info(f"CSV saved to {dest_path}")
        
        # Load and process the CSV
        df_raw = pd.read_csv(dest_path)
        
        # Transform: wide format -> long format
        if df_raw.empty:
            return None
            
        time_col = df_raw.columns[0]
        # Rename the first column to 'Ora'
        df_raw.rename(columns={time_col: "Ora"}, inplace=True)
        
        # Ensure all other columns have the " [A]" suffix if missing
        new_cols = {}
        for col in df_raw.columns:
            if col != "Ora" and "MPPT" in col and " [A]" not in col:
                new_cols[col] = f"{col} [A]"
        
        if new_cols:
            df_raw.rename(columns=new_cols, inplace=True)
            
        return df_raw 
        
    except Exception as e:
        logger.error(f"Failed to download/parse Highcharts CSV: {e}")
        return None
