import logging
import time
import pandas as pd
from .base_monitor import toggle_minute_values, refresh_chart, click_dati_tab, parse_italian_number, dismiss_popup, extract_infotab_table_js

logger = logging.getLogger(__name__)


def extract_pr(page) -> pd.DataFrame:
    """Extract PR Inverter data from the VCOM Evaluation dashboard."""
    logger.info("--- Extracting PR Inverter Data ---")

    logger.info("Clicking 'PR inverter' tab...")
    page.locator('text=/^\\s*PR inverter\\s*$/i').first.click(force=True)
    time.sleep(2)
    dismiss_popup(page)

    logger.info("Toggling minute values for PR...")
    toggle_minute_values(page, "PR inverter")
    refresh_chart(page)
    
    logger.info("Switching to Dati tab for PR...")
    click_dati_tab(page, extra_wait=1)

    # Use the robust JS extraction for the metrics table
    logger.info("Extracting PR table via JS...")
    df = extract_infotab_table_js(page, "PR inverter")
    
    if df.empty:
        return df

    # In PR inverter tab, the first column is the Inverter Name, second is PR
    # Let's map it into a standardized format if possible, or just return.
    # Actually, PR tab usually looks like: [Inverter, PR, ...].
    # But for Mazara, we want a daily time-series if 'Valori in minuti' works.
    
    return df

