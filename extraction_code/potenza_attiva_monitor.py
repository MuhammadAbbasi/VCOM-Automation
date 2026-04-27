import logging
import time
import pandas as pd
from .base_monitor import toggle_minute_values, refresh_chart, click_dati_tab, extract_infotab_table_js, dismiss_popup

logger = logging.getLogger(__name__)

def extract_potenza_attiva(page) -> pd.DataFrame:
    """Extract 'Potenza attiva' (Power Control) limit from VCOM."""
    logger.info("Opening 'Power control' menu if needed...")
    try:
        # Check if Potenza attiva is already visible
        target = page.locator('text=/^\\s*Potenza attiva\\s*$/i').first
        if not target.is_visible(timeout=3000):
            logger.info("Clicking 'Power control' parent menu...")
            page.locator('text=/^\\s*Power control\\s*$/i').first.click(force=True)
            time.sleep(1)
        
        logger.info("Clicking 'Potenza attiva' sub-menu...")
        target.click(force=True)
        time.sleep(2)
        dismiss_popup(page)
    except Exception as e:
        logger.warning(f"Navigation to Potenza attiva failed: {e}")
        # Try a direct click if the above failed
        page.locator('text=/^\\s*Potenza attiva\\s*$/i').first.click(force=True)

    logger.info("Toggling minute values for Potenza attiva...")
    toggle_minute_values(page, "Potenza attiva")
    refresh_chart(page)

    logger.info("Switching to Dati tab for Potenza attiva...")
    click_dati_tab(page, extra_wait=1)

    logger.info("Extracting Potenza attiva table via JS...")
    # This table usually has one column for the limit (%) and one for the grid nominal power (kW)
    return extract_infotab_table_js(page, "Potenza attiva")
