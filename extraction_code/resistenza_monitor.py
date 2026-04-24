import logging
import time
import pandas as pd
from .base_monitor import toggle_minute_values, refresh_chart, click_dati_tab, extract_infotab_table_js, dismiss_popup

logger = logging.getLogger(__name__)


def extract_resistenza(page) -> pd.DataFrame:
    """Extract Resistenza di isolamento data from the VCOM Evaluation dashboard."""
    logger.info("--- Extracting Resistenza di isolamento Data ---")
    logger.info("Clicking 'Resistenza di isolamento' tab...")
    page.locator('text=/^\\s*Resistenza di isolamento\\s*$/i').first.click(force=True)
    time.sleep(2)
    dismiss_popup(page)

    logger.info("Toggling minute values for Resistenza di isolamento...")
    toggle_minute_values(page, "Resistenza di isolamento")
    refresh_chart(page)

    logger.info("Switching to Dati tab for Resistenza di isolamento...")
    click_dati_tab(page, extra_wait=1)

    logger.info("Extracting Resistenza di isolamento table via JS...")
    return extract_infotab_table_js(page, "Resistenza di isolamento")
