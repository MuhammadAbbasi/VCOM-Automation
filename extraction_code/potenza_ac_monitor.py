import logging
import time
import pandas as pd
from .base_monitor import toggle_minute_values, refresh_chart, click_dati_tab, extract_infotab_table_js, dismiss_popup

logger = logging.getLogger(__name__)


def extract_potenza_ac(page) -> pd.DataFrame:
    """Extract Potenza AC data from the VCOM Evaluation dashboard."""
    logger.info("Clicking 'Potenza AC' tab...")
    page.locator('text=/^\\s*Potenza AC\\s*$/i').first.click(force=True)
    time.sleep(2)
    dismiss_popup(page)

    logger.info("Toggling minute values for Potenza AC...")
    toggle_minute_values(page, "Potenza AC")
    refresh_chart(page)

    logger.info("Switching to Dati tab for Potenza AC...")
    click_dati_tab(page, extra_wait=1)

    logger.info("Extracting Potenza AC table via JS...")
    return extract_infotab_table_js(page, "Potenza AC")
