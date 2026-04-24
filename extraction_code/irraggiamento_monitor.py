import logging
import time
import pandas as pd
from .base_monitor import toggle_minute_values, refresh_chart, click_dati_tab, parse_italian_number, dismiss_popup, extract_infotab_table_js

logger = logging.getLogger(__name__)

IRR_SENSORS = [
    "JB-SM1_AL-1-DOWN", "JB-SM1_AL-1-UP", "JB-SM3_AL-3-DOWN", "JB-SM3_AL-3-UP",
    "JB-SM3_GHI-3", "JB1_GHI-1", "JB1_IT-1-1", "JB1_IT-1-2", "JB1_POA-1",
    "JB2_IT-2-1", "JB2_IT-2-2", "JB3_IT-3-1", "JB3_IT-3-2", "JB3_POA-3",
]


def extract_irraggiamento(page) -> pd.DataFrame:
    """Extract Irraggiamento data for the 14 named environmental sensors."""
    logger.info("--- Extracting Irraggiamento Data ---")

    logger.info("Clicking 'Irraggiamento' tab...")
    page.locator('text=/^\\s*Irraggiamento\\s*$/i').first.click(force=True)
    time.sleep(2)
    dismiss_popup(page)

    logger.info("Toggling minute values for Irraggiamento...")
    toggle_minute_values(page, "Irraggiamento")
    refresh_chart(page)

    logger.info("Switching to Dati tab for Irraggiamento...")
    dismiss_popup(page)
    click_dati_tab(page, extra_wait=1)

    logger.info("Extracting Irraggiamento table via JS...")
    df = extract_infotab_table_js(page, "Irraggiamento")

    if df.empty:
        return df

    # Keep only Ora + the 14 target sensor columns
    valid_cols = [c for c in df.columns if c == "Ora" or any(s in c for s in IRR_SENSORS)]
    
    return df[valid_cols] if valid_cols else df
