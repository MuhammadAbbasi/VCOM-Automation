import logging
import time
import pandas as pd
from .base_monitor import refresh_chart, click_dati_tab, extract_infotab_table_js, dismiss_popup

logger = logging.getLogger(__name__)


def extract_corrente_dc(page) -> pd.DataFrame:
    """Extract Corrente DC (string-level) data from the VCOM Evaluation dashboard.

    DC current has ~808 string columns — the chart takes much longer to render
    than other metrics, and 'Valori in minuti' is often unavailable for it.

    Strategy:
      - Skip the minute-values toggle (it triggers a blocking popup for DC)
      - Wait for networkidle after the chart loads before switching to Dati
      - Extra 2s wait after clicking Dati for the large table to finish rendering
      - Use JavaScript extraction to avoid per-cell OOM crashes
    """
    logger.info("--- Extracting Corrente DC Data ---")

    logger.info("Clicking 'Corrente DC' tab...")
    page.locator('text="Corrente DC"').first.click()
    time.sleep(3)
    dismiss_popup(page)

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
