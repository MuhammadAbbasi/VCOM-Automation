"""
backfill_extraction.py — Semi-automated historical data backfill from VCOM.

Reuses the proven login/inverter-selection/extraction code from
extraction_code/*.py (the same functions the live vcom_monitor.py uses),
but targets a specific PAST date instead of "today", and writes to that
date in the database instead of today's date.

The one thing this script does NOT automate: selecting the historical
date in VCOM's own date picker. Nobody has scripted that UI yet, and
guessing at its selectors against the live production account risked
either getting it wrong or — worse — colliding with the currently
running extraction session's server-side UI state (inverter selection,
minute-value toggles) the way two concurrent sessions did before.
So: the script logs in, selects the 36 inverters, then PAUSES with the
browser visible — you navigate to the target date by hand in VCOM's own
picker, confirm in the terminal, and the script takes over again for
the actual per-metric scraping and DB writes.

Runs with a SEPARATE Playwright profile (playwright_profile_backfill)
so it cannot conflict with the live extraction's persistent session.

Checked whether the per-metric URLs already captured in
db/attribute_links.json could skip the date-picker step entirely (e.g.
a date query param) — they don't: the `key=XXXXX` in each URL is a
stable per-metric chart identifier (Potenza AC is always key=5EJH8,
PR inverter always key=C6DI3, etc.), constant across every capture
regardless of what date is on screen. The date is pure client-side UI
state, not reflected in the URL, so there's no way around the manual
step below.

Best run around midnight: the plant is off then, so the live extraction
cycle's own concurrent data collection is flat/low-variance at that
time too — the lowest-risk window for two sessions to be open at once,
on the off chance VCOM's server-side UI state (inverter selection,
minute-value toggles) isn't purely per-session.

Usage:
    python backfill_extraction.py --date 2026-06-02
    python backfill_extraction.py --date 2026-06-02 --metrics "Potenza AC,Temperatura"
"""

import argparse
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from extraction_code.base_monitor import login, select_inverters
from extraction_code.pr_monitor import extract_pr
from extraction_code.potenza_ac_monitor import extract_potenza_ac
from extraction_code.corrente_dc_monitor import extract_corrente_dc
from extraction_code.resistenza_monitor import extract_resistenza
from extraction_code.temperatura_monitor import extract_temperatura
from extraction_code.irraggiamento_monitor import extract_irraggiamento
from extraction_code.potenza_attiva_monitor import extract_potenza_attiva
from db.db_manager import save_metric, save_extraction_status

ROOT = Path(__file__).resolve().parent

METRICS = [
    ("PR inverter", extract_pr),
    ("Potenza AC", extract_potenza_ac),
    ("Corrente DC", extract_corrente_dc),
    ("Resistenza di isolamento", extract_resistenza),
    ("Temperatura", extract_temperatura),
    ("Irraggiamento", extract_irraggiamento),
    ("Potenza attiva", extract_potenza_attiva),
]

KEY_NAME_MAP = {
    "PR inverter": "PR",
    "Resistenza di isolamento": "Resistenza_Isolamento",
    "Potenza attiva": "Potenza_Attiva",
}


def save_for_date(df, prefix: str, target_date: str) -> None:
    """Same logic as extraction_code.base_monitor.export_metric(), but for
    an explicit historical date instead of today. Deliberately not reusing
    export_metric() itself since it hardcodes today's date."""
    if df is None or df.empty:
        print(f"  [SKIP] {prefix}: empty DataFrame.")
        return
    save_metric(df, prefix, target_date)
    key_name = KEY_NAME_MAP.get(prefix, prefix.replace(" ", "_"))
    save_extraction_status(target_date, key_name, "success")
    print(f"  [OK] {prefix}: saved {len(df)} rows -> {target_date}")


def main():
    parser = argparse.ArgumentParser(description="Backfill a historical date from VCOM.")
    parser.add_argument("--date", required=True, help="Target date, YYYY-MM-DD")
    parser.add_argument("--metrics", default=None, help="Comma-separated subset of metric names (default: all)")
    args = parser.parse_args()

    target_date = args.date
    wanted = {m.strip() for m in args.metrics.split(",")} if args.metrics else None
    metrics_to_run = [(n, f) for n, f in METRICS if not wanted or n in wanted]
    if not metrics_to_run:
        print(f"No matching metrics for --metrics {args.metrics!r}. Available: {[n for n, _ in METRICS]}")
        sys.exit(1)

    print(f"\n{'='*60}\nBACKFILL: {target_date}\nMetrics: {[n for n, _ in metrics_to_run]}\n{'='*60}\n")

    with sync_playwright() as p:
        user_data_dir = ROOT / "playwright_profile_backfill"
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,
            viewport={"width": 1450, "height": 900},
        )
        page = context.pages[0] if context.pages else context.new_page()

        print("[*] Logging in...")
        login(page)

        print("[*] Selecting the 36 target inverters...")
        select_inverters(page)

        print(f"\n{'='*60}")
        print(f"MANUAL STEP: in the browser window, use VCOM's date picker")
        print(f"to navigate to {target_date}.")
        print(f"{'='*60}")
        input("Press Enter here once that date is showing on screen... ")

        for name, extractor in metrics_to_run:
            print(f"\n[*] Extracting: {name}")
            try:
                df = extractor(page)
                save_for_date(df, name, target_date)
            except Exception as e:
                print(f"  [FAIL] {name}: {e}")
            time.sleep(1)

        print(f"\n{'='*60}\nBackfill for {target_date} complete. Browser stays open —\nclose it manually, or Ctrl+C here.\n{'='*60}")
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass
        context.close()


if __name__ == "__main__":
    main()
