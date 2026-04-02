"""
processor_watchdog_final.py — VCOM Forensic Analysis (Memory-Efficient)

Analyzes extracted data without massive merges:
- Potenza_AC: Master time series (23,040 rows per day)
- Other metrics: Loaded as-needed for rule evaluation
- No full merges - lookups and comparisons only

Generates JSON snapshots (health flags) + optional CSV audit trails.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# ---------------------------------------------------------------------------
# Paths & Logging
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "extracted_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LOG_PATH = ROOT / "watchdog.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WATCHDOG] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger("watchdog_final")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INVERTER_IDS = [
    "TX1-01", "TX1-02", "TX1-03", "TX1-04", "TX1-05", "TX1-06",
    "TX1-07", "TX1-08", "TX1-09", "TX1-10", "TX1-11", "TX1-12",
    "TX2-01", "TX2-02", "TX2-03", "TX2-04", "TX2-05", "TX2-06",
    "TX2-07", "TX2-08", "TX2-09", "TX2-10", "TX2-11", "TX2-12",
    "TX3-01", "TX3-02", "TX3-03", "TX3-04", "TX3-05", "TX3-06",
    "TX3-07", "TX3-08", "TX3-09", "TX3-10", "TX3-11", "TX3-12",
]

# Thresholds
PR_THRESHOLD = 85.0
TEMP_CRITICAL = 45.0
TEMP_WARNING = 40.0
AC_HEALTHY_MIN = 5000  # AC > 5kW = healthy during daylight
DAYLIGHT_START = 7.0
DAYLIGHT_END = 19.0


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def excel_to_csv(excel_path: Path, csv_path: Path) -> bool:
    """Convert Excel to CSV, return True if successful."""
    try:
        df = pd.read_excel(str(excel_path))
        df.to_csv(str(csv_path), index=False)
        logger.info(f"Converted {excel_path.name} -> {csv_path.name}")
        return True
    except Exception as e:
        logger.error(f"Failed to convert {excel_path.name}: {e}")
        return False


def load_metric(date_str: str, metric_prefix: str) -> pd.DataFrame:
    """Load metric from CSV or Excel."""
    csv_path = DATA_DIR / f"{metric_prefix}_{date_str}.csv"
    excel_path = DATA_DIR / f"{metric_prefix}_{date_str}.xlsx"

    # Try CSV first
    if csv_path.exists():
        try:
            return pd.read_csv(str(csv_path))
        except Exception:
            pass

    # Try Excel
    if excel_path.exists():
        try:
            if excel_to_csv(excel_path, csv_path):
                return pd.read_csv(str(csv_path))
        except Exception:
            pass

    logger.warning(f"{metric_prefix}_{date_str} not found")
    return None


def normalize_pr(val):
    """Convert PR to 0-100% scale."""
    if pd.isna(val):
        return None
    return val if val > 1.5 else val * 100


# ---------------------------------------------------------------------------
# Health Computation (from latest data)
# ---------------------------------------------------------------------------

def compute_latest_health(date_str: str, ac_df: pd.DataFrame, temp_df: pd.DataFrame,
                         dc_df: pd.DataFrame, pr_df: pd.DataFrame) -> dict:
    """
    Compute health flags from the latest available NON-NAN values in each metric file.
    """
    inverter_health = {}

    # Get latest PR values
    pr_latest = {}
    if pr_df is not None:
        pr_df_clean = pr_df.copy()
        pr_df_clean["PR"] = pr_df_clean["PR"].apply(normalize_pr)
        for inv_id in INVERTER_IDS:
            rows = pr_df_clean[pr_df_clean["Inverter"] == f"INV {inv_id}"]
            if len(rows) > 0:
                pr_latest[inv_id] = rows.iloc[-1]["PR"]

    # Find latest AC row with valid data (not all NaN)
    ac_row = None
    ora = 0
    if ac_df is not None and len(ac_df) > 0:
        # Find last row that has at least some non-NaN AC values
        for idx in range(len(ac_df) - 1, -1, -1):
            row = ac_df.iloc[idx]
            ac_cols = [c for c in ac_df.columns if "Potenza AC" in c]
            ac_values = [row.get(c) for c in ac_cols]
            non_nan_count = sum(1 for v in ac_values if v is not None and not pd.isna(v))
            if non_nan_count > 30:  # At least 30 inverters have valid data
                ac_row = row
                ora = row.get("Ora", 0)
                logger.info(f"Found latest valid AC row at index {idx} (Ora={ora}, {non_nan_count} valid values)")
                break

        if ac_row is None:
            # Fallback to last row even if NaN
            ac_row = ac_df.iloc[-1]
            ora = ac_row.get("Ora", 0)

    # Get latest temp and DC rows with valid data
    temp_row = None
    if temp_df is not None and len(temp_df) > 0:
        for idx in range(len(temp_df) - 1, -1, -1):
            row = temp_df.iloc[idx]
            temp_cols = [c for c in temp_df.columns if "Temperatura" in c]
            temp_values = [row.get(c) for c in temp_cols]
            non_nan_count = sum(1 for v in temp_values if v is not None and not pd.isna(v))
            if non_nan_count > 30:
                temp_row = row
                break
        if temp_row is None and len(temp_df) > 0:
            temp_row = temp_df.iloc[-1]

    dc_row = None
    if dc_df is not None and len(dc_df) > 0:
        for idx in range(len(dc_df) - 1, -1, -1):
            row = dc_df.iloc[idx]
            dc_cols = [c for c in dc_df.columns if "Corrente DC" in c]
            dc_values = [row.get(c) for c in dc_cols]
            non_nan_count = sum(1 for v in dc_values if v is not None and not pd.isna(v))
            if non_nan_count > 400:  # At least 400 MPPT channels have valid data
                dc_row = row
                break
        if dc_row is None and len(dc_df) > 0:
            dc_row = dc_df.iloc[-1]

    # Compute health for each inverter
    for inv_id in INVERTER_IDS:
        inv_label = f"INV {inv_id}"
        health = {}

        # PR LED
        pr_val = pr_latest.get(inv_id)
        if pr_val is None:
            health["pr"] = "grey"
        elif pr_val >= PR_THRESHOLD:
            health["pr"] = "green"
        elif pr_val >= (PR_THRESHOLD - 10):
            health["pr"] = "yellow"
        else:
            health["pr"] = "red"

        # Temperature LED
        temp_col = f"Temperatura inverter (INV {inv_id}) [°C]"
        temp_val = None
        if temp_row is not None and temp_col in temp_row.index:
            temp_val = temp_row[temp_col]

        if temp_val is None or pd.isna(temp_val):
            health["temp"] = "grey"
        elif temp_val > TEMP_CRITICAL:
            health["temp"] = "red"
        elif temp_val > TEMP_WARNING:
            health["temp"] = "yellow"
        else:
            health["temp"] = "green"

        # DC Current LED (average of all MPPTs for this inverter)
        # Thresholds adjusted for real-world conditions (late afternoon DC ~1-2A/MPPT is normal)
        dc_values = []
        if dc_row is not None:
            for mppt in range(1, 13):  # 12 MPPTs per inverter
                dc_col = f"Corrente DC MPPT {mppt} (INV {inv_id}) [A]"
                if dc_col in dc_row.index:
                    val = dc_row[dc_col]
                    if val is not None and not pd.isna(val):
                        dc_values.append(val)

        if dc_values:
            avg_dc = np.mean(dc_values)
            # Contextualize to time of day: early morning/evening < afternoon
            if DAYLIGHT_START <= ora <= 12:  # Morning ramp-up
                dc_threshold_green = 10  # Morning: expect higher current
                dc_threshold_yellow = 2
            elif 12 < ora <= DAYLIGHT_END:  # Afternoon decline
                dc_threshold_green = 5  # Afternoon: lower thresholds
                dc_threshold_yellow = 0.5
            else:  # Off-hours
                dc_threshold_green = 0.1
                dc_threshold_yellow = 0

            if avg_dc >= dc_threshold_green:
                health["dc_current"] = "green"
            elif avg_dc >= dc_threshold_yellow:
                health["dc_current"] = "yellow"
            elif avg_dc > 0:
                health["dc_current"] = "red"
            else:
                health["dc_current"] = "grey"
        else:
            health["dc_current"] = "grey"

        # AC Power LED
        ac_col = f"Potenza AC (INV {inv_id}) [W]"
        ac_val = None
        if ac_row is not None and ac_col in ac_row.index:
            ac_val = ac_row[ac_col]

        if ac_val is None or pd.isna(ac_val):
            health["ac_power"] = "grey"
        elif ac_val > AC_HEALTHY_MIN:
            health["ac_power"] = "green"
        elif ac_val > 1000:
            health["ac_power"] = "yellow"
        elif ac_val > 0:
            health["ac_power"] = "red"
        else:
            # 0 W - depends on time of day
            if DAYLIGHT_START <= ora <= DAYLIGHT_END:
                health["ac_power"] = "red"  # Should be generating
            else:
                health["ac_power"] = "grey"  # Off-hours is OK

        # Overall = worst of 4 LEDs
        scores = []
        score_map = {"green": 0, "yellow": 1, "red": 2, "grey": -1}
        for k in ["pr", "temp", "dc_current", "ac_power"]:
            score = score_map.get(health[k], -1)
            if score >= 0:
                scores.append(score)

        if not scores:
            health["overall_status"] = "grey"
        else:
            worst = max(scores)
            health["overall_status"] = ["green", "yellow", "red"][worst]

        inverter_health[inv_label] = health

    return inverter_health


# ---------------------------------------------------------------------------
# Macro Health
# ---------------------------------------------------------------------------

def compute_macro_health(inverter_health: dict) -> dict:
    """Compute plant-wide health summary."""
    total = len(inverter_health)
    online = sum(1 for h in inverter_health.values() if h["ac_power"] in ["green", "yellow"])
    tripped = sum(1 for h in inverter_health.values() if h["ac_power"] == "red")
    comms_lost = sum(1 for h in inverter_health.values() if h["ac_power"] == "grey")

    return {
        "total_inverters": total,
        "online": online,
        "tripped": tripped,
        "comms_lost": comms_lost,
        "last_sync": datetime.now().isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# Main Analysis
# ---------------------------------------------------------------------------

def analyze_site(date_str: str) -> None:
    """Main analysis: load data, compute health, write JSON."""
    try:
        logger.info(f"Starting analysis for {date_str}...")

        # Load metrics
        logger.info("Loading metrics...")
        ac_df = load_metric(date_str, "Potenza_AC")
        pr_df = load_metric(date_str, "PR")
        temp_df = load_metric(date_str, "Temperatura")
        dc_df = load_metric(date_str, "Corrente_DC")
        resist_df = load_metric(date_str, "Resistenza_Isolamento")
        irrad_df = load_metric(date_str, "Irraggiamento")

        if ac_df is None:
            logger.warning(f"Potenza_AC not found for {date_str}")
            return

        # Compute health from latest values
        logger.info("Computing health flags...")
        inverter_health = compute_latest_health(date_str, ac_df, temp_df, dc_df, pr_df)
        macro_health = compute_macro_health(inverter_health)

        # Build JSON snapshot
        timestamp = datetime.now().isoformat(timespec="seconds")
        snapshot = {
            timestamp: {
                "macro_health": macro_health,
                "inverter_health": inverter_health,
                "active_anomalies": [],  # Placeholder for future anomaly detection
            }
        }

        # Write JSON
        json_path = DATA_DIR / f"dashboard_data_{date_str}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
        logger.info(f"Wrote JSON: {json_path}")

        # Log summary
        logger.info(f"Health: {macro_health['online']} online, {macro_health['tripped']} tripped, "
                   f"{macro_health['comms_lost']} comms_lost")

    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# File Watcher
# ---------------------------------------------------------------------------

class MetricFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".xlsx"):
            return
        time.sleep(2)
        self._check_and_analyze()

    def on_modified(self, event):
        if event.is_directory or not event.src_path.endswith(".xlsx"):
            return
        self._check_and_analyze()

    def _check_and_analyze(self):
        today = datetime.now().strftime("%Y-%m-%d")
        required = [
            f"PR_{today}.xlsx",
            f"Potenza_AC_{today}.xlsx",
            f"Corrente_DC_{today}.xlsx",
            f"Resistenza_Isolamento_{today}.xlsx",
            f"Temperatura_{today}.xlsx",
            f"Irraggiamento_{today}.xlsx",
        ]

        present = [f for f in required if (DATA_DIR / f).exists()]
        if len(present) == len(required):
            logger.info(f"Complete set for {today}. Analyzing...")
            analyze_site(today)


def main():
    logger.info("Starting VCOM Watchdog (Final)...")
    logger.info(f"Monitoring: {DATA_DIR}")

    handler = MetricFileHandler()
    observer = Observer()
    observer.schedule(handler, str(DATA_DIR), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()
