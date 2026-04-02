"""
processor_watchdog_v3.py — VCOM Forensic Analysis (Fixed Data Merging)

Properly handles actual data structures:
- PR: Long format (one metric per inverter per extraction)
- Time series (Potenza_AC, Temperatura, etc): Wide format indexed by Ora (hours decimal)
- Corrente_DC: Ultra-wide (12 MPPTs per inverter)

Merges on Ora (time of day in decimal hours), applies forensic rules,
computes health flags, outputs JSON snapshots + CSV audit trails.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
from watchdog.events import FileModifiedEvent, FileSystemEventHandler
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
logger = logging.getLogger("watchdog_v3")

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

# Rule thresholds
PR_THRESHOLD = 85.0  # If PR is 0-100 scale, <85 is low
TEMP_CRITICAL = 45.0  # >45°C is critical
TEMP_WARNING = 40.0   # >40°C is warning
DC_FAILURE_THRESHOLD = 0.2  # <0.2A is string failure
AC_DEVIATION_PERCENT = 0.03  # >3% deviation
SITE_MIN_POWER = 5000  # Only eval deviations when site >5kW

DAYLIGHT_START = 7.0  # 07:00
DAYLIGHT_END = 19.0   # 19:00

DEDUP_WINDOW = 3600  # 1 hour


# ---------------------------------------------------------------------------
# Data Loading & Conversion
# ---------------------------------------------------------------------------

def excel_to_csv(excel_path: Path, csv_path: Path) -> None:
    """Convert Excel file to CSV for faster processing."""
    try:
        df = pd.read_excel(str(excel_path))
        df.to_csv(str(csv_path), index=False)
        logger.info(f"Converted {excel_path.name} -> {csv_path.name}")
    except Exception as e:
        logger.error(f"Failed to convert {excel_path.name}: {e}")
        raise


def load_metric_csv(date_str: str, metric_prefix: str) -> pd.DataFrame:
    """Load metric data from CSV (or Excel if CSV doesn't exist)."""
    csv_path = DATA_DIR / f"{metric_prefix}_{date_str}.csv"
    excel_path = DATA_DIR / f"{metric_prefix}_{date_str}.xlsx"

    if csv_path.exists():
        return pd.read_csv(str(csv_path))
    elif excel_path.exists():
        excel_to_csv(excel_path, csv_path)
        return pd.read_csv(str(csv_path))
    else:
        logger.warning(f"No file found for {metric_prefix}_{date_str}")
        return None


# ---------------------------------------------------------------------------
# Data Cleaning
# ---------------------------------------------------------------------------

def normalize_pr(pr_val):
    """Normalize PR to 0-100% scale. Handle both 0-1 and 0-100 formats."""
    if pd.isna(pr_val):
        return None
    if pr_val > 1.5:  # Assume 0-100 scale
        return pr_val
    else:  # Assume 0-1 scale
        return pr_val * 100


def clean_and_merge_timeseries(date_str: str, potenza_ac: pd.DataFrame,
                               temp_df: pd.DataFrame, dc_df: pd.DataFrame,
                               resist_df: pd.DataFrame, irrad_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge all time-series data on Ora (time of day in decimal hours).
    Returns single merged DataFrame with all metrics aligned by Ora.
    """
    # Use Potenza_AC as master (most complete time series)
    merged = potenza_ac.copy()

    # Ensure Ora is numeric
    merged["Ora"] = pd.to_numeric(merged["Ora"], errors="coerce")

    # Merge Temperatura (by Ora)
    if temp_df is not None:
        temp_df["Ora"] = pd.to_numeric(temp_df["Ora"], errors="coerce")
        temp_cols = {col: col for col in temp_df.columns if "Temperatura" in col}
        temp_merge = temp_df[list(temp_cols.keys()) + ["Ora"]].copy()
        merged = merged.merge(temp_merge, on="Ora", how="left", suffixes=("", "_temp"))

    # Merge Resistenza (by Ora)
    if resist_df is not None:
        resist_df["Ora"] = pd.to_numeric(resist_df["Ora"], errors="coerce")
        resist_cols = {col: col for col in resist_df.columns if "Resistenza" in col}
        resist_merge = resist_df[list(resist_cols.keys()) + ["Ora"]].copy()
        merged = merged.merge(resist_merge, on="Ora", how="left", suffixes=("", "_resist"))

    # Merge DC Current (by Ora) - keep all MPPT columns
    if dc_df is not None:
        dc_df["Ora"] = pd.to_numeric(dc_df["Ora"], errors="coerce")
        dc_cols = [col for col in dc_df.columns if "Corrente DC" in col]
        dc_merge = dc_df[dc_cols + ["Ora"]].copy()
        merged = merged.merge(dc_merge, on="Ora", how="left", suffixes=("", "_dc"))

    # Merge Irradiance (by Ora)
    if irrad_df is not None:
        irrad_df["Ora"] = pd.to_numeric(irrad_df["Ora"], errors="coerce")
        irrad_cols = [col for col in irrad_df.columns if "Irraggiamento" in col]
        irrad_merge = irrad_df[irrad_cols + ["Ora"]].copy()
        merged = merged.merge(irrad_merge, on="Ora", how="left", suffixes=("", "_irrad"))

    # Convert numeric columns
    for col in merged.columns:
        if col not in ["Timestamp Fetch", "Ora"]:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

    return merged


# ---------------------------------------------------------------------------
# Forensic Rules
# ---------------------------------------------------------------------------

def apply_forensic_rules(date_str: str, merged_df: pd.DataFrame, pr_df: pd.DataFrame) -> list:
    """
    Apply 6 forensic rules on merged time-series data.
    PR values come from the separate PR dataframe (latest values per inverter).
    """
    anomalies = []

    # Get latest PR values per inverter
    pr_latest = {}
    if pr_df is not None:
        for inv_id in INVERTER_IDS:
            inv_rows = pr_df[pr_df["Inverter"] == f"INV {inv_id}"]
            if len(inv_rows) > 0:
                pr_val = inv_rows.iloc[-1]["PR"]
                pr_latest[inv_id] = normalize_pr(pr_val)

    # Process each row (time slice)
    for idx, row in merged_df.iterrows():
        ora = row["Ora"]
        timestamp = row["Timestamp Fetch"]

        if pd.isna(timestamp):
            timestamp = f"{int(ora):02d}:{int((ora % 1) * 60):02d}"

        # Get AC power values for all inverters
        ac_cols = [c for c in merged_df.columns if "Potenza AC" in c and "[W]" in c]
        ac_values = {col: row.get(col) for col in ac_cols}

        # Calculate site median AC power
        ac_numeric = [v for v in ac_values.values() if v is not None and not pd.isna(v)]
        site_ac_median = np.median(ac_numeric) if ac_numeric else 0

        # Process each inverter
        for inv_id in INVERTER_IDS:
            ac_col = f"Potenza AC (INV {inv_id}) [W]"
            temp_col = f"Temperatura inverter (INV {inv_id}) [°C]"

            ac_power = row.get(ac_col)
            if ac_power is not None:
                ac_power = float(ac_power)

            temp_val = row.get(temp_col)
            if temp_val is not None:
                temp_val = float(temp_val)

            # Rule 1: Low PR (during daylight)
            if DAYLIGHT_START <= ora <= DAYLIGHT_END:
                pr_val = pr_latest.get(inv_id)
                if pr_val is not None and pr_val < PR_THRESHOLD:
                    anomalies.append({
                        "timestamp": timestamp,
                        "ora": round(ora, 2),
                        "inverter": f"INV {inv_id}",
                        "rule_id": 1,
                        "rule_name": "Low PR",
                        "severity": "critical",
                        "description": f"PR {pr_val:.1f}% < {PR_THRESHOLD}% at {int(ora):02d}:00",
                        "value": pr_val,
                    })

            # Rule 2: High Temperature
            if temp_val is not None:
                if temp_val > TEMP_CRITICAL:
                    anomalies.append({
                        "timestamp": timestamp,
                        "ora": round(ora, 2),
                        "inverter": f"INV {inv_id}",
                        "rule_id": 2,
                        "rule_name": "High Temperature",
                        "severity": "critical",
                        "description": f"Temperature {temp_val:.1f}C > {TEMP_CRITICAL}C",
                        "value": temp_val,
                    })
                elif temp_val > TEMP_WARNING:
                    anomalies.append({
                        "timestamp": timestamp,
                        "ora": round(ora, 2),
                        "inverter": f"INV {inv_id}",
                        "rule_id": 2,
                        "rule_name": "High Temperature",
                        "severity": "warning",
                        "description": f"Temperature {temp_val:.1f}C > {TEMP_WARNING}C",
                        "value": temp_val,
                    })

            # Rule 3: DC String Failure
            if ac_power is not None and ac_power > 500:
                mppt_cols = [c for c in merged_df.columns if "Corrente DC" in c and inv_id in c]
                if mppt_cols:
                    dc_values = [row.get(c) for c in mppt_cols if row.get(c) is not None and not pd.isna(row.get(c))]
                    if dc_values and min(dc_values) < DC_FAILURE_THRESHOLD:
                        anomalies.append({
                            "timestamp": timestamp,
                            "ora": round(ora, 2),
                            "inverter": f"INV {inv_id}",
                            "rule_id": 3,
                            "rule_name": "DC String Failure",
                            "severity": "critical",
                            "description": f"DC current {min(dc_values):.2f}A < {DC_FAILURE_THRESHOLD}A while AC {ac_power:.0f}W",
                            "value": min(dc_values),
                        })

            # Rule 4: AC Power Deviation
            if ac_power is not None and DAYLIGHT_START <= ora <= DAYLIGHT_END and site_ac_median > SITE_MIN_POWER:
                deviation = abs(ac_power - site_ac_median) / site_ac_median
                if deviation > AC_DEVIATION_PERCENT:
                    anomalies.append({
                        "timestamp": timestamp,
                        "ora": round(ora, 2),
                        "inverter": f"INV {inv_id}",
                        "rule_id": 4,
                        "rule_name": "AC Power Deviation",
                        "severity": "critical",
                        "description": f"AC deviation {deviation*100:.1f}% from median {site_ac_median:.0f}W",
                        "value": ac_power,
                    })

            # Rule 5: Communication Loss
            if (ac_power is None or pd.isna(ac_power)) and DAYLIGHT_START <= ora <= DAYLIGHT_END:
                anomalies.append({
                    "timestamp": timestamp,
                    "ora": round(ora, 2),
                    "inverter": f"INV {inv_id}",
                    "rule_id": 5,
                    "rule_name": "Communication Loss",
                    "severity": "high",
                    "description": f"No AC data at {int(ora):02d}:00",
                    "value": None,
                })

            # Rule 6: Inverter Trip
            if ac_power is not None and ac_power == 0 and DAYLIGHT_START <= ora <= DAYLIGHT_END and site_ac_median > 2000:
                anomalies.append({
                    "timestamp": timestamp,
                    "ora": round(ora, 2),
                    "inverter": f"INV {inv_id}",
                    "rule_id": 6,
                    "rule_name": "Inverter Trip",
                    "severity": "critical",
                    "description": f"AC = 0W while site median {site_ac_median:.0f}W at {int(ora):02d}:00",
                    "value": ac_power,
                })

    return anomalies


# ---------------------------------------------------------------------------
# Health Flags
# ---------------------------------------------------------------------------

def compute_inverter_health(inv_id: str, latest_row: pd.Series, pr_latest: dict) -> dict:
    """Compute 4 health LEDs for an inverter from latest row."""
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
    temp_val = latest_row.get(temp_col)
    if temp_val is None or pd.isna(temp_val):
        health["temp"] = "grey"
    elif temp_val > TEMP_CRITICAL:
        health["temp"] = "red"
    elif temp_val > TEMP_WARNING:
        health["temp"] = "yellow"
    else:
        health["temp"] = "green"

    # DC Current LED (average of all MPPTs)
    mppt_cols = [c for c in latest_row.index if "Corrente DC" in c and inv_id in c]
    if mppt_cols:
        dc_values = [latest_row.get(c) for c in mppt_cols if latest_row.get(c) is not None and not pd.isna(latest_row.get(c))]
        if dc_values:
            avg_dc = np.mean(dc_values)
            if avg_dc >= 20:
                health["dc_current"] = "green"
            elif avg_dc >= 10:
                health["dc_current"] = "yellow"
            elif avg_dc > 0:
                health["dc_current"] = "red"
            else:
                health["dc_current"] = "grey"
        else:
            health["dc_current"] = "grey"
    else:
        health["dc_current"] = "grey"

    # AC Power LED
    ac_col = f"Potenza AC (INV {inv_id}) [W]"
    ac_val = latest_row.get(ac_col)
    if ac_val is None or pd.isna(ac_val):
        health["ac_power"] = "grey"
    elif ac_val > 5000:
        health["ac_power"] = "green"
    elif ac_val > 1000:
        health["ac_power"] = "yellow"
    elif ac_val > 0:
        health["ac_power"] = "red"
    else:
        health["ac_power"] = "grey"

    # Overall = worst of 4
    led_values = {"green": 0, "yellow": 1, "red": 2, "grey": -1}
    scores = [led_values.get(health[k], -1) for k in ["pr", "temp", "dc_current", "ac_power"]]
    scores = [s for s in scores if s >= 0]

    if not scores:
        health["overall_status"] = "grey"
    else:
        worst = max(scores)
        health["overall_status"] = ["green", "yellow", "red"][worst]

    return health


def deduplicate_anomalies(anomalies: list) -> list:
    """Collapse consecutive same anomalies within DEDUP_WINDOW."""
    if not anomalies:
        return []

    sorted_anom = sorted(anomalies, key=lambda x: x["timestamp"])
    deduped = []
    last_key = None
    last_time = None

    for anom in sorted_anom:
        key = (anom["inverter"], anom["rule_id"])
        try:
            anom_time = datetime.strptime(anom["timestamp"], "%H:%M:%S").timestamp()
        except:
            anom_time = time.time()

        if key != last_key or (last_time and abs(anom_time - last_time) > DEDUP_WINDOW):
            deduped.append(anom)
            last_key = key
            last_time = anom_time

    return deduped[-50:]  # Keep last 50


# ---------------------------------------------------------------------------
# Main Analysis
# ---------------------------------------------------------------------------

def analyze_site(date_str: str) -> None:
    """Complete analysis pipeline."""
    try:
        logger.info(f"Starting analysis for {date_str}...")

        # Load all metrics
        logger.info("Loading metrics...")
        potenza_ac = load_metric_csv(date_str, "Potenza_AC")
        pr_df = load_metric_csv(date_str, "PR")
        temp_df = load_metric_csv(date_str, "Temperatura")
        dc_df = load_metric_csv(date_str, "Corrente_DC")
        resist_df = load_metric_csv(date_str, "Resistenza_Isolamento")
        irrad_df = load_metric_csv(date_str, "Irraggiamento")

        if potenza_ac is None:
            logger.warning(f"Potenza_AC not found for {date_str}")
            return

        # Merge all time-series data on Ora
        logger.info("Merging data...")
        merged = clean_and_merge_timeseries(date_str, potenza_ac, temp_df, dc_df, resist_df, irrad_df)

        # Apply rules
        logger.info("Applying forensic rules...")
        anomalies = apply_forensic_rules(date_str, merged, pr_df)
        anomalies = deduplicate_anomalies(anomalies)
        logger.info(f"Found {len(anomalies)} anomalies")

        # Compute health from latest row
        latest_row = merged.iloc[-1]
        pr_latest = {}
        if pr_df is not None:
            for inv_id in INVERTER_IDS:
                inv_rows = pr_df[pr_df["Inverter"] == f"INV {inv_id}"]
                if len(inv_rows) > 0:
                    pr_latest[inv_id] = normalize_pr(inv_rows.iloc[-1]["PR"])

        inverter_health = {}
        for inv_id in INVERTER_IDS:
            inverter_health[f"INV {inv_id}"] = compute_inverter_health(inv_id, latest_row, pr_latest)

        # Build JSON
        timestamp = datetime.now().isoformat(timespec="seconds")
        snapshot = {
            timestamp: {
                "macro_health": {
                    "total_inverters": len(INVERTER_IDS),
                    "online": len([h for h in inverter_health.values() if h["ac_power"] in ["green", "yellow"]]),
                    "tripped": len([h for h in inverter_health.values() if h["ac_power"] == "red"]),
                    "comms_lost": len([h for h in inverter_health.values() if h["ac_power"] == "grey"]),
                    "last_sync": timestamp,
                },
                "inverter_health": inverter_health,
                "active_anomalies": anomalies,
            }
        }

        # Write JSON
        json_path = DATA_DIR / f"dashboard_data_{date_str}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
        logger.info(f"Wrote JSON: {json_path}")

        # Write CSV if anomalies exist
        if anomalies:
            csv_path = DATA_DIR / f"anomalies_{date_str}.csv"
            anom_df = pd.DataFrame(anomalies)
            anom_df.to_csv(str(csv_path), index=False)
            logger.info(f"Wrote CSV: {csv_path}")

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
            logger.info(f"Complete set for {today} detected. Analyzing...")
            analyze_site(today)


def main():
    logger.info("Starting VCOM Watchdog v3...")
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
