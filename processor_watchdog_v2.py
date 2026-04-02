"""
processor_watchdog_v2.py — VCOM Forensic Analysis with CSV format

Monitors extracted_data/ for complete daily metric sets, runs forensic analysis,
generates health flags, and outputs JSON snapshots + CSV audit trails.

Uses CSV format for faster processing and lighter footprint than Excel.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
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
logger = logging.getLogger("watchdog_v2")

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

MPPT_COUNTS = {inv_id: 12 for inv_id in INVERTER_IDS}  # 12 MPPTs per inverter

IRRADIANCE_SENSORS = [
    "JB-SM1_AL-1-DOWN", "JB-SM1_AL-1-UP", "JB1_POA-1", "JB1_GHI-1",
    "JB1_IT-1-1", "JB1_IT-1-2", "JB2_IT-2-2", "JB2_IT-2-1",
    "JB-SM3_AL-3-DOWN", "JB-SM3_AL-3-UP", "JB-SM3_GHI-3", "JB3_POA-3",
    "JB3_IT-3-2", "JB3_IT-3-1",
]

# Rule thresholds
PR_THRESHOLD = 0.85  # <85% is low PR
TEMP_WARNING = 40.0  # >40°C is warning, >45°C is critical
DC_FAILURE_THRESHOLD = 0.2  # <0.2A is string failure
AC_DEVIATION_PERCENT = 0.03  # >3% deviation is anomaly
SITE_MIN_POWER = 5000  # Only evaluate deviations when site >5kW

DAYLIGHT_START = 7.0  # 07:00
DAYLIGHT_END = 19.0  # 19:00

# Deduplication window (seconds)
DEDUP_WINDOW = 3600  # 1 hour


# ---------------------------------------------------------------------------
# Data Loading & Conversion to CSV
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

    # Use CSV if available, otherwise convert Excel to CSV
    if csv_path.exists():
        return pd.read_csv(str(csv_path))
    elif excel_path.exists():
        excel_to_csv(excel_path, csv_path)
        return pd.read_csv(str(csv_path))
    else:
        logger.warning(f"No file found for {metric_prefix}_{date_str}")
        return None


# ---------------------------------------------------------------------------
# Data Cleaning & Preparation
# ---------------------------------------------------------------------------

def clean_potenza_ac(df: pd.DataFrame) -> pd.DataFrame:
    """Clean AC power data: ensure numeric columns, handle NaNs."""
    # Ensure Ora is numeric (hours as float)
    df["Ora"] = pd.to_numeric(df["Ora"], errors="coerce")

    # Convert inverter columns to numeric
    for col in df.columns:
        if "Potenza AC" in col:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def clean_temperature(df: pd.DataFrame) -> pd.DataFrame:
    """Clean temperature data."""
    df["Ora"] = pd.to_numeric(df["Ora"], errors="coerce")
    for col in df.columns:
        if "Temperatura" in col:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def clean_resistance(df: pd.DataFrame) -> pd.DataFrame:
    """Clean insulation resistance data."""
    df["Ora"] = pd.to_numeric(df["Ora"], errors="coerce")
    for col in df.columns:
        if "Resistenza" in col:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def clean_dc_current(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DC current data (all MPPT columns)."""
    df["Ora"] = pd.to_numeric(df["Ora"], errors="coerce")
    for col in df.columns:
        if "Corrente DC" in col:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def clean_irradiance(df: pd.DataFrame) -> pd.DataFrame:
    """Clean irradiance sensor data."""
    df["Ora"] = pd.to_numeric(df["Ora"], errors="coerce")
    for col in df.columns:
        if "Irraggiamento" in col:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Inverter Health Computation
# ---------------------------------------------------------------------------

def compute_inverter_health(inverter_id: str, row_data: dict) -> dict:
    """
    Compute 4 health LEDs for an inverter based on latest row data.

    Returns: {
        'pr': 'green'|'yellow'|'red'|'grey',
        'temp': 'green'|'yellow'|'red'|'grey',
        'dc_current': 'green'|'yellow'|'red'|'grey',
        'ac_power': 'green'|'yellow'|'red'|'grey',
        'overall_status': 'green'|'yellow'|'red'|'grey'
    }
    """
    health = {}

    # PR Health (0-1 scale or 0-100%)
    pr_val = row_data.get(f"pr_{inverter_id}")
    if pr_val is None or pd.isna(pr_val):
        health["pr"] = "grey"
    elif pr_val >= 0.85:
        health["pr"] = "green"
    elif pr_val >= 0.75:
        health["pr"] = "yellow"
    else:
        health["pr"] = "red"

    # Temperature Health
    temp_val = row_data.get(f"temp_{inverter_id}")
    if temp_val is None or pd.isna(temp_val):
        health["temp"] = "grey"
    elif temp_val <= 40.0:
        health["temp"] = "green"
    elif temp_val <= 45.0:
        health["temp"] = "yellow"
    else:
        health["temp"] = "red"

    # DC Current Health (% of site median)
    dc_val = row_data.get(f"dc_current_{inverter_id}")
    site_dc_median = row_data.get("dc_current_site_median")
    if dc_val is None or pd.isna(dc_val) or site_dc_median is None:
        health["dc_current"] = "grey"
    else:
        dc_pct = dc_val / site_dc_median if site_dc_median > 0 else 0
        if dc_pct >= 0.85:
            health["dc_current"] = "green"
        elif dc_pct >= 0.50:
            health["dc_current"] = "yellow"
        else:
            health["dc_current"] = "red"

    # AC Power Health (% of site median)
    ac_val = row_data.get(f"ac_power_{inverter_id}")
    site_ac_median = row_data.get("ac_power_site_median")
    if ac_val is None or pd.isna(ac_val) or site_ac_median is None:
        health["ac_power"] = "grey"
    elif ac_val == 0 and site_ac_median > 500:
        health["ac_power"] = "red"  # Inverter tripped
    else:
        ac_pct = ac_val / site_ac_median if site_ac_median > 0 else 0
        if ac_pct >= 0.97:
            health["ac_power"] = "green"
        elif ac_pct >= 0.85:
            health["ac_power"] = "yellow"
        else:
            health["ac_power"] = "red"

    # Overall = worst of 4 LEDs
    led_values = {"green": 0, "yellow": 1, "red": 2, "grey": -1}
    led_scores = [led_values.get(health[k], -1) for k in ["pr", "temp", "dc_current", "ac_power"]]
    led_scores = [s for s in led_scores if s >= 0]  # Exclude grey

    if not led_scores:
        health["overall_status"] = "grey"
    else:
        worst = max(led_scores)
        if worst == 2:
            health["overall_status"] = "red"
        elif worst == 1:
            health["overall_status"] = "yellow"
        else:
            health["overall_status"] = "green"

    return health


# ---------------------------------------------------------------------------
# Forensic Analysis Rules
# ---------------------------------------------------------------------------

def apply_forensic_rules(date_str: str, potenza_ac: pd.DataFrame, pr_df: pd.DataFrame,
                        temp_df: pd.DataFrame, dc_df: pd.DataFrame,
                        resist_df: pd.DataFrame, irrad_df: pd.DataFrame) -> list:
    """
    Apply 6 forensic rules to detect anomalies.
    Returns list of {timestamp, inverter_id, rule_id, severity, description}
    """
    anomalies = []

    # Pivot PR data: long -> wide
    pr_wide = pr_df.pivot_table(index="Timestamp Fetch", columns="Inverter", values="PR")
    pr_wide.columns = [f"pr_{col.replace('INV ', '')}" for col in pr_wide.columns]

    # Process by row (time slice)
    for idx, row in potenza_ac.iterrows():
        ora = row["Ora"]
        timestamp = row["Timestamp Fetch"]

        # Get corresponding rows from other metrics (match by Ora)
        temp_row = temp_df[temp_df["Ora"] == ora].iloc[0] if len(temp_df[temp_df["Ora"] == ora]) > 0 else None
        resist_row = resist_df[resist_df["Ora"] == ora].iloc[0] if len(resist_df[resist_df["Ora"] == ora]) > 0 else None

        # Calculate site medians for this time slice
        ac_cols = [c for c in potenza_ac.columns if "Potenza AC" in c]
        site_ac_values = [row[c] for c in ac_cols if not pd.isna(row[c]) and row[c] > 0]
        site_ac_median = pd.Series(site_ac_values).median() if site_ac_values else 0

        # DC Current: average of all MPPT trackers per inverter
        dc_cols = [c for c in dc_df.columns if "Corrente DC" in c]
        dc_row = dc_df[dc_df["Ora"] == ora].iloc[0] if len(dc_df[dc_df["Ora"] == ora]) > 0 else None

        site_dc_values = []
        if dc_row is not None:
            for inv_id in INVERTER_IDS:
                mppt_cols = [c for c in dc_cols if inv_id in c]
                if mppt_cols:
                    inv_dc_values = [dc_row[c] for c in mppt_cols if not pd.isna(dc_row[c])]
                    if inv_dc_values:
                        site_dc_values.append(pd.Series(inv_dc_values).mean())

        site_dc_median = pd.Series(site_dc_values).median() if site_dc_values else 0

        # Evaluate each inverter
        for inv_id in INVERTER_IDS:
            ac_col = f"Potenza AC (INV {inv_id}) [W]"
            temp_col = f"Temperatura inverter (INV {inv_id}) [°C]"

            ac_power = row.get(ac_col)
            if ac_power is None or pd.isna(ac_power):
                ac_power = None

            temp_val = temp_row[temp_col] if temp_row is not None and temp_col in temp_row else None
            if temp_val is not None and pd.isna(temp_val):
                temp_val = None

            # Rule 1: Low PR (during daylight)
            if DAYLIGHT_START <= ora <= DAYLIGHT_END:
                pr_col = f"pr_{inv_id}"
                pr_row = pr_df[pr_df["Inverter"] == f"INV {inv_id}"]
                if len(pr_row) > 0:
                    pr_val = pr_row.iloc[-1]["PR"]  # Latest PR value
                    if pr_val < PR_THRESHOLD:
                        anomalies.append({
                            "timestamp": timestamp,
                            "ora": ora,
                            "inverter": f"INV {inv_id}",
                            "rule_id": 1,
                            "rule_name": "Low PR",
                            "severity": "critical",
                            "description": f"PR {pr_val:.2f} < {PR_THRESHOLD} during {int(ora):02d}:00",
                            "value": pr_val,
                        })

            # Rule 2: High Temperature
            if temp_val is not None:
                if temp_val > 45.0:
                    anomalies.append({
                        "timestamp": timestamp,
                        "ora": ora,
                        "inverter": f"INV {inv_id}",
                        "rule_id": 2,
                        "rule_name": "High Temperature",
                        "severity": "critical",
                        "description": f"Temperature {temp_val:.1f}°C > 45°C",
                        "value": temp_val,
                    })
                elif temp_val > 40.0:
                    anomalies.append({
                        "timestamp": timestamp,
                        "ora": ora,
                        "inverter": f"INV {inv_id}",
                        "rule_id": 2,
                        "rule_name": "High Temperature",
                        "severity": "warning",
                        "description": f"Temperature {temp_val:.1f}°C > 40°C",
                        "value": temp_val,
                    })

            # Rule 3: DC String Failure (while AC generating)
            if ac_power is not None and ac_power > 500:
                mppt_cols = [c for c in dc_cols if inv_id in c]
                if dc_row is not None and mppt_cols:
                    dc_values = [dc_row.get(c) for c in mppt_cols if not pd.isna(dc_row.get(c))]
                    if dc_values and min(dc_values) < DC_FAILURE_THRESHOLD:
                        anomalies.append({
                            "timestamp": timestamp,
                            "ora": ora,
                            "inverter": f"INV {inv_id}",
                            "rule_id": 3,
                            "rule_name": "DC String Failure",
                            "severity": "critical",
                            "description": f"DC current {min(dc_values):.2f}A < {DC_FAILURE_THRESHOLD}A while AC = {ac_power:.0f}W",
                            "value": min(dc_values),
                        })

            # Rule 4: AC Power Deviation (during daylight when site > 5kW)
            if ac_power is not None and DAYLIGHT_START <= ora <= DAYLIGHT_END and site_ac_median > SITE_MIN_POWER:
                deviation = abs(ac_power - site_ac_median) / site_ac_median
                if deviation > AC_DEVIATION_PERCENT:
                    anomalies.append({
                        "timestamp": timestamp,
                        "ora": ora,
                        "inverter": f"INV {inv_id}",
                        "rule_id": 4,
                        "rule_name": "AC Power Deviation",
                        "severity": "critical",
                        "description": f"AC power deviation {deviation*100:.1f}% from site median {site_ac_median:.0f}W",
                        "value": ac_power,
                    })

            # Rule 5: Communication Loss (NaN during daylight)
            if ac_power is None and DAYLIGHT_START <= ora <= DAYLIGHT_END:
                anomalies.append({
                    "timestamp": timestamp,
                    "ora": ora,
                    "inverter": f"INV {inv_id}",
                    "rule_id": 5,
                    "rule_name": "Communication Loss",
                    "severity": "high",
                    "description": f"No AC power data (NaN) at {int(ora):02d}:00",
                    "value": None,
                })

            # Rule 6: Inverter Trip (AC=0 while site generating)
            if ac_power is not None and ac_power == 0 and DAYLIGHT_START <= ora <= DAYLIGHT_END and site_ac_median > 2000:
                anomalies.append({
                    "timestamp": timestamp,
                    "ora": ora,
                    "inverter": f"INV {inv_id}",
                    "rule_id": 6,
                    "rule_name": "Inverter Trip",
                    "severity": "critical",
                    "description": f"AC power = 0W while site median {site_ac_median:.0f}W at {int(ora):02d}:00",
                    "value": ac_power,
                })

    return anomalies


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate_anomalies(anomalies: list) -> list:
    """
    Collapse consecutive same-inverter/same-rule anomalies within DEDUP_WINDOW.
    Keep max 50 most recent anomalies.
    """
    if not anomalies:
        return []

    # Sort by timestamp
    sorted_anom = sorted(anomalies, key=lambda x: x["timestamp"])

    deduped = []
    last_key = None
    last_time = None

    for anom in sorted_anom:
        key = (anom["inverter"], anom["rule_id"])
        anom_time = datetime.strptime(anom["timestamp"], "%H:%M:%S").timestamp()

        if key != last_key or (last_time and anom_time - last_time > DEDUP_WINDOW):
            deduped.append(anom)
            last_key = key
            last_time = anom_time

    # Keep last 50
    return deduped[-50:]


# ---------------------------------------------------------------------------
# Analysis Orchestration
# ---------------------------------------------------------------------------

def analyze_site(date_str: str) -> None:
    """
    Main analysis pipeline:
    1. Load all 6 metrics (convert Excel to CSV if needed)
    2. Clean data
    3. Apply forensic rules
    4. Compute health flags
    5. Write JSON snapshot + CSV audit trail
    """
    try:
        logger.info(f"Starting analysis for {date_str}...")

        # Load metrics
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

        # Clean data
        logger.info("Cleaning data...")
        potenza_ac = clean_potenza_ac(potenza_ac)
        if temp_df is not None:
            temp_df = clean_temperature(temp_df)
        if dc_df is not None:
            dc_df = clean_dc_current(dc_df)
        if resist_df is not None:
            resist_df = clean_resistance(resist_df)
        if irrad_df is not None:
            irrad_df = clean_irradiance(irrad_df)
        if pr_df is not None:
            pr_df["PR"] = pd.to_numeric(pr_df["PR"], errors="coerce")

        # Apply forensic rules
        logger.info("Applying forensic rules...")
        anomalies = apply_forensic_rules(date_str, potenza_ac, pr_df, temp_df, dc_df, resist_df, irrad_df)
        anomalies = deduplicate_anomalies(anomalies)
        logger.info(f"Found {len(anomalies)} unique anomalies")

        # Compute inverter health from latest row
        latest_row = potenza_ac.iloc[-1]
        inverter_health = {}
        for inv_id in INVERTER_IDS:
            row_data = {}

            # Collect latest values for this inverter
            pr_rows = pr_df[pr_df["Inverter"] == f"INV {inv_id}"]
            if len(pr_rows) > 0:
                row_data[f"pr_{inv_id}"] = pr_rows.iloc[-1]["PR"]

            temp_col = f"Temperatura inverter (INV {inv_id}) [°C]"
            if temp_col in temp_df.columns:
                temp_rows = temp_df[temp_df["Ora"] == latest_row["Ora"]]
                if len(temp_rows) > 0:
                    row_data[f"temp_{inv_id}"] = temp_rows.iloc[-1][temp_col]

            ac_col = f"Potenza AC (INV {inv_id}) [W]"
            if ac_col in potenza_ac.columns:
                row_data[f"ac_power_{inv_id}"] = latest_row[ac_col]

            row_data["ac_power_site_median"] = potenza_ac[ac_col].median()
            row_data["dc_current_site_median"] = 0  # Placeholder

            inverter_health[f"INV {inv_id}"] = compute_inverter_health(inv_id, row_data)

        # Build JSON snapshot
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
                "file_status": {
                    "PR": {"status": "success" if pr_df is not None else "failed", "timestamp": timestamp},
                    "Potenza_AC": {"status": "success", "timestamp": timestamp},
                    "Corrente_DC": {"status": "success" if dc_df is not None else "failed", "timestamp": timestamp},
                    "Resistenza_Isolamento": {"status": "success" if resist_df is not None else "failed", "timestamp": timestamp},
                    "Temperatura": {"status": "success" if temp_df is not None else "failed", "timestamp": timestamp},
                    "Irraggiamento": {"status": "success" if irrad_df is not None else "failed", "timestamp": timestamp},
                },
                "inverter_health": inverter_health,
                "active_anomalies": anomalies,
                "historical_trail": anomalies,
            }
        }

        # Write JSON
        json_path = DATA_DIR / f"dashboard_data_{date_str}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
        logger.info(f"Wrote JSON snapshot: {json_path}")

        # Write CSV audit trail
        if anomalies:
            csv_path = DATA_DIR / f"anomalies_{date_str}.csv"
            anom_df = pd.DataFrame(anomalies)
            anom_df.to_csv(str(csv_path), index=False)
            logger.info(f"Wrote anomaly trail: {csv_path} ({len(anomalies)} rows)")

    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# File System Watcher
# ---------------------------------------------------------------------------

class MetricFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_directory:
            return

        filename = Path(event.src_path).name
        if not filename.endswith(".xlsx"):
            return

        logger.debug(f"File modified: {filename}")
        self._check_and_analyze()

    def on_created(self, event):
        if event.is_directory:
            return

        filename = Path(event.src_path).name
        if not filename.endswith(".xlsx"):
            return

        logger.debug(f"File created: {filename}")
        time.sleep(2)  # Wait for file to stabilize
        self._check_and_analyze()

    def _check_and_analyze(self):
        """Check if all 6 metrics exist for today, trigger analysis."""
        today = datetime.now().strftime("%Y-%m-%d")
        required_files = [
            f"PR_{today}.xlsx",
            f"Potenza_AC_{today}.xlsx",
            f"Corrente_DC_{today}.xlsx",
            f"Resistenza_Isolamento_{today}.xlsx",
            f"Temperatura_{today}.xlsx",
            f"Irraggiamento_{today}.xlsx",
        ]

        present = [f for f in required_files if (DATA_DIR / f).exists()]
        if len(present) == len(required_files):
            logger.info(f"Complete metric set for {today} detected. Triggering analysis...")
            analyze_site(today)


def main():
    logger.info("Starting VCOM Watchdog v2...")
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
