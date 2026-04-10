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
from mppt_dc_analyzer import analyze_dc_current

# ---------------------------------------------------------------------------
# Paths & Logging
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
DATA_DIR = ROOT / "extracted_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_config() -> dict:
    """Load configuration from config.json."""
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

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


class NumpyEncoder(json.JSONEncoder):
    """JSON encoder that handles numpy int64/float64 and NaN values."""
    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            if np.isnan(obj):
                return None
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)

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
DAYLIGHT_END = 19.0
STABILIZATION_MINUTES = 30  # Wait 30m after production start before PR alarms

def get_production_start_time(ac_df: pd.DataFrame) -> float:
    """Find the first Ora where >10 inverters are producing >0W."""
    if ac_df is None or "Ora" not in ac_df.columns:
        return 7.0 # Fallback
    
    ac_cols = [c for c in ac_df.columns if "Potenza AC" in c]
    df_clean = ac_df.replace(["x", " x "], np.nan).apply(pd.to_numeric, errors='coerce')
    
    for idx, row in df_clean.iterrows():
        # Count how many inverters have production > 100W (more stable than >0)
        producing = sum(1 for c in ac_cols if row.get(c, 0) > 100)
        if producing > 10:
            return float(row.get("Ora", 7.0))
            
    return 7.0


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
    """Load metric from CSV or Excel.
    
    Always re-converts the Excel to CSV when the .xlsx is newer than the .csv,
    so the analysis uses the latest appended data, not a stale first-export.
    """
    csv_path = DATA_DIR / f"{metric_prefix}_{date_str}.csv"
    excel_path = DATA_DIR / f"{metric_prefix}_{date_str}.xlsx"

    # Re-convert from Excel if it exists and is newer than the CSV
    if excel_path.exists():
        need_convert = True
        if csv_path.exists():
            try:
                xlsx_mtime = excel_path.stat().st_mtime
                csv_mtime = csv_path.stat().st_mtime
                need_convert = (xlsx_mtime > csv_mtime)
            except Exception:
                need_convert = True

        if need_convert:
            try:
                if excel_to_csv(excel_path, csv_path):
                    df = pd.read_csv(str(csv_path))
                    logger.info(f"Loaded {csv_path.name} (sep=None)")
                    return df
            except Exception as e:
                logger.warning(f"Failed to convert/load {excel_path.name}: {e}")

    # Fall back to existing CSV
    if csv_path.exists():
        try:
            df = pd.read_csv(str(csv_path), sep=None, engine='python')
            logger.info(f"Loaded {csv_path.name} (sep=None)")
            return df
        except Exception:
            pass

    logger.warning(f"{metric_prefix}_{date_str} not found")
    return None


def normalize_pr(val):
    """Convert PR to 0-100% scale, handles Italian strings like '95,5'."""
    if pd.isna(val):
        return None
    if isinstance(val, str):
        try:
            val = float(val.replace(".", "").replace(",", "."))
        except:
            return None
    return val if val > 1.5 else val * 100


# ---------------------------------------------------------------------------
# Health Computation (from latest data)
# ---------------------------------------------------------------------------

def compute_latest_health(date_str: str, ac_df: pd.DataFrame, temp_df: pd.DataFrame,
                         dc_df: pd.DataFrame, pr_df: pd.DataFrame, daylight_start: float = 7.0) -> dict:
    """
    Compute health flags from the latest available NON-NAN values in each metric file.
    """
    inverter_health = {}

    # Get latest PR values
    pr_latest = {}
    if pr_df is not None:
        pr_df_clean = pr_df.copy()
        pr_col = "PR" if "PR" in pr_df_clean.columns else "PR inverter [%]" if "PR inverter [%]" in pr_df_clean.columns else None
        inv_col = "Inverter" if "Inverter" in pr_df_clean.columns else "PR inverter" if "PR inverter" in pr_df_clean.columns else None

        if pr_col and inv_col:
            pr_df_clean[pr_col] = pr_df_clean[pr_col].apply(normalize_pr)
            for inv_id in INVERTER_IDS:
                rows = pr_df_clean[pr_df_clean[inv_col] == f"INV {inv_id}"]
                if len(rows) > 0:
                    pr_latest[inv_id] = rows.iloc[-1][pr_col]

    # Find the latest AC row with valid data.
    # The file has many appended batches (each with Ora 0.00-23.55).
    # We deduplicate by Ora (keep the LAST value for each slot), then
    # find the highest Ora that has real data.
    ac_row = None
    ora = 0
    latest_ac_missing = set()
    
    if ac_df is not None and len(ac_df) > 0:
        ac_cols = [c for c in ac_df.columns if "Potenza AC" in c]
        
        # Deduplicate: keep only the last occurrence of each Ora value
        if "Ora" in ac_df.columns:
            ac_dedup = ac_df.drop_duplicates(subset=["Ora"], keep="last").copy()
            ac_dedup["Ora_num"] = pd.to_numeric(ac_dedup["Ora"], errors="coerce")
            ac_dedup = ac_dedup.sort_values("Ora_num", ascending=False)
        else:
            ac_dedup = ac_df.copy()
        
        # Search from highest Ora downward for a row with valid values
        for _, row in ac_dedup.iterrows():
            valid_count = sum(
                1 for c in ac_cols
                if pd.notna(row.get(c)) and str(row.get(c)).strip().lower() not in ['x', '']
            )
            if valid_count > 10:
                ac_row = row
                ora = row.get("Ora", 0)
                try: ora = float(ora)
                except: ora = 0.0
                logger.info(f"Found latest valid AC data at Ora={ora} ({valid_count} valid inverters)")
                break

        if ac_row is None:
            ac_row = ac_df.iloc[-1]
            try: ora = float(ac_row.get("Ora", 0))
            except: ora = 0.0
            logger.warning(f"No valid AC row found, using last row (Ora={ora})")

        # Now figure out which specific inverters are offline in this row
        for c in ac_df.columns:
            if "Potenza AC" in c:
                val = ac_row.get(c)
                if pd.isna(val) or str(val).strip().lower() in ['x', '']:
                    latest_ac_missing.add(c)

    # Get latest temp row (deduplicated by Ora)
    temp_row = None
    if temp_df is not None and len(temp_df) > 0:
        temp_cols = [c for c in temp_df.columns if "Temperatura" in c]
        if "Ora" in temp_df.columns:
            temp_dedup = temp_df.drop_duplicates(subset=["Ora"], keep="last").copy()
            temp_dedup["Ora_num"] = pd.to_numeric(temp_dedup["Ora"], errors="coerce")
            temp_dedup = temp_dedup.sort_values("Ora_num", ascending=False)
        else:
            temp_dedup = temp_df.copy()
        
        for _, row in temp_dedup.iterrows():
            temp_values = [row.get(c) for c in temp_cols]
            non_nan_count = sum(1 for v in temp_values if v is not None and not pd.isna(v))
            if non_nan_count > 30:
                temp_row = row
                break
        if temp_row is None and len(temp_df) > 0:
            temp_row = temp_df.iloc[-1]

    # Get latest DC row (deduplicated by Ora)
    dc_row = None
    if dc_df is not None and len(dc_df) > 0:
        dc_cols = [c for c in dc_df.columns if "Corrente DC" in c]
        if "Ora" in dc_df.columns:
            dc_dedup = dc_df.drop_duplicates(subset=["Ora"], keep="last").copy()
            dc_dedup["Ora_num"] = pd.to_numeric(dc_dedup["Ora"], errors="coerce")
            dc_dedup = dc_dedup.sort_values("Ora_num", ascending=False)
        else:
            dc_dedup = dc_df.copy()
        
        for _, row in dc_dedup.iterrows():
            dc_values = [row.get(c) for c in dc_cols]
            non_nan_count = sum(1 for v in dc_values if v is not None and not pd.isna(v))
            if non_nan_count > 400:
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
        else:
            # Strictly use 'red' for anything below threshold to match user requirement
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
            avg_dc = float(np.mean(dc_values))
            # Contextualize to time of day: early morning/evening < afternoon
            if daylight_start <= ora <= 12:  # Morning ramp-up
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
        val_raw = ac_row.get(ac_col) if ac_row is not None else None
        
        try:
            ac_val = float(val_raw)
        except (ValueError, TypeError):
            ac_val = None

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
            if daylight_start <= ora <= DAYLIGHT_END:
                health["ac_power"] = "red"  # Should be generating
            else:
                health["ac_power"] = "grey"  # Off-hours is OK

        ac_col = f"Potenza AC (INV {inv_id}) [W]"
        if ac_col in latest_ac_missing:
            health["comms_lost_flag"] = True
        else:
            health["comms_lost_flag"] = False

        # Overall = worst of 4 LEDs
        scores = []
        score_map = {"green": 0, "yellow": 1, "red": 2, "grey": -1}
        for k in ["pr", "temp", "dc_current", "ac_power"]:
            score = score_map.get(health[k], -1)
            if score >= 0:
                scores.append(score)

        if health.get("comms_lost_flag"):
            health["overall_status"] = "grey"
            health["ac_power"] = "grey"
        elif not scores:
            health["overall_status"] = "grey"
        else:
            worst = max(scores)
            health["overall_status"] = ["green", "yellow", "red"][worst]

        # Also store raw values and metadata
        health["raw_pr"] = float(pr_val) if pr_val is not None else None
        health["data_time"] = format_ora(ora)
        health["is_stabilized"] = bool(ora >= (daylight_start + (STABILIZATION_MINUTES / 60.0)))

        inverter_health[inv_label] = health

    return inverter_health


# ---------------------------------------------------------------------------
# Downtime Tracker
# ---------------------------------------------------------------------------

def format_ora(val):
    """Convert float like 9.25 to '09:25'"""
    if pd.isna(val): return "Unknown"
    try:
        h = int(val)
        m = int(round((val % 1) * 100))
        return f"{h:02d}:{m:02d}"
    except:
        return "Unknown"

def compute_downtime(ac_df: pd.DataFrame, irrad_df: pd.DataFrame, daylight_start: float = 7.0) -> dict:
    """Calculate downtime events based on 0.0 W strings during daylight hours."""
    downtime_tracker = {}
    if ac_df is None or len(ac_df) == 0:
        return downtime_tracker

    # Filter for daylight hours
    if "Ora" not in ac_df.columns:
        return downtime_tracker

    # Deduplicate by Ora (keep last batch's values)
    ac_dedup = ac_df.drop_duplicates(subset=["Ora"], keep="last").copy()
    ac_dedup["Ora"] = pd.to_numeric(ac_dedup["Ora"], errors="coerce")
        
    mask = (ac_dedup["Ora"] >= daylight_start) & (ac_dedup["Ora"] <= DAYLIGHT_END)
    df_day = ac_dedup[mask].copy()
    if len(df_day) == 0:
        return downtime_tracker

    df_day = df_day.replace(["x", " x "], np.nan)
    
    # Get POA array if available
    poa_array = None
    if irrad_df is not None and not irrad_df.empty:
        poa_cols = [c for c in irrad_df.columns if "POA" in c]
        if poa_cols:
            poa_array = irrad_df[poa_cols[0]].replace(["x", " x "], np.nan)
            poa_array = pd.to_numeric(poa_array, errors="coerce")

    for inv_id in INVERTER_IDS:
        inv_label = f"INV {inv_id}"
        ac_col = f"Potenza AC ({inv_label}) [W]"
        if ac_col not in df_day.columns:
            continue
            
        data = pd.to_numeric(df_day[ac_col], errors='coerce')
        is_zero = (data == 0.0)
        
        zero_indices = is_zero[is_zero].index
        if len(zero_indices) > 0:
            # Group contiguous zeros to find the latest block
            blocks = (is_zero != is_zero.shift()).cumsum()
            zero_blocks = blocks[is_zero]
            last_block_id = zero_blocks.iloc[-1]
            last_block_indices = zero_blocks[zero_blocks == last_block_id].index
            
            idx_start = last_block_indices[0]
            idx_end = last_block_indices[-1]
            
            time_stopped = format_ora(df_day.loc[idx_start].get("Ora"))
            
            # Check if it's still off
            valid_data_idx = data.dropna().index
            if len(valid_data_idx) > 0 and idx_end == valid_data_idx[-1]:
                started_again = "still off"
            else:
                after_end = data.loc[idx_end:].dropna()
                if len(after_end) > 1:
                    rec_idx = after_end.index[1]
                    started_again = format_ora(df_day.loc[rec_idx].get("Ora"))
                else:
                    started_again = "still off"
                    
            # Last available data timestamp
            last_data_idx = valid_data_idx[-1] if len(valid_data_idx) > 0 else df_day.index[-1]
            last_ts = format_ora(df_day.loc[last_data_idx].get("Ora"))
            
            last_poa = "—"
            if poa_array is not None and len(poa_array.dropna()) > 0:
                last_poa_val = poa_array.dropna().iloc[-1]
                last_poa = f"{last_poa_val:.1f}"

            def ora_to_minutes(o):
                """Convert decimal hour (e.g., 8.1666) or HH.MM style float to total minutes."""
                if pd.isna(o): return 0
                try:
                    f = float(o)
                    # Handle both decimal hour (8.5 = 8:30) and HH.MM (8.30 = 8:30)
                    # The VCOM Ora column is typically decimal hours
                    hours = int(f)
                    minutes = int(round((f - hours) * 60))
                    return hours * 60 + minutes
                except: return 0

            stop_min = ora_to_minutes(df_day.loc[idx_start].get("Ora"))
            
            # If it's recovered, use recovery time, else use last available data time
            if started_again == "still off":
                end_min = ora_to_minutes(df_day.loc[last_data_idx].get("Ora"))
            else:
                # We used format_ora(df_day.loc[rec_idx].get("Ora")) to get started_again string
                end_min = ora_to_minutes(df_day.loc[rec_idx].get("Ora"))
            
            # Duration in minutes
            total_time_off_calc = end_min - stop_min
            if total_time_off_calc < 0: total_time_off_calc = 0 # Safety

            if total_time_off_calc >= 9:
                downtime_tracker[inv_label] = {
                    "inverter": inv_label,
                    "last_data_fetched": last_ts,
                    "last_poa": last_poa,
                    "time_stopped": time_stopped,
                    "started_again": started_again,
                    "total_time_off": int(total_time_off_calc)
                }
            
    return downtime_tracker

# Macro Health
# ---------------------------------------------------------------------------

def compute_macro_health(inverter_health: dict, daylight_start: float = 7.0) -> dict:
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
        "plant_start_time": format_ora(daylight_start),
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

        # Compute dynamic production start
        daylight_start = get_production_start_time(ac_df)
        logger.info(f"Dynamic Daylight Start detected at: {format_ora(daylight_start)}")

        # Compute health from latest values
        logger.info("Computing health flags...")
        inverter_health = compute_latest_health(date_str, ac_df, temp_df, dc_df, pr_df, daylight_start=daylight_start)
        macro_health = compute_macro_health(inverter_health, daylight_start=daylight_start)
        
        # Latest sync from extraction status if available
        status_path = DATA_DIR / "extraction_status.json"
        if status_path.exists():
            try:
                with open(status_path, "r", encoding="utf-8") as f:
                    estatus = json.load(f)
                    if date_str in estatus:
                        # Find the latest timestamp across all metrics for that day
                        ts_list = [v["timestamp"] for v in estatus[date_str].values() if "timestamp" in v]
                        if ts_list:
                            macro_health["last_data_fetch"] = max(ts_list)
            except:
                pass

        logger.info("Evaluating MPPT DC Data...")
        md_report_path = DATA_DIR / f"mppt_analysis_report_{date_str}.md"
        analyze_dc_current(dc_df, md_report_path, date_str)
        # -----------------------------------
        
        # Compute Downtime array
        logger.info("Evaluating downtime...")
        downtime_tracker = compute_downtime(ac_df, irrad_df, daylight_start=daylight_start)

        # Build JSON snapshot and process anomalies
        timestamp = datetime.now().isoformat(timespec="seconds")
        json_path = DATA_DIR / f"dashboard_data_{date_str}.json"
        
        # Load previous state
        existing_data = {}
        if json_path.exists():
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
            except:
                pass
                
        historical_trail = []
        active_anomalies_prev = []
        
        if existing_data:
            last_ts = sorted(existing_data.keys())[-1]
            last_snap = existing_data[last_ts]
            historical_trail = last_snap.get("historical_trail", [])
            active_anomalies_prev = last_snap.get("active_anomalies", [])
            
        current_active = []
        
        # We need a quick lookup of previous alarms
        prev_alarm_map = {a.get("id"): a for a in active_anomalies_prev}
        
        for inv_id in INVERTER_IDS:
            inv_label = f"INV {inv_id}"
            h = inverter_health.get(inv_label, {})
            
            # --- 1. COMMS LOST Alarm ---
            is_comms_lost = h.get("comms_lost_flag", False)
            comms_alarm_id = f"{inv_id}_COMMS_LOST"
            
            was_comms_lost = comms_alarm_id in prev_alarm_map
            
            if is_comms_lost:
                if not was_comms_lost:
                    # New alarm
                    current_active.append({
                        "id": comms_alarm_id,
                        "inverter": inv_label,
                        "type": "COMMS LOST",
                        "severity": "grey",
                        "trip_time": timestamp,
                        "message": "Missing data for this component."
                    })
                else:
                    # Carry over existing
                    current_active.append(prev_alarm_map[comms_alarm_id])
            else:
                # Recovered?
                if was_comms_lost:
                    prev_alarm = prev_alarm_map[comms_alarm_id]
                    prev_alarm["recovery_time"] = timestamp
                    historical_trail.append(prev_alarm)

            # --- 2. PR Alarm ---
            pr_val = h.get("raw_pr")
            pr_alarm_id = f"{inv_id}_LOW_PR"
            data_time_str = h.get("data_time", "Unknown")
            is_stabilized = h.get("is_stabilized", True)
            
            was_low_pr = pr_alarm_id in prev_alarm_map
            
            # Trigger alarm only if PR < 85 and system is stabilized (30m after production start)
            if pr_val is not None and pr_val < PR_THRESHOLD and is_stabilized:
                if not was_low_pr:
                    current_active.append({
                        "id": pr_alarm_id,
                        "inverter": inv_label,
                        "type": "LOW PR",
                        "severity": "red",
                        "trip_time": data_time_str, # Use ACTUAL time from data
                        "message": f"Performance Ratio dropped to {pr_val:.1f}%"
                    })
                else:
                    # Carry over
                    prev_alarm = prev_alarm_map[pr_alarm_id]
                    prev_alarm["message"] = f"Performance Ratio is {pr_val:.1f}%"
                    current_active.append(prev_alarm)
            else:
                # Recovered
                if was_low_pr:
                    prev_alarm = prev_alarm_map[pr_alarm_id]
                    prev_alarm["recovery_time"] = timestamp
                    historical_trail.append(prev_alarm)

        # Preserve any other active anomalies that we didn't handle in this loop
        handled_ids = set([a["id"] for a in current_active] + [a["id"] for a in historical_trail if "recovery_time" in a])
        for a in active_anomalies_prev:
            if a.get("id") not in handled_ids:
                current_active.append(a)
                
        snapshot = {
            "macro_health": macro_health,
            "inverter_health": inverter_health,
            "active_anomalies": current_active,
            "historical_trail": historical_trail,
            "downtime_tracker": downtime_tracker
        }

        # Save merged data (keep last 50 timestamps)
        existing_data[timestamp] = snapshot
        timestamps = sorted(existing_data.keys())
        if len(timestamps) > 50:
            for old_ts in timestamps[:-50]:
                del existing_data[old_ts]
        
        # Write JSON
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=2, cls=NumpyEncoder)
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
