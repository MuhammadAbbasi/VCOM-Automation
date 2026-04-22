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
from logging.handlers import RotatingFileHandler

# ---------------------------------------------------------------------------
# Paths & Logging
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
DATA_DIR = ROOT / "extracted_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# --- User Settings ---
USER_SETTINGS_PATH = ROOT / "user_settings.json"

DEFAULT_SETTINGS = {
    "collection_interval": 15,
    "thresholds": {
        "pr": {
            "green": 85.0,
            "yellow": 75.0
        },
        "temp": {
            "yellow": 40.0,
            "red": 45.0
        },
        "dc": {
            "morning_green": 10.0,
            "morning_yellow": 2.0,
            "afternoon_green": 5.0,
            "afternoon_yellow": 0.5
        },
        "ac": {
            "green": 5000.0,
            "yellow": 1000.0
        },
        "min_downtime_minutes": 9
    },
    "colors": {
        "green": "#10b981",
        "yellow": "#f59e0b",
        "red": "#ef4444",
        "grey": "#6b7280"
    },
    "telegram": {
        "enabled": False,
        "bot_token": "",
        "chat_id": "",
        "personal_id": ""
    }
}

def load_user_settings() -> dict:
    if not USER_SETTINGS_PATH.exists():
        with open(USER_SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_SETTINGS, f, indent=4)
        return DEFAULT_SETTINGS
    try:
        with open(USER_SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load user settings: {e}. using defaults.")
        return DEFAULT_SETTINGS


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
        RotatingFileHandler(LOG_PATH, maxBytes=1_000_000_000, backupCount=3, encoding="utf-8"),
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

DAYLIGHT_END = 20.0
STABILIZATION_MINUTES = 30  # Wait 30m after production start before PR alarms

def calculate_sunrise(date_str: str) -> float:
    """Calculate approximate sunrise for Mazara del Vallo (37.6N, 12.6E)."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        doy = dt.timetuple().tm_yday
        lat_rad = np.radians(37.67)
        # Solar declination
        decl = 0.409 * np.sin(2 * np.pi * (doy - 81) / 365)
        # Hour angle sunset/sunrise: cos(h) = -tan(lat)*tan(decl)
        cos_h = -np.tan(lat_rad) * np.tan(decl)
        cos_h = np.clip(cos_h, -1, 1)
        h = np.arccos(cos_h)
        # Sunrise base 12:00 - hour angle offset
        sunrise_utc_base = 12.0 - (np.degrees(h) / 15.0)
        # Longitude adjustment (Mazara 12.6E is ~9.6 mins later than 15E meridian)
        lon_adj = (15.0 - 12.59) * 4 / 60.0
        # Equation of time approx
        b = 2 * np.pi * (doy - 81) / 365
        eot = 9.87 * np.sin(2 * b) - 7.53 * np.cos(b) - 1.5 * np.sin(b)
        eot_adj = eot / 60.0
        # DST: April to October is +1
        dst_adj = 1.0 if (3 <= dt.month <= 10) else 0.0
        
        sunrise = sunrise_utc_base + lon_adj + dst_adj - eot_adj
        
        # Sunset is 12:00 + hour angle
        sunset_utc_base = 12.0 + (np.degrees(h) / 15.0)
        sunset = sunset_utc_base + lon_adj + dst_adj - eot_adj
        
        # Hard safety: Sunrise cannot be at midnight
        if sunrise < 4.0: sunrise = 6.0
        if sunset < 16.0: sunset = 19.5
        
        return float(sunrise), float(sunset)
    except Exception as e:
        logger.error(f"Sun calculation failed: {e}")
        return 6.5, 19.5

def is_floatable(val):
    if pd.isna(val) or val == "" or val is None:
        return False
    try:
        float(str(val).replace(",", "."))
        return True
    except ValueError:
        return False

def format_ora(val):
    """Convert float like 9.25 to '09:25'"""
    if pd.isna(val): return "Unknown"
    try:
        h = int(val)
        m = int(round((val % 1) * 100))
        return f"{h:02d}:{m:02d}"
    except:
        return "Unknown"

def get_production_start_time(ac_df: pd.DataFrame) -> tuple:
    """Find the first and last Ora where production is active and return (start_ora, end_ora, theo_sunset)."""
    if ac_df is None or "Ora" not in ac_df.columns:
        return 7.0, 19.5, 19.5
    
    ac_cols = [c for c in ac_df.columns if "Potenza AC" in c]
    df_clean = ac_df.replace(["x", " x "], np.nan).apply(pd.to_numeric, errors='coerce')
    
    # Dynamic search bound: 30 mins before sunrise
    date_str = ac_df["Data"].iloc[0] if ("Data" in ac_df.columns and not pd.isna(ac_df["Data"].iloc[0])) else datetime.now().strftime("%Y-%m-%d")
    sunrise, sunset = calculate_sunrise(date_str)
    
    # HARD SAFETY: Ignore anything before 5 AM
    search_start = max(5.0, sunrise - 0.5)
    
    prod_start = sunrise
    found_start = False
    for idx, row in df_clean.iterrows():
        ora = float(row.get("Ora", 0))
        if ora < search_start: continue
        
        producing = sum(1 for c in ac_cols if row.get(c, 0) > 300)
        if producing > 15: 
            prod_start = ora
            found_start = True
            break
    
    prod_end = sunset
    found_end = False
    # Backward scan for production end
    for idx, row in df_clean.iloc[::-1].iterrows():
        ora = float(row.get("Ora", 0))
        if ora < prod_start: break
        
        producing = sum(1 for c in ac_cols if row.get(c, 0) > 300)
        if producing > 15:
            prod_end = ora
            found_end = True
            break

    if found_start:
        logger.info(f"Production START at {format_ora(prod_start)} (Sun: {format_ora(sunrise)})")
    if found_end:
        logger.info(f"Production END at {format_ora(prod_end)} (Sun: {format_ora(sunset)})")
            
    return prod_start, prod_end, sunset






def send_telegram_notification(text: str, settings: dict, use_personal: bool = False) -> None:
    """Send a notification via Telegram Bot API."""
    tg = settings.get("telegram", {})
    if not tg.get("enabled", False):
        return

    token = tg.get("bot_token")
    # For personal ID, fallback to chat_id if not present
    chat_id = tg.get("personal_id") if use_personal else tg.get("chat_id")
    if not chat_id: chat_id = tg.get("chat_id")

    if not token or not chat_id:
        logger.warning("Telegram enabled but token/chat_id missing.")
        return

    import requests
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            logger.error(f"Telegram API error: {resp.text}")
    except Exception as e:
        logger.error(f"Failed to send Telegram notification: {e}")


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_metric(date_str: str, metric_prefix: str) -> pd.DataFrame:
    """Load metric from CSV and deduplicate. Handles space/underscore prefixes."""
    # Try both underscore and space versions
    paths = [
        DATA_DIR / f"{metric_prefix}_{date_str}.csv",
        DATA_DIR / f"{metric_prefix.replace('_', ' ')}_{date_str}.csv"
    ]
    
    csv_path = None
    for p in paths:
        if p.exists():
            csv_path = p
            break

    if csv_path:
        try:
            # Load with auto-separator detection
            df = pd.read_csv(str(csv_path), sep=None, engine='python', encoding="utf-8")
            
            # Critical: Data cleaning and deduplication
            if not df.empty and "Ora" in df.columns:
                # Keep the last occurrence (most recent scrape) for each timestamp
                df = df.drop_duplicates(subset=["Ora"], keep="last").reset_index(drop=True)
                
            logger.info(f"Loaded {csv_path.name} ({len(df)} unique rows)")
            return df
        except Exception as e:
            logger.error(f"Failed to load {csv_path.name}: {e}")
            pass

    logger.warning(f"{metric_prefix}_{date_str}.csv not found")
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
                         dc_df: pd.DataFrame, pr_df: pd.DataFrame, irrad_df: pd.DataFrame, daylight_start: float = 7.0, daylight_end: float = 19.5, settings: dict = None) -> dict:
    """
    Compute health flags from the latest available NON-NAN values in each metric file.
    """
    if settings is None:
        settings = DEFAULT_SETTINGS
    thresholds = settings.get("thresholds", DEFAULT_SETTINGS["thresholds"])

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
        else:
            logger.warning(f"PR column or Inverter column not found in PR data. Columns: {pr_df_clean.columns.tolist()}")

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
        
        # Site-wide data drop history
        # Identify internal gaps during active production hours.
        plant_drop_history = []
        if not ac_dedup.empty:
            # Look at everything from start to theoretical end of day
            hist_day = ac_dedup[(ac_dedup["Ora_num"] >= daylight_start) & (ac_dedup["Ora_num"] <= daylight_end)].sort_values("Ora_num")
            
            potential_blocks = []
            for _, row in hist_day.iterrows():
                v_count = sum(1 for c in ac_cols if pd.notna(row.get(c)) and str(row.get(c)).strip().lower() not in ['x', ''])
                if v_count < 2:
                    potential_blocks.append({"ora": row.get("Ora_num"), "time": format_ora(row.get("Ora_num"))})
                else:
                    if len(potential_blocks) >= 3:
                        # Internal gap closed by valid data
                        start_time = potential_blocks[0]["time"]
                        end_time = format_ora(row.get("Ora_num"))
                        duration = len(potential_blocks) * 5 # Approx 5m intervals
                        plant_drop_history.append({
                            "type": "INTERNAL GAP",
                            "start": start_time,
                            "end": end_time,
                            "duration": duration,
                            "time": start_time, # Fallback
                            "ora": potential_blocks[0]["ora"]
                        })
                    potential_blocks = []
            
            # Check for trailing drop
            if len(potential_blocks) >= 3:
                start_time = potential_blocks[0]["time"]
                duration = len(potential_blocks) * 5
                # Only report trailing drop if it starts before 18:00 (prime hours)
                # Otherwise it's likely just end-of-day shutdown.
                if potential_blocks[0]["ora"] < 18.0:
                    plant_drop_history.append({
                        "type": "POST-PRODUCTION DROP",
                        "start": start_time,
                        "end": "still missing",
                        "duration": duration,
                        "time": start_time,
                        "ora": potential_blocks[0]["ora"]
                    })

    else:
        plant_drop_history = []

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

    # -------------------------------------------------------------------
    # NEW AC LOGIC (Dynamic POA + Plant Average)
    # -------------------------------------------------------------------
    avg_ac_power = 0
    poa_val = 0
    ac_valid_vals = []
    
    if ac_row is not None:
        for c in ac_df.columns:
            if "Potenza AC" in c:
                val = ac_row.get(c)
                if pd.notna(val) and str(val).strip().lower() not in ['x', '']:
                    try:
                        ac_valid_vals.append(float(val))
                    except:
                        pass
        if ac_valid_vals:
            avg_ac_power = sum(ac_valid_vals) / len(ac_valid_vals)
            
    sensor_data = {}
    if irrad_df is not None and not irrad_df.empty:
        # 1. Unified POA for AC logic (keep existing poa_val for backward compatibility)
        poa_cols = [c for c in irrad_df.columns if "POA" in c]
        if ac_row is not None:
            ac_ora = ac_row.get("Ora", 0)
            try:
                irrad_copy = irrad_df.copy()
                irrad_copy["Ora_num"] = pd.to_numeric(irrad_copy["Ora"], errors="coerce")
                diff = (irrad_copy["Ora_num"] - ac_ora).abs()
                best_idx = diff.idxmin()
                
                if poa_cols:
                    poa_val = float(irrad_copy.loc[best_idx, poa_cols[0]])
                
                # 2. Extract ALL relevant sensor columns at this timestamp
                # (Excluding Ora and internal numeric columns)
                for col in irrad_df.columns:
                    if col not in ["Ora", "Ora_num"]:
                        val = irrad_copy.loc[best_idx, col]
                        if pd.notna(val) and val != "x":
                            try:
                                sensor_data[col] = float(val)
                            except:
                                sensor_data[col] = val
            except Exception as e:
                logger.error(f"Error mapping irradiance sensors: {e}")
                poa_val = 0

    # Compute health for each inverter
    for inv_id in INVERTER_IDS:
        inv_label = f"INV {inv_id}"
        health = {}

        # PR LED
        pr_val = pr_latest.get(inv_id)
        health["pr_v"] = pr_val
        if pr_val is None:
            health["pr"] = "grey"
        elif pr_val >= thresholds["pr"].get("green", 85.0):
            health["pr"] = "green"
        elif pr_val >= thresholds["pr"].get("yellow", 75.0):
            health["pr"] = "yellow"
        else:
            health["pr"] = "red"

        # Temperature LED
        temp_col = f"Temperatura inverter (INV {inv_id}) [°C]"
        temp_val = None
        if temp_row is not None and temp_col in temp_row.index:
            temp_val = temp_row[temp_col]

        if temp_val is None or pd.isna(temp_val):
            health["temp_v"] = None
            health["temp"] = "grey"
        else:
            health["temp_v"] = float(temp_val)
            if temp_val > thresholds["temp"].get("red", 45.0):
                health["temp"] = "red"
            elif temp_val > thresholds["temp"].get("yellow", 40.0):
                health["temp"] = "yellow"
            else:
                health["temp"] = "green"

        # DC Current LED (average of all MPPTs or total inverter current)
        dc_values = []
        if dc_row is not None:
            # 1. Try finding specific MPPT columns (standard naming)
            for mppt in range(1, 13):
                dc_col = f"Corrente DC MPPT {mppt} (INV {inv_id}) [A]"
                if dc_col in dc_row.index:
                    val = dc_row[dc_col]
                    if val is not None and not pd.isna(val):
                        dc_values.append(val)
            
            # 2. If no MPPT columns found, try finding ANY column with DC and Inv ID
            # This handles 'Corrente DC (INV TX#-##) [A]' or other variants
            if not dc_values:
                for col in dc_row.index:
                    if "Corrente DC" in col and inv_id in col:
                        val = dc_row[col]
                        if val is not None and not pd.isna(val):
                            dc_values.append(val)

        if dc_values:
            avg_dc = float(np.mean(dc_values))
            health["dc_v"] = avg_dc
            # Contextualize to time of day: early morning/evening < afternoon
            if daylight_start <= ora <= 12:  # Morning ramp-up
                dc_threshold_green = thresholds["dc"].get("morning_green", 10.0)
                dc_threshold_yellow = thresholds["dc"].get("morning_yellow", 2.0)
            elif 12 < ora <= DAYLIGHT_END:  # Afternoon decline
                dc_threshold_green = thresholds["dc"].get("afternoon_green", 5.0)
                dc_threshold_yellow = thresholds["dc"].get("afternoon_yellow", 0.5)
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
            health["ac_v"] = None
            health["ac_power"] = "grey"
        else:
            health["ac_v"] = ac_val
            if poa_val < 50 and avg_ac_power < 1000:
                health["ac_power"] = "green" if (daylight_start <= ora <= DAYLIGHT_END) else "grey"
            else:
                if ac_val >= avg_ac_power * 0.85:
                    health["ac_power"] = "green"
                elif ac_val >= avg_ac_power * 0.05:
                    health["ac_power"] = "yellow"
                else:
                    if daylight_start <= ora <= DAYLIGHT_END:
                        health["ac_power"] = "red"  # Effectively tripped (< 5% average)
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

    return inverter_health, plant_drop_history, sensor_data



# ---------------------------------------------------------------------------
# Downtime Tracker
# ---------------------------------------------------------------------------

def compute_downtime(ac_df: pd.DataFrame, irrad_df: pd.DataFrame, daylight_start: float = 7.0, daylight_end: float = 19.5, settings: dict = None) -> dict:
    """Calculate downtime events based on 0.0 W strings during daylight hours."""
    if settings is None:
        settings = {
            "min_downtime_minutes": 9,
            "collection_interval": 15,
            "thresholds": DEFAULT_SETTINGS["thresholds"]
        }
    
    min_downtime_minutes = settings.get("thresholds", DEFAULT_SETTINGS["thresholds"]).get("min_downtime_minutes", 9)

    downtime_tracker = {}
    if ac_df is None or len(ac_df) == 0:
        return downtime_tracker

    # Filter for daylight hours
    if "Ora" not in ac_df.columns:
        return downtime_tracker

    # Deduplicate by Ora (keep last batch's values)
    ac_dedup = ac_df.drop_duplicates(subset=["Ora"], keep="last").copy()
    ac_dedup["Ora"] = pd.to_numeric(ac_dedup["Ora"], errors="coerce")
    
    # HARD FILTER: Ensure we never count downtime before 5:00 AM or after sunset
    d_start = max(5.0, daylight_start)
    mask = (ac_dedup["Ora"] >= d_start) & (ac_dedup["Ora"] <= daylight_end)
    df_day = ac_dedup[mask].copy()


    if len(df_day) == 0:
        return downtime_tracker

    df_day = df_day.replace(["x", " x "], np.nan)
    
    # KEY FIX: The extractor writes empty rows for future timestamps.
    # These all-NaN rows look like "off" periods and cause phantom downtime.
    # Trim df_day to only include rows up to the last row where ANY inverter
    # had real data, removing empty future-timestamp rows before analysis.
    all_ac_cols = [c for c in df_day.columns if "Potenza AC" in c]
    if all_ac_cols:
        any_valid = df_day[all_ac_cols].apply(pd.to_numeric, errors='coerce').notna().any(axis=1)
        valid_rows = any_valid[any_valid].index
        if len(valid_rows) > 0:
            last_valid_idx = valid_rows[-1]
            df_day = df_day.loc[:last_valid_idx].copy()
            logger.debug(f"Trimmed df_day to {len(df_day)} rows (last real data @ {format_ora(df_day['Ora'].iloc[-1])})")
        else:
            return downtime_tracker  # No real data at all — nothing to track
    
    # Get POA array if available
    poa_array = None
    if irrad_df is not None and not irrad_df.empty:
        poa_cols = [c for c in irrad_df.columns if "POA" in c]
        if poa_cols:
            poa_array = irrad_df[poa_cols[0]].replace(["x", " x "], np.nan)
            poa_array = pd.to_numeric(poa_array, errors="coerce")


    def ora_to_minutes(o):
        """Convert VCOM HH.MM float (e.g. 8.30 → 8:30 → 510 min) to total minutes."""
        if pd.isna(o): return 0
        try:
            f = float(o)
            hours = int(f)
            minutes = int(round((f - hours) * 100))
            if minutes >= 60:
                minutes = int(round((f - hours) * 60))
            return hours * 60 + minutes
        except:
            return 0

    # ── Step 1: Determine "plant active" rows ────────────────────────────────
    # A row is "plant active" if ≥ 20 of the 36 inverters are producing > 200 W.
    # This filters out natural startup, shutdown and full-plant outages.
    all_ac_cols = [c for c in df_day.columns if "Potenza AC" in c and "[W]" in c]
    ac_numeric = df_day[all_ac_cols].apply(pd.to_numeric, errors='coerce')
    plant_active = (ac_numeric > 200).sum(axis=1) >= 20   # Series[bool], index = df_day.index

    # Last available POA for display
    last_poa = "—"
    if poa_array is not None and len(poa_array.dropna()) > 0:
        last_poa_val = poa_array.dropna().iloc[-1]
        last_poa = f"{last_poa_val:.1f}"

    # ── Step 2: Per-inverter analysis — only during plant-active windows ─────
    for inv_id in INVERTER_IDS:
        inv_label = f"INV {inv_id}"
        ac_col = f"Potenza AC ({inv_label}) [W]"
        if ac_col not in df_day.columns:
            continue

        data = pd.to_numeric(df_day[ac_col], errors='coerce')

        # An inverter is "off" when it produces ≤50 W or has no data
        inv_off = (data <= 50.0) | (pd.isna(data))

        # Only flag rows where the PLANT is active but THIS inverter is off
        fault = inv_off & plant_active

        fault_indices = fault[fault].index
        if len(fault_indices) == 0:
            continue   # Inverter was fine during all plant-active periods

        # ── Find the last contiguous fault block ────────────────────────────
        blocks = (fault != fault.shift()).cumsum()
        fault_blocks = blocks[fault]
        last_block_id = fault_blocks.iloc[-1]
        last_block_indices = fault_blocks[fault_blocks == last_block_id].index

        idx_start = last_block_indices[0]
        idx_end   = last_block_indices[-1]

        time_stopped = format_ora(df_day.loc[idx_start].get("Ora"))

        # Determine if it recovered after this block
        # Look for a plant-active row WHERE the inverter is also on after idx_end
        after = data.loc[idx_end:]
        recovered_rows = after[(after > 50) & plant_active.loc[idx_end:]]
        if len(recovered_rows) > 0:
            rec_idx = recovered_rows.index[0]
            started_again = format_ora(df_day.loc[rec_idx].get("Ora"))
        else:
            started_again = "still off"

        # Last data timestamp (last row where this inverter had ANY data)
        valid_data_idx = data.dropna().index
        last_data_idx = valid_data_idx[-1] if len(valid_data_idx) > 0 else df_day.index[-1]
        last_ts = format_ora(df_day.loc[last_data_idx].get("Ora"))

        # ── Duration ────────────────────────────────────────────────────────
        stop_min = ora_to_minutes(df_day.loc[idx_start].get("Ora"))
        if started_again == "still off":
            # End = last plant-active row (most recent time we know it was off)
            active_rows = plant_active[plant_active].index
            last_active_idx = active_rows[-1] if len(active_rows) > 0 else idx_end
            end_min = ora_to_minutes(df_day.loc[last_active_idx].get("Ora"))
        else:
            end_min = ora_to_minutes(df_day.loc[rec_idx].get("Ora"))

        total_time_off_calc = end_min - stop_min
        if total_time_off_calc <= 0:
            total_time_off_calc = settings.get("collection_interval", 15)

        if total_time_off_calc >= min_downtime_minutes:
            downtime_tracker[inv_label] = {
                "inverter": inv_label,
                "last_data_fetched": last_ts,
                "last_poa": last_poa,
                "time_stopped": time_stopped,
                "started_again": started_again,
                "total_time_off": int(total_time_off_calc),
            }

    return downtime_tracker


# Macro Health
# ---------------------------------------------------------------------------

def format_duration(minutes):
    """Convert minutes to readable Xh Ym or Xm."""
    if pd.isna(minutes) or minutes is None: return "0m"
    try:
        m_int = int(minutes)
        if m_int < 60:
            return f"{m_int}m"
        h = m_int // 60
        remainder = m_int % 60
        return f"{h}h {remainder}m"
    except:
        return f"{minutes}m"

def compute_macro_health(inverter_health: dict, daylight_start: float = 7.0) -> dict:
    """Compute plant-wide health summary."""
    total = len(inverter_health)
    online = sum(1 for h in inverter_health.values() if h["ac_power"] in ["green", "yellow"])
    tripped = sum(1 for h in inverter_health.values() if h["ac_power"] == "red")
    comms_lost = sum(1 for h in inverter_health.values() if h["ac_power"] == "grey")

    total_ac_w = sum(h.get("ac_v", 0.0) or 0.0 for h in inverter_health.values())
    total_ac_power_mw = total_ac_w / 1_000_000.0

    pr_values = [h.get("pr_v") for h in inverter_health.values() if h.get("pr_v") is not None]
    avg_pr = sum(pr_values) / len(pr_values) if pr_values else 0.0

    return {
        "total_inverters": total,
        "online": online,
        "tripped": tripped,
        "comms_lost": comms_lost,
        "total_ac_power_mw": total_ac_power_mw,
        "avg_pr": avg_pr,
        "plant_start_time": format_ora(daylight_start),
        "last_sync": datetime.now().isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# Main Analysis
# ---------------------------------------------------------------------------

def analyze_site(date_str: str) -> None:
    """Main analysis: load data, compute health, write JSON."""
    plant_drop_history = []
    try:
        logger.info(f"Starting analysis for {date_str}...")

        # Load metrics
        logger.info("Loading metrics...")
        ac_df = load_metric(date_str, "Potenza_AC")
        pr_df = load_metric(date_str, "PR inverter")
        temp_df = load_metric(date_str, "Temperatura")
        dc_df = load_metric(date_str, "Corrente_DC")
        irrad_df = load_metric(date_str, "Irraggiamento")

        if ac_df is None:
            logger.warning(f"Potenza_AC not found for {date_str}")
            return
        # Compute dynamic production start
        daylight_start, actual_sunset, theory_sunset = get_production_start_time(ac_df)
        logger.info(f"Dynamic Daylight Start detected at: {format_ora(daylight_start)}")
        logger.info(f"Dynamic Sunset (actual) at: {format_ora(actual_sunset)}")
        logger.info(f"Dynamic Sunset (theoretical) at: {format_ora(theory_sunset)}")

        settings = load_user_settings()

        # Compute health from latest values (use actual_sunset and theory_sunset for context)
        logger.info("Computing health flags...")
        inverter_health, plant_drop_history, sensor_data = compute_latest_health(date_str, ac_df, temp_df, dc_df, pr_df, irrad_df=irrad_df, daylight_start=daylight_start, daylight_end=actual_sunset, settings=settings)

        macro_health = compute_macro_health(inverter_health, daylight_start=daylight_start)
        macro_health["sunset_time"] = format_ora(theory_sunset)
        
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
        dc_analysis_results = analyze_dc_current(dc_df, md_report_path, date_str)
        
        if isinstance(dc_analysis_results, dict):
            dc_faults = dc_analysis_results.get("faults", [])
            mppt_details = dc_analysis_results.get("mppt_details", {})
        else:
            dc_faults = dc_analysis_results if dc_analysis_results else []
            mppt_details = {}

        # Add MPPT details to inverter_health
        for inv_label, details in mppt_details.items():
            if inv_label in inverter_health:
                inverter_health[inv_label]["mppt_data"] = details
        # -----------------------------------
        
        # Compute Downtime array
        logger.info("Evaluating downtime...")
        downtime_tracker = compute_downtime(ac_df, irrad_df, daylight_start=daylight_start, daylight_end=actual_sunset, settings=settings)


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
        
        tg_messages = []

        # --- Site-Wide Data Drop Alarms ---
        active_site_drop_ids = set()
        for idx, gap in enumerate(plant_drop_history):
            drop_alarm_id = f"SITE_DATA_GAP_{gap['start'].replace(':', '')}"
            active_site_drop_ids.add(drop_alarm_id)
            
            if drop_alarm_id not in prev_alarm_map:
                message = f"Data outage detected from {gap['start']} to {gap['end']} ({gap['duration']} min)."
                if gap['type'] == "POST-PRODUCTION DROP":
                    message = f"Global data outage started at {gap['start']} and has not recovered."
                
                alarm = {
                    "id": drop_alarm_id,
                    "inverter": "SITE",
                    "type": gap['type'],
                    "severity": "red",
                    "trip_time": gap['start'],
                    "message": message
                }
                current_active.append(alarm)
                tg_messages.append(f"📡 *{alarm['type']}*\nSystem: {alarm['inverter']}\nTime: {alarm['trip_time']}\n{alarm['message']}")
            else:
                current_active.append(prev_alarm_map[drop_alarm_id])

        # Recover resolved site drops
        for past_alarm_id, past_alarm in prev_alarm_map.items():
            if past_alarm_id.startswith("SITE_DATA_GAP_") and past_alarm_id not in active_site_drop_ids:
                past_alarm["recovery_time"] = timestamp
                historical_trail.append(past_alarm)
                tg_messages.append(f"✅ *RECOVERED*: {past_alarm['type']}\nSystem: SITE\nTime: {timestamp}")
                
        # Handle the legacy SITE_DATA_DROP id for backward compatibility/cleanup
        if "SITE_DATA_DROP" in prev_alarm_map and "SITE_DATA_DROP" not in active_site_drop_ids:
             prev_alarm = prev_alarm_map["SITE_DATA_DROP"]
             prev_alarm["recovery_time"] = timestamp
             historical_trail.append(prev_alarm)
             tg_messages.append(f"✅ *LEGACY ALERT CLEANUP*: SITE_DATA_DROP recovered.")
        
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
                    alarm = {
                        "id": comms_alarm_id,
                        "inverter": inv_label,
                        "type": "COMMS LOST",
                        "severity": "grey",
                        "trip_time": timestamp,
                        "message": "Missing data for this component."
                    }
                    current_active.append(alarm)
                    tg_messages.append(f"⚠️ *{alarm['type']}*\nInverter: {alarm['inverter']}\nTime: {alarm['trip_time']}\n{alarm['message']}")
                else:
                    # Carry over existing
                    current_active.append(prev_alarm_map[comms_alarm_id])
            else:
                # Recovered?
                if was_comms_lost:
                    prev_alarm = prev_alarm_map[comms_alarm_id]
                    prev_alarm["recovery_time"] = timestamp
                    historical_trail.append(prev_alarm)
                    tg_messages.append(f"✅ *RECOVERED*: {prev_alarm['type']}\nInverter: {prev_alarm['inverter']}\nTime: {timestamp}")

            # --- 2. PR Alarm ---
            pr_val = h.get("raw_pr")
            pr_alarm_id = f"{inv_id}_LOW_PR"
            data_time_str = h.get("data_time", "Unknown")
            is_stabilized = h.get("is_stabilized", True)
            
            was_low_pr = pr_alarm_id in prev_alarm_map
            
            # Trigger alarm only if PR < Yellow threshold and system is stabilized (30m after production start)
            pr_yellow_thresh = settings.get("thresholds", DEFAULT_SETTINGS["thresholds"])["pr"].get("yellow", 75.0)
            if pr_val is not None and pr_val < pr_yellow_thresh and is_stabilized:
                if not was_low_pr:
                    alarm = {
                        "id": pr_alarm_id,
                        "inverter": inv_label,
                        "type": "LOW PR",
                        "severity": "red",
                        "trip_time": data_time_str, # Use ACTUAL time from data
                        "message": f"Performance Ratio dropped to {pr_val:.1f}%"
                    }
                    current_active.append(alarm)
                    tg_messages.append(f"🔴 *{alarm['type']}*\nInverter: {alarm['inverter']}\nTime: {alarm['trip_time']}\n{alarm['message']}")
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
                    tg_messages.append(f"✅ *RECOVERED*: {prev_alarm['type']}\nInverter: {prev_alarm['inverter']}\nTime: {timestamp}")
                    
            # --- 3. AC Power Alarm ---
            ac_alarm_id = f"{inv_id}_LOW_AC"
            was_low_ac = ac_alarm_id in prev_alarm_map
            
            ac_status = h.get("ac_power")
            if ac_status in ["red", "yellow"]:
                if not was_low_ac:
                    alarm = {
                        "id": ac_alarm_id,
                        "inverter": inv_label,
                        "type": "LOW AC POWER" if ac_status == "yellow" else "INVERTER TRIPPED",
                        "severity": ac_status,
                        "trip_time": timestamp,
                        "message": "Power is critically below plant average." if ac_status == "yellow" else "Failed to produce >5% of plant average."
                    }
                    current_active.append(alarm)
                    icon = "🟡" if ac_status == "yellow" else "🔴"
                    tg_messages.append(f"{icon} *{alarm['type']}*\nInverter: {alarm['inverter']}\nTime: {alarm['trip_time']}\n{alarm['message']}")
                else:
                    current_active.append(prev_alarm_map[ac_alarm_id])
            else:
                if was_low_ac:
                    prev_alarm = prev_alarm_map[ac_alarm_id]
                    prev_alarm["recovery_time"] = timestamp
                    historical_trail.append(prev_alarm)
                    tg_messages.append(f"✅ *RECOVERED*: {prev_alarm['type']}\nInverter: {prev_alarm['inverter']}\nTime: {timestamp}")
                    
        # --- 4. DC MPPT Faults ---
        active_dc_fault_ids = set()
        for f in dc_faults:
            alarm_id = f"DC_{f['Inverter']}_MPPT_{f['MPPT']}"
            active_dc_fault_ids.add(alarm_id)
            is_new = alarm_id not in prev_alarm_map
            
            if is_new:
                alarm = {
                    "id": alarm_id,
                    "inverter": f"INV {f['Inverter']}",
                    "type": f['Type'],
                    "severity": "red" if f['Severity'] == "CRITICAL" else "yellow",
                    "trip_time": timestamp,
                    "message": f"MPPT {f['MPPT']} Measured: {f['Measured']}A (Expected: {f['Expected']}A) for {format_duration(f['Duration'])}."
                }
                current_active.append(alarm)
                icon = "🔴" if f['Severity'] == "CRITICAL" else "🟡"
                tg_messages.append(f"{icon} *{alarm['type']}*\nInverter: {alarm['inverter']}\nTime: {alarm['trip_time']}\n{alarm['message']}")
            else:
                current_active.append(prev_alarm_map[alarm_id])
                
        # Recover resolved DC faults
        for past_alarm_id, past_alarm in prev_alarm_map.items():
            if past_alarm_id.startswith("DC_") and past_alarm_id not in active_dc_fault_ids:
                past_alarm["recovery_time"] = timestamp
                historical_trail.append(past_alarm)
                tg_messages.append(f"✅ *RECOVERED*: {past_alarm['type']}\nInverter: {past_alarm['inverter']}\nTime: {timestamp}")

        # Send Telegram updates if any
        for msg in tg_messages:
            send_telegram_notification(msg, settings)

        # Preserve any other active anomalies that we didn't handle in this loop
        handled_ids = set([a["id"] for a in current_active] + [a["id"] for a in historical_trail if "recovery_time" in a])
        for a in active_anomalies_prev:
            if a.get("id") not in handled_ids:
                current_active.append(a)
                
        # Load extraction status for dashboard ingestion cards
        file_status = {}
        if status_path.exists():
            try:
                with open(status_path, "r", encoding="utf-8") as f:
                    estatus = json.load(f)
                    file_status = estatus.get(date_str, {})
            except Exception:
                pass

        snapshot = {
            "macro_health": macro_health,
            "inverter_health": inverter_health,
            "active_anomalies": current_active,
            "historical_trail": historical_trail,
            "downtime_tracker": downtime_tracker,
            "file_status": file_status,
            "sensor_data": sensor_data
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
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
        time.sleep(1)
        self._check_and_analyze()

    def on_modified(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
        self._check_and_analyze()

    def _check_and_analyze(self):
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Updated to match extractor naming conventions (spaces vs underscores)
        required_prefixes = [
            "PR inverter", "Potenza AC", "Corrente DC",
            "Resistenza di isolamento", "Temperatura", "Irraggiamento"
        ]

        missing = []
        for prefix in required_prefixes:
            # Check both space and underscore versions
            p1 = DATA_DIR / f"{prefix}_{today}.csv"
            p2 = DATA_DIR / f"{prefix.replace(' ', '_')}_{today}.csv"
            if not p1.exists() and not p2.exists():
                missing.append(prefix)

        if not missing:
            logger.info(f"Complete set for {today}. Analyzing...")
            analyze_site(today)
        else:
            # Optional: log what's missing every once in a while
            pass


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
