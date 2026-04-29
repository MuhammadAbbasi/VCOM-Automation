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
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from mppt_dc_analyzer import analyze_dc_current
from logging.handlers import RotatingFileHandler

# Ensure UTF-8 for console output on Windows
if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        except Exception:
            pass

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
    },
    "odoo": {
        "enabled": False,
        "url": "http://localhost:8069",
        "db": "odoo_db",
        "user": "admin",
        "password": "api_password_or_key",
        "ticket_model": "helpdesk.ticket",
        "min_duration_minutes": 60,
        "assignments": {
            "TX1": 1,
            "TX2": 2,
            "TX3": 3,
            "DEFAULT": 1
        }
    },
    "alert_preferences": {
        "comm_lost": { "dashboard": True, "telegram": True },
        "plant_drop": { "dashboard": True, "telegram": True },
        "inverter_trip": { "dashboard": True, "telegram": True },
        "ac_drop": { "dashboard": True, "telegram": True },
        "low_pr": { "dashboard": True, "telegram": True },
        "crit_pr": { "dashboard": True, "telegram": True },
        "high_temp": { "dashboard": True, "telegram": True },
        "crit_temp": { "dashboard": True, "telegram": True },
        "dc_warning": { "dashboard": True, "telegram": True },
        "dc_critical": { "dashboard": True, "telegram": True },
        "iso_fault": { "dashboard": True, "telegram": True },
        "grid_limit_change": { "dashboard": True, "telegram": True },
        "tracker_comm": { "dashboard": True, "telegram": True },
        "mqtt_pulse": { "dashboard": True, "telegram": True },
        "recovery": { "telegram": True }
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

# Add SQLite log handler
try:
    from db.db_manager import SQLiteLogHandler
    _sqlite_handler = SQLiteLogHandler(source_name="watchdog")
    _sqlite_handler.setFormatter(logging.Formatter("%(asctime)s [WATCHDOG] %(levelname)s %(message)s"))
    logger.addHandler(_sqlite_handler)
except Exception:
    pass  # DB module may not be ready yet


class NumpyEncoder(json.JSONEncoder):
    """JSON encoder that handles numpy types and Pandas objects."""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

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
        
        return to_hhmm(sunrise), to_hhmm(sunset)
    except Exception as e:
        logger.error(f"Sun calculation failed: {e}")
        return 6.30, 19.30

def to_hhmm(val: float) -> float:
    """Convert decimal hours (6.5) to HH.mm format (6.30)."""
    h = int(val)
    m = int(round((val - h) * 60))
    if m >= 60:
        h += 1
        m = 0
    return h + (m / 100.0)

def is_floatable(val):
    if pd.isna(val) or val == "" or val is None:
        return False
    try:
        float(str(val).replace(",", "."))
        return True
    except ValueError:
        return False

def format_ora(val):
    """Convert float like 9.25 (HH.mm) to '09:25'"""
    if pd.isna(val): return "Unknown"
    try:
        h = int(val)
        m = int(round((val % 1) * 100))
        if m >= 60: # Handle cases like 9.60 or floating errors
            h += 1
            m = 0
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
    """Load metric from the SQLite database. Falls back to CSV if DB has no data."""
    # Try database first
    try:
        from db.db_manager import load_metric as db_load_metric
        df = db_load_metric(date_str, metric_prefix)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.debug(f"[DB] Failed to load {metric_prefix} from DB: {e}")

    # Fallback: try CSV files (backward compatibility)
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
            df = pd.read_csv(str(csv_path), sep=None, engine='python', encoding="utf-8")
            
            if not df.empty and "Ora" in df.columns:
                df = df.drop_duplicates(subset=["Ora"], keep="last").reset_index(drop=True)
                
            logger.info(f"[CSV Fallback] Loaded {csv_path.name} ({len(df)} unique rows)")
            return df
        except Exception as e:
            logger.error(f"Failed to load {csv_path.name}: {e}")
            pass

    logger.warning(f"{metric_prefix}_{date_str} not found in DB or CSV")
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
                         dc_df: pd.DataFrame, pr_df: pd.DataFrame, irrad_df: pd.DataFrame, 
                         iso_df: pd.DataFrame = None,
                         daylight_start: float = 7.0, daylight_end: float = 19.5, settings: dict = None) -> dict:
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
                    if len(potential_blocks) >= 5: # Increased threshold to 5 mins
                        # Internal gap closed by valid data
                        start_time = potential_blocks[0]["time"]
                        end_time = format_ora(row.get("Ora_num"))
                        duration = len(potential_blocks)
                        plant_drop_history.append({
                            "type": "INTERNAL GAP",
                            "start": start_time,
                            "end": end_time,
                            "duration": duration,
                            "time": start_time,
                            "ora": potential_blocks[0]["ora"]
                        })
                    potential_blocks = []
            
            # Check for trailing drop
            if len(potential_blocks) >= 5:
                start_time = potential_blocks[0]["time"]
                duration = len(potential_blocks)
                # Only report trailing drop if it starts before 18:00 (prime hours)
                # AND only if POA is still high (meaning we SHOULD be producing)
                # OR if it's still well before theoretical sunset.
                poa_is_low = (poa_val < 30) if 'poa_val' in locals() else False
                if potential_blocks[0]["ora"] < 18.0 and not poa_is_low and potential_blocks[0]["ora"] < (sunset - 0.5 if 'sunset' in locals() else 19.0):
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

    # Get latest ISO row (deduplicated by Ora)
    iso_row = None
    if iso_df is not None and len(iso_df) > 0:
        iso_cols = [c for c in iso_df.columns if "Resistenza" in c]
        if "Ora" in iso_df.columns:
            iso_dedup = iso_df.drop_duplicates(subset=["Ora"], keep="last").copy()
            iso_dedup["Ora_num"] = pd.to_numeric(iso_dedup["Ora"], errors="coerce")
            iso_dedup = iso_dedup.sort_values("Ora_num", ascending=False)
        else:
            iso_dedup = iso_df.copy()
        
        for _, row in iso_dedup.iterrows():
            iso_values = [row.get(c) for c in iso_cols]
            non_nan_count = sum(1 for v in iso_values if v is not None and not pd.isna(v) and str(v).strip().lower() not in ['x',''])
            if non_nan_count > 10:
                iso_row = row
                break
        if iso_row is None and len(iso_df) > 0:
            iso_row = iso_df.iloc[-1]

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
                irrad_copy.columns = [str(c).encode('ascii', 'ignore').decode('utf-8') for c in irrad_copy.columns]
                irrad_copy["Ora_num"] = pd.to_numeric(irrad_copy["Ora"], errors="coerce")
                
                # Re-calculate poa_cols on sanitized names
                poa_cols = [c for c in irrad_copy.columns if "POA" in c]
                
                diff = (irrad_copy["Ora_num"] - ac_ora).abs()
                best_idx = diff.idxmin()
                
                if poa_cols:
                    val = irrad_copy.loc[best_idx, poa_cols[0]]
                    if is_floatable(val):
                        poa_val = float(str(val).replace(",", "."))
                
                # 2. Extract ALL relevant sensor columns at this timestamp
                # (Excluding Ora and internal numeric columns)
                for col in irrad_copy.columns:
                    if col not in ["Ora", "Ora_num"]:
                        val = irrad_copy.loc[best_idx, col]
                        if pd.notna(val) and val != "x" and not (isinstance(val, str) and "Irraggiamento" in val):
                            try:
                                sensor_data[col] = float(str(val).replace(",", "."))
                            except:
                                sensor_data[col] = val
            except Exception as e:
                logger.error(f"Error mapping irradiance sensors: {e}")
                poa_val = 0

    # Compute health for each inverter
    is_stabilized = True
    if ac_row is not None:
        try:
            ora_val = float(ac_row.get("Ora", 0))
            if ora_val < daylight_start + (STABILIZATION_MINUTES / 60.0):
                is_stabilized = False
            if ora_val > daylight_end - 0.25: # Suppress near sunset
                is_stabilized = False
            if poa_val < 100: # Low light PR is meaningless
                is_stabilized = False
        except:
            pass

    for inv_id in INVERTER_IDS:
        inv_label = f"INV {inv_id}"
        health = {"is_stabilized": is_stabilized}

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

        if ac_col in latest_ac_missing:
            health["comms_lost_flag"] = True
        else:
            health["comms_lost_flag"] = False

        # ISO LED (Insulation Resistance)
        iso_col = next((c for c in iso_row.index if inv_id in c and "Resistenza" in c), None) if iso_row is not None else None
        iso_val = None
        if iso_row is not None and iso_col:
            raw_val = iso_row[iso_col]
            if pd.notna(raw_val) and str(raw_val).strip().lower() not in ['x', '']:
                try:
                    iso_val = float(str(raw_val).replace(",", "."))
                except:
                    iso_val = None
        
        health["iso_v"] = iso_val
        if iso_val is None:
            health["iso"] = "grey"
        elif iso_val < 50:
            health["iso"] = "red"
        else:
            health["iso"] = "green"

        # Overall = worst of 4 LEDs
        scores = []
        score_map = {"green": 0, "yellow": 1, "red": 2, "grey": -1}
        for k in ["pr", "temp", "dc_current", "ac_power", "iso"]:
            score = score_map.get(health[k], -1)
            if score >= 0:
                scores.append(score)

        if health.get("comms_lost_flag"):
            health["overall_status"] = "grey"
            health["ac_power"] = "grey"
        elif ac_val is None or pd.isna(ac_val):
            health["overall_status"] = "no_state"
            health["ac_power"] = "no_state"
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

def compute_macro_health(inverter_health: dict, daylight_start: float = 7.0, ac_df: pd.DataFrame = None) -> dict:
    """Compute plant-wide health summary."""
    total = len(inverter_health)
    online = sum(1 for h in inverter_health.values() if h["ac_power"] in ["green", "yellow"])
    tripped = sum(1 for h in inverter_health.values() if h["ac_power"] == "red")
    comms_lost = sum(1 for h in inverter_health.values() if h["ac_power"] == "grey")
    no_state = sum(1 for h in inverter_health.values() if h["ac_power"] == "no_state")

    total_ac_w = sum(h.get("ac_v", 0.0) or 0.0 for h in inverter_health.values())
    total_ac_power_mw = total_ac_w / 1_000_000.0

    pr_values = [h.get("pr_v") for h in inverter_health.values() if h.get("pr_v") is not None]
    avg_pr = sum(pr_values) / len(pr_values) if pr_values else 0.0

    total_energy_mwh = 0.0
    if ac_df is not None:
        try:
            ac_cols = [c for c in ac_df.columns if "Potenza AC" in c]
            if ac_cols:
                # Calculate total power at each 1-min interval
                total_power_curve = ac_df[ac_cols].apply(pd.to_numeric, errors='coerce').fillna(0).sum(axis=1)
                # Integration (summing minute-by-minute W and dividing by 60 to get Wh)
                total_energy_mwh = total_power_curve.sum() / 60 / 1_000_000
        except Exception as e:
            logger.error(f"Error computing total energy: {e}")

    return {
        "total_inverters": total,
        "online": online,
        "tripped": tripped,
        "comms_lost": comms_lost,
        "no_state": no_state,
        "total_ac_power_mw": total_ac_power_mw,
        "total_energy_mwh": total_energy_mwh,
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
        iso_df = load_metric(date_str, "Resistenza di isolamento")
        attiva_df = load_metric(date_str, "Potenza attiva")

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
        inverter_health, plant_drop_history, sensor_data = compute_latest_health(date_str, ac_df, temp_df, dc_df, pr_df, irrad_df=irrad_df, iso_df=iso_df, daylight_start=daylight_start, daylight_end=actual_sunset, settings=settings)

        macro_health = compute_macro_health(inverter_health, daylight_start=daylight_start, ac_df=ac_df)
        macro_health["sunset_time"] = format_ora(theory_sunset)
        
        # Extract POA from sensor data for header summary
        poa_key = next((k for k in sensor_data if "POA" in k), None)
        poa_val = sensor_data.get(poa_key, 0) if poa_key else 0
        macro_health["poa"] = poa_val # Plane of Array Irradiance
        macro_health["MW"] = macro_health["total_ac_power_mw"] # Alias for chatbot compatibility
        
        # Grid Power Limit Check
        current_grid_limit = 87.6
        if attiva_df is not None and not attiva_df.empty:
            # Priority 1: General limit, Priority 2: Gestore (Network), Priority 3: Terzi
            priority_names = [
                "Valore nominale potenza attiva",
                "Valore nominale della potenza attiva (gestore di rete)",
                "Valore nominale della potenza attiva (terzi)"
            ]
            
            limit_col = None
            for p_name in priority_names:
                found = [c for c in attiva_df.columns if p_name in c]
                if found:
                    limit_col = found[0]
                    break
            
            if not limit_col:
                # Fallback to any column containing "Valore nominale" and "potenza attiva"
                limit_cols = [c for c in attiva_df.columns if "Valore nominale" in c and "potenza attiva" in c.lower()]
                if limit_cols:
                    limit_col = limit_cols[0]

            if limit_col:
                logger.info(f"[GRID] Using limit column: {limit_col}")
                # Get the latest non-null value
                temp_df = attiva_df.dropna(subset=[limit_col])
                if not temp_df.empty:
                    latest_row = temp_df.iloc[-1]
                    try:
                        val_raw = latest_row[limit_col]
                        # Handle strings like "87,6 %"
                        if isinstance(val_raw, str):
                            val_str = val_raw.replace("%", "").replace(",", ".").strip()
                            current_grid_limit = float(val_str)
                        else:
                            current_grid_limit = float(val_raw)
                        
                        logger.info(f"[GRID] Detected Grid Limit: {current_grid_limit}% (from {limit_col})")
                    except Exception as e:
                        logger.warning(f"[GRID] Failed to parse grid limit value '{val_raw}': {e}")
                else:
                    logger.warning(f"[GRID] Column '{limit_col}' has no non-null values.")
        
        macro_health["grid_limit"] = current_grid_limit
        
        # Latest sync from extraction status if available
        try:
            from db.db_manager import get_extraction_status
            estatus = get_extraction_status(date_str)
            if estatus:
                ts_list = [v["timestamp"] for v in estatus.values() if "timestamp" in v]
                if ts_list:
                    macro_health["last_data_fetch"] = max(ts_list)
        except Exception:
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
        
        # Load previous state from database
        try:
            from db.db_manager import load_latest_snapshot
            last_snap = load_latest_snapshot(date_str)
        except Exception:
            last_snap = None
                
        historical_trail = []
        active_anomalies_prev = []
        
        if last_snap:
            historical_trail = last_snap.get("historical_trail", [])
            active_anomalies_prev = last_snap.get("active_anomalies", [])
            
        current_active = []
        
        # We need a quick lookup of previous alarms
        prev_alarm_map = {a.get("id"): a for a in active_anomalies_prev}
        
        tg_groups = {} # { "Category": [ "Message line 1", "Message line 2" ] }

        def add_tg_msg(category, line):
            if category not in tg_groups: tg_groups[category] = []
            if line not in tg_groups[category]:
                tg_groups[category].append(line)

        # --- USER ALARM PREFERENCES ---
        prefs = settings.get("alert_preferences", {})
        def should_alert(category, target):
            # Default to True if preference is missing
            return prefs.get(category, {}).get(target, True)

        # --- TELEGRAM RE-FIRE RULES ---
        # To avoid spamming the technician:
        # Critical (red): re-fire every 2 hours while active.
        # Non-critical (yellow/grey): re-fire every 4 hours while active.
        now_dt = datetime.fromisoformat(timestamp)
        TG_REFIRE_CRIT_SEC = 7200    # 2 hours
        TG_REFIRE_NONCRIT_SEC = 14400 # 4 hours

        def should_send_tg(alarm: dict) -> bool:
            """Decide whether to push a Telegram message for this active alarm now."""
            last_sent = alarm.get("last_tg_sent")
            if not last_sent:
                return True  # First time notifying
            
            severity = alarm.get("severity", "yellow")
            threshold = TG_REFIRE_CRIT_SEC if severity == "red" else TG_REFIRE_NONCRIT_SEC
            
            try:
                last_dt = datetime.fromisoformat(last_sent)
                return (now_dt - last_dt).total_seconds() >= threshold
            except Exception:
                return True

        def fire_tg(alarm: dict, category: str, line: str) -> None:
            """Queue a Telegram line and stamp the alarm with the send time."""
            # Only send critical (red) alarms or urgent grid alerts to Telegram to reduce noise
            severity = alarm.get("severity", "yellow")
            if severity == "red" or "GRID" in category:
                add_tg_msg(category, line)
            
            alarm["last_tg_sent"] = timestamp

        # Track ALL evaluated IDs to prevent filtered alerts from leaking back in via cleanup loop
        checked_ids = set()

        # --- Site-Wide Data Drop Alarms ---
        active_site_drop_ids = set()
        # Handle site data drop alerts with preferences
        site_comm_db = should_alert("plant_drop", "dashboard")
        site_comm_tg = should_alert("plant_drop", "telegram")

        for idx, gap in enumerate(plant_drop_history):
            drop_alarm_id = f"SITE_DATA_GAP_{gap['start'].replace(':', '')}"
            active_site_drop_ids.add(drop_alarm_id)
            checked_ids.add(drop_alarm_id)

            message = f"Data outage detected from {gap['start']} to {gap['end']} ({gap['duration']} min)."
            if gap['type'] == "POST-PRODUCTION DROP":
                # Suppress if irradiance is low (likely evening)
                poa = macro_health.get("poa", 100) 
                if poa is not None and poa < 10:
                    continue
                message = f"Global data outage started at {gap['start']} and has not recovered."

            if drop_alarm_id in prev_alarm_map:
                alarm = prev_alarm_map[drop_alarm_id]
                alarm["message"] = message  # refresh duration/end time
            else:
                alarm = {
                    "id": drop_alarm_id,
                    "inverter": "SITE",
                    "type": gap['type'],
                    "severity": "red",
                    "trip_time": gap['start'],
                    "message": message
                }

            if site_comm_db:
                current_active.append(alarm)
            if site_comm_tg and should_send_tg(alarm):
                fire_tg(alarm, alarm['type'], f"📡 *{alarm['type']}* (SITE)\n{alarm['message']}")

        # Recover resolved site drops
        for past_alarm_id, past_alarm in prev_alarm_map.items():
            if past_alarm_id.startswith("SITE_DATA_GAP_") and past_alarm_id not in active_site_drop_ids:
                past_alarm["recovery_time"] = timestamp
                historical_trail.append(past_alarm)
                checked_ids.add(past_alarm_id)
                if should_alert("recovery", "telegram"):
                    add_tg_msg("RECOVERED", f"✅ {past_alarm['type']} (SITE)")
                
        # Retire the legacy SITE_DATA_DROP id (flat id, never re-created)
        if "SITE_DATA_DROP" in prev_alarm_map:
            checked_ids.add("SITE_DATA_DROP")
            prev_alarm = prev_alarm_map["SITE_DATA_DROP"]
            prev_alarm["recovery_time"] = timestamp
            historical_trail.append(prev_alarm)
            if should_alert("recovery", "telegram"):
                add_tg_msg("RECOVERED", f"✅ SITE DATA DROP (legacy cleanup)")

        # --- Grid Limit Change Alarm ---
        grid_alarm_id = "GRID_LIMIT_CHANGE"
        checked_ids.add(grid_alarm_id)

        # Standard is 87.6% (maximum allowed for this plant)
        STANDARD_LIMIT = 87.6
        current_limit = macro_health.get("grid_limit", STANDARD_LIMIT)
        
        # We alert if the limit drops BELOW 87.6%
        if current_limit < (STANDARD_LIMIT - 0.01):
            if grid_alarm_id in prev_alarm_map:
                alarm = prev_alarm_map[grid_alarm_id]
                alarm["message"] = f"Grid production limit is restricted to {current_limit:.1f}% (Below plant max of 87.6%)."
                alarm["severity"] = "red" # Critical as per user request
            else:
                alarm = {
                    "id": grid_alarm_id,
                    "inverter": "GRID",
                    "type": "GRID LIMIT CHANGE",
                    "severity": "red", # Critical as per user request
                    "trip_time": timestamp,
                    "message": f"Grid production limit dropped to {current_limit:.1f}% (Below plant max of 87.6%)."
                }
            if should_alert("grid_limit_change", "dashboard"):
                current_active.append(alarm)
            if should_alert("grid_limit_change", "telegram") and should_send_tg(alarm):
                fire_tg(alarm, "URGENT: GRID LIMIT", f"🚨 *CRITICAL: GRID LIMIT DROP*\nLimit is now *{current_limit:.1f}%* (Below max allowed 87.6%)")
        else:
            # Recovery if it goes back to 87.6% or above
            if grid_alarm_id in prev_alarm_map:
                past_alarm = prev_alarm_map[grid_alarm_id]
                past_alarm["recovery_time"] = timestamp
                historical_trail.append(past_alarm)
                checked_ids.add(grid_alarm_id)
                # Only send recovery for grid if it was a real restriction
                if should_alert("recovery", "telegram"):
                    add_tg_msg("RECOVERED", f"✅ GRID LIMIT restored to {current_limit:.1f}%")

        # --- MQTT Pulse Alert ---
        mqtt_alarm_id = "MQTT_PULSE_LOST"
        checked_ids.add(mqtt_alarm_id)
        
        link_status_path = ROOT / "db" / "link_status.json"
        link_info = {"status": "offline"}
        if link_status_path.exists():
            try:
                with open(link_status_path, "r") as f:
                    link_info = json.load(f)
                if "last_heartbeat" in link_info:
                    last_ts = datetime.fromisoformat(link_info["last_heartbeat"])
                    if (datetime.now() - last_ts).total_seconds() > 300: # 5 minutes
                        link_info["status"] = "stale"
            except:
                pass
        
        if link_info["status"] != "online":
            msg = "MQTT Bridge Link is OFFLINE." if link_info["status"] == "offline" else "MQTT Bridge Link is STALE (last data > 5m ago)."
            if mqtt_alarm_id in prev_alarm_map:
                alarm = prev_alarm_map[mqtt_alarm_id]
                alarm["message"] = msg
            else:
                alarm = {
                    "id": mqtt_alarm_id,
                    "inverter": "MQTT",
                    "type": "MQTT LINK LOST",
                    "severity": "red",
                    "trip_time": timestamp,
                    "message": msg
                }
            if should_alert("mqtt_pulse", "dashboard"):
                current_active.append(alarm)
            if should_alert("mqtt_pulse", "telegram") and should_send_tg(alarm):
                fire_tg(alarm, "MQTT PULSE", f"📡 *MQTT LINK {link_info['status'].upper()}*")
        else:
            if mqtt_alarm_id in prev_alarm_map:
                past_alarm = prev_alarm_map[mqtt_alarm_id]
                past_alarm["recovery_time"] = timestamp
                historical_trail.append(past_alarm)
                checked_ids.add(mqtt_alarm_id)
                if should_alert("recovery", "telegram"):
                    add_tg_msg("RECOVERED", f"✅ MQTT LINK restored")

        # --- Tracker Comms Alert ---
        try:
            from db.db_manager import get_all_tracker_status
            trackers = get_all_tracker_status()
            if trackers:
                TOTAL_TRACKERS = 370
                connected_count = len([t for t in trackers if t.get("mode")])
                
                if connected_count < TOTAL_TRACKERS * 0.9: # More than 10% offline
                    tracker_alarm_id = "TRACKER_MASS_OFFLINE"
                    checked_ids.add(tracker_alarm_id)
                    
                    msg = f"{TOTAL_TRACKERS - connected_count} trackers are offline or missing data."
                    if tracker_alarm_id in prev_alarm_map:
                        alarm = prev_alarm_map[tracker_alarm_id]
                        alarm["message"] = msg
                    else:
                        alarm = {
                            "id": tracker_alarm_id,
                            "inverter": "TRACKER",
                            "type": "TRACKER MASS OFFLINE",
                            "severity": "yellow",
                            "trip_time": timestamp,
                            "message": msg
                        }
                    if should_alert("tracker_comm", "dashboard"):
                        current_active.append(alarm)
                    if should_alert("tracker_comm", "telegram") and should_send_tg(alarm):
                        fire_tg(alarm, "TRACKER COMMS", f"🛰️ *TRACKER ALERT*\n{msg}")
                else:
                    if "TRACKER_MASS_OFFLINE" in prev_alarm_map:
                        past_alarm = prev_alarm_map["TRACKER_MASS_OFFLINE"]
                        past_alarm["recovery_time"] = timestamp
                        historical_trail.append(past_alarm)
                        checked_ids.add("TRACKER_MASS_OFFLINE")
                        if should_alert("recovery", "telegram") and past_alarm.get("severity") == "red":
                            add_tg_msg("RECOVERED", f"✅ Tracker connectivity restored")
        except Exception as e:
            logger.debug(f"Tracker alert check failed: {e}")

        # --- Per-Inverter Alarms ---
        thresh = settings.get("thresholds", DEFAULT_SETTINGS["thresholds"])
        pr_yellow_thresh = thresh["pr"].get("yellow", 75.0)
        pr_green_thresh  = thresh["pr"].get("green", 85.0)

        for inv_id in INVERTER_IDS:
            inv_label = f"INV {inv_id}"
            h = inverter_health.get(inv_label, {})

            # --- 1. COMMS LOST ---
            comms_alarm_id = f"{inv_id}_COMMS_LOST"
            checked_ids.add(comms_alarm_id)
            is_comms_lost = h.get("comms_lost_flag", False)

            if is_comms_lost:
                if comms_alarm_id in prev_alarm_map:
                    alarm = prev_alarm_map[comms_alarm_id]
                else:
                    alarm = {
                        "id": comms_alarm_id,
                        "inverter": inv_label,
                        "type": "COMMS LOST",
                        "severity": "grey",
                        "trip_time": timestamp,
                        "message": "Missing data for this inverter."
                    }
                if should_alert("comm_lost", "dashboard"):
                    current_active.append(alarm)
                if should_alert("comm_lost", "telegram") and should_send_tg(alarm):
                    fire_tg(alarm, "COMMS LOST", f"⚠️ {inv_label} — no data")
            else:
                if comms_alarm_id in prev_alarm_map:
                    prev_alarm = prev_alarm_map[comms_alarm_id]
                    prev_alarm["recovery_time"] = timestamp
                    historical_trail.append(prev_alarm)
                    if should_alert("recovery", "telegram") and prev_alarm.get("severity") == "red":
                        add_tg_msg("RECOVERED", f"✅ COMMS LOST ({inv_label})")

            # --- 2. PR Alarm ---
            pr_alarm_id = f"{inv_id}_LOW_PR"
            checked_ids.add(pr_alarm_id)
            pr_val = h.get("raw_pr")
            is_stabilized = h.get("is_stabilized", True)

            if pr_val is not None and is_stabilized and pr_val < pr_green_thresh:
                is_critical_pr = pr_val < pr_yellow_thresh
                pr_cat      = "crit_pr" if is_critical_pr else "low_pr"
                pr_severity = "red"    if is_critical_pr else "yellow"
                pr_type     = "CRITICAL PR" if is_critical_pr else "LOW PR"

                if pr_alarm_id in prev_alarm_map:
                    alarm = prev_alarm_map[pr_alarm_id]
                    alarm["severity"] = pr_severity
                    alarm["type"]     = pr_type
                    alarm["message"]  = f"PR is {pr_val:.1f}% (Threshold: {pr_yellow_thresh}%)"
                else:
                    alarm = {
                        "id": pr_alarm_id,
                        "inverter": inv_label,
                        "type": pr_type,
                        "severity": pr_severity,
                        "trip_time": timestamp,
                        "message": f"PR is {pr_val:.1f}% (Threshold: {pr_yellow_thresh}%)",
                        "pref_category": pr_cat
                    }
                if should_alert(pr_cat, "dashboard"):
                    current_active.append(alarm)
                if should_alert(pr_cat, "telegram") and should_send_tg(alarm):
                    icon = "🚨" if is_critical_pr else "⚡"
                    fire_tg(alarm, pr_type, f"{icon} {inv_label} — PR *{pr_val:.1f}%*")
            else:
                if pr_alarm_id in prev_alarm_map:
                    prev_alarm = prev_alarm_map[pr_alarm_id]
                    prev_alarm["recovery_time"] = timestamp
                    historical_trail.append(prev_alarm)
                    orig_cat = prev_alarm.get("pref_category", "low_pr")
                    orig_sev = prev_alarm.get("severity", "yellow")
                    if should_alert("recovery", "telegram") and should_alert(orig_cat, "telegram"):
                        # Only send recovery to telegram if the original alert was critical (red)
                        if orig_sev == "red":
                            add_tg_msg("RECOVERED", f"✅ {prev_alarm['type']} ({inv_label})")

            # --- 3. AC Power Alarm ---
            ac_alarm_id = f"{inv_id}_LOW_AC"
            checked_ids.add(ac_alarm_id)
            ac_status = h.get("ac_power")

            if ac_status in ["red", "yellow"]:
                ac_cat  = "inverter_trip" if ac_status == "red" else "ac_drop"
                ac_type = "INVERTER TRIPPED" if ac_status == "red" else "LOW AC POWER"
                ac_msg  = "Producing <5% of plant average — likely tripped." if ac_status == "red" else "Power significantly below plant average."

                if ac_alarm_id in prev_alarm_map:
                    alarm = prev_alarm_map[ac_alarm_id]
                else:
                    alarm = {
                        "id": ac_alarm_id,
                        "inverter": inv_label,
                        "type": ac_type,
                        "severity": ac_status,
                        "trip_time": timestamp,
                        "message": ac_msg,
                        "pref_category": ac_cat
                    }
                if should_alert(ac_cat, "dashboard"):
                    current_active.append(alarm)
                if should_alert(ac_cat, "telegram") and should_send_tg(alarm):
                    icon = "🔴" if ac_status == "red" else "🟡"
                    fire_tg(alarm, ac_type, f"{icon} {inv_label}")
            else:
                if ac_alarm_id in prev_alarm_map:
                    prev_alarm = prev_alarm_map[ac_alarm_id]
                    prev_alarm["recovery_time"] = timestamp
                    historical_trail.append(prev_alarm)
                    orig_cat = prev_alarm.get("pref_category", "ac_drop")
                    orig_sev = prev_alarm.get("severity", "yellow")
                    if should_alert("recovery", "telegram") and should_alert(orig_cat, "telegram"):
                        # Only send recovery for critical trips
                        if orig_sev == "red":
                            add_tg_msg("RECOVERED", f"✅ {prev_alarm['type']} ({inv_label})")

            # --- 4. Temperature Alarm ---
            temp_alarm_id = f"TEMP_{inv_id}"
            checked_ids.add(temp_alarm_id)
            temp_status = h.get("temp")
            temp_val    = h.get("temp_v")

            if temp_status in ["red", "yellow"]:
                temp_cat  = "crit_temp" if temp_status == "red" else "high_temp"
                temp_type = "CRITICAL TEMP" if temp_status == "red" else "HIGH TEMP"

                if temp_alarm_id in prev_alarm_map:
                    alarm = prev_alarm_map[temp_alarm_id]
                    alarm["severity"] = temp_status
                    alarm["type"]     = temp_type
                    alarm["message"]  = f"Temperature {temp_status}: {temp_val}°C"
                else:
                    alarm = {
                        "id": temp_alarm_id,
                        "inverter": inv_label,
                        "type": temp_type,
                        "severity": temp_status,
                        "trip_time": timestamp,
                        "message": f"Temperature {temp_status}: {temp_val}°C",
                        "pref_category": temp_cat
                    }
                if should_alert(temp_cat, "dashboard"):
                    current_active.append(alarm)
                if should_alert(temp_cat, "telegram") and should_send_tg(alarm):
                    icon = "🔥" if temp_status == "red" else "🌡️"
                    fire_tg(alarm, temp_type, f"{icon} {inv_label} — *{temp_val}°C*")
            else:
                if temp_alarm_id in prev_alarm_map:
                    prev_alarm = prev_alarm_map[temp_alarm_id]
                    prev_alarm["recovery_time"] = timestamp
                    historical_trail.append(prev_alarm)
                    orig_cat = prev_alarm.get("pref_category", "high_temp")
                    orig_sev = prev_alarm.get("severity", "yellow")
                    if should_alert("recovery", "telegram") and should_alert(orig_cat, "telegram"):
                        if orig_sev == "red":
                            add_tg_msg("RECOVERED", f"✅ {prev_alarm['type']} ({inv_label})")

            # --- 5. Insulation Resistance (ISO) Alarm ---
            iso_alarm_id = f"ISO_{inv_id}"
            checked_ids.add(iso_alarm_id)
            iso_status = h.get("iso")
            iso_val    = h.get("iso_v")

            if iso_status == "red":
                if iso_alarm_id in prev_alarm_map:
                    alarm = prev_alarm_map[iso_alarm_id]
                    alarm["message"] = f"ISO resistance critically low: {iso_val} kΩ (Threshold: 50 kΩ)"
                else:
                    alarm = {
                        "id": iso_alarm_id,
                        "inverter": inv_label,
                        "type": "INSULATION FAULT",
                        "severity": "red",
                        "trip_time": timestamp,
                        "message": f"ISO resistance critically low: {iso_val} kΩ (Threshold: 50 kΩ)"
                    }
                if should_alert("iso_fault", "dashboard"):
                    current_active.append(alarm)
                if should_alert("iso_fault", "telegram") and should_send_tg(alarm):
                    fire_tg(alarm, "INSULATION FAULT", f"🔌 *INSULATION FAULT* ({inv_label})\nValue: *{iso_val} kΩ* — field inspection required")
            else:
                if iso_alarm_id in prev_alarm_map:
                    prev_alarm = prev_alarm_map[iso_alarm_id]
                    prev_alarm["recovery_time"] = timestamp
                    historical_trail.append(prev_alarm)
                    if should_alert("recovery", "telegram"):
                        add_tg_msg("RECOVERED", f"✅ INSULATION FAULT ({inv_label})")

        # --- DC MPPT Faults Aggregation ---
        active_dc_fault_ids = set()
        inv_dc_summary = {} # { "INV TX1-05": { "crit": [], "warn": [], "alarms": [] } }
        
        for f in dc_faults:
            inv_id_f    = f["Inverter"]
            inv_label_f = f"INV {inv_id_f}"
            dc_alarm_id = f"DC_{inv_id_f}_MPPT_{f['MPPT']}"
            active_dc_fault_ids.add(dc_alarm_id)
            checked_ids.add(dc_alarm_id)

            is_crit  = f['Severity'] == "CRITICAL"
            dc_cat   = "dc_critical" if is_crit else "dc_warning"
            dc_type  = "DC CRITICAL" if is_crit else "DC WARNING"
            
            if inv_label_f not in inv_dc_summary:
                inv_dc_summary[inv_label_f] = {"crit": [], "warn": [], "alarms": []}
            
            if dc_alarm_id in prev_alarm_map:
                alarm = prev_alarm_map[dc_alarm_id]
            else:
                alarm = {
                    "id": dc_alarm_id,
                    "inverter": inv_label_f,
                    "type": dc_type,
                    "severity": "red" if is_crit else "yellow",
                    "trip_time": timestamp
                }
            
            if should_alert(dc_cat, "dashboard"):
                current_active.append(alarm)
            
            if should_alert(dc_cat, "telegram") and should_send_tg(alarm):
                if is_crit:
                    inv_dc_summary[inv_label_f]["crit"].append(str(f['MPPT']))
                else:
                    inv_dc_summary[inv_label_f]["warn"].append(str(f['MPPT']))
                inv_dc_summary[inv_label_f]["alarms"].append((alarm, dc_type))

        # Send aggregated DC alerts
        for inv_label, details in inv_dc_summary.items():
            if details["crit"]:
                mppt_list = ", ".join(details["crit"])
                msg = f"⚡ {inv_label} — CRITICAL DC (MPPTs: {mppt_list})"
                add_tg_msg("DC CRITICAL", msg)
                for alarm, dtype in details["alarms"]:
                    if alarm["severity"] == "red": alarm["last_tg_sent"] = timestamp
            if details["warn"]:
                mppt_list = ", ".join(details["warn"])
                msg = f"🔌 {inv_label} — DC Warning (MPPTs: {mppt_list})"
                add_tg_msg("DC WARNING", msg)
                for alarm, dtype in details["alarms"]:
                    if alarm["severity"] != "red": alarm["last_tg_sent"] = timestamp

        # Recover resolved DC faults
        for past_alarm_id, past_alarm in prev_alarm_map.items():
            if past_alarm_id.startswith("DC_") and past_alarm_id not in active_dc_fault_ids:
                past_alarm["recovery_time"] = timestamp
                historical_trail.append(past_alarm)
                checked_ids.add(past_alarm_id)
                if should_alert("recovery", "telegram") and past_alarm.get("severity") == "red":
                    add_tg_msg("RECOVERED", f"✅ {past_alarm['type']} ({past_alarm['inverter']})")

        # Final recovery grouping
        if "RECOVERED" in tg_groups:
            try:
                import re # Local import to be absolutely safe
                raw_rec = tg_groups["RECOVERED"]
                grouped_rec = []
                type_map = {} # { "TYPE": [ "INV1", "INV2" ] }
                
                for msg in raw_rec:
                    match = re.search(r'✅ (.*) \((.*)\)', msg)
                    if match:
                        atype, target = match.groups()
                        if atype not in type_map: type_map[atype] = []
                        if target not in type_map[atype]: type_map[atype].append(target)
                    else:
                        grouped_rec.append(msg)
                
                for atype, targets in type_map.items():
                    if len(targets) > 3:
                        grouped_rec.append(f"✅ {atype} ({len(targets)} components recovered)")
                    else:
                        grouped_rec.append(f"✅ {atype} ({', '.join(targets)})")
                tg_groups["RECOVERED"] = grouped_rec
            except Exception as e:
                logger.error(f"Error grouping recoveries: {e}")

        # Send Telegram updates grouped by type
        for category, lines in tg_groups.items():
            combined_msg = f"🔔 *{category}* ({timestamp})\n\n" + "\n".join(lines)
            send_telegram_notification(combined_msg, settings)

        # Preserve any other active anomalies that we didn't handle in this loop
        # (e.g. manually added alerts or categories not yet in the forensic loop)
        for a in active_anomalies_prev:
            aid = a.get("id")
            if aid not in checked_ids:
                current_active.append(a)
                
        # Load extraction status for dashboard ingestion cards
        file_status = {}
        try:
            from db.db_manager import get_extraction_status
            file_status = get_extraction_status(date_str)
        except Exception:
            pass

        # --- Odoo Integration ---
        odoo_cfg = settings.get("odoo", {})
        if odoo_cfg.get("enabled"):
            try:
                from db.odoo_client import OdooClient
                client = OdooClient(odoo_cfg["url"], odoo_cfg["db"], odoo_cfg["user"], odoo_cfg["password"])
                min_dur = odoo_cfg.get("min_duration_minutes", 60)
                
                for alarm in current_active:
                    if alarm.get("odoo_ticket_id"):
                        continue
                    
                    trip_time_str = alarm.get("trip_time", "")
                    if not trip_time_str: continue
                    
                    try:
                        if "T" in trip_time_str:
                            trip_dt = datetime.fromisoformat(trip_time_str)
                        else:
                            trip_dt = datetime.strptime(f"{date_str} {trip_time_str}", "%Y-%m-%d %H:%M")
                        
                        duration = (datetime.now() - trip_dt).total_seconds() / 60
                        if duration >= min_dur:
                            inv = alarm.get("inverter", "DEFAULT")
                            assignee_id = odoo_cfg.get("assignments", {}).get("DEFAULT", 1)
                            for key, uid in odoo_cfg.get("assignments", {}).items():
                                if key in inv:
                                    assignee_id = uid
                                    break
                            
                            ticket_vals = {
                                "name": f"[{alarm['type']}] {alarm['inverter']} - {alarm['message'][:50]}",
                                "description": (f"SCADA ALERT\n-----------\n"
                                               f"Inverter: {alarm['inverter']}\n"
                                               f"Type: {alarm['type']}\n"
                                               f"Severity: {alarm['severity']}\n"
                                               f"Message: {alarm['message']}\n"
                                               f"Duration: {int(duration)} minutes"),
                                "user_id": assignee_id,
                                "team_id": 1 # Optional: Helpdesk team
                            }
                            
                            # Add priority mapping
                            if "priority" in [c[0] for c in client.models.execute_kw(odoo_cfg["db"], 1, odoo_cfg["password"], odoo_cfg["ticket_model"], 'fields_get', [], {'attributes': ['name']})]:
                                ticket_vals["priority"] = "3" if alarm['severity'] == "red" else "1"

                            ticket_id = client.create_ticket(odoo_cfg["ticket_model"], ticket_vals)
                            if ticket_id:
                                alarm["odoo_ticket_id"] = ticket_id
                                logger.info(f"Created Odoo ticket #{ticket_id} for {alarm['id']}")
                    except Exception as ex:
                        logger.debug(f"Duration check failed for {alarm['id']}: {ex}")
            except Exception as e:
                logger.error(f"Odoo integration error: {e}")

        snapshot = {
            "macro_health": macro_health,
            "inverter_health": inverter_health,
            "active_anomalies": current_active,
            "historical_trail": historical_trail,
            "downtime_tracker": downtime_tracker,
            "file_status": file_status,
            "sensor_data": sensor_data
        }

        # Save snapshot to database
        try:
            from db.db_manager import save_analysis_snapshot
            save_analysis_snapshot(date_str, timestamp, snapshot)
            logger.info(f"Saved analysis snapshot to DB for {date_str} at {timestamp}")
        except Exception as e:
            logger.error(f"Failed to save snapshot to DB: {e}")
            # Fallback: write JSON file
            json_path = DATA_DIR / f"dashboard_data_{date_str}.json"
            existing_data = {}
            if json_path.exists():
                try:
                    with open(json_path, "r", encoding="utf-8") as f:
                        existing_data = json.load(f)
                except Exception:
                    pass
            existing_data[timestamp] = snapshot
            timestamps_sorted = sorted(existing_data.keys())
            if len(timestamps_sorted) > 50:
                for old_ts in timestamps_sorted[:-50]:
                    del existing_data[old_ts]
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(existing_data, f, indent=2, cls=NumpyEncoder)
            logger.warning(f"[FALLBACK] Wrote JSON: {json_path}")

        # Log summary
        logger.info(f"Health: {macro_health['online']} online, {macro_health['tripped']} tripped, "
                   f"{macro_health['comms_lost']} comms_lost")

    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# File Watcher
# ---------------------------------------------------------------------------

class MetricFileHandler(FileSystemEventHandler):
    """Watches for database or CSV file changes and triggers analysis."""
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".csv") or event.src_path.endswith(".db"):
            time.sleep(1)
            self._check_and_analyze()

    def on_modified(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".csv") or event.src_path.endswith(".db"):
            self._check_and_analyze()

    def _check_and_analyze(self):
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Check the database for available metrics first
        try:
            from db.db_manager import get_extraction_status
            estatus = get_extraction_status(today)
            if len(estatus) >= 5:  # At least 5 of 6 metrics extracted
                logger.info(f"DB has {len(estatus)} metrics for {today}. Analyzing...")
                analyze_site(today)
                return
        except Exception:
            pass

        # Fallback: check CSV files
        required_prefixes = [
            "PR inverter", "Potenza AC", "Corrente DC",
            "Resistenza di isolamento", "Temperatura", "Irraggiamento"
        ]

        missing = []
        for prefix in required_prefixes:
            p1 = DATA_DIR / f"{prefix}_{today}.csv"
            p2 = DATA_DIR / f"{prefix.replace(' ', '_')}_{today}.csv"
            if not p1.exists() and not p2.exists():
                missing.append(prefix)

        if not missing:
            logger.info(f"Complete CSV set for {today}. Analyzing...")
            analyze_site(today)


def main():
    # Initialize databases on startup
    try:
        from db.db_manager import init_databases
        init_databases()
        logger.info("Databases initialized.")
    except Exception as e:
        logger.warning(f"Could not initialize databases: {e}")

    logger.info("Starting VCOM Watchdog (Final)...")
    logger.info(f"Monitoring: {DATA_DIR}")

    handler = MetricFileHandler()
    observer = Observer()
    observer.schedule(handler, str(DATA_DIR), recursive=False)
    
    # Also watch the DB directory for changes
    db_dir = ROOT / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    observer.schedule(handler, str(db_dir), recursive=False)
    
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
