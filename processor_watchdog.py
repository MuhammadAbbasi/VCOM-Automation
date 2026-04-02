"""
processor_watchdog.py — File-system watcher + forensic analyser.

Monitors extracted_data/ for new/modified Excel files.  When all 6 daily
metric files are present, triggers analyze_site() which:
  1. Loads and merges all 6 DataFrames
  2. Applies 6 forensic rules to every inverter × timestep
  3. Computes per-inverter health flags (PR / Temp / DC / AC + overall)
  4. Writes a time-series JSON snapshot (dashboard_data_{date}.json)
  5. Purges JSON snapshots older than 7 days

Run with:
    python processor_watchdog.py
"""

import json
import logging
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "extracted_data"
LOG_PATH = ROOT / "analysis.log"

REQUIRED_PREFIXES = [
    "PR",
    "Potenza_AC",
    "Corrente_DC",
    "Resistenza_Isolamento",
    "Temperatura",
    "Irraggiamento",
]

# Inverter names ordered TX1-01…TX3-12
INVERTER_NAMES = (
    [f"INV TX1-{i:02d}" for i in range(1, 13)]
    + [f"INV TX2-{i:02d}" for i in range(1, 13)]
    + [f"INV TX3-{i:02d}" for i in range(1, 13)]
)

JSON_RETENTION_DAYS = 7
STABILIZATION_SECONDS = 5
MAX_ANOMALIES = 50
DEDUP_WINDOW_HOURS = 1.0

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WATCHDOG] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger("processor_watchdog")


# ---------------------------------------------------------------------------
# Helper: Italian number / time conversions
# ---------------------------------------------------------------------------

def parse_italian_number(s) -> float | None:
    if not isinstance(s, str):
        return s
    s = s.strip()
    if not s or s in ("-", "—", "n/a", "N/A", "--"):
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def to_hours(t) -> float | None:
    """Convert HH:MM or HH:MM:SS string to decimal hours."""
    if pd.isna(t):
        return None
    s = str(t).strip()
    parts = s.split(":")
    try:
        if len(parts) >= 2:
            return int(parts[0]) + int(parts[1]) / 60 + (int(parts[2]) / 3600 if len(parts) > 2 else 0)
    except ValueError:
        pass
    return None


# ---------------------------------------------------------------------------
# Data cleaning
# ---------------------------------------------------------------------------

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Drop Timestamp Fetch column and ensure there is an 'Ora' column."""
    df = df.copy()
    
    # 1. Remove our internal processing timestamp
    if "Timestamp Fetch" in df.columns:
        df = df.drop(columns=["Timestamp Fetch"])
    
    # 2. Identify the 'Time' column (usually 'Ora' or 'DateTime')
    time_col = None
    if "Ora" in df.columns:
        time_col = "Ora"
    elif "DateTime" in df.columns:
        time_col = "DateTime"
    else:
        for col in df.columns:
            if "Ora" in str(col) or "DateTime" in str(col):
                time_col = col
                break
        if not time_col and len(df.columns) > 0:
            time_col = df.columns[0]
            
    if time_col:
        if time_col != "Ora":
            df = df.rename(columns={time_col: "Ora"})
        
        # Standardize 'Ora' to string to prevent merge type-mismatch errors
        # (e.g. converting 12.0 float or 12:00 object both to "12:00" or similar)
        df["Ora"] = df["Ora"].astype(str).str.replace(".0", "", regex=False).str.strip()

    # 3. Clean value columns (convert Italian formatting to numeric)
    for col in df.columns:
        if col == "Ora":
            continue
        if df[col].dtype == object:
            df[col] = df[col].apply(parse_italian_number)
        df[col] = pd.to_numeric(df[col], errors="coerce")
        
    # 4. Standardize inverter column names (if they start with 'TX', prepend 'INV ')
    #    so it matches INVERTER_NAMES perfectly.
    rename_dict = {}
    for col in df.columns:
        if col.startswith("TX"):
            rename_dict[col] = f"INV {col}"
    if rename_dict:
        df = df.rename(columns=rename_dict)
    
    return df


# ---------------------------------------------------------------------------
# File loader
# ---------------------------------------------------------------------------

def load_metric(prefix: str, date_str: str) -> pd.DataFrame | None:
    path = DATA_DIR / f"{prefix}_{date_str}.xlsx"
    if not path.exists():
        return None
    try:
        df = pd.read_excel(path, engine="openpyxl")
        return df
    except Exception as e:
        log.error(f"Failed to load {path.name}: {e}")
        return None


# ---------------------------------------------------------------------------
# Health flags
# ---------------------------------------------------------------------------

def compute_health_flags(latest_row: pd.Series, inverter: str, dc_df: pd.DataFrame | None) -> dict:
    """Compute the 4 LED flags and overall status for one inverter."""

    flags = {}

    # --- PR flag ---
    pr_col = f"{inverter}_PR" if f"{inverter}_PR" in latest_row.index else inverter
    pr_val = latest_row.get(pr_col, np.nan)
    if pd.isna(pr_val):
        flags["pr"] = "grey"
    else:
        # Normalise to 0-100 scale
        if pr_val <= 1.0:
            pr_val *= 100
        if pr_val >= 85:
            flags["pr"] = "green"
        elif pr_val >= 75:
            flags["pr"] = "yellow"
        else:
            flags["pr"] = "red"

    # --- Temperature flag ---
    temp_col = f"{inverter}_TEMP" if f"{inverter}_TEMP" in latest_row.index else f"{inverter}_Temperatura"
    temp_val = latest_row.get(temp_col, np.nan)
    if pd.isna(temp_val):
        flags["temp"] = "grey"
    elif temp_val <= 40:
        flags["temp"] = "green"
    elif temp_val <= 45:
        flags["temp"] = "yellow"
    else:
        flags["temp"] = "red"

    # --- DC current flag (relative to site median) ---
    site_dc_median = np.nan
    inv_dc_mean = np.nan
    if dc_df is not None:
        # DC DataFrame has string-level columns; inverter columns start with inverter name
        inv_dc_cols = [c for c in dc_df.columns if c.startswith(inverter)]
        if inv_dc_cols:
            inv_dc_mean = dc_df[inv_dc_cols].mean(axis=1).iloc[-1] if len(dc_df) else np.nan
            all_dc_cols = [c for c in dc_df.columns if c not in ("Ora", "DateTime", "Timestamp Fetch")]
            site_dc_median = dc_df[all_dc_cols].mean(axis=1).median()

    if pd.isna(inv_dc_mean) or pd.isna(site_dc_median) or site_dc_median <= 0:
        flags["dc_current"] = "grey"
    elif inv_dc_mean >= 0.15 * site_dc_median:
        flags["dc_current"] = "green"
    else:
        flags["dc_current"] = "red"

    # --- AC power flag (relative to site median) ---
    ac_col = inverter  # Potenza_AC columns are named by inverter
    ac_val = latest_row.get(ac_col, np.nan)
    # Site AC median from the same row (all inverter columns)
    inv_cols = [c for c in latest_row.index if c.startswith("INV ")]
    site_ac_median = latest_row[inv_cols].median() if inv_cols else np.nan

    if pd.isna(ac_val) or pd.isna(site_ac_median) or site_ac_median <= 0:
        flags["ac_power"] = "grey"
    elif ac_val >= 0.97 * site_ac_median:
        flags["ac_power"] = "green"
    else:
        flags["ac_power"] = "red"

    # --- Overall status: worst non-grey ---
    priority = {"red": 3, "yellow": 2, "green": 1, "grey": 0}
    worst = max(flags.values(), key=lambda x: priority.get(x, 0))
    flags["overall_status"] = worst if worst != "grey" or all(v == "grey" for v in flags.values()) else worst

    return flags


# ---------------------------------------------------------------------------
# Forensic analysis rules
# ---------------------------------------------------------------------------

def analyze_site(date_str: str) -> None:
    log.info(f"Running analysis for {date_str}")

    # --- Load ---
    ac = load_metric("Potenza_AC", date_str)
    pr = load_metric("PR", date_str)
    dc = load_metric("Corrente_DC", date_str)
    res = load_metric("Resistenza_Isolamento", date_str)
    temp = load_metric("Temperatura", date_str)
    irr = load_metric("Irraggiamento", date_str)

    if ac is None:
        log.warning("Potenza_AC file missing — skipping analysis")
        return

    # --- Clean ---
    ac_clean = clean_data(ac)
    pr_clean = clean_data(pr) if pr is not None else None
    dc_clean = clean_data(dc) if dc is not None else None
    res_clean = clean_data(res) if res is not None else None
    temp_clean = clean_data(temp) if temp is not None else None
    irr_clean = clean_data(irr) if irr is not None else None

    # --- Merge on Ora ---
    merged = ac_clean.copy()
    if "Ora" not in merged.columns and "DateTime" in merged.columns:
        merged = merged.rename(columns={"DateTime": "Ora"})

    def left_join(base, other, suffix):
        if other is None:
            return base
        
        # If the 'other' dataframe doesn't have 1440 rows (time-series), skip time-join
        # (e.g. PR summary with only 36 rows)
        if len(other) != 1440:
            log.info(f"Skipping time-join for {suffix}: summary-style data found ({len(other)} rows).")
            return base

        if "Ora" not in other.columns:
            return base
            
        value_cols = [c for c in other.columns if c not in ("Ora",)]
        other_renamed = other.rename(columns={c: f"{c}_{suffix}" for c in value_cols})
        
        # Force both keys to string for safety during merge
        base["Ora"] = base["Ora"].astype(str).str.replace(".0", "", regex=False).str.strip()
        other_renamed["Ora"] = other_renamed["Ora"].astype(str).str.replace(".0", "", regex=False).str.strip()
        
        return base.merge(other_renamed, on="Ora", how="left")

    try:
        merged = left_join(merged, pr_clean, "PR")
        merged = left_join(merged, temp_clean, "TEMP")
        merged = left_join(merged, res_clean, "RES")
        merged = left_join(merged, irr_clean, "IRR")
    except Exception as merge_err:
        log.error(f"Merge error: {merge_err}")
    
    # Ensure 'Ora' exists for subsequent steps
    if "Ora" not in merged.columns:
        log.error("Fatal: 'Ora' column missing after merge/clean. Aborting analysis.")
        return

    # Add hours column for daylight window checks
    merged["_hours"] = merged["Ora"].apply(to_hours)

    # Identify inverter columns in AC (master) data
    # VCOM may export columns as "INV TX1-01" or just "TX1-01"
    inv_cols = [c for c in ac_clean.columns if c.startswith("INV ") or c.startswith("TX")]
    
    if not inv_cols:
        log.warning(f"No inverter columns found! Available columns: {ac_clean.columns.tolist()}")

    # --- Anomaly detection ---
    anomalies = []

    for _, row in merged.iterrows():
        hour = row.get("_hours")
        if hour is None:
            continue

        site_ac_median = row[inv_cols].median() if inv_cols else np.nan
        timestamp_str = str(row.get("Ora", ""))

        for inv in inv_cols:
            ac_val = row.get(inv, np.nan)
            pr_val = row.get(f"{inv}_PR", np.nan)
            temp_val = row.get(f"{inv}_TEMP", np.nan)

            # Rule 5: Communication Loss (NaN during daylight)
            if pd.isna(ac_val) and 7 <= hour <= 19:
                anomalies.append({
                    "timestamp": timestamp_str,
                    "inverter": inv,
                    "type": "Communication Loss",
                    "severity": "High",
                    "details": "No data stream",
                })
                continue  # Priority: only first matching rule fires

            # Rule 6: Inverter Trip (0W while plant active)
            if (not pd.isna(ac_val) and ac_val == 0
                    and not pd.isna(site_ac_median) and site_ac_median > 2000
                    and 7 <= hour <= 19):
                anomalies.append({
                    "timestamp": timestamp_str,
                    "inverter": inv,
                    "type": "Inverter Trip",
                    "severity": "Critical",
                    "details": "Zero production detected",
                })
                continue

            # Rule 1: Low Performance Ratio
            if not pd.isna(pr_val) and 9 <= hour <= 17:
                pr_norm = pr_val * 100 if pr_val <= 1.0 else pr_val
                if pr_norm < 85:
                    anomalies.append({
                        "timestamp": timestamp_str,
                        "inverter": inv,
                        "type": "Low Performance Ratio",
                        "severity": "Critical",
                        "details": f"PR: {pr_norm:.1f}%",
                    })
                    continue

            # Rule 2: High Operating Temperature
            if not pd.isna(temp_val) and temp_val > 40:
                anomalies.append({
                    "timestamp": timestamp_str,
                    "inverter": inv,
                    "type": "High Operating Temperature",
                    "severity": "Warning",
                    "details": f"Temp: {temp_val:.1f}°C",
                })
                continue

            # Rule 3: DC String Failure
            if dc_clean is not None and not pd.isna(ac_val) and ac_val > 500:
                if "Ora" in dc_clean.columns:
                    dc_row = dc_clean[dc_clean["Ora"] == row.get("Ora")]
                    if not dc_row.empty:
                        string_cols = [c for c in dc_clean.columns if c.startswith(inv) and c != inv]
                        for sc in string_cols:
                            sv = dc_row.iloc[0].get(sc, np.nan)
                            if not pd.isna(sv) and sv < 0.2:
                                anomalies.append({
                                    "timestamp": timestamp_str,
                                    "inverter": inv,
                                    "type": "DC String Failure",
                                    "severity": "Critical",
                                    "details": f"String {sc}: {sv:.3f}A",
                                })
                                break  # Only one per inverter per timestep

            # Rule 4: Power Yield Deviation
            if (not pd.isna(ac_val) and not pd.isna(site_ac_median)
                    and site_ac_median > 5000):
                deviation = (ac_val - site_ac_median) / site_ac_median
                if abs(deviation) > 0.03:
                    anomalies.append({
                        "timestamp": timestamp_str,
                        "inverter": inv,
                        "type": "Power Yield Deviation",
                        "severity": "Critical",
                        "details": f"Deviation: {deviation*100:+.1f}%",
                    })

    # --- Deduplication ---
    anomalies = _dedup_anomalies(anomalies)

    # --- Per-inverter health flags from latest valid row ---
    health = {}
    
    # Drop rows where all inverter values are NaN to find the actual current time context
    latest = None
    if not merged.empty and inv_cols:
        valid_data = merged.dropna(subset=inv_cols, how="all")
        if not valid_data.empty:
            latest = valid_data.iloc[-1]
            
    if latest is None and not merged.empty:
        latest = merged.iloc[-1]
        
    if latest is not None:
        for inv in INVERTER_NAMES:
            health[inv] = compute_health_flags(latest, inv, dc_clean)

    # --- Macro health ---
    online = tripped = comms_lost = 0
    if latest is not None and inv_cols:
        latest_ac = latest[inv_cols]
        online = int((latest_ac > 0).sum())
        tripped = int((latest_ac == 0).sum())
        comms_lost = int(latest_ac.isna().sum())

    macro = {
        "total_inverters": len(INVERTER_NAMES),
        "online": online,
        "tripped": tripped,
        "comms_lost": comms_lost,
        "last_sync": datetime.now().isoformat(timespec="seconds"),
    }

    # --- File ingestion status ---
    file_status = {}
    
    # Try to load the detailed extraction status from vcom_monitor
    ext_status = {}
    status_path = DATA_DIR / "extraction_status.json"
    if status_path.exists():
        try:
            with open(status_path, "r", encoding="utf-8") as f:
                ext_status = json.load(f).get(date_str, {})
        except Exception:
            pass

    for prefix in REQUIRED_PREFIXES:
        path = DATA_DIR / f"{prefix}_{date_str}.xlsx"
        
        # Default status based on file existence
        status = "success" if path.exists() else "pending"
        mtime = datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds") if path.exists() else None
        
        # Override with detailed status if available
        if prefix in ext_status:
            status = ext_status[prefix]["status"]
            if not mtime:
                mtime = ext_status[prefix].get("timestamp")

        file_status[prefix] = {
            "status": status,
            "timestamp": mtime,
        }

    # --- Build snapshot ---
    snapshot_key = datetime.now().strftime("%Y-%m-%d %H:%M")
    snapshot = {
        "macro_health": macro,
        "file_status": file_status,
        "inverter_health": health,
        "active_anomalies": anomalies[:MAX_ANOMALIES],
        "historical_trail": anomalies[:100],
    }

    # --- Write JSON ---
    json_path = DATA_DIR / f"dashboard_data_{date_str}.json"
    existing = {}
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = {}
    existing[snapshot_key] = snapshot
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

    log.info(f"Snapshot written -> {json_path.name}  ({len(anomalies)} anomalies)")

    # --- Purge old JSON files ---
    _purge_old_json()


# ---------------------------------------------------------------------------
# Alert deduplication
# ---------------------------------------------------------------------------

def _dedup_anomalies(anomalies: list) -> list:
    """Collapse consecutive identical (inverter + type) alerts within 1 hour."""
    if not anomalies:
        return []
    # Sort by inverter, type, timestamp
    anomalies.sort(key=lambda a: (a["inverter"], a["type"], a["timestamp"]))
    deduplicated = [anomalies[0]]
    for alert in anomalies[1:]:
        prev = deduplicated[-1]
        if alert["inverter"] == prev["inverter"] and alert["type"] == prev["type"]:
            # Check time gap
            prev_h = to_hours(prev["timestamp"].split(" ")[-1]) or to_hours(prev["timestamp"])
            curr_h = to_hours(alert["timestamp"].split(" ")[-1]) or to_hours(alert["timestamp"])
            if prev_h is not None and curr_h is not None and abs(curr_h - prev_h) < DEDUP_WINDOW_HOURS:
                continue
        deduplicated.append(alert)
    return deduplicated


# ---------------------------------------------------------------------------
# JSON purge
# ---------------------------------------------------------------------------

def _purge_old_json() -> None:
    cutoff = datetime.now() - timedelta(days=JSON_RETENTION_DAYS)
    for f in DATA_DIR.glob("dashboard_data_*.json"):
        try:
            date_str = f.stem.replace("dashboard_data_", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            if file_date < cutoff:
                f.unlink()
                log.info(f"Purged old snapshot: {f.name}")
        except ValueError:
            pass


# ---------------------------------------------------------------------------
# File-system watcher
# ---------------------------------------------------------------------------

class MetricFileHandler(FileSystemEventHandler):
    def __init__(self):
        self._lock = threading.Lock()
        self._pending: dict[str, float] = {}  # date_str -> last event time

    def on_modified(self, event):
        self._handle(event)

    def on_created(self, event):
        self._handle(event)

    def _handle(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() != ".xlsx":
            return
        date_str = _extract_date_from_path(path)
        if not date_str:
            return
        with self._lock:
            self._pending[date_str] = time.monotonic()

    def flush_due(self) -> list[str]:
        """Return date strings whose last event was > STABILIZATION_SECONDS ago."""
        now = time.monotonic()
        due = []
        with self._lock:
            for ds, ts in list(self._pending.items()):
                if now - ts >= STABILIZATION_SECONDS:
                    due.append(ds)
                    del self._pending[ds]
        return due


def _extract_date_from_path(path: Path) -> str | None:
    import re
    m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
    return m.group(1) if m else None


def _all_files_present(date_str: str) -> bool:
    """True if all 6 metrics have either an Excel file OR a failed/empty status."""
    ext_status = {}
    status_path = DATA_DIR / "extraction_status.json"
    if status_path.exists():
        try:
            with open(status_path, "r", encoding="utf-8") as f:
                ext_status = json.load(f).get(date_str, {})
        except Exception:
            pass

    for prefix in REQUIRED_PREFIXES:
        exists = (DATA_DIR / f"{prefix}_{date_str}.xlsx").exists()
        status = ext_status.get(prefix, {}).get("status")
        if not exists and status not in ("failed", "empty"):
            return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"Watchdog started — monitoring {DATA_DIR}")

    handler = MetricFileHandler()
    observer = Observer()
    observer.schedule(handler, str(DATA_DIR), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
            for date_str in handler.flush_due():
                print(f"[WATCHDOG] Change detected for {date_str}. Checking file status...", flush=True)
                if _all_files_present(date_str):
                    print(f"[WATCHDOG] All requirements met for {date_str}. Starting Forensic Analysis...", flush=True)
                    try:
                        analyze_site(date_str)
                    except Exception:
                        print(f"[WATCHDOG] Analysis FAILED for {date_str}.", flush=True)
                        log.error(f"Analysis failed for {date_str}:\n{traceback.format_exc()}")
                else:
                    missing = [p for p in REQUIRED_PREFIXES if not (DATA_DIR / f"{p}_{date_str}.xlsx").exists()]
                    print(f"[WATCHDOG] Waiting for more data ({date_str}). Missing: {missing}", flush=True)
                    log.info(f"Not all files present for {date_str} — missing: {missing}")
    except KeyboardInterrupt:
        pass
    finally:
        observer.stop()
        observer.join()
        log.info("Watchdog stopped")


if __name__ == "__main__":
    main()
