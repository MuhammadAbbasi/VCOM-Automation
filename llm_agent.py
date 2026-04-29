import json
import logging
import requests
import os
import re
import socket
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger("llm_agent")
logger.setLevel(logging.INFO)

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "extracted_data"

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
def get_user_settings():
    path = ROOT / "user_settings.json"
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

_settings = get_user_settings()
OLLAMA_API_URL = _settings.get("ollama_url", "http://localhost:11434/api/generate")
MODEL_NAME = "qwen2.5:7b"
DEBUG_MODE = _settings.get("debug_mode", False)

# ---------------------------------------------------------------------------
# Conversational Memory (30 minute window)
# ---------------------------------------------------------------------------
CHAT_HISTORY = {}
MEMORY_WINDOW_MINUTES = 30

def get_user_context(user_id):
    if not user_id or user_id not in CHAT_HISTORY:
        return ""
    now = datetime.now()
    history = CHAT_HISTORY[user_id]
    valid = [e for e in history if (now - e["ts"]).total_seconds() / 60 <= MEMORY_WINDOW_MINUTES]
    CHAT_HISTORY[user_id] = valid
    if not valid:
        return ""
    ctx = "\nRECENT CONVERSATION:\n"
    for e in valid[-5:]:
        ctx += f"User: {e['q']}\nAI: {e['a'][:200]}\n"
    return ctx + "\n"

# ---------------------------------------------------------------------------
# Database Data Engine
# ---------------------------------------------------------------------------
import pandas as pd
import numpy as np
from db.db_manager import (
    load_metric, load_latest_snapshot, get_db_stats, get_data_conn, 
    get_logs_conn, get_tracker_summary, get_all_tracker_status,
    load_all_snapshots
)

INV_IDS = [f"INV TX{tx}-{i:02d}" for tx in range(1, 4) for i in range(1, 13)]

def _load_csv(filename):
    """Deprecated. Use load_metric instead."""
    # Mapping filename to metric name for backward compatibility if model still tries to call it
    mapping = {
        "Potenza_AC.csv": "Potenza AC",
        "Temperatura.csv": "Temperatura",
        "Corrente_DC.csv": "Corrente DC",
        "Irraggiamento.csv": "Irraggiamento",
        "PR_Inverter.csv": "PR inverter"
    }
    metric = mapping.get(filename)
    if metric:
        return load_metric(datetime.now().strftime("%Y-%m-%d"), metric)
    return pd.DataFrame()

def get_public_url():
    """Try to fetch ngrok public URL from local ngrok API."""
    try:
        resp = requests.get("http://localhost:4040/api/tunnels", timeout=1)
        if resp.ok:
            tunnels = resp.json().get("tunnels", [])
            if tunnels:
                return tunnels[0].get("public_url")
    except:
        pass
    return None

def get_available_dates():
    """List all dates that have data in the database."""
    try:
        conn = get_data_conn()
        # Check available dates across some major tables
        tables = ["potenza_ac", "corrente_dc", "irraggiamento"]
        dates = set()
        for t in tables:
            try:
                res = conn.execute(f"SELECT DISTINCT date FROM {t}").fetchall()
                for r in res:
                    dates.add(r[0])
            except:
                pass
        return sorted(dates, reverse=True)
    except Exception:
        return []

def calculate_sun_times(date_str=None):
    """Approximate sunrise/sunset for Mazara del Vallo."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.now()
        doy = dt.timetuple().tm_yday
        lat_rad = np.radians(37.67)
        decl = 0.409 * np.sin(2 * np.pi * (doy - 81) / 365)
        cos_h = -np.tan(lat_rad) * np.tan(decl)
        cos_h = np.clip(cos_h, -1, 1)
        h = np.arccos(cos_h)
        sunrise_base = 12.0 - (np.degrees(h) / 15.0)
        sunset_base = 12.0 + (np.degrees(h) / 15.0)
        lon_adj = (15.0 - 12.59) * 4 / 60.0
        dst_adj = 1.0 if (3 <= dt.month <= 10) else 0.0
        sunrise = sunrise_base + lon_adj + dst_adj
        sunset = sunset_base + lon_adj + dst_adj
        return sunrise, sunset
    except:
        return 6.5, 19.5

def get_total_production(date_str=None):
    """Total plant energy in MWh for a given date."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(date_str, "Potenza AC")
    
    if df.empty:
        return {"date": date_str, "total_mwh": None, "error": "No data"}
        
    ac_cols = [c for c in df.columns if "Potenza AC (INV" in c or "Potenza AC(INV" in c]
    if not ac_cols:
        return {"date": date_str, "total_mwh": None, "error": "No inverter columns"}
    
    df[ac_cols] = df[ac_cols].fillna(0)
    total_mwh = round(float(df[ac_cols].sum().sum() * (1/60)) / 1_000_000, 3)
    
    # Calculate average MW during production hours
    sunrise, sunset = calculate_sun_times(date_str)
    if "Ora" in df.columns:
        prod_df = df[(df["Ora"].astype(float) >= sunrise) & (df["Ora"].astype(float) <= sunset)]
        if not prod_df.empty:
            avg_mw = round(float(prod_df[ac_cols].sum(axis=1).mean()) / 1_000_000, 3)
        else:
            avg_mw = 0.0
    else:
        avg_mw = 0.0
        
    return {"date": date_str, "total_mwh": total_mwh, "average_mw": avg_mw, "inverter_count": len(ac_cols)}

def get_peak_production(date_str=None):
    """Find peak instantaneous power and its time."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(date_str, "Potenza AC")
    if df.empty:
        return {"date": date_str, "error": "No data"}
        
    ac_cols = [c for c in df.columns if "Potenza AC (INV" in c]
    if not ac_cols:
        return {"date": date_str, "error": "No inverter columns"}
        
    df["total_power"] = df[ac_cols].sum(axis=1)
    peak_idx = df["total_power"].idxmax()
    peak_val = df.loc[peak_idx, "total_power"]
    peak_ora = df.loc[peak_idx, "Ora"] if "Ora" in df.columns else "Unknown"
    
    # Format peak_ora (HH.mm -> HH:mm)
    peak_time = "Unknown"
    if peak_ora != "Unknown":
        try:
            f = float(peak_ora)
            h = int(f)
            m = int(round((f - h) * 100))
            if m >= 60: m = int(round((f - h) * 60))
            peak_time = f"{h:02d}:{m:02d}"
        except:
            peak_time = str(peak_ora)

    # Try to get POA at that time
    poa_val = None
    irr_df = load_metric(date_str, "Irraggiamento")
    if not irr_df.empty and "Ora" in irr_df.columns:
        # Find matching time in irradiance
        match = irr_df[irr_df["Ora"] == peak_ora]
        if not match.empty:
            poa_cols = [c for c in irr_df.columns if "POA" in c or "Irraggiamento" in c]
            if poa_cols:
                poa_val = match.iloc[0][poa_cols[0]]

    return {
        "date": date_str, 
        "peak_power_w": round(float(peak_val)), 
        "peak_time": peak_time,
        "poa_at_peak": round(float(poa_val)) if poa_val is not None else None
    }

def get_temperatures(date_str=None, threshold=None):
    """Get latest temperature readings for all inverters."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(date_str, "Temperatura")
    if df.empty:
        return {"date": date_str, "error": "No temperature data"}
    temp_cols = [c for c in df.columns if "Temperatura inverter (INV" in c or "Temperatura (INV" in c]
    if not temp_cols:
        return {"date": date_str, "error": "No temperature columns found"}
    
    # Get latest non-empty row
    df_valid = df[temp_cols].dropna(how='all')
    if df_valid.empty:
        return {"date": date_str, "error": "All temperature rows are empty"}
    
    latest = df_valid.iloc[-1]
    result = {}
    for col in temp_cols:
        val = latest[col]
        if pd.notna(val) and val != 0:
            # Extract inverter name from column
            inv_match = re.search(r'\(INV (TX\d-\d{2})\)', col)
            inv_name = f"INV {inv_match.group(1)}" if inv_match else col
            result[inv_name] = round(float(val), 1)
    
    output = {"date": date_str, "temperatures": result, "max_temp": max(result.values()) if result else 0, "min_temp": min(result.values()) if result else 0}
    if threshold:
        above = {k: v for k, v in result.items() if v > threshold}
        output["above_threshold"] = above
        output["threshold"] = threshold
        output["count_above"] = len(above)
    return output

def get_inverter_status(date_str=None):
    """Get latest power for each inverter, flag zeros/low."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(date_str, "Potenza AC")
    if df.empty:
        return {"date": date_str, "error": "No AC power data"}
    ac_cols = [c for c in df.columns if "Potenza AC (INV" in c]
    if not ac_cols:
        return {"date": date_str, "error": "No inverter columns"}
    
    df_valid = df[ac_cols].dropna(how='all')
    if df_valid.empty:
        return {"date": date_str, "error": "All rows empty"}
    latest = df_valid.iloc[-1]
    
    status = {}
    for col in ac_cols:
        inv_match = re.search(r'\(INV (TX\d-\d{2})\)', col)
        inv_name = f"INV {inv_match.group(1)}" if inv_match else col
        val = float(latest[col]) if pd.notna(latest[col]) else 0
        if val <= 0:
            status[inv_name] = {"power_w": 0, "status": "OFF"}
        elif val < 300:
            status[inv_name] = {"power_w": round(val), "status": "LOW"}
        else:
            status[inv_name] = {"power_w": round(val), "status": "OK"}
    
    off = [k for k, v in status.items() if v["status"] == "OFF"]
    low = [k for k, v in status.items() if v["status"] == "LOW"]
    ok = [k for k, v in status.items() if v["status"] == "OK"]
    return {"date": date_str, "off_inverters": off, "low_inverters": low, "online_count": len(ok), "total": len(status), "details": status}

def get_inverter_production_detail(inv_id, date_str=None):
    """Get today's energy production in kWh for a specific inverter (e.g. TX1-01)."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(date_str, "Potenza AC")
    if df is None or df.empty:
        return 0.0
    
    # Standard format in DB: "Potenza AC (INV TX1-01) [W]"
    col = next((c for c in df.columns if inv_id.upper() in c and "Potenza AC" in c), None)
    if not col:
        return 0.0
        
    # Integration: Sum of Watts * (1 minute / 60 minutes/hour) = Watt-hours
    try:
        energy_wh = float(df[col].apply(pd.to_numeric, errors='coerce').fillna(0).sum()) / 60.0
        return round(energy_wh / 1000.0, 2) # Convert to kWh
    except:
        return 0.0

def get_transformer_comparison(date_str=None):
    """Compare TX1, TX2, TX3 production."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(date_str, "Potenza AC")
    if df.empty:
        return {"date": date_str, "error": "No data"}
    
    result = {}
    for tx in ["TX1", "TX2", "TX3"]:
        cols = [c for c in df.columns if f"(INV {tx}-" in c]
        if cols:
            # sum() skips NaNs. Filling with 0 to be explicit.
            tx_data = df[cols].fillna(0)
            total_mwh = round(float(tx_data.sum().sum() * (1/60)) / 1_000_000, 3)
            
            # Latest MW should be from the last row that ACTUALLY had data
            valid_rows = df[cols].dropna(how='all')
            if not valid_rows.empty:
                latest_mw = round(float(valid_rows.iloc[-1].fillna(0).sum()) / 1_000_000, 3)
            else:
                latest_mw = 0.0
                
            result[tx] = {"total_mwh": total_mwh, "latest_mw": latest_mw, "inverter_count": len(cols)}
    return {"date": date_str, "transformers": result}

def get_dc_currents(date_str=None, threshold=None):
    """Get DC current info for strings/MPPTs."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(date_str, "Corrente DC")
    if df.empty:
        return {"date": date_str, "error": "No DC current data"}
    dc_cols = [c for c in df.columns if "Corrente DC" in c and "MPPT" in c]
    if not dc_cols:
        return {"date": date_str, "error": "No DC current columns"}
    
    df_valid = df[dc_cols].dropna(how='all')
    if df_valid.empty:
        return {"date": date_str, "error": "All DC rows empty"}
    latest = df_valid.iloc[-1]
    
    readings = {}
    for col in dc_cols:
        val = float(latest[col]) if pd.notna(latest[col]) else 0
        readings[col] = round(val, 2)
    
    vals = list(readings.values())
    output = {
        "date": date_str, 
        "total_strings": len(dc_cols),
        "avg": round(sum(vals)/len(vals), 2) if vals else 0,
        "max": max(vals) if vals else 0,
        "min": min(vals) if vals else 0
    }
    
    if threshold is not None:
        below = {k: v for k, v in readings.items() if v < threshold}
        output["below_threshold"] = below
        output["threshold"] = threshold
        output["count_below"] = len(below)
    else:
        output["readings"] = readings
    return output

def get_irradiance(date_str=None):
    """Get latest irradiance readings."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(date_str, "Irraggiamento")
    if df.empty:
        return {"date": date_str, "error": "No irradiance data"}
    irr_cols = [c for c in df.columns if "Irraggiamento" in c and c != "Ora" and c != "Timestamp Fetch"]
    if not irr_cols:
        return {"date": date_str, "error": "No irradiance columns"}
    
    df_valid = df[irr_cols].dropna(how='all')
    if df_valid.empty:
        return {"date": date_str, "error": "All rows empty"}
    latest_vals = {col: round(float(df_valid.iloc[-1][col]), 1) for col in irr_cols if pd.notna(df_valid.iloc[-1][col])}
    peak_vals = {col: round(float(df[col].max()), 1) for col in irr_cols}
    
    # Calculate averages during daylight hours
    sunrise, sunset = calculate_sun_times(date_str)
    avg_vals = {}
    if "Ora" in df.columns:
        daylight_df = df[(df["Ora"].astype(float) >= sunrise) & (df["Ora"].astype(float) <= sunset)]
        if not daylight_df.empty:
            for col in irr_cols:
                avg_vals[col] = round(float(daylight_df[col].mean()), 1)
    
    return {"date": date_str, "latest": latest_vals, "peak": peak_vals, "daylight_average": avg_vals}

def get_downtime_events(date_str=None):
    """Check which inverters went offline during production hours."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(date_str, "Potenza AC")
    if df.empty:
        return {"date": date_str, "error": "No data"}
    
    ac_cols = [c for c in df.columns if "Potenza AC (INV" in c]
    if not ac_cols or "Ora" not in df.columns:
        return {"date": date_str, "error": "Missing columns"}
    
    df[ac_cols] = df[ac_cols].fillna(0)
    # Find production window (when at least 10 inverters produce > 300W)
    df["active_count"] = (df[ac_cols] > 300).sum(axis=1)
    prod_rows = df[df["active_count"] >= 10]
    if prod_rows.empty:
        return {"date": date_str, "error": "No production detected"}
    
    prod_start = float(prod_rows.iloc[0]["Ora"])
    prod_end = float(prod_rows.iloc[-1]["Ora"])
    
    # In production window, find inverters with zero power
    prod_df = df[(df["Ora"] >= prod_start) & (df["Ora"] <= prod_end)]
    events = {}
    for col in ac_cols:
        inv_match = re.search(r'\(INV (TX\d-\d{2})\)', col)
        inv_name = f"INV {inv_match.group(1)}" if inv_match else col
        zero_minutes = int((prod_df[col] <= 10).sum())
        if zero_minutes > 5:  # At least 5 minutes off
            events[inv_name] = {"offline_minutes": zero_minutes}
    
    return {"date": date_str, "production_start": prod_start, "production_end": prod_end, "downtime_events": events}

def get_alarm_history(date_str=None, inverter=None, alarm_type=None):
    """Search all snapshots for a date to find specific alarm events."""
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    snaps = load_all_snapshots(date_str)
    if not snaps:
        return []
    
    found = []
    seen_ids = set()
    
    try:
        latest_ts = sorted(snaps.keys())[-1]
        latest_snap = snaps[latest_ts]
        
        # Watchdog carries forward history in these keys
        pool = latest_snap.get("historical_trail", []) + latest_snap.get("active_anomalies", [])
        
        for a in pool:
            # Filter by inverter (e.g. "TX3-07")
            target_inv = inverter.upper() if inverter else ""
            if target_inv and target_inv not in a.get("inverter", "").upper() and target_inv not in a.get("id", "").upper():
                continue
            # Filter by type (e.g. "INSULATION")
            target_type = alarm_type.upper() if alarm_type else ""
            if target_type and target_type not in a.get("type", "").upper() and target_type not in a.get("id", "").upper():
                continue
                
            aid = a.get("id")
            if aid not in seen_ids:
                found.append(a)
                seen_ids.add(aid)
    except Exception as e:
        logger.error(f"Error in get_alarm_history: {e}")
            
    return found

def get_active_anomalies():
    """Get currently active plant anomalies."""
    today = datetime.now().strftime("%Y-%m-%d")
    snap = load_latest_snapshot(today)
    if snap:
        return snap.get("active_anomalies", [])
    return []

def get_tracker_data_summary():
    """Get concise summary of tracker field."""
    try:
        return get_tracker_summary()
    except Exception as e:
        return {"error": str(e)}

def get_tracker_data_all():
    """Retrieve all tracker statuses from the database."""
    try:
        return get_all_tracker_status()
    except Exception as e:
        return {"error": str(e)}

# ---------------------------------------------------------------------------
# Data Snapshot Builder — builds the context the LLM reads
# ---------------------------------------------------------------------------
def build_data_snapshot(plant_data, question):
    """Dynamically fetch relevant data based on semantic categories and device mentions."""
    snapshot = []
    q = question.lower()
    
    # 1. Semantic Categories & Synonyms (Lite RAG Routing)
    CATEGORIES = {
        "TEMPERATURE": ["temperature", "temp", "hot", "°c", "caldo", "thermal", "heat", "warm", "fresco", "freddo", "gradi"],
        "PRODUCTION": ["production", "energy", "mwh", "kwh", "produzione", "total", "power", "quanto", "generate", "yield", "peak", "massima", "picco", "potenza", "energia"],
        "TRANSFORMERS": ["tx1", "tx2", "tx3", "transformer", "compare", "comparison", "confronto", "trasformatore", "trafi"],
        "DOWNTIME": ["off", "down", "zero", "offline", "stopped", "trip", "fault", "spento", "not working", "not producing", "fermo", "anomalia", "allarme"],
        "DC_STRINGS": ["current", "dc", "string", "mppt", "corrente", "amper", "stringhe"],
        "IRRADIANCE": ["irradiance", "sun", "irraggiamento", "solar", "radiation", "pyranometer", "sole", "luce"],
        "INSULATION": ["insulation", "iso", "isolamento", "resistenza", "kohm", "kohm", "omega"],
        "HISTORY": ["history", "historical", "trail", "past", "last alarms", "storia", "passato", "ieri", "precedente", "quanti", "how many", "how long", "durata", "duration"],
        "TRACKERS": ["tracker", "trackers", "ncu", "tcu", "angle", "position", "tilt", "seguimento", "inclinazione", "angolo", "motore", "motor"]
    }
    
    # Determine active categories
    active_cats = [cat for cat, synonyms in CATEGORIES.items() if any(s in q for s in synonyms)]
    
    # 2. Determine target date
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    target_date = today
    if any(w in q for w in ["yesterday", "ieri", "l'altro ieri"]):
        target_date = yesterday
    else:
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', q)
        if date_match: target_date = date_match.group(1)
    
    # 3. Device Auto-Detection (Always fetch status for mentioned devices)
    devices = re.findall(r'tx\d-\d{2}', q)
    if devices:
        snapshot.append(f"FOCUS DEVICES: {', '.join(d.upper() for d in devices)}")
        status_data = get_inverter_status(target_date)
        if "details" in status_data:
            dev_details = {}
            for d in devices:
                did = d.upper()
                info = status_data["details"].get(f"INV {did}", {})
                # ADD ENERGY to prevent hallucination (current power != energy)
                info["today_energy_kwh"] = get_inverter_production_detail(did, target_date)
                dev_details[f"INV {did}"] = info
            snapshot.append(f"DEVICE STATUS ({target_date}): {json.dumps(dev_details)}")

    # 4. Global State (Always include)
    if plant_data:
        macro = plant_data.get("macro_health", {})
        # Extract POA with fallbacks
        poa = macro.get("poa")
        if poa is None:
            # Try sensor_data fallback
            s_data = plant_data.get("sensor_data", {})
            poa_key = next((k for k in s_data.keys() if "POA" in k.upper()), None)
            poa = s_data.get(poa_key, 0) if poa_key else 0
            
        pr = macro.get("avg_pr", "?")
        mw = macro.get('MW') or macro.get('total_ac_power_mw', '?')
        online_count = macro.get('online', '?')
        total_count = macro.get('total_inverters', macro.get('total', '?'))
        
        # NIGHT MODE DETECTION vs SENSOR FAILURE
        now = datetime.now()
        now_hr = now.hour + (now.minute / 60.0)
        sunrise, sunset = calculate_sun_times(target_date)
        
        is_dark_hours = (now_hr > sunset + 0.5 or now_hr < sunrise - 0.5)
        is_sensor_zero = (poa < 10) # Very low reading
        
        if is_dark_hours:
            mode = "NIGHT MODE (Offline)"
            status_msg = f"It is currently night time at the plant. POA is {poa} W/m² as expected."
        elif is_sensor_zero and now_hr > (sunrise + 1) and now_hr < (sunset - 1):
            mode = "SENSOR ALERT (Irregularity)"
            status_msg = f"ALERT: Irradiance (POA) is reporting {poa} W/m² during daylight hours. This likely indicates a sensor communication issue or failure."
        else:
            mode = "PRODUCTION MODE (Daylight)"
            status_msg = f"Plant is in production. POA={poa} W/m²."

        snapshot.append(f"CURRENT LOCAL TIME: {now.strftime('%H:%M')}")
        snapshot.append(f"CURRENT STATUS (Latest Sync): {mode}, MW={mw}, Online={online_count}/{total_count}, {status_msg}, Avg PR={pr}%")
        snapshot.append(f"SUN SCHEDULE ({target_date}): Sunrise ~{int(sunrise)}:{int((sunrise%1)*60):02d}, Sunset ~{int(sunset)}:{int((sunset%1)*60):02d}")
        
    pub = get_public_url() or "https://carl-perkiest-paniculately.ngrok-free.dev/"
    snapshot.append(f"DASHBOARD URL: {pub} (Local: http://localhost:8080)")
    
    # 5. Semantic Data Fetching
    if "TEMPERATURE" in active_cats:
        data = get_temperatures(target_date, 50)
        snapshot.append(f"TEMPERATURES: {json.dumps(data, default=str)}")
        
    if "PRODUCTION" in active_cats:
        prod = get_total_production(target_date)
        peak = get_peak_production(target_date)
        snapshot.append(f"PRODUCTION: {json.dumps(prod, default=str)}")
        snapshot.append(f"PEAK: {json.dumps(peak, default=str)}")
        
    if "TRANSFORMERS" in active_cats:
        data = get_transformer_comparison(target_date)
        snapshot.append(f"TRANSFORMER COMPARISON: {json.dumps(data, default=str)}")
        
    if "DOWNTIME" in active_cats or "HISTORY" in active_cats:
        dt = get_downtime_events(target_date)
        snapshot.append(f"DOWNTIME EVENTS: {json.dumps(dt, default=str)}")
        if plant_data:
            if "active_anomalies" in plant_data:
                snapshot.append(f"ACTIVE ANOMALIES: {json.dumps(plant_data['active_anomalies'], default=str)}")
            if "historical_trail" in plant_data:
                snapshot.append(f"RECENT ALARM TRAIL (Last 20): {json.dumps(plant_data['historical_trail'][-20:], default=str)}")
            
    if "INSULATION" in active_cats:
        iso_df = load_metric(target_date, "Resistenza di isolamento")
        if iso_df is not None and not iso_df.empty:
             snapshot.append(f"ISO READINGS (Latest): {json.dumps(iso_df.iloc[-1].to_dict(), default=str)}")
        # Also check history for insulation
        iso_history = get_alarm_history(target_date, alarm_type="INSULATION")
        snapshot.append(f"INSULATION ALARM HISTORY: {json.dumps(iso_history, default=str)}")
            
    if "DC_STRINGS" in active_cats:
        # If specific device, fetch all DC for them. Otherwise, just fetch offline strings.
        if devices:
            data = get_dc_currents(target_date) # all
            filtered = {k: v for k, v in data.get("readings", {}).items() if any(d.upper() in k for d in devices)}
            snapshot.append(f"DC READINGS (FOCUS): {json.dumps(filtered)}")
        else:
            data = get_dc_currents(target_date, threshold=0.1) 
            snapshot.append(f"DC CURRENT STATS: Avg={data.get('avg')}A, Max={data.get('max')}A, Min={data.get('min')}A.")
            snapshot.append(f"LOW DC STRINGS (<0.1A): {data.get('count_below')}/{data.get('total_strings')} strings. Examples: {json.dumps(dict(list(data.get('below_threshold', {}).items())[:10]))}")
            
    if "IRRADIANCE" in active_cats:
        data = get_irradiance(target_date)
        snapshot.append(f"IRRADIANCE: {json.dumps(data)}")

    if "TRACKERS" in active_cats:
        summary = get_tracker_data_summary()
        plant_data["tracker_summary"] = summary
        snapshot.append(f"TRACKER SUMMARY: {json.dumps(summary, default=str)}")
        # If user mentions a specific TCU or asks for full list, include full data
        if any(w in q for w in ["list", "tcu", "every", "all trackers"]):
             data = get_tracker_data_all()
             plant_data["trackers"] = data
             # Only show first 5 to keep context manageable in the prompt text
             snapshot.append(f"TRACKER DATA (Sample of 5): {json.dumps(data[:5], default=str)}")
             snapshot.append("NOTE: The full 'trackers' list is available in the 'data' variable for analysis.")

    # 6. Fallback (If no categories matched, provide general overview)
    if not snapshot or len(snapshot) <= 2:
        prod = get_total_production(target_date)
        inv = get_inverter_status(target_date)
        snapshot.append(f"GENERAL OVERVIEW ({target_date}):")
        snapshot.append(f"- Production: {prod.get('total_mwh')} MWh")
        snapshot.append(f"- Inverters: {inv.get('online_count')}/{inv.get('total')} online")

    return "\n\n".join(snapshot)

# ---------------------------------------------------------------------------
# Code Library (Persistent cache) — kept for backward compatibility
# ---------------------------------------------------------------------------
CODE_LIB_DIR = ROOT / "temp_llm_codes"
CODE_LIB_DIR.mkdir(exist_ok=True)

def save_code_to_library(question, code):
    import hashlib
    safe_q = re.sub(r'[^a-zA-Z0-9]', '_', question[:30]).lower()
    q_hash = hashlib.md5(question.encode()).hexdigest()[:6]
    filename = f"tool_{safe_q}_{q_hash}.py"
    header = f"# TASK: {question}\n# SAVED: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
    (CODE_LIB_DIR / filename).write_text(header + code, encoding="utf-8")

# ---------------------------------------------------------------------------
# Code Execution (fallback for complex queries)
# ---------------------------------------------------------------------------
def run_python_analysis(code: str, plant_data: dict) -> tuple:
    """Execute code in a sandbox with all helpers available."""
    import io, contextlib
    
    if plant_data is None:
        plant_data = {}
    
    namespace = {
        "pd": pd, "np": np, "os": os, "re": re, "json": json, "socket": socket,
        "Path": Path, "glob": __import__("glob"),
        "data": plant_data, "DATA": plant_data,
        "datetime": datetime, "timedelta": timedelta,
        "load_metric": load_metric,
        "load_latest_snapshot": load_latest_snapshot,
        "get_db_stats": get_db_stats,
        "get_total_production": get_total_production,
        "get_peak_production": get_peak_production,
        "get_temperatures": get_temperatures,
        "get_inverter_status": get_inverter_status,
        "get_transformer_comparison": get_transformer_comparison,
        "get_inverter_production_detail": get_inverter_production_detail,
        "get_dc_currents": get_dc_currents,
        "get_irradiance": get_irradiance,
        "get_downtime_events": get_downtime_events,
        "get_available_dates": get_available_dates,
        "get_tracker_summary": get_tracker_summary,
        "get_tracker_data": get_all_tracker_status,
        "get_tracker_data_summary": get_tracker_data_summary,
        "get_alarm_history": get_alarm_history,
        "get_active_anomalies": get_active_anomalies,
        "search_logs": search_logs,
        "load_csv": _load_csv, # Kept but hidden from prompt
        "INV_IDS": INV_IDS,
        "TODAY": datetime.now().strftime("%Y-%m-%d"),
        "YESTERDAY": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "result": None,
    }
    
    output_capture = io.StringIO()
    try:
        with contextlib.redirect_stdout(output_capture):
            exec(code, namespace)
        res = namespace.get("result")
        stdout_val = output_capture.getvalue().strip()
        if res is not None:
            return (str(res), True)
        elif stdout_val:
            return (stdout_val, True)
        else:
            return ("Execution finished (no output).", True)
    except Exception as e:
        return (str(e), False)

# ---------------------------------------------------------------------------
# Main LLM Interface
# ---------------------------------------------------------------------------
def _load_system_prompt():
    path = ROOT / "ai_system_prompt.txt"
    if path.exists():
        return path.read_text(encoding="utf-8")
    # Fallback if file is missing
    return "You are the Mazara Solar Plant AI Analyst. Respond in the same language as the user."

def ask_llm(question: str, plant_data: dict = None, attempt: int = 1, last_code: str = None, last_error: str = None, user_id: str = "default") -> str:
    """Main entry point. Pre-computes data, then asks LLM to interpret."""
    if attempt > 3:
        return f"⚠️ AI Error: Analysis failed after multiple attempts.\nLast error: {last_error}"

    # 1. Load dynamic system prompt
    system_prompt = _load_system_prompt()
    
    # 2. Build data snapshot
    if plant_data is None:
        plant_data = {}
    snapshot = build_data_snapshot(plant_data, question)
    
    # 3. Get conversation history
    history_context = get_user_context(user_id)
    
    # 4. Build prompt
    correction_hint = ""
    if last_code and last_error:
        correction_hint = f"\n\nPREVIOUS CODE FAILED:\n```python\n{last_code}\n```\nError: {last_error}\nFix the code and try again."
    
    prompt = (
        f"{system_prompt}\n\n"
        f"{history_context}"
        f"PRE-COMPUTED DATA:\n{snapshot}\n\n"
        f"USER QUESTION: {question}{correction_hint}\n\n"
        f"Answer directly. If data is already available, just read it and respond. Only write code if the answer is NOT in the data above."
    )
    
    try:
        print(f"\n[AI] (ANALYZING) Question: {question}")
    except:
        pass
    
    # 4. Call LLM
    try:
        payload = {
            "model": MODEL_NAME,
            "prompt": prompt,
            "stream": False,
            "options": {"num_ctx": 8192, "temperature": 0.1}
        }
        resp = requests.post(OLLAMA_API_URL, json=payload, timeout=120)
        resp.raise_for_status()
        answer = resp.json().get("response", "").strip()
        
        if not answer:
            return "⚠️ AI returned an empty response. Try rephrasing your question."
        
        try:
            print(f"[AI] (RECEIVED) Response length: {len(answer)}")
        except:
            pass
        
        # 5. Check if LLM wrote code (fallback execution)
        if "```python" in answer:
            code = answer.split("```python")[1].split("```")[0].strip()
            res_val, success = run_python_analysis(code, plant_data)
            
            if not success:
                logger.warning(f"AI code failed (Attempt {attempt}): {res_val}")
                return ask_llm(question, plant_data, attempt + 1, code, res_val, user_id)
            
            # Strip code block from response, append result
            clean = answer.split("```python")[0].strip()
            parts = answer.split("```")
            if len(parts) > 2:
                clean += "\n" + parts[-1].strip()
            
            final = f"{clean}\n\n🔍 **ANALYSIS REPORT:**\n{res_val}"
            
            # Save to history
            if attempt == 1:
                _save_history(user_id, question, clean)
                try:
                    save_code_to_library(question, code)
                except:
                    pass
            return final.strip()
        
        # 6. Direct answer (no code needed — best case!)
        if attempt == 1:
            _save_history(user_id, question, answer)
        return answer
        
    except Exception as e:
        return f"⚠️ Technical Error: {str(e)}"

def _save_history(user_id, question, answer):
    if user_id not in CHAT_HISTORY:
        CHAT_HISTORY[user_id] = []
    CHAT_HISTORY[user_id].append({"q": question, "a": answer, "ts": datetime.now()})
    if len(CHAT_HISTORY[user_id]) > 5:
        CHAT_HISTORY[user_id].pop(0)

if __name__ == "__main__":
    print(ask_llm("Status?"))
