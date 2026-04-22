import json
import logging
import requests
import os
import re
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
OLLAMA_API_URL = _settings.get("ollama_url", "http://192.168.10.126:11434/api/generate")
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
# CSV Data Engine — Pre-built analysis functions
# ---------------------------------------------------------------------------
import pandas as pd
import numpy as np

INV_IDS = [f"INV TX{tx}-{i:02d}" for tx in range(1, 4) for i in range(1, 13)]

def _load_csv(filename):
    """Load a CSV from extracted_data, clean it, return DataFrame."""
    path = DATA_DIR / filename
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        df.columns = [str(c).strip() for c in df.columns]
        for col in df.columns:
            if col in ("Ora", "Timestamp Fetch"):
                continue
            try:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
            except:
                pass
        return df
    except Exception as e:
        logger.error(f"load_csv error for {filename}: {e}")
        return pd.DataFrame()

def _get_date_str(offset_days=0):
    return (datetime.now() - timedelta(days=offset_days)).strftime("%Y-%m-%d")

def get_available_dates():
    """List all dates that have AC power data."""
    dates = set()
    for f in DATA_DIR.glob("Potenza AC_*.csv"):
        try:
            d = f.stem.replace("Potenza AC_", "")
            dates.add(d)
        except:
            pass
    # Also check old naming
    for f in DATA_DIR.glob("Potenza_AC_*.csv"):
        try:
            d = f.stem.replace("Potenza_AC_", "")
            dates.add(d)
        except:
            pass
    return sorted(dates, reverse=True)

def get_total_production(date_str=None):
    """Total plant energy in MWh for a given date."""
    date_str = date_str or _get_date_str()
    df = _load_csv(f"Potenza AC_{date_str}.csv")
    if df.empty:
        df = _load_csv(f"Potenza_AC_{date_str}.csv")
    if df.empty:
        return {"date": date_str, "total_mwh": None, "error": "No data"}
    ac_cols = [c for c in df.columns if "Potenza AC (INV" in c or "Potenza AC(INV" in c]
    if not ac_cols:
        return {"date": date_str, "total_mwh": None, "error": "No inverter columns"}
    df[ac_cols] = df[ac_cols].fillna(0)
    total_mwh = round(float(df[ac_cols].sum().sum() * (1/60)) / 1_000_000, 3)
    return {"date": date_str, "total_mwh": total_mwh, "inverter_count": len(ac_cols)}

def get_temperatures(date_str=None, threshold=None):
    """Get latest temperature readings for all inverters."""
    date_str = date_str or _get_date_str()
    df = _load_csv(f"Temperatura_{date_str}.csv")
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
    date_str = date_str or _get_date_str()
    df = _load_csv(f"Potenza AC_{date_str}.csv")
    if df.empty:
        df = _load_csv(f"Potenza_AC_{date_str}.csv")
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

def get_transformer_comparison(date_str=None):
    """Compare TX1, TX2, TX3 production."""
    date_str = date_str or _get_date_str()
    df = _load_csv(f"Potenza AC_{date_str}.csv")
    if df.empty:
        df = _load_csv(f"Potenza_AC_{date_str}.csv")
    if df.empty:
        return {"date": date_str, "error": "No data"}
    
    result = {}
    for tx in ["TX1", "TX2", "TX3"]:
        cols = [c for c in df.columns if f"(INV {tx}-" in c]
        if cols:
            df[cols] = df[cols].fillna(0)
            total_mwh = round(float(df[cols].sum().sum() * (1/60)) / 1_000_000, 3)
            latest_mw = round(float(df[cols].iloc[-1].sum()) / 1_000_000, 3) if not df[cols].dropna(how='all').empty else 0
            result[tx] = {"total_mwh": total_mwh, "latest_mw": latest_mw, "inverter_count": len(cols)}
    return {"date": date_str, "transformers": result}

def get_dc_currents(date_str=None, threshold=None):
    """Get DC current info for strings/MPPTs."""
    date_str = date_str or _get_date_str()
    df = _load_csv(f"Corrente DC_{date_str}.csv")
    if df.empty:
        df = _load_csv(f"Corrente_DC_{date_str}.csv")
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
    
    output = {"date": date_str, "total_strings": len(dc_cols)}
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
    date_str = date_str or _get_date_str()
    df = _load_csv(f"Irraggiamento_{date_str}.csv")
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
    return {"date": date_str, "latest": latest_vals, "peak": peak_vals}

def get_downtime_events(date_str=None):
    """Check which inverters went offline during production hours."""
    date_str = date_str or _get_date_str()
    df = _load_csv(f"Potenza AC_{date_str}.csv")
    if df.empty:
        df = _load_csv(f"Potenza_AC_{date_str}.csv")
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

# ---------------------------------------------------------------------------
# Data Snapshot Builder — builds the context the LLM reads
# ---------------------------------------------------------------------------
def build_data_snapshot(plant_data, question):
    """Pre-compute relevant data based on the question keywords."""
    snapshot = []
    q = question.lower()
    today = _get_date_str()
    yesterday = _get_date_str(1)
    
    # Determine target date from question
    target_date = today
    if "yesterday" in q or "ieri" in q:
        target_date = yesterday
    else:
        # Check for explicit date  YYYY-MM-DD
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', q)
        if date_match:
            target_date = date_match.group(1)
    
    # Always include plant state if available
    if plant_data:
        macro = plant_data.get("macro_health", {})
        anomalies = plant_data.get("active_anomalies", [])
        trail = plant_data.get("historical_trail", [])
        snapshot.append(f"LIVE STATE (from watchdog): MW={macro.get('MW','?')}, PR={macro.get('PR','?')}%")
        
        if anomalies:
            snapshot.append(f"ACTIVE ANOMALIES: {json.dumps(anomalies[:10], default=str)}")
        else:
            snapshot.append("ACTIVE ANOMALIES: None")

        if any(w in q.lower() for w in ["history", "historical", "trail", "past", "last alarms"]):
            if trail:
                # Include the 20 most recent historical records
                snapshot.append(f"HISTORICAL ALARM TRAIL (Resolved): {json.dumps(trail[-20:], default=str)}")
            else:
                snapshot.append("HISTORICAL ALARM TRAIL: None")
    
    # Smart routing — compute data relevant to the question
    if any(w in q for w in ["temperature", "temp", "hot", "°c", "caldo", "thermal", "heat", "highest", "lowest", "max temp", "min temp", "warm"]):
        threshold = 50  # default
        t_match = re.search(r'(\d+)\s*°?\s*[cC]', q)
        if t_match:
            threshold = float(t_match.group(1))
        data = get_temperatures(target_date, threshold)
        snapshot.append(f"TEMPERATURE DATA ({target_date}):\n{json.dumps(data, indent=2, default=str)}")
    
    if any(w in q for w in ["production", "energy", "mwh", "kwh", "produzione", "total", "power", "quanto", "generate", "yield"]):
        data = get_total_production(target_date)
        snapshot.append(f"PRODUCTION DATA ({target_date}):\n{json.dumps(data, indent=2, default=str)}")
    
    if any(w in q for w in ["tx1", "tx2", "tx3", "transformer", "compare", "comparison", "confronto"]):
        data = get_transformer_comparison(target_date)
        snapshot.append(f"TRANSFORMER COMPARISON ({target_date}):\n{json.dumps(data, indent=2, default=str)}")
    
    if any(w in q for w in ["off", "down", "zero", "offline", "stopped", "trip", "fault", "spento", "not working", "not producing"]):
        data = get_inverter_status(target_date)
        snapshot.append(f"INVERTER STATUS ({target_date}):\n{json.dumps(data, indent=2, default=str)}")
        dt_data = get_downtime_events(target_date)
        snapshot.append(f"DOWNTIME EVENTS ({target_date}):\n{json.dumps(dt_data, indent=2, default=str)}")
    
    if any(w in q.lower() for w in ["current", "dc", "string", "mppt", "corrente"]):
        threshold = None
        t_match = re.search(r'(?:less than|below|under|<)\s*(\d+)', q)
        if t_match:
            threshold = float(t_match.group(1))
        
        # FOCUS OPTIMIZATION: If a specific inverter is mentioned (e.g. TX3-01)
        # only send that inverter's data to avoid overwhelming the model.
        target_inv = None
        inv_match = re.search(r'([tT][xX]\d+-\d+)', q)
        if inv_match:
            target_inv = inv_match.group(1).upper().replace("-", "-")
            
        data = get_dc_currents(target_date, threshold)
        if target_inv:
            # Filter the large DC dict for just the target inverter
            filtered_data = {k: v for k, v in data.items() if target_inv in k}
            snapshot.append(f"DC CURRENT DATA (FOCUS: {target_inv} on {target_date}):\n{json.dumps(filtered_data, indent=2, default=str)}")
        else:
            snapshot.append(f"DC CURRENT DATA ({target_date}):\n{json.dumps(data, indent=2, default=str)}")
    
    if any(w in q for w in ["irradiance", "sun", "irraggiamento", "solar", "radiation", "pyranometer"]):
        data = get_irradiance(target_date)
        snapshot.append(f"IRRADIANCE DATA ({target_date}):\n{json.dumps(data, indent=2, default=str)}")
    
    if any(w in q for w in ["alert", "alarm", "anomaly", "warning", "problem", "issue"]):
        data = get_inverter_status(target_date)
        snapshot.append(f"INVERTER STATUS ({target_date}):\n{json.dumps(data, indent=2, default=str)}")
        temp_data = get_temperatures(target_date, 50)
        snapshot.append(f"HIGH TEMP CHECK ({target_date}):\n{json.dumps(temp_data, indent=2, default=str)}")
    
    if any(w in q for w in ["early hours", "morning", "mattina"]):
        data = get_downtime_events(target_date)
        snapshot.append(f"DOWNTIME DATA ({target_date}):\n{json.dumps(data, indent=2, default=str)}")
        inv_data = get_inverter_status(target_date)
        snapshot.append(f"INVERTER STATUS ({target_date}):\n{json.dumps(inv_data, indent=2, default=str)}")
    
    if any(w in q for w in ["inverter", "inv ", "inv.", "health", "performance", "pr ", "pr%"]):
        inv_data = get_inverter_status(target_date)
        if f"INVERTER STATUS" not in "\n".join(snapshot):
            snapshot.append(f"INVERTER STATUS ({target_date}):\n{json.dumps(inv_data, indent=2, default=str)}")
    
    if any(w in q for w in ["status", "how is", "overview", "summary", "come sta", "plant ok"]):
        prod = get_total_production(target_date)
        inv = get_inverter_status(target_date)
        temp = get_temperatures(target_date, 50)
        if f"PRODUCTION" not in "\n".join(snapshot):
            snapshot.append(f"PRODUCTION ({target_date}): {json.dumps(prod, default=str)}")
        if f"INVERTER STATUS" not in "\n".join(snapshot):
            snapshot.append(f"INVERTER STATUS ({target_date}): offline={inv.get('off_inverters', [])}, online={inv.get('online_count', '?')}/{inv.get('total', '?')}")
        if f"TEMPERATURE" not in "\n".join(snapshot):
            above50 = temp.get('above_threshold', {})
            snapshot.append(f"TEMP CHECK: {len(above50)} inverters above 50°C: {list(above50.keys()) if above50 else 'None'}")
    
    # If nothing was triggered, provide general overview
    if not snapshot or len(snapshot) <= 2:
        prod = get_total_production(target_date)
        inv = get_inverter_status(target_date)
        snapshot.append(f"PRODUCTION ({target_date}): {json.dumps(prod, default=str)}")
        snapshot.append(f"INVERTER STATUS ({target_date}): offline={inv.get('off_inverters', [])}, online={inv.get('online_count', '?')}/{inv.get('total', '?')}")
    
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
        "pd": pd, "np": np, "os": os, "re": re, "json": json,
        "data": plant_data, "DATA": plant_data,
        "datetime": datetime, "timedelta": timedelta,
        "load_csv": _load_csv, "DATA_DIR": DATA_DIR, "ROOT": ROOT,
        "get_total_production": get_total_production,
        "get_temperatures": get_temperatures,
        "get_inverter_status": get_inverter_status,
        "get_transformer_comparison": get_transformer_comparison,
        "get_dc_currents": get_dc_currents,
        "get_irradiance": get_irradiance,
        "get_downtime_events": get_downtime_events,
        "get_available_dates": get_available_dates,
        "INV_IDS": INV_IDS,
        "TODAY": _get_date_str(),
        "YESTERDAY": _get_date_str(1),
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
        resp = requests.post(OLLAMA_API_URL, json=payload, timeout=300)
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
