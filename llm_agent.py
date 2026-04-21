import json
import logging
import requests
import os
from pathlib import Path
from datetime import datetime

logger = logging.getLogger("llm_agent")
logger.setLevel(logging.INFO)

OLLAMA_API_URL = "http://192.168.10.126:11434/api/generate"
MODEL_NAME = "qwen2.5:7b"
ROOT = Path(__file__).resolve().parent
DEBUG_MODE = True # User-requested transparency

def get_project_context():
    """Contextual 'knowledge base' for the remote Qwen 3.5 model."""
    return (
        "Mazara Suite: Cloud -> CSV (extracted_data/) -> Watchdog -> State JSON (data var).\n"
        "AI: Qwen 3.5. Task: Site Forensic Analysis."
    )

def get_plant_topology():
    """Hardcoded plant specs for LLM reasoning."""
    return (
        "Plant Topology - Mazara 01:\n"
        "- Transformers (TX): TX1 (Inv 01-12), TX2 (Inv 13-24), TX3 (Inv 25-36).\n"
        "- Inverters are named: INV TX1-01 through INV TX3-12.\n"
        "- Key Logic: Production is 'active' if >15 inverters are producing >300W.\n"
        "- Metadata: Location is Mazara del Vallo, Italy."
    )

def get_data_structure_guide():
    """How to use the 'data' variable in the sandbox."""
    return (
        "STATE 'data': {macro_health: {MW, PR}, inverter_health: {id: {ac_v, temp_v, pr_v, dc_v, overall_status}}, active_anomalies: []}\n"
        "CSV: load_csv(fn) fills NaN with 0. Files: Potenza_AC_{TODAY}.csv, Corrente_DC_{TODAY}.csv, Temperatura_{TODAY}.csv.\n"
        "MPPT: get_mppt_imbalance(id) returns {MPPT_1: val...}."
    )

PROJECT_MEMORY = get_project_context()
TOPOLOGY = get_plant_topology()
GUIDE = get_data_structure_guide()

def run_python_analysis(code: str, plant_data: dict) -> tuple[str, bool]:
    """Executes code and returns (result, success)."""
    import pandas as pd
    import numpy as np
    def load_csv(filename):
        path = (ROOT / "extracted_data" / filename)
        if not path.exists(): return pd.DataFrame()
        try:
            df = pd.read_csv(path)
            # Clean column names once for the AI
            df.columns = [str(c).strip() for c in df.columns]
            # Convert all numeric-looking columns to float, handling commas/dots
            for col in df.columns:
                if col == "Ora": continue
                try:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
                except:
                    pass
            # Fill NaNs with 0 for production metrics
            df = df.fillna(0.0)
            return df
        except Exception as e:
            logger.error(f"AI load_csv error: {e}")
            return pd.DataFrame()

    def get_transformer_totals(metric='ac_v'):
        """Sums a metric by transformer (TX1, TX2, TX3)."""
        tx_stats = {"TX1": 0.0, "TX2": 0.0, "TX3": 0.0}
        for inv_id, vals in plant_data.get("inverter_health", {}).items():
            val = vals.get(metric, 0.0) or 0.0
            if "TX1" in inv_id: tx_stats["TX1"] += val
            elif "TX2" in inv_id: tx_stats["TX2"] += val
            elif "TX3" in inv_id: tx_stats["TX3"] += val
        return tx_stats

    def find_underperformers(metric='ac_v', threshold_ratio=0.85):
        """Finds inverters producing < X% of the median."""
        health = plant_data.get("inverter_health", {})
        vals = {k: (v.get(metric, 0.0) or 0.0) for k, v in health.items()}
        valid_vals = [v for v in vals.values() if v > 0]
        if not valid_vals: return []
        median_val = np.median(valid_vals)
        return [k for k, v in vals.items() if v < median_val * threshold_ratio]

    def detect_outliers(metric='temp_v', sigma=2.0):
        """Finds inverters with metrics significantly different from average."""
        health = plant_data.get("inverter_health", {})
        vals = {k: (v.get(metric, 0.0) or 0.0) for k, v in health.items()}
        valid_vals = list(vals.values())
        if len(valid_vals) < 5: return []
        avg, std = np.mean(valid_vals), np.std(valid_vals)
        return [k for k, v in vals.items() if abs(v - avg) > (sigma * std)]

    def get_peak_power():
        """Returns the maximum AC power seen today for each inverter."""
        peaks = {}
        df = load_csv(f"Potenza_AC_{datetime.now().strftime('%Y-%m-%d')}.csv")
        if df.empty: return {}
        for col in df.columns:
            if "Potenza AC (INV" in col:
                peaks[col] = df[col].max()
        return peaks

    def check_clipping(threshold=59500):
        """Identifies inverters that are plateauing at high power (clipping)."""
        df = load_csv(f"Potenza_AC_{datetime.now().strftime('%Y-%m-%d')}.csv")
        if df.empty: return []
        clipping = []
        for col in df.columns:
            if "Potenza AC" in col:
                # If last 3 values are near peak and very close to each other
                tail = df[col].tail(10)
                if tail.max() > threshold and tail.std() < 50:
                    clipping.append(col)
        return clipping

    def get_mppt_imbalance(inv_id):
        """Compares MPPT currents for a single inverter to find string faults."""
        df = load_csv(f"Corrente_DC_{datetime.now().strftime('%Y-%m-%d')}.csv")
        if df.empty: return {}
        mppt_cols = [c for c in df.columns if f"(INV {inv_id})" in c and "MPPT" in c]
        if not mppt_cols: return {}
        # Look at the latest non-zero row
        latest = df[mppt_cols].tail(5).mean()
        return latest.to_dict()

    if plant_data is None: plant_data = {}
    
    namespace = {
        "pd": pd, "np": np, "data": plant_data, "DATA": plant_data,
        "load_csv": load_csv, 
        "get_tx_totals": get_transformer_totals,
        "find_low_performers": find_underperformers,
        "find_outliers": detect_outliers,
        "get_peaks": get_peak_power,
        "check_clipping": check_clipping,
        "get_mppt_status": get_mppt_imbalance,
        "get_mppt_imbalance": get_mppt_imbalance,
        "result": None, "ROOT": ROOT, "DATA_DIR": ROOT / "extracted_data",
        "TODAY": datetime.now().strftime("%Y-%m-%d")
    }
    try:
        exec(code, namespace)
        res = namespace.get("result")
        return (res if res is not None else "Execution finished.", True)
    except Exception as e:
        return (str(e), False)

def ask_llm(question: str, plant_data: dict = None, attempt: int = 1, last_code: str = None, last_error: str = None) -> str:
    """Highly efficient agent with self-correction."""
    if attempt > 3:
        return f"⚠️ AI Error: Deep analysis failed after multiple attempts.\nLast Traceback: {last_error}"

    inverter_list = list(plant_data.get("inverter_health", {}).keys()) if plant_data else []
    
    # Guide for keys that the AI should use
    guide = (
        "Key fields in data['inverter_health'][inv_id]:\n"
        "- 'dc_v': DC Current (A)\n"
        "- 'ac_v': AC Power (W)\n"
        "- 'pr_v': Performance Ratio (%)\n"
        "- 'temp_v': Temperature (°C)"
    )

    correction_hint = ""
    if last_code and last_error:
        correction_hint = f"\n\nPREVIOUS ATTEMPT FAILED:\nCode:\n```python\n{last_code}\n```\nError: {last_error}\nPlease fix the logic and try again."

    prompt = (
        f"DEBUG MODE IS ACTIVE. Explain your logic before writing code.\n"
        f"PLANT CONTEXT:\n{PROJECT_MEMORY}\n{TOPOLOGY}\n\n"
        f"DATA STRUCTURE:\n{GUIDE}\n\n"
        f"TASK: Answer briefly using the 'data' variable if possible. Only write code if necessary.\n"
        f"QUESTION: {question}{correction_hint}"
    )

    try:
        print(f"\n[AI] (ANALYZING) Question: {question}")
    except: pass
    
    try:
        payload = {
            "model": MODEL_NAME, 
            "prompt": prompt, 
            "stream": False, 
            "options": {"num_ctx": 4096, "temperature": 0.0}
        }
        resp = requests.post(OLLAMA_API_URL, json=payload, timeout=300)
        resp.raise_for_status()
        answer = resp.json().get("response", "").strip()
        
        if not answer:
            return "⚠️ AI Error: Remote server returned an empty response. Please try rephrasing."
        
        try:
            print(f"[AI] (RECEIVED) Response length: {len(answer)}")
        except: pass

        if "```python" in answer:
            # 1. Extract and run code
            code = answer.split("```python")[1].split("```")[0].strip()
            res_val, success = run_python_analysis(code, plant_data)
            
            if not success:
                logger.warning(f"AI analysis failed (Attempt {attempt}): {res_val}")
                return ask_llm(question, plant_data, attempt + 1, code, res_val)
            
            if not DEBUG_MODE:
                # 2. HIDE CODE: Strip the python block from the final response
                clean_answer = answer.split("```python")[0].strip()
                # If there was text after the code block, grab it too
                parts = answer.split("```")
                if len(parts) > 2:
                    clean_answer += "\n" + parts[2].strip()
                answer_to_show = clean_answer
            else:
                answer_to_show = answer # Show everything in Debug mode

            # 3. Clean format for the result
            if isinstance(res_val, list):
                result_str = "\n".join([f"• {item}" for item in res_val])
            else:
                result_str = str(res_val)

            final_report = f"{answer_to_show}\n\n🔍 **ANALYSIS REPORT:**\n{result_str}"
            return final_report.strip()
            
        return answer
    except Exception as e:
        return f"⚠️ Technical Error: {str(e)}"

if __name__ == "__main__":
    print(ask_llm("Status?"))
