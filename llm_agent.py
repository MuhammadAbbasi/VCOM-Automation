import json
import logging
import requests
import os
from pathlib import Path
from datetime import datetime

logger = logging.getLogger("llm_agent")
logger.setLevel(logging.INFO)

OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen2.5-coder:7b"
ROOT = Path(__file__).resolve().parent

def get_project_context():
    """Contextual 'knowledge base' for the local Qwen 2.5 Coder model."""
    return (
        "Mazara Solar Plant Monitoring Suite (System Architecture):\n"
        "1. Extraction (vcom_monitor.py): Automated scraping of VCOM Cloud energy data every 15m into daily CSV files.\n"
        "2. Analysis (processor_watchdog_final.py): Real-time anomaly detection using dynamic daylight calculation and CSV trend analysis.\n"
        "3. Intelligence (llm_agent.py): YOU. A local Qwen 2.5 Coder model running sandboxed Python on raw CSV files.\n"
        "4. Interface (telegram_bot.py & dashboard/app.py): Real-time visibility and alerting via Telegram and a Web Dashboard.\n"
        "5. Data Flow: Cloud -> CSV (extracted_data/) -> Watchdog -> State JSON (dashboard_data_*.json)."
    )

def get_plant_topology():
    """Hardcoded plant specs for LLM reasoning."""
    return (
        "Plant Topology - Mazara 01:\n"
        "- Transformers (TX): TX1 (Inv 01-12), TX2 (Inv 13-24), TX3 (Inv 25-36).\n"
        "- Strings: 808 total monitored via 12 MPPTs per inverter.\n"
        "- Key Logic: Production is 'active' if >15 inverters are producing >300W.\n"
        "- Metadata: Location is Mazara del Vallo, Italy."
    )

def get_data_structure_guide():
    """How to use the 'data' variable in the sandbox."""
    return (
        "Sandbox Variable 'data' Structure:\n"
        "- data['macro_health']: {online, last_sync...}\n"
        "- data['inverter_health']: {inv_id: {ac_v, temp_v, pr_v, dc_v, overall_status...}}\n"
        "- Files in 'extracted_data/': Use suffix _{TODAY}.csv (e.g. Potenza_AC_{TODAY}.csv).\n"
        "- CSV Column Patterns: 'Corrente DC MPPT 1 (INV TX1-01) [A]', 'Potenza AC (INV TX1-01) [W]', 'Temperatura inverter (INV TX1-01) [°C]'.\n"
        "- Time Format: 'Ora' column is a decimal (HH.MM), e.g., 6.54 means 06:54 AM.\n"
        "- CSV Loading: Always use load_csv(f'Potenza_AC_{TODAY}.csv') which cleans columns and handles paths.\n"
        "- NAN HANDLING: Metrics in 'data' can be None. Treat None as 0.0 for sums/power. load_csv already fills NaNs with 0.0.\n"
        "- SKILLS: get_tx_totals(m), find_low_performers(m, rat), find_outliers(m, sig), get_peaks(), check_clipping(th), get_mppt_status(id).\n"
        "- Available vars: load_csv(fn), DATA (dict), DATA_DIR (path), TODAY (str: YYYY-MM-DD)."
    )

PROJECT_MEMORY = get_project_context()

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
        f"CORE LOGIC:\n{PROJECT_MEMORY}\n{get_plant_topology()}\n\n"
        f"SANDBOX VARIABLE 'data':\n{get_data_structure_guide()}\n{guide}\n\n"
        f"BRIEF: {len(inverter_list)} inverters. Goal: Answer '{question}'{correction_hint}\n\n"
        f"RULE: Use ```python blocks for logic. Set 'result' variable. No JSON files."
    )

    try:
        # Increase context and reduce temperature for coding stability
        payload = {"model": MODEL_NAME, "prompt": prompt, "stream": False, "options": {"num_ctx": 4096, "temperature": 0.0}}
        resp = requests.post(OLLAMA_API_URL, json=payload, timeout=300)
        resp.raise_for_status()
        answer = resp.json().get("response", "")

        if "```python" in answer:
            # 1. Extract and run code
            code = answer.split("```python")[1].split("```")[0].strip()
            res_val, success = run_python_analysis(code, plant_data)
            
            if not success:
                logger.warning(f"AI analysis failed (Attempt {attempt}): {res_val}")
                return ask_llm(question, plant_data, attempt + 1, code, res_val)
            
            # 2. HIDE CODE: Strip the python block from the final response
            clean_answer = answer.split("```python")[0].strip()
            # If there was text after the code block, grab it too
            parts = answer.split("```")
            if len(parts) > 2:
                clean_answer += "\n" + parts[2].strip()

            # 3. Clean format for the result
            if isinstance(res_val, list):
                result_str = "\n".join([f"• {item}" for item in res_val])
            else:
                result_str = str(res_val)

            final_report = f"{clean_answer}\n\n🔍 **ANALYSIS REPORT:**\n{result_str}"
            return final_report.strip()
            
        return answer
    except Exception as e:
        return f"⚠️ Technical Error: {str(e)}"

if __name__ == "__main__":
    print(ask_llm("Status?"))
