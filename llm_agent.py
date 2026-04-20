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
        "- Available vars: DATA (dict), DATA_DIR (path), TODAY (str: YYYY-MM-DD)."
    )

PROJECT_MEMORY = get_project_context()

def run_python_analysis(code: str, plant_data: dict) -> tuple[str, bool]:
    """Executes code and returns (result, success)."""
    import pandas as pd
    import numpy as np
    namespace = {
        "pd": pd, "np": np, "data": plant_data, "DATA": plant_data,
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
