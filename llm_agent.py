import json
import logging
import requests
import os
from pathlib import Path
from datetime import datetime

logger = logging.getLogger("llm_agent")
logger.setLevel(logging.INFO)

OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "llama3.1"
ROOT = Path(__file__).resolve().parent

def get_project_context():
    """Provides a lighter weight summary of the code to avoid bloat."""
    return (
        "Mazara Solar Plant Monitor Logic:\n"
        "- vcom_monitor.py: Uses Playwright to scrape VCOM dashboard every 15m.\n"
        "- processor_watchdog_final.py: Analyzes CSVs for anomalies.\n"
        "- telegram_bot.py: Sends alerts and handles /ai command."
    )

def get_plant_topology():
    """Detailed map of the plant for reasoning."""
    return (
        "Plant Topology (Mazara 01):\n"
        "- Total: 36 SMA Inverters + 808 Strings.\n"
        "- Domains: TX1 (01-12), TX2 (13-24), TX3 (25-36).\n"
        "- Sensors: POA (Irradiance), T_Amb, T_Mod."
    )

def get_data_structure_guide():
    """How to use the 'data' variable in the sandbox."""
    return (
        "Sandbox Variable 'data' Structure:\n"
        "- data['macro_health']: {online, last_sync...}\n"
        "- data['inverter_health']: {inv_id: {ac_power_kw, temperature, pr...}}\n"
        "- data['active_anomalies']: list of alerts\n"
        "- Files in 'extracted_data/': Potenza_AC, Corrente_DC, Temperatura, PR, Irraggiamento."
    )

PROJECT_MEMORY = get_project_context()

def run_python_analysis(code: str, plant_data: dict) -> tuple[str, bool]:
    """Executes code and returns (result, success)."""
    import pandas as pd
    import numpy as np
    namespace = {
        "pd": pd, "np": np, "data": plant_data, "DATA": plant_data,
        "result": None, "ROOT": ROOT, "DATA_DIR": ROOT / "extracted_data"
    }
    try:
        exec(code, namespace)
        res = namespace.get("result")
        return (str(res) if res is not None else "Execution finished (no result set).", True)
    except Exception as e:
        return (str(e), False)

def ask_llm(question: str, plant_data: dict = None, attempt: int = 1) -> str:
    """Highly efficient agent with self-correction."""
    if attempt > 2:
        return "⚠️ AI Error: Script failed after retries."

    inverter_list = list(plant_data.get("inverter_health", {}).keys()) if plant_data else []
    
    prompt = (
        f"CORE LOGIC:\n{PROJECT_MEMORY}\n{get_plant_topology()}\n\n"
        f"SANDBOX VARIABLE 'data':\n{get_data_structure_guide()}\n\n"
        f"BRIEF: {len(inverter_list)} inverters.\n\n"
        f"QUESTION: {question}\n\n"
        f"RULE: Write python in ```python blocks. Set 'result'. If fixing an error, explain it."
    )

    try:
        payload = {"model": MODEL_NAME, "prompt": prompt, "stream": False, "options": {"num_ctx": 4096, "temperature": 0.1}}
        resp = requests.post(OLLAMA_API_URL, json=payload, timeout=120)
        resp.raise_for_status()
        answer = resp.json().get("response", "")

        if "```python" in answer:
            code = answer.split("```python")[1].split("```")[0].strip()
            res_val, success = run_python_analysis(code, plant_data)
            
            if not success:
                retry_q = f"Code failed: {res_val}. Rewrite and fix it for: {question}"
                return ask_llm(retry_q, plant_data, attempt + 1)
            
            return f"{answer}\n\n[ANALYSIS RESULT]:\n{res_val}"
            
        return answer
    except Exception as e:
        return f"⚠️ Technical Error: {str(e)}"

if __name__ == "__main__":
    print(ask_llm("Status?"))
