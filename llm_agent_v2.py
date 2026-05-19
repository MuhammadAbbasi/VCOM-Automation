import os
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
import requests

# Import existing database helpers
from db.db_manager import (
    load_metric, load_latest_snapshot, get_db_stats, get_data_conn, 
    get_logs_conn, get_tracker_summary, get_all_tracker_status,
    load_all_snapshots
)
# Import logic from v1 to reuse
import llm_agent as v1

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("llm_agent_v2")

ROOT = Path(__file__).resolve().parent
_settings = v1.get_user_settings()
OLLAMA_API_URL = _settings.get("ollama_url", "http://localhost:11434")
MODEL_NAME = "qwen2.5:7b"

# ---------------------------------------------------------------------------
# 1. Manual Tools Library
# ---------------------------------------------------------------------------

def get_plant_summary(date: str = None, **kwargs) -> str:
    """Get high-level production and health summary."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    prod = v1.get_total_production(d)
    status = v1.get_inverter_status(d)
    res = {
        "production_mwh": prod.get("total_mwh"),
        "online_count": status.get("online_count"),
        "total_inverters": status.get("total")
    }
    return json.dumps(res)

def get_temperatures(date: str = None, threshold: float = None, **kwargs) -> str:
    """Get latest temperature readings for all inverters, optionally filtering above a threshold (e.g. threshold=50)."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    thresh = float(threshold) if threshold is not None else None
    return json.dumps(v1.get_temperatures(d, thresh), default=str)

def get_dc_currents(date: str = None, threshold: float = None, **kwargs) -> str:
    """Get DC currents for all strings, optionally filtering below a threshold (e.g. threshold=0.1)."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    thresh = float(threshold) if threshold is not None else None
    return json.dumps(v1.get_dc_currents(d, thresh), default=str)

def get_inverter_status(date: str = None, **kwargs) -> str:
    """Get the status of all inverters, identifying which are offline (OFF), low-power (LOW), or active (OK)."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    return json.dumps(v1.get_inverter_status(d), default=str)

def get_total_production(date: str = None, **kwargs) -> str:
    """Get today's total plant energy production in MWh and average production in MW."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    return json.dumps(v1.get_total_production(d), default=str)

def get_peak_production(date: str = None, **kwargs) -> str:
    """Get peak instantaneous power generation in Watts and the time it occurred, with corresponding POA."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    return json.dumps(v1.get_peak_production(d), default=str)

def get_transformer_comparison(date: str = None, **kwargs) -> str:
    """Compare MWh production and active MW across the three transformers (TX1, TX2, TX3)."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    return json.dumps(v1.get_transformer_comparison(d), default=str)

def get_irradiance(date: str = None, **kwargs) -> str:
    """Get latest, peak, and average solar irradiance (POA) readings for the day."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    return json.dumps(v1.get_irradiance(d), default=str)

def get_downtime_events(date: str = None, **kwargs) -> str:
    """Identify which inverters went offline during daylight production hours and for how many minutes."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    return json.dumps(v1.get_downtime_events(d), default=str)

def analyze_alarms(date: str = None, inverter: str = None, type: str = None, **kwargs) -> str:
    """Search for historical alarms."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    return json.dumps(v1.get_alarm_history(d, inverter, type))

def get_latest_readings(metric: str, date: str = None, **kwargs) -> str:
    """Fetch raw sensor data."""
    d = date or datetime.now().strftime("%Y-%m-%d")
    df = load_metric(d, metric)
    if df is None or df.empty: return "No data."
    
    # Filter out rows that are completely empty/null (common in VCOM pre-populated tables)
    data_cols = [c for c in df.columns if c not in ("Ora", "Timestamp Fetch")]
    if data_cols:
        df_valid = df.dropna(subset=data_cols, how='all')
        if not df_valid.empty:
            df = df_valid
            
    return json.dumps(df.tail(3).to_dict(orient='records'))

def get_tracker_data(ncu: str = None, **kwargs) -> str:
    """Get status summary for trackers and NCUs."""
    summary = get_tracker_summary()
    if ncu:
        match = re.search(r'(\d+)', str(ncu))
        ncu_key = f"NCU {int(match.group(1)):02d}" if match else ncu
        stats = summary.get("ncu_stats", {}).get(ncu_key)
        return json.dumps(stats or {"error": f"NCU {ncu_key} not found", "available": list(summary.get("ncu_stats", {}).keys())})
    return json.dumps(summary)

def query_db(sql: str, **kwargs) -> str:
    """Run a read-only SQL query on the plant database. Use for custom analysis."""
    if not sql.strip().upper().startswith("SELECT"):
        return "Error: Only SELECT queries are allowed."
    try:
        import pandas as pd
        conn = get_data_conn()
        df = pd.read_sql_query(sql, conn)
        return df.to_json(orient='records', date_format='iso')
    except Exception as e:
        return f"SQL Error: {e}"

def search_logs(query: str, limit: int = 20, **kwargs) -> str:
    """Search system logs for specific patterns or errors."""
    try:
        conn = get_logs_conn()
        q = f"%{query}%"
        cursor = conn.execute("SELECT * FROM logs WHERE message LIKE ? OR service LIKE ? ORDER BY timestamp DESC LIMIT ?", (q, q, limit))
        cols = [column[0] for column in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        return json.dumps(rows)
    except Exception as e:
        return f"Log Search Error: {e}"

def list_data_files(**kwargs) -> str:
    """List raw data files available in the extraction directory."""
    try:
        files = list(Path(ROOT / "extracted_data").glob("*"))
        return json.dumps([f.name for f in files])
    except Exception as e:
        return f"File Error: {e}"

TOOLS = {
    "get_plant_summary": get_plant_summary,
    "get_temperatures": get_temperatures,
    "get_dc_currents": get_dc_currents,
    "get_inverter_status": get_inverter_status,
    "get_total_production": get_total_production,
    "get_peak_production": get_peak_production,
    "get_transformer_comparison": get_transformer_comparison,
    "get_irradiance": get_irradiance,
    "get_downtime_events": get_downtime_events,
    "analyze_alarms": analyze_alarms,
    "get_latest_readings": get_latest_readings,
    "get_tracker_data": get_tracker_data,
    "get_tracker_summary": get_tracker_data,
    "query_db": query_db,
    "search_logs": search_logs,
    "list_data_files": list_data_files
}

# ---------------------------------------------------------------------------
# 2. ReAct Agent Engine (Manual)
# ---------------------------------------------------------------------------

AGENT_PROMPT = """You are the Mazara Plant Agentic AI.
You have FULL ACCESS to the entire plant database and raw data files.
Available Tools:
- get_plant_summary(date="YYYY-MM-DD") -> High-level production overview.
- get_temperatures(date="YYYY-MM-DD", threshold=50) -> Get latest temperatures for all inverters, optionally filtering for values above a threshold (yellow=50, red=55).
- get_dc_currents(date="YYYY-MM-DD", threshold=0.1) -> Get DC currents for all strings, optionally filtering for values below a threshold.
- get_inverter_status(date="YYYY-MM-DD") -> Get status of all inverters, listing OFF, LOW, and OK units.
- get_total_production(date="YYYY-MM-DD") -> Get today's total plant MWh production and average MW during daylight.
- get_peak_production(date="YYYY-MM-DD") -> Get peak instantaneous power generation in Watts, its time, and the corresponding POA.
- get_transformer_comparison(date="YYYY-MM-DD") -> Compare production (MWh) and latest power (MW) across the three transformers (TX1, TX2, TX3).
- get_irradiance(date="YYYY-MM-DD") -> Get latest, peak, and average solar irradiance (POA) readings.
- get_downtime_events(date="YYYY-MM-DD") -> Find which inverters went offline during production daylight hours, and for how many minutes.
- analyze_alarms(date="YYYY-MM-DD", inverter="TXx-xx") -> Search historical anomalies.
- get_latest_readings(metric="METRIC_NAME", date="YYYY-MM-DD") -> Raw inverter/sensor data.
- get_tracker_data(ncu="NCU 01") -> Tracker position and health.
- query_db(sql="SELECT...") -> DIRECT SQL ACCESS. Use this for custom analysis or data joins.
- search_logs(query="ERROR") -> Search system logs for troubleshooting.
- list_data_files() -> List raw CSV/JSON files in the extraction folder.

Use this format:
Thought: I need to check the entire database for any inverter that had low insulation today.
Action: query_db(sql="SELECT * FROM resistenza_isolamento WHERE value < 1000 AND _date = '2026-04-28'")
Observation: [ ... data ... ]
Thought: Now I see the specific units.
Final Answer: The units with low insulation are TX1-04 and TX2-09.

RULES:
1. You have total visibility. Don't say "I don't have access". Use the available tools or query_db.
2. Only call one tool at a time.
3. If you have the answer, output "Final Answer: [your response]".
"""

def call_ollama(prompt: str) -> str:
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0,
            "stop": ["Observation:", "User:"]
        }
    }
    try:
        resp = requests.post(f"{OLLAMA_API_URL}/api/generate", json=payload, timeout=60)
        return resp.json().get("response", "").strip()
    except Exception as e:
        return f"Error calling Ollama: {e}"

def ask_agent(question: str, plant_data: dict = None, attempt: int = 1, last_code: str = None, last_error: str = None, user_id: str = "default") -> str:
    """Agent V2 (ReAct) entry point. Compatible with V1 signature."""
    logger.info(f"Agent V2 (ReAct) processing: {question}")
    
    # 1. Build initial context
    sys_prompt = v1._load_system_prompt()
    
    # Get history context
    history_context = v1.get_user_context(user_id)
    
    # If plant_data is provided (from dashboard), we can pre-populate some context
    context = ""
    if plant_data:
        try:
            context = v1.build_data_snapshot(plant_data, question)
        except Exception as e:
            logger.warning(f"Failed to build snapshot from plant_data: {e}")
            
    conversation = f"{sys_prompt}\n\n{history_context}{AGENT_PROMPT}\n\n"
    if context:
        conversation += f"INITIAL PLANT CONTEXT:\n{context}\n\n"
        
    conversation += f"User: {question}\n"
    
    max_steps = 5
    for i in range(max_steps):
        logger.info(f"Step {i+1}...")
        response = call_ollama(conversation)
        
        # Prevent AI from hallucinating the observation
        if "Observation:" in response:
            response = response.split("Observation:")[0].strip()
            
        logger.info(f"AI Response: {response}")
        
        if not response:
            return "⚠️ AI failed to generate a response."
            
        conversation += response + "\n"
        
        # Check for Action first (priority)
        action_match = re.search(r"Action:\s*(\w+)\((.*)\)", response)
        if action_match:
            tool_name = action_match.group(1)
            args_str = action_match.group(2)
            
            # Robust parser for args (handles key="val", key='val', key=val, key=TODAY, numbers, etc.)
            kwargs = {}
            if args_str:
                pattern = r'(\w+)\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([a-zA-Z0-9_\-\.]+))'
                matches = re.findall(pattern, args_str)
                for item in matches:
                    k = item[0]
                    val = item[1] or item[2] or item[3]
                    
                    if val is not None:
                        val = val.strip()
                        # Resolve TODAY and YESTERDAY constants
                        if val.upper() == "TODAY":
                            val = datetime.now().strftime("%Y-%m-%d")
                        elif val.upper() == "YESTERDAY":
                            val = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                        elif re.match(r'^\-?\d+$', val):
                            val = int(val)
                        elif re.match(r'^\-?\d+\.\d+$', val):
                            val = float(val)
                        
                        kwargs[k] = val
            
            logger.info(f"Executing tool: {tool_name} with {kwargs}")
            if tool_name in TOOLS:
                try:
                    obs = TOOLS[tool_name](**kwargs)
                except Exception as e:
                    obs = f"Error: {e}"
            else:
                obs = f"Error: Tool {tool_name} not found."
            
            logger.info(f"Observation: {obs}")
            conversation += f"Observation: {obs}\n"
            continue # Move to next step with the observation
            
        # Check for Final Answer only if no action was triggered
        if "Final Answer:" in response:
            ans = response.split("Final Answer:")[1].strip()
            v1._save_history(user_id, question, ans)
            return ans
            
        # If no action and no final answer, something is wrong
        if i == max_steps - 1:
            return response
        conversation += "Thought: I must either call a tool using the 'Action: tool_name(key=\"val\")' format, or output a 'Final Answer: ...'.\n"

    return "⚠️ Agent reached maximum reasoning steps without a final answer."

# Alias for backward compatibility
ask_llm = ask_agent

if __name__ == "__main__":
    print(ask_agent("How is the plant doing?"))
