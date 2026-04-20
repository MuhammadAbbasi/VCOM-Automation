import sys
import json
from pathlib import Path

# Add root to sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

import llm_agent

def test_ai_queries():
    # Load some dummy or real data for the test
    data_dir = ROOT / "extracted_data"
    latest_json = sorted(data_dir.glob("dashboard_data_*.json"))[-1]
    
    with open(latest_json, "r", encoding="utf-8") as f:
        full_data = json.load(f)
        snapshot = full_data[sorted(full_data.keys())[-1]]

    print("--- TEST 1: Single Inverter Status (TX3-01) ---")
    q1 = "Can you tell me the status of inverter TX3-01?"
    print(f"Q: {q1}")
    print(f"A: {llm_agent.ask_llm(q1, snapshot)}")
    print("\n" + "="*50 + "\n")

    print("--- TEST 2: All Inverters in a Transformer (TX1) ---")
    q2 = "What is the status of all inverters in transformer TX1?"
    print(f"Q: {q2}")
    print(f"A: {llm_agent.ask_llm(q2, snapshot)}")

if __name__ == "__main__":
    try:
        test_ai_queries()
    except Exception as e:
        print(f"ERROR DURING TEST: {e}")
