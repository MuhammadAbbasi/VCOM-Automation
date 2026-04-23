import pandas as pd
from pathlib import Path
import json
from db.db_manager import load_metric

ROOT = Path(".")
date_str = "2026-04-23"
metric_name = "Potenza AC"

print(f"Checking {metric_name} for {date_str} in Database...")
df = load_metric(date_str, metric_name)

if df is None:
    print("Not found in Database, checking CSV...")
    DATA_DIR = ROOT / "extracted_data"
    filename = f"{metric_name}_{date_str}.csv"
    path = DATA_DIR / filename
    if not path.exists():
        print("CSV File not found either")
        exit()
    df = pd.read_csv(path)
    # Clean it like the old logic
    df.columns = [str(c).strip() for c in df.columns]
    for col in df.columns:
        if col in ("Ora", "Timestamp Fetch"): continue
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

print(f"Loaded {len(df)} rows. Columns: {len(df.columns)}")

print("Sample values from TX1-01 (row 500):")
if len(df) > 500:
    # Find column by TX1-01
    cols = [c for c in df.columns if "TX1-01" in c]
    if cols:
        print(f"Column: {cols[0]}")
        print(f"Value: {df.iloc[500][cols[0]]}")
        print(f"Dtype: {df[cols[0]].dtype}")

result = {}
for tx in ["TX1", "TX2", "TX3"]:
    cols = [c for c in df.columns if f"(INV {tx}-" in c]
    if cols:
        total_mwh = round(float(df[cols].sum().sum() * (1/60)) / 1_000_000, 3)
        valid_rows = df[cols].dropna(how='all')
        if not valid_rows.empty:
            true_latest_mw = round(float(valid_rows.iloc[-1].sum()) / 1_000_000, 3)
            true_latest_ora = df.loc[valid_rows.index[-1], "Ora"]
            print(f"{tx} - True latest: {true_latest_mw} MW at Ora {true_latest_ora}")
        else:
            print(f"{tx} - No valid rows found")
        
        result[tx] = {"total_mwh": total_mwh, "inverter_count": len(cols)}

print(json.dumps(result, indent=2))
