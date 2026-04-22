import pandas as pd
from pathlib import Path
import json

ROOT = Path(".")
DATA_DIR = ROOT / "extracted_data"
date_str = "2026-04-22"
filename = f"Potenza AC_{date_str}.csv"
path = DATA_DIR / filename

print(f"Checking {path}")
if not path.exists():
    print("File not found")
else:
    try:
        df = pd.read_csv(path)
        print(f"Loaded {len(df)} rows. Columns: {len(df.columns)}")
        df.columns = [str(c).strip() for c in df.columns]
        
        print("Sample raw values from TX1-01 (row 500):")
        if len(df) > 500:
            print(df.iloc[500, 2])
            
        for col in df.columns:
            if col in ("Ora", "Timestamp Fetch"):
                continue
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
        
        print("Sample cleaned values from TX1-01 (row 500):")
        if len(df) > 500:
             print(df.iloc[500, 2])
             print(f"Dtype: {df.iloc[:, 2].dtype}")
        
        result = {}
        for tx in ["TX1", "TX2", "TX3"]:
            cols = [c for c in df.columns if f"(INV {tx}-" in c]
            if cols:
                # Check sum of first col to see if it even has data
                sum_first = df[cols[0]].sum()
                print(f"{tx} - First inverter sum: {sum_first}")
                
                total_mwh = round(float(df[cols].sum().sum() * (1/60)) / 1_000_000, 3)
                latest_mw = round(float(df[cols].iloc[-1].sum()) / 1_000_000, 3) if not df[cols].dropna(how='all').empty else 0
                
                # Find the actual latest valid row
                valid_rows = df[cols].dropna(how='all')
                if not valid_rows.empty:
                    true_latest_mw = round(float(valid_rows.iloc[-1].sum()) / 1_000_000, 3)
                    true_latest_ora = df.loc[valid_rows.index[-1], "Ora"]
                    print(f"{tx} - True latest: {true_latest_mw} MW at Ora {true_latest_ora}")
                else:
                    print(f"{tx} - No valid rows found")
                
                result[tx] = {"total_mwh": total_mwh, "latest_mw": latest_mw, "inverter_count": len(cols)}

        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {e}")
