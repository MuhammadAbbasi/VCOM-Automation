import pandas as pd
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mazara Plant Configuration (Loaded from CSV)
# ---------------------------------------------------------------------------

def load_mppt_config() -> dict:
    """Load the 36x12 MPPT string configuration from the CSV."""
    csv_path = Path(__file__).parent / "plant_configuration_original.csv"
    config = {}
    if not csv_path.exists():
        logger.error(f"Config CSV NOT FOUND at {csv_path}")
        return {}
    
    try:
        # T1;11;2;1 -> Inverse mapping to INV TX1-01
        # TX1 maps to T1, TX2 to T2, TX3 to T3
        # TX1-01 maps to T1;11
        # TX1-10 maps to T1;110
        df_cfg = pd.read_csv(csv_path, sep=";", encoding="utf-8", dtype={1: str})
        
        for _, row in df_cfg.iterrows():
            area = str(row.iloc[0]).strip() # T1
            box = str(row.iloc[1]).strip()  # 1.01 or 11
            strings = int(row.iloc[2])      # 2
            mppt_num = int(row.iloc[3])     # 1
            
            # Convert area T1 -> TX1
            tx_area = area.replace("T", "TX")
            
            # Handle both T.IN (1.01) and legacy (11) formats
            if "." in box:
                # 1.10 -> 10, 2.01 -> 1
                try:
                    box_num = int(box.split(".")[1])
                except:
                    box_num = int(float(box) % 1 * 100 + 0.5) 
            else:
                # 11 -> 1, 112 -> 12
                box_num = int(box[1:]) 
                
            inv_key = f"{tx_area}-{box_num:02d}"
            
            if inv_key not in config:
                config[inv_key] = [0] * 12
            
            if 1 <= mppt_num <= 12:
                config[inv_key][mppt_num-1] = strings
        
        return config
    except Exception as e:
        logger.error(f"Error loading plant config at {csv_path}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}

MPPT_CONFIG = load_mppt_config()

# ... (get_current_streak_minutes remains same)

# ... (analyze_dc_current start remains same)

def get_current_streak_minutes(mask, times):
    """Return the duration in minutes of the currently active fault streak."""
    # Ensure we only look at non-NA data
    combined = pd.concat([mask, times], axis=1).dropna()
    if combined.empty: return 0
    
    s = combined.iloc[:, 0].astype(int)
    t = combined.iloc[:, 1]
    
    if s.iloc[-1] == 0: 
        return 0
        
    blocks = (s != s.shift()).cumsum()
    last_block = blocks.iloc[-1]
    last_streak_times = t[blocks == last_block]
    
    if last_streak_times.empty: return 0
    
    # Calculate duration based on Ora values (Decimal Hours)
    def to_total_minutes(val):
        h = int(val)
        m = round((val - h) * 60)
        return h * 60 + m
        
    start_m = to_total_minutes(last_streak_times.iloc[0])
    end_m = to_total_minutes(last_streak_times.iloc[-1])
    
    # We add 15 minutes to account for the interval the last point represents
    return int((end_m - start_m) + 15)

def analyze_dc_current(dc_df: pd.DataFrame, output_md_path: Path, date_str: str):
    """Parses Corrente_DC dataset, calculates thresholds, and generates MD report."""
    if dc_df is None or len(dc_df) == 0:
        logger.warning("No DC Current DataFrame provided to analyzer.")
        return

    # Clean data & filter daylight (06:30 - 19:00)
    df = dc_df.copy()
    
    # Internal fail-safe deduplication
    if "Ora" in df.columns:
        df = df.drop_duplicates(subset=["Ora"], keep="last").reset_index(drop=True)

    df.replace(['x', ' x '], np.nan, inplace=True)
    if "Ora" in df.columns:
        df["Ora"] = pd.to_numeric(df["Ora"], errors="coerce")
        df = df[(df["Ora"] >= 6.30) & (df["Ora"] <= 19.00)].copy()

    for col in df.columns:
        if col not in ["Ora", "Timestamp Fetch", "Data"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Global 2-string median for fleet expected current
    all_2_string_cols = []
    for inv, mppt_cfg in MPPT_CONFIG.items():
        for i, strings in enumerate(mppt_cfg):
            if strings == 2:
                col = f"Corrente DC MPPT {i+1} (INV {inv}) [A]"
                if col in df.columns: all_2_string_cols.append(col)

    if not all_2_string_cols: return
    fleet_2str_median = df[all_2_string_cols].median(axis=1)

    faults = []
    inv_summary = {inv: {"Domain": inv[:3], "Status": "Online", "Critical": 0, "Warnings": 0, "Info": 0, "Notes": []} for inv in MPPT_CONFIG}

    for inv, mppt_cfg in MPPT_CONFIG.items():
        inv_cols = [c for c in df.columns if inv in c]
        mppt_cols = [c for c in inv_cols if "MPPT" in c]
        
        # Inverter Offline exceptions
        if not inv_cols:
            inv_summary[inv]["Status"] = "Offline"
            inv_summary[inv]["Notes"].append("No channels data found in CSV")
            continue
            
        # Check if all available columns (MPPT or aggregate) are NaN
        if df[inv_cols].isna().all(axis=None):
            inv_summary[inv]["Status"] = "Offline"
            inv_summary[inv]["Notes"].append("All data reads missing/x all day")
            continue

        # Single Inverter Medians
        inv_2str_cols = [f"Corrente DC MPPT {i+1} (INV {inv}) [A]" for i, s in enumerate(mppt_cfg) if s == 2 and f"Corrente DC MPPT {i+1} (INV {inv}) [A]" in df.columns]
        inv_2str_median = df[inv_2str_cols].median(axis=1) if inv_2str_cols else pd.Series(np.nan, index=df.index)
        inv_1str_cols = [f"Corrente DC MPPT {i+1} (INV {inv}) [A]" for i, s in enumerate(mppt_cfg) if s == 1 and f"Corrente DC MPPT {i+1} (INV {inv}) [A]" in df.columns]
        inv_1str_median = df[inv_1str_cols].median(axis=1) if inv_1str_cols else pd.Series(np.nan, index=df.index)

    mppt_details = {}

    for inv, mppt_cfg in MPPT_CONFIG.items():
        inv_label = f"INV {inv}"
        mppt_details[inv_label] = []

        inv_cols = [c for c in df.columns if inv in c]
        
        # Single Inverter Medians
        inv_2str_cols = [f"Corrente DC MPPT {i+1} (INV {inv}) [A]" for i, s in enumerate(mppt_cfg) if s == 2 and f"Corrente DC MPPT {i+1} (INV {inv}) [A]" in df.columns]
        inv_2str_median = df[inv_2str_cols].median(axis=1) if inv_2str_cols else pd.Series(np.nan, index=df.index)

        domain = inv[:3]

        for mppt_idx, string_count in enumerate(mppt_cfg):
            mppt_num = mppt_idx + 1
            col_name = f"Corrente DC MPPT {mppt_num} (INV {inv}) [A]"
            
            # Fallback for aggregate columns
            if col_name not in df.columns:
                if mppt_num == 1:
                    alt_col = f"Corrente DC (INV {inv}) [A]"
                    if alt_col in df.columns: col_name = alt_col
                    else:
                        mppt_details[inv_label].append({"mppt": mppt_num, "strings": string_count, "v": None, "exp": None})
                        continue
                else:
                    mppt_details[inv_label].append({"mppt": mppt_num, "strings": string_count, "v": None, "exp": None})
                    continue
            
            series = df[col_name]
            # Get latest non-nan value
            latest_val = None
            series_valid = series.dropna()
            if not series_valid.empty:
                latest_val = float(series_valid.iloc[-1])

            # Expected current proportional logic
            nominal = 18.0 if string_count == 2 else 9.0
            expected_series = nominal * (fleet_2str_median / 18.0)
            expected_val = None
            expected_valid = expected_series.dropna()
            if not expected_valid.empty:
                expected_val = float(expected_valid.iloc[-1])

            mppt_details[inv_label].append({
                "mppt": mppt_num,
                "strings": string_count,
                "v": latest_val,
                "exp": expected_val
            })

            if series.isna().all(): continue

            # Analysis logic (Existing)
            series_filled = series.ffill()
            expected_current = expected_series

            # RULE: OPEN CIRCUIT (Critical)
            cond_openC = (series_filled < 0.1 * expected_current.ffill()) & (expected_current.ffill() > 1.0)
            open_streak_m = get_current_streak_minutes(cond_openC, df["Ora"])

            # RULE: SINGLE STRING LOSS
            ss_loss_m = 0
            if string_count == 2 and not inv_2str_median.isna().all():
                cond_ssLoss = (series_filled >= 0.4 * inv_2str_median.ffill()) & (series_filled <= 0.6 * inv_2str_median.ffill()) & (inv_2str_median.ffill() > 2.0)
                ss_loss_m = get_current_streak_minutes(cond_ssLoss, df["Ora"])

            # RULE: UNDERPERFORMANCE ABSOLUTE
            up_m = 0
            same_inv_peer_median = inv_2str_median if string_count == 2 else (inv_2str_median / 2.0)
            if not same_inv_peer_median.isna().all():
                cond_up = (series_filled < 0.75 * same_inv_peer_median.ffill()) & (series_filled > 0.0) & (same_inv_peer_median.ffill() > 1.0)
                up_m = get_current_streak_minutes(cond_up, df["Ora"])

            # RULE: CROSS-INVERTER DEVIATION
            domain_peer_cols = [f"Corrente DC MPPT {mppt_num} (INV {oi}) [A]" for oi, ocfg in MPPT_CONFIG.items() if oi[:3] == domain and ocfg[mppt_idx] == string_count and f"Corrente DC MPPT {mppt_num} (INV {oi}) [A]" in df.columns]
            if domain_peer_cols:
                domain_median = df[domain_peer_cols].median(axis=1)
                cond_cross = (series_filled < 0.65 * domain_median.ffill()) & (series_filled > 0.0) & (domain_median.ffill() > 2.0)
                cross_m = get_current_streak_minutes(cond_cross, df["Ora"])

            # Alarms
            if open_streak_m >= 15:
                faults.append({"Inverter": inv, "MPPT": mppt_num, "Strings": string_count, "Type": "OPEN CIRCUIT", "Severity": "CRITICAL", "Measured": f"{latest_val:.1f}" if latest_val is not None else "0.0", "Expected": f"{expected_val:.1f}" if expected_val is not None else "0.0", "Duration": int(open_streak_m), "Deviation": "<10%", "Action": "Check connection"})
                inv_summary[inv]["Critical"] += 1
            elif string_count == 2 and ss_loss_m >= 45:
                faults.append({"Inverter": inv, "MPPT": mppt_num, "Strings": string_count, "Type": "SINGLE STRING LOSS", "Severity": "WARNING", "Measured": f"{latest_val:.1f}" if latest_val is not None else "0.0", "Expected": f"{expected_val:.1f}" if expected_val is not None else "0.0", "Duration": int(ss_loss_m), "Deviation": "~50%", "Action": "Check fuse"})
                inv_summary[inv]["Warnings"] += 1
            elif cross_m >= 60:
                faults.append({"Inverter": inv, "MPPT": mppt_num, "Strings": string_count, "Type": "LOW CURRENT (vs PEERS)", "Severity": "WARNING", "Measured": f"{latest_val:.1f}" if latest_val is not None else "0.0", "Expected": f"{expected_val:.1f}" if expected_val is not None else "0.0", "Duration": int(cross_m), "Deviation": "<65%", "Action": "Check shading/strings"})
                inv_summary[inv]["Warnings"] += 1
            elif up_m >= 60:
                faults.append({"Inverter": inv, "MPPT": mppt_num, "Strings": string_count, "Type": "UNDERPERFORMANCE", "Severity": "INFO", "Measured": f"{latest_val:.1f}" if latest_val is not None else "0.0", "Expected": f"{expected_val:.1f}" if expected_val is not None else "0.0", "Duration": int(up_m), "Deviation": "<75%", "Action": "Monitor"})
                inv_summary[inv]["Info"] += 1

    # Formatting structured Markdown report
    md = [f"# Mazara PV Plant - DC MPPT Analysis Report ({date_str})\n", "## Section 1: Fault Table"]
    md.append("| Inverter | MPPT | Strings | Fault Type | Severity | Measured(A) | Expected(A) | Duration(m) | Deviation | Action |")
    md.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    
    severity_rank = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
    faults.sort(key=lambda x: (severity_rank.get(x["Severity"], 3), x["Inverter"], x["MPPT"]))
    
    for f in faults: md.append(f"| {f['Inverter']} | {f['MPPT']} | {f['Strings']} | {f['Type']} | {f['Severity']} | {f['Measured']} | {f['Expected']} | {f['Duration']} | {f['Deviation']} | {f['Action']} |")
    if not faults: md.append("| - | - | - | No faults detected | - | - | - | - | - | - |")
    
    md.extend(["\n## Section 2: Per-Inverter Summary", "| Inverter | Domain | Status | Critical | Warnings | Details (Info) |", "| --- | --- | --- | --- | --- | --- |"])
    for inv in sorted(inv_summary.keys()):
        s = inv_summary[inv]
        notes = ", ".join(s["Notes"]) if s["Notes"] else "Nominal"
        md.append(f"| {inv} | {s['Domain']} | {s['Status']} | {s['Critical']} | {s['Warnings']} | {s['Info']} 1-str checks OK, {notes} |")
        
    md.extend([
        "\n## Section 3: Fleet Overview",
        f"- Total Inverters Configured: {len(inv_summary)}",
        f"- Offline Inverters: {sum(1 for v in inv_summary.values() if v['Status'] == 'Offline')}",
        f"- Critical Alarms (Open Circuits): {sum(1 for f in faults if f['Severity'] == 'CRITICAL')}",
        f"- Warning Alarms: {sum(1 for f in faults if f['Severity'] == 'WARNING')}",
        "\n**Actions Recommended**: Verify all CRITICAL open circuits immediately."
    ])

    output_path = Path(output_md_path)
    output_path.write_text("\n".join(md), encoding="utf-8")
    
    return {
        "faults": faults,
        "mppt_details": mppt_details
    }

