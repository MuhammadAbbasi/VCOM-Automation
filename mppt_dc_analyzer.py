import pandas as pd
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mazara Plant Configuration (1-string vs 2-string layout)
# ---------------------------------------------------------------------------
MPPT_CONFIG = {
    "TX1-01": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1], "TX1-02": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1],
    "TX1-03": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1], "TX1-04": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1],
    "TX1-05": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1], "TX1-06": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1],
    "TX1-07": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1], "TX1-08": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX1-09": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1], "TX1-10": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX1-11": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1], "TX1-12": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1],
    "TX2-01": [2, 2, 2, 2, 2, 2, 2, 1, 2, 2, 1, 1], "TX2-02": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1],
    "TX2-03": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1], "TX2-04": [2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1],
    "TX2-05": [2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1], "TX2-06": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX2-07": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1], "TX2-08": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX2-09": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1], "TX2-10": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1],
    "TX2-11": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1], "TX2-12": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX3-01": [2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2], "TX3-02": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX3-03": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1], "TX3-04": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX3-05": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1], "TX3-06": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX3-07": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1], "TX3-08": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX3-09": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1], "TX3-10": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
    "TX3-11": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1], "TX3-12": [2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1]
}

def get_streak_lengths(mask):
    """Return max contiguous True items in the mask series."""
    s = mask.astype(int)
    if s.empty: return 0
    streaks = s.groupby((s != s.shift()).cumsum()).sum()
    return streaks.max() if not streaks.empty else 0

def analyze_dc_current(dc_df: pd.DataFrame, output_md_path: Path, date_str: str):
    """Parses Corrente_DC dataset, calculates thresholds, and generates MD report."""
    if dc_df is None or len(dc_df) == 0:
        logger.warning("No DC Current DataFrame provided to analyzer.")
        return

    # Clean data & filter daylight (06:30 - 19:00)
    df = dc_df.copy()
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
        if not mppt_cols:
            inv_summary[inv]["Status"] = "Offline"
            inv_summary[inv]["Notes"].append("Appears fully offline/aggregate only" if inv_cols else "No channels data")
            continue
        if df[mppt_cols].isna().all(axis=None):
            inv_summary[inv]["Status"] = "Offline"
            inv_summary[inv]["Notes"].append("All MPPTs read missing/x all day")
            continue

        # Single Inverter Medians
        inv_2str_cols = [f"Corrente DC MPPT {i+1} (INV {inv}) [A]" for i, s in enumerate(mppt_cfg) if s == 2 and f"Corrente DC MPPT {i+1} (INV {inv}) [A]" in df.columns]
        inv_2str_median = df[inv_2str_cols].median(axis=1) if inv_2str_cols else pd.Series(np.nan, index=df.index)
        inv_1str_cols = [f"Corrente DC MPPT {i+1} (INV {inv}) [A]" for i, s in enumerate(mppt_cfg) if s == 1 and f"Corrente DC MPPT {i+1} (INV {inv}) [A]" in df.columns]
        inv_1str_median = df[inv_1str_cols].median(axis=1) if inv_1str_cols else pd.Series(np.nan, index=df.index)

        domain = inv[:3]

        for mppt_idx, string_count in enumerate(mppt_cfg):
            mppt_num = mppt_idx + 1
            col_name = f"Corrente DC MPPT {mppt_num} (INV {inv}) [A]"
            if col_name not in df.columns: continue
            
            series = df[col_name]
            if series.isna().all(): continue

            # Expected current proportional logic
            nominal = 18.0 if string_count == 2 else 9.0
            expected_current = nominal * (fleet_2str_median / 18.0)

            # RULE: OPEN CIRCUIT (Critical)
            cond_openC = (series < 0.1 * expected_current) & (expected_current > 1.0)
            openC_streak = get_streak_lengths(cond_openC)

            # RULE: SINGLE STRING LOSS (Warning, only 2-string configs)
            ss_loss_streak = 0
            if string_count == 2 and not inv_2str_median.isna().all():
                cond_ssLoss = (series >= 0.4 * inv_2str_median) & (series <= 0.6 * inv_2str_median) & (inv_2str_median > 2.0)
                ss_loss_streak = get_streak_lengths(cond_ssLoss)

            # RULE: UNDERPERFORMANCE ABSOLUTE (Warning)
            up_streak = 0
            same_inv_peer_median = inv_2str_median if string_count == 2 else inv_1str_median
            if not same_inv_peer_median.isna().all():
                cond_up = (series < 0.7 * same_inv_peer_median) & (series > 0.0) & (same_inv_peer_median > 2.0)
                up_streak = get_streak_lengths(cond_up)

            # RULE: CROSS-INVERTER DEVIATION (Warning)
            cross_streak = 0
            domain_peer_cols = [f"Corrente DC MPPT {mppt_num} (INV {oi}) [A]" for oi, ocfg in MPPT_CONFIG.items() if oi[:3] == domain and ocfg[mppt_idx] == string_count and f"Corrente DC MPPT {mppt_num} (INV {oi}) [A]" in df.columns]
            if domain_peer_cols:
                domain_median = df[domain_peer_cols].median(axis=1)
                cond_cross = (series < 0.65 * domain_median) & (series > 0.0) & (domain_median > 2.0)
                cross_streak = get_streak_lengths(cond_cross)

            # Add Normal Info
            if string_count == 1:
                inv_summary[inv]["Info"] += 1
                if "Confirmed normal 1-string MPPT currents observed" not in inv_summary[inv]["Notes"]:
                    inv_summary[inv]["Notes"].append("Confirmed normal 1-string MPPT currents observed")

            # Finalize priority tracking
            if openC_streak >= 3:
                faults.append({"Inverter": inv, "MPPT": mppt_num, "Strings": string_count, "Type": "OPEN CIRCUIT", "Severity": "CRITICAL", "Measured": f"{series.median():.1f}", "Expected": f"{expected_current.median():.1f}", "Duration": int(openC_streak), "Deviation": "<10%", "Action": "Check connection"})
                inv_summary[inv]["Critical"] += 1
            elif string_count == 2 and ss_loss_streak >= 15:
                faults.append({"Inverter": inv, "MPPT": mppt_num, "Strings": string_count, "Type": "SINGLE STRING LOSS", "Severity": "WARNING", "Measured": f"{series.median():.1f}", "Expected": f"{expected_current.median():.1f}", "Duration": int(ss_loss_streak), "Deviation": "~50%", "Action": "Check fuse"})
                inv_summary[inv]["Warnings"] += 1
            elif cross_streak >= 30:
                faults.append({"Inverter": inv, "MPPT": mppt_num, "Strings": string_count, "Type": "CROSS-INVERTER DEVIATION", "Severity": "WARNING", "Measured": f"{series.median():.1f}", "Expected": f"{expected_current.median():.1f}", "Duration": int(cross_streak), "Deviation": "<65%", "Action": "Check domain shading"})
                inv_summary[inv]["Warnings"] += 1
            elif up_streak >= 30:
                faults.append({"Inverter": inv, "MPPT": mppt_num, "Strings": string_count, "Type": "UNDERPERFORMANCE - ABSOLUTE", "Severity": "WARNING", "Measured": f"{series.median():.1f}", "Expected": f"{expected_current.median():.1f}", "Duration": int(up_streak), "Deviation": "<70%", "Action": "Check shading/soiling"})
                inv_summary[inv]["Warnings"] += 1

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
    logger.info(f"Wrote DC Analysis Report to {output_path}")

