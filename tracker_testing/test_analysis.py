import json
from pathlib import Path
from collections import Counter

def analyze_tracker_data(file_path):
    print(f"--- Analyzing Tracker Data: {file_path.name} ---")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    total_trackers = len(data)
    print(f"Total Trackers Received: {total_trackers} / 370")
    
    # Expected counts
    EXPECTED = {
        "NCU_01": 121,
        "NCU_02": 122,
        "NCU_03": 127
    }
    
    # Counters
    alarms = Counter()
    modes = Counter()
    ncus = Counter()
    ncu_received_ids = { "NCU_01": set(), "NCU_02": set(), "NCU_03": set() }
    angle_deviations = []
    
    for entry in data:
        ncu = entry.get('ncu', 'Unknown')
        tcu = entry.get('tcu', 'Unknown')
        
        alarms[entry.get('alarm', 'Unknown')] += 1
        modes[entry.get('mode', 'Unknown')] += 1
        ncus[ncu] += 1
        
        # Track which TCU IDs were received for which NCU
        if ncu in ncu_received_ids:
            try:
                # Extract number from "TCU 01"
                tcu_num = int(tcu.replace("TCU ", ""))
                ncu_received_ids[ncu].add(tcu_num)
            except Exception:
                pass
        
        # Check angle deviation
        target = entry.get('target_angle', 0)
        actual = entry.get('actual_angle', 0)
        deviation = abs(target - actual)
        if deviation > 0.5:
            angle_deviations.append({
                "tcu": tcu,
                "ncu": ncu,
                "target": target,
                "actual": actual,
                "deviation": deviation
            })

    print("\n--- Gaps / Missing Trackers ---")
    total_missing = 0
    for ncu_id, expected_count in EXPECTED.items():
        received_count = ncus.get(ncu_id, 0)
        missing_count = expected_count - received_count
        total_missing += missing_count
        print(f"  {ncu_id}: {received_count} received, {missing_count} MISSING (Expected {expected_count})")
        
        # Identify specific missing IDs (assuming sequential 1 to expected_count)
        if missing_count > 0:
            missing_ids = []
            for i in range(1, expected_count + 1):
                if i not in ncu_received_ids[ncu_id]:
                    missing_ids.append(f"TCU {i:02d}")
            
            # Print first 20 missing IDs
            preview = ", ".join(missing_ids[:15])
            if len(missing_ids) > 15:
                preview += f" ... (+{len(missing_ids)-15} more)"
            print(f"    Missing: {preview}")

    print(f"\nTOTAL MISSING: {total_missing}")

    print("\n--- Alarms Summary ---")
    for alarm, count in alarms.items():
        print(f"  {alarm}: {count}")

    print("\n--- Modes Summary ---")
    for mode, count in modes.items():
        print(f"  {mode}: {count}")

    if angle_deviations:
        print(f"\n--- Critical Angle Deviations (>0.5°) ---")
        print(f"  Total deviating trackers: {len(angle_deviations)}")
        for dev in angle_deviations[:10]: # Show first 10
            print(f"  {dev['tcu']}: Target {dev['target']} | Actual {dev['actual']} (Diff: {dev['deviation']:.2f})")
        if len(angle_deviations) > 10:
            print(f"  ... and {len(angle_deviations) - 10} more.")
    else:
        print("\n[OK] All trackers are within target angle tolerance (0.5 deg).")

    print("\n" + "="*40 + "\n")

if __name__ == "__main__":
    ROOT = Path(__file__).parent
    # Find the latest sync file
    sync_files = list(ROOT.glob("sync_*.json"))
    if not sync_files:
        print("No sync files found to analyze.")
    else:
        # Sort by name (which includes timestamp) to get the latest
        latest_file = sorted(sync_files)[-1]
        analyze_tracker_data(latest_file)
