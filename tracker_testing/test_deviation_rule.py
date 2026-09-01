"""Self-check for receiver.find_deviating_trackers().

Run directly: python tracker_testing/test_deviation_rule.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from receiver import find_deviating_trackers

THRESHOLD = 6.0

# Real fault: both angles non-zero, deviation over threshold
assert find_deviating_trackers(
    [{"ncu": "NCU_01", "tcu": "TCU 05", "actual_angle": 12.0, "target_angle": 34.0}], THRESHOLD
) == [{"ncu": "NCU_01", "tcu": "TCU 05", "actual_angle": 12.0, "target_angle": 34.0, "_deviation": 22.0}]

# Zero-guard: actual_angle == 0 is a resting state, not a fault
assert find_deviating_trackers(
    [{"ncu": "NCU_01", "tcu": "TCU 06", "actual_angle": 0, "target_angle": 45.0}], THRESHOLD
) == []

# Zero-guard: target_angle == 0 is a resting state, not a fault
assert find_deviating_trackers(
    [{"ncu": "NCU_01", "tcu": "TCU 07", "actual_angle": 12.0, "target_angle": 0}], THRESHOLD
) == []

# Within threshold: not a fault
assert find_deviating_trackers(
    [{"ncu": "NCU_01", "tcu": "TCU 08", "actual_angle": 10.0, "target_angle": 12.0}], THRESHOLD
) == []

# Malformed OCR reading must not crash and must not suppress a real fault elsewhere in the batch
result = find_deviating_trackers(
    [
        {"ncu": "NCU_03", "tcu": "TCU 04", "actual_angle": None, "target_angle": "n/a"},
        {"ncu": "NCU_02", "tcu": "TCU 45", "actual_angle": 12.0, "target_angle": 34.0},
    ],
    THRESHOLD,
)
assert len(result) == 1 and result[0]["tcu"] == "TCU 45"

print("All tracker deviation rule checks passed.")
