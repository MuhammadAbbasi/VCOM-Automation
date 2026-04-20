import os
from pathlib import Path

root = Path(r"\\s01\get\2025.01 Mazara 01 A2A\03 - REPORT\Report\09 Testing\VCOM Automation")
print(f"Checking in: {root}")
print(f"Busy file exists: {(root / '.extraction_busy').exists()}")
print(f"Trigger file exists: {(root / '.trigger_extraction').exists()}")
print(f"Contents: {os.listdir(root)}")
