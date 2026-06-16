from pathlib import Path

log_path = Path("monitoring.log")
if not log_path.exists():
    print("monitoring.log not found")
    exit(1)

with open(log_path, "r", encoding="utf-8", errors="replace") as f:
    lines = f.readlines()

print(f"Total lines in monitoring.log: {len(lines)}")
print("Last 20 lines of monitoring.log:")
for l in lines[-20:]:
    print(l.strip())

print("\nSearching for CRITICAL or FATAL or Exception in the last 2000 lines:")
found = 0
for i in range(max(0, len(lines)-2000), len(lines)):
    l = lines[i]
    if any(k in l for k in ["CRITICAL", "FATAL", "Exception", "Traceback", "sys.exit"]):
        print(f"Line {i}: {l.strip()}")
        found += 1
        if found > 15:
            print("... too many matches, truncating")
            break
