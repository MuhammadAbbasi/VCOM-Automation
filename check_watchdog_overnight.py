from pathlib import Path

log_path = Path("logs/watchdog.log")
with open(log_path, "r", encoding="utf-8", errors="replace") as f:
    lines = f.readlines()

print("Searching for 2026-05-30 02: in logs/watchdog.log:")
for l in lines:
    if "2026-05-30 02:" in l or "2026-05-30 03:" in l or "2026-05-30 01:" in l:
        print(l.strip())
