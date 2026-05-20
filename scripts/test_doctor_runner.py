"""Test runner for db.doctor: inserts a fake log entry and runs scan/remediate."""
from datetime import datetime, timedelta
import json
import logging
import sys
from pathlib import Path

# Ensure package imports work when run as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.db_manager import get_logs_conn
from db import doctor

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test_doctor")

def insert_test_log(msg: str):
    conn = get_logs_conn()
    ts = (datetime.utcnow() - timedelta(minutes=30)).isoformat(timespec="seconds")
    conn.execute("INSERT INTO logs (timestamp, source, level, message) VALUES (?, ?, ?, ?)", (ts, "test", "ERROR", msg))
    conn.commit()
    log.info("Inserted test log")


def main():
    insert_test_log("TEST ERROR: database is locked during snapshot write")
    issues = doctor.scan_recent_logs(hours=1)
    print(f"Found {len(issues)} recent issues")
    for i in issues:
        print(i)
    results = doctor.remediate_issues(issues, ask_admin=False)
    print("Remediation results:")
    print(json.dumps(results, indent=2))

if __name__ == '__main__':
    main()
