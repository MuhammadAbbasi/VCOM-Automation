"""db/doctor.py

Lightweight "doctor" agent: scans recent logs, suggests remediations,
and performs safe automatic fixes. For actions that require elevated
permissions, it creates an approval request in `telegram_approvals.json`.

API:
 - scan_recent_logs(hours=1) -> list[dict]
 - remediate_issues(issues, ask_admin=True, admin_timeout=300)
 - request_admin_approval(action, details, timeout=300) -> bool
 - propose_feature(description, notify=True) -> req_id
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from db.db_manager import get_logs_conn, get_data_conn, _repair_data_db, _reset_data_conn

ROOT = Path(__file__).resolve().parent.parent
APPROVALS_PATH = ROOT / "telegram_approvals.json"
FEATURES_PATH = ROOT / "features_requests.json"
LOG = logging.getLogger(__name__)


def _load_approvals() -> dict:
    try:
        if not APPROVALS_PATH.exists():
            return {}
        return json.loads(APPROVALS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_approvals(data: dict) -> None:
    try:
        APPROVALS_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        LOG.error(f"Failed writing approvals file: {e}")


def scan_recent_logs(hours: int = 1) -> list[dict]:
    """Scan the sqlite logs DB and the logs/ folder for ERROR/CRITICAL
    entries in the last `hours` hours. Returns a list of issue dicts.
    """
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    cutoff_iso = cutoff.isoformat(timespec="seconds")
    issues = []
    try:
        conn = get_logs_conn()
        rows = conn.execute(
            "SELECT timestamp, source, level, message FROM logs WHERE timestamp >= ? ORDER BY timestamp ASC",
            (cutoff_iso,)
        ).fetchall()
        for ts, source, level, message in rows:
            if level.upper() in ("ERROR", "CRITICAL") or "database is locked" in (message or "").lower():
                issues.append({
                    "timestamp": ts,
                    "source": source,
                    "level": level,
                    "message": message,
                })
    except Exception as e:
        LOG.warning(f"scan_recent_logs: logs DB read failed: {e}")

    # Also scan plain log files for 'database is locked' patterns
    try:
        logs_dir = ROOT / "logs"
        now = datetime.utcnow()
        if logs_dir.exists():
            for f in logs_dir.glob("*.log"):
                try:
                    for line in f.read_text(encoding="utf-8", errors="ignore").splitlines():
                        if "database is locked" in line.lower() or "traceback" in line.lower() or "exception" in line.lower():
                            # crude timestamp if present
                            issues.append({
                                "timestamp": now.isoformat(timespec="seconds"),
                                "source": f.name,
                                "level": "ERROR",
                                "message": line.strip(),
                            })
                except Exception:
                    continue
    except Exception:
        pass

    return issues


def _auto_remedy_db_lock() -> dict:
    """Attempt a non-destructive remediation for `database is locked` errors.
    Returns a result dict.
    """
    try:
        # Try a WAL checkpoint to flush writers
        conn = get_data_conn()
        try:
            conn.execute("PRAGMA wal_checkpoint(FULL)")
            conn.commit()
        except Exception:
            # best-effort
            pass

        # Reset thread-local connection so other threads reopen a fresh conn
        _reset_data_conn()
        return {"ok": True, "action": "wal_checkpoint_and_reset"}
    except Exception as e:
        LOG.error(f"_auto_remedy_db_lock failed: {e}")
        return {"ok": False, "error": str(e)}


def _repair_corrupt_db() -> dict:
    try:
        _repair_data_db()
        return {"ok": True, "action": "repair_data_db"}
    except Exception as e:
        LOG.error(f"_repair_corrupt_db failed: {e}")
        return {"ok": False, "error": str(e)}


def request_admin_approval(action: str, details: Any, timeout: int = 300) -> bool:
    """Create an approval request and wait for admin to approve/deny.

    Writes an entry to `telegram_approvals.json` with a unique id. The
    `telegram_bot` process should listen for `/approve <id>` or `/deny <id>`
    and update the file. This function polls the file until timeout.
    """
    req_id = uuid.uuid4().hex[:10]
    approvals = _load_approvals()
    approvals[req_id] = {
        "action": action,
        "details": details,
        "status": "pending",
        "requested_at": datetime.utcnow().isoformat(timespec="seconds"),
    }
    _save_approvals(approvals)

    LOG.info(f"Admin approval requested: {req_id} {action}")

    # Poll for approval
    deadline = time.time() + timeout
    while time.time() < deadline:
        approvals = _load_approvals()
        state = approvals.get(req_id, {})
        if state.get("status") == "approved":
            return True
        if state.get("status") == "denied":
            return False
        time.sleep(2)

    # Timeout — mark denied
    approvals = _load_approvals()
    if req_id in approvals:
        approvals[req_id]["status"] = "timed_out"
        _save_approvals(approvals)
    return False


def remediate_issues(issues: list[dict], ask_admin: bool = True, admin_timeout: int = 300) -> list[dict]:
    """Attempt to remediate discovered issues. Returns list of remediation results.

    For some failure types (permission changes, file moves) this function will
    request admin approval if `ask_admin` is True.
    """
    results = []
    for issue in issues:
        msg = (issue.get("message") or "").lower()
        if "database is locked" in msg:
            res = _auto_remedy_db_lock()
            results.append({"issue": issue, "result": res})
            continue

        if "disk image is malformed" in msg or "file is not a database" in msg:
            # Requires repair (may rename file) — ask admin
            if ask_admin:
                approved = request_admin_approval("repair_db", {"issue": issue}, timeout=admin_timeout)
                if not approved:
                    results.append({"issue": issue, "result": {"ok": False, "reason": "admin_denied"}})
                    continue
            res = _repair_corrupt_db()
            results.append({"issue": issue, "result": res})
            continue

        # Generic fallback: log it and return no-action
        results.append({"issue": issue, "result": {"ok": False, "reason": "no_auto_action", "message": issue.get("message")}})
    return results


def propose_feature(description: str, notify: bool = True) -> str:
    """Record a requested feature and optionally notify admin via approvals file.

    Returns request id.
    """
    req_id = uuid.uuid4().hex[:10]
    entry = {
        "id": req_id,
        "description": description,
        "requested_at": datetime.utcnow().isoformat(timespec="seconds"),
        "status": "pending",
    }
    try:
        data = {}
        if FEATURES_PATH.exists():
            data = json.loads(FEATURES_PATH.read_text(encoding="utf-8"))
        data[req_id] = entry
        FEATURES_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
        LOG.info(f"Feature proposed: {req_id}")
    except Exception as e:
        LOG.error(f"propose_feature failed: {e}")

    # Also create an approval-style notification so admin sees it
    if notify:
        approvals = _load_approvals()
        approvals[req_id] = {"action": "feature_request", "details": description, "status": "pending", "requested_at": entry["requested_at"]}
        _save_approvals(approvals)
    return req_id
