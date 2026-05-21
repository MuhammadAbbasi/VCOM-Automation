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
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from db.db_manager import (
    get_logs_conn,
    get_data_conn,
    _repair_data_db,
    _repair_snapshot_db,
    _reset_data_conn,
    _reset_snapshot_conn,
    _get_snapshot_conn,
)

ROOT = Path(__file__).resolve().parent.parent
APPROVALS_PATH = ROOT / "telegram_approvals.json"
FEATURES_PATH = ROOT / "features_requests.json"
LOG = logging.getLogger(__name__)

KNOWN_SERVICE_SCRIPTS = {
    "watchdog": "processor_watchdog_final.py",
    "extraction": "vcom_monitor.py",
    "telegram": "telegram_bot.py",
    "dashboard": "dashboard" + os.sep + "app.py",
}


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


def scan_recent_logs(
    hours: float = 1.0,
    source_filter: str | None = None,
    message_filter: str | None = None,
) -> list[dict]:
    """Scan the sqlite logs DB and logs/ files for recent ERROR/CRITICAL entries.

    The optional filters help unit tests and targeted diagnostics.
    """
    cutoff = datetime.now() - timedelta(hours=hours)
    issues = []

    def _matches_filters(source: str | None, message: str | None) -> bool:
        if source_filter and source_filter.lower() not in (source or "").lower():
            return False
        if message_filter and message_filter.lower() not in (message or "").lower():
            return False
        return True

    try:
        conn = get_logs_conn()
        rows = conn.execute(
            "SELECT timestamp, source, level, message FROM logs ORDER BY timestamp ASC"
        ).fetchall()
        for ts, source, level, message in rows:
            try:
                row_ts = datetime.fromisoformat(ts)
            except Exception:
                continue
            if row_ts < cutoff:
                continue
            if (level and level.upper() in ("ERROR", "CRITICAL")) or "database is locked" in (message or "").lower():
                if _matches_filters(source, message):
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
        now = datetime.utcnow()
        search_paths = [ROOT.glob("*.log")]
        logs_dir = ROOT / "logs"
        if logs_dir.exists():
            search_paths.append(logs_dir.glob("*.log"))

        for path_iter in search_paths:
            for f in path_iter:
                try:
                    for line in f.read_text(encoding="utf-8", errors="ignore").splitlines():
                        if "database is locked" in line.lower() or "traceback" in line.lower() or "exception" in line.lower():
                            if _matches_filters(f.name, line):
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
    data_result = {"ok": False}
    snapshot_result = {"ok": False}

    try:
        conn = get_data_conn()
        try:
            conn.execute("PRAGMA wal_checkpoint(FULL)")
            conn.commit()
            data_result = {"ok": True, "action": "wal_checkpoint_data_db"}
        except Exception:
            pass
    except Exception as exc:
        LOG.warning(f"_auto_remedy_db_lock data checkpoint failed: {exc}")
    finally:
        _reset_data_conn()

    try:
        conn = _get_snapshot_conn()
        try:
            conn.execute("PRAGMA wal_checkpoint(FULL)")
            conn.commit()
            snapshot_result = {"ok": True, "action": "wal_checkpoint_snapshot_db"}
        except Exception:
            pass
    except Exception as exc:
        LOG.warning(f"_auto_remedy_db_lock snapshot checkpoint failed: {exc}")
    finally:
        _reset_snapshot_conn()

    if data_result["ok"] or snapshot_result["ok"]:
        actions = []
        if data_result["ok"]:
            actions.append(data_result["action"])
        if snapshot_result["ok"]:
            actions.append(snapshot_result["action"])
        return {"ok": True, "action": "+".join(actions)}

    return {"ok": False, "error": "checkpoint_failed", "details": {"data": data_result, "snapshot": snapshot_result}}


def _repair_corrupt_db() -> dict:
    try:
        _repair_data_db()
        return {"ok": True, "action": "repair_data_db"}
    except Exception as e:
        LOG.error(f"_repair_corrupt_db failed: {e}")
        return {"ok": False, "error": str(e)}


def _fix_file_permissions(target: Path | str | None = None) -> dict:
    try:
        if target is None:
            target = ROOT / "db" / "scada_data.db"
        target_path = Path(target)
        if not target_path.exists():
            return {"ok": False, "action": "fix_permissions", "error": "target_not_found", "target": str(target_path)}
        current_mode = target_path.stat().st_mode
        target_path.chmod(current_mode | 0o600)
        return {"ok": True, "action": "fix_permissions", "target": str(target_path)}
    except Exception as e:
        LOG.error(f"_fix_file_permissions failed: {e}")
        return {"ok": False, "error": str(e)}


def _guess_service_restart_target(issue: dict) -> str | None:
    source = (issue.get("source") or "").lower()
    message = (issue.get("message") or "").lower()
    for key, script in KNOWN_SERVICE_SCRIPTS.items():
        if key in source or key in message:
            return script
    return None


def _restart_service(script_name: str) -> dict:
    try:
        if not script_name:
            return {"ok": False, "error": "missing_service_name"}
        path = ROOT / script_name
        if not path.exists():
            return {"ok": False, "error": "service_script_not_found", "service": script_name}
        proc = subprocess.Popen([sys.executable, "-u", str(path)], cwd=str(ROOT))
        return {"ok": True, "action": "restart_service", "service": script_name, "pid": proc.pid}
    except Exception as e:
        LOG.error(f"_restart_service failed: {e}")
        return {"ok": False, "error": str(e), "service": script_name}


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

            if "snapshot" in msg or "analysis_snapshots" in msg or "scada_snapshots.db" in msg:
                res = _repair_snapshot_db()
            else:
                res = _repair_data_db()
            results.append({"issue": issue, "result": res})
            continue

        if "permission denied" in msg or "access is denied" in msg or "unable to open database file" in msg:
            if ask_admin:
                approved = request_admin_approval("fix_permissions", {"issue": issue}, timeout=admin_timeout)
                if not approved:
                    results.append({"issue": issue, "result": {"ok": False, "reason": "admin_denied"}})
                    continue
            res = _fix_file_permissions(ROOT / "db" / "scada_data.db")
            results.append({"issue": issue, "result": res})
            continue

        restart_keywords = ["crashed", "not running", "terminated", "exited", "failed to start", "service unavailable"]
        if any(keyword in msg for keyword in restart_keywords):
            candidate = _guess_service_restart_target(issue)
            if candidate:
                if ask_admin:
                    approved = request_admin_approval("restart_service", {"issue": issue, "service": candidate}, timeout=admin_timeout)
                    if not approved:
                        results.append({"issue": issue, "result": {"ok": False, "reason": "admin_denied"}})
                        continue
                res = _restart_service(candidate)
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
