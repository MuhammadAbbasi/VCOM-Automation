
import os
import json
import re
import shutil
import time
import logging
import subprocess
import sys
import requests
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("dashboard_doctor.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DashboardDoctor")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "db" / "scada_data.db"
LOGS_DB_PATH = ROOT / "db" / "scada_logs.db"
DB_BACKUP_DIR = ROOT / "db" / "backups"
DB_BACKUP_STATE = ROOT / "db" / "last_db_backup.json"
LINK_STATUS_PATH = ROOT / "db" / "link_status.json"
EXTRACTION_BUSY_PATH = ROOT / ".extraction_busy"
SETTINGS_PATH = ROOT / "user_settings.json"
CONFIG_PATH = ROOT / "config.json"

TOTAL_TRACKERS_EXPECTED = 374
FLATLINE_MIN_INTERVALS = 4         # consecutive same non-zero readings
INTERVAL_HOURS = 15 / 60           # 15-minute data collection intervals

# Mazara del Vallo approximate solar window (local time, April)
# Sunrise ~06:10, Sunset ~20:00 — use conservative bounds
DAYLIGHT_START_HOUR = 6
DAYLIGHT_END_HOUR = 20


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_settings() -> dict:
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}


def get_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}


def is_daylight() -> bool:
    h = datetime.now().hour
    return DAYLIGHT_START_HOUR <= h < DAYLIGHT_END_HOUR


def send_telegram(message: str):
    settings = get_settings()
    tg = settings.get("telegram", {})
    if not tg.get("enabled"):
        return
    token = tg.get("bot_token")
    chat_id = tg.get("personal_id") or tg.get("chat_id")
    if not token or not chat_id:
        logger.error("Telegram token or chat_id missing in settings.")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if not resp.ok:
            logger.error(f"Telegram error: {resp.text}")
    except Exception as e:
        logger.error(f"Failed to send Telegram: {e}")


def _open_db() -> sqlite3.Connection:
    """Open a fresh connection each time to avoid stale schema cache."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _load_backup_state() -> dict:
    if not DB_BACKUP_STATE.exists():
        return {}
    try:
        return json.loads(DB_BACKUP_STATE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_backup_state(state: dict) -> None:
    DB_BACKUP_STATE.parent.mkdir(parents=True, exist_ok=True)
    DB_BACKUP_STATE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def backup_database() -> Path | None:
    DB_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    backup_name = f"scada_data_backup_{now.strftime('%Y%m%d_%H%M%S')}.db"
    backup_path = DB_BACKUP_DIR / backup_name

    try:
        with sqlite3.connect(str(DB_PATH), timeout=30) as src, sqlite3.connect(str(backup_path), timeout=30) as dst:
            src.backup(dst)

        state = {
            "date": now.strftime("%Y-%m-%d"),
            "timestamp": now.isoformat(),
            "backup_file": backup_name,
        }
        _save_backup_state(state)
        logger.info(f"[DOCTOR] Database backup created: {backup_path}")
        return backup_path
    except Exception as e:
        logger.error(f"[DOCTOR] Database backup failed: {e}")
        try:
            if backup_path.exists():
                backup_path.unlink()
        except Exception:
            pass
        return None


def maybe_backup_database() -> None:
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    state = _load_backup_state()
    if state.get("date") == today:
        return

    backup_path = backup_database()
    if backup_path is not None:
        send_telegram(
            f"💾 <b>Daily Database Backup</b>\n"
            f"File: <code>{backup_path.name}</code>\n"
            f"Date: <code>{today}</code>\n"
            f"Status: <b>SUCCESS</b>"
        )
    else:
        send_telegram(
            f"💾 <b>Daily Database Backup</b>\n"
            f"Date: <code>{today}</code>\n"
            f"Status: <b>FAILED</b>\n"
            f"Please check dashboard_doctor.log for details."
        )


def check_trackers_granular(conn: sqlite3.Connection) -> dict:
    """Analyze tracker_status table for missing units per NCU and 'No State' modes."""
    # Expected counts based on site registry
    expected = {
        "NCU 01": 121,
        "NCU 02": 122,
        "NCU 03": 127
    }
    
    report = {"ncu_stats": {}, "missing_details": [], "no_state": []}
    
    try:
        rows = conn.execute("SELECT ncu_id, tcu_id, mode FROM tracker_status").fetchall()
        
        # Mapping results
        found = {"NCU 01": set(), "NCU 02": set(), "NCU 03": set()}
        for r in rows:
            ncu_raw = r["ncu_id"] or ""
            ncu = ncu_raw.replace("_", " ").upper()
            if "NCU 01" in ncu:
                ncu_key = "NCU 01"
            elif "NCU 02" in ncu:
                ncu_key = "NCU 02"
            elif "NCU 03" in ncu:
                ncu_key = "NCU 03"
            else:
                continue
            
            tcu = r["tcu_id"]
            if tcu:
                match = re.search(r"(\d+)", str(tcu))
                if match:
                    try:
                        found[ncu_key].add(int(match.group(1)))
                    except ValueError:
                        pass
            
            if r["mode"] == "No State":
                report["no_state"].append(f"{ncu_key}-TCU{tcu}")
                
        for ncu, exp_count in expected.items():
            present = found.get(ncu, set())
            missing = []
            for i in range(1, exp_count + 1):
                if i not in present:
                    missing.append(i)
            
            report["ncu_stats"][ncu] = {
                "present": len(present),
                "expected": exp_count,
                "missing_count": len(missing),
                "missing_samples": missing[:5]
            }
    except Exception as e:
        logger.error(f"check_trackers_granular error: {e}")
        
    return report


# ---------------------------------------------------------------------------
# 1. DB Health
# ---------------------------------------------------------------------------

def check_db_health() -> dict:
    results = {"status": "HEALTHY", "issues": [], "info": []}
    daylight = is_daylight()

    try:
        conn = _open_db()

        # --- potenza_ac freshness ---
        if not _table_exists(conn, "potenza_ac"):
            msg = "potenza_ac table missing (no data extracted yet today)"
            if daylight:
                results["issues"].append(f"❌ {msg}")
                results["status"] = "CRITICAL"
            else:
                results["info"].append(f"ℹ️ {msg} — expected at night")
        else:
            row = conn.execute(
                "SELECT _date, Ora FROM potenza_ac ORDER BY _date DESC, Ora DESC LIMIT 1"
            ).fetchone()
            if row:
                date_str, ora = row["_date"], row["Ora"]
                try:
                    h = int(ora)
                    m = int(round((ora - h) * 100))
                    last_dt = datetime.strptime(f"{date_str} {h:02d}:{m:02d}", "%Y-%m-%d %H:%M")
                    age_min = (datetime.now() - last_dt).total_seconds() / 60
                    if daylight and age_min > 20:
                        results["issues"].append(
                            f"❌ Data Stale: Last Potenza AC was {date_str} {h:02d}:{m:02d} ({int(age_min)} min ago)"
                        )
                        results["status"] = "WARNING"
                    else:
                        results["info"].append(
                            f"✅ Potenza AC: last entry {date_str} {h:02d}:{m:02d}"
                        )
                except Exception as e:
                    results["issues"].append(f"⚠️ Could not parse Potenza AC timestamp: {e}")
            else:
                if daylight:
                    results["issues"].append("❌ potenza_ac is empty during daylight hours")
                    results["status"] = "CRITICAL"
                else:
                    results["info"].append("ℹ️ potenza_ac empty — expected at night")

        # --- Granular Tracker Health ---
        if _table_exists(conn, "tracker_status"):
            tracker_report = check_trackers_granular(conn)
            results["tracker_report"] = tracker_report
            
            total_found = sum(s["present"] for s in tracker_report["ncu_stats"].values())
            total_exp = sum(s["expected"] for s in tracker_report["ncu_stats"].values())
            
            latest_row = conn.execute(
                "SELECT last_update FROM tracker_status WHERE last_update IS NOT NULL ORDER BY last_update DESC LIMIT 1"
            ).fetchone()

            if latest_row and latest_row["last_update"]:
                last_ts = datetime.fromisoformat(latest_row["last_update"])
                age_min = (datetime.now() - last_ts).total_seconds() / 60
                
                if daylight:
                    if age_min > 60:
                        results["issues"].append(
                            f"❌ Trackers Stale: Last update {int(age_min)} min ago ({last_ts.strftime('%H:%M')})"
                        )
                        results["status"] = "WARNING"
                    else:
                        results["info"].append(
                            f"✅ Trackers: {total_found}/{total_exp} online, last sync {int(age_min)}m ago"
                        )
                else:
                    results["info"].append(f"ℹ️ Trackers: {total_found}/{total_exp} in DB (Night Mode)")

                # NCU Specific Issues
                for ncu, stats in tracker_report["ncu_stats"].items():
                    if stats["missing_count"] > 0:
                        results["issues"].append(
                            f"⚠️ {ncu}: {stats['missing_count']} trackers missing (e.g. TCU {', '.join(map(str, stats['missing_samples']))})"
                        )
                        if results["status"] == "HEALTHY": results["status"] = "WARNING"
                
                # No State Issues
                if tracker_report["no_state"]:
                    results["issues"].append(
                        f"🟣 No State: {len(tracker_report['no_state'])} trackers in undefined state"
                    )
            else:
                results["issues"].append("❌ Trackers: no last_update timestamps found")
                results["status"] = "CRITICAL"
        else:
            results["issues"].append("❌ tracker_status table missing")
            results["status"] = "CRITICAL"

        conn.close()
    except Exception as e:
        results["status"] = "CRITICAL"
        results["issues"].append(f"❌ DB Error: {e}")
        logger.exception("DB health check failed")

    return results


# ---------------------------------------------------------------------------
# 2. Connection Health
# ---------------------------------------------------------------------------

def check_connections() -> dict:
    results = {"status": "HEALTHY", "issues": [], "info": []}

    # link_status.json heartbeat
    if LINK_STATUS_PATH.exists():
        try:
            with open(LINK_STATUS_PATH, encoding="utf-8") as f:
                link = json.load(f)

            link_status = link.get("status", "unknown")
            if link_status != "online":
                results["issues"].append(f"❌ Link Offline: status='{link_status}'")
                results["status"] = "CRITICAL"

            hb = link.get("last_heartbeat") or link.get("timestamp")
            if hb:
                ts = datetime.fromisoformat(hb)
                age_s = (datetime.now() - ts).total_seconds()
                if age_s > 300:
                    results["issues"].append(
                        f"⚠️ Heartbeat Stale: last seen {int(age_s // 60)} min ago"
                    )
                    if results["status"] == "HEALTHY":
                        results["status"] = "WARNING"
                else:
                    results["info"].append(f"✅ MQTT heartbeat: {int(age_s)}s ago")
        except Exception as e:
            results["issues"].append(f"⚠️ link_status.json parse error: {e}")
    else:
        results["issues"].append("❌ link_status.json not found")
        results["status"] = "CRITICAL"

    # Dashboard API
    try:
        cfg = get_config()
        user = cfg.get("DASHBOARD_USER", "admin")
        pw = cfg.get("DASHBOARD_PASS", "mazara2025!")
        resp = requests.get("http://localhost:8080/api/status", auth=(user, pw), timeout=5)
        if resp.status_code == 401:
            results["issues"].append("❌ Dashboard API: authentication failed (check DASHBOARD_USER/PASS in config.json)")
            results["status"] = "CRITICAL"
        elif resp.ok:
            results["info"].append("✅ Dashboard API: online and authenticated")
        else:
            results["issues"].append(f"❌ Dashboard API: returned {resp.status_code}")
            results["status"] = "CRITICAL"
    except requests.exceptions.ConnectionError:
        results["issues"].append("❌ Dashboard Unreachable: port 8080 not responding")
        results["status"] = "CRITICAL"
    except Exception as e:
        results["issues"].append(f"❌ Dashboard check error: {e}")
        results["status"] = "CRITICAL"

    return results


# ---------------------------------------------------------------------------
# 3. Process Health (broker + receiver)
# ---------------------------------------------------------------------------

def _get_running_cmdlines() -> list:
    """Return list of command-line strings for all running python processes."""
    try:
        import psutil
        lines = []
        for proc in psutil.process_iter(["cmdline"]):
            try:
                cmdline = proc.info.get("cmdline") or []
                if cmdline:
                    lines.append(" ".join(cmdline))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return lines
    except ImportError:
        pass
    # Fallback: tasklist (image name only — less precise)
    try:
        out = subprocess.check_output(
            "tasklist", shell=True, text=True, encoding="utf-8", errors="replace", timeout=10
        )
        return [out]
    except Exception:
        return []


def check_process_health() -> dict:
    results = {"status": "HEALTHY", "issues": [], "info": [], "dead": []}
    targets = {
        "BROKER": "broker.py",
        "RECEIVER": "receiver.py",
        "WATCHDOG": "processor_watchdog_final.py",
        "EXTRACTION": "vcom_monitor.py",
    }
    cmdlines = _get_running_cmdlines()
    if not cmdlines:
        results["info"].append("ℹ️ Process check unavailable (no cmdline data)")
        return results

    combined = "\n".join(cmdlines)
    for name, script in targets.items():
        if script in combined:
            results["info"].append(f"✅ {name}: running ({script})")
        else:
            results["issues"].append(f"❌ {name}: not detected ({script})")
            results["dead"].append(name)
            if results["status"] == "HEALTHY":
                results["status"] = "WARNING"

    return results


def check_logs_health() -> dict:
    results = {"status": "HEALTHY", "issues": [], "info": []}
    try:
        from db.doctor import scan_recent_logs, remediate_issues

        issues = scan_recent_logs(hours=1)
        if issues:
            results["status"] = "WARNING"
            for issue in issues:
                source = issue.get("source") or "logs"
                level = issue.get("level") or "ERROR"
                message = issue.get("message") or ""
                results["issues"].append(
                    f"❌ Log {source} [{level}]: {message}"
                )

            remediate_results = remediate_issues(issues, ask_admin=False)
            for item in remediate_results:
                result = item.get("result", {})
                if result.get("ok"):
                    action = result.get("action") or result.get("service")
                    if action and action != "deduped_db_lock_remedy":
                        results["info"].append(f"Auto-healed: {action}")
                else:
                    reason = result.get("reason") or ""
                    err = result.get("error") or ""
                    # no_auto_action means no handler exists — not a new problem, skip logging
                    if reason == "no_auto_action":
                        continue
                    msg = err or reason
                    if msg:
                        results["issues"].append(f"Auto-heal failed: {msg}")
    except Exception as e:
        logger.warning(f"[LOG] check_logs_health failed: {e}")
    return results


# ---------------------------------------------------------------------------
# 4. Flatline Detection
# ---------------------------------------------------------------------------

def check_flatline() -> list:
    """Return list of inverter names with flatline readings (daytime only)."""
    if not is_daylight():
        return []
    flatlines = []
    try:
        conn = _open_db()
        if not _table_exists(conn, "potenza_ac"):
            conn.close()
            return []

        today = datetime.now().strftime("%Y-%m-%d")
        cols_info = conn.execute("PRAGMA table_info(potenza_ac)").fetchall()
        inv_cols = [r["name"] for r in cols_info if r["name"].startswith("Potenza AC (INV")]

        if not inv_cols:
            conn.close()
            return []

        cols_sql = ", ".join(f'"{c}"' for c in inv_cols)
        rows = conn.execute(
            f'SELECT Ora, {cols_sql} FROM potenza_ac WHERE _date=? ORDER BY Ora ASC', (today,)
        ).fetchall()
        conn.close()

        if len(rows) < FLATLINE_MIN_INTERVALS:
            return []

        for col_idx, col_name in enumerate(inv_cols):
            run = 1
            for i in range(1, len(rows)):
                prev = rows[i - 1][col_idx + 1]
                curr = rows[i][col_idx + 1]
                if curr is not None and prev is not None and curr > 0 and curr == prev:
                    run += 1
                    if run >= FLATLINE_MIN_INTERVALS:
                        inv_name = col_name.replace("Potenza AC (INV ", "").replace(") [W]", "")
                        if inv_name not in flatlines:
                            flatlines.append(inv_name)
                else:
                    run = 1
    except Exception as e:
        logger.error(f"Flatline check error: {e}")
    return flatlines


# ---------------------------------------------------------------------------
# 5. UI / Extraction State
# ---------------------------------------------------------------------------

def check_ui_state() -> dict:
    results = {"status": "HEALTHY", "issues": [], "info": []}

    if EXTRACTION_BUSY_PATH.exists():
        age_s = (
            datetime.now() - datetime.fromtimestamp(EXTRACTION_BUSY_PATH.stat().st_mtime)
        ).total_seconds()
        if age_s > 1800:
            results["issues"].append(
                f"⚠️ .extraction_busy stuck for {int(age_s // 60)} min — possible hang"
            )
            results["status"] = "WARNING"
        else:
            results["info"].append(f"ℹ️ Extraction in progress ({int(age_s)}s)")
    else:
        results["info"].append("✅ No extraction in progress")

    return results


# ---------------------------------------------------------------------------
# 6. Self-Healing
# ---------------------------------------------------------------------------

def attempt_self_heal(dead_processes: list, tracker_stale: bool) -> list:
    healed = []
    cfg = get_config()
    user = cfg.get("DASHBOARD_USER", "admin")
    pw = cfg.get("DASHBOARD_PASS", "mazara2025!")

    # Trigger forensic rescan
    try:
        resp = requests.post(
            "http://localhost:8080/api/forensic/rescan", auth=(user, pw), timeout=15
        )
        if resp.ok:
            healed.append("Forensic re-scan triggered via Dashboard API")
            logger.info("[HEAL] Forensic rescan triggered.")
        else:
            logger.warning(f"[HEAL] Rescan API returned {resp.status_code}")
    except Exception as e:
        logger.debug(f"[HEAL] Rescan not available: {e}")

    # Trigger manual extraction
    try:
        resp = requests.post(
            "http://localhost:8080/api/extraction/trigger", auth=(user, pw), timeout=10
        )
        if resp.ok:
            healed.append("Manual extraction triggered via API")
            logger.info("[HEAL] Extraction triggered.")
    except Exception as e:
        logger.debug(f"[HEAL] Extraction trigger skipped: {e}")

    # Attempt to restart dead services by killing their processes via psutil
    # run_monitor.py will auto-respawn them
    script_map = {
        "BROKER": "broker.py",
        "RECEIVER": "receiver.py",
        "WATCHDOG": "processor_watchdog_final.py",
        "EXTRACTION": "vcom_monitor.py",
    }
    for svc_name in dead_processes:
        script = script_map.get(svc_name)
        if not script:
            continue
        try:
            import psutil
            killed = 0
            for proc in psutil.process_iter(["cmdline"]):
                try:
                    cmdline = " ".join(proc.info.get("cmdline") or [])
                    if script in cmdline:
                        proc.terminate()
                        killed += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            if killed:
                healed.append(f"{svc_name} ({script}) terminated ({killed} proc) — orchestrator will restart")
                logger.info(f"[HEAL] Terminated {killed} instance(s) of {svc_name}.")
        except Exception as e:
            logger.warning(f"[HEAL] Could not terminate {svc_name}: {e}")

    # For stale trackers during daylight: unstuck the busy flag if it's stuck
    if tracker_stale and EXTRACTION_BUSY_PATH.exists():
        age_s = (
            datetime.now() - datetime.fromtimestamp(EXTRACTION_BUSY_PATH.stat().st_mtime)
        ).total_seconds()
        if age_s > 1800:
            try:
                EXTRACTION_BUSY_PATH.unlink()
                healed.append("Removed stale .extraction_busy flag")
                logger.info("[HEAL] Removed stuck .extraction_busy flag.")
            except Exception as e:
                logger.warning(f"[HEAL] Could not remove busy flag: {e}")

    return healed


# ---------------------------------------------------------------------------
# 7. Daily Analytics
# ---------------------------------------------------------------------------

def build_daily_analytics() -> dict:
    analytics = {
        "energy_mwh": None,
        "peak_mw": None,
        "peak_time": None,
        "alarm_count_24h": 0,
        "energy_date": None,
    }
    try:
        conn = _open_db()
        today = datetime.now().strftime("%Y-%m-%d")
        data_date = today

        if _table_exists(conn, "potenza_ac"):
            cols_info = conn.execute("PRAGMA table_info(potenza_ac)").fetchall()
            inv_cols = [r["name"] for r in cols_info if r["name"].startswith("Potenza AC (INV")]

            if inv_cols:
                cols_sql = ", ".join(f'"{c}"' for c in inv_cols)
                rows = conn.execute(
                    f'SELECT Ora, {cols_sql} FROM potenza_ac WHERE _date=? ORDER BY Ora ASC',
                    (data_date,)
                ).fetchall()

                if not rows:
                    latest_date_row = conn.execute(
                        "SELECT DISTINCT _date FROM potenza_ac ORDER BY _date DESC LIMIT 1"
                    ).fetchone()
                    if latest_date_row and latest_date_row["_date"]:
                        data_date = latest_date_row["_date"]
                        rows = conn.execute(
                            f'SELECT Ora, {cols_sql} FROM potenza_ac WHERE _date=? ORDER BY Ora ASC',
                            (data_date,)
                        ).fetchall()

                total_wh = 0.0
                peak_w = 0.0
                peak_ora = None

                for row in rows:
                    ora = row["Ora"]
                    vals = [row[c] for c in inv_cols if row[c] is not None and row[c] > 0]
                    plant_w = sum(vals)
                    total_wh += plant_w * INTERVAL_HOURS
                    if plant_w > peak_w:
                        peak_w = plant_w
                        peak_ora = ora

                if total_wh > 0:
                    analytics["energy_mwh"] = round(total_wh / 1_000_000, 3)
                if peak_w > 0:
                    analytics["peak_mw"] = round(peak_w / 1_000_000, 3)
                if peak_ora is not None:
                    h = int(peak_ora)
                    m = int(round((peak_ora - h) * 100))
                    analytics["peak_time"] = f"{h:02d}:{m:02d}"
                analytics["energy_date"] = data_date

        conn.close()
    except Exception as e:
        logger.error(f"Daily analytics DB error: {e}")

    # Alarm count from logs DB
    try:
        conn2 = sqlite3.connect(str(LOGS_DB_PATH), timeout=10)
        cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
        row = conn2.execute(
            "SELECT COUNT(*) FROM logs WHERE source='watchdog' AND level IN ('WARNING','ERROR','CRITICAL') AND timestamp > ?",
            (cutoff,)
        ).fetchone()
        analytics["alarm_count_24h"] = row[0] if row else 0
        conn2.close()
    except Exception as e:
        logger.error(f"Alarm count error: {e}")

    return analytics


# ---------------------------------------------------------------------------
# 7.5 Daily Deep AI Audit
# ---------------------------------------------------------------------------

def run_daily_ai_audit():
    logger.info("[DOCTOR] Starting Daily Deep AI Audit...")
    try:
        sys.path.append(str(ROOT))
        import llm_agent
    except Exception as e:
        logger.error(f"Failed to import llm_agent: {e}")
        send_telegram(f"🚨 <b>Daily Deep AI Audit Error</b>\nFailed to import llm_agent: {e}")
        return

    questions = [
        "1. Grid Limit & Curtailment: Analyze today's potenza_attiva table. What was the nominal grid limit today? Did the plant experience any curtailment or exceed the grid limit? Retrieve the latest nominal grid limit value.",
        "2. Inverter Production: Check today's potenza_ac table. Are there any inverters with significantly lower production than others, or any suspicious flatline/zero-production intervals during daylight?",
        "3. Tracker Anomalies: Review the tracker_status table. Are there missing trackers or trackers in 'No State'? Which NCUs are affected?",
        "4. Process & Log Health: Search the logs using search_logs for 'error' or 'warning' from today. Are there any critical system software anomalies?"
    ]
    
    report_sections = []
    for q in questions:
        logger.info(f"[DOCTOR] Asking AI: {q}")
        try:
            ans = llm_agent.ask_llm(q, {}, user_id="doctor_daily")
            section_title = q.split(":")[0].strip()
            report_sections.append(f"❓ <b>{section_title}</b>\n{ans}")
        except Exception as e:
            logger.error(f"AI Audit question failed: {e}")
            section_title = q.split(":")[0].strip()
            report_sections.append(f"❓ <b>{section_title}</b>\n⚠️ AI Error: {e}")
            
    header = f"🤖 <b>AI DAILY DEEP AUDIT REPORT</b>\n⏰ <code>{datetime.now().strftime('%Y-%m-%d %H:%M')}</code>\n\n"
    
    current_msg = ""
    for section in report_sections:
        if len(current_msg) + len(section) + 50 > 3800:
            send_telegram(header + current_msg)
            current_msg = section
        else:
            if current_msg:
                current_msg += "\n\n" + section
            else:
                current_msg = section
    if current_msg:
        send_telegram(header + current_msg)


# ---------------------------------------------------------------------------
# 8. Main Doctor Run
# ---------------------------------------------------------------------------

def run_doctor():
    logger.info("[DOCTOR] Starting health check...")
    daylight = is_daylight()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    db_report = check_db_health()
    conn_report = check_connections()
    proc_report = check_process_health()
    log_report = check_logs_health()
    ui_report = check_ui_state()
    flatlines = check_flatline()
    analytics = build_daily_analytics()

    all_statuses = [
        db_report["status"], conn_report["status"],
        proc_report["status"], log_report["status"], ui_report["status"]
    ]
    if "CRITICAL" in all_statuses:
        overall_status = "CRITICAL"
    elif "WARNING" in all_statuses:
        overall_status = "WARNING"
    else:
        overall_status = "HEALTHY"

    all_issues = (
        db_report["issues"] + conn_report["issues"]
        + proc_report["issues"] + log_report["issues"] + ui_report["issues"]
    )
    for issue in all_issues:
        logger.warning(f"[ISSUE] {issue}")

    all_info = db_report["info"] + conn_report["info"] + proc_report["info"] + log_report["info"] + ui_report["info"]

    # Self-heal if warranted (only during daylight for tracker issues)
    heal_actions = []
    tracker_stale = any("Stale" in i and "Tracker" in i for i in db_report["issues"])
    need_heal = overall_status in ("CRITICAL", "WARNING") and (
        proc_report["dead"] or tracker_stale
    )
    if need_heal:
        heal_actions = attempt_self_heal(proc_report["dead"], tracker_stale)

    # ---- Build Telegram message ----
    status_icon = {"HEALTHY": "✅", "WARNING": "⚠️", "CRITICAL": "🚨"}.get(overall_status, "❓")
    mode_tag = "☀️ DAYLIGHT" if daylight else "🌙 NIGHT MODE"

    lines = [
        f"🏥 <b>DASHBOARD DOCTOR — HOURLY SNAPSHOT</b>",
        f"⏰ <code>{now_str}</code> | {mode_tag}",
        f"",
        f"{status_icon} <b>Status: {overall_status}</b>",
        "",
        "📊 <b>Daily Analytics:</b>",
    ]

    energy_date = analytics.get("energy_date")
    if analytics["energy_mwh"] is not None:
        if energy_date and energy_date != datetime.now().strftime("%Y-%m-%d"):
            lines.append(
                f"  ⚡ Energy on <code>{energy_date}</code>: <code>{analytics['energy_mwh']} MWh</code>"
            )
        else:
            lines.append(f"  ⚡ Energy today: <code>{analytics['energy_mwh']} MWh</code>")
    else:
        lines.append("  ⚡ Energy today: <code>N/A</code>")

    if analytics["peak_mw"] is not None:
        t = f" at <code>{analytics['peak_time']}</code>" if analytics["peak_time"] else ""
        lines.append(f"  🔝 Peak Power: <code>{analytics['peak_mw']} MW</code>{t}")
    else:
        lines.append("  🔝 Peak Power: <code>N/A</code>")

    lines.append(f"  🔔 Watchdog Alerts (24h): <code>{analytics['alarm_count_24h']}</code>")

    if flatlines:
        lines += [
            "",
            f"📉 <b>Flatline Detected ({len(flatlines)} inverter(s)):</b>",
        ] + [f"  • <code>{inv}</code>" for inv in flatlines[:10]]
        if len(flatlines) > 10:
            lines.append(f"  … and {len(flatlines) - 10} more")

    if all_issues:
        lines += ["", "<b>Issues Detected:</b>"]
        for issue in all_issues:
            # Escape basic HTML chars in issues just in case
            issue_safe = issue.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            lines.append(f"  {issue_safe}")
    else:
        lines += ["", "✅ All systems nominal."]

    # Show key info items
    all_info = db_report["info"] + conn_report["info"] + proc_report["info"] + ui_report["info"]
    if all_info:
        lines += ["", "<b>System Info:</b>"]
        for info in all_info[:6]:  # Cap to avoid huge messages
            info_safe = info.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            lines.append(f"  {info_safe}")

    if heal_actions:
        lines += ["", "🔧 <b>Auto-Heal Actions:</b>"]
        for action in heal_actions:
            action_safe = action.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            lines.append(f"  • {action_safe}")

    report = "\n".join(lines)
    if len(report) > 4000:
        report = report[:3990] + "\n…<i>(truncated)</i>"

    logger.info(f"[DOCTOR] Report generated. Status: {overall_status}")
    send_telegram(report)

    # Check if we should trigger the Daily Deep AI Audit (run at 20:00 every day)
    try:
        now = datetime.now()
        audit_state_path = ROOT / "db" / "last_ai_audit.json"
        last_date = ""
        if audit_state_path.exists():
            try:
                with open(audit_state_path, encoding="utf-8") as f:
                    last_date = json.load(f).get("date", "")
            except:
                pass
        
        today_str = now.strftime("%Y-%m-%d")
        if now.hour >= 20 and last_date != today_str:
            run_daily_ai_audit()
            with open(audit_state_path, "w", encoding="utf-8") as f:
                json.dump({"date": today_str}, f)
    except Exception as e:
        logger.error(f"Daily AI Audit trigger failed: {e}")

    return overall_status


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("[DOCTOR] Dashboard Doctor v3 started.")
    while True:
        try:
            maybe_backup_database()
            run_doctor()
        except Exception as e:
            logger.error(f"[DOCTOR] Crashed: {e}", exc_info=True)
            try:
                send_telegram(f"🚨 <b>Dashboard Doctor CRASHED</b>\nError: <code>{e}</code>")
            except Exception:
                pass

        logger.info("[DOCTOR] Sleeping for 1 hour...")
        time.sleep(3600)
