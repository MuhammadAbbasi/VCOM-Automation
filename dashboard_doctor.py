
import os
import json
import time
import logging
import requests
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("dashboard_doctor.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DashboardDoctor")

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "db" / "scada_data.db"
LOG_DB_PATH = ROOT / "db" / "scada_logs.db"
LINK_STATUS_PATH = ROOT / "db" / "link_status.json"
SETTINGS_PATH = ROOT / "user_settings.json"
CONFIG_PATH = ROOT / "config.json"

def get_settings():
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH) as f:
            return json.load(f)
    return {}

def get_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}

def send_telegram(message):
    settings = get_settings()
    tg = settings.get("telegram", {})
    if not tg.get("enabled"): return
    
    token = tg.get("bot_token")
    chat_id = tg.get("personal_id") or tg.get("chat_id")
    
    if not token or not chat_id:
        logger.error("Telegram token or chat_id missing in settings.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if not resp.ok:
            logger.error(f"Telegram error: {resp.text}")
    except Exception as e:
        logger.error(f"Failed to send telegram: {e}")

def check_db_health():
    """Check if tables are receiving data."""
    results = {"status": "HEALTHY", "issues": []}
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check Potenza AC (Main production table)
        cursor.execute("SELECT timestamp FROM potenza_ac ORDER BY timestamp DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            last_ts = datetime.fromisoformat(row[0].replace('Z', '+00:00'))
            if datetime.now() - last_ts.replace(tzinfo=None) > timedelta(minutes=15):
                results["issues"].append(f"❌ Data Stale: Last Potenza AC entry was {row[0]}")
                results["status"] = "WARNING"
        else:
            results["issues"].append("❌ Data Missing: potenza_ac table is empty.")
            results["status"] = "CRITICAL"
            
        # Check Tracker Status
        cursor.execute("SELECT COUNT(DISTINCT ncu_id) FROM tracker_status")
        ncu_count = cursor.fetchone()[0]
        if ncu_count < 1:
             results["issues"].append("❌ Trackers Offline: No NCUs found in DB.")
             results["status"] = "CRITICAL"
        
        cursor.execute("SELECT timestamp FROM tracker_status ORDER BY timestamp DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            last_ts = datetime.fromisoformat(row[0].replace('Z', '+00:00'))
            if datetime.now() - last_ts.replace(tzinfo=None) > timedelta(minutes=60):
                results["issues"].append(f"❌ Tracker Stale: Last update was {row[0]}")
                if results["status"] != "CRITICAL": results["status"] = "WARNING"

        conn.close()
    except Exception as e:
        results["status"] = "CRITICAL"
        results["issues"].append(f"❌ DB Error: {e}")
    return results

def check_connections():
    results = {"status": "HEALTHY", "issues": []}
    
    # 1. Check link_status.json
    if LINK_STATUS_PATH.exists():
        with open(LINK_STATUS_PATH) as f:
            link = json.load(f)
            if link.get("status") != "online":
                results["issues"].append(f"❌ Link Offline: Status is {link.get('status')}")
                results["status"] = "CRITICAL"
            
            ts_str = link.get("timestamp")
            if ts_str:
                ts = datetime.fromisoformat(ts_str)
                if datetime.now() - ts > timedelta(minutes=5):
                    results["issues"].append("❌ Heartbeat Stale: link_status.json not updated recently.")
                    if results["status"] == "HEALTHY": results["status"] = "WARNING"
    else:
        results["issues"].append("❌ File Missing: link_status.json not found.")
        results["status"] = "CRITICAL"

    # 2. Check Dashboard API
    try:
        cfg = get_config()
        user = cfg.get("DASHBOARD_USER", "admin")
        pw = cfg.get("DASHBOARD_PASS", "password")
        
        resp = requests.get("http://localhost:8080/api/status", auth=(user, pw), timeout=5)
        if not resp.ok:
            results["issues"].append(f"❌ API Error: /api/status returned {resp.status_code}")
            results["status"] = "CRITICAL"
    except Exception as e:
        results["issues"].append(f"❌ Dashboard Unreachable: {e}")
        results["status"] = "CRITICAL"

    return results

def run_doctor():
    logger.info("[DOCTOR] Starting health check...")
    
    db_report = check_db_health()
    conn_report = check_connections()
    
    summary = []
    summary.append("🏥 *DASHBOARD DOCTOR REPORT*")
    summary.append(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    overall_status = "HEALTHY"
    if db_report["status"] == "CRITICAL" or conn_report["status"] == "CRITICAL":
        overall_status = "CRITICAL"
    elif db_report["status"] == "WARNING" or conn_report["status"] == "WARNING":
        overall_status = "WARNING"
    else:
        overall_status = "HEALTHY"
        
    summary.append(f"Status: {overall_status}")
    
    all_issues = db_report["issues"] + conn_report["issues"]
    if all_issues:
        summary.append("\n*Issues Found:*")
        for issue in all_issues:
            summary.append(issue)
            logger.warning(f"[ISSUE] {issue}")
    else:
        summary.append("\n✅ All systems nominal.")
        logger.info("All systems nominal.")
        
    report_text = "\n".join(summary)
    logger.info(f"Report generated. Status: {overall_status}")
    
    send_telegram(report_text)

if __name__ == "__main__":
    while True:
        try:
            run_doctor()
        except Exception as e:
            logger.error(f"Doctor crashed: {e}")
            send_telegram(f"🚨 *Dashboard Doctor CRASHED*\nError: {e}")
        
        logger.info("Doctor sleeping for 1 hour...")
        time.sleep(3600)
