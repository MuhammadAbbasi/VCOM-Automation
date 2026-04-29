"""
odoo_ticket_engine.py — Fault-to-Ticket bridge between the Mazara SCADA
system and Odoo 18 (fv_interventi / fv_monitoraggio modules).

Runs every 15 minutes (via run_monitor.py).

Lifecycle per fault:
  1. Fault appears in active_anomalies (watchdog snapshot)
  2. Duration threshold met → create fv.sessione.scada + fv.anomalia + fv.intervento
  3. Admin assigns technician in Odoo → engine detects change → Telegram notification
  4. Fault disappears from active_anomalies → auto-resolve both records + report

State persistence: db/fault_state.json
"""

import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "db" / "fault_state.json"
DB_PATH = ROOT / "db" / "scada_data.db"
SETTINGS_PATH = ROOT / "user_settings.json"
CONFIG_PATH = ROOT / "config.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TICKET] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(ROOT / "odoo_tickets.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("odoo_ticket_engine")

# ---------------------------------------------------------------------------
# Odoo connection constants
# ---------------------------------------------------------------------------

ODOO_URL = "http://localhost:8069"
ODOO_DB  = "odoo"
ODOO_USER = "pietro.artale@gmail.com"
ODOO_PASS = "odoo"

DAYLIGHT_START = 6
DAYLIGHT_END   = 20

# ---------------------------------------------------------------------------
# Fault configuration
# ---------------------------------------------------------------------------

# Minutes of sustained fault before a ticket is opened (0 = immediate)
FAULT_THRESHOLDS: dict[str, int] = {
    "INVERTER TRIP":      60,
    "LOW PR":             60,
    "CRIT PR":            60,
    "ISO FAULT":          30,
    "COMM LOST":          60,
    "DC CRITICAL":        30,
    "HIGH TEMP":          30,
    "CRIT TEMP":          30,
    "TRACKER":            60,
    "GRID LIMIT CHANGE":   0,  # immediate
}

# Maps snapshot fault type → Odoo model field values
FAULT_ODOO_MAP: dict[str, dict] = {
    "INVERTER TRIP": {
        "anomalia_tipo":       "inverter_fault",
        "anomalia_priorita":   "urgente",
        "intervento_tipo":     "guasto",
        "intervento_priorita": "urgente",
        "intervento_richiesto": True,
    },
    "LOW PR": {
        "anomalia_tipo":       "produzione_bassa",
        "anomalia_priorita":   "alta",
        "intervento_tipo":     "ispezione",
        "intervento_priorita": "alta",
        "intervento_richiesto": False,
    },
    "CRIT PR": {
        "anomalia_tipo":       "produzione_bassa",
        "anomalia_priorita":   "urgente",
        "intervento_tipo":     "ispezione",
        "intervento_priorita": "urgente",
        "intervento_richiesto": False,
    },
    "ISO FAULT": {
        "anomalia_tipo":       "inverter_fault",
        "anomalia_priorita":   "urgente",
        "intervento_tipo":     "guasto",
        "intervento_priorita": "urgente",
        "intervento_richiesto": True,
    },
    "COMM LOST": {
        "anomalia_tipo":       "comunicazione",
        "anomalia_priorita":   "alta",
        "intervento_tipo":     "ispezione",
        "intervento_priorita": "alta",
        "intervento_richiesto": True,
    },
    "DC CRITICAL": {
        "anomalia_tipo":       "inverter_fault",
        "anomalia_priorita":   "alta",
        "intervento_tipo":     "guasto",
        "intervento_priorita": "alta",
        "intervento_richiesto": True,
    },
    "HIGH TEMP": {
        "anomalia_tipo":       "inverter_fault",
        "anomalia_priorita":   "alta",
        "intervento_tipo":     "ispezione",
        "intervento_priorita": "alta",
        "intervento_richiesto": False,
    },
    "CRIT TEMP": {
        "anomalia_tipo":       "inverter_fault",
        "anomalia_priorita":   "urgente",
        "intervento_tipo":     "ispezione",
        "intervento_priorita": "urgente",
        "intervento_richiesto": True,
    },
    "TRACKER": {
        "anomalia_tipo":       "tracker",
        "anomalia_priorita":   "alta",
        "intervento_tipo":     "ispezione",
        "intervento_priorita": "alta",
        "intervento_richiesto": True,
    },
    "GRID LIMIT CHANGE": {
        "anomalia_tipo":       "rete",
        "anomalia_priorita":   "urgente",
        "intervento_tipo":     "ispezione",
        "intervento_priorita": "alta",
        "intervento_richiesto": True,
    },
}

# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# SCADA data helpers
# ---------------------------------------------------------------------------

def get_latest_snapshot() -> dict | None:
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        row = conn.execute(
            "SELECT snapshot_json FROM analysis_snapshots ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return json.loads(row[0]) if row else None
    except Exception as e:
        logger.error(f"Snapshot read error: {e}")
        return None


def get_tracker_status() -> list[dict]:
    """Return tracker rows that are stale during daylight hours."""
    if not (DAYLIGHT_START <= datetime.now().hour < DAYLIGHT_END):
        return []
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        cutoff = (datetime.now() - timedelta(minutes=60)).isoformat()
        rows = conn.execute(
            "SELECT ncu_id, tcu_id, last_update FROM tracker_status "
            "WHERE last_update < ? OR last_update IS NULL",
            (cutoff,)
        ).fetchall()
        conn.close()
        return [{"ncu_id": r[0], "tcu_id": r[1], "last_update": r[2]} for r in rows]
    except Exception as e:
        logger.error(f"Tracker status error: {e}")
        return []


# ---------------------------------------------------------------------------
# Fault scanning
# ---------------------------------------------------------------------------

def scan_active_faults(snapshot: dict, stale_trackers: list) -> dict[str, dict]:
    """
    Parse the watchdog snapshot and return a dict of currently active faults.

    Returns: { fault_id: { type, inverter, message, severity } }
    """
    faults: dict[str, dict] = {}

    # From active_anomalies (watchdog-tracked faults)
    for anomaly in snapshot.get("active_anomalies", []):
        fault_type = anomaly.get("type", "").strip().upper()
        fault_id   = anomaly.get("id", "")
        inverter   = anomaly.get("inverter", "PLANT")
        message    = anomaly.get("message", "")
        severity   = anomaly.get("severity", "yellow")

        # Normalise type names to match FAULT_THRESHOLDS keys
        matched_type = _match_fault_type(fault_type)
        if matched_type and fault_id:
            faults[fault_id] = {
                "type":     matched_type,
                "inverter": inverter,
                "message":  message,
                "severity": severity,
            }

    # Tracker faults (plant-level, grouped as one fault)
    if stale_trackers:
        ncus = list({r["ncu_id"] for r in stale_trackers})
        faults["TRACKER_OFFLINE"] = {
            "type":     "TRACKER",
            "inverter": f"{len(stale_trackers)} trackers",
            "message":  f"{len(stale_trackers)} tracker(s) stale >60 min. NCUs: {', '.join(ncus[:5])}",
            "severity": "red",
        }

    return faults


def _match_fault_type(raw: str) -> str | None:
    """Map snapshot type string to FAULT_THRESHOLDS key."""
    for key in FAULT_THRESHOLDS:
        if key in raw or raw in key:
            return key
    return None


def scan_resolved_faults(snapshot: dict, state: dict) -> list[str]:
    """
    Return fault_ids that were active (in state) but no longer appear in
    active_anomalies AND now have a recovery_time in historical_trail.
    """
    active_ids = {a.get("id") for a in snapshot.get("active_anomalies", [])}
    resolved_ids_in_trail = {
        h.get("id") for h in snapshot.get("historical_trail", [])
        if h.get("recovery_time")
    }

    resolved = []
    for fault_id, entry in state.items():
        if (
            fault_id not in active_ids
            and (fault_id in resolved_ids_in_trail or fault_id == "TRACKER_OFFLINE")
            and entry.get("odoo_intervento_id")
            and not entry.get("resolved")
        ):
            resolved.append(fault_id)
    return resolved


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def _get_tg() -> tuple[str | None, str | None]:
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH, encoding="utf-8") as f:
            s = json.load(f)
        tg = s.get("telegram", {})
        if tg.get("enabled"):
            return tg.get("bot_token"), tg.get("personal_id") or tg.get("chat_id")
    return None, None


def send_telegram(message: str):
    token, chat_id = _get_tg()
    if not token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"Telegram send error: {e}")


# ---------------------------------------------------------------------------
# Human-readable ticket body
# ---------------------------------------------------------------------------

def build_ticket_body(fault_id: str, fault: dict, first_detected: str) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    fd  = first_detected[:16].replace("T", " ")
    lines = [
        f"SCADA FAULT REPORT — Auto-generated by Mazara SCADA",
        f"{'='*50}",
        f"Fault ID        : {fault_id}",
        f"Type            : {fault['type']}",
        f"Device          : {fault['inverter']}",
        f"First Detected  : {fd}",
        f"Ticket Created  : {now}",
        f"Severity        : {fault['severity'].upper()}",
        f"",
        f"DESCRIPTION",
        f"{'-'*50}",
        f"{fault['message']}",
        f"",
        f"ACTION REQUIRED",
        f"{'-'*50}",
        _action_text(fault["type"]),
        f"",
        f"SOURCE: Mazara SCADA Monitoring System (automatic ticket)",
    ]
    return "\n".join(lines)


def build_resolution_body(fault_id: str, fault: dict, first_detected: str) -> str:
    fd  = first_detected[:16].replace("T", " ")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    duration_min = int(
        (datetime.now() - datetime.fromisoformat(first_detected)).total_seconds() / 60
    )
    lines = [
        f"AUTO-RESOLUTION REPORT",
        f"{'='*50}",
        f"Fault ID        : {fault_id}",
        f"Type            : {fault['type']}",
        f"Device          : {fault['inverter']}",
        f"First Detected  : {fd}",
        f"Resolved At     : {now}",
        f"Total Duration  : {duration_min} minutes",
        f"",
        f"The fault cleared automatically without field intervention.",
        f"No further action required unless the fault recurs.",
    ]
    return "\n".join(lines)


def _action_text(fault_type: str) -> str:
    actions = {
        "INVERTER TRIP":      "Check inverter panel for fault code. Reset if safe.",
        "LOW PR":             "Verify irradiance sensor and string health.",
        "CRIT PR":            "Immediate inspection required. Check all strings.",
        "ISO FAULT":          "DANGER: Insulation risk. Disconnect and test cables.",
        "COMM LOST":          "Check network/VCOM connection for this inverter.",
        "DC CRITICAL":        "Inspect MPPT inputs and string connections.",
        "HIGH TEMP":          "Check inverter ventilation and ambient conditions.",
        "CRIT TEMP":          "URGENT: Risk of thermal shutdown. Inspect immediately.",
        "TRACKER":            "Check tracker NCU/TCU communication and power supply.",
        "GRID LIMIT CHANGE":  "Grid operator has applied production curtailment.",
    }
    return actions.get(fault_type, "Inspect device and consult SCADA logs.")


# ---------------------------------------------------------------------------
# Core ticket creation
# ---------------------------------------------------------------------------

def open_ticket(client, fault_id: str, fault: dict, state: dict, session_id: int) -> dict:
    """Create fv.anomalia + fv.intervento for one fault. Returns updated state entry."""
    cfg = FAULT_ODOO_MAP.get(fault["type"], FAULT_ODOO_MAP["GRID LIMIT CHANGE"])
    first_detected = state.get(fault_id, {}).get("first_detected", datetime.now().isoformat())

    titolo = f"[{fault['type']}] {fault['inverter']} — Mazara 01"
    descrizione = build_ticket_body(fault_id, fault, first_detected)
    causa_guasto = build_ticket_body(fault_id, fault, first_detected)

    # Create anomalia
    anomalia_id = client.create_anomalia(
        session_id=session_id,
        titolo=titolo,
        tipo=cfg["anomalia_tipo"],
        priorita=cfg["anomalia_priorita"],
        descrizione=descrizione,
        intervento_richiesto=cfg["intervento_richiesto"],
    )

    # Create intervento
    intervento_id = client.create_intervento(
        titolo=titolo,
        tipo_intervento=cfg["intervento_tipo"],
        priorita=cfg["intervento_priorita"],
        causa_guasto=causa_guasto,
        session_id=session_id,
    )

    # Link anomalia → intervento
    if anomalia_id and intervento_id:
        client.link_anomalia_to_intervento(anomalia_id, intervento_id)

    return {
        "first_detected":       first_detected,
        "message":              fault["message"],
        "odoo_anomalia_id":     anomalia_id,
        "odoo_intervento_id":   intervento_id,
        "notified":             False,
        "assignment_notified":  False,
        "resolved":             False,
        "last_technician":      None,
    }


def resolve_ticket(client, fault_id: str, fault: dict, entry: dict):
    """Auto-resolve both fv.anomalia and fv.intervento for a cleared fault."""
    resolution = build_resolution_body(fault_id, fault, entry["first_detected"])

    if entry.get("odoo_anomalia_id"):
        client.resolve_anomalia(entry["odoo_anomalia_id"], resolution)

    if entry.get("odoo_intervento_id"):
        client.auto_resolve_intervento(entry["odoo_intervento_id"], resolution)


def check_assignment_changes(client, state: dict) -> list[str]:
    """
    Poll open interventions. If tecnico_id was just assigned (state=in_corso),
    return a notification message.
    """
    notifications = []
    for fault_id, entry in state.items():
        intv_id = entry.get("odoo_intervento_id")
        if not intv_id or entry.get("resolved") or entry.get("assignment_notified"):
            continue
        intv = client.get_intervento(intv_id)
        if not intv:
            continue
        tecnico = intv.get("tecnico_id")
        state_val = intv.get("state")
        # Notify as soon as a technician is assigned (assegnato) or starts (in_corso)
        if tecnico and state_val in ("assegnato", "in_corso") and not entry.get("assignment_notified"):
            tech_name = tecnico[1] if isinstance(tecnico, list) else str(tecnico)
            state_label = "Assegnato" if state_val == "assegnato" else "In Corso"
            msg = (
                f"👷 *Ticket Assegnato — Mazara 01*\n"
                f"Fault: `{fault_id}`\n"
                f"Ticket: `{intv['name']}`\n"
                f"Assegnato a: *{tech_name}*\n"
                f"Stato: {state_label}\n"
                f"Tipo: {intv['tipo_intervento']}\n"
                f"Priorità: {intv['priorita'].upper()}"
            )
            notifications.append(msg)
            entry["assignment_notified"] = True
            entry["last_technician"] = tech_name
    return notifications


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def run():
    logger.info("[ENGINE] Starting ticket engine run...")

    # 1. Load data
    snapshot = get_latest_snapshot()
    if not snapshot:
        logger.warning("[ENGINE] No snapshot available — skipping run.")
        return

    stale_trackers = get_tracker_status()
    state = load_state()
    now = datetime.now()

    # 2. Scan faults
    active_faults = scan_active_faults(snapshot, stale_trackers)
    resolved_ids  = scan_resolved_faults(snapshot, state)

    # 3. Connect to Odoo
    from db.odoo_client import OdooClient
    client = OdooClient(ODOO_URL, ODOO_DB, ODOO_USER, ODOO_PASS)
    if not client.login():
        logger.error("[ENGINE] Odoo login failed — aborting run.")
        return

    # 4. Update first_detected timestamps for newly seen faults
    for fault_id, fault in active_faults.items():
        if fault_id not in state:
            state[fault_id] = {
                "first_detected": now.isoformat(),
                "type":           fault["type"],
                "message":        fault["message"],
                "odoo_anomalia_id":   None,
                "odoo_intervento_id": None,
                "notified":           False,
                "assignment_notified": False,
                "resolved":           False,
                "last_technician":    None,
            }
            logger.info(f"[ENGINE] New fault detected: {fault_id}")
        else:
            # Refresh message in case values changed
            state[fault_id]["message"] = fault["message"]

    # 5. Open tickets for faults that have exceeded their duration threshold
    tickets_opened = []
    session_id = None  # lazily created

    for fault_id, fault in active_faults.items():
        entry = state[fault_id]

        # Skip if ticket already exists
        if entry.get("odoo_intervento_id"):
            continue

        # Check duration threshold
        first_dt   = datetime.fromisoformat(entry["first_detected"])
        elapsed    = (now - first_dt).total_seconds() / 60
        threshold  = FAULT_THRESHOLDS.get(fault["type"], 60)

        if elapsed < threshold:
            logger.info(
                f"[ENGINE] {fault_id}: {int(elapsed)}/{threshold} min — not yet"
            )
            continue

        # Create shared session on first ticket of this run
        if session_id is None:
            fault_count = sum(1 for f in active_faults.values()
                              if not state.get(f.get("id", ""), {}).get("odoo_intervento_id"))
            session_id = client.create_scada_session(
                fault_summary=f"Auto-session: {len(active_faults)} active fault(s)",
                stato_impianto="alarm" if any(
                    f["severity"] == "red" for f in active_faults.values()
                ) else "warning",
            )

        logger.info(f"[ENGINE] Opening Odoo ticket for {fault_id}")
        updated = open_ticket(client, fault_id, fault, state, session_id)
        state[fault_id].update(updated)

        if updated["odoo_intervento_id"]:
            tickets_opened.append((fault_id, fault, updated))

    # 6. Auto-resolve cleared faults
    tickets_resolved = []
    for fault_id in resolved_ids:
        entry  = state[fault_id]
        fault  = {"type": entry["type"], "inverter": "—", "message": entry.get("message", "")}
        logger.info(f"[ENGINE] Auto-resolving {fault_id}")
        resolve_ticket(client, fault_id, fault, entry)
        entry["resolved"] = True
        tickets_resolved.append((fault_id, entry))

    # 7. Check for assignment notifications
    assignment_msgs = check_assignment_changes(client, state)
    for msg in assignment_msgs:
        send_telegram(msg)
        logger.info(f"[ENGINE] Assignment notification sent.")

    # 8. Send Telegram notifications for new tickets
    for fault_id, fault, entry in tickets_opened:
        intv_id  = entry["odoo_intervento_id"]
        anom_id  = entry["odoo_anomalia_id"]
        cfg = FAULT_ODOO_MAP.get(fault["type"], {})
        priority = cfg.get("intervento_priorita", "normale").upper()
        icon = {"URGENTE": "🔴", "ALTA": "🟠", "NORMALE": "🟡", "BASSA": "🟢"}.get(priority, "⚪")

        elapsed_min = int(
            (now - datetime.fromisoformat(entry["first_detected"])).total_seconds() / 60
        )

        msg = (
            f"{icon} *NEW FAULT TICKET — Mazara 01*\n"
            f"\n"
            f"*Fault:* `{fault_id}`\n"
            f"*Type:* {fault['type']}\n"
            f"*Device:* {fault['inverter']}\n"
            f"*Priority:* {priority}\n"
            f"*Duration:* {elapsed_min} min\n"
            f"\n"
            f"*Details:*\n{fault['message']}\n"
            f"\n"
            f"*Odoo:* Intervento #{intv_id} | Anomalia #{anom_id}\n"
            f"📋 Stato: `Nuovo` — in attesa di assegnazione admin\n"
            f"\n"
            f"_Action:_ {_action_text(fault['type'])}"
        )
        send_telegram(msg)
        state[fault_id]["notified"] = True

    # 9. Send Telegram notifications for resolved tickets
    for fault_id, entry in tickets_resolved:
        intv_id = entry.get("odoo_intervento_id")
        
        # Filter: Only notify recovery for High/Urgent priority tickets to reduce noise
        cfg = FAULT_ODOO_MAP.get(entry.get("type"), {})
        priority = cfg.get("intervento_priorita", "normale").upper()
        if priority not in ["ALTA", "URGENTE"]:
            continue

        elapsed_min = 0
        try:
            elapsed_min = int(
                (now - datetime.fromisoformat(entry["first_detected"])).total_seconds() / 60
            )
        except Exception:
            pass

        msg = (
            f"✅ *FAULT AUTO-RESOLVED — Mazara 01*\n"
            f"\n"
            f"*Fault:* `{fault_id}`\n"
            f"*Type:* {entry.get('type','?')}\n"
            f"*Total Duration:* {elapsed_min} min\n"
            f"\n"
            f"*Odoo:* Intervento #{intv_id} → segnato come `Chiuso` (auto-risolto)\n"
            f"_The fault cleared without field intervention._"
        )
        send_telegram(msg)

    # 10. Purge old resolved entries (keep last 200)
    resolved_entries = {k: v for k, v in state.items() if v.get("resolved")}
    if len(resolved_entries) > 200:
        oldest = sorted(resolved_entries, key=lambda k: state[k].get("first_detected", ""))
        for k in oldest[: len(resolved_entries) - 200]:
            del state[k]

    save_state(state)

    logger.info(
        f"[ENGINE] Run complete. Opened: {len(tickets_opened)}, "
        f"Resolved: {len(tickets_resolved)}, Active: {len(active_faults)}"
    )


# ---------------------------------------------------------------------------
# Entry point (runs as standalone service every 15 min)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("[ENGINE] Odoo Ticket Engine started.")
    while True:
        try:
            run()
        except Exception as e:
            logger.error(f"[ENGINE] Crashed: {e}", exc_info=True)
            try:
                send_telegram(f"🚨 *Odoo Ticket Engine CRASHED*\nError: `{e}`")
            except Exception:
                pass
        logger.info("[ENGINE] Sleeping 15 min...")
        time.sleep(900)
