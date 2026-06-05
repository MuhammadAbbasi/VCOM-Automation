"""
telegram_bot.py — Mazara 01 Solar Plant Monitoring Bot

Real-time SCADA intelligence delivered via Telegram.
Any free-text message triggers the LLM directly — no /ai needed.

Commands:
  /status           — Live AC power, PR, inverter health
  /alerts           — Active anomalies and fault conditions
  /daily            — Daily energy production summary
  /week             — 7-day production history
  /inverters        — All 36 inverters health matrix
  /inverter <name>  — Single inverter deep-dive (e.g. /inverter TX1-03)
  /peak             — Today's peak power and time
  /compare          — TX1 vs TX2 vs TX3 production comparison
  /pr               — Performance Ratio breakdown by transformer
  /energy           — Monthly / yearly energy totals
  /weather          — Irradiance and temperature sensor readings
  /uptime           — Plant uptime percentage today
  /generate_ticket  — Create a new fault ticket in Odoo
  /help             — Full command reference
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
import threading
from pathlib import Path
import requests
from logging.handlers import RotatingFileHandler

if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        except Exception:
            pass

try:
    import llm_agent
except ImportError:
    llm_agent = None

ROOT          = Path(__file__).resolve().parent
DATA_DIR      = ROOT / "extracted_data"
SETTINGS_PATH = ROOT / "user_settings.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BOT] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(ROOT / "telegram_bot.log", maxBytes=1_000_000_000, backupCount=3, encoding="utf-8"),
    ],
)
logger = logging.getLogger("telegram_bot")

POLL_INTERVAL = 4
API_TIMEOUT   = 10

# ---------------------------------------------------------------------------
# Ticket flow
# ---------------------------------------------------------------------------

FAULT_TYPES_TG = {
    "1":  ("INVERTER TRIP",     "inverter_fault",   "guasto",    "urgente", "urgente"),
    "2":  ("LOW PR",            "produzione_bassa",  "ispezione", "alta",    "alta"),
    "3":  ("CRIT PR",           "produzione_bassa",  "ispezione", "urgente", "urgente"),
    "4":  ("ISO FAULT",         "inverter_fault",    "guasto",    "urgente", "urgente"),
    "5":  ("COMM LOST",         "comunicazione",     "ispezione", "alta",    "alta"),
    "6":  ("DC MPPT FAULT",     "inverter_fault",    "guasto",    "alta",    "alta"),
    "7":  ("HIGH TEMP",         "inverter_fault",    "ispezione", "alta",    "alta"),
    "8":  ("CRIT TEMP",         "inverter_fault",    "ispezione", "urgente", "urgente"),
    "9":  ("TRACKER OFFLINE",   "tracker",           "ispezione", "alta",    "alta"),
    "10": ("GRID LIMIT CHANGE", "rete",              "ispezione", "normale", "media"),
    "11": ("CUSTOM",            "altro",             "altro",     "normale", "bassa"),
}

PRIORITIES_TG = {"1": "bassa", "2": "normale", "3": "alta", "4": "urgente"}

INTERVENTION_TYPES_TG = {
    "1": "manutenzione_ordinaria",
    "2": "manutenzione_straordinaria",
    "3": "guasto",
    "4": "ispezione",
    "5": "sfalcio",
    "6": "collaudo",
    "7": "altro",
}

_FAULT_MENU = (
    "🎫 *Crea Ticket — Mazara 01*\n\n"
    "Tipo di guasto:\n"
    "1️⃣ INVERTER TRIP\n2️⃣ LOW PR\n3️⃣ CRIT PR\n4️⃣ ISO FAULT\n"
    "5️⃣ COMM LOST\n6️⃣ DC MPPT FAULT\n7️⃣ HIGH TEMP\n8️⃣ CRIT TEMP\n"
    "9️⃣ TRACKER OFFLINE\n🔟 GRID LIMIT CHANGE\n1️⃣1️⃣ CUSTOM\n\n"
    "Rispondi con 1-11 oppure /cancel"
)

_PRIO_MENU = (
    "⭐ *Priorità:*\n"
    "1️⃣ Bassa   2️⃣ Normale   3️⃣ Alta   4️⃣ Urgente\n\n"
    "Rispondi con 1-4 oppure `ok` per usare il default"
)

_INTV_MENU = (
    "🔧 *Tipo Intervento:*\n"
    "1️⃣ Manutenzione Ordinaria\n2️⃣ Manutenzione Straordinaria\n"
    "3️⃣ Guasto / Riparazione\n4️⃣ Ispezione\n5️⃣ Sfalcio / Pulizia\n"
    "6️⃣ Collaudo\n7️⃣ Altro\n\n"
    "Rispondi con 1-7 oppure `ok` per usare il default"
)

ticket_sessions: dict = {}


def _odoo_client():
    """Build OdooClient from environment variables."""
    from db.odoo_client import OdooClient
    url  = os.environ.get("ODOO_URL",  "http://localhost:8069")
    db   = os.environ.get("ODOO_DB",   "odoo")
    user = os.environ.get("ODOO_USER", "")
    pw   = os.environ.get("ODOO_PASS", "")
    return OdooClient(url, db, user, pw)


def _create_odoo_ticket(data: dict):
    client = _odoo_client()
    if not client.login():
        raise RuntimeError("Odoo login failed")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    label   = data["fault_label"]
    device  = data["device"]
    titolo  = f"[{label}] {device} — Mazara 01"
    causa   = "\n".join([
        "SCADA FAULT REPORT — Submitted via Telegram",
        "=" * 50,
        f"Fault Type      : {label}",
        f"Device          : {device}",
        f"Submitted At    : {now_str}",
        f"Priority        : {data['intv_prio'].upper()}",
        "",
        "DESCRIPTION",
        "-" * 50,
        data["description"],
    ])
    if data.get("notes"):
        causa += f"\n\nNOTES\n{'-'*50}\n{data['notes']}"

    session_id    = client.create_scada_session(
        fault_summary=f"Telegram ticket: {label} on {device}",
        stato_impianto="alarm" if data["intv_prio"] == "urgente" else "warning",
    )
    anomalia_id   = client.create_anomalia(
        session_id=session_id, titolo=titolo,
        tipo=data["anom_tipo"], priorita=data["anom_prio"],
        descrizione=causa, intervento_richiesto=data["field_work"],
    )
    intervento_id = client.create_intervento(
        titolo=titolo, tipo_intervento=data["intv_tipo"],
        priorita=data["intv_prio"], causa_guasto=causa,
        session_id=session_id,
    )
    if anomalia_id and intervento_id:
        client.link_anomalia_to_intervento(anomalia_id, intervento_id)
    intv_data = client.get_intervento(intervento_id)
    intv_name = intv_data["name"] if intv_data else f"ID-{intervento_id}"
    return intv_name, intervento_id, anomalia_id


def _handle_ticket_step(bot, chat_id: int, text: str) -> bool:
    session = ticket_sessions.get(chat_id)
    if not session:
        return False

    step = session["step"]
    data = session["data"]
    t    = text.strip()

    if t.lower() in ("/cancel", "cancel"):
        ticket_sessions.pop(chat_id, None)
        bot.send_message(chat_id, "❌ Ticket annullato.")
        return True

    if step == "fault_type":
        if t not in FAULT_TYPES_TG:
            bot.send_message(chat_id, "⚠️ Scelta non valida. Rispondi con 1-11 oppure /cancel")
            return True
        label, anom_tipo, intv_tipo, intv_prio, anom_prio = FAULT_TYPES_TG[t]
        data.update(fault_key=t, anom_tipo=anom_tipo, intv_tipo=intv_tipo,
                    intv_prio=intv_prio, anom_prio=anom_prio)
        if t == "11":
            session["step"] = "custom_name"
            bot.send_message(chat_id, "📝 Nome del guasto personalizzato:")
            return True
        data["fault_label"] = label
        session["step"] = "device"
        bot.send_message(chat_id,
            f"✅ Fault: *{label}*\n\n🔌 *Quale dispositivo?*\n"
            "Es. `TX1-03`, `TX2-11`, `PLANT`, `GRID`\n\noppure /cancel")
        return True

    if step == "custom_name":
        if not t or t.startswith("/"):
            bot.send_message(chat_id, "⚠️ Inserisci un nome per il guasto.")
            return True
        data["fault_label"] = t.upper()
        session["step"] = "device"
        bot.send_message(chat_id,
            f"✅ Fault: *{data['fault_label']}*\n\n🔌 *Quale dispositivo?*\n"
            "Es. `TX1-03`, `TX2-11`, `PLANT`")
        return True

    if step == "device":
        if not t or t.startswith("/"):
            bot.send_message(chat_id, "⚠️ Inserisci il nome del dispositivo.")
            return True
        data["device"] = t.upper()
        session["step"] = "priority"
        bot.send_message(chat_id,
            f"✅ Device: *{data['device']}*\n\n{_PRIO_MENU}\n"
            f"Default per *{data['fault_label']}*: `{data['intv_prio'].upper()}`")
        return True

    if step == "priority":
        if t.lower() != "ok":
            if t not in PRIORITIES_TG:
                bot.send_message(chat_id, "⚠️ Rispondi con 1-4 oppure `ok`.")
                return True
            data["intv_prio"] = PRIORITIES_TG[t]
            data["anom_prio"] = "media" if data["intv_prio"] == "normale" else data["intv_prio"]
        session["step"] = "intv_type"
        bot.send_message(chat_id,
            f"✅ Priorità: *{data['intv_prio'].upper()}*\n\n{_INTV_MENU}\n"
            f"Default per *{data['fault_label']}*: `{data['intv_tipo']}`")
        return True

    if step == "intv_type":
        if t.lower() != "ok":
            if t not in INTERVENTION_TYPES_TG:
                bot.send_message(chat_id, "⚠️ Rispondi con 1-7 oppure `ok`.")
                return True
            data["intv_tipo"] = INTERVENTION_TYPES_TG[t]
        session["step"] = "description"
        bot.send_message(chat_id,
            f"✅ Tipo: *{data['intv_tipo']}*\n\n"
            "📋 *Descrivi il guasto:*\noppure /cancel")
        return True

    if step == "description":
        if not t or t.startswith("/"):
            bot.send_message(chat_id, "⚠️ Inserisci una descrizione.")
            return True
        data["description"] = t
        session["step"] = "notes"
        bot.send_message(chat_id,
            "✅ Descrizione salvata.\n\n📝 *Note aggiuntive?*\n"
            "Invia le note oppure `none` per saltare")
        return True

    if step == "notes":
        data["notes"] = "" if t.lower() == "none" else t
        session["step"] = "field_work"
        bot.send_message(chat_id,
            "📌 *Richiede intervento sul campo?*\nRispondi `y` oppure `n`")
        return True

    if step == "field_work":
        if t.lower() not in ("y", "n", "yes", "no", "si", "sì"):
            bot.send_message(chat_id, "⚠️ Rispondi con `y` oppure `n`.")
            return True
        data["field_work"] = t.lower() in ("y", "yes", "si", "sì")
        session["step"] = "confirm"
        desc_preview = data["description"][:120] + ("…" if len(data["description"]) > 120 else "")
        preview = (
            f"📋 *ANTEPRIMA TICKET*\n{'─'*32}\n"
            f"*Tipo:*       {data['fault_label']}\n"
            f"*Device:*     {data['device']}\n"
            f"*Priorità:*   {data['intv_prio'].upper()}\n"
            f"*Intervento:* {data['intv_tipo']}\n"
            f"*Campo:*      {'Sì' if data['field_work'] else 'No'}\n"
            f"*Descrizione:* {desc_preview}\n"
        )
        if data.get("notes"):
            preview += f"*Note:* {data['notes'][:80]}\n"
        preview += "\n✅ Confermi? (`y` / `n`)"
        bot.send_message(chat_id, preview)
        return True

    if step == "confirm":
        if t.lower() in ("n", "no"):
            ticket_sessions.pop(chat_id, None)
            bot.send_message(chat_id, "❌ Ticket annullato.")
            return True
        if t.lower() not in ("y", "yes", "si", "sì"):
            bot.send_message(chat_id, "⚠️ Rispondi con `y` oppure `n`.")
            return True
        ticket_sessions.pop(chat_id, None)
        bot.send_message(chat_id, "⏳ Creazione ticket in Odoo…")
        try:
            intv_name, intv_id, anom_id = _create_odoo_ticket(data)
            bot.send_message(chat_id,
                f"✅ *TICKET CREATO — Mazara 01*\n\n"
                f"*Intervento:* `{intv_name}`\n"
                f"*Anomalia:*  #{anom_id}\n"
                f"*Stato:*     Nuovo — in attesa assegnazione\n\n"
                f"📍 Odoo → FV Interventi"
            )
        except Exception as e:
            logger.error(f"Ticket creation error: {e}")
            bot.send_message(chat_id, f"❌ Errore nella creazione: {str(e)[:120]}")
        return True

    return False


def start_ticket_flow(bot, chat_id: int) -> None:
    ticket_sessions[chat_id] = {"step": "fault_type", "data": {}}
    bot.send_message(chat_id, _FAULT_MENU)


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def load_settings() -> dict:
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def get_latest_dashboard_json() -> dict | None:
    try:
        from db.db_manager import get_latest_snapshot_date, load_latest_snapshot, get_all_tracker_status
        today = datetime.now().strftime("%Y-%m-%d")
        data = load_latest_snapshot(today)
        if not data:
            latest_date = get_latest_snapshot_date()
            if latest_date:
                data = load_latest_snapshot(latest_date)
        if data:
            data["trackers"] = get_all_tracker_status()
            return data
    except Exception as e:
        logger.warning(f"DB snapshot read failed: {e}")
    return None


def get_snapshots_for_days(n: int) -> list[dict]:
    """Return list of (date_str, snapshot) for the last n days that have data."""
    results = []
    try:
        from db.db_manager import load_latest_snapshot
        for i in range(n):
            d = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            snap = load_latest_snapshot(d)
            if snap:
                results.append((d, snap))
    except Exception as e:
        logger.warning(f"Multi-day snapshot read failed: {e}")
    return results


def _tx_of(name: str) -> str:
    n = name.upper()
    if "TX1" in n: return "TX1"
    if "TX2" in n: return "TX2"
    if "TX3" in n: return "TX3"
    return "OTHER"


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def build_status_message(data: dict) -> str:
    macro = data.get("macro_health", {})
    try:
        sync_time = datetime.fromisoformat(
            macro.get("last_sync", "").replace("Z", "+00:00")
        ).strftime("%H:%M")
    except Exception:
        sync_time = datetime.now().strftime("%H:%M")

    inv_health = data.get("inverter_health", {})
    total_mw = macro.get("total_ac_power_mw", 0.0)
    if not total_mw:
        total_mw = sum(float(h.get("ac_v", 0) or 0) for h in inv_health.values()) / 1e6
    avg_pr = macro.get("avg_pr", 0.0)
    if not avg_pr:
        pr_vals = [float(h.get("pr_v", 0)) for h in inv_health.values() if h.get("pr_v") is not None]
        avg_pr = sum(pr_vals) / len(pr_vals) if pr_vals else 0.0
    energy_mwh = macro.get("total_energy_mwh", 0.0)
    poa = macro.get("poa", macro.get("avg_irradiance", 0.0))
    online  = macro.get("online", "—")
    tripped = macro.get("tripped", "—")
    comms   = macro.get("comms_lost", "—")

    lines = [
        "🌞 *Mazara 01 — Live Status*",
        f"🕐 Last sync: *{sync_time}*",
        "━━━━━━━━━━━━━━━━━━━",
        "",
        f"⚡ *Power:*    {total_mw:.2f} MW",
        f"🔋 *Energy:*   {energy_mwh:.1f} MWh today",
        f"📊 *Avg PR:*   {avg_pr:.1f}%",
    ]
    if poa:
        lines.append(f"☀️ *Irrad:*    {poa:.0f} W/m²")
    lines += [
        "",
        f"🟢 Online: *{online}*   🔴 Tripped: *{tripped}*   🔇 Comms: *{comms}*",
    ]

    alerts = data.get("active_anomalies", [])
    if alerts:
        lines += ["", f"🚨 *Active Alerts ({len(alerts)}):*"]
        seen, count = set(), 0
        for a in alerts:
            inv   = a.get("inverter", "?") if isinstance(a, dict) else str(a)
            atype = a.get("rule", a.get("type", "Anomaly")) if isinstance(a, dict) else ""
            entry = f" • {inv} — {atype}" if atype else f" • {inv}"
            if entry not in seen:
                seen.add(entry)
                lines.append(entry)
                count += 1
                if count >= 5:
                    if len(alerts) - 5 > 0:
                        lines.append(f" _...and {len(alerts)-5} more_")
                    break
    else:
        lines += ["", "✅ No active alerts"]

    return "\n".join(lines)


def build_alerts_message(data: dict) -> str:
    alerts = data.get("active_anomalies", [])
    if not alerts:
        return "✅ *No active alerts* — all inverters nominal."
    critical, warnings = [], []
    seen = set()
    for a in alerts:
        if not isinstance(a, dict):
            warnings.append(f"🟡  • {a}")
            continue
        inv      = a.get("inverter", "?")
        atype    = a.get("rule", a.get("type", "Anomaly"))
        severity = str(a.get("severity", "")).upper()
        since    = a.get("since", a.get("start_time", ""))
        try:
            since_fmt = datetime.fromisoformat(since).strftime("%H:%M") if since else ""
        except Exception:
            since_fmt = ""
        line = f" • *{inv}* — {atype}" + (f" _(since {since_fmt})_" if since_fmt else "")
        if line in seen: continue
        seen.add(line)
        (critical if severity == "CRITICAL" else warnings).append(line)

    lines = [f"🚨 *Active Alerts ({len(critical)+len(warnings)})*", "━━━━━━━━━━━━━━━━━━━"]
    if critical:
        lines += ["\n🔴 *Critical:*"] + [f"🔴{l}" for l in critical[:15]]
    if warnings:
        lines += ["\n🟡 *Warnings:*"] + [f"🟡{l}" for l in warnings[:15]]
    return "\n".join(lines)


def build_daily_message(data: dict) -> str:
    h = data.get("macro_health", {})
    try:
        sync_fmt = datetime.fromisoformat(
            h.get("last_sync", "").replace("Z", "+00:00")
        ).strftime("%H:%M")
    except Exception:
        sync_fmt = "—"
    poa     = h.get("poa", h.get("avg_irradiance", 0.0))
    tripped = h.get("tripped", 0)
    comms   = h.get("comms_lost", 0)
    today   = datetime.now().strftime("%d/%m/%Y")
    msg = (
        f"📅 *Daily Report — {today}*\n━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚡ *Current Power:* {h.get('total_ac_power_mw', 0):.2f} MW\n"
        f"🔋 *Energy Today:*  {h.get('total_energy_mwh', 0):.2f} MWh\n"
        f"📈 *Average PR:*    {h.get('avg_pr', 0):.1f}%\n"
    )
    if poa:
        msg += f"☀️ *Irradiance:*    {poa:.0f} W/m²\n"
    msg += f"\n🟢 Online: *{h.get('online','—')}/36*\n"
    if tripped: msg += f"🔴 Tripped: *{tripped}*\n"
    if comms:   msg += f"🔇 Comms Lost: *{comms}*\n"
    msg += f"\n⏰ Last Sync: *{sync_fmt}*"
    return msg


def build_weekly_message() -> str:
    days = get_snapshots_for_days(7)
    if not days:
        return "⚠️ No historical data available."
    lines = ["📆 *7-Day Production*", "━━━━━━━━━━━━━━━━━━━", ""]
    total = 0.0
    for date_str, snap in reversed(days):
        h   = snap.get("macro_health", {})
        mwh = float(h.get("total_energy_mwh") or 0.0)
        pr  = float(h.get("avg_pr") or 0.0)
        d   = datetime.strptime(date_str, "%Y-%m-%d").strftime("%a %d/%m")
        lines.append(f"`{d}`  *{mwh:.1f} MWh*  PR:{pr:.0f}%")
        total += mwh
    lines += ["", f"📊 *7-day total: {total:.1f} MWh*"]
    avg = total / len(days)
    lines.append(f"📉 *Daily avg: {avg:.1f} MWh*")
    return "\n".join(lines)


def build_inverters_message(data: dict) -> str:
    inv_health = data.get("inverter_health", {})
    if not inv_health:
        return "⚠️ No inverter data."
    lines = ["🔌 *All Inverters — Health Matrix*", "━━━━━━━━━━━━━━━━━━━", ""]
    by_tx: dict[str, list] = {"TX1": [], "TX2": [], "TX3": []}
    for name, h in sorted(inv_health.items()):
        tx = _tx_of(name)
        status = str(h.get("status", "ok")).upper()
        pr     = h.get("pr_v")
        ac     = h.get("ac_v")
        if status in ("CRITICAL", "TRIPPED", "FAULT"):
            icon = "🔴"
        elif status in ("WARNING", "COMM_LOST", "COMMS_LOST"):
            icon = "🟡"
        elif status == "OFFLINE":
            icon = "⚫"
        else:
            icon = "🟢"
        detail = ""
        if pr is not None:
            detail += f" PR:{float(pr):.0f}%"
        if ac is not None:
            detail += f" {float(ac)/1000:.0f}kW"
        by_tx.setdefault(tx, []).append(f"{icon} `{name}`{detail}")
    for tx in ["TX1", "TX2", "TX3"]:
        if by_tx.get(tx):
            lines.append(f"*{tx}:*")
            lines.extend(by_tx[tx])
            lines.append("")
    return "\n".join(lines)


def build_inverter_detail(data: dict, name: str) -> str:
    inv_health = data.get("inverter_health", {})
    key = next((k for k in inv_health if k.upper() == name.upper()), None)
    if not key:
        return f"⚠️ Inverter `{name}` not found. Check the name (e.g. TX1-03)."
    h      = inv_health[key]
    status = str(h.get("status", "ok")).upper()
    lines  = [
        f"🔌 *Inverter {key}*",
        "━━━━━━━━━━━━━━━━━━━",
        f"*Status:*  {status}",
    ]
    fields = [
        ("pr_v",   "PR",          "%",   1),
        ("ac_v",   "AC Power",    "W",   0),
        ("dc_v",   "DC Current",  "A",   2),
        ("temp_v", "Temperature", "°C",  1),
        ("iso_v",  "Isolation",   "kΩ",  1),
    ]
    for key_f, label, unit, dec in fields:
        v = h.get(key_f)
        if v is not None:
            lines.append(f"*{label}:*  {float(v):.{dec}f} {unit}")

    alerts = [
        a for a in data.get("active_anomalies", [])
        if isinstance(a, dict) and a.get("inverter", "").upper() == key.upper()
    ]
    if alerts:
        lines += ["", f"🚨 *Active faults ({len(alerts)}):*"]
        for a in alerts:
            lines.append(f" • {a.get('rule', 'Anomaly')}")
    else:
        lines.append("\n✅ No active faults")
    return "\n".join(lines)


def build_peak_message(data: dict) -> str:
    h = data.get("macro_health", {})
    peak_mw   = h.get("peak_power_mw")
    peak_time = h.get("peak_time", "")
    cur_mw    = float(h.get("total_ac_power_mw") or 0.0)

    # Fallback: use current power as a lower-bound estimate of today's peak
    if not peak_mw and cur_mw:
        peak_mw   = cur_mw
        peak_time = h.get("last_sync", "")

    try:
        peak_fmt = datetime.fromisoformat(peak_time).strftime("%H:%M") if peak_time else "—"
    except Exception:
        peak_fmt = peak_time or "—"

    rated = 12.6
    lines = [
        "⚡ *Peak Power — Today*",
        "━━━━━━━━━━━━━━━━━━━",
        f"🏆 *Peak:*    {peak_mw:.2f} MW" if peak_mw else "🏆 *Peak:*    N/A",
        f"⏰ *At:*      {peak_fmt}",
        f"📡 *Now:*     {cur_mw:.2f} MW",
    ]
    if peak_mw:
        cf = (peak_mw / rated) * 100
        lines.append(f"📊 *Capacity:* {cf:.1f}% of {rated} MWp")
    return "\n".join(lines)


def build_compare_message(data: dict) -> str:
    inv_health = data.get("inverter_health", {})
    tx_stats: dict[str, dict] = {}
    for name, h in inv_health.items():
        tx = _tx_of(name)
        if tx == "OTHER": continue
        if tx not in tx_stats:
            tx_stats[tx] = {"power": 0.0, "pr_vals": [], "count": 0, "faults": 0}
        ac = float(h.get("ac_v") or 0)
        pr = h.get("pr_v")
        tx_stats[tx]["power"] += ac
        if pr is not None:
            tx_stats[tx]["pr_vals"].append(float(pr))
        tx_stats[tx]["count"] += 1
        if str(h.get("status", "")).upper() in ("CRITICAL", "TRIPPED", "FAULT"):
            tx_stats[tx]["faults"] += 1

    lines = ["📊 *Transformer Comparison*", "━━━━━━━━━━━━━━━━━━━", ""]
    for tx in ["TX1", "TX2", "TX3"]:
        s = tx_stats.get(tx, {})
        mw  = s.get("power", 0) / 1e6
        pr  = sum(s.get("pr_vals", [])) / len(s["pr_vals"]) if s.get("pr_vals") else 0
        cnt = s.get("count", 0)
        flt = s.get("faults", 0)
        lines.append(
            f"*{tx}* ({cnt} inv)\n"
            f"  ⚡ {mw:.2f} MW  📊 PR:{pr:.0f}%"
            + (f"  🔴 {flt} fault(s)" if flt else "")
        )
        lines.append("")
    return "\n".join(lines)


def build_pr_message(data: dict) -> str:
    inv_health = data.get("inverter_health", {})
    macro_pr   = data.get("macro_health", {}).get("avg_pr", 0.0)
    tx_pr: dict[str, list] = {}
    for name, h in inv_health.items():
        tx = _tx_of(name)
        pr = h.get("pr_v")
        if pr is not None:
            tx_pr.setdefault(tx, []).append(float(pr))
    lines = [
        "📈 *Performance Ratio*",
        "━━━━━━━━━━━━━━━━━━━",
        f"🏭 *Plant avg PR:* {macro_pr:.1f}%",
        "",
    ]
    for tx in ["TX1", "TX2", "TX3"]:
        vals = tx_pr.get(tx, [])
        if not vals: continue
        avg  = sum(vals) / len(vals)
        low  = min(vals)
        high = max(vals)
        icon = "🟢" if avg >= 80 else ("🟡" if avg >= 70 else "🔴")
        lines.append(f"{icon} *{tx}:* avg {avg:.1f}%  (min {low:.0f}% / max {high:.0f}%)")

    lines += ["", "_PR < 75%: inspection recommended_"]
    return "\n".join(lines)


def build_energy_message() -> str:
    try:
        days_30 = get_snapshots_for_days(30)
        days_7  = get_snapshots_for_days(7)
        today_snap = get_latest_dashboard_json()

        def _mwh(snap):
            return float(snap.get("macro_health", {}).get("total_energy_mwh") or 0)

        today_mwh = _mwh(today_snap) if today_snap else 0.0
        week_mwh  = sum(_mwh(s) for _, s in days_7)
        month_mwh = sum(_mwh(s) for _, s in days_30)

        rated_kwp = 12625.0
        lines = [
            "🔋 *Energy Summary*",
            "━━━━━━━━━━━━━━━━━━━",
            f"📅 *Today:*        {today_mwh:.1f} MWh",
            f"📆 *Last 7 days:*  {week_mwh:.1f} MWh",
            f"🗓  *Last 30 days:* {month_mwh:.1f} MWh",
        ]
        if month_mwh and rated_kwp:
            spec_yield = (month_mwh * 1000) / rated_kwp
            lines.append(f"📊 *Specific yield (30d):* {spec_yield:.1f} kWh/kWp")
        if week_mwh:
            avg_daily = week_mwh / max(len(days_7), 1)
            lines.append(f"📉 *Avg daily (7d):* {avg_daily:.1f} MWh")
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"build_energy_message error: {e}")
        return "⚠️ Energy data temporarily unavailable."


def build_weather_message(data: dict) -> str:
    h   = data.get("macro_health", {})
    inv = data.get("inverter_health", {})
    poa = h.get("poa", h.get("avg_irradiance", 0.0))
    temps = [float(v.get("temp_v") or 0) for v in inv.values() if v.get("temp_v") is not None]
    lines = [
        "🌤 *Environmental Data*",
        "━━━━━━━━━━━━━━━━━━━",
        f"☀️  *Irradiance:*  {poa:.0f} W/m²",
    ]
    if temps:
        lines += [
            f"🌡  *Inv Temp avg:* {sum(temps)/len(temps):.1f}°C",
            f"🌡  *Inv Temp max:* {max(temps):.1f}°C",
            f"🌡  *Inv Temp min:* {min(temps):.1f}°C",
        ]
    if poa >= 800:
        lines.append("\n☀️ _High irradiance — expect peak production_")
    elif poa >= 400:
        lines.append("\n⛅ _Moderate irradiance — partial production_")
    elif poa > 0:
        lines.append("\n🌥 _Low irradiance — reduced output expected_")
    else:
        lines.append("\n🌙 _No irradiance — night mode_")
    return "\n".join(lines)


def build_uptime_message(data: dict) -> str:
    inv_health = data.get("inverter_health", {})
    total   = len(inv_health)
    online  = sum(1 for h in inv_health.values()
                  if str(h.get("status", "ok")).upper() not in
                  ("CRITICAL", "TRIPPED", "FAULT", "OFFLINE", "COMM_LOST", "COMMS_LOST"))
    uptime  = (online / total * 100) if total else 0
    tripped = [k for k, h in inv_health.items()
               if str(h.get("status", "")).upper() in ("CRITICAL", "TRIPPED", "FAULT")]
    comm    = [k for k, h in inv_health.items()
               if str(h.get("status", "")).upper() in ("COMM_LOST", "COMMS_LOST")]
    icon    = "🟢" if uptime >= 95 else ("🟡" if uptime >= 80 else "🔴")
    lines   = [
        "⏱ *Plant Uptime — Today*",
        "━━━━━━━━━━━━━━━━━━━",
        f"{icon} *Uptime:* {uptime:.1f}%",
        f"🟢 Online: {online}/{total}",
    ]
    if tripped:
        lines.append(f"🔴 Tripped: {', '.join(sorted(tripped)[:6])}")
    if comm:
        lines.append(f"🔇 Comms lost: {', '.join(sorted(comm)[:6])}")
    return "\n".join(lines)


HELP_TEXT = (
    "🌞 *Mazara 01 — Command Reference*\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "*Plant Status*\n"
    "📊 /status — Live power, PR & health summary\n"
    "🚨 /alerts — Active faults & anomalies\n"
    "⏱ /uptime — Plant availability % today\n"
    "🌤 /weather — Irradiance & temperature sensors\n\n"
    "*Production*\n"
    "📅 /daily — Today's energy report\n"
    "📆 /week — 7-day production history\n"
    "🔋 /energy — Monthly & 30-day energy totals\n"
    "⚡ /peak — Today's peak power & time\n\n"
    "*Inverter Analysis*\n"
    "🔌 /inverters — All 36 inverters health matrix\n"
    "🔍 /inverter TX1-03 — Single inverter deep-dive\n"
    "📊 /compare — TX1 vs TX2 vs TX3 comparison\n"
    "📈 /pr — Performance Ratio by transformer\n\n"
    "*Operations*\n"
    "🎫 /generate\\_ticket — Create Odoo fault ticket\n"
    "/approve <id> — Approve a pending action\n"
    "/deny <id>    — Deny a pending action\n\n"
    "*AI Assistant*\n"
    "💬 /ai — Ask a question (or just type freely)\n"
    "Examples:\n"
    "• `Which inverters are above 60°C?`\n"
    "• `Compare yesterday vs today production`\n"
    "• `Any DC string anomalies on TX2?`\n"
    "• `What caused the drop at 14:00?`"
)


# ---------------------------------------------------------------------------
# Telegram API
# ---------------------------------------------------------------------------

class TelegramBot:
    def __init__(self, token: str):
        self.token  = token
        self.base   = f"https://api.telegram.org/bot{token}"
        self.offset = 0

    def get_updates(self) -> tuple[bool, list]:
        try:
            r = requests.get(
                f"{self.base}/getUpdates",
                params={"offset": self.offset, "timeout": 5},
                timeout=15,
            )
            if not r.ok:
                logger.error(f"Telegram error: {r.text}")
                if r.status_code == 409:
                    return False, []
            return r.ok, (r.json().get("result", []) if r.ok else [])
        except Exception as e:
            logger.error(f"Error fetching updates: {e}")
            return False, []

    def send_message(self, chat_id, text: str, markdown: bool = True) -> int | None:
        payload = {"chat_id": chat_id, "text": text}
        if markdown:
            payload["parse_mode"] = "Markdown"
        preview = text.replace("\n", " ").strip()[:120]
        logger.info(f"[REPLY -> {chat_id}] {preview}")
        try:
            resp = requests.post(f"{self.base}/sendMessage", json=payload, timeout=API_TIMEOUT)
            if not resp.ok and markdown:
                payload.pop("parse_mode")
                resp = requests.post(f"{self.base}/sendMessage", json=payload, timeout=API_TIMEOUT)
            if resp.ok:
                return resp.json().get("result", {}).get("message_id")
        except Exception as e:
            logger.warning(f"Failed to send: {e}")
        return None

    def set_my_commands(self) -> None:
        commands = [
            {"command": "start",           "description": "Welcome & overview"},
            {"command": "status",          "description": "📊 Live power, PR & health"},
            {"command": "alerts",          "description": "🚨 Active faults"},
            {"command": "daily",           "description": "📅 Daily energy report"},
            {"command": "week",            "description": "📆 7-day production history"},
            {"command": "inverters",       "description": "🔌 All inverters health matrix"},
            {"command": "compare",         "description": "📊 TX1 vs TX2 vs TX3"},
            {"command": "pr",              "description": "📈 PR breakdown by transformer"},
            {"command": "energy",          "description": "🔋 Monthly energy totals"},
            {"command": "peak",            "description": "⚡ Today's peak power"},
            {"command": "weather",         "description": "🌤 Irradiance & temperature"},
            {"command": "uptime",          "description": "⏱ Plant availability today"},
            {"command": "generate_ticket", "description": "🎫 Create fault ticket"},
            {"command": "ai",              "description": "💬 Ask AI a question"},
            {"command": "help",            "description": "📋 Full command list"},
        ]
        try:
            requests.post(f"{self.base}/setMyCommands", json={"commands": commands}, timeout=API_TIMEOUT)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _dispatch_ai(bot, chat_id, question: str, data, settings, ai_semaphore):
    """Run LLM in a thread and reply."""
    if not llm_agent:
        bot.send_message(chat_id, "❌ AI agent not available.")
        return

    if not ai_semaphore.acquire(blocking=False):
        bot.send_message(chat_id, "⚠️ AI is busy. Try again in a moment.")
        return

    personal_id = str(settings.get("telegram", {}).get("personal_id", ""))

    def _run():
        try:
            reply = llm_agent.ask_llm(question, data, user_id=f"TG_{chat_id}")
            if reply.startswith("⚠️ Technical Error") or reply.startswith("⚠️ AI Agent error"):
                bot.send_message(chat_id, "⚠️ Error during AI processing. Admin notified.")
                if personal_id:
                    bot.send_message(personal_id, f"AI Error in chat {chat_id}:\n{reply}")
            else:
                bot.send_message(chat_id, reply)
        except Exception as e:
            logger.error(f"AI thread error: {e}")
            bot.send_message(chat_id, "⚠️ AI failed to respond.")
            if personal_id:
                bot.send_message(personal_id, f"AI Exception {chat_id}: {str(e)[:200]}")
        finally:
            ai_semaphore.release()

    threading.Thread(target=_run, daemon=True).start()


def main() -> None:
    logger.info("Bot starting...")
    settings = load_settings()
    tg = settings.get("telegram", {})
    if not tg.get("enabled"):
        return

    bot = TelegramBot(tg.get("bot_token", ""))
    bot.set_my_commands()

    ai_semaphore        = threading.Semaphore(20)
    consecutive_errors  = 0

    while True:
        try:
            settings = load_settings()
            tg = settings.get("telegram", {})
            if not tg.get("enabled"):
                time.sleep(30)
                continue

            ALLOWED_IDS = {str(tg.get("chat_id")), str(tg.get("personal_id"))}
            for tid in tg.get("trusted_ids", []):
                ALLOWED_IDS.add(str(tid))

            success, updates = bot.get_updates()
            if not success:
                consecutive_errors += 1
                if consecutive_errors >= 5:
                    logger.critical("5 consecutive Telegram conflicts — exiting.")
                    sys.exit(409)
                backoff = min(300, 10 * (2 ** consecutive_errors))
                logger.warning(f"Telegram conflict/error. Sleeping {backoff}s.")
                time.sleep(backoff)
                continue

            consecutive_errors = 0

            for update in updates:
                bot.offset = update["update_id"] + 1
                msg = update.get("message") or update.get("channel_post")
                if not msg:
                    continue
                chat_id = msg.get("chat", {}).get("id")
                text    = (msg.get("text", "") or "").strip()
                if not text:
                    continue
                if str(chat_id) not in ALLOWED_IDS:
                    logger.warning(f"Unauthorized: {chat_id}")
                    continue

                cmd = text.lower().split()[0] if text.startswith("/") else ""

                # Ticket flow takes priority
                if chat_id in ticket_sessions:
                    _handle_ticket_step(bot, chat_id, text)
                    continue

                # Admin approval
                if cmd in ("/approve", "/deny"):
                    parts = text.strip().split()
                    if len(parts) >= 2:
                        action, req_id = parts[0].lstrip("/"), parts[1]
                        try:
                            from db.doctor import _load_approvals, _save_approvals
                            approvals = _load_approvals()
                            if req_id in approvals:
                                approvals[req_id]["status"]   = "approved" if action == "approve" else "denied"
                                approvals[req_id]["acted_by"] = str(chat_id)
                                approvals[req_id]["acted_at"] = datetime.utcnow().isoformat(timespec="seconds")
                                _save_approvals(approvals)
                                bot.send_message(chat_id, f"Request `{req_id}` → {approvals[req_id]['status']}.")
                            else:
                                bot.send_message(chat_id, f"Request `{req_id}` not found.")
                        except Exception as e:
                            bot.send_message(chat_id, f"Approval error: {e}")
                    else:
                        bot.send_message(chat_id, "Usage: `/approve <id>` or `/deny <id>`")
                    continue

                # ── Commands ──────────────────────────────────────────────
                try:
                    if cmd in ("/start", "/help"):
                        bot.send_message(chat_id, HELP_TEXT)

                    elif cmd == "/status":
                        data = get_latest_dashboard_json()
                        bot.send_message(chat_id, build_status_message(data) if data else "⚠️ No data.")

                    elif cmd == "/alerts":
                        data = get_latest_dashboard_json()
                        bot.send_message(chat_id, build_alerts_message(data) if data else "⚠️ No data.")

                    elif cmd == "/daily":
                        data = get_latest_dashboard_json()
                        bot.send_message(chat_id, build_daily_message(data) if data else "⚠️ No data.")

                    elif cmd == "/week":
                        bot.send_message(chat_id, build_weekly_message())

                    elif cmd == "/inverters":
                        data = get_latest_dashboard_json()
                        bot.send_message(chat_id, build_inverters_message(data) if data else "⚠️ No data.")

                    elif cmd == "/inverter":
                        parts = text.split(maxsplit=1)
                        if len(parts) < 2:
                            bot.send_message(chat_id, "Usage: `/inverter TX1-03`")
                        else:
                            data = get_latest_dashboard_json()
                            bot.send_message(
                                chat_id,
                                build_inverter_detail(data, parts[1].strip()) if data else "⚠️ No data.",
                            )

                    elif cmd == "/peak":
                        data = get_latest_dashboard_json()
                        bot.send_message(chat_id, build_peak_message(data) if data else "⚠️ No data.")

                    elif cmd == "/compare":
                        data = get_latest_dashboard_json()
                        bot.send_message(chat_id, build_compare_message(data) if data else "⚠️ No data.")

                    elif cmd == "/pr":
                        data = get_latest_dashboard_json()
                        bot.send_message(chat_id, build_pr_message(data) if data else "⚠️ No data.")

                    elif cmd == "/energy":
                        bot.send_message(chat_id, build_energy_message())

                    elif cmd == "/weather":
                        data = get_latest_dashboard_json()
                        bot.send_message(chat_id, build_weather_message(data) if data else "⚠️ No data.")

                    elif cmd == "/uptime":
                        data = get_latest_dashboard_json()
                        bot.send_message(chat_id, build_uptime_message(data) if data else "⚠️ No data.")

                    elif cmd == "/generate_ticket":
                        start_ticket_flow(bot, chat_id)

                    elif cmd == "/cancel":
                        bot.send_message(chat_id, "ℹ️ Nessuna operazione attiva.")

                    elif cmd == "/ai":
                        question = text[3:].strip() if len(text) > 3 else text
                        if not question:
                            bot.send_message(chat_id, "💬 Usage: `/ai <your question>`\nOr just type your question directly.")
                        else:
                            bot.send_message(chat_id, "⏳ _Thinking..._")
                            data = get_latest_dashboard_json()
                            _dispatch_ai(bot, chat_id, question, data, settings, ai_semaphore)

                    else:
                        # ── Any free text → LLM ───────────────────────────────
                        bot.send_message(chat_id, "⏳ _Thinking..._")
                        data = get_latest_dashboard_json()
                        _dispatch_ai(bot, chat_id, text, data, settings, ai_semaphore)

                except Exception as cmd_err:
                    logger.error(f"Command error [{cmd!r}]: {cmd_err}", exc_info=True)
                    try:
                        bot.send_message(chat_id, f"⚠️ Error processing `{cmd or 'message'}`. Check logs.", markdown=False)
                    except Exception:
                        pass

        except KeyboardInterrupt:
            logger.info("Bot stopping — KeyboardInterrupt.")
            break
        except Exception as e:
            logger.error(f"Loop error: {e}")
            try:
                time.sleep(5)
            except KeyboardInterrupt:
                break

        try:
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
