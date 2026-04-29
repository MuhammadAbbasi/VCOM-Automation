"""
telegram_bot.py — Mazara 01 Solar Plant Monitoring Bot

Real-time SCADA intelligence delivered via Telegram.
Polls the Telegram Bot API for incoming commands and responds
with live plant data from the latest dashboard snapshot.

Commands:
  /status   — Live AC power, PR, inverter health
  /alerts   — Active anomalies and fault conditions
  /daily    — Daily energy production summary
  /ai <q>   — Natural language plant analysis (Qwen 2.5 + Pre-computed Data Engine)

Launched automatically by run_monitor.py.
"""

import json
import logging
import sys
import time
from datetime import datetime
import threading
from pathlib import Path
import requests
from logging.handlers import RotatingFileHandler

# Ensure UTF-8 for console output on Windows
if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        except Exception:
            pass

# Pre-import AI agent to avoid startup lag in the loop
try:
    import llm_agent
except ImportError:
    llm_agent = None

# ---------------------------------------------------------------------------
# Paths & Config
# ---------------------------------------------------------------------------
ROOT       = Path(__file__).resolve().parent
DATA_DIR   = ROOT / "extracted_data"
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

# ---------------------------------------------------------------------------
# /generate_ticket — interactive ticket creation flow
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
    "1️⃣ INVERTER TRIP\n"
    "2️⃣ LOW PR\n"
    "3️⃣ CRIT PR\n"
    "4️⃣ ISO FAULT\n"
    "5️⃣ COMM LOST\n"
    "6️⃣ DC MPPT FAULT\n"
    "7️⃣ HIGH TEMP\n"
    "8️⃣ CRIT TEMP\n"
    "9️⃣ TRACKER OFFLINE\n"
    "🔟 GRID LIMIT CHANGE\n"
    "1️⃣1️⃣ CUSTOM\n\n"
    "Rispondi con 1–11 oppure /cancel"
)

_PRIO_MENU = (
    "⭐ *Priorità:*\n"
    "1️⃣ Bassa   2️⃣ Normale   3️⃣ Alta   4️⃣ Urgente\n\n"
    "Rispondi con 1–4 oppure `ok` per usare il default"
)

_INTV_MENU = (
    "🔧 *Tipo Intervento:*\n"
    "1️⃣ Manutenzione Ordinaria\n"
    "2️⃣ Manutenzione Straordinaria\n"
    "3️⃣ Guasto / Riparazione\n"
    "4️⃣ Ispezione\n"
    "5️⃣ Sfalcio / Pulizia\n"
    "6️⃣ Collaudo\n"
    "7️⃣ Altro\n\n"
    "Rispondi con 1–7 oppure `ok` per usare il default"
)

# ticket_sessions[chat_id] = {"step": str, "data": dict}
ticket_sessions: dict = {}


def _create_odoo_ticket(data: dict):
    """Create SCADA session + anomalia + intervento in Odoo. Returns (name, intv_id, anom_id)."""
    from db.odoo_client import OdooClient
    client = OdooClient("http://localhost:8069", "odoo", "pietro.artale@gmail.com", "odoo")
    if not client.login():
        raise RuntimeError("Odoo login failed")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    label  = data["fault_label"]
    device = data["device"]
    titolo = f"[{label}] {device} — Mazara 01"
    causa  = "\n".join([
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

    session_id  = client.create_scada_session(
        fault_summary=f"Telegram ticket: {label} on {device}",
        stato_impianto="alarm" if data["intv_prio"] == "urgente" else "warning",
    )
    anomalia_id = client.create_anomalia(
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
    """
    Handle one step of the /generate_ticket flow.
    Returns True if the message was consumed by the flow, False otherwise.
    """
    session = ticket_sessions.get(chat_id)
    if not session:
        return False

    step = session["step"]
    data = session["data"]
    t    = text.strip()

    # /cancel anywhere aborts
    if t.lower() in ("/cancel", "cancel"):
        ticket_sessions.pop(chat_id, None)
        bot.send_message(chat_id, "❌ Ticket annullato.")
        return True

    # ── Step: fault_type ────────────────────────────────────────
    if step == "fault_type":
        if t not in FAULT_TYPES_TG:
            bot.send_message(chat_id, "⚠️ Scelta non valida. Rispondi con 1–11 oppure /cancel")
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
            f"✅ Fault: *{label}*\n\n"
            "🔌 *Quale dispositivo è interessato?*\n"
            "Scrivi il nome (es. `TX1-03`, `TX2-11`, `PLANT`, `GRID`)\n\n"
            "oppure /cancel")
        return True

    # ── Step: custom_name ───────────────────────────────────────
    if step == "custom_name":
        if not t or t.startswith("/"):
            bot.send_message(chat_id, "⚠️ Inserisci un nome per il guasto.")
            return True
        data["fault_label"] = t.upper()
        session["step"] = "device"
        bot.send_message(chat_id,
            f"✅ Fault: *{data['fault_label']}*\n\n"
            "🔌 *Quale dispositivo è interessato?*\n"
            "Scrivi il nome (es. `TX1-03`, `TX2-11`, `PLANT`)")
        return True

    # ── Step: device ────────────────────────────────────────────
    if step == "device":
        if not t or t.startswith("/"):
            bot.send_message(chat_id, "⚠️ Inserisci il nome del dispositivo.")
            return True
        data["device"] = t.upper()
        session["step"] = "priority"
        bot.send_message(chat_id,
            f"✅ Device: *{data['device']}*\n\n"
            f"{_PRIO_MENU}\n"
            f"Default per *{data['fault_label']}*: `{data['intv_prio'].upper()}`")
        return True

    # ── Step: priority ───────────────────────────────────────────
    if step == "priority":
        if t.lower() != "ok":
            if t not in PRIORITIES_TG:
                bot.send_message(chat_id, "⚠️ Rispondi con 1–4 oppure `ok`.")
                return True
            data["intv_prio"] = PRIORITIES_TG[t]
            # Map 'normale' to 'media' for fv.anomalia model compatibility
            data["anom_prio"] = "media" if data["intv_prio"] == "normale" else data["intv_prio"]
        session["step"] = "intv_type"
        bot.send_message(chat_id,
            f"✅ Priorità: *{data['intv_prio'].upper()}*\n\n"
            f"{_INTV_MENU}\n"
            f"Default per *{data['fault_label']}*: `{data['intv_tipo']}`")
        return True

    # ── Step: intv_type ──────────────────────────────────────────
    if step == "intv_type":
        if t.lower() != "ok":
            if t not in INTERVENTION_TYPES_TG:
                bot.send_message(chat_id, "⚠️ Rispondi con 1–7 oppure `ok`.")
                return True
            data["intv_tipo"] = INTERVENTION_TYPES_TG[t]
        session["step"] = "description"
        bot.send_message(chat_id,
            f"✅ Tipo: *{data['intv_tipo']}*\n\n"
            "📋 *Descrivi il guasto:*\n"
            "Invia la descrizione in un unico messaggio\n\n"
            "oppure /cancel")
        return True

    # ── Step: description ────────────────────────────────────────
    if step == "description":
        if not t or t.startswith("/"):
            bot.send_message(chat_id, "⚠️ Inserisci una descrizione.")
            return True
        data["description"] = t
        session["step"] = "notes"
        bot.send_message(chat_id,
            "✅ Descrizione salvata.\n\n"
            "📝 *Note aggiuntive?*\n"
            "Invia le note oppure scrivi `none` per saltare")
        return True

    # ── Step: notes ──────────────────────────────────────────────
    if step == "notes":
        data["notes"] = "" if t.lower() == "none" else t
        session["step"] = "field_work"
        bot.send_message(chat_id,
            "📌 *Richiede intervento sul campo?*\n"
            "Rispondi `y` (sì) oppure `n` (no)")
        return True

    # ── Step: field_work ─────────────────────────────────────────
    if step == "field_work":
        if t.lower() not in ("y", "n", "yes", "no", "si", "sì"):
            bot.send_message(chat_id, "⚠️ Rispondi con `y` oppure `n`.")
            return True
        data["field_work"] = t.lower() in ("y", "yes", "si", "sì")
        session["step"] = "confirm"
        desc_preview = data["description"][:120] + ("…" if len(data["description"]) > 120 else "")
        preview = (
            f"📋 *ANTEPRIMA TICKET*\n"
            f"{'─'*32}\n"
            f"*Tipo:*       {data['fault_label']}\n"
            f"*Device:*     {data['device']}\n"
            f"*Priorità:*   {data['intv_prio'].upper()}\n"
            f"*Intervento:* {data['intv_tipo']}\n"
            f"*Campo:*      {'Sì' if data['field_work'] else 'No'}\n"
            f"*Descrizione:* {desc_preview}\n"
        )
        if data.get("notes"):
            preview += f"*Note:* {data['notes'][:80]}\n"
        preview += "\n✅ Confermi la creazione? (`y` / `n`)"
        bot.send_message(chat_id, preview)
        return True

    # ── Step: confirm ────────────────────────────────────────────
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
                f"*Stato:*     Nuovo — in attesa di assegnazione admin\n\n"
                f"📍 http://localhost:8069/odoo/fv-interventi"
            )
        except Exception as e:
            logger.error(f"Ticket creation error: {e}")
            bot.send_message(chat_id, f"❌ Errore nella creazione: {str(e)[:120]}")
        return True

    return False


def start_ticket_flow(bot, chat_id: int) -> None:
    ticket_sessions[chat_id] = {"step": "fault_type", "data": {}}
    bot.send_message(chat_id, _FAULT_MENU)


TRIGGER_KEYWORDS = [
    "/status", "status", "plant status", "stato", "potenza",
    "stato impianto", "stato dell'impianto", "/potenza",
    "how is the plant", "plant ok", "update",
]

POLL_INTERVAL = 4   # Slightly faster polling
API_TIMEOUT   = 10

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_settings() -> dict:
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def get_latest_dashboard_json() -> dict | None:
    """Retrieve the latest analysis snapshot and trackers from the database."""
    try:
        from db.db_manager import load_latest_snapshot, get_all_tracker_status
        today = datetime.now().strftime("%Y-%m-%d")
        latest_data = load_latest_snapshot(today)
        
        # Fallback: check recent dates if today is empty
        if not latest_data:
            from db.db_manager import _get_data_conn
            conn = _get_data_conn()
            res = conn.execute("SELECT date FROM analysis_snapshots ORDER BY date DESC, timestamp DESC LIMIT 1").fetchone()
            if res:
                latest_data = load_latest_snapshot(res[0])
        
        if latest_data:
            # Inject tracker status for AI context
            latest_data["trackers"] = get_all_tracker_status()
            return latest_data
            
    except Exception as e:
        logger.warning(f"Database snapshot read failed: {e}")
    return None

def build_status_message(data: dict) -> str:
    macro = data.get("macro_health", {})
    last_sync_iso = macro.get("last_sync", "")
    try:
        if last_sync_iso:
            sync_time = datetime.fromisoformat(last_sync_iso.replace("Z", "+00:00")).strftime("%H:%M")
        else:
            sync_time = datetime.now().strftime("%H:%M")
    except Exception:
        sync_time = datetime.now().strftime("%H:%M")

    lines = [f"🌞 *Mazara 01 — Status* ({sync_time})"]
    inv_health = data.get("inverter_health", {})
    
    # Use precomputed macro stats if available, otherwise fallback
    total_mw = macro.get("total_ac_power_mw", 0.0)
    avg_pr = macro.get("avg_pr", 0.0)
    
    if not total_mw:
        total_w = sum(float(h.get("ac_v", 0) or 0) for h in inv_health.values())
        total_mw = total_w / 1_000_000.0
    
    lines.append(f"⚡ AC: {total_mw:.2f} MW")
    lines.append(f"🟢 {macro.get('online', '—')} | 🔴 {macro.get('tripped', '—')} | 🔇 {macro.get('comms_lost', '—')}")

    if not avg_pr:
        pr_vals = [float(h.get("pr_v", 0)) for h in inv_health.values() if h.get("pr_v") is not None]
        avg_pr = sum(pr_vals)/len(pr_vals) if pr_vals else 0.0
    
    lines.append(f"📊 AVG PR: {avg_pr:.1f}%")

    alerts = data.get("active_anomalies", [])
    if alerts:
        lines.append(f"\n🚨 *Alerts ({len(alerts)}):*")
        for a in alerts[:3]:
            lines.append(f" • {a.get('inverter')} - {a.get('type')}")
    else:
        lines.append("\n✅ No active alerts")
    
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Telegram API
# ---------------------------------------------------------------------------

class TelegramBot:
    def __init__(self, token: str):
        self.token = token
        self.base = f"https://api.telegram.org/bot{token}"
        self.offset = 0

    def get_updates(self) -> list:
        try:
            r = requests.get(f"{self.base}/getUpdates", params={"offset": self.offset, "timeout": 5}, timeout=15)
            # Log error if not OK
            if not r.ok: logger.error(f"Telegram error: {r.text}")
            return r.json().get("result", []) if r.ok else []
        except Exception: return []

    def delete_message(self, chat_id: str | int, message_id: int) -> None:
        try:
            requests.post(f"{self.base}/deleteMessage", json={"chat_id": chat_id, "message_id": message_id}, timeout=API_TIMEOUT)
        except Exception: pass

    def send_message(self, chat_id: str | int, text: str, markdown: bool = True, force_reply: bool = False) -> int | None:
        payload = {"chat_id": chat_id, "text": text}
        if markdown: payload["parse_mode"] = "Markdown"
        if force_reply:
            payload["reply_markup"] = {"force_reply": True, "selective": True}
            
        try:
            resp = requests.post(f"{self.base}/sendMessage", json=payload, timeout=API_TIMEOUT)
            if not resp.ok and markdown:
                # Retry without markdown if it failed (parsing errors)
                payload.pop("parse_mode")
                resp = requests.post(f"{self.base}/sendMessage", json=payload, timeout=API_TIMEOUT)
            
            if resp.ok:
                return resp.json().get("result", {}).get("message_id")
        except Exception as e:
            logger.warning(f"Failed to send: {e}")
        return None

    def is_trigger(self, text: str) -> bool:
        lower = text.strip().lower()
        return any(kw in lower for kw in TRIGGER_KEYWORDS)

    def set_my_commands(self) -> None:
        """Sets the bot's command list for the '/' menu."""
        commands = [
            {"command": "start",           "description": "Welcome & capabilities overview"},
            {"command": "status",          "description": "📊 Live plant power, PR & health"},
            {"command": "alerts",          "description": "🚨 Active faults & anomalies"},
            {"command": "daily",           "description": "📅 Daily energy production report"},
            {"command": "ai",              "description": "🧠 AI-powered plant analysis"},
            {"command": "generate_ticket", "description": "🎫 Crea nuovo ticket di guasto"},
        ]
        try:
            requests.post(f"{self.base}/setMyCommands", json={"commands": commands}, timeout=API_TIMEOUT)
        except Exception: pass

    def set_chat_description(self, chat_id: str | int) -> None:
        """Updates the Telegram group/chat description."""
        desc = (
            "🌞 Mazara 01 — Solar Plant Intelligence\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "12.6 MWp | 36 Inverters | 3 Transformers\n"
            "Mazara del Vallo, Sicily\n\n"
            "📊 /status — Real-time power & health\n"
            "🚨 /alerts — Active anomalies\n"
            "📅 /daily — Energy production report\n"
            "🧠 /ai <question> — AI plant analysis\n\n"
            "Powered by VCOM Automation"
        )
        try:
            requests.post(f"{self.base}/setChatDescription", json={"chat_id": chat_id, "description": desc[:512]}, timeout=API_TIMEOUT)
        except Exception: pass

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Bot starting...")
    settings = load_settings()
    tg = settings.get("telegram", {})
    if not tg.get("enabled"): return

    bot = TelegramBot(tg.get("bot_token", ""))
    
    # Initialize UI/Metadata
    bot.set_my_commands()
    if tg.get("chat_id"):
        bot.set_chat_description(tg.get("chat_id"))
    
    ai_semaphore = threading.Semaphore(20)
    
    # State for handling /ai question waiting
    waiting_for_ai = {} # {chat_id: {"prompt_msg_id": int}}
    
    while True:
        try:
            # Reload settings dynamically to pick up new authorized IDs
            settings = load_settings()
            tg = settings.get("telegram", {})
            if not tg.get("enabled"): 
                time.sleep(30)
                continue
                
            ALLOWED_IDS = [str(tg.get("chat_id")), str(tg.get("personal_id"))]
            for tid in tg.get("trusted_ids", []):
                ALLOWED_IDS.append(str(tid))

            updates = bot.get_updates()
            for update in updates:
                bot.offset = update["update_id"] + 1
                msg = update.get("message") or update.get("channel_post")
                if not msg: continue
                
                chat_id = msg.get("chat", {}).get("id")
                text = msg.get("text", "")
                if not text: continue
                
                # Check whitelist
                sender_id = str(chat_id)
                if sender_id not in ALLOWED_IDS:
                    logger.warning(f"Unauthorized access attempt from chat_id {chat_id}")
                    continue
                
                # Special response for the new trusted user as requested
                if sender_id == "8222569154":
                    bot.send_message(chat_id, "8222569154")

                # Active ticket-creation session takes priority over all other routing
                if chat_id in ticket_sessions:
                    _handle_ticket_step(bot, chat_id, text)
                    continue

                # Check if this is a reply to the AI prompt or if we are waiting for one
                reply_to = msg.get("reply_to_message")
                is_ai_reply = reply_to and "What would you like to know" in reply_to.get("text", "")
                is_waiting_state = chat_id in waiting_for_ai

                if text.lower().startswith("/start"):
                    # Clear waiting state on /start
                    waiting_for_ai.pop(chat_id, None)
                    bot.send_message(chat_id,
                        "🌞 *Mazara 01 — Solar Plant Intelligence*\n"
                        "━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "Welcome! I monitor your 21 MWp solar plant in real time.\n\n"
                        "*Available Commands:*\n"
                        "📊 /status — Live AC power, PR & inverter health\n"
                        "🚨 /alerts — Active faults & anomalies\n"
                        "📅 /daily — Today's energy production report\n"
                        "🧠 /ai `<question>` — Ask anything about the plant\n"
                        "🎫 /generate\\_ticket — Create a new fault ticket\n\n"
                        "*Example AI Questions:*\n"
                        "• `/ai What was yesterday's total production?`\n"
                        "• `/ai Which inverters are above 50°C?`\n"
                        "• `/ai Compare TX1 vs TX2 production`\n"
                        "• `/ai Any inverters offline today?`"
                    )
                    continue

                elif text.lower().startswith("/generate_ticket"):
                    waiting_for_ai.pop(chat_id, None)
                    start_ticket_flow(bot, chat_id)
                    continue

                elif text.lower() in ("/cancel", "cancel") and chat_id not in ticket_sessions:
                    bot.send_message(chat_id, "ℹ️ Nessuna operazione attiva da annullare.")
                    continue

                elif text.lower().startswith("/ai") or is_ai_reply or is_waiting_state:
                    question = text.strip()
                    
                    if text.lower().startswith("/ai"):
                        question = text[3:].strip()
                        if not question:
                            msg_id = bot.send_message(chat_id, "🤖 *I'm listening!* What would you like to know about the plant?", force_reply=True)
                            waiting_for_ai[chat_id] = {"prompt_msg_id": msg_id}
                            continue
                    
                    # If we got here and were waiting, clear the state
                    state = waiting_for_ai.pop(chat_id, None)
                    
                    bot.send_message(chat_id, "⏳ _Thinking..._")
                    data = get_latest_dashboard_json()
                    if llm_agent:
                        if not ai_semaphore.acquire(blocking=False):
                            bot.send_message(chat_id, "⚠️ AI Agent is busy processing other requests. Please try again later.")
                            continue

                        def run_and_reply():
                            try:
                                reply = llm_agent.ask_llm(question, data, user_id=f"TG_{chat_id}")
                                bot.send_message(chat_id, reply)
                            except Exception as ai_e:
                                logger.error(f"Telegram AI Thread error: {ai_e}")
                                error_msg = "⚠️ AI Agent failed to respond (Timeout)." if "timeout" in str(ai_e).lower() else f"⚠️ AI Agent error: {str(ai_e)[:50]}"
                                bot.send_message(chat_id, error_msg)
                            finally:
                                ai_semaphore.release()

                        threading.Thread(target=run_and_reply, daemon=True).start()
                    else:
                        bot.send_message(chat_id, "❌ AI agent not found.")

                elif text.lower().startswith("/alerts") or "alert" in text.lower():
                    waiting_for_ai.pop(chat_id, None)
                    data = get_latest_dashboard_json()
                    if data and data.get("active_anomalies"):
                        alert_lines = []
                        seen = set()
                        for a in data["active_anomalies"]:
                            if not isinstance(a, dict):
                                line = f" {str(a)}"
                            else:
                                inv_name = a.get("inverter", "Unknown")
                                a_type = a.get("rule", a.get("type", "Anomaly"))
                                line = f" {inv_name} - {a_type}"
                            
                            if line not in seen:
                                seen.add(line)
                                alert_lines.append(line)
                        
                        bot.send_message(chat_id, f"🚨 *Active Alerts:*\n\n" + "\n".join(alert_lines))
                    else:
                        bot.send_message(chat_id, "✅ No active alerts.")

                elif text.lower().startswith("/daily") or "daily" in text.lower():
                    waiting_for_ai.pop(chat_id, None)
                    data = get_latest_dashboard_json()
                    if data:
                        h = data.get("macro_health", {})
                        msg = (f"📅 *Daily Report ({datetime.now().strftime('%d/%m/%Y')})*\n\n"
                               f"⚡ Power: {h.get('total_ac_power_mw', 0):.2f} MW\n"
                               f"🔋 Energy: {h.get('total_energy_mwh', 0):.2f} MWh\n"
                               f"📈 Avg PR: {h.get('avg_pr', 0):.1f}%\n"
                               f"✅ Inverters: {h.get('online', 0)}/36 Online\n"
                               f"⏰ Last Sync: {h.get('last_sync', '—')}")
                        bot.send_message(chat_id, msg)
                    else:
                        bot.send_message(chat_id, "⚠️ Data not found.")

                elif bot.is_trigger(text) or text.lower().startswith("/status"):
                    waiting_for_ai.pop(chat_id, None)
                    data = get_latest_dashboard_json()
                    bot.send_message(chat_id, build_status_message(data) if data else "⚠️ No data.")
                
                elif chat_id > 0:
                    bot.send_message(chat_id,
                        "🌞 *Mazara 01 — Commands*\n\n"
                        "📊 /status — Live plant summary\n"
                        "🚨 /alerts — Active anomalies\n"
                        "📅 /daily — Production report\n"
                        "🧠 /ai `<question>` — AI analysis\n"
                        "🎫 /generate\\_ticket — Create fault ticket\n\n"
                        "_Type /start for a full overview._"
                    )

        except Exception as e:
            logger.error(f"Loop error: {e}")
            time.sleep(5)
            
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
