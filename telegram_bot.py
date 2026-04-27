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
            {"command": "start",  "description": "Welcome & capabilities overview"},
            {"command": "status", "description": "📊 Live plant power, PR & health"},
            {"command": "alerts", "description": "🚨 Active faults & anomalies"},
            {"command": "daily",  "description": "📅 Daily energy production report"},
            {"command": "ai",     "description": "🧠 AI-powered plant analysis"},
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
    
    ALLOWED_IDS = [str(tg.get("chat_id")), str(tg.get("personal_id"))]
    ai_semaphore = threading.Semaphore(20)
    
    # State for handling /ai question waiting
    waiting_for_ai = {} # {chat_id: {"prompt_msg_id": int}}
    while True:
        try:
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
                        "🧠 /ai `<question>` — Ask anything about the plant\n\n"
                        "*Example AI Questions:*\n"
                        "• `/ai What was yesterday's total production?`\n"
                        "• `/ai Which inverters are above 50°C?`\n"
                        "• `/ai Compare TX1 vs TX2 production`\n"
                        "• `/ai Any inverters offline today?`"
                    )
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
                        "🧠 /ai `<question>` — AI analysis\n\n"
                        "_Type /start for a full overview._"
                    )

        except Exception as e:
            logger.error(f"Loop error: {e}")
            time.sleep(5)
            
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
