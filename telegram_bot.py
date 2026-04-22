"""
telegram_bot.py — Telegram command bot for Mazara PV plant status queries.

Polls the Telegram Bot API for incoming messages every ~5 seconds.
Responds to any message containing trigger keywords with live plant data
read from the latest dashboard_data_<date>.json snapshot.

Supported triggers:
  /status  |  status           -> Live plant summary
  /ai <question>               -> Remote AI Forensic Analysis (Qwen 3.5)

Run standalone:
    python telegram_bot.py

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
    # Try multiple times in case of file lock
    for _ in range(3):
        try:
            # 1. Try today's file first
            today = datetime.now().strftime("%Y-%m-%d")
            path = DATA_DIR / f"dashboard_data_{today}.json"
            
            # 2. Fallback to any dashboard file if today's is missing
            if not path.exists():
                json_files = sorted(DATA_DIR.glob("dashboard_data_*.json"))
                if not json_files: return None
                path = json_files[-1]

            with open(path, "r", encoding="utf-8") as f:
                full_history = json.load(f)
                if not full_history: 
                    time.sleep(0.5)
                    continue
                latest_ts = sorted(full_history.keys())[-1]
                return full_history[latest_ts]
        except Exception as e:
            logger.warning(f"Dashboard JSON read attempt failed: {e}")
            time.sleep(0.5)
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

    def send_message(self, chat_id: str | int, text: str, markdown: bool = True) -> None:
        payload = {"chat_id": chat_id, "text": text}
        if markdown: payload["parse_mode"] = "Markdown"
        try:
            resp = requests.post(f"{self.base}/sendMessage", json=payload, timeout=API_TIMEOUT)
            if not resp.ok and markdown:
                # Retry without markdown if it failed (parsing errors)
                payload.pop("parse_mode")
                requests.post(f"{self.base}/sendMessage", json=payload, timeout=API_TIMEOUT)
        except Exception as e:
            logger.warning(f"Failed to send: {e}")

    def is_trigger(self, text: str) -> bool:
        lower = text.strip().lower()
        return any(kw in lower for kw in TRIGGER_KEYWORDS)

    def set_my_commands(self) -> None:
        """Sets the bot's command list for the '/' menu."""
        commands = [
            {"command": "status", "description": "Live plant summary"},
            {"command": "alerts", "description": "List all active site anomalies"},
            {"command": "daily",  "description": "Daily production report"},
            {"command": "ai",     "description": "Deep Forensic AI Analysis (add question)"},
        ]
        try:
            requests.post(f"{self.base}/setMyCommands", json={"commands": commands}, timeout=API_TIMEOUT)
        except Exception: pass

    def set_chat_description(self, chat_id: str | int) -> None:
        """Updates the Telegram group description to match bot capabilities."""
        desc = (
            "Mazara Solar Plant Forensic Intelligence. \n\n"
            "📊 /status - Real-time plant production & health\n"
            "🚨 /alerts - Active inverter & performance anomalies\n"
            "📅 /daily  - Daily consolidation report\n"
            "🤖 /ai <question> - Forensic analysis via Remote high-speed Qwen 3.5"
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

                if text.lower().startswith("/ai"):
                    question = text[3:].strip()
                    if not question:
                        bot.send_message(chat_id, "🤖 Ask me after /ai")
                        continue
                    
                    bot.send_message(chat_id, "⏳ _Thinking..._")
                    data = get_latest_dashboard_json()
                    if llm_agent:
                        if not ai_semaphore.acquire(blocking=False):
                            bot.send_message(chat_id, "⚠️ AI Agent is busy processing other requests. Please try again later.")
                            continue

                        def run_and_reply():
                            try:
                                reply = llm_agent.ask_llm(question, data)
                                bot.send_message(chat_id, reply)
                            except Exception as ai_e:
                                logger.error(f"Telegram AI Thread error: {ai_e}")
                                bot.send_message(chat_id, "⚠️ AI Agent connection failed.")
                            finally:
                                ai_semaphore.release()

                        threading.Thread(target=run_and_reply, daemon=True).start()
                    else:
                        bot.send_message(chat_id, "❌ AI agent not found.")

                elif text.lower().startswith("/alerts") or "alert" in text.lower():
                    data = get_latest_dashboard_json()
                    if data and data.get("active_anomalies"):
                        alert_lines = []
                        for a in data["active_anomalies"]:
                            if not isinstance(a, dict):
                                alert_lines.append(f"🔴 {str(a)}")
                                continue
                            inv_name = a.get("inverter", "Unknown")
                            a_type = a.get("rule", a.get("type", "Anomaly"))
                            alert_lines.append(f"🔴 {inv_name} - {a_type}")
                        
                        bot.send_message(chat_id, f"🚨 *Active Alerts:*\n\n" + "\n".join(alert_lines))
                    else:
                        bot.send_message(chat_id, "✅ No active alerts.")

                elif text.lower().startswith("/daily") or "daily" in text.lower():
                    data = get_latest_dashboard_json()
                    if data:
                        h = data.get("macro_health", {})
                        msg = (f"📅 *Daily Report ({datetime.now().strftime('%d/%m/%Y')})*\n\n"
                               f"⚡ Production: {h.get('total_ac_power_mw', 0):.2f} MW\n"
                               f"📈 Avg PR: {h.get('avg_pr', 0):.1f}%\n"
                               f"✅ Inverters Online: {h.get('online', 0)}/36\n"
                               f"⏰ Last Sync: {h.get('last_sync', '—')}")
                        bot.send_message(chat_id, msg)
                    else:
                        bot.send_message(chat_id, "⚠️ Data not found.")

                elif bot.is_trigger(text) or text.lower().startswith("/status"):
                    data = get_latest_dashboard_json()
                    bot.send_message(chat_id, build_status_message(data) if data else "⚠️ No data.")
                
                elif chat_id > 0:
                    bot.send_message(chat_id, "❓ *Mazara Bot Shortcuts:*\n\n"
                                     "📊 **/status** — Live Plant Summary\n"
                                     "🚨 **/alerts** — List Active Anomalies\n"
                                     "📅 **/daily** — Production Report\n"
                                     "🤖 **/ai <question>** — Deep Analysis")

        except Exception as e:
            logger.error(f"Loop error: {e}")
            time.sleep(5)
            
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
