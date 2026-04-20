"""
telegram_bot.py — Telegram command bot for Mazara PV plant status queries.

Polls the Telegram Bot API for incoming messages every ~5 seconds.
Responds to any message containing trigger keywords with live plant data
read from the latest dashboard_data_<date>.json snapshot.

Supported triggers (case-insensitive):
  /status  |  status  |  plant status  |  stato  |  potenza

Run standalone:
    python telegram_bot.py

Launched automatically by run_monitor.py.
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
import requests

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
        logging.FileHandler(ROOT / "telegram_bot.log", encoding="utf-8"),
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
    today = datetime.now().strftime("%Y-%m-%d")
    path  = DATA_DIR / f"dashboard_data_{today}.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                full_history = json.load(f)
                if not full_history: return None
                latest_ts = sorted(full_history.keys())[-1]
                return full_history[latest_ts]
        except Exception as e:
            logger.warning(f"Failed to read dashboard JSON: {e}")
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
    
    # Corrected keys: ac_v is the power, pr_v is the percentage
    total_w = sum(float(h.get("ac_v", 0) or 0) for h in inv_health.values())
    total_mw = total_w / 1000000.0
    lines.append(f"⚡ AC: {total_mw:.2f} MW")
    lines.append(f"🟢 {macro.get('online', '—')} | 🔴 {macro.get('tripped', '—')} | 🔇 {macro.get('comms_lost', '—')}")

    # Aggregates
    pr_vals = [float(h.get("pr_v", 0)) for h in inv_health.values() if h.get("pr_v") is not None]
    if pr_vals: lines.append(f"📊 AVG PR: {sum(pr_vals)/len(pr_vals):.1f}%")

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

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Bot starting...")
    settings = load_settings()
    tg = settings.get("telegram", {})
    if not tg.get("enabled"): return

    bot = TelegramBot(tg.get("bot_token", ""))
    
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

                if bot.is_trigger(text):
                    data = get_latest_dashboard_json()
                    bot.send_message(chat_id, build_status_message(data) if data else "⚠️ No data.")
                
                elif text.lower().startswith("/ai"):
                    question = text[3:].strip()
                    if not question:
                        bot.send_message(chat_id, "🤖 Ask me after /ai")
                        continue
                    
                    bot.send_message(chat_id, "⏳ _Thinking..._")
                    data = get_latest_dashboard_json()
                    
                    if llm_agent:
                        # Robust execution
                        reply = llm_agent.ask_llm(question, data)
                        bot.send_message(chat_id, reply)
                    else:
                        bot.send_message(chat_id, "❌ AI agent not found.")
                
                elif chat_id > 0:
                    bot.send_message(chat_id, "❓ Use *status* or */ai <question>*")

        except Exception as e:
            logger.error(f"Loop error: {e}")
            time.sleep(5)
            
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
