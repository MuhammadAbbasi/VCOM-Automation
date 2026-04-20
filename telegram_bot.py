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

# ---------------------------------------------------------------------------
# Trigger keywords — any incoming message matching these will get a reply
# ---------------------------------------------------------------------------
TRIGGER_KEYWORDS = [
    "/status", "status", "plant status", "stato", "potenza",
    "stato impianto", "stato dell'impianto", "/potenza",
    "how is the plant", "plant ok", 
]

POLL_INTERVAL = 5   # seconds between Telegram getUpdates calls
API_TIMEOUT   = 10  # seconds for each HTTP request


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
    """Return the most recent dashboard_data_<date>.json snapshot."""
    today = datetime.now().strftime("%Y-%m-%d")
    path  = DATA_DIR / f"dashboard_data_{today}.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                full_history = json.load(f)
                if not full_history:
                    return None
                # Get the most recent timestamp key
                latest_ts = sorted(full_history.keys())[-1]
                return full_history[latest_ts]
        except Exception as e:
            logger.warning(f"Failed to read dashboard JSON: {e}")
    return None


def build_status_message(data: dict) -> str:
    """Build a human-readable status message from the dashboard snapshot."""
    # Try to use the actual sync time from the data if available
    macro = data.get("macro_health", {})
    last_sync_iso = macro.get("last_sync", "")
    try:
        if last_sync_iso:
            sync_time = datetime.fromisoformat(last_sync_iso.replace("Z", "+00:00")).strftime("%H:%M")
        else:
            sync_time = datetime.now().strftime("%H:%M")
    except Exception:
        sync_time = datetime.now().strftime("%H:%M")

    lines = [f"🌞 *Mazara 01 — Plant Status* ({sync_time})"]

    macro   = data.get("macro_health", {})
    sensors = data.get("sensor_data", {})

    # ── Power ───────────────────────────────────────────────────────────────
    total_mw = 0.0
    inv_health = data.get("inverter_health", {})
    for h in inv_health.values():
        pwr = h.get("ac_power_kw", 0) or 0
        try:
            total_mw += float(pwr)
        except Exception:
            pass
    total_mw /= 1000   # kW → MW
    lines.append(f"\n⚡ *Total AC Power:* {total_mw:.2f} MW")

    online   = macro.get("online", "—")
    tripped  = macro.get("tripped", "—")
    comms    = macro.get("comms_lost", "—")
    lines.append(f"🟢 Online: {online}  🔴 Tripped: {tripped}  🔇 Comms lost: {comms}")

    # ── PR ──────────────────────────────────────────────────────────────────
    pr_vals = []
    for h in inv_health.values():
        pr = h.get("pr")
        if pr is not None:
            try:
                pr_vals.append(float(pr))
            except Exception:
                pass
    if pr_vals:
        avg_pr = sum(pr_vals) / len(pr_vals)
        lines.append(f"\n📊 *Avg PR:* {avg_pr:.1f}%")
    else:
        lines.append("\n📊 *PR:* no data")

    # ── Temperature ─────────────────────────────────────────────────────────
    temp_vals = []
    for h in inv_health.values():
        t = h.get("temperature")
        if t is not None:
            try:
                temp_vals.append(float(t))
            except Exception:
                pass
    if temp_vals:
        avg_temp = sum(temp_vals) / len(temp_vals)
        max_temp = max(temp_vals)
        lines.append(f"\n🌡️ *Avg Temp:* {avg_temp:.1f}°C  |  Max: {max_temp:.1f}°C")
    else:
        # Fall back to sensor data
        irr_keys = [k for k in sensors if "IT" in k or "Temp" in k.lower()]
        if irr_keys:
            t_val = sensors[irr_keys[0]]
            lines.append(f"\n🌡️ *Temperature (sensor):* {t_val}")
        else:
            lines.append("\n🌡️ *Temperature:* no data")

    # ── Irradiance (POA) ────────────────────────────────────────────────────
    poa_val = None
    for k, v in sensors.items():
        if "POA" in k:
            try:
                poa_val = float(v)
                break
            except Exception:
                pass
    if poa_val is not None:
        lines.append(f"☀️ *POA Irradiance:* {poa_val:.1f} W/m²")

    # ── Active Alerts ────────────────────────────────────────────────────────
    alerts = data.get("active_anomalies", [])
    if alerts:
        lines.append(f"\n🚨 *Active Alerts ({len(alerts)}):*")
        for a in alerts[:5]:   # cap at 5 to avoid huge messages
            inv  = a.get("inverter", "?")
            atype = a.get("type", "?")
            t    = a.get("trip_time", "?")
            lines.append(f"  • {inv} — {atype} @ {t}")
        if len(alerts) > 5:
            lines.append(f"  _…and {len(alerts) - 5} more_")
    else:
        lines.append("\n✅ *No active alerts*")

    # ── Last Sync ────────────────────────────────────────────────────────────
    last_sync = macro.get("last_sync", "unknown")
    lines.append(f"\n🔄 _Last data sync: {last_sync}_")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Telegram API helpers
# ---------------------------------------------------------------------------

class TelegramBot:
    def __init__(self, token: str):
        self.token   = token
        self.base    = f"https://api.telegram.org/bot{token}"
        self.offset  = 0

    def get_updates(self) -> list:
        try:
            r = requests.get(
                f"{self.base}/getUpdates",
                params={"offset": self.offset, "timeout": 5},
                timeout=API_TIMEOUT,
            )
            data = r.json()
            if data.get("ok"):
                return data.get("result", [])
        except Exception as e:
            logger.debug(f"getUpdates error: {e}")
        return []

    def send_message(self, chat_id: str | int, text: str) -> None:
        try:
            requests.post(
                f"{self.base}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=API_TIMEOUT,
            )
            logger.info(f"Sent status reply to chat_id={chat_id}")
        except Exception as e:
            logger.warning(f"Failed to send message: {e}")

    def is_trigger(self, text: str) -> bool:
        """Return True if the message text matches any trigger keyword."""
        lower = text.strip().lower()
        return any(kw in lower for kw in TRIGGER_KEYWORDS)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Telegram status bot starting…")
    settings = load_settings()
    tg       = settings.get("telegram", {})

    if not tg.get("enabled", False):
        logger.warning("Telegram is disabled in user_settings.json — exiting.")
        return

    token  = tg.get("bot_token")
    if not token:
        logger.error("No bot_token configured — exiting.")
        return

    bot = TelegramBot(token)
    logger.info("Bot ready. Polling for messages…")

    while True:
        updates = bot.get_updates()
        for update in updates:
            # Advance offset so we don't re-process the same message
            bot.offset = update["update_id"] + 1

            msg  = update.get("message") or update.get("channel_post")
            if not msg:
                continue

            chat_id = msg.get("chat", {}).get("id")
            text    = msg.get("text", "")

            if not text or not chat_id:
                continue

            if bot.is_trigger(text):
                logger.info(f"Standard trigger received from chat_id={chat_id}: '{text}'")
                data = get_latest_dashboard_json()
                if data:
                    reply = build_status_message(data)
                else:
                    reply = "⚠️ *No live data available yet.*"
                bot.send_message(chat_id, reply)

            elif text.lower().startswith("/ai"):
                # Explicit AI question command
                question = text[3:].strip()
                if not question:
                    bot.send_message(chat_id, "🤖 *AI Mode:* Please ask a question after /ai (e.g. `/ai how is TX3 performing?`)")
                    continue

                logger.info(f"AI question (via /ai) from chat_id={chat_id}: '{question}'")
                bot.send_message(chat_id, "⏳ _AI is thinking... checking plant data..._")
                
                data = get_latest_dashboard_json()
                try:
                    from llm_agent import ask_llm
                    reply = ask_llm(question, data)
                except Exception as e:
                    logger.error(f"LLM Error: {e}")
                    reply = f"🤖 My AI brain is offline. Error: {e}"
                
                bot.send_message(chat_id, reply)
            
            else:
                # For any other text, suggest the commands
                if chat_id > 0: # Only reply to direct messages, ignore group noise
                    bot.send_message(chat_id, "❓ Unknown command. Use *status* for a quick report or */ai <question>* for the chatbot.")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
