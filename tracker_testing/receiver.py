import sys
import json
import logging
from datetime import datetime
from pathlib import Path
import paho.mqtt.client as mqtt

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# --- Configuration ---
BROKER_IP = "localhost"  # Change to the broker's IP if not running locally
TOPIC_REQUEST = "telemetry/plant/sync/request"
TOPIC_ACK = "telemetry/plant/sync/ack"
TOPIC_HEARTBEAT = "telemetry/plant/heartbeat"

# Ensure log directory exists
ROOT = Path(__file__).parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(ROOT / "receiver.log"),
        logging.StreamHandler()
    ]
)

# Corrections for known SCADA OCR misreads on specific NCU/TCU pairs.
# Key: (ncu, tcu) exactly as received. Value: correct tracker_no.
TRACKER_CORRECTIONS = {
    ("NCU_03", "TCU 04"): "Tracker 247",
    ("NCU_03", "TCU 92"): "Tracker 325",
}

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logging.info("Connected to MQTT Broker successfully.")
        client.subscribe(TOPIC_REQUEST, qos=1)
        client.subscribe(TOPIC_HEARTBEAT, qos=0)
        logging.info(f"Subscribed to: {TOPIC_REQUEST} and {TOPIC_HEARTBEAT}")
    else:
        logging.error(f"Connection failed with code {reason_code}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        
        if msg.topic == TOPIC_HEARTBEAT:
            handle_heartbeat(payload)
        elif msg.topic == TOPIC_REQUEST:
            handle_sync_request(client, payload)
            
    except Exception as e:
        logging.error(f"Failed to process message on {msg.topic}: {e}")

def handle_heartbeat(payload):
    site_id = payload.get("site", "Unknown")
    status = payload.get("status", "unknown")
    logging.info(f"Heartbeat from {site_id}: {status}")
    
    # Update Link Status file for Dashboard
    status_file = ROOT_DIR / "db" / "link_status.json"
    try:
        status_data = {
            "last_heartbeat": datetime.now().isoformat(),
            "site": site_id,
            "status": status
        }
        with open(status_file, "w", encoding="utf-8") as f:
            json.dump(status_data, f)
    except Exception as e:
        logging.error(f"Failed to update link status: {e}")

def handle_sync_request(client, payload):
    site_id = payload.get("site", "Unknown")
    data = payload.get("data", [])
    
    logging.info(f"--- Incoming Sync Request ---")
    logging.info(f"Site ID: {site_id}")
    logging.info(f"Records: {len(data)}")

    # 2. DATA PROCESSING
    seen = set()
    unique_data = []
    duplicates = []
    
    for record in data:
        key = (record.get("ncu"), record.get("tcu"))
        if key in seen:
            duplicates.append(f"{key[0]} - {key[1]}")
        else:
            seen.add(key)
            unique_data.append(record)

    if duplicates:
        logging.warning(f"Detected {len(duplicates)} duplicates in batch.")

    # Apply tracker number corrections for known SCADA OCR misreads
    for record in unique_data:
        key = (record.get("ncu"), record.get("tcu"))
        if key in TRACKER_CORRECTIONS:
            original = record.get("tracker_no")
            record["tracker_no"] = TRACKER_CORRECTIONS[key]
            logging.info(f"Corrected {key}: {original} -> {record['tracker_no']}")

    # Send Telegram Summary
    try:
        import requests
        settings_path = ROOT_DIR / "user_settings.json"
        config = {}
        if settings_path.exists():
            with open(settings_path, "r", encoding="utf-8") as f:
                config = json.load(f)
        
        if config and config.get("telegram", {}).get("enabled"):
            token = config["telegram"]["bot_token"]
            # Use personal_id ONLY for tracker updates as requested
            chat_id = config["telegram"].get("personal_id")
            
            # compute some stats from unique_data
            total_trackers = len(unique_data)
            
            # Use dynamic deviation threshold from config (default to 5.0)
            dev_threshold = config.get("thresholds", {}).get("tracker_deviation", 5.0)
            critical_dev = sum(1 for t in unique_data if abs(t.get("actual_angle", 0) - t.get("target_angle", 0)) > dev_threshold)
            
            # Check Telegram preference for tracker deviation (default to True)
            alert_pref_tg = config.get("alert_preferences", {}).get("tracker_deviation", {}).get("telegram", True)
            
            ncu_counts = {}
            for t in unique_data:
                ncu = t.get("ncu", "Unknown")
                ncu_counts[ncu] = ncu_counts.get(ncu, 0) + 1
                
            ncu_str = ", ".join([f"{str(k).replace('&', '&amp;').replace('<', '&lt;')}: {v}" for k, v in ncu_counts.items()])
            
            if critical_dev > 0 and alert_pref_tg:
                msg = f"📡 <b>New Tracker Data Received</b>\n\n" \
                      f"• <b>Total Units:</b> {total_trackers}\n" \
                      f"• <b>NCUs:</b> {ncu_str}\n" \
                      f"• <b>Critical Deviations (>{dev_threshold}°):</b> {critical_dev}\n"
                      
                if duplicates:
                    msg += f"\n⚠️ <b>Duplicates filtered:</b> {len(duplicates)}"
                    
                resp = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                             json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=15)
                resp.raise_for_status()
                logging.info("Telegram summary alert sent due to critical deviations.")
            else:
                logging.info(f"No critical deviations ({critical_dev}) or Telegram alert disabled ({alert_pref_tg}). Skipping Telegram alert.")
    except Exception as te:
        logging.error(f"Failed to send Telegram summary: {te}")
        if hasattr(te, 'response') and te.response is not None:
            logging.error(f"Telegram response: {te.response.text}")

    # Save to database
    try:
        from db.db_manager import save_tracker_data
        save_tracker_data(unique_data)
        logging.info(f"Successfully saved {len(unique_data)} records to database.")
    except Exception as e:
        logging.error(f"Failed to save to database: {e}")

    # Save records for inspection as fallback
    filename = ROOT / f"sync_{site_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w") as f:
         json.dump(data, f, indent=4)
    logging.info(f"Raw data saved to {filename.name}")

    # 3. Send Handshake Acknowledgment (ACK)
    ack_payload = json.dumps({
        "status": "received",
        "site": site_id,
        "server_timestamp": datetime.now().isoformat(),
        "duplicates_ignored": len(duplicates)
    })
    client.publish(TOPIC_ACK, ack_payload, qos=1)
    logging.info(f"ACK sent to {site_id}")
    logging.info("-" * 30)


# --- Main Initialization ---
if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="DASHBOARD_HOST_RECEIVER")
    client.on_connect = on_connect
    client.on_message = on_message

    logging.info("Starting Dashboard Receiver Service...")
    try:
        client.connect(BROKER_IP, 1883, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        logging.info("Receiver stopped by user.")
    except Exception as e:
        logging.error(f"Critical error: {e}")
