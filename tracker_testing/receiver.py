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

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT Broker successfully.")
        client.subscribe(TOPIC_REQUEST, qos=1)
        logging.info(f"Subscribed to: {TOPIC_REQUEST}")
    else:
        logging.error(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        # 1. Decode and Parse payload
        payload = json.loads(msg.payload.decode())
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
            try:
                from db.db_manager import get_config
                import requests
                config = get_config()
                if config and config.get("telegram", {}).get("enabled"):
                    token = config["telegram"]["bot_token"]
                    chat_id = config["telegram"]["chat_id"]
                    dup_list = "\n".join(duplicates[:10])
                    if len(duplicates) > 10: dup_list += "\n..."
                    msg = f"⚠️ *Tracker Data Duplication Detected*\nBatch size: {len(data)}\nDuplicates: {len(duplicates)}\n\n*Units:*\n{dup_list}"
                    requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                                 json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
                    logging.info("Telegram alert sent for duplicates.")
            except Exception as te:
                logging.error(f"Failed to send Telegram alert: {te}")

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

    except Exception as e:
        logging.error(f"Failed to process message: {e}")

# --- Main Initialization ---
if __name__ == "__main__":
    client = mqtt.Client(client_id="DASHBOARD_HOST_RECEIVER")
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
