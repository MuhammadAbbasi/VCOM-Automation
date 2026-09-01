"""
db/vcom_status_helpers.py — Helper functions to manage and read VCOM portal connectivity status.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATUS_FILE = ROOT / "db" / "vcom_status.json"

logger = logging.getLogger(__name__)

def save_vcom_status(status: str, http_code: int = 200, error_message: str = "") -> dict:
    """
    Save VCOM portal status ('online' | 'down').
    Returns the updated status dictionary.
    """
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    current = get_vcom_status()
    now_iso = datetime.now().isoformat()
    
    down_since = current.get("down_since")
    if status == "down":
        if not down_since or current.get("status") != "down":
            down_since = now_iso
    else:
        down_since = None
        
    data = {
        "status": status,
        "http_code": http_code,
        "error_message": error_message,
        "last_checked": now_iso,
        "down_since": down_since
    }
    
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        if current.get("status") != status:
            logger.warning(f"[VCOM STATUS] Changed to '{status.upper()}' (Code: {http_code}, Error: {error_message})")
    except Exception as e:
        logger.error(f"[VCOM STATUS] Failed to save status: {e}")
        
    return data

def get_vcom_status() -> dict:
    """Read the current VCOM portal status from disk."""
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
            
    return {
        "status": "online",
        "http_code": 200,
        "error_message": "",
        "last_checked": datetime.now().isoformat(),
        "down_since": None
    }
