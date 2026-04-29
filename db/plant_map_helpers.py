"""
plant_map_helpers.py — Data generation and health scoring for 3-level plant map visualization.

Provides:
  - String health calculation (composite score from DC, Temp, ISO, PR)
  - Inverter overview aggregation
  - Tracker-to-string mapping
"""

import json
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from db.db_manager import (
    get_data_conn, load_metric, INVERTER_IDS
)

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
PLANT_LAYOUT_PATH = ROOT / "db" / "plant_layout.json"


def load_plant_layout() -> dict:
    """Load plant topology from JSON."""
    try:
        with open(PLANT_LAYOUT_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load plant layout: {e}")
        return {}


def calculate_string_health(
    inverter_id: str,
    mppt_number: int,
    date: str = None
) -> dict:
    """
    Calculate composite health score for a string (0-100).

    Combines: DC current + Temperature + Insulation Resistance + PR
    Returns: {
        'status': 'green|yellow|red',
        'health_score': 0-100,
        'metrics': {...}
    }
    """
    date = date or datetime.now().strftime("%Y-%m-%d")
    conn = get_data_conn()

    try:
        # 1. DC Current (should be 10-150A for healthy string)
        dc_score = 50
        try:
            cursor = conn.execute(
                "SELECT value FROM corrente_dc WHERE date=? AND inverter_id=? AND mppt_number=? ORDER BY ora DESC LIMIT 1",
                (date, inverter_id, mppt_number)
            )
            row = cursor.fetchone()
            if row and row[0]:
                dc_val = row[0]
                if 10 < dc_val < 150:
                    dc_score = 100
                elif 5 < dc_val < 200:
                    dc_score = 70
                else:
                    dc_score = 30
        except Exception:
            pass

        # 2. Temperature (should be < 65°C)
        temp_score = 50
        try:
            df = load_metric(date, "Temperatura")
            if df is not None and not df.empty and inverter_id in df.columns:
                temp_val = pd.to_numeric(df[inverter_id].iloc[-1], errors='coerce')
                if temp_val and temp_val < 65:
                    temp_score = 100
                elif temp_val and temp_val < 75:
                    temp_score = 70
                else:
                    temp_score = 30
        except Exception:
            pass

        # 3. Insulation Resistance (should be > 50kΩ)
        iso_score = 50
        try:
            df = load_metric(date, "Resistenza di isolamento")
            if df is not None and not df.empty and inverter_id in df.columns:
                iso_val = pd.to_numeric(df[inverter_id].iloc[-1], errors='coerce')
                if iso_val and iso_val > 50:
                    iso_score = 100
                elif iso_val and iso_val > 20:
                    iso_score = 70
                else:
                    iso_score = 30
        except Exception:
            pass

        # 4. PR (should be > 0.75)
        pr_score = 50
        try:
            df = load_metric(date, "PR inverter")
            if df is not None and not df.empty and inverter_id in df.columns:
                pr_val = pd.to_numeric(df[inverter_id].iloc[-1], errors='coerce')
                if pr_val and pr_val > 0.75:
                    pr_score = 100
                elif pr_val and pr_val > 0.65:
                    pr_score = 70
                else:
                    pr_score = 30
        except Exception:
            pass

        # Composite score (average)
        health_score = (dc_score + temp_score + iso_score + pr_score) / 4

        # Status determination
        if health_score >= 80:
            status = 'green'
        elif health_score >= 60:
            status = 'yellow'
        else:
            status = 'red'

        return {
            'status': status,
            'health_score': round(health_score, 1),
            'metrics': {
                'dc_current_score': dc_score,
                'temperature_score': temp_score,
                'iso_score': iso_score,
                'pr_score': pr_score
            }
        }

    except Exception as e:
        logger.error(f"Error calculating string health: {e}")
        return {'status': 'unknown', 'health_score': 0, 'metrics': {}}


def get_inverter_health_overview(inverter_id: str, date: str = None) -> dict:
    """
    Get quick health snapshot for an inverter (samples 2 strings per MPPT).
    """
    date = date or datetime.now().strftime("%Y-%m-%d")
    layout = load_plant_layout()

    if inverter_id not in layout.get("inverter_locations", {}):
        return {'error': f'Inverter {inverter_id} not found'}

    inv_loc = layout["inverter_locations"][inverter_id]
    inv_detail = layout.get("inverter_details", {}).get(inverter_id, {})

    # Sample health from both MPPTs
    health_samples = [
        calculate_string_health(inverter_id, 1, date),
        calculate_string_health(inverter_id, 2, date)
    ]

    avg_health = sum(s['health_score'] for s in health_samples) / len(health_samples)

    # Overall status
    if avg_health >= 80:
        overall_status = 'green'
    elif avg_health >= 60:
        overall_status = 'yellow'
    else:
        overall_status = 'red'

    return {
        'inverter_id': inverter_id,
        'location': {'x': inv_loc['x'], 'y': inv_loc['y']},
        'section': inv_loc['section_name'],
        'health_status': overall_status,
        'health_score': round(avg_health, 1),
        'trackers': inv_detail.get('trackers', 10),
        'strings': inv_detail.get('strings', 30),
        'mppts': inv_detail.get('mppts', 2),
        'sample_scores': [round(s['health_score'], 1) for s in health_samples]
    }


def get_inverter_strings_detail(inverter_id: str, date: str = None) -> dict:
    """
    LEVEL 2: Get all strings in an inverter with individual health status.
    Strings are generated based on plant_layout.json configuration.
    """
    date = date or datetime.now().strftime("%Y-%m-%d")
    layout = load_plant_layout()

    if inverter_id not in layout.get("inverter_locations", {}):
        return {'error': f'Inverter {inverter_id} not found'}

    inv_loc = layout["inverter_locations"][inverter_id]
    inv_detail = layout.get("inverter_details", {}).get(inverter_id, {})

    num_trackers = inv_detail.get('trackers', 10)
    num_mppts = inv_detail.get('mppts', 2)
    total_strings_per_inv = inv_detail.get('strings', 30)

    strings = []
    summary = {'green': 0, 'yellow': 0, 'red': 0, 'total': 0}

    # Distribute strings evenly across trackers and MPPTs
    strings_per_tracker = max(1, total_strings_per_inv // num_trackers) if num_trackers > 0 else 1

    string_counter = 0
    for tracker_num in range(1, num_trackers + 1):
        tracker_id = f"TCU_{inverter_id.replace('-', '')}_{tracker_num:02d}"

        # Distribute strings for this tracker across MPPTs
        for mppt in range(1, num_mppts + 1):
            strings_for_this_mppt = strings_per_tracker // num_mppts
            if mppt == 1:
                # Give remainder to first MPPT
                strings_for_this_mppt += strings_per_tracker % num_mppts

            for s in range(1, strings_for_this_mppt + 1):
                string_counter += 1
                if string_counter > total_strings_per_inv:
                    break

                string_id = f"S_{inverter_id}_{tracker_num:02d}_{mppt}_{s}"
                health = calculate_string_health(inverter_id, mppt, date)

                string_data = {
                    'string_id': string_id,
                    'tracker_id': tracker_id,
                    'mppt': mppt,
                    'health_status': health['status'],
                    'health_score': health['health_score'],
                    'position_in_tracker': s
                }
                strings.append(string_data)

                summary[health['status']] += 1
                summary['total'] += 1

            if string_counter >= total_strings_per_inv:
                break

        if string_counter >= total_strings_per_inv:
            break

    return {
        'inverter_id': inverter_id,
        'section': inv_loc.get('section_name', 'Unknown'),
        'location': {'x': inv_loc['x'], 'y': inv_loc['y']},
        'num_strings': len(strings),
        'strings': strings,
        'summary': {
            'healthy': summary['green'],
            'warning': summary['yellow'],
            'critical': summary['red'],
            'total': summary['total']
        }
    }


def get_plant_overview(date: str = None) -> dict:
    """
    LEVEL 1: Get all inverters with summary health for map visualization.
    """
    date = date or datetime.now().strftime("%Y-%m-%d")
    layout = load_plant_layout()

    inverters = []
    summary = {'online': 0, 'warning': 0, 'critical': 0}

    for inv_id in INVERTER_IDS:
        overview = get_inverter_health_overview(inv_id, date)
        if 'error' not in overview:
            inverters.append(overview)

            if overview['health_status'] == 'green':
                summary['online'] += 1
            elif overview['health_status'] == 'yellow':
                summary['warning'] += 1
            else:
                summary['critical'] += 1

    return {
        'timestamp': datetime.now().isoformat(),
        'date': date,
        'inverters': inverters,
        'summary': {
            'online': summary['online'],
            'warning': summary['warning'],
            'critical': summary['critical'],
            'total': len(inverters)
        },
        'plant_info': {
            'width': layout.get('metadata', {}).get('plant_width', 1000),
            'height': layout.get('metadata', {}).get('plant_height', 800)
        }
    }


# For compatibility with existing code
if __name__ == "__main__":
    print("Plant Layout Helpers Module")
    print("Use in API endpoints or import functions as needed")
