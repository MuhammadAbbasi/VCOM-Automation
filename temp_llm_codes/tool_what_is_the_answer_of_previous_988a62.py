# TASK: what is the answer of previous question?
# SAVED: 2026-04-22 11:15

from datetime import datetime

def is_production_active(data):
    # Check if more than 15 inverters are producing >300W
    active_inverters = sum(1 for inv_id, power in data['inverter_health'].items() 
                           if get_inverter_power(inv_id, TODAY) > 300)
    
    return active_inverters > 15

# Assuming TODAY is defined somewhere in the context
TODAY = datetime.today().strftime('%Y-%m-%d')

# Example data structure for 'data'
data = {
    'macro_health': {'MW': None, 'PR': None},
    'inverter_health': {
        'INV TX1-01': {'ac_v': 400, 'temp_v': 35, 'pr_v': 98.5, 'dc_v': 20, 'overall_status': 'active'},
        # Add other inverter data here
    },
    'active_anomalies': []
}

# Check if production is active
production_active = is_production_active(data)
print(f"Production is {'active' if production_active else 'inactive'}")