# TASK: write the final answer here
# SAVED: 2026-04-22 11:16

def is_production_active(data):
    # Count how many inverters are producing > 300W
    count = sum(1 for inv_id in data['inverter_health'] if get_inverter_power(inv_id, TODAY) > 300)
    
    return count > 15

# Assuming `data` is the provided state JSON structure
production_status = is_production_active(data)

if production_status:
    print("Production is active.")
else:
    print("No active production.")