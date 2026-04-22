# TASK: Are there any active alerts right now?
# SAVED: 2026-04-22 11:19

# Check if there are any active anomalies (alerts)
if data['active_anomalies']:
    print("There are active alerts right now.")
else:
    print("No active alerts right now.")