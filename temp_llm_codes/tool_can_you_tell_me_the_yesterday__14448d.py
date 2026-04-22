# TASK: can you tell me the yesterday's total production?
# SAVED: 2026-04-22 11:17

from datetime import datetime, timedelta

# Get today's date
today = datetime.today()

# Calculate yesterday's date
yesterday = today - timedelta(days=1)

# Format the date as YYYY-MM-DD
date_str_yesterday = yesterday.strftime('%Y-%m-%d')

# Get total production for yesterday
total_production_yesterday = get_total_production(date_str_yesterday)

print(f"Yesterday's total production: {total_production_yesterday} MWh")