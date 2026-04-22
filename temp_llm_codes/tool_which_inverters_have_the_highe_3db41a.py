# TASK: Which inverters have the highest temperature?
# SAVED: 2026-04-22 11:27

import os
from datetime import datetime

# Assuming data is already loaded as a dictionary containing relevant information
data = {
    'macro_health': {'MW': 0.5, 'PR': 0.8},
    'inverter_health': {f'INV TX{i//12+1}-{i%12+1}': 
                        {'ac_v': 400, 'temp_v': 35, 'pr_v': 950, 'dc_v': 600, 'overall_status': 'active'}
                        for i in range(1, 37)},
    'active_anomalies': []
}

# Get today's date
today = datetime.today().strftime('%Y-%m-%d')

# Check if temperature data file exists for today
temperature_file = f'Temperatura_{today}.csv'
if not os.path.exists(f'extracted_data/{temperature_file}'):
    print(f'Data for {today} is not available in extracted_data/')
else:
    # Load the temperature CSV file
    df_temp = pd.read_csv(f'extracted_data/{temperature_file}')
    
    # Filter columns based on the exact pattern 'Temperatura (INV TX1-01) [°C]'
    temp_columns = [col for col in df_temp.columns if 'Temperatura (INV' in col]
    
    # Find the maximum temperature and corresponding inverters
    max_temp = df_temp[temp_columns].max().max()
    inverters_with_max_temp = [col.split(' ')[2] for col in temp_columns if df_temp[col].max() == max_temp]
    
    print(f'The inverters with the highest temperature on {today} are: {inverters_with_max_temp}')