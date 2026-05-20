import sqlite3
import json
import os
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'db', 'scada_data.db')
LOGS_DB = os.path.join(ROOT, 'db', 'scada_logs.db')
LINK_STATUS = os.path.join(ROOT, 'db', 'link_status.json')

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
print('DB:', DB)
print('tables:', [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()])

cols_info = conn.execute('PRAGMA table_info(potenza_ac)').fetchall()
print('potenza_ac cols count:', len(cols_info))
inv_cols = [r['name'] for r in cols_info if r['name'].startswith('Potenza AC (INV')]
print('inv cols count:', len(inv_cols))
print('first 5 inv cols:', inv_cols[:5])

today = datetime.now().strftime('%Y-%m-%d')
rows = conn.execute('SELECT COUNT(*) AS c FROM potenza_ac WHERE _date=?', (today,)).fetchone()['c']
print('today rows:', rows)
if rows:
    sample = conn.execute('SELECT Ora, ' + ','.join(f'"{c}"' for c in inv_cols[:5]) + ' FROM potenza_ac WHERE _date=? ORDER BY Ora ASC LIMIT 5', (today,)).fetchall()
    print('sample rows:')
    for r in sample:
        print({k: r[k] for k in r.keys()})

    total_wh = 0.0
    peak_w = 0.0
    peak_ora = None
    for r in conn.execute('SELECT Ora, ' + ','.join(f'"{c}"' for c in inv_cols) + ' FROM potenza_ac WHERE _date=? ORDER BY Ora ASC', (today,)):
        values = [r[c] for c in inv_cols if r[c] is not None and r[c] > 0]
        plant = sum(values)
        total_wh += plant * 0.25
        if plant > peak_w:
            peak_w = plant
            peak_ora = r['Ora']
    print('computed total_Wh:', total_wh)
    print('computed total_MWh:', total_wh / 1_000_000)
    print('computed peak_W:', peak_w)
    print('computed peak_MW:', peak_w / 1_000_000)
    print('peak_ora:', peak_ora)

print('\nLatest tracker_status rows:')
for r in conn.execute('SELECT last_update, ncu_id, tcu_id, mode FROM tracker_status ORDER BY last_update DESC LIMIT 10').fetchall():
    print({k: r[k] for k in r.keys()})
conn.close()

if os.path.exists(LINK_STATUS):
    print('\nlink_status.json:')
    with open(LINK_STATUS, encoding='utf-8') as f:
        print(json.dumps(json.load(f), indent=2))
else:
    print('\nlink_status.json missing')

if os.path.exists(LOGS_DB):
    conn2 = sqlite3.connect(LOGS_DB)
    conn2.row_factory = sqlite3.Row
    cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
    print('\nlog tables:', [r[0] for r in conn2.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()])
    print('watchdog log count 24h:', conn2.execute("SELECT COUNT(*) FROM logs WHERE source='watchdog' AND level IN ('WARNING','ERROR','CRITICAL') AND timestamp > ?", (cutoff,)).fetchone()[0])
    conn2.close()
else:
    print('\nlogs DB missing')

# Additional tracker status insight
try:
    conn3 = sqlite3.connect(DB)
    conn3.row_factory = sqlite3.Row
    tracker_counts = conn3.execute('SELECT ncu_id, COUNT(*) AS c FROM tracker_status GROUP BY ncu_id').fetchall()
    print('\nTracker counts per NCU:')
    total = 0
    for r in tracker_counts:
        print(f"  {r['ncu_id']}: {r['c']}")
        total += r['c']
    print('  total:', total)
    sample = conn3.execute('SELECT ncu_id, tcu_id FROM tracker_status ORDER BY ncu_id, tcu_id LIMIT 20').fetchall()
    print('Tracker sample rows:')
    for r in sample:
        print({k: r[k] for k in r.keys()})
    conn3.close()
except Exception as e:
    print('Could not inspect tracker_status:', e)

# Additional daily energy insight
try:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    recent_dates = [r['_date'] for r in conn.execute("SELECT DISTINCT _date FROM potenza_ac ORDER BY _date DESC LIMIT 5").fetchall()]
    print('\nRecent potenza_ac dates:', recent_dates)
    for date in recent_dates:
        count = conn.execute('SELECT COUNT(*) AS c FROM potenza_ac WHERE _date=?', (date,)).fetchone()['c']
        print(f"date {date} rows {count}")
    conn.close()
except Exception as e:
    print('Could not inspect recent potenza_ac dates:', e)
