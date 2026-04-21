# VCOM Analysis System - Data Structure & Fix Summary

## Problem Statement

The original analysis code (`processor_watchdog.py`) was producing all "grey" health LEDs and no anomalies because:

1. **Data structure mismatch**: Extraction files have different formats (long vs. wide, different row counts)
2. **Row merging failure**: Attempted full left-joins on "Ora" column with files of different sizes caused memory exhaustion
3. **Last row bias**: Analysis always used final row index, which often contains NaN values (because data is only available up to current time, not 23:59)
4. **Unrealistic thresholds**: DC current thresholds were too high for late afternoon (1.6 A/MPPT is healthy, not red)

## Actual Data Structures

### File Size Comparison
```
Potenza_AC:              23,040 rows × 38 cols   (minute-by-minute, 16 days)
Temperatura:             20,160 rows × 38 cols   (different extraction freq)
Irraggiamento:           20,160 rows × 16 cols
Resistenza_Isolamento:   20,160 rows × 38 cols
Corrente_DC:             11,520 rows × 434 cols  (extra wide: 12 MPPTs × 36 inv)
PR:                         720 rows ×  3 cols   (long format: 20 extractions × 36 inv)
```

### Column Types

**Time Series (Wide Format)**:
- Timestamp Fetch: str (HH:MM:SS)
- Ora: float (0.00-23.99, decimal hours, e.g., 17.45 = 5:27 PM)
- Metric columns: float (numeric inverter data)

**PR (Long Format)**:
- Timestamp Fetch: str
- Inverter: str ("INV TX1-01", etc.)
- PR: float (0-100% scale, e.g., 96.74%)

## Solution: processor_watchdog_final.py

### Key Improvements

**1. No Massive Merges**
- Uses Potenza_AC as master time series
- Looks up values from other files on-demand
- Avoids memory exhaustion from full outer joins

**2. Finds Latest Valid Data**
```python
# Instead of ac_df.iloc[-1] (which is NaN)
# Search backwards for last row with >30 valid inverter values
for idx in range(len(ac_df) - 1, -1, -1):
    if count_non_nan_values(row) > 30:
        use_this_row()
        break
```

**3. Time-Aware Thresholds**
```python
# DC current threshold varies by time of day
if 7:00 <= time <= 12:00:  # Morning
    green_threshold = 10 A
    yellow_threshold = 2 A
elif 12:00 < time <= 19:00:  # Afternoon decline
    green_threshold = 5 A
    yellow_threshold = 0.5 A
```

**4. Realistic Health Flags**
- **Green**: All metrics healthy for current conditions
- **Yellow**: Warning (minor degradation), normal for late afternoon
- **Red**: Critical (string failure, overtemp, comms loss)
- **Grey**: No data available

### Four Health LEDs

Each inverter gets scored on 4 independent metrics:

| LED | Green | Yellow | Red | Grey |
|-----|-------|--------|-----|------|
| **PR** | ≥85% | 75-85% | <75% | NaN |
| **Temperature** | ≤40°C | 40-45°C | >45°C | NaN |
| **DC Current** | Time-dependent (see above) | | <threshold | NaN |
| **AC Power** | >5kW | 1-5kW | <1kW or 0 during daylight | NaN |
| **Overall** | All green | Any yellow | Any red | All grey/NaN |

### Analysis Results (2026-04-02)

```
Timestamp: 17:45 (5:27 PM, late afternoon)

Macro Health:
  Online: 35/36 inverters
  Tripped: 0
  Comms Lost: 1

Status Distribution:
  Green: 0    (not expected at end of day)
  Yellow: 36  (normal for declining power)
  Red: 0
  Grey: 0

Sample Inverter (TX1-01):
  PR: green       (96.74%)
  Temp: green     (23.3°C)
  DC Current: yellow  (1.61 A/MPPT average, threshold=0.5A at 17:45)
  AC Power: green (healthy output for time of day)
  Overall: YELLOW
```

## Data Format Recommendation

### CSV for Analysis
- **Speed**: 10-50x faster than openpyxl (no Excel parsing)
- **File Size**: ~70% smaller
- **Memory**: Lower footprint
- **Current System**: Automatically converts Excel→CSV on first load

### Storage Hierarchy
1. **Extract**: Excel files (openpyxl native output)
2. **Analyze**: Convert to CSV on-demand
3. **Export**: JSON snapshots (real-time) + optional CSV audit trails (anomalies only)
4. **Archive**: Old CSV/JSON files (> 7 days)

## File Naming & Locations

**Extraction Output**:
```
extracted_data/
├── PR_2026-04-02.xlsx                    # Long format
├── Potenza_AC_2026-04-02.xlsx            # Wide time series (master)
├── Corrente_DC_2026-04-02.xlsx           # Wide time series (434 cols)
├── Temperatura_2026-04-02.xlsx           # Wide time series
├── Resistenza_Isolamento_2026-04-02.xlsx # Wide time series
├── Irraggiamento_2026-04-02.xlsx         # Wide time series (14 sensors)
└── extraction_status.json                # Status tracking
```

**Analysis Output** (auto-generated):
```
├── dashboard_data_2026-04-02.json        # Health snapshot (realtime update)
├── anomalies_2026-04-02.csv              # Anomaly trail (if any detected)
├── PR_2026-04-02.csv                     # Converted (cache)
├── Potenza_AC_2026-04-02.csv
└── ... (other CSV caches)
```

## How to Use

### Replace Watchdog Process

```bash
# Old: python processor_watchdog.py
# New:
python processor_watchdog_final.py
```

### Manual Analysis (Debugging)

```python
from processor_watchdog_final import analyze_site

# Analyze a specific date
analyze_site("2026-04-02")

# Check output
import json
with open("extracted_data/dashboard_data_2026-04-02.json") as f:
    snapshot = json.load(f)
    print(snapshot)
```

## Thresholds & Tuning

All thresholds can be adjusted in the file:

```python
# Line ~40
PR_THRESHOLD = 85.0        # % (normalize to 0-100 scale)
TEMP_CRITICAL = 45.0       # °C
TEMP_WARNING = 40.0        # °C
AC_HEALTHY_MIN = 5000      # W (for midday check)
DAYLIGHT_START = 7.0       # hours (07:00)
DAYLIGHT_END = 19.0        # hours (19:00)
```

### Expected Values by Time

**00:00-06:59 (Night)**:
- AC: 0 W
- DC: 0 A
- Temp: 15-25°C
- LED: All grey (off-hours)

**07:00-09:00 (Morning ramp)**:
- AC: 0 → 5,000 W
- DC: 0 → 20 A/MPPT
- Temp: 25 → 30°C
- LED: Yellow (warming up)

**09:00-14:00 (Peak)**:
- AC: 10,000-25,000 W
- DC: 20-40 A/MPPT
- Temp: 30-45°C
- LED: Green (if PR ≥85%)

**14:00-19:00 (Afternoon decline)**:
- AC: 5,000 → 500 W (declining)
- DC: 10 → 1 A/MPPT
- Temp: 45 → 30°C
- LED: Yellow (normal decline)

**19:00-23:59 (Evening/Night)**:
- AC: 0 W
- DC: 0 A
- Temp: 20 → 15°C
- LED: Grey (off-hours)

## File Versions

Three analysis implementations provided:

| File | Status | Notes |
|------|--------|-------|
| `processor_watchdog.py` | ❌ OLD | Caused memory crash on data merge |
| `processor_watchdog_v2.py` | ❌ DEPRECATED | Attempted forensic rules, still memory issues |
| `processor_watchdog_v3.py` | ❌ DEPRECATED | Better merge logic, still OOM on Resistenza |
| `processor_watchdog_final.py` | ✅ ACTIVE | Memory-efficient, working correctly |

**Use ONLY**: `processor_watchdog_final.py`

## Integration with Dashboard

The dashboard automatically reads the latest JSON snapshot:

```bash
# In a separate window:
python dashboard/app.py

# Frontend polls every 10 seconds:
GET /api/status → Returns latest dashboard_data_{today}.json
```

LED colors displayed on dashboard:
- **Green (#10b981)**: Healthy
- **Yellow (#f59e0b)**: Warning (Performance/Temp)
- **Red (#ef4444)**: Critical (Trip/Severe Loss)
- **Slate Grey (#94a3b8)**: Communications Lost (Plant Overview specific)
- **Dark Grey (#6b7280)**: No data / outside window

## Verification Checklist

After deployment:

- [ ] `processor_watchdog_final.py` runs without errors
- [ ] JSON snapshot generated within 5 seconds of file creation
- [ ] Dashboard shows health for all 36 inverters
- [ ] LED colors match expected patterns for time of day
- [ ] AC power values match Potenza_AC file
- [ ] Temperature ranges realistic (15-50°C)
- [ ] No memory warnings in watchdog logs
- [ ] Old `processor_watchdog.py` removed or backed up

---

## Next Steps: Future Enhancements

1. **Anomaly Detection** (6 forensic rules):
   - Low PR detection
   - High temperature alerts
   - DC string failures
   - AC power deviations
   - Communication loss
   - Inverter trip detection

2. **Time-Series Visualization**:
   - Power curves (AC generation over day)
   - Temperature trends
   - Efficiency curves

3. **Predictive Maintenance**:
   - Temperature trend analysis
   - Insulation resistance tracking
   - String degradation detection

4. **Email/Webhook Alerts**:
   - Critical anomalies → email to operators
   - Daily health summaries

5. **Historical Data Archive**:
   - Move CSV files to separate archive directory
   - Compress > 30 days old
   - SQL database backend option

---

## Hardening & Stability (April 2026)

The system has been significantly hardened to handle real-world browser and platform instabilities:

**1. Browser Session Shield**
- **Modal Stripping**: VCOM often overlays "Minimum values not available" Bootstrap-Vue modals that block interactions. The scraper now uses a JS DOM-stripping method to cleanly remove these without relying on frail 'click' events.
- **Session Recovery**: Checks for visibility of core UI elements every cycle; triggers re-login immediately if the session has timed out.

**2. Data Ingestion Sync**
- **Extraction Status**: `base_monitor.py` now hooks into the export cycle to update `extraction_status.json`. This provides real-time "Success/Pending" feedback on the dashboard.
- **Filename Flexibility**: The watchdog loader is now case and character-agnostic, handling both `Potenza AC` (Spaces) and `Potenza_AC` (Underscores) seamlessly to prevent missing data alerts.

**3. Orchestrator Stability**
- **Smarter Hot-Reload**: Disabled the aggressive "restart all" logic during automated development to prevent browser cycle interruptions.
- **Deduplication**: Telegram bot and Watchdog now use improved lookups to prevent "Alarm Storms" during intermittent comms drops.

---

**Document Version**: 1.1
**Date**: 2026-04-21
**System**: VCOM Mazara del Vallo (36 inverters)
**Status**: Production-Ready / Hardened
