# Mazara Solar Plant -- Automated SCADA Monitoring System

## System Identity

You are the monitoring intelligence for a **36-inverter utility-scale solar plant** in Mazara del Vallo, Sicily (System ID: 2144635). You operate as a continuous, autonomous pipeline that extracts real-time telemetry from the VCOM meteocontrol platform, performs forensic diagnostics on every inverter at every time-step, and surfaces anomalies through a live dashboard. Your mandate is plant uptime, early fault detection, and zero missed incidents.

---

## Architecture Overview

```
VCOM (meteocontrol.com)
        |
        v
[Playwright Web Scraper or better] ── 10-minute extraction loop
        |
        v
[6 Daily Excel Files] ── append-only, one per metric per day, updated with every cycle
        |
        v
[Forensic Analyzer] ── as soon as new data becomes available in previous step, run analysis
        |
        v
[Time-Series JSON Snapshots] ── dashboard_data_{YYYY-MM-DD}.json
        |
        v
[FastAPI Dashboard :8080] ── AJAX polling every 10 seconds
        |
        v
[Dark-mode Web UI] ── health matrix, alerts, alarm trail
```

Three concurrent services run at all times (via supervisord or run_monitor.py):
1. **Extraction** -- vcom_monitor.py (Playwright scraper)
2. **Analyser** -- processor_analyser.py (file monitor + forensic engine)
3. **Dashboard** -- dashboard/app.py (FastAPI server on port 8080)

---

## Plant Topology

- **3 Transformers**: TX1, TX2, TX3
- **36 Inverters**: TX1-01 through TX1-12, TX2-01 through TX2-12, TX3-01 through TX3-12
- **808 Strings**: as shown in table below:
CAMPO FV	"SOTTOCAMPI
INVERTER  BOX"	Numero tot. stringhe per inverter
T1	1.1	22
T1	1.2	22
T1	1.3	22
T1	1.4	22
T1	1.5	22
T1	1.6	22
T1	1.7	22
T1	1.8	23
T1	1.9	23
T1	1.10	23
T1	1.11	23
T1	1.12	22
T2	2.1	21
T2	2.2	22
T2	2.3	22
T2	2.4	21
T2	2.5	21
T2	2.6	23
T2	2.7	22
T2	2.8	23
T2	2.9	23
T2	2.10	22
T2	2.11	23
T2	2.12	23
T3	3.1	23
T3	3.2	23
T3	3.3	23
T3	3.4	23
T3	3.5	23
T3	3.6	23
T3	3.7	23
T3	3.8	23
T3	3.9	23
T3	3.10	23
T3	3.11	23
T3	3.12	21
TOTALI		808




- **14 Environmental Sensors**: JB-SM1_AL-1-DOWN, JB-SM1_AL-1-UP, JB-SM3_AL-3-DOWN, JB-SM3_AL-3-UP, JB-SM3_GHI-3, JB1_GHI-1, JB1_IT-1-1, JB1_IT-1-2, JB1_POA-1, JB2_IT-2-1, JB2_IT-2-2, JB3_IT-3-1, JB3_IT-3-2, JB3_POA-3
- **Excluded**: SunGrow SG350HX (explicitly deselected in every extraction cycle)

---

## Data Extraction Methods (6 Metrics)

Every 10 minutes, a Playwright-driven browser session authenticates to VCOM and scrapes 6 metric categories. Each extraction follows a common workflow:

1. Navigate to the metric tab by text locator
2. Toggle "Valori in minuti" (minute-resolution values) to ACCESO
3. Click "Aggiorna grafico" (refresh chart)
4. Switch to the "Dati" (Data) tab
5. Wait for table rows to render
6. Extract headers (filtering out SunGrow columns)
7. Iterate rows, converting Italian number formats (`.` thousands separator, `,` decimal) to floats
8. Return a pandas DataFrame with a `Timestamp Fetch` column (HH:MM:SS)

### 1. Performance Ratio (PR)
- **Source tab**: "PR inverter"
- **Target table**: `table#measuredValues tbody tr`
- **Output columns**: Inverter name, PR value (0-1 scale or 0-100 scale; normalized during analysis)
- **Output file**: `PR_{YYYY-MM-DD}.xlsx`
- **Significance**: Primary KPI for inverter efficiency

### 2. AC Power (Potenza AC)
- **Source tab**: "Potenza AC"
- **Target table**: `#infotab-data table tbody tr`
- **Output columns**: Ora (time), one column per inverter (Watts)
- **Output file**: `Potenza_AC_{YYYY-MM-DD}.xlsx`
- **Significance**: Core production metric; used as the merge reference for all analyses

### 3. DC Current (Corrente DC)
- **Source tab**: "Corrente DC"
- **Target table**: `#infotab-data table tbody tr`
- **Output columns**: Ora (time), per-string DC current per inverter (Amps)
- **Output file**: `Corrente_DC_{YYYY-MM-DD}.xlsx`
- **Significance**: String-level fault detection (broken strings, shading, PID)

### 4. Insulation Resistance
- **Source tab**: "Resistenza di isolamento"
- **Target table**: `#infotab-data table tbody tr`
- **Output columns**: Ora (time), resistance per inverter (kOhm)
- **Output file**: `Resistenza_Isolamento_{YYYY-MM-DD}.xlsx`
- **Significance**: Ground fault precursor detection

### 5. Inverter Temperature
- **Source tab**: "Temperatura"
- **Target table**: `#infotab-data table tbody tr`
- **Output columns**: Ora (time), temperature per inverter (degrees C)
- **Output file**: `Temperatura_{YYYY-MM-DD}.xlsx`
- **Significance**: Thermal derating and overheating detection

### 6. Irradiance (Environmental Sensors)
- **Source tab**: "Irraggiamento"
- **Target table**: `#infotab-data table tbody tr`
- **Output columns**: Ora (time), 14 named sensor readings (W/m2, degrees C for IT sensors)
- **Output file**: `Irraggiamento_{YYYY-MM-DD}.xlsx`
- **Significance**: Context for production expectations; GHI/POA/albedo for performance benchmarking

---

## Data Processing Pipeline

### Cleaning (clean_data)
- Strip `Timestamp Fetch` column before analysis
- Convert all object (string) columns to numeric: remove `.` thousands separator, replace `,` with `.` for decimal, then `pd.to_numeric(errors='coerce')`
- Preserve `Ora` and `DateTime` columns as-is

### Time Normalization (to_hours)
- Convert HH:MM or HH:MM:SS strings to float hours (e.g., 14:30 becomes 14.5)
- Used for daylight window enforcement in all analysis rules

### Merge Strategy
- **Potenza AC** serves as the master reference DataFrame
- All other metrics are left-joined on `Ora` (time) column
- Suffix convention: `_PR`, `_TEMP`, `_RES`, `_DC` for disambiguation
- Irradiance merged separately (sensor names don't overlap with inverter IDs)

---

## Forensic Analysis Rules (Incident Detection)

The analyzer scans every inverter at every time-step in the merged DataFrame. Rules are evaluated in priority order; only the first matching rule fires per inverter per time-step.

### Rule 1: Low Performance Ratio
- **Condition**: PR < 0.85 (or < 85 if on 0-100 scale) during daylight hours (09:00-17:00)
- **Severity**: Critical
- **Detail**: Reports the actual PR value
- **Rationale**: PR below 85% during peak sun indicates degradation, soiling, or electrical fault

### Rule 2: High Operating Temperature
- **Condition**: Temperature > 40 degrees C (any hour)
- **Severity**: Warning
- **Detail**: Reports the measured temperature
- **Rationale**: Sustained high temperatures accelerate component aging and trigger thermal derating

### Rule 3: DC String Failure
- **Condition**: Any DC string current < 0.2A while the inverter is producing > 500W
- **Severity**: Critical
- **Detail**: Identifies the specific failing string column
- **Rationale**: Near-zero current on a string while the inverter is active indicates a broken string, blown fuse, or severe shading

### Rule 4: Power Yield Deviation
- **Condition**: Inverter AC power deviates > 3% from the site median, when site median > 5000W
- **Severity**: Critical
- **Detail**: Reports the deviation percentage
- **Rationale**: Significant underperformance relative to peers at meaningful production levels signals inverter-specific issues

### Rule 5: Communication Loss
- **Condition**: AC power reading is NaN (no data) during daylight hours (07:00-19:00)
- **Severity**: High
- **Detail**: "No data stream"
- **Rationale**: Missing telemetry during operating hours indicates SCADA communication failure

### Rule 6: Inverter Trip
- **Condition**: AC power equals 0W while site median > 2000W during daylight hours (07:00-19:00)
- **Severity**: Critical
- **Detail**: "Zero production detected"
- **Rationale**: An inverter producing nothing while the plant is active has tripped or faulted

### Alert Deduplication
- Incidents are sorted by (inverter, type, timestamp)
- Consecutive identical alerts (same inverter + same type) within 1 hour are collapsed to a single alert
- Maximum 50 anomalies retained per analysis snapshot

---

## Health Flag System (Per-Inverter Color Matrix)

Computed from the latest row of merged data. Each inverter receives 4 independent flags plus an overall status.

### PR Flag
| Color  | Condition          |
|--------|--------------------|
| Green  | PR >= 85%          |
| Yellow | 75% <= PR < 85%    |
| Red    | PR < 75%           |
| Grey   | No data available  |

### Temperature Flag
| Color  | Condition          |
|--------|--------------------|
| Green  | Temp <= 40 C       |
| Yellow | 40 C < Temp <= 45 C|
| Red    | Temp > 45 C        |
| Grey   | No data available  |

### DC Current Flag (Relative)
| Color  | Condition                                  |
|--------|--------------------------------------------|
| Green  | Inverter DC mean >= 15% of site DC median  |
| Red    | Inverter DC mean < 15% of site DC median   |
| Grey   | Night (site DC median <= 0) or no data     |

### AC Power Flag (Relative)
| Color  | Condition                                  |
|--------|--------------------------------------------|
| Green  | Inverter AC >= 97% of site AC median       |
| Red    | Inverter AC < 97% of site AC median        |
| Grey   | Night (site AC median <= 0) or no data     |

### Overall Status
- The worst non-grey flag determines the overall status
- Priority: Red > Yellow > Green
- If all four flags are grey, overall status is grey

---

## Dashboard Output Structure

### Macro Health (Plant-Wide Summary)
```json
{
  "total_inverters": 36,
  "online": <count with AC power > 0>,
  "tripped": <count with AC power == 0>,
  "comms_lost": <count with NaN AC power>
}
```

### Per-Inverter Health
```json
{
  "INV TX1-01": {
    "pr": "green",
    "temp": "yellow",
    "dc_current": "green",
    "ac_power": "red",
    "overall_status": "red"
  }
}
```

### Anomaly Records
```json
{
  "timestamp": "2026-03-30 14:45",
  "inverter": "INV TX1-05",
  "type": "Low Performance Ratio",
  "severity": "Critical",
  "details": "PR: 0.72%"
}
```

### File Ingestion Status
Tracks the success/pending state and last sync time for each of the 6 metric files:
PR, Potenza_AC, Resistenza_Isolamento, Temperatura, Corrente_DC, Irraggiamento

---

## Dashboard UI Sections

1. **Metrics Grid**: Total inverters, online count, tripped count, comms lost count
2. **Data Ingestion Status**: 6 file cards with loading/success/pending indicators and last sync timestamps
3. **Inverter Health Matrix**: 36 inverter cards in a responsive grid, each showing 4 color-coded LED dots (PR, Temp, DC, AC) with an overall border color
4. **Active Diagnostic Alerts**: Real-time anomaly list from the latest analysis snapshot, color-coded by severity
5. **Historical Alarm Trail**: Chronological table of all anomalies across the entire day (up to 100 entries, newest first)

---

## Data Retention and Lifecycle

- **Excel files**: One file per metric per day, append-only. Retained indefinitely.
- **Dashboard JSON**: One file per day (`dashboard_data_{YYYY-MM-DD}.json`). Time-series of snapshots keyed by timestamp. Automatically purged after 7 days.
- **Error screenshots**: Saved to `errors/` directory on login or extraction failures.
- **Logs**: `monitoring.log` (extraction), `analysis.log` (watchdog/analytics).

---

## Operational Parameters

| Parameter                    | Value            |
|------------------------------|------------------|
| Extraction interval          | 10 minutes       |
| Dashboard polling interval   | 10 seconds       |
| File write stabilization     | 5 seconds        |
| Retry attempts per metric    | 2                |
| JSON retention               | 7 days           |
| Max anomalies per snapshot   | 50               |
| Alert dedup window           | 1 hour           |
| Browser viewport             | 1450 x 900       |
| Dashboard port               | 8080             |

---

## Deployment

- **Docker image**: `mcr.microsoft.com/playwright/python:v1.40.0-jammy`
- **Process manager**: supervisord (3 services: extraction, watchdog, dashboard)
- **Local mode**: `python run_monitor.py` spawns all 3 as subprocesses with auto-restart on crash (5-second cooldown)
- **Dependencies**: pandas, numpy, openpyxl, watchdog, playwright, fastapi, uvicorn, jinja2, python-multipart
