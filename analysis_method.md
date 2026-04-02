# Mazara Solar Plant — Forensic Analysis Method

## Document Purpose
This document defines the **data structure, analysis workflow, and forensic rules** used by `processor_watchdog.py` to detect anomalies and compute health status for the 36-inverter solar plant at Mazara del Vallo (System ID: 2144635).

---

## 1. INPUT DATA STRUCTURE

### 1.1 Six Daily Excel Files (Extracted every 10 minutes)

| Metric | File | Columns | Rows | Units | Significance |
|--------|------|---------|------|-------|--------------|
| **AC Power** | `Potenza_AC_{date}.xlsx` | Ora (time) + 36 inverters | 1440/day (1 per min) | Watts | Master reference; production KPI |
| **PR** | `PR_{date}.xlsx` | Inverter name + PR value | Variable | 0-1 or 0-100% | Efficiency vs baseline |
| **DC Current** | `Corrente_DC_{date}.xlsx` | Ora + 808 string columns | 1440/day | Amps | String-level fault detection |
| **Temperature** | `Temperatura_{date}.xlsx` | Ora + 36 inverters | 1440/day | °C | Thermal derating / overheating |
| **Insulation Resistance** | `Resistenza_Isolamento_{date}.xlsx` | Ora + 36 inverters | 1440/day | kOhm | Ground fault precursor |
| **Irradiance** | `Irraggiamento_{date}.xlsx` | Ora + 14 sensors | 1440/day | W/m² (GHI/POA) / °C (IT) | Context for baselines |

### 1.2 Data Characteristics

- **Time resolution**: 1-minute intervals (06:00 → 19:00 typical operation window)
- **Number of inverters**: 36 (TX1-01 → TX1-12, TX2-01 → TX2-12, TX3-01 → TX3-12)
- **Number of strings**: 808 (ranging 21–23 per inverter, unevenly distributed)
- **Italian number format**: decimal comma (`,`), thousands separator (`.`) → must convert to float
- **Night hours (00:00–06:00, 19:00–23:59)**: zero production expected; rules with daylight windows skip these

---

## 2. DATA LOADING & CLEANING PIPELINE

### 2.1 Load Phase
```
Load 6 Excel files by metric prefix:
  - Potenza_AC_{date}.xlsx  ← AC Power (MASTER)
  - PR_{date}.xlsx
  - Corrente_DC_{date}.xlsx
  - Resistenza_Isolamento_{date}.xlsx
  - Temperatura_{date}.xlsx
  - Irraggiamento_{date}.xlsx

If Potenza_AC file is missing → abort analysis
All others are optional (None is acceptable)
```

### 2.2 Clean Phase (per DataFrame)

1. **Drop internal timestamp**: Remove `Timestamp Fetch` column (extraction metadata)
2. **Identify time column**: Rename first time-like column to `Ora` for merge key
3. **Standardize `Ora` column**:
   - Convert to string: `df["Ora"].astype(str)`
   - Strip trailing `.0`: `.str.replace(".0", "")`
   - Trim whitespace: `.str.strip()`
   - Purpose: Prevent float vs string merge mismatches (e.g., 12.0 vs "12:00")
4. **Convert metric columns to numeric**:
   - Skip `Ora` column
   - Italian format: remove `.` (thousands sep) → replace `,` with `.` (decimal sep)
   - Force to float: `pd.to_numeric(..., errors="coerce")` → NaN for unparseable cells
5. **Standardize inverter column names**:
   - If column starts with `TX` (not `INV `), prepend: `TX1-01` → `INV TX1-01`
   - Ensures consistent matching against INVERTER_NAMES list

### 2.3 Merge Phase

**Master DataFrame**: Start with cleaned `Potenza_AC` (AC power is the reference)

**Left-join all other metrics on `Ora` column**:
- PR: columns renamed to `{inverter_name}_PR`
- Temperature: columns renamed to `{inverter_name}_TEMP`
- Insulation: columns renamed to `{inverter_name}_RES`
- Irradiance: columns renamed to `{sensor_name}_IRR` (14 sensors)
- DC Current: **NOT merged by time** (see Special Handling below)

**Special Handling for DC Current**:
- DC DataFrame has **808 string columns** (too wide for left-join)
- Stored separately; queried **row-by-row during anomaly detection**
- Column naming: `{inverter}_string_1`, `{inverter}_string_2`, etc.
- Used only for Rule 3 (DC String Failure detection)

**Merge condition check**:
- If another metric has < 1440 rows (e.g., PR summary = 36 rows), skip its merge
- Log: "Skipping time-join for PR: summary-style data found (36 rows)"

**Result**: One merged DataFrame with columns:
```
[Ora, INV TX1-01, INV TX1-02, ..., INV TX3-12,    # AC Power
 INV TX1-01_PR, INV TX1-02_PR, ...,               # PR
 INV TX1-01_TEMP, INV TX1-02_TEMP, ...,           # Temperature
 INV TX1-01_RES, INV TX1-02_RES, ...,             # Insulation
 JB-SM1_AL-1-DOWN_IRR, JB-SM1_AL-1-UP_IRR, ...,   # Irradiance sensors
 _hours]                                           # Derived: decimal hours
```

---

## 3. TIME WINDOW NORMALIZATION

### 3.1 Convert `Ora` to Decimal Hours

```python
def to_hours(t: str) -> float | None:
    # Input: "14:30:45" or "14:30"
    # Output: 14.5125 (14 + 30/60 + 45/3600)
    # Used for daylight window checks in rules
```

**Purpose**: Enable numeric comparisons for "between 09:00 and 17:00" checks
- 9.0 = 09:00 (start of peak sun)
- 17.0 = 17:00 (end of peak sun)
- 7.0 = 07:00 (early morning start)
- 19.0 = 19:00 (evening stop)

---

## 4. FORENSIC ANALYSIS RULES

### Overview
For **each row (timestep)** and **each inverter**:
1. Extract metrics from merged DataFrame
2. Evaluate rules **in priority order** (below)
3. **First matching rule fires** — only one anomaly per inverter per timestep
4. Continue to next inverter

### Rule 1: Low Performance Ratio (Critical)
```
Condition:  PR < 85% AND 09:00–17:00 (daylight peak)
Threshold:  PR < 0.85 (or < 85 if already 0–100 scale)
Severity:   Critical
Detail:     "PR: {pr_norm:.1f}%"
Rationale:  Below-85% during peak sun indicates soiling, shading, or electrical fault
```

### Rule 2: High Operating Temperature (Warning)
```
Condition:  Temperature > 40°C (any hour)
Threshold:  Temp > 40.0°C
Severity:   Warning
Detail:     "Temp: {temp_val:.1f}°C"
Rationale:  Sustained heat accelerates aging, triggers thermal derating
```

### Rule 3: DC String Failure (Critical)
```
Condition:  ANY string < 0.2 A AND inverter AC output > 500 W
Threshold:  String current < 0.2 A (near-zero)
Severity:   Critical
Detail:     "String {column_name}: {current:.3f}A"
Rationale:  Open-circuit string, blown fuse, or severe local shading
Lookup:     Query DC DataFrame for that timestamp; check all string columns for inverter
```

### Rule 4: Power Yield Deviation (Critical)
```
Condition:  |AC - Site_Median| / Site_Median > 3% AND Site_Median > 5000 W
Threshold:  Deviation > ±3% when plant is producing > 5 kW
Severity:   Critical
Detail:     "Deviation: {deviation*100:+.1f}%"
Rationale:  Underperformance vs peers at high production = inverter-specific issue
Calculation: site_median = median(all 36 inverters' AC at this timestep)
```

### Rule 5: Communication Loss (High)
```
Condition:  AC power = NaN (no data) AND 07:00–19:00 (operation window)
Threshold:  pd.isna(ac_val)
Severity:   High
Detail:     "No data stream"
Rationale:  Missing telemetry during operating hours = SCADA comms failure
```

### Rule 6: Inverter Trip (Critical)
```
Condition:  AC power = 0 W AND Site_Median > 2000 W AND 07:00–19:00
Threshold:  ac_val == 0.0 (exactly zero, not NaN)
Severity:   Critical
Detail:     "Zero production detected"
Rationale:  Tripped or faulted inverter while plant is active
```

---

## 5. HEALTH FLAG COMPUTATION

### Per-Inverter (Latest Row Only)

Computed from the most recent valid timestep. Four independent flags + one overall:

#### Flag 1: Performance Ratio (PR)
| Color | Range | Meaning |
|-------|-------|---------|
| 🟢 Green | PR ≥ 85% | Healthy efficiency |
| 🟡 Yellow | 75% ≤ PR < 85% | Acceptable degradation |
| 🔴 Red | PR < 75% | Significant loss |
| ⚫ Grey | No data | Missing PR data |

#### Flag 2: Temperature
| Color | Range | Meaning |
|-------|-------|---------|
| 🟢 Green | Temp ≤ 40°C | Normal operating range |
| 🟡 Yellow | 40°C < Temp ≤ 45°C | Warm, approaching limit |
| 🔴 Red | Temp > 45°C | Overheating risk |
| ⚫ Grey | No data | Missing sensor data |

#### Flag 3: DC Current (String-Level, Relative)
| Color | Condition | Meaning |
|-------|-----------|---------|
| 🟢 Green | Inv_DC_Mean ≥ 15% × Site_Median | Sufficient string current |
| 🔴 Red | Inv_DC_Mean < 15% × Site_Median | Weak/failing strings |
| ⚫ Grey | Site_Median ≤ 0 (night) or no data | Not applicable |

#### Flag 4: AC Power (Relative)
| Color | Condition | Meaning |
|-------|-----------|---------|
| 🟢 Green | Inv_AC ≥ 97% × Site_Median | Matching peers |
| 🔴 Red | Inv_AC < 97% × Site_Median | Underperforming |
| ⚫ Grey | Site_Median ≤ 0 (night) or no data | Not applicable |

#### Overall Status
Priority: **Red > Yellow > Green > Grey**
- If any flag is red → Overall = Red
- Else if any flag is yellow → Overall = Yellow
- Else if any flag is green → Overall = Green
- Else (all grey) → Overall = Grey

---

## 6. MACRO HEALTH (Plant-Wide)

Computed from latest row:
- **Total Inverters**: 36 (fixed)
- **Online**: Count where AC > 0 W
- **Tripped**: Count where AC = 0 W
- **Comms Lost**: Count where AC is NaN

---

## 7. ANOMALY DEDUPLICATION

### Goal
Avoid alert fatigue: collapse consecutive identical incidents within 1-hour window.

### Algorithm
1. Sort anomalies by (inverter, type, timestamp)
2. For each anomaly:
   - If same inverter + same type as previous anomaly AND
   - Time gap < 1 hour → skip (suppress duplicate)
   - Else → keep (include in output)
3. Retain max **50 most recent anomalies** (limit list size)
4. Store **100 historical anomalies** in dashboard JSON for "Alarm Trail" tab

---

## 8. OUTPUT: JSON SNAPSHOT

### File Format
```
dashboard_data_{YYYY-MM-DD}.json
{
  "HH:MM timestamp_1": {
    "macro_health": {
      "total_inverters": 36,
      "online": <count>,
      "tripped": <count>,
      "comms_lost": <count>,
      "last_sync": "2026-04-02T14:30:00"
    },
    "file_status": {
      "Potenza_AC": {"status": "success", "timestamp": "2026-04-02T14:30:00"},
      "PR": {"status": "pending", "timestamp": null},
      ...
    },
    "inverter_health": {
      "INV TX1-01": {"pr": "green", "temp": "yellow", "dc_current": "green", "ac_power": "red", "overall_status": "red"},
      ...
    },
    "active_anomalies": [
      {"timestamp": "14:25", "inverter": "INV TX2-05", "type": "High Operating Temperature", "severity": "Warning", "details": "Temp: 42.3°C"},
      ...
    ],
    "historical_trail": [
      // Last 100 anomalies across the day (newest first)
    ]
  },
  "HH:MM timestamp_2": { ... }  // Updated every ~10 min after extraction
}
```

---

## 9. EXECUTION FLOW

```
1. EXTRACT (every 10 min via vcom_monitor.py)
   └─> Write 6 Excel files to extracted_data/

2. WATCHDOG (processor_watchdog.py)
   └─> File system monitor detects new/modified .xlsx
   └─> Wait 5 sec for file write stabilization
   └─> Check if all 6 daily files exist
   └─> If YES:
       ├─> Load all 6 files
       ├─> Clean (Italian format, rename columns)
       ├─> Merge on Ora
       ├─> Apply 6 forensic rules (row by row, inverter by inverter)
       ├─> Dedup anomalies
       ├─> Compute health flags (latest row)
       ├─> Build macro health
       ├─> Write/append dashboard_data_{date}.json
       └─> Purge JSON snapshots > 7 days old

3. DASHBOARD (dashboard/app.py)
   └─> FastAPI server on port 8080
   └─> GET /api/status
       └─> Read latest snapshot from dashboard_data_{date}.json
       └─> Return JSON (or empty if no data yet)
   └─> Frontend (app.js)
       └─> Poll /api/status every 10 seconds
       └─> Update 5 sections: Macro, Ingestion, Matrix, Alerts, History
```

---

## 10. SPECIAL CONSIDERATIONS

### Night Hours & Daylight Windows
- Daylight windows vary by rule:
  - **Peak sun (09:00–17:00)**: Rules 1 (PR), 4 (Deviation)
  - **Operation window (07:00–19:00)**: Rules 5 (Comms), 6 (Trip)
  - **24/7**: Rule 2 (Temperature), Rule 3 (DC Strings)
- Rationale: Night hours have zero expected production → false alarms avoided

### SunGrow Exclusion
- SunGrow SG350HX inverter is **explicitly excluded** from extraction
- Column headers with "SunGrow" are filtered during merge
- Reason: Different MPPT curve, separate performance baseline

### PR Scale Normalization
- VCOM may report PR as 0–1 (e.g., 0.87) or 0–100 (e.g., 87%)
- Detected: if PR ≤ 1.0 → multiply by 100
- All rules use 0–100 scale internally

### DC Current as String Matrix
- **Not time-merged** due to width (808 columns)
- **Row-by-row lookup** during Rule 3 evaluation
- Column names: `{inverter}{suffix}` (e.g., `INV TX1-01_1`, `INV TX1-01_2`, ...)

---

## 11. ERROR HANDLING & EDGE CASES

| Case | Handling |
|------|----------|
| Missing Potenza_AC | Abort analysis (AC is master reference) |
| Missing other metrics | Continue; treat as None (optional) |
| Empty Excel file | Read successfully but 0 rows → no merge impact |
| NaN in metric | Treat as missing; rule conditions check `pd.isna()` |
| Italian format parse fail | Coerce to NaN via `errors="coerce"` |
| Ora column mismatch | Rename first time-like column to "Ora" |
| Inverter name mismatch | Add "INV " prefix if missing; standardize |
| No inverter columns found | Log warning; skip analysis (degenerate case) |

---

## 12. KEY METRICS & THRESHOLDS

| Metric | Threshold | Unit | Rule |
|--------|-----------|------|------|
| Performance Ratio | < 85%, > 75%, < 75% | % | 1 |
| Temperature | > 40°C (warn), > 45°C (crit) | °C | 2 |
| DC String Current | < 0.2 A | A | 3 |
| Power Deviation | > 3% | % of median | 4 |
| Site AC Threshold | > 5000 W | W | 4 |
| Site AC Active | > 2000 W | W | 6 |
| DC Current Threshold | 15% of median | % | Flag |
| AC Power Threshold | 97% of median | % | Flag |
| Daylight Start | 07:00 (Rules 5,6), 09:00 (Rule 1) | HH:MM | All |
| Daylight End | 19:00 (Rules 5,6), 17:00 (Rule 1) | HH:MM | All |

---

## 13. VERIFICATION CHECKLIST

Before deploying analysis to production:

- [ ] All 6 input files parsed without errors
- [ ] Italian number format handled (`,` → `.`, `.` removed)
- [ ] Ora column standardized to string; merge succeeds without type mismatches
- [ ] 36 inverter columns identified and named consistently
- [ ] 6 forensic rules execute in correct priority order
- [ ] Deduplication removes redundant same-inverter/same-type alerts within 1 hour
- [ ] Health flags (4 LEDs + overall) computed for all 36 inverters
- [ ] Macro health counts (online/tripped/comms) match inverter flag distribution
- [ ] JSON output is valid; readable by dashboard app.js
- [ ] Anomalies sorted (latest first) in historical trail
- [ ] Old JSON files (> 7 days) purged automatically

---

## 14. Future Enhancements

1. **Predictive Alerts**: Trend analysis (degrading PR over 7 days)
2. **String-Level Baselines**: Per-string current thresholds (vs global 0.2 A)
3. **Thermal Derating Model**: Adjust PR baseline per temperature
4. **Weather Correlation**: Benchmark against cloud cover / irradiance
5. **Multi-Day Patterns**: Seasonal efficiency curves
6. **Root Cause Classification**: Tie DC/Temperature/PR to shading vs electrical faults
7. **SLA Compliance Tracking**: % uptime, MTTR metrics
