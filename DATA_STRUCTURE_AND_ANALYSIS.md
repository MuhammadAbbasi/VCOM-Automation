# VCOM Extracted Data Structure & Analysis Guide

## Overview

The VCOM automation system extracts 6 metrics every 10 minutes from the Mazara del Vallo 36-inverter plant. Each metric file has a distinct structure optimized for that measurement type.

**Data Format**: Excel (openpyxl) → CSV (for analysis)
**Reason**: CSV is 10-50x faster for forensic analysis than Excel. Conversion happens automatically during analysis.

---

## File Structures

### 1. PR (Performance Ratio) — `PR_YYYY-MM-DD.xlsx`

**Format**: Long (tidy format)
**Rows**: ~720 (20 extractions × 36 inverters)
**Columns**: 3

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp Fetch` | string | Extraction time (HH:MM:SS) |
| `Inverter` | string | Inverter ID (e.g., "INV TX1-01") |
| `PR` | float | Performance ratio (0.0–1.0 or 0–100 scale) |

**Sample rows**:
```
Timestamp Fetch | Inverter     | PR
11:18:13        | INV TX1-12   | 1.66
11:18:13        | INV TX1-09   | 70.64
11:19:25        | INV TX1-12   | 2.34
```

**Why long format?**
PR is a summary metric (one value per inverter per extraction), not a time series. Long format is natural fit.

**Analysis use**:
Pivot to wide (Inverter → columns) to match other metrics for rule evaluation.

---

### 2. Potenza AC (AC Power) — `Potenza_AC_YYYY-MM-DD.xlsx`

**Format**: Wide time series
**Rows**: ~23,040 (all minutes in 16 days)
**Columns**: 38 (Timestamp Fetch, Ora, + 36 inverters)

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp Fetch` | string | Latest extraction time for this row |
| `Ora` | float | Time in hours (0.0–23.99, decimal) |
| `Potenza AC (INV TX1-01) [W]` | float | AC output power, Watts |
| ... | float | (35 more inverter columns) |
| `Potenza AC (INV TX3-12) [W]` | float | Last inverter |

**Sample row**:
```
Timestamp Fetch | Ora | Potenza AC (INV TX1-01) [W] | ... | Potenza AC (INV TX3-12) [W]
11:18:54        | 0.00| 0.0                         | ... | 0.0
11:18:54        | 0.01| 0.0                         | ... | 0.0
```

**Data characteristics**:
- Collected every 1 minute
- Ranges 0–50,000 W per inverter
- Off-hours (00:00–06:59, 19:00–23:59): typically 0.0 W
- Peak hours (09:00–17:00): 2000–15,000 W per inverter
- Missing data: NaN (pandas reads as float NaN)

**Analysis use**:
- Master time series (join point for all rules)
- Rule 4: Power deviation detection (compare each inverter to site median)
- Rule 6: Inverter trip detection (AC = 0 while site generating)
- Threshold: 500 W for string failure, 2000 W for trip, 5000 W for deviation

---

### 3. Corrente DC (DC Current) — `Corrente_DC_YYYY-MM-DD.xlsx`

**Format**: Wide time series (ultra-wide)
**Rows**: ~11,520 (fewer entries than AC, possibly lower resolution)
**Columns**: 434 (Timestamp Fetch, Ora, + 12 MPPT trackers × 36 inverters = 432)

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp Fetch` | string | Extraction time |
| `Ora` | float | Time in hours |
| `Corrente DC MPPT 1 (INV TX1-01) [A]` | float | MPPT 1 current, Amps |
| `Corrente DC MPPT 2 (INV TX1-01) [A]` | float | MPPT 2 current, Amps |
| ... | float | (432 MPPT columns total) |

**Data characteristics**:
- 12 MPPT trackers per inverter
- Off-hours: 0.0 A
- Peak hours: 5–30 A per MPPT
- String failure detection: if any MPPT < 0.2 A while AC > 500 W

**Analysis use**:
- Rule 3: DC String Failure (low current on one MPPT while inverter generating)
- Calculation: Average all 12 MPPTs per inverter to get inverter-level DC current
- Comparison: Check each MPPT against site median DC current

---

### 4. Temperatura (Inverter Temperature) — `Temperatura_YYYY-MM-DD.xlsx`

**Format**: Wide time series
**Rows**: ~20,160
**Columns**: 38

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp Fetch` | string | Extraction time |
| `Ora` | float | Time in hours |
| `Temperatura inverter (INV TX1-01) [°C]` | float | Inverter case temp, Celsius |
| ... | float | (35 more inverters) |

**Data characteristics**:
- Ranges 15–50 °C typically
- Correlates with solar irradiance
- Sample: 23.3°C, 24.4°C, 25.8°C (morning) → peaks 35–45°C at midday

**Analysis use**:
- Rule 2: High Temperature warning (>40°C) and critical (>45°C)
- Threshold: 40°C = yellow LED, 45°C = red LED

---

### 5. Resistenza Isolamento (Insulation Resistance) — `Resistenza_Isolamento_YYYY-MM-DD.xlsx`

**Format**: Wide time series
**Rows**: ~20,160
**Columns**: 38

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp Fetch` | string | Extraction time |
| `Ora` | float | Time in hours |
| `Resistenza di isolamento (INV TX1-01) [kOhm]` | float | Insulation resistance, kΩ |
| ... | float | (35 more inverters) |

**Data characteristics**:
- Normal range: 50–100+ kOhm
- Sample values: 67, 63, 51, 76, 94 kOhm
- Used for inverter health monitoring (low values = insulation fault risk)

**Analysis use**:
- Currently not used in active rules (informational only)
- Could be extended: if < 50 kΩ = warning

---

### 6. Irraggiamento (Irradiance & Environment) — `Irraggiamento_YYYY-MM-DD.xlsx`

**Format**: Wide time series
**Rows**: ~20,160
**Columns**: 16

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp Fetch` | string | Extraction time |
| `Ora` | float | Time in hours |
| `Irraggiamento (JB-SM1_AL-1-DOWN) [W/m²]` | float | Albedo (reflected), W/m² |
| `Irraggiamento (JB-SM1_AL-1-UP) [W/m²]` | float | Albedo (upward) |
| `Irraggiamento (JB1_POA-1) [W/m²]` | float | Plane-of-Array (POA) irradiance |
| `Irraggiamento (JB1_GHI-1) [W/m²]` | float | Global Horizontal Irradiance |
| ... (14 sensors total) |

**Sensor locations**:
- **JB1**: Junction Box 1 (TX1 quadrant)
- **JB2**: Junction Box 2 (TX2 quadrant)
- **JB3**: Junction Box 3 (TX3 quadrant)
- **SM (Solar Meter)**: Albedo monitors
- **POA**: Plane-of-Array (tilted panel angle)
- **GHI**: Global Horizontal (flat surface reference)
- **IT**: Irradiance × Temperature sensors

**Data characteristics**:
- Off-hours: 0.0 W/m²
- Peak hours: 400–1000 W/m²
- Used for performance normalization

**Analysis use**:
- Context for Rule 4 (AC power deviation)
- Performance ratio context
- Currently not directly used in active rules

---

## Data Processing Pipeline

### Step 1: Load Excel → Convert to CSV

```python
# Input: PR_2026-04-02.xlsx (36 KB)
# Output: PR_2026-04-02.csv (12 KB)
# Speedup: 3x faster to read/write, lighter memory footprint

def excel_to_csv(excel_path, csv_path):
    df = pd.read_excel(excel_path)
    df.to_csv(csv_path, index=False)
```

**Benefits of CSV over Excel**:
- No openpyxl overhead
- Native pandas support (no special engine needed)
- ~70% smaller file size
- 10-50x faster for large files (especially Corrente_DC with 434 columns)
- Easy to inspect with text editors/command line

### Step 2: Clean Data

**PR (pivot from long → wide)**:
```python
pr_wide = pr_df.pivot_table(
    index="Timestamp Fetch",
    columns="Inverter",
    values="PR"
)
# Columns: pr_TX1-01, pr_TX1-02, ..., pr_TX3-12
```

**Numeric conversion** (Italian format):
```python
# Italian format: comma = decimal, period = thousands sep
# Example: "1.234,56" = 1234.56
# Pandas auto-detects when reading Excel, but CSV may need manual handling

df["Ora"] = pd.to_numeric(df["Ora"], errors="coerce")
df["Potenza AC (INV TX1-01) [W]"] = pd.to_numeric(df["Potenza AC (INV TX1-01) [W]"], errors="coerce")
```

### Step 3: Apply Forensic Rules

Each rule operates on a specific metric:

| Rule | Metric | Condition | Severity |
|------|--------|-----------|----------|
| 1. Low PR | PR | < 85% during 09:00–17:00 | Critical |
| 2. High Temp | Temperatura | > 40°C (warning), > 45°C (critical) | Warning/Critical |
| 3. DC String Failure | Corrente DC | Any MPPT < 0.2 A while AC > 500 W | Critical |
| 4. AC Power Deviation | Potenza AC | Deviation > 3% from median when site > 5 kW, 09:00–17:00 | Critical |
| 5. Comm Loss | Potenza AC | NaN during 07:00–19:00 | High |
| 6. Inverter Trip | Potenza AC | 0 W while site > 2 kW, 07:00–19:00 | Critical |

### Step 4: Deduplicate Anomalies

Collapse consecutive same-inverter/same-rule anomalies within 1 hour window.
Keep max 50 most recent anomalies.

```python
# Before: [Rule6_TX1-01 @ 09:00, Rule6_TX1-01 @ 09:01, Rule6_TX1-01 @ 09:02, ...]
# After:  [Rule6_TX1-01 @ 09:02]  (merged into one entry)
```

### Step 5: Compute Health Flags

For each inverter, assess 4 metrics:

**LED Color Mapping**:
```
GREEN  (#10b981): Healthy
YELLOW (#f59e0b): Warning
RED    (#ef4444): Critical
GREY   (#6b7280): No data / outside evaluation window
```

**PR LED**:
- Green: ≥ 85%
- Yellow: 75–84%
- Red: < 75%
- Grey: NaN or outside 09:00–17:00

**Temperature LED**:
- Green: ≤ 40°C
- Yellow: 40–45°C
- Red: > 45°C
- Grey: NaN

**DC Current LED**:
- Green: ≥ 85% of site median DC
- Yellow: 50–85% of median
- Red: < 50% of median
- Grey: NaN or outside daylight

**AC Power LED**:
- Green: ≥ 97% of site median AC
- Yellow: 85–97% of median
- Red: < 85% or 0 W during daylight
- Grey: NaN or outside 07:00–19:00

**Overall LED** = worst of 4 LEDs

### Step 6: Write JSON Snapshot

**Format**:
```json
{
  "2026-04-02 17:16": {
    "macro_health": {
      "total_inverters": 36,
      "online": 35,
      "tripped": 1,
      "comms_lost": 0,
      "last_sync": "2026-04-02T17:16:43"
    },
    "inverter_health": {
      "INV TX1-01": {
        "pr": "green",
        "temp": "green",
        "dc_current": "green",
        "ac_power": "green",
        "overall_status": "green"
      }
    },
    "active_anomalies": [
      {
        "timestamp": "17:15:42",
        "ora": 17.26,
        "inverter": "INV TX2-05",
        "rule_id": 6,
        "rule_name": "Inverter Trip",
        "severity": "critical",
        "description": "AC power = 0W while site median 8500W at 17:00",
        "value": 0.0
      }
    ]
  }
}
```

### Step 7: Write CSV Audit Trail (Optional)

**Format**: `anomalies_YYYY-MM-DD.csv`
```
timestamp,ora,inverter,rule_id,rule_name,severity,description,value
17:15:42,17.26,INV TX2-05,6,Inverter Trip,critical,AC power = 0W...,0.0
17:14:15,17.24,INV TX1-08,2,High Temperature,warning,Temperature 42.5°C > 40°C,42.5
```

---

## Data Quality Observations

### Expected Patterns

**Off-hours (00:00–06:59)**:
- Potenza AC: 0.0 W
- Corrente DC: 0.0 A
- Temperature: 15–25°C (night ambient)
- PR: NaN or undefined (no generation)

**Ramp-up (07:00–09:00)**:
- AC power: 0 → 5,000 W
- Temperature: 20°C → 30°C
- PR: Stabilizes
- DC current: Ramps up

**Peak (10:00–16:00)**:
- AC power: 8,000–20,000 W per inverter
- Temperature: 35–50°C
- PR: 0.8–1.0 (or 80–100%)
- DC current: 20–40 A per MPPT

**Ramp-down (16:00–19:00)**:
- AC power: 5,000 → 0 W
- Temperature: dropping
- DC current: dropping

### Common Issues & Fixes

**Issue**: PR values like 1.66, 70.64 (appear as percentages × 100 or ratios × 100)
**Fix**: Normalize to 0–1 scale: `pr_normalized = pr_raw / 100 if pr_raw > 1 else pr_raw`

**Issue**: Missing data at specific times (NaN columns)
**Fix**: Use `pd.fillna()` or `interpolate()` for interpolation, or mark as "grey" LED

**Issue**: Ora column type (sometimes string "11:18:54", sometimes float 0.00–24.00)
**Fix**: Convert with `pd.to_datetime()` if string, keep float if hours decimal

---

## CSV vs Excel vs JSON Decision

| Aspect | CSV | Excel | JSON |
|--------|-----|-------|------|
| Read Speed | Fast | Slow (openpyxl) | Medium |
| Write Speed | Fast | Slow | Medium |
| File Size | Small (70% smaller) | Large | Medium |
| Memory | Low | High | Medium |
| Numeric Types | Inferred | Preserved | String (requires parsing) |
| Multi-sheet | Not supported | Native | Must split files |
| Human readable | Yes (with editor) | No (binary) | Yes |
| Analysis Ready | Yes (direct pandas) | Yes (openpyxl) | Requires parsing |

**Recommendation**:
- **Store**: Excel (current) - compatible with VCOM system
- **Analyze**: Convert Excel → CSV on first read
- **Export**: JSON (snapshots) + CSV (audit trails)
- **Real-time**: CSV + JSON polling from dashboard

---

## Implementation Notes

### file_status.json

Tracks extraction success/failure per metric:
```json
{
  "2026-04-02": {
    "PR": {
      "status": "success",
      "timestamp": "2026-04-02T17:50:35"
    },
    "Potenza_AC": {
      "status": "success",
      "timestamp": "2026-04-02T17:37:57"
    }
  }
}
```

### Handling Missing Files

If a metric file is missing:
1. Load available files
2. Mark as "failed" in file_status
3. Gracefully handle NaN values in rules
4. Still compute health for other metrics
5. Log warning but continue

---

## Example: Full Analysis Cycle

**Input**: 6 Excel files for 2026-04-02
**Process**:
```python
# Load & convert
potenza_ac_csv = excel_to_csv("Potenza_AC_2026-04-02.xlsx")
pr_csv = excel_to_csv("PR_2026-04-02.xlsx")
# ... (4 more)

# Clean
potenza_ac = clean_potenza_ac(potenza_ac_csv)
pr_wide = pr_csv.pivot(...)

# Apply rules
anomalies = apply_forensic_rules(potenza_ac, pr_wide, temp, dc, ...)
anomalies = deduplicate_anomalies(anomalies)

# Compute health
health = {inv: compute_inverter_health(inv, ...) for inv in INVERTER_IDS}

# Export
write_json_snapshot(...)
write_csv_audit_trail(anomalies)
```

**Output**:
- `dashboard_data_2026-04-02.json` (2 KB, ~5 sec to parse on dashboard)
- `anomalies_2026-04-02.csv` (variable size, audit trail)
- `PR_2026-04-02.csv` (12 KB)
- `Potenza_AC_2026-04-02.csv` (280 KB)
- ... (4 more CSV files)

**Performance**: ~2–5 seconds for complete analysis of 1 day of data

---

## Next Steps

1. Run `processor_watchdog_v2.py` to analyze existing extracted data
2. Verify anomaly detection matches expected inverter behavior
3. Adjust rule thresholds based on plant operational history
4. Add email alerts when Critical anomalies detected
5. Archive old JSON/CSV files (> 7 days) for compliance
