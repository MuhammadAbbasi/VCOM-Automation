# Skill: Mazara VCOM Forensic Analysis

This skill provides instructions for analyzing Mazara PV plant data extracted from VCOM. Use this skill when you need to perform a deep dive into CSV datasets to identify underperformance or technical faults.

## 1. Context & Data Access
- **Root Directory**: The current workspace.
- **Data Source**: `extracted_data/` (Look for subfolders by date, e.g., `2026-04-15/`).
- **Configuration**:
    - `user_settings.json`: Contains performance thresholds.
    - `plant_configuration_original.csv`: Defines the 36x12 MPPT/String layout.
    - `config.json`: Contains Inverter IDs and system credentials.

## 2. Analysis Workflow

### Step 1: Metric Ingestion
Load the following CSVs from the daily folder:
- **Potenza_AC**: The "Master" timeline. Use the `Ora` column (decimal hours) as the time index.
- **PR**: Performance Ratio data.
- **Temperatura**: Inverter internal temperatures.
- **Corrente_DC**: All MPPT current readings.

### Step 2: Diagnostic Rules

#### Rule A: AC Power & Tripping
- **Condition**: If `AC_Power == 0` while `Irradiance > 50W/m²` for more than 9 minutes.
- **Severity**: Critical (Inverter Tripped).
- **Comparison**: Compare each inverter against the **Fleet Average**. If an inverter is < 85% of the fleet average, flag it as "Low AC Power."

#### Rule B: DC MPPT Normalization (CRITICAL)
- **Mapping**: Use `plant_configuration_original.csv` to identify if an MPPT has 0, 1, or 2 strings.
- **Normalization**: 
    - 2 Strings: Baseline current.
    - 1 String: Baseline / 2.
- **Faults**:
    - **Open Circuit**: Current < 10% of normalized expected value.
    - **Single String Loss**: Current is ~50% of expected (only for 2-string configs).

#### Rule C: Thermal Health
- **Yellow**: > 40°C.
- **Red**: > 45°C.
- **Logic**: Check if higher temperatures correlate with reduced AC power (Thermal Derating).

#### Rule D: PR (Performance Ratio)
- **Healthy**: &ge; 85%.
- **Warning**: &ge; 75%.
- **Critical**: < 75%.

### Step 3: Reporting
1.  **Generate Snapshot**: Produce a `dashboard_data_YYYY-MM-DD.json` file for the web UI.
2.  **Generate Markdown**: Create `mppt_analysis_report_YYYY-MM-DD.md` with the fault table.
3.  **Telegram**: If `telegram.enabled` is true in `user_settings.json`, send summaries of **new** Critical or Warning alarms.

## 3. Tool Usage Constraints
- Use `pandas` and `numpy` for data manipulation.
- Do NOT merge large CSV files. Use `Ora` lookups to compare data points at specific timestamps to save memory.
- Filter data to **Daylight Hours** (typically 07:00 to 19:00) before calculating averages.
