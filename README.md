# VCOM Automation — Mazara SCADA Monitoring Pipeline

A complete automated monitoring system for solar photovoltaic (PV) plants. This project extracts real-time telemetry from VCOM (meteocontrol.com) every 10 minutes, performs forensic health analysis on 36 inverters, and serves a live dark-mode dashboard.

**System Status:** ✅ Production-ready — Memory-efficient, tested with real Mazara del Vallo plant data.

---

## 🎯 Quick Start

### Prerequisites
- **Python 3.9+** (tested on 3.10, 3.11, 3.12, 3.14)
- **Windows** (native batch scripting; Linux/macOS may require path adjustments)
- **Network access** to meteocontrol.com and a writable network share (or local `extracted_data/`)

### Installation

```bash
# Clone the repository
git clone https://github.com/MuhammadAbbasi/VCOM-Automation.git
cd VCOM-Automation

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Create .env file with credentials
cp .env.example .env
# Edit .env and add VCOM_USER, VCOM_PASS, VCOM_SYSTEM_ID
```

### Run the System

```bash
# Start all three services (extraction, analysis, dashboard)
python run_monitor.py
```

Then open your browser:
```
http://localhost:8080
```

---

## 📋 What This Does

### 1. **Extraction Pipeline** (`vcom_monitor.py`)
Logs into VCOM every 10 minutes and scrapes 6 metrics:
- **PR** (Performance Ratio) — inverter efficiency
- **Potenza AC** — AC power output per inverter
- **Corrente DC** — DC string current (12 strings × 36 inverters)
- **Temperatura** — inverter temperature
- **Resistenza Isolamento** — insulation resistance
- **Irraggiamento** — irradiance from 14 environmental sensors

**Output:** Daily Excel files in `extracted_data/`

### 2. **Forensic Analysis** (`processor_watchdog_final.py`)
Watches for new extraction files and analyzes health:
- Scans for 6 anomaly types (Low PR, High Temp, DC String Failure, Power Deviation, Comms Loss, Inverter Trip)
- Computes 4 health LEDs per inverter: PR / Temperature / DC Current / AC Power
- Generates real-time JSON snapshots for the dashboard
- **Memory-efficient:** Uses on-demand lookups instead of massive DataFrame merges

### 3. **Live Dashboard** (`dashboard/app.py`)
Dark-mode web UI on port 8080:
- **Metrics Grid** — Total/Online/Tripped/Comms Lost counts
- **Data Status** — Which extraction files are ready
- **36-Inverter Health Matrix** — 4 LED dots per inverter
- **Active Alerts** — Color-coded anomalies (critical/high/warning/info)
- **Alarm Trail** — Historical events (newest first)

**Polling:** AJAX every 10 seconds for real-time updates

---

## 🏗️ Architecture

```
VCOM Automation/
├── vcom_monitor.py                    ← Extraction loop (10-min cycle)
├── extraction_code/                   ← 6 metric scrapers (sync-Playwright)
│   ├── base_monitor.py                ← Shared login, nav, helpers
│   ├── pr_monitor.py
│   ├── potenza_ac_monitor.py
│   ├── corrente_dc_monitor.py
│   ├── resistenza_monitor.py
│   ├── temperatura_monitor.py
│   └── irraggiamento_monitor.py
├── processor_watchdog_final.py        ← Forensic analyzer (ACTIVE)
├── processor_watchdog*.py             ← Legacy versions (reference only)
├── dashboard/
│   ├── app.py                         ← FastAPI server
│   └── static/
│       ├── index.html
│       ├── app.js
│       └── style.css
├── run_monitor.py                     ← Orchestrator (all 3 services)
├── extracted_data/                    ← Generated at runtime
│   ├── PR_YYYY-MM-DD.xlsx
│   ├── Potenza_AC_YYYY-MM-DD.xlsx
│   ├── ... (4 more metrics)
│   └── dashboard_data_YYYY-MM-DD.json
└── requirements.txt
```

**Data Flow:**
```
VCOM (meteocontrol)
  ↓
vcom_monitor.py (Playwright scraper)
  ↓
extracted_data/*.xlsx (daily rolling files)
  ↓
processor_watchdog_final.py (file watcher + analyzer)
  ↓
dashboard_data_YYYY-MM-DD.json (JSON snapshots)
  ↓
dashboard/app.py (FastAPI)
  ↓
http://localhost:8080 (dark-mode UI)
```

---

## ⚙️ Configuration

### `.env` File (REQUIRED)

```env
# VCOM Credentials
VCOM_USER=your_username
VCOM_PASS=your_password
VCOM_SYSTEM_ID=2144635

# Optional: Custom URLs (defaults to production VCOM)
VCOM_URL=https://vcom.meteocontrol.com/vcom/
DASHBOARD_PORT=8080
```

**Security Note:** `.env` is in `.gitignore` — never commit credentials.

### Health Thresholds (`processor_watchdog_final.py`)

Adjust these constants to tune alerting:

```python
# Line ~58 in processor_watchdog_final.py
PR_THRESHOLD = 85.0           # % (normalize to 0-100)
TEMP_CRITICAL = 45.0          # °C
TEMP_WARNING = 40.0           # °C
AC_HEALTHY_MIN = 5000         # W (during daylight)
DAYLIGHT_START = 7.0          # hours (07:00)
DAYLIGHT_END = 19.0           # hours (19:00)
```

### Time-Aware DC Thresholds

DC current expectations vary by time of day:
- **Morning (07:00–12:00):** Green ≥10A, Yellow ≥2A
- **Afternoon (12:00–19:00):** Green ≥5A, Yellow ≥0.5A
- **Off-hours:** Grey (no generation expected)

This prevents false alerts for normal late-afternoon power decline.

---

## 📊 Dashboard Colors & Meanings

### LED Status
- 🟢 **Green** — Healthy (all metrics within thresholds)
- 🟡 **Yellow** — Warning (minor degradation, normal for late afternoon)
- 🔴 **Red** — Critical (failure condition, requires action)
- ⚫ **Grey** — No data available (offline or out-of-hours)

### Inverter Health Matrix
Each inverter shows 4 dots:
1. **PR** — Performance ratio (0–100%)
2. **Temp** — Temperature (°C)
3. **DC Current** — String current (A/MPPT, time-dependent)
4. **AC Power** — Output power (W)

**Overall Status** — Worst of the 4 metrics

### Alert Severity Badges
- 🔴 **Critical** — Action required (Low PR during peak, Inverter Trip, DC String Failure)
- 🟠 **High** — Watch closely (Communications loss during daylight)
- 🟡 **Warning** — Informational (High temperature, normal afternoon decline)
- 🔵 **Info** — Tracking (Normal off-hours transition)

---

## 📈 Forensic Rules

The watchdog applies 6 rules in priority order:

| Rule | Condition | Severity |
|------|-----------|----------|
| **Low PR** | PR < 85% during 09:00–17:00 | 🔴 Critical |
| **High Temp** | Temperature > 40°C | 🟡 Warning |
| **DC String Failure** | String current < 0.2A while AC > 500W | 🔴 Critical |
| **Power Yield Deviation** | AC deviates >3% from site median (when median >5kW) | 🔴 Critical |
| **Comms Loss** | AC is NaN during 07:00–19:00 | 🟠 High |
| **Inverter Trip** | AC = 0W while site median >2kW during daylight | 🔴 Critical |

Consecutive alerts on the same inverter/rule within 1 hour are deduplicated.

---

## 🔧 Usage & Troubleshooting

### Starting the System

```bash
python run_monitor.py
```

**Output:**
```
============================================================
   [ORCHESTRATOR] Mazara SCADA Monitor System Control
============================================================
[*] Root Directory: \\S01\get\...\VCOM Automation
[*] Launching WATCHDOG (Forensic Analysis)...
[*] Launching EXTRACTION (VCOM Browser Automation)...
[*] DASHBOARD must be run separately: 'python dashboard/app.py'
------------------------------------------------------------
[ORCHESTRATOR] Started WATCHDOG (pid=12345)
[ORCHESTRATOR] Started EXTRACTION (pid=12346)
```

### Logs

Check real-time logs:
```bash
# Extraction logs (browser automation)
tail -f monitoring.log

# Watchdog logs (analysis)
tail -f watchdog.log

# Dashboard logs (FastAPI)
# (outputs to console)
```

### Common Issues

**Issue:** Browser doesn't open VCOM login page
- **Fix:** Check network connectivity. Verify `VCOM_URL` in `.env` is reachable.

**Issue:** "Valori minimi non disponibili" popup blocks extraction
- **Fix:** This is normal — the code automatically dismisses it. Wait 2–3 seconds for data to load.

**Issue:** Dashboard shows all grey LEDs
- **Fix:** Normal during off-hours (19:00–07:00). Check that `extracted_data/` contains today's Excel files.

**Issue:** Memory usage grows over time
- **Fix:** Logs and old JSON files accumulate. Manually clean `extracted_data/` files older than 7 days.

**Issue:** Port 8080 already in use
- **Fix:** Change `DASHBOARD_PORT=8080` in `.env` or kill the process: `lsof -ti :8080 | xargs kill -9`

---

## 📚 Documentation

| File | Purpose |
|------|---------|
| `ANALYSIS_FIX_SUMMARY.md` | Problem/solution analysis, thresholds, and migration guide |
| `DATA_STRUCTURE_AND_ANALYSIS.md` | Comprehensive data format docs for all 6 metrics |
| `analysis_method.md` | Forensic rule definitions and implementation details |
| `SYSTEM_PROMPT.md` | Plant topology (36 inverters, 14 sensors, string mapping) |
| `README.md` | This file |

---

## 🚀 Performance & Optimization

### Extraction Cycle
- **Duration:** ~2–5 minutes per 10-minute cycle
- **Data Format:** Excel (openpyxl append mode)
- **CSV Conversion:** Automatic (Excel→CSV for faster analysis)

### Analysis
- **Memory:** ~200–400 MB (no massive merges)
- **Duration:** <5 seconds per analysis run
- **Method:** Potenza_AC master + on-demand metric lookups

### Dashboard
- **Polling Interval:** 10 seconds (AJAX)
- **Response Time:** <100ms (JSON read + render)
- **Supported Browsers:** Chrome, Firefox, Safari, Edge (dark mode compatible)

---

## 🔐 Security

- **Credentials:** Stored in `.env` (git-ignored)
- **Sensitive Data:** Excel/CSV files stored in `extracted_data/` (git-ignored)
- **Dashboard:** Local-only (port 8080, no auth required — use firewall rules for production)
- **Browser Automation:** Headless Chromium, screenshots saved to `errors/` on failure

**For production deployment:**
1. Use HTTPS reverse proxy (nginx, Apache)
2. Add authentication (e.g., Basic Auth, OAuth)
3. Restrict network access to internal subnets
4. Implement log rotation and archival

---

## 📋 Plant Topology (Mazara del Vallo)

- **System ID:** 2144635
- **Inverters:** 36 total (TX1-01 through TX3-12)
- **Topology:** 3 transformers (TX1, TX2, TX3), 12 inverters each
- **DC Strings:** 12 MPPT channels per inverter
- **Environmental Sensors:** 14 (irradiance, temperature, etc.)
- **Excluded Devices:** SunGrow SG350HX (filtered in extraction)

---

## 🛠️ Development & Contributing

### File Versions
| File | Status | Use Case |
|------|--------|----------|
| `processor_watchdog_final.py` | ✅ ACTIVE | Production analyzer |
| `processor_watchdog.py` | ⚠️ Deprecated | Legacy reference |
| `processor_watchdog_v2/v3.py` | ❌ Archived | Old attempts, do not use |

### Adding a New Metric

1. Create `extraction_code/new_metric_monitor.py`
2. Import `base_monitor` helpers
3. Implement `extract_new_metric(page) -> pd.DataFrame`
4. Add to `METRICS` list in `vcom_monitor.py`
5. Update watchdog rules in `processor_watchdog_final.py`

### Testing Locally

```bash
# Test extraction (single cycle)
python vcom_monitor.py

# Test analysis (on existing data)
python processor_watchdog_final.py

# Test dashboard (standalone)
cd dashboard && python app.py
```

---

## 📞 Support & Issues

- **Reference Implementation:** https://github.com/MuhammadAbbasi/SCADA_monitoring_automation
- **VCOM Platform:** https://vcom.meteocontrol.com
- **Playwright Docs:** https://playwright.dev/python/

For bugs, feature requests, or questions, open an issue on GitHub.

---

## 📄 License

This project is provided as-is. Adapt and use freely, but ensure compliance with VCOM's terms of service and local regulations for SCADA monitoring.

---

**Last Updated:** 2026-04-02
**System Status:** ✅ Fully operational with real Mazara plant data
