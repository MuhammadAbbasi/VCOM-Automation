# 🌞 Mazara VCOM Automation — AI-Powered SCADA Monitoring Pipeline

[![Local AI: Qwen 3.5 9B](https://img.shields.io/badge/Local%20AI-Qwen%203.5%209B-blueviolet)](https://ollama.com)
[![Status: Production](https://img.shields.io/badge/Status-Production--Ready-success)](#)

A complete, high-performance automated monitoring system for utility-scale solar photovoltaic (PV) plants. This project integrates **Local LLMs (Qwen 2.5 Coder)** for deep forensic analysis, extracts real-time telemetry from VCOM (meteocontrol.com) every 15 minutes, and serves a reactive, WebSocket-driven dark-mode dashboard.

> [!TIP]
> **AI-Search Ready:** This repository is optimized for LLM indexing (see [llms.txt](./llms.txt)).

**System Status:** ✅ Active Forensic Analysis | ✅ Remote AI Agent (High Speed) | ✅ Concurrent Telegram Bot | ✅ Production-Stable Orchestrator

---

## 🚀 Key Improvements (April 2026 Update)

Seamlessly integrates **Qwen 3.5 9B** via local Ollama (localhost) for plant diagnostics:
- **Deep CSV Correlation:** Automatically scans historic CSVs to verify startup behavior (e.g., "Early Hours" production checks).
- **Hardened Data Loading:** Custom `load_csv` helper with auto-column stripping and encoding detection (UTF-8/Latin-1) to handle SCADA formatting quirks.
- **Data Collision Shield:** Built-in retries and historical fallbacks to prevent crashes during concurrent file writes by the Watchdog.

### 📱 Multi-User Telegram Bot (`telegram_bot.py`)
- **Concurrency:** Fully multi-threaded; handles dozens of simultaneous AI requests without freezing.
- **Quick Shortcuts:** Instant commands like `/alerts`, `/daily`, and `/status`.
- **Instant Feedback:** Immediate "⏳ Thinking..." status while the local GPU processes complex logic.

- **Stable Reliability:** Hot-reload is controlled (semi-automated) to prevent excessive restarts during long extraction cycles, ensuring the browser session remains stable.

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

# Setup Configuration
cp config.json.example config.json
cp user_settings.json.example user_settings.json
# Edit config.json and user_settings.json with your credentials and preferences
```

### 📦 Migration Guide (Moving to another system)
1. **Copy Files**: Transfer the entire project folder to the new system.
2. **Environment**: Re-run the installation steps above.
3. **Data Preservation**: If you want to keep your history, ensure you copy the `extracted_data/` folder, specifically the `dashboard_data_*.json` files.
4. **Hardware**: Ensure the new system has at least 8GB RAM and stable network access for the browser automation.

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
- **PR** (Performance Ratio), **Potenza AC**, **Corrente DC**, **Temperatura**, **Resistenza Isolamento**, **Irraggiamento**.

**Universal Login & Session Shield:** Automatically handles both legacy VCOM login and modern Keycloak flows. Includes automated session-expiry detection and real-time Bootstrap modal dismissal (DOM-stripping method) to prevent extraction stalls.

### 2. **Forensic Analysis** (`processor_watchdog_final.py`)
- Scans for 6 anomaly types.
- **Downtime Filter:** Events < 9 minutes are automatically ignored to reduce noise.
- **Dynamic Daylight:** Detects plant start time from production data.

### 3. **Live Dashboard** (`dashboard/static/`)
- **Health Matrix:** 36-inverters × 4 LEDs (PR | Temp | DC | AC).
- **Downtime Tracker:** Tracks production interruptions based on user-configured duration limits.
- **Dynamic Configuration:** Front-end "⚙️ SETTINGS" modal saves configurations to `user_settings.json` across reboots.
- **Premium Mission Control UI:** A high-fidelity "Plant Reference Manual" footer providing:
  - **Technical Specifications:** Accurate site metadata (12.625 MWp, 808 strings, 3 TX stations).
  - **Diagnostic Matrix:** A color-coded SCADA guide with luminous LED status indicators.
  - **System Metadata:** Real-time visibility into the forensic engine and AI inference nodes.

**Data Push:** FastAPI **WebSockets** stream real-time JSON updates continuously without page reloads.

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
├── processor_watchdog_final.py        ← Forensic analyzer (ACTIVE v4.2)
├── processor_watchdog*.py             ← Legacy versions (reference only)
├── dashboard/
│   ├── app.py                         ← FastAPI server
│   └── static/
│       ├── index.html                 ← Premium Glassmorphism UI
│       ├── app.js
│       └── style.css                  ← Outfit Typography & Luminous Accents
├── run_monitor.py                     ← Orchestrator (all 3 services)
├── extracted_data/                    ← Generated at runtime
│   ├── PR_YYYY-MM-DD.xlsx
│   ├── Potenza_AC_YYYY-MM-DD.xlsx
│   ├── ... (4 more metrics)
│   ├── extraction_status.json         ← Real-time ingestion progress
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
dashboard/app.py (FastAPI background task broadasts JSON via WebSocket)
  ↓
http://localhost:8080 (Reactive dark-mode UI with dynamic settings)
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

### LED Status (PR, Temp, DC, AC)
- 🟢 **Green** — Healthy (all metrics within thresholds)
- 🟡 **Yellow** — Warning / Sub-optimal (e.g., thermal warning or slight DC deviation)
- 🔴 **Red** — Critical (e.g., inverter tripped or severe low PR)
- ⚪ **Slate Grey** — Communications Lost (Distinguished from warnings)
- ⚫ **Dark Grey** — Off-hours / No data

### Thresholds (Customizable via Dashboard Settings UI)
- **PR:** 🟢&ge;x% | 🟡&ge;y% | 🔴<y% (*active after 30m stabilization, handled dynamically*)
- **Temperature:** 🟢&le;x°C | 🟡&le;y°C | 🔴>y°C
- **AC Power:** Evaluated relatively: 🟢>95% Plant Avg | 🔴<95% Plant Avg. Exceptions granted for low-POA conditions (<50 W/m²).
- **DC Current:** Deep string deviations detected dynamically by checking internal MPPTs and domain-levels.

---

## 📈 Forensic Rules

The watchdog applies deep diagnostic rules in priority order:

| Rule | Condition | Severity |
|------|-----------|----------|
| **Low PR** | PR < thresholds after 30m stabilization period | 🔴 Critical |
| **High Temp** | Temperature > configured limit | 🔴 Critical |
| **DC String Loss** | String fault/open circuit/underperformance detected via dynamic MPPT comparison | 🔴/🟡 Fault/Warning |
| **Comms Loss** | Data missing (x) for entire component | 🟡 Warning |
| **Inverter Trip / AC Power Loss** | AC output deviates >5% below the plant average during nominal POA | 🔴 Critical |

Historical alarms feature a category drop-down filter, and consecutive alerts on the same inverter/rule are deduplicated dynamically.

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
- **Communication Channel:** Persistent FastAPI WebSocket
- **Response Time:** Real-time push logic immediately on payload build
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

**Last Updated:** 2026-04-22
**System Status:** ✅ Production-hardened with high-fidelity footer, verified site metadata (12.625 MWp), session protection, and premium dashboard UI.
