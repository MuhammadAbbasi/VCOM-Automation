# VCOM Automation — Future Works

**Last updated:** 2026-06-16
**System:** Mazara del Vallo (12.625 MWp, 36 inverters)

This document lists possible improvements ordered roughly by priority and estimated value. Items marked ⚡ are considered high-impact with low implementation cost.

---

## 1. Telegram Bot

### 1.1 ⚡ Scheduled Daily Summary
Send an automatic end-of-day report at 19:00 local time with: total energy produced, average PR, worst-performing inverter, peak power, and any alarms that fired during the day. No user trigger required.

### 1.2 Weekly & Monthly Energy Report
Extend `/week` and add `/month` with bar-chart-style ASCII or an attached PNG plot (matplotlib → BytesIO → Telegram photo). Include energy vs. reference (GHI-corrected expected).

### 1.3 Curtailment Duration Tracking
Track and report how many hours per day the plant was under grid curtailment (grid_limit < 87%). Surface in `/daily` and the morning summary. Useful for discussions with the grid operator.

### 1.4 Alarm Escalation to Personal Chat
If a Critical alarm (inverter trip, comms loss > 30 min) is not acknowledged within N minutes, forward it to `TELEGRAM_PERSONAL_ID` (already in `.env`) as a direct push.

### 1.5 ⚡ Inline Alarm Acknowledge
Add `/ack <alarm_key>` command so operators can mark an alarm as acknowledged in `fault_state.json` from Telegram without touching the dashboard.

---

## 2. Forensic Analyzer (`processor_watchdog_final.py`)

### 2.1 ⚡ Insulation Resistance Trending
`Resistenza_Isolamento` is extracted but not yet analyzed for per-inverter degradation trends. Implement a rolling 7-day baseline and alert when resistance drops >20% from baseline — a precursor to ground faults.

### 2.2 String-Level DC Fault Localization
Current DC analysis flags inverters with low MPPT mean but does not identify which specific string (column) is faulty. Extend to flag individual string columns whose current is < 30% of the inverter's own MPPT median.

### 2.3 Clipping Detection
When POA > 900 W/m² and AC power is flat across multiple consecutive time-steps, the plant may be clipping at the inverter AC limit. Detect and log clipping events so they are excluded from PR penalization.

### 2.4 Soiling vs. Fault Differentiation
Correlate PR drops with GHI/POA ratio. If POA is lower than expected from GHI (high albedo or dust), flag as "possible soiling" rather than "fault", reducing false work orders.

### 2.5 Grid Limit Threshold Configurability
The 87% curtailment threshold is hardcoded. Move it to `user_settings.json` so it can be changed from the dashboard settings modal without a code deployment.

---

## 3. Dashboard (`dashboard/`)

### 3.1 ⚡ Grid Limit Status Widget
Add a visible "Grid Limit" widget to the top of the dashboard showing the current grid_limit %, a green/amber/red badge, and how long the curtailment has been active. Currently only visible in Telegram.

### 3.2 Power Curve Chart
Plot AC power vs. time of day for the current day (all 36 inverters as a band, plus plant total). Use Chart.js (already referenced in the codebase). Helps operators spot morning ramp issues and afternoon anomalies visually.

### 3.3 PR Trend (7-Day Sparkline)
Show a small 7-day PR trend sparkline per inverter on the health matrix card. Allows at-a-glance identification of slowly degrading inverters before they hit the alarm threshold.

### 3.4 Alarm Acknowledge in Dashboard
Add a "Dismiss" button per active alarm in the UI that calls a `/api/ack/{alarm_key}` endpoint, writing the acknowledgment to `fault_state.json`.

### 3.5 Mobile-Responsive Layout
The current health matrix assumes a wide screen. Add a responsive breakpoint for 768px so field technicians can use the dashboard from a phone.

---

## 4. Data Pipeline

### 4.1 ⚡ Log Rotation
`monitoring.log` and `watchdog.log` grow indefinitely. Implement `logging.handlers.RotatingFileHandler` (5 MB max, 3 backups) in both `vcom_monitor.py` and `processor_watchdog_final.py`.

### 4.2 Extraction Health Heartbeat
Write a `db/extraction_health.json` on every successful extraction cycle with: timestamp, duration, rows extracted per metric. The dashboard and Telegram can use this to detect stalled extraction earlier than the current 10-minute silence check.

### 4.3 Retry Budget Tracking
Currently retries are silent. Log per-metric retry counts to `extraction_status.json` so the dashboard can surface "TX3-Temperatura extraction required 4 retries" as a maintenance hint.

### 4.4 CSV → Parquet Migration
The `extracted_data/` CSV files for Corrente_DC (434 columns) are large. Parquet with snappy compression would reduce disk usage ~60% and cut analysis load time. Low risk since the read path already uses pandas.

---

## 5. Odoo Integration

### 5.1 ⚡ Alarm Auto-Close on Odoo
When `fault_state.json` removes a key (alarm resolved), automatically set the linked Odoo `anomalia` record to `done`/`closed` state. Currently the close must be done manually in Odoo.

### 5.2 Daily Energy Entry
After end-of-day, write the total MWh produced to an Odoo `energy_production` model (or a custom field on the plant record). This populates Odoo's production log without manual entry.

### 5.3 Odoo Ticket → Telegram Link
When an Odoo ticket is created via the bot, post a clickable Odoo URL back into the Telegram chat so the technician can open the ticket directly.

---

## 6. Deployment

Docker Compose has been removed — `run_monitor.py` orchestrates all services
natively. Remote access is a native Cloudflare Tunnel (`tunnel_manager.py` +
the `Cloudflared` Windows service).

### 6.1 `/healthz` Endpoint + Process Watchdog
Add a `/healthz` endpoint to the dashboard FastAPI app that checks DB
connectivity and last-extraction freshness, so `dashboard_doctor.py` (or an
external uptime monitor) has a single cheap thing to poll instead of
inferring health from log parsing.

### 6.2 Automated DB Backup to Google Drive
Schedule the nightly DB backup to also upload to a Google Drive folder (already integrated in the project via MCP) so off-site recovery is always current.

### 6.3 Stronger Login Protection
The dashboard now requires login (session cookie or Basic Auth) for
`mazara.<domain>` and every protected route/asset. Remaining hardening:
rate-limit `/api/auth/login` attempts, and consider a Cloudflare Access
policy (One-Time PIN to allowed emails) as a second layer in front of the
app-level login.

### 6.4 Grafana + InfluxDB Side-Car (Optional)
For advanced time-series visualization, a Grafana + InfluxDB container could receive the same JSON data that goes to the dashboard. This would provide historical trending, multi-day plots, and alerting rules without code changes to the analyzer.

---

## 7. Testing & Reliability

### 7.1 ⚡ Smoke Test Suite
A small `pytest` suite that:
- loads a fixture `dashboard_data_YYYY-MM-DD.json`
- calls each `build_*_message()` function in `telegram_bot.py`
- asserts no exception and that required strings are present

Covers the Telegram message formatters which are frequently changed.

### 7.2 Extraction Dry-Run Mode
Add a `--dry-run` flag to `vcom_monitor.py` that authenticates and scrapes one metric but does not write to disk. Useful for verifying login and VCOM navigation still works after a platform update without producing data noise.

### 7.3 Watchdog Unit Tests for Alarm Logic
Unit tests for the curtailment suppression, PR stabilization, and temperature thresholds in `processor_watchdog_final.py` using synthetic DataFrames. Prevents regressions when thresholds are tuned.

---

## 8. AI / LLM

### 8.1 ⚡ Context Injection for Curtailment
When the AI assistant answers a PR or production question and curtailment is active, automatically prepend the curtailment status to the context so the AI always explains the reduced output correctly without the operator needing to ask.

### 8.2 Anomaly Root-Cause Narration
After the watchdog detects a new alarm, trigger a brief LLM inference to generate a 2-sentence human-readable explanation of the likely cause and recommended action. Attach this to the Telegram alarm message.

### 8.3 Weekly AI Performance Review
Every Monday morning, run an LLM analysis across the past 7 days of `dashboard_data_*.json` snapshots and post a summary: worst day, best day, inverters with recurring anomalies, PR trend, and energy vs. reference.
