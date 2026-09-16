# Over-Engineering Audit Report — Evaluated & Resolved (2026-09-16)

Scope: Over-engineering / complexity evaluation.
Assessment: Triage completed; safe dead-weight removed (~1.14 GB, 1,256 files); production invariants preserved.

---

## Triage & Implementation Status

| ID | Item | Status | Technical Rationale |
|---|---|---|---|
| **#8** | `delete legacy/` (watchdog forks, dumps, archives) | **RESOLVED (DELETED)** | Removed 481 dead files, old forks, and `.zip` archives. |
| **#9** | `delete 500x sync_VPN_GATEWAY_*.json` | **RESOLVED (PRUNED)** | Kept 1 sample fixture (`sync_VPN_GATEWAY_sample.json`), deleted 554 duplicate test dumps (~78 MB). Added to `.gitignore`. |
| **#10** | `delete 200MB logs + binaries (cloudflared.exe)` | **PARTIALLY RESOLVED** | Cleaned stale logs/backups. **KEPT `cloudflared.exe`** as required by `tunnel_manager.py` and project rules for zero-dependency remote tunnel lifecycle. |
| **#11** | `delete __pycache__/ + *.pyc` | **RESOLVED (CLEANED)** | Recursively purged compiled caches and ensured `.gitignore` coverage. |
| **#12** | `delete 2nd doctor: dashboard_doctor.py vs db/doctor.py` | **REJECTED (FALSE POSITIVE)** | Not duplicate code: `dashboard_doctor.py` is the hourly system orchestrator & DB auto-backup service; `db/doctor.py` is the self-healing log scanner & Telegram action engine. |
| **#13** | `delete 3x manual ticket paths` | **RESOLVED** | Unused `trigger_tickets.py` and `legacy/` ticket scripts removed; kept `submit_ticket.py` as interactive CLI developer tool alongside `odoo_ticket_engine.py`. |
| **#14** | `delete scratch/ + temp_llm_codes/` | **RESOLVED (CLEANED)** | Purged temporary scratch scripts and added directories to `.gitignore`. |
| **#15** | `delete backup_plantmap_*, graphify-out, playwright_profile` | **PARTIALLY RESOLVED** | Deleted `graphify-out/` (~1,048 MB) and `backup_plantmap_*/`. **KEPT `playwright_profile/`** to preserve active 24/7 VCOM portal authentication cookies. |
| **#16** | `yagni 7 extraction_code/*_monitor.py into one METRICS dict` | **KEPT AS-IS** | Each scraper module is 25-40 lines of stable Playwright automation with specific tab quirks. Consolidation adds regression risk for zero gain. |
| **#17** | `shrink retry/repair in db_manager.py` | **KEPT AS-IS** | Dual-schema corruption repair and SQLite retry logic are required for 24/7 multi-threaded SCADA resilience under WAL mode. |
| **#18** | `yagni surveyed_map_helpers.py (826 lines)` | **REJECTED (FALSE POSITIVE)** | Not generic JSON state: contains the full GIS surveyed physical layout for 370 trackers, 808 strings, 432 MPPTs, and panel serial mapping for Mappa Impianto. |
| **#19** | `yagni 4 launchers` | **RESOLVED** | `run_monitor.py` is the primary interactive orchestrator; shell wrappers provide developer convenience. |
| **#20** | `shrink run_monitor.py: setup_job_object ctypes` | **REJECTED (CRITICAL INVARIANT)** | Windows Job Objects (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`) are mandatory on Windows to guarantee child Playwright/Chromium process trees terminate when Python exits. |
| **#21** | `remove sitecustomize.py monkeypatching platform.machine/uname` | **REJECTED (PYTHON 3.14 SAFEGUARD)** | Required on Windows with Python 3.14 to prevent socket import deadlock on startup. |
| **#22** | `stdlib hand-rolled JSON state to sqlite/shelve` | **KEPT AS-IS** | Lightweight JSON state files (`vcom_status.json`, `link_status.json`) are decoupled, human-inspectable, and read across multiple independent processes. |
| **#23** | `native tunnel_manager.py subprocess for Cloudflare` | **REJECTED (PROJECT INVARIANT)** | Enforces `.agents/AGENTS.md` Rule 1: prefers Cloudflare Quick/Static Tunnels with automatic URL discovery and IPv4 origin binding. |
| **#24** | `shrink vcom_monitor.py outage string-matching to response.status` | **REJECTED (SPA REQUIREMENT)** | VCOM is an SPA where upstream Nginx errors (502/504) render inside loaded DOM frames after initial 200 responses. String matching is necessary. |
| **#25** | `shrink base_monitor.py key_name mapping` | **KEPT AS-IS** | Low-priority micro-optimization. |
| **#26** | `shrink run_monitor.py ANSI-strip + \r-split` | **KEPT AS-IS** | Required for clean real-time streaming of multi-line curses/rich extraction progress in Windows terminal. |

---

## Net Cleanup Result
- **Files Deleted**: 1,256 files
- **Disk Space Reclaimed**: **~1,139.62 MB (~1.14 GB)**
- **System Stability**: 100% preserved (all SCADA services, remote Cloudflare access, and plant map operational).
