# Docker Containerization — EXECUTION PLAN
## Mazara 01 SCADA / VCOM Automation

---

## 1. Context & Goal

Containerize the Mazara 01 solar-plant SCADA monitoring system into a production-grade
multi-container Docker environment. The source repository (`../`) remains read-only.
All Docker-specific files live here in `VCOM Automation Docker/`.

---

## 2. Security Findings (addressed in this layer)

| Severity | Finding | Source File(s) | Docker Remediation |
|----------|---------|---------------|-------------------|
| CRITICAL | VCOM credentials in plaintext | `config.json` | Moved to `.env`, rendered via template at startup |
| CRITICAL | Telegram Bot Token in plaintext | `config.json`, `user_settings.json` | `.env` + template |
| CRITICAL | Ngrok Auth Token hardcoded | `config.json` | `.env` + template |
| CRITICAL | Odoo creds hardcoded in source code | `odoo_ticket_engine.py:52`, `telegram_bot.py:131` | Wrapper scripts patch module constants from env vars |
| HIGH | No version pinning in requirements.txt | `requirements.txt` | `requirements.docker.txt` with pinned versions |
| HIGH | DB backups unencrypted in same dir | `db/backups/` | Isolated named Docker volume |
| MEDIUM | Weak dashboard password | `config.json` | User sets strong value in `.env` |

> `config.json` and `user_settings.json` are already in `.gitignore` (generated files).
> They are regenerated at every container startup by `scripts/entrypoint.sh` via `envsubst`.

---

## 3. Architecture Decisions

| Decision | Approach |
|----------|---------|
| Orchestrator | `docker-compose` replaces `run_monitor.py` (Windows-only). Each service = one container with `restart: always`. |
| Playwright headless | `VCOM_HEADLESS=true` env var — already supported in `vcom_monitor.py:329`. No source change. |
| Hardcoded Odoo URL | `wrappers/run_tickets.py` and `wrappers/run_telegram.py` monkey-patch module constants from env vars before calling `main()`. |
| Config generation | `entrypoint.sh` runs `envsubst` on JSON templates at container start, writes `config.json` + `user_settings.json`. |
| Database | Three SQLite files in shared named volume `scada_db_data` at `/app/db`. No separate DB container needed. |
| LLM (Ollama) | Runs natively on host, always available. `user_settings.json` template sets `ollama_url` to `http://host.docker.internal:11434`. No Ollama container. |
| Odoo | Runs in existing Docker container on same host. Accessible via `http://host.docker.internal:8069`. `extra_hosts: [host.docker.internal:host-gateway]` added to all services. |
| Playwright shm | `shm_size: '2gb'` on the extraction service prevents Chromium `/dev/shm` OOM crashes. |
| envsubst safety | Explicit variable list passed to `envsubst` to avoid mangling JSON bracket characters. |

---

## 4. Directory Layout

```
VCOM Automation/                          ← git root (build context for docker-compose)
│
├── [all source files — READ-ONLY]
│
└── VCOM Automation Docker/               ← THIS folder — all Docker files
    ├── EXECUTION_PLAN.md                 ← this file
    ├── .env.example                      ← copy → .env and fill in secrets
    ├── .env                              ← (gitignored) runtime secrets
    ├── .dockerignore
    ├── docker-compose.yml
    ├── requirements.docker.txt           ← pinned pip packages
    │
    ├── docker/
    │   ├── base.Dockerfile               ← python:3.12-slim + all packages
    │   └── extraction.Dockerfile         ← base + playwright + chromium
    │
    ├── wrappers/
    │   ├── run_tickets.py                ← patches Odoo constants before odoo_ticket_engine.main()
    │   └── run_telegram.py              ← patches Odoo URL before telegram_bot.main()
    │
    ├── config/
    │   ├── config.json.template          ← ${VAR} placeholders
    │   └── user_settings.json.template   ← ${VAR} placeholders
    │
    └── scripts/
        └── entrypoint.sh                 ← envsubst render + exec CMD
```

**Build context path logic:**
`docker-compose.yml` sets `context: ../` (= git root `VCOM Automation/`) and
`dockerfile: VCOM Automation Docker/docker/base.Dockerfile` (relative to that context).
Inside each Dockerfile: `COPY . /app/` with `.dockerignore` excluding runtime-generated files.

---

## 5. Service Stack

| Service | Image | Entry Point | Published Port | Volumes |
|---------|-------|------------|---------------|---------|
| `dashboard` | `scada-app` | `dashboard/app.py` | **8080 → 8080** | `scada_db_data`, `extracted_data`, `logs` |
| `extraction` | `scada-extraction` | `vcom_monitor.py` | — | `scada_db_data`, `extracted_data`, `logs`, `playwright_profile` |
| `watchdog` | `scada-app` | `processor_watchdog_final.py` | — | `scada_db_data`, `extracted_data`, `logs` |
| `telegram` | `scada-app` | `wrappers/run_telegram.py` | — | `scada_db_data`, `extracted_data`, `logs` |
| `tickets` | `scada-app` | `wrappers/run_tickets.py` | — | `scada_db_data`, `logs` |
| `broker` | `scada-app` | `tracker_testing/broker.py` | 1883 (internal) | `logs` |
| `tracker` | `scada-app` | `tracker_testing/receiver.py` | — | `scada_db_data`, `logs` |

All services: internal network `scada_net` · `restart: always` · `TZ=Europe/Rome`
`extra_hosts: [host.docker.internal:host-gateway]` on every service.

---

## 6. Named Volumes

| Volume | Purpose |
|--------|---------|
| `scada_db_data` | SQLite databases (`scada_data.db`, `scada_snapshots.db`, `scada_logs.db`) + JSON state files |
| `extracted_data` | Daily CSV metric files + dashboard JSON snapshots |
| `logs` | All `.log` files from every service |
| `playwright_profile` | Chromium session cookies / storage state (persists VCOM login) |

---

## 7. Step-by-Step Execution Order

1. ✅ Create git branch `feature/docker-containerized`
2. ✅ Create directory structure (`docker/`, `wrappers/`, `config/`, `scripts/`)
3. ✅ Write `EXECUTION_PLAN.md`
4. ⬜ Write `.env.example`
5. ⬜ Write `requirements.docker.txt`
6. ⬜ Write `docker/base.Dockerfile`
7. ⬜ Write `docker/extraction.Dockerfile`
8. ⬜ Write `config/config.json.template`
9. ⬜ Write `config/user_settings.json.template`
10. ⬜ Write `scripts/entrypoint.sh`
11. ⬜ Write `wrappers/run_tickets.py`
12. ⬜ Write `wrappers/run_telegram.py`
13. ⬜ Write `docker-compose.yml`
14. ⬜ Write `.dockerignore`
15. ⬜ Commit + push each file to `origin/feature/docker-containerized`

---

## 8. State Verification

| Check | Command |
|-------|---------|
| All containers up | `docker compose -f "VCOM Automation Docker/docker-compose.yml" ps` |
| Dashboard healthy | `curl http://localhost:8080/` |
| Extraction started | `docker compose logs extraction \| grep "VCOM MONITOR STARTING"` |
| Databases created | `docker compose exec dashboard ls /app/db/*.db` |
| Watchdog active | `docker compose logs watchdog \| grep "Observer started"` |
| Ollama reachable | `docker compose exec dashboard curl -s http://host.docker.internal:11434` |
| Telegram connected | `docker compose logs telegram \| grep "polling"` |
| Odoo reachable | `docker compose logs tickets \| grep "Odoo"` |

---

## 9. Fallback / Handover

If this session is interrupted, the next agent should:
1. Read this file first
2. Run `git status` in `VCOM Automation/` to see which files exist
3. Resume from the first ⬜ step in Section 7
4. **Never modify** any file outside `VCOM Automation Docker/`
5. Key source constraints:
   - `odoo_ticket_engine.py:52-55` — hardcoded Odoo constants → wrapper patches these
   - `vcom_monitor.py:329` — `VCOM_HEADLESS` env var already supported
   - `run_monitor.py` — Windows-only, bypassed entirely by docker-compose
   - `dashboard/app.py` — FastAPI on port 8080, reads `config.json`
   - `llm_agent.py:30` — reads `ollama_url` from `user_settings.json`
