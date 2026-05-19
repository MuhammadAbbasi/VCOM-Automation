# VCOM Automation System - Docker Deployment Checklist

Use this checklist before deploying to ensure everything is configured correctly.

---

## Pre-Deployment Setup (Required)

- [ ] **Docker & Docker Compose installed**
  ```bash
  docker --version  # ≥ 20.10
  docker-compose --version  # ≥ 1.29
  ```

- [ ] **Project directory prepared**
  ```bash
  cd /path/to/VCOM\ Automation
  ls -la Dockerfile docker-compose.yml .env.example
  ```

- [ ] **.env file created and configured**
  ```bash
  cp .env.example .env
  nano .env  # Edit with your actual values
  ```

- [ ] **Data directories created**
  ```bash
  mkdir -p extracted_data logs db errors artifacts
  ```

---

## Environment Configuration Checklist

### Essential VCOM System Settings

- [ ] `VCOM_USERNAME` - Your MeteoControl username
  - Example: `MarcelloPhoton`
  - Verify: Can log in at https://vcom.meteocontrol.com

- [ ] `VCOM_PASSWORD` - Your MeteoControl password
  - ⚠️ Keep secure, never commit to git
  - Verify: Test login manually

- [ ] `VCOM_SYSTEM_URL` - Full system URL
  - Format: `https://vcom.meteocontrol.com/vcom/evaluation/index/index/systemId/XXXX`
  - Copy exact URL from your MeteoControl account
  - Verify: URL returns 200 OK with curl

- [ ] `NGROK_AUTH_TOKEN` - Ngrok tunnel token
  - Get from: https://dashboard.ngrok.com/auth/your-authtoken
  - Verify: `docker-compose exec vcom-automation curl http://localhost:4040` works

### Dashboard Configuration

- [ ] `DASHBOARD_PORT` - Web interface port
  - Default: `8080`
  - Change if port is already in use
  - Verify: `netstat -tuln | grep 8080` returns nothing (or shows different process)

- [ ] `DASHBOARD_USER` - Web login username
  - Example: `mazaraAdmin`
  - Used for `/app/dashboard/app.py`

- [ ] `DASHBOARD_PASS` - Web login password
  - Example: `mazara2025!`
  - Keep secure

### LLM Provider Configuration

Choose ONE of the following:

#### Option A: Local Ollama (Recommended - Privacy First)

- [ ] `OLLAMA_URL` - Set to `http://localhost:11434/api/generate`
- [ ] `OLLAMA_MODEL` - Select model:
  - `qwen2.5:7b` (balanced, recommended)
  - `mistral` (faster)
  - `neural-chat` (specialized)
  - `dolphin-phi` (lightweight)
  
  **Setup required:**
  ```bash
  # Install Ollama
  # https://ollama.ai
  ollama serve
  # In another terminal:
  ollama pull qwen2.5:7b
  ```

#### Option B: OpenAI (Cloud - Commercial)

- [ ] `OPENAI_API_KEY` - Get from https://platform.openai.com/api-keys
  - Format: `sk-...`
  - ⚠️ Keep secret, never commit
  - Verify: `curl https://api.openai.com/v1/models -H "Authorization: Bearer $OPENAI_API_KEY"`

#### Option C: Anthropic (Cloud - Commercial)

- [ ] `ANTHROPIC_API_KEY` - Get from https://console.anthropic.com
  - Format: `sk-ant-...`
  - ⚠️ Keep secret, never commit
  - Verify: Test with Python:
    ```bash
    docker-compose exec vcom-automation python -c "
    import os
    from anthropic import Anthropic
    client = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
    print('Connected to Anthropic')
    "
    ```

- [ ] **Verify LLM choice:**
  - Only ONE provider configured
  - All required variables filled
  - No conflicting env vars

### Telegram Alerts (Optional)

- [ ] `TELEGRAM_BOT_TOKEN` (if using Telegram alerts)
  - Create bot: https://t.me/BotFather
  - Format: `1234567890:ABCDEFghijklmnopqrstuvwxyz...`
  - Test: Send message to bot

- [ ] `TELEGRAM_CHAT_ID` (if using Telegram alerts)
  - Get ID: Send message to bot, then:
    ```bash
    curl https://api.telegram.org/bot<TOKEN>/getUpdates
    ```
  - Look for `chat.id` field

### Odoo Integration (Optional)

- [ ] `ODOO_URL` - Your Odoo instance URL
  - Example: `https://odoo.mycompany.com`
  - Verify: Accessible from container

- [ ] `ODOO_DATABASE` - Database name
  - Example: `production`

- [ ] `ODOO_USERNAME` - Odoo login username
  - Example: `admin`

- [ ] `ODOO_PASSWORD` - Odoo password
  - ⚠️ Keep secure

- [ ] `ODOO_API_KEY` - API key (if applicable)
  - May be same as password or separate
  - Check Odoo documentation

---

## File & Directory Verification

### Critical Files

- [ ] `Dockerfile` - Present and unchanged
  ```bash
  ls -la Dockerfile
  head -5 Dockerfile | grep "multi-stage\|builder\|playwright"
  ```

- [ ] `docker-compose.yml` - Present and valid
  ```bash
  docker-compose config > /dev/null  # Validate syntax
  echo "Config valid"
  ```

- [ ] `.dockerignore` - Present and updated
  ```bash
  wc -l .dockerignore  # Should have ~100+ lines
  ```

- [ ] `.env` - Created from .env.example
  ```bash
  ls -la .env
  file .env  # Should be ASCII text
  ```

### Application Code

- [ ] Core modules present:
  ```bash
  ls run_monitor.py processor_watchdog_final.py vcom_monitor.py llm_agent.py
  ```

- [ ] Extraction code included:
  ```bash
  ls extraction_code/base_monitor.py extraction_code/*_monitor.py
  ```

- [ ] Dashboard files present:
  ```bash
  ls dashboard/app.py dashboard/plant_map_routes.py
  ```

- [ ] Deprecated code removed:
  ```bash
  # Should return 0 files
  find . -name "processor_watchdog_v*.py" -o -name "processor_watchdog_deprecated.py" | wc -l
  ```

### Data Directories

- [ ] Directories created:
  ```bash
  ls -d extracted_data logs db errors artifacts
  # All should exist
  ```

- [ ] Directories are writable:
  ```bash
  touch extracted_data/.test && rm extracted_data/.test
  echo "Success: extracted_data is writable"
  ```

- [ ] No secrets in directories:
  ```bash
  grep -r "PASSWORD\|TOKEN\|sk-" extracted_data logs db 2>/dev/null
  # Should return nothing
  ```

---

## Security Verification

### Credentials

- [ ] **.env is in .gitignore**
  ```bash
  cat .gitignore | grep "^\.env"
  ```

- [ ] **No hardcoded secrets in code**
  ```bash
  grep -r "PASSWORD\|API_KEY\|TOKEN" *.py extraction_code/ dashboard/
  # Should only find env var references, not actual values
  ```

- [ ] **No config.json committed**
  ```bash
  git log --all -- config.json | head -1
  # Should show: fatal: No commits yet
  ```

- [ ] **.env file permissions secure**
  ```bash
  ls -la .env | awk '{print $1}' | grep "^-rw-------"
  # Expected: -rw------- or -rw-r-----
  ```

### Docker Security

- [ ] **Non-root user configured**
  ```bash
  grep "USER vcom" Dockerfile
  ```

- [ ] **Multi-stage build present**
  ```bash
  grep "as builder" Dockerfile
  ```

- [ ] **Health check included**
  ```bash
  grep "HEALTHCHECK" Dockerfile
  ```

- [ ] **Signal handling present**
  ```bash
  grep "STOPSIGNAL\|SIGTERM" Dockerfile
  ```

---

## Pre-Build Verification

- [ ] **Verify Dockerfile syntax**
  ```bash
  docker build --dry-run .
  # Or use Docker linter:
  docker run --rm -i hadolint/hadolint < Dockerfile
  ```

- [ ] **Check available disk space**
  ```bash
  df -h /
  # Need: ≥ 5GB free for build and runtime
  ```

- [ ] **Verify Docker daemon is running**
  ```bash
  docker ps
  # Should list containers without error
  ```

- [ ] **Check network connectivity**
  ```bash
  curl -I https://vcom.meteocontrol.com
  # Should return HTTP 200 or 301/302 (redirect)
  ```

---

## Build Phase Checklist

- [ ] **Build completes without errors**
  ```bash
  docker-compose build 2>&1 | tee build.log
  grep -i "error\|failed" build.log
  # Should return 0 results
  ```

- [ ] **Image created successfully**
  ```bash
  docker images | grep vcom-automation
  # Should show image with size ~900MB
  ```

- [ ] **Image size is reasonable**
  ```bash
  docker images --format "table {{.Repository}}\t{{.Size}}" | grep vcom
  # Should be: ~900MB (not >2GB)
  ```

---

## Pre-Runtime Verification

- [ ] **Required directories exist**
  ```bash
  ls -d extracted_data logs db errors artifacts
  ```

- [ ] **Config file accessible**
  ```bash
  ls -la config.json
  # Should exist and be readable
  ```

- [ ] **Port is available**
  ```bash
  lsof -i :8080 || echo "Port 8080 is free"
  ```

- [ ] **LLM service is ready** (if using local Ollama)
  ```bash
  curl http://localhost:11434/api/tags
  # Should return JSON with model list
  ```

---

## Startup Verification

- [ ] **Container starts without errors**
  ```bash
  docker-compose up -d
  sleep 10
  docker-compose ps
  # Status should show "Up" and "healthy"
  ```

- [ ] **Services are launching**
  ```bash
  docker-compose logs | head -50 | grep -i "started\|orchestrator"
  # Should see multiple "Started [SERVICE]" messages
  ```

- [ ] **Dashboard is accessible**
  ```bash
  curl http://localhost:8080/
  # Should return HTTP 200 with HTML
  ```

- [ ] **Health check is passing**
  ```bash
  docker-compose ps | grep vcom
  # Should show "healthy" status
  ```

---

## Operational Verification (First Run)

- [ ] **Data extraction is working**
  ```bash
  # Wait 2 minutes, then:
  ls -la extracted_data/
  # Should show CSV/JSON files with current date
  ```

- [ ] **Logs are being generated**
  ```bash
  ls -la logs/
  # Should contain files with recent modification time
  ```

- [ ] **Database is created**
  ```bash
  ls -la db/*.db
  # Should show database file(s)
  ```

- [ ] **No critical errors in logs**
  ```bash
  docker-compose logs | grep -i "error\|exception\|failed"
  # Review any errors, should be non-critical
  ```

- [ ] **LLM agent is initialized**
  ```bash
  docker-compose logs | grep -i "ollama\|openai\|anthropic\|llm"
  # Should show model initialization
  ```

- [ ] **All 8 services are running**
  ```bash
  docker-compose logs | grep "Started" | wc -l
  # Should be: 8 (Dashboard, Watchdog, Extraction, Telegram, Doctor, Tickets, Broker, Tracker)
  ```

---

## Access Verification

- [ ] **Dashboard accessible**
  - Open: http://localhost:8080
  - Should display: Login page or dashboard
  - Credentials: Use DASHBOARD_USER/DASHBOARD_PASS from .env

- [ ] **Real-time data visible**
  - Log in to dashboard
  - Should show: Inverter status, power metrics, alerts
  - Data should update every 30-60 seconds

- [ ] **Plant map displays**
  - Navigate to: Plant Layout tab
  - Should show: Visual representation of plant with inverter locations

- [ ] **Historical data available**
  - Check: Data History section
  - Should show: Extracted CSV files from today

---

## Monitoring & Maintenance

- [ ] **Set up log rotation** (recommended)
  ```bash
  # Configure docker logging driver in docker-compose.yml
  logging:
    driver: "json-file"
    options:
      max-size: "100m"
      max-file: "10"
  ```

- [ ] **Schedule backups**
  ```bash
  # Backup daily
  0 2 * * * cp -r /path/to/vcom/extracted_data /backup/vcom_$(date +\%Y\%m\%d)
  ```

- [ ] **Monitor resource usage**
  ```bash
  # Check weekly
  docker stats vcom_automation_system
  # CPU <30%, Memory <3GB (normal range)
  ```

- [ ] **Review logs regularly**
  ```bash
  # Weekly review
  docker-compose logs | grep -i "warning\|error" | tail -20
  ```

---

## Troubleshooting Verification

If deployment fails, verify:

- [ ] **Configuration file syntax**
  ```bash
  docker-compose config --services
  # Should list: vcom-automation
  ```

- [ ] **Environment variables are set**
  ```bash
  grep -v "^#\|^$" .env | wc -l
  # Should show >10 configured variables
  ```

- [ ] **Dockerfile is valid**
  ```bash
  docker build --dry-run . 2>&1 | grep -i "error"
  ```

- [ ] **Network connectivity**
  ```bash
  docker-compose exec vcom-automation ping 8.8.8.8
  # Should get response (internet access)
  ```

- [ ] **Container can access config**
  ```bash
  docker-compose exec vcom-automation cat /app/config.json | head
  # Should show JSON content
  ```

---

## Post-Deployment Documentation

After successful deployment, document:

- [ ] **Deployment date**: ________________
- [ ] **Host OS**: Linux / macOS / Windows (WSL2)
- [ ] **Docker version**: ________________
- [ ] **Python version inside container**: ________________
- [ ] **LLM provider used**: Ollama / OpenAI / Anthropic
- [ ] **Data volume mount point**: ________________
- [ ] **Network**: Private / Public / VPN
- [ ] **Notes**: ________________

---

## Success Criteria

Your deployment is successful when ALL of the following are true:

✅ Container is running and healthy
✅ Dashboard accessible at http://localhost:8080
✅ Data extraction is active (files in extracted_data/)
✅ No critical errors in logs
✅ LLM agent is initialized and ready
✅ All 8 services show "Started" in logs
✅ Telegram alerts (if configured) sending successfully
✅ Odoo tickets (if configured) creating successfully

---

## Rollback Plan

If deployment fails:

```bash
# 1. Stop current deployment
docker-compose down

# 2. Review logs
docker-compose logs > debug_logs.txt

# 3. Fix configuration
nano .env
# or
nano docker-compose.yml

# 4. Try again
docker-compose up -d

# 5. If still failing, rebuild
docker-compose build --no-cache
docker-compose up -d
```

---

## Sign-Off

- [ ] **Deployer Name**: ________________
- [ ] **Deployment Date**: ________________
- [ ] **All checks completed**: YES / NO
- [ ] **Issues found**: ________________
- [ ] **Approved for production**: YES / NO

---

**Document Version**: 1.0
**Last Updated**: May 2026

For issues or questions, see `DOCKER_README.md` or `DOCKER_QUICKSTART.md`
