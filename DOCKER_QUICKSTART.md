# VCOM Automation System - Docker Quick Start Guide

**TL;DR** — Get up and running in 5 minutes

---

## 🚀 Quick Start (5 Minutes)

```bash
# 1. Configure environment
cp .env.example .env
nano .env  # Edit with your credentials

# 2. Create data directories
mkdir -p extracted_data logs db errors artifacts

# 3. Build image (5-10 minutes, first time only)
docker-compose build

# 4. Start system
docker-compose up -d

# 5. View logs
docker-compose logs -f

# 6. Access dashboard
# Open browser: http://localhost:8080
```

---

## 📋 Essential Commands

### Status & Logs

```bash
# Show running containers
docker-compose ps

# View logs (real-time)
docker-compose logs -f

# View last 100 lines
docker-compose logs --tail=100

# View logs for specific service
docker-compose logs -f vcom-automation
```

### Start/Stop

```bash
# Start system
docker-compose up -d

# Stop system
docker-compose stop

# Force stop
docker-compose kill

# Restart
docker-compose restart

# Stop and remove containers
docker-compose down

# Stop and remove everything
docker-compose down -v  # ⚠️ Removes volumes!
```

### Debugging

```bash
# Access container shell
docker-compose exec vcom-automation bash

# Run Python command
docker-compose exec vcom-automation python -V

# Check installed packages
docker-compose exec vcom-automation pip list | grep pandas

# View running processes
docker-compose exec vcom-automation ps aux

# Check environment variables
docker-compose exec vcom-automation env | grep VCOM
```

### Rebuild & Clean

```bash
# Rebuild without cache
docker-compose build --no-cache

# Remove unused images
docker system prune

# Remove all stopped containers
docker container prune

# Remove unused volumes
docker volume prune
```

---

## ⚙️ Configuration

### 1. Set Environment Variables

```bash
cp .env.example .env
```

**Must configure:**
```env
VCOM_USERNAME=YourUsername
VCOM_PASSWORD=YourPassword
VCOM_SYSTEM_URL=https://vcom.meteocontrol.com/...
NGROK_AUTH_TOKEN=your_token
```

**Choose LLM Provider** (pick one):
```env
# Option 1: Local Ollama (recommended)
OLLAMA_URL=http://localhost:11434/api/generate
OLLAMA_MODEL=qwen2.5:7b

# Option 2: OpenAI
OPENAI_API_KEY=sk-...

# Option 3: Anthropic
ANTHROPIC_API_KEY=sk-ant-...
```

**Optional:**
```env
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
DASHBOARD_PORT=8080  # Change if 8080 is busy
```

### 2. Create Data Directories

```bash
mkdir -p extracted_data logs db errors artifacts
```

---

## ✅ Verification Checklist

```bash
# ✓ Dashboard is accessible
curl http://localhost:8080

# ✓ Services are running
docker-compose ps | grep vcom

# ✓ Data is being extracted
ls -la extracted_data/ | head

# ✓ Logs show no errors
docker-compose logs | grep -i "error\|failed" | wc -l
# Should return 0 or very small number

# ✓ LLM agent is initialized
docker-compose logs | grep -i "ollama\|openai\|anthropic"

# ✓ Database exists
ls -la db/*.db
```

---

## 🐛 Quick Troubleshooting

### Dashboard Won't Start

```bash
# Check if port 8080 is in use
lsof -i :8080  # macOS/Linux
netstat -ano | findstr :8080  # Windows

# Change port in .env
DASHBOARD_PORT=8081

# Restart
docker-compose restart
```

### VCOM Authentication Failed

```bash
# Check credentials
grep VCOM .env

# View auth logs
docker-compose logs | grep -i "auth\|login"

# Test VCOM URL (should be accessible)
curl https://vcom.meteocontrol.com
```

### Out of Memory

```bash
# Check memory usage
docker stats

# Increase limit in .env
CONTAINER_MEMORY_LIMIT=8G

# Rebuild and restart
docker-compose down
docker-compose build
docker-compose up -d
```

### No Data Being Extracted

```bash
# Check extraction logs
docker-compose logs | grep -i "extraction\|vcom_monitor"

# Verify config.json exists
ls -la config.json

# Test extraction manually
docker-compose exec vcom-automation python vcom_monitor.py
```

### LLM Agent Not Working

```bash
# Check logs
docker-compose logs | grep -i "llm\|ollama\|openai"

# For Ollama, ensure it's running
ollama serve

# Test Ollama endpoint
docker-compose exec vcom-automation curl http://localhost:11434/api/tags
```

---

## 📊 Performance Tips

### Reduce Resource Usage

```bash
# In .env
CONTAINER_CPU_LIMIT=2          # Was 4
CONTAINER_MEMORY_LIMIT=2G      # Was 4G
CONTAINER_CPU_RESERVE=1        # Was 2
CONTAINER_MEMORY_RESERVE=1G    # Was 2G
```

### Improve Performance

```bash
# Increase resources
CONTAINER_CPU_LIMIT=8
CONTAINER_MEMORY_LIMIT=8G

# Use SSD for mounted volumes
# (faster than network storage)
```

---

## 💾 Backup & Restore

### Backup Data

```bash
# Quick backup
cp -r extracted_data extracted_data_$(date +%Y%m%d_%H%M%S)
cp -r db db_$(date +%Y%m%d_%H%M%S)

# Full backup
tar czf vcom_backup_$(date +%Y%m%d_%H%M%S).tar.gz \
  extracted_data/ logs/ db/ errors/ artifacts/
```

### Restore Data

```bash
# From backup
tar xzf vcom_backup_2026-05-18_120000.tar.gz

# Restart with restored data
docker-compose restart
```

---

## 🔐 Security Checklist

```bash
# ✓ Don't commit .env
git status | grep .env
# Should show ".env" is ignored

# ✓ Make .env read-only
chmod 600 .env

# ✓ Don't hardcode secrets
grep -r "PASSWORD\|TOKEN\|API_KEY" *.py
# Should only find references to environment vars

# ✓ Use read-only config volumes
# Check docker-compose.yml has ":ro" on config mounts

# ✓ Container runs as non-root
docker-compose exec vcom-automation whoami
# Should output: vcom (not root)
```

---

## 📱 Access the Dashboard

```
URL: http://localhost:8080
Login: See DASHBOARD_USER in .env
```

**Features:**
- Real-time system monitoring
- Plant layout visualization
- Inverter status dashboard
- Alert history
- Data export

---

## 🔧 Advanced Usage

### Run Specific Service

```bash
# Run only dashboard
docker-compose exec vcom-automation python dashboard/app.py

# Run only VCOM monitor
docker-compose exec vcom-automation python vcom_monitor.py

# Run LLM agent
docker-compose exec vcom-automation python llm_agent.py
```

### Override Command

```bash
# Run custom Python script
docker-compose exec vcom-automation python my_script.py

# Interactive Python shell
docker-compose exec vcom-automation python
```

### View Resource Usage

```bash
# Real-time stats
docker stats vcom_automation_system

# One-time snapshot
docker inspect --format='{{json .State}}' vcom_automation_system
```

### Export Logs

```bash
# Save logs to file
docker-compose logs > logs_all.txt

# Save logs with timestamps
docker-compose logs --timestamps > logs_timestamped.txt

# Save error logs only
docker-compose logs | grep -i error > errors.txt
```

---

## 🆘 Get Help

1. **Check logs**: `docker-compose logs -f`
2. **Verify config**: `cat .env | grep -v '^#' | grep -v '^$'`
3. **Test connectivity**: `docker-compose exec vcom-automation curl https://vcom.meteocontrol.com`
4. **View diagnostics**: `docker inspect vcom_automation_system`
5. **Review DOCKER_README.md** for detailed troubleshooting

---

## 📚 Key Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Container image definition |
| `docker-compose.yml` | Service orchestration |
| `.env.example` | Configuration template |
| `.dockerignore` | Build exclusions |
| `DOCKER_README.md` | Complete guide |
| `DOCKER_ARCHITECTURE.md` | Technical deep-dive |

---

## 🎯 Common Workflows

### Deploy to New Server

```bash
# 1. Copy files to server
scp .env user@server:vcom/
scp docker-compose.yml user@server:vcom/
scp Dockerfile user@server:vcom/

# 2. Build on server
ssh user@server
cd vcom
docker-compose build

# 3. Start
docker-compose up -d
docker-compose logs -f
```

### Update Code

```bash
# 1. Pull latest code
git pull

# 2. Rebuild image
docker-compose build

# 3. Restart
docker-compose restart
```

### Rotate Credentials

```bash
# 1. Update .env
nano .env  # Change PASSWORD, TOKEN, API_KEY

# 2. Restart (reads new environment)
docker-compose restart

# 3. Verify
docker-compose logs | grep -i "connected\|authenticated"
```

### Migrate to Another Machine

```bash
# On source machine
docker save vcom-automation:latest | gzip > vcom.tar.gz

# On target machine
zcat vcom.tar.gz | docker load

# Verify
docker images | grep vcom-automation

# Run
docker-compose up -d
```

---

## 📞 Command Summary (Cheat Sheet)

```bash
# Build
docker-compose build
docker-compose build --no-cache

# Run
docker-compose up -d
docker-compose up -d --scale vcom=3  # Multiple instances

# Status
docker-compose ps
docker-compose ps -a

# Logs
docker-compose logs
docker-compose logs -f
docker-compose logs --tail=50

# Stop/Start
docker-compose stop
docker-compose restart
docker-compose kill

# Clean up
docker-compose down
docker-compose down -v

# Shell access
docker-compose exec vcom-automation bash
docker-compose exec vcom-automation python

# Diagnostics
docker stats
docker-compose config
docker logs <container_id>
docker inspect <container_id>
```

---

**Version**: 1.0
**Last Updated**: May 2026
**Docker Compose Version**: 1.29+
