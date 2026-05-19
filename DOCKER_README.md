# VCOM Automation System - Docker Deployment Guide

**Complete containerization of the VCOM Automation System for seamless portability and production deployment.**

## Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture Overview](#architecture-overview)
3. [Prerequisites](#prerequisites)
4. [Building the Image](#building-the-image)
5. [Configuration](#configuration)
6. [Running the Container](#running-the-container)
7. [Verification & Testing](#verification--testing)
8. [Data Persistence](#data-persistence)
9. [Logs & Troubleshooting](#logs--troubleshooting)
10. [Advanced Usage](#advanced-usage)
11. [Security Considerations](#security-considerations)

---

## Quick Start

For the impatient:

```bash
# 1. Copy the environment file and edit with your credentials
cp .env.example .env
# Edit .env with your VCOM, LLM, and Telegram credentials

# 2. Create data directories
mkdir -p extracted_data logs db errors artifacts

# 3. Build the image (first time only)
docker-compose build

# 4. Start the system
docker-compose up -d

# 5. View logs
docker-compose logs -f vcom-automation

# 6. Access dashboard
# Open http://localhost:8080 in your browser

# 7. Stop the system
docker-compose down
```

---

## Architecture Overview

The VCOM Automation System is now containerized as a **single optimized multi-service Docker container** that orchestrates:

```
┌─────────────────────────────────────────────────────────┐
│          VCOM Automation Docker Container               │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Orchestrator (run_monitor.py)                    │  │
│  │ - Manages all child processes                    │  │
│  │ - Handles graceful shutdown & restarts           │  │
│  └──────────────────────────────────────────────────┘  │
│           │                    │                        │
│  ┌────────▼────────┐  ┌────────▼────────┐            │
│  │ Dashboard       │  │ Core Monitoring │             │
│  │ app.py:8080     │  │ Services        │             │
│  │ - Web UI        │  │ ───────────────  │             │
│  │ - Real-time     │  │ 1. processor_    │             │
│  │   monitoring    │  │    watchdog_     │             │
│  │ - Plant map     │  │    final.py      │             │
│  └─────────────────┘  │ 2. vcom_monitor │             │
│                       │    .py           │             │
│  ┌──────────────────┐ │ 3. telegram_bot  │             │
│  │ LLM Agents       │ │    .py           │             │
│  │ ──────────────── │ │ 4. dashboard_    │             │
│  │ • llm_agent.py   │ │    doctor.py     │             │
│  │ • Analysis &     │ │ 5. odoo_ticket_  │             │
│  │   reporting      │ │    engine.py     │             │
│  └──────────────────┘ └─────────────────┘             │
│                                                         │
│  ┌──────────────────┐  ┌──────────────────┐           │
│  │ Data Extraction  │  │ Broker & Tracker │           │
│  │ ──────────────── │  │ ───────────────── │           │
│  │ extraction_code/ │  │ broker.py        │           │
│  │ • base_monitor   │  │ receiver.py      │           │
│  │ • Metric         │  │ Event streaming  │           │
│  │   collectors     │  │ Data sync        │           │
│  └──────────────────┘  └──────────────────┘           │
│                                                         │
└─────────────────────────────────────────────────────────┘
                           │
                    ┌──────▼──────┐
                    │ Persistent  │
                    │ Volumes     │
                    │ ─────────── │
                    │ • extracted_│
                    │   data/     │
                    │ • logs/     │
                    │ • db/       │
                    │ • errors/   │
                    │ • artifacts/│
                    └─────────────┘
```

### Key Components

| Service | Purpose | Port |
|---------|---------|------|
| **Orchestrator** | Launches & manages all services, handles restarts | Internal |
| **Dashboard** | Web-based real-time monitoring interface | 8080 |
| **Watchdog** | Processor monitoring & fault detection | Internal |
| **VCOM Monitor** | Continuous data extraction from MeteoControl | Internal |
| **Telegram Bot** | Alert delivery via Telegram | Internal |
| **LLM Agents** | AI-powered analysis & decision-making | Internal |
| **Odoo Tickets** | Automated ticket creation in Odoo | Internal |
| **Broker** | Event streaming & data synchronization | Internal |
| **Tracker** | Receives and logs system events | Internal |

---

## Prerequisites

### System Requirements

- **Docker & Docker Compose**: [Install here](https://docs.docker.com/get-docker/)
  - Docker version: 20.10+
  - Docker Compose: 1.29+
  
- **Hardware Minimum**:
  - CPU: 2 cores (4+ recommended)
  - RAM: 2GB (4GB+ recommended)
  - Disk: 10GB free space

- **Network**:
  - Outbound HTTPS to `vcom.meteocontrol.com`
  - Outbound to `api.telegram.org` (if using Telegram)
  - Outbound to LLM endpoint (Ollama, OpenAI, or Anthropic)

### Verify Docker Installation

```bash
docker --version        # Should be 20.10+
docker-compose --version # Should be 1.29+
docker run hello-world  # Should print "Hello from Docker!"
```

---

## Building the Image

### Option 1: Docker Compose (Recommended)

```bash
# Build the image automatically
docker-compose build

# Or rebuild with no cache (useful if dependencies changed)
docker-compose build --no-cache
```

### Option 2: Manual Docker Build

```bash
# Build with default tag
docker build -t vcom-automation:latest .

# Build with custom tag
docker build -t vcom-automation:v1.0.0 .

# Build for specific platform (e.g., for ARM64 on Mac M1/M2)
docker build --platform linux/amd64 -t vcom-automation:latest .
```

### Verify Build Success

```bash
# List images
docker images | grep vcom

# Inspect image details
docker inspect vcom-automation:latest

# Check image size
docker images vcom-automation:latest --format "{{.Size}}"
```

---

## Configuration

### 1. Environment Variables (.env File)

```bash
# Copy the example file
cp .env.example .env

# Edit with your credentials
nano .env  # or use your favorite editor
```

**Critical variables you MUST configure:**

```env
# VCOM System
VCOM_USERNAME=YourUsername
VCOM_PASSWORD=YourPassword
VCOM_SYSTEM_URL=https://vcom.meteocontrol.com/vcom/evaluation/index/index/systemId/YOUR_ID

# LLM Provider (choose one)
# Option 1: Local Ollama (recommended)
OLLAMA_URL=http://localhost:11434/api/generate
OLLAMA_MODEL=qwen2.5:7b

# Option 2: OpenAI
OPENAI_API_KEY=sk-your_key_here

# Option 3: Anthropic
ANTHROPIC_API_KEY=sk-ant-your_key_here

# Telegram (optional)
TELEGRAM_BOT_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

### 2. Directory Structure

Ensure these directories exist on the host (Docker will create them if missing):

```bash
mkdir -p extracted_data logs db errors artifacts
```

**Directory purposes:**
- `extracted_data/` - CSV exports, dashboard JSON snapshots, metric exports
- `logs/` - Application logs from all services
- `db/` - SQLite databases and data state files
- `errors/` - Error screenshots and diagnostic data
- `artifacts/` - Generated reports and analysis artifacts

### 3. Configuration Files

If using external configuration:

```bash
# Mount your config files as read-only
# They'll be specified in docker-compose.yml volumes section
docker cp config.json container-id:/app/config.json
```

---

## Running the Container

### Start the System

```bash
# Start in background (detached mode)
docker-compose up -d

# Start with live logs
docker-compose up

# Start with specific service only (advanced)
docker-compose up vcom-automation
```

### View Logs

```bash
# Follow logs in real-time
docker-compose logs -f vcom-automation

# View last 100 lines
docker-compose logs --tail=100 vcom-automation

# View logs with timestamps
docker-compose logs -f --timestamps vcom-automation

# Save logs to file
docker-compose logs vcom-automation > vcom.log
```

### Check Service Status

```bash
# Show running containers
docker-compose ps

# Detailed status
docker-compose ps --no-trunc

# Check if container is healthy
docker-compose ps | grep vcom-automation
```

### Stop the System

```bash
# Graceful stop (30-second grace period)
docker-compose stop

# Force stop (immediate)
docker-compose kill

# Stop and remove containers (data in volumes persists)
docker-compose down

# Stop and remove everything including volumes (CAUTION: data loss)
docker-compose down -v
```

### Restart Services

```bash
# Restart the entire system
docker-compose restart

# Restart just the VCOM automation service
docker-compose restart vcom-automation
```

---

## Verification & Testing

### 1. Verify Dashboard is Running

```bash
# Method 1: Access via browser
http://localhost:8080

# Method 2: Health check via curl
curl http://localhost:8080/

# Expected response: HTTP 200 OK with HTML
```

### 2. Verify Core Services are Active

```bash
# Check container logs for startup messages
docker-compose logs | grep -E "\[ORCHESTRATOR\]|Started|ERROR"

# Look for lines like:
# [ORCHESTRATOR] Started DASHBOARD (pid=...)
# [ORCHESTRATOR] Started WATCHDOG (pid=...)
# [ORCHESTRATOR] Started EXTRACTION (pid=...)
```

### 3. Verify Data Extraction

```bash
# Check if extracted_data directory is being populated
ls -la extracted_data/

# Should see CSV or JSON files with recent timestamps
# Example output:
# PR_2026-05-18.csv
# Potenza_AC_2026-05-18.csv
# dashboard_data_2026-05-18.json
```

### 4. Verify LLM Integration

```bash
# Check logs for LLM model loading
docker-compose logs vcom-automation | grep -i "ollama\|openai\|anthropic\|llm"

# Expected: Model initialization messages, no API errors
```

### 5. Verify Database Connectivity

```bash
# Check database file creation
ls -la db/

# Should contain .db files:
# tracker.db
# Any other data files
```

### 6. Test Individual Components

```bash
# Run a test command inside the container
docker-compose exec vcom-automation python -c "import extraction_code; print('Import OK')"

# Check system imports
docker-compose exec vcom-automation python -c "
import sys
print(f'Python version: {sys.version}')
import pandas; import numpy; import playwright
print('Core dependencies: OK')
"
```

### 7. Full System Test

```bash
# Monitor the system for 5 minutes
docker-compose logs -f vcom-automation &
sleep 300
kill %1

# Analyze logs for errors
docker-compose logs vcom-automation | grep -i "error\|exception\|failed"

# Check data was extracted
find extracted_data -mmin -5 -type f | wc -l
# Should show files modified in last 5 minutes
```

---

## Data Persistence

### Volume Mounting

All data is stored in Docker volumes on the host filesystem:

```yaml
volumes:
  vcom_extracted_data:  → ./extracted_data/
  vcom_logs:           → ./logs/
  vcom_database:       → ./db/
  vcom_errors:         → ./errors/
  vcom_artifacts:      → ./artifacts/
```

### Backing Up Data

```bash
# Backup extracted data
cp -r extracted_data exported_data_$(date +%Y%m%d_%H%M%S)

# Backup entire volumes
docker-compose exec vcom-automation tar czf /tmp/backup.tar.gz /app/extracted_data /app/db /app/logs
docker cp vcom_automation_system:/tmp/backup.tar.gz ./backups/

# Restore from backup
tar xzf backup.tar.gz -C ./
```

### Manual Volume Management

```bash
# List volumes
docker volume ls | grep vcom

# Inspect volume location
docker volume inspect vcom_extracted_data

# Clean up unused volumes (careful!)
docker volume prune
```

---

## Logs & Troubleshooting

### Common Issues & Solutions

#### Issue 1: Dashboard Port Already in Use

```bash
# Error: bind: address already in use

# Solution 1: Change port in .env
DASHBOARD_PORT=8081

# Solution 2: Kill process using port 8080
lsof -i :8080          # Find PID
kill -9 <PID>          # Kill process

# Docker-specific
docker ps -a | grep 8080
docker stop <container_id>
```

#### Issue 2: VCOM Authentication Failed

```bash
# Check credentials in logs
docker-compose logs | grep -i "auth\|login\|failed"

# Verify credentials in .env
cat .env | grep VCOM_

# Test VCOM credentials manually
docker-compose exec vcom-automation python -c "
import requests
from pathlib import Path
import json
config = json.load(open('config.json'))
print(f'URL: {config[\"SYSTEM_URL\"]}')
print(f'User: {config[\"USERNAME\"]}')
# Don't print password
"
```

#### Issue 3: Out of Memory

```bash
# Error: OOMKilled or "Cannot allocate memory"

# Solution: Increase memory limit in .env
CONTAINER_MEMORY_LIMIT=8G

# Or via docker-compose up
docker run -m 4g vcom-automation:latest

# Check actual memory usage
docker stats vcom_automation_system
```

#### Issue 4: Playwright Browser Issues

```bash
# Error: "Failed to launch chromium"

# Reinstall browsers
docker-compose exec vcom-automation playwright install chromium

# Or rebuild from scratch
docker-compose down
docker system prune
docker-compose build --no-cache
docker-compose up -d
```

#### Issue 5: LLM Connection Failed

```bash
# Error: Cannot connect to Ollama/OpenAI/Anthropic

# For Ollama:
# Make sure Ollama is running locally
ollama serve

# Check LLM logs
docker-compose logs | grep -i "llm\|error"

# Test LLM endpoint manually
docker-compose exec vcom-automation curl http://localhost:11434/api/tags
```

### Inspecting Container Internals

```bash
# Access container shell (for debugging)
docker-compose exec vcom-automation bash

# Inside container:
ls -la                           # List files
python -V                        # Check Python version
pip list | head                  # Check installed packages
ps aux | grep python             # See running processes
cat /proc/sys/kernel/hostname    # Container name
env | grep VCOM                  # Check environment variables
```

### Collecting Diagnostic Information

```bash
# Save comprehensive diagnostic logs
docker-compose logs vcom-automation > diagnostic.log
docker version >> diagnostic.log
docker info >> diagnostic.log
docker-compose config >> diagnostic.log
env | grep -E "VCOM|DASHBOARD|LLM" >> diagnostic.log

# Share diagnostic.log (remove sensitive info first!)
grep -v "PASSWORD\|TOKEN\|KEY" diagnostic.log
```

---

## Advanced Usage

### Running Multiple VCOM Instances

```bash
# Use docker-compose with profile suffix
docker-compose -f docker-compose.yml -p vcom_site1 up -d
docker-compose -f docker-compose.yml -p vcom_site2 up -d

# List all instances
docker-compose ps -a
```

### Using External Database

```bash
# Modify docker-compose.yml to use external db service
# Add a postgres/mysql service or mount network storage
volumes:
  vcom_database:
    driver: local
    driver_opts:
      type: nfs
      o: addr=192.168.1.100,vers=4,soft,timeo=180,bg,tcp,rw
      device: ":/exports/vcom_db"
```

### Building for Different Architectures

```bash
# Build for ARM64 (e.g., Apple Silicon, ARM servers)
docker buildx build --platform linux/arm64 -t vcom-automation:arm64 .

# Build multi-platform image
docker buildx build --platform linux/amd64,linux/arm64 -t vcom-automation:latest .
```

### Custom Entry Point

```bash
# Override default command
docker-compose exec vcom-automation python dashboard/app.py

# Run one-off extraction
docker-compose exec vcom-automation python vcom_monitor.py

# Run LLM agent only
docker-compose exec vcom-automation python llm_agent.py
```

### Export Image for Offline Use

```bash
# Save image to tar file
docker save vcom-automation:latest -o vcom-automation.tar

# Load image on another machine
docker load -i vcom-automation.tar

# Verify
docker images | grep vcom-automation
```

---

## Security Considerations

### Best Practices Implemented

✅ **Multi-stage build** - Minimal production image size
✅ **Non-root user** - Container runs as `vcom:vcom` (uid:gid = 1000:1000)
✅ **No hardcoded secrets** - All credentials via environment variables
✅ **Read-only config** - Config files mounted as read-only
✅ **Health checks** - Automatic monitoring of service health
✅ **Graceful shutdown** - Proper signal handling (SIGTERM)
✅ **Resource limits** - CPU and memory constraints prevent runaway resource use
✅ **Logging** - Structured logs with rotation

### Additional Hardening Steps

```bash
# 1. Use secrets management instead of .env files (production)
# Recommended: AWS Secrets Manager, HashiCorp Vault, Docker Secrets

# 2. Enable read-only filesystem (except volumes)
# In docker-compose.yml:
security_opt:
  - no-new-privileges=true
read_only: true  # Mount only necessary dirs as RW

# 3. Restrict network access
networks:
  isolated:
    driver: bridge
    ipam:
      config:
        - subnet: 172.28.0.0/16

# 4. Use user namespace remapping
userns_mode: "host"

# 5. Scan image for vulnerabilities
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
  aquasec/trivy image vcom-automation:latest
```

### .env File Security

```bash
# Make .env file read-only
chmod 600 .env
chown $(whoami):$(whoami) .env

# Don't commit to git (already in .gitignore)
# Use git-crypt or similar for encrypted secrets if needed

# For CI/CD pipelines
# Use: GitHub Secrets, GitLab Variables, Jenkins Credentials, etc.
```

---

## Troubleshooting Commands Reference

```bash
# System status
docker-compose ps
docker-compose ps -a

# View logs
docker-compose logs
docker-compose logs -f
docker-compose logs -f --tail=50

# Resource usage
docker stats vcom_automation_system

# Container inspection
docker-compose exec vcom-automation sh
docker-compose exec vcom-automation python -V
docker-compose exec vcom-automation pip list

# Clean up
docker-compose down          # Stop and remove containers
docker-compose down -v       # Remove volumes too (data loss!)
docker system prune          # Remove unused images/volumes
docker volume prune          # Remove unused volumes

# Rebuild
docker-compose build --no-cache

# Force restart
docker-compose restart

# Logs to file
docker-compose logs > /tmp/vcom.log 2>&1
```

---

## Next Steps

1. **Setup**: Copy `.env.example` to `.env` and configure your credentials
2. **Build**: Run `docker-compose build`
3. **Deploy**: Run `docker-compose up -d`
4. **Verify**: Check logs and access dashboard at `http://localhost:8080`
5. **Monitor**: Watch logs with `docker-compose logs -f`
6. **Backup**: Regularly backup data in extracted_data/ and db/ directories

---

## Support & Maintenance

### Regular Maintenance Tasks

```bash
# Weekly: Check logs for errors
docker-compose logs | grep -i "error"

# Monthly: Backup data
cp -r extracted_data backups/extracted_data_$(date +%Y%m%d)
cp -r db backups/db_$(date +%Y%m%d)

# Quarterly: Update base image
docker-compose down
docker pull mcr.microsoft.com/playwright/python:latest
docker-compose build --no-cache
docker-compose up -d
```

### Getting Help

1. Check logs: `docker-compose logs -f`
2. Review this guide's troubleshooting section
3. Test components individually with `docker-compose exec`
4. Check Docker documentation: https://docs.docker.com/
5. Check project repository for issues

---

## License & Attribution

VCOM Automation System - Docker Edition
Built with production-grade security and reliability standards.

---

**Last Updated**: May 2026
**Docker Version**: 20.10+
**Compose Version**: 1.29+
