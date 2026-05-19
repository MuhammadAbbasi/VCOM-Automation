# Docker Implementation Guide for VCOM Automation System

## Objective
Containerize the VCOM Automation System into a production-ready Docker setup that enables portable deployment across any machine with Docker installed.

## System Overview
The VCOM Automation System consists of 8 concurrent services:

1. **DASHBOARD** - FastAPI web dashboard (port 8080)
2. **WATCHDOG** - SCADA data monitoring & analysis
3. **EXTRACTION** - Playwright-based web scraping from VCOM system
4. **TELEGRAM** - Alert bot notifications
5. **BROKER** - MQTT message broker (port 1883)
6. **TRACKER** - MQTT client receiving telemetry
7. **DOCTOR** - Health monitoring & diagnostics
8. **TICKETS** - Odoo ticket engine integration

## Key Requirements

### Build Requirements
- **Base Image**: Microsoft Playwright Python image (v1.60.0-jammy minimum)
  - Pre-installed with Chromium browser
  - Xvfb for headless display
  - Python 3.10+
- **Multi-stage Build**: Separate builder and runtime stages for security and size optimization
- **Non-root User**: Execute container as `vcom:vcom` user (not root)
- **Python Dependencies**: All packages from requirements.txt (pandas, numpy, playwright, fastapi, etc.)

### Runtime Requirements
- **Environment Variables**: All secrets via `.env` file (VCOM credentials, API keys, etc.)
- **Volume Mounts**: 
  - `/app/extracted_data` - Extraction output
  - `/app/logs` - System logs
  - `/app/db` - SQLite database
  - `/app/errors` - Error logs
  - `/app/artifacts` - Temporary files
- **X11 Display**: xvfb-run for headless browser automation
  - Use shell form CMD to propagate DISPLAY environment to child processes
  - Format: `CMD sh -c "xvfb-run -a --server-args='-screen 0 1450x900x24 -ac' python -u run_monitor.py"`
- **Health Checks**: HTTP endpoint test on dashboard port 8080
  - Interval: 30s, Timeout: 10s, Retries: 3, Start period: 30s
- **Resource Limits**: 
  - CPU: 4 cores (limit), 2 cores (reserve)
  - Memory: 4GB (limit), 2GB (reserve)
- **Graceful Shutdown**: SIGTERM handling with 30-second grace period

### Docker Compose Configuration
- **Service**: Single container with all services inside (not microservices)
- **Network**: Custom bridge network named `vcom-network`
- **Logging**: JSON-file driver with 100MB max size, 10 files rotation
- **Ports**: Expose 8080 (dashboard) and map any external services

## Critical Implementation Details

### 1. Dockerfile Multi-Stage Build
```
Stage 1 (Builder):
- Install build dependencies (gcc, make, libssl-dev, etc.)
- Copy requirements.txt and install Python packages
- Run: python -m playwright install chromium
- Output: /root/.local (Python packages) to be copied to Stage 2

Stage 2 (Runtime):
- Install only runtime dependencies (xvfb, curl, libgbm-dev, etc.)
- Create non-root user (vcom:vcom)
- Copy Python packages from builder
- Copy application code (selective, exclude test files)
- Create volume directories with proper ownership
- Set PYTHONUNBUFFERED=1 and DISPLAY=:99
- Expose port 8080
- Use shell-form CMD with xvfb-run
```

### 2. Environment Variables (.env file)
Required variables:
- `VCOM_USERNAME`, `VCOM_PASSWORD`, `VCOM_SYSTEM_URL` - SCADA credentials
- `NGROK_AUTH_TOKEN` - Ngrok tunnel authentication
- `DASHBOARD_USER`, `DASHBOARD_PASS` - Web dashboard login
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` - Alert notifications
- `ODOO_URL`, `ODOO_DATABASE`, `ODOO_USERNAME`, `ODOO_PASSWORD` - Ticket system
- `OLLAMA_URL`, `OLLAMA_MODEL` - LLM configuration (local) or cloud API keys
- `DASHBOARD_PORT`, `CONTAINER_CPU_LIMIT`, `CONTAINER_MEMORY_LIMIT` - Deployment settings
- `TZ` - Timezone (UTC)

### 3. .dockerignore Optimization
Exclude to reduce build context:
- Deprecated code: `processor_watchdog_v[123]`, `processor_watchdog_deprecated`
- Test scripts: `scratch/` directory (70+ test files)
- Temporary files: `temp_llm_codes/`, `*.tmp`, `*.pyc`
- Version control: `.git/`, `.gitignore`
- Documentation: `*.md` (except required docs)
- Data: `.extraction_busy`, `extracted_data/`, `logs/`, `errors/`

### 4. Health Check Implementation
```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1
```
This ensures Docker knows when the container is healthy by verifying the dashboard responds.

### 5. X11 Display Fix
**Critical Issue**: Child processes spawned by run_monitor.py don't inherit DISPLAY from xvfb-run.

**Solution**: Use shell-form CMD instead of array form:
- ❌ Wrong: `CMD ["xvfb-run", "--server-args=...", "python", "run_monitor.py"]`
- ✅ Correct: `CMD sh -c "xvfb-run -a --server-args='-screen 0 1450x900x24 -ac' python -u run_monitor.py"`

This ensures xvfb-run properly sets the environment for all child processes.

### 6. Process Management
- **Orchestrator**: `run_monitor.py` spawns all 8 services as subprocesses
- **Service Tracking**: Each service has a restart mechanism (5-second cooldown)
- **Logging**: STDOUT/STDERR captured and prefixed with service name
- **Shutdown**: Orchestrator handles SIGTERM and gracefully stops all services

## Implementation Steps

1. **Create Dockerfile**:
   - Multi-stage build with builder and runtime stages
   - Use Playwright v1.60.0 or later
   - Install all runtime dependencies
   - Create non-root user and set ownership
   - Use shell-form CMD with xvfb-run

2. **Create docker-compose.yml**:
   - Define single service: vcom-automation
   - Mount all required volumes
   - Set all environment variables from .env
   - Configure health checks
   - Set resource limits
   - Configure logging

3. **Create .dockerignore**:
   - Exclude deprecated code
   - Exclude test files
   - Exclude runtime data
   - Exclude version control

4. **Create .env.example**:
   - Document all required variables
   - Provide sensible defaults
   - Include usage instructions
   - Add security warnings

5. **Create deploy.ps1** (for Windows):
   - Pre-flight checks (Docker, Docker Compose installed)
   - Create data directories
   - Validate configuration
   - Build image with --no-cache flag
   - Start container and wait for health check
   - Display next steps and logs command

## Testing & Validation

1. **Build Image**:
   ```bash
   docker-compose build --no-cache
   ```
   Verify: No errors, Playwright installed correctly

2. **Start Container**:
   ```bash
   docker-compose up -d
   ```
   Verify: Container starts, health check passes within 60 seconds

3. **Test Services**:
   - Dashboard: `curl http://localhost:8080/`
   - Logs: `docker-compose logs --tail=50`
   - Extraction: Check `extracted_data/` folder for new files
   - Database: Verify `db/scada_data.db` is accessible

4. **Verify All 8 Services**:
   ```bash
   docker-compose logs | grep -E "DASHBOARD|WATCHDOG|EXTRACTION|TELEGRAM|BROKER|TRACKER|DOCTOR|TICKETS"
   ```

## Known Issues & Solutions

### Issue 1: Playwright Browser Not Found
**Symptom**: `BrowserType.launch_persistent_context: Executable doesn't exist at /ms-playwright/chromium-1223/chrome-linux64/chrome`
**Cause**: Playwright base image version mismatch
**Solution**: Use Playwright v1.60.0 or later in base image

### Issue 2: X11 Display Not Found
**Symptom**: `Missing X server or $DISPLAY` error in Playwright
**Cause**: Array-form CMD doesn't propagate DISPLAY to child processes
**Solution**: Use shell-form CMD with xvfb-run

### Issue 3: MQTT Port Already in Use
**Symptom**: `OSError: [Errno 98] error while attempting to bind on address ('0.0.0.0', 1883)`
**Cause**: Previous container instance still occupies port
**Solution**: Run `docker-compose down` before starting

### Issue 4: Health Check Fails
**Symptom**: Container shows "(unhealthy)" status
**Cause**: Dashboard takes time to start or dependencies not ready
**Solution**: Increase start-period to 60+ seconds in health check config

## Performance Optimization

1. **Build Context**: Use .dockerignore to exclude 57% of files (reduces from ~200MB to ~85MB)
2. **Layer Caching**: Separate dependencies from code changes for faster rebuilds
3. **Multi-stage Build**: Final image excludes build tools, reducing size by ~500MB
4. **Resource Limits**: Set CPU/memory limits to prevent system strain

## Security Hardening

1. **Non-root User**: All processes run as `vcom:vcom`, not root
2. **Secrets Management**: Use .env file (not hardcoded in image)
3. **Read-only Filesystem**: Consider `--read-only` flag for production
4. **Network Isolation**: Custom bridge network separates from other containers
5. **Registry Scanning**: Scan image for vulnerabilities before deployment

## Production Deployment

1. **Push Image to Registry**:
   ```bash
   docker tag vcomautomation-vcom-automation:latest myregistry/vcom:v1.0
   docker push myregistry/vcom:v1.0
   ```

2. **Deploy with Docker Swarm or Kubernetes**:
   - Use registry image reference
   - Mount volumes on persistent storage
   - Set resource requests and limits
   - Configure auto-restart policies

3. **Monitoring**:
   - Set up log aggregation (ELK, Splunk, etc.)
   - Monitor health check status
   - Track resource usage
   - Alert on service failures

## Rollback & Troubleshooting

1. **If Build Fails**:
   - Check Dockerfile syntax
   - Verify base image availability
   - Run with `--progress=plain` for detailed output

2. **If Container Won't Start**:
   - Check logs: `docker-compose logs`
   - Verify environment variables in .env
   - Ensure volumes have proper permissions

3. **If Services Crash**:
   - Check individual service logs
   - Verify database connectivity
   - Check credential validity
   - Review resource limits (may be too low)

## References

- **Dockerfile Best Practices**: https://docs.docker.com/develop/dev-best-practices/
- **Docker Compose**: https://docs.docker.com/compose/
- **Playwright Containerization**: https://playwright.dev/python/docs/docker
- **xvfb-run**: `man xvfb-run`
