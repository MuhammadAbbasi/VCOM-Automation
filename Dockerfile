# ============================================================================
# MULTI-STAGE BUILD: VCOM Automation System
# ============================================================================
# Stage 1: Builder - Install dependencies and prepare environment
# ============================================================================
FROM mcr.microsoft.com/playwright/python:v1.60.0-jammy as builder

WORKDIR /build

# Install system dependencies required for build
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Install Playwright browsers in build stage
RUN python -m playwright install chromium


# ============================================================================
# Stage 2: Runtime - Lightweight production image
# ============================================================================
FROM mcr.microsoft.com/playwright/python:v1.60.0-jammy

LABEL maintainer="VCOM Automation Team"
LABEL description="VCOM Automation System - SCADA Monitoring & LLM Agents"
LABEL version="1.0"

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    VCOM_HEADLESS=false \
    DISPLAY=:99 \
    TZ=UTC

# Install runtime dependencies (no build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    xvfb \
    libgbm-dev \
    libxss1 \
    libappindicator1 \
    libindicator7 \
    sqlite3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r vcom && useradd -r -g vcom vcom

# Copy Python dependencies from builder stage
COPY --from=builder /root/.local /home/vcom/.local

# Ensure the local bin directory is in PATH
ENV PATH=/home/vcom/.local/bin:$PATH

# Copy only essential application files (excluding redundant/temp code)
# Core orchestrator and main modules
COPY run_monitor.py \
    processor_watchdog_final.py \
    vcom_monitor.py \
    telegram_bot.py \
    dashboard_doctor.py \
    odoo_ticket_engine.py \
    submit_ticket.py \
    llm_agent.py \
    llm_agent_v2.py \
    mppt_dc_analyzer.py \
    ai_system_prompt.txt \
    requirements.txt \
    ./

# Core application code
COPY extraction_code/ ./extraction_code/
COPY dashboard/ ./dashboard/
COPY tracker_testing/ ./tracker_testing/
COPY db/ ./db/
COPY assets/ ./assets/

# Create necessary directories for volumes with proper permissions
RUN mkdir -p /app/extracted_data \
    && mkdir -p /app/logs \
    && mkdir -p /app/db \
    && mkdir -p /app/errors \
    && mkdir -p /app/artifacts \
    && chown -R vcom:vcom /app

# Set working directory ownership
RUN chown -R vcom:vcom /home/vcom

# Switch to non-root user
USER vcom

# Health check - verify dashboard is responding
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Expose dashboard and monitoring ports
EXPOSE 8080

# Volume mounts for persistent data
VOLUME ["/app/extracted_data", "/app/logs", "/app/db", "/app/errors", "/app/artifacts"]

# Graceful shutdown signal handler
STOPSIGNAL SIGTERM

# Entry point with xvfb-run for headless browser support
# Default command - runs the main orchestrator
# Using shell form to ensure DISPLAY is properly set for all subprocesses
CMD sh -c "xvfb-run -a --server-args='-screen 0 1450x900x24 -ac' python -u run_monitor.py"
