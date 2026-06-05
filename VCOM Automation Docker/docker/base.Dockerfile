# ============================================================
# base.Dockerfile — SCADA App Services Base Image
# Used by: dashboard, watchdog, telegram, tickets, broker, tracker
# Build context: VCOM Automation/ (git root)
# ============================================================

FROM python:3.12-slim-bookworm AS base

# System dependencies
#   - gettext-base  : provides envsubst for entrypoint config rendering
#   - libsqlite3-0  : SQLite runtime (Python sqlite3 module uses this)
#   - curl          : health-check and Ollama reachability tests
# NOTE: gcc is intentionally omitted — all pip packages use pre-compiled
# manylinux wheels. Installing gcc pulls in python3-setuptools via apt,
# then apt-autoremove wipes pkg_resources and breaks amqtt at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gettext-base \
    libsqlite3-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (Docker layer cache friendly)
COPY ["VCOM Automation Docker/requirements.docker.txt", "/tmp/requirements.docker.txt"]
RUN pip install --no-cache-dir -r /tmp/requirements.docker.txt

# Copy application source code (source repo root)
# .dockerignore excludes runtime-generated files (db/, logs/, etc.)
COPY . /app/

# Copy config templates and entrypoint into well-known locations
RUN cp -r /app/"VCOM Automation Docker/config" /templates && \
    cp /app/"VCOM Automation Docker/scripts/entrypoint.sh" /entrypoint.sh && \
    chmod +x /entrypoint.sh && \
    cp -r /app/"VCOM Automation Docker/wrappers" /wrappers

# Pre-create directories that services write to at runtime.
# These are overridden by named volume mounts in docker-compose.
RUN mkdir -p /app/db /app/extracted_data /app/logs /app/errors

# Ensure PYTHONPATH includes the app root so all local imports resolve
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/entrypoint.sh"]
