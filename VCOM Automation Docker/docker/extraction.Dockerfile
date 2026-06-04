# ============================================================
# extraction.Dockerfile — VCOM Playwright Scraper Image
# Used by: extraction service only
# Build context: VCOM Automation/ (git root)
# ============================================================
# Extends base with Chromium browser + Playwright system deps.
# Chromium is installed once at build time and cached in the
# image layer — not re-downloaded on each container start.
# ============================================================

FROM python:3.12-slim-bookworm AS base

# System dependencies (same as base.Dockerfile + Playwright deps)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gettext-base \
    libsqlite3-0 \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python packages (including playwright pip package)
COPY ["VCOM Automation Docker/requirements.docker.txt", "/tmp/requirements.docker.txt"]
RUN pip install --no-cache-dir -r /tmp/requirements.docker.txt \
    && apt-get purge -y gcc \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --force-reinstall "setuptools>=69.0.0"

# Set browser path BEFORE installing so playwright bakes it into the right location
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Install Playwright Chromium browser + all its system-level dependencies.
# --with-deps installs libnss, libatk, libglib, libdrm, etc. automatically.
RUN playwright install chromium --with-deps

# Copy application source
COPY . /app/

# Copy config templates, entrypoint, and wrappers
RUN cp -r /app/"VCOM Automation Docker/config" /templates && \
    cp /app/"VCOM Automation Docker/scripts/entrypoint.sh" /entrypoint.sh && \
    chmod +x /entrypoint.sh && \
    cp -r /app/"VCOM Automation Docker/wrappers" /wrappers

# Pre-create runtime directories
RUN mkdir -p /app/db /app/extracted_data /app/logs /app/errors /app/VCOM_Screenshots
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# VCOM_HEADLESS is set to "true" in docker-compose for container use.
# Override to "false" only for local debugging with a real display.
ENV VCOM_HEADLESS=true

ENTRYPOINT ["/entrypoint.sh"]
