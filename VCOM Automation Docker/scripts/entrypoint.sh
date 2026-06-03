#!/bin/sh
# ============================================================
# entrypoint.sh — Container startup: render configs then exec
# ============================================================
# Runs as PID 1 inside every SCADA container.
# 1. Uses envsubst to render JSON config files from templates,
#    injecting secrets from environment variables at runtime.
# 2. Execs the CMD passed by docker-compose (the service script).
#
# IMPORTANT: envsubst is called with an explicit variable list
# to avoid corrupting JSON characters like { } $ [ ].
# ============================================================

set -e

TEMPLATES_DIR="/templates"
APP_DIR="/app"

echo "[entrypoint] Rendering config files from templates..."

# ---- config.json ----
envsubst '${VCOM_USERNAME} ${VCOM_PASSWORD} ${VCOM_SYSTEM_URL} ${NGROK_AUTH_TOKEN} ${DASHBOARD_USER} ${DASHBOARD_PASS} ${INVERTER_IDS_JSON}' \
    < "${TEMPLATES_DIR}/config.json.template" \
    > "${APP_DIR}/config.json"

echo "[entrypoint] config.json written (VCOM_USERNAME=${VCOM_USERNAME})"

# ---- user_settings.json ----
envsubst '${TELEGRAM_BOT_TOKEN} ${TELEGRAM_CHAT_ID} ${TELEGRAM_PERSONAL_ID}' \
    < "${TEMPLATES_DIR}/user_settings.json.template" \
    > "${APP_DIR}/user_settings.json"

echo "[entrypoint] user_settings.json written"

# ---- Ensure runtime directories exist ----
# (Named volumes are mounted here; mkdir -p is idempotent)
mkdir -p "${APP_DIR}/db" \
         "${APP_DIR}/extracted_data" \
         "${APP_DIR}/logs" \
         "${APP_DIR}/errors"

echo "[entrypoint] Runtime directories verified."
echo "[entrypoint] Starting: $@"
echo "[entrypoint] ----------------------------------------"

# Hand off to the CMD specified by docker-compose (exec replaces
# this shell process so the service script becomes PID 1 and
# receives SIGTERM properly for graceful shutdown).
exec "$@"
