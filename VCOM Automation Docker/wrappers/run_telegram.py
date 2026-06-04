"""
wrappers/run_telegram.py

Thin entry-point wrapper for telegram_bot.py.

Purpose:
  telegram_bot.py hardcodes the Odoo client instantiation at line 131:
      client = OdooClient("http://localhost:8069", "odoo", ...)
  In Docker, localhost:8069 resolves to the container itself, not the
  Odoo container on the host.

  This wrapper monkey-patches OdooClient so its __init__ receives the
  correct env-var-sourced URL before telegram_bot is imported (and its
  module-level code runs).

Source file is never modified.
"""

import logging
import os
import sys

# Ensure /app (git root) is on the path.
sys.path.insert(0, "/app")

# ---------------------------------------------------------------------------
# Read Odoo config from environment
# ---------------------------------------------------------------------------
ODOO_URL  = os.environ.get("ODOO_URL",  "http://host.docker.internal:8069")
ODOO_DB   = os.environ.get("ODOO_DB",   "odoo")
ODOO_USER = os.environ.get("ODOO_USER", "")
ODOO_PASS = os.environ.get("ODOO_PASS", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TELEGRAM-WRAPPER] %(levelname)s %(message)s",
)
log = logging.getLogger("telegram_wrapper")

log.info("Odoo endpoint (for ticket creation): %s", ODOO_URL)

# ---------------------------------------------------------------------------
# Patch OdooClient before telegram_bot imports it
# telegram_bot.py line 131: OdooClient("http://localhost:8069", "odoo", ...)
# Strategy: wrap OdooClient.__init__ to intercept the hardcoded URL arg.
# ---------------------------------------------------------------------------
from db.odoo_client import OdooClient as _OrigOdooClient  # noqa: E402

_original_init = _OrigOdooClient.__init__

def _patched_init(self, url, db, user, password):
    """Always use env-var values — ignore whatever was hardcoded in source."""
    resolved_url  = ODOO_URL  or url
    resolved_db   = ODOO_DB   or db
    resolved_user = ODOO_USER or user
    resolved_pass = ODOO_PASS or password
    _original_init(self, resolved_url, resolved_db, resolved_user, resolved_pass)

_OrigOdooClient.__init__ = _patched_init
log.info("OdooClient.__init__ patched — hardcoded localhost URLs will be redirected.")

# ---------------------------------------------------------------------------
# Now import and run telegram_bot
# ---------------------------------------------------------------------------
import telegram_bot as _bot  # noqa: E402

log.info("Handing off to telegram_bot main().")

if hasattr(_bot, "main"):
    _bot.main()
else:
    log.error("telegram_bot has no main() function. Check the source file.")
    sys.exit(1)
