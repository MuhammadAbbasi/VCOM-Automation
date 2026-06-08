"""
wrappers/run_tickets.py

Thin entry-point wrapper for odoo_ticket_engine.py.

Purpose:
  odoo_ticket_engine.py hardcodes Odoo connection constants at module level
  (lines 52-55) and does not read environment variables for them.
  In Docker, 127.0.0.1:8069 won't route to the Odoo container on the host.
  This wrapper monkey-patches those constants from env vars BEFORE the module
  executes any code that uses them, then calls the original main() function.

Source file is never modified.
"""

import logging
import os
import sys
import time

# Ensure /app (git root) is on the path so all source imports resolve.
sys.path.insert(0, "/app")

# ---------------------------------------------------------------------------
# Read Odoo config from environment (set via docker-compose / .env)
# ---------------------------------------------------------------------------
ODOO_URL  = os.environ.get("ODOO_URL",      "http://host.docker.internal:8069")
ODOO_DB   = os.environ.get("ODOO_DB")   or os.environ.get("ODOO_DATABASE",  "odoo")
ODOO_USER = os.environ.get("ODOO_USER") or os.environ.get("ODOO_USERNAME",  "")
ODOO_PASS = os.environ.get("ODOO_PASS") or os.environ.get("ODOO_PASSWORD",  "") \
                                        or os.environ.get("ODOO_API_KEY",   "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TICKETS-WRAPPER] %(levelname)s %(message)s",
)
log = logging.getLogger("tickets_wrapper")

log.info("Odoo endpoint : %s", ODOO_URL)
log.info("Odoo DB       : %s", ODOO_DB)
log.info("Odoo user     : %s", ODOO_USER)

if not ODOO_USER or not ODOO_PASS:
    log.warning(
        "ODOO_USER or ODOO_PASS is empty — ticket creation will fail. "
        "Set these in your .env file."
    )

# ---------------------------------------------------------------------------
# Connectivity pre-check (non-blocking)
# ---------------------------------------------------------------------------
try:
    import requests as _req
    resp = _req.get(f"{ODOO_URL}/web/database/selector", timeout=5)
    log.info("Odoo reachable — HTTP %s", resp.status_code)
except Exception as exc:
    log.warning("Odoo not reachable at startup (%s) — will retry inside engine.", exc)

# ---------------------------------------------------------------------------
# Import the source module and patch its module-level constants
# ---------------------------------------------------------------------------
import odoo_ticket_engine as _eng  # noqa: E402

_eng.ODOO_URL  = ODOO_URL
_eng.ODOO_DB   = ODOO_DB
_eng.ODOO_USER = ODOO_USER
_eng.ODOO_PASS = ODOO_PASS

log.info("Patched odoo_ticket_engine constants — handing off to engine main().")

# ---------------------------------------------------------------------------
# Run the engine loop (mirrors the if __name__ == '__main__' block in
# odoo_ticket_engine.py which calls run() every 15 minutes)
# ---------------------------------------------------------------------------
log.info("Odoo Ticket Engine started.")
while True:
    try:
        _eng.run()
    except KeyboardInterrupt:
        log.info("Interrupted — shutting down.")
        break
    except Exception as exc:
        log.error("Engine crashed: %s — resuming after 60s.", exc)
        try:
            _eng.send_telegram(f"🚨 *Odoo Ticket Engine CRASHED*\nError: `{exc}`")
        except Exception:
            pass
    log.info("Sleeping 15 min...")
    try:
        time.sleep(900)
    except KeyboardInterrupt:
        log.info("Interrupted during sleep — shutting down.")
        break

log.info("Odoo Ticket Engine shut down.")
