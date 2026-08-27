"""
plant_map_routes.py — FastAPI routes for 3-level interactive plant map.

Routes:
  GET /api/plant/layout        → Plant topology (sections, inverter positions)
  GET /api/plant/overview      → Level 1: All inverters with health
  GET /api/plant/inverter/{id}/strings  → Level 2: Strings in an inverter
"""

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Depends, Request

# Add parent to path for imports
DASHBOARD_DIR = Path(__file__).resolve().parent
ROOT = DASHBOARD_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from db.plant_map_helpers import (
    load_plant_layout,
    get_plant_overview,
    get_inverter_health_overview,
    get_inverter_strings_detail
)
from processor_watchdog_final import load_config

async def check_auth(request: Request):
    from dashboard.app import is_authenticated
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

# Router for plant map routes
router = APIRouter(prefix="/api/plant", tags=["plant-map"], dependencies=[Depends(check_auth)])


@router.get("/layout")
async def get_plant_layout():
    """
    Returns plant topology: sections, inverter locations, boundaries.

    Use this for the initial map rendering (Level 1).
    """
    try:
        layout = load_plant_layout()
        if not layout:
            raise HTTPException(status_code=404, detail="Plant layout not found")

        return {
            "metadata": layout.get("metadata", {}),
            "sections": layout.get("sections", []),
            "inverter_locations": layout.get("inverter_locations", {})
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading plant layout: {str(e)}")


@router.get("/overview")
async def get_plant_health_overview(
    date: str = Query(None, description="YYYY-MM-DD format, defaults to today")
):
    """
    LEVEL 1: Get all inverters with health status for map visualization.

    Response includes:
      - Each inverter's location (x, y)
      - Health status (green/yellow/red)
      - Health score (0-100)
      - Tracker/string counts
      - Sample metrics

    Used to render colored circles on the plant map.
    """
    try:
        overview = await asyncio.to_thread(get_plant_overview, date)
        return overview
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating plant overview: {str(e)}")


@router.get("/inverter/{inverter_id}/health")
async def get_inverter_quick_health(
    inverter_id: str,
    date: str = Query(None)
):
    """
    Quick health check for a single inverter (used for hover/tooltip).
    """
    try:
        health = await asyncio.to_thread(get_inverter_health_overview, inverter_id, date)
        if "error" in health:
            raise HTTPException(status_code=404, detail=health["error"])
        return health
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/inverter/{inverter_id}/strings")
async def get_inverter_strings(
    inverter_id: str,
    date: str = Query(None)
):
    """
    LEVEL 2: Get detailed string layout for a clicked inverter.

    Response includes:
      - All strings in the inverter
      - Each string's health status (green/yellow/red)
      - Tracker assignments
      - MPPT assignments
      - Summary counts (healthy, warning, critical)

    Used to render a grid/table of strings with color coding.
    """
    try:
        strings_detail = await asyncio.to_thread(get_inverter_strings_detail, inverter_id, date)
        if "error" in strings_detail:
            raise HTTPException(status_code=404, detail=strings_detail["error"])
        return strings_detail
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# Export router for inclusion in main FastAPI app
# In dashboard/app.py, add:
#   from dashboard.plant_map_routes import router as plant_map_router
#   app.include_router(plant_map_router)


# ---------------------------------------------------------------------------
# Surveyed plant map: as-built topology (370 trackers, 808 strings, 432 MPPTs)
# with severity taken from the watchdog's existing anomalies and health flags.
# ---------------------------------------------------------------------------

@router.get("/surveyed/layout")
async def get_surveyed_layout_route():
    """Static topology and geometry for the surveyed map."""
    try:
        from db.surveyed_map_helpers import load_surveyed_layout
        layout = load_surveyed_layout()
        if not layout:
            raise HTTPException(status_code=404, detail="Surveyed layout not found")
        return layout
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading surveyed layout: {e}")


@router.get("/surveyed/state")
async def get_surveyed_state_route(
    date: str = Query(None, description="YYYY-MM-DD, defaults to today")
):
    """Per-element severity plus the active problem list behind it."""
    try:
        from db.surveyed_map_helpers import get_surveyed_state
        return await asyncio.to_thread(get_surveyed_state, date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error building surveyed state: {e}")


@router.get("/surveyed/serials")
async def get_surveyed_serials_route(
    string: str = Query(None, description="String id, e.g. TX1-INV07-STR22"),
    tracker: str = Query(None, description="Tracker id, e.g. TRACKER 76"),
):
    """Panel serial numbers for one string or one tracker (20 200 in total)."""
    try:
        from db.surveyed_map_helpers import get_panel_serials
        return await asyncio.to_thread(get_panel_serials, string, tracker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading panel serials: {e}")
