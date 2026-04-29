"""
plant_map_routes.py — FastAPI routes for 3-level interactive plant map.

Routes:
  GET /api/plant/layout        → Plant topology (sections, inverter positions)
  GET /api/plant/overview      → Level 1: All inverters with health
  GET /api/plant/inverter/{id}/strings  → Level 2: Strings in an inverter
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query

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

# Router for plant map routes
router = APIRouter(prefix="/api/plant", tags=["plant-map"])


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
        overview = get_plant_overview(date)
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
        health = get_inverter_health_overview(inverter_id, date)
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
        strings_detail = get_inverter_strings_detail(inverter_id, date)
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
