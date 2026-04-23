"""
db/migrate_csv_to_db.py — One-time migration of existing CSV/JSON data to SQLite.

Scans the extracted_data/ directory for all CSV and JSON files, parses them,
and imports them into the scada_data.db database.

Run with:
    python -m db.migrate_csv_to_db
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.db_manager import (
    init_databases,
    save_metric,
    save_analysis_snapshot,
    save_extraction_status,
    METRIC_TABLE_MAP,
    _resolve_table_name,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MIGRATION] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("migration")

DATA_DIR = ROOT / "extracted_data"

# Map of filename prefixes to metric names.
# Handles both space and underscore naming conventions that evolved over time.
PREFIX_MAP = {
    "Potenza AC":                  "Potenza AC",
    "Potenza_AC":                  "Potenza AC",
    "Temperatura":                 "Temperatura",
    "Resistenza di isolamento":    "Resistenza di isolamento",
    "Resistenza_Isolamento":       "Resistenza di isolamento",
    "Irraggiamento":               "Irraggiamento",
    "PR inverter":                 "PR inverter",
    "PR":                          "PR inverter",
    "Corrente DC":                 "Corrente DC",
    "Corrente_DC":                 "Corrente DC",
}


def parse_csv_filename(filename: str) -> tuple[str, str] | None:
    """
    Parse a CSV filename into (metric_name, date_str).

    Examples:
        "Potenza AC_2026-04-20.csv"   -> ("Potenza AC", "2026-04-20")
        "Corrente_DC_2026-04-02.csv"  -> ("Corrente DC", "2026-04-02")
        "PR_2026-04-02.csv"           -> ("PR inverter", "2026-04-02")
    """
    # Try to match: <prefix>_<YYYY-MM-DD>.csv
    match = re.match(r'^(.+?)_(\d{4}-\d{2}-\d{2})\.csv$', filename)
    if not match:
        return None

    prefix = match.group(1)
    date_str = match.group(2)

    metric_name = PREFIX_MAP.get(prefix)
    if metric_name is None:
        return None

    return metric_name, date_str


def migrate_csv_files() -> dict:
    """Import all CSV files from extracted_data/ into the database."""
    stats = {"total": 0, "success": 0, "skipped": 0, "failed": 0}

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files in {DATA_DIR}")

    for csv_path in csv_files:
        parsed = parse_csv_filename(csv_path.name)
        if parsed is None:
            logger.debug(f"  Skipping non-metric CSV: {csv_path.name}")
            stats["skipped"] += 1
            continue

        metric_name, date_str = parsed
        stats["total"] += 1

        try:
            # Read CSV with auto-detection (same logic as processor_watchdog_final)
            df = pd.read_csv(str(csv_path), sep=None, engine="python", encoding="utf-8")

            if df.empty:
                logger.warning(f"  Empty: {csv_path.name}")
                stats["skipped"] += 1
                continue

            # Deduplicate by Ora if present
            if "Ora" in df.columns:
                df = df.drop_duplicates(subset=["Ora"], keep="last").reset_index(drop=True)

            save_metric(df, metric_name, date_str)
            logger.info(f"  [OK] {csv_path.name} -> {_resolve_table_name(metric_name)} ({len(df)} rows)")
            stats["success"] += 1

        except Exception as e:
            logger.error(f"  [FAIL] {csv_path.name}: {e}")
            stats["failed"] += 1

    return stats


def migrate_json_snapshots() -> dict:
    """Import all dashboard_data_*.json files into the database."""
    stats = {"total": 0, "success": 0, "failed": 0}

    json_files = sorted(DATA_DIR.glob("dashboard_data_*.json"))
    logger.info(f"Found {len(json_files)} dashboard JSON files")

    for json_path in json_files:
        # Parse date from filename: dashboard_data_2026-04-20.json
        match = re.match(r'dashboard_data_(\d{4}-\d{2}-\d{2})\.json$', json_path.name)
        if not match:
            continue

        date_str = match.group(1)
        stats["total"] += 1

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not data:
                continue

            # Each key is a timestamp, each value is a snapshot dict
            for timestamp_str, snapshot_data in data.items():
                save_analysis_snapshot(date_str, timestamp_str, snapshot_data)

            logger.info(f"  [OK] {json_path.name} -> {len(data)} snapshots")
            stats["success"] += 1

        except Exception as e:
            logger.error(f"  [FAIL] {json_path.name}: {e}")
            stats["failed"] += 1

    return stats


def migrate_extraction_status() -> None:
    """Import extraction_status.json into the database."""
    status_path = DATA_DIR / "extraction_status.json"
    if not status_path.exists():
        logger.info("No extraction_status.json found — skipping.")
        return

    try:
        with open(status_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        count = 0
        for date_str, metrics in data.items():
            for metric_type, info in metrics.items():
                status = info.get("status", "success")
                save_extraction_status(date_str, metric_type, status)
                count += 1

        logger.info(f"  [OK] Imported {count} extraction status records")

    except Exception as e:
        logger.error(f"  [FAIL] extraction_status.json: {e}")


def main():
    start = time.time()

    print("\n" + "=" * 60)
    print("  SCADA DATA MIGRATION: CSV/JSON -> SQLite")
    print("=" * 60 + "\n")

    logger.info("Initializing databases...")
    init_databases()

    logger.info("\n--- Phase 1: Migrating CSV metric files ---")
    csv_stats = migrate_csv_files()

    logger.info("\n--- Phase 2: Migrating dashboard JSON snapshots ---")
    json_stats = migrate_json_snapshots()

    logger.info("\n--- Phase 3: Migrating extraction status ---")
    migrate_extraction_status()

    elapsed = time.time() - start

    print("\n" + "=" * 60)
    print("  MIGRATION COMPLETE")
    print("=" * 60)
    print(f"\n  CSV files:  {csv_stats['success']} imported, {csv_stats['failed']} failed, {csv_stats['skipped']} skipped")
    print(f"  JSON files: {json_stats['success']} imported, {json_stats['failed']} failed")
    print(f"  Time:       {elapsed:.1f} seconds")
    print()

    # Show DB stats
    from db.db_manager import get_db_stats
    stats = get_db_stats()
    print("  Database Statistics:")
    for table, count in stats.items():
        if table.endswith("_mb"):
            print(f"    {table}: {count} MB")
        else:
            print(f"    {table}: {count} rows")
    print()


if __name__ == "__main__":
    main()
