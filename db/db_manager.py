"""
db/db_manager.py — SQLite database manager for Mazara SCADA monitoring system.

Two separate databases:
  scada_data.db  — Extracted SCADA measurements
  scada_snapshots.db — Analysis snapshots
  scada_logs.db  — Application logs (extraction, watchdog, dashboard, telegram)

Design:
  - Wide metrics (Potenza AC, Temperatura, Resistenza, Irraggiamento, PR)
    are stored using pandas to_sql/read_sql with original column names.
  - Corrente DC is normalized from ~432 columns to (inverter_id, mppt_number, value)
    rows and pivoted back to wide format on read.
  - Analysis snapshots store the full dashboard JSON blob per timestamp.
  - Logs use a simple indexed table with source/level filtering.
  - All database connections use WAL journal mode for concurrent read/write safety.
"""

import json
import logging
import random
import re
import sqlite3
import time
import traceback as tb_module
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from threading import local as thread_local

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
DB_DIR = ROOT / "db"
DATA_DB_PATH = DB_DIR / "scada_data.db"
LOGS_DB_PATH = DB_DIR / "scada_logs.db"
SNAPSHOT_DB_PATH = DB_DIR / "scada_snapshots.db"

logger = logging.getLogger(__name__)

# SQLite connection tuning
DB_TIMEOUT_SECONDS = 60
DB_BUSY_TIMEOUT_MS = 60000

# Thread-local storage for database connections
_thread_local = thread_local()


# ---------------------------------------------------------------------------
# Connection Management
# ---------------------------------------------------------------------------

def _refresh_bound_method(func, args):
    """Refresh a bound sqlite3.Connection method after the connection was reset."""
    if hasattr(func, "__self__") and hasattr(func, "__name__"):
        self_obj = func.__self__
        if isinstance(self_obj, sqlite3.Connection):
            if self_obj is getattr(_thread_local, "snapshot_conn", None):
                return getattr(_get_snapshot_conn(), func.__name__)
            return getattr(get_data_conn(), func.__name__)
    return func


def _refresh_connection_args(args, kwargs):
    """Replace thread-local sqlite3.Connection objects in args/kwargs with a fresh connection."""
    def resolve_connection(conn):
        if conn is getattr(_thread_local, "snapshot_conn", None):
            return _get_snapshot_conn()
        return get_data_conn()

    new_args = []
    for arg in args:
        if isinstance(arg, sqlite3.Connection):
            new_args.append(resolve_connection(arg))
        else:
            new_args.append(arg)

    new_kwargs = {}
    for key, value in kwargs.items():
        if isinstance(value, sqlite3.Connection):
            new_kwargs[key] = resolve_connection(value)
        else:
            new_kwargs[key] = value
    return tuple(new_args), new_kwargs


def _retry_operation(func, *args, kind: str, retries: int = 6, delay: float = 0.25, **kwargs):
    """Retry a database operation when SQLite reports a locked, closed, or corrupted database."""
    repair_fn = _repair_data_db if kind == "data" else _repair_snapshot_db
    reset_fn = _reset_data_conn if kind == "data" else _reset_snapshot_conn
    label = "data DB" if kind == "data" else "snapshot DB"
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except sqlite3.DatabaseError as exc:
            msg = str(exc).lower()
            if any(keyword in msg for keyword in ("database is locked", "cannot operate on a closed database", "disk image is malformed", "file is not a database", "file is encrypted")):
                if attempt == retries:
                    raise
                if any(keyword in msg for keyword in ("disk image is malformed", "file is not a database", "file is encrypted")):
                    logger.warning(f"[DB] Detected corrupted {label} during operation: {exc}")
                    repair_fn()
                time.sleep(delay * attempt)
                reset_fn()
                func = _refresh_bound_method(func, args)
                args, kwargs = _refresh_connection_args(args, kwargs)
                continue
            raise


def _retry_data_operation(func, *args, retries: int = 6, delay: float = 0.25, **kwargs):
    """Retry a data database operation when SQLite reports a locked, closed, or corrupted database."""
    return _retry_operation(func, *args, kind="data", retries=retries, delay=delay, **kwargs)


def _retry_snapshot_operation(func, *args, retries: int = 6, delay: float = 0.25, **kwargs):
    """Retry a snapshot database operation when SQLite reports a lock, closed, or corrupted database."""
    return _retry_operation(func, *args, kind="snapshot", retries=retries, delay=delay, **kwargs)


def _repair_db(path: Path, reset_fn, label: str) -> None:
    """Rename a corrupted database and prepare a fresh replacement."""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if path.exists():
        backup_path = path.with_name(f"{path.name}.corrupt_{timestamp}.bak")
        try:
            path.rename(backup_path)
            logger.warning(f"[DB] Corrupted {label} renamed to: {backup_path}")
        except Exception as exc:
            logger.error(f"[DB] Failed to backup corrupted {label}: {exc}")
    for suffix in ("-wal", "-shm"):
        extra = path.with_name(f"{path.name}{suffix}")
        if extra.exists():
            backup_extra = extra.with_name(f"{extra.name}.corrupt_{timestamp}.bak")
            try:
                extra.rename(backup_extra)
                logger.warning(f"[DB] Corrupted {label} journal renamed to: {backup_extra}")
            except Exception as exc:
                logger.error(f"[DB] Failed to backup corrupted {label} journal {extra}: {exc}")
    reset_fn()


def _repair_data_db() -> None:
    _repair_db(DATA_DB_PATH, _reset_data_conn, "data DB")


def _repair_snapshot_db() -> None:
    _repair_db(SNAPSHOT_DB_PATH, _reset_snapshot_conn, "snapshot DB")


def get_data_conn() -> sqlite3.Connection:
    """Get or create a thread-local connection to the data database."""
    conn = getattr(_thread_local, "data_conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1").fetchone()
        except sqlite3.DatabaseError as exc:
            msg = str(exc).lower()
            if any(keyword in msg for keyword in ("disk image is malformed", "cannot operate on a closed database", "file is not a database", "file is encrypted")):
                logger.warning(f"[DB] Detected corrupted scada_data.db: {exc}")
                _repair_data_db()
            _reset_data_conn()
            conn = None

    if conn is None:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        try:
            conn = sqlite3.connect(str(DATA_DB_PATH), check_same_thread=False, timeout=DB_TIMEOUT_SECONDS)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
            conn.execute("PRAGMA cache_size=-64000")  # 64 MB cache
            conn.execute("SELECT 1").fetchone()
        except sqlite3.DatabaseError as exc:
            msg = str(exc).lower()
            if any(keyword in msg for keyword in ("disk image is malformed", "file is not a database", "file is encrypted")):
                logger.warning(f"[DB] Failed opening scada_data.db due corruption: {exc}")
                if "conn" in locals() and conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                _repair_data_db()
                return get_data_conn()
            raise
        _thread_local.data_conn = conn
    return conn


def get_logs_conn() -> sqlite3.Connection:
    """Get or create a thread-local connection to the logs database."""
    conn = getattr(_thread_local, "logs_conn", None)
    if conn is None:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(LOGS_DB_PATH), check_same_thread=False, timeout=DB_TIMEOUT_SECONDS)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
        _thread_local.logs_conn = conn
    return conn


def _get_fresh_data_conn() -> sqlite3.Connection:
    """Open a fresh data DB connection for single-shot writes.

    This is used for snapshot writes that may contend with concurrent
    readers/writers in other processes or threads.
    """
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DATA_DB_PATH), check_same_thread=False, timeout=DB_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA cache_size=-64000")  # 64 MB cache
    return conn


def _get_snapshot_conn() -> sqlite3.Connection:
    """Get or create a thread-local connection to the snapshot database."""
    conn = getattr(_thread_local, "snapshot_conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1").fetchone()
        except sqlite3.DatabaseError as exc:
            msg = str(exc).lower()
            if any(keyword in msg for keyword in ("disk image is malformed", "cannot operate on a closed database", "file is not a database", "file is encrypted")):
                logger.warning(f"[DB] Detected corrupted snapshot DB: {exc}")
                _repair_snapshot_db()
            _reset_snapshot_conn()
            conn = None

    if conn is None:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(SNAPSHOT_DB_PATH), check_same_thread=False, timeout=DB_TIMEOUT_SECONDS)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA cache_size=-64000")
        _thread_local.snapshot_conn = conn
    return conn


def _get_fresh_snapshot_conn() -> sqlite3.Connection:
    """Open a fresh connection to the snapshot database for single-shot writes."""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(SNAPSHOT_DB_PATH), check_same_thread=False, timeout=DB_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA cache_size=-64000")
    return conn


def _reset_snapshot_conn() -> None:
    conn = getattr(_thread_local, "snapshot_conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _thread_local.snapshot_conn = None


# ---------------------------------------------------------------------------
# Database Initialization
# ---------------------------------------------------------------------------

def init_databases() -> None:
    """Create all managed databases and required tables/indexes."""
    _init_data_db()
    _init_snapshot_db()
    _init_logs_db()
    logger.info("Databases initialized successfully.")


def _init_snapshot_db() -> None:
    """Create the snapshot database tables."""
    conn = _get_snapshot_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analysis_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            snapshot_json TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_date ON analysis_snapshots(date)")
    conn.commit()


def _apply_data_db_swap() -> None:
    """If a pre-built replacement DB exists, swap it in before opening any connection.

    Run `db/scada_data_new.db` past all active connections by waiting until the
    process starts fresh.  Call this once at the top of `_init_data_db()`.
    """
    new_path = DATA_DB_PATH.with_name("scada_data_new.db")
    if not new_path.exists():
        return
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    old_backup = DATA_DB_PATH.with_name(f"scada_data.db.replaced_{timestamp}.bak")
    try:
        if DATA_DB_PATH.exists():
            DATA_DB_PATH.rename(old_backup)
            logger.warning(f"[DB] Swapped corrupted data DB → {old_backup.name}")
        for suffix in ("-wal", "-shm"):
            stale = DATA_DB_PATH.with_name(f"scada_data.db{suffix}")
            if stale.exists():
                try:
                    stale.unlink()
                except Exception:
                    pass
        new_path.rename(DATA_DB_PATH)
        logger.info("[DB] scada_data_new.db promoted to scada_data.db")
    except Exception as exc:
        logger.error(f"[DB] DB swap failed: {exc}")


def _init_data_db() -> None:
    """Create the data database tables."""
    _apply_data_db_swap()
    conn = get_data_conn()

    # Corrente DC — normalized (one row per inverter/MPPT/timestamp)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS corrente_dc (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            ora TEXT NOT NULL,
            timestamp_fetch TEXT,
            inverter_id TEXT NOT NULL,
            mppt_number INTEGER NOT NULL,
            value REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dc_date ON corrente_dc(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dc_date_inv ON corrente_dc(date, inverter_id)")
    # Covers "latest reading for this inverter/MPPT" lookups (plant map string health)
    # without a temp B-tree sort — table has tens of millions of rows.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dc_date_inv_mppt_ora ON corrente_dc(date, inverter_id, mppt_number, ora)")
    # Covers the analytics chart query's GROUP BY date, ora, inverter_id
    # (avoids a second temp B-tree on top of the one above).
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dc_date_ora_inv ON corrente_dc(date, ora, inverter_id, value)")

    # Tracker Status — real-time snapshot of the tracker field
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tracker_status (
            ncu_id TEXT,
            tcu_id TEXT,
            tracker_no TEXT,
            target_angle REAL,
            actual_angle REAL,
            alarm TEXT,
            mode TEXT,
            last_update TEXT,
            PRIMARY KEY (ncu_id, tcu_id)
        )
    """)

    # Tracker History — historical measurements of the tracker field
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tracker_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            ncu_id TEXT,
            tcu_id TEXT,
            tracker_no TEXT,
            target_angle REAL,
            actual_angle REAL,
            alarm TEXT,
            mode TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_thistory_ts ON tracker_history(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_thistory_ncu_tcu ON tracker_history(ncu_id, tcu_id)")

    # Extraction status — tracks which metrics were successfully extracted
    conn.execute("""
        CREATE TABLE IF NOT EXISTS extraction_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            metric_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'success',
            timestamp TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_estatus_date ON extraction_status(date)")

    conn.commit()

    # Pre-populate history from existing files if table is empty
    try:
        _migrate_tracker_history_from_json()
    except Exception as me:
        logger.error(f"Auto-migration on startup failed: {me}")


def _init_logs_db() -> None:
    """Create the logs database table."""
    conn = get_logs_conn()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            source TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            traceback TEXT,
            metadata TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_source ON logs(source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_src_lvl ON logs(source, level)")

    conn.commit()


# ---------------------------------------------------------------------------
# Metric Name Mapping
# ---------------------------------------------------------------------------

# Map the metric display name (used in VCOM) to the SQLite table name.
# Wide metrics get a simple sanitized table name.
METRIC_TABLE_MAP = {
    "Potenza AC":                "potenza_ac",
    "Temperatura":               "temperatura",
    "Resistenza di isolamento":  "resistenza_isolamento",
    "Irraggiamento":             "irraggiamento",
    "PR inverter":               "pr_readings",
    "Corrente DC":               "corrente_dc",     # normalized — special handling
    "Potenza attiva":            "potenza_attiva",
}

# List of all standard inverter IDs in the plant
INVERTER_IDS = [
    "TX1-01", "TX1-02", "TX1-03", "TX1-04", "TX1-05", "TX1-06",
    "TX1-07", "TX1-08", "TX1-09", "TX1-10", "TX1-11", "TX1-12",
    "TX2-01", "TX2-02", "TX2-03", "TX2-04", "TX2-05", "TX2-06",
    "TX2-07", "TX2-08", "TX2-09", "TX2-10", "TX2-11", "TX2-12",
    "TX3-01", "TX3-02", "TX3-03", "TX3-04", "TX3-05", "TX3-06",
    "TX3-07", "TX3-08", "TX3-09", "TX3-10", "TX3-11", "TX3-12",
]

# Reverse: table -> metric name (for migration)
TABLE_METRIC_MAP = {v: k for k, v in METRIC_TABLE_MAP.items()}


def _resolve_table_name(metric_name: str) -> str:
    """Resolve a metric name (with spaces/underscores) to its table name."""
    # Try exact match first
    if metric_name in METRIC_TABLE_MAP:
        return METRIC_TABLE_MAP[metric_name]

    # Try with underscores -> spaces
    alt = metric_name.replace("_", " ")
    if alt in METRIC_TABLE_MAP:
        return METRIC_TABLE_MAP[alt]

    # Try with spaces -> underscores
    alt2 = metric_name.replace(" ", "_")
    for k, v in METRIC_TABLE_MAP.items():
        if k.replace(" ", "_") == alt2:
            return v

    # Fallback: sanitize name
    return re.sub(r'[^a-z0-9_]', '_', metric_name.lower())


# ---------------------------------------------------------------------------
# Save Metric Data
# ---------------------------------------------------------------------------

def save_metric(df: pd.DataFrame, metric_name: str, date_str: str = None) -> None:
    """
    Save a metric DataFrame to the database.

    For Corrente DC: normalizes the wide DataFrame into (inverter_id, mppt, value) rows.
    For all other metrics: stores in a wide table using pandas to_sql.
    """
    if df is None or df.empty:
        logger.warning(f"[DB] Empty DataFrame for {metric_name} — skipping save.")
        return

    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    table_name = _resolve_table_name(metric_name)

    if table_name == "corrente_dc":
        _save_corrente_dc(df, date_str)
    else:
        _save_wide_metric(df, table_name, date_str)

    # Clear query cache since database has new data
    try:
        _load_metric_cached.cache_clear()
    except Exception:
        pass

    logger.info(f"[DB] Saved {len(df)} rows -> {table_name} (date={date_str})")


def _save_wide_metric(df: pd.DataFrame, table_name: str, date_str: str) -> None:
    """Save a wide-format metric DataFrame to its table."""
    conn = get_data_conn()

    # Add a _date column for partitioning by day
    df_out = df.copy()
    df_out.insert(0, "_date", date_str)

    # Delete existing data for this date (overwrite semantics like the CSV system)
    table_exists = False
    try:
        _retry_data_operation(conn.execute, f'DELETE FROM "{table_name}" WHERE _date = ?', (date_str,))
        table_exists = True
    except sqlite3.OperationalError:
        pass  # Table doesn't exist yet — to_sql will create it

    # Auto-migrate schema: if the source added new columns (e.g. VCOM renamed a field),
    # add them so to_sql(if_exists="append") doesn't fail with "no such column".
    if table_exists:
        try:
            existing_cols = {r[1] for r in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()}
            for col in df_out.columns:
                if col not in existing_cols:
                    col_type = "TEXT" if str(df_out[col].dtype) == "object" else "REAL"
                    conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {col_type}')
                    logger.info(f"[DB] Schema migration: added column '{col}' to {table_name}")
            conn.commit()
        except Exception as e:
            logger.warning(f"[DB] Schema migration check failed for {table_name}: {e}")

    # Write to DB in chunks to avoid long-held write locks
    CHUNK = 200
    for start in range(0, len(df_out), CHUNK):
        chunk_df = df_out.iloc[start:start + CHUNK]
        _retry_data_operation(chunk_df.to_sql, table_name, conn, if_exists="append", index=False)
    _retry_data_operation(conn.commit)

    # Keep WAL size bounded after high-volume writes
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except Exception:
        pass


def _save_corrente_dc(df: pd.DataFrame, date_str: str) -> None:
    """Normalize the wide Corrente DC DataFrame and save to the normalized table."""
    conn = get_data_conn()

    # Identify the Ora and Timestamp Fetch columns
    id_cols = [c for c in df.columns if c in ("Ora", "Timestamp Fetch")]
    value_cols = [c for c in df.columns if c not in id_cols]

    if not value_cols:
        logger.warning("[DB] Corrente DC has no data columns — skipping.")
        return

    # Melt the wide DataFrame into long format (vectorized — fast)
    melted = df.melt(id_vars=id_cols, value_vars=value_cols,
                     var_name="_col_name", value_name="value")

    # Parse inverter_id and mppt_number from column names
    # Pattern: "Corrente DC MPPT 1 (INV TX1-01) [A]"
    pattern = r'Corrente DC MPPT (\d+) \(INV (TX\d+-\d+)\)'
    parsed = melted["_col_name"].str.extract(pattern)
    melted["mppt_number"] = pd.to_numeric(parsed[0], errors="coerce")
    melted["inverter_id"] = parsed[1]

    # Drop rows that didn't match the pattern or have no value
    melted = melted.dropna(subset=["inverter_id"])

    # Convert value to numeric
    melted["value"] = pd.to_numeric(melted["value"], errors="coerce")

    # Build the final normalized table
    result = pd.DataFrame({
        "date": date_str,
        "ora": melted["Ora"].astype(str) if "Ora" in melted.columns else "",
        "timestamp_fetch": melted["Timestamp Fetch"].astype(str) if "Timestamp Fetch" in melted.columns else "",
        "inverter_id": melted["inverter_id"],
        "mppt_number": melted["mppt_number"].astype(int),
        "value": melted["value"],
    })

    # Delete existing data for this date
    _retry_data_operation(conn.execute, "DELETE FROM corrente_dc WHERE date = ?", (date_str,))

    # Bulk insert in chunks to avoid long-held write locks
    CHUNK = 2000
    for start in range(0, len(result), CHUNK):
        chunk_df = result.iloc[start:start + CHUNK]
        _retry_data_operation(chunk_df.to_sql, "corrente_dc", conn, if_exists="append", index=False)
    _retry_data_operation(conn.commit)

    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except Exception:
        pass

    logger.info(f"[DB] Normalized {len(df)} wide rows -> {len(result)} DC readings")


# ---------------------------------------------------------------------------
# Load Metric Data
# ---------------------------------------------------------------------------

@lru_cache(maxsize=128)
def _load_metric_cached(date_str: str, metric_name: str) -> pd.DataFrame | None:
    table_name = _resolve_table_name(metric_name)

    if table_name == "corrente_dc":
        return _load_corrente_dc(date_str)
    else:
        return _load_wide_metric(table_name, date_str)


def load_metric(date_str: str, metric_name: str) -> pd.DataFrame | None:
    """
    Load a metric from the database as a DataFrame.

    Returns the same wide-format DataFrame that the processor/watchdog expects,
    preserving full backward compatibility with the CSV-based system.

    Returns None if the metric is not found or the table doesn't exist.
    """
    df = _load_metric_cached(date_str, metric_name)
    if df is not None:
        return df.copy()
    return None


def _load_wide_metric(table_name: str, date_str: str) -> pd.DataFrame | None:
    """Load a wide-format metric from its table."""
    conn = get_data_conn()

    try:
        df = pd.read_sql_query(
            f'SELECT * FROM "{table_name}" WHERE _date = ?',
            conn,
            params=(date_str,)
        )
    except Exception:
        return None

    if df.empty:
        return None

    # Remove the internal _date column to match the original CSV format
    df = df.drop(columns=["_date"], errors="ignore")

    # Deduplicate by Ora if present (same logic as old load_metric).
    # Skip deduplication for pr_readings because PR data lists inverters line-by-line
    # rather than minute time-slots.
    if "Ora" in df.columns and table_name != "pr_readings":
        df = df.drop_duplicates(subset=["Ora"], keep="last").reset_index(drop=True)

    logger.debug(f"[DB] Loaded {table_name} for {date_str} ({len(df)} rows)")
    return df


def _load_corrente_dc(date_str: str) -> pd.DataFrame | None:
    """Load normalized DC data and pivot it back to wide format."""
    conn = get_data_conn()

    try:
        df = pd.read_sql_query(
            "SELECT ora, timestamp_fetch, inverter_id, mppt_number, value "
            "FROM corrente_dc WHERE date = ?",
            conn,
            params=(date_str,)
        )
    except Exception:
        return None

    if df.empty:
        return None

    # Reconstruct the original wide column name
    df["col_name"] = df.apply(
        lambda r: f"Corrente DC MPPT {int(r['mppt_number'])} (INV {r['inverter_id']}) [A]",
        axis=1
    )

    # Pivot back to wide format
    wide = df.pivot_table(
        index="ora",
        columns="col_name",
        values="value",
        aggfunc="last"
    )
    wide = wide.reset_index()
    wide.columns.name = None

    # Rename 'ora' to 'Ora' for backward compatibility
    wide = wide.rename(columns={"ora": "Ora"})

    # Add Timestamp Fetch if present in the data
    if "timestamp_fetch" in df.columns:
        ts_map = df.drop_duplicates(subset=["ora"], keep="last").set_index("ora")["timestamp_fetch"]
        wide["Timestamp Fetch"] = wide["Ora"].map(ts_map)
        # Move Timestamp Fetch to position 0
        cols = ["Timestamp Fetch", "Ora"] + [c for c in wide.columns if c not in ("Timestamp Fetch", "Ora")]
        wide = wide[cols]

    # Deduplicate by Ora
    if "Ora" in wide.columns:
        wide = wide.drop_duplicates(subset=["Ora"], keep="last").reset_index(drop=True)

    logger.debug(f"[DB] Loaded corrente_dc for {date_str} -> {wide.shape[0]} rows × {wide.shape[1]} cols")
    return wide


# ---------------------------------------------------------------------------
# Analysis Snapshots (replaces dashboard_data_{date}.json)
# ---------------------------------------------------------------------------

class NumpyEncoder(json.JSONEncoder):
    """JSON encoder that handles numpy types and Pandas objects."""
    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


def save_analysis_snapshot(date_str: str, timestamp_str: str, snapshot_data: dict) -> None:
    """Save an analysis snapshot to the snapshot database."""
    snapshot_json = json.dumps(snapshot_data, cls=NumpyEncoder)

    for attempt in range(1, 11):
        try:
            with _get_fresh_snapshot_conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO analysis_snapshots (date, timestamp, snapshot_json) VALUES (?, ?, ?)",
                    (date_str, timestamp_str, snapshot_json)
                )
                conn.execute(
                    """
                    DELETE FROM analysis_snapshots
                    WHERE date = ? AND id NOT IN (
                        SELECT id FROM analysis_snapshots
                        WHERE date = ?
                        ORDER BY timestamp DESC
                        LIMIT 50
                    )
                """,
                    (date_str, date_str)
                )
                conn.commit()
                try:
                    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except Exception:
                    pass
            return
        except sqlite3.DatabaseError as exc:
            msg = str(exc).lower()
            if "database is locked" in msg and attempt < 10:
                time.sleep(1.5 * attempt + random.random())
                continue
            if any(keyword in msg for keyword in ("disk image is malformed", "file is not a database", "file is encrypted")):
                logger.warning(f"[DB] Detected corrupted snapshot DB during save: {exc}")
                _repair_snapshot_db()
                if attempt < 10:
                    time.sleep(1.5 * attempt + random.random())
                    continue
            raise


def _reset_data_conn() -> None:
    """Discard the cached thread-local data connection so the next call reopens it."""
    conn = getattr(_thread_local, "data_conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _thread_local.data_conn = None


def _load_snapshot_from_json(date_str: str) -> dict | None:
    """Fallback: read the latest snapshot from the watchdog's JSON file."""
    json_path = ROOT / "extracted_data" / f"dashboard_data_{date_str}.json"
    if not json_path.exists():
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            return None
        latest_key = sorted(data.keys())[-1]
        return data[latest_key]
    except Exception:
        return None


def save_snapshot_fallback(date_str: str, timestamp_str: str, snapshot_data: dict) -> str:
    """Write a snapshot to the watchdog JSON fallback file when DB persistence fails."""
    fallback_dir = ROOT / "extracted_data"
    fallback_dir.mkdir(parents=True, exist_ok=True)
    json_path = fallback_dir / f"dashboard_data_{date_str}.json"
    existing_data = {}
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        except Exception:
            existing_data = {}

    existing_data[timestamp_str] = snapshot_data
    timestamps_sorted = sorted(existing_data.keys())
    if len(timestamps_sorted) > 50:
        for old_ts in timestamps_sorted[:-50]:
            existing_data.pop(old_ts, None)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=2, cls=NumpyEncoder)

    return str(json_path)


def load_latest_snapshot(date_str: str) -> dict | None:
    """Load the most recent analysis snapshot for a given date.

    Falls back to the watchdog's JSON file when the database is unavailable or
    corrupted (e.g. 'disk image is malformed', 'database is locked').
    """
    try:
        conn = _get_snapshot_conn()
        row = conn.execute(
            "SELECT snapshot_json FROM analysis_snapshots "
            "WHERE date = ? ORDER BY timestamp DESC LIMIT 1",
            (date_str,)
        ).fetchone()
        if row is not None:
            return json.loads(row[0])
    except sqlite3.DatabaseError as exc:
        logger.warning(f"[DB] snapshot DB error in load_latest_snapshot ({exc}) — resetting connection, falling back to JSON")
        _reset_snapshot_conn()
    except Exception as exc:
        logger.warning(f"[DB] unexpected error in load_latest_snapshot: {exc}")

    return _load_snapshot_from_json(date_str)


def get_latest_snapshot_date() -> str | None:
    """Return the most recent snapshot date available in the snapshot database."""
    conn = _get_snapshot_conn()
    row = conn.execute(
        "SELECT date FROM analysis_snapshots ORDER BY date DESC, timestamp DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def load_all_snapshots(date_str: str) -> dict:
    """Load all analysis snapshots for a given date as {timestamp: data}."""
    conn = _get_snapshot_conn()

    try:
        rows = conn.execute(
            "SELECT timestamp, snapshot_json FROM analysis_snapshots "
            "WHERE date = ? ORDER BY timestamp ASC",
            (date_str,)
        ).fetchall()
    except Exception:
        return {}

    result = {}
    for ts, snap_json in rows:
        result[ts] = json.loads(snap_json)
    return result


def delete_snapshots(date_str: str) -> None:
    """Delete all analysis snapshots for a given date (used by rescan)."""
    conn = _get_snapshot_conn()
    _retry_snapshot_operation(conn.execute, "DELETE FROM analysis_snapshots WHERE date = ?", (date_str,))
    _retry_snapshot_operation(conn.commit)


# ---------------------------------------------------------------------------
# Extraction Status (replaces extraction_status.json)
# ---------------------------------------------------------------------------

def save_extraction_status(date_str: str, metric_type: str, status: str = "success") -> None:
    """Record that a metric was extracted successfully."""
    conn = get_data_conn()

    timestamp = datetime.now().isoformat(timespec="seconds")

    # Upsert: delete old status for this metric+date, then insert new
    _retry_data_operation(
        conn.execute,
        "DELETE FROM extraction_status WHERE date = ? AND metric_type = ?",
        (date_str, metric_type)
    )
    _retry_data_operation(
        conn.execute,
        "INSERT INTO extraction_status (date, metric_type, status, timestamp) VALUES (?, ?, ?, ?)",
        (date_str, metric_type, status, timestamp)
    )
    _retry_data_operation(conn.commit)


def get_extraction_status(date_str: str) -> dict:
    """Get extraction status for all metrics on a given date.

    Returns dict like: {"PR": {"status": "success", "timestamp": "..."}, ...}
    """
    conn = get_data_conn()

    try:
        rows = conn.execute(
            "SELECT metric_type, status, timestamp FROM extraction_status WHERE date = ?",
            (date_str,)
        ).fetchall()
    except Exception:
        return {}

    return {
        row[0]: {"status": row[1], "timestamp": row[2]}
        for row in rows
    }


# ---------------------------------------------------------------------------
# SQLite Log Handler
# ---------------------------------------------------------------------------

class SQLiteLogHandler(logging.Handler):
    """
    Custom logging handler that writes log records to the scada_logs.db database.

    Usage:
        handler = SQLiteLogHandler(source_name="extraction")
        logger.addHandler(handler)

    Each log record is stored with its timestamp, source, level, message,
    and optional traceback for error-level records.
    """

    def __init__(self, source_name: str, level=logging.DEBUG):
        super().__init__(level)
        self.source_name = source_name
        self._conn = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = self._open_conn()
        return self._conn

    def _open_conn(self) -> sqlite3.Connection:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(LOGS_DB_PATH), check_same_thread=False, timeout=DB_TIMEOUT_SECONDS
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                traceback TEXT,
                metadata TEXT
            )
        """)
        conn.commit()
        return conn

    def _reset_db(self) -> None:
        """Close and delete the corrupted database file, then reconnect fresh."""
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None
        try:
            if LOGS_DB_PATH.exists():
                LOGS_DB_PATH.unlink()
        except Exception:
            pass
        try:
            self._conn = self._open_conn()
        except Exception:
            pass

    def emit(self, record: logging.LogRecord) -> None:
        try:
            conn = self._get_conn()

            traceback_str = None
            if record.exc_info and record.exc_info[0] is not None:
                traceback_str = "".join(tb_module.format_exception(*record.exc_info))

            conn.execute(
                "INSERT INTO logs (timestamp, source, level, message, traceback) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    datetime.fromtimestamp(record.created).isoformat(timespec="milliseconds"),
                    self.source_name,
                    record.levelname,
                    self.format(record),
                    traceback_str,
                )
            )
            conn.commit()
        except sqlite3.DatabaseError:
            # Database is corrupted — delete and recreate it silently
            self._reset_db()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        """Close the database connection when the handler is shut down."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        super().close()


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

def get_available_dates() -> list[str]:
    """Return a sorted list of all dates that have data in the DB."""
    conn = get_data_conn()

    dates = set()
    try:
        # Check all wide metric tables
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'"
        ).fetchall()
        for (table_name,) in tables:
            if table_name in ("corrente_dc", "analysis_snapshots", "extraction_status"):
                rows = conn.execute(f"SELECT DISTINCT date FROM \"{table_name}\"").fetchall()
            else:
                try:
                    rows = conn.execute(f'SELECT DISTINCT _date FROM "{table_name}"').fetchall()
                except Exception:
                    continue
            dates.update(r[0] for r in rows)
    except Exception:
        pass

    return sorted(dates)


def get_db_stats() -> dict:
    """Return basic statistics about the databases."""
    stats = {}
    conn = get_data_conn()      

    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'"
        ).fetchall()
        for (table_name,) in tables:
            count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            stats[table_name] = count
    except Exception:
        pass

    # Logs DB
    try:
        logs_conn =     get_logs_conn()
        count = logs_conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
        stats["logs"] = count
    except Exception:
        stats["logs"] = 0

    # File sizes
    if DATA_DB_PATH.exists():
        stats["data_db_size_mb"] = round(DATA_DB_PATH.stat().st_size / (1024 * 1024), 2)
    if LOGS_DB_PATH.exists():
        stats["logs_db_size_mb"] = round(LOGS_DB_PATH.stat().st_size / (1024 * 1024), 2)

    return stats


def get_available_inverters() -> list[str]:
    """Return the list of all standard inverter IDs."""
    return INVERTER_IDS


def get_metric_history(metric_name: str, date_start: str, date_end: str, inverter_ids: list[str] = None) -> dict:
    """
    Fetch historical data for a metric across a date range.
    Optimized for charting: returns a dict with timestamps and per-inverter data series.
    """
    table_name = _resolve_table_name(metric_name)
    conn = get_data_conn()
    
    # We'll return a structure optimized for ApexCharts:
    # {
    #   "timestamps": ["2026-04-20T08:00:00", ...],
    #   "series": [
    #      {"name": "INV TX1-01", "data": [10.5, 11.2, ...]},
    #      ...
    #   ]
    # }
    
    try:
        if table_name == "corrente_dc":
            # Normalized query
            query = """
                SELECT date, ora, inverter_id, AVG(value) as val
                FROM corrente_dc
                WHERE date >= ? AND date <= ?
            """
            params = [date_start, date_end]
            if inverter_ids:
                placeholders = ",".join(["?"] * len(inverter_ids))
                query += f" AND inverter_id IN ({placeholders})"
                params.extend(inverter_ids)
            
            query += " GROUP BY date, ora, inverter_id ORDER BY date ASC, CAST(ora AS REAL) ASC"
            df = pd.read_sql_query(query, conn, params=params)
            
            if df.empty: return {"timestamps": [], "series": []}
            
            # Format Ora (HH.mm -> HH:mm)
            def format_ora(o):
                try:
                    o_str = str(o)
                    if ":" in o_str: return o_str
                    if "." in o_str:
                        h, m = o_str.split(".")
                        m = m.ljust(2, "0")[:2]
                        return f"{int(h):02d}:{int(m):02d}:00"
                    return f"{int(o):02d}:00:00"
                except: return str(o)
            
            df["ora"] = df["ora"].apply(format_ora)
            
            # Pivot to wide format
            df["ts"] = df["date"] + "T" + df["ora"]
            pivoted = df.pivot(index="ts", columns="inverter_id", values="val")
            
        else:
            # Wide table query
            cols_info = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
            col_names = [c[1] for c in cols_info]
            has_ora = "Ora" in col_names
            
            order_clause = "ORDER BY _date ASC, Ora ASC" if has_ora else "ORDER BY _date ASC"
            query = f'SELECT * FROM "{table_name}" WHERE _date >= ? AND _date <= ? {order_clause}'
            df = pd.read_sql_query(query, conn, params=(date_start, date_end))
            
            if df.empty: return {"timestamps": [], "series": []}
            
            # Filter columns by inverter_ids if provided
            cols_to_keep = []
            if inverter_ids and any(i.strip() for i in inverter_ids if i):
                for inv in inverter_ids:
                    if not inv or not inv.strip(): continue
                    # Match column like "Potenza AC (INV TX1-01) [W]"
                    match = [c for c in df.columns if f"({inv})" in c or f"INV {inv}" in c]
                    cols_to_keep.extend(match)
                
                if not cols_to_keep:
                    cols_to_keep = [c for c in df.columns if c not in ["_date", "Ora", "Timestamp Fetch"]]
            else:
                cols_to_keep = [c for c in df.columns if c not in ["_date", "Ora", "Timestamp Fetch"]]
            
            # Construct timestamps
            if has_ora:
                def format_ora(o):
                    try:
                        o_str = str(o)
                        if ":" in o_str: return o_str
                        if "." in o_str:
                            h, m = o_str.split(".")
                            m = m.ljust(2, "0")[:2]
                            return f"{int(h):02d}:{int(m):02d}:00"
                        return f"{int(o):02d}:00:00"
                    except: return str(o)
                df["Ora"] = df["Ora"].apply(format_ora)
                df["ts"] = df["_date"] + "T" + df["Ora"].astype(str)
            else:
                df["ts"] = df["_date"]
            
            df = df.set_index("ts")
            
            pivoted = df[cols_to_keep]

        # Final formatting
        pivoted = pivoted.replace([np.inf, -np.inf], np.nan).fillna(0)
        
        # Limit points to ~1000 for performance if range is long
        if len(pivoted) > 1500:
            step = len(pivoted) // 1000
            pivoted = pivoted.iloc[::step]

        result = {
            "timestamps": pivoted.index.tolist(),
            "series": []
        }
        
        for col in pivoted.columns:
            result["series"].append({
                "name": col,
                "data": pivoted[col].tolist()
            })
            
        return result

    except Exception as e:
        logger.error(f"Error fetching metric history: {e}")
        return {"timestamps": [], "series": [], "error": str(e)}


def get_daily_sensor_history(date_str: str) -> dict:
    """
    Fetch all historical data for environmental sensors (irraggiamento table)
    for a specific day, returning it in a format suitable for sparkline lookups.
    """
    conn = get_data_conn()
    try:
        # Irraggiamento table contains irradiance + JB temperatures
        df = pd.read_sql_query(
            'SELECT * FROM irraggiamento WHERE _date = ? ORDER BY CAST(Ora AS REAL) ASC',
            conn,
            params=(date_str,)
        )
        if df.empty:
            return {}

        # Deduplicate by Ora to ensure clean series
        df = df.drop_duplicates(subset=["Ora"], keep="last").reset_index(drop=True)

        # Handle numeric conversion for all sensor columns
        for col in df.columns:
            if col not in ["_date", "Ora", "Timestamp Fetch"]:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Convert to dict of lists: { "Ora": [8.0, 8.1, ...], "Sensor A": [100, 105, ...], ... }
        result = df.to_dict(orient="list")
        
        # Cleanup internal columns from the payload
        if "_date" in result: del result["_date"]
        
        return result
    except Exception as e:
        logger.error(f"Error fetching daily sensor history: {e}")
        return {}
def resolve_tracker_id(ncu, tcu) -> int:
    """Map NCU/TCU local numbers to a global 1-370 tracker index."""
    try:
        # Extract numeric part of NCU (e.g. "NCU 01" or "NCU_01" -> 1)
        if isinstance(ncu, str):
            match = re.search(r'(\d+)', ncu)
            ncu_num = int(match.group(1)) if match else 0
        else:
            ncu_num = int(ncu)
            
        # Extract numeric part of TCU (e.g. "TCU 01" -> 1)
        if isinstance(tcu, str):
            match = re.search(r'(\d+)', tcu)
            tcu_num = int(match.group(1)) if match else 0
        else:
            tcu_num = int(tcu)
        
        if ncu_num == 0 or tcu_num == 0:
            return 0

        # Logic:
        # NCU 1: 1-121
        # NCU 2: 122-243 (121 + 122)
        # NCU 3: 244-370 (243 + 127)
        if ncu_num == 1:
            return tcu_num
        elif ncu_num == 2:
            return 121 + tcu_num
        elif ncu_num == 3:
            return 121 + 122 + tcu_num
        return 0 # Unknown mapping
    except Exception:
        return 0

def save_tracker_data(records: list) -> None:
    """Batch upsert tracker records into the database and prune stale records older than 24/36 hours."""
    conn = get_data_conn()
    # Clean timestamp without microseconds for cleaner chart displays
    timestamp = datetime.now().replace(microsecond=0).isoformat()
    from datetime import timedelta
    limit_time_status = (datetime.now() - timedelta(hours=24)).isoformat()
    limit_time_history = (datetime.now() - timedelta(hours=36)).isoformat()
    
    try:
        with conn:
            # Delete stale tracker status records older than 24 hours
            conn.execute("DELETE FROM tracker_status WHERE last_update < ?", (limit_time_status,))
            
            # Delete stale tracker history older than 36 hours
            conn.execute("DELETE FROM tracker_history WHERE timestamp < ?", (limit_time_history,))
            
            # Prepare processed records list
            processed_records = []
            for r in records:
                ncu = r.get("ncu")
                tcu = r.get("tcu")
                
                # Robust parsing of TCU number bounds to filter out VCOM navigation/OCR errors
                try:
                    tcu_num = int(re.search(r'(\d+)', str(tcu)).group(1)) if tcu else 0
                except Exception:
                    tcu_num = 0
                
                ncu_clean = str(ncu or "").upper().replace("_", "").replace(" ", "")
                if ncu_clean == "NCU01" and tcu_num > 121:
                    logger.warning(f"Skipping invalid tracker record: {ncu} {tcu} (NCU01 limit is 121)")
                    continue
                if ncu_clean == "NCU02" and tcu_num > 122:
                    logger.warning(f"Skipping invalid tracker record: {ncu} {tcu} (NCU02 limit is 122)")
                    continue
                if ncu_clean == "NCU03" and tcu_num > 127:
                    logger.warning(f"Skipping invalid tracker record: {ncu} {tcu} (NCU03 limit is 127)")
                    continue
                
                tno = str(re.search(r'(\d+)', str(r.get("tracker_no"))).group(1)) if r.get("tracker_no") and re.search(r'(\d+)', str(r.get("tracker_no"))) else str(resolve_tracker_id(ncu, tcu))
                processed_records.append((
                    ncu,
                    tcu,
                    tno,
                    r.get("target_angle"),
                    r.get("actual_angle"),
                    r.get("alarm"),
                    r.get("mode")
                ))

            # Upsert into tracker_status
            conn.executemany("""
                INSERT INTO tracker_status (
                    ncu_id, tcu_id, tracker_no, target_angle, actual_angle, alarm, mode, last_update
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ncu_id, tcu_id) DO UPDATE SET
                    tracker_no = excluded.tracker_no,
                    target_angle = excluded.target_angle,
                    actual_angle = excluded.actual_angle,
                    alarm = excluded.alarm,
                    mode = excluded.mode,
                    last_update = excluded.last_update
            """, [rec + (timestamp,) for rec in processed_records])

            # Insert into tracker_history
            conn.executemany("""
                INSERT INTO tracker_history (
                    timestamp, ncu_id, tcu_id, tracker_no, target_angle, actual_angle, alarm, mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [(timestamp,) + rec for rec in processed_records])
            
    except Exception as e:
        logger.error(f"Failed to save tracker data: {e}")

def get_tracker_history_compact() -> dict:
    """
    Fetch all tracker history for the last 36 hours in a highly compact format for charting.
    """
    from datetime import datetime, timedelta
    conn = get_data_conn()
    limit_time = (datetime.now() - timedelta(hours=36)).isoformat()
    try:
        # Get all records from tracker_history order by timestamp
        cursor = conn.execute("""
            SELECT timestamp, ncu_id, tcu_id, tracker_no, target_angle, actual_angle, alarm, mode
            FROM tracker_history
            WHERE timestamp >= ?
            ORDER BY timestamp ASC
        """, (limit_time,))
        
        rows = cursor.fetchall()
        
        # Organize data
        # Unique timestamps in ascending order
        timestamps_set = sorted(list(set(row[0] for row in rows)))
        ts_index = {ts: idx for idx, ts in enumerate(timestamps_set)}
        
        trackers_data = {}
        
        for timestamp, ncu_id, tcu_id, tracker_no, target_angle, actual_angle, alarm, mode in rows:
            tid = None
            if tracker_no and str(tracker_no).isdigit():
                tid = int(tracker_no)
            else:
                tid = resolve_tracker_id(ncu_id, tcu_id)
                
            if not tid or tid == 0:
                continue
                
            tid_str = str(tid)
            if tid_str not in trackers_data:
                trackers_data[tid_str] = {
                    "actual": [None] * len(timestamps_set),
                    "target": [None] * len(timestamps_set),
                    "mode": [None] * len(timestamps_set),
                    "ncu": ncu_id or "Unknown"
                }
                
            idx = ts_index[timestamp]
            trackers_data[tid_str]["actual"][idx] = actual_angle
            trackers_data[tid_str]["target"][idx] = target_angle
            trackers_data[tid_str]["mode"][idx] = mode
            
        return {
            "timestamps": timestamps_set,
            "trackers": trackers_data
        }
    except Exception as e:
        logger.error(f"Failed to get tracker history: {e}")
        return {"timestamps": [], "trackers": {}}

def _migrate_tracker_history_from_json() -> None:
    """Populate tracker_history table from existing sync JSON files if it's empty."""
    conn = get_data_conn()
    try:
        # Check if table has rows
        cursor = conn.execute("SELECT COUNT(*) FROM tracker_history")
        count = cursor.fetchone()[0]
        if count > 0:
            return
            
        # Find sync files in tracker_testing directory
        import re
        tracker_testing_dir = ROOT / "tracker_testing"
        if not tracker_testing_dir.exists():
            return
            
        sync_files = sorted(tracker_testing_dir.glob("sync_VPN_GATEWAY_SITE_01_*.json"))
        if not sync_files:
            return
            
        logger.info(f"Pre-populating tracker_history from {len(sync_files)} sync files...")
        
        all_records = []
        for file_path in sync_files:
            # Extract timestamp from filename: sync_VPN_GATEWAY_SITE_01_YYYYMMDD_HHMMSS.json
            match = re.search(r'(\d{8})_(\d{6})', file_path.name)
            if not match:
                continue
                
            date_part, time_part = match.groups()
            # Construct ISO timestamp: YYYY-MM-DDTHH:MM:SS
            ts = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]}T{time_part[:2]}:{time_part[2:4]}:{time_part[4:]}"
            
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                
                if not isinstance(data, list):
                    continue
                    
                for r in data:
                    tracker_no_val = r.get("tracker_no")
                    if tracker_no_val and re.search(r'(\d+)', str(tracker_no_val)):
                        tracker_no_clean = str(re.search(r'(\d+)', str(tracker_no_val)).group(1))
                    else:
                        tracker_no_clean = str(resolve_tracker_id(r.get("ncu"), r.get("tcu")))
                        
                    all_records.append((
                        ts,
                        r.get("ncu"),
                        r.get("tcu"),
                        tracker_no_clean,
                        r.get("target_angle"),
                        r.get("actual_angle"),
                        r.get("alarm"),
                        r.get("mode")
                    ))
            except Exception as fe:
                logger.error(f"Error parsing file {file_path.name}: {fe}")
                
        if all_records:
            with conn:
                conn.executemany("""
                    INSERT INTO tracker_history (
                        timestamp, ncu_id, tcu_id, tracker_no, target_angle, actual_angle, alarm, mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, all_records)
            logger.info(f"Successfully migrated {len(all_records)} historical tracker records to DB.")
    except Exception as e:
        logger.error(f"Failed to migrate tracker history from JSON: {e}")

def get_all_tracker_status() -> list:
    """Retrieve current status for all trackers, filtering out stale records older than 24 hours."""
    from datetime import timedelta
    conn = get_data_conn()
    try:
        limit_time = (datetime.now() - timedelta(hours=24)).isoformat()
        cursor = conn.execute(
            "SELECT * FROM tracker_status WHERE last_update >= ? ORDER BY ncu_id, tcu_id",
            (limit_time,)
        )
        cols = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(cols, row)))
        return results
    except Exception as e:
        logger.error(f"Failed to load tracker status: {e}")
        return []

def get_tracker_summary() -> dict:
    """Get high-level summary of tracker field (Avg angles, alarms), filtering out stale records."""
    from datetime import timedelta
    conn = get_data_conn()
    try:
        limit_time = (datetime.now() - timedelta(hours=24)).isoformat()
        # 1. NCU-wise Averages
        cursor = conn.execute("""
            SELECT ncu_id, 
                   AVG(actual_angle) as avg_angle,
                   COUNT(*) as total,
                   SUM(CASE WHEN alarm != 'Normal' AND alarm != '' THEN 1 ELSE 0 END) as alarms
            FROM tracker_status 
            WHERE last_update >= ?
            GROUP BY ncu_id
        """, (limit_time,))
        summary = {}
        for row in cursor.fetchall():
            ncu_val = row[0]
            # Handle both integer and string (e.g. 'NCU_01') ncu_id
            if isinstance(ncu_val, str):
                import re
                match = re.search(r'(\d+)', ncu_val)
                ncu_label = f"NCU {int(match.group(1)):02d}" if match else ncu_val
            else:
                ncu_label = f"NCU {int(ncu_val):02d}"
                
            summary[ncu_label] = {
                "avg_angle": round(row[1], 2) if row[1] is not None else 0,
                "total_trackers": row[2],
                "active_alarms": row[3]
            }
        
        # 2. Modes distribution
        cursor = conn.execute(
            "SELECT mode, COUNT(*) FROM tracker_status WHERE last_update >= ? GROUP BY mode",
            (limit_time,)
        )
        modes = {row[0]: row[1] for row in cursor.fetchall()}
        
        return {
            "ncu_stats": summary,
            "modes": modes,
            "total_plant_trackers": sum(s["total_trackers"] for s in summary.values())
        }
    except Exception as e:
        logger.error(f"Failed to get tracker summary: {e}")
        return {}


def get_heatmap_matrix(date_str: str, metric: str = "ac") -> dict:
    """
    Extract real database metrics for all 36 inverters for the specified date and metric,
    mapped into a 96-slot (15-minute granularity) matrix across 24 hours.
    Missing/future data slots return None (rendered as grey missing tile).
    """
    import pandas as pd
    metric_map = {
        "ac": "Potenza AC",
        "pr": "PR inverter",
        "temp": "Temperatura",
        "dc": "Corrente DC"
    }
    raw_metric = metric_map.get(metric, "Potenza AC")
    df = load_metric(date_str, raw_metric)
    dates = get_available_dates()

    slots = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 15, 30, 45)]
    matrix = {inv_id: [None] * 96 for inv_id in INVERTER_IDS}

    if df is None or df.empty:
        return {
            "date": date_str,
            "metric": metric,
            "slots": slots,
            "inverters": INVERTER_IDS,
            "matrix": matrix,
            "available_dates": dates
        }

    # Match inverter columns for each INVERTER_ID
    inv_col_map = {}
    for inv_id in INVERTER_IDS:
        matches = [c for c in df.columns if f"TX{inv_id}" in c or f"INV {inv_id}" in c or f"({inv_id})" in c or f" {inv_id} " in c or c == inv_id or c == f"INV {inv_id}"]
        if not matches:
            matches = [c for c in df.columns if inv_id in c]
        if matches:
            inv_col_map[inv_id] = matches[0]

    # Handle vertical PR table vs wide time-series tables
    if raw_metric == "PR inverter" and "PR inverter [%]" in df.columns:
        inv_col = "PR inverter" if "PR inverter" in df.columns else "Inverter"
        pr_val_col = "PR inverter [%]"
        for _, row in df.iterrows():
            inv_name = str(row.get(inv_col, ""))
            pr_val = row.get(pr_val_col)
            if pd.notna(pr_val):
                for inv_id in INVERTER_IDS:
                    if inv_id in inv_name:
                        try:
                            val = float(pr_val)
                            for s_idx in range(24, 80):
                                matrix[inv_id][s_idx] = round(val, 1)
                        except Exception:
                            pass
        return {
            "date": date_str,
            "metric": metric,
            "slots": slots,
            "inverters": INVERTER_IDS,
            "matrix": matrix,
            "available_dates": dates
        }

    # Map time-series rows by Ora column
    for _, row in df.iterrows():
        ora_val = row.get("Ora")
        if pd.isna(ora_val):
            continue

        try:
            if isinstance(ora_val, (int, float)):
                h = int(ora_val)
                m = int(round((ora_val - h) * 100))
            else:
                parts = str(ora_val).split(":")
                h, m = int(parts[0]), int(parts[1])
            slot_idx = min(95, max(0, h * 4 + (m // 15)))
        except Exception:
            continue

        for inv_id, col in inv_col_map.items():
            val = row.get(col)
            if pd.notna(val):
                try:
                    num = float(val)
                    if metric == "ac":
                        num = round(num / 1000.0, 1)  # Watts to kW
                    else:
                        num = round(num, 1)
                    matrix[inv_id][slot_idx] = num
                except Exception:
                    pass

    return {
        "date": date_str,
        "metric": metric,
        "slots": slots,
        "inverters": INVERTER_IDS,
        "matrix": matrix,
        "available_dates": dates
    }
