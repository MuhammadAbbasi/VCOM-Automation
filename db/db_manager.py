"""
db/db_manager.py — SQLite database manager for Mazara SCADA monitoring system.

Two separate databases:
  scada_data.db  — Extracted SCADA measurements + analysis snapshots
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
import re
import sqlite3
import traceback as tb_module
from datetime import datetime
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

logger = logging.getLogger(__name__)

# Thread-local storage for database connections
_thread_local = thread_local()


# ---------------------------------------------------------------------------
# Connection Management
# ---------------------------------------------------------------------------

def get_data_conn() -> sqlite3.Connection:
    """Get or create a thread-local connection to the data database."""
    conn = getattr(_thread_local, "data_conn", None)
    if conn is None:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DATA_DB_PATH), check_same_thread=False, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")  # 64 MB cache
        _thread_local.data_conn = conn
    return conn


def get_logs_conn() -> sqlite3.Connection:
    """Get or create a thread-local connection to the logs database."""
    conn = getattr(_thread_local, "logs_conn", None)
    if conn is None:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(LOGS_DB_PATH), check_same_thread=False, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _thread_local.logs_conn = conn
    return conn


# ---------------------------------------------------------------------------
# Database Initialization
# ---------------------------------------------------------------------------

def init_databases() -> None:
    """Create both databases and all required tables/indexes."""
    _init_data_db()
    _init_logs_db()
    logger.info("Databases initialized successfully.")


def _init_data_db() -> None:
    """Create the data database tables."""
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

    # Analysis snapshots — stores full dashboard JSON per timestamp
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analysis_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            snapshot_json TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_date ON analysis_snapshots(date)")

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

    logger.info(f"[DB] Saved {len(df)} rows -> {table_name} (date={date_str})")


def _save_wide_metric(df: pd.DataFrame, table_name: str, date_str: str) -> None:
    """Save a wide-format metric DataFrame to its table."""
    conn = get_data_conn()

    # Add a _date column for partitioning by day
    df_out = df.copy()
    df_out.insert(0, "_date", date_str)

    # Delete existing data for this date (overwrite semantics like the CSV system)
    try:
        conn.execute(f'DELETE FROM "{table_name}" WHERE _date = ?', (date_str,))
    except sqlite3.OperationalError:
        pass  # Table doesn't exist yet — to_sql will create it

    # Write to DB (creates table on first call, appends on subsequent)
    df_out.to_sql(table_name, conn, if_exists="append", index=False)
    conn.commit()


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
    conn.execute("DELETE FROM corrente_dc WHERE date = ?", (date_str,))

    # Bulk insert
    result.to_sql("corrente_dc", conn, if_exists="append", index=False)
    conn.commit()

    logger.info(f"[DB] Normalized {len(df)} wide rows -> {len(result)} DC readings")


# ---------------------------------------------------------------------------
# Load Metric Data
# ---------------------------------------------------------------------------

def load_metric(date_str: str, metric_name: str) -> pd.DataFrame | None:
    """
    Load a metric from the database as a DataFrame.

    Returns the same wide-format DataFrame that the processor/watchdog expects,
    preserving full backward compatibility with the CSV-based system.

    Returns None if the metric is not found or the table doesn't exist.
    """
    table_name = _resolve_table_name(metric_name)

    if table_name == "corrente_dc":
        return _load_corrente_dc(date_str)
    else:
        return _load_wide_metric(table_name, date_str)


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

    # Deduplicate by Ora if present (same logic as old load_metric)
    if "Ora" in df.columns:
        df = df.drop_duplicates(subset=["Ora"], keep="last").reset_index(drop=True)

    logger.info(f"[DB] Loaded {table_name} for {date_str} ({len(df)} rows)")
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

    logger.info(f"[DB] Loaded corrente_dc for {date_str} -> {wide.shape[0]} rows × {wide.shape[1]} cols")
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
    """Save an analysis snapshot to the database."""
    conn = get_data_conn()

    snapshot_json = json.dumps(snapshot_data, cls=NumpyEncoder)
    conn.execute(
        "INSERT INTO analysis_snapshots (date, timestamp, snapshot_json) VALUES (?, ?, ?)",
        (date_str, timestamp_str, snapshot_json)
    )

    # Keep only the last 50 snapshots per date (same policy as old JSON file)
    conn.execute("""
        DELETE FROM analysis_snapshots
        WHERE date = ? AND id NOT IN (
            SELECT id FROM analysis_snapshots
            WHERE date = ?
            ORDER BY timestamp DESC
            LIMIT 50
        )
    """, (date_str, date_str))

    conn.commit()


def load_latest_snapshot(date_str: str) -> dict | None:
    """Load the most recent analysis snapshot for a given date."""
    conn = get_data_conn()

    try:
        row = conn.execute(
            "SELECT snapshot_json FROM analysis_snapshots "
            "WHERE date = ? ORDER BY timestamp DESC LIMIT 1",
            (date_str,)
        ).fetchone()
    except Exception:
        return None

    if row is None:
        return None

    return json.loads(row[0])


def load_all_snapshots(date_str: str) -> dict:
    """Load all analysis snapshots for a given date as {timestamp: data}."""
    conn =  get_data_conn()

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
    conn = get_data_conn()
    conn.execute("DELETE FROM analysis_snapshots WHERE date = ?", (date_str,))
    conn.commit()


# ---------------------------------------------------------------------------
# Extraction Status (replaces extraction_status.json)
# ---------------------------------------------------------------------------

def save_extraction_status(date_str: str, metric_type: str, status: str = "success") -> None:
    """Record that a metric was extracted successfully."""
    conn = get_data_conn()

    timestamp = datetime.now().isoformat(timespec="seconds")

    # Upsert: delete old status for this metric+date, then insert new
    conn.execute(
        "DELETE FROM extraction_status WHERE date = ? AND metric_type = ?",
        (date_str, metric_type)
    )
    conn.execute(
        "INSERT INTO extraction_status (date, metric_type, status, timestamp) VALUES (?, ?, ?, ?)",
        (date_str, metric_type, status, timestamp)
    )
    conn.commit()


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
            DB_DIR.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(
                str(LOGS_DB_PATH), check_same_thread=False, timeout=30
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            # Ensure table exists
            self._conn.execute("""
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
            self._conn.commit()
        return self._conn

    def emit(self, record: logging.LogRecord) -> None:
        try:
            conn = self._get_conn()

            # Format the traceback if present
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
