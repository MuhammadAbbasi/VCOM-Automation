"""
db_migrate_to_docker.py — Migrate local SCADA databases into Docker volumes.

Steps performed:
  1. Checkpoint WAL journals into each database (ensures no data loss)
  2. VACUUM INTO optimized copies (compresses, defragments, reduces size)
  3. Integrity check on each vacuumed copy
  4. Create Docker named volumes if they don't exist
  5. Copy all db files + JSON state into scada_db_data volume
  6. Optionally copy backups into the volume
  7. Print before/after size summary

Run BEFORE the first `docker compose up --build -d`.
"""

import os
import sys
import shutil
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR  = Path(__file__).resolve().parent
DB_DIR      = SCRIPT_DIR / "db"
BACKUP_DIR  = DB_DIR / "backups"
TEMP_DIR    = Path(tempfile.gettempdir()) / "scada_migrate"

SQLITE_DBS = [
    "scada_data.db",
    "scada_snapshots.db",
    "scada_logs.db",
    "plant_data.db",
]

JSON_STATE_FILES = [
    "fault_state.json",
    "last_extraction.json",
    "last_db_backup.json",
    "last_ai_audit.json",
    "link_status.json",
    "plant_layout.json",
]

DOCKER_VOLUME = "scada_db_data"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def hr():
    print("-" * 60)

def fmt_size(path: Path) -> str:
    if not path.exists():
        return "missing"
    b = path.stat().st_size
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"

def run(cmd: list, check=True) -> subprocess.CompletedProcess:
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, capture_output=True, text=True)

def docker_available() -> bool:
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
        return result.returncode == 0
    except Exception:
        return False

def volume_exists(name: str) -> bool:
    result = subprocess.run(
        ["docker", "volume", "inspect", name],
        capture_output=True, text=True
    )
    return result.returncode == 0

# ---------------------------------------------------------------------------
# Step 1 — Checkpoint WAL journals
# ---------------------------------------------------------------------------

def checkpoint_wal(db_path: Path) -> None:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return
    wal = db_path.with_suffix(db_path.suffix + "-wal")
    if not wal.exists():
        return
    print(f"  Checkpointing WAL for {db_path.name} ({fmt_size(wal)} WAL) ...")
    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        print(f"    WAL checkpointed OK")
    except Exception as e:
        print(f"    WARNING: checkpoint failed: {e}")

# ---------------------------------------------------------------------------
# Step 2 — VACUUM INTO optimized copy
# ---------------------------------------------------------------------------

def vacuum_db(db_path: Path, out_path: Path) -> bool:
    if not db_path.exists() or db_path.stat().st_size == 0:
        print(f"  Skipping {db_path.name} (empty or missing)")
        return False

    before = db_path.stat().st_size
    print(f"  VACUUM {db_path.name}  [{fmt_size(db_path)}] → {out_path.name} ...", end=" ", flush=True)
    t0 = time.time()
    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"VACUUM INTO '{str(out_path)}'")
        conn.close()
    except Exception as e:
        print(f"\n    ERROR during VACUUM: {e}")
        # Fall back to plain copy
        shutil.copy2(db_path, out_path)
        print(f"    Fell back to plain copy")
        return True

    after = out_path.stat().st_size
    elapsed = time.time() - t0
    saved_pct = (1 - after / before) * 100 if before > 0 else 0
    print(f"done in {elapsed:.1f}s  [{fmt_size(out_path)}]  saved {saved_pct:.1f}%")
    return True

# ---------------------------------------------------------------------------
# Step 3 — Integrity check
# ---------------------------------------------------------------------------

def integrity_check(db_path: Path) -> bool:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return True
    print(f"  Integrity check {db_path.name} ...", end=" ", flush=True)
    try:
        conn = sqlite3.connect(str(db_path))
        result = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        ok = result[0] == "ok"
        print("OK" if ok else f"FAILED: {result[0]}")
        return ok
    except Exception as e:
        print(f"ERROR: {e}")
        return False

# ---------------------------------------------------------------------------
# Step 4 — Create Docker volume
# ---------------------------------------------------------------------------

def ensure_volume(name: str) -> None:
    if volume_exists(name):
        print(f"  Volume '{name}' already exists — keeping it.")
    else:
        print(f"  Creating volume '{name}' ...")
        run(["docker", "volume", "create", name])
        print(f"  Volume '{name}' created.")

# ---------------------------------------------------------------------------
# Step 5 — Copy files into volume via temp container
# ---------------------------------------------------------------------------

def copy_dir_to_volume(src_dir: Path, volume: str, dest_subpath: str = "") -> None:
    """Mount volume at /data and copy src_dir contents into dest_subpath."""
    dest = f"/data/{dest_subpath}".rstrip("/")
    src_posix = src_dir.as_posix()

    print(f"  Copying {src_dir} → volume:{dest} ...")
    result = run([
        "docker", "run", "--rm",
        "-v", f"{volume}:/data",
        "-v", f"{src_posix}:/src:ro",
        "python:3.12-slim-bookworm",
        "sh", "-c", f"mkdir -p {dest} && cp -r /src/. {dest}/"
    ])
    if result.returncode != 0:
        print(f"    ERROR: {result.stderr}")
    else:
        print(f"    Done.")

def copy_file_to_volume(src_file: Path, volume: str, dest_subpath: str = "") -> None:
    """Copy a single file into the volume."""
    dest_dir = f"/data/{dest_subpath}".rstrip("/") if dest_subpath else "/data"
    src_posix = src_file.parent.as_posix()
    filename = src_file.name

    result = run([
        "docker", "run", "--rm",
        "-v", f"{volume}:/data",
        "-v", f"{src_posix}:/src:ro",
        "python:3.12-slim-bookworm",
        "sh", "-c", f"mkdir -p {dest_dir} && cp /src/{filename} {dest_dir}/{filename}"
    ])
    if result.returncode != 0:
        print(f"    ERROR copying {filename}: {result.stderr}")

def verify_volume_contents(volume: str) -> None:
    print("\n  Volume contents after migration:")
    result = run([
        "docker", "run", "--rm",
        "-v", f"{volume}:/data",
        "python:3.12-slim-bookworm",
        "sh", "-c", "find /data -maxdepth 2 -type f | sort && du -sh /data"
    ], check=False)
    print(result.stdout)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    include_backups = "--with-backups" in sys.argv

    print("=" * 60)
    print("  SCADA Docker Database Migration")
    print("=" * 60)

    # Check Docker
    hr()
    print("Checking Docker ...")
    if not docker_available():
        print("ERROR: Docker is not running. Start Docker Desktop first.")
        sys.exit(1)
    print("  Docker OK")

    # Prepare temp working directory
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)
    TEMP_DIR.mkdir(parents=True)
    optimized_dir = TEMP_DIR / "optimized"
    optimized_dir.mkdir()

    # -----------------------------------------------------------------------
    # Step 1+2 — Checkpoint WAL + VACUUM each database
    # -----------------------------------------------------------------------
    hr()
    print("Checkpointing WAL journals and vacuuming databases ...")
    migrated_dbs = []
    for db_name in SQLITE_DBS:
        src = DB_DIR / db_name
        if not src.exists() or src.stat().st_size == 0:
            continue
        checkpoint_wal(src)
        out = optimized_dir / db_name
        if vacuum_db(src, out):
            migrated_dbs.append(out)

    # -----------------------------------------------------------------------
    # Step 3 — Integrity check
    # -----------------------------------------------------------------------
    hr()
    print("Running integrity checks ...")
    failed = []
    for db_path in migrated_dbs:
        if not integrity_check(db_path):
            failed.append(db_path.name)

    if failed:
        print(f"\nWARNING: Integrity check failed for: {failed}")
        answer = input("Continue anyway? (y/N): ").strip().lower()
        if answer != "y":
            sys.exit(1)
    else:
        print("  All databases passed integrity check.")

    # Copy JSON state files into temp dir
    json_dir = TEMP_DIR / "json"
    json_dir.mkdir()
    print("\nCopying JSON state files ...")
    for fname in JSON_STATE_FILES:
        src = DB_DIR / fname
        if src.exists():
            shutil.copy2(src, json_dir / fname)
            print(f"  Copied {fname}  [{fmt_size(src)}]")

    # -----------------------------------------------------------------------
    # Step 4 — Create volume
    # -----------------------------------------------------------------------
    hr()
    print(f"Setting up Docker volume '{DOCKER_VOLUME}' ...")
    ensure_volume(DOCKER_VOLUME)

    # -----------------------------------------------------------------------
    # Step 5 — Copy optimized databases into volume
    # -----------------------------------------------------------------------
    hr()
    print("Copying optimized databases into volume ...")
    copy_dir_to_volume(optimized_dir, DOCKER_VOLUME)

    hr()
    print("Copying JSON state files into volume ...")
    copy_dir_to_volume(json_dir, DOCKER_VOLUME)

    # -----------------------------------------------------------------------
    # Optional: backups
    # -----------------------------------------------------------------------
    if include_backups:
        hr()
        print(f"Copying backups ({fmt_size(BACKUP_DIR)}) into volume ...")
        if BACKUP_DIR.exists():
            copy_dir_to_volume(BACKUP_DIR, DOCKER_VOLUME, dest_subpath="backups")
        else:
            print("  No backups directory found, skipping.")
    else:
        print("\nTip: Run with --with-backups to also migrate db/backups/ (11 GB).")

    # -----------------------------------------------------------------------
    # Verify
    # -----------------------------------------------------------------------
    hr()
    print("Verifying volume contents ...")
    verify_volume_contents(DOCKER_VOLUME)

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    hr()
    print("Migration complete. Size summary:\n")
    total_before = total_after = 0
    for db_name in SQLITE_DBS:
        src = DB_DIR / db_name
        opt = optimized_dir / db_name
        if src.exists() and src.stat().st_size > 0:
            b = src.stat().st_size
            a = opt.stat().st_size if opt.exists() else b
            saved = (1 - a / b) * 100 if b > 0 else 0
            total_before += b
            total_after  += a
            print(f"  {db_name:<35} {fmt_size(src):>10} → {fmt_size(opt):>10}  ({saved:.1f}% saved)")

    if total_before > 0:
        total_saved = (1 - total_after / total_before) * 100
        print(f"\n  Total: {total_before/1e9:.2f} GB → {total_after/1e9:.2f} GB  ({total_saved:.1f}% saved)")

    hr()
    print("Next step:")
    print("  docker compose up --build -d")
    print()

    # Cleanup temp files
    shutil.rmtree(TEMP_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
