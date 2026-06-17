"""db/snapshot_queue.py

Background single-writer queue for analysis snapshots.

Use `enqueue_snapshot(date_str, timestamp_str, snapshot)` to push snapshots
from any process/thread. The worker serializes writes and retries on failure.
"""
from __future__ import annotations

import logging
import queue
import random
import threading
import time
from typing import Any

from db.db_manager import save_analysis_snapshot, save_snapshot_fallback

logger = logging.getLogger(__name__)

# Bounded queue to avoid unlimited memory growth
_Q_MAXSIZE = 256
_snapshot_q: "queue.Queue[tuple[str, str, dict] | None]" = queue.Queue(maxsize=_Q_MAXSIZE)
_worker_started = False
_worker_thread: threading.Thread | None = None


def _worker_loop() -> None:
    while True:
        item = _snapshot_q.get()
        if item is None:
            _snapshot_q.task_done()
            break

        date_str, timestamp_str, snapshot = item
        try:
            # Save with a longer backoff for transient DB locks
            for attempt in range(1, 11):
                try:
                    save_analysis_snapshot(date_str, timestamp_str, snapshot)
                    logger.info(f"[SNAPSHOT-WORKER] Saved snapshot {date_str} {timestamp_str}")
                    break
                except Exception as e:
                    logger.warning(f"[SNAPSHOT-WORKER] Save attempt {attempt} failed: {e}")
                    if attempt == 10:
                        raise
                    time.sleep(0.5 * attempt + random.random())
        except Exception as exc:
            logger.error(f"[SNAPSHOT-WORKER] Failed to save snapshot after retries: {date_str} {timestamp_str}: {exc}")
            try:
                fallback_path = save_snapshot_fallback(date_str, timestamp_str, snapshot)
                logger.warning(f"[SNAPSHOT-WORKER] Snapshot fallback saved to JSON: {fallback_path}")
            except Exception as fallback_exc:
                logger.error(f"[SNAPSHOT-WORKER] Snapshot fallback failed: {fallback_exc}")
        finally:
            _snapshot_q.task_done()


def start_snapshot_worker() -> None:
    """Start the background worker thread (idempotent)."""
    global _worker_started, _worker_thread
    if _worker_started:
        return
    _worker_thread = threading.Thread(target=_worker_loop, name="snapshot-worker", daemon=True)
    _worker_thread.start()
    _worker_started = True
    logger.info("[SNAPSHOT-WORKER] Started snapshot worker thread")


def enqueue_snapshot(date_str: str, timestamp_str: str, snapshot: dict) -> None:
    """Queue a snapshot for background persistence.

    This function is safe to call from multiple threads. If the queue is full
    the snapshot will be dropped and an error logged (avoids blocking realtime loops).
    """
    start_snapshot_worker()
    try:
        _snapshot_q.put_nowait((date_str, timestamp_str, snapshot))
    except queue.Full:
        logger.error("[SNAPSHOT-WORKER] Snapshot queue full — dropping snapshot")


def flush_snapshot_queue(timeout: float = 30.0) -> bool:
    """Wait for all pending snapshots to be written."""
    start_snapshot_worker()
    try:
        _snapshot_q.join()
        return True
    except Exception as e:
        logger.warning(f"[SNAPSHOT-WORKER] Flush interrupted: {e}")
        return False


def shutdown_snapshot_worker(timeout: float = 30.0) -> bool:
    """Gracefully stop the snapshot worker after flushing pending items."""
    global _worker_started, _worker_thread
    if not _worker_started:
        return True
    try:
        _snapshot_q.put(None, timeout=5)
    except queue.Full:
        logger.warning("[SNAPSHOT-WORKER] Shutdown queue full; forcing stop")
    try:
        start = time.time()
        while _worker_thread is not None and _worker_thread.is_alive() and time.time() - start < timeout:
            time.sleep(0.1)
        if _worker_thread is not None and _worker_thread.is_alive():
            logger.warning("[SNAPSHOT-WORKER] Worker did not stop cleanly within timeout")
            return False
        return True
    except Exception as e:
        logger.warning(f"[SNAPSHOT-WORKER] Shutdown failed: {e}")
        return False
    finally:
        _worker_started = False
        _worker_thread = None
