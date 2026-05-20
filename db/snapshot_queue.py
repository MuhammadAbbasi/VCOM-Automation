"""db/snapshot_queue.py

Background single-writer queue for analysis snapshots.

Use `enqueue_snapshot(date_str, timestamp_str, snapshot)` to push snapshots
from any process/thread. The worker serializes writes and retries on failure.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any

from db.db_manager import save_analysis_snapshot

logger = logging.getLogger(__name__)

# Bounded queue to avoid unlimited memory growth
_Q_MAXSIZE = 256
_snapshot_q: "queue.Queue[tuple[str, str, dict]]" = queue.Queue(maxsize=_Q_MAXSIZE)
_worker_started = False


def _worker_loop() -> None:
    while True:
        item = _snapshot_q.get()
        if item is None:
            _snapshot_q.task_done()
            break

        date_str, timestamp_str, snapshot = item
        try:
            # Save with a few internal attempts to handle transient DB locks
            for attempt in range(1, 5):
                try:
                    save_analysis_snapshot(date_str, timestamp_str, snapshot)
                    logger.info(f"[SNAPSHOT-WORKER] Saved snapshot {date_str} {timestamp_str}")
                    break
                except Exception as e:
                    logger.warning(f"[SNAPSHOT-WORKER] Save attempt {attempt} failed: {e}")
                    time.sleep(0.5 * attempt)
            else:
                logger.error(f"[SNAPSHOT-WORKER] Failed to save snapshot after retries: {date_str} {timestamp_str}")
        finally:
            _snapshot_q.task_done()


def start_snapshot_worker() -> None:
    """Start the background worker thread (idempotent)."""
    global _worker_started
    if _worker_started:
        return
    t = threading.Thread(target=_worker_loop, name="snapshot-worker", daemon=True)
    t.start()
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
