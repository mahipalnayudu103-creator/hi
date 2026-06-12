"""
build_history.py — Persistent SQLite store for completed Renko builds.

One record per completed build; enables cross-session cache reuse detection.
DB path: backend/cache_store/build_history.sqlite
"""

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import CACHE_DIR

logger = logging.getLogger("renko_playback.build_history")

DB_PATH = CACHE_DIR / "build_history.sqlite"

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS builds (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    cache_key     TEXT    NOT NULL UNIQUE,
    job_id        TEXT    NOT NULL DEFAULT '',
    csv_path      TEXT    NOT NULL,
    csv_size      INTEGER NOT NULL,
    csv_mtime     REAL    NOT NULL,
    start_utc     TEXT    NOT NULL,
    end_utc       TEXT    NOT NULL,
    price_source  TEXT    NOT NULL,
    reversal_boxes INTEGER NOT NULL,
    pip_size      REAL    NOT NULL,
    anchor        TEXT    NOT NULL,
    chart_pips    TEXT    NOT NULL,   -- JSON array
    brick_counts  TEXT    NOT NULL,   -- JSON object {pip: count}
    ticks_used    INTEGER NOT NULL,
    rows_scanned  INTEGER NOT NULL,
    engine_used   TEXT    NOT NULL,
    created_at    REAL    NOT NULL,   -- unix timestamp
    cache_files   TEXT    NOT NULL    -- JSON array of file paths
);
CREATE INDEX IF NOT EXISTS idx_csv_path   ON builds (csv_path);
CREATE INDEX IF NOT EXISTS idx_cache_key  ON builds (cache_key);
CREATE INDEX IF NOT EXISTS idx_created_at ON builds (created_at);
"""


def _connect() -> sqlite3.Connection:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.executescript(_CREATE_SQL)
    conn.commit()
    
    # Run migration to add job_id if it doesn't exist
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(builds)")
        columns = [row["name"] for row in cursor.fetchall()]
        if "job_id" not in columns:
            cursor.execute("ALTER TABLE builds ADD COLUMN job_id TEXT NOT NULL DEFAULT ''")
            conn.commit()
            logger.info("Database migration: Added job_id column to builds table.")
    except Exception as exc:
        logger.warning(f"Database migration check failed: {exc}")
        
    return conn



def _normalize_utc(utc_str: str) -> str:
    """Normalize timestamp string to ISO 8601 UTC format with T and Z and exactly 3 millisecond digits."""
    if not utc_str:
        return ""
    try:
        import pandas as pd
        ts = pd.Timestamp(utc_str)
        return ts.strftime("%Y-%m-%dT%H:%M:%S.%3f") + "Z"
    except Exception:
        # Fallback to string manipulation if pandas parsing fails
        s = utc_str.replace(" ", "T")
        if s.endswith("+00:00"):
            s = s[:-6] + "Z"
        elif s.endswith("+0000"):
            s = s[:-5] + "Z"
        if not s.endswith("Z"):
            s = s + "Z"
        return s


def record_build(
    *,
    cache_key: str,
    csv_path: Path,
    start_utc: str,
    end_utc: str,
    price_source: str,
    reversal_boxes: int,
    pip_size: float,
    anchor: str,
    chart_pips: List[float],
    brick_counts: Dict[str, int],
    ticks_used: int,
    rows_scanned: int,
    engine_used: str,
    cache_files: Optional[List[str]] = None,
    job_id: str = "",
) -> None:
    """Insert or replace a completed build record."""
    try:
        stat = csv_path.stat()
        csv_size = stat.st_size
        csv_mtime = stat.st_mtime
    except Exception:
        csv_size = 0
        csv_mtime = 0.0

    try:
        norm_start = _normalize_utc(start_utc)
        norm_end = _normalize_utc(end_utc)
        conn = _connect()
        conn.execute(
            """
            INSERT INTO builds
                (cache_key, job_id, csv_path, csv_size, csv_mtime,
                 start_utc, end_utc, price_source, reversal_boxes,
                 pip_size, anchor, chart_pips, brick_counts,
                 ticks_used, rows_scanned, engine_used, created_at, cache_files)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cache_key) DO UPDATE SET
                job_id       = excluded.job_id,
                brick_counts = excluded.brick_counts,
                ticks_used   = excluded.ticks_used,
                rows_scanned = excluded.rows_scanned,
                engine_used  = excluded.engine_used,
                created_at   = excluded.created_at,
                cache_files  = excluded.cache_files
            """,
            (
                cache_key,
                job_id,
                str(csv_path),
                csv_size,
                csv_mtime,
                norm_start,
                norm_end,
                price_source,
                reversal_boxes,
                pip_size,
                anchor,
                json.dumps(sorted(chart_pips)),
                json.dumps(brick_counts),
                ticks_used,
                rows_scanned,
                engine_used,
                time.time(),
                json.dumps(cache_files or []),
            ),
        )
        conn.commit()
        conn.close()
        logger.info(f"Build history recorded: {cache_key[:16]}…")
    except Exception as exc:
        logger.warning(f"Failed to record build history: {exc}")



def lookup_exact(cache_key: str) -> Optional[Dict[str, Any]]:
    """Return the build record for an exact cache key, or None."""
    try:
        conn = _connect()
        row = conn.execute(
            "SELECT * FROM builds WHERE cache_key = ?", (cache_key,)
        ).fetchone()
        conn.close()
        if row:
            return _row_to_dict(row)
        return None
    except Exception as exc:
        logger.warning(f"Build history lookup failed: {exc}")
        return None


def lookup_similar(
    csv_path: Path,
    price_source: str,
    reversal_boxes: int,
    pip_size: float,
    anchor: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Return builds for the same CSV file with matching core params
    (different chart_pips, date range, or cache variant counts as 'similar').
    Validates csv_size and csv_mtime to detect stale/replaced files.
    """
    try:
        stat = csv_path.stat()
        csv_size = stat.st_size
        csv_mtime = stat.st_mtime
    except Exception:
        return []

    try:
        conn = _connect()
        rows = conn.execute(
            """
            SELECT * FROM builds
            WHERE csv_path = ?
              AND csv_size  = ?
              AND csv_mtime = ?
              AND price_source   = ?
              AND reversal_boxes = ?
              AND pip_size  = ?
              AND anchor    = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (
                str(csv_path), csv_size, csv_mtime,
                price_source, reversal_boxes, pip_size, anchor, limit,
            ),
        ).fetchall()
        conn.close()
        return [_row_to_dict(r) for r in rows]
    except Exception as exc:
        logger.warning(f"Similar build lookup failed: {exc}")
        return []


def list_recent(limit: int = 20) -> List[Dict[str, Any]]:
    """Return the most recent builds."""
    try:
        conn = _connect()
        rows = conn.execute(
            "SELECT * FROM builds ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [_row_to_dict(r) for r in rows]
    except Exception as exc:
        logger.warning(f"list_recent failed: {exc}")
        return []


def delete_by_key(cache_key: str) -> bool:
    """Delete a single build record."""
    try:
        conn = _connect()
        conn.execute("DELETE FROM builds WHERE cache_key = ?", (cache_key,))
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    d = dict(row)
    d["chart_pips"]   = json.loads(d["chart_pips"])
    d["brick_counts"] = json.loads(d["brick_counts"])
    d["cache_files"]  = json.loads(d["cache_files"])
    return d


def lookup_sub_range(
    csv_path: Path,
    price_source: str,
    reversal_boxes: int,
    pip_size: float,
    anchor: str,
    start_utc: str,
    end_utc: str,
    chart_pips: List[float],
) -> Optional[Dict[str, Any]]:
    """
    Find a completed build that contains the requested start_utc and end_utc
    sub-range.
    """
    try:
        stat = csv_path.stat()
        csv_size = stat.st_size
        csv_mtime = stat.st_mtime
    except Exception:
        return None

    try:
        norm_start = _normalize_utc(start_utc)
        norm_end = _normalize_utc(end_utc)
        conn = _connect()
        # Query builds that cover the requested range (cached_start <= requested_start and cached_end >= requested_end)
        # Note: start_utc and end_utc are ISO 8601 strings and can be compared lexicographically.
        rows = conn.execute(
            """
            SELECT * FROM builds
            WHERE csv_path = ?
              AND csv_size  = ?
              AND csv_mtime = ?
              AND price_source   = ?
              AND reversal_boxes = ?
              AND pip_size  = ?
              AND anchor    = ?
              AND start_utc <= ?
              AND end_utc   >= ?
            ORDER BY created_at DESC
            """,
            (
                str(csv_path), csv_size, csv_mtime,
                price_source, reversal_boxes, pip_size, anchor,
                norm_start, norm_end
            ),
        ).fetchall()
        conn.close()

        req_pips_sorted = sorted(chart_pips)
        for r in rows:
            record = _row_to_dict(r)
            # Verify chart pips match exactly
            if sorted(record.get("chart_pips", [])) == req_pips_sorted:
                return record
        return None
    except Exception as exc:
        logger.warning(f"lookup_sub_range failed: {exc}")
        return None

