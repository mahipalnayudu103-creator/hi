import sqlite3
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from config import CACHE_DIR

logger = logging.getLogger("renko_playback.build_history")

DB_PATH = CACHE_DIR / "build_history.sqlite"

def init_db() -> None:
    """Initialize the SQLite database and create tables if they do not exist."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # Create table for build history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS build_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cache_key TEXT UNIQUE,
            job_id TEXT,
            csv_path TEXT,
            csv_size INTEGER,
            csv_mtime REAL,
            start_utc TEXT,
            end_utc TEXT,
            price_source TEXT,
            reversal_boxes INTEGER,
            pip_size REAL,
            anchor TEXT,
            chart_pips TEXT,       -- JSON array of floats
            brick_counts TEXT,     -- JSON dictionary (pip_str -> count)
            ticks_used INTEGER,
            rows_scanned INTEGER,
            engine_used TEXT,
            created_at REAL,
            cache_file_paths TEXT  -- JSON array/dictionary of file paths
        )
    """)
    
    # Create indexes for fast lookup
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_build_history_csv ON build_history(csv_path, csv_size, csv_mtime)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_build_history_key ON build_history(cache_key)")
    
    conn.commit()
    conn.close()
    logger.info(f"Build history database initialized at {DB_PATH}")

def save_build_record(record: Dict[str, Any]) -> None:
    """Insert or replace a build record in the database."""
    init_db()
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    try:
        # Prepare list and dict parameters as serialized JSON strings
        chart_pips_json = json.dumps(record.get("chart_pips", []))
        brick_counts_json = json.dumps(record.get("brick_counts", {}))
        cache_file_paths_json = json.dumps(record.get("cache_file_paths", {}))
        
        cursor.execute("""
            INSERT OR REPLACE INTO build_history (
                cache_key, job_id, csv_path, csv_size, csv_mtime,
                start_utc, end_utc, price_source, reversal_boxes,
                pip_size, anchor, chart_pips, brick_counts,
                ticks_used, rows_scanned, engine_used, created_at, cache_file_paths
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record["cache_key"],
            record.get("job_id", ""),
            str(record["csv_path"]),
            int(record["csv_size"]),
            float(record["csv_mtime"]),
            record["start_utc"],
            record["end_utc"],
            record["price_source"],
            int(record["reversal_boxes"]),
            float(record["pip_size"]),
            record["anchor"],
            chart_pips_json,
            brick_counts_json,
            int(record.get("ticks_used", 0)),
            int(record.get("rows_scanned", 0)),
            record.get("engine_used", ""),
            record.get("created_at", time.time()),
            cache_file_paths_json
        ))
        conn.commit()
        logger.info(f"Saved build record in SQLite: cache_key={record['cache_key']}")
    except Exception as e:
        logger.exception(f"Failed to save build record in SQLite: {e}")
    finally:
        conn.close()

def delete_build_record(cache_key: str) -> None:
    """Delete a record from the database by cache key."""
    init_db()
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM build_history WHERE cache_key = ?", (cache_key,))
        conn.commit()
        logger.info(f"Deleted build record from SQLite: cache_key={cache_key}")
    except Exception as e:
        logger.exception(f"Failed to delete build record: {e}")
    finally:
        conn.close()

def lookup_exact_match(
    csv_path: str,
    csv_size: int,
    csv_mtime: float,
    price_source: str,
    reversal_boxes: int,
    pip_size: float,
    anchor: str,
    chart_pips: List[float],
    start_utc: str,
    end_utc: str
) -> Optional[Dict[str, Any]]:
    """
    Find a record that exactly matches the CSV identifier, parameters, and time range.
    Returns the record dict if found, else None.
    """
    init_db()
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT * FROM build_history 
            WHERE csv_path = ? AND csv_size = ? AND csv_mtime = ?
              AND price_source = ? AND reversal_boxes = ? AND pip_size = ?
              AND anchor = ? AND start_utc = ? AND end_utc = ?
        """, (
            str(csv_path), int(csv_size), float(csv_mtime),
            price_source, int(reversal_boxes), float(pip_size),
            anchor, start_utc, end_utc
        ))
        
        row = cursor.fetchone()
        if not row:
            return None
            
        record = dict(row)
        
        # Deserialize JSON fields
        record["chart_pips"] = json.loads(record["chart_pips"])
        record["brick_counts"] = json.loads(record["brick_counts"])
        record["cache_file_paths"] = json.loads(record["cache_file_paths"])
        
        # Verify that all pips in chart_pips match the requested pips
        req_pips_sorted = sorted(chart_pips)
        rec_pips_sorted = sorted(record["chart_pips"])
        if req_pips_sorted != rec_pips_sorted:
            return None
            
        return record
    except Exception as e:
        logger.exception(f"Error lookup_exact_match: {e}")
        return None
    finally:
        conn.close()

def lookup_similar_matches(
    csv_path: str,
    csv_size: int,
    csv_mtime: float
) -> List[Dict[str, Any]]:
    """
    Find previous completed builds for the exact same CSV file.
    Matches must share the same file size and modification timestamp to ensure freshness.
    """
    init_db()
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT * FROM build_history 
            WHERE csv_path = ? AND csv_size = ? AND csv_mtime = ?
            ORDER BY created_at DESC
            LIMIT 10
        """, (str(csv_path), int(csv_size), float(csv_mtime)))
        
        rows = cursor.fetchall()
        results = []
        for row in rows:
            record = dict(row)
            record["chart_pips"] = json.loads(record["chart_pips"])
            record["brick_counts"] = json.loads(record["brick_counts"])
            record["cache_file_paths"] = json.loads(record["cache_file_paths"])
            results.append(record)
        return results
    except Exception as e:
        logger.exception(f"Error lookup_similar_matches: {e}")
        return []
    finally:
        conn.close()
