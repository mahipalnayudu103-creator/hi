"""
parquet_cache.py — Incremental Parquet writer for confirmed Renko bricks.

Architecture:
  - One Parquet file per (job_id, pip_value)
  - Bricks are flushed in batches (BRICK_WRITE_BATCH_SIZE) to avoid RAM build-up
  - PyArrow ParquetWriter is kept open during build, closed at end
  - read_window() enables lazy frontend loading without reading full file

Cache layout:
  cache_store/
    <job_id>/
      renko_1.0pip.parquet
      renko_2.0pip.parquet
      renko_3.0pip.parquet
      renko_4.0pip.parquet
      job_meta.json
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from config import CACHE_DIR, BRICK_BATCH_SIZE

logger = logging.getLogger("renko_playback.parquet_cache")

# ── Config ────────────────────────────────────────────────────────────────────
BRICK_WRITE_BATCH_SIZE: int = BRICK_BATCH_SIZE

# ── PyArrow ───────────────────────────────────────────────────────────────────
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _ARROW_OK = True
except ImportError:
    pa = pq = None  # type: ignore
    _ARROW_OK = False
    logger.warning("pyarrow not available — Parquet caching disabled.")


# ── Schema ────────────────────────────────────────────────────────────────────
_BRICK_SCHEMA = (
    pa.schema([
        ("time",               pa.int64()),
        ("confirm_tick_index", pa.int64()),
        ("confirm_time",       pa.string()),
        ("open",               pa.float64()),
        ("high",               pa.float64()),
        ("low",                pa.float64()),
        ("close",              pa.float64()),
        ("direction",          pa.string()),
        ("tick_count",         pa.int32()),
        ("brick_size_pips",    pa.float32()),
        ("bid",                pa.float64()),
        ("ask",                pa.float64()),
    ])
    if _ARROW_OK else None
)


class RenkoParquetWriter:
    """
    Manages per-pip Parquet writers for one build job.

    Usage:
        writer = RenkoParquetWriter(job_id, [1.0, 2.0, 3.0, 4.0])
        writer.write_batch("1.0", bricks_list)
        writer.flush_all(remaining_buffers)
        writer.close_all()
        writer.write_meta({"rows_scanned": ...})
    """

    def __init__(self, job_id: str, chart_pips: List[float]) -> None:
        self.job_id      = job_id
        self.chart_pips  = chart_pips
        self.job_dir     = CACHE_DIR / job_id
        self.job_dir.mkdir(parents=True, exist_ok=True)

        self._writers: Dict[str, Any]   = {}   # pip_str → pq.ParquetWriter
        self._brick_counts: Dict[str, int] = {str(p): 0 for p in chart_pips}

        if _ARROW_OK:
            for pip in chart_pips:
                pip_str = str(pip)
                path    = self._parquet_path(pip_str)
                try:
                    self._writers[pip_str] = pq.ParquetWriter(
                        str(path), _BRICK_SCHEMA, compression="snappy"
                    )
                except Exception as exc:
                    logger.error(f"Failed to open ParquetWriter for pip {pip}: {exc}")

    # ── Write helpers ──────────────────────────────────────────────────────────

    def write_batch(self, pip_str: str, bricks: List[Dict[str, Any]]) -> None:
        """Append a batch of confirmed brick dicts to the Parquet file for pip_str."""
        if not bricks or not _ARROW_OK:
            return
        writer = self._writers.get(pip_str)
        if writer is None:
            return
        try:
            table = _bricks_to_table(bricks)
            writer.write_table(table)
            self._brick_counts[pip_str] = self._brick_counts.get(pip_str, 0) + len(bricks)
            del table
        except Exception as exc:
            logger.error(f"Parquet write_batch failed for pip {pip_str}: {exc}")

    def write_table(self, pip_str: str, table: Any) -> None:
        """Append a PyArrow Table directly to the Parquet file for pip_str."""
        if not _ARROW_OK or table is None:
            return
        writer = self._writers.get(pip_str)
        if writer is None:
            return
        try:
            writer.write_table(table)
            self._brick_counts[pip_str] = self._brick_counts.get(pip_str, 0) + table.num_rows
        except Exception as exc:
            logger.error(f"Parquet write_table failed for pip {pip_str}: {exc}")

    def flush_all(self, buffers: Dict[str, List[Dict[str, Any]]]) -> None:
        """Write any remaining bricks from the in-memory buffers to Parquet."""
        for pip_str, brick_list in buffers.items():
            if brick_list:
                self.write_batch(pip_str, brick_list)
                brick_list.clear()

    def close_all(self) -> None:
        """Close all open Parquet file handles."""
        for pip_str, writer in self._writers.items():
            try:
                writer.close()
            except Exception as exc:
                logger.warning(f"ParquetWriter close error (pip {pip_str}): {exc}")
        self._writers.clear()

    def write_meta(self, meta: Dict[str, Any]) -> None:
        """Write job_meta.json with build summary."""
        meta_path = self.job_dir / "job_meta.json"
        try:
            full_meta = {
                "job_id":       self.job_id,
                "created_at":   time.time(),
                "brick_counts": self._brick_counts,
                **meta,
            }
            meta_path.write_text(json.dumps(full_meta, indent=2))
        except Exception as exc:
            logger.warning(f"Meta write failed: {exc}")

    def brick_counts(self) -> Dict[str, int]:
        return dict(self._brick_counts)

    # ── Path helpers ───────────────────────────────────────────────────────────

    def _parquet_path(self, pip_str: str) -> Path:
        safe = pip_str.replace(".", "_")
        return self.job_dir / f"renko_{safe}pip.parquet"


# ── Read helpers (for frontend lazy-load) ─────────────────────────────────────

def read_window(
    job_id:    str,
    pip_str:   str,
    from_x:    Optional[int] = None,
    to_x:      Optional[int] = None,
    max_bricks: int = 1_000_000_000,
) -> List[Dict[str, Any]]:
    """
    Read a window of bricks from Parquet for lazy frontend loading.

    from_x / to_x are brick `time` values (tick_index * 1000 + seq).
    If both are None, returns the last max_bricks rows.
    """
    safe     = pip_str.replace(".", "_")
    path     = CACHE_DIR / job_id / f"renko_{safe}pip.parquet"
    if not path.exists():
        return []

    # 1. Primary path: Use Polars for ultra-fast multi-threaded memory-mapped reading & C++ filtering
    try:
        import polars as pl
        lf = pl.scan_parquet(str(path))
        if from_x is not None:
            lf = lf.filter(pl.col("time") >= from_x)
        if to_x is not None:
            lf = lf.filter(pl.col("time") <= to_x)

        lf = lf.tail(max_bricks)
        df = lf.collect()
        if df.is_empty():
            return []
        return df.to_dicts()
    except Exception as pl_exc:
        logger.info(f"Polars read_window failed/unavailable: {pl_exc}. Falling back to PyArrow...")

    # 2. Fallback path: Use PyArrow with memory mapping enabled
    if not _ARROW_OK:
        return []

    try:
        table = pq.read_table(str(path), memory_map=True)
        if table.num_rows == 0:
            return []

        if from_x is not None or to_x is not None:
            times = table.column("time").to_pylist()
            mask  = [
                (from_x is None or t >= from_x) and
                (to_x   is None or t <= to_x)
                for t in times
            ]
            indices = [i for i, m in enumerate(mask) if m]
            if indices:
                table = table.take(indices)

        # Cap to max_bricks (take the last N)
        if table.num_rows > max_bricks:
            table = table.slice(table.num_rows - max_bricks, max_bricks)

        bricks = table.to_pylist()
        del table
        return bricks
    except Exception as exc:
        logger.warning(f"read_window fallback failed for job {job_id} pip {pip_str}: {exc}")
        return []


def read_last_n_bricks(
    job_id: str,
    pip_str: str,
    n: int = 1_000_000_000,
) -> List[Dict[str, Any]]:
    """Return the last N confirmed bricks for initial chart render."""
    return read_window(job_id, pip_str, max_bricks=n)


def read_legacy_pip_cache_window(
    cache_key: str,
    pip_str: str,
    max_bricks: int = 20_000,
) -> List[Dict[str, Any]]:
    """Read the last N bricks from the flat per-pip cache without loading the full file."""
    path = _legacy_parquet_path(cache_key, float(pip_str))
    if not path.exists():
        return []

    try:
        import polars as pl
        df = pl.scan_parquet(str(path)).tail(max_bricks).collect()
        if df.is_empty():
            return []
        return df.to_dicts()
    except Exception as pl_exc:
        logger.info(f"Polars read_legacy_pip_cache_window failed/unavailable: {pl_exc}. Falling back to PyArrow...")

    if not _ARROW_OK:
        return []

    try:
        table = pq.read_table(str(path), memory_map=True)
        if table.num_rows > max_bricks:
            table = table.slice(table.num_rows - max_bricks, max_bricks)
        bricks = table.to_pylist()
        del table
        return bricks
    except Exception as exc:
        logger.warning(f"read_legacy_pip_cache_window failed for {path.name}: {exc}")
        return []


def get_job_meta(job_id: str) -> Optional[Dict[str, Any]]:
    """Read job_meta.json for a completed build."""
    meta_path = CACHE_DIR / job_id / "job_meta.json"
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text())
    except Exception:
        return None


def list_cached_jobs() -> List[str]:
    """List all job IDs with cached data."""
    if not CACHE_DIR.exists():
        return []
    return [d.name for d in CACHE_DIR.iterdir() if d.is_dir()]


def delete_job_cache(job_id: str) -> None:
    """Delete all cached files for a job."""
    import shutil
    job_dir = CACHE_DIR / job_id
    if job_dir.exists():
        shutil.rmtree(job_dir, ignore_errors=True)
        logger.info(f"Deleted cache for job {job_id}")


# ── Legacy cache helpers (kept for backward compat with existing cache_store) ──

def _legacy_parquet_path(cache_key: str, pip: float) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pip_str = str(pip).replace(".", "_")
    return CACHE_DIR / f"{cache_key}_pip{pip_str}.parquet"


def check_legacy_cache_per_pip(cache_key: str, chart_pips: List[float]) -> Dict[str, Optional[List[Dict]]]:
    """
    Check cache for each pip individually.
    Returns dict: pip_str → list of bricks (or None if not cached).
    Allows partial cache hits — only missing pips need to be built.
    """
    result: Dict[str, Optional[List[Dict]]] = {}
    for pip in chart_pips:
        pip_str = str(pip)
        path = _legacy_parquet_path(cache_key, pip)
        if not path.exists():
            result[pip_str] = None
            continue
        try:
            import polars as pl
            df = pl.read_parquet(str(path))
            result[pip_str] = df.to_dicts()
        except Exception:
            try:
                if _ARROW_OK:
                    table = pq.read_table(str(path), memory_map=True)
                    result[pip_str] = table.to_pylist()
                    del table
                else:
                    result[pip_str] = None
            except Exception:
                result[pip_str] = None
    return result


def check_legacy_cache_per_pip_counts(cache_key: str, chart_pips: List[float]) -> Dict[str, Optional[int]]:
    """
    Lightweight per-pip cache lookup.
    Returns pip_str -> row count, or None when that pip cache is missing/unreadable.
    Does not materialize brick rows, so it is safe for very large multi-year caches.
    """
    result: Dict[str, Optional[int]] = {}
    for pip in chart_pips:
        pip_str = str(pip)
        path = _legacy_parquet_path(cache_key, pip)
        if not path.exists():
            result[pip_str] = None
            continue
        try:
            if _ARROW_OK:
                parquet_file = pq.ParquetFile(str(path), memory_map=True)
                result[pip_str] = int(parquet_file.metadata.num_rows)
            else:
                result[pip_str] = None
        except Exception:
            result[pip_str] = None
    return result


def save_pip_cache(cache_key: str, pip: float, bricks: List[Dict[str, Any]]) -> None:
    """Save bricks for a single pip to the flat-file legacy cache."""
    if not _ARROW_OK:
        return
    path = _legacy_parquet_path(cache_key, pip)
    try:
        table = _bricks_to_table(bricks)
        pq.write_table(table, str(path), compression="snappy")
        logger.info(f"Saved pip {pip} cache: {path.name} ({len(bricks)} bricks)")
    except Exception as exc:
        logger.warning(f"save_pip_cache failed for pip {pip}: {exc}")


def check_legacy_cache(cache_key: str, chart_pips: List[float]) -> Optional[Dict[str, List[Dict]]]:
    """Check old flat-file cache format. Returns None if any pip is missing."""
    result = {}
    
    # 1. Try Polars (multi-threaded memory mapped read)
    try:
        import polars as pl
        for pip in chart_pips:
            path = _legacy_parquet_path(cache_key, pip)
            if not path.exists():
                return None
            df = pl.read_parquet(str(path))
            result[str(pip)] = df.to_dicts()
        return result
    except Exception as pl_exc:
        logger.info(f"Polars check_legacy_cache failed/unavailable: {pl_exc}. Falling back to PyArrow...")
        
    # 2. Fallback to PyArrow
    if not _ARROW_OK:
        return None
        
    result = {}
    for pip in chart_pips:
        path = _legacy_parquet_path(cache_key, pip)
        if not path.exists():
            return None
        try:
            table = pq.read_table(str(path), memory_map=True)
            result[str(pip)] = table.to_pylist()
            del table
        except Exception:
            return None
    return result


# ── Internal ──────────────────────────────────────────────────────────────────

def _bricks_to_table(bricks: List[Dict[str, Any]]) -> "pa.Table":
    """Convert list of brick dicts to a PyArrow table matching _BRICK_SCHEMA."""
    try:
        import polars as pl
        schema = {
            "time": pl.Int64,
            "confirm_tick_index": pl.Int64,
            "confirm_time": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "direction": pl.String,
            "tick_count": pl.Int32,
            "brick_size_pips": pl.Float32,
            "bid": pl.Float64,
            "ask": pl.Float64,
        }
        df = pl.DataFrame(bricks, schema=schema)
        return df.to_arrow().cast(_BRICK_SCHEMA)
    except Exception as exc:
        logger.warning(f"Polars _bricks_to_table failed: {exc}. Falling back to Python loop...")
        cols: Dict[str, list] = {field.name: [] for field in _BRICK_SCHEMA}
        for b in bricks:
            cols["time"].append(int(b.get("time", 0)))
            cols["confirm_tick_index"].append(int(b.get("confirm_tick_index", 0)))
            cols["confirm_time"].append(str(b.get("confirm_time", "")))
            cols["open"].append(float(b.get("open", 0)))
            cols["high"].append(float(b.get("high", 0)))
            cols["low"].append(float(b.get("low", 0)))
            cols["close"].append(float(b.get("close", 0)))
            cols["direction"].append(str(b.get("direction", "up")))
            cols["tick_count"].append(int(b.get("tick_count", 0)))
            cols["brick_size_pips"].append(float(b.get("brick_size_pips", 0)))
            cols["bid"].append(float(b.get("bid", 0)))
            cols["ask"].append(float(b.get("ask", 0)))

        arrays = [pa.array(cols[f.name], type=f.type) for f in _BRICK_SCHEMA]
        return pa.table(arrays, schema=_BRICK_SCHEMA)
