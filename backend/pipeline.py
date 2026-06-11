"""
pipeline.py — Streaming Renko build pipeline (low-RAM architecture).

Architecture
============

  CSV file
    └─► csv_stream.stream_ticks()   (yields one chunk at a time)
         └─► tick-by-tick loop       (4 RenkoState engines, one pass)
              └─► confirmed bricks   (metadata only, no raw ticks)
                   └─► brick_buffers  (per pip, in-memory list)
                        └─► RenkoParquetWriter.write_batch()  (flush every 50k)
                             └─► per-job Parquet files

RAM footprint at any moment:
  - Current chunk:  ~10 MB  (250k rows × 40 bytes)
  - 4 × RenkoState: < 1 KB  (7 scalars each)
  - 4 × brick_buffer: ~5 MB at most (50k bricks × 100 bytes)
  - Progress counters: negligible
  Total: ≈ 15–20 MB regardless of CSV size

Key rules:
  - Never concatenate chunks
  - Never store raw ticks past the current chunk
  - Check job.cancel_requested each chunk
  - Broadcast progress every ~0.5s
  - Flush remaining buffers and close Parquet writers at the end
"""

import asyncio
import gc
import logging
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("renko_playback.pipeline")

# ── Thread pool ───────────────────────────────────────────────────────────────
_CPU_COUNT = os.cpu_count() or 4
_IO_POOL   = ThreadPoolExecutor(max_workers=min(4, _CPU_COUNT), thread_name_prefix="csv_io")

# ── Imports from refactored modules ──────────────────────────────────────────
from csv_stream    import stream_ticks, CSV_CHUNK_ROWS
from renko_state   import build_streaming_engines
from parquet_cache import (
    RenkoParquetWriter, BRICK_WRITE_BATCH_SIZE,
    check_legacy_cache, _legacy_parquet_path,
    read_last_n_bricks,
)
from memory        import log_chunk_memory, get_process_ram_mb, force_gc, MAX_RAM_MB
from job_manager   import RenkoJob, SENTINEL
from monitor       import get_system_stats

# Legacy helpers kept for backward compat with old cache + sync endpoint
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _ARROW_OK = True
except ImportError:
    pa = pq = None    # type: ignore
    _ARROW_OK = False


# ── Legacy cache path (flat format — used by sync /api/build-renko) ───────────
CACHE_DIR = Path(__file__).resolve().parent / "cache_store"

def _cache_parquet_path(cache_key: str, pip: float) -> Path:
    return _legacy_parquet_path(cache_key, pip)

def _check_full_cache(cache_key: str, chart_pips: List[float]) -> Optional[Dict]:
    return check_legacy_cache(cache_key, chart_pips)


# ── Main streaming pipeline ───────────────────────────────────────────────────

async def run_build_pipeline(
    job:               RenkoJob,
    csv_path:          Path,
    delimiter:         str,
    time_col:          str,
    source:            str,
    bid_col:           Optional[str],
    ask_col:           Optional[str],
    start_t:           pd.Timestamp,
    end_t:             pd.Timestamp,
    chart_pips:        List[float],
    reversal_boxes:    int,
    pip_size:          float,
    anchor:            str,
    cache_key:         str,
    chunk_rows:        int = CSV_CHUNK_ROWS,
    max_rows:          Optional[int] = None,
    return_ticks:      bool = False,   # kept for API compat — always False now
) -> None:
    """
    Streaming Renko build pipeline.

    Reads CSV in chunks → processes tick-by-tick → writes bricks to Parquet.
    RAM stays flat regardless of CSV size.
    """
    loop = asyncio.get_event_loop()
    job.status = "running"
    job.stage  = "streaming_build"
    job.log("Streaming pipeline started.")

    # ── Check legacy cache first ──────────────────────────────────────────────
    cached = check_legacy_cache(cache_key, chart_pips)
    if cached is not None:
        job.log("CACHE HIT: Loaded pre-built bricks from legacy cache.")
        brick_counts = {str(pip): len(v) for pip, v in cached.items()}
        job.log(f"Bricks from cache: {brick_counts}")
        job.set_done(brick_counts, engine_used="Disk Cache (Legacy)")
        return

    # ── Build 4 streaming Renko engines ──────────────────────────────────────
    engines = build_streaming_engines(chart_pips, pip_size, reversal_boxes, anchor)
    job.log(f"Built {len(engines)} streaming RenkoState engines.")

    # ── Open incremental Parquet writers ──────────────────────────────────────
    writer = RenkoParquetWriter(job.job_id, chart_pips)

    # ── In-memory brick buffers (flushed every BRICK_WRITE_BATCH_SIZE) ────────
    brick_buffers: Dict[str, List[Dict[str, Any]]] = {str(p): [] for p in chart_pips}

    # ── Counters ──────────────────────────────────────────────────────────────
    rows_scanned   = 0
    ticks_loaded   = 0
    chunk_count    = 0
    last_progress  = time.perf_counter()
    t_start        = time.perf_counter()

    job.log(f"Reading {csv_path.name} in chunks of {chunk_rows:,} rows.")
    job.log(f"Range: {start_t} → {end_t}")

    # ── Detect engine ─────────────────────────────────────────────────────────
    try:
        import polars
        engine_label = "CPU Polars"
    except ImportError:
        try:
            import duckdb
            engine_label = "CPU DuckDB"
        except ImportError:
            engine_label = "CPU pandas"
    job.engine_used = f"{engine_label} + Streaming RenkoState"

    try:
        # ── Main streaming loop ───────────────────────────────────────────────
        def _stream_in_thread():
            """Run the generator in a thread to avoid blocking event loop."""
            return list(stream_ticks(
                csv_path   = csv_path,
                delimiter  = delimiter,
                time_col   = time_col,
                source     = source,
                bid_col    = bid_col,
                ask_col    = ask_col,
                start_t    = start_t,
                end_t      = end_t,
                chunk_rows = chunk_rows,
            ))

        # Process chunk by chunk using the generator directly in a thread
        # We use asyncio.to_thread so the event loop stays responsive
        def _process_next_chunk(gen_state: dict) -> Optional[tuple]:
            """Advance generator by one chunk. Returns None at end."""
            gen = gen_state.get("gen")
            if gen is None:
                gen = stream_ticks(
                    csv_path   = csv_path,
                    delimiter  = delimiter,
                    time_col   = time_col,
                    source     = source,
                    bid_col    = bid_col,
                    ask_col    = ask_col,
                    start_t    = start_t,
                    end_t      = end_t,
                    chunk_rows = chunk_rows,
                )
                gen_state["gen"] = gen
            try:
                return next(gen)
            except StopIteration:
                return None

        gen_state: Dict[str, Any] = {}

        while True:
            # Check cancellation
            if job.cancel_requested:
                job.log("Build cancelled by user request.")
                writer.flush_all(brick_buffers)
                writer.close_all()
                return

            # Get next chunk from CSV (run in thread pool to avoid blocking)
            chunk = await loop.run_in_executor(
                _IO_POOL, _process_next_chunk, gen_state
            )

            if chunk is None:
                # Generator exhausted
                break

            prices, times, bids, asks, nrows = chunk
            if nrows == 0:
                del prices, times, bids, asks, chunk
                continue

            chunk_count    += 1
            rows_scanned   += nrows
            global_tick_start = ticks_loaded

            # ── Parallel Batch processing via Numba JIT ───────────────────────
            # Since the JIT function releases the GIL, we can map execution
            # across our thread pool to process all engines in parallel on all CPU cores!
            def run_engine_batch(item):
                pip_str, engine = item
                return pip_str, engine.process_ticks_batch(prices, times, bids, asks, global_tick_start)

            # Process in thread pool
            batch_results = list(_IO_POOL.map(run_engine_batch, list(engines.items())))

            for pip_str, new_bricks in batch_results:
                if new_bricks:
                    brick_buffers[pip_str].extend(new_bricks)
                    # Flush buffer when it reaches batch size
                    if len(brick_buffers[pip_str]) >= BRICK_WRITE_BATCH_SIZE:
                        writer.write_batch(pip_str, brick_buffers[pip_str])
                        brick_buffers[pip_str].clear()
                        gc.collect()

            ticks_loaded += nrows

            # Get last time from chunk before deletion
            current_time_str = str(times[-1]) if len(times) > 0 else ""

            # ── Delete chunk from RAM immediately ─────────────────────────────
            del prices, times, bids, asks, chunk
            gc.collect()

            # ── Progress update every 0.5 s ───────────────────────────────────
            now = time.perf_counter()
            if now - last_progress >= 0.5:
                last_progress = now
                stats     = get_system_stats()
                ram_mb    = get_process_ram_mb()
                brick_counts = {k: len(v) + writer._brick_counts.get(k, 0)
                                for k, v in brick_buffers.items()}

                log_chunk_memory(
                    job.job_id, chunk_count, rows_scanned,
                    current_time_str, brick_counts
                )

                # RAM pressure check
                if ram_mb >= MAX_RAM_MB:
                    job.log(f"[WARNING] RAM={ram_mb:.0f}MB exceeds limit. Forcing GC + buffer flush.")
                    writer.flush_all(brick_buffers)
                    for buf in brick_buffers.values():
                        buf.clear()
                    ram_mb = force_gc()

                # Estimate progress from file size
                file_size = csv_path.stat().st_size
                elapsed   = now - t_start
                pct       = min(95.0, (rows_scanned / max(1, _estimate_rows(csv_path))) * 95.0)

                job.update_progress(
                    pct,
                    rows_scanned      = rows_scanned,
                    ticks_used        = ticks_loaded,
                    bricks_built      = brick_counts,
                    cpu_percent       = stats.get("cpu_percent"),
                    gpu_percent       = stats.get("gpu_percent"),
                    ram_used_gb       = stats.get("ram_used_gb"),
                    ram_used_mb       = ram_mb,
                    current_tick_time = current_time_str,
                    chunk_count       = chunk_count,
                    stage             = "streaming_build",
                )

                # Yield to event loop to allow WebSocket sends
                await asyncio.sleep(0)

        # ── Flush remaining buffers ───────────────────────────────────────────
        job.log("Flushing remaining brick buffers to Parquet…")
        writer.flush_all(brick_buffers)
        writer.close_all()

        # ── Write metadata ────────────────────────────────────────────────────
        final_counts = writer.brick_counts()
        writer.write_meta({
            "rows_scanned": rows_scanned,
            "ticks_loaded": ticks_loaded,
            "chunk_count":  chunk_count,
            "engine":       job.engine_used,
            "elapsed_s":    round(time.perf_counter() - t_start, 2),
        })

        elapsed = time.perf_counter() - t_start
        job.log(
            f"✅ Complete: {ticks_loaded:,} ticks | "
            f"{sum(final_counts.values()):,} total bricks | "
            f"{elapsed:.1f}s | {job.engine_used}"
        )
        for pip_str, count in sorted(final_counts.items()):
            job.log(f"  Pip {pip_str}: {count:,} bricks")

        job.set_done(final_counts, job.engine_used)

    except Exception as exc:
        import traceback
        logger.exception("Pipeline error")
        try:
            writer.flush_all(brick_buffers)
            writer.close_all()
        except Exception:
            pass
        job.log(f"CRITICAL ERROR: {exc}")
        job.log(traceback.format_exc())
        job.set_error(str(exc))


# ── Helper ────────────────────────────────────────────────────────────────────

def _estimate_rows(path: Path) -> int:
    """Fast row count estimate."""
    try:
        from csv_stream import _estimate_total_rows
        with path.open("rb") as fh:
            data_start = len(fh.readline())
        return _estimate_total_rows(path, data_start)
    except Exception:
        return 1_000_000
