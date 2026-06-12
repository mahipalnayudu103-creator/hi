"""
pipeline.py — Streaming Renko build pipeline (low-RAM architecture).

Architecture
============

  CSV file
    └─► csv/stream.py:stream_ticks()   (yields one chunk at a time)
         └─► tick-by-tick loop          (4 RenkoState engines, one pass)
              └─► confirmed bricks      (metadata only, no raw ticks)
                   └─► brick_buffers    (per pip, in-memory list)
                        └─► parquet_store.py:RenkoParquetWriter.write_batch()  (flush every 50k)
                             └─► per-job Parquet files

RAM footprint at any moment:
  - Current chunk:  ~10 MB  (250k rows × 40 bytes)
  - 4 × RenkoState: < 1 KB  (7 scalars each)
  - 4 × brick_buffer: ~5 MB at most (50k bricks × 100 bytes)
  - Progress counters: negligible
  Total: ≈ 15–20 MB regardless of CSV size
"""

import asyncio
import gc
import logging
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
_IO_POOL   = ThreadPoolExecutor(max_workers=_CPU_COUNT, thread_name_prefix="csv_io")

# ── Imports from refactored modules ──────────────────────────────────────────
from services.csv.stream import stream_ticks, CSV_CHUNK_ROWS
from services.renko.state import build_streaming_engines
from services.cache.parquet_store import (
    RenkoParquetWriter, BRICK_WRITE_BATCH_SIZE,
    check_legacy_cache, _legacy_parquet_path,
    read_last_n_bricks, _BRICK_SCHEMA
)
from services.cache.build_history import record_build
from utils.memory import log_chunk_memory, get_process_ram_mb, force_gc, MAX_RAM_MB
from services.jobs.manager import RenkoJob, SENTINEL
from utils.monitor import get_system_stats
from utils.parquet_meta_cache import invalidate_job as _pmeta_invalidate_job
from config import CACHE_DIR

# GPU chunk size (~800 MB raw at 8 bytes/tick)
GPU_CHUNK_TICKS = int(50_000_000)


def _cache_parquet_path(cache_key: str, pip: float) -> Path:
    return _legacy_parquet_path(cache_key, pip)


def _check_full_cache(cache_key: str, chart_pips: List[float]) -> Optional[Dict]:
    return check_legacy_cache(cache_key, chart_pips)


# ── CPU Streaming Pipeline ────────────────────────────────────────────────────

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
    return_ticks:      bool = False,
    price_source:      str = "bid",
) -> None:
    """
    Streaming Renko build pipeline.
    Reads CSV in chunks → processes tick-by-tick → writes bricks to Parquet.
    """
    loop = asyncio.get_event_loop()
    job.status = "running"
    job.stage  = "streaming_build"
    job.log("Streaming pipeline started.")

    # Check legacy cache first
    cached = check_legacy_cache(cache_key, chart_pips)
    if cached is not None:
        job.log("CACHE HIT: Loaded pre-built bricks from legacy cache.")
        brick_counts = {str(pip): len(v) for pip, v in cached.items()}
        job.log(f"Bricks from cache: {brick_counts}")
        _pmeta_invalidate_job(job.job_id)
        job.set_done(brick_counts, engine_used="Disk Cache (Legacy)")
        return

    # Build 4 streaming Renko engines
    engines = build_streaming_engines(chart_pips, pip_size, reversal_boxes, anchor)
    job.log(f"Built {len(engines)} streaming RenkoState engines.")

    # Open incremental Parquet writers
    writer = RenkoParquetWriter(job.job_id, chart_pips)

    # In-memory brick buffers (flushed every BRICK_WRITE_BATCH_SIZE)
    brick_buffers: Dict[str, List[Dict[str, Any]]] = {str(p): [] for p in chart_pips}
    broadcast_accum: Dict[str, List[Dict[str, Any]]] = {str(p): [] for p in chart_pips}

    # Counters
    rows_scanned   = 0
    ticks_loaded   = 0
    chunk_count    = 0
    last_progress  = time.perf_counter()
    t_start        = time.perf_counter()

    job.log(f"Reading {csv_path.name} in chunks of {chunk_rows:,} rows.")
    job.log(f"Range: {start_t} → {end_t}")

    # Detect engine
    try:
        import polars
        engine_label = "CPU Polars"
    except ImportError:
        try:
            import duckdb
            engine_label = "CPU DuckDB"
        except ImportError:
            engine_label = "CPU pandas"
    job.engine_used = f"{engine_label} + Streaming RenkoState + cTrader body v2"

    try:
        # State helper for thread processing
        def _process_next_chunk(gen_state: dict) -> Optional[tuple]:
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

            # Get next chunk from CSV
            chunk = await loop.run_in_executor(
                _IO_POOL, _process_next_chunk, gen_state
            )

            if chunk is None:
                break

            prices, times, bids, asks, nrows = chunk
            if nrows == 0:
                del prices, times, bids, asks, chunk
                continue

            if max_rows is not None and ticks_loaded >= max_rows:
                del prices, times, bids, asks, chunk
                break

            if max_rows is not None and ticks_loaded + nrows > max_rows:
                keep_rows = max(0, max_rows - ticks_loaded)
                if keep_rows <= 0:
                    del prices, times, bids, asks, chunk
                    break
                prices = prices[:keep_rows]
                times = times[:keep_rows]
                bids = bids[:keep_rows] if bids is not None else bids
                asks = asks[:keep_rows] if asks is not None else asks
                nrows = keep_rows

            chunk_count    += 1
            rows_scanned   += nrows
            global_tick_start = ticks_loaded

            # Parallel Batch processing via Numba JIT (releases GIL)
            def run_engine_batch(item):
                pip_str, engine = item
                return pip_str, engine.process_ticks_batch(prices, times, bids, asks, global_tick_start)

            # Process in thread pool
            batch_results = list(_IO_POOL.map(run_engine_batch, list(engines.items())))

            for pip_str, new_bricks in batch_results:
                if new_bricks:
                    brick_buffers[pip_str].extend(new_bricks)
                    broadcast_accum[pip_str].extend(new_bricks)
                    if len(brick_buffers[pip_str]) >= BRICK_WRITE_BATCH_SIZE:
                        writer.write_batch(pip_str, brick_buffers[pip_str])
                        brick_buffers[pip_str].clear()
                        gc.collect()

            ticks_loaded += nrows
            current_time_str = str(times[-1]).replace("T", " ") if len(times) > 0 else ""

            # Delete chunk from RAM
            del prices, times, bids, asks, chunk
            gc.collect()

            # Progress update
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

                if ram_mb >= MAX_RAM_MB:
                    job.log(f"[WARNING] RAM={ram_mb:.0f}MB exceeds limit. Forcing GC + buffer flush.")
                    writer.flush_all(brick_buffers)
                    for buf in brick_buffers.values():
                        buf.clear()
                    ram_mb = force_gc()

                elapsed   = now - t_start
                pct       = min(95.0, (rows_scanned / max(1, _estimate_rows(csv_path))) * 95.0)
                to_send = {k: list(v[:500]) for k, v in broadcast_accum.items() if v}
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
                    new_bricks        = to_send if to_send else None,
                )
                for k in broadcast_accum:
                    broadcast_accum[k].clear()

                await asyncio.sleep(0)

        # Flush remaining buffers
        remaining_broadcast = {k: list(v[:500]) for k, v in broadcast_accum.items() if v}
        if any(remaining_broadcast.values()):
            job.update_progress(
                percent=100.0,
                rows_scanned=rows_scanned,
                ticks_used=ticks_loaded,
                bricks_built={k: len(v) + writer._brick_counts.get(k, 0) for k, v in brick_buffers.items()},
                current_tick_time=current_time_str,
                chunk_count=chunk_count,
                stage="streaming_build",
                new_bricks=remaining_broadcast
            )
            for k in broadcast_accum:
                broadcast_accum[k].clear()

        job.log("Flushing remaining brick buffers to Parquet…")
        writer.flush_all(brick_buffers)
        writer.close_all()

        # Copy per-pip Parquet files to base-key flat cache
        try:
            import shutil as _shutil
            for pip in chart_pips:
                pip_str = str(pip)
                src = CACHE_DIR / job.job_id / f"renko_{pip_str.replace('.', '_')}pip.parquet"
                dest = CACHE_DIR / f"{cache_key}_pip{pip_str.replace('.', '_')}.parquet"
                if src.exists() and not dest.exists():
                    _shutil.copy2(str(src), str(dest))
        except Exception as _cp_exc:
            logger.warning(f"Per-pip cache copy failed: {_cp_exc}")

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

        # Merge any partially-cached pips
        cached_pips_data: dict = getattr(job, "_partial_cached_pips", {}) or {}
        for k, v in cached_pips_data.items():
            if k not in final_counts:
                final_counts[k] = len(v)
        cached_pip_counts: dict = getattr(job, "_partial_cached_pip_counts", {}) or {}
        for k, v in cached_pip_counts.items():
            if k not in final_counts:
                final_counts[k] = int(v)

        _pmeta_invalidate_job(job.job_id)
        job.set_done(final_counts, job.engine_used)

        # Record in persistent build history
        if "preview" not in cache_key:
            try:
                cache_files = [
                    str(CACHE_DIR / job.job_id / f"renko_{str(p).replace('.', '_')}pip.parquet")
                    for p in chart_pips
                ]
                record_build(
                    cache_key     = cache_key,
                    csv_path      = csv_path,
                    start_utc     = str(start_t),
                    end_utc       = str(end_t),
                    price_source  = price_source,
                    reversal_boxes= reversal_boxes,
                    pip_size      = pip_size,
                    anchor        = anchor,
                    chart_pips    = chart_pips,
                    brick_counts  = {k: int(v) for k, v in final_counts.items()},
                    ticks_used    = ticks_loaded,
                    rows_scanned  = rows_scanned,
                    engine_used   = job.engine_used or "",
                    cache_files   = cache_files,
                    job_id        = job.job_id,
                )
            except Exception as _bh_exc:
                logger.warning(f"Build history write skipped: {_bh_exc}")

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


# ── GPU Streaming Pipeline ────────────────────────────────────────────────────

def _bricks_from_gpu_results(results, pips, bids, asks, tick_offset: int):
    import polars as pl
    tables = {}
    for idx, pip in enumerate(pips):
        pip_str = str(pip)
        res = results[idx]
        _, opens, closes, tops, bottoms, highs, lows, colors, borders, brick_times, ticks, dir_strs, times_idx, _ = res

        if len(opens) == 0:
            tables[pip_str] = None
            continue

        global_times_idx = times_idx + tick_offset
        bid_vals = bids[times_idx] if bids is not None else closes
        ask_vals = asks[times_idx] if asks is not None else closes

        try:
            confirm_time_str = pl.Series("confirm_time", brick_times).dt.strftime("%Y-%m-%d %H:%M:%S.%3f")
            df = pl.DataFrame({
                "confirm_tick_index": global_times_idx.astype(np.int64),
                "confirm_time":       confirm_time_str,
                "open":               opens.astype(np.float64),
                "high":               highs.astype(np.float64),
                "low":                lows.astype(np.float64),
                "close":              closes.astype(np.float64),
                "direction":          dir_strs.astype(str),
                "tick_count":         ticks.astype(np.int32),
                "brick_size_pips":    np.full(len(opens), float(pip), dtype=np.float32),
                "bid":                bid_vals.astype(np.float64),
                "ask":                ask_vals.astype(np.float64),
            })

            df = df.with_columns(
                (pl.col("confirm_tick_index").cum_count().over("confirm_tick_index") - 1).alias("seq")
            ).with_columns(
                (pl.col("confirm_tick_index") * 1000 + pl.col("seq")).alias("time")
            ).drop("seq")

            df = df.select([
                pl.col("time").cast(pl.Int64),
                pl.col("confirm_tick_index").cast(pl.Int64),
                pl.col("confirm_time").cast(pl.String),
                pl.col("open").round(10).cast(pl.Float64),
                pl.col("high").round(10).cast(pl.Float64),
                pl.col("low").round(10).cast(pl.Float64),
                pl.col("close").round(10).cast(pl.Float64),
                pl.col("direction").cast(pl.String),
                pl.col("tick_count").cast(pl.Int32),
                pl.col("brick_size_pips").cast(pl.Float32),
                pl.col("bid").round(10).cast(pl.Float64),
                pl.col("ask").round(10).cast(pl.Float64),
            ])

            tables[pip_str] = df.to_arrow().cast(_BRICK_SCHEMA)
        except Exception as exc:
            logger.warning(f"GPU chunk→Arrow failed for pip {pip_str}: {exc}")
            tables[pip_str] = None

    return tables


async def run_gpu_streaming_pipeline(
    job: RenkoJob,
    csv_path: Path,
    delimiter: str,
    time_col: str,
    source: str,
    bid_col: Optional[str],
    ask_col: Optional[str],
    start_t: pd.Timestamp,
    end_t: pd.Timestamp,
    chart_pips: List[float],
    reversal_boxes: int,
    pip_size: float,
    anchor: str,
    base_cache_key: str,
    max_rows: Optional[int] = None,
    chunk_rows: int = GPU_CHUNK_TICKS,
    price_source: str = "bid",
) -> None:
    """
    Streaming GPU Renko build.
    """
    import cupy as cp
    from services.renko.gpu_engine import build_renko_gpu_multi, detect_cupy_available
    from config import CUDA_DEVICE

    if not detect_cupy_available():
        raise RuntimeError("CuPy not available for GPU streaming pipeline.")

    cp.cuda.Device(CUDA_DEVICE).use()
    loop = asyncio.get_event_loop()
    job.status = "running"
    job.stage  = "gpu_streaming"
    job.log("GPU streaming pipeline started.")

    writer = RenkoParquetWriter(job.job_id, chart_pips)

    num_charts = len(chart_pips)
    state_last_closes = cp.zeros(num_charts, dtype=cp.float64)
    state_directions = cp.zeros(num_charts, dtype=cp.int32)
    state_live_opens = cp.zeros(num_charts, dtype=cp.float64)
    state_live_highs = cp.zeros(num_charts, dtype=cp.float64)
    state_live_lows = cp.zeros(num_charts, dtype=cp.float64)
    state_live_tick_counts = cp.zeros(num_charts, dtype=cp.int32)
    state_has_firsts = cp.zeros(num_charts, dtype=cp.int8)

    rows_scanned  = 0
    ticks_loaded  = 0
    chunk_count   = 0
    tick_offset   = 0
    brick_counts: Dict[str, int] = {str(p): 0 for p in chart_pips}
    t_start = time.perf_counter()

    def _run_chunk(prices_np, times_np, bids_np, asks_np, chunk_n):
        results = build_renko_gpu_multi(
            prices        = prices_np,
            times         = times_np,
            pips          = chart_pips,
            reversal_boxes= reversal_boxes,
            pip_size      = pip_size,
            anchor_mode   = anchor,
            state_last_closes = state_last_closes,
            state_directions = state_directions,
            state_live_opens = state_live_opens,
            state_live_highs = state_live_highs,
            state_live_lows = state_live_lows,
            state_live_tick_counts = state_live_tick_counts,
            state_has_firsts = state_has_firsts,
        )

        tables = _bricks_from_gpu_results(results, chart_pips, bids_np, asks_np, tick_offset)
        cp.get_default_memory_pool().free_all_blocks()
        cp.get_default_pinned_memory_pool().free_all_blocks()
        return tables

    job.log(f"Streaming {csv_path.name} in GPU chunks of {chunk_rows:,} ticks.")

    tick_gen = stream_ticks(
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

    async for chunk in _async_stream(tick_gen):
        if job.cancel_requested:
            job.log("GPU streaming cancelled.")
            break

        prices_np, times_np, bids_np, asks_np, nrows = chunk
        chunk_n = len(prices_np)

        if chunk_n == 0:
            rows_scanned += nrows
            continue

        if max_rows is not None and ticks_loaded >= max_rows:
            break

        if max_rows is not None and ticks_loaded + chunk_n > max_rows:
            keep_rows = max(0, max_rows - ticks_loaded)
            if keep_rows <= 0:
                break
            prices_np = prices_np[:keep_rows]
            times_np = times_np[:keep_rows]
            bids_np = bids_np[:keep_rows] if bids_np is not None else bids_np
            asks_np = asks_np[:keep_rows] if asks_np is not None else asks_np
            chunk_n = keep_rows
            nrows = keep_rows

        rows_scanned += nrows
        ticks_loaded += chunk_n
        chunk_count  += 1

        tables = await asyncio.to_thread(_run_chunk, prices_np, times_np, bids_np, asks_np, chunk_n)

        for pip_str, table in tables.items():
            if table is not None:
                writer.write_table(pip_str, table)
                brick_counts[pip_str] = brick_counts.get(pip_str, 0) + table.num_rows

        tick_offset += chunk_n

        if max_rows and ticks_loaded >= max_rows:
            job.log(f"Reached max_rows cap ({max_rows:,}).")
            break

        # Progress update
        pct = min(95.0, (ticks_loaded / max(1, _estimate_total_ticks(csv_path))) * 95.0)
        job.update_progress(
            pct,
            rows_scanned = rows_scanned,
            ticks_used   = ticks_loaded,
            bricks_built = brick_counts,
            stage        = "gpu_streaming",
        )
        await asyncio.sleep(0)

    writer.flush_all({})
    writer.close_all()

    # Copy per-pip files to base-key flat cache
    import shutil
    for pip in chart_pips:
        pip_str = str(pip)
        src  = CACHE_DIR / job.job_id / f"renko_{pip_str.replace('.', '_')}pip.parquet"
        dest = CACHE_DIR / f"{base_cache_key}_pip{pip_str.replace('.', '_')}.parquet"
        if src.exists() and not dest.exists():
            try:
                shutil.copy2(str(src), str(dest))
            except Exception as e:
                logger.warning(f"Per-pip cache copy failed: {e}")

    engine_used = "GPU CuPy Streaming + cTrader body v2"

    writer.write_meta({
        "rows_scanned": rows_scanned,
        "ticks_loaded": ticks_loaded,
        "chunk_count":  chunk_count,
        "engine":       engine_used,
        "elapsed_s":    round(time.perf_counter() - t_start, 2),
    })

    elapsed = time.perf_counter() - t_start
    job.log(
        f"✅ GPU streaming complete: {ticks_loaded:,} ticks | "
        f"{sum(brick_counts.values()):,} bricks | "
        f"{elapsed:.1f}s | {chunk_count} chunks"
    )
    _pmeta_invalidate_job(job.job_id)
    job.set_done(brick_counts, engine_used)


async def _async_stream(gen):
    for item in gen:
        yield item
        await asyncio.sleep(0)


def _estimate_total_ticks(csv_path: Path) -> int:
    try:
        size = csv_path.stat().st_size
        return max(1, size // 40)
    except Exception:
        return 1_000_000


# ── Helper ────────────────────────────────────────────────────────────────────

def _estimate_rows(path: Path) -> int:
    try:
        from services.csv.reader import open_compressed_file
        from services.csv.stream import _estimate_total_rows
        with open_compressed_file(path, "rb") as fh:
            data_start = len(fh.readline())
        return _estimate_total_rows(path, data_start)
    except Exception:
        return 1_000_000
