"""
gpu_pipeline.py — Streaming GPU Renko build pipeline.

Streams CSV in chunks → uploads each chunk to VRAM → runs CUDA kernel →
appends bricks to Parquet → frees VRAM. RAM stays flat for arbitrarily large files.

Architecture:
  CSV (NVMe) → RAM chunk (50M ticks) → VRAM → CUDA kernel → Parquet (SSD)
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd

from config import CACHE_DIR, CUDA_DEVICE, CUDA_PINNED_MEM
from services.parquet_cache import RenkoParquetWriter, _BRICK_SCHEMA
from utils.csv_stream import stream_ticks
from utils.parquet_meta_cache import invalidate_job as _pmeta_invalidate_job

logger = logging.getLogger("renko_playback.gpu_pipeline")

# Ticks per GPU chunk (~800 MB raw at 8 bytes/tick)
GPU_CHUNK_TICKS = int(50_000_000)


def _bricks_from_gpu_results(results, pips, bids, asks, tick_offset: int):
    """
    Convert GPU kernel results (for one chunk) to per-pip Arrow tables.
    tick_offset is added to times_idx so indices are global across chunks.
    """
    import polars as pl

    tables = {}
    for idx, pip in enumerate(pips):
        pip_str = str(pip)
        res = results[idx]
        _, opens, closes, tops, bottoms, highs, lows, colors, borders, brick_times, ticks, dir_strs, times_idx, _ = res

        if len(opens) == 0:
            tables[pip_str] = None
            continue

        # Adjust times_idx to global tick index
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
    job,
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
    Streaming GPU Renko build. Processes CSV in chunks to keep RAM/VRAM flat.
    Writes bricks incrementally to Parquet as each chunk completes.
    """
    import cupy as cp
    from services.gpu_engine import build_renko_gpu_multi, detect_cupy_available

    if not detect_cupy_available():
        raise RuntimeError("CuPy not available for GPU streaming pipeline.")

    cp.cuda.Device(CUDA_DEVICE).use()

    loop = asyncio.get_event_loop()
    job.status = "running"
    job.stage  = "gpu_streaming"
    job.log("GPU streaming pipeline started.")

    writer = RenkoParquetWriter(job.job_id, chart_pips)

    # Allocate CuPy state arrays that carry over between chunks
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

    # Build brick_sizes array for GPU kernel
    brick_sizes_arr = np.array([pip * pip_size for pip in chart_pips], dtype=np.float64)

    def _run_chunk(prices_np, times_np, bids_np, asks_np, chunk_n):
        """Upload chunk to VRAM, run kernel, download results, free VRAM."""
        # Use the existing kernel via build_renko_gpu_multi with state carry-over
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

        # Free VRAM immediately
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

        # Run GPU kernel in thread (don't block event loop)
        tables = await asyncio.to_thread(_run_chunk, prices_np, times_np, bids_np, asks_np, chunk_n)

        # Write output tables to Parquet
        for pip_str, table in tables.items():
            if table is not None:
                writer.write_table(pip_str, table)
                brick_counts[pip_str] = brick_counts.get(pip_str, 0) + table.num_rows

        tick_offset += chunk_n

        # Enforce max_rows cap
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

    # Copy per-pip files to base-key flat cache for future partial reuse
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
    """Wrap a synchronous generator to yield in async context."""
    for item in gen:
        yield item
        await asyncio.sleep(0)


def _estimate_total_ticks(csv_path: Path) -> int:
    """Rough estimate of total ticks in file for progress %."""
    try:
        size = csv_path.stat().st_size
        return max(1, size // 40)  # ~40 bytes per tick row
    except Exception:
        return 1_000_000
