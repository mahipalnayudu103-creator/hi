import math
from pathlib import Path
from io import BytesIO
from typing import Tuple, List, Any
import numpy as np
import pandas as pd
import polars as pl
from csv_reader import seek_first_timestamp_offset, read_header_columns, DEFAULT_DELIMITER

# Global caches for GPU availability
_cudf_available_cache = None
_cupy_available_cache = None
_gpu_polars_available_cache = None
_renko_multi_kernel = None

UP_FILL = "#22c55e"
UP_LINE = "#16a34a"
DOWN_FILL = "#fb7185"
DOWN_LINE = "#e11d48"

def detect_cudf_available() -> bool:
    global _cudf_available_cache
    if _cudf_available_cache is not None:
        return _cudf_available_cache
    try:
        import cudf
        _cudf_available_cache = True
    except Exception:
        _cudf_available_cache = False
    return _cudf_available_cache

def detect_cupy_available() -> bool:
    global _cupy_available_cache
    if _cupy_available_cache is not None:
        return _cupy_available_cache
    try:
        import cupy as cp
        x = cp.array([1.0])
        y = x * 2.0
        if float(y[0]) != 2.0:
            raise ValueError("Verification failed")
        _cupy_available_cache = True
    except Exception:
        _cupy_available_cache = False
    return _cupy_available_cache

def detect_gpu_polars_available() -> bool:
    global _gpu_polars_available_cache
    if _gpu_polars_available_cache is not None:
        return _gpu_polars_available_cache
    try:
        # Test if collecting with engine="gpu" works
        df = pl.DataFrame({"a": [1]}).lazy().collect(engine="gpu")
        _gpu_polars_available_cache = True
    except Exception:
        _gpu_polars_available_cache = False
    return _gpu_polars_available_cache

def detect_gpu_available() -> bool:
    return detect_cudf_available() or detect_cupy_available() or detect_gpu_polars_available()

def read_selected_range_gpu_polars(
    path: Path,
    delimiter: str,
    time_col: str,
    source: str,
    bid_col: str | None,
    ask_col: str | None,
    start_t: pd.Timestamp,
    end_t: pd.Timestamp,
    max_rows: int | None,
) -> Tuple[np.ndarray, np.ndarray, int, int, str]:
    # Scan the CSV lazy frame
    lf = pl.scan_csv(
        path,
        separator=delimiter,
        try_parse_dates=True,
        infer_schema=True,
    )
    
    # Filter by time range
    start_naive = start_t.tz_localize(None) if start_t.tzinfo is not None else start_t
    end_naive = end_t.tz_localize(None) if end_t.tzinfo is not None else end_t
    
    lf = lf.filter(
        (pl.col(time_col) >= start_naive) & (pl.col(time_col) < end_naive)
    )
    
    # Limit rows to max_rows when requested
    if max_rows is not None:
        lf = lf.limit(max_rows)
    
    # Price projection
    if source == "__mid__":
        if not bid_col or not ask_col:
            raise ValueError("Mid price requires both bid and ask columns.")
        lf = lf.with_columns(
            ((pl.col(bid_col).cast(pl.Float64) + pl.col(ask_col).cast(pl.Float64)) / 2.0).alias("price")
        )
    else:
        lf = lf.with_columns(
            pl.col(source).cast(pl.Float64).alias("price")
        )
        
    lf = lf.filter(pl.col("price").is_not_null() & pl.col("price").is_finite())
    
    # Collect using Polars GPU Engine
    df = lf.collect(engine="gpu")
    
    prices = df["price"].to_numpy()
    
    # String timestamps formatted
    times_arr = df[time_col].dt.strftime("%Y-%m-%d %H:%M:%S.%f").to_numpy()
    times = np.array([t[:-3] if len(t) > 23 else t for t in times_arr], dtype=object)
    
    return prices, times, len(df), len(df), "GPU Polars"

def read_selected_range_gpu(
    path: Path,
    delimiter: str,
    time_col: str,
    source: str,
    bid_col: str | None,
    ask_col: str | None,
    start_t: pd.Timestamp,
    end_t: pd.Timestamp,
    max_rows: int | None,
) -> Tuple[np.ndarray, np.ndarray, int, int, str]:
    import cudf
    import cupy as cp
    
    usecols = [time_col]
    if source == "__mid__":
        if not bid_col or not ask_col:
            raise ValueError("Mid price requires both bid and ask columns.")
        usecols.extend([bid_col, ask_col])
    else:
        usecols.append(source)
    usecols = list(dict.fromkeys(usecols))
    
    columns = read_header_columns(path, delimiter)
    time_index = columns.index(time_col)
    
    with path.open("rb") as handle:
        header = handle.readline()
        data_start = handle.tell()
    offset = seek_first_timestamp_offset(path, start_t, data_start, time_index, delimiter)
    
    prices_list = []
    times_list = []
    rows_scanned = 0
    rows_loaded = 0
    chunk_size_bytes = 100 * 1024 * 1024  # 100MB chunk size
    
    with path.open("rb") as handle:
        handle.seek(offset)
        while True:
            chunk_data = handle.read(chunk_size_bytes)
            if not chunk_data:
                break
            last_newline = chunk_data.rfind(b"\n")
            if last_newline == -1:
                aligned_data = chunk_data
            else:
                aligned_data = chunk_data[:last_newline + 1]
                back_seek = len(chunk_data) - (last_newline + 1)
                handle.seek(handle.tell() - back_seek)
                
            df = cudf.read_csv(
                BytesIO(header + aligned_data),
                sep=delimiter,
                usecols=usecols,
            )
            if df.empty:
                continue
                
            rows_scanned += len(df)
            
            df[time_col] = cudf.to_datetime(df[time_col], errors="coerce")
            df = df.dropna(subset=[time_col])
            if df.empty:
                continue
                
            batch_min = df[time_col].min()
            batch_max = df[time_col].max()
            
            if batch_min >= end_t:
                break
            if batch_max < start_t:
                continue
                
            mask = (df[time_col] >= start_t) & (df[time_col] < end_t)
            df_sub = df.loc[mask]
            if df_sub.empty:
                continue
                
            if source == "__mid__":
                df_sub["price"] = (cudf.to_numeric(df_sub[bid_col], errors="coerce") + cudf.to_numeric(df_sub[ask_col], errors="coerce")) / 2.0
            else:
                df_sub["price"] = cudf.to_numeric(df_sub[source], errors="coerce")
                
            df_sub = df_sub.dropna(subset=["price"])
            if df_sub.empty:
                continue
                
            p_arr = df_sub["price"].values.to_numpy()
            t_arr = df_sub[time_col].dt.strftime("%Y-%m-%d %H:%M:%S.%f").to_numpy()
            t_arr = np.array([t[:-3] if len(t) > 23 else t for t in t_arr], dtype=object)
            
            prices_list.append(p_arr)
            times_list.append(t_arr)
            rows_loaded += len(p_arr)
            
            if max_rows is not None and rows_loaded >= max_rows:
                break
                
    if not prices_list:
        return np.array([], dtype=np.float64), np.array([], dtype=object), rows_scanned, 0, "GPU cuDF"
        
    return np.concatenate(prices_list), np.concatenate(times_list), rows_scanned, rows_loaded, "GPU cuDF"

def build_renko_gpu_multi(
    prices: np.ndarray,
    times: np.ndarray,
    pips: List[float],
    reversal_boxes: int,
    pip_size: float,
    anchor_mode: str,
) -> List[Tuple]:
    global _renko_multi_kernel
    
    if not detect_cupy_available():
        raise RuntimeError("CuPy GPU Engine is not available on this system.")
        
    import cupy as cp
    
    if _renko_multi_kernel is None:
        _renko_multi_kernel = cp.RawKernel(r'''
        extern "C" __global__
        void calculate_renko_multi(
            const double* prices,
            const long long n,
            const double* brick_sizes,
            const int reversal_boxes,
            const int anchor_mode, // 0=floor, 1=round, 2=first
            double* out_opens,      // shape: 4 * n
            double* out_closes,     // shape: 4 * n
            double* out_highs,      // shape: 4 * n
            double* out_lows,       // shape: 4 * n
            int* out_directions,    // shape: 4 * n
            int* out_ticks,         // shape: 4 * n
            long long* out_times_idx, // shape: 4 * n
            long long* out_brick_counts // length: 4
        ) {
            int chart_idx = blockIdx.x;
            if (chart_idx >= 4) return;
            if (threadIdx.x != 0) return;
            
            double brick_size = brick_sizes[chart_idx];
            double* my_opens = out_opens + chart_idx * n;
            double* my_closes = out_closes + chart_idx * n;
            double* my_highs = out_highs + chart_idx * n;
            double* my_lows = out_lows + chart_idx * n;
            int* my_directions = out_directions + chart_idx * n;
            int* my_ticks = out_ticks + chart_idx * n;
            long long* my_times_idx = out_times_idx + chart_idx * n;
            
            double last_close = 0.0;
            int direction = 0;
            long long brick_idx = 0;
            
            double live_open = 0.0;
            double live_high = 0.0;
            double live_low = 0.0;
            int live_tick_count = 0;
            
            bool has_first = false;
            double eps = brick_size / 1000000.0;
            
            for (long long i = 0; i < n; ++i) {
                double price = prices[i];
                if (!isfinite(price)) continue;
                
                if (!has_first) {
                    if (anchor_mode == 2) {
                        last_close = price;
                    } else if (anchor_mode == 1) {
                        last_close = round(price / brick_size) * brick_size;
                    } else {
                        last_close = floor(price / brick_size) * brick_size;
                    }
                    live_open = last_close;
                    live_high = price;
                    live_low = price;
                    live_tick_count = 1;
                    has_first = true;
                    continue;
                }
                
                live_high = max(live_high, price);
                live_low = min(live_low, price);
                live_tick_count++;
                
                while (true) {
                    int reversal = max(1, reversal_boxes);
                    double up_distance = (direction >= 0) ? brick_size : reversal * brick_size;
                    double down_distance = (direction <= 0) ? brick_size : reversal * brick_size;
                    double up_trigger = last_close + up_distance;
                    double down_trigger = last_close - down_distance;
                    
                    if (price >= up_trigger - eps) {
                        double brick_open = (direction < 0) ? (last_close + (reversal - 1) * brick_size) : last_close;
                        double brick_close = brick_open + brick_size;
                        
                        my_opens[brick_idx] = brick_open;
                        my_closes[brick_idx] = brick_close;
                        my_highs[brick_idx] = brick_close;
                        my_lows[brick_idx] = min(live_low, min(brick_open, brick_close));
                        my_directions[brick_idx] = 1;
                        my_ticks[brick_idx] = live_tick_count;
                        my_times_idx[brick_idx] = i;
                        
                        brick_idx++;
                        last_close = brick_close;
                        direction = 1;
                        
                        live_open = last_close;
                        live_high = last_close;
                        live_low = last_close;
                        live_tick_count = 0;
                        continue;
                    }
                    
                    if (price <= down_trigger + eps) {
                        double brick_open = (direction > 0) ? (last_close - (reversal - 1) * brick_size) : last_close;
                        double brick_close = brick_open - brick_size;
                        
                        my_opens[brick_idx] = brick_open;
                        my_closes[brick_idx] = brick_close;
                        my_highs[brick_idx] = max(live_high, max(brick_open, brick_close));
                        my_lows[brick_idx] = brick_close;
                        my_directions[brick_idx] = -1;
                        my_ticks[brick_idx] = live_tick_count;
                        my_times_idx[brick_idx] = i;
                        
                        brick_idx++;
                        last_close = brick_close;
                        direction = -1;
                        
                        live_open = last_close;
                        live_high = last_close;
                        live_low = last_close;
                        live_tick_count = 0;
                        continue;
                    }
                    
                    break;
                }
            }
            out_brick_counts[chart_idx] = brick_idx;
        }
        ''', 'calculate_renko_multi', options=('--use_fast_math',))
        
    prices_gpu = cp.asarray(prices, dtype=cp.float64)
    n = prices_gpu.size
    
    num_charts = len(pips)
    brick_sizes = [p * pip_size for p in pips]
    brick_sizes_gpu = cp.asarray(brick_sizes, dtype=cp.float64)
    
    out_opens = cp.zeros(num_charts * n, dtype=cp.float64)
    out_closes = cp.zeros(num_charts * n, dtype=cp.float64)
    out_highs = cp.zeros(num_charts * n, dtype=cp.float64)
    out_lows = cp.zeros(num_charts * n, dtype=cp.float64)
    out_directions = cp.zeros(num_charts * n, dtype=cp.int32)
    out_ticks = cp.zeros(num_charts * n, dtype=cp.int32)
    out_times_idx = cp.zeros(num_charts * n, dtype=cp.int64)
    out_brick_counts = cp.zeros(num_charts, dtype=cp.int64)
    
    anchor_mode_int = 0
    if anchor_mode == "round":
        anchor_mode_int = 1
    elif anchor_mode == "first":
        anchor_mode_int = 2
        
    _renko_multi_kernel((num_charts,), (1,), (
        prices_gpu, n, brick_sizes_gpu, reversal_boxes, anchor_mode_int,
        out_opens, out_closes, out_highs, out_lows,
        out_directions, out_ticks, out_times_idx, out_brick_counts
    ))
    
    counts = out_brick_counts.get()
    
    opens_all = out_opens.get()
    closes_all = out_closes.get()
    highs_all = out_highs.get()
    lows_all = out_lows.get()
    directions_all = out_directions.get()
    ticks_all = out_ticks.get()
    times_idx_all = out_times_idx.get()
    
    results = []
    for chart_idx in range(num_charts):
        brick_count = int(counts[chart_idx])
        start_offset = chart_idx * n
        end_offset = start_offset + brick_count
        
        opens = opens_all[start_offset:end_offset]
        closes = closes_all[start_offset:end_offset]
        highs = highs_all[start_offset:end_offset]
        lows = lows_all[start_offset:end_offset]
        directions = directions_all[start_offset:end_offset]
        ticks = ticks_all[start_offset:end_offset]
        times_idx = times_idx_all[start_offset:end_offset]
        
        tops = np.maximum(opens, closes)
        bottoms = np.minimum(opens, closes)
        indices = np.arange(brick_count, dtype=np.int32)
        
        colors = np.where(directions == 1, UP_FILL, DOWN_FILL)
        borders = np.where(directions == 1, UP_LINE, DOWN_LINE)
        dir_strs = np.where(directions == 1, "up", "down")
        brick_times = times[times_idx]
        results.append((
            indices, opens, closes, tops, bottoms, highs, lows,
            colors, borders, brick_times, ticks, dir_strs, times_idx, "GPU cuPy Multi"
        ))
        
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Playback Pipeline: GPU/CPU sort + frame-plan precomputation
# ─────────────────────────────────────────────────────────────────────────────

def sort_timeline_by_timestamp_gpu(timeline_events: list) -> tuple:
    """
    Sort a list of timeline event dicts by their 'ts' (pd.Timestamp) field
    using CuPy GPU argsort on nanosecond integer values.

    Returns:
        (sorted_list, engine_label)
    """
    import cupy as cp

    n = len(timeline_events)
    if n == 0:
        return timeline_events, "GPU CuPy argsort (empty)"

    # Convert pd.Timestamp → int64 nanoseconds for GPU comparison
    ts_ns = cp.array([ev["ts"].value for ev in timeline_events], dtype=cp.int64)
    sorted_indices = cp.argsort(ts_ns, stable=True)          # stable sort on GPU
    sorted_indices_cpu = sorted_indices.get()                 # one D2H transfer

    sorted_events = [timeline_events[int(i)] for i in sorted_indices_cpu]
    return sorted_events, "GPU CuPy argsort"


def sort_timeline_by_timestamp_cpu(timeline_events: list) -> tuple:
    """
    Sort a list of timeline event dicts by their 'ts' (pd.Timestamp) field
    using NumPy argsort on nanosecond integer values.

    Returns:
        (sorted_list, engine_label)
    """
    import numpy as np

    n = len(timeline_events)
    if n == 0:
        return timeline_events, "CPU NumPy argsort (empty)"

    ts_ns = np.fromiter((ev["ts"].value for ev in timeline_events),
                         dtype=np.int64, count=n)
    sorted_indices = np.argsort(ts_ns, kind="stable")

    sorted_events = [timeline_events[int(i)] for i in sorted_indices]
    return sorted_events, "CPU NumPy argsort"


def precompute_frame_plan_gpu(n_bricks: int, speed: float, frame_rate: int = 20) -> tuple:
    """
    Precompute all (start_idx, end_idx) frame boundaries for the 20 FPS
    playback loop using CuPy vectorised arange + minimum.

    At 20 FPS with a given speed multiplier, each frame delivers
        bricks_per_frame = max(1, int(speed / frame_rate))
    bricks.  All boundaries are computed in one GPU kernel call and
    transferred back as a Python list of (int, int) tuples.

    Returns:
        (frame_plan_list, engine_label)
    """
    import cupy as cp

    if n_bricks == 0:
        return [], "GPU CuPy arange (empty)"

    bricks_per_frame = max(1, int(speed * (1.0 / frame_rate)))

    starts = cp.arange(0, n_bricks, bricks_per_frame, dtype=cp.int32)
    ends   = cp.minimum(starts + bricks_per_frame, n_bricks)

    starts_cpu = starts.get().tolist()
    ends_cpu   = ends.get().tolist()

    frame_plan = list(zip(starts_cpu, ends_cpu))
    return frame_plan, f"GPU CuPy arange ({len(frame_plan)} frames, {bricks_per_frame} bricks/frame)"


def precompute_frame_plan_cpu(n_bricks: int, speed: float, frame_rate: int = 20) -> tuple:
    """
    Precompute all (start_idx, end_idx) frame boundaries for the 20 FPS
    playback loop using NumPy vectorised arange + minimum.

    Returns:
        (frame_plan_list, engine_label)
    """
    import numpy as np

    if n_bricks == 0:
        return [], "CPU NumPy arange (empty)"

    bricks_per_frame = max(1, int(speed * (1.0 / frame_rate)))

    starts = np.arange(0, n_bricks, bricks_per_frame, dtype=np.int32)
    ends   = np.minimum(starts + bricks_per_frame, n_bricks)

    frame_plan = list(zip(starts.tolist(), ends.tolist()))
    return frame_plan, f"CPU NumPy arange ({len(frame_plan)} frames, {bricks_per_frame} bricks/frame)"


def recompute_frame_plan_from_position(
    n_bricks: int,
    current_index: int,
    speed: float,
    frame_rate: int = 20,
    use_gpu: bool = False,
) -> tuple:
    """
    Recompute frame plan from `current_index` onwards after a speed change.
    Uses GPU if use_gpu=True and CuPy is available, otherwise CPU NumPy.

    Returns:
        (frame_plan_list, new_frame_index, engine_label)
        where new_frame_index=0 (frame_plan starts at current_index).
    """
    remaining = n_bricks - current_index
    if remaining <= 0:
        return [], 0, "CPU NumPy (nothing remaining)"

    import numpy as np
    bricks_per_frame = max(1, int(speed * (1.0 / frame_rate)))

    if use_gpu and detect_cupy_available():
        try:
            import cupy as cp
            offsets = cp.arange(0, remaining, bricks_per_frame, dtype=cp.int32)
            ends_rel = cp.minimum(offsets + bricks_per_frame, remaining)
            offsets_cpu = (offsets + current_index).get().tolist()
            ends_cpu    = (ends_rel + current_index).get().tolist()
            frame_plan  = list(zip(offsets_cpu, ends_cpu))
            label = f"GPU CuPy arange ({len(frame_plan)} frames from idx {current_index})"
            return frame_plan, 0, label
        except Exception:
            pass  # fall through to CPU

    offsets = np.arange(0, remaining, bricks_per_frame, dtype=np.int32) + current_index
    ends    = np.minimum(offsets + bricks_per_frame, n_bricks)
    frame_plan = list(zip(offsets.tolist(), ends.tolist()))
    label = f"CPU NumPy arange ({len(frame_plan)} frames from idx {current_index})"
    return frame_plan, 0, label
