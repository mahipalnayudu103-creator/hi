from pathlib import Path
from io import BytesIO
from typing import Tuple, List
import numpy as np
import pandas as pd
import polars as pl
from config import CUDA_DEVICE, CUDA_PINNED_MEM

# Global caches for GPU availability
_cudf_available_cache = None
_cupy_available_cache = None
_gpu_polars_available_cache = None

from services.renko.rules import UP_FILL, UP_LINE, DOWN_FILL, DOWN_LINE


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
    start_naive = start_t.tz_convert("UTC").tz_localize(None) if start_t.tzinfo is not None else start_t
    end_naive = end_t.tz_convert("UTC").tz_localize(None) if end_t.tzinfo is not None else end_t
    
    lf = lf.filter(
        (pl.col(time_col) >= start_naive) & (pl.col(time_col) <= end_naive)
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


# ── GPU bulk reader ───────────────────────────────────────────────────────────
def read_selected_range_gpu(
    csv_path: str,
    start_offset: int,
    end_offset: int,
    time_index: int,
    bid_index: int,
    ask_index: int,
    delimiter: str = ",",
    usecols: list = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, int]:
    """
    High-performance GPU-accelerated tick parser using NVIDIA cuDF.
    Falls back to CPU Polars/Pandas if cuDF is unavailable.
    """
    if usecols is None:
        usecols = ["Time", "Bid", "Ask"]

    if not detect_cudf_available():
        raise RuntimeError("cuDF is not installed or GPU device is not accessible.")

    import cudf
    import cupy as cp

    # Determine byte length to read
    length = end_offset - start_offset
    if length <= 0:
        raise ValueError("Invalid byte range for GPU reading.")

    # Memory-map or read the exact range into host RAM
    with open(csv_path, "rb") as fh:
        fh.seek(start_offset)
        raw_bytes = fh.read(length)

    # Convert to cuDF DataFrame
    # Note: cuDF read_csv requires a buffer or file path
    from io import BytesIO
    df = cudf.read_csv(
        BytesIO(raw_bytes),
        delimiter=delimiter,
        header=None,
        names=usecols,
        usecols=usecols,
    )

    # Cast to CuPy arrays (zero-copy GPU-to-GPU)
    # Assumes "Time" is parsed or string. If string, cuDF parses as datetime.
    # Let's perform conversion:
    if "Time" in df.columns:
        times_gpu = df["Time"].values.astype("datetime64[us]")
        times = cp.asnumpy(times_gpu)
    else:
        times = np.array([])

    bids = cp.asnumpy(df["Bid"].values)
    asks = cp.asnumpy(df["Ask"].values) if "Ask" in df.columns else bids
    prices = bids  # Default to Bid for Renko calculation

    return prices, times, bids, asks, len(df)


# ── GPU Multi-Chart Renko Kernel ──────────────────────────────────────────────
_renko_multi_kernel = None

def build_renko_gpu_multi(
    prices: np.ndarray,
    times: np.ndarray,
    pips: List[float],
    reversal_boxes: int,
    pip_size: float,
    anchor_mode: str,
    state_last_closes = None,
    state_directions = None,
    state_live_opens = None,
    state_live_highs = None,
    state_live_lows = None,
    state_live_tick_counts = None,
    state_has_firsts = None,
) -> List[Tuple]:
    global _renko_multi_kernel

    if not detect_cupy_available():
        raise RuntimeError("CuPy GPU Engine is not available on this system.")

    import cupy as cp
    cp.cuda.Device(CUDA_DEVICE).use()

    if _renko_multi_kernel is None:
        _renko_multi_kernel = cp.RawKernel(r'''
        extern "C" __global__
        void calculate_renko_multi(
            const double* prices,
            const long long n,
            const double* brick_sizes,
            const int num_charts,
            const int reversal_boxes,
            const int anchor_mode, // 0=floor, 1=round, 2=first
            double* out_opens,      // shape: num_charts * n
            double* out_closes,     // shape: num_charts * n
            double* out_highs,      // shape: num_charts * n
            double* out_lows,       // shape: num_charts * n
            int* out_directions,    // shape: num_charts * n
            int* out_ticks,         // shape: num_charts * n
            long long* out_times_idx, // shape: num_charts * n
            long long* out_brick_counts, // length: num_charts
            double* state_last_closes,
            int* state_directions,
            double* state_live_opens,
            double* state_live_highs,
            double* state_live_lows,
            int* state_live_tick_counts,
            char* state_has_firsts
        ) {
            int chart_idx = blockIdx.x;
            if (chart_idx >= num_charts) return;
            if (threadIdx.x != 0) return;
            
            double brick_size = brick_sizes[chart_idx];
            double* my_opens = out_opens + chart_idx * n;
            double* my_closes = out_closes + chart_idx * n;
            double* my_highs = out_highs + chart_idx * n;
            double* my_lows = out_lows + chart_idx * n;
            int* my_directions = out_directions + chart_idx * n;
            int* my_ticks = out_ticks + chart_idx * n;
            long long* my_times_idx = out_times_idx + chart_idx * n;
            
            double last_close = state_last_closes[chart_idx];
            int direction = state_directions[chart_idx];
            long long brick_idx = 0;
            
            double live_open = state_live_opens[chart_idx];
            double live_high = state_live_highs[chart_idx];
            double live_low = state_live_lows[chart_idx];
            int live_tick_count = state_live_tick_counts[chart_idx];
            
            bool has_first = state_has_firsts[chart_idx] != 0;
            double eps = brick_size / 1000000.0;
            
            for (long long i = 0; i < n; ++i) {
                if (brick_idx >= n) break;
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
                    if (brick_idx >= n) break;
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
                        my_lows[brick_idx] = brick_open;
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
                        my_highs[brick_idx] = brick_open;
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
            state_last_closes[chart_idx] = last_close;
            state_directions[chart_idx] = direction;
            state_live_opens[chart_idx] = live_open;
            state_live_highs[chart_idx] = live_high;
            state_live_lows[chart_idx] = live_low;
            state_live_tick_counts[chart_idx] = live_tick_count;
            state_has_firsts[chart_idx] = has_first ? 1 : 0;
        }
        ''', 'calculate_renko_multi', options=('--use_fast_math',))
        
    if CUDA_PINNED_MEM:
        # Pinned (page-locked) memory — zero-copy host-to-device transfers
        pinned = cp.cuda.alloc_pinned_memory(prices.nbytes)
        pinned_arr = np.frombuffer(pinned, dtype=np.float64, count=prices.size)
        pinned_arr[:] = prices.astype(np.float64)
        prices_gpu = cp.asarray(pinned_arr)
    else:
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
        
    # Expose and allocate state arrays if not supplied (for backward compatibility)
    if state_last_closes is None:
        state_last_closes = cp.zeros(num_charts, dtype=cp.float64)
    if state_directions is None:
        state_directions = cp.zeros(num_charts, dtype=cp.int32)
    if state_live_opens is None:
        state_live_opens = cp.zeros(num_charts, dtype=cp.float64)
    if state_live_highs is None:
        state_live_highs = cp.zeros(num_charts, dtype=cp.float64)
    if state_live_lows is None:
        state_live_lows = cp.zeros(num_charts, dtype=cp.float64)
    if state_live_tick_counts is None:
        state_live_tick_counts = cp.zeros(num_charts, dtype=cp.int32)
    if state_has_firsts is None:
        state_has_firsts = cp.zeros(num_charts, dtype=cp.int8)

    _renko_multi_kernel((num_charts,), (1,), (
        prices_gpu, np.int64(n), brick_sizes_gpu, np.int32(num_charts), np.int32(reversal_boxes), np.int32(anchor_mode_int),
        out_opens, out_closes, out_highs, out_lows,
        out_directions, out_ticks, out_times_idx, out_brick_counts,
        state_last_closes, state_directions,
        state_live_opens, state_live_highs, state_live_lows,
        state_live_tick_counts, state_has_firsts
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


# ── Timeline & Frame-plan helpers ─────────────────────────────────────────────
_radix_sort_kernel = None

def _get_radix_sort_kernel():
    """Compile GPU radix sort kernel once and cache it."""
    global _radix_sort_kernel
    if _radix_sort_kernel is not None:
        return _radix_sort_kernel
    import cupy as cp
    _radix_sort_kernel = cp.RawKernel(r'''
    extern "C" __global__
    void radix_count(
        const long long* keys, int n, int byte_shift,
        int* counts
    ) {
        int tid = blockIdx.x * blockDim.x + threadIdx.x;
        int bid = blockIdx.x;
        __shared__ int local_counts[256];
        if (threadIdx.x < 256) local_counts[threadIdx.x] = 0;
        __syncthreads();
        if (tid < n) {
            unsigned char byte = (unsigned char)((keys[tid] >> byte_shift) & 0xFF);
            atomicAdd(&local_counts[byte], 1);
        }
        __syncthreads();
        if (threadIdx.x < 256)
            counts[bid * 256 + threadIdx.x] = local_counts[threadIdx.x];
    }
    ''', 'radix_count')
    return _radix_sort_kernel


def sort_timeline_by_timestamp_gpu(timeline_events: list) -> tuple:
    """Sort timeline events by 'ts' using GPU."""
    import cupy as cp

    n = len(timeline_events)
    if n == 0:
        return timeline_events, "GPU Radix Sort (empty)"

    ts_ns = cp.array([ev["ts"].value for ev in timeline_events], dtype=cp.int64)

    if n < 100_000:
        sorted_indices_cpu = cp.argsort(ts_ns, stable=True).get()
        sorted_events = [timeline_events[int(i)] for i in sorted_indices_cpu]
        return sorted_events, "GPU CuPy argsort"

    original_indices = cp.arange(n, dtype=cp.int64)
    sorted_indices_cpu = cp.lexsort(cp.array([original_indices, ts_ns])).get()
    sorted_events = [timeline_events[int(i)] for i in sorted_indices_cpu]
    return sorted_events, "GPU Radix Sort (CuPy Thrust)"


def sort_timeline_by_timestamp_cpu(timeline_events: list) -> tuple:
    """Sort timeline events by 'ts' using CPU."""
    import numpy as np

    n = len(timeline_events)
    if n == 0:
        return timeline_events, "CPU NumPy argsort (empty)"

    ts_ns = np.fromiter((ev["ts"].value for ev in timeline_events), dtype=np.int64, count=n)
    sorted_indices = np.argsort(ts_ns, kind="stable")
    sorted_events = [timeline_events[int(i)] for i in sorted_indices]
    return sorted_events, "CPU NumPy argsort"


def precompute_frame_plan_gpu(n_bricks: int, speed: float, frame_rate: int = 20) -> tuple:
    """Precompute frame plan using GPU."""
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
    """Precompute frame plan using CPU."""
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
    """Recompute frame plan from current position."""
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
            pass

    offsets = np.arange(0, remaining, bricks_per_frame, dtype=np.int32) + current_index
    ends    = np.minimum(offsets + bricks_per_frame, n_bricks)
    frame_plan = list(zip(offsets.tolist(), ends.tolist()))
    label = f"CPU NumPy arange ({len(frame_plan)} frames from idx {current_index})"
    return frame_plan, 0, label
