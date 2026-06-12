# 🏗️ RenkoTerminal Architecture

This document describes the high-performance system design, data flows, parallel engines, and low-RAM caching strategies of **RenkoTerminal**.

---

## ── Architecture Overview ──

```
   Tick CSV File (Polars Chunk Scanning)
                 │
                 ▼ (250,000 tick chunks)
         [utils/csv_stream.py]
                 │
                 ▼ (Parallel Thread Pool Mapping)
     ┌───────────┼───────────┬───────────┐
     ▼           ▼           ▼           ▼
  Engine 1    Engine 2    Engine 3    Engine 4
 (1.0 Pip)   (2.0 Pip)   (3.0 Pip)   (4.0 Pip)
     └───────────┼───────────┬───────────┘
                 ▼ (Confirmed Bricks)
         [services/parquet_cache.py]
                 │
                 ├─► incremental write batch (50k limit)
                 ├─► Parquet Disk Files (<job_id>/renko_*.parquet)
                 ▼
          Browser client (Lazy scrolling windows)
```

---

## ── Core Components ──

### 1. Data Streaming Engine (`utils/csv_stream.py`)
- Reads the input CSV file in lightweight chunks (default `250,000` rows) rather than parsing the full file into memory.
- Uses **Polars** as the primary parser (releasing the GIL for fast parallel tokenization), with **DuckDB** and **pandas** as robust fallbacks.
- Memory footprint is bound to a single chunk size (typically **15–20 MB** max) regardless of whether the CSV is 10 MB or 10 GB.

### 2. Multi-Threaded JIT Calculators (`services/renko_state.py`)
- Employs **Numba JIT compilation** (`nogil=True`) for high-speed calculation loops.
- Maps chunk tick calculations across a thread pool using Python's `ThreadPoolExecutor`, so all 4 chart engines (C1, C2, C3, C4) calculate concurrent outputs across separate CPU cores.
- Maintains only 7 scalar values to track forming brick state, meaning no historical data arrays accumulate in RAM.

### 3. Incremental Caching (`services/parquet_cache.py`)
- Saves confirmed Renko bricks incrementally to disk as columnar **Apache Parquet** files using **PyArrow**.
- Flushes buffers in batches of `50,000` bricks to prevent RAM buildup.
- Exposes `read_window()` and `read_last_n_bricks()` to let the frontend lazy-load bricks dynamically as the user scrolls, conserving network bandwidth and client memory.

### 4. GPU Acceleration (`services/gpu_engine.py`)
- Integrates **CuPy** and **cuDF** for hardware-accelerated numeric calculations.
- Utilizes custom CUDA kernels to calculate multiple Renko chart paths simultaneously on the GPU.
- Performs argsort and timeline sorting on GPU VRAM, minimizing host-to-device memory copy overhead.
