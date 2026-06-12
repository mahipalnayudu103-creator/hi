# ⚡ RenkoTerminal

**RenkoTerminal** is a high-performance tick playback dashboard designed to process millions of market ticks, calculate Renko bricks concurrently using CPU/GPU capabilities, and display the result on TradingView Lightweight Charts in real-time.

---

## 🚀 Key Features
- **Zero-RAM Footprint Build**: Streams ticks from CSV files in chunks using Polars/DuckDB, keeping RAM flat (~15-20 MB) regardless of CSV file size.
- **Concurrent Processing**: Parallelizes Renko calculations across CPU cores via Numba JIT (releasing the GIL), or speeds up calculation with CuPy on NVIDIA GPUs.
- **Lazy Parquet Caching**: Automatically caches Renko bricks as columnar Parquet files on disk and streams windowed subsets to the client upon scroll.
- **Fast playback**: Interactive playback loop runs on WebSockets or client-side Web Workers at 20 frames per second.

---

## 🛠️ Tech Stack
- **Backend**: FastAPI, Uvicorn, Polars, DuckDB, NumPy, Numba JIT, PyArrow, CuPy.
- **Frontend**: HTML5, Vanilla CSS, Vanilla JS, TradingView Lightweight Charts.

---

## 📂 Project Organization
Please refer to [PROJECT_MAP.md](PROJECT_MAP.md) for a complete overview of the directory structure and file functions.

---

## 🏃‍♂️ Getting Started
Please refer to [RUN_LOCAL.md](RUN_LOCAL.md) for step-by-step setup and startup commands.
