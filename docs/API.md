# 🔌 RenkoTerminal API Documentation

This document lists the HTTP and WebSocket endpoints provided by the backend API router.

---

## ── HTTP API Endpoints ──

### 1. Health Check
* **Method**: `GET`
* **Path**: `/api/health`
* **Description**: Returns `{ "status": "ok" }` when the backend is active.

### 2. Engine Matrix Status
* **Method**: `GET`
* **Path**: `/api/engine-status`
* **Description**: Returns the availability matrix of Polars, DuckDB, CuPy, cuDF, PyArrow, Numba, and JSON parser versions.

### 3. Load CSV Metadata
* **Method**: `POST`
* **Path**: `/api/metadata`
* **Body**: `MetadataRequest` (JSON)
  * `csv_path`: Absolute path of file
* **Description**: Parses boundaries, estimates row counts, and auto-detects delimiter/pip sizes from the CSV header.

### 4. Direct Build Renko (Sync)
* **Method**: `POST`
* **Path**: `/api/build-renko`
* **Body**: `RenkoRequest` (JSON)
* **Description**: Reconstructs full Renko charts synchronously. Best for small files or short ranges.

### 5. Submit Async Build Job
* **Method**: `POST`
* **Path**: `/api/jobs/build-renko`
* **Body**: `JobBuildRequest` (JSON)
* **Description**: Submits a high-performance background build task. Returns a unique `job_id` immediately.

### 6. Job Status
* **Method**: `GET`
* **Path**: `/api/jobs/{job_id}/status`
* **Description**: Returns the real-time execution progress, status (running, done, error, cancelled), diagnostics, and system stats (CPU, GPU, RAM).

### 7. Lazy window scrolling
* **Method**: `GET`
* **Path**: `/api/renko-window`
* **Params**: `job_id`, `chart_id` (pip size), `from_x`, `to_x`, `max_bricks`
* **Description**: Queries a sliced range of bricks from job Parquet files on disk.

### 8. Cancel Job
* **Method**: `POST`
* **Path**: `/api/jobs/{job_id}/cancel`
* **Description**: Triggers graceful interruption of a running build job.

---

## ── WebSocket Connections ──

### 1. Live Playback Loop
* **Path**: `/ws/playback`
* **Description**: Establishes a bidirectional playback loop. Client sends playback commands (`start`, `pause`, `resume`, `step`, `skip_to`, `speed`), and backend streams tick-by-tick frames at 20 FPS.

### 2. Job Progress Stream
* **Path**: `/ws/jobs/{job_id}`
* **Description**: Streams progress updates and build logs for the background job as they occur, together with system performance snapshots every 2 seconds.
