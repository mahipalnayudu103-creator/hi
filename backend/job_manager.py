"""
job_manager.py — Background Renko build job tracking.

Each job:
  - Has a UUID
  - Runs in background via asyncio.create_task
  - Streams progress to WebSocket subscribers via asyncio.Queue
  - Does NOT store raw tick data
  - Stores only brick counts (not brick data — that lives in Parquet)
  - Supports cancellation via cancel_requested flag
"""

import asyncio
import time
import uuid
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("renko_playback.jobs")

_SENTINEL = object()   # signals end-of-stream to subscribers


@dataclass
class RenkoJob:
    job_id: str

    # Publicly readable state
    status:             str   = "pending"  # pending|running|done|error|cancelled
    progress_percent:   float = 0.0
    rows_scanned:       int   = 0
    ticks_used:         int   = 0
    bricks_built:       Dict[str, int] = field(default_factory=dict)
    cpu_percent:        Optional[float] = None
    gpu_percent:        Optional[float] = None
    ram_used_gb:        Optional[float] = None
    ram_used_mb:        Optional[float] = None
    gpu_vram_used_gb:   Optional[float] = None
    engine_used:        str  = ""
    diagnostics:        List[str] = field(default_factory=list)
    error_message:      str  = ""
    current_tick_time:  str  = ""
    chunk_count:        int  = 0
    stage:              str  = "pending"

    # Cancellation flag — pipeline checks this each chunk
    cancel_requested:   bool = False

    # Results — no raw ticks stored here; bricks are in Parquet
    # result_charts holds only the brick counts, not the brick data
    result_charts:      Dict[str, int] = field(default_factory=dict)

    created_at:    float           = field(default_factory=time.time)
    completed_at:  Optional[float] = None

    # Private — WS broadcast queue (each subscriber gets its own)
    _subscribers: List[asyncio.Queue] = field(default_factory=list, repr=False)

    # ── Cancellation ──────────────────────────────────────────────────────────

    def cancel(self) -> None:
        """Request graceful cancellation of the build pipeline."""
        if self.status in ("pending", "running"):
            self.cancel_requested = True
            self.status = "cancelled"
            self.completed_at = time.time()
            self.broadcast({"type": "cancelled", **self._base_status()})
            self.broadcast_done()
            logger.info(f"[Job {self.job_id[:8]}] Cancellation requested.")

    # ── Subscriber management ─────────────────────────────────────────────────

    def add_subscriber(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subscribers.append(q)
        return q

    def remove_subscriber(self, q: asyncio.Queue):
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    def broadcast(self, payload: Dict[str, Any]):
        for q in list(self._subscribers):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                pass  # slow subscriber — drop frame

    def broadcast_done(self):
        for q in list(self._subscribers):
            try:
                q.put_nowait(_SENTINEL)
            except asyncio.QueueFull:
                pass

    # ── Progress helpers ──────────────────────────────────────────────────────

    def log(self, message: str):
        self.diagnostics.append(message)
        logger.info(f"[Job {self.job_id[:8]}] {message}")
        self.broadcast({"type": "log", "message": message, **self._base_status()})

    def update_progress(
        self,
        percent:          float,
        rows_scanned:     int = 0,
        ticks_used:       int = 0,
        bricks_built:     Optional[Dict[str, int]] = None,
        cpu_percent:      Optional[float] = None,
        gpu_percent:      Optional[float] = None,
        ram_used_gb:      Optional[float] = None,
        ram_used_mb:      Optional[float] = None,
        gpu_vram_used_gb: Optional[float] = None,
        current_tick_time: str = "",
        chunk_count:      int = 0,
        stage:            str = "",
    ):
        self.progress_percent = min(100.0, percent)
        if rows_scanned:       self.rows_scanned    = rows_scanned
        if ticks_used:         self.ticks_used      = ticks_used
        if bricks_built:       self.bricks_built    = bricks_built
        if cpu_percent      is not None: self.cpu_percent      = cpu_percent
        if gpu_percent      is not None: self.gpu_percent      = gpu_percent
        if ram_used_gb      is not None: self.ram_used_gb      = ram_used_gb
        if ram_used_mb      is not None: self.ram_used_mb      = ram_used_mb
        if gpu_vram_used_gb is not None: self.gpu_vram_used_gb = gpu_vram_used_gb
        if current_tick_time: self.current_tick_time = current_tick_time
        if chunk_count:       self.chunk_count      = chunk_count
        if stage:             self.stage            = stage
        self.broadcast({"type": "progress", **self.to_status_dict()})

    def set_done(self, brick_counts: Dict[str, int], engine_used: str):
        """Mark job done. Only brick counts stored — raw bricks live in Parquet."""
        self.status         = "done"
        self.result_charts  = brick_counts
        self.bricks_built   = brick_counts
        self.engine_used    = engine_used
        self.progress_percent = 100.0
        self.completed_at   = time.time()
        self.stage          = "done"
        payload = {"type": "done", **self.to_status_dict()}
        self.broadcast(payload)
        self.broadcast_done()

    def set_error(self, message: str):
        self.status        = "error"
        self.error_message = message
        self.completed_at  = time.time()
        self.stage         = "error"
        self.broadcast({"type": "error", "error_message": message, **self._base_status()})
        self.broadcast_done()

    # ── Serialisation ─────────────────────────────────────────────────────────

    def _base_status(self) -> Dict[str, Any]:
        return {"job_id": self.job_id, "status": self.status}

    def to_status_dict(self) -> Dict[str, Any]:
        return {
            "job_id":             self.job_id,
            "status":             self.status,
            "stage":              self.stage,
            "progress_percent":   round(self.progress_percent, 2),
            "rows_scanned":       self.rows_scanned,
            "ticks_used":         self.ticks_used,
            "bricks_built":       self.bricks_built,
            "cpu_percent":        self.cpu_percent,
            "gpu_percent":        self.gpu_percent,
            "ram_used_gb":        self.ram_used_gb,
            "ram_used_mb":        self.ram_used_mb,
            "gpu_vram_used_gb":   self.gpu_vram_used_gb,
            "engine_used":        self.engine_used,
            "current_tick_time":  self.current_tick_time,
            "chunk_count":        self.chunk_count,
            "diagnostics":        self.diagnostics[-80:],
            "error_message":      self.error_message,
        }


# ─────────────────────────────────────────────────────────────────────────────

class JobManager:
    """Global singleton that stores and serves all Renko jobs."""

    def __init__(self):
        self._jobs: Dict[str, RenkoJob] = {}

    def create_job(self) -> RenkoJob:
        job_id = str(uuid.uuid4())
        job    = RenkoJob(job_id=job_id)
        self._jobs[job_id] = job
        logger.info(f"Created job {job_id}")
        return job

    def get_job(self, job_id: str) -> Optional[RenkoJob]:
        return self._jobs.get(job_id)

    def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job:
            job.cancel()
            return True
        return False

    def list_jobs(self) -> List[Dict[str, Any]]:
        return [
            {
                "job_id":           jid,
                "status":           j.status,
                "progress_percent": j.progress_percent,
                "created_at":       j.created_at,
                "stage":            j.stage,
            }
            for jid, j in self._jobs.items()
        ]

    def cleanup_old(self, max_age_s: int = 7200):
        now = time.time()
        stale = [
            jid for jid, j in self._jobs.items()
            if j.status in ("done", "error", "cancelled")
            and j.completed_at and (now - j.completed_at) > max_age_s
        ]
        for jid in stale:
            del self._jobs[jid]
        if stale:
            logger.info(f"Cleaned {len(stale)} old jobs.")


# Singleton
job_manager = JobManager()
SENTINEL = _SENTINEL
