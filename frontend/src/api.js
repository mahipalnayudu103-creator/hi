/**
 * api.js — RenkoAPI client
 *
 * Wraps all backend endpoints:
 *  - /api/metadata            (CSV summary)
 *  - /api/build-renko         (legacy sync build)
 *  - /api/jobs/build-renko    (background job — returns job_id)
 *  - /api/jobs/{id}/status    (poll progress + system stats)
 *  - /api/jobs/{id}/result    (fetch final bricks when done)
 *  - /api/system-stats        (live CPU/GPU/RAM)
 *  - /api/engine-status       (library availability matrix)
 *  - WS /ws/jobs/{id}         (live job progress stream)
 */

"use strict";

const API_BASE = window.location.origin;

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function _post(endpoint, data) {
    const res = await fetch(`${API_BASE}${endpoint}`, {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify(data),
    });
    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${res.status}`);
    }
    return res.json();
}

async function _get(endpoint) {
    const res = await fetch(`${API_BASE}${endpoint}`);
    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${res.status}`);
    }
    return res.json();
}

// ─── Public API ───────────────────────────────────────────────────────────────

const RenkoAPI = {

    // ── CSV metadata ──────────────────────────────────────────────────────────
    async getMetadata(csvPath) {
        return _post("/api/metadata", { csv_path: csvPath });
    },

    // ── Legacy sync build (kept for fallback) ─────────────────────────────────
    async buildRenko(params) {
        return _post("/api/build-renko", params);
    },

    // ── Job-based build (returns {job_id, cache_hit}) ─────────────────────────
    async submitBuildJob(params) {
        return _post("/api/jobs/build-renko", params);
    },

    // ── Poll job status (includes CPU/GPU/RAM) ────────────────────────────────
    async getJobStatus(jobId) {
        return _get(`/api/jobs/${jobId}/status`);
    },

    // ── Fetch final bricks when job.status === "done" ─────────────────────────
    async getJobResult(jobId) {
        return _get(`/api/jobs/${jobId}/result`);
    },

    // ── Live system stats (CPU/GPU/RAM) ───────────────────────────────────────
    async getSystemStats() {
        return _get("/api/system-stats");
    },

    // ── Engine status (library matrix) ───────────────────────────────────────
    async getEngineStatus() {
        return _get("/api/engine-status");
    },

    // ── WebSocket job progress stream ─────────────────────────────────────────
    /**
     * Opens a WebSocket to /ws/jobs/{jobId} and calls handlers.
     *
     * @param {string}   jobId
     * @param {Object}   handlers  { onProgress, onLog, onDone, onError, onStats, onClose }
     * @returns {WebSocket}        so caller can close early if needed
     */
    connectJobWS(jobId, handlers = {}) {
        const proto = location.protocol === "https:" ? "wss:" : "ws:";
        const ws = new WebSocket(`${proto}//${location.host}/ws/jobs/${jobId}`);

        ws.onmessage = (e) => {
            let msg;
            try { msg = JSON.parse(e.data); } catch { return; }

            switch (msg.type) {
                case "progress":
                    handlers.onProgress?.(msg);
                    break;
                case "log":
                    handlers.onLog?.(msg.message, msg);
                    break;
                case "done":
                    handlers.onDone?.(msg);
                    break;
                case "error":
                    handlers.onError?.(msg.error_message || msg.message, msg);
                    break;
                case "system_stats":
                    handlers.onStats?.(msg);
                    break;
                case "heartbeat":
                    break;
                default:
                    handlers.onLog?.(`[${msg.type}] ${JSON.stringify(msg)}`);
            }
        };

        ws.onerror = (e) => handlers.onError?.("WebSocket error", e);
        ws.onclose = () => handlers.onClose?.();
        return ws;
    },
};

window.RenkoAPI = RenkoAPI;
