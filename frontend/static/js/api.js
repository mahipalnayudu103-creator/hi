/**
 * api.js — RenkoAPI client
 *
 * Wraps all backend endpoints:
 *  - /api/metadata            (CSV summary)
 *  - /api/build-renko         (legacy sync build)
 *  - /api/jobs/build-renko    (background job — returns job_id)
 *  - /api/jobs/{id}/status    (poll progress + system stats)
 *  - /api/jobs/{id}/result    (fetch final bricks when done)
 *  - /api/jobs/{id}/window    (lazy viewport slice)
 *  - /api/system-stats        (live CPU/GPU/RAM)
 *  - /api/engine-status       (library availability matrix)
 *  - WS /ws/jobs/{id}         (live job progress stream)
 *  - WS /ws/playback          (live tick playback stream)
 */

"use strict";

const API_BASE = window.location.origin;

// ─── MsgPack binary decode ────────────────────────────────────────────────────
// Uses @msgpack/msgpack if loaded, else falls back to JSON.parse
function _decode(data) {
    if (data instanceof ArrayBuffer || data instanceof Uint8Array) {
        if (window.MessagePack) {
            return window.MessagePack.decode(data instanceof ArrayBuffer ? new Uint8Array(data) : data);
        }
        // Fallback: try to parse as UTF-8 JSON
        return JSON.parse(new TextDecoder().decode(data));
    }
    try { return JSON.parse(data); } catch { return {}; }
}

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

// ─── WebSocket message handler (binary-aware) ─────────────────────────────────
function _wsOnMessage(e, handlers) {
    let msg;
    try {
        msg = _decode(e.data);
    } catch { return; }

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
        case "playback_frame":
            handlers.onFrame?.(msg);
            break;
        case "status":
            handlers.onStatus?.(msg);
            break;
        default:
            handlers.onLog?.(`[${msg.type}] ${JSON.stringify(msg)}`);
    }
}

// ─── Public API ───────────────────────────────────────────────────────────────

const RenkoAPI = {

    // ── CSV metadata ──────────────────────────────────────────────────────────
    async getMetadata(csvPath) {
        return _post("/api/metadata", { csv_path: csvPath });
    },

    // ── Cache lookup ──────────────────────────────────────────────────────────
    async lookupCache(params) {
        return _post("/api/cache/lookup", params);
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
    // maxBricks caps how many bricks come back so a multi-year / 1-pip build can't
    // return millions of points and OOM-crash the browser tab. The backend returns the
    // most-recent N (and LTTB-downsamples), and panning to older data uses getWindow().
    async getJobResult(jobId, startUtc = "", endUtc = "", maxBricks = 20000) {
        let url = `/api/jobs/${jobId}/result`;
        const params = [];
        if (startUtc) params.push(`start_utc=${encodeURIComponent(startUtc)}`);
        if (endUtc)   params.push(`end_utc=${encodeURIComponent(endUtc)}`);
        if (maxBricks) params.push(`max_bricks=${maxBricks}`);
        if (params.length > 0) url += "?" + params.join("&");
        return _get(url);
    },

    // ── Lazy viewport window (predicate-pushdown Parquet) ─────────────────────
    async getWindow(jobId, pip, startTime = 0, endTime = 0, maxBricks = 10000) {
        let url = `/api/jobs/${jobId}/window?pip=${encodeURIComponent(pip)}`;
        if (startTime > 0) url += `&start_time=${startTime}`;
        if (endTime > 0)   url += `&end_time=${endTime}`;
        url += `&max_bricks=${maxBricks}`;
        return _get(url);
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
    connectJobWS(jobId, handlers = {}) {
        const proto = location.protocol === "https:" ? "wss:" : "ws:";
        const ws = new WebSocket(`${proto}//${location.host}/ws/jobs/${jobId}`);
        ws.binaryType = "arraybuffer";  // receive binary MsgPack frames
        ws.onmessage = (e) => _wsOnMessage(e, handlers);
        ws.onerror   = (e) => handlers.onError?.("WebSocket error", e);
        ws.onclose   = ()  => handlers.onClose?.();
        return ws;
    },
};

window.RenkoAPI = RenkoAPI;
window._wsDecode = _decode;   // expose for playback.worker.js
