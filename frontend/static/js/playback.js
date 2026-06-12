// ─── Web Worker + RAF ─────────────────────────────────────────────────────────
let _worker      = null;
let _workerReady = false;
let _rafId       = null;
let _pendingFrame = null;

function initWorker() {
    if (_worker) { _worker.terminate(); _worker = null; }
    _worker = new Worker("/static/js/playback.worker.js?v=30");
    _workerReady = false;

    _worker.onmessage = (e) => {
        const msg = e.data;
        switch (msg.type) {
            case "timeline_ready":
                _workerReady = true;
                console.log(`[Worker] Tick playback initialized. Ticks: ${msg.total_ticks.toLocaleString()}`);
                _worker.postMessage({ type: "start" });
                break;
            case "frame":
                _pendingFrame = msg;
                if (!_rafId) scheduleRAF();
                statusText.textContent = `Playback: ${Number(msg.speed).toLocaleString()}× market time | Processed ticks: ${Number(msg.processed_ticks).toLocaleString()} / ${Number(msg.total_ticks).toLocaleString()} | Formed bricks: ${Number(msg.formed_bricks).toLocaleString()}`;
                statusText.className = "value success";
                updatePlaybackMetrics(msg);
                updateDebugPanel({ currentTickTime: msg.latest_time });
                break;
            case "ended":
                isPlaying = false;
                if (_rafId) { cancelAnimationFrame(_rafId); _rafId = null; }
                _pendingFrame = null;
                statusText.textContent = `Playback Ended (Processed ${Number(msg.total_ticks).toLocaleString()} ticks)`;
                statusText.className = "value success";
                updatePlaybackMetrics({
                    processed_ticks: msg.total_ticks,
                    total_ticks: msg.total_ticks,
                    formed_bricks: msg.formed_bricks,
                    speed: 0
                });
                break;
            case "status":
                if (msg.message === "clear") {
                    charts.forEach(c => c.clear());
                }
                console.log("[Worker]", msg.message);
                break;
        }
    };
    _worker.onerror = (err) => { console.error("[Worker] Error:", err.message); isPlaying = false; };
}

const _WORKER_FRAME_MS = 1000 / 60;
function scheduleRAF() {
    _rafId = requestAnimationFrame(() => {
        _rafId = null;
        if (_pendingFrame) {
            const frame = _pendingFrame;
            _pendingFrame = null;

            // 1. Confirmed bricks
            for (const [chartIdxStr, bricks] of Object.entries(frame.bricks_by_chart || {})) {
                const idx = parseInt(chartIdxStr, 10) - 1;
                if (charts[idx]) for (const brick of bricks) charts[idx].appendBrick(brick);
            }

            // 2. Live bar initial state
            if (frame.live_bricks_by_chart) {
                for (const [chartIdxStr, liveBrick] of Object.entries(frame.live_bricks_by_chart)) {
                    const idx = parseInt(chartIdxStr, 10) - 1;
                    if (charts[idx] && liveBrick) charts[idx].appendBrick(liveBrick);
                }
            }

            // 3. Tick-by-tick live bar animation
            if (frame.tick_prices && frame.tick_prices.length > 1) {
                charts.forEach(c => { if (c) c.animateTickPrices(frame.tick_prices, _WORKER_FRAME_MS); });
            }
        }
        if (isPlaying && _worker) scheduleRAF();
    });
}

// ─── Playback ─────────────────────────────────────────────────────────────────
function startPlayback() {
    if (isPlaying) return;

    if (stagedCache) {
        loadStagedCacheAndPlay();
        return;
    }

    if (cachePlaybackData) {
        startCachePlayback();
        return;
    }

    // Auto-build and play if no cache/ticks are loaded yet
    if (!stagedCache && !cachePlaybackData) {
        const mode = playbackMode.value;
        if (mode === "local" || (!currentTicksData || currentTicksData.length === 0)) {
            console.log("[PLAYBACK] No cache staged or ticks available. Auto-building charts first...");
            autoPlayAfterBuild = true;
            runBuildCharts();
            return;
        }
    }

    if (!selectedStartUtc || !selectedEndUtc) {
        statusText.textContent = "Select date range first.";
        statusText.className = "value error";
        return;
    }

    if (new Date(selectedStartUtc) >= new Date(selectedEndUtc)) {
        statusText.textContent = "Invalid range: start must be before end.";
        statusText.className = "value error";
        return;
    }

    const mode = playbackMode.value;

    if (mode === "local") {
        if (!currentTicksData || currentTicksData.length === 0) {
            statusText.textContent = "Build charts first (no ticks available).";
            statusText.className = "value error";
            return;
        }

        if (_worker && _workerReady) {
            isPlaying = true;
            statusText.textContent = "Resuming playback...";
            statusText.className = "value success";
            _worker.postMessage({ type: "resume" });
            if (!_rafId) scheduleRAF();
            return;
        }

        const chartPips = getCurrentPips();

        // Only clear charts on first play, not when resuming from pause
        if (!playbackInitialized) {
            charts.forEach(c => c.clear());
            playbackInitialized = true;
        }

        isPlaying = true;
        statusText.textContent = "Initializing playback in worker…";
        statusText.className = "value success";
        initWorker();
        _worker.postMessage({
            type: "init_playback",
            ticks: currentTicksData,
            chart_pips: chartPips,
            reversal_boxes: parseInt(reversalBoxes.value),
            pip_size: parseFloat(pipSize.value),
            anchor: anchorModeSelect.value,
            speed: getSelectedSpeed(),
            speed_mode: "time"
        });
        scheduleRAF();

    } else if (mode === "websocket") {
        if (ws?.readyState === WebSocket.OPEN) {
            isPlaying = true;
            statusText.textContent = "Resuming WS playback...";
            statusText.className = "value success";
            ws.send(JSON.stringify({ action: "resume" }));
            return;
        }

        const proto  = location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl  = `${proto}//${location.host}/ws/playback`;
        statusText.textContent = "Connecting playback WS…";
        statusText.className = "value warning";
        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            isPlaying = true;
            charts.forEach(c => c.clear());
            const chartPips = getCurrentPips();
            ws.send(JSON.stringify({
                action: "start", csv_path: csvPathInput.value.trim(),
                start_utc: selectedStartUtc, end_utc: selectedEndUtc,
                price_source: priceSourceSelect.value,
                reversal_boxes: parseInt(reversalBoxes.value),
                pip_size: parseFloat(pipSize.value),
                anchor: anchorModeSelect.value,
                chart_pips: chartPips,
                processing_engine: processingEngineSelect.value,
                speed: getSelectedSpeed(),
                speed_mode: "time",
            }));
            statusText.textContent = "WS Playback connecting…";
            statusText.className = "value success";

            updateDebugPanel({
                backendReqStart: selectedStartUtc,
                backendReqEnd: selectedEndUtc
            });
        };

        // RAF coalescing for server-mode playback frames
        let _wsPendingFrame = null;
        let _wsRafId = null;
        const _WS_FRAME_MS = 1000 / 60; // matches backend 60 FPS target
        function _wsScheduleRAF() {
            if (_wsRafId) return;
            _wsRafId = requestAnimationFrame(() => {
                _wsRafId = null;
                const frame = _wsPendingFrame;
                _wsPendingFrame = null;
                if (!frame) return;

                // 1. Render confirmed bricks
                for (const [chartIdxStr, bricks] of Object.entries(frame.bricks_by_chart || {})) {
                    const idx = parseInt(chartIdxStr, 10) - 1;
                    if (charts[idx]) for (const brick of bricks) charts[idx].appendBrick(brick);
                }

                // 2. Set live bar initial state
                if (frame.live_bricks_by_chart) {
                    for (const [chartIdxStr, liveBrick] of Object.entries(frame.live_bricks_by_chart)) {
                        const idx = parseInt(chartIdxStr, 10) - 1;
                        if (charts[idx] && liveBrick) charts[idx].appendBrick(liveBrick);
                    }
                }

                // 3. Animate the forming bar tick-by-tick within this frame's time window
                if (frame.tick_prices && frame.tick_prices.length > 1) {
                    // Split tick_prices evenly across charts (same price stream, all charts see same ticks)
                    charts.forEach(c => {
                        if (c) c.animateTickPrices(frame.tick_prices, _WS_FRAME_MS);
                    });
                }

                statusText.textContent = `Playback: ${Number(frame.speed).toLocaleString()}× | Ticks: ${Number(frame.processed_ticks).toLocaleString()} / ${Number(frame.total_ticks).toLocaleString()} | Bricks: ${Number(frame.formed_bricks).toLocaleString()}`;
                statusText.className = "value success";
                updatePlaybackMetrics(frame);
                updateDebugPanel({ currentTickTime: frame.latest_time });
            });
        }

        ws.binaryType = "arraybuffer";
        ws.onmessage = (e) => {
            const msg = window._wsDecode ? window._wsDecode(e.data) : JSON.parse(e.data);
            if (msg.type === "playback_frame") {
                _wsPendingFrame = msg;
                _wsScheduleRAF();
            } else if (msg.type === "status") {
                if (msg.status === "ready") {
                    updateDebugPanel({
                        loadedFirstTick: msg.first_tick,
                        loadedLastTick: msg.last_tick,
                        loadedTickCount: msg.ticks_loaded,
                        backendReqStart: selectedStartUtc,
                        backendReqEnd: selectedEndUtc
                    });
                }
                if (msg.status === "ended") {
                    isPlaying = false;
                    statusText.textContent = `WS Playback Ended (Processed ${Number(msg.total_ticks || 0).toLocaleString()} ticks)`;
                    ws.close();
                    updatePlaybackMetrics({
                        processed_ticks: msg.total_ticks || 0,
                        total_ticks: msg.total_ticks || 0,
                        formed_bricks: msg.formed_bricks || 0,
                        speed: 0
                    });
                }
                else if (msg.status === "loading") { statusText.textContent = msg.message || "Loading…"; }
                else if (msg.status === "paused") { isPlaying = false; statusText.textContent = "Paused"; }
                else if (msg.status === "reset")  { charts.forEach(c => c.clear()); }
                if (msg.diagnostics) msg.diagnostics.forEach(d => console.log("[WS]", d));
            } else if (msg.type === "error") {
                isPlaying = false;
                console.error("WS Error:", msg.message);
                statusText.textContent = `WS Error: ${msg.message}`;
                ws.close();
            }
        };

        ws.onerror  = () => { isPlaying = false; statusText.textContent = "WS Error"; };
        ws.onclose  = () => { isPlaying = false; };
    }
}

function pausePlayback() {
    isPlaying = false;
    if (_rafId) { cancelAnimationFrame(_rafId); _rafId = null; }
    if (cachePlaybackTimer) { cancelAnimationFrame(cachePlaybackTimer); cachePlaybackTimer = null; }
    if (_worker && _workerReady) _worker.postMessage({ type: "pause" });
    if (playbackMode.value === "websocket" && ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "pause" }));
    } else {
        statusText.textContent = "Paused";
        statusText.className = "value warning";
    }
}

function stepPlayback() {
    if (stagedCache) {
        loadStagedCacheAndStep();
        return;
    }
    if (cachePlaybackData) {
        stepCachePlayback();
        return;
    }
    if (playbackMode.value === "local") {
        if (_worker && _workerReady) {
            _worker.postMessage({ type: "step" });
        } else if (currentTicksData) {
            // Start worker if not initialized
            startPlayback();
            setTimeout(() => { if (_worker) _worker.postMessage({ type: "pause" }); }, 200);
        }
    } else if (ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "step" }));
    }
}

function resetPlayback() {
    isPlaying = false;
    playbackInitialized = false;
    if (_rafId) { cancelAnimationFrame(_rafId); _rafId = null; }
    if (cachePlaybackTimer) { cancelAnimationFrame(cachePlaybackTimer); cachePlaybackTimer = null; }
    charts.forEach(c => c.clear());
    lastConfirmedBricks = [];
    
    if (stagedCache) {
        stagedCache = null;
        statusText.textContent = "Staged cache reset. Charts cleared.";
        statusText.className = "value warning";
        ticksLoadedText.textContent = "—";
        bricksBuiltText.textContent = "—";
        dataEngineStatus.textContent = "—";
        return;
    }
    
    if (cachePlaybackData && cachePlaybackSequence.length > 0) {
        cachePlaybackIndex = 0;
        cachePlaybackTime = cachePlaybackSequence[0].timestamp;
        statusText.textContent = "Cache playback reset. Ready.";
        statusText.className = "value warning";
        updatePlaybackMetrics({
            processed_ticks: 0,
            total_ticks: cachePlaybackSequence.length,
            formed_bricks: 0,
            speed: 0
        });
        return;
    }
    
    if (_worker) { _worker.postMessage({ type: "reset" }); }
    if (playbackMode.value === "websocket" && ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "reset" }));
    } else {
        statusText.textContent = "Reset. Charts Cleared.";
        statusText.className = "value warning";
    }
}

function stopPlayback() {
    isPlaying = false;
    playbackInitialized = false;
    if (_rafId) { cancelAnimationFrame(_rafId); _rafId = null; }
    if (cachePlaybackTimer) { cancelAnimationFrame(cachePlaybackTimer); cachePlaybackTimer = null; }
    cachePlaybackData = null;
    cachePlaybackSequence = [];
    cachePlaybackIndex = 0;
    cachePlaybackTime = null;
    stagedCache = null;

    if (_worker) { _worker.postMessage({ type: "stop" }); _worker = null; }
    if (ws) { try { ws.close(); } catch {} ws = null; }
}

// ─── Speed handler ────────────────────────────────────────────────────────────
function onSpeedInput(e) {
    const val = getSelectedSpeed();
    if (playbackSpeedValue) playbackSpeedValue.textContent = formatMultiplier(val);
    if (playbackMode.value === "websocket" && ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "speed", speed: val, mode: "time" }));
    } else if (_worker && _workerReady) {
        _worker.postMessage({ type: "speed", speed: val, mode: "time" });
    }
}

function handleStepMulti(count, direction) {
    const mode = playbackMode.value;
    if (mode === "local") {
        if (_worker && _workerReady) {
            _worker.postMessage({ type: "step_multi", count, direction });
        } else {
            console.warn("Worker not ready or playback not started.");
        }
    } else if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "step_multi", count, direction }));
    } else {
        console.warn("WebSocket not connected.");
    }
}

function handleJump(targetIndex) {
    const mode = playbackMode.value;
    if (mode === "local") {
        if (_worker && _workerReady) {
            _worker.postMessage({ type: "skip_to", index: targetIndex });
        }
    } else if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "skip_to", index: targetIndex }));
    }
}

function findClosestTickIndex(timeStr) {
    if (!currentTicksData || currentTicksData.length === 0) return -1;
    let targetMs;
    try {
        let cleanStr = timeStr.trim();
        if (!cleanStr.endsWith("Z") && !cleanStr.includes("+") && !cleanStr.includes("-")) {
            cleanStr += "Z";
        }
        targetMs = Date.parse(cleanStr);
    } catch (e) {
        return -1;
    }
    if (isNaN(targetMs)) return -1;

    let low = 0;
    let high = currentTicksData.length - 1;
    const startMs = new Date(currentTicksData[low].time).getTime();
    const endMs = new Date(currentTicksData[high].time).getTime();
    if (targetMs <= startMs) return 0;
    if (targetMs >= endMs) return high;

    while (low < high - 1) {
        const mid = (low + high) >> 1;
        const midMs = new Date(currentTicksData[mid].time).getTime();
        if (midMs === targetMs) return mid;
        if (midMs < targetMs) low = mid;
        else high = mid;
    }
    const diffLow = Math.abs(new Date(currentTicksData[low].time).getTime() - targetMs);
    const diffHigh = Math.abs(new Date(currentTicksData[high].time).getTime() - targetMs);
    return diffLow <= diffHigh ? low : high;
}

function jumpToTimeStr(timeStr) {
    if (!timeStr) return;
    const index = findClosestTickIndex(timeStr);
    if (index !== -1) {
        console.log(`[JUMP] Resolved "${timeStr}" to tick index ${index}`);
        handleJump(index);
    } else {
        alert("Could not resolve time. Please ensure format is YYYY-MM-DD HH:MM:SS.");
    }
}

