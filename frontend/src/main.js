/**
 * main.js — RenkoTerminal Dashboard Controller
 *
 * Uses the new job-based API:
 *   POST /api/jobs/build-renko  → job_id
 *   WS   /ws/jobs/{job_id}      → live progress
 *   GET  /api/jobs/{job_id}/result → final bricks
 *
 * Local playback uses Web Worker + requestAnimationFrame (no setTimeout).
 * WebSocket playback uses the backend WS loop.
 */

"use strict";

// ─── Console redirect to UI terminal ─────────────────────────────────────────
let consoleLogOutput;

function logToUI(message, type = "info") {
    if (!consoleLogOutput) consoleLogOutput = document.getElementById("consoleLogOutput");
    if (!consoleLogOutput) return;
    const timeStr = new Date().toTimeString().slice(0, 8);
    const line = document.createElement("div");
    line.className = `log-line ${type}`;
    const t = document.createElement("span");
    t.className = "time";
    t.textContent = `[${timeStr}]`;
    line.appendChild(t);
    line.appendChild(document.createTextNode(` ${message}`));
    consoleLogOutput.appendChild(line);
    // Keep only last 500 lines to prevent DOM bloat
    while (consoleLogOutput.children.length > 500) {
        consoleLogOutput.removeChild(consoleLogOutput.firstChild);
    }
    consoleLogOutput.scrollTop = consoleLogOutput.scrollHeight;
}

function fmtArgs(args) {
    return args.map(a => typeof a === "object" && a !== null
        ? (() => { try { return JSON.stringify(a); } catch { return String(a); } })()
        : String(a)
    ).join(" ");
}

const _origLog   = console.log;
const _origWarn  = console.warn;
const _origError = console.error;
console.log   = (...a) => { _origLog.apply(console, a);   logToUI(fmtArgs(a), "info");    };
console.warn  = (...a) => { _origWarn.apply(console, a);  logToUI(fmtArgs(a), "warning"); };
console.error = (...a) => { _origError.apply(console, a); logToUI(fmtArgs(a), "error");   };

// ─── DOM references ───────────────────────────────────────────────────────────
let csvPathInput, loadMetadataBtn, rangeCard, dateRangeSlider, selectedRangeText;
let processingEngineSelect, chart1Pips, chart2Pips, chart3Pips, chart4Pips;
let priceSourceSelect, reversalBoxes, pipSize, anchorModeSelect, buildRenkoBtn, chunkSizeMb;
let playBtn, pauseBtn, stepBtn, resetBtn, playbackSpeed, playbackSpeedValue, playbackMode, syncChartsCheckbox;
let statusText, dataEngineStatus, ticksLoadedText, bricksBuiltText;
let metadataInfoBoard, metaRows, metaSize, metaStart, metaEnd, metaDelim, metaPip;
let progressSection, progressLabel, progressPct, progressFill, progressTicks, progressBricks, progressRows;
let consoleJobId;

// Monitor elements
let cpuGaugeFill, cpuPct, cpuCores;
let ramGaugeFill, ramGb, ramTotal;
let gpuGaugeFill, gpuPct, gpuName;
let vramGaugeFill, vramGb, vramTotal;
let pipelineEngine, pipeRead, pipeProc, pipeRenko, pipeCache;

// Chart instances
let chart1, chart2, chart3, chart4;
let charts = [];

// ─── App state ────────────────────────────────────────────────────────────────
let metadata        = null;
let currentBricksData = null;
let currentTicksData = null;
let isPlaying       = false;
let ws              = null;           // playback WS
let jobWS           = null;           // build job WS
let currentJobId    = null;
let statsIntervalId = null;           // polling interval for system stats
let selectedStartUtc = null;
let selectedEndUtc   = null;
let dataDirty       = false;
const SPEED_VALUES = [0.1, 0.25, 0.5, 1, 2, 5, 10, 25, 50, 100, 500, 1000, 5000, 10000, 50000, 100000];

// ─── Web Worker + RAF ─────────────────────────────────────────────────────────
let _worker      = null;
let _workerReady = false;
let _rafId       = null;
let _pendingFrame = null;

function initWorker() {
    if (_worker) { _worker.terminate(); _worker = null; }
    _worker = new Worker("/src/playback.worker.js");
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
                statusText.textContent = `Playback: ${Number(msg.speed).toLocaleString()} ticks/sec | Processed ticks: ${Number(msg.processed_ticks).toLocaleString()} / ${Number(msg.total_ticks).toLocaleString()} | Formed bricks: ${Number(msg.formed_bricks).toLocaleString()}`;
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

function scheduleRAF() {
    _rafId = requestAnimationFrame(() => {
        _rafId = null;
        if (_pendingFrame) {
            const frame = _pendingFrame;
            _pendingFrame = null;
            
            // Render formed bricks
            for (const [chartIdxStr, bricks] of Object.entries(frame.bricks_by_chart)) {
                const idx = parseInt(chartIdxStr, 10) - 1;
                if (charts[idx]) {
                    for (const brick of bricks) charts[idx].appendBrick(brick);
                }
            }
            
            // Render live/forming bricks
            if (frame.live_bricks_by_chart) {
                for (const [chartIdxStr, liveBrick] of Object.entries(frame.live_bricks_by_chart)) {
                    const idx = parseInt(chartIdxStr, 10) - 1;
                    if (charts[idx] && liveBrick) {
                        charts[idx].appendBrick(liveBrick);
                    }
                }
            }
        }
        if (isPlaying && _worker) scheduleRAF();
    });
}

// ─── Monitor helpers ──────────────────────────────────────────────────────────
function updateMonitor(stats) {
    if (!cpuPct) return;

    // CPU
    if (stats.cpu_percent != null) {
        const p = Math.round(stats.cpu_percent);
        cpuPct.textContent = `${p}%`;
        cpuGaugeFill.style.width = `${p}%`;
        cpuGaugeFill.className = `gauge-fill cpu ${p > 90 ? "hot" : p > 70 ? "warm" : ""}`;
    }
    if (stats.cpu_count != null) cpuCores.textContent = `${stats.cpu_count} cores`;

    // RAM
    if (stats.ram_used_gb != null && stats.ram_total_gb != null) {
        const pRam = Math.round((stats.ram_used_gb / stats.ram_total_gb) * 100);
        ramGb.textContent = `${stats.ram_used_gb.toFixed(1)} GB`;
        ramTotal.textContent = `${stats.ram_total_gb.toFixed(0)} GB total`;
        ramGaugeFill.style.width = `${pRam}%`;
        ramGaugeFill.className = `gauge-fill ram ${pRam > 90 ? "hot" : pRam > 70 ? "warm" : ""}`;
    }

    // GPU
    if (stats.gpu_available) {
        if (stats.gpu_percent != null) {
            const p = Math.round(stats.gpu_percent);
            gpuPct.textContent = `${p}%`;
            gpuGaugeFill.style.width = `${p}%`;
            gpuGaugeFill.className = `gauge-fill gpu ${p > 90 ? "hot" : p > 70 ? "warm" : ""}`;
        }
        if (stats.gpu_name) gpuName.textContent = stats.gpu_name.replace("NVIDIA ", "");
        if (stats.gpu_vram_used_gb != null && stats.gpu_vram_total_gb != null) {
            const pV = Math.round((stats.gpu_vram_used_gb / stats.gpu_vram_total_gb) * 100);
            vramGb.textContent = `${stats.gpu_vram_used_gb.toFixed(1)} GB`;
            vramTotal.textContent = `${stats.gpu_vram_total_gb.toFixed(0)} GB total`;
            vramGaugeFill.style.width = `${pV}%`;
        }
    } else {
        gpuPct.textContent = "N/A";
        gpuName.textContent = "No NVIDIA GPU";
        vramGb.textContent = "N/A";
    }
}

function setPipelineStage(stage) {
    const stages = { read: pipeRead, proc: pipeProc, renko: pipeRenko, cache: pipeCache };
    Object.values(stages).forEach(el => el?.classList.remove("active"));
    if (stage && stages[stage]) stages[stage].classList.add("active");
}

function updateProgress(pct, ticks, bricks, rows) {
    if (!progressSection) return;
    progressSection.classList.remove("hidden");
    const p = Math.round(pct);
    progressPct.textContent = `${p}%`;
    progressFill.style.width = `${p}%`;
    if (ticks   != null) progressTicks.textContent   = `Ticks: ${Number(ticks).toLocaleString()}`;
    if (rows    != null) progressRows.textContent     = `Rows scanned: ${Number(rows).toLocaleString()}`;
    if (bricks  != null) {
        const total = Object.values(bricks).reduce((s, v) => s + v, 0);
        progressBricks.textContent = `Bricks: ${total.toLocaleString()}`;
    }
}

// ─── System stats polling ─────────────────────────────────────────────────────
function startStatsPolling() {
    if (statsIntervalId) return;
    statsIntervalId = setInterval(async () => {
        try {
            const s = await RenkoAPI.getSystemStats();
            updateMonitor(s);
        } catch {}
    }, 2000);
}

function stopStatsPolling() {
    if (statsIntervalId) { clearInterval(statsIntervalId); statsIntervalId = null; }
}

// ─── Date helpers ─────────────────────────────────────────────────────────────
function formatDate(date) {
    const M = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    const d = String(date.getUTCDate()).padStart(2,"0");
    const m = M[date.getUTCMonth()];
    const y = date.getUTCFullYear();
    const h = String(date.getUTCHours()).padStart(2,"0");
    const mn = String(date.getUTCMinutes()).padStart(2,"0");
    const s  = String(date.getUTCSeconds()).padStart(2,"0");
    return `${d} ${m} ${y} ${h}:${mn}:${s}`;
}

function getSliderDates() {
    if (!dateRangeSlider?.noUiSlider) return { startISO: "", endISO: "" };
    const [s, e] = dateRangeSlider.noUiSlider.get().map(parseFloat);
    
    const startDate = new Date(s);
    const endDate = new Date(e);
    
    let startISO = startDate.toISOString();
    let endISO = endDate.toISOString();
    
    const isDateOnly = (startDate.getUTCHours() === 0 && startDate.getUTCMinutes() === 0 && startDate.getUTCSeconds() === 0 &&
                        endDate.getUTCHours() === 0 && endDate.getUTCMinutes() === 0 && endDate.getUTCSeconds() === 0);
    
    if (isDateOnly && s !== e) {
        // Treat end as full end day: add 24 hours to e for endISO
        const adjustedEndDate = new Date(e + 86_400_000);
        endISO = adjustedEndDate.toISOString();
    }
    
    return { startISO, endISO, startMs: s, endMs: e, isDateOnly };
}

function updateDebugPanel(info = {}) {
    const dbSelectedRange = document.getElementById("dbSelectedRange");
    const dbFeStart = document.getElementById("dbFeStart");
    const dbFeEnd = document.getElementById("dbFeEnd");
    const dbBeReqStart = document.getElementById("dbBeReqStart");
    const dbBeReqEnd = document.getElementById("dbBeReqEnd");
    const dbFirstTick = document.getElementById("dbFirstTick");
    const dbLastTick = document.getElementById("dbLastTick");
    const dbTickCount = document.getElementById("dbTickCount");
    const dbCurrentTickTime = document.getElementById("dbCurrentTickTime");

    if (dbSelectedRange) {
        if (selectedStartUtc && selectedEndUtc) {
            dbSelectedRange.textContent = `${selectedStartUtc.slice(0, 10)} .. ${selectedEndUtc.slice(0, 10)}`;
        } else {
            dbSelectedRange.textContent = "—";
        }
    }
    if (dbFeStart) dbFeStart.textContent = selectedStartUtc || "—";
    if (dbFeEnd) dbFeEnd.textContent = selectedEndUtc || "—";

    if (info.backendReqStart) {
        if (dbBeReqStart) dbBeReqStart.textContent = info.backendReqStart;
    }
    if (info.backendReqEnd) {
        if (dbBeReqEnd) dbBeReqEnd.textContent = info.backendReqEnd;
    }
    if (info.loadedFirstTick) {
        if (dbFirstTick) dbFirstTick.textContent = info.loadedFirstTick;
    }
    if (info.loadedLastTick) {
        if (dbLastTick) dbLastTick.textContent = info.loadedLastTick;
    }
    if (info.loadedTickCount !== undefined) {
        if (dbTickCount) dbTickCount.textContent = Number(info.loadedTickCount).toLocaleString();
    }
    if (info.currentTickTime) {
        if (dbCurrentTickTime) dbCurrentTickTime.textContent = info.currentTickTime;
    }
}

function handleRangeChange() {
    stopPlayback();
    currentTicksData = null;
    currentBricksData = null;
    dataDirty = true;
    charts.forEach(c => c.clear());
    if (_worker) {
        _worker.terminate();
        _worker = null;
        _workerReady = false;
    }
    statusText.textContent = "Range changed. Build Renko Charts again.";
    statusText.className = "value warning";
    
    updateDebugPanel({
        loadedFirstTick: "—",
        loadedLastTick: "—",
        loadedTickCount: 0,
        currentTickTime: "—"
    });
}

// ─── Load CSV Metadata ────────────────────────────────────────────────────────
async function onLoadMetadata() {
    const csvPath = csvPathInput.value.trim();
    if (!csvPath) { alert("Please enter a CSV file path."); return; }

    console.log("Loading metadata:", csvPath);
    statusText.textContent = "Loading CSV metadata…";
    statusText.className = "value warning";

    try {
        metadata = await RenkoAPI.getMetadata(csvPath);
        console.log("Metadata:", metadata);

        metadataInfoBoard.classList.remove("hidden");
        metaRows.textContent  = metadata.rows_estimated.toLocaleString();
        metaSize.textContent  = (metadata.size_bytes / 1_048_576).toFixed(2) + " MB";
        metaStart.textContent = formatDate(new Date(metadata.file_start_utc));
        metaEnd.textContent   = formatDate(new Date(metadata.file_end_utc));
        metaDelim.textContent = `"${metadata.delimiter}"`;
        metaPip.textContent   = metadata.detected_pip_size;
        pipSize.value = metadata.detected_pip_size;

        rangeCard.classList.remove("hidden");
        const fileStartMs = new Date(metadata.file_start_utc).getTime();
        const fileEndMs   = new Date(metadata.file_end_utc).getTime();
        const defEndMs    = Math.min(fileStartMs + 3 * 86_400_000, fileEndMs);

        if (dateRangeSlider.noUiSlider) dateRangeSlider.noUiSlider.destroy();
        noUiSlider.create(dateRangeSlider, {
            start: [fileStartMs, defEndMs],
            connect: true,
            range: { min: fileStartMs, max: fileEndMs },
            step: 1000,
        });
        
        dateRangeSlider.noUiSlider.on("update", (values) => {
            const s = parseFloat(values[0]);
            const e = parseFloat(values[1]);
            const startDate = new Date(s);
            const endDate = new Date(e);

            const isDateOnly = (startDate.getUTCHours() === 0 && startDate.getUTCMinutes() === 0 && startDate.getUTCSeconds() === 0 &&
                                endDate.getUTCHours() === 0 && endDate.getUTCMinutes() === 0 && endDate.getUTCSeconds() === 0);

            let startISO = startDate.toISOString();
            let endISO = endDate.toISOString();

            if (isDateOnly && s !== e) {
                // Treat end as full end day: add 24 hours to e for endISO
                const adjustedEndDate = new Date(e + 86400000);
                endISO = adjustedEndDate.toISOString();
            }

            selectedStartUtc = startISO;
            selectedEndUtc = endISO;

            // Format displayed text
            let displayText = "";
            if (isDateOnly) {
                // 2026-01-01 00:00:00 UTC .. 2026-01-07 23:59:59 UTC
                const startStr = startISO.replace("T", " ").replace(".000Z", " UTC");
                const displayEndDate = new Date(e + 86400000 - 1000);
                const endStr = displayEndDate.toISOString().replace("T", " ").replace(".000Z", " UTC");
                displayText = `${startStr} .. ${endStr}`;
            } else {
                const startStr = startISO.replace("T", " ").replace(".000Z", " UTC");
                const endStr = endISO.replace("T", " ").replace(".000Z", " UTC");
                displayText = `${startStr} .. ${endStr}`;
            }
            selectedRangeText.textContent = displayText;

            // Update debug panel
            updateDebugPanel();
        });

        dateRangeSlider.noUiSlider.on("change", () => {
            handleRangeChange();
        });

        statusText.textContent = "Metadata loaded. Select range and Build Renko.";
        statusText.className = "value success";
    } catch (err) {
        console.error("Metadata error:", err);
        statusText.textContent = `Error: ${err.message}`;
        statusText.className = "value warning";
    }
}

// ─── Build Renko (Job-based) ──────────────────────────────────────────────────
async function runBuildCharts() {
    if (!metadata) { console.error("Load metadata first."); return; }

    stopPlayback();

    const chartPips = [
        parseFloat(chart1Pips.value), parseFloat(chart2Pips.value),
        parseFloat(chart3Pips.value), parseFloat(chart4Pips.value),
    ];
    const buildMode = document.getElementById("buildModeSelect")?.value || "full";
    const isPreview = buildMode === "preview";
    const isCacheOnly = buildMode === "cache_only";
    const buildLabel = isPreview ? "preview build" : (isCacheOnly ? "cache-only build" : "build job");

    if (!selectedStartUtc || !selectedEndUtc) {
        statusText.textContent = "Select date range first.";
        statusText.className = "value error";
        return;
    }

    const params = {
        csv_path:          csvPathInput.value.trim(),
        start_utc:         selectedStartUtc,
        end_utc:           selectedEndUtc,
        price_source:      priceSourceSelect.value,
        reversal_boxes:    parseInt(reversalBoxes.value),
        pip_size:          parseFloat(pipSize.value),
        anchor:            anchorModeSelect.value,
        chart_pips:        chartPips,
        processing_engine: processingEngineSelect.value,
        chunk_size_mb:     parseInt(chunkSizeMb?.value || "64"),
        build_mode:        buildMode,
        preview_ticks:     50000,
        return_ticks:      !isCacheOnly,
    };

    updateDebugPanel({
        backendReqStart: selectedStartUtc,
        backendReqEnd: selectedEndUtc
    });

    console.log("Submitting build job:", params);
    statusText.textContent = `Submitting ${buildLabel}...`;
    statusText.className = "value warning";
    updateProgress(0, null, null, null);
    setPipelineStage("read");
    progressLabel.textContent = `Submitting ${buildLabel}...`;

    let jobId;
    try {
        const resp = await RenkoAPI.submitBuildJob(params);
        jobId = resp.job_id;
        currentJobId = jobId;
        if (consoleJobId) consoleJobId.textContent = `Job: ${jobId.slice(0, 8)}…`;
        console.log(`Job submitted: ${jobId} (cache_hit=${resp.cache_hit})`);

        if (resp.cache_hit) {
            if (isCacheOnly) {
                statusText.textContent = "Cache is already ready for this selection.";
                statusText.className = "value success";
                progressSection.classList.add("hidden");
                setPipelineStage(null);
                return;
            }
            // Instant cache hit — fetch result directly
            statusText.textContent = "Cache hit! Loading bricks…";
            await _loadAndRenderResult(jobId, chartPips);
            return;
        }
    } catch (err) {
        console.error("Job submit error:", err);
        statusText.textContent = `Submit error: ${err.message}`;
        statusText.className = "value warning";
        return;
    }

    // Connect to job WebSocket for live progress
    if (jobWS) { try { jobWS.close(); } catch {} }

    jobWS = RenkoAPI.connectJobWS(jobId, {
        onProgress: (msg) => {
            const pct = msg.progress_percent ?? 0;
            updateProgress(pct, msg.ticks_used, msg.bricks_built, msg.rows_scanned);
            updateMonitor(msg);
            statusText.textContent = `Building… ${pct.toFixed(1)}%`;
            progressLabel.textContent = `Building Renko… ${pct.toFixed(1)}%`;

            // Update pipeline stage indicator
            if (pct < 85)       setPipelineStage("read");
            else if (pct < 92)  setPipelineStage("proc");
            else if (pct < 98)  setPipelineStage("renko");
            else                setPipelineStage("cache");

            if (msg.engine_used) {
                pipelineEngine.textContent = msg.engine_used;
                dataEngineStatus.textContent = msg.engine_used;
            }
        },
        onLog: (message) => console.log("[Build]", message),
        onDone: async (msg) => {
            setPipelineStage("cache");
            updateProgress(100, msg.ticks_used, msg.bricks_built, msg.rows_scanned);
            progressLabel.textContent = "✅ Build complete!";
            dataEngineStatus.textContent = msg.engine_used || "Done";
            ticksLoadedText.textContent = (msg.ticks_used || 0).toLocaleString();
            const totalBricks = Object.values(msg.bricks_built || {}).reduce((s, v) => s + v, 0);
            bricksBuiltText.textContent = totalBricks.toLocaleString();
            if (isCacheOnly) {
                progressLabel.textContent = "Cache build complete.";
                statusText.textContent = `Cache ready - ${totalBricks.toLocaleString()} bricks stored`;
                statusText.className = "value success";
                progressSection.classList.add("hidden");
                setPipelineStage(null);
                return;
            }
            await _loadAndRenderResult(jobId, chartPips);
        },
        onError: (errMsg) => {
            console.error("[Build Error]", errMsg);
            statusText.textContent = `Build error: ${errMsg.slice(0, 120)}`;
            statusText.className = "value error";
            progressSection.classList.add("hidden");
            setPipelineStage(null);
        },
        onStats: (stats) => updateMonitor(stats),
        onClose: () => console.log("Job WS closed."),
    });

    statusText.textContent = `${buildLabel.charAt(0).toUpperCase()}${buildLabel.slice(1)} running...`;
    statusText.className = "value warning";
}

async function _loadAndRenderResult(jobId, chartPips) {
    try {
        statusText.textContent = "Fetching bricks…";
        const result = await RenkoAPI.getJobResult(jobId);
        currentBricksData = result.charts;
        currentTicksData = result.ticks || [];

        // Update labels
        charts.forEach((c, i) => c.setTitle(`Chart ${i + 1} (${chartPips[i]} Pip${chartPips[i] !== 1 ? "s" : ""})`));

        // Render all 4 charts
        charts.forEach((c, i) => {
            const key = String(chartPips[i]);
            c.setData(result.charts[key] || []);
        });

        const totalBricks = Object.values(result.bricks_built || {}).reduce((s, v) => s + v, 0);
        ticksLoadedText.textContent  = (result.ticks_used || 0).toLocaleString();
        bricksBuiltText.textContent  = totalBricks.toLocaleString();
        dataEngineStatus.textContent = result.engine_used || "Done";

        if (result.ticks && result.ticks.length > 0) {
            updateDebugPanel({
                loadedFirstTick: result.ticks[0].time,
                loadedLastTick: result.ticks[result.ticks.length - 1].time,
                loadedTickCount: result.ticks.length,
                backendReqStart: selectedStartUtc,
                backendReqEnd: selectedEndUtc
            });
        }

        console.log(`✅ Rendered: ${totalBricks.toLocaleString()} bricks | Engine: ${result.engine_used}`);
        result.diagnostics?.forEach(d => logToUI(`[DIAG] ${d}`, "info"));

        statusText.textContent = `Charts ready — ${totalBricks.toLocaleString()} bricks`;
        statusText.className = "value success";
        progressSection.classList.add("hidden");
        setPipelineStage(null);
    } catch (err) {
        console.error("Fetch result error:", err);
        statusText.textContent = `Result fetch error: ${err.message}`;
        statusText.className = "value warning";
    }
}

// ─── Playback ─────────────────────────────────────────────────────────────────
function startPlayback() {
    if (isPlaying) return;

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
        const chartPips = [
            parseFloat(chart1Pips.value), parseFloat(chart2Pips.value),
            parseFloat(chart3Pips.value), parseFloat(chart4Pips.value),
        ];
        charts.forEach(c => c.clear());

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
            speed: getSelectedSpeed()
        });
        scheduleRAF();

    } else if (mode === "websocket") {
        const proto  = location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl  = `${proto}//${location.host}/ws/playback`;
        statusText.textContent = "Connecting playback WS…";
        statusText.className = "value warning";
        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            isPlaying = true;
            charts.forEach(c => c.clear());
            const chartPips = [
                parseFloat(chart1Pips.value), parseFloat(chart2Pips.value),
                parseFloat(chart3Pips.value), parseFloat(chart4Pips.value),
            ];
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
            }));
            statusText.textContent = "WS Playback connecting…";
            statusText.className = "value success";

            updateDebugPanel({
                backendReqStart: selectedStartUtc,
                backendReqEnd: selectedEndUtc
            });
        };

        ws.onmessage = (e) => {
            const msg = JSON.parse(e.data);
            if (msg.type === "playback_frame") {
                // Render formed bricks
                for (const [chartIdxStr, bricks] of Object.entries(msg.bricks_by_chart)) {
                    const idx = parseInt(chartIdxStr, 10) - 1;
                    if (charts[idx]) {
                        for (const brick of bricks) charts[idx].appendBrick(brick);
                    }
                }
                // Render live/forming bricks
                if (msg.live_bricks_by_chart) {
                    for (const [chartIdxStr, liveBrick] of Object.entries(msg.live_bricks_by_chart)) {
                        const idx = parseInt(chartIdxStr, 10) - 1;
                        if (charts[idx] && liveBrick) {
                            charts[idx].appendBrick(liveBrick);
                        }
                    }
                }
                statusText.textContent = `Playback: ${Number(msg.speed).toLocaleString()} ticks/sec | Processed ticks: ${Number(msg.processed_ticks).toLocaleString()} / ${Number(msg.total_ticks).toLocaleString()} | Formed bricks: ${Number(msg.formed_bricks).toLocaleString()}`;
                statusText.className = "value success";
                updatePlaybackMetrics(msg);
                updateDebugPanel({ currentTickTime: msg.latest_time });
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
    if (_worker && _workerReady) _worker.postMessage({ type: "pause" });
    if (playbackMode.value === "websocket" && ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "pause" }));
    } else {
        statusText.textContent = "Paused";
        statusText.className = "value warning";
    }
}

function stepPlayback() {
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
    if (_rafId) { cancelAnimationFrame(_rafId); _rafId = null; }
    if (_worker) { _worker.postMessage({ type: "reset" }); }
    charts.forEach(c => c.clear());
    if (playbackMode.value === "websocket" && ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "reset" }));
    } else {
        statusText.textContent = "Reset. Charts Cleared.";
        statusText.className = "value warning";
    }
}

function stopPlayback() {
    isPlaying = false;
    if (_rafId) { cancelAnimationFrame(_rafId); _rafId = null; }
    if (_worker) { _worker.postMessage({ type: "stop" }); _worker = null; }
    if (ws) { try { ws.close(); } catch {} ws = null; }
}

// ─── Speed handler ────────────────────────────────────────────────────────────
function onSpeedInput(e) {
    const idx = parseInt(e.target.value, 10);
    const val = SPEED_VALUES[idx] !== undefined ? SPEED_VALUES[idx] : 100.0;
    if (playbackSpeedValue) playbackSpeedValue.textContent = `${val}/s`;
    if (playbackMode.value === "websocket" && ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "speed", speed: val }));
    } else if (_worker && _workerReady) {
        _worker.postMessage({ type: "speed", speed: val });
    }
}

// ─── Engine status display ────────────────────────────────────────────────────
async function loadEngineStatus() {
    try {
        const s = await RenkoAPI.getEngineStatus();
        if (dataEngineStatus) {
            dataEngineStatus.innerHTML =
                `<span title="Data: ${s.data_engine}">🗂️ ${s.data_engine}</span>` +
                ` · <span title="Calc: ${s.calc_engine}">⚡ ${s.calc_engine}</span>` +
                ` · <span title="Cache: ${s.cache_engine}">🗃️ ${s.cache_engine}</span>` +
                ` · <span title="JSON: ${s.json_engine}">🚀 ${s.json_engine}</span>`;
        }
        if (pipelineEngine) pipelineEngine.textContent = `${s.data_engine} + ${s.calc_engine}`;
        console.log("[Engine]", JSON.stringify(s.available));
        // Fetch initial stats
        const stats = await RenkoAPI.getSystemStats();
        updateMonitor(stats);
    } catch (e) {
        console.warn("Engine status fetch failed:", e.message);
    }
}

// ─── DOMContentLoaded ─────────────────────────────────────────────────────────
window.addEventListener("DOMContentLoaded", () => {
    console.log("RenkoTerminal initializing…");

    // Bind all DOM refs
    csvPathInput         = document.getElementById("csvPathInput");
    loadMetadataBtn      = document.getElementById("loadMetadataBtn");
    rangeCard            = document.getElementById("rangeCard");
    dateRangeSlider      = document.getElementById("dateRangeSlider");
    selectedRangeText    = document.getElementById("selectedRangeText");
    processingEngineSelect = document.getElementById("processingEngineSelect");

    chart1Pips           = document.getElementById("chart1Pips");
    chart2Pips           = document.getElementById("chart2Pips");
    chart3Pips           = document.getElementById("chart3Pips");
    chart4Pips           = document.getElementById("chart4Pips");
    priceSourceSelect    = document.getElementById("priceSourceSelect");
    reversalBoxes        = document.getElementById("reversalBoxes");
    pipSize              = document.getElementById("pipSize");
    anchorModeSelect     = document.getElementById("anchorModeSelect");
    buildRenkoBtn        = document.getElementById("buildRenkoBtn");
    chunkSizeMb          = document.getElementById("chunkSizeMb");

    playBtn              = document.getElementById("playBtn");
    pauseBtn             = document.getElementById("pauseBtn");
    stepBtn              = document.getElementById("stepBtn");
    resetBtn             = document.getElementById("resetBtn");
    playbackSpeed        = document.getElementById("playbackSpeed");
    playbackSpeedValue   = document.getElementById("playbackSpeedValue");
    playbackMode         = document.getElementById("playbackMode");
    syncChartsCheckbox   = document.getElementById("syncChartsCheckbox");

    statusText           = document.getElementById("statusText");
    dataEngineStatus     = document.getElementById("dataEngineStatus");
    ticksLoadedText      = document.getElementById("ticksLoadedText");
    bricksBuiltText      = document.getElementById("bricksBuiltText");

    metadataInfoBoard    = document.getElementById("metadataInfoBoard");
    metaRows             = document.getElementById("metaRows");
    metaSize             = document.getElementById("metaSize");
    metaStart            = document.getElementById("metaStart");
    metaEnd              = document.getElementById("metaEnd");
    metaDelim            = document.getElementById("metaDelim");
    metaPip              = document.getElementById("metaPip");

    progressSection      = document.getElementById("progressSection");
    progressLabel        = document.getElementById("progressLabel");
    progressPct          = document.getElementById("progressPct");
    progressFill         = document.getElementById("progressFill");
    progressTicks        = document.getElementById("progressTicks");
    progressBricks       = document.getElementById("progressBricks");
    progressRows         = document.getElementById("progressRows");
    consoleJobId         = document.getElementById("consoleJobId");

    // Monitor refs
    cpuGaugeFill         = document.getElementById("cpuGaugeFill");
    cpuPct               = document.getElementById("cpuPct");
    cpuCores             = document.getElementById("cpuCores");
    ramGaugeFill         = document.getElementById("ramGaugeFill");
    ramGb                = document.getElementById("ramGb");
    ramTotal             = document.getElementById("ramTotal");
    gpuGaugeFill         = document.getElementById("gpuGaugeFill");
    gpuPct               = document.getElementById("gpuPct");
    gpuName              = document.getElementById("gpuName");
    vramGaugeFill        = document.getElementById("vramGaugeFill");
    vramGb               = document.getElementById("vramGb");
    vramTotal            = document.getElementById("vramTotal");
    pipelineEngine       = document.getElementById("pipelineEngine");
    pipeRead             = document.getElementById("pipeRead");
    pipeProc             = document.getElementById("pipeProc");
    pipeRenko            = document.getElementById("pipeRenko");
    pipeCache            = document.getElementById("pipeCache");

    // Init charts
    chart1 = new RenkoCharts.RenkoChart("chart1Container", "chart1Legend", "chart1Title", 1);
    chart2 = new RenkoCharts.RenkoChart("chart2Container", "chart2Legend", "chart2Title", 2);
    chart3 = new RenkoCharts.RenkoChart("chart3Container", "chart3Legend", "chart3Title", 3);
    chart4 = new RenkoCharts.RenkoChart("chart4Container", "chart4Legend", "chart4Title", 4);
    charts = [chart1, chart2, chart3, chart4];
    RenkoCharts.setupSync(charts);
    RenkoCharts.setupCrosshairSync(charts);

    // Events
    loadMetadataBtn.addEventListener("click", onLoadMetadata);
    buildRenkoBtn.addEventListener("click",   runBuildCharts);
    // "Load Selected Range" button also triggers build
    document.getElementById("loadSelectedRangeBtn")?.addEventListener("click", runBuildCharts);

    playBtn.addEventListener("click",  startPlayback);
    pauseBtn.addEventListener("click", pausePlayback);
    stepBtn.addEventListener("click",  stepPlayback);
    resetBtn.addEventListener("click", resetPlayback);
    playbackSpeed.addEventListener("input", onSpeedInput);
    syncChartsCheckbox.addEventListener("change", (e) => {
        const enabled = e.target.checked;
        RenkoCharts.setSyncEnabled(enabled);
        console.log(`[SYSTEM] Chart synchronization ${enabled ? "enabled" : "disabled"}.`);
    });

    document.getElementById("clearConsoleBtn")?.addEventListener("click", () => {
        if (consoleLogOutput) {
            consoleLogOutput.innerHTML = "";
            logToUI("[SYSTEM] Terminal cleared.", "system");
        }
    });

    // Theme Switcher
    document.getElementById("themeSwitcher")?.addEventListener("change", (e) => {
        applyAppTheme(e.target.value);
    });

    // Layout buttons
    const gridEl = document.getElementById("chartsGrid");
    const wrapperEls = document.querySelectorAll(".chart-wrapper");
    document.getElementById("layout2x2Btn")?.addEventListener("click", () => {
        gridEl.classList.remove("layout-stack", "layout-focus");
        wrapperEls.forEach(w => w.classList.remove("focused"));
        resizeAllCharts();
    });
    document.getElementById("layoutStackBtn")?.addEventListener("click", () => {
        gridEl.classList.add("layout-stack");
        gridEl.classList.remove("layout-focus");
        wrapperEls.forEach(w => w.classList.remove("focused"));
        resizeAllCharts();
    });
    document.getElementById("layoutFocusBtn")?.addEventListener("click", () => {
        window.toggleMaximizeChart(activeChartIndex);
    });
    document.getElementById("layoutRestoreBtn")?.addEventListener("click", () => {
        gridEl.classList.remove("layout-stack", "layout-focus");
        wrapperEls.forEach(w => w.classList.remove("focused"));
        resizeAllCharts();
    });

    // Right Sidebar toggle
    document.getElementById("toggleRightSidebarBtn")?.addEventListener("click", () => {
        const container = document.querySelector(".app-container");
        const btn = document.getElementById("toggleRightSidebarBtn");
        container.classList.toggle("hide-right");
        btn.textContent = container.classList.contains("hide-right") ? "⬅️" : "➡️";
        resizeAllCharts();
    });

    // Hover highlighting on wrappers
    wrapperEls.forEach((wrapper, idx) => {
        wrapper.addEventListener("mouseenter", () => {
            wrapperEls.forEach(w => w.classList.remove("active-chart-border"));
            wrapper.classList.add("active-chart-border");
            activeChartIndex = idx + 1;
            
            const chartPipsList = [
                parseFloat(chart1Pips.value), parseFloat(chart2Pips.value),
                parseFloat(chart3Pips.value), parseFloat(chart4Pips.value)
            ];
            const pip = chartPipsList[idx];
            document.getElementById("activeChartTitleValue").textContent = `Active Chart: Chart ${activeChartIndex}`;
            document.getElementById("activeChartPipValue").textContent = `Pip Size: ${pip} Pip${pip !== 1 ? "s" : ""}`;
        });
    });

    // Wire up brick click callbacks on all charts
    charts.forEach((chart, idx) => {
        chart.onBrickClickCallback = (brick) => handleBrickClick(brick, idx + 1);
    });
    
    // Crosshair listener
    setupCrosshairDetails();

    // Jump & Step events
    document.getElementById("jumpStartBtn")?.addEventListener("click", () => handleJump(0));
    document.getElementById("jumpEndBtn")?.addEventListener("click", () => {
        if (currentTicksData && currentTicksData.length > 0) {
            handleJump(currentTicksData.length - 1);
        }
    });
    document.getElementById("jumpTimeBtn")?.addEventListener("click", () => {
        const val = document.getElementById("jumpTimeInput").value;
        jumpToTimeStr(val);
    });
    
    document.getElementById("step1Btn")?.addEventListener("click", () => handleStepMulti(1, "forward"));
    document.getElementById("step10Btn")?.addEventListener("click", () => handleStepMulti(10, "forward"));
    document.getElementById("step100Btn")?.addEventListener("click", () => handleStepMulti(100, "forward"));
    document.getElementById("step1000Btn")?.addEventListener("click", () => handleStepMulti(1000, "forward"));

    document.getElementById("stepBack1Btn")?.addEventListener("click", () => handleStepMulti(1, "backward"));
    document.getElementById("stepBack10Btn")?.addEventListener("click", () => handleStepMulti(10, "backward"));
    document.getElementById("stepBack100Btn")?.addEventListener("click", () => handleStepMulti(100, "backward"));
    document.getElementById("stepBack1000Btn")?.addEventListener("click", () => handleStepMulti(1000, "backward"));

    // Checkboxes
    document.getElementById("autoScrollCheckbox")?.addEventListener("change", (e) => {
        const checked = e.target.checked;
        charts.forEach(c => c.autoScroll = checked);
    });
    document.getElementById("dateSeparatorsCheckbox")?.addEventListener("change", (e) => {
        const checked = e.target.checked;
        charts.forEach(c => {
            c.separatorsEnabled = checked;
            c.updateMarkers();
        });
    });
    document.getElementById("reversalMarkersCheckbox")?.addEventListener("change", (e) => {
        const checked = e.target.checked;
        charts.forEach(c => {
            c.reversalEnabled = checked;
            c.updateMarkers();
        });
    });

    // Precision select
    document.getElementById("precisionOverride")?.addEventListener("change", (e) => {
        const val = e.target.value;
        let prec = 5;
        if (val === "auto") {
            prec = metadata && metadata.detected_pip_size < 0.001 ? 5 : 3;
        } else {
            prec = parseInt(val, 10);
        }
        charts.forEach(c => {
            c.setPrecision(prec, 1 / Math.pow(10, prec));
            if (c.bricks.length > 0) {
                c.updateLegend(c.bricks[c.bricks.length - 1]);
            }
        });
    });

    // Clear Backend Cache
    document.getElementById("clearCacheBtn")?.addEventListener("click", async () => {
        console.log("[CACHE] Clearing cache...");
        try {
            const res = await fetch("/api/clear-cache", { method: "POST" });
            const data = await res.json();
            console.log(`[CACHE] ${data.message || "Cache cleared."}`);
        } catch (err) {
            console.error("[CACHE] Clear failed:", err);
        }
    });

    // Exports
    document.getElementById("exportPngBtn")?.addEventListener("click", exportChartPng);
    document.getElementById("exportCsvBtn")?.addEventListener("click", exportCsv);
    document.getElementById("exportSettingsBtn")?.addEventListener("click", exportSettingsPreset);

    // Preset import change event
    const fileInput = document.getElementById("presetImportFile");
    fileInput?.addEventListener("change", (e) => {
        const file = e.target.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = (evt) => {
            try {
                const preset = JSON.parse(evt.target.result);
                console.log("[SETTINGS] Importing preset settings:", preset);
                
                if (preset.csv_path !== undefined) csvPathInput.value = preset.csv_path;
                if (preset.chart1_pips !== undefined) chart1Pips.value = preset.chart1_pips;
                if (preset.chart2_pips !== undefined) chart2Pips.value = preset.chart2_pips;
                if (preset.chart3_pips !== undefined) chart3Pips.value = preset.chart3_pips;
                if (preset.chart4_pips !== undefined) chart4Pips.value = preset.chart4_pips;
                if (preset.price_source !== undefined) priceSourceSelect.value = preset.price_source;
                if (preset.reversal_boxes !== undefined) reversalBoxes.value = preset.reversal_boxes;
                if (preset.pip_size !== undefined) pipSize.value = preset.pip_size;
                if (preset.anchor !== undefined) anchorModeSelect.value = preset.anchor;
                if (preset.chunk_size_mb !== undefined && chunkSizeMb) chunkSizeMb.value = preset.chunk_size_mb;
                if (preset.processing_engine !== undefined) processingEngineSelect.value = preset.processing_engine;
                if (preset.playback_speed !== undefined) {
                    playbackSpeed.value = preset.playback_speed;
                    if (playbackSpeedValue) playbackSpeedValue.textContent = `${preset.playback_speed}/s`;
                }
                if (preset.playback_mode !== undefined) playbackMode.value = preset.playback_mode;
                if (preset.sync_charts !== undefined) {
                    syncChartsCheckbox.checked = preset.sync_charts;
                    RenkoCharts.setSyncEnabled(preset.sync_charts);
                }
                if (preset.precision_override !== undefined) {
                    document.getElementById("precisionOverride").value = preset.precision_override;
                }
                if (preset.build_mode !== undefined) {
                    document.getElementById("buildModeSelect").value = preset.build_mode;
                }
                if (preset.chart_data_mode !== undefined) {
                    document.getElementById("chartDataMode").value = preset.chart_data_mode;
                }
                if (preset.date_separators !== undefined) {
                    const chk = document.getElementById("dateSeparatorsCheckbox");
                    chk.checked = preset.date_separators;
                    charts.forEach(c => { c.separatorsEnabled = preset.date_separators; c.updateMarkers(); });
                }
                if (preset.reversal_markers !== undefined) {
                    const chk = document.getElementById("reversalMarkersCheckbox");
                    chk.checked = preset.reversal_markers;
                    charts.forEach(c => { c.reversalEnabled = preset.reversal_markers; c.updateMarkers(); });
                }
                console.log("✅ Preset settings imported and applied successfully.");
            } catch (err) {
                console.error("Failed to parse settings JSON:", err);
            }
        };
        reader.readAsText(file);
    });

    // Measure tool active toggle
    document.getElementById("toggleMeasureBtn")?.addEventListener("click", (e) => {
        isMeasureEnabled = !isMeasureEnabled;
        const btn = e.target;
        if (isMeasureEnabled) {
            btn.textContent = "Disable";
            btn.style.backgroundColor = "var(--danger-color)";
            console.log("[MEASURE] Measure tool enabled. Click two bricks on a chart to measure.");
            measureStartBrick = null;
            measureEndBrick = null;
            measureChartIndex = null;
            document.getElementById("measureStartValue").textContent = "Start: click a brick";
            document.getElementById("measureEndValue").textContent = "End: click a brick";
            document.getElementById("measureResultOutput").classList.add("hidden");
        } else {
            btn.textContent = "Enable";
            btn.style.backgroundColor = "";
            console.log("[MEASURE] Measure tool disabled.");
            document.getElementById("measureResultOutput").classList.add("hidden");
        }
    });

    // Load engine status + initial system stats
    loadEngineStatus();
    // Start background stats polling (every 2s)
    startStatsPolling();

    console.log("RenkoTerminal ready ✅");
});


// ─── Playback Metrics, Themes, layouts, and Measure tool helpers ───────────
let activeChartIndex = 1;
let isMeasureEnabled = false;
let measureStartBrick = null;
let measureEndBrick = null;
let measureChartIndex = null;

function getSelectedSpeed() {
    const idx = parseInt(playbackSpeed.value, 10);
    return SPEED_VALUES[idx] !== undefined ? SPEED_VALUES[idx] : 100.0;
}

const THEMES = {
    "dark-theme": {
        chartBg: '#0b0f19',
        textColor: '#9ca3af',
        vertLines: '#1f2937',
        horzLines: '#1f2937',
        borderColor: '#374151'
    },
    "light-theme": {
        chartBg: '#ffffff',
        textColor: '#111827',
        vertLines: '#f3f4f6',
        horzLines: '#f3f4f6',
        borderColor: '#e5e7eb'
    },
    "tradingview-dark": {
        chartBg: '#1c2030',
        textColor: '#d1d4dc',
        vertLines: '#2a2e39',
        horzLines: '#2a2e39',
        borderColor: '#2a2e39'
    },
    "ctrader-dark": {
        chartBg: '#0c0f16',
        textColor: '#cbd2db',
        vertLines: '#242a38',
        horzLines: '#242a38',
        borderColor: '#242a38'
    }
};

function applyAppTheme(themeName) {
    const theme = THEMES[themeName];
    if (!theme) return;
    document.body.className = themeName;
    charts.forEach(c => c.applyTheme(theme));
    console.log(`[THEME] Switched theme to: ${themeName}`);
}

function updatePlaybackMetrics(msg) {
    if (!msg) return;
    const speedSetVal = getSelectedSpeed();
    const metricsSpeedSet = document.getElementById("metricsSpeedSet");
    if (metricsSpeedSet) {
        metricsSpeedSet.textContent = `${speedSetVal} ticks/s`;
    }

    const metricsActualSpeed = document.getElementById("metricsActualSpeed");
    if (metricsActualSpeed) {
        metricsActualSpeed.textContent = `${msg.speed !== undefined ? msg.speed : speedSetVal} ticks/s`;
    }

    const total = msg.total_ticks || 0;
    const processed = msg.processed_ticks || 0;
    const pct = total > 0 ? (processed / total) * 100 : 0;

    const metricsProgress = document.getElementById("metricsProgress");
    if (metricsProgress) {
        metricsProgress.textContent = `${pct.toFixed(1)}%`;
    }

    const metricsProcessed = document.getElementById("metricsProcessed");
    if (metricsProcessed) {
        metricsProcessed.textContent = processed.toLocaleString();
    }

    const metricsRemaining = document.getElementById("metricsRemaining");
    if (metricsRemaining) {
        metricsRemaining.textContent = (total - processed).toLocaleString();
    }

    const metricsFormedBricks = document.getElementById("metricsFormedBricks");
    if (metricsFormedBricks) {
        let detailStr = `${(msg.formed_bricks || 0).toLocaleString()} total`;
        if (msg.engine_confirmed_counts) {
            const chartPipsList = [
                parseFloat(chart1Pips.value), parseFloat(chart2Pips.value),
                parseFloat(chart3Pips.value), parseFloat(chart4Pips.value)
            ];
            detailStr = msg.engine_confirmed_counts.map((c, idx) => `${chartPipsList[idx]}p: ${c}`).join(" | ");
        }
        metricsFormedBricks.textContent = detailStr;
    }

    const metricsCurrentUtc = document.getElementById("metricsCurrentUtc");
    if (metricsCurrentUtc) {
        let utcStr = "—";
        if (msg.latest_time) {
            utcStr = msg.latest_time;
        } else if (msg.live_bricks_by_chart) {
            const firstLive = Object.values(msg.live_bricks_by_chart)[0];
            if (firstLive && firstLive.confirm_time) {
                utcStr = firstLive.confirm_time;
            }
        }
        metricsCurrentUtc.textContent = utcStr;
    }

    const metricsLatestPrice = document.getElementById("metricsLatestPrice");
    if (metricsLatestPrice) {
        let priceStr = "—";
        if (msg.latest_price !== undefined && msg.latest_price > 0) {
            priceStr = Number(msg.latest_price).toFixed(parseFloat(pipSize.value) < 0.001 ? 5 : 3);
        } else if (msg.latest_bid !== undefined && msg.latest_bid > 0) {
            priceStr = Number(msg.latest_bid).toFixed(parseFloat(pipSize.value) < 0.001 ? 5 : 3);
        }
        metricsLatestPrice.textContent = priceStr;
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

function calculateTicksBetween(chartIndex, startIndex, endIndex) {
    const chart = charts[chartIndex - 1];
    if (!chart) return 0;
    const bricks = chart.bricks;
    const start = Math.min(startIndex, endIndex);
    const end = Math.max(startIndex, endIndex);
    let sum = 0;
    for (let i = start; i <= end; i++) {
        if (bricks[i]) {
            sum += bricks[i].tick_count || 0;
        }
    }
    return sum;
}

function handleBrickClick(brick, chartIdx) {
    if (!isMeasureEnabled) return;
    if (measureChartIndex !== null && measureChartIndex !== chartIdx) {
        measureStartBrick = null;
        measureEndBrick = null;
    }
    measureChartIndex = chartIdx;

    if (!measureStartBrick) {
        measureStartBrick = brick;
        document.getElementById("measureStartValue").textContent = `Start: Brick ${brick.time} (${brick.close.toFixed(charts[chartIdx-1].precision)})`;
        document.getElementById("measureEndValue").textContent = "End: click a brick";
        document.getElementById("measureResultOutput").classList.add("hidden");
    } else {
        measureEndBrick = brick;
        document.getElementById("measureEndValue").textContent = `End: Brick ${brick.time} (${brick.close.toFixed(charts[chartIdx-1].precision)})`;
        
        const priceDiff = measureEndBrick.close - measureStartBrick.close;
        const pips = priceDiff / parseFloat(pipSize.value);
        const bricksDiff = measureEndBrick.time - measureStartBrick.time;
        
        const activeChart = charts[chartIdx - 1];
        const idxStart = activeChart.bricks.indexOf(measureStartBrick);
        const idxEnd = activeChart.bricks.indexOf(measureEndBrick);
        const ticksSum = calculateTicksBetween(chartIdx, idxStart, idxEnd);
        
        const timeStart = new Date(measureStartBrick.confirm_time).getTime();
        const timeEnd = new Date(measureEndBrick.confirm_time).getTime();
        const timeDiffMs = Math.abs(timeEnd - timeStart);
        let timeStr = "";
        if (isNaN(timeDiffMs)) {
            timeStr = "forming brick";
        } else {
            const seconds = Math.floor(timeDiffMs / 1000);
            const minutes = Math.floor(seconds / 60);
            const hours = Math.floor(minutes / 60);
            const days = Math.floor(hours / 24);
            if (days > 0) timeStr = `${days}d ${hours % 24}h`;
            else if (hours > 0) timeStr = `${hours}h ${minutes % 60}m`;
            else if (minutes > 0) timeStr = `${minutes}m ${seconds % 60}s`;
            else timeStr = `${seconds}s`;
        }

        const out = document.getElementById("measureResultOutput");
        document.getElementById("measureMove").textContent = `${priceDiff > 0 ? "+" : ""}${priceDiff.toFixed(activeChart.precision)} (${pips > 0 ? "+" : ""}${pips.toFixed(1)} pips)`;
        document.getElementById("measureMove").className = priceDiff >= 0 ? "text-success" : "text-danger";
        document.getElementById("measureBricks").textContent = `${Math.abs(bricksDiff)} bricks`;
        document.getElementById("measureTicks").textContent = `${ticksSum.toLocaleString()} ticks`;
        document.getElementById("measureTime").textContent = timeStr;
        out.classList.remove("hidden");
        
        measureStartBrick = null;
        measureEndBrick = null;
    }
}

window.toggleMaximizeChart = function(index) {
    const grid = document.getElementById("chartsGrid");
    const wrappers = document.querySelectorAll(".chart-wrapper");
    const wrapper = document.getElementById(`chartWrapper${index}`);
    
    if (grid.classList.contains("layout-focus") && wrapper.classList.contains("focused")) {
        grid.classList.remove("layout-focus");
        wrappers.forEach(w => w.classList.remove("focused"));
        console.log(`[LAYOUT] Restored charts grid layout.`);
    } else {
        grid.classList.add("layout-focus");
        grid.classList.remove("layout-stack");
        wrappers.forEach(w => {
            if (w === wrapper) w.classList.add("focused");
            else w.classList.remove("focused");
        });
        console.log(`[LAYOUT] Maximized Chart ${index}.`);
    }
    resizeAllCharts();
};

function resizeAllCharts() {
    setTimeout(() => {
        charts.forEach(c => {
            c.chart.resize(c.container.clientWidth, c.container.clientHeight);
        });
    }, 50);
}

function exportChartPng() {
    const chartInst = charts[activeChartIndex - 1];
    if (!chartInst) return;
    const canvases = chartInst.container.querySelectorAll("canvas");
    if (canvases.length === 0) return;
    
    const tempCanvas = document.createElement("canvas");
    tempCanvas.width = canvases[0].width;
    tempCanvas.height = canvases[0].height;
    const ctx = tempCanvas.getContext("2d");
    canvases.forEach(canvas => ctx.drawImage(canvas, 0, 0));
    
    const link = document.createElement("a");
    link.download = `chart_${activeChartIndex}_export.png`;
    link.href = tempCanvas.toDataURL("image/png");
    link.click();
    console.log(`[EXPORT] Chart ${activeChartIndex} exported as PNG.`);
}

function exportCsv() {
    const chartInst = charts[activeChartIndex - 1];
    if (!chartInst || chartInst.bricks.length === 0) {
        console.warn("No bricks data to export.");
        return;
    }
    const rows = [["Index", "ConfirmTime", "Open", "High", "Low", "Close", "Direction", "Ticks"]];
    chartInst.bricks.forEach(b => {
        rows.push([b.time, b.confirm_time || "", b.open, b.high, b.low, b.close, b.direction, b.tick_count]);
    });
    const csvString = rows.map(r => r.join(",")).join("\n");
    const blob = new Blob([csvString], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    link.setAttribute("href", URL.createObjectURL(blob));
    link.setAttribute("download", `chart_${activeChartIndex}_bricks.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    console.log(`[EXPORT] Chart ${activeChartIndex} bricks exported as CSV.`);
}

function exportSettingsPreset() {
    const preset = {
        csv_path: csvPathInput.value.trim(),
        chart1_pips: parseFloat(chart1Pips.value),
        chart2_pips: parseFloat(chart2Pips.value),
        chart3_pips: parseFloat(chart3Pips.value),
        chart4_pips: parseFloat(chart4Pips.value),
        price_source: priceSourceSelect.value,
        reversal_boxes: parseInt(reversalBoxes.value),
        pip_size: parseFloat(pipSize.value),
        anchor: anchorModeSelect.value,
        chunk_size_mb: parseInt(chunkSizeMb?.value || "64"),
        processing_engine: processingEngineSelect.value,
        playback_speed: getSelectedSpeed(),
        playback_mode: playbackMode.value,
        sync_charts: syncChartsCheckbox.checked,
        precision_override: document.getElementById("precisionOverride").value,
        build_mode: document.getElementById("buildModeSelect").value,
        chart_data_mode: document.getElementById("chartDataMode").value,
        date_separators: document.getElementById("dateSeparatorsCheckbox").checked,
        reversal_markers: document.getElementById("reversalMarkersCheckbox").checked,
    };
    const blob = new Blob([JSON.stringify(preset, null, 2)], { type: "application/json" });
    const link = document.createElement("a");
    link.setAttribute("href", URL.createObjectURL(blob));
    link.setAttribute("download", "renko_preset_settings.json");
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    console.log("[SETTINGS] Settings preset exported as JSON.");
}

function setupCrosshairDetails() {
    charts.forEach((chart, idx) => {
        chart.onCrosshairChangeCallback = (brick) => {
            const cursorChart = document.getElementById("cursorChart");
            const cursorIndex = document.getElementById("cursorIndex");
            const cursorTime = document.getElementById("cursorTime");
            const cursorOpen = document.getElementById("cursorOpen");
            const cursorHigh = document.getElementById("cursorHigh");
            const cursorLow = document.getElementById("cursorLow");
            const cursorClose = document.getElementById("cursorClose");
            const cursorDirection = document.getElementById("cursorDirection");
            const cursorTickCount = document.getElementById("cursorTickCount");
            const cursorBrickSize = document.getElementById("cursorBrickSize");
            const cursorSource = document.getElementById("cursorSource");
            const cursorSpread = document.getElementById("cursorSpread");

            if (!brick) {
                cursorChart.textContent = "—";
                cursorIndex.textContent = "—";
                cursorTime.textContent = "—";
                cursorOpen.textContent = "—";
                cursorHigh.textContent = "—";
                cursorLow.textContent = "—";
                cursorClose.textContent = "—";
                cursorDirection.textContent = "—";
                cursorDirection.className = "val";
                cursorTickCount.textContent = "—";
                cursorBrickSize.textContent = "—";
                cursorSource.textContent = "—";
                cursorSpread.textContent = "—";
                return;
            }

            const chartPipsList = [
                parseFloat(chart1Pips.value), parseFloat(chart2Pips.value),
                parseFloat(chart3Pips.value), parseFloat(chart4Pips.value)
            ];
            const pip = chartPipsList[idx];
            const prec = chart.precision;

            cursorChart.textContent = `Chart ${idx + 1}`;
            cursorIndex.textContent = brick.time;
            cursorTime.textContent = brick.confirm_time || "Forming";
            cursorOpen.textContent = brick.open.toFixed(prec);
            cursorHigh.textContent = brick.high.toFixed(prec);
            cursorLow.textContent = brick.low.toFixed(prec);
            cursorClose.textContent = brick.close.toFixed(prec);
            cursorDirection.textContent = brick.direction.toUpperCase();
            cursorDirection.className = brick.direction === "up" ? "val text-success" : "val text-danger";
            cursorTickCount.textContent = brick.tick_count;
            cursorBrickSize.textContent = `${pip} Pip${pip !== 1 ? "s" : ""}`;
            cursorSource.textContent = priceSourceSelect.value;
            
            if (brick.bid !== undefined && brick.ask !== undefined && brick.bid > 0 && brick.ask > 0) {
                const spreadPips = (brick.ask - brick.bid) / parseFloat(pipSize.value);
                cursorSpread.textContent = spreadPips.toFixed(1);
            } else {
                cursorSpread.textContent = "—";
            }
        };
    });
}
