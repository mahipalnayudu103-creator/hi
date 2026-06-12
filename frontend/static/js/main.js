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

function debounce(func, wait) {
    let timeout;
    return function(...args) {
        const context = this;
        clearTimeout(timeout);
        timeout = setTimeout(() => func.apply(context, args), wait);
    };
}

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
let processingEngineSelect;
let pipAddInput, pipListInput;
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

// Chart instances (dynamic — managed by buildChartUI)
let charts = [];

// ─── App state ────────────────────────────────────────────────────────────────
let pipList         = [1, 2, 3, 4];  // active pip sizes — source of truth for all charts
let metadata        = null;
let currentBricksData = null;
let currentTicksData = null;
let isPlaying       = false;
let playbackInitialized = false;      // true after first play (for pause/resume)
window.isPlaybackPlaying = () => isPlaying;
let ws              = null;           // playback WS
let jobWS           = null;           // build job WS
let currentJobId    = null;
let statsIntervalId = null;           // polling interval for system stats
let selectedStartUtc = null;
let selectedEndUtc   = null;
let dataDirty       = false;
let chartsLoaded    = false;
let autoPlayAfterBuild = false;
let autoStepAfterBuild = false;
// Playback speed is a market-time multiplier: 1× to 100,000× real time.
// Slider is 0–100; mapped logarithmically so each 20 steps = one decade:
//   0→1×  20→10×  40→100×  60→1000×  80→10000×  100→100000×
function sliderToMultiplier(sliderVal) {
    const v = Math.max(0, Math.min(100, Number(sliderVal)));
    return Math.round(Math.pow(10, v * 5 / 100));
}
function formatMultiplier(x) {
    if (x >= 1000) return (x / 1000).toFixed(x % 1000 === 0 ? 0 : 1) + 'k×';
    return x + '×';
}

// ─── Dynamic Chart Management ─────────────────────────────────────────────────
function getCurrentPips() { return [...pipList]; }

function parsePipList(str) {
    if (!str || !str.trim()) return [];
    return str.split(/[\s,;]+/)
        .map(s => parseFloat(s.trim()))
        .filter(v => !isNaN(v) && v > 0);
}

function updatePipListInput() {
    if (pipListInput) pipListInput.value = pipList.join(',');
}

function renderPipListUI() {
    const container = document.getElementById('activePipList');
    const countEl   = document.getElementById('activeChartCount');
    if (!container) return;
    if (countEl) countEl.textContent = pipList.length;
    container.innerHTML = '';
    if (pipList.length === 0) {
        container.innerHTML = '<div class="pip-empty">No charts — add pip sizes above.</div>';
        return;
    }
    pipList.forEach((pip, i) => {
        const item = document.createElement('div');
        item.className = 'pip-list-item';
        item.innerHTML = `
            <span class="pip-item-badge">${pip} pip${pip !== 1 ? 's' : ''}</span>
            <button class="pip-item-remove" title="Remove this chart">✕</button>
        `;
        item.querySelector('.pip-item-remove').addEventListener('click', () => {
            const next = [...pipList];
            next.splice(i, 1);
            buildChartUI(next);
        });
        container.appendChild(item);
    });
}

function setupChartHoverListeners() {
    document.querySelectorAll('.chart-wrapper').forEach((wrapper, idx) => {
        wrapper.addEventListener('mouseenter', () => {
            document.querySelectorAll('.chart-wrapper').forEach(w => w.classList.remove('active-chart-border'));
            wrapper.classList.add('active-chart-border');
            activeChartIndex = idx + 1;
            const pip = pipList[idx] !== undefined ? pipList[idx] : 1;
            const titleEl = document.getElementById('activeChartTitleValue');
            const pipEl   = document.getElementById('activeChartPipValue');
            if (titleEl) titleEl.textContent = `Active Chart: Chart ${activeChartIndex}`;
            if (pipEl)   pipEl.textContent   = `Pip Size: ${pip} Pip${pip !== 1 ? 's' : ''}`;
        });
    });
}

function buildChartUI(pips) {
    if (!Array.isArray(pips)) return;
    const count = pips.length;

    // Performance gates
    if (count > 30) {
        const ok = confirm(`Creating ${count} charts may significantly slow down the browser. Continue?`);
        if (!ok) return;
        console.warn(`[CHARTS] ${count} charts — large configuration.`);
    } else if (count > 16) {
        const ok = confirm(`Creating ${count} charts (>16) may impact performance. Continue?`);
        if (!ok) return;
    } else if (count > 8) {
        console.warn(`[CHARTS] ${count} charts configured — performance may be affected above 8 charts.`);
    }

    stopPlayback();
    currentBricksData = null;
    currentTicksData  = null;
    chartsLoaded      = false;

    // Destroy old chart instances
    charts.forEach(c => { try { c.destroy(); } catch(e) {} });
    charts = [];
    if (window.RenkoCharts) window.RenkoCharts.allInstances = [];

    const grid = document.getElementById('chartsGrid');
    if (!grid) return;
    grid.innerHTML = '';

    if (count === 0) {
        grid.style.gridTemplateColumns = '1fr';
        grid.innerHTML = '<div class="pip-empty-grid">No charts configured.<br>Add pip sizes in ⚙️ Renko Parameters.</div>';
        pipList = [];
        renderPipListUI();
        updatePipListInput();
        return;
    }

    // Responsive column count
    const cols = count <= 1 ? 1 : count <= 2 ? 2 : count <= 4 ? 2 : count <= 6 ? 3 : count <= 9 ? 3 : 4;
    grid.style.gridTemplateColumns = `repeat(${cols}, minmax(0, 1fr))`;

    pips.forEach((pip, i) => {
        const idx = i + 1;
        const wrapper = document.createElement('div');
        wrapper.className = 'chart-wrapper';
        wrapper.id = `chartWrapper${idx}`;
        wrapper.dataset.index = String(idx);
        wrapper.dataset.pip   = String(pip);
        wrapper.innerHTML = `
            <div class="chart-title-bar">
                <span class="title"  id="chartTitle${idx}">Chart ${idx} (${pip} Pip${pip !== 1 ? 's' : ''})</span>
                <span class="legend" id="chartLegend${idx}">O: — H: — L: — C: —</span>
            </div>
            <div id="chartContainer${idx}" class="chart-container"></div>
        `;
        grid.appendChild(wrapper);

        const chart = new RenkoCharts.RenkoChart(
            `chartContainer${idx}`, `chartLegend${idx}`, `chartTitle${idx}`, idx, pip
        );
        chart.onBrickClickCallback = (brick) => handleBrickClick(brick, idx);
        charts.push(chart);
    });

    RenkoCharts.setupSync(charts);
    RenkoCharts.setupCrosshairSync(charts);
    setupChartHoverListeners();
    setupCrosshairDetails();

    pipList = [...pips];
    renderPipListUI();
    updatePipListInput();

    if (statusText) {
        statusText.textContent = `${count} chart${count !== 1 ? 's' : ''} configured. Build Renko Charts to load data.`;
        statusText.className = 'value success';
    }
    console.log(`[CHARTS] Built ${count} chart panel${count !== 1 ? 's' : ''}: [${pips.join(', ')}] pip`);
    checkCacheLookup();
}

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

// ─── Backend heartbeat: detect server shutdown and close/flag the tab ──────────
// When the user closes the "Renko Backend" window, the page would otherwise sit
// there looking alive. We ping /api/health; after a few consecutive failures we
// treat the server as stopped, try to close the tab (browsers usually only allow
// this for script-opened tabs), and fall back to a clear "server stopped" overlay.
let _heartbeatIntervalId = null;
let _heartbeatFails = 0;
let _serverDownHandled = false;
let _buildRequestActive = false;

function startHeartbeat() {
    if (_heartbeatIntervalId) return;
    _heartbeatIntervalId = setInterval(async () => {
        try {
            const ctrl = new AbortController();
            const timer = setTimeout(() => ctrl.abort(), 1500);
            const res = await fetch("/api/health", { cache: "no-store", signal: ctrl.signal });
            clearTimeout(timer);
            if (!res.ok) throw new Error("status " + res.status);
            // Healthy — reset and restore the badge if it had gone red.
            _heartbeatFails = 0;
            const badge = document.getElementById("serverBadge");
            if (badge && !badge.classList.contains("green")) {
                badge.textContent = "Backend Connected";
                badge.classList.remove("red");
                badge.classList.add("green");
            }
        } catch {
            if (_buildRequestActive) {
                _heartbeatFails = 0;
                return;
            }
            _heartbeatFails++;
            if (_heartbeatFails >= 3 && !_serverDownHandled) {
                handleServerShutdown();
            }
        }
    }, 2000);
}

function handleServerShutdown() {
    _serverDownHandled = true;
    stopStatsPolling();
    const badge = document.getElementById("serverBadge");
    if (badge) {
        badge.textContent = "Backend Disconnected";
        badge.classList.remove("green");
        badge.classList.add("red");
    }
    // Best-effort: close the tab. Browsers block script-close for tabs they opened
    // themselves (the usual case here), but it's a harmless no-op when denied — so we
    // call it directly WITHOUT the window.open("","_self") trick, which would blank the
    // page to about:blank and wipe the overlay. The overlay is the reliable fallback.
    showServerStoppedOverlay();
}

function showServerStoppedOverlay() {
    if (document.getElementById("serverStoppedOverlay")) return;
    const ov = document.createElement("div");
    ov.id = "serverStoppedOverlay";
    ov.style.cssText = [
        "position:fixed", "inset:0", "z-index:99999",
        "display:flex", "flex-direction:column", "align-items:center", "justify-content:center",
        "gap:18px", "background:rgba(8,11,18,0.94)", "backdrop-filter:blur(4px)",
        "font-family:Inter,sans-serif", "color:#e2e8f0", "text-align:center", "padding:24px"
    ].join(";");
    ov.innerHTML =
        '<div style="font-size:48px;line-height:1">🔌</div>' +
        '<div style="font-size:22px;font-weight:700">Server stopped</div>' +
        '<div style="font-size:14px;color:#94a3b8;max-width:420px">' +
        'The Renko backend was closed. This tab is no longer live — you can close it.' +
        '</div>' +
        '<button id="serverStoppedCloseBtn" style="margin-top:8px;padding:10px 22px;' +
        'border:none;border-radius:8px;background:linear-gradient(135deg,#8b5cf6,#6366f1);' +
        'color:#fff;font-weight:600;font-size:14px;cursor:pointer">Close Tab</button>';
    document.body.appendChild(ov);
    const btn = document.getElementById("serverStoppedCloseBtn");
    if (btn) btn.addEventListener("click", () => showServerStoppedOverlay());
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
    if (!chartsLoaded) {
        charts.forEach(c => c.clear());
    }
    if (_worker) {
        _worker.terminate();
        _worker = null;
        _workerReady = false;
    }
    if (!chartsLoaded) {
        statusText.textContent = "Range changed. Build Renko Charts again.";
        statusText.className = "value warning";
    }
    // Debounce cache lookup so it doesn't fire on every slider tick
    clearTimeout(handleRangeChange._cacheTimer);
    handleRangeChange._cacheTimer = setTimeout(checkCacheLookup, 400);
    
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

        const fileStartMs = new Date(metadata.file_start_utc).getTime();
        const fileEndMs   = new Date(metadata.file_end_utc).getTime();
        if (!Number.isFinite(fileStartMs) || !Number.isFinite(fileEndMs) || fileStartMs >= fileEndMs) {
            rangeCard.classList.add("hidden");
            statusText.textContent = "CSV loaded, but first/last tick time was not detected. Check the Time column format before building.";
            statusText.className = "value error";
            return;
        }

        rangeCard.classList.remove("hidden");
        const defEndMs    = fileEndMs;

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

            // Debounce cache lookup so it updates dynamically as the range changes
            clearTimeout(dateRangeSlider._cacheTimer);
            dateRangeSlider._cacheTimer = setTimeout(checkCacheLookup, 300);
        });

        dateRangeSlider.noUiSlider.on("change", () => {
            handleRangeChange();
        });

        statusText.textContent = "Metadata loaded. Select range and Build Renko.";
        statusText.className = "value success";
        checkCacheLookup();
    } catch (err) {
        console.error("Metadata error:", err);
        statusText.textContent = `Error: ${err.message}`;
        statusText.className = "value warning";
    }
}

// ─── Cache lookup panel ───────────────────────────────────────────────────────
let _lastCacheLookup = null; // {exact_match, sub_range_match, metadata, similar_matches}

async function checkCacheLookup() {
    const panel = document.getElementById("cacheReuseCard");
    if (!panel) return;
    if (!metadata || !selectedStartUtc || !selectedEndUtc) {
        panel.classList.add("hidden");
        return;
    }
    const chartPips = getCurrentPips();
    if (chartPips.length === 0) { panel.classList.add("hidden"); return; }

    try {
        const resp = await fetch("/api/cache/lookup", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                csv_path:       csvPathInput.value.trim(),
                start_utc:      selectedStartUtc,
                end_utc:        selectedEndUtc,
                price_source:   priceSourceSelect.value,
                reversal_boxes: parseInt(reversalBoxes.value),
                pip_size:       parseFloat(pipSize.value),
                anchor:         anchorModeSelect.value,
                chart_pips:     chartPips,
            }),
        });
        if (!resp.ok) return;
        const data = await resp.json();
        _lastCacheLookup = data;
        renderCacheLookupPanel(data);

        if (data.exact_match || data.sub_range_match) {
            if (chartsLoaded) {
                console.log("[AUTO-LOAD] Date range changed & cache match found. Auto-loading slice:", selectedStartUtc, "to", selectedEndUtc);
                _loadAndRenderResult(data.job_id, chartPips, selectedStartUtc, selectedEndUtc);
            }
        } else {
            if (chartsLoaded) {
                charts.forEach(c => c.clear());
                chartsLoaded = false;
                statusText.textContent = "Range changed. Build Renko Charts again.";
                statusText.className = "value warning";
            }
        }
    } catch (_) { /* network errors are silent */ }
}

function renderCacheLookupPanel(data) {
    const card = document.getElementById("cacheReuseCard");
    if (!card) return;

    const titleEl = document.getElementById("cacheCardTitle");
    const fileEl  = document.getElementById("cacheFileNameText");
    const rangeEl = document.getElementById("cacheRangeText");
    const pipsEl  = document.getElementById("cachePipsText");
    const bricksEl = document.getElementById("cacheBricksText");
    const builtEl = document.getElementById("cacheBuiltTimeText");
    const simSec  = document.getElementById("similarCachePanel");
    const simList = document.getElementById("similarCacheList");

    const hasMatch = data.exact_match || data.sub_range_match;
    const meta = data.metadata;
    const similar = data.similar_matches || [];

    if (hasMatch && meta) {
        if (titleEl) {
            titleEl.textContent = data.exact_match ? "💾 Previous Build Found" : "⚡ Cache Reuse Available";
        }
        
        // Extract filename from csv_path
        const csvPath = meta.csv_path || "";
        const filename = csvPath.split(/[\\/]/).pop();
        if (fileEl) fileEl.textContent = filename || "—";

        // Display range
        const mStart = meta.start_utc ? meta.start_utc.slice(0, 19).replace("T", " ") : "";
        const mEnd = meta.end_utc ? meta.end_utc.slice(0, 19).replace("T", " ") : "";
        if (rangeEl) rangeEl.textContent = `${mStart} to ${mEnd}`;

        const selRangeRow = document.getElementById("cacheSelectedRangeRow");
        const selRangeText = document.getElementById("cacheSelectedRangeText");
        if (data.sub_range_match) {
            if (selRangeRow) selRangeRow.style.display = "block";
            const sStart = selectedStartUtc ? selectedStartUtc.slice(0, 19).replace("T", " ").replace("Z", "").replace(".000", "") : "";
            const sEnd = selectedEndUtc ? selectedEndUtc.slice(0, 19).replace("T", " ").replace("Z", "").replace(".000", "") : "";
            if (selRangeText) selRangeText.textContent = `${sStart} to ${sEnd}`;
        } else {
            if (selRangeRow) selRangeRow.style.display = "none";
        }

        // Brick counts formatting
        const bc = meta.brick_counts || {};
        const pips = meta.chart_pips || [];
        const brickInfo = pips.map(p => {
            const pNum = parseFloat(p);
            let count = 0;
            for (const [k, val] of Object.entries(bc)) {
                if (Math.abs(parseFloat(k) - pNum) < 1e-6) {
                    count = val;
                    break;
                }
            }
            return `${p}p: ${count.toLocaleString()}`;
        }).join(" | ");
        if (pipsEl) pipsEl.textContent = brickInfo;

        // Bricks info (ticks used + engine)
        if (bricksEl) {
            bricksEl.textContent = `${meta.ticks_used?.toLocaleString() ?? "?"} ticks · ${meta.engine_used}`;
        }

        // Built time
        const d = new Date(meta.created_at * 1000);
        if (builtEl) builtEl.textContent = d.toLocaleString();

        card.classList.remove("hidden");
    } else {
        card.classList.add("hidden");
        const selRangeRow = document.getElementById("cacheSelectedRangeRow");
        if (selRangeRow) selRangeRow.style.display = "none";
    }

    if (similar.length > 0) {
        if (simList) {
            simList.innerHTML = similar.map((s, i) => {
                const pips = (s.chart_pips || []).join(", ");
                const range = `${(s.start_utc || "").slice(0,10)} → ${(s.end_utc || "").slice(0,10)}`;
                return `<li class="cache-similar-item" data-idx="${i}" style="cursor: pointer; padding: 4px 6px; margin-bottom: 4px; background: rgba(255,255,255,0.03); border-radius: 4px;">
                    <span class="sim-pips" style="font-weight: 500; color: var(--text-highlight);">[${pips}]</span> ${range}
                    <span style="float:right;opacity:0.6">${s.ticks_used?.toLocaleString() ?? "?"} ticks</span>
                </li>`;
            }).join("");
        }
        if (simSec) simSec.classList.remove("hidden");
    } else {
        if (simSec) simSec.classList.add("hidden");
    }
}

// ─── Build Renko (Job-based) ──────────────────────────────────────────────────
async function runBuildCharts() {
    if (!metadata) { console.error("Load metadata first."); return; }

    stopPlayback();

    const chartPips = getCurrentPips();
    const buildMode = document.getElementById("buildModeSelect")?.value || "full";
    const isPreview = buildMode === "preview";
    const isCacheOnly = buildMode === "cache_only";
    const buildLabel = isPreview ? "preview build" : (isCacheOnly ? "cache-only build" : "build job");

    // Clear charts before starting a new build
    if (!isCacheOnly) {
        charts.forEach(c => c.clear());
    }

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
    _buildRequestActive = true;
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
            _buildRequestActive = false;
            if (isCacheOnly) {
                statusText.textContent = "Cache is already ready for this selection.";
                statusText.className = "value success";
                progressSection.classList.add("hidden");
                setPipelineStage(null);
                return;
            }
            
            stagedCache = {
                jobId: jobId,
                chartPips: chartPips,
                startUtc: selectedStartUtc,
                endUtc: selectedEndUtc,
                metadata: resp.metadata || (_lastCacheLookup ? _lastCacheLookup.metadata : null)
            };

            // Clear charts & update titles
            charts.forEach((c, i) => {
                c.clear();
                c.setTitle(`Chart ${i + 1} (${chartPips[i]} Pip${chartPips[i] !== 1 ? "s" : ""}) [Staged]`);
            });
            chartsLoaded = false;

            // Update indicators
            const meta = resp.metadata || (_lastCacheLookup ? _lastCacheLookup.metadata : null);
            ticksLoadedText.textContent  = (meta?.ticks_used || 0).toLocaleString();
            bricksBuiltText.textContent  = "Staged";
            dataEngineStatus.textContent = "Staged (" + (meta?.engine_used || "Disk Cache") + ")";

            if (autoPlayAfterBuild) {
                autoPlayAfterBuild = false;
                autoStepAfterBuild = false;
                loadStagedCacheAndPlay();
            } else if (autoStepAfterBuild) {
                autoPlayAfterBuild = false;
                autoStepAfterBuild = false;
                loadStagedCacheAndStep();
            } else {
                statusText.textContent = "⚡ Cache hit & staged. Tap Play/Step to start.";
                statusText.className = "value success";
                progressSection.classList.add("hidden");
                setPipelineStage(null);
            }
            return;
        }
    } catch (err) {
        _buildRequestActive = false;
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
            
            const isAutoPlay = autoPlayAfterBuild || autoStepAfterBuild;
            statusText.textContent = isAutoPlay ? `Building & Playing… ${pct.toFixed(1)}%` : `Building… ${pct.toFixed(1)}%`;
            progressLabel.textContent = `Building Renko… ${pct.toFixed(1)}%`;

            // If new bricks are received during the build, and we are in auto-play/step mode, append them to the charts
            if (isAutoPlay && msg.new_bricks) {
                Object.entries(msg.new_bricks).forEach(([pipStr, bricks]) => {
                    const chartIdx = pipList.indexOf(parseFloat(pipStr));
                    if (chartIdx !== -1 && charts[chartIdx]) {
                        const chart = charts[chartIdx];
                        const remaining = Math.max(0, MAX_LIVE_BUILD_BRICKS_PER_CHART - chart.bricks.length);
                        bricks.slice(0, remaining).forEach(brick => {
                            chart.appendBrick(brick);
                        });
                    }
                });
            }

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
            _buildRequestActive = false;
            setPipelineStage("cache");
            updateProgress(100, msg.ticks_used, msg.bricks_built, msg.rows_scanned);
            progressLabel.textContent = "✅ Build complete!";
            dataEngineStatus.textContent = msg.engine_used || "Done";
            ticksLoadedText.textContent = (msg.ticks_used || 0).toLocaleString();
            
            const totalBricks = Object.values(msg.bricks_built || {}).reduce((s, v) => s + v, 0);
            bricksBuiltText.textContent = "Staged";
            
            if (isCacheOnly) {
                progressLabel.textContent = "Cache build complete.";
                statusText.textContent = `Cache ready - ${totalBricks.toLocaleString()} bricks stored`;
                statusText.className = "value success";
                progressSection.classList.add("hidden");
                setPipelineStage(null);
                return;
            }

            stagedCache = {
                jobId: jobId,
                chartPips: chartPips,
                startUtc: selectedStartUtc,
                endUtc: selectedEndUtc,
                metadata: {
                    ticks_used: msg.ticks_used,
                    engine_used: msg.engine_used,
                    brick_counts: msg.bricks_built
                }
            };

            // Clear charts & update titles
            charts.forEach((c, i) => {
                c.clear();
                c.setTitle(`Chart ${i + 1} (${chartPips[i]} Pip${chartPips[i] !== 1 ? "s" : ""}) [Staged]`);
            });
            chartsLoaded = false;

            if (autoPlayAfterBuild) {
                autoPlayAfterBuild = false;
                autoStepAfterBuild = false;
                loadStagedCacheAndPlay();
            } else if (autoStepAfterBuild) {
                autoPlayAfterBuild = false;
                autoStepAfterBuild = false;
                loadStagedCacheAndStep();
            } else {
                statusText.textContent = "⚡ Build complete & cache staged. Tap Play/Step to start.";
                statusText.className = "value success";
                progressSection.classList.add("hidden");
                setPipelineStage(null);
            }
        },
        onError: (errMsg) => {
            _buildRequestActive = false;
            console.error("[Build Error]", errMsg);
            statusText.textContent = `Build error: ${errMsg.slice(0, 120)}`;
            statusText.className = "value error";
            progressSection.classList.add("hidden");
            setPipelineStage(null);
        },
        onStats: (stats) => updateMonitor(stats),
        onClose: () => {
            console.log("Job WS closed.");
            setTimeout(() => { _buildRequestActive = false; }, 5000);
        },
    });

    statusText.textContent = `${buildLabel.charAt(0).toUpperCase()}${buildLabel.slice(1)} running...`;
    statusText.className = "value warning";
}

async function _loadAndRenderResult(jobId, chartPips, startUtc = "", endUtc = "") {
    try {
        statusText.textContent = "Fetching bricks…";
        const result = await RenkoAPI.getJobResult(jobId, startUtc, endUtc);
        currentBricksData = result.charts;
        currentTicksData = result.ticks || [];

        currentJobId = jobId;
        const consoleJobId = document.getElementById("consoleJobId");
        if (consoleJobId) consoleJobId.textContent = `Job: ${jobId.slice(0, 8)}…`;

        // Update labels
        charts.forEach((c, i) => c.setTitle(`Chart ${i + 1} (${chartPips[i]} Pip${chartPips[i] !== 1 ? "s" : ""})`));

        // Render all 4 charts
        charts.forEach((c, i) => {
            const pNum = parseFloat(chartPips[i]);
            let data = [];
            for (const [k, val] of Object.entries(result.charts || {})) {
                if (Math.abs(parseFloat(k) - pNum) < 1e-6) {
                    data = val;
                    break;
                }
            }
            c.setData(data);
        });

        const renderedBricks = Object.values(result.bricks_built || {}).reduce((s, v) => s + v, 0);
        const totalBricks = Object.values(result.total_bricks_built || result.bricks_built || {}).reduce((s, v) => s + v, 0);
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
        console.log(`Rendered ${renderedBricks.toLocaleString()} visible bricks from ${totalBricks.toLocaleString()} built bricks | Engine: ${result.engine_used}`);
        result.diagnostics?.forEach(d => logToUI(`[DIAG] ${d}`, "info"));

        chartsLoaded = true;
        statusText.textContent = `Charts ready — ${totalBricks.toLocaleString()} bricks`;
        statusText.textContent = `Charts ready - ${renderedBricks.toLocaleString()} visible / ${totalBricks.toLocaleString()} built bricks`;
        statusText.className = "value success";
        progressSection.classList.add("hidden");
        setPipelineStage(null);
    } catch (err) {
        console.error("Fetch result error:", err);
        statusText.textContent = `Result fetch error: ${err.message}`;
        statusText.className = "value warning";
    }
}

let cachePlaybackData = null;
let cachePlaybackSequence = [];
let cachePlaybackIndex = 0;
let cachePlaybackTime = null;
const MAX_LIVE_BUILD_BRICKS_PER_CHART = 20000;
let cachePlaybackLastRealTime = null;
let cachePlaybackTimer = null;
let stagedCache = null;
let lastConfirmedBricks = [];

async function loadStagedCacheAndPlay() {
    if (!stagedCache) return;
    const cache = stagedCache;
    stagedCache = null;

    statusText.textContent = "Loading staged cache…";
    statusText.className = "value warning";
    progressSection.classList.remove("hidden");
    setPipelineStage("cache");
    progressLabel.textContent = "Loading bricks from Parquet cache...";
    updateProgress(50, null, null, null);

    try {
        await _loadAndHoldCacheResult(cache.jobId, cache.chartPips, cache.startUtc, cache.endUtc);
        startCachePlayback();
    } catch (err) {
        console.error("Failed to load staged cache for playback:", err);
        stagedCache = cache;
        statusText.textContent = `Load cache error: ${err.message}`;
        statusText.className = "value error";
        progressSection.classList.add("hidden");
        setPipelineStage(null);
    }
}

async function loadStagedCacheAndStep() {
    if (!stagedCache) return;
    const cache = stagedCache;
    stagedCache = null;

    statusText.textContent = "Loading staged cache…";
    statusText.className = "value warning";
    progressSection.classList.remove("hidden");
    setPipelineStage("cache");
    progressLabel.textContent = "Loading bricks from Parquet cache...";
    updateProgress(50, null, null, null);

    try {
        await _loadAndHoldCacheResult(cache.jobId, cache.chartPips, cache.startUtc, cache.endUtc);
        stepCachePlayback();
    } catch (err) {
        console.error("Failed to load staged cache for step:", err);
        stagedCache = cache;
        statusText.textContent = `Load cache error: ${err.message}`;
        statusText.className = "value error";
        progressSection.classList.add("hidden");
        setPipelineStage(null);
    }
}

async function _loadAndHoldCacheResult(jobId, chartPips, startUtc = "", endUtc = "") {
    try {
        statusText.textContent = "Loading cache details…";
        statusText.className = "value warning";
        // The job's Parquet already holds exactly the built range, so we DON'T forward
        // start/end here: passing them makes the backend read every brick (millions for a
        // multi-year 1-pip build) and run a per-brick timestamp filter that blocks the
        // event loop. Omitting them takes the fast path — read only the most-recent capped
        // bricks — which avoids both the server stall and the browser OOM crash.
        const result = await RenkoAPI.getJobResult(jobId, "", "");
        
        cachePlaybackData = result.charts;
        currentBricksData = result.charts;
        currentTicksData = [];
        currentJobId = jobId;

        const consoleJobId = document.getElementById("consoleJobId");
        if (consoleJobId) consoleJobId.textContent = `Job: ${jobId.slice(0, 8)}…`;

        // Clear charts and update titles
        charts.forEach((c, i) => {
            c.clear();
            c.setTitle(`Chart ${i + 1} (${chartPips[i]} Pip${chartPips[i] !== 1 ? "s" : ""})`);
        });

        // Construct cache playback sequence
        cachePlaybackSequence = [];
        charts.forEach((c, i) => {
            const pNum = parseFloat(chartPips[i]);
            let data = [];
            for (const [k, val] of Object.entries(result.charts || {})) {
                if (Math.abs(parseFloat(k) - pNum) < 1e-6) {
                    data = val;
                    break;
                }
            }
            for (const brick of data) {
                const ts = Date.parse(String(brick.confirm_time).replace(" ", "T") + "Z") || Date.parse(String(brick.confirm_time)) || brick.time;
                cachePlaybackSequence.push({
                    chartIdx: i,
                    brick: brick,
                    timestamp: ts
                });
            }
        });

        // Sort sequence chronologically
        cachePlaybackSequence.sort((a, b) => a.timestamp - b.timestamp);
        cachePlaybackIndex = 0;
        cachePlaybackTime = cachePlaybackSequence.length > 0 ? cachePlaybackSequence[0].timestamp : null;

        const totalBricks = cachePlaybackSequence.length;
        ticksLoadedText.textContent  = (result.ticks_used || 0).toLocaleString();
        bricksBuiltText.textContent  = totalBricks.toLocaleString();
        dataEngineStatus.textContent = result.engine_used || "Disk Cache (Held)";

        console.log(`✅ Loaded cache: ${totalBricks.toLocaleString()} bricks | Ready for playback.`);
        result.diagnostics?.forEach(d => logToUI(`[DIAG] ${d}`, "info"));

        chartsLoaded = true;
        statusText.textContent = `Cache Loaded — ${totalBricks.toLocaleString()} bricks. Tap Play to playback.`;
        statusText.className = "value success";
        progressSection.classList.add("hidden");
        setPipelineStage(null);

        updatePlaybackMetrics({
            processed_ticks: 0,
            total_ticks: totalBricks,
            formed_bricks: 0,
            speed: 0
        });

    } catch (err) {
        console.error("Load cache error:", err);
        statusText.textContent = `Load cache error: ${err.message}`;
        statusText.className = "value warning";
        progressSection.classList.add("hidden");
        setPipelineStage(null);
        throw err;
    }
}

function startCachePlayback() {
    if (isPlaying) return;
    if (!cachePlaybackSequence || cachePlaybackSequence.length === 0) {
        statusText.textContent = "No cached bricks to play.";
        statusText.className = "value error";
        return;
    }

    isPlaying = true;
    statusText.textContent = "Playing from Parquet cache...";
    statusText.className = "value success";
    
    if (cachePlaybackIndex === 0) {
        charts.forEach(c => c.clear());
        cachePlaybackTime = cachePlaybackSequence[0].timestamp;
        lastConfirmedBricks = [];
    }
    
    cachePlaybackLastRealTime = performance.now();
    
    if (cachePlaybackTimer) cancelAnimationFrame(cachePlaybackTimer);
    cachePlaybackTimer = requestAnimationFrame(cachePlaybackLoop);
}

function getNextUpcomingBrick(chartIdx, startIndex) {
    for (let i = startIndex; i < cachePlaybackSequence.length; i++) {
        if (cachePlaybackSequence[i].chartIdx === chartIdx) {
            return cachePlaybackSequence[i];
        }
    }
    return null;
}

function cachePlaybackLoop() {
    if (!isPlaying) return;
    
    const now = performance.now();
    const deltaSec = (now - cachePlaybackLastRealTime) / 1000;
    cachePlaybackLastRealTime = now;
    
    const speed = getSelectedSpeed();
    cachePlaybackTime += deltaSec * 1000 * speed;
    
    let end = cachePlaybackIndex;
    while (end < cachePlaybackSequence.length && cachePlaybackSequence[end].timestamp <= cachePlaybackTime) {
        const ev = cachePlaybackSequence[end];
        const chart = charts[ev.chartIdx];
        if (chart) {
            chart.appendBrick(ev.brick);
            lastConfirmedBricks[ev.chartIdx] = ev;
        }
        end++;
    }
    
    cachePlaybackIndex = end;
    
    // Animate the forming/live bar for each chart's next upcoming brick
    charts.forEach((chart, chartIdx) => {
        if (!chart) return;
        const upcoming = getNextUpcomingBrick(chartIdx, cachePlaybackIndex);
        const lastConfirmed = lastConfirmedBricks[chartIdx];
        if (upcoming && lastConfirmed) {
            const startTime = lastConfirmed.timestamp;
            const endTime = upcoming.timestamp;
            const duration = endTime - startTime;
            if (duration > 0 && cachePlaybackTime >= startTime && cachePlaybackTime < endTime) {
                const ratio = (cachePlaybackTime - startTime) / duration;
                const O = Number(upcoming.brick.open);
                const H = Number(upcoming.brick.high);
                const L = Number(upcoming.brick.low);
                const C = Number(upcoming.brick.close);
                const direction = upcoming.brick.direction;
                
                let price = O;
                let runHigh = O;
                let runLow = O;
                
                const Target1 = (direction === "UP") ? L : H;
                const Target2 = (direction === "UP") ? H : L;
                
                if (ratio < 0.3) {
                    const t = ratio / 0.3;
                    price = O + t * (Target1 - O);
                    runHigh = Math.max(O, price);
                    runLow = Math.min(O, price);
                } else if (ratio < 0.7) {
                    const t = (ratio - 0.3) / 0.4;
                    price = Target1 + t * (Target2 - Target1);
                    runHigh = Math.max(O, Target1, price);
                    runLow = Math.min(O, Target1, price);
                } else {
                    const t = (ratio - 0.7) / 0.3;
                    price = Target2 + t * (C - Target2);
                    runHigh = Math.max(O, Target1, Target2, price);
                    runLow = Math.min(O, Target1, Target2, price);
                }
                
                chart.appendBrick({
                    open: O,
                    high: runHigh,
                    low: runLow,
                    close: price,
                    time: upcoming.brick.time,
                    confirm_time: upcoming.brick.confirm_time,
                    direction: direction,
                    is_live: true
                });
            }
        }
    });
    
    const currentBrick = end > 0 ? cachePlaybackSequence[end - 1].brick : null;
    const latestTime = currentBrick ? currentBrick.confirm_time : "";
    
    updatePlaybackMetrics({
        processed_ticks: cachePlaybackIndex,
        total_ticks: cachePlaybackSequence.length,
        formed_bricks: cachePlaybackIndex,
        speed: speed
    });
    
    statusText.textContent = `Cache Playback: ${speed.toLocaleString()}× | Bricks: ${cachePlaybackIndex.toLocaleString()} / ${cachePlaybackSequence.length.toLocaleString()}`;
    statusText.className = "value success";
    
    if (latestTime) {
        updateDebugPanel({ currentTickTime: latestTime });
    }
    
    if (cachePlaybackIndex >= cachePlaybackSequence.length) {
        isPlaying = false;
        cancelAnimationFrame(cachePlaybackTimer);
        cachePlaybackTimer = null;
        statusText.textContent = `Cache Playback Ended (${cachePlaybackSequence.length.toLocaleString()} bricks)`;
        statusText.className = "value success";
        return;
    }
    
    if (isPlaying) {
        cachePlaybackTimer = requestAnimationFrame(cachePlaybackLoop);
    }
}

function stepCachePlayback() {
    if (!cachePlaybackSequence || cachePlaybackSequence.length === 0) return;
    
    isPlaying = false;
    if (cachePlaybackTimer) {
        cancelAnimationFrame(cachePlaybackTimer);
        cachePlaybackTimer = null;
    }
    
    if (cachePlaybackIndex >= cachePlaybackSequence.length) return;
    
    if (cachePlaybackIndex === 0) {
        charts.forEach(c => c.clear());
        lastConfirmedBricks = [];
    }
    
    const ev = cachePlaybackSequence[cachePlaybackIndex];
    const chart = charts[ev.chartIdx];
    if (chart) {
        chart.appendBrick(ev.brick);
        lastConfirmedBricks[ev.chartIdx] = ev;
    }
    cachePlaybackIndex++;
    cachePlaybackTime = ev.timestamp;
    
    updatePlaybackMetrics({
        processed_ticks: cachePlaybackIndex,
        total_ticks: cachePlaybackSequence.length,
        formed_bricks: cachePlaybackIndex,
        speed: 0
    });
    
    statusText.textContent = `Step Cache: Bricks ${cachePlaybackIndex} / ${cachePlaybackSequence.length}`;
    statusText.className = "value success";
    if (ev.brick.confirm_time) {
        updateDebugPanel({ currentTickTime: ev.brick.confirm_time });
    }
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
function initSidebarCollapsibles() {
    const defaultClosedTitles = [
        "Chart & Build",
        "Layout & Theme",
        "Cache & Export",
    ];

    document.querySelectorAll(".sidebar .sidebar-card").forEach((card, index) => {
        const header = card.querySelector(":scope > .card-header");
        const body = card.querySelector(":scope > .card-body");
        const title = header?.querySelector("h2");
        if (!header || !body || !title || header.querySelector(".section-toggle")) return;

        const cleanTitle = title.textContent.replace(/\s+/g, " ").trim();
        const bodyId = body.id || `sidebarSectionBody${index + 1}`;
        body.id = bodyId;

        const button = document.createElement("button");
        button.type = "button";
        button.className = "section-toggle";
        button.setAttribute("aria-controls", bodyId);

        const chevron = document.createElement("span");
        chevron.className = "section-chevron";
        chevron.setAttribute("aria-hidden", "true");
        chevron.textContent = ">";

        button.appendChild(title);
        button.appendChild(chevron);
        header.appendChild(button);

        const startsClosed = defaultClosedTitles.some((text) => cleanTitle.includes(text));
        card.classList.toggle("is-collapsed", startsClosed);
        button.setAttribute("aria-expanded", startsClosed ? "false" : "true");

        button.addEventListener("click", () => {
            const collapsed = card.classList.toggle("is-collapsed");
            button.setAttribute("aria-expanded", collapsed ? "false" : "true");
        });
    });
}

window.addEventListener("DOMContentLoaded", () => {
    console.log("RenkoTerminal initializing…");

    // Bind all DOM refs
    csvPathInput         = document.getElementById("csvPathInput");
    loadMetadataBtn      = document.getElementById("loadMetadataBtn");
    rangeCard            = document.getElementById("rangeCard");
    dateRangeSlider      = document.getElementById("dateRangeSlider");
    selectedRangeText    = document.getElementById("selectedRangeText");
    processingEngineSelect = document.getElementById("processingEngineSelect");

    pipAddInput          = document.getElementById("pipAddInput");
    pipListInput         = document.getElementById("pipListInput");
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

    initSidebarCollapsibles();

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

    // Init charts dynamically (default: 1, 2, 3, 4 pip)
    buildChartUI([1, 2, 3, 4]);

    // Pip list management
    document.getElementById("addChartBtn")?.addEventListener("click", () => {
        const val = parseFloat(pipAddInput?.value);
        if (isNaN(val) || val <= 0) { console.warn("[CHARTS] Invalid pip size."); return; }
        buildChartUI([...pipList, val]);
    });
    document.getElementById("applyPipListBtn")?.addEventListener("click", () => {
        const pips = parsePipList(pipListInput?.value || "");
        if (pips.length === 0) { console.warn("[CHARTS] No valid pip sizes in list."); return; }
        buildChartUI(pips);
    });
    document.getElementById("resetDefaultPipsBtn")?.addEventListener("click", () => {
        buildChartUI([1, 2, 3, 4]);
    });
    document.getElementById("removeAllPipsBtn")?.addEventListener("click", () => {
        if (confirm("Remove all charts?")) buildChartUI([]);
    });
    // Also apply on Enter key in pipListInput
    pipListInput?.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
            const pips = parsePipList(pipListInput.value || "");
            if (pips.length > 0) buildChartUI(pips);
        }
    });

    // Cache lookup panel buttons
    document.getElementById("loadCacheBtn")?.addEventListener("click", () => {
        if (!_lastCacheLookup || (!_lastCacheLookup.exact_match && !_lastCacheLookup.sub_range_match)) {
            return;
        }
        const jobId = _lastCacheLookup.job_id;
        const chartPips = getCurrentPips();

        stopPlayback();
        
        stagedCache = {
            jobId: jobId,
            chartPips: chartPips,
            startUtc: selectedStartUtc,
            endUtc: selectedEndUtc,
            metadata: _lastCacheLookup.metadata
        };

        // Clear charts & update titles
        charts.forEach((c, i) => {
            c.clear();
            c.setTitle(`Chart ${i + 1} (${chartPips[i]} Pip${chartPips[i] !== 1 ? "s" : ""}) [Staged]`);
        });
        chartsLoaded = false;

        // Update indicators
        ticksLoadedText.textContent  = (_lastCacheLookup.metadata?.ticks_used || 0).toLocaleString();
        bricksBuiltText.textContent  = "Staged";
        dataEngineStatus.textContent = "Staged (" + (_lastCacheLookup.metadata?.engine_used || "Disk Cache") + ")";

        statusText.textContent = "⚡ Cache staged. Tap Play/Step to start.";
        statusText.className = "value success";

        progressSection.classList.add("hidden");
        setPipelineStage(null);
    });
    document.getElementById("ignoreCacheBuildBtn")?.addEventListener("click", () => {
        document.getElementById("cacheReuseCard")?.classList.add("hidden");
        runBuildCharts();
    });
    document.getElementById("similarCacheList")?.addEventListener("click", (e) => {
        const item = e.target.closest(".cache-similar-item");
        if (!item || !_lastCacheLookup) return;
        const idx = parseInt(item.dataset.idx);
        const s = (_lastCacheLookup.similar_matches || [])[idx];
        if (!s) return;
        // Apply chart pips from the selected similar build
        buildChartUI(s.chart_pips || []);
        // Trigger lookup again with the new pips
        checkCacheLookup();
    });

    // Auto-update cache lookup on parameter changes
    priceSourceSelect?.addEventListener("change", () => checkCacheLookup());
    reversalBoxes?.addEventListener("change", () => checkCacheLookup());
    pipSize?.addEventListener("change", () => checkCacheLookup());
    anchorModeSelect?.addEventListener("change", () => checkCacheLookup());

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
        const btn = document.getElementById("syncZoomBtn");
        if (btn) btn.classList.toggle("active", enabled);
        console.log(`[SYSTEM] Zoom sync ${enabled ? "enabled" : "disabled"}.`);
    });

    // Sync Zoom toggle button (mirrors syncChartsCheckbox)
    document.getElementById("syncZoomBtn")?.addEventListener("click", (e) => {
        const btn = e.currentTarget;
        const nowActive = !btn.classList.contains("active");
        btn.classList.toggle("active", nowActive);
        syncChartsCheckbox.checked = nowActive;
        RenkoCharts.setSyncEnabled(nowActive);
        console.log(`[SYSTEM] Zoom sync ${nowActive ? "enabled" : "disabled"}.`);
    });

    // Sync Crosshair toggle button
    document.getElementById("syncCrosshairBtn")?.addEventListener("click", (e) => {
        const btn = e.currentTarget;
        const nowActive = !btn.classList.contains("active");
        btn.classList.toggle("active", nowActive);
        RenkoCharts.setCrosshairSyncEnabled(nowActive);
        console.log(`[SYSTEM] Crosshair sync ${nowActive ? "enabled" : "disabled"}.`);
    });

    document.getElementById("clearConsoleBtn")?.addEventListener("click", () => {
        if (consoleLogOutput) {
            consoleLogOutput.innerHTML = "";
            logToUI("[SYSTEM] Terminal cleared.", "system");
        }
    });

    // Console header click to expand/collapse
    const consoleHeader = document.querySelector(".console-header");
    consoleHeader?.addEventListener("click", (e) => {
        if (e.target.closest("#clearConsoleBtn") || e.target.closest("button")) return;
        const consoleEl = document.querySelector(".system-console");
        consoleEl?.classList.toggle("collapsed");
    });

    // Theme Switcher
    document.getElementById("themeSwitcher")?.addEventListener("change", (e) => {
        applyAppTheme(e.target.value);
    });

    // Layout buttons (query .chart-wrapper live so they work after rebuilds)
    document.getElementById("layout2x2Btn")?.addEventListener("click", () => {
        const g = document.getElementById("chartsGrid");
        g.classList.remove("layout-stack", "layout-focus");
        document.querySelectorAll(".chart-wrapper").forEach(w => w.classList.remove("focused"));
        resizeAllCharts();
    });
    document.getElementById("layoutStackBtn")?.addEventListener("click", () => {
        const g = document.getElementById("chartsGrid");
        g.classList.add("layout-stack");
        g.classList.remove("layout-focus");
        document.querySelectorAll(".chart-wrapper").forEach(w => w.classList.remove("focused"));
        resizeAllCharts();
    });
    document.getElementById("layoutFocusBtn")?.addEventListener("click", () => {
        window.toggleMaximizeChart(activeChartIndex);
    });
    document.getElementById("layoutRestoreBtn")?.addEventListener("click", () => {
        const g = document.getElementById("chartsGrid");
        g.classList.remove("layout-stack", "layout-focus");
        document.querySelectorAll(".chart-wrapper").forEach(w => w.classList.remove("focused"));
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

    // Left Sidebar toggle
    const toggleSidebarBtn = document.getElementById("toggleSidebarBtn");
    const sidebarOverlay = document.getElementById("sidebarOverlay");
    
    function toggleLeftSidebar() {
        const container = document.querySelector(".app-container");
        const overlay = document.getElementById("sidebarOverlay");
        if (window.innerWidth <= 1024) {
            // Mobile/Tablet mode: toggle slide-out drawer
            container.classList.toggle("show-left-drawer");
            overlay.classList.toggle("active", container.classList.contains("show-left-drawer"));
        } else {
            // Desktop mode: collapse sidebar in grid
            container.classList.toggle("hide-left");
            resizeAllCharts();
        }
    }
    
    toggleSidebarBtn?.addEventListener("click", toggleLeftSidebar);
    sidebarOverlay?.addEventListener("click", () => {
        const container = document.querySelector(".app-container");
        const overlay = document.getElementById("sidebarOverlay");
        container.classList.remove("show-left-drawer");
        overlay.classList.remove("active");
    });

    // Note: chart hover listeners and brick click callbacks are wired in buildChartUI → setupChartHoverListeners + setupCrosshairDetails
    
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
                if (preset.pip_list !== undefined) {
                    buildChartUI(preset.pip_list);
                } else if (preset.chart1_pips !== undefined) {
                    // backward compat with old 4-chart presets
                    const pips = [preset.chart1_pips, preset.chart2_pips, preset.chart3_pips, preset.chart4_pips].filter(v => v != null && !isNaN(v));
                    if (pips.length > 0) buildChartUI(pips);
                }
                if (preset.price_source !== undefined) priceSourceSelect.value = preset.price_source;
                if (preset.reversal_boxes !== undefined) reversalBoxes.value = preset.reversal_boxes;
                if (preset.pip_size !== undefined) pipSize.value = preset.pip_size;
                if (preset.anchor !== undefined) anchorModeSelect.value = preset.anchor;
                if (preset.chunk_size_mb !== undefined && chunkSizeMb) chunkSizeMb.value = preset.chunk_size_mb;
                if (preset.processing_engine !== undefined) processingEngineSelect.value = preset.processing_engine;
                if (preset.playback_speed !== undefined) {
                    playbackSpeed.value = preset.playback_speed;
                    if (playbackSpeedValue) playbackSpeedValue.textContent = `${preset.playback_speed}×`;
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

                if (preset.indicators) {
                    Object.entries(preset.indicators).forEach(([id, visible]) => {
                        const chk = document.getElementById(`toggle_${id}`);
                        if (chk) chk.checked = visible;
                        charts.forEach(c => {
                            const series = c.indicatorSeries[id];
                            if (series) {
                                series.applyOptions({ visible: visible });
                            }
                        });
                    });
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
    // Start backend heartbeat — closes/flags this tab when the server is shut down
    startHeartbeat();

    // Initialize dynamic indicators framework UI components
    initializeDynamicIndicators();

    // Auto-layout switching based on screen width
    let lastWidth = window.innerWidth;
    const handleScreenResize = debounce(() => {
        const currentWidth = window.innerWidth;
        const grid = document.getElementById("chartsGrid");
        if (!grid) return;

        if (currentWidth <= 768) {
            if (!grid.classList.contains("layout-stack")) {
                grid.classList.add("layout-stack");
                grid.classList.remove("layout-focus");
                document.querySelectorAll(".chart-wrapper").forEach(w => w.classList.remove("focused"));
                console.log("[LAYOUT] Mobile auto-switch: Stack layout activated.");
            }
        } else if (currentWidth <= 1024) {
            if (!grid.classList.contains("layout-stack") && !grid.classList.contains("layout-focus")) {
                grid.classList.add("layout-stack");
                console.log("[LAYOUT] Tablet auto-switch: Stack layout activated.");
            }
        } else {
            if (grid.classList.contains("layout-stack") && lastWidth <= 1024) {
                grid.classList.remove("layout-stack");
                console.log("[LAYOUT] Desktop auto-switch: Grid layout restored.");
            }
        }
        
        lastWidth = currentWidth;
        resizeAllCharts();
    }, 150);
    window.addEventListener("resize", handleScreenResize);
    
    // Trigger initial check
    setTimeout(() => {
        const grid = document.getElementById("chartsGrid");
        if (grid) {
            const w = window.innerWidth;
            if (w <= 768) {
                grid.classList.add("layout-stack");
            } else if (w <= 1024) {
                grid.classList.add("layout-stack");
            }
            resizeAllCharts();
        }
    }, 100);

    console.log("RenkoTerminal ready ✅");
});


// ─── Playback Metrics, Themes, layouts, and Measure tool helpers ───────────
let activeChartIndex = 1;
let isMeasureEnabled = false;
let measureStartBrick = null;
let measureEndBrick = null;
let measureChartIndex = null;

function getSelectedSpeed() {
    return sliderToMultiplier(playbackSpeed.value);
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
        metricsSpeedSet.textContent = `${speedSetVal}×`;
    }

    const metricsActualSpeed = document.getElementById("metricsActualSpeed");
    if (metricsActualSpeed) {
        metricsActualSpeed.textContent = `${msg.speed !== undefined ? msg.speed : speedSetVal}×`;
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
            detailStr = msg.engine_confirmed_counts.map((c, idx) => `${pipList[idx] !== undefined ? pipList[idx] : '?'}p: ${c}`).join(" | ");
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
    const grid    = document.getElementById("chartsGrid");
    const wrappers = document.querySelectorAll(".chart-wrapper");
    const wrapper  = document.getElementById(`chartWrapper${index}`);
    
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
        pip_list: [...pipList],
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
        show_wicks: true,
        indicators: {}
    };
    Object.keys(window.INDICATOR_REGISTRY || {}).forEach(id => {
        const chk = document.getElementById(`toggle_${id}`);
        if (chk) {
            preset.indicators[id] = chk.checked;
        }
    });
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

                // Reset dynamic indicators
                Object.keys(window.INDICATOR_REGISTRY || {}).forEach(id => {
                    const el = document.getElementById(`cursor_${id}`);
                    if (el) el.textContent = "—";
                });

                return;
            }

            const pip = pipList[idx] !== undefined ? pipList[idx] : 1;
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

            // Dynamic indicators cursor display
            Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
                const el = document.getElementById(`cursor_${id}`);
                if (el) {
                    let val = brick.indicators ? brick.indicators[id] : undefined;
                    if (val === undefined && brick.time !== undefined) {
                        const meta = metaByChart[chart.chartId]?.get(brick.time);
                        if (meta && meta.indicators) {
                            val = meta.indicators[id];
                        }
                    }
                    if (val !== undefined && val !== null) {
                        el.textContent = val.toFixed(prec);
                    } else {
                        el.textContent = "—";
                    }
                }
            });

        };
    });
}

function initializeDynamicIndicators() {
    // 1. Sidebar Toggles
    const togglesContainer = document.getElementById("dynamicIndicatorToggles");
    if (togglesContainer) {
        togglesContainer.innerHTML = "";
        Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
            const row = document.createElement("div");
            row.className = "toggle-row";
            row.innerHTML = `
                <label for="toggle_${id}">${config.name}</label>
                <input type="checkbox" id="toggle_${id}" ${config.visible ? 'checked' : ''} class="toggle-check" />
            `;
            togglesContainer.appendChild(row);

            document.getElementById(`toggle_${id}`)?.addEventListener("change", (e) => {
                const checked = e.target.checked;
                charts.forEach(c => {
                    const series = c.indicatorSeries[id];
                    if (series) {
                        series.applyOptions({ visible: checked });
                    }
                });
                console.log(`[SYSTEM] ${config.name} visibility set to ${checked}.`);
            });
        });
    }

    // 2. Cursor Data Panel Rows
    const cursorDataGrid = document.getElementById("cursorDataGrid");
    if (cursorDataGrid) {
        // Remove any old indicators rows first
        const oldRows = cursorDataGrid.querySelectorAll(".indicator-row");
        oldRows.forEach(r => r.remove());

        Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
            const row = document.createElement("div");
            row.className = "data-row indicator-row";
            row.innerHTML = `<span class="label">${config.name}</span><span class="val" id="cursor_${id}">—</span>`;
            cursorDataGrid.appendChild(row);
        });
    }
}
