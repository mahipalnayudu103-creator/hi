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
