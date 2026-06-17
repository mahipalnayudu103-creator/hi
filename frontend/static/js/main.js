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
