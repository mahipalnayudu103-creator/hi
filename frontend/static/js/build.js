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

