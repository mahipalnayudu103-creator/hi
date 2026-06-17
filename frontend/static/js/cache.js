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

