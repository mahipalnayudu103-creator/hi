const metaByChart = {
    1: new Map(),
    2: new Map(),
    3: new Map(),
    4: new Map()
};

const xValuesByChart = {
    1: [],
    2: [],
    3: [],
    4: []
};

function findNearestX(chartId, x) {
    const arr = xValuesByChart[chartId];
    if (!arr || arr.length === 0) return null;

    let lo = 0;
    let hi = arr.length - 1;

    if (x <= arr[0]) return arr[0];
    if (x >= arr[hi]) return arr[hi];

    while (lo <= hi) {
        const mid = Math.floor((lo + hi) / 2);
        if (arr[mid] === x) return arr[mid];
        if (arr[mid] < x) lo = mid + 1;
        else hi = mid - 1;
    }

    return arr[Math.max(0, hi)];
}

function getConfirmTimeForAxis(chartId, x) {
    const exact = metaByChart[chartId].get(Number(x));
    if (exact) return exact.confirm_time;

    const nearestX = findNearestX(chartId, Number(x));
    if (nearestX === null) return null;

    const nearest = metaByChart[chartId].get(nearestX);
    return nearest?.confirm_time ?? null;
}

function getNearestPriceByX(chartId, x) {
    const nearestX = findNearestX(chartId, Number(x));
    if (nearestX === null) return null;

    const meta = metaByChart[chartId].get(nearestX);
    return meta?.close ?? null;
}

function formatUtcTime(isoTime) {
    if (!isoTime) return "forming";
    const d = new Date(isoTime);
    if (isNaN(d.getTime())) return String(isoTime);
    return d.toISOString().replace("T", " ").replace("Z", " UTC");
}

function getSymbolFromUI() {
    const csvPathInput = document.getElementById("csvPathInput");
    if (!csvPathInput) return "EURUSD";
    const path = csvPathInput.value.trim();
    if (!path) return "EURUSD";
    const filename = path.split(/[/\\]/).pop().toUpperCase();
    if (filename.includes("JPY")) return "USDJPY";
    if (filename.includes("BTC")) return "BTCUSD";
    if (filename.includes("XAU") || filename.includes("GOLD")) return "XAUUSD";
    const match = filename.match(/[A-Z]{6}/);
    if (match) return match[0];
    return "EURUSD";
}

function getPriceFormat(symbol) {
    const s = symbol.toUpperCase();
    if (s.includes("JPY")) {
        return { type: "price", precision: 3, minMove: 0.001 };
    }
    if (s.includes("XAU") || s.includes("GOLD") || s.includes("BTC")) {
        return { type: "price", precision: 2, minMove: 0.01 };
    }
    return { type: "price", precision: 5, minMove: 0.00001 };
}

class RenkoChart {
    constructor(containerId, legendId, titleId, chartId) {
        this.containerId = containerId;
        this.legendId = legendId;
        this.titleId = titleId;
        this.chartId = chartId;
        this.container = document.getElementById(containerId);
        this.legend = document.getElementById(legendId);
        this.title = document.getElementById(titleId);
        this.wrapper = this.container.closest('.chart-wrapper');
        this.bricks = [];
        this.autoScroll = true;
        this.currentPriceLine = null;
        this.reversalEnabled = true;
        this.separatorsEnabled = false;
        
        const symbol = getSymbolFromUI();
        const priceFormat = getPriceFormat(symbol);
        this.precision = priceFormat.precision;
        this.minMove = priceFormat.minMove;
        this.onCrosshairChangeCallback = null;
        this.onBrickClickCallback = null;
        this.pipSize = 0.0001;
        
        // Create floating tooltip card inside this container
        this.tooltip = document.createElement('div');
        this.tooltip.className = 'floating-tooltip-card';
        this.tooltip.style.position = 'absolute';
        this.tooltip.style.display = 'none';
        this.tooltip.style.zIndex = '100';
        this.tooltip.style.pointerEvents = 'none';
        this.tooltip.style.backgroundColor = 'rgba(15, 23, 42, 0.9)';
        this.tooltip.style.border = '1px solid #334155';
        this.tooltip.style.borderRadius = '6px';
        this.tooltip.style.padding = '8px 12px';
        this.tooltip.style.color = '#cbd5e1';
        this.tooltip.style.fontFamily = 'Inter, sans-serif';
        this.tooltip.style.fontSize = '12px';
        this.tooltip.style.lineHeight = '1.4';
        this.tooltip.style.boxShadow = '0 10px 15px -3px rgba(0, 0, 0, 0.5)';
        this.container.style.position = 'relative';
        this.container.appendChild(this.tooltip);

        this.chart = LightweightCharts.createChart(this.container, {
            layout: {
                background: { type: 'solid', color: '#0b0f19' },
                textColor: '#cbd5e1',
                fontSize: 11,
                fontFamily: 'Inter, sans-serif',
            },
            grid: {
                vertLines: { color: '#1f2937' },
                horzLines: { color: '#1f2937' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    visible: true,
                    labelVisible: false
                },
                horzLine: {
                    visible: true,
                    labelVisible: true
                }
            },
            rightPriceScale: {
                visible: true,
                borderVisible: true,
                autoScale: true,
                borderColor: '#374151',
            },
            leftPriceScale: {
                visible: false
            },
            timeScale: {
                visible: true,
                timeVisible: false,
                secondsVisible: false,
                borderVisible: true,
                borderColor: '#374151',
                rightOffset: 10,
                barSpacing: 6,
                fixLeftEdge: true,
                fixRightEdge: false,
                tickMarkFormatter: () => ""
            },
        });
        
        this.candlestickSeries = this.chart.addSeries(LightweightCharts.CandlestickSeries, {
            priceFormat: priceFormat,
            upColor: '#22c55e',
            downColor: '#ef4444',
            borderUpColor: '#16a34a',
            borderDownColor: '#dc2626',
            wickUpColor: '#16a34a',
            wickDownColor: '#dc2626'
        });
        
        this.setupCrosshairListener();
        this.setupClickListener();
        this.setupDoubleClickListener();

        // Responsive chart resize listener using ResizeObserver
        this.resizeObserver = new ResizeObserver(entries => {
            if (!entries || entries.length === 0) return;
            const { width, height } = entries[0].contentRect;
            if (width > 0 && height > 0) {
                requestAnimationFrame(() => {
                    try {
                        this.chart.resize(width, height);
                    } catch (e) {
                        console.warn("Resize failed:", e);
                    }
                });
            }
        });
        this.resizeObserver.observe(this.container);
        
        if (!window.RenkoCharts) {
            window.RenkoCharts = {};
        }
        if (!window.RenkoCharts.allInstances) {
            window.RenkoCharts.allInstances = [];
        }
        window.RenkoCharts.allInstances.push(this);
    }
    
    applyTheme(theme) {
        this.chart.applyOptions({
            layout: {
                background: { type: 'solid', color: theme.chartBg },
                textColor: theme.textColor,
            },
            grid: {
                vertLines: { color: theme.vertLines },
                horzLines: { color: theme.horzLines },
            },
            rightPriceScale: {
                borderColor: theme.borderColor,
            },
            timeScale: {
                borderColor: theme.borderColor,
            }
        });
    }

    setPrecision(precision, minMove) {
        this.precision = parseInt(precision, 10);
        this.minMove = parseFloat(minMove);
        this.candlestickSeries.applyOptions({
            priceFormat: {
                type: 'price',
                precision: this.precision,
                minMove: this.minMove,
            }
        });
    }
    
    setData(bricks) {
        if (metaByChart[this.chartId]) {
            metaByChart[this.chartId].clear();
        }
        xValuesByChart[this.chartId] = [];

        const symbol = getSymbolFromUI();
        const priceFormat = getPriceFormat(symbol);
        this.precision = priceFormat.precision;
        this.minMove = priceFormat.minMove;
        this.candlestickSeries.applyOptions({
            priceFormat: priceFormat
        });

        this.bricks = bricks.map((brick) => {
            const x = Number(brick.brick_index ?? brick.time);
            
            metaByChart[this.chartId].set(x, {
                confirm_time: brick.confirm_time,
                direction: brick.direction,
                tick_count: brick.tick_count,
                open: Number(brick.open),
                high: Number(brick.high),
                low: Number(brick.low),
                close: Number(brick.close)
            });
            
            xValuesByChart[this.chartId].push(x);
            
            brick.time = x;
            if (brick.timeMs === undefined) {
                brick.timeMs = brick.confirm_time ? new Date(brick.confirm_time).getTime() : Date.now();
            }
            return brick;
        });

        xValuesByChart[this.chartId].sort((a, b) => a - b);

        const seriesData = this.bricks.map(b => ({
            time: b.time,
            open: Number(b.open),
            high: Number(b.high),
            low: Number(b.low),
            close: Number(b.close)
        }));
        this.candlestickSeries.setData(seriesData);
        
        if (this.bricks.length > 0) {
            const last = this.bricks[this.bricks.length - 1];
            this.updatePriceLine(last.close, last.direction);
        } else {
            this.removePriceLine();
        }
        this.updateMarkers();
    }
    
    appendBrick(brick) {
        const isLive = !!brick.is_live;
        const x = Number(brick.brick_index ?? brick.time);
        brick.time = x;

        metaByChart[this.chartId].set(x, {
            confirm_time: brick.confirm_time,
            direction: brick.direction,
            tick_count: brick.tick_count,
            open: Number(brick.open),
            high: Number(brick.high),
            low: Number(brick.low),
            close: Number(brick.close)
        });

        if (!xValuesByChart[this.chartId].includes(x)) {
            xValuesByChart[this.chartId].push(x);
            xValuesByChart[this.chartId].sort((a, b) => a - b);
        }

        if (brick.timeMs === undefined) {
            brick.timeMs = brick.confirm_time ? new Date(brick.confirm_time).getTime() : Date.now();
        }
        
        if (this.bricks.length > 0 && this.bricks[this.bricks.length - 1].time === brick.time) {
            this.bricks[this.bricks.length - 1] = brick;
        } else {
            this.bricks.push(brick);
        }
        
        this.candlestickSeries.update({
            time: brick.time,
            open: Number(brick.open),
            high: Number(brick.high),
            low: Number(brick.low),
            close: Number(brick.close)
        });

        // Current price line (on latest tick close)
        this.updatePriceLine(brick.close, brick.direction);

        // Update markers periodically or when confirmed
        if (!isLive) {
            this.updateMarkers();
        }

        if (this.autoScroll) {
            this.chart.timeScale().scrollToPosition(0, true);
        }
    }
    
    updatePriceLine(price, direction) {
        this.removePriceLine();
        const color = direction === "up" ? '#22c55e' : '#fb7185';
        this.currentPriceLine = this.candlestickSeries.createPriceLine({
            price: price,
            color: color,
            lineWidth: 1.5,
            lineStyle: LightweightCharts.LineStyle.Dashed,
            axisLabelVisible: true,
            title: 'Current Price',
        });
    }

    removePriceLine() {
        if (this.currentPriceLine) {
            this.candlestickSeries.removePriceLine(this.currentPriceLine);
            this.currentPriceLine = null;
        }
    }

    updateMarkers() {
        const markers = [];
        if (this.bricks.length === 0) return;

        // Compile date/session separators and reversals
        for (let i = 0; i < this.bricks.length; i++) {
            const brick = this.bricks[i];
            
            // Session/Date separators
            if (this.separatorsEnabled && i > 0) {
                const prev = this.bricks[i - 1];
                const dateCurr = new Date(brick.timeMs);
                const datePrev = new Date(prev.timeMs);
                
                // Day separator
                if (dateCurr.getUTCDate() !== datePrev.getUTCDate()) {
                    const dayStr = dateCurr.toUTCString().slice(5, 11); // e.g. "11 Jun"
                    markers.push({
                        time: brick.time,
                        position: 'belowBar',
                        color: '#3b82f6',
                        shape: 'square',
                        text: dayStr
                    });
                } else {
                    // Session Separators (London 08:00 UTC, New York 13:00 UTC, Tokyo 00:00 UTC)
                    const hCurr = dateCurr.getUTCHours();
                    const hPrev = datePrev.getUTCHours();
                    if (hCurr >= 8 && hPrev < 8) {
                        markers.push({ time: brick.time, position: 'belowBar', color: '#f59e0b', shape: 'circle', text: 'London' });
                    } else if (hCurr >= 13 && hPrev < 13) {
                        markers.push({ time: brick.time, position: 'belowBar', color: '#8b5cf6', shape: 'circle', text: 'New York' });
                    }
                }
            }

            // Reversal markers
            if (this.reversalEnabled && i > 0) {
                const prev = this.bricks[i - 1];
                if (brick.direction !== prev.direction && brick.direction !== "none") {
                    markers.push({
                        time: brick.time,
                        position: 'aboveBar',
                        color: '#c084fc',
                        shape: 'circle',
                        text: 'Reversal'
                    });
                }
            }
        }

        // Add "Latest" marker on the last confirmed brick
        const lastConfirmedIdx = this.bricks.findIndex(b => !b.is_live);
        const lastBrick = lastConfirmedIdx !== -1 ? this.bricks[this.bricks.length - 1] : this.bricks[this.bricks.length - 1];
        if (lastBrick && !lastBrick.is_live) {
            markers.push({
                time: lastBrick.time,
                position: lastBrick.direction === "up" ? 'aboveBar' : 'belowBar',
                color: lastBrick.direction === "up" ? '#22c55e' : '#fb7185',
                shape: lastBrick.direction === "up" ? 'arrowUp' : 'arrowDown',
                text: 'Latest'
            });
        }

        // Sort markers by time/logical index to satisfy TV library constraint
        markers.sort((a, b) => a.time - b.time);
        
        if (!this.markersApi) {
            this.markersApi = LightweightCharts.createSeriesMarkers(this.candlestickSeries, markers);
        } else {
            this.markersApi.setMarkers(markers);
        }
    }
    
    clear() {
        this.bricks = [];
        this.candlestickSeries.setData([]);
        if (this.markersApi) {
            this.markersApi.setMarkers([]);
        }
        this.removePriceLine();
        this.updateLegend(null);
        if (metaByChart[this.chartId]) {
            metaByChart[this.chartId].clear();
        }
        xValuesByChart[this.chartId] = [];
    }
    
    updateLegend(brick) {
        if (!brick) {
            this.legend.textContent = "O: - H: - L: - C: -";
            return;
        }
        const prec = this.precision;
        this.legend.textContent = `O: ${brick.open.toFixed(prec)} H: ${brick.high.toFixed(prec)} L: ${brick.low.toFixed(prec)} C: ${brick.close.toFixed(prec)}`;
    }
    
    setTitle(text) {
        this.title.textContent = text;
    }
    
    formatPrice(price) {
        if (price === undefined || price === null) return "-";
        return Number(price).toFixed(this.precision);
    }

    setupCrosshairListener() {
        this.chart.subscribeCrosshairMove((param) => {
            const hasData = param && param.time !== undefined && param.point !== undefined;
            if (!hasData) {
                this.updateLegend(null);
                this.tooltip.style.display = "none";
                const globalTooltip = document.getElementById("hoverTooltip");
                if (globalTooltip) globalTooltip.classList.add("hidden");
                if (this.onCrosshairChangeCallback) {
                    this.onCrosshairChangeCallback(null);
                }
                return;
            }
            
            const ohlc = param.seriesData.get(this.candlestickSeries);
            if (!ohlc) {
                this.tooltip.style.display = "none";
                const globalTooltip = document.getElementById("hoverTooltip");
                if (globalTooltip) globalTooltip.classList.add("hidden");
                return;
            }
            
            const x = Number(param.time);
            const meta = metaByChart[this.chartId].get(x);
            const confirmTime = meta?.confirm_time ?? getConfirmTimeForAxis(this.chartId, x);
            
            this.updateLegend(ohlc);
            
            this.tooltip.innerHTML = `
                <div><b>${this.chartId} Pip Renko</b></div>
                <div>Time: ${formatUtcTime(confirmTime)}</div>
                <div>Open: ${this.formatPrice(ohlc.open)}</div>
                <div>High: ${this.formatPrice(ohlc.high)}</div>
                <div>Low: ${this.formatPrice(ohlc.low)}</div>
                <div>Close: ${this.formatPrice(ohlc.close)}</div>
                <div>Direction: ${meta?.direction ? (meta.direction.charAt(0).toUpperCase() + meta.direction.slice(1)) : "-"}</div>
                <div>Ticks: ${meta?.tick_count ?? "-"}</div>
            `;
            this.tooltip.style.display = "block";
            
            // Position tooltip
            const toolTipWidth = 180;
            const toolTipHeight = 170;
            const toolTipMargin = 15;
            
            let left = param.point.x + toolTipMargin;
            if (left + toolTipWidth > this.container.clientWidth) {
                left = param.point.x - toolTipWidth - toolTipMargin;
            }
            
            let top = param.point.y + toolTipMargin;
            if (top + toolTipHeight > this.container.clientHeight) {
                top = param.point.y - toolTipHeight - toolTipMargin;
            }
            
            this.tooltip.style.left = left + 'px';
            this.tooltip.style.top = top + 'px';

            // Update fallback global tooltip in the status bar
            const globalTooltip = document.getElementById("hoverTooltip");
            if (globalTooltip) {
                globalTooltip.classList.remove("hidden");
                const tooltipConfirmTime = document.getElementById("tooltipConfirmTime");
                if (tooltipConfirmTime) tooltipConfirmTime.textContent = `Time: ${formatUtcTime(confirmTime)}`;
                const tooltipTickCount = document.getElementById("tooltipTickCount");
                if (tooltipTickCount) tooltipTickCount.textContent = `Ticks: ${meta?.tick_count ?? "-"}`;
            }

            if (this.onCrosshairChangeCallback) {
                const brickData = this.bricks.find(b => b.time === x) || {
                    time: x,
                    confirm_time: confirmTime,
                    direction: meta?.direction,
                    tick_count: meta?.tick_count,
                    open: ohlc.open,
                    high: ohlc.high,
                    low: ohlc.low,
                    close: ohlc.close
                };
                this.onCrosshairChangeCallback(brickData);
            }
        });
    }

    setupClickListener() {
        this.chart.subscribeClick((param) => {
            if (!param.time || !this.onBrickClickCallback) return;
            const brick = this.bricks.find(b => b.time === param.time);
            if (brick) {
                this.onBrickClickCallback(brick);
            }
        });
    }

    setupDoubleClickListener() {
        this.container.addEventListener('dblclick', () => {
            if (typeof window.toggleMaximizeChart === 'function') {
                const chartsList = window.RenkoCharts.allInstances;
                const index = chartsList.indexOf(this) + 1;
                window.toggleMaximizeChart(index);
            }
        });
    }
}

let isSyncingRange = false;
let syncEnabled = true;

function setSyncEnabled(enabled) {
    syncEnabled = !!enabled;
    if (!syncEnabled && window.RenkoCharts && window.RenkoCharts.allInstances) {
        window.RenkoCharts.allInstances.forEach(oc => {
            if (typeof oc.chart.clearCrosshairPosition === "function") {
                try { oc.chart.clearCrosshairPosition(); } catch(e) {}
            }
            oc.updateLegend(null);
        });
    }
}

function getSyncEnabled() {
    return syncEnabled;
}

function findClosestIndex(bricks, targetTimeMs) {
    if (!bricks || bricks.length === 0) return -1;
    let lo = 0;
    let hi = bricks.length - 1;
    if (targetTimeMs <= bricks[0].timeMs) return 0;
    if (targetTimeMs >= bricks[hi].timeMs) return hi;
    while (lo <= hi) {
        let mid = Math.floor((lo + hi) / 2);
        if (bricks[mid].timeMs === targetTimeMs) return mid;
        if (bricks[mid].timeMs < targetTimeMs) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    if (hi < 0) return lo;
    if (lo >= bricks.length) return hi;
    const diffHi = Math.abs(bricks[hi].timeMs - targetTimeMs);
    const diffLo = Math.abs(bricks[lo].timeMs - targetTimeMs);
    return diffHi < diffLo ? hi : lo;
}

function setupSync(chartsList) {
    chartsList.forEach((activeChart, activeIndex) => {
        activeChart.chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
            if (!syncEnabled || isSyncingRange || !range) return;
            isSyncingRange = true;
            
            const fromIdx = Math.max(0, Math.floor(range.from));
            const toIdx = Math.min(activeChart.bricks.length - 1, Math.ceil(range.to));
            
            if (activeChart.bricks.length === 0 || fromIdx > toIdx) {
                isSyncingRange = false;
                return;
            }
            
            const fromTimeMs = activeChart.bricks[fromIdx].timeMs;
            const toTimeMs = activeChart.bricks[toIdx].timeMs;
            
            chartsList.forEach((targetChart, targetIndex) => {
                if (targetIndex === activeIndex) return;
                if (targetChart.bricks.length === 0) return;
                
                const targetFromIdx = findClosestIndex(targetChart.bricks, fromTimeMs);
                const targetToIdx = findClosestIndex(targetChart.bricks, toTimeMs);
                
                if (targetFromIdx !== -1 && targetToIdx !== -1) {
                    const logicalRange = {
                        from: targetFromIdx + (range.from - fromIdx),
                        to: targetToIdx + (range.to - toIdx)
                    };
                    try {
                        targetChart.chart.timeScale().setVisibleLogicalRange(logicalRange);
                    } catch (e) {
                        console.error("Error setting visible range:", e);
                    }
                }
            });
            
            isSyncingRange = false;
        });
    });
}

function setupCrosshairSync(chartsList) {
    chartsList.forEach((activeChart) => {
        activeChart.chart.subscribeCrosshairMove((param) => {
            if (!syncEnabled || isSyncingRange) return;
            isSyncingRange = true;
            
            const otherCharts = chartsList.filter(c => c !== activeChart);
            
            if (param.point === undefined || !param.time) {
                otherCharts.forEach(oc => {
                    if (typeof oc.chart.clearCrosshairPosition === "function") {
                        try { oc.chart.clearCrosshairPosition(); } catch(e) {}
                    }
                    oc.updateLegend(null);
                });
                isSyncingRange = false;
                return;
            }
            
            const x = Number(param.time);
            const activeBrick = activeChart.bricks.find(b => b.time === x);
            if (!activeBrick) {
                isSyncingRange = false;
                return;
            }
            
            const activeTimeMs = activeBrick.timeMs;
            
            otherCharts.forEach(oc => {
                if (oc.bricks.length === 0) return;
                
                const closestIdx = findClosestIndex(oc.bricks, activeTimeMs);
                if (closestIdx === -1) return;
                const closestBrick = oc.bricks[closestIdx];
                
                if (typeof oc.chart.setCrosshairPosition === "function") {
                    try {
                        oc.chart.setCrosshairPosition(
                            closestBrick.close,
                            closestBrick.time,
                            oc.candlestickSeries
                        );
                    } catch(e) {}
                }
                
                oc.updateLegend(closestBrick);
            });
            
            isSyncingRange = false;
        });
    });
}

window.RenkoCharts = {
    RenkoChart,
    setupSync,
    setupCrosshairSync,
    setSyncEnabled,
    getSyncEnabled
};
