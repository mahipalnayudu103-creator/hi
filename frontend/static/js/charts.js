// ============================================================================
// INDICATOR REGISTRY (Extension point for adding custom indicators)
// Register any indicator configuration here. The system dynamically handles:
//   - Chart series creation (Line/Histogram)
//   - Data calculations in bulk load (setData) and live stream (appendBrick)
//   - Sidebar checkbox toggles and state synchronization
//   - Cursor Data Window row rendering and updating
//   - Tooltip details on hover
// ============================================================================
window.INDICATOR_REGISTRY = {
    /*
    ema200: {
        name: '200 EMA',
        type: 'line',         // 'line' or 'histogram'
        color: '#f97316',     // line/histogram color
        lineWidth: 2.5,
        visible: false,       // default visibility on load
        calculate: (brick, idx, computedBricks) => {
            const period = 200;
            const alpha = 2 / (period + 1);
            const sourceVal = (brick.bid !== undefined && brick.bid > 0) ? Number(brick.bid) : Number(brick.close);
            if (idx === 0) return sourceVal;
            const prev = computedBricks[idx - 1]?.indicators?.ema200;
            return prev !== undefined ? (sourceVal * alpha + prev * (1 - alpha)) : sourceVal;
        }
    }
    */
};

// Dynamically-keyed per chartId — populated on first RenkoChart construction.
const metaByChart = {};
const xValuesByChart = {};

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
    if (!metaByChart[chartId]) return null;
    const exact = metaByChart[chartId].get(Number(x));
    if (exact) return exact.confirm_time;

    const nearestX = findNearestX(chartId, Number(x));
    if (nearestX === null) return null;

    const nearest = metaByChart[chartId].get(nearestX);
    return nearest?.confirm_time ?? null;
}

function getNearestPriceByX(chartId, x) {
    if (!metaByChart[chartId]) return null;
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
    constructor(containerId, legendId, titleId, chartId, brickPips = 1) {
        this.containerId = containerId;
        this.legendId = legendId;
        this.titleId = titleId;
        this.chartId = chartId;
        this.brickPips = brickPips;  // pip size of this chart's Renko bricks
        this.container = document.getElementById(containerId);
        this.legend = document.getElementById(legendId);
        this.title = document.getElementById(titleId);
        this.wrapper = this.container ? this.container.closest('.chart-wrapper') : null;
        this.bricks = [];
        this._confirmedBrickCount = 0;  // stable counter for live-brick positioning
        this._xSet = new Set();         // O(1) duplicate check for xValuesByChart
        this._tickAnimRaf = null;       // in-flight tick animation RAF id
        this._suppressMarkers = false;  // true during bulk setData to defer marker rebuild
        const autoScrollCheckbox = document.getElementById("autoScrollCheckbox");
        this.autoScroll = autoScrollCheckbox ? autoScrollCheckbox.checked : false;
        this.currentPriceLine = null;
        this._priceLinePrice = null;    // cached price so we skip no-op updates
        this.reversalEnabled = true;
        this.separatorsEnabled = false;
        // Wicks are always plotted tick-by-tick along with the bars
        this.showWicks = true;

        // Lazy-init per-chart lookup tables
        if (!metaByChart[this.chartId]) metaByChart[this.chartId] = new Map();
        if (!xValuesByChart[this.chartId]) xValuesByChart[this.chartId] = [];

        const symbol = getSymbolFromUI();
        const priceFormat = getPriceFormat(symbol);
        this.precision = priceFormat.precision;
        this.minMove = priceFormat.minMove;
        this.onCrosshairChangeCallback = null;
        this.onBrickClickCallback = null;
        this.pipSize = 0.0001;
        this.isCrosshairActive = false;
        
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
        
        // Hollow Renko bar style (section 11 of cTrader MD spec):
        // transparent fill + colored outline border + one-sided wick.
        // The OHLC data already encodes correct one-sided wicks:
        //   UP  — high=close (no upper wick), low=formation_low (lower wick only)
        //   DOWN — high=formation_high (upper wick only), low=close (no lower wick)
        this.candlestickSeries = this.chart.addSeries(LightweightCharts.CandlestickSeries, {
            priceFormat: priceFormat,
            upColor:         'rgba(34, 197, 94, 0)',   // hollow — transparent fill
            downColor:       'rgba(239, 68, 68, 0)',   // hollow — transparent fill
            borderUpColor:   '#22c55e',
            borderDownColor: '#ef4444',
            wickUpColor:     '#22c55e',
            wickDownColor:   '#ef4444',
        });

        // Initialize dynamic indicators series
        this.indicatorSeries = {};
        Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
            const seriesOptions = {
                priceScaleId: 'right',
                title: config.name,
                visible: config.visible ?? true,
                priceFormat: priceFormat,
                ...config.options
            };
            if (config.color) seriesOptions.color = config.color;
            if (config.lineWidth) seriesOptions.lineWidth = config.lineWidth;

            let series;
            if (config.type === 'line') {
                series = this.chart.addSeries(LightweightCharts.LineSeries, seriesOptions);
            } else if (config.type === 'histogram') {
                series = this.chart.addSeries(LightweightCharts.HistogramSeries, seriesOptions);
            } else {
                series = this.chart.addSeries(LightweightCharts.LineSeries, seriesOptions);
            }
            this.indicatorSeries[id] = series;
        });
        
        this.setupCrosshairListener();
        this.setupClickListener();
        this.setupDoubleClickListener();

        // Responsive chart resize listener using ResizeObserver with debouncing to prevent thrashing
        const debouncedResize = debounce((width, height) => {
            try {
                this.chart.resize(width, height);
            } catch (e) {
                console.warn("Resize failed:", e);
            }
        }, 120);

        this.resizeObserver = new ResizeObserver(entries => {
            if (!entries || entries.length === 0) return;
            const { width, height } = entries[0].contentRect;
            if (width > 0 && height > 0) {
                debouncedResize(width, height);
            }
        });
        this.resizeObserver.observe(this.container);
        
        if (window.RenkoCharts) window.RenkoCharts.allInstances.push(this);
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
        const format = {
            type: 'price',
            precision: this.precision,
            minMove: this.minMove,
        };
        this.candlestickSeries.applyOptions({ priceFormat: format });
        if (this.indicatorSeries) {
            Object.values(this.indicatorSeries).forEach(series => {
                series.applyOptions({ priceFormat: format });
            });
        }
    }
    
    // Returns the candle high/low to plot. cTrader-default Renko is wickless, so unless
    // wicks are explicitly enabled the high/low collapse onto the body (open/close).
    _candleHL(open, high, low, close) {
        if (this.showWicks) {
            return { high: Number(high), low: Number(low) };
        }
        const o = Number(open), c = Number(close);
        return { high: Math.max(o, c), low: Math.min(o, c) };
    }

    // Toggle wick rendering and repaint the already-loaded confirmed bricks.
    setShowWicks(enabled) {
        this.showWicks = !!enabled;
        if (!this.bricks || this.bricks.length === 0) return;
        const confirmed = this.bricks.filter(b => !b.is_live);
        const seriesData = confirmed.map(b => {
            const hl = this._candleHL(b.open, b.high, b.low, b.close);
            return { time: b.time, open: Number(b.open), high: hl.high, low: hl.low, close: Number(b.close) };
        });
        this.candlestickSeries.setData(seriesData);
    }

    setData(bricks) {
        if (this._tickAnimRaf) { cancelAnimationFrame(this._tickAnimRaf); this._tickAnimRaf = null; }
        this._confirmedBrickCount = 0;
        this._xSet.clear();
        if (metaByChart[this.chartId]) metaByChart[this.chartId].clear();
        xValuesByChart[this.chartId] = [];
        this._suppressMarkers = true;  // defer marker rebuild until end of setData

        const symbol = getSymbolFromUI();
        const priceFormat = getPriceFormat(symbol);
        this.precision = priceFormat.precision;
        this.minMove = priceFormat.minMove;
        this.candlestickSeries.applyOptions({
            priceFormat: priceFormat
        });
        if (this.indicatorSeries) {
            Object.values(this.indicatorSeries).forEach(series => {
                series.applyOptions({ priceFormat: priceFormat });
            });
        }

        const computedBricks = [];
        this.bricks = bricks.map((brick, idx) => {
            const x = Number(brick.brick_index ?? brick.time);
            const close = Number(brick.close);

            brick.indicators = brick.indicators || {};
            Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
                if (typeof config.calculate === 'function') {
                    brick.indicators[id] = config.calculate(brick, idx, computedBricks);
                }
            });

            metaByChart[this.chartId].set(x, {
                confirm_time: brick.confirm_time,
                direction: brick.direction,
                tick_count: brick.tick_count,
                open: Number(brick.open),
                high: Number(brick.high),
                low: Number(brick.low),
                close: close,
                indicators: { ...brick.indicators }
            });
            
            xValuesByChart[this.chartId].push(x);
            this._xSet.add(x);

            brick.time = x;
            if (brick.timeMs === undefined) {
                brick.timeMs = brick.confirm_time ? new Date(brick.confirm_time).getTime() : Date.now();
            }
            computedBricks.push(brick);
            return brick;
        });

        // Bricks from API arrive in order — no need to sort
        // xValuesByChart is already sequential from the map above

        const seriesData = this.bricks.map(b => {
            const hl = this._candleHL(b.open, b.high, b.low, b.close);
            return {
                time: b.time,
                open: Number(b.open),
                high: hl.high,
                low: hl.low,
                close: Number(b.close)
            };
        });
        this.candlestickSeries.setData(seriesData);

        // Update indicators series
        Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
            const series = this.indicatorSeries[id];
            if (series) {
                const indicatorData = this.bricks
                    .map(b => ({
                        time: b.time,
                        value: b.indicators ? b.indicators[id] : null
                    }))
                    .filter(item => item.value !== undefined && item.value !== null);
                series.setData(indicatorData);
            }
        });

        this._confirmedBrickCount = this.bricks.length;  // sync counter after bulk load
        this._suppressMarkers = false;  // allow marker updates again

        if (this.bricks.length > 0) {
            const last = this.bricks[this.bricks.length - 1];
            this.updatePriceLine(last.close, last.direction);
            if (!this.isCrosshairActive) {
                this.updateLegend(last);
                if (this.onCrosshairChangeCallback) this.onCrosshairChangeCallback(last);
            }
        } else {
            this.removePriceLine();
            if (!this.isCrosshairActive) {
                this.updateLegend(null);
                if (this.onCrosshairChangeCallback) this.onCrosshairChangeCallback(null);
            }
        }
        this.updateMarkers();
    }
    
    appendBrick(brick) {
        const isLive = !!brick.is_live;
        let x = Number(brick.brick_index ?? brick.time);

        if (isLive) {
            // Force the live (forming) brick to a stable sequential position so it
            // always overwrites itself rather than adding ghost bars each tick.
            // The backend WS sends tick-index-based times that change every tick;
            // pinning to confirmedCount+1 fixes the accumulation bug.
            x = this._confirmedBrickCount + 1;
        } else {
            // Map confirmed bricks to a sequential 1-based index so they stay
            // contiguous and always come before the live brick position.
            this._confirmedBrickCount += 1;
            x = this._confirmedBrickCount;
        }

        brick.time = x;

        const close = Number(brick.close);

        brick.indicators = brick.indicators || {};
        const isUpdate = (this.bricks.length > 0 && this.bricks[this.bricks.length - 1].time === x);
        Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
            if (typeof config.calculate === 'function') {
                if (isUpdate) {
                    brick.indicators[id] = config.calculate(brick, this.bricks.length - 1, this.bricks.slice(0, -1));
                } else {
                    brick.indicators[id] = config.calculate(brick, this.bricks.length, this.bricks);
                }
            }
        });

        metaByChart[this.chartId].set(x, {
            confirm_time: brick.confirm_time,
            direction: brick.direction,
            tick_count: brick.tick_count,
            open: Number(brick.open),
            high: Number(brick.high),
            low: Number(brick.low),
            close: close,
            indicators: { ...brick.indicators }
        });

        if (!this._xSet.has(x)) {
            this._xSet.add(x);
            xValuesByChart[this.chartId].push(x);
            // Only sort when inserting out-of-order (live brick re-uses a slot so always skips)
            if (xValuesByChart[this.chartId].length > 1) {
                const last = xValuesByChart[this.chartId];
                if (last[last.length - 1] < last[last.length - 2]) {
                    last.sort((a, b) => a - b);
                }
            }
        }

        if (brick.timeMs === undefined) {
            brick.timeMs = brick.confirm_time ? new Date(brick.confirm_time).getTime() : Date.now();
        }
        
        if (this.bricks.length > 0 && this.bricks[this.bricks.length - 1].time === brick.time) {
            this.bricks[this.bricks.length - 1] = brick;
        } else {
            this.bricks.push(brick);
        }
        
        const hl = this._candleHL(brick.open, brick.high, brick.low, close);
        this.candlestickSeries.update({
            time: brick.time,
            open: Number(brick.open),
            high: hl.high,
            low: hl.low,
            close: close
        });

        // Update indicator series
        Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
            const series = this.indicatorSeries[id];
            if (series && brick.indicators && brick.indicators[id] !== undefined && brick.indicators[id] !== null) {
                series.update({
                    time: brick.time,
                    value: brick.indicators[id]
                });
            }
        });



        // Current price line (on latest tick close)
        this.updatePriceLine(brick.close, brick.direction);

        // Update markers periodically or when confirmed
        if (!isLive) {
            this.updateMarkers();
        }

        if (!this.isCrosshairActive) {
            this.updateLegend(brick);
            if (this.onCrosshairChangeCallback) {
                this.onCrosshairChangeCallback(brick);
            }
        }

        if (this.autoScroll) {
            this.chart.timeScale().scrollToPosition(0, false);
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

    /**
     * Animate the live/forming bar through an array of tick prices.
     * Each RAF step advances one tick so the bar visibly moves price-by-price.
     * Called once per playback_frame with the frame's sampled tick_prices.
     */
    animateTickPrices(tickPrices, frameIntervalMs) {
        if (!tickPrices || tickPrices.length === 0) return;

        // Cancel any in-flight animation from the previous frame
        if (this._tickAnimRaf) {
            cancelAnimationFrame(this._tickAnimRaf);
            this._tickAnimRaf = null;
        }

        // Find current live bar (last brick with is_live flag)
        const liveBrick = this.bricks.length > 0 && this.bricks[this.bricks.length - 1].is_live
            ? this.bricks[this.bricks.length - 1]
            : null;
        if (!liveBrick) return;

        const liveX    = liveBrick.time;
        const liveOpen = Number(liveBrick.open);
        let   runHigh  = Number(liveBrick.high);
        let   runLow   = Number(liveBrick.low);

        // Spread ticks evenly across the frame interval
        const msPerTick = frameIntervalMs / tickPrices.length;
        let   tickIdx   = 0;
        let   lastStamp = performance.now();

        const step = (now) => {
            const elapsed = now - lastStamp;
            const ticksToAdvance = Math.max(1, Math.floor(elapsed / msPerTick));

            for (let i = 0; i < ticksToAdvance && tickIdx < tickPrices.length; i++, tickIdx++) {
                const price = tickPrices[tickIdx];
                if (price > runHigh) runHigh = price;
                if (price < runLow)  runLow  = price;
            }

            if (tickIdx > 0) {
                const close = tickPrices[Math.min(tickIdx, tickPrices.length) - 1];
                // Apply same cTrader wick restriction during live formation (UP=lower wick only, DOWN=upper wick only)
                let hi, lo;
                if (this.showWicks) {
                    const isUp = close >= liveOpen;
                    hi = isUp ? close : runHigh;
                    lo = isUp ? runLow : close;
                } else {
                    hi = Math.max(liveOpen, close);
                    lo = Math.min(liveOpen, close);
                }
                this.candlestickSeries.update({
                    time:  liveX,
                    open:  liveOpen,
                    high:  hi,
                    low:   lo,
                    close: close,
                });
                if (this.currentPriceLine && close !== this._priceLinePrice) {
                    this.currentPriceLine.applyOptions({ price: close, color: close >= liveOpen ? '#22c55e' : '#fb7185' });
                    this._priceLinePrice = close;
                } else if (!this.currentPriceLine) {
                    this.updatePriceLine(close, close >= liveOpen ? 'up' : 'down');
                    this._priceLinePrice = close;
                }
            }

            lastStamp = now;
            if (tickIdx < tickPrices.length) {
                this._tickAnimRaf = requestAnimationFrame(step);
            } else {
                this._tickAnimRaf = null;
            }
        };

        this._tickAnimRaf = requestAnimationFrame(step);
    }

    updateMarkers() {
        if (this._suppressMarkers) return;  // defer during bulk setData
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

        // Add "Latest" marker on the last confirmed brick (scan from end, not findIndex from start)
        let lastBrick = null;
        for (let i = this.bricks.length - 1; i >= 0; i--) {
            if (!this.bricks[i].is_live) { lastBrick = this.bricks[i]; break; }
        }
        if (lastBrick) {
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
        // Cancel any in-flight tick animation
        if (this._tickAnimRaf) { cancelAnimationFrame(this._tickAnimRaf); this._tickAnimRaf = null; }
        this.bricks = [];
        this._confirmedBrickCount = 0;
        this._xSet.clear();
        this.candlestickSeries.setData([]);
        if (this.indicatorSeries) {
            Object.values(this.indicatorSeries).forEach(series => { series.setData([]); });
        }
        if (this.markersApi) { this.markersApi.setMarkers([]); }
        this.removePriceLine();
        this._priceLinePrice = null;
        this.updateLegend(null);
        if (metaByChart[this.chartId]) metaByChart[this.chartId].clear();
        xValuesByChart[this.chartId] = [];
    }
    
    updateLegend(brick) {
        if (!brick) {
            this.legend.textContent = "O: — H: — L: — C: —";
            return;
        }
        const prec = this.precision;
        let txt = `O: ${brick.open.toFixed(prec)} H: ${brick.high.toFixed(prec)} L: ${brick.low.toFixed(prec)} C: ${brick.close.toFixed(prec)}`;
        
        // Dynamic indicators display
        if (brick.indicators) {
            Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
                const val = brick.indicators[id];
                if (val !== undefined && val !== null) {
                    txt += ` | ${config.name}: ${val.toFixed(prec)}`;
                }
            });
        } else if (brick.time !== undefined) {
            const meta = metaByChart[this.chartId].get(brick.time);
            if (meta && meta.indicators) {
                Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
                    const val = meta.indicators[id];
                    if (val !== undefined && val !== null) {
                        txt += ` | ${config.name}: ${val.toFixed(prec)}`;
                    }
                });
            }
        }

        this.legend.textContent = txt;
    }
    
    setTitle(text) {
        if (this.title) this.title.textContent = text;
    }

    destroy() {
        if (this._tickAnimRaf) { cancelAnimationFrame(this._tickAnimRaf); this._tickAnimRaf = null; }
        if (this.resizeObserver) { this.resizeObserver.disconnect(); this.resizeObserver = null; }
        if (this.markersApi) {
            try { this.markersApi.setMarkers([]); } catch(e) {}
            this.markersApi = null;
        }
        this.removePriceLine();
        try { this.chart.remove(); } catch(e) {}
        if (window.RenkoCharts && window.RenkoCharts.allInstances) {
            const idx = window.RenkoCharts.allInstances.indexOf(this);
            if (idx !== -1) window.RenkoCharts.allInstances.splice(idx, 1);
        }
        delete metaByChart[this.chartId];
        delete xValuesByChart[this.chartId];
        this._xSet.clear();
    }

    formatPrice(price) {
        if (price === undefined || price === null) return "-";
        return Number(price).toFixed(this.precision);
    }

    setupCrosshairListener() {
        this.chart.subscribeCrosshairMove((param) => {
            const hasData = param && param.time !== undefined && param.point !== undefined;
            if (!hasData) {
                this.isCrosshairActive = false;
                const latestBrick = this.bricks.length > 0 ? this.bricks[this.bricks.length - 1] : null;
                this.updateLegend(latestBrick);
                this.tooltip.style.display = "none";
                const globalTooltip = document.getElementById("hoverTooltip");
                if (globalTooltip) globalTooltip.classList.add("hidden");
                if (this.onCrosshairChangeCallback) {
                    this.onCrosshairChangeCallback(latestBrick);
                }
                return;
            }
            
            this.isCrosshairActive = true;
            
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
            
            ohlc.time = x;
            this.updateLegend(ohlc);
            
            let tooltipHtml = `
                <div><b>${this.brickPips} Pip Renko</b></div>
                <div>Time: ${formatUtcTime(confirmTime)}</div>
                <div>Open: ${this.formatPrice(ohlc.open)}</div>
                <div>High: ${this.formatPrice(ohlc.high)}</div>
                <div>Low: ${this.formatPrice(ohlc.low)}</div>
                <div>Close: ${this.formatPrice(ohlc.close)}</div>
            `;

            // Hover tooltip indicators display
            if (meta && meta.indicators) {
                Object.entries(window.INDICATOR_REGISTRY || {}).forEach(([id, config]) => {
                    const val = meta.indicators[id];
                    if (val !== undefined && val !== null) {
                        tooltipHtml += `<div>${config.name}: ${this.formatPrice(val)}</div>`;
                    }
                });
            }

            tooltipHtml += `
                <div>Direction: ${meta?.direction ? (meta.direction.charAt(0).toUpperCase() + meta.direction.slice(1)) : "-"}</div>
                <div>Ticks: ${meta?.tick_count ?? "-"}</div>
            `;
            this.tooltip.innerHTML = tooltipHtml;
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

let isSyncingRange      = false;
let syncEnabled         = false;   // controls time-scale (zoom/pan) sync
let crosshairSyncEnabled = true;  // controls crosshair position sync (independent)

function setSyncEnabled(enabled) {
    syncEnabled = !!enabled;
    if (!syncEnabled && window.RenkoCharts && window.RenkoCharts.allInstances) {
        window.RenkoCharts.allInstances.forEach(oc => {
            try { oc.chart.clearCrosshairPosition?.(); } catch(e) {}
            oc.updateLegend(null);
        });
    }
}

function setCrosshairSyncEnabled(enabled) {
    crosshairSyncEnabled = !!enabled;
    if (!crosshairSyncEnabled && window.RenkoCharts && window.RenkoCharts.allInstances) {
        window.RenkoCharts.allInstances.forEach(oc => {
            try { oc.chart.clearCrosshairPosition?.(); } catch(e) {}
            oc.updateLegend(null);
        });
    }
}

function getCrosshairSyncEnabled() { return crosshairSyncEnabled; }

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
            if (!crosshairSyncEnabled || isSyncingRange) return;
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
    getSyncEnabled,
    setCrosshairSyncEnabled,
    getCrosshairSyncEnabled,
    allInstances: []
};

