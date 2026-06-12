/**
 * playback.worker.js  —  Frontend Web Worker
 *
 * Tick-based local playback runner.
 * Feeds ticks one-by-one into RenkoEngine instances,
 * collects formed bricks, and posts frames at 20 FPS.
 */

"use strict";

// ─── RenkoEngine Class ────────────────────────────────────────────────────────
class RenkoEngine {
    constructor(brickPips, reversalBoxes = 2, pipSize = 0.0001, anchor = "floor") {
        this.brickPips = parseFloat(brickPips);
        this.pipSize = parseFloat(pipSize);
        this.brickSize = this.brickPips * this.pipSize;
        this.reversalBoxes = parseInt(reversalBoxes, 10);
        this.anchor = anchor;
        this.reset();
    }

    reset() {
        this.lastClose = null;
        this.direction = 0;
        this.confirmedCount = 0;
        this.liveOpen = null;
        this.liveHigh = null;
        this.liveLow = null;
        this.liveTickCount = 0;
    }

    _initialClose(price) {
        if (this.anchor === "first") {
            return price;
        }
        if (this.anchor === "round") {
            return Math.round(price / this.brickSize) * this.brickSize;
        }
        return Math.floor(price / this.brickSize) * this.brickSize;
    }

    processTick(price, timeStr, bid = 0.0, ask = 0.0) {
        if (!isFinite(price)) {
            return [];
        }

        if (this.lastClose === null) {
            this.lastClose = this._initialClose(price);
            this.liveOpen = this.lastClose;
            this.liveHigh = price;
            this.liveLow = price;
            this.liveTickCount = 1;
            return [];
        }

        this.liveHigh = Math.max(this.liveHigh, price);
        this.liveLow = Math.min(this.liveLow, price);
        this.liveTickCount += 1;

        const newBricks = [];
        const eps = this.brickSize / 1000000.0;

        while (true) {
            const reversal = Math.max(1, this.reversalBoxes);
            const upDistance = this.direction >= 0 ? this.brickSize : reversal * this.brickSize;
            const downDistance = this.direction <= 0 ? this.brickSize : reversal * this.brickSize;
            const upTrigger = this.lastClose + upDistance;
            const downTrigger = this.lastClose - downDistance;

            if (price >= upTrigger - eps) {
                let brickOpen, brickClose;
                if (this.direction < 0) {
                    brickOpen = this.lastClose + (reversal - 1) * this.brickSize;
                    brickClose = this.lastClose + reversal * this.brickSize;
                } else {
                    brickOpen = this.lastClose;
                    brickClose = this.lastClose + this.brickSize;
                }

                const index = this.confirmedCount;
                this.confirmedCount += 1;

                newBricks.push({
                    time: index + 1,
                    confirm_time: timeStr,
                    open: brickOpen,
                    high: brickClose,
                    low: Math.min(this.liveLow, brickOpen, brickClose),
                    close: brickClose,
                    direction: "up",
                    tick_count: this.liveTickCount,
                    bid: bid,
                    ask: ask
                });

                this.lastClose = brickClose;
                this.direction = 1;
                this._resetLiveFromClose();
                continue;
            }

            if (price <= downTrigger + eps) {
                let brickOpen, brickClose;
                if (this.direction > 0) {
                    brickOpen = this.lastClose - (reversal - 1) * this.brickSize;
                    brickClose = this.lastClose - reversal * this.brickSize;
                } else {
                    brickOpen = this.lastClose;
                    brickClose = this.lastClose - this.brickSize;
                }

                const index = this.confirmedCount;
                this.confirmedCount += 1;

                newBricks.push({
                    time: index + 1,
                    confirm_time: timeStr,
                    open: brickOpen,
                    high: Math.max(this.liveHigh, brickOpen, brickClose),
                    low: brickClose,
                    close: brickClose,
                    direction: "down",
                    tick_count: this.liveTickCount,
                    bid: bid,
                    ask: ask
                });

                this.lastClose = brickClose;
                this.direction = -1;
                this._resetLiveFromClose();
                continue;
            }

            break;
        }

        return newBricks;
    }

    _resetLiveFromClose() {
        this.liveOpen = this.lastClose;
        this.liveHigh = this.lastClose;
        this.liveLow = this.lastClose;
        this.liveTickCount = 0;
    }

    getLiveBrick(currentPrice, currentBid = 0.0, currentAsk = 0.0) {
        if (this.lastClose === null || this.liveOpen === null) {
            return null;
        }
        const index = this.confirmedCount;
        return {
            time: index + 1,
            confirm_time: "", // live, not confirmed
            open: this.liveOpen,
            high: Math.max(this.liveHigh, currentPrice),
            low: Math.min(this.liveLow, currentPrice),
            close: currentPrice,
            direction: currentPrice >= this.liveOpen ? "up" : "down",
            tick_count: this.liveTickCount,
            bid: currentBid,
            ask: currentAsk,
            is_live: true
        };
    }
}

// ─── Worker State ─────────────────────────────────────────────────────────────
let _ticks = [];
let _engines = [];
let _currentTickIndex = 0;
let _isPlaying = false;
let _speed = 1.0;
let _speedMode = "time";   // "time" = market-time multiplier, "tick" = ticks/sec
let _tickAccumulator = 0.0;
let _lastFrameTime = 0.0;
let _intervalId = null;
let _virtualMs = null;     // virtual market clock (epoch ms) for time mode
let _tickTimesMs = null;   // lazily parsed tick timestamps (epoch ms)
const FRAME_RATE = 20;   // Hz
const FRAME_MS   = 1000 / FRAME_RATE;  // 50ms
const MAX_MARKET_GAP_MS = 10000;  // quiet periods longer than this are skipped in time mode

function tickTimeMs(i) {
    if (!_tickTimesMs) _tickTimesMs = new Float64Array(_ticks.length).fill(NaN);
    let v = _tickTimesMs[i];
    if (Number.isNaN(v)) {
        // "YYYY-MM-DD HH:MM:SS.mmm" (UTC) → epoch ms
        v = Date.parse(String(_ticks[i].time).replace(" ", "T") + "Z");
        if (Number.isNaN(v)) v = i; // fallback: keep ordering monotonic
        _tickTimesMs[i] = v;
    }
    return v;
}

// ─── Playback Loop ────────────────────────────────────────────────────────────
function emitNextFrame() {
    if (!_isPlaying) return;

    const now = performance.now();
    const deltaSeconds = (now - _lastFrameTime) / 1000;
    _lastFrameTime = now;

    if (_speedMode === "time") {
        if (_speed <= 0) return; // 0× = hold position

        if (_currentTickIndex >= _ticks.length) {
            processNextTicks(1); // triggers "ended"
            return;
        }

        const nextMs = tickTimeMs(_currentTickIndex);
        if (_virtualMs === null) _virtualMs = nextMs;
        _virtualMs += deltaSeconds * 1000 * _speed;

        if (_virtualMs < nextMs) {
            const gap = nextMs - _virtualMs;
            if (gap > MAX_MARKET_GAP_MS) {
                _virtualMs = nextMs; // skip quiet period
            } else {
                return; // wait for market clock to catch up
            }
        }

        let end = _currentTickIndex;
        while (end < _ticks.length && tickTimeMs(end) <= _virtualMs) end++;
        const ticksToProcess = end - _currentTickIndex;
        if (ticksToProcess > 0) processNextTicks(ticksToProcess);
        return;
    }

    _tickAccumulator += deltaSeconds * _speed;

    const ticksToProcess = Math.floor(_tickAccumulator);
    _tickAccumulator -= ticksToProcess;

    if (ticksToProcess > 0) {
        processNextTicks(ticksToProcess);
    }
}

function processNextTicks(count) {
    const endIndex = Math.min(_currentTickIndex + count, _ticks.length);

    if (_currentTickIndex >= _ticks.length) {
        _isPlaying = false;
        if (_intervalId) { clearInterval(_intervalId); _intervalId = null; }
        self.postMessage({
            type: "ended",
            total_ticks: _ticks.length,
            formed_bricks: getTotalFormedBricks()
        });
        return;
    }

    const bricksByChart = {};
    for (let idx = 0; idx < _engines.length; idx++) {
        bricksByChart[idx + 1] = [];
    }

    const tickPrices = [];

    for (let i = _currentTickIndex; i < endIndex; i++) {
        const tick = _ticks[i];
        const price = parseFloat(tick.price);
        tickPrices.push(price);
        const tick_time = tick.time;
        const bid = tick.bid !== undefined ? parseFloat(tick.bid) : price;
        const ask = tick.ask !== undefined ? parseFloat(tick.ask) : price;

        for (let idx = 0; idx < _engines.length; idx++) {
            const formed = _engines[idx].processTick(price, tick_time, bid, ask);
            if (formed && formed.length > 0) {
                for (const brick of formed) {
                    brick.brick_index = brick.time;
                    brick.confirm_tick_index = i;
                    brick.time = brick.brick_index;
                    brick.confirm_time = tick_time;
                }
                bricksByChart[idx + 1].push(...formed);
            }
        }
    }

    _currentTickIndex = endIndex;

    const lastTick = endIndex > 0 ? _ticks[endIndex - 1] : null;
    const lastPrice = lastTick ? parseFloat(lastTick.price) : 0.0;
    const lastBid = lastTick && lastTick.bid !== undefined ? parseFloat(lastTick.bid) : lastPrice;
    const lastAsk = lastTick && lastTick.ask !== undefined ? parseFloat(lastTick.ask) : lastPrice;
    const lastTime = lastTick ? lastTick.time : "";

    const liveBricksByChart = {};
    for (let idx = 0; idx < _engines.length; idx++) {
        const liveBrick = _engines[idx].getLiveBrick(lastPrice, lastBid, lastAsk);
        if (liveBrick) {
            liveBrick.brick_index = liveBrick.time;
            liveBrick.confirm_tick_index = endIndex - 1;
            liveBrick.time = liveBrick.brick_index;
            liveBrick.confirm_time = lastTime;
            liveBricksByChart[idx + 1] = liveBrick;
        }
    }

    self.postMessage({
        type: "frame",
        bricks_by_chart: bricksByChart,
        live_bricks_by_chart: liveBricksByChart,
        tick_prices: tickPrices,
        processed_ticks: endIndex,
        total_ticks: _ticks.length,
        formed_bricks: getTotalFormedBricks(),
        speed: _speed,
        latest_price: lastPrice,
        latest_time: lastTime,
        engine_confirmed_counts: _engines.map(e => e.confirmedCount)
    });
}

function getTotalFormedBricks() {
    let total = 0;
    for (let idx = 0; idx < _engines.length; idx++) {
        total += _engines[idx].confirmedCount;
    }
    return total;
}

// ─── Message Handler ──────────────────────────────────────────────────────────
self.onmessage = function (e) {
    const msg = e.data;

    switch (msg.type) {
        case "init_playback": {
            _ticks = msg.ticks || [];
            _speed = parseFloat(msg.speed);
            if (!Number.isFinite(_speed)) _speed = 1.0;
            _speedMode = msg.speed_mode || "time";
            _currentTickIndex = 0;
            _tickAccumulator = 0.0;
            _virtualMs = null;
            _tickTimesMs = null;
            _isPlaying = false;
            if (_intervalId) { clearInterval(_intervalId); _intervalId = null; }

            const reversal = parseInt(msg.reversal_boxes, 10) || 2;
            const pip = parseFloat(msg.pip_size) || 0.0001;
            const anchor = msg.anchor || "floor";

            _engines = (msg.chart_pips || [1, 2, 3, 4]).map(pipVal => {
                return new RenkoEngine(pipVal, reversal, pip, anchor);
            });

            self.postMessage({
                type: "timeline_ready",
                total_ticks: _ticks.length
            });
            break;
        }

        case "start": {
            if (_ticks.length === 0) {
                self.postMessage({ type: "status", message: "No ticks loaded. Call init_playback first." });
                return;
            }
            _isPlaying  = true;
            _lastFrameTime = performance.now();
            if (_intervalId) clearInterval(_intervalId);
            _intervalId = setInterval(emitNextFrame, FRAME_MS);
            self.postMessage({ type: "status", message: "Playback started." });
            break;
        }

        case "pause": {
            _isPlaying = false;
            if (_intervalId) { clearInterval(_intervalId); _intervalId = null; }
            self.postMessage({ type: "status", message: "Paused." });
            break;
        }

        case "resume": {
            if (!_isPlaying) {
                _isPlaying = true;
                _lastFrameTime = performance.now();
                if (_intervalId) clearInterval(_intervalId);
                _intervalId = setInterval(emitNextFrame, FRAME_MS);
                self.postMessage({ type: "status", message: "Resumed." });
            }
            break;
        }

        case "step": {
            _isPlaying = false;
            if (_intervalId) { clearInterval(_intervalId); _intervalId = null; }
            _virtualMs = null;
            processNextTicks(1);
            break;
        }

        case "reset": {
            _isPlaying  = false;
            _currentTickIndex = 0;
            _tickAccumulator = 0.0;
            _virtualMs = null;
            if (_intervalId) { clearInterval(_intervalId); _intervalId = null; }
            _engines.forEach(e => e.reset());
            self.postMessage({ type: "status", message: "Reset to start." });
            break;
        }

        case "speed": {
            _speed = parseFloat(msg.speed);
            if (!Number.isFinite(_speed)) _speed = 1.0;
            if (msg.mode) _speedMode = msg.mode;
            self.postMessage({
                type: "status",
                message: _speedMode === "time" ? `Speed set to ${_speed}× market time.` : `Speed set to ${_speed} ticks/s.`
            });
            break;
        }

        case "stop": {
            _isPlaying = false;
            if (_intervalId) { clearInterval(_intervalId); _intervalId = null; }
            _ticks = [];
            _engines = [];
            _currentTickIndex = 0;
            break;
        }

        case "skip_to": {
            const targetIdx = parseInt(msg.index, 10);
            _isPlaying = false;
            if (_intervalId) { clearInterval(_intervalId); _intervalId = null; }
            _engines.forEach(e => e.reset());
            _currentTickIndex = 0;
            _tickAccumulator = 0.0;
            _virtualMs = null;
            self.postMessage({ type: "status", message: "clear" });
            processNextTicks(targetIdx);
            self.postMessage({ type: "status", message: `Skipped to tick index ${targetIdx}.` });
            break;
        }

        case "step_multi": {
            _isPlaying = false;
            if (_intervalId) { clearInterval(_intervalId); _intervalId = null; }
            const count = parseInt(msg.count, 10) || 1;
            const dir = msg.direction || "forward";
            _virtualMs = null; // re-anchor market clock after any seek
            if (dir === "forward") {
                processNextTicks(count);
            } else {
                const targetIdx = Math.max(0, _currentTickIndex - count);
                _engines.forEach(e => e.reset());
                _currentTickIndex = 0;
                _tickAccumulator = 0.0;
                self.postMessage({ type: "status", message: "clear" });
                processNextTicks(targetIdx);
            }
            break;
        }

        default:
            self.postMessage({ type: "status", message: `Unknown message type: ${msg.type}` });
    }
};
