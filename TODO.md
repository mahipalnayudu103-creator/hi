# 📋 RenkoTerminal Development Roadmap (TODO)

## 📱 Responsive UI & Theme Improvements
- [x] Implement slide-out menu drawer for the Left Sidebar on mobile/tablet.
- [x] Add debounce resizing listeners inside `charts.js` for TradingView charts when screen sizes change.
- [x] Integrate screen width detection inside `main.js` to automatically switch chart layout modes (Focus, Stack, Grid).
- [x] Complete CSS layout tweaks inside `responsive.css`.

## ⚙️ Backend Enhancements
- [x] Add support for compressed tick inputs (`.zip`, `.gz`) directly in `csv_stream.py`.
- [ ] Implement adaptive chunk sizing based on system RAM utilization.
- [ ] Support custom price formulas (e.g. `(Bid + Ask + Open + Close) / 4`).

## 🧪 Testing Coverage
- [ ] Write integration test cases for WebSocket connections under `tests/`.
- [ ] Build end-to-end performance benchmarks for CPU vs GPU calculations.
