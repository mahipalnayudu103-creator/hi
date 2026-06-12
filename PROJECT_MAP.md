# 🗺️ RenkoTerminal Project Map

This document explains the repository structure and the purpose of each file and folder in simple words.

```
renko_playback/
├── backend/                  # Python FastAPI high-performance server
│   ├── app.py                # Main server entry & lifecycle initialization
│   ├── config.py             # Server settings, paths (CACHE_DIR), limits
│   ├── routes/
│   │   └── api.py            # API routes (Metadata, Build Renko, WebSockets)
│   ├── services/
│   │   ├── pipeline.py       # High-throughput streaming build job manager
│   │   ├── job_manager.py    # Background build job tracker
│   │   ├── parquet_cache.py  # Incremental Parquet cache writer & lazy window reader
│   │   ├── renko_engine.py   # Synchronous JIT-compiled CPU Renko calculator
│   │   ├── renko_state.py    # Memory-minimal streaming state Renko calculator
│   │   └── gpu_engine.py     # GPU-accelerated (CuPy, cuDF) calculations
│   └── utils/
│       ├── cache.py          # Flat file disk cache interface (pickle/msgpack)
│       ├── csv_reader.py     # CSV delimiter, year extraction & metadata analyzer
│       ├── csv_stream.py     # High-performance chunk-by-chunk tick generator
│       ├── memory.py         # RAM pressure monitor and garbage collection helper
│       └── monitor.py        # System utilization probes (CPU/GPU/RAM)
│
├── frontend/                 # Client dashboard interface
│   ├── index.html            # Main viewport structure & UI panels
│   └── static/
│       ├── css/
│       │   ├── styles.css     # Dark-mode styling, gauges, console logs
│       │   ├── responsive.css # Screen layout overrides (mobile & tablet)
│       │   └── nouislider.min.css # Range slider theme styles
│       ├── js/
│       │   ├── main.js        # UI event listener, state machine & WS client
│       │   ├── api.js         # HTTP backend wrappers
│       │   ├── charts.js      # TradingView Lightweight Charts rendering rules
│       │   ├── playback.worker.js # Web Worker for tick playback loop
│       │   ├── nouislider.min.js  # Range slider controls
│       │   └── lightweight-charts.js # Chart engine library
│       └── assets/           # Dynamic assets, images, icons
│
├── docs/                     # Technical documentation files
│   ├── ARCHITECTURE.md       # Multi-threaded pipeline & JIT architecture
│   ├── API.md                # REST endpoints and WebSocket protocols
│   └── UI_RESPONSIVE_PLAN.md # Roadmap for responsive web design
│
├── tests/                    # Testing suites
│   └── test_smoke.py         # Basic smoke tests for server endpoints
│
├── data/                     # Local data workspace (CSVs are ignored by git)
│   └── .gitkeep
│
├── requirements.txt          # Python dependency list
├── README.md                 # Project introduction
├── RUN_LOCAL.md              # Startup instructions
├── TODO.md                   # Development roadmaps
└── CHANGELOG.md              # Change log history
```

---

### Root Configuration Files
- [README.md](README.md): Simple overview of the application, tech stack, and components.
- [RUN_LOCAL.md](RUN_LOCAL.md): Explains how to install and launch the server in detail.
- [TODO.md](TODO.md): Development checklist.
- [CHANGELOG.md](CHANGELOG.md): History of updates.
- [.gitignore](.gitignore): Excludes environment files (`.venv`), temporary checkpoints, large CSV datasets, and local caches from Git commits.
- [requirements.txt](requirements.txt): Lists all third-party libraries required by the Python application (FastAPI, Polars, DuckDB, PyArrow, Numba, Cupy).
