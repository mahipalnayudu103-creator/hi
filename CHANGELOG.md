# 📜 Changelog

All notable changes to **RenkoTerminal** will be documented in this file.

---

## [1.1.0] - 2026-06-12
### Added
- Reorganized codebase into clean structured directories (`backend/routes`, `backend/services`, `backend/models`, `backend/utils`, `frontend/static/`).
- Centralized configurations (`backend/config.py`) containing default paths, limits, and server configurations.
- Created architectural (`docs/ARCHITECTURE.md`), API routing (`docs/API.md`), responsive design roadmap (`docs/UI_RESPONSIVE_PLAN.md`), local execution (`RUN_LOCAL.md`), and folder maps (`PROJECT_MAP.md`) documentation files.
- Added empty stylesheet (`frontend/static/css/responsive.css`) for device layout overrides.
- Implemented basic backend smoke tests (`tests/test_smoke.py`).

### Changed
- Moved root configuration files (`requirements.txt`, `.gitignore`) to the project root directory.
- Updated startup scripts (`setup.bat`, `run.bat`) to comply with the new folder structure.
- Refactored all backend Python files to use absolute package imports (`from utils.csv_stream import ...`, etc.).
- Refactored frontend html assets (`frontend/index.html`) to source CSS/JS files from `static/js/` and `static/css/` respectively.

---

## [1.0.0] - 2026-06-11
### Added
- High-performance streaming build pipeline to handle large CSV datasets.
- PyArrow Parquet caching layer for lazy chart scrolling.
- CPU Numba multithreading optimization.
- GPU CuPy multi-chart calculation support.
