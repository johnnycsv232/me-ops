# Decisions Record

## 1. Docker Mode
- **Options Considered**: None, Hybrid, Full.
- **Decision**: Mode A (No Docker).
- **Official Sources**: Docker Official Guidelines vs local-first Python CLI tools.
- **Tradeoffs**: Best local speed and lowest execution friction for a single Python app with embedded databases (DuckDB/SQLite). It sacrifices containerization portability, but since the system operates on local paths and local DB files, running natively via `.venv` is strictly preferred.
- **Verification**: `python -m unittest` and `python master.py` run natively.

## 2. Package Manager and Runtime
- **Options Considered**: pip, poetry, uv.
- **Decision**: `pip` + `requirements.txt` encapsulated in standard `.venv`.
- **Official Sources**: Python Packaging Authority (PyPA) guidelines.
- **Tradeoffs**: Simple and stable. It lacks strict lockfile hashing out-of-the-box compared to `poetry.lock`, but `requirements.txt` correctly bounds versions. Keeps the barrier to entry minimal.
- **Verification**: `pip install -r requirements.txt`

## 3. Workspace Strategy (IDE)
- **Options Considered**: Monorepo tooling (NX/Turbo) vs Single App.
- **Decision**: Native Python application utilizing standard directory layout, standardized by `me_ops.code-workspace`.
- **Official Sources**: VSCode Multi-root / Workspace docs.
- **Tradeoffs**: Simplest setup. Prevents linking complexities while standardizing Pyright, auto-formatting, and paths via the `.code-workspace` JSON.

## 4. Database Strategy
- **Options Considered**: PostgreSQL/MySQL via Docker vs Embedded (SQLite/DuckDB).
- **Decision**: Embedded Databases (`me_ops.db` for SQLite, `me_ops.duckdb` and `warehouse.duckdb` for analytics).
- **Official Sources**: DuckDB official use cases for analytical workloads on local machines.
- **Tradeoffs**: Tremendous speed and zero-setup networking, at the cost of concurrent multi-writer limitations. Fits exactly the Agentic/Pipeline nature of ME-OPS.

## 5. Security & Environment
- **Options Considered**: Hardcoded config vs `.env` vs Secret Manager.
- **Decision**: `.env` parsed by `python-dotenv`.
- **Official Sources**: The Twelve-Factor App methodology (Config).
- **Tradeoffs**: Simple, keeps secrets (`GEMINI_API_KEY`) out of Git, easily injectable in CI flows if needed.
