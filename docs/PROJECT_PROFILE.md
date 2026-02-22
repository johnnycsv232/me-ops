# Project Profile

## Environment Snapshot
- **OS**: Linux GettUppENT 6.6.87.2-microsoft-standard-WSL2 (x86_64, Ubuntu/WSL2)
- **Shell**: `/bin/bash`
- **Current Working Directory**: `/home/finan/dev/labs/me_ops`
- **Repo Root Path**: `/home/finan/dev/labs/me_ops`

## Workspace Boundaries
- **Type**: Single Python application (not a monorepo).
- **Virtual Environment**: `.venv` detected.
- **Dependencies**: Driven by `requirements.txt` (needs inspection for pip/poetry/etc).
- **IDE Context**: `.vscode`, `.idea` (implied by typical usage, to be verified), and `me_ops.code-workspace` present.

## Stack Profile
- **Language**: Python (.py files dominant)
- **Database**: DuckDB (`me_ops.duckdb`, `warehouse.duckdb`), SQLite (`me_ops.db`)
- **Tools**: VSCode workspace, potential JetBrains. `pyrightconfig.json` detected for typing.

## Git Snapshot (At point of discovery)
- **Branch**: `main`
- **Status**: Ahead by 2 commits. Many modified and untracked files indicating a high volume of recent work on engines and pipelines.
- **Last 5 Commits**:
  - `43d069b` fix(ingest): handle Optional subscript errors
  - `03ad0de` chore: address pyright errors, improve code quality, and add foundation guard
  - `25d6f1f` feat: add workflow DNA + evolving insight engine with unified master pipeline
  - `2842d81` feat: architect engine - extract, modularize, rebuild workflows
  - `f9adf0a` feat: deep analysis engine — 5-dimension behavioral self-architecture
