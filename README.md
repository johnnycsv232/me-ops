# ME-OPS: You, Without the Flaws

A self-updating personal analytics system that learns your patterns, removes failure modes, and executes better than you — with proof.

## Architecture

```
me_ops/                  # Project root (git repo)
├── ingest.py            # Raw JSON → DuckDB normalized events
├── __init__.py          # Python package marker
├── requirements.txt     # Python dependencies
├── pyrightconfig.json   # Type checking config
├── .githooks/           # Pre-commit + commit-msg hooks
├── .agent/workflows/    # Health check + inception workflows
├── data/                # Processed data artifacts
├── output/              # Generated reports
└── Johnny Cage LTM/     # Life Intelligence Report
```

> **Future modules:** `entities.py`, `workflows.py`, `mistakes.py`, `vectors.py`, `daily_report.py`, `queries.py`

## Data Sources

19 Pieces LTM JSON exports (~65K records, ~416MB) stored in parent directory (`../`).

## Quick Start

```bash
# Activate venv
source ../.venv/bin/activate

# Run full ingestion pipeline
python ingest.py

# Run with custom paths
python ingest.py --data-dir /path/to/json --db ./me_ops.duckdb
```

## Real Database Setup

```bash
# Create/initialize a persistent DB with all core schemas
python scripts/setup_real_database.py

# Optionally ingest data immediately
python scripts/setup_real_database.py --ingest --data-dir /path/to/json/exports
```

The setup command uses `$ME_OPS_DB_PATH` when set; otherwise it defaults to `./me_ops.duckdb`.

## Health Check

```bash
# Run elite environment check
ops-check

# Or directly:
~/.local/bin/elite-check /home/finan/dev/labs/me_ops
```

## Core Principle

> Every claim includes `evidence: [event_ids]`, `source_file`, and timestamps.
> No guessing. If not evidenced: `unknown` or `candidate` with confidence.
