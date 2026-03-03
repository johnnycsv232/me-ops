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

JSON exports stored in the `data/` directory.

## Quick Start

```bash
# Activate venv
source ../.venv/bin/activate

# Install base dependencies
pip install -r requirements.txt

# Or install all (including ML/Clustering)
pip install numpy pandas scikit-learn sentence-transformers qdrant-client umap-learn hdbscan networkx

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

## Feature Boundaries

**Core offline analytics** (no network): `ingest.py`, `workflows.py`, `mistakes.py`, `architect.py` (scoring only), `graph.py`, `cluster.py`, `predict.py`, `live.py`.

**External API integrations**: `agent.py`, `briefing.py`, `deep_analysis.py`, and `architect.py --ai` call Google Gemini. `sources/github.py` calls the GitHub REST API.

## Agent Function Calling

The `agent.py` module uses explicit, local dispatch for function calling rather than relying entirely on the model's automatic function calling features. Automatic calling is disabled; dispatch is handled via a local loop.
