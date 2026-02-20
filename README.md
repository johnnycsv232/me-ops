# ME-OPS: You, Without the Flaws

A self-updating personal analytics system that learns your patterns, removes failure modes, and executes better than you — with proof.

## Architecture

```
me_ops/              # Core Python package
├── ingest.py        # Raw JSON → DuckDB normalized events
├── entities.py      # Entity extraction + link tables
├── workflows.py     # Session/sequence mining (networkx)
├── mistakes.py      # Failure pattern detection + playbooks
├── vectors.py       # Qdrant semantic search over events
├── daily_report.py  # Automated daily metrics report
├── queries.py       # 10 validation queries
└── requirements.txt
```

## Data Sources

19 Pieces LTM JSON exports (~65K records, ~416MB) + 1 Markdown intelligence report.

## Quick Start

```bash
# Activate venv
source me_ops/.venv/bin/activate

# Run full ingestion pipeline
python me_ops/ingest.py

# Run validation queries
python me_ops/queries.py

# Generate daily report
python me_ops/daily_report.py
```

## Core Principle

> Every claim includes `evidence: [event_ids]`, `source_file`, and timestamps.
> No guessing. If not evidenced: `unknown` or `candidate` with confidence.
