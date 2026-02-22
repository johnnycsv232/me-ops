#!/usr/bin/env python3
"""Bootstrap a persistent ME-OPS DuckDB database.

Usage examples:
  python scripts/setup_real_database.py
  python scripts/setup_real_database.py --db /path/to/me_ops.duckdb --ingest --data-dir /path/to/exports
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import architect
import entities
import ingest
import insights
import workflow_dna
import workflows


MISTAKES_BOOTSTRAP_DDL = """
CREATE SEQUENCE IF NOT EXISTS fail_seq;

CREATE TABLE IF NOT EXISTS failure_patterns (
    id INTEGER PRIMARY KEY DEFAULT nextval('fail_seq'),
    pattern_type TEXT,
    description TEXT,
    severity TEXT,
    evidence_count INTEGER,
    last_detected TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anti_playbook (
    id INTEGER PRIMARY KEY,
    rule_text TEXT,
    trigger_condition TEXT,
    evidence TEXT,
    confidence REAL
);
"""


def resolve_db_path(explicit_db_path: Path | None = None) -> Path:
    if explicit_db_path is not None:
        return explicit_db_path.expanduser().resolve()

    env_path = os.getenv("ME_OPS_DB_PATH")
    if env_path:
        return Path(env_path).expanduser().resolve()

    return (PROJECT_ROOT / "me_ops.duckdb").resolve()


def _execute_ddl_block(con: duckdb.DuckDBPyConnection, ddl: str) -> None:
    for stmt in ddl.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def setup_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        _execute_ddl_block(con, ingest.DDL)
        _execute_ddl_block(con, workflows.WORKFLOW_DDL)
        _execute_ddl_block(con, entities.ENTITY_DDL)
        _execute_ddl_block(con, architect.SCHEMA_DDL)
        _execute_ddl_block(con, insights.INSIGHT_MEMORY_DDL)
        _execute_ddl_block(con, workflow_dna.DNA_DDL)
        _execute_ddl_block(con, MISTAKES_BOOTSTRAP_DDL)
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Setup a persistent ME-OPS database")
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="DuckDB file path (defaults to $ME_OPS_DB_PATH or ./me_ops.duckdb)",
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Run ingestion after schema setup",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=PROJECT_ROOT.parent,
        help="Directory containing Pieces JSON exports (used with --ingest)",
    )
    args = parser.parse_args()

    db_path = resolve_db_path(args.db)
    print(f"Setting up database at: {db_path}")
    setup_schema(db_path)

    if args.ingest:
        print(f"Running ingestion from: {args.data_dir}")
        if not ingest.run(args.data_dir, db_path):
            return 1

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchone()
        table_count = int(row[0]) if row else 0
    finally:
        con.close()

    print(f"Database ready. Tables available: {table_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
