from __future__ import annotations

from pathlib import Path
import sys

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.setup_real_database import setup_schema


def test_setup_schema_creates_real_database_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "real.duckdb"
    setup_schema(db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            row[0]
            for row in con.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
    finally:
        con.close()

    expected_tables = {
        "events",
        "sessions",
        "workflow_patterns",
        "coaching_rules",
        "daily_scores",
        "insight_memory",
        "workflow_dna_markers",
        "failure_patterns",
        "anti_playbook",
    }
    assert expected_tables.issubset(tables)
