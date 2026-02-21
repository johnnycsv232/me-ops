"""
ME-OPS Validation Queries
=========================
10 queries that validate data integrity and surface key insights from the DuckDB warehouse.

Usage:
    python queries.py [--db me_ops.duckdb]
"""

import argparse
import sys
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"

QUERIES: list[tuple[str, str]] = [
    # --- Data Integrity ---
    (
        "1. Source coverage (all 19 files registered)",
        "SELECT COUNT(*) AS sources, SUM(record_count) AS total_records, "
        "printf('%.1f MB', SUM(byte_size)/1e6) AS total_size FROM raw_sources",
    ),
    (
        "2. Event count by source file",
        "SELECT source_file, COUNT(*) AS events FROM events "
        "GROUP BY source_file ORDER BY events DESC",
    ),
    (
        "3. Events with NULL timestamps (data quality check)",
        "SELECT COUNT(*) AS null_ts_events, "
        "ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM events), 1) AS pct "
        "FROM events WHERE ts_start IS NULL",
    ),
    (
        "4. Duplicate event IDs (should be 0)",
        "SELECT event_id, COUNT(*) AS dupes FROM events "
        "GROUP BY event_id HAVING COUNT(*) > 1 LIMIT 10",
    ),
    # --- Behavioral Insights ---
    (
        "5. Daily activity heatmap (last 14 days)",
        "SELECT ts_start::DATE AS day, COUNT(*) AS events, "
        "COUNT(DISTINCT action) AS unique_actions "
        "FROM events WHERE ts_start IS NOT NULL "
        "AND ts_start >= CURRENT_DATE - INTERVAL '14 days' "
        "GROUP BY day ORDER BY day DESC",
    ),
    (
        "6. Top 10 actions by frequency",
        "SELECT action, COUNT(*) AS freq, "
        "COUNT(DISTINCT source_file) AS sources "
        "FROM events GROUP BY action ORDER BY freq DESC LIMIT 10",
    ),
    (
        "7. Project activity summary",
        "SELECT p.name, COUNT(ep.event_id) AS events, "
        "p.first_seen::DATE AS started, p.last_seen::DATE AS last_active, "
        "DATEDIFF('day', p.first_seen, p.last_seen) AS span_days "
        "FROM projects p "
        "JOIN event_projects ep ON p.project_id = ep.project_id "
        "GROUP BY p.name, p.first_seen, p.last_seen "
        "ORDER BY events DESC",
    ),
    (
        "8. Tool usage ranking",
        "SELECT t.name AS tool, COUNT(et.event_id) AS events "
        "FROM tools t JOIN event_tools et ON t.tool_id = et.tool_id "
        "GROUP BY t.name ORDER BY events DESC",
    ),
    (
        "9. Top 15 tags by event linkage",
        "SELECT tag_text, COUNT(DISTINCT event_id) AS linked_events "
        "FROM event_tags WHERE tag_text != '' "
        "GROUP BY tag_text ORDER BY linked_events DESC LIMIT 15",
    ),
    (
        "10. Hourly distribution (when are you most active?)",
        "SELECT EXTRACT(HOUR FROM ts_start) AS hour, COUNT(*) AS events "
        "FROM events WHERE ts_start IS NOT NULL "
        "GROUP BY hour ORDER BY hour",
    ),
]


def run(db_path: Path) -> bool:
    """Execute all validation queries and print results."""
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        print("   Run ingest.py first.")
        return False

    con = duckdb.connect(str(db_path), read_only=True)
    all_pass = True

    for title, sql in QUERIES:
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")
        try:
            rows = con.execute(sql).fetchall()
            cols = [desc[0] for desc in con.description]

            if not rows:
                print("  (no results)")
                continue

            # Auto-format column widths
            widths = [max(len(str(c)), max(len(str(r[i])) for r in rows))
                      for i, c in enumerate(cols)]
            header = "  ".join(f"{c:<{w}}" for c, w in zip(cols, widths))
            print(f"  {header}")
            print(f"  {'  '.join('-' * w for w in widths)}")
            for row in rows:
                line = "  ".join(f"{str(v):<{w}}" for v, w in zip(row, widths))
                print(f"  {line}")

        except Exception as e:
            print(f"  ❌ ERROR: {e}")
            all_pass = False

    con.close()
    status = "✅ ALL QUERIES PASSED" if all_pass else "⚠️ SOME QUERIES FAILED"
    print(f"\n{'='*60}")
    print(f"  {status}")
    print(f"{'='*60}")
    return all_pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ME-OPS Validation Queries")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    args = parser.parse_args()
    sys.exit(0 if run(args.db) else 1)
