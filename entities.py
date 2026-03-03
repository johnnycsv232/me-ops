"""
ME-OPS Entity Extraction
=========================
Cross-references entities (people, tools, projects, files) to build
a richer relationship graph. Adds computed tables:
  - entity_summary: aggregated stats per entity
  - tool_project_matrix: which tools are used in which projects
  - file_extensions: file type distribution per project

Usage:
    python entities.py [--db me_ops.duckdb]
"""

import argparse
import sys
from pathlib import Path

import duckdb
from typing import Optional

DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"

ENTITY_DDL = """
-- Aggregated entity summary
CREATE TABLE IF NOT EXISTS entity_summary (
    entity_type     VARCHAR NOT NULL,
    entity_id       VARCHAR NOT NULL,
    entity_name     VARCHAR,
    event_count     INTEGER DEFAULT 0,
    first_seen      TIMESTAMP,
    last_seen        TIMESTAMP,
    PRIMARY KEY (entity_type, entity_id)
);

-- Tool-project co-occurrence matrix
CREATE TABLE IF NOT EXISTS tool_project_matrix (
    tool_name       VARCHAR NOT NULL,
    project_name    VARCHAR NOT NULL,
    co_occurrences  INTEGER DEFAULT 0,
    PRIMARY KEY (tool_name, project_name)
);

-- File extension distribution
CREATE TABLE IF NOT EXISTS file_extension_stats (
    extension       VARCHAR,
    file_count      INTEGER DEFAULT 0,
    linked_events   INTEGER DEFAULT 0,
    top_repo        VARCHAR
);

-- Tag clusters (most connected tags)
CREATE TABLE IF NOT EXISTS tag_stats (
    tag_text        VARCHAR PRIMARY KEY,
    event_count     INTEGER DEFAULT 0,
    unique_actions  INTEGER DEFAULT 0,
    first_seen      TIMESTAMP,
    last_seen       TIMESTAMP
);
"""


def run(db_path: Path, con: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """Build entity cross-reference tables."""
    print("ME-OPS Entity Extraction")
    print("=" * 60)

    local_con = False
    if con is None:
        if not db_path.exists():
            print(f"❌ Database not found: {db_path}")
            return False
        con = duckdb.connect(str(db_path))
        local_con = True

    # Create schema
    for stmt in ENTITY_DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    # 1. Build entity_summary from tools
    print("  Building entity_summary...")
    con.execute("DELETE FROM entity_summary")

    # Tools
    con.execute("""
        INSERT INTO entity_summary
        SELECT 'tool', t.tool_id, t.name, COUNT(et.event_id),
               MIN(e.ts_start), MAX(e.ts_start)
        FROM tools t
        LEFT JOIN event_tools et ON t.tool_id = et.tool_id
        LEFT JOIN events e ON et.event_id = e.event_id
        GROUP BY t.tool_id, t.name
    """)

    # Projects
    con.execute("""
        INSERT INTO entity_summary
        SELECT 'project', p.project_id, p.name, COUNT(ep.event_id),
               p.first_seen, p.last_seen
        FROM projects p
        LEFT JOIN event_projects ep ON p.project_id = ep.project_id
        GROUP BY p.project_id, p.name, p.first_seen, p.last_seen
    """)

    # People
    con.execute("""
        INSERT INTO entity_summary
        SELECT 'person', person_id, name, 0, created_at, updated_at
        FROM people
    """)

    count_row = con.execute("SELECT COUNT(*) FROM entity_summary").fetchone()
    count = count_row[0] if count_row else 0
    print(f"    → {count} entities indexed")

    # 2. Tool-project co-occurrence
    print("  Building tool-project matrix...")
    con.execute("DELETE FROM tool_project_matrix")
    con.execute("""
        INSERT INTO tool_project_matrix
        SELECT t.name, p.name, COUNT(DISTINCT e.event_id)
        FROM events e
        JOIN event_tools et ON e.event_id = et.event_id
        JOIN tools t ON et.tool_id = t.tool_id
        JOIN event_projects ep ON e.event_id = ep.event_id
        JOIN projects p ON ep.project_id = p.project_id
        GROUP BY t.name, p.name
    """)
    count_row = con.execute("SELECT COUNT(*) FROM tool_project_matrix").fetchone()
    count = count_row[0] if count_row else 0
    print(f"    → {count} tool-project pairs")

    # 3. File extension stats
    print("  Building file extension stats...")
    con.execute("DELETE FROM file_extension_stats")
    con.execute("""
        INSERT INTO file_extension_stats
        SELECT f.extension,
               COUNT(DISTINCT f.file_id),
               COUNT(DISTINCT ef.event_id),
               -- MODE() returns most frequent value; ties are non-deterministic
               MODE(f.repo_root)
        FROM files f
        LEFT JOIN event_files ef ON f.file_id = ef.file_id
        GROUP BY f.extension
    """)
    count_row = con.execute("SELECT COUNT(*) FROM file_extension_stats").fetchone()
    count = count_row[0] if count_row else 0
    print(f"    → {count} file extensions tracked")

    # 4. Tag stats
    print("  Building tag stats...")
    con.execute("DELETE FROM tag_stats")
    con.execute("""
        INSERT INTO tag_stats
        SELECT et.tag_text, COUNT(DISTINCT et.event_id),
               0, NULL, NULL
        FROM event_tags et
        WHERE et.tag_text != ''
        GROUP BY et.tag_text
    """)
    count_row = con.execute("SELECT COUNT(*) FROM tag_stats").fetchone()
    count = count_row[0] if count_row else 0
    print(f"    → {count} tags analyzed")

    # Print top entities
    print(f"\n{'='*60}")
    print("  TOP ENTITIES BY EVENT COUNT:")
    rows = con.execute("""
        SELECT entity_type, entity_name, event_count
        FROM entity_summary
        WHERE event_count > 0
        ORDER BY event_count DESC LIMIT 15
    """).fetchall()
    for r in rows:
        print(f"    [{r[0]:<8}] {r[1]:<30} {r[2]:>6} events")

    print("\n  TOP TAGS:")
    rows = con.execute("""
        SELECT tag_text, event_count FROM tag_stats
        ORDER BY event_count DESC LIMIT 10
    """).fetchall()
    for r in rows:
        print(f"    {r[0]:<40} {r[1]:>6} events")

    if local_con:
        con.close()
    print(f"\n{'='*60}")
    print("✅ Entity extraction complete")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ME-OPS Entity Extraction")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    args = parser.parse_args()
    sys.exit(0 if run(args.db) else 1)
