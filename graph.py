#!/usr/bin/env python3
"""ME-OPS Knowledge Graph — Pure-SQL property graph over behavioral data.

Builds an adjacency-based knowledge graph from existing link tables
(event_projects, event_tools, event_files, event_tags) and exposes
multi-hop traversal queries without requiring the DuckPGQ extension.

Skills used: software-architecture (DDD: entities as vertices, clean separation),
             production-code-audit (verification loop)

Usage:
    python graph.py              # Build graph + run all traversals
    python graph.py --query      # Interactive graph query mode
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parent / "me_ops.duckdb"


# ---------------------------------------------------------------------------
# 1. Graph construction — materialized edge tables
# ---------------------------------------------------------------------------

def build_graph(con: duckdb.DuckDBPyConnection) -> None:
    """Create materialized graph edges from link tables."""

    # Unified edge table: (src_type, src_id, rel, dst_type, dst_id, weight)
    con.execute("DROP TABLE IF EXISTS graph_edges")
    con.execute("""
        CREATE TABLE graph_edges (
            src_type  VARCHAR NOT NULL,
            src_id    VARCHAR NOT NULL,
            rel       VARCHAR NOT NULL,
            dst_type  VARCHAR NOT NULL,
            dst_id    VARCHAR NOT NULL,
            weight    INTEGER DEFAULT 1
        )
    """)

    # Event → Project edges
    con.execute("""
        INSERT INTO graph_edges
        SELECT 'event', ep.event_id, 'belongs_to', 'project', ep.project_id, 1
        FROM event_projects ep
    """)

    # Event → Tool edges
    con.execute("""
        INSERT INTO graph_edges
        SELECT 'event', et.event_id, 'uses', 'tool', et.tool_id, 1
        FROM event_tools et
    """)

    # Event → File edges
    con.execute("""
        INSERT INTO graph_edges
        SELECT 'event', ef.event_id, 'touches', 'file', ef.file_id, 1
        FROM event_files ef
    """)

    # Project ↔ Tool co-occurrence (from tool_project_matrix)
    con.execute("""
        INSERT INTO graph_edges
        SELECT 'project', p.project_id, 'uses_tool', 'tool', t.tool_id,
               tpm.co_occurrences
        FROM tool_project_matrix tpm
        JOIN projects p ON p.name = tpm.project_name
        JOIN tools t ON t.name = tpm.tool_name
        WHERE tpm.co_occurrences > 0
    """)

    # Session → Project edges (parsed from sessions.projects CSV)
    con.execute("""
        INSERT INTO graph_edges
        SELECT 'session', CAST(s.session_id AS VARCHAR), 'works_on',
               'project', p.project_id, s.event_count
        FROM sessions s
        JOIN projects p ON s.projects LIKE '%' || p.name || '%'
        WHERE s.projects IS NOT NULL AND s.projects != ''
    """)

    # Workflow transition edges (action → action)
    con.execute("""
        INSERT INTO graph_edges
        SELECT 'action', we.from_action, 'transitions_to',
               'action', we.to_action, we.weight
        FROM workflow_edges we
    """)

    total = con.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    node_types = con.execute("""
        SELECT src_type, COUNT(DISTINCT src_id) FROM graph_edges GROUP BY src_type
        UNION ALL
        SELECT dst_type, COUNT(DISTINCT dst_id) FROM graph_edges GROUP BY dst_type
    """).fetchall()

    # Aggregate node counts by type
    type_counts: dict[str, int] = defaultdict(int)
    for t, c in node_types:
        type_counts[t] = max(type_counts[t], c)

    print(f"  Graph built: {total:,} edges")
    for t, c in sorted(type_counts.items()):
        print(f"    {t}: {c:,} nodes")

    # Create index for traversal performance
    con.execute("CREATE INDEX IF NOT EXISTS idx_graph_src ON graph_edges(src_type, src_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_graph_dst ON graph_edges(dst_type, dst_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_graph_rel ON graph_edges(rel)")


# ---------------------------------------------------------------------------
# 2. Traversal queries
# ---------------------------------------------------------------------------

def query_neighbors(
    con: duckdb.DuckDBPyConnection,
    node_type: str,
    node_id: str,
    depth: int = 1,
) -> list[tuple]:
    """Find all neighbors within N hops from a given node."""
    if depth == 1:
        return con.execute("""
            SELECT rel, dst_type, dst_id, weight
            FROM graph_edges
            WHERE src_type = ? AND src_id = ?
            UNION ALL
            SELECT rel, src_type, src_id, weight
            FROM graph_edges
            WHERE dst_type = ? AND dst_id = ?
            ORDER BY weight DESC
        """, [node_type, node_id, node_type, node_id]).fetchall()

    # Multi-hop via recursive CTE
    return con.execute("""
        WITH RECURSIVE hops AS (
            -- Base: direct neighbors
            SELECT 1 AS depth, rel, dst_type AS node_type, dst_id AS node_id, weight
            FROM graph_edges
            WHERE src_type = ? AND src_id = ?
            UNION ALL
            SELECT 1 AS depth, rel, src_type, src_id, weight
            FROM graph_edges
            WHERE dst_type = ? AND dst_id = ?

            UNION ALL

            -- Recursive: next hop
            SELECT h.depth + 1, g.rel, g.dst_type, g.dst_id, g.weight
            FROM hops h
            JOIN graph_edges g ON g.src_type = h.node_type AND g.src_id = h.node_id
            WHERE h.depth < ?
        )
        SELECT DISTINCT depth, rel, node_type, node_id, weight
        FROM hops
        ORDER BY depth, weight DESC
    """, [node_type, node_id, node_type, node_id, depth]).fetchall()


def project_tool_report(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Which tools does each project use, ranked by edge weight?"""
    return con.execute("""
        SELECT p.name AS project, t.name AS tool, ge.weight AS strength
        FROM graph_edges ge
        JOIN projects p ON ge.src_id = p.project_id
        JOIN tools t ON ge.dst_id = t.tool_id
        WHERE ge.rel = 'uses_tool'
        ORDER BY ge.weight DESC
    """).fetchall()


def action_hub_scores(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Compute hub score for actions: sum of inbound + outbound transition weights."""
    return con.execute("""
        SELECT node, SUM(total_weight) AS hub_score FROM (
            SELECT src_id AS node, SUM(weight) AS total_weight
            FROM graph_edges WHERE rel = 'transitions_to'
            GROUP BY src_id
            UNION ALL
            SELECT dst_id AS node, SUM(weight) AS total_weight
            FROM graph_edges WHERE rel = 'transitions_to'
            GROUP BY dst_id
        )
        GROUP BY node
        ORDER BY hub_score DESC
        LIMIT 10
    """).fetchall()


def project_file_hotspots(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Find files most connected to each project (2-hop: project←event→file)."""
    return con.execute("""
        SELECT p.name AS project, f.fullpath AS file, COUNT(*) AS touches
        FROM graph_edges gp
        JOIN graph_edges gf ON gp.src_id = gf.src_id AND gf.rel = 'touches'
        JOIN projects p ON gp.dst_id = p.project_id
        JOIN files f ON gf.dst_id = f.file_id
        WHERE gp.rel = 'belongs_to'
        GROUP BY p.name, f.fullpath
        ORDER BY touches DESC
        LIMIT 20
    """).fetchall()


def temporal_project_graph(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """How do projects connect over time? (same-session co-occurrence)."""
    return con.execute("""
        SELECT p1.name AS project_a, p2.name AS project_b,
               COUNT(DISTINCT s.session_id) AS shared_sessions
        FROM sessions s
        JOIN projects p1 ON s.projects LIKE '%' || p1.name || '%'
        JOIN projects p2 ON s.projects LIKE '%' || p2.name || '%'
        WHERE p1.project_id < p2.project_id
        GROUP BY p1.name, p2.name
        HAVING shared_sessions > 0
        ORDER BY shared_sessions DESC
    """).fetchall()


def late_night_project_analysis(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Which projects are you working on after 11 PM? (graph + temporal)."""
    return con.execute("""
        SELECT p.name AS project,
               COUNT(*) AS late_events,
               MIN(e.ts_start) AS earliest_late,
               MAX(e.ts_start) AS latest_late
        FROM events e
        JOIN event_projects ep ON e.event_id = ep.event_id
        JOIN projects p ON ep.project_id = p.project_id
        WHERE EXTRACT(HOUR FROM e.ts_start) >= 23
           OR EXTRACT(HOUR FROM e.ts_start) < 5
        GROUP BY p.name
        ORDER BY late_events DESC
    """).fetchall()


# ---------------------------------------------------------------------------
# 3. Interactive query mode
# ---------------------------------------------------------------------------

def interactive_mode(con: duckdb.DuckDBPyConnection) -> None:
    """REPL for graph exploration."""
    print("\n  Interactive Graph Query Mode")
    print("  Commands: neighbors <type> <id> [depth], hubs, files, temporal, late, quit\n")
    while True:
        try:
            cmd = input("graph> ").strip().split()
        except (EOFError, KeyboardInterrupt):
            break
        if not cmd or cmd[0] == "quit":
            break
        elif cmd[0] == "neighbors" and len(cmd) >= 3:
            depth = int(cmd[3]) if len(cmd) > 3 else 1
            rows = query_neighbors(con, cmd[1], cmd[2], depth)
            for r in rows[:30]:
                print(f"    {r}")
        elif cmd[0] == "hubs":
            for r in action_hub_scores(con):
                print(f"    {r[0]:30s}  score={r[1]}")
        elif cmd[0] == "files":
            for r in project_file_hotspots(con):
                print(f"    [{r[0]:15s}] {r[2]:4d}x  {r[1]}")
        elif cmd[0] == "temporal":
            for r in temporal_project_graph(con):
                print(f"    {r[0]:20s} ↔ {r[1]:20s}  {r[2]} shared sessions")
        elif cmd[0] == "late":
            for r in late_night_project_analysis(con):
                print(f"    {r[0]:20s}  {r[1]} late events ({r[2]} → {r[3]})")
        else:
            print("    Unknown command. Try: neighbors, hubs, files, temporal, late, quit")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("ME-OPS Knowledge Graph")
    print("=" * 60)

    con = duckdb.connect(str(DB_PATH))

    print("  Building graph edges...")
    build_graph(con)

    # --- Run standard reports ---
    print()
    print("=" * 60)
    print("  PROJECT ↔ TOOL RELATIONSHIPS:")
    for r in project_tool_report(con):
        print(f"    {r[0]:20s} → {r[1]:30s}  (strength: {r[2]})")

    print()
    print("  ACTION HUB SCORES (most connected actions):")
    for r in action_hub_scores(con):
        print(f"    {r[0]:30s}  hub_score={r[1]}")

    print()
    print("  FILE HOTSPOTS (most touched per project):")
    for r in project_file_hotspots(con)[:10]:
        print(f"    [{r[0]:15s}] {r[2]:4d}x  {r[1]}")

    print()
    print("  TEMPORAL PROJECT GRAPH (session co-occurrence):")
    for r in temporal_project_graph(con):
        print(f"    {r[0]:20s} ↔ {r[1]:20s}  {r[2]} shared sessions")

    print()
    print("  LATE NIGHT ANALYSIS (events after 11 PM):")
    for r in late_night_project_analysis(con):
        print(f"    {r[0]:20s}  {r[1]} late events")

    print()
    print("=" * 60)
    print("✅ Knowledge graph complete")

    if "--query" in sys.argv:
        interactive_mode(con)

    con.close()


if __name__ == "__main__":
    main()
