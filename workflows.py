"""
ME-OPS Workflow Mining
======================
Mines session sequences, finds common workflows,
and identifies behavioral patterns from event chains.

Creates tables:
  - sessions: detected work sessions with boundaries
  - workflow_edges: action-to-action transition graph
  - workflow_patterns: most common action sequences

Usage:
    python workflows.py [--db me_ops.duckdb] [--gap-minutes 30]
"""

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

import duckdb

DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"

WORKFLOW_DDL = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id      INTEGER PRIMARY KEY,
    ts_start        TIMESTAMP NOT NULL,
    ts_end          TIMESTAMP NOT NULL,
    duration_min    DOUBLE,
    event_count     INTEGER,
    unique_actions  INTEGER,
    dominant_action VARCHAR,
    projects        VARCHAR
);

CREATE TABLE IF NOT EXISTS workflow_edges (
    from_action     VARCHAR NOT NULL,
    to_action       VARCHAR NOT NULL,
    weight          INTEGER DEFAULT 1,
    avg_gap_sec     DOUBLE,
    PRIMARY KEY (from_action, to_action)
);

CREATE TABLE IF NOT EXISTS workflow_patterns (
    pattern_id      INTEGER PRIMARY KEY,
    sequence        VARCHAR NOT NULL,
    frequency       INTEGER DEFAULT 1,
    avg_duration_min DOUBLE,
    example_session INTEGER
);
"""


def detect_sessions(
    con: duckdb.DuckDBPyConnection, gap_minutes: int = 30
) -> list[dict]:
    """Detect work sessions by splitting on time gaps."""
    rows = con.execute("""
        SELECT event_id, ts_start, action
        FROM events
        WHERE ts_start IS NOT NULL
        ORDER BY ts_start
    """).fetchall()

    if not rows:
        return []

    sessions: list[dict] = []
    current: dict = {
        "events": [rows[0]],
        "actions": [rows[0][2]],
    }

    for i in range(1, len(rows)):
        prev_ts = rows[i - 1][1]
        curr_ts = rows[i][1]
        gap = (curr_ts - prev_ts).total_seconds() / 60.0

        if gap > gap_minutes:
            # Close current session
            sessions.append(current)
            current = {"events": [rows[i]], "actions": [rows[i][2]]}
        else:
            current["events"].append(rows[i])
            current["actions"].append(rows[i][2])

    sessions.append(current)  # last session
    return sessions


def build_transition_graph(
    sessions: list[dict],
) -> tuple[set[str], list[tuple[str, str, int, float]]]:
    """Build action transition edges from sessions."""
    nodes: set[str] = set()
    edge_stats: dict[tuple[str, str], tuple[int, float]] = {}

    for sess in sessions:
        events = sess["events"]
        for i in range(1, len(events)):
            a = events[i - 1][2]  # action
            b = events[i][2]
            gap = (events[i][1] - events[i - 1][1]).total_seconds()
            nodes.add(a)
            nodes.add(b)
            weight, gap_sum = edge_stats.get((a, b), (0, 0.0))
            edge_stats[(a, b)] = (weight + 1, gap_sum + gap)

    edges = [
        (a, b, weight, round(gap_sum / max(weight, 1), 1))
        for (a, b), (weight, gap_sum) in edge_stats.items()
    ]
    return nodes, edges


def compute_action_centrality(
    edge_rows: list[tuple[str, str, int, float]],
) -> dict[str, float]:
    """Compute a lightweight weighted centrality score from edge weights."""
    scores: dict[str, float] = {}
    for from_action, to_action, weight, _ in edge_rows:
        scores[from_action] = scores.get(from_action, 0.0) + float(weight)
        scores[to_action] = scores.get(to_action, 0.0) + float(weight)

    total = sum(scores.values()) or 1.0
    return {action: value / total for action, value in scores.items()}


def mine_patterns(sessions: list[dict], window: int = 3) -> list[tuple]:
    """Mine common action sequences (n-grams) from sessions."""
    ngrams: Counter[str] = Counter()
    session_examples: dict[str, int] = {}

    for sid, sess in enumerate(sessions):
        actions = sess["actions"]
        for i in range(len(actions) - window + 1):
            seq = " → ".join(actions[i : i + window])
            ngrams[seq] += 1
            if seq not in session_examples:
                session_examples[seq] = sid

    return [
        (seq, count, session_examples[seq])
        for seq, count in ngrams.most_common(50)
        if count >= 3
    ]


def _event_projects_map(con: duckdb.DuckDBPyConnection) -> dict[str, set[str]]:
    """Build event_id -> project-name mapping in one query."""
    mapping: dict[str, set[str]] = {}
    rows = con.execute("""
        SELECT ep.event_id, p.name
        FROM event_projects ep
        JOIN projects p ON ep.project_id = p.project_id
    """).fetchall()
    for event_id, project_name in rows:
        mapping.setdefault(event_id, set()).add(project_name)
    return mapping


def run(
    db_path: Path,
    gap_minutes: int = 30,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> bool:
    """Execute workflow mining pipeline."""
    if con is None and not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return False

    owns_connection = con is None
    if con is None:
        con = duckdb.connect(str(db_path))
    print("ME-OPS Workflow Mining")
    print("=" * 60)

    try:
        # Create schema
        for stmt in WORKFLOW_DDL.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        # 1. Detect sessions
        print(f"  Detecting sessions (gap threshold: {gap_minutes}min)...")
        sessions = detect_sessions(con, gap_minutes)
        print(f"    → {len(sessions)} sessions detected")

        event_projects = _event_projects_map(con)

        # Store sessions
        con.execute("DELETE FROM sessions")
        session_rows: list[list[object]] = []
        for sid, sess in enumerate(sessions):
            events = sess["events"]
            actions = sess["actions"]
            ts_start = events[0][1]
            ts_end = events[-1][1]
            duration = (ts_end - ts_start).total_seconds() / 60.0
            dominant = Counter(actions).most_common(1)[0][0] if actions else None

            session_projects: set[str] = set()
            for event_id, _, _ in events:
                session_projects.update(event_projects.get(event_id, set()))
            projects = ", ".join(sorted(session_projects)) if session_projects else None

            session_rows.append(
                [
                    sid,
                    ts_start,
                    ts_end,
                    round(duration, 1),
                    len(events),
                    len(set(actions)),
                    dominant,
                    projects,
                ]
            )

        con.execute("BEGIN TRANSACTION")
        try:
            if session_rows:
                con.executemany(
                    "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
                    session_rows,
                )

            # 2. Build transition graph
            print("  Building action transition graph...")
            nodes, edge_rows = build_transition_graph(sessions)
            con.execute("DELETE FROM workflow_edges")
            if edge_rows:
                con.executemany(
                    "INSERT INTO workflow_edges VALUES (?,?,?,?)",
                    [[a, b, weight, avg_gap] for a, b, weight, avg_gap in edge_rows],
                )
            print(f"    → {len(nodes)} nodes, {len(edge_rows)} edges")

            # 3. Mine patterns
            print("  Mining action patterns (3-grams)...")
            patterns = mine_patterns(sessions)
            con.execute("DELETE FROM workflow_patterns")
            pattern_rows = [
                [pid, seq, freq, None, example_sid]
                for pid, (seq, freq, example_sid) in enumerate(patterns)
            ]
            if pattern_rows:
                con.executemany(
                    "INSERT INTO workflow_patterns VALUES (?,?,?,?,?)",
                    pattern_rows,
                )
            print(f"    → {len(patterns)} recurring patterns found")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        # Print insights
        print(f"\n{'='*60}")
        print("  SESSION STATS:")
        rows = con.execute("""
            SELECT COUNT(*) AS sessions,
                   ROUND(AVG(duration_min), 1) AS avg_min,
                   ROUND(AVG(event_count), 0) AS avg_events,
                   MAX(duration_min) AS longest_min
            FROM sessions
        """).fetchone()
        print(f"    Sessions: {rows[0]}, Avg: {rows[1]}min, "
              f"Avg events: {rows[2]}, Longest: {rows[3]}min")

        print("\n  TOP 10 WORKFLOW PATTERNS:")
        rows = con.execute("""
            SELECT sequence, frequency FROM workflow_patterns
            ORDER BY frequency DESC LIMIT 10
        """).fetchall()
        for r in rows:
            print(f"    [{r[1]:>4}x] {r[0]}")

        print("\n  STRONGEST TRANSITIONS:")
        rows = con.execute("""
            SELECT from_action, to_action, weight, avg_gap_sec
            FROM workflow_edges ORDER BY weight DESC LIMIT 10
        """).fetchall()
        for r in rows:
            print(f"    {r[0]} → {r[1]}  ({r[2]}x, avg gap: {r[3]:.0f}s)")

        # Action centrality
        if nodes:
            print("\n  GRAPH CENTRALITY (most pivotal actions):")
            centrality = compute_action_centrality(edge_rows)
            top = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:8]
            for action, score in top:
                print(f"    {action:<40} Score: {score:.4f}")

        print(f"\n{'='*60}")
        print("✅ Workflow mining complete")
        return True
    finally:
        if owns_connection:
            con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ME-OPS Workflow Mining")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--gap-minutes", type=int, default=30,
                        help="Minutes of inactivity to split sessions")
    args = parser.parse_args()
    sys.exit(0 if run(args.db, args.gap_minutes) else 1)
