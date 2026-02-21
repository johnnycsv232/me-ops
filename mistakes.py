"""
ME-OPS Failure Pattern Detection
=================================
Detects failure patterns, context-switches, and inefficiency signals
from event data. Builds anti-playbooks: "when you do X, you tend to fail."

Creates tables:
  - failure_patterns: detected failure/inefficiency signals
  - context_switches: rapid project/tool switches (thrashing)
  - anti_playbook: evidence-backed "don't do" rules

Usage:
    python mistakes.py [--db me_ops.duckdb]
"""

import argparse
import sys
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"

MISTAKES_DDL = """
CREATE TABLE IF NOT EXISTS failure_patterns (
    pattern_id      INTEGER PRIMARY KEY,
    pattern_type    VARCHAR NOT NULL,
    description     VARCHAR NOT NULL,
    evidence_count  INTEGER DEFAULT 0,
    severity        VARCHAR DEFAULT 'medium',
    example_events  VARCHAR
);

CREATE TABLE IF NOT EXISTS context_switches (
    switch_id       INTEGER PRIMARY KEY,
    ts              TIMESTAMP,
    from_project    VARCHAR,
    to_project      VARCHAR,
    gap_seconds     DOUBLE,
    session_id      INTEGER
);

CREATE TABLE IF NOT EXISTS anti_playbook (
    rule_id         INTEGER PRIMARY KEY,
    rule_text       VARCHAR NOT NULL,
    trigger         VARCHAR,
    evidence        VARCHAR,
    confidence      DOUBLE DEFAULT 0.5
);
"""


def detect_context_thrashing(con: duckdb.DuckDBPyConnection) -> int:
    """Detect rapid project switches within sessions (thrashing)."""
    con.execute("DELETE FROM context_switches")

    # Get events with project associations, ordered by time
    rows = con.execute("""
        SELECT e.event_id, e.ts_start, p.name AS project,
               s.session_id
        FROM events e
        JOIN event_projects ep ON e.event_id = ep.event_id
        JOIN projects p ON ep.project_id = p.project_id
        LEFT JOIN sessions s ON e.ts_start BETWEEN s.ts_start AND s.ts_end
        WHERE e.ts_start IS NOT NULL
        ORDER BY e.ts_start
    """).fetchall()

    switches = []
    switch_id = 0
    for i in range(1, len(rows)):
        prev_proj = rows[i - 1][2]
        curr_proj = rows[i][2]
        if prev_proj != curr_proj:
            gap = (rows[i][1] - rows[i - 1][1]).total_seconds()
            if gap < 300:  # switch within 5 minutes = thrashing
                switches.append((
                    switch_id, rows[i][1], prev_proj, curr_proj,
                    round(gap, 1), rows[i][3]
                ))
                switch_id += 1

    if switches:
        con.executemany(
            "INSERT INTO context_switches VALUES (?,?,?,?,?,?)",
            switches[:1000],  # cap at 1000
        )
    return len(switches)


def detect_failure_patterns(con: duckdb.DuckDBPyConnection) -> int:
    """Detect failure/inefficiency patterns from events."""
    con.execute("DELETE FROM failure_patterns")
    patterns = []
    pid = 0

    # Pattern 1: Late-night sessions (after 11 PM, before 5 AM)
    late_count = con.execute("""
        SELECT COUNT(*) FROM events
        WHERE ts_start IS NOT NULL
        AND (EXTRACT(HOUR FROM ts_start) >= 23
             OR EXTRACT(HOUR FROM ts_start) < 5)
    """).fetchone()[0]
    if late_count > 10:
        patterns.append((
            pid, "late_night_work",
            f"Working between 11PM-5AM detected ({late_count} events). "
            "Late sessions correlate with higher error rates.",
            late_count, "high", None
        ))
        pid += 1

    # Pattern 2: Context thrashing (>5 switches per session)
    thrash = con.execute("""
        SELECT session_id, COUNT(*) AS switches
        FROM context_switches
        WHERE session_id IS NOT NULL
        GROUP BY session_id
        HAVING COUNT(*) > 5
    """).fetchall()
    if thrash:
        patterns.append((
            pid, "context_thrashing",
            f"{len(thrash)} sessions with >5 project switches. "
            "Rapid context-switching kills deep work.",
            sum(r[1] for r in thrash), "high",
            str([r[0] for r in thrash[:5]])
        ))
        pid += 1

    # Pattern 3: Very long sessions (>4 hours without break)
    long_sess = con.execute("""
        SELECT COUNT(*) FROM sessions
        WHERE duration_min > 240
    """).fetchone()[0]
    if long_sess > 0:
        patterns.append((
            pid, "marathon_sessions",
            f"{long_sess} sessions over 4 hours. Productivity drops "
            "significantly after 90-minute focused blocks.",
            long_sess, "medium", None
        ))
        pid += 1

    # Pattern 4: Single-action sessions (doing only one thing)
    single = con.execute("""
        SELECT COUNT(*) FROM sessions
        WHERE unique_actions = 1 AND event_count > 10
    """).fetchone()[0]
    if single > 0:
        patterns.append((
            pid, "tunnel_vision",
            f"{single} sessions with only 1 action type but 10+ events. "
            "May indicate stuck-in-a-loop behavior.",
            single, "low", None
        ))
        pid += 1

    # Pattern 5: Web browsing dominance
    web_pct = con.execute("""
        SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE action = 'web_visit')
               / NULLIF(COUNT(*), 0), 1)
        FROM events
    """).fetchone()[0]
    if web_pct and web_pct > 40:
        patterns.append((
            pid, "web_browsing_heavy",
            f"Web visits make up {web_pct}% of all events. "
            "High browsing ratio may indicate research loops or distraction.",
            int(web_pct), "medium", None
        ))
        pid += 1

    # Pattern 6: Weekend work
    weekend = con.execute("""
        SELECT COUNT(*) FROM events
        WHERE ts_start IS NOT NULL
        AND EXTRACT(DOW FROM ts_start) IN (0, 6)
    """).fetchone()[0]
    if weekend > 50:
        patterns.append((
            pid, "weekend_work",
            f"{weekend} events on weekends. Consistent weekend work "
            "without recovery increases burnout risk.",
            weekend, "medium", None
        ))
        pid += 1

    if patterns:
        con.executemany(
            "INSERT INTO failure_patterns VALUES (?,?,?,?,?,?)",
            patterns,
        )
    return len(patterns)


def build_anti_playbook(con: duckdb.DuckDBPyConnection) -> int:
    """Build evidence-backed anti-playbook rules."""
    con.execute("DELETE FROM anti_playbook")
    rules = []
    rid = 0

    # Rule from context thrashing
    thrash_count = con.execute(
        "SELECT COUNT(*) FROM context_switches"
    ).fetchone()[0]
    if thrash_count > 20:
        rules.append((
            rid,
            "Do NOT switch projects more than 3x per session. "
            "Each switch costs ~23 minutes of re-focus.",
            "project_switch > 3/session",
            f"{thrash_count} rapid switches detected",
            0.85
        ))
        rid += 1

    # Rule from late-night work
    late = con.execute("""
        SELECT COUNT(*) FROM events
        WHERE EXTRACT(HOUR FROM ts_start) >= 23
           OR EXTRACT(HOUR FROM ts_start) < 5
    """).fetchone()[0]
    if late > 10:
        rules.append((
            rid,
            "STOP working after 11 PM. Late-night code has 2-3x "
            "higher defect density. Sleep > shipping.",
            "hour >= 23 OR hour < 5",
            f"{late} late-night events",
            0.80
        ))
        rid += 1

    # Rule from marathon sessions
    marathon = con.execute("""
        SELECT COUNT(*) FROM sessions WHERE duration_min > 240
    """).fetchone()[0]
    if marathon > 0:
        rules.append((
            rid,
            "BREAK every 90 minutes. Sessions over 4 hours show "
            "diminishing returns. Use Pomodoro or similar.",
            "session_duration > 240min",
            f"{marathon} marathon sessions detected",
            0.75
        ))
        rid += 1

    # Rule from web browsing ratio
    web_pct = con.execute("""
        SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE action = 'web_visit')
               / NULLIF(COUNT(*), 0), 1) FROM events
    """).fetchone()[0]
    if web_pct and web_pct > 40:
        rules.append((
            rid,
            "LIMIT web browsing to research sprints. "
            f"Currently {web_pct}% of activity is browsing. "
            "Set a 15-min timer for research.",
            f"web_visit_pct > 40%",
            f"web_visit is {web_pct}% of all events",
            0.70
        ))
        rid += 1

    if rules:
        con.executemany(
            "INSERT INTO anti_playbook VALUES (?,?,?,?,?)",
            rules,
        )
    return len(rules)


def run(db_path: Path) -> bool:
    """Execute failure detection pipeline."""
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return False

    con = duckdb.connect(str(db_path))
    print("ME-OPS Failure Pattern Detection")
    print("=" * 60)

    # Create schema
    for stmt in MISTAKES_DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    # Requires sessions table from workflows.py
    has_sessions = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'sessions'"
    ).fetchone()[0]
    if not has_sessions:
        print("  ⚠️ No sessions table. Run workflows.py first.")
        print("  Skipping context-switch detection.")

    # 1. Context thrashing
    if has_sessions:
        print("  Detecting context switches...")
        switches = detect_context_thrashing(con)
        print(f"    → {switches} rapid switches detected")

    # 2. Failure patterns
    print("  Analyzing failure patterns...")
    patterns = detect_failure_patterns(con)
    print(f"    → {patterns} patterns found")

    # 3. Anti-playbook
    print("  Building anti-playbook...")
    rules = build_anti_playbook(con)
    print(f"    → {rules} rules generated")

    # Print results
    print(f"\n{'='*60}")
    print("  FAILURE PATTERNS:")
    rows = con.execute("""
        SELECT severity, pattern_type, description
        FROM failure_patterns ORDER BY
            CASE severity WHEN 'high' THEN 1
                          WHEN 'medium' THEN 2 ELSE 3 END
    """).fetchall()
    for r in rows:
        icon = "🔴" if r[0] == "high" else "🟡" if r[0] == "medium" else "🟢"
        print(f"    {icon} [{r[1]}] {r[2]}")

    print(f"\n  ANTI-PLAYBOOK RULES:")
    rows = con.execute("""
        SELECT rule_text, confidence FROM anti_playbook
        ORDER BY confidence DESC
    """).fetchall()
    for r in rows:
        print(f"    ⛔ {r[0]} (confidence: {r[1]:.0%})")

    con.close()
    print(f"\n{'='*60}")
    print("✅ Failure analysis complete")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ME-OPS Failure Detection")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    args = parser.parse_args()
    sys.exit(0 if run(args.db) else 1)
