import os
import sys
import argparse
import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import duckdb

MISTAKES_DDL = """
DROP TABLE IF EXISTS failure_patterns;
DROP TABLE IF EXISTS anti_playbook;
DROP SEQUENCE IF EXISTS fail_seq;

CREATE SEQUENCE fail_seq;
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

# --- Detection Logic ---

def detect_context_thrashing(con: duckdb.DuckDBPyConnection) -> int:
    """Detect rapid project switching."""
    # Logic is simplified for Level 10 baseline
    res = con.execute("""
        SELECT COUNT(*) FROM sessions
        WHERE (length(projects) - length(replace(projects, ',', ''))) >= 3
    """).fetchone()
    count = res[0] if res else 0

    if count > 0:
        con.execute("""
            INSERT INTO failure_patterns (pattern_type, description, severity, evidence_count, last_detected)
            VALUES ('context_thrashing', 'Switching projects more than 3x in a session', 'high', ?, CURRENT_TIMESTAMP)
        """, [count])
    return count

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
            f"{long_sess} sessions over 4 hours. Sustained intensity tends "
            "to decline in extended sessions; consider scheduled breaks.",
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
    """Generate rules to prevent known failure modes."""
    rules: List[Tuple[Any, ...]] = []
    rid = 1

    # Rule from late night work
    res = con.execute("""
        SELECT COUNT(*) FROM events
        WHERE EXTRACT(HOUR FROM ts_start) >= 23
           OR EXTRACT(HOUR FROM ts_start) < 5
    """).fetchone()
    late = res[0] if res else 0
    if late > 10:
        rules.append((
            rid,
            "Stop working after 11 PM. Late-night code is associated with higher error rates.",
            "hour >= 23 OR hour < 5",
            f"{late} late-night events",
            0.80
        ))
        rid += 1

    # Rule from marathon sessions
    res = con.execute("""
        SELECT COUNT(*) FROM sessions WHERE duration_min > 240
    """).fetchone()
    marathon = res[0] if res else 0
    if marathon > 0:
        rules.append((
            rid,
            "Take regular breaks during long sessions. Sessions over 4 hours "
            "often show diminishing returns. Use Pomodoro or similar.",
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
        con.executemany("INSERT INTO anti_playbook VALUES (?,?,?,?,?)", rules)
    return len(rules)

def run(db_path: Path, con: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """Execute failure detection pipeline."""
    if con is None and not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return False

    owns_connection = con is None
    if con is None:
        con = duckdb.connect(str(db_path))

    print("ME-OPS Failure Pattern Detection")
    print("=" * 60)

    try:
        # Create schema
        for stmt in MISTAKES_DDL.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        res = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sessions'"
        ).fetchone()
        has_sessions = (res[0] > 0) if res else False

        con.execute("BEGIN TRANSACTION")
        try:
            if has_sessions:
                detect_context_thrashing(con)
            detect_failure_patterns(con)
            build_anti_playbook(con)
            con.execute("COMMIT")
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"Error during analysis: {e}")
            raise

        export_mistakes_to_orchestrator(con)
        return True
    finally:
        if owns_connection and con:
            con.close()

def export_mistakes_to_orchestrator(con: duckdb.DuckDBPyConnection) -> None:
    """Export high-severity failure patterns to the orchestrator."""
    try:
        from orchestrator import Orchestrator, Signal, ActionItem
    except ImportError:
        print("Orchestrator not found, skipping export.")
        return

    orch = Orchestrator()
    rows = con.execute("""
        SELECT pattern_type, description, severity, evidence_count
        FROM failure_patterns
        WHERE severity IN ('high', 'critical')
    """).fetchall()

    for p_type, desc, sev, count in rows:
        sig = Signal(
            type="behavioral",
            severity=sev,
            source="MistakesEngine",
            description=f"Pattern: {p_type} - {desc}",
            metadata={"evidence_count": count, "pattern": p_type}
        )
        sid = orch.register_signal(sig)
        if sid:
            action = ActionItem(
                title=f"Mitigate {p_type}",
                description=f"Failure pattern detected: {desc}. Evidence count: {count}",
                category="intervention",
                signal_id=sid,
                priority=8 if sev == "high" else 10
            )
            orch.add_to_queue(action)
            print(f"  📤 Exported failure signal to orchestrator: {p_type}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ME-OPS Failure Detection")
    parser.add_argument("--db", type=Path, default=Path("me_ops.duckdb"))
    args = parser.parse_args()
    sys.exit(0 if run(args.db) else 1)
