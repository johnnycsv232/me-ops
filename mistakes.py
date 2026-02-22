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
    """Detect repetitive command failure loops."""
    # MVP: Just check for command failure streaks
    res = con.execute("""
        SELECT COUNT(*) FROM events WHERE error_signature IS NOT NULL
    """).fetchone()
    count = res[0] if res else 0
    
    if count > 50:
        con.execute("""
            INSERT INTO failure_patterns (pattern_type, description, severity, evidence_count, last_detected)
            VALUES ('Query Loop Fatigue', 'High volume of failing commands detected', 'medium', ?, CURRENT_TIMESTAMP)
        """, [count])
        return 1
    return 0

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
            "STOP working after 11 PM. Late-night code has 2-3x higher defect density.",
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
            "BREAK every 90 minutes. Sessions over 4 hours show diminishing returns.",
            "session_duration > 240min",
            f"{marathon} marathon sessions detected",
            0.75
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
