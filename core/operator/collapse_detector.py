"""
Phase 2 — Context Collapse Detector.
Scans sessions for collapse signals and writes guardrail interventions.
"""
import json, uuid, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from core.storage.db import get_conn, ts

COLLAPSE_THRESHOLDS = {
    "tool_switches":        2,
    "concurrent_projects":  2,
    "fragmentation_score":  0.35,
    "context_load_score":   0.45,
}


def detect_context_collapse(conn) -> list:
    """Scan sessions, generate collapse scores and interventions."""
    sessions = conn.execute("SELECT * FROM sessions").fetchall()
    interventions = []

    for sess in sessions:
        score, reasons = _score_session(sess)

        # Write context metric
        conn.execute("""
            INSERT INTO context_metrics
              (session_id, recorded_at, tool_switch_count, fragmentation_score,
               collapse_score, concurrent_projects)
            VALUES (?,?,?,?,?,?)
        """, (
            sess["id"], ts(),
            sess["tool_switch_count"] or 0,
            sess["fragmentation_score"] or 0.0,
            round(score, 3),
            len(json.loads(sess["tags"] or "[]")),
        ))

        if score >= 0.6:
            iid = "int_" + uuid.uuid4().hex[:8]
            message = _build_intervention_message(score, reasons, sess)
            conn.execute("""
                INSERT INTO entities
                  (id, type, source, source_refs, created_at, updated_at,
                   session_id, project_id, actor, confidence, tags, summary, data)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                iid, "intervention", "derived",
                json.dumps([sess["id"]]),
                ts(), ts(),
                sess["id"],
                sess["project_id"] if (sess["project_id"] and sess["project_id"] != "unknown") else None,
                "system", score,
                json.dumps(["context_collapse"]),
                f"Context collapse risk: {score:.0%}",
                json.dumps({
                    "trigger_pattern":  ", ".join(reasons),
                    "message":          message,
                    "suggested_action": _suggest_action(score, reasons),
                    "guardrail_type":   "context_collapse",
                    "collapse_score":   score,
                })
            ))
            interventions.append(iid)

    conn.commit()
    return interventions


def _score_session(sess) -> tuple:
    score   = 0.0
    reasons = []
    ts_count = sess["tool_switch_count"] or 0
    frag     = sess["fragmentation_score"] or 0.0
    load     = sess["context_load_score"] or 0.0
    n_projs  = len(json.loads(sess["tags"] or "[]"))

    if ts_count >= COLLAPSE_THRESHOLDS["tool_switches"]:
        score += min(0.35, 0.12 * ts_count)
        reasons.append(f"{ts_count} tool switches")
    if n_projs >= COLLAPSE_THRESHOLDS["concurrent_projects"]:
        score += min(0.3, 0.15 * (n_projs - 1))
        reasons.append(f"{n_projs} concurrent projects")
    if frag >= COLLAPSE_THRESHOLDS["fragmentation_score"]:
        score += min(0.2, frag * 0.25)
        reasons.append(f"fragmentation {frag:.0%}")
    if load >= COLLAPSE_THRESHOLDS["context_load_score"]:
        score += min(0.3, load * 0.35)
        reasons.append(f"context load {load:.0%}")

    return min(score, 1.0), reasons


def _build_intervention_message(score: float, reasons: list, sess) -> str:
    level = "🔴 CRITICAL" if score >= 0.8 else "🟡 WARNING"
    return (
        f"{level}: Context collapse risk {score:.0%} in session {sess['id']}.\n"
        f"Signals: {', '.join(reasons)}.\n"
        f"This pattern preceded wasted sessions in prior history."
    )


def _suggest_action(score: float, reasons: list) -> str:
    if "tool switches" in str(reasons):
        return "Stop switching tools. Complete current thread before opening new one."
    if "concurrent projects" in str(reasons):
        return "Close all but one project. Choose highest-revenue task and ship it first."
    if "fragmentation" in str(reasons):
        return "Start a new focused session. Write down ONE deliverable before opening any tool."
    return "Reduce active surface area. One project, one tool, one outcome."
