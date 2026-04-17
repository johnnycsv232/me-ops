"""Phase 2 — Win Signature builder."""
import json, uuid, sys, re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from core.storage.db import get_conn, ts
from core.ledger.classify import extract_tools

WIN_SIGNALS = [
    "resolved","fixed","working","success","complete","done",
    "shipped","deployed","solved","operational","connected",
    "verified","confirmed","installed","stable","migrated",
]
SPEED_MARKERS = {
    "fast": ["quickly","immediately","right away","in minutes","rapid"],
    "slow": ["hours","took a long time","after many attempts","eventually"],
}
PRECONDITION_SIGNALS = {
    "single_target":    ["focused on","specifically","only","just one"],
    "direct_test_loop": ["curl","test endpoint","verify","probe","check status"],
    "low_tool_count":   ["directly","simple","straightforward","just ran"],
}

def _safe_pid(pid):
    """Return None if project_id is falsy or 'unknown'."""
    return pid if (pid and pid != "unknown") else None

def _ensure_project(conn, pid):
    if not pid: return
    conn.execute("""INSERT OR IGNORE INTO projects
      (id,name,description,status,created_at,updated_at,tags)
      VALUES (?,?,?,?,datetime('now'),datetime('now'),'[]')""",
      (pid, pid.replace("-"," ").title(), "", "active"))

def build_win_signatures(conn):
    outcomes = conn.execute(
        "SELECT * FROM entities WHERE type='outcome' ORDER BY created_at DESC"
    ).fetchall()
    wins = []
    for out in outcomes:
        data = json.loads(out["data"] or "{}")
        if data.get("status") not in ("success", None):
            continue
        text = out["summary"] or ""
        ev_refs = json.loads(out["evidence_refs"] or "[]")
        event_text = ""
        for ref in ev_refs[:2]:
            ev = conn.execute("SELECT data FROM entities WHERE id=?", (ref,)).fetchone()
            if ev:
                ed = json.loads(ev["data"] or "{}")
                event_text += " " + (ed.get("raw_content") or "")
        full_text = text + " " + event_text
        tools = extract_tools(full_text)
        preconditions = _extract_preconditions(full_text)
        sequence = _extract_sequence(full_text)
        speed = _estimate_speed(full_text)
        out_pid = _safe_pid(out["project_id"])
        _ensure_project(conn, out_pid)
        case_id = "win_" + uuid.uuid4().hex[:8]
        conn.execute("""
            INSERT OR IGNORE INTO cases
              (id,case_kind,title,symptom,trigger,final_fix,
               resolution_min,context_switches,output_quality,
               confidence,recurrence,project_id,created_at,updated_at,data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            case_id, "win_signature",
            f"Win: {out['summary'][:80]}",
            None, None, out["summary"][:200],
            speed, 0, 0.75, 0.70, 1, out_pid,
            ts(), ts(),
            json.dumps({
                "preconditions": preconditions,
                "sequence": sequence,
                "tools_used": tools,
                "future_pattern": _derive_pattern(preconditions, tools),
                "outcome_id": out["id"],
            })
        ))
        conn.execute(
            "INSERT OR IGNORE INTO case_members (case_id,entity_id,role) VALUES (?,?,?)",
            (case_id, out["id"], "outcome")
        )
        conn.execute(
            "INSERT INTO edges (from_id,to_id,edge_type,confidence,created_at) VALUES (?,?,?,?,?)",
            (out["id"], case_id, "DERIVED_FROM", 0.7, ts())
        )
        wins.append(case_id)
    conn.commit()
    return wins

def _extract_preconditions(text):
    found = []
    tl = text.lower()
    for cond, signals in PRECONDITION_SIGNALS.items():
        if any(s in tl for s in signals):
            found.append(cond)
    if "single" in tl or "one task" in tl or "focused" in tl:
        if "single_target" not in found:
            found.append("single_clear_target")
    return found or ["unknown_preconditions"]

def _extract_sequence(text):
    seq = []
    tl = text.lower()
    if "check" in tl or "verify" in tl:    seq.append("verify_state")
    if "curl" in tl or "test" in tl:       seq.append("test_endpoint")
    if "edit" in tl or "config" in tl:     seq.append("edit_config")
    if "restart" in tl:                    seq.append("restart_service")
    if "confirm" in tl or "verified" in tl: seq.append("confirm_resolution")
    return seq or ["direct_fix"]

def _estimate_speed(text):
    tl = text.lower()
    for w in SPEED_MARKERS["fast"]:
        if w in tl: return 10
    m = re.search(r"(\d+)\s*min", tl)
    if m: return int(m.group(1))
    for w in SPEED_MARKERS["slow"]:
        if w in tl: return 120
    return 30

def _derive_pattern(preconditions, tools):
    parts = []
    if "single_target" in preconditions or "single_clear_target" in preconditions:
        parts.append("narrow to single target")
    if "direct_test_loop" in preconditions:
        parts.append("use direct test loop")
    if tools:
        parts.append(f"use minimal toolset ({', '.join(tools[:3])})")
    return " -> ".join(parts) if parts else "focus and iterate"
