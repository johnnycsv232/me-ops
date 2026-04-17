"""Phase 2 — Failure chain builder."""
import json, uuid, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from core.storage.db import get_conn, ts
from core.ledger.classify import detect_failure_signals, detect_outcome_signals, extract_tools

def _safe_pid(pid):
    return pid if (pid and pid != "unknown") else None

def _ensure_project(conn, pid):
    if not pid: return
    conn.execute("""INSERT OR IGNORE INTO projects
      (id,name,description,status,created_at,updated_at,tags)
      VALUES (?,?,?,?,datetime('now'),datetime('now'),'[]')""",
      (pid, pid.replace("-"," ").title(), "", "active"))

def lift_failures_and_outcomes(conn):
    events = conn.execute(
        "SELECT * FROM entities WHERE type='event' ORDER BY created_at ASC"
    ).fetchall()
    failures_created = outcomes_created = 0
    for row in events:
        data = json.loads(row["data"] or "{}")
        text = data.get("raw_content") or row["summary"] or ""
        if not text: continue
        event_id = row["id"]
        project_id = _safe_pid(row["project_id"])
        _ensure_project(conn, project_id)
        if detect_failure_signals(text) and not _already_lifted(conn, event_id, "failure"):
            fail_id = "fail_" + uuid.uuid4().hex[:8]
            symptom = _extract_symptom(text)
            conn.execute("""INSERT INTO entities
              (id,type,source,source_refs,created_at,updated_at,
               timestamp_start,project_id,actor,confidence,evidence_refs,
               tags,summary,data)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                fail_id,"failure","derived",json.dumps([event_id]),ts(),ts(),
                row["timestamp_start"],project_id,"system",0.7,
                json.dumps([event_id]),
                json.dumps([project_id] if project_id else []),
                symptom,
                json.dumps({"failure_kind":_classify_failure_kind(text),
                  "symptom":symptom,"severity":_classify_severity(text),
                  "trigger_refs":[event_id],
                  "candidate_causes":_extract_candidates(text),
                  "tools_implicated":extract_tools(text)})))
            conn.execute("INSERT INTO edges (from_id,to_id,edge_type,confidence,created_at) VALUES (?,?,?,?,?)",
                (event_id,fail_id,"TRIGGERED",0.7,ts()))
            failures_created += 1
        if detect_outcome_signals(text) and not _already_lifted(conn, event_id, "outcome"):
            out_id = "out_" + uuid.uuid4().hex[:8]
            conn.execute("""INSERT INTO entities
              (id,type,source,source_refs,created_at,updated_at,
               timestamp_start,project_id,actor,confidence,evidence_refs,
               tags,summary,data)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                out_id,"outcome","derived",json.dumps([event_id]),ts(),ts(),
                row["timestamp_start"],project_id,"system",0.7,
                json.dumps([event_id]),
                json.dumps([project_id] if project_id else []),
                _extract_outcome_summary(text),
                json.dumps({"outcome_kind":"fix","status":"success",
                  "linked_decision_refs":[],"linked_failure_refs":[]})))
            conn.execute("INSERT INTO edges (from_id,to_id,edge_type,confidence,created_at) VALUES (?,?,?,?,?)",
                (event_id,out_id,"RESULTED_IN",0.7,ts()))
            outcomes_created += 1
    conn.commit()
    return failures_created, outcomes_created

def build_failure_chains(conn, project_id=None):
    q = "SELECT * FROM entities WHERE type='failure'"
    params = []
    if project_id:
        q += " AND project_id=?"
        params.append(project_id)
    q += " ORDER BY created_at ASC"
    failures = conn.execute(q, params).fetchall()
    case_ids = []
    by_project = {}
    for f in failures:
        pid = _safe_pid(f["project_id"])
        key = pid or "none"
        by_project.setdefault(key, []).append((pid, f))
    for key, pid_fails in by_project.items():
        for i in range(0, len(pid_fails), 5):
            chunk = pid_fails[i:i+5]
            if not chunk: continue
            pid = chunk[0][0]
            fails = [pf[1] for pf in chunk]
            _ensure_project(conn, pid)
            case_id = "case_" + uuid.uuid4().hex[:8]
            symptoms = [json.loads(f["data"] or "{}").get("symptom","") for f in fails]
            tools_all = []
            for f in fails:
                tools_all.extend(json.loads(f["data"] or "{}").get("tools_implicated",[]))
            conn.execute("""INSERT OR IGNORE INTO cases
              (id,case_kind,title,symptom,trigger,final_fix,
               time_lost_min,confidence,recurrence,project_id,
               created_at,updated_at,data)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                case_id,"failure_chain",
                f"Failure chain: {pid or 'unknown'} ({len(fails)} failures)",
                "; ".join(set(s for s in symptoms if s))[:300],
                None,None,0,0.6,len(fails),pid,ts(),ts(),
                json.dumps({"failure_ids":[f["id"] for f in fails],
                  "tools":list(set(tools_all)),"diagnosis_path":[],
                  "false_paths":[],"reusable_fix":[]})))
            for f in fails:
                conn.execute("INSERT OR IGNORE INTO case_members (case_id,entity_id,role) VALUES (?,?,?)",
                    (case_id,f["id"],"evidence"))
            case_ids.append(case_id)
    conn.commit()
    return case_ids

def _already_lifted(conn, event_id, entity_type):
    row = conn.execute(
        "SELECT 1 FROM entities WHERE type=? AND source_refs LIKE ?",
        (entity_type, f'%"{event_id}"%')
    ).fetchone()
    return row is not None

def _extract_symptom(text):
    for s in text.replace("\n"," ").split("."):
        s = s.strip()
        if any(sig in s.lower() for sig in ["error","failed","failure","broken","refused"]):
            return s[:200]
    return text[:150]

def _extract_outcome_summary(text):
    for s in text.replace("\n"," ").split("."):
        s = s.strip()
        if any(sig in s.lower() for sig in ["resolved","fixed","working","success","complete"]):
            return s[:200]
    return text[:150]

def _extract_candidates(text):
    candidates = []
    tl = text.lower()
    if "bind" in tl:      candidates.append("interface/bind mismatch")
    if "port" in tl:      candidates.append("port conflict or blocked")
    if "path" in tl:      candidates.append("path or env variable wrong")
    if "auth" in tl:      candidates.append("authentication failure")
    if "key" in tl:       candidates.append("missing API key")
    if "network" in tl:   candidates.append("network configuration issue")
    if "wsl" in tl:       candidates.append("WSL networking issue")
    if "config" in tl:    candidates.append("configuration mismatch")
    if "depend" in tl:    candidates.append("missing dependency")
    return candidates[:5]

def _classify_failure_kind(text):
    tl = text.lower()
    if any(w in tl for w in ["network","port","bind","wsl","connect"]): return "network"
    if any(w in tl for w in ["auth","key","token","permission","denied"]): return "auth"
    if any(w in tl for w in ["config","setting","env","path"]): return "config"
    if any(w in tl for w in ["depend","package","install","module"]): return "dependency"
    return "unknown"

def _classify_severity(text):
    tl = text.lower()
    if any(w in tl for w in ["critical","blocked","completely","nothing works"]): return "high"
    if any(w in tl for w in ["hours","repeated","recurring","again"]): return "medium"
    return "low"
