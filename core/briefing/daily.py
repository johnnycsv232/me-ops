"""
Operator Briefing generator.
Produces structured briefings using live DB + heuristics.
"""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from core.storage.db import get_conn, ts

BRIEFING_TEMPLATES = {
    "daily": """
╔══════════════════════════════════════════════════════════════╗
║             ME-OPS OPERATOR BRIEFING — {date}              ║
╚══════════════════════════════════════════════════════════════╝

🎯 PRIMARY FOCUS
  {primary_focus}

⚠️  ACTIVE RISK
  {active_risk}

🔁 PATTERN MATCH
  {pattern_match}

🚫 KNOWN DEAD END
  {known_dead_end}

✅ BEST NEXT MOVE
  {best_next_move}

🔄 IF STUCK, RUN THIS SEQUENCE
  {if_stuck}

📊 WATCH METRIC
  {watch_metric}

📋 ACTIVE HEURISTICS
{heuristics}

📂 OPEN FAILURE CHAINS
{failure_chains}
""",

    "incident": """
╔══════════════════════════════════════════════════════════════╗
║                  ME-OPS INCIDENT BRIEF                      ║
╚══════════════════════════════════════════════════════════════╝

🔴 SYMPTOM
  {symptom}

🔍 LIKELY CAUSES (ranked)
{causes}

🔁 SIMILAR PRIOR CASES
{prior_cases}

⚡ HISTORICALLY FASTEST FIRST STEP
  {fastest_first_step}

🚫 DEAD ENDS TO SKIP
{dead_ends}

📊 CONFIDENCE: {confidence}
""",

    "project": """
╔══════════════════════════════════════════════════════════════╗
║         ME-OPS PROJECT BRIEF — {project_id}                ║
╚══════════════════════════════════════════════════════════════╝

📈 ACTIVITY THIS WEEK: {event_count} sessions
💥 ACTIVE FAILURES: {failure_count}
✅ RESOLVED OUTCOMES: {outcome_count}

🔁 RECURRING PATTERNS
{patterns}

⚠️  TOP RISKS
{risks}

✅ BEST NEXT MOVE
  {best_next_move}
""",
}


def generate_daily_briefing(project_id: str = None) -> str:
    conn = get_conn()

    # Get active heuristics
    heuristics = conn.execute(
        "SELECT * FROM heuristics WHERE active=1 ORDER BY utility_score DESC LIMIT 5"
    ).fetchall()

    # Get recent failure chains
    q = "SELECT * FROM cases WHERE case_kind='failure_chain' ORDER BY created_at DESC LIMIT 5"
    params = []
    if project_id:
        q = "SELECT * FROM cases WHERE case_kind='failure_chain' AND project_id=? ORDER BY created_at DESC LIMIT 5"
        params = [project_id]
    chains = conn.execute(q, params).fetchall()

    # Get recent events to determine primary focus
    recent = conn.execute("""
        SELECT project_id, COUNT(*) as n FROM entities
        WHERE type='event' AND created_at > datetime('now', '-2 days')
        GROUP BY project_id ORDER BY n DESC LIMIT 1
    """).fetchone()

    active_project = (recent["project_id"] if recent else project_id) or "unknown"

    # Build heuristics block
    heu_block = ""
    for i, h in enumerate(heuristics, 1):
        heu_block += f"  {i}. [{h['heuristic_kind'].upper()}] {h['statement']}\n"

    # Build failure chains block
    chain_block = ""
    for c in chains:
        data = json.loads(c["data"] or "{}")
        n_fails = len(data.get("failure_ids", []))
        chain_block += f"  • [{c['project_id'] or '?'}] {c['title']} ({n_fails} failures)\n"

    if not chain_block:
        chain_block = "  No active failure chains.\n"

    return BRIEFING_TEMPLATES["daily"].format(
        date=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        primary_focus=f"Project: {active_project} — highest activity last 48h",
        active_risk="Context collapse risk if tool-switching continues without completing active thread",
        pattern_match=f"Infrastructure reconnect loop (Paperclip tunnel) — matches heu_003 + heu_007",
        known_dead_end="Do NOT reinstall service without verifying bind/port/env path first (heu_001)",
        best_next_move="Fix tunnel persistence for Paperclip before starting next OpenClaw task",
        if_stuck="1. curl http://localhost:39300/.well-known/version  2. check bind/port  3. verify env key  4. restart service  5. retest",
        watch_metric="Tool switch count per 20-min window. Target: <= 3. Current threshold: 4 = alert.",
        heuristics=heu_block,
        failure_chains=chain_block,
    )


def generate_incident_briefing(symptom: str) -> str:
    conn = get_conn()

    # Find similar failure chains
    similar = conn.execute("""
        SELECT * FROM cases WHERE case_kind='failure_chain'
        AND (title LIKE ? OR symptom LIKE ?)
        ORDER BY confidence DESC LIMIT 3
    """, (f"%{symptom[:30]}%", f"%{symptom[:30]}%")).fetchall()

    # Find relevant heuristics
    relevant_heu = conn.execute("""
        SELECT * FROM heuristics
        WHERE statement LIKE ? OR scope LIKE ?
        ORDER BY confidence DESC LIMIT 3
    """, (f"%{symptom[:20]}%", f"%{symptom[:20]}%")).fetchall()

    causes_block = "  1. Interface/bind mismatch\n  2. Missing env variable or API key\n  3. Port conflict\n  4. WSL networking issue\n"
    prior_block = ""
    for c in similar:
        prior_block += f"  • {c['title']} (confidence: {c['confidence']})\n"
    if not prior_block:
        prior_block = "  No prior matching cases found.\n"

    dead_ends = "  • Full reinstall before verifying bind/port\n  • Changing unrelated config\n"

    return BRIEFING_TEMPLATES["incident"].format(
        symptom=symptom,
        causes=causes_block,
        prior_cases=prior_block,
        fastest_first_step="curl/test the endpoint first — confirm whether service is unreachable or misconfigured",
        dead_ends=dead_ends,
        confidence="0.80 (rule-based, pre-embedding)",
    )


def generate_project_briefing(project_id: str) -> str:
    conn = get_conn()
    event_count = conn.execute(
        "SELECT COUNT(*) FROM entities WHERE type='event' AND project_id=? AND created_at > datetime('now','-7 days')",
        (project_id,)
    ).fetchone()[0]
    failure_count = conn.execute(
        "SELECT COUNT(*) FROM entities WHERE type='failure' AND project_id=?",
        (project_id,)
    ).fetchone()[0]
    outcome_count = conn.execute(
        "SELECT COUNT(*) FROM entities WHERE type='outcome' AND project_id=?",
        (project_id,)
    ).fetchone()[0]

    return BRIEFING_TEMPLATES["project"].format(
        project_id=project_id,
        event_count=event_count,
        failure_count=failure_count,
        outcome_count=outcome_count,
        patterns="Infrastructure reconnect loops\nContext-switch fragmentation during debug\nPlan sessions without executable artifact",
        risks="Tunnel persistence not solved (recurring Paperclip invites)\nCodex OPENAI_API_KEY not set",
        best_next_move="Complete tunnel persistence fix before next agent task",
    )


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    if mode == "daily":
        print(generate_daily_briefing())
    elif mode == "incident":
        symptom = " ".join(sys.argv[2:]) or "connection refused"
        print(generate_incident_briefing(symptom))
    elif mode == "project":
        pid = sys.argv[2] if len(sys.argv) > 2 else "openclaw"
        print(generate_project_briefing(pid))
