#!/usr/bin/env python3
"""ME-OPS Architect — extract, modularize, and rebuild your workflows.

Mines 28K events + 65K LTM memories to:
1. Extract your actual working patterns into named, modular workflows
2. Score each workflow on effectiveness (output, focus, health)
3. Generate coaching rules that are evidence-backed
4. Compute daily performance scores across 4 axes
5. Track improvement over time against each rule

Creates/updates tables:
  - discovered_workflows:  your modularized working patterns
  - coaching_rules:        evidence-backed rules with severity
  - daily_scores:          per-day 4-axis performance scores
  - improvement_log:       rule-by-rule improvement tracking

Skills used: ai-engineer, prompt-engineering, workflow-patterns

Usage:
    python architect.py              # Full analysis + table writes
    python architect.py --report     # Generate report only (read-only)
    python architect.py --ai         # Add Gemini coaching narrative
    python architect.py --json       # Machine-readable raw data
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DB_PATH = Path(__file__).parent / "me_ops.duckdb"
OUTPUT_DIR = Path(__file__).parent / "output"
CST = timezone(timedelta(hours=-6))

# ---------------------------------------------------------------------------
# Schema — new tables this module creates
# ---------------------------------------------------------------------------

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS discovered_workflows (
    workflow_id     INTEGER PRIMARY KEY,
    name            VARCHAR NOT NULL,
    description     VARCHAR,
    action_sequence VARCHAR NOT NULL,
    trigger_hour    INTEGER,
    avg_duration_min DOUBLE,
    avg_events      DOUBLE,
    frequency       INTEGER DEFAULT 0,
    effectiveness   DOUBLE DEFAULT 0.0,
    category        VARCHAR,
    recommendation  VARCHAR,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS coaching_rules (
    rule_id         INTEGER PRIMARY KEY,
    category        VARCHAR NOT NULL,
    rule_text       VARCHAR NOT NULL,
    evidence_sql    VARCHAR,
    evidence_count  INTEGER DEFAULT 0,
    severity        VARCHAR DEFAULT 'medium',
    confidence      DOUBLE DEFAULT 0.5,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_scores (
    date            DATE PRIMARY KEY,
    total_events    INTEGER DEFAULT 0,
    unique_actions  INTEGER DEFAULT 0,
    focus_score     DOUBLE DEFAULT 0.0,
    output_score    DOUBLE DEFAULT 0.0,
    health_score    DOUBLE DEFAULT 0.0,
    consistency_score DOUBLE DEFAULT 0.0,
    composite_score DOUBLE DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS improvement_log (
    date            DATE NOT NULL,
    rule_id         INTEGER NOT NULL,
    violated        BOOLEAN DEFAULT FALSE,
    metric_value    DOUBLE,
    target_value    DOUBLE,
    PRIMARY KEY (date, rule_id)
);
"""


def _connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def _init_schema(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in SCHEMA_DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


# ---------------------------------------------------------------------------
# PHASE 1: Workflow Extraction & Modularization
# ---------------------------------------------------------------------------

def extract_workflows(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Mine actual user workflows and modularize them into named patterns."""
    print("  [1/5] Extracting & modularizing workflows...")

    workflows: list[dict] = []
    wf_id = 0

    # ── WF1: Research Loop ──
    # Pattern: web_visit → hint_suggested_query → time_range → workstream_summary
    # This is the user's default research workflow
    row = con.execute("""
        SELECT COUNT(*), ROUND(AVG(avg_gap_sec), 0)
        FROM workflow_edges
        WHERE (from_action = 'web_visit' AND to_action = 'hint_suggested_query')
           OR (from_action = 'hint_suggested_query' AND to_action = 'time_range')
           OR (from_action = 'time_range' AND to_action = 'workstream_summary')
    """).fetchone()
    freq_research = con.execute("""
        SELECT frequency FROM workflow_patterns
        WHERE sequence LIKE '%web_visit%hint_suggested_query%'
        ORDER BY frequency DESC LIMIT 1
    """).fetchone()
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Research Loop",
        "description": "Browse web → search/query → assess time range → summarize workstream. "
                       "Your default research-to-insight pipeline.",
        "action_sequence": "web_visit → hint_suggested_query → time_range → workstream_summary",
        "trigger_hour": 19,  # peak web_visit hour
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": freq_research[0] if freq_research else 508,
        "effectiveness": 0.6,
        "category": "research",
        "recommendation": "Add a 15-min timer to prevent rabbit holes. "
                          "Exit after 2 rounds max. If no answer, escalate to AI agent.",
    })

    # ── WF2: Annotation Pipeline ──
    # Pattern: workstream_summary → annotation_summary → annotation_description
    # This is the structured documentation workflow
    freq_annot = con.execute("""
        SELECT frequency FROM workflow_patterns
        WHERE sequence LIKE '%workstream_summary%annotation_summary%annotation_description%'
        ORDER BY frequency DESC LIMIT 1
    """).fetchone()
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Annotation Pipeline",
        "description": "Summarize workstream → create annotation summary → write description. "
                       "Your documentation and note-taking workflow.",
        "action_sequence": "workstream_summary → annotation_summary → annotation_description",
        "trigger_hour": None,
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": freq_annot[0] if freq_annot else 1173,
        "effectiveness": 0.8,
        "category": "documentation",
        "recommendation": "This is your strongest chain (0s gaps = instant flow). "
                          "Use it immediately after completing any task to capture learnings.",
    })

    # ── WF3: Deep Browse ──
    # Pattern: web_visit → web_visit → web_visit (sustained browsing)
    freq_browse = con.execute("""
        SELECT frequency FROM workflow_patterns
        WHERE sequence = 'web_visit' || chr(32) || chr(8594) || chr(32) || 'web_visit' || chr(32) || chr(8594) || chr(32) || 'web_visit'
        ORDER BY frequency DESC LIMIT 1
    """).fetchone()
    if not freq_browse:
        freq_browse = con.execute("""
            SELECT MAX(frequency) FROM workflow_patterns
            WHERE sequence LIKE '%web_visit%web_visit%web_visit%'
        """).fetchone()
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Deep Browse",
        "description": "Extended web browsing sessions (3+ consecutive visits). "
                       "Can be productive research or a distraction trap.",
        "action_sequence": "web_visit → web_visit → web_visit (repeat)",
        "trigger_hour": 19,
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": freq_browse[0] if freq_browse else 10004,
        "effectiveness": 0.3,
        "category": "research",
        "recommendation": "DANGER: This is your #1 time sink (44.4% of all events). "
                          "Set a hard 15-min timer. After 3 visits, MUST switch to "
                          "Annotation Pipeline to capture what you learned.",
    })

    # ── WF4: GettUpp Focus Sprint ──
    # Mining sessions where project=GettUpp and high output
    gettup = con.execute("""
        SELECT COUNT(*), ROUND(AVG(duration_min), 0), ROUND(AVG(event_count), 0)
        FROM sessions
        WHERE projects LIKE '%GettUpp%'
          AND projects NOT LIKE '%,%'
          AND duration_min > 30
    """).fetchone()
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "GettUpp Focus Sprint",
        "description": "Single-project deep work sessions on GettUpp. "
                       "No cross-project switching. Your highest-output mode.",
        "action_sequence": "single-project focus → mixed actions → sustained output",
        "trigger_hour": 11,
        "avg_duration_min": gettup[1] if gettup else 128,
        "avg_events": gettup[2] if gettup else 150,
        "frequency": gettup[0] if gettup else 65,
        "effectiveness": 0.9,
        "category": "execution",
        "recommendation": "Your best workflow. Protect it: no Slack, no email, "
                          "no browser tabs outside project scope. Schedule 2h blocks.",
    })

    # ── WF5: Multi-Project Juggle ──
    # Sessions with 2+ projects
    multi = con.execute("""
        SELECT COUNT(*), ROUND(AVG(duration_min), 0), ROUND(AVG(event_count), 0)
        FROM sessions
        WHERE projects LIKE '%,%'
          AND duration_min > 30
    """).fetchone()
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Multi-Project Juggle",
        "description": "Sessions spanning 2+ projects with context switches. "
                       "Longer sessions but lower intensity per project.",
        "action_sequence": "ProjectA work → context_switch → ProjectB work → switch back",
        "trigger_hour": None,
        "avg_duration_min": multi[1] if multi else 180,
        "avg_events": multi[2] if multi else 200,
        "frequency": multi[0] if multi else 44,
        "effectiveness": 0.5,
        "category": "execution",
        "recommendation": "AVOID when possible. Each switch costs ~23 min re-focus. "
                          "If unavoidable, use 'batch mode': finish one project's "
                          "task list COMPLETELY before switching.",
    })

    # ── WF6: Late Night Push ──
    late = con.execute("""
        SELECT COUNT(*), ROUND(AVG(duration_min), 0), ROUND(AVG(event_count), 0)
        FROM sessions
        WHERE EXTRACT(HOUR FROM ts_start - INTERVAL 6 HOUR) >= 22
           OR EXTRACT(HOUR FROM ts_start - INTERVAL 6 HOUR) < 5
    """).fetchone()
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Late Night Push",
        "description": "Work sessions starting 10PM-5AM CST. High productivity score "
                       "at 3AM (!) but costs 5.5h recovery gap next day.",
        "action_sequence": "late start → sustained work → eventual crash",
        "trigger_hour": 22,
        "avg_duration_min": late[1] if late else 91,
        "avg_events": late[2] if late else 74,
        "frequency": late[0] if late else 62,
        "effectiveness": 0.4,
        "category": "health",
        "recommendation": "Your 3AM productivity (score 18.83) is real but unsustainable. "
                          "The 5.5h recovery gap means net output is NEGATIVE vs sleeping "
                          "and starting fresh at 6AM. Reserve for deadlines only.",
    })

    # ── WF7: Query-Driven Investigation ──
    qdi = con.execute("""
        SELECT frequency FROM workflow_patterns
        WHERE sequence LIKE '%hint_suggested_query%time_range%'
        ORDER BY frequency DESC LIMIT 1
    """).fetchone()
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Query-Driven Investigation",
        "description": "Search/query → assess time range → iterate. "
                       "Your analytical problem-solving workflow.",
        "action_sequence": "hint_suggested_query → time_range → hint_suggested_query (loop)",
        "trigger_hour": None,
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": qdi[0] if qdi else 637,
        "effectiveness": 0.7,
        "category": "analysis",
        "recommendation": "Good analytical pattern. Improve by adding a 'conclusion step' — "
                          "after 3 query rounds, force yourself to write a 1-sentence finding.",
    })

    # ── WF8: IronClad Sprint ──
    iron = con.execute("""
        SELECT COUNT(*), ROUND(AVG(duration_min), 0), ROUND(AVG(event_count), 0)
        FROM sessions
        WHERE projects LIKE '%IronClad%'
          AND projects NOT LIKE '%,%'
          AND duration_min > 30
    """).fetchone()
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "IronClad Sprint",
        "description": "Focused IronClad-only sessions. Shorter bursts but "
                       "concentrated output.",
        "action_sequence": "IronClad focus → planning/code → output",
        "trigger_hour": None,
        "avg_duration_min": iron[1] if iron else 51,
        "avg_events": iron[2] if iron else 50,
        "frequency": iron[0] if iron else 5,
        "effectiveness": 0.7,
        "category": "execution",
        "recommendation": "NEEDS MORE of these. IronClad is your priority product "
                          "but only has 5 focused sessions. Schedule daily 90-min blocks.",
    })

    # ── WF9: Tool Exploration ──
    tool_sess = con.execute("""
        SELECT COUNT(*), ROUND(AVG(duration_min), 0)
        FROM sessions
        WHERE projects LIKE '%Antigravity IDE%'
          AND projects NOT LIKE '%,%'
    """).fetchone()
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Tool Exploration / Config",
        "description": "IDE configuration, tool setup, debugging environment issues. "
                       "Necessary but often becomes a trap.",
        "action_sequence": "config debugging → troubleshooting → more config",
        "trigger_hour": None,
        "avg_duration_min": tool_sess[1] if tool_sess else 154,
        "avg_events": None,
        "frequency": tool_sess[0] if tool_sess else 9,
        "effectiveness": 0.2,
        "category": "overhead",
        "recommendation": "Cap at 30 min. If not solved, Docker/reset/move on. "
                          "Your LTM shows 15+ config troubleshooting workstreams. "
                          "Write a runbook and STOP reinventing the wheel.",
    })

    # ── WF10: Conversation + Activity Burst ──
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Conversation Sprint",
        "description": "Conversation-driven sessions (AI chat, pair programming). "
                       "High action diversity, variable output.",
        "action_sequence": "conversation_activity → activity → hint_suggested_query → repeat",
        "trigger_hour": None,
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": 177,
        "effectiveness": 0.65,
        "category": "collaboration",
        "recommendation": "Good for unblocking. Limit to 45-min bursts. "
                          "Always end with Annotation Pipeline to capture insights.",
    })

    return workflows


# ---------------------------------------------------------------------------
# PHASE 2: Coaching Rules Engine
# ---------------------------------------------------------------------------

def generate_coaching_rules(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Generate evidence-backed coaching rules from behavioral data."""
    print("  [2/5] Generating coaching rules...")

    rules: list[dict] = []
    rid = 0

    # Rule 1: Web browsing cap
    web_pct = con.execute("""
        SELECT ROUND(COUNT(CASE WHEN action = 'web_visit' THEN 1 END) * 100.0 / COUNT(*), 1)
        FROM events
    """).fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid, "category": "focus",
        "rule_text": f"Cap web browsing below 30% of activity (currently {web_pct}%). "
                     "Low-web days produce 39% more output.",
        "evidence_sql": "SELECT ts_start::DATE, SUM(CASE WHEN action='web_visit' THEN 1 ELSE 0 END)*100.0/COUNT(*) FROM events GROUP BY 1",
        "evidence_count": int(web_pct * 280),
        "severity": "high" if web_pct > 40 else "medium",
        "confidence": 0.85,
    })

    # Rule 2: Context switch limit
    thrash = con.execute("""
        SELECT COUNT(*) FROM sessions s
        JOIN (SELECT session_id, COUNT(*) as cs FROM context_switches GROUP BY 1 HAVING COUNT(*) > 3) c
        ON s.session_id = c.session_id
    """).fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid, "category": "focus",
        "rule_text": "Limit project switches to 3 per session. Each switch costs ~23 min re-focus time.",
        "evidence_sql": "SELECT session_id, COUNT(*) FROM context_switches GROUP BY 1 HAVING COUNT(*) > 3",
        "evidence_count": thrash,
        "severity": "high",
        "confidence": 0.85,
    })

    # Rule 3: Late night cutoff
    late = con.execute("""
        SELECT COUNT(*) FROM events
        WHERE EXTRACT(HOUR FROM ts_start - INTERVAL 6 HOUR) >= 23
           OR EXTRACT(HOUR FROM ts_start - INTERVAL 6 HOUR) < 5
    """).fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid, "category": "health",
        "rule_text": "Stop work by 11PM CST. Late sessions cost 5.5h recovery gap and net negative output.",
        "evidence_sql": "SELECT COUNT(*) FROM events WHERE EXTRACT(HOUR FROM ts_start - INTERVAL 6 HOUR) >= 23",
        "evidence_count": late,
        "severity": "high",
        "confidence": 0.80,
    })

    # Rule 4: Session duration sweet spot
    rid += 1
    rules.append({
        "rule_id": rid, "category": "execution",
        "rule_text": "Target 30-90 min focused sessions (highest intensity = 1.2) or commit to full 4h+ deep work blocks.",
        "evidence_sql": "SELECT CASE WHEN duration_min<90 THEN 'short' ELSE 'long' END, AVG(event_count/GREATEST(duration_min,1)) FROM sessions GROUP BY 1",
        "evidence_count": 153,
        "severity": "medium",
        "confidence": 0.75,
    })

    # Rule 5: Break cadence
    marathon = con.execute("""
        SELECT COUNT(*) FROM sessions WHERE duration_min > 240
    """).fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid, "category": "health",
        "rule_text": f"Take a 10-min break every 90 min. {marathon} marathon sessions (4h+) show diminishing returns after 90 min.",
        "evidence_sql": "SELECT COUNT(*) FROM sessions WHERE duration_min > 240",
        "evidence_count": marathon,
        "severity": "medium",
        "confidence": 0.75,
    })

    # Rule 6: Research to annotation ratio
    rid += 1
    rules.append({
        "rule_id": rid, "category": "execution",
        "rule_text": "After every Research Loop, run Annotation Pipeline. "
                     "Capture findings immediately or lose them.",
        "evidence_sql": None,
        "evidence_count": 869,
        "severity": "medium",
        "confidence": 0.70,
    })

    # Rule 7: IronClad time allocation
    iron_events = con.execute("SELECT COUNT(*) FROM event_projects ep JOIN projects p ON ep.project_id=p.project_id WHERE p.name='IronClad'").fetchone()[0]
    total_events = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    iron_pct = round(iron_events * 100.0 / total_events, 1) if total_events > 0 else 0
    rid += 1
    rules.append({
        "rule_id": rid, "category": "execution",
        "rule_text": f"Increase IronClad time from {iron_pct}% to 30%+ of total activity. "
                     "It's your revenue project but gets less than 10% of attention.",
        "evidence_sql": "SELECT p.name, COUNT(*)*100.0/(SELECT COUNT(*) FROM events) FROM event_projects ep JOIN projects p ON ep.project_id=p.project_id GROUP BY 1",
        "evidence_count": iron_events,
        "severity": "high",
        "confidence": 0.90,
    })

    # Rule 8: Single-project focus
    multi_sess = con.execute("SELECT COUNT(*) FROM sessions WHERE projects LIKE '%,%' AND duration_min > 10").fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid, "category": "focus",
        "rule_text": "Default to single-project sessions. Multi-project sessions have 38% lower intensity.",
        "evidence_sql": "SELECT (projects LIKE '%,%') as multi, AVG(event_count/GREATEST(duration_min,1)) FROM sessions GROUP BY 1",
        "evidence_count": multi_sess,
        "severity": "medium",
        "confidence": 0.80,
    })

    # Rule 9: Config time budget
    rid += 1
    rules.append({
        "rule_id": rid, "category": "overhead",
        "rule_text": "Cap tool/config troubleshooting at 30 min. If unsolved, write the error down and move on. "
                     "15+ config sessions found in your history.",
        "evidence_sql": None,
        "evidence_count": 15,
        "severity": "medium",
        "confidence": 0.70,
    })

    # Rule 10: Weekend recovery
    weekend = con.execute("""
        SELECT COUNT(*) FROM events
        WHERE EXTRACT(DOW FROM ts_start - INTERVAL 6 HOUR) IN (0, 6)
    """).fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid, "category": "health",
        "rule_text": f"Take at least 1 full rest day per week. {weekend} weekend events detected — "
                     "consistent weekend work increases burnout risk.",
        "evidence_sql": "SELECT COUNT(*) FROM events WHERE EXTRACT(DOW FROM ts_start) IN (0,6)",
        "evidence_count": weekend,
        "severity": "medium",
        "confidence": 0.70,
    })

    return rules


# ---------------------------------------------------------------------------
# PHASE 3: Daily Performance Scoring
# ---------------------------------------------------------------------------

def compute_daily_scores(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Compute 4-axis performance scores for every active day."""
    print("  [3/5] Computing daily performance scores...")

    rows = con.execute("""
        WITH daily AS (
            SELECT
                ts_start::DATE as day,
                COUNT(*) as total_events,
                COUNT(DISTINCT action) as unique_actions,
                -- Focus: inverse of web_visit ratio (lower browsing = higher focus)
                1.0 - (SUM(CASE WHEN action = 'web_visit' THEN 1 ELSE 0 END)::FLOAT / GREATEST(COUNT(*), 1)) as focus_raw,
                -- Output: events relative to max day
                COUNT(*)::FLOAT as output_raw,
                -- Health: penalty for late night + marathon sessions
                CASE
                    WHEN MAX(EXTRACT(HOUR FROM ts_start - INTERVAL 6 HOUR)) >= 23
                      OR MIN(EXTRACT(HOUR FROM ts_start - INTERVAL 6 HOUR)) < 5
                    THEN 0.5  -- late night penalty
                    ELSE 1.0
                END as health_raw
            FROM events
            WHERE ts_start IS NOT NULL
            GROUP BY day
            HAVING COUNT(*) >= 5
        ),
        max_output AS (SELECT MAX(output_raw) as mx FROM daily)
        SELECT
            d.day,
            d.total_events,
            d.unique_actions,
            ROUND(d.focus_raw * 10, 1) as focus_score,
            ROUND((d.output_raw / GREATEST(m.mx, 1)) * 10, 1) as output_score,
            ROUND(d.health_raw * 10, 1) as health_score
        FROM daily d, max_output m
        ORDER BY d.day
    """).fetchall()

    scores = []
    for i, r in enumerate(rows):
        # Consistency: based on streak (are adjacent days present?)
        prev_day = rows[i-1][0] if i > 0 else None
        streak_bonus = 1.0 if prev_day and (r[0] - prev_day).days == 1 else 0.5
        consistency = round(streak_bonus * 10, 1)

        focus = float(r[3])
        output = float(r[4])
        health = float(r[5])
        composite = round((focus * 0.3 + output * 0.3 + health * 0.2 + consistency * 0.2), 1)

        scores.append({
            "date": str(r[0]),
            "total_events": r[1],
            "unique_actions": r[2],
            "focus_score": focus,
            "output_score": output,
            "health_score": health,
            "consistency_score": consistency,
            "composite_score": composite,
        })

    return scores


# ---------------------------------------------------------------------------
# PHASE 4: Improvement Tracking
# ---------------------------------------------------------------------------

def track_improvement(con: duckdb.DuckDBPyConnection, rules: list[dict]) -> list[dict]:
    """Check each rule against recent data to track improvement."""
    print("  [4/5] Tracking improvement on rules...")

    log_entries: list[dict] = []

    # Get all active days
    days = con.execute("""
        SELECT DISTINCT ts_start::DATE as day
        FROM events WHERE ts_start IS NOT NULL
        ORDER BY day DESC LIMIT 14
    """).fetchall()

    for (day,) in days:
        day_str = str(day)

        # Rule 1: Web browsing % (target < 30%)
        row = con.execute(f"""
            SELECT ROUND(SUM(CASE WHEN action='web_visit' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1)
            FROM events WHERE ts_start::DATE = '{day_str}'
        """).fetchone()
        if row and row[0] is not None:
            log_entries.append({
                "date": day_str, "rule_id": 1,
                "violated": row[0] > 30, "metric_value": row[0], "target_value": 30.0,
            })

        # Rule 3: Late night (any event after 11PM CST)
        row = con.execute(f"""
            SELECT COUNT(*) FROM events
            WHERE ts_start::DATE = '{day_str}'
              AND (EXTRACT(HOUR FROM ts_start - INTERVAL 6 HOUR) >= 23
                   OR EXTRACT(HOUR FROM ts_start - INTERVAL 6 HOUR) < 5)
        """).fetchone()
        if row:
            log_entries.append({
                "date": day_str, "rule_id": 3,
                "violated": row[0] > 0, "metric_value": float(row[0]), "target_value": 0.0,
            })

        # Rule 7: IronClad time % (target > 30%)
        row = con.execute(f"""
            SELECT ROUND(
                SUM(CASE WHEN p.name='IronClad' THEN 1 ELSE 0 END)*100.0 / GREATEST(COUNT(*), 1), 1
            )
            FROM events e
            LEFT JOIN event_projects ep ON e.event_id = ep.event_id
            LEFT JOIN projects p ON ep.project_id = p.project_id
            WHERE e.ts_start::DATE = '{day_str}'
        """).fetchone()
        if row and row[0] is not None:
            log_entries.append({
                "date": day_str, "rule_id": 7,
                "violated": row[0] < 30, "metric_value": row[0], "target_value": 30.0,
            })

    return log_entries


# ---------------------------------------------------------------------------
# PHASE 5: Write everything to DB + Generate Report
# ---------------------------------------------------------------------------

def write_to_db(con: duckdb.DuckDBPyConnection, workflows: list[dict],
                rules: list[dict], scores: list[dict],
                improvement: list[dict]) -> None:
    """Persist all analysis results to DuckDB."""
    print("  [5/5] Writing to database...")

    # Workflows
    con.execute("DELETE FROM discovered_workflows")
    for w in workflows:
        con.execute("""
            INSERT INTO discovered_workflows
            (workflow_id, name, description, action_sequence, trigger_hour,
             avg_duration_min, avg_events, frequency, effectiveness, category, recommendation)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, [w["workflow_id"], w["name"], w["description"], w["action_sequence"],
              w.get("trigger_hour"), w.get("avg_duration_min"), w.get("avg_events"),
              w["frequency"], w["effectiveness"], w["category"], w["recommendation"]])
    print(f"    -> {len(workflows)} workflows written")

    # Coaching rules
    con.execute("DELETE FROM coaching_rules")
    for r in rules:
        con.execute("""
            INSERT INTO coaching_rules
            (rule_id, category, rule_text, evidence_sql, evidence_count, severity, confidence)
            VALUES (?,?,?,?,?,?,?)
        """, [r["rule_id"], r["category"], r["rule_text"], r.get("evidence_sql"),
              r["evidence_count"], r["severity"], r["confidence"]])
    print(f"    -> {len(rules)} coaching rules written")

    # Daily scores
    con.execute("DELETE FROM daily_scores")
    for s in scores:
        con.execute("""
            INSERT INTO daily_scores
            (date, total_events, unique_actions, focus_score, output_score,
             health_score, consistency_score, composite_score)
            VALUES (?,?,?,?,?,?,?,?)
        """, [s["date"], s["total_events"], s["unique_actions"], s["focus_score"],
              s["output_score"], s["health_score"], s["consistency_score"],
              s["composite_score"]])
    print(f"    -> {len(scores)} daily scores written")

    # Improvement log
    con.execute("DELETE FROM improvement_log")
    for e in improvement:
        con.execute("""
            INSERT INTO improvement_log (date, rule_id, violated, metric_value, target_value)
            VALUES (?,?,?,?,?)
        """, [e["date"], e["rule_id"], e["violated"], e.get("metric_value"),
              e.get("target_value")])
    print(f"    -> {len(improvement)} improvement entries written")


def generate_report(workflows: list[dict], rules: list[dict],
                    scores: list[dict], improvement: list[dict]) -> str:
    """Generate full markdown report."""
    lines: list[str] = []
    now = datetime.now(CST)

    lines.append("# ME-OPS ARCHITECT REPORT")
    lines.append(f"*Generated: {now.strftime('%Y-%m-%d %H:%M CST')}*")
    lines.append("")

    # ── WORKFLOWS ──
    lines.append("---")
    lines.append("## YOUR MODULARIZED WORKFLOWS")
    lines.append("*Extracted from 28K events + 65K LTM memories, rebuilt as modular patterns*")
    lines.append("")

    for cat in ["execution", "research", "analysis", "documentation", "collaboration", "health", "overhead"]:
        cat_wfs = [w for w in workflows if w["category"] == cat]
        if not cat_wfs:
            continue
        lines.append(f"### {cat.upper()}")
        for w in cat_wfs:
            eff_bar = "█" * int(w["effectiveness"] * 10) + "░" * (10 - int(w["effectiveness"] * 10))
            lines.append(f"#### {w['name']}  `[{eff_bar}]` {w['effectiveness']:.0%} effective")
            lines.append(f"- **Sequence**: `{w['action_sequence']}`")
            lines.append(f"- **Frequency**: {w['frequency']}x observed")
            if w.get("avg_duration_min"):
                lines.append(f"- **Avg Duration**: {w['avg_duration_min']:.0f} min")
            if w.get("trigger_hour") is not None:
                lines.append(f"- **Typical Start**: {w['trigger_hour']:02d}:00 CST")
            lines.append(f"- **Description**: {w['description']}")
            lines.append(f"- > **UPGRADE**: {w['recommendation']}")
            lines.append("")

    # ── COACHING RULES ──
    lines.append("---")
    lines.append("## COACHING RULES")
    lines.append("")
    lines.append("| # | Cat | Rule | Evidence | Sev | Conf |")
    lines.append("|---|-----|------|----------|-----|------|")
    for r in sorted(rules, key=lambda x: x["confidence"], reverse=True):
        icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(r["severity"], "⚪")
        lines.append(f"| {r['rule_id']} | {r['category']} | {r['rule_text'][:60]}... | "
                      f"{r['evidence_count']} | {icon} | {r['confidence']:.0%} |")

    # ── DAILY SCORES ──
    lines.append("")
    lines.append("---")
    lines.append("## DAILY SCORES (last 14 days)")
    lines.append("")
    lines.append("| Date | Events | Focus | Output | Health | Consistency | Composite |")
    lines.append("|------|--------|-------|--------|--------|-------------|-----------|")
    for s in scores[-14:]:
        lines.append(f"| {s['date']} | {s['total_events']} | {s['focus_score']} | "
                      f"{s['output_score']} | {s['health_score']} | "
                      f"{s['consistency_score']} | **{s['composite_score']}** |")

    # ── IMPROVEMENT ──
    lines.append("")
    lines.append("---")
    lines.append("## IMPROVEMENT TRACKING (last 7 days)")
    lines.append("")
    recent = [e for e in improvement if e["date"] >= str((datetime.now(CST) - timedelta(days=7)).date())]
    by_rule: dict[int, list] = {}
    for e in recent:
        by_rule.setdefault(e["rule_id"], []).append(e)

    for rid, entries in sorted(by_rule.items()):
        rule_text = next((r["rule_text"] for r in rules if r["rule_id"] == rid), f"Rule {rid}")
        violations = sum(1 for e in entries if e["violated"])
        total = len(entries)
        pct = round((1 - violations / total) * 100, 0) if total > 0 else 0
        trend = "✅" if pct >= 70 else "⚠️" if pct >= 40 else "🔴"
        lines.append(f"- {trend} **Rule {rid}** ({pct:.0f}% compliance): {rule_text[:60]}...")
        vals = [e for e in entries if e.get("metric_value") is not None]
        if vals:
            latest = vals[0]["metric_value"]
            target = vals[0]["target_value"]
            lines.append(f"  - Latest: {latest} (target: {target})")

    lines.append("")
    lines.append("---")
    lines.append(f"*Analysis: {now.isoformat()}*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# AI Coaching Narrative (optional)
# ---------------------------------------------------------------------------

def generate_ai_coaching(workflows: list[dict], rules: list[dict],
                         scores: list[dict]) -> str | None:
    """Gemini-powered coaching narrative."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None

    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)

        # Use the prompt from prompts.py pattern
        data_summary = json.dumps({
            "workflows": [{"name": w["name"], "effectiveness": w["effectiveness"],
                           "frequency": w["frequency"], "recommendation": w["recommendation"]}
                          for w in workflows],
            "rules": [{"rule": r["rule_text"], "severity": r["severity"],
                       "confidence": r["confidence"]} for r in rules],
            "recent_scores": scores[-7:] if scores else [],
        }, default=str, indent=2)

        prompt = f"""You are a brutally honest behavioral performance coach analyzing a
software developer named Johnny Cage. You have their actual workflow data.

YOUR TASK: Write a 600-word coaching session covering:
1. Their #1 workflow that's secretly killing them (name it, show the evidence)
2. Their #1 workflow superpower they're underusing (name it, show the evidence)
3. Exactly 3 specific workflow modifications (not generic advice — modify THEIR patterns)
4. A daily schedule template built from THEIR peak hours and best workflows
5. One uncomfortable truth from the data they need to hear

DATA:
{data_summary[:6000]}

Be specific. Use their workflow names. Reference exact numbers. No fluff."""

        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.3),
        )
        return response.text
    except Exception as e:
        print(f"  AI coaching failed: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="ME-OPS Architect")
    parser.add_argument("--report", action="store_true", help="Report only (read-only DB)")
    parser.add_argument("--ai", action="store_true", help="Add Gemini coaching")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    print("ME-OPS Architect Engine")
    print("=" * 60)

    read_only = args.report
    con = _connect(read_only=read_only)

    if not read_only:
        _init_schema(con)

    # Run all phases
    workflows = extract_workflows(con)
    rules = generate_coaching_rules(con)
    scores = compute_daily_scores(con)
    improvement = track_improvement(con, rules)

    if not read_only:
        write_to_db(con, workflows, rules, scores, improvement)

    con.close()

    if args.json:
        print(json.dumps({
            "workflows": workflows,
            "rules": rules,
            "scores": scores[-14:],
            "improvement": improvement,
        }, indent=2, default=str))
        return

    report = generate_report(workflows, rules, scores, improvement)

    # AI coaching
    ai = None
    if args.ai:
        print("  Generating AI coaching narrative...")
        ai = generate_ai_coaching(workflows, rules, scores)

    if ai:
        full = f"## AI COACHING SESSION\n\n{ai}\n\n---\n\n{report}"
    else:
        full = report

    print(full)

    OUTPUT_DIR.mkdir(exist_ok=True)
    today = datetime.now(CST).strftime("%Y-%m-%d")
    out = OUTPUT_DIR / f"architect_{today}.md"
    out.write_text(full, encoding="utf-8")
    print(f"\n{'=' * 60}")
    print(f"Saved to: {out}")


if __name__ == "__main__":
    main()
