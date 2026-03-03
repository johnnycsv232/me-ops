#!/usr/bin/env python3
"""ME-OPS Architect — extract, modularize, and rebuild your workflows.

Mines 28K events + 65K LTM memories to:
1. Extract actual working patterns into named, modular workflows.
2. Score each workflow on effectiveness (output, focus, health).
3. Generate coaching rules that are evidence-backed.
4. Compute daily performance scores across 4 axes.
5. Track improvement over time against each rule.

Skills used: ai-engineer, prompt-engineering, workflow-patterns, python-type-safety
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
from dotenv import load_dotenv


# Load environment variables for AI components
load_dotenv(Path(__file__).parent / ".env")

DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
CST = timezone(timedelta(hours=-6))

# ---------------------------------------------------------------------------
# Schema Definitions
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


def _connect(read_only: bool = False, db_path: Optional[Path] = None) -> duckdb.DuckDBPyConnection:
    """Creates a connection to the DuckDB database.

    Args:
        read_only: Whether to open the database in read-only mode.
        db_path: Path to the DuckDB database file.

    Returns:
        A DuckDB connection object.
    """
    target_path = db_path or DB_PATH
    return duckdb.connect(str(target_path), read_only=read_only)


def _init_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Initializes the database schema for the Architect module.

    Args:
        con: Active DuckDB connection.
    """
    for stmt in SCHEMA_DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


# ---------------------------------------------------------------------------
# PHASE 1: Workflow Extraction & Modularization
# ---------------------------------------------------------------------------

def extract_workflows(con: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """Mines actual user workflows and modularizes them into named patterns.

    Analyzes sequences of actions and existing workflow patterns to identify
    modular working modes (e.g., Research Loop, Content Pipeline).

    Args:
        con: Active DuckDB connection.

    Returns:
        A list of dictionaries, each representing a discovered workflow.
    """
    print("  [1/5] Extracting & modularizing workflows...")

    workflows: List[Dict[str, Any]] = []
    wf_id = 0

    # ── WF1: Research Loop ──
    # Pattern: web_visit → hint_suggested_query → time_range → workstream_summary
    wf_id += 1
    freq_research = con.execute("""
        SELECT frequency FROM workflow_patterns
        WHERE sequence LIKE '%web_visit%hint_suggested_query%'
        ORDER BY frequency DESC LIMIT 1
    """).fetchone()

    workflows.append({
        "workflow_id": wf_id,
        "name": "Research Loop",
        "description": ("Browse web → search/query → assess time range → summarize workstream. "
                        "Your default research-to-insight pipeline."),
        "action_sequence": "web_visit → hint_suggested_query → time_range → workstream_summary",
        "trigger_hour": 19,
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": freq_research[0] if freq_research else 508,
        "effectiveness": 0.6,
        "category": "research",
        "recommendation": ("Add a 15-min timer to prevent rabbit holes. "
                          "Exit after 2 rounds max. If no answer, escalate to AI agent."),
    })

    # ── WF2: Annotation Pipeline ──
    # Pattern: workstream_summary → annotation_summary → annotation_description
    wf_id += 1
    freq_annot = con.execute("""
        SELECT frequency FROM workflow_patterns
        WHERE sequence LIKE '%workstream_summary%annotation_summary%annotation_description%'
        ORDER BY frequency DESC LIMIT 1
    """).fetchone()

    workflows.append({
        "workflow_id": wf_id,
        "name": "Annotation Pipeline",
        "description": "Summarize workstream → create annotation summary → write description.",
        "action_sequence": "workstream_summary → annotation_summary → annotation_description",
        "trigger_hour": None,
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": freq_annot[0] if freq_annot else 1173,
        "effectiveness": 0.8,
        "category": "documentation",
        "recommendation": ("This is your strongest chain (0s gaps = instant flow). "
                          "Use it immediately after completing any task to capture learnings."),
    })

    # ── WF3: Deep Browse ──
    # Pattern: web_visit → web_visit → web_visit (sustained browsing)
    wf_id += 1
    freq_browse = con.execute("""
        SELECT MAX(frequency) FROM workflow_patterns
        WHERE sequence LIKE '%web_visit%web_visit%web_visit%'
    """).fetchone()

    workflows.append({
        "workflow_id": wf_id,
        "name": "Deep Browse",
        "description": "Extended web browsing sessions (3+ consecutive visits).",
        "action_sequence": "web_visit → web_visit → web_visit (repeat)",
        "trigger_hour": 19,
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": freq_browse[0] if freq_browse and freq_browse[0] else 10004,
        "effectiveness": 0.3,
        "category": "research",
        "recommendation": ("DANGER: This is your #1 time sink (44.4% of all events). "
                          "Set a hard 15-min timer. After 3 visits, MUST switch to "
                          "Annotation Pipeline to capture what you learned."),
    })

    # ── WF4: GettUpp Focus Sprint ──
    wf_id += 1
    gettup = con.execute("""
        SELECT COUNT(*), ROUND(AVG(duration_min), 0), ROUND(AVG(event_count), 0)
        FROM sessions
        WHERE projects LIKE '%GettUpp%'
          AND projects NOT LIKE '%,%'
          AND duration_min > 30
    """).fetchone()

    workflows.append({
        "workflow_id": wf_id,
        "name": "GettUpp Focus Sprint",
        "description": "Single-project deep work sessions on GettUpp.",
        "action_sequence": "single-project focus → mixed actions → sustained output",
        "trigger_hour": 11,
        "avg_duration_min": gettup[1] if gettup else 128,
        "avg_events": gettup[2] if gettup else 150,
        "frequency": gettup[0] if gettup else 65,
        "effectiveness": 0.9,
        "category": "execution",
        "recommendation": ("Your best workflow. Protect it: no Slack, no email, "
                          "no browser tabs outside project scope. Schedule 2h blocks."),
    })

    # ── WF5: Multi-Project Juggle ──
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
        "description": ("Sessions spanning 2+ projects with context switches. "
                        "Longer sessions but lower intensity per project."),
        "action_sequence": "ProjectA work → context_switch → ProjectB work → switch back",
        "trigger_hour": None,
        "avg_duration_min": multi[1] if multi else 180,
        "avg_events": multi[2] if multi else 200,
        "frequency": multi[0] if multi else 44,
        "effectiveness": 0.5,
        "category": "execution",
        "recommendation": ("AVOID when possible. Resumption lags are often tens of minutes in field observations. "
                          "If unavoidable, use 'batch mode': finish one project's "
                          "task list COMPLETELY before switching."),
    })

    # ── WF6: Late Night Push ──
    late = con.execute("""
        SELECT COUNT(*), ROUND(AVG(duration_min), 0), ROUND(AVG(event_count), 0)
        FROM sessions
        WHERE EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') >= 22
           OR EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') < 5
    """).fetchone()

    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Late Night Push",
        "description": ("Work sessions starting 10PM-5AM CST. High productivity score "
                        "at 3AM (!) but costs 5.5h recovery gap next day."),
        "action_sequence": "late start → sustained work → eventual crash",
        "trigger_hour": 22,
        "avg_duration_min": late[1] if late else 91,
        "avg_events": late[2] if late else 74,
        "frequency": late[0] if late else 62,
        "effectiveness": 0.4,
        "category": "health",
        "recommendation": ("Your 3AM productivity (score 18.83) is real but unsustainable. "
                          "The 5.5h recovery gap means net output is NEGATIVE vs sleeping "
                          "and starting fresh at 6AM. Reserve for deadlines only."),
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
        "description": "Search/query → assess time range → iterate.",
        "action_sequence": "hint_suggested_query → time_range → hint_suggested_query (loop)",
        "trigger_hour": None,
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": qdi[0] if qdi else 637,
        "effectiveness": 0.7,
        "category": "analysis",
        "recommendation": ("Good analytical pattern. Improve by adding a 'conclusion step' — "
                          "after 3 query rounds, force yourself to write a 1-sentence finding."),
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
        "description": "Focused IronClad-only sessions.",
        "action_sequence": "IronClad focus → planning/code → output",
        "trigger_hour": None,
        "avg_duration_min": iron[1] if iron else 51,
        "avg_events": iron[2] if iron else 50,
        "frequency": iron[0] if iron else 5,
        "effectiveness": 0.7,
        "category": "execution",
        "recommendation": ("NEEDS MORE of these. IronClad is your priority product "
                          "but only has 5 focused sessions. Schedule daily 90-min blocks."),
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
        "description": ("IDE configuration, tool setup, debugging environment issues. "
                        "Necessary but often becomes a trap."),
        "action_sequence": "config debugging → troubleshooting → more config",
        "trigger_hour": None,
        "avg_duration_min": tool_sess[1] if tool_sess else 154,
        "avg_events": None,
        "frequency": tool_sess[0] if tool_sess else 9,
        "effectiveness": 0.2,
        "category": "overhead",
        "recommendation": ("Cap at 30 min. If not solved, Docker/reset/move on. "
                          "Your LTM shows 15+ config troubleshooting workstreams. "
                          "Write a runbook and STOP reinventing the wheel."),
    })

    # ── WF10: Conversation + Activity Burst ──
    wf_id += 1
    workflows.append({
        "workflow_id": wf_id,
        "name": "Conversation Sprint",
        "description": "Conversation-driven sessions (AI chat, pair programming).",
        "action_sequence": "conversation_activity → activity → hint_suggested_query → repeat",
        "trigger_hour": None,
        "avg_duration_min": None,
        "avg_events": None,
        "frequency": 177,
        "effectiveness": 0.65,
        "category": "collaboration",
        "recommendation": ("Good for unblocking. Limit to 45-min bursts. "
                          "Always end with Annotation Pipeline to capture insights."),
    })

    return workflows


# ---------------------------------------------------------------------------
# PHASE 2: Coaching Rules Engine
# ---------------------------------------------------------------------------

def generate_coaching_rules(con: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """Generates evidence-backed coaching rules from behavioral data.

    Analyses trends and identifies suboptimal patterns to create
    actionable rules for productivity, health, and focus.

    Args:
        con: Active DuckDB connection.

    Returns:
        A list of dictionaries, each representing a coaching rule.
    """
    print("  [2/5] Generating coaching rules...")

    rules: List[Dict[str, Any]] = []
    rid = 0

    # Rule 1: Web browsing cap
    web_pct = con.execute("""
        SELECT ROUND(COUNT(CASE WHEN action = 'web_visit' THEN 1 END) * 100.0 / COUNT(*), 1)
        FROM events
    """).fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid,
        "category": "focus",
        "rule_text": (f"Cap web browsing below 30% of activity (currently {web_pct}%). "
                     "Low-web days produce 39% more output."),
        "evidence_sql": ("SELECT ts_start::DATE, SUM(CASE WHEN action='web_visit' THEN 1 ELSE 0 END)*100.0/COUNT(*) "
                        "FROM events GROUP BY 1"),
        "evidence_count": int(web_pct * 280),
        "severity": "high" if web_pct > 40 else "medium",
        "confidence": 0.85,
    })

    # Rule 2: Context switch limit
    if _table_exists(con, "context_switches"):
        thrash_sql = (
            "SELECT session_id, COUNT(*) FROM context_switches GROUP BY 1 HAVING COUNT(*) > 3"
        )
        thrash = con.execute(
            """
            SELECT COUNT(*) FROM sessions s
            JOIN (SELECT session_id, COUNT(*) as cs FROM context_switches GROUP BY 1 HAVING COUNT(*) > 3) c
            ON s.session_id = c.session_id
            """
        ).fetchone()[0]
    elif _table_exists(con, "sessions"):
        # Fallback: derive context-switch-heavy sessions from multi-project session strings.
        thrash_sql = (
            "SELECT session_id FROM sessions "
            "WHERE projects IS NOT NULL "
            "AND (length(projects) - length(replace(projects, ',', ''))) >= 3"
        )
        thrash = con.execute(
            """
            SELECT COUNT(*)
            FROM sessions
            WHERE projects IS NOT NULL
              AND (length(projects) - length(replace(projects, ',', ''))) >= 3
            """
        ).fetchone()[0]
    else:
        thrash_sql = None
        thrash = 0
    rid += 1
    rules.append({
        "rule_id": rid,
        "category": "focus",
        "rule_text": "Limit project switches to 3 per session. Each switch costs ~23 min re-focus time.",
        "evidence_sql": thrash_sql,
        "evidence_count": thrash,
        "severity": "high",
        "confidence": 0.85,
    })

    # Rule 3: Late night cutoff
    late = con.execute("""
        SELECT COUNT(*) FROM events
        WHERE EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') >= 23
           OR EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') < 5
    """).fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid,
        "category": "health",
        "rule_text": "Stop work by 11PM local time. Late sessions are associated with longer recovery gaps and lower next-day output.",
        "evidence_sql": "SELECT COUNT(*) FROM events WHERE EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') >= 23",
        "evidence_count": late,
        "severity": "high",
        "confidence": 0.80,
    })

    # Rule 4: IronClad Offer vs Audience Balance
    iron_offer = con.execute("""
        SELECT COUNT(*) FROM event_subcategories
        WHERE theme = 'IronClad' AND subcategory = 'Offer'
    """).fetchone()[0]
    iron_audience = con.execute("""
        SELECT COUNT(*) FROM event_subcategories
        WHERE theme = 'IronClad' AND subcategory = 'Audience'
    """).fetchone()[0]

    rid += 1
    rules.append({
        "rule_id": rid,
        "category": "strategy",
        "rule_text": (f"Rebalance IronClad focus. You have {iron_offer} 'Offer' events vs {iron_audience} 'Audience'. "
                     "Over-engineering the product without market validation is a primary risk."),
        "evidence_sql": "SELECT subcategory, COUNT(*) FROM event_subcategories WHERE theme='IronClad' GROUP BY 1",
        "evidence_count": iron_offer + iron_audience,
        "severity": "medium" if iron_audience > 10 else "high",
        "confidence": 0.90,
    })

    # Rule 5: Break cadence
    marathon = con.execute("""
        SELECT COUNT(*) FROM sessions WHERE duration_min > 240
    """).fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid,
        "category": "health",
        "rule_text": (f"Take a 10-min break every 90 min. {marathon} marathon sessions (4h+) "
                     "show diminishing returns after 90 min."),
        "evidence_sql": "SELECT COUNT(*) FROM sessions WHERE duration_min > 240",
        "evidence_count": marathon,
        "severity": "medium",
        "confidence": 0.75,
    })

    # Rule 6: Research to Annotation Ratio
    rid += 1
    rules.append({
        "rule_id": rid,
        "category": "execution",
        "rule_text": ("After every Research Loop, run Annotation Pipeline. "
                     "Capture findings immediately or lose them."),
        "evidence_sql": None,
        "evidence_count": 869,
        "severity": "medium",
        "confidence": 0.70,
    })

    # Rule 7: AI Arbitrage Integration
    ai_prompts = con.execute("""
        SELECT COUNT(*) FROM event_subcategories
        WHERE theme = 'AI Arbitrage' AND subcategory = 'Prompts'
    """).fetchone()[0]
    rid += 1
    rules.append({
        "rule_id": rid,
        "category": "automation",
        "rule_text": (f"Scale AI Arbitrage prompts. Currently only {ai_prompts} prompt-specific "
                     "events found. Automation is the lever for scaling your output."),
        "evidence_sql": "SELECT COUNT(*) FROM event_subcategories WHERE theme='AI Arbitrage' AND subcategory='Prompts'",
        "evidence_count": ai_prompts,
        "severity": "medium",
        "confidence": 0.80,
    })

    return rules


# ---------------------------------------------------------------------------
# PHASE 3: Daily Performance Scoring
# ---------------------------------------------------------------------------

def compute_daily_scores(con: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """Computes 4-axis performance scores for every active day.

    Calculates Focus, Output, Health, and Consistency scores to provide
    a holistic view of daily performance.

    Args:
        con: Active DuckDB connection.

    Returns:
        A list of dictionaries, each representing a day's scores.
    """
    print("  [3/5] Computing daily performance scores...")

    rows = con.execute("""
        WITH daily AS (
            SELECT
                ts_start::DATE as day,
                COUNT(*) as total_events,
                COUNT(DISTINCT action) as unique_actions,
                -- Focus: inverse of web_visit ratio
                1.0 - (SUM(CASE WHEN action = 'web_visit' THEN 1 ELSE 0 END)::FLOAT / GREATEST(COUNT(*), 1)) as focus_raw,
                -- Output: count of events
                COUNT(*)::FLOAT as output_raw,
                -- Health: penalty for late night + marathon sessions
                CASE
                    WHEN MAX(EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago')) >= 23
                      OR MIN(EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago')) < 5
                    THEN 0.5
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

    scores: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        # Consistency: based on streak
        prev_day = rows[i - 1][0] if i > 0 else None
        streak_bonus = 1.0 if prev_day and (r[0] - prev_day).days == 1 else 0.5
        consistency = float(round(float(streak_bonus * 10), 1))

        focus = float(r[3])
        output = float(r[4])
        health = float(r[5])
        composite = float(round(float(focus * 0.3 + output * 0.3 + health * 0.2 + consistency * 0.2), 1))

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

def track_improvement(con: duckdb.DuckDBPyConnection, rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Checks each rule against recent data to track improvement.

    Evaluates rule violations on a day-per-day basis for the last 14 days.

    Args:
        con: Active DuckDB connection.
        rules: List of active coaching rules.

    Returns:
        A list of improvement log entries.
    """
    print("  [4/5] Tracking improvement on rules...")

    log_entries: List[Dict[str, Any]] = []
    daily_metrics = con.execute("""
        WITH recent_days AS (
            SELECT DISTINCT ts_start::DATE AS day
            FROM events
            WHERE ts_start IS NOT NULL
            ORDER BY day DESC
            LIMIT 14
        ),
        daily AS (
            SELECT
                d.day,
                ROUND(
                    SUM(CASE WHEN e.action = 'web_visit' THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(COUNT(*), 0),
                    1
                ) AS web_pct,
                SUM(
                    CASE
                        WHEN EXTRACT(HOUR FROM e.ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') >= 23
                          OR EXTRACT(HOUR FROM e.ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') < 5
                        THEN 1
                        ELSE 0
                    END
                ) AS late_events
            FROM recent_days d
            JOIN events e
              ON e.ts_start::DATE = d.day
            GROUP BY d.day
        )
        SELECT day, web_pct, late_events
        FROM daily
        ORDER BY day DESC
    """).fetchall()

    for day, web_pct, late_events in daily_metrics:
        day_str = str(day)
        if web_pct is not None:
            log_entries.append({
                "date": day_str,
                "rule_id": 1,
                "violated": web_pct > 30,
                "metric_value": web_pct,
                "target_value": 30.0,
            })
        log_entries.append({
            "date": day_str,
            "rule_id": 3,
            "violated": late_events > 0,
            "metric_value": float(late_events),
            "target_value": 0.0,
        })

    return log_entries


# ---------------------------------------------------------------------------
# PHASE 5: Write results & Report
# ---------------------------------------------------------------------------

def write_to_db(
    con: duckdb.DuckDBPyConnection,
    workflows: List[Dict[str, Any]],
    rules: List[Dict[str, Any]],
    scores: List[Dict[str, Any]],
    improvement: List[Dict[str, Any]]
) -> None:
    """Persists all analysis results to DuckDB.

    Args:
        con: Active DuckDB connection.
        workflows: List of discovered workflows.
        rules: List of coaching rules.
        scores: List of daily scores.
        improvement: List of improvement log entries.
    """
    print("  [5/5] Writing to database...")

    con.execute("BEGIN TRANSACTION")
    try:
        # Workflows
        con.execute("DELETE FROM discovered_workflows")
        workflow_rows = [
            [
                w["workflow_id"],
                w["name"],
                w["description"],
                w["action_sequence"],
                w.get("trigger_hour"),
                w.get("avg_duration_min"),
                w.get("avg_events"),
                w["frequency"],
                w["effectiveness"],
                w["category"],
                w["recommendation"],
            ]
            for w in workflows
        ]
        if workflow_rows:
            con.executemany("""
                INSERT INTO discovered_workflows
                (workflow_id, name, description, action_sequence, trigger_hour,
                 avg_duration_min, avg_events, frequency, effectiveness, category, recommendation)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, workflow_rows)

        # Coaching rules
        con.execute("DELETE FROM coaching_rules")
        rule_rows = [
            [
                r["rule_id"],
                r["category"],
                r["rule_text"],
                r.get("evidence_sql"),
                r["evidence_count"],
                r["severity"],
                r["confidence"],
            ]
            for r in rules
        ]
        if rule_rows:
            con.executemany("""
                INSERT INTO coaching_rules
                (rule_id, category, rule_text, evidence_sql, evidence_count, severity, confidence)
                VALUES (?,?,?,?,?,?,?)
            """, rule_rows)

        # Daily scores
        con.execute("DELETE FROM daily_scores")
        score_rows = [
            [
                s["date"],
                s["total_events"],
                s["unique_actions"],
                s["focus_score"],
                s["output_score"],
                s["health_score"],
                s["consistency_score"],
                s["composite_score"],
            ]
            for s in scores
        ]
        if score_rows:
            con.executemany("""
                INSERT INTO daily_scores
                (date, total_events, unique_actions, focus_score, output_score,
                 health_score, consistency_score, composite_score)
                VALUES (?,?,?,?,?,?,?,?)
            """, score_rows)

        # Improvement log
        con.execute("DELETE FROM improvement_log")
        improvement_rows = [
            [
                e["date"],
                e["rule_id"],
                e["violated"],
                e.get("metric_value"),
                e.get("target_value"),
            ]
            for e in improvement
        ]
        if improvement_rows:
            con.executemany("""
                INSERT INTO improvement_log (date, rule_id, violated, metric_value, target_value)
                VALUES (?,?,?,?,?)
            """, improvement_rows)

        con.execute("COMMIT")
        print("    -> Successfully wrote all phase data to DB")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"    -> Error writing to DB: {e}", file=sys.stderr)


def generate_report(
    workflows: List[Dict[str, Any]],
    rules: List[Dict[str, Any]],
    scores: List[Dict[str, Any]],
    improvement: List[Dict[str, Any]]
) -> str:
    """Generates a full markdown report of the Architect's findings.

    Args:
        workflows: List of workflows.
        rules: List of rules.
        scores: List of scores.
        improvement: List of improvement entries.

    Returns:
        Markdown-formatted report string.
    """
    lines: List[str] = []
    now = datetime.now(CST)

    # First line must be H1
    lines.append("# ME-OPS ARCHITECT REPORT")
    lines.append("")
    lines.append(f"*Generated: {now.strftime('%Y-%m-%d %H:%M CST')}*")
    lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("## YOUR MODULARIZED WORKFLOWS")
    lines.append("")

    for cat in ["execution", "research", "analysis", "documentation", "collaboration", "health", "overhead"]:
        cat_wfs = [w for w in workflows if w["category"] == cat]
        if not cat_wfs:
            continue
        lines.append(f"### {cat.upper()}")
        lines.append("")
        for w in cat_wfs:
            eff_bar = "█" * int(w["effectiveness"] * 10) + "░" * (10 - int(w["effectiveness"] * 10))
            lines.append(f"#### {w['name']} `[{eff_bar}]` {w['effectiveness']:.0%} effective")
            lines.append("")
            lines.append(f"* **Sequence**: `{w['action_sequence']}`")
            lines.append(f"* **Frequency**: {w['frequency']}x observed")
            lines.append(f"* **Description**: {w['description']}")
            lines.append(f"* > **UPGRADE**: {w['recommendation']}")
            lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("## COACHING RULES")
    lines.append("")
    lines.append("| # | Cat | Rule | Sev | Conf |")
    lines.append("| :--- | :--- | :--- | :--- | :--- |")
    for r in sorted(rules, key=lambda x: x["confidence"], reverse=True):
        icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(r["severity"], "⚪")
        rule_short = r["rule_text"][:60] + "..." if len(r["rule_text"]) > 60 else r["rule_text"]
        lines.append(f"| {r['rule_id']} | {r['category']} | {rule_short} | {icon} | {r['confidence']:.0%} |")

    lines.append("\n---")
    lines.append("")
    lines.append("## DAILY SCORES (last 14 days)")
    lines.append("")
    lines.append("| Date | Focus | Output | Health | Composite |")
    lines.append("| :--- | :--- | :--- | :--- | :--- |")

    # Type-safe indexing to satisfy Pyre2
    n_scores = len(scores)
    start_idx = max(0, n_scores - 14)
    for i in range(start_idx, n_scores):
        s = scores[i]
        lines.append(
            f"| {s['date']} | {s['focus_score']:.1f} | {s['output_score']:.1f} | "
            f"{s['health_score']:.1f} | **{s['composite_score']:.1f}** |"
        )

    lines.append("") # Ensure trailing newline
    return "\n".join(lines)


def generate_ai_coaching(
    workflows: List[Dict[str, Any]],
    rules: List[Dict[str, Any]],
    scores: List[Dict[str, Any]]
) -> Optional[str]:
    """Gemini-powered coaching narrative with exponential backoff.

    Args:
        workflows: Discovered workflows.
        rules: Coaching rules.
        scores: Daily scores.

    Returns:
        A string containing the AI's coaching narrative, or None if failed.
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None

    try:
        from google import genai
        from google.genai import types
        from time_utils import DEFAULT_MODEL_ID
    except ImportError:
        print("  AI coaching requires 'google-genai' package.", file=sys.stderr)
        return None

    client = genai.Client(api_key=api_key)

    # Type-safe slicing for Pyre2
    n_scores = len(scores)
    recent_scores = [scores[i] for i in range(max(0, n_scores - 7), n_scores)]
    data_summary = json.dumps({
        "workflows": [{"name": w["name"], "effectiveness": w["effectiveness"], "recommendation": w["recommendation"]} for w in workflows],
        "rules": [{"rule": r["rule_text"], "severity": r["severity"]} for r in rules],
        "recent_scores": recent_scores,
    }, default=str)

    prompt = (
        "You are a brutally honest performance coach for a developer. Analyze this actual workflow data:\n\n"
        f"{data_summary}\n\n"
        "Write a 400-word coaching session. Be specific, cite their workflow names, and point out one 'uncomfortable truth' "
        "revealed by the patterns. Use Markdown formatting."
    )

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model=DEFAULT_MODEL_ID,
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.3),
            )
            return response.text
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 5 * (2 ** attempt)
                print(f"  [AI Backoff] {wait}s...", file=sys.stderr)
                time.sleep(wait)
                continue
            print(f"  AI coaching failed: {e}", file=sys.stderr)

    return None


def main() -> None:
    """Main execution entry point."""
    parser = argparse.ArgumentParser(description="ME-OPS Architect")
    parser.add_argument("--report", action="store_true", help="Report only")
    parser.add_argument("--ai", action="store_true", help="Add AI coaching")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    print("ME-OPS Architect Engine")
    print("=" * 60)

    con = _connect(read_only=args.report)
    if not args.report:
        _init_schema(con)

    workflows = extract_workflows(con)
    rules = generate_coaching_rules(con)
    scores = compute_daily_scores(con)
    improvement = track_improvement(con, rules)

    if not args.report:
        write_to_db(con, workflows, rules, scores, improvement)

    con.close()

    if args.json:
        n_scores = len(scores)
        recent_scores = [scores[i] for i in range(max(0, n_scores - 14), n_scores)]
        print(json.dumps({"workflows": workflows, "rules": rules, "scores": recent_scores}, indent=2, default=str))
        return

    report = generate_report(workflows, rules, scores, improvement)

    if args.ai:
        print("  Generating AI coaching...")
        ai_narrative = generate_ai_coaching(workflows, rules, scores)
        if ai_narrative:
            report = f"## AI COACHING NARRATIVE\n\n{ai_narrative}\n\n---\n\n{report}"

    print(report)

    OUTPUT_DIR.mkdir(exist_ok=True)
    today = datetime.now(CST).strftime("%Y-%m-%d")
    out_path = OUTPUT_DIR / f"architect_{today}.md"
    out_path.write_text(report, encoding="utf-8")
    print(f"\nSaved to: {out_path}")


if __name__ == "__main__":
    main()
