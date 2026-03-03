#!/usr/bin/env python3
"""ME-OPS Deep Analysis — behavioral self-architecture engine.

Mines 28K+ events across 64 days to extract:
1. Temporal superpowers (when you're unstoppable)
2. Flow state triggers (what enters you into deep work)
3. Failure modes (exact conditions that cause underperformance)
4. Success patterns (proven recipes for high-output days)
5. Hidden insights (non-obvious correlations and superpowers)

Generates a self-architecture blueprint with evidence-backed rules.

Skills used: ai-engineer (multi-dimensional analysis pipelines),
             production-code-audit (data integrity validation),
             workflow-patterns (structured analytical phases)

Usage:
    python deep_analysis.py              # Full analysis (saved to output/)
    python deep_analysis.py --json       # Machine-readable output
    python deep_analysis.py --ai         # Add Gemini-powered narrative
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DB_PATH = Path(__file__).parent / "me_ops.duckdb"
OUTPUT_DIR = Path(__file__).parent / "output"
from time_utils import local_now, LOCAL_TZ


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


# ---------------------------------------------------------------------------
# ANALYSIS DIMENSION 1: Temporal Superpowers
# ---------------------------------------------------------------------------

def analyze_temporal(con: duckdb.DuckDBPyConnection) -> dict:
    """When are you most/least productive? What hours unlock superpowers?"""
    print("  [1/5] Temporal superpowers...")

    # Productivity score by hour (events/min * action_diversity)
    hourly = con.execute("""
        SELECT
            EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as hr,
            COUNT(*) as sessions,
            ROUND(AVG(event_count::FLOAT / GREATEST(duration_min, 1)), 3) as evt_per_min,
            ROUND(AVG(unique_actions), 1) as avg_actions,
            ROUND(AVG(event_count::FLOAT / GREATEST(duration_min, 1)) * AVG(unique_actions), 2) as prod_score
        FROM sessions WHERE duration_min > 5
        GROUP BY hr ORDER BY prod_score DESC
    """).fetchall()

    # Day of week ranking
    daily = con.execute("""
        SELECT
            DAYNAME(ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as dow,
            EXTRACT(DOW FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as dow_num,
            COUNT(*) as events,
            COUNT(DISTINCT ts_start::DATE) as active_days,
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT ts_start::DATE), 0), 0) as events_per_day
        FROM events WHERE ts_start IS NOT NULL
        GROUP BY dow, dow_num ORDER BY events_per_day DESC
    """).fetchall()

    # Weekly evolution trend
    weekly = con.execute("""
        SELECT
            EXTRACT(WEEK FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as week,
            MIN(ts_start::DATE) as week_start,
            COUNT(*) as events,
            COUNT(DISTINCT ts_start::DATE) as active_days,
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT ts_start::DATE), 0), 0) as events_per_day
        FROM events WHERE ts_start IS NOT NULL
        GROUP BY week ORDER BY week_start
    """).fetchall()

    return {
        "peak_hours": [
            {"hour": int(r[0]), "score": r[4], "sessions": r[1],
             "evt_per_min": r[2], "action_diversity": r[3]}
            for r in hourly[:5]
        ],
        "dead_hours": [
            {"hour": int(r[0]), "score": r[4], "sessions": r[1]}
            for r in hourly[-3:]
        ],
        "best_days": [
            {"day": r[0], "events_per_day": r[4]} for r in daily[:3]
        ],
        "weekly_trend": [
            {"week_start": str(r[1]), "events": r[2],
             "active_days": r[3], "events_per_day": r[4]}
            for r in weekly
        ],
    }


# ---------------------------------------------------------------------------
# ANALYSIS DIMENSION 2: Flow State Triggers
# ---------------------------------------------------------------------------

def analyze_flow_states(con: duckdb.DuckDBPyConnection) -> dict:
    """What conditions trigger your deepest focus?"""
    print("  [2/5] Flow state triggers...")

    # Flow sessions: high output AND high diversity AND long duration
    flow = con.execute("""
        SELECT s.session_id, s.ts_start::DATE as date,
               EXTRACT(HOUR FROM s.ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as start_hr,
               s.duration_min, s.event_count, s.unique_actions,
               COALESCE(s.projects, '—') as projects,
               COALESCE(sc.cluster_name, '—') as cluster
        FROM sessions s
        LEFT JOIN session_clusters sc ON s.session_id = sc.session_id
        WHERE s.event_count > 100 AND s.unique_actions >= 4 AND s.duration_min > 60
        ORDER BY s.event_count DESC LIMIT 10
    """).fetchall()

    # What time do flow states start?
    flow_hours = con.execute("""
        SELECT EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as hr, COUNT(*) as cnt
        FROM sessions
        WHERE event_count > 100 AND unique_actions >= 4 AND duration_min > 60
        GROUP BY hr ORDER BY cnt DESC
    """).fetchall()

    # Top 25% vs Bottom 25%
    comparison = con.execute("""
        WITH ranked AS (
            SELECT *, NTILE(4) OVER (ORDER BY event_count) as quartile,
                EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as hr
            FROM sessions WHERE duration_min > 10
        )
        SELECT
            CASE quartile WHEN 4 THEN 'top_25' WHEN 1 THEN 'bottom_25' END as tier,
            COUNT(*) as cnt, ROUND(AVG(duration_min), 0) as avg_dur,
            ROUND(AVG(event_count), 0) as avg_events,
            ROUND(AVG(unique_actions), 1) as avg_actions,
            ROUND(AVG(hr), 1) as avg_start_hr
        FROM ranked WHERE quartile IN (1, 4) GROUP BY tier
    """).fetchall()

    return {
        "top_flow_sessions": [
            {"date": str(r[1]), "start_hour": int(r[2]),
             "duration_min": r[3], "events": r[4],
             "actions": r[5], "projects": r[6], "cluster": r[7]}
            for r in flow
        ],
        "flow_trigger_hours": [
            {"hour": int(r[0]), "count": r[1]} for r in flow_hours[:5]
        ],
        "quartile_comparison": {
            r[0]: {"count": r[1], "avg_duration": r[2], "avg_events": r[3],
                   "avg_actions": r[4], "avg_start_hour": r[5]}
            for r in comparison
        },
    }


# ---------------------------------------------------------------------------
# ANALYSIS DIMENSION 3: Failure Modes
# ---------------------------------------------------------------------------

def analyze_failures(con: duckdb.DuckDBPyConnection) -> dict:
    """What exact conditions cause you to underperform?"""
    print("  [3/5] Failure modes...")

    # Existing failure patterns
    patterns = con.execute("""
        SELECT pattern_type, description, evidence_count, severity
        FROM failure_patterns ORDER BY evidence_count DESC
    """).fetchall()

    # Anti-playbook rules
    rules = con.execute("""
        SELECT rule_text, trigger, confidence, evidence
        FROM anti_playbook ORDER BY confidence DESC
    """).fetchall()

    # Context switch impact on session output
    switch_impact = con.execute("""
        SELECT
            CASE
                WHEN switch_count = 0 THEN '0 switches'
                WHEN switch_count <= 3 THEN '1-3 switches'
                WHEN switch_count <= 5 THEN '4-5 switches'
                ELSE '6+ switches'
            END as bucket,
            COUNT(*) as sessions,
            ROUND(AVG(event_count), 0) as avg_events,
            ROUND(AVG(duration_min), 0) as avg_dur,
            ROUND(AVG(event_count::FLOAT / GREATEST(duration_min, 1)), 2) as intensity
        FROM (
            SELECT s.session_id, s.event_count, s.duration_min,
                   COALESCE(cs_count, 0) as switch_count
            FROM sessions s
            LEFT JOIN (
                SELECT session_id, COUNT(*) as cs_count
                FROM context_switches GROUP BY session_id
            ) cs ON s.session_id = cs.session_id
            WHERE s.duration_min > 10
        ) sub
        GROUP BY bucket
        ORDER BY MIN(switch_count)
    """).fetchall()

    # Late night aftermath
    aftermath = con.execute("""
        WITH late AS (
            SELECT session_id, ts_start, ts_end, duration_min, event_count
            FROM sessions
            WHERE EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') >= 22
               OR EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') < 5
        ),
        next_session AS (
            SELECT l.session_id as late_id,
                   l.event_count as late_events,
                   s.duration_min as next_dur, s.event_count as next_events,
                   EXTRACT(HOUR FROM s.ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as next_start_hr,
                   EXTRACT(EPOCH FROM s.ts_start - l.ts_end) / 3600 as gap_hours
            FROM late l JOIN sessions s ON s.ts_start > l.ts_end
            QUALIFY ROW_NUMBER() OVER (PARTITION BY l.session_id ORDER BY s.ts_start) = 1
        )
        SELECT ROUND(AVG(next_dur), 1), ROUND(AVG(next_events), 0),
               ROUND(AVG(next_start_hr), 1), ROUND(AVG(gap_hours), 1),
               COUNT(*),
               ROUND(AVG(late_events), 0) as avg_late_events
        FROM next_session
    """).fetchone()

    return {
        "failure_patterns": [
            {"type": r[0], "description": r[1],
             "evidence": r[2], "severity": r[3]}
            for r in patterns
        ],
        "anti_playbook": [
            {"rule": r[0], "trigger": r[1], "confidence": r[2]}
            for r in rules
        ],
        "context_switch_impact": [
            {"bucket": r[0], "sessions": r[1], "avg_events": r[2],
             "avg_duration": r[3], "intensity": r[4]}
            for r in switch_impact
        ],
        "late_night_aftermath": {
            "sample_size": aftermath[4] if aftermath else 0,
            "avg_late_events": aftermath[5] if aftermath else 0,
            "next_session_start_hr": aftermath[2] if aftermath else 0,
            "recovery_gap_hours": aftermath[3] if aftermath else 0,
            "next_session_duration": aftermath[0] if aftermath else 0,
            "next_session_events": aftermath[1] if aftermath else 0,
        },
    }


# ---------------------------------------------------------------------------
# ANALYSIS DIMENSION 4: Success Patterns
# ---------------------------------------------------------------------------

def analyze_success(con: duckdb.DuckDBPyConnection) -> dict:
    """What exact conditions lead to your best work?"""
    print("  [4/5] Success patterns...")

    # Best days: what made them special?
    best_days = con.execute("""
        SELECT
            ts_start::DATE as day,
            DAYNAME(ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') as dow,
            COUNT(*) as events,
            COUNT(DISTINCT action) as actions,
            MIN(EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago')) as earliest_hr,
            MAX(EXTRACT(HOUR FROM ts_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago')) as latest_hr
        FROM events WHERE ts_start IS NOT NULL
        GROUP BY day, dow ORDER BY events DESC LIMIT 5
    """).fetchall()

    # Project productivity comparison
    project_prod = con.execute("""
        SELECT p.name, COUNT(ep.event_id) as events,
               COUNT(DISTINCT e.ts_start::DATE) as active_days,
               ROUND(COUNT(ep.event_id) * 1.0 / NULLIF(COUNT(DISTINCT e.ts_start::DATE), 0), 0) as events_per_day
        FROM event_projects ep
        JOIN projects p ON ep.project_id = p.project_id
        JOIN events e ON ep.event_id = e.event_id
        GROUP BY p.name ORDER BY events DESC
    """).fetchall()

    # Workflow chains that lead to high output
    chains = con.execute("""
        SELECT from_action, to_action, weight, ROUND(avg_gap_sec, 0) as gap
        FROM workflow_edges WHERE from_action != to_action
        ORDER BY weight DESC LIMIT 8
    """).fetchall()

    return {
        "best_days": [
            {"date": str(r[0]), "dow": r[1], "events": r[2],
             "actions": r[3], "earliest_hr": int(r[4]),
             "latest_hr": int(r[5])}
            for r in best_days
        ],
        "project_productivity": [
            {"name": r[0], "events": r[1], "active_days": r[2],
             "events_per_day": r[3]}
            for r in project_prod
        ],
        "strongest_chains": [
            {"from": r[0], "to": r[1], "count": r[2],
             "gap_sec": r[3]}
            for r in chains
        ],
    }


# ---------------------------------------------------------------------------
# ANALYSIS DIMENSION 5: Hidden Insights / Superpowers
# ---------------------------------------------------------------------------

def analyze_hidden(con: duckdb.DuckDBPyConnection) -> dict:
    """Non-obvious correlations and superpower discoveries."""
    print("  [5/5] Hidden insights...")

    # Consistency score: streaks of active days
    streaks = con.execute("""
        WITH days AS (
            SELECT DISTINCT ts_start::DATE as day
            FROM events WHERE ts_start IS NOT NULL ORDER BY day
        ),
        numbered AS (
            SELECT day, ROW_NUMBER() OVER (ORDER BY day) as rn,
                   day - INTERVAL (ROW_NUMBER() OVER (ORDER BY day)) DAY as grp
            FROM days
        )
        SELECT MIN(day) as start, MAX(day) as end,
               COUNT(*) as streak_days
        FROM numbered GROUP BY grp
        HAVING COUNT(*) > 1
        ORDER BY streak_days DESC LIMIT 5
    """).fetchall()

    # Session duration sweet spot: which durations yield highest events/min?
    sweet_spot = con.execute("""
        SELECT
            CASE
                WHEN duration_min < 30 THEN 'short (<30m)'
                WHEN duration_min < 90 THEN 'medium (30-90m)'
                WHEN duration_min < 180 THEN 'focused (90-180m)'
                WHEN duration_min < 300 THEN 'extended (3-5h)'
                ELSE 'marathon (5h+)'
            END as bucket,
            COUNT(*) as sessions,
            ROUND(AVG(event_count::FLOAT / GREATEST(duration_min, 1)), 2) as intensity,
            ROUND(SUM(event_count) * 100.0 / SUM(SUM(event_count)) OVER(), 1) as pct_total_output
        FROM sessions WHERE duration_min > 5
        GROUP BY bucket
        ORDER BY intensity DESC
    """).fetchall()

    # Web visit ratio vs output correlation
    web_impact = con.execute("""
        WITH daily AS (
            SELECT ts_start::DATE as day,
                   COUNT(*) as total,
                   SUM(CASE WHEN action = 'web_visit' THEN 1 ELSE 0 END) as web_visits,
                   ROUND(SUM(CASE WHEN action = 'web_visit' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as web_pct,
                   COUNT(DISTINCT action) as actions
            FROM events WHERE ts_start IS NOT NULL
            GROUP BY day HAVING COUNT(*) > 20
        )
        SELECT
            CASE
                WHEN web_pct < 30 THEN 'low_web (<30%)'
                WHEN web_pct < 50 THEN 'med_web (30-50%)'
                ELSE 'high_web (50%+)'
            END as category,
            COUNT(*) as days,
            ROUND(AVG(total), 0) as avg_events,
            ROUND(AVG(actions), 1) as avg_actions,
            ROUND(AVG(web_pct), 1) as avg_web_pct
        FROM daily
        GROUP BY category
        ORDER BY avg_events DESC
    """).fetchall()

    # Cluster performance comparison
    cluster_perf = con.execute("""
        SELECT sc.cluster_name,
               COUNT(*) as sessions,
               ROUND(AVG(s.duration_min), 1) as avg_dur,
               ROUND(AVG(s.event_count), 0) as avg_events,
               ROUND(AVG(s.unique_actions), 1) as avg_actions,
               ROUND(AVG(s.event_count::FLOAT / GREATEST(s.duration_min, 1)), 2) as intensity
        FROM session_clusters sc
        JOIN sessions s ON sc.session_id = s.session_id
        GROUP BY sc.cluster_name
    """).fetchall()

    return {
        "best_streaks": [
            {"start": str(r[0]), "end": str(r[1]), "days": r[2]}
            for r in streaks
        ],
        "duration_sweet_spot": [
            {"bucket": r[0], "sessions": r[1], "intensity": r[2],
             "pct_total_output": r[3]}
            for r in sweet_spot
        ],
        "web_visit_impact": [
            {"category": r[0], "days": r[1], "avg_events": r[2],
             "avg_actions": r[3], "avg_web_pct": r[4]}
            for r in web_impact
        ],
        "cluster_performance": [
            {"cluster": r[0], "sessions": r[1], "avg_duration": r[2],
             "avg_events": r[3], "avg_actions": r[4], "intensity": r[5]}
            for r in cluster_perf
        ],
    }


# ---------------------------------------------------------------------------
# Report Generator
# ---------------------------------------------------------------------------

def generate_blueprint(analysis: dict) -> str:
    """Generate the self-architecture blueprint from analysis results."""
    lines: list[str] = []

    lines.append("# ME-OPS DEEP ANALYSIS")
    lines.append("## Self-Architecture Blueprint")
    lines.append(f"*Generated: {local_now().strftime('%Y-%m-%d %H:%M %Z')}*")
    lines.append("*Data drawn from ingested events — counts computed at runtime.*")
    lines.append("")

    # ── SECTION 1: TEMPORAL SUPERPOWERS ──
    lines.append("---")
    lines.append("## 1. TEMPORAL SUPERPOWERS")
    lines.append("*When you're unstoppable vs when you're wasting time*")
    lines.append("")

    temporal = analysis["temporal"]
    lines.append("### Peak Performance Hours")
    lines.append("| Rank | Hour (CST) | Score | Sessions | Events/min | Actions |")
    lines.append("|------|-----------|-------|----------|------------|---------|")
    for i, h in enumerate(temporal["peak_hours"]):
        medal = ["🥇", "🥈", "🥉", "4th", "5th"][i]
        lines.append(f"| {medal} | {h['hour']:02d}:00 | **{h['score']}** | {h['sessions']} | {h['evt_per_min']} | {h['action_diversity']} |")

    lines.append("")
    lines.append("### Dead Hours (lowest productivity)")
    for h in temporal["dead_hours"]:
        lines.append(f"- **{h['hour']:02d}:00** → score {h['score']} ({h['sessions']} sessions)")

    lines.append("")
    lines.append("### Best Days of Week")
    for d in temporal["best_days"]:
        lines.append(f"- **{d['day']}**: {d['events_per_day']:.0f} events/day")

    lines.append("")
    trend = temporal["weekly_trend"]
    if len(trend) >= 2:
        first_epd = trend[0]["events_per_day"]
        last_epd = trend[-1]["events_per_day"]
        growth = ((last_epd / first_epd - 1) * 100) if first_epd > 0 else 0
        direction = "📈 GROWING" if growth > 0 else "📉 declining"
        lines.append(f"### Weekly Trend: {direction} ({growth:+.0f}%)")
        lines.append(f"- Start: {first_epd:.0f} events/day → Now: {last_epd:.0f} events/day")

    # ── SECTION 2: FLOW STATE TRIGGERS ──
    lines.append("")
    lines.append("---")
    lines.append("## 2. FLOW STATE TRIGGERS")
    lines.append("*What puts you in the zone*")
    lines.append("")

    flow = analysis["flow_states"]
    lines.append("### Your Top Flow Sessions")
    lines.append("| Date | Start | Duration | Events | Actions | Projects | Mode |")
    lines.append("|------|-------|----------|--------|---------|----------|------|")
    for s in flow["top_flow_sessions"][:5]:
        lines.append(f"| {s['date']} | {s['start_hour']:02d}:00 | {s['duration_min']:.0f}m | **{s['events']}** | {s['actions']} | {s['projects'][:25]} | {s['cluster']} |")

    lines.append("")
    lines.append("### Flow Entry Points")
    for h in flow["flow_trigger_hours"]:
        lines.append(f"- **{h['hour']:02d}:00 CST** → {h['count']} flow sessions started here")

    comp = flow.get("quartile_comparison", {})
    top = comp.get("top_25", {})
    bot = comp.get("bottom_25", {})
    if top and bot:
        lines.append("")
        lines.append("### Top 25% vs Bottom 25%")
        lines.append("| Metric | Top 25% | Bottom 25% | Multiplier |")
        lines.append("|--------|---------|------------|------------|")
        for metric, key in [("Duration", "avg_duration"), ("Events", "avg_events"), ("Actions", "avg_actions")]:
            t_val = top.get(key, 0) or 0
            b_val = bot.get(key, 0) or 1
            mult = t_val / b_val if b_val > 0 else 0
            lines.append(f"| {metric} | {t_val:.0f} | {b_val:.0f} | **{mult:.1f}x** |")

    # ── SECTION 3: FAILURE MODES ──
    lines.append("")
    lines.append("---")
    lines.append("## 3. FAILURE MODES")
    lines.append("*Exact conditions that sabotage your output*")
    lines.append("")

    failures = analysis["failures"]

    lines.append("### Detected Patterns")
    for p in failures["failure_patterns"]:
        icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(p["severity"], "⚪")
        lines.append(f"- {icon} **{p['type']}**: {p['description']}")

    lines.append("")
    lines.append("### Context Switching Penalty")
    lines.append("| Switches | Sessions | Avg Events | Intensity |")
    lines.append("|----------|----------|------------|-----------|")
    for cs in failures["context_switch_impact"]:
        lines.append(f"| {cs['bucket']} | {cs['sessions']} | {cs['avg_events']:.0f} | {cs['intensity']} |")

    aft = failures["late_night_aftermath"]
    if aft["sample_size"] > 0:
        lines.append("")
        lines.append("### Late Night Aftermath")
        lines.append(f"- After **{aft['sample_size']}** late sessions (10PM-5AM):")
        lines.append(f"  - Avg late session output: **{aft['avg_late_events']:.0f}** events")
        lines.append(f"  - Recovery gap: **{aft['recovery_gap_hours']:.1f} hours**")
        lines.append(f"  - Next session starts: **{aft['next_session_start_hr']:.1f}h CST**")
        lines.append(f"  - Next session output: **{aft['next_session_events']:.0f}** events in {aft['next_session_duration']:.0f}m")

    # ── SECTION 4: SUCCESS PATTERNS ──
    lines.append("")
    lines.append("---")
    lines.append("## 4. SUCCESS PATTERNS")
    lines.append("*Proven recipes for your best work*")
    lines.append("")

    success = analysis["success"]
    lines.append("### Your Peak Days")
    for d in success["best_days"]:
        span = d["latest_hr"] - d["earliest_hr"]
        lines.append(f"- **{d['date']}** ({d['dow']}): {d['events']:,} events, "
                      f"{d['actions']} action types, {span}h active window")

    lines.append("")
    lines.append("### Project Focus")
    for p in success["project_productivity"]:
        lines.append(f"- **{p['name']}**: {p['events']:,} events across {p['active_days']} days "
                      f"({p['events_per_day']:.0f}/day)")

    lines.append("")
    lines.append("### Power Chains (action sequences that produce output)")
    for c in success["strongest_chains"][:5]:
        speed = "⚡" if c["gap_sec"] < 10 else "🏃" if c["gap_sec"] < 60 else "🐢"
        lines.append(f"- {speed} **{c['from']}** → **{c['to']}** ({c['count']}x, {c['gap_sec']:.0f}s gap)")

    # ── SECTION 5: HIDDEN INSIGHTS ──
    lines.append("")
    lines.append("---")
    lines.append("## 5. HIDDEN INSIGHTS & SUPERPOWERS")
    lines.append("")

    hidden = analysis["hidden"]

    lines.append("### Consistency Streaks")
    for s in hidden["best_streaks"]:
        lines.append(f"- 🔥 **{s['days']}-day streak**: {s['start']} → {s['end']}")

    lines.append("")
    lines.append("### Duration Sweet Spot")
    lines.append("| Duration | Sessions | Intensity | % Total Output |")
    lines.append("|----------|----------|-----------|----------------|")
    for ss in hidden["duration_sweet_spot"]:
        lines.append(f"| {ss['bucket']} | {ss['sessions']} | **{ss['intensity']}** | {ss['pct_total_output']}% |")

    lines.append("")
    lines.append("### Web Browsing Impact")
    for w in hidden["web_visit_impact"]:
        lines.append(f"- **{w['category']}**: {w['days']} days, avg {w['avg_events']:.0f} events, "
                      f"{w['avg_actions']:.1f} actions")

    lines.append("")
    lines.append("### Cluster Performance")
    for c in hidden["cluster_performance"]:
        lines.append(f"- **{c['cluster']}**: {c['sessions']} sessions, "
                      f"{c['avg_duration']:.0f}m avg, {c['avg_events']:.0f} events, "
                      f"intensity={c['intensity']}")

    # ── SELF-ARCHITECTURE BLUEPRINT ──
    lines.append("")
    lines.append("---")
    lines.append("## 🏗️ SELF-ARCHITECTURE BLUEPRINT")
    lines.append("*Evidence-backed operating manual*")
    lines.append("")

    # Derive rules from data
    peak = temporal["peak_hours"][0] if temporal["peak_hours"] else None
    dead = temporal["dead_hours"][0] if temporal["dead_hours"] else None

    lines.append("### ✅ DO (proven by your data)")
    if peak:
        lines.append(f"1. **Schedule deep work at {peak['hour']:02d}:00 CST** — your peak "
                      f"productivity score ({peak['score']}) is {peak['score'] / (dead['score'] if dead else 1):.1f}x "
                      f"higher than your worst hour")

    flow_hrs = flow.get("flow_trigger_hours", [])
    if flow_hrs:
        hrs = ", ".join(f"{h['hour']:02d}:00" for h in flow_hrs[:3])
        lines.append(f"2. **Start sessions at {hrs}** — these hours trigger flow state most often")

    if top and bot:
        lines.append(f"3. **Commit to 4+ hour blocks** — your top 25% sessions average "
                      f"{top.get('avg_duration', 0):.0f} min and produce "
                      f"{top.get('avg_events', 0) / max(bot.get('avg_events', 1), 1):.0f}x more output")

    best_days_data = temporal.get("best_days", [])
    if best_days_data:
        lines.append(f"4. **Front-load {best_days_data[0]['day']}s** — your most productive "
                      f"day ({best_days_data[0]['events_per_day']:.0f} events/day)")

    sweet = hidden.get("duration_sweet_spot", [])
    if sweet:
        lines.append(f"5. **Target {sweet[0]['bucket']} sessions** — highest intensity ({sweet[0]['intensity']})")

    lines.append("")
    lines.append("### 🚫 NEVER DO (proven harmful)")
    for rule in failures.get("anti_playbook", []):
        lines.append(f"- **[{rule['confidence']:.0%}]** {rule['rule']}")

    lines.append("")
    lines.append("### 🔮 YOUR SUPERPOWERS")
    if hidden.get("best_streaks"):
        max_streak = max(hidden["best_streaks"], key=lambda x: x["days"])
        lines.append(f"1. **Streak builder**: You've hit {max_streak['days']}-day consecutive work streaks")

    if top:
        lines.append(f"2. **Deep work monster**: Your best sessions produce {top.get('avg_events', 0):.0f} "
                      f"events in {top.get('avg_duration', 0):.0f} min")

    if growth > 0:
        lines.append(f"3. **Accelerating**: Your output is growing {growth:+.0f}% week over week")

    for c in hidden.get("cluster_performance", []):
        if "Deep Work" in c["cluster"]:
            lines.append(f"4. **{c['cluster']}** is your dominant mode ({c['sessions']} sessions, "
                          f"{c['avg_events']:.0f} events avg)")

    lines.append("")
    lines.append("---")
    lines.append(f"*Analysis complete. {local_now().isoformat()}*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# AI-Enhanced Narrative (optional)
# ---------------------------------------------------------------------------

def generate_ai_narrative(analysis: dict) -> str | None:
    """Use Gemini to produce a narrative insight layer."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None

    try:
        from google import genai
        from google.genai import types
        from time_utils import DEFAULT_MODEL_ID

        client = genai.Client(api_key=api_key)

        prompt = f"""You are a world-class behavioral scientist analyzing a software
developer's work patterns. Based on this data, write a brutally honest but
motivating 500-word analysis. Focus on:
1. Their #1 hidden superpower the data reveals
2. Their biggest blind spot they probably don't know about
3. Exactly 3 leverage points that would 10x their output
4. One uncomfortable truth they need to hear

Data summary:
{json.dumps(analysis, indent=2, default=str)[:8000]}

Be specific. Use numbers. No generic advice."""

        response = client.models.generate_content(
            model=DEFAULT_MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.4),
        )
        return response.text
    except Exception as e:
        print(f"  AI narrative failed: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(db_path: Path, *, con: Optional[duckdb.DuckDBPyConnection] = None, ai: bool = False) -> dict:
    """Run full analysis pipeline."""
    print("ME-OPS Deep Analysis Engine")
    print("=" * 60)

    close_con = False
    if con is None:
        con = duckdb.connect(str(db_path), read_only=True)
        close_con = True

    try:
        analysis = {
            "temporal": analyze_temporal(con),
            "flow_states": analyze_flow_states(con),
            "failures": analyze_failures(con),
            "success": analyze_success(con),
            "hidden": analyze_hidden(con),
        }

        # Generate blueprint
        blueprint = generate_blueprint(analysis)

        # Optional AI narrative
        ai_narrative = None
        if ai:
            print("  Generating AI narrative...")
            ai_narrative = generate_ai_narrative(analysis)

        if ai_narrative:
            full_report = f"# 🧠 AI NARRATIVE\n\n{ai_narrative}\n\n---\n\n{blueprint}"
        else:
            full_report = blueprint

        # Save
        OUTPUT_DIR.mkdir(exist_ok=True)
        today = local_now().strftime("%Y-%m-%d")
        out_file = OUTPUT_DIR / f"deep_analysis_{today}.md"
        out_file.write_text(full_report, encoding="utf-8")
        print(f"\n{'=' * 60}")
        print(f"Saved to: {out_file}")

        return {"report": full_report, "analysis": analysis}
    finally:
        if close_con:
            con.close()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="ME-OPS Deep Analysis")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--ai", action="store_true", help="Add Gemini narrative")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    args = parser.parse_args()

    if args.json:
        close_con = False
        con = duckdb.connect(str(args.db), read_only=True)
        try:
            analysis = {
                "temporal": analyze_temporal(con),
                "flow_states": analyze_flow_states(con),
                "failures": analyze_failures(con),
                "success": analyze_success(con),
                "hidden": analyze_hidden(con),
            }
            print(json.dumps(analysis, indent=2, default=str))
        finally:
            con.close()
        return

    res = run(args.db, ai=args.ai)
    print(res["report"])


if __name__ == "__main__":
    main()
