#!/usr/bin/env python3
"""ME-OPS Morning Briefing — automated daily chief of staff.

Aggregates data from all ME-OPS modules and uses Gemini to produce
a concise, actionable morning briefing. Can run standalone or as
a cron job.

Skills used: ai-engineer (orchestrator pattern + LLM summary),
             prompt-engineering (structured output, CoT prompting),
             workflow-patterns (data aggregation pipeline)

Ref: https://ai.google.dev/gemini-api/docs/text-generation (official)

Usage:
    python briefing.py                    # Generate today's briefing
    python briefing.py --date 2026-02-19  # Specific date
    python briefing.py --no-ai            # Data-only briefing (no Gemini)
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

# CST offset
CST = timezone(timedelta(hours=-6))


# ---------------------------------------------------------------------------
# 1. Data collection — gather raw facts from all tables
# ---------------------------------------------------------------------------

def collect_briefing_data(con: duckdb.DuckDBPyConnection, target_date: str) -> dict:
    """Aggregate cross-module data for the briefing.

    Pulls from: events, sessions, projects, graph_edges,
    session_clusters, prediction_metrics, failure_patterns, anti_playbook.
    """
    data: dict = {"date": target_date, "sections": {}}

    # --- Daily stats ---
    row = con.execute("""
        SELECT
            COUNT(*) AS events,
            COUNT(DISTINCT action) AS actions,
            MIN(ts_start)::TIME::VARCHAR AS first,
            MAX(ts_start)::TIME::VARCHAR AS last
        FROM events
        WHERE ts_start::DATE = CAST(? AS DATE)
    """, [target_date]).fetchone()
    data["sections"]["daily_stats"] = {
        "events": row[0], "actions": row[1],
        "first_active": row[2], "last_active": row[3],
    }

    # --- Sessions ---
    def _table_exists(name: str) -> bool:
        return con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()[0] > 0

    if _table_exists("sessions"):
        rows = con.execute("""
            SELECT session_id, duration_min, event_count,
                   dominant_action, projects
            FROM sessions
            WHERE ts_start::DATE = CAST(? AS DATE)
            ORDER BY ts_start
        """, [target_date]).fetchall()
        data["sections"]["sessions"] = [
            {"id": r[0], "duration_min": r[1], "events": r[2],
             "dominant_action": r[3], "projects": r[4]}
            for r in rows
        ]

    # --- Projects ---
    rows = con.execute("""
        SELECT p.name, COUNT(ep.event_id) AS events
        FROM event_projects ep
        JOIN projects p ON ep.project_id = p.project_id
        JOIN events e ON ep.event_id = e.event_id
        WHERE e.ts_start::DATE = CAST(? AS DATE)
        GROUP BY p.name ORDER BY events DESC
    """, [target_date]).fetchall()
    data["sections"]["projects"] = [{"name": r[0], "events": r[1]} for r in rows]

    # --- Context switches ---
    if _table_exists("context_switches"):
        rows = con.execute("""
            SELECT from_project, to_project, gap_seconds
            FROM context_switches
            WHERE ts::DATE = CAST(? AS DATE)
            ORDER BY ts
        """, [target_date]).fetchall()
        data["sections"]["context_switches"] = [
            {"from": r[0], "to": r[1], "gap_s": r[2]} for r in rows
        ]

    # --- Cluster assignment ---
    if _table_exists("session_clusters"):
        rows = con.execute("""
            SELECT sc.cluster_name, COUNT(*) AS count
            FROM session_clusters sc
            JOIN sessions s ON sc.session_id = s.session_id
            WHERE s.ts_start::DATE = CAST(? AS DATE)
            GROUP BY sc.cluster_name
        """, [target_date]).fetchall()
        data["sections"]["clusters"] = [
            {"mode": r[0], "count": r[1]} for r in rows
        ]

    # --- Failure patterns ---
    if _table_exists("failure_patterns"):
        rows = con.execute("""
            SELECT severity, pattern_type, description
            FROM failure_patterns
            ORDER BY CASE severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END
        """).fetchall()
        data["sections"]["warnings"] = [
            {"severity": r[0], "type": r[1], "desc": r[2]} for r in rows
        ]

    # --- 7-day trend ---
    rows = con.execute("""
        SELECT ts_start::DATE::VARCHAR AS day, COUNT(*) AS events
        FROM events
        WHERE ts_start IS NOT NULL
          AND ts_start::DATE > CAST(? AS DATE) - INTERVAL '7 days'
          AND ts_start::DATE <= CAST(? AS DATE)
        GROUP BY day ORDER BY day DESC
    """, [target_date, target_date]).fetchall()
    data["sections"]["trend"] = [{"day": r[0], "events": r[1]} for r in rows]

    return data


# ---------------------------------------------------------------------------
# 2. AI-powered summary (optional)
# ---------------------------------------------------------------------------

BRIEFING_PROMPT = """\
You are a personal chief of staff for a software developer. Analyze this
behavioral data and produce a concise morning briefing.

## Data
{data_json}

## Instructions
1. Start with a one-line headline summarizing the day.
2. List top 3 accomplishments or patterns observed.
3. Identify 1-2 concerns (late nights, context thrashing, low output).
4. Suggest 2-3 concrete actions for today.
5. End with a motivational insight based on the data.

## Format
Use markdown with headers, bullet points, and bold emphasis.
Keep the briefing under 300 words. Be direct and data-driven.
"""


def generate_ai_briefing(data: dict) -> str | None:
    """Use Gemini to produce a narrative briefing.

    Returns None if API key not set or call fails.
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None

    try:
        from google import genai
        from google.genai import types
        from time_utils import DEFAULT_MODEL_ID

        client = genai.Client(api_key=api_key)

        prompt = BRIEFING_PROMPT.format(data_json=json.dumps(data, indent=2))

        response = client.models.generate_content(
            model=DEFAULT_MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.3,
            ),
        )
        return response.text
    except Exception as e:
        print(f"  ⚠️  AI briefing failed: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# 3. Data-only briefing (no AI needed)
# ---------------------------------------------------------------------------

def format_data_briefing(data: dict) -> str:
    """Format a structured briefing from raw data (no AI)."""
    lines: list[str] = []
    date = data["date"]

    lines.append(f"# 🌅 ME-OPS Morning Briefing — {date}")
    lines.append("")

    # Stats
    stats = data["sections"].get("daily_stats", {})
    lines.append("## 📊 Daily Stats")
    lines.append(f"- **Events:** {stats.get('events', 0)}")
    lines.append(f"- **Unique actions:** {stats.get('actions', 0)}")
    lines.append(f"- **Active window:** {stats.get('first_active', '—')} → {stats.get('last_active', '—')}")

    # Sessions
    sessions = data["sections"].get("sessions", [])
    if sessions:
        total_min = sum(s.get("duration_min", 0) or 0 for s in sessions)
        lines.append(f"- **Sessions:** {len(sessions)} ({total_min / 60:.1f} hours)")

    # Projects
    projects = data["sections"].get("projects", [])
    if projects:
        lines.append("")
        lines.append("## 🏗️ Projects")
        for p in projects:
            lines.append(f"- **{p['name']}**: {p['events']} events")

    # Context switches
    switches = data["sections"].get("context_switches", [])
    if switches:
        lines.append(f"\n*⚠️ {len(switches)} context switches detected.*")

    # Clusters
    clusters = data["sections"].get("clusters", [])
    if clusters:
        lines.append("")
        lines.append("## 🧠 Behavioral Modes")
        for c in clusters:
            lines.append(f"- {c['mode']}: {c['count']} session(s)")

    # Warnings
    warnings = data["sections"].get("warnings", [])
    if warnings:
        lines.append("")
        lines.append("## ⚠️ Active Warnings")
        for w in warnings:
            icon = {"high": "🔴", "medium": "🟡"}.get(w["severity"], "🟢")
            lines.append(f"- {icon} **{w['type']}**: {w['desc']}")

    # Trend
    trend = data["sections"].get("trend", [])
    if trend:
        lines.append("")
        lines.append("## 📈 7-Day Trend")
        lines.append("| Date | Events |")
        lines.append("|------|--------|")
        for t in trend:
            lines.append(f"| {t['day']} | {t['events']} |")

    lines.append("")
    lines.append(f"*Generated: {datetime.now(CST).isoformat()}*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
def run(
    db_path: Path,
    *,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    target_date: Optional[str] = None,
    no_ai: bool = False,
    save: bool = True,
) -> dict:
    """Run briefing pipeline supporting shared connection."""
    close_con = False
    if con is None:
        con = duckdb.connect(str(db_path), read_only=True)
        close_con = True

    if target_date is None:
        target_date = datetime.now(CST).strftime("%Y-%m-%d")

    try:
        data = collect_briefing_data(con, target_date)
    finally:
        if close_con:
            con.close()

    # Data-only briefing (always generated)
    data_briefing = format_data_briefing(data)

    # AI-enhanced briefing (optional)
    ai_briefing = None
    if not no_ai:
        print("  Generating AI briefing...")
        ai_briefing = generate_ai_briefing(data)

    # Combine
    if ai_briefing:
        final = ai_briefing + "\n\n---\n\n" + data_briefing
    else:
        final = data_briefing

    # Save
    out_file = None
    if save:
        OUTPUT_DIR.mkdir(exist_ok=True)
        out_file = OUTPUT_DIR / f"briefing_{target_date}.md"
        out_file.write_text(final, encoding="utf-8")

    return {"report": final, "path": out_file, "data": data}


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="ME-OPS Morning Briefing")
    parser.add_argument("--date", type=str, default=None,
                        help="Target date YYYY-MM-DD (default: today)")
    parser.add_argument("--no-ai", action="store_true",
                        help="Skip AI-generated narrative")
    parser.add_argument("--save", action="store_true", default=True,
                        help="Save briefing to output/ (default: True)")
    args = parser.parse_args()

    target_date = args.date or datetime.now(CST).strftime("%Y-%m-%d")

    print(f"ME-OPS Morning Briefing — {target_date}")
    print("=" * 60)

    result = run(DB_PATH, target_date=target_date, no_ai=args.no_ai, save=args.save)
    print(result["report"])

    if result["path"]:
        print(f"\n{'=' * 60}")
        print(f"✅ Briefing saved to: {result['path']}")


if __name__ == "__main__":
    main()
