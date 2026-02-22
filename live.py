#!/usr/bin/env python3
"""ME-OPS Live Dashboard — real-time awareness of your work patterns.

Polls the DuckDB database and renders a terminal dashboard with:
- Current session status (duration, events, project)
- Today's running totals vs 7-day average
- Active behavioral cluster assignment
- Alert bar for anti-playbook violations

Skills used: workflow-patterns (polling loop + state management),
             production-code-audit (error handling, graceful shutdown),
             ai-engineer (real-time inference on session data)

Ref: https://docs.python.org/3/library/curses.html (official)
     https://rich.readthedocs.io/en/stable/live.html (Rich Live)

Usage:
    python live.py                 # Start dashboard (polls every 10s)
    python live.py --interval 5    # Custom poll interval
    python live.py --once          # Render once and exit
"""
from __future__ import annotations

import signal
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parent / "me_ops.duckdb"

# Timezone offset for CST (UTC-6)
CST = timezone(timedelta(hours=-6))


# ---------------------------------------------------------------------------
# Dashboard data collection
# ---------------------------------------------------------------------------

def _connect() -> duckdb.DuckDBPyConnection:
    """Read-only DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Check if a table exists in the database."""
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(result and result[0] > 0)


def get_today_stats(con: duckdb.DuckDBPyConnection) -> dict:
    """Get today's running totals."""
    today = datetime.now(CST).strftime("%Y-%m-%d")
    row = con.execute("""
        SELECT
            COUNT(*) AS events,
            COUNT(DISTINCT action) AS unique_actions,
            MIN(ts_start)::TIME AS first_event,
            MAX(ts_start)::TIME AS last_event,
            COUNT(DISTINCT source_file) AS sources
        FROM events
        WHERE ts_start::DATE = CAST(? AS DATE)
    """, [today]).fetchone()

    sessions_count = 0
    total_min = 0.0
    if _table_exists(con, "sessions"):
        srow = con.execute("""
            SELECT COUNT(*), COALESCE(SUM(duration_min), 0)
            FROM sessions
            WHERE ts_start::DATE = CAST(? AS DATE)
        """, [today]).fetchone()
        if srow:
            sessions_count, total_min = srow[0], srow[1]

    return {
        "date": today,
        "events": row[0] if row else 0,
        "unique_actions": row[1] if row else 0,
        "first_event": str(row[2]) if row and row[2] else "—",
        "last_event": str(row[3]) if row and row[3] else "—",
        "sources": row[4] if row else 0,
        "sessions": sessions_count,
        "total_hours": round(total_min / 60, 1),
    }


def get_7day_avg(con: duckdb.DuckDBPyConnection) -> dict:
    """Get 7-day daily averages for comparison."""
    row = con.execute("""
        SELECT
            AVG(day_events) AS avg_events,
            AVG(day_actions) AS avg_actions,
            AVG(day_sessions) AS avg_sessions
        FROM (
            SELECT
                ts_start::DATE AS day,
                COUNT(*) AS day_events,
                COUNT(DISTINCT action) AS day_actions,
                0 AS day_sessions
            FROM events
            WHERE ts_start IS NOT NULL
              AND ts_start::DATE >= CURRENT_DATE - INTERVAL '7 days'
              AND ts_start::DATE < CURRENT_DATE
            GROUP BY day
        ) daily
    """).fetchone()

    return {
        "avg_events": round(row[0], 0) if row and row[0] else 0,
        "avg_actions": round(row[1], 0) if row and row[1] else 0,
        "avg_sessions": round(row[2], 0) if row and row[2] else 0,
    }


def get_active_cluster(con: duckdb.DuckDBPyConnection) -> str | None:
    """Get the most recent session's cluster assignment."""
    if not _table_exists(con, "session_clusters"):
        return None
    row = con.execute("""
        SELECT sc.cluster_name
        FROM session_clusters sc
        JOIN sessions s ON sc.session_id = s.session_id
        ORDER BY s.ts_start DESC LIMIT 1
    """).fetchone()
    return row[0] if row else None


def get_active_warnings(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Get any active failure pattern warnings."""
    warnings: list[str] = []
    if _table_exists(con, "failure_patterns"):
        rows = con.execute("""
            SELECT severity, pattern_type, description
            FROM failure_patterns
            WHERE severity IN ('high', 'medium')
            ORDER BY CASE severity WHEN 'high' THEN 1 ELSE 2 END
            LIMIT 3
        """).fetchall()
        for r in rows:
            icon = "🔴" if r[0] == "high" else "🟡"
            warnings.append(f"{icon} {r[1]}: {r[2]}")
    return warnings


def get_top_projects_today(con: duckdb.DuckDBPyConnection) -> list[tuple[str, int]]:
    """Get today's top projects by event count."""
    today = datetime.now(CST).strftime("%Y-%m-%d")
    rows = con.execute("""
        SELECT p.name, COUNT(ep.event_id) AS events
        FROM event_projects ep
        JOIN projects p ON ep.project_id = p.project_id
        JOIN events e ON ep.event_id = e.event_id
        WHERE e.ts_start::DATE = CAST(? AS DATE)
        GROUP BY p.name
        ORDER BY events DESC
        LIMIT 5
    """, [today]).fetchall()
    return [(r[0], r[1]) for r in rows]


# ---------------------------------------------------------------------------
# Terminal renderer
# ---------------------------------------------------------------------------

def render_dashboard(once: bool = False) -> None:
    """Render dashboard to terminal."""
    con = _connect()

    now = datetime.now(CST)
    today = get_today_stats(con)
    avg = get_7day_avg(con)
    cluster = get_active_cluster(con)
    warnings = get_active_warnings(con)
    projects = get_top_projects_today(con)

    con.close()

    # Build output
    lines: list[str] = []
    lines.append("")
    lines.append("═" * 60)
    lines.append(f"  ME-OPS LIVE DASHBOARD  │  {now.strftime('%Y-%m-%d %H:%M:%S CST')}")
    lines.append("═" * 60)

    # Today vs average
    lines.append("")
    lines.append("  📊 TODAY vs 7-DAY AVG")
    lines.append("  ─────────────────────────────────────────")

    ev = today["events"]
    avg_ev = avg["avg_events"]
    delta = ((ev / avg_ev - 1) * 100) if avg_ev > 0 else 0
    arrow = "▲" if delta > 0 else "▼" if delta < 0 else "─"
    lines.append(f"    Events:      {ev:>6}  │  avg {avg_ev:>5.0f}  {arrow} {abs(delta):>5.1f}%")

    act = today["unique_actions"]
    avg_act = avg["avg_actions"]
    lines.append(f"    Actions:     {act:>6}  │  avg {avg_act:>5.0f}")

    lines.append(f"    Sessions:    {today['sessions']:>6}  │  {today['total_hours']:>4.1f} hours")
    lines.append(f"    Active:      {today['first_event']}  →  {today['last_event']}")

    # Projects
    if projects:
        lines.append("")
        lines.append("  🏗️  PROJECTS TODAY")
        lines.append("  ─────────────────────────────────────────")
        for name, count in projects:
            bar = "█" * min(count // 5, 30)
            lines.append(f"    {name:<20s} {count:>4}  {bar}")

    # Cluster
    if cluster:
        lines.append("")
        lines.append(f"  🧠 Current mode: {cluster}")

    # Warnings
    if warnings:
        lines.append("")
        lines.append("  ⚠️  ALERTS")
        lines.append("  ─────────────────────────────────────────")
        for w in warnings:
            lines.append(f"    {w}")

    lines.append("")
    lines.append("═" * 60)

    output = "\n".join(lines)

    if once:
        print(output)
    else:
        # Clear screen and render
        print("\033[2J\033[H", end="")
        print(output)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="ME-OPS Live Dashboard")
    parser.add_argument("--interval", type=int, default=10,
                        help="Poll interval in seconds (default: 10)")
    parser.add_argument("--once", action="store_true",
                        help="Render once and exit")
    args = parser.parse_args()

    if args.once:
        render_dashboard(once=True)
        return

    # Graceful shutdown on Ctrl+C
    running = True

    def handle_signal(sig: int, frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print(f"ME-OPS Live Dashboard — polling every {args.interval}s (Ctrl+C to stop)")

    while running:
        try:
            render_dashboard(once=False)
            time.sleep(args.interval)
        except Exception as e:
            print(f"\n  ERROR: {e}")
            time.sleep(args.interval)

    print("\n\nDashboard stopped.")


if __name__ == "__main__":
    main()
