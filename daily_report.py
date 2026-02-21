import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import duckdb


DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def generate_report(db_path: Path, report_date: Optional[str] = None) -> bool:
    """Generates a daily briefing combining insights from all modules.

    Analyzes events, projects, sessions, and patterns for a specific date 
    to produce a comprehensive markdown report.

    Args:
        db_path: Path to the DuckDB database.
        report_date: The date to report on (YYYY-MM-DD). If None, uses the latest activity.

    Returns:
        True if the report was generated successfully, False otherwise.
    """
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return False

    con = duckdb.connect(str(db_path), read_only=True)

    # Determine report date
    if report_date:
        target = report_date
    else:
        # Use most recent date with events
        row = con.execute("""
            SELECT MAX(ts_start::DATE)::VARCHAR FROM events
            WHERE ts_start IS NOT NULL
        """).fetchone()
        target = row[0] if row and row[0] else str(datetime.now(timezone.utc).date())

    print(f"ME-OPS Daily Report: {target}")
    print("=" * 60)

    lines: List[str] = [f"# ME-OPS Daily Report: {target}", ""]

    # --- Section 1: Daily Stats ---
    lines.append("## 📊 Daily Stats\n")

    stats = con.execute("""
        SELECT COUNT(*) AS events,
               COUNT(DISTINCT action) AS unique_actions,
               COUNT(DISTINCT app_tool) AS tools_used,
               MIN(ts_start)::TIME AS first_event,
               MAX(ts_start)::TIME AS last_event
        FROM events
        WHERE ts_start::DATE = ?
    """, [target]).fetchone()

    if stats and stats[0] > 0:
        lines.append(f"- **Events:** {stats[0]}")
        lines.append(f"- **Unique actions:** {stats[1]}")
        lines.append(f"- **Tools used:** {stats[2]}")
        lines.append(f"- **Active window:** {stats[3]} → {stats[4]}")
    else:
        lines.append("- No events on this date.")
        con.close()
        report = "\n".join(lines)
        print(report)
        return True

    # --- Section 2: Top Actions ---
    lines.extend(["", "## 🎯 Top Actions", ""])
    rows = con.execute("""
        SELECT action, COUNT(*) AS n FROM events
        WHERE ts_start::DATE = ?
        GROUP BY action ORDER BY n DESC LIMIT 8
    """, [target]).fetchall()
    for r in rows:
        lines.append(f"- `{r[0]}`: {r[1]}")

    # --- Section 3: Projects Touched ---
    lines.extend(["", "## 🏗️ Projects Touched", ""])
    rows = con.execute("""
        SELECT p.name, COUNT(ep.event_id) AS events
        FROM event_projects ep
        JOIN projects p ON ep.project_id = p.project_id
        JOIN events e ON ep.event_id = e.event_id
        WHERE e.ts_start::DATE = ?
        GROUP BY p.name ORDER BY events DESC
    """, [target]).fetchall()
    if rows:
        for r in rows:
            lines.append(f"- **{r[0]}**: {r[1]} events")
    else:
        lines.append("- No project references detected.")

    # --- Section 4: Sessions ---
    has_sessions = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sessions'"
    ).fetchone()[0]

    if has_sessions:
        lines.extend(["", "## ⏱️ Sessions", ""])
        rows = con.execute("""
            SELECT session_id, ts_start::TIME, ts_end::TIME,
                   duration_min, event_count, dominant_action, projects
            FROM sessions
            WHERE ts_start::DATE = ?
            ORDER BY ts_start
        """, [target]).fetchall()
        if rows:
            for r in rows:
                proj = f" [{r[6]}]" if r[6] else ""
                lines.append(
                    f"- **Session {r[0]}**: {r[1]}→{r[2]} ({r[3]}min, {r[4]} events, dominant: `{r[5]}`){proj}"
                )
        else:
            lines.append("- No sessions on this date.")

    # --- Section 5: Warnings ---
    has_patterns = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'failure_patterns'"
    ).fetchone()[0]

    if has_patterns:
        lines.extend(["", "## ⚠️ Active Warnings", ""])
        rows = con.execute("""
            SELECT severity, pattern_type, description
            FROM failure_patterns
            ORDER BY CASE severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END
        """).fetchall()
        if rows:
            for r in rows:
                icon = "🔴" if r[0] == "high" else "🟡" if r[0] == "medium" else "🟢"
                lines.append(f"- {icon} **{r[1]}**: {r[2]}")
        else:
            lines.append("- ✅ No active warnings.")

    # --- Section 6: Anti-Playbook ---
    has_playbook = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'anti_playbook'"
    ).fetchone()[0]

    if has_playbook:
        lines.extend(["", "## ⛔ Anti-Playbook Reminders", ""])
        rows = con.execute("""
            SELECT rule_text, confidence FROM anti_playbook
            ORDER BY confidence DESC
        """).fetchall()
        if rows:
            for r in rows:
                lines.append(f"- {r[0]} *(conf: {float(r[1]):.0%})*")

    # --- Section 7: 7-day trend ---
    lines.extend(["", "## 📈 7-Day Trend", "", "| Date | Events | Actions | Projects |", "|------|--------|---------|----------|"])
    rows = con.execute("""
        SELECT e.ts_start::DATE AS day,
               COUNT(*) AS events,
               COUNT(DISTINCT e.action) AS actions,
               COUNT(DISTINCT ep.project_id) AS projects
        FROM events e
        LEFT JOIN event_projects ep ON e.event_id = ep.event_id
        WHERE e.ts_start IS NOT NULL
        AND e.ts_start::DATE > CAST(? AS DATE) - INTERVAL '7 days'
        AND e.ts_start::DATE <= CAST(? AS DATE)
        GROUP BY day ORDER BY day DESC
    """, [target, target]).fetchall()
    for r in rows:
        lines.append(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} |")

    lines.extend(["", "---", f"*Generated: {datetime.now(timezone.utc).isoformat()}*"])

    report = "\n".join(lines)

    # Save to file
    OUTPUT_DIR.mkdir(exist_ok=True)
    report_file = OUTPUT_DIR / f"report_{target}.md"
    report_file.write_text(report, encoding="utf-8")

    # Also print
    print(report)
    print(f"\n{'='*60}")
    print(f"✅ Report saved to: {report_file}")

    con.close()
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ME-OPS Daily Report")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--date", type=str, default=None, help="Report date (YYYY-MM-DD). Default: latest.")
    args = parser.parse_args()
    sys.exit(0 if generate_report(args.db, args.date) else 1)
