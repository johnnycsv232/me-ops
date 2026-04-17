"""
Phase 2 — Sessionizer.
Groups events into sessions by time proximity and computes focus/load scores.
"""
import json
import sys
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

SESSION_GAP_MINUTES = 30
MAX_SESSION_HOURS = 4


def build_sessions(conn) -> int:
    """Group events into sessions using timeline order instead of project buckets."""
    events = conn.execute("""
        SELECT id, project_id, created_at, data
        FROM entities
        WHERE type='event'
        ORDER BY created_at ASC
    """).fetchall()

    sessions_created = 0
    current_session: dict | None = None
    session_events: list = []

    def _flush_session(sess: dict | None, evts: list) -> None:
        nonlocal sessions_created
        if not sess or not evts:
            return

        tool_counter: Counter[str] = Counter()
        project_sequence: list[str] = []
        for event in evts:
            data = json.loads(event["data"] or "{}")
            for tool in data.get("tools", []):
                tool_counter[tool] += 1
            if event["project_id"]:
                project_sequence.append(event["project_id"])

        project_ids = list(dict.fromkeys(project_sequence))
        project_switches = sum(
            1 for a, b in zip(project_sequence, project_sequence[1:]) if a != b
        )
        distinct_tools = len(tool_counter)
        tool_switches = max(0, distinct_tools - 1) + project_switches
        fragmentation = min(
            1.0,
            (distinct_tools / 6.0) + (project_switches / 4.0) + (max(0, len(evts) - 6) / 24.0),
        )
        context_load = min(
            1.0,
            (len(project_ids) * 0.22) + (project_switches * 0.12) + (fragmentation * 0.42),
        )
        primary_project = Counter(project_sequence).most_common(1)[0][0] if project_sequence else None
        primary_tool = tool_counter.most_common(1)[0][0] if tool_counter else None

        conn.execute("""
            INSERT OR IGNORE INTO sessions
              (id, project_id, started_at, ended_at, summary,
               tool_switch_count, fragmentation_score, context_load_score,
               primary_app, tags)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            sess["id"],
            primary_project,
            sess["started_at"],
            sess["ended_at"],
            f"{len(evts)} events, {len(project_ids)} projects, {distinct_tools} tools",
            tool_switches,
            round(fragmentation, 3),
            round(context_load, 3),
            primary_tool,
            json.dumps(project_ids),
        ))

        for event in evts:
            conn.execute("UPDATE entities SET session_id=? WHERE id=?", (sess["id"], event["id"]))
        sessions_created += 1

    for row in events:
        created_at = _parse_dt(row["created_at"])
        if current_session is None:
            current_session = _new_session(row)
            session_events = [row]
            continue

        last_dt = _parse_dt(current_session["ended_at"])
        started_dt = _parse_dt(current_session["started_at"])
        gap_minutes = (created_at - last_dt).total_seconds() / 60
        session_age_hours = (created_at - started_dt).total_seconds() / 3600

        if gap_minutes > SESSION_GAP_MINUTES or session_age_hours > MAX_SESSION_HOURS:
            _flush_session(current_session, session_events)
            current_session = _new_session(row)
            session_events = [row]
            continue

        current_session["ended_at"] = row["created_at"]
        session_events.append(row)

    _flush_session(current_session, session_events)
    conn.commit()
    return sessions_created


def _new_session(row) -> dict:
    return {
        "id": "sess_" + uuid.uuid4().hex[:8],
        "started_at": row["created_at"],
        "ended_at": row["created_at"],
    }


def _parse_dt(value: str | None) -> datetime:
    if not value:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return datetime.utcnow()
