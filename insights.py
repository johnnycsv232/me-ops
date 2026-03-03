import hashlib
import json
import math
from datetime import datetime, timezone
from time_utils import local_now, local_date, LOCAL_TZ
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb


DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"

INSIGHT_MEMORY_DDL = """
CREATE TABLE IF NOT EXISTS insight_memory (
    insight_key      VARCHAR PRIMARY KEY,
    insight_type     VARCHAR NOT NULL,
    title            VARCHAR NOT NULL,
    first_seen       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    occurrences      INTEGER DEFAULT 1
);
"""


def _stable_key(insight_type: str, title: str, evidence_key: str) -> str:
    raw = f"{insight_type}|{title}|{evidence_key}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _table_has_column(
    con: duckdb.DuckDBPyConnection, table_name: str, column_name: str
) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and row[0])


def _ensure_memory_table(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute(INSIGHT_MEMORY_DDL)
        return True
    except Exception:
        return False


def _confidence_score(
    support_days: int,
    recent_support_days: int,
    uplift: float,
    low_with_rate: float,
    includes_generic: bool,
) -> float:
    support_component = min(0.30, support_days / 120.0)
    recency_component = min(0.20, recent_support_days / 40.0)
    uplift_component = min(0.30, max(0.0, uplift) / 3.0)
    risk_penalty = min(0.20, max(0.0, low_with_rate) * 0.6)
    generic_penalty = 0.08 if includes_generic else 0.0
    confidence = 0.28 + support_component + recency_component + uplift_component
    confidence -= risk_penalty + generic_penalty
    return round(max(0.20, min(0.95, confidence)), 2)


def _coverage_snapshot(con: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    events_count = 0
    active_days = 0
    signal_count = 0
    if _table_exists(con, "events"):
        row = con.execute(
            """
            SELECT
                COUNT(*) AS events_count,
                COUNT(DISTINCT ts_start::DATE) AS active_days
            FROM events
            WHERE ts_start IS NOT NULL
            """
        ).fetchone()
        if row:
            events_count = int(row[0] or 0)
            active_days = int(row[1] or 0)
    if _table_exists(con, "event_subcategories"):
        row = con.execute(
            """
            SELECT COUNT(DISTINCT theme || '::' || subcategory)
            FROM event_subcategories
            """
        ).fetchone()
        if row:
            signal_count = int(row[0] or 0)
    return {
        "events_count": events_count,
        "active_days": active_days,
        "signal_count": signal_count,
    }


def _pair_examples(
    con: duckdb.DuckDBPyConnection, signal_a: str, signal_b: str
) -> Dict[str, Any]:
    target_expr = "COALESCE(SUBSTR(e.target, 1, 100), '')"
    if not _table_has_column(con, "events", "target"):
        target_expr = "''"

    rows = con.execute(
        f"""
        WITH daily_signal AS (
            SELECT DISTINCT
                e.ts_start::DATE AS day_key,
                es.theme || '::' || es.subcategory AS signal
            FROM events e
            JOIN event_subcategories es ON e.event_id = es.event_id
            WHERE e.ts_start IS NOT NULL
        ),
        pair_days AS (
            SELECT a.day_key
            FROM daily_signal a
            JOIN daily_signal b
              ON a.day_key = b.day_key
             AND a.signal = ?
             AND b.signal = ?
        )
        SELECT
            e.event_id,
            e.ts_start::DATE AS day_key,
            e.action,
            {target_expr} AS target_excerpt
        FROM events e
        JOIN event_subcategories es ON e.event_id = es.event_id
        WHERE e.ts_start::DATE IN (SELECT day_key FROM pair_days)
          AND (es.theme || '::' || es.subcategory) IN (?, ?)
          AND e.ts_start IS NOT NULL
        ORDER BY e.ts_start DESC
        LIMIT 8
        """,
        [signal_a, signal_b, signal_a, signal_b],
    ).fetchall()

    top_actions = con.execute(
        """
        WITH daily_signal AS (
            SELECT DISTINCT
                e.ts_start::DATE AS day_key,
                es.theme || '::' || es.subcategory AS signal
            FROM events e
            JOIN event_subcategories es ON e.event_id = es.event_id
            WHERE e.ts_start IS NOT NULL
        ),
        pair_days AS (
            SELECT a.day_key
            FROM daily_signal a
            JOIN daily_signal b
              ON a.day_key = b.day_key
             AND a.signal = ?
             AND b.signal = ?
        )
        SELECT e.action, COUNT(*) AS n
        FROM events e
        WHERE e.ts_start::DATE IN (SELECT day_key FROM pair_days)
        GROUP BY e.action
        ORDER BY n DESC
        LIMIT 5
        """,
        [signal_a, signal_b],
    ).fetchall()

    return {
        "sample_events": [
            {
                "event_id": str(event_id),
                "day": str(day_key),
                "action": action,
                "target_excerpt": target_excerpt,
            }
            for event_id, day_key, action, target_excerpt in rows
        ],
        "top_actions": [{"action": action, "count": int(n)} for action, n in top_actions],
    }


def _fetch_pair_uplifts(
    con: duckdb.DuckDBPyConnection, limit: int
) -> List[Dict[str, Any]]:
    if (
        not _table_exists(con, "daily_scores")
        or not _table_exists(con, "event_subcategories")
        or not _table_exists(con, "events")
        or not _table_has_column(con, "daily_scores", "composite_score")
    ):
        return []

    rows = con.execute(
        """
        WITH daily_signal AS (
            SELECT DISTINCT
                e.ts_start::DATE AS day_key,
                es.theme || '::' || es.subcategory AS signal
            FROM events e
            JOIN event_subcategories es ON e.event_id = es.event_id
            WHERE e.ts_start IS NOT NULL
        ),
        pair_days AS (
            SELECT
                a.signal AS signal_a,
                b.signal AS signal_b,
                a.day_key
            FROM daily_signal a
            JOIN daily_signal b
              ON a.day_key = b.day_key
             AND a.signal < b.signal
        ),
        pair_keys AS (
            SELECT DISTINCT signal_a, signal_b
            FROM pair_days
        ),
        ds AS (
            SELECT date AS day_key, composite_score
            FROM daily_scores
        ),
        stats AS (
            SELECT
                pk.signal_a,
                pk.signal_b,
                COUNT(CASE WHEN pd.day_key IS NOT NULL THEN 1 END) AS support_days,
                COUNT(
                    CASE
                        WHEN pd.day_key IS NOT NULL
                         AND ds.day_key >= CURRENT_DATE - INTERVAL '30 days'
                        THEN 1
                    END
                ) AS recent_support_days,
                AVG(CASE WHEN pd.day_key IS NOT NULL THEN ds.composite_score END) AS avg_with_pair,
                AVG(CASE WHEN pd.day_key IS NULL THEN ds.composite_score END) AS avg_without_pair,
                AVG(
                    CASE
                        WHEN pd.day_key IS NOT NULL AND ds.composite_score < 4 THEN 1.0
                        WHEN pd.day_key IS NOT NULL THEN 0.0
                    END
                ) AS low_with_pair_rate,
                STDDEV_SAMP(CASE WHEN pd.day_key IS NOT NULL THEN ds.composite_score END) AS std_with_pair,
                STDDEV_SAMP(CASE WHEN pd.day_key IS NULL THEN ds.composite_score END) AS std_without_pair
            FROM pair_keys pk
            JOIN ds ON TRUE
            LEFT JOIN pair_days pd
              ON pd.signal_a = pk.signal_a
             AND pd.signal_b = pk.signal_b
             AND pd.day_key = ds.day_key
            GROUP BY pk.signal_a, pk.signal_b
        ),
        signal_stats AS (
            SELECT
                signal,
                AVG(composite_score) AS avg_when_present
            FROM (
                SELECT
                    ds.day_key,
                    ds.composite_score,
                    d.signal
                FROM ds
                JOIN daily_signal d ON d.day_key = ds.day_key
            ) t
            GROUP BY signal
        )
        SELECT
            s.signal_a,
            s.signal_b,
            s.support_days,
            s.recent_support_days,
            ROUND(s.avg_with_pair, 3) AS avg_with_pair,
            ROUND(s.avg_without_pair, 3) AS avg_without_pair,
            ROUND(COALESCE(sa.avg_when_present, s.avg_with_pair), 3) AS avg_a_present,
            ROUND(COALESCE(sb.avg_when_present, s.avg_with_pair), 3) AS avg_b_present,
            ROUND(COALESCE(s.low_with_pair_rate, 0.0), 3) AS low_with_pair_rate,
            ROUND(COALESCE(s.std_with_pair, 0.0), 3) AS std_with_pair,
            ROUND(COALESCE(s.std_without_pair, 0.0), 3) AS std_without_pair,
            ROUND(s.avg_with_pair - s.avg_without_pair, 3) AS uplift
        FROM stats s
        LEFT JOIN signal_stats sa ON sa.signal = s.signal_a
        LEFT JOIN signal_stats sb ON sb.signal = s.signal_b
        WHERE s.support_days >= 4
          AND s.avg_with_pair IS NOT NULL
          AND s.avg_without_pair IS NOT NULL
        ORDER BY uplift DESC, support_days DESC
        LIMIT ?
        """,
        [limit * 5],
    ).fetchall()

    candidates: List[Dict[str, Any]] = []
    for row in rows:
        (
            signal_a,
            signal_b,
            support_days,
            recent_support_days,
            avg_with,
            avg_without,
            avg_a,
            avg_b,
            low_rate,
            std_with,
            std_without,
            uplift,
        ) = row

        if uplift <= 0.35:
            continue

        synergy = float(avg_with) - max(float(avg_a), float(avg_b))
        includes_generic = (
            signal_a == "Miscellaneous::General" or signal_b == "Miscellaneous::General"
        )
        recency_ratio = (
            float(recent_support_days) / float(support_days) if support_days else 0.0
        )

        confidence = _confidence_score(
            support_days=int(support_days),
            recent_support_days=int(recent_support_days),
            uplift=float(uplift),
            low_with_rate=float(low_rate),
            includes_generic=includes_generic,
        )

        specificity_multiplier = 0.85 if includes_generic else 1.0
        synergy_multiplier = 1.0 if synergy >= 0 else 0.6
        downside_multiplier = max(0.35, 1.0 - float(low_rate))
        recency_multiplier = max(0.40, recency_ratio)
        impact_score = (
            float(uplift)
            * (1.0 + min(2.0, math.log1p(float(support_days)) / 2.0))
            * specificity_multiplier
            * synergy_multiplier
            * downside_multiplier
            * recency_multiplier
        )
        impact_score = round(impact_score, 2)

        counter_notes: List[str] = []
        if includes_generic:
            counter_notes.append(
                "One signal is Miscellaneous::General, so this may partly reflect unstructured work days."
            )
        if float(low_rate) > 0.20:
            counter_notes.append(
                f"Downside risk is non-trivial: {float(low_rate):.0%} of pair days still scored below 4.0."
            )
        if recency_ratio < 0.35:
            counter_notes.append(
                "Pattern is aging: less than 35% of observed pair days are in the last 30 days."
            )
        if synergy < 0:
            counter_notes.append(
                "Synergy is negative: each signal alone appears stronger than the pair."
            )
        if not counter_notes:
            counter_notes.append("No major statistical red flags detected at current thresholds.")

        candidates.append(
            {
                "type": "connection",
                "title": f"{signal_a} + {signal_b}: uplift {uplift:+.2f}",
                "insight": (
                    f"Pair days average {float(avg_with):.2f} vs {float(avg_without):.2f} when absent. "
                    f"Estimated synergy vs strongest single signal: {synergy:+.2f}."
                ),
                "evidence": {
                    "signal_a": signal_a,
                    "signal_b": signal_b,
                    "support_days": int(support_days),
                    "recent_support_days": int(recent_support_days),
                    "recency_ratio": round(recency_ratio, 2),
                    "avg_with_pair": float(avg_with),
                    "avg_without_pair": float(avg_without),
                    "avg_signal_a_present": float(avg_a),
                    "avg_signal_b_present": float(avg_b),
                    "uplift": float(uplift),
                    "synergy": round(float(synergy), 2),
                    "low_with_pair_rate": float(low_rate),
                    "std_with_pair": float(std_with),
                    "std_without_pair": float(std_without),
                },
                "counter_evidence": counter_notes,
                "next_action": (
                    f"Run a controlled block this week combining `{signal_a}` then `{signal_b}`, "
                    "and compare resulting composite score against your rolling 14-day baseline."
                ),
                "impact_score": impact_score,
                "confidence": confidence,
                "evidence_key": f"{signal_a}|{signal_b}",
            }
        )

    candidates.sort(
        key=lambda x: (
            x["impact_score"],
            x["confidence"],
            x["evidence"]["support_days"],
            x["evidence"]["recent_support_days"],
        ),
        reverse=True,
    )
    top = candidates[:limit]

    # Attach expensive example evidence only for the final ranked set.
    for item in top:
        signal_a = item["evidence"]["signal_a"]
        signal_b = item["evidence"]["signal_b"]
        item["evidence"]["examples"] = _pair_examples(
            con, signal_a=signal_a, signal_b=signal_b
        )

    return top


def _fetch_signal_drift(con: duckdb.DuckDBPyConnection, limit: int) -> List[Dict[str, Any]]:
    if not _table_exists(con, "event_subcategories") or not _table_exists(con, "events"):
        return []

    rows = con.execute(
        """
        WITH signal_days AS (
            SELECT
                e.ts_start::DATE AS day_key,
                es.theme || '::' || es.subcategory AS signal
            FROM events e
            JOIN event_subcategories es ON e.event_id = es.event_id
            WHERE e.ts_start IS NOT NULL
        ),
        signal_stats AS (
            SELECT
                signal,
                COUNT(*) AS total_events,
                MAX(day_key) AS last_seen,
                SUM(CASE WHEN day_key >= CURRENT_DATE - INTERVAL '30 days' THEN 1 ELSE 0 END) AS recent_30,
                SUM(
                    CASE
                        WHEN day_key >= CURRENT_DATE - INTERVAL '90 days'
                         AND day_key < CURRENT_DATE - INTERVAL '30 days'
                        THEN 1
                        ELSE 0
                    END
                ) AS prior_60
            FROM signal_days
            GROUP BY signal
        )
        SELECT
            signal,
            total_events,
            last_seen,
            recent_30,
            prior_60
        FROM signal_stats
        WHERE total_events >= 20
          AND prior_60 >= 8
          AND recent_30 <= prior_60 * 0.35
        ORDER BY (prior_60 - recent_30) DESC, total_events DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()

    insights: List[Dict[str, Any]] = []
    for signal, total_events, last_seen, recent_30, prior_60 in rows:
        drop_ratio = 1.0 - (
            float(recent_30) / float(prior_60) if float(prior_60) > 0 else 0.0
        )
        impact_score = round(
            min(8.0, (float(prior_60 - recent_30) / 5.0) + (float(total_events) / 300.0)),
            2,
        )
        confidence = round(min(0.90, 0.45 + min(0.35, float(prior_60) / 80.0)), 2)
        insights.append(
            {
                "type": "signal_drift",
                "title": f"Signal drift detected: {signal}",
                "insight": (
                    f"{signal} dropped from {int(prior_60)} hits in the prior 60-day window "
                    f"to {int(recent_30)} in the last 30 days ({drop_ratio:.0%} decline)."
                ),
                "evidence": {
                    "signal": signal,
                    "total_events": int(total_events),
                    "last_seen": str(last_seen),
                    "recent_30": int(recent_30),
                    "prior_60": int(prior_60),
                    "decline_ratio": round(drop_ratio, 2),
                },
                "counter_evidence": [
                    "Decline can be intentional deprioritization; confirm against roadmap before reactivating."
                ],
                "next_action": (
                    f"Decide explicitly: revive `{signal}` this week or archive it to remove mental overhead."
                ),
                "impact_score": impact_score,
                "confidence": confidence,
                "evidence_key": signal,
            }
        )
    return insights


def _attach_novelty(
    con: duckdb.DuckDBPyConnection,
    insights_list: List[Dict[str, Any]],
    persist_memory: bool,
) -> None:
    for item in insights_list:
        key = _stable_key(item["type"], item["title"], item["evidence_key"])
        item["insight_key"] = key

        if not persist_memory:
            item["novelty"] = "untracked"
            item["previous_occurrences"] = 0
            continue

        row = con.execute(
            "SELECT occurrences FROM insight_memory WHERE insight_key = ?",
            [key],
        ).fetchone()
        if row:
            con.execute(
                """
                UPDATE insight_memory
                SET occurrences = occurrences + 1,
                    last_seen = CURRENT_TIMESTAMP
                WHERE insight_key = ?
                """,
                [key],
            )
            item["novelty"] = "recurring"
            item["previous_occurrences"] = int(row[0])
        else:
            con.execute(
                """
                INSERT INTO insight_memory (insight_key, insight_type, title)
                VALUES (?, ?, ?)
                """,
                [key, item["type"], item["title"]],
            )
            item["novelty"] = "new"
            item["previous_occurrences"] = 0


def generate_evolving_insights(
    db_path: Path = DB_PATH,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    max_items: int = 12,
) -> Dict[str, Any]:
    if con is None and not db_path.exists():
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "coverage": {},
            "insights": [],
            "action_queue": [],
        }

    owns_connection = con is None
    if con is None:
        con = duckdb.connect(str(db_path))

    try:
        coverage = _coverage_snapshot(con)
        persist_memory = _ensure_memory_table(con)

        insights_list: List[Dict[str, Any]] = []
        insights_list.extend(_fetch_pair_uplifts(con, limit=max_items))
        insights_list.extend(_fetch_signal_drift(con, limit=max_items))

        insights_list.sort(
            key=lambda x: (
                x["impact_score"],
                x["confidence"],
                x["evidence"].get("support_days", 0),
                x["evidence"].get("recent_support_days", 0),
            ),
            reverse=True,
        )
        insights_list = insights_list[:max_items]
        _attach_novelty(con, insights_list, persist_memory=persist_memory)

        action_queue: List[Dict[str, Any]] = []
        for idx, item in enumerate(insights_list[:7], start=1):
            action_queue.append(
                {
                    "priority": idx,
                    "action": item["next_action"],
                    "source_title": item["title"],
                    "impact_score": item["impact_score"],
                    "confidence": item["confidence"],
                    "novelty": item["novelty"],
                }
            )

        summary = {
            "insight_count": len(insights_list),
            "new_count": sum(1 for x in insights_list if x["novelty"] == "new"),
            "recurring_count": sum(1 for x in insights_list if x["novelty"] == "recurring"),
            "high_impact_count": sum(1 for x in insights_list if x["impact_score"] >= 2.0),
        }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "memory_mode": "persistent" if persist_memory else "read_only",
            "coverage": coverage,
            "summary": summary,
            "insights": insights_list,
            "action_queue": action_queue,
        }
    finally:
        if owns_connection:
            con.close()


def render_evolving_insights_markdown(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Evolving Insights")
    lines.append(f"*Generated: {payload.get('generated_at', '')}*")
    lines.append("")

    coverage = payload.get("coverage", {})
    summary = payload.get("summary", {})
    lines.append("## Coverage")
    lines.append(f"- Events analyzed: {coverage.get('events_count', 0)}")
    lines.append(f"- Active days analyzed: {coverage.get('active_days', 0)}")
    lines.append(f"- Signals tracked: {coverage.get('signal_count', 0)}")
    lines.append("")

    lines.append("## Summary")
    lines.append(f"- Insights: {summary.get('insight_count', 0)}")
    lines.append(f"- New: {summary.get('new_count', 0)}")
    lines.append(f"- Recurring: {summary.get('recurring_count', 0)}")
    lines.append(f"- High impact: {summary.get('high_impact_count', 0)}")
    lines.append("")

    lines.append("## Action Queue")
    queue = payload.get("action_queue", [])
    if not queue:
        lines.append("1. No prioritized actions yet; ingest more data and rerun.")
    else:
        for item in queue:
            lines.append(
                f"{item['priority']}. {item['action']} "
                f"(impact: {item['impact_score']}, conf: {item['confidence']:.2f}, novelty: {item['novelty']})"
            )
    lines.append("")

    lines.append("## Insight Cards")
    for item in payload.get("insights", []):
        evidence = item.get("evidence", {})
        lines.append(f"### {item['title']}")
        lines.append(f"- Type: `{item['type']}`")
        lines.append(f"- Novelty: `{item.get('novelty', 'untracked')}`")
        lines.append(f"- Impact: `{item['impact_score']}`")
        lines.append(f"- Confidence: `{item['confidence']:.2f}`")
        lines.append(f"- Why it matters: {item['insight']}")
        if "support_days" in evidence:
            lines.append(
                f"- Evidence density: support={evidence.get('support_days', 0)} days, "
                f"recent={evidence.get('recent_support_days', 0)} days, "
                f"uplift={evidence.get('uplift', 0):+.2f}, synergy={evidence.get('synergy', 0):+.2f}"
            )
            lines.append(
                f"- Risk profile: low-score-rate={evidence.get('low_with_pair_rate', 0):.0%}, "
                f"std_with={evidence.get('std_with_pair', 0):.2f}, std_without={evidence.get('std_without_pair', 0):.2f}"
            )
        lines.append("- Counter-evidence:")
        for note in item.get("counter_evidence", []):
            lines.append(f"  - {note}")
        lines.append(f"- Next action: {item['next_action']}")
        lines.append("")

    return "\n".join(lines)


def get_insight_prompts(
    db_path: Path = DB_PATH,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Backward-compatible prompt surface for existing callers."""
    payload = generate_evolving_insights(db_path=db_path, con=con, max_items=8)
    queue = payload.get("action_queue", [])
    cards = payload.get("insights", [])

    return {
        "Priority Actions": [
            {
                "title": f"Priority {item['priority']}",
                "prompt": item["action"],
                "value": item["source_title"],
            }
            for item in queue
        ],
        "Gamechanging Connections": [
            {
                "title": card["title"],
                "prompt": card["insight"],
                "value": card["next_action"],
            }
            for card in cards
            if card.get("type") == "connection"
        ],
    }


if __name__ == "__main__":
    results = generate_evolving_insights()
    print(json.dumps(results, indent=2))
