from __future__ import annotations
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import duckdb

import architect
import master
import workflow_dna


def _seed_minimal_inputs(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE events (
            ts_start TIMESTAMP,
            action VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO events VALUES
        ('2026-02-21 10:00:00', 'web_visit'),
        ('2026-02-21 10:05:00', 'hint_suggested_query'),
        ('2026-02-21 23:40:00', 'web_visit')
        """
    )

    con.execute(
        """
        CREATE TABLE sessions (
            session_id INTEGER,
            duration_min DOUBLE,
            projects VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO sessions VALUES
        (1, 120.0, 'A, B, C, D'),
        (2, 45.0, 'A, B')
        """
    )

    con.execute(
        """
        CREATE TABLE event_subcategories (
            theme VARCHAR,
            subcategory VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO event_subcategories VALUES
        ('IronClad', 'Offer'),
        ('IronClad', 'Audience'),
        ('AI Arbitrage', 'Prompts')
        """
    )


def test_generate_coaching_rules_without_context_switches_table() -> None:
    con = duckdb.connect(":memory:")
    try:
        _seed_minimal_inputs(con)

        rules = architect.generate_coaching_rules(con)

        context_rule = next(
            rule for rule in rules if "Limit project switches to 3 per session" in rule["rule_text"]
        )
        assert context_rule["evidence_count"] == 1
        assert "sessions" in context_rule["evidence_sql"]
    finally:
        con.close()


def test_workflow_dna_report_renders_current_problems() -> None:
    payload = {
        "coverage": {
            "event_count": 1,
            "active_days": 1,
            "window_start": "2026-02-20",
            "window_end": "2026-02-20",
        },
        "style_profile": {
            "unique_style": ["Notion-First Planning"],
            "metrics": {
                "notion_first_ratio": 0.5,
                "closure_ratio": 0.5,
                "query_ladder_ratio": 0.5,
            },
            "orchestration_signature": "notion -> web",
            "prompt_fingerprint": {
                "query_count": 1.0,
                "question_ratio": 0.5,
                "evidence_seeking_ratio": 0.5,
                "systems_language_ratio": 0.5,
            },
        },
        "genetic_markers": [],
        "web_transition_breakdowns": [],
        "bottlenecks": [],
        "current_problems": [
            {
                "name": "Browser Drift",
                "severity": "medium",
                "impact_score": 42.0,
                "evidence": "3 drift runs",
                "recommendation": "Force synthesis after 3 web hops",
                "sample_count": 3,
                "activity_breakdown": [
                    {
                        "destination": "news.example.com",
                        "count": 3,
                        "share_pct": 100.0,
                        "top_intent": "page_visit",
                        "top_next_step": "web_visit (web)",
                        "avg_next_gap_sec": 4.0,
                        "top_app_tool": "OS_SERVER",
                        "sample_target": "https://news.example.com",
                    }
                ],
            }
        ],
        "premium_workflows": [],
    }

    report = workflow_dna.generate_workflow_dna_report(payload)

    assert "## Current Problems (Detailed)" in report
    assert "### Browser Drift (medium | impact 42.0)" in report


def test_master_current_problems_normalization_filters_invalid_rows() -> None:
    raw_payload = [
        {"name": "Browser Drift"},
        "invalid-row",
        {"name": "Query Loop Fatigue"},
        123,
    ]

    normalized = master._normalize_current_problems(raw_payload, max_items=3)

    assert normalized == [{"name": "Browser Drift"}, {"name": "Query Loop Fatigue"}]


def test_master_current_problems_snapshot_includes_hotspot() -> None:
    lines: list[str] = []
    problems = [
        {
            "name": "Browser Drift",
            "severity": "high",
            "impact_score": 99.9,
            "evidence": "many drift episodes",
            "recommendation": "force synthesis",
            "activity_breakdown": [
                {"destination": "unknown", "top_next_step": "web_visit (web)"}
            ],
        }
    ]

    master._append_current_problems_snapshot(
        report_lines=lines,
        problems=problems,
        heading="### Current Problems",
        include_hotspot=True,
    )

    snapshot = "\n".join(lines)
    assert "### Current Problems" in snapshot
    assert "Browser Drift (high, impact 99.9)" in snapshot
    assert "Hotspot: unknown -> web_visit (web)" in snapshot
