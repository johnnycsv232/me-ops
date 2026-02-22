from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import workflow_dna


def _build_signal_rich_events() -> list[dict[str, object]]:
    start = datetime(2026, 2, 20, 14, 0, tzinfo=timezone.utc)
    events: list[dict[str, object]] = []

    # Repeat a signature chain to establish strong DNA markers.
    for i in range(6):
        base = start + timedelta(minutes=i * 15)
        events.extend(
            [
                {
                    "event_id": f"n{i}",
                    "ts_start": base,
                    "action": "web_visit",
                    "app_tool": "OS_SERVER",
                    "target": "https://www.notion.so/Command-Center",
                },
                {
                    "event_id": f"w{i}",
                    "ts_start": base + timedelta(seconds=10),
                    "action": "web_visit",
                    "app_tool": "OS_SERVER",
                    "target": "https://docs.pieces.app/prod/cli",
                },
                {
                    "event_id": f"q{i}",
                    "ts_start": base + timedelta(seconds=20),
                    "action": "hint_suggested_query",
                    "app_tool": "OS_SERVER",
                    "target": "Which logs validate this workflow change?",
                },
                {
                    "event_id": f"t{i}",
                    "ts_start": base + timedelta(seconds=30),
                    "action": "time_range",
                    "app_tool": "OS_SERVER",
                    "target": "2026-02-20T14:00:00Z → 2026-02-20T14:10:00Z",
                },
                {
                    "event_id": f"s{i}",
                    "ts_start": base + timedelta(seconds=40),
                    "action": "workstream_summary",
                    "app_tool": "OS_SERVER",
                    "target": "summary:abc",
                },
                {
                    "event_id": f"a{i}",
                    "ts_start": base + timedelta(seconds=50),
                    "action": "annotation_summary",
                    "app_tool": "OS_SERVER",
                    "target": "### Core Tasks",
                },
                {
                    "event_id": f"d{i}",
                    "ts_start": base + timedelta(seconds=60),
                    "action": "annotation_description",
                    "app_tool": "OS_SERVER",
                    "target": "Finished the implementation and documented evidence.",
                },
                {
                    "event_id": f"f{i}",
                    "ts_start": base + timedelta(seconds=70),
                    "action": "file_reference",
                    "app_tool": "VS_CODE",
                    "target": "/repo/src/workflow_dna.py",
                },
            ]
        )

    # Add drift noise to ensure bottleneck detection has something to score.
    drift_base = start + timedelta(hours=3)
    for j in range(5):
        events.append(
            {
                "event_id": f"drift{j}",
                "ts_start": drift_base + timedelta(seconds=j * 5),
                "action": "web_visit",
                "app_tool": "OS_SERVER",
                "target": "https://random-site.example",
            }
        )

    return events


def test_extract_genetic_markers_finds_signature_transitions() -> None:
    events = _build_signal_rich_events()

    markers = workflow_dna.extract_genetic_markers(events, top_n=8)
    transitions = {m["transition"] for m in markers}

    assert "notion -> web" in transitions
    assert "query -> framing" in transitions
    assert any(m["strength"] > 0 for m in markers)


def test_profile_creation_style_detects_notion_first_and_zero_drift() -> None:
    events = _build_signal_rich_events()
    markers = workflow_dna.extract_genetic_markers(events, top_n=8)

    profile = workflow_dna.profile_creation_style(events, markers)
    unique_style = set(profile["unique_style"])

    assert "Notion-First Planning" in unique_style
    assert "Zero-Drift Orchestration" in unique_style
    assert profile["prompt_fingerprint"]["evidence_seeking_ratio"] > 0


def test_detect_bottlenecks_flags_browser_drift() -> None:
    events = _build_signal_rich_events()

    bottlenecks = workflow_dna.detect_bottlenecks(events)

    assert any(item["name"] == "Browser Drift" for item in bottlenecks)
    assert all(item["impact_score"] >= 0 for item in bottlenecks)


def test_generate_premium_workflows_returns_exactly_three() -> None:
    events = _build_signal_rich_events()
    markers = workflow_dna.extract_genetic_markers(events, top_n=8)
    profile = workflow_dna.profile_creation_style(events, markers)
    bottlenecks = workflow_dna.detect_bottlenecks(events)

    workflows = workflow_dna.generate_premium_workflows(profile, markers, bottlenecks)

    assert len(workflows) == 3
    assert all("name" in wf and "steps" in wf and "kpi" in wf for wf in workflows)


def test_run_mock_writes_report_and_payload(tmp_path: Path) -> None:
    db_path = tmp_path / "dna.duckdb"

    payload = workflow_dna.run(db_path=db_path, mock=True, output_dir=tmp_path)

    report = tmp_path / "WORKFLOW_DNA_REPORT.md"
    assert report.exists()
    report_text = report.read_text(encoding="utf-8")
    assert "Unique Style" in report_text
    assert "Current Problems (Detailed)" in report_text
    assert len(payload["premium_workflows"]) == 3
    assert "current_problems" in payload


def _build_synthesis_to_web_events() -> list[dict[str, object]]:
    start = datetime(2026, 2, 21, 9, 0, tzinfo=timezone.utc)
    events: list[dict[str, object]] = []
    sites = [
        "https://gitlab.com/example/repo/issues/1",
        "https://docs.python.org/3/library/pathlib.html",
        "https://stackoverflow.com/questions/123",
        "https://docs.python.org/3/library/typing.html",
        "https://linear.app/acme/issue/ME-101",
    ]

    for idx, url in enumerate(sites):
        base = start + timedelta(minutes=idx)
        events.extend(
            [
                {
                    "event_id": f"s{idx}",
                    "ts_start": base,
                    "action": "annotation_summary",
                    "app_tool": "OS_SERVER",
                    "target": f"synthesis note {idx}",
                    "metadata_text": "",
                },
                {
                    "event_id": f"w{idx}",
                    "ts_start": base + timedelta(seconds=8),
                    "action": "web_visit",
                    "app_tool": "OS_SERVER",
                    "target": url,
                    "metadata_text": "",
                },
            ]
        )

    return events


def test_extract_web_transition_breakdowns_surfaces_sites() -> None:
    events = _build_synthesis_to_web_events()
    markers = [
        {
            "name": "Synthesis to Web Relay",
            "transition": "synthesis -> web",
            "frequency": 5,
            "support_days": 1,
            "avg_gap_sec": 8.0,
            "strength": 0.5,
        }
    ]

    breakdowns = workflow_dna.extract_web_transition_breakdowns(events, markers)

    assert len(breakdowns) == 1
    first = breakdowns[0]
    assert first["transition"] == "synthesis -> web"
    assert first["total_hops"] == 5
    destinations = first["destinations"]
    assert destinations[0]["destination"] == "docs.python.org"
    assert any(item["destination"] == "linear.app" for item in destinations)
    assert all("sample_target" in item for item in destinations)
    assert all("top_intent" in item for item in destinations)
    assert all("top_next_step" in item for item in destinations)


def test_build_current_problem_breakdowns_adds_activity_context() -> None:
    events = _build_signal_rich_events()
    bottlenecks = workflow_dna.detect_bottlenecks(events)

    problems = workflow_dna.build_current_problem_breakdowns(events, bottlenecks)

    assert problems
    browser = next((p for p in problems if p["name"] == "Browser Drift"), None)
    assert browser is not None
    assert browser["sample_count"] > 0
    assert browser["activity_breakdown"]
    row = browser["activity_breakdown"][0]
    assert "destination" in row
    assert "top_intent" in row
    assert "top_next_step" in row
