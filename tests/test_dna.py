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
    assert "Unique Style" in report.read_text(encoding="utf-8")
    assert len(payload["premium_workflows"]) == 3
