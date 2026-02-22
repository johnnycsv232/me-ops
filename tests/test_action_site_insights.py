from __future__ import annotations
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from pathlib import Path

import duckdb

import action_site_insights


def test_infer_site_prefers_url_then_keywords() -> None:
    assert (
        action_site_insights.infer_site("https://www.notion.so/Command-Center", "")
        == "notion.so"
    )
    assert (
        action_site_insights.infer_site(
            "Review integration checklist", '{"notes":"slack auth pending"}'
        )
        == "slack"
    )
    assert action_site_insights.infer_site("/repo/src/main.py", "") == "filesystem"


def test_run_generates_action_site_report(tmp_path: Path) -> None:
    db_path = tmp_path / "events.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE events (
                event_id VARCHAR,
                ts_start TIMESTAMP,
                action VARCHAR,
                target VARCHAR,
                metadata_json JSON,
                app_tool VARCHAR
            );
            """
        )
        con.execute(
            """
            INSERT INTO events VALUES
            ('e1', TIMESTAMP '2026-02-20 10:00:00', 'web_visit', 'https://www.notion.so/Command-Center', '{}', 'OS_SERVER'),
            ('e2', TIMESTAMP '2026-02-20 10:01:00', 'hint_suggested_query', 'Which notion page needs updates?', '{}', 'OS_SERVER'),
            ('e3', TIMESTAMP '2026-02-20 10:02:00', 'file_reference', '/repo/src/main.py', '{}', 'VS_CODE'),
            ('e4', TIMESTAMP '2026-02-20 10:03:00', 'annotation_summary', 'Stripe migration decisions', '{}', 'OS_SERVER'),
            ('e5', TIMESTAMP '2026-02-20 10:04:00', 'hint_suggested_query', 'What should I do next?', '{}', 'OS_SERVER'),
            ('e6', TIMESTAMP '2026-02-20 10:05:00', 'activity', '', '{}', 'OS_SERVER'),
            ('e7', TIMESTAMP '2026-02-20 10:06:00', 'time_range', '', '{}', 'OS_SERVER');
            """
        )

        payload = action_site_insights.run(
            db_path=db_path,
            con=con,
            output_dir=tmp_path,
            report_date="2026-02-20",
        )
    finally:
        con.close()

    report_path = tmp_path / "action_site_insights_2026-02-20.md"
    assert report_path.exists()
    report_text = report_path.read_text(encoding="utf-8")
    assert "Action x Site Insights" in report_text
    assert "Site -> Top 5 Actions" in report_text
    assert "## Current Problems" in report_text

    assert payload["totals"]["events"] == 7
    assert payload["top_actions"][0][0] in {"web_visit", "hint_suggested_query", "file_reference", "annotation_summary"}
    sites = [site for site, _ in payload["top_sites"]]
    assert "notion.so" in sites
    assert "current_problems" in payload
    assert isinstance(payload["current_problems"], list)
    assert payload["current_problems"]
