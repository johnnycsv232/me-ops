#!/usr/bin/env python3
"""ME-OPS Action x Site Insights.

Builds action-by-site and site-by-action summaries from events data and writes
an insight report to output/action_site_insights_<date>.md.
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import duckdb


DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
SITE_HINTS = {
    "notion": "notion",
    "github": "github",
    "slack": "slack",
    "hubspot": "hubspot",
    "stripe": "stripe",
    "figma": "figma",
    "intercom": "intercom",
    "amplitude": "amplitude",
    "wiz": "wiz",
    "openai": "openai",
    "google": "google",
    "microsoft": "microsoft",
    "aws": "aws",
    "azure": "azure",
    "linear": "linear",
    "jira": "jira",
    "confluence": "confluence",
}
URL_RE = re.compile(r"https?://[^\s\])>'\"]+")
FILE_PATH_RE = re.compile(r"([A-Za-z]:\\\\|/)[^\s]+\.[A-Za-z0-9]{1,8}(?:$|\s)")


def infer_site(target: str, metadata_text: str = "") -> str:
    """Infer a site label from target + metadata payload text."""
    target_s = target or ""
    metadata_s = metadata_text or ""
    text = f"{target_s} {metadata_s}".lower()

    url_match = URL_RE.search(text)
    if url_match:
        try:
            host = urlparse(url_match.group(0)).netloc.lower().strip()
            if host.startswith("www."):
                host = host[4:]
            if host:
                return host
        except Exception:
            pass

    for token, label in SITE_HINTS.items():
        if token in text:
            return label

    if "localhost" in text or "127.0.0.1" in text:
        return "local"

    if FILE_PATH_RE.search(target_s):
        return "filesystem"

    return "unknown"


def _fetch_rows(con: duckdb.DuckDBPyConnection) -> List[Tuple[str, str, str, str]]:
    exists = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'events'
        """
    ).fetchone()
    if not exists or int(exists[0]) == 0:
        return []

    return con.execute(
        """
        SELECT
            CAST(ts_start::DATE AS VARCHAR) AS day,
            action,
            COALESCE(target, '') AS target,
            COALESCE(CAST(metadata_json AS VARCHAR), '') AS metadata_text
        FROM events
        WHERE ts_start IS NOT NULL
        """
    ).fetchall()


def _top_pairs(counter: Dict[Tuple[str, str], int], first_value: str) -> List[Tuple[str, int]]:
    pairs = [(second, count) for (first, second), count in counter.items() if first == first_value]
    pairs.sort(key=lambda item: item[1], reverse=True)
    return pairs


def _build_payload(rows: Iterable[Tuple[str, str, str, str]]) -> Dict[str, object]:
    action_site: Dict[Tuple[str, str], int] = defaultdict(int)
    site_action: Dict[Tuple[str, str], int] = defaultdict(int)
    action_total: Dict[str, int] = defaultdict(int)
    site_total: Dict[str, int] = defaultdict(int)
    site_days: Dict[str, set[str]] = defaultdict(set)

    count_rows = 0
    for day, action, target, metadata_text in rows:
        count_rows += 1
        site = infer_site(target, metadata_text)

        action_site[(action, site)] += 1
        site_action[(site, action)] += 1
        action_total[action] += 1
        site_total[site] += 1
        site_days[site].add(day)

    top_actions = sorted(action_total.items(), key=lambda item: item[1], reverse=True)
    top_sites = sorted(site_total.items(), key=lambda item: item[1], reverse=True)

    return {
        "totals": {
            "events": count_rows,
            "actions": len(action_total),
            "sites": len(site_total),
        },
        "action_site": dict(action_site),
        "site_action": dict(site_action),
        "action_total": dict(action_total),
        "site_total": dict(site_total),
        "site_days": {key: sorted(days) for key, days in site_days.items()},
        "top_actions": top_actions,
        "top_sites": top_sites,
    }


def _derive_current_problems(payload: Dict[str, object]) -> List[Dict[str, object]]:
    totals = payload.get("totals", {})
    action_total = payload.get("action_total", {})
    site_total = payload.get("site_total", {})
    action_site = payload.get("action_site", {})

    if not isinstance(totals, dict) or not isinstance(action_total, dict):
        return []
    if not isinstance(site_total, dict) or not isinstance(action_site, dict):
        return []

    total_events = int(totals.get("events", 0) or 0)
    unknown_events = int(site_total.get("unknown", 0) or 0)

    problems: List[Dict[str, object]] = []

    if total_events > 0:
        unknown_share = float(unknown_events) / float(total_events)
        if unknown_share >= 0.40 and unknown_events >= 3:
            problems.append(
                {
                    "name": "Missing Site Attribution",
                    "severity": "high" if unknown_share >= 0.80 else "medium",
                    "impact_score": round(unknown_share * 100.0, 1),
                    "evidence": (
                        f"unknown site inferred for {unknown_events}/{total_events} events "
                        f"({unknown_share * 100.0:.1f}%)"
                    ),
                    "recommendation": (
                        "Improve target/metadata capture so intent and destination are attributable."
                    ),
                }
            )

    query_total = int(action_total.get("hint_suggested_query", 0) or 0)
    query_unknown = int(action_site.get(("hint_suggested_query", "unknown"), 0) or 0)
    if query_total > 0:
        query_unknown_share = float(query_unknown) / float(query_total)
        if query_unknown_share >= 0.60 and query_unknown >= 1:
            problems.append(
                {
                    "name": "Query Attribution Blind Spot",
                    "severity": "high" if query_unknown_share >= 0.85 else "medium",
                    "impact_score": round(query_unknown_share * 100.0, 1),
                    "evidence": (
                        f"hint_suggested_query is unknown for {query_unknown}/{query_total} events "
                        f"({query_unknown_share * 100.0:.1f}%)"
                    ),
                    "recommendation": (
                        "Attach query intent tags and source destination to each query event."
                    ),
                }
            )

    return problems


def render_report(payload: Dict[str, object], generated_at: Optional[str] = None) -> str:
    stamp = generated_at or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    totals = payload["totals"]
    top_actions = payload["top_actions"]
    top_sites = payload["top_sites"]
    action_site = payload["action_site"]
    site_action = payload["site_action"]
    site_days = payload["site_days"]
    current_problems = payload.get("current_problems", [])

    lines: List[str] = []
    lines.append("# Action x Site Insights")
    lines.append("")
    lines.append(f"Generated: {stamp}")
    lines.append("")
    lines.append(f"* Events analyzed: {totals['events']}")
    lines.append(f"* Distinct actions: {totals['actions']}")
    lines.append(f"* Distinct inferred sites: {totals['sites']}")
    lines.append("")

    lines.append("## Top Actions")
    lines.append("")
    lines.append("| Action | Events | Primary Site | Primary Share |")
    lines.append("| :--- | ---: | :--- | ---: |")
    for action, count in top_actions[:15]:
        pairs = _top_pairs(action_site, action)
        top_site, top_count = pairs[0] if pairs else ("n/a", 0)
        share = (100.0 * top_count / count) if count else 0.0
        lines.append(f"| {action} | {count} | {top_site} | {share:.1f}% |")
    lines.append("")

    lines.append("## Top Sites")
    lines.append("")
    lines.append("| Site | Events | Active Days | Dominant Action | Dominant Share |")
    lines.append("| :--- | ---: | ---: | :--- | ---: |")
    for site, count in top_sites[:20]:
        actions = _top_pairs(site_action, site)
        dominant_action, dominant_count = actions[0] if actions else ("n/a", 0)
        share = (100.0 * dominant_count / count) if count else 0.0
        lines.append(
            f"| {site} | {count} | {len(site_days.get(site, []))} | {dominant_action} | {share:.1f}% |"
        )
    lines.append("")

    lines.append("## Current Problems")
    lines.append("")
    if isinstance(current_problems, list) and current_problems:
        for problem in current_problems:
            lines.append(
                "### {name} ({severity} | impact {impact_score})".format(
                    name=problem.get("name", "Unknown Problem"),
                    severity=problem.get("severity", "n/a"),
                    impact_score=problem.get("impact_score", "n/a"),
                )
            )
            lines.append("")
            lines.append(f"* Evidence: {problem.get('evidence', 'n/a')}")
            lines.append(f"* Fix: {problem.get('recommendation', 'n/a')}")
            lines.append("")
    else:
        lines.append("* No current problems detected from action x site patterns.")
        lines.append("")

    lines.append("## Action -> Top 5 Sites")
    lines.append("")
    for action, count in top_actions[:10]:
        lines.append(f"### {action} ({count})")
        lines.append("")
        for site, site_count in _top_pairs(action_site, action)[:5]:
            share = (100.0 * site_count / count) if count else 0.0
            lines.append(f"* {site}: {site_count} ({share:.1f}%)")
        lines.append("")

    lines.append("## Site -> Top 5 Actions")
    lines.append("")
    for site, count in top_sites[:12]:
        lines.append(f"### {site} ({count})")
        lines.append("")
        for action, action_count in _top_pairs(site_action, site)[:5]:
            share = (100.0 * action_count / count) if count else 0.0
            lines.append(f"* {action}: {action_count} ({share:.1f}%)")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def run(
    db_path: Path,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    output_dir: Optional[Path] = None,
    report_date: Optional[str] = None,
) -> Dict[str, object]:
    """Generate and write action/site insights from events."""
    target_dir = output_dir or OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    owns_connection = False
    live_con = con
    if live_con is None:
        live_con = duckdb.connect(str(db_path), read_only=True)
        owns_connection = True

    try:
        rows = _fetch_rows(live_con)
        payload = _build_payload(rows)
        payload["current_problems"] = _derive_current_problems(payload)

        day_stamp = report_date or datetime.now().strftime("%Y-%m-%d")
        report_path = target_dir / f"action_site_insights_{day_stamp}.md"
        report_text = render_report(payload)
        report_path.write_text(report_text, encoding="utf-8")

        payload["report_path"] = str(report_path)
        return payload
    finally:
        if owns_connection and live_con is not None:
            live_con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate action x site insight report")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()

    payload = run(args.db, output_dir=args.output_dir)
    print(f"Action x site report: {payload['report_path']}")


if __name__ == "__main__":
    main()
