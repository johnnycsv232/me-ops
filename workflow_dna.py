#!/usr/bin/env python3
"""ME-OPS Workflow DNA Engine.

Extracts creation-process DNA from event streams and derives:
1. Genetic markers in tool/stage transitions.
2. Prompt + orchestration style fingerprints.
3. Bottlenecks and drift patterns.
4. Three premium workflow upgrades customized to observed style.

Usage:
    python workflow_dna.py --db me_ops.duckdb
    python workflow_dna.py --db me_ops.duckdb --mock
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import duckdb

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))

from action_site_insights import infer_site


DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


DNA_DDL = """
CREATE TABLE IF NOT EXISTS workflow_dna_markers (
    snapshot_date    DATE NOT NULL,
    marker_rank      INTEGER NOT NULL,
    marker_name      VARCHAR NOT NULL,
    transition       VARCHAR NOT NULL,
    frequency        INTEGER DEFAULT 0,
    support_days     INTEGER DEFAULT 0,
    avg_gap_sec      DOUBLE DEFAULT 0.0,
    strength         DOUBLE DEFAULT 0.0,
    PRIMARY KEY (snapshot_date, marker_rank)
);

CREATE TABLE IF NOT EXISTS workflow_dna_profiles (
    snapshot_date            DATE PRIMARY KEY,
    unique_style             VARCHAR,
    orchestration_signature  VARCHAR,
    prompt_signature         VARCHAR,
    bottleneck_count         INTEGER DEFAULT 0,
    premium_workflow_count   INTEGER DEFAULT 0,
    payload_json             JSON,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

RESEARCH_STAGES = {"notion", "web", "local_web", "github", "query", "framing"}
SYNTHESIS_STAGES = {"synthesis", "rebuilding"}
ENVIRONMENT_STAGES = {"environment"}
CODE_EXTENSIONS = {
    "py",
    "ts",
    "tsx",
    "js",
    "jsx",
    "md",
    "sql",
    "json",
    "sh",
    "yaml",
    "yml",
    "toml",
    "go",
    "rs",
    "java",
    "kt",
    "swift",
    "php",
    "rb",
    "cs",
}

TRANSITION_NAMES: Dict[Tuple[str, str], str] = {
    ("notion", "web"): "Command-Center Launch",
    ("notion", "query"): "Plan-to-Question Bridge",
    ("web", "query"): "Research Funnel",
    ("query", "framing"): "Constraint Framing",
    ("framing", "synthesis"): "Evidence Compression",
    ("query", "synthesis"): "Direct Synthesis Jump",
    ("synthesis", "code"): "Insight-to-Build Handoff",
    ("code", "synthesis"): "Build-to-Document Feedback",
    ("environment", "code"): "Setup-to-Execution Transition",
    ("rebuilding", "code"): "Strategy-to-Code Transition",
}


# ---------------------------------------------------------------------------
# Core Classification Helpers
# ---------------------------------------------------------------------------

def _safe_text(value: object | None) -> str:
    if value is None:
        return ""
    return str(value)


def _extract_extension(target: str) -> str:
    match = re.search(r"\.([A-Za-z0-9]+)(?:$|\?|#)", target)
    if not match:
        return ""
    return match.group(1).lower()


def classify_stage(action: object, target: object = None, app_tool: object = None) -> str:
    """Normalize raw event attributes into coarse workflow stages."""
    action_l = _safe_text(action).strip().lower()
    target_l = _safe_text(target).strip().lower()
    app_tool_l = _safe_text(app_tool).strip().lower()

    if "notion.so" in target_l or "notion.com" in target_l or "notion.site" in target_l:
        return "notion"

    if "github.com" in target_l:
        return "github"

    if action_l == "web_visit":
        if "localhost" in target_l or "127.0.0.1" in target_l:
            return "local_web"
        return "web"

    if action_l.startswith("hint_") or "query" in action_l:
        return "query"

    if action_l == "time_range":
        return "framing"

    if action_l in {"workstream_summary", "annotation_summary", "annotation_description"}:
        return "synthesis"

    if action_l in {"file_reference", "code_snippet"}:
        return "code"

    if action_l.startswith("conversation"):
        return "conversation"

    if "python" in target_l or _extract_extension(target_l) in CODE_EXTENSIONS:
        return "code"

    if "vscode" in app_tool_l or "antigravity" in app_tool_l:
        return "code"

    if any(term in target_l for term in ["setup", "install", "config", "env", "venv", ".git"]):
        return "environment"

    if "rebuild" in target_l or "reconstruct" in target_l:
        return "rebuilding"

    return "activity"


def _sort_events(events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(event: Dict[str, object]) -> datetime:
        ts = event.get("ts_start")
        if isinstance(ts, datetime):
            if ts.tzinfo is not None:
                return ts.astimezone(timezone.utc).replace(tzinfo=None)
            return ts
        return datetime.min

    return sorted(
        events,
        key=_key,
    )


def _sessionize(
    events: Sequence[Dict[str, Any]], gap_minutes: int = 30
) -> List[List[Dict[str, Any]]]:
    ordered = _sort_events(events)
    if not ordered:
        return []

    sessions: List[List[Dict[str, object]]] = [[ordered[0]]]
    gap_sec_threshold = float(gap_minutes * 60)

    ordered_list = list(ordered)
    for prev, curr in zip(ordered_list[:-1], ordered_list[1:]):
        prev_ts = prev.get("ts_start")
        curr_ts = curr.get("ts_start")
        if isinstance(prev_ts, datetime) and isinstance(curr_ts, datetime):
            gap = (curr_ts - prev_ts).total_seconds()
        else:
            gap = 0.0

        if gap > gap_sec_threshold:
            sessions.append([curr])
        else:
            sessions[-1].append(curr)

    return sessions


# ---------------------------------------------------------------------------
# DNA Extraction
# ---------------------------------------------------------------------------

def extract_genetic_markers(
    events: Sequence[Dict[str, Any]], top_n: int = 8
) -> List[Dict[str, Any]]:
    """Extract high-signal transition markers from event stage changes."""
    ordered = _sort_events(events)
    if len(ordered) < 2:
        return []

    transition_stats: Dict[Tuple[str, str], Dict[str, Any]] = {}
    triads: Counter[Tuple[str, str, str]] = Counter()

    stages = [
        classify_stage(e.get("action"), e.get("target"), e.get("app_tool"))
        for e in ordered
    ]

    for idx in range(2, len(stages)):
        key = (stages[idx - 2], stages[idx - 1], stages[idx])
        triads[key] = triads[key] + 1

    ordered_list = list(ordered)
    for prev, curr in zip(ordered_list[:-1], ordered_list[1:]):
        src = classify_stage(prev.get("action"), prev.get("target"), prev.get("app_tool"))
        dst = classify_stage(curr.get("action"), curr.get("target"), curr.get("app_tool"))

        if src == dst:
            continue

        prev_ts = prev.get("ts_start")
        curr_ts = curr.get("ts_start")
        if isinstance(prev_ts, datetime) and isinstance(curr_ts, datetime):
            gap = max(0.0, (curr_ts - prev_ts).total_seconds())
            day_key = curr_ts.date().isoformat()
        else:
            gap = 0.0
            day_key = "unknown"

        stats = transition_stats.setdefault(
            (src, dst),
            {
                "count": 0,
                "gap_sum": 0.0,
                "days": set(),
            },
        )
        stats["count"] = int(float(stats.get("count", 0))) + 1
        stats["gap_sum"] = float(stats.get("gap_sum", 0.0)) + float(gap)
        days = stats["days"]
        if isinstance(days, set):
            days.add(day_key)

    if not transition_stats:
        return []

    max_count = max(int(float(v.get("count", 1))) for v in transition_stats.values())
    markers: List[Dict[str, Any]] = []

    for (src, dst), stats in transition_stats.items():
        count = int(stats["count"])
        if count < 2:
            continue

        days = stats["days"]
        support_days = len(days) if isinstance(days, set) else 0
        avg_gap = float(stats["gap_sum"]) / max(count, 1)
        triad_support = sum(
            freq for (a, b, _), freq in triads.items() if a == src and b == dst
        )

        freq_score = count / max(max_count, 1)
        coverage_score = min(1.0, support_days / 14.0)
        speed_score = max(0.0, 1.0 - (avg_gap / 600.0))
        triad_score = min(1.0, triad_support / max(1, count * 2))
        strength = round(
            float(
                (freq_score * 0.5)
                + (coverage_score * 0.2)
                + (speed_score * 0.15)
                + (triad_score * 0.15)
            ),
            3,
        )

        markers.append(
            {
                "name": TRANSITION_NAMES.get((src, dst), f"{src.title()} to {dst.title()} Relay"),
                "transition": f"{src} -> {dst}",
                "frequency": count,
                "support_days": support_days,
                "avg_gap_sec": round(float(avg_gap), 1),
                "triad_support": triad_support,
                "strength": strength,
            }
        )

    markers_list = list(markers)
    markers_list.sort(
        key=lambda item: (
            float(item.get("strength", 0.0)),
            int(float(item.get("frequency", 0))),
            -float(item.get("avg_gap_sec", 0.0)),
        ),
        reverse=True,
    )

    return markers_list[:top_n]


def _prompt_fingerprint(events: Sequence[Dict[str, Any]]) -> Dict[str, float]:
    queries: List[str] = []
    for event in events:
        action = _safe_text(event.get("action")).lower()
        target = _safe_text(event.get("target"))
        if action.startswith("hint_") or "query" in action:
            if target:
                queries.append(target)
        elif "?" in target and len(target) > 8:
            queries.append(target)

    if not queries:
        return {
            "query_count": 0.0,
            "avg_words": 0.0,
            "question_ratio": 0.0,
            "evidence_seeking_ratio": 0.0,
            "systems_language_ratio": 0.0,
            "imperative_ratio": 0.0,
        }

    evidence_terms = {
        "which",
        "what",
        "why",
        "how",
        "validate",
        "result",
        "evidence",
        "outcome",
        "metric",
        "logs",
        "confirm",
    }
    systems_terms = {
        "workflow",
        "engine",
        "pipeline",
        "schema",
        "integration",
        "orchestr",
        "agent",
        "automation",
        "system",
        "mcp",
    }
    imperative_starts = {
        "build",
        "create",
        "fix",
        "audit",
        "optimize",
        "implement",
        "generate",
        "validate",
        "design",
    }

    total_words = 0
    question_like = 0
    evidence_hits = 0
    systems_hits = 0
    imperative_hits = 0

    for query in queries:
        q = query.strip()
        q_lower = q.lower()
        words = re.findall(r"[a-zA-Z0-9_']+", q_lower)
        total_words += len(words)
        if "?" in q:
            question_like += 1

        if any(term in q_lower for term in evidence_terms):
            evidence_hits += 1
        if any(term in q_lower for term in systems_terms):
            systems_hits += 1
        if words and words[0] in imperative_starts:
            imperative_hits += 1

    n = float(len(queries))
    return {
        "query_count": n,
        "avg_words": round(float(total_words / max(n, 1.0)), 2),
        "question_ratio": round(float(question_like / max(n, 1.0)), 3),
        "evidence_seeking_ratio": round(float(evidence_hits / max(n, 1.0)), 3),
        "systems_language_ratio": round(float(systems_hits / max(n, 1.0)), 3),
        "imperative_ratio": round(float(imperative_hits / max(n, 1.0)), 3),
    }


def profile_creation_style(
    events: Sequence[Dict[str, Any]], markers: Sequence[Dict[str, Any]]
) -> Dict[str, Any]:
    """Generate a style profile from event flow and prompt semantics."""
    ordered = _sort_events(events)
    if not ordered:
        return {
            "unique_style": ["Insufficient Signal"],
            "metrics": {},
            "prompt_fingerprint": _prompt_fingerprint([]),
            "orchestration_signature": "No signal",
        }

    sessions = _sessionize(ordered, gap_minutes=30)
    total_sessions = len(sessions)

    notion_first = 0
    eligible_sessions = 0
    closure_hits = 0
    closure_opportunities = 0
    query_edges = 0

    stage_counts: Counter[str] = Counter()

    for session in sessions:
        stages = [
            classify_stage(e.get("action"), e.get("target"), e.get("app_tool"))
            for e in session
        ]
        stage_counts.update(stages)

        primary = [s for s in stages if s != "activity"]
        if primary:
            eligible_sessions += 1
            if primary[0] == "notion":
                notion_first += 1

        for i, stage in enumerate(stages):
            if stage in RESEARCH_STAGES:
                closure_opportunities += 1
                window = list(stages)[i + 1 : i + 6]
                if any(w in SYNTHESIS_STAGES for w in window):
                    closure_hits += 1

            if i > 0:
                prev = stages[i - 1]
                if (prev == "query" and stage in {"query", "framing"}) or (
                    prev == "framing" and stage == "query"
                ):
                    query_edges += 1

    total_events = float(len(ordered))
    notion_first_ratio = notion_first / max(eligible_sessions, 1)
    closure_ratio = closure_hits / max(closure_opportunities, 1)
    query_ladder_ratio = query_edges / max(stage_counts.get("query", 0), 1)
    synthesis_share = stage_counts.get("synthesis", 0) / max(total_events, 1.0)
    web_share = (
        stage_counts.get("web", 0) + stage_counts.get("local_web", 0)
    ) / max(total_events, 1.0)

    prompt_fp = _prompt_fingerprint(ordered)

    style_scores: Dict[str, float] = {}
    if notion_first_ratio >= 0.18:
        style_scores["Notion-First Planning"] = notion_first_ratio
    if closure_ratio >= 0.40:
        style_scores["Zero-Drift Orchestration"] = closure_ratio
    if query_ladder_ratio >= 0.35:
        style_scores["Query-Ladder Refinement"] = query_ladder_ratio
    if float(prompt_fp["evidence_seeking_ratio"]) >= 0.35:
        style_scores["Evidence-Seeking Prompting"] = float(
            prompt_fp["evidence_seeking_ratio"]
        )
    if synthesis_share >= 0.12:
        style_scores["Capture-First Consolidation"] = synthesis_share
    if web_share >= 0.35:
        style_scores["Exploration-Heavy Discovery"] = web_share

    if not style_scores:
        style_scores["Adaptive Multi-Modal Execution"] = 0.5

    unique_style = [
        str(name)
        for name, _ in sorted(style_scores.items(), key=lambda item: item[1], reverse=True)
    ][:4]

    top_marker = markers[0]["transition"] if markers else "query -> synthesis"
    orchestration_signature = (
        f"{top_marker}; closure={closure_ratio:.0%}; query_ladder={query_ladder_ratio:.0%}"
    )

    return {
        "unique_style": unique_style,
        "metrics": {
            "sessions": total_sessions,
            "notion_first_ratio": round(float(notion_first_ratio), 3),
            "closure_ratio": round(float(closure_ratio), 3),
            "query_ladder_ratio": round(float(query_ladder_ratio), 3),
            "synthesis_share": round(float(synthesis_share), 3),
            "web_share": round(float(web_share), 3),
        },
        "prompt_fingerprint": prompt_fp,
        "orchestration_signature": orchestration_signature,
    }


def detect_bottlenecks(events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Detect efficiency bottlenecks from stage dynamics."""
    ordered = _sort_events(events)
    if not ordered:
        return []

    stages = [
        classify_stage(e.get("action"), e.get("target"), e.get("app_tool"))
        for e in ordered
    ]

    bottlenecks: List[Dict[str, Any]] = []

    # Browser drift: long consecutive web streaks.
    web_runs: List[int] = []
    current_web = 0
    for stage in stages:
        if stage in {"web", "local_web"}:
            current_web += 1
        else:
            if current_web >= 3:
                web_runs.append(current_web)
            current_web = 0
    if current_web >= 3:
        web_runs.append(current_web)

    if web_runs:
        total_drift_events = sum(web_runs)
        avg_run = total_drift_events / max(len(web_runs), 1)
        impact = round(float(total_drift_events * 0.7 + avg_run * 2.0), 2)
        bottlenecks.append(
            {
                "name": "Browser Drift",
                "severity": "high" if max(web_runs) >= 8 else "medium",
                "impact_score": impact,
                "evidence": (
                    f"{len(web_runs)} drift episodes, avg run {avg_run:.1f}, "
                    f"max run {max(web_runs)}"
                ),
                "recommendation": (
                    "Trigger a forced synthesis checkpoint after 3 consecutive web events."
                ),
            }
        )

    # Query loop fatigue: repeated query/framing loops without synthesis.
    query_runs: List[int] = []
    current_q = 0
    for stage in stages:
        if stage in {"query", "framing"}:
            current_q += 1
        else:
            if current_q >= 4:
                query_runs.append(current_q)
            current_q = 0
    if current_q >= 4:
        query_runs.append(current_q)

    if query_runs:
        total_query = sum(query_runs)
        impact = round(float(total_query * 0.45 + len(query_runs) * 3.0), 2)
        bottlenecks.append(
            {
                "name": "Query Loop Fatigue",
                "severity": "medium",
                "impact_score": impact,
                "evidence": (
                    f"{len(query_runs)} loops >=4 steps, {total_query} total query/framing events"
                ),
                "recommendation": (
                    "After two query rounds, require a one-sentence decision or next action."
                ),
            }
        )

    # Low closure bottleneck.
    closures = 0
    opportunities = 0
    for i, stage in enumerate(stages):
        if stage in RESEARCH_STAGES:
            opportunities += 1
            window = stages[i + 1 : i + 8]
            if any(w in SYNTHESIS_STAGES for w in window):
                closures += 1

    if opportunities:
        closure_ratio = closures / opportunities
        if closure_ratio < 0.35:
            missing = opportunities - closures
            impact = round(float(missing * 1.2), 2)
            bottlenecks.append(
                {
                    "name": "Low Research Closure",
                    "severity": "high" if closure_ratio < 0.20 else "medium",
                    "impact_score": impact,
                    "evidence": (
                        f"closure ratio {closure_ratio:.1%} ({closures}/{opportunities})"
                    ),
                    "recommendation": (
                        "Install a mandatory workstream summary at the end of each research burst."
                    ),
                }
            )

    bottlenecks_list = list(bottlenecks)
    bottlenecks_list.sort(key=lambda b: float(b.get("impact_score", 0.0)), reverse=True)
    return bottlenecks_list[:5]


def _parse_transition(value: object) -> Tuple[str, str]:
    text = _safe_text(value).strip().lower()
    if "->" not in text:
        return "", ""
    src, dst = text.split("->", 1)
    return src.strip(), dst.strip()


def _clip(text: str, max_len: int = 100) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _infer_web_intent(target: str) -> str:
    text = (target or "").strip().lower()
    if not text:
        return "unknown"

    if not text.startswith("http://") and not text.startswith("https://"):
        if "search" in text:
            return "search"
        if "docs" in text:
            return "docs_read"
        return "unstructured_target"

    try:
        parsed = urlparse(text)
    except Exception:
        return "unknown"

    host = parsed.netloc.lower()
    path = parsed.path.lower()
    query = parsed.query.lower()

    if "localhost" in host or "127.0.0.1" in host:
        return "local_debug"
    if any(token in path for token in ["/signin", "/login", "/auth", "/oauth"]):
        return "auth_flow"
    if "googleapis.com" in host or host.startswith("api."):
        return "api_call"
    if any(token in path for token in ["/dashboard", "/console", "/settings", "/admin", "/workbench"]):
        return "dashboard_ops"
    if "youtube.com" in host or "/watch" in path:
        return "video_review"
    if any(token in path for token in ["/issues", "/pull", "/pulls", "/blob", "/tree", "/commit"]):
        return "repo_review"
    if host.startswith("docs.") or "/docs" in path or "/documentation" in path:
        return "docs_read"
    if any(token in path for token in ["/search", "/find"]) or "q=" in query or "query=" in query:
        return "search"
    return "page_visit"


def _resolve_contextual_destination(
    ordered: Sequence[Dict[str, object]],
    stages: Sequence[str],
    idx: int,
) -> Tuple[str, str, str, str]:
    """Resolve destination + intent for an event, using nearby web context when needed."""
    event = ordered[idx]
    stage = stages[idx]
    target = _safe_text(event.get("target")).strip()
    metadata_text = _safe_text(event.get("metadata_text")).strip()
    app_tool = _safe_text(event.get("app_tool")).strip() or "n/a"

    if stage in {"web", "local_web"}:
        destination = infer_site(target, metadata_text) or "unknown"
        return destination, _infer_web_intent(target), app_tool, _clip(target or metadata_text or "n/a", 120)

    # Non-web stages: anchor to nearest web event for concrete context.
    for radius in (1, 2, 3):
        for j in (idx - radius, idx + radius):
            if j < 0 or j >= len(ordered):
                continue
            if stages[j] not in {"web", "local_web"}:
                continue
            web_event = ordered[j]
            web_target = _safe_text(web_event.get("target")).strip()
            web_metadata = _safe_text(web_event.get("metadata_text")).strip()
            destination = infer_site(web_target, web_metadata) or "unknown"
            return (
                destination,
                f"context_{stage}",
                app_tool,
                _clip(web_target or web_metadata or target or "n/a", 120),
            )

    return (
        stage or "activity",
        f"stage_{stage or 'activity'}",
        app_tool,
        _clip(target or metadata_text or "n/a", 120),
    )


def _summarize_indices_as_activity(
    ordered: Sequence[Dict[str, Any]],
    stages: Sequence[str],
    indices: Sequence[int],
    top_items: int = 8,
) -> List[Dict[str, Any]]:
    destination_counts: Counter[str] = Counter()
    destination_intents: Dict[str, Counter[str]] = defaultdict(Counter)
    destination_next_steps: Dict[str, Counter[str]] = defaultdict(Counter)
    destination_next_gap_sum: Dict[str, float] = {}
    destination_next_gap_count: Counter[str] = Counter()
    destination_tools: Dict[str, Counter[str]] = defaultdict(Counter)
    destination_examples: Dict[str, str] = {}

    for idx in indices:
        if idx < 0 or idx >= len(ordered):
            continue

        destination, intent, app_tool, sample_target = _resolve_contextual_destination(
            ordered, stages, idx
        )
        destination_counts[destination] += 1
        destination_intents[destination][intent] += 1
        destination_tools[destination][app_tool] += 1
        if destination not in destination_examples:
            destination_examples[destination] = sample_target

        if idx + 1 < len(ordered):
            nxt = ordered[idx + 1]
            next_action = _safe_text(nxt.get("action")).strip() or "unknown_action"
            next_stage = stages[idx + 1]
            destination_next_steps[destination][f"{next_action} ({next_stage})"] += 1

            curr_ts = ordered[idx].get("ts_start")
            next_ts = nxt.get("ts_start")
            if isinstance(curr_ts, datetime) and isinstance(next_ts, datetime):
                gap = max(0.0, (next_ts - curr_ts).total_seconds())
                destination_next_gap_sum[destination] = destination_next_gap_sum.get(destination, 0.0) + float(gap)
                destination_next_gap_count[destination] += 1

    total = sum(destination_counts.values())
    if total == 0:
        return []

    rows: List[Dict[str, object]] = []
    for destination, count in destination_counts.most_common(top_items):
        intent_counter = destination_intents.get(destination, Counter())
        next_counter = destination_next_steps.get(destination, Counter())
        tool_counter = destination_tools.get(destination, Counter())
        rows.append(
            {
                "destination": destination,
                "count": count,
                "share_pct": round(float(100.0 * count) / max(total, 1), 1),
                "top_intent": intent_counter.most_common(1)[0][0] if intent_counter else "unknown",
                "top_next_step": next_counter.most_common(1)[0][0] if next_counter else "n/a",
                "avg_next_gap_sec": (
                    round(
                        float(destination_next_gap_sum[destination])
                        / max(float(destination_next_gap_count[destination]), 1.0),
                        1,
                    )
                    if destination_next_gap_count[destination] > 0
                    else "n/a"
                ),
                "top_app_tool": tool_counter.most_common(1)[0][0] if tool_counter else "n/a",
                "sample_target": destination_examples.get(destination, "n/a"),
            }
        )
    return list(rows)


def build_current_problem_breakdowns(
    events: Sequence[Dict[str, Any]],
    bottlenecks: Sequence[Dict[str, Any]],
    top_items: int = 8,
) -> List[Dict[str, Any]]:
    """Attach concrete activity/site detail to each current problem."""
    ordered = _sort_events(events)
    if not ordered or not bottlenecks:
        return []

    stages = [
        classify_stage(e.get("action"), e.get("target"), e.get("app_tool"))
        for e in ordered
    ]

    problems: List[Dict[str, Any]] = []
    for bottleneck in bottlenecks:
        name = _safe_text(bottleneck.get("name"))
        indices: List[int] = []

        if name == "Browser Drift":
            run: List[int] = []
            for i, stage in enumerate(stages):
                if stage in {"web", "local_web"}:
                    run.append(i)
                else:
                    if len(run) >= 3:
                        indices.extend(run)
                    run = []
            if len(run) >= 3:
                indices.extend(run)

        elif name == "Query Loop Fatigue":
            run = []
            for i, stage in enumerate(stages):
                if stage in {"query", "framing"}:
                    run.append(i)
                else:
                    if len(run) >= 4:
                        indices.extend(run)
                    run = []
            if len(run) >= 4:
                indices.extend(run)

        elif name == "Low Research Closure":
            for i, stage in enumerate(stages):
                if stage not in RESEARCH_STAGES:
                    continue
                window = list(stages)[i + 1 : i + 8]
                if not any(w in SYNTHESIS_STAGES for w in window):
                    indices.append(i)

        activity_breakdown = _summarize_indices_as_activity(
            ordered, stages, indices, top_items=top_items
        )

        problems.append(
            {
                "name": bottleneck.get("name"),
                "severity": bottleneck.get("severity"),
                "impact_score": bottleneck.get("impact_score"),
                "evidence": bottleneck.get("evidence"),
                "recommendation": bottleneck.get("recommendation"),
                "sample_count": len(indices),
                "activity_breakdown": activity_breakdown,
            }
        )

    return problems


def extract_web_transition_breakdowns(
    events: Sequence[Dict[str, Any]],
    markers: Sequence[Dict[str, Any]],
    top_destinations: int = 8,
) -> List[Dict[str, Any]]:
    """Break down web-ending transitions into concrete destinations/tools."""
    if len(events) < 2 or not markers:
        return []

    ordered = _sort_events(events)
    selected: List[Tuple[Dict[str, Any], str, str]] = []

    for marker in markers:
        src, dst = _parse_transition(marker.get("transition"))
        if dst in {"web", "local_web"} and src:
            selected.append((dict(marker), src, dst))

    if not selected:
        return []

    breakdowns: List[Dict[str, Any]] = []

    for marker, src_filter, dst_filter in selected:
        destination_counts: Counter[str] = Counter()
        destination_tools: Dict[str, Counter[str]] = defaultdict(Counter)
        destination_intents: Dict[str, Counter[str]] = defaultdict(Counter)
        destination_next_steps: Dict[str, Counter[str]] = defaultdict(Counter)
        destination_next_gap_sum: Dict[str, float] = {}
        destination_next_gap_count: Counter[str] = Counter()
        destination_examples: Dict[str, str] = {}
        total_hops = 0

        for idx in range(len(ordered) - 1):
            prev = ordered[idx]
            curr = ordered[idx + 1]
            src = classify_stage(prev.get("action"), prev.get("target"), prev.get("app_tool"))
            dst = classify_stage(curr.get("action"), curr.get("target"), curr.get("app_tool"))
            if src != src_filter or dst != dst_filter:
                continue

            total_hops += 1
            target = _safe_text(curr.get("target")).strip()
            metadata_text = _safe_text(curr.get("metadata_text")).strip()
            destination = infer_site(target, metadata_text)
            if not destination:
                destination = "unknown"

            destination_counts[destination] += 1
            app_tool = _safe_text(curr.get("app_tool")).strip() or "n/a"
            destination_tools[destination][app_tool] += 1
            destination_intents[destination][_infer_web_intent(target)] += 1

            if destination not in destination_examples:
                sample = target or metadata_text or "n/a"
                destination_examples[destination] = _clip(sample, 120)

            if idx + 2 < len(ordered):
                nxt = ordered[idx + 2]
                next_action = _safe_text(nxt.get("action")).strip() or "unknown_action"
                next_stage = classify_stage(
                    nxt.get("action"), nxt.get("target"), nxt.get("app_tool")
                )
                destination_next_steps[destination][f"{next_action} ({next_stage})"] += 1

                curr_ts = curr.get("ts_start")
                next_ts = nxt.get("ts_start")
                if isinstance(curr_ts, datetime) and isinstance(next_ts, datetime):
                    gap = max(0.0, (next_ts - curr_ts).total_seconds())
                    destination_next_gap_sum[destination] = destination_next_gap_sum.get(destination, 0.0) + float(gap)
                    destination_next_gap_count[destination] += 1

        if total_hops == 0:
            continue

        destinations: List[Dict[str, Any]] = []
        for destination, count in destination_counts.most_common(top_destinations):
            tool_counter = destination_tools.get(destination, Counter())
            intent_counter = destination_intents.get(destination, Counter())
            next_counter = destination_next_steps.get(destination, Counter())
            top_tool = tool_counter.most_common(1)[0][0] if tool_counter else "n/a"
            top_intent = intent_counter.most_common(1)[0][0] if intent_counter else "unknown"
            top_next_step = next_counter.most_common(1)[0][0] if next_counter else "n/a"
            avg_next_gap_sec = (
                round(
                    float(destination_next_gap_sum.get(destination, 0.0))
                    / max(float(destination_next_gap_count.get(destination, 1)), 1.0),
                    1,
                )
                if destination_next_gap_count.get(destination, 0) > 0
                else None
            )
            destinations.append(
                {
                    "destination": destination,
                    "count": count,
                    "share_pct": round(float(100.0 * count) / max(total_hops, 1), 1),
                    "top_app_tool": top_tool,
                    "top_intent": top_intent,
                    "top_next_step": top_next_step,
                    "avg_next_gap_sec": avg_next_gap_sec,
                    "sample_target": destination_examples.get(destination, "n/a"),
                }
            )

        breakdowns.append(
            {
                "name": marker.get("name", f"{src_filter.title()} -> {dst_filter.title()}"),
                "transition": marker.get("transition", f"{src_filter} -> {dst_filter}"),
                "total_hops": total_hops,
                "destinations": destinations,
            }
        )

    return breakdowns


def generate_premium_workflows(
    profile: Dict[str, Any],
    markers: Sequence[Dict[str, Any]],
    bottlenecks: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate exactly three premium workflow upgrades."""
    unique_style = set(profile.get("unique_style", []))
    top_marker = markers[0]["transition"] if markers else "query -> synthesis"
    top_bottleneck = bottlenecks[0]["name"] if bottlenecks else "Context Drift"

    wf1_name = (
        "Premium Workflow 01: Notion Command Center Sprint"
        if "Notion-First Planning" in unique_style
        else "Premium Workflow 01: Intent Command Sprint"
    )

    wf1 = {
        "name": wf1_name,
        "trigger": "Start of deep-work block",
        "base_marker": top_marker,
        "steps": [
            "Open command center and define one measurable outcome.",
            "Run two focused research hops max before forcing a query decision.",
            "Compress findings into workstream summary plus annotation notes.",
            "Hand off to code/file action within five minutes.",
            "Close with a one-line execution checkpoint.",
        ],
        "kpi": "Research-to-synthesis closure rate >= 70%",
    }

    wf2 = {
        "name": "Premium Workflow 02: Query Ladder to Artifact Forge",
        "trigger": "Any high-ambiguity problem",
        "base_marker": "query -> framing -> synthesis",
        "steps": [
            "Issue one evidence-seeking query (not open-ended browsing).",
            "Pin a time range or scope boundary immediately.",
            "Write one decisive synthesis statement.",
            "Transform synthesis into artifact: code, schema, or deliverable note.",
            "Capture counterfactual: what would invalidate this decision?",
        ],
        "kpi": "Decision latency from query to synthesis <= 90 seconds median",
    }

    wf3 = {
        "name": "Premium Workflow 03: Anti-Drift Recovery Loop",
        "trigger": f"When {top_bottleneck} signal appears",
        "base_marker": "web -> web (drift interrupt)",
        "steps": [
            "Detect drift trigger (3+ consecutive web actions or looping queries).",
            "Pause exploration and run a 60-second framing checkpoint.",
            "Force a workstream summary before resuming exploration.",
            "Either commit to build mode or intentionally exit the loop.",
            "Log the interruption source to improve future guardrails.",
        ],
        "kpi": "Browser drift episodes reduced by 40% over 14 days",
    }

    return [wf1, wf2, wf3]


# ---------------------------------------------------------------------------
# IO + Reporting
# ---------------------------------------------------------------------------

def _load_events(con: duckdb.DuckDBPyConnection) -> List[Dict[str, object]]:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'events'
        """
    ).fetchone()
    if not row or int(row[0]) == 0:
        return []

    rows = con.execute(
        """
        SELECT
            event_id,
            ts_start,
            action,
            app_tool,
            target,
            COALESCE(CAST(metadata_json AS VARCHAR), '') AS metadata_text
        FROM events
        WHERE ts_start IS NOT NULL
        ORDER BY ts_start
        """
    ).fetchall()

    return [
        {
            "event_id": event_id,
            "ts_start": ts_start,
            "action": action,
            "app_tool": app_tool,
            "target": target,
            "metadata_text": metadata_text,
        }
        for event_id, ts_start, action, app_tool, target, metadata_text in rows
    ]


def _mock_events() -> List[Dict[str, object]]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    events: List[Dict[str, object]] = []

    for i in range(4):
        base = now + timedelta(minutes=i * 20)
        events.extend(
            [
                {
                    "event_id": f"mock_n{i}",
                    "ts_start": base,
                    "action": "web_visit",
                    "app_tool": "OS_SERVER",
                    "target": "https://www.notion.so/Command-Center",
                },
                {
                    "event_id": f"mock_w{i}",
                    "ts_start": base + timedelta(seconds=8),
                    "action": "web_visit",
                    "app_tool": "OS_SERVER",
                    "target": "https://docs.pieces.app/prod/cli",
                },
                {
                    "event_id": f"mock_q{i}",
                    "ts_start": base + timedelta(seconds=16),
                    "action": "hint_suggested_query",
                    "app_tool": "OS_SERVER",
                    "target": "Which evidence validates this workflow?",
                },
                {
                    "event_id": f"mock_t{i}",
                    "ts_start": base + timedelta(seconds=24),
                    "action": "time_range",
                    "app_tool": "OS_SERVER",
                    "target": "2026-02-20T14:00:00Z -> 2026-02-20T14:10:00Z",
                },
                {
                    "event_id": f"mock_s{i}",
                    "ts_start": base + timedelta(seconds=32),
                    "action": "workstream_summary",
                    "app_tool": "OS_SERVER",
                    "target": "summary:mock",
                },
                {
                    "event_id": f"mock_a{i}",
                    "ts_start": base + timedelta(seconds=40),
                    "action": "annotation_description",
                    "app_tool": "OS_SERVER",
                    "target": "Captured the decision and next steps.",
                },
            ]
        )

    drift_base = now + timedelta(hours=2)
    for j in range(6):
        events.append(
            {
                "event_id": f"mock_drift{j}",
                "ts_start": drift_base + timedelta(seconds=j * 4),
                "action": "web_visit",
                "app_tool": "OS_SERVER",
                "target": "https://news.example.com",
            }
        )

    return events


def _extract_coverage_profile(events: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate date coverage stats."""
    ordered = _sort_events(events)
    dates = []
    for e in ordered:
        ts = e.get("ts_start")
        if isinstance(ts, datetime):
            dates.append(ts.date())
    days_seen = set(dates)

    if not dates:
        return {
            "event_count": 0,
            "active_days": 0,
            "window_start": "n/a",
            "window_end": "n/a",
        }

    return {
        "event_count": int(len(list(events))),
        "active_days": int(len(list(days_seen))),
        "window_start": min(dates).isoformat(),
        "window_end": max(dates).isoformat(),
    }


def generate_workflow_dna_report(payload: Dict[str, Any]) -> str:
    now = local_now().strftime("%Y-%m-%d %H:%M CST")
    coverage = payload.get("coverage", {})
    profile = payload.get("style_profile", {})
    markers = payload.get("genetic_markers", [])
    web_breakdowns = payload.get("web_transition_breakdowns", [])
    bottlenecks = payload.get("bottlenecks", [])
    current_problems = payload.get("current_problems", [])
    premium = payload.get("premium_workflows", [])

    lines: List[str] = []
    lines.append("# WORKFLOW DNA REPORT")
    lines.append("")
    lines.append(f"*Generated: {now}*")
    lines.append("")

    lines.append("## Coverage")
    lines.append("")
    lines.append(
        f"* Events analyzed: {coverage.get('event_count', 0)} | Active days: {coverage.get('active_days', 0)}"
    )
    lines.append(
        f"* Observation window: {coverage.get('window_start', 'n/a')} -> {coverage.get('window_end', 'n/a')}"
    )
    lines.append("")

    lines.append("## Unique Style")
    lines.append("")
    for style in profile.get("unique_style", []):
        lines.append(f"* {style}")

    metrics = profile.get("metrics", {})
    if isinstance(metrics, dict) and metrics:
        lines.append("")
        lines.append(
            "* Key metrics: "
            f"notion_first={metrics.get('notion_first_ratio', 0)}, "
            f"closure={metrics.get('closure_ratio', 0)}, "
            f"query_ladder={metrics.get('query_ladder_ratio', 0)}"
        )

    lines.append("")
    lines.append("## Genetic Markers")
    lines.append("")
    lines.append("| Marker | Transition | Frequency | Days | Avg Gap (s) | Strength |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | ---: |")
    for marker in markers:
        lines.append(
            "| {name} | `{transition}` | {frequency} | {support_days} | {avg_gap_sec} | {strength} |".format(
                name=marker.get("name", "n/a"),
                transition=marker.get("transition", "n/a"),
                frequency=marker.get("frequency", 0),
                support_days=marker.get("support_days", 0),
                avg_gap_sec=marker.get("avg_gap_sec", 0),
                strength=marker.get("strength", 0),
            )
        )

    lines.append("")
    lines.append("## Web Destination Breakdown")
    lines.append("")
    if web_breakdowns:
        lines.append(
            "Exact destinations behind high-signal transitions that end on web/local_web."
        )
        lines.append("")
        for item in web_breakdowns:
            lines.append(
                "### {name} (`{transition}` | {total_hops} hops)".format(
                    name=item.get("name", "n/a"),
                    transition=item.get("transition", "n/a"),
                    total_hops=item.get("total_hops", 0),
                )
            )
            lines.append("")
            lines.append(
                "| Destination | Hits | Share | Intent | Next Step | Avg Next Gap (s) | Example Target |"
            )
            lines.append("| :--- | ---: | ---: | :--- | :--- | ---: | :--- |")
            for dest in item.get("destinations", []):
                lines.append(
                    "| {destination} | {count} | {share_pct}% | {top_intent} | {top_next_step} | {avg_next_gap_sec} | `{sample_target}` |".format(
                        destination=dest.get("destination", "unknown"),
                        count=dest.get("count", 0),
                        share_pct=round(float(dest.get("share_pct", 0.0)), 1),
                        top_intent=dest.get("top_intent", "unknown"),
                        top_next_step=dest.get("top_next_step", "n/a"),
                        avg_next_gap_sec=round(float(dest.get("avg_next_gap_sec", 0.0)), 1) if isinstance(dest.get("avg_next_gap_sec"), (int, float)) else "n/a",
                        sample_target=dest.get("sample_target", "n/a"),
                    )
                )
            lines.append("")
    else:
        lines.append("* No high-signal transitions into web/local_web found in this window.")

    lines.append("")
    lines.append("## Prompt + Orchestration Profile")
    lines.append("")
    lines.append(f"* Orchestration signature: `{profile.get('orchestration_signature', 'n/a')}`")
    fp = profile.get("prompt_fingerprint", {})
    if isinstance(fp, dict):
        lines.append(
            "* Prompt fingerprint: "
            f"query_count={fp.get('query_count', 0)}, "
            f"question_ratio={round(float(fp.get('question_ratio', 0.0)), 2)}, "
            f"evidence_ratio={round(float(fp.get('evidence_seeking_ratio', 0.0)), 2)}, "
            f"systems_ratio={round(float(fp.get('systems_language_ratio', 0.0)), 2)}"
        )

    lines.append("")
    lines.append("## Bottlenecks")
    lines.append("")
    if bottlenecks:
        for item in bottlenecks:
            lines.append(
                f"* **{item.get('name')}** ({item.get('severity')} | impact {item.get('impact_score')})"
            )
            lines.append(f"  * Evidence: {item.get('evidence')}")
            lines.append(f"  * Fix: {item.get('recommendation')}")
    else:
        lines.append("* No material bottlenecks detected in current window.")

    lines.append("")
    lines.append("## Current Problems (Detailed)")
    lines.append("")
    if current_problems:
        lines.append(
            "Concrete activity context for each active problem: where it happens and what you do next."
        )
        lines.append("")
        for problem in current_problems:
            lines.append(
                f"### {problem.get('name')} ({problem.get('severity')} | impact {problem.get('impact_score')})"
            )
            lines.append("")
            lines.append(f"* Evidence: {problem.get('evidence')}")
            lines.append(f"* Fix: {problem.get('recommendation')}")
            lines.append(f"* Sampled events: {problem.get('sample_count', 0)}")
            rows = problem.get("activity_breakdown", [])
            if rows:
                lines.append(
                    "| Destination/Stage | Hits | Share | Intent | Next Step | Avg Next Gap (s) | App Tool | Example Target |"
                )
                lines.append("| :--- | ---: | ---: | :--- | :--- | ---: | :--- | :--- |")
                for row in rows:
                    lines.append(
                        "| {destination} | {count} | {share_pct}% | {top_intent} | {top_next_step} | {avg_next_gap_sec} | {top_app_tool} | `{sample_target}` |".format(
                            destination=row.get("destination", "unknown"),
                            count=row.get("count", 0),
                            share_pct=round(float(row.get("share_pct", 0.0)), 1),
                            top_intent=row.get("top_intent", "unknown"),
                            top_next_step=row.get("top_next_step", "n/a"),
                            avg_next_gap_sec=round(float(row.get("avg_next_gap_sec", 0.0)), 1) if isinstance(row.get("avg_next_gap_sec"), (int, float)) else "n/a",
                            top_app_tool=row.get("top_app_tool", "n/a"),
                            sample_target=row.get("sample_target", "n/a"),
                        )
                    )
            else:
                lines.append("* No detailed activity samples available for this problem yet.")
            lines.append("")
    else:
        lines.append("* No current problems detailed for this window.")

    lines.append("")
    lines.append("## Premium Workflows")
    lines.append("")
    for workflow in premium:
        lines.append(f"### {workflow.get('name', 'Premium Workflow')}")
        lines.append("")
        lines.append(f"* **Process**: {workflow.get('process', 'n/a')}")
        lines.append(f"* **Why it fits**: {workflow.get('fit', 'n/a')}")
        lines.append(f"* Trigger: {workflow.get('trigger')}")
        lines.append(f"* Base marker: `{workflow.get('base_marker')}`")
        lines.append(f"* KPI: {workflow.get('kpi')}")
        steps = workflow.get("steps", [])
        if isinstance(steps, list):
            for idx, step in enumerate(steps, start=1):
                lines.append(f"  {idx}. {step}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _init_schema(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in DNA_DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def _persist_payload(con: duckdb.DuckDBPyConnection, payload: Dict[str, Any]) -> None:
    snapshot_date = local_now().date().isoformat()
    markers = payload.get("genetic_markers", [])
    profile = payload.get("style_profile", {})

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM workflow_dna_markers WHERE snapshot_date = ?", [snapshot_date])
        con.execute("DELETE FROM workflow_dna_profiles WHERE snapshot_date = ?", [snapshot_date])

        marker_rows: List[List[object]] = []
        if isinstance(markers, list):
            for idx, marker in enumerate(markers, start=1):
                marker_rows.append(
                    [
                        snapshot_date,
                        idx,
                        marker.get("name"),
                        marker.get("transition"),
                        marker.get("frequency", 0),
                        marker.get("support_days", 0),
                        marker.get("avg_gap_sec", 0.0),
                        marker.get("strength", 0.0),
                    ]
                )
        if marker_rows:
            con.executemany(
                """
                INSERT INTO workflow_dna_markers
                (snapshot_date, marker_rank, marker_name, transition, frequency, support_days, avg_gap_sec, strength)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                marker_rows,
            )

        unique_style = profile.get("unique_style", []) if isinstance(profile, dict) else []
        prompt_fp = profile.get("prompt_fingerprint", {}) if isinstance(profile, dict) else {}
        prompt_signature = json.dumps(prompt_fp, default=str)

        con.execute(
            """
            INSERT INTO workflow_dna_profiles
            (snapshot_date, unique_style, orchestration_signature, prompt_signature,
             bottleneck_count, premium_workflow_count, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                snapshot_date,
                "; ".join(unique_style) if isinstance(unique_style, list) else str(unique_style),
                profile.get("orchestration_signature") if isinstance(profile, dict) else None,
                prompt_signature,
                len(payload.get("bottlenecks", [])),
                len(payload.get("premium_workflows", [])),
                json.dumps(payload, default=str),
            ],
        )

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def run(
    db_path: Path,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    mock: bool = False,
    output_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Run the Workflow DNA extraction pipeline."""
    output_root = output_dir or OUTPUT_DIR
    output_root.mkdir(parents=True, exist_ok=True)

    owns_connection = False
    live_con = con

    if live_con is None and not mock:
        live_con = duckdb.connect(str(db_path))
        owns_connection = True

    try:
        if mock:
            events = _mock_events()
        else:
            assert live_con is not None
            events = _load_events(live_con)

        markers = extract_genetic_markers(events)
        web_breakdowns = extract_web_transition_breakdowns(events, markers)
        style_profile = profile_creation_style(events, markers)
        bottlenecks = detect_bottlenecks(events)
        current_problems = build_current_problem_breakdowns(events, bottlenecks)
        premium = generate_premium_workflows(style_profile, markers, bottlenecks)

        dna_payload: Dict[str, Any] = {
            "coverage": _extract_coverage_profile(events),
            "genetic_markers": markers,
            "web_transition_breakdowns": web_breakdowns,
            "style_profile": style_profile,
            "bottlenecks": bottlenecks,
            "current_problems": current_problems,
            "premium_workflows": premium,
        }

        report_text = generate_workflow_dna_report(dna_payload)
        report_path = output_root / "WORKFLOW_DNA_REPORT.md"
        report_path.write_text(report_text, encoding="utf-8")

        today = local_date(local_now())
        json_path = output_root / f"workflow_dna_{today}.json"

        dna_payload["report_path"] = str(report_path)
        dna_payload["json_path"] = str(json_path)

        if live_con is not None and not mock:
            _init_schema(live_con)
            _persist_payload(live_con, dna_payload)

        return dna_payload
    finally:
        if owns_connection and live_con is not None:
            live_con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="ME-OPS Workflow DNA Engine")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--mock", action="store_true", help="Run extraction on mock data")
    parser.add_argument("--json", action="store_true", help="Print JSON payload")
    args = parser.parse_args()

    # Establish connection for the entire main execution
    con = duckdb.connect(str(args.db))

    payload = run(args.db, con=con, mock=args.mock)

    # Export to orchestrator
    export_signals_to_orchestrator(con, payload.get("bottlenecks", []), payload.get("style_profile", {}))

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print("WORKFLOW DNA COMPLETE")
        print(f"Report: {payload.get('report_path')}")
        print(f"JSON:   {payload.get('json_path')}")
        print("Unique Style:")
        for item in payload.get("style_profile", {}).get("unique_style", []):
            print(f"  - {item}")
    print("\n✅ Workflow DNA analysis complete")
    con.close()


def export_signals_to_orchestrator(
    con: duckdb.DuckDBPyConnection,
    bottlenecks: List[Dict[str, Any]],
    profile: Dict[str, Any],
) -> None:
    """Export critical behavioral signals to the orchestrator."""
    from orchestrator import Orchestrator, Signal, ActionItem

    orch = Orchestrator()

    for b in bottlenecks:
        if b.get("severity") in ["high", "critical"] or float(b.get("impact_score", 0)) > 500:
            sig = Signal(
                type="behavioral",
                severity=b.get("severity", "medium"),
                source="DNAEngine",
                description=f"Bottleneck: {b['name']}",
                metadata={
                    "impact_score": b.get("impact_score"),
                    "evidence": b.get("evidence"),
                    "recommendation": b.get("recommendation"),
                    "profile_style": profile.get("unique_style")
                }
            )
            sid = orch.register_signal(sig)
            if sid:
                action = ActionItem(
                    title=f"Mitigate {b['name']}",
                    description=f"High-impact bottleneck detected: {b['evidence']}. Recommendation: {b['recommendation']}",
                    category="intervention",
                    signal_id=sid,
                    priority=9 if b.get("severity") == "critical" else 6
                )
                orch.add_to_queue(action)
                print(f"  📤 Exported behavioral signal to orchestrator: {b['name']}")


if __name__ == "__main__":
    main()
