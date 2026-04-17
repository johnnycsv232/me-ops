"""
Phase 2 — Decision extractor.
Lifts pivot/choice/strategy moments from event text into Decision entities.
"""
import json
import re
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from core.storage.db import ts

DECISION_TRIGGERS = [
    "decided to", "decision to", "chose to", "switched to", "moved to",
    "going to use", "instead of", "rather than", "pivot", "changed approach",
    "will use", "going with", "settled on", "opted for", "rather use",
    "dropped", "abandoned in favor", "selected", "refined the scope",
    "migrated to", "focus on", "shifted focus", "re-scoped", "chose not to",
    "abandoned", "reverted to", "transitioned to", "determined to",
    "elected to", "resolved to", "committed to", "confirmed that",
    "approved the", "reviewed and implicitly approved",
]

OPTION_MARKERS = [
    "could have", "option was", "alternatively", "or instead",
    "vs ", "versus", "compared to", "considered",
]

SECTION_HEADER = re.compile(
    r"^#+\s*\*{0,2}\s*key discussions\s*&\s*decisions\s*\*{0,2}\s*$",
    re.IGNORECASE,
)
HEADING_LINE = re.compile(r"^#+\s")
BULLET_LINE = re.compile(r"^\s*[-*•]\s+")
STRUCTURED_DECISION_MARKERS = (
    "decided", "decision", "approved", "confirmed", "selected", "chose",
    "switched", "migrated", "transitioned", "pivot", "defer", "deferred",
    "focus", "priorit", "committed", "resolved", "store", "use ",
)


def extract_decisions(conn) -> int:
    """Scan events for decision signals and lift them into decision entities."""
    events = conn.execute(
        "SELECT * FROM entities WHERE type='event' ORDER BY created_at ASC"
    ).fetchall()

    created = 0
    for row in events:
        data = json.loads(row["data"] or "{}")
        text = data.get("raw_content") or row["summary"] or ""
        if len(text) < 30:
            continue

        structured_choices = _extract_structured_choices(text)
        if structured_choices:
            choices = structured_choices
            confidence = 0.82
        else:
            tl = text.lower()
            if not any(sig in tl for sig in DECISION_TRIGGERS):
                continue
            choices = [_extract_choice(text)]
            confidence = 0.65

        existing = {
            decision["summary"]
            for decision in conn.execute(
                "SELECT summary FROM entities WHERE type='decision' AND source_refs LIKE ?",
                (f'%"{row["id"]}"%',),
            ).fetchall()
        }

        project_id = row["project_id"] if row["project_id"] and row["project_id"] != "unknown" else None
        if project_id:
            conn.execute("""
                INSERT OR IGNORE INTO projects
                  (id, name, description, status, created_at, updated_at, tags)
                VALUES (?,?,?,?,datetime('now'),datetime('now'),'[]')
            """, (project_id, project_id.replace("-", " ").title(), "", "active"))

        for choice in choices[:5]:
            summary = choice[:120]
            if not summary or summary in existing:
                continue

            decision_id = "dec_" + uuid.uuid4().hex[:8]
            conn.execute("""
                INSERT INTO entities
                  (id, type, source, source_refs, created_at, updated_at,
                   timestamp_start, project_id, actor, confidence,
                   evidence_refs, tags, summary, data)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                decision_id,
                "decision",
                "derived",
                json.dumps([row["id"]]),
                ts(),
                ts(),
                row["timestamp_start"],
                project_id,
                "user",
                confidence,
                json.dumps([row["id"]]),
                json.dumps([project_id] if project_id else []),
                summary,
                json.dumps({
                    "decision_kind": _classify_decision_kind(choice),
                    "context": _extract_context(text, choice),
                    "trigger_refs": [row["id"]],
                    "options_considered": _extract_options(text),
                    "choice_made": choice,
                    "rationale": "",
                    "later_verdict": "unknown",
                    "decision_quality_score": 0.0,
                }),
            ))
            conn.execute(
                "INSERT INTO edges (from_id,to_id,edge_type,confidence,created_at) VALUES (?,?,?,?,?)",
                (row["id"], decision_id, "DERIVED_FROM", confidence, ts()),
            )
            existing.add(summary)
            created += 1

    conn.commit()
    return created


def _extract_structured_choices(text: str) -> list[str]:
    choices: list[str] = []
    in_section = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not in_section:
            if SECTION_HEADER.match(line):
                in_section = True
            continue

        if not line:
            continue
        if HEADING_LINE.match(line):
            break
        if not BULLET_LINE.match(line):
            continue

        choice = BULLET_LINE.sub("", line)
        choice = re.sub(r"\*\*", "", choice).strip()
        choice = re.sub(r"\s+", " ", choice)
        lower_choice = choice.lower()
        if choice and any(marker in lower_choice for marker in STRUCTURED_DECISION_MARKERS) and choice not in choices:
            choices.append(choice)
    return choices


def _extract_context(text: str, choice: str = "") -> str:
    if choice:
        return choice[:300]
    sentences = re.split(r"[.!\n]", text)
    for i, sentence in enumerate(sentences):
        if any(sig in sentence.lower() for sig in DECISION_TRIGGERS):
            start = max(0, i - 1)
            end = min(len(sentences), i + 2)
            return ". ".join(sentences[start:end]).strip()[:300]
    return text[:200]


def _extract_choice(text: str) -> str:
    tl = text.lower()
    for trigger in DECISION_TRIGGERS:
        idx = tl.find(trigger)
        if idx >= 0:
            snippet = text[idx:idx + 180]
            return snippet.split(".")[0].strip()
    return text[:100]


def _extract_options(text: str) -> list[str]:
    options = []
    tl = text.lower()
    for marker in OPTION_MARKERS:
        idx = tl.find(marker)
        if idx >= 0:
            snippet = text[idx:idx + 100].split(".")[0].strip()
            if snippet and snippet not in options:
                options.append(snippet)
    return options[:4]


def _classify_decision_kind(text: str) -> str:
    tl = text.lower()
    if any(word in tl for word in ["tool", "using", "installed", "switched to", "integrated"]):
        return "tool_choice"
    if any(word in tl for word in ["architect", "design", "structure", "stack", "framework"]):
        return "architecture"
    if any(word in tl for word in ["debug", "troubleshoot", "fix", "stabiliz"]):
        return "debug_strategy"
    if any(word in tl for word in ["priority", "focus", "first", "next"]):
        return "priority"
    if any(word in tl for word in ["pivot", "abandon", "stop", "drop", "re-scope"]):
        return "pivot"
    return "workflow"
