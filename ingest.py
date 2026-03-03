"""
ME-OPS Ingestion Pipeline
=========================
Parses Pieces LTM JSON exports into a normalized DuckDB warehouse.

Tables created:
  - raw_sources: file metadata + SHA256 hashes
  - events: canonical event table with provenance
  - people, tools, models: reference/config tables

Usage:
  python ingest.py [--data-dir /path/to/json/files] [--db /path/to/me_ops.duckdb]
"""

import argparse
import hashlib
import json
import time
from datetime import datetime
from time_utils import local_now, local_date, LOCAL_TZ
from pathlib import Path
from typing import Any

import duckdb
from taxonomy import categorize_event



# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent  # /dev/labs/ where pieces_*.json live
DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"

SOURCE_FILES = [
    "pieces_activities.json",
    "pieces_allocations.json",
    "pieces_anchors.json",
    "pieces_annotations.json",
    "pieces_applications.json",
    "pieces_assets_snippets.json",
    "pieces_backups.json",
    "pieces_conversations_ltm.json",
    "pieces_distributions.json",
    "pieces_formats.json",
    "pieces_hints.json",
    "pieces_models.json",
    "pieces_persons.json",
    "pieces_ranges.json",
    "pieces_shares.json",
    "pieces_tags.json",
    "pieces_user_profile.json",
    "pieces_websites.json",
    "pieces_workstream_summaries.json",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_ts(obj: dict | None, key: str = "value") -> str | None:
    """Extract ISO timestamp from Pieces nested timestamp object."""
    if obj and isinstance(obj, dict) and key in obj:
        return obj[key]
    return None


def safe_str(val: Any, max_len: int = 500) -> str | None:
    """Truncate string to max_len, return None for empty."""
    if val is None:
        return None
    s = str(val)
    s = s[:max_len]  # type: ignore[index]
    return s if s else None


def load_json(path: Path) -> list[dict[str, Any]]:
    """Load a Pieces JSON export, returning the iterable array."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Most files have {"iterable": [...]}
    if isinstance(data, dict):
        result = data.get("iterable", data.get("items", []))
        return result if isinstance(result, list) else []
    if isinstance(data, list):
        return data
    return []


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------
DDL = """
-- Raw source file registry
CREATE TABLE IF NOT EXISTS raw_sources (
    source_file     VARCHAR PRIMARY KEY,
    sha256_hash     VARCHAR NOT NULL,
    record_count    INTEGER NOT NULL,
    byte_size       BIGINT NOT NULL,
    ingested_at     TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- Canonical events table
CREATE TABLE IF NOT EXISTS events (
    event_id        VARCHAR PRIMARY KEY,
    ts_start        TIMESTAMP,
    ts_end          TIMESTAMP,
    source_file     VARCHAR NOT NULL,
    raw_pointer     VARCHAR,
    app_tool        VARCHAR,
    action          VARCHAR NOT NULL,
    target          VARCHAR,
    metadata_json   JSON,
    duration_ms     BIGINT,
    error_signature VARCHAR,
    outcome_label   VARCHAR DEFAULT 'unknown'
);

-- Reference: People
CREATE TABLE IF NOT EXISTS people (
    person_id   VARCHAR PRIMARY KEY,
    name        VARCHAR,
    email       VARCHAR,
    github_url  VARCHAR,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    source_file VARCHAR
);

-- Reference: Tools / Applications
CREATE TABLE IF NOT EXISTS tools (
    tool_id     VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    version     VARCHAR,
    platform    VARCHAR,
    category    VARCHAR,
    source_file VARCHAR
);

-- Reference: AI Models
CREATE TABLE IF NOT EXISTS models (
    model_id    VARCHAR PRIMARY KEY,
    name        VARCHAR,
    provider    VARCHAR,
    foundation  VARCHAR,
    unique_name VARCHAR,
    max_tokens  INTEGER,
    source_file VARCHAR
);

-- Reference: Projects (mined from data)
CREATE TABLE IF NOT EXISTS projects (
    project_id  VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    first_seen  TIMESTAMP,
    last_seen   TIMESTAMP
);

-- Reference: Files (from anchors)
CREATE TABLE IF NOT EXISTS files (
    file_id     VARCHAR PRIMARY KEY,
    fullpath    VARCHAR,
    extension   VARCHAR,
    repo_root   VARCHAR,
    source_file VARCHAR
);

-- Link tables
CREATE TABLE IF NOT EXISTS event_tools (
    event_id    VARCHAR REFERENCES events(event_id),
    tool_id     VARCHAR REFERENCES tools(tool_id),
    PRIMARY KEY (event_id, tool_id)
);

CREATE TABLE IF NOT EXISTS event_files (
    event_id    VARCHAR REFERENCES events(event_id),
    file_id     VARCHAR REFERENCES files(file_id),
    PRIMARY KEY (event_id, file_id)
);

CREATE TABLE IF NOT EXISTS event_projects (
    event_id    VARCHAR REFERENCES events(event_id),
    project_id  VARCHAR REFERENCES projects(project_id),
    PRIMARY KEY (event_id, project_id)
);

CREATE TABLE IF NOT EXISTS event_tags (
    event_id    VARCHAR,
    tag_id      VARCHAR,
    tag_text    VARCHAR,
    PRIMARY KEY (event_id, tag_id)
);

-- Subcategorization table
CREATE TABLE IF NOT EXISTS event_subcategories (
    event_id    VARCHAR PRIMARY KEY REFERENCES events(event_id),
    theme       VARCHAR NOT NULL,
    subcategory VARCHAR NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Ingest functions per source file type
# ---------------------------------------------------------------------------
def compute_duration(ts_start: str | None, ts_end: str | None) -> int | None:
    """Compute duration in ms between two ISO timestamps."""
    if ts_start is None or ts_end is None:
        return None
    try:
        s = datetime.fromisoformat(ts_start.replace("Z", "+00:00"))  # type: ignore[union-attr]
        e = datetime.fromisoformat(ts_end.replace("Z", "+00:00"))  # type: ignore[union-attr]
        return max(0, int((e - s).total_seconds() * 1000))
    except (ValueError, TypeError):
        return None


def detect_error_signature(text: str | None) -> str | None:
    """Extract error signature from text if present."""
    if not text:
        return None
    error_keywords = [
        "error", "failed", "failure", "exception", "troubleshoot",
        "bug", "crash", "broken", "not found", "cannot", "unable",
        "fatal", "critical", "timeout", "refused", "denied",
    ]
    text_lower = text.lower()
    for kw in error_keywords:
        if kw in text_lower:
            # Return first sentence containing the keyword
            for sentence in text.split("."):
                if kw in sentence.lower():
                    trimmed = sentence.strip()
                    return trimmed[:200]  # type: ignore[index]
    return None


def detect_outcome(text: str | None, action: str | None) -> str:
    """Infer outcome label from event text and action."""
    if not text:
        return "unknown"
    text_lower = text.lower()
    if any(w in text_lower for w in ["commit", "push", "deploy", "shipped", "merged", "pr ", "pull request"]):
        return "shipped_code"
    if any(w in text_lower for w in ["created", "saved", "exported", "generated", "built", "designed"]):
        return "shipped_asset"
    if any(w in text_lower for w in ["sent", "replied", "messaged", "posted", "shared"]):
        return "shipped_message"
    return "unknown"


def ingest_activities(records: list[dict], source: str) -> list[tuple]:
    """Parse pieces_activities.json → event tuples."""
    rows = []
    for i, rec in enumerate(records):
        event_id = rec.get("id", f"activity_{i}")
        ts_start = safe_ts(rec.get("created"))
        ts_end = safe_ts(rec.get("updated"))
        app_name = None
        app = rec.get("application")
        if app:
            app_name = app.get("name")

        # Determine target from event type
        event_obj = rec.get("event", {})
        target = None
        action = "activity"
        if "conversation" in event_obj:
            conv = event_obj["conversation"]
            action = "conversation_activity"
            if isinstance(conv, dict):
                inner = conv.get("conversation", {})
                target = inner.get("id") if isinstance(inner, dict) else str(inner)
        elif "workstream" in event_obj:
            action = "workstream_activity"

        mechanism = rec.get("mechanism", "unknown")
        meta = {"mechanism": mechanism, "rank": rec.get("rank")}

        rows.append((
            event_id, ts_start, ts_end, source, f"iterable[{i}]",
            app_name, action, safe_str(target),
            json.dumps(meta), compute_duration(ts_start, ts_end),
            None, "unknown",
        ))
    return rows


def ingest_workstream_summaries(records: list[dict], source: str) -> list[tuple]:
    """Parse pieces_workstream_summaries.json → event tuples."""
    rows = []
    for i, rec in enumerate(records):
        event_id = rec.get("id", f"ws_{i}")
        ts_start = safe_ts(rec.get("created"))
        ts_end = safe_ts(rec.get("updated"))

        # The text content is in linked annotations, not directly here
        # Store event indices as metadata for linking
        events_indices = rec.get("events", {}).get("indices", {})
        event_keys = list(events_indices.keys())
        meta = {
            "event_count": len(events_indices),
            "event_ids": event_keys[:50],  # type: ignore[index]
        }

        rows.append((
            event_id, ts_start, ts_end, source, f"iterable[{i}]",
            None, "workstream_summary", safe_str(f"summary:{event_id}"),
            json.dumps(meta), compute_duration(ts_start, ts_end),
            None, "unknown",
        ))
    return rows


def ingest_websites(records: list[dict], source: str) -> list[tuple]:
    """Parse pieces_websites.json → event tuples."""
    rows = []
    for i, rec in enumerate(records):
        event_id = rec.get("id", f"web_{i}")
        ts_start = safe_ts(rec.get("created"))
        ts_end = safe_ts(rec.get("updated"))
        url = rec.get("url", "")
        interactions = rec.get("interactions", 0)

        meta = {"url": url, "interactions": interactions}

        rows.append((
            event_id, ts_start, ts_end, source, f"iterable[{i}]",
            None, "web_visit", safe_str(url),
            json.dumps(meta), compute_duration(ts_start, ts_end),
            None, "unknown",
        ))
    return rows


def ingest_annotations(records: list[dict], source: str) -> list[tuple]:
    """Parse pieces_annotations.json → event tuples."""
    rows = []
    for i, rec in enumerate(records):
        event_id = rec.get("id", f"ann_{i}")
        ts_start = safe_ts(rec.get("created"))
        ts_end = safe_ts(rec.get("updated"))
        ann_type = rec.get("type", "UNKNOWN").lower()
        text = rec.get("text", "")
        mechanism = rec.get("mechanism", "unknown")

        error_sig = detect_error_signature(text)
        outcome = detect_outcome(text, f"annotation_{ann_type}")

        meta = {"type": ann_type, "mechanism": mechanism, "text_preview": text[:300]}

        rows.append((
            event_id, ts_start, ts_end, source, f"iterable[{i}]",
            None, f"annotation_{ann_type}", safe_str(text, 200),
            json.dumps(meta), compute_duration(ts_start, ts_end),
            error_sig, outcome,
        ))
    return rows


def ingest_anchors(records: list[dict], source: str) -> list[tuple]:
    """Parse pieces_anchors.json → event tuples."""
    rows = []
    for i, rec in enumerate(records):
        event_id = rec.get("id", f"anchor_{i}")
        ts_start = safe_ts(rec.get("created"))
        ts_end = safe_ts(rec.get("updated"))
        anchor_type = rec.get("type", "UNKNOWN")

        # Extract fullpath from points
        fullpath = None
        points = rec.get("points", {}).get("iterable", [])
        if points:
            ref = points[0].get("reference", {})
            fullpath = ref.get("fullpath")

        meta = {"type": anchor_type, "fullpath": fullpath}

        rows.append((
            event_id, ts_start, ts_end, source, f"iterable[{i}]",
            None, "file_reference", safe_str(fullpath),
            json.dumps(meta), compute_duration(ts_start, ts_end),
            None, "unknown",
        ))
    return rows


def ingest_conversations(records: list[dict], source: str) -> list[tuple]:
    """Parse pieces_conversations_ltm.json → event tuples."""
    rows = []
    for i, rec in enumerate(records):
        event_id = rec.get("id", f"conv_{i}")
        ts_start = safe_ts(rec.get("created"))
        ts_end = safe_ts(rec.get("updated"))
        name = rec.get("name", "unnamed_conversation")

        msg_count = len(rec.get("messages", {}).get("indices", {}))
        ann_count = len(rec.get("annotations", {}).get("indices", {}))

        meta = {"message_count": msg_count, "annotation_count": ann_count}

        rows.append((
            event_id, ts_start, ts_end, source, f"iterable[{i}]",
            None, "conversation", safe_str(name),
            json.dumps(meta), compute_duration(ts_start, ts_end),
            None, "unknown",
        ))
    return rows


def ingest_hints(records: list[dict], source: str) -> list[tuple]:
    """Parse pieces_hints.json → event tuples."""
    rows = []
    for i, rec in enumerate(records):
        event_id = rec.get("id", f"hint_{i}")
        ts_start = safe_ts(rec.get("created"))
        ts_end = safe_ts(rec.get("updated"))
        hint_type = rec.get("type", "UNKNOWN").lower()
        text = rec.get("text", "")

        meta = {"type": hint_type, "text": text[:300]}

        rows.append((
            event_id, ts_start, ts_end, source, f"iterable[{i}]",
            None, f"hint_{hint_type}", safe_str(text, 200),
            json.dumps(meta), compute_duration(ts_start, ts_end),
            None, "unknown",
        ))
    return rows


def ingest_ranges(records: list[dict], source: str) -> list[tuple]:
    """Parse pieces_ranges.json → event tuples."""
    rows = []
    for i, rec in enumerate(records):
        event_id = rec.get("id", f"range_{i}")
        ts_start = safe_ts(rec.get("from"))
        ts_end = safe_ts(rec.get("to"))
        created = safe_ts(rec.get("created"))

        summary_ids = list(rec.get("summaries", {}).get("indices", {}).keys())
        meta = {"summary_ids": summary_ids}

        rows.append((
            event_id, created, created, source, f"iterable[{i}]",
            None, "time_range", safe_str(f"{ts_start} → {ts_end}"),
            json.dumps(meta), compute_duration(ts_start, ts_end),
            None, "unknown",
        ))
    return rows


def ingest_assets(records: list[dict], source: str) -> list[tuple]:
    """Parse pieces_assets_snippets.json → event tuples."""
    rows = []
    for i, rec in enumerate(records):
        event_id = rec.get("id", f"asset_{i}")
        ts_start = safe_ts(rec.get("created"))
        ts_end = safe_ts(rec.get("updated"))
        name = rec.get("name", "unnamed_snippet")

        formats = rec.get("formats", {}).get("iterable", [])
        classification = None
        if formats:
            cls = formats[0].get("classification", {})
            classification = cls.get("specific", cls.get("generic"))

        meta = {
            "name": name,
            "classification": classification,
            "mechanism": rec.get("mechanism"),
        }

        rows.append((
            event_id, ts_start, ts_end, source, f"iterable[{i}]",
            None, "code_snippet", safe_str(name),
            json.dumps(meta), compute_duration(ts_start, ts_end),
            None, "shipped_asset",
        ))
    return rows


# ---------------------------------------------------------------------------
# Reference table ingestors
# ---------------------------------------------------------------------------
def ingest_people(con: duckdb.DuckDBPyConnection, records: list[dict], source: str):
    """Load people reference table."""
    for rec in records:
        person_id = rec.get("id")
        if not person_id:
            continue
        platform = rec.get("type", {}).get("platform", {})
        name = platform.get("name")
        email = platform.get("email")

        # Extract GitHub URL from providers
        github_url = None
        providers = platform.get("providers", {}).get("iterable", [])
        for p in providers:
            if p.get("type") == "github":
                profile = p.get("profileData", {})
                github_url = profile.get("html_url")
                break

        con.execute(
            "INSERT OR REPLACE INTO people VALUES (?, ?, ?, ?, ?, ?, ?)",
            [person_id, name, email, github_url,
             safe_ts(platform.get("created")), safe_ts(platform.get("updated")),
             source],
        )


def ingest_tools(con: duckdb.DuckDBPyConnection, records: list[dict], source: str):
    """Load tools reference table from applications."""
    for rec in records:
        tool_id = rec.get("id")
        if not tool_id:
            continue
        con.execute(
            "INSERT OR REPLACE INTO tools VALUES (?, ?, ?, ?, ?, ?)",
            [tool_id, rec.get("name"), rec.get("version"),
             rec.get("platform"), rec.get("capabilities"), source],
        )


def ingest_models_ref(con: duckdb.DuckDBPyConnection, records: list[dict], source: str):
    """Load models reference table."""
    for rec in records:
        model_id = rec.get("id")
        if not model_id:
            continue
        max_tokens = None
        mt = rec.get("maxTokens", {})
        if mt:
            max_tokens = mt.get("total")

        con.execute(
            "INSERT OR REPLACE INTO models VALUES (?, ?, ?, ?, ?, ?, ?)",
            [model_id, rec.get("name"), rec.get("provider"),
             rec.get("foundation"), rec.get("unique"),
             max_tokens, source],
        )


def ingest_files_from_anchors(con: duckdb.DuckDBPyConnection, records: list[dict], source: str):
    """Extract unique files from anchors into files table."""
    seen = set()
    for rec in records:
        points = rec.get("points", {}).get("iterable", [])
        for pt in points:
            ref = pt.get("reference", {})
            fullpath = ref.get("fullpath")
            file_id = ref.get("id")
            if not file_id or file_id in seen:
                continue
            seen.add(file_id)

            ext = None
            repo_root = None
            if fullpath:
                ext = Path(fullpath).suffix.lstrip(".")
                # Derive repo root: look for common project markers
                parts = fullpath.replace("\\", "/").split("/")
                for j, part in enumerate(parts):
                    if part in (".git", "node_modules", "src", "lib"):
                        repo_root = "/".join(parts[:j])
                        break

            con.execute(
                "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?)",
                [file_id, fullpath, ext, repo_root, source],
            )


def ingest_tags_links(con: duckdb.DuckDBPyConnection, records: list[dict[str, Any]]) -> None:
    """Link tags to workstream events via event_tags (chunked bulk insert).

    Args:
        con: Active DuckDB connection.
        records: List of tag records from pieces_tags.json.
    """
    batch: list[tuple[str, str, str]] = []
    for rec in records:
        tag_id = rec.get("id")
        tag_text = rec.get("text", "")
        if not tag_id or not isinstance(tag_id, str):
            continue
            
        ws_events = rec.get("workstream_events", {}).get("indices", {})
        for ws_event_id in ws_events:
            batch.append((ws_event_id, tag_id, str(tag_text)))

    if not batch:
        return

    CHUNK = 5000
    con.execute("BEGIN TRANSACTION")
    for i in range(0, len(batch), CHUNK):
        chunk = batch[i : i + CHUNK]
        placeholders = ",".join(["(?,?,?)"] * len(chunk))
        flat = [v for row in chunk for v in row]
        con.execute(f"INSERT OR IGNORE INTO event_tags VALUES {placeholders}", flat)
    con.execute("COMMIT")


# ---------------------------------------------------------------------------
# Project mining
# ---------------------------------------------------------------------------
PROJECT_PATTERNS = {
    "ironclad": "IronClad",
    "gettupp": "GettUpp",
    "antigravity": "Antigravity IDE",
    "knowhow": "knOWHOW",
    "ai cofounder": "AI Cofounder OS",
    "openclaw": "OpenClaw",
}


def mine_projects(con: duckdb.DuckDBPyConnection):
    """Mine project references from events and tags."""
    # Insert known projects
    for key, name in PROJECT_PATTERNS.items():
        con.execute(
            """INSERT OR IGNORE INTO projects VALUES (?, ?, NULL, NULL)""",
            [key, name],
        )

    # Link events to projects based on target/metadata text matching
    for key, name in PROJECT_PATTERNS.items():
        con.execute(f"""
            INSERT OR IGNORE INTO event_projects
            SELECT event_id, '{key}'
            FROM events
            WHERE LOWER(COALESCE(target, '')) LIKE '%{key}%'
               OR LOWER(COALESCE(CAST(metadata_json AS VARCHAR), '')) LIKE '%{key}%'
        """)

    # Update first_seen / last_seen
    con.execute("""
        UPDATE projects SET
            first_seen = sub.min_ts,
            last_seen = sub.max_ts
        FROM (
            SELECT ep.project_id, MIN(e.ts_start) AS min_ts, MAX(e.ts_start) AS max_ts
            FROM event_projects ep
            JOIN events e ON e.event_id = ep.event_id
            GROUP BY ep.project_id
        ) sub
        WHERE projects.project_id = sub.project_id
    """)


def mine_subcategories(con: duckdb.DuckDBPyConnection) -> None:
    """Assign theme/subcategory to all events based on taxonomy.

    Extracts all events from the DB, runs them through the categorization 
    logic in taxonomy.py, and persists the results in event_subcategories.

    Args:
        con: Active DuckDB connection.
    """
    print("  Categorizing events...")
    events = con.execute("SELECT event_id, target, metadata_json FROM events").fetchall()

    batch: list[tuple[str, str, str]] = []
    for eid, target, meta_json in events:
        meta_text = ""
        if meta_json:
            try:
                # Handle both string and dict types for metadata_json
                if isinstance(meta_json, str):
                    meta_text = meta_json
                else:
                    meta_text = json.dumps(meta_json)
            except Exception:
                meta_text = str(meta_json)

        theme, subcat = categorize_event(target, meta_text)
        batch.append((eid, theme, subcat))

    if batch:
        CHUNK = 5000
        con.execute("BEGIN TRANSACTION")
        for i in range(0, len(batch), CHUNK):
            chunk = batch[i : i + CHUNK]
            placeholders = ",".join(["(?,?,?)"] * len(chunk))
            flat = [v for row in chunk for v in row]
            con.execute(f"INSERT OR IGNORE INTO event_subcategories VALUES {placeholders}", flat)
        con.execute("COMMIT")

    print(f"    Categorized {len(batch)} events")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def run(data_dir: Path, db_path: Path):
    """Execute full ingestion pipeline."""
    print("ME-OPS Ingestion Pipeline")
    print(f"{'='*60}")
    print(f"Data dir : {data_dir}")
    print(f"DB path  : {db_path}")
    print(f"Started  : {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    # Remove stale DB for clean rebuild
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    con.execute("SET enable_progress_bar = true")

    # Create schema
    for stmt in DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    print(f"{'File':<40} {'Records':>8}  {'Size':>10}  {'Events':>8}  Status")
    print(f"{'-'*40} {'-'*8}  {'-'*10}  {'-'*8}  {'-'*10}")

    total_events = 0

    # Dispatch table: source_file → (ingest_fn | None, ref_fn | None)
    dispatch = {
        "pieces_activities.json":           (ingest_activities, None),
        "pieces_workstream_summaries.json":  (ingest_workstream_summaries, None),
        "pieces_websites.json":             (ingest_websites, None),
        "pieces_annotations.json":          (ingest_annotations, None),
        "pieces_anchors.json":              (ingest_anchors, lambda c, r, s: ingest_files_from_anchors(c, r, s)),
        "pieces_conversations_ltm.json":    (ingest_conversations, None),
        "pieces_hints.json":                (ingest_hints, None),
        "pieces_ranges.json":               (ingest_ranges, None),
        "pieces_assets_snippets.json":      (ingest_assets, None),
        "pieces_applications.json":         (None, lambda c, r, s: ingest_tools(c, r, s)),
        "pieces_models.json":               (None, lambda c, r, s: ingest_models_ref(c, r, s)),
        "pieces_persons.json":              (None, lambda c, r, s: ingest_people(c, r, s)),
        "pieces_tags.json":                 (None, lambda c, r, s: ingest_tags_links(c, r)),
        # Config/empty — store hash only
        "pieces_allocations.json":          (None, None),
        "pieces_backups.json":              (None, None),
        "pieces_distributions.json":        (None, None),
        "pieces_formats.json":              (None, None),
        "pieces_shares.json":               (None, None),
        "pieces_user_profile.json":         (None, None),
    }

    for fname in SOURCE_FILES:
        fpath = data_dir / fname
        if not fpath.exists():
            print(f"{fname:<40} {'MISSING':>8}  {'':>10}  {'':>8}  SKIP")
            continue

        t0 = time.time()

        # Compute hash + size
        file_hash = sha256_file(fpath)
        file_size = fpath.stat().st_size
        records = load_json(fpath)
        rec_count = len(records)

        # Register raw source
        con.execute(
            "INSERT OR REPLACE INTO raw_sources VALUES (?, ?, ?, ?, current_timestamp)",
            [fname, file_hash, rec_count, file_size],
        )

        event_fn, ref_fn = dispatch.get(fname, (None, None))
        event_count = 0

        # Run event ingestor
        if event_fn:
            event_rows = event_fn(records, fname)
            event_count = len(event_rows)
            if event_rows:
                placeholders = "(" + ",".join(["?"] * 12) + ")"
                flat = [v for row in event_rows for v in row]
                con.execute(
                    "INSERT OR IGNORE INTO events VALUES " + ",".join([placeholders] * len(event_rows)),
                    flat,
                )

        # Run reference ingestor
        if ref_fn:
            ref_fn(con, records, fname)

        elapsed = time.time() - t0
        total_events += event_count
        size_str = f"{file_size / 1024 / 1024:.1f}MB" if file_size > 1024 * 1024 else f"{file_size / 1024:.0f}KB"
        print(f"{fname:<40} {rec_count:>8}  {size_str:>10}  {event_count:>8}  OK ({elapsed:.1f}s)")

    print(f"\n{'='*60}")
    print(f"Total events ingested: {total_events}")

    # Mine projects
    print("\nMining project references...")
    mine_projects(con)
    proj_count_row = con.execute("SELECT COUNT(*) FROM projects WHERE first_seen IS NOT NULL").fetchone()
    proj_count = proj_count_row[0] if proj_count_row else 0
    print(f"  Projects with evidence: {proj_count}")

    # Link events to tools from activities
    print("Linking events to tools...")
    con.execute("""
        INSERT OR IGNORE INTO event_tools
        SELECT e.event_id, t.tool_id
        FROM events e
        JOIN tools t ON e.app_tool = t.name
        WHERE e.app_tool IS NOT NULL
    """)
    tool_links_row = con.execute("SELECT COUNT(*) FROM event_tools").fetchone()
    tool_links = tool_links_row[0] if tool_links_row else 0
    print(f"  Event-tool links: {tool_links}")

    # Mine subcategories
    mine_subcategories(con)

    # Final stats
    print(f"\n{'='*60}")
    print("FINAL TABLE COUNTS:")
    for table in ["raw_sources", "events", "people", "tools", "models",
                   "projects", "files", "event_tools", "event_files",
                   "event_projects", "event_tags", "event_subcategories"]:
        count_row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        count = count_row[0] if count_row else 0
        print(f"  {table:<20} {count:>8}")

    con.close()
    print(f"\n✅ Database written to: {db_path}")
    print(f"   Completed: {datetime.now(timezone.utc).isoformat()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ME-OPS Ingestion Pipeline")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR,
                        help="Directory containing pieces_*.json files")
    parser.add_argument("--db", type=Path, default=DB_PATH,
                        help="Output DuckDB database path")
    args = parser.parse_args()
    run(args.data_dir, args.db)
