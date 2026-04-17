"""
Phase 2 ingestion — re-classifies from local raw cache + fetches new from MCP.
"""
import json, hashlib, uuid, sys, io, os
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.storage.db import get_conn, init_db, ts, EntityRepo
from core.ledger.classify import classify_project, classify_event_kind, extract_tools

ANNOTATION_TYPE_PRIORITY = {
    "SUMMARY": 0,
    "DESCRIPTION": 1,
    "COMMENT": 2,
}


def _chunked(values: list[str], size: int = 50):
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _annotation_ids(item: dict) -> list[str]:
    return list(((item.get("annotations") or {}).get("indices") or {}).keys())


def _summary_ids_for_annotation(annotation: dict) -> list[str]:
    summaries = annotation.get("summaries") or {}
    ids = list((summaries.get("indices") or {}).keys())
    for summary in summaries.get("iterable") or []:
        sid = summary.get("id")
        if sid:
            ids.append(sid)
    return list(dict.fromkeys(ids))


def _annotation_sort_key(annotation: dict) -> tuple[int, int]:
    text = (annotation.get("text") or "").strip()
    return (ANNOTATION_TYPE_PRIORITY.get(annotation.get("type"), 99), -len(text))


def _merge_annotation_texts(annotations: list[dict]) -> str:
    parts: list[str] = []
    for annotation in sorted(annotations, key=_annotation_sort_key):
        text = (annotation.get("text") or "").strip()
        if len(text) < 20 or text in parts:
            continue
        parts.append(text)
    if not parts:
        return ""
    return "\n\n".join(parts[:2])


def _build_annotation_map(items: list[dict]) -> dict[str, list[dict]]:
    from pieces_bridge.mcp_client import fetch_annotations_batch_mcp

    annotation_ids: list[str] = []
    for item in items:
        annotation_ids.extend(_annotation_ids(item))
    annotation_ids = list(dict.fromkeys(annotation_ids))
    if not annotation_ids:
        return {}

    annotations: list[dict] = []
    for chunk in _chunked(annotation_ids, 50):
        annotations.extend(fetch_annotations_batch_mcp(chunk))

    by_summary: dict[str, list[dict]] = {}
    for annotation in annotations:
        for summary_id in _summary_ids_for_annotation(annotation):
            by_summary.setdefault(summary_id, []).append(annotation)
    return by_summary


def _extract_summary_text(item: dict, annotation_map: dict[str, list[dict]] | None = None) -> str:
    summary_id = item.get("id", "")
    if summary_id and annotation_map:
        rich_text = _merge_annotation_texts(annotation_map.get(summary_id, []))
        if rich_text:
            return rich_text
    anns = (item.get("annotations") or {}).get("iterable", [])
    for ann in anns:
        t = ann.get("text", "")
        if t and len(t) > 20:
            return t
    return item.get("name", "")


def _extract_name(item: dict, text: str) -> str:
    name = (item.get("name") or "").strip()
    if name:
        return name
    for line in text.splitlines():
        clean = line.strip().lstrip("#*- ").strip()
        if clean:
            return clean[:120]
    return ""


def _extract_created(item: dict) -> str:
    c = item.get("created", {})
    if isinstance(c, dict):
        return c.get("value", ts())
    return ts()


def refresh_cached_summary_texts(conn, min_length: int = 120) -> int:
    """Hydrate short cached summaries with rich annotation bodies from MCP."""
    from pieces_bridge.mcp_client import fetch_summaries_batch_mcp

    rows = conn.execute("""
        SELECT pieces_id
        FROM raw_pieces_summaries
        WHERE annotation_text IS NULL OR length(annotation_text) < ?
    """, (min_length,)).fetchall()
    target_ids = [row["pieces_id"] for row in rows]
    if not target_ids:
        return 0

    refreshed = 0
    for chunk in _chunked(target_ids, 50):
        items = fetch_summaries_batch_mcp(chunk)
        annotation_map = _build_annotation_map(items)
        for item in items:
            pieces_id = item.get("id", "")
            if not pieces_id:
                continue
            text = _extract_summary_text(item, annotation_map)
            if len(text) < 20:
                continue
            name = _extract_name(item, text)
            conn.execute("""
                UPDATE raw_pieces_summaries
                SET name=COALESCE(NULLIF(?, ''), name),
                    updated_at=?,
                    annotation_text=?,
                    processed=0,
                    canonical_id=NULL
                WHERE pieces_id=?
            """, (name, _extract_created(item), text, pieces_id))
            refreshed += 1
        conn.commit()
    return refreshed


def reingest_from_mcp_items(items: list, conn, batch_id: str, annotation_map: dict[str, list[dict]] | None = None) -> int:
    repo = EntityRepo(conn)
    count = 0
    for item in items:
        pieces_id = item.get("id", "")
        if not pieces_id:
            continue
        text = _extract_summary_text(item, annotation_map)
        name = _extract_name(item, text)
        created = _extract_created(item)
        if not text:
            continue
        full_text = name + " " + text
        project_id = classify_project(full_text)
        event_kind = classify_event_kind(full_text)
        tools_found = extract_tools(full_text)
        conn.execute("""
            INSERT OR REPLACE INTO raw_pieces_summaries
              (pieces_id, batch_id, name, created_at, annotation_text, processed)
            VALUES (?,?,?,?,?,1)
        """, (pieces_id, batch_id, name, created, text))
        event_id = "evt_" + hashlib.md5(pieces_id.encode()).hexdigest()[:8]
        repo.upsert({
            "id": event_id, "type": "event", "source": "pieces",
            "source_refs": [pieces_id], "created_at": created,
            "project_id": project_id, "actor": "user", "confidence": 0.85,
            "tags": ([project_id] if project_id else []) + tools_found[:3],
            "summary": (name or text[:120]),
            "event_kind": event_kind, "raw_content": text,
            "pieces_event_id": pieces_id, "tools": tools_found,
        })
        conn.execute("UPDATE raw_pieces_summaries SET canonical_id=? WHERE pieces_id=?",
                     (event_id, pieces_id))
        count += 1
    conn.commit()
    return count


def reingest_from_raw_cache(conn) -> int:
    """Re-classify all raw summaries already in DB — no MCP calls needed."""
    repo = EntityRepo(conn)
    rows = conn.execute("""
        SELECT pieces_id, name, annotation_text, created_at
        FROM raw_pieces_summaries
        WHERE processed=0 AND annotation_text IS NOT NULL
          AND annotation_text NOT LIKE '[fetch_error%'
          AND length(annotation_text) > 10
    """).fetchall()
    count = 0
    for row in rows:
        pieces_id = row["pieces_id"]
        name = row["name"] or ""
        text = row["annotation_text"] or ""
        created = row["created_at"] or ts()
        full_text = name + " " + text
        project_id = classify_project(full_text)
        event_kind = classify_event_kind(full_text)
        tools_found = extract_tools(full_text)
        event_id = "evt_" + hashlib.md5(pieces_id.encode()).hexdigest()[:8]
        repo.upsert({
            "id": event_id, "type": "event", "source": "pieces",
            "source_refs": [pieces_id], "created_at": created,
            "project_id": project_id, "actor": "user", "confidence": 0.85,
            "tags": ([project_id] if project_id else []) + tools_found[:3],
            "summary": (name or text[:120]),
            "event_kind": event_kind, "raw_content": text,
            "pieces_event_id": pieces_id, "tools": tools_found,
        })
        conn.execute("UPDATE raw_pieces_summaries SET processed=1, canonical_id=? WHERE pieces_id=?",
                     (event_id, pieces_id))
        count += 1
        if count % 100 == 0:
            conn.commit()
            print(f"  [cache] {count}/{len(rows)} re-classified...")
    conn.commit()
    return count


def fetch_new_from_mcp(conn, batch_id: str) -> int:
    """Fetch only summaries not already in raw cache."""
    from pieces_bridge.mcp_client import fetch_all_summary_ids_mcp, fetch_summaries_batch_mcp
    existing = set(r[0] for r in conn.execute(
        "SELECT pieces_id FROM raw_pieces_summaries WHERE processed=1"
    ).fetchall())
    all_ids = fetch_all_summary_ids_mcp()
    new_ids = [i for i in all_ids if i not in existing]
    print(f"  [mcp] {len(all_ids)} total, {len(new_ids)} new to fetch")
    total = 0
    for i in range(0, len(new_ids), 50):
        chunk = new_ids[i:i+50]
        items = fetch_summaries_batch_mcp(chunk)
        annotation_map = _build_annotation_map(items)
        n = reingest_from_mcp_items(items, conn, batch_id, annotation_map=annotation_map)
        total += n
    return total


def run_phase2_ingest():
    init_db()
    conn = get_conn()
    conn.execute("DELETE FROM entities WHERE type='event' AND (project_id IS NULL OR project_id='unknown')")
    conn.execute("DELETE FROM raw_pieces_summaries WHERE annotation_text LIKE '[fetch_error%'")
    conn.commit()
    batch_id = "batch_p2_" + uuid.uuid4().hex[:6]
    conn.execute("INSERT OR REPLACE INTO raw_import_batches (id,imported_at,source,status) VALUES (?,?,?,?)",
                 (batch_id, ts(), "pieces_mcp_cache", "running"))
    conn.commit()

    print("[p2-ingest] Refreshing short cached summaries from MCP...")
    n_refreshed = refresh_cached_summary_texts(conn)
    print(f"[p2-ingest] Enriched cache: {n_refreshed} summaries")

    print("[p2-ingest] Re-classifying from local cache...")
    n_cache = reingest_from_raw_cache(conn)
    print(f"[p2-ingest] Cache: {n_cache} events classified")

    print("[p2-ingest] Fetching new summaries from MCP...")
    try:
        n_new = fetch_new_from_mcp(conn, batch_id)
        print(f"[p2-ingest] MCP new: {n_new} events")
    except Exception as e:
        print(f"[p2-ingest] MCP fetch skipped ({e})")
        n_new = 0

    total = n_cache + n_new
    conn.execute("UPDATE raw_import_batches SET status='complete', record_count=? WHERE id=?",
                 (total, batch_id))
    conn.commit()
    print(f"\n[p2-ingest] Done. {total} events ingested.")
    print("\nProject breakdown:")
    rows = conn.execute(
        "SELECT project_id, COUNT(*) as n FROM entities WHERE type='event' "
        "GROUP BY project_id ORDER BY n DESC"
    ).fetchall()
    for r in rows:
        print(f"  {(r['project_id'] or 'unknown'):<30} {r['n']:>5}")
    conn.close()


if __name__ == "__main__":
    run_phase2_ingest()
