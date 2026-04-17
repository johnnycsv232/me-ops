"""
Fix: raw_content in entities is just the summary name — not the full annotation text.
This patches all 526 events to have the real annotation text in raw_content,
then re-classifies, re-extracts decisions + failures, and updates the DB.
No Gemini re-embedding needed — embeddings already include the name which is in summary.
"""
import sys, os, json, hashlib
sys.path.insert(0, r'/home/finan/dev/me-ops')
os.environ['PYTHONIOENCODING'] = 'utf-8'
from core.config import load_project_env
load_project_env()
from core.storage.db import get_conn, ts, EntityRepo
from core.ledger.classify import classify_project, classify_event_kind, extract_tools

conn = get_conn()
repo = EntityRepo(conn)

print("[fix] Loading raw annotation text...")
raw_rows = conn.execute(
    "SELECT pieces_id, name, annotation_text, canonical_id FROM raw_pieces_summaries "
    "WHERE processed=1 AND annotation_text IS NOT NULL AND length(annotation_text) > 10"
).fetchall()
print(f"[fix] {len(raw_rows)} raw summaries with annotation text")

# Build lookup: canonical_id -> annotation_text + name
lookup = {}
for r in raw_rows:
    if r['canonical_id']:
        lookup[r['canonical_id']] = {
            'name': r['name'] or '',
            'text': r['annotation_text'] or '',
            'pieces_id': r['pieces_id'],
        }
print(f"[fix] {len(lookup)} canonical_id mappings")

# Update each event entity with real raw_content
updated = 0
reclassified = 0
for eid, item in lookup.items():
    ent = conn.execute("SELECT * FROM entities WHERE id=? AND type='event'", (eid,)).fetchone()
    if not ent:
        continue
    data = json.loads(ent['data'] or '{}')
    full_text = item['name'] + ' ' + item['text']

    # Re-classify with full text
    new_project_id = classify_project(full_text)
    new_event_kind = classify_event_kind(full_text)
    new_tools = extract_tools(full_text)

    old_project = ent['project_id']
    if new_project_id != old_project:
        reclassified += 1

    # Update entity
    data['raw_content'] = item['text']
    data['event_kind'] = new_event_kind
    data['tools'] = new_tools

    # Ensure project exists
    if new_project_id:
        conn.execute("""INSERT OR IGNORE INTO projects
          (id,name,description,status,created_at,updated_at,tags)
          VALUES (?,?,?,?,datetime('now'),datetime('now'),'[]')""",
          (new_project_id, new_project_id.replace('-',' ').title(), '', 'active'))

    conn.execute("""UPDATE entities SET
        project_id=?, updated_at=?,
        tags=?, data=?, summary=?
        WHERE id=?""", (
        new_project_id,
        ts(),
        json.dumps(([new_project_id] if new_project_id else []) + new_tools[:2]),
        json.dumps(data),
        (item['name'] or item['text'][:120]),
        eid,
    ))
    updated += 1
    if updated % 100 == 0:
        conn.commit()
        print(f"  [fix] {updated}/{len(lookup)} updated...")

conn.commit()
print(f"\n[fix] Updated {updated} events, re-classified {reclassified} to new project")

print("\n[fix] New project breakdown:")
for r in conn.execute(
    "SELECT project_id, COUNT(*) n FROM entities WHERE type='event' "
    "GROUP BY project_id ORDER BY n DESC"
).fetchall():
    print(f"  {(r['project_id'] or 'NULL'):<25} {r['n']:>5}")

# Spot-check raw_content is real now
sample = conn.execute(
    "SELECT id, project_id, data FROM entities WHERE type='event' "
    "AND project_id IS NOT NULL ORDER BY created_at DESC LIMIT 3"
).fetchall()
print("\n[fix] Spot-check raw_content (first 200 chars):")
for r in sample:
    d = json.loads(r['data'] or '{}')
    content = (d.get('raw_content') or '')[:200]
    print(f"  [{r['project_id']}] {content[:150]}")

conn.close()
