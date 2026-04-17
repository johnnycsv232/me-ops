import sys, json
sys.path.insert(0, r'C:\Users\finan\me-ops')
from core.storage.db import get_conn
conn = get_conn()

# Check what we have
total = conn.execute("SELECT COUNT(*) FROM raw_pieces_summaries").fetchone()[0]
with_text = conn.execute("SELECT COUNT(*) FROM raw_pieces_summaries WHERE annotation_text IS NOT NULL AND length(annotation_text) > 10").fetchone()[0]
print(f"Total summaries: {total}, with annotation_text: {with_text}")

# Sample
rows = conn.execute("SELECT pieces_id, name, annotation_text FROM raw_pieces_summaries LIMIT 5").fetchall()
for r in rows:
    print(f"\nID: {r['pieces_id'][:12]}")
    print(f"Name: {r['name']}")
    print(f"Ann text ({len(r['annotation_text'] or '')} chars): {(r['annotation_text'] or '')[:150]}")

# Check entities data field
e = conn.execute("SELECT id, project_id, summary, data FROM entities LIMIT 3").fetchall()
for row in e:
    d = json.loads(row['data'] or '{}')
    print(f"\nEntity {row['id']}: proj={row['project_id']} summary={row['summary'][:80]}")
    print(f"  raw_content: {(d.get('raw_content') or '')[:100]}")
