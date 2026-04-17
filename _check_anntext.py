import sys, os
sys.path.insert(0, r'/home/finan/dev/me-ops')
os.environ['PYTHONIOENCODING'] = 'utf-8'
from core.config import load_project_env
load_project_env()
from core.storage.db import get_conn

conn = get_conn()

print("=== raw_pieces_summaries annotation_text samples ===")
rows = conn.execute(
    "SELECT pieces_id, name, annotation_text FROM raw_pieces_summaries "
    "WHERE annotation_text IS NOT NULL ORDER BY rowid DESC LIMIT 10"
).fetchall()
for r in rows:
    text = r['annotation_text'] or ''
    print(f"\nID: {r['pieces_id'][:12]}")
    print(f"Name: {r['name']}")
    print(f"ann_text ({len(text)} chars): [{text[:300]}]")

print("\n\n=== Check annotation_text length distribution ===")
lengths = conn.execute(
    "SELECT length(annotation_text) as l FROM raw_pieces_summaries "
    "WHERE annotation_text IS NOT NULL"
).fetchall()
buckets = {'0': 0, '1-50': 0, '51-200': 0, '201-500': 0, '500+': 0}
for r in lengths:
    l = r['l'] or 0
    if l == 0: buckets['0'] += 1
    elif l <= 50: buckets['1-50'] += 1
    elif l <= 200: buckets['51-200'] += 1
    elif l <= 500: buckets['201-500'] += 1
    else: buckets['500+'] += 1
for k, v in buckets.items():
    print(f"  len {k:>7}: {v:>4} summaries")

print("\n\n=== Check a batch_snapshot response directly via MCP ===")
conn.close()
