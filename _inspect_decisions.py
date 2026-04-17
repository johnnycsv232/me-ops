import sys, os
sys.path.insert(0, r'C:\Users\finan\me-ops')
os.environ['PYTHONIOENCODING'] = 'utf-8'
from core.config import load_project_env
load_project_env()
from core.storage.db import get_conn
import json

conn = get_conn()

print("=== SAMPLE raw_content from events (10) ===")
rows = conn.execute(
    "SELECT id, project_id, summary, data FROM entities WHERE type='event' "
    "ORDER BY created_at DESC LIMIT 10"
).fetchall()
for r in rows:
    d = json.loads(r['data'] or '{}')
    content = (d.get('raw_content') or '')[:300]
    print(f"\n[{r['project_id'] or 'NULL'}] {r['summary'][:60]}")
    print(f"  {content[:200]}")

print("\n\n=== DECISION LANGUAGE SEARCH in raw_content ===")
decision_phrases = [
    "decided to", "user decided", "chose to", "switched to",
    "opted for", "pivoted", "migrated to", "transitioned to",
    "refined the scope", "re-scoped", "shifted focus",
]
hits = {}
all_events = conn.execute(
    "SELECT id, project_id, data FROM entities WHERE type='event'"
).fetchall()
for r in all_events:
    d = json.loads(r['data'] or '{}')
    text = (d.get('raw_content') or '').lower()
    for phrase in decision_phrases:
        if phrase in text:
            hits.setdefault(phrase, []).append(r['project_id'] or 'NULL')

print(f"  Decision phrase hits across {len(all_events)} events:")
for phrase, projs in sorted(hits.items(), key=lambda x: -len(x[1])):
    print(f"  {len(projs):>4}x  '{phrase}'")

if hits:
    # Show one example
    sample_phrase = max(hits, key=lambda p: len(hits[p]))
    sample_event = next(
        r for r in all_events
        if sample_phrase in (json.loads(r['data'] or '{}').get('raw_content') or '').lower()
    )
    d = json.loads(sample_event['data'] or '{}')
    print(f"\n  Sample event with '{sample_phrase}':")
    print(f"  {(d.get('raw_content') or '')[:400]}")

print("\n\n=== SESSION fragmentation distribution ===")
frags = conn.execute(
    "SELECT tool_switch_count, fragmentation_score, context_load_score, "
    "project_id FROM sessions ORDER BY context_load_score DESC"
).fetchall()
print(f"  Total sessions: {len(frags)}")
buckets = {'0.0-0.2': 0, '0.2-0.4': 0, '0.4-0.6': 0, '0.6-0.8': 0, '0.8+': 0}
for r in frags:
    s = r['context_load_score'] or 0
    if s < 0.2: buckets['0.0-0.2'] += 1
    elif s < 0.4: buckets['0.2-0.4'] += 1
    elif s < 0.6: buckets['0.4-0.6'] += 1
    elif s < 0.8: buckets['0.6-0.8'] += 1
    else: buckets['0.8+'] += 1
for k, v in buckets.items():
    print(f"  context_load {k}: {v:>4} sessions")

print(f"\n  Max context_load_score: {max(r['context_load_score'] or 0 for r in frags):.3f}")
print(f"  Max tool_switch_count: {max(r['tool_switch_count'] or 0 for r in frags)}")
top5 = sorted(frags, key=lambda r: r['context_load_score'] or 0, reverse=True)[:5]
for r in top5:
    print(f"  [{r['project_id'] or '?'}] switches:{r['tool_switch_count']} "
          f"frag:{r['fragmentation_score']:.2f} load:{r['context_load_score']:.2f}")

conn.close()
