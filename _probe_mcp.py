"""
Probe actual MCP batch_snapshot response to find where the TLDR text lives.
"""
import sys, os, json, time
sys.path.insert(0, r'/home/finan/dev/me-ops')
os.environ['PYTHONIOENCODING'] = 'utf-8'
from core.config import load_project_env
load_project_env()
from core.storage.db import get_conn
from pieces_bridge.mcp_client import fetch_summaries_batch_mcp, fetch_all_summary_ids_mcp

conn = get_conn()

# Get 3 known IDs with rich content we saw earlier (from the deep pull)
# We know summary "IronClad Plan & Stack Check" exists with full TLDR
ids = conn.execute(
    "SELECT pieces_id FROM raw_pieces_summaries LIMIT 3"
).fetchall()
sample_ids = [r['pieces_id'] for r in ids]
print(f"Fetching {len(sample_ids)} summaries from MCP batch_snapshot...")

items = fetch_summaries_batch_mcp(sample_ids)
print(f"Got {len(items)} items back")

if items:
    item = items[0]
    print(f"\nTop-level keys: {list(item.keys())}")
    # Drill into annotations
    anns = item.get('annotations') or {}
    print(f"annotations keys: {list(anns.keys())}")
    iterable = anns.get('iterable') or []
    print(f"annotations.iterable count: {len(iterable)}")
    if iterable:
        for i, ann in enumerate(iterable[:3]):
            print(f"\n  ann[{i}] keys: {list(ann.keys())}")
            print(f"  ann[{i}].type: {ann.get('type')}")
            text = ann.get('text') or ''
            print(f"  ann[{i}].text ({len(text)} chars): [{text[:300]}]")

    # Also check ranges/workstreamSummariesVector
    for key in ['ranges', 'workstreamSummariesVector', 'events', 'sources', 'processing']:
        val = item.get(key)
        if val:
            print(f"\n  {key}: {str(val)[:100]}")

    print(f"\nFull item (first 800 chars):")
    print(json.dumps(item)[:800])

conn.close()
