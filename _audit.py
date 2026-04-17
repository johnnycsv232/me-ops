import sys, os, sqlite3, json, struct
sys.path.insert(0, r'C:\Users\finan\me-ops')

print("=== PYTHON ===")
print("version:", sys.version)
print("executable:", sys.executable)
print("encoding:", sys.stdout.encoding)

print("\n=== ENV VARS ===")
for k in ['GEMINI_API_KEY','MEOPS_DB','PYTHONIOENCODING']:
    v = os.environ.get(k, 'NOT SET')
    print(f"  {k} = {v[:20]}..." if len(v) > 20 else f"  {k} = {v}")

print("\n=== DB ===")
db = r'C:\Users\finan\me-ops\meops.db'
print("exists:", os.path.exists(db))
print("size:", os.path.getsize(db), "bytes")

conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

print("\n=== TABLE ROW COUNTS ===")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
for t in tables:
    n = conn.execute(f"SELECT COUNT(*) FROM {t['name']}").fetchone()[0]
    print(f"  {t['name']:<35} {n:>6}")

print("\n=== ENTITIES BY TYPE ===")
for r in conn.execute("SELECT type, COUNT(*) n FROM entities GROUP BY type ORDER BY n DESC").fetchall():
    print(f"  {r['type']:<20} {r['n']:>5}")

print("\n=== EVENTS BY PROJECT ===")
for r in conn.execute("SELECT project_id, COUNT(*) n FROM entities WHERE type='event' GROUP BY project_id ORDER BY n DESC").fetchall():
    print(f"  {r['project_id'] or 'NULL':<25} {r['n']:>5}")

print("\n=== EMBEDDING SAMPLE ===")
r = conn.execute("SELECT entity_id, entity_type, model, length(vector) bytes FROM embeddings LIMIT 3").fetchall()
for row in r:
    print(f"  {row['entity_id'][:12]} type:{row['entity_type']} model:{row['model']} vec_bytes:{row['bytes']}")

print("\n=== CASES ===")
for r in conn.execute("SELECT case_kind, COUNT(*) n FROM cases GROUP BY case_kind").fetchall():
    print(f"  {r['case_kind']:<25} {r['n']:>5}")

print("\n=== HEURISTICS ===")
for r in conn.execute("SELECT id, heuristic_kind, confidence, utility_score, statement FROM heuristics ORDER BY utility_score DESC").fetchall():
    print(f"  {r['id']} [{r['heuristic_kind']}] conf:{r['confidence']} util:{r['utility_score']}")
    print(f"    {r['statement'][:80]}")

print("\n=== RAW CACHE ===")
r = conn.execute("SELECT COUNT(*) total, SUM(CASE WHEN processed=1 THEN 1 ELSE 0 END) proc, SUM(CASE WHEN annotation_text LIKE '[fetch_error%' THEN 1 ELSE 0 END) bad FROM raw_pieces_summaries").fetchone()
print(f"  total:{r[0]} processed:{r[1]} bad_fetch:{r[2]} unprocessed:{r[0]-r[1]}")

print("\n=== SESSIONS FRAGMENTATION SAMPLE ===")
for r in conn.execute("SELECT id, project_id, tool_switch_count, fragmentation_score, context_load_score FROM sessions ORDER BY context_load_score DESC LIMIT 5").fetchall():
    print(f"  {r['id']} proj:{r['project_id'] or '?'} switches:{r['tool_switch_count']} frag:{r['fragmentation_score']:.2f} load:{r['context_load_score']:.2f}")

print("\n=== SCHEMA COLUMNS context_metrics ===")
for r in conn.execute("PRAGMA table_info(context_metrics)").fetchall():
    print(f"  col:{r['name']} type:{r['type']}")

print("\n=== PROJECTS ===")
for r in conn.execute("SELECT id, name, status FROM projects ORDER BY id").fetchall():
    print(f"  {r['id']:<25} {r['name']}")

conn.close()
print("\nDONE")
