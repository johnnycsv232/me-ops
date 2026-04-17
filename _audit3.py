import sys, pathlib, re
sys.path.insert(0, r'C:\Users\finan\me-ops')

root = pathlib.Path(r'C:\Users\finan\me-ops')

print("=== HARDCODED API KEYS / SECRETS SCAN ===")
key_pattern = re.compile(r'AIza[A-Za-z0-9_\-]{35}')
for f in root.rglob('*.py'):
    text = f.read_text(encoding='utf-8', errors='replace')
    for m in key_pattern.finditer(text):
        line_no = text[:m.start()].count('\n') + 1
        print(f"  {f.relative_to(root)}:{line_no}  KEY={m.group()}")

print("\n=== HARDCODED PORTS ===")
port_pattern = re.compile(r'39[0-9]{3}')
for f in root.rglob('*.py'):
    text = f.read_text(encoding='utf-8', errors='replace')
    for m in port_pattern.finditer(text):
        line_no = text[:m.start()].count('\n') + 1
        ctx = text.splitlines()[line_no-1].strip()[:80]
        print(f"  {f.relative_to(root)}:{line_no}  {ctx}")

print("\n=== HARDCODED PATHS ===")
path_pattern = re.compile(r'C:\\\\Users\\\\finan|C:/Users/finan|C:\\Users\\finan')
for f in root.rglob('*.py'):
    text = f.read_text(encoding='utf-8', errors='replace')
    for m in path_pattern.finditer(text):
        line_no = text[:m.start()].count('\n') + 1
        ctx = text.splitlines()[line_no-1].strip()[:80]
        print(f"  {f.relative_to(root)}:{line_no}  {ctx}")

print("\n=== requirements.txt ===")
req = (root / 'requirements.txt').read_text()
print(req)

print("\n=== INGEST.PY STATUS (first 30 lines) ===")
ing = (root / 'pieces_bridge' / 'ingest.py').read_text(encoding='utf-8', errors='replace')
for i, line in enumerate(ing.splitlines()[:30], 1):
    print(f"  {i:3}: {line}")

print("\n=== MCP_CLIENT.PY KEY LINES ===")
mcp = (root / 'pieces_bridge' / 'mcp_client.py').read_text(encoding='utf-8', errors='replace')
for i, line in enumerate(mcp.splitlines(), 1):
    if any(k in line for k in ['PIECES_HOST','PIECES_PORT','SSE_PATH','39300']):
        print(f"  {i:3}: {line}")

print("\n=== RUN_PHASE2.PY KEY LINES ===")
rp = (root / 'run_phase2.py').read_text(encoding='utf-8', errors='replace')
for i, line in enumerate(rp.splitlines(), 1):
    if any(k in line for k in ['GEMINI','API_KEY','AIza','environ','os.environ']):
        print(f"  {i:3}: {line}")

print("\n=== GEMINI_EMBED.PY KEY LINES ===")
ge = (root / 'core' / 'recall' / 'gemini_embed.py').read_text(encoding='utf-8', errors='replace')
for i, line in enumerate(ge.splitlines(), 1):
    if any(k in line for k in ['GEMINI','API_KEY','AIza','EMBED_MODEL','BATCH_SIZE','BATCH_DELAY','EMBED_DIMS']):
        print(f"  {i:3}: {line}")

print("\n=== CLASSIFY.PY PROJECT SIGNALS COUNT ===")
clf = (root / 'core' / 'ledger' / 'classify.py').read_text(encoding='utf-8', errors='replace')
projects_section = re.findall(r'"([a-z-]+)": \[', clf)
print(f"  {len(projects_section)} project keys: {projects_section}")

print("\n=== DECISION_REPLAY.PY TRIGGERS COUNT ===")
dr = (root / 'core' / 'cases' / 'decision_replay.py').read_text(encoding='utf-8', errors='replace')
triggers = re.findall(r'"([^"]+)"', dr[:2000])
print(f"  {len(triggers)} trigger phrases found in first 2000 chars")

print("\n=== DB: NULL project_id check ===")
import sqlite3
conn = sqlite3.connect(r'C:\Users\finan\me-ops\meops.db')
conn.row_factory = sqlite3.Row
r = conn.execute("SELECT COUNT(*) FROM entities WHERE project_id IS NULL").fetchone()
print(f"  entities with NULL project_id: {r[0]}")
r = conn.execute("SELECT COUNT(*) FROM entities WHERE project_id='unknown'").fetchone()
print(f"  entities with project_id='unknown': {r[0]}")
r = conn.execute("SELECT COUNT(*) FROM cases WHERE project_id IS NULL").fetchone()
print(f"  cases with NULL project_id: {r[0]}")
r = conn.execute("SELECT COUNT(*) FROM raw_pieces_summaries").fetchone()
print(f"  raw_pieces_summaries total: {r[0]}")
conn.close()
print("DONE")
