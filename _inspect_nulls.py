import sys, os
sys.path.insert(0, r'/home/finan/dev/me-ops')
os.environ['PYTHONIOENCODING'] = 'utf-8'
from core.config import load_project_env
load_project_env()
from core.storage.db import get_conn

conn = get_conn()

print("=== SAMPLE: NULL project_id event summaries (25) ===")
rows = conn.execute(
    "SELECT summary, created_at FROM entities WHERE project_id IS NULL "
    "ORDER BY created_at DESC LIMIT 25"
).fetchall()
for r in rows:
    print(f"  {r['created_at'][:10]}  {r['summary'][:85]}")

print("\n=== TOP PATTERNS IN NULL EVENTS (keyword frequency) ===")
all_nulls = conn.execute(
    "SELECT summary FROM entities WHERE project_id IS NULL"
).fetchall()
from collections import Counter
import re
words = Counter()
for r in all_nulls:
    text = (r['summary'] or '').lower()
    for w in re.findall(r'[a-z]{4,}', text):
        words[w] += 1
stop = {'with','that','this','from','were','have','been','also','into','their',
        'session','primary','focused','involved','user','activities','multiple',
        'using','including','during','after','before','other','then','some',
        'active','work','based','through','which','about','when','both','only',
        'further','making','then','such','each','while','being','these'}
top = [(w,n) for w,n in words.most_common(60) if w not in stop][:30]
print("  Top non-stop words in NULL-project events:")
for w, n in top:
    print(f"    {n:>4}  {w}")

print("\n=== CURRENT test results ===")
import subprocess
r = subprocess.run(['python', '-m', 'unittest', 'discover', '-s', 'tests'],
                   capture_output=True, text=True,
                   cwd=r'/home/finan/dev/me-ops')
print(r.stdout or r.stderr)

conn.close()
