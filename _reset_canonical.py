import sys, os, io
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
sys.path.insert(0, str(Path(__file__).resolve().parent))
from core.storage.db import get_conn

confirm = input("Type DESTROY to wipe derived ME-OPS data and requeue raw summaries: ").strip()
if confirm != "DESTROY":
    print("Aborted.")
    raise SystemExit(1)

conn = get_conn()
# Clear canonical layer only — keep raw summaries
conn.execute("DELETE FROM entities")
conn.execute("DELETE FROM cases")
conn.execute("DELETE FROM case_members")
conn.execute("DELETE FROM edges")
conn.execute("DELETE FROM sessions")
conn.execute("DELETE FROM context_metrics")
conn.execute("DELETE FROM briefings")
conn.execute("DELETE FROM interventions")
# Reset processed flag so re-ingest picks them up
conn.execute("UPDATE raw_pieces_summaries SET processed=0, canonical_id=NULL")
conn.commit()
n = conn.execute("SELECT COUNT(*) FROM raw_pieces_summaries").fetchone()[0]
print(f"Reset complete. {n} raw summaries queued for re-ingest.")
conn.close()
