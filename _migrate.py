import sys, os
sys.path.insert(0, r'C:\Users\finan\me-ops')
os.environ['PYTHONIOENCODING'] = 'utf-8'
from core.storage.db import get_conn

conn = get_conn()

# Add missing columns to context_metrics
migrations = [
    "ALTER TABLE context_metrics ADD COLUMN fragmentation_score REAL DEFAULT 0.0",
    "ALTER TABLE context_metrics ADD COLUMN concurrent_projects INTEGER DEFAULT 0",
    "ALTER TABLE context_metrics ADD COLUMN session_fragments INTEGER DEFAULT 0",
    "ALTER TABLE context_metrics ADD COLUMN unresolved_branches INTEGER DEFAULT 0",
    "ALTER TABLE context_metrics ADD COLUMN rereads INTEGER DEFAULT 0",
    "ALTER TABLE context_metrics ADD COLUMN minutes_no_artifact INTEGER DEFAULT 0",
    "ALTER TABLE context_metrics ADD COLUMN collapse_score REAL DEFAULT 0.0",
]
for sql in migrations:
    try:
        conn.execute(sql)
        print(f"OK: {sql[:60]}")
    except Exception as e:
        if "duplicate column" in str(e).lower():
            print(f"skip (exists): {sql[30:60]}")
        else:
            print(f"ERR: {e}")

conn.commit()

# Verify columns
cols = [r[1] for r in conn.execute("PRAGMA table_info(context_metrics)").fetchall()]
print(f"\ncontext_metrics columns: {cols}")
conn.close()
