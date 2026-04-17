"""
ME-OPS v2 — Legacy CLI wrapper.
Use run_phase2.py for the active pipeline; this file keeps a few helper commands.

Usage:
  python run.py               # delegate to run_phase2.py full
  python run.py sync          # delegate to active ingest path
  python run.py cases         # build cases only
  python run.py brief         # print daily briefing
  python run.py brief incident "symptom text"
  python run.py brief project openclaw
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from core.storage.db import init_db, get_conn
from core.heuristics.seed import seed_heuristics


def cmd_sync():
    from pieces_bridge.ingest_p2 import run_phase2_ingest
    print("[legacy] run.py sync delegates to pieces_bridge.ingest_p2.run_phase2_ingest()")
    run_phase2_ingest()


def cmd_cases():
    conn = get_conn()
    from core.cases.failure_chain import lift_failures_and_outcomes, build_failure_chains
    print("[pipeline] Lifting failures and outcomes from events...")
    f, o = lift_failures_and_outcomes(conn)
    print(f"[pipeline] Lifted: {f} failures, {o} outcomes")
    print("[pipeline] Building failure chains...")
    case_ids = build_failure_chains(conn)
    print(f"[pipeline] Built {len(case_ids)} failure chain cases")
    conn.close()


def cmd_brief(mode="daily", arg=""):
    from core.briefing.daily import (
        generate_daily_briefing,
        generate_incident_briefing,
        generate_project_briefing,
    )
    if mode == "incident":
        print(generate_incident_briefing(arg or "connection refused"))
    elif mode == "project":
        print(generate_project_briefing(arg or "openclaw"))
    else:
        print(generate_daily_briefing())


def cmd_stats():
    conn = get_conn()
    print("\n=== ME-OPS v2 — Database Stats ===")
    tables = ["entities","cases","heuristics","briefings","edges",
              "raw_pieces_summaries","projects","sessions"]
    for t in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:<30} {n:>6}")
        except Exception:
            pass
    print("\n  Entities by type:")
    rows = conn.execute(
        "SELECT type, COUNT(*) as n FROM entities GROUP BY type ORDER BY n DESC"
    ).fetchall()
    for r in rows:
        print(f"    {r['type']:<20} {r['n']:>6}")
    print("\n  Events by project:")
    rows = conn.execute(
        "SELECT project_id, COUNT(*) as n FROM entities WHERE type='event' "
        "GROUP BY project_id ORDER BY n DESC"
    ).fetchall()
    for r in rows:
        print(f"    {(r['project_id'] or 'unknown'):<20} {r['n']:>6}")
    conn.close()


def run_full_pipeline():
    print("[legacy] run.py is superseded; delegating to run_phase2.py full")
    from run_phase2 import run_phase2
    run_phase2()


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        run_full_pipeline()
    elif args[0] == "sync":
        init_db()
        cmd_sync()
    elif args[0] == "cases":
        cmd_cases()
    elif args[0] == "brief":
        mode = args[1] if len(args) > 1 else "daily"
        arg = args[2] if len(args) > 2 else ""
        cmd_brief(mode, arg)
    elif args[0] == "stats":
        cmd_stats()
    elif args[0] == "seed":
        init_db()
        seed_heuristics()
        print("Done.")
    else:
        print(f"Unknown command: {args[0]}")
        print(__doc__)
