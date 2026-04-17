"""ME-OPS Phase 2+3 — cases + Gemini semantic recall."""
import sys, os, io, json
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
elif sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from core.storage.db import init_db, get_conn
from core.heuristics.seed import seed_heuristics


def _hard_reset(conn):
    """Wipe all derived layers, preserve raw summaries."""
    # Order matters — clear FKs from child to parent
    conn.execute("DELETE FROM context_metrics")
    conn.execute("DELETE FROM case_members")
    conn.execute("DELETE FROM edges")
    conn.execute("DELETE FROM briefings")
    conn.execute("DELETE FROM interventions")
    conn.execute("DELETE FROM entities WHERE source='derived'")
    conn.execute("UPDATE entities SET session_id=NULL")   # clear before deleting sessions
    conn.execute("DELETE FROM sessions")
    conn.execute("DELETE FROM cases")
    conn.execute("DELETE FROM embeddings")
    # Reset raw cache processed flag so re-ingest runs
    conn.execute("UPDATE raw_pieces_summaries SET processed=0, canonical_id=NULL "
                 "WHERE annotation_text IS NOT NULL AND annotation_text NOT LIKE '[fetch_error%'")
    conn.commit()
    print("[reset] Derived layers cleared. Raw cache reset for re-ingest.")


def _skip_embedding() -> bool:
    return os.environ.get("MEOPS_SKIP_EMBED", "").strip() == "1"


def run_phase2():
    print("\n" + "="*60)
    print("  ME-OPS v2 -- Phase 2+3 Pipeline")
    print("="*60)
    init_db()
    seed_heuristics()
    from core.recall.gemini_embed import init_embed_table
    init_embed_table()

    conn = get_conn()
    _hard_reset(conn)
    conn.close()

    print("\n[1/8] Ingesting from local cache (re-classify) + MCP delta...")
    from pieces_bridge.ingest_p2 import run_phase2_ingest
    run_phase2_ingest()

    conn = get_conn()

    print("\n[2/8] Sessions...")
    from core.ledger.sessionize import build_sessions
    print(f"  -> {build_sessions(conn)} sessions")

    print("\n[3/8] Decisions...")
    from core.cases.decision_replay import extract_decisions
    print(f"  -> {extract_decisions(conn)} decisions")

    print("\n[4/8] Failures + Outcomes...")
    from core.cases.failure_chain import lift_failures_and_outcomes
    n_f, n_o = lift_failures_and_outcomes(conn)
    print(f"  -> {n_f} failures, {n_o} outcomes")

    print("\n[5/8] Cases (failure chains + win signatures)...")
    from core.cases.failure_chain import build_failure_chains
    from core.cases.win_signature import build_win_signatures
    chains = build_failure_chains(conn)
    wins   = build_win_signatures(conn)
    print(f"  -> {len(chains)} failure chains, {len(wins)} win signatures")

    print("\n[6/8] Context collapse detection...")
    from core.operator.collapse_detector import detect_context_collapse
    print(f"  -> {len(detect_context_collapse(conn))} alerts")
    conn.close()

    print("\n[7/8] Gemini embeddings (gemini-embedding-2-preview, 768d)...")
    from core.recall.gemini_embed import has_gemini_api_key, embed_all_entities, embed_all_cases
    if _skip_embedding():
        print("  -> skipped (MEOPS_SKIP_EMBED=1)")
    elif has_gemini_api_key():
        n_ent = embed_all_entities()
        n_cas = embed_all_cases()
        print(f"  -> {n_ent} entities + {n_cas} cases embedded")
    else:
        print("  -> skipped (GEMINI_API_KEY not set; recall will fall back to TF-IDF)")

    print("\n[8/8] Output...")
    _print_stats()
    _print_recalls()
    _print_briefings()


def _print_stats():
    conn = get_conn()
    print("\n" + "="*60)
    print("  STATS")
    print("="*60)
    for label, q in [
        ("events",         "SELECT COUNT(*) FROM entities WHERE type='event'"),
        ("failures",       "SELECT COUNT(*) FROM entities WHERE type='failure'"),
        ("outcomes",       "SELECT COUNT(*) FROM entities WHERE type='outcome'"),
        ("decisions",      "SELECT COUNT(*) FROM entities WHERE type='decision'"),
        ("interventions",  "SELECT COUNT(*) FROM entities WHERE type='intervention'"),
        ("sessions",       "SELECT COUNT(*) FROM sessions"),
        ("failure chains", "SELECT COUNT(*) FROM cases WHERE case_kind='failure_chain'"),
        ("win signatures", "SELECT COUNT(*) FROM cases WHERE case_kind='win_signature'"),
        ("embeddings",     "SELECT COUNT(*) FROM embeddings"),
        ("heuristics",     "SELECT COUNT(*) FROM heuristics WHERE active=1"),
        ("edges",          "SELECT COUNT(*) FROM edges"),
    ]:
        print(f"  {label:<22} {conn.execute(q).fetchone()[0]:>6}")
    print("\n  Events by project:")
    for r in conn.execute(
        "SELECT project_id, COUNT(*) n FROM entities WHERE type='event' "
        "GROUP BY project_id ORDER BY n DESC"
    ).fetchall():
        print(f"    {(r['project_id'] or 'unknown'):<22} {r['n']:>5}  {'#'*min(r['n']//3,20)}")
    print("\n  Failure chains:")
    for c in conn.execute(
        "SELECT title, project_id FROM cases WHERE case_kind='failure_chain' LIMIT 8"
    ).fetchall():
        print(f"    [{(c['project_id'] or '?'):<16}] {c['title'][:52]}")
    print("\n  Collapse alerts (top 3):")
    for r in conn.execute(
        "SELECT data FROM entities WHERE type='intervention' ORDER BY confidence DESC LIMIT 3"
    ).fetchall():
        d = json.loads(r["data"] or "{}")
        print(f"    {d.get('collapse_score',0):.2f}  {d.get('message','')[:65]}")
    conn.close()


def _print_recalls():
    from core.recall.gemini_embed import format_semantic_recall
    print("\n" + "="*60)
    print("  SEMANTIC RECALL (Gemini embedding-2-preview)")
    print("="*60)
    for q in [
        "IronClad ZIP API revenue blocked deployment",
        "connection refused gateway WSL startup failed",
        "Paperclip tunnel reconnect invite loop",
        "Antigravity IDE broken reinstall environment",
        "OpenClaw skill context collapse agent",
    ]:
        print(format_semantic_recall(q, top_k=3))


def _print_briefings():
    from core.briefing.daily import generate_daily_briefing, generate_project_briefing
    print("\n" + "="*60)
    print("  DAILY OPERATOR BRIEFING")
    print("="*60)
    print(generate_daily_briefing())
    for pid in ["ironclad", "openclaw", "antigravity"]:
        print(f"\n  [{pid.upper()}]")
        print(generate_project_briefing(pid))


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    if   mode == "full":   run_phase2()
    elif mode == "stats":  _print_stats()
    elif mode == "brief":  _print_briefings()
    elif mode == "embed":
        init_db()
        from core.recall.gemini_embed import (
            has_gemini_api_key,
            init_embed_table,
            embed_all_entities,
            embed_all_cases,
        )
        if not has_gemini_api_key():
            raise SystemExit("GEMINI_API_KEY is not set. Configure it in the environment or .env before running embed.")
        init_embed_table()
        print(f"Embedded: {embed_all_entities()} entities, {embed_all_cases()} cases")
    elif mode == "ui":
        from ui.server import run_server
        port = int(sys.argv[2]) if len(sys.argv) > 2 else 8008
        run_server(port=port)
    elif mode == "recall":
        q = " ".join(sys.argv[2:]) or "IronClad revenue blocked"
        from core.recall.gemini_embed import format_semantic_recall
        print(format_semantic_recall(q))
