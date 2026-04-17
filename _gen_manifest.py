"""Generate handoff_manifest.json — machine-readable file inventory with evidence."""
import sys, os, json, sqlite3, hashlib, time
from pathlib import Path

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root))

def sha8(p):
    try: return hashlib.md5(p.read_bytes()).hexdigest()[:8]
    except: return "unreadable"

manifest = {
    "generated": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    "machine": "GettUppENT", "user": "finan",
    "project_root": str(root),
    "db_path": str(root / "meops.db"),
    "entry_points": {
        "primary": "run_phase2.py",
        "legacy": "run.py",
        "note": "Always use run_phase2.py — run.py is Phase 1 only"
    },
    "files": {}
}

FILES = [
    # path, status, safe_to_edit, entrypoint_relevance, owner_module, notes
    ("run_phase2.py",                   "PRIMARY",      True,  "HIGH",   "root",            "All production CLI commands. Uses env-backed config and skips embedding if GEMINI_API_KEY is missing."),
    ("run.py",                          "SUPERSEDED",   True,  "LOW",    "root",            "Phase 1 only. Lacks Phase 2+ logic. Prefer run_phase2.py."),
    ("requirements.txt",                "CURRENT",      True,  "HIGH",   "root",            "Bootstrap scripts install from this file. Includes google-genai, numpy, pydantic, websockets, and python-dotenv."),
    ("meops.db",                        "PRECIOUS",     False, "HIGH",   "root",            "2.07 MB SQLite. Single source of all canonical data. Back up before any schema change."),
    ("_reset_canonical.py",             "DANGEROUS",    False, "HIGH",   "root",            "DESTRUCTIVE — wipes all derived layers. Now requires typing DESTROY before proceeding."),
    ("_test_embed.py",                  "HEALTH_CHECK", True,  "LOW",    "root",            "Verifies Gemini API + model using GEMINI_API_KEY from env/.env."),
    ("_boot_verdict.py",                "SCRATCH",      True,  "LOW",    "root",            "Audit script. Can delete after handoff."),
    ("_debug.py",                       "SCRATCH",      True,  "NONE",   "root",            "One-off diagnostic. Delete."),
    ("_enc.py",                         "SCRATCH",      True,  "NONE",   "root",            "Encoding test. Delete."),
    ("_migrate.py",                     "COMPLETED",    True,  "NONE",   "root",            "Ran ALTER TABLE once. Column now in SCHEMA_SQL. Can delete."),
    ("core/storage/schema.py",          "LOAD_BEARING", False, "HIGH",   "storage",         "Full SQLite DDL. Source of truth. Schema changes require migration + potential data wipe."),
    ("core/storage/db.py",              "LOAD_BEARING", False, "HIGH",   "storage",         "get_conn(), init_db(), EntityRepo.upsert(), ensure_project(). All ingestion flows through here."),
    ("core/ledger/classify.py",         "HIGH_IMPACT",  True,  "HIGH",   "ledger",          "Maps text -> project_id (12 buckets) + event_kind. 311/526 events return NULL. Safe to add keywords."),
    ("core/ledger/sessionize.py",       "ACTIVE",       True,  "MEDIUM", "ledger",          "Groups events into sessions by 30-min gap/4-hr max. tool_switch_count is distinct tools, not real switches."),
    ("core/cases/failure_chain.py",     "ACTIVE",       True,  "MEDIUM", "cases",           "lift_failures_and_outcomes() + build_failure_chains(). 26 failures, 9 outcomes, 9 chains from 526 events."),
    ("core/cases/win_signature.py",     "ACTIVE",       True,  "MEDIUM", "cases",           "build_win_signatures(). 9 win sigs from outcomes. Uses sqlite3.Row['key'] not .get()."),
    ("core/cases/decision_replay.py",   "BROKEN_0OUT",  True,  "LOW",    "cases",           "extract_decisions() returns 0 from 526 events. Trigger phrases wrong for summary language."),
    ("core/recall/gemini_embed.py",     "PHASE3_CORE",  False, "HIGH",   "recall",          "Gemini embed pipeline. Reads GEMINI_API_KEY from env/.env, supports TF-IDF fallback, and keeps batch 50 / 65s pacing."),
    ("core/recall/retrieve.py",         "ACTIVE",       True,  "MEDIUM", "recall",          "TF-IDF fallback. 53 docs indexed. Returns 0 results — low doc count in index."),
    ("core/heuristics/seed.py",         "ACTIVE",       True,  "LOW",    "heuristics",      "10 manually seeded heuristics. Idempotent (INSERT OR IGNORE). Safe to add more."),
    ("core/briefing/daily.py",          "ACTIVE",       True,  "MEDIUM", "briefing",        "3 briefing templates. Hardcoded narrative, not ML-derived. DB stat injection works."),
    ("core/operator/collapse_detector.py","ACTIVE_0OUT", True, "MEDIUM", "operator",        "Scans sessions but produces 0 alerts. tool_switch_count always 0-1, threshold is 4."),
    ("core/models/base.py",             "DOC_ONLY",     True,  "LOW",    "models",          "Pydantic BaseRecord. NOT validated on DB writes. Reference only."),
    ("core/models/event.py",            "DOC_ONLY",     True,  "LOW",    "models",          "Event, Artifact, SystemState. Reference only."),
    ("core/models/decision.py",         "DOC_ONLY",     True,  "LOW",    "models",          "Decision (with quality scoring formula), Failure, Outcome. Reference only."),
    ("core/models/case.py",             "DOC_ONLY",     True,  "LOW",    "models",          "Case, Heuristic, Briefing, Intervention. Reference only."),
    ("core/causal/__init__.py",         "EMPTY_STUB",   True,  "NONE",   "causal",          "Phase 4 causal inference. Not started."),
    ("pipelines/__init__.py",           "EMPTY_STUB",   True,  "NONE",   "pipelines",       "Standalone pipeline scripts. Not built."),
    ("pieces_bridge/ingest.py",         "DEPRECATED",   False, "HIGH",   "pieces_bridge",   "Deprecated compatibility shim that redirects legacy callers to ingest_p2."),
    ("pieces_bridge/ingest_p2.py",      "PRIMARY_INGEST",True, "MEDIUM", "pieces_bridge",   "Active ingestion. Re-classifies 526 from cache + MCP delta. Requires PiecesOS running."),
    ("pieces_bridge/mcp_client.py",     "LOAD_BEARING", False, "HIGH",   "pieces_bridge",   "MCP over SSE. Port 39300 hardcoded. Protocol path must match PiecesOS version."),
    ("tmp/pdfs/generate_me_ops_app_summary.py","ORPHAN",True,  "NONE",   "tmp",             "Not imported anywhere. Likely PDF doc generator. Not wired to pipeline."),
    ("docs/boot_verdict.json",          "GENERATED",    True,  "LOW",    "docs",            "Machine-readable boot test results."),
    ("docs/ME_OPS_v2_FORENSIC_HANDOFF.pdf","PRIMARY_DOC",False,"HIGH",  "docs",            "Current forensic handoff. Source of truth over older ME_OPS_v2_HANDOFF.pdf."),
]

for row in FILES:
    fpath, status, safe, relevance, owner, notes = row
    full = root / fpath.replace('/', os.sep)
    exists = full.exists()
    size = full.stat().st_size if exists else 0
    lines = 0
    if exists and fpath.endswith('.py'):
        try: lines = len(full.read_text(encoding='utf-8',errors='replace').splitlines())
        except: pass
    manifest["files"][fpath] = {
        "status": status,
        "safe_to_edit": safe,
        "entrypoint_relevance": relevance,
        "owner_module": owner,
        "exists": exists,
        "size_bytes": size,
        "lines": lines,
        "hash_md5_8": sha8(full) if exists else None,
        "notes": notes
    }

# Add DB stats
conn = sqlite3.connect(str(root / "meops.db"))
conn.row_factory = sqlite3.Row
tables = {r['name']: conn.execute(f"SELECT COUNT(*) FROM {r['name']}").fetchone()[0]
          for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
conn.close()
manifest["db_state"] = {"tables": tables}

# Add dependency truth
import importlib
deps = {}
for pkg, attr in [('google.genai','__version__'),('pydantic','__version__'),
                   ('numpy','__version__'),('websockets','__version__')]:
    try:
        m = importlib.import_module(pkg)
        deps[pkg] = getattr(m,'__version__','installed')
    except Exception as e:
        deps[pkg] = f"MISSING: {e}"
manifest["confirmed_versions"] = {
    "python": sys.version.split()[0],
    "packages": deps,
    "requirements_txt_complete": all("MISSING" not in v for v in deps.values())
}

out = root / "docs" / "handoff_manifest.json"
with open(str(out), 'w', encoding='utf-8') as f:
    json.dump(manifest, f, indent=2)
print(f"Manifest written: {out}")
print(f"  {len(manifest['files'])} files documented")
print(f"  requirements_txt_complete: {manifest['confirmed_versions']['requirements_txt_complete']}")
for k, v in manifest['confirmed_versions']['packages'].items():
    print(f"  {k}: {v}")
