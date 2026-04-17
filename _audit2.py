import sys, os, sqlite3, json, importlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
os.environ['PYTHONIOENCODING'] = 'utf-8'

from core.config import require_env

print("=== IMPORT TESTS ===")
for mod in ['pydantic','numpy','google.genai','websockets','sqlite3']:
    try:
        m = importlib.import_module(mod)
        v = getattr(m, '__version__', 'no __version__')
        print(f"  OK  {mod} {v}")
    except Exception as e:
        print(f"  FAIL {mod}: {e}")

print("\n=== CORE MODULE IMPORTS ===")
for mod in [
    'core.storage.db',
    'core.storage.schema',
    'core.ledger.classify',
    'core.ledger.sessionize',
    'core.cases.failure_chain',
    'core.cases.win_signature',
    'core.cases.decision_replay',
    'core.recall.gemini_embed',
    'core.recall.retrieve',
    'core.heuristics.seed',
    'core.briefing.daily',
    'core.operator.collapse_detector',
    'pieces_bridge.mcp_client',
    'pieces_bridge.ingest_p2',
]:
    try:
        importlib.import_module(mod)
        print(f"  OK  {mod}")
    except Exception as e:
        print(f"  FAIL {mod}: {e}")

print("\n=== PIECESOS HEALTH ===")
import urllib.request
try:
    r = urllib.request.urlopen('http://localhost:39300/.well-known/version', timeout=3)
    print(f"  PiecesOS: {r.read().decode()}")
except Exception as e:
    print(f"  PiecesOS: UNREACHABLE ({e})")

print("\n=== GEMINI API TEST ===")
try:
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=require_env("GEMINI_API_KEY"))
    result = client.models.embed_content(
        model="gemini-embedding-2-preview",
        contents=["test"],
        config=types.EmbedContentConfig(task_type="RETRIEVAL_QUERY", output_dimensionality=768)
    )
    print(f"  Gemini embed: OK, dims={len(result.embeddings[0].values)}")
except Exception as e:
    print(f"  Gemini embed: FAIL ({str(e)[:100]})")

print("\n=== FILE SIZES ===")
import pathlib
root = pathlib.Path(ROOT)
for f in sorted(root.rglob('*.py')):
    lines = len(f.read_text(encoding='utf-8',errors='replace').splitlines())
    status = "SCRATCH" if f.name.startswith('_') else "ACTIVE"
    print(f"  {status:<8} {lines:>4}L  {str(f.relative_to(root))}")
