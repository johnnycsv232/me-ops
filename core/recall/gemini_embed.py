"""
ME-OPS v2 — Gemini Embedding Layer
Model: gemini-embedding-2-preview
Dims: 768 (MRL, normalized)
Tasks: RETRIEVAL_DOCUMENT for indexing, RETRIEVAL_QUERY for search
Free tier: 1500 RPM — batch 100 texts, ~0.5s delay between batches
Storage: SQLite blob column, numpy for cosine sim
"""
import time, json, struct, sys, os, io
import numpy as np
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))
from core.config import require_env
from core.storage.db import get_conn, ts

EMBED_MODEL    = "gemini-embedding-2-preview"
EMBED_DIMS     = 768
BATCH_SIZE  = 50    # free tier: 100 RPM — smaller batches = more safety margin
BATCH_DELAY = 65   # 65s between batches — stays under 100 RPM comfortably

# ── SQLite embedding store ────────────────────────────────────────────────────

EMBED_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS embeddings (
    entity_id    TEXT PRIMARY KEY,
    entity_type  TEXT NOT NULL,
    text_hash    TEXT NOT NULL,
    vector       BLOB NOT NULL,         -- float32 array, 768 dims
    model        TEXT DEFAULT 'gemini-embedding-2-preview',
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_embeddings_type ON embeddings(entity_type);
"""


def init_embed_table():
    conn = get_conn()
    conn.executescript(EMBED_TABLE_SQL)
    conn.commit()
    conn.close()


def vec_to_blob(vec: list[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


def blob_to_vec(blob: bytes) -> np.ndarray:
    n = len(blob) // 4
    return np.array(struct.unpack(f"{n}f", blob), dtype=np.float32)


def normalize(vec: np.ndarray) -> np.ndarray:
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def store_embedding(conn, entity_id: str, entity_type: str,
                    text: str, vector: list[float]):
    import hashlib
    text_hash = hashlib.md5(text.encode()).hexdigest()
    blob = vec_to_blob(vector)
    conn.execute("""
        INSERT OR REPLACE INTO embeddings
          (entity_id, entity_type, text_hash, vector, model, created_at)
        VALUES (?,?,?,?,?,?)
    """, (entity_id, entity_type, text_hash, blob, EMBED_MODEL, ts()))


def get_embedding(conn, entity_id: str) -> Optional[np.ndarray]:
    row = conn.execute(
        "SELECT vector FROM embeddings WHERE entity_id=?", (entity_id,)
    ).fetchone()
    if row:
        return normalize(blob_to_vec(row["vector"]))
    return None


def already_embedded(conn, entity_id: str, text: str | None = None) -> bool:
    row = conn.execute(
        "SELECT text_hash FROM embeddings WHERE entity_id=?", (entity_id,)
    ).fetchone()
    if row is None:
        return False
    if text is None:
        return True
    import hashlib
    return row["text_hash"] == hashlib.md5(text.encode()).hexdigest()


# ── Gemini API client ─────────────────────────────────────────────────────────

def has_gemini_api_key() -> bool:
    return bool(os.environ.get("GEMINI_API_KEY") or _try_require_key())


def _try_require_key() -> Optional[str]:
    try:
        return require_env("GEMINI_API_KEY")
    except RuntimeError:
        return None


def _get_client():
    from google import genai
    return genai.Client(api_key=require_env("GEMINI_API_KEY"))


def _extract_retry_delay(e: Exception) -> float:
    """Pull retryDelay seconds from a 429 error, default 65s."""
    try:
        msg = str(e)
        import re
        m = re.search(r"retryDelay.*?(\d+)", msg)
        if m:
            return float(m.group(1)) + 2
    except Exception:
        pass
    return 65.0


def embed_batch(texts: list[str], task_type: str = "RETRIEVAL_DOCUMENT") -> list[list[float]]:
    """Embed a batch of texts. Retries on 429 with correct delay."""
    from google.genai import types
    client = _get_client()
    for attempt in range(5):
        try:
            result = client.models.embed_content(
                model=EMBED_MODEL,
                contents=texts,
                config=types.EmbedContentConfig(
                    task_type=task_type,
                    output_dimensionality=EMBED_DIMS
                )
            )
            return [e.values for e in result.embeddings]
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                delay = _extract_retry_delay(e)
                print(f"  [embed] 429 — waiting {delay:.0f}s (attempt {attempt+1}/5)")
                time.sleep(delay)
            else:
                raise
    raise RuntimeError("embed_batch failed after 5 retries")


def embed_query(text: str) -> np.ndarray:
    """Embed a single query. Returns normalized 768-dim vector. Falls back to zeros on quota error."""
    try:
        vecs = embed_batch([text], task_type="RETRIEVAL_QUERY")
        return normalize(np.array(vecs[0], dtype=np.float32))
    except RuntimeError as e:
        if "GEMINI_API_KEY" in str(e):
            print("  [embed_query] GEMINI_API_KEY not set — falling back to TF-IDF recall")
            return None
        raise
    except Exception as e:
        if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
            print(f"  [embed_query] Quota hit — falling back to TF-IDF recall")
            return None  # signal to caller to use fallback
        raise


# ── Bulk embedder ─────────────────────────────────────────────────────────────

def embed_all_entities(entity_types: list[str] = None, force: bool = False) -> int:
    """
    Embed all canonical entities not yet in embeddings table.
    Respects free tier limits with batching + delays.
    Returns count of newly embedded items.
    """
    init_embed_table()
    conn = get_conn()

    types_to_embed = entity_types or ["event", "failure", "outcome", "decision"]
    q_parts = ",".join(f"'{t}'" for t in types_to_embed)

    rows = conn.execute(f"""
        SELECT id, type, summary, data
        FROM entities
        WHERE type IN ({q_parts})
        ORDER BY created_at DESC
    """).fetchall()

    # Filter to unembedded (unless force)
    to_embed = []
    for row in rows:
        data  = json.loads(row["data"] or "{}")
        text  = " ".join(filter(None, [
            row["summary"] or "",
            data.get("raw_content", "")[:500],
            data.get("symptom", ""),
            data.get("choice_made", ""),
        ])).strip()
        if text and (force or not already_embedded(conn, row["id"], text)):
                to_embed.append((row["id"], row["type"], text))

    print(f"[embed] {len(to_embed)} entities to embed ({len(rows) - len(to_embed)} already done)")

    total = 0
    for i in range(0, len(to_embed), BATCH_SIZE):
        batch = to_embed[i:i + BATCH_SIZE]
        texts = [b[2] for b in batch]
        try:
            vectors = embed_batch(texts, task_type="RETRIEVAL_DOCUMENT")
            for (eid, etype, text), vec in zip(batch, vectors):
                store_embedding(conn, eid, etype, text, vec)
            conn.commit()
            total += len(batch)
            print(f"  [embed] {total}/{len(to_embed)} embedded...")
            if i + BATCH_SIZE < len(to_embed):
                time.sleep(BATCH_DELAY)
        except Exception as e:
            print(f"  [embed] batch error: {e} — retrying in 5s")
            time.sleep(5)
            try:
                # retry one at a time
                for eid, etype, text in batch:
                    vecs = embed_batch([text], task_type="RETRIEVAL_DOCUMENT")
                    store_embedding(conn, eid, etype, text, vecs[0])
                    total += 1
                conn.commit()
            except Exception as e2:
                print(f"  [embed] retry failed: {e2}")

    conn.close()
    print(f"[embed] Done. {total} entities embedded.")
    return total


def embed_all_cases(force: bool = False) -> int:
    """Embed all cases (failure chains, win signatures)."""
    init_embed_table()
    conn = get_conn()
    rows = conn.execute("SELECT id, case_kind, title, symptom, data FROM cases").fetchall()
    to_embed = []
    for row in rows:
        data = json.loads(row["data"] or "{}")
        text = " ".join(filter(None, [
            row["title"] or "",
            row["symptom"] or "",
            " ".join(data.get("reusable_fix", [])),
            " ".join(data.get("diagnosis_path", [])),
            " ".join(data.get("preconditions", [])),
            data.get("future_pattern", ""),
        ])).strip()
        if text and (force or not already_embedded(conn, row["id"], text)):
            to_embed.append((row["id"], row["case_kind"], text))

    total = 0
    for i in range(0, len(to_embed), BATCH_SIZE):
        batch = to_embed[i:i + BATCH_SIZE]
        texts = [b[2] for b in batch]
        try:
            vectors = embed_batch(texts, task_type="RETRIEVAL_DOCUMENT")
            for (eid, etype, text), vec in zip(batch, vectors):
                store_embedding(conn, eid, etype, text, vec)
            conn.commit()
            total += len(batch)
            if i + BATCH_SIZE < len(to_embed):
                time.sleep(BATCH_DELAY)
        except Exception as e:
            print(f"  [embed cases] error: {e}")
    conn.close()
    print(f"[embed] Cases: {total} embedded.")
    return total


# ── Cosine similarity search ──────────────────────────────────────────────────

def cosine_search(query: str, top_k: int = 5,
                  filter_types: list[str] = None) -> list[dict]:
    """Semantic search. Falls back to TF-IDF if quota exceeded."""
    q_vec = embed_query(query)
    if q_vec is None:
        # Quota hit — fall back to TF-IDF
        from core.recall.retrieve import find_similar_cases
        results = find_similar_cases(query, top_k=top_k)
        for item in results:
            item["engine"] = "TF-IDF fallback"
        return results
    conn = get_conn()

    type_filter = ""
    if filter_types:
        type_filter = "WHERE entity_type IN (" + ",".join(f"'{t}'" for t in filter_types) + ")"

    rows = conn.execute(
        f"SELECT entity_id, entity_type, vector FROM embeddings {type_filter}"
    ).fetchall()
    if not rows:
        conn.close()
        from core.recall.retrieve import find_similar_cases
        results = find_similar_cases(query, top_k=top_k)
        for item in results:
            item["engine"] = "TF-IDF fallback"
        return results

    scores = []
    for row in rows:
        vec = normalize(blob_to_vec(row["vector"]))
        score = float(np.dot(q_vec, vec))  # cosine sim (both normalized)
        scores.append((score, row["entity_id"], row["entity_type"]))

    scores.sort(reverse=True)
    top = scores[:top_k]

    results = []
    for score, eid, etype in top:
        # Fetch full record
        case = conn.execute("SELECT * FROM cases WHERE id=?", (eid,)).fetchone()
        if case:
            data = json.loads(case["data"] or "{}")
            results.append({
                "id": eid, "type": case["case_kind"],
                "engine": "Gemini embed-2",
                "score": round(score, 4),
                "title": case["title"],
                "symptom": case["symptom"] or "",
                "reusable_fix": data.get("reusable_fix", []),
                "diagnosis_path": data.get("diagnosis_path", []),
                "false_paths": data.get("false_paths", []),
                "future_pattern": data.get("future_pattern", ""),
                "project_id": case["project_id"],
                "recurrence": case["recurrence"],
            })
            continue
        ent = conn.execute("SELECT * FROM entities WHERE id=?", (eid,)).fetchone()
        if ent:
            data = json.loads(ent["data"] or "{}")
            results.append({
                "id": eid, "type": ent["type"],
                "engine": "Gemini embed-2",
                "score": round(score, 4),
                "title": ent["summary"],
                "symptom": data.get("symptom", ""),
                "reusable_fix": ([data.get("future_rule")] if data.get("future_rule") else []),
                "false_paths": [],
                "project_id": ent["project_id"],
                "recurrence": data.get("recurrence_count", 1),
            })
    conn.close()
    return results


def format_semantic_recall(query: str, top_k: int = 5) -> str:
    results = cosine_search(query, top_k=top_k)
    engine = results[0].get("engine", "Gemini embed-2") if results else "TF-IDF fallback"
    lines = [
        f"\n{'='*58}",
        f"  RECALL [{engine}]: {query[:46]}",
        f"{'='*58}",
    ]
    if not results:
        lines.append("  No embedded results yet — run embed pipeline first.")
        return "\n".join(lines)
    for i, r in enumerate(results, 1):
        lines += [
            f"\n  {i}. [{r.get('project_id','?'):20}] {r['title'][:55]}",
            f"     sim:{r['score']:.4f}  type:{r['type']}  recurs:{r.get('recurrence',1)}",
        ]
        if r.get("symptom"):
            lines.append(f"     symptom: {r['symptom'][:80]}")
        if r.get("reusable_fix"):
            lines.append(f"     fix path: {' -> '.join(r['reusable_fix'][:4])}")
        if r.get("false_paths"):
            lines.append(f"     dead ends: {', '.join(r['false_paths'][:2])}")
    return "\n".join(lines)


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "IronClad revenue blocked deployment"
    print(format_semantic_recall(q))
