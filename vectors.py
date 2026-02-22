"""
ME-OPS Semantic Search (Vectors)
================================
Embeds events with sentence-transformers and stores in a local Qdrant
collection for semantic search ("find when I did X").

Uses Qdrant's local (file-based) mode — no server required.

Usage:
    python vectors.py [--db me_ops.duckdb] [--query "debugging auth issues"]
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Optional

import duckdb

DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"
QDRANT_PATH = Path(__file__).resolve().parent / "vectors_store"
COLLECTION = "me_ops_events"
BATCH_SIZE = 256
MODEL_NAME = "all-MiniLM-L6-v2"  # Fast, 384-dim, good for short text


def build_event_text(row: tuple) -> str:
    """Build searchable text from event tuple."""
    parts = []
    action = row[1] or ""
    target = row[2] or ""
    app = row[3] or ""
    source = row[4] or ""

    if action:
        parts.append(f"Action: {action}")
    if target:
        parts.append(f"Target: {target[:200]}")
    if app:
        parts.append(f"Tool: {app}")
    if source:
        parts.append(f"Source: {source}")

    return " | ".join(parts) if parts else "unknown event"


def search(query: str, db_path: Path, top_k: int = 10) -> bool:
    """Semantic search over indexed events."""
    try:
        from qdrant_client import QdrantClient
        from sentence_transformers import SentenceTransformer
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        return False

    if not QDRANT_PATH.exists():
        print("❌ Vector index not found. Run: python vectors.py --index")
        return False

    model = SentenceTransformer(MODEL_NAME)
    client = QdrantClient(path=str(QDRANT_PATH))

    query_vec = model.encode(query).tolist()
    results = client.query_points(
        collection_name=COLLECTION,
        query=query_vec,
        limit=top_k,
    ).points

    print(f"\n{'='*60}")
    print(f"  Search: \"{query}\"")
    print(f"  Results: {len(results)}")
    print(f"{'='*60}")

    for i, hit in enumerate(results):
        p = hit.payload or {}
        score = hit.score
        print(f"\n  [{i+1}] Score: {score:.4f}")
        print(f"      Action: {p.get('action')}")
        print(f"      Target: {p.get('target', 'N/A')}")
        print(f"      Tool:   {p.get('app_tool', 'N/A')}")
        print(f"      Time:   {p.get('timestamp', 'N/A')}")

    return True


def run(
    db_path: Path,
    *,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    do_index: bool = False,
    query: Optional[str] = None,
    top_k: int = 10,
) -> bool:
    """Main entry point supporting shared connection."""
    if do_index or (not query):
        return index_events(db_path, con=con)
    if query:
        return search(query, db_path, top_k=top_k)
    return False


def index_events(db_path: Path, *, con: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """Build vector index from events."""
    try:
        from qdrant_client import QdrantClient
        from qdrant_client.models import Distance, PointStruct, VectorParams
        from sentence_transformers import SentenceTransformer
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("   pip install sentence-transformers qdrant-client")
        return False

    close_con = False
    if con is None:
        if not db_path.exists():
            print(f"❌ Database not found: {db_path}")
            return False
        con = duckdb.connect(str(db_path), read_only=True)
        close_con = True

    try:
        print("ME-OPS Vector Indexing")
        print("=" * 60)

        # Load model
        print(f"  Loading model: {MODEL_NAME}...")
        t0 = time.time()
        model = SentenceTransformer(MODEL_NAME)
        print(f"    → Model loaded in {time.time()-t0:.1f}s")

        # Get events
        rows = con.execute("""
            SELECT event_id, action, target, app_tool, source_file,
                   ts_start::VARCHAR AS ts
            FROM events
            WHERE action IS NOT NULL
            ORDER BY ts_start
        """).fetchall()
        print(f"  Events to index: {len(rows)}")

        # Build texts
        texts = [build_event_text(r) for r in rows]

        # Encode
        print(f"  Encoding {len(texts)} events...")
        t0 = time.time()
        embeddings = model.encode(texts, batch_size=BATCH_SIZE, show_progress_bar=True)
        print(f"    → Encoded in {time.time()-t0:.1f}s")

        # Store in Qdrant (local file mode)
        print(f"  Storing in Qdrant (local: {QDRANT_PATH})...")
        client = QdrantClient(path=str(QDRANT_PATH))

        # Recreate collection
        if client.collection_exists(COLLECTION):
            client.delete_collection(COLLECTION)

        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(
                size=embeddings.shape[1],
                distance=Distance.COSINE,
            ),
        )

        # Upsert in batches
        points = []
        for i, (row, emb) in enumerate(zip(rows, embeddings)):
            points.append(PointStruct(
                id=i,
                vector=emb.tolist(),
                payload={
                    "event_id": row[0],
                    "action": row[1],
                    "target": str(row[2])[:200] if row[2] else None,
                    "app_tool": row[3],
                    "source_file": row[4],
                    "timestamp": row[5],
                    "text": texts[i][:300],
                },
            ))

        # Batch upsert
        for i in range(0, len(points), 500):
            client.upsert(
                collection_name=COLLECTION,
                points=points[i : i + 500],
            )

        count = client.get_collection(COLLECTION).points_count
        print(f"    → {count} vectors stored")
        print(f"\n{'='*60}")
        print("✅ Vector index built")
        return True
    finally:
        if close_con:
            con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ME-OPS Semantic Search")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--index", action="store_true",
                        help="Build vector index from events")
    parser.add_argument("--query", type=str, default=None,
                        help='Search query, e.g. "debugging login issues"')
    parser.add_argument("--top-k", type=int, default=10)
    args = parser.parse_args()

    if args.query:
        sys.exit(0 if search(args.query, args.db, args.top_k) else 1)
    else:
        sys.exit(0 if index_events(args.db) else 1)
