"""
Phase 3 — Similar-case recall engine.
Strategy: TF-IDF + keyword overlap as fast baseline (no model required).
If sentence-transformers is available, upgrades to dense embeddings.
"""
from __future__ import annotations
import json, math, re, sys
from pathlib import Path
from collections import Counter
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from core.storage.db import get_conn


# ── Tokenizer ────────────────────────────────────────────────────────────────

def tokenize(text: str) -> list[str]:
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    tokens = text.split()
    stopwords = {
        'the','a','an','and','or','but','in','on','at','to','for','of','with',
        'was','is','are','were','has','have','had','be','been','being',
        'this','that','these','those','it','its','i','we','you','they',
        'he','she','his','her','their','our','my','your','from','by',
        'as','so','if','then','when','while','after','before','during',
        'not','no','yes','also','just','only','more','some','all','any',
        'user','session','work','working','using','used','new','current',
    }
    return [t for t in tokens if t not in stopwords and len(t) > 2]


# ── TF-IDF corpus builder ────────────────────────────────────────────────────

class TFIDFIndex:
    def __init__(self):
        self.docs: list[tuple[str, str, str]] = []  # (id, type, text)
        self.tf: list[dict[str, float]] = []
        self.idf: dict[str, float] = {}
        self._built = False

    def add(self, doc_id: str, doc_type: str, text: str):
        self.docs.append((doc_id, doc_type, text))
        tokens = tokenize(text)
        counts = Counter(tokens)
        total = max(len(tokens), 1)
        self.tf.append({t: c/total for t, c in counts.items()})

    def build(self):
        n = len(self.docs)
        if n == 0:
            return
        df: dict[str, int] = {}
        for tf_doc in self.tf:
            for term in tf_doc:
                df[term] = df.get(term, 0) + 1
        self.idf = {t: math.log((n + 1) / (d + 1)) + 1 for t, d in df.items()}
        self._built = True

    def tfidf_vec(self, idx: int) -> dict[str, float]:
        return {t: self.tf[idx][t] * self.idf.get(t, 1.0)
                for t in self.tf[idx]}

    def query_vec(self, text: str) -> dict[str, float]:
        tokens = tokenize(text)
        counts = Counter(tokens)
        total = max(len(tokens), 1)
        tf = {t: c/total for t, c in counts.items()}
        return {t: tf[t] * self.idf.get(t, 1.0) for t in tf}

    def cosine(self, v1: dict, v2: dict) -> float:
        common = set(v1) & set(v2)
        if not common:
            return 0.0
        dot = sum(v1[t] * v2[t] for t in common)
        n1 = math.sqrt(sum(x*x for x in v1.values()))
        n2 = math.sqrt(sum(x*x for x in v2.values()))
        if n1 == 0 or n2 == 0:
            return 0.0
        return dot / (n1 * n2)

    def search(self, query: str, top_k: int = 5,
               filter_type: str = None) -> list[dict]:
        if not self._built:
            self.build()
        qv = self.query_vec(query)
        scores = []
        for i, (doc_id, doc_type, _) in enumerate(self.docs):
            if filter_type and doc_type != filter_type:
                continue
            dv = self.tfidf_vec(i)
            score = self.cosine(qv, dv)
            if score > 0:
                scores.append((score, doc_id, doc_type))
        scores.sort(reverse=True)
        return [
            {"id": did, "type": dt, "score": round(s, 4)}
            for s, did, dt in scores[:top_k]
        ]


# ── Index builder ────────────────────────────────────────────────────────────

_INDEX: TFIDFIndex | None = None


def build_index(force: bool = False) -> TFIDFIndex:
    global _INDEX
    if _INDEX is not None and not force:
        return _INDEX
    conn = get_conn()
    idx = TFIDFIndex()
    # Index cases
    cases = conn.execute("SELECT id, case_kind, title, symptom, data FROM cases").fetchall()
    for c in cases:
        data = json.loads(c["data"] or "{}")
        text = " ".join(filter(None, [
            c["title"] or "",
            c["symptom"] or "",
            c["data"] or "",
            " ".join(data.get("reusable_fix", [])),
            " ".join(data.get("diagnosis_path", [])),
            data.get("future_pattern", ""),
        ]))
        idx.add(c["id"], c["case_kind"], text)
    # Index failure entities
    fails = conn.execute(
        "SELECT id, data, summary FROM entities WHERE type='failure'"
    ).fetchall()
    for f in fails:
        data = json.loads(f["data"] or "{}")
        text = " ".join(filter(None, [
            f["summary"] or "",
            data.get("symptom", ""),
            " ".join(data.get("candidate_causes", [])),
            data.get("future_rule", ""),
        ]))
        idx.add(f["id"], "failure", text)
    # Index outcome entities
    outcomes = conn.execute(
        "SELECT id, summary FROM entities WHERE type='outcome'"
    ).fetchall()
    for o in outcomes:
        idx.add(o["id"], "outcome", o["summary"] or "")
    conn.close()
    idx.build()
    _INDEX = idx
    print(f"[recall] Index built: {len(idx.docs)} documents")
    return idx


# ── Public recall API ─────────────────────────────────────────────────────────

def find_similar_cases(symptom_or_context: str, top_k: int = 5) -> list[dict]:
    """Main entry point: find most similar cases for a given symptom/context."""
    idx = build_index()
    results = idx.search(symptom_or_context, top_k=top_k)
    if not results:
        return []
    conn = get_conn()
    enriched = []
    for r in results:
        eid = r["id"]
        # Try cases table first
        case = conn.execute("SELECT * FROM cases WHERE id=?", (eid,)).fetchone()
        if case:
            data = json.loads(case["data"] or "{}")
            enriched.append({
                "id": eid,
                "type": case["case_kind"],
                "score": r["score"],
                "title": case["title"],
                "symptom": case["symptom"] or "",
                "reusable_fix": data.get("reusable_fix", []),
                "diagnosis_path": data.get("diagnosis_path", []),
                "future_pattern": data.get("future_pattern", ""),
                "project_id": case["project_id"],
                "recurrence": case["recurrence"],
            })
            continue
        # Try entities
        ent = conn.execute("SELECT * FROM entities WHERE id=?", (eid,)).fetchone()
        if ent:
            data = json.loads(ent["data"] or "{}")
            enriched.append({
                "id": eid,
                "type": ent["type"],
                "score": r["score"],
                "title": ent["summary"],
                "symptom": data.get("symptom", ""),
                "reusable_fix": [data.get("future_rule", "")] if data.get("future_rule") else [],
                "project_id": ent["project_id"],
                "recurrence": data.get("recurrence_count", 1),
            })
    conn.close()
    return enriched


def retrieve_known_fix_paths(symptom: str) -> list[str]:
    """Return ranked list of known fix paths for a symptom."""
    cases = find_similar_cases(symptom, top_k=3)
    fixes = []
    for c in cases:
        if c.get("reusable_fix"):
            fixes.extend(c["reusable_fix"])
        if c.get("diagnosis_path"):
            fixes.extend(c["diagnosis_path"])
    return list(dict.fromkeys(fixes))  # dedupe preserving order


def retrieve_dead_ends(symptom: str) -> list[str]:
    """Return false paths / known dead ends for a symptom."""
    idx = build_index()
    results = idx.search(symptom, top_k=3)
    dead_ends = []
    conn = get_conn()
    for r in results:
        case = conn.execute("SELECT data FROM cases WHERE id=?", (r["id"],)).fetchone()
        if case:
            data = json.loads(case["data"] or "{}")
            dead_ends.extend(data.get("false_paths", []))
    conn.close()
    return list(dict.fromkeys(dead_ends))


def format_recall_report(symptom: str) -> str:
    """Human-readable recall report for a symptom."""
    cases = find_similar_cases(symptom, top_k=5)
    fixes = retrieve_known_fix_paths(symptom)
    dead_ends = retrieve_dead_ends(symptom)
    lines = [
        f"\n{'='*55}",
        f"  RECALL REPORT: {symptom[:50]}",
        f"{'='*55}",
    ]
    if cases:
        lines.append(f"\n  {len(cases)} SIMILAR PRIOR CASES:")
        for i, c in enumerate(cases, 1):
            lines.append(f"  {i}. [{c['project_id'] or '?'}] {c['title'][:60]}")
            lines.append(f"     score:{c['score']:.3f}  recurrence:{c.get('recurrence',1)}")
            if c.get("symptom"):
                lines.append(f"     symptom: {c['symptom'][:80]}")
    else:
        lines.append("  No similar cases found yet — more data needed.")
    if fixes:
        lines.append(f"\n  KNOWN FIX PATHS ({len(fixes)}):")
        for i, f in enumerate(fixes[:6], 1):
            lines.append(f"  {i}. {f}")
    if dead_ends:
        lines.append(f"\n  KNOWN DEAD ENDS (avoid):")
        for d in dead_ends[:4]:
            lines.append(f"  - {d}")
    return "\n".join(lines)


if __name__ == "__main__":
    query = " ".join(sys.argv[1:]) or "connection refused gateway startup"
    print(format_recall_report(query))
