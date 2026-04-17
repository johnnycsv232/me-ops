"""
Heuristic seed store — first 10 heuristics pre-loaded from observed patterns.
These bootstrap the heuristics table before mining begins.
"""
from __future__ import annotations
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from core.storage.db import get_conn, ts

SEED_HEURISTICS = [
    {
        "id": "heu_001",
        "heuristic_kind": "failure_rule",
        "statement": "When a startup or network failure appears, verify bind address, port, and env path BEFORE reinstalling the service.",
        "scope": "debugging/network",
        "applies_when": ["network symptoms", "startup failure", "WSL stack", "multi-layer service"],
        "support_count": 6,
        "contradiction_count": 0,
        "confidence": 0.92,
        "utility_score": 0.95,
    },
    {
        "id": "heu_002",
        "heuristic_kind": "anti_pattern",
        "statement": "More than 4 tool/context switches in a 20-minute window reliably predicts debugging drag and session fragmentation.",
        "scope": "workflow/focus",
        "applies_when": ["debugging session", "multi-tool environment", "WSL + host mixed"],
        "support_count": 8,
        "contradiction_count": 1,
        "confidence": 0.85,
        "utility_score": 0.88,
    },
    {
        "id": "heu_003",
        "heuristic_kind": "failure_rule",
        "statement": "Paperclip connectivity always requires a tunnel (ngrok/cloudflared) when OpenClaw runs on a VPS. Skipping this step is the most common reconnect failure.",
        "scope": "infrastructure/paperclip",
        "applies_when": ["Paperclip invite", "VPS runtime", "OpenClaw remote"],
        "support_count": 4,
        "contradiction_count": 0,
        "confidence": 0.95,
        "utility_score": 0.90,
    },
    {
        "id": "heu_004",
        "heuristic_kind": "failure_rule",
        "statement": "A missing OPENAI_API_KEY silently blocks all Codex tasks even after successful installation. Always verify env before testing adapter.",
        "scope": "auth/codex",
        "applies_when": ["Codex adapter", "Paperclip agent setup", "new AI tool install"],
        "support_count": 2,
        "contradiction_count": 0,
        "confidence": 0.90,
        "utility_score": 0.85,
    },
    {
        "id": "heu_005",
        "heuristic_kind": "success_rule",
        "statement": "The fastest debugging sessions share three conditions: single clear target, local controllable surface, direct test loop with < 3 context switches.",
        "scope": "debugging/speed",
        "applies_when": ["infrastructure bug", "config issue", "startup failure"],
        "support_count": 5,
        "contradiction_count": 1,
        "confidence": 0.82,
        "utility_score": 0.88,
    },
    {
        "id": "heu_006",
        "heuristic_kind": "anti_pattern",
        "statement": "Planning sessions that do not end with an executable artifact (file, command, deploy) reliably precede zero-ship outcomes.",
        "scope": "workflow/planning",
        "applies_when": ["strategy session", "architecture discussion", "roadmap planning"],
        "support_count": 7,
        "contradiction_count": 2,
        "confidence": 0.78,
        "utility_score": 0.92,
    },
    {
        "id": "heu_007",
        "heuristic_kind": "warning",
        "statement": "Receiving more than 2 Paperclip invite URLs in one day signals a broken tunnel. Stop reconnecting and fix the persistent tunnel first.",
        "scope": "infrastructure/paperclip",
        "applies_when": ["pcp_invite", "multiple reconnect attempts"],
        "support_count": 4,
        "contradiction_count": 0,
        "confidence": 0.93,
        "utility_score": 0.87,
    },
    {
        "id": "heu_008",
        "heuristic_kind": "decision_rule",
        "statement": "IronClad revenue blockers always resolve faster through direct Vercel/Stripe API inspection than through web search or code review.",
        "scope": "ironclad/debugging",
        "applies_when": ["IronClad", "production issue", "revenue blocked", "ZIP API"],
        "support_count": 2,
        "contradiction_count": 0,
        "confidence": 0.88,
        "utility_score": 0.90,
    },
    {
        "id": "heu_009",
        "heuristic_kind": "anti_pattern",
        "statement": "New venture ideas explored during active infrastructure debugging sessions (OpenClaw/WSL work) are rarely acted on. Capture them but do not context-switch.",
        "scope": "workflow/focus",
        "applies_when": ["debugging session", "new idea", "context switch"],
        "support_count": 3,
        "contradiction_count": 0,
        "confidence": 0.80,
        "utility_score": 0.85,
    },
    {
        "id": "heu_010",
        "heuristic_kind": "success_rule",
        "statement": "Early morning sessions (4:00–7:30 AM) consistently produce the highest-quality, highest-leverage work. Protect this window from reactive tasks.",
        "scope": "workflow/schedule",
        "applies_when": ["scheduling", "task allocation", "peak performance window"],
        "support_count": 5,
        "contradiction_count": 0,
        "confidence": 0.88,
        "utility_score": 0.93,
    },
]


def seed_heuristics():
    conn = get_conn()
    inserted = 0
    for h in SEED_HEURISTICS:
        existing = conn.execute(
            "SELECT 1 FROM heuristics WHERE id=?", (h["id"],)
        ).fetchone()
        if existing:
            continue
        conn.execute("""
            INSERT INTO heuristics
              (id, heuristic_kind, statement, scope, applies_when,
               derived_from_cases, support_count, contradiction_count,
               confidence, utility_score, active, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?)
        """, (
            h["id"], h["heuristic_kind"], h["statement"],
            h["scope"],
            json.dumps(h["applies_when"]),
            json.dumps([]),
            h["support_count"], h["contradiction_count"],
            h["confidence"], h["utility_score"],
            ts(), ts()
        ))
        inserted += 1
    conn.commit()
    conn.close()
    print(f"[heuristics] Seeded {inserted} heuristics.")


if __name__ == "__main__":
    seed_heuristics()
