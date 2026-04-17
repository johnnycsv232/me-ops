"""SQLite schema — full canonical store for ME-OPS v2."""

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ─────────────────────────────────────────
-- SUPPORT LAYER
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS projects (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    status      TEXT DEFAULT 'active',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    tags        TEXT DEFAULT '[]'   -- JSON array
);

CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY,
    project_id      TEXT REFERENCES projects(id),
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    summary         TEXT,
    tool_switch_count INTEGER DEFAULT 0,
    fragmentation_score REAL DEFAULT 0.0,
    context_load_score  REAL DEFAULT 0.0,
    primary_app     TEXT,
    tags            TEXT DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS tags (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT UNIQUE NOT NULL,
    scope   TEXT DEFAULT 'general'
);

-- ─────────────────────────────────────────
-- RAW LAYER (Pieces direct)
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_import_batches (
    id          TEXT PRIMARY KEY,
    imported_at TEXT NOT NULL,
    source      TEXT NOT NULL,  -- pieces_ws | pieces_rest | manual
    record_count INTEGER DEFAULT 0,
    status      TEXT DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS raw_pieces_summaries (
    pieces_id       TEXT PRIMARY KEY,
    batch_id        TEXT REFERENCES raw_import_batches(id),
    name            TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT,
    annotation_text TEXT,   -- full summary content
    range_from      TEXT,
    range_to        TEXT,
    processed       INTEGER DEFAULT 0,
    canonical_id    TEXT    -- FK to events.id after processing
);

CREATE TABLE IF NOT EXISTS raw_pieces_events (
    pieces_id       TEXT PRIMARY KEY,
    batch_id        TEXT REFERENCES raw_import_batches(id),
    event_type      TEXT,   -- clipboard|vision|audio
    app_title       TEXT,
    window_title    TEXT,
    url             TEXT,
    content         TEXT,
    created_at      TEXT NOT NULL,
    processed       INTEGER DEFAULT 0,
    canonical_id    TEXT
);

-- ─────────────────────────────────────────
-- CANONICAL LAYER
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS entities (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    source          TEXT NOT NULL,
    source_refs     TEXT DEFAULT '[]',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    timestamp_start TEXT,
    timestamp_end   TEXT,
    project_id      TEXT REFERENCES projects(id),
    session_id      TEXT REFERENCES sessions(id),
    actor           TEXT DEFAULT 'user',
    confidence      REAL DEFAULT 0.0,
    evidence_refs   TEXT DEFAULT '[]',
    tags            TEXT DEFAULT '[]',
    summary         TEXT DEFAULT '',
    data            TEXT DEFAULT '{}'  -- JSON blob for type-specific fields
);

CREATE INDEX IF NOT EXISTS idx_entities_type       ON entities(type);
CREATE INDEX IF NOT EXISTS idx_entities_project    ON entities(project_id);
CREATE INDEX IF NOT EXISTS idx_entities_session    ON entities(session_id);
CREATE INDEX IF NOT EXISTS idx_entities_created    ON entities(created_at);

-- ─────────────────────────────────────────
-- INTELLIGENCE LAYER
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cases (
    id              TEXT PRIMARY KEY,
    case_kind       TEXT NOT NULL,
    title           TEXT NOT NULL,
    symptom         TEXT,
    trigger         TEXT,
    final_fix       TEXT,
    future_pattern  TEXT,
    time_lost_min   INTEGER DEFAULT 0,
    resolution_min  INTEGER DEFAULT 0,
    context_switches INTEGER DEFAULT 0,
    output_quality  REAL DEFAULT 0.0,
    confidence      REAL DEFAULT 0.0,
    recurrence      INTEGER DEFAULT 1,
    project_id      TEXT REFERENCES projects(id),
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    data            TEXT DEFAULT '{}'  -- JSON: full case fields
);

CREATE TABLE IF NOT EXISTS case_members (
    case_id     TEXT NOT NULL REFERENCES cases(id),
    entity_id   TEXT NOT NULL REFERENCES entities(id),
    role        TEXT DEFAULT 'evidence',  -- evidence|trigger|outcome|false_path
    PRIMARY KEY (case_id, entity_id)
);

CREATE TABLE IF NOT EXISTS heuristics (
    id                  TEXT PRIMARY KEY,
    heuristic_kind      TEXT NOT NULL,
    statement           TEXT NOT NULL,
    scope               TEXT DEFAULT '',
    applies_when        TEXT DEFAULT '[]',
    derived_from_cases  TEXT DEFAULT '[]',
    support_count       INTEGER DEFAULT 0,
    contradiction_count INTEGER DEFAULT 0,
    confidence          REAL DEFAULT 0.0,
    utility_score       REAL DEFAULT 0.0,
    active              INTEGER DEFAULT 1,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS briefings (
    id                      TEXT PRIMARY KEY,
    primary_focus           TEXT NOT NULL,
    active_risk             TEXT DEFAULT '',
    pattern_match           TEXT,
    known_dead_end          TEXT,
    best_next_move          TEXT NOT NULL,
    if_stuck_fallback       TEXT DEFAULT '[]',
    watch_metric            TEXT,
    context_collapse_score  REAL DEFAULT 0.0,
    matched_case_ids        TEXT DEFAULT '[]',
    active_heuristic_ids    TEXT DEFAULT '[]',
    project_id              TEXT REFERENCES projects(id),
    created_at              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS interventions (
    id              TEXT PRIMARY KEY,
    trigger_pattern TEXT NOT NULL,
    message         TEXT NOT NULL,
    suggested_action TEXT NOT NULL,
    guardrail_type  TEXT NOT NULL,
    was_followed    INTEGER,
    outcome_ref     TEXT,
    project_id      TEXT REFERENCES projects(id),
    created_at      TEXT NOT NULL
);

-- ─────────────────────────────────────────
-- RELATIONSHIP / EDGE LAYER
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS edges (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id     TEXT NOT NULL,
    to_id       TEXT NOT NULL,
    edge_type   TEXT NOT NULL,
    -- PRECEDES | CAUSED_BY | CONTRIBUTES_TO | DERIVED_FROM | EVIDENCES
    -- RESOLVES | BELONGS_TO_PROJECT | OCCURRED_IN_SESSION | SIMILAR_TO
    -- CONTRADICTS | SUPPORTS | TRIGGERED | RESULTED_IN
    confidence  REAL DEFAULT 0.5,
    inferred    INTEGER DEFAULT 1,  -- 0=explicit, 1=inferred, 2=weak
    created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_id);
CREATE INDEX IF NOT EXISTS idx_edges_to   ON edges(to_id);
CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(edge_type);

-- ─────────────────────────────────────────
-- SCORING / METRICS
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS context_metrics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          TEXT REFERENCES sessions(id),
    recorded_at         TEXT NOT NULL,
    tool_switch_count   INTEGER DEFAULT 0,
    concurrent_projects INTEGER DEFAULT 0,
    session_fragments   INTEGER DEFAULT 0,
    unresolved_branches INTEGER DEFAULT 0,
    rereads             INTEGER DEFAULT 0,
    minutes_no_artifact INTEGER DEFAULT 0,
    collapse_score      REAL DEFAULT 0.0
);
"""

SEED_PROJECTS_SQL = """
INSERT OR IGNORE INTO projects (id, name, description, status, created_at, updated_at, tags)
VALUES
  ('ironclad',     'IronClad',          'DFY missed-call lead capture for local trades', 'active', datetime('now'), datetime('now'), '["revenue","saas","priority-2"]'),
  ('gettupp-ent',  'GettUpp ENT',       'Nightlife content engine',                       'active', datetime('now'), datetime('now'), '["brand","content","priority-1"]'),
  ('gettupp-girls','GettUpp Girls',     'Nightlife lifestyle and apparel',                'active', datetime('now'), datetime('now'), '["brand","apparel","priority-3"]'),
  ('ai-time-arb',  'AI Time Arbitrage', '3-5 day enterprise app builds via AI playbook',  'active', datetime('now'), datetime('now'), '["consulting","ai","priority-4"]'),
  ('ai-agent-infra','AI Agent Infra',   'Cross-venture AI tooling, memory, and agent infrastructure', 'active', datetime('now'), datetime('now'), '["infra","ai","shared"]'),
  ('openclaw',     'OpenClaw',          'Agent management and orchestration layer',        'active', datetime('now'), datetime('now'), '["agent","infra"]'),
  ('antigravity',  'Antigravity',       'AI toolkit and browser control system',           'active', datetime('now'), datetime('now'), '["agent","infra","tool"]'),
  ('notion-ops',   'Notion Ops',        'Knowledge management, specs, and workspace operations', 'active', datetime('now'), datetime('now'), '["ops","knowledge"]'),
  ('me-ops',       'ME-OPS',            'Personal causal intelligence system',             'active', datetime('now'), datetime('now'), '["meta","intelligence"]');
"""
