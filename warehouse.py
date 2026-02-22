#!/usr/bin/env python3
"""ME-OPS Tactical Persistence Layer.

Handles the storage of refined intelligence snapshots in me_ops.duckdb.
"""

from __future__ import annotations

from typing import Any, Dict

import duckdb

WAREHOUSE_DDL = """
-- Refined AI Narratives
CREATE TABLE IF NOT EXISTS intelligence_briefings (
    snapshot_date    DATE PRIMARY KEY,
    narrative_text   VARCHAR NOT NULL,
    model_id         VARCHAR,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Site-level intelligence metrics
CREATE TABLE IF NOT EXISTS site_intelligence (
    snapshot_date    DATE NOT NULL,
    site             VARCHAR NOT NULL,
    action           VARCHAR NOT NULL,
    event_count      INTEGER DEFAULT 0,
    impact_score     DOUBLE DEFAULT 0.0,
    PRIMARY KEY (snapshot_date, site, action)
);

-- Refined bottlenecks and friction points
CREATE TABLE IF NOT EXISTS intelligence_bottlenecks (
    snapshot_date    DATE NOT NULL,
    engine           VARCHAR NOT NULL,
    marker_name      VARCHAR NOT NULL,
    impact_score     DOUBLE DEFAULT 0.0,
    description      VARCHAR,
    PRIMARY KEY (snapshot_date, engine, marker_name)
);

-- Tactical prioritized actions
CREATE TABLE IF NOT EXISTS tactical_action_queue (
    snapshot_date    DATE NOT NULL,
    priority         INTEGER NOT NULL,
    action_text      VARCHAR NOT NULL,
    impact_score     DOUBLE DEFAULT 0.0,
    source_insight   VARCHAR,
    PRIMARY KEY (snapshot_date, priority)
);
"""


def init_warehouse(con: duckdb.DuckDBPyConnection) -> None:
    """Initialize the integrated intelligence tables."""
    con.execute(WAREHOUSE_DDL)


def persist_intelligence_snapshot(
    con: duckdb.DuckDBPyConnection,
    snapshot_date: str,
    payloads: Dict[str, Any],
) -> None:
    """Persists a full intelligence snapshot from all engines.

    Args:
        con: Active DuckDB connection.
        snapshot_date: Date string (YYYY-MM-DD).
        payloads: Dictionary containing:
            - briefings: {narrative_text, model_id}
            - site_intelligence: List of {site, action, count, impact}
            - bottlenecks: List of {engine, name, impact, description}
            - action_queue: List of {priority, action, impact, source}
    """
    print(f"  [Warehouse] Persisting snapshot for {snapshot_date}...")

    con.execute("BEGIN TRANSACTION")
    try:
        # 1. Briefings
        briefing = payloads.get("briefings")
        if briefing:
            con.execute("DELETE FROM intelligence_briefings WHERE snapshot_date = ?", [snapshot_date])
            con.execute(
                """
                INSERT INTO intelligence_briefings (snapshot_date, narrative_text, model_id)
                VALUES (?, ?, ?)
                """,
                [snapshot_date, briefing.get("narrative_text"), briefing.get("model_id")],
            )

        # 2. Site Intelligence
        sites = payloads.get("site_intelligence", [])
        if sites:
            con.execute("DELETE FROM site_intelligence WHERE snapshot_date = ?", [snapshot_date])
            rows = [
                [snapshot_date, s["site"], s["action"], s["event_count"], s["impact_score"]]
                for s in sites
            ]
            con.executemany(
                """
                INSERT INTO site_intelligence (snapshot_date, site, action, event_count, impact_score)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )

        # 3. Bottlenecks
        bottlenecks = payloads.get("bottlenecks", [])
        if bottlenecks:
            con.execute("DELETE FROM intelligence_bottlenecks WHERE snapshot_date = ?", [snapshot_date])
            rows = [
                [snapshot_date, b["engine"], b["marker_name"], b["impact_score"], b["description"]]
                for b in bottlenecks
            ]
            con.executemany(
                """
                INSERT INTO intelligence_bottlenecks (snapshot_date, engine, marker_name, impact_score, description)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )

        # 4. Tactical Action Queue
        queue = payloads.get("action_queue", [])
        if queue:
            con.execute("DELETE FROM tactical_action_queue WHERE snapshot_date = ?", [snapshot_date])
            rows = [
                [snapshot_date, q["priority"], q["action_text"], q["impact_score"], q["source_insight"]]
                for q in queue
            ]
            con.executemany(
                """
                INSERT INTO tactical_action_queue (snapshot_date, priority, action_text, impact_score, source_insight)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )

        con.execute("COMMIT")
        print("  [Warehouse] Success.")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"  [Warehouse] Error: {e}")
        raise
