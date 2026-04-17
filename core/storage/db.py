"""SQLite DB connection and repository base."""
import sqlite3
import json
from pathlib import Path
from datetime import datetime

from core.config import get_env, load_project_env
from .schema import SCHEMA_SQL, SEED_PROJECTS_SQL

load_project_env()


def get_db_path() -> Path:
    return Path(get_env("MEOPS_DB", str(Path.home() / "me-ops" / "meops.db")))


def get_conn() -> sqlite3.Connection:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript(SCHEMA_SQL)
    conn.executescript(SEED_PROJECTS_SQL)
    conn.commit()
    conn.close()
    print(f"[DB] Initialized at {get_db_path()}")


def ts() -> str:
    return datetime.utcnow().isoformat() + "Z"


class EntityRepo:
    """CRUD for the canonical entities table."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def ensure_project(self, project_id: str):
        """Auto-create project row if it doesn't exist."""
        if not project_id:
            return
        self.conn.execute("""
            INSERT OR IGNORE INTO projects
              (id, name, description, status, created_at, updated_at, tags)
            VALUES (?,?,?,?,datetime('now'),datetime('now'),'[]')
        """, (project_id, project_id.replace("-"," ").title(), "", "active"))
        self.conn.commit()

    def upsert(self, obj: dict):
        if obj.get("project_id"):
            self.ensure_project(obj["project_id"])
        data = {k: v for k, v in obj.items()
                if k not in ("id","type","source","source_refs","created_at","updated_at",
                             "timestamp_start","timestamp_end","project_id","session_id",
                             "actor","confidence","evidence_refs","tags","summary")}
        self.conn.execute("""
            INSERT INTO entities
              (id,type,source,source_refs,created_at,updated_at,timestamp_start,
               timestamp_end,project_id,session_id,actor,confidence,evidence_refs,
               tags,summary,data)
            VALUES
              (:id,:type,:source,:source_refs,:created_at,:updated_at,:timestamp_start,
               :timestamp_end,:project_id,:session_id,:actor,:confidence,:evidence_refs,
               :tags,:summary,:data)
            ON CONFLICT(id) DO UPDATE SET
              updated_at=excluded.updated_at,
              timestamp_start=excluded.timestamp_start,
              timestamp_end=excluded.timestamp_end,
              project_id=excluded.project_id,
              session_id=excluded.session_id,
              confidence=excluded.confidence,
              source_refs=excluded.source_refs,
              tags=excluded.tags,
              data=excluded.data,
              summary=excluded.summary
        """, {
            "id":            obj["id"],
            "type":          obj.get("type","event"),
            "source":        obj.get("source","pieces"),
            "source_refs":   json.dumps(obj.get("source_refs",[])),
            "created_at":    obj.get("created_at", ts()),
            "updated_at":    ts(),
            "timestamp_start": obj.get("timestamp_start"),
            "timestamp_end":   obj.get("timestamp_end"),
            "project_id":    obj.get("project_id"),
            "session_id":    obj.get("session_id"),
            "actor":         obj.get("actor","user"),
            "confidence":    obj.get("confidence",0.0),
            "evidence_refs": json.dumps(obj.get("evidence_refs",[])),
            "tags":          json.dumps(obj.get("tags",[])),
            "summary":       obj.get("summary",""),
            "data":          json.dumps(data),
        })
        self.conn.commit()

    def get(self, entity_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM entities WHERE id=?", (entity_id,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["data"] = json.loads(d.get("data","{}"))
        d["tags"] = json.loads(d.get("tags","[]"))
        d["evidence_refs"] = json.loads(d.get("evidence_refs","[]"))
        return d

    def find_by_type(self, entity_type: str, project_id: str = None,
                     limit: int = 100) -> list:
        q = "SELECT * FROM entities WHERE type=?"
        params = [entity_type]
        if project_id:
            q += " AND project_id=?"
            params.append(project_id)
        q += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(q, params).fetchall()
        return [dict(r) for r in rows]

    def add_edge(self, from_id: str, to_id: str, edge_type: str, confidence: float = 0.5):
        self.conn.execute("""
            INSERT INTO edges (from_id,to_id,edge_type,confidence,created_at)
            VALUES (?,?,?,?,?)
        """, (from_id, to_id, edge_type, confidence, ts()))
        self.conn.commit()
