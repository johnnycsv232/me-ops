"""Dependency-free UI server for browsing ME-OPS state."""
from __future__ import annotations

import json
import mimetypes
import sqlite3
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from core.config import PROJECT_ROOT
from core.recall.gemini_embed import cosine_search
from core.storage.db import get_conn, get_db_path

STATIC_DIR = Path(__file__).resolve().parent / "static"


def run_server(host: str = "127.0.0.1", port: int = 8008) -> None:
    server = ThreadingHTTPServer((host, port), MeOpsHandler)
    print(f"[ui] Serving ME-OPS UI at http://{host}:{port}")
    server.serve_forever()


class MeOpsHandler(BaseHTTPRequestHandler):
    server_version = "MEOPSUI/0.1"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            return self._serve_static("index.html")
        if parsed.path.startswith("/static/"):
            return self._serve_static(parsed.path.replace("/static/", "", 1))
        if parsed.path == "/api/overview":
            return self._send_json(_get_overview())
        if parsed.path == "/api/events":
            params = parse_qs(parsed.query)
            return self._send_json(_get_events(
                limit=_int_arg(params, "limit", 80),
                project=_str_arg(params, "project"),
                query=_str_arg(params, "q"),
            ))
        if parsed.path == "/api/cases":
            params = parse_qs(parsed.query)
            return self._send_json(_get_cases(
                limit=_int_arg(params, "limit", 40),
                kind=_str_arg(params, "kind"),
                project=_str_arg(params, "project"),
            ))
        if parsed.path == "/api/decisions":
            params = parse_qs(parsed.query)
            return self._send_json(_get_entities("decision", limit=_int_arg(params, "limit", 30)))
        if parsed.path == "/api/interventions":
            params = parse_qs(parsed.query)
            return self._send_json(_get_entities("intervention", limit=_int_arg(params, "limit", 20)))
        if parsed.path == "/api/recall":
            params = parse_qs(parsed.query)
            query = _str_arg(params, "q")
            if not query:
                return self._send_error(HTTPStatus.BAD_REQUEST, "Missing q parameter")
            limit = _int_arg(params, "limit", 5)
            return self._send_json({"query": query, "results": cosine_search(query, top_k=limit)})
        return self._send_error(HTTPStatus.NOT_FOUND, "Not found")

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return None

    def _serve_static(self, relative_path: str) -> None:
        file_path = (STATIC_DIR / relative_path).resolve()
        if not str(file_path).startswith(str(STATIC_DIR)) or not file_path.exists():
            return self._send_error(HTTPStatus.NOT_FOUND, "Static asset not found")
        content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        content = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_json(self, payload: dict | list) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status: HTTPStatus, message: str) -> None:
        body = json.dumps({"error": message}).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _get_overview() -> dict:
    conn = get_conn()
    stats = {
        "events": _scalar(conn, "SELECT COUNT(*) FROM entities WHERE type='event'"),
        "failures": _scalar(conn, "SELECT COUNT(*) FROM entities WHERE type='failure'"),
        "outcomes": _scalar(conn, "SELECT COUNT(*) FROM entities WHERE type='outcome'"),
        "decisions": _scalar(conn, "SELECT COUNT(*) FROM entities WHERE type='decision'"),
        "interventions": _scalar(conn, "SELECT COUNT(*) FROM entities WHERE type='intervention'"),
        "sessions": _scalar(conn, "SELECT COUNT(*) FROM sessions"),
        "failure_chains": _scalar(conn, "SELECT COUNT(*) FROM cases WHERE case_kind='failure_chain'"),
        "win_signatures": _scalar(conn, "SELECT COUNT(*) FROM cases WHERE case_kind='win_signature'"),
        "embeddings": _scalar(conn, "SELECT COUNT(*) FROM embeddings"),
    }
    projects = [
        {"project_id": row["project_id"] or "unknown", "count": row["n"]}
        for row in conn.execute(
            "SELECT project_id, COUNT(*) n FROM entities WHERE type='event' "
            "GROUP BY project_id ORDER BY n DESC"
        ).fetchall()
    ]
    recent_events = _query_events(conn, limit=8)
    recent_cases = _query_cases(conn, limit=8)
    recent_interventions = _query_entities(conn, "intervention", limit=5)
    conn.close()
    return {
        "stats": stats,
        "projects": projects,
        "recent_events": recent_events,
        "recent_cases": recent_cases,
        "recent_interventions": recent_interventions,
        "db_path": str(get_db_path()),
        "project_root": str(PROJECT_ROOT),
    }


def _get_events(limit: int, project: str | None, query: str | None) -> dict:
    conn = get_conn()
    items = _query_events(conn, limit=limit, project=project, query=query)
    conn.close()
    return {"items": items}


def _get_cases(limit: int, kind: str | None, project: str | None) -> dict:
    conn = get_conn()
    items = _query_cases(conn, limit=limit, kind=kind, project=project)
    conn.close()
    return {"items": items}


def _get_entities(entity_type: str, limit: int) -> dict:
    conn = get_conn()
    items = _query_entities(conn, entity_type, limit=limit)
    conn.close()
    return {"items": items}


def _query_events(conn: sqlite3.Connection, limit: int, project: str | None = None, query: str | None = None) -> list[dict]:
    sql = """
        SELECT id, project_id, created_at, session_id, summary, data
        FROM entities
        WHERE type='event'
    """
    params: list = []
    if project and project != "all":
        sql += " AND COALESCE(project_id, 'unknown') = ?"
        params.append(project)
    if query:
        sql += " AND (summary LIKE ? OR data LIKE ?)"
        needle = f"%{query}%"
        params.extend([needle, needle])
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [_event_payload(row) for row in rows]


def _query_cases(conn: sqlite3.Connection, limit: int, kind: str | None = None, project: str | None = None) -> list[dict]:
    sql = """
        SELECT id, case_kind, title, symptom, project_id, recurrence, confidence, created_at, data
        FROM cases
        WHERE 1=1
    """
    params: list = []
    if kind and kind != "all":
        sql += " AND case_kind=?"
        params.append(kind)
    if project and project != "all":
        sql += " AND COALESCE(project_id, 'unknown')=?"
        params.append(project)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    items = []
    for row in rows:
        data = json.loads(row["data"] or "{}")
        items.append({
            "id": row["id"],
            "case_kind": row["case_kind"],
            "title": row["title"],
            "symptom": row["symptom"] or "",
            "project_id": row["project_id"] or "unknown",
            "recurrence": row["recurrence"],
            "confidence": row["confidence"],
            "created_at": row["created_at"],
            "reusable_fix": data.get("reusable_fix", []),
        })
    return items


def _query_entities(conn: sqlite3.Connection, entity_type: str, limit: int) -> list[dict]:
    rows = conn.execute("""
        SELECT id, project_id, created_at, summary, data
        FROM entities
        WHERE type=?
        ORDER BY created_at DESC
        LIMIT ?
    """, (entity_type, limit)).fetchall()
    items = []
    for row in rows:
        data = json.loads(row["data"] or "{}")
        items.append({
            "id": row["id"],
            "project_id": row["project_id"] or "unknown",
            "created_at": row["created_at"],
            "summary": row["summary"],
            "details": data,
        })
    return items


def _event_payload(row: sqlite3.Row) -> dict:
    data = json.loads(row["data"] or "{}")
    return {
        "id": row["id"],
        "project_id": row["project_id"] or "unknown",
        "created_at": row["created_at"],
        "session_id": row["session_id"],
        "summary": row["summary"],
        "event_kind": data.get("event_kind", "unknown"),
        "tools": data.get("tools", []),
        "snippet": (data.get("raw_content") or "")[:300],
    }


def _scalar(conn: sqlite3.Connection, query: str) -> int:
    return conn.execute(query).fetchone()[0]


def _int_arg(params: dict, key: str, default: int) -> int:
    try:
        return int(params.get(key, [default])[0])
    except Exception:
        return default


def _str_arg(params: dict, key: str) -> str | None:
    value = params.get(key, [None])[0]
    return value.strip() if isinstance(value, str) and value.strip() else None


if __name__ == "__main__":
    run_server()
