"""
Pieces → ME-OPS bridge using PROVEN MCP protocol.
Uses SSE + JSON-RPC (same as our working Node.js tests).
No raw REST — MCP batch_snapshot is the reliable path.
"""
import json, threading, time, uuid, hashlib, http.client, urllib.parse
from datetime import datetime, timezone
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.storage.db import get_conn, init_db, ts, EntityRepo
from core.ledger.classify import classify_project, classify_event_kind, extract_tools

PIECES_HOST = "localhost"
PIECES_PORT = 39300
SSE_PATH    = "/model_context_protocol/2024-11-05/sse"


def _open_session():
    """Open SSE connection, return (conn, session_url_path)."""
    conn = http.client.HTTPConnection(PIECES_HOST, PIECES_PORT, timeout=15)
    conn.request("GET", SSE_PATH, headers={"Accept": "text/event-stream"})
    resp = conn.getresponse()
    buf = b""
    while True:
        chunk = resp.read(1)
        if not chunk:
            break
        buf += chunk
        if b"\n" in buf:
            line = buf.decode("utf-8", errors="replace").strip()
            buf = b""
            if line.startswith("data:"):
                path = line[5:].strip()
                if path.startswith("/model_context_protocol"):
                    return conn, resp, path
    raise RuntimeError("Could not get SSE session path")


def _mcp_call(conn, resp, msg_path: str, call_id: int, tool: str, args: dict) -> dict:
    """Send one MCP tool call, read response from SSE stream."""
    body = json.dumps({
        "jsonrpc": "2.0", "id": call_id,
        "method": "tools/call",
        "params": {"name": tool, "arguments": args}
    }).encode()
    # parse URL params from msg_path
    parsed = urllib.parse.urlparse("http://localhost" + msg_path)
    post_conn = http.client.HTTPConnection(PIECES_HOST, PIECES_PORT, timeout=30)
    post_conn.request("POST", msg_path, body=body, headers={"Content-Type": "application/json"})
    post_resp = post_conn.getresponse()
    post_resp.read()  # consume, result comes via SSE
    post_conn.close()
    # read SSE for result
    buf = b""
    deadline = time.time() + 30
    while time.time() < deadline:
        chunk = resp.read(1)
        if not chunk:
            time.sleep(0.01)
            continue
        buf += chunk
        if b"\n" in buf:
            line = buf.decode("utf-8", errors="replace").strip()
            buf = b""
            if line.startswith("data:"):
                raw = line[5:].strip()
                try:
                    parsed_msg = json.loads(raw)
                    if parsed_msg.get("id") == call_id:
                        return parsed_msg.get("result", {})
                except Exception:
                    continue
    return {}


def _result_json(result: dict) -> dict:
    raw = (result.get("content") or [{}])[0].get("text", "{}")
    return json.loads(raw) if raw else {}


def fetch_all_summary_ids_mcp() -> list:
    """Get all summary IDs via material_identifiers MCP tool."""
    conn, resp, msg_path = _open_session()
    result = _mcp_call(conn, resp, msg_path, 1, "material_identifiers", {
        "material_type": "WORKSTREAM_SUMMARIES",
        "limit": 1000
    })
    data = _result_json(result)
    ids = data.get("identifiers", [])
    conn.close()
    return ids


def fetch_summaries_batch_mcp(ids: list) -> list:
    """Batch fetch summary content via workstream_summaries_batch_snapshot."""
    conn, resp, msg_path = _open_session()
    result = _mcp_call(conn, resp, msg_path, 2, "workstream_summaries_batch_snapshot", {
        "identifiers": ids[:50]
    })
    data = _result_json(result)
    items = data.get("items", [])
    conn.close()
    return items


def fetch_annotations_batch_mcp(ids: list) -> list:
    """Batch fetch annotations by ID."""
    if not ids:
        return []
    conn, resp, msg_path = _open_session()
    result = _mcp_call(conn, resp, msg_path, 3, "annotations_batch_snapshot", {
        "identifiers": ids[:100]
    })
    data = _result_json(result)
    items = data.get("items", [])
    conn.close()
    return items


def fetch_annotations_batch_mcp(ids: list) -> list:
    """Batch fetch annotation content for summary-linked annotation ids."""
    if not ids:
        return []
    conn, resp, msg_path = _open_session()
    result = _mcp_call(conn, resp, msg_path, 3, "annotations_batch_snapshot", {
        "identifiers": ids[:50]
    })
    raw = (result.get("content") or [{}])[0].get("text", "{}")
    data = json.loads(raw) if raw else {}
    items = data.get("items", [])
    conn.close()
    return items
