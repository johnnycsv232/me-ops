#!/usr/bin/env python3
"""ME-OPS Personal Agent — Gemini-powered natural-language interface to your data.

Uses the google-genai SDK (GA) with automatic function calling to let
Gemini query your DuckDB behavioral database in natural language.

Skills used: ai-engineer (agent architecture + tool definition),
             prompt-engineering (system prompt design),
             software-architecture (clean separation of concerns)

Setup:
    1. Set GEMINI_API_KEY in .env or environment
    2. python agent.py "What did I work on last night?"

Ref: https://ai.google.dev/gemini-api/docs/function-calling (official)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parent / ".env")

DB_PATH = Path(__file__).parent / "me_ops.duckdb"
MODEL_ID = "gemini-2.5-flash"  # fast + tool-calling capable


# ---------------------------------------------------------------------------
# System prompt — follows prompt-engineering SKILL best practices:
#   - Role assignment, constraints, output format, examples
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are ME-OPS Agent — a personal analytics assistant for a software developer.
You have access to a DuckDB database containing the user's behavioral data:
events, sessions, projects, tools, files, tags, workflow patterns, and more.

## Your capabilities
- Query any table in the database using SQL
- Analyze temporal patterns (late nights, session durations, context switches)
- Identify projects, tools, and files the user works with
- Detect failure patterns and anti-playbook violations
- Explore the knowledge graph of relationships between entities

## Rules
1. ALWAYS use the query_database tool to answer questions about the user's data.
2. Write DuckDB-compatible SQL. Key tables:
   - events (event_id, ts_start, ts_end, action, target, duration_ms, outcome_label)
   - sessions (session_id, ts_start, ts_end, duration_min, event_count, projects)
   - projects (project_id, name)
   - tools (tool_id, name, category)
   - files (file_id, fullpath, extension, repo_root)
   - workflow_edges (from_action, to_action, weight)
   - failure_patterns (pattern_type, description, severity)
   - anti_playbook (rule_text, trigger, evidence)
   - graph_edges (src_type, src_id, rel, dst_type, dst_id, weight)
   - context_switches (ts, from_project, to_project, gap_seconds)
3. Present results in clear, concise markdown.
4. If a query returns no results, explain why and suggest alternatives.
5. For temporal queries, timestamps are in UTC. Adjust for the user's timezone (CST/CDT = UTC-6).
6. Do NOT fabricate data. Only report what the database returns.
"""


# ---------------------------------------------------------------------------
# Tool definitions — google-genai automatic function calling
# ---------------------------------------------------------------------------

def _get_connection() -> duckdb.DuckDBPyConnection:
    """Get a read-only DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def query_database(sql: str) -> str:
    """Execute a read-only SQL query against the ME-OPS DuckDB database.

    Use this tool to answer ANY question about the user's behavioral data.
    Write valid DuckDB SQL. The database contains tables: events, sessions,
    projects, tools, files, workflow_edges, failure_patterns, anti_playbook,
    graph_edges, context_switches, entity_summary, tag_stats, and more.

    Args:
        sql: A valid DuckDB SQL query string (read-only, no INSERT/UPDATE/DELETE).

    Returns:
        JSON string with query results (list of dicts) or error message.
    """
    con = _get_connection()
    try:
        # Safety: reject mutating queries
        normalized = sql.strip().upper()
        if any(normalized.startswith(kw) for kw in [
            "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE"
        ]):
            return json.dumps({"error": "Mutating queries are not allowed."})

        result = con.execute(sql).fetchdf()
        # Limit output to avoid token overflow
        if len(result) > 50:
            result = result.head(50)
            return json.dumps({
                "data": json.loads(result.to_json(orient="records", date_format="iso")),
                "note": f"Showing first 50 of {len(result)} rows. Refine your query for more specific results."
            }, indent=2)
        return result.to_json(orient="records", date_format="iso", indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        con.close()


def list_tables() -> str:
    """List all available tables and their columns in the ME-OPS database.

    Use this tool FIRST if you need to discover what data is available
    before writing a SQL query.

    Returns:
        JSON string mapping table names to their column definitions.
    """
    con = _get_connection()
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        schema: dict[str, list[dict]] = {}
        for (table_name,) in tables:
            cols = con.execute(f"DESCRIBE {table_name}").fetchall()
            schema[table_name] = [
                {"name": c[0], "type": c[1]} for c in cols
            ]
        return json.dumps(schema, indent=2)
    finally:
        con.close()


def get_session_summary(date: str) -> str:
    """Get a summary of work sessions for a specific date.

    Args:
        date: Date in YYYY-MM-DD format.

    Returns:
        JSON with session count, total hours, top projects, and dominant actions.
    """
    con = _get_connection()
    try:
        result = con.execute("""
            SELECT
                COUNT(*) AS total_sessions,
                ROUND(SUM(duration_min) / 60.0, 1) AS total_hours,
                SUM(event_count) AS total_events,
                STRING_AGG(DISTINCT dominant_action, ', ') AS actions
            FROM sessions
            WHERE CAST(ts_start AS DATE) = CAST(? AS DATE)
        """, [date]).fetchdf()

        projects = con.execute("""
            SELECT DISTINCT projects FROM sessions
            WHERE CAST(ts_start AS DATE) = CAST(? AS DATE)
              AND projects IS NOT NULL AND projects != ''
        """, [date]).fetchall()

        all_projects = set()
        for (p,) in projects:
            for name in p.split(","):
                name = name.strip()
                if name:
                    all_projects.add(name)

        summary = json.loads(result.to_json(orient="records"))[0]
        summary["projects"] = sorted(all_projects)
        return json.dumps(summary, indent=2)
    finally:
        con.close()


def get_failure_patterns() -> str:
    """Retrieve all detected failure patterns and anti-playbook rules.

    Returns:
        JSON with failure patterns and anti-playbook violations.
    """
    con = _get_connection()
    try:
        patterns = con.execute("""
            SELECT pattern_type, description, severity, evidence_count
            FROM failure_patterns
            ORDER BY evidence_count DESC
        """).fetchdf()

        rules = con.execute("""
            SELECT rule_text, trigger, confidence
            FROM anti_playbook
            ORDER BY confidence DESC
        """).fetchdf()

        return json.dumps({
            "failure_patterns": json.loads(patterns.to_json(orient="records")),
            "anti_playbook_rules": json.loads(rules.to_json(orient="records")),
        }, indent=2)
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Agent runner
# ---------------------------------------------------------------------------

# All tools the agent can use
TOOLS = [query_database, list_tables, get_session_summary, get_failure_patterns]


def run_agent(user_query: str, *, verbose: bool = False) -> str:
    """Run a single agent query and return the response.

    Uses google-genai automatic function calling per official docs:
    https://ai.google.dev/gemini-api/docs/function-calling

    Args:
        user_query: Natural language question about your behavioral data.
        verbose: If True, print tool call details.

    Returns:
        The agent's final text response.
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: Set GEMINI_API_KEY in .env or environment.", file=sys.stderr)
        sys.exit(1)

    client = genai.Client(api_key=api_key)

    # Configure automatic function calling per official docs
    # Ref: google-genai SDK types.GenerateContentConfig
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        tools=TOOLS,
        temperature=0.1,  # Low temp for analytical accuracy
    )

    if verbose:
        print(f"  Agent query: {user_query}")
        print(f"  Model: {MODEL_ID}")
        print(f"  Tools: {[t.__name__ for t in TOOLS]}")
        print()

    response = client.models.generate_content(
        model=MODEL_ID,
        contents=user_query,
        config=config,
    )

    return response.text or "(No response generated)"


def interactive_mode() -> None:
    """REPL for chatting with the agent."""
    print("ME-OPS Agent — Interactive Mode")
    print("=" * 50)
    print("Ask questions about your behavioral data.")
    print("Type 'quit' to exit.\n")

    while True:
        try:
            query = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not query or query.lower() in ("quit", "exit", "q"):
            break

        print()
        result = run_agent(query, verbose=True)
        print(result)
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if "--interactive" in sys.argv or "-i" in sys.argv:
        interactive_mode()
        return

    # Single query from command line args
    query = " ".join(arg for arg in sys.argv[1:] if not arg.startswith("-"))
    if not query:
        print("Usage:")
        print('  python agent.py "What did I work on yesterday?"')
        print('  python agent.py --interactive')
        print('  python agent.py --verbose "Show my late night patterns"')
        return

    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    result = run_agent(query, verbose=verbose)
    print(result)


if __name__ == "__main__":
    main()
