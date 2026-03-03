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
from insights import get_insight_prompts
from dotenv import load_dotenv
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parent / ".env")
from time_utils import DEFAULT_MODEL_ID

DB_PATH = Path(__file__).parent / "me_ops.duckdb"
MODEL_ID = DEFAULT_MODEL_ID


# ---------------------------------------------------------------------------
# System prompt — follows prompt-engineering SKILL best practices:
#   - Role assignment, constraints, output format, examples
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are ME-OPS Agent — a personal analytics assistant AND behavioral coach
for a software developer named Johnny Cage. You have access to a DuckDB
database containing 28K+ behavioral events, modularized workflows, coaching
rules, and daily performance scores.

## Your capabilities
- Query any table in the database using SQL
- Retrieve modularized workflows (use get_workflows tool)
- Check coaching rules and compliance (use get_coaching_rules tool)
- Get daily performance scores (use get_daily_scores tool)
- Analyze temporal patterns, failure modes, and improvement trends

## Modular Workflows (reference by name)
1. Research Loop — 60% effective
2. Annotation Pipeline — 80% effective
3. Deep Browse — 30% effective (DANGER)
4. GettUpp Focus Sprint — 90% effective (BEST)
5. Multi-Project Juggle — 50% effective
6. Late Night Push — 40% effective
7. Query-Driven Investigation — 70% effective
8. IronClad Sprint — 70% effective
9. Tool Exploration / Config — 20% effective (DANGER)
10. Conversation Sprint — 65% effective

## Rules
1. ALWAYS use tools to answer data questions. Never fabricate data.
2. Write DuckDB-compatible SQL. Key tables:
   - events, sessions, projects, tools, files
   - discovered_workflows (10 named modular patterns)
   - coaching_rules (10 evidence-backed rules)
   - daily_scores (4-axis: focus, output, health, consistency)
   - improvement_log (per-day rule compliance tracking)
   - workflow_edges, failure_patterns, anti_playbook, graph_edges
   - context_switches, entity_summary, session_clusters
3. When discussing performance, reference coaching rules by number.
4. When discussing work patterns, use modular workflow names.
5. Timestamps are UTC. User timezone = CST (UTC-6).
6. Present results in clear, concise markdown.
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


def get_coaching_rules() -> str:
    """Retrieve all coaching rules with severity, confidence, and evidence.

    Use this tool when the user asks about their performance rules,
    what they should or shouldn't do, or how to improve.

    Returns:
        JSON array of coaching rules.
    """
    con = _get_connection()
    try:
        result = con.execute("""
            SELECT rule_id, category, rule_text, evidence_count, severity, confidence
            FROM coaching_rules ORDER BY confidence DESC
        """).fetchdf()
        return result.to_json(orient="records", indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        con.close()


def get_daily_scores(date: str = "") -> str:
    """Get daily performance scores (focus, output, health, consistency, composite).

    Args:
        date: Optional YYYY-MM-DD. If empty, returns last 14 days.

    Returns:
        JSON with daily scores across 4 axes plus composite.
    """
    con = _get_connection()
    try:
        if date:
            result = con.execute("""
                SELECT * FROM daily_scores WHERE date = CAST(? AS DATE)
            """, [date]).fetchdf()
        else:
            result = con.execute("""
                SELECT * FROM daily_scores ORDER BY date DESC LIMIT 14
            """).fetchdf()
        return result.to_json(orient="records", date_format="iso", indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        con.close()


def get_workflows() -> str:
    """Retrieve all discovered modular workflows with effectiveness scores.

    Use this when the user asks about their work patterns or process.

    Returns:
        JSON array of modular workflows with names, sequences,
        effectiveness, frequency, and recommendations.
    """
    con = _get_connection()
    try:
        result = con.execute("""
            SELECT workflow_id, name, description, action_sequence,
                   frequency, effectiveness, category, recommendation
            FROM discovered_workflows ORDER BY effectiveness DESC
        """).fetchdf()
        return result.to_json(orient="records", indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Agent runner
# ---------------------------------------------------------------------------

def get_insight_prompts_tool() -> str:
    """Retrieve discovery-based prompts to unlock hidden behavioral insights.
    
    Use this tool when the user asks for inspiration, 'crazy useful' insights,
    or how to unlock deeper understanding of their data.
    """
    return json.dumps(get_insight_prompts(), indent=2, default=str)


# All tools the agent can use
TOOLS = [
    query_database, list_tables, get_session_summary, get_failure_patterns,
    get_coaching_rules, get_daily_scores, get_workflows, get_insight_prompts_tool,
]


def run_agent(user_query: str, *, verbose: bool = False) -> str:
    """Run a single agent query and return the response.

    Uses manual function calling loop (model emits FunctionCall parts,
    we dispatch locally, send FunctionResponse back).

    Ref: https://ai.google.dev/gemini-api/docs/function-calling

    Args:
        user_query: Natural language question about your behavioral data.
        verbose: If True, print tool call details.

    Returns:
        The agent's final text response.
    """
    import time as _time
    from google.genai.errors import ClientError

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: Set GEMINI_API_KEY in .env or environment.", file=sys.stderr)
        sys.exit(1)

    client = genai.Client(api_key=api_key)

    # Tool dispatch table
    tool_fns = {fn.__name__: fn for fn in TOOLS}

    # Declare tools via schema (no auto-calling)
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        tools=TOOLS,
        automatic_function_calling=types.AutomaticFunctionCallingConfig(
            disable=True,
        ),
        temperature=0.1,
    )

    if verbose:
        print(f"  Agent query: {user_query}")
        print(f"  Model: {MODEL_ID}")
        print(f"  Tools: {list(tool_fns.keys())}")
        print()

    # Build conversation history
    contents: list = [types.Content(role="user", parts=[types.Part.from_text(text=user_query)])]

    max_rounds = 10
    for round_num in range(max_rounds):
        # Call model with retry on rate limit
        response = None
        for attempt in range(1, 4):
            try:
                response = client.models.generate_content(
                    model=MODEL_ID,
                    contents=contents,
                    config=config,
                )
                break
            except ClientError as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    wait = 15 * attempt
                    if verbose:
                        print(f"  ⏳ Rate limited (attempt {attempt}/3), retrying in {wait}s...")
                    _time.sleep(wait)
                else:
                    return f"❌ API error: {e}"

        if response is None:
            return "❌ Rate limit exceeded after retries. Try again shortly."

        # Check if model wants to call functions
        candidate = response.candidates[0] if response.candidates else None
        if not candidate or not candidate.content or not candidate.content.parts:
            return "(No response generated)"

        parts = candidate.content.parts
        fn_calls = [p for p in parts if p.function_call]

        # If no function calls, return the text response
        if not fn_calls:
            text_parts = [p.text for p in parts if p.text]
            return "\n".join(text_parts) if text_parts else "(No response generated)"

        # Append model's response (with function calls) to history
        contents.append(candidate.content)

        # Execute each function call and build response parts
        fn_response_parts: list = []
        for fc_part in fn_calls:
            fc = fc_part.function_call
            fn_name = fc.name
            fn_args = dict(fc.args) if fc.args else {}

            if verbose:
                print(f"  🔧 Tool call [{round_num+1}]: {fn_name}({json.dumps(fn_args, default=str)[:200]})")

            if fn_name in tool_fns:
                try:
                    result = tool_fns[fn_name](**fn_args)
                except Exception as e:
                    result = json.dumps({"error": str(e)})
            else:
                result = json.dumps({"error": f"Unknown tool: {fn_name}"})

            fn_response_parts.append(
                types.Part.from_function_response(
                    name=fn_name,
                    response={"result": result},
                )
            )

        # Append function responses to history
        contents.append(types.Content(role="user", parts=fn_response_parts))

    return "❌ Agent exceeded max rounds (possible loop). Try a simpler question."


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
