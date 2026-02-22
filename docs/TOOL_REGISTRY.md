# Tool Registry

## 1. Native Agent Capabilities
- **Filesystem**: `read_file`, `write_to_file`, `replace_file_content`, `list_dir`, `find_by_name`, `grep_search`
- **Shell / Terminal**: `run_command`, `send_command_input`, `command_status`, `read_terminal`
- **Git Operations**: Native via `run_command` (`git status`, `git log`, `git diff`)
- **Web Lookups**: `search_web`, `read_url_content`, `browser_subagent` (for official docs validation)
- **Sequential Thinking**: `sequentialthinking` (for complex logic trees)

## 2. Model Context Protocol (MCP) Servers
- **StitchMCP**: UI/Dashboard generation and web styling tasks.
- **atlassian-mcp-server**: Jira/Confluence integration for issue tracking.
- **firebase-mcp-server**: Firebase project configuration, DB rules, and SDK integrations.
- **genkit-mcp-server**: AI flow and framework testing.
- **github-mcp-server**: Advanced GitHub repository queries, PR creation, and issue management.
- **sequential-thinking**: Advanced dynamic reasoning capabilities.

## 3. Discovered Local Environment Tools
- **Python Tooling**: `ruff` / `pyright` / `pytest` (Inferred for this Python environment, need to verify).
- **Database Clients**: duckdb (embedded database `.duckdb` files discovered).
- **Security Scanners**: standard dependency audit/inspect to be checked.

## 4. Tool Reliability Policy
- Prefer exact AST / direct file manipulation (`replace_file_content`) over `sed`.
- Prefer `grep_search` and `find_by_name` over bash equivalents to prevent escaping errors.
- If any tool errors, retry once with corrected arguments. If still failing, fallback to `run_command` with careful error handling.
