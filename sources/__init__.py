"""ME-OPS Sources — cross-device data ingestors.

Each module in this package pulls behavioral data from an external
source and writes it into the ME-OPS DuckDB warehouse.

Available ingestors:
    github.py    — GitHub API (commits, PRs, issues)
    terminal.py  — Shell history (.bash_history / .zsh_history)

Skills used: production-code-audit (env var security, error handling),
             ai-engineer (data pipeline architecture)
"""
