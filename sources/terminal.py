#!/usr/bin/env python3
"""ME-OPS Terminal Ingestor — parse shell history into events.

Reads .bash_history or .zsh_history and converts commands into
ME-OPS events, enriching with project detection and tool classification.
Fish history (~/.local/share/fish/fish_history) is detected but not
parsed in this module yet.

Skills used: production-code-audit (path security, input validation),
             workflow-patterns (incremental processing with watermark)

Ref: https://www.gnu.org/software/bash/manual/html_node/Bash-History-Facilities.html
     https://zsh.sourceforge.io/Doc/Release/Options.html#History

Usage:
    python -m sources.terminal                     # Auto-detect shell
    python -m sources.terminal --file ~/.zsh_history
    python -m sources.terminal --shell bash
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "me_ops.duckdb"

# Ensure the parent directory is in the Python path for imports
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from time_utils import local_now, local_date, LOCAL_TZ

# Tool classification rules
TOOL_PATTERNS: dict[str, str] = {
    r"^git\b": "git",
    r"^npm\b|^npx\b|^yarn\b|^pnpm\b": "node_package_manager",
    r"^python\b|^pip\b|^python3\b": "python",
    r"^docker\b|^docker-compose\b": "docker",
    r"^code\b|^cursor\b": "editor",
    r"^curl\b|^wget\b|^http\b": "http_client",
    r"^ssh\b|^scp\b|^rsync\b": "remote",
    r"^cat\b|^less\b|^head\b|^tail\b|^grep\b|^find\b|^rg\b|^fd\b": "file_ops",
    r"^cd\b|^ls\b|^pwd\b|^mkdir\b|^rm\b|^mv\b|^cp\b": "filesystem",
    r"^make\b|^cmake\b|^cargo\b|^go\b": "build",
    r"^sudo\b|^apt\b|^brew\b|^pacman\b": "system",
    r"^firebase\b|^gcloud\b|^aws\b|^az\b": "cloud",
}

# Project detection from paths
PROJECT_PATTERNS: list[tuple[str, str]] = [
    (r"/GettUpp", "GettUpp"),
    (r"/IronClad|/ironclad", "IronClad"),
    (r"/me.ops|/me_ops", "ME-OPS"),
    (r"/Antigravity|/antigravity", "Antigravity IDE"),
]


# ---------------------------------------------------------------------------
# History parsing
# ---------------------------------------------------------------------------

def detect_history_file() -> Path | None:
    """Auto-detect shell history file."""
    home = Path.home()
    candidates = [
        home / ".zsh_history",
        home / ".bash_history",
        home / ".local/share/fish/fish_history",
    ]
    for f in candidates:
        if f.exists() and f.stat().st_size > 0:
            return f
    return None


def parse_bash_history(filepath: Path) -> list[dict]:
    """Parse bash_history (one command per line, optional #timestamp).

    Ref: https://www.gnu.org/software/bash/manual/html_node/Bash-History-Facilities.html
    """
    events: list[dict] = []
    current_ts: str | None = None

    with open(filepath, "r", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Bash HISTTIMEFORMAT: lines starting with #<epoch>
            if line.startswith("#") and str(line)[1:].isdigit():
                epoch = int(str(line)[1:])
                current_ts = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
                continue

            events.append({
                "command": line,
                "timestamp": current_ts,
            })
            current_ts = None

    return events


def parse_zsh_history(filepath: Path) -> list[dict]:
    """Parse zsh_history (extended format: `: <epoch>:<duration>;<command>`).

    Ref: https://zsh.sourceforge.io/Doc/Release/Options.html#History
    """
    events: list[dict] = []
    # zsh extended format regex
    zsh_re = re.compile(r"^: (\d+):(\d+);(.+)$")

    with open(filepath, "r", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            m = zsh_re.match(line)
            if m:
                epoch = int(m.group(1))
                duration = int(m.group(2))
                cmd = m.group(3)
                ts = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
                events.append({
                    "command": cmd,
                    "timestamp": ts,
                    "duration_s": duration,
                })
            else:
                # Plain format or continuation
                events.append({
                    "command": line,
                    "timestamp": None,
                })

    return events


def classify_command(cmd: str) -> tuple[str, str | None]:
    """Classify a command into action + tool.

    Returns (action, tool_name).
    """
    cmd_stripped = cmd.strip()
    # Get first word as base action
    parts = cmd_stripped.split()
    base = parts[0] if parts else cmd_stripped

    # Classify tool
    tool = None
    for pattern, tool_name in TOOL_PATTERNS.items():
        if re.match(pattern, base):
            tool = tool_name
            break

    # Action = first word normalized
    action = f"shell_{base.replace('-', '_')}" if base else "shell_unknown"

    return action, tool


def detect_project(cmd: str) -> str | None:
    """Detect project from command paths."""
    for pattern, project in PROJECT_PATTERNS:
        if re.search(pattern, cmd, re.IGNORECASE):
            return project
    return None


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def ingest_history(
    con: duckdb.DuckDBPyConnection,
    events: list[dict],
) -> int:
    """Insert parsed shell history into events table.

    Idempotent: uses content hash for event_id deduplication.
    """
    count = 0

    for ev in events:
        cmd = ev["command"]
        ts = ev.get("timestamp")

        if not cmd or len(cmd) < 2:
            continue

        # Skip sensitive commands (security guardrail)
        lower_cmd = cmd.lower()
        if any(kw in lower_cmd for kw in [
            "password", "passwd", "secret", "token", "api_key",
            "export GITHUB_TOKEN", "export GEMINI_API_KEY",
        ]):
            continue

        # Generate deterministic event_id from content hash
        hash_input = f"{ts or ''}:{cmd}"
        event_id = f"term_{hashlib.sha256(hash_input.encode()).hexdigest()[:16]}"

        # Check if already ingested
        existing = con.execute(
            "SELECT 1 FROM events WHERE event_id = ?", [event_id]
        ).fetchone()
        if existing:
            continue

        action, tool = classify_command(cmd)
        project = detect_project(cmd)

        # Truncate cmd to 500 chars safely, ensuring it's treated as string
        cmd_str = str(cmd)

        con.execute("""
            INSERT INTO events (event_id, ts_start, action, target,
                               app_tool, source_file)
            VALUES (?, ?, ?, ?, ?, 'shell_history')
        """, [event_id, ts, action, cmd_str[:500], tool or "terminal"])

        # Link to project if detected
        if project:
            proj_row = con.execute(
                "SELECT project_id FROM projects WHERE name = ?",
                [project],
            ).fetchone()
            if proj_row:
                try:
                    con.execute(
                        "INSERT INTO event_projects (event_id, project_id) VALUES (?, ?)",
                        [event_id, proj_row[0]],
                    )
                except Exception:
                    pass  # Duplicate key — safe to ignore

        count += 1

    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ME-OPS Terminal Ingestor")
    parser.add_argument("--file", type=Path, default=None,
                        help="Path to history file")
    parser.add_argument("--shell", choices=["bash", "zsh"], default=None,
                        help="Shell type (auto-detected if not specified)")
    args = parser.parse_args()

    # Determine history file
    filepath = args.file or detect_history_file()
    if not filepath or not filepath.exists():
        print("❌ No shell history file found.")
        print("   Use --file to specify one.")
        sys.exit(1)

    # Determine parser
    shell = args.shell
    if not shell:
        if "zsh" in filepath.name:
            shell = "zsh"
        else:
            shell = "bash"

    print("ME-OPS Terminal Ingestor")
    print("=" * 60)
    print(f"  History file: {filepath}")
    print(f"  Shell type:   {shell}")

    # Parse
    if shell == "zsh":
        events = parse_zsh_history(filepath)
    else:
        events = parse_bash_history(filepath)

    print(f"  Raw commands: {len(events)}")

    # Ingest
    con = duckdb.connect(str(DB_PATH))
    count = ingest_history(con, events)
    con.close()

    print(f"\n{'=' * 60}")
    print(f"✅ Ingested {count} new terminal events")


if __name__ == "__main__":
    main()
