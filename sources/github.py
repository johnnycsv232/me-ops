#!/usr/bin/env python3
"""ME-OPS GitHub Ingestor — pull commit, PR, and issue data.

Fetches activity from the GitHub API and inserts into the ME-OPS
DuckDB warehouse as new events.

Skills used: production-code-audit (token from env, rate limit handling),
             ai-engineer (incremental ingest pattern)

Ref: https://docs.github.com/en/rest (official GitHub REST API)
     https://docs.github.com/en/rest/commits/commits (commits endpoint)

Setup:
    1. Set GITHUB_TOKEN in .env (personal access token with repo scope)
    2. python -m sources.github --repos johnnycsv232/me-ops

Usage:
    python -m sources.github --repos owner/repo1 owner/repo2
    python -m sources.github --repos owner/repo --since 2026-02-01
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DB_PATH = Path(__file__).resolve().parent.parent / "me_ops.duckdb"


# ---------------------------------------------------------------------------
# GitHub API client
# ---------------------------------------------------------------------------

def _gh_headers() -> dict[str, str]:
    """Build auth headers. Token from env for security (never hardcode).

    Ref: https://docs.github.com/en/rest/authentication
    """
    token = os.environ.get("GITHUB_TOKEN")
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        print("  ⚠️  GITHUB_TOKEN not set — rate-limited to 60 req/hr",
              file=sys.stderr)
    return headers


def _gh_get(url: str, params: dict | None = None) -> list[dict]:
    """GET from GitHub API with pagination.

    Ref: https://docs.github.com/en/rest/guides/using-pagination
    """
    import urllib.request
    import urllib.parse

    headers = _gh_headers()
    all_results: list[dict] = []

    while url:
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
            params = None  # Only use params on first request

        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())
                if isinstance(data, list):
                    all_results.extend(data)
                else:
                    all_results.append(data)

                # Check for pagination Link header
                link_header = resp.headers.get("Link", "")
                url = None
                if 'rel="next"' in link_header:
                    for part in link_header.split(","):
                        if 'rel="next"' in part:
                            url = part.split("<")[1].split(">")[0]
                            break
        except Exception as e:
            print(f"  ❌ GitHub API error: {e}", file=sys.stderr)
            break

    return all_results


# ---------------------------------------------------------------------------
# Ingest functions
# ---------------------------------------------------------------------------

def ingest_commits(
    con: duckdb.DuckDBPyConnection,
    owner: str,
    repo: str,
    since: str | None = None,
) -> int:
    """Fetch commits and insert as events.

    Ref: https://docs.github.com/en/rest/commits/commits#list-commits
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    params: dict = {"per_page": "100"}
    if since:
        params["since"] = since

    commits = _gh_get(url, params)
    count = 0

    for c in commits:
        sha = c.get("sha", "")
        msg = c.get("commit", {}).get("message", "")
        author = c.get("commit", {}).get("author", {})
        ts = author.get("date", "")

        event_id = f"gh_commit_{sha[:12]}"

        # Check if already ingested (idempotent)
        existing = con.execute(
            "SELECT 1 FROM events WHERE event_id = ?", [event_id]
        ).fetchone()
        if existing:
            continue

        con.execute("""
            INSERT INTO events (event_id, ts_start, action, target,
                               app_tool, source_file, outcome_label)
            VALUES (?, ?, 'git_commit', ?, 'github', ?, 'success')
        """, [event_id, ts, msg[:500], f"{owner}/{repo}"])
        count += 1

    return count


def ingest_prs(
    con: duckdb.DuckDBPyConnection,
    owner: str,
    repo: str,
    state: str = "all",
) -> int:
    """Fetch pull requests and insert as events.

    Ref: https://docs.github.com/en/rest/pulls/pulls#list-pull-requests
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
    params = {"per_page": "100", "state": state}

    prs = _gh_get(url, params)
    count = 0

    for pr in prs:
        pr_num = pr.get("number", 0)
        event_id = f"gh_pr_{owner}_{repo}_{pr_num}"

        existing = con.execute(
            "SELECT 1 FROM events WHERE event_id = ?", [event_id]
        ).fetchone()
        if existing:
            continue

        title = pr.get("title", "")
        state_val = pr.get("state", "")
        ts = pr.get("created_at", "")

        con.execute("""
            INSERT INTO events (event_id, ts_start, action, target,
                               app_tool, source_file, outcome_label)
            VALUES (?, ?, 'pull_request', ?, 'github', ?, ?)
        """, [event_id, ts, title[:500], f"{owner}/{repo}", state_val])
        count += 1

    return count


def ingest_issues(
    con: duckdb.DuckDBPyConnection,
    owner: str,
    repo: str,
    state: str = "all",
) -> int:
    """Fetch issues and insert as events.

    Ref: https://docs.github.com/en/rest/issues/issues#list-repository-issues
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    params = {"per_page": "100", "state": state}

    issues = _gh_get(url, params)
    count = 0

    for issue in issues:
        if "pull_request" in issue:
            continue  # Skip PRs (they appear in issues endpoint too)

        issue_num = issue.get("number", 0)
        event_id = f"gh_issue_{owner}_{repo}_{issue_num}"

        existing = con.execute(
            "SELECT 1 FROM events WHERE event_id = ?", [event_id]
        ).fetchone()
        if existing:
            continue

        title = issue.get("title", "")
        state_val = issue.get("state", "")
        ts = issue.get("created_at", "")

        con.execute("""
            INSERT INTO events (event_id, ts_start, action, target,
                               app_tool, source_file, outcome_label)
            VALUES (?, ?, 'github_issue', ?, 'github', ?, ?)
        """, [event_id, ts, title[:500], f"{owner}/{repo}", state_val])
        count += 1

    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ME-OPS GitHub Ingestor")
    parser.add_argument("--repos", nargs="+", required=True,
                        help="GitHub repos as owner/repo")
    parser.add_argument("--since", type=str, default=None,
                        help="Only commits since (ISO date)")
    args = parser.parse_args()

    con = duckdb.connect(str(DB_PATH))

    print("ME-OPS GitHub Ingestor")
    print("=" * 60)

    total = 0
    for repo_full in args.repos:
        owner, repo = repo_full.split("/")
        print(f"\n  📦 {owner}/{repo}")

        n_commits = ingest_commits(con, owner, repo, args.since)
        n_prs = ingest_prs(con, owner, repo)
        n_issues = ingest_issues(con, owner, repo)

        subtotal = n_commits + n_prs + n_issues
        total += subtotal
        print(f"    Commits: {n_commits}, PRs: {n_prs}, Issues: {n_issues}")

    con.close()
    print(f"\n{'=' * 60}")
    print(f"✅ Ingested {total} new events from GitHub")


if __name__ == "__main__":
    main()
