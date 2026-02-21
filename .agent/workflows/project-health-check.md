---
description: Health check when returning to a project (locally or remotely). Confirms environment, git, IDE, deps, and runtime are healthy before any new work.
---

# Project Health Check

Run this workflow **every time you return to a project** (new session, new terminal, switching from another project, or reconnecting to a remote/WSL instance). Confirms everything is healthy before moving forward.

Skill used: verification-before-completion, git-advanced-workflows, python-pro, systematic-debugging.

## 1. Environment Detection

// turbo

```bash
echo "=== ENVIRONMENT ===" \
&& echo "OS: $(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')" \
&& echo "Kernel: $(uname -r)" \
&& echo "Host: $(hostname)" \
&& echo "User: $(whoami)" \
&& echo "Shell: $SHELL" \
&& echo "WSL: $(grep -qiE 'microsoft|wsl' /proc/version 2>/dev/null && echo 'YES' || echo 'NO')" \
&& echo "PWD: $(pwd)"
```

## 2. Nested Git Detection (CRITICAL)

// turbo

```bash
echo "=== NESTED GIT CHECK ===" \
&& REPO_ROOT="$(git rev-parse --show-toplevel)" \
&& echo "Repo root: $REPO_ROOT" \
&& echo "--- Parent .git ---" \
&& PARENT="$(dirname "$REPO_ROOT")" \
&& FOUND="" \
&& CHECK="$PARENT" \
&& while [ "$CHECK" != "/" ]; do \
     if [ -d "$CHECK/.git" ]; then FOUND="$CHECK"; break; fi; \
     CHECK="$(dirname "$CHECK")"; \
   done \
&& if [ -n "$FOUND" ]; then \
     echo "🚨 FAIL: Parent git repo at $FOUND/.git"; \
     echo "   FIX: rm -rf $FOUND/.git"; \
   else echo "✅ No parent .git"; fi \
&& echo "--- Child .git ---" \
&& CHILDREN="$(find "$REPO_ROOT" -mindepth 2 -maxdepth 4 -name .git -type d 2>/dev/null)" \
&& if [ -n "$CHILDREN" ]; then \
     echo "🚨 FAIL: Child .git found:"; echo "$CHILDREN"; \
   else echo "✅ No child .git"; fi
```

**If ANY nested .git is found, fix it BEFORE proceeding. Do not skip.**

## 3. Git Repository Health

// turbo

```bash
echo "=== GIT HEALTH ===" \
&& echo "--- Status ---" && git status --short --branch \
&& echo "--- Remote ---" && git remote -v \
&& echo "--- Branch ---" && git branch -a --sort=-committerdate \
&& echo "--- Last Commit ---" && git log -1 --stat \
&& echo "--- Stash ---" && git stash list \
&& echo "--- Untracked ---" && git ls-files --others --exclude-standard | head -10 \
&& echo "--- FSCK ---" && git fsck --no-dangling 2>&1 | tail -3 \
&& echo "--- Hooks ---" && ls -la $(git config --get core.hooksPath 2>/dev/null || echo ".git/hooks") 2>/dev/null | grep -v total
```

**Check for:**

- Uncommitted changes (stage/commit or stash before proceeding)
- Remote configured and reachable
- No corrupted objects (fsck clean)
- Hooks are executable and in place
- No stale stashes from previous sessions

## 3. Sync with Remote

```bash
git fetch --all --prune
git status --short --branch
```

**If behind remote:**

```bash
git pull --rebase
```

**If diverged:**

```bash
# Investigate before merging
git log --oneline HEAD..origin/main
git log --oneline origin/main..HEAD
```

## 4. Python Environment Health

// turbo

```bash
echo "=== PYTHON HEALTH ===" \
&& echo "--- Interpreter ---" && .venv/bin/python --version \
&& echo "--- Venv Active ---" && ls -la .venv/bin/python \
&& echo "--- Pip ---" && .venv/bin/pip --version \
&& echo "--- Installed Deps ---" && .venv/bin/pip list --format=columns 2>/dev/null | head -20 \
&& echo "--- Requirements Check ---" && .venv/bin/pip check 2>&1 | tail -5 \
&& echo "--- Import Test ---" && .venv/bin/python -c "
import sys
print(f'Python {sys.version}')
# Test critical imports
failures = []
for mod in ['duckdb', 'networkx']:
    try:
        __import__(mod)
        print(f'  ✅ {mod}')
    except ImportError:
        failures.append(mod)
        print(f'  ❌ {mod} MISSING')
if failures:
    print(f'FAIL: Missing {failures}')
    sys.exit(1)
print('All critical imports OK')
"
```

**If deps are missing:**

```bash
.venv/bin/pip install -r requirements.txt
```

## 5. IDE / Workspace Health

// turbo

```bash
echo "=== WORKSPACE HEALTH ===" \
&& echo "--- Workspace File ---" && find . -name "*.code-workspace" -not -path "./.venv/*" -exec echo "Found: {}" \; \
&& echo "--- Pyright Config ---" && (test -f pyrightconfig.json && echo "✅ pyrightconfig.json exists" || echo "❌ pyrightconfig.json MISSING") \
&& echo "--- Formatters ---" && (grep -q "editor.defaultFormatter" *.code-workspace 2>/dev/null && echo "✅ Formatters configured" || echo "⚠️ No formatter config found") \
&& echo "--- Extensions ---" && (grep -c "recommendations" *.code-workspace 2>/dev/null && echo "✅ Extension recommendations present" || echo "⚠️ No extension recommendations")
```

**Check for:**

- Workspace file exists inside project directory (not parent)
- `pyrightconfig.json` exists with correct venv path
- No duplicate workspace files
- Formatter configured for all languages (python, json, jsonc, markdown)
- Only valid/installed extensions in recommendations

## 6. Data & Output Health

// turbo

```bash
echo "=== DATA HEALTH ===" \
&& echo "--- Data Dir ---" && ls -lhS data/ 2>/dev/null | head -10 || echo "No data/ dir" \
&& echo "--- Output Dir ---" && ls -lhS output/ 2>/dev/null | head -10 || echo "No output/ dir" \
&& echo "--- DB Files ---" && find . -name "*.duckdb" -not -path "./.venv/*" -exec ls -lh {} \; 2>/dev/null || echo "No DuckDB files" \
&& echo "--- Disk ---" && df -h . | tail -1
```

## 7. Running Processes Check

// turbo

```bash
echo "=== RUNNING PROCESSES ===" \
&& ps aux | grep -E "(python|node|npm)" | grep -v grep | head -10 || echo "No relevant processes"
```

**Check for:**

- Orphaned processes from previous sessions
- Port conflicts
- Background jobs that should be restarted

## 8. GitHub Remote Health

// turbo

```bash
echo "=== GITHUB HEALTH ===" \
&& (gh auth status 2>&1 || echo "gh CLI not authenticated") \
&& echo "--- Repo ---" && (gh repo view --json name,visibility,defaultBranchRef 2>&1 || echo "Cannot reach GitHub repo")
```

## 9. Security Scan

// turbo

```bash
echo "=== SECURITY SCAN ===" \
&& echo "--- Secrets in tracked files ---" && git grep -nEi "(api_key|secret_key|password|token)\s*=\s*['\"][^'\"]{8,}['\"]" -- '*.py' '*.json' '*.yaml' '*.yml' '*.env' 2>/dev/null | head -5 || echo "✅ No hardcoded secrets found" \
&& echo "--- .env in git ---" && (git ls-files .env .env.* 2>/dev/null | head -3 || echo "✅ No .env files tracked") \
&& echo "--- Debug artifacts ---" && git grep -nE "(breakpoint\(\)|import pdb|pdb\.set_trace)" -- '*.py' 2>/dev/null | head -5 || echo "✅ No debug artifacts"
```

## 10. Final Verdict

After all checks pass, output:

```
✅ PROJECT HEALTHY — Ready to proceed.
   Branch: main (clean)
   Remote: origin → https://github.com/...
   Python: 3.12.x (.venv active)
   Deps: All installed, no conflicts
   Hooks: commit-msg + pre-commit active
   Workspace: Configured, no errors
```

If ANY check fails, fix it before starting new work. Do not proceed with a degraded environment.
