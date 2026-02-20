---
description: Full project stabilization from scratch — environment, git, IDE, deps, hooks, and verification. Run at the very start of any new project.
---

# Project Inception Stabilization

Run this workflow at the **very start** of a new project to establish a bulletproof foundation.
Skill used: git-advanced-workflows, git-pushing, python-pro, verification-before-completion.

## 1. Create Project Directory Structure

```bash
mkdir -p $PROJECT_NAME/{data,output,scripts,tests}
touch $PROJECT_NAME/__init__.py $PROJECT_NAME/data/.gitkeep $PROJECT_NAME/output/.gitkeep
```

## 2. Initialize Python Environment

```bash
cd $PROJECT_NAME
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
```

## 3. Create requirements.txt

List all dependencies. Pin major versions. Example:

```
duckdb>=1.0
networkx>=3.0
```

// turbo
```bash
pip install -r requirements.txt
```

## 4. Initialize Git Repository

```bash
git init
git config --local user.name "Johnny Cage"
git config --local user.email "info.gettupp@gmail.com"
git config --local init.defaultBranch main
```

## 5. Create .gitignore

Generate a comprehensive .gitignore covering:
- Python: `__pycache__/`, `.venv/`, `*.pyc`, `*.egg-info/`, `dist/`, `build/`
- Data: large data files, DB files (`*.duckdb`, `*.duckdb.wal`)
- IDE: `.vscode/`, `.idea/`
- OS: `.DS_Store`, `Thumbs.db`
- Secrets: `.env`, `.env.*`
- Logs: `*.log`, `*.tmp`

## 6. Apply Elite Git Configuration

// turbo
```bash
# Core behavior
git config --local core.autocrlf input
git config --local core.eol lf
git config --local core.whitespace trailing-space,space-before-tab

# Pull/Push strategy
git config --local pull.rebase true
git config --local push.default current
git config --local push.autoSetupRemote true

# Fetch hygiene
git config --local fetch.prune true
git config --local fetch.pruneTags true

# Rebase power
git config --local rebase.autoSquash true
git config --local rebase.autoStash true

# Merge safety
git config --local merge.ff only

# Diff quality
git config --local diff.algorithm histogram
git config --local diff.colorMoved default

# Commit discipline
git config --local commit.verbose true
git config --local commit.template .gitmessage

# Display
git config --local status.showUntrackedFiles all
git config --local log.abbrevCommit true
git config --local log.date iso-local
git config --local column.ui auto
git config --local branch.sort -committerdate
git config --local tag.sort -version:refname

# Conflict memory
git config --local rerere.enabled true

# Hooks
git config --local core.hooksPath .githooks
```

## 7. Create Git Hooks

### 7a. Conventional Commit Hook (`.githooks/commit-msg`)

Create executable hook that enforces: `<type>(<scope>): <subject>`
Types: feat, fix, refactor, docs, test, chore, ci, perf, style, build, revert
Max subject: 72 chars. Reject non-conforming messages.

### 7b. Pre-Commit Quality Gate (`.githooks/pre-commit`)

Create executable hook that checks staged Python files for:
- Debug artifacts (`breakpoint()`, `import pdb`, `pdb.set_trace`)
- Hardcoded secrets (`api_key = "..."`, `password = "..."`)
- Syntax errors (`py_compile`)

// turbo
```bash
chmod +x .githooks/commit-msg .githooks/pre-commit
```

## 8. Create Commit Template (`.gitmessage`)

Template with project-specific scopes and conventional commit format guide.

## 9. Add Git Aliases

// turbo
```bash
git config --local alias.st "status --short --branch"
git config --local alias.lg "log --graph --oneline --decorate --all"
git config --local alias.last "log -1 --stat"
git config --local alias.unstage "restore --staged"
git config --local alias.amend "commit --amend --no-edit"
git config --local alias.undo "reset --soft HEAD~1"
git config --local alias.pushf "push --force-with-lease"
git config --local alias.fresh "!git fetch --all --prune && git rebase origin/main"
git config --local alias.changed "diff --name-only"
git config --local alias.staged "diff --cached --name-only"
git config --local alias.branches "branch -a --sort=-committerdate"
git config --local alias.tags "tag -l --sort=-version:refname"
git config --local alias.cleanup "!git branch --merged main | grep -v main | xargs -r git branch -d"
```

## 10. Create IDE Workspace File

Create `<project>.code-workspace` **inside** the project directory with:
- Folder entries with named paths
- Python interpreter path pointing to `.venv/bin/python`
- `python.analysis.extraPaths` to `.venv/lib/python3.XX/site-packages`
- `python.venvPath` and `python.venvFolders`
- `pyrefly.searchRoot`
- Language-specific formatters: `[python]`, `[json]`, `[jsonc]`, `[markdown]`
- `files.exclude` and `search.exclude` for noise
- `editor.formatOnSave: true`
- Extension recommendations (only verified-installed extensions)
- Debug/launch configurations

## 11. Create Type Checker Config

Create `pyrightconfig.json` in project root:

```json
{
  "pythonVersion": "3.12",
  "pythonPlatform": "Linux",
  "venvPath": ".",
  "venv": ".venv",
  "include": ["."],
  "exclude": [".venv", "__pycache__", "data", "output"],
  "typeCheckingMode": "basic"
}
```

## 12. Create GitHub Remote

```bash
gh repo create <repo-name> --private --description "<description>" --source=. --remote=origin --push
```

If `gh` is not installed:
```bash
# Install gh CLI
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
sudo apt-get update -qq && sudo apt-get install -y -qq gh
```

If `gh` needs auth, use token: `echo "$GITHUB_TOKEN" | gh auth login --with-token`

## 13. Initial Commit & Push

```bash
git add -A
git commit -m "feat: initial project scaffold"
git push -u origin main
```

## 14. Verification Checklist

// turbo
```bash
echo "=== VERIFICATION ===" \
&& git status --short --branch \
&& echo "--- Remote ---" && git remote -v \
&& echo "--- Hooks ---" && ls -la .githooks/ \
&& echo "--- Config ---" && git config --local --list | grep -E "^(pull|push|rebase|merge|diff|core.hooks)" \
&& echo "--- Python ---" && .venv/bin/python --version \
&& echo "--- Deps ---" && .venv/bin/pip list --format=columns | head -20 \
&& echo "--- Workspace ---" && find . -name "*.code-workspace" -not -path "./.venv/*" \
&& echo "--- Pyright ---" && cat pyrightconfig.json 2>/dev/null | head -5 \
&& echo "--- Git Log ---" && git log --oneline \
&& echo "--- FSCK ---" && git fsck --no-dangling 2>&1 | tail -3
```

All items must pass with no errors. If any fail, fix before proceeding.
