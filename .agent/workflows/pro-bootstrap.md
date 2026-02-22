---
description: Bootstrap a new project with Pro-Level standards (IDE-Agnostic). Establishes structured directories, isolated venv, and toxic-path Git guardrails.
---

# Pro Project Bootstrap

Use this workflow to start any new project with the Pro-Level standard.

## 1. Project Inception

// turbo

```bash
read -p "Enter project name: " PROJECT_NAME
mkdir -p $PROJECT_NAME/{data,output,scripts,tests}
cd $PROJECT_NAME
```

## 2. Environment Hardening

// turbo

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install duckdb networkx
```

## 3. Git Shield Activation

// turbo

```bash
git init
cat > .git/hooks/pre-commit <<EOF
#!/bin/bash
TOXIC=\$(git diff --cached --name-only | xargs grep -lEi "C:\\\\\\\\|D:\\\\\\\\" 2>/dev/null)
if [ -n "\$TOXIC" ]; then
    echo "!!! TOXIC WINDOWS PATH DETECTED !!!"
    exit 1
fi
EOF
chmod +x .git/hooks/pre-commit
```

## 4. Verification

// turbo

```bash
git add .
git commit -m "chore: pro-level bootstrap"
echo "✅ Project Bootstrap Complete."
```
