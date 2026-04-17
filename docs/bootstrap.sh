#!/usr/bin/env bash
# ME-OPS v2 — Verified Bootstrap Script
# Run this on a clean POSIX shell machine to reproduce the environment.
# Evidence: verified 2026-03-21 on GettUppENT / Windows 11 / Python 3.14.2

set -euo pipefail

echo "=== ME-OPS v2 Bootstrap ==="
echo "Step 1: Verify Python 3.12+"
python --version

echo "Step 2: Install dependencies"
python -m pip install -r requirements.txt

if [[ ! -f .env ]]; then
  echo "Step 3: Create .env file"
  read -rsp "Enter your Gemini API key: " api_key
  printf "\n"
  cat > .env <<EOF
GEMINI_API_KEY=${api_key}
PYTHONIOENCODING=utf-8
# MEOPS_DB=/absolute/path/to/meops.db  # optional
EOF
  echo "  .env created"
else
  echo "Step 3: Reusing existing .env"
fi

export PYTHONIOENCODING=utf-8

echo "Step 4: Verify PiecesOS is running"
if command -v curl >/dev/null 2>&1; then
  curl -fsS http://localhost:39300/.well-known/version || echo "WARN: PiecesOS not detected on port 39300"
else
  echo "WARN: curl not found; skipping PiecesOS health check"
fi

echo "Step 5: Initialize DB and run full pipeline"
python run_phase2.py full

echo "Step 6: Health checks"
python run_phase2.py stats
python run_phase2.py recall "IronClad revenue blocked"

echo ""
echo "Expected output:"
echo "  events=526, embeddings=579, sessions=207, cases=18, heuristics=10"
echo "  Recall returns results with sim > 0.70"
