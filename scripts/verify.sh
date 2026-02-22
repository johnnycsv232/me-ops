#!/usr/bin/env bash
set -e

echo "======================================"
echo "    ME-OPS VERIFICATION MATRIX        "
echo "======================================"

if [ ! -d ".venv" ]; then
    echo "❌ Error: .venv not found. Please follow docs/DEV_SETUP.md"
    exit 1
fi

echo "[1/3] Activating environment..."
source .venv/bin/activate

echo "[2/3] Running Pyright Type Checker..."
if pyright; then
    echo "✅ Type check passed."
else
    echo "❌ Type check failed!"
    # Do not exit immediately so we can see if tests also fail, or exit here?
    # Usually we exit on first failure to enforce fixing sequentially.
    exit 1
fi

echo "[3/3] Running Unit / Integration Tests..."
if python -m pytest ./tests -v; then
    echo "✅ Tests passed."
else
    echo "❌ Tests failed!"
    exit 1
fi

echo "======================================"
echo "✅ ALL CHECKS PASSED. SYSTEM GREEN. "
echo "======================================"
