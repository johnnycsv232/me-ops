#!/bin/bash
set -euo pipefail

echo "Setting up ME-OPS environment..."

# Create virtual environment
python3 -m venv .venv
echo "Virtual environment created at .venv/"

# Activate and install dependencies
source .venv/bin/activate
pip install -r requirements.txt

echo ""
echo "Setup complete."
echo "  Activate:  source .venv/bin/activate"
echo "  Run:       python run.py"
