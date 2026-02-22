# ME-OPS Developer Setup

## 1. Prerequisites
- Python 3.12+
- Linux/macOS or WSL2

## 2. Environment Setup
Create and activate the virtual environment, then install all required packages:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Configuration
Create the local environment file. This requires standard expected variables such as `GEMINI_API_KEY`:
```bash
touch .env
# Add API keys to .env
```

## 4. Verification Matrix
Run the single-source-of-truth verification script to ensure formatting, typing, and tests pass:
```bash
./scripts/verify.sh
```

**Rule**: All PRs and commits MUST pass `./scripts/verify.sh` before merge.
