# ME-OPS v2 — Verified Bootstrap (Windows / PowerShell)
# Run this on a clean Windows machine to reproduce the environment.
# Evidence: verified 2026-03-21 on GettUppENT / Windows 11 / Python 3.14.2

# STEP 0: Prerequisites (must do manually)
# 1. Install Python 3.12+ from https://python.org
# 2. Install PiecesOS from https://pieces.app and enable LTM-2.7 engine
# 3. Get a Gemini API key from https://aistudio.google.com/apikey

# STEP 1: Verify Python
Write-Host "=== Step 1: Python ===" -ForegroundColor Cyan
python --version

# STEP 2: Install required dependencies
Write-Host "`n=== Step 2: Dependencies ===" -ForegroundColor Cyan
python -m pip install -r requirements.txt

# STEP 3: Create .env file if missing
Write-Host "`n=== Step 3: Environment ===" -ForegroundColor Cyan
if (-not (Test-Path ".env")) {
    $apiKey = Read-Host "Enter your Gemini API key"
    @"
GEMINI_API_KEY=$apiKey
PYTHONIOENCODING=utf-8
# MEOPS_DB=C:\path\to\custom\meops.db
"@ | Out-File -FilePath ".env" -Encoding utf8
    Write-Host "  .env created"
    $env:GEMINI_API_KEY = $apiKey
} else {
    Write-Host "  Reusing existing .env"
}

# STEP 4: Set env vars for this session
$env:PYTHONIOENCODING = "utf-8"

# STEP 5: Verify PiecesOS
Write-Host "`n=== Step 5: PiecesOS health ===" -ForegroundColor Cyan
try {
    $r = Invoke-RestMethod -Uri "http://localhost:39300/.well-known/version" -TimeoutSec 3
    Write-Host "  PiecesOS OK: $r" -ForegroundColor Green
} catch {
    Write-Host "  WARN: PiecesOS not detected on port 39300. Ingestion will fail." -ForegroundColor Yellow
}

# STEP 6: Initialize DB and run full pipeline
Write-Host "`n=== Step 6: Full pipeline ===" -ForegroundColor Cyan
Write-Host "  NOTE: Gemini embedding takes ~11 min at free tier (65s per batch of 50)"
python run_phase2.py full

# STEP 7: Health checks
Write-Host "`n=== Step 7: Health checks ===" -ForegroundColor Cyan
python run_phase2.py stats
python run_phase2.py recall "IronClad revenue blocked"

# STEP 8: Verify expected output
Write-Host "`n=== Expected results ===" -ForegroundColor Cyan
Write-Host "  events=526, embeddings=579, sessions=207, cases=18, heuristics=10"
Write-Host "  Recall: top result similarity > 0.70"
Write-Host "`n  If counts differ: PiecesOS may have more/less history than GettUppENT machine"
