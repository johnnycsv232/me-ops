# Requirements (SSOT)

## Objective
Standardize the ME-OPS project environment by removing fragmentation, enforcing strict validation checks, and delivering a zero-error development state. Provide a one-step verification script.

## Scope In
- Consolidating IDE formats into `.editorconfig`.
- Creating cross-environment standard `scripts/verify.sh`.
- Writing `docs/DEV_SETUP.md`.
- Executing the Verification Matrix (Lint, Typecheck, Test).
- Fixing Pyright type bypasses where trivial, otherwise documenting constraints.

## Scope Out
- Refactoring the core AI or DB logic unless it fails tests.
- Transitioning to Docker (explicitly denied in DECISIONS).

## Acceptance Criteria
1. `bash ./scripts/verify.sh` executes fully and exits with code `0`.
2. Pyright reports 0 errors or warnings (ignoring explicit `# type: ignore` lines).
3. All unit tests in `./tests/` pass successfully.

## Non-Functional Requirements (NFRs)
- **Security**: No secrets hardcoded. API keys loaded conditionally via `.env`.
- **Reliability**: Script idempotency (re-running `verify.sh` produces identical results without side effects).
- **Observability**: `verify.sh` outputs clear, human-readable pass/fail blocks for each matrix step.

## Definition of Done
- All files formatted correctly.
- Strict type checking passed.
- All acceptance criteria verified by command outputs.
- `docs/DELIVERY_REPORT.md` provided with proof logs.
