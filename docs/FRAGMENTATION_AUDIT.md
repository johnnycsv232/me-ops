# Fragmentation Audit

## Environment Conflicts
- **Package Managers**: Only 1 mechanism detected (`requirements.txt`). No conflicts or competing lockfiles.
- **Python / Node Versions**: Bound to Python 3.12 (as per `pyrightconfig.json` and `.venv`).
- **IDE Configurations**: Strongly tied to VSCode (`me_ops.code-workspace`). No `.idea` or competing configs found. Settings are well-defined for formatting (`ms-python.python`), testing (`unittest`), and environment paths.

## Hotspot / Edge Case Sweep
- **`TODO / FIXME / HACK`**: Zero instances found. Clean codebase.
- **`os.environ` Usage**: Detected in `briefing.py`, `architect.py`, `agent.py`, `github.py`, `deep_analysis.py`.
  - **Risk**: Environment variables (e.g., `GEMINI_API_KEY`, `GITHUB_TOKEN`) are accessed locally. Startup flow could crash if `.env` fails to mount these values.
  - **Mitigation**: Ensure `python-dotenv` is loaded at the root of `master.py` or `ingest.py` before these modules instantiate.
- **`# type: ignore`**: Found heavily in `vectors.py`, `insights.py`, `ingest.py`, `agent.py`, `master.py`.
  - **Risk**: Intentional bypass of Pyright for un-typed third-party payloads (like Gemini SDK parts) and dynamic dictionary access.
  - **Mitigation**: Defer full fixing as they are controlled ignores. Will be noted as minor technical debt.

## Risk Register
1. **Unchecked API Keys**: `os.environ.get()` returns `None` if absent, potentially causing downstream crashes in LLM calls if not immediately validated.
2. **Type Overrides**: A subset of files disables Pyright checks for specific lines. This can mask structural changes in external APIs (e.g., `google-genai`).
