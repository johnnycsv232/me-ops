"""Project-level configuration helpers."""
from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - fallback for partial installs
    def load_dotenv(*_args, **_kwargs):
        return False


PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_LOADED = False


def load_project_env() -> Path:
    """Load the repository .env once so modules can read env-backed config."""
    global _ENV_LOADED
    if not _ENV_LOADED:
        env_path = PROJECT_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        _ENV_LOADED = True
    return PROJECT_ROOT


def get_env(name: str, default: str | None = None) -> str | None:
    load_project_env()
    return os.environ.get(name, default)


def require_env(name: str) -> str:
    value = get_env(name)
    if value:
        return value
    raise RuntimeError(
        f"{name} is not set. Export it or create {PROJECT_ROOT / '.env'} from .env.example."
    )
