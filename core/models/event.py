"""Event, Artifact, SystemState models."""
from __future__ import annotations
from enum import Enum
from typing import Dict, List, Optional, Any
from .base import BaseRecord, ObjectType, new_id


class EventKind(str, Enum):
    COMMAND = "command"
    EDIT = "edit"
    MESSAGE = "message"
    OPEN = "open"
    SEARCH = "search"
    RUN = "run"
    ERROR = "error"
    CONTEXT_SWITCH = "context_switch"
    DEPLOY = "deploy"
    COMMIT = "commit"
    DEBUG_STEP = "debug_step"
    CLIPBOARD = "clipboard"
    AUDIO = "audio"
    VISION = "vision"
    SUMMARY = "summary"   # Pieces workstream summary
    UNKNOWN = "unknown"


class LocationKind(str, Enum):
    LOCAL = "local"
    WSL = "wsl"
    REPO = "repo"
    BROWSER = "browser"
    TERMINAL = "terminal"
    UNKNOWN = "unknown"


class Event(BaseRecord):
    type: str = ObjectType.EVENT
    event_kind: EventKind = EventKind.UNKNOWN
    tool: Optional[str] = None
    location: LocationKind = LocationKind.UNKNOWN
    input_refs: List[str] = []
    output_refs: List[str] = []
    state_before_ref: Optional[str] = None
    state_after_ref: Optional[str] = None
    raw_content: Optional[str] = None       # original text, transcript, clipboard
    app_name: Optional[str] = None
    window_title: Optional[str] = None
    url: Optional[str] = None
    pieces_event_id: Optional[str] = None   # original Pieces ID

    @classmethod
    def from_pieces_summary(cls, pieces_id: str, created_at, project_id: str,
                             summary_text: str, name: str = "") -> "Event":
        return cls(
            id=new_id("evt"),
            type=ObjectType.EVENT,
            event_kind=EventKind.SUMMARY,
            pieces_event_id=pieces_id,
            timestamp_start=created_at,
            project_id=project_id,
            raw_content=summary_text,
            summary=name or summary_text[:120],
        )


class ArtifactKind(str, Enum):
    FILE = "file"
    PROMPT = "prompt"
    CONFIG = "config"
    COMMIT = "commit"
    NOTE = "note"
    OUTPUT = "output"
    SCREENSHOT = "screenshot"
    TRANSCRIPT = "transcript"
    DOC = "doc"
    CODE = "code"
    UNKNOWN = "unknown"


class Artifact(BaseRecord):
    type: str = ObjectType.ARTIFACT
    artifact_kind: ArtifactKind = ArtifactKind.UNKNOWN
    name: Optional[str] = None
    path: Optional[str] = None
    content_hash: Optional[str] = None
    version: Optional[str] = None
    language: Optional[str] = None
    created_from_event: Optional[str] = None
    raw_content: Optional[str] = None


class StateKind(str, Enum):
    ENV = "env"
    REPO = "repo"
    MACHINE = "machine"
    WORKFLOW = "workflow"
    RELATIONSHIP = "relationship"
    COGNITIVE = "cognitive"


class SystemState(BaseRecord):
    type: str = ObjectType.SYSTEM_STATE
    state_kind: StateKind = StateKind.ENV
    machine: Optional[str] = None
    os: Optional[str] = None
    network_mode: Optional[str] = None
    shell: Optional[str] = None
    repo_branch: Optional[str] = None
    service_status: Dict[str, str] = {}
    context_load_score: float = 0.0
    fragmentation_score: float = 0.0
    tool_switch_count: int = 0
    concurrent_projects: int = 0
