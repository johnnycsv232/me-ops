"""ME-OPS v2 — Base canonical model. Every object inherits from this."""
from __future__ import annotations
from enum import Enum
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
import uuid


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


class ObjectType(str, Enum):
    EVENT = "event"
    ARTIFACT = "artifact"
    DECISION = "decision"
    FAILURE = "failure"
    HYPOTHESIS = "hypothesis"
    RELATIONSHIP = "relationship"
    SYSTEM_STATE = "system_state"
    OUTCOME = "outcome"
    CASE = "case"
    HEURISTIC = "heuristic"
    BRIEFING = "briefing"
    INTERVENTION = "intervention"


class SourceKind(str, Enum):
    PIECES = "pieces"
    MANUAL = "manual"
    DERIVED = "derived"
    IMPORTED = "imported"


class ActorKind(str, Enum):
    USER = "user"
    SYSTEM = "system"
    AGENT = "agent"
    EXTERNAL = "external"


class BaseRecord(BaseModel):
    id: str
    type: ObjectType
    source: SourceKind = SourceKind.PIECES
    source_refs: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    timestamp_start: Optional[datetime] = None
    timestamp_end: Optional[datetime] = None
    project_id: Optional[str] = None
    session_id: Optional[str] = None
    actor: ActorKind = ActorKind.USER
    confidence: float = 0.0
    evidence_refs: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    summary: str = ""

    class Config:
        use_enum_values = True
