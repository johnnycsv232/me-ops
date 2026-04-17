"""Decision, Failure, Outcome models."""
from __future__ import annotations
from enum import Enum
from typing import List, Optional
from .base import BaseRecord, ObjectType


class DecisionKind(str, Enum):
    TOOL_CHOICE = "tool_choice"
    ARCHITECTURE = "architecture"
    DEBUG_STRATEGY = "debug_strategy"
    PRIORITY = "priority"
    SCOPE = "scope"
    RELATIONSHIP = "relationship"
    WORKFLOW = "workflow"
    PRICING = "pricing"
    PIVOT = "pivot"


class VerdictKind(str, Enum):
    GOOD = "good"
    BAD = "bad"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class Decision(BaseRecord):
    type: str = ObjectType.DECISION
    decision_kind: DecisionKind = DecisionKind.DEBUG_STRATEGY
    context: str = ""
    trigger_refs: List[str] = []
    options_considered: List[str] = []
    choice_made: str = ""
    rationale: str = ""
    expected_outcome: str = ""
    actual_outcome_ref: Optional[str] = None
    later_verdict: VerdictKind = VerdictKind.UNKNOWN
    decision_quality_score: float = 0.0
    # scoring components
    evidence_quality: float = 0.0
    option_coverage: float = 0.0
    outcome_quality: float = 0.0
    speed_score: float = 0.0
    downstream_value: float = 0.0
    reversibility_score: float = 0.0

    def compute_quality_score(self) -> float:
        score = (
            0.20 * self.evidence_quality +
            0.15 * self.option_coverage +
            0.25 * self.outcome_quality +
            0.10 * self.speed_score +
            0.15 * self.downstream_value +
            0.15 * self.reversibility_score
        )
        self.decision_quality_score = round(score, 3)
        return self.decision_quality_score


class FailureKind(str, Enum):
    CONFIG = "config"
    AUTH = "auth"
    NETWORK = "network"
    ENV = "env"
    DEPENDENCY = "dependency"
    CONTEXT = "context"
    DECISION = "decision"
    PEOPLE = "people"
    WORKFLOW = "workflow"
    UNKNOWN = "unknown"


class SeverityKind(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Failure(BaseRecord):
    type: str = ObjectType.FAILURE
    failure_kind: FailureKind = FailureKind.UNKNOWN
    symptom: str = ""
    severity: SeverityKind = SeverityKind.MEDIUM
    trigger_refs: List[str] = []
    candidate_causes: List[str] = []
    actual_cause: Optional[str] = None
    time_lost_minutes: int = 0
    resolution_ref: Optional[str] = None
    future_rule: Optional[str] = None
    recurrence_count: int = 1


class OutcomeKind(str, Enum):
    FIX = "fix"
    SHIP = "ship"
    DELAY = "delay"
    ABANDON = "abandon"
    INSIGHT = "insight"
    REVENUE = "revenue"
    CONFLICT = "conflict"
    LEARNING = "learning"


class OutcomeStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILURE = "failure"


class Outcome(BaseRecord):
    type: str = ObjectType.OUTCOME
    outcome_kind: OutcomeKind = OutcomeKind.FIX
    status: OutcomeStatus = OutcomeStatus.SUCCESS
    linked_decision_refs: List[str] = []
    linked_failure_refs: List[str] = []
    time_to_resolution_minutes: int = 0
    quality_score: float = 0.0
    downstream_value_score: float = 0.0
    verdict: str = ""
