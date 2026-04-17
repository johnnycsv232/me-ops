"""Case and Heuristic models — the intelligence layer."""
from __future__ import annotations
from enum import Enum
from typing import List, Optional
from .base import BaseRecord, ObjectType


class CaseKind(str, Enum):
    FAILURE_CHAIN = "failure_chain"
    WIN_SIGNATURE = "win_signature"
    DECISION_REPLAY = "decision_replay"
    PROJECT_MILESTONE = "project_milestone"
    PLANNING_SPIRAL = "planning_spiral"
    ENV_DRIFT_CLUSTER = "env_drift_cluster"
    CONTEXT_COLLAPSE = "context_collapse"


class Case(BaseRecord):
    type: str = ObjectType.CASE
    case_kind: CaseKind = CaseKind.FAILURE_CHAIN
    title: str = ""
    symptom: Optional[str] = None
    trigger: Optional[str] = None

    # Failure chain specific
    diagnosis_path: List[str] = []
    false_paths: List[str] = []
    final_fix: Optional[str] = None
    time_lost_minutes: int = 0
    reusable_fix: List[str] = []

    # Win signature specific
    preconditions: List[str] = []
    sequence: List[str] = []
    tools_used: List[str] = []
    context_switch_count: int = 0
    resolution_speed_minutes: int = 0
    output_quality_score: float = 0.0
    future_pattern: Optional[str] = None

    # Common
    member_ids: List[str] = []   # event/failure/outcome IDs in this case
    recurrence_count: int = 1


class HeuristicKind(str, Enum):
    SUCCESS_RULE = "success_rule"
    FAILURE_RULE = "failure_rule"
    WARNING = "warning"
    DECISION_RULE = "decision_rule"
    WORKFLOW_RULE = "workflow_rule"
    ANTI_PATTERN = "anti_pattern"


class Heuristic(BaseRecord):
    type: str = ObjectType.HEURISTIC
    heuristic_kind: HeuristicKind = HeuristicKind.FAILURE_RULE
    statement: str = ""
    scope: str = ""
    applies_when: List[str] = []
    derived_from_cases: List[str] = []
    support_count: int = 0
    contradiction_count: int = 0
    utility_score: float = 0.0
    active: bool = True

    @property
    def net_support(self) -> int:
        return self.support_count - self.contradiction_count

    @property
    def reliability(self) -> float:
        total = self.support_count + self.contradiction_count
        if total == 0:
            return 0.0
        return round(self.support_count / total, 3)


class Briefing(BaseRecord):
    type: str = ObjectType.BRIEFING
    primary_focus: str = ""
    active_risk: str = ""
    pattern_match: Optional[str] = None
    known_dead_end: Optional[str] = None
    best_next_move: str = ""
    if_stuck_fallback: List[str] = []
    watch_metric: Optional[str] = None
    context_collapse_score: float = 0.0
    matched_case_ids: List[str] = []
    active_heuristic_ids: List[str] = []


class Intervention(BaseRecord):
    type: str = ObjectType.INTERVENTION
    trigger_pattern: str = ""
    message: str = ""
    suggested_action: str = ""
    guardrail_type: str = ""   # dead_end | context_collapse | drift | repeat
    was_followed: Optional[bool] = None
    outcome_ref: Optional[str] = None
