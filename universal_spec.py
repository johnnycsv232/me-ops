import json
from pathlib import Path
from typing import List
from pydantic import BaseModel, Field
from datetime import datetime

class UniversalSpec(BaseModel):
    title: str
    description: str
    required_skills: List[str]
    goals: List[str]
    acceptance_criteria: List[str]
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class SpecEngine:
    """Universal Spec Engine for bridging Global Operator Ideas to Elite Code."""
    
    def __init__(self, registry_path: Path):
        self.registry_path = registry_path
        self.registry = {}
        if self.registry_path.exists():
            with open(self.registry_path, "r") as f:
                self.registry = json.load(f)

    def generate_spec_from_idea(self, idea_text: str) -> UniversalSpec:
        # Heuristic mapping
        relevant_skills = [s for s in self.registry.keys() if any(word in s.lower() for word in idea_text.lower().split())]
        
        return UniversalSpec(
            title=f"Elite Implementation: {idea_text[:30]}...",
            description=f"Automated translation of Operator idea: '{idea_text}'",
            required_skills=relevant_skills[:2], # Top 2 matches
            goals=["Establish elite foundation", "Enforce universal-skill standards", "Ensure global stability"],
            acceptance_criteria=[
                "Zero drift detected by FoundationGuard",
                "100% Type safety (mypy passing)",
                "Documented in UNIVERSAL_HEALTH_PROOF.md"
            ]
        )

    def save_spec(self, spec: UniversalSpec, output_path: Path):
        with open(output_path, "w") as f:
            f.write(f"# {spec.title}\n\n")
            f.write("**Status**: Generated from Operator Intent\n\n")
            f.write(f"## Description\n{spec.description}\n\n")
            f.write("## Realized via Professional Skills\n")
            for skill in spec.required_skills:
                f.write(f"- {skill}\n")
            f.write("\n## Goals\n")
            for goal in spec.goals:
                f.write(f"- {goal}\n")
            f.write("\n## Acceptance Criteria\n")
            for criteria in spec.acceptance_criteria:
                f.write(f"- [ ] {criteria}\n")
