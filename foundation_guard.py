import sys
import subprocess
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

class FoundationIssue(BaseModel):
    category: str
    severity: str
    description: str
    fix_instruction: str
    skill_link: Optional[str] = None

class FoundationGuard:
    """Service dedicated to Day 0 stability and elite level-10 foundation enforcement."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.blueprint_path = project_root / "environment_blueprint.json"
        self.blueprint = self._load_blueprint()

    def _load_blueprint(self) -> Dict[str, Any]:
        if self.blueprint_path.exists():
            with open(self.blueprint_path, "r") as f:
                return json.load(f)
        return {}

    def check_venv(self) -> Optional[FoundationIssue]:
        venv_path = self.project_root / ".venv"
        if not venv_path.exists():
            return FoundationIssue(
                category="Infrastructure",
                severity="critical",
                description="Python Virtual Environment (.venv) is missing.",
                fix_instruction="Run 'python3 -m venv .venv' and install requirements.",
                skill_link="pro-project-bootstrap"
            )
        return None

    def check_env_symlink(self) -> Optional[FoundationIssue]:
        env_path = self.project_root / ".env"
        if not env_path.exists():
            return FoundationIssue(
                category="Security",
                severity="high",
                description=".env file or symlink is missing.",
                fix_instruction="Create a .env file or symlink it from your global secrets store.",
                skill_link="environment-hardening"
            )
        return None

    def check_mandatory_structure(self) -> List[FoundationIssue]:
        issues = []
        for folder in self.blueprint.get("mandatory_structure", []):
            path = self.project_root / folder
            if not path.exists():
                issues.append(FoundationIssue(
                    category="Structure",
                    severity="medium",
                    description=f"Mandatory directory '{folder}' is missing.",
                    fix_instruction=f"Run 'mkdir -p {folder}'",
                    skill_link="software-architecture"
                )
            )
        return issues

    def check_tooling_parity(self) -> List[FoundationIssue]:
        issues = []
        required_tools = self.blueprint.get("required_tools", {})
        
        # Check Python Version
        if "python" in required_tools:
            major, minor = sys.version_info[:2]
            version = f"{major}.{minor}"
            if not version.startswith("3.12") and not version.startswith("3.13"):
                issues.append(FoundationIssue(
                    category="Parity",
                    severity="high",
                    description=f"Python version {version} is not Level-10 compliant (requires >=3.12).",
                    fix_instruction="Switch to Python 3.12 using pyenv or your package manager.",
                    skill_link="python-pro"
                ))
        
        # Check for Ruff/Mypy existence
        for tool in ["ruff", "mypy"]:
            try:
                subprocess.run([tool, "--version"], capture_output=True, check=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                issues.append(FoundationIssue(
                    category="Parity",
                    severity="high",
                    description=f"Elite tool '{tool}' is not installed or available in PATH.",
                    fix_instruction=f"Install {tool} via pip: 'pip install {tool}'",
                    skill_link="python-type-safety"
                ))
                
        return issues

    def audit_all(self) -> List[FoundationIssue]:
        issues = []
        # Basic Checks
        for check in [self.check_venv, self.check_env_symlink]:
            issue = check()
            if issue:
                issues.append(issue)
        
        # Structural Checks
        issues.extend(self.check_mandatory_structure())
        
        # Tooling Parity
        issues.extend(self.check_tooling_parity())
        
        return issues

def run(project_root: Path) -> bool:
    """Run all foundational audits."""
    print("ME-OPS Foundation Guard")
    print("=" * 60)
    guard = FoundationGuard(project_root)
    issues = guard.audit_all()
    for issue in issues:
        print(f"[{issue.severity.upper()}] {issue.description}")
    
    if not issues:
        print("✅ Foundation is stable.")
    else:
        print(f"⚠️ Foundation issues detected: {len(issues)} finding(s).")
    print("=" * 60)
    return len(issues) == 0


if __name__ == "__main__":
    if not run(Path(".").resolve()):
        sys.exit(1)
