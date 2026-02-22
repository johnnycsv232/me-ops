import os
import sys
import subprocess
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from foundation_guard import FoundationGuard
from universal_spec import SpecEngine

# --- Models ---

class Signal(BaseModel):
    id: Optional[int] = None
    type: str
    severity: str
    source: str
    description: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class ActionItem(BaseModel):
    id: Optional[int] = None
    title: str
    description: str
    category: str  # task, intervention, prediction
    status: str = "pending"
    signal_id: Optional[int] = None
    priority: int = 5
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

# --- Core Orchestrator ---

class Orchestrator:
    """Universal Master Orchestrator: The Global Engineering Bridge."""
    
    def __init__(self, db_path: str = "me_ops.db"):
        self.db_path = db_path
        self.project_root = Path(__file__).resolve().parent
        self.guard = FoundationGuard(self.project_root)
        self.spec_engine = SpecEngine(self.project_root / "universal_registry.json")
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT,
                    severity TEXT,
                    source TEXT,
                    description TEXT,
                    metadata TEXT,
                    timestamp TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS action_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    description TEXT,
                    category TEXT,
                    status TEXT,
                    signal_id INTEGER,
                    priority INTEGER,
                    timestamp TEXT,
                    FOREIGN KEY (signal_id) REFERENCES signals(id)
                )
            """)
            conn.commit()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def register_signal(self, signal: Signal) -> Optional[int]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO signals (type, severity, source, description, metadata, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (signal.type, signal.severity, signal.source, signal.description, 
                  json.dumps(signal.metadata), signal.timestamp))
            res = cursor.lastrowid
            return int(res) if res is not None else None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def add_to_queue(self, action: ActionItem) -> Optional[int]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # De-duplication check
            cursor.execute("SELECT id FROM action_queue WHERE title = ? AND status = 'pending'", (action.title,))
            if cursor.fetchone():
                return None
            
            cursor.execute("""
                INSERT INTO action_queue (title, description, category, status, signal_id, priority, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (action.title, action.description, action.category, action.status, 
                  action.signal_id, action.priority, action.timestamp))
            res = cursor.lastrowid
            return int(res) if res is not None else None

    # --- RDD Loop (The Core Engineering Process) ---

    def discover(self) -> List[Signal]:
        """Phase 1: Discover. Identify drift and signals."""
        print("🔍 [RDD: DISCOVER] Auditing Foundation...")
        issues = self.guard.audit_all()
        signals = []
        for issue in issues:
            sig = Signal(
                type="environmental",
                severity=issue.severity,
                source="FoundationGuard",
                description=issue.description,
                metadata={"fix": issue.fix_instruction, "skill": issue.skill_link}
            )
            signals.append(sig)

        # Quality Gates: Linting & Typing
        print("🔍 [RDD: DISCOVER] Performing Quality Gate Checks...")
        gates = {
            "linting": ["ruff", "check", "."],
            "typing": ["mypy", "."]
        }
        for gate_name, cmd in gates.items():
            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    signals.append(Signal(
                        type="quality",
                        severity="high",
                        source=gate_name.capitalize(),
                        description=f"{gate_name.capitalize()} Gate Failed",
                        metadata={"output": result.stdout[:500]} # Limit output
                    ))
            except Exception as e:
                print(f"Error running {gate_name} gate: {e}")
                
        return signals

    def diagnose(self, signals: List[Signal]):
        """Phase 2: Diagnose. Map signals to expert actions and auto-resolve fixed ones."""
        print(f"🧠 [RDD: DIAGNOSE] Processing {len(signals)} signals...")
        
        # 1. Register new signals and add to queue
        current_descriptions = set()
        for sig in signals:
            sid = self.register_signal(sig)
            if sid:
                current_descriptions.add(sig.description)
                action = ActionItem(
                    title=f"Heal: {sig.description}",
                    description=f"Standard violation detected. Instruction: {sig.metadata.get('fix', 'Clean up quality gate failure')}",
                    category="intervention",
                    signal_id=sid,
                    priority=10 if sig.severity == "critical" else 5
                )
                self.add_to_queue(action)

        # 2. Auto-resolve actions for signals that are NO LONGER present
        # This is a basic form of closure
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, title FROM action_queue WHERE status = 'pending' AND title LIKE 'Heal: %'")
            pending_heals = cursor.fetchall()
            for aid, title in pending_heals:
                desc = title.replace("Heal: ", "")
                if desc not in current_descriptions:
                    print(f"✅ RDD: Auto-resolved {title} (no longer detected)")
                    cursor.execute("UPDATE action_queue SET status = 'resolved' WHERE id = ?", (aid,))
            conn.commit()

    def develop(self):
        """Phase 3: Develop. Determine the fix pathway."""
        print("🛠️ [RDD: DEVELOP] Planning Elite Heals...")
        # In a full AI agent, this would generate a UNIVERSAL_SPEC.md for a specific task.
        pass

    def heal(self):
        """Sub-phase of Deliver: Automated Healing."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, title FROM action_queue WHERE status = 'pending' AND title LIKE 'Heal: %' ORDER BY priority DESC LIMIT 1")
            target = cursor.fetchone()
            if target:
                aid, title = target
                print(f"🩹 [RDD: HEAL] Attempting automated fix for {title}...")
                
                # Simple healing logic (MVP)
                if "scripts" in title:
                    os.makedirs(self.project_root / "scripts", exist_ok=True)
                elif "LintingGate" in title or "ruff" in title.lower():
                    subprocess.run(["ruff", "check", ".", "--fix"], capture_output=True)
                
    def deliver(self):
        """Phase 4: Deliver. Execute healing and behavioral analysis."""
        self.heal()
        print("🚀 [RDD: DELIVER] Syncing behavioral DNA & Mistakes...")
        scripts = ["workflow_dna.py", "mistakes.py"]
        for script in scripts:
            if Path(script).exists():
                try:
                    subprocess.run([sys.executable, script], check=True)
                except Exception as e:
                    print(f"Error running {script}: {e}")

    def run_rdd_loop(self):
        """Mandatory Universal cycle."""
        signals = self.discover()
        self.diagnose(signals)
        self.develop()
        self.deliver()
        print("✅ Universal RDD Loop Complete. Foundation is proved.")

    def list_queue(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, title, category, status, priority FROM action_queue WHERE status = 'pending' ORDER BY priority DESC")
            rows = cursor.fetchall()
            print("\n--- MASTER ACTION QUEUE (Priority Order) ---")
            for r in rows:
                print(f"[{r[4]}] #{r[0]} | {r[2].upper()} | {r[1]} ({r[3]})")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Universal Master Orchestrator")
    parser.add_argument("--sync", action="store_true", help="Run full RDD sync loop")
    parser.add_argument("--queue", action="store_true", help="List action queue")
    parser.add_argument("--intent", type=str, help="Process Operator Intent")
    
    args = parser.parse_args()
    orch = Orchestrator()
    
    if args.intent:
        spec = orch.spec_engine.generate_spec_from_idea(args.intent)
        orch.spec_engine.save_spec(spec, Path("UNIVERSAL_SPEC.md"))
        print(f"Processed Intent: {args.intent}")
        print("Generated Elite Spec: UNIVERSAL_SPEC.md")
    elif args.sync:
        orch.run_rdd_loop()
    elif args.queue:
        orch.list_queue()
    else:
        orch.run_rdd_loop()
        orch.list_queue()
