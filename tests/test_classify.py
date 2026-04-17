import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.ledger.classify import classify_event_kind, classify_project, extract_tools


class ClassifyTests(unittest.TestCase):
    def test_known_project_signal(self):
        text = "OpenClaw agent install and heartbeat setup in WSL"
        self.assertEqual(classify_project(text), "openclaw")

    def test_unknown_project_returns_none(self):
        self.assertIsNone(classify_project("Plain note without a known project marker"))

    def test_ai_agent_infra_signal(self):
        text = "Perfect Memory prompt engineering and AI assistant configuration audit"
        self.assertEqual(classify_project(text), "ai-agent-infra")

    def test_notion_signal(self):
        text = "Notion workspace documentation and knowledge base cleanup"
        self.assertEqual(classify_project(text), "notion-ops")

    def test_debug_event_kind(self):
        text = "Debug session to investigate a timeout and fix gateway startup"
        self.assertEqual(classify_event_kind(text), "debug_step")

    def test_extract_tools_in_order(self):
        tools = extract_tools("Used PowerShell, Git, and Pieces from WSL")
        self.assertEqual(tools, ["powershell", "wsl", "git", "pieces"])


if __name__ == "__main__":
    unittest.main()
