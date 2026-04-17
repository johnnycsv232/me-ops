"""
Project and event-kind classifier — enhanced v2.
Maps raw Pieces content → canonical project_id and event_kind.
"""
from __future__ import annotations

PROJECT_SIGNALS: dict[str, list[str]] = {
    "ironclad": [
        "ironclad", "iron clad", "ironcladd", "zip api", "zip availability",
        "stripe branding", "missed call", "lead capture", "local trades",
        "revenue recovery", "ironclad-mvp", "ironclad_mvp", "ironclad recovery",
        "ironclad audit", "ironclad plan", "ironclad business", "ironclad stack",
        "ironclad fixes", "ironclad website", "ironclad live",
        "saas platform design", "automated revenue", "after-hours",
    ],
    "gettupp-ent": [
        "gettupp ent", "gettupp|ent", "gettuppent", "gettupp entertainment",
        "nightlife content", "pilot $345", "audit $300", "retainer",
        "ge wordmark", "gold #d4af37", "hot pink #ff1493", "minneapolis skyline",
        "nightlife brand", "gettuppbot", "nightlife keyword", "nightlife ai",
        "minneapolis nightlife", "gettupp landing", "gettupplanding",
    ],
    "gettupp-girls": [
        "gettupp girls", "gettuppgirls", "gettupp girls apparel",
        "lifestyle brand", "deep navy", "upward arrow swoosh",
    ],
    "ai-time-arb": [
        "ai time arbitrage", "3-5 day build", "enterprise app build",
        "electric blue #00d4ff", "deep purple #7b2d8e",
    ],
    "ai-agent-infra": [
        "perfect memory", "pieces desktop", "pieces pro",
        "ai assistant configuration", "ai configuration", "agent infrastructure",
        "agent hardening", "agent diagnostics", "agent orchestration",
        "prompt engineering", "ltm insight", "memory retention",
        "autonomous ai agent", "ai co-founder", "deep work insight extraction",
        "gemini assistant", "model selection", "ai agent configuration",
        "ai agent setup", "agent integration", "ai ecosystem setup",
        "ai stack", "stack validation", "stack reset", "stack overhaul",
        "environment troubleshooting", "environment setup diagnostics",
        "nvidia ai infrastructure", "nvidia cloud integration",
    ],
    "openclaw": [
        "openclaw", "open claw", "clawdbot", "moltbot", "mcporter",
        "openclaw gateway", "claw.kilosessions", "openclaw control",
        "openclaw agent", "openclaw profile", "openclaw install",
        "openclaw setup", "openclaw environment", "openclaw lead",
        "openclaw cleanup", "openclaw reinstall", "openclaw wsl",
        "openclaw research", "openclaw onboarding", "openclaw platform",
        "openclaw skill", "openclaw heartbeat", "openclaw telegram",
        "openclaw browser", "openclaw permissions", "openclaw task",
        "lead gen setup", "gettuppbot", "sovereign", "overlord", "elite",
        "professional persona", "persona report", "elite system architect",
        "elite overlord protocol", "repository sovereign",
        "kimi k2.5", "telegram autonomous", "paperclip + ollama",
    ],
    "antigravity": [
        "antigravity", "anti_active", "antigravity kit", "antigravity.exe",
        "antigravity browser", "antigravity-kit", "antigravity skills",
        "handoff_final", "reinstallation_complete", "cutting_edge_2026",
        "antigravity ide", "antigravity setup", "antigravity config",
        "antigravity install", "antigravity health", "antigravity cleanup",
        "antigravity fix", "antigravity reset", "antigravity stabiliz",
        "antigravity overhaul", "antigravity troubleshoot",
        "antigravity reinstall", "antigravity agent",
        "anti_active", "anti active",
    ],
    "paperclip": [
        "paperclip", "paperclipai", "pcp_invite", "founding engineer",
        "paperclip server", "paperclip org", "paperclip wsl",
        "paperclip troubleshoot", "paperclip startup", "paperclip agent",
        "paperclip integration", "paperclip onboard", "paperclip connect",
        "paperclip codex", "paperclip gateway", "paperclip v1",
        "openwork", "accomplish formerly",
    ],
    "nemo-claw": [
        "nemoclaw", "nemo claw", "nvidia nemo", "nemoclaw deployment",
        "nemoclaw wsl", "nemoclaw setup", "nemoclaw debug", "nemoclaw stack",
        "nemoclaw install", "nemoclaw onboard", "nemoclaw troubleshoot",
        "nemoclaw recovery", "nemoclaw wsl2", "nemo wsl",
        "brev cli", "brev cloud", "nvidia nim", "safer ai agents",
    ],
    "me-ops": [
        "me-ops", "me_ops", "meops", "causal intelligence", "ledger",
        "operator briefing", "heuristic", "failure chain", "win signature",
        "decision replay", "canonical", "me ops v2", "personal analytics",
        "me-ops v2", "personal causal", "workflow dna", "dna reporting",
        "dna extraction", "architect intelligence warehouse", "spec engine",
    ],
    "notion-ops": [
        "notion", "knowledge base", "wiki", "database property",
        "page property", "workspace doc", "workspace documentation",
        "notion spec", "notion setup", "notion workflow", "cofounder os",
        "morning brief", "standup update", "time breakdown",
        "top of mind", "today's headlines",
    ],
    "wsl-infra": [
        "wsl stabiliz", "wsl2 stabiliz", "wsl2 recovery", "wsl2 setup",
        "wsl dns fix", "wsl nix", "wsl docker", "wsl filesystem",
        "wsl ide", "wsl mcp", "wsl ai", "wsl cleanup", "wsl audit",
        "wsl2 ide", "wsl2 ai", "wsl2 debug", "wsl2 dev env",
        "dev environment setup", "development environment",
        "ide stabilization", "ide troubleshoot", "ide health",
        "ide overhaul", "system cleanup", "system audit",
        "system remediation", "package manager", "nvm debug",
        "nvm setup", "node setup", "gateway fix windsurf",
        "windsurf connectivity", "windsurf wsl", "windsurf mcp",
        "windsurf directory", "codewiki", "code wiki",
    ],
    "blink-agent": [
        "blink ai", "blink agent", "blink debug", "blink model",
    ],
    "claude-integration": [
        "claude integration", "claude config", "claude setup",
        "mcp integration", "mcp debug", "mcp server config",
        "mcp setup", "claude desktop mcp", "pieces mcp",
        "multi-platform memory", "ai personalization",
        "agent identity", "agent onboarding", "agent profile",
        "ai environment config", "cloud setup ai",
        "ai infrastructure setup",
    ],
}

EVENT_KIND_SIGNALS: dict[str, list[str]] = {
    "error":          ["error", "failed", "failure", "exception", "traceback", "crash", "broken", "refused", "timeout"],
    "deploy":         ["deploy", "deployment", "vercel", "production", "promote", "ship", "released"],
    "commit":         ["commit", "git", "branch", "push", "pull request", "pr "],
    "debug_step":     ["debug", "diagnose", "troubleshoot", "investigate", "fixing", "resolv"],
    "context_switch": ["switch", "moved to", "pivot", "changed focus", "now working on", "shifted"],
    "run":            ["ran", "executed", "running", "run ", "started", "launched", "installed"],
    "edit":           ["edited", "updated", "modified", "changed", "refactored", "configured"],
    "search":         ["searched", "looked up", "researched", "found", "browsing", "reviewed"],
    "message":        ["message", "telegram", "slack", "dm ", "chat", "replied", "sent"],
    "audio":          ["audio", "voice", "transcription", "microphone", "speaker"],
    "clipboard":      ["clipboard", "copied", "pasted"],
    "vision":         ["screenshot", "screen capture", "ocr", "vision"],
    "summary":        ["summary", "summarized", "workstream", "session summary", "tldr"],
    "plan":           ["plan", "planning", "roadmap", "strategy", "design", "architect"],
}


def classify_project(text: str) -> str | None:
    text_lower = text.lower()
    scores: dict[str, int] = {}
    for proj, signals in PROJECT_SIGNALS.items():
        for sig in signals:
            if sig in text_lower:
                scores[proj] = scores.get(proj, 0) + 1
    if not scores:
        return None
    return max(scores, key=lambda p: scores[p])


def classify_event_kind(text: str) -> str:
    text_lower = text.lower()
    scores: dict[str, int] = {}
    for kind, signals in EVENT_KIND_SIGNALS.items():
        for sig in signals:
            if sig in text_lower:
                scores[kind] = scores.get(kind, 0) + 1
    if not scores:
        return "unknown"
    return max(scores, key=lambda k: scores[k])


def extract_tools(text: str) -> list:
    tools = [
        "powershell", "wsl", "curl", "node", "python", "git", "docker",
        "windsurf", "antigravity", "claude", "chatgpt", "telegram", "chrome",
        "codex", "ollama", "pieces", "vercel", "stripe", "postgres", "sqlite",
        "ngrok", "cloudflared", "pnpm", "npm", "nvm", "brev",
        "openai", "gemini", "anthropic",
    ]
    tl = text.lower()
    return [t for t in tools if t in tl]


def detect_failure_signals(text: str) -> bool:
    signals = [
        "failed", "error", "broken", "not working", "can't connect",
        "refused", "timeout", "crash", "exception", "stuck",
        "couldn't", "could not", "no route", "connection refused",
        "not found", "404", "500", "econnrefused", "traceback",
        "fatal", "critical", "blocked", "hung", "unresponsive",
        "mismatch", "invalid", "unauthorized", "forbidden",
        # workstream summary language
        "persistent issue", "recurring problem", "unable to",
        "did not work", "kept failing", "would not",
        "encountered an error", "internal server error",
        "permission denied", "quota exceeded", "invalid environment variables",
        "continued to fail", "still broken", "not resolving",
        "credit-related limitation", "credit limit",
    ]
    tl = text.lower()
    return any(s in tl for s in signals)


def detect_outcome_signals(text: str) -> bool:
    signals = [
        "resolved", "fixed", "working", "successfully",
        "shipped", "deployed", "solved", "operational",
        "connected", "verified", "confirmed", "stable",
        "migrated", "now works", "is working",
        "passed", "green", "up and running", "running state",
        # workstream summary language
        "successfully configured", "successfully installed",
        "successfully connected", "successfully resolved",
        "was resolved", "has been fixed", "now operational",
        "now functional", "properly configured", "fully operational",
    ]
    tl = text.lower()
    return any(s in tl for s in signals)
