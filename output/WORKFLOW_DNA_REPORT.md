# WORKFLOW DNA REPORT

*Generated: 2026-02-20 22:53 CST*

## Coverage

- Events analyzed: 14367 | Active days: 58
- Observation window: 2025-12-02 -> 2026-02-19

## Unique Style

- Query-Ladder Refinement
- Evidence-Seeking Prompting
- Zero-Drift Orchestration
- Capture-First Consolidation

- Key metrics: notion_first=0.0, closure=0.553, query_ladder=0.937

## Genetic Markers

| Marker | Transition | Frequency | Days | Avg Gap (s) | Strength |
| :--- | :--- | ---: | ---: | ---: | ---: |
| Evidence Compression | `framing -> synthesis` | 1211 | 50 | 54.0 | 0.911 |
| Constraint Framing | `query -> framing` | 640 | 29 | 23.1 | 0.683 |
| Query to Activity Relay | `query -> activity` | 253 | 12 | 5.0 | 0.5 |
| Synthesis to Query Relay | `synthesis -> query` | 529 | 28 | 1621.1 | 0.493 |
| Synthesis to Framing Relay | `synthesis -> framing` | 481 | 29 | 3256.8 | 0.474 |
| Synthesis to Conversation Relay | `synthesis -> conversation` | 129 | 24 | 173.3 | 0.435 |
| Conversation to Framing Relay | `conversation -> framing` | 86 | 22 | 190.5 | 0.413 |
| Activity to Query Relay | `activity -> query` | 308 | 33 | 2980.7 | 0.402 |

## Prompt + Orchestration Profile

- Orchestration signature: `framing -> synthesis; closure=55%; query_ladder=94%`
- Prompt fingerprint: query_count=3684.0, question_ratio=0.906, evidence_ratio=0.795, systems_ratio=0.154

## Bottlenecks

- **Query Loop Fatigue** (medium | impact 4766.55)
  - Evidence: 668 loops >=4 steps, 6139 total query/framing events
  - Fix: After two query rounds, require a one-sentence decision or next action.

## Premium Workflows

### Premium Workflow 01: Intent Command Sprint
- Trigger: Start of deep-work block
- Base marker: `framing -> synthesis`
- KPI: Research-to-synthesis closure rate >= 70%
  1. Open command center and define one measurable outcome.
  2. Run two focused research hops max before forcing a query decision.
  3. Compress findings into workstream summary plus annotation notes.
  4. Hand off to code/file action within five minutes.
  5. Close with a one-line execution checkpoint.

### Premium Workflow 02: Query Ladder to Artifact Forge
- Trigger: Any high-ambiguity problem
- Base marker: `query -> framing -> synthesis`
- KPI: Decision latency from query to synthesis <= 90 seconds median
  1. Issue one evidence-seeking query (not open-ended browsing).
  2. Pin a time range or scope boundary immediately.
  3. Write one decisive synthesis statement.
  4. Transform synthesis into artifact: code, schema, or deliverable note.
  5. Capture counterfactual: what would invalidate this decision?

### Premium Workflow 03: Anti-Drift Recovery Loop
- Trigger: When Query Loop Fatigue signal appears
- Base marker: `web -> web (drift interrupt)`
- KPI: Browser drift episodes reduced by 40% over 14 days
  1. Detect drift trigger (3+ consecutive web actions or looping queries).
  2. Pause exploration and run a 60-second framing checkpoint.
  3. Force a workstream summary before resuming exploration.
  4. Either commit to build mode or intentionally exit the loop.
  5. Log the interruption source to improve future guardrails.
