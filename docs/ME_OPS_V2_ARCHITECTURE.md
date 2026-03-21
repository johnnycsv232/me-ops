# ME-OPS v2 Architecture: Personal Causal Intelligence System

## Status
Proposed architecture spec for the next major evolution of ME-OPS.

## Reframe
ME-OPS should evolve from a personal analytics pipeline into a **Personal Causal Intelligence System** built on top of Pieces-derived evidence.

### Core Thesis
- **Pieces** is the raw evidence substrate.
- **ME-OPS Ledger** becomes the canonical life ledger.
- **ME-OPS Intelligence** extracts causality, cases, heuristics, and operator guidance.
- **Operator Layer** applies those learnings in the moment.

This preserves the repo's current evidence-first standard while upgrading the system from descriptive reporting to actionable causal memory.

## Product Definition
ME-OPS v2 is a 3-layer intelligence stack:

1. **Canonical Life Ledger**: forensic, evidence-linked ground truth.
2. **Meaning Engine**: case extraction, causal inference, failure/success patterning.
3. **Active Operator**: recall, prediction, recommendation, guardrails, and compounding playbooks.

## Design Principles
1. **Evidence first**: every claim must resolve back to raw exports, normalized events, artifacts, or derived case evidence.
2. **Causality over activity**: prioritize sequences, triggers, branches, and outcomes over flat counts.
3. **Decision-centric modeling**: actions alone are insufficient; decisions must be explicit first-class objects.
4. **Separate memory types**: artifact memory and behavior memory are modeled separately and joined intentionally.
5. **Case-based reasoning by default**: new situations should be matched against prior incidents, wins, and dead ends.
6. **Operator usefulness over introspection theater**: outputs should help decide what to do now.
7. **Verdict loops**: important decisions and hypotheses must be revisited and scored after outcomes arrive.

## The 3-Layer Intelligence Stack

### Layer 1: Canonical Life Ledger
The ledger is not a generic activity log. It is a normalized, append-only evidence graph describing what happened, in what order, and with what proof.

#### Ledger object types
Every normalized record should be expressible as one or more of the following object classes:
- `event`
- `artifact`
- `decision`
- `failure`
- `hypothesis`
- `relationship`
- `system_state`
- `outcome`

#### Questions Layer 1 must answer
- What happened?
- In what order?
- What evidence proves it?
- What context existed at the time?
- Which entities, systems, and projects were involved?
- Which events appear causally adjacent or causally relevant?

### Layer 2: Meaning Engine
The Meaning Engine transforms ledger evidence into reusable intelligence.

#### Core outputs
- recurring failure chains
- recurring success chains
- win signatures
- anti-playbooks
- decision heuristics
- cognitive blind spots
- environment drift patterns
- project-specific operating patterns
- context-collapse indicators
- reusable resolution paths

#### Key questions
- Which sequences usually precede a win?
- Which sequences usually precede wasted hours?
- Which environments reliably generate failure?
- Which plans correlate with shipping versus looping?
- Which interventions resolve specific bug families fastest?
- Which decisions looked correct locally but harmed downstream outcomes?

### Layer 3: Active Operator
The Operator consumes current context and returns situation-aware interventions.

#### Required capabilities
1. **Recall**: retrieve similar past cases, fix paths, and decision precedents.
2. **Predict**: estimate risk of spirals, sinks, drift, or delay.
3. **Recommend**: suggest next best steps from evidence-weighted heuristics.
4. **Guardrail**: warn when current behavior matches a known failed path.
5. **Compound**: turn repeated cases into SOPs, decision trees, and playbooks.

#### Example operator prompts
- “This looks similar to bug cluster #14; last successful fix path was env validation → config check → service restart → endpoint test.”
- “This workflow historically turns into a planning spiral unless you isolate scope within 15 minutes.”
- “Do not open more tools yet; similar incidents resolved fastest with terminal-first inspection.”

## Domain Model

### Core canonical entities
The v2 domain model should extend current event/entity/workflow tables with explicit causal and evaluative objects.

#### 1. `ledger_events`
Atomic facts derived from Pieces or ingestion transforms.

Suggested fields:
- `ledger_event_id`
- `source_event_id`
- `event_type`
- `ts_start`
- `ts_end`
- `actor_id`
- `project_id`
- `session_id`
- `summary`
- `raw_payload_ref`
- `confidence`
- `evidence_refs`
- `ingest_version`

#### 2. `artifacts`
Created, modified, referenced, or inspected digital objects.

Suggested fields:
- `artifact_id`
- `artifact_type` (`file`, `prompt`, `commit`, `config`, `terminal_output`, `web_page`, `message`)
- `uri`
- `title`
- `project_id`
- `created_at`
- `updated_at`
- `content_hash`
- `evidence_refs`

#### 3. `decisions`
First-class decision objects.

Suggested fields:
- `decision_id`
- `ts`
- `project_id`
- `session_id`
- `decision_type`
- `context_summary`
- `problem_statement`
- `options_considered` (JSON)
- `choice_made`
- `rationale`
- `confidence`
- `expected_cost`
- `expected_benefit`
- `evidence_refs`
- `status` (`proposed`, `taken`, `revisited`, `superseded`)

#### 4. `decision_outcomes`
Retrospective evaluation of decisions.

Suggested fields:
- `decision_outcome_id`
- `decision_id`
- `outcome_id`
- `verdict` (`good`, `mixed`, `bad`, `unknown`)
- `actual_cost`
- `actual_benefit`
- `time_to_outcome_sec`
- `surprises` (JSON)
- `lesson`
- `scored_at`

#### 5. `failure_chains`
Compact models of problems and their resolution paths.

Suggested fields:
- `failure_chain_id`
- `project_id`
- `case_title`
- `symptom`
- `suspected_trigger`
- `first_response`
- `diagnosis_path` (JSON)
- `false_paths` (JSON)
- `final_fix`
- `time_lost_min`
- `severity`
- `future_rule`
- `evidence_refs`
- `resolution_confidence`

#### 6. `win_cases`
Structured positive cases for success-pattern mining.

Suggested fields:
- `win_case_id`
- `project_id`
- `goal`
- `preconditions` (JSON)
- `sequence` (JSON)
- `tools_used` (JSON)
- `context_switch_count`
- `duration_min`
- `quality_signal`
- `downstream_value`
- `why_it_worked`
- `evidence_refs`

#### 7. `hypotheses`
Candidate interpretations awaiting confirmation.

Suggested fields:
- `hypothesis_id`
- `statement`
- `scope`
- `supporting_evidence_refs`
- `contradicting_evidence_refs`
- `confidence`
- `status` (`candidate`, `supported`, `rejected`, `stale`)
- `review_due_at`

#### 8. `system_states`
State snapshots for environments and runtime conditions.

Suggested fields:
- `system_state_id`
- `ts`
- `environment_name`
- `repo_root`
- `branch`
- `tool_versions` (JSON)
- `env_vars_fingerprint`
- `ports_state` (JSON)
- `service_state` (JSON)
- `drift_signals` (JSON)
- `evidence_refs`

#### 9. `relationships`
Explicit or inferred links between objects.

Suggested fields:
- `relationship_id`
- `from_object_type`
- `from_object_id`
- `to_object_type`
- `to_object_id`
- `relationship_type` (`caused`, `supported_by`, `blocked_by`, `resolved_by`, `similar_to`, `belongs_to`)
- `confidence`
- `evidence_refs`
- `inference_method`

#### 10. `operator_briefings`
Daily or on-demand action-oriented briefings.

Suggested fields:
- `briefing_id`
- `generated_at`
- `time_horizon`
- `active_risks` (JSON)
- `relevant_cases` (JSON)
- `recommended_moves` (JSON)
- `avoid_list` (JSON)
- `highest_leverage_move`
- `evidence_refs`

## Separation of Memory Types

### Artifact memory
Answers: **What did I make, inspect, change, or reuse?**

Examples:
- code files
- prompts
- commits
- configs
- reports
- terminal outputs
- screenshots
- exported records

### Behavior memory
Answers: **How do I operate under specific conditions?**

Examples:
- planning loops
- panic debugging
- context thrashing
- productive windows
- tool-hopping
- late-night degradation
- recovery patterns
- momentum signatures

### Why the separation matters
Artifact memory helps retrieval of concrete work product. Behavior memory helps intervention on operating patterns. Both should be queryable independently and linked through cases, sessions, projects, and outcomes.

## Major Modules

### 1. `ledger`
Purpose: normalize Pieces exports and current project evidence into canonical objects and relationship edges.

Responsibilities:
- maintain append-only evidence ingestion
- map raw exports to canonical object types
- preserve source lineage and evidence references
- emit stable IDs for cross-module joins

Possible implementation components:
- extend `ingest.py`
- add `ledger.py`
- add canonical object mappers and validators

### 2. `cases`
Purpose: turn sequences into reusable incident and success cases.

Responsibilities:
- group events into bug cases, decisions, launches, wins, failures, and recoveries
- construct decision objects and verdict loops
- materialize failure chains and win cases

Possible implementation components:
- add `cases.py`
- extend `workflows.py` sessionization outputs
- extend `mistakes.py` from pattern counts to chain extraction

### 3. `causal`
Purpose: infer likely causal links and temporal dependencies.

Responsibilities:
- estimate “A led to B” relationships
- build trigger → response → outcome chains
- detect common branch points and dead-end paths
- score confidence for inferred relationships

Possible implementation components:
- add `causal.py`
- store causal edges in `relationships`
- derive confidence from recurrence, temporal locality, and explicit evidence

### 4. `heuristics`
Purpose: extract reusable operating rules.

Responsibilities:
- compute “when X, do Y” heuristics
- identify anti-playbooks and win signatures
- score rule quality from outcome evidence
- version heuristics as new cases accumulate

Possible implementation components:
- add `heuristics.py`
- extend `mistakes.py` anti-playbook logic
- persist rules with evidence-weighted confidence

### 5. `briefing`
Purpose: generate present-tense operator briefings instead of passive summaries.

Responsibilities:
- answer what matters now
- surface active risks and likely traps
- recall similar prior cases
- recommend next actions with rationale

Possible implementation components:
- evolve `briefing.py`
- change output shape from historical report to operator decision support

### 6. `operator`
Purpose: real-time application layer for recall, prediction, and guardrails.

Responsibilities:
- accept current issue/context input
- retrieve similar cases
- rank likely next actions
- warn on known dead ends
- capture whether advice worked

Possible implementation components:
- evolve `agent.py` and `live.py`
- add feedback capture for recommendation quality

## End-to-End Flow

### Flow A: Canonical ingestion
1. Import raw Pieces JSON exports.
2. Normalize to current events, entities, files, projects, tools, and sessions.
3. Map normalized records into canonical ledger object classes.
4. Attach evidence references and lineage metadata.
5. Store explicit and inferred relationships.

### Flow B: Case construction
1. Group related events within sessions and projects.
2. Detect candidate decisions, failures, and outcomes.
3. Build structured case objects.
4. Link artifacts, system states, and evidence.
5. Score confidence and mark unresolved hypotheses.

### Flow C: Meaning extraction
1. Cluster similar failure chains and win cases.
2. Mine recurring sequences and trigger patterns.
3. Generate heuristics and anti-playbooks.
4. Re-score prior heuristics based on later outcomes.
5. Materialize operator-ready insights.

### Flow D: Active operator loop
1. User enters a live issue or current focus.
2. Retrieve most similar prior cases and environment matches.
3. Predict likely sink, risk, or success trajectory.
4. Recommend next best moves.
5. Capture user response and final outcome.
6. Feed the result back into decisions, cases, and heuristics.

## Query Patterns the System Should Support

### Ledger queries
- What exactly happened between timestamps X and Y?
- Show the evidence trail for this bug or decision.
- Which artifacts and tools were involved in this case?

### Case-based queries
- Show the 5 most similar incidents to this current bug.
- Which prior fixes worked for this symptom in this environment?
- What false paths should I avoid here?

### Heuristic queries
- What usually works when config drift appears?
- Which project conditions predict wasted time?
- What planning patterns most often lead to shipment?

### Operator queries
- What matters now?
- What is the highest-leverage next move?
- What active risk should I guard against today?
- Which prior case should I remember before acting?

## Scoring Frameworks

### Decision quality score
Score dimensions:
- clarity of problem framing
- breadth of options considered
- rationale quality
- evidence support at decision time
- downstream outcome quality
- speed-to-verdict

### Failure-chain quality score
Score dimensions:
- symptom specificity
- causal trace completeness
- false-path capture quality
- final-fix reliability
- transferability to future incidents

### Win-signature quality score
Score dimensions:
- reproducibility
- output quality
- time efficiency
- context stability
- downstream value

### Heuristic quality score
Score dimensions:
- recurrence support
- outcome lift
- domain specificity
- freshness
- contradiction rate

## Recommended Implementation Roadmap

### Phase 1: Canonical Ledger Foundation
Build first:
- canonical object schema
- evidence reference standard
- decision object support
- system state snapshots
- relationship storage

Deliverables:
- `docs/ME_OPS_V2_ARCHITECTURE.md`
- new DDL for ledger tables
- ingestion upgrade for canonical object emission

### Phase 2: Case Extraction Engine
Build next:
- decision extraction
- failure chain construction
- win case construction
- outcome verdict loops

Deliverables:
- `cases.py`
- `failure_chains`, `win_cases`, `decision_outcomes` tables
- initial backfill job over existing data

### Phase 3: Causal + Heuristic Layer
Build next:
- causal edge inference
- case similarity retrieval
- anti-playbooks and win signatures
- rule scoring and versioning

Deliverables:
- `causal.py`
- `heuristics.py`
- operator-facing case retrieval queries

### Phase 4: Operator Briefing System
Build next:
- operator briefing output format
- risk flags
- similar-case recall section
- highest-leverage next move logic

Deliverables:
- v2 `briefing.py`
- `operator_briefings` table
- markdown and JSON briefing outputs

### Phase 5: Live Operator Loop
Build last in the first major wave:
- interactive recall/predict/recommend loop
- dead-end warnings
- recommendation feedback capture
- continuous learning from outcomes

Deliverables:
- upgraded `agent.py` / `live.py`
- recommendation outcome capture
- briefing-to-action-to-verdict loop

## Concrete Build Order
1. **Extend schema before models**: make decision, outcome, state, relationship, and case tables real first.
2. **Upgrade ingest before analysis**: if the ledger is weak, all higher layers will hallucinate structure.
3. **Extract cases before heuristics**: rules should be learned from cases, not directly from noisy flat events.
4. **Ship recall before prediction**: accurate retrieval of similar prior cases creates immediate operator value.
5. **Ship guardrails before complex autonomy**: dead-end prevention is safer and likely more useful than aggressive auto-planning.

## Mapping From Current Repo to v2

### Current strengths to preserve
- evidence-first posture in the README
- normalized ingestion pipeline in `ingest.py`
- workflow/session extraction in `workflows.py`
- entity cross-referencing in `entities.py`
- anti-pattern logic in `mistakes.py`
- reporting/orchestration infrastructure already present in `briefing.py`, `master.py`, and `agent.py`

### Main upgrades required
- move from report-centric outputs to case-centric memory
- treat decisions as first-class records
- convert failure detection into failure-chain modeling
- add win signatures, not just failure counts
- add system-state snapshots for environment drift detection
- add relationship edges for causality and similarity
- add verdict loops so learning compounds over time

## Definition of Success for v2
ME-OPS v2 succeeds when it can reliably do all of the following:
- reconstruct the evidence trail behind meaningful work episodes
- retrieve similar past incidents during a live problem
- identify recurring success and failure sequences
- explain why a recommendation is being made
- warn when the current path matches a known dead end
- convert repeated outcomes into reusable playbooks and anti-playbooks

## Summary
The strategic pivot is not from archive to smaller tracker.

It is from **archive of life** to **computable memory of causality**.

That means:
- **Pieces** remains total recall.
- **ME-OPS v2** becomes the intelligence layer.
- **The Operator** becomes the real-time applied advantage.
