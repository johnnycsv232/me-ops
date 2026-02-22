# WORKFLOW DNA REPORT

*Generated: 2026-02-22 05:41 CST*

## Coverage

* Events analyzed: 28013 | Active days: 64
* Observation window: 2025-12-02 -> 2026-02-19

## Unique Style

* Query-Ladder Refinement
* Evidence-Seeking Prompting
* Exploration-Heavy Discovery
* Capture-First Consolidation

* Key metrics: notion_first=0.056, closure=0.29, query_ladder=0.927

## Genetic Markers

| Marker | Transition | Frequency | Days | Avg Gap (s) | Strength |
| :--- | :--- | ---: | ---: | ---: | ---: |
| Evidence Compression | `framing -> synthesis` | 1234 | 52 | 1.2 | 0.925 |
| Constraint Framing | `query -> framing` | 639 | 29 | 22.9 | 0.678 |
| Synthesis to Web Relay | `synthesis -> web` | 729 | 49 | 1733.9 | 0.57 |
| Query to Activity Relay | `query -> activity` | 251 | 12 | 5.0 | 0.497 |
| Github to Web Relay | `github -> web` | 217 | 42 | 73.7 | 0.494 |
| Activity to Query Relay | `activity -> query` | 270 | 21 | 188.7 | 0.487 |
| Local_Web to Web Relay | `local_web -> web` | 153 | 39 | 120.7 | 0.457 |
| Synthesis to Conversation Relay | `synthesis -> conversation` | 107 | 24 | 59.4 | 0.454 |

## Web Destination Breakdown

Exact destinations behind high-signal transitions that end on web/local_web.

### Synthesis to Web Relay (`synthesis -> web` | 729 hops)

| Destination | Hits | Share | Intent | Next Step | Avg Next Gap (s) | Example Target |
| :--- | ---: | ---: | :--- | :--- | ---: | :--- |
| unknown | 80 | 11.0% | unstructured_target | web_visit (web) | 424.1 | `@. @DOCS @DOCS @GettUppent Status Audit @GettUpp OS Production Audit` |
| google.com | 32 | 4.4% | search | web_visit (web) | 152.4 | `https://google.com/search?gs_ssp=eJzj4tTP1Tcwy8ioNDBg9OLKLEvMU0gpSkzPBwBPcAce&q=ivan+drago&oq=ivan+dr&gs_lcrp=EgZjaHJ...` |
| aistudio.google.com | 27 | 3.7% | page_visit | web_visit (web) | 237.3 | `https://aistudio.google.com/prompts/1UqZTiF52aYU5ombjz59CQ4ijE83CnWtI` |
| gemini.google.com | 20 | 2.7% | page_visit | web_visit (web) | 169.0 | `https://gemini.google.com/app/0af436064141f2da#4f5a97e2a2dba7ff` |
| notebooklm.google.com | 17 | 2.3% | page_visit | web_visit (web) | 3038.2 | `https://notebooklm.google.com/?icid=home_maincta` |
| youtube.com | 16 | 2.2% | video_review | web_visit (web) | 235.2 | `https://youtube.com/watch?v=AwMZGb_IMRk` |
| daily-cloudcode-pa.googleapis.com | 16 | 2.2% | api_call | web_visit (web) | 192.5 | `https://daily-cloudcode-pa.googleapis.com/vlinternal:streamGenerateContent?alt=sse` |
| nextjs.org | 14 | 1.9% | docs_read | web_visit (web) | 139.5 | `https://nextjs.org/docs/app/guides/incremental-static-regeneration` |

### Github to Web Relay (`github -> web` | 217 hops)

| Destination | Hits | Share | Intent | Next Step | Avg Next Gap (s) | Example Target |
| :--- | ---: | ---: | :--- | :--- | ---: | :--- |
| unknown | 13 | 6.0% | unstructured_target | web_visit (web) | 107.8 | `{"url": "", "interactions": 0}` |
| opencollective.com | 7 | 3.2% | page_visit | web_visit (web) | 1.1 | `https://opencollective.com/postcss/` |
| windsurf.com | 5 | 2.3% | page_visit | hint_suggested_query (query) | 132.2 | `https://windsurf.com/codemaps/494f4667-f0c9-4ded-a561-e001241a53ad-a3ad7ad60b616a34` |
| vercel.com | 5 | 2.3% | page_visit | web_visit (web) | 7.4 | `https://vercel.com/gettupp-ents-projects/gettupp-enterprise/settings/environments` |
| google.com | 5 | 2.3% | search | web_visit (web) | 105.4 | `https://google.com/search?q=dataplex&oq=dataplex&gs_lcrp=EgZjaHJvbWUyCQgAEEUYORiABDIHCAEQABiABDIHCAIQABiABDIHCAMQABiA...` |
| gemini.google.com | 5 | 2.3% | page_visit | web_visit (web) | 306.0 | `https://gemini.google.com/app/03f4411771bb0b84` |
| accounts.google.com | 5 | 2.3% | auth_flow | web_visit (web) | 13.4 | `https://accounts.google.com/v3/signin/challenge/pwd?TL=AHE1sGWDfookGS3DYYOHwubJVc6` |
| codesandbox.io | 4 | 1.8% | page_visit | web_visit (web) | 2.9 | `https://codesandbox.io/s/piecs-6b80q1` |

### Local_Web to Web Relay (`local_web -> web` | 153 hops)

| Destination | Hits | Share | Intent | Next Step | Avg Next Gap (s) | Example Target |
| :--- | ---: | ---: | :--- | :--- | ---: | :--- |
| accounts.google.com | 22 | 14.4% | auth_flow | web_visit (web) | 33.3 | `https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fclou...` |
| unknown | 11 | 7.2% | unstructured_target | web_visit (web) | 112.3 | `perplexity.ai/search/thoroughly-audit-test-as-a-cus-WvLVSNNcSRqe8z4kirDoKg` |
| daily-cloudcode-pa.googleapis.com | 8 | 5.2% | api_call | web_visit (web) | 145.7 | `https://daily-cloudcode-pa.googleapis.com/vlinternal:streamGenerateContent?aIt=sse` |
| docs.pieces.app | 4 | 2.6% | docs_read | web_visit (web) | 42.1 | `https://docs.pieces.app/products/meet-pieces/fundamentals#ltm-27` |
| nextjs.org | 4 | 2.6% | docs_read | web_visit (web) | 17.5 | `https://nextjs.org/docs/api-reference/config/next-config-options` |
| gemini.google.com | 3 | 2.0% | page_visit | web_visit (web) | 153.5 | `https://gemini.google.com/u/1/app/d59b4e23c469c806?pageId=none` |
| go.microsoft.com | 3 | 2.0% | page_visit | hint_suggested_query (query) | 93.1 | `https://go.microsoft.com/fwlink/p/?Linkld=615615` |
| notebooklm.google.com | 3 | 2.0% | page_visit | web_visit (web) | 65.9 | `https://notebooklm.google.com/notebook/acef10e7-71c1-4fb6-944c-6e8d93dcaeab` |


## Prompt + Orchestration Profile

* Orchestration signature: `framing -> synthesis; closure=29%; query_ladder=93%`
* Prompt fingerprint: query_count=8402.0, question_ratio=0.96, evidence_ratio=0.38, systems_ratio=0.12

## Bottlenecks

* **Browser Drift** (high | impact 6410.61)
  * Evidence: 841 drift episodes, avg run 10.9, max run 128
  * Fix: Trigger a forced synthesis checkpoint after 3 consecutive web events.
* **Query Loop Fatigue** (medium | impact 4848.3)
  * Evidence: 705 loops >=4 steps, 6074 total query/framing events
  * Fix: After two query rounds, require a one-sentence decision or next action.

## Current Problems (Detailed)

Concrete activity context for each active problem: where it happens and what you do next.

### Browser Drift (high | impact 6410.61)

* Evidence: 841 drift episodes, avg run 10.9, max run 128
* Fix: Trigger a forced synthesis checkpoint after 3 consecutive web events.
* Sampled events: 9127
| Destination/Stage | Hits | Share | Intent | Next Step | Avg Next Gap (s) | App Tool | Example Target |
| :--- | ---: | ---: | :--- | :--- | ---: | :--- | :--- |
| unknown | 1024 | 11.2% | unstructured_target | web_visit (web) | 248.6 | n/a | `youtube.com` |
| google | 789 | 8.6% | unstructured_target | web_visit (web) | 62.4 | n/a | `aistudio.google.com` |
| accounts.google.com | 326 | 3.6% | auth_flow | web_visit (web) | 43.8 | n/a | `https://accounts.google.com/v3/signin/accountchooser?client_id=468316177930-dkmnhti8obocc4r8o11vn478j9qqdglt.apps.goo...` |
| google.com | 266 | 2.9% | search | web_visit (web) | 314.1 | n/a | `https://google.com/chrome/?brand=GGRF&utm_source=google.com&utm_medium=material-callout&utm_campaign=cws&utm_keyword=...` |
| youtube.com | 174 | 1.9% | video_review | web_visit (web) | 390.0 | n/a | `https://youtube.com/` |
| console.cloud.google.com | 165 | 1.8% | page_visit | web_visit (web) | 17.0 | n/a | `https://console.cloud.google.com/security/recaptcha?authuser=1&project=gettuppent-production&hl=en-US` |
| vercel.com | 141 | 1.5% | page_visit | web_visit (web) | 20.1 | n/a | `https://vercel.com/` |
| grok.com | 139 | 1.5% | page_visit | web_visit (web) | 69.5 | n/a | `https://grok.com/` |

### Query Loop Fatigue (medium | impact 4848.3)

* Evidence: 705 loops >=4 steps, 6074 total query/framing events
* Fix: After two query rounds, require a one-sentence decision or next action.
* Sampled events: 6074
| Destination/Stage | Hits | Share | Intent | Next Step | Avg Next Gap (s) | App Tool | Example Target |
| :--- | ---: | ---: | :--- | :--- | ---: | :--- | :--- |
| framing | 2456 | 40.4% | stage_framing | time_range (framing) | 2.2 | n/a | `2025-12-03T06:00:00Z → 2025-12-04T00:14:28.708964Z` |
| query | 2043 | 33.6% | stage_query | hint_suggested_query (query) | 4.9 | n/a | `How does MCP abstract raw data?` |
| google.com | 63 | 1.0% | context_query | hint_suggested_query (query) | 0.0 | n/a | `https://www.google.com/search?q=f` |
| accounts.google.com | 52 | 0.9% | context_query | hint_suggested_query (query) | 1.3 | n/a | `https://accounts.google.com/o/oauth2/auth` |
| aistudio.google.com | 44 | 0.7% | context_query | hint_suggested_query (query) | 0.0 | n/a | `https://aistudio.google.com/u/2/prompts/1eyY62WdyEUobpFDyMgSGwMkUVyaAziCm` |
| chatgpt.com | 41 | 0.7% | context_query | hint_suggested_query (query) | 3.6 | n/a | `https://chatgpt.com/c/Workspace%20Rules%20and%20Automations#loc_0_24_ago_2025-12-23_06_45_05_Tuesday_December_23_2025_4` |
| daily-cloudcode-pa.googleapis.com | 38 | 0.6% | context_query | hint_suggested_query (query) | 0.0 | n/a | `https://daily-cloudcode-pa.googleapis.com/vlinternal:streamGenerateContent?alt=sse%60` |
| notebooklm.google.com | 30 | 0.5% | context_query | hint_suggested_query (query) | 18.0 | n/a | `https://notebooklm.google.com/notebook/ffbb` |


## Premium Workflows

### Premium Workflow 01: Intent Command Sprint

* **Process**: n/a
* **Why it fits**: n/a
* Trigger: Start of deep-work block
* Base marker: `framing -> synthesis`
* KPI: Research-to-synthesis closure rate >= 70%
  1. Open command center and define one measurable outcome.
  2. Run two focused research hops max before forcing a query decision.
  3. Compress findings into workstream summary plus annotation notes.
  4. Hand off to code/file action within five minutes.
  5. Close with a one-line execution checkpoint.

### Premium Workflow 02: Query Ladder to Artifact Forge

* **Process**: n/a
* **Why it fits**: n/a
* Trigger: Any high-ambiguity problem
* Base marker: `query -> framing -> synthesis`
* KPI: Decision latency from query to synthesis <= 90 seconds median
  1. Issue one evidence-seeking query (not open-ended browsing).
  2. Pin a time range or scope boundary immediately.
  3. Write one decisive synthesis statement.
  4. Transform synthesis into artifact: code, schema, or deliverable note.
  5. Capture counterfactual: what would invalidate this decision?

### Premium Workflow 03: Anti-Drift Recovery Loop

* **Process**: n/a
* **Why it fits**: n/a
* Trigger: When Browser Drift signal appears
* Base marker: `web -> web (drift interrupt)`
* KPI: Browser drift episodes reduced by 40% over 14 days
  1. Detect drift trigger (3+ consecutive web actions or looping queries).
  2. Pause exploration and run a 60-second framing checkpoint.
  3. Force a workstream summary before resuming exploration.
  4. Either commit to build mode or intentionally exit the loop.
  5. Log the interruption source to improve future guardrails.
