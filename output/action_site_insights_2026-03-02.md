# Action x Site Insights

Generated: 2026-03-03 04:35 UTC

* Events analyzed: 28013
* Distinct actions: 14
* Distinct inferred sites: 1834

## Top Actions

| Action | Events | Primary Site | Primary Share |
| :--- | ---: | :--- | ---: |
| web_visit | 12428 | unknown | 9.2% |
| hint_suggested_query | 3680 | unknown | 92.7% |
| time_range | 3356 | unknown | 100.0% |
| activity | 2132 | unknown | 100.0% |
| file_reference | 2090 | unknown | 96.8% |
| annotation_summary | 1335 | unknown | 70.9% |
| annotation_description | 1281 | unknown | 66.6% |
| workstream_summary | 1218 | unknown | 100.0% |
| conversation_activity | 177 | unknown | 100.0% |
| code_snippet | 93 | unknown | 98.9% |
| conversation | 77 | unknown | 92.2% |
| annotation_hierarchical_profile_summary | 56 | unknown | 100.0% |
| annotation_explanation | 46 | unknown | 93.5% |
| annotation_comment | 44 | unknown | 100.0% |

## Top Sites

| Site | Events | Active Days | Dominant Action | Dominant Share |
| :--- | ---: | ---: | :--- | ---: |
| unknown | 15562 | 58 | hint_suggested_query | 21.9% |
| notion.so | 1109 | 35 | web_visit | 100.0% |
| google | 983 | 42 | web_visit | 82.3% |
| notion | 868 | 38 | web_visit | 43.3% |
| github.com | 663 | 46 | web_visit | 99.7% |
| accounts.google.com | 367 | 38 | web_visit | 100.0% |
| github | 355 | 36 | web_visit | 62.0% |
| stripe | 318 | 32 | annotation_description | 27.0% |
| google.com | 294 | 43 | web_visit | 100.0% |
| youtube.com | 185 | 29 | web_visit | 100.0% |
| console.cloud.google.com | 170 | 8 | web_visit | 100.0% |
| notebooklm.google.com | 148 | 25 | web_visit | 100.0% |
| vercel.com | 145 | 15 | web_visit | 100.0% |
| grok.com | 139 | 13 | web_visit | 100.0% |
| chatgpt.com | 131 | 20 | web_visit | 100.0% |
| aistudio.google.com | 125 | 31 | web_visit | 100.0% |
| console.firebase.google.com | 118 | 11 | web_visit | 100.0% |
| coolmathgames.com | 114 | 6 | web_visit | 100.0% |
| gemini.google.com | 102 | 36 | web_visit | 100.0% |
| localhost:39300 | 98 | 14 | web_visit | 92.9% |

## Current Problems

### Missing Site Attribution (medium | impact 55.6)

* Evidence: unknown site inferred for 15562/28013 events (55.6%)
* Fix: Improve target/metadata capture so intent and destination are attributable.

### Query Attribution Blind Spot (high | impact 92.7)

* Evidence: hint_suggested_query is unknown for 3412/3680 events (92.7%)
* Fix: Attach query intent tags and source destination to each query event.

## Action -> Top 5 Sites

### web_visit (12428)

* unknown: 1138 (9.2%)
* notion.so: 1109 (8.9%)
* google: 809 (6.5%)
* github.com: 661 (5.3%)
* notion: 376 (3.0%)

### hint_suggested_query (3680)

* unknown: 3412 (92.7%)
* notion: 98 (2.7%)
* stripe: 51 (1.4%)
* google: 34 (0.9%)
* github: 28 (0.8%)

### time_range (3356)

* unknown: 3356 (100.0%)

### activity (2132)

* unknown: 2132 (100.0%)

### file_reference (2090)

* unknown: 2024 (96.8%)
* stripe: 24 (1.1%)
* notion: 16 (0.8%)
* slack: 8 (0.4%)
* local: 7 (0.3%)

### annotation_summary (1335)

* unknown: 946 (70.9%)
* notion: 176 (13.2%)
* stripe: 81 (6.1%)
* google: 65 (4.9%)
* github: 40 (3.0%)

### annotation_description (1281)

* unknown: 853 (66.6%)
* notion: 197 (15.4%)
* stripe: 86 (6.7%)
* google: 74 (5.8%)
* github: 60 (4.7%)

### workstream_summary (1218)

* unknown: 1218 (100.0%)

### conversation_activity (177)

* unknown: 177 (100.0%)

### code_snippet (93)

* unknown: 92 (98.9%)
* stripe: 1 (1.1%)

## Site -> Top 5 Actions

### unknown (15562)

* hint_suggested_query: 3412 (21.9%)
* time_range: 3356 (21.6%)
* activity: 2132 (13.7%)
* file_reference: 2024 (13.0%)
* workstream_summary: 1218 (7.8%)

### notion.so (1109)

* web_visit: 1109 (100.0%)

### google (983)

* web_visit: 809 (82.3%)
* annotation_description: 74 (7.5%)
* annotation_summary: 65 (6.6%)
* hint_suggested_query: 34 (3.5%)
* conversation: 1 (0.1%)

### notion (868)

* web_visit: 376 (43.3%)
* annotation_description: 197 (22.7%)
* annotation_summary: 176 (20.3%)
* hint_suggested_query: 98 (11.3%)
* file_reference: 16 (1.8%)

### github.com (663)

* web_visit: 661 (99.7%)
* annotation_summary: 2 (0.3%)

### accounts.google.com (367)

* web_visit: 367 (100.0%)

### github (355)

* web_visit: 220 (62.0%)
* annotation_description: 60 (16.9%)
* annotation_summary: 40 (11.3%)
* hint_suggested_query: 28 (7.9%)
* file_reference: 5 (1.4%)

### stripe (318)

* annotation_description: 86 (27.0%)
* annotation_summary: 81 (25.5%)
* web_visit: 74 (23.3%)
* hint_suggested_query: 51 (16.0%)
* file_reference: 24 (7.5%)

### google.com (294)

* web_visit: 294 (100.0%)

### youtube.com (185)

* web_visit: 185 (100.0%)

### console.cloud.google.com (170)

* web_visit: 170 (100.0%)

### notebooklm.google.com (148)

* web_visit: 148 (100.0%)
