#!/usr/bin/env python3
"""ME-OPS Master Runner — unified, efficient intelligence pipeline.

Executes the full chain
(Ingest -> Workflows -> Architect -> Workflow DNA -> Insights -> Action x Site)
with shared connections and optimized concurrency.

Usage:
    python master.py [--ai] [--refresh]
"""
import sys
import time
import json
import os
import argparse
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Tuple
import duckdb
from concurrent.futures import ThreadPoolExecutor, as_completed
import cProfile
import pstats
from pstats import SortKey

# Add current dir to path to import modules
sys.path.append(str(Path(__file__).parent))

import workflows
import mistakes
import architect
import insights
import workflow_dna
import action_site_insights
import warehouse
import entities
import graph
import queries
import cluster
import deep_analysis
import predict
import vectors
import daily_report
import briefing
import dashboard_bridge
import foundation_guard


def _normalize_current_problems(raw: object, max_items: int = 3) -> list[dict[str, Any]]:
    """Normalize current_problems payload into a bounded list of dict rows."""
    if not isinstance(raw, list):
        return []

    rows: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            rows.append(item)
        if len(rows) >= max_items:
            break
    return rows


def _append_current_problems_snapshot(
    report_lines: list[str],
    problems: list[dict[str, Any]],
    heading: str,
    include_hotspot: bool,
) -> None:
    """Append a compact current_problems section to report lines."""
    if not problems:
        return

    report_lines.append("")
    report_lines.append(heading)
    report_lines.append("")
    for problem in problems:
        report_lines.append(
            f"* {problem.get('name')} ({problem.get('severity')}, impact {problem.get('impact_score')})"
        )
        report_lines.append(f"  * Evidence: {problem.get('evidence')}")
        report_lines.append(f"  * Fix: {problem.get('recommendation')}")
        if include_hotspot:
            rows = problem.get("activity_breakdown", [])
            if isinstance(rows, list) and rows:
                first_row = rows[0]
                if isinstance(first_row, dict):
                    report_lines.append(
                        f"  * Hotspot: {first_row.get('destination')} -> {first_row.get('top_next_step')}"
                    )


def main():
    root_dir = Path(__file__).resolve().parent
    default_db = Path(
        os.getenv("ME_OPS_DB_PATH", str(root_dir / "me_ops.duckdb"))
    ).expanduser().resolve()
    default_data_dir = root_dir.parent

    parser = argparse.ArgumentParser(description="ME-OPS Master Runner")
    parser.add_argument("--ai", action="store_true", help="Include Gemini coaching")
    parser.add_argument("--refresh", action="store_true", help="Force refresh data")
    parser.add_argument("--deep", action="store_true", help="Run computationally intensive analytics")
    parser.add_argument(
        "--db",
        type=Path,
        default=default_db,
        help="DuckDB file path (default: $ME_OPS_DB_PATH or ./me_ops.duckdb)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=default_data_dir,
        help="Directory containing Pieces JSON exports",
    )
    parser.add_argument(
        "--report-date",
        type=str,
        default=None,
        help="Override report date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Enable PhD-level cProfile execution to identify bottlenecks",
    )
    args = parser.parse_args()

    if args.report_date:
        try:
            datetime.strptime(args.report_date, "%Y-%m-%d")
        except ValueError:
            print("Invalid --report-date format. Expected YYYY-MM-DD.")
            return 1
    report_date = args.report_date or datetime.now().strftime("%Y-%m-%d")

    t00 = time.time()
    print("ME-OPS MASTER INTELLIGENCE PIPELINE")
    print("=" * 60)

    db_path = args.db.expanduser().resolve()
    data_dir = args.data_dir.expanduser().resolve()

    # 0. Foundation Guard (Pre-flight)
    print("\n[0/18] FOUNDATION: Running environment audit...")
    if not foundation_guard.run(db_path.parent):
        print("❌ Foundation audit failed. Please fix environment issues.")
        return 1

    def execute_pipeline() -> int:
        print(f"\n[1/18] INGESTION: Processing {data_dir}...")
        if args.refresh:
            import ingest
            ingest.run(data_dir, db_path)
        else:
            print("    (Skipping full ingest; use --refresh to rebuild)")

        # --- Advanced Database Tuning (DuckDB) ---
        print("\n[DB TUNING] Applying PhD-level database optimizations...")
        # These PRAGMAs optimize memory and thread allocation for heavy analytical workloads
        con = duckdb.connect(str(db_path))
        con.execute("PRAGMA threads=8;")  # Adjust to logical cores if necessary
        con.execute("PRAGMA memory_limit='8GB';") # Safe bounded memory for execution
        con.execute("PRAGMA checkpoint_threshold='256MB';")

        try:
            # 2. Workflows
            print("\n[2/18] WORKFLOWS: Mining sessions & patterns...")
            workflows.run(db_path, con=con)

            # 3. Mistakes
            print("\n[3/18] MISTAKES: Detecting failure patterns & thrashing...")
            mistakes.run(db_path, con=con)

            # 4. Architect
            print("\n[4/18] ARCHITECT: Scoring & Coaching...")
            warehouse.init_warehouse(con)  # Ensure intelligence tables exist
            # We'll run the individual phases to avoid the main() overhead
            architect._init_schema(con)
            wfs = architect.extract_workflows(con)
            rules = architect.generate_coaching_rules(con)
            scores = architect.compute_daily_scores(con)
            improvement = architect.track_improvement(con, rules)
            architect.write_to_db(con, wfs, rules, scores, improvement)

            report = architect.generate_report(wfs, rules, scores, improvement)

            # 5. Entities
            print("\n[5/18] ENTITIES: Extracting cross-references...")
            entities.run(db_path, con=con)

            # 6. Graph
            print("\n[6/18] GRAPH: Building relationship network...")
            graph.run(db_path, con=con)

            # 7. Queries (Validation)
            print("\n[7/18] VALIDATION: Running integrity checks...")
            queries.run(db_path, con=con)

            # --- Advanced Analytics (Gated by --deep) ---
            if args.deep:
                # 8. Cluster
                print("\n[8/18] CLUSTER: Segmenting sessions...")
                cluster.run(db_path, con=con)

                # 9. Deep Analysis
                print("\n[9/18] DEEP ANALYSIS: Uncovering hidden patterns...")
                deep_res = deep_analysis.run(db_path, con=con, ai=args.ai)
                # Fold deep analysis into report
                report += f"\n\n---\n\n## Deep Analysis Blueprint\n\n{deep_res['report']}"

                # 10. Predict
                print("\n[10/18] PREDICT: Training behavior models...")
                predict.run(db_path, con=con)

                # 11. Vectors
                print("\n[11/18] VECTORS: Updating semantic index...")
                vectors.run(db_path, con=con, do_index=True)
            else:
                print("\n[8-11/18] ADVANCED ANALYTICS: Skipping (use --deep to enable)")

            # 12. AI Narrative
            ai_narrative = None
            if args.ai:
                print("\n[12/18] AI COACHING: Generating narrative...")
                ai_narrative = architect.generate_ai_coaching(wfs, rules, scores)
                if ai_narrative:
                    # Ensure H1 stays at the top. Insert AI narrative after the title.
                    parts = report.split("\n", 2)
                    if len(parts) >= 2 and parts[0].startswith("# "):
                        title = parts[0]
                        content = parts[2] if len(parts) > 2 else ""
                        report = f"{title}\n\n## AI COACHING NARRATIVE\n\n{ai_narrative}\n\n---\n\n{content}"
                    else:
                        report = f"## AI COACHING NARRATIVE\n\n{ai_narrative}\n\n---\n\n{report}"

            # 13-18. Concurrent Read-Only Analytical Execution
            print("\n[13-18/18] CONCURRENT ANALYTICS: Launching read-only engines...")
            
            # We define thread-safe helper functions for each read-only analytical phase
            def run_workflow_dna() -> Dict[str, Any]:
                return workflow_dna.run(db_path=db_path, con=con.cursor(), output_dir=root_dir / "output")

            def run_insights() -> Tuple[Dict[str, Any], str]:
                payload = insights.generate_evolving_insights(db_path, con=con.cursor())
                md = insights.render_evolving_insights_markdown(payload)
                return payload, md

            def run_action_site() -> Dict[str, Any]:
                return action_site_insights.run(db_path=db_path, con=con.cursor(), output_dir=root_dir / "output", report_date=report_date)

            def run_daily_report() -> None:
                daily_report.run(db_path, con=con.cursor(), report_date=report_date)

            def run_briefing() -> None:
                briefing.run(db_path, con=con.cursor(), target_date=report_date, no_ai=(not args.ai))

            def run_dashboard_bridge() -> None:
                dashboard_bridge.run(db_path, con=con.cursor())

            # Execute concurrent tasks
            with ThreadPoolExecutor(max_workers=6) as executor:
                future_dna = executor.submit(run_workflow_dna) # type: ignore
                future_insights = executor.submit(run_insights) # type: ignore
                future_action_site = executor.submit(run_action_site) # type: ignore
                # These three are primarily fire-and-forget or produce their own files
                executor.submit(run_daily_report) # type: ignore
                executor.submit(run_briefing) # type: ignore
                executor.submit(run_dashboard_bridge) # type: ignore

                # Await targeted payloads for the master report
                dna_payload = future_dna.result()
                insight_payload, insight_md = future_insights.result()
                action_site_payload = future_action_site.result()

            # Compose Workflow DNA section
            print(" -> Aggregating Workflow DNA...")
            dna_style = dna_payload.get("style_profile", {}).get("unique_style", [])
            dna_markers = dna_payload.get("genetic_markers", [])
            dna_bottlenecks = dna_payload.get("bottlenecks", [])
            dna_current_problems = _normalize_current_problems(
                dna_payload.get("current_problems")
            )
            dna_workflows = dna_payload.get("premium_workflows", [])
            report_lines = [report.strip(), "", "---", "", "## Workflow DNA Snapshot", ""]
            if dna_style:
                report_lines.append(f"* Unique style: {', '.join(dna_style[:4])}")
            if dna_markers:
                top_marker = dna_markers[0]
                report_lines.append(
                    "* Top marker: "
                    f"{top_marker['name']} ({top_marker['transition']}, strength {top_marker['strength']})"
                )
            if dna_bottlenecks:
                top_bottleneck = dna_bottlenecks[0]
                report_lines.append(
                    f"* Primary bottleneck: {top_bottleneck['name']} "
                    f"(impact {top_bottleneck['impact_score']})"
                )
            if dna_workflows:
                report_lines.append("* Premium workflow set:")
                for wf in dna_workflows[:3]:
                    report_lines.append(f"  * {wf['name']}")
            _append_current_problems_snapshot(
                report_lines=report_lines,
                problems=dna_current_problems,
                heading="### Current Problems",
                include_hotspot=True,
            )
            report = "\n".join(report_lines)

            # Compose Insights section
            print(" -> Aggregating Insights...")
            action_queue = insight_payload.get("action_queue", [])
            if action_queue:
                # Ensure blank lines around the separator and section title
                insight_lines = [report.strip(), "", "---", "", "## Insight Action Queue", ""]
                for item in action_queue[:5]:
                    insight_lines.append(
                        f"{item['priority']}. {item['action']} "
                        f"(impact: {item['impact_score']}, conf: {item['confidence']:.2f}, novelty: {item['novelty']})"
                    )
                report = "\n".join(insight_lines).strip() + "\n" # Ensure report ends with a single newline

            # Compose Action x Site section
            print(" -> Aggregating Action x Site Insights...")
            top_action = action_site_payload.get("top_actions", [])
            top_site = action_site_payload.get("top_sites", [])
            action_site_problems = _normalize_current_problems(
                action_site_payload.get("current_problems")
            )
            # Ensure blank lines around the separator and section title
            report_lines = [report.strip(), "", "---", "", "## Action x Site Snapshot", ""]
            if top_action:
                report_lines.append(
                    f"* Top action: {top_action[0][0]} ({top_action[0][1]} events)"
                )
            if top_site:
                report_lines.append(
                    f"* Top inferred site: {top_site[0][0]} ({top_site[0][1]} events)"
                )
            if action_site_problems:
                report_lines.append("* Current problems:")
                for problem in action_site_problems:
                    report_lines.append(
                        f"  * {problem.get('name')} ({problem.get('severity')}, impact {problem.get('impact_score')})"
                    )
            report_lines.append(
                f"* Full report: `{action_site_payload.get('report_path', 'output/action_site_insights_*.md')}`"
            )
            report = "\n".join(report_lines).strip() + "\n" # Ensure report ends with a single newline

            # 8. Warehouse Persistence
            print("\n[WAREHOUSE] Storing integrated intelligence...")
            
            # Prepare payloads for warehouse
            # Only persist a briefing if AI narrative was actually generated
            briefing_data = None
            if ai_narrative:
                briefing_data = {
                    "narrative_text": ai_narrative,
                    "model_id": "gemini-2.0-flash",
                }

            warehouse_payload: Dict[str, Any] = {
                "briefings": briefing_data,
                "site_intelligence": [],
                "bottlenecks": [],
                "action_queue": [],
            }

            # Site intelligence from action_site_insights
            site_intel_rows: list[Dict[str, Any]] = []
            action_site_dict = action_site_payload.get("action_site", {})
            for (act, site), count in action_site_dict.items():
                site_intel_rows.append({
                    "site": site,
                    "action": act,
                    "event_count": count,
                    "impact_score": 0.0,
                })
            warehouse_payload["site_intelligence"] = site_intel_rows

            # Bottlenecks from DNA and site problems
            bottleneck_rows: list[Dict[str, Any]] = []
            for b in dna_bottlenecks:
                bottleneck_rows.append({
                    "engine": "workflow_dna",
                    "marker_name": b.get("name", "Unknown"),
                    "impact_score": b.get("impact_score", 0.0),
                    "description": b.get("evidence", ""),
                })
            for p in action_site_problems:
                bottleneck_rows.append({
                    "engine": "action_site",
                    "marker_name": p.get("name", "Unknown"),
                    "impact_score": p.get("impact_score", 0.0),
                    "description": p.get("evidence", ""),
                })
            warehouse_payload["bottlenecks"] = bottleneck_rows

            # Action queue from insights
            queue_rows: list[Dict[str, Any]] = []
            for idx, item in enumerate(action_queue):
                queue_rows.append({
                    "priority": idx + 1,
                    "action_text": item.get("action", ""),
                    "impact_score": item.get("impact_score", 0.0),
                    "source_insight": item.get("explanation", item.get("source_insight", "")),
                })
            warehouse_payload["action_queue"] = queue_rows

            warehouse.persist_intelligence_snapshot(con, report_date, warehouse_payload)
        finally:
            con.close()
        
        # Final Output
        print("\n" + "=" * 60)
        print("MASTER REPORT GENERATED")
        print("=" * 60)
        
        out_file = Path("output") / f"master_report_{report_date}.md"
        out_file.parent.mkdir(exist_ok=True)
        out_file.write_text(report, encoding="utf-8")

        insight_json_file = Path("output") / f"insights_{report_date}.json"
        insight_md_file = Path("output") / f"insights_{report_date}.md"
        insight_json_file.write_text(json.dumps(insight_payload, indent=2), encoding="utf-8")
        insight_md_file.write_text(insight_md, encoding="utf-8")
        
        print(f"\nReport saved to: {out_file}")
        print(f"Insights JSON saved to: {insight_json_file}")
        print(f"Insights markdown saved to: {insight_md_file}")
        dna_report_file = Path("output") / "WORKFLOW_DNA_REPORT.md"
        print(f"Workflow DNA report saved to: {dna_report_file}")
        action_site_file = Path("output") / f"action_site_insights_{report_date}.md"
        print(f"Action x site report saved to: {action_site_file}")
        print(f"Total time elapsed: {time.time() - t00:.1f}s")
        return 0

    if args.profile:
        print("\n[PROFILING] Executing with cProfile...")
        profiler = cProfile.Profile()
        profiler.enable()
        exit_code = execute_pipeline()
        profiler.disable()
        
        prof_file = "master.prof"
        profiler.dump_stats(prof_file)
        print(f"\n[PROFILING] Profile saved to {prof_file}")
        
        print("\n[PROFILING] Top 20 cumulative time bottlenecks:")
        stats = pstats.Stats(profiler)
        stats.sort_stats(SortKey.CUMULATIVE).print_stats(20)
        return exit_code
    else:
        return execute_pipeline()

if __name__ == "__main__":
    raise SystemExit(main())
