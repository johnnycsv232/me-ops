#!/usr/bin/env python3
"""ME-OPS Master Runner — unified, efficient intelligence pipeline.

Executes the full chain (Ingest -> Workflows -> Architect -> Workflow DNA -> Insights)
with shared connections and optimized concurrency.

Usage:
    python master.py [--ai] [--refresh]
"""
import sys
import time
import json
import argparse
from pathlib import Path
from datetime import datetime
import duckdb

# Add current dir to path to import modules
sys.path.append(str(Path(__file__).parent))

import workflows
import mistakes
import architect
import insights
import workflow_dna

def main():
    parser = argparse.ArgumentParser(description="ME-OPS Master Runner")
    parser.add_argument("--ai", action="store_true", help="Include Gemini coaching")
    parser.add_argument("--refresh", action="store_true", help="Force refresh data")
    args = parser.parse_args()

    t00 = time.time()
    print("ME-OPS MASTER INTELLIGENCE PIPELINE")
    print("=" * 60)

    root_dir = Path(__file__).resolve().parent
    db_path = root_dir / "me_ops.duckdb"
    data_dir = root_dir.parent

    # 1. Ingest
    print(f"\n[1/6] INGESTION: Processing {data_dir}...")
    if args.refresh:
        import ingest
        ingest.run(data_dir, db_path)
    else:
        print("    (Skipping full ingest; use --refresh to rebuild)")

    con = duckdb.connect(str(db_path))
    try:
        # 2. Workflows
        print(f"\n[2/6] WORKFLOWS: Mining sessions & patterns...")
        workflows.run(db_path, con=con)

        # 3. Mistakes
        print(f"\n[3/6] MISTAKES: Detecting failure patterns & thrashing...")
        mistakes.run(db_path, con=con)

        # 4. Architect
        print(f"\n[4/6] ARCHITECT: Scoring & Coaching...")
        # We'll run the individual phases to avoid the main() overhead
        architect._init_schema(con)
        wfs = architect.extract_workflows(con)
        rules = architect.generate_coaching_rules(con)
        scores = architect.compute_daily_scores(con)
        improvement = architect.track_improvement(con, rules)
        architect.write_to_db(con, wfs, rules, scores, improvement)

        report = architect.generate_report(wfs, rules, scores, improvement)

        if args.ai:
            print("\n  Generating AI Coaching Narrative...")
            ai_narrative = architect.generate_ai_coaching(wfs, rules, scores)
            if ai_narrative:
                report = f"## AI COACHING NARRATIVE\n\n{ai_narrative}\n\n---\n\n{report}"

        # 5. Workflow DNA
        print(f"\n[5/6] WORKFLOW DNA: Extracting process genetics & premium upgrades...")
        dna_payload = workflow_dna.run(
            db_path=db_path,
            con=con,
            output_dir=root_dir / "output",
        )

        dna_style = dna_payload.get("style_profile", {}).get("unique_style", [])
        dna_markers = dna_payload.get("genetic_markers", [])
        dna_bottlenecks = dna_payload.get("bottlenecks", [])
        dna_workflows = dna_payload.get("premium_workflows", [])
        report_lines = [report, "", "---", "## Workflow DNA Snapshot"]
        if dna_style:
            report_lines.append(f"- Unique style: {', '.join(dna_style[:4])}")
        if dna_markers:
            top_marker = dna_markers[0]
            report_lines.append(
                "- Top marker: "
                f"{top_marker['name']} ({top_marker['transition']}, strength {top_marker['strength']})"
            )
        if dna_bottlenecks:
            top_bottleneck = dna_bottlenecks[0]
            report_lines.append(
                f"- Primary bottleneck: {top_bottleneck['name']} "
                f"(impact {top_bottleneck['impact_score']})"
            )
        if dna_workflows:
            report_lines.append("- Premium workflow set:")
            for wf in dna_workflows[:3]:
                report_lines.append(f"  - {wf['name']}")
        report = "\n".join(report_lines)

        # 6. Insights
        print(f"\n[6/6] INSIGHTS: Generating evolving connections...")
        insight_payload = insights.generate_evolving_insights(db_path, con=con)
        insight_md = insights.render_evolving_insights_markdown(insight_payload)

        # Fold top actions into the main report for immediate usability.
        action_queue = insight_payload.get("action_queue", [])
        if action_queue:
            report_lines = [report, "", "---", "## Insight Action Queue"]
            for item in action_queue[:5]:
                report_lines.append(
                    f"{item['priority']}. {item['action']} "
                    f"(impact: {item['impact_score']}, conf: {item['confidence']:.2f}, novelty: {item['novelty']})"
                )
            report = "\n".join(report_lines)
    finally:
        con.close()
    
    # Final Output
    print("\n" + "=" * 60)
    print("MASTER REPORT GENERATED")
    print("=" * 60)
    
    today = datetime.now().strftime("%Y-%m-%d")
    out_file = Path("output") / f"master_report_{today}.md"
    out_file.parent.mkdir(exist_ok=True)
    out_file.write_text(report, encoding="utf-8")

    insight_json_file = Path("output") / f"insights_{today}.json"
    insight_md_file = Path("output") / f"insights_{today}.md"
    insight_json_file.write_text(json.dumps(insight_payload, indent=2), encoding="utf-8")
    insight_md_file.write_text(insight_md, encoding="utf-8")
    
    print(f"\nReport saved to: {out_file}")
    print(f"Insights JSON saved to: {insight_json_file}")
    print(f"Insights markdown saved to: {insight_md_file}")
    dna_report_file = Path("output") / "WORKFLOW_DNA_REPORT.md"
    print(f"Workflow DNA report saved to: {dna_report_file}")
    print(f"Total time elapsed: {time.time() - t00:.1f}s")

if __name__ == "__main__":
    main()
