import duckdb
import json
import os
from datetime import datetime
from insights import get_insight_prompts

def export_dashboard_data():
    """Exports all intelligence for the Dashboard UI."""
    con = duckdb.connect('me_ops.duckdb', read_only=True)
    
    data = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "user": "Johnny Cage",
            "system": "ME-OPS v1.0"
        },
        "scores": con.execute("SELECT * FROM daily_scores ORDER BY date DESC LIMIT 14").fetchdf().to_dict(orient="records"),
        "workflows": con.execute("SELECT * FROM discovered_workflows").fetchdf().to_dict(orient="records"),
        "insights": get_insight_prompts()
    }
    
    output_path = "output/dashboard_data.json"
    os.makedirs("output", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    
    con.close()
    print(f"Dashboard data exported to {output_path}")

if __name__ == "__main__":
    export_dashboard_data()
