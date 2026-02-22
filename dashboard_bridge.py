import duckdb
import json
import os
from pathlib import Path
from typing import Optional
from datetime import datetime
from insights import get_insight_prompts

def run(db_path: Path, *, con: Optional[duckdb.DuckDBPyConnection] = None) -> str:
    """Wrapper for export_dashboard_data supporting shared connection."""
    return export_dashboard_data(db_path, con=con)

def export_dashboard_data(db_path: Path, *, con: Optional[duckdb.DuckDBPyConnection] = None) -> str:
    """Exports all intelligence for the Dashboard UI."""
    close_con = False
    if con is None:
        con = duckdb.connect(str(db_path), read_only=True)
        close_con = True
    
    try:
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
        
        output_path = Path("output/dashboard_data.json")
        output_path.parent.mkdir(exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"Dashboard data exported to {output_path}")
        return str(output_path)
    finally:
        if close_con:
            con.close()

if __name__ == "__main__":
    from pathlib import Path
    DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"
    export_dashboard_data(DB_PATH)
