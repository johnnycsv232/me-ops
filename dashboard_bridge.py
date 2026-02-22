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
        def fetch_as_dicts(connection, query):
            res = connection.execute(query)
            cols = [desc[0] for desc in res.description]
            return [dict(zip(cols, row)) for row in res.fetchall()]

        data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "user": "Johnny Cage",
                "system": "ME-OPS v1.0"
            },
            "scores": fetch_as_dicts(con, "SELECT * FROM daily_scores ORDER BY date DESC LIMIT 14"),
            "workflows": fetch_as_dicts(con, "SELECT * FROM discovered_workflows"),
            "insights": get_insight_prompts()
        }
        
        output_path = Path("output/dashboard_data.json")
        output_path.parent.mkdir(exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"Dashboard data exported to {output_path}")
        return str(output_path)
    except Exception as e:
        print(f"Error exporting dashboard data: {e}")
        return ""
    finally:
        if close_con:
            con.close()

if __name__ == "__main__":
    from pathlib import Path
    DB_PATH = Path(__file__).resolve().parent / "me_ops.duckdb"
    export_dashboard_data(DB_PATH)
