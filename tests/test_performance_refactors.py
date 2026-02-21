import tempfile
import unittest
from pathlib import Path

import duckdb

import architect
import insights
import workflows


class TestArchitectConnect(unittest.TestCase):
    def test_connect_uses_runtime_db_path_when_not_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_db = Path(tmp_dir) / "runtime.duckdb"
            original_db_path = architect.DB_PATH
            try:
                architect.DB_PATH = tmp_db
                con = architect._connect(read_only=False)
                try:
                    db_file = con.execute("PRAGMA database_list").fetchone()[2]
                finally:
                    con.close()
            finally:
                architect.DB_PATH = original_db_path

            self.assertEqual(Path(db_file).resolve(), tmp_db.resolve())


class TestWorkflowsRunConnectionReuse(unittest.TestCase):
    def test_run_accepts_existing_connection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "wf.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    CREATE TABLE events (
                        event_id VARCHAR,
                        ts_start TIMESTAMP,
                        action VARCHAR
                    );
                    """
                )
                con.execute(
                    """
                    CREATE TABLE event_projects (
                        event_id VARCHAR,
                        project_id INTEGER
                    );
                    """
                )
                con.execute(
                    """
                    CREATE TABLE projects (
                        project_id INTEGER,
                        name VARCHAR
                    );
                    """
                )

                con.execute(
                    """
                    INSERT INTO events VALUES
                    ('e1', TIMESTAMP '2026-01-01 10:00:00', 'coding'),
                    ('e2', TIMESTAMP '2026-01-01 10:05:00', 'coding'),
                    ('e3', TIMESTAMP '2026-01-01 11:00:00', 'testing');
                    """
                )
                con.execute(
                    """
                    INSERT INTO projects VALUES
                    (1, 'IronClad'),
                    (2, 'GettUpp');
                    """
                )
                con.execute(
                    """
                    INSERT INTO event_projects VALUES
                    ('e1', 1),
                    ('e2', 1),
                    ('e3', 2);
                    """
                )

                result = workflows.run(db_path, gap_minutes=30, con=con)
                self.assertTrue(result)
                self.assertGreater(con.execute("SELECT COUNT(*) FROM sessions").fetchone()[0], 0)
                self.assertEqual(con.execute("SELECT 1").fetchone()[0], 1)
            finally:
                con.close()


class TestInsightsConnectionReuse(unittest.TestCase):
    def test_get_insight_prompts_accepts_existing_connection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "insights.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    CREATE TABLE daily_scores (
                        date DATE,
                        health_score DOUBLE,
                        focus_score DOUBLE
                    );
                    """
                )
                con.execute(
                    """
                    CREATE TABLE event_subcategories (
                        event_id VARCHAR,
                        theme VARCHAR,
                        subcategory VARCHAR
                    );
                    """
                )
                con.execute(
                    """
                    INSERT INTO daily_scores VALUES
                    (DATE '2026-01-01', 5.0, 5.0);
                    """
                )
                con.execute(
                    """
                    INSERT INTO event_subcategories VALUES
                    ('e1', 'IronClad', 'Offer'),
                    ('e2', 'IronClad', 'Audience'),
                    ('e3', 'AI Arbitrage', 'Prompts');
                    """
                )

                prompts = insights.get_insight_prompts(db_path=db_path, con=con)
                self.assertIn("Priority Actions", prompts)
                self.assertEqual(con.execute("SELECT 1").fetchone()[0], 1)
            finally:
                con.close()


class TestEvolvingInsights(unittest.TestCase):
    def test_generate_evolving_insights_outputs_action_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "evolving.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    CREATE TABLE events (
                        event_id VARCHAR,
                        ts_start TIMESTAMP,
                        action VARCHAR
                    );
                    """
                )
                con.execute(
                    """
                    CREATE TABLE daily_scores (
                        date DATE,
                        health_score DOUBLE,
                        focus_score DOUBLE,
                        composite_score DOUBLE
                    );
                    """
                )
                con.execute(
                    """
                    CREATE TABLE event_subcategories (
                        event_id VARCHAR,
                        theme VARCHAR,
                        subcategory VARCHAR
                    );
                    """
                )
                con.execute(
                    """
                    INSERT INTO events VALUES
                    ('e1', TIMESTAMP '2026-01-01 10:00:00', 'a'),
                    ('e2', TIMESTAMP '2026-01-02 10:00:00', 'b'),
                    ('e3', TIMESTAMP '2026-01-03 10:00:00', 'c');
                    """
                )
                con.execute(
                    """
                    INSERT INTO daily_scores VALUES
                    (DATE '2026-01-01', 8.0, 8.0, 7.5),
                    (DATE '2026-01-02', 8.0, 8.0, 7.0),
                    (DATE '2026-01-03', 8.0, 8.0, 6.5);
                    """
                )
                con.execute(
                    """
                    INSERT INTO event_subcategories VALUES
                    ('e1', 'IronClad', 'Offer'),
                    ('e2', 'IronClad', 'Audience'),
                    ('e3', 'AI Arbitrage', 'Prompts');
                    """
                )

                payload = insights.generate_evolving_insights(db_path=db_path, con=con)
                self.assertIn("insights", payload)
                self.assertIn("action_queue", payload)
                self.assertIsInstance(payload["action_queue"], list)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
