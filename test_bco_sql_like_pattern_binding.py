"""Regression checks for parameterized SQL LIKE patterns in BCO/Postgres."""
import sqlite3
import unittest
from pathlib import Path


class LikePatternBindingTests(unittest.TestCase):
    def test_app_binds_wildcard_patterns_instead_of_sql_literals(self):
        source = Path(__file__).with_name("app.py").read_text(encoding="utf-8")
        self.assertIn("status LIKE ?", source)
        self.assertIn("error LIKE ? OR UPPER(COALESCE(error,'')) LIKE ?", source)
        self.assertIn("trigger_reason LIKE ?", source)
        self.assertNotIn("status LIKE 'ARMED%'", source)
        self.assertNotIn("error LIKE '%429%'", source)
        self.assertNotIn("trigger_reason LIKE '%SHORT_RESEARCH_%'", source)

    def test_bound_patterns_keep_retry_and_research_matching(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("CREATE TABLE observer (error TEXT, trigger_reason TEXT)")
            conn.executemany("INSERT INTO observer VALUES (?,?)", [
                ("HTTP 429 rate limit", "SHORT_RESEARCH_CANDIDATE_ON"),
                ("too many requests", "OTHER"),
                ("internal error", "SHORT_RESEARCH_STATE_CHANGE"),
                ("internal error", "OTHER"),
            ])
            retry_rows = conn.execute(
                "SELECT error FROM observer WHERE "
                "(error LIKE ? OR UPPER(COALESCE(error,'')) LIKE ?)",
                ("%429%", "%TOO MANY REQUESTS%"),
            ).fetchall()
            research_rows = conn.execute(
                "SELECT trigger_reason FROM observer WHERE trigger_reason LIKE ?",
                ("%SHORT_RESEARCH_%",),
            ).fetchall()
        self.assertEqual(len(retry_rows), 2)
        self.assertEqual(len(research_rows), 2)


if __name__ == "__main__":
    unittest.main()
