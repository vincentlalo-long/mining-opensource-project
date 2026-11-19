"""Status table and results-header checks."""

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from gh_ecosystem.ingest import EXPERIMENT_COLUMNS, write_csv
from gh_ecosystem.status import STAGES, Stage, check_results_header, collect, main, stage_state


class StatusTests(unittest.TestCase):
    def test_stage_state_distinguishes_done_partial_and_todo(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = Stage("S0", "sample", ("a.txt", "b.txt"))
            self.assertEqual(stage_state(root, stage)["state"], "todo")
            (root / "a.txt").write_text("x", encoding="utf-8")
            self.assertEqual(stage_state(root, stage)["state"], "partial")
            (root / "b.txt").write_text("x", encoding="utf-8")
            self.assertEqual(stage_state(root, stage)["state"], "done")

    def test_collect_reports_missing_s0_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for path in STAGES[0].artifacts:
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("x", encoding="utf-8")
            rows = collect(root)
            self.assertEqual(rows[0]["state"], "done")
            self.assertTrue(all(row["state"] == "todo" for row in rows[1:]))

    def test_json_output_is_machine_readable(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = main(["--json", "--root", "/nonexistent"])
        self.assertEqual(code, 0)
        rows = json.loads(stdout.getvalue())
        self.assertEqual([row["code"] for row in rows], ["S0", "S1", "S2", "S3", "S4", "S5"])
        self.assertTrue(all(row["state"] == "todo" for row in rows))

    def test_results_header_accepts_expected_columns_and_rejects_others(self):
        with tempfile.TemporaryDirectory() as directory:
            good = Path(directory) / "good.csv"
            bad = Path(directory) / "bad.csv"
            write_csv(good, EXPERIMENT_COLUMNS, [])
            write_csv(bad, ["wrong"], [])
            self.assertIsNone(check_results_header(good))
            with self.assertRaises(ValueError):
                check_results_header(bad)

    def test_check_results_cli_fails_on_bad_header(self):
        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory) / "bad.csv"
            write_csv(bad, ["wrong"], [])
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = main(["--check-results", "--results-path", str(bad)])
            self.assertEqual(code, 1)
            self.assertIn("FAIL: results header", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
