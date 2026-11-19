"""Source-capacity tests with synthetic data; no network or benchmark claims."""

import gzip
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from gh_ecosystem.capacity import check_capacity, hourly_range, measure_source, planning_values
from gh_ecosystem.ingest import EXPERIMENT_COLUMNS, MANIFEST_COLUMNS, load_config, read_csv, write_csv


ROOT = Path(__file__).resolve().parents[1]


class CapacityTests(unittest.TestCase):
    def test_range_crosses_month_and_rejects_non_daily_samples(self):
        hours = hourly_range("2024-01-31-12", "2024-02-01-11", 24)
        self.assertEqual(len(hours), 24)
        self.assertEqual(hours[-1], "2024-02-01-11")
        with self.assertRaises(ValueError):
            hourly_range("2024-01-01-0", "2024-01-01-1", 24)

    def test_capacity_plan_has_explicit_raw_budget(self):
        plans, days = planning_values(100, 1000, [1, 7, 30], 800)
        self.assertEqual(days, 7)
        self.assertEqual(plans[30]["raw_bytes"], 3000)
        self.assertEqual(plans[7]["events"], 7000)
        with self.assertRaises(ValueError):
            planning_values(100, 1000, [1, 7, 30], 99)

    def test_full_day_is_measured_and_cache_rerun_keeps_distinct_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(ROOT / "config/contract-local.example.yml")
            config["storage"]["raw_dir"] = str(root / "raw")
            config["inspection"]["manifest_path"] = str(root / "manifest.csv")
            config["inspection"]["experiments_path"] = str(root / "experiments.csv")
            config["capacity"]["evidence_path"] = str(root / "evidence.json")
            config_path = root / "local.yml"
            config_path.write_text(json.dumps(config), encoding="utf-8")

            def response(request, timeout):
                hour = request.full_url.rsplit("/", 1)[1].replace(".json.gz", "")
                event = {"id": hour, "type": "PushEvent", "payload": {},
                         "created_at": hour[:10] + "T" + hour[11:].zfill(2) + ":00:00Z"}
                data = gzip.compress((json.dumps(event) + "\n").encode())
                stream = io.BytesIO(data)
                stream.headers = {"Content-Length": str(len(data))}
                return stream

            with patch("urllib.request.urlopen", side_effect=response), redirect_stdout(io.StringIO()):
                first = measure_source(config_path, "2024-01-01-0", "2024-01-01-23")
            with patch("urllib.request.urlopen") as download, redirect_stdout(io.StringIO()):
                second = measure_source(config_path, "2024-01-01-0", "2024-01-01-23")
                check_capacity(config_path)
            download.assert_not_called()
            self.assertNotEqual(first["run_id"], second["run_id"])
            self.assertEqual(second["status"], "complete")
            self.assertEqual(len(read_csv(config["inspection"]["manifest_path"], MANIFEST_COLUMNS)), 24)
            rows = read_csv(config["inspection"]["experiments_path"], EXPERIMENT_COLUMNS)
            latest = {row["metric"]: row for row in rows if row["run_id"] == second["run_id"]
                      and not row["metric"].startswith("measured.hour.")}
            self.assertEqual(latest["measured.total.events"]["value"], "24")
            self.assertEqual(latest["measured.total.cache_reused_files"]["value"], "24")
            self.assertEqual(latest["planning.7_days.raw_bytes"]["unit"], "bytes_assumption")
            self.assertEqual(latest["planning.selected_days"]["value"], "30")
            self.assertEqual(second["sample_range"], {"from": "2024-01-01-0", "to": "2024-01-01-23", "hours": 24})
            raw_path = root / "raw" / "2024-01-01-0.json.gz"
            original = raw_path.read_bytes()
            with raw_path.open("ab") as stream:
                stream.write(b"tampered")
            with self.assertRaisesRegex(ValueError, "checksum/size"):
                check_capacity(config_path)
            raw_path.write_bytes(original)
            estimate = latest["planning.7_days.same_environment_scan_seconds"]
            estimate["value"] = str(float(estimate["value"]) + 1)
            write_csv(config["inspection"]["experiments_path"], EXPERIMENT_COLUMNS, rows)
            with self.assertRaisesRegex(ValueError, r"planning\.7_days\.same_environment_scan_seconds"):
                check_capacity(config_path)

    def test_failed_hour_cannot_produce_a_successful_recommendation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config(ROOT / "config/contract-local.example.yml")
            config["inspection"]["experiments_path"] = str(root / "experiments.csv")
            config["capacity"]["evidence_path"] = str(root / "evidence.json")
            path = root / "local.yml"
            path.write_text(json.dumps(config), encoding="utf-8")
            with patch("gh_ecosystem.capacity.acquire_sample", side_effect=OSError("network unavailable")), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "Incomplete sample"):
                    measure_source(path, "2024-01-01-0", "2024-01-01-23")
            evidence = load_config(config["capacity"]["evidence_path"])
            self.assertEqual(evidence["status"], "incomplete")
            self.assertNotIn("recommended_range", evidence)
            self.assertEqual(len(evidence["failed_hours"]), 24)


if __name__ == "__main__":
    unittest.main()
