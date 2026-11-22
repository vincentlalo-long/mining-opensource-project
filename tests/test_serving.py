"""Unit tests for Trino/Superset serving orchestration without cluster access."""

import base64
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from gh_ecosystem import serving


class ServingTests(unittest.TestCase):
    def test_sql_literal_escapes_single_quotes(self):
        self.assertEqual(serving.sql_literal("Push'Event"), "'Push''Event'")

    def test_load_gold_validates_hour_and_reads_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            gold_dir = root / "data" / "gold" / "event_volume_daily" / "event_date=2024-01-01"
            gold_dir.mkdir(parents=True)
            rows = [
                {"event_date": "2024-01-01", "event_type": "PushEvent", "event_count": 10},
                {"event_date": "2024-01-01", "event_type": "WatchEvent", "event_count": 3},
            ]
            (gold_dir / "volume.jsonl").write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps({"storage": {"raw_dir": str(root / "data" / "raw"), "buckets": {"gold": "gold"}}}),
                encoding="utf-8",
            )

            loaded = serving.load_gold(config_path, "2024-01-01-0")
            self.assertEqual(loaded, rows)

            with self.assertRaises(ValueError):
                serving.load_gold(config_path, "2024-01-01-00")

            with self.assertRaises(FileNotFoundError):
                serving.load_gold(config_path, "2024-01-02-0")

    def test_load_gold_rejects_empty_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            gold_dir = root / "data" / "gold" / "event_volume_daily" / "event_date=2024-01-01"
            gold_dir.mkdir(parents=True)
            (gold_dir / "volume.jsonl").write_text("\n", encoding="utf-8")
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps({"storage": {"raw_dir": str(root / "data" / "raw"), "buckets": {"gold": "gold"}}}),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                serving.load_gold(config_path, "2024-01-01-0")

    def test_load_into_trino_executes_partitioned_ddl_and_parses_counts(self):
        sql_calls = []

        def fake_trino(sql):
            sql_calls.append(sql)
            if sql.startswith("SELECT count(*)"):
                return "2\t13\t1"
            return ""

        rows = [
            {"event_date": "2024-01-01", "event_type": "PushEvent", "event_count": 10},
            {"event_date": "2024-01-01", "event_type": "WatchEvent", "event_count": 3},
        ]
        with mock.patch.object(serving, "trino", side_effect=fake_trino):
            result = serving.load_into_trino(rows)

        self.assertEqual(result, {"rows": 2, "event_count": 13, "dates": 1})
        self.assertEqual(len(sql_calls), 5)
        self.assertIn("partitioning = ARRAY['event_date']", sql_calls[2])
        self.assertIn("INSERT INTO iceberg.gold.event_volume_daily VALUES", sql_calls[3])

    def test_bootstrap_superset_decodes_marker_and_rejects_missing_marker(self):
        payload = {"database_id": 1, "dataset_id": 1, "chart_id": 1, "chart_rows": 2}
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        with mock.patch.object(serving, "run", return_value=f"noise\nSERVING_BOOTSTRAP_JSON={encoded}"):
            self.assertEqual(serving.bootstrap_superset(), payload)

        with mock.patch.object(serving, "run", return_value="no marker here"):
            with self.assertRaises(RuntimeError):
                serving.bootstrap_superset()

    def test_serve_writes_evidence_and_fails_on_count_mismatch(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "results").mkdir()
            gold_dir = root / "custom_zone" / "gold_bucket" / "event_volume_daily" / "event_date=2024-01-01"
            gold_dir.mkdir(parents=True)
            rows = [
                {"event_date": "2024-01-01", "event_type": "PushEvent", "event_count": 10},
                {"event_date": "2024-01-01", "event_type": "WatchEvent", "event_count": 3},
            ]
            expected_gold = gold_dir / "volume.jsonl"
            expected_gold.write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps({
                    "storage": {"raw_dir": str(root / "custom_zone" / "raw"), "buckets": {"gold": "gold_bucket"}},
                    "iceberg": {"file_format": "parquet"},
                }),
                encoding="utf-8",
            )

            superset_payload = {"database_id": 1, "dataset_id": 1, "chart_id": 1, "chart_rows": 2}
            with (
                mock.patch.object(serving, "ROOT", root),
                mock.patch.object(serving, "load_into_trino", return_value={"rows": 2, "event_count": 13, "dates": 1}) as load_mock,
                mock.patch.object(serving, "bootstrap_superset", return_value=superset_payload),
            ):
                evidence = serving.serve(config_path, "2024-01-01-0")

            load_mock.assert_called_once_with(rows, file_format="PARQUET")
            saved = json.loads((root / "results" / "issue_4_serving.json").read_text(encoding="utf-8"))
            self.assertEqual(saved, evidence)
            self.assertEqual(saved["gold_file"], "custom_zone/gold_bucket/event_volume_daily/event_date=2024-01-01/volume.jsonl")
            self.assertEqual(saved["gold_rows"], 2)
            self.assertEqual(saved["gold_event_count"], 13)
            self.assertEqual(saved["status"], "complete")

            with (
                mock.patch.object(serving, "ROOT", root),
                mock.patch.object(serving, "load_into_trino", return_value={"rows": 1, "event_count": 13, "dates": 1}),
            ):
                with self.assertRaises(RuntimeError):
                    serving.serve(config_path, "2024-01-01-0")


if __name__ == "__main__":
    unittest.main()
