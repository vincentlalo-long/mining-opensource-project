"""Unit tests for gold layer metrics."""

import json
import tempfile
import unittest
from pathlib import Path

from gh_ecosystem.gold.activity import compute_event_volume_daily
from gh_ecosystem.gold.driver import build_gold


class GoldTests(unittest.TestCase):
    def test_compute_event_volume_daily(self):
        events = [
            {"created_at": "2024-01-01T01:00:00Z", "event_type": "PushEvent"},
            {"created_at": "2024-01-01T02:00:00Z", "event_type": "PushEvent"},
            {"created_at": "2024-01-01T03:00:00Z", "event_type": "CreateEvent"},
            {"created_at": "2024-01-02T04:00:00Z", "event_type": "WatchEvent"},
        ]
        metrics = compute_event_volume_daily(events)
        self.assertEqual(len(metrics), 3)

        metric_dict = {(m["event_date"], m["event_type"]): m["event_count"] for m in metrics}
        self.assertEqual(metric_dict[("2024-01-01", "PushEvent")], 2)
        self.assertEqual(metric_dict[("2024-01-01", "CreateEvent")], 1)
        self.assertEqual(metric_dict[("2024-01-02", "WatchEvent")], 1)

    def test_compute_event_volume_daily_fails_on_missing_fields(self):
        with self.assertRaises(ValueError):
            compute_event_volume_daily([{"event_id": "1", "event_type": "PushEvent"}])
        with self.assertRaises(ValueError):
            compute_event_volume_daily([{"event_id": "1", "created_at": "2024-01-01T00:00:00Z"}])

    def test_build_gold_idempotent_rerun(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            silver_dir = temp_path / "data" / "silver" / "events" / "event_date=2024-01-01"
            silver_dir.mkdir(parents=True, exist_ok=True)

            events = [
                {"event_id": "1", "created_at": "2024-01-01T10:00:00Z", "event_type": "PushEvent"},
                {"event_id": "2", "created_at": "2024-01-01T11:00:00Z", "event_type": "PushEvent"},
                {"event_id": "3", "created_at": "2024-01-01T12:00:00Z", "event_type": "WatchEvent"},
            ]
            (silver_dir / "events.jsonl").write_text(
                "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
            )

            config_path = temp_path / "config.json"
            config_data = {
                "storage": {
                    "raw_dir": str(temp_path / "data" / "raw"),
                    "buckets": {"silver": "silver", "gold": "gold"},
                }
            }
            config_path.write_text(json.dumps(config_data), encoding="utf-8")

            res1 = build_gold(config_path)
            res2 = build_gold(config_path)
            self.assertEqual(res1["total_metric_rows"], 2)
            self.assertEqual(res2["total_metric_rows"], 2)

            # Verification: output file has exactly 2 lines and no CSV was written
            metric_file = temp_path / "data" / "gold" / "event_volume_daily" / "event_date=2024-01-01" / "volume.jsonl"
            lines = [line for line in metric_file.read_text(encoding="utf-8").splitlines() if line]
            self.assertEqual(len(lines), 2)
            self.assertFalse((temp_path / "data" / "gold" / "event_volume_daily" / "event_date=2024-01-01" / "volume.csv").exists())

    def test_build_gold_rebuilds_all_dates_and_removes_stale(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            silver_events = temp_path / "data" / "silver" / "events"
            day1 = silver_events / "event_date=2024-01-01"
            day2 = silver_events / "event_date=2024-01-02"
            day1.mkdir(parents=True)
            day2.mkdir(parents=True)

            day1_events = [
                {"event_id": "1", "created_at": "2024-01-01T10:00:00Z", "event_type": "PushEvent"},
                {"event_id": "2", "created_at": "2024-01-01T11:00:00Z", "event_type": "PushEvent"},
                {"event_id": "3", "created_at": "2024-01-01T12:00:00Z", "event_type": "WatchEvent"},
            ]
            day2_events = [
                {"event_id": "4", "created_at": "2024-01-02T10:00:00Z", "event_type": "PushEvent"},
            ]
            (day1 / "events.jsonl").write_text(
                "\n".join(json.dumps(e) for e in day1_events) + "\n", encoding="utf-8"
            )
            (day2 / "events.jsonl").write_text(
                "\n".join(json.dumps(e) for e in day2_events) + "\n", encoding="utf-8"
            )
            # Legacy per-hour file must be ignored as gold input
            (day1 / "events_2024-01-01-0.jsonl").write_text(
                json.dumps(day1_events[0]) + "\n", encoding="utf-8"
            )

            config_path = temp_path / "config.json"
            config_data = {
                "storage": {
                    "raw_dir": str(temp_path / "data" / "raw"),
                    "buckets": {"silver": "silver", "gold": "gold"},
                }
            }
            config_path.write_text(json.dumps(config_data), encoding="utf-8")

            gold_base = temp_path / "data" / "gold" / "event_volume_daily"
            (gold_base / "event_date=2023-12-31").mkdir(parents=True)
            (gold_base / "event_date=2023-12-31" / "volume.jsonl").write_text("", encoding="utf-8")

            res = build_gold(config_path)
            self.assertEqual(res["total_metric_rows"], 3)

            day1_volume = gold_base / "event_date=2024-01-01" / "volume.jsonl"
            day2_volume = gold_base / "event_date=2024-01-02" / "volume.jsonl"
            self.assertEqual(len(day1_volume.read_text(encoding="utf-8").splitlines()), 2)
            self.assertEqual(len(day2_volume.read_text(encoding="utf-8").splitlines()), 1)
            self.assertFalse((gold_base / "event_date=2023-12-31").exists())


if __name__ == "__main__":
    unittest.main()
