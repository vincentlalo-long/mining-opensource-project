"""Unit tests for silver layer deduplication and typed event extraction."""

import json
import tempfile
import unittest
from pathlib import Path

from gh_ecosystem.silver.driver import build_silver, canonical_content_hash, deduplicate_records
from gh_ecosystem.silver.events import extract_push_event


class SilverTests(unittest.TestCase):
    def test_extract_push_event_typed_fields(self):
        record = {
            "event_id": "1001",
            "event_type": "PushEvent",
            "actor_id": 50,
            "repo_id": 100,
            "org_id": 200,
            "created_at": "2024-01-01T00:15:00Z",
            "payload": '{"push_id": 777, "size": 2, "distinct_size": 1, "ref": "refs/heads/main", "head": "abc", "before": "def", "commits": [{"sha": "abc"}]}',
        }
        push = extract_push_event(record)
        self.assertIsNotNone(push)
        self.assertEqual(push["push_id"], 777)
        self.assertEqual(push["size"], 2)
        self.assertEqual(push["distinct_size"], 1)
        self.assertEqual(push["ref"], "refs/heads/main")
        self.assertEqual(push["commit_count"], 1)

    def test_extract_push_event_rejects_malformed_types(self):
        record = {
            "event_id": "1001",
            "event_type": "PushEvent",
            "actor_id": 50,
            "repo_id": 100,
            "org_id": None,
            "created_at": "2024-01-01T00:15:00Z",
            "payload": '{"size": "not an int"}',
        }
        with self.assertRaises(ValueError):
            extract_push_event(record)

    def test_extract_push_event_ignores_other_types(self):
        record = {
            "event_id": "1002",
            "event_type": "WatchEvent",
            "actor_id": 50,
            "repo_id": 100,
            "org_id": None,
            "created_at": "2024-01-01T00:15:00Z",
            "payload": "{}",
        }
        self.assertIsNone(extract_push_event(record))

    def test_deduplicate_records_tie_breaking_and_conflict_evidence(self):
        records = [
            {
                "event_id": "duplicate_1",
                "event_type": "PushEvent",
                "actor_id": 1,
                "repo_id": 2,
                "org_id": None,
                "created_at": "2024-01-01T00:00:00Z",
                "ingested_at": "2024-01-01T01:00:00Z",
                "source_file": "file_b",
                "payload": '{"msg": "b"}',
            },
            {
                "event_id": "duplicate_1",
                "event_type": "PushEvent",
                "actor_id": 1,
                "repo_id": 2,
                "org_id": None,
                "created_at": "2024-01-01T00:00:00Z",
                "ingested_at": "2024-01-01T00:00:00Z",  # Earliest
                "source_file": "file_a",
                "payload": '{"msg": "a"}',
            },
            {
                "event_id": "unique_2",
                "event_type": "CreateEvent",
                "actor_id": 3,
                "repo_id": 4,
                "org_id": None,
                "created_at": "2024-01-01T00:00:00Z",
                "ingested_at": "2024-01-01T00:00:00Z",
                "source_file": "file_a",
                "payload": "{}",
            },
        ]
        deduped, conflicts = deduplicate_records(records)
        self.assertEqual(len(deduped), 2)
        dup = [r for r in deduped if r["event_id"] == "duplicate_1"][0]
        self.assertEqual(dup["source_file"], "file_a")
        # Conflict evidence was captured for differing occurrences
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["event_id"], "duplicate_1")
        self.assertEqual(conflicts[0]["occurrences"], 2)

    def test_build_silver_global_dedup_and_partition_cleanup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bronze_base = temp_path / "data" / "bronze" / "events"
            hour_dir = bronze_base / "event_date=2024-01-01"
            hour_dir.mkdir(parents=True, exist_ok=True)

            def event(event_id, ingested_at, payload, event_type="PushEvent"):
                return {
                    "event_id": event_id,
                    "event_type": event_type,
                    "actor_id": 1,
                    "repo_id": 2,
                    "org_id": None,
                    "created_at": "2024-01-01T12:00:00Z",
                    "payload": payload,
                    "ingested_at": ingested_at,
                    "source_file": f"data/raw/{ingested_at}.json.gz",
                }

            hour0 = [
                event("9999", "2024-01-01T12:05:00Z", '{"push_id": 100, "size": 1, "commits": []}'),
                event("2000", "2024-01-01T12:05:00Z", '{"msg": "a"}'),
            ]
            hour1 = [
                event("9999", "2024-01-01T12:06:00Z", '{"push_id": 100, "size": 1, "commits": []}'),
                event("2000", "2024-01-01T12:10:00Z", '{"msg": "b"}'),
                event("1000", "2024-01-01T12:10:00Z", "{}", event_type="CreateEvent"),
            ]
            (hour_dir / "events_2024-01-01-0.jsonl").write_text(
                "\n".join(json.dumps(e) for e in hour0) + "\n", encoding="utf-8"
            )
            (hour_dir / "events_2024-01-01-1.jsonl").write_text(
                "\n".join(json.dumps(e) for e in hour1) + "\n", encoding="utf-8"
            )

            config_path = temp_path / "config.json"
            config_data = {
                "storage": {
                    "raw_dir": str(temp_path / "data" / "raw"),
                    "buckets": {"bronze": "bronze", "silver": "silver"},
                }
            }
            config_path.write_text(json.dumps(config_data), encoding="utf-8")

            res = build_silver(config_path)
            self.assertEqual(res["bronze_records_read"], 5)
            self.assertEqual(res["silver_events_written"], 3)
            self.assertEqual(res["duplicates_removed"], 2)
            self.assertEqual(res["conflicts_detected"], 1)
            self.assertEqual(res["push_events_written"], 2)

            silver_base = temp_path / "data" / "silver"
            events_file = silver_base / "events" / "event_date=2024-01-01" / "events.jsonl"
            push_file = silver_base / "push_events" / "event_date=2024-01-01" / "push_events.jsonl"
            conflicts_file = silver_base / "conflicts.jsonl"
            self.assertEqual(len(events_file.read_text(encoding="utf-8").splitlines()), 3)
            self.assertEqual(len(push_file.read_text(encoding="utf-8").splitlines()), 2)
            self.assertEqual(len(conflicts_file.read_text(encoding="utf-8").splitlines()), 1)

            # Stale date and legacy per-hour files from an older run must disappear
            (silver_base / "events" / "event_date=2023-12-31").mkdir(parents=True)
            (silver_base / "events" / "event_date=2023-12-31" / "events.jsonl").write_text("", encoding="utf-8")
            (silver_base / "events" / "event_date=2024-01-01" / "events_2024-01-01-1.jsonl").write_text(
                "", encoding="utf-8"
            )

            res2 = build_silver(config_path)
            self.assertEqual(res2["silver_events_written"], 3)
            self.assertFalse((silver_base / "events" / "event_date=2023-12-31").exists())
            self.assertFalse(
                (silver_base / "events" / "event_date=2024-01-01" / "events_2024-01-01-1.jsonl").exists()
            )
            self.assertEqual(sorted(p.name for p in events_file.parent.iterdir()), ["events.jsonl"])


if __name__ == "__main__":
    unittest.main()
