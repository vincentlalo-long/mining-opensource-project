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

    def test_build_silver_idempotent_rerun(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bronze_dir = temp_path / "data" / "bronze" / "events" / "event_date=2024-01-01"
            bronze_dir.mkdir(parents=True, exist_ok=True)

            sample_event = {
                "event_id": "9999",
                "event_type": "PushEvent",
                "actor_id": 1,
                "repo_id": 2,
                "org_id": None,
                "created_at": "2024-01-01T12:00:00Z",
                "payload": '{"push_id": 100, "size": 1, "commits": []}',
                "ingested_at": "2024-01-01T12:05:00Z",
                "source_file": "data/raw/2024-01-01-12.json.gz",
            }
            (bronze_dir / "events_2024-01-01-12.jsonl").write_text(json.dumps(sample_event) + "\n", encoding="utf-8")

            config_path = temp_path / "config.json"
            config_data = {
                "storage": {
                    "raw_dir": str(temp_path / "data" / "raw"),
                    "buckets": {"bronze": "bronze", "silver": "silver"},
                }
            }
            config_path.write_text(json.dumps(config_data), encoding="utf-8")

            res1 = build_silver(config_path)
            res2 = build_silver(config_path)
            self.assertEqual(res1["silver_events_written"], 1)
            self.assertEqual(res2["silver_events_written"], 1)

            # Check output file has exactly 1 record after rerun
            events_file = temp_path / "data" / "silver" / "events" / "event_date=2024-01-01" / "events.jsonl"
            lines = [line for line in events_file.read_text(encoding="utf-8").splitlines() if line]
            self.assertEqual(len(lines), 1)


if __name__ == "__main__":
    unittest.main()
