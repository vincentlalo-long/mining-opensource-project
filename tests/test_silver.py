"""Unit tests for silver layer deduplication and typed event extraction."""

import unittest

from gh_ecosystem.silver.driver import deduplicate_records
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

    def test_deduplicate_records_tie_breaking(self):
        records = [
            {
                "event_id": "duplicate_1",
                "event_type": "PushEvent",
                "ingested_at": "2024-01-01T01:00:00Z",
                "source_file": "file_b",
                "payload": "{}",
            },
            {
                "event_id": "duplicate_1",
                "event_type": "PushEvent",
                "ingested_at": "2024-01-01T00:00:00Z",  # Earliest
                "source_file": "file_a",
                "payload": "{}",
            },
            {
                "event_id": "unique_2",
                "event_type": "CreateEvent",
                "ingested_at": "2024-01-01T00:00:00Z",
                "source_file": "file_a",
                "payload": "{}",
            },
        ]
        deduped = deduplicate_records(records)
        self.assertEqual(len(deduped), 2)
        dup = [r for r in deduped if r["event_id"] == "duplicate_1"][0]
        self.assertEqual(dup["source_file"], "file_a")


if __name__ == "__main__":
    unittest.main()
