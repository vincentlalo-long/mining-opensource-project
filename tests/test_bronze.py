"""Unit tests for bronze layer processing."""

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from gh_ecosystem.bronze import build_bronze, extract_event_date


class BronzeTests(unittest.TestCase):
    def test_extract_event_date_from_utc_timestamp(self):
        self.assertEqual(extract_event_date("2024-01-01T00:30:00Z"), "2024-01-01")
        self.assertEqual(extract_event_date("2024-01-01T23:59:59+00:00"), "2024-01-01")

    def test_build_bronze_preserves_payload_and_projects_common_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            raw_dir = temp_path / "data" / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)

            contract_path = temp_path / "contract.json"
            contract_data = {
                "bronze": {
                    "columns": {
                        "event_id": {"source": "id", "type": "string", "nullable": False},
                        "event_type": {"source": "type", "type": "string", "nullable": False},
                        "actor_id": {"source": "actor.id", "type": "long", "nullable": False},
                        "repo_id": {"source": "repo.id", "type": "long", "nullable": False},
                        "org_id": {"source": "org.id", "type": "long", "nullable": True},
                        "created_at": {"source": "created_at", "type": "timestamp", "nullable": False},
                        "payload": {"source": "payload", "type": "string", "nullable": False, "encoding": "json_object_string"},
                        "ingested_at": {"source": "$ingested_at", "type": "timestamp", "nullable": False},
                        "source_file": {"source": "$source_file", "type": "string", "nullable": False},
                    }
                }
            }
            contract_path.write_text(json.dumps(contract_data), encoding="utf-8")

            manifest_path = temp_path / "manifest.csv"
            manifest_path.write_text("hour,source_url,local_path,sha256,compressed_bytes,downloaded_at,status,ingested_at,error\n", encoding="utf-8")

            config_path = temp_path / "config.json"
            config_data = {
                "storage": {
                    "raw_dir": str(raw_dir),
                    "buckets": {
                        "bronze": "bronze",
                    },
                },
                "inspection": {
                    "contract_path": str(contract_path),
                    "manifest_path": str(manifest_path),
                },
            }
            config_path.write_text(json.dumps(config_data), encoding="utf-8")

            # Create sample raw gzip
            sample_file = raw_dir / "2024-01-01-0.json.gz"
            payload_1 = {"push_id": 999, "size": 1, "commits": [{"sha": "abc"}]}
            events = [
                {
                    "id": "1001",
                    "type": "PushEvent",
                    "actor": {"id": 10},
                    "repo": {"id": 20},
                    "org": {"id": 30},
                    "created_at": "2024-01-01T00:15:00Z",
                    "payload": payload_1,
                },
                {
                    "id": "1002",
                    "type": "WatchEvent",
                    "actor": {"id": 11},
                    "repo": {"id": 21},
                    "org": None,
                    "created_at": "2024-01-01T00:20:00Z",
                    "payload": {"action": "started"},
                },
            ]
            with gzip.open(sample_file, "wb") as gz:
                for ev in events:
                    gz.write((json.dumps(ev) + "\n").encode("utf-8"))

            result = build_bronze(config_path, hour="2024-01-01-0")
            self.assertEqual(result["total_events"], 2)
            self.assertIn("2024-01-01", result["partitions"])
            self.assertEqual(result["partitions"]["2024-01-01"], 2)

            # Verification: Read output file and assert payload was preserved as valid JSON string
            output_file = temp_path / "data" / "bronze" / "events" / "event_date=2024-01-01" / "events_2024-01-01-0.jsonl"
            self.assertTrue(output_file.exists())
            records = [json.loads(line) for line in output_file.read_text(encoding="utf-8").splitlines() if line]
            self.assertEqual(len(records), 2)

            rec_1 = records[0]
            self.assertEqual(rec_1["event_id"], "1001")
            self.assertIsInstance(rec_1["payload"], str)
            self.assertEqual(json.loads(rec_1["payload"]), payload_1)
            self.assertEqual(rec_1["source_file"], f"{raw_dir}/2024-01-01-0.json.gz".replace("\\", "/"))

    def test_invalid_raw_policy_fails_on_malformed_event(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            raw_dir = temp_path / "data" / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)

            contract_path = temp_path / "contract.json"
            contract_data = {
                "bronze": {
                    "columns": {
                        "event_id": {"source": "id", "type": "string", "nullable": False},
                        "event_type": {"source": "type", "type": "string", "nullable": False},
                        "actor_id": {"source": "actor.id", "type": "long", "nullable": False},
                        "repo_id": {"source": "repo.id", "type": "long", "nullable": False},
                        "org_id": {"source": "org.id", "type": "long", "nullable": True},
                        "created_at": {"source": "created_at", "type": "timestamp", "nullable": False},
                        "payload": {"source": "payload", "type": "string", "nullable": False, "encoding": "json_object_string"},
                        "ingested_at": {"source": "$ingested_at", "type": "timestamp", "nullable": False},
                        "source_file": {"source": "$source_file", "type": "string", "nullable": False},
                    }
                }
            }
            contract_path.write_text(json.dumps(contract_data), encoding="utf-8")

            manifest_path = temp_path / "manifest.csv"
            manifest_path.write_text("hour,source_url,local_path,sha256,compressed_bytes,downloaded_at,status,ingested_at,error\n", encoding="utf-8")

            config_path = temp_path / "config.json"
            config_data = {
                "storage": {
                    "raw_dir": str(raw_dir),
                    "buckets": {"bronze": "bronze"},
                },
                "inspection": {
                    "contract_path": str(contract_path),
                    "manifest_path": str(manifest_path),
                },
            }
            config_path.write_text(json.dumps(config_data), encoding="utf-8")

            # Corrupt archive with malformed JSON line
            sample_file = raw_dir / "2024-01-01-0.json.gz"
            with gzip.open(sample_file, "wb") as gz:
                gz.write(b"not a valid json\n")

            with self.assertRaises(ValueError) as ctx:
                build_bronze(config_path, hour="2024-01-01-0")
            self.assertIn("Contract violation at line 1", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
