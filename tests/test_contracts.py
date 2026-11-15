"""Contract behavior checks using small synthetic archives and mocked HTTP."""

import copy
import gzip
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from gh_ecosystem.contracts import bronze_projection, check_sample, validate_contract
from gh_ecosystem.ingest import (
    EXPERIMENT_COLUMNS, MANIFEST_COLUMNS, acquire_sample, load_config,
    parse_hour, read_csv, write_csv,
)
from gh_ecosystem.inspect import inspect_hour, profile_archive


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = load_config(ROOT / "src/gh_ecosystem/event_contract_v1.json")
LOCAL = load_config(ROOT / "config/contract-local.example.yml")
CLUSTER = load_config(ROOT / "config/contract-cluster.example.yml")
HOUR = "2024-01-01-0"
STAMP = "2024-01-02T00:00:00Z"


def raw_event():
    return {
        "id": "9007199254740993", "type": "PushEvent",
        "actor": {"id": 123}, "repo": {"id": 456},
        "created_at": "2024-01-01T00:10:00Z", "public": True,
        "payload": {"text": "Tiếng Việt", "commits": [{"sha": "abc"}]},
    }


def archive_bytes(events):
    return gzip.compress("".join(json.dumps(event) + "\n" for event in events).encode())


class Response(io.BytesIO):
    def __init__(self, content, advertised_size=None):
        super().__init__(content)
        self.headers = {"Content-Length": str(len(content) if advertised_size is None else advertised_size)}


class ProjectionTests(unittest.TestCase):
    def project(self, event):
        return bronze_projection(event, "raw/sample.json.gz", STAMP, CONTRACT)

    def test_optional_org_and_payload_roundtrip(self):
        event = raw_event()
        projected = self.project(event)
        self.assertIsNone(projected["org_id"])
        self.assertEqual(projected["event_id"], event["id"])
        self.assertIsInstance(projected["payload"], str)
        self.assertEqual(json.loads(projected["payload"]), event["payload"])
        self.assertIn("Tiếng Việt", projected["payload"])
        for org in (None, {}, {"id": None}):
            event["org"] = org
            self.assertIsNone(self.project(event)["org_id"])
        event["org"] = {"id": 789}
        self.assertEqual(self.project(event)["org_id"], 789)

    def test_unknown_type_and_extra_fields_remain_acceptable(self):
        event = raw_event()
        event["type"] = "FutureEvent"
        event["new_top_level"] = {"nested": True}
        event["payload"]["future_array"] = [None, False, {"key": 1}]
        projected = self.project(event)
        self.assertEqual(projected["event_type"], "FutureEvent")
        self.assertEqual(json.loads(projected["payload"]), event["payload"])

    def test_rejects_invalid_identifiers_payload_and_time(self):
        bad_events = []
        for bad in (True, "123", 0, -1, 2**63):
            event = raw_event()
            event["actor"]["id"] = bad
            bad_events.append(event)
        for key, value in (("id", 123), ("type", ""), ("payload", "{}"),
                           ("payload", []), ("created_at", "2024-01-01T00:00:00")):
            event = raw_event()
            event[key] = value
            bad_events.append(event)
        for event in bad_events:
            with self.subTest(event=event), self.assertRaises(ValueError):
                self.project(event)

    def test_timestamp_is_normalized_to_utc(self):
        event = raw_event()
        event["created_at"] = "2024-01-01T07:10:00+07:00"
        self.assertEqual(self.project(event)["created_at"], "2024-01-01T00:10:00Z")

    def test_contract_config_and_table_boundaries(self):
        validate_contract(CONTRACT, LOCAL, CLUSTER)
        mutations = [
            lambda item: item["spark"].update(unreviewed_setting=1),
            lambda item: item["spark"].update(shuffle_partitions=False),
            lambda item: item["iceberg"].update(warehouse="s3://other/warehouse"),
            lambda item: item["storage"].update(s3_access_key_env="actual-secret"),
        ]
        for mutate in mutations:
            config = copy.deepcopy(CLUSTER)
            mutate(config)
            with self.subTest(config=config), self.assertRaises(ValueError):
                validate_contract(CONTRACT, LOCAL, config)
        contract = copy.deepcopy(CONTRACT)
        contract["silver"][0]["partition"] = ["repo_id"]
        with self.assertRaisesRegex(ValueError, "partition"):
            validate_contract(contract, LOCAL, CLUSTER)


class InspectionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config = copy.deepcopy(LOCAL)
        self.config["storage"]["raw_dir"] = str(self.root / "raw")
        for key in ("manifest_path", "fields_path", "event_types_path", "evidence_path", "experiments_path"):
            original = Path(self.config["inspection"][key]).name
            self.config["inspection"][key] = str(self.root / original)
        self.config["inspection"]["contract_path"] = str(ROOT / "src/gh_ecosystem/event_contract_v1.json")
        self.config["inspection"]["cluster_config_path"] = str(ROOT / "config/contract-cluster.example.yml")
        self.config_path = self.root / "local.yml"
        self.config_path.write_text(json.dumps(self.config), encoding="utf-8")
        self.sample = archive_bytes([raw_event()])

    def inspect(self):
        with patch("urllib.request.urlopen", return_value=Response(self.sample)), redirect_stdout(io.StringIO()):
            return inspect_hour(self.config_path, HOUR)

    def test_repeated_inspection_reuses_bytes_and_keeps_distinct_runs(self):
        first = self.inspect()
        with patch("urllib.request.urlopen") as download, redirect_stdout(io.StringIO()):
            second = inspect_hour(self.config_path, HOUR)
            check_sample(self.config_path)
        download.assert_not_called()
        self.assertEqual(first["sha256"], second["sha256"])
        self.assertNotEqual(first["run_id"], second["run_id"])
        rows = read_csv(self.config["inspection"]["experiments_path"], EXPERIMENT_COLUMNS)
        latest = {row["metric"]: row["value"] for row in rows if row["run_id"] == second["run_id"]}
        self.assertEqual(latest["events"], "1")
        self.assertEqual(latest["cache_reused"], "1")
        self.assertNotIn("download_and_gzip_validation_seconds", latest)
        manifest = read_csv(self.config["inspection"]["manifest_path"], MANIFEST_COLUMNS)
        self.assertEqual(len(manifest), 1)

    def test_truncated_download_is_not_published(self):
        with patch("urllib.request.urlopen", return_value=Response(self.sample, len(self.sample) + 1)):
            with self.assertRaisesRegex(ValueError, "Incomplete"):
                acquire_sample(self.config, HOUR)
        self.assertFalse(list((self.root / "raw").iterdir()))
        self.assertFalse(Path(self.config["inspection"]["manifest_path"]).exists())

    def test_corrupt_gzip_is_not_published(self):
        with patch("urllib.request.urlopen", return_value=Response(b"not a gzip archive")):
            with self.assertRaises(gzip.BadGzipFile):
                acquire_sample(self.config, HOUR)
        self.assertFalse(list((self.root / "raw").iterdir()))

    def test_incompatible_measurement_header_fails_before_download(self):
        Path(self.config["inspection"]["experiments_path"]).write_text("wrong,header\n", encoding="utf-8")
        with patch("urllib.request.urlopen") as download, self.assertRaisesRegex(ValueError, "header"):
            inspect_hour(self.config_path, HOUR)
        download.assert_not_called()

    def test_sample_tampering_fails_checksum(self):
        evidence = self.inspect()
        with Path(evidence["local_path"]).open("ab") as stream:
            stream.write(b"tampered")
        with self.assertRaisesRegex(ValueError, "checksum"):
            check_sample(self.config_path)

    def test_measurement_tampering_is_detected(self):
        self.inspect()
        path = self.config["inspection"]["experiments_path"]
        rows = read_csv(path, EXPERIMENT_COLUMNS)
        next(row for row in rows if row["metric"] == "events")["value"] = "999"
        write_csv(path, EXPERIMENT_COLUMNS, rows)
        with self.assertRaisesRegex(ValueError, "measurement mismatch"):
            check_sample(self.config_path)

    def test_stale_profile_is_detected(self):
        self.inspect()
        Path(self.config["inspection"]["event_types_path"]).write_text("event_type\nIssuesEvent\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Event profile"):
            check_sample(self.config_path)

    def test_profiler_reports_duplicates_malformed_and_missing_fields(self):
        path = self.root / "mixed.json.gz"
        event = raw_event()
        other = raw_event()
        other["org"] = None
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            stream.write(json.dumps(event) + "\n")
            stream.write(json.dumps(other) + "\n")
            stream.write("invalid json\n")
        fields, events, metrics = profile_archive(path, HOUR)
        self.assertEqual(metrics["events"], 2)
        self.assertEqual(metrics["duplicate_event_ids"], 1)
        self.assertEqual(metrics["malformed_lines"], 1)
        self.assertEqual(fields["org"]["null"], 1)
        self.assertEqual(events["PushEvent"], 2)

    def test_hour_must_be_single_canonical_utc_hour(self):
        for hour in ("2024-01-01-00", "2024-01-01-24", "2024-01-01-{0..23}"):
            with self.subTest(hour=hour), self.assertRaises(ValueError):
                parse_hour(hour)


if __name__ == "__main__":
    unittest.main()
