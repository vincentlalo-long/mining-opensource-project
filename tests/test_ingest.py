"""Exercise ingestion and recovery with small archives and simulated failures."""

import gzip
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import call, patch
from urllib.error import HTTPError

from gh_ecosystem.ingest import (
    LEGACY_MANIFEST_COLUMNS, MANIFEST_COLUMNS, acquire_sample, load_config, main,
    read_csv, read_manifest, write_csv,
)


ROOT = Path(__file__).resolve().parents[1]
HOUR = "2024-01-01-0"


class Response(io.BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.headers = {"Content-Length": str(len(data))}


class IngestTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.raw = self.root / "raw"
        self.manifest = self.root / "manifest.csv"
        self.config = load_config(ROOT / "config/contract-local.example.yml")
        self.config["storage"]["raw_dir"] = self.raw.as_posix()
        self.config["inspection"]["manifest_path"] = self.manifest.as_posix()
        self.config["ingestion"].update(max_attempts=4, retry_initial_seconds=1, retry_max_seconds=2)
        self.archive = gzip.compress(b'{"id":"123"}\n')

    def test_transient_errors_retry_then_publish_one_verified_archive(self):
        statuses = []

        def download(request, timeout):
            statuses.append(read_manifest(self.manifest)[HOUR]["status"])
            if len(statuses) == 1:
                raise TimeoutError("read timed out")
            if len(statuses) == 2:
                raise HTTPError(request.full_url, 503, "Service Unavailable", {}, None)
            return Response(self.archive)

        with patch("urllib.request.urlopen", side_effect=download), patch("time.sleep") as wait:
            path, row, _, cached = acquire_sample(self.config, HOUR)
        self.assertEqual(statuses, ["pending", "pending", "pending"])
        self.assertEqual(wait.call_args_list, [call(1), call(2)])
        self.assertFalse(cached)
        self.assertEqual(path.read_bytes(), self.archive)
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["sha256"], hashlib.sha256(self.archive).hexdigest())
        self.assertEqual(row["compressed_bytes"], str(len(self.archive)))
        self.assertTrue(row["ingested_at"].endswith("Z"))
        self.assertEqual(read_manifest(self.manifest), {HOUR: row})

    def test_exhausted_retries_leave_failed_row_and_clean_partial_file(self):
        with patch("urllib.request.urlopen", side_effect=TimeoutError("read timed out")) as download:
            with patch("time.sleep") as wait, self.assertRaises(TimeoutError):
                acquire_sample(self.config, HOUR)
        self.assertEqual(download.call_count, 4)
        self.assertEqual(wait.call_args_list, [call(1), call(2), call(2)])
        row = read_manifest(self.manifest)[HOUR]
        self.assertEqual(row["status"], "failed")
        self.assertIn("Attempt 4: TimeoutError: read timed out", row["error"])
        self.assertEqual(row["ingested_at"], "")
        self.assertEqual(list(self.raw.iterdir()), [])

    def test_permanent_error_does_not_retry(self):
        error = HTTPError("https://data.gharchive.org/sample", 404, "Not Found", {}, None)
        with patch("urllib.request.urlopen", side_effect=error) as download:
            with patch("time.sleep") as wait, self.assertRaises(HTTPError):
                acquire_sample(self.config, HOUR)
        download.assert_called_once()
        wait.assert_not_called()
        self.assertIn("404", read_manifest(self.manifest)[HOUR]["error"])

    def test_resume_reuses_ok_and_recovers_pending_and_failed_hours(self):
        pending, failed = "2024-01-01-1", "2024-01-01-2"
        with patch("urllib.request.urlopen", return_value=Response(self.archive)):
            path, ok, _, _ = acquire_sample(self.config, HOUR)
        with patch("urllib.request.urlopen", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                acquire_sample(self.config, pending)
        error = HTTPError("https://data.gharchive.org/sample", 404, "Not Found", {}, None)
        with patch("urllib.request.urlopen", side_effect=error), self.assertRaises(HTTPError):
            acquire_sample(self.config, failed)
        rows = read_manifest(self.manifest)
        self.assertEqual(rows[pending]["status"], "pending")
        self.assertEqual(rows[failed]["status"], "failed")
        (self.raw / f"{pending}.json.gz.part").write_bytes(b"partial")

        config_path = self.root / "config.yml"
        config_path.write_text(json.dumps(self.config), encoding="utf-8")
        with patch("sys.argv", ["ingest", "--resume", "--config", str(config_path)]):
            with patch("urllib.request.urlopen", side_effect=lambda *a, **kw: Response(self.archive)) as download:
                with redirect_stdout(io.StringIO()):
                    main()
        urls = [request.args[0].full_url.rsplit("/", 1)[-1] for request in download.call_args_list]
        self.assertEqual(urls, [f"{pending}.json.gz", f"{failed}.json.gz"])
        self.assertEqual(path.read_bytes(), self.archive)
        rows = read_manifest(self.manifest)
        self.assertEqual(set(rows), {HOUR, pending, failed})
        self.assertEqual(rows[HOUR], ok)
        self.assertEqual({row["status"] for row in rows.values()}, {"ok"})
        self.assertEqual(list(self.raw.glob("*.part")), [])

    def test_legacy_manifest_is_migrated_and_rerun_changes_no_bytes(self):
        with patch("urllib.request.urlopen", return_value=Response(self.archive)):
            path, row, _, _ = acquire_sample(self.config, HOUR)
        legacy = {column: row[column] for column in LEGACY_MANIFEST_COLUMNS}
        write_csv(self.manifest, LEGACY_MANIFEST_COLUMNS, [legacy])
        with patch("urllib.request.urlopen") as download:
            self.assertTrue(acquire_sample(self.config, HOUR)[3])
            migrated = self.manifest.read_bytes()
            self.assertTrue(acquire_sample(self.config, HOUR)[3])
        download.assert_not_called()
        self.assertEqual(path.read_bytes(), self.archive)
        self.assertEqual(self.manifest.read_bytes(), migrated)
        self.assertEqual(read_csv(self.manifest, MANIFEST_COLUMNS), [row])

    def test_checksum_mismatch_at_same_size_repairs_ok_file(self):
        with patch("urllib.request.urlopen", return_value=Response(self.archive)):
            path, _, _, _ = acquire_sample(self.config, HOUR)
        path.write_bytes(self.archive[:-1] + bytes([self.archive[-1] ^ 1]))
        with patch("urllib.request.urlopen", return_value=Response(self.archive)) as download:
            _, row, _, cached = acquire_sample(self.config, HOUR)
        download.assert_called_once()
        self.assertFalse(cached)
        self.assertEqual(path.read_bytes(), self.archive)
        self.assertEqual(read_manifest(self.manifest), {HOUR: row})

    def test_concurrent_writer_cannot_lose_manifest_rows(self):
        next_hour = "2024-01-01-1"

        def download(request, timeout):
            with self.assertRaisesRegex(BlockingIOError, "Another ingest"):
                acquire_sample(self.config, next_hour)
            return Response(self.archive)

        with patch("urllib.request.urlopen", side_effect=download):
            acquire_sample(self.config, HOUR)
        with patch("urllib.request.urlopen", return_value=Response(self.archive)):
            acquire_sample(self.config, next_hour)
        self.assertEqual(set(read_manifest(self.manifest)), {HOUR, next_hour})


if __name__ == "__main__":
    unittest.main()
