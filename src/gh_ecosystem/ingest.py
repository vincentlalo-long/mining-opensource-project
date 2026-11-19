"""Download and resume hourly raw archives using a durable manifest."""

import argparse
import csv
import errno
import gzip
import hashlib
import http.client
import json
import os
import socket
import time
import urllib.error
import urllib.request
import zlib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


EXPERIMENT_COLUMNS = [
    "experiment_id", "run_id", "recorded_at", "config", "source_file",
    "metric", "value", "unit",
]
LEGACY_MANIFEST_COLUMNS = [
    "hour", "source_url", "local_path", "sha256", "compressed_bytes", "downloaded_at",
]
MANIFEST_COLUMNS = [*LEGACY_MANIFEST_COLUMNS, "status", "ingested_at", "error"]
MANIFEST_STATES = {"pending", "ok", "failed"}
RETRYABLE_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
INGEST_ERRORS = (OSError, ValueError, EOFError, http.client.HTTPException, zlib.error)


class IncompleteArchiveError(ValueError):
    """The response ended before reaching its advertised length."""


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_config(path):
    """Use the JSON subset of YAML without adding a YAML dependency."""
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def parse_hour(value):
    parsed = datetime.strptime(value, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc)
    canonical = f"{parsed:%Y-%m-%d}-{parsed.hour}"
    if value != canonical:
        raise ValueError(f"Hour must use YYYY-MM-DD-H in UTC: {canonical}")
    return parsed


def read_csv(path, columns):
    path = Path(path)
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != columns:
            raise ValueError(f"Unexpected CSV header: {path}")
        return list(reader)


def write_csv(path, columns, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_manifest(path):
    path = Path(path)
    if not path.exists():
        return {}, False
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        legacy = reader.fieldnames == LEGACY_MANIFEST_COLUMNS
        if not legacy and reader.fieldnames != MANIFEST_COLUMNS:
            raise ValueError(f"Unexpected manifest header: {path}")
        rows = {}
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Malformed manifest row: {path}:{reader.line_num}")
            hour = row["hour"]
            parse_hour(hour)
            if hour in rows:
                raise ValueError(f"Duplicate manifest hour: {hour}")
            if legacy:
                row.update(status="ok", ingested_at=row["downloaded_at"], error="")
            if row["status"] not in MANIFEST_STATES:
                raise ValueError(f"Invalid manifest status for {hour}: {row['status']}")
            rows[hour] = row
        return rows, legacy


def read_manifest(path):
    """Read both current rows and the successful downloads recorded by #1 and #3."""
    rows, _ = _load_manifest(path)
    return rows


@contextmanager
def _manifest_writer(path):
    """Let only one ingest publish files and update a given manifest at a time."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    # Leave the lock file in place so all processes lock the same file.
    with lock_path.open("a+b") as stream:
        try:
            if os.name == "nt":
                import msvcrt

                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in (errno.EACCES, errno.EAGAIN):
                raise BlockingIOError(f"Another ingest is using {path}; retry after it finishes") from error
            raise
        yield


def sha256_file(path, chunk_bytes):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_bytes), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _matches_archive(row, path, source_url, chunk_bytes):
    if (row is None or row["status"] != "ok" or row["source_url"] != source_url
            or row["local_path"] != path.as_posix()):
        return False
    try:
        return (str(path.stat().st_size) == row["compressed_bytes"]
                and sha256_file(path, chunk_bytes) == row["sha256"])
    except FileNotFoundError:
        return False


def _download_archive(source_url, path, chunk_bytes, timeout):
    temporary = path.with_suffix(path.suffix + ".part")
    digest = hashlib.sha256()
    size = 0
    try:
        request = urllib.request.Request(source_url, headers={"User-Agent": "gh-ecosystem-ingest"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            expected = response.headers.get("Content-Length")
            with temporary.open("wb") as stream:
                while chunk := response.read(chunk_bytes):
                    stream.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        if expected is not None and size != int(expected):
            raise IncompleteArchiveError(f"Incomplete archive download: expected {expected} bytes, got {size}")
        if not size:
            raise IncompleteArchiveError("Empty archive download")
        # Reading to EOF verifies the gzip footer and CRC before publishing.
        with gzip.open(temporary, "rb") as stream:
            while stream.read(chunk_bytes):
                pass
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return digest.hexdigest(), size


def _is_transient(error):
    if isinstance(error, urllib.error.HTTPError):
        return error.code in RETRYABLE_HTTP_STATUS
    if isinstance(error, urllib.error.URLError):
        error = error.reason
    if isinstance(error, socket.gaierror):
        return error.errno == socket.EAI_AGAIN
    return isinstance(error, (TimeoutError, ConnectionError, http.client.IncompleteRead,
                              IncompleteArchiveError))


def acquire_sample(config, hour):
    """Return a verified raw file, its manifest row, timing, and cache status."""
    inspected = config["inspection"]
    retry = config.get("ingestion", {})
    attempts = retry.get("max_attempts")
    delay = retry.get("retry_initial_seconds")
    max_delay = retry.get("retry_max_seconds")
    if any(type(value) is not int or value < 1 for value in (attempts, delay, max_delay)) or delay > max_delay:
        raise ValueError("Ingestion requires positive retry settings and initial delay <= maximum delay")
    parse_hour(hour)
    source_url = f"{config['source']['base_url'].rstrip('/')}/{hour}.json.gz"
    path = Path(config["storage"]["raw_dir"]) / f"{hour}.json.gz"
    manifest = Path(inspected["manifest_path"])
    chunk_bytes = inspected["chunk_bytes"]

    with _manifest_writer(manifest):
        rows, legacy = _load_manifest(manifest)
        previous = rows.get(hour)
        if _matches_archive(previous, path, source_url, chunk_bytes):
            if legacy:
                write_csv(manifest, MANIFEST_COLUMNS, rows.values())
            return path, previous, None, True

        row = {
            "hour": hour, "source_url": source_url, "local_path": path.as_posix(),
            "sha256": "", "compressed_bytes": "", "downloaded_at": "",
            "status": "pending", "ingested_at": "", "error": "",
        }
        rows[hour] = row
        write_csv(manifest, MANIFEST_COLUMNS, rows.values())
        for attempt in range(1, attempts + 1):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                started = time.perf_counter()
                checksum, size = _download_archive(source_url, path, chunk_bytes, inspected["timeout_seconds"])
            except INGEST_ERRORS as error:
                if attempt == attempts or not _is_transient(error):
                    row.update(status="failed", error=f"Attempt {attempt}: {type(error).__name__}: {error}")
                    write_csv(manifest, MANIFEST_COLUMNS, rows.values())
                    raise
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
            else:
                elapsed = time.perf_counter() - started
                timestamp = utc_now()
                row.update(status="ok", sha256=checksum, compressed_bytes=str(size),
                           downloaded_at=timestamp, ingested_at=timestamp)
                write_csv(manifest, MANIFEST_COLUMNS, rows.values())
                return path, row, elapsed, False


def ingest_hour(config, hour):
    path, _, _, cached = acquire_sample(config, hour)
    action = "Reused verified raw archive" if cached else "Ingested raw archive"
    print(f"{action}: {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--hour")
    selection.add_argument("--resume", action="store_true", help="Recheck and resume hours already in the manifest")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    try:
        config = load_config(args.config)
        if not args.resume:
            ingest_hour(config, args.hour)
            return
        hours = read_manifest(config["inspection"]["manifest_path"])
        failed = []
        for hour in sorted(hours, key=parse_hour):
            try:
                ingest_hour(config, hour)
            except INGEST_ERRORS as error:
                failed.append(hour)
                print(f"Ingest failed for {hour}: {error}")
        if failed:
            parser.exit(1, f"Resume failed for {', '.join(failed)}; see the manifest for details\n")
        print(f"Resume complete: {len(hours)} manifest hours checked")
    except INGEST_ERRORS as error:
        parser.exit(1, f"Ingest failed: {error}\n")


if __name__ == "__main__":
    main()
