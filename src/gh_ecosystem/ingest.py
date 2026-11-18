"""Minimal one-hour raw ingestion; production retry/state handling belongs to issue #5."""

import argparse
import csv
import gzip
import hashlib
import json
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


EXPERIMENT_COLUMNS = [
    "experiment_id", "run_id", "recorded_at", "config", "source_file",
    "metric", "value", "unit",
]
MANIFEST_COLUMNS = [
    "hour", "source_url", "local_path", "sha256", "compressed_bytes", "downloaded_at",
]


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
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def sha256_file(path, chunk_bytes):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_bytes), b""):
            digest.update(chunk)
    return digest.hexdigest()


def acquire_sample(config, hour):
    """Retain original bytes; only reuse a cache matching the sample manifest."""
    inspect = config["inspection"]
    parse_hour(hour)
    source_url = f"{config['source']['base_url'].rstrip('/')}/{hour}.json.gz"
    path = Path(config["storage"]["raw_dir"]) / f"{hour}.json.gz"
    manifest = Path(inspect["manifest_path"])
    rows = read_csv(manifest, MANIFEST_COLUMNS)
    previous = next((row for row in rows if row["hour"] == hour), None)
    chunk_bytes = inspect["chunk_bytes"]
    if path.exists() and previous and previous["source_url"] == source_url:
        if (path.stat().st_size == int(previous["compressed_bytes"])
                and sha256_file(path, chunk_bytes) == previous["sha256"]):
            return path, previous, None, True
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    started = time.perf_counter()
    digest = hashlib.sha256()
    size = 0
    try:
        request = urllib.request.Request(source_url, headers={"User-Agent": "gh-ecosystem-s0"})
        with urllib.request.urlopen(request, timeout=inspect["timeout_seconds"]) as response:
            expected = response.headers.get("Content-Length")
            with temporary.open("wb") as stream:
                while chunk := response.read(chunk_bytes):
                    stream.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        if expected is not None and size != int(expected):
            raise ValueError("Incomplete archive download")
        # Reading to EOF verifies the gzip footer/CRC before publishing the cache.
        with gzip.open(temporary, "rb") as stream:
            while stream.read(chunk_bytes):
                pass
        elapsed = time.perf_counter() - started
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    row = {
        "hour": hour, "source_url": source_url, "local_path": path.as_posix(),
        "sha256": digest.hexdigest(), "compressed_bytes": size, "downloaded_at": utc_now(),
    }
    write_csv(manifest, MANIFEST_COLUMNS,
              [item for item in rows if item["hour"] != hour] + [row])
    return path, row, elapsed, False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hour", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    path, _, _, _ = acquire_sample(load_config(args.config), args.hour)
    print(f"Raw archive and sample manifest ready: {path}")


if __name__ == "__main__":
    main()
