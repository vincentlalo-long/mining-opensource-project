"""Inspect one raw archive using the shared one-hour ingest primitive."""

import argparse
import csv
import gzip
import json
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


from gh_ecosystem.ingest import (
    EXPERIMENT_COLUMNS, acquire_sample, load_config, parse_hour,
    read_csv, utc_now, write_csv,
)


def json_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def profile_archive(path, hour, validate=None):
    expected_hour = parse_hour(hour)
    fields = defaultdict(Counter)
    events = Counter()
    metrics = Counter()
    seen_ids = set()
    with gzip.open(path, "rb") as stream:
        for line in stream:
            metrics["uncompressed_bytes"] += len(line)
            metrics["lines"] += 1
            try:
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise ValueError("Event must be an object")
            except (ValueError, UnicodeDecodeError):
                metrics["malformed_lines"] += 1
                continue
            metrics["events"] += 1
            if validate is not None:
                validate(event, metrics["lines"])
            for name, value in event.items():
                fields[name][json_type(value)] += 1
            event_type = event.get("type")
            if not isinstance(event_type, str) or not event_type:
                metrics["invalid_event_type"] += 1
            else:
                events[event_type] += 1
            event_id = event.get("id")
            if isinstance(event_id, str) and event_id:
                metrics["duplicate_event_ids"] += int(event_id in seen_ids)
                seen_ids.add(event_id)
            else:
                metrics["invalid_event_id"] += 1
            try:
                stamp = datetime.fromisoformat(event["created_at"].replace("Z", "+00:00"))
                if stamp.utcoffset() is None:
                    raise ValueError("Timestamp needs a timezone")
                if stamp.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0) != expected_hour:
                    metrics["events_outside_hour"] += 1
            except (KeyError, TypeError, ValueError, AttributeError):
                metrics["invalid_created_at"] += 1
    # Materialize zeroes so reruns and checks never have to infer missing metrics.
    for name in ["events", "malformed_lines", "duplicate_event_ids", "invalid_event_id",
                 "invalid_event_type", "invalid_created_at", "events_outside_hour"]:
        metrics[name] += 0
    return fields, events, metrics


def inspect_hour(config_path, hour):
    config = load_config(config_path)
    inspect = config["inspection"]
    # Reject an incompatible shared measurement header before downloading anything.
    read_csv(inspect["experiments_path"], EXPERIMENT_COLUMNS)
    path, manifest, download_seconds, reused = acquire_sample(config, hour)
    started = time.perf_counter()
    fields, events, metrics = profile_archive(path, hour)
    scan_seconds = time.perf_counter() - started
    metrics["compressed_bytes"] = path.stat().st_size
    run_id = str(uuid.uuid4())
    recorded_at = utc_now()
    rows = []

    def measure(name, value, unit):
        rows.append(dict(zip(EXPERIMENT_COLUMNS, [
            "S0-SCHEMA-001", run_id, recorded_at, Path(config_path).as_posix(),
            manifest["source_url"], name, value, unit,
        ])))

    for name, value in sorted(metrics.items()):
        measure(name, value, "bytes" if name.endswith("bytes") else "count")
    measure("scan_seconds", scan_seconds, "seconds")
    measure("cache_reused", int(reused), "boolean")
    if download_seconds is not None:
        measure("download_and_gzip_validation_seconds", download_seconds, "seconds")
    field_rows = []
    for name, types in sorted(fields.items()):
        present = sum(types.values())
        missing = metrics["events"] - present
        field_rows.append({"field": name, "observed_types": "|".join(sorted(types)),
                           "missing_in_sample": str(missing > 0).lower(),
                           "null_in_sample": str(types["null"] > 0).lower()})
        measure(f"field.{name}.missing", missing, "count")
        for kind, count in sorted(types.items()):
            measure(f"field.{name}.{kind}", count, "count")
    for event_type, count in sorted(events.items()):
        measure(f"event_type.{event_type}", count, "count")
    # Measurements are append-only; profile files identify the latest successful run.
    result_path = Path(inspect["experiments_path"])
    result_path.parent.mkdir(parents=True, exist_ok=True)
    exists = result_path.exists()
    with result_path.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=EXPERIMENT_COLUMNS, lineterminator="\n")
        if not exists:
            writer.writeheader()
        writer.writerows(rows)
    write_csv(inspect["fields_path"], list(field_rows[0]) if field_rows else
              ["field", "observed_types", "missing_in_sample", "null_in_sample"], field_rows)
    write_csv(inspect["event_types_path"], ["event_type"],
              [{"event_type": name} for name in sorted(events)])
    evidence = {
        "issue": "https://github.com/vincentlalo-long/mining-opensource-project/issues/1",
        "scope": "One observed UTC hour; not full event coverage or a scale benchmark.",
        "hour": hour, "source_url": manifest["source_url"],
        "local_path": path.as_posix(), "sha256": manifest["sha256"],
        "downloaded_at": manifest["downloaded_at"], "run_id": run_id,
        "config": Path(config_path).as_posix(),
        "experiments_path": inspect["experiments_path"],
        "fields_path": inspect["fields_path"], "event_types_path": inspect["event_types_path"],
        "contract_status": load_config(inspect["contract_path"])["status"],
    }
    evidence_path = Path(inspect["evidence_path"])
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(f"Inspection evidence: {evidence_path}; measurements run_id={run_id}", flush=True)
    if not metrics["events"] or metrics["malformed_lines"]:
        raise ValueError("Sample is empty or contains malformed lines; inspect measurement rows")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hour", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    inspect_hour(args.config, args.hour)


if __name__ == "__main__":
    main()
