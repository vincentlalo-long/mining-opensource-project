"""Silver processing driver: global deduplication and typed event extraction."""

import argparse
import hashlib
import json
import shutil
from collections import defaultdict
from pathlib import Path

from gh_ecosystem.bronze import extract_event_date
from gh_ecosystem.ingest import load_config
from gh_ecosystem.silver.events import extract_push_event

ROOT = Path(__file__).resolve().parents[3]


def canonical_content_hash(record):
    """SHA-256 of canonical JSON of all bronze event columns except ingested_at/source_file."""
    content = {
        "event_id": str(record["event_id"]),
        "event_type": str(record["event_type"]),
        "actor_id": int(record["actor_id"]),
        "repo_id": int(record["repo_id"]),
        "org_id": int(record["org_id"]) if record.get("org_id") is not None else None,
        "created_at": record["created_at"],
        "payload": record["payload"],
    }
    canonical_json = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def deduplicate_records(records):
    """Global deduplication by event_id with contract dedup_winner tie-breaking.
    
    Rule: earliest ingested_at, then lexical source_file, then SHA-256 of canonical content.
    Retains conflict evidence when duplicate event_ids have differing content.
    """
    grouped = defaultdict(list)
    for record in records:
        event_id = str(record["event_id"])
        grouped[event_id].append(record)

    deduped = []
    conflicts = []

    for event_id, candidates in grouped.items():
        if len(candidates) == 1:
            deduped.append(candidates[0])
            continue

        # Check for differing occurrences (conflict evidence)
        hashes = {canonical_content_hash(c) for c in candidates}
        if len(hashes) > 1:
            conflicts.append({
                "event_id": event_id,
                "occurrences": len(candidates),
                "distinct_hashes": sorted(hashes),
                "source_files": [c.get("source_file") for c in candidates],
            })

        # Tie-break sort per contract dedup_winner
        candidates.sort(
            key=lambda r: (
                r.get("ingested_at", ""),
                r.get("source_file", ""),
                canonical_content_hash(r),
            )
        )
        deduped.append(candidates[0])

    return deduped, conflicts


def build_silver(config_path, hour=None):
    """Build silver.events and silver.push_events from bronze zone with idempotent partition replacement."""
    config = load_config(config_path)
    raw_dir = Path(config["storage"]["raw_dir"])
    raw_dir_resolved = raw_dir if raw_dir.is_absolute() else ROOT / raw_dir
    storage_root = raw_dir_resolved.parent

    bronze_dir = storage_root / config["storage"]["buckets"]["bronze"] / "events"
    if not bronze_dir.exists():
        raise FileNotFoundError(f"Bronze events directory missing: {bronze_dir}")

    silver_base = storage_root / config["storage"]["buckets"]["silver"]
    silver_events_dir = silver_base / "events"
    silver_push_dir = silver_base / "push_events"
    silver_events_dir.mkdir(parents=True, exist_ok=True)
    silver_push_dir.mkdir(parents=True, exist_ok=True)

    # Determine files to read
    file_pattern = f"event_date=*/events_{hour}.jsonl" if hour else "event_date=*/*.jsonl"
    files_to_read = sorted(bronze_dir.glob(file_pattern))

    raw_records = []
    for jsonl_file in files_to_read:
        with jsonl_file.open("r", encoding="utf-8") as stream:
            for line in stream:
                line = line.strip()
                if line:
                    try:
                        raw_records.append(json.loads(line))
                    except json.JSONDecodeError as error:
                        raise ValueError(f"Malformed record in bronze file {jsonl_file}: {error}") from error

    total_bronze_records = len(raw_records)
    if not total_bronze_records:
        raise ValueError(f"No bronze records found in {bronze_dir} for pattern {file_pattern}")

    deduped_records, conflicts = deduplicate_records(raw_records)
    total_silver_events = len(deduped_records)
    duplicate_count = total_bronze_records - total_silver_events

    # Group records by event_date
    events_by_date = defaultdict(list)
    push_by_date = defaultdict(list)

    for record in deduped_records:
        event_date = extract_event_date(record["created_at"])
        events_by_date[event_date].append(record)
        if record.get("event_type") == "PushEvent":
            push_record = extract_push_event(record)
            if push_record:
                push_by_date[event_date].append(push_record)

    # Idempotent write: clean and write affected partitions
    for event_date, records in events_by_date.items():
        part_dir = silver_events_dir / f"event_date={event_date}"
        if part_dir.exists() and not hour:
            shutil.rmtree(part_dir)
        part_dir.mkdir(parents=True, exist_ok=True)
        out_file = part_dir / (f"events_{hour}.jsonl" if hour else "events.jsonl")
        with out_file.open("w", encoding="utf-8") as stream:
            for rec in records:
                stream.write(json.dumps(rec, ensure_ascii=False) + "\n")

    total_push_events = 0
    for event_date, records in push_by_date.items():
        part_dir = silver_push_dir / f"event_date={event_date}"
        if part_dir.exists() and not hour:
            shutil.rmtree(part_dir)
        part_dir.mkdir(parents=True, exist_ok=True)
        out_file = part_dir / (f"push_events_{hour}.jsonl" if hour else "push_events.jsonl")
        with out_file.open("w", encoding="utf-8") as stream:
            for rec in records:
                stream.write(json.dumps(rec, ensure_ascii=False) + "\n")
        total_push_events += len(records)

    result = {
        "status": "success",
        "zone": "silver",
        "bronze_records_read": total_bronze_records,
        "silver_events_written": total_silver_events,
        "duplicates_removed": duplicate_count,
        "conflicts_detected": len(conflicts),
        "push_events_written": total_push_events,
        "partitions": sorted(events_by_date.keys()),
        "output_dirs": {
            "events": str(silver_events_dir.relative_to(ROOT)) if silver_events_dir.is_relative_to(ROOT) else str(silver_events_dir),
            "push_events": str(silver_push_dir.relative_to(ROOT)) if silver_push_dir.is_relative_to(ROOT) else str(silver_push_dir),
        },
    }
    print(
        f"Silver complete: {total_silver_events} unique events ({duplicate_count} deduped, "
        f"{len(conflicts)} conflicts, {total_push_events} PushEvents) into {len(events_by_date)} partitions"
    )
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "config/dev.yml"), type=Path)
    parser.add_argument("--hour", default=None, help="Process single UTC hour (YYYY-MM-DD-H)")
    args = parser.parse_args(argv)
    build_silver(args.config, args.hour)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
