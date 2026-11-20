"""Silver processing driver: global deduplication and typed event extraction."""

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from gh_ecosystem.bronze import extract_event_date
from gh_ecosystem.ingest import load_config
from gh_ecosystem.silver.events import extract_push_event

ROOT = Path(__file__).resolve().parents[3]


def payload_hash(payload_str):
    if isinstance(payload_str, dict):
        payload_str = json.dumps(payload_str, sort_keys=True)
    return hashlib.sha256(str(payload_str).encode("utf-8")).hexdigest()


def deduplicate_records(records):
    """Global deduplication by event_id with tie-break ordering.
    
    Tie-break rule: earliest ingested_at, then source_file, then content hash.
    """
    grouped = defaultdict(list)
    for record in records:
        event_id = str(record["event_id"])
        grouped[event_id].append(record)

    deduped = []
    for event_id, candidates in grouped.items():
        if len(candidates) == 1:
            deduped.append(candidates[0])
        else:
            candidates.sort(
                key=lambda r: (
                    r.get("ingested_at", ""),
                    r.get("source_file", ""),
                    payload_hash(r.get("payload", "")),
                )
            )
            deduped.append(candidates[0])
    return deduped


def build_silver(config_path):
    """Build silver.events and silver.push_events from bronze zone."""
    config = load_config(config_path)
    bronze_dir = Path(config["storage"]["bronze_dir"]) if "bronze_dir" in config["storage"] else ROOT / "data" / config["storage"]["buckets"]["bronze"] / "events"
    if not bronze_dir.exists():
        raise FileNotFoundError(f"Bronze events directory missing: {bronze_dir}")

    silver_base = Path(config["storage"]["silver_dir"]) if "silver_dir" in config["storage"] else ROOT / "data" / config["storage"]["buckets"]["silver"]
    silver_events_dir = silver_base / "events"
    silver_push_dir = silver_base / "push_events"
    silver_events_dir.mkdir(parents=True, exist_ok=True)
    silver_push_dir.mkdir(parents=True, exist_ok=True)

    # Read all bronze records
    raw_records = []
    for jsonl_file in sorted(bronze_dir.glob("event_date=*/*.jsonl")):
        with jsonl_file.open("r", encoding="utf-8") as stream:
            for line in stream:
                line = line.strip()
                if line:
                    try:
                        raw_records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

    total_bronze_records = len(raw_records)
    if not total_bronze_records:
        raise ValueError("No bronze records found to build silver tables")

    deduped_records = deduplicate_records(raw_records)
    total_silver_events = len(deduped_records)
    duplicate_count = total_bronze_records - total_silver_events

    # Group and write silver.events and silver.push_events partitioned by event_date
    events_by_date = defaultdict(list)
    push_by_date = defaultdict(list)

    for record in deduped_records:
        event_date = extract_event_date(record["created_at"])
        events_by_date[event_date].append(record)
        if record.get("event_type") == "PushEvent":
            push_record = extract_push_event(record)
            if push_record:
                push_by_date[event_date].append(push_record)

    for event_date, records in events_by_date.items():
        part_dir = silver_events_dir / f"event_date={event_date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_file = part_dir / "events.jsonl"
        with out_file.open("w", encoding="utf-8") as stream:
            for rec in records:
                stream.write(json.dumps(rec, ensure_ascii=False) + "\n")

    total_push_events = 0
    for event_date, records in push_by_date.items():
        part_dir = silver_push_dir / f"event_date={event_date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_file = part_dir / "push_events.jsonl"
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
        "push_events_written": total_push_events,
        "partitions": sorted(events_by_date.keys()),
        "output_dirs": {
            "events": str(silver_events_dir.relative_to(ROOT)) if silver_events_dir.is_relative_to(ROOT) else str(silver_events_dir),
            "push_events": str(silver_push_dir.relative_to(ROOT)) if silver_push_dir.is_relative_to(ROOT) else str(silver_push_dir),
        },
    }
    print(
        f"Silver complete: {total_silver_events} unique events ({duplicate_count} deduped, "
        f"{total_push_events} PushEvents) into {len(events_by_date)} partitions"
    )
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "config/dev.yml"), type=Path)
    args = parser.parse_args(argv)
    build_silver(args.config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
