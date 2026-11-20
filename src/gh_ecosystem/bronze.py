"""Build the bronze.events zone from raw GH Archive archives."""

import argparse
import gzip
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from gh_ecosystem.contracts import bronze_projection
from gh_ecosystem.ingest import load_config, parse_hour, read_manifest, utc_now

ROOT = Path(__file__).resolve().parents[2]


def extract_event_date(created_at_str):
    """Extract YYYY-MM-DD partition string from an ISO-8601 timestamp."""
    dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


def build_bronze(config_path, hour=None):
    """Transform raw gzip archives into bronze.events partition dataset."""
    config = load_config(config_path)
    contract_path = config["inspection"]["contract_path"]
    contract = load_config(ROOT / contract_path if not Path(contract_path).is_absolute() else contract_path)

    raw_dir = ROOT / config["storage"]["raw_dir"]
    manifest_path = ROOT / config["inspection"]["manifest_path"]
    manifest = read_manifest(manifest_path)

    hours_to_process = []
    if hour:
        parse_hour(hour)
        hours_to_process.append(hour)
    else:
        for h, row in sorted(manifest.items()):
            if row.get("status") == "ok":
                hours_to_process.append(h)

    if not hours_to_process:
        # Check raw_dir directly if manifest has no entries
        if raw_dir.exists():
            for file in sorted(raw_dir.glob("*.json.gz")):
                hour_name = file.name.replace(".json.gz", "")
                try:
                    parse_hour(hour_name)
                    hours_to_process.append(hour_name)
                except ValueError:
                    continue

    if not hours_to_process:
        raise ValueError("No raw archives found to process into bronze")

    if "bronze_dir" in config["storage"]:
        bronze_dir = Path(config["storage"]["bronze_dir"])
    else:
        bronze_dir = ROOT / "data" / config["storage"]["buckets"]["bronze"] / "events"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    total_events = 0
    partition_counts = defaultdict(int)
    partition_writers = {}
    partition_files = {}

    now_utc = utc_now()

    try:
        for h in hours_to_process:
            file_path = raw_dir / f"{h}.json.gz"
            if not file_path.exists():
                raise FileNotFoundError(f"Raw archive file missing: {file_path}")

            manifest_entry = manifest.get(h, {})
            ingested_at = manifest_entry.get("ingested_at") or now_utc
            source_file = f"data/raw/{h}.json.gz"

            with gzip.open(file_path, "rb") as stream:
                for line in stream:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw_event = json.loads(line)
                        if not isinstance(raw_event, dict):
                            continue
                        record = bronze_projection(raw_event, source_file, ingested_at, contract)
                    except (ValueError, TypeError, KeyError):
                        continue

                    event_date = extract_event_date(record["created_at"])
                    if event_date not in partition_writers:
                        part_dir = bronze_dir / f"event_date={event_date}"
                        part_dir.mkdir(parents=True, exist_ok=True)
                        out_file = part_dir / f"events_{h}.jsonl"
                        partition_files[event_date] = out_file
                        partition_writers[event_date] = out_file.open("w", encoding="utf-8")

                    partition_writers[event_date].write(json.dumps(record, ensure_ascii=False) + "\n")
                    partition_counts[event_date] += 1
                    total_events += 1
    finally:
        for writer in partition_writers.values():
            writer.close()

    result = {
        "status": "success",
        "zone": "bronze",
        "entity": "events",
        "hours_processed": hours_to_process,
        "total_events": total_events,
        "partitions": dict(partition_counts),
        "output_dir": str(bronze_dir.relative_to(ROOT)) if bronze_dir.is_relative_to(ROOT) else str(bronze_dir),
    }
    print(f"Bronze complete: {total_events} events into {len(partition_counts)} partitions at {result['output_dir']}")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "config/dev.yml"), type=Path)
    parser.add_argument("--hour", default=None, help="Process single UTC hour (YYYY-MM-DD-H)")
    args = parser.parse_args(argv)
    build_bronze(args.config, args.hour)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
