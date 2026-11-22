"""Build the bronze.events zone from raw GH Archive archives."""

import argparse
import gzip
import json
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

    raw_dir = Path(config["storage"]["raw_dir"])
    raw_dir_resolved = raw_dir if raw_dir.is_absolute() else ROOT / raw_dir
    manifest_path = Path(config["inspection"]["manifest_path"])
    manifest_resolved = manifest_path if manifest_path.is_absolute() else ROOT / manifest_path
    manifest = read_manifest(manifest_resolved)

    hours_to_process = []
    if hour:
        parse_hour(hour)
        hours_to_process.append(hour)
    else:
        for h, row in manifest.items():
            if row.get("status") == "ok":
                try:
                    parse_hour(h)
                    hours_to_process.append(h)
                except ValueError:
                    continue

    if not hours_to_process and raw_dir_resolved.exists():
        for file in raw_dir_resolved.glob("*.json.gz"):
            hour_name = file.name.replace(".json.gz", "")
            try:
                parse_hour(hour_name)
                hours_to_process.append(hour_name)
            except ValueError:
                continue

    if not hours_to_process:
        raise ValueError(f"No raw archives found to process into bronze at {raw_dir}")

    # Canonical chronological sort by parsed UTC datetime
    hours_to_process.sort(key=parse_hour)

    storage_root = raw_dir_resolved.parent
    bronze_dir = storage_root / config["storage"]["buckets"]["bronze"] / "events"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    total_events = 0
    partition_counts = defaultdict(int)
    partition_writers = {}
    now_utc = utc_now()

    try:
        for h in hours_to_process:
            file_path = raw_dir_resolved / f"{h}.json.gz"
            if not file_path.exists():
                raise FileNotFoundError(f"Raw archive file missing: {file_path}")

            manifest_entry = manifest.get(h, {})
            ingested_at = manifest_entry.get("ingested_at") or now_utc
            source_file = manifest_entry.get("local_path") or str(raw_dir / f"{h}.json.gz").replace("\\", "/")

            line_number = 0
            with gzip.open(file_path, "rb") as stream:
                for line in stream:
                    line_number += 1
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw_event = json.loads(line)
                        if not isinstance(raw_event, dict):
                            raise ValueError("Event line must be a JSON object")
                        record = bronze_projection(raw_event, source_file, ingested_at, contract)
                    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as error:
                        raise ValueError(f"Contract violation at line {line_number} in {file_path}: {error}") from error

                    event_date = extract_event_date(record["created_at"])
                    writer_key = (event_date, h)
                    if writer_key not in partition_writers:
                        part_dir = bronze_dir / f"event_date={event_date}"
                        part_dir.mkdir(parents=True, exist_ok=True)
                        out_file = part_dir / f"events_{h}.jsonl"
                        partition_writers[writer_key] = out_file.open("w", encoding="utf-8")

                    partition_writers[writer_key].write(json.dumps(record, ensure_ascii=False) + "\n")
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
