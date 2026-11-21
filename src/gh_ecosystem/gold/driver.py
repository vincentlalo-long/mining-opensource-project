"""Gold metrics processing driver: computes and materializes daily event volume."""

import argparse
import json
import shutil
from collections import defaultdict
from pathlib import Path

from gh_ecosystem.gold.activity import compute_event_volume_daily
from gh_ecosystem.ingest import load_config

ROOT = Path(__file__).resolve().parents[3]


def build_gold(config_path, hour=None):
    """Build gold.event_volume_daily from silver.events with idempotent date replacement."""
    config = load_config(config_path)
    raw_dir = Path(config["storage"]["raw_dir"])
    raw_dir_resolved = raw_dir if raw_dir.is_absolute() else ROOT / raw_dir
    storage_root = raw_dir_resolved.parent

    silver_events_dir = storage_root / config["storage"]["buckets"]["silver"] / "events"
    if not silver_events_dir.exists():
        raise FileNotFoundError(f"Silver events directory missing: {silver_events_dir}")

    gold_base = storage_root / config["storage"]["buckets"]["gold"]
    gold_metric_dir = gold_base / "event_volume_daily"
    gold_metric_dir.mkdir(parents=True, exist_ok=True)

    file_pattern = f"event_date=*/events_{hour}.jsonl" if hour else "event_date=*/*.jsonl"
    files_to_read = sorted(silver_events_dir.glob(file_pattern))

    silver_records = []
    for jsonl_file in files_to_read:
        with jsonl_file.open("r", encoding="utf-8") as stream:
            for line in stream:
                line = line.strip()
                if line:
                    try:
                        silver_records.append(json.loads(line))
                    except json.JSONDecodeError as error:
                        raise ValueError(f"Malformed record in silver file {jsonl_file}: {error}") from error

    if not silver_records:
        raise ValueError(f"No silver events found in {silver_events_dir} for pattern {file_pattern}")

    daily_metrics = compute_event_volume_daily(silver_records)

    by_date = defaultdict(list)
    for row in daily_metrics:
        by_date[row["event_date"]].append(row)

    total_partitions = 0
    total_metric_rows = 0

    # Idempotent replacement: clean affected partition directory before writing
    for event_date, rows in by_date.items():
        part_dir = gold_metric_dir / f"event_date={event_date}"
        if part_dir.exists() and not hour:
            shutil.rmtree(part_dir)
        part_dir.mkdir(parents=True, exist_ok=True)

        jsonl_out = part_dir / (f"volume_{hour}.jsonl" if hour else "volume.jsonl")
        with jsonl_out.open("w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")

        total_partitions += 1
        total_metric_rows += len(rows)

    result = {
        "status": "success",
        "zone": "gold",
        "metric": "event_volume_daily",
        "total_metric_rows": total_metric_rows,
        "partitions": sorted(by_date.keys()),
        "output_dir": str(gold_metric_dir.relative_to(ROOT)) if gold_metric_dir.is_relative_to(ROOT) else str(gold_metric_dir),
    }
    print(f"Gold complete: {total_metric_rows} metric rows into {total_partitions} partitions at {result['output_dir']}")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "config/dev.yml"), type=Path)
    parser.add_argument("--hour", default=None, help="Process single UTC hour (YYYY-MM-DD-H)")
    args = parser.parse_args(argv)
    build_gold(args.config, args.hour)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
