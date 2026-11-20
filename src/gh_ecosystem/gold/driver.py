"""Gold metrics processing driver: computes and materializes daily event volume."""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from gh_ecosystem.gold.activity import compute_event_volume_daily
from gh_ecosystem.ingest import load_config

ROOT = Path(__file__).resolve().parents[3]


def build_gold(config_path):
    """Build gold.event_volume_daily from silver.events."""
    config = load_config(config_path)
    silver_events_dir = Path(config["storage"]["silver_dir"]) / "events" if "silver_dir" in config["storage"] else ROOT / "data" / config["storage"]["buckets"]["silver"] / "events"
    if not silver_events_dir.exists():
        raise FileNotFoundError(f"Silver events directory missing: {silver_events_dir}")

    gold_base = Path(config["storage"]["gold_dir"]) if "gold_dir" in config["storage"] else ROOT / "data" / config["storage"]["buckets"]["gold"]
    gold_metric_dir = gold_base / "event_volume_daily"
    gold_metric_dir.mkdir(parents=True, exist_ok=True)

    silver_records = []
    for jsonl_file in sorted(silver_events_dir.glob("event_date=*/*.jsonl")):
        with jsonl_file.open("r", encoding="utf-8") as stream:
            for line in stream:
                line = line.strip()
                if line:
                    try:
                        silver_records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

    if not silver_records:
        raise ValueError("No silver events found to compute gold metrics")

    daily_metrics = compute_event_volume_daily(silver_records)

    # Group metrics by event_date partition
    by_date = defaultdict(list)
    for row in daily_metrics:
        by_date[row["event_date"]].append(row)

    total_partitions = 0
    total_metric_rows = 0

    for event_date, rows in by_date.items():
        part_dir = gold_metric_dir / f"event_date={event_date}"
        part_dir.mkdir(parents=True, exist_ok=True)

        jsonl_out = part_dir / "volume.jsonl"
        with jsonl_out.open("w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")

        # Also write CSV in the partition directory for easy querying / inspection
        csv_out = part_dir / "volume.csv"
        with csv_out.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=["event_date", "event_type", "event_count"])
            writer.writeheader()
            writer.writerows(rows)

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
    args = parser.parse_args(argv)
    build_gold(args.config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
