"""Load the local gold snapshot into Trino Iceberg and bootstrap one Superset chart."""

import argparse
import base64
import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

from gh_ecosystem.ingest import load_config, parse_hour

ROOT = Path(__file__).resolve().parents[2]
NAMESPACE = "gh"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "gold"
TRINO_TABLE = "event_volume_daily"


def run(command, input_text=None):
    result = subprocess.run(command, input=input_text, text=True, capture_output=True)
    if result.returncode:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(command)}\n{detail}")
    return result.stdout.strip()


def trino(sql):
    return run(["kubectl", "-n", NAMESPACE, "exec", "deploy/trino", "--",
                "trino", "--output-format", "TSV", "--execute", sql])


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def gold_snapshot_path(config, hour):
    parsed_hour = parse_hour(hour)
    raw_dir = Path(config["storage"]["raw_dir"])
    raw_dir_resolved = raw_dir if raw_dir.is_absolute() else ROOT / raw_dir
    storage_root = raw_dir_resolved.parent
    event_date = parsed_hour.strftime("%Y-%m-%d")
    return storage_root / config["storage"]["buckets"]["gold"] / "event_volume_daily" / f"event_date={event_date}" / "volume.jsonl"


def load_gold(config_path, hour):
    config = load_config(config_path)
    path = gold_snapshot_path(config, hour)
    if not path.exists():
        raise FileNotFoundError(f"Gold snapshot missing: {path}")
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not rows:
        raise ValueError(f"Gold snapshot is empty: {path}")
    return rows


def load_into_trino(rows, file_format="PARQUET"):
    trino(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}")
    trino(f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}")
    trino(
        f"CREATE TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} ("
        "event_date date, event_type varchar, event_count bigint"
        f") WITH (format = {sql_literal(file_format)}, partitioning = ARRAY['event_date'])"
    )
    values = ", ".join(
        f"(DATE {sql_literal(row['event_date'])}, {sql_literal(row['event_type'])}, {int(row['event_count'])})"
        for row in rows
    )
    trino(f"INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} VALUES {values}")
    output = trino(
        f"SELECT count(*), coalesce(sum(event_count), 0), count(DISTINCT event_date) "
        f"FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}"
    )
    lines = [line for line in output.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("Trino verification query returned empty output")
    fields = lines[-1].split("\t")
    if len(fields) != 3:
        raise RuntimeError(f"Unexpected Trino verification output: {output!r}")
    return {"rows": int(fields[0]), "event_count": int(fields[1]), "dates": int(fields[2])}


def bootstrap_superset():
    script = (ROOT / "k8s/superset/bootstrap.py").read_text(encoding="utf-8")
    output = run(["kubectl", "-n", NAMESPACE, "exec", "-i", "deploy/superset", "--", "python", "-"], script)
    marker = "SERVING_BOOTSTRAP_JSON="
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            return json.loads(base64.b64decode(line[len(marker):]).decode())
    raise RuntimeError(f"Superset bootstrap output missing JSON marker:\n{output[-4000:]}")


def serve(config_path, hour):
    config_path = Path(config_path)
    config = load_config(config_path)
    gold_path = gold_snapshot_path(config, hour)
    rows = load_gold(config_path, hour)
    gold_total = sum(int(row["event_count"]) for row in rows)
    file_format = str(config.get("iceberg", {}).get("file_format", "parquet")).upper()
    trino_result = load_into_trino(rows, file_format=file_format)
    if trino_result["rows"] != len(rows) or trino_result["event_count"] != gold_total:
        raise RuntimeError(f"Trino verification mismatch: local={len(rows)}/{gold_total}, trino={trino_result}")

    superset_result = bootstrap_superset()
    if superset_result.get("chart_rows", 0) <= 0:
        raise RuntimeError(f"Superset verification returned no rows: {superset_result}")

    resolved_config = config_path.resolve()
    config_label = str(resolved_config.relative_to(ROOT)) if resolved_config.is_relative_to(ROOT) else str(config_path)
    resolved_gold = gold_path.resolve()
    gold_label = str(resolved_gold.relative_to(ROOT)) if resolved_gold.is_relative_to(ROOT) else str(gold_path)
    evidence = {
        "issue": "https://github.com/vincentlalo-long/mining-opensource-project/issues/4",
        "run_id": str(uuid.uuid4()),
        "recorded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "config": config_label,
        "source_hour": hour,
        "gold_file": gold_label,
        "gold_rows": len(rows),
        "gold_event_count": gold_total,
        "trino": {
            "catalog": TRINO_CATALOG,
            "schema": TRINO_SCHEMA,
            "table": TRINO_TABLE,
            **trino_result,
        },
        "superset": superset_result,
        "status": "complete",
    }
    path = ROOT / "results/issue_4_serving.json"
    path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(f"Serving evidence: {path.relative_to(ROOT)}")
    print(f"Trino: {trino_result['rows']} rows / {trino_result['event_count']} events")
    print(f"Superset chart: {superset_result.get('chart_url')}")
    return evidence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "config/dev.yml"), type=Path)
    parser.add_argument("--hour", default="2024-01-01-0")
    args = parser.parse_args(argv)
    serve(args.config, args.hour)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
