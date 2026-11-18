"""Executable data-contract checks, without Spark or table writes."""

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from gh_ecosystem.ingest import (
    EXPERIMENT_COLUMNS, MANIFEST_COLUMNS, load_config, read_csv, sha256_file,
)
from gh_ecosystem.inspect import profile_archive


def utc_timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Timestamp must be an ISO-8601 string")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.utcoffset() is None:
        raise ValueError("Timestamp must include a timezone")
    return result.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def lookup(event, source):
    value = event
    for part in source.split("."):
        if not isinstance(value, dict):
            raise ValueError(f"Expected an object while reading {source}")
        value = value.get(part)
        if value is None:
            return None
    return value


def bronze_projection(event, source_file, ingested_at, contract):
    """Reference projection for validation only; never creates a bronze table."""
    if not isinstance(event, dict):
        raise ValueError("Raw event must be an object")
    projected = {}
    for name, column in contract["bronze"]["columns"].items():
        source = column["source"]
        value = {"$source_file": source_file, "$ingested_at": ingested_at}.get(source)
        if not source.startswith("$"):
            value = lookup(event, source)
        if value is None:
            if not column["nullable"]:
                raise ValueError(f"Missing required field: {name}")
            projected[name] = None
            continue
        kind = column["type"]
        if column.get("encoding") == "json_object_string":
            if not isinstance(value, dict):
                raise ValueError("Raw payload must be an object")
            value = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if kind == "string":
            if not isinstance(value, str) or not value:
                raise ValueError(f"Expected a nonempty string: {name}")
        elif kind == "long":
            if type(value) is not int or not 0 < value <= 2**63 - 1:
                raise ValueError(f"Expected a positive signed 64-bit integer: {name}")
        elif kind == "timestamp":
            value = utc_timestamp(value)
        else:
            raise ValueError(f"Unsupported contract type: {kind}")
        projected[name] = value
    return projected


def flatten_keys(value, prefix=""):
    result = {}
    for name, item in value.items():
        key = f"{prefix}.{name}" if prefix else name
        if isinstance(item, dict):
            result.update(flatten_keys(item, key))
        else:
            result[key] = item
    return result


def validate_contract(contract, local, cluster):
    expected_columns = {
        "event_id", "event_type", "actor_id", "repo_id", "org_id", "created_at",
        "payload", "ingested_at", "source_file",
    }
    columns = contract["bronze"]["columns"]
    if set(columns) != expected_columns:
        raise ValueError("Bronze common fields do not match the issue contract")
    if columns["payload"] != {
        "source": "payload", "type": "string", "nullable": False,
        "encoding": "json_object_string",
    }:
        raise ValueError("Bronze payload must be a JSON object encoded as a string")
    local_keys, cluster_keys = flatten_keys(local), flatten_keys(cluster)
    if set(local_keys) != set(cluster_keys) or set(local_keys) != set(contract["config_keys"]):
        raise ValueError("Local, cluster and documented config key sets must match")
    types = {"string": str, "integer": int, "boolean": bool}
    for key, spec in contract["config_keys"].items():
        for values in (local_keys, cluster_keys):
            value = values[key]
            if type(value) is not types[spec["type"]]:
                raise ValueError(f"Invalid config type: {key}")
            if isinstance(value, str) and not value:
                raise ValueError(f"Empty config value: {key}")
            if type(value) is int and value <= 0:
                raise ValueError(f"Config integer must be positive: {key}")
    for config in (local, cluster):
        for key, expected in contract["config_invariants"].items():
            if flatten_keys(config)[key] != expected:
                raise ValueError(f"Config invariant mismatch: {key}")
        for secret in ("storage.s3_access_key_env", "storage.s3_secret_key_env",
                       "iceberg.jdbc_user_env", "iceberg.jdbc_password_env"):
            if not re.fullmatch(r"[A-Z][A-Z0-9_]*", flatten_keys(config)[secret]):
                raise ValueError(f"Expected an environment variable name: {secret}")
    seen = set()
    event_types = set()
    for table in [contract["bronze"], *contract["silver"], *contract["gold"]]:
        name = table["name"]
        if not re.fullmatch(r"(bronze|silver|gold)\.[a-z][a-z0-9_]*", name) or name in seen:
            raise ValueError(f"Invalid or duplicate table name: {name}")
        seen.add(name)
        if not table["grain"] or not table["key"] or not table["partition"]:
            raise ValueError(f"Missing grain, key or partition: {name}")
        if table["name"].startswith("gold."):
            if not set(table["key"]).issubset(table["dimensions"]):
                raise ValueError(f"Gold key must be drawn from dimensions: {name}")
            if table["partition"] != ["event_date"]:
                raise ValueError(f"Gold v1 partition must be event_date: {name}")
        elif table["partition"] != ["days(created_at)"]:
            raise ValueError(f"Event v1 partition must be days(created_at): {name}")
        if table.get("event_type"):
            if table["event_type"] in event_types:
                raise ValueError("An event type has multiple typed tables")
            event_types.add(table["event_type"])
    return event_types


def check_sample(config_path):
    config = load_config(config_path)
    inspect = config["inspection"]
    contract = load_config(inspect["contract_path"])
    cluster = load_config(inspect["cluster_config_path"])
    event_types = validate_contract(contract, config, cluster)
    evidence = load_config(inspect["evidence_path"])
    path = Path(evidence["local_path"])
    source_url = f"{config['source']['base_url'].rstrip('/')}/{evidence['hour']}.json.gz"
    if evidence["source_url"] != source_url or path != Path(config["storage"]["raw_dir"]) / f"{evidence['hour']}.json.gz":
        raise ValueError("Sample does not match configured source and raw directory")
    if sha256_file(path, inspect["chunk_bytes"]) != evidence["sha256"]:
        raise ValueError("Sample checksum does not match evidence")
    manifest = [row for row in read_csv(inspect["manifest_path"], MANIFEST_COLUMNS)
                if row["hour"] == evidence["hour"]]
    if len(manifest) != 1 or any(manifest[0][key] != evidence[evidence_key] for key, evidence_key in
                                [("source_url", "source_url"), ("local_path", "local_path"),
                                 ("sha256", "sha256"), ("downloaded_at", "downloaded_at")]):
        raise ValueError("Sample manifest and evidence do not match")
    if int(manifest[0]["compressed_bytes"]) != path.stat().st_size:
        raise ValueError("Manifest byte count does not match sample")
    measured = read_csv(inspect["experiments_path"], EXPERIMENT_COLUMNS)
    run_rows = [row for row in measured if row["run_id"] == evidence["run_id"]]
    metrics = {row["metric"]: row for row in run_rows}
    if len(metrics) != len(run_rows):
        raise ValueError("Duplicate measurements within a run")
    if any(row["source_file"] != evidence["source_url"] or row["config"] != evidence["config"]
           or row["experiment_id"] != "S0-SCHEMA-001" for row in run_rows):
        raise ValueError("Measurements do not match sample provenance")

    def validate(raw, line_number):
        try:
            bronze_projection(raw, evidence["local_path"], evidence["downloaded_at"], contract)
        except (ValueError, TypeError, KeyError) as error:
            raise ValueError(f"Contract violation at input line {line_number}: {error}") from error

    fields, observed, actual = profile_archive(path, evidence["hour"], validate=validate)
    actual["compressed_bytes"] = path.stat().st_size
    for field, types in fields.items():
        actual[f"field.{field}.missing"] = actual["events"] - sum(types.values())
        for kind, count in types.items():
            actual[f"field.{field}.{kind}"] = count
    for event_type, count in observed.items():
        actual[f"event_type.{event_type}"] = count
    for name, count in actual.items():
        if name not in metrics or int(metrics[name]["value"]) != count:
            raise ValueError(f"Sample measurement mismatch: {name}")
    for metric in ("malformed_lines", "invalid_event_id", "invalid_event_type", "invalid_created_at"):
        if int(metrics[metric]["value"]):
            raise ValueError(f"Sample has violations: {metric}")
    if not actual["events"]:
        raise ValueError("Sample is empty")
    if set(observed) - event_types:
        raise ValueError(f"Observed event types need a proposed table: {sorted(set(observed) - event_types)}")
    expected_profile = [{"field": field, "observed_types": "|".join(sorted(types)),
                         "missing_in_sample": str(actual["events"] > sum(types.values())).lower(),
                         "null_in_sample": str(types["null"] > 0).lower()}
                        for field, types in sorted(fields.items())]
    profiled_fields = read_csv(inspect["fields_path"],
                              ["field", "observed_types", "missing_in_sample", "null_in_sample"])
    if profiled_fields != expected_profile:
        raise ValueError("Field profile does not match sample")
    profiled_types = read_csv(inspect["event_types_path"], ["event_type"])
    if list(sorted(observed)) != [row["event_type"] for row in profiled_types]:
        raise ValueError("Event profile does not match sample")
    print(f"PASS: sample projection, measurements, tables and config parity; run_id={evidence['run_id']}")
    print("Checks completed locally; no cluster or table writes were run.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    check_sample(parser.parse_args().config)


if __name__ == "__main__":
    main()
