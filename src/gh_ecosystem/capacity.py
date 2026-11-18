"""Measure a complete 24-hour raw sample, separating observations from planning assumptions."""

import argparse
import csv
import json
import math
import time
import uuid
from datetime import timedelta
from pathlib import Path

from gh_ecosystem.ingest import (
    EXPERIMENT_COLUMNS, INGEST_ERRORS, acquire_sample, load_config, parse_hour,
    read_csv, read_manifest, sha256_file, utc_now,
)
from gh_ecosystem.inspect import profile_archive


def hourly_range(first, last, required_hours):
    start, end = parse_hour(first), parse_hour(last)
    hours = int((end - start).total_seconds() // 3600) + 1
    if hours != required_hours:
        raise ValueError(f"Expected exactly {required_hours} consecutive hours, got {hours}")
    return [f"{stamp:%Y-%m-%d}-{stamp.hour}"
            for stamp in (start + timedelta(hours=index) for index in range(hours))]


def planning_values(compressed_bytes, events, days, budget):
    """Linear capacity assumptions only; these are never measured benchmark values."""
    plans = {day: {"raw_bytes": compressed_bytes * day, "events": events * day} for day in days}
    fitting = [day for day in days if plans[day]["raw_bytes"] <= budget]
    if not fitting:
        raise ValueError("Even the measured daily raw sample exceeds the configured raw budget")
    return plans, max(fitting)


def measure_source(config_path, first, last):
    config = load_config(config_path)
    capacity = config["capacity"]
    hours = hourly_range(first, last, capacity["sample_hours"])
    if capacity["sample_hours"] != 24:
        raise ValueError("This issue requires a 24-hour sample")
    output = Path(config["inspection"]["experiments_path"])
    read_csv(output, EXPERIMENT_COLUMNS)
    run_id = str(uuid.uuid4())
    recorded_at = utc_now()
    source_range = f"{config['source']['base_url'].rstrip('/')}/{first}..{last}"
    output.parent.mkdir(parents=True, exist_ok=True)
    exists = output.exists()
    evidence_path = Path(capacity["evidence_path"])
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    totals = {"files": 0, "compressed_bytes": 0, "uncompressed_bytes": 0, "events": 0,
              "failures": 0, "cache_reused_files": 0, "downloaded_files": 0,
              "new_download_and_validation_seconds": 0.0, "scan_seconds": 0.0}
    failures = []
    started = time.perf_counter()
    with output.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=EXPERIMENT_COLUMNS)
        if not exists:
            writer.writeheader()

        def measure(metric, value, unit, source=source_range):
            writer.writerow(dict(zip(EXPERIMENT_COLUMNS, [
                "EXP-001", run_id, recorded_at, Path(config_path).as_posix(),
                source, metric, value, unit,
            ])))
            stream.flush()

        for hour in hours:
            print(f"Measuring {hour}", flush=True)
            source = f"{config['source']['base_url'].rstrip('/')}/{hour}.json.gz"
            try:
                path, manifest, download_seconds, cached = acquire_sample(config, hour)
                scan_started = time.perf_counter()
                _, _, stats = profile_archive(path, hour)
                scan_seconds = time.perf_counter() - scan_started
                if not stats["events"] or stats["malformed_lines"]:
                    raise ValueError("Archive is empty or has malformed JSON events")
                totals["files"] += 1
                totals["compressed_bytes"] += int(manifest["compressed_bytes"])
                totals["uncompressed_bytes"] += stats["uncompressed_bytes"]
                totals["events"] += stats["events"]
                totals["cache_reused_files"] += int(cached)
                totals["downloaded_files"] += int(not cached)
                totals["scan_seconds"] += scan_seconds
                for metric, value, unit in [
                    ("compressed_bytes", manifest["compressed_bytes"], "bytes"),
                    ("uncompressed_bytes", stats["uncompressed_bytes"], "bytes"),
                    ("events", stats["events"], "count"),
                    ("scan_seconds", scan_seconds, "seconds"),
                    ("cache_reused", int(cached), "boolean"),
                    ("failures", 0, "count"),
                ]:
                    measure(f"measured.hour.{metric}", value, unit, source)
                if download_seconds is not None:
                    totals["new_download_and_validation_seconds"] += download_seconds
                    measure("measured.hour.download_and_gzip_validation_seconds", download_seconds, "seconds", source)
                print(f"OK {hour}: measurements appended", flush=True)
            except INGEST_ERRORS as error:
                totals["failures"] += 1
                failures.append({"hour": hour, "error": str(error)})
                measure("measured.hour.failures", 1, "count", source)
                print(f"FAILED {hour}: {error}", flush=True)

        totals["wall_seconds"] = time.perf_counter() - started
        for key, value in totals.items():
            unit = "seconds" if key.endswith("seconds") else "bytes" if key.endswith("bytes") else "count"
            measure(f"measured.total.{key}", value, unit)
        evidence = {
            "issue": "https://github.com/vincentlalo-long/mining-opensource-project/issues/3",
            "run_id": run_id, "config": Path(config_path).as_posix(),
            "sample_from": first, "sample_to": last,
            "manifest_path": config["inspection"]["manifest_path"],
            "experiments_path": output.as_posix(), "failed_hours": failures,
            "status": "incomplete" if failures else "complete",
            "measurement_note": "Download time covers newly downloaded files only; cache hits are separately counted. Scan time is measured locally, not on Spark/Trino.",
            "planning_note": "planning.* rows are linear capacity assumptions based on this observed day; not benchmarks or guaranteed future volume. Compressed raw bytes only; curated storage and Spark processing cost are not measured.",
        }
        if not failures:
            plans, selected_days = planning_values(totals["compressed_bytes"], totals["events"],
                                                  capacity["planning_days"], capacity["raw_budget_bytes"])
            measure("planning.raw_budget_bytes", capacity["raw_budget_bytes"], "bytes_assumption")
            for day, plan in plans.items():
                measure(f"planning.{day}_days.raw_bytes", plan["raw_bytes"], "bytes_assumption")
                measure(f"planning.{day}_days.events", plan["events"], "events_assumption")
                measure(f"planning.{day}_days.same_environment_scan_seconds", totals["scan_seconds"] * day, "seconds_assumption")
            measure("planning.selected_days", selected_days, "days_assumption")
            target_end = parse_hour(first) + timedelta(days=selected_days, hours=-1)
            evidence["recommended_range"] = {
                "from": first, "to": f"{target_end:%Y-%m-%d}-{target_end.hour}",
                "basis": "Largest configured planning horizon fitting the assumed raw-only budget; verify curated storage and distributed processing separately before backfill.",
            }
        evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    if failures:
        raise ValueError(f"Incomplete sample; see {evidence_path} and rerun to reuse verified files")
    print(f"Capacity evidence: {evidence_path}; run_id={run_id}", flush=True)
    return evidence


def check_capacity(config_path):
    config = load_config(config_path)
    evidence = load_config(config["capacity"]["evidence_path"])
    if evidence["status"] != "complete" or evidence["failed_hours"]:
        raise ValueError("Cannot accept an incomplete daily sample")
    hours = hourly_range(evidence["sample_from"], evidence["sample_to"], 24)
    measured = [row for row in read_csv(config["inspection"]["experiments_path"], EXPERIMENT_COLUMNS)
                if row["run_id"] == evidence["run_id"]]
    indexed = {(row["source_file"], row["metric"]): row for row in measured}
    if len(indexed) != len(measured) or any(row["experiment_id"] != "EXP-001" for row in measured):
        raise ValueError("Duplicate or misidentified daily measurements")
    manifest = read_manifest(config["inspection"]["manifest_path"])
    source_range = f"{config['source']['base_url'].rstrip('/')}/{evidence['sample_from']}..{evidence['sample_to']}"
    totals = {"files": 0, "events": 0, "compressed_bytes": 0, "uncompressed_bytes": 0,
              "cache_reused_files": 0, "downloaded_files": 0, "scan_seconds": 0.0,
              "new_download_and_validation_seconds": 0.0, "failures": 0}

    def value(source, metric):
        try:
            return float(indexed[(source, metric)]["value"])
        except KeyError as error:
            raise ValueError(f"Missing daily measurement: {metric}") from error

    for hour in hours:
        row = manifest.get(hour)
        if row is None:
            raise ValueError(f"Missing sample manifest hour: {hour}")
        if row["status"] != "ok":
            raise ValueError(f"Sample manifest hour is not ok: {hour}")
        path = Path(config["storage"]["raw_dir"]) / f"{hour}.json.gz"
        source = f"{config['source']['base_url'].rstrip('/')}/{hour}.json.gz"
        if row["source_url"] != source or row["local_path"] != path.as_posix():
            raise ValueError(f"Manifest provenance mismatch: {hour}")
        if path.stat().st_size != int(row["compressed_bytes"]) or sha256_file(path, config["inspection"]["chunk_bytes"]) != row["sha256"]:
            raise ValueError(f"Manifest checksum/size mismatch: {hour}")
        totals["files"] += 1
        for metric in ("events", "compressed_bytes", "uncompressed_bytes", "scan_seconds", "failures"):
            measured_value = value(source, f"measured.hour.{metric}")
            if measured_value < 0:
                raise ValueError(f"Negative measurement: {metric}")
            totals[metric] += measured_value
        if value(source, "measured.hour.compressed_bytes") != path.stat().st_size:
            raise ValueError(f"Measured bytes do not match file: {hour}")
        cached = value(source, "measured.hour.cache_reused")
        if cached not in (0, 1):
            raise ValueError("Invalid cache flag")
        totals["cache_reused_files"] += cached
        totals["downloaded_files"] += 1 - cached
        if not cached:
            totals["new_download_and_validation_seconds"] += value(source, "measured.hour.download_and_gzip_validation_seconds")
    for metric, expected in totals.items():
        if not math.isclose(value(source_range, f"measured.total.{metric}"), expected, abs_tol=1e-9):
            raise ValueError(f"Daily total mismatch: {metric}")
    if totals["failures"]:
        raise ValueError("Daily sample contains failures")
    capacity = config["capacity"]
    plans, selected = planning_values(totals["compressed_bytes"], totals["events"],
                                      capacity["planning_days"], capacity["raw_budget_bytes"])

    def check_assumption(name, expected, unit):
        metric = f"planning.{name}"
        recorded = value(source_range, metric)
        if (not math.isclose(recorded, expected, rel_tol=0, abs_tol=1e-9)
                or indexed[(source_range, metric)]["unit"] != unit):
            raise ValueError(f"Capacity assumption mismatch: {metric}")

    check_assumption("raw_budget_bytes", capacity["raw_budget_bytes"], "bytes_assumption")
    for day, plan in plans.items():
        prefix = f"{day}_days"
        check_assumption(f"{prefix}.raw_bytes", plan["raw_bytes"], "bytes_assumption")
        check_assumption(f"{prefix}.events", plan["events"], "events_assumption")
        check_assumption(f"{prefix}.same_environment_scan_seconds",
                         totals["scan_seconds"] * day, "seconds_assumption")
    check_assumption("selected_days", selected, "days_assumption")
    expected_end = parse_hour(evidence["sample_from"]) + timedelta(days=selected, hours=-1)
    recommendation = evidence["recommended_range"]
    if recommendation["from"] != evidence["sample_from"] or recommendation["to"] != f"{expected_end:%Y-%m-%d}-{expected_end.hour}":
        raise ValueError("Recommended range does not match the selected horizon")
    print(f"PASS: daily manifest, archive checksums, CSV totals and assumptions; run_id={evidence['run_id']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="first")
    parser.add_argument("--to", dest="last")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    if args.check:
        check_capacity(args.config)
    elif args.first and args.last:
        measure_source(args.config, args.first, args.last)
    else:
        parser.error("Supply --from and --to, or use --check")


if __name__ == "__main__":
    main()
