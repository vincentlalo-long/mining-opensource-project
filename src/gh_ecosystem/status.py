"""Artifact status for the S0-S5 delivery stages and result-file checks."""

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from gh_ecosystem.ingest import EXPERIMENT_COLUMNS, read_csv


ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Stage:
    code: str
    name: str
    artifacts: tuple[str, ...]


STAGES = (
    Stage("S0", "Contracts & skeleton", (
        "config/dev.yml",
        "src/gh_ecosystem/event_contract_v1.json",
        "results/issue_1_sample.json",
        "results/issue_3_capacity.json",
        "docs/decisions.md",
    )),
    Stage("S1", "Platform & ingest", (
        "k8s/k3d-config.yaml",
        ".github/workflows/ci.yml",
        "src/gh_ecosystem/ingest.py",
        "src/gh_ecosystem/platform.py",
        "jobs/build_bronze.py",
        "jobs/build_silver.py",
        "jobs/build_gold.py",
    )),
    Stage("S2", "Curated data", (
        "src/gh_ecosystem/silver/events.py",
        "src/gh_ecosystem/gold/activity.py",
        "src/gh_ecosystem/gold/community.py",
        "src/gh_ecosystem/storage/parquet.py",
    )),
    Stage("S3", "Serving & dashboard", (
        "k8s/trino/trino.yaml",
        "k8s/superset/superset.yaml",
        "results/issue_2_smoke.json",
        "results/experiments.csv",
    )),
    Stage("S4", "Experiments & fault", (
        "src/gh_ecosystem/experiments.py",
        "src/gh_ecosystem/fault.py",
        "results/experiments.csv",
    )),
    Stage("S5", "Final evidence", (
        "docs/report.md",
        "results/experiments.csv",
        "README.md",
    )),
)


def stage_state(root, stage):
    missing = [path for path in stage.artifacts if not (root / path).exists()]
    if not missing:
        state = "done"
    elif len(missing) == len(stage.artifacts):
        state = "todo"
    else:
        state = "partial"
    return {
        "code": stage.code,
        "name": stage.name,
        "state": state,
        "found": len(stage.artifacts) - len(missing),
        "total": len(stage.artifacts),
        "missing": missing,
    }


def collect(root):
    return [stage_state(root, stage) for stage in STAGES]


def check_results_header(path):
    read_csv(path, EXPERIMENT_COLUMNS)


def print_table(rows):
    print("stage  state    artifacts  missing")
    for row in rows:
        missing = ", ".join(row["missing"]) if row["missing"] else "-"
        print(f"{row['code']:<6} {row['state']:<8} {row['found']}/{row['total']:<7} {missing}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=ROOT, type=Path)
    parser.add_argument("--json", action="store_true", help="print machine-readable status")
    parser.add_argument("--check-results", action="store_true", help="validate results/experiments.csv header")
    parser.add_argument("--results-path", default=None, type=Path)
    args = parser.parse_args(argv)

    if args.check_results:
        path = args.results_path or args.root / "results/experiments.csv"
        try:
            check_results_header(path)
        except (OSError, ValueError) as error:
            print(f"FAIL: results header: {error}")
            return 1
        print("OK: results header")
        return 0

    rows = collect(args.root)
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        print_table(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
