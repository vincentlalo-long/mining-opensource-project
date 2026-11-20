# Mining the Global Open-Source Software Ecosystem

Capstone project 3 — **Big Data Storage and Processing** (School of Information and Communication
Technology, HUST). Team of 3.

Focus: semi-structured JSON, schema design, columnar storage and distributed aggregation.

## Business question

How can a scalable data platform characterize the evolution of the open-source ecosystem and detect
repositories or communities whose activity is changing rapidly?

## Dataset

[GH Archive](https://www.gharchive.org/) hourly public GitHub events (`.json.gz`), processed from the
raw archives ourselves. Recommended scope: 6–24 months, ~30–80 GB. The baseline for the required scale
experiments is 30 days (`2024-01-01-0` to `2024-01-30-23`, ~116M events, ~54 GB compressed); extend
toward 6 months when storage allows. Querying a hosted copy alone does not satisfy the project.

## Reference architecture

```
GH Archive -> Ingestion -> MinIO/S3/HDFS -> Raw JSON -> Spark -> Iceberg/Parquet -> Trino -> Superset
```

Deployment must run on Kubernetes or a cloud environment; a single-machine Docker Compose setup is not
sufficient. Major architecture decisions are recorded in [docs/decisions.md](docs/decisions.md).

## Repository layout (planned)

```
config/          environment config, no hardcoded paths or buckets
src/             package gh_ecosystem: ingest, bronze, silver, gold, serving
jobs/            thin spark-submit entrypoints that call src/
k8s/             manifests / Helm for the cluster deployment
notebooks/       scratch exploration only, never imported by jobs/
results/         experiment measurements (CSV) and generated plots
docs/            decisions log + final report
docs/reference/  course brief and other material to read
```

Data is never committed. The cluster target is MinIO/S3 for the raw, bronze, silver and gold zones;
local ingestion keeps a gitignored raw cache until the MinIO upload lands with the platform issue.

## Team ownership

| Responsibility | A — Data Ingestion & Semi-Structured | B — Distributed Processing & Data Model | C — Platform, Serving & Reliability |
|---|---|---|---|
| Technical axis | Semi-structured data engineering: JSON parsing, schema evolution, columnar encoding, file sizing | Distributed compute: data modeling, partitioning, shuffle/skew, scale-out | Distributed storage and serving: catalog consistency, Trino performance, fault recovery |
| System slice (code) | `src/gh_ecosystem/ingest.py`, `jobs/build_bronze.py`, `src/gh_ecosystem/silver/events.py`, `src/gh_ecosystem/gold/activity.py`, `src/gh_ecosystem/storage/parquet.py`, event schema | `jobs/build_silver.py` (common/typed), `jobs/build_gold.py` (driver), `src/gh_ecosystem/gold/community.py`, silver/gold schemas, partitioning | `k8s/`, MinIO, Iceberg, Trino, Superset, Spark runtime/packaging, `experiments.py`, `fault.py`, serving SQL |
| Hard problems owned | Nested `payload` extraction, malformed/unknown events, schema evolution, Parquet sizing/compaction, ingest idempotency | Dedup keys, partition pruning vs small files, shuffle/skew, join strategy, 10M→100M scale-out | Spark↔Trino catalog consistency, Trino query performance, worker failure/recovery, reproducibility |
| Mandatory analytics | Event volume by type/time; active repositories; temporal distribution | Unique contributors; Push/Issue/PR/Fork patterns; repository growth; contributor concentration | Issue-to-PR relationships; fast-growth detection; student-defined community metric |
| Experiments | JSON vs Parquet (size/scan/latency); Parquet file sizing/compaction; ingest/bronze throughput | Partition layouts A/B; representative query at 10M/50M/100M; Spark execution plan/skew | Trino query performance; fault injection/recovery; measurement harness; cross-check |
| Reviews | B | C | A |
| Members | Nguyễn Trí Hiếu | Phạm Gia Linh | Nguyễn Đăng Long |

Report writing, rubric evidence and end-to-end review are shared by all three owners: everyone contributes,
reviews and can explain the full pipeline.

**Hand-off contracts**

- Bronze contract: A owns `ingest.py` and `build_bronze.py`; `bronze.events` keeps `payload` as a string. A owns event-specific extraction in `silver/events.py`; B owns common silver and the gold driver. Shared schema changes require A + B review.
- Local parity: A and B develop against `config/local.yml` and a 1–2 day fixture; the same code runs on the cluster through `CONFIG`, with no local-only paths.
- Catalog: Spark and Trino share one Iceberg JDBC catalog on a dedicated Postgres database; changes are recorded in `docs/decisions.md`.

**Team rules**

- Reviews are executable: the reviewer runs the acceptance command and checks the CSV row, not just the diff.
- Merge to `main` through a pull request with at least one approval and the `ci` check; do not push directly.
  The repository owner can bypass for an emergency, but the normal path is review first.
- Every owner can run the full `make` pipeline and explain the end-to-end flow.
- Integration work rotates; no owner is the permanent integrator.

## How to run

Copy the local config once, then use `CONFIG=config/local.yml` for local runs:

```bash
cp config/contract-local.example.yml config/local.yml
make help
make inspect HOUR=2024-01-01-0 CONFIG=config/local.yml
make measure-source CONFIG=config/local.yml
make ingest HOUR=2024-01-01-0 CONFIG=config/local.yml
make status
make test
make check
```

`inspect`, `contract-check`, `measure-source`, `capacity-check`, `ingest`, `status`, `doctor`, `up`,
`down` and `test` are implemented. `bronze`, `silver`, `gold`, `all`, `exp` and `fault` still fail with
the file they expect until their issue lands. `config/dev.yml` is the tracked default;
`config/local.yml` is gitignored and may override it.

`make up` needs Docker plus `k3d` and `kubectl`, creates the local `gh` cluster with one server and two
agents, and starts MinIO, Spark, Trino and Superset inside it. It checks ports 9000/8080/8088, uploads and
downloads the smoke hour, and writes `results/issue_2_smoke.json`; it needs about 4 GiB free memory.
Run `make doctor` first, and `make down` to delete the cluster. Cluster secrets are generated on the
first run and are not committed.

## Documentation rules

- Root holds exactly two markdown files: this one and [AGENTS.md](AGENTS.md).
- The course brief is [docs/reference/BDSP_6_Project_Topics_2026-2.pdf](docs/reference/BDSP_6_Project_Topics_2026-2.pdf).
- Working notes, decisions and results go to [docs/decisions.md](docs/decisions.md) and `results/`.
- The final report is written once, at the end, in `docs/report.md`.
