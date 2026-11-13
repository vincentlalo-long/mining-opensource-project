# Mining the Global Open-Source Software Ecosystem

Capstone project 3 — **Big Data Storage and Processing** (School of Information and Communication
Technology, HUST). Team of 3, 12 weeks.

Focus: semi-structured JSON, schema design, columnar storage and distributed aggregation.

## Business question

How can a scalable data platform characterize the evolution of the open-source ecosystem and detect
repositories or communities whose activity is changing rapidly?

## Dataset

[GH Archive](https://www.gharchive.org/) hourly public GitHub events (`.json.gz`), processed from the
raw archives ourselves. Target scope: 6–24 months, ~30–80 GB. Querying a hosted copy alone does not
satisfy the project.

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

Data is never committed; object storage (MinIO/S3) holds the raw, bronze, silver and gold zones.

## How to run

`make help` lists the entrypoints (`ingest`, `bronze`, `silver`, `gold`, `all`, `exp`, `fault`, `up`,
`down`); `make check` verifies the documentation rules. The steps are not implemented yet: each target
fails with the file it expects until that milestone is reached (see the 12-week table in the brief).

## Documentation rules

- Root holds exactly two markdown files: this one and [AGENTS.md](AGENTS.md).
- The course brief is [docs/reference/BDSP_6_Project_Topics_2026-2.pdf](docs/reference/BDSP_6_Project_Topics_2026-2.pdf).
- Working notes, decisions and results go to [docs/decisions.md](docs/decisions.md) and `results/`.
- The final report is written once, at the end, in `docs/report.md`.
