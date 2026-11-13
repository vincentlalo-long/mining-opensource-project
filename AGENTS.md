# Rules for AI agents in this repository

- Do not create new `.md` files at the repository root. The root holds only `README.md` and `AGENTS.md`.
- Before writing any documentation, read `docs/decisions.md` and **append** there. Never create a new
  document for a decision, an architecture note, a plan or a summary.
- Never create `NOTES.md`, `PLAN.md`, `SUMMARY.md`, `ANALYSIS.md`, `TODO.md`, `ARCHITECTURE.md`,
  `DECISIONS.md` or similar. Delete scratch files before finishing a task.
- Record experiment numbers in `results/experiments.csv`, not as prose. Interpretation goes into the
  final report, written once at the end.
- Logic that is actually used must live in `src/`; a notebook in `notebooks/` is throwaway and must not
  be imported by code in `jobs/`.
- Do not commit, do not push, do not create branches. The team commits manually.
- Do not rename, move or delete files outside the scope of the current task. Do not edit or move
  `docs/reference/BDSP_6_Project_Topics_2026-2.pdf`.
- Do not add dependencies to this repository without asking first.

## Project conventions

- Follow the brief's reference architecture: Spark (PySpark) -> Iceberg/Parquet on MinIO/S3 -> Trino -> Superset,
  deployed on Kubernetes or a cloud cluster.
- Storage zones are `raw`, `bronze`, `silver`, `gold`; tables are `bronze.<entity>`, `silver.<entity>`,
  `gold.<metric>`, lowercase snake_case.
- Bucket names, paths and scale knobs come from `config/*.yml`. No hardcoded paths in code.
- Every number in the report or slides must trace to a row in `results/experiments.csv`. Never invent,
  estimate or extrapolate a benchmark value.
- Code, comments and README in English; `docs/decisions.md` in Vietnamese.

## Commands

Everything runs through `make`; there is no other supported way to run a step.

- `make ingest HOUR=YYYY-MM-DD-H` — one hour of raw archives into `raw/` plus a manifest row.
- `make bronze`, `make silver`, `make gold` — Spark jobs; behaviour comes from `CONFIG`
  (`config/dev.yml` by default, `make gold CONFIG=config/prod.yml` to switch).
- `make all FROM=... TO=...` — the whole pipeline over an hour range.
- `make exp EXP=EXP-001` — run one experiment and append its rows to `results/experiments.csv`.
- `make fault` — kill a worker mid-run and record the recovery.
- `make up`, `make down` — cluster manifests.

A target whose code does not exist yet exits with `not implemented yet: <file>`. When you reach that
step, create that exact file and keep the target as the entrypoint — never add a second way to run the
same step (`python scripts/run_final_v2.py` is wrong).

## Check before finishing

Run `make check`. It prints any markdown file that is outside the allowlist `README.md`, `AGENTS.md`,
`docs/decisions.md`, `docs/report.md`. A printed filename means its content must be merged into the right
file and the stray file deleted.
