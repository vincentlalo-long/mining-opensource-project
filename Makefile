# Single entrypoint for the GH Archive pipeline.
#
# Every step of the project is run through a target below. If a step has no code
# yet, the target fails with the exact file it expects, so that "add a script
# somewhere" is never mistaken for progress. Add new work as a new target here,
# not as a standalone script.

SHELL := /bin/bash
.DEFAULT_GOAL := help

CONFIG  ?= config/dev.yml
HOUR    ?= 2024-01-01-0
SCOPE_HOUR ?=
RESUME  ?= 0
FROM    ?= 2024-01-01-0
TO      ?= 2024-01-01-23
EXP     ?= EXP-001
SMOKE_HOUR ?= 2024-01-01-0
PY      ?= python3
SPARK   ?= spark-submit
KUBECTL ?= kubectl

export PYTHONPATH := src

# Usage: $(call require,path/to/file)  -> fails fast when the file does not exist
define require
	@[ -e $(1) ] || { printf 'not implemented yet: %s\n' '$(1)'; exit 1; }
endef

.PHONY: help status check test inspect contract-check test-contracts measure-source capacity-check test-capacity ingest test-ingest test-status test-bronze test-silver test-gold test-serving bronze silver gold serve all exp fault doctor up down

help: ## list targets
	@printf 'targets (CONFIG=%s):\n' '$(CONFIG)'
	@grep -hE '^[a-z][a-z-]*:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t- /'

status: ## show S0-S5 artifact status (JSON=1 for JSON)
	$(PY) -m gh_ecosystem.status $(if $(filter 1,$(JSON)),--json,)

check: ## markdown allowlist, no data files and results header
	@git ls-files --cached --others --exclude-standard '*.md' \
	| grep -vxE '(README|AGENTS)\.md|docs/(decisions|report)\.md' \
	&& { printf 'FAIL: markdown outside the allowlist (see AGENTS.md)\n'; exit 1; } \
	|| printf 'OK: markdown allowlist\n'
	@git ls-files --cached | grep -E '\.(json\.gz|parquet|avro|orc|zip)$$' \
	&& { printf 'FAIL: data file staged/tracked - see .gitignore\n'; exit 1; } \
	|| printf 'OK: no data files tracked\n'
	@git ls-files --others | grep -vE '^data/' | grep -E '\.(json\.gz|parquet|avro|orc|zip)$$' \
	&& { printf 'FAIL: data file outside data/ is untracked - see .gitignore\n'; exit 1; } \
	|| printf 'OK: no untracked data files outside data/\n'
	@$(PY) -m gh_ecosystem.status --check-results

test: test-contracts test-capacity test-ingest test-status test-bronze test-silver test-gold test-serving ## run all network-free tests

inspect: ## inspect one GH Archive hour and record its schema
	$(call require,src/gh_ecosystem/inspect.py)
	$(PY) -m gh_ecosystem.inspect --hour $(HOUR) --config $(CONFIG)

contract-check: ## validate the data contracts against the inspected hour
	$(call require,src/gh_ecosystem/contracts.py)
	$(PY) -m gh_ecosystem.contracts --config $(CONFIG)

test-contracts: ## run isolated contract and inspection checks without network access
	$(PY) -m unittest discover -s tests -p 'test_contracts.py' -v

measure-source: ## measure exactly 24 consecutive GH Archive hours and plan capacity
	$(call require,src/gh_ecosystem/capacity.py)
	$(PY) -m gh_ecosystem.capacity --from $(FROM) --to $(TO) --config $(CONFIG)

capacity-check: ## verify the measured sample files, CSV totals and capacity assumptions
	$(call require,src/gh_ecosystem/capacity.py)
	$(PY) -m gh_ecosystem.capacity --check --config $(CONFIG)

test-capacity: ## check source measurement and capacity assumptions without network access
	$(PY) -m unittest discover -s tests -p 'test_capacity.py' -v

ingest: ## ingest one raw hour, or recheck/resume manifest hours with RESUME=1
	$(call require,src/gh_ecosystem/ingest.py)
	$(PY) -m gh_ecosystem.ingest $(if $(filter 1,$(RESUME)),--resume,--hour $(HOUR)) --config $(CONFIG)

test-ingest: ## exercise ingestion, retries and resume without a network dependency
	$(PY) -m unittest discover -s tests -p 'test_ingest.py' -v

test-status: ## exercise the status table and results-header check
	$(PY) -m unittest discover -s tests -p 'test_status.py' -v

test-bronze: ## exercise bronze projection, partitioning and payload retention
	$(PY) -m unittest discover -s tests -p 'test_bronze.py' -v

test-silver: ## exercise silver deduplication and typed PushEvent extraction
	$(PY) -m unittest discover -s tests -p 'test_silver.py' -v

test-gold: ## exercise gold daily volume aggregation and metrics
	$(PY) -m unittest discover -s tests -p 'test_gold.py' -v

test-serving: ## exercise Trino/Superset serving orchestration without cluster access
	$(PY) -m unittest discover -s tests -p 'test_serving.py' -v

bronze: ## raw .json.gz -> bronze.events; SCOPE_HOUR=... for one UTC hour
	$(call require,jobs/build_bronze.py)
	$(SPARK) jobs/build_bronze.py --config $(CONFIG) $(if $(SCOPE_HOUR),--hour $(SCOPE_HOUR),)

silver: ## bronze -> silver typed tables, always deduplicated globally
	$(call require,jobs/build_silver.py)
	$(SPARK) jobs/build_silver.py --config $(CONFIG)

gold: ## silver -> gold metric tables, one row per date and event type
	$(call require,jobs/build_gold.py)
	$(SPARK) jobs/build_gold.py --config $(CONFIG)

serve: ## load gold snapshot into Trino Iceberg and create the Superset chart
	$(call require,src/gh_ecosystem/serving.py)
	$(PY) -m gh_ecosystem.serving --config $(CONFIG) --hour $(SMOKE_HOUR)

all: ## run ingest -> bronze -> silver -> gold for the FROM..TO hour range
	$(call require,src/gh_ecosystem/pipeline.py)
	$(PY) -m gh_ecosystem.pipeline --from $(FROM) --to $(TO) --config $(CONFIG)

exp: ## run one experiment and append rows to results/experiments.csv
	$(call require,src/gh_ecosystem/experiments.py)
	$(PY) -m gh_ecosystem.experiments --id $(EXP) --config $(CONFIG)

fault: ## kill one worker during processing, record recovery time
	$(call require,src/gh_ecosystem/fault.py)
	$(PY) -m gh_ecosystem.fault --config $(CONFIG)

doctor: ## check docker, k3d, kubectl, ports, disk, memory and config parity
	$(PY) -m gh_ecosystem.platform --config $(CONFIG) doctor

up: ## create k3d and run MinIO/Spark/Trino/Superset smoke checks
	$(PY) -m gh_ecosystem.platform --config $(CONFIG) up --hour $(SMOKE_HOUR)

down: ## delete the local k3d cluster
	$(PY) -m gh_ecosystem.platform --config $(CONFIG) down
