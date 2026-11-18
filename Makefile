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
FROM    ?= 2024-01-01-0
TO      ?= 2024-01-01-23
EXP     ?= EXP-001
PY      ?= python3
SPARK   ?= spark-submit
KUBECTL ?= kubectl

export PYTHONPATH := src

# Usage: $(call require,path/to/file)  -> fails fast when the file does not exist
define require
	@[ -e $(1) ] || { printf 'not implemented yet: %s\n' '$(1)'; exit 1; }
endef

.PHONY: help check inspect contract-check test-contracts measure-source capacity-check test-capacity ingest bronze silver gold all exp fault up down

help: ## list targets
	@printf 'targets (CONFIG=%s):\n' '$(CONFIG)'
	@grep -hE '^[a-z][a-z-]*:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t- /'

check: ## markdown allowlist + no staged data; run before finishing any task
	@git ls-files --cached --others --exclude-standard '*.md' \
	| grep -vxE '(README|AGENTS)\.md|docs/(decisions|report)\.md' \
	&& { printf 'FAIL: markdown outside the allowlist (see AGENTS.md)\n'; exit 1; } \
	|| printf 'OK: markdown allowlist\n'
	@git ls-files --cached | grep -E '\.(json\.gz|parquet|avro|orc|zip)$$' \
	&& { printf 'FAIL: data file staged/tracked - see .gitignore\n'; exit 1; } \
	|| printf 'OK: no data files tracked\n'

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

ingest: ## download one hour of raw archives into raw/ plus manifest row (W3)
	$(call require,src/gh_ecosystem/ingest.py)
	$(PY) -m gh_ecosystem.ingest --hour $(HOUR) --config $(CONFIG)

bronze: ## raw .json.gz -> bronze.events, payload kept as string (W4-W5)
	$(call require,jobs/build_bronze.py)
	$(SPARK) jobs/build_bronze.py --config $(CONFIG)

silver: ## bronze -> silver typed tables, deduplicated (W5-W6)
	$(call require,jobs/build_silver.py)
	$(SPARK) jobs/build_silver.py --config $(CONFIG)

gold: ## silver -> gold metric tables for the dashboard (W6-W7)
	$(call require,jobs/build_gold.py)
	$(SPARK) jobs/build_gold.py --config $(CONFIG)

all: ## run ingest -> bronze -> silver -> gold for the FROM..TO hour range
	$(call require,src/gh_ecosystem/pipeline.py)
	$(PY) -m gh_ecosystem.pipeline --from $(FROM) --to $(TO) --config $(CONFIG)

exp: ## run one experiment and append rows to results/experiments.csv (W10)
	$(call require,src/gh_ecosystem/experiments.py)
	$(PY) -m gh_ecosystem.experiments --id $(EXP) --config $(CONFIG)

fault: ## kill one worker during processing, record recovery time (W11)
	$(call require,src/gh_ecosystem/fault.py)
	$(PY) -m gh_ecosystem.fault --config $(CONFIG)

up: ## apply the cluster manifests (Spark, Trino, Superset)
	$(call require,k8s/kustomization.yaml)
	$(KUBECTL) apply -k k8s/

down: ## remove the cluster manifests
	$(call require,k8s/kustomization.yaml)
	$(KUBECTL) delete -k k8s/
