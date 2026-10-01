SHELL := /bin/sh

CARGO ?= cargo
PYTHON ?= python3
PLOT_PYTHON ?= $(PYTHON)

DATASET ?= tpch
QUERIES ?= optd/connectors/datafusion/tests/slt/tpch/results
SLT_FILTER ?=
REGRESSION_ROOT ?= target/cardinality-regression
OPTD_OUTPUT ?= $(REGRESSION_ROOT)/optd
POSTGRES_OUTPUT ?= $(REGRESSION_ROOT)/postgres
OPTD_EXTRA_ARGS ?=

POSTGRES_CONTAINER ?= optd-postgres-18
POSTGRES_DATABASE ?= optd_bench
POSTGRES_USER ?= optd
POSTGRES_STATEMENT_TIMEOUT ?= 300
POSTGRES_EXTRA_ARGS ?=
NORMALIZATION ?= all
TPCH_DATA_DIR ?= optd/connectors/datafusion/data/tpch/sf-0.1

.PHONY: help build build-binaries test test-core test-core-no-default test-slt \
	test-tpch test-job test-features test-regression update-baselines \
	update-tpch-baselines update-job-baselines update-feature-baselines \
	fmt lint check load-postgres-tpch regression-optd \
	regression-optd-smoke regression-postgres regression-postgres-smoke \
	regression-compare regression-compare-smoke regression-all

help:
	@printf '%s\n' \
		'Build and verification:' \
		'  make build-binaries       Build all DataFusion connector binaries in release mode' \
		'  make test                 Run the release-mode workspace test suite' \
		'  make test-core            Run optd-core tests' \
		'  make test-core-no-default Run optd-core tests without default features' \
		'  make test-slt             Run DataFusion SQLLogicTests (SLT_FILTER=<filter>)' \
		'  make test-tpch            Run the TPC-H SQLLogicTests' \
		'  make test-job             Run the JOB SQLLogicTests' \
		'  make test-features        Run the feature SQLLogicTests' \
		'  make test-regression      Run focused Rust regression-harness tests' \
		'  make update-baselines     Rewrite expected SLT results (SLT_FILTER=<filter>)' \
		'  make update-tpch-baselines Rewrite expected TPC-H SLT results' \
		'  make update-job-baselines Rewrite expected JOB SLT results' \
		'  make update-feature-baselines Rewrite expected feature SLT results' \
		'  make check                Run formatting, Clippy, and tests' \
		'' \
		'Cardinality regression:' \
		'  make load-postgres-tpch   Load local TPC-H Parquet data into PostgreSQL' \
		'  make regression-optd      Collect optd per-subtree q-errors' \
		'  make regression-optd-smoke Collect one optd query' \
		'  make regression-postgres  Collect PostgreSQL chosen-plan q-errors' \
		'  make regression-postgres-smoke Collect one PostgreSQL query' \
		'  make regression-compare   Plot and summarize optd versus PostgreSQL' \
		'  make regression-compare-smoke Compare reports that may contain no joins' \
		'  make regression-all       Run both collectors and compare their reports' \
		'' \
		'Common overrides:' \
		'  DATASET=tpch|job QUERIES=<file-or-directory>' \
		'  OPTD_OUTPUT=<dir> POSTGRES_OUTPUT=<dir>' \
		'  OPTD_EXTRA_ARGS="--limit 5 --target-partitions 4"' \
		'  POSTGRES_CONTAINER=<name> POSTGRES_DATABASE=<db> POSTGRES_USER=<user>' \
		'  POSTGRES_EXTRA_ARGS="--limit 5 --resume --continue-on-error"' \
		'  NORMALIZATION=none|wrappers|row-preserving|joins|all' \
		'  PLOT_PYTHON="conda run -n c0bench python"'

build: build-binaries

build-binaries:
	$(CARGO) build --release -p optd-datafusion --bins

test:
	$(CARGO) test --release --workspace

test-core:
	$(CARGO) test -p optd-core

test-core-no-default:
	$(CARGO) test -p optd-core --no-default-features

test-slt:
	$(CARGO) test --release -p optd-datafusion --test slt -- $(SLT_FILTER)

test-tpch:
	$(MAKE) test-slt SLT_FILTER=tpch

test-job:
	$(MAKE) test-slt SLT_FILTER=job/results

test-features:
	$(MAKE) test-slt SLT_FILTER=features

test-regression:
	$(CARGO) test --release -p optd-datafusion --lib cardinality_regression

update-baselines:
	$(CARGO) test --release -p optd-datafusion --test slt -- --override $(SLT_FILTER)

update-tpch-baselines:
	$(MAKE) update-baselines SLT_FILTER=tpch

update-job-baselines:
	$(MAKE) update-baselines SLT_FILTER=job/results

update-feature-baselines:
	$(MAKE) update-baselines SLT_FILTER=features

fmt:
	$(CARGO) fmt --all --check

lint:
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings

check: fmt lint test

load-postgres-tpch:
	POSTGRES_CONTAINER='$(POSTGRES_CONTAINER)' \
	POSTGRES_DB='$(POSTGRES_DATABASE)' \
	POSTGRES_USER='$(POSTGRES_USER)' \
		optd/connectors/datafusion/scripts/load_tpch_postgres.sh '$(TPCH_DATA_DIR)'

regression-optd: build-binaries
	target/release/cardinality-regression \
		--dataset '$(DATASET)' \
		--queries '$(QUERIES)' \
		--output '$(OPTD_OUTPUT)' $(OPTD_EXTRA_ARGS)

regression-optd-smoke:
	$(MAKE) regression-optd OPTD_EXTRA_ARGS='--limit 1 $(OPTD_EXTRA_ARGS)'

regression-postgres:
	$(PYTHON) optd/connectors/datafusion/scripts/postgres_cardinality_regression.py \
		--queries '$(QUERIES)' \
		--output '$(POSTGRES_OUTPUT)' \
		--container '$(POSTGRES_CONTAINER)' \
		--database '$(POSTGRES_DATABASE)' \
		--user '$(POSTGRES_USER)' \
		--statement-timeout '$(POSTGRES_STATEMENT_TIMEOUT)' $(POSTGRES_EXTRA_ARGS)

regression-postgres-smoke:
	$(MAKE) regression-postgres POSTGRES_EXTRA_ARGS='--limit 1 $(POSTGRES_EXTRA_ARGS)'

regression-compare:
	$(PLOT_PYTHON) optd/connectors/datafusion/scripts/plot_cardinality_regression.py \
		'$(OPTD_OUTPUT)/report.json' \
		--compare-report '$(POSTGRES_OUTPUT)/report.json' \
		--output '$(REGRESSION_ROOT)/comparison' \
		--normalization '$(NORMALIZATION)' \
		--summary

regression-compare-smoke:
	$(MAKE) regression-compare NORMALIZATION=none

regression-all:
	$(MAKE) regression-optd
	$(MAKE) regression-postgres
	$(MAKE) regression-compare
