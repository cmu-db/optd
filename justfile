set shell := ["sh", "-cu"]

cargo := env_var_or_default("CARGO", "cargo")
python := env_var_or_default("PYTHON", "python3")
plot_python := env_var_or_default("PLOT_PYTHON", python)

dataset := env_var_or_default("DATASET", "all")
tpch_queries := env_var_or_default("TPCH_QUERIES", "optd/connectors/datafusion/tests/slt/tpch/results")
job_queries := env_var_or_default("JOB_QUERIES", "optd/connectors/datafusion/tests/slt/job/results")
slt_filter := env_var_or_default("SLT_FILTER", "")
regression_root := env_var_or_default("REGRESSION_ROOT", "target/cardinality-regression")
optd_output := env_var_or_default("OPTD_OUTPUT", regression_root + "/optd")
postgres_output := env_var_or_default("POSTGRES_OUTPUT", regression_root + "/postgres")
optd_extra_args := env_var_or_default("OPTD_EXTRA_ARGS", "")
optd_query_timeout := env_var_or_default("OPTD_QUERY_TIMEOUT", "900")

postgres_container := env_var_or_default("POSTGRES_CONTAINER", "optd-postgres-18")
postgres_database := env_var_or_default("POSTGRES_DATABASE", "optd_bench")
postgres_user := env_var_or_default("POSTGRES_USER", "optd")
postgres_statement_timeout := env_var_or_default("POSTGRES_STATEMENT_TIMEOUT", "300")
postgres_extra_args := env_var_or_default("POSTGRES_EXTRA_ARGS", "")
normalization := env_var_or_default("NORMALIZATION", "all")
tpch_data_dir := env_var_or_default("TPCH_DATA_DIR", "optd/connectors/datafusion/data/tpch/sf-0.1")
job_data_dir := env_var_or_default("JOB_DATA_DIR", "optd/connectors/datafusion/data/job")

# List available recipes and common variable overrides.
[default]
help:
    @printf '%s\n' \
        'Build and verification:' \
        '  just build-binaries       Build all DataFusion connector binaries in release mode' \
        '  just test                 Run the release-mode workspace test suite' \
        '  just test-core            Run optd-core tests' \
        '  just test-core-no-default Run optd-core tests without default features' \
        '  just test-slt             Run DataFusion SQLLogicTests (SLT_FILTER=<filter>)' \
        '  just test-tpch            Run the TPC-H SQLLogicTests' \
        '  just test-job             Run the JOB SQLLogicTests' \
        '  just test-features        Run the feature SQLLogicTests' \
        '  just test-regression      Run focused Rust regression-harness tests' \
        '  just update-baselines     Rewrite expected SLT results (SLT_FILTER=<filter>)' \
        '  just update-tpch-baselines Rewrite expected TPC-H SLT results' \
        '  just update-job-baselines Rewrite expected JOB SLT results' \
        '  just update-feature-baselines Rewrite expected feature SLT results' \
        '  just check                Run formatting, Clippy, and tests' \
        '' \
        'Cardinality regression:' \
        '  just load-postgres-tpch   Load local TPC-H Parquet data into PostgreSQL' \
        '  just load-postgres-job    Load local JOB Parquet data into PostgreSQL' \
        '  just load-postgres-all    Load both benchmark datasets into PostgreSQL' \
        '  just regression-optd      Collect optd per-subtree q-errors' \
        '  just regression-optd-smoke Collect one optd query' \
        '  just regression-postgres  Collect PostgreSQL chosen-plan q-errors' \
        '  just regression-postgres-smoke Collect one PostgreSQL query' \
        '  just regression-compare   Plot and summarize optd versus PostgreSQL' \
        '  just regression-compare-smoke Compare reports that may contain no joins' \
        '  just regression-all       Run both collectors and compare their reports' \
        '  just regression-run       Provision PostgreSQL and run the complete portable workflow' \
        '' \
        'Common overrides (place before the recipe):' \
        '  DATASET=all|tpch|job just regression-optd' \
        '  DATASET=all|tpch|job just regression-postgres' \
        '  TPCH_QUERIES=<path> JOB_QUERIES=<path> just regression-all' \
        '  OPTD_OUTPUT=<dir> POSTGRES_OUTPUT=<dir> just regression-compare' \
        '  OPTD_EXTRA_ARGS="--limit 5 --target-partitions 4" just regression-optd' \
        '  OPTD_QUERY_TIMEOUT=900 just regression-optd' \
        '  POSTGRES_CONTAINER=<name> POSTGRES_DATABASE=<db> POSTGRES_USER=<user> just regression-postgres' \
        '  POSTGRES_EXTRA_ARGS="--limit 5 --resume --continue-on-error" just regression-postgres' \
        '  NORMALIZATION=none|wrappers|row-preserving|joins|all just regression-compare' \
        '  PLOT_PYTHON="conda run -n c0bench python" just regression-compare'

# Build all DataFusion connector binaries in release mode.
build: build-binaries
build-binaries:
    {{cargo}} build --release -p optd-datafusion --bins

# Run the release-mode workspace test suite.
test:
    {{cargo}} test --release --workspace

test-core:
    {{cargo}} test -p optd-core

test-core-no-default:
    {{cargo}} test -p optd-core --no-default-features

test-slt:
    {{cargo}} test --release -p optd-datafusion --test slt -- {{slt_filter}}

test-tpch:
    {{cargo}} test --release -p optd-datafusion --test slt -- tpch

test-job:
    {{cargo}} test --release -p optd-datafusion --test slt -- job/results

test-features:
    {{cargo}} test --release -p optd-datafusion --test slt -- features

test-regression:
    {{cargo}} test --release -p optd-datafusion --lib cardinality_regression

update-baselines:
    {{cargo}} test --release -p optd-datafusion --test slt -- --override {{slt_filter}}

update-tpch-baselines:
    {{cargo}} test --release -p optd-datafusion --test slt -- --override tpch

update-job-baselines:
    {{cargo}} test --release -p optd-datafusion --test slt -- --override job/results

update-feature-baselines:
    {{cargo}} test --release -p optd-datafusion --test slt -- --override features

fmt:
    {{cargo}} fmt --all --check

lint:
    {{cargo}} clippy --workspace --all-targets --locked -- -D warnings

check: fmt lint test

load-postgres-tpch:
    POSTGRES_CONTAINER='{{postgres_container}}' POSTGRES_DB='{{postgres_database}}' POSTGRES_USER='{{postgres_user}}' optd/connectors/datafusion/scripts/load_tpch_postgres.sh '{{tpch_data_dir}}'

load-postgres-job:
    POSTGRES_CONTAINER='{{postgres_container}}' POSTGRES_DB='{{postgres_database}}' POSTGRES_USER='{{postgres_user}}' optd/connectors/datafusion/scripts/load_job_postgres.sh '{{job_data_dir}}'

load-postgres-all: load-postgres-tpch load-postgres-job

regression-optd: build-binaries
    {{python}} optd/connectors/datafusion/scripts/optd_cardinality_regression.py --suite '{{dataset}}' --tpch-queries '{{tpch_queries}}' --job-queries '{{job_queries}}' --output '{{optd_output}}' --query-timeout '{{optd_query_timeout}}' --resume --continue-on-error {{optd_extra_args}}

regression-optd-smoke: build-binaries
    {{python}} optd/connectors/datafusion/scripts/optd_cardinality_regression.py --suite '{{dataset}}' --tpch-queries '{{tpch_queries}}' --job-queries '{{job_queries}}' --output '{{optd_output}}' --limit 1 {{optd_extra_args}}

regression-postgres:
    {{python}} optd/connectors/datafusion/scripts/postgres_cardinality_regression.py --suite '{{dataset}}' --tpch-queries '{{tpch_queries}}' --job-queries '{{job_queries}}' --output '{{postgres_output}}' --container '{{postgres_container}}' --database '{{postgres_database}}' --user '{{postgres_user}}' --statement-timeout '{{postgres_statement_timeout}}' {{postgres_extra_args}}

regression-postgres-smoke:
    {{python}} optd/connectors/datafusion/scripts/postgres_cardinality_regression.py --suite '{{dataset}}' --tpch-queries '{{tpch_queries}}' --job-queries '{{job_queries}}' --output '{{postgres_output}}' --container '{{postgres_container}}' --database '{{postgres_database}}' --user '{{postgres_user}}' --statement-timeout '{{postgres_statement_timeout}}' --limit 1 {{postgres_extra_args}}

regression-compare:
    {{plot_python}} optd/connectors/datafusion/scripts/plot_cardinality_regression.py '{{optd_output}}/report.json' --compare-report '{{postgres_output}}/report.json' --output '{{regression_root}}/comparison' --normalization '{{normalization}}' --suite-matrix --summary

regression-compare-smoke:
    {{plot_python}} optd/connectors/datafusion/scripts/plot_cardinality_regression.py '{{optd_output}}/report.json' --compare-report '{{postgres_output}}/report.json' --output '{{regression_root}}/comparison' --normalization none --summary

regression-all: regression-optd regression-postgres regression-compare

regression-run:
    scripts/run_cardinality_regression.sh --suite '{{dataset}}'
