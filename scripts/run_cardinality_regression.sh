#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/run_cardinality_regression.sh [options]

Provision PostgreSQL, load benchmark Parquet data, run optd and PostgreSQL
cardinality collectors, generate suite-aware plots, and build the dashboard.

Options:
  --suite all|tpch|job  Suite selection (default: job)
  --skip-download       Require Parquet files to already exist
  --skip-load           Reuse tables already loaded in PostgreSQL
  --skip-optd           Do not run the optd collector
  --skip-postgres       Do not run the PostgreSQL collector
  --help                Show this help

Environment overrides:
  POSTGRES_CONTAINER          Container name (default: optd-postgres-18)
  POSTGRES_IMAGE              Image (default: postgres:18.6-bookworm)
  POSTGRES_PORT               Host port (default: 55432)
  POSTGRES_DATABASE          Database (default: optd_bench)
  POSTGRES_USER              User (default: optd)
  POSTGRES_PASSWORD          Password (default: optd)
  POSTGRES_DATA_DIR          Bind-mount directory (default: data/optd-pg18-data)
  REGRESSION_ROOT            Output directory (default: target/cardinality-regression)
  OPTD_QUERY_TIMEOUT         Per-query optd timeout in seconds (default: 900)
  POSTGRES_STATEMENT_TIMEOUT Per-query PostgreSQL timeout in seconds (default: 300)
  PYTHON                     Python command (default: python3)
  PLOT_PYTHON                Plotting Python command; may include arguments
  CARGO                      Cargo command (default: cargo)
EOF
}

suite="job"
download=true
load_data=true
run_optd=true
run_postgres=true
while (($#)); do
    case "$1" in
        --suite)
            [[ $# -ge 2 ]] || { echo "error: --suite requires a value" >&2; exit 2; }
            suite="$2"
            shift 2
            ;;
        --skip-download) download=false; shift ;;
        --skip-load) load_data=false; shift ;;
        --skip-optd) run_optd=false; shift ;;
        --skip-postgres) run_postgres=false; shift ;;
        --help|-h) usage; exit 0 ;;
        *) echo "error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done
case "${suite}" in all|tpch|job) ;; *) echo "error: invalid suite: ${suite}" >&2; exit 2 ;; esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"

container="${POSTGRES_CONTAINER:-optd-postgres-18}"
image="${POSTGRES_IMAGE:-postgres:18.6-bookworm}"
port="${POSTGRES_PORT:-55432}"
database="${POSTGRES_DATABASE:-optd_bench}"
user="${POSTGRES_USER:-optd}"
password="${POSTGRES_PASSWORD:-optd}"
postgres_data_dir="${POSTGRES_DATA_DIR:-data/optd-pg18-data}"
regression_root="${REGRESSION_ROOT:-target/cardinality-regression}"
optd_timeout="${OPTD_QUERY_TIMEOUT:-900}"
postgres_timeout="${POSTGRES_STATEMENT_TIMEOUT:-300}"
tpch_data_dir="${TPCH_DATA_DIR:-optd/connectors/datafusion/data/tpch/sf-0.1}"
job_data_dir="${JOB_DATA_DIR:-optd/connectors/datafusion/data/job}"
tpch_queries="${TPCH_QUERIES:-optd/connectors/datafusion/tests/slt/tpch/results}"
job_queries="${JOB_QUERIES:-optd/connectors/datafusion/tests/slt/job/results}"

read -r -a python_cmd <<<"${PYTHON:-python3}"
read -r -a plot_python_cmd <<<"${PLOT_PYTHON:-${PYTHON:-python3}}"
read -r -a cargo_cmd <<<"${CARGO:-cargo}"

for command in docker duckdb curl git; do
    command -v "${command}" >/dev/null 2>&1 || {
        echo "error: ${command} is required" >&2
        exit 1
    }
done
command -v "${python_cmd[0]}" >/dev/null 2>&1 || { echo "error: ${python_cmd[0]} is required" >&2; exit 1; }
command -v "${plot_python_cmd[0]}" >/dev/null 2>&1 || { echo "error: ${plot_python_cmd[0]} is required" >&2; exit 1; }
command -v "${cargo_cmd[0]}" >/dev/null 2>&1 || { echo "error: ${cargo_cmd[0]} is required" >&2; exit 1; }

mkdir -p "${regression_root}/logs"
log_path="${regression_root}/logs/external-$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee -a "${log_path}") 2>&1

echo "suite=${suite}"
echo "repository=${repo_root}"
echo "output=${regression_root}"
echo "log=${log_path}"

if ! docker inspect "${container}" >/dev/null 2>&1; then
    mkdir -p "${postgres_data_dir}"
    postgres_data_dir="$(cd "${postgres_data_dir}" && pwd)"
    echo "creating PostgreSQL container ${container} from ${image}"
    docker run -d \
        --name "${container}" \
        -e "POSTGRES_USER=${user}" \
        -e "POSTGRES_PASSWORD=${password}" \
        -e "POSTGRES_DB=${database}" \
        -p "${port}:5432" \
        -v "${postgres_data_dir}:/var/lib/postgresql" \
        "${image}" >/dev/null
elif [[ "$(docker inspect -f '{{.State.Running}}' "${container}")" != "true" ]]; then
    echo "starting PostgreSQL container ${container}"
    docker start "${container}" >/dev/null
else
    echo "PostgreSQL container ${container} is already running"
fi

for _ in $(seq 1 60); do
    if docker exec "${container}" pg_isready -q -U "${user}" -d "${database}"; then
        break
    fi
    sleep 1
done
docker exec "${container}" pg_isready -q -U "${user}" -d "${database}" || {
    echo "error: PostgreSQL did not become ready" >&2
    exit 1
}

if [[ "${download}" == true ]]; then
    case "${suite}" in
        all|tpch) scripts/download_tpch_hf.sh "${tpch_data_dir}" ;;
    esac
    case "${suite}" in
        all|job) scripts/download_job_hf.sh "${job_data_dir}" ;;
    esac
fi

if [[ "${load_data}" == true ]]; then
    case "${suite}" in
        all|tpch)
            POSTGRES_CONTAINER="${container}" POSTGRES_DB="${database}" POSTGRES_USER="${user}" \
                optd/connectors/datafusion/scripts/load_tpch_postgres.sh "${tpch_data_dir}"
            ;;
    esac
    case "${suite}" in
        all|job)
            POSTGRES_CONTAINER="${container}" POSTGRES_DB="${database}" POSTGRES_USER="${user}" \
                optd/connectors/datafusion/scripts/load_job_postgres.sh "${job_data_dir}"
            ;;
    esac
fi

"${cargo_cmd[@]}" build --release -p optd-datafusion --bin cardinality-regression

if [[ "${run_optd}" == true ]]; then
    "${python_cmd[@]}" optd/connectors/datafusion/scripts/optd_cardinality_regression.py \
        --suite "${suite}" \
        --tpch-queries "${tpch_queries}" \
        --job-queries "${job_queries}" \
        --output "${regression_root}/optd" \
        --query-timeout "${optd_timeout}" \
        --resume \
        --continue-on-error
fi

if [[ "${run_postgres}" == true ]]; then
    "${python_cmd[@]}" optd/connectors/datafusion/scripts/postgres_cardinality_regression.py \
        --suite "${suite}" \
        --tpch-queries "${tpch_queries}" \
        --job-queries "${job_queries}" \
        --output "${regression_root}/postgres" \
        --container "${container}" \
        --database "${database}" \
        --user "${user}" \
        --statement-timeout "${postgres_timeout}" \
        --resume \
        --continue-on-error
fi

"${plot_python_cmd[@]}" optd/connectors/datafusion/scripts/plot_cardinality_regression.py \
    "${regression_root}/optd/report.json" \
    --compare-report "${regression_root}/postgres/report.json" \
    --output "${regression_root}/comparison" \
    --normalization all \
    --suite-matrix \
    --summary

"${plot_python_cmd[@]}" optd/connectors/datafusion/scripts/build_cardinality_dashboard.py \
    --optd-report "${regression_root}/optd/report.json" \
    --postgres-report "${regression_root}/postgres/report.json" \
    --queries "${tpch_queries}" \
    --job-queries "${job_queries}" \
    --suite-label "${suite}" \
    --output "${regression_root}/dashboard.html"

"${python_cmd[@]}" - "${regression_root}" "${suite}" "${container}" "${image}" <<'PY'
import datetime
import json
import platform
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
metadata = {
    "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "suite": sys.argv[2],
    "postgres_container": sys.argv[3],
    "postgres_image": sys.argv[4],
    "platform": platform.platform(),
    "git_commit": subprocess.run(
        ["git", "rev-parse", "HEAD"], text=True, capture_output=True, check=True
    ).stdout.strip(),
    "engines": {},
}
for engine in ("optd", "postgres"):
    report_path = root / engine / "report.json"
    errors_path = root / engine / "errors.json"
    report = json.loads(report_path.read_text())
    errors = json.loads(errors_path.read_text()) if errors_path.exists() else []
    metadata["engines"][engine] = {
        "measurements": len(report),
        "queries": len(
            {(row.get("suite", "tpch"), row["query"]) for row in report}
        ),
        "failures": len(errors),
        "report": str(report_path),
        "errors": str(errors_path) if errors_path.exists() else None,
    }
(root / "run-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
PY

echo "completed cardinality regression"
echo "dashboard=${regression_root}/dashboard.html"
echo "metadata=${regression_root}/run-metadata.json"
