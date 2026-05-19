#!/usr/bin/env bash
set -euo pipefail

scale_factor="${1:-0.1}"
output_dir="${2:-connectors/optd-datafusion/data/tpch/sf-${scale_factor}}"
compression="${TPCH_PARQUET_COMPRESSION:-SNAPPY}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

absolute_path() {
    case "$1" in
        /*) printf '%s\n' "$1" ;;
        *) printf '%s/%s\n' "${repo_root}" "$1" ;;
    esac
}

if ! command -v tpchgen-cli >/dev/null 2>&1; then
    cat >&2 <<'EOF'
error: tpchgen-cli is required.

Install it with the package manager you normally use for this workspace, then rerun this script.
EOF
    exit 1
fi

output_path="$(absolute_path "${output_dir}")"

mkdir -p "${output_path}"

tpchgen-cli \
    --scale-factor "${scale_factor}" \
    --format parquet \
    --output-dir "${output_path}" \
    --parquet-compression "${compression}"

cat <<EOF
TPC-H Parquet files written to ${output_dir}
EOF
