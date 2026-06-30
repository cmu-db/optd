#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-optd/connectors/datafusion/data/tpch/sf-0.1}"
revision="${TPCH_HF_REVISION:-main}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base_url="https://huggingface.co/datasets/liangyc/tpch-sf-0_1/resolve/${revision}/data/sf-0_1"

absolute_path() {
    case "$1" in
        /*) printf '%s\n' "$1" ;;
        *) printf '%s/%s\n' "${repo_root}" "$1" ;;
    esac
}

tables=(
    customer
    lineitem
    nation
    orders
    part
    partsupp
    region
    supplier
)

if ! command -v curl >/dev/null 2>&1; then
    echo "error: curl is required to download TPC-H Parquet files." >&2
    exit 1
fi

output_path="$(absolute_path "${output_dir}")"
mkdir -p "${output_path}"

for table in "${tables[@]}"; do
    parquet_path="${output_path}/${table}.parquet"
    if [[ -f "${parquet_path}" ]]; then
        continue
    fi
    tmp_path="${parquet_path}.tmp"
    curl -fL --retry 3 --retry-all-errors "${base_url}/${table}.parquet" -o "${tmp_path}"
    mv "${tmp_path}" "${parquet_path}"
done

cat <<EOF
TPC-H Parquet files downloaded to ${output_dir}
EOF
