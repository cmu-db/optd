#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-optd/connectors/datafusion/data/job}"
revision="51a5d2387a6a63ebbf80fde95d2db597edf345e5"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base_url="https://huggingface.co/datasets/liangyc/job/resolve/${revision}/data"

absolute_path() {
    case "$1" in
        /*) printf '%s\n' "$1" ;;
        *) printf '%s/%s\n' "${repo_root}" "$1" ;;
    esac
}

tables=(
    aka_name
    aka_title
    cast_info
    char_name
    comp_cast_type
    company_name
    company_type
    complete_cast
    info_type
    keyword
    kind_type
    link_type
    movie_companies
    movie_info
    movie_info_idx
    movie_keyword
    movie_link
    name
    person_info
    role_type
    title
)

if ! command -v curl >/dev/null 2>&1; then
    echo "error: curl is required to download JOB Parquet files." >&2
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
JOB Parquet files downloaded to ${output_dir}
EOF
