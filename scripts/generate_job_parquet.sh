#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-optd/connectors/datafusion/data/job}"
work_dir="${2:-optd/connectors/datafusion/data/job-source}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
schema_sql="${repo_root}/scripts/job_schema_duckdb.sql"
source_url="${JOB_IMDB_URL:-https://bonsai.cedardb.com/job/imdb.tgz}"

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

if ! command -v duckdb >/dev/null 2>&1; then
    echo "error: duckdb is required." >&2
    exit 1
fi

output_path="$(absolute_path "${output_dir}")"
work_path="$(absolute_path "${work_dir}")"

mkdir -p "${output_path}" "${work_path}"

archive="${work_path}/imdb.tgz"
if [[ ! -f "${work_path}/title.csv" ]]; then
    if [[ ! -f "${archive}" ]]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "error: curl is required to download the JOB data archive." >&2
            exit 1
        fi
        curl -L "${source_url}" -o "${archive}"
    fi
    tar -xzf "${archive}" -C "${work_path}"
fi

for table in "${tables[@]}"; do
    if [[ ! -f "${work_path}/${table}.csv" ]]; then
        echo "error: missing JOB CSV file: ${work_path}/${table}.csv" >&2
        echo "The extracted CedarDB/JOB archive should contain one CSV per table." >&2
        exit 1
    fi
done

db_path="${work_path}/job.duckdb"
sql_path="${work_path}/generate_job_parquet.sql"

rm -f "${db_path}" "${sql_path}"

{
    printf ".read '%s'\n" "${schema_sql}"
    for table in "${tables[@]}"; do
        csv_path="${work_path}/${table}.csv"
        parquet_path="${output_path}/${table}.parquet"
        printf "COPY %s FROM '%s' (FORMAT csv, DELIMITER ',', HEADER true, NULL '', QUOTE '\"', ESCAPE '\\\\');\n" "${table}" "${csv_path}"
        printf "COPY %s TO '%s' (FORMAT parquet, COMPRESSION zstd);\n" "${table}" "${parquet_path}"
    done
} >"${sql_path}"

duckdb "${db_path}" <"${sql_path}"

cat <<EOF
JOB Parquet files written to ${output_dir}
DuckDB staging database kept at ${work_dir}/job.duckdb
EOF
