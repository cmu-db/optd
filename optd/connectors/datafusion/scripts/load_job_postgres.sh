#!/usr/bin/env bash
set -euo pipefail

container="${POSTGRES_CONTAINER:-optd-postgres-18}"
database="${POSTGRES_DB:-optd_bench}"
user="${POSTGRES_USER:-optd}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../../.." && pwd)"
data_dir="${1:-${repo_root}/optd/connectors/datafusion/data/job}"
schema="${repo_root}/scripts/job_schema_duckdb.sql"
indexes="${script_dir}/job_postgres_indexes.sql"
tables=(
    aka_name aka_title cast_info char_name comp_cast_type company_name company_type
    complete_cast info_type keyword kind_type link_type movie_companies movie_info
    movie_info_idx movie_keyword movie_link name person_info role_type title
)

for command in docker duckdb; do
    if ! command -v "${command}" >/dev/null 2>&1; then
        echo "error: ${command} is required" >&2
        exit 1
    fi
done

if ! docker inspect "${container}" >/dev/null 2>&1; then
    echo "error: PostgreSQL container ${container} does not exist" >&2
    exit 1
fi

for table in "${tables[@]}"; do
    if [[ ! -f "${data_dir}/${table}.parquet" ]]; then
        echo "error: missing ${data_dir}/${table}.parquet" >&2
        exit 1
    fi
done

printf 'DROP TABLE IF EXISTS %s CASCADE;\n' "${tables[@]}" |
    docker exec -i "${container}" \
        psql -X -q -v ON_ERROR_STOP=1 -U "${user}" -d "${database}"

echo "creating JOB schema in ${container}/${database}"
docker exec -i "${container}" \
    psql -X -v ON_ERROR_STOP=1 -U "${user}" -d "${database}" <"${schema}"

for table in "${tables[@]}"; do
    echo "loading ${table}"
    duckdb -csv -noheader -nullvalue '' -c \
        "SELECT * FROM read_parquet('${data_dir}/${table}.parquet')" |
        docker exec -i "${container}" \
            psql -X -q -v ON_ERROR_STOP=1 -U "${user}" -d "${database}" \
            -c "\\copy ${table} FROM STDIN WITH (FORMAT csv)"
done

echo "creating JOB benchmark indexes"
docker exec -i "${container}" \
    psql -X -v ON_ERROR_STOP=1 -U "${user}" -d "${database}" <"${indexes}"

echo "analyzing JOB tables"
for table in "${tables[@]}"; do
    docker exec -i "${container}" \
        psql -X -q -v ON_ERROR_STOP=1 -U "${user}" -d "${database}" \
        -c "ANALYZE ${table};"
done

echo "JOB data loaded into ${container}/${database}"
