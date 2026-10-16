#!/usr/bin/env bash
set -euo pipefail

container="${POSTGRES_CONTAINER:-optd-postgres-18}"
database="${POSTGRES_DB:-optd_bench}"
user="${POSTGRES_USER:-optd}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../../.." && pwd)"
data_dir="${1:-${repo_root}/optd/connectors/datafusion/data/tpch/sf-0.1}"
schema="${script_dir}/tpch_postgres_schema.sql"
indexes="${script_dir}/tpch_postgres_indexes.sql"
tables=(region nation supplier customer part partsupp orders lineitem)

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

echo "creating TPC-H schema in ${container}/${database}"
docker exec -i "${container}" \
    psql -X -v ON_ERROR_STOP=1 -U "${user}" -d "${database}" <"${schema}"

for table in "${tables[@]}"; do
    echo "loading ${table}"
    duckdb -csv -noheader -c \
        "SELECT * FROM read_parquet('${data_dir}/${table}.parquet')" |
        docker exec -i "${container}" \
            psql -X -q -v ON_ERROR_STOP=1 -U "${user}" -d "${database}" \
            -c "\\copy ${table} FROM STDIN WITH (FORMAT csv)"
done

echo "creating TPC-H benchmark indexes"
docker exec -i "${container}" \
    psql -X -v ON_ERROR_STOP=1 -U "${user}" -d "${database}" <"${indexes}"

echo "analyzing TPC-H tables"
docker exec -i "${container}" \
    psql -X -v ON_ERROR_STOP=1 -U "${user}" -d "${database}" \
    -c "ANALYZE;"

echo "TPC-H data loaded into ${container}/${database}"
