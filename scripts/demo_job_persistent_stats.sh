#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CATALOG_DB="${1:-/tmp/optd-cli-stats.sqlite}"
JOB_TITLE="$ROOT_DIR/optd/connectors/datafusion/data/job/title.parquet"

if [[ ! -f "$JOB_TITLE" ]]; then
    echo "Missing JOB data at $JOB_TITLE" >&2
    echo "Generate it with: ./scripts/download_job_hf.sh" >&2
    exit 1
fi

# Existing catalog metadata and statistics are intentionally retained. JOB table
# registration is idempotent when the durable schema still matches the Parquet
# providers registered in this process.
cd "$ROOT_DIR"

echo "Persistent statistics demo using the JOB title table"
echo "Catalog database: $CATALOG_DB (created if absent, reused if present)"
echo
echo "The actual table cardinality is compared with estimates before and after collection."
echo

cargo run --bin optd-cli --release -- \
    --job \
    --no-log-explain-steps \
    --catalog-db "$CATALOG_DB" \
    -c "SELECT COUNT(*) AS actual_rows FROM title" \
    -c "SELECT cardest('SELECT * FROM title') AS estimate_before" \
    -c "SELECT collect_stats('title') AS statistics_snapshot" \
    -c "SELECT cardest('SELECT * FROM title') AS estimate_after"

echo
echo "The SQLite catalog remains at $CATALOG_DB"
