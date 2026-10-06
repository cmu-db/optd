# Persistent statistics demo

The first statistics slice persists full-scan table and column summaries in the
snapshot-aware catalog. It is intentionally narrower than the complete model in
[catalog_design.md](catalog_design.md): one implicit table-statistics definition
is created per relation, while every collection produces a new immutable
payload.

The persistence tables are:

- `optd_statistics`: temporal statistics definition and stable `statistics_id`
- `optd_statistics_payload`: relation row count and collection snapshot
- `optd_column_statistics`: non-null count, distinct count, and typed min/max
  bounds for durable `(relation_id, column_id)` identities

A collection commits and publishes a new catalog snapshot. A reader pinned
before collection sees no payload; a reader pinned to the new snapshot sees it.
Recollection keeps the same `statistics_id`, appends another immutable payload,
and preserves the old result for historical readers.

## SQL helpers

Installing the persistent DataFusion catalog registers two volatile UDFs:

```sql
SELECT cardest('SELECT * FROM numbers');
SELECT collect_stats('numbers');
```

`cardest(sql)` plans the supplied query with statistics from the current
persistent catalog snapshot and returns the root cardinality estimate as a
`DOUBLE`. It does not scan tables to obtain missing statistics; missing payloads
therefore use optd's normal fallback estimates.

`collect_stats(table_name)` requires one scalar string argument, performs a full
aggregate scan over every table column, persists the result, publishes the next
catalog snapshot, and returns that snapshot ID as a `BIGINT`. Column-driven or
batch invocation is rejected so one SQL call cannot accidentally publish one
snapshot per input row. Qualified names such as `analytics.orders` are supported.

Both UDFs are removed when the persistent catalog adapter is uninstalled and
restored when it is installed again. Previously persisted payloads remain in the
catalog.

## JOB demonstration script

With the local JOB Parquet data installed, run:

```shell
./scripts/demo_job_persistent_stats.sh
```

The script creates the SQLite catalog when absent and otherwise reuses it. JOB
table registration is idempotent when the persisted definitions match the
current Parquet schemas. It compares the actual `title` row count with `cardest`,
runs `collect_stats('title')`, and shows the persisted estimate afterward. Pass
an alternate SQLite path as the first argument when needed.

## Running through `optd-cli`

```shell
rm -f /tmp/optd-cli-stats.sqlite
cargo run -q -p optd-datafusion --bin optd-cli -- \
  --no-log-explain-steps \
  --catalog-db /tmp/optd-cli-stats.sqlite \
  -c "CREATE TABLE numbers AS SELECT * FROM (VALUES (1), (2), (2)) AS source(value)" \
  -c "SELECT cardest('SELECT * FROM numbers') AS estimate_before" \
  -c "SELECT collect_stats('numbers') AS statistics_snapshot" \
  -c "SELECT cardest('SELECT * FROM numbers') AS estimate_after"
```

This prints estimates of `1000` before collection and `3` afterward, with
snapshot `3` returned by `collect_stats`. The same flags can be used without
`-c` to explore interactively in the REPL.

## Running the dedicated demo

```shell
cargo run -p optd-datafusion --bin catalog-stats-demo
```

Expected shape of the output:

```text
relation snapshot: 2
estimate before collection: 1000
statistics snapshot: 3
statistics visible at relation snapshot: false
persisted row count: Some(3)
estimate after collection: 3
```

The SQLite path can be supplied as the first argument. The demo recreates that
file before running.

## Current limitations

- Collection is a full scan; sampling, histograms, and most-common values are not
  implemented.
- `data_snapshot_id` is currently unset, so freshness across DML is not yet
  provable. The demo assumes immutable table contents between collection and
  planning.
- `cardest` reads the current branch head. Historical catalog reads are covered
  by the Rust API; a SQL `cardest_at` helper is not currently exposed.
- Persisted constraints are not yet merged into the planner-side statistics
  catalog.
