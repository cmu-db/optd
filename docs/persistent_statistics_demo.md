# Persistent statistics demo

The first statistics slice persists full-scan table and column summaries in the
snapshot-aware catalog. One implicit table-statistics definition is created per
relation, while every collection produces a new immutable payload. This existing
basic layout and the SQL collection path remain unchanged.

The persistence tables are:

- `optd_statistics`: temporal statistics definition and stable `statistics_id`
- `optd_statistics_payload`: relation row count and collection snapshot
- `optd_column_statistics`: non-null count, distinct count, and typed min/max
  bounds for durable `(relation_id, column_id)` identities

A collection commits and publishes a new catalog snapshot. Recollection keeps the
same `statistics_id`, closes the previous payload, and preserves old results for
historical readers. Explicit invalidation closes the active basic payload and all
active advanced objects in one commit. There is no automatic detection of external
writes. Original DataFusion providers retain native DML; the shared observed executor
and runner notify optd after tracked writes. See [observed_execution.md](observed_execution.md).

Optional advanced statistics live separately in `optd_adv_stats`; they do not
replace these tables or change `collect_stats`/`cardest`. Advanced contents are
currently supplied through the Rust API, not collected automatically or consumed
by the DataFusion estimator. See [advanced_statistics.md](advanced_statistics.md).

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
catalog. `optd-cli --catalog-db` also configures ordinary `OptdRunner` planning
to read these payloads; missing or invalidated statistics use fallback estimates
rather than triggering an automatic table scan.

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

The SQLite path can be supplied as the first argument and must not already exist.
The demo refuses to overwrite existing files. Without an argument it chooses a
unique temporary path and prints that path; the database is retained after the run.

## Current limitations

- Collection is a full scan; sampling, histograms, and most-common values are not
  implemented.
- Non-finite floating-point minimum/maximum bounds (NaN and positive/negative
  infinity) are stored as unknown rather than invalid JSON numeric values. Row
  counts, non-null counts, and distinct counts are retained. Previously written
  version-1 `{"Float64":null}` bounds are also read as unknown; other malformed
  bounds still produce an error. Finite bounds keep their existing encoding, so
  no encoding-version change or database migration is needed.
- DML keeps the source provider's native capabilities. The shared observed executor
  and runner invalidate basic and advanced statistics after tracked write attempts;
  metadata maintenance is not atomic with source writes and failures are reported
  separately. Raw session/provider writes, external clients and writes while
  persistence is disabled require explicit invalidation. Ended statistics currently
  use default estimates until recollection; stale-statistics fallback is not implemented.
- `cardest` reads the latest committed global metadata snapshot. Historical catalog reads are covered
  by the Rust API; a SQL `cardest_at` helper is not currently exposed.
- Persisted constraints are not yet merged into the planner-side statistics
  catalog.
