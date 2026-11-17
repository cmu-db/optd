# optd-catalog

Snapshot-aware catalog persistence for Optd.

## Current implementation

The first executable catalog kernel was adapted from `origin/spring26` and
flattened into this single crate. It currently provides:

- SeaORM entities and migrations for one global snapshot history, external catalogs, schemas, relations,
  columns, relational indexes, statistics definitions, and temporal statistics
  payloads. These basic statistics tables retain their established layout.
- Optional advanced statistics objects in a separate `optd_adv_stats` table, with
  stable catalog IDs and temporal typed contents (joint NDV and scalar histograms).
  See [`../../docs/advanced_statistics.md`](../../docs/advanced_statistics.md).
- Transactional schema and typed base-table create/drop operations.
- Explicit relation and storage kinds for relational and file-backed tables.
- Readers pinned to the latest committed global snapshot or an explicit historical snapshot.
- Expected-base checks, structural schema versions, and typed change summaries atomically
  published by a shared, serialized commit layer. PostgreSQL uses READ COMMITTED and
  a row mutex; SQLite reserves its writer before any visibility reads.
- Structured SNAFU catalog errors for semantic and persistence failures.
- Temporal relational-index definitions with ordered key/include columns,
  backend access methods, uniqueness semantics, and physical usability state;
  file-backed relations reject ordinary indexes.
- Basic relation and column statistics persisted by durable relation/column ID,
  with collection publishing a new snapshot and historical payload visibility.
- SQLite coverage and opt-in disposable PostgreSQL coverage of concurrency, migration,
  historical visibility, stale writers, statistics versions, and publication rollback.

The test workflow has a dedicated PostgreSQL service-backed catalog job. Service-free
workspace, coverage and nightly jobs exclude the opt-in PostgreSQL tests; SQLite remains
enabled there. For local PostgreSQL verification, point `OPTD_TEST_POSTGRES_URL` at a
disposable local instance and run `cargo test -p optd-catalog --locked`.

This is intentionally a narrow baseline, not the final schema. Full pass-level telemetry,
the synchronous core adapter, and the old multi-crate structure remain outside this crate.

## Query history

`query_history::QueryHistoryStore` stores immutable SQL templates in `optd_query`
and individual optimization requests in `optd_query_instance`. Requests use UUIDv7
identifiers and may have no template when input cannot be normalized. Template
fingerprints include normalization version, dialect, scope and parameter shape;
canonical contents are checked before deduplication.

History writes do not publish snapshots or consume catalog object IDs. An instance
may reference the actual planning snapshot, then transition once from running to
optimized, bypassed, failed or cancelled. Completion is idempotent; conflicting
updates are rejected. Optimized does not imply successful query execution. Abrupt
cancellation can leave an unfinished instance.

Reads are bounded and use `(started_at, query_instance_id)` keyset cursors. JSON
payloads are versioned and bounded; callers must apply a capture policy before
passing potentially sensitive input. Execution feedback, candidate plans and
append-only pass-event logs are separate follow-ups.

## Current architecture

- `entity`: SeaORM persistence entities.
- `migration`: executable catalog migrations.
- `snapshot`, `schema`, `relation`, `index`, and `statistics`: catalog operations.
- `catalog`: transactional writes and snapshot-pinned reads.
- `query_history`: non-temporal templates and optimization-request lifecycle records.

The current model, DuckLake comparison, migration procedure, and limitations are documented in
[`../../docs/catalog_single_history.md`](../../docs/catalog_single_history.md).
Snapshots describe planning metadata, not the contents of externally owned tables.

## Next revisions

1. Keep persistence-only row keys private and enforce stable ID scopes,
   including relation-scoped column IDs, in catalog operations.
2. Add rename end-to-end coverage.
3. Add view definitions and a typed `create_view` operation.
4. Extend the [shared observed executor](../../docs/observed_execution.md) with source-specific
   commit/reconciliation notifications if needed; add sampling and richer distributions separately.
