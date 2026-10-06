# optd-catalog

Snapshot-aware catalog persistence for Optd.

## Current implementation

The first executable catalog kernel was adapted from `origin/spring26` and
flattened into this single crate. It currently provides:

- SeaORM entities and migrations for snapshots, branches, schemas, relations,
  columns, relational indexes, statistics definitions, and temporal statistics
  payloads.
- Transactional schema and typed base-table create/drop operations.
- Explicit relation and storage kinds for relational and file-backed tables.
- Readers pinned to a branch head or an explicit historical snapshot.
- Expected-base checks and atomic branch-head publication.
- Structured SNAFU catalog errors for semantic and persistence failures.
- Temporal relational-index definitions with ordered key/include columns,
  backend access methods, uniqueness semantics, and physical usability state;
  file-backed relations reject ordinary indexes.
- Basic relation and column statistics persisted by durable relation/column ID,
  with collection publishing a new snapshot and historical payload visibility.
- SQLite-backed end-to-end coverage of historical visibility, stale writers,
  statistics versions, and failed-publication rollback.

This is intentionally a narrow baseline, not the final schema. Query telemetry,
the synchronous core adapter, and the old multi-crate structure were not ported.

## Current architecture

- `entity`: SeaORM persistence entities.
- `migration`: executable catalog migrations.
- `snapshot`, `schema`, `relation`, `index`, and `statistics`: catalog operations.
- `catalog`: transactional writes and snapshot-pinned reads.

The target model is documented in
[`../../docs/catalog_design.md`](../../docs/catalog_design.md).

## Next revisions

1. Keep persistence-only row keys private and enforce stable ID scopes,
   including relation-scoped column IDs, in catalog operations.
2. Add rename end-to-end coverage.
3. Add view definitions and a typed `create_view` operation.
4. Route DML through snapshot-based statistics invalidation, then add sampling and richer distributions.
