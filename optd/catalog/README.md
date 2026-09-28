# optd-catalog

Snapshot-aware catalog persistence for Optd.

## Current implementation

The first executable catalog kernel was adapted from `origin/spring26` and
flattened into this single crate. It currently provides:

- SeaORM entities and a SeaORM migration for `optd_snapshot`, `optd_branch`,
  `optd_schema`, `optd_relation`, and `optd_column`.
- Transactional schema and relation create/drop operations.
- Readers pinned to a branch head or an explicit historical snapshot.
- Expected-base checks and atomic branch-head publication.
- SQLite-backed end-to-end coverage of historical visibility, stale writers,
  and failed-publication rollback.

This is intentionally a donor baseline, not the final schema. Query telemetry,
statistics persistence, the synchronous core adapter, and the old multi-crate
structure were not ported.

## Current architecture

- `entity`: SeaORM persistence entities.
- `migration`: executable catalog migrations.
- `snapshot`, `schema`, and `relation`: catalog operations.
- `catalog`: transactional writes and snapshot-pinned reads.

The target model is documented in
[`../../docs/catalog_design.md`](../../docs/catalog_design.md).

## Next revisions

1. Add relation and storage kinds.
2. Keep persistence-only row keys private and enforce stable ID scopes,
   including relation-scoped column IDs, in catalog operations.
3. Add rename and drop end-to-end coverage.
