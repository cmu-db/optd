# optd-catalog

Snapshot-aware catalog persistence for Optd.

## Current implementation

The first executable catalog kernel was adapted from `origin/spring26` and
flattened into this single crate. It currently provides:

- SeaORM entities and a SeaORM migration for `optd_snapshot`, `optd_schema`,
  `optd_relation`, and `optd_column`.
- Transactional schema and relation create/drop operations.
- Readers pinned to current or historical snapshots.
- SQLite-backed end-to-end coverage of historical visibility.

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

1. Replace latest-snapshot discovery with an explicit locked `optd_branch`
   pointer and expected-base conflict checks.
2. Add relation and storage kinds.
3. Keep persistence-only row keys private and enforce stable ID scopes,
   including relation-scoped column IDs, in catalog operations.
4. Add rename and stale-writer end-to-end coverage.
