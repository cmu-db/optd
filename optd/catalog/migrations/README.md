# Catalog migrations

Executable SeaORM migrations live in `src/migration/` and currently create:

- `optd_snapshot`
- `optd_snapshot_changes`
- `optd_catalog_metadata` (writer mutex, not a head)
- `optd_catalog` (external source registrations)
- `optd_schema`
- `optd_relation`
- `optd_column`
- `optd_index`
- `optd_statistics`
- `optd_statistics_payload`
- `optd_column_statistics`
- `optd_adv_stats` (optional advanced objects)

Temporal catalog entities use private auto-generated `row_id` primary keys for
SeaORM persistence only; durable catalog identity remains in stable object IDs.
The frozen initial migration temporarily bootstraps the legacy snapshot `0` and
`main` branch. The third migration validates that legacy history, preserves its
IDs and intervals, adds schema versions, namespaces and summaries, then removes
the branch table. Runtime publication uses the single-history commit layer.
Relational index definitions store ordered key/include fields as JSON and are
rejected for file-backed relations.

Basic statistics keep their original definition/payload/column-values schema.
Definitions store canonical sorted column IDs as JSON; implicit table definitions
use an empty array. Payloads carry a typed-value encoding version and unified
`begin_snapshot`/`end_snapshot` visibility. Existing version-reference fields remain.

The fourth migration (`advanced_statistics`) is transactional and strictly additive:
it creates only `optd_adv_stats` and its indexes, without changing basic tables, rows,
IDs, snapshots or allocators. Advanced objects use stable global IDs, temporal
contents, and immutable relation/name/target/kind metadata. Unique indexes protect
`(statistics_id, begin_snapshot)` and relation-local active names. Advanced writes
use the same atomic publication path; invalidation closes their active versions.
See [advanced_statistics.md](../../../docs/advanced_statistics.md).

The briefly proposed flattening migration was withdrawn before deployment; it is
not part of the migration chain. Do not deploy an intermediate development binary.

Back up existing catalogs and stop all writers before upgrading. The single-history
migration is transactional on SQLite and PostgreSQL and rejects ambiguous legacy
branch state. Do not delete/recreate databases to bypass an upgrade error. Automatic
downgrade is refused; restore a pre-upgrade backup when returning to the old binary.
See [the migration procedure](../../../docs/catalog_single_history.md#non-destructive-migration-and-deployment).
