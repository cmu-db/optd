# DuckLake-inspired single-history metadata catalog

## Scope and source of truth

Sources own data and physical objects. Optd stores versioned registrations, planning metadata,
and optimizer statistics. An optd snapshot records what the planner knew, **not** a historical
snapshot of externally owned rows. Source writes, including writes outside this adapter, are
not detected automatically. File manifests, delete files, file allocation, compaction, garbage
collection, physical data time travel, and new storage providers are deferred.

## Upstream comparison

Reference: https://github.com/duckdb/ducklake at
`ed31fddd089b038a800fa35e155f30e3c3abc8f0`.

Specification:
- https://ducklake.select/docs/stable/specification/tables/overview
- https://ducklake.select/docs/stable/specification/tables/ducklake_snapshot
- https://ducklake.select/docs/stable/specification/tables/ducklake_snapshot_changes

Implementation evidence at that revision:
- `src/storage/ducklake_transaction_state.cpp:2651–2764`: candidate snapshot, schema
  version, atomic commit batch, semantic conflict checking and retry boundaries.
- `src/storage/ducklake_metadata_manager.cpp:4693–4709,4824–4828`: snapshot and summary
  inserts; latest snapshot via `MAX(snapshot_id)`; separate format metadata.
- `src/metadata_manager/sqlite_metadata_manager.cpp:31–53`: lock retry classification,
  terminal locked-COMMIT handling and WAL advice.
- `src/metadata_manager/postgres_metadata_manager.cpp:289–305,420–428`: PostgreSQL error
  classification and a statement-consistent latest snapshot/change/statistics query.
- `test/sql/issues/issue_1798_sqlite_locked_commit.test` and
  `test/sql/concurrent/concurrent_insert_conflict.test`: backend locking and concurrent
  append behavior (inspected as references, not executed in optd).

| DuckLake mechanism | Optd adaptation |
| --- | --- |
| Single snapshot order per metadata catalog | One global order per optd metadata database, across its registered sources |
| Snapshot PK insertion arbitrates optimistic writers | Conservative global writer mutex; reject every stale expected snapshot |
| Semantic conflict checks, rebasing and ID remapping on retries | Deferred; never retry stale mutations merely by choosing another ID |
| Snapshot, metadata and summary committed together | Same invariant in a shared SeaORM transaction |
| Structural generation separate from snapshot ID | `schema_version`; catalog/schema/relation/index changes advance it |
| Change summaries used for inspection and conflicts | Typed JSON operation summaries; currently inspection, not conflict-aware rebasing |
| File/data counters and managed storage | Omitted; source systems own the data |
| Table/file statistics | Existing basic definition/payload/column-values layout plus optional temporal advanced objects |
| Lake schema namespace | External source registrations plus catalog-owned schemas |

This is a semantic adaptation, **not** an interoperable DuckLake metadata database or a port
of its C++ execution/storage engine. In particular, optd intentionally serializes metadata
writers instead of copying DuckLake's optimistic retry protocol.

## Schema and identities

```text
optd_catalog_metadata(id=1, format_version)
  -- writer mutex and format marker; contains NO snapshot head

optd_snapshot(snapshot_id PK, snapshot_time, schema_version, next_catalog_id)
optd_snapshot_changes(snapshot_id PK, changes_made, author?, commit_message?, extra_info?)

optd_catalog(row_id PK, catalog_id, begin_snapshot, end_snapshot?,
             catalog_name, catalog_kind, external_catalog_name?,
             adapter_name, connection_profile_reference)
optd_schema(row_id PK, schema_id, catalog_id, begin_snapshot, end_snapshot?, schema_name)
optd_relation(row_id PK, relation_id, schema_id, begin_snapshot, end_snapshot?,
              relation_name, relation_kind, storage_kind)
optd_column(row_id PK, relation_id, column_id, begin_snapshot, end_snapshot?, ...)
optd_index(row_id PK, index_id, relation_id, begin_snapshot, end_snapshot?,
           index_name, access_method, is_unique, nulls_not_distinct, fields JSON, status)

optd_statistics(... statistics_id, relation_id, begin_snapshot, end_snapshot?, ...)
optd_statistics_payload(... statistics_id, statistics_begin_snapshot,
                        begin_snapshot, end_snapshot?, collected_at, ...)
optd_column_statistics(... statistics_id, statistics_begin_snapshot,
                       payload_begin_snapshot, column_id, ...)

optd_adv_stats(row_id PK, statistics_id, relation_id, statistics_name,
               statistics_kind, column_ids JSON, begin_snapshot, end_snapshot?,
               value_encoding_version, contents TEXT, collected_at)
```

Top-level metadata object IDs share `next_catalog_id`; columns retain relation-scoped identities.
Query templates and instances use independent UUIDs in non-temporal history tables;
recording them does not publish a metadata snapshot or use this allocator. Names are not
identities. Private persistence row keys are not planner handles. References and visible-name
uniqueness are validated under the shared writer lock. Unique `(catalog_id, begin_snapshot)`
and `(schema_id, begin_snapshot)` indexes additionally protect version identities. Ordinary
foreign keys alone cannot establish cross-object temporal visibility.

Catalog 0 is the legacy/default `datafusion` namespace, visible from snapshot 0. Registrations
contain logical connection-profile references, not credentials or live connection state.
Registration currently supports creation; source refresh, registration replacement/drop, and
source-native rename reconciliation require future typed operations, not direct row edits.

A metadata row is visible at snapshot S iff:

```sql
begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)
```

## Reader and commit API

`Catalog::new(db)` selects legacy catalog 0. `Catalog::on_catalog(db, catalog_id)` scopes schema,
relation, index, and statistics access. Registrations and snapshot IDs remain global. Every
reader pins the latest committed snapshot or `reader_at(snapshot_id)`. `snapshot_info()`,
`schema_version()`, `snapshot_change()`, and `get_catalogs()` expose auditable planning inputs.

Mutations require `Catalog<DatabaseConnection>`: they own a fresh top-level transaction.
Transaction-backed handles support reads only, so publication cannot accidentally occur through
a savepoint inside an older SQLite read snapshot or a caller-selected PostgreSQL isolation level.

All public mutations route to `snapshot/commit.rs`:

1. Begin the transaction; explicitly choose READ COMMITTED on PostgreSQL.
2. First data statement: no-op UPDATE of the singleton metadata row. PostgreSQL acquires
   its row lock; SQLite reserves its single writer before establishing visibility reads.
   A missing mutex row fails closed.
3. Read the latest global snapshot and compare the caller's expected snapshot. Reject stale
   callers without mutation. Validate registration visibility and scoped object ownership.
4. Allocate S = B + 1 and IDs from the locked snapshot's counter. Validate and apply the
   typed mutation, closing replaced intervals and inserting new versions.
5. Insert snapshot S and its typed change summary in the same transaction. Structural
   operations increment schema_version once; statistics refresh/invalidation does not.
6. Commit. Only committed snapshots are visible to other connections. Any failure rolls
   back the mutation, snapshot, counter advance, and summary together.

There is no mutable current-head pointer and no runtime branch API. Reading the maximum
snapshot is safe as part of this protocol; `MAX` alone without writer coordination is not.
No transparent replay or semantic rebase is attempted. SQLite lock timeouts and PostgreSQL
connection/commit errors remain errors. An uncertain COMMIT outcome must be inspected through
history before retrying; no promise of exactly-once network retry is made.

Basic statistics retain their existing definition/payload/column-values layout. Refresh
replaces only the active payload; invalidation closes it without deleting its definition,
so basic recollection retains the stable statistics ID. Historical readers retain old values.

Advanced statistics are additional catalog objects in `optd_adv_stats`, not a redesign of the
basic tables. Each receives an ID from `next_catalog_id` and carries its own temporal contents.
Ordinary updates preserve ID and immutable metadata; drop/invalidation retires the object, and
recollection creates a new ID. Relation-wide invalidation closes the basic payload and every
active advanced object atomically. Relation drops also close advanced objects. Advanced-only
create/update/drop never modifies basic values, and DataFusion continues to consume only the
basic statistics. Joint NDV and scalar histogram contents have typed Rust storage APIs;
automatic collection, dependencies/sketches, and optimizer consumption remain future work.
See [advanced_statistics.md](advanced_statistics.md) for contracts, examples, and validation.

Snapshot visibility is distinct from source freshness. A future collection contract should
record provenance and source version/fingerprint evidence when available.

## DataFusion integration

Installation registers or resolves the named DataFusion catalog in `optd_catalog` and binds
provider DDL and statistics to that durable catalog ID. Same-named schemas and relations in
other registrations cannot be selected by name accident. `collect_stats` rejects foreign
catalog references; planner catalog construction uses fallback estimates for those scans.
The session-global UDF names are rebound by the most recent installation; persistent runners
carry their catalog name explicitly.

Source providers retain their native DML capabilities; metadata persistence no longer installs
a write-rejecting table wrapper. The shared [observed executor](observed_execution.md), also used
by the runner, handles both DML and DDL: it collects actual write-through DDL receipts and
invalidates basic plus advanced statistics after tracked DML attempts. Relation drops close both
inside their existing DropRelation snapshot. Source results and metadata maintenance status
are separate; no atomicity with source writes is claimed. Raw session/provider/external writes,
source transaction commits and cancellation outside completed observed execution require
explicit notification or reconciliation. Metadata snapshots describe planning knowledge, not
source data versions. The original fail-closed policy and its acceptance evidence below are
historical, superseded by the observed-execution contract.

## Extension points (not implemented here)

- **Constraints:** add temporal `optd_constraint(constraint_id, relation_id, begin_snapshot,
  end_snapshot, kind, fields JSON, expression?, dialect?, referenced_relation_id?)`. Ordered
  field records pair local and referenced durable column IDs for foreign keys. Validate both
  endpoints at the same snapshot. Provider-confirmed validity and enforcement semantics must
  be distinguished from untrusted assertions before the optimizer consumes them. Add typed
  mutations/readers through the same commit layer; structural changes advance schema_version.
- **Views:** add temporal `optd_view_definition(relation_id, begin_snapshot, end_snapshot,
  sql, dialect, column_aliases JSON)` alongside relation kind VIEW. Preserve authoritative
  source SQL and dependencies rather than reconstructing SQL from a generic provider.
- **Indexes:** the existing temporal key/include records, access method, uniqueness and
  usability state remain. Expression keys, partial predicates, and source identities can be
  added explicitly; registering metadata does not build a physical backend index.
- **Files:** add storage-definition/manifests behind stable relation IDs when optd actually
  owns their lifecycle. Do not introduce unused file counters or imply data time travel now.

## Non-destructive migration and deployment

The single-history strategy is implemented as the third forward-only migration, with frozen
original snapshot/schema/branch models used by the initial migration. A fourth transactional
migration adds the independent advanced-statistics table without altering the existing basic
statistics schema. Fresh databases traverse the same steps as upgraded databases.

1. Back up the metadata database and stop all legacy and new writers. This is **not** an online
   rolling upgrade. Test restoration and the upgrade on a copy first.
2. The upgrade requires exactly one legacy branch named `main`, pointing to the largest stored
   snapshot ID. Extra branches, missing main, dangling heads, and unpublished newer snapshots
   are refused. Resolve such histories separately; the migration never selects/discards one.
3. In one transaction on **both** backends, add schema versions and catalog ownership, create
   registrations/change summaries/the mutex, and backfill the legacy history. Every existing
   schema version belongs to catalog 0. IDs, timestamps, allocators, payloads and validity
   intervals are preserved.
4. Backfill schema_version by counting distinct stored snapshots with schema/relation/column/
   index begin **or end** boundaries. Snapshot-ID gaps are allowed. Statistics-only changes
   do not count. Prior summaries are marked `legacy_import` with unknown original provenance;
   they are not reconstructed user intent.
5. Remove the validated legacy branch table and record migration success transactionally.
   A DDL/backfill failure rolls back the entire third migration and is safe to retry after correction.
6. The fourth migration creates `optd_adv_stats` and its indexes only. All basic-statistics tables,
   rows, IDs, validity intervals and snapshot descriptors remain untouched. Each migration is
   transactional, not the whole multi-migration sequence, so a completed third step remains
   applied if the fourth fails. Correct the reported issue and retry with writers still stopped.

Automatic downgrade is deliberately refused; removing catalog ownership after multi-source
writes would be lossy. To use the old binary, restore a pre-upgrade backup. Do not run the old
binary against an upgraded database. The DataFusion installer runs pending migrations, so
perform the backup and offline preparation **before** starting the new application.

## Tests

Primary test artifacts:
- `optd/catalog/tests/single_history.rs` and independent `fixtures/legacy.sql`: fresh and legacy
  schema, history/ID/time preservation, gaps, inferred structural versions, migration rejection,
  injected migration failure and retry, publication rollback, namespace isolation, mutex failure,
  actual held-lock waiting, and overlapping writers in SQLite rollback-journal/WAL modes.
- Opt-in PostgreSQL tests in the same file: disposable per-run schemas, legacy upgrade,
  transactional DDL/backfill rollback/retry, summary rollback, explicit isolation with a non-default
  session default, held-lock waiting, and overlapping independent connection pools.
- Existing catalog tests retain temporal relation/column/index/statistics behavior and now check
  statistics refresh/invalidation schema versions and typed summaries.
- DataFusion persistent tests retain issue 1/2 regressions and exercise same-named persistent
  catalogs sharing one database, scoped drops, and statistics independence.
- Compile-fail API documentation proves transaction-backed handles cannot invoke mutation APIs.

Run (the PostgreSQL URL must point only to a disposable LOCAL instance):

```sh
OPTD_TEST_POSTGRES_URL=postgres://postgres@127.0.0.1:32768/postgres cargo test -p optd-catalog --locked
cargo test -p optd-datafusion --lib persistent_ --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
git diff --check
```
