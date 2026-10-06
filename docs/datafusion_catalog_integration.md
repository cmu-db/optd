# DataFusion catalog integration

This note records how DataFusion DDL is mirrored into `optd-catalog` and why the
integration differs from the earlier `origin/spring26` implementation.

## Earlier implementation

The `spring26` connector wrapped `SessionContext::execute_logical_plan`. It first
executed DataFusion DDL and then inspected the completed `LogicalPlan::Ddl` to
copy metadata into the repository catalog. It handled `CREATE EXTERNAL TABLE`,
`CREATE MEMORY TABLE`, and `DROP TABLE`. Schema and view DDL were not mirrored.

That approach had three important limitations:

- DataFusion could commit a DDL operation before catalog persistence failed.
- Mutations performed through DataFusion catalog-provider APIs bypassed the
  wrapper.
- Reconstructing external-table SQL produced an incomplete representation of
  the original statement and was not suitable as durable physical metadata.

The old synchronous core-catalog adapter also called asynchronous SeaORM code
with `block_in_place`. That adapter mixed planner lookup, runtime registration,
and durable catalog mutation in one type.

## Current design

DataFusion 53 executes DDL through `CatalogProvider` and `SchemaProvider`:

- `CatalogProvider::register_schema` and `deregister_schema`
- `SchemaProvider::register_table` and `deregister_table`

`optd-datafusion::persistent_catalog` wraps DataFusion's existing providers at
these boundaries. DataFusion remains responsible for runtime `TableProvider`
objects and data execution; `optd-catalog` records durable metadata and snapshot
history.

The initial mapping is:

| DataFusion operation | optd-catalog operation |
| --- | --- |
| `CREATE SCHEMA` | `create_schema` |
| `CREATE MEMORY TABLE` / CTAS | `create_base_table` with relational storage |
| `CREATE EXTERNAL TABLE` backed by `ListingTable` | `create_base_table` with file storage |
| `DROP TABLE` | `drop_relation` |

A missing schema is created before its first table. Ordinary duplicate DDL is a
name conflict, even when the requested schema matches durable metadata. Startup
imports may explicitly hold `idempotent_import_guard`; while that guard is
active, matching relation kind, storage kind, column order, names, types, and
nullability are treated as an already-imported provider. The CLI uses this scope
only while registering JOB or TPC-H providers, so a new process can rebuild its
runtime providers without replacing durable metadata or advancing the snapshot.
A mismatched definition remains a name conflict. Each persistent mutation reads
the current branch head and supplies that snapshot as the expected base. The
catalog's conditional branch-head publication remains the serialization point.

`install_persistent_catalog` enables the adapter for a named DataFusion catalog;
`uninstall_persistent_catalog` restores the exact wrapped provider. Reinstalling
wraps that same provider again, so runtime tables remain available. DDL executed
while persistence is disabled is intentionally not backfilled into
`optd-catalog`, and repeated installation is idempotent.

DataFusion provider methods are synchronous while SeaORM is asynchronous. The
initial adapter isolates that mismatch in a small bridge that runs each owned
catalog future on a dedicated temporary thread and Tokio runtime. This is
correct for current tests and avoids nesting a Tokio runtime or requiring a
multi-thread caller runtime. A long-lived catalog actor can replace the
per-operation thread if profiling shows that DDL frequency makes the overhead
meaningful.

## Failure handling

Registration is applied to the wrapped DataFusion provider and then persisted.
If persistence fails, the adapter compensates by restoring the previous
DataFusion provider state. Deregistration follows the same pattern: a failed
catalog drop re-registers the removed provider.

This is best-effort consistency across two systems, not a distributed
transaction. The adapter serializes durable publication in `optd-catalog`, but
DataFusion's in-memory provider registry and the metadata database cannot commit
atomically. Tests must cover compensation paths as catalog behavior expands.

## Current boundaries

- The adapter is write-through only. It does not reconstruct DataFusion table
  providers from persisted catalog metadata at startup. Turning persistence off
  and on in one process preserves runtime providers, but restarting the process
  does not restore them from metadata.
- File-backed relations record `StorageKind::Files`, but file locations and
  format options await the physical file tables.
- View registration is rejected until `optd_view_definition` and `create_view`
  exist. Authoritative SQL must come from the DDL plan rather than from a generic
  `TableProvider`.
- Temporary tables and views remain DataFusion concerns and are not persisted.
- The initial persistent-statistics UDFs are documented in
  [persistent_statistics_demo.md](persistent_statistics_demo.md). Statistics
  payloads use the unified catalog snapshot interval; DataFusion DML still needs
  to publish through that path so successful writes close visible payloads.
- PostgreSQL concurrency coverage remains deferred; SQLite exercises DDL
  mirroring, historical visibility, and branch publication.

## Next integration steps

1. Route successful DML through catalog snapshot publication and statistics
   invalidation.
2. Add a file-definition payload for external tables and preserve location,
   format, and options from `CreateExternalTable`.
3. Add `optd_view_definition` and route `CreateView`/`DropView` through typed
   catalog operations.
4. Decide whether startup hydration is required for file-backed providers.
5. Replace the temporary-thread bridge with a long-lived actor only if needed.
