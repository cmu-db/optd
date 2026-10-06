# What is in a database catalog?

This document describes a snapshot-aware catalog for both relational storage and immutable files. The catalog is SQL-queryable, like PostgreSQL's `pg_catalog`, while also tracking file manifests, partition layouts, and sort orders like a lakehouse catalog.

## Goals and scope

- Represent catalogs, schemas, tables, views, columns, constraints, indexes, statistics, partitions, sort orders, and physical storage.
- Support relational and file-backed relations.
- Give every query a consistent metadata snapshot.
- Keep enough history for time travel, auditing, and garbage collection.
- Expose metadata to both the planner and users.

The catalog describes logical objects separately from their physical storage. A table can therefore move from relational storage to files, or use both, without changing its logical identity.

The first version does not need to reproduce every PostgreSQL catalog. Roles, privileges, triggers, procedures, and extension metadata can be added after the planner-facing model is stable.

**Decision:** build a relational metadata catalog with explicit snapshot IDs.

**Alternatives considered:**

- PostgreSQL-style current-state catalogs are simpler, but do not provide durable history.
- A collection of JSON documents is flexible, but makes joins, constraints, migrations, and selective reads harder.
- Reconstructing metadata entirely from commit logs makes current-state reads expensive.

## Core representation rules

- Stable IDs identify logical objects across multiple temporal rows.
- Catalog rows use `begin_snapshot` and `end_snapshot` directly.
- `end_snapshot` is exclusive and `NULL` means currently visible.
- Logical metadata is distinct from physical storage metadata, even when stored in the same temporal row.
- Small ordered components are stored as arrays of typed records.

A catalog row is visible at snapshot `S` when:

```sql
begin_snapshot <= S
AND (end_snapshot IS NULL OR S < end_snapshot)
```

Renaming a table inserts a new `optd_relation` row with the same `relation_id` and closes the old row. Dropping and recreating the same name uses a new `relation_id`.

There are no separate `*_object` identity tables. Creation is the earliest `begin_snapshot` for an ID. Foreign-key-like temporal integrity is checked by the catalog commit layer because ordinary SQL foreign keys cannot verify that two objects are visible at the same snapshot. Long-term creation history belongs in the catalog audit log if early rows are garbage-collected.

Use catalog-local `bigint` IDs for compact indexes and joins. Schema and relation IDs are unique within a catalog. Column IDs are unique only within their owning relation, so a durable column reference is the pair `(relation_id, column_id)`. An optional UUID may be stored for cross-catalog import and export. Snapshot IDs are also `bigint` and are allocated in catalog commit order.

A relation-scoped column ID remains stable across renames and temporal versions, is never renumbered when another column is dropped, and is never reused within that relation. It is distinct from `ordinal`, which may change when the visible column order changes. Dropping and recreating a relation creates a new `relation_id`, so its column-ID sequence may start over safely.

A column reference that must stand on its own carries both components:

```text
CatalogColumnRef {
  relation_id: bigint
  column_id: bigint
}
```

Temporal SeaORM entities use an auto-generated `row_id` primary key as a persistence adapter. This key has no catalog semantics: it is not exposed through catalog or planner APIs, is not used by temporal visibility queries, and is never referenced as durable object identity. Stable IDs remain directly on temporal rows. The database does not require uniqueness constraints over temporal identity tuples solely to satisfy the ORM; the branch-locked catalog writer is responsible for producing non-overlapping versions. User tables receive a primary key only when one is explicitly declared or required by a storage feature.

**Decision:** permit private persistence-only row keys for temporal SeaORM entities, store stable IDs directly in temporal rows, scope column IDs to their owning relation, and enforce temporal references and version consistency in the commit layer.

**Alternatives considered:**

- Separate identity and version tables provide ordinary foreign-key targets but add a join to common reads.
- Using names as IDs makes rename, dependency tracking, and historical lookup fragile.
- Storing `created_snapshot` duplicates the earliest `begin_snapshot` and can diverge from it.

## Nested field records

- Correlated per-position attributes are grouped into one typed record.
- Small ordered collections use arrays of those records.
- Unbounded or independently managed collections remain separate tables.
- JSON Schema documents the logical record shapes but does not prescribe JSON storage.

The nested record types are shown below. `optional` means the field may be `NULL`.

```text
ConstraintField {
  column_id: bigint
  referenced_column_id: bigint optional
}
```

```text
IndexField {
  role: KEY | INCLUDE
  column_id: bigint
  direction: ASC | DESC optional
  null_order: FIRST | LAST optional
  collation_id: bigint optional
  operator_class_id: bigint optional
}
```

```text
PartitionField {
  partition_field_id: bigint
  source_column_id: bigint
  transform: IDENTITY | YEAR | MONTH | DAY | HOUR | BUCKET | TRUNCATE
  transform_argument: integer optional
  partition_name: text
  result_type_id: bigint
}
```

```text
SortField {
  source_column_id: bigint
  transform: IDENTITY | YEAR | MONTH | DAY | HOUR | BUCKET | TRUNCATE optional
  transform_argument: integer optional
  direction: ASC | DESC
  null_order: FIRST | LAST
  collation_id: bigint optional
}
```

```text
StatisticsField {
  column_id: bigint
}
```

```text
PartitionValue {
  partition_field_id: bigint
  value: typed_scalar
}
```

```text
FileColumnStatistics {
  column_id: bigint
  null_count: bigint
  nan_count: bigint optional
  lower_bound: typed_scalar optional
  upper_bound: typed_scalar optional
}
```

In a PostgreSQL implementation these can be arrays of composite types; in another engine they can be native `LIST<STRUCT>` values. Column IDs in these records are relation-scoped. The owning constraint, index, partition specification, sort order, statistics object, or data file supplies the local `relation_id`; a cross-relation reference must carry the referenced relation explicitly. The initial field schemas are column-based. Expression-based index and statistics fields are deferred until the catalog expression representation is designed.

The formal JSON Schemas are in [`catalog_fields.schema.json`](catalog_fields.schema.json). They provide a language-neutral contract for validation, generated bindings, and interchange. Physical catalog storage remains typed and need not contain JSON.

**Decision:** use arrays of typed records instead of parallel arrays.

**Alternatives considered:**

- PostgreSQL-style parallel arrays are compact but require positional cardinality checks across several columns.
- Child tables are easy to query relationally but add joins when the planner always needs the whole collection.
- JSON arrays are flexible but give up strong physical typing and efficient decoding.

## Catalog hierarchy

- A catalog contains schemas and snapshot history.
- A schema contains named relations, types, functions, and operators.
- Tables, views, and materialized views are relation kinds.
- A relation owns columns, constraints, indexes, statistics, partition specifications, sort orders, and storage metadata.
- IDs, not names, connect levels.

The ownership hierarchy is:

```text
catalog
├── snapshots and branches
└── schema
    ├── type, function, operator, and collation
    └── relation
        ├── columns
        ├── constraints
        ├── indexes
        ├── statistics
        ├── partition specifications
        ├── sort orders
        ├── physical storage and files
        └── view definition and dependencies   (views only)
```

Internal catalog tables use the `optd_` prefix. The main temporal tables are:

```text
optd_snapshot(snapshot_id, catalog_id, parent_snapshot_id, ...)
optd_branch(catalog_id, branch_name, current_snapshot_id, ...)
optd_catalog(catalog_id, begin_snapshot, end_snapshot, ...)
optd_schema(schema_id, catalog_id, begin_snapshot, end_snapshot, ...)
optd_relation(relation_id, schema_id, begin_snapshot, end_snapshot, ...)
optd_column(relation_id, column_id, begin_snapshot, end_snapshot, ...)
optd_constraint(constraint_id, relation_id, begin_snapshot, end_snapshot, ...)
optd_index(index_id, relation_id, begin_snapshot, end_snapshot, ...)
optd_statistics(statistics_id, relation_id, begin_snapshot, end_snapshot, ...)
```

Rows sharing an object identity are historical definitions of the same logical object. For a column, that identity is `(relation_id, column_id)`; for a relation it is `relation_id`. Partition specifications, sort orders, and files also have IDs, but represent physical layouts belonging to a relation rather than SQL namespace objects.

A view is an `optd_relation` row with `relation_kind = VIEW`; its query is stored in `optd_view_definition`. The same rule applies to materialized views, which additionally own physical storage and directly collected statistics.

An `optd_relation` row stores `schema_id` because moving it between schemas creates a new temporal row. An `optd_column` row stores its owning `relation_id` on every temporal row, and its `column_id` is interpreted only within that relation.

Name uniqueness is enforced at commit time for the target snapshot. An ordinary SQL unique constraint cannot express "unique among overlapping visibility intervals" portably.

**Decision:** use typed temporal tables under a clear ownership hierarchy, with views represented as relation kinds and planner metadata owned by relations.

**Alternatives considered:**

- A universal temporal table simplifies generic dependencies but creates many nullable fields and weak type-specific integrity.
- Making views separate from relations duplicates column, naming, statistics, and dependency behavior.
- Separate `*_object` and `*_version` tables provide stronger ordinary foreign keys but add joins without solving temporal integrity.

## Relations

- Tables, views, and materialized views share a relation identity.
- `relation_kind` identifies logical behavior.
- `storage_kind` identifies physical storage.
- Kind-specific details live in extension tables.

A visible `optd_relation` row contains:

```text
relation_id
schema_id
relation_name
relation_kind       TABLE | VIEW | MATERIALIZED_VIEW | SEQUENCE
storage_kind        RELATIONAL | FILES | NONE
owner_id
properties
begin_snapshot
end_snapshot
```

Logical views use `storage_kind = NONE`. Base tables and materialized views use relational or file storage.

Extension tables hold fields that do not apply to every relation:

```text
optd_relational_storage
optd_file_storage
optd_view_definition
optd_materialized_view_state
```

**Decision:** use one relation abstraction with typed extensions.

**Alternatives considered:**

- Separate top-level table and view catalogs avoid nullable fields but duplicate naming, ownership, columns, and dependency logic.
- One very wide relation table is easier to query but becomes difficult to validate.

## Types and expressions

- Columns refer to type IDs rather than storing only type names.
- Type parameters are stored separately from type identity.
- Non-query expressions such as defaults, generated expressions, checks, and index expressions will use an inline representation owned by their catalog row; the representation is intentionally undecided.
- View definitions initially store authoritative SQL and are parsed and bound when expanded; a persisted resolved-tree format is deferred.

A visible `optd_column` row contains:

```text
column_id
column_name
ordinal
logical_type_id
type_parameters
nullable
default_expression
generated_expression
collation_id
begin_snapshot
end_snapshot
```

Parameterized values such as `decimal(12,2)` use one decimal type plus parameters. Nested types use child type nodes for struct fields, arrays, and maps.

There is no separate expression table. A non-query expression belongs to its owning column, constraint, index, or statistics definition. Its representation is deliberately deferred until the first such feature is implemented; the catalog should not expose a generic opaque byte wrapper before its validation, type, reference, and compatibility requirements are known.

Before commit, expression validation must reject unresolved names and invalid object references, and dependency edges must be extracted into `optd_dependency`. Query-local arena handles are not durable catalog identities and cannot be interpreted without their owning query context.

**Decision:** keep owned non-query expressions inline rather than assigning expression IDs, but do not choose an encoding yet.

**Alternatives considered:**

- SQL is portable and inspectable but must be parsed and resolved repeatedly.
- A dedicated typed representation can preserve resolved identities but requires a compatibility and migration policy.
- A separate expression table enables sharing but adds joins and an unnecessary lifecycle.

## Constraints

- Constraints describe logical truths, not access paths.
- Primary, unique, and foreign constraints store ordered column-ID arrays.
- Check constraints store an expression and expression dependencies.
- Enforcement and validation are explicit.

Constraints are stored in one temporal table:

```text
optd_constraint(
  constraint_id,
  relation_id,
  constraint_name,
  constraint_type,
  fields ConstraintField[],
  referenced_relation_id,
  referenced_constraint_id,
  match_type,
  on_update_action,
  on_delete_action,
  check_expression,
  enforcement_mode,
  validation_state,
  deferrable,
  initially_deferred,
  backing_index_id,
  begin_snapshot,
  end_snapshot
)
```

`ConstraintField` groups each positional pair:

```text
ConstraintField {
  column_id,
  referenced_column_id optional
}
```

For a composite key, field order preserves key order. For a foreign key, each field pairs one referencing column with its referenced column. `referenced_column_id` is absent for primary and unique constraints.

A check constraint stores `check_expression`. Its referenced columns are extracted into dependencies because checks do not have ordered key columns. The physical expression encoding is deferred.

Enforcement modes are:

```text
ENFORCED
VALIDATED_NOT_ENFORCED
RELY
INFORMATIONAL
```

This matters for file-backed tables, where a declared key may be trusted metadata without being checked on every write.

A primary or unique constraint may point to a backing unique index. A foreign key points to the referenced primary or unique constraint, not directly to its index. Referencing-side indexes remain independent and are not created automatically.

**Decision:** keep small ordered constraint components as arrays on `optd_constraint`.

**Alternatives considered:**

- A child row per column is easier to query globally but adds joins to every constraint lookup.
- Arrays are compact and are always read with the constraint; the commit layer validates cardinality and column references.
- Treating indexes as constraints conflates logical validity with physical implementation.

## Indexes

- An index row contains both its logical definition and current physical state.
- Key and included fields are stored in one ordered array of typed records.
- Keys can be columns or expressions.
- Partial predicates are stored as expressions.

The catalog stores:

```text
optd_index(
  index_id,
  relation_id,
  index_name,
  access_method_id,
  is_unique,
  nulls_not_distinct,
  fields IndexField[],
  predicate_expression,
  built_from_snapshot,
  storage_kind,
  physical_location,
  status,
  properties,
  begin_snapshot,
  end_snapshot
)
```

Initially, each `IndexField` contains `role`, `column_id`, `direction`, `null_order`, `collation_id`, and `operator_class_id`. Included fields contain only `role = INCLUDE` and `column_id`. Expression keys and the Boolean `predicate_expression` for partial indexes remain part of the intended model, but their physical expression encoding is deferred.

The current design publishes a replacement physical index by closing the old `optd_index` row and inserting a new row with the same `index_id`. A future implementation can reintroduce physical generations if it must expose multiple builds simultaneously.

Partitioning, sort order, Parquet min/max values, and row-group statistics are not modeled as ordinary indexes.

**Decision:** fold current physical state into `optd_index` and store its small ordered field list as `IndexField[]`.

**Alternatives considered:**

- A separate generation table supports simultaneous online builds but adds a join and is not required initially.
- Child rows make individual fields easier to query but the planner always needs the complete index definition.
- Modeling all data-skipping metadata as indexes hides important differences between ordering, partitioning, statistics, and search structures.

## Partitioning and sort order

- Partition specifications and sort orders evolve independently.
- Files retain the specification and order under which they were written.
- Fields are ordered and refer to stable source columns.
- Transforms are explicit.

Partition metadata groups correlated attributes:

```text
optd_partition_spec(
  partition_spec_id,
  relation_id,
  fields PartitionField[],
  begin_snapshot,
  end_snapshot
)
```

A `PartitionField` contains `partition_field_id`, `source_column_id`, `transform`, optional `transform_argument`, `partition_name`, and `result_type_id`.

Supported transforms initially include `IDENTITY`, `YEAR`, `MONTH`, `DAY`, `HOUR`, `BUCKET`, and `TRUNCATE`.

Sort metadata uses the same pattern:

```text
optd_sort_order(
  sort_order_id,
  relation_id,
  fields SortField[],
  begin_snapshot,
  end_snapshot
)
```

A `SortField` contains `source_column_id`, optional `transform`, `direction`, `null_order`, and optional `collation_id`. Array position defines field order.

Each data file records `partition_spec_id`, typed partition values, and `sort_order_id`. Old and new layouts may coexist after evolution.

Sort order is a physical property, not a uniqueness constraint. The planner may use it to avoid sorts, choose merge joins, or optimize ordered limits only when the physical guarantee applies to all selected files.

**Decision:** store each small ordered specification as an array of typed records and attach its ID to files.

**Alternatives considered:**

- Child field tables are easier to query globally but add joins when reading every specification.
- Storing only the current partition definition on the table loses the interpretation of old files.
- Deriving partition values from paths makes renames and non-Hive layouts fragile.
- Treating sort order as an index overstates what globally sorted files guarantee.

## Physical storage

- Relational storage uses engine-specific locators.
- File storage uses immutable file manifests.
- File visibility is snapshot-versioned.
- Delete files and row groups are first-class metadata when supported.

Relational storage records:

```text
optd_relational_storage(
  relation_id,
  relation_begin_snapshot,
  engine_id,
  physical_database,
  physical_schema,
  physical_name,
  access_method,
  tablespace,
  version_capability,
  transaction_watermark
)
```

`version_capability` states whether storage supports only current reads, transaction snapshots, or durable time travel. A catalog snapshot alone cannot make mutable relational pages historically readable.

File storage records:

```text
optd_data_file(
  file_id,
  relation_id,
  file_path,
  file_format,
  file_size_bytes,
  row_count,
  content_hash,
  schema_snapshot_id,
  partition_spec_id,
  partition_values PartitionValue[],
  sort_order_id,
  column_statistics FileColumnStatistics[],
  properties
)

optd_data_file_visibility(
  file_id,
  begin_snapshot,
  end_snapshot,
  data_sequence_number
)
```

`PartitionValue` groups a `partition_field_id` with its typed value. `FileColumnStatistics` groups a `column_id` with null count, NaN count, lower bound, and upper bound. Separating immutable file facts from visibility avoids duplicating file metadata. Row groups, position deletes, equality deletes, deletion vectors, and encryption keys remain separate because they can be numerous or independently managed.

Files are written before the catalog commit. The commit publishes them atomically. Files left by failed commits are garbage-collected after a grace period.

**Decision:** immutable files plus versioned manifest visibility.

**Alternatives considered:**

- Updating files in place breaks snapshot isolation and safe retries.
- Encoding visibility only in directory names makes atomic commits and branching difficult.
- Keeping all file details in one JSON manifest is efficient for bulk reads but poor for SQL inspection and incremental updates; materialized manifest files can still be generated as a cache.

## Statistics

- Statistics definitions are separate from collected payloads.
- Payloads identify both metadata and data snapshots.
- Single-column, multicolumn, file, and row-group statistics are supported.
- Freshness is explicit.

The main tables are:

```text
optd_statistics(
  statistics_id,
  relation_id,
  statistics_name,
  statistics_kind,
  collection_mode,
  fields StatisticsField[],
  begin_snapshot,
  end_snapshot
)

optd_statistics_payload(
  statistics_id,
  statistics_begin_snapshot,
  catalog_snapshot_id,
  data_snapshot_id,
  collected_at,
  sample_fraction,
  row_count,
  freshness,
  payload
)
```

The first executable slice implements one implicit table-statistics definition per relation. Relation-level payload rows are immutable, and basic per-column values are stored in `optd_column_statistics`, keyed by the definition version and collection snapshot. This normalized side table is the initial typed representation for non-null frequency, distinct count, and min/max bounds; collection modes, sampling, freshness fingerprints, and richer payload components remain follow-up work. See [persistent_statistics_demo.md](persistent_statistics_demo.md).

Initially, a `StatisticsField` contains `column_id`. Expression-based statistics fields are deferred with the catalog expression representation. Single-column payloads can include null count, distinct count, average width, min/max, most-common values, frequencies, histograms, and physical correlation. Multicolumn payloads can include joint distinct counts, most-common combinations, and functional dependencies.

File and row-group statistics use typed min/max values and null counts to support pruning. Large histograms and MCV sets may live in external statistics blobs referenced by the catalog.

Logical views normally derive estimates from their expanded query. Persisted view statistics are allowed only when they record the view-definition snapshot and every dependency's data snapshot. Materialized views are analyzed like tables.

**Decision:** versioned statistics payloads with dependency fingerprints.

**Alternatives considered:**

- Keeping only the newest statistics is simpler but makes historical planning inconsistent.
- Recomputing all view estimates is always correct but can be expensive for repeated complex views.
- Persisted logical-view statistics without dependencies can silently become stale and are therefore rejected.

## Views and dependencies

- View definitions are attached to temporal relation rows.
- Dependencies use stable object IDs.
- Materialized views track refresh state and source snapshots.
- Dependency changes invalidate cached plans and derived statistics.

Common relation metadata stays in `optd_relation`; the initial implementation stores view-specific syntax in a one-to-one `optd_view_definition` row:

```text
optd_view_definition(
  relation_id,
  relation_begin_snapshot,
  definition_sql,
  security_mode,
  check_option
)
```

`(relation_id, relation_begin_snapshot)` identifies the owning `optd_relation` row. The view definition therefore shares that row's visibility interval instead of maintaining another `begin_snapshot`/`end_snapshot` pair. `definition_sql` is authoritative and is parsed and bound against the reader's pinned catalog snapshot when the view is expanded. The visible view columns remain ordinary `optd_column` rows with durable relation-scoped column IDs. Replacing a view inserts a new `optd_relation` row and `optd_view_definition` row while preserving `relation_id` according to the view-replacement compatibility rules.

A parsed AST or resolved node tree may later be stored as a versioned derived artifact or cache. Such an artifact is never authoritative: it must identify its encoding version and dependency snapshot, and it must be safe to discard and rebuild from `definition_sql`. This defers a commitment to persisted query-local column identity until the planner integration and format requirements are clearer.

Dependencies are represented generically:

```text
optd_dependency(
  dependent_object_type,
  dependent_object_id,
  referenced_object_type,
  referenced_object_id,
  dependency_kind,
  begin_snapshot,
  end_snapshot
)
```

Dependency kinds distinguish normal references, ownership, automatic dependencies, and internal implementation dependencies. This graph supports `DROP RESTRICT`, `DROP CASCADE`, lineage, invalidation, and refresh planning.

A materialized view records the catalog snapshot and data snapshots from which it was refreshed. It can therefore report whether it is current, stale, or invalid.

**Decision:** start with authoritative view SQL plus explicit dependency edges. Parse and bind the SQL when expanding a view; revisit a persisted AST or resolved-tree cache only after measuring that path.

**Alternatives considered:**

- Persisting only a resolved query tree avoids repeated binding but commits the catalog to a planner serialization format and query-local identity model too early.
- Re-parsing and binding SQL has runtime cost, but it keeps the first format inspectable and allows that cost to be addressed later with a disposable cache.
- Rediscovering dependencies from SQL for every dependency operation is unnecessary because validated dependency edges are stored explicitly at create or replace time.

## Planner metadata

- Types, functions, operators, casts, collations, and access methods are catalog objects.
- Planner properties are explicit rather than inferred from implementation names.
- The planner reads resolved descriptors rather than issuing catalog SQL.
- Caches are keyed by snapshot ID.

Important metadata includes:

```text
function: argument types, result type, cost, expected rows, volatility,
          parallel safety, support function
operator: operand types, result type, implementation function,
          restriction selectivity, join selectivity, hash/merge capability
cast: source type, target type, context, method
access method: handler and supported capabilities
operator class/family: supported operators and support procedures
collation: provider, locale, deterministic flag, provider version
```

The catalog service resolves these into a `RelationDescriptor` containing columns, constraints, indexes, partitioning, sort orders, statistics, visible files, storage locators, and dependencies.

Descriptor cache keys are:

```text
(catalog_id, snapshot_id, relation_id)
```

An immutable snapshot entry never needs invalidation. Only branch-head and current-snapshot caches change.

**Decision:** expose a typed planner API over relational catalog storage.

**Alternatives considered:**

- Letting the planner run SQL against catalog views is easy to prototype but recursively invokes parsing and planning and produces excessive overhead.
- A cache keyed only by relation ID can return metadata from the wrong snapshot.

## Catalog output

- Internal temporal tables live under `sys_catalog` and embed small owned collections as typed arrays.
- Human-readable views live under `catalog`.
- Standard compatibility views live under `information_schema`.
- Historical reads use explicit snapshot functions.

Examples of user-facing views are:

```text
catalog.catalogs
catalog.schemas
catalog.relations
catalog.columns
catalog.constraints
catalog.constraint_columns
catalog.indexes
catalog.index_fields
catalog.partition_specs
catalog.sort_orders
catalog.files
catalog.statistics
catalog.snapshots
```

Summary views include stable IDs, resolved names, structured arrays, enforcement state, and reconstructed SQL. Detail views use `unnest`-style expansion to present one row per array position for tools that should not parse arrays.

Current-state queries use the session's pinned catalog snapshot:

```sql
SELECT *
FROM catalog.constraints
WHERE schema_name = 'tpch' AND table_name = 'orders';
```

Historical access is explicit:

```sql
SELECT *
FROM catalog.constraints_at_snapshot(120)
WHERE schema_name = 'tpch' AND table_name = 'orders';
```

The output should include the snapshot used to resolve it. Internal expression encodings and storage implementation details are hidden unless a diagnostic view is requested.

**Decision:** compact array-based storage, expanded detail views, and typed planner descriptors.

**Alternatives considered:**

- Exposing only internal tables forces every consumer to rebuild the same joins.
- Exposing only summary views makes schema tools parse text and arrays.

## Catalog transactions

- A branch points to one current snapshot.
- Commits publish metadata and file visibility atomically.
- Writers use optimistic conflict detection.
- Snapshot expiration is separate from commit processing.

A commit proceeds as follows:

1. Read and pin base snapshot `B`.
2. Write new immutable files outside the catalog transaction.
3. Begin a catalog transaction and lock the target branch.
4. Compare the branch head with `B`.
5. Check intervening `optd_snapshot_change` rows for conflicts.
6. Rebase safe changes or reject conflicting changes.
7. Allocate snapshot `S` in serialization order.
8. Close replaced temporal rows and insert their replacements.
9. Publish file additions and removals.
10. Advance the branch to `S` and commit.

Sequence allocation alone does not define commit order. The branch lock and transaction establish serialization.

Snapshot expiration first marks snapshots unreachable. Garbage collection later removes metadata and files not reachable from any branch, tag, retained snapshot, or active query.

**Decision:** optimistic commits serialized at the branch head.

**Alternatives considered:**

- A global catalog lock is simpler but prevents independent table commits.
- Last-writer-wins can silently lose schema and manifest updates.
- Immediate deletion makes failed commits and long-running readers unsafe.

## Worked example

- Represent `tpch.orders` as a stable relation.
- Attach columns, constraints, indexes, files, and statistics by ID.
- Resolve all metadata at one snapshot.
- Keep logical identity independent of storage layout.

Assume `tpch.orders` has `relation_id = 42` at snapshot `100`.

Its columns have relation-scoped IDs that remain stable across temporal rows:

```text
relation_id  column_id  name             ordinal  type
-----------  ---------  ---------------  -------  ------------
42           1          o_orderkey       1        bigint
42           2          o_custkey        2        bigint
42           3          o_orderstatus    3        char(1)
42           4          o_totalprice     4        decimal(15,2)
42           5          o_orderdate      5        date
```

The primary key is represented as one constraint row:

```text
constraint 501: PRIMARY_KEY
fields = [{column_id: 1}]
backing_index_id = 701
```

The customer foreign key is:

```text
constraint 502: FOREIGN_KEY to customer primary constraint
referenced_relation_id = 7
fields = [{column_id: 2, referenced_column_id: 1}]
```

The customer lookup index is independent:

```text
index 702: btree, READY
fields = [{role: KEY, column_id: 2, direction: ASC}]
```

A file-backed version may use:

```text
partition spec 801: MONTH(o_orderdate)
sort order 901: o_custkey ASC, o_orderdate DESC
```

Each data file records spec `801`, its month value, sort order `901`, row count, byte size, and column min/max statistics. Table-level statistics record row count and distributions for `o_orderstatus`, `o_custkey`, and `o_orderdate`. A multicolumn statistics object may capture the relationship between customer and order status.

At planning time, the catalog resolves relation `42` at the query's snapshot and returns one descriptor containing the visible columns, trusted constraints, usable indexes, active partition specifications, file manifest, sort properties, and compatible statistics.
