# Catalog implementation handoff

## Decision summary

- Build the catalog in `optd/catalog` with package name `optd-catalog`.
- Reuse the useful architecture from `origin/spring26`: snapshot-pinned readers, transactional mutations, temporal visibility, and memory-backed tests.
- Use SeaORM 2 for connections, transactions, row decoding, and migrations.
- Use SeaQuery for explicit temporal predicates, branch locking, interval updates, and bulk writes.
- Do not adopt Toasty. Its ORM/query-engine layer does not supply snapshot, manifest, or catalog commit semantics and overlaps with Optd's own planner.

## Data model decisions

- Internal tables use the `optd_` prefix.
- Temporal rows carry stable IDs directly; there are no separate `*_object` and `*_version` tables.
- Temporal SeaORM entities may have a private auto-generated `row_id` primary key used only by the persistence adapter. It is never exposed as catalog identity or used for snapshot visibility.
- Schema and relation IDs are catalog-scoped. Column IDs are relation-scoped, remain stable across temporal versions and renames, are never reused within a relation, and are distinct from mutable ordinals. Cross-relation APIs identify a column with `(relation_id, column_id)`.
- Visibility uses half-open intervals: `begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)`.
- Creation is the earliest `begin_snapshot`; there is no duplicated `created_snapshot`.
- Tables, logical views, and materialized views are represented by `optd_relation` with different relation and storage kinds.
- Small, bounded, ordered components use arrays of typed records rather than child-table joins or parallel arrays.
- View definitions initially store authoritative SQL and are parsed and bound when expanded against a pinned catalog snapshot. A parsed AST or resolved node tree may be added later only as a disposable, versioned cache.
- Non-query expressions remain inline with their owning rows; their final typed encoding is deferred until defaults, checks, generated columns, or expression indexes are implemented.
- Constraints express logical truth; indexes express physical access. A primary or unique constraint can refer to a backing index, while a foreign key refers to a primary or unique constraint.
- Current physical index state is folded into `optd_index`. A generation table can be added later if simultaneous published builds become necessary.
- Files, row groups, statistics payloads, dependencies, snapshots, and changes remain separate because they are unbounded or independently managed.

## First implementation slice

Implement only:

1. `optd_snapshot`
2. `optd_branch`
3. `optd_schema`
4. `optd_relation`

Initially support the linear `main` branch. Divergent branches require ancestry-aware visibility and are out of scope for interval-based visibility.

Required behavior:

- Bootstrap snapshot `0` and branch `main`.
- Pin readers to one immutable snapshot.
- Lock the `main` branch row with `FOR UPDATE` before publishing.
- Compare the expected base snapshot with the locked branch head.
- Create, rename, and drop schemas and relations by inserting and closing temporal rows.
- Advance the branch head in the same transaction.
- Reject stale writers and leave the branch unchanged after failed commits.

Add `optd_column` only after choosing the first persisted type representation.

## Provisional view-definition decision

Start with `optd_view_definition.definition_sql` rather than a persisted bound query tree. View creation or replacement validates the SQL and records explicit dependency edges; view expansion parses and binds the SQL using the reader's pinned snapshot. This avoids committing the catalog to Optd arena-handle serialization before the planner integration is stable.

Revisit this choice only after measuring expansion cost or identifying a requirement that SQL cannot satisfy. A future parsed AST or resolved tree must be versioned, tied to its dependency snapshot, and safely rebuildable from the authoritative SQL.

## Verification priorities

- Half-open visibility boundaries.
- Historical reads before and after rename/drop.
- Atomic commit rollback.
- Two-writer stale-base conflict.
- Name uniqueness at the target snapshot.
- View SQL is validated and dependencies are recorded at create or replace time.
- Any future compiled view artifact is invalidated when its encoding or dependency snapshot changes.

The complete model is documented in [catalog_design.md](catalog_design.md). Nested record contracts are documented in [catalog_fields.schema.json](catalog_fields.schema.json).
