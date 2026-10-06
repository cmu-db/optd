# Catalog migrations

Executable SeaORM migrations live in `src/migration/` and currently create:

- `optd_snapshot`
- `optd_branch`
- `optd_schema`
- `optd_relation`
- `optd_column`
- `optd_index`
- `optd_statistics`
- `optd_statistics_payload`
- `optd_column_statistics`

Temporal catalog entities use private auto-generated `row_id` primary keys for
SeaORM persistence only; durable catalog identity remains in stable object IDs.
The initial migration bootstraps snapshot `0` and the `main` branch; writes
publish new snapshots through an expected-base branch-head update. Relational
index definitions store ordered key/include fields as JSON and are rejected for
file-backed relations.

Statistics definitions store their canonical sorted durable column IDs as a
JSON array; the implicit table definition uses an empty array. Statistics
payloads carry an explicit typed-value encoding version and use unified
`begin_snapshot`/`end_snapshot` visibility. The statistics migration
creates the current tables and composite non-unique indexes for definition
visibility, payload visibility, and per-payload column lookup. During this
initial development phase, incompatible local SQLite catalogs should be deleted
and recreated rather than upgraded through intermediate schema migrations.
