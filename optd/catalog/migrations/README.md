# Catalog migrations

Executable SeaORM migrations live in `src/migration/` and currently create:

- `optd_snapshot`
- `optd_branch`
- `optd_schema`
- `optd_relation`
- `optd_column`
- `optd_statistics`
- `optd_statistics_payload`
- `optd_column_statistics`

Temporal catalog entities use private auto-generated `row_id` primary keys for
SeaORM persistence only; durable catalog identity remains in stable object IDs.
The initial migration bootstraps snapshot `0` and the `main` branch; writes
publish new snapshots through an expected-base branch-head update.

Statistics payloads carry an explicit typed-value encoding version. A follow-up
migration adds that version column to catalogs created by the first statistics
migration, while fresh catalogs receive it directly from the current entity
schema.
