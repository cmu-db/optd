# Catalog migrations

The executable migration currently lives in `src/migration/initial.rs`. It was
ported from `origin/spring26` as a working baseline and creates:

- `optd_snapshot`
- `optd_branch`
- `optd_schema`
- `optd_relation`
- `optd_column`

Temporal catalog entities use private auto-generated `row_id` primary keys for
SeaORM persistence only; durable catalog identity remains in the stable object
IDs. The migration bootstraps snapshot `0` and the `main` branch; writes publish
new snapshots through an expected-base branch-head update.
