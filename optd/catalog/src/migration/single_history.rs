//! Non-destructive upgrade of the original single-main history.
use crate::entity;
use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, Schema, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    // SQLite does not wrap migrations by default. DDL, backfill, and migration bookkeeping
    // must either all succeed or all roll back on both supported backends.
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        let backend = manager.get_database_backend();
        let rows = db
            .query_all_raw(Statement::from_string(
                backend,
                "SELECT branch_name, current_snapshot_id FROM optd_branch",
            ))
            .await?;
        let latest = db
            .query_one_raw(Statement::from_string(
                backend,
                "SELECT MAX(snapshot_id) AS latest FROM optd_snapshot",
            ))
            .await?
            .ok_or_else(|| DbErr::Custom("missing legacy snapshots".into()))?
            .try_get::<i64>("", "latest")?;
        if rows.len() != 1
            || rows[0].try_get::<String>("", "branch_name")? != "main"
            || rows[0].try_get::<i64>("", "current_snapshot_id")? != latest
        {
            return Err(DbErr::Custom("single-history migration requires exactly main at the latest snapshot; restore/resolve other histories explicitly".into()));
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("optd_snapshot"))
                    .add_column(
                        ColumnDef::new(Alias::new("schema_version"))
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("optd_schema"))
                    .add_column(
                        ColumnDef::new(Alias::new("catalog_id"))
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;
        let schema = Schema::new(backend);
        for statement in [
            schema
                .create_table_from_entity(entity::source_catalog::Entity)
                .to_owned(),
            schema
                .create_table_from_entity(entity::snapshot_change::Entity)
                .to_owned(),
            schema
                .create_table_from_entity(entity::metadata::Entity)
                .to_owned(),
        ] {
            manager.create_table(statement).await?;
        }
        db.execute_unprepared(
            "INSERT INTO optd_catalog_metadata (id, format_version) VALUES (1, 1)",
        )
        .await?;
        db.execute_unprepared("INSERT INTO optd_catalog (catalog_id, begin_snapshot, catalog_name, catalog_kind, adapter_name, connection_profile_reference) VALUES (0, 0, 'datafusion', 'datafusion', 'datafusion', 'runtime://datafusion')").await?;
        db.execute_unprepared("UPDATE optd_snapshot SET schema_version = (SELECT COUNT(*) FROM optd_snapshot AS s WHERE s.snapshot_id > 0 AND s.snapshot_id <= optd_snapshot.snapshot_id AND (EXISTS (SELECT 1 FROM optd_schema AS m WHERE m.begin_snapshot = s.snapshot_id OR m.end_snapshot = s.snapshot_id) OR EXISTS (SELECT 1 FROM optd_relation AS m WHERE m.begin_snapshot = s.snapshot_id OR m.end_snapshot = s.snapshot_id) OR EXISTS (SELECT 1 FROM optd_column AS m WHERE m.begin_snapshot = s.snapshot_id OR m.end_snapshot = s.snapshot_id) OR EXISTS (SELECT 1 FROM optd_index AS m WHERE m.begin_snapshot = s.snapshot_id OR m.end_snapshot = s.snapshot_id)))").await?;
        db.execute_unprepared("INSERT INTO optd_snapshot_changes (snapshot_id, changes_made) SELECT snapshot_id, '{\"kind\":\"legacy_import\"}' FROM optd_snapshot").await?;
        for (name, table, columns) in [
            (
                "idx_optd_catalog_version",
                "optd_catalog",
                vec!["catalog_id", "begin_snapshot"],
            ),
            (
                "idx_optd_schema_version",
                "optd_schema",
                vec!["schema_id", "begin_snapshot"],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table(Alias::new(table)).unique();
            for column in columns {
                index.col(Alias::new(column));
            }
            manager.create_index(index.to_owned()).await?;
        }
        manager
            .drop_table(Table::drop().table(Alias::new("optd_branch")).to_owned())
            .await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom("single-history upgrade cannot be downgraded safely; restore a pre-upgrade backup into the old binary".into()))
    }
}
