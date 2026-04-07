pub use optd_repository_entity as entity;
use optd_repository_entity::snapshot_changes::ChangesMade;
use sea_orm::{ConnectionTrait, DbErr, TransactionTrait};

pub mod schema;
pub mod snapshot;
pub mod stats;
pub mod table;

pub enum RepositoryRequest {
    CreateTable(table::CreateTableInfo),
    DropTable(table::DropTableInfo),
    GetTable(table::GetTableInfo),
    GetAllTables,
    CreateSchema(schema::CreateSchemaInfo),
    DropSchema(schema::DropSchemaInfo),
    GetSchema(schema::GetSchemaInfo),
    GetAllSchemas,
    UpdateTableStats(stats::UpdateTableStatsInfo),
    GetTableStats(stats::GetTableStatsInfo),
    GetAllTableStats,
}

pub struct Repository<T> {
    db: T,
}

fn flatten_transaction_err<T>(
    result: Result<T, sea_orm::TransactionError<DbErr>>,
) -> Result<T, DbErr> {
    result.map_err(|err| match err {
        sea_orm::TransactionError::Connection(err)
        | sea_orm::TransactionError::Transaction(err) => err,
    })
}

impl<T> Repository<T> {
    pub fn new(db: T) -> Self {
        Self { db }
    }
}

impl<T: TransactionTrait> Repository<T> {
    pub async fn create_table(&mut self, info: table::CreateTableInfo) -> Result<i64, DbErr> {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let changes_made = ChangesMade::CreateTable(info.table_name.to_string());
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    let table_id = table::create_table(info, txn, &mut current_snapshot).await?;
                    let new_snapshot_id = snapshot::commit_snapshot(txn, current_snapshot).await?;
                    snapshot::log_snapshot_changes(txn, new_snapshot_id, &[changes_made]).await?;
                    Ok(table_id)
                })
            })
            .await)
    }

    pub async fn drop_table(&mut self, info: table::DropTableInfo) -> Result<i64, DbErr> {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let table_id = info.table_id;
                    let changes_made = ChangesMade::DropTable(table_id);
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    table::drop_table(info, txn, &mut current_snapshot).await?;
                    let new_snapshot_id = snapshot::commit_snapshot(txn, current_snapshot).await?;
                    snapshot::log_snapshot_changes(txn, new_snapshot_id, &[changes_made]).await?;
                    Ok(table_id)
                })
            })
            .await)
    }

    pub async fn get_all_tables(&self) -> Result<Vec<table::TableInfo>, DbErr>
    where
        T: ConnectionTrait,
    {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    table::get_all_table_infos(txn, &mut current_snapshot).await
                })
            })
            .await)
    }

    pub async fn get_table(&self, info: table::GetTableInfo) -> Result<table::TableInfo, DbErr>
    where
        T: ConnectionTrait,
    {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let table_id = info.table_id;
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    table::get_table(info, txn, &mut current_snapshot)
                        .await?
                        .ok_or_else(|| DbErr::RecordNotFound(format!("table {table_id} not found")))
                })
            })
            .await)
    }

    pub async fn create_schema(&self, info: schema::CreateSchemaInfo) -> Result<i64, DbErr> {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let changes_made = ChangesMade::CreateSchema(info.schema_name.clone());
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    let schema_id =
                        schema::create_new_schema(info, txn, &mut current_snapshot).await?;
                    let new_snapshot_id = snapshot::commit_snapshot(txn, current_snapshot).await?;
                    snapshot::log_snapshot_changes(txn, new_snapshot_id, &[changes_made]).await?;
                    Ok(schema_id)
                })
            })
            .await)
    }

    pub async fn drop_schema(&self, info: schema::DropSchemaInfo) -> Result<(), DbErr> {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let changes_made = ChangesMade::DropSchema(info.schema_id);
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    schema::drop_schema(info, txn, &mut current_snapshot).await?;
                    let new_snapshot_id = snapshot::commit_snapshot(txn, current_snapshot).await?;
                    snapshot::log_snapshot_changes(txn, new_snapshot_id, &[changes_made]).await?;
                    Ok(())
                })
            })
            .await)
    }

    pub async fn get_schema(&self, info: schema::GetSchemaInfo) -> Result<schema::SchemaInfo, DbErr>
    where
        T: ConnectionTrait,
    {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let schema_id = info.schema_id;
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    schema::get_schema(info, txn, &mut current_snapshot)
                        .await?
                        .ok_or_else(|| DbErr::RecordNotFound(format!("schema {schema_id} not found")))
                })
            })
            .await)
    }

    pub async fn get_all_schemas(&self) -> Result<Vec<schema::SchemaInfo>, DbErr>
    where
        T: ConnectionTrait,
    {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    schema::get_all_schema_infos(txn, &mut current_snapshot).await
                })
            })
            .await)
    }

    pub async fn update_table_stats(
        &mut self,
        info: stats::UpdateTableStatsInfo,
    ) -> Result<(), DbErr>
    where
        T: ConnectionTrait,
    {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let changes_made = ChangesMade::UpdateTableStats(info.table_id);
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    stats::update_table_stats(info, txn, &mut current_snapshot).await?;
                    let new_snapshot_id = snapshot::commit_snapshot(txn, current_snapshot).await?;
                    snapshot::log_snapshot_changes(txn, new_snapshot_id, &[changes_made]).await?;
                    Ok(())
                })
            })
            .await)
    }

    pub async fn get_table_stats(
        &self,
        info: stats::GetTableStatsInfo,
    ) -> Result<Option<stats::TableStatsInfo>, DbErr>
    where
        T: ConnectionTrait,
    {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    stats::get_table_stats(info, txn, &mut current_snapshot).await
                })
            })
            .await)
    }

    pub async fn get_all_table_stats(&self) -> Result<Vec<stats::TableStatsInfo>, DbErr>
    where
        T: ConnectionTrait,
    {
        flatten_transaction_err(self.db
            .transaction::<_, _, DbErr>(|txn| {
                Box::pin(async move {
                    let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                        .await?
                        .unwrap_or_default();
                    stats::get_all_table_stats(txn, &mut current_snapshot).await
                })
            })
            .await)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use optd_core::ir::statistics::{ColumnStatistics, TableStatistics};
    use optd_core::ir::table_ref::TableRef;
    use optd_repository_entity::{column::ColumnType, prelude::SnapshotChanges};
    use optd_repository_migration::{Migrator, MigratorTrait};
    use sea_orm::{Database, DatabaseConnection, EntityTrait};
    use uuid::Uuid;

    use super::*;

    async fn setup_repository()
    -> Result<(Repository<DatabaseConnection>, DatabaseConnection), DbErr> {
        let db = Database::connect("sqlite::memory:").await?;
        Migrator::up(&db, None).await?;
        Ok((Repository { db: db.clone() }, db))
    }

    async fn current_snapshot_id(db: &DatabaseConnection) -> Result<i64, DbErr> {
        Ok(snapshot::get_current_snapshot_info(db)
            .await?
            .expect("migration should create the initial snapshot")
            .snapshot_id)
    }

    async fn latest_changes(db: &DatabaseConnection) -> Result<String, DbErr> {
        let snapshot_id = current_snapshot_id(db).await?;
        Ok(SnapshotChanges::find_by_id(snapshot_id)
            .one(db)
            .await?
            .expect("repository writes should log snapshot changes")
            .changes_made)
    }

    #[tokio::test]
    async fn repository_schema_lifecycle_commits_snapshots_and_logs_changes() -> Result<(), DbErr> {
        let (repo, db) = setup_repository().await?;
        let initial_snapshot_id = current_snapshot_id(&db).await?;

        assert!(repo.get_all_schemas().await?.is_empty());

        let schema_name = format!("schema_{}", Uuid::new_v4().simple());
        let schema_id = repo
            .create_schema(schema::CreateSchemaInfo {
                schema_name: schema_name.clone(),
            })
            .await?;

        assert!(current_snapshot_id(&db).await? > initial_snapshot_id);
        assert_eq!(
            latest_changes(&db).await?,
            format!("created_schema:{schema_name}")
        );

        let fetched_schema = repo.get_schema(schema::GetSchemaInfo { schema_id }).await?;
        assert_eq!(fetched_schema.schema_id, schema_id);
        assert_eq!(fetched_schema.schema_name, schema_name);
        assert!(
            repo.get_all_schemas()
                .await?
                .iter()
                .any(|schema| schema.schema_id == schema_id)
        );

        let snapshot_after_create = current_snapshot_id(&db).await?;
        repo.drop_schema(schema::DropSchemaInfo { schema_id })
            .await?;

        assert!(current_snapshot_id(&db).await? > snapshot_after_create);
        assert_eq!(
            latest_changes(&db).await?,
            format!("dropped_schema:{schema_id}")
        );
        assert!(matches!(
            repo.get_schema(schema::GetSchemaInfo { schema_id }).await,
            Err(DbErr::RecordNotFound(message)) if message == format!("schema {schema_id} not found")
        ));

        Ok(())
    }

    #[tokio::test]
    async fn repository_table_lifecycle_commits_snapshots_and_logs_changes() -> Result<(), DbErr> {
        let (mut repo, db) = setup_repository().await?;
        let schema_name = format!("schema_{}", Uuid::new_v4().simple());
        let table_name = format!("table_{}", Uuid::new_v4().simple());
        let schema_id = repo
            .create_schema(schema::CreateSchemaInfo {
                schema_name: schema_name.clone(),
            })
            .await?;
        let initial_table_snapshot = current_snapshot_id(&db).await?;

        assert!(repo.get_all_tables().await?.is_empty());

        let table_ref = TableRef::partial(schema_name.clone(), table_name.clone());
        let table_id = repo
            .create_table(table::CreateTableInfo {
                table_name: table_ref.clone(),
                columns: vec![
                    table::CreateColumnInfo {
                        column_name: "id".to_owned(),
                        column_type: ColumnType(arrow_schema::DataType::Int64),
                        nulls_allowed: false,
                        initial_default: None,
                        default_value: None,
                    },
                    table::CreateColumnInfo {
                        column_name: "note".to_owned(),
                        column_type: ColumnType(arrow_schema::DataType::Utf8),
                        nulls_allowed: true,
                        initial_default: None,
                        default_value: None,
                    },
                ],
            })
            .await?;

        assert!(current_snapshot_id(&db).await? > initial_table_snapshot);
        assert_eq!(
            latest_changes(&db).await?,
            format!("created_table:{table_ref}")
        );

        let fetched_table = repo.get_table(table::GetTableInfo { table_id }).await?;
        assert_eq!(fetched_table.table_id, table_id);
        assert_eq!(fetched_table.schema_id, schema_id);
        assert_eq!(fetched_table.table_name, table_name);
        assert_eq!(fetched_table.columns.len(), 2);
        assert_eq!(fetched_table.columns[0].column_name, "id");
        assert_eq!(fetched_table.columns[1].column_name, "note");
        assert!(
            repo.get_all_tables()
                .await?
                .iter()
                .any(|table| table.table_id == table_id)
        );

        let snapshot_after_create = current_snapshot_id(&db).await?;
        assert_eq!(
            repo.drop_table(table::DropTableInfo { table_id }).await?,
            table_id
        );

        assert!(current_snapshot_id(&db).await? > snapshot_after_create);
        assert_eq!(
            latest_changes(&db).await?,
            format!("dropped_table:{table_id}")
        );
        assert!(matches!(
            repo.get_table(table::GetTableInfo { table_id }).await,
            Err(DbErr::RecordNotFound(message)) if message == format!("table {table_id} not found")
        ));
        assert!(
            repo.get_all_tables()
                .await?
                .iter()
                .all(|table| table.table_id != table_id)
        );

        Ok(())
    }

    #[tokio::test]
    async fn repository_table_stats_lifecycle_commits_snapshots_and_logs_changes(
    ) -> Result<(), DbErr> {
        let (mut repo, db) = setup_repository().await?;
        let schema_name = format!("schema_{}", Uuid::new_v4().simple());
        let table_name = format!("table_{}", Uuid::new_v4().simple());
        repo.create_schema(schema::CreateSchemaInfo { schema_name: schema_name.clone() })
            .await?;

        let table_id = repo
            .create_table(table::CreateTableInfo {
                table_name: TableRef::partial(schema_name, table_name),
                columns: vec![
                    table::CreateColumnInfo {
                        column_name: "id".to_owned(),
                        column_type: ColumnType(arrow_schema::DataType::Int64),
                        nulls_allowed: false,
                        initial_default: None,
                        default_value: None,
                    },
                    table::CreateColumnInfo {
                        column_name: "note".to_owned(),
                        column_type: ColumnType(arrow_schema::DataType::Utf8),
                        nulls_allowed: true,
                        initial_default: None,
                        default_value: None,
                    },
                ],
            })
            .await?;

        assert_eq!(
            repo.get_table_stats(stats::GetTableStatsInfo { table_id }).await?,
            None
        );
        assert!(repo.get_all_table_stats().await?.is_empty());

        let table = repo.get_table(table::GetTableInfo { table_id }).await?;
        let expected_stats = TableStatistics {
            row_count: 42,
            size_bytes: Some(4096),
            column_statistics: HashMap::from([
                (
                    table.columns[0].column_id as usize,
                    ColumnStatistics {
                        min_value: Some("1".to_owned()),
                        max_value: Some("42".to_owned()),
                        null_count: Some(0),
                        distinct_count: Some(42),
                        advanced_stats: vec![],
                    },
                ),
                (
                    table.columns[1].column_id as usize,
                    ColumnStatistics {
                        min_value: Some("\"alpha\"".to_owned()),
                        max_value: Some("\"omega\"".to_owned()),
                        null_count: Some(3),
                        distinct_count: Some(11),
                        advanced_stats: vec![],
                    },
                ),
            ]),
        };

        let snapshot_before_stats_update = current_snapshot_id(&db).await?;
        repo.update_table_stats(stats::UpdateTableStatsInfo {
            table_id,
            stats: expected_stats.clone(),
        })
        .await?;

        assert!(current_snapshot_id(&db).await? > snapshot_before_stats_update);
        assert_eq!(
            latest_changes(&db).await?,
            format!("updated_table_stats:{table_id}")
        );
        assert_eq!(
            repo.get_table_stats(stats::GetTableStatsInfo { table_id })
                .await?,
            Some(stats::TableStatsInfo {
                table_id,
                stats: expected_stats.clone(),
            })
        );
        assert_eq!(
            repo.get_all_table_stats().await?,
            vec![stats::TableStatsInfo {
                table_id,
                stats: expected_stats,
            }]
        );

        Ok(())
    }
}
