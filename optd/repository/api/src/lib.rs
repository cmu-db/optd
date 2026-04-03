pub use optd_repository_entity as entity;
use optd_repository_entity::snapshot_changes::ChangesMade;
use sea_orm::{ConnectionTrait, DbErr, TransactionTrait};

pub mod schema;
pub mod snapshot;
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
}

pub struct Repository<T> {
    db: T,
}

impl<T: TransactionTrait> Repository<T> {
    pub async fn create_table(&mut self, info: table::CreateTableInfo) -> Result<i64, DbErr> {
        self.db
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
            .await
            .map_err(|err| match err {
                sea_orm::TransactionError::Connection(err)
                | sea_orm::TransactionError::Transaction(err) => err,
            })
    }

    pub async fn drop_table(&mut self, info: table::DropTableInfo) -> Result<i64, DbErr> {
        self.db
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
            .await
            .map_err(|err| match err {
                sea_orm::TransactionError::Connection(err)
                | sea_orm::TransactionError::Transaction(err) => err,
            })
    }

    pub async fn get_all_tables(&self) -> Result<Vec<table::TableInfo>, DbErr>
    where
        T: ConnectionTrait,
    {
        let mut current_snapshot = snapshot::get_current_snapshot_info(&self.db)
            .await?
            .unwrap_or_default();
        table::get_all_table_infos(&self.db, &mut current_snapshot).await
    }

    pub async fn get_table(&self, info: table::GetTableInfo) -> Result<table::TableInfo, DbErr> {
        todo!()
    }
    pub async fn create_schema(&self, info: schema::CreateSchemaInfo) -> Result<i64, DbErr> {
        todo!()
    }
    pub async fn drop_schema(&self, info: schema::DropSchemaInfo) -> Result<(), DbErr> {
        todo!()
    }
    pub async fn get_schema(
        &self,
        info: schema::GetSchemaInfo,
    ) -> Result<schema::SchemaInfo, DbErr> {
        todo!()
    }
    pub async fn get_all_schemas(&self) -> Result<Vec<schema::SchemaInfo>, DbErr> {
        todo!()
    }
}
