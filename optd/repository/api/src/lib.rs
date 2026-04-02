pub use optd_repository_entity as entity;
use sea_orm::DbErr;

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

pub struct Repository {}

impl Repository {
    async fn create_table(&mut self, info: table::CreateTableInfo) -> Result<i64, DbErr> {
        todo!()
    }
    async fn drop_table(&mut self, info: table::DropTableInfo) -> Result<i64, DbErr> {
        todo!()
    }
    async fn get_table(&self, info: table::GetTableInfo) -> Result<table::TableInfo, DbErr> {
        todo!()
    }
    async fn get_all_tables(&self) -> Result<Vec<table::TableInfo>, DbErr> {
        todo!()
    }
    async fn create_schema(&self, info: schema::CreateSchemaInfo) -> Result<i64, DbErr> {
        todo!()
    }
    async fn drop_schema(&self, info: schema::DropSchemaInfo) -> Result<(), DbErr> {
        todo!()
    }
    async fn get_schema(&self, info: schema::GetSchemaInfo) -> Result<schema::SchemaInfo, DbErr> {
        todo!()
    }
    async fn get_all_schemas(&self) -> Result<Vec<schema::SchemaInfo>, DbErr> {
        todo!()
    }
}
