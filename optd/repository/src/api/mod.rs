pub mod schema;
pub mod snapshot;
pub mod table;

pub enum RepositoryApi {
    CreateTable(table::CreateTableInfo),
    DropTable(table::DropTableInfo),
    GetTable(table::GetTableInfo),
    GetAllTables,
    CreateSchema(schema::CreateSchemaInfo),
    DropSchema(schema::DropSchemaInfo),
    GetSchema(schema::GetSchemaInfo),
    GetAllSchemas,
}
