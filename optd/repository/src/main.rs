use optd_core::ir::table_ref::TableRef;
use optd_repository_api::{
    Repository,
    schema::{CreateSchemaInfo, DropSchemaInfo, GetSchemaInfo},
    table::{CreateColumnInfo, CreateTableInfo, DropTableInfo, GetTableInfo},
};
use optd_repository_entity::column::ColumnType;
use optd_repository_migration::Migrator;
use sea_orm::{Database, DatabaseConnection, DbErr};
use sea_orm_migration::MigratorTrait;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv()?;
    let database_url = std::env::var("DATABASE_URL")?;
    let db = Database::connect(database_url).await?;
    // let db = Database::connect("sqlite::memory:").await?;

    Migrator::up(&db, None).await?;

    smoke_test_all_apis(&db).await?;

    Ok(())
}

async fn smoke_test_all_apis(db: &DatabaseConnection) -> Result<(), Box<dyn std::error::Error>> {
    let mut repo = Repository::new(db.clone());
    let suffix = Uuid::new_v4().simple().to_string();
    let schema_name = format!("smoke_schema_{suffix}");
    let table_name = format!("smoke_table_{suffix}");

    let schema_id = repo
        .create_schema(CreateSchemaInfo {
            schema_name: schema_name.clone(),
        })
        .await?;
    assert_eq!(
        repo.get_schema(GetSchemaInfo { schema_id })
            .await?
            .schema_name,
        schema_name
    );
    assert!(
        repo.get_all_schemas()
            .await?
            .iter()
            .any(|schema| schema.schema_id == schema_id)
    );

    let table_id = repo
        .create_table(CreateTableInfo {
            table_name: TableRef::partial(schema_name.clone(), table_name.clone()),
            columns: vec![
                CreateColumnInfo {
                    column_name: "id".to_owned(),
                    column_type: ColumnType(arrow_schema::DataType::Int64),
                    nulls_allowed: false,
                    initial_default: None,
                    default_value: None,
                },
                CreateColumnInfo {
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
        repo.get_table(GetTableInfo { table_id }).await?.table_name,
        table_name
    );
    assert!(
        repo.get_all_tables()
            .await?
            .iter()
            .any(|table| table.table_id == table_id)
    );

    repo.drop_table(DropTableInfo { table_id }).await?;
    assert!(matches!(
        repo.get_table(GetTableInfo { table_id }).await,
        Err(DbErr::RecordNotFound(_))
    ));
    assert!(
        repo.get_all_tables()
            .await?
            .iter()
            .all(|table| table.table_id != table_id)
    );

    repo.drop_schema(DropSchemaInfo { schema_id }).await?;
    assert!(matches!(
        repo.get_schema(GetSchemaInfo { schema_id }).await,
        Err(DbErr::RecordNotFound(_))
    ));
    assert!(
        repo.get_all_schemas()
            .await?
            .iter()
            .all(|schema| schema.schema_id != schema_id)
    );

    Ok(())
}
