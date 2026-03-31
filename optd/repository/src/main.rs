use optd_core::ir::table_ref::TableRef;
use optd_repository_api::{
    schema::{
        CreateSchemaInfo, DropSchemaInfo, GetSchemaInfo, create_new_schema, drop_schema,
        get_all_schema_infos, get_schema,
    },
    snapshot::{commit_snapshot, get_current_snapshot_info},
    table::{
        CreateColumnInfo, CreateTableInfo, DropTableInfo, GetTableInfo, create_table, drop_table,
        get_all_table_infos, get_table,
    },
};
use optd_repository_entity::column::ColumnType;
use optd_repository_migration::Migrator;
use sea_orm_migration::MigratorTrait;
use sea_orm::{Database, DatabaseConnection};
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
    let mut current_snapshot = get_current_snapshot_info(db)
        .await?
        .expect("migration should initialize the first snapshot");

    let suffix = Uuid::new_v4().simple().to_string();
    let schema_name = format!("smoke_schema_{suffix}");
    let table_name = format!("smoke_table_{suffix}");

    let schema_id = create_new_schema(
        CreateSchemaInfo {
            schema_name: schema_name.clone(),
        },
        db,
        &mut current_snapshot,
    )
    .await?;
    assert_eq!(
        get_schema(GetSchemaInfo { schema_id }, db, &mut current_snapshot)
            .await?
            .expect("created schema should be visible")
            .schema_name,
        schema_name
    );
    assert!(
        get_all_schema_infos(db, &mut current_snapshot)
            .await?
            .iter()
            .any(|schema| schema.schema_id == schema_id)
    );

    commit_snapshot(db, current_snapshot.clone()).await?;
    current_snapshot = get_current_snapshot_info(db)
        .await?
        .expect("committed schema snapshot should be visible");

    let table_id = create_table(
        CreateTableInfo {
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
        },
        db,
        &mut current_snapshot,
    )
    .await?;
    assert_eq!(
        get_table(GetTableInfo { table_id }, db, &mut current_snapshot)
            .await?
            .expect("created table should be visible")
            .table_name,
        table_name
    );
    assert!(
        get_all_table_infos(db, &mut current_snapshot)
            .await?
            .iter()
            .any(|table| table.table_id == table_id)
    );

    commit_snapshot(db, current_snapshot.clone()).await?;
    current_snapshot = get_current_snapshot_info(db)
        .await?
        .expect("committed table snapshot should be visible");

    drop_table(DropTableInfo { table_id }, db, &mut current_snapshot).await?;
    assert!(
        get_table(GetTableInfo { table_id }, db, &mut current_snapshot)
            .await?
            .is_none()
    );

    commit_snapshot(db, current_snapshot.clone()).await?;
    current_snapshot = get_current_snapshot_info(db)
        .await?
        .expect("committed drop-table snapshot should be visible");

    drop_schema(DropSchemaInfo { schema_id }, db, &mut current_snapshot).await?;
    assert!(
        get_schema(GetSchemaInfo { schema_id }, db, &mut current_snapshot)
            .await?
            .is_none()
    );

    Ok(())
}
