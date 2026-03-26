use arrow_schema::DataType;
use optd_repository::{
    api::{
        schema::{create_new_schema, drop_schemas, get_all_schema_infos},
        snapshot::{SnapshotInfo, commit_snapshot, get_current_snapshot_info},
        table::{ColumnInfo, TableInfo, create_tables, drop_tables, get_all_table_infos},
    },
    entity::column::ColumnType,
};
use sea_orm::{Database, DatabaseConnection};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv()?;
    let database_url = std::env::var("DATABASE_URL")?;
    let db = Database::connect(database_url).await?;
    // let db = Database::connect("sqlite::memory:").await?;

    db.get_schema_registry("optd_repository::entity::*")
        .sync(&db)
        .await?;

    smoke_test_all_apis(&db).await?;

    Ok(())
}

async fn smoke_test_all_apis(db: &DatabaseConnection) -> Result<(), Box<dyn std::error::Error>> {
    commit_snapshot(db, SnapshotInfo::default()).await?;

    let mut snapshot = get_current_snapshot_info(db)
        .await?
        .expect("snapshot should exist after initial commit");
    assert!(snapshot.snapshot_id > 0);

    let suffix = Uuid::new_v4().simple().to_string();
    let table_schema_name = format!("api_smoke_schema_{suffix}");
    let empty_schema_name = format!("api_smoke_empty_schema_{suffix}");
    let table_name = format!("api_smoke_table_{suffix}");

    create_new_schema(&table_schema_name, db, &mut snapshot).await?;
    create_new_schema(&empty_schema_name, db, &mut snapshot).await?;

    let schemas = get_all_schema_infos(db, &mut snapshot).await?;
    let table_schema = schemas
        .iter()
        .find(|schema| schema.schema_name == table_schema_name)
        .expect("created table schema should be visible");
    let empty_schema = schemas
        .iter()
        .find(|schema| schema.schema_name == empty_schema_name)
        .expect("created empty schema should be visible");

    create_tables(
        &[TableInfo {
            table_id: 0,
            schema_id: table_schema.schema_id,
            table_uuid: Uuid::nil(),
            table_name: table_name.clone(),
            columns: vec![
                ColumnInfo {
                    column_id: 0,
                    column_name: "id".to_owned(),
                    column_type: ColumnType(DataType::Int32),
                    initial_default: None,
                    default_value: None,
                    nulls_allowed: false,
                    children: vec![],
                },
                ColumnInfo {
                    column_id: 0,
                    column_name: "payload".to_owned(),
                    column_type: ColumnType(DataType::Struct(
                        vec![arrow_schema::Field::new(
                            "region",
                            DataType::Utf8,
                            false,
                        )]
                        .into(),
                    )),
                    initial_default: None,
                    default_value: None,
                    nulls_allowed: true,
                    children: vec![ColumnInfo {
                        column_id: 0,
                        column_name: "region".to_owned(),
                        column_type: ColumnType(DataType::Utf8),
                        initial_default: None,
                        default_value: None,
                        nulls_allowed: false,
                        children: vec![],
                    }],
                },
            ],
        }],
        db,
        &mut snapshot,
    )
    .await?;

    let tables = get_all_table_infos(db, &mut snapshot).await?;
    let created_table = tables
        .iter()
        .find(|table| {
            table.schema_id == table_schema.schema_id && table.table_name == table_name
        })
        .expect("created table should be visible");
    assert_eq!(created_table.columns.len(), 2);
    assert_eq!(created_table.columns[0].column_type, ColumnType(DataType::Int32));
    assert_eq!(
        created_table.columns[1].children[0].column_type,
        ColumnType(DataType::Utf8)
    );

    let drop_schema_error = drop_schemas(&[table_schema.schema_id], db, &mut snapshot)
        .await
        .expect_err("schema drop should fail while an active table still exists");
    assert!(
        drop_schema_error
            .to_string()
            .contains("active table"),
        "unexpected schema drop error: {drop_schema_error}"
    );

    drop_tables(&[created_table.table_id], db, &mut snapshot).await?;
    let tables_after_drop = get_all_table_infos(db, &mut snapshot).await?;
    assert!(
        tables_after_drop
            .iter()
            .all(|table| table.table_id != created_table.table_id)
    );

    drop_schemas(
        &[table_schema.schema_id, empty_schema.schema_id],
        db,
        &mut snapshot,
    )
    .await?;

    let schemas_after_drop = get_all_schema_infos(db, &mut snapshot).await?;
    assert!(
        schemas_after_drop
            .iter()
            .all(|schema| schema.schema_id != table_schema.schema_id
                && schema.schema_id != empty_schema.schema_id)
    );

    commit_snapshot(db, snapshot.clone()).await?;
    let latest_snapshot = get_current_snapshot_info(db)
        .await?
        .expect("latest snapshot should exist");
    assert_eq!(latest_snapshot.next_catalog_id, snapshot.next_catalog_id);

    Ok(())
}
