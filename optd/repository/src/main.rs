use std::collections::HashMap;

use optd_core::ir::statistics::{ColumnStatistics, TableStatistics};
use optd_core::ir::table_ref::TableRef;
use optd_repository_api::{
    Repository,
    schema::{CreateSchemaInfo, DropSchemaInfo, GetSchemaInfo},
    stats::{GetTableStatsInfo, TableStatsInfo, UpdateTableStatsInfo},
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
            definition: None,
        })
        .await?;
    let fetched_table = repo.get_table(GetTableInfo { table_id }).await?;
    assert_eq!(fetched_table.table_name, table_name);
    assert!(
        repo.get_all_tables()
            .await?
            .iter()
            .any(|table| table.table_id == table_id)
    );

    let initial_stats = TableStatistics {
        row_count: 42,
        size_bytes: Some(4096),
        column_statistics: HashMap::from([
            (
                fetched_table.columns[0].column_id as usize,
                ColumnStatistics {
                    min_value: Some("1".to_owned()),
                    max_value: Some("42".to_owned()),
                    null_count: Some(0),
                    distinct_count: Some(42),
                    advanced_stats: vec![],
                },
            ),
            (
                fetched_table.columns[1].column_id as usize,
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

    assert_eq!(
        repo.get_table_stats(GetTableStatsInfo { table_id }).await?,
        None
    );

    repo.update_table_stats(UpdateTableStatsInfo {
        table_id,
        stats: initial_stats.clone(),
    })
    .await?;

    assert_eq!(
        repo.get_table_stats(GetTableStatsInfo { table_id }).await?,
        Some(TableStatsInfo {
            table_id,
            stats: initial_stats.clone(),
        })
    );
    assert_eq!(
        repo.get_all_table_stats().await?,
        vec![TableStatsInfo {
            table_id,
            stats: initial_stats,
        }]
    );

    let updated_stats = TableStatistics {
        row_count: 84,
        size_bytes: Some(8192),
        column_statistics: HashMap::from([
            (
                fetched_table.columns[0].column_id as usize,
                ColumnStatistics {
                    min_value: Some("10".to_owned()),
                    max_value: Some("99".to_owned()),
                    null_count: Some(0),
                    distinct_count: Some(64),
                    advanced_stats: vec![],
                },
            ),
            (
                fetched_table.columns[1].column_id as usize,
                ColumnStatistics {
                    min_value: Some("\"beta\"".to_owned()),
                    max_value: Some("\"zeta\"".to_owned()),
                    null_count: Some(1),
                    distinct_count: Some(17),
                    advanced_stats: vec![],
                },
            ),
        ]),
    };

    repo.update_table_stats(UpdateTableStatsInfo {
        table_id,
        stats: updated_stats.clone(),
    })
    .await?;

    assert_eq!(
        repo.get_table_stats(GetTableStatsInfo { table_id }).await?,
        Some(TableStatsInfo {
            table_id,
            stats: updated_stats.clone(),
        })
    );
    assert_eq!(
        repo.get_all_table_stats().await?,
        vec![TableStatsInfo {
            table_id,
            stats: updated_stats,
        }]
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
