use std::sync::Arc;

use arrow_schema::SchemaRef;
use optd_core::ir::{
    catalog::{Catalog, CatalogError, DataSourceId, TableMetadata},
    statistics::TableStatistics,
    table_ref::{ResolvedTableRef, TableRef},
};
use sea_orm::DatabaseConnection;

use crate::{
    Repository,
    entity::column::ColumnType,
    schema::{CreateSchemaInfo, SchemaInfo},
    stats::UpdateTableStatsInfo,
    table::{CreateColumnInfo, CreateTableInfo, GetTableInfo, TableInfo},
};

pub struct RepositoryCatalog {
    db: DatabaseConnection,
    default_catalog: String,
    default_schema: String,
}

impl RepositoryCatalog {
    pub fn new(
        db: DatabaseConnection,
        default_catalog: impl Into<String>,
        default_schema: impl Into<String>,
    ) -> Self {
        Self {
            db,
            default_catalog: default_catalog.into(),
            default_schema: default_schema.into(),
        }
    }

    fn backend_error(err: impl std::fmt::Display) -> CatalogError {
        CatalogError::Backend {
            message: err.to_string(),
        }
    }

    fn block_on<F, T>(&self, future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            tokio::runtime::Runtime::new()
                .expect("failed to create tokio runtime for repository catalog")
                .block_on(future)
        }
    }

    fn resolved_table(&self, table: TableRef) -> Result<ResolvedTableRef, CatalogError> {
        let resolved = table.resolve(&self.default_catalog, &self.default_schema);
        if resolved.catalog.as_ref() != self.default_catalog {
            return Err(CatalogError::Backend {
                message: format!(
                    "catalog '{}' is not supported; expected '{}'",
                    resolved.catalog, self.default_catalog
                ),
            });
        }
        Ok(resolved)
    }

    fn find_schema_by_name(&self, schema_name: &str) -> Result<Option<SchemaInfo>, CatalogError> {
        let db = self.db.clone();
        let schema_name = schema_name.to_owned();
        self.block_on(async move {
            let repo = Repository::new(db);
            repo.get_all_schemas()
                .await
                .map(|schemas| {
                    schemas
                        .into_iter()
                        .find(|schema| schema.schema_name == schema_name)
                })
                .map_err(Self::backend_error)
        })
    }

    fn ensure_schema(&self, schema_name: &str) -> Result<SchemaInfo, CatalogError> {
        if let Some(schema) = self.find_schema_by_name(schema_name)? {
            return Ok(schema);
        }

        let db = self.db.clone();
        let schema_name = schema_name.to_owned();
        self.block_on(async move {
            let repo = Repository::new(db);
            repo.create_schema(CreateSchemaInfo {
                schema_name: schema_name.clone(),
            })
            .await
            .map_err(Self::backend_error)?;
            repo.get_all_schemas()
                .await
                .map_err(Self::backend_error)?
                .into_iter()
                .find(|schema| schema.schema_name == schema_name)
                .ok_or_else(|| CatalogError::Backend {
                    message: format!("created schema '{schema_name}' but could not resolve it"),
                })
        })
    }

    fn find_table_by_resolved_ref(
        &self,
        table: &ResolvedTableRef,
    ) -> Result<Option<TableInfo>, CatalogError> {
        let Some(schema) = self.find_schema_by_name(&table.schema)? else {
            return Ok(None);
        };

        let db = self.db.clone();
        let schema_id = schema.schema_id;
        let table_name = table.table.to_string();
        self.block_on(async move {
            let repo = Repository::new(db);
            repo.get_all_tables()
                .await
                .map(|tables| {
                    tables.into_iter().find(|candidate| {
                        candidate.schema_id == schema_id && candidate.table_name == table_name
                    })
                })
                .map_err(Self::backend_error)
        })
    }

    fn get_table_metadata_by_id(&self, table_id: i64) -> Result<TableMetadata, CatalogError> {
        let db = self.db.clone();
        let default_catalog = self.default_catalog.clone();
        self.block_on(async move {
            let repo = Repository::new(db);
            let table = repo
                .get_table(GetTableInfo { table_id })
                .await
                .map_err(Self::backend_error)?;
            let schemas = repo.get_all_schemas().await.map_err(Self::backend_error)?;
            let schema = schemas
                .into_iter()
                .find(|schema| schema.schema_id == table.schema_id)
                .ok_or_else(|| CatalogError::Backend {
                    message: format!(
                        "schema_id {} not found for table {}",
                        table.schema_id, table_id
                    ),
                })?;
            let statistics = repo
                .get_table_stats(crate::stats::GetTableStatsInfo { table_id })
                .await
                .map_err(Self::backend_error)?
                .map(|stats| stats.stats);

            Ok(TableMetadata {
                id: DataSourceId(table.table_id),
                table: ResolvedTableRef {
                    catalog: default_catalog.into(),
                    schema: schema.schema_name.into(),
                    table: table.table_name.clone().into(),
                },
                schema: Arc::new(table.arrow_schema()),
                statistics,
                definition: table.definition,
            })
        })
    }
}

impl Catalog for RepositoryCatalog {
    fn kind(&self) -> &str {
        "repository"
    }

    fn create_table(
        &self,
        table: TableRef,
        schema: SchemaRef,
        definition: Option<String>,
    ) -> Result<DataSourceId, CatalogError> {
        let resolved = self.resolved_table(table.clone())?;
        if let Some(existing) = self.find_table_by_resolved_ref(&resolved)? {
            return Err(CatalogError::TableAlreadyExists {
                table: resolved,
                existing_id: DataSourceId(existing.table_id),
            });
        }

        self.ensure_schema(&resolved.schema)?;
        let db = self.db.clone();
        let schema_name = resolved.schema.to_string();
        let table_name = resolved.table.to_string();
        self.block_on(async move {
            let mut repo = Repository::new(db);
            repo.create_table(CreateTableInfo {
                table_name: TableRef::partial(schema_name, table_name),
                columns: schema
                    .fields()
                    .iter()
                    .map(|field| CreateColumnInfo {
                        column_name: field.name().clone(),
                        column_type: ColumnType(field.data_type().clone()),
                        nulls_allowed: field.is_nullable(),
                        initial_default: None,
                        default_value: None,
                    })
                    .collect(),
                definition,
            })
            .await
            .map(DataSourceId)
            .map_err(Self::backend_error)
        })
    }

    fn table(&self, table_id: DataSourceId) -> Result<TableMetadata, CatalogError> {
        match self.get_table_metadata_by_id(table_id.0) {
            Ok(metadata) => Ok(metadata),
            Err(CatalogError::Backend { .. }) => Err(CatalogError::DataSourceNotFound {
                data_source_id: table_id,
            }),
            Err(err) => Err(err),
        }
    }

    fn table_by_ref(&self, table: &TableRef) -> Result<TableMetadata, CatalogError> {
        let resolved = self.resolved_table(table.clone())?;
        let Some(table_info) = self.find_table_by_resolved_ref(&resolved)? else {
            return Err(CatalogError::TableNotFound {
                table: table.clone(),
            });
        };
        self.get_table_metadata_by_id(table_info.table_id)
    }

    fn drop_table(&self, table: TableRef) -> Result<(), CatalogError> {
        let resolved = self.resolved_table(table.clone())?;
        let Some(table_info) = self.find_table_by_resolved_ref(&resolved)? else {
            return Err(CatalogError::TableNotFound { table });
        };

        let db = self.db.clone();
        let table_id = table_info.table_id;
        self.block_on(async move {
            let mut repo = Repository::new(db);
            repo.drop_table(crate::table::DropTableInfo { table_id })
                .await
                .map(|_| ())
                .map_err(Self::backend_error)
        })
    }

    fn set_table_statistics(
        &self,
        table: TableRef,
        stats: TableStatistics,
    ) -> Result<(), CatalogError> {
        let resolved = self.resolved_table(table.clone())?;
        let Some(table_info) = self.find_table_by_resolved_ref(&resolved)? else {
            return Err(CatalogError::TableNotFound { table });
        };

        let db = self.db.clone();
        let table_id = table_info.table_id;
        self.block_on(async move {
            let mut repo = Repository::new(db);
            repo.update_table_stats(UpdateTableStatsInfo { table_id, stats })
                .await
                .map_err(Self::backend_error)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use optd_core::ir::{
        catalog::Catalog,
        statistics::{ColumnStatistics, TableStatistics},
        table_ref::TableRef,
    };
    use optd_repository_migration::{Migrator, MigratorTrait};
    use sea_orm::{Database, DbErr};

    use super::RepositoryCatalog;

    #[test]
    fn repository_catalog_round_trips_tables_and_stats() -> Result<(), DbErr> {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create runtime");
        let db_path = std::env::temp_dir().join(format!(
            "optd-repository-catalog-{}.db",
            uuid::Uuid::new_v4().simple()
        ));
        let db_url = format!("sqlite://{}?mode=rwc", db_path.display());
        let db = runtime.block_on(async {
            let db = Database::connect(&db_url).await?;
            Migrator::up(&db, None).await?;
            Ok::<_, DbErr>(db)
        })?;
        let catalog = RepositoryCatalog::new(db, "datafusion", "public");

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("note", arrow_schema::DataType::Utf8, true),
        ]));

        let table_ref = TableRef::partial("smoke", "t1");
        let table_id = catalog
            .create_table(table_ref.clone(), schema.clone(), None)
            .unwrap();
        let metadata = catalog.table_by_ref(&table_ref).unwrap();
        assert_eq!(metadata.id, table_id);
        assert_eq!(metadata.schema, schema);
        assert!(metadata.statistics.is_none());

        let stats = TableStatistics {
            row_count: 10,
            size_bytes: Some(128),
            column_statistics: HashMap::from([
                (
                    0,
                    ColumnStatistics {
                        min_value: Some("1".to_owned()),
                        max_value: Some("10".to_owned()),
                        null_count: Some(0),
                        distinct_count: Some(10),
                        advanced_stats: vec![],
                    },
                ),
                (
                    1,
                    ColumnStatistics {
                        min_value: Some("\"a\"".to_owned()),
                        max_value: Some("\"z\"".to_owned()),
                        null_count: Some(1),
                        distinct_count: Some(5),
                        advanced_stats: vec![],
                    },
                ),
            ]),
        };

        catalog
            .set_table_statistics(table_ref.clone(), stats.clone())
            .unwrap();
        let metadata = catalog.table(table_id).unwrap();
        assert_eq!(metadata.statistics, Some(stats));

        catalog.drop_table(table_ref.clone()).unwrap();
        assert!(matches!(
            catalog.table_by_ref(&table_ref),
            Err(optd_core::ir::catalog::CatalogError::TableNotFound { .. })
        ));

        Ok(())
    }
}
