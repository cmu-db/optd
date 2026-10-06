//! DataFusion catalog providers that mirror DDL into `optd-catalog`.

use std::{
    any::Any,
    fmt,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use datafusion::{
    datasource::listing::ListingTable,
    error::{DataFusionError, Result as DFResult},
    logical_expr::TableType,
    prelude::SessionContext,
};
use datafusion_catalog::{CatalogProvider, SchemaProvider, TableProvider};
use optd_catalog::{
    Catalog, CatalogError, CatalogResult,
    entity::column::ColumnType,
    migration::{Migrator, MigratorTrait},
    relation::{
        BaseTableStorage, CreateBaseTableInfo, CreateColumnInfo, DropRelationInfo, RelationInfo,
        RelationKind, StorageKind,
    },
    schema::{CreateSchemaInfo, DropSchemaInfo},
};
use sea_orm::DatabaseConnection;

use crate::statistics_udfs::{CARDEST_UDF, COLLECT_STATS_UDF, register_statistics_udfs};

/// Installs a write-through catalog adapter around a DataFusion catalog.
pub async fn install_persistent_catalog(
    session: &SessionContext,
    catalog_name: &str,
    db: DatabaseConnection,
) -> DFResult<()> {
    Migrator::up(&db, None).await.map_err(external_error)?;
    let inner = session.catalog(catalog_name).ok_or_else(|| {
        DataFusionError::Execution(format!("DataFusion catalog '{catalog_name}' not found"))
    })?;
    if let Some(persistent) = inner.as_any().downcast_ref::<PersistentCatalogProvider>() {
        register_statistics_udfs(session, persistent.db.clone());
        return Ok(());
    }
    register_statistics_udfs(session, db.clone());
    session.register_catalog(
        catalog_name,
        Arc::new(PersistentCatalogProvider::new(inner, db)),
    );
    Ok(())
}

/// Enables idempotent registration of matching durable objects until the guard is dropped.
///
/// This mode is intended for startup import of known providers such as the JOB and TPC-H
/// Parquet tables. Ordinary SQL DDL should not hold this guard: a duplicate `CREATE` must remain
/// a name conflict even when its schema matches an existing durable object.
pub fn idempotent_import_guard(
    session: &SessionContext,
    catalog_name: &str,
) -> DFResult<IdempotentImportGuard> {
    let current = session.catalog(catalog_name).ok_or_else(|| {
        DataFusionError::Execution(format!("DataFusion catalog '{catalog_name}' not found"))
    })?;
    let persistent = current
        .as_any()
        .downcast_ref::<PersistentCatalogProvider>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "DataFusion catalog '{catalog_name}' is not persistent"
            ))
        })?;
    persistent.import_depth.fetch_add(1, Ordering::AcqRel);
    Ok(IdempotentImportGuard {
        import_depth: Arc::clone(&persistent.import_depth),
    })
}

/// Scope guard for idempotent startup imports.
pub struct IdempotentImportGuard {
    import_depth: Arc<AtomicUsize>,
}

impl Drop for IdempotentImportGuard {
    fn drop(&mut self) {
        let previous = self.import_depth.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "import guard depth must not underflow");
    }
}

/// Disables write-through persistence and restores the wrapped DataFusion catalog.
pub fn uninstall_persistent_catalog(
    session: &SessionContext,
    catalog_name: &str,
) -> DFResult<bool> {
    let current = session.catalog(catalog_name).ok_or_else(|| {
        DataFusionError::Execution(format!("DataFusion catalog '{catalog_name}' not found"))
    })?;
    let Some(persistent) = current.as_any().downcast_ref::<PersistentCatalogProvider>() else {
        return Ok(false);
    };
    session.register_catalog(catalog_name, Arc::clone(&persistent.inner));
    session.deregister_udf(CARDEST_UDF);
    session.deregister_udf(COLLECT_STATS_UDF);
    Ok(true)
}

/// DataFusion catalog provider that persists schema DDL.
pub struct PersistentCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    db: DatabaseConnection,
    import_depth: Arc<AtomicUsize>,
}

impl PersistentCatalogProvider {
    /// Creates a provider that delegates runtime behavior to `inner` and persists DDL through `db`.
    pub fn new(inner: Arc<dyn CatalogProvider>, db: DatabaseConnection) -> Self {
        Self {
            inner,
            db,
            import_depth: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl fmt::Debug for PersistentCatalogProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistentCatalogProvider")
            .finish_non_exhaustive()
    }
}

impl CatalogProvider for PersistentCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.inner.schema(name).map(|inner| {
            Arc::new(PersistentSchemaProvider::with_import_depth(
                name.to_owned(),
                inner,
                self.db.clone(),
                Arc::clone(&self.import_depth),
            )) as Arc<dyn SchemaProvider>
        })
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        let previous = self.inner.register_schema(name, Arc::clone(&schema))?;
        let allow_existing = self.import_depth.load(Ordering::Acquire) > 0;
        if let Err(error) = persist_create_schema(self.db.clone(), name.to_owned(), allow_existing)
        {
            let _ = self.inner.deregister_schema(name, true);
            if let Some(previous) = previous.as_ref() {
                let _ = self.inner.register_schema(name, Arc::clone(previous));
            }
            return Err(error);
        }
        Ok(previous)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        let removed = self.inner.deregister_schema(name, cascade)?;
        if removed.is_some()
            && let Err(error) = persist_drop_schema(self.db.clone(), name.to_owned())
        {
            if let Some(schema) = removed.as_ref() {
                let _ = self.inner.register_schema(name, Arc::clone(schema));
            }
            return Err(error);
        }
        Ok(removed)
    }
}

/// DataFusion schema provider that persists table DDL.
pub struct PersistentSchemaProvider {
    schema_name: String,
    inner: Arc<dyn SchemaProvider>,
    db: DatabaseConnection,
    import_depth: Arc<AtomicUsize>,
}

impl PersistentSchemaProvider {
    /// Creates a schema wrapper for `schema_name` around an existing DataFusion provider.
    pub fn new(
        schema_name: String,
        inner: Arc<dyn SchemaProvider>,
        db: DatabaseConnection,
    ) -> Self {
        Self::with_import_depth(schema_name, inner, db, Arc::new(AtomicUsize::new(0)))
    }

    fn with_import_depth(
        schema_name: String,
        inner: Arc<dyn SchemaProvider>,
        db: DatabaseConnection,
        import_depth: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            schema_name,
            inner,
            db,
            import_depth,
        }
    }
}

impl fmt::Debug for PersistentSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistentSchemaProvider")
            .field("schema_name", &self.schema_name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for PersistentSchemaProvider {
    fn owner_name(&self) -> Option<&str> {
        self.inner.owner_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.table(name).await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if table.table_type() != TableType::Base {
            return Err(DataFusionError::NotImplemented(
                "persistent view DDL is not implemented yet".to_owned(),
            ));
        }

        let previous = self
            .inner
            .register_table(name.clone(), Arc::clone(&table))?;
        let storage = if table.as_any().is::<ListingTable>() {
            BaseTableStorage::Files
        } else {
            BaseTableStorage::Relational
        };
        let info = CreateBaseTableInfo {
            schema_name: self.schema_name.clone(),
            relation_name: name.clone(),
            storage,
            columns: table
                .schema()
                .fields()
                .iter()
                .map(|field| CreateColumnInfo {
                    column_name: field.name().clone(),
                    column_type: ColumnType(field.data_type().clone()),
                    nulls_allowed: field.is_nullable(),
                })
                .collect(),
        };

        let allow_existing = self.import_depth.load(Ordering::Acquire) > 0;
        if let Err(error) = persist_create_table(self.db.clone(), info, allow_existing) {
            let _ = self.inner.deregister_table(&name);
            if let Some(previous) = previous.as_ref() {
                let _ = self
                    .inner
                    .register_table(name.clone(), Arc::clone(previous));
            }
            return Err(error);
        }

        Ok(previous)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let removed = self.inner.deregister_table(name)?;
        if let Some(table) = removed.as_ref() {
            if table.table_type() != TableType::Base {
                let _ = self
                    .inner
                    .register_table(name.to_owned(), Arc::clone(table));
                return Err(DataFusionError::NotImplemented(
                    "persistent view DDL is not implemented yet".to_owned(),
                ));
            }
            if let Err(error) =
                persist_drop_table(self.db.clone(), self.schema_name.clone(), name.to_owned())
            {
                let _ = self
                    .inner
                    .register_table(name.to_owned(), Arc::clone(table));
                return Err(error);
            }
        }
        Ok(removed)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

fn persist_create_schema(
    db: DatabaseConnection,
    schema_name: String,
    allow_existing: bool,
) -> DFResult<()> {
    run_catalog_future(async move {
        let catalog = Catalog::new(db);
        let reader = catalog.reader().await?;
        if reader
            .get_all_schemas()
            .await?
            .iter()
            .any(|schema| schema.schema_name == schema_name)
        {
            return if allow_existing {
                Ok(())
            } else {
                Err(CatalogError::SchemaAlreadyExists { schema_name })
            };
        }
        catalog
            .create_schema(reader.snapshot_id(), CreateSchemaInfo { schema_name })
            .await?;
        Ok(())
    })
}

fn persist_drop_schema(db: DatabaseConnection, schema_name: String) -> DFResult<()> {
    run_catalog_future(async move {
        let catalog = Catalog::new(db);
        let reader = catalog.reader().await?;
        let schema = reader
            .get_all_schemas()
            .await?
            .into_iter()
            .find(|schema| schema.schema_name == schema_name)
            .ok_or_else(|| CatalogError::SchemaNameNotFound {
                schema_name: schema_name.clone(),
            })?;
        catalog
            .drop_schema(
                reader.snapshot_id(),
                DropSchemaInfo {
                    schema_id: schema.schema_id,
                },
            )
            .await
    })
}

fn persist_create_table(
    db: DatabaseConnection,
    info: CreateBaseTableInfo,
    allow_existing: bool,
) -> DFResult<()> {
    run_catalog_future(async move {
        let catalog = Catalog::new(db);
        let mut reader = catalog.reader().await?;
        if !reader
            .get_all_schemas()
            .await?
            .iter()
            .any(|schema| schema.schema_name == info.schema_name)
        {
            catalog
                .create_schema(
                    reader.snapshot_id(),
                    CreateSchemaInfo {
                        schema_name: info.schema_name.clone(),
                    },
                )
                .await?;
            reader = catalog.reader().await?;
        }
        let schemas = reader.get_all_schemas().await?;
        let schema = schemas
            .iter()
            .find(|schema| schema.schema_name == info.schema_name)
            .expect("the schema was created or observed above");
        if let Some(existing) = reader
            .get_all_relations()
            .await?
            .into_iter()
            .find(|relation| {
                relation.schema_id == schema.schema_id
                    && relation.relation_name == info.relation_name
            })
        {
            if allow_existing && base_table_matches(&existing, &info) {
                return Ok(());
            }
            return Err(CatalogError::RelationAlreadyExists {
                schema_name: info.schema_name,
                relation_name: info.relation_name,
            });
        }

        catalog
            .create_base_table(reader.snapshot_id(), info)
            .await?;
        Ok(())
    })
}

fn base_table_matches(existing: &RelationInfo, requested: &CreateBaseTableInfo) -> bool {
    existing.relation_kind == RelationKind::Table
        && existing.storage_kind == StorageKind::from(requested.storage)
        && existing.columns.len() == requested.columns.len()
        && existing
            .columns
            .iter()
            .zip(&requested.columns)
            .all(|(existing, requested)| {
                existing.column_name == requested.column_name
                    && existing.column_type == requested.column_type
                    && existing.nulls_allowed == requested.nulls_allowed
            })
}

fn persist_drop_table(
    db: DatabaseConnection,
    schema_name: String,
    relation_name: String,
) -> DFResult<()> {
    run_catalog_future(async move {
        let catalog = Catalog::new(db);
        let reader = catalog.reader().await?;
        let schemas = reader.get_all_schemas().await?;
        let schema = schemas
            .iter()
            .find(|schema| schema.schema_name == schema_name)
            .ok_or_else(|| CatalogError::SchemaNameNotFound {
                schema_name: schema_name.clone(),
            })?;
        let relation = reader
            .get_all_relations()
            .await?
            .into_iter()
            .find(|relation| {
                relation.schema_id == schema.schema_id
                    && relation.relation_name == relation_name
                    && relation.relation_kind == RelationKind::Table
            })
            .ok_or_else(|| CatalogError::RelationNameNotFound {
                schema_name: schema_name.clone(),
                relation_name: relation_name.clone(),
            })?;
        catalog
            .drop_relation(
                reader.snapshot_id(),
                DropRelationInfo {
                    relation_id: relation.relation_id,
                },
            )
            .await?;
        Ok(())
    })
}

fn run_catalog_future<F, T>(future: F) -> DFResult<T>
where
    F: Future<Output = CatalogResult<T>> + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .expect("catalog adapter runtime should be created")
            .block_on(future)
    })
    .join()
    .map_err(|_| DataFusionError::Execution("catalog adapter thread panicked".to_owned()))?
    .map_err(external_error)
}

fn external_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use optd_catalog::{Catalog, relation::StorageKind};
    use sea_orm::Database;

    use super::{
        idempotent_import_guard, install_persistent_catalog, uninstall_persistent_catalog,
    };
    use crate::setup::session_context_with_information_schema;

    #[tokio::test(flavor = "multi_thread")]
    async fn datafusion_ddl_updates_persistent_catalog() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("catalog.sqlite");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let db = Database::connect(&url).await.unwrap();
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();

        session
            .sql("CREATE SCHEMA analytics")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session
            .sql("CREATE TABLE analytics.orders AS SELECT 1 AS order_id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let csv_path = temp_dir.path().join("external_orders.csv");
        std::fs::write(&csv_path, "order_id\n1\n").unwrap();
        session
            .sql(&format!(
                "CREATE EXTERNAL TABLE analytics.external_orders(order_id BIGINT) \
                 STORED AS CSV LOCATION '{}'",
                csv_path.display()
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let catalog = Catalog::new(db.clone());
        let created = catalog.reader().await.unwrap();
        let created_snapshot_id = created.snapshot_id();
        let schemas = created.get_all_schemas().await.unwrap();
        let schema = schemas
            .iter()
            .find(|schema| schema.schema_name == "analytics")
            .unwrap();
        let relations = created.get_all_relations().await.unwrap();
        let relation = relations
            .iter()
            .find(|relation| relation.relation_name == "orders")
            .unwrap();
        assert_eq!(relation.schema_id, schema.schema_id);
        assert_eq!(relation.storage_kind, StorageKind::Relational);
        assert_eq!(relation.columns.len(), 1);
        assert_eq!(relation.columns[0].column_name, "order_id");
        let external_relation = relations
            .iter()
            .find(|relation| relation.relation_name == "external_orders")
            .unwrap();
        assert_eq!(external_relation.storage_kind, StorageKind::Files);

        session
            .sql("DROP TABLE analytics.orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session
            .sql("DROP TABLE analytics.external_orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_all_relations()
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            catalog
                .reader_at(created_snapshot_id)
                .await
                .unwrap()
                .get_all_relations()
                .await
                .unwrap()
                .len(),
            2
        );

        drop(catalog);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persistence_can_be_disabled_and_reenabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("catalog.sqlite");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let db = Database::connect(&url).await.unwrap();
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        session
            .sql("CREATE SCHEMA analytics")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert!(uninstall_persistent_catalog(&session, "datafusion").unwrap());
        assert!(!uninstall_persistent_catalog(&session, "datafusion").unwrap());
        session
            .sql("CREATE TABLE analytics.untracked AS SELECT 1 AS value")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        session
            .sql("CREATE TABLE analytics.tracked AS SELECT 2 AS value")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let catalog = Catalog::new(db.clone());
        let relation_names = catalog
            .reader()
            .await
            .unwrap()
            .get_all_relations()
            .await
            .unwrap()
            .into_iter()
            .map(|relation| relation.relation_name)
            .collect::<Vec<_>>();
        assert_eq!(relation_names, vec!["tracked"]);
        assert!(session.table("analytics.untracked").await.is_ok());
        assert!(session.table("analytics.tracked").await.is_ok());

        drop(catalog);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn statistics_udfs_persist_stats_and_advance_the_snapshot() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("catalog.sqlite");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let db = Database::connect(&url).await.unwrap();
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        session
            .sql(
                "CREATE TABLE numbers AS \
                 SELECT * FROM (VALUES (1), (2), (2)) AS source(value)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let catalog = Catalog::new(db.clone());
        let before_statistics = catalog.reader().await.unwrap().snapshot_id();
        session
            .sql("SELECT collect_stats(CAST(value AS VARCHAR)) FROM numbers")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("collect_stats must reject column-driven invocation");
        assert_eq!(
            catalog.reader().await.unwrap().snapshot_id(),
            before_statistics
        );
        let before_batches = session
            .sql("SELECT cardest('SELECT * FROM numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let before_estimate = before_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(before_estimate, 1000.0);

        let collect_batches = session
            .sql("SELECT collect_stats('numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let statistics_snapshot = collect_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert!(statistics_snapshot > before_statistics.as_i64());

        let relation_id = catalog
            .reader()
            .await
            .unwrap()
            .get_all_relations()
            .await
            .unwrap()[0]
            .relation_id;
        assert!(
            catalog
                .reader_at(before_statistics)
                .await
                .unwrap()
                .get_relation_statistics(relation_id)
                .await
                .unwrap()
                .is_none()
        );
        let persisted = catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(relation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.row_count, Some(3));
        assert_eq!(persisted.column_statistics[0].distinct_count, Some(2));

        let snapshot_before_restart_import = catalog.reader().await.unwrap().snapshot_id();
        let ordinary_session = session_context_with_information_schema();
        install_persistent_catalog(&ordinary_session, "datafusion", db.clone())
            .await
            .unwrap();
        ordinary_session
            .sql(
                "CREATE TABLE numbers AS \
                 SELECT * FROM (VALUES (1), (2), (2)) AS source(value)",
            )
            .await
            .expect_err("ordinary CREATE must conflict with durable metadata");
        assert!(ordinary_session.table("numbers").await.is_err());

        let restarted_session = session_context_with_information_schema();
        install_persistent_catalog(&restarted_session, "datafusion", db.clone())
            .await
            .unwrap();
        let import_guard = idempotent_import_guard(&restarted_session, "datafusion").unwrap();
        restarted_session
            .sql(
                "CREATE TABLE numbers AS \
                 SELECT * FROM (VALUES (1), (2), (2)) AS source(value)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        drop(import_guard);
        assert_eq!(
            catalog.reader().await.unwrap().snapshot_id(),
            snapshot_before_restart_import
        );
        let after_batches = restarted_session
            .sql("SELECT cardest('SELECT * FROM numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let after_estimate = after_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(after_estimate, 3.0);

        drop(catalog);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_persistence_restores_deregistered_table_and_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("catalog.sqlite");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let db = Database::connect(&url).await.unwrap();
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        session
            .sql("CREATE SCHEMA analytics")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session
            .sql("CREATE TABLE analytics.orders AS SELECT 1 AS order_id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session
            .sql("CREATE SCHEMA empty_schema")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let catalog = Catalog::new(db.clone());
        let reader = catalog.reader().await.unwrap();
        let relation_id = reader.get_all_relations().await.unwrap()[0].relation_id;
        catalog
            .drop_relation(
                reader.snapshot_id(),
                optd_catalog::relation::DropRelationInfo { relation_id },
            )
            .await
            .unwrap();

        session
            .sql("DROP TABLE analytics.orders")
            .await
            .expect_err("missing durable relation should reject the runtime drop");
        assert!(session.table("analytics.orders").await.is_ok());

        let reader = catalog.reader().await.unwrap();
        let schema_id = reader
            .get_all_schemas()
            .await
            .unwrap()
            .into_iter()
            .find(|schema| schema.schema_name == "empty_schema")
            .unwrap()
            .schema_id;
        catalog
            .drop_schema(
                reader.snapshot_id(),
                optd_catalog::schema::DropSchemaInfo { schema_id },
            )
            .await
            .unwrap();

        session
            .sql("DROP SCHEMA empty_schema")
            .await
            .expect_err("missing durable schema should reject the runtime drop");
        assert!(
            session
                .catalog("datafusion")
                .unwrap()
                .schema("empty_schema")
                .is_some()
        );

        drop(catalog);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_persistence_rolls_back_datafusion_registration() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("catalog.sqlite");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let db = Database::connect(&url).await.unwrap();
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();

        let catalog = Catalog::new(db.clone());
        let initial_snapshot = catalog.reader().await.unwrap().snapshot_id();
        catalog
            .create_schema(
                initial_snapshot,
                optd_catalog::schema::CreateSchemaInfo {
                    schema_name: "durable_only".to_owned(),
                },
            )
            .await
            .unwrap();
        session
            .sql("CREATE SCHEMA durable_only")
            .await
            .expect_err("persistent schema conflict should roll back runtime registration");
        assert!(
            session
                .catalog("datafusion")
                .unwrap()
                .schema("durable_only")
                .is_none()
        );
        session
            .sql("CREATE SCHEMA analytics")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let reader = catalog.reader().await.unwrap();
        catalog
            .create_base_table(
                reader.snapshot_id(),
                optd_catalog::relation::CreateBaseTableInfo {
                    schema_name: "analytics".to_owned(),
                    relation_name: "conflict".to_owned(),
                    storage: optd_catalog::relation::BaseTableStorage::Relational,
                    columns: Vec::new(),
                },
            )
            .await
            .unwrap();

        session
            .sql("CREATE TABLE analytics.conflict AS SELECT 1 AS value")
            .await
            .expect_err("persistent name conflict should reject DataFusion registration");
        assert!(session.table("analytics.conflict").await.is_err());
        assert_eq!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_all_relations()
                .await
                .unwrap()
                .len(),
            1
        );

        drop(catalog);
        db.close().await.unwrap();
    }
}
