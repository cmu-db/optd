//! DataFusion catalog providers that mirror DDL into `optd-catalog`.

use std::{
    any::Any,
    fmt,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
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

use crate::statistics_udfs::{register_statistics_udfs, unregister_statistics_udfs};

use crate::observed_execution::{DdlChange, DdlOperation, DdlOutcome, record_ddl_change};

static NEXT_BINDING_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) struct PersistentBinding {
    pub db: DatabaseConnection,
    pub catalog_id: i64,
    pub binding_id: u64,
}

pub(crate) fn persistent_binding(
    session: &SessionContext,
    name: &str,
) -> Option<PersistentBinding> {
    let provider = session.catalog(name)?;
    let persistent = provider
        .as_any()
        .downcast_ref::<PersistentCatalogProvider>()?;
    Some(PersistentBinding {
        db: persistent.db.clone(),
        catalog_id: persistent.catalog_id,
        binding_id: persistent.binding_id,
    })
}

fn record_ddl(
    catalog_id: i64,
    operation: DdlOperation,
    schema: Option<&str>,
    name: &str,
    outcome: DdlOutcome,
) {
    record_ddl_change(DdlChange {
        catalog_id,
        operation,
        outcome,
        schema_name: schema.map(str::to_owned),
        object_name: name.to_owned(),
    });
}

/// Installs a write-through catalog adapter around a DataFusion catalog.
///
/// Source table providers retain their native read/write capabilities. Use the shared
/// observed-execution API to invalidate statistics after DML and collect DDL receipts.
/// Raw session/provider writes require explicit statistics invalidation.
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
        register_statistics_udfs(session, catalog_name, persistent.db.clone());
        return Ok(());
    }
    let catalog = Catalog::new(db.clone());
    let reader = catalog.reader().await.map_err(external_error)?;
    let catalog_id = match reader
        .get_catalogs()
        .await
        .map_err(external_error)?
        .iter()
        .find(|c| c.catalog_name == catalog_name)
    {
        Some(registration) => registration.catalog_id,
        None => catalog
            .register_catalog(
                reader.snapshot_id(),
                optd_catalog::source::RegisterCatalogInfo {
                    catalog_name: catalog_name.to_owned(),
                    catalog_kind: "datafusion".to_owned(),
                    external_catalog_name: Some(catalog_name.to_owned()),
                    adapter_name: "datafusion".to_owned(),
                    connection_profile_reference: format!("runtime://{catalog_name}"),
                },
            )
            .await
            .map_err(external_error)?,
    };
    register_statistics_udfs(session, catalog_name, db.clone());
    session.register_catalog(
        catalog_name,
        Arc::new(PersistentCatalogProvider::new(inner, db, catalog_id)),
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
    unregister_statistics_udfs(session, catalog_name);
    Ok(true)
}

/// DataFusion catalog provider that persists schema DDL.
pub struct PersistentCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    db: DatabaseConnection,
    catalog_id: i64,
    binding_id: u64,
    import_depth: Arc<AtomicUsize>,
}

impl PersistentCatalogProvider {
    /// Creates a provider that delegates runtime behavior to `inner` and persists DDL through `db`.
    pub fn new(inner: Arc<dyn CatalogProvider>, db: DatabaseConnection, catalog_id: i64) -> Self {
        Self {
            inner,
            db,
            catalog_id,
            binding_id: NEXT_BINDING_ID.fetch_add(1, Ordering::Relaxed),
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
                self.catalog_id,
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
        if let Err(error) = persist_create_schema(
            self.db.clone(),
            self.catalog_id,
            name.to_owned(),
            allow_existing,
        ) {
            let mut compensation_errors = vec![];
            if let Err(e) = self.inner.deregister_schema(name, true) {
                compensation_errors.push(e.to_string());
            }
            if let Some(previous) = previous.as_ref()
                && let Err(e) = self.inner.register_schema(name, Arc::clone(previous))
            {
                compensation_errors.push(e.to_string());
            }
            record_ddl(
                self.catalog_id,
                DdlOperation::CreateSchema,
                None,
                name,
                DdlOutcome::Failed {
                    error: error.to_string(),
                    compensation_errors,
                },
            );
            return Err(error);
        }
        record_ddl(
            self.catalog_id,
            DdlOperation::CreateSchema,
            None,
            name,
            DdlOutcome::Confirmed,
        );
        Ok(previous)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        // Durable schema deletion is restrictive. Reject this known unsupported
        // operation before removing a populated runtime schema and compensating.
        if cascade
            && self
                .inner
                .schema(name)
                .is_some_and(|schema| !schema.table_names().is_empty())
        {
            return Err(DataFusionError::NotImplemented(
                "DROP SCHEMA CASCADE for non-empty persistent schemas is not supported".to_owned(),
            ));
        }
        let removed = self.inner.deregister_schema(name, cascade)?;
        if removed.is_some()
            && let Err(error) =
                persist_drop_schema(self.db.clone(), self.catalog_id, name.to_owned())
        {
            let mut compensation_errors = vec![];
            if let Some(schema) = removed.as_ref()
                && let Err(e) = self.inner.register_schema(name, Arc::clone(schema))
            {
                compensation_errors.push(e.to_string());
            }
            record_ddl(
                self.catalog_id,
                DdlOperation::DropSchema,
                None,
                name,
                DdlOutcome::Failed {
                    error: error.to_string(),
                    compensation_errors,
                },
            );
            return Err(error);
        }
        record_ddl(
            self.catalog_id,
            DdlOperation::DropSchema,
            None,
            name,
            if removed.is_some() {
                DdlOutcome::Confirmed
            } else {
                DdlOutcome::Noop
            },
        );
        Ok(removed)
    }
}

/// DataFusion schema provider that persists table DDL.
pub struct PersistentSchemaProvider {
    catalog_id: i64,
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
        catalog_id: i64,
    ) -> Self {
        Self::with_import_depth(
            schema_name,
            inner,
            db,
            catalog_id,
            Arc::new(AtomicUsize::new(0)),
        )
    }

    fn with_import_depth(
        schema_name: String,
        inner: Arc<dyn SchemaProvider>,
        db: DatabaseConnection,
        catalog_id: i64,
        import_depth: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            catalog_id,
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
        if let Err(error) =
            persist_create_table(self.db.clone(), self.catalog_id, info, allow_existing)
        {
            let mut compensation_errors = vec![];
            if let Err(e) = self.inner.deregister_table(&name) {
                compensation_errors.push(e.to_string());
            }
            if let Some(previous) = previous.as_ref()
                && let Err(e) = self
                    .inner
                    .register_table(name.clone(), Arc::clone(previous))
            {
                compensation_errors.push(e.to_string());
            }
            record_ddl(
                self.catalog_id,
                DdlOperation::CreateTable,
                Some(&self.schema_name),
                &name,
                DdlOutcome::Failed {
                    error: error.to_string(),
                    compensation_errors,
                },
            );
            return Err(error);
        }
        record_ddl(
            self.catalog_id,
            DdlOperation::CreateTable,
            Some(&self.schema_name),
            &name,
            DdlOutcome::Confirmed,
        );
        Ok(previous)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let removed = self.inner.deregister_table(name)?;
        if let Some(table) = removed.as_ref() {
            if table.table_type() != TableType::Base {
                let error = DataFusionError::NotImplemented(
                    "persistent view DDL is not implemented yet".to_owned(),
                );
                let compensation_errors = self
                    .inner
                    .register_table(name.to_owned(), Arc::clone(table))
                    .err()
                    .map(|e| e.to_string())
                    .into_iter()
                    .collect();
                record_ddl(
                    self.catalog_id,
                    DdlOperation::DropTable,
                    Some(&self.schema_name),
                    name,
                    DdlOutcome::Failed {
                        error: error.to_string(),
                        compensation_errors,
                    },
                );
                return Err(error);
            }
            if let Err(error) = persist_drop_table(
                self.db.clone(),
                self.catalog_id,
                self.schema_name.clone(),
                name.to_owned(),
            ) {
                let compensation_errors = self
                    .inner
                    .register_table(name.to_owned(), Arc::clone(table))
                    .err()
                    .map(|e| e.to_string())
                    .into_iter()
                    .collect();
                record_ddl(
                    self.catalog_id,
                    DdlOperation::DropTable,
                    Some(&self.schema_name),
                    name,
                    DdlOutcome::Failed {
                        error: error.to_string(),
                        compensation_errors,
                    },
                );
                return Err(error);
            }
        }
        record_ddl(
            self.catalog_id,
            DdlOperation::DropTable,
            Some(&self.schema_name),
            name,
            if removed.is_some() {
                DdlOutcome::Confirmed
            } else {
                DdlOutcome::Noop
            },
        );
        Ok(removed)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

/// Resolves one installed source namespace without confusing same-named schemas.
pub(crate) async fn catalog_for_name(
    db: DatabaseConnection,
    name: &str,
) -> DFResult<Catalog<DatabaseConnection>> {
    let catalog = Catalog::new(db.clone());
    let reader = catalog.reader().await.map_err(external_error)?;
    let id = reader
        .get_catalogs()
        .await
        .map_err(external_error)?
        .iter()
        .find(|c| c.catalog_name == name)
        .map(|c| c.catalog_id)
        .ok_or_else(|| {
            DataFusionError::Execution(format!("persistent catalog '{name}' is not registered"))
        })?;
    Ok(Catalog::on_catalog(db, id))
}

fn persist_create_schema(
    db: DatabaseConnection,
    catalog_id: i64,
    schema_name: String,
    allow_existing: bool,
) -> DFResult<()> {
    run_catalog_future(async move {
        let catalog = Catalog::on_catalog(db, catalog_id);
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

fn persist_drop_schema(
    db: DatabaseConnection,
    catalog_id: i64,
    schema_name: String,
) -> DFResult<()> {
    run_catalog_future(async move {
        let catalog = Catalog::on_catalog(db, catalog_id);
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
    catalog_id: i64,
    info: CreateBaseTableInfo,
    allow_existing: bool,
) -> DFResult<()> {
    run_catalog_future(async move {
        let catalog = Catalog::on_catalog(db, catalog_id);
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
    catalog_id: i64,
    schema_name: String,
    relation_name: String,
) -> DFResult<()> {
    run_catalog_future(async move {
        let catalog = Catalog::on_catalog(db, catalog_id);
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

    use super::{
        idempotent_import_guard, install_persistent_catalog, uninstall_persistent_catalog,
    };
    use crate::setup::session_context_with_information_schema;
    use crate::test_utils::test_sqlite_connection;

    #[tokio::test(flavor = "multi_thread")]
    async fn datafusion_ddl_updates_persistent_catalog() {
        let (temp_dir, db) = test_sqlite_connection().await;
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
        let (_temp_dir, db) = test_sqlite_connection().await;
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
    async fn persistent_catalogs_share_database_without_aliasing_objects() {
        let (_temp_dir, db) = test_sqlite_connection().await;
        let session = session_context_with_information_schema();
        for sql in ["CREATE DATABASE other", "CREATE SCHEMA other.public"] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        for name in ["datafusion", "other"] {
            install_persistent_catalog(&session, name, db.clone())
                .await
                .unwrap();
        }
        for sql in [
            "CREATE TABLE datafusion.public.numbers AS SELECT 1 AS x",
            "CREATE TABLE other.public.numbers AS SELECT * FROM (VALUES (1), (2), (3)) AS t(x)",
        ] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        let mut relation_ids = Vec::new();
        for (name, expected) in [("other", 3), ("datafusion", 1)] {
            // Reinstallation rebinds the session-global UDFs without advancing history.
            install_persistent_catalog(&session, name, db.clone())
                .await
                .unwrap();
            session
                .sql(&format!("SELECT collect_stats('{name}.public.numbers')"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let catalog = super::catalog_for_name(db.clone(), name).await.unwrap();
            let reader = catalog.reader().await.unwrap();
            let relations = reader.get_all_relations().await.unwrap();
            assert_eq!(relations.len(), 1);
            let relation_id = relations[0].relation_id;
            assert_eq!(
                reader
                    .get_relation_statistics(relation_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .row_count,
                Some(expected)
            );
            relation_ids.push(relation_id);
        }
        assert_ne!(relation_ids[0], relation_ids[1]);
        session
            .sql("DROP TABLE other.public.numbers")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session
            .sql("DROP SCHEMA other.public")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let other = super::catalog_for_name(db.clone(), "other").await.unwrap();
        assert!(
            other
                .reader()
                .await
                .unwrap()
                .get_all_relations()
                .await
                .unwrap()
                .is_empty()
        );
        let default = super::catalog_for_name(db.clone(), "datafusion")
            .await
            .unwrap();
        let reader = default.reader().await.unwrap();
        assert_eq!(
            reader.get_all_schemas().await.unwrap()[0].schema_name,
            "public"
        );
        assert_eq!(
            reader
                .get_relation_statistics(relation_ids[1])
                .await
                .unwrap()
                .unwrap()
                .row_count,
            Some(1)
        );
        assert!(
            reader
                .get_relation_statistics(relation_ids[0])
                .await
                .unwrap()
                .is_none()
        );
        assert!(uninstall_persistent_catalog(&session, "other").unwrap());
        // The helpers are currently bound to datafusion, not the catalog being removed.
        let estimates = session
            .sql("SELECT cardest('SELECT * FROM numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            estimates[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );
        assert!(uninstall_persistent_catalog(&session, "datafusion").unwrap());
        assert!(
            session
                .sql("SELECT cardest('SELECT * FROM numbers')")
                .await
                .is_err()
        );
        drop(other);
        drop(default);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persistent_tables_preserve_native_insert() {
        let (_temp_dir, db) = test_sqlite_connection().await;
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        session
            .sql("CREATE TABLE numbers AS SELECT 1 AS x")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session
            .sql("INSERT INTO numbers VALUES (2)")
            .await
            .unwrap()
            .collect()
            .await
            .expect("metadata persistence must preserve native INSERT support");
        let batches = session
            .sql("SELECT COUNT(*) FROM numbers")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persistent_raw_dml_preserves_native_behavior_without_observation() {
        let (_temp_dir, db) = test_sqlite_connection().await;
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        for sql in [
            "CREATE TABLE numbers AS SELECT 1 AS x",
            "SELECT collect_stats('numbers')",
            "CREATE DATABASE other",
            "CREATE SCHEMA other.public",
            "CREATE TABLE other.public.numbers AS SELECT 1 AS x",
        ] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        let catalog = Catalog::new(db.clone());
        let before = catalog.reader().await.unwrap();
        let relation_id = before.get_all_relations().await.unwrap()[0].relation_id;
        let statistics = before.get_relation_statistics(relation_id).await.unwrap();
        let runner = crate::runner::OptdRunner::new_with_persistent_catalog(
            session.clone(),
            "datafusion",
            db.clone(),
        );
        // Raw session calls preserve native writes, but are outside the opt-in
        // observation contract. The shared executor and runner are covered separately.
        for sql in [
            "INSERT INTO numbers VALUES (2)",
            "UPDATE numbers SET x = 3 WHERE x = 2",
            "DELETE FROM numbers WHERE x = 3",
        ] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        for sql in [
            "INSERT OVERWRITE numbers VALUES (2)",
            "TRUNCATE TABLE numbers",
        ] {
            let result = match session.sql(sql).await {
                Ok(frame) => frame.collect().await,
                Err(error) => Err(error),
            };
            let error = result.expect_err("retain the provider's native unsupported operation");
            assert!(
                !error
                    .to_string()
                    .contains("DML is not supported for persistent table")
            );
        }
        let after = catalog.reader().await.unwrap();
        assert_eq!(after.snapshot_id(), before.snapshot_id());
        assert_eq!(
            after.get_relation_statistics(relation_id).await.unwrap(),
            statistics
        );
        let batches = session
            .sql("SELECT COUNT(*) FROM numbers")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        let estimates = session
            .sql("SELECT cardest('SELECT * FROM numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            estimates[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );

        // Other namespaces and uninstalled providers retain their native writes too.
        session
            .sql("INSERT INTO other.public.numbers VALUES (2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            catalog.reader().await.unwrap().snapshot_id(),
            before.snapshot_id()
        );
        assert!(uninstall_persistent_catalog(&session, "datafusion").unwrap());
        session
            .sql("INSERT INTO numbers VALUES (2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        drop(runner);
        drop(catalog);
        db.close().await.unwrap();
    }

    #[derive(Debug)]
    struct CountingCatalog {
        inner: std::sync::Arc<dyn datafusion_catalog::CatalogProvider>,
        removals: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl datafusion_catalog::CatalogProvider for CountingCatalog {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn schema_names(&self) -> Vec<String> {
            self.inner.schema_names()
        }
        fn schema(
            &self,
            name: &str,
        ) -> Option<std::sync::Arc<dyn datafusion_catalog::SchemaProvider>> {
            self.inner.schema(name)
        }
        fn register_schema(
            &self,
            name: &str,
            schema: std::sync::Arc<dyn datafusion_catalog::SchemaProvider>,
        ) -> datafusion::error::Result<Option<std::sync::Arc<dyn datafusion_catalog::SchemaProvider>>>
        {
            self.inner.register_schema(name, schema)
        }
        fn deregister_schema(
            &self,
            name: &str,
            cascade: bool,
        ) -> datafusion::error::Result<Option<std::sync::Arc<dyn datafusion_catalog::SchemaProvider>>>
        {
            self.removals
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.deregister_schema(name, cascade)
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persistent_populated_cascade_is_rejected_before_runtime_removal() {
        use optd_catalog::advanced_statistics::{
            AdvancedStatisticsContents, CreateAdvancedStatisticsInfo,
        };
        let (_temp, db) = test_sqlite_connection().await;
        let session = session_context_with_information_schema();
        let original = session.catalog("datafusion").unwrap();
        let removals = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        session.register_catalog(
            "datafusion",
            std::sync::Arc::new(CountingCatalog {
                inner: std::sync::Arc::clone(&original),
                removals: std::sync::Arc::clone(&removals),
            }),
        );
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        for sql in [
            "CREATE SCHEMA analytics",
            "CREATE TABLE analytics.numbers AS SELECT 1 AS x, 2 AS y",
            "SELECT collect_stats('analytics.numbers')",
        ] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        let catalog = Catalog::new(db.clone());
        let relation_id = catalog
            .reader()
            .await
            .unwrap()
            .get_all_relations()
            .await
            .unwrap()[0]
            .relation_id;
        let advanced_id = catalog
            .create_advanced_statistics(
                catalog.reader().await.unwrap().snapshot_id(),
                CreateAdvancedStatisticsInfo {
                    relation_id,
                    statistics_name: "xy".into(),
                    column_ids: vec![0, 1],
                    contents: AdvancedStatisticsContents::JointNdv { distinct_count: 1 },
                },
            )
            .await
            .unwrap();
        let reader = catalog.reader().await.unwrap();
        let before = reader.snapshot_info().clone();
        let basic = reader.get_relation_statistics(relation_id).await.unwrap();
        let advanced = reader.get_advanced_statistics(advanced_id).await.unwrap();
        let runtime_schema = original.schema("analytics").unwrap();
        let output = crate::observed_execution::execute_observed_sql(
            &session,
            "DROP SCHEMA analytics CASCADE",
        )
        .await
        .unwrap();
        assert!(output.result.is_err());
        assert_eq!(
            removals.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "must reject before calling the source registry"
        );
        assert!(std::sync::Arc::ptr_eq(
            &runtime_schema,
            &original.schema("analytics").unwrap()
        ));
        assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
        assert_eq!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_relation_statistics(relation_id)
                .await
                .unwrap(),
            basic
        );
        assert_eq!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_advanced_statistics(advanced_id)
                .await
                .unwrap(),
            advanced
        );
        assert_eq!(
            reader.get_relation_statistics(relation_id).await.unwrap(),
            basic
        );
        drop(catalog);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persistent_statistics_follow_execution_time_schema_defaults() {
        let (_temp, db) = test_sqlite_connection().await;
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        for sql in [
            "CREATE TABLE numbers AS SELECT 1 AS x",
            "SELECT collect_stats('numbers')",
            "CREATE SCHEMA analytics",
            "CREATE TABLE analytics.numbers AS SELECT * FROM (VALUES (1), (2), (3)) AS t(x)",
            "SET datafusion.catalog.default_schema = 'analytics'",
            "SELECT collect_stats('numbers')",
        ] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        let catalog = Catalog::new(db.clone());
        let reader = catalog.reader().await.unwrap();
        let analytics = reader
            .get_all_schemas()
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.schema_name == "analytics")
            .unwrap()
            .schema_id;
        let relation = reader
            .get_all_relations()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.schema_id == analytics)
            .unwrap()
            .relation_id;
        assert_eq!(
            reader
                .get_relation_statistics(relation)
                .await
                .unwrap()
                .unwrap()
                .row_count,
            Some(3)
        );
        let estimate = session
            .sql("SELECT cardest('SELECT * FROM numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            estimate[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            3.0
        );
        for sql in [
            "RESET datafusion.catalog.default_schema",
            "INSERT INTO public.numbers VALUES (2)",
            "SELECT collect_stats('numbers')",
        ] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        let estimate = session
            .sql("SELECT cardest('SELECT * FROM numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            estimate[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
        assert_eq!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_relation_statistics(relation)
                .await
                .unwrap()
                .unwrap()
                .row_count,
            Some(3)
        );
        drop(catalog);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persistent_statistics_follow_execution_time_catalog_defaults() {
        let (_temp, db) = test_sqlite_connection().await;
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        for sql in [
            "CREATE TABLE numbers AS SELECT 1 AS x",
            "SELECT collect_stats('numbers')",
            "CREATE DATABASE other",
            "CREATE SCHEMA other.public",
        ] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        install_persistent_catalog(&session, "other", db.clone())
            .await
            .unwrap();
        for sql in [
            "CREATE TABLE other.public.numbers AS SELECT * FROM (VALUES (1),(2),(3)) AS t(x)",
            "SET datafusion.catalog.default_catalog = 'other'",
            "SELECT collect_stats('numbers')",
        ] {
            session.sql(sql).await.unwrap().collect().await.unwrap();
        }
        let estimate = session
            .sql("SELECT cardest('SELECT * FROM numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            estimate[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            3.0
        );
        session
            .sql("RESET datafusion.catalog.default_catalog")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let catalog = super::catalog_for_name(db.clone(), "other").await.unwrap();
        let before = catalog.reader().await.unwrap().snapshot_info().clone();
        let rejected = session
            .sql("SELECT collect_stats('numbers')")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(
            rejected.is_err(),
            "RESET restores the default catalog, not the UDF's bound catalog"
        );
        assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
        let estimate = session
            .sql("SELECT cardest('SELECT * FROM numbers')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            estimate[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1000.0
        );
        drop(catalog);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn non_finite_statistics_do_not_break_cardinality_estimation() {
        let (_temp_dir, db) = test_sqlite_connection().await;
        let session = session_context_with_information_schema();
        install_persistent_catalog(&session, "datafusion", db.clone())
            .await
            .unwrap();
        session
            .sql(
                "CREATE TABLE float_values AS SELECT \
             CAST('Infinity' AS DOUBLE) AS positive, \
             CAST('-Infinity' AS DOUBLE) AS negative, \
             CAST('NaN' AS DOUBLE) AS nan_value",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session
            .sql("SELECT collect_stats('float_values')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batches = session
            .sql("SELECT cardest('SELECT * FROM float_values')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );
        let catalog = Catalog::new(db.clone());
        let reader = catalog.reader().await.unwrap();
        let relation_id = reader.get_all_relations().await.unwrap()[0].relation_id;
        let statistics = reader
            .get_relation_statistics(relation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(statistics.row_count, Some(1));
        assert_eq!(statistics.column_statistics.len(), 3);
        for column in statistics.column_statistics {
            assert_eq!(column.frequency, Some(1));
            assert_eq!(column.distinct_count, Some(1));
            assert_eq!(column.lower_bound, None);
            assert_eq!(column.upper_bound, None);
        }
        drop(catalog);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persistent_statistics_are_scoped_to_the_installed_catalog() {
        for bound_catalog in ["datafusion", "other"] {
            let (_temp_dir, db) = test_sqlite_connection().await;
            let session = session_context_with_information_schema();
            for sql in ["CREATE DATABASE other", "CREATE SCHEMA other.public"] {
                session.sql(sql).await.unwrap().collect().await.unwrap();
            }
            install_persistent_catalog(&session, bound_catalog, db.clone())
                .await
                .unwrap();
            for sql in [
                "CREATE TABLE datafusion.public.numbers AS SELECT 1 AS x",
                "CREATE TABLE other.public.numbers AS \
                 SELECT * FROM (VALUES (1), (2), (3)) AS source(x)",
            ] {
                session.sql(sql).await.unwrap().collect().await.unwrap();
            }
            session
                .sql(&format!(
                    "SELECT collect_stats('{bound_catalog}.public.numbers')"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();

            let catalog = super::catalog_for_name(db.clone(), bound_catalog)
                .await
                .unwrap();
            let before = catalog.reader().await.unwrap();
            let relation_id = before.get_all_relations().await.unwrap()[0].relation_id;
            let statistics = before
                .get_relation_statistics(relation_id)
                .await
                .unwrap()
                .unwrap();
            let expected_rows = if bound_catalog == "datafusion" { 1 } else { 3 };
            assert_eq!(statistics.row_count, Some(expected_rows));
            let foreign_catalog = if bound_catalog == "datafusion" {
                "other"
            } else {
                "datafusion"
            };
            let error = session
                .sql(&format!(
                    "SELECT collect_stats('{foreign_catalog}.public.numbers')"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .expect_err("foreign-catalog collection must not overwrite bound statistics");
            assert!(
                error.to_string().contains("bound to DataFusion catalog"),
                "{error}"
            );
            let after = catalog.reader().await.unwrap();
            assert_eq!(after.snapshot_id(), before.snapshot_id());
            assert_eq!(
                after.get_relation_statistics(relation_id).await.unwrap(),
                Some(statistics)
            );

            // Bare references resolve using the session default, not the installed catalog.
            if bound_catalog == "other" {
                session
                    .sql("SELECT collect_stats('numbers')")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .expect_err("bare numbers belongs to datafusion");
            }
            for (table, expected) in [
                (
                    format!("{bound_catalog}.public.numbers"),
                    expected_rows as f64,
                ),
                (format!("{foreign_catalog}.public.numbers"), 1000.0),
                (
                    "numbers".to_owned(),
                    if bound_catalog == "datafusion" {
                        1.0
                    } else {
                        1000.0
                    },
                ),
            ] {
                let batches = session
                    .sql(&format!("SELECT cardest('SELECT * FROM {table}')"))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                let estimate = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0);
                assert_eq!(
                    estimate, expected,
                    "bound catalog {bound_catalog}, table {table}"
                );
            }
            drop(catalog);
            db.close().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn statistics_udfs_persist_stats_and_advance_the_snapshot() {
        let (_temp_dir, db) = test_sqlite_connection().await;
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
        let (_temp_dir, db) = test_sqlite_connection().await;
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
        let (_temp_dir, db) = test_sqlite_connection().await;
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
