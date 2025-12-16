use datafusion::{
    arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::TableProvider,
    common::Result,
    datasource::MemTable,
    logical_expr::Expr,
};
use optd_catalog::CatalogServiceHandle;
use std::sync::Arc;

/// User-Defined Table Function for listing snapshots
///
/// Usage: SELECT * FROM list_snapshots()
#[derive(Debug)]
pub struct ListSnapshotsFunction {
    catalog_handle: Arc<Option<CatalogServiceHandle>>,
}

impl ListSnapshotsFunction {
    pub fn new(catalog_handle: Option<CatalogServiceHandle>) -> Self {
        Self {
            catalog_handle: Arc::new(catalog_handle),
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("schema_version", DataType::Int64, false),
            Field::new("next_catalog_id", DataType::Int64, false),
            Field::new("next_file_id", DataType::Int64, false),
        ]))
    }
}

impl datafusion::catalog::TableFunctionImpl for ListSnapshotsFunction {
    fn call(&self, _exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let catalog_handle = self.catalog_handle.as_ref();

        if let Some(handle) = catalog_handle {
            // Use tokio runtime to execute async code
            let handle_clone = handle.clone();
            let snapshots = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(async { handle_clone.list_snapshots().await })
            })
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

            // Build arrays from snapshot metadata
            let ids: Vec<i64> = snapshots.iter().map(|s| s.id.0).collect();
            let schema_versions: Vec<i64> = snapshots.iter().map(|s| s.schema_version).collect();
            let next_catalog_ids: Vec<i64> = snapshots.iter().map(|s| s.next_catalog_id).collect();
            let next_file_ids: Vec<i64> = snapshots.iter().map(|s| s.next_file_id).collect();

            let schema = Self::schema();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(schema_versions)),
                    Arc::new(Int64Array::from(next_catalog_ids)),
                    Arc::new(Int64Array::from(next_file_ids)),
                ],
            )?;

            Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
        } else {
            // No catalog handle - return empty table
            let schema = Self::schema();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(Vec::<i64>::new())),
                    Arc::new(Int64Array::from(Vec::<i64>::new())),
                    Arc::new(Int64Array::from(Vec::<i64>::new())),
                    Arc::new(Int64Array::from(Vec::<i64>::new())),
                ],
            )?;

            Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
        }
    }
}

/// User-Defined Table Function for listing tables at a specific snapshot
///
/// Usage: SELECT * FROM list_tables_at_snapshot(5)
#[derive(Debug)]
pub struct ListTablesAtSnapshotFunction {
    catalog_handle: Arc<Option<CatalogServiceHandle>>,
}

impl ListTablesAtSnapshotFunction {
    pub fn new(catalog_handle: Option<CatalogServiceHandle>) -> Self {
        Self {
            catalog_handle: Arc::new(catalog_handle),
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("location", DataType::Utf8, false),
            Field::new("file_format", DataType::Utf8, false),
            Field::new("compression", DataType::Utf8, true),
        ]))
    }
}

impl datafusion::catalog::TableFunctionImpl for ListTablesAtSnapshotFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        // Extract snapshot_id from arguments
        if exprs.len() != 1 {
            return datafusion::common::exec_err!(
                "list_tables_at_snapshot requires exactly 1 argument (snapshot_id), got {}",
                exprs.len()
            );
        }

        // Parse the snapshot_id from the expression
        let snapshot_id = match &exprs[0] {
            Expr::Literal(datafusion::scalar::ScalarValue::Int64(Some(id)), _) => *id,
            _ => {
                return datafusion::common::exec_err!(
                    "list_tables_at_snapshot requires an integer snapshot_id argument"
                );
            }
        };

        let catalog_handle = self.catalog_handle.as_ref();

        if let Some(handle) = catalog_handle {
            // Use tokio runtime to execute async code
            let handle_clone = handle.clone();
            let tables = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    handle_clone
                        .list_external_tables_at_snapshot(None, snapshot_id)
                        .await
                })
            })
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

            // Build arrays from table metadata
            let table_names: Vec<String> = tables.iter().map(|t| t.table_name.clone()).collect();
            let locations: Vec<String> = tables.iter().map(|t| t.location.clone()).collect();
            let formats: Vec<String> = tables.iter().map(|t| t.file_format.clone()).collect();
            let compressions: Vec<Option<String>> =
                tables.iter().map(|t| t.compression.clone()).collect();

            let schema = Self::schema();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(table_names)),
                    Arc::new(StringArray::from(locations)),
                    Arc::new(StringArray::from(formats)),
                    Arc::new(StringArray::from(compressions)),
                ],
            )?;

            Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
        } else {
            return datafusion::common::exec_err!("Catalog not available for time-travel queries");
        }
    }
}
