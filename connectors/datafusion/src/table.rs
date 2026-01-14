use std::{any::Any, borrow::Cow, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{Constraints, Statistics},
    datasource::{TableType, listing::ListingTable},
    error::Result,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
    sql::TableReference,
};

#[allow(dead_code)]
pub struct OptdTable {
    inner: Box<ListingTable>,
    name: String,
    table_reference: TableReference,
}

impl OptdTable {
    pub fn try_new(
        inner: ListingTable,
        name: String,
        table_reference: TableReference,
    ) -> Result<Self> {
        Ok(Self {
            inner: Box::new(inner),
            name,
            table_reference,
        })
    }

    pub fn new_with_inner(
        inner: Box<ListingTable>,
        name: String,
        table_reference: TableReference,
    ) -> Self {
        Self {
            inner,
            name,
            table_reference,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn table_reference(&self) -> &TableReference {
        &self.table_reference
    }
}

#[derive(Debug, Clone)]
pub struct OptdTableProvider {
    inner: Arc<dyn TableProvider>,
    table_name: String,
}

impl OptdTableProvider {
    pub fn new(inner: Arc<dyn TableProvider>, table_name: String) -> Self {
        Self { inner, table_name }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[async_trait::async_trait]
impl TableProvider for OptdTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    fn statistics(&self) -> Option<Statistics> {
        let stats = self.inner.statistics();

        if let Some(ref s) = stats {
            tracing::debug!(
                "Retrieved statistics from inner provider for table {} (num_rows={:?}, total_byte_size={:?})",
                self.table_name,
                s.num_rows,
                s.total_byte_size
            );
        } else {
            tracing::debug!(
                "No statistics available for table {} from inner provider",
                self.table_name
            );
        }

        stats
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(_state, _input, _insert_op).await
    }
}
