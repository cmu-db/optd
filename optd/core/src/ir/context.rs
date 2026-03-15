//! IRContext holds shared context for the IR, including catalog access,
//! cardinality estimation, and cost modeling.

use crate::error::Result;
use crate::ir::binder::Binding;
use crate::ir::{
    Column, ColumnMeta, binder::BindContext, catalog::Catalog, cost::CostModel,
    properties::CardinalityEstimator, table_ref::TableRef,
};
use arrow_schema::{Field, SchemaRef};
use snafu::OptionExt;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct IRContext {
    /// An accessor to the catalog interface.
    pub catalog: Arc<dyn Catalog>,
    /// An accessor to the cardinality estimator.
    pub card: Arc<dyn CardinalityEstimator>,
    /// An accessor to the cost model.
    pub cm: Arc<dyn CostModel>,

    pub binder: Arc<RwLock<BindContext>>,
}

impl IRContext {
    pub fn new(
        cat: Arc<dyn Catalog>,
        card: Arc<dyn CardinalityEstimator>,
        cost: Arc<dyn CostModel>,
    ) -> Self {
        Self {
            card,
            catalog: cat,
            cm: cost,
            binder: Arc::new(RwLock::new(BindContext::new())),
        }
    }

    pub fn add_binding(&self, table_ref: Option<TableRef>, schema: SchemaRef) -> Result<i64> {
        self.binder.write().unwrap().add_binding(table_ref, schema)
    }

    pub fn get_binding(&self, table_index: &i64) -> Result<Binding> {
        let guard = self.binder.read().unwrap();
        guard
            .get_binding(table_index)
            .cloned()
            .whatever_context("binding not found")
    }

    pub fn binder_begin_scope(&self) {
        self.binder.write().unwrap().begin_scope()
    }

    pub fn binder_end_scope(&self) {
        self.binder.write().unwrap().end_scope()
    }

    pub fn get_column_meta(&self, column: &Column) -> ColumnMeta {
        let Column(table_index, column_index) = column;
        let guard = self.binder.read().unwrap();
        let binding = guard.get_binding(table_index).unwrap();
        let field = binding.field(*column_index).unwrap();
        ColumnMeta {
            table_ref: binding.table_ref().clone(),
            data_type: field.data_type().clone(),
            name: field.name().clone(),
        }
    }

    pub fn get_column_name(&self, column: &Column) -> Result<(TableRef, Arc<Field>)> {
        let Column(table_index, column_index) = column;
        let guard = self.binder.read().unwrap();
        let binding = guard
            .get_binding(table_index)
            .whatever_context("binding not found")?;
        let field = binding
            .field(*column_index)
            .whatever_context("column not found")?;
        Ok((binding.table_ref().clone(), field.clone()))
    }

    pub fn get_column_by_name(
        &self,
        table_ref: Option<&TableRef>,
        column_name: &str,
    ) -> Result<Column> {
        let binder = self.binder.read().unwrap();
        match table_ref {
            Some(table_ref) => {
                let binding = binder.get_binding_by_table_ref(table_ref)?;
                binding
                    .and_then(|binding| {
                        binding
                            .column_with_name(column_name)
                            .map(|(index, _)| Column(binding.table_index, index))
                    })
                    .whatever_context("column not found")
            }

            None => {
                // TODO(yuchen): when there is no table_ref, do we consider outer scopes for binding?
                // In the current implementation, we don't.
                let local_bindings = binder.get_local_bindings();
                local_bindings
                    .iter()
                    .find_map(|x| {
                        x.column_with_name(column_name)
                            .map(|(index, _)| Column(x.table_index, index))
                    })
                    .whatever_context("column not found")
            }
        }
    }
}
