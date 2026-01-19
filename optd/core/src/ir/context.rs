//! IRContext holds shared context for the IR, including catalog access,
//! cardinality estimation, and cost modeling.

use crate::ir::{
    Column, ColumnMeta, ColumnMetaStore, DataType,
    catalog::{Catalog, DataSourceId, Schema},
    cost::CostModel,
    properties::CardinalityEstimator,
};
use itertools::Itertools;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct IRContext {
    /// An accessor to the catalog interface.
    pub cat: Arc<dyn Catalog>,
    /// An accessor to the cardinality estimator.
    pub card: Arc<dyn CardinalityEstimator>,
    /// An accessor to the cost model.
    pub cm: Arc<dyn CostModel>,

    pub(crate) source_to_first_column_id: Arc<Mutex<HashMap<DataSourceId, Column>>>,
    pub(crate) column_meta: Arc<Mutex<ColumnMetaStore>>,
}

impl IRContext {
    pub fn new(
        cat: Arc<dyn Catalog>,
        card: Arc<dyn CardinalityEstimator>,
        cost: Arc<dyn CostModel>,
    ) -> Self {
        Self {
            card,
            cat,
            cm: cost,
            source_to_first_column_id: Arc::default(),
            column_meta: Arc::default(),
        }
    }

    pub fn add_base_table_columns(&self, source: DataSourceId, schema: &Schema) -> Column {
        let mut mapping = self.source_to_first_column_id.lock().unwrap();
        use std::collections::hash_map::Entry;
        match mapping.entry(source) {
            Entry::Occupied(occupied) => *occupied.get(),
            Entry::Vacant(vacant) => {
                let mut column_meta = self.column_meta.lock().unwrap();
                let columns = schema
                    .columns()
                    .iter()
                    .map(|field| column_meta.new_column(field.data_type, Some(field.name.clone())))
                    .collect_vec();
                vacant.insert(columns[0]);
                columns[0]
            }
        }
    }

    pub fn define_column(&self, data_type: DataType, name: Option<String>) -> Column {
        let mut column_meta = self.column_meta.lock().unwrap();
        column_meta.new_column(data_type, name)
    }

    pub fn rename_column_alias(&self, column: Column, alias: String) {
        let mut column_meta = self.column_meta.lock().unwrap();
        column_meta.add_column_alias(column, alias);
    }

    pub fn column_by_name(&self, ident: &str) -> Option<Column> {
        self.columns_by_name(&[ident]).map(|v| v[0])
    }

    pub fn columns_by_name(&self, idents: &[&str]) -> Option<Vec<Column>> {
        let column_meta = self.column_meta.lock().unwrap();
        idents
            .iter()
            .map(|name| column_meta.column_by_name(name))
            .collect()
    }

    pub fn get_column_meta(&self, column: &Column) -> Arc<ColumnMeta> {
        let column_meta = self.column_meta.lock().unwrap();
        column_meta.get(column)
    }
}
