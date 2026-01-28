//! This module defines a metadata type for columns in tables. See column/mod.rs
//! for information about the base column type

use crate::ir::{Column, DataType};
use std::{collections::HashMap, sync::Arc};

/// Note that the column data type only stores the column identifier, all other
/// metadata (data type, name) is stored in the ColumnMeta type and accessible
/// from a column meta store given a column
pub struct ColumnMeta {
    pub data_type: DataType,
    pub name: String,
}

/// The context for a given execution needs to store metadata for all columns
/// in the plan, and the ColumnMetaStore is the data structure used for this
/// purpose
///
/// The columns field is indexed by the column's globally unique ID, i.e. the
/// metadata for Column(id) is in columns[id]. Accessor methods (get) are
/// provided for this conversion
#[derive(Default)]
pub struct ColumnMetaStore {
    columns: Vec<Arc<ColumnMeta>>,
    name_to_column_id: HashMap<String, Column>,
}

impl ColumnMetaStore {
    pub fn get(&self, column: &Column) -> Arc<ColumnMeta> {
        let Column(index) = *column;
        let Some(res) = self.columns.get(index) else {
            panic!("{column} does not have meta");
        };
        res.clone()
    }

    pub fn column_by_name(&self, name: &str) -> Option<Column> {
        self.name_to_column_id.get(name).cloned()
    }

    /// The name of this column must be globally unique
    pub fn new_column(&mut self, data_type: DataType, name: Option<String>) -> Column {
        let column = Column(self.columns.len());
        let name = name.unwrap_or_else(|| column.to_string());
        let old = self.name_to_column_id.insert(name.clone(), column);
        assert!(old.is_none());
        self.columns.push(Arc::new(ColumnMeta { data_type, name }));
        column
    }

    /// Adds a new alias in the name_to_column_id map for this column, and
    /// renames it in the column meta (i.e. this becomes its primary name)
    pub fn add_column_alias(&mut self, column: Column, alias: String) {
        let Column(index) = column;
        let x = self
            .columns
            .get_mut(index)
            .expect("Invalid column ID provided to meta-store");
        match Arc::get_mut(x) {
            Some(column_meta) => column_meta.name = alias.clone(),
            None => {
                *x = Arc::new(ColumnMeta {
                    name: alias.clone(),
                    data_type: x.data_type.clone(),
                })
            }
        }
        self.name_to_column_id.insert(alias, column);
    }
}
