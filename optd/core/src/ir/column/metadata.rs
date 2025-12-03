use std::{collections::HashMap, sync::Arc};

use crate::ir::{Column, DataType, catalog::DataSourceId};

pub struct ColumnMeta {
    pub data_type: DataType,
    pub name: String,
    pub source_table: Option<DataSourceId>,
}

#[derive(Default)]
pub struct ColumnMetaStore {
    columns: Vec<Arc<ColumnMeta>>,
    name_to_column_id: HashMap<String, Column>,
}

impl ColumnMetaStore {
    pub fn get(&self, column: &Column) -> &Arc<ColumnMeta> {
        let Column(index) = *column;
        let Some(res) = self.columns.get(index) else {
            panic!("{column} does not have meta");
        };
        res
    }

    pub fn column_by_name(&self, name: &str) -> Option<Column> {
        self.name_to_column_id.get(name).cloned()
    }

    pub fn new_column(
        &mut self,
        data_type: DataType,
        name: Option<String>,
        source_table: Option<DataSourceId>,
    ) -> Column {
        let column = Column(self.columns.len());
        let name = name.unwrap_or_else(|| column.to_string());
        let old = self.name_to_column_id.insert(name.clone(), column);
        // assert!(old.is_none());
        self.columns.push(Arc::new(ColumnMeta {
            data_type,
            name,
            source_table,
        }));
        column
    }

    pub fn add_column_alias(&mut self, column: Column, alias: String) {
        let Column(index) = column;
        let x = self.columns.get_mut(index).unwrap();
        match Arc::get_mut(x) {
            Some(column_meta) => column_meta.name = alias.clone(),
            None => {
                *x = Arc::new(ColumnMeta {
                    name: alias.clone(),
                    data_type: x.data_type,
                    source_table: x.source_table,
                })
            }
        }
        self.name_to_column_id.insert(alias, column);
    }
}
