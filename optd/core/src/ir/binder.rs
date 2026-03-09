use std::collections::HashMap;

use arrow_schema::{Field, SchemaRef};
use snafu::{OptionExt, whatever};

use crate::error::{Error, Result};
use crate::ir::schema::OptdSchema;
use crate::ir::table_ref::TableRef;

pub struct BindContext {
    next_table_index: i64,
    bindings: HashMap<i64, Binding>,
    scopes: Vec<Vec<(TableRef, i64)>>,
}

impl Default for BindContext {
    fn default() -> Self {
        Self::new()
    }
}

impl BindContext {
    pub fn new() -> Self {
        Self {
            next_table_index: 1,
            bindings: HashMap::new(),
            scopes: vec![Vec::new()],
        }
    }

    pub fn begin_scope(&mut self) {
        self.scopes.push(Vec::new());
    }

    pub fn end_scope(&mut self) {
        self.scopes.pop();
    }

    fn get_next_table_index(&mut self) -> Option<i64> {
        let table_index = self.next_table_index;
        self.next_table_index = self.next_table_index.checked_add(1)?;
        Some(table_index)
    }

    pub fn add_binding(&mut self, table_ref: Option<TableRef>, schema: SchemaRef) -> Result<i64> {
        let table_index = self
            .get_next_table_index()
            .whatever_context("run out of table index")?;
        let table_ref =
            table_ref.unwrap_or_else(|| TableRef::bare(format!("__internal_#{table_index}")));
        self.bindings.insert(
            table_index,
            Binding::new(
                OptdSchema::try_from_qualified_schema(table_ref.clone(), &schema)
                    .map_err(|message| Error::Schema { message })?,
                table_index,
            ),
        );
        let scope = self.scopes.last_mut().unwrap();
        scope.push((table_ref, table_index));
        Ok(table_index)
    }

    pub fn get_binding(&self, table_index: &i64) -> Option<&Binding> {
        self.bindings.get(table_index)
    }

    pub fn get_local_bindings(&self) -> Vec<&Binding> {
        let current_scope = self.scopes.last().expect("no scope exists");
        current_scope
            .iter()
            .map(|(_, table_index)| {
                self.get_binding(table_index)
                    .expect("no binding for given table index")
            })
            .collect()
    }

    pub fn get_binding_by_table_ref(&self, table_ref: &TableRef) -> Result<Option<&Binding>> {
        let mut matches = Vec::new();
        for scope in self.scopes.iter().rev() {
            for (t, table_index) in scope {
                if t == table_ref {
                    let binding = &self
                        .bindings
                        .get(table_index)
                        .whatever_context("binding not found")?;
                    matches.push(*binding);
                }
            }
            if !matches.is_empty() {
                // Do not go to an outer scope if there is some match.
                break;
            }
        }
        match &matches[..] {
            [] => Ok(None),
            [binding] => Ok(Some(*binding)),
            _ => whatever!(
                "table reference {:?} matches with multiple bindings within a scope: {:?}",
                table_ref,
                matches
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Binding {
    pub schema: OptdSchema,
    pub table_index: i64,
}

impl Binding {
    pub fn new(schema: OptdSchema, table_index: i64) -> Self {
        Self {
            schema,
            table_index,
        }
    }

    pub fn column_with_name(&self, column_name: &str) -> Option<(usize, &Field)> {
        self.schema.column_with_name(column_name)
    }

    pub fn schema(&self) -> &OptdSchema {
        &self.schema
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Schema};
    use itertools::Itertools;

    use super::*;

    fn make_schema(fields: Vec<&str>) -> SchemaRef {
        let fields = fields
            .into_iter()
            .map(|name| Field::new(name, DataType::Int32, false))
            .collect_vec();
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_add_binding_and_get_binding() {
        let mut ctx = BindContext::new();
        let table_ref = TableRef::bare("t1");
        let schema = make_schema(vec!["a", "b"]);
        assert_eq!(None, ctx.get_binding_by_table_ref(&table_ref).unwrap());
        let table_index = ctx
            .add_binding(Some(table_ref.clone()), schema.clone())
            .unwrap();

        let binding = ctx.get_binding_by_table_ref(&table_ref).unwrap().unwrap();
        assert_eq!(binding.table_index, table_index);
        assert_eq!(binding.schema.fields().len(), 2);
        assert!(matches!(binding.column_with_name("a"), Some((0, _))));
        assert!(matches!(binding.column_with_name("b"), Some((1, _))));
        assert!(binding.column_with_name("c").is_none());

        ctx.begin_scope();

        let binding = ctx.get_binding_by_table_ref(&table_ref).unwrap().unwrap();
        assert_eq!(binding.table_index, table_index);
        assert_eq!(binding.schema.fields().len(), 2);
        assert!(matches!(binding.column_with_name("a"), Some((0, _))));
        assert!(matches!(binding.column_with_name("b"), Some((1, _))));
        assert!(binding.column_with_name("c").is_none());

        ctx.end_scope();

        let binding = ctx.get_binding_by_table_ref(&table_ref).unwrap().unwrap();
        assert_eq!(binding.table_index, table_index);
        assert_eq!(binding.schema.fields().len(), 2);
        assert!(matches!(binding.column_with_name("a"), Some((0, _))));
        assert!(matches!(binding.column_with_name("b"), Some((1, _))));
        assert!(binding.column_with_name("c").is_none());
    }

    #[test]
    fn test_add_binding_and_get_binding_inner_scope() {
        let mut ctx = BindContext::new();
        let table_ref = TableRef::bare("t1");
        let schema = make_schema(vec!["a", "b"]);
        assert_eq!(None, ctx.get_binding_by_table_ref(&table_ref).unwrap());

        ctx.begin_scope();

        let table_index = ctx
            .add_binding(Some(table_ref.clone()), schema.clone())
            .unwrap();

        let binding = ctx.get_binding_by_table_ref(&table_ref).unwrap().unwrap();
        assert_eq!(binding.table_index, table_index);
        assert_eq!(binding.schema.fields().len(), 2);
        assert!(matches!(binding.column_with_name("a"), Some((0, _))));
        assert!(matches!(binding.column_with_name("b"), Some((1, _))));
        assert!(binding.column_with_name("c").is_none());

        ctx.end_scope();

        // The table ref defined in the inner scope should no longer be available.
        assert_eq!(None, ctx.get_binding_by_table_ref(&table_ref).unwrap());
    }
}
