use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use arrow_schema::{Field, FieldRef, Fields, Schema, SchemaBuilder, SchemaRef};
use snafu::{Snafu, ensure};

use crate::ir::table_ref::TableRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptdSchema {
    inner: SchemaRef,
    table_refs: Vec<TableRef>,
}

/// Schema-related errors
#[derive(Debug, Snafu)]
pub enum SchemaError {
    /// Schema contains duplicate fields.
    #[snafu(display("Schema contains duplicate fields: {}.{}", table_ref, name))]
    DuplicateField { table_ref: TableRef, name: String },
}

impl OptdSchema {
    pub fn new(schema: SchemaRef, table_refs: Vec<TableRef>) -> Self {
        Self {
            inner: schema,
            table_refs,
        }
    }
    pub fn new_with_metadata(
        qualified_fields: Vec<(TableRef, Arc<Field>)>,
        metadata: HashMap<String, String>,
    ) -> Result<Self, SchemaError> {
        let (table_refs, fields): (Vec<TableRef>, Vec<Arc<Field>>) =
            qualified_fields.into_iter().unzip();

        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));

        let optd_schema = Self::new(schema, table_refs);
        optd_schema.check_names()?;
        Ok(optd_schema)
    }

    /// Get the inner Arrow schema.
    pub fn inner(&self) -> &SchemaRef {
        &self.inner
    }

    pub fn column_with_name(&self, column_name: &str) -> Option<(usize, &Field)> {
        self.inner.column_with_name(column_name)
    }

    /// Create a `OptdSchema` from an Arrow schema and a given qualifier
    pub fn try_from_qualified_schema(
        table_ref: impl Into<TableRef>,
        schema: &Schema,
    ) -> Result<Self, SchemaError> {
        let table_ref = table_ref.into();
        let schema = Self::new(schema.clone().into(), vec![table_ref; schema.fields.len()]);
        // schema.check_names()?;
        Ok(schema)
    }

    /// Check if the schema have some fields with the same name
    pub fn check_names(&self) -> Result<(), SchemaError> {
        let mut qualified_names = BTreeSet::new();

        for (field, qualifier) in self.inner.fields().iter().zip(&self.table_refs) {
            ensure!(
                qualified_names.insert((qualifier, field.name())),
                DuplicateFieldSnafu {
                    table_ref: qualifier.clone(),
                    name: field.name().to_string(),
                }
            )
        }

        Ok(())
    }

    /// Create a new schema that contains the fields from this schema followed by the fields
    /// from the supplied schema. An error will be returned if there are duplicate field names.
    pub fn join(&self, schema: &OptdSchema) -> Result<Self, SchemaError> {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.extend(self.inner.fields().iter().cloned());
        schema_builder.extend(schema.fields().iter().cloned());
        let new_schema = schema_builder.finish();

        let mut new_metadata = self.inner.metadata.clone();
        new_metadata.extend(schema.inner.metadata.clone());
        let new_schema_with_metadata = new_schema.with_metadata(new_metadata);

        let mut new_table_refs = self.table_refs.clone();
        new_table_refs.extend_from_slice(schema.table_refs.as_slice());

        let new_self = Self {
            inner: Arc::new(new_schema_with_metadata),
            table_refs: new_table_refs,
        };
        new_self.check_names()?;
        Ok(new_self)
    }

    /// Get a list of fields for this schema
    pub fn fields(&self) -> &Fields {
        &self.inner.fields
    }

    /// Returns a reference to [`FieldRef`] for a column at specific index
    /// within the schema.
    ///
    /// See also [Self::qualified_field] to get both qualifier and field
    pub fn field(&self, i: usize) -> &FieldRef {
        &self.inner.fields[i]
    }

    /// Iterate over the qualifiers and fields in the DFSchema
    pub fn iter(&self) -> impl Iterator<Item = (&TableRef, &FieldRef)> {
        self.table_refs.iter().zip(self.inner.fields().iter())
    }
}

impl std::fmt::Display for OptdSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fields:[{}]",
            self.iter()
                .map(|(q, f)| format!("{q}.{}", f.name()))
                .collect::<Vec<String>>()
                .join(", "),
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn test_duplicate_field_detection() -> Result<(), SchemaError> {
        let t1 = TableRef::bare("t1");
        let schema = OptdSchema::try_from_qualified_schema(
            t1.clone(),
            &Schema::new(vec![Field::new("a", DataType::Int64, false)]),
        )?;

        let other = schema.clone();
        let res = schema.join(&other);

        assert!(matches!(res, Err(SchemaError::DuplicateField { .. })));
        Ok(())
    }
}
