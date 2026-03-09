use std::sync::Arc;

/// A fully resolved path to a table of the form "catalog.schema.table"
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ResolvedTableRef {
    /// The catalog (aka database) containing the table
    pub catalog: Arc<str>,
    /// The schema containing the table
    pub schema: Arc<str>,
    /// The table name
    pub table: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableRef {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: Arc<str>,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: Arc<str>,
        /// The table name
        table: Arc<str>,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: Arc<str>,
        /// The schema containing the table
        schema: Arc<str>,
        /// The table name
        table: Arc<str>,
    },
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableRef::Bare { table } => write!(f, "{table}"),
            TableRef::Partial { schema, table } => {
                write!(f, "{schema}.{table}")
            }
            TableRef::Full {
                catalog,
                schema,
                table,
            } => write!(f, "{catalog}.{schema}.{table}"),
        }
    }
}

impl TableRef {
    /// Convenience method for creating a typed none `None`
    pub fn none() -> Option<Self> {
        None
    }

    /// Convenience method for creating a [`TableReference::Bare`]
    ///
    /// As described on [`TableReference`] this does *NO* normalization at
    /// all, so "Foo.Bar" stays as a reference to the table named
    /// "Foo.Bar" (rather than "foo"."bar")
    pub fn bare(table: impl Into<Arc<str>>) -> Self {
        Self::Bare {
            table: table.into(),
        }
    }

    /// Convenience method for creating a [`TableReference::Partial`].
    ///
    /// Note: *NO* normalization is applied to the schema or table name.
    pub fn partial(schema: impl Into<Arc<str>>, table: impl Into<Arc<str>>) -> Self {
        Self::Partial {
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// Convenience method for creating a [`TableReference::Full`]
    ///
    /// Note: *NO* normalization is applied to the catalog, schema or table
    /// name.
    pub fn full(
        catalog: impl Into<Arc<str>>,
        schema: impl Into<Arc<str>>,
        table: impl Into<Arc<str>>,
    ) -> Self {
        Self::Full {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// Retrieve the table name, regardless of qualification.
    pub fn table(&self) -> &str {
        match self {
            Self::Full { table, .. } | Self::Partial { table, .. } | Self::Bare { table } => table,
        }
    }

    /// Retrieve the schema name if [`Self::Partial]` or [`Self::`Full`],
    /// `None` otherwise.
    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::Full { schema, .. } | Self::Partial { schema, .. } => Some(schema),
            _ => None,
        }
    }

    /// Retrieve the catalog name if  [`Self::Full`], `None` otherwise.
    pub fn catalog(&self) -> Option<&str> {
        match self {
            Self::Full { catalog, .. } => Some(catalog),
            _ => None,
        }
    }

    /// Given a default catalog and schema, ensure this table reference is fully
    /// resolved
    pub fn resolve(self, default_catalog: &str, default_schema: &str) -> ResolvedTableRef {
        match self {
            Self::Full {
                catalog,
                schema,
                table,
            } => ResolvedTableRef {
                catalog,
                schema,
                table,
            },
            Self::Partial { schema, table } => ResolvedTableRef {
                catalog: default_catalog.into(),
                schema,
                table,
            },
            Self::Bare { table } => ResolvedTableRef {
                catalog: default_catalog.into(),
                schema: default_schema.into(),
                table,
            },
        }
    }
}
