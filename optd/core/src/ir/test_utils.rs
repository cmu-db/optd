use std::sync::Arc;

use snafu::ResultExt;

use crate::{
    error::CatalogSnafu,
    ir::{
        Column, DataType, IRContext,
        catalog::{Catalog, Field, Schema},
        table_ref::TableRef,
    },
    magic::MemoryCatalog,
};

pub(crate) fn test_ctx_with_tables(tables: &[(&str, usize)]) -> crate::error::Result<IRContext> {
    let catalog = MemoryCatalog::new("optd", "public");
    for (table_name, width) in tables {
        let schema = Arc::new(Schema::new(
            (0..*width)
                .map(|idx| Field::new(format!("c{idx}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        ));
        catalog
            .create_table(TableRef::bare(*table_name), schema)
            .context(CatalogSnafu)?;
    }
    Ok(IRContext::with_memory_catalog(catalog))
}

pub(crate) fn test_col(
    ctx: &IRContext,
    table: &str,
    column: &str,
) -> crate::error::Result<Column> {
    ctx.col(Some(&TableRef::bare(table)), column)
}
