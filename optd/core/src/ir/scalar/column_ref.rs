//! A reference to a Column in the IR.

use pretty_xmlish::Pretty;
use crate::ir::{
    Column, IRCommon,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    ColumnRef, ColumnRefBorrowed {
        properties: ScalarProperties,
        metadata: ColumnRefMetadata {
            column: Column,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_scalar_conversion!(ColumnRef, ColumnRefBorrowed);

/// Metadata:
/// - column: The referenced column.
/// Scalars: (none)
impl ColumnRef {
    pub fn new(column: Column) -> Self {
        Self {
            meta: ColumnRefMetadata { column },
            common: IRCommon::empty(),
        }
    }
}

impl Explain for ColumnRefBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        _option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let meta = ctx.get_column_meta(self.column());
        Pretty::display(&format!("{}({})", meta.name, self.column()))
    }
}
