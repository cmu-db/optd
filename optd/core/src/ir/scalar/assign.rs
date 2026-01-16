//! Assigns a scalar expression to a new Column

use std::sync::Arc;
use pretty_xmlish::Pretty;
use crate::ir::{
    Column, IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    ColumnAssign, ColumnAssignBorrowed {
        properties: ScalarProperties,
        metadata: ColumnAssignMetadata {
            column: Column,
        },
        inputs: {
            operators: [],
            scalars: [expr],
        }
    }
);
impl_scalar_conversion!(ColumnAssign, ColumnAssignBorrowed);

/// Metadata:
/// - column: The column being assigned to.
/// Scalars:
/// - expr: The expression being assigned to this new column
impl ColumnAssign {
    pub fn new(column: Column, expr: Arc<Scalar>) -> Self {
        Self {
            meta: ColumnAssignMetadata { column },
            common: IRCommon::with_input_scalars_only(Arc::new([expr])),
        }
    }
}

impl Explain for ColumnAssignBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let column_meta = ctx.get_column_meta(self.column());
        let fmt = format!(
            "{}({}) := {}",
            &column_meta.name,
            self.column(),
            self.expr().explain(ctx, option).to_one_line_string(true)
        );
        Pretty::display(&fmt)
    }
}
