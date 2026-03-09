//! The logical remap operator remaps columns in a relation to new aliases.

use crate::ir::{
    IRCommon, Operator,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata: (none)
    /// Scalars:
    /// - mappings: A list defining the remapping of columns to new aliases.
    LogicalRemap, LogicalRemapBorrowed {
        properties: OperatorProperties,
        metadata: LogicalRemapMetadata {
            table_index: i64,
        },
        inputs: {
            operators: [input],
            scalars: [],
        },
    }
);
impl_operator_conversion!(LogicalRemap, LogicalRemapBorrowed);

impl LogicalRemap {
    pub fn new(table_index: i64, input: Arc<Operator>) -> Self {
        Self {
            meta: LogicalRemapMetadata { table_index },
            common: IRCommon::with_input_operators_only(Arc::new([input])),
        }
    }
}

impl Explain for LogicalRemapBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::new();
        fields.push((".table_index", Pretty::display(&self.table_index())));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("LogicalRemap", fields, children)
    }
}
