//! The logical remap operator remaps columns in a relation to new aliases.

use crate::ir::{
    IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    LogicalRemap, LogicalRemapBorrowed {
        properties: OperatorProperties,
        metadata: LogicalRemapMetadata {},
        inputs: {
            operators: [input],
            scalars: [mappings],
        },
    }
);
impl_operator_conversion!(LogicalRemap, LogicalRemapBorrowed);

/// Metadata: (none)
/// Scalars:
/// - mappings: A list defining the remapping of columns to new aliases.
impl LogicalRemap {
    pub fn new(input: Arc<Operator>, mappings: Arc<Scalar>) -> Self {
        Self {
            meta: LogicalRemapMetadata {},
            common: IRCommon::new(Arc::new([input]), Arc::new([mappings])),
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
        let projecions_explained = self.mappings().explain(ctx, option);
        fields.push((".mappings", projecions_explained));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("LogicalRemap", fields, children)
    }
}
