//! The logical select operator filters incoming data based on some predicate.

use crate::ir::{
    IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    LogicalSelect, LogicalSelectBorrowed {
        properties: OperatorProperties,
        metadata: LogicalSelectMetadata {},
        inputs: {
            operators: [input],
            scalars: [predicate],
        }
    }
);
impl_operator_conversion!(LogicalSelect, LogicalSelectBorrowed);

/// Metadata: (none)
/// Scalars:
/// - predicate: The predicate to filter rows by.
impl LogicalSelect {
    pub fn new(input: Arc<Operator>, predicate: Arc<Scalar>) -> Self {
        Self {
            meta: LogicalSelectMetadata {},
            common: IRCommon::new(Arc::new([input]), Arc::new([predicate])),
        }
    }
}

impl Explain for LogicalSelectBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(3);
        fields.push((".predicate", self.predicate().explain(ctx, option)));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("LogicalSelect", fields, children)
    }
}
