//! The subquery operator is used to represent subqueries in logical plans.

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
    /// Scalars: (none)
    LogicalSubquery, LogicalSubqueryBorrowed {
        properties: OperatorProperties,
        metadata: LogicalSubqueryMetadata {},
        inputs: {
            operators: [input],
            scalars: [],
        }
    }
);
impl_operator_conversion!(LogicalSubquery, LogicalSubqueryBorrowed);

impl LogicalSubquery {
    pub fn new(input: Arc<Operator>) -> Self {
        Self {
            meta: LogicalSubqueryMetadata {},
            common: IRCommon::with_input_operators_only(Arc::new([input])),
        }
    }
}

impl Explain for LogicalSubqueryBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::new();
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("LogicalSubquery", fields, children)
    }
}
