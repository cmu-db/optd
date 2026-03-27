//! Represents a SQL IN scalar operation.

use crate::ir::{
    IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - negated: Whether the IN operation is negated (NOT IN).
    /// Scalars:
    /// - expr: The expression to be tested for membership.
    /// - list: The scalar list expression to test against.
    InList, InListBorrowed {
        properties: ScalarProperties,
        metadata: InListMetadata {
            negated: bool,
        },
        inputs: {
            operators: [],
            scalars: [expr, list],
        }
    }
);
impl_scalar_conversion!(InList, InListBorrowed);

impl InList {
    pub fn new(expr: Arc<Scalar>, list: Arc<Scalar>, negated: bool) -> Self {
        Self {
            meta: InListMetadata { negated },
            common: IRCommon::with_input_scalars_only(Arc::new([expr, list])),
        }
    }
}

impl Explain for InListBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> Pretty<'a> {
        let expr = self.expr().explain(ctx, option).to_one_line_string(true);
        let list = self.list().explain(ctx, option).to_one_line_string(true);
        let operator = if *self.negated() { "NOT IN" } else { "IN" };
        Pretty::display(&format!("{expr} {operator} {list}"))
    }
}
