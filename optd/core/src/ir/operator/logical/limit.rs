//! The logical limit operator restricts the number of rows produced by its
//! input.

use crate::ir::{
    IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata: (none)
    /// Scalars:
    /// - skip: Number of rows to skip before fetch.
    /// - fetch: Maximum number of rows to fetch.
    LogicalLimit, LogicalLimitBorrowed {
        properties: OperatorProperties,
        metadata: LimitMetadata {},
        inputs: {
            operators: [input],
            scalars: [skip, fetch],
        }
    }
);
impl_operator_conversion!(LogicalLimit, LogicalLimitBorrowed);

impl LogicalLimit {
    pub fn new(input: Arc<Operator>, skip: Arc<Scalar>, fetch: Arc<Scalar>) -> Self {
        Self {
            meta: LimitMetadata {},
            common: IRCommon::new(Arc::new([input]), Arc::new([skip, fetch])),
        }
    }
}

impl Explain for LogicalLimitBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> Pretty<'a> {
        let mut fields = Vec::with_capacity(3);
        fields.push((".skip", self.skip().explain(ctx, option)));
        fields.push((".fetch", self.fetch().explain(ctx, option)));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("LogicalLimit", fields, children)
    }
}
