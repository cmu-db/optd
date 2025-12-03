use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};

define_node!(
    LogicalLimit, LogicalLimitBorrowed {
        properties: OperatorProperties,
        metadata: LogicalLimitMetadata {
            limit: usize,
        },
        inputs: {
            operators: [input],
            scalars: [],
        }
    }
);
impl_operator_conversion!(LogicalLimit, LogicalLimitBorrowed);

impl LogicalLimit {
    pub fn new(limit: usize, input: Arc<Operator>) -> Self {
        Self {
            meta: LogicalLimitMetadata { limit },
            common: IRCommon::new(Arc::new([input]), Arc::new([])),
        }
    }
}

impl Explain for LogicalLimitBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(1);
        fields.push((".limit", Pretty::debug(self.limit())));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("LogicalLimit", fields, children)
    }
}
