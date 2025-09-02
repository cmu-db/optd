use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};

// NOTE: We do not handle grouping set semantics now.
define_node!(
    PhysicalHashAggregate, PhysicalHashAggregateBorrowed {
        properties: OperatorProperties,
        metadata: PhysicalHashAggregateMetadata {},
        inputs: {
            operators: [input],
            scalars: [exprs, keys],
        }
    }
);
impl_operator_conversion!(PhysicalHashAggregate, PhysicalHashAggregateBorrowed);

impl PhysicalHashAggregate {
    pub fn new(input: Arc<Operator>, exprs: Arc<Scalar>, keys: Arc<Scalar>) -> Self {
        Self {
            meta: PhysicalHashAggregateMetadata {},
            common: IRCommon::new(Arc::new([input]), Arc::new([exprs, keys])),
        }
    }
}

impl Explain for PhysicalHashAggregateBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::new();
        let exprs = self.exprs().explain(ctx, option);
        let keys = self.keys().explain(ctx, option);
        fields.push((".exprs", exprs));
        fields.push((".keys", keys));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("PhysicalHashAggregate", fields, children)
    }
}
