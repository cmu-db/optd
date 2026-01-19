//! The aggregate operator groups rows by a set of keys and applies an aggregate
//! function to each group, using hashing as the aggregation strategy -
//! implementing the logical aggregate operator
//!
//! UNIMPLEMENTED: Currently, grouping set semantics are not handled.

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
    /// - exprs: The aggregate expressions to compute.
    /// - keys: The grouping keys.
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
