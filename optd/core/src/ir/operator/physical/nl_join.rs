use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    operator::join::JoinType,
    properties::OperatorProperties,
};

define_node!(
    PhysicalNLJoin, PhysicalNLJoinBorrowed  {
        properties: OperatorProperties,
        metadata: PhysicalNLJoinJoinMetadata {
            join_type: JoinType,
        },
        inputs: {
            operators: [outer, inner],
            scalars: [join_cond],
        }
    }
);
impl_operator_conversion!(PhysicalNLJoin, PhysicalNLJoinBorrowed);

impl PhysicalNLJoin {
    pub fn new(
        join_type: JoinType,
        outer: Arc<Operator>,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
    ) -> Self {
        Self {
            meta: PhysicalNLJoinJoinMetadata { join_type },
            common: IRCommon::new(Arc::new([outer, inner]), Arc::new([join_cond])),
        }
    }
}

impl Explain for PhysicalNLJoinBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(3);
        fields.push((".join_type", Pretty::debug(self.join_type())));
        fields.push((".join_cond", self.join_cond().explain(ctx, option)));
        let children = self
            .common
            .input_operators
            .iter()
            .map(|input_op| input_op.explain(ctx, option))
            .collect();
        Pretty::simple_record("PhysicalNLJoin", fields, children)
    }
}
