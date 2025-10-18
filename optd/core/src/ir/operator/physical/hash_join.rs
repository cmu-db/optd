use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    Column, IRCommon, Operator, Scalar,
    builder::column_ref,
    convert::IntoScalar,
    macros::{define_node, impl_operator_conversion},
    operator::join::JoinType,
    properties::OperatorProperties,
    scalar::NaryOp,
};

define_node!(
    PhysicalHashJoin, PhysicalHashJoinBorrowed {
        properties: OperatorProperties,
        metadata: PhysicalHashJoinMetadata {
            join_type: JoinType,
            keys: Arc<[(Column, Column)]>,
        },
        inputs: {
            operators: [build_side, probe_side],
            scalars: [non_equi_conds],
        }
    }
);

impl_operator_conversion!(PhysicalHashJoin, PhysicalHashJoinBorrowed);

impl PhysicalHashJoin {
    pub fn new(
        join_type: JoinType,
        build_side: Arc<Operator>,
        probe_side: Arc<Operator>,
        keys: Arc<[(Column, Column)]>,
        non_equi_conds: Arc<Scalar>,
    ) -> Self {
        Self {
            meta: PhysicalHashJoinMetadata { join_type, keys },
            common: IRCommon::new(
                Arc::new([build_side, probe_side]),
                Arc::new([non_equi_conds]),
            ),
        }
    }
}

impl crate::ir::explain::Explain for PhysicalHashJoinBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(3);
        fields.push((".join_type", Pretty::debug(self.join_type())));

        let terms = self
            .keys()
            .iter()
            .map(|(l, r)| column_ref(*l).eq(column_ref(*r)))
            .chain(std::iter::once(self.non_equi_conds().clone()))
            .collect();

        let join_cond = NaryOp::new(crate::ir::scalar::NaryOpKind::And, terms).into_scalar();
        fields.push((".join_conds", join_cond.explain(ctx, option)));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("PhysicalHashJoin", fields, children)
    }
}
