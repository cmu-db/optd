use std::sync::Arc;

use crate::ir::{
    IRCommon, Operator, Scalar,
    macros::{define_node, impl_operator_conversion},
    operator::join::JoinType,
    properties::OperatorProperties,
};

define_node!(
    PhysicalNLJoin  {
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
impl_operator_conversion!(PhysicalNLJoin);

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
