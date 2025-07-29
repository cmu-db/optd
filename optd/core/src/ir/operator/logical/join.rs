use std::sync::Arc;

use crate::ir::{
    IRCommon, Operator, Scalar,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};

define_node!(
    LogicalJoin, LogicalJoinBorrowed {
        properties: OperatorProperties,
        metadata: LogicalJoinMetadata {
            join_type: JoinType,
        },
        inputs: {
            operators: [outer, inner],
            scalars: [join_cond],
        }
    }
);
impl_operator_conversion!(LogicalJoin, LogicalJoinBorrowed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
}

impl LogicalJoin {
    pub fn new(
        join_type: JoinType,
        outer: Arc<Operator>,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
    ) -> Self {
        Self {
            meta: LogicalJoinMetadata { join_type },
            common: IRCommon::new(Arc::new([outer, inner]), Arc::new([join_cond])),
        }
    }
}
