use std::sync::Arc;

use crate::ir::{
    IRCommon, Scalar,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    BinaryOp, BinaryOpBorrowed {
        properties: ScalarProperties,
        metadata: BinaryOpMetadata {
            op_kind: BinaryOpKind,
        },
        inputs: {
            operators: [],
            scalars: [lhs, rhs],
        }
    }
);
impl_scalar_conversion!(BinaryOp, BinaryOpBorrowed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOpKind {
    Plus,
    Equal,
}

impl BinaryOp {
    pub fn new(op_kind: BinaryOpKind, lhs: Arc<Scalar>, rhs: Arc<Scalar>) -> Self {
        Self {
            meta: BinaryOpMetadata { op_kind },
            common: IRCommon::with_input_scalars_only(Arc::new([lhs, rhs])),
        }
    }
}
