use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, Scalar,
    explain::Explain,
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

impl std::fmt::Display for BinaryOpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BinaryOpKind::Plus => "+",
            BinaryOpKind::Equal => "=",
        };
        write!(f, "{s}")
    }
}

impl BinaryOp {
    pub fn new(op_kind: BinaryOpKind, lhs: Arc<Scalar>, rhs: Arc<Scalar>) -> Self {
        Self {
            meta: BinaryOpMetadata { op_kind },
            common: IRCommon::with_input_scalars_only(Arc::new([lhs, rhs])),
        }
    }
}

impl Explain for BinaryOpBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let lhs = self.lhs().explain(ctx, option).to_one_line_string(true);
        let rhs = self.rhs().explain(ctx, option).to_one_line_string(true);
        Pretty::display(&format!("{lhs} {} {rhs}", self.op_kind()))
    }
}
