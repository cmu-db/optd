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
    Minus,
    Multiply,
    Divide,
    Modulo,
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl std::fmt::Display for BinaryOpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BinaryOpKind::Plus => "+",
            BinaryOpKind::Minus => "-",
            BinaryOpKind::Multiply => "*",
            BinaryOpKind::Divide => "/",
            BinaryOpKind::Modulo => "%",
            BinaryOpKind::Eq => "=",
            BinaryOpKind::Lt => "<",
            BinaryOpKind::Le => "<=",
            BinaryOpKind::Gt => ">",
            BinaryOpKind::Ge => ">=",
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

impl BinaryOpBorrowed<'_> {
    pub fn is_plus(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Plus)
    }

    pub fn is_minus(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Minus)
    }

    pub fn is_multiply(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Multiply)
    }

    pub fn is_divide(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Divide)
    }

    pub fn is_modulo(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Modulo)
    }

    pub fn is_eq(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Eq)
    }

    pub fn is_lt(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Lt)
    }

    pub fn is_le(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Le)
    }

    pub fn is_gt(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Gt)
    }

    pub fn is_ge(&self) -> bool {
        matches!(self.op_kind(), BinaryOpKind::Ge)
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
