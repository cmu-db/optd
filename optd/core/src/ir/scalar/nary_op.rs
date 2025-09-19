use std::sync::Arc;

use itertools::Itertools;
use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    NaryOp, NaryOpBorrowed {
        properties: ScalarProperties,
        metadata: NaryOpMetadata {
            op_kind: NaryOpKind,
        },
        inputs: {
            operators: [],
            scalars: terms[],
        }
    }
);
impl_scalar_conversion!(NaryOp, NaryOpBorrowed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NaryOpKind {
    And,
    Or,
}

impl std::fmt::Display for NaryOpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            NaryOpKind::And => "AND",
            NaryOpKind::Or => "OR",
        };
        write!(f, "{s}")
    }
}

impl NaryOp {
    pub fn new(op_kind: NaryOpKind, terms: Arc<[Arc<Scalar>]>) -> Self {
        Self {
            meta: NaryOpMetadata { op_kind },
            common: IRCommon::with_input_scalars_only(terms),
        }
    }
}

impl NaryOpBorrowed<'_> {
    pub fn is_and(&self) -> bool {
        matches!(self.op_kind(), NaryOpKind::And)
    }

    pub fn is_or(&self) -> bool {
        matches!(self.op_kind(), NaryOpKind::Or)
    }
}

impl Explain for NaryOpBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let explained_terms = self
            .terms()
            .iter()
            .map(|t| format!("({})", t.explain(ctx, option).to_one_line_string(true)))
            .join(&format!(" {} ", self.op_kind()));

        Pretty::display(&explained_terms)
    }
}
