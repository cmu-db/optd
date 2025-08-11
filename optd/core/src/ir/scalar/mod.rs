mod assign;
mod binary_op;
mod column_ref;
mod literal;
mod projection_list;

use std::{collections::HashSet, sync::Arc};

pub use assign::*;
pub use binary_op::*;
pub use column_ref::*;
pub use literal::*;

pub use projection_list::*;

use crate::ir::{ColumnSet, IRCommon, Operator, explain::Explain, properties::ScalarProperties};

/// The scalar type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarKind {
    Literal(LiteralMetadata),
    ColumnRef(ColumnRefMetadata),
    Assign(AssignMetadata),
    ProjectionList(ProjectionListMetadata),
    BinaryOp(BinaryOpMetadata),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Scalar {
    pub kind: ScalarKind,
    pub(crate) common: IRCommon<ScalarProperties>,
}

impl Scalar {
    /// Gets the slice to the input operators.
    pub fn input_operators(&self) -> &[Arc<Operator>] {
        &self.common.input_operators
    }

    /// Gets the slice to the input scalar expressions.
    pub fn input_scalars(&self) -> &[Arc<Scalar>] {
        &self.common.input_scalars
    }

    /// Clones the operator, optionally replacing the input operators and the input scalar expressions.
    pub fn clone_with_inputs(
        &self,
        scalars: Option<Arc<[Arc<Scalar>]>>,
        operators: Option<Arc<[Arc<Operator>]>>,
    ) -> Self {
        let operators = operators.unwrap_or_else(|| self.common.input_operators.clone());
        let scalars = scalars.unwrap_or_else(|| self.common.input_scalars.clone());
        Self {
            kind: self.kind.clone(),
            common: IRCommon::new(operators, scalars),
        }
    }

    pub fn used_columns(&self) -> ColumnSet {
        match &self.kind {
            ScalarKind::Literal(_) => HashSet::new(),
            ScalarKind::ColumnRef(meta) => HashSet::from_iter(std::iter::once(meta.column)),
            ScalarKind::BinaryOp(_) => self
                .input_scalars()
                .iter()
                .fold(HashSet::new(), |x, y| &x & &y.used_columns()),
            ScalarKind::Assign(_) => todo!(),
            ScalarKind::ProjectionList(_) => todo!(),
        }
    }
}

impl Explain for Scalar {
    fn explain<'a>(
        &self,
        ctx: &super::IRContext,
        option: &super::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        match &self.kind {
            ScalarKind::Literal(meta) => {
                Literal::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::ColumnRef(meta) => {
                ColumnRef::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Assign(_) => todo!(),
            ScalarKind::ProjectionList(_) => todo!(),
            ScalarKind::BinaryOp(meta) => {
                BinaryOp::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
        }
    }
}
