mod assign;
mod binary_op;
mod cast;
mod column_ref;
mod function;
mod list;
mod literal;
mod nary_op;

use std::sync::Arc;

pub use assign::*;
pub use binary_op::*;
pub use cast::*;
pub use column_ref::*;
pub use function::*;
pub use list::*;
pub use literal::*;
pub use nary_op::*;

use crate::ir::{ColumnSet, IRCommon, Operator, explain::Explain, properties::ScalarProperties};

/// The scalar type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarKind {
    Literal(LiteralMetadata),
    ColumnRef(ColumnRefMetadata),
    ColumnAssign(ColumnAssignMetadata),
    BinaryOp(BinaryOpMetadata),
    NaryOp(NaryOpMetadata),
    List(ListMetadata),
    Function(FunctionMetadata),
    Cast(CastMetadata),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Scalar {
    pub kind: ScalarKind,
    pub common: IRCommon<ScalarProperties>,
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
            ScalarKind::Literal(_) => ColumnSet::default(),
            ScalarKind::ColumnRef(meta) => ColumnSet::from_iter(std::iter::once(meta.column)),
            ScalarKind::BinaryOp(_)
            | ScalarKind::NaryOp(_)
            | ScalarKind::ColumnAssign(_)
            | ScalarKind::List(_)
            | ScalarKind::Cast(_)
            | ScalarKind::Function(_) => self.input_scalars().iter().fold(
                ColumnSet::default(),
                |mut used_columns, scalar| {
                    used_columns |= &scalar.used_columns();
                    used_columns
                },
            ),
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
            ScalarKind::BinaryOp(meta) => {
                BinaryOp::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::NaryOp(meta) => {
                NaryOp::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::ColumnAssign(meta) => {
                ColumnAssign::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::List(meta) => {
                List::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Function(meta) => {
                Function::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Cast(meta) => {
                Cast::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
        }
    }
}
