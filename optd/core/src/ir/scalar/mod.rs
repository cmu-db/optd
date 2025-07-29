mod assign;
mod binary_op;
mod column_ref;
mod literal;
mod projection_list;

use std::sync::Arc;

pub use assign::*;
pub use binary_op::*;
pub use column_ref::*;
pub use literal::*;

pub use projection_list::*;

use crate::ir::{IRCommon, Operator, ScalarGroupMetadata, properties::ScalarProperties};

/// The scalar type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarKind {
    Group(ScalarGroupMetadata),
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
}
