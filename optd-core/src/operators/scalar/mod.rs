//! Type definitions for scalar operators.

// For now, we can hold off on documenting stuff here until that is stabilized.
#![allow(missing_docs)]

pub mod add;
pub mod column_ref;
pub mod constants;
pub mod equal;

use add::Add;
use column_ref::ColumnRef;
use constants::Constant;
use equal::Equal;
use serde::Deserialize;

/// Each variant of `ScalarOperator` represents a specific kind of scalar operator.
///
/// This type is generic over one type:
/// - `Scalar`: Specifies whether the children scalars are other scalar operators or a group id.
///
/// This makes it possible to reuse the `ScalarOperator` type in [`LogicalPlan`],
/// [`PhysicalPlan`], and [`PartialLogicalPlan`].
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PhysicalPlan`]: crate::plan::physical_plan::PhysicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
/// TODO(Alexis): deprecated doc
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum ScalarOperator<Value, Scalar> {
    Constant(Constant<Value>),
    ColumnRef(ColumnRef<Value>),
    Add(Add<Scalar>),
    Equal(Equal<Scalar>),
}

/// The kind of scalar operator.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, sqlx::Type)]
pub enum ScalarOperatorKind {
    Constant,
    ColumnRef,
    Add,
    Equal,
}

impl<Value, Scalar> ScalarOperator<Value, Scalar> {
    pub fn operator_kind(&self) -> ScalarOperatorKind {
        match self {
            ScalarOperator::Constant(_) => ScalarOperatorKind::Constant,
            ScalarOperator::ColumnRef(_) => ScalarOperatorKind::ColumnRef,
            ScalarOperator::Add(_) => ScalarOperatorKind::Add,
            ScalarOperator::Equal(_) => ScalarOperatorKind::Equal,
        }
    }
}

impl<Value, Scalar> ScalarOperator<Value, Scalar>
where
    Value: Clone,
    Scalar: Clone,
{
    pub fn children_scalars(&self) -> Vec<Scalar> {
        match self {
            ScalarOperator::Constant(_) => vec![],
            ScalarOperator::ColumnRef(_) => vec![],
            ScalarOperator::Add(add) => vec![add.left.clone(), add.right.clone()],
            ScalarOperator::Equal(equal) => vec![equal.left.clone(), equal.right.clone()],
        }
    }

    pub fn values(&self) -> Vec<Value> {
        match self {
            ScalarOperator::Constant(constant) => vec![constant.value.clone()],
            ScalarOperator::ColumnRef(column_ref) => vec![column_ref.column_index.clone()],
            ScalarOperator::Add(_) => vec![],
            ScalarOperator::Equal(_) => vec![],
        }
    }
}
