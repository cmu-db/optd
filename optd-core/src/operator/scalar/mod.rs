//! Type definitions for scalar operators.

// For now, we can hold off on documenting stuff here until that is stabilized.
#![allow(missing_docs)]

pub mod add;
pub mod column_ref;
pub mod constants;

use add::Add;
use column_ref::ColumnRef;
use constants::Constant;

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
#[derive(Clone)]
pub enum ScalarOperator<Scalar> {
    Constant(Constant),
    ColumnRef(ColumnRef),
    Add(Add<Scalar>),
}

/// Trait for getting the children scalars of a scalar operator.
pub trait ScalarChildren {
    type Scalar;

    /// Get the children scalars of this scalar operator.
    fn children_scalars(&self) -> Vec<Self::Scalar>;
}
