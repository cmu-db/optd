//! Type definitions for scalar operators.
pub mod add;
pub mod column_ref;
pub mod constants;

use add::Add;
use column_ref::ColumnRef;
use constants::Constant;

/// Each variant of `ScalarOperator` represents a specific kind of scalar operator.
///
/// This type is generic over one type:
/// - `ScalarLink`: Specifies whether the children scalars are other scalar operators or a group id.
///
/// This makes it possible to reuse the `ScalarOperator` type in [`LogicalPlan`],
/// [`PhysicalPlan`], [`PartialLogicalPlan`], and [`ScalarExpr`].
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PhysicalPlan`]: crate::plan::physical_plan::PhysicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
/// [`ScalarExpr`]: crate::expression::scalar::ScalarExpr
#[derive(Clone)]
pub enum ScalarOperator<ScalarLink> {
    Add(Add<ScalarLink>),
    ColumnRef(ColumnRef),
    Constant(Constant),
}
