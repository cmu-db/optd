use std::{marker::PhantomData, sync::Arc};

/// A type representing a logical operator in an input logical query plan.
///
/// Each variant of `LogicalOperator` represents a specific kind of logical operator. The current
/// supported operators are `Scan`, `Filter`, and `Join`.
///
/// This type is generic over a `Link` type, which specifies what kind of children this operator is
/// allowed to have in whatever kind of plan it is contained in. This makes it possible to reuse the
/// `LogicalOperator` type in differnt kinds of trees.
///
/// For example, `LogicalOperator` is a valid operator in [`LogicalPlan`], [`PhysicalPlan`],
/// [`PartialLogicalPlan`], and [`PartialPhysicalPlan`].
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PhysicalPlan`]: crate::plan::physical_plan::PhysicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
/// [`PartialPhysicalPlan`]: crate::plan::partial_physical_plan::PartialPhysicalPlan
pub enum LogicalOperator<Link> {
    Scan(LogicalScanOperator<Link>),
    Filter(LogicalFilterOperator<Link>),
    Join(LogicalJoinOperator<Link>),
}

/// TODO Add docs.
pub struct LogicalScanOperator<Link> {
    stuff: (),
    _phantom: PhantomData<Link>,
}

/// TODO Add docs.
pub struct LogicalFilterOperator<Link> {
    child: Arc<Link>,
    predicate: Arc<Link>,
}

/// TODO Add docs.
pub struct LogicalJoinOperator<Link> {
    left: Arc<Link>,
    right: Arc<Link>,
    condition: Arc<Link>,
}
