use super::Children;

/// A type representing a physical operator in an output physical query execution plan.
///
/// Each variant of `PhysicalOperator` represents a specific kind of logical operator. The current
/// supported operators are `TableScan`, `PhysicalFilter`, and `HashJoin`.
///
/// This type is generic over a `Link` type, which specifies what kind of children this operator is
/// allowed to have in whatever kind of plan it is contained in. This makes it possible to reuse the
/// `PhysicalOperator` type in differnt kinds of trees.
///
/// For example, `PhysicalOperator` _**is**_ a valid operator in [`PhysicalPlan`] and
/// [`PartialPhysicalPlan`], _**but it is not a valid operator in**_ [`LogicalPlan`] nor
/// [`PartialLogicalPlan`].
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PhysicalPlan`]: crate::plan::physical_plan::PhysicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
/// [`PartialPhysicalPlan`]: crate::plan::partial_physical_plan::PartialPhysicalPlan
pub enum PhysicalOperator<Link> {
    Scan(TableScanOperator<Link>),
    Filter(PhysicalFilterOperator<Link>),
    Join(HashJoinOperator<Link>),
}

/// TODO Add docs.
pub struct TableScanOperator<Link> {
    stuff: (),
    children: Children<Link>,
}

/// TODO Add docs.
pub struct PhysicalFilterOperator<Link> {
    stuff: (),
    children: Children<Link>,
}

/// TODO Add docs.
pub struct HashJoinOperator<Link> {
    stuff: (),
    children: Children<Link>,
}
