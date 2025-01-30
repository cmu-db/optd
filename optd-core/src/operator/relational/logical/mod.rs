//! Type definitions of logical operators in `optd`.

pub mod filter;
pub mod join;
pub mod project;
pub mod scan;

use filter::Filter;
use join::Join;
use project::Project;
use scan::Scan;

use super::RelationChildren;

/// Each variant of `LogicalOperator` represents a specific kind of logical operator.
///
/// This type is generic over two types:
/// - `Relation`: Specifies whether the children relations are other logical operators or a group id.
/// - `Scalar`: Specifies whether the children scalars are other scalar operators or a group id.
///
/// This makes it possible to reuse the `LogicalOperator` type in [`LogicalPlan`],
/// [`PartialLogicalPlan`], and [`LogicalExpression`].
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
/// [`LogicalExpression`]: crate::expression::LogicalExpression
#[derive(Clone)]
pub enum LogicalOperator<Relation, Scalar> {
    Scan(Scan<Relation, Scalar>),
    Filter(Filter<Relation, Scalar>),
    Project(Project<Relation, Scalar>),
    Join(Join<Relation, Scalar>),
}

impl<Relation, Scalar> RelationChildren for LogicalOperator<Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    type Relation = Relation;
    type Scalar = Scalar;

    fn children_relations(&self) -> Vec<Self::Relation> {
        match self {
            LogicalOperator::Scan(scan) => scan.children_relations(),
            LogicalOperator::Filter(filter) => filter.children_relations(),
            LogicalOperator::Project(project) => project.children_relations(),
            LogicalOperator::Join(join) => join.children_relations(),
        }
    }

    fn children_scalars(&self) -> Vec<Self::Scalar> {
        match self {
            LogicalOperator::Scan(scan) => scan.children_scalars(),
            LogicalOperator::Filter(filter) => filter.children_scalars(),
            LogicalOperator::Project(project) => project.children_scalars(),
            LogicalOperator::Join(join) => join.children_scalars(),
        }
    }
}
