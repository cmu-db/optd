//! Type definitions of logical operators in `optd`.

pub mod filter;
pub mod join;
pub mod project;
pub mod scan;

use filter::Filter;
use join::{Join, JoinType};
use project::Project;
use scan::Scan;
use serde::Deserialize;

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
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum LogicalOperator<Relation, Scalar> {
    Scan(Scan<Relation, Scalar>),
    Filter(Filter<Relation, Scalar>),
    Project(Project<Relation, Scalar>),
    Join(Join<Relation, Scalar>),
}

/// The kind of logical operator.
#[allow(missing_docs)]
#[derive(Debug, Clone, sqlx::Type)]
pub enum LogicalOperatorKind {
    Scan,
    Filter,
    // Project,
    Join,
}

impl<Relation, Scalar> LogicalOperator<Relation, Scalar> {
    /// Creates a scan logical operator.
    pub fn scan(table_name: &str, predicate: Scalar) -> Self {
        Self::Scan(Scan::new(table_name, predicate))
    }

    /// Creates a filter logical operator.
    pub fn filter(child: Relation, predicate: Scalar) -> Self {
        Self::Filter(Filter::new(child, predicate))
    }

    /// Creates a join logical operator.
    pub fn join(join_type: JoinType, left: Relation, right: Relation, condition: Scalar) -> Self {
        Self::Join(Join {
            join_type,
            left,
            right,
            condition,
        })
    }
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
