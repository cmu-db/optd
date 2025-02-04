//! Type definitions of logical operators in `optd`.

pub mod filter;
pub mod join;
pub mod scan;

use filter::Filter;
use join::Join;
use scan::Scan;
use serde::Deserialize;

use crate::values::OptdValue;

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
pub enum LogicalOperator<Value, Relation, Scalar> {
    Scan(Scan<Value, Scalar>),
    Filter(Filter<Relation, Scalar>),
    Join(Join<Value, Relation, Scalar>),
}

/// The kind of logical operator.
#[derive(Debug, Clone, sqlx::Type)]
pub enum LogicalOperatorKind {
    Scan,
    Filter,
    Join,
}

/// Creates a scan logical operator.
pub fn scan<Relation, Scalar>(
    table_name: &str,
    predicate: Scalar,
) -> LogicalOperator<OptdValue, Relation, Scalar> {
    LogicalOperator::Scan(Scan::new(table_name, predicate))
}

/// Creates a filter logical operator.
pub fn filter<Relation, Scalar>(
    child: Relation,
    predicate: Scalar,
) -> LogicalOperator<OptdValue, Relation, Scalar> {
    LogicalOperator::Filter(Filter::new(child, predicate))
}

/// Creates a join logical operator.
pub fn join<Relation, Scalar>(
    join_type: &str,
    left: Relation,
    right: Relation,
    condition: Scalar,
) -> LogicalOperator<OptdValue, Relation, Scalar> {
    LogicalOperator::Join(Join::new(join_type, left, right, condition))
}

impl<Value, Relation, Scalar> LogicalOperator<Value, Relation, Scalar>
where
    Value: Clone,
    Relation: Clone,
    Scalar: Clone,
{
    pub fn children_relations(&self) -> Vec<Relation> {
        match self {
            LogicalOperator::Scan(_) => vec![],
            LogicalOperator::Filter(filter) => vec![filter.child.clone()],
            LogicalOperator::Join(join) => vec![join.left.clone(), join.right.clone()],
        }
    }

    pub fn children_scalars(&self) -> Vec<Scalar> {
        match self {
            LogicalOperator::Scan(scan) => vec![scan.predicate.clone()],
            LogicalOperator::Filter(filter) => vec![filter.predicate.clone()],
            LogicalOperator::Join(join) => vec![join.condition.clone()],
        }
    }
}
