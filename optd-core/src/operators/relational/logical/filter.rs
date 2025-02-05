//! A logical filter.

use super::LogicalOperator;
use crate::values::OptdValue;
use serde::Deserialize;

/// Logical filter operator that selects rows matching a condition.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Filter<Relation, Scalar> {
    /// The input relation.
    pub child: Relation,
    /// The filter predicate condition.
    pub predicate: Scalar,
}

impl<Relation, Scalar> Filter<Relation, Scalar> {
    /// Create a new filter operator.
    pub fn new(child: Relation, predicate: Scalar) -> Self {
        Self { child, predicate }
    }
}

/// Creates a filter logical operator.
pub fn filter<Relation, Scalar>(
    child: Relation,
    predicate: Scalar,
) -> LogicalOperator<OptdValue, Relation, Scalar> {
    LogicalOperator::Filter(Filter::new(child, predicate))
}
