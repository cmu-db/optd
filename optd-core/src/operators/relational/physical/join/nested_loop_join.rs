//! A physical nested loop join operator.

use crate::{operators::relational::physical::PhysicalOperator, values::OptdValue};
use serde::Deserialize;

/// A physical operator that performs a nested loop join.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct NestedLoopJoin<Value, Relation, Scalar> {
    /// The type of join.
    pub join_type: Value,
    /// The outer relation.
    pub outer: Relation,
    /// The inner relation.
    pub inner: Relation,
    /// The join condition.
    pub condition: Scalar,
}

impl<Relation, Scalar> NestedLoopJoin<OptdValue, Relation, Scalar> {
    /// Create a new nested loop join operator.
    pub fn new(join_type: &str, outer: Relation, inner: Relation, condition: Scalar) -> Self {
        Self {
            join_type: OptdValue::String(join_type.into()),
            outer,
            inner,
            condition,
        }
    }
}

/// Creates a nested loop join physical operator.
pub fn nested_loop_join<Relation, Scalar>(
    join_type: &str,
    outer: Relation,
    inner: Relation,
    condition: Scalar,
) -> PhysicalOperator<OptdValue, Relation, Scalar> {
    PhysicalOperator::NestedLoopJoin(NestedLoopJoin::new(join_type, outer, inner, condition))
}
