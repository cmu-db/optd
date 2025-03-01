//! A physical merge join operator.

use crate::{cascades::ir::OperatorData, operators::relational::physical::PhysicalOperator};
use serde::Deserialize;

/// A physical operator that performs a sort-merge join.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct MergeJoin<Value, Relation, Scalar> {
    /// The type of join.
    pub join_type: Value,
    /// Left sorted relation.
    pub left_sorted: Relation,
    /// Right sorted relation.
    pub right_sorted: Relation,
    /// The join condition.
    pub condition: Scalar,
}

impl<Relation, Scalar> MergeJoin<OperatorData, Relation, Scalar> {
    /// Create a new merge join operator.
    pub fn new(
        join_type: &str,
        left_sorted: Relation,
        right_sorted: Relation,
        condition: Scalar,
    ) -> Self {
        Self {
            join_type: OperatorData::String(join_type.into()),
            left_sorted,
            right_sorted,
            condition,
        }
    }
}

/// Creates a merge join physical operator.
pub fn merge_join<Relation, Scalar>(
    join_type: &str,
    left_sorted: Relation,
    right_sorted: Relation,
    condition: Scalar,
) -> PhysicalOperator<OperatorData, Relation, Scalar> {
    PhysicalOperator::SortMergeJoin(MergeJoin::new(
        join_type,
        left_sorted,
        right_sorted,
        condition,
    ))
}
