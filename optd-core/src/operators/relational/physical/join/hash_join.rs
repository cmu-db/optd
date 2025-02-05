//! A physical hash join operator.

use crate::{operators::relational::physical::PhysicalOperator, values::OptdValue};
use serde::Deserialize;

/// A physical operator that performs a hash-based join.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct HashJoin<Value, Relation, Scalar> {
    /// The type of join.
    pub join_type: Value,
    /// Left relation that probes hash table.
    pub probe_side: Relation,
    /// Right relation used to build hash table.
    pub build_side: Relation,
    /// The join condition.
    pub condition: Scalar,
}

impl<Relation, Scalar> HashJoin<OptdValue, Relation, Scalar> {
    /// Create a new hash join operator.
    pub fn new(
        join_type: &str,
        probe_side: Relation,
        build_side: Relation,
        condition: Scalar,
    ) -> Self {
        Self {
            join_type: OptdValue::String(join_type.into()),
            probe_side,
            build_side,
            condition,
        }
    }
}

/// Creates a hash join physical operator.
pub fn hash_join<Relation, Scalar>(
    join_type: &str,
    probe_side: Relation,
    build_side: Relation,
    condition: Scalar,
) -> PhysicalOperator<OptdValue, Relation, Scalar> {
    PhysicalOperator::HashJoin(HashJoin::new(join_type, probe_side, build_side, condition))
}
