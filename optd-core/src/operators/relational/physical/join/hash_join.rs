use serde::Deserialize;

use crate::values::OptdValue;

/// Hash-based join operator that matches rows based on equality conditions.
///
/// Takes left and right input relations (`Relation`) and joins their rows using
/// a join condition (`Scalar`). Builds hash table from build side (right)
/// and probes with rows from probe side (left).
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct HashJoin<Value, Relation, Scalar> {
    pub join_type: Value,
    /// Left relation that probes hash table.
    pub probe_side: Relation,
    /// Right relation used to build hash table.
    pub build_side: Relation,
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
