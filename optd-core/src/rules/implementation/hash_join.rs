//! The rule for implementing `Join` as a `HashJoin`.
//!
//! See [`HashJoinRule`] for more information.

use super::*;
use crate::operator::relational::{
    logical::{join::Join, LogicalOperator},
    physical::{join::hash_join::HashJoin, PhysicalOperator},
};

/// A unit / marker struct for implementing `HashJoin`.
///
/// This implementation rule converts a logical `Join` into a physical `HashJoin` operator.
pub struct HashJoinRule;

// TODO: rule may fail, need to check join condition
// https://github.com/cmu-db/optd/issues/15
impl ImplementationRule for HashJoinRule {
    fn check_and_apply(&self, expr: LogicalExpression) -> Option<PhysicalExpression> {
        let LogicalOperator::Join(Join {
            join_type,
            left,
            right,
            condition,
        }) = expr
        else {
            return None;
        };

        Some(PhysicalOperator::HashJoin(HashJoin {
            join_type,
            probe_side: left,
            build_side: right,
            condition,
        }))
    }
}
