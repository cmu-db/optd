use super::*;
use crate::operator::relational::{
    logical::LogicalOperator,
    physical::{join::hash_join::HashJoin, PhysicalOperator},
};

/// Implementation rule that converts a logical join into a hash join physical operator
pub struct HashJoinRule;

// TODO: rule may fail, need to check join condition
// https://github.com/cmu-db/optd/issues/15
impl ImplementationRule for HashJoinRule {
    fn check_and_apply(&self, expr: LogicalExpression) -> Option<PhysicalExpression> {
        if let LogicalExpression::Relational(LogicalOperator::Join(join)) = expr {
            return Some(PhysicalExpression::Relational(PhysicalOperator::HashJoin(
                HashJoin {
                    join_type: join.join_type,
                    probe_side: join.left,
                    build_side: join.right,
                    condition: join.condition,
                },
            )));
        }
        None
    }
}
