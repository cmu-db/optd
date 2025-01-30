//! The rule for join associativity.
//! 
//! See [`JoinAssociativityRule`] for more information.

use super::*;

/// A unit / marker struct for join associativity.
/// 
/// Since joining is an associative operation, we can convert a `Join(Join(A, B), C)` into a
/// `Join(A, Join(B, C))`.
pub struct JoinAssociativityRule;

impl TransformationRule for JoinAssociativityRule {
    async fn check_pattern(
        &self,
        _expr: LogicalExpression,
        _memo: &Memo,
    ) -> Vec<PartialLogicalPlan> {
        todo!()
    }

    fn apply(&self, _expr: PartialLogicalPlan) -> PartialLogicalPlan {
        todo!()
    }
}
