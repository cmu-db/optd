//! The rule for join commutativity.
//!
//! See [`JoinCommutativityRule`] for more information.

use super::*;

/// A unit / marker struct for join commutativity.
///
/// Since joining is an commutative operation, we can convert a `Join(A, B)` into a `Join(B, C)`.
pub struct JoinCommutativityRule;

impl TransformationRule for JoinCommutativityRule {
    async fn check_pattern(
        &self,
        _expr: LogicalExpression,
        _memo: &impl Memoize,
    ) -> Vec<PartialLogicalPlan> {
        todo!()
    }

    fn apply(&self, _expr: PartialLogicalPlan) -> PartialLogicalPlan {
        todo!()
    }
}
