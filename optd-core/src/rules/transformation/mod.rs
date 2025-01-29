//! This module contains the transformation rule trait / API, as well as the rules that implement
//! said trait.
//!
//! TODO(connor) Add more docs.

use crate::{
    expression::LogicalExpression, memo::Memo, plan::partial_logical_plan::PartialLogicalPlan,
};

#[trait_variant::make(Send)]
#[allow(dead_code)]
pub trait TransformationRule {
    /// Checks if the transformation rule matches the current expression and its childrenm, and
    /// returns a vector of partially materialized logical plans.
    ///
    /// This returns a vector because the rule matching the input root expression could have matched
    /// with multiple child expressions.
    ///
    /// For example, let's say the input expression is `Filter(G1)`, and the group G1 has two
    /// expressions `e1 = Join(Join(A, B), C)` and `e2 = Join(A, Join(B, C))`.
    ///
    /// If the rule wants to match against `Filter(Join(?L, ?R))`, then this function will partially
    /// materialize two expressions `Filter(e1)` and `Filter(e2)`. It is then up to the memo table
    /// API to apply modifications to the partially materialized logical plans (for example, a
    /// filter pushdown under a `Join`).
    ///
    /// TODO: Ideally this should return a `Stream` instead of a fully materialized Vector.
    async fn check_pattern(&self, expr: LogicalExpression, memo: &Memo) -> Vec<PartialLogicalPlan>;

    /// Applies modifications to a partially materialized logical plan.
    ///
    /// These changes can create new logical or scalar expressions. However, note that
    /// transformation rules will _not_ create new physical expressions.
    fn apply(&self, expr: PartialLogicalPlan) -> PartialLogicalPlan;
}

pub mod join_associativity;
pub mod join_commutativity;
