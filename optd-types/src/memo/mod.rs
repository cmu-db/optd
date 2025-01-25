use crate::expression::{Expr, LogicalExpr};
use crate::{PartialLogicalPlan, PartialPhysicalPlan};

/// A type representing a transformation or implementation rule for query operators.
///
/// TODO The variants are just placeholders.
pub enum Rule {
    Transformation,
    Implementation,
}

pub struct Memo;

impl Memo {
    /// Checks if the transformation rule matches the current expression and any of it's children.
    /// Returns a vector of partially materialized plans.
    ///
    /// This returns a vector because the rule matching the input root expression could have matched
    /// with multiple child expressions.
    ///
    /// For example, let's say the input expression is `Filter(G1)`, and the group G1 has two
    /// expressions `e1 = Join(Join(A, B), C)` and `e2 = Join(A, Join(B, C))`.
    ///
    /// If the rule wants to match against `Filter(Join(?L, ?R))`, then this function will partially
    /// materialize two expressions `Filter(e1)` and `Filter(e2)`.
    pub async fn check_transformation(
        &self,
        expr: LogicalExpr,
        rule: Rule,
    ) -> Vec<PartialLogicalPlan> {
        todo!()
    }

    pub async fn check_implementation(
        &self,
        expr: LogicalExpr,
        rule: Rule,
    ) -> Vec<PartialPhysicalPlan> {
        todo!()
    }

    pub fn apply_transformation(
        &mut self,
        expr: PartialLogicalPlan,
        rule: Rule,
    ) -> Vec<Expr> {
        todo!()
    }

    pub fn apply_implementation(
        &mut self,
        expr: PartialPhysicalPlan,
        rule: Rule,
    ) -> Vec<Expr> {
        todo!()
    }

    pub async fn add_expressions(&mut self, new_exprs: Vec<Expr>) {
        todo!()
    }
}
