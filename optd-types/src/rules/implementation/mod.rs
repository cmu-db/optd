use crate::{expression::Expr, memo::Memo, plan::partial_physical_plan::PartialPhysicalPlan};

#[allow(dead_code)]
#[trait_variant::make(Send)]
pub trait ImplementationRule {
    /// Checks if the implementation rule matches the current expression and its children.
    /// Returns a vector of partially materialized physical plans.
    ///
    /// This returns a vector because the rule matching the input root expression could have matched
    /// with multiple child expressions.
    ///
    /// For example, let's say the input expression is `Filter(G1)`, and the group G1 has
    /// two expressions `e1 = HashJoin(HashJoin(A, B), C)` and `e2 = HashJoin(A, HashJoin(B, C))`.
    ///
    /// If the rule wants to match against `Filter(HashJoin(?L, ?R))`, then this function will
    /// partially materialize two expressions `Filter(e1)` and `Filter(e2)`. It is then up to the
    /// memo table API to apply modifications to the partially materialized physical plans (for
    /// example, a pushing a filter predicate into the condition of the `HashJoin`).
    ///
    /// TODO: Ideally this should return a `Stream` instead of a fully materialized Vector.
    async fn check_pattern(&self, expr: Expr, memo: &Memo) -> Vec<PartialPhysicalPlan>;

    /// Applys modifications to a partially materialized physical plan.
    ///
    /// These changes can create new expressions (logical, physical, and scalar).
    fn apply(&self, expr: PartialPhysicalPlan) -> Vec<Expr>;
}

pub mod hash_join;
pub mod physical_filter;
pub mod table_scan;
