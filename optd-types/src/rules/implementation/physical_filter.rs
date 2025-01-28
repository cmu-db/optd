use super::*;

pub struct PhysicalFilterRule;

impl ImplementationRule for PhysicalFilterRule {
    async fn check_pattern(&self, expr: Expr, memo: &Memo) -> Vec<PartialPhysicalPlan> {
        todo!()
    }

    fn apply(&self, expr: PartialPhysicalPlan) -> Vec<Expr> {
        todo!()
    }
}
