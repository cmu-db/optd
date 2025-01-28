use super::*;

pub struct HashJoinRule;

impl ImplementationRule for HashJoinRule {
    async fn check_pattern(&self, expr: Expr, memo: &Memo) -> Vec<PartialPhysicalPlan> {
        todo!()
    }

    fn apply(&self, expr: PartialPhysicalPlan) -> Vec<Expr> {
        todo!()
    }
}
