use super::*;

pub struct JoinCommutativityRule;

impl TransformationRule for JoinCommutativityRule {
    async fn check_pattern(&self, expr: LogicalExpr, memo: &Memo) -> Vec<PartialLogicalPlan> {
        todo!()
    }

    fn apply(&self, expr: PartialLogicalPlan) -> Vec<Expr> {
        todo!()
    }
}
