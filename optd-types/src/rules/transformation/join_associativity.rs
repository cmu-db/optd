use super::*;

pub struct JoinAssociativityRule;

impl TransformationRule for JoinAssociativityRule {
    async fn check_pattern(&self, expr: LogicalExpr, memo: &Memo) -> Vec<PartialLogicalPlan> {
        todo!()
    }

    fn apply(&self, expr: PartialLogicalPlan) -> Vec<Expr> {
        todo!()
    }
}
