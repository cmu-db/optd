use super::*;

pub struct TableScanRule;

impl ImplementationRule for TableScanRule {
    async fn check_pattern(&self, expr: Expr, memo: &Memo) -> Vec<PartialPhysicalPlan> {
        todo!()
    }

    fn apply(&self, expr: PartialPhysicalPlan) -> Vec<Expr> {
        todo!()
    }
}
