use super::*;

pub struct JoinCommutativityRule;

impl TransformationRule for JoinCommutativityRule {
    async fn check_pattern(
        &self,
        _expr: LogicalExpression,
        _memo: &Memo,
    ) -> Vec<PartialLogicalPlan> {
        todo!()
    }

    fn apply(&self, _expr: PartialLogicalPlan) -> Vec<LogicalExpression> {
        todo!()
    }
}
