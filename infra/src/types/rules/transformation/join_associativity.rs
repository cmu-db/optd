use super::*;

pub struct JoinAssociativityRule;

impl TransformationRule for JoinAssociativityRule {
    async fn check_pattern(
        &self,
        _expr: LogicalExpression,
        _memo: &Memo,
    ) -> Vec<PartialLogicalPlan> {
        todo!()
    }

    fn apply(&self, _expr: PartialLogicalPlan) -> PartialLogicalPlan {
        todo!()
    }
}
