use crate::ir::{
    OperatorKind,
    operator::LogicalSelect,
    rule::{OperatorPattern, Rule},
};

pub struct LogicalSelectAsPhysicalFilterRule {
    pattern: OperatorPattern,
}

impl Default for LogicalSelectAsPhysicalFilterRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalSelectAsPhysicalFilterRule {
    pub fn new() -> Self {
        let pattern = OperatorPattern::with_top_matches(|kind| {
            matches!(kind, OperatorKind::LogicalSelect(_))
        });
        Self { pattern }
    }
}

impl Rule for LogicalSelectAsPhysicalFilterRule {
    fn name(&self) -> &'static str {
        "logical_select_as_physical_filter"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        _ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<Vec<std::sync::Arc<crate::ir::Operator>>> {
        let select = operator.try_borrow::<LogicalSelect>().unwrap();
        Ok(vec![
            select
                .input()
                .clone()
                .physical_filter(select.predicate().clone()),
        ])
    }
}
