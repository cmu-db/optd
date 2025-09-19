use std::sync::Arc;

use crate::ir::{
    convert::IntoOperator,
    operator::EnforcerSort,
    properties::TupleOrdering,
    rule::{OperatorPattern, Rule},
};

pub struct EnforceTupleOrderingRule {
    ordering: TupleOrdering,
    pattern: OperatorPattern,
}

impl Rule for EnforceTupleOrderingRule {
    fn name(&self) -> &'static str {
        "enforce_tuple_ordering"
    }

    fn pattern(&self) -> &crate::ir::rule::OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        _ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<Vec<Arc<crate::ir::Operator>>> {
        if self.ordering.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![
            EnforcerSort::new(self.ordering.clone(), Arc::new(operator.clone())).into_operator(),
        ])
    }
}

impl EnforceTupleOrderingRule {
    pub fn new(ordering: TupleOrdering) -> Self {
        let pattern = OperatorPattern::with_top_matches(|_| true);
        Self { ordering, pattern }
    }
}
