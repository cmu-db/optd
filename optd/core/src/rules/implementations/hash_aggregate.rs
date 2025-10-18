use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{LogicalAggregate, PhysicalHashAggregate},
    rule::{OperatorPattern, Rule},
};

pub struct LogicalAggregateAsPhysicalHashAggregateRule {
    pattern: OperatorPattern,
}

impl Default for LogicalAggregateAsPhysicalHashAggregateRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalAggregateAsPhysicalHashAggregateRule {
    pub fn new() -> Self {
        let pattern = OperatorPattern::with_top_matches(|kind| {
            matches!(kind, OperatorKind::LogicalAggregate(_))
        });
        Self { pattern }
    }
}

impl Rule for LogicalAggregateAsPhysicalHashAggregateRule {
    fn name(&self) -> &'static str {
        "logical_aggregate_as_physical_hash_aggregate"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        _ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<Vec<std::sync::Arc<crate::ir::Operator>>> {
        let logical_agg = operator.try_borrow::<LogicalAggregate>().unwrap();
        let hash_agg = PhysicalHashAggregate::new(
            logical_agg.input().clone(),
            logical_agg.exprs().clone(),
            logical_agg.keys().clone(),
        );

        Ok(vec![hash_agg.into_operator()])
    }
}
