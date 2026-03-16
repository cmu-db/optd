use crate::ir::{
    OperatorKind,
    convert::IntoOperator,
    operator::{Aggregate, AggregateImplementation},
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
        let pattern = OperatorPattern::with_top_matches(
            |kind| matches!(kind, OperatorKind::Aggregate(meta) if meta.implementation.is_none()),
        );
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
        let logical_agg = operator.try_borrow::<Aggregate>().unwrap();
        let hash_agg = Aggregate::new(
            *logical_agg.aggregate_table_index(),
            logical_agg.input().clone(),
            logical_agg.exprs().clone(),
            logical_agg.keys().clone(),
            Some(AggregateImplementation::Hash),
        );

        Ok(vec![hash_agg.into_operator()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{
        IRContext,
        builder::column_ref,
        convert::{IntoOperator, IntoScalar},
        operator::Aggregate,
        scalar::List,
    };

    #[test]
    fn lowers_unimplemented_aggregate_to_hash_aggregate() {
        let ctx = IRContext::with_empty_magic();
        let input = ctx.mock_scan(1, 2, 10.);
        let exprs = List::new(vec![column_ref(crate::ir::Column(1, 1))].into()).into_scalar();
        let keys = List::new(vec![column_ref(crate::ir::Column(1, 0))].into()).into_scalar();
        let aggregate = Aggregate::new(2, input, exprs.clone(), keys.clone(), None).into_operator();

        let rule = LogicalAggregateAsPhysicalHashAggregateRule::new();
        let transformed = rule.transform(aggregate.as_ref(), &ctx).unwrap();

        assert_eq!(transformed.len(), 1);
        let lowered = transformed[0].try_borrow::<Aggregate>().unwrap();
        assert_eq!(
            lowered.implementation(),
            &Some(AggregateImplementation::Hash)
        );
        assert_eq!(lowered.exprs(), &exprs);
        assert_eq!(lowered.keys(), &keys);
    }

    #[test]
    fn does_not_match_already_lowered_aggregate() {
        let rule = LogicalAggregateAsPhysicalHashAggregateRule::new();
        assert!(!rule.pattern().top_matches(&OperatorKind::Aggregate(
            crate::ir::operator::AggregateMetadata {
                aggregate_table_index: 1,
                implementation: Some(AggregateImplementation::Hash),
            }
        )));
    }
}
