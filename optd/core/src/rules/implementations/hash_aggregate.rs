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
        builder::column_ref,
        operator::Aggregate,
        table_ref::TableRef,
        test_utils::{test_col, test_ctx_with_tables},
    };

    #[test]
    fn lowers_unimplemented_aggregate_to_hash_aggregate() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2)])?;
        let input = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let c0 = test_col(&ctx, "t1", "c0")?;
        let c1 = test_col(&ctx, "t1", "c1")?;
        let aggregate = input
            .with_ctx(&ctx)
            .logical_aggregate([column_ref(c1)], [column_ref(c0)])?
            .build();
        let logical_agg = aggregate.try_borrow::<Aggregate>().unwrap();
        let exprs = logical_agg.exprs().clone();
        let keys = logical_agg.keys().clone();

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
        Ok(())
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
