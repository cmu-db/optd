use crate::ir::{
    operator::*,
    properties::{Cardinality, CardinalityEstimator, GetProperty},
    scalar::*,
};

pub struct MagicCardinalityEstimator;

impl MagicCardinalityEstimator {
    const MAGIC_JOIN_COND_SELECTIVITY: f64 = 0.4;
    const MAGIC_PREDICATE_SELECTIVITY: f64 = 0.1;
    // const MAGIC_DISTRIBUTION: Cardinality = Cardinality::with_single_value(0.05);
}

impl CardinalityEstimator for MagicCardinalityEstimator {
    fn estimate(
        &self,
        op: &crate::ir::Operator,
        ctx: &crate::ir::IRContext,
    ) -> crate::ir::properties::Cardinality {
        use crate::ir::OperatorKind;
        match &op.kind {
            OperatorKind::Group(_) => {
                // Relies on the normalized expression's cardinality estimation.
                panic!("right now should always be set");
            }
            OperatorKind::LogicalGet(meta) => {
                let exact_row_count = ctx.cat.describe_table(meta.table_id).row_count;
                Cardinality::with_count_lossy(exact_row_count)
            }
            OperatorKind::PhysicalTableScan(meta) => {
                let exact_row_count = ctx.cat.describe_table(meta.table_id).row_count;
                Cardinality::with_count_lossy(exact_row_count)
            }
            OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoinBorrowed::from_raw_parts(meta, &op.common);
                let selectivity = if let Ok(literal) = join.join_cond().try_bind_ref::<Literal>() {
                    match literal.value() {
                        crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                        crate::ir::ScalarValue::Boolean(_) => 0.,
                        _ => unreachable!("join condition must be boolean"),
                    }
                } else {
                    MagicCardinalityEstimator::MAGIC_JOIN_COND_SELECTIVITY
                };
                let left_card = join.outer().get_property::<Cardinality>(ctx);
                let right_card = join.inner().get_property::<Cardinality>(ctx);
                selectivity * left_card * right_card
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoinBorrowed::from_raw_parts(meta, &op.common);
                let selectivity = if let Ok(literal) = join.join_cond().try_bind_ref::<Literal>() {
                    match literal.value() {
                        crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                        crate::ir::ScalarValue::Boolean(_) => 0.,
                        _ => unreachable!("join condition must be boolean"),
                    }
                } else {
                    MagicCardinalityEstimator::MAGIC_JOIN_COND_SELECTIVITY
                };
                let left_card = join.outer().get_property::<Cardinality>(ctx);
                let right_card = join.inner().get_property::<Cardinality>(ctx);
                selectivity * left_card * right_card
            }
            OperatorKind::LogicalSelect(meta) => {
                let filter = LogicalSelectBorrowed::from_raw_parts(meta, &op.common);
                let selectivity = if let Ok(literal) = filter.predicate().try_bind_ref::<Literal>()
                {
                    match literal.value() {
                        crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                        crate::ir::ScalarValue::Boolean(_) => 0.,
                        _ => unreachable!("join condition must be boolean"),
                    }
                } else {
                    Self::MAGIC_PREDICATE_SELECTIVITY
                };

                selectivity * op.input_operators()[0].get_property::<Cardinality>(ctx)
            }
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilterBorrowed::from_raw_parts(meta, &op.common);
                let selectivity = if let Ok(literal) = filter.predicate().try_bind_ref::<Literal>()
                {
                    match literal.value() {
                        crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                        crate::ir::ScalarValue::Boolean(_) => 0.,
                        _ => unreachable!("join condition must be boolean"),
                    }
                } else {
                    Self::MAGIC_PREDICATE_SELECTIVITY
                };

                selectivity * op.input_operators()[0].get_property::<Cardinality>(ctx)
            }
            OperatorKind::EnforcerSort(_) => {
                op.input_operators()[0].get_property::<Cardinality>(ctx)
            }
            #[cfg(test)]
            OperatorKind::MockScan(meta) => meta.spec.mocked_card,
        }
    }
}
