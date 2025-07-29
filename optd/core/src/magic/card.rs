use crate::ir::{
    operator::*,
    properties::{Cardinality, CardinalityEstimator, GetProperty},
    scalar::*,
};

pub struct MagicCardinalityEstimator;

impl MagicCardinalityEstimator {
    const MAGIC_JOIN_COND_SELECTIVITY: f64 = 0.4;
    // const MAGIC_PREDICATE_SELECTIVITY: Cardinality = Cardinality::with_single_value(0.1);
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
            OperatorKind::Group(meta) => {
                // Relies on the normalized expression's cardinality estimation.
                meta.normalized.get_property::<Cardinality>(ctx)
            }
            OperatorKind::LogicalGet(meta) => {
                let exact_row_count = ctx.cat.describe_table(meta.table_id).row_count;
                Cardinality::with_count_lossy(exact_row_count)
            }
            OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoin::from_raw_parts(meta.clone(), op.common.clone());
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
            OperatorKind::EnforcerSort(_) => {
                op.input_operators()[0].get_property::<Cardinality>(ctx)
            }
            OperatorKind::PhysicalTableScan(meta) => {
                let exact_row_count = ctx.cat.describe_table(meta.table_id).row_count;
                Cardinality::with_count_lossy(exact_row_count)
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoin::from_raw_parts(meta.clone(), op.common.clone());
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
            #[cfg(test)]
            OperatorKind::MockScan(meta) => meta.spec.mocked_card,
        }
    }
}
