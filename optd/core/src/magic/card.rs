use crate::ir::{
    Operator, Scalar,
    operator::join::JoinType,
    operator::*,
    properties::{Cardinality, CardinalityEstimator},
    scalar::*,
};

pub struct MagicCardinalityEstimator;

impl MagicCardinalityEstimator {
    const MAGIC_JOIN_COND_SELECTIVITY: f64 = 0.4;
    const MAGIC_PREDICATE_SELECTIVITY: f64 = 0.1;
    const MAGIC_GROUP_BY_KEY_NDV_FACTOR: f64 = 0.2;
}

impl CardinalityEstimator for MagicCardinalityEstimator {
    fn estimate(
        &self,
        op: &crate::ir::Operator,
        ctx: &crate::ir::IRContext,
    ) -> crate::ir::properties::Cardinality {
        use crate::ir::OperatorKind;
        let join_selectivity = |join_cond: &Scalar| {
            if let Ok(literal) = join_cond.try_borrow::<Literal>() {
                match literal.value() {
                    crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                    crate::ir::ScalarValue::Boolean(_) => 0.,
                    _ => unreachable!("join condition must be boolean"),
                }
            } else {
                Self::MAGIC_JOIN_COND_SELECTIVITY
            }
        };
        let estimate_join =
            |join_type: &JoinType, outer: &Operator, inner: &Operator, join_cond: &Scalar| {
                let left_card = outer.cardinality(ctx);
                let right_card = inner.cardinality(ctx);
                match *join_type {
                    JoinType::Mark(_) => left_card,
                    JoinType::Single => {
                        let selectivity = join_selectivity(join_cond);
                        (selectivity * left_card * right_card)
                            .map(|value| value.min(left_card.as_f64()))
                    }
                    _ => {
                        let selectivity = join_selectivity(join_cond);
                        selectivity * left_card * right_card
                    }
                }
            };
        match &op.kind {
            OperatorKind::Group(_) => {
                // Relies on the normalized expression's cardinality estimation.
                panic!("right now should always be set");
            }
            OperatorKind::LogicalGet(meta) => {
                let exact_row_count = ctx.cat.describe_table(meta.source).row_count;
                Cardinality::with_count_lossy(exact_row_count)
            }
            OperatorKind::PhysicalTableScan(meta) => {
                let exact_row_count = ctx.cat.describe_table(meta.source).row_count;
                Cardinality::with_count_lossy(exact_row_count)
            }
            OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoin::borrow_raw_parts(meta, &op.common);
                estimate_join(
                    join.join_type(),
                    join.outer().as_ref(),
                    join.inner().as_ref(),
                    join.join_cond().as_ref(),
                )
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                let join = LogicalDependentJoin::borrow_raw_parts(meta, &op.common);
                estimate_join(
                    join.join_type(),
                    join.outer().as_ref(),
                    join.inner().as_ref(),
                    join.join_cond().as_ref(),
                )
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoin::borrow_raw_parts(meta, &op.common);
                let selectivity = if let Ok(literal) = join.join_cond().try_borrow::<Literal>() {
                    match literal.value() {
                        crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                        crate::ir::ScalarValue::Boolean(_) => 0.,
                        _ => unreachable!("join condition must be boolean"),
                    }
                } else {
                    Self::MAGIC_JOIN_COND_SELECTIVITY
                };
                let left_card = join.outer().cardinality(ctx);
                let right_card = join.inner().cardinality(ctx);
                selectivity * left_card * right_card
            }
            OperatorKind::PhysicalHashJoin(meta) => {
                let join = PhysicalHashJoin::borrow_raw_parts(meta, &op.common);
                let left_card = join.build_side().cardinality(ctx);
                let right_card = join.probe_side().cardinality(ctx);
                Self::MAGIC_JOIN_COND_SELECTIVITY * left_card * right_card
            }
            OperatorKind::LogicalSelect(meta) => {
                let filter = LogicalSelect::borrow_raw_parts(meta, &op.common);
                let selectivity = if let Ok(literal) = filter.predicate().try_borrow::<Literal>() {
                    match literal.value() {
                        crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                        crate::ir::ScalarValue::Boolean(_) => 0.,
                        _ => unreachable!("join condition must be boolean"),
                    }
                } else {
                    Self::MAGIC_PREDICATE_SELECTIVITY
                };

                selectivity * filter.input().cardinality(ctx)
            }
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilter::borrow_raw_parts(meta, &op.common);
                let selectivity = if let Ok(literal) = filter.predicate().try_borrow::<Literal>() {
                    match literal.value() {
                        crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                        crate::ir::ScalarValue::Boolean(_) => 0.,
                        _ => unreachable!("join condition must be boolean"),
                    }
                } else {
                    Self::MAGIC_PREDICATE_SELECTIVITY
                };

                selectivity * filter.input().cardinality(ctx)
            }
            OperatorKind::LogicalOrderBy(meta) => {
                LogicalOrderBy::borrow_raw_parts(meta, &op.common)
                    .input()
                    .cardinality(ctx)
            }
            OperatorKind::LogicalSubquery(meta) => {
                LogicalSubquery::borrow_raw_parts(meta, &op.common)
                    .input()
                    .cardinality(ctx)
            }
            OperatorKind::EnforcerSort(meta) => EnforcerSort::borrow_raw_parts(meta, &op.common)
                .input()
                .cardinality(ctx),
            OperatorKind::MockScan(meta) => meta.spec.mocked_card,
            OperatorKind::LogicalProject(meta) => {
                LogicalProject::borrow_raw_parts(meta, &op.common)
                    .input()
                    .cardinality(ctx)
            }
            OperatorKind::PhysicalProject(meta) => {
                PhysicalProject::borrow_raw_parts(meta, &op.common)
                    .input()
                    .cardinality(ctx)
            }
            OperatorKind::LogicalAggregate(meta) => {
                let agg = LogicalAggregate::borrow_raw_parts(meta, &op.common);
                let len = agg.keys().borrow::<List>().members().len();

                Cardinality::new(
                    Self::MAGIC_GROUP_BY_KEY_NDV_FACTOR.powi(i32::try_from(len).unwrap()),
                )
            }
            OperatorKind::PhysicalHashAggregate(meta) => {
                let agg = PhysicalHashAggregate::borrow_raw_parts(meta, &op.common);
                let len = agg.keys().borrow::<List>().members().len();

                Cardinality::new(
                    Self::MAGIC_GROUP_BY_KEY_NDV_FACTOR.powi(i32::try_from(len).unwrap()),
                )
            }
            OperatorKind::LogicalRemap(meta) => LogicalRemap::borrow_raw_parts(meta, &op.common)
                .input()
                .cardinality(ctx),
        }
    }
}
