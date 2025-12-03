use std::{collections::HashMap, hash::Hash, sync::Arc};

use crate::ir::{
    Column, ScalarValue,
    operator::*,
    properties::{Cardinality, CardinalityEstimator, ColumnRestrictions, GetProperty},
    scalar::*,
    statistics::{ColumnStatistics, Distribution},
};

pub struct MagicCardinalityEstimator;

fn extract_filter_predicate(
    expr: &Arc<crate::ir::Scalar>,
    predicates: &mut HashMap<Column, Vec<Arc<Scalar>>>,
) {
    if let Ok(binary_op) = expr.try_borrow::<BinaryOp>() {
        if binary_op.is_comparison() {
            let lhs = binary_op.lhs();
            let rhs = binary_op.rhs();
            match (lhs.try_borrow::<ColumnRef>(), rhs.try_borrow::<ColumnRef>()) {
                (Ok(col_ref), Err(_)) => {
                    predicates
                        .entry(col_ref.column().clone())
                        .or_default()
                        .push(expr.clone());
                }
                (Err(_), Ok(col_ref)) => {
                    predicates
                        .entry(col_ref.column().clone())
                        .or_default()
                        .push(expr.clone());
                }
                _ => {}
            }
        }
    }
    if let Ok(nary_op) = expr.try_borrow::<NaryOp>() {
        if nary_op.is_and() {
            nary_op.terms().iter().for_each(|term| {
                extract_filter_predicate(term, predicates);
            });
        }
    }
}

fn estimate_le_selecitivity(stats: &ColumnStatistics, value: &ScalarValue) -> f64 {
    let hist_leq_freq = stats.histogram.cdf(value);
    let (mcv_count, _) = stats.mcvs.count_with(|x| x <= value);
    let mcv_freq = mcv_count as f64 / stats.count as f64;
    hist_leq_freq + mcv_freq
}

fn estimate_eq_selecitivty(stats: &ColumnStatistics, value: &ScalarValue) -> f64 {
    stats.frequency(value)
}

fn extract_join_keys(
    join: &LogicalJoinBorrowed<'_>,
    left_keys: &mut Vec<Column>,
    right_keys: &mut Vec<Column>,
    ctx: &crate::ir::IRContext,
) {
    if let Ok(binary_op) = join.join_cond().try_borrow::<BinaryOp>() {
        if binary_op.is_eq() {
            let lhs = binary_op.lhs();
            let rhs = binary_op.rhs();
            match (lhs.try_borrow::<ColumnRef>(), rhs.try_borrow::<ColumnRef>()) {
                (Ok(left_col_ref), Ok(right_col_ref)) => {
                    if join
                        .outer()
                        .output_columns(ctx)
                        .contains(left_col_ref.column())
                        && join
                            .inner()
                            .output_columns(ctx)
                            .contains(right_col_ref.column())
                    {
                        left_keys.push(left_col_ref.column().clone());
                        right_keys.push(right_col_ref.column().clone());
                    } else if join
                        .outer()
                        .output_columns(ctx)
                        .contains(right_col_ref.column())
                        && join
                            .inner()
                            .output_columns(ctx)
                            .contains(left_col_ref.column())
                    {
                        left_keys.push(right_col_ref.column().clone());
                        right_keys.push(left_col_ref.column().clone());
                    }
                }
                _ => {}
            }
        }
    }
}

fn estimate_atom_selectivity(
    expr: &Arc<crate::ir::Scalar>,
    ctx: &crate::ir::IRContext,
) -> Option<f64> {
    // println!("estimating selectivity for expr: {:?}", expr);
    let binary_op = expr.borrow::<BinaryOp>();
    let lhs = binary_op.lhs().borrow::<ColumnRef>();
    let rhs = binary_op.rhs().borrow::<Literal>();
    let stats = ctx.get_column_stats(lhs.column())?;
    let value = rhs.value();
    if binary_op.is_eq() {
        return Some(estimate_eq_selecitivty(&stats, value));
    } else if binary_op.is_le() {
        return Some(estimate_le_selecitivity(&stats, value));
    } else if binary_op.is_lt() {
        // P(X < v) = P(X <= v) - P(X = v)
        let le_freq = estimate_le_selecitivity(&stats, value);
        let eq_freq = estimate_eq_selecitivty(&stats, value);
        return Some(le_freq - eq_freq);
    } else if binary_op.is_ge() {
        // P(X >= v) = 1 - P(X < v) = 1 - (P(X <= v) - P(X = v))
        let le_freq = estimate_le_selecitivity(&stats, value);
        let eq_freq = estimate_eq_selecitivty(&stats, value);
        return Some(1.0 - (le_freq - eq_freq));
    } else if binary_op.is_gt() {
        // P(X > v) = 1 - P(X <= v)
        let le_freq = estimate_le_selecitivity(&stats, value);
        return Some(1.0 - le_freq);
    }
    None
}

impl MagicCardinalityEstimator {
    const MAGIC_JOIN_COND_SELECTIVITY: f64 = 0.4;
    const MAGIC_PREDICATE_SELECTIVITY: f64 = 0.2;
    const MAGIC_GROUP_BY_KEY_NDV_FACTOR: f64 = 0.2;

    pub fn estimate_selectivity(
        &self,
        op: &crate::ir::Operator,
        ctx: &crate::ir::IRContext,
    ) -> f64 {
        match &op.kind {
            crate::ir::OperatorKind::LogicalSelect(meta) => {
                let filter = LogicalSelect::borrow_raw_parts(&meta, &op.common);
                let mut predicates = HashMap::new();
                extract_filter_predicate(filter.predicate(), &mut predicates);
                predicates
                    .iter()
                    .map(|(column, restrictions)| {
                        let Some(stats) = ctx.get_column_stats(column) else {
                            // println!(
                            //     "No statistics for column {:?}, use default selectivity",
                            //     column
                            // );
                            return Self::MAGIC_PREDICATE_SELECTIVITY;
                        };
                        restrictions.iter().fold(1.0, |acc, r| {
                            acc * {
                                let result = if let Some(sel) = estimate_atom_selectivity(r, ctx) {
                                    sel * stats.count as f64
                                } else {
                                    stats.count as f64 * Self::MAGIC_PREDICATE_SELECTIVITY
                                };
                                result / stats.count as f64
                            }
                        })
                    })
                    .fold(1., |acc, x| acc * x)
            }
            crate::ir::OperatorKind::PhysicalFilter(_) => Self::MAGIC_PREDICATE_SELECTIVITY,
            crate::ir::OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoin::borrow_raw_parts(&meta, &op.common);
                let mut left_keys = vec![];
                let mut right_keys = vec![];
                extract_join_keys(&join, &mut left_keys, &mut right_keys, ctx);
                if left_keys.is_empty() || right_keys.is_empty() {
                    return 1.0;
                }
                let left_n_distinct_product: f64 = left_keys
                    .iter()
                    .map(|col| {
                        ctx.get_column_stats(col)
                            .map(|s| s.n_distinct as f64)
                            .unwrap_or(0.)
                    })
                    .product();
                let right_n_distinct_product: f64 = right_keys
                    .iter()
                    .map(|col| {
                        ctx.get_column_stats(col)
                            .map(|s| s.n_distinct as f64)
                            .unwrap_or(0.)
                    })
                    .product();
                if left_n_distinct_product == 0.0 || right_n_distinct_product == 0.0 {
                    return Self::MAGIC_JOIN_COND_SELECTIVITY;
                }
                1.0 / left_n_distinct_product.max(right_n_distinct_product)
            }
            crate::ir::OperatorKind::PhysicalNLJoin(_)
            | crate::ir::OperatorKind::PhysicalHashJoin(_) => Self::MAGIC_JOIN_COND_SELECTIVITY,
            _ => 1.0,
        }
    }
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
                let exact_row_count = ctx.cat.describe_table(meta.source).row_count;
                Cardinality::with_count_lossy(exact_row_count)
            }
            OperatorKind::PhysicalTableScan(meta) => {
                let exact_row_count = ctx.cat.describe_table(meta.source).row_count;
                Cardinality::with_count_lossy(exact_row_count)
            }
            OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoin::borrow_raw_parts(meta, &op.common);
                let selectivity = if let Ok(literal) = join.join_cond().try_borrow::<Literal>() {
                    match literal.value() {
                        crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                        crate::ir::ScalarValue::Boolean(_) => 0.,
                        _ => unreachable!("join condition must be boolean"),
                    }
                } else {
                    MagicCardinalityEstimator::MAGIC_JOIN_COND_SELECTIVITY
                };
                let left_card = join.outer().cardinality(ctx);
                let right_card = join.inner().cardinality(ctx);
                selectivity * left_card * right_card
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
                    let sel = self.estimate_selectivity(op, ctx);
                    // println!("estimated selectivity for LogicalSelect: {}", sel);
                    sel
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
            OperatorKind::LogicalLimit(meta) => {
                let limit = LogicalLimit::borrow_raw_parts(meta, &op.common);
                Cardinality::with_count_lossy(*limit.limit())
            }
        }
    }
}
