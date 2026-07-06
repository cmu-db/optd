//! Cost model interfaces.

use crate::analysis::CardinalityEstimationV1;
use crate::hypergraph::QueryHypergraph;
use crate::optimize::{OptimizeError, OptimizeResult};
use crate::{
    AnalysisContext, BinaryOp, CardinalityProfile, Expr, ExprData, NaryOp, Operator, OperatorData,
    QueryContext, Relation,
};

/// Estimates operator costs for an optd query tree.
pub trait CostModel: Send + Sync + 'static {
    type Cost: Clone + PartialOrd;

    fn zero(&self) -> Self::Cost;

    fn add(&self, left: Self::Cost, right: Self::Cost) -> Self::Cost;

    /// Computes this operator's local cost, excluding input costs.
    fn operator_cost(
        &self,
        op: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost>;

    /// Computes local cost for a virtual binary relational operator.
    ///
    /// This is used by search algorithms before a candidate operator exists in
    /// the query arena.
    fn binary_operator_cost(
        &self,
        left: &CardinalityProfile,
        right: &CardinalityProfile,
        output: &CardinalityProfile,
        class: JoinAlgorithmClass,
    ) -> Self::Cost;

    /// Recursively computes total costs for the operator's inputs.
    fn input_costs(
        &self,
        op: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Vec<Self::Cost>> {
        op.get(ctx)
            .inputs()
            .into_iter()
            .map(|input| self.total_cost(input, ctx, analyses))
            .collect()
    }

    /// Computes total cost from this operator's local cost and precomputed child costs.
    fn total_cost_from_children(
        &self,
        op: Operator,
        child_costs: &[Self::Cost],
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        let input_count = op.get(ctx).inputs().len();
        if input_count != child_costs.len() {
            return Err(OptimizeError::PassError {
                pass: "CostModel",
                message: format!(
                    "operator {op} has {input_count} inputs but received {} child costs",
                    child_costs.len()
                ),
            });
        }

        let operator_cost = self.operator_cost(op, ctx, analyses)?;
        Ok(child_costs
            .iter()
            .cloned()
            .fold(operator_cost, |acc, cost| self.add(acc, cost)))
    }

    /// Recursively computes total cost for the operator subtree.
    fn total_cost(
        &self,
        op: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        let input_costs = self.input_costs(op, ctx, analyses)?;
        self.total_cost_from_children(op, &input_costs, ctx, analyses)
    }
}

/// Default scalar cost model used by optimizer heuristics.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultCostModel;

impl CostModel for DefaultCostModel {
    type Cost = f64;

    fn zero(&self) -> Self::Cost {
        0.0
    }

    fn add(&self, left: Self::Cost, right: Self::Cost) -> Self::Cost {
        left + right
    }

    fn operator_cost(
        &self,
        op: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        let output = cardinality_profile(ctx, analyses, op)?;

        Ok(match op.get(ctx) {
            OperatorData::Scan(_) | OperatorData::ConstScan(_) | OperatorData::TableFunction(_) => {
                row_materialization_cost(&output)
            }
            OperatorData::Selection(_) | OperatorData::Map(_) | OperatorData::Projection(_) => {
                output.rows.value
            }
            OperatorData::Aggregation(_) => row_materialization_cost(&output),
            OperatorData::Sort(_) => sort_cost(output.rows.value),
            OperatorData::Limit(_) | OperatorData::Output(_) | OperatorData::Rename(_) => 0.0,
            OperatorData::CrossProduct(data) => binary_rel_cost(
                ctx,
                analyses,
                data.outer,
                data.inner,
                &output,
                JoinAlgorithmClass::NestedLoopLike,
            )?,
            OperatorData::Join(data) => binary_rel_cost(
                ctx,
                analyses,
                data.outer,
                data.inner,
                &output,
                join_algorithm_class_for_predicate(data.on, ctx),
            )?,
        })
    }

    fn binary_operator_cost(
        &self,
        left: &CardinalityProfile,
        right: &CardinalityProfile,
        output: &CardinalityProfile,
        class: JoinAlgorithmClass,
    ) -> Self::Cost {
        join_algorithm_cost_for_profiles(left, right, output, class)
    }
}

impl Operator {
    /// Computes this operator's local cost with the default cost model.
    pub fn operator_cost(self, ctx: &QueryContext) -> OptimizeResult<f64> {
        let mut analyses = ctx.analyze();
        self.operator_cost_with_analyses(ctx, &mut analyses)
    }

    /// Computes this operator's local cost with caller-provided analyses.
    pub fn operator_cost_with_analyses(
        self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<f64> {
        DefaultCostModel.operator_cost(self, ctx, analyses)
    }

    /// Computes this operator subtree's total cost with the default cost model.
    pub fn total_cost(self, ctx: &QueryContext) -> OptimizeResult<f64> {
        let mut analyses = ctx.analyze();
        self.total_cost_with_analyses(ctx, &mut analyses)
    }

    /// Computes this operator subtree's total cost with caller-provided analyses.
    pub fn total_cost_with_analyses(
        self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<f64> {
        DefaultCostModel.total_cost(self, ctx, analyses)
    }
}

/// How expensive a join is to execute after cardinality estimation predicts output rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinAlgorithmClass {
    HashLike,
    NestedLoopLike,
}

/// Estimates execution work for a candidate join.
///
/// `*_width` is currently a column-count proxy for row materialization cost.
/// Cardinality estimation decides how many rows are produced; this function
/// decides whether producing them looks hash-like or pairwise.
pub fn join_algorithm_cost(
    left_rows: f64,
    left_width: usize,
    right_rows: f64,
    right_width: usize,
    output_rows: f64,
    output_width: usize,
    class: JoinAlgorithmClass,
) -> f64 {
    match class {
        JoinAlgorithmClass::HashLike => {
            let left_bytes = left_rows * left_width.max(1) as f64;
            let right_bytes = right_rows * right_width.max(1) as f64;
            let output_bytes = output_rows * output_width.max(1) as f64;
            left_bytes + right_bytes + (left_rows * right_rows).powf(0.75) + output_bytes
        }
        JoinAlgorithmClass::NestedLoopLike => left_rows * right_rows + output_rows,
    }
}

pub(crate) fn join_algorithm_cost_for_profiles(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    output: &CardinalityProfile,
    class: JoinAlgorithmClass,
) -> f64 {
    join_algorithm_cost(
        left.rows.value,
        left.columns.len(),
        right.rows.value,
        right.columns.len(),
        output.rows.value,
        output.columns.len(),
        class,
    )
}

pub(crate) fn join_algorithm_class(
    edge_indices: &[usize],
    hg: &QueryHypergraph,
    ctx: &QueryContext,
) -> JoinAlgorithmClass {
    if edge_indices.iter().any(|idx| {
        hg.edges[*idx]
            .predicate
            .is_some_and(|predicate| contains_hash_join_key(predicate, ctx))
    }) {
        JoinAlgorithmClass::HashLike
    } else {
        JoinAlgorithmClass::NestedLoopLike
    }
}

pub(crate) fn join_algorithm_class_for_predicate(
    predicate: Expr,
    ctx: &QueryContext,
) -> JoinAlgorithmClass {
    if contains_hash_join_key(predicate, ctx) {
        JoinAlgorithmClass::HashLike
    } else {
        JoinAlgorithmClass::NestedLoopLike
    }
}

fn cardinality_profile(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    op: Operator,
) -> OptimizeResult<CardinalityProfile> {
    analyses
        .get::<CardinalityEstimationV1>(ctx, op)
        .map_err(|err| OptimizeError::PassError {
            pass: "CostModel",
            message: err.to_string(),
        })
}

fn row_materialization_cost(profile: &CardinalityProfile) -> f64 {
    profile.rows.value * profile.columns.len().max(1) as f64
}

fn sort_cost(rows: f64) -> f64 {
    rows * rows.max(2.0).log2()
}

fn binary_rel_cost(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    outer: Operator,
    inner: Operator,
    output: &CardinalityProfile,
    class: JoinAlgorithmClass,
) -> OptimizeResult<f64> {
    let left = cardinality_profile(ctx, analyses, outer)?;
    let right = cardinality_profile(ctx, analyses, inner)?;
    Ok(join_algorithm_cost_for_profiles(
        &left, &right, output, class,
    ))
}

/// Returns true when `expr` contains a column-to-column equality predicate.
///
/// This identifies predicates that can plausibly be executed with hash-style
/// key matching. Residual predicates may still be present; an `AND` expression
/// is hash-like if any conjunct is `col = col` or `col IS NOT DISTINCT FROM col`.
fn contains_hash_join_key(expr: Expr, ctx: &QueryContext) -> bool {
    match expr.get(ctx) {
        ExprData::Binary {
            op: BinaryOp::Eq | BinaryOp::IsNotDistinctFrom,
            left,
            right,
        } => {
            matches!(
                (left.get(ctx), right.get(ctx)),
                (ExprData::ColumnRef(_), ExprData::ColumnRef(_))
            )
        }
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => exprs.iter().any(|expr| contains_hash_join_key(*expr, ctx)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::connecting_edge_indices;
    use crate::{
        Column, ColumnData, CrossProduct, ExprData, Join, JoinType, NullOrdering, OperatorData,
        Output, Scan, Selection, Sort, SortDirection, SortKey, TableRef, build_hypergraph,
        nodeset_singleton,
    };
    use arrow_schema::DataType;

    fn add_single_i64_column_scan(
        ctx: &mut QueryContext,
        table: &str,
        column: &str,
    ) -> (Operator, Column) {
        let column = ColumnData::new(column, DataType::Int64).add(ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare(table),
            columns: vec![column],
        })
        .add(ctx);
        (scan, column)
    }

    fn binary_predicate(ctx: &mut QueryContext, op: BinaryOp, left: Column, right: Column) -> Expr {
        ExprData::Binary {
            op,
            left: ExprData::ColumnRef(left).add(ctx),
            right: ExprData::ColumnRef(right).add(ctx),
        }
        .add(ctx)
    }

    fn two_way_join_with_predicate(
        predicate: impl FnOnce(&mut QueryContext, Column, Column) -> Expr,
    ) -> (QueryContext, Operator) {
        let mut ctx = QueryContext::new();
        let (sa, a) = add_single_i64_column_scan(&mut ctx, "A", "a");
        let (sb, b) = add_single_i64_column_scan(&mut ctx, "B", "b");
        let on = predicate(&mut ctx, a, b);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: sa,
            inner: sb,
        })
        .add(&mut ctx);
        ctx.set_root(join);
        (ctx, join)
    }

    fn join_algorithm_class_for_root(ctx: &QueryContext, root: Operator) -> JoinAlgorithmClass {
        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(ctx, &mut analyses, root);
        let edge_indices = connecting_edge_indices(nodeset_singleton(0), nodeset_singleton(1), &hg);
        join_algorithm_class(&edge_indices, &hg, ctx)
    }

    #[test]
    fn total_cost_sums_local_operator_costs() {
        let mut ctx = QueryContext::new();
        let (scan, column) = add_single_i64_column_scan(&mut ctx, "A", "a");
        let predicate = ExprData::ColumnRef(column).add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: selection }).add(&mut ctx);
        let mut analyses = AnalysisContext::new();
        let model = DefaultCostModel;

        let total = model.total_cost(output, &ctx, &mut analyses).unwrap();

        assert_eq!(total, 1250.0);
    }

    #[test]
    fn total_cost_from_children_rejects_wrong_input_count() {
        let mut ctx = QueryContext::new();
        let (scan, _) = add_single_i64_column_scan(&mut ctx, "A", "a");
        let output = OperatorData::Output(Output { input: scan }).add(&mut ctx);
        let mut analyses = AnalysisContext::new();
        let model = DefaultCostModel;

        let err = model
            .total_cost_from_children(output, &[], &ctx, &mut analyses)
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("has 1 inputs but received 0 child costs")
        );
    }

    #[test]
    fn default_model_costs_scan_and_sort() {
        let mut ctx = QueryContext::new();
        let (scan, column) = add_single_i64_column_scan(&mut ctx, "A", "a");
        let sort = OperatorData::Sort(Sort {
            keys: vec![SortKey {
                expr: ExprData::ColumnRef(column).add(&mut ctx),
                direction: SortDirection::Asc,
                nulls: NullOrdering::Last,
            }],
            input: scan,
        })
        .add(&mut ctx);
        let mut analyses = AnalysisContext::new();
        let model = DefaultCostModel;

        assert_eq!(
            model.operator_cost(scan, &ctx, &mut analyses).unwrap(),
            1000.0
        );
        assert_eq!(
            model.operator_cost(sort, &ctx, &mut analyses).unwrap(),
            1000.0 * f64::log2(1000.0)
        );
    }

    #[test]
    fn operator_cost_convenience_uses_default_model() {
        let mut ctx = QueryContext::new();
        let (scan, column) = add_single_i64_column_scan(&mut ctx, "A", "a");
        let selection = OperatorData::Selection(Selection {
            predicate: ExprData::ColumnRef(column).add(&mut ctx),
            input: scan,
        })
        .add(&mut ctx);

        assert_eq!(scan.operator_cost(&ctx).unwrap(), 1000.0);
        assert_eq!(selection.total_cost(&ctx).unwrap(), 1250.0);
    }

    #[test]
    fn default_model_costs_cross_product_and_joins() {
        let mut ctx = QueryContext::new();
        let (left_scan, left_col) = add_single_i64_column_scan(&mut ctx, "A", "a");
        let (right_scan, right_col) = add_single_i64_column_scan(&mut ctx, "B", "b");
        let cross = OperatorData::CrossProduct(CrossProduct {
            outer: left_scan,
            inner: right_scan,
        })
        .add(&mut ctx);
        let eq = binary_predicate(&mut ctx, BinaryOp::Eq, left_col, right_col);
        let hash_join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: eq,
            outer: left_scan,
            inner: right_scan,
        })
        .add(&mut ctx);
        let gt = binary_predicate(&mut ctx, BinaryOp::Gt, left_col, right_col);
        let nested_loop_join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: gt,
            outer: left_scan,
            inner: right_scan,
        })
        .add(&mut ctx);
        let mut analyses = AnalysisContext::new();
        let model = DefaultCostModel;

        assert_eq!(
            model.operator_cost(cross, &ctx, &mut analyses).unwrap(),
            2_000_000.0
        );
        assert_eq!(
            model.operator_cost(hash_join, &ctx, &mut analyses).unwrap(),
            join_algorithm_cost(
                1000.0,
                1,
                1000.0,
                1,
                10000.0,
                2,
                JoinAlgorithmClass::HashLike
            )
        );
        assert_eq!(
            model
                .operator_cost(nested_loop_join, &ctx, &mut analyses)
                .unwrap(),
            join_algorithm_cost(
                1000.0,
                1,
                1000.0,
                1,
                250000.0,
                2,
                JoinAlgorithmClass::NestedLoopLike
            )
        );
    }

    #[test]
    fn sort_cost_is_finite_for_zero_and_one_rows() {
        for rows in [0.0_f64, 1.0] {
            assert!(sort_cost(rows).is_finite());
        }
    }

    #[test]
    fn equi_join_is_hash_like_for_costing() {
        let (ctx, root) =
            two_way_join_with_predicate(|ctx, a, b| binary_predicate(ctx, BinaryOp::Eq, a, b));

        assert_eq!(
            join_algorithm_class_for_root(&ctx, root),
            JoinAlgorithmClass::HashLike
        );
    }

    #[test]
    fn is_not_distinct_from_join_is_hash_like_for_costing() {
        let (ctx, root) = two_way_join_with_predicate(|ctx, a, b| {
            binary_predicate(ctx, BinaryOp::IsNotDistinctFrom, a, b)
        });

        assert_eq!(
            join_algorithm_class_for_root(&ctx, root),
            JoinAlgorithmClass::HashLike
        );
    }

    #[test]
    fn mixed_equi_and_residual_join_is_hash_like_for_costing() {
        let (ctx, root) = two_way_join_with_predicate(|ctx, a, b| {
            let eq = binary_predicate(ctx, BinaryOp::Eq, a, b);
            let residual = binary_predicate(ctx, BinaryOp::Gt, a, b);
            ExprData::Nary {
                op: NaryOp::And,
                exprs: vec![eq, residual],
            }
            .add(ctx)
        });

        assert_eq!(
            join_algorithm_class_for_root(&ctx, root),
            JoinAlgorithmClass::HashLike
        );
    }

    #[test]
    fn pure_non_equi_join_is_nested_loop_like_for_costing() {
        let (ctx, root) =
            two_way_join_with_predicate(|ctx, a, b| binary_predicate(ctx, BinaryOp::Gt, a, b));

        assert_eq!(
            join_algorithm_class_for_root(&ctx, root),
            JoinAlgorithmClass::NestedLoopLike
        );
    }

    #[test]
    fn hash_like_cost_does_not_use_pairwise_input_product() {
        assert_eq!(
            join_algorithm_cost(
                1_000_000.0,
                1,
                1_000_000.0,
                1,
                10.0,
                1,
                JoinAlgorithmClass::HashLike
            ),
            1_002_000_010.0
        );
        assert_eq!(
            join_algorithm_cost(
                1_000_000.0,
                1,
                1_000_000.0,
                1,
                10.0,
                1,
                JoinAlgorithmClass::NestedLoopLike
            ),
            1_000_000_000_010.0
        );
        assert_eq!(
            join_algorithm_cost(10.0, 3, 20.0, 4, 5.0, 7, JoinAlgorithmClass::HashLike),
            30.0 + 80.0 + f64::powf(200.0, 0.75) + 35.0
        );
    }

    #[test]
    fn literal_predicate_join_is_nested_loop_like() {
        let mut ctx = QueryContext::new();
        let predicate = ExprData::Literal(crate::ScalarValue::Boolean(true)).add(&mut ctx);

        assert_eq!(
            join_algorithm_class_for_predicate(predicate, &ctx),
            JoinAlgorithmClass::NestedLoopLike
        );
    }
}
