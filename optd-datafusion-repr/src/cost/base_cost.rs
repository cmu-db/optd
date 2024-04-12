mod agg;
mod filter;
mod limit;

use std::ops::Bound;

use crate::plan_nodes::{
    BinOpType, ColumnRefExpr, ConstantExpr, ConstantType, Expr, ExprList, InListExpr, LogOpExpr,
    LogOpType, OptRelNode,
};
use crate::properties::column_ref::{ColumnRefPropertyBuilder, GroupColumnRefs};
use crate::{
    plan_nodes::{JoinType, OptRelNodeRef, OptRelNodeTyp},
    properties::column_ref::ColumnRef,
};
use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::{Cost, CostModel},
    rel_node::{RelNode, RelNodeTyp, Value},
};

use super::stats::{BaseTableStats, Distribution, MostCommonValues, PerColumnStats, PerTableStats};

fn compute_plan_node_cost<T: RelNodeTyp, C: CostModel<T>>(
    model: &C,
    node: &RelNode<T>,
    total_cost: &mut Cost,
) -> Cost {
    let children = node
        .children
        .iter()
        .map(|child| compute_plan_node_cost(model, child, total_cost))
        .collect_vec();
    let cost = model.compute_cost(&node.typ, &node.data, &children, None, None);
    model.accumulate(total_cost, &cost);
    cost
}

pub struct OptCostModel<M: MostCommonValues, D: Distribution> {
    per_table_stats_map: BaseTableStats<M, D>,
}

// Default statistics. All are from selfuncs.h in Postgres unless specified otherwise
// Default selectivity estimate for equalities such as "A = b"
const DEFAULT_EQ_SEL: f64 = 0.005;
// Default selectivity estimate for inequalities such as "A < b"
const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
// Default selectivity estimate for pattern-match operators such as LIKE
const DEFAULT_MATCH_SEL: f64 = 0.005;
// Default n-distinct estimate for derived columns or columns lacking statistics
const DEFAULT_NUM_DISTINCT: u64 = 200;
// Default selectivity if we have no information
const DEFAULT_UNK_SEL: f64 = 0.005;

// A placeholder for unimplemented!() for codepaths which are accessed by plannertest
const UNIMPLEMENTED_SEL: f64 = 0.01;

pub const ROW_COUNT: usize = 1;
pub const COMPUTE_COST: usize = 2;
pub const IO_COST: usize = 3;

impl<M: MostCommonValues, D: Distribution> OptCostModel<M, D> {
    pub fn row_cnt(Cost(cost): &Cost) -> f64 {
        cost[ROW_COUNT]
    }

    pub fn compute_cost(Cost(cost): &Cost) -> f64 {
        cost[COMPUTE_COST]
    }

    pub fn io_cost(Cost(cost): &Cost) -> f64 {
        cost[IO_COST]
    }

    pub fn cost_tuple(Cost(cost): &Cost) -> (f64, f64, f64) {
        (cost[ROW_COUNT], cost[COMPUTE_COST], cost[IO_COST])
    }

    pub fn weighted_cost(row_cnt: f64, compute_cost: f64, io_cost: f64) -> f64 {
        let _ = row_cnt;
        compute_cost + io_cost
    }

    pub fn cost(row_cnt: f64, compute_cost: f64, io_cost: f64) -> Cost {
        Cost(vec![
            Self::weighted_cost(row_cnt, compute_cost, io_cost),
            row_cnt,
            compute_cost,
            io_cost,
        ])
    }
}

impl<M: MostCommonValues, D: Distribution> CostModel<OptRelNodeTyp> for OptCostModel<M, D> {
    fn explain(&self, cost: &Cost) -> String {
        format!(
            "weighted={},row_cnt={},compute={},io={}",
            cost.0[0],
            Self::row_cnt(cost),
            Self::compute_cost(cost),
            Self::io_cost(cost)
        )
    }

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost) {
        // do not accumulate row count
        total_cost.0[COMPUTE_COST] += Self::compute_cost(cost);
        total_cost.0[IO_COST] += Self::io_cost(cost);
        total_cost.0[0] = Self::weighted_cost(
            total_cost.0[ROW_COUNT],
            total_cost.0[COMPUTE_COST],
            total_cost.0[IO_COST],
        );
    }

    fn zero(&self) -> Cost {
        Self::cost(0.0, 0.0, 0.0)
    }

    fn compute_cost(
        &self,
        node: &OptRelNodeTyp,
        data: &Option<Value>,
        children: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        match node {
            OptRelNodeTyp::PhysicalScan => {
                let table = data.as_ref().unwrap().as_str();
                let row_cnt = self.get_row_cnt(table.as_ref()).unwrap_or(1) as f64;
                Self::cost(row_cnt, 0.0, row_cnt)
            }
            OptRelNodeTyp::PhysicalEmptyRelation => Self::cost(0.5, 0.01, 0.0),
            OptRelNodeTyp::PhysicalLimit => Self::get_limit_cost(children, context, optimizer),
            OptRelNodeTyp::PhysicalFilter => self.get_filter_cost(children, context, optimizer),
            OptRelNodeTyp::PhysicalNestedLoopJoin(join_typ) => {
                let (row_cnt_1, _, _) = Self::cost_tuple(&children[0]);
                let (row_cnt_2, _, _) = Self::cost_tuple(&children[1]);
                let (_, compute_cost, _) = Self::cost_tuple(&children[2]);
                let selectivity = if let (Some(context), Some(optimizer)) = (context, optimizer) {
                    let column_refs = optimizer
                        .get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
                    let expr_group_id = context.children_group_ids[2];
                    let expr_trees = optimizer.get_all_group_bindings(expr_group_id, false);
                    // there may be more than one expression tree in a group. see comment in OptRelNodeTyp::PhysicalFilter(_) for more information
                    let expr_tree = expr_trees.first().expect("expression missing");
                    self.get_join_selectivity_from_expr_tree(
                        *join_typ,
                        expr_tree.clone(),
                        &column_refs,
                        row_cnt_1,
                        row_cnt_2,
                    )
                } else {
                    DEFAULT_UNK_SEL
                };
                Self::cost(
                    (row_cnt_1 * row_cnt_2 * selectivity).max(1.0),
                    row_cnt_1 * row_cnt_2 * compute_cost + row_cnt_1,
                    0.0,
                )
            }
            OptRelNodeTyp::PhysicalProjection => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                let (_, compute_cost, _) = Self::cost_tuple(&children[1]);
                Self::cost(row_cnt, compute_cost * row_cnt, 0.0)
            }
            OptRelNodeTyp::PhysicalHashJoin(join_typ) => {
                let (row_cnt_1, _, _) = Self::cost_tuple(&children[0]);
                let (row_cnt_2, _, _) = Self::cost_tuple(&children[1]);
                let selectivity = if let (Some(context), Some(optimizer)) = (context, optimizer) {
                    let column_refs = optimizer
                        .get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
                    let left_keys_group_id = context.children_group_ids[2];
                    let right_keys_group_id = context.children_group_ids[3];
                    let left_keys_list =
                        optimizer.get_all_group_bindings(left_keys_group_id, false);
                    let right_keys_list =
                        optimizer.get_all_group_bindings(right_keys_group_id, false);
                    // there may be more than one expression tree in a group. see comment in OptRelNodeTyp::PhysicalFilter(_) for more information
                    let left_keys = left_keys_list.first().expect("left keys missing");
                    let right_keys = right_keys_list.first().expect("right keys missing");
                    self.get_join_selectivity_from_keys(
                        *join_typ,
                        ExprList::from_rel_node(left_keys.clone())
                            .expect("left_keys should be an ExprList"),
                        ExprList::from_rel_node(right_keys.clone())
                            .expect("right_keys should be an ExprList"),
                        &column_refs,
                        row_cnt_1,
                        row_cnt_2,
                    )
                } else {
                    DEFAULT_UNK_SEL
                };
                Self::cost(
                    (row_cnt_1 * row_cnt_2 * selectivity).max(1.0),
                    row_cnt_1 * 2.0 + row_cnt_2,
                    0.0,
                )
            }

            OptRelNodeTyp::PhysicalSort => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                Self::cost(row_cnt, row_cnt * row_cnt.ln_1p().max(1.0), 0.0)
            }
            OptRelNodeTyp::PhysicalAgg => self.get_agg_cost(children, context, optimizer),
            OptRelNodeTyp::List => {
                let compute_cost = children
                    .iter()
                    .map(|child| {
                        let (_, compute_cost, _) = Self::cost_tuple(child);
                        compute_cost
                    })
                    .sum::<f64>();
                Self::cost(1.0, compute_cost + 0.01, 0.0)
            }
            OptRelNodeTyp::ColumnRef => Self::cost(1.0, 0.01, 0.0),
            _ if node.is_expression() => {
                let compute_cost = children
                    .iter()
                    .map(|child| {
                        let (_, compute_cost, _) = Self::cost_tuple(child);
                        compute_cost
                    })
                    .sum::<f64>();
                Self::cost(1.0, compute_cost + 1.0, 0.0)
            }
            x => unimplemented!("cannot compute cost for {}", x),
        }
    }

    fn compute_plan_node_cost(&self, node: &RelNode<OptRelNodeTyp>) -> Cost {
        let mut cost = self.zero();
        let top = compute_plan_node_cost(self, node, &mut cost);
        cost.0[ROW_COUNT] = top.0[ROW_COUNT];
        cost
    }
}

impl<M: MostCommonValues, D: Distribution> OptCostModel<M, D> {
    pub fn new(per_table_stats_map: BaseTableStats<M, D>) -> Self {
        Self {
            per_table_stats_map,
        }
    }

    /// Check if an expr_tree is a join condition, returning the join on col ref pair if it is.
    /// The reason the check and the info are in the same function is because their code is almost identical.
    /// It only picks out equality conditions between two column refs on different tables
    fn get_on_col_ref_pair(
        expr_tree: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
    ) -> Option<(ColumnRefExpr, ColumnRefExpr)> {
        // 1. Check that it's equality
        if expr_tree.typ == OptRelNodeTyp::BinOp(BinOpType::Eq) {
            let left_child = expr_tree.child(0);
            let right_child = expr_tree.child(1);
            // 2. Check that both sides are column refs
            if left_child.typ == OptRelNodeTyp::ColumnRef
                && right_child.typ == OptRelNodeTyp::ColumnRef
            {
                // 3. Check that both sides don't belong to the same table (if we don't know, that means they don't belong)
                let left_col_ref_expr = ColumnRefExpr::from_rel_node(left_child)
                    .expect("we already checked that the type is ColumnRef");
                let right_col_ref_expr = ColumnRefExpr::from_rel_node(right_child)
                    .expect("we already checked that the type is ColumnRef");
                let left_col_ref = &column_refs[left_col_ref_expr.index()];
                let right_col_ref = &column_refs[right_col_ref_expr.index()];
                let is_same_table = if let (
                    ColumnRef::BaseTableColumnRef {
                        table: left_table, ..
                    },
                    ColumnRef::BaseTableColumnRef {
                        table: right_table, ..
                    },
                ) = (left_col_ref, right_col_ref)
                {
                    left_table == right_table
                } else {
                    false
                };
                if !is_same_table {
                    Some((left_col_ref_expr, right_col_ref_expr))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    /// The expr_tree input must be a "mixed expression tree", just like with get_filter_selectivity()
    /// This is a "wrapper" to separate the equality conditions from the filter conditions before calling
    ///   the "main" get_join_selectivity_core() function.
    fn get_join_selectivity_from_expr_tree(
        &self,
        join_typ: JoinType,
        expr_tree: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
        left_row_cnt: f64,
        right_row_cnt: f64,
    ) -> f64 {
        assert!(expr_tree.typ.is_expression());
        if expr_tree.typ == OptRelNodeTyp::LogOp(LogOpType::And) {
            let mut on_col_ref_pairs = vec![];
            let mut filter_expr_trees = vec![];
            for child_expr_tree in &expr_tree.children {
                if let Some(on_col_ref_pair) =
                    Self::get_on_col_ref_pair(child_expr_tree.clone(), column_refs)
                {
                    on_col_ref_pairs.push(on_col_ref_pair)
                } else {
                    let child_expr = Expr::from_rel_node(child_expr_tree.clone()).expect(
                        "everything that is a direct child of an And node must be an expression",
                    );
                    filter_expr_trees.push(child_expr);
                }
            }
            assert!(on_col_ref_pairs.len() + filter_expr_trees.len() == expr_tree.children.len());
            let filter_expr_tree = if filter_expr_trees.is_empty() {
                None
            } else {
                Some(
                    LogOpExpr::new(LogOpType::And, ExprList::new(filter_expr_trees))
                        .into_rel_node(),
                )
            };
            self.get_join_selectivity_core(
                join_typ,
                on_col_ref_pairs,
                filter_expr_tree,
                column_refs,
                left_row_cnt,
                right_row_cnt,
            )
        } else {
            #[allow(clippy::collapsible_else_if)]
            if let Some(on_col_ref_pair) = Self::get_on_col_ref_pair(expr_tree.clone(), column_refs)
            {
                self.get_join_selectivity_core(
                    join_typ,
                    vec![on_col_ref_pair],
                    None,
                    column_refs,
                    left_row_cnt,
                    right_row_cnt,
                )
            } else {
                self.get_join_selectivity_core(
                    join_typ,
                    vec![],
                    Some(expr_tree),
                    column_refs,
                    left_row_cnt,
                    right_row_cnt,
                )
            }
        }
    }

    /// A wrapper to convert the join keys to the format expected by get_join_selectivity_core()
    fn get_join_selectivity_from_keys(
        &self,
        join_typ: JoinType,
        left_keys: ExprList,
        right_keys: ExprList,
        column_refs: &GroupColumnRefs,
        left_row_cnt: f64,
        right_row_cnt: f64,
    ) -> f64 {
        assert!(left_keys.len() == right_keys.len());
        // I assume that the keys are already in the right order s.t. the ith key of left_keys corresponds with the ith key of right_keys
        let on_col_ref_pairs = left_keys
            .to_vec()
            .into_iter()
            .zip(right_keys.to_vec())
            .map(|(left_key, right_key)| {
                (
                    ColumnRefExpr::from_rel_node(left_key.into_rel_node())
                        .expect("keys should be ColumnRefExprs"),
                    ColumnRefExpr::from_rel_node(right_key.into_rel_node())
                        .expect("keys should be ColumnRefExprs"),
                )
            })
            .collect_vec();
        self.get_join_selectivity_core(
            join_typ,
            on_col_ref_pairs,
            None,
            column_refs,
            left_row_cnt,
            right_row_cnt,
        )
    }

    /// The core logic of join selectivity which assumes we've already separated the expression into the on conditions and the filters
    fn get_join_selectivity_core(
        &self,
        join_typ: JoinType,
        on_col_ref_pairs: Vec<(ColumnRefExpr, ColumnRefExpr)>,
        filter_expr_tree: Option<OptRelNodeRef>,
        column_refs: &GroupColumnRefs,
        left_row_cnt: f64,
        right_row_cnt: f64,
    ) -> f64 {
        let join_on_selectivity = self.get_join_on_selectivity(&on_col_ref_pairs, column_refs);
        // Currently, there is no difference in how we handle a join filter and a select filter, so we use the same function
        // One difference (that we *don't* care about right now) is that join filters can contain expressions from multiple
        //   different tables. Currently, this doesn't affect the get_filter_selectivity() function, but this may change in
        //   the future
        let join_filter_selectivity = match filter_expr_tree {
            Some(filter_expr_tree) => self.get_filter_selectivity(filter_expr_tree, column_refs),
            None => 1.0,
        };
        let inner_join_selectivity = join_on_selectivity * join_filter_selectivity;
        match join_typ {
            JoinType::Inner => inner_join_selectivity,
            JoinType::LeftOuter => f64::max(inner_join_selectivity, 1.0 / right_row_cnt),
            JoinType::RightOuter => f64::max(inner_join_selectivity, 1.0 / left_row_cnt),
            JoinType::Cross => {
                assert!(
                    on_col_ref_pairs.is_empty(),
                    "Cross joins should not have on columns"
                );
                join_filter_selectivity
            }
            _ => unimplemented!("join_typ={} is not implemented", join_typ),
        }
    }

    fn get_per_column_stats_from_col_ref(
        &self,
        col_ref: &ColumnRef,
    ) -> Option<&PerColumnStats<M, D>> {
        if let ColumnRef::BaseTableColumnRef { table, col_idx } = col_ref {
            self.get_per_column_stats(table, *col_idx)
        } else {
            None
        }
    }

    fn get_per_column_stats(&self, table: &str, col_idx: usize) -> Option<&PerColumnStats<M, D>> {
        self.per_table_stats_map
            .get(table)
            .and_then(|per_table_stats| per_table_stats.per_column_stats_vec[col_idx].as_ref())
    }

    /// Get the selectivity of the on conditions
    /// Note that the selectivity of the on conditions does not depend on join type. Join type is accounted for separately in get_join_selectivity_core()
    fn get_join_on_selectivity(
        &self,
        on_col_ref_pairs: &[(ColumnRefExpr, ColumnRefExpr)],
        column_refs: &GroupColumnRefs,
    ) -> f64 {
        // multiply the selectivities of all individual conditions together
        on_col_ref_pairs.iter().map(|on_col_ref_pair| {
            // the formula for each pair is min(1 / ndistinct1, 1 / ndistinct2) (see https://postgrespro.com/blog/pgsql/5969618)
            let ndistincts = vec![&on_col_ref_pair.0, &on_col_ref_pair.1].into_iter().map(|on_col_ref_expr| {
                match self.get_per_column_stats_from_col_ref(&column_refs[on_col_ref_expr.index()]) {
                    Some(per_col_stats) => per_col_stats.ndistinct,
                    None => DEFAULT_NUM_DISTINCT,
                }
            });
            // using reduce(f64::min) is the idiomatic workaround to min() because f64 does not implement Ord due to NaN
            let selectivity = ndistincts.map(|ndistinct| 1.0 / ndistinct as f64).reduce(f64::min).expect("reduce() only returns None if the iterator is empty, which is impossible since col_ref_exprs.len() == 2");
            assert!(!selectivity.is_nan(), "it should be impossible for selectivity to be NaN since n-distinct is never 0");
            selectivity
        }).product()
    }

    /// Convert the left and right child nodes of some operation to what they semantically are
    /// This is convenient to avoid repeating the same logic just with "left" and "right" swapped
    fn get_semantic_nodes(
        left: OptRelNodeRef,
        right: OptRelNodeRef,
    ) -> (Vec<ColumnRefExpr>, Vec<OptRelNodeRef>, bool) {
        let mut col_ref_exprs = vec![];
        let mut non_col_ref_exprs = vec![];
        let is_left_col_ref;
        // I intentionally performed moves on left and right. This way, we don't accidentally use them after this block
        // We always want to use "col_ref_expr" and "non_col_ref_expr" instead of "left" or "right"
        if left.as_ref().typ == OptRelNodeTyp::ColumnRef {
            is_left_col_ref = true;
            col_ref_exprs.push(
                ColumnRefExpr::from_rel_node(left)
                    .expect("we already checked that the type is ColumnRef"),
            );
        } else {
            is_left_col_ref = false;
            non_col_ref_exprs.push(left);
        }
        if right.as_ref().typ == OptRelNodeTyp::ColumnRef {
            col_ref_exprs.push(
                ColumnRefExpr::from_rel_node(right)
                    .expect("we already checked that the type is ColumnRef"),
            );
        } else {
            non_col_ref_exprs.push(right);
        }
        (col_ref_exprs, non_col_ref_exprs, is_left_col_ref)
    }

    /// The default selectivity of a comparison expression
    /// Used when one side of the comparison is a column while the other side is something too
    ///   complex/impossible to evaluate (subquery, UDF, another column, we have no stats, etc.)
    fn get_default_comparison_op_selectivity(comp_bin_op_typ: BinOpType) -> f64 {
        assert!(comp_bin_op_typ.is_comparison());
        match comp_bin_op_typ {
            BinOpType::Eq => DEFAULT_EQ_SEL,
            BinOpType::Neq => 1.0 - DEFAULT_EQ_SEL,
            BinOpType::Lt | BinOpType::Leq | BinOpType::Gt | BinOpType::Geq => DEFAULT_INEQ_SEL,
            _ => unreachable!(
                "all comparison BinOpTypes were enumerated. this should be unreachable"
            ),
        }
    }

    pub fn get_row_cnt(&self, table: &str) -> Option<usize> {
        self.per_table_stats_map
            .get(table)
            .map(|per_table_stats| per_table_stats.row_cnt)
    }
}

/// I thought about using the system's own parser and planner to generate these expression trees, but
/// this is not currently feasible because it would create a cyclic dependency between optd-datafusion-bridge
/// and optd-datafusion-repr
#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use optd_core::rel_node::Value;
    use std::collections::HashMap;

    use crate::{
        cost::{base_cost::DEFAULT_EQ_SEL, stats::*},
        plan_nodes::{
            BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, Expr, ExprList, InListExpr,
            JoinType, LogOpExpr, LogOpType, OptRelNode, OptRelNodeRef, UnOpExpr, UnOpType,
        },
        properties::column_ref::{ColumnRef, GroupColumnRefs},
    };

    use super::*;
    type TestPerColumnStats = PerColumnStats<TestMostCommonValues, TestDistribution>;
    type TestOptCostModel = OptCostModel<TestMostCommonValues, TestDistribution>;

    struct TestMostCommonValues {
        mcvs: HashMap<Value, f64>,
    }

    struct TestDistribution {
        cdfs: HashMap<Value, f64>,
    }

    impl TestMostCommonValues {
        fn new(mcvs_vec: Vec<(Value, f64)>) -> Self {
            Self {
                mcvs: mcvs_vec.into_iter().collect(),
            }
        }

        pub fn empty() -> Self {
            TestMostCommonValues::new(vec![])
        }
    }

    impl MostCommonValues for TestMostCommonValues {
        fn freq(&self, value: &Value) -> Option<f64> {
            self.mcvs.get(value).copied()
        }

        fn total_freq(&self) -> f64 {
            self.mcvs.values().sum()
        }

        fn freq_over_pred(&self, pred: Box<dyn Fn(&Value) -> bool>) -> f64 {
            self.mcvs
                .iter()
                .filter(|(val, _)| pred(val))
                .map(|(_, freq)| freq)
                .sum()
        }

        fn cnt(&self) -> usize {
            self.mcvs.len()
        }
    }

    impl TestDistribution {
        fn new(cdfs_vec: Vec<(Value, f64)>) -> Self {
            Self {
                cdfs: cdfs_vec.into_iter().collect(),
            }
        }

        fn empty() -> Self {
            TestDistribution::new(vec![])
        }
    }

    impl Distribution for TestDistribution {
        fn cdf(&self, value: &Value) -> f64 {
            *self.cdfs.get(value).unwrap_or(&0.0)
        }
    }

    const TABLE1_NAME: &str = "table1";
    const TABLE2_NAME: &str = "table2";

    // one column is sufficient for all filter selectivity tests
    fn create_one_column_cost_model(per_column_stats: TestPerColumnStats) -> TestOptCostModel {
        OptCostModel::new(
            vec![(
                String::from(TABLE1_NAME),
                PerTableStats::new(100, vec![Some(per_column_stats)]),
            )]
            .into_iter()
            .collect(),
        )
    }

    /// Two columns is sufficient for all join selectivity tests
    fn create_two_table_cost_model(
        tbl1_per_column_stats: TestPerColumnStats,
        tbl2_per_column_stats: TestPerColumnStats,
    ) -> TestOptCostModel {
        create_two_table_cost_model_custom_row_cnts(
            tbl1_per_column_stats,
            tbl2_per_column_stats,
            100,
            100,
        )
    }

    /// We need custom row counts because some join algorithms rely on the row cnt
    fn create_two_table_cost_model_custom_row_cnts(
        tbl1_per_column_stats: TestPerColumnStats,
        tbl2_per_column_stats: TestPerColumnStats,
        tbl1_row_cnt: usize,
        tbl2_row_cnt: usize,
    ) -> TestOptCostModel {
        OptCostModel::new(
            vec![
                (
                    String::from(TABLE1_NAME),
                    PerTableStats::new(tbl1_row_cnt, vec![Some(tbl1_per_column_stats)]),
                ),
                (
                    String::from(TABLE2_NAME),
                    PerTableStats::new(tbl2_row_cnt, vec![Some(tbl2_per_column_stats)]),
                ),
            ]
            .into_iter()
            .collect(),
        )
    }

    fn col_ref(idx: u64) -> OptRelNodeRef {
        // this conversion is always safe because idx was originally a usize
        let idx_as_usize = idx as usize;
        ColumnRefExpr::new(idx_as_usize).into_rel_node()
    }

    fn cnst(value: Value) -> OptRelNodeRef {
        ConstantExpr::new(value).into_rel_node()
    }

    fn bin_op(op_type: BinOpType, left: OptRelNodeRef, right: OptRelNodeRef) -> OptRelNodeRef {
        BinOpExpr::new(
            Expr::from_rel_node(left).expect("left should be an Expr"),
            Expr::from_rel_node(right).expect("right should be an Expr"),
            op_type,
        )
        .into_rel_node()
    }

    fn log_op(op_type: LogOpType, children: Vec<OptRelNodeRef>) -> OptRelNodeRef {
        LogOpExpr::new(
            op_type,
            ExprList::new(
                children
                    .into_iter()
                    .map(|opt_rel_node_ref| {
                        Expr::from_rel_node(opt_rel_node_ref).expect("all children should be Expr")
                    })
                    .collect(),
            ),
        )
        .into_rel_node()
    }

    fn un_op(op_type: UnOpType, child: OptRelNodeRef) -> OptRelNodeRef {
        UnOpExpr::new(
            Expr::from_rel_node(child).expect("child should be an Expr"),
            op_type,
        )
        .into_rel_node()
    }

    fn in_list(col_ref_idx: u64, list: Vec<Value>, negated: bool) -> InListExpr {
        InListExpr::new(
            Expr::from_rel_node(col_ref(col_ref_idx)).unwrap(),
            ExprList::new(
                list.into_iter()
                    .map(|v| Expr::from_rel_node(cnst(v)).unwrap())
                    .collect_vec(),
            ),
            negated,
        )
    }

    /// The reason this isn't an associated function of PerColumnStats is because that would require
    ///   adding an empty() function to the trait definitions of MostCommonValues and Distribution,
    ///   which I wanted to avoid
    fn get_empty_per_col_stats() -> TestPerColumnStats {
        TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            0,
            0.0,
            TestDistribution::empty(),
        )
    }

    #[test]
    fn test_filtersel_const() {
        let cost_model = create_one_column_cost_model(get_empty_per_col_stats());
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(cnst(Value::Bool(true)), &vec![]),
            1.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(cnst(Value::Bool(false)), &vec![]),
            0.0
        );
    }

    #[test]
    fn test_filtersel_colref_eq_constint_in_mcv() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.3)]),
            0,
            0.0,
            TestDistribution::empty(),
        ));
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(1)));
        let expr_tree_rev = bin_op(BinOpType::Eq, cnst(Value::Int32(1)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.3
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.3
        );
    }

    #[test]
    fn test_filtersel_colref_eq_constint_not_in_mcv_no_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.2), (Value::Int32(3), 0.44)]),
            5,
            0.0,
            TestDistribution::empty(),
        ));
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(2)));
        let expr_tree_rev = bin_op(BinOpType::Eq, cnst(Value::Int32(2)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.12
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.12
        );
    }

    #[test]
    fn test_filtersel_colref_eq_constint_not_in_mcv_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.2), (Value::Int32(3), 0.44)]),
            5,
            0.03,
            TestDistribution::empty(),
        ));
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(2)));
        let expr_tree_rev = bin_op(BinOpType::Eq, cnst(Value::Int32(2)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.11
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.11
        );
    }

    /// I only have one test for NEQ since I'll assume that it uses the same underlying logic as EQ
    #[test]
    fn test_filtersel_colref_neq_constint_in_mcv() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.3)]),
            0,
            0.0,
            TestDistribution::empty(),
        ));
        let expr_tree = bin_op(BinOpType::Neq, col_ref(0), cnst(Value::Int32(1)));
        let expr_tree_rev = bin_op(BinOpType::Neq, cnst(Value::Int32(1)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            1.0 - 0.3
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            1.0 - 0.3
        );
    }

    #[test]
    fn test_filtersel_colref_leq_constint_no_mcvs_in_range() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.7
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.7
        );
    }

    #[test]
    fn test_filtersel_colref_leq_constint_no_mcvs_in_range_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.1,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.7 * 0.9
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.7 * 0.9
        );
    }

    #[test]
    fn test_filtersel_colref_leq_constint_with_mcvs_in_range_not_at_border() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (Value::Int32(6), 0.05),
                    (Value::Int32(10), 0.1),
                    (Value::Int32(17), 0.08),
                    (Value::Int32(25), 0.07),
                ]
                .into_iter()
                .collect(),
            },
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.85
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.85
        );
    }

    #[test]
    fn test_filtersel_colref_leq_constint_with_mcv_at_border() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![
                (Value::Int32(6), 0.05),
                (Value::Int32(10), 0.1),
                (Value::Int32(15), 0.08),
                (Value::Int32(25), 0.07),
            ]),
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.93
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.93
        );
    }

    #[test]
    fn test_filtersel_colref_lt_constint_no_mcvs_in_range() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Lt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.6
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.6
        );
    }

    #[test]
    fn test_filtersel_colref_lt_constint_no_mcvs_in_range_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            9, // 90% of the values aren't nulls since null_frac = 0.1. if there are 9 distinct non-null values, each will have 0.1 frequency
            0.1,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Lt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.6 * 0.9
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.6 * 0.9
        );
    }

    #[test]
    fn test_filtersel_colref_lt_constint_with_mcvs_in_range_not_at_border() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (Value::Int32(6), 0.05),
                    (Value::Int32(10), 0.1),
                    (Value::Int32(17), 0.08),
                    (Value::Int32(25), 0.07),
                ]
                .into_iter()
                .collect(),
            },
            11, // there are 4 MCVs which together add up to 0.3. With 11 total ndistinct, each remaining value has freq 0.1
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Lt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
        // TODO(phw2): make column_refs a function
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.75
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.75
        );
    }

    #[test]
    fn test_filtersel_colref_lt_constint_with_mcv_at_border() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (Value::Int32(6), 0.05),
                    (Value::Int32(10), 0.1),
                    (Value::Int32(15), 0.08),
                    (Value::Int32(25), 0.07),
                ]
                .into_iter()
                .collect(),
            },
            11, // there are 4 MCVs which together add up to 0.3. With 11 total ndistinct, each remaining value has freq 0.1
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Lt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.85
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            0.85
        );
    }

    /// I have fewer tests for GT since I'll assume that it uses the same underlying logic as LEQ
    /// The only interesting thing to test is that if there are nulls, those aren't included in GT
    #[test]
    fn test_filtersel_colref_gt_constint_no_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Gt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Leq, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            1.0 - 0.7
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            1.0 - 0.7
        );
    }

    #[test]
    fn test_filtersel_colref_gt_constint_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.1,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Gt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Leq, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            (1.0 - 0.7) * 0.9
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            (1.0 - 0.7) * 0.9
        );
    }

    /// As with above, I have one test without nulls and one test with nulls
    #[test]
    fn test_filtersel_colref_geq_constint_no_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Geq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Lt, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            1.0 - 0.6
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            1.0 - 0.6
        );
    }

    #[test]
    fn test_filtersel_colref_geq_constint_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            9, // 90% of the values aren't nulls since null_frac = 0.1. if there are 9 distinct non-null values, each will have 0.1 frequency
            0.1,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Geq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Lt, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        // we have to add 0.1 since it's Geq
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            (1.0 - 0.7 + 0.1) * 0.9
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            (1.0 - 0.7 + 0.1) * 0.9
        );
    }

    #[test]
    fn test_filtersel_and() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (Value::Int32(1), 0.3),
                    (Value::Int32(5), 0.5),
                    (Value::Int32(8), 0.2),
                ]
                .into_iter()
                .collect(),
            },
            0,
            0.0,
            TestDistribution::empty(),
        ));
        let eq1 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(1)));
        let eq5 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(5)));
        let eq8 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(8)));
        let expr_tree = log_op(LogOpType::And, vec![eq1.clone(), eq5.clone(), eq8.clone()]);
        let expr_tree_shift1 = log_op(LogOpType::And, vec![eq5.clone(), eq8.clone(), eq1.clone()]);
        let expr_tree_shift2 = log_op(LogOpType::And, vec![eq8.clone(), eq1.clone(), eq5.clone()]);
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.03
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_shift1, &column_refs),
            0.03
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_shift2, &column_refs),
            0.03
        );
    }

    #[test]
    fn test_filtersel_or() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues {
                mcvs: vec![
                    (Value::Int32(1), 0.3),
                    (Value::Int32(5), 0.5),
                    (Value::Int32(8), 0.2),
                ]
                .into_iter()
                .collect(),
            },
            0,
            0.0,
            TestDistribution::empty(),
        ));
        let eq1 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(1)));
        let eq5 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(5)));
        let eq8 = bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(8)));
        let expr_tree = log_op(LogOpType::Or, vec![eq1.clone(), eq5.clone(), eq8.clone()]);
        let expr_tree_shift1 = log_op(LogOpType::Or, vec![eq5.clone(), eq8.clone(), eq1.clone()]);
        let expr_tree_shift2 = log_op(LogOpType::Or, vec![eq8.clone(), eq1.clone(), eq5.clone()]);
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.72
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_shift1, &column_refs),
            0.72
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_shift2, &column_refs),
            0.72
        );
    }

    #[test]
    fn test_filtersel_not_no_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.3)]),
            0,
            0.0,
            TestDistribution::empty(),
        ));
        let expr_tree = un_op(
            UnOpType::Not,
            bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(1))),
        );
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.7
        );
    }

    #[test]
    fn test_filtersel_not_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.3)]),
            0,
            0.1,
            TestDistribution::empty(),
        ));
        let expr_tree = un_op(
            UnOpType::Not,
            bin_op(BinOpType::Eq, col_ref(0), cnst(Value::Int32(1))),
        );
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        // not doesn't care about nulls. it just reverses the selectivity
        // for instance, != _will not_ include nulls but "NOT ==" _will_ include nulls
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            0.7
        );
    }

    #[test]
    fn test_filtersel_in_list() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.8), (Value::Int32(2), 0.2)]),
            2,
            0.0,
            TestDistribution::empty(),
        ));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_in_list_selectivity(
                &in_list(0, vec![Value::Int32(1)], false),
                &column_refs
            ),
            0.8
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_in_list_selectivity(
                &in_list(0, vec![Value::Int32(1), Value::Int32(2)], false),
                &column_refs
            ),
            1.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_in_list_selectivity(
                &in_list(0, vec![Value::Int32(3)], false),
                &column_refs
            ),
            0.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_in_list_selectivity(
                &in_list(0, vec![Value::Int32(1)], true),
                &column_refs
            ),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_in_list_selectivity(
                &in_list(0, vec![Value::Int32(1), Value::Int32(2)], true),
                &column_refs
            ),
            0.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_in_list_selectivity(
                &in_list(0, vec![Value::Int32(3)], true),
                &column_refs
            ),
            1.0
        );
    }

    /// A wrapper around get_join_selectivity_from_expr_tree that extracts the table row counts from the cost model
    fn test_get_join_selectivity(
        cost_model: &TestOptCostModel,
        reverse_tables: bool,
        join_typ: JoinType,
        expr_tree: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
    ) -> f64 {
        let table1_row_cnt = cost_model.per_table_stats_map[TABLE1_NAME].row_cnt as f64;
        let table2_row_cnt = cost_model.per_table_stats_map[TABLE2_NAME].row_cnt as f64;
        if !reverse_tables {
            cost_model.get_join_selectivity_from_expr_tree(
                join_typ,
                expr_tree,
                column_refs,
                table1_row_cnt,
                table2_row_cnt,
            )
        } else {
            cost_model.get_join_selectivity_from_expr_tree(
                join_typ,
                expr_tree,
                column_refs,
                table2_row_cnt,
                table1_row_cnt,
            )
        }
    }

    #[test]
    fn test_joinsel_inner_const() {
        let cost_model = create_one_column_cost_model(get_empty_per_col_stats());
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_join_selectivity_from_expr_tree(
                JoinType::Inner,
                cnst(Value::Bool(true)),
                &vec![],
                f64::NAN,
                f64::NAN
            ),
            1.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_join_selectivity_from_expr_tree(
                JoinType::Inner,
                cnst(Value::Bool(false)),
                &vec![],
                f64::NAN,
                f64::NAN
            ),
            0.0
        );
    }

    #[test]
    fn test_joinsel_inner_oncond() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                TestDistribution::empty(),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                TestDistribution::empty(),
            ),
        );
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let expr_tree_rev = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let column_refs = vec![
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &column_refs
            ),
            0.2
        );
    }

    #[test]
    fn test_joinsel_inner_and_of_onconds() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                TestDistribution::empty(),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                TestDistribution::empty(),
            ),
        );
        let eq0and1 = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let eq1and0 = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let expr_tree = log_op(LogOpType::And, vec![eq0and1.clone(), eq1and0.clone()]);
        let expr_tree_rev = log_op(LogOpType::And, vec![eq1and0.clone(), eq0and1.clone()]);
        let column_refs = vec![
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            0.04
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &column_refs
            ),
            0.04
        );
    }

    #[test]
    fn test_joinsel_inner_and_of_oncond_and_filter() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                TestDistribution::empty(),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                TestDistribution::empty(),
            ),
        );
        let eq0and1 = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let eq100 = bin_op(BinOpType::Eq, col_ref(1), cnst(Value::Int32(100)));
        let expr_tree = log_op(LogOpType::And, vec![eq0and1.clone(), eq100.clone()]);
        let expr_tree_rev = log_op(LogOpType::And, vec![eq100.clone(), eq0and1.clone()]);
        let column_refs = vec![
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            0.05
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &column_refs
            ),
            0.05
        );
    }

    #[test]
    fn test_joinsel_inner_and_of_filters() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                TestDistribution::empty(),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                TestDistribution::empty(),
            ),
        );
        let neq12 = bin_op(BinOpType::Neq, col_ref(0), cnst(Value::Int32(12)));
        let eq100 = bin_op(BinOpType::Eq, col_ref(1), cnst(Value::Int32(100)));
        let expr_tree = log_op(LogOpType::And, vec![neq12.clone(), eq100.clone()]);
        let expr_tree_rev = log_op(LogOpType::And, vec![eq100.clone(), neq12.clone()]);
        let column_refs = vec![
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev,
                &column_refs
            ),
            0.2
        );
    }

    #[test]
    fn test_joinsel_inner_colref_eq_colref_same_table_is_not_oncond() {
        let cost_model = create_two_table_cost_model(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                TestDistribution::empty(),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                TestDistribution::empty(),
            ),
        );
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(0));
        let column_refs = vec![
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
        ];
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(&cost_model, false, JoinType::Inner, expr_tree, &column_refs),
            DEFAULT_EQ_SEL
        );
    }

    // We don't test joinsel or with oncond because if there is an oncond (on condition), the top-level operator must be an AND

    /// I made this helper function to avoid copying all eight lines over and over
    fn assert_joinsel_outer_selectivities(
        cost_model: &TestOptCostModel,
        expr_tree: OptRelNodeRef,
        expr_tree_rev: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
        expected_table1_outer_sel: f64,
        expected_table2_outer_sel: f64,
    ) {
        // all table 1 outer combinations
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::LeftOuter,
                expr_tree.clone(),
                column_refs
            ),
            expected_table1_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::LeftOuter,
                expr_tree_rev.clone(),
                column_refs
            ),
            expected_table1_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::RightOuter,
                expr_tree.clone(),
                column_refs
            ),
            expected_table1_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::RightOuter,
                expr_tree_rev.clone(),
                column_refs
            ),
            expected_table1_outer_sel
        );
        // all table 2 outer combinations
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::LeftOuter,
                expr_tree.clone(),
                column_refs
            ),
            expected_table2_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                true,
                JoinType::LeftOuter,
                expr_tree_rev.clone(),
                column_refs
            ),
            expected_table2_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::RightOuter,
                expr_tree.clone(),
                column_refs
            ),
            expected_table2_outer_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                cost_model,
                false,
                JoinType::RightOuter,
                expr_tree_rev.clone(),
                column_refs
            ),
            expected_table2_outer_sel
        );
    }

    /// Unique oncond means an oncondition on columns which are unique in both tables
    /// There's only one case if both columns are unique and have different row counts: the inner will be < 1 / row count
    ///   of one table and = 1 / row count of another
    #[test]
    fn test_joinsel_outer_unique_oncond() {
        let cost_model = create_two_table_cost_model_custom_row_cnts(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                TestDistribution::empty(),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                TestDistribution::empty(),
            ),
            5,
            4,
        );
        // the left/right of the join refers to the tables, not the order of columns in the predicate
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let expr_tree_rev = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let column_refs = vec![
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
        ];
        // sanity check the expected inner sel
        let expected_inner_sel = 0.2;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_joinsel_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_rev,
            &column_refs,
            0.25,
            0.2,
        );
    }

    /// Non-unique oncond means the column is not unique in either table
    /// Inner always >= row count means that the inner join result is >= 1 / the row count of both tables
    #[test]
    fn test_joinsel_outer_nonunique_oncond_inner_always_geq_rowcnt() {
        let cost_model = create_two_table_cost_model_custom_row_cnts(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                5,
                0.0,
                TestDistribution::empty(),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                TestDistribution::empty(),
            ),
            10,
            8,
        );
        // the left/right of the join refers to the tables, not the order of columns in the predicate
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let expr_tree_rev = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let column_refs = vec![
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
        ];
        // sanity check the expected inner sel
        let expected_inner_sel = 0.2;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_joinsel_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_rev,
            &column_refs,
            0.2,
            0.2,
        );
    }

    /// Non-unique oncond means the column is not unique in either table
    /// Inner sometimes < row count means that the inner join result < 1 / the row count of exactly one table.
    ///   Note that without a join filter, it's impossible to be less than the row count of both tables
    #[test]
    fn test_joinsel_outer_nonunique_oncond_inner_sometimes_lt_rowcnt() {
        let cost_model = create_two_table_cost_model_custom_row_cnts(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                10,
                0.0,
                TestDistribution::empty(),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                2,
                0.0,
                TestDistribution::empty(),
            ),
            20,
            4,
        );
        // the left/right of the join refers to the tables, not the order of columns in the predicate
        let expr_tree = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let expr_tree_rev = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let column_refs = vec![
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
        ];
        // sanity check the expected inner sel
        let expected_inner_sel = 0.1;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_rev.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_joinsel_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_rev,
            &column_refs,
            0.25,
            0.1,
        );
    }

    /// Unique oncond means an oncondition on columns which are unique in both tables
    /// Filter means we're adding a join filter
    /// There's only one case if both columns are unique and there's a filter: the inner will be < 1 / row count of both tables
    #[test]
    fn test_joinsel_outer_unique_oncond_filter() {
        let cost_model = create_two_table_cost_model_custom_row_cnts(
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                50,
                0.0,
                TestDistribution::new(vec![(Value::Int32(128), 0.4)]),
            ),
            TestPerColumnStats::new(
                TestMostCommonValues::empty(),
                4,
                0.0,
                TestDistribution::empty(),
            ),
            50,
            4,
        );
        // the left/right of the join refers to the tables, not the order of columns in the predicate
        let eq0and1 = bin_op(BinOpType::Eq, col_ref(0), col_ref(1));
        let eq1and0 = bin_op(BinOpType::Eq, col_ref(1), col_ref(0));
        let filter = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(128)));
        let expr_tree = log_op(LogOpType::And, vec![eq0and1, filter.clone()]);
        // inner rev means its the inner expr (the eq op) whose children are being reversed, as opposed to the and op
        let expr_tree_inner_rev = log_op(LogOpType::And, vec![eq1and0, filter.clone()]);
        let column_refs = vec![
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE1_NAME),
                col_idx: 0,
            },
            ColumnRef::BaseTableColumnRef {
                table: String::from(TABLE2_NAME),
                col_idx: 0,
            },
        ];
        // sanity check the expected inner sel
        let expected_inner_sel = 0.008;
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        assert_approx_eq::assert_approx_eq!(
            test_get_join_selectivity(
                &cost_model,
                false,
                JoinType::Inner,
                expr_tree_inner_rev.clone(),
                &column_refs
            ),
            expected_inner_sel
        );
        // check the outer sels
        assert_joinsel_outer_selectivities(
            &cost_model,
            expr_tree,
            expr_tree_inner_rev,
            &column_refs,
            0.25,
            0.02,
        );
    }

    // I didn't test any non-unique cases with filter. The non-unique tests without filter should cover that
}
