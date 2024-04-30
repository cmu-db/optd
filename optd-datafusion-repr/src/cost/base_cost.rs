mod agg;
mod filter;
mod join;
mod limit;
pub(crate) mod stats;

use crate::{
    plan_nodes::OptRelNodeTyp,
    properties::column_ref::{BaseTableColumnRef, ColumnRef},
};
use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::{Cost, CostModel},
    rel_node::{RelNode, RelNodeTyp, Value},
};
use serde::{de::DeserializeOwned, Serialize};

use super::base_cost::stats::{
    BaseTableStats, ColumnCombValueStats, Distribution, MostCommonValues,
};

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

pub struct OptCostModel<
    M: MostCommonValues + Serialize + DeserializeOwned,
    D: Distribution + Serialize + DeserializeOwned,
> {
    per_table_stats_map: BaseTableStats<M, D>,
}

// Default statistics. All are from selfuncs.h in Postgres unless specified otherwise
// Default selectivity estimate for equalities such as "A = b"
const DEFAULT_EQ_SEL: f64 = 0.005;
// Default selectivity estimate for inequalities such as "A < b"
const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
// Default n-distinct estimate for derived columns or columns lacking statistics
const DEFAULT_NUM_DISTINCT: u64 = 200;
// Default selectivity if we have no information
const DEFAULT_UNK_SEL: f64 = 0.005;

// A placeholder for unimplemented!() for codepaths which are accessed by plannertest
const UNIMPLEMENTED_SEL: f64 = 0.01;

pub const ROW_COUNT: usize = 1;
pub const COMPUTE_COST: usize = 2;
pub const IO_COST: usize = 3;

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > OptCostModel<M, D>
{
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

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > CostModel<OptRelNodeTyp> for OptCostModel<M, D>
{
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
                let row_cnt = self
                    .per_table_stats_map
                    .get(table.as_ref())
                    .map(|per_table_stats| per_table_stats.row_cnt)
                    .unwrap_or(1) as f64;
                Self::cost(row_cnt, 0.0, row_cnt)
            }
            OptRelNodeTyp::PhysicalEmptyRelation => Self::cost(0.5, 0.01, 0.0),
            OptRelNodeTyp::PhysicalLimit => Self::get_limit_cost(children, context, optimizer),
            OptRelNodeTyp::PhysicalFilter => self.get_filter_cost(children, context, optimizer),
            OptRelNodeTyp::PhysicalNestedLoopJoin(join_typ) => {
                self.get_nlj_cost(*join_typ, children, context, optimizer)
            }
            OptRelNodeTyp::PhysicalProjection => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                let (_, compute_cost, _) = Self::cost_tuple(&children[1]);
                Self::cost(row_cnt, compute_cost * row_cnt, 0.0)
            }
            OptRelNodeTyp::PhysicalHashJoin(join_typ) => {
                self.get_hash_join_cost(*join_typ, children, context, optimizer)
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

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > OptCostModel<M, D>
{
    pub fn new(per_table_stats_map: BaseTableStats<M, D>) -> Self {
        Self {
            per_table_stats_map,
        }
    }

    fn get_single_column_stats_from_col_ref(
        &self,
        col_ref: &ColumnRef,
    ) -> Option<&ColumnCombValueStats<M, D>> {
        if let ColumnRef::BaseTableColumnRef(BaseTableColumnRef { table, col_idx }) = col_ref {
            self.get_column_comb_stats(table, &[*col_idx])
        } else {
            None
        }
    }

    fn get_column_comb_stats(
        &self,
        table: &str,
        col_comb: &[usize],
    ) -> Option<&ColumnCombValueStats<M, D>> {
        self.per_table_stats_map
            .get(table)
            .and_then(|per_table_stats| per_table_stats.column_comb_stats.get(col_comb))
    }
}

/// I thought about using the system's own parser and planner to generate these expression trees, but
/// this is not currently feasible because it would create a cyclic dependency between optd-datafusion-bridge
/// and optd-datafusion-repr
#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use itertools::Itertools;
    use optd_core::rel_node::Value;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    use crate::{
        cost::base_cost::stats::*,
        plan_nodes::{
            BinOpExpr, BinOpType, CastExpr, ColumnRefExpr, ConstantExpr, Expr, ExprList,
            InListExpr, LikeExpr, LogOpExpr, LogOpType, OptRelNode, OptRelNodeRef, UnOpExpr,
            UnOpType,
        },
    };

    use super::*;
    pub type TestPerColumnStats = ColumnCombValueStats<TestMostCommonValues, TestDistribution>;
    pub type TestOptCostModel = OptCostModel<TestMostCommonValues, TestDistribution>;

    #[derive(Serialize, Deserialize)]
    pub struct TestMostCommonValues {
        pub mcvs: HashMap<Vec<Option<Value>>, f64>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct TestDistribution {
        cdfs: HashMap<Value, f64>,
    }

    impl TestMostCommonValues {
        pub fn new(mcvs_vec: Vec<(Value, f64)>) -> Self {
            Self {
                mcvs: mcvs_vec
                    .into_iter()
                    .map(|(v, freq)| (vec![Some(v)], freq))
                    .collect(),
            }
        }

        pub fn empty() -> Self {
            TestMostCommonValues::new(vec![])
        }
    }

    impl MostCommonValues for TestMostCommonValues {
        fn freq(&self, value: &ColumnCombValue) -> Option<f64> {
            self.mcvs.get(value).copied()
        }

        fn total_freq(&self) -> f64 {
            self.mcvs.values().sum()
        }

        fn freq_over_pred(&self, pred: Box<dyn Fn(&ColumnCombValue) -> bool>) -> f64 {
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
        pub fn new(cdfs_vec: Vec<(Value, f64)>) -> Self {
            Self {
                cdfs: cdfs_vec.into_iter().collect(),
            }
        }

        pub fn empty() -> Self {
            TestDistribution::new(vec![])
        }
    }

    impl Distribution for TestDistribution {
        fn cdf(&self, value: &Value) -> f64 {
            *self.cdfs.get(value).unwrap_or(&0.0)
        }
    }

    pub const TABLE1_NAME: &str = "table1";
    pub const TABLE2_NAME: &str = "table2";
    pub const TABLE3_NAME: &str = "table3";
    pub const TABLE4_NAME: &str = "table4";

    // one column is sufficient for all filter selectivity tests
    pub fn create_one_column_cost_model(per_column_stats: TestPerColumnStats) -> TestOptCostModel {
        OptCostModel::new(
            vec![(
                String::from(TABLE1_NAME),
                TableStats::new(100, vec![(vec![0], per_column_stats)].into_iter().collect()),
            )]
            .into_iter()
            .collect(),
        )
    }

    /// Create a cost model with two columns, one for each table. Each column has 100 values.
    pub fn create_two_table_cost_model(
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

    /// Create a cost model with three columns, one for each table. Each column has 100 values.
    pub fn create_three_table_cost_model(
        tbl1_per_column_stats: TestPerColumnStats,
        tbl2_per_column_stats: TestPerColumnStats,
        tbl3_per_column_stats: TestPerColumnStats,
    ) -> TestOptCostModel {
        OptCostModel::new(
            vec![
                (
                    String::from(TABLE1_NAME),
                    TableStats::new(
                        100,
                        vec![(vec![0], tbl1_per_column_stats)].into_iter().collect(),
                    ),
                ),
                (
                    String::from(TABLE2_NAME),
                    TableStats::new(
                        100,
                        vec![(vec![0], tbl2_per_column_stats)].into_iter().collect(),
                    ),
                ),
                (
                    String::from(TABLE3_NAME),
                    TableStats::new(
                        100,
                        vec![(vec![0], tbl3_per_column_stats)].into_iter().collect(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
        )
    }

    /// Create a cost model with three columns, one for each table. Each column has 100 values.
    pub fn create_four_table_cost_model(
        tbl1_per_column_stats: TestPerColumnStats,
        tbl2_per_column_stats: TestPerColumnStats,
        tbl3_per_column_stats: TestPerColumnStats,
        tbl4_per_column_stats: TestPerColumnStats,
    ) -> TestOptCostModel {
        OptCostModel::new(
            vec![
                (
                    String::from(TABLE1_NAME),
                    TableStats::new(
                        100,
                        vec![(vec![0], tbl1_per_column_stats)].into_iter().collect(),
                    ),
                ),
                (
                    String::from(TABLE2_NAME),
                    TableStats::new(
                        100,
                        vec![(vec![0], tbl2_per_column_stats)].into_iter().collect(),
                    ),
                ),
                (
                    String::from(TABLE3_NAME),
                    TableStats::new(
                        100,
                        vec![(vec![0], tbl3_per_column_stats)].into_iter().collect(),
                    ),
                ),
                (
                    String::from(TABLE4_NAME),
                    TableStats::new(
                        100,
                        vec![(vec![0], tbl4_per_column_stats)].into_iter().collect(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
        )
    }

    /// We need custom row counts because some join algorithms rely on the row cnt
    pub fn create_two_table_cost_model_custom_row_cnts(
        tbl1_per_column_stats: TestPerColumnStats,
        tbl2_per_column_stats: TestPerColumnStats,
        tbl1_row_cnt: usize,
        tbl2_row_cnt: usize,
    ) -> TestOptCostModel {
        OptCostModel::new(
            vec![
                (
                    String::from(TABLE1_NAME),
                    TableStats::new(
                        tbl1_row_cnt,
                        vec![(vec![0], tbl1_per_column_stats)].into_iter().collect(),
                    ),
                ),
                (
                    String::from(TABLE2_NAME),
                    TableStats::new(
                        tbl2_row_cnt,
                        vec![(vec![0], tbl2_per_column_stats)].into_iter().collect(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
        )
    }

    pub fn col_ref(idx: u64) -> OptRelNodeRef {
        // this conversion is always safe because idx was originally a usize
        let idx_as_usize = idx as usize;
        ColumnRefExpr::new(idx_as_usize).into_rel_node()
    }

    pub fn cnst(value: Value) -> OptRelNodeRef {
        ConstantExpr::new(value).into_rel_node()
    }

    pub fn cast(child: OptRelNodeRef, cast_type: DataType) -> OptRelNodeRef {
        CastExpr::new(
            Expr::from_rel_node(child).expect("child should be an Expr"),
            cast_type,
        )
        .into_rel_node()
    }

    pub fn bin_op(op_type: BinOpType, left: OptRelNodeRef, right: OptRelNodeRef) -> OptRelNodeRef {
        BinOpExpr::new(
            Expr::from_rel_node(left).expect("left should be an Expr"),
            Expr::from_rel_node(right).expect("right should be an Expr"),
            op_type,
        )
        .into_rel_node()
    }

    pub fn log_op(op_type: LogOpType, children: Vec<OptRelNodeRef>) -> OptRelNodeRef {
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

    pub fn un_op(op_type: UnOpType, child: OptRelNodeRef) -> OptRelNodeRef {
        UnOpExpr::new(
            Expr::from_rel_node(child).expect("child should be an Expr"),
            op_type,
        )
        .into_rel_node()
    }

    pub fn in_list(col_ref_idx: u64, list: Vec<Value>, negated: bool) -> InListExpr {
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

    pub fn like(col_ref_idx: u64, pattern: &str, negated: bool) -> LikeExpr {
        LikeExpr::new(
            negated,
            false,
            Expr::from_rel_node(col_ref(col_ref_idx)).unwrap(),
            Expr::from_rel_node(cnst(Value::String(pattern.into()))).unwrap(),
        )
    }

    /// The reason this isn't an associated function of PerColumnStats is because that would require
    ///   adding an empty() function to the trait definitions of MostCommonValues and Distribution,
    ///   which I wanted to avoid
    pub fn get_empty_per_col_stats() -> TestPerColumnStats {
        TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            0,
            0.0,
            Some(TestDistribution::empty()),
        )
    }
}
