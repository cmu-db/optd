mod agg;
mod filter;
mod join;
mod limit;
pub mod stats;

use optd_datafusion_repr::properties::column_ref::{BaseTableColumnRef, ColumnRef};
use serde::{de::DeserializeOwned, Serialize};

use super::adv_stats::stats::{
    BaseTableStats, ColumnCombValueStats, Distribution, MostCommonValues,
};

pub struct AdvStats<
    M: MostCommonValues + Serialize + DeserializeOwned,
    D: Distribution + Serialize + DeserializeOwned,
> {
    pub(crate) per_table_stats_map: BaseTableStats<M, D>,
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

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > AdvStats<M, D>
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
    use optd_datafusion_repr::plan_nodes::{
        BinOpExpr, BinOpType, CastExpr, ColumnRefExpr, ConstantExpr, Expr, ExprList, InListExpr,
        LikeExpr, LogOpExpr, LogOpType, OptRelNode, OptRelNodeRef, UnOpExpr, UnOpType,
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    use super::{stats::*, *};
    pub type TestPerColumnStats = ColumnCombValueStats<TestMostCommonValues, TestDistribution>;
    pub type TestOptCostModel = AdvStats<TestMostCommonValues, TestDistribution>;

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
        AdvStats::new(
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
        AdvStats::new(
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
        AdvStats::new(
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
        AdvStats::new(
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
    pub(crate) fn get_empty_per_col_stats() -> TestPerColumnStats {
        TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            0,
            0.0,
            Some(TestDistribution::empty()),
        )
    }
}
