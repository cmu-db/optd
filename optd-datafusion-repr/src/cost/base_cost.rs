use std::ops::Bound;
use std::{collections::HashMap, sync::Arc};

use crate::plan_nodes::{
    BinOpType, ColumnRefExpr, ConstantExpr, ConstantType, Expr, ExprList, LogOpExpr, LogOpType,
    OptRelNode, UnOpType,
};
use crate::properties::column_ref::{ColumnRefPropertyBuilder, GroupColumnRefs};
use crate::{
    plan_nodes::{JoinType, OptRelNodeRef, OptRelNodeTyp},
    properties::column_ref::ColumnRef,
};
use arrow_schema::{ArrowError, DataType};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int8Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
    UInt16Array, UInt32Array, UInt8Array,
};
use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::{Cost, CostModel},
    rel_node::{RelNode, RelNodeTyp, Value},
};
use optd_gungnir::stats::hyperloglog::{self, HyperLogLog};
use optd_gungnir::stats::tdigest::{self, TDigest};
use optd_gungnir::utils::arith_encoder;
use serde::{Deserialize, Serialize};

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

pub type BaseTableStats<M, D> = HashMap<String, PerTableStats<M, D>>;

// The "standard" concrete types that optd currently uses
// All of optd (except unit tests) must use the same types
pub type DataFusionMostCommonValues = MockMostCommonValues;
pub type DataFusionDistribution = TDigest;
pub type DataFusionBaseTableStats =
    BaseTableStats<DataFusionMostCommonValues, DataFusionDistribution>;
pub type DataFusionPerTableStats =
    PerTableStats<DataFusionMostCommonValues, DataFusionDistribution>;
pub type DataFusionPerColumnStats =
    PerColumnStats<DataFusionMostCommonValues, DataFusionDistribution>;

pub struct OptCostModel<M: MostCommonValues, D: Distribution> {
    per_table_stats_map: BaseTableStats<M, D>,
}

#[derive(Serialize, Deserialize)]
pub struct MockMostCommonValues {
    mcvs: HashMap<Value, f64>,
}

impl MockMostCommonValues {
    pub fn empty() -> Self {
        MockMostCommonValues {
            mcvs: HashMap::new(),
        }
    }
}

impl MostCommonValues for MockMostCommonValues {
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

#[derive(Serialize, Deserialize)]
pub struct PerTableStats<M: MostCommonValues, D: Distribution> {
    row_cnt: usize,
    // This is a Vec of Options instead of just a Vec because some columns may not have stats
    //   due to their type being non-comparable.
    // Further, I chose to represent it as a Vec of Options instead of a HashMap because a Vec
    //   of Options clearly differentiates between two different failure modes: "out-of-bounds
    //   access" and "column has no stats".
    per_column_stats_vec: Vec<Option<PerColumnStats<M, D>>>,
}

impl DataFusionPerTableStats {
    pub fn from_record_batches<I: IntoIterator<Item = Result<RecordBatch, ArrowError>>>(
        batch_iter: RecordBatchIterator<I>,
    ) -> anyhow::Result<Self> {
        let schema = batch_iter.schema();
        let col_types = schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect_vec();
        let col_cnt = col_types.len();

        let mut row_cnt = 0;
        let mut mcvs = col_types
            .iter()
            .map(|col_type| {
                if Self::is_type_supported(col_type) {
                    Some(MockMostCommonValues::empty())
                } else {
                    None
                }
            })
            .collect_vec();
        let mut distr = col_types
            .iter()
            .map(|col_type| {
                if Self::is_type_supported(col_type) {
                    Some(TDigest::new(tdigest::DEFAULT_COMPRESSION))
                } else {
                    None
                }
            })
            .collect_vec();
        let mut hlls = vec![HyperLogLog::new(hyperloglog::DEFAULT_PRECISION); col_cnt];
        let mut null_cnt = vec![0; col_cnt];

        for batch in batch_iter {
            let batch = batch?;
            row_cnt += batch.num_rows();

            // Enumerate the columns.
            for (i, col) in batch.columns().iter().enumerate() {
                let col_type = &col_types[i];
                if Self::is_type_supported(col_type) {
                    // Update null cnt.
                    null_cnt[i] += col.null_count();

                    Self::generate_stats_for_column(col, col_type, &mut distr[i], &mut hlls[i]);
                }
            }
        }

        // Assemble the per-column stats.
        let mut per_column_stats_vec = Vec::with_capacity(col_cnt);
        for i in 0..col_cnt {
            per_column_stats_vec.push(if Self::is_type_supported(&col_types[i]) {
                Some(PerColumnStats::new(
                    mcvs[i].take().unwrap(),
                    hlls[i].n_distinct(),
                    null_cnt[i] as f64 / row_cnt as f64,
                    distr[i].take().unwrap(),
                ))
            } else {
                None
            });
        }
        Ok(Self {
            row_cnt,
            per_column_stats_vec,
        })
    }

    fn is_type_supported(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
        )
    }

    /// Generate statistics for a column.
    fn generate_stats_for_column(
        col: &Arc<dyn Array>,
        col_type: &DataType,
        distr: &mut Option<TDigest>,
        hll: &mut HyperLogLog,
    ) {
        macro_rules! generate_stats_for_col {
            ({ $col:expr, $distr:expr, $hll:expr, $array_type:path, $to_f64:ident }) => {{
                let array = $col.as_any().downcast_ref::<$array_type>().unwrap();
                // Filter out `None` values.
                let values = array.iter().filter_map(|x| x).collect::<Vec<_>>();

                // Update distribution.
                *$distr = {
                    let mut f64_values = values.iter().map(|x| $to_f64(*x)).collect::<Vec<_>>();
                    Some($distr.take().unwrap().merge_values(&mut f64_values))
                };

                // Update hll.
                $hll.aggregate(&values);
            }};
        }

        /// Convert a value to f64 with no out of range or precision loss.
        fn to_f64_safe<T: Into<f64>>(val: T) -> f64 {
            val.into()
        }

        /// Convert i128 to f64 with possible precision loss.
        ///
        /// Note: optd represents decimal with the significand as f64 (see `ConstantExpr::decimal`).
        /// For instance 0.04 of type `Decimal128(15, 2)` is just 4.0, the type information
        /// is discarded. Therefore we must use the significand to generate the statistics.
        fn i128_to_f64(val: i128) -> f64 {
            val as f64
        }

        fn str_to_f64(string: &str) -> f64 {
            arith_encoder::encode(string)
        }

        match col_type {
            DataType::Boolean => {
                generate_stats_for_col!({ col, distr, hll, BooleanArray, to_f64_safe })
            }
            DataType::Int8 => {
                generate_stats_for_col!({ col, distr, hll, Int8Array, to_f64_safe })
            }
            DataType::Int16 => {
                generate_stats_for_col!({ col, distr, hll, Int16Array, to_f64_safe })
            }
            DataType::Int32 => {
                generate_stats_for_col!({ col, distr, hll, Int32Array, to_f64_safe })
            }
            DataType::UInt8 => {
                generate_stats_for_col!({ col, distr, hll, UInt8Array, to_f64_safe })
            }
            DataType::UInt16 => {
                generate_stats_for_col!({ col, distr, hll, UInt16Array, to_f64_safe })
            }
            DataType::UInt32 => {
                generate_stats_for_col!({ col, distr, hll, UInt32Array, to_f64_safe })
            }
            DataType::Float32 => {
                generate_stats_for_col!({ col, distr, hll, Float32Array, to_f64_safe })
            }
            DataType::Float64 => {
                generate_stats_for_col!({ col, distr, hll, Float64Array, to_f64_safe })
            }
            DataType::Date32 => {
                generate_stats_for_col!({ col, distr, hll, Date32Array, to_f64_safe })
            }
            DataType::Decimal128(_, _) => {
                generate_stats_for_col!({ col, distr, hll, Decimal128Array, i128_to_f64 })
            }
            DataType::Utf8 => {
                generate_stats_for_col!({ col, distr, hll, StringArray, str_to_f64 })
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct PerColumnStats<M: MostCommonValues, D: Distribution> {
    // even if nulls are the most common, they cannot appear in mcvs
    mcvs: M,

    // ndistinct _does_ include the values in mcvs
    // ndistinct _does not_ include nulls
    ndistinct: u64,

    // postgres uses null_frac instead of something like "num_nulls" so we'll follow suit
    // my guess for why they use null_frac is because we only ever use the fraction of nulls, not the #
    null_frac: f64,

    // distribution _does not_ include the values in mcvs
    // distribution _does not_ include nulls
    distr: D,
}

impl<M: MostCommonValues, D: Distribution> PerColumnStats<M, D> {
    pub fn new(mcvs: M, ndistinct: u64, null_frac: f64, distr: D) -> Self {
        Self {
            mcvs,
            ndistinct,
            null_frac,
            distr,
        }
    }
}

pub trait MostCommonValues: 'static + Send + Sync {
    // it is true that we could just expose freq_over_pred() and use that for freq() and total_freq()
    // however, freq() and total_freq() each have potential optimizations (freq() is O(1) instead of
    //     O(n) and total_freq() can be cached)
    // additionally, it makes sense to return an Option<f64> for freq() instead of just 0 if value doesn't exist
    // thus, I expose three different functions
    fn freq(&self, value: &Value) -> Option<f64>;
    fn total_freq(&self) -> f64;
    fn freq_over_pred(&self, pred: Box<dyn Fn(&Value) -> bool>) -> f64;

    // returns the # of entries (i.e. value + freq) in the most common values structure
    fn cnt(&self) -> usize;
}

// A more general interface meant to perform the task of a histogram
// This more general interface is still compatible with histograms but allows
//     more powerful statistics like TDigest
pub trait Distribution: 'static + Send + Sync {
    // Give the probability of a random value sampled from the distribution being <= `value`
    fn cdf(&self, value: &Value) -> f64;
}

pub const ROW_COUNT: usize = 1;
pub const COMPUTE_COST: usize = 2;
pub const IO_COST: usize = 3;

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
            OptRelNodeTyp::PhysicalLimit => {
                let (row_cnt, compute_cost, _) = Self::cost_tuple(&children[0]);
                let row_cnt = if let (Some(context), Some(optimizer)) = (context, optimizer) {
                    let mut fetch_expr =
                        optimizer.get_all_group_bindings(context.children_group_ids[2], false);
                    assert!(
                        fetch_expr.len() == 1,
                        "fetch expression should be the only expr in the group"
                    );
                    let fetch_expr = fetch_expr.pop().unwrap();
                    assert!(
                        matches!(
                            fetch_expr.typ,
                            OptRelNodeTyp::Constant(ConstantType::UInt64)
                        ),
                        "fetch type can only be UInt64"
                    );
                    let fetch = ConstantExpr::from_rel_node(fetch_expr)
                        .unwrap()
                        .value()
                        .as_u64();
                    // u64::MAX represents None
                    if fetch == u64::MAX {
                        row_cnt
                    } else {
                        row_cnt.min(fetch as f64)
                    }
                } else {
                    (row_cnt * DEFAULT_UNK_SEL).max(1.0)
                };
                Self::cost(row_cnt, compute_cost, 0.0)
            }
            OptRelNodeTyp::PhysicalFilter => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                let (_, compute_cost, _) = Self::cost_tuple(&children[1]);
                let selectivity = if let (Some(context), Some(optimizer)) = (context, optimizer) {
                    let column_refs = optimizer
                        .get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
                    let expr_group_id = context.children_group_ids[1];
                    let expr_trees = optimizer.get_all_group_bindings(expr_group_id, false);
                    // there may be more than one expression tree in a group (you can see this trivially as you can just swap the order of two subtrees for commutative operators)
                    // however, we just take an arbitrary expression tree from the group to compute selectivity
                    let expr_tree = expr_trees.first().expect("expression missing");
                    self.get_filter_selectivity(expr_tree.clone(), &column_refs)
                } else {
                    DEFAULT_UNK_SEL
                };
                Self::cost(
                    (row_cnt * selectivity).max(1.0),
                    row_cnt * compute_cost,
                    0.0,
                )
            }
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
            OptRelNodeTyp::PhysicalAgg => {
                let child_row_cnt = Self::row_cnt(&children[0]);
                let row_cnt = self.get_agg_row_cnt(context, optimizer, child_row_cnt);
                let (_, compute_cost_1, _) = Self::cost_tuple(&children[1]);
                let (_, compute_cost_2, _) = Self::cost_tuple(&children[2]);
                Self::cost(
                    row_cnt,
                    child_row_cnt * (compute_cost_1 + compute_cost_2),
                    0.0,
                )
            }
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

    fn get_agg_row_cnt(
        &self,
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
        child_row_cnt: f64,
    ) -> f64 {
        if let (Some(context), Some(optimizer)) = (context, optimizer) {
            let group_by_id = context.children_group_ids[2];
            let mut group_by_exprs: Vec<Arc<RelNode<OptRelNodeTyp>>> =
                optimizer.get_all_group_bindings(group_by_id, false);
            assert!(
                group_by_exprs.len() == 1,
                "ExprList expression should be the only expression in the GROUP BY group"
            );
            let group_by = group_by_exprs.pop().unwrap();
            let group_by = ExprList::from_rel_node(group_by).unwrap();
            if group_by.is_empty() {
                1.0
            } else {
                // Multiply the n-distinct of all the group by columns.
                // TODO: improve with multi-dimensional n-distinct
                let base_table_col_refs = optimizer
                    .get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
                base_table_col_refs
                    .iter()
                    .take(group_by.len())
                    .map(|col_ref| match col_ref {
                        ColumnRef::BaseTableColumnRef { table, col_idx } => {
                            let table_stats = self.per_table_stats_map.get(table);
                            let column_stats = table_stats.map(|table_stats| {
                                table_stats.per_column_stats_vec.get(*col_idx).unwrap()
                            });

                            if let Some(Some(column_stats)) = column_stats {
                                column_stats.ndistinct as f64
                            } else {
                                // The column type is not supported or stats are missing.
                                DEFAULT_NUM_DISTINCT as f64
                            }
                        }
                        ColumnRef::Derived => DEFAULT_NUM_DISTINCT as f64,
                        _ => panic!(
                            "GROUP BY base table column ref must either be derived or base table"
                        ),
                    })
                    .product()
            }
        } else {
            (child_row_cnt * DEFAULT_UNK_SEL).max(1.0)
        }
    }

    /// The expr_tree input must be a "mixed expression tree"
    ///     An "expression node" refers to a RelNode that returns true for is_expression()
    ///     A "full expression tree" is where every node in the tree is an expression node
    ///     A "mixed expression tree" is where every base-case node and all its parents are expression nodes
    ///     A "base-case node" is a node that doesn't lead to further recursion (such as a BinOp(Eq))
    /// The schema input is the schema the predicate represented by the expr_tree is applied on
    /// The output will be the selectivity of the expression tree if it were a "filter predicate".
    /// A "filter predicate" operates on one input node, unlike a "join predicate" which operates on two input nodes.
    ///     This is why the function only takes in a single schema.
    fn get_filter_selectivity(
        &self,
        expr_tree: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
    ) -> f64 {
        assert!(expr_tree.typ.is_expression());
        match &expr_tree.typ {
            OptRelNodeTyp::Constant(_) => Self::get_constant_selectivity(expr_tree),
            OptRelNodeTyp::ColumnRef => unimplemented!("check bool type or else panic"),
            OptRelNodeTyp::UnOp(un_op_typ) => {
                assert!(expr_tree.children.len() == 1);
                let child = expr_tree.child(0);
                match un_op_typ {
                    // not doesn't care about nulls so there's no complex logic. it just reverses the selectivity
                    // for instance, != _will not_ include nulls but "NOT ==" _will_ include nulls
                    UnOpType::Not => 1.0 - self.get_filter_selectivity(child, column_refs),
                    UnOpType::Neg => panic!(
                        "the selectivity of operations that return numerical values is undefined"
                    ),
                }
            }
            OptRelNodeTyp::BinOp(bin_op_typ) => {
                assert!(expr_tree.children.len() == 2);
                let left_child = expr_tree.child(0);
                let right_child = expr_tree.child(1);

                if bin_op_typ.is_comparison() {
                    self.get_filter_comp_op_selectivity(
                        *bin_op_typ,
                        left_child,
                        right_child,
                        column_refs,
                    )
                } else if bin_op_typ.is_numerical() {
                    panic!(
                        "the selectivity of operations that return numerical values is undefined"
                    )
                } else {
                    unreachable!("all BinOpTypes should be true for at least one is_*() function")
                }
            }
            OptRelNodeTyp::LogOp(log_op_typ) => {
                self.get_filter_log_op_selectivity(*log_op_typ, &expr_tree.children, column_refs)
            }
            OptRelNodeTyp::Func(_) => unimplemented!("check bool type or else panic"),
            OptRelNodeTyp::SortOrder(_) => {
                panic!("the selectivity of sort order expressions is undefined")
            }
            OptRelNodeTyp::Between => UNIMPLEMENTED_SEL,
            OptRelNodeTyp::Cast => unimplemented!("check bool type or else panic"),
            OptRelNodeTyp::Like => DEFAULT_MATCH_SEL,
            OptRelNodeTyp::DataType(_) => {
                panic!("the selectivity of a data type is not defined")
            }
            OptRelNodeTyp::InList => UNIMPLEMENTED_SEL,
            _ => unreachable!(
                "all expression OptRelNodeTyp were enumerated. this should be unreachable"
            ),
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

    /// Comparison operators are the base case for recursion in get_filter_selectivity()
    fn get_filter_comp_op_selectivity(
        &self,
        comp_bin_op_typ: BinOpType,
        left: OptRelNodeRef,
        right: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
    ) -> f64 {
        assert!(comp_bin_op_typ.is_comparison());

        // I intentionally performed moves on left and right. This way, we don't accidentally use them after this block
        let (col_ref_exprs, non_col_ref_exprs, is_left_col_ref) =
            Self::get_semantic_nodes(left, right);

        // handle the different cases of column nodes
        if col_ref_exprs.is_empty() {
            UNIMPLEMENTED_SEL
        } else if col_ref_exprs.len() == 1 {
            let col_ref_expr = col_ref_exprs
                .first()
                .expect("we just checked that col_ref_exprs.len() == 1");
            let col_ref_idx = col_ref_expr.index();

            if let ColumnRef::BaseTableColumnRef { table, col_idx } = &column_refs[col_ref_idx] {
                let non_col_ref_expr = non_col_ref_exprs
                    .first()
                    .expect("non_col_ref_exprs should have a value since col_ref_exprs.len() == 1");

                match non_col_ref_expr.as_ref().typ {
                    OptRelNodeTyp::Constant(_) => {
                        let value = non_col_ref_expr
                            .as_ref()
                            .data
                            .as_ref()
                            .expect("constants should have data");
                        match comp_bin_op_typ {
                            BinOpType::Eq => {
                                self.get_column_equality_selectivity(table, *col_idx, value, true)
                            }
                            BinOpType::Neq => {
                                self.get_column_equality_selectivity(table, *col_idx, value, false)
                            }
                            BinOpType::Lt | BinOpType::Leq | BinOpType::Gt | BinOpType::Geq => {
                                let start = match (comp_bin_op_typ, is_left_col_ref) {
                                    (BinOpType::Lt, true) | (BinOpType::Geq, false) => Bound::Unbounded,
                                    (BinOpType::Leq, true) | (BinOpType::Gt, false) => Bound::Unbounded,
                                    (BinOpType::Gt, true) | (BinOpType::Leq, false) => Bound::Excluded(value),
                                    (BinOpType::Geq, true) | (BinOpType::Lt, false) => Bound::Included(value),
                                    _ => unreachable!("all comparison BinOpTypes were enumerated. this should be unreachable"),
                                };
                                let end = match (comp_bin_op_typ, is_left_col_ref) {
                                    (BinOpType::Lt, true) | (BinOpType::Geq, false) => Bound::Excluded(value),
                                    (BinOpType::Leq, true) | (BinOpType::Gt, false) => Bound::Included(value),
                                    (BinOpType::Gt, true) | (BinOpType::Leq, false) => Bound::Unbounded,
                                    (BinOpType::Geq, true) | (BinOpType::Lt, false) => Bound::Unbounded,
                                    _ => unreachable!("all comparison BinOpTypes were enumerated. this should be unreachable"),
                                };
                                self.get_column_range_selectivity(
                                    table,
                                    *col_idx,
                                    start,
                                    end,
                                )
                            },
                            _ => unreachable!("all comparison BinOpTypes were enumerated. this should be unreachable"),
                        }
                    }
                    OptRelNodeTyp::BinOp(_) => {
                        Self::get_default_comparison_op_selectivity(comp_bin_op_typ)
                    }
                    OptRelNodeTyp::Cast => UNIMPLEMENTED_SEL,
                    _ => unimplemented!(
                        "unhandled case of comparing a column ref node to {}",
                        non_col_ref_expr.as_ref().typ
                    ),
                }
            } else {
                Self::get_default_comparison_op_selectivity(comp_bin_op_typ)
            }
        } else if col_ref_exprs.len() == 2 {
            Self::get_default_comparison_op_selectivity(comp_bin_op_typ)
        } else {
            unreachable!("we could have at most pushed left and right into col_ref_exprs")
        }
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

    fn get_constant_selectivity(const_node: OptRelNodeRef) -> f64 {
        if let OptRelNodeTyp::Constant(const_typ) = const_node.typ {
            if matches!(const_typ, ConstantType::Bool) {
                let value = const_node
                    .as_ref()
                    .data
                    .as_ref()
                    .expect("constants should have data");
                if let Value::Bool(bool_value) = value {
                    if *bool_value {
                        1.0
                    } else {
                        0.0
                    }
                } else {
                    unreachable!(
                        "if the typ is ConstantType::Bool, the value should be a Value::Bool"
                    )
                }
            } else {
                panic!("selectivity is not defined on constants which are not bools")
            }
        } else {
            panic!("get_constant_selectivity must be called on a constant")
        }
    }

    /// Get the selectivity of an expression of the form "column equals value" (or "value equals column")
    /// Will handle the case of statistics missing
    /// Equality predicates are handled entirely differently from range predicates so this is its own function
    /// Also, get_column_equality_selectivity is a subroutine when computing range selectivity, which is another
    ///     reason for separating these into two functions
    /// is_eq means whether it's == or !=
    fn get_column_equality_selectivity(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
        is_eq: bool,
    ) -> f64 {
        if let Some(per_column_stats) = self.get_per_column_stats(table, col_idx) {
            let eq_freq = if let Some(freq) = per_column_stats.mcvs.freq(value) {
                freq
            } else {
                let non_mcv_freq = 1.0 - per_column_stats.mcvs.total_freq();
                // always safe because usize is at least as large as i32
                let ndistinct_as_usize = per_column_stats.ndistinct as usize;
                let non_mcv_cnt = ndistinct_as_usize - per_column_stats.mcvs.cnt();
                // note that nulls are not included in ndistinct so we don't need to do non_mcv_cnt - 1 if null_frac > 0
                (non_mcv_freq - per_column_stats.null_frac) / (non_mcv_cnt as f64)
            };
            if is_eq {
                eq_freq
            } else {
                1.0 - eq_freq - per_column_stats.null_frac
            }
        } else {
            #[allow(clippy::collapsible_else_if)]
            if is_eq {
                DEFAULT_EQ_SEL
            } else {
                1.0 - DEFAULT_EQ_SEL
            }
        }
    }

    /// Compute the frequency of values in a column less than or equal to the given value.
    fn get_column_leq_value_freq(per_column_stats: &PerColumnStats<M, D>, value: &Value) -> f64 {
        // because distr does not include the values in MCVs, we need to compute the CDFs there as well
        // because nulls return false in any comparison, they are never included when computing range selectivity
        let distr_leq_freq = per_column_stats.distr.cdf(value);
        let value = value.clone();
        let pred = Box::new(move |val: &Value| val <= &value);
        let mcvs_leq_freq = per_column_stats.mcvs.freq_over_pred(pred);
        distr_leq_freq + mcvs_leq_freq
    }

    /// Compute the frequency of values in a column less than the given value.
    fn get_column_lt_value_freq(
        &self,
        per_column_stats: &PerColumnStats<M, D>,
        table: &str,
        col_idx: usize,
        value: &Value,
    ) -> f64 {
        // depending on whether value is in mcvs or not, we use different logic to turn total_lt_cdf into total_leq_cdf
        // this logic just so happens to be the exact same logic as get_column_equality_selectivity implements
        Self::get_column_leq_value_freq(per_column_stats, value)
            - self.get_column_equality_selectivity(table, col_idx, value, true)
    }

    /// Get the selectivity of an expression of the form "column </<=/>=/> value" (or "value </<=/>=/> column").
    /// Computes selectivity based off of statistics.
    /// Range predicates are handled entirely differently from equality predicates so this is its own function.
    /// If it is unable to find the statistics, it returns DEFAULT_INEQ_SEL.
    /// The selectivity is computed as quantile of the right bound minus quantile of the left bound.
    fn get_column_range_selectivity(
        &self,
        table: &str,
        col_idx: usize,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> f64 {
        if let Some(per_column_stats) = self.get_per_column_stats(table, col_idx) {
            let left_quantile = match start {
                Bound::Unbounded => 0.0,
                Bound::Included(value) => {
                    self.get_column_lt_value_freq(per_column_stats, table, col_idx, value)
                }
                Bound::Excluded(value) => Self::get_column_leq_value_freq(per_column_stats, value),
            };
            let right_quantile = match end {
                Bound::Unbounded => 1.0,
                Bound::Included(value) => Self::get_column_leq_value_freq(per_column_stats, value),
                Bound::Excluded(value) => {
                    self.get_column_lt_value_freq(per_column_stats, table, col_idx, value)
                }
            };
            assert!(left_quantile <= right_quantile);
            // `Distribution` does not account for NULL values, so the selectivity is smaller than frequency.
            (right_quantile - left_quantile) * (1.0 - per_column_stats.null_frac)
        } else {
            DEFAULT_INEQ_SEL
        }
    }

    fn get_filter_log_op_selectivity(
        &self,
        log_op_typ: LogOpType,
        children: &[OptRelNodeRef],
        column_refs: &GroupColumnRefs,
    ) -> f64 {
        let children_sel = children
            .iter()
            .map(|expr| self.get_filter_selectivity(expr.clone(), column_refs));

        match log_op_typ {
            LogOpType::And => children_sel.product(),
            // the formula is 1.0 - the probability of _none_ of the events happening
            LogOpType::Or => 1.0 - children_sel.fold(1.0, |acc, sel| acc * (1.0 - sel)),
        }
    }

    pub fn get_row_cnt(&self, table: &str) -> Option<usize> {
        self.per_table_stats_map
            .get(table)
            .map(|per_table_stats| per_table_stats.row_cnt)
    }
}

impl<M: MostCommonValues, D: Distribution> PerTableStats<M, D> {
    pub fn new(row_cnt: usize, per_column_stats_vec: Vec<Option<PerColumnStats<M, D>>>) -> Self {
        Self {
            row_cnt,
            per_column_stats_vec,
        }
    }
}

/// I thought about using the system's own parser and planner to generate these expression trees, but
/// this is not currently feasible because it would create a cyclic dependency between optd-datafusion-bridge
/// and optd-datafusion-repr
#[cfg(test)]
mod tests {
    use optd_core::rel_node::Value;
    use std::collections::HashMap;

    use crate::{
        cost::base_cost::DEFAULT_EQ_SEL,
        plan_nodes::{
            BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, Expr, ExprList, JoinType, LogOpExpr,
            LogOpType, OptRelNode, OptRelNodeRef, UnOpExpr, UnOpType,
        },
        properties::column_ref::{ColumnRef, GroupColumnRefs},
    };

    use super::{Distribution, MostCommonValues, OptCostModel, PerColumnStats, PerTableStats};
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
