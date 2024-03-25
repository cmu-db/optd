use std::{collections::HashMap, sync::Arc};

use crate::plan_nodes::{BinOpType, ColumnRefExpr, LogOpType, OptRelNode, UnOpType};
use crate::properties::column_ref::{ColumnRefPropertyBuilder, GroupColumnRefs};
use crate::{
    plan_nodes::{OptRelNodeRef, OptRelNodeTyp},
    properties::column_ref::ColumnRef,
};
use arrow_schema::{ArrowError, DataType};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int8Array, RecordBatch, RecordBatchIterator, RecordBatchReader, UInt16Array,
    UInt32Array, UInt8Array,
};
use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::{Cost, CostModel},
    rel_node::{RelNode, RelNodeTyp, Value},
};
use optd_gungnir::stats::hyperloglog::{self, HyperLogLog};
use optd_gungnir::stats::tdigest::{self, TDigest};
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
// used to indicate a combination of unimplemented!(), unreachable!(), or panic!()
// TODO: a future PR will remove this and get the code working for all of TPC-H
const INVALID_SELECTIVITY: f64 = 0.001;

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
                let selectivity = 0.001;
                Self::cost((row_cnt * selectivity).max(1.0), compute_cost, 0.0)
            }
            OptRelNodeTyp::PhysicalFilter => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                let (_, compute_cost, _) = Self::cost_tuple(&children[1]);
                let selectivity = match context {
                    Some(context) => {
                        if let Some(optimizer) = optimizer {
                            let column_refs = optimizer
                                .get_property_by_group::<ColumnRefPropertyBuilder>(
                                    context.group_id,
                                    1,
                                );
                            let expr_group_id = context.children_group_ids[1];
                            let expr_trees = optimizer.get_all_group_bindings(expr_group_id, false);
                            // there may be more than one expression tree in a group (you can see this trivially as you can just swap the order of two subtrees for commutative operators)
                            // however, we just take an arbitrary expression tree from the group to compute selectivity
                            if let Some(expr_tree) = expr_trees.first() {
                                self.get_filter_selectivity(Arc::clone(expr_tree), &column_refs)
                            } else {
                                INVALID_SELECTIVITY
                            }
                        } else {
                            INVALID_SELECTIVITY
                        }
                    }
                    None => INVALID_SELECTIVITY,
                };

                Self::cost(
                    (row_cnt * selectivity).max(1.0),
                    row_cnt * compute_cost,
                    0.0,
                )
            }
            OptRelNodeTyp::PhysicalNestedLoopJoin(_) => {
                let (row_cnt_1, _, _) = Self::cost_tuple(&children[0]);
                let (row_cnt_2, _, _) = Self::cost_tuple(&children[1]);
                let (_, compute_cost, _) = Self::cost_tuple(&children[2]);
                let selectivity = 0.01;
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
            OptRelNodeTyp::PhysicalHashJoin(_) => {
                let (row_cnt_1, _, _) = Self::cost_tuple(&children[0]);
                let (row_cnt_2, _, _) = Self::cost_tuple(&children[1]);
                Self::cost(
                    row_cnt_1.min(row_cnt_2).max(1.0),
                    row_cnt_1 * 2.0 + row_cnt_2,
                    0.0,
                )
            }

            OptRelNodeTyp::PhysicalSort => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                Self::cost(row_cnt, row_cnt * row_cnt.ln_1p().max(1.0), 0.0)
            }
            OptRelNodeTyp::PhysicalAgg => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                let (_, compute_cost_1, _) = Self::cost_tuple(&children[1]);
                let (_, compute_cost_2, _) = Self::cost_tuple(&children[2]);
                Self::cost(row_cnt, row_cnt * (compute_cost_1 + compute_cost_2), 0.0)
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
        match expr_tree.typ {
            OptRelNodeTyp::BinOp(bin_op_typ) => {
                assert!(expr_tree.children.len() == 2);
                let left_child = expr_tree.child(0);
                let right_child = expr_tree.child(1);

                if bin_op_typ.is_comparison() {
                    self.get_comparison_op_selectivity(
                        bin_op_typ,
                        left_child,
                        right_child,
                        column_refs,
                    )
                } else if bin_op_typ.is_numerical() {
                    INVALID_SELECTIVITY
                } else {
                    unreachable!("all BinOpTypes should be true for at least one is_*() function")
                }
            }
            OptRelNodeTyp::LogOp(log_op_typ) => {
                self.get_log_op_selectivity(log_op_typ, &expr_tree.children, column_refs)
            }
            OptRelNodeTyp::UnOp(un_op_typ) => {
                assert!(expr_tree.children.len() == 1);
                let child = expr_tree.child(0);
                match un_op_typ {
                    // not doesn't care about nulls so there's no complex logic. it just reverses the selectivity
                    // for instance, != _will not_ include nulls but "NOT ==" _will_ include nulls
                    UnOpType::Not => 1.0 - self.get_filter_selectivity(child, column_refs),
                    _ => INVALID_SELECTIVITY,
                }
            }
            _ => INVALID_SELECTIVITY,
        }
    }

    /// Comparison operators are the base case for recursion in get_filter_selectivity()
    fn get_comparison_op_selectivity(
        &self,
        bin_op_typ: BinOpType,
        left: OptRelNodeRef,
        right: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
    ) -> f64 {
        assert!(bin_op_typ.is_comparison());

        // the # of column refs determines how we handle the logic
        let mut col_ref_nodes = vec![];
        let mut non_col_ref_nodes = vec![];
        let is_left_col_ref;
        // I intentionally performed moves on left and right. This way, we don't accidentally use them after this block
        // We always want to use "col_ref_node" and "non_col_ref_node" instead of "left" or "right"
        if left.as_ref().typ == OptRelNodeTyp::ColumnRef {
            is_left_col_ref = true;
            col_ref_nodes.push(
                ColumnRefExpr::from_rel_node(left)
                    .expect("we already checked that the type is ColumnRef"),
            );
        } else {
            is_left_col_ref = false;
            non_col_ref_nodes.push(left);
        }
        if right.as_ref().typ == OptRelNodeTyp::ColumnRef {
            col_ref_nodes.push(
                ColumnRefExpr::from_rel_node(right)
                    .expect("we already checked that the type is ColumnRef"),
            );
        } else {
            non_col_ref_nodes.push(right);
        }

        if col_ref_nodes.is_empty() {
            INVALID_SELECTIVITY
        } else if col_ref_nodes.len() == 1 {
            let col_ref_node = col_ref_nodes
                .pop()
                .expect("we just checked that col_ref_nodes.len() == 1");
            let col_ref_idx = col_ref_node.index();

            if let ColumnRef::BaseTableColumnRef { table, col_idx } = &column_refs[col_ref_idx] {
                let non_col_ref_node = non_col_ref_nodes
                    .pop()
                    .expect("non_col_ref_nodes should have a value since col_ref_nodes.len() == 1");

                if let OptRelNodeTyp::Constant(_) = non_col_ref_node.as_ref().typ {
                    let value = non_col_ref_node
                        .as_ref()
                        .data
                        .as_ref()
                        .expect("constants should have data");
                    match match bin_op_typ {
                        BinOpType::Eq => {
                            self.get_column_equality_selectivity(table, *col_idx, value, true)
                        }
                        BinOpType::Neq => {
                            self.get_column_equality_selectivity(table, *col_idx, value, false)
                        }
                        BinOpType::Lt => self.get_column_range_selectivity(
                            table,
                            *col_idx,
                            value,
                            is_left_col_ref,
                            false,
                        ),
                        BinOpType::Leq => self.get_column_range_selectivity(
                            table,
                            *col_idx,
                            value,
                            is_left_col_ref,
                            true,
                        ),
                        BinOpType::Gt => self.get_column_range_selectivity(
                            table,
                            *col_idx,
                            value,
                            !is_left_col_ref,
                            false,
                        ),
                        BinOpType::Geq => self.get_column_range_selectivity(
                            table,
                            *col_idx,
                            value,
                            !is_left_col_ref,
                            true,
                        ),
                        _ => None,
                    } {
                        Some(sel) => sel,
                        None => INVALID_SELECTIVITY,
                    }
                } else {
                    INVALID_SELECTIVITY
                }
            } else {
                INVALID_SELECTIVITY
            }
        } else if col_ref_nodes.len() == 2 {
            INVALID_SELECTIVITY
        } else {
            unreachable!("We could have at most pushed left and right into col_ref_nodes")
        }
    }

    /// Get the selectivity of an expression of the form "column equals value" (or "value equals column")
    /// Computes selectivity based off of statistics
    /// Equality predicates are handled entirely differently from range predicates so this is its own function
    /// Also, get_column_equality_selectivity is a subroutine when computing range selectivity, which is another
    ///     reason for separating these into two functions
    /// If it is unable to find the statistics, it returns None
    /// is_eq means whether it's == or !=
    fn get_column_equality_selectivity(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
        is_eq: bool,
    ) -> Option<f64> {
        if let Some(per_table_stats) = self.per_table_stats_map.get(table) {
            if let Some(Some(per_column_stats)) = per_table_stats.per_column_stats_vec.get(col_idx)
            {
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
                Some(if is_eq {
                    eq_freq
                } else {
                    1.0 - eq_freq - per_column_stats.null_frac
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get the selectivity of an expression of the form "column </<=/>=/> value" (or "value </<=/>=/> column")
    /// Computes selectivity based off of statistics
    /// Range predicates are handled entirely differently from equality predicates so this is its own function
    /// If it is unable to find the statistics, it returns None
    /// Like in the Postgres source code, we decompose the four operators "</<=/>=/>" into "is_lt" and "is_eq"
    /// The "is_lt" and "is_eq" values are set as if column is on the left hand side
    fn get_column_range_selectivity(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
        is_col_lt_val: bool,
        is_col_eq_val: bool,
    ) -> Option<f64> {
        if let Some(per_table_stats) = self.per_table_stats_map.get(table) {
            if let Some(Some(per_column_stats)) = per_table_stats.per_column_stats_vec.get(col_idx)
            {
                // because distr does not include the values in MCVs, we need to compute the CDFs there as well
                // because nulls return false in any comparison, they are never included when computing range selectivity
                let distr_leq_freq = per_column_stats.distr.cdf(value);
                let value_clone = value.clone(); // clone the value so that we can move it into the closure to avoid lifetime issues
                                                 // TODO: in a future PR, figure out how to make Values comparable. rn I just hardcoded as_i32() to work around this
                let pred = Box::new(move |val: &Value| val.as_i32() <= value_clone.as_i32());
                let mcvs_leq_freq = per_column_stats.mcvs.freq_over_pred(pred);
                let total_leq_freq = distr_leq_freq + mcvs_leq_freq;

                // depending on whether value is in mcvs or not, we use different logic to turn total_leq_cdf into total_lt_cdf
                // this logic just so happens to be the exact same logic as get_column_equality_selectivity implements
                let total_lt_freq = total_leq_freq
                    - self
                        .get_column_equality_selectivity(table, col_idx, value, true)
                        .expect("we already know that table and col_idx exist");

                // use either total_leq_freq or total_lt_freq to get the selectivity
                Some(if is_col_lt_val {
                    if is_col_eq_val {
                        // this branch means <=
                        total_leq_freq
                    } else {
                        // this branch means <
                        total_lt_freq
                    }
                } else {
                    // clippy wants me to collapse this into an else if, but keeping two nested if else statements is clearer
                    #[allow(clippy::collapsible_else_if)]
                    if is_col_eq_val {
                        // this branch means >=, which is 1 - < - null_frac
                        // we need to subtract null_frac since that isn't included in >= either
                        1.0 - total_lt_freq - per_column_stats.null_frac
                    } else {
                        // this branch means >. same logic as above
                        1.0 - total_leq_freq - per_column_stats.null_frac
                    }
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_log_op_selectivity(
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
        plan_nodes::{
            BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, Expr, ExprList, LogOpExpr,
            LogOpType, OptRelNode, OptRelNodeRef, UnOpExpr, UnOpType,
        },
        properties::column_ref::ColumnRef,
    };

    use super::{Distribution, MostCommonValues, OptCostModel, PerColumnStats, PerTableStats};
    type TestPerColumnStats = PerColumnStats<TestMostCommonValues, TestDistribution>;

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

    const TABLE1_NAME: &str = "t1";

    // one column is sufficient for all filter selectivity predicates
    fn create_one_column_cost_model(
        per_column_stats: TestPerColumnStats,
    ) -> OptCostModel<TestMostCommonValues, TestDistribution> {
        OptCostModel::new(
            vec![(
                String::from(TABLE1_NAME),
                PerTableStats::new(100, vec![Some(per_column_stats)]),
            )]
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

    #[test]
    fn test_colref_eq_constint_in_mcv() {
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
    fn test_colref_eq_constint_not_in_mcv_no_nulls() {
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
    fn test_colref_eq_constint_not_in_mcv_with_nulls() {
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
    fn test_colref_neq_constint_in_mcv() {
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
    fn test_colref_leq_constint_no_mcvs_in_range() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
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
    fn test_colref_leq_constint_no_mcvs_in_range_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.1,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Leq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
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
    fn test_colref_leq_constint_with_mcvs_in_range_not_at_border() {
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

    #[test]
    fn test_colref_leq_constint_with_mcv_at_border() {
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
        let expr_tree_rev = bin_op(BinOpType::Geq, cnst(Value::Int32(15)), col_ref(0));
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
    fn test_colref_lt_constint_no_mcvs_in_range() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Lt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
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
    fn test_colref_lt_constint_no_mcvs_in_range_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            9, // 90% of the values aren't nulls since null_frac = 0.1. if there are 9 distinct non-null values, each will have 0.1 frequency
            0.1,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Lt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
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
    fn test_colref_lt_constint_with_mcvs_in_range_not_at_border() {
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
        let expr_tree_rev = bin_op(BinOpType::Gt, cnst(Value::Int32(15)), col_ref(0));
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
    fn test_colref_lt_constint_with_mcv_at_border() {
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

    /// I have fewer tests for GT since I'll assume that it uses the same underlying logic as LEQ
    /// The only interesting thing to test is that if there are nulls, those aren't included in GT
    #[test]
    fn test_colref_gt_constint_no_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Gt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Lt, cnst(Value::Int32(15)), col_ref(0));
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
    fn test_colref_gt_constint_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.1,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Gt, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Lt, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        // we have to subtract 0.1 since we don't want to include them in GT either
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            1.0 - 0.7 - 0.1
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            1.0 - 0.7 - 0.1
        );
    }

    /// As with above, I have one test without nulls and one test with nulls
    #[test]
    fn test_colref_geq_constint_no_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            10,
            0.0,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Geq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Leq, cnst(Value::Int32(15)), col_ref(0));
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
    fn test_colref_geq_constint_with_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::empty(),
            9, // 90% of the values aren't nulls since null_frac = 0.1. if there are 9 distinct non-null values, each will have 0.1 frequency
            0.1,
            TestDistribution::new(vec![(Value::Int32(15), 0.7)]),
        ));
        let expr_tree = bin_op(BinOpType::Geq, col_ref(0), cnst(Value::Int32(15)));
        let expr_tree_rev = bin_op(BinOpType::Leq, cnst(Value::Int32(15)), col_ref(0));
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from(TABLE1_NAME),
            col_idx: 0,
        }];
        // we have to subtract 0.1 since we don't want to include them in GT either
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            1.0 - 0.6 - 0.1
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_filter_selectivity(expr_tree_rev, &column_refs),
            1.0 - 0.6 - 0.1
        );
    }

    #[test]
    fn test_and() {
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
    fn test_or() {
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
    fn test_not_no_nulls() {
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
    fn test_not_with_nulls() {
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
}
