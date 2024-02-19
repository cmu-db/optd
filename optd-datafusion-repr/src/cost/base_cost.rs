use std::{collections::HashMap, sync::Arc};

use crate::properties::column_ref::{ColumnRefPropertyBuilder, GroupColumnRefs};
use crate::{
    plan_nodes::{OptRelNodeRef, OptRelNodeTyp},
    properties::column_ref::ColumnRef,
};
use itertools::Itertools;
use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::{Cost, CostModel},
    rel_node::{RelNode, RelNodeTyp, Value},
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

pub struct OptCostModel {
    per_table_stats_map: HashMap<String, PerTableStats>,
}

pub struct PerTableStats {
    row_cnt: usize,
    per_column_stats_vec: Vec<PerColumnStats>,
}

pub struct PerColumnStats {
    mcvs: Box<dyn MostCommonValues>,
}

pub trait MostCommonValues: 'static + Send + Sync {
    fn get_freq(&self, value: &Value) -> Option<f64>;
}

pub const ROW_COUNT: usize = 1;
pub const COMPUTE_COST: usize = 2;
pub const IO_COST: usize = 3;
// used to indicate a combination of unimplemented!(), unreachable!(), or panic!()
const INVALID_SELECTIVITY: f64 = -1.0;

impl OptCostModel {
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

impl CostModel<OptRelNodeTyp> for OptCostModel {
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

                // to have a sensible "default" for now
                let selectivity = if selectivity == INVALID_SELECTIVITY {
                    0.001
                } else {
                    selectivity
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

impl OptCostModel {
    pub fn new(per_table_stats_map: HashMap<String, PerTableStats>) -> Self {
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
                if bin_op_typ.is_comparison() {
                    let left_child = expr_tree.child(0);
                    let right_child = expr_tree.child(1);

                    if left_child.as_ref().typ == OptRelNodeTyp::ColumnRef {
                        let col_ref_idx = left_child.as_ref().data.as_ref().unwrap().as_u64();
                        // this is always safe since col_ref_idx was initially a usize in ColumnRefExpr::new()
                        let usize_col_ref_idx = col_ref_idx as usize;
                        if let ColumnRef::BaseTableColumnRef { table, col_idx } =
                            &column_refs[usize_col_ref_idx]
                        {
                            if let OptRelNodeTyp::Constant(_) = right_child.as_ref().typ {
                                self.get_column_equality_selectivity(
                                    table,
                                    *col_idx,
                                    right_child.as_ref().data.as_ref().unwrap(),
                                )
                            } else {
                                INVALID_SELECTIVITY
                            }
                        } else {
                            INVALID_SELECTIVITY
                        }
                    } else {
                        INVALID_SELECTIVITY
                    }
                } else if bin_op_typ.is_numerical() || bin_op_typ.is_logical() {
                    INVALID_SELECTIVITY
                } else {
                    unreachable!("all BinOpTypes should be true for at least one is_*() function")
                }
            }
            _ => INVALID_SELECTIVITY,
        }
    }

    fn get_column_equality_selectivity(&self, table: &str, col_idx: usize, value: &Value) -> f64 {
        if let Some(per_table_stats) = self.per_table_stats_map.get(table) {
            if let Some(per_column_stats) = per_table_stats.per_column_stats_vec.get(col_idx) {
                if let Some(freq) = per_column_stats.mcvs.get_freq(value) {
                    freq
                } else {
                    INVALID_SELECTIVITY
                }
            } else {
                INVALID_SELECTIVITY
            }
        } else {
            INVALID_SELECTIVITY
        }
    }

    pub fn get_row_cnt(&self, table: &str) -> Option<usize> {
        self.per_table_stats_map
            .get(table)
            .map(|per_table_stats| per_table_stats.row_cnt)
    }
}

impl PerTableStats {
    pub fn new(row_cnt: usize, per_column_stats_vec: Vec<PerColumnStats>) -> Self {
        Self {
            row_cnt,
            per_column_stats_vec,
        }
    }
}

impl PerColumnStats {
    pub fn new(mcvs: Box<dyn MostCommonValues>) -> Self {
        Self { mcvs }
    }
}

/// I thought about using the system's own parser and planner to generate these expression trees, but
/// this is not currently feasible because it would create a cyclic dependency between optd-datafusion-bridge
/// and optd-datafusion-repr
#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use optd_core::rel_node::{RelNode, Value};

    use crate::{
        plan_nodes::{BinOpType, ConstantType, OptRelNodeTyp},
        properties::column_ref::ColumnRef,
    };

    use super::{MostCommonValues, OptCostModel, PerColumnStats, PerTableStats};

    struct MockMostCommonValues {
        mcvs: HashMap<Value, f64>,
    }

    impl MostCommonValues for MockMostCommonValues {
        fn get_freq(&self, value: &Value) -> Option<f64> {
            self.mcvs.get(value).copied()
        }
    }

    #[test]
    fn test_colref_eq_constint_in_mcv() {
        let t1_c0_v1_freq = 0.1;
        let cost_model = OptCostModel::new(
            vec![(
                String::from("t1"),
                PerTableStats::new(
                    100,
                    vec![PerColumnStats::new(Box::new(MockMostCommonValues {
                        mcvs: vec![(Value::Int32(1), t1_c0_v1_freq)].into_iter().collect(),
                    }))],
                ),
            )]
            .into_iter()
            .collect(),
        );
        let expr_tree = Arc::new(RelNode::<OptRelNodeTyp> {
            typ: OptRelNodeTyp::BinOp(BinOpType::Eq),
            children: vec![
                Arc::new(RelNode::<OptRelNodeTyp> {
                    typ: OptRelNodeTyp::ColumnRef,
                    children: vec![],
                    data: Some(Value::UInt64(0)),
                }),
                Arc::new(RelNode::<OptRelNodeTyp> {
                    typ: OptRelNodeTyp::Constant(ConstantType::Int32),
                    children: vec![],
                    data: Some(Value::Int32(1)),
                }),
            ],
            data: None,
        });
        let column_refs = vec![ColumnRef::BaseTableColumnRef {
            table: String::from("t1"),
            col_idx: 0,
        }];
        assert_eq!(
            cost_model.get_filter_selectivity(expr_tree, &column_refs),
            t1_c0_v1_freq
        );
    }
}
