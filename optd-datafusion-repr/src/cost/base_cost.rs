use std::collections::HashMap;

use crate::plan_nodes::OptRelNodeTyp;
use itertools::Itertools;
use optd_core::{
    cascades::RelNodeContext,
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
    let cost = model.compute_cost(&node.typ, &node.data, &children, None);
    model.accumulate(total_cost, &cost);
    cost
}

pub struct OptCostModel {
    table_stat: HashMap<String, usize>,
}

pub const ROW_COUNT: usize = 1;
pub const COMPUTE_COST: usize = 2;
pub const IO_COST: usize = 3;

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
        compute_cost + io_cost * 10.0
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
        _context: Option<RelNodeContext>,
    ) -> Cost {
        match node {
            OptRelNodeTyp::PhysicalScan => {
                let table_name = data.as_ref().unwrap().as_str();
                let row_cnt = self
                    .table_stat
                    .get(table_name.as_ref())
                    .copied()
                    .unwrap_or(1) as f64;
                Self::cost(row_cnt, 0.0, row_cnt)
            }
            OptRelNodeTyp::PhysicalFilter => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                let (_, compute_cost, _) = Self::cost_tuple(&children[1]);
                let selectivity = 0.001;
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
                    row_cnt_1 * row_cnt_2 * compute_cost,
                    0.0,
                )
            }
            OptRelNodeTyp::PhysicalProjection => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                let (_, compute_cost, _) = Self::cost_tuple(&children[0]);
                Self::cost(row_cnt, compute_cost * row_cnt, 0.0)
            }
            OptRelNodeTyp::PhysicalHashJoin(_) => {
                let (row_cnt_1, _, _) = Self::cost_tuple(&children[0]);
                let (row_cnt_2, _, _) = Self::cost_tuple(&children[1]);
                let selectivity = 0.01;
                Self::cost(
                    (row_cnt_1 * row_cnt_2 * selectivity).max(1.0),
                    row_cnt_1 + row_cnt_2,
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
                Self::cost(1.0, compute_cost + 1.0, 0.0)
            }
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
    pub fn new(table_stat: HashMap<String, usize>) -> Self {
        Self { table_stat }
    }
}
