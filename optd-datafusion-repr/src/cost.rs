use std::collections::HashMap;

use crate::plan_nodes::OptRelNodeTyp;
use itertools::Itertools;
use optd_core::{
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
    let cost = model.compute_cost(&node.typ, &node.data, &children);
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
    fn row_cnt(Cost(cost): &Cost) -> f64 {
        cost[ROW_COUNT]
    }

    fn compute_cost(Cost(cost): &Cost) -> f64 {
        cost[COMPUTE_COST]
    }

    fn io_cost(Cost(cost): &Cost) -> f64 {
        cost[IO_COST]
    }

    fn cost_tuple(Cost(cost): &Cost) -> (f64, f64, f64) {
        (cost[ROW_COUNT], cost[COMPUTE_COST], cost[IO_COST])
    }

    fn weighted_cost(row_cnt: f64, compute_cost: f64, io_cost: f64) -> f64 {
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
        total_cost.0[ROW_COUNT] += Self::row_cnt(cost);
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

    fn compute_cost(&self, node: &OptRelNodeTyp, data: &Option<Value>, children: &[Cost]) -> Cost {
        match node {
            OptRelNodeTyp::PhysicalScan => {
                let table_name = data.as_ref().unwrap().as_str();
                let row_cnt = self.table_stat.get(table_name.as_ref()).copied().unwrap() as f64;
                Self::cost(row_cnt, 0.0, row_cnt)
            }
            OptRelNodeTyp::PhysicalFilter => {
                let (row_cnt, _, _) = Self::cost_tuple(&children[0]);
                let selectivity = 0.1;
                Self::cost(row_cnt * selectivity, row_cnt, 0.0)
            }
            OptRelNodeTyp::PhysicalNestedLoopJoin(_) => {
                let (row_cnt_1, _, _) = Self::cost_tuple(&children[0]);
                let (row_cnt_2, _, _) = Self::cost_tuple(&children[1]);
                let selectivity = 0.1;
                Self::cost(
                    row_cnt_1 * row_cnt_2 * selectivity,
                    row_cnt_1 * row_cnt_2,
                    0.0,
                )
            }
            _ if node.is_expression() => Self::cost(0.0, 0.0, 0.0),
            _ => Self::cost(1.0, 0.0, 0.0),
        }
    }

    fn compute_plan_node_cost(&self, node: &RelNode<OptRelNodeTyp>) -> Cost {
        let mut cost = self.zero();
        compute_plan_node_cost(self, node, &mut cost);
        cost
    }
}

impl OptCostModel {
    pub fn new(table_stat: HashMap<String, usize>) -> Self {
        Self { table_stat }
    }
}
