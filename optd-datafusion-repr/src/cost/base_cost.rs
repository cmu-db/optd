// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::HashMap;

use itertools::Itertools;
use optd_core::cascades::{CascadesOptimizer, NaiveMemo, RelNodeContext};
use optd_core::cost::{Cost, CostModel, Statistics};

use crate::plan_nodes::{ArcDfPredNode, ConstantPred, DfNodeType, DfReprPredNode};

#[derive(Debug, Clone)]
pub struct DfStatistics {
    row_cnt: f64,
}

pub struct DfCostModel {
    table_stat: HashMap<String, usize>,
}

pub const COMPUTE_COST: usize = 0;
pub const IO_COST: usize = 1;

pub(crate) const DEFAULT_TABLE_ROW_CNT: usize = 1000;

impl DfCostModel {
    pub fn compute_cost(Cost(cost): &Cost) -> f64 {
        cost[COMPUTE_COST]
    }

    pub fn io_cost(Cost(cost): &Cost) -> f64 {
        cost[IO_COST]
    }

    pub fn row_cnt(Statistics(stat): &Statistics) -> f64 {
        stat.downcast_ref::<DfStatistics>().unwrap().row_cnt
    }

    pub fn cost(compute_cost: f64, io_cost: f64) -> Cost {
        Cost(vec![compute_cost, io_cost])
    }

    pub fn stat(row_cnt: f64) -> Statistics {
        Statistics(Box::new(DfStatistics { row_cnt }))
    }

    pub fn cost_tuple(Cost(cost): &Cost) -> (f64, f64) {
        (cost[COMPUTE_COST], cost[IO_COST])
    }

    fn get_row_cnt(&self, predicates: &[ArcDfPredNode]) -> f64 {
        let table_name = ConstantPred::from_pred_node(predicates[0].clone())
            .unwrap()
            .value()
            .as_str();
        self.table_stat
            .get(table_name.as_ref())
            .copied()
            .unwrap_or(DEFAULT_TABLE_ROW_CNT) as f64
    }
}

impl CostModel<DfNodeType, NaiveMemo<DfNodeType>> for DfCostModel {
    fn explain_cost(&self, cost: &Cost) -> String {
        format!(
            "{{compute={},io={}}}",
            Self::compute_cost(cost),
            Self::io_cost(cost)
        )
    }

    fn explain_statistics(&self, stat: &Statistics) -> String {
        format!("{{row_cnt={}}}", Self::row_cnt(stat))
    }

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost) {
        total_cost.0[COMPUTE_COST] += Self::compute_cost(cost);
        total_cost.0[IO_COST] += Self::io_cost(cost);
    }

    fn zero(&self) -> Cost {
        Cost(vec![0.0, 0.0])
    }

    fn derive_statistics(
        &self,
        node: &DfNodeType,
        predicates: &[ArcDfPredNode],
        children: &[&Statistics],
        _context: RelNodeContext,
        _optimizer: &CascadesOptimizer<DfNodeType>,
    ) -> Statistics {
        match node {
            DfNodeType::PhysicalScan => {
                let row_cnt = self.get_row_cnt(predicates);
                Self::stat(row_cnt)
            }
            DfNodeType::PhysicalLimit => {
                let row_cnt = Self::row_cnt(children[0]);
                let selectivity = 0.001;
                Self::stat((row_cnt * selectivity).max(1.0))
            }
            DfNodeType::PhysicalEmptyRelation => Self::stat(0.01),
            DfNodeType::PhysicalFilter => {
                let row_cnt = Self::row_cnt(children[0]);
                let selectivity = 0.001;
                Self::stat((row_cnt * selectivity).max(1.0))
            }
            DfNodeType::PhysicalNestedLoopJoin(_) => {
                let row_cnt_1 = Self::row_cnt(children[0]);
                let row_cnt_2 = Self::row_cnt(children[1]);
                let selectivity = 0.01;
                Self::stat((row_cnt_1 * row_cnt_2 * selectivity).max(1.0))
            }
            DfNodeType::PhysicalHashJoin(_) => {
                let row_cnt_1 = Self::row_cnt(children[0]);
                let row_cnt_2 = Self::row_cnt(children[1]);
                Self::stat(row_cnt_1.min(row_cnt_2).max(1.0))
            }
            DfNodeType::PhysicalSort | DfNodeType::PhysicalAgg | DfNodeType::PhysicalProjection => {
                let row_cnt = Self::row_cnt(children[0]);
                Self::stat(row_cnt)
            }
            x => unimplemented!("cannot derive statistics for {}", x),
        }
    }

    fn compute_operation_cost(
        &self,
        node: &DfNodeType,
        predicates: &[ArcDfPredNode],
        children: &[Option<&Statistics>],
        _context: RelNodeContext,
        _optimizer: &CascadesOptimizer<DfNodeType>,
    ) -> Cost {
        let row_cnts = children
            .iter()
            .map(|child| child.map(Self::row_cnt).unwrap_or(0 as f64))
            .collect_vec();
        match node {
            DfNodeType::PhysicalScan => {
                let row_cnt = self.get_row_cnt(predicates);
                Self::cost(0.0, row_cnt)
            }
            DfNodeType::PhysicalLimit => {
                let row_cnt = row_cnts[0];
                Self::cost(row_cnt, 0.0)
            }
            DfNodeType::PhysicalEmptyRelation => Self::cost(0.01, 0.0),
            DfNodeType::PhysicalFilter => {
                let row_cnt = row_cnts[0];
                let (compute_cost, _) = Self::cost_tuple(&derive_pred_cost(&predicates[0]));
                Self::cost(row_cnt * compute_cost, 0.0)
            }
            DfNodeType::PhysicalNestedLoopJoin(_) => {
                let row_cnt_1 = row_cnts[0];
                let row_cnt_2 = row_cnts[1];
                let (compute_cost, _) = Self::cost_tuple(&derive_pred_cost(&predicates[0]));
                Self::cost(row_cnt_1 * row_cnt_2 * compute_cost + row_cnt_1, 0.0)
            }
            DfNodeType::PhysicalProjection => {
                let row_cnt = row_cnts[0];
                let (compute_cost, _) = Self::cost_tuple(&derive_pred_cost(&predicates[0]));
                Self::cost(row_cnt * compute_cost, 0.0)
            }
            DfNodeType::PhysicalHashJoin(_) => {
                let row_cnt_1 = row_cnts[0];
                let row_cnt_2 = row_cnts[1];
                Self::cost(row_cnt_1 * 2.0 + row_cnt_2, 0.0)
            }
            DfNodeType::PhysicalSort => {
                let row_cnt = row_cnts[0];
                Self::cost(row_cnt * row_cnt.ln_1p().max(1.0), 0.0)
            }
            DfNodeType::PhysicalAgg => {
                let row_cnt = row_cnts[0];
                let (compute_cost_1, _) = Self::cost_tuple(&derive_pred_cost(&predicates[0]));
                let (compute_cost_2, _) = Self::cost_tuple(&derive_pred_cost(&predicates[1]));
                Self::cost(row_cnt * (compute_cost_1 + compute_cost_2), 0.0)
            }
            x => unimplemented!("cannot compute cost for {}", x),
        }
    }

    fn weighted_cost(&self, cost: &Cost) -> f64 {
        Self::compute_cost(cost) + Self::io_cost(cost)
    }
}

fn derive_pred_cost(pred: &ArcDfPredNode) -> Cost {
    let compute_cost = pred
        .children
        .iter()
        .map(|child| {
            let (compute_cost, _) = DfCostModel::cost_tuple(&derive_pred_cost(child));
            compute_cost
        })
        .sum::<f64>();
    DfCostModel::cost(compute_cost + 1.0, 0.0)
}

impl DfCostModel {
    pub fn new(table_stat: HashMap<String, usize>) -> Self {
        Self { table_stat }
    }
}
