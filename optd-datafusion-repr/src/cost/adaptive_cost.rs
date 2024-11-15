// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use optd_core::cascades::{CascadesOptimizer, GroupId, NaiveMemo, RelNodeContext};
use optd_core::cost::{Cost, CostModel, Statistics};

use super::base_cost::DEFAULT_TABLE_ROW_CNT;
use crate::cost::DfCostModel;
use crate::plan_nodes::{ArcDfPredNode, DfNodeType};

pub type RuntimeAdaptionStorage = Arc<Mutex<RuntimeAdaptionStorageInner>>;

#[derive(Default, Debug)]
pub struct RuntimeAdaptionStorageInner {
    pub history: HashMap<GroupId, (usize, usize)>,
    pub iter_cnt: usize,
}

pub struct AdaptiveCostModel {
    runtime_row_cnt: RuntimeAdaptionStorage,
    base_model: DfCostModel,
    decay: usize,
}

impl AdaptiveCostModel {
    fn get_row_cnt(&self, context: &Option<RelNodeContext>) -> f64 {
        let guard = self.runtime_row_cnt.lock().unwrap();
        if let Some((runtime_row_cnt, iter)) =
            guard.history.get(&context.as_ref().unwrap().group_id)
        {
            if *iter + self.decay >= guard.iter_cnt {
                return (*runtime_row_cnt).max(1) as f64;
            }
        }
        DEFAULT_TABLE_ROW_CNT as f64
    }
}

impl CostModel<DfNodeType, NaiveMemo<DfNodeType>> for AdaptiveCostModel {
    fn explain_cost(&self, cost: &Cost) -> String {
        self.base_model.explain_cost(cost)
    }

    fn explain_statistics(&self, cost: &Statistics) -> String {
        self.base_model.explain_statistics(cost)
    }

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost) {
        self.base_model.accumulate(total_cost, cost)
    }

    fn zero(&self) -> Cost {
        self.base_model.zero()
    }

    fn weighted_cost(&self, cost: &Cost) -> f64 {
        self.base_model.weighted_cost(cost)
    }

    fn compute_operation_cost(
        &self,
        node: &DfNodeType,
        predicates: &[ArcDfPredNode],
        children: &[Option<&Statistics>],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<DfNodeType>>,
    ) -> Cost {
        if let DfNodeType::PhysicalScan = node {
            let row_cnt = self.get_row_cnt(&context);
            return DfCostModel::cost(0.0, row_cnt);
        }
        self.base_model
            .compute_operation_cost(node, predicates, children, context, optimizer)
    }

    fn derive_statistics(
        &self,
        node: &DfNodeType,
        predicates: &[ArcDfPredNode],
        children: &[&Statistics],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<DfNodeType>>,
    ) -> Statistics {
        if let DfNodeType::PhysicalScan = node {
            let row_cnt = self.get_row_cnt(&context);
            return DfCostModel::stat(row_cnt);
        }
        self.base_model
            .derive_statistics(node, predicates, children, context, optimizer)
    }
}

impl AdaptiveCostModel {
    pub fn new(decay: usize) -> Self {
        Self {
            runtime_row_cnt: Arc::new(Mutex::new(RuntimeAdaptionStorageInner::default())),
            base_model: DfCostModel::new(HashMap::new()),
            decay,
        }
    }

    pub fn get_runtime_map(&self) -> RuntimeAdaptionStorage {
        self.runtime_row_cnt.clone()
    }
}
