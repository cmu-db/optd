use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{cost::OptCostModel, plan_nodes::OptRelNodeTyp};
use optd_core::{
    cascades::{CascadesOptimizer, GroupId, RelNodeContext},
    cost::{Cost, CostModel},
    rel_node::{RelNode, Value},
};
use serde::{de::DeserializeOwned, Serialize};

use super::base_cost::stats::{
    BaseTableStats, DataFusionDistribution, DataFusionMostCommonValues, Distribution,
    MostCommonValues,
};

pub type RuntimeAdaptionStorage = Arc<Mutex<RuntimeAdaptionStorageInner>>;
pub type DataFusionAdaptiveCostModel =
    AdaptiveCostModel<DataFusionMostCommonValues, DataFusionDistribution>;

#[derive(Default, Debug)]
pub struct RuntimeAdaptionStorageInner {
    pub history: HashMap<GroupId, (usize, usize)>,
    pub iter_cnt: usize,
}

pub const DEFAULT_DECAY: usize = 50;

pub struct AdaptiveCostModel<
    M: MostCommonValues + Serialize + DeserializeOwned,
    D: Distribution + Serialize + DeserializeOwned,
> {
    runtime_row_cnt: RuntimeAdaptionStorage,
    base_model: OptCostModel<M, D>,
    decay: usize,
}

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > CostModel<OptRelNodeTyp> for AdaptiveCostModel<M, D>
{
    fn explain(&self, cost: &Cost) -> String {
        self.base_model.explain(cost)
    }

    fn accumulate(&self, total_cost: &mut Cost, cost: &Cost) {
        self.base_model.accumulate(total_cost, cost)
    }

    fn zero(&self) -> Cost {
        self.base_model.zero()
    }

    fn compute_cost(
        &self,
        node: &OptRelNodeTyp,
        data: &Option<Value>,
        children: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        if let OptRelNodeTyp::PhysicalScan = node {
            let guard = self.runtime_row_cnt.lock().unwrap();
            if let Some((runtime_row_cnt, iter)) =
                guard.history.get(&context.as_ref().unwrap().group_id)
            {
                if *iter + self.decay >= guard.iter_cnt {
                    let runtime_row_cnt = (*runtime_row_cnt).max(1) as f64;
                    return OptCostModel::<M, D>::cost(runtime_row_cnt, 0.0, runtime_row_cnt);
                }
            }
        }
        let (mut row_cnt, compute_cost, io_cost) = OptCostModel::<M, D>::cost_tuple(
            &self
                .base_model
                .compute_cost(node, data, children, context.clone(), optimizer),
        );
        if let Some(context) = context {
            let guard = self.runtime_row_cnt.lock().unwrap();
            if let Some((runtime_row_cnt, iter)) = guard.history.get(&context.group_id) {
                if *iter + self.decay >= guard.iter_cnt {
                    let runtime_row_cnt = (*runtime_row_cnt).max(1) as f64;
                    row_cnt = runtime_row_cnt;
                }
            }
        }
        OptCostModel::<M, D>::cost(row_cnt, compute_cost, io_cost)
    }

    fn compute_plan_node_cost(&self, node: &RelNode<OptRelNodeTyp>) -> Cost {
        self.base_model.compute_plan_node_cost(node)
    }
}

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > AdaptiveCostModel<M, D>
{
    pub fn new(decay: usize, stats: BaseTableStats<M, D>) -> Self {
        Self {
            runtime_row_cnt: RuntimeAdaptionStorage::default(),
            base_model: OptCostModel::new(stats),
            decay,
        }
    }

    pub fn get_runtime_map(&self) -> RuntimeAdaptionStorage {
        self.runtime_row_cnt.clone()
    }
}
