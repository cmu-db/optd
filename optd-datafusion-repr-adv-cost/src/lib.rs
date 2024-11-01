use std::sync::{Arc, Mutex};

use adv_stats::{
    stats::{DataFusionBaseTableStats, DataFusionDistribution, DataFusionMostCommonValues},
    AdvStats,
};

use optd_datafusion_repr::{
    cost::{adaptive_cost::RuntimeAdaptionStorageInner, OptCostModel, RuntimeAdaptionStorage},
    plan_nodes::OptRelNodeTyp,
    properties::schema::Catalog,
    DatafusionOptimizer,
};

pub mod adv_stats;

use std::collections::HashMap;

use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::{Cost, CostModel, Statistics},
    rel_node::Value,
};

pub struct AdvancedCostModel {
    base_model: OptCostModel,
    stats: AdvStats<DataFusionMostCommonValues, DataFusionDistribution>,
}

impl AdvancedCostModel {
    pub fn new(stats: DataFusionBaseTableStats) -> Self {
        let stats = AdvStats::new(stats);
        let base_model = OptCostModel::new(HashMap::new());
        Self { base_model, stats }
    }
}

impl CostModel<OptRelNodeTyp> for AdvancedCostModel {
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
        node: &OptRelNodeTyp,
        data: &Option<Value>,
        children: &[Option<&Statistics>],
        children_cost: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        self.base_model.compute_operation_cost(
            node,
            data,
            children,
            children_cost,
            context,
            optimizer,
        )
    }

    fn derive_statistics(
        &self,
        node: &OptRelNodeTyp,
        data: &Option<Value>,
        children: &[&Statistics],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Statistics {
        let row_cnts = children
            .iter()
            .map(|child| OptCostModel::row_cnt(child))
            .collect::<Vec<f64>>();
        match node {
            OptRelNodeTyp::PhysicalScan => {
                let table = data.as_ref().unwrap().as_str();
                let row_cnt = self
                    .stats
                    .per_table_stats_map
                    .get(table.as_ref())
                    .map(|per_table_stats| per_table_stats.row_cnt)
                    .unwrap_or(1) as f64;
                OptCostModel::stat(row_cnt)
            }
            OptRelNodeTyp::PhysicalLimit => {
                let row_cnt = self
                    .stats
                    .get_limit_row_cnt(row_cnts[0], context, optimizer);
                OptCostModel::stat(row_cnt)
            }
            OptRelNodeTyp::PhysicalFilter => {
                let row_cnt = self
                    .stats
                    .get_filter_row_cnt(row_cnts[0], context, optimizer);
                OptCostModel::stat(row_cnt)
            }
            OptRelNodeTyp::PhysicalNestedLoopJoin(join_typ) => {
                let row_cnt = self.stats.get_nlj_row_cnt(
                    *join_typ,
                    row_cnts[0],
                    row_cnts[1],
                    context,
                    optimizer,
                );
                OptCostModel::stat(row_cnt)
            }
            OptRelNodeTyp::PhysicalHashJoin(join_typ) => {
                let row_cnt = self.stats.get_hash_join_row_cnt(
                    *join_typ,
                    row_cnts[0],
                    row_cnts[1],
                    context,
                    optimizer,
                );
                OptCostModel::stat(row_cnt)
            }
            OptRelNodeTyp::PhysicalAgg => {
                let row_cnt = self.stats.get_agg_row_cnt(context, optimizer, row_cnts[0]);
                OptCostModel::stat(row_cnt)
            }
            _ => self
                .base_model
                .derive_statistics(node, data, children, context, optimizer),
        }
    }
}

pub fn new_physical_adv_cost(
    catalog: Arc<dyn Catalog>,
    stats: DataFusionBaseTableStats,
    enable_adaptive: bool,
) -> DatafusionOptimizer {
    let cost_model = AdvancedCostModel::new(stats);
    // This cost model does not accept adaptive (runtime) statistics.
    let runtime_map =
        RuntimeAdaptionStorage::new(Mutex::new(RuntimeAdaptionStorageInner::default()));
    DatafusionOptimizer::new_physical_with_cost_model(
        catalog,
        enable_adaptive,
        cost_model,
        runtime_map,
    )
}
