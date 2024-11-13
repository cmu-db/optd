// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::{Arc, Mutex};

use adv_stats::stats::{
    DataFusionBaseTableStats, DataFusionDistribution, DataFusionMostCommonValues,
};
use adv_stats::AdvStats;
use optd_datafusion_repr::cost::adaptive_cost::RuntimeAdaptionStorageInner;
use optd_datafusion_repr::cost::{DfCostModel, RuntimeAdaptionStorage};
use optd_datafusion_repr::plan_nodes::{ArcDfPredNode, DfNodeType, DfReprPredNode, ListPred};
use optd_datafusion_repr::properties::schema::Catalog;
use optd_datafusion_repr::{DatafusionOptimizer, OptimizerExt};

pub mod adv_stats;

use std::collections::HashMap;

use optd_core::cascades::{CascadesOptimizer, NaiveMemo, RelNodeContext};
use optd_core::cost::{Cost, CostModel, Statistics};

pub struct AdvancedCostModel {
    base_model: DfCostModel,
    stats: AdvStats<DataFusionMostCommonValues, DataFusionDistribution>,
}

impl AdvancedCostModel {
    pub fn new(stats: DataFusionBaseTableStats) -> Self {
        let stats = AdvStats::new(stats);
        let base_model = DfCostModel::new(HashMap::new());
        Self { base_model, stats }
    }
}

impl CostModel<DfNodeType, NaiveMemo<DfNodeType>> for AdvancedCostModel {
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
        children_stats: &[Option<&Statistics>],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<DfNodeType>>,
    ) -> Cost {
        self.base_model
            .compute_operation_cost(node, predicates, children_stats, context, optimizer)
    }

    fn derive_statistics(
        &self,
        node: &DfNodeType,
        predicates: &[ArcDfPredNode],
        children_stats: &[&Statistics],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<DfNodeType>>,
    ) -> Statistics {
        let context = context.as_ref();
        let optimizer = optimizer.as_ref();
        let row_cnts = children_stats
            .iter()
            .map(|child| DfCostModel::row_cnt(child))
            .collect::<Vec<f64>>();
        match node {
            DfNodeType::PhysicalScan => {
                let table = predicates[0].data.as_ref().unwrap().as_str(); // TODO: use df-repr to retrieve it
                let row_cnt = self
                    .stats
                    .per_table_stats_map
                    .get(table.as_ref())
                    .map(|per_table_stats| per_table_stats.row_cnt)
                    .unwrap_or(1) as f64;
                DfCostModel::stat(row_cnt)
            }
            DfNodeType::PhysicalLimit => {
                let row_cnt = self
                    .stats
                    .get_limit_row_cnt(row_cnts[0], predicates[1].clone());
                DfCostModel::stat(row_cnt)
            }
            DfNodeType::PhysicalFilter => {
                let output_schema = optimizer
                    .unwrap()
                    .get_schema_of(context.unwrap().group_id.into());
                let output_column_ref = optimizer
                    .unwrap()
                    .get_column_ref_of(context.unwrap().group_id.into());
                let row_cnt = self.stats.get_filter_row_cnt(
                    row_cnts[0],
                    output_schema,
                    output_column_ref,
                    predicates[0].clone(),
                );
                DfCostModel::stat(row_cnt)
            }
            DfNodeType::PhysicalNestedLoopJoin(join_typ) => {
                let output_schema = optimizer
                    .unwrap()
                    .get_schema_of(context.unwrap().group_id.into());
                let output_column_ref = optimizer
                    .unwrap()
                    .get_column_ref_of(context.unwrap().group_id.into());
                let left_column_ref = optimizer
                    .unwrap()
                    .get_column_ref_of(context.unwrap().children_group_ids[0].into());
                let right_column_ref = optimizer
                    .unwrap()
                    .get_column_ref_of(context.unwrap().children_group_ids[1].into());
                let row_cnt = self.stats.get_nlj_row_cnt(
                    *join_typ,
                    row_cnts[0],
                    row_cnts[1],
                    output_schema,
                    output_column_ref,
                    predicates[0].clone(),
                    left_column_ref,
                    right_column_ref,
                );
                DfCostModel::stat(row_cnt)
            }
            DfNodeType::PhysicalHashJoin(join_typ) => {
                let output_schema = optimizer
                    .unwrap()
                    .get_schema_of(context.unwrap().group_id.into());
                let output_column_ref = optimizer
                    .unwrap()
                    .get_column_ref_of(context.unwrap().group_id.into());
                let left_column_ref = optimizer
                    .unwrap()
                    .get_column_ref_of(context.unwrap().children_group_ids[0].into());
                let right_column_ref = optimizer
                    .unwrap()
                    .get_column_ref_of(context.unwrap().children_group_ids[1].into());
                let row_cnt = self.stats.get_hash_join_row_cnt(
                    *join_typ,
                    row_cnts[0],
                    row_cnts[1],
                    ListPred::from_pred_node(predicates[0].clone()).unwrap(),
                    ListPred::from_pred_node(predicates[1].clone()).unwrap(),
                    output_schema,
                    output_column_ref,
                    left_column_ref,
                    right_column_ref,
                );
                DfCostModel::stat(row_cnt)
            }
            DfNodeType::PhysicalAgg => {
                let output_column_ref = optimizer
                    .unwrap()
                    .get_column_ref_of(context.unwrap().group_id.into());
                let row_cnt = self
                    .stats
                    .get_agg_row_cnt(predicates[1].clone(), output_column_ref);
                DfCostModel::stat(row_cnt)
            }
            _ => self.base_model.derive_statistics(
                node,
                predicates,
                children_stats,
                context.cloned(),
                optimizer.copied(),
            ),
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
