// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

#![allow(clippy::new_without_default)]

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use cost::{AdaptiveCostModel, RuntimeAdaptionStorage};
pub use memo_ext::{LogicalJoinOrder, MemoExt};
use optd_core::cascades::{CascadesOptimizer, GroupId, NaiveMemo, OptimizerProperties};
use optd_core::cost::CostModel;
use optd_core::heuristics::{ApplyOrder, HeuristicsOptimizer};
use optd_core::nodes::PlanNodeMetaMap;
pub use optd_core::nodes::Value;
use optd_core::optimizer::Optimizer;
use optd_core::property::PropertyBuilderAny;
use optd_core::rules::Rule;
pub use optimizer_ext::OptimizerExt;
use plan_nodes::{ArcDfPlanNode, DfNodeType};
use properties::column_ref::ColumnRefPropertyBuilder;
use properties::schema::{Catalog, SchemaPropertyBuilder};

pub mod cost;
mod explain;
mod memo_ext;
mod optimizer_ext;
pub mod plan_nodes;
pub mod properties;
pub mod rules;
mod utils;

#[cfg(test)]
mod testing;

pub struct DatafusionOptimizer {
    heuristic_optimizer: HeuristicsOptimizer<DfNodeType>,
    cascades_optimizer: CascadesOptimizer<DfNodeType>,
    pub runtime_statistics: RuntimeAdaptionStorage,
    enable_adaptive: bool,
    enable_heuristic: bool,
}

impl DatafusionOptimizer {
    pub fn enable_adaptive(&mut self, enable: bool) {
        self.enable_adaptive = enable;
    }

    pub fn adaptive_enabled(&self) -> bool {
        self.enable_adaptive
    }

    pub fn enable_heuristic(&mut self, enable: bool) {
        self.enable_heuristic = enable;
    }

    pub fn is_heuristic_enabled(&self) -> bool {
        self.enable_heuristic
    }

    pub fn optd_cascades_optimizer(&self) -> &CascadesOptimizer<DfNodeType> {
        &self.cascades_optimizer
    }

    pub fn optd_hueristic_optimizer(&self) -> &HeuristicsOptimizer<DfNodeType> {
        &self.heuristic_optimizer
    }

    pub fn optd_optimizer_mut(&mut self) -> &mut CascadesOptimizer<DfNodeType> {
        &mut self.cascades_optimizer
    }

    pub fn default_heuristic_rules(
    ) -> Vec<Arc<dyn Rule<DfNodeType, HeuristicsOptimizer<DfNodeType>>>> {
        vec![
            Arc::new(rules::EliminateProjectRule::new()),
            Arc::new(rules::SimplifyFilterRule::new()),
            Arc::new(rules::SimplifyJoinCondRule::new()),
            Arc::new(rules::EliminateFilterRule::new()),
            Arc::new(rules::EliminateJoinRule::new()),
            Arc::new(rules::EliminateLimitRule::new()),
            Arc::new(rules::EliminateDuplicatedSortExprRule::new()),
            Arc::new(rules::EliminateDuplicatedAggExprRule::new()),
            Arc::new(rules::DepJoinEliminate::new()),
            Arc::new(rules::DepInitialDistinct::new()),
            Arc::new(rules::DepJoinPastProj::new()),
            Arc::new(rules::DepJoinPastFilter::new()),
            Arc::new(rules::DepJoinPastAgg::new()),
            Arc::new(rules::ProjectMergeRule::new()),
            Arc::new(rules::FilterMergeRule::new()),
        ]
    }

    pub fn default_cascades_rules() -> (
        Vec<Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>>,
        Vec<Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>>,
    ) {
        // Transformation rules (logical->logical)
        let mut transformation_rules: Vec<
            Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>,
        > = vec![];
        // TODO: There's a significant cycle issue in this PR!
        // transformation_rules.push(Arc::new(rules::FilterProjectTransposeRule::new()));
        transformation_rules.push(Arc::new(rules::FilterCrossJoinTransposeRule::new()));
        transformation_rules.push(Arc::new(rules::FilterInnerJoinTransposeRule::new()));
        transformation_rules.push(Arc::new(rules::FilterSortTransposeRule::new()));
        transformation_rules.push(Arc::new(rules::FilterAggTransposeRule::new()));
        transformation_rules.push(Arc::new(rules::JoinCommuteRule::new()));
        transformation_rules.push(Arc::new(rules::JoinAssocRule::new()));
        // transformation_rules.push(Arc::new(rules::ProjectionPullUpJoin::new()));
        transformation_rules.push(Arc::new(rules::EliminateProjectRule::new()));
        // transformation_rules.push(Arc::new(rules::ProjectMergeRule::new()));
        transformation_rules.push(Arc::new(rules::EliminateLimitRule::new()));
        transformation_rules.push(Arc::new(rules::EliminateJoinRule::new()));
        transformation_rules.push(Arc::new(rules::EliminateFilterRule::new()));
        // transformation_rules.push(Arc::new(rules::ProjectFilterTransposeRule::new()));

        // Implementation rules (logical->physical)
        let mut implementation_rules: Vec<
            Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>,
        > = rules::PhysicalConversionRule::all_conversions();
        implementation_rules.push(Arc::new(rules::HashJoinRule::new()));

        (transformation_rules, implementation_rules)
    }

    /// Create an optimizer with partial explore (otherwise it's too slow).
    pub fn new_physical(catalog: Arc<dyn Catalog>, enable_adaptive: bool) -> Self {
        let cost_model = AdaptiveCostModel::new(50);
        let map = cost_model.get_runtime_map();
        Self::new_physical_with_cost_model(catalog, enable_adaptive, cost_model, map)
    }

    pub fn new_physical_with_cost_model(
        catalog: Arc<dyn Catalog>,
        enable_adaptive: bool,
        cost_model: impl CostModel<DfNodeType, NaiveMemo<DfNodeType>>,
        runtime_map: RuntimeAdaptionStorage,
    ) -> Self {
        let (cascades_transformation_rules, cascades_implementation_rules) =
            Self::default_cascades_rules();
        let heuristic_rules = Self::default_heuristic_rules();
        let property_builders: Arc<[Box<dyn PropertyBuilderAny<DfNodeType>>]> = Arc::new([
            Box::new(SchemaPropertyBuilder::new(catalog.clone())),
            Box::new(ColumnRefPropertyBuilder::new(catalog.clone())),
        ]);
        Self {
            runtime_statistics: runtime_map,
            cascades_optimizer: CascadesOptimizer::new_with_prop(
                cascades_transformation_rules.into(),
                cascades_implementation_rules.into(),
                Box::new(cost_model),
                vec![
                    Box::new(SchemaPropertyBuilder::new(catalog.clone())),
                    Box::new(ColumnRefPropertyBuilder::new(catalog.clone())),
                ],
                OptimizerProperties {
                    panic_on_budget: false,
                    partial_explore_iter: Some(1 << 20),
                    partial_explore_space: Some(1 << 10),
                    disable_pruning: false,
                },
            ),
            heuristic_optimizer: HeuristicsOptimizer::new_with_rules(
                heuristic_rules,
                ApplyOrder::TopDown, // uhh TODO reconsider
                property_builders.clone(),
            ),
            enable_adaptive,
            enable_heuristic: true,
        }
    }

    /// The optimizer settings for three-join demo as a perfect optimizer.
    pub fn new_alternative_physical_for_demo(catalog: Arc<dyn Catalog>) -> Self {
        let mut impl_rules: Vec<Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>> =
            rules::PhysicalConversionRule::all_conversions();

        let mut transform_rules: Vec<Arc<dyn Rule<DfNodeType, CascadesOptimizer<DfNodeType>>>> =
            vec![];

        impl_rules.push(Arc::new(rules::HashJoinRule::new()));
        transform_rules.insert(0, Arc::new(rules::JoinCommuteRule::new()));
        transform_rules.insert(1, Arc::new(rules::JoinAssocRule::new()));
        transform_rules.insert(2, Arc::new(rules::ProjectionPullUpJoin::new()));
        transform_rules.insert(3, Arc::new(rules::EliminateFilterRule::new()));

        let cost_model = AdaptiveCostModel::new(1000);
        let runtime_statistics = cost_model.get_runtime_map();
        let optimizer = CascadesOptimizer::new(
            impl_rules.into(),
            transform_rules.into(),
            Box::new(cost_model),
            vec![
                Box::new(SchemaPropertyBuilder::new(catalog.clone())),
                Box::new(ColumnRefPropertyBuilder::new(catalog)),
            ],
        );
        Self {
            runtime_statistics,
            cascades_optimizer: optimizer,
            enable_adaptive: true,
            enable_heuristic: false,
            heuristic_optimizer: HeuristicsOptimizer::new_with_rules(
                vec![],
                ApplyOrder::BottomUp,
                Arc::new([]),
            ),
        }
    }

    pub fn heuristic_optimize(&mut self, root_rel: ArcDfPlanNode) -> ArcDfPlanNode {
        self.heuristic_optimizer
            .optimize(root_rel)
            .expect("heuristics returns error")
    }

    pub fn cascades_optimize(
        &mut self,
        root_rel: ArcDfPlanNode,
    ) -> Result<(GroupId, ArcDfPlanNode, PlanNodeMetaMap)> {
        if self.enable_adaptive {
            self.runtime_statistics.lock().unwrap().iter_cnt += 1;
            self.cascades_optimizer.step_clear_winner();
        } else {
            self.cascades_optimizer.step_clear();
        }

        let group_id = self.cascades_optimizer.step_optimize_rel(root_rel)?;

        let mut meta = Some(HashMap::new());
        let optimized_rel = self
            .cascades_optimizer
            .step_get_optimize_rel(group_id, &mut meta)?;

        Ok((group_id, optimized_rel, meta.unwrap()))
    }
}
