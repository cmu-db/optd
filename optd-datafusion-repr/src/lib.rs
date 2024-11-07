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
use optd_core::rules::{Rule, RuleWrapper};
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

    pub fn default_cascades_rules(
    ) -> Vec<Arc<RuleWrapper<DfNodeType, CascadesOptimizer<DfNodeType>>>> {
        let rules = rules::PhysicalConversionRule::all_conversions();
        let mut rule_wrappers = vec![];
        for rule in rules {
            rule_wrappers.push(RuleWrapper::new_cascades(rule));
        }
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::FilterProjectTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::FilterCrossJoinTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::FilterInnerJoinTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::FilterSortTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::FilterAggTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::HashJoinRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::JoinCommuteRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::JoinAssocRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::ProjectionPullUpJoin::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::EliminateProjectRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::ProjectMergeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::EliminateLimitRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::EliminateJoinRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::EliminateFilterRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            rules::ProjectFilterTransposeRule::new(),
        )));
        rule_wrappers
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
        let cascades_rules = Self::default_cascades_rules();
        let heuristic_rules = Self::default_heuristic_rules();
        let property_builders: Arc<[Box<dyn PropertyBuilderAny<DfNodeType>>]> = Arc::new([
            Box::new(SchemaPropertyBuilder::new(catalog.clone())),
            Box::new(ColumnRefPropertyBuilder::new(catalog.clone())),
        ]);
        Self {
            runtime_statistics: runtime_map,
            cascades_optimizer: CascadesOptimizer::new_with_prop(
                cascades_rules,
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
        let rules = rules::PhysicalConversionRule::all_conversions();
        let mut rule_wrappers = Vec::new();
        for rule in rules {
            rule_wrappers.push(RuleWrapper::new_cascades(rule));
        }
        // rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(HashJoinRule::new())));
        // rule_wrappers.insert(
        //     0,
        //     RuleWrapper::new_cascades(Arc::new(JoinCommuteRule::new())),
        // );
        // rule_wrappers.insert(1, RuleWrapper::new_cascades(Arc::new(JoinAssocRule::new())));
        // rule_wrappers.insert(
        //     2,
        //     RuleWrapper::new_cascades(Arc::new(ProjectionPullUpJoin::new())),
        // );
        // rule_wrappers.insert(
        //     3,
        //     RuleWrapper::new_heuristic(Arc::new(EliminateFilterRule::new())),
        // );

        let cost_model = AdaptiveCostModel::new(1000);
        let runtime_statistics = cost_model.get_runtime_map();
        let optimizer = CascadesOptimizer::new(
            rule_wrappers,
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
