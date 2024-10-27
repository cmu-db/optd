#![allow(clippy::new_without_default)]

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use cost::{
    adaptive_cost::DataFusionAdaptiveCostModel, AdaptiveCostModel, DataFusionBaseTableStats,
    RuntimeAdaptionStorage, DEFAULT_DECAY,
};
use optd_core::{
    cascades::{CascadesOptimizer, GroupId, OptimizerProperties},
    heuristics::{ApplyOrder, HeuristicsOptimizer},
    optimizer::Optimizer,
    property::PropertyBuilderAny,
    rel_node::RelNodeMetaMap,
    rules::{Rule, RuleWrapper},
};

use plan_nodes::{OptRelNodeRef, OptRelNodeTyp};
use properties::{
    column_ref::ColumnRefPropertyBuilder,
    schema::{Catalog, SchemaPropertyBuilder},
};
use rules::{
    EliminateDuplicatedAggExprRule, EliminateDuplicatedSortExprRule, EliminateFilterRule,
    EliminateJoinRule, EliminateLimitRule, EliminateProjectRule, FilterAggTransposeRule,
    FilterCrossJoinTransposeRule, FilterInnerJoinTransposeRule, FilterMergeRule,
    FilterProjectTransposeRule, FilterSortTransposeRule, HashJoinRule, JoinAssocRule,
    JoinCommuteRule, PhysicalConversionRule, ProjectMergeRule, ProjectionPullUpJoin,
    SimplifyFilterRule, SimplifyJoinCondRule,
};

pub use optd_core::rel_node::Value;

use crate::rules::{
    DepInitialDistinct, DepJoinEliminateAtScan, DepJoinPastAgg, DepJoinPastFilter, DepJoinPastProj,
};

pub mod cost;
mod explain;
pub mod plan_nodes;
pub mod properties;
pub mod rules;
#[cfg(test)]
mod testing;
// mod expand;

pub struct DatafusionOptimizer {
    heuristic_optimizer: HeuristicsOptimizer<OptRelNodeTyp>,
    cascades_optimizer: CascadesOptimizer<OptRelNodeTyp>,
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

    pub fn optd_cascades_optimizer(&self) -> &CascadesOptimizer<OptRelNodeTyp> {
        &self.cascades_optimizer
    }

    pub fn optd_hueristic_optimizer(&self) -> &HeuristicsOptimizer<OptRelNodeTyp> {
        &self.heuristic_optimizer
    }

    pub fn optd_optimizer_mut(&mut self) -> &mut CascadesOptimizer<OptRelNodeTyp> {
        &mut self.cascades_optimizer
    }

    pub fn default_heuristic_rules(
    ) -> Vec<Arc<dyn Rule<OptRelNodeTyp, HeuristicsOptimizer<OptRelNodeTyp>>>> {
        vec![
            Arc::new(SimplifyFilterRule::new()),
            Arc::new(SimplifyJoinCondRule::new()),
            Arc::new(EliminateFilterRule::new()),
            Arc::new(EliminateJoinRule::new()),
            Arc::new(EliminateLimitRule::new()),
            Arc::new(EliminateDuplicatedSortExprRule::new()),
            Arc::new(EliminateDuplicatedAggExprRule::new()),
            Arc::new(DepJoinEliminateAtScan::new()),
            Arc::new(DepInitialDistinct::new()),
            Arc::new(DepJoinPastProj::new()),
            Arc::new(DepJoinPastFilter::new()),
            Arc::new(DepJoinPastAgg::new()),
            Arc::new(ProjectMergeRule::new()),
            Arc::new(FilterMergeRule::new()),
            // disabled due to logical properties are not implemented in heuristics
            // Arc::new(EliminateProjectRule::new()),
        ]
    }

    pub fn default_cascades_rules(
    ) -> Vec<Arc<RuleWrapper<OptRelNodeTyp, CascadesOptimizer<OptRelNodeTyp>>>> {
        let rules = PhysicalConversionRule::all_conversions();
        let mut rule_wrappers = vec![];
        for rule in rules {
            rule_wrappers.push(RuleWrapper::new_cascades(rule));
        }
        // project transpose rules
        // only do filter-project one way for now to reduce search space
        // rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
        //     ProjectFilterTransposeRule::new(),
        // )));
        // add all filter pushdown rules as heuristic rules
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            FilterProjectTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            FilterCrossJoinTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            FilterInnerJoinTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            FilterSortTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            FilterAggTransposeRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(HashJoinRule::new())));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(JoinCommuteRule::new())));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(JoinAssocRule::new())));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            ProjectionPullUpJoin::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            EliminateProjectRule::new(),
        )));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(ProjectMergeRule::new())));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            EliminateFilterRule::new(),
        )));
        rule_wrappers
    }

    /// Create an optimizer with partial explore (otherwise it's too slow).
    pub fn new_physical(
        catalog: Arc<dyn Catalog>,
        stats: DataFusionBaseTableStats,
        enable_adaptive: bool,
    ) -> Self {
        let cascades_rules = Self::default_cascades_rules();
        let heuristic_rules = Self::default_heuristic_rules();
        let property_builders: Arc<[Box<dyn PropertyBuilderAny<OptRelNodeTyp>>]> = Arc::new([
            Box::new(SchemaPropertyBuilder::new(catalog.clone())),
            Box::new(ColumnRefPropertyBuilder::new(catalog.clone())),
        ]);
        let cost_model = AdaptiveCostModel::new(DEFAULT_DECAY, stats);
        Self {
            runtime_statistics: cost_model.get_runtime_map(),
            cascades_optimizer: CascadesOptimizer::new_with_prop(
                cascades_rules,
                Box::new(cost_model),
                vec![
                    Box::new(SchemaPropertyBuilder::new(catalog.clone())),
                    Box::new(ColumnRefPropertyBuilder::new(catalog.clone())),
                ],
                OptimizerProperties {
                    partial_explore_temporarily_disabled: false,
                    partial_explore_iter: Some(1 << 20),
                    partial_explore_space: Some(1 << 10),
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
        let rules = PhysicalConversionRule::all_conversions();
        let mut rule_wrappers = Vec::new();
        for rule in rules {
            rule_wrappers.push(RuleWrapper::new_cascades(rule));
        }
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(HashJoinRule::new())));
        rule_wrappers.insert(
            0,
            RuleWrapper::new_cascades(Arc::new(JoinCommuteRule::new())),
        );
        rule_wrappers.insert(1, RuleWrapper::new_cascades(Arc::new(JoinAssocRule::new())));
        rule_wrappers.insert(
            2,
            RuleWrapper::new_cascades(Arc::new(ProjectionPullUpJoin::new())),
        );
        rule_wrappers.insert(
            3,
            RuleWrapper::new_heuristic(Arc::new(EliminateFilterRule::new())),
        );

        let cost_model =
            DataFusionAdaptiveCostModel::new(1000, DataFusionBaseTableStats::default()); // very large decay
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

    pub fn heuristic_optimize(&mut self, root_rel: OptRelNodeRef) -> OptRelNodeRef {
        self.heuristic_optimizer
            .optimize(root_rel)
            .expect("heuristics returns error")
    }

    pub fn cascades_optimize(
        &mut self,
        root_rel: OptRelNodeRef,
    ) -> Result<(GroupId, OptRelNodeRef, RelNodeMetaMap)> {
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

    pub fn dump(&self, group_id: Option<GroupId>) {
        self.cascades_optimizer.dump(group_id)
    }
}
