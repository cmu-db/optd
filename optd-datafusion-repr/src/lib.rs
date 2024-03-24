#![allow(clippy::new_without_default)]

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use cost::{
    adaptive_cost::DataFusionAdaptiveCostModel, base_cost::DataFusionBaseTableStats,
    AdaptiveCostModel, BaseTableStats, RuntimeAdaptionStorage, DEFAULT_DECAY,
};
use optd_core::{
    cascades::{CascadesOptimizer, GroupId, OptimizerProperties},
    rel_node::RelNodeMetaMap,
    rules::RuleWrapper,
};

use plan_nodes::{OptRelNodeRef, OptRelNodeTyp};
use properties::{
    column_ref::ColumnRefPropertyBuilder,
    schema::{Catalog, SchemaPropertyBuilder},
};
use rules::{
    EliminateDuplicatedAggExprRule, EliminateDuplicatedSortExprRule, EliminateFilterRule,
    EliminateJoinRule, EliminateLimitRule, HashJoinRule, JoinAssocRule, JoinCommuteRule,
    PhysicalConversionRule, ProjectionPullUpJoin, SimplifyFilterRule, SimplifyJoinCondRule,
};

pub use optd_core::rel_node::Value;

pub mod cost;
mod explain;
pub mod plan_nodes;
pub mod properties;
pub mod rules;

pub struct DatafusionOptimizer {
    optimizer: CascadesOptimizer<OptRelNodeTyp>,
    pub runtime_statistics: RuntimeAdaptionStorage,
    enable_adaptive: bool,
}

impl DatafusionOptimizer {
    pub fn enable_adaptive(&mut self, enable: bool) {
        self.enable_adaptive = enable;
    }

    pub fn adaptive_enabled(&self) -> bool {
        self.enable_adaptive
    }

    pub fn optd_optimizer(&self) -> &CascadesOptimizer<OptRelNodeTyp> {
        &self.optimizer
    }

    pub fn optd_optimizer_mut(&mut self) -> &mut CascadesOptimizer<OptRelNodeTyp> {
        &mut self.optimizer
    }

    pub fn default_rules() -> Vec<Arc<RuleWrapper<OptRelNodeTyp, CascadesOptimizer<OptRelNodeTyp>>>>
    {
        let rules = PhysicalConversionRule::all_conversions();
        let mut rule_wrappers = vec![
            RuleWrapper::new_heuristic(Arc::new(SimplifyFilterRule::new())),
            RuleWrapper::new_heuristic(Arc::new(SimplifyJoinCondRule::new())),
            RuleWrapper::new_heuristic(Arc::new(EliminateFilterRule::new())),
            RuleWrapper::new_heuristic(Arc::new(EliminateJoinRule::new())),
            RuleWrapper::new_heuristic(Arc::new(EliminateLimitRule::new())),
            RuleWrapper::new_heuristic(Arc::new(EliminateDuplicatedSortExprRule::new())),
            RuleWrapper::new_heuristic(Arc::new(EliminateDuplicatedAggExprRule::new())),
        ];
        for rule in rules {
            rule_wrappers.push(RuleWrapper::new_cascades(rule));
        }
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(HashJoinRule::new()))); // 17
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(JoinCommuteRule::new()))); // 18
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(JoinAssocRule::new())));
        rule_wrappers.push(RuleWrapper::new_cascades(Arc::new(
            ProjectionPullUpJoin::new(),
        )));

        rule_wrappers
    }

    /// Create an optimizer with partial explore (otherwise it's too slow).
    pub fn new_physical(
        catalog: Arc<dyn Catalog>,
        stats: DataFusionBaseTableStats,
        enable_adaptive: bool,
    ) -> Self {
        let rules = Self::default_rules();
        let cost_model = AdaptiveCostModel::new(DEFAULT_DECAY, stats);
        Self {
            runtime_statistics: cost_model.get_runtime_map(),
            optimizer: CascadesOptimizer::new_with_prop(
                rules,
                Box::new(cost_model),
                vec![
                    Box::new(SchemaPropertyBuilder::new(catalog.clone())),
                    Box::new(ColumnRefPropertyBuilder::new(catalog)),
                ],
                OptimizerProperties {
                    partial_explore_iter: Some(1 << 20),
                    partial_explore_space: Some(1 << 10),
                },
            ),
            enable_adaptive,
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

        let cost_model = DataFusionAdaptiveCostModel::new(1000, BaseTableStats::default()); // very large decay
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
            optimizer,
            enable_adaptive: true,
        }
    }

    pub fn optimize(
        &mut self,
        root_rel: OptRelNodeRef,
    ) -> Result<(GroupId, OptRelNodeRef, RelNodeMetaMap)> {
        if self.enable_adaptive {
            self.runtime_statistics.lock().unwrap().iter_cnt += 1;
            self.optimizer.step_clear_winner();
        } else {
            self.optimizer.step_clear();
        }

        let group_id = self.optimizer.step_optimize_rel(root_rel)?;

        let mut meta = Some(HashMap::new());
        let optimized_rel = self.optimizer.step_get_optimize_rel(group_id, &mut meta)?;

        Ok((group_id, optimized_rel, meta.unwrap()))
    }

    pub fn dump(&self, group_id: Option<GroupId>) {
        self.optimizer.dump(group_id)
    }
}
