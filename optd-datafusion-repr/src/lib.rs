#![allow(clippy::new_without_default)]

use std::sync::Arc;

use anyhow::Result;
use cost::{AdaptiveCostModel, RuntimeAdaptionStorage};
use optd_core::cascades::{CascadesOptimizer, GroupId, OptimizerProperties};
use plan_nodes::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};
use properties::{
    column_ref::ColumnRefPropertyBuilder,
    schema::{Catalog, SchemaPropertyBuilder},
};
use rules::{
    EliminateFilterRule, EliminateJoinRule, HashJoinRule, JoinAssocRule, JoinCommuteRule,
    PhysicalConversionRule, ProjectionPullUpJoin,
};

pub use adaptive::PhysicalCollector;
pub use optd_core::rel_node::Value;

mod adaptive;
pub mod cost;
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

    pub fn optd_optimizer(&self) -> &CascadesOptimizer<OptRelNodeTyp> {
        &self.optimizer
    }

    pub fn optd_optimizer_mut(&mut self) -> &mut CascadesOptimizer<OptRelNodeTyp> {
        &mut self.optimizer
    }

    /// Create an optimizer with default settings: adaptive + partial explore.
    pub fn new_physical(catalog: Arc<dyn Catalog>) -> Self {
        let mut rules = PhysicalConversionRule::all_conversions();
        rules.push(Arc::new(HashJoinRule::new()));
        rules.push(Arc::new(JoinCommuteRule::new()));
        rules.push(Arc::new(JoinAssocRule::new()));
        rules.push(Arc::new(ProjectionPullUpJoin::new()));
        rules.push(Arc::new(EliminateJoinRule::new()));
        rules.push(Arc::new(EliminateFilterRule::new()));

        let cost_model = AdaptiveCostModel::new(50);
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
            enable_adaptive: true,
        }
    }

    /// The optimizer settings for three-join demo as a perfect optimizer.
    pub fn new_alternative_physical_for_demo(catalog: Arc<dyn Catalog>) -> Self {
        let mut rules = PhysicalConversionRule::all_conversions();
        rules.push(Arc::new(HashJoinRule::new()));
        rules.insert(0, Arc::new(JoinCommuteRule::new()));
        rules.insert(1, Arc::new(JoinAssocRule::new()));
        rules.insert(2, Arc::new(ProjectionPullUpJoin::new()));
        rules.insert(3, Arc::new(EliminateFilterRule::new()));

        let cost_model = AdaptiveCostModel::new(1000); // very large decay
        let runtime_statistics = cost_model.get_runtime_map();
        let optimizer = CascadesOptimizer::new(
            rules,
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

    pub fn optimize(&mut self, root_rel: OptRelNodeRef) -> Result<(GroupId, OptRelNodeRef)> {
        if self.enable_adaptive {
            self.runtime_statistics.lock().unwrap().iter_cnt += 1;
            self.optimizer.step_clear_winner();
        } else {
            self.optimizer.step_clear();
        }

        let group_id = self.optimizer.step_optimize_rel(root_rel)?;

        let optimized_rel =
            self.optimizer
                .step_get_optimize_rel(group_id, |rel_node, group_id| {
                    if rel_node.typ.is_plan_node() && self.enable_adaptive {
                        return PhysicalCollector::new(
                            PlanNode::from_rel_node(rel_node).unwrap(),
                            group_id,
                        )
                        .into_rel_node();
                    }
                    rel_node
                })?;

        Ok((group_id, optimized_rel))
    }

    pub fn dump(&self, group_id: Option<GroupId>) {
        self.optimizer.dump(group_id)
    }
}
