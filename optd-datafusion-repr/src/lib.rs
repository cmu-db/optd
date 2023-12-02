#![allow(clippy::new_without_default)]

use std::sync::Arc;

use anyhow::Result;
use cost::{AdaptiveCostModel, RuntimeAdaptionStorage};
use optd_core::cascades::{CascadesOptimizer, GroupId};
use plan_nodes::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};
use properties::schema::{Catalog, SchemaPropertyBuilder};
use rules::{
    HashJoinRule, JoinAssocRule, JoinCommuteRule, PhysicalConversionRule, ProjectionPullUpJoin,
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
    pub fn optd_optimizer(&self) -> &CascadesOptimizer<OptRelNodeTyp> {
        &self.optimizer
    }

    pub fn new_physical(catalog: Box<dyn Catalog>) -> Self {
        let mut rules = PhysicalConversionRule::all_conversions();
        rules.push(Arc::new(HashJoinRule::new()));
        rules.push(Arc::new(JoinCommuteRule::new()));
        rules.push(Arc::new(JoinAssocRule::new()));
        rules.push(Arc::new(ProjectionPullUpJoin::new()));
        let cost_model = AdaptiveCostModel::new();
        Self {
            runtime_statistics: cost_model.get_runtime_map(),
            optimizer: CascadesOptimizer::new(
                rules,
                Box::new(cost_model),
                vec![Box::new(SchemaPropertyBuilder::new(catalog))],
            ),
            enable_adaptive: true,
        }
    }

    pub fn optimize(&mut self, root_rel: OptRelNodeRef) -> Result<(GroupId, OptRelNodeRef)> {
        self.runtime_statistics.lock().unwrap().iter_cnt += 1;
        self.optimizer
            .optimize_with_on_produce_callback(root_rel, |rel_node, group_id| {
                if rel_node.typ.is_plan_node() && self.enable_adaptive {
                    return PhysicalCollector::new(
                        PlanNode::from_rel_node(rel_node).unwrap(),
                        group_id,
                    )
                    .into_rel_node();
                }
                rel_node
            })
    }

    pub fn dump(&self, group_id: Option<GroupId>) {
        self.optimizer.dump(group_id)
    }
}
