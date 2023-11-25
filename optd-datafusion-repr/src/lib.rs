#![allow(clippy::new_without_default)]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use cost::OptCostModel;
pub use optd_core::rel_node::Value;
use optd_core::{cascades::CascadesOptimizer, optimizer::Optimizer};
use plan_nodes::{OptRelNodeRef, OptRelNodeTyp};
use properties::schema::{Catalog, Schema, SchemaPropertyBuilder};
use rules::{HashJoinRule, JoinAssocRule, JoinCommuteRule, PhysicalConversionRule};

pub mod cost;
pub mod plan_nodes;
pub mod properties;
pub mod rules;

pub struct DatafusionOptimizer {
    optimizer: CascadesOptimizer<OptRelNodeTyp>,
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
        Self {
            optimizer: CascadesOptimizer::new(
                rules,
                Box::new(OptCostModel::new(
                    [("t1", 1000), ("t2", 100), ("t3", 10000)]
                        .into_iter()
                        .map(|(x, y)| (x.to_string(), y))
                        .collect(),
                )),
                vec![Box::new(SchemaPropertyBuilder::new(catalog))],
            ),
        }
    }

    pub fn optimize(&mut self, root_rel: OptRelNodeRef) -> Result<OptRelNodeRef> {
        self.optimizer.optimize(root_rel)
    }

    pub fn dump(&self) {
        self.optimizer.dump()
    }
}
