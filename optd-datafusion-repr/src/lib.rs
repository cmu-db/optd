#![allow(clippy::new_without_default)]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use cost::OptCostModel;
use optd_core::cascades::CascadesOptimizer;
pub use optd_core::rel_node::Value;
use plan_nodes::{OptRelNodeRef, OptRelNodeTyp};
use properties::schema::{Catalog, Schema, SchemaPropertyBuilder};
use rules::PhysicalConversionRule;

pub mod cost;
pub mod plan_nodes;
pub mod properties;
pub mod rules;

pub struct DatafusionOptimizer {
    optimizer: CascadesOptimizer<OptRelNodeTyp>,
}

impl DatafusionOptimizer {
    pub fn new_physical(catalog: Box<dyn Catalog>) -> Self {
        Self {
            optimizer: CascadesOptimizer::new(
                PhysicalConversionRule::all_conversions(),
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
