use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use optd_core::property::PropertyBuilder;

use crate::plan_nodes::{ConstantType, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct Schema(pub Vec<ConstantType>);

impl Schema {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

pub trait Catalog: Send + Sync + 'static {
    fn get(&self, name: &str) -> Schema;
}

pub struct SchemaPropertyBuilder {
    catalog: Box<dyn Catalog>,
}

impl SchemaPropertyBuilder {
    pub fn new(catalog: Box<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

impl PropertyBuilder<OptRelNodeTyp> for SchemaPropertyBuilder {
    type Prop = Schema;

    fn derive(
        &self,
        typ: OptRelNodeTyp,
        data: Option<optd_core::rel_node::Value>,
        children: &[&Self::Prop],
    ) -> Self::Prop {
        match typ {
            OptRelNodeTyp::Scan => {
                let name = data.unwrap().as_str().to_string();
                self.catalog.get(&name)
            }
            OptRelNodeTyp::Projection => children[1].clone(),
            OptRelNodeTyp::Filter => children[0].clone(),
            OptRelNodeTyp::Join(_) => {
                let mut schema = children[0].clone();
                schema.0.extend(children[1].clone().0);
                schema
            }
            OptRelNodeTyp::List => Schema(vec![ConstantType::Any; children.len()]),
            _ => Schema(vec![]),
        }
    }

    fn property_name(&self) -> &'static str {
        "schema"
    }
}
