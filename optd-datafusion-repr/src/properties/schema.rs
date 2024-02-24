use std::sync::Arc;

use optd_core::property::PropertyBuilder;

use crate::plan_nodes::{ConstantType, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct Field {
    pub name: String,
    pub typ: ConstantType,
    pub nullable: bool,
}
#[derive(Clone, Debug)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait Catalog: Send + Sync + 'static {
    fn get(&self, name: &str) -> Schema;
}

pub struct SchemaPropertyBuilder {
    catalog: Arc<dyn Catalog>,
}

impl SchemaPropertyBuilder {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
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
                let schema2 = children[1].clone();
                schema.fields.extend(schema2.fields);
                schema
            }
            OptRelNodeTyp::List => {
                // TODO: calculate real is_nullable for aggregations
                Schema {
                    fields: vec![
                        Field {
                            name: "unnamed".to_string(),
                            typ: ConstantType::Any,
                            nullable: true
                        };
                        children.len()
                    ],
                }
            }
            _ => Schema { fields: vec![] },
        }
    }

    fn property_name(&self) -> &'static str {
        "schema"
    }
}
