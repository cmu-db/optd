use serde::{Deserialize, Serialize};
use std::sync::Arc;

use optd_core::property::PropertyBuilder;

use super::DEFAULT_NAME;
use crate::plan_nodes::{ConstantType, EmptyRelationData, FuncType, OptRelNodeTyp};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub typ: ConstantType,
    pub nullable: bool,
}

impl Field {
    /// Generate a field that is only a place holder whose members are never used.
    fn placeholder() -> Self {
        Self {
            name: DEFAULT_NAME.to_string(),
            typ: ConstantType::Any,
            nullable: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

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
            OptRelNodeTyp::DepJoin(_) | OptRelNodeTyp::Join(_) => {
                let mut schema = children[0].clone();
                let schema2 = children[1].clone();
                schema.fields.extend(schema2.fields);
                schema
            }
            OptRelNodeTyp::EmptyRelation => {
                let data = data.unwrap().as_slice();
                let empty_relation_data: EmptyRelationData =
                    bincode::deserialize(data.as_ref()).unwrap();
                empty_relation_data.schema
            }
            OptRelNodeTyp::ColumnRef => {
                let data_typ = ConstantType::get_data_type_from_value(&data.unwrap());
                Schema {
                    fields: vec![Field {
                        name: DEFAULT_NAME.to_string(),
                        typ: data_typ,
                        nullable: true,
                    }],
                }
            }
            OptRelNodeTyp::List => {
                let mut fields = vec![];
                for child in children {
                    fields.extend(child.fields.clone());
                }
                Schema { fields }
            }
            OptRelNodeTyp::LogOp(_) => Schema {
                fields: vec![Field::placeholder(); children.len()],
            },
            OptRelNodeTyp::Agg => {
                let mut group_by_schema = children[1].clone();
                let agg_schema = children[2].clone();
                group_by_schema.fields.extend(agg_schema.fields);
                group_by_schema
            }
            OptRelNodeTyp::Cast => Schema {
                fields: children[0]
                    .fields
                    .iter()
                    .map(|field| Field {
                        typ: children[1].fields[0].typ,
                        ..field.clone()
                    })
                    .collect(),
            },
            OptRelNodeTyp::DataType(data_type) => Schema {
                fields: vec![Field {
                    // name and nullable are just placeholders since
                    // they'll be overwritten by Cast
                    name: DEFAULT_NAME.to_string(),
                    typ: ConstantType::from_data_type(data_type),
                    nullable: true,
                }],
            },
            OptRelNodeTyp::Func(FuncType::Agg(_)) => Schema {
                // TODO: this is just a place holder now.
                // The real type should be the column type.
                fields: vec![Field::placeholder()],
            },
            _ => Schema { fields: vec![] },
        }
    }

    fn property_name(&self) -> &'static str {
        "schema"
    }
}
