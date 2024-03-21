use pretty_xmlish::Pretty;

use bincode;
use optd_core::rel_node::{RelNode, RelNodeMetaMap, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::explain::Insertable;

use super::{replace_typ, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

use crate::properties::schema::Schema;

#[derive(Clone, Debug)]
pub struct LogicalEmptyRelation(pub PlanNode);

impl OptRelNode for LogicalEmptyRelation {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::EmptyRelation {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::childless_record(
            "LogicalEmptyRelation",
            vec![("produce_one_row", self.produce_one_row().to_string().into())],
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmptyRelationData {
    pub produce_one_row: bool,
    pub schema: Schema,
}

impl LogicalEmptyRelation {
    pub fn new(produce_one_row: bool, schema: Schema) -> LogicalEmptyRelation {
        let data = EmptyRelationData {
            produce_one_row,
            schema,
        };
        let serialized_data: Arc<[u8]> = bincode::serialize(&data).unwrap().into_iter().collect();
        LogicalEmptyRelation(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::EmptyRelation,
                children: vec![],
                data: Some(Value::Serialized(serialized_data)),
            }
            .into(),
        ))
    }

    fn get_data(&self) -> EmptyRelationData {
        let serialized_data = self
            .clone()
            .into_rel_node()
            .data
            .as_ref()
            .unwrap()
            .as_slice();

        bincode::deserialize(serialized_data.as_ref()).unwrap()
    }

    pub fn empty_relation_schema(&self) -> Schema {
        let data = self.get_data();
        data.schema
    }

    pub fn produce_one_row(&self) -> bool {
        let data = self.get_data();
        data.produce_one_row
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalEmptyRelation(pub PlanNode);

impl OptRelNode for PhysicalEmptyRelation {
    fn into_rel_node(self) -> OptRelNodeRef {
        replace_typ(self.0.into_rel_node(), OptRelNodeTyp::PhysicalEmptyRelation)
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::PhysicalEmptyRelation {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        let mut fields = vec![("produce_one_row", self.produce_one_row().to_string().into())];
        if let Some(meta_map) = meta_map {
            fields = fields.with_meta(self.0.get_meta(meta_map));
        }
        Pretty::childless_record("PhysicalEmptyRelation", fields)
    }
}

impl PhysicalEmptyRelation {
    pub fn new(node: PlanNode) -> PhysicalEmptyRelation {
        Self(node)
    }

    fn get_data(&self) -> EmptyRelationData {
        let serialized_data = self
            .clone()
            .into_rel_node()
            .data
            .as_ref()
            .unwrap()
            .as_slice();

        bincode::deserialize(serialized_data.as_ref()).unwrap()
    }

    pub fn produce_one_row(&self) -> bool {
        let data = self.get_data();
        data.produce_one_row
    }

    pub fn empty_relation_schema(&self) -> Schema {
        let data = self.get_data();
        data.schema
    }
}
