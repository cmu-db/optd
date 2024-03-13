use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::explain::Insertable;
use optd_core::rel_node::{RelNode, RelNodeMetaMap, Value};

use super::{replace_typ, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalScan(pub PlanNode);

impl OptRelNode for LogicalScan {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Scan {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::childless_record(
            "LogicalScan",
            vec![("table", self.table().to_string().into())],
        )
    }
}

impl LogicalScan {
    pub fn new(table: String) -> LogicalScan {
        LogicalScan(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::Scan,
                children: vec![],
                data: Some(Value::String(table.into())),
            }
            .into(),
        ))
    }

    pub fn table(&self) -> Arc<str> {
        self.clone().into_rel_node().data.as_ref().unwrap().as_str()
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalScan(pub PlanNode);

impl OptRelNode for PhysicalScan {
    fn into_rel_node(self) -> OptRelNodeRef {
        replace_typ(self.0.into_rel_node(), OptRelNodeTyp::PhysicalScan)
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::PhysicalScan {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        let mut fields = vec![("table", self.table().to_string().into())];
        if let Some(meta_map) = meta_map {
            fields = fields.with_meta(self.0.get_meta(meta_map));
        }
        Pretty::childless_record("PhysicalScan", fields)
    }
}

impl PhysicalScan {
    pub fn new(node: PlanNode) -> PhysicalScan {
        Self(node)
    }

    pub fn table(&self) -> Arc<str> {
        self.clone().into_rel_node().data.as_ref().unwrap().as_str()
    }
}
