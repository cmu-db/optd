use std::sync::Arc;

use pretty_xmlish::Pretty;

use optd_core::rel_node::{RelNode, Value};

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

    fn dispatch_explain(&self) -> Pretty<'static> {
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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::childless_record(
            "PhysicalScan",
            vec![("table", self.table().to_string().into())],
        )
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
