use pretty_xmlish::Pretty;

use optd_core::rel_node::{RelNode, Value};

use super::{replace_typ, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::childless_record(
            "LogicalEmptyRelation",
            vec![("produce_one_row", self.produce_one_row().to_string().into())],
        )
    }
}

impl LogicalEmptyRelation {
    pub fn new(produce_one_row: bool) -> LogicalEmptyRelation {
        LogicalEmptyRelation(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::EmptyRelation,
                children: vec![],
                data: Some(Value::Bool(produce_one_row)),
            }
            .into(),
        ))
    }

    pub fn produce_one_row(&self) -> bool {
        self.clone()
            .into_rel_node()
            .data
            .as_ref()
            .unwrap()
            .as_bool()
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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::childless_record(
            "PhysicalEmptyRelation",
            vec![("produce_one_row", self.produce_one_row().to_string().into())],
        )
    }
}

impl PhysicalEmptyRelation {
    pub fn new(node: PlanNode) -> PhysicalEmptyRelation {
        Self(node)
    }

    pub fn produce_one_row(&self) -> bool {
        self.clone()
            .into_rel_node()
            .data
            .as_ref()
            .unwrap()
            .as_bool()
    }
}
