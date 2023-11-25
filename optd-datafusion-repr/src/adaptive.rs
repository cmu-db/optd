// TODO: move this to a separate crate

use optd_core::cascades::GroupId;

use pretty_xmlish::Pretty;

use optd_core::rel_node::RelNode;

use crate::plan_nodes::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct PhysicalCollector(pub PlanNode);

impl OptRelNode for PhysicalCollector {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::PhysicalCollector(_)) {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        // Pretty::simple_record(
        //     "PhysicalCollector",
        //     vec![("group_id", self.group_id().to_string().into())],
        //     vec![self.child().explain()],
        // )
        self.child().explain()
    }
}

impl PhysicalCollector {
    pub fn new(child: PlanNode, group_id: GroupId) -> PhysicalCollector {
        PhysicalCollector(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::PhysicalCollector(group_id),
                children: vec![child.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    pub fn group_id(&self) -> GroupId {
        if let OptRelNodeTyp::PhysicalCollector(group_id) = self.clone().into_rel_node().typ {
            group_id
        } else {
            panic!("not a physical collector")
        }
    }

    pub fn child(&self) -> PlanNode {
        PlanNode::from_rel_node(self.clone().into_rel_node().child(0)).unwrap()
    }
}
