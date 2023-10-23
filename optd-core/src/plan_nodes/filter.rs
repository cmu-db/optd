use crate::rel_node::RelNode;

use super::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalFilter(pub PlanNode);

impl OptRelNode for LogicalFilter {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Filter {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }
}

impl LogicalFilter {
    /// Creates a new `LogicalFilter` plan node.
    pub fn new(child: PlanNode, cond: Expr) -> Self {
        LogicalFilter(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::Filter,
                children: vec![child.into_rel_node(), cond.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    /// Gets the child plan node.
    pub fn child(&self) -> PlanNode {
        PlanNode::from_rel_node(self.clone().into_rel_node().children[0].clone()).unwrap()
    }

    /// Gets the filter condition.
    pub fn cond(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().children[1].clone()).unwrap()
    }
}
