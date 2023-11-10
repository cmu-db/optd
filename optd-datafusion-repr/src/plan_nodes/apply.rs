use core::fmt;
use std::fmt::Display;

use pretty_xmlish::Pretty;

use optd_core::rel_node::RelNode;

use super::{Expr, JoinType, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ApplyType {
    Cross = 1,
    LeftOuter,
    Semi,
    AntiSemi,
}

impl ApplyType {
    pub fn to_join_type(self) -> JoinType {
        match self {
            Self::Cross => JoinType::Cross,
            Self::LeftOuter => JoinType::LeftOuter,
            Self::Semi => JoinType::Semi,
            Self::AntiSemi => JoinType::AntiSemi,
        }
    }
}

impl Display for ApplyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogicalApply(pub PlanNode);

impl OptRelNode for LogicalApply {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if let OptRelNodeTyp::Apply(_) = rel_node.typ {
            PlanNode::from_rel_node(rel_node).map(Self)
        } else {
            None
        }
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "LogicalApply",
            vec![
                ("typ", self.apply_type().to_string().into()),
                ("cond", self.cond().explain()),
            ],
            vec![self.left_child().explain(), self.right_child().explain()],
        )
    }
}

impl LogicalApply {
    pub fn new(left: PlanNode, right: PlanNode, cond: Expr, apply_type: ApplyType) -> LogicalApply {
        LogicalApply(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::Apply(apply_type),
                children: vec![
                    left.into_rel_node(),
                    right.into_rel_node(),
                    cond.into_rel_node(),
                ],
                data: None,
            }
            .into(),
        ))
    }

    pub fn left_child(&self) -> PlanNode {
        PlanNode::from_rel_node(self.clone().into_rel_node().child(0)).unwrap()
    }

    pub fn right_child(&self) -> PlanNode {
        PlanNode::from_rel_node(self.clone().into_rel_node().child(1)).unwrap()
    }

    pub fn cond(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(2)).unwrap()
    }

    pub fn apply_type(&self) -> ApplyType {
        if let OptRelNodeTyp::Apply(jty) = self.0 .0.typ {
            jty
        } else {
            unreachable!()
        }
    }
}
