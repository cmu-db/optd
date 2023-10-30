use core::fmt;
use std::fmt::Display;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use pretty_xmlish::Pretty;

use crate::rel_node::RelNode;

use super::{replace_typ, Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(FromPrimitive, Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner = 1,
    FullOuter,
    LeftOuter,
    RightOuter,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogicalJoin(pub PlanNode);

impl OptRelNode for LogicalJoin {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if let OptRelNodeTyp::Join(_) = rel_node.typ {
            PlanNode::from_rel_node(rel_node).map(Self)
        } else {
            None
        }
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "LogicalJoin",
            vec![
                ("typ", self.join_type().to_string().into()),
                ("cond", self.cond().explain()),
            ],
            vec![self.left_child().explain(), self.right_child().explain()],
        )
    }
}

impl LogicalJoin {
    pub fn new(left: PlanNode, right: PlanNode, cond: Expr, join_type: JoinType) -> LogicalJoin {
        LogicalJoin(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::Join(join_type),
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
        PlanNode::from_rel_node(self.clone().into_rel_node().children[0].clone()).unwrap()
    }

    pub fn right_child(&self) -> PlanNode {
        PlanNode::from_rel_node(self.clone().into_rel_node().children[1].clone()).unwrap()
    }

    pub fn cond(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().children[2].clone()).unwrap()
    }

    pub fn join_type(&self) -> JoinType {
        if let OptRelNodeTyp::Join(jty) = self.0 .0.typ {
            jty
        } else {
            unreachable!()
        }
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalNestedLoopJoin(pub LogicalJoin);

impl OptRelNode for PhysicalNestedLoopJoin {
    fn into_rel_node(self) -> OptRelNodeRef {
        let jtp = self.0.join_type();
        replace_typ(
            self.0.into_rel_node(),
            OptRelNodeTyp::PhysicalNestedLoopJoin(jtp),
        )
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if let OptRelNodeTyp::PhysicalNestedLoopJoin(x) = rel_node.typ {
            LogicalJoin::from_rel_node(replace_typ(rel_node, OptRelNodeTyp::Join(x))).map(Self)
        } else {
            None
        }
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "PhysicalNestedLoopJoin",
            vec![
                ("typ", self.join_type().to_string().into()),
                ("cond", self.cond().explain()),
            ],
            vec![self.left_child().explain(), self.right_child().explain()],
        )
    }
}

impl PhysicalNestedLoopJoin {
    pub fn new(logical_node: LogicalJoin) -> PhysicalNestedLoopJoin {
        PhysicalNestedLoopJoin(logical_node)
    }

    pub fn left_child(&self) -> PlanNode {
        self.0.left_child()
    }

    pub fn right_child(&self) -> PlanNode {
        self.0.right_child()
    }

    pub fn cond(&self) -> Expr {
        self.0.cond()
    }

    pub fn join_type(&self) -> JoinType {
        self.0.join_type()
    }
}
