use pretty_xmlish::Pretty;

use optd_core::rel_node::RelNode;

use super::{replace_typ, Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "LogicalFilter",
            vec![("cond", self.cond().explain())],
            vec![self.child().explain()],
        )
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
        PlanNode::from_rel_node(self.clone().into_rel_node().child(0)).unwrap()
    }

    /// Gets the filter condition.
    pub fn cond(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(1)).unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalFilter(pub LogicalFilter);

impl OptRelNode for PhysicalFilter {
    fn into_rel_node(self) -> OptRelNodeRef {
        replace_typ(self.0.into_rel_node(), OptRelNodeTyp::PhysicalFilter)
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::PhysicalFilter {
            return None;
        }
        LogicalFilter::from_rel_node(replace_typ(rel_node, OptRelNodeTyp::Filter)).map(Self)
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "PhysicalFilter",
            vec![("cond", self.cond().explain())],
            vec![self.child().explain()],
        )
    }
}

impl PhysicalFilter {
    /// Creates a new `PhysicalFilter` plan node.
    pub fn new(node: LogicalFilter) -> Self {
        PhysicalFilter(node)
    }

    /// Gets the child plan node.
    pub fn child(&self) -> PlanNode {
        self.0.child()
    }

    /// Gets the filter condition.
    pub fn cond(&self) -> Expr {
        self.0.cond()
    }
}
