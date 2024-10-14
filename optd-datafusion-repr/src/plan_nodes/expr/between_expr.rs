use optd_core::rel_node::{RelNode, RelNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct BetweenExpr(pub Expr);

impl BetweenExpr {
    pub fn new(expr: Expr, lower: Expr, upper: Expr) -> Self {
        BetweenExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Between,
                children: vec![
                    expr.into_rel_node(),
                    lower.into_rel_node(),
                    upper.into_rel_node(),
                ],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn lower(&self) -> Expr {
        Expr(self.0.child(1))
    }

    pub fn upper(&self) -> Expr {
        Expr(self.0.child(2))
    }
}

impl OptRelNode for BetweenExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::Between) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "Between",
            vec![
                ("expr", self.child().explain(meta_map)),
                ("lower", self.lower().explain(meta_map)),
                ("upper", self.upper().explain(meta_map)),
            ],
            vec![],
        )
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalBetweenExpr(pub Expr);

impl PhysicalBetweenExpr {
    pub fn new(expr: Expr, lower: Expr, upper: Expr) -> Self {
        PhysicalBetweenExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::PhysicalBetween,
                children: vec![
                    expr.into_rel_node(),
                    lower.into_rel_node(),
                    upper.into_rel_node(),
                ],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn lower(&self) -> Expr {
        Expr(self.0.child(1))
    }

    pub fn upper(&self) -> Expr {
        Expr(self.0.child(2))
    }
}

impl OptRelNode for PhysicalBetweenExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::PhysicalBetween) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "PhysicalBetween",
            vec![
                ("expr", self.child().explain(meta_map)),
                ("lower", self.lower().explain(meta_map)),
                ("upper", self.upper().explain(meta_map)),
            ],
            vec![],
        )
    }
}
