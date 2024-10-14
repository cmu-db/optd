use std::sync::Arc;

use optd_core::rel_node::{RelNode, RelNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct LikeExpr(pub Expr);

impl LikeExpr {
    pub fn new(negated: bool, case_insensitive: bool, expr: Expr, pattern: Expr) -> Self {
        // TODO: support multiple values in data.
        let negated = if negated { 1 } else { 0 };
        let case_insensitive = if case_insensitive { 1 } else { 0 };
        LikeExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Like,
                children: vec![expr.into_rel_node(), pattern.into_rel_node()],
                data: Some(Value::Serialized(Arc::new([negated, case_insensitive]))),
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn pattern(&self) -> Expr {
        Expr(self.0.child(1))
    }

    /// `true` for `NOT LIKE`.
    pub fn negated(&self) -> bool {
        match self.0 .0.data.as_ref().unwrap() {
            Value::Serialized(data) => data[0] != 0,
            _ => panic!("not a serialized value"),
        }
    }

    pub fn case_insensitive(&self) -> bool {
        match self.0 .0.data.as_ref().unwrap() {
            Value::Serialized(data) => data[1] != 0,
            _ => panic!("not a serialized value"),
        }
    }
}

impl OptRelNode for LikeExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::Like) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "Like",
            vec![
                ("expr", self.child().explain(meta_map)),
                ("pattern", self.pattern().explain(meta_map)),
                ("negated", self.negated().to_string().into()),
                (
                    "case_insensitive",
                    self.case_insensitive().to_string().into(),
                ),
            ],
            vec![],
        )
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalLikeExpr(pub Expr);

impl PhysicalLikeExpr {
    pub fn new(negated: bool, case_insensitive: bool, expr: Expr, pattern: Expr) -> Self {
        // TODO: support multiple values in data.
        let negated = if negated { 1 } else { 0 };
        let case_insensitive = if case_insensitive { 1 } else { 0 };
        PhysicalLikeExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::PhysicalLike,
                children: vec![expr.into_rel_node(), pattern.into_rel_node()],
                data: Some(Value::Serialized(Arc::new([negated, case_insensitive]))),
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn pattern(&self) -> Expr {
        Expr(self.0.child(1))
    }

    /// `true` for `NOT LIKE`.
    pub fn negated(&self) -> bool {
        match self.0 .0.data.as_ref().unwrap() {
            Value::Serialized(data) => data[0] != 0,
            _ => panic!("not a serialized value"),
        }
    }

    pub fn case_insensitive(&self) -> bool {
        match self.0 .0.data.as_ref().unwrap() {
            Value::Serialized(data) => data[1] != 0,
            _ => panic!("not a serialized value"),
        }
    }
}

impl OptRelNode for PhysicalLikeExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::PhysicalLike) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "PhysicalLike",
            vec![
                ("expr", self.child().explain(meta_map)),
                ("pattern", self.pattern().explain(meta_map)),
                ("negated", self.negated().to_string().into()),
                (
                    "case_insensitive",
                    self.case_insensitive().to_string().into(),
                ),
            ],
            vec![],
        )
    }
}
