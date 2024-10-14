use std::fmt::Display;

use optd_core::rel_node::{RelNode, RelNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

use super::{ExprList, PhysicalExprList};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum LogOpType {
    And,
    Or,
}

impl Display for LogOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogOpExpr(pub Expr);

impl LogOpExpr {
    pub fn new(op_type: LogOpType, expr_list: ExprList) -> Self {
        LogOpExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::LogOp(op_type),
                children: expr_list
                    .to_vec()
                    .into_iter()
                    .map(|x| x.into_rel_node())
                    .collect(),
                data: None,
            }
            .into(),
        ))
    }

    /// flatten_nested_logical is a helper function to flatten nested logical operators with same op type
    /// eg. (a AND (b AND c)) => ExprList([a, b, c])
    ///    (a OR (b OR c)) => ExprList([a, b, c])
    /// It assume the children of the input expr_list are already flattened
    ///  and can only be used in bottom up manner
    pub fn new_flattened_nested_logical(op: LogOpType, expr_list: ExprList) -> Self {
        // Since we assume that we are building the children bottom up,
        // there is no need to call flatten_nested_logical recursively
        let mut new_expr_list = Vec::new();
        for child in expr_list.to_vec() {
            if let OptRelNodeTyp::LogOp(child_op) = child.typ() {
                if child_op == op {
                    let child_log_op_expr =
                        LogOpExpr::from_rel_node(child.into_rel_node()).unwrap();
                    new_expr_list.extend(child_log_op_expr.children().to_vec());
                    continue;
                }
            }
            new_expr_list.push(child.clone());
        }
        LogOpExpr::new(op, ExprList::new(new_expr_list))
    }

    pub fn children(&self) -> Vec<Expr> {
        self.0
             .0
            .children
            .iter()
            .map(|x| Expr::from_rel_node(x.clone()).unwrap())
            .collect()
    }

    pub fn child(&self, idx: usize) -> Expr {
        Expr::from_rel_node(self.0.child(idx)).unwrap()
    }

    pub fn op_type(&self) -> LogOpType {
        if let OptRelNodeTyp::LogOp(op_type) = self.clone().into_rel_node().typ {
            op_type
        } else {
            panic!("not a log op")
        }
    }
}

impl OptRelNode for LogOpExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::LogOp(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            self.children()
                .iter()
                .map(|x| x.explain(meta_map))
                .collect(),
        )
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalLogOpExpr(pub Expr);

impl PhysicalLogOpExpr {
    pub fn new(op_type: LogOpType, expr_list: PhysicalExprList) -> Self {
        PhysicalLogOpExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::PhysicalLogOp(op_type),
                children: expr_list
                    .to_vec()
                    .into_iter()
                    .map(|x| x.into_rel_node())
                    .collect(),
                data: None,
            }
            .into(),
        ))
    }

    /// flatten_nested_logical is a helper function to flatten nested logical operators with same op type
    /// eg. (a AND (b AND c)) => PhysicalExprList([a, b, c])
    ///    (a OR (b OR c)) => PhysicalExprList([a, b, c])
    /// It assume the children of the input expr_list are already flattened
    ///  and can only be used in bottom up manner
    pub fn new_flattened_nested_logical(op: LogOpType, expr_list: PhysicalExprList) -> Self {
        // Since we assume that we are building the children bottom up,
        // there is no need to call flatten_nested_logical recursively
        let mut new_expr_list = Vec::new();
        for child in expr_list.to_vec() {
            if let OptRelNodeTyp::PhysicalLogOp(child_op) = child.typ() {
                if child_op == op {
                    let child_log_op_expr =
                        PhysicalLogOpExpr::from_rel_node(child.into_rel_node()).unwrap();
                    new_expr_list.extend(child_log_op_expr.children().to_vec());
                    continue;
                }
            }
            new_expr_list.push(child.clone());
        }
        PhysicalLogOpExpr::new(op, PhysicalExprList::new(new_expr_list))
    }

    pub fn children(&self) -> Vec<Expr> {
        self.0
             .0
            .children
            .iter()
            .map(|x| Expr::from_rel_node(x.clone()).unwrap())
            .collect()
    }

    pub fn child(&self, idx: usize) -> Expr {
        Expr::from_rel_node(self.0.child(idx)).unwrap()
    }

    pub fn op_type(&self) -> LogOpType {
        if let OptRelNodeTyp::PhysicalLogOp(op_type) = self.clone().into_rel_node().typ {
            op_type
        } else {
            panic!("not a log op")
        }
    }
}

impl OptRelNode for PhysicalLogOpExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::PhysicalLogOp(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            self.children()
                .iter()
                .map(|x| x.explain(meta_map))
                .collect(),
        )
    }
}
