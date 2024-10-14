use optd_core::rel_node::{RelNode, RelNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

use super::{ExprList, PhysicalExprList};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FuncType {
    Scalar(datafusion_expr::BuiltinScalarFunction),
    Agg(datafusion_expr::AggregateFunction),
    Case,
}

impl std::fmt::Display for FuncType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FuncType {
    pub fn new_scalar(func_id: datafusion_expr::BuiltinScalarFunction) -> Self {
        FuncType::Scalar(func_id)
    }

    pub fn new_agg(func_id: datafusion_expr::AggregateFunction) -> Self {
        FuncType::Agg(func_id)
    }
}

#[derive(Clone, Debug)]
pub struct FuncExpr(Expr);

impl FuncExpr {
    pub fn new(func_id: FuncType, argv: ExprList) -> Self {
        FuncExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Func(func_id),
                children: vec![argv.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    /// Gets the i-th argument of the function.
    pub fn arg_at(&self, i: usize) -> Expr {
        self.children().child(i)
    }

    /// Get all children.
    pub fn children(&self) -> ExprList {
        ExprList::from_rel_node(self.0.child(0)).unwrap()
    }

    /// Gets the function id.
    pub fn func(&self) -> FuncType {
        if let OptRelNodeTyp::Func(func_id) = &self.clone().into_rel_node().typ {
            func_id.clone()
        } else {
            panic!("not a function")
        }
    }
}

impl OptRelNode for FuncExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::Func(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.func().to_string(),
            vec![],
            vec![self.children().explain(meta_map)],
        )
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalFuncExpr(Expr);

impl PhysicalFuncExpr {
    pub fn new(func_id: FuncType, argv: PhysicalExprList) -> Self {
        PhysicalFuncExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::PhysicalFunc(func_id),
                children: vec![argv.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    /// Gets the i-th argument of the function.
    pub fn arg_at(&self, i: usize) -> Expr {
        self.children().child(i)
    }

    /// Get all children.
    pub fn children(&self) -> PhysicalExprList {
        PhysicalExprList::from_rel_node(self.0.child(0)).unwrap()
    }

    /// Gets the function id.
    pub fn func(&self) -> FuncType {
        if let OptRelNodeTyp::PhysicalFunc(func_id) = &self.clone().into_rel_node().typ {
            func_id.clone()
        } else {
            panic!("not a function")
        }
    }
}

impl OptRelNode for PhysicalFuncExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::PhysicalFunc(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.func().to_string(),
            vec![],
            vec![self.children().explain(meta_map)],
        )
    }
}
