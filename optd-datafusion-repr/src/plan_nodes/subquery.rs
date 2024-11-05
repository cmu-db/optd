use optd_core::rel_node::{MaybeRelNode, RelNode, RelNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use super::macros::define_plan_node;
use super::{Expr, ExprList, JoinType, OptRelNode, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct RawDependentJoin(pub PlanNode);

define_plan_node!(
    RawDependentJoin : PlanNode,
    RawDepJoin, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, cond: Expr },
        { 3, extern_cols: ExprList }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct DependentJoin(pub PlanNode);

define_plan_node!(
    DependentJoin : PlanNode,
    DepJoin, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, cond: Expr },
        { 3, extern_cols: ExprList }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct ExternColumnRefExpr(pub Expr);

impl ExternColumnRefExpr {
    /// Creates a new `DepExternColumnRef` expression.
    pub fn new(column_idx: usize) -> ExternColumnRefExpr {
        // this conversion is always safe since usize is at most u64
        let u64_column_idx = column_idx as u64;
        ExternColumnRefExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::ExternColumnRef,
                children: vec![],
                data: Some(Value::UInt64(u64_column_idx)),
                predicates: Vec::new(), /* TODO: refactor */
            }
            .into(),
        ))
    }

    fn get_data_usize(&self) -> usize {
        self.unwrap_rel_node().data.as_ref().unwrap().as_u64() as usize
    }

    /// Gets the column index.
    pub fn index(&self) -> usize {
        self.get_data_usize()
    }
}

impl OptRelNode for ExternColumnRefExpr {
    fn interpret(rel_node: impl Into<MaybeRelNode<OptRelNodeTyp>>) -> Self {
        Self(Expr::interpret(rel_node))
    }

    fn strip(self) -> MaybeRelNode<OptRelNodeTyp> {
        self.0.strip()
    }

    fn is_typ(typ: OptRelNodeTyp) -> bool {
        if let OptRelNodeTyp::ExternColumnRef = typ {
            true
        } else {
            false
        }
    }

    fn dispatch_explain(&self, _meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&format!("Extern(#{})", self.index()))
    }
}
