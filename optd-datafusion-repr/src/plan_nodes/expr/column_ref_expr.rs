use optd_core::rel_node::{RelNode, RelNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct ColumnRefExpr(pub Expr);

impl ColumnRefExpr {
    /// Creates a new `ColumnRef` expression.
    pub fn new(column_idx: usize) -> ColumnRefExpr {
        // this conversion is always safe since usize is at most u64
        let u64_column_idx = column_idx as u64;
        ColumnRefExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::ColumnRef,
                children: vec![],
                data: Some(Value::UInt64(u64_column_idx)),
            }
            .into(),
        ))
    }

    fn get_data_usize(&self) -> usize {
        self.0 .0.data.as_ref().unwrap().as_u64() as usize
    }

    /// Gets the column index.
    pub fn index(&self) -> usize {
        self.get_data_usize()
    }
}

impl OptRelNode for ColumnRefExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::ColumnRef {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&format!("#{}", self.index()))
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalColumnRefExpr(pub Expr);

impl PhysicalColumnRefExpr {
    /// Creates a new `ColumnRef` expression.
    pub fn new(column_idx: usize) -> PhysicalColumnRefExpr {
        // this conversion is always safe since usize is at most u64
        let u64_column_idx = column_idx as u64;
        PhysicalColumnRefExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::PhysicalColumnRef,
                children: vec![],
                data: Some(Value::UInt64(u64_column_idx)),
            }
            .into(),
        ))
    }

    fn get_data_usize(&self) -> usize {
        self.0 .0.data.as_ref().unwrap().as_u64() as usize
    }

    /// Gets the column index.
    pub fn index(&self) -> usize {
        self.get_data_usize()
    }
}

impl OptRelNode for PhysicalColumnRefExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::PhysicalColumnRef {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&format!("#{}", self.index()))
    }
}
