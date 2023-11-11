//! Typed interface of plan nodes.

mod apply;
mod expr;
mod filter;
mod join;
pub(super) mod macros;
mod scan;

use std::sync::Arc;

use optd_core::{
    cascades::GroupId,
    rel_node::{RelNode, RelNodeRef, RelNodeTyp},
};

pub use apply::{ApplyType, LogicalApply};
pub use expr::{BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, FuncExpr, UnOpExpr, UnOpType};
pub use filter::{LogicalFilter, PhysicalFilter};
pub use join::{JoinType, LogicalJoin, PhysicalNestedLoopJoin};
use pretty_xmlish::{Pretty, PrettyConfig};
pub use scan::{LogicalScan, PhysicalScan};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptRelNodeTyp {
    Placeholder(GroupId),
    List,
    // Plan nodes
    // Developers: update `is_plan_node` function after adding new elements
    Projection,
    Filter,
    Scan,
    Join(JoinType),
    Apply(ApplyType),
    // Physical plan nodes
    PhysicalProjection,
    PhysicalFilter,
    PhysicalScan,
    PhysicalNestedLoopJoin(JoinType),
    // Expressions
    Constant,
    ColumnRef,
    UnOp(UnOpType),
    BinOp(BinOpType),
    Func(usize),
}

impl OptRelNodeTyp {
    pub fn is_plan_node(&self) -> bool {
        matches!(
            self,
            Self::Projection
                | Self::Filter
                | Self::Scan
                | Self::Join(_)
                | Self::Apply(_)
                | Self::PhysicalProjection
                | Self::PhysicalFilter
                | Self::PhysicalNestedLoopJoin(_)
                | Self::PhysicalScan
        )
    }

    pub fn is_expression(&self) -> bool {
        matches!(
            self,
            Self::Constant | Self::ColumnRef | Self::UnOp(_) | Self::BinOp(_) | Self::Func(_)
        )
    }
}

impl std::fmt::Display for OptRelNodeTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl RelNodeTyp for OptRelNodeTyp {
    fn is_logical(&self) -> bool {
        matches!(
            self,
            Self::Projection | Self::Filter | Self::Scan | Self::Join(_) | Self::Apply(_)
        )
    }

    fn group_typ(group_id: GroupId) -> Self {
        Self::Placeholder(group_id)
    }

    fn list_typ() -> Self {
        Self::List
    }

    fn extract_group(&self) -> Option<GroupId> {
        if let Self::Placeholder(group_id) = self {
            Some(*group_id)
        } else {
            None
        }
    }
}

pub type OptRelNodeRef = RelNodeRef<OptRelNodeTyp>;

pub trait OptRelNode: 'static + Clone {
    fn into_rel_node(self) -> OptRelNodeRef;

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self>
    where
        Self: Sized;

    fn dispatch_explain(&self) -> Pretty<'static>;

    fn explain(&self) -> Pretty<'static> {
        explain(self.clone().into_rel_node())
    }

    fn explain_to_string(&self) -> String {
        let mut config = PrettyConfig {
            need_boundaries: false,
            reduced_spaces: false,
            ..Default::default()
        };
        let mut out = String::new();
        config.unicode(&mut out, &self.explain());
        out
    }
}

#[derive(Clone, Debug)]
pub struct PlanNode(OptRelNodeRef);

impl OptRelNode for PlanNode {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !rel_node.typ.is_plan_node() {
            return None;
        }
        Some(Self(rel_node))
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "<PlanNode>",
            vec![(
                "node_type",
                self.clone().into_rel_node().typ.to_string().into(),
            )],
            self.0
                .children
                .iter()
                .map(|child| explain(child.clone()))
                .collect(),
        )
    }
}

#[derive(Clone, Debug)]
pub struct Expr(OptRelNodeRef);

impl OptRelNode for Expr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !rel_node.typ.is_expression() {
            return None;
        }
        Some(Self(rel_node))
    }
    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "<Expr>",
            vec![(
                "node_type",
                self.clone().into_rel_node().typ.to_string().into(),
            )],
            self.0
                .children
                .iter()
                .map(|child| explain(child.clone()))
                .collect(),
        )
    }
}

pub fn explain(rel_node: OptRelNodeRef) -> Pretty<'static> {
    match rel_node.typ {
        OptRelNodeTyp::ColumnRef => ColumnRefExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Constant => ConstantExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::UnOp(_) => UnOpExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::BinOp(_) => BinOpExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Func(_) => FuncExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Join(_) => LogicalJoin::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Scan => LogicalScan::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Filter => LogicalFilter::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Apply(_) => LogicalApply::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalFilter => PhysicalFilter::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalScan => PhysicalScan::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalNestedLoopJoin(_) => PhysicalNestedLoopJoin::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        _ => unimplemented!(),
    }
}

fn replace_typ(node: OptRelNodeRef, target_type: OptRelNodeTyp) -> OptRelNodeRef {
    Arc::new(RelNode {
        typ: target_type,
        children: node.children.clone(),
        data: node.data.clone(),
    })
}
