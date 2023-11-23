//! Typed interface of plan nodes.

mod agg;
mod apply;
mod expr;
mod filter;
mod join;
pub(super) mod macros;
mod projection;
mod scan;
mod sort;

use std::sync::Arc;

use optd_core::{
    cascades::GroupId,
    rel_node::{RelNode, RelNodeRef, RelNodeTyp},
};

pub use agg::{LogicalAgg, PhysicalAgg};
pub use apply::{ApplyType, LogicalApply};
pub use expr::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ConstantType, ExprList, FuncExpr, FuncType,
    LogOpExpr, LogOpType, SortOrderExpr, SortOrderType, UnOpExpr, UnOpType,
};
pub use filter::{LogicalFilter, PhysicalFilter};
pub use join::{JoinType, LogicalJoin, PhysicalNestedLoopJoin};
use pretty_xmlish::{Pretty, PrettyConfig};
pub use projection::{LogicalProjection, PhysicalProjection};
pub use scan::{LogicalScan, PhysicalScan};
pub use sort::{LogicalSort, PhysicalSort};

use self::join::PhysicalHashJoin;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OptRelNodeTyp {
    Placeholder(GroupId),
    List,
    // Plan nodes
    // Developers: update `is_plan_node` function after adding new elements
    Projection,
    Filter,
    Scan,
    Join(JoinType),
    Sort,
    Agg,
    Apply(ApplyType),
    // Physical plan nodes
    PhysicalProjection,
    PhysicalFilter,
    PhysicalScan,
    PhysicalSort,
    PhysicalAgg,
    PhysicalHashJoin(JoinType),
    PhysicalNestedLoopJoin(JoinType),
    // Expressions
    Constant(ConstantType),
    ColumnRef,
    UnOp(UnOpType),
    BinOp(BinOpType),
    LogOp(LogOpType),
    Func(FuncType),
    SortOrder(SortOrderType),
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
                | Self::Sort
                | Self::Agg
                | Self::PhysicalProjection
                | Self::PhysicalFilter
                | Self::PhysicalNestedLoopJoin(_)
                | Self::PhysicalScan
                | Self::PhysicalSort
                | Self::PhysicalAgg
                | Self::PhysicalHashJoin(_)
        )
    }

    pub fn is_expression(&self) -> bool {
        matches!(
            self,
            Self::Constant(_)
                | Self::ColumnRef
                | Self::UnOp(_)
                | Self::BinOp(_)
                | Self::Func(_)
                | Self::SortOrder(_)
                | Self::LogOp(_)
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
            Self::Projection
                | Self::Filter
                | Self::Scan
                | Self::Join(_)
                | Self::Apply(_)
                | Self::Sort
                | Self::Agg
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

    fn into_plan_node(self) -> PlanNode {
        let rel_node = self.into_rel_node();
        let typ = rel_node.typ.clone();
        let Some(p) = PlanNode::from_rel_node(rel_node) else {
            panic!("expect plan node, found {}", typ)
        };
        p
    }

    fn into_expr(self) -> Expr {
        let node = self.into_rel_node();
        let typ = node.typ.clone();
        let Some(e) = Expr::from_rel_node(node) else {
            panic!("expect expr, found {}", typ)
        };
        e
    }
}

#[derive(Clone, Debug)]
pub struct PlanNode(OptRelNodeRef);

impl PlanNode {
    pub fn typ(&self) -> OptRelNodeTyp {
        self.0.typ.clone()
    }
}

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

impl Expr {
    pub fn typ(&self) -> OptRelNodeTyp {
        self.0.typ.clone()
    }

    pub fn child(&self, idx: usize) -> OptRelNodeRef {
        self.0.child(idx)
    }
}

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
        OptRelNodeTyp::Constant(_) => ConstantExpr::from_rel_node(rel_node)
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
        OptRelNodeTyp::Placeholder(_) => unreachable!("should not explain a placeholder"),
        OptRelNodeTyp::List => {
            ExprList::from_rel_node(rel_node) // ExprList is the only place that we will have list in the datafusion repr
                .unwrap()
                .dispatch_explain()
        }
        OptRelNodeTyp::Agg => LogicalAgg::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Sort => LogicalSort::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Projection => LogicalProjection::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalProjection => PhysicalProjection::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalAgg => PhysicalAgg::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalSort => PhysicalSort::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalHashJoin(_) => PhysicalHashJoin::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::SortOrder(_) => SortOrderExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::LogOp(_) => LogOpExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
    }
}

fn replace_typ(node: OptRelNodeRef, target_type: OptRelNodeTyp) -> OptRelNodeRef {
    Arc::new(RelNode {
        typ: target_type,
        children: node.children.clone(),
        data: node.data.clone(),
    })
}
