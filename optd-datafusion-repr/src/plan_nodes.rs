//! Typed interface of plan nodes.

mod agg;
mod apply;
mod empty_relation;
mod expr;
mod filter;
mod join;
mod limit;
pub(super) mod macros;
mod projection;
mod scan;
mod sort;

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::DataType;
use optd_core::{
    cascades::{CascadesOptimizer, GroupId},
    rel_node::{RelNode, RelNodeMeta, RelNodeMetaMap, RelNodeRef, RelNodeTyp},
};

pub use agg::{LogicalAgg, PhysicalAgg};
pub use apply::{ApplyType, LogicalApply};
pub use empty_relation::{EmptyRelationData, LogicalEmptyRelation, PhysicalEmptyRelation};
pub use expr::{
    BetweenExpr, BinOpExpr, BinOpType, CastExpr, ColumnRefExpr, ConstantExpr, ConstantType,
    DataTypeExpr, ExprList, FuncExpr, FuncType, InListExpr, LikeExpr, LogOpExpr, LogOpType,
    SortOrderExpr, SortOrderType, UnOpExpr, UnOpType,
};
pub use filter::{LogicalFilter, PhysicalFilter};
pub use join::{JoinType, LogicalJoin, PhysicalHashJoin, PhysicalNestedLoopJoin};
pub use limit::{LogicalLimit, PhysicalLimit};
use pretty_xmlish::{Pretty, PrettyConfig};
pub use projection::{LogicalProjection, PhysicalProjection};
pub use scan::{LogicalScan, PhysicalScan};
pub use sort::{LogicalSort, PhysicalSort};

use crate::properties::schema::{Schema, SchemaPropertyBuilder};

/// OptRelNodeTyp FAQ:
///   - The define_plan_node!() macro defines what the children of each join node are
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
    EmptyRelation,
    Limit,
    // Physical plan nodes
    PhysicalProjection,
    PhysicalFilter,
    PhysicalScan,
    PhysicalSort,
    PhysicalAgg,
    PhysicalHashJoin(JoinType),
    PhysicalNestedLoopJoin(JoinType),
    PhysicalEmptyRelation,
    PhysicalLimit,
    // Expressions
    Constant(ConstantType),
    ColumnRef,
    UnOp(UnOpType),
    BinOp(BinOpType),
    LogOp(LogOpType),
    Func(FuncType),
    SortOrder(SortOrderType),
    Between,
    Cast,
    Like,
    DataType(DataType),
    InList,
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
                | Self::EmptyRelation
                | Self::Limit
                | Self::PhysicalProjection
                | Self::PhysicalFilter
                | Self::PhysicalNestedLoopJoin(_)
                | Self::PhysicalScan
                | Self::PhysicalSort
                | Self::PhysicalAgg
                | Self::PhysicalHashJoin(_)
                | Self::PhysicalLimit
                | Self::PhysicalEmptyRelation
        )
    }

    pub fn is_expression(&self) -> bool {
        matches!(
            self,
            Self::Constant(_)
                | Self::ColumnRef
                | Self::UnOp(_)
                | Self::BinOp(_)
                | Self::LogOp(_)
                | Self::Func(_)
                | Self::SortOrder(_)
                | Self::Between
                | Self::Cast
                | Self::Like
                | Self::DataType(_)
                | Self::InList
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
                | Self::EmptyRelation
                | Self::Limit
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

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static>;

    fn explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        explain(self.clone().into_rel_node(), meta_map)
    }

    fn explain_to_string(&self, meta_map: Option<&RelNodeMetaMap>) -> String {
        let mut config = PrettyConfig {
            need_boundaries: false,
            reduced_spaces: false,
            width: 300,
            ..Default::default()
        };
        let mut out = String::new();
        config.unicode(&mut out, &self.explain(meta_map));
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

/// Plan nodes that are defined through `define_plan_node` macro with data
/// field should implement this trait.
///
/// We require plan nodes to explicitly implement this instead of using `Debug`,
/// because for complex data type (struct), derived debug printing
/// displays struct name which should be hidden from the user. It also wraps
/// the fields in braces, unlike the rest of the fields as children.
pub trait ExplainData<T>: OptRelNode {
    fn explain_data(data: &T) -> Vec<(&'static str, Pretty<'static>)>;
}

#[derive(Clone, Debug)]
pub struct PlanNode(pub(crate) OptRelNodeRef);

impl PlanNode {
    pub fn typ(&self) -> OptRelNodeTyp {
        self.0.typ.clone()
    }

    pub fn schema(&self, optimizer: &CascadesOptimizer<OptRelNodeTyp>) -> Schema {
        let group_id = optimizer.resolve_group_id(self.0.clone());
        optimizer.get_property_by_group::<SchemaPropertyBuilder>(group_id, 0)
    }

    pub fn from_group(rel_node: OptRelNodeRef) -> Self {
        Self(rel_node)
    }

    pub fn get_meta<'a>(&self, meta_map: &'a RelNodeMetaMap) -> &'a RelNodeMeta {
        meta_map
            .get(&(self.0.as_ref() as *const _ as usize))
            .unwrap()
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

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "<PlanNode>",
            vec![(
                "node_type",
                self.clone().into_rel_node().typ.to_string().into(),
            )],
            self.0
                .children
                .iter()
                .map(|child| explain(child.clone(), meta_map))
                .collect(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "<Expr>",
            vec![(
                "node_type",
                self.clone().into_rel_node().typ.to_string().into(),
            )],
            self.0
                .children
                .iter()
                .map(|child| explain(child.clone(), meta_map))
                .collect(),
        )
    }
}

pub fn explain(rel_node: OptRelNodeRef, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
    match rel_node.typ {
        OptRelNodeTyp::ColumnRef => ColumnRefExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Constant(_) => ConstantExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::UnOp(_) => UnOpExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::BinOp(_) => BinOpExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Func(_) => FuncExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Join(_) => LogicalJoin::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Scan => LogicalScan::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Filter => LogicalFilter::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Apply(_) => LogicalApply::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::EmptyRelation => LogicalEmptyRelation::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Limit => LogicalLimit::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::PhysicalFilter => PhysicalFilter::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::PhysicalScan => PhysicalScan::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::PhysicalNestedLoopJoin(_) => PhysicalNestedLoopJoin::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Placeholder(_) => unreachable!("should not explain a placeholder"),
        OptRelNodeTyp::List => {
            ExprList::from_rel_node(rel_node) // ExprList is the only place that we will have list in the datafusion repr
                .unwrap()
                .dispatch_explain(meta_map)
        }
        OptRelNodeTyp::Agg => LogicalAgg::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Sort => LogicalSort::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Projection => LogicalProjection::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::PhysicalProjection => PhysicalProjection::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::PhysicalAgg => PhysicalAgg::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::PhysicalSort => PhysicalSort::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::PhysicalHashJoin(_) => PhysicalHashJoin::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::SortOrder(_) => SortOrderExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::LogOp(_) => LogOpExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::PhysicalEmptyRelation => PhysicalEmptyRelation::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::PhysicalLimit => PhysicalLimit::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Between => BetweenExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Cast => CastExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::Like => LikeExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::DataType(_) => DataTypeExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
        OptRelNodeTyp::InList => InListExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(meta_map),
    }
}

fn replace_typ(node: OptRelNodeRef, target_type: OptRelNodeTyp) -> OptRelNodeRef {
    Arc::new(RelNode {
        typ: target_type,
        children: node.children.clone(),
        data: node.data.clone(),
    })
}
