//! Typed interface of plan nodes.

mod agg;
mod empty_relation;
mod expr;
mod filter;
mod join;
mod limit;
pub(super) mod macros;
mod projection;
mod scan;
mod sort;
mod subquery;

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::DataType;
use itertools::Itertools;
use optd_core::{
    cascades::GroupId,
    rel_node::{MaybeRelNode, RelNode, RelNodeMeta, RelNodeMetaMap, RelNodeRef, RelNodeTyp},
};

pub use agg::{LogicalAgg, PhysicalAgg};
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
pub use subquery::{DependentJoin, ExternColumnRefExpr, RawDependentJoin}; // Add missing import

/// OptRelNodeTyp FAQ:
///   - The define_plan_node!() macro defines what the children of each join node are
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OptRelNodeTyp {
    List,
    // Plan nodes
    // Developers: update `is_plan_node` function after adding new elements
    Projection,
    Filter,
    Scan,
    Join(JoinType),
    RawDepJoin(JoinType),
    DepJoin(JoinType),
    Sort,
    Agg,
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
    ExternColumnRef,
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
                | Self::RawDepJoin(_)
                | Self::DepJoin(_)
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
                | Self::ExternColumnRef
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
    type PredType = usize; /* TODO: refactor */

    fn is_logical(&self) -> bool {
        matches!(
            self,
            Self::Projection
                | Self::Filter
                | Self::Scan
                | Self::Join(_)
                | Self::Sort
                | Self::Agg
                | Self::EmptyRelation
                | Self::Limit
        )
    }

    fn list_typ() -> Self {
        Self::List
    }
}

pub trait OptRelNode: 'static + Clone {
    // Strip the datafusion-repr object into a optd-core object.
    fn strip(self) -> MaybeRelNode<OptRelNodeTyp>;

    /// Interpret an optd-core object as a datafusion-repr object.
    fn interpret(rel_node: impl Into<MaybeRelNode<OptRelNodeTyp>>) -> Self
    where
        Self: Sized;

    /// Interpret an optd-core object as a datafusion-repr object with the type checked.
    fn is_typ(typ: OptRelNodeTyp) -> bool;

    /// Interpret an optd-core object as a datafusion-repr object with the type checked.
    fn ensures_interpret(rel_node: impl Into<MaybeRelNode<OptRelNodeTyp>>) -> Self
    where
        Self: Sized,
    {
        let rel_node = rel_node.into();
        let typ = rel_node.unwrap_rel_node().typ;
        if !Self::is_typ(typ.clone()) {
            panic!("unexpected type: {}", typ);
        }
        Self::interpret(rel_node)
    }

    /// Interpret an optd-core object as a datafusion-repr object with the type checked.
    fn try_interpret(rel_node: impl Into<MaybeRelNode<OptRelNodeTyp>>) -> Option<Self>
    where
        Self: Sized,
    {
        let rel_node = rel_node.into();
        let typ = rel_node.unwrap_rel_node().typ;
        if !Self::is_typ(typ.clone()) {
            return None;
        }
        Some(Self::interpret(rel_node))
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static>;

    fn explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        match self.clone().strip() {
            MaybeRelNode::RelNode(node) => explain(node, meta_map),
            MaybeRelNode::Group(_) => unimplemented!("cannot explain a group"),
        }
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

    /// Unwrap this object as a rel node.
    fn unwrap_rel_node(self) -> RelNodeRef<OptRelNodeTyp> {
        self.strip().unwrap_rel_node()
    }

    fn into_plan_node(self) -> PlanNode
    where
        Self: Sized,
    {
        PlanNode::ensures_interpret(self.strip())
    }

    fn into_expr(self) -> Expr
    where
        Self: Sized,
    {
        Expr::ensures_interpret(self.strip())
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
pub struct PlanNode(pub(crate) MaybeRelNode<OptRelNodeTyp>);

impl PlanNode {
    pub fn typ(&self) -> OptRelNodeTyp {
        self.unwrap_rel_node().typ
    }

    // TODO: remove this function and use `interpret` instead
    pub fn from_group(rel_node: MaybeRelNode<OptRelNodeTyp>) -> Self {
        Self(rel_node)
    }

    pub fn get_meta<'a>(&self, meta_map: &'a RelNodeMetaMap) -> &'a RelNodeMeta {
        meta_map
            .get(&(self.unwrap_rel_node().as_ref() as *const _ as usize))
            .unwrap()
    }
}

impl OptRelNode for PlanNode {
    fn strip(self) -> MaybeRelNode<OptRelNodeTyp> {
        self.0
    }

    fn interpret(rel_node: impl Into<MaybeRelNode<OptRelNodeTyp>>) -> Self {
        Self(rel_node.into())
    }

    fn is_typ(typ: OptRelNodeTyp) -> bool {
        typ.is_plan_node()
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "<PlanNode>",
            vec![("node_type", self.unwrap_rel_node().typ.to_string().into())],
            self.unwrap_rel_node()
                .children
                .iter()
                .map(|child| explain(child.clone().unwrap_rel_node(), meta_map))
                .collect(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Expr(MaybeRelNode<OptRelNodeTyp>);

impl Expr {
    pub fn typ(&self) -> OptRelNodeTyp {
        self.unwrap_rel_node().typ.clone()
    }

    pub fn child(&self, idx: usize) -> MaybeRelNode<OptRelNodeTyp> {
        self.unwrap_rel_node().child(idx)
    }

    /// Recursively rewrite all column references in the expression.using a provided
    /// function that replaces a column index.
    /// The provided function will, given a ColumnRefExpr's index,
    /// return either Some(usize) or None.
    /// - If it is Some, the column index can be rewritten with the value.
    /// - If any of the columns is None, we will return None all the way up
    /// the call stack, and no expression will be returned.
    pub fn rewrite_column_refs(
        &self,
        rewrite_fn: &mut impl FnMut(usize) -> Option<usize>,
    ) -> Option<Self> {
        assert!(self.typ().is_expression());
        if let OptRelNodeTyp::ColumnRef = self.typ() {
            let col_ref = ColumnRefExpr::ensures_interpret(self.0.clone());
            let rewritten = rewrite_fn(col_ref.index());
            return if let Some(rewritten_idx) = rewritten {
                let new_col_ref = ColumnRefExpr::new(rewritten_idx);
                Some(Expr::ensures_interpret(new_col_ref.strip()))
            } else {
                None
            };
        }
        let children = self.unwrap_rel_node().children.clone();
        let children = children
            .into_iter()
            .map(|child| {
                let child = child.unwrap_rel_node();
                if child.typ == OptRelNodeTyp::List {
                    return Some(Expr::ensures_interpret(
                        ExprList::new(
                            ExprList::ensures_interpret(child.clone())
                                .to_vec()
                                .into_iter()
                                .map(|x| x.rewrite_column_refs(rewrite_fn).unwrap())
                                .collect(),
                        )
                        .strip(),
                    ));
                }
                Expr::ensures_interpret(child.clone())
                    .rewrite_column_refs(rewrite_fn)
                    .map(|x| Expr::ensures_interpret(x.strip()))
            })
            .collect::<Option<Vec<_>>>()?;
        let rel_node = self.unwrap_rel_node();
        Some(Expr::ensures_interpret(RelNode {
            typ: rel_node.typ.clone(),
            children: children.into_iter().map(|e| e.strip()).collect_vec(),
            data: rel_node.data.clone(),
            predicates: Vec::new(), /* TODO: refactor */
        }))
    }

    /// Recursively retrieves all column references in the expression
    /// using a provided function.
    /// The provided function will, given a ColumnRefExpr's index,
    /// return a Vec<Expr> including the expr in col ref.
    pub fn get_column_refs(&self) -> Vec<Expr> {
        assert!(self.typ().is_expression());
        if let OptRelNodeTyp::ColumnRef = self.typ() {
            return vec![self.clone()];
        }

        let rel_node = self.unwrap_rel_node();
        let children = rel_node.children.into_iter().map(|child| {
            let child = child.unwrap_rel_node();
            if child.typ == OptRelNodeTyp::List {
                // TODO: What should we do with List?
                return vec![];
            }
            Expr::ensures_interpret(child).get_column_refs()
        });
        children.collect_vec().concat()
    }
}

impl OptRelNode for Expr {
    fn strip(self) -> MaybeRelNode<OptRelNodeTyp> {
        self.0
    }

    fn interpret(rel_node: impl Into<MaybeRelNode<OptRelNodeTyp>>) -> Self {
        Self(rel_node.into())
    }

    fn is_typ(typ: OptRelNodeTyp) -> bool {
        typ.is_expression()
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "<Expr>",
            vec![("node_type", self.unwrap_rel_node().typ.to_string().into())],
            self.unwrap_rel_node()
                .children
                .iter()
                .map(|child| explain(child.unwrap_rel_node(), meta_map))
                .collect(),
        )
    }
}

pub fn explain(
    rel_node: RelNodeRef<OptRelNodeTyp>,
    meta_map: Option<&RelNodeMetaMap>,
) -> Pretty<'static> {
    match rel_node.typ {
        OptRelNodeTyp::ColumnRef => {
            ColumnRefExpr::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::ExternColumnRef => {
            ExternColumnRefExpr::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::Constant(_) => {
            ConstantExpr::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::UnOp(_) => UnOpExpr::ensures_interpret(rel_node).dispatch_explain(meta_map),
        OptRelNodeTyp::BinOp(_) => {
            BinOpExpr::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::Func(_) => FuncExpr::ensures_interpret(rel_node).dispatch_explain(meta_map),
        OptRelNodeTyp::Join(_) => {
            LogicalJoin::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::RawDepJoin(_) => {
            RawDependentJoin::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::DepJoin(_) => {
            DependentJoin::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::Scan => LogicalScan::ensures_interpret(rel_node).dispatch_explain(meta_map),
        OptRelNodeTyp::Filter => {
            LogicalFilter::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::EmptyRelation => {
            LogicalEmptyRelation::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::Limit => {
            LogicalLimit::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::PhysicalFilter => {
            PhysicalFilter::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::PhysicalScan => {
            PhysicalScan::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::PhysicalNestedLoopJoin(_) => {
            PhysicalNestedLoopJoin::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::List => {
            ExprList::ensures_interpret(rel_node) // ExprList is the only place that we will have list in the datafusion repr
                .dispatch_explain(meta_map)
        }
        OptRelNodeTyp::Agg => LogicalAgg::ensures_interpret(rel_node).dispatch_explain(meta_map),
        OptRelNodeTyp::Sort => LogicalSort::ensures_interpret(rel_node).dispatch_explain(meta_map),
        OptRelNodeTyp::Projection => {
            LogicalProjection::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::PhysicalProjection => {
            PhysicalProjection::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::PhysicalAgg => {
            PhysicalAgg::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::PhysicalSort => {
            PhysicalSort::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::PhysicalHashJoin(_) => {
            PhysicalHashJoin::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::SortOrder(_) => {
            SortOrderExpr::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::LogOp(_) => {
            LogOpExpr::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::PhysicalEmptyRelation => {
            PhysicalEmptyRelation::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::PhysicalLimit => {
            PhysicalLimit::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::Between => {
            BetweenExpr::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::Cast => CastExpr::ensures_interpret(rel_node).dispatch_explain(meta_map),
        OptRelNodeTyp::Like => LikeExpr::ensures_interpret(rel_node).dispatch_explain(meta_map),
        OptRelNodeTyp::DataType(_) => {
            DataTypeExpr::ensures_interpret(rel_node).dispatch_explain(meta_map)
        }
        OptRelNodeTyp::InList => InListExpr::ensures_interpret(rel_node).dispatch_explain(meta_map),
    }
}

fn replace_typ(
    node: MaybeRelNode<OptRelNodeTyp>,
    target_type: OptRelNodeTyp,
) -> MaybeRelNode<OptRelNodeTyp> {
    let node = node.unwrap_rel_node();
    Arc::new(RelNode {
        typ: target_type,
        children: node.children.clone(),
        data: node.data.clone(),
        predicates: Vec::new(), /* TODO: refactor */
    })
    .into()
}
