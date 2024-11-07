//! Typed interface of plan nodes.

mod agg;
mod empty_relation;
mod filter;
mod join;
mod limit;
pub(super) mod macros;
mod predicates;
mod projection;
mod scan;
mod sort;
mod subquery;

use std::fmt::Debug;

pub use agg::{LogicalAgg, PhysicalAgg};
use arrow_schema::DataType;
pub use empty_relation::{
    decode_empty_relation_schema, LogicalEmptyRelation, PhysicalEmptyRelation,
};
pub use filter::{LogicalFilter, PhysicalFilter};
pub use join::{JoinType, LogicalJoin, PhysicalHashJoin, PhysicalNestedLoopJoin};
pub use limit::{LogicalLimit, PhysicalLimit};
use optd_core::nodes::{
    ArcPlanNode, ArcPredNode, NodeType, PlanNode, PlanNodeMeta, PlanNodeMetaMap, PredNode,
};
pub use predicates::{
    BetweenPred, BinOpPred, BinOpType, CastPred, ColumnRefPred, ConstantPred, ConstantType,
    DataTypePred, ExternColumnRefPred, FuncPred, FuncType, InListPred, LikePred, ListPred,
    LogOpPred, LogOpType, PredExt, SortOrderPred, SortOrderType, UnOpPred, UnOpType,
};
use pretty_xmlish::{Pretty, PrettyConfig};
pub use projection::{LogicalProjection, PhysicalProjection};
pub use scan::{LogicalScan, PhysicalScan};
pub use sort::{LogicalSort, PhysicalSort};
pub use subquery::{DependentJoin, RawDependentJoin}; // Add missing import

use crate::explain::{explain_plan_node, explain_pred_node};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DfPredType {
    List,
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

impl std::fmt::Display for DfPredType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// DfNodeType FAQ:
///   - The define_plan_node!() macro defines what the children of each join node are
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DfNodeType {
    // Developers: update `is_logical` function after adding new plan nodes
    // Plan nodes
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
}

impl std::fmt::Display for DfNodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl NodeType for DfNodeType {
    type PredType = DfPredType;
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
}

pub type DfPlanNode = PlanNode<DfNodeType>;
pub type ArcDfPlanNode = ArcPlanNode<DfNodeType>;

pub trait DfReprPlanNode: 'static + Clone {
    fn into_plan_node(self) -> ArcDfPlanNode;

    fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self>;

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static>;

    fn explain_to_string(&self, meta_map: Option<&PlanNodeMetaMap>) -> String {
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

    fn get_meta<'a>(&self, meta_map: &'a PlanNodeMetaMap) -> &'a PlanNodeMeta {
        meta_map
            .get(&(self.clone().into_plan_node().as_ref() as *const _ as usize))
            .unwrap()
    }
}

impl DfReprPlanNode for ArcDfPlanNode {
    fn into_plan_node(self) -> ArcDfPlanNode {
        self
    }

    fn from_plan_node(pred_node: ArcDfPlanNode) -> Option<Self> {
        Some(pred_node)
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        explain_plan_node(self.clone(), meta_map)
    }
}

pub type DfPredNode = PredNode<DfNodeType>;
pub type ArcDfPredNode = ArcPredNode<DfNodeType>;

pub trait DfReprPredNode: 'static + Clone {
    fn into_pred_node(self) -> ArcDfPredNode;

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self>;

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static>;

    fn explain_to_string(&self, meta_map: Option<&PlanNodeMetaMap>) -> String {
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
}

impl DfReprPredNode for ArcDfPredNode {
    fn into_pred_node(self) -> ArcDfPredNode {
        self
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        Some(pred_node)
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        explain_pred_node(self.clone(), meta_map)
    }
}

pub fn dispatch_plan_explain_to_string(
    plan_node: ArcDfPlanNode,
    meta_map: Option<&PlanNodeMetaMap>,
) -> String {
    let mut config = PrettyConfig {
        need_boundaries: false,
        reduced_spaces: false,
        width: 300,
        ..Default::default()
    };
    let mut out = String::new();
    config.unicode(&mut out, &plan_node.explain(meta_map));
    out
}
