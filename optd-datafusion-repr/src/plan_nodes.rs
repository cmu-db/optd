// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

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
pub use empty_relation::{
    decode_empty_relation_schema, LogicalEmptyRelation, PhysicalEmptyRelation,
};
pub use filter::{LogicalFilter, PhysicalFilter};
pub use join::{JoinType, LogicalJoin, PhysicalHashJoin, PhysicalNestedLoopJoin};
pub use limit::{LogicalLimit, PhysicalLimit};
use optd_core::nodes::{
    ArcPlanNode, ArcPredNode, NodeType, PlanNode, PlanNodeMeta, PlanNodeMetaMap, PredNode,
    VariantTag,
};
pub use predicates::{
    BetweenPred, BinOpPred, BinOpType, CastPred, ColumnRefPred, ConstantPred, ConstantType,
    DataTypePred, ExternColumnRefPred, FuncPred, FuncType, InListPred, LikePred, ListPred,
    LogOpPred, LogOpType, PredExt, SortOrderPred, SortOrderType, UnOpPred, UnOpType,
};
use pretty_xmlish::{Pretty, PrettyConfig};
pub use projection::{LogicalProjection, PhysicalProjection};
pub use scan::{LogicalScan, PhysicalScan};
use serde::{Deserialize, Serialize};
pub use sort::{LogicalSort, PhysicalSort};
pub use subquery::{DependentJoin, RawDependentJoin}; // Add missing import

use crate::explain::{explain_plan_node, explain_pred_node};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, strum::FromRepr)]
#[repr(u8)]
pub enum DfPredType {
    List,
    Constant(ConstantType),
    ColumnRef,
    ExternColumnRef,
    UnOp(UnOpType),
    BinOp(BinOpType),
    LogOp(LogOpType),
    Func,
    SortOrder(SortOrderType),
    Between,
    Cast,
    Like,
    DataType,
    InList,
}

impl DfPredType {
    /// See https://doc.rust-lang.org/std/mem/fn.discriminant.html.
    fn discriminant(&self) -> u8 {
        // SAFETY: Because `Self` is marked `repr(u8)`, its layout is a `repr(C)` `union`
        // between `repr(C)` structs, each of which has the `u8` discriminant as its first
        // field, so we can read the discriminant without offsetting the pointer.
        unsafe { *<*const _>::from(self).cast::<u8>() }
    }
}

impl TryFrom<VariantTag> for DfPredType {
    type Error = u16;

    fn try_from(value: VariantTag) -> Result<Self, Self::Error> {
        let VariantTag(v) = value;
        let [discriminant, rest] = v.to_be_bytes();
        let typ = {
            let typ = Self::from_repr(discriminant).ok_or_else(|| v)?;
            match typ {
                DfPredType::Constant(_) => {
                    DfPredType::Constant(ConstantType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredType::UnOp(_) => {
                    DfPredType::UnOp(UnOpType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredType::BinOp(_) => {
                    DfPredType::BinOp(BinOpType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredType::LogOp(_) => {
                    DfPredType::LogOp(LogOpType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredType::SortOrder(_) => {
                    DfPredType::SortOrder(SortOrderType::from_repr(rest).ok_or_else(|| v)?)
                }
                _ => typ,
            }
        };

        Ok(typ)
    }
}

impl From<DfPredType> for VariantTag {
    fn from(value: DfPredType) -> Self {
        let discriminant = (value.discriminant() as u16) << 8;
        let tag = match value {
            DfPredType::Constant(constant_type) => discriminant | constant_type as u16,
            DfPredType::UnOp(un_op_type) => discriminant | un_op_type as u16,
            DfPredType::BinOp(bin_op_type) => discriminant | bin_op_type as u16,
            DfPredType::LogOp(log_op_type) => discriminant | log_op_type as u16,
            DfPredType::SortOrder(sort_order_type) => discriminant | sort_order_type as u16,
            _ => discriminant,
        };
        VariantTag(tag)
    }
}

impl std::fmt::Display for DfPredType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// DfNodeType FAQ:
///   - The define_plan_node!() macro defines what the children of each join node are
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::FromRepr)]
#[repr(u8)]
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

impl DfNodeType {
    /// See https://doc.rust-lang.org/std/mem/fn.discriminant.html.
    fn discriminant(&self) -> u8 {
        // SAFETY: Because `Self` is marked `repr(u8)`, its layout is a `repr(C)` `union`
        // between `repr(C)` structs, each of which has the `u8` discriminant as its first
        // field, so we can read the discriminant without offsetting the pointer.
        unsafe { *<*const _>::from(self).cast::<u8>() }
    }
}

impl std::fmt::Display for DfNodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl TryFrom<VariantTag> for DfNodeType {
    type Error = u16;

    fn try_from(value: VariantTag) -> Result<Self, Self::Error> {
        let VariantTag(v) = value;
        let [discriminant, rest] = v.to_be_bytes();
        let typ = {
            let typ = Self::from_repr(discriminant).ok_or_else(|| v)?;
            match typ {
                DfNodeType::Join(_) => {
                    DfNodeType::Join(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeType::RawDepJoin(_) => {
                    DfNodeType::RawDepJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeType::DepJoin(_) => {
                    DfNodeType::DepJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeType::PhysicalHashJoin(_) => {
                    DfNodeType::PhysicalHashJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeType::PhysicalNestedLoopJoin(_) => {
                    DfNodeType::PhysicalNestedLoopJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                _ => typ,
            }
        };
        Ok(typ)
    }
}

impl From<DfNodeType> for VariantTag {
    fn from(value: DfNodeType) -> Self {
        let discriminant = value.discriminant();
        let rest = match value {
            DfNodeType::Join(join_type) => join_type as u8,
            DfNodeType::RawDepJoin(join_type) => join_type as u8,
            DfNodeType::DepJoin(join_type) => join_type as u8,
            DfNodeType::PhysicalHashJoin(join_type) => join_type as u8,
            DfNodeType::PhysicalNestedLoopJoin(join_type) => join_type as u8,
            _ => 0,
        };
        VariantTag(u16::from_be_bytes([discriminant, rest]))
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
