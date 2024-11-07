// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use optd_core::nodes::{PlanNodeMeta, PlanNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{
    ArcDfPlanNode, ArcDfPredNode, BetweenPred, BinOpPred, CastPred, ColumnRefPred, ConstantPred,
    DataTypePred, DependentJoin, DfNodeType, DfPredType, DfReprPlanNode, DfReprPredNode,
    ExternColumnRefPred, FuncPred, InListPred, LikePred, ListPred, LogOpPred, LogicalAgg,
    LogicalEmptyRelation, LogicalFilter, LogicalJoin, LogicalLimit, LogicalProjection, LogicalScan,
    LogicalSort, PhysicalAgg, PhysicalEmptyRelation, PhysicalFilter, PhysicalHashJoin,
    PhysicalLimit, PhysicalNestedLoopJoin, PhysicalProjection, PhysicalScan, PhysicalSort,
    RawDependentJoin, SortOrderPred, UnOpPred,
};

pub trait Insertable<'a> {
    fn with_meta(self, meta: &PlanNodeMeta) -> Self;
}

impl<'a> Insertable<'a> for Vec<(&'a str, Pretty<'a>)> {
    // FIXME: this assumes we are using OptCostModel
    fn with_meta(mut self, meta: &PlanNodeMeta) -> Self {
        self.push(("cost", Pretty::display(&meta.cost_display)));
        self.push(("stat", Pretty::display(&meta.stat_display)));
        self
    }
}

pub fn explain_pred_node(
    node: ArcDfPredNode,
    meta_map: Option<&PlanNodeMetaMap>,
) -> Pretty<'static> {
    match node.typ {
        DfPredType::ColumnRef => ColumnRefPred::from_pred_node(node)
            .unwrap()
            .explain(meta_map),
        DfPredType::ExternColumnRef => ExternColumnRefPred::from_pred_node(node)
            .unwrap()
            .explain(meta_map),
        DfPredType::Constant(_) => ConstantPred::from_pred_node(node)
            .unwrap()
            .explain(meta_map),
        DfPredType::UnOp(_) => UnOpPred::from_pred_node(node).unwrap().explain(meta_map),
        DfPredType::BinOp(_) => BinOpPred::from_pred_node(node).unwrap().explain(meta_map),
        DfPredType::Func(_) => FuncPred::from_pred_node(node).unwrap().explain(meta_map),
        DfPredType::List => {
            ListPred::from_pred_node(node) // ExprList is the only place that we will have list in the datafusion repr
                .unwrap()
                .explain(meta_map)
        }
        DfPredType::SortOrder(_) => SortOrderPred::from_pred_node(node)
            .unwrap()
            .explain(meta_map),
        DfPredType::LogOp(_) => LogOpPred::from_pred_node(node).unwrap().explain(meta_map),

        DfPredType::Between => BetweenPred::from_pred_node(node).unwrap().explain(meta_map),
        DfPredType::Cast => CastPred::from_pred_node(node).unwrap().explain(meta_map),
        DfPredType::Like => LikePred::from_pred_node(node).unwrap().explain(meta_map),
        DfPredType::DataType(_) => DataTypePred::from_pred_node(node)
            .unwrap()
            .explain(meta_map),
        DfPredType::InList => InListPred::from_pred_node(node).unwrap().explain(meta_map),
    }
}

pub fn explain_plan_node(
    node: ArcDfPlanNode,
    meta_map: Option<&PlanNodeMetaMap>,
) -> Pretty<'static> {
    match node.typ {
        DfNodeType::Join(_) => LogicalJoin::from_plan_node(node).unwrap().explain(meta_map),
        DfNodeType::RawDepJoin(_) => RawDependentJoin::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::DepJoin(_) => DependentJoin::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::Scan => LogicalScan::from_plan_node(node).unwrap().explain(meta_map),
        DfNodeType::Filter => LogicalFilter::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::EmptyRelation => LogicalEmptyRelation::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::Limit => LogicalLimit::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalFilter => PhysicalFilter::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalScan => PhysicalScan::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::Agg => LogicalAgg::from_plan_node(node).unwrap().explain(meta_map),
        DfNodeType::Sort => LogicalSort::from_plan_node(node).unwrap().explain(meta_map),
        DfNodeType::Projection => LogicalProjection::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalProjection => PhysicalProjection::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalAgg => PhysicalAgg::from_plan_node(node).unwrap().explain(meta_map),
        DfNodeType::PhysicalSort => PhysicalSort::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalHashJoin(_) => PhysicalHashJoin::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalEmptyRelation => PhysicalEmptyRelation::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalLimit => PhysicalLimit::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
        DfNodeType::PhysicalNestedLoopJoin(_) => PhysicalNestedLoopJoin::from_plan_node(node)
            .unwrap()
            .explain(meta_map),
    }
}
