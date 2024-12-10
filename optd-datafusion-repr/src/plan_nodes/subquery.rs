// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use core::fmt;
use std::fmt::Display;

use super::macros::define_plan_node;
use super::{
    ArcDfPlanNode, ArcDfPredNode, BinOpType, DfNodeType, DfPlanNode, DfPredNode, DfReprPlanNode,
    ListPred,
};

/// These are the only three fundamental types of subqueries.
/// Refer to the Unnesting Arbitrary Queries talk by Mark Raasveldt for
/// info on how to translate other subquery types to these three.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubqueryType {
    Scalar,
    Exists,
    Any { pred: DfPredNode, op: BinOpType },
}

impl Display for SubqueryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct RawDependentJoin(pub ArcDfPlanNode);

define_plan_node!(
    RawDependentJoin : DfReprPlanNode,
    RawDepJoin, [
        { 0, left: ArcDfPlanNode },
        { 1, right: ArcDfPlanNode }
    ], [
        { 0, cond: ArcDfPredNode },
        { 1, extern_cols: ListPred }
    ], { sq_type: SubqueryType }
);

#[derive(Clone, Debug)]
pub struct DependentJoin(pub ArcDfPlanNode);

define_plan_node!(
    DependentJoin : DfReprPlanNode,
    DepJoin, [
        { 0, left: ArcDfPlanNode },
        { 1, right: ArcDfPlanNode }
    ], [
        { 0, cond: ArcDfPredNode },
        { 1, extern_cols: ListPred }
    ]
);
