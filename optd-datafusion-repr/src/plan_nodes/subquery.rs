// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use super::macros::define_plan_node;
use super::{
    ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfReprPlanNode, JoinType, ListPred,
};

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
    ], { join_type: JoinType }
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
    ], { join_type: JoinType }
);
