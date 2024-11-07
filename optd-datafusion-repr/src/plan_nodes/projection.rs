// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use super::macros::define_plan_node;
use super::{ArcDfPlanNode, DfNodeType, DfPlanNode, DfReprPlanNode, ListPred};

#[derive(Clone, Debug)]
pub struct LogicalProjection(pub ArcDfPlanNode);

define_plan_node!(
    LogicalProjection : DfPlanNode,
    Projection, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, exprs: ListPred }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalProjection(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalProjection : DfPlanNode,
    PhysicalProjection, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, exprs: ListPred }
    ]
);
