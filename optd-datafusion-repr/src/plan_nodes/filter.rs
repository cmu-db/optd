// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use super::macros::define_plan_node;
use super::{ArcDfPlanNode, ArcDfPredNode, DfNodeType, DfPlanNode, DfReprPlanNode};

#[derive(Clone, Debug)]
pub struct LogicalFilter(pub ArcDfPlanNode);

define_plan_node!(
    LogicalFilter : DfPlanNode,
    Filter, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, cond: ArcDfPredNode }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalFilter(pub ArcDfPlanNode);

define_plan_node!(
    PhysicalFilter : DfPlanNode,
    PhysicalFilter, [
        { 0, child: ArcDfPlanNode }
    ], [
        { 0, cond: ArcDfPredNode }
    ]
);
