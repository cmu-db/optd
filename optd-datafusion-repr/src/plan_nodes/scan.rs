// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use optd_core::nodes::PlanNodeMetaMap;
use pretty_xmlish::Pretty;

use super::{ArcDfPlanNode, ConstantPred, DfNodeType, DfPlanNode, DfReprPlanNode, DfReprPredNode};
use crate::explain::Insertable;

#[derive(Clone, Debug)]
pub struct LogicalScan(pub ArcDfPlanNode);

impl DfReprPlanNode for LogicalScan {
    fn into_plan_node(self) -> ArcDfPlanNode {
        self.0
    }

    fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self> {
        if plan_node.typ != DfNodeType::Scan {
            return None;
        }
        Some(Self(plan_node))
    }

    fn explain(&self, _meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::childless_record(
            "LogicalScan",
            vec![("table", self.table().to_string().into())],
        )
    }
}

impl LogicalScan {
    pub fn new(table: String) -> LogicalScan {
        LogicalScan(
            DfPlanNode {
                typ: DfNodeType::Scan,
                children: vec![],
                predicates: vec![ConstantPred::string(table).into_pred_node()],
            }
            .into(),
        )
    }

    pub fn table(&self) -> Arc<str> {
        ConstantPred::from_pred_node(self.0.predicates.first().unwrap().clone())
            .unwrap()
            .value()
            .as_str()
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalScan(pub ArcDfPlanNode);

impl DfReprPlanNode for PhysicalScan {
    fn into_plan_node(self) -> ArcDfPlanNode {
        self.0
    }

    fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self> {
        if plan_node.typ != DfNodeType::PhysicalScan {
            return None;
        }
        Some(Self(plan_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        let mut fields = vec![("table", self.table().to_string().into())];
        if let Some(meta_map) = meta_map {
            fields = fields.with_meta(self.0.get_meta(meta_map));
        }
        Pretty::childless_record("PhysicalScan", fields)
    }
}

impl PhysicalScan {
    pub fn table(&self) -> Arc<str> {
        ConstantPred::from_pred_node(self.0.predicates.first().unwrap().clone())
            .unwrap()
            .value()
            .as_str()
    }
}
