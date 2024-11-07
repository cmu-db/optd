// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use optd_core::nodes::PlanNodeMetaMap;
use pretty_xmlish::Pretty;

use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

#[derive(Clone, Debug)]
pub struct BetweenPred(pub ArcDfPredNode);

impl BetweenPred {
    pub fn new(child: ArcDfPredNode, lower: ArcDfPredNode, upper: ArcDfPredNode) -> Self {
        BetweenPred(
            DfPredNode {
                typ: DfPredType::Between,
                children: vec![child, lower, upper],
                data: None,
            }
            .into(),
        )
    }

    pub fn child(&self) -> ArcDfPredNode {
        self.0.child(0)
    }

    pub fn lower(&self) -> ArcDfPredNode {
        self.0.child(1)
    }

    pub fn upper(&self) -> ArcDfPredNode {
        self.0.child(2)
    }
}

impl DfReprPredNode for BetweenPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::Between) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "Between",
            vec![
                ("child", self.child().explain(meta_map)),
                ("lower", self.lower().explain(meta_map)),
                ("upper", self.upper().explain(meta_map)),
            ],
            vec![],
        )
    }
}
