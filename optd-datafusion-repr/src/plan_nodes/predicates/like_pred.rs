// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use optd_core::nodes::{PlanNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

#[derive(Clone, Debug)]
pub struct LikePred(pub ArcDfPredNode);

impl LikePred {
    pub fn new(
        negated: bool,
        case_insensitive: bool,
        child: ArcDfPredNode,
        pattern: ArcDfPredNode,
    ) -> Self {
        // TODO: support multiple values in data.
        let negated = if negated { 1 } else { 0 };
        let case_insensitive = if case_insensitive { 1 } else { 0 };
        LikePred(
            DfPredNode {
                typ: DfPredType::Like,
                children: vec![child.into_pred_node(), pattern.into_pred_node()],
                data: Some(Value::Serialized(Arc::new([negated, case_insensitive]))),
            }
            .into(),
        )
    }

    pub fn child(&self) -> ArcDfPredNode {
        self.0.child(0)
    }

    pub fn pattern(&self) -> ArcDfPredNode {
        self.0.child(1)
    }

    /// `true` for `NOT LIKE`.
    pub fn negated(&self) -> bool {
        match self.0.data.as_ref().unwrap() {
            Value::Serialized(data) => data[0] != 0,
            _ => panic!("not a serialized value"),
        }
    }

    pub fn case_insensitive(&self) -> bool {
        match self.0.data.as_ref().unwrap() {
            Value::Serialized(data) => data[1] != 0,
            _ => panic!("not a serialized value"),
        }
    }
}

impl DfReprPredNode for LikePred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::Like) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "Like",
            vec![
                ("expr", self.child().explain(meta_map)),
                ("pattern", self.pattern().explain(meta_map)),
                ("negated", self.negated().to_string().into()),
                (
                    "case_insensitive",
                    self.case_insensitive().to_string().into(),
                ),
            ],
            vec![],
        )
    }
}
