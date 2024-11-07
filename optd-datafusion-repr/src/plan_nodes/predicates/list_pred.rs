// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use itertools::Itertools;
use optd_core::nodes::PlanNodeMetaMap;
use pretty_xmlish::Pretty;

use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

#[derive(Clone, Debug)]
pub struct ListPred(pub ArcDfPredNode);

impl ListPred {
    pub fn new(preds: Vec<ArcDfPredNode>) -> Self {
        ListPred(
            DfPredNode {
                typ: DfPredType::List,
                children: preds,
                data: None,
            }
            .into(),
        )
    }

    /// Gets number of expressions in the list
    pub fn len(&self) -> usize {
        self.0.children.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.children.is_empty()
    }

    pub fn child(&self, idx: usize) -> ArcDfPredNode {
        self.0.child(idx)
    }

    pub fn to_vec(&self) -> Vec<ArcDfPredNode> {
        self.0.children.clone()
    }
}

impl DfReprPredNode for ListPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if pred_node.typ != DfPredType::List {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::Array(
            (0..self.len())
                .map(|x| self.child(x).explain(meta_map))
                .collect_vec(),
        )
    }
}
