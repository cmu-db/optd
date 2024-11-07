// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::fmt::Display;

use optd_core::nodes::PlanNodeMetaMap;
use pretty_xmlish::Pretty;

use super::ListPred;
use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum LogOpType {
    And,
    Or,
}

impl Display for LogOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogOpPred(pub ArcDfPredNode);

impl LogOpPred {
    pub fn new(op_type: LogOpType, preds: Vec<ArcDfPredNode>) -> Self {
        LogOpPred(
            DfPredNode {
                typ: DfPredType::LogOp(op_type),
                children: preds,
                data: None,
            }
            .into(),
        )
    }

    /// flatten_nested_logical is a helper function to flatten nested logical operators with same op
    /// type eg. (a AND (b AND c)) => ExprList([a, b, c])
    ///    (a OR (b OR c)) => ExprList([a, b, c])
    /// It assume the children of the input expr_list are already flattened
    ///  and can only be used in bottom up manner
    pub fn new_flattened_nested_logical(op: LogOpType, expr_list: ListPred) -> Self {
        // Since we assume that we are building the children bottom up,
        // there is no need to call flatten_nested_logical recursively
        let mut new_expr_list = Vec::new();
        for child in expr_list.to_vec() {
            if let DfPredType::LogOp(child_op) = child.typ {
                if child_op == op {
                    let child_log_op_expr = LogOpPred::from_pred_node(child).unwrap();
                    new_expr_list.extend(child_log_op_expr.children().to_vec());
                    continue;
                }
            }
            new_expr_list.push(child.clone());
        }
        LogOpPred::new(op, new_expr_list)
    }

    pub fn children(&self) -> Vec<ArcDfPredNode> {
        self.0.children.clone()
    }

    pub fn child(&self, idx: usize) -> ArcDfPredNode {
        self.0.child(idx)
    }

    pub fn op_type(&self) -> LogOpType {
        if let DfPredType::LogOp(op_type) = self.0.typ {
            op_type
        } else {
            panic!("not a log op")
        }
    }
}

impl DfReprPredNode for LogOpPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::LogOp(_)) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            self.children()
                .iter()
                .map(|x| x.explain(meta_map))
                .collect(),
        )
    }
}
