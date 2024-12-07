// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use optd_core::nodes::PlanNodeMetaMap;
use pretty_xmlish::Pretty;

use super::ListPred;
use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FuncType {
    Scalar(String),
    Agg(String),
    Case,
}

impl std::fmt::Display for FuncType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FuncType::Scalar(func_id) => write!(f, "Scalar({})", heck::AsUpperCamelCase(func_id)),
            FuncType::Agg(func_id) => write!(f, "Agg({})", heck::AsUpperCamelCase(func_id)),
            _ => write!(f, "{:?}", self),
        }
    }
}

impl FuncType {
    pub fn new_scalar(func_id: String) -> Self {
        FuncType::Scalar(func_id)
    }

    pub fn new_agg(func_id: String) -> Self {
        FuncType::Agg(func_id)
    }
}

#[derive(Clone, Debug)]
pub struct FuncPred(pub ArcDfPredNode);

impl FuncPred {
    pub fn new(func_id: FuncType, argv: ListPred) -> Self {
        FuncPred(
            DfPredNode {
                typ: DfPredType::Func(func_id),
                children: vec![argv.into_pred_node()],
                data: None,
            }
            .into(),
        )
    }

    /// Gets the i-th argument of the function.
    pub fn arg_at(&self, i: usize) -> ArcDfPredNode {
        self.children().child(i)
    }

    /// Get all children.
    pub fn children(&self) -> ListPred {
        ListPred::from_pred_node(self.0.child(0)).unwrap()
    }

    /// Gets the function id.
    pub fn func(&self) -> FuncType {
        if let DfPredType::Func(ref func_id) = self.0.typ {
            func_id.clone()
        } else {
            panic!("not a function")
        }
    }
}

impl DfReprPredNode for FuncPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0.into_pred_node()
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::Func(_)) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.func().to_string(),
            vec![],
            vec![self.children().explain(meta_map)],
        )
    }
}
