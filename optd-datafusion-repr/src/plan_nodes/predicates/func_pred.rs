// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::str::FromStr;

use optd_core::nodes::PlanNodeMetaMap;
use pretty_xmlish::Pretty;
use serde::{Deserialize, Serialize};

use super::ListPred;
use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BuiltinScalarFunction(datafusion_expr::BuiltinScalarFunction);

impl Serialize for BuiltinScalarFunction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.to_string().serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for BuiltinScalarFunction {
    fn deserialize<D>(deserializer: D) -> Result<BuiltinScalarFunction, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(BuiltinScalarFunction(
            datafusion_expr::BuiltinScalarFunction::from_str(&s).unwrap(),
        ))
    }
}

impl From<datafusion_expr::BuiltinScalarFunction> for BuiltinScalarFunction {
    fn from(func: datafusion_expr::BuiltinScalarFunction) -> Self {
        Self(func)
    }
}

impl Into<datafusion_expr::BuiltinScalarFunction> for BuiltinScalarFunction {
    fn into(self) -> datafusion_expr::BuiltinScalarFunction {
        self.0
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AggregateFunction(datafusion_expr::AggregateFunction);

impl Serialize for AggregateFunction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.to_string().serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for AggregateFunction {
    fn deserialize<D>(deserializer: D) -> Result<AggregateFunction, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(AggregateFunction(
            datafusion_expr::AggregateFunction::from_str(&s).unwrap(),
        ))
    }
}

impl From<datafusion_expr::AggregateFunction> for AggregateFunction {
    fn from(func: datafusion_expr::AggregateFunction) -> Self {
        Self(func)
    }
}

impl Into<datafusion_expr::AggregateFunction> for AggregateFunction {
    fn into(self) -> datafusion_expr::AggregateFunction {
        self.0
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum FuncType {
    Scalar(BuiltinScalarFunction),
    Agg(AggregateFunction),
    Case,
}

impl std::fmt::Display for FuncType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FuncType {
    pub fn new_scalar(func_id: datafusion_expr::BuiltinScalarFunction) -> Self {
        FuncType::Scalar(func_id.into())
    }

    pub fn new_agg(func_id: datafusion_expr::AggregateFunction) -> Self {
        FuncType::Agg(func_id.into())
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
