// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::{str::FromStr, sync::Arc};

use optd_core::nodes::{PlanNodeMetaMap, Value};
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
        let x = self.0.clone() as u16;
        x.serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for AggregateFunction {
    fn deserialize<D>(deserializer: D) -> Result<AggregateFunction, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let x = u16::deserialize(deserializer)?;
        let v = if x == datafusion_expr::AggregateFunction::Count as u16 {
            datafusion_expr::AggregateFunction::Count
        } else if x == datafusion_expr::AggregateFunction::Sum as u16 {
            datafusion_expr::AggregateFunction::Sum
        } else if x == datafusion_expr::AggregateFunction::Min as u16 {
            datafusion_expr::AggregateFunction::Min
        } else if x == datafusion_expr::AggregateFunction::Max as u16 {
            datafusion_expr::AggregateFunction::Max
        } else if x == datafusion_expr::AggregateFunction::Avg as u16 {
            datafusion_expr::AggregateFunction::Avg
        } else if x == datafusion_expr::AggregateFunction::Median as u16 {
            datafusion_expr::AggregateFunction::Median
        } else if x == datafusion_expr::AggregateFunction::ApproxDistinct as u16 {
            datafusion_expr::AggregateFunction::ApproxDistinct
        } else if x == datafusion_expr::AggregateFunction::ArrayAgg as u16 {
            datafusion_expr::AggregateFunction::ArrayAgg
        } else if x == datafusion_expr::AggregateFunction::FirstValue as u16 {
            datafusion_expr::AggregateFunction::FirstValue
        } else if x == datafusion_expr::AggregateFunction::LastValue as u16 {
            datafusion_expr::AggregateFunction::LastValue
        } else if x == datafusion_expr::AggregateFunction::Variance as u16 {
            datafusion_expr::AggregateFunction::Variance
        } else if x == datafusion_expr::AggregateFunction::VariancePop as u16 {
            datafusion_expr::AggregateFunction::VariancePop
        } else if x == datafusion_expr::AggregateFunction::Stddev as u16 {
            datafusion_expr::AggregateFunction::Stddev
        } else if x == datafusion_expr::AggregateFunction::StddevPop as u16 {
            datafusion_expr::AggregateFunction::StddevPop
        } else if x == datafusion_expr::AggregateFunction::Covariance as u16 {
            datafusion_expr::AggregateFunction::Covariance
        } else if x == datafusion_expr::AggregateFunction::CovariancePop as u16 {
            datafusion_expr::AggregateFunction::CovariancePop
        } else if x == datafusion_expr::AggregateFunction::Correlation as u16 {
            datafusion_expr::AggregateFunction::Correlation
        } else if x == datafusion_expr::AggregateFunction::RegrSlope as u16 {
            datafusion_expr::AggregateFunction::RegrSlope
        } else if x == datafusion_expr::AggregateFunction::RegrIntercept as u16 {
            datafusion_expr::AggregateFunction::RegrIntercept
        } else if x == datafusion_expr::AggregateFunction::RegrCount as u16 {
            datafusion_expr::AggregateFunction::RegrCount
        } else if x == datafusion_expr::AggregateFunction::RegrR2 as u16 {
            datafusion_expr::AggregateFunction::RegrR2
        } else if x == datafusion_expr::AggregateFunction::RegrAvgx as u16 {
            datafusion_expr::AggregateFunction::RegrAvgx
        } else if x == datafusion_expr::AggregateFunction::RegrAvgy as u16 {
            datafusion_expr::AggregateFunction::RegrAvgy
        } else if x == datafusion_expr::AggregateFunction::RegrSXX as u16 {
            datafusion_expr::AggregateFunction::RegrSXX
        } else if x == datafusion_expr::AggregateFunction::RegrSYY as u16 {
            datafusion_expr::AggregateFunction::RegrSYY
        } else if x == datafusion_expr::AggregateFunction::RegrSXY as u16 {
            datafusion_expr::AggregateFunction::RegrSXY
        } else if x == datafusion_expr::AggregateFunction::ApproxPercentileCont as u16 {
            datafusion_expr::AggregateFunction::ApproxPercentileCont
        } else if x == datafusion_expr::AggregateFunction::ApproxPercentileContWithWeight as u16 {
            datafusion_expr::AggregateFunction::ApproxPercentileContWithWeight
        } else if x == datafusion_expr::AggregateFunction::ApproxMedian as u16 {
            datafusion_expr::AggregateFunction::ApproxMedian
        } else if x == datafusion_expr::AggregateFunction::Grouping as u16 {
            datafusion_expr::AggregateFunction::Grouping
        } else if x == datafusion_expr::AggregateFunction::BitAnd as u16 {
            datafusion_expr::AggregateFunction::BitAnd
        } else if x == datafusion_expr::AggregateFunction::BitOr as u16 {
            datafusion_expr::AggregateFunction::BitOr
        } else if x == datafusion_expr::AggregateFunction::BitXor as u16 {
            datafusion_expr::AggregateFunction::BitXor
        } else if x == datafusion_expr::AggregateFunction::BoolAnd as u16 {
            datafusion_expr::AggregateFunction::BoolAnd
        } else if x == datafusion_expr::AggregateFunction::BoolOr as u16 {
            datafusion_expr::AggregateFunction::BoolOr
        } else {
            panic!("invalid aggregate function")
        };

        Ok(AggregateFunction(v))
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
        let func_id_serialized: Arc<[u8]> =
            bincode::serialize(&func_id).unwrap().into_iter().collect();
        FuncPred(
            DfPredNode {
                typ: DfPredType::Func,
                children: vec![argv.into_pred_node()],
                data: Some(Value::Serialized(func_id_serialized)),
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
        if let DfPredType::Func = self.0.typ {
            let func_id_serialized = self.0.data.as_ref().unwrap().as_slice();
            bincode::deserialize(&func_id_serialized).unwrap()
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
        if !matches!(pred_node.typ, DfPredType::Func) {
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
