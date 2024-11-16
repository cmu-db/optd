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
        let x = match self.0 {
            datafusion_expr::AggregateFunction::Count => 0,
            datafusion_expr::AggregateFunction::Sum => 1,
            datafusion_expr::AggregateFunction::Min => 2,
            datafusion_expr::AggregateFunction::Max => 3,
            datafusion_expr::AggregateFunction::Avg => 4,
            datafusion_expr::AggregateFunction::Median => 5,
            datafusion_expr::AggregateFunction::ApproxDistinct => 6,
            datafusion_expr::AggregateFunction::ArrayAgg => 7,
            datafusion_expr::AggregateFunction::FirstValue => 8,
            datafusion_expr::AggregateFunction::LastValue => 9,
            datafusion_expr::AggregateFunction::Variance => 10,
            datafusion_expr::AggregateFunction::VariancePop => 11,
            datafusion_expr::AggregateFunction::Stddev => 12,
            datafusion_expr::AggregateFunction::StddevPop => 13,
            datafusion_expr::AggregateFunction::Covariance => 14,
            datafusion_expr::AggregateFunction::CovariancePop => 15,
            datafusion_expr::AggregateFunction::Correlation => 16,
            datafusion_expr::AggregateFunction::RegrSlope => 17,
            datafusion_expr::AggregateFunction::RegrIntercept => 18,
            datafusion_expr::AggregateFunction::RegrCount => 19,
            datafusion_expr::AggregateFunction::RegrR2 => 20,
            datafusion_expr::AggregateFunction::RegrAvgx => 21,
            datafusion_expr::AggregateFunction::RegrAvgy => 22,
            datafusion_expr::AggregateFunction::RegrSXX => 23,
            datafusion_expr::AggregateFunction::RegrSYY => 24,
            datafusion_expr::AggregateFunction::RegrSXY => 25,
            datafusion_expr::AggregateFunction::ApproxPercentileCont => 26,
            datafusion_expr::AggregateFunction::ApproxPercentileContWithWeight => 27,
            datafusion_expr::AggregateFunction::ApproxMedian => 28,
            datafusion_expr::AggregateFunction::Grouping => 29,
            datafusion_expr::AggregateFunction::BitAnd => 30,
            datafusion_expr::AggregateFunction::BitOr => 31,
            datafusion_expr::AggregateFunction::BitXor => 32,
            datafusion_expr::AggregateFunction::BoolAnd => 33,
            datafusion_expr::AggregateFunction::BoolOr => 34,
        };
        x.serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for AggregateFunction {
    fn deserialize<D>(deserializer: D) -> Result<AggregateFunction, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let x = u16::deserialize(deserializer)?;
        let v = match x {
            0 => datafusion_expr::AggregateFunction::Count,
            1 => datafusion_expr::AggregateFunction::Sum,
            2 => datafusion_expr::AggregateFunction::Min,
            3 => datafusion_expr::AggregateFunction::Max,
            4 => datafusion_expr::AggregateFunction::Avg,
            5 => datafusion_expr::AggregateFunction::Median,
            6 => datafusion_expr::AggregateFunction::ApproxDistinct,
            7 => datafusion_expr::AggregateFunction::ArrayAgg,
            8 => datafusion_expr::AggregateFunction::FirstValue,
            9 => datafusion_expr::AggregateFunction::LastValue,
            10 => datafusion_expr::AggregateFunction::Variance,
            11 => datafusion_expr::AggregateFunction::VariancePop,
            12 => datafusion_expr::AggregateFunction::Stddev,
            13 => datafusion_expr::AggregateFunction::StddevPop,
            14 => datafusion_expr::AggregateFunction::Covariance,
            15 => datafusion_expr::AggregateFunction::CovariancePop,
            16 => datafusion_expr::AggregateFunction::Correlation,
            17 => datafusion_expr::AggregateFunction::RegrSlope,
            18 => datafusion_expr::AggregateFunction::RegrIntercept,
            19 => datafusion_expr::AggregateFunction::RegrCount,
            20 => datafusion_expr::AggregateFunction::RegrR2,
            21 => datafusion_expr::AggregateFunction::RegrAvgx,
            22 => datafusion_expr::AggregateFunction::RegrAvgy,
            23 => datafusion_expr::AggregateFunction::RegrSXX,
            24 => datafusion_expr::AggregateFunction::RegrSYY,
            25 => datafusion_expr::AggregateFunction::RegrSXY,
            26 => datafusion_expr::AggregateFunction::ApproxPercentileCont,
            27 => datafusion_expr::AggregateFunction::ApproxPercentileContWithWeight,
            28 => datafusion_expr::AggregateFunction::ApproxMedian,
            29 => datafusion_expr::AggregateFunction::Grouping,
            30 => datafusion_expr::AggregateFunction::BitAnd,
            31 => datafusion_expr::AggregateFunction::BitOr,
            32 => datafusion_expr::AggregateFunction::BitXor,
            33 => datafusion_expr::AggregateFunction::BoolAnd,
            34 => datafusion_expr::AggregateFunction::BoolOr,
            _ => panic!("invalid aggregate function"),
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
